#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "rtp_llm/cpp/config/ConfigModules.h"
#include "rtp_llm/cpp/model_rpc/GenerateContext.h"
#include "rtp_llm/cpp/model_rpc/LocalRpcServer.h"
#include "rtp_llm/cpp/model_rpc/PrefillRpcServer.h"
#include "rtp_llm/cpp/normal_engine/NormalGenerateStream.h"

using namespace ::testing;

namespace rtp_llm {

class MockGenerateStream: public GenerateStream {
public:
    MockGenerateStream(const std::shared_ptr<GenerateInput>& input,
                       const ModelConfig&                    model_config,
                       const RuntimeConfig&                  runtime_config):
        GenerateStream(input, model_config, runtime_config, ResourceContext{}, nullptr) {}

    MOCK_METHOD((ErrorResult<GenerateOutputs>), nextOutput, (int64_t), (override));
    MOCK_METHOD(void, updateOutput, (const StreamUpdateInfo&), (override));
};

class TestLocalRpcServer: public LocalRpcServer {
public:
    grpc::Status poll(std::shared_ptr<GenerateStream>& stream) {
        return pollStreamOutput(nullptr, "request", nullptr, stream);
    }

    grpc::Status poll(WriterInterface* writer, std::shared_ptr<GenerateStream>& stream) {
        return pollStreamOutput(nullptr, "request", writer, stream);
    }

    ErrorInfo collect(std::shared_ptr<GenerateStream>& stream) {
        GenerateOutputs last_outputs;
        return collectStreamOutput(nullptr, stream, nullptr, last_outputs);
    }

    std::future<void> cancellationChecked() {
        return cancellation_checked_.get_future();
    }

    std::atomic<bool> cancelled{false};

protected:
    bool isCancelled(grpc::ServerContext*) const override {
        std::call_once(cancellation_check_once_, [this] { cancellation_checked_.set_value(); });
        return cancelled.load();
    }

private:
    mutable std::once_flag     cancellation_check_once_;
    mutable std::promise<void> cancellation_checked_;
};

class RecordingWriter: public LocalRpcServer::WriterInterface {
public:
    bool Write(const GenerateOutputsPB& outputs, grpc::WriteOptions) override {
        outputs_.push_back(outputs);
        return true;
    }

    std::vector<GenerateOutputsPB> outputs_;
};

class FailingWriter: public LocalRpcServer::WriterInterface {
public:
    bool Write(const GenerateOutputsPB&, grpc::WriteOptions) override {
        ++write_count_;
        return false;
    }

    size_t write_count_{0};
};

enum class WakeReason {
    OUTPUT,
    FINISHED,
    STREAM_ERROR,
    TIMEOUT
};

std::shared_ptr<MockGenerateStream> createMockStream() {
    auto input             = std::make_shared<GenerateInput>();
    input->generate_config = std::make_shared<GenerateConfig>();
    input->input_ids       = torch::tensor({1, 2, 3}, torch::kInt32);

    ModelConfig model_config;
    model_config.max_seq_len = 3;
    return std::make_shared<MockGenerateStream>(input, model_config, RuntimeConfig{});
}

template<typename Stream = NormalGenerateStream>
std::shared_ptr<Stream> createNormalStream() {
    auto input             = std::make_shared<GenerateInput>();
    input->generate_config = std::make_shared<GenerateConfig>();
    input->begin_time_us   = autil::TimeUtility::currentTimeInMicroSeconds();
    input->input_ids       = torch::tensor({1, 2, 3}, torch::kInt32);

    ModelConfig model_config;
    model_config.max_seq_len = 3;
    return std::make_shared<Stream>(input, model_config, RuntimeConfig{}, ResourceContext{}, nullptr);
}

class CleanupObservedStream: public NormalGenerateStream {
public:
    using NormalGenerateStream::NormalGenerateStream;

    std::future<void> cleanupPolling() {
        return cleanup_polling_.get_future();
    }

    void observeCleanupOnCurrentThread() {
        cleanup_thread_ = std::this_thread::get_id();
        observing_.store(true, std::memory_order_release);
    }

    StreamState getStatus() const override {
        const auto state = GenerateStream::getStatus();
        if (observing_.load(std::memory_order_acquire) && std::this_thread::get_id() == cleanup_thread_
            && state == StreamState::RUNNING && ++running_reads_ == 2) {
            // A repeated RUNNING read proves cleanup has executed its cancellation decision.
            cleanup_polling_.set_value();
        }
        return state;
    }

private:
    std::thread::id            cleanup_thread_;
    std::atomic<bool>          observing_{false};
    mutable size_t            running_reads_{0};
    mutable std::promise<void> cleanup_polling_;
};

class TestGenerateContext: public GenerateContext {
public:
    using GenerateContext::GenerateContext;
    using GenerateContext::stopStream;
};

enum class ContextCleanupPath {
    DESTRUCTOR,
    STOP_STREAM
};

enum class RunningCleanupScenario {
    GENERATE_DONE,
    UNFINISHED,
    EXECUTION_ERROR,
    TIMEOUT_ERROR,
    REMOTE_HANDOFF
};

void checkRunningContextCleanup(ContextCleanupPath path, RunningCleanupScenario scenario) {
    SCOPED_TRACE(static_cast<int>(path));
    SCOPED_TRACE(static_cast<int>(scenario));
    auto stream = createNormalStream<CleanupObservedStream>();
    stream->generate_status_->status.store(StreamState::RUNNING);
    ErrorCode expected_error = ErrorCode::CANCELLED;
    if (scenario == RunningCleanupScenario::GENERATE_DONE) {
        stream->reportEvent(StreamEvents::GenerateDone);
        expected_error = ErrorCode::NONE_ERROR;
    } else if (scenario == RunningCleanupScenario::EXECUTION_ERROR) {
        stream->reportError(ErrorCode::EXECUTION_EXCEPTION, "original execution failure");
        expected_error = ErrorCode::EXECUTION_EXCEPTION;
    } else if (scenario == RunningCleanupScenario::TIMEOUT_ERROR) {
        stream->reportError(ErrorCode::GENERATE_TIMEOUT, "original timeout");
        expected_error = ErrorCode::GENERATE_TIMEOUT;
    } else if (scenario == RunningCleanupScenario::REMOTE_HANDOFF) {
        stream->reportEvent(StreamEvents::NeedRemoteGenerate);
    }
    const auto original_status = stream->statusInfo();
    if (scenario == RunningCleanupScenario::GENERATE_DONE || scenario == RunningCleanupScenario::REMOTE_HANDOFF) {
        const auto consumer_result = stream->nextOutput(1);
        ASSERT_FALSE(consumer_result.ok());
        EXPECT_EQ(consumer_result.status().code(), ErrorCode::FINISHED);
        EXPECT_EQ(stream->getStatus(), StreamState::RUNNING);
        EXPECT_FALSE(stream->hasError());
    }

    auto                        meta = std::make_shared<RpcServerRuntimeMeta>();
    kmonitor::MetricsReporterPtr metrics_reporter;
    auto context = std::make_unique<TestGenerateContext>(0, 0, nullptr, metrics_reporter, meta);
    context->setStream(stream);
    auto cleanup_polling = stream->cleanupPolling();
    auto cleanup = std::async(std::launch::async, [context = std::move(context), stream, path]() mutable {
        stream->observeCleanupOnCurrentThread();
        bool detached = true;
        if (path == ContextCleanupPath::STOP_STREAM) {
            context->stopStream();
            detached = context->getStream() == nullptr;
        }
        context.reset();
        return detached;
    });

    const auto polling_status = cleanup_polling.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(polling_status, std::future_status::ready);
    if (polling_status == std::future_status::ready) {
        EXPECT_EQ(cleanup.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
        EXPECT_EQ(stream->getStatus(), StreamState::RUNNING);
        EXPECT_EQ(stream->statusInfo().code(), expected_error);
        if (original_status.hasError()) {
            EXPECT_EQ(stream->statusInfo().ToString(), original_status.ToString());
        }
        EXPECT_FALSE(stream->stream_cache_resource_->isResourceReleased());
    }

    // Always unblock cleanup, including after a failed observation or assertion.
    stream->reportEvent(StreamEvents::GenerateDone);
    EXPECT_EQ(stream->moveToNext(), StreamState::FINISHED);
    const auto cleanup_status = cleanup.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(cleanup_status, std::future_status::ready);
    if (cleanup_status == std::future_status::ready) {
        EXPECT_TRUE(cleanup.get());
    }
    EXPECT_EQ(stream->getStatus(), StreamState::FINISHED);
    EXPECT_EQ(stream->statusInfo().code(), expected_error);
    if (original_status.hasError()) {
        EXPECT_EQ(stream->statusInfo().ToString(), original_status.ToString());
    }

    const auto runtime = meta->getEngineScheduleInfo(-1);
    ASSERT_EQ(runtime.finished_task_info_list.size(), 1u);
    if (scenario == RunningCleanupScenario::GENERATE_DONE || original_status.hasError()
        || path == ContextCleanupPath::DESTRUCTOR) {
        EXPECT_EQ(runtime.finished_task_info_list[0].error_code, static_cast<int64_t>(expected_error));
    }
}

TEST(LocalRpcServerTest, ContextCleanupPreservesPendingGenerateDoneUntilSchedulerCommit) {
    for (const auto path : std::array{ContextCleanupPath::DESTRUCTOR, ContextCleanupPath::STOP_STREAM}) {
        checkRunningContextCleanup(path, RunningCleanupScenario::GENERATE_DONE);
    }
}

TEST(LocalRpcServerTest, ContextCleanupCancelsUnfinishedRunningStreamAndWaitsForScheduler) {
    for (const auto path : std::array{ContextCleanupPath::DESTRUCTOR, ContextCleanupPath::STOP_STREAM}) {
        checkRunningContextCleanup(path, RunningCleanupScenario::UNFINISHED);
    }
}

TEST(LocalRpcServerTest, ContextCleanupPreservesExistingRunningError) {
    for (const auto path : std::array{ContextCleanupPath::DESTRUCTOR, ContextCleanupPath::STOP_STREAM}) {
        for (const auto scenario :
             std::array{RunningCleanupScenario::EXECUTION_ERROR, RunningCleanupScenario::TIMEOUT_ERROR}) {
            checkRunningContextCleanup(path, scenario);
        }
    }
}

TEST(LocalRpcServerTest, ContextCleanupDoesNotMistakeRemoteHandoffForGenerateDone) {
    for (const auto path : std::array{ContextCleanupPath::DESTRUCTOR, ContextCleanupPath::STOP_STREAM}) {
        checkRunningContextCleanup(path, RunningCleanupScenario::REMOTE_HANDOFF);
    }
}

TEST(LocalRpcServerTest, ContextCleanupLeavesCommittedFinishedStreamUnchanged) {
    for (const auto path : std::array{ContextCleanupPath::DESTRUCTOR, ContextCleanupPath::STOP_STREAM}) {
        SCOPED_TRACE(static_cast<int>(path));
        auto stream = createNormalStream();
        stream->generate_status_->status.store(StreamState::RUNNING);
        stream->reportEvent(StreamEvents::GenerateDone);
        ASSERT_EQ(stream->moveToNext(), StreamState::FINISHED);
        auto                        meta = std::make_shared<RpcServerRuntimeMeta>();
        kmonitor::MetricsReporterPtr metrics_reporter;
        {
            TestGenerateContext context(0, 0, nullptr, metrics_reporter, meta);
            context.setStream(stream);
            if (path == ContextCleanupPath::STOP_STREAM) {
                context.stopStream();
                EXPECT_EQ(context.getStream(), nullptr);
            }
        }
        EXPECT_FALSE(stream->hasError());
        EXPECT_FALSE(stream->cancelIfNotFinished("late cancellation"));
        EXPECT_EQ(stream->getStatus(), StreamState::FINISHED);
    }
}

TEST(LocalRpcServerTest, ContextCleanupCancelsUnfinishedWaitingAndLoadingStreams) {
    for (const auto path : std::array{ContextCleanupPath::DESTRUCTOR, ContextCleanupPath::STOP_STREAM}) {
        for (const auto state : std::array{StreamState::WAITING, StreamState::LOADING_CACHE}) {
            SCOPED_TRACE(static_cast<int>(path));
            SCOPED_TRACE(static_cast<int>(state));
            auto stream = createNormalStream();
            stream->generate_status_->status.store(state);
            stream->reportEvent(StreamEvents::GenerateDone);
            auto                        meta = std::make_shared<RpcServerRuntimeMeta>();
            kmonitor::MetricsReporterPtr metrics_reporter;
            {
                TestGenerateContext context(0, 0, nullptr, metrics_reporter, meta);
                context.setStream(stream);
                if (path == ContextCleanupPath::STOP_STREAM) {
                    context.stopStream();
                    EXPECT_EQ(context.getStream(), nullptr);
                }
            }
            EXPECT_EQ(stream->getStatus(), state);
            EXPECT_EQ(stream->statusInfo().code(), ErrorCode::CANCELLED);
            const auto cancelled = stream->statusInfo().ToString();
            EXPECT_FALSE(stream->cancelIfNotFinished("second cancellation"));
            EXPECT_EQ(stream->statusInfo().ToString(), cancelled);
        }
    }
}

TEST(LocalRpcServerTest, GeneralReportErrorStillPublishesPostprocessingFailureAfterGenerateDone) {
    auto stream = createNormalStream();
    stream->generate_status_->status.store(StreamState::RUNNING);
    stream->reportEvent(StreamEvents::GenerateDone);
    EXPECT_FALSE(stream->cancelIfNotFinished("late cancellation"));
    EXPECT_FALSE(stream->hasError());
    stream->reportError(ErrorCode::EXECUTION_EXCEPTION, "postprocessing failed");
    EXPECT_EQ(stream->statusInfo().code(), ErrorCode::EXECUTION_EXCEPTION);
    const auto result = stream->nextOutput(1);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), ErrorCode::EXECUTION_EXCEPTION);
    EXPECT_EQ(stream->getStatus(), StreamState::RUNNING);
}

ErrorResult<GenerateOutputs> wakeResult(WakeReason reason) {
    switch (reason) {
        case WakeReason::OUTPUT: {
            GenerateOutputs outputs;
            return ErrorResult<GenerateOutputs>(std::move(outputs));
        }
        case WakeReason::FINISHED:
            return ErrorResult<GenerateOutputs>(ErrorCode::FINISHED, "finished");
        case WakeReason::STREAM_ERROR:
            return ErrorResult<GenerateOutputs>(ErrorCode::EXECUTION_EXCEPTION, "failed");
        case WakeReason::TIMEOUT:
            return ErrorResult<GenerateOutputs>(ErrorCode::GENERATE_TIMEOUT, "timeout");
    }
    return ErrorResult<GenerateOutputs>(ErrorCode::UNKNOWN_ERROR, "unknown wake reason");
}

void publishWakeError(MockGenerateStream* stream, WakeReason reason) {
    if (reason == WakeReason::STREAM_ERROR) {
        stream->reportError(ErrorCode::EXECUTION_EXCEPTION, "failed");
    } else if (reason == WakeReason::TIMEOUT) {
        stream->reportError(ErrorCode::GENERATE_TIMEOUT, "timeout");
    }
}

ErrorCode expectedStreamError(WakeReason reason) {
    if (reason == WakeReason::STREAM_ERROR) {
        return ErrorCode::EXECUTION_EXCEPTION;
    }
    if (reason == WakeReason::TIMEOUT) {
        return ErrorCode::GENERATE_TIMEOUT;
    }
    return ErrorCode::CANCELLED;
}

TEST(LocalRpcServerTest, PollChecksCancellationBeforeHandlingEveryWakeReason) {
    for (const auto reason :
         std::array{WakeReason::OUTPUT, WakeReason::FINISHED, WakeReason::STREAM_ERROR, WakeReason::TIMEOUT}) {
        TestLocalRpcServer server;
        auto               mock_stream     = createMockStream();
        auto*              mock_stream_ptr = mock_stream.get();
        EXPECT_CALL(*mock_stream, nextOutput(_)).WillOnce(InvokeWithoutArgs([&server, mock_stream_ptr, reason] {
            publishWakeError(mock_stream_ptr, reason);
            server.cancelled = true;
            return wakeResult(reason);
        }));
        std::shared_ptr<GenerateStream> stream = mock_stream;

        const auto status = server.poll(stream);

        EXPECT_EQ(status.error_code(), grpc::StatusCode::CANCELLED);
        EXPECT_EQ(stream->statusInfo().code(), expectedStreamError(reason));
    }
}

TEST(LocalRpcServerTest, CollectChecksCancellationBeforeHandlingEveryWakeReason) {
    for (const auto reason :
         std::array{WakeReason::OUTPUT, WakeReason::FINISHED, WakeReason::STREAM_ERROR, WakeReason::TIMEOUT}) {
        TestLocalRpcServer server;
        auto               mock_stream     = createMockStream();
        auto*              mock_stream_ptr = mock_stream.get();
        EXPECT_CALL(*mock_stream, nextOutput(_)).WillOnce(InvokeWithoutArgs([&server, mock_stream_ptr, reason] {
            publishWakeError(mock_stream_ptr, reason);
            server.cancelled = true;
            return wakeResult(reason);
        }));
        std::shared_ptr<GenerateStream> stream = mock_stream;

        const auto status = server.collect(stream);

        EXPECT_EQ(status.code(), ErrorCode::CANCELLED);
        EXPECT_EQ(stream->statusInfo().code(), expectedStreamError(reason));
    }
}

TEST(LocalRpcServerTest, PollInterruptsBlockedNextOutputAfterClientCancellation) {
    TestLocalRpcServer              server;
    auto                            cancellation_checked = server.cancellationChecked();
    std::shared_ptr<GenerateStream> stream               = createNormalStream();
    auto poll_result = std::async(std::launch::async, [&server, &stream] { return server.poll(stream); });

    EXPECT_EQ(cancellation_checked.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    server.cancelled = true;

    const auto wait_status = poll_result.wait_for(std::chrono::seconds(5));
    if (wait_status != std::future_status::ready) {
        stream->reportError(ErrorCode::EXECUTION_EXCEPTION, "test poll cancellation timed out");
    }
    EXPECT_EQ(wait_status, std::future_status::ready);
    EXPECT_EQ(poll_result.get().error_code(), grpc::StatusCode::CANCELLED);
    EXPECT_EQ(stream->statusInfo().code(), ErrorCode::CANCELLED);
}

TEST(LocalRpcServerTest, PollClassifiesClosedResponseStreamAsCancelled) {
    TestLocalRpcServer server;
    FailingWriter      writer;
    auto               mock_stream = createMockStream();
    EXPECT_CALL(*mock_stream, nextOutput(_)).WillOnce(InvokeWithoutArgs([] {
        GenerateOutputs outputs;
        outputs.request_id = 1;
        return ErrorResult<GenerateOutputs>(std::move(outputs));
    }));
    std::shared_ptr<GenerateStream> stream = mock_stream;

    const auto status = server.poll(&writer, stream);

    EXPECT_EQ(status.error_code(), grpc::StatusCode::CANCELLED);
    EXPECT_EQ(status.error_message(), "request output consumer closed");
    EXPECT_EQ(stream->statusInfo().code(), ErrorCode::CANCELLED);
    EXPECT_EQ(writer.write_count_, 1);
}

TEST(LocalRpcServerTest, CollectInterruptsBlockedNextOutputAfterClientCancellation) {
    TestLocalRpcServer              server;
    auto                            cancellation_checked = server.cancellationChecked();
    std::shared_ptr<GenerateStream> stream               = createNormalStream();
    auto collect_result = std::async(std::launch::async, [&server, &stream] { return server.collect(stream); });

    EXPECT_EQ(cancellation_checked.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    server.cancelled = true;

    const auto wait_status = collect_result.wait_for(std::chrono::seconds(5));
    if (wait_status != std::future_status::ready) {
        stream->reportError(ErrorCode::EXECUTION_EXCEPTION, "test collect cancellation timed out");
    }
    EXPECT_EQ(wait_status, std::future_status::ready);
    EXPECT_EQ(collect_result.get().code(), ErrorCode::CANCELLED);
    EXPECT_EQ(stream->statusInfo().code(), ErrorCode::CANCELLED);
}

TEST(LocalRpcServerTest, PollWritesFinalLocalOutputBeforeRemoteHandoff) {
    TestLocalRpcServer              server;
    RecordingWriter                 writer;
    auto                            normal_stream = createNormalStream();
    std::shared_ptr<GenerateStream> stream        = normal_stream;
    normal_stream->setNeedReleaseResource(true);
    normal_stream->generate_status_->status.store(StreamState::RUNNING);

    {
        std::lock_guard<std::mutex> lock(*normal_stream->mutex_);
        GenerateOutputs             outputs;
        outputs.request_id = 123;
        normal_stream->enqueueGenerateOutput(std::move(outputs));
        normal_stream->reportEventWithoutLock(StreamEvents::NeedRemoteGenerate);
    }

    const auto status = server.poll(&writer, stream);

    EXPECT_TRUE(status.ok());
    ASSERT_EQ(writer.outputs_.size(), 1);
    EXPECT_EQ(writer.outputs_[0].request_id(), 123);
    EXPECT_TRUE(stream->hasEvent(StreamEvents::NeedRemoteGenerate));
    EXPECT_EQ(stream->getStatus(), StreamState::RUNNING);
    EXPECT_FALSE(normal_stream->stream_cache_resource_->isResourceReleased());
    EXPECT_FALSE(normal_stream->hasOutput());
}

}  // namespace rtp_llm
