package org.flexlb.mock.grpc;

import org.flexlb.balance.scheduler.RequestLifecycleState;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.mock.FlexLBMockTestBase;
import org.flexlb.mock.InflightAssertions;
import org.flexlb.mock.MockWorkerBehavior;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A real EnqueueBatch deadline retains ownership until a real Cancel RPC fences admission. */
class GrpcTimeoutTest extends FlexLBMockTestBase {

    @Override
    protected MockWorkerBehavior createPrefillBehavior() {
        return MockWorkerBehavior.builder()
                .enqueueDelayMs(3000)  // 3s: far exceeds the 500ms deadline
                .build();
    }

    @Override
    protected FlexlbConfig createConfig() {
        FlexlbConfig cfg = new FlexlbConfig();
        cfg.setFlexlbBatchSizeMax(1);        // single request triggers immediate dispatch
        cfg.setFlexlbBatchWindowMs(300);
        cfg.setCostSloMs(50_000L);
        cfg.setCostSloRiskMarginMs(50L);
        cfg.setFlexlbBatchEnqueueDeadlineMs(500);  // 500ms deadline — will time out
        cfg.setFlexlbInflightTtlMs(300_000L);
        return cfg;
    }

    @Test
    @Timeout(15)
    void grpcTimeout_requestFailsAndRecovers() throws Exception {
        CountDownLatch allowAdmission = new CountDownLatch(1);
        CountDownLatch allowCancelFence = new CountDownLatch(1);
        mockPrefillWorker.getRpcService().setEnqueueAdmissionGate(allowAdmission);
        mockPrefillWorker.getRpcService().setCancelFenceGate(allowCancelFence);
        try {
            CompletableFuture<Response> future = submitRequest(10001);
            awaitCondition(() -> mockPrefillWorker.getRpcService().getCancelCallCount() > 0,
                    3_000, "the real gRPC deadline must initiate Cancel reconciliation");
            assertEquals(1, mockPrefillWorker.getEnqueueCount());
            assertFalse(future.isDone(), "transport timeout alone cannot settle ownership");
            assertEquals(1, getPrefillEndpoint().getInflightBatchCount());
            assertEquals(1, getDecodeEndpoint().getInflightCount());
            assertFalse(mockPrefillWorker.getRpcService().isAdmitted(10001));

            allowCancelFence.countDown();
            Response response = future.get(5, TimeUnit.SECONDS);

            // 2. Verify: request failed with an explicit timeout result
            assertFalse(response.isSuccess(), "Request should fail when EnqueueBatch times out");
            assertEquals(StrategyErrorType.BATCH_SLO_EXPIRED.getErrorCode(), response.getCode(),
                    "Request should have BATCH_SLO_EXPIRED error code");
            assertEquals(RequestLifecycleState.TIMED_OUT,
                    scheduler.getRequestState(10001L, 0).state());
            assertTrue(response.getErrorMessage().contains("engine fenced late enqueue"));
            assertTrue(mockPrefillWorker.getRpcService().isAbsentFenced(10001));

            // 3. Verify: mock prefill received the EnqueueBatch call (recorded before sleep)
            assertTrue(mockPrefillWorker.getEnqueueCount() >= 1,
                    "Prefill worker should have received at least 1 EnqueueBatch call");

            // 4. Verify: timeout handling cleans up PrefillEndpoint inflight state
            InflightAssertions.assertResourcesReleasedWithin(getPrefillEndpoint(), getDecodeEndpoint(), 1_000);

            // The original handler is still live after its client deadline. It must
            // enforce the same fence before any late admission or success response.
            allowAdmission.countDown();
            awaitCondition(() -> mockPrefillWorker.getRpcService().wasFencedEnqueueRejected(10001),
                    5_000, "the late EnqueueBatch must be rejected by the engine fence");
            assertFalse(mockPrefillWorker.getRpcService().isAdmitted(10001));

            // 5. Verify: decode worker never received any request (PD-separated)
            assertEquals(0, mockDecodeWorker.getEnqueueCount(),
                    "Decode worker should not have received any request");

            // 6. Recover: change behavior to normal delay
            mockPrefillWorker.setBehavior(MockWorkerBehavior.builder().build());

            // 7. Submit a new request — should succeed on the same gRPC channel
            //    (deadline exceeded only cancels the specific call, not the channel)
            CompletableFuture<Response> future2 = submitRequest(10002);
            Response response2 = future2.get(5, TimeUnit.SECONDS);
            assertTrue(response2.isSuccess(), "Subsequent request should succeed after recovery");
            assertTrue(mockPrefillWorker.getRpcService().isAdmitted(10002));
        } finally {
            allowCancelFence.countDown();
            allowAdmission.countDown();
        }
    }
}
