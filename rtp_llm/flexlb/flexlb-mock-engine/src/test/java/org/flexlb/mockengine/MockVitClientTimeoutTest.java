package org.flexlb.mockengine;

import com.google.protobuf.ByteString;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.flexlb.engine.grpc.EngineRpcService;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** No network or production requests. Transport deadline delivery is deliberately absent. */
class MockVitClientTimeoutTest {
    static void check(boolean b, String s) {
        if (!b) throw new AssertionError(s);
    }

    static class FakeChannel extends ManagedChannel {
        int cancelled;
        long deadlineMs;
        String mode = "never";

        public String authority() {
            return "fixture";
        }

        public ManagedChannel shutdown() {
            return this;
        }

        public ManagedChannel shutdownNow() {
            return this;
        }

        public boolean isShutdown() {
            return false;
        }

        public boolean isTerminated() {
            return false;
        }

        public boolean awaitTermination(long n, TimeUnit u) {
            return true;
        }

        public <Q, S> ClientCall<Q, S> newCall(MethodDescriptor<Q, S> method, CallOptions options) {
            deadlineMs = options.getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
            return new ClientCall<Q, S>() {
                Listener<S> listener;

                public void start(Listener<S> l, Metadata h) {
                    listener = l;
                }

                public void request(int n) {}

                public void sendMessage(Q q) {}

                public void cancel(String m, Throwable t) {
                    cancelled++;
                    listener.onClose(Status.CANCELLED, new Metadata());
                }

                @SuppressWarnings("unchecked")
                public void halfClose() {
                    if (mode.equals("error")) listener.onClose(Status.UNAVAILABLE, new Metadata());
                    if (mode.equals("success")) {
                        var tensor =
                                EngineRpcService.TensorPB.newBuilder()
                                        .setDataType(EngineRpcService.TensorPB.DataType.BF16)
                                        .addShape(1)
                                        .addShape(4)
                                        .setBf16Data(ByteString.copyFrom(new byte[8]));
                        listener.onMessage(
                                (S)
                                        EngineRpcService.MultimodalOutputPB.newBuilder()
                                                .addSplitSize(1)
                                                .setMultimodalEmbedding(tensor)
                                                .build());
                        listener.onClose(Status.OK, new Metadata());
                    }
                }
            };
        }
    }

    static EngineRpcService.GenerateInputPB input() {
        var b = EngineRpcService.GenerateInputPB.newBuilder().addTokenIds(7);
        b.getGenerateConfigBuilder()
                .addRoleAddrs(
                        EngineRpcService.RoleAddrPB.newBuilder()
                                .setRoleStr("VIT")
                                .setRole(EngineRpcService.RoleAddrPB.RoleType.VIT)
                                .setIp("127.0.0.1")
                                .setGrpcPort(1));
        b.setV41Inputs(
                EngineRpcService.V41RequestInputsPB.newBuilder()
                        .setSchemaVersion(1)
                        .addTokenTypes(0)
                        .addImageMask(true)
                        .addImages(
                                EngineRpcService.V41ImageInputPB.newBuilder()
                                        .setStart(0)
                                        .addTypes(0)));
        return b.build();
    }

    @SuppressWarnings("unchecked")
    @org.junit.jupiter.api.Test
    void boundsWaitWithoutTransportTimerAndPreservesErrors() throws Exception {
        try (var client = new MockVitClient()) {
            var channel = new FakeChannel();
            Field f = MockVitClient.class.getDeclaredField("channels");
            f.setAccessible(true);
            ((Map<String, ManagedChannel>) f.get(client)).put("127.0.0.1:1", channel);
            var input = input();
            long start = System.nanoTime();
            try {
                client.prepare(input, 75);
                throw new AssertionError("unbounded wait accepted");
            } catch (StatusRuntimeException e) {
                check(e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED, "timeout status");
            }
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            check(elapsed < 1000 && channel.cancelled == 1, "independent timeout/cancel");
            channel.mode = "error";
            try {
                client.prepare(input, 1000);
                throw new AssertionError("RPC error hidden");
            } catch (StatusRuntimeException e) {
                check(e.getStatus().getCode() == Status.Code.UNAVAILABLE, "error propagation");
            }
            channel.mode = "success";
            check(client.prepare(input, 500000) == input, "canonical input changed");
            check(
                    channel.deadlineMs <= 120000 && channel.deadlineMs > 119000,
                    "native deadline changed");
            channel.mode = "never";
            Thread.currentThread().interrupt();
            try {
                client.prepare(input, 1000);
                throw new AssertionError("interrupt hidden");
            } catch (StatusRuntimeException e) {
                check(
                        e.getStatus().getCode() == Status.Code.CANCELLED
                                && Thread.currentThread().isInterrupted(),
                        "interrupt propagation");
            } finally {
                Thread.interrupted();
            }
            check(channel.cancelled == 2, "interrupted RPC not cancelled");
        }
    }
}
