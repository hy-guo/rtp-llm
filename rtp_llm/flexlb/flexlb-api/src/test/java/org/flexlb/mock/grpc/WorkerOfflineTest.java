package org.flexlb.mock.grpc;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import org.flexlb.balance.scheduler.RequestLifecycleState;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.engine.grpc.AbstractGrpcClient;
import org.flexlb.mock.FlexLBMockTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Outage keeps ambiguous ownership; the restored control owner must fence it before cleanup. */
class WorkerOfflineTest extends FlexLBMockTestBase {

    @Override
    protected FlexlbConfig createConfig() {
        FlexlbConfig cfg = new FlexlbConfig();
        cfg.setFlexlbBatchSizeMax(1);        // single request triggers immediate dispatch
        cfg.setFlexlbBatchWindowMs(300);
        cfg.setCostSloMs(50_000L);
        cfg.setCostSloRiskMarginMs(50L);
        cfg.setFlexlbBatchEnqueueDeadlineMs(5_000L);
        cfg.setFlexlbInflightTtlMs(300_000L);
        return cfg;
    }

    @Test
    @Timeout(30)
    void workerOffline_retainsOwnershipUntilRestoredControlOwnerFences() throws Exception {
        // 1. Submit request with normal worker — should succeed
        CompletableFuture<Response> future1 = submitRequest(20001);
        Response ackResponse = future1.get(5, TimeUnit.SECONDS);
        assertTrue(ackResponse.isSuccess(), "First request should succeed while worker is online");
        assertTrue(ackResponse.isEnqueuedByMaster(), "Should be enqueued by master");
        ManagedChannel enqueueChannel = batchEnqueueChannel();
        int existingBatches = getPrefillEndpoint().getInflightBatchCount();
        int existingDecodeReservations = getDecodeEndpoint().getInflightCount();

        // 2. Stop the mock prefill worker's gRPC server (simulates worker crash)
        mockPrefillWorker.stop();

        // Connection failure after RPC invocation is ambiguous, even when this
        // fixture knows that its socket was already closed.
        CompletableFuture<Response> future2 = submitRequest(20002);
        awaitCondition(() -> cancelAttempts.get() > 0, 8_000,
                "the failed real RPC must initiate Cancel reconciliation");
        assertFalse(future2.isDone(), "an unreachable Cancel owner cannot prove absence");
        assertEquals(existingBatches + 1, getPrefillEndpoint().getInflightBatchCount());
        assertEquals(existingDecodeReservations + 1, getDecodeEndpoint().getInflightCount());
        assertFalse(mockPrefillWorker.getRpcService().isAdmitted(20002));

        // Restart the same control owner and the same service state on the same
        // real gRPC endpoint. A retry now atomically fences the absent request.
        mockPrefillWorker.start(prefillGrpcPort);
        Response failResponse = future2.get(10, TimeUnit.SECONDS);

        assertFalse(failResponse.isSuccess(),
                "the fenced request must fail instead of being silently retried");
        assertEquals(StrategyErrorType.BATCH_SLO_EXPIRED.getErrorCode(), failResponse.getCode());
        assertEquals(RequestLifecycleState.TIMED_OUT, scheduler.getRequestState(20002L, 0).state());
        assertTrue(failResponse.getErrorMessage().contains("engine fenced late enqueue"));
        assertTrue(mockPrefillWorker.getRpcService().isAbsentFenced(20002));
        assertFalse(mockPrefillWorker.getRpcService().isAdmitted(20002));

        // 8. Verify: the failed request does not add leaked prefill inflight state.
        awaitCondition(() -> getPrefillEndpoint().getInflightBatchCount() == existingBatches
                        && getDecodeEndpoint().getInflightCount() == existingDecodeReservations,
                1_000, "only the now-fenced request's ledgers must be released");

        // 9. Verify: decode worker never received any enqueue request (PD-separated)
        assertEquals(0, mockDecodeWorker.getEnqueueCount(),
                "Decode worker should not have received any request");
        // Cancel and EnqueueBatch use independent pooled transports.
        awaitReady(enqueueChannel, 5_000);
        assertSame(enqueueChannel, batchEnqueueChannel(),
                "recovery must use the original EnqueueBatch channel");
        Response recovered = submitRequest(20003).get(5, TimeUnit.SECONDS);
        assertTrue(recovered.isSuccess(), () -> "new requests recover on the restored real channel: code="
                + recovered.getCode() + " error=" + recovered.getErrorMessage()
                + " channel=" + enqueueChannel.getState(false));
        assertTrue(mockPrefillWorker.getRpcService().isAdmitted(20003));
        assertFalse(mockPrefillWorker.getRpcService().isAbsentFenced(20003));
    }

    private ManagedChannel batchEnqueueChannel() throws ReflectiveOperationException {
        Field poolField = AbstractGrpcClient.class.getDeclaredField("channelPool");
        poolField.setAccessible(true);
        Map<?, ?> pool = (Map<?, ?>) poolField.get(grpcClient);
        String key = prefillIp + ":" + prefillGrpcPort + ":"
                + AbstractGrpcClient.ServiceType.BATCH_ENQUEUE.getSuffix();
        AbstractGrpcClient<?>.Invoker invoker = (AbstractGrpcClient<?>.Invoker) pool.get(key);
        assertNotNull(invoker, "the first real ACK must establish the EnqueueBatch channel");
        return invoker.getChannel();
    }

    private static void awaitReady(ManagedChannel channel, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        ConnectivityState state = channel.getState(true);
        while (state != ConnectivityState.READY) {
            assertFalse(state == ConnectivityState.SHUTDOWN, "the original EnqueueBatch channel shut down");
            long remaining = deadline - System.nanoTime();
            assertTrue(remaining > 0, () -> "EnqueueBatch channel did not reconnect: " + channel.getState(false));
            CountDownLatch transition = new CountDownLatch(1);
            channel.notifyWhenStateChanged(state, transition::countDown);
            transition.await(remaining, TimeUnit.NANOSECONDS);
            state = channel.getState(true);
        }
    }
}
