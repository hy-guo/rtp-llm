package org.flexlb.mock.grpc;

import org.flexlb.balance.scheduler.priority.EngineCancelChannel;
import org.flexlb.balance.scheduler.priority.GrpcEngineCancelChannel;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.mock.FlexLBMockTestBase;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The admission-wins counterpart of GrpcTimeoutTest, using both real gRPC RPCs. */
class CancelFenceTest extends FlexLBMockTestBase {

    @Test
    void admittedRequestCannotBeReportedAsAbsent() throws Exception {
        Response response = submitRequest(30001).get(5, TimeUnit.SECONDS);
        assertTrue(response.isSuccess());
        assertTrue(mockPrefillWorker.getRpcService().isAdmitted(30001));

        EngineCancelChannel channel = new GrpcEngineCancelChannel(grpcClient);
        EngineCancelChannel.CancelTarget target =
                new EngineCancelChannel.CancelTarget(prefillIp, prefillGrpcPort);
        EngineCancelChannel.CancelOutcome outcome = channel.cancel(target, 30001, 1_000)
                .get(2, TimeUnit.SECONDS);
        assertEquals(EngineCancelChannel.CancelAck.NOT_FOUND, outcome.ack(),
                "this fixture cannot forge an absent fence for an already-admitted request");
        assertFalse(mockPrefillWorker.getRpcService().isAbsentFenced(30001));
        assertEquals(1, getPrefillEndpoint().getInflightBatchCount());
        assertEquals(1, getDecodeEndpoint().getInflightCount());
        assertEquals(1, mockPrefillWorker.getEnqueueCount());
        assertEquals(1L, mockPrefillWorker.getRpcService().getCancelCallCount());
    }
}
