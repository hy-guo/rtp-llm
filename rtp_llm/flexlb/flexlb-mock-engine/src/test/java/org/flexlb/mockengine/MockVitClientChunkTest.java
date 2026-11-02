package org.flexlb.mockengine;

import com.google.protobuf.ByteString;

import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.flexlb.engine.grpc.EngineRpcService;
import org.flexlb.engine.grpc.MultimodalRpcServiceGrpc;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Tests real serialization, validation, pixel computation and gRPC on loopback only. */
class MockVitClientChunkTest {
    static final String ID = "0".repeat(64);

    static void check(boolean b, String s) {
        if (!b) throw new AssertionError(s);
    }

    static ServerBuilder<?> builder() throws Exception {
        Class<?> c;
        try {
            c = Class.forName("io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder");
        } catch (ClassNotFoundException e) {
            c = Class.forName("io.grpc.netty.NettyServerBuilder");
        }
        return (ServerBuilder<?>)
                c.getMethod("forAddress", java.net.SocketAddress.class)
                        .invoke(null, new InetSocketAddress("127.0.0.1", 0));
    }

    static void run(int ratio, int height, int width, int count, boolean baseline)
            throws Exception {
        AtomicInteger calls = new AtomicInteger(), images = new AtomicInteger();
        var model = new MockV41Image.Config(4, 14, ratio, 10000, 1_000_000, ID);
        var cfg = new MockVitService.Config(4, 1, 64, 0, 0, 32 << 20, 512L << 20, 0);
        var pixels = ByteString.copyFrom(new byte[height * width * 3 * 14 * 14 * 2]);
        var delegate = new MockVitService(cfg, model);
        var service =
                new MultimodalRpcServiceGrpc.MultimodalRpcServiceImplBase() {
                    @Override
                    public void remoteMultimodalEmbedding(
                            EngineRpcService.MultimodalInputsPB r,
                            StreamObserver<EngineRpcService.MultimodalOutputPB> o) {
                        calls.incrementAndGet();
                        images.addAndGet(r.getV41Inputs().getImagesCount());
                        for (var im : r.getV41Inputs().getImagesList())
                            check(
                                    im.getPatches().getBf16Data().equals(pixels),
                                    "pixel bytes changed");
                        delegate.remoteMultimodalEmbedding(r, o);
                    }
                };
        var server = builder().maxInboundMessageSize(32 << 20).addService(service).build().start();
        try (var client = new MockVitClient()) {
            var input = EngineRpcService.GenerateInputPB.newBuilder().setRequestId(20261101);
            input.getGenerateConfigBuilder()
                    .addRoleAddrs(
                            EngineRpcService.RoleAddrPB.newBuilder()
                                    .setRoleStr("VIT")
                                    .setRole(EngineRpcService.RoleAddrPB.RoleType.VIT)
                                    .setIp("127.0.0.1")
                                    .setGrpcPort(server.getPort()));
            var typed = EngineRpcService.V41RequestInputsPB.newBuilder().setSchemaVersion(1);
            var types = MockV41Image.types(height, width, ratio);
            for (int i = 0; i < count; i++) {
                for (int j = 0; j < 3; j++) {
                    input.addTokenIds(17);
                    typed.addTokenTypes(-1).addImageMask(false);
                }
                int start = input.getTokenIdsCount();
                typed.addImages(
                        EngineRpcService.V41ImageInputPB.newBuilder()
                                .setStart(start)
                                .setNVitH(height)
                                .setNVitW(width)
                                .setProcessorIdentity(ID)
                                .setContentSha256(String.format("%064x", i + 1))
                                .addAllTypes(types)
                                .setPatches(
                                        EngineRpcService.TensorPB.newBuilder()
                                                .setDataType(
                                                        EngineRpcService.TensorPB.DataType.BF16)
                                                .addAllShape(
                                                        List.of(
                                                                (long) height * width,
                                                                3L,
                                                                14L,
                                                                14L))
                                                .setBf16Data(pixels)));
                for (int type : types) {
                    input.addTokenIds(12345 + i);
                    typed.addTokenTypes(type).addImageMask(true);
                }
            }
            var original = input.setV41Inputs(typed).build();
            try {
                var result = client.prepare(original, 30000);
                check(
                        !baseline || original.getSerializedSize() < (32 << 20),
                        "baseline unexpectedly accepted oversize");
                check(
                        result == original && result.equals(original),
                        "canonical input was replaced");
                check(images.get() == count, "missing or duplicated image");
                if (!baseline
                        && (original.getSerializedSize() > (32 << 20)
                                || (long) types.size() * count > 8192))
                    check(calls.get() > 1, "large request was not chunked");
                else check(calls.get() == 1, "small request split unnecessarily");
                int before = calls.get();
                var malformed =
                        original.toBuilder()
                                .setV41Inputs(typed.clone().setTokenTypes(0, 0))
                                .build();
                try {
                    client.prepare(malformed, 30000);
                    throw new AssertionError("invalid layout accepted");
                } catch (StatusRuntimeException e) {
                    check(
                            e.getStatus().getCode() == Status.Code.INVALID_ARGUMENT,
                            "wrong validation status");
                }
                check(calls.get() == before, "invalid layout reached server");
                try {
                    client.prepare(original, 0);
                    throw new AssertionError("expired request accepted");
                } catch (StatusRuntimeException e) {
                    check(
                            e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED,
                            "wrong deadline status");
                }
                System.out.println(
                        "PASS ratio="
                                + ratio
                                + " images="
                                + count
                                + " bytes="
                                + original.getSerializedSize()
                                + " calls="
                                + calls
                                + " exact canonical input, pixels, validation and deadline");
            } catch (StatusRuntimeException e) {
                if (!baseline
                        || original.getSerializedSize() < (32 << 20)
                        || e.getStatus().getCode() != Status.Code.RESOURCE_EXHAUSTED) throw e;
                System.out.println(
                        "PASS baseline reproduces RESOURCE_EXHAUSTED bytes="
                                + original.getSerializedSize());
            }
        } finally {
            server.shutdownNow().awaitTermination();
            delegate.close();
        }
    }

    @org.junit.jupiter.api.Test
    void chunksLargeRequestsAndPreservesCanonicalImagesAndDeadline() throws Exception {
        run(4, 128, 40, 16, false);
        run(4, 16, 16, 2, false);
        run(1, 64, 64, 3, false);
    }
}
