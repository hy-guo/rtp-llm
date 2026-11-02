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

    @org.junit.jupiter.api.Test
    void genericChunksOverlapAndRetainTextImageOrderAndOffsets() throws Exception {
        var started = new java.util.concurrent.CountDownLatch(4);
        var calls = new AtomicInteger();
        var service = new MultimodalRpcServiceGrpc.MultimodalRpcServiceImplBase() {
            @Override public void remoteMultimodalEmbedding(EngineRpcService.MultimodalInputsPB r,
                    StreamObserver<EngineRpcService.MultimodalOutputPB> o) {
                int image = Integer.parseInt(r.getMultimodalInputs(0).getMultimodalUrl());
                calls.incrementAndGet();
                started.countDown();
                try {
                    check(started.await(5, java.util.concurrent.TimeUnit.SECONDS), "serial chunk RPCs");
                    int rows = 1 + image % 3;
                    byte[] data = new byte[rows * 4 * 2];
                    java.util.Arrays.fill(data, (byte) image);
                    o.onNext(EngineRpcService.MultimodalOutputPB.newBuilder().addSplitSize(rows)
                            .setMultimodalEmbedding(EngineRpcService.TensorPB.newBuilder()
                                    .setDataType(EngineRpcService.TensorPB.DataType.BF16)
                                    .addShape(rows).addShape(4).setBf16Data(ByteString.copyFrom(data)))
                            .build());
                    o.onCompleted();
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    o.onError(Status.CANCELLED.asRuntimeException());
                }
            }
        };
        var server = builder().addService(service).build().start();
        try (var client = new MockVitClient()) {
            client.configureV41(new MockV41Image.Config(4, 14, 1, 8192, 10000, ID, 196, 7));
            var input = EngineRpcService.GenerateInputPB.newBuilder().setRequestId(7).addTokenIds(100);
            input.getGenerateConfigBuilder().addRoleAddrs(EngineRpcService.RoleAddrPB.newBuilder()
                    .setRoleStr("VIT").setIp("127.0.0.1").setGrpcPort(server.getPort()));
            for (int i = 0; i < 9; i++) {
                input.addTokenIds(7).addTokenIds(200 + i);
                input.addMultimodalInputs(EngineRpcService.MultimodalInputPB.newBuilder().setMultimodalUrl("" + i));
            }
            var result = client.prepareRequest(input.build(), 15000);
            var expected = new java.util.ArrayList<Integer>();
            expected.add(100);
            for (int i = 0; i < 9; i++) {
                int rows = 1 + i % 3;
                check(result.spans().get(i).getOffset() == expected.size(), "image offset/order changed");
                check(result.spans().get(i).getRowHashesCount() == rows, "image rows changed");
                byte[] row = new byte[8]; java.util.Arrays.fill(row, (byte) i);
                int hash = MockFeatureHash.row(row, 0, row.length);
                for (int value : result.spans().get(i).getRowHashesList()) check(value == hash, "image identity/order changed");
                for (int j = 0; j < rows; j++) expected.add(7);
                expected.add(200 + i);
            }
            check(result.input().getTokenIdsList().equals(expected), "text or image order changed");
            check(calls.get() == 9, "missing or duplicate chunk RPC");
        } finally { server.shutdownNow().awaitTermination(); }
    }
}
