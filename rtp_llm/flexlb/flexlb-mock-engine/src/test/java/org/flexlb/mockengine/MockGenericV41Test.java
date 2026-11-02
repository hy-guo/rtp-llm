package org.flexlb.mockengine;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;

import org.flexlb.engine.grpc.EngineRpcService;
import org.flexlb.engine.grpc.MultimodalRpcServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Generic URL frontend -> model-specific ViT -> semantic IDs + cache sidecar. */
class MockGenericV41Test {
    static final MockV41Image.Config MODEL = MockV41ImageTest.config();
    private MockVitService service;
    private Server server;
    private ManagedChannel channel;

    private MultimodalRpcServiceGrpc.MultimodalRpcServiceBlockingStub start() throws Exception {
        service = new MockVitService(MockVitService.Config.defaults(), MODEL);
        server = NettyServerBuilder.forPort(0).addService(service).build().start();
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
        return MultimodalRpcServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
    }

    @AfterEach void close() throws Exception {
        if (channel != null) channel.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
        if (server != null) server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
        if (service != null) service.close();
    }

    @Test void gridMatchesBackendResizeIncludingExtremeAspectRatios() {
        // Values from deepseek_v41_processor.plan_image_grid, not Qwen resize.
        int[][] cases = {{47, 31, 32, 48, 189}, {224, 224, 39, 39, 184},
                {1920, 1080, 69, 122, 968}, {1080, 1920, 122, 69, 986},
                {1, 10000, 1533, 3, 1024}, {10000, 1, 3, 3063, 1024},
                {8192, 8192, 93, 93, 994}, {42, 84, 55, 28, 211}};
        for (var value : cases) {
            var grid = MockV41Image.grid(value[0], value[1], MODEL);
            assertArrayEquals(new int[] {value[2], value[3]}, grid);
            assertEquals(value[4], MockV41Image.types(grid[0], grid[1], MODEL.ratio()).size());
        }
    }

    @Test void genericHashProbeAndEmbeddingAgreeAndUseBf16ModelWidth() throws Exception {
        var stub = start();
        var image = MockVitServiceTest.image(47, 31, 0xabcdef);
        var query = EngineRpcService.MultimodalHashRequestPB.newBuilder().addKeys(MockVitImage.key(image));
        assertFalse(stub.getMultimodalHashes(query.build()).getEntries(0).getHashHit());
        var metadata = stub.getMultimodalHashes(query.setInputs(MockVitServiceTest.inputs(image)).build());
        var result = stub.remoteMultimodalEmbedding(MockVitServiceTest.inputs(image));
        assertEquals(List.of(189), result.getSplitSizeList());
        assertEquals(List.of(189L, 8L), result.getMultimodalEmbedding().getShapeList());
        assertEquals(EngineRpcService.TensorPB.DataType.BF16, result.getMultimodalEmbedding().getDataType());
        assertEquals(result.getMultimodalFeatureHash().getInt32Data(), metadata.getEntries(0).getFeatureHashes());
        assertEquals(1, service.computations.get());
        var status = stub.getWorkerStatus(EngineRpcService.StatusVersionPB.getDefaultInstance());
        assertTrue(status.hasMmInflightInputs());
        assertEquals(0, status.getMmInflightInputs().getValue());
    }

    static EngineRpcService.GenerateInputPB input(int port, EngineRpcService.MultimodalInputPB... images) {
        var input = EngineRpcService.GenerateInputPB.newBuilder().setRequestId(91).addTokenIds(42);
        for (var image : images) input.addTokenIds(MODEL.imageTokenId()).addTokenIds(43).addMultimodalInputs(image);
        return input.setGenerateConfig(EngineRpcService.GenerateConfigPB.newBuilder().setMaxNewTokens(4)
                .addRoleAddrs(EngineRpcService.RoleAddrPB.newBuilder().setRole(EngineRpcService.RoleAddrPB.RoleType.VIT)
                        .setIp("127.0.0.1").setGrpcPort(port))).build();
    }

    @Test void semanticTokensStayCanonicalWhileDifferentImagesChangeCacheIdentity() throws Exception {
        start();
        var a = MockVitServiceTest.image(47, 31, 0xaaaaaa);
        var b = MockVitServiceTest.image(47, 31, 0xbbbbbb);
        try (var client = new MockVitClient()) {
            client.configureV41(MODEL);
            var first = client.prepareRequest(input(server.getPort(), a), 3000);
            var second = client.prepareRequest(input(server.getPort(), b), 3000);
            assertEquals(first.input().getTokenIdsList(), second.input().getTokenIdsList());
            assertEquals(191, first.input().getTokenIdsCount());
            assertFalse(first.input().hasV41Inputs());
            assertFalse(first.input().hasMultimodalTokenLayout());
            assertEquals(1, first.spans().get(0).getOffset());
            assertEquals(189, first.spans().get(0).getRowHashesCount());
            assertNotEquals(first.spans(), second.spans());
            var keys = MockImageCacheKeys.fullBlocks(first.input(), 16, MockImageCacheKeys.v41Seed(), first.spans());
            assertFalse(keys.isEmpty());
            assertNotEquals(keys, MockImageCacheKeys.fullBlocks(second.input(), 16, MockImageCacheKeys.v41Seed(), second.spans()));
            // An independent transcription of CompleteTokenIds's -42 domain and overlap coordinates.
            var expected = new ArrayList<Long>();
            long hash = MockImageCacheKeys.v41Seed();
            for (int begin = 0; begin + 16 <= 191; begin += 16) {
                for (int i = begin; i < begin + 16; i++) hash = combine(hash, first.input().getTokenIds(i));
                int lo = Math.max(begin, 1), hi = Math.min(begin + 16, 190);
                for (int word : new int[] {-42, lo - begin, 189, lo - 1, hi - lo}) hash = combine(hash, word);
                for (int i = lo - 1; i < hi - 1; i++) hash = combine(hash, first.spans().get(0).getRowHashes(i));
                expected.add(hash);
            }
            assertEquals(expected, keys);
        }
    }

    private static long combine(long hash, int word) {
        return hash ^ ((long) word + 0x9e3779b97f4a7c15L + (hash << 12) + (hash >> 32));
    }

    @Test void placeholderMismatchAndMissingModelFailExplicitly() throws Exception {
        start();
        var request = input(server.getPort(), MockVitServiceTest.image(47, 31, 1));
        try (var client = new MockVitClient()) {
            assertEquals(Status.Code.FAILED_PRECONDITION, assertThrows(StatusRuntimeException.class,
                    () -> client.prepareRequest(request, 3000)).getStatus().getCode());
            client.configureV41(MODEL);
            var missing = request.toBuilder().clearTokenIds().addTokenIds(42).build();
            assertEquals(Status.Code.INVALID_ARGUMENT, assertThrows(StatusRuntimeException.class,
                    () -> client.prepareRequest(missing, 3000)).getStatus().getCode());
            var extra = request.toBuilder().addTokenIds(MODEL.imageTokenId()).build();
            assertEquals(Status.Code.INVALID_ARGUMENT, assertThrows(StatusRuntimeException.class,
                    () -> client.prepareRequest(extra, 3000)).getStatus().getCode());
        }
    }

    @Test void genericChunksPreserveTextOrderAndGlobalSidecarOffsets() throws Exception {
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        service = new MockVitService(MockVitService.Config.defaults(), MODEL);
        server = NettyServerBuilder.forPort(0).addService(io.grpc.ServerInterceptors.intercept(service,
                new io.grpc.ServerInterceptor() {
                    @Override public <R, S> io.grpc.ServerCall.Listener<R> interceptCall(io.grpc.ServerCall<R, S> call,
                            io.grpc.Metadata headers, io.grpc.ServerCallHandler<R, S> next) {
                        if (call.getMethodDescriptor().getBareMethodName().equals("RemoteMultimodalEmbedding")) calls.incrementAndGet();
                        return next.startCall(call, headers);
                    }
                })).build().start();
        var images = new EngineRpcService.MultimodalInputPB[9];
        for (int i = 0; i < images.length; i++) images[i] = MockVitServiceTest.image(47, 31, i * 1000);
        try (var client = new MockVitClient()) {
            client.configureV41(MODEL);
            var original = input(server.getPort(), images);
            var prepared = client.prepareRequest(original, 5000);
            assertEquals(2, calls.get(), "8192-row bound uses model max image tokens before fetching features");
            assertEquals(9, prepared.spans().size());
            assertEquals(1 + 9 * 190, prepared.input().getTokenIdsCount());
            assertEquals(42, prepared.input().getTokenIds(0));
            for (int i = 0; i < 9; i++) {
                assertEquals(1 + i * 190, prepared.spans().get(i).getOffset());
                assertEquals(189, prepared.spans().get(i).getRowHashesCount());
                assertEquals(43, prepared.input().getTokenIds((i + 1) * 190));
            }
            assertEquals(original.getMultimodalInputsList(), prepared.input().getMultimodalInputsList());
        }
    }

    @Test void field28CountsDeduplicatedMediaUntilPublication() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var bytes = java.util.Base64.getDecoder().decode(MockVitServiceTest.image(47, 31, 1)
                .getMultimodalUrl().split(",", 2)[1]);
        var http = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 8);
        http.createContext("/image", exchange -> {
            entered.countDown();
            try { assertTrue(release.await(5, TimeUnit.SECONDS)); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new java.io.IOException(e); }
            exchange.sendResponseHeaders(200, bytes.length);
            try (var out = exchange.getResponseBody()) { out.write(bytes); }
        });
        http.start();
        try {
            var stub = start();
            var request = MockVitServiceTest.inputs(EngineRpcService.MultimodalInputPB.newBuilder()
                    .setMultimodalType(1).setMultimodalUrl("http://127.0.0.1:" + http.getAddress().getPort() + "/image").build());
            var future = MultimodalRpcServiceGrpc.newFutureStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
            var first = future.remoteMultimodalEmbedding(request);
            assertTrue(entered.await(3, TimeUnit.SECONDS));
            var second = future.remoteMultimodalEmbedding(request);
            var status = stub.getWorkerStatus(EngineRpcService.StatusVersionPB.getDefaultInstance());
            assertTrue(status.hasMmInflightInputs());
            assertEquals(1, status.getMmInflightInputs().getValue());
            release.countDown();
            assertEquals(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS));
        } finally { release.countDown(); http.stop(0); }
    }
}
