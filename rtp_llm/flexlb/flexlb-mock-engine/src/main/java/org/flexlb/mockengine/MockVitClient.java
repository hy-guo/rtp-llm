package org.flexlb.mockengine;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;

import org.flexlb.engine.grpc.EngineRpcService;
import org.flexlb.engine.grpc.MultimodalRpcServiceGrpc;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Prefill consumes the same image hashes used by frontend routing. Never called on D. */
final class MockVitClient implements AutoCloseable {
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private MockV41Image.Config v41;

    void configureV41(MockV41Image.Config config) { v41 = config; }

    record Prepared(EngineRpcService.GenerateInputPB input,
                    List<EngineRpcService.MultimodalCacheSpanPB> spans) {
        Prepared { spans = List.copyOf(spans); }
    }

    Prepared prepareRequest(EngineRpcService.GenerateInputPB input, long budgetMs) {
        if (!input.hasV41Inputs() && input.getMultimodalInputsCount() > 0
                && !input.hasMultimodalTokenLayout()) return prepareGeneric(input, budgetMs);
        return new Prepared(prepare(input, budgetMs), List.of());
    }

    private static final long CHUNK_BYTES = 32L << 20;
    private static final int CHUNK_ROWS = 8192;

    private Prepared prepareGeneric(EngineRpcService.GenerateInputPB input, long budgetMs) {
        if (v41 == null) return prepareResponse(input, budgetMs); // explicit metadata precondition
        int count = input.getMultimodalInputsCount();
        long imageBytes = (long) v41.maxTokens() * v41.hidden() * 2;
        int perChunk = (int) Math.max(1, Math.min(CHUNK_ROWS / v41.maxTokens(), CHUNK_BYTES / imageBytes));
        if (count <= perChunk) return prepareResponse(input, budgetMs);
        var placeholders = new ArrayList<Integer>();
        for (int i = 0; i < input.getTokenIdsCount(); i++)
            if (input.getTokenIds(i) == v41.imageTokenId()) placeholders.add(i);
        if (placeholders.size() != count) throw Status.INVALID_ARGUMENT
                .withDescription("image placeholder count does not match media inputs").asRuntimeException();
        var output = input.toBuilder().clearTokenIds();
        var spans = new ArrayList<EngineRpcService.MultimodalCacheSpanPB>();
        output.addAllTokenIds(input.getTokenIdsList().subList(0, placeholders.get(0)));
        MockVitChunks.run((count + perChunk - 1) / perChunk, budgetMs, (index, remaining) -> {
            int first = index * perChunk;
            int last = Math.min(count, first + perChunk);
            int begin = placeholders.get(first);
            int end = last == count ? input.getTokenIdsCount() : placeholders.get(last);
            var chunk = input.toBuilder().clearTokenIds().clearMultimodalInputs()
                    .addAllTokenIds(input.getTokenIdsList().subList(begin, end))
                    .addAllMultimodalInputs(input.getMultimodalInputsList().subList(first, last)).build();
            return prepareResponse(chunk, remaining);
        }, prepared -> {
            int offset = output.getTokenIdsCount();
            output.addAllTokenIds(prepared.input().getTokenIdsList());
            for (var span : prepared.spans())
                spans.add(span.toBuilder().setOffset(Math.addExact(offset, span.getOffset())).build());
            if (output.getTokenIdsCount() >= v41.maxSequence()) throw Status.INVALID_ARGUMENT
                    .withDescription("expanded multimodal input exceeds model sequence limit").asRuntimeException();
        });
        if (output.getTokenIdsCount() >= v41.maxSequence()) throw Status.INVALID_ARGUMENT
                .withDescription("expanded multimodal input exceeds model sequence limit").asRuntimeException();
        return new Prepared(output.build(), spans);
    }

    EngineRpcService.GenerateInputPB prepare(EngineRpcService.GenerateInputPB input, long budgetMs) {
        if (!input.hasV41Inputs() || input.getV41Inputs().getImagesCount() <= 1)
            return prepareOne(input, budgetMs);
        validateV41Layout(input);
        var images = input.getV41Inputs().getImagesList();
        long totalRows =
                images.stream().mapToLong(EngineRpcService.V41ImageInputPB::getTypesCount).sum();
        if (input.getSerializedSize() <= CHUNK_BYTES && totalRows <= CHUNK_ROWS)
            return prepareOne(input, budgetMs);
        var ranges = new ArrayList<int[]>();
        int first = 0;
        while (first < images.size()) {
            if (Context.current().isCancelled()) throw Status.CANCELLED.asRuntimeException();
            int last = first;
            long bytes = 0, rows = 0;
            while (last < images.size()) {
                var image = images.get(last);
                if (last > first
                        && (bytes + image.getSerializedSize() > CHUNK_BYTES
                                || rows + image.getTypesCount() > CHUNK_ROWS)) break;
                bytes += image.getSerializedSize();
                rows += image.getTypesCount();
                last++;
            }
            ranges.add(new int[] {first, last});
            first = last;
        }
        MockVitChunks.run(ranges.size(), budgetMs, (index, remaining) -> {
            int beginIndex = ranges.get(index)[0], last = ranges.get(index)[1];
            int begin = images.get(beginIndex).getStart();
            var finalImage = images.get(last - 1);
            int end = Math.addExact(finalImage.getStart(), finalImage.getTypesCount());
            var typed =
                    input.getV41Inputs().toBuilder()
                            .clearImages()
                            .clearTokenTypes()
                            .clearImageMask();
            typed.addAllTokenTypes(input.getV41Inputs().getTokenTypesList().subList(begin, end));
            typed.addAllImageMask(input.getV41Inputs().getImageMaskList().subList(begin, end));
            for (int i = beginIndex; i < last; i++)
                typed.addImages(
                        images.get(i).toBuilder().setStart(images.get(i).getStart() - begin));
            var chunk =
                    input.toBuilder()
                            .clearTokenIds()
                            .addAllTokenIds(input.getTokenIdsList().subList(begin, end))
                            .setV41Inputs(typed)
                            .build();
            // The original validation and feature checks run for every bounded response.
            return prepareOne(chunk, remaining);
        }, ignored -> {});
        // The scheduler/cache/EOS path receives the exact original canonical input.
        return input;
    }

    private EngineRpcService.GenerateInputPB prepareOne(
            EngineRpcService.GenerateInputPB input, long budgetMs) {
        return prepareResponse(input, budgetMs).input();
    }

    private Prepared prepareResponse(EngineRpcService.GenerateInputPB input, long budgetMs) {
        boolean typed = input.hasV41Inputs();
        boolean generic = !typed && input.getMultimodalInputsCount() > 0 && !input.hasMultimodalTokenLayout();
        if (generic && v41 == null) throw Status.FAILED_PRECONDITION
                .withDescription("URL-only V4.1 requests require matching --model-path metadata").asRuntimeException();
        if (typed) validateV41Layout(input);
        if (input.getMultimodalInputsCount() == 0 && (!typed || input.getV41Inputs().getImagesCount() == 0)) return new Prepared(input, List.of());
        var address = input.getGenerateConfig().getRoleAddrsList().stream()
                .filter(a -> a.getRoleStr().equals("VIT") || a.getRole() == EngineRpcService.RoleAddrPB.RoleType.VIT)
                .findFirst().orElseThrow(() -> Status.FAILED_PRECONDITION.withDescription("image request has no VIT role address").asRuntimeException());
        if (budgetMs <= 0) throw Status.DEADLINE_EXCEEDED.asRuntimeException();
        String target = address.getIp() + ":" + address.getGrpcPort();
        var channel =
                channels.computeIfAbsent(
                        target,
                        ignored ->
                                ManagedChannelBuilder.forAddress(
                                                address.getIp(), address.getGrpcPort())
                                        .usePlaintext()
                                        .maxInboundMessageSize(1024 * 1024 * 1024)
                                        .disableRetry()
                                        .build());
        var request = EngineRpcService.MultimodalInputsPB.newBuilder().setRequestId(input.getRequestId())
                .addAllMultimodalInputs(input.getMultimodalInputsList()).setTimeoutMs(Math.min(budgetMs, 120000));
        if (typed) request.setV41Inputs(input.getV41Inputs());
        long rpcBudgetMs = Math.min(budgetMs, 120000);
        var callerDeadline = Context.current().getDeadline();
        if (callerDeadline != null)
            rpcBudgetMs =
                    Math.min(rpcBudgetMs, callerDeadline.timeRemaining(TimeUnit.MILLISECONDS));
        if (rpcBudgetMs <= 0) throw Status.DEADLINE_EXCEEDED.asRuntimeException();
        if (Context.current().isCancelled()) throw Status.CANCELLED.asRuntimeException();
        var future =
                MultimodalRpcServiceGrpc.newFutureStub(channel)
                        .withDeadlineAfter(rpcBudgetMs, TimeUnit.MILLISECONDS)
                        .remoteMultimodalEmbedding(request.build());
        EngineRpcService.MultimodalOutputPB response;
        try {
            // A separate timed wait bounds callback occupancy even if transport
            // cancellation delivery or a blocking-stub wakeup stops progressing.
            response = future.get(rpcBudgetMs, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            future.cancel(true);
            throw Status.DEADLINE_EXCEEDED
                    .withDescription("Mock VIT response wait exceeded budget")
                    .asRuntimeException();
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw Status.CANCELLED
                    .withDescription("Mock VIT response wait interrupted")
                    .asRuntimeException();
        } catch (java.util.concurrent.ExecutionException e) {
            throw Status.fromThrowable(e.getCause())
                    .asRuntimeException(Status.trailersFromThrowable(e.getCause()));
        }
        if (Context.current().isCancelled()) throw Status.CANCELLED.asRuntimeException();
        var features = response.getMultimodalEmbedding();
        long rows = response.getSplitSizeList().stream().mapToLong(Integer::longValue).sum();
        int segments = typed ? input.getV41Inputs().getImagesCount() : input.getMultimodalInputsCount();
        boolean bf16 = features.getDataType() == EngineRpcService.TensorPB.DataType.BF16;
        var data = bf16 ? features.getBf16Data() : features.getFp32Data();
        if (response.getSplitSizeCount() != segments
                || response.getSplitSizeList().stream().anyMatch(size -> size <= 0)
                || features.getShapeCount() != 2 || features.getShape(0) != rows
                || features.getShape(1) <= 0
                || (typed || generic ? !bf16 : features.getDataType() != EngineRpcService.TensorPB.DataType.FP32)
                || rows * features.getShape(1) * (bf16 ? 2 : 4) != data.size())
            throw Status.DATA_LOSS.withDescription("invalid Mock ViT embedding shape or split sizes").asRuntimeException();
        if (typed) {
            for (int i = 0; i < segments; i++) if (response.getSplitSize(i) != input.getV41Inputs().getImages(i).getTypesCount())
                throw Status.DATA_LOSS.withDescription("V4.1 feature rows do not match canonical image span").asRuntimeException();
            return new Prepared(input, List.of()); // Prepared frontend IDs are already canonical.
        }
        byte[] tensor = data.toByteArray();
        int stride = Math.toIntExact(features.getShape(1) * (bf16 ? 2 : 4));
        var hashes = new ArrayList<Integer>();
        for (int i = 0; i < rows; i++) hashes.add(MockFeatureHash.row(tensor, i * stride, stride));
        if (response.hasMultimodalFeatureHash()) {
            var hashTensor = response.getMultimodalFeatureHash();
            if (hashTensor.getDataType() != EngineRpcService.TensorPB.DataType.INT32
                    || !hashTensor.getShapeList().equals(List.of(rows)))
                throw Status.DATA_LOSS.withDescription("invalid ViT hash tensor").asRuntimeException();
            var wire = hashTensor.getInt32Data().asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
            if (wire.remaining() != hashes.size() * 4) throw Status.DATA_LOSS.withDescription("invalid ViT hash size").asRuntimeException();
            for (int hash : hashes) if (wire.getInt() != hash)
                throw Status.DATA_LOSS.withDescription("ViT hash does not match embedding").asRuntimeException();
        }
        if (generic) return expandGeneric(input, response.getSplitSizeList(), hashes);
        if (input.getMultimodalTokenLayout().getSpansCount() != response.getSplitSizeCount())
            throw Status.FAILED_PRECONDITION.withDescription("frontend must provide expanded multimodal token layout").asRuntimeException();
        int cursor = 0, previousEnd = 0;
        for (int i = 0; i < response.getSplitSizeCount(); i++) {
            var span = input.getMultimodalTokenLayout().getSpans(i);
            if (span.getLength() != response.getSplitSize(i) || span.getOffset() < previousEnd
                    || (long)span.getOffset() + span.getLength() > input.getTokenIdsCount())
                throw Status.DATA_LOSS.withDescription("frontend/ViT image length mismatch").asRuntimeException();
            for (int j = 0; j < span.getLength(); j++) if (input.getTokenIds(span.getOffset() + j) != hashes.get(cursor++))
                throw Status.DATA_LOSS.withDescription("frontend/ViT image token identity mismatch").asRuntimeException();
            previousEnd = span.getOffset() + span.getLength();
        }
        return new Prepared(input, List.of());
    }

    private Prepared expandGeneric(EngineRpcService.GenerateInputPB input, List<Integer> sizes, List<Integer> hashes) {
        // MultimodalProcessor::expandTokenIds: semantic model/history IDs and
        // cache-only feature identity have different owners. Never put hashes in token_ids.
        var output = input.toBuilder().clearTokenIds();
        var spans = new ArrayList<EngineRpcService.MultimodalCacheSpanPB>();
        int image = 0, cursor = 0;
        for (int token : input.getTokenIdsList()) {
            if (token != v41.imageTokenId()) output.addTokenIds(token);
            else {
                if (image >= sizes.size()) throw Status.INVALID_ARGUMENT
                        .withDescription("more image placeholders than media inputs").asRuntimeException();
                int size = sizes.get(image++);
                spans.add(EngineRpcService.MultimodalCacheSpanPB.newBuilder()
                        .setOffset(output.getTokenIdsCount()).addAllRowHashes(hashes.subList(cursor, cursor + size)).build());
                cursor += size;
                for (int i = 0; i < size; i++) output.addTokenIds(v41.imageTokenId());
            }
            if (output.getTokenIdsCount() >= v41.maxSequence()) throw Status.INVALID_ARGUMENT
                    .withDescription("expanded multimodal input exceeds model sequence limit").asRuntimeException();
        }
        if (image != sizes.size()) throw Status.INVALID_ARGUMENT
                .withDescription("image placeholder count does not match media inputs").asRuntimeException();
        return new Prepared(output.build(), spans);
    }

    private static void validateV41Layout(EngineRpcService.GenerateInputPB input) {
        var typed = input.getV41Inputs();
        int n = input.getTokenIdsCount();
        if (typed.getSchemaVersion() != 1 || input.getMultimodalInputsCount() != 0
                || typed.getTokenTypesCount() != n || typed.getImageMaskCount() != n)
            throw Status.INVALID_ARGUMENT.withDescription("V4.1 token metadata must match input vector").asRuntimeException();
        int[] expected = new int[n];
        java.util.Arrays.fill(expected, -1);
        int end = 0;
        for (var image : typed.getImagesList()) {
            if (image.getStart() < end || image.getTypesCount() == 0
                    || (long)image.getStart() + image.getTypesCount() > n)
                throw Status.INVALID_ARGUMENT.withDescription("invalid V4.1 image span").asRuntimeException();
            for (int j = 0; j < image.getTypesCount(); j++) {
                int type = image.getTypes(j);
                if (type < 0 || type > 3) throw Status.INVALID_ARGUMENT.withDescription("invalid V4.1 image type").asRuntimeException();
                expected[image.getStart() + j] = type;
            }
            end = image.getStart() + image.getTypesCount();
        }
        for (int i = 0; i < n; i++) if (typed.getTokenTypes(i) != expected[i] || typed.getImageMask(i) != (expected[i] != -1))
            throw Status.INVALID_ARGUMENT.withDescription("V4.1 image spans do not cover token metadata").asRuntimeException();
    }

    int channelCount() { return channels.size(); }
    @Override public void close() {
        channels.values().forEach(ManagedChannel::shutdownNow);
        channels.clear();
    }
}
