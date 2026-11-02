package org.flexlb.mockengine;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.flexlb.engine.grpc.EngineRpcService;
import org.flexlb.engine.grpc.MultimodalRpcServiceGrpc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Main ViT wire contract with deterministic forward, byte-bounded caches and shared computation. */
public final class MockVitService extends MultimodalRpcServiceGrpc.MultimodalRpcServiceImplBase implements AutoCloseable {
    public record Config(int hiddenSize, int concurrency, int queueCapacity, long embeddingCacheBytes,
                         long hashCacheBytes, int maxInputBytes, long maxOutputBytes, long computeDelayMs) {
        public Config {
            if (hiddenSize <= 0 || concurrency <= 0 || queueCapacity <= 0 || embeddingCacheBytes < 0
                    || hashCacheBytes < 0 || maxInputBytes <= 0 || maxOutputBytes <= 0 || computeDelayMs < 0)
                throw new IllegalArgumentException("invalid Mock ViT limits");
        }
        public static Config defaults() {
            return new Config(64, 4, 64, 64L << 20, 8L << 20, 16 << 20, 64L << 20, 1);
        }
    }
    private record HashEntry(EngineRpcService.TensorPB hashes, int rows, String generation) {
        long bytes(String key) { return hashes.getInt32Data().size() + key.length() * 2L + 128; }
    }
    final Config config;
    private final MockV41Image.Config v41;
    final String instance = UUID.randomUUID().toString();
    private final ThreadPoolExecutor executor;
    private final Map<String, Work> pending = new ConcurrentHashMap<>();
    @FunctionalInterface interface Forward {
        MockVitImage.Result compute(long remainingMs) throws Exception;
    }
    private static final class Work {
        final CompletableFuture<MockVitImage.Result> result = new CompletableFuture<>();
        final AtomicLong deadline;
        Work(long deadline) { this.deadline = new AtomicLong(deadline); }
        long remainingMs() {
            long nanos = deadline.get() - System.nanoTime();
            if (nanos <= 0) throw Status.DEADLINE_EXCEEDED
                    .withDescription("Mock ViT budget exhausted in forward queue").asRuntimeException();
            return Math.max(1, TimeUnit.NANOSECONDS.toMillis(nanos));
        }
    }
    private final LinkedHashMap<String, MockVitImage.Result> embeddings = new LinkedHashMap<>(16, .75f, true);
    private final LinkedHashMap<String, HashEntry> hashes = new LinkedHashMap<>(16, .75f, true);
    private long embeddingBytes, hashBytes;
    private volatile boolean closed;
    final AtomicLong computations = new AtomicLong();

    public MockVitService(Config config) {
        this(config, null);
    }

    MockVitService(Config config, MockV41Image.Config v41) {
        this.config = config;
        this.v41 = v41;
        executor = new ThreadPoolExecutor(config.concurrency(), config.concurrency(), 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.queueCapacity()), task -> {
                    Thread thread = new Thread(task, "mock-vit-forward"); thread.setDaemon(true); return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    private synchronized MockVitImage.Result cached(String key) { return embeddings.get(key); }
    private synchronized HashEntry hash(String key) { return hashes.get(key); }
    private synchronized void remember(String key, MockVitImage.Result result) {
        if (closed || key.startsWith("uncached:")) return;
        long bytes = result.embeddingBytes() + result.hashBytes() + key.length() * 2L + 128;
        if (bytes <= config.embeddingCacheBytes()) {
            var old = embeddings.put(key, result);
            if (old != null) embeddingBytes -= old.embeddingBytes() + old.hashBytes() + key.length() * 2L + 128;
            embeddingBytes += bytes;
            while (embeddingBytes > config.embeddingCacheBytes()) {
                var victim = embeddings.entrySet().iterator().next();
                embeddingBytes -= victim.getValue().embeddingBytes() + victim.getValue().hashBytes()
                        + victim.getKey().length() * 2L + 128;
                embeddings.remove(victim.getKey());
            }
        }
        var previousEntry = hashes.get(key);
        var entry = new HashEntry(result.hashes(), result.rows(),
                previousEntry == null ? UUID.randomUUID().toString() : previousEntry.generation());
        long charge = entry.bytes(key);
        var previous = hashes.remove(key);
        if (previous != null) hashBytes -= previous.bytes(key);
        if (charge <= config.hashCacheBytes()) {
            hashes.put(key, entry); hashBytes += charge;
            while (hashBytes > config.hashCacheBytes()) {
                var victim = hashes.entrySet().iterator().next();
                hashBytes -= victim.getValue().bytes(victim.getKey()); hashes.remove(victim.getKey());
            }
        }
    }

    CompletableFuture<MockVitImage.Result> acquire(EngineRpcService.MultimodalInputPB image, long timeoutMs) {
        return acquire(MockVitImage.key(image), timeoutMs, remaining -> v41 == null
                ? MockVitImage.compute(image, config.hiddenSize(), config.maxInputBytes(), config.maxOutputBytes(), remaining)
                : MockV41Image.compute(image, v41, config.maxInputBytes(), config.maxOutputBytes(), remaining));
    }

    CompletableFuture<MockVitImage.Result> acquire(String key, long timeoutMs, Forward forward) {
        if (closed) return CompletableFuture.failedFuture(Status.UNAVAILABLE.asRuntimeException());
        if (timeoutMs <= 0) return CompletableFuture.failedFuture(Status.DEADLINE_EXCEEDED.asRuntimeException());
        var result = cached(key);
        if (result != null) {
            if (hash(key) == null) remember(key, result);
            return CompletableFuture.completedFuture(result);
        }
        long deadline = System.nanoTime() + Math.min(TimeUnit.MILLISECONDS.toNanos(timeoutMs), Long.MAX_VALUE / 2);
        var created = new Work(deadline);
        var existing = pending.putIfAbsent(key, created);
        if (existing != null) {
            // A short-lived waiter must not expire shared work needed by a live
            // caller. Waiter cancellation still affects only respond(), not Work.
            existing.deadline.accumulateAndGet(deadline, Math::max);
            return existing.result;
        }
        try {
            executor.execute(() -> {
                try {
                    // Like MMWorkItem.remaining_timeout_s(), queue residence is
                    // part of the absolute producer budget, not a fresh I/O budget.
                    created.remainingMs();
                    var resident = cached(key);
                    if (resident != null) { created.result.complete(resident); return; }
                    if (config.computeDelayMs() > 0) Thread.sleep(config.computeDelayMs());
                    var computed = forward.compute(created.remainingMs());
                    computations.incrementAndGet(); remember(key, computed); created.result.complete(computed);
                } catch (Throwable error) { created.result.completeExceptionally(error); }
                finally { pending.remove(key, created); }
            });
        } catch (RejectedExecutionException error) {
            pending.remove(key, created); created.result.completeExceptionally(Status.RESOURCE_EXHAUSTED
                    .withDescription("Mock ViT forward queue is full").asRuntimeException());
        }
        return created.result;
    }

    static long timeout(Context context, long requested) {
        long remaining = context.getDeadline() == null ? 120000 : context.getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
        return Math.min(requested > 0 ? requested : 120000, remaining);
    }
    private static Throwable cause(Throwable error) {
        while (error instanceof CompletionException && error.getCause() != null) error = error.getCause();
        return error;
    }
    private static <T> void respond(CompletableFuture<T> work, StreamObserver<T> observer, long budget) {
        Context context = Context.current();
        var waiting = new CompletableFuture<T>();
        Context.CancellationListener cancelled = ignored -> waiting.completeExceptionally(
                Status.CANCELLED.withDescription("ViT caller cancelled").asRuntimeException());
        context.addListener(cancelled, Runnable::run);
        if (budget <= 0) waiting.completeExceptionally(Status.DEADLINE_EXCEEDED.asRuntimeException());
        else waiting.orTimeout(budget, TimeUnit.MILLISECONDS);
        work.whenComplete((value, error) -> {
            if (error == null) waiting.complete(value); else waiting.completeExceptionally(error);
        });
        waiting.whenComplete((value, error) -> {
            context.removeListener(cancelled);
            if (context.isCancelled()) return;
            if (error == null) { observer.onNext(value); observer.onCompleted(); }
            else {
                Throwable root = cause(error);
                Status status = root instanceof java.util.concurrent.TimeoutException ? Status.DEADLINE_EXCEEDED
                        : root instanceof IllegalArgumentException ? Status.INVALID_ARGUMENT
                        : Status.fromThrowable(root);
                if (status.getCode() == Status.Code.UNKNOWN) status = Status.INTERNAL;
                observer.onError(status.withDescription(root.getMessage()).asRuntimeException());
            }
        });
    }

    private static void validate(EngineRpcService.MultimodalInputsPB inputs) {
        if (inputs.hasV41Inputs()) throw new IllegalArgumentException("packed V4.1 images require a model-specific Mock ViT backend");
        if (inputs.getMultimodalInputsCount() == 0 || inputs.getMultimodalInputsCount() > 256)
            throw new IllegalArgumentException("expected 1..256 image inputs");
        for (var input : inputs.getMultimodalInputsList())
            if (input.getMultimodalUrl().isBlank() || input.getMultimodalType() != 1)
                throw new IllegalArgumentException("Mock ViT accepts URL IMAGE inputs only");
    }
    private List<CompletableFuture<MockVitImage.Result>> submitAll(
            EngineRpcService.MultimodalInputsPB inputs, long budget) {
        List<CompletableFuture<MockVitImage.Result>> values;
        if (inputs.hasV41Inputs()) {
            var work = MockV41Image.validate(inputs, v41, config.maxInputBytes());
            values = work.stream().map(item -> acquire(item.key(), budget,
                    remaining -> MockV41Image.compute(item.image(), v41, config.maxOutputBytes()))).toList();
        } else {
            validate(inputs);
            values = inputs.getMultimodalInputsList().stream().map(image -> acquire(image, budget)).toList();
        }
        return values;
    }
    private CompletableFuture<List<MockVitImage.Result>> acquireAll(EngineRpcService.MultimodalInputsPB inputs, long budget) {
        var values = submitAll(inputs, budget);
        return CompletableFuture.allOf(values.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> values.stream().map(CompletableFuture::join).toList());
    }

    @Override public void remoteMultimodalEmbedding(EngineRpcService.MultimodalInputsPB inputs,
                                                   StreamObserver<EngineRpcService.MultimodalOutputPB> observer) {
        long budget = timeout(Context.current(), inputs.getTimeoutMs());
        try { respond(acquireAll(inputs, budget).thenApply(this::output), observer, budget); }
        catch (IllegalArgumentException error) { observer.onError(Status.INVALID_ARGUMENT.withDescription(error.getMessage()).asRuntimeException()); }
    }
    private EngineRpcService.MultimodalOutputPB output(List<MockVitImage.Result> results) {
        ByteString data = ByteString.EMPTY, ids = ByteString.EMPTY;
        var output = EngineRpcService.MultimodalOutputPB.newBuilder();
        int rows = 0;
        boolean bf16 = !results.isEmpty() && results.get(0).embedding().getDataType() == EngineRpcService.TensorPB.DataType.BF16;
        for (var result : results) {
            if ((long)data.size() + result.embeddingBytes() > config.maxOutputBytes())
                throw new IllegalArgumentException("aggregate embedding exceeds output byte limit");
            // Immutable ropes keep the same wire bytes without flattening every
            // image and then copying the whole response another two times.
            data = data.concat(bf16 ? result.embedding().getBf16Data() : result.embedding().getFp32Data());
            ids = ids.concat(result.hashes().getInt32Data());
            output.addSplitSize(result.rows()); rows = Math.addExact(rows, result.rows());
        }
        var tensor = EngineRpcService.TensorPB.newBuilder().addShape(rows)
                .addShape(bf16 ? v41.hidden() : config.hiddenSize());
        if (bf16) tensor.setDataType(EngineRpcService.TensorPB.DataType.BF16).setBf16Data(data);
        else tensor.setDataType(EngineRpcService.TensorPB.DataType.FP32).setFp32Data(data);
        return output.setMultimodalEmbedding(tensor)
                .setMultimodalFeatureHash(EngineRpcService.TensorPB.newBuilder()
                        .setDataType(EngineRpcService.TensorPB.DataType.INT32).addShape(rows)
                        .setInt32Data(ids)).build();
    }

    @Override public void getMultimodalHashes(EngineRpcService.MultimodalHashRequestPB request,
                                             StreamObserver<EngineRpcService.MultimodalHashResponsePB> observer) {
        long budget = timeout(Context.current(), request.getTimeoutMs());
        try {
            if (request.getKeysCount() > 256 || request.getKeysList().stream().anyMatch(k -> k.isBlank() || k.length() > 4096))
                throw new IllegalArgumentException("invalid multimodal cache keys");
            var keys = new LinkedHashSet<>(request.getKeysList());
            Map<String, HashEntry> transientResults = new ConcurrentHashMap<>();
            List<CompletableFuture<?>> work = new ArrayList<>();
            if (request.hasInputs()) {
                validate(request.getInputs());
                Map<String, EngineRpcService.MultimodalInputPB> submitted = new LinkedHashMap<>();
                for (var image : request.getInputs().getMultimodalInputsList()) {
                    String key = MockVitImage.key(image);
                    if (!keys.contains(key)) throw new IllegalArgumentException("image does not match requested cache keys");
                    submitted.put(key, image);
                }
                for (String key : keys) if (hash(key) == null) {
                    var image = submitted.get(key);
                    if (image == null) throw new IllegalArgumentException("missing image for uncached key");
                    work.add(acquire(image, budget).thenAccept(value -> transientResults.put(key,
                            new HashEntry(value.hashes(), value.rows(), ""))));
                }
            }
            respond(CompletableFuture.allOf(work.toArray(CompletableFuture[]::new)).thenApply(ignored -> {
                var response = EngineRpcService.MultimodalHashResponsePB.newBuilder().setWorkerInstance(instance);
                for (String key : keys) {
                    var entry = response.addEntriesBuilder().setKey(key);
                    HashEntry found = hash(key);
                    var computed = transientResults.get(key);
                    if (found != null || computed != null) {
                        entry.setHashHit(true).setEntryGeneration(found == null ? "" : found.generation())
                                .addSplitSize(found == null ? computed.rows() : found.rows())
                                .setFeatureHashes(found == null ? computed.hashes().getInt32Data() : found.hashes().getInt32Data());
                    }
                    if (cached(key) != null) entry.setEmbeddingHit(true).setEmbeddingTier("cpu");
                }
                return response.build();
            }), observer, budget);
        } catch (IllegalArgumentException error) { observer.onError(Status.INVALID_ARGUMENT.withDescription(error.getMessage()).asRuntimeException()); }
    }

    @Override public void asyncSubmitEmbedding(EngineRpcService.MultimodalInputsPB inputs,
                                               StreamObserver<EngineRpcService.EmptyPB> observer) {
        try {
            // Async publication owns no response tensor list. Drop each result
            // after its bounded caches have published it, rather than retaining
            // all completed embeddings until the last image finishes.
            var values = submitAll(inputs, timeout(Context.current(), inputs.getTimeoutMs())).stream()
                    .map(value -> value.thenAccept(ignored -> {})).toList();
            var futures = CompletableFuture.allOf(values.toArray(CompletableFuture[]::new));
            if (futures.isCompletedExceptionally()) futures.join();
            observer.onNext(EngineRpcService.EmptyPB.getDefaultInstance()); observer.onCompleted();
        } catch (Throwable error) {
            Throwable root = cause(error);
            observer.onError((root instanceof IllegalArgumentException ? Status.INVALID_ARGUMENT : Status.fromThrowable(root))
                    .withDescription(root.getMessage()).asRuntimeException());
        }
    }
    @Override public void waitGreenNetVerdict(EngineRpcService.MultimodalInputsPB inputs,
                                              StreamObserver<EngineRpcService.EmptyPB> observer) {
        observer.onNext(EngineRpcService.EmptyPB.getDefaultInstance()); observer.onCompleted();
    }
    @Override public void releaseRdmaLease(EngineRpcService.ReleaseLeasePB request,
                                           StreamObserver<EngineRpcService.EmptyPB> observer) {
        if (request.getLeaseIdCount() != 0) {
            observer.onError(Status.FAILED_PRECONDITION.withDescription("inline Mock ViT owns no RDMA leases").asRuntimeException()); return;
        }
        observer.onNext(EngineRpcService.EmptyPB.getDefaultInstance()); observer.onCompleted();
    }
    @Override public void getWorkerStatus(EngineRpcService.StatusVersionPB request,
                                          StreamObserver<EngineRpcService.WorkerStatusPB> observer) {
        observer.onNext(EngineRpcService.WorkerStatusPB.newBuilder().setRole("VIT")
                .setRoleType(EngineRpcService.RoleTypePB.ROLE_TYPE_VIT).setStatusVersion(1).setAlive(true)
                .setMmInflightInputs(com.google.protobuf.Int64Value.of(pending.size())).build());
        observer.onCompleted();
    }
    @Override public synchronized void getCacheStatus(EngineRpcService.CacheVersionPB request,
                                                      StreamObserver<EngineRpcService.CacheStatusPB> observer) {
        var output = EngineRpcService.CacheStatusPB.newBuilder();
        if (request.getNeedCacheKeys()) output.setMultimodalCache(EngineRpcService.MultimodalCacheStatusPB.newBuilder()
                .setWorkerInstance(instance).addAllKeys(hashes.keySet().stream().limit(100000).toList())
                .addAllCpuEmbeddingKeys(embeddings.keySet().stream().limit(100000).toList()));
        if (output.build().getSerializedSize() > (16 << 20)) {
            observer.onError(Status.RESOURCE_EXHAUSTED.withDescription("ViT directory exceeds 16 MiB").asRuntimeException()); return;
        }
        observer.onNext(output.build()); observer.onCompleted();
    }
    public synchronized Map<String, Long> snapshot() {
        return Map.of("embedding_bytes", embeddingBytes, "hash_bytes", hashBytes,
                "pending", (long)pending.size(), "active", (long)executor.getActiveCount(),
                "queued", (long)executor.getQueue().size(), "computations", computations.get());
    }
    @Override public void close() {
        closed = true;
        executor.shutdownNow();
        pending.values().forEach(value -> value.result.completeExceptionally(Status.UNAVAILABLE.asRuntimeException())); pending.clear();
        synchronized (this) { embeddings.clear(); hashes.clear(); embeddingBytes = hashBytes = 0; }
    }
}
