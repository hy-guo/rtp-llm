package org.flexlb.mockengine;

import io.grpc.Context;
import io.grpc.Status;

import java.util.ArrayDeque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/** Bounded inline-transport adaptation of one real MMRemoteOutputTransport fetch.
 * Four response buffers at most; all chunks share the caller's deadline and cancellation.
 */
final class MockVitChunks {
    static final int MAX_INFLIGHT = 4;

    private MockVitChunks() {}

    static <T> void run(int count, long budgetMs, BiFunction<Integer, Long, T> work,
                       Consumer<T> orderedResult) {
        if (count == 0) return;
        if (budgetMs <= 0) throw Status.DEADLINE_EXCEEDED.asRuntimeException();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.min(budgetMs, 120000));
        var context = Context.current().withCancellation();
        var executor = Executors.newFixedThreadPool(Math.min(count, MAX_INFLIGHT),
                Thread.ofVirtual().name("mock-vit-chunk-", 0).factory());
        var active = new ConcurrentLinkedQueue<Future<T>>();
        var ordered = new ArrayDeque<Future<T>>();
        Context.CancellationListener cancellation = ignored -> active.forEach(f -> f.cancel(true));
        context.addListener(cancellation, Runnable::run);
        try {
            int submitted = 0;
            for (int consumed = 0; consumed < count; consumed++) {
                while (submitted < count && ordered.size() < MAX_INFLIGHT) {
                    int index = submitted++;
                    Future<T> next = executor.submit(context.wrap(() -> {
                        checkCancelled(context);
                        return work.apply(index, remaining(deadline));
                    }));
                    active.add(next);
                    ordered.addLast(next);
                    if (context.isCancelled()) next.cancel(true);
                }
                checkCancelled(context);
                Future<T> next = ordered.removeFirst();
                T value = next.get(remaining(deadline), TimeUnit.MILLISECONDS);
                active.remove(next);
                checkCancelled(context);
                orderedResult.accept(value);
            }
        } catch (TimeoutException error) {
            throw Status.DEADLINE_EXCEEDED.withDescription("Mock ViT chunks exceeded shared budget")
                    .asRuntimeException();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED.withDescription("Mock ViT chunks interrupted").asRuntimeException();
        } catch (CancellationException error) {
            checkCancelled(context);
            throw Status.CANCELLED.asRuntimeException();
        } catch (ExecutionException error) {
            if (error.getCause() instanceof RuntimeException cause) throw cause;
            if (error.getCause() instanceof Error cause) throw cause;
            throw Status.INTERNAL.withCause(error.getCause()).asRuntimeException();
        } finally {
            context.removeListener(cancellation);
            context.cancel(null);
            active.forEach(f -> f.cancel(true));
            executor.shutdownNow();
        }
    }

    private static long remaining(long deadline) {
        long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        if (remaining <= 0) throw Status.DEADLINE_EXCEEDED.asRuntimeException();
        return remaining;
    }

    private static void checkCancelled(Context context) {
        if (context.isCancelled()) {
            if (context.getDeadline() != null && context.getDeadline().isExpired())
                throw Status.DEADLINE_EXCEEDED.asRuntimeException();
            throw Status.CANCELLED.asRuntimeException();
        }
    }
}
