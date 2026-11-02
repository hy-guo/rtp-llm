package org.flexlb.mockengine;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MockVitChunksTest {
    @Test
    void boundedParallelismPreservesOrderAndContext() throws Exception {
        var ready = new CountDownLatch(4);
        var active = new AtomicInteger();
        var maximum = new AtomicInteger();
        var output = new ArrayList<Integer>();
        var key = Context.<String>key("fixture");
        Context.current().withValue(key, "caller").call(() -> {
            MockVitChunks.run(11, 5000, (index, remaining) -> {
                assertEquals("caller", key.get());
                assertTrue(remaining > 0 && remaining <= 5000);
                maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
                try {
                    ready.countDown();
                    assertTrue(ready.await(2, TimeUnit.SECONDS), "chunks were serialized");
                    return index;
                } catch (InterruptedException error) {
                    throw new AssertionError(error);
                } finally { active.decrementAndGet(); }
            }, output::add);
            return null;
        });
        assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), output);
        assertEquals(4, maximum.get());
    }

    @Test
    void sharedDeadlineCancelsEveryOutstandingChunk() throws Exception {
        var stopped = new CountDownLatch(4);
        var never = new CountDownLatch(1);
        var error = assertThrows(StatusRuntimeException.class, () ->
                MockVitChunks.run(20, 500, (index, remaining) -> {
                    try { never.await(); return index; }
                    catch (InterruptedException interrupted) { throw Status.CANCELLED.asRuntimeException(); }
                    finally { stopped.countDown(); }
                }, ignored -> fail("expired chunk delivered")));
        assertEquals(Status.Code.DEADLINE_EXCEEDED, error.getStatus().getCode());
        assertTrue(stopped.await(2, TimeUnit.SECONDS), "deadline left chunks running");
    }

    @Test
    void callerCancellationStopsInFlightAndDoesNotStartTail() throws Exception {
        var started = new CountDownLatch(4);
        var stopped = new CountDownLatch(4);
        var calls = new AtomicInteger();
        var never = new CountDownLatch(1);
        try (var parent = Context.current().withCancellation();
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var result = executor.submit(parent.wrap(() -> {
                var error = assertThrows(StatusRuntimeException.class, () ->
                        MockVitChunks.run(20, 10000, (index, remaining) -> {
                            calls.incrementAndGet(); started.countDown();
                            try { never.await(); return index; }
                            catch (InterruptedException interrupted) { throw Status.CANCELLED.asRuntimeException(); }
                            finally { stopped.countDown(); }
                        }, ignored -> fail("cancelled chunk delivered")));
                return error.getStatus().getCode();
            }));
            assertTrue(started.await(2, TimeUnit.SECONDS));
            parent.cancel(null);
            assertEquals(Status.Code.CANCELLED, result.get(2, TimeUnit.SECONDS));
            assertTrue(stopped.await(2, TimeUnit.SECONDS));
            assertEquals(4, calls.get());
        }
    }

    @Test
    void transportErrorIsPreservedAndOtherChunksAreCancelled() throws Exception {
        var started = new CountDownLatch(4);
        var stopped = new CountDownLatch(4);
        var never = new CountDownLatch(1);
        var error = assertThrows(StatusRuntimeException.class, () ->
                MockVitChunks.run(20, 5000, (index, remaining) -> {
                    started.countDown();
                    try {
                        assertTrue(started.await(2, TimeUnit.SECONDS));
                        if (index == 0) throw Status.UNAVAILABLE.withDescription("fixture").asRuntimeException();
                        never.await(); return index;
                    } catch (InterruptedException interrupted) { throw Status.CANCELLED.asRuntimeException(); }
                    finally { stopped.countDown(); }
                }, ignored -> fail("failed chunk delivered")));
        assertEquals(Status.Code.UNAVAILABLE, error.getStatus().getCode());
        assertEquals("fixture", error.getStatus().getDescription());
        assertTrue(stopped.await(2, TimeUnit.SECONDS));
    }
}
