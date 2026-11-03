package org.flexlb.mockengine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import static org.flexlb.mockengine.MockEngineTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

class NativeCachePreloadTest {
    @TempDir Path directory;

    @Test void deterministicSharedAndPrivateKeysAndSeedChanges() {
        var a = new HashSet<Long>();
        var b = new HashSet<Long>();
        long workerA = MockCachePreload.workerId("10.1.2.3");
        long workerB = MockCachePreload.workerId("10.1.2.4");
        for (int i = 0; i < 1000; i++) {
            long key = MockCachePreload.key(i, 1000, workerA, 20261102L);
            assertEquals(key, MockCachePreload.key(i, 1000, workerA, 20261102L));
            assertNotEquals(key, MockCachePreload.key(i, 1000, workerA, 20261103L));
            a.add(key);
            b.add(MockCachePreload.key(i, 1000, workerB, 20261102L));
        }
        assertEquals(1000, a.size());
        assertEquals(1000, b.size());
        a.retainAll(b);
        assertEquals(860, a.size());
        assertEquals(1, MockCachePreload.sharedBlocks(2));
        assertThrows(IllegalArgumentException.class, () -> MockCachePreload.workerId("0.0.0.0"));
        assertThrows(IllegalArgumentException.class, () -> MockCachePreload.workerId("1.2.3.256"));
        assertThrows(IllegalArgumentException.class, () -> MockCachePreload.validateMode("unknown"));
    }

    @Test void fullPoolIsAvailableIndependentAndNaturallyEvictable() {
        var cache = new MockLruBlockCache(100);
        long worker = MockCachePreload.workerId("10.1.2.3");
        cache.preloadIndependentRoots(worker, 20261102L);
        assertEquals(100, cache.lruKeyBlocks());
        assertEquals(100, cache.availableBlocks());
        assertEquals(0, cache.freeBlocks());
        assertEquals(0, cache.heldBlocks());
        assertEquals(0, cache.referencedKeyBlocks());
        long hit = MockCachePreload.key(0, 100, worker, 20261102L);
        assertEquals(1, cache.prefixHitBlocks(List.of(hit)));
        cache.admit(List.of(42L));
        assertEquals(1, cache.evictions(), "independent roots must not form one giant eviction chain");
        assertEquals(100, cache.lruKeyBlocks());
        assertEquals(1, cache.prefixHitBlocks(List.of(hit)));
        assertTrue(cache.snapshotKeys().contains(42L));
        assertFalse(cache.snapshotKeys().contains(MockCachePreload.key(1, 100, worker, 20261102L)));
    }

    @Test void invalidOrNonfreshPreloadFailsWithoutReplacingCache() {
        var limited = new MockLruBlockCache(100);
        limited.setRetentionBlocks(99);
        assertThrows(IllegalArgumentException.class, () -> limited.preloadIndependentRoots(1, 0));
        assertEquals(0, limited.lruKeyBlocks());
        var occupied = new MockLruBlockCache(100);
        occupied.admit(List.of(42L));
        assertThrows(IllegalArgumentException.class, () -> occupied.preloadIndependentRoots(1, 0));
        assertEquals(java.util.Set.of(42L), occupied.snapshotKeys());
        assertThrows(IllegalArgumentException.class,
                () -> new MockLruBlockCache(4_000_001).preloadIndependentRoots(1, 0));
        var published = new MockLruBlockCache(1);
        published.setMutationListener(m -> fail("preload must not enqueue millions of per-key events"));
        assertThrows(IllegalArgumentException.class, () -> published.preloadIndependentRoots(1, 0));
    }

    @Test void serviceProofDoesNotFakeInferenceCountersAndDecodeRemainsEmpty() throws Exception {
        try (var cluster = MockEngineTestCluster.start(performanceModel(directory, "10"), 61430, 1, 1)) {
            var p = cluster.prefill(0);
            p.initializeCachePreload(MockCachePreload.MODE, 20261102L);
            var proof = p.cachePreloadStatus();
            assertEquals(6144, proof.get("initial_blocks"));
            assertEquals(0, proof.get("initial_held_blocks"));
            assertEquals(0, proof.get("initial_referenced_blocks"));
            assertEquals(0L, ((Number) p.getMetricsSnapshot().get("cache_key_hits")).longValue());
            assertThrows(IllegalArgumentException.class,
                    () -> p.initializeCachePreload(MockCachePreload.MODE, 20261102L));
            var d = cluster.decode(0);
            d.initializeCachePreload(MockCachePreload.MODE, 20261102L);
            assertEquals(false, d.cachePreloadStatus().get("completed"));
            assertEquals(0, d.cachePreloadStatus().get("live_key_blocks"));
            try (var http = java.net.http.HttpClient.newHttpClient()) {
                var response = http.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"
                                + cluster.controlPort() + "/cache_preload")).build(),
                        java.net.http.HttpResponse.BodyHandlers.ofString());
                assertEquals(200, response.statusCode());
                assertTrue(response.body().contains("initial_blocks"));
            }
        }
    }

    @Test void fourMillionBlockStartupFitsEightGiBAndStaysEvictable() {
        var cache = new MockLruBlockCache(4_000_000);
        long started = System.nanoTime();
        cache.preloadIndependentRoots(MockCachePreload.workerId("10.1.2.3"), 20261102L);
        assertEquals(4_000_000, cache.lruKeyBlocks());
        assertEquals(4_000_000, cache.availableBlocks());
        cache.admit(List.of(42L));
        assertEquals(1, cache.evictions());
        System.out.println("native_preload_scale blocks=4000000 elapsed_ms=" + (System.nanoTime()-started)/1_000_000L
                + " used_heap_bytes=" + (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()));
    }
}
