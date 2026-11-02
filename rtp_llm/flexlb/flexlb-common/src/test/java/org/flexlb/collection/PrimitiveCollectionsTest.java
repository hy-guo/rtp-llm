package org.flexlb.collection;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class PrimitiveCollectionsTest {
    @Test
    void mapAndSetMatchStandardCollectionsIncludingExtremeKeys() {
        var map = new LongObjectMap<String>();
        var expected = new HashMap<Long, String>();
        var random = new Random(9917);
        for (long key : List.of(0L, Long.MIN_VALUE, Long.MAX_VALUE, -1L)) {
            map.put(key, "edge");
            expected.put(key, "edge");
        }
        for (int i = 0; i < 100000; i++) {
            long key = random.nextInt(10000) - 5000;
            String value = Integer.toString(i);
            switch (random.nextInt(4)) {
                case 0 -> assertEquals(expected.put(key, value), map.put(key, value));
                case 1 ->
                        assertEquals(expected.putIfAbsent(key, value), map.putIfAbsent(key, value));
                case 2 -> assertEquals(expected.remove(key), map.remove(key));
                default -> assertEquals(expected.get(key), map.get(key));
            }
        }
        assertEquals(expected, map);
        assertEquals(map, expected);
        var copied = new HashMap<Long, String>();
        map.forEach(copied::put);
        assertEquals(expected, copied);
        var keys = new LongKeySet(map.keySet());
        assertEquals(expected.keySet(), keys);
        assertEquals(expected.keySet().hashCode(), keys.hashCode());
        keys.removeIf(key -> key < 0);
        assertTrue(keys.stream().allMatch(key -> key >= 0));
        assertEquals(keys, keys.clone());
        map.clear();
        assertEquals(0, map.mappingCount());
        assertTrue(map.entrySet().isEmpty());
    }

    @Test
    void resizingConcurrentWritersRetainAllKeysAndStableReads() throws Exception {
        var map = new LongObjectMap<String>();
        for (long i = 0; i < 1000; i++) map.put(i, "stable");
        try (var workers = Executors.newFixedThreadPool(8)) {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int worker = 0; worker < 4; worker++) {
                final int id = worker;
                futures.add(
                        workers.submit(
                                () -> {
                                    for (int i = 0; i < 25000; i++)
                                        map.put(1000L + id * 25000L + i, "value");
                                }));
                futures.add(
                        workers.submit(
                                () -> {
                                    for (int i = 0; i < 100000; i++)
                                        assertEquals("stable", map.get((long) i % 1000));
                                }));
            }
            for (var future : futures) future.get(10, TimeUnit.SECONDS);
        }
        assertEquals(101000, map.size());
        assertEquals(101000, map.entrySet().size());
        var actual = new HashSet<Long>();
        map.forEach((key, value) -> actual.add(key));
        assertEquals(101000, actual.size());
    }

    @Test
    void ownerSetIterationRetainsSnapshotAndRemovesCurrentOwner() {
        var owners = new SmallOwnerSet();
        owners.add("a");
        owners.add("b");
        var snapshot = owners.iterator();
        owners.add("c");
        assertEquals("a", snapshot.next());
        snapshot.remove();
        assertEquals("b", snapshot.next());
        assertFalse(snapshot.hasNext());
        assertEquals(java.util.Set.of("b", "c"), owners);
    }
}
