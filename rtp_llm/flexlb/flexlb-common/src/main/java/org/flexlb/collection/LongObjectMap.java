package org.flexlb.collection;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import java.util.function.LongConsumer;

/** Primitive keys; public operations used by the existing index retain Map semantics. */
public final class LongObjectMap<V> extends AbstractMap<Long, V> {
    private static final int SHARDS = 64;
    private final Shard[] shards = new Shard[SHARDS];

    public LongObjectMap() {
        this(0);
    }

    public LongObjectMap(int expectedSize) {
        if (expectedSize < 0) throw new IllegalArgumentException("Negative size");
        long perShard = ((long) expectedSize + SHARDS - 1) / SHARDS;
        int capacity = 16;
        while (capacity * 6L < perShard * 10L && capacity < (1 << 26)) capacity *= 2;
        for (int i = 0; i < SHARDS; i++) shards[i] = new Shard(capacity);
    }

    static long mix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        return k ^ (k >>> 33);
    }

    private Shard shard(long k) {
        return shards[(int) mix(k) & (SHARDS - 1)];
    }

    @SuppressWarnings("unchecked")
    public V getLong(long k) {
        return (V) shard(k).get(k);
    }

    @Override
    public V get(Object k) {
        return k instanceof Long ? getLong((Long) k) : null;
    }

    @Override
    public boolean containsKey(Object k) {
        return get(k) != null;
    }

    @SuppressWarnings("unchecked")
    public V putLong(long k, V v, boolean absentOnly) {
        return (V) shard(k).put(k, Objects.requireNonNull(v), absentOnly, null);
    }

    @Override
    public V put(Long k, V v) {
        return putLong(k, v, false);
    }

    @Override
    public V putIfAbsent(Long k, V v) {
        return putLong(k, v, true);
    }

    @SuppressWarnings("unchecked")
    @Override
    public V computeIfAbsent(Long k, Function<? super Long, ? extends V> f) {
        Objects.requireNonNull(f);
        return (V) shard(k).put(k, null, true, f);
    }

    @SuppressWarnings("unchecked")
    public V removeLong(long k) {
        return (V) shard(k).remove(k);
    }

    @Override
    public V remove(Object k) {
        return k instanceof Long ? removeLong((Long) k) : null;
    }

    @Override
    public void clear() {
        for (Shard s : shards) s.clear();
    }

    public long mappingCount() {
        long n = 0;
        for (Shard s : shards) n += s.size;
        return n;
    }

    @Override
    public int size() {
        return (int) Math.min(Integer.MAX_VALUE, mappingCount());
    }

    @Override
    public boolean isEmpty() {
        return mappingCount() == 0;
    }

    /** Same per-shard snapshot semantics as longIterator, without boxing. */
    public void forEachLong(LongConsumer action) {
        Objects.requireNonNull(action);
        for (Shard shard : shards) for (long key : shard.snapshotKeys()) action.accept(key);
    }

    public Iterator<Long> longIterator() {
        return new Iterator<>() {
            int shard = 0, pos = 0;
            long[] keys = new long[0];
            Long last;

            private void advance() {
                while (pos == keys.length && shard < SHARDS) {
                    keys = shards[shard++].snapshotKeys();
                    pos = 0;
                }
            }

            public boolean hasNext() {
                advance();
                return pos < keys.length;
            }

            public Long next() {
                if (!hasNext()) throw new NoSuchElementException();
                return last = keys[pos++];
            }

            public void remove() {
                if (last == null) throw new IllegalStateException();
                removeLong(last);
                last = null;
            }
        };
    }

    @Override
    public Set<Map.Entry<Long, V>> entrySet() {
        return new AbstractSet<>() {
            @Override
            public int size() {
                return LongObjectMap.this.size();
            }

            @Override
            public void clear() {
                LongObjectMap.this.clear();
            }

            @Override
            public Iterator<Map.Entry<Long, V>> iterator() {
                Iterator<Long> keys = longIterator();
                return new Iterator<>() {
                    Map.Entry<Long, V> next;
                    Long last;

                    public boolean hasNext() {
                        while (next == null && keys.hasNext()) {
                            Long key = keys.next();
                            V value = get(key);
                            if (value != null)
                                next = new AbstractMap.SimpleImmutableEntry<>(key, value);
                        }
                        return next != null;
                    }

                    public Map.Entry<Long, V> next() {
                        if (!hasNext()) throw new NoSuchElementException();
                        var result = next;
                        next = null;
                        last = result.getKey();
                        return result;
                    }

                    public void remove() {
                        if (last == null) throw new IllegalStateException();
                        LongObjectMap.this.remove(last);
                        last = null;
                    }
                };
            }
        };
    }

    private static final class Shard {
        final StampedLock lock = new StampedLock();
        long[] keys;
        Object[] values;
        volatile int size;

        Shard(int capacity) {
            keys = new long[capacity];
            values = new Object[capacity];
        }

        int index(long key, long[] ks, Object[] vs) {
            int p = (int) (mix(key) >>> 6) & (ks.length - 1);
            for (int n = 0; n < ks.length; n++, p = (p + 1) & (ks.length - 1))
                if (vs[p] == null || ks[p] == key) return p;
            return -1;
        }

        Object get(long key) {
            long stamp = lock.tryOptimisticRead();
            long[] ks = keys;
            Object[] vs = values;
            if (stamp != 0 && ks.length == vs.length) {
                int p = index(key, ks, vs);
                Object v = p < 0 ? null : vs[p];
                if (lock.validate(stamp)) return v;
            }
            stamp = lock.readLock();
            try {
                int p = index(key, keys, values);
                return p < 0 ? null : values[p];
            } finally {
                lock.unlockRead(stamp);
            }
        }

        Object put(long key, Object value, boolean absentOnly, Function<?, ?> factory) {
            long stamp = lock.writeLock();
            try {
                int p = index(key, keys, values);
                Object previous = p < 0 ? null : values[p];
                if (previous != null) {
                    if (!absentOnly) values[p] = value;
                    return previous;
                }
                if (factory != null) {
                    @SuppressWarnings("unchecked")
                    Function<Long, ?> f = (Function<Long, ?>) factory;
                    value = f.apply(key);
                    if (value == null) return null;
                }
                if ((size + 1) * 10L > keys.length * 6L) {
                    resize();
                    p = index(key, keys, values);
                }
                keys[p] = key;
                values[p] = value;
                size++;
                return factory != null ? value : null;
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        void resize() {
            long[] old = keys;
            Object[] ov = values;
            keys = new long[old.length * 2];
            values = new Object[ov.length * 2];
            for (int i = 0; i < old.length; i++)
                if (ov[i] != null) {
                    int p = index(old[i], keys, values);
                    keys[p] = old[i];
                    values[p] = ov[i];
                }
        }

        Object remove(long key) {
            long stamp = lock.writeLock();
            try {
                int hole = index(key, keys, values);
                if (hole < 0 || values[hole] == null) return null;
                Object old = values[hole];
                int mask = keys.length - 1;
                values[hole] = null;
                size--;
                for (int p = (hole + 1) & mask; values[p] != null; p = (p + 1) & mask) {
                    int home = (int) (mix(keys[p]) >>> 6) & mask;
                    if (((p - home) & mask) >= ((p - hole) & mask)) {
                        keys[hole] = keys[p];
                        values[hole] = values[p];
                        values[p] = null;
                        hole = p;
                    }
                }
                return old;
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        void clear() {
            long stamp = lock.writeLock();
            try {
                keys = new long[16];
                values = new Object[16];
                size = 0;
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        long[] snapshotKeys() {
            long stamp = lock.readLock();
            try {
                long[] out = new long[size];
                int n = 0;
                for (int i = 0; i < keys.length; i++) if (values[i] != null) out[n++] = keys[i];
                return out;
            } finally {
                lock.unlockRead(stamp);
            }
        }
    }
}
