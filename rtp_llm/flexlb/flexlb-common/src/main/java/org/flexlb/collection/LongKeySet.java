package org.flexlb.collection;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/** HashSet-compatible surface without one boxed Long and node per retained key. */
public final class LongKeySet extends AbstractSet<Long> implements PrimitiveLongSource {
    private final LongObjectMap<Boolean> keys;

    public LongKeySet() {
        this(0);
    }

    public LongKeySet(int expectedSize) {
        keys = new LongObjectMap<>(expectedSize);
    }

    public LongKeySet(Collection<? extends Long> input) {
        this(input.size());
        if (input instanceof PrimitiveLongSource source) source.forEachLong(this::addLong);
        else addAll(input);
    }

    public static Set<Long> create() {
        return new LongKeySet();
    }

    public static Set<Long> create(int expectedSize) {
        return new LongKeySet(expectedSize);
    }

    public boolean addLong(long key) {
        return keys.putLong(key, Boolean.TRUE, true) == null;
    }

    public boolean containsLong(long key) {
        return keys.getLong(key) != null;
    }

    public void forEachLong(LongConsumer action) {
        keys.forEachLong(action);
    }

    @Override
    public boolean add(Long k) {
        return keys.putLong(k, Boolean.TRUE, true) == null;
    }

    @Override
    public boolean contains(Object k) {
        return keys.containsKey(k);
    }

    @Override
    public boolean remove(Object k) {
        return keys.remove(k) != null;
    }

    @Override
    public int size() {
        return keys.size();
    }

    @Override
    public boolean isEmpty() {
        return keys.isEmpty();
    }

    @Override
    public void clear() {
        keys.clear();
    }

    @Override
    public Iterator<Long> iterator() {
        return keys.longIterator();
    }

    @Override
    public Spliterator<Long> spliterator() {
        return Spliterators.spliterator(
                iterator(),
                size(),
                Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.CONCURRENT);
    }

    @Override
    public Object[] toArray() {
        return arrayList().toArray();
    }

    @Override
    public <T> T[] toArray(T[] target) {
        return arrayList().toArray(target);
    }

    private ArrayList<Long> arrayList() {
        ArrayList<Long> out = new ArrayList<>(size());
        for (Long k : this) out.add(k);
        return out;
    }

    @Override
    public Object clone() {
        return new LongKeySet(this);
    }

    @Override
    public void forEach(Consumer<? super Long> action) {
        Objects.requireNonNull(action);
        for (Long k : this) action.accept(k);
    }
}
