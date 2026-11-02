package org.flexlb.collection;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/** Volatile immutable owner snapshots: a singleton needs no backing table. */
public final class SmallOwnerSet extends AbstractSet<String> {
    private volatile Object owners;

    public static Set<String> create() {
        return new SmallOwnerSet();
    }

    @Override
    public synchronized boolean add(String owner) {
        Objects.requireNonNull(owner);
        Object old = owners;
        if (old == null) {
            owners = owner;
            return true;
        }
        if (old instanceof String) {
            if (old.equals(owner)) return false;
            owners = new String[] {(String) old, owner};
            return true;
        }
        String[] a = (String[]) old;
        for (String s : a) if (s.equals(owner)) return false;
        String[] next = Arrays.copyOf(a, a.length + 1);
        next[a.length] = owner;
        owners = next;
        return true;
    }

    @Override
    public synchronized boolean remove(Object owner) {
        Object old = owners;
        if (old == null) return false;
        if (old instanceof String) {
            if (!old.equals(owner)) return false;
            owners = null;
            return true;
        }
        String[] a = (String[]) old;
        int at = -1;
        for (int i = 0; i < a.length; i++)
            if (a[i].equals(owner)) {
                at = i;
                break;
            }
        if (at < 0) return false;
        if (a.length == 2) {
            owners = a[1 - at];
            return true;
        }
        String[] next = new String[a.length - 1];
        System.arraycopy(a, 0, next, 0, at);
        System.arraycopy(a, at + 1, next, at, a.length - at - 1);
        owners = next;
        return true;
    }

    @Override
    public boolean contains(Object owner) {
        Object a = owners;
        if (a == null) return false;
        if (a instanceof String) return a.equals(owner);
        for (String s : (String[]) a) if (s.equals(owner)) return true;
        return false;
    }

    @Override
    public int size() {
        Object a = owners;
        return a == null ? 0 : a instanceof String ? 1 : ((String[]) a).length;
    }

    @Override
    public synchronized void clear() {
        owners = null;
    }

    @Override
    public Iterator<String> iterator() {
        Object a = owners;
        Iterator<String> it =
                (a == null
                                ? Collections.<String>emptyList()
                                : a instanceof String
                                        ? Collections.singletonList((String) a)
                                        : Arrays.asList((String[]) a))
                        .iterator();
        return new Iterator<>() {
            String last;

            public boolean hasNext() {
                return it.hasNext();
            }

            public String next() {
                return last = it.next();
            }

            public void remove() {
                if (last == null) throw new IllegalStateException();
                SmallOwnerSet.this.remove(last);
                last = null;
            }
        };
    }
}
