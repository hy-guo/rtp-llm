package org.flexlb.collection;

import java.util.function.LongConsumer;

/** Internal read-only primitive traversal contract; preserves every long key. */
public interface PrimitiveLongSource {
    void forEachLong(LongConsumer visitor);
}
