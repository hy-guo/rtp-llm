package org.flexlb.mockengine;

/** Mock-only startup workload: ordinary, evictable independent prefix roots. */
final class MockCachePreload {
    static final String MODE = "independent_prefixes_86pct_shared";
    static final int MAX_BLOCKS = 4_000_000;

    static void validateMode(String mode) {
        if (!"none".equals(mode) && !MODE.equals(mode))
            throw new IllegalArgumentException("unsupported cache preload mode: " + mode);
    }

    static long workerId(String host) {
        String[] parts = host.split("\\.", -1);
        if (parts.length != 4) throw new IllegalArgumentException("cache preload requires an IPv4 host");
        long id = 0;
        for (String part : parts) {
            if (!part.matches("[0-9]{1,3}")) throw new IllegalArgumentException("invalid preload IPv4 host");
            int value = Integer.parseInt(part);
            if (value > 255) throw new IllegalArgumentException("invalid preload IPv4 host");
            id = (id << 8) | value;
        }
        if (id == 0) throw new IllegalArgumentException("cache preload requires a nonzero IPv4 identity");
        return id;
    }

    static int sharedBlocks(int count) { return (int) (count * 86L / 100); }

    static long key(int index, int count, long worker, long seed) {
        int shared = sharedBlocks(count);
        long identity = index < shared ? index : ((worker << 32) | (index - shared));
        // A bijection over 64 bits: no hash collisions or temporary duplicate-key set.
        long n = identity ^ seed;
        n = (n ^ (n >>> 30)) * 0xbf58476d1ce4e5b9L;
        n = (n ^ (n >>> 27)) * 0x94d049bb133111ebL;
        return n ^ (n >>> 31);
    }

    private MockCachePreload() {}
}
