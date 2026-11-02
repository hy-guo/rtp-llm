package org.flexlb.mockengine;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class MockVitPillowDecoderTest {
    private static final byte[] WEBP =
            Base64.getDecoder().decode("UklGRh4AAABXRUJQVlA4TBEAAAAvAQAAAAdQkUZ0pv+BiOh/AAA=");
    @TempDir Path temporary;

    @Test
    void webpUsesRealLoaderRgbAndCorrectGeometry() throws Exception {
        var input =
                MockVitServiceTest.image(2, 1, 0x112233).toBuilder()
                        .setMultimodalUrl(
                                "data:image/webp;base64,"
                                        + Base64.getEncoder().encodeToString(WEBP))
                        .build();
        var image = MockVitImage.decode(input, 1024, 5000);
        assertEquals(2, image.getWidth());
        assertEquals(1, image.getHeight());
        assertEquals(0x112233, image.getRGB(0, 0) & 0xffffff);
        assertEquals(0x112233, image.getRGB(1, 0) & 0xffffff);
        assertTrue(MockVitImage.compute(input, 8, 1024, 1 << 20, 5000).rows() > 0);
        var v41 = new MockV41Image.Config(8, 14, 4, 1024, 32768, "0".repeat(64));
        assertTrue(MockV41Image.compute(input, v41, 1024, 1 << 20, 5000).rows() > 0);
    }

    @Test
    void invalidBytesRemainInvalidInsteadOfReturningSyntheticPixels() {
        assertThrows(
                IllegalArgumentException.class,
                () -> MockVitPillowDecoder.decode(new byte[12], 5000));
    }

    private String executable(String body) throws Exception {
        Path file = temporary.resolve("decoder");
        Files.writeString(file, "#!/bin/sh\n" + body);
        assertTrue(file.toFile().setExecutable(true));
        return file.toString();
    }

    @Test
    void aStalledDecoderIsKilledAndDeadlineIsReported() throws Exception {
        String decoder = executable("exec sleep 30\n");
        assertTimeoutPreemptively(
                Duration.ofSeconds(3),
                () ->
                        assertThrows(
                                TimeoutException.class,
                                () -> MockVitPillowDecoder.decode(new byte[0], 50, decoder)));
    }

    @Test
    void oversizedDecodedDimensionsAreRejectedBeforeRasterAllocation() throws Exception {
        String decoder = executable("printf '\\177\\377\\377\\377\\177\\377\\377\\377'\n");
        assertThrows(
                IllegalArgumentException.class,
                () -> MockVitPillowDecoder.decode(new byte[0], 1000, decoder));
    }

    @Test
    void fallbackChildrenAreBoundedAndQueueWaitConsumesTheDeadline() throws Exception {
        int slots = Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors()));
        Path started = Files.createDirectory(temporary.resolve("started"));
        Path release = temporary.resolve("release");
        String decoder = executable("touch '" + started + "/'$$\n"
                + "while [ ! -e '" + release + "' ]; do sleep 0.02; done\n"
                + "printf '\\000\\000\\000\\001\\000\\000\\000\\001\\021\\042\\063'\n");
        var executor = Executors.newFixedThreadPool(slots + 2);
        var calls = new ArrayList<Future<java.awt.image.BufferedImage>>();
        try {
            for (int i = 0; i < slots + 2; i++)
                calls.add(executor.submit(() -> MockVitPillowDecoder.decode(new byte[0], 10000, decoder)));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            long count;
            do {
                try (var entries = Files.list(started)) { count = entries.count(); }
                if (count >= slots) break;
                TimeUnit.MILLISECONDS.sleep(10);
            } while (System.nanoTime() < deadline);
            assertEquals(slots, count);
            assertTimeoutPreemptively(Duration.ofSeconds(2), () -> assertThrows(TimeoutException.class,
                    () -> MockVitPillowDecoder.decode(new byte[0], 50, decoder)));
            try (var entries = Files.list(started)) {
                assertEquals(slots, entries.count(), "queued/expired callers must not start extra children");
            }
            Files.createFile(release);
            for (var call : calls) {
                var image = call.get(5, TimeUnit.SECONDS);
                assertEquals(0x112233, image.getRGB(0, 0) & 0xffffff);
            }
            try (var entries = Files.list(started)) { assertEquals(slots + 2, entries.count()); }
        } finally {
            if (!Files.exists(release)) Files.createFile(release);
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void decoderFailureAndTimeoutReleaseTheProcessSlot() throws Exception {
        String broken = executable("exit 1\n");
        assertThrows(IllegalArgumentException.class,
                () -> MockVitPillowDecoder.decode(new byte[0], 1000, broken));
        String valid = executable("printf '\\000\\000\\000\\001\\000\\000\\000\\001\\021\\042\\063'\n");
        assertEquals(0x112233,
                MockVitPillowDecoder.decode(new byte[0], 1000, valid).getRGB(0, 0) & 0xffffff);
    }

    @Test
    void rgbChunksPreservePixelsAcrossTheChunkBoundary() throws Exception {
        var original = new java.awt.image.BufferedImage(4101, 2, java.awt.image.BufferedImage.TYPE_INT_RGB);
        for (int y = 0; y < 2; y++) for (int x = 0; x < 4101; x++)
            original.setRGB(x, y, (x * 0x713 + y * 0x1731) & 0xffffff);
        var encoded = new java.io.ByteArrayOutputStream();
        assertTrue(javax.imageio.ImageIO.write(original, "png", encoded));
        var decoded = MockVitPillowDecoder.decode(encoded.toByteArray(), 5000);
        assertArrayEquals(original.getRGB(0, 0, 4101, 2, null, 0, 4101),
                decoded.getRGB(0, 0, 4101, 2, null, 0, 4101));
    }
}
