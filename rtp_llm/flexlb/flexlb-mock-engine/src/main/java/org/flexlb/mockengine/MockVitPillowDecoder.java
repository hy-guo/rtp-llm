package org.flexlb.mockengine;

import java.awt.image.BufferedImage;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/** CPU-only fallback for the codecs supported by the real Pillow image loader. */
final class MockVitPillowDecoder {
    private static final int MAX_PIXELS = 32 << 20;
    private static final ScheduledThreadPoolExecutor TIMER =
            new ScheduledThreadPoolExecutor(
                    1,
                    task -> {
                        Thread thread = new Thread(task, "mock-vit-image-deadline");
                        thread.setDaemon(true);
                        return thread;
                    });

    static {
        TIMER.setRemoveOnCancelPolicy(true);
    }

    private static final String SCRIPT = script();

    private MockVitPillowDecoder() {}

    private static String script() {
        try (var input =
                MockVitPillowDecoder.class.getResourceAsStream("/mock_vit_image_decoder.py")) {
            if (input == null)
                throw new IllegalStateException("Pillow decoder resource is missing");
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException error) {
            throw new IllegalStateException("Cannot load Pillow decoder", error);
        }
    }

    static BufferedImage decode(byte[] bytes, long timeoutMs) throws Exception {
        return decode(bytes, timeoutMs, System.getProperty("mock.vit.imagePython", "python3"));
    }

    static BufferedImage decode(byte[] bytes, long timeoutMs, String python) throws Exception {
        if (timeoutMs <= 0) throw new TimeoutException("Image decode deadline expired");
        var process =
                new ProcessBuilder(python, "-c", SCRIPT, Integer.toString(MAX_PIXELS))
                        .redirectError(ProcessBuilder.Redirect.DISCARD)
                        .start();
        var expired = new AtomicBoolean();
        var alarm =
                TIMER.schedule(
                        () -> {
                            expired.set(true);
                            process.destroyForcibly();
                        },
                        timeoutMs,
                        TimeUnit.MILLISECONDS);
        try {
            try (var input = process.getOutputStream()) {
                input.write(bytes);
            }
            try (var output = new DataInputStream(process.getInputStream())) {
                int width = output.readInt(), height = output.readInt();
                if (width <= 0 || height <= 0 || (long) width * height > MAX_PIXELS)
                    throw new IllegalArgumentException("image exceeds Mock decoded-pixel limit");
                int length = Math.multiplyExact(Math.multiplyExact(width, height), 3);
                byte[] rgb = output.readNBytes(length + 1);
                if (expired.get()) throw new TimeoutException("Image decode deadline expired");
                if (rgb.length != length)
                    throw new IllegalArgumentException("invalid decoded image payload");
                int exit = process.waitFor();
                if (expired.get()) throw new TimeoutException("Image decode deadline expired");
                if (exit != 0) throw decodeError(exit);
                var image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
                int offset = 0;
                for (int y = 0; y < height; y++)
                    for (int x = 0; x < width; x++) {
                        int pixel =
                                ((rgb[offset] & 255) << 16)
                                        | ((rgb[offset + 1] & 255) << 8)
                                        | (rgb[offset + 2] & 255);
                        image.setRGB(x, y, pixel);
                        offset += 3;
                    }
                return image;
            }
        } catch (IOException error) {
            // stdout can close just before the child's exit status becomes visible.
            int exit = process.waitFor();
            if (expired.get()) throw new TimeoutException("Image decode deadline expired");
            throw decodeError(exit);
        } finally {
            alarm.cancel(false);
            if (process.isAlive()) process.destroyForcibly();
            process.waitFor(1, TimeUnit.SECONDS);
        }
    }

    private static IllegalArgumentException decodeError(int code) {
        return new IllegalArgumentException(
                code == 3
                        ? "image exceeds Mock decoded-pixel limit"
                        : "unsupported or invalid image");
    }
}
