package org.flexlb.mockengine;

import java.awt.image.BufferedImage;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/** CPU-only fallback for the codecs supported by the real Pillow image loader. */
final class MockVitPillowDecoder {
    private static final int MAX_PIXELS = 32 << 20;
    // Match the real preprocessor's bounded process ownership. This is a Mock
    // memory guard, independent of forward/RPC concurrency and JVM heap limits.
    private static final Semaphore PROCESSES =
            new Semaphore(Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors())), true);
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
        long deadline = System.nanoTime()
                + Math.min(TimeUnit.MILLISECONDS.toNanos(timeoutMs), Long.MAX_VALUE / 2);
        if (!PROCESSES.tryAcquire(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
            throw new TimeoutException("Image decode deadline expired in process queue");
        Process process;
        try {
            if (System.nanoTime() - deadline >= 0)
                throw new TimeoutException("Image decode deadline expired in process queue");
            process = new ProcessBuilder(python, "-c", SCRIPT, Integer.toString(MAX_PIXELS))
                    .redirectError(ProcessBuilder.Redirect.DISCARD).start();
        } catch (Throwable error) {
            PROCESSES.release();
            throw error;
        }
        var expired = new AtomicBoolean();
        ScheduledFuture<?> alarm = null;
        try {
            alarm = TIMER.schedule(
                        () -> {
                            expired.set(true);
                            process.destroyForcibly();
                        },
                        Math.max(0, deadline - System.nanoTime()),
                        TimeUnit.NANOSECONDS);
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
                int offset = 0, chunk = Math.min(width, 4096);
                int[] pixels = new int[chunk];
                for (int y = 0; y < height; y++) {
                    for (int x = 0; x < width; x += chunk) {
                        int count = Math.min(chunk, width - x);
                        for (int i = 0; i < count; i++) {
                            pixels[i] = ((rgb[offset] & 255) << 16)
                                    | ((rgb[offset + 1] & 255) << 8) | (rgb[offset + 2] & 255);
                            offset += 3;
                        }
                        image.setRGB(x, y, count, 1, pixels, 0, count);
                    }
                }
                if (expired.get() || System.nanoTime() - deadline >= 0)
                    throw new TimeoutException("Image decode deadline expired");
                return image;
            }
        } catch (IOException error) {
            // stdout can close just before the child's exit status becomes visible.
            int exit = process.waitFor();
            if (expired.get()) throw new TimeoutException("Image decode deadline expired");
            throw decodeError(exit);
        } finally {
            if (alarm != null) alarm.cancel(false);
            if (process.isAlive()) process.destroyForcibly();
            // A stuck child still owns its slot. Do not over-admit replacements
            // merely because the caller's bounded cleanup wait elapsed.
            process.onExit().whenComplete((done, error) -> PROCESSES.release());
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
