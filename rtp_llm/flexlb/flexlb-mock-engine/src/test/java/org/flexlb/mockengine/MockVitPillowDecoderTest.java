package org.flexlb.mockengine;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
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
}
