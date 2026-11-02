package org.flexlb.mockengine;

import org.junit.jupiter.api.Test;

import java.awt.image.BufferedImage;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class MockVitRgbDigestTest {
    private static void verify(int width, int height, int type) throws Exception {
        var image = new BufferedImage(width, height, type);
        for (int y = 0; y < height; y++) for (int x = 0; x < width; x++)
            image.setRGB(x, y, (x * 9701 + y * 7919) ^ 0x7f123456);
        var reference = MessageDigest.getInstance("SHA-256");
        var actual = MessageDigest.getInstance("SHA-256");
        // Preserve both an existing digest prefix and a suffix, as the V4.1
        // model identity and the generic preprocessing geometry require.
        reference.update(new byte[] {1, 2, 3}); actual.update(new byte[] {1, 2, 3});
        for (int y = 0; y < height; y++) for (int x = 0; x < width; x++) {
            int rgb = image.getRGB(x, y);
            reference.update((byte)(rgb >>> 16)); reference.update((byte)(rgb >>> 8)); reference.update((byte)rgb);
        }
        MockVitImage.updateRgbDigest(actual, image);
        reference.update(new byte[] {4, 5, 6}); actual.update(new byte[] {4, 5, 6});
        assertArrayEquals(reference.digest(), actual.digest(), "image type=" + type + " width=" + width);
    }

    @Test void allStandardColorModelsPreserveRgbBytes() throws Exception {
        for (int type = BufferedImage.TYPE_INT_RGB; type <= BufferedImage.TYPE_BYTE_INDEXED; type++)
            verify(37, 23, type);
    }

    @Test void rowsAcrossChunkBoundariesPreserveOrderAndPartialTail() throws Exception {
        for (int width : new int[] {1, 4095, 4096, 4097, 8193})
            verify(width, 3, BufferedImage.TYPE_4BYTE_ABGR_PRE);
    }
}
