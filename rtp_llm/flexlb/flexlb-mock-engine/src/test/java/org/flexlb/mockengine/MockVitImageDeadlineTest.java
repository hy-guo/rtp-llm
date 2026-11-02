package org.flexlb.mockengine;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class MockVitImageDeadlineTest {
    @Test void responseHeadersDoNotRemoveTheImageBodyDeadline() throws Exception {
        var release = new CountDownLatch(1);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/image", exchange -> {
            try {
                exchange.sendResponseHeaders(200, 1000);
                exchange.getResponseBody().write(0);
                exchange.getResponseBody().flush();
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException error) { Thread.currentThread().interrupt(); }
            finally { exchange.close(); }
        });
        server.start();
        try {
            var image = MockVitServiceTest.image(28, 28, 0x112233).toBuilder()
                    .setMultimodalUrl("http://127.0.0.1:" + server.getAddress().getPort() + "/image").build();
            assertTimeoutPreemptively(Duration.ofSeconds(3), () ->
                    assertThrows(TimeoutException.class, () -> MockVitImage.decode(image, 2000, 150)));
        } finally { release.countDown(); server.stop(0); }
    }
}
