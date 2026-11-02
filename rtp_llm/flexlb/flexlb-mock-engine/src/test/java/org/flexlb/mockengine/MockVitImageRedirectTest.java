package org.flexlb.mockengine;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MockVitImageRedirectTest {
    @TempDir Path temporary;

    @Test void followsHttpsToHttpImageRedirectWithoutChangingBytes() throws Exception {
        // Ephemeral, loopback-only TLS fixture. No production trust-store changes.
        Path store = temporary.resolve("fixture.p12");
        var keytool = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair", "-alias", "fixture", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                "-storetype", "PKCS12", "-keystore", store.toString(), "-storepass", "fixturepass",
                "-keyalg", "RSA", "-keysize", "2048", "-validity", "1", "-noprompt")
                .redirectErrorStream(true).redirectOutput(temporary.resolve("keytool.log").toFile()).start();
        try {
            assertTrue(keytool.waitFor(20, TimeUnit.SECONDS), "TLS fixture generation timed out");
            assertEquals(0, keytool.exitValue());
        } finally { if (keytool.isAlive()) keytool.destroyForcibly(); }
        var keys = KeyStore.getInstance("PKCS12");
        try (var input = Files.newInputStream(store)) { keys.load(input, "fixturepass".toCharArray()); }
        var km = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        km.init(keys, "fixturepass".toCharArray());
        var tm = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tm.init(keys);
        var ssl = SSLContext.getInstance("TLS");
        ssl.init(km.getKeyManagers(), tm.getTrustManagers(), null);
        String dataUrl = MockVitServiceTest.image(28, 28, 0x112233).getMultimodalUrl();
        byte[] pixels = Base64.getDecoder().decode(dataUrl.substring(dataUrl.indexOf(',') + 1));
        var plain = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var secure = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        secure.setHttpsConfigurator(new HttpsConfigurator(ssl));
        plain.createContext("/image", exchange -> {
            try {
                exchange.sendResponseHeaders(200, pixels.length);
                exchange.getResponseBody().write(pixels);
            } finally { exchange.close(); }
        });
        secure.createContext("/redirect", exchange -> {
            exchange.getResponseHeaders().set("Location", "http://127.0.0.1:" + plain.getAddress().getPort() + "/image");
            exchange.sendResponseHeaders(302, -1); exchange.close();
        });
        plain.start(); secure.start();
        try (var client = MockVitImage.httpClientBuilder().sslContext(ssl).build()) {
            var response = client.send(HttpRequest.newBuilder(URI.create("https://127.0.0.1:"
                            + secure.getAddress().getPort() + "/redirect")).timeout(Duration.ofSeconds(5)).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, response.statusCode());
            assertArrayEquals(pixels, response.body());
        } finally { secure.stop(0); plain.stop(0); }
    }
}
