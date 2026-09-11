package io.github.tuyucheng777.websocket.http;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Unit tests for {@link Handshaker}: RFC 6455 digest vector, mandatory header validation, and response construction.
class HandshakerTest {

    /// The standard test vector given in RFC 6455 section 4.2.2.
    @Test
    void computesRfcVectorAcceptKey() {
        String accept = Handshaker.acceptKey("dGhlIHNhbXBsZSBub25jZQ==");
        assertEquals("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", accept);
    }

    @Test
    void acceptKeyIsDeterministic() {
        String nonce = Base64.getEncoder().encodeToString(new byte[16]);
        assertEquals(Handshaker.acceptKey(nonce), Handshaker.acceptKey(nonce));
    }

    @Test
    void rejectsKeyWithWrongDecodedLength() {
        String nonce = Base64.getEncoder().encodeToString(new byte[15]);
        assertThrows(IllegalArgumentException.class, () -> Handshaker.acceptKey(nonce));
    }

    @Test
    void rejectsKeyThatIsNotBase64() {
        assertThrows(IllegalArgumentException.class,
                () -> Handshaker.acceptKey("not-base64!!"));
    }

    private static HttpRequest requestWith(String upgrade, String connection,
                                           String version, String key) {
        Map<String, List<String>> headers = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (upgrade != null) {
            headers.put("Upgrade", new ArrayList<>(List.of(upgrade)));
        }
        if (connection != null) {
            headers.put("Connection", new ArrayList<>(List.of(connection)));
        }
        if (version != null) {
            headers.put("Sec-WebSocket-Version", new ArrayList<>(List.of(version)));
        }
        if (key != null) {
            headers.put("Sec-WebSocket-Key", new ArrayList<>(List.of(key)));
        }
        return new HttpRequest("GET", "/echo", "/echo", Map.of(), Map.copyOf(headers));
    }

    private static HttpRequest validRequest() {
        return requestWith("websocket", "Upgrade", "13",
                Base64.getEncoder().encodeToString(new byte[16]));
    }

    @Test
    void acceptsValidUpgradeRequest() throws HandshakeException {
        Handshaker.validate(validRequest());
    }

    @Test
    void acceptsTokenListedConnectionHeader() throws HandshakeException {
        // Common browser form: Connection: keep-alive, Upgrade
        Handshaker.validate(requestWith("websocket", "keep-alive, Upgrade", "13",
                Base64.getEncoder().encodeToString(new byte[16])));
    }

    @Test
    void rejectsMissingUpgradeHeader() {
        HttpRequest request = requestWith(null, "Upgrade", "13",
                Base64.getEncoder().encodeToString(new byte[16]));
        HandshakeException error = assertThrows(HandshakeException.class,
                () -> Handshaker.validate(request));
        assertEquals(400, error.httpStatus());
    }

    @Test
    void rejectsWrongUpgradeToken() {
        HttpRequest request = requestWith("h2c", "Upgrade", "13",
                Base64.getEncoder().encodeToString(new byte[16]));
        assertEquals(400, assertThrows(HandshakeException.class,
                () -> Handshaker.validate(request)).httpStatus());
    }

    @Test
    void rejectsWrongVersionWith426() {
        HttpRequest request = requestWith("websocket", "Upgrade", "8",
                Base64.getEncoder().encodeToString(new byte[16]));
        assertEquals(426, assertThrows(HandshakeException.class,
                () -> Handshaker.validate(request)).httpStatus());
    }

    @Test
    void rejectsMissingKey() {
        HttpRequest request = requestWith("websocket", "Upgrade", "13", null);
        assertEquals(400, assertThrows(HandshakeException.class,
                () -> Handshaker.validate(request)).httpStatus());
    }

    @Test
    void buildsUpgradeResponseWithCorrectAccept() throws HandshakeException {
        HttpRequest request = new HttpRequest("GET", "/echo", "/echo", Map.of(),
                Map.of("Sec-WebSocket-Key", List.of("dGhlIHNhbXBsZSBub25jZQ=="),
                        "Upgrade", List.of("websocket")));

        byte[] response = Handshaker.upgradeResponse(request);
        String text = new String(response, StandardCharsets.ISO_8859_1);

        assertTrue(text.startsWith("HTTP/1.1 101 Switching Protocols\r\n"));
        assertTrue(text.contains("Upgrade: websocket\r\n"));
        assertTrue(text.contains("Connection: Upgrade\r\n"));
        assertTrue(text.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"));
        assertTrue(text.endsWith("\r\n\r\n"));
    }

    @Test
    void upgradeResponseRejectsMissingKeyWithoutPriorValidation() {
        HttpRequest request = new HttpRequest("GET", "/echo", "/echo", Map.of(),
                Map.of("Upgrade", List.of("websocket")));

        HandshakeException e = assertThrows(HandshakeException.class,
                () -> Handshaker.upgradeResponse(request));
        assertEquals(400, e.httpStatus());
    }

    @Test
    void buildsErrorResponse() {
        byte[] response = Handshaker.errorResponse(404, "Not Found");
        String text = new String(response, StandardCharsets.ISO_8859_1);

        assertTrue(text.startsWith("HTTP/1.1 404 Not Found\r\n"));
        assertTrue(text.contains("Content-Length: "));
        assertTrue(text.contains("Connection: close\r\n"));
    }
}
