package io.github.tuyucheng777.websocket.http;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Unit tests for {@link HttpParser}: request line, headers, query string parsing, and malformed request rejection.
class HttpParserTest {

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void parsesBasicUpgradeRequest() throws HandshakeException {
        String raw = """
                GET /echo HTTP/1.1\r
                Host: localhost:8080\r
                Upgrade: websocket\r
                Connection: Upgrade\r
                Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r
                Sec-WebSocket-Version: 13\r
                """;
        byte[] data = bytes(raw);

        HttpRequest request = HttpParser.parse(data, data.length);

        assertEquals("GET", request.method());
        assertEquals("/echo", request.path());
        assertEquals("/echo", request.target());
        assertEquals("websocket", request.header("upgrade"));
        assertEquals("13", request.header("sec-websocket-version"));
        assertEquals("dGhlIHNhbXBsZSBub25jZQ==", request.header("Sec-WebSocket-Key"));
    }

    @Test
    void parsesQueryParameters() throws HandshakeException {
        String raw = "GET /chat/42?user=alice&room=java HTTP/1.1\r\nHost: x\r\n\r\n";
        byte[] data = bytes(raw);

        HttpRequest request = HttpParser.parse(data, data.length);

        assertEquals("/chat/42", request.path());
        assertEquals("alice", request.query("user"));
        assertEquals("java", request.query("room"));
    }

    @Test
    void decodesPercentEncodedQueryIncludingMultibyteUtf8() throws HandshakeException {
        // percent-encoded UTF-8 of café: %63%61%66%C3%A9
        String raw = "GET /echo?name=%63%61%66%C3%A9&a=b%20c HTTP/1.1\r\nHost: x\r\n\r\n";
        byte[] data = bytes(raw);

        HttpRequest request = HttpParser.parse(data, data.length);

        assertEquals("café", request.query("name"));
        assertEquals("b c", request.query("a"));
    }

    @Test
    void keepsPlusCharacterInQuery() throws HandshakeException {
        String raw = "GET /echo?q=a+b HTTP/1.1\r\nHost: x\r\n\r\n";
        byte[] data = bytes(raw);

        HttpRequest request = HttpParser.parse(data, data.length);
        assertEquals("a+b", request.query("q"));
    }

    @Test
    void parsesRepeatedHeadersAsList() throws HandshakeException {
        String raw = "GET / HTTP/1.1\r\nX-Multi: one\r\nX-Multi: two\r\n\r\n";
        byte[] data = bytes(raw);

        HttpRequest request = HttpParser.parse(data, data.length);

        assertArrayEquals(new String[]{"one", "two"},
                request.headers().get("x-multi").toArray(String[]::new));
    }

    @Test
    void rejectsMalformedRequestLine() {
        byte[] data = bytes("GARBAGE\r\n\r\n");
        HandshakeException error = assertThrows(HandshakeException.class,
                () -> HttpParser.parse(data, data.length));
        assertEquals(400, error.httpStatus());
    }

    @Test
    void rejectsNonGetMethod() {
        byte[] data = bytes("POST / HTTP/1.1\r\n\r\n");
        assertEquals(405, assertThrows(HandshakeException.class,
                () -> HttpParser.parse(data, data.length)).httpStatus());
    }

    @Test
    void rejectsHttpVersion10() {
        byte[] data = bytes("GET / HTTP/1.0\r\n\r\n");
        assertEquals(400, assertThrows(HandshakeException.class,
                () -> HttpParser.parse(data, data.length)).httpStatus());
    }

    @Test
    void rejectsHeaderWithoutColon() {
        byte[] data = bytes("GET / HTTP/1.1\r\nBadHeader\r\n\r\n");
        assertEquals(400, assertThrows(HandshakeException.class,
                () -> HttpParser.parse(data, data.length)).httpStatus());
    }

    @Test
    void rejectsBadPercentEscape() {
        byte[] data = bytes("GET /?x=%zz HTTP/1.1\r\n\r\n");
        assertEquals(400, assertThrows(HandshakeException.class,
                () -> HttpParser.parse(data, data.length)).httpStatus());
    }

    @Test
    void locatesHeadTerminatorAndIgnoresBodyBytes() throws HandshakeException {
        byte[] head = bytes("GET / HTTP/1.1\r\nHost: x\r\n\r\n");
        byte[] trailing = bytes("ignored-bytes");
        byte[] all = new byte[head.length + trailing.length];
        System.arraycopy(head, 0, all, 0, head.length);
        System.arraycopy(trailing, 0, all, head.length, trailing.length);

        int end = HttpParser.indexOfHeadEnd(all, all.length);
        assertEquals(head.length - 4, end);

        HttpRequest request = HttpParser.parse(all, end);
        assertEquals("/", request.path());
    }
}
