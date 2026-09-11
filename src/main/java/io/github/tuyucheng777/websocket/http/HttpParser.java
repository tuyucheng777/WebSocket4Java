package io.github.tuyucheng777.websocket.http;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/// Minimal HTTP/1.1 request header parser, used solely for the WebSocket handshake.
///
/// It does not rely on high-level APIs such as {@code com.sun.net.httpserver}; per RFC 7230 it
/// splits the request line and headers directly; headers are decoded as ISO-8859-1 (the field value
/// encoding required by HTTP), and query parameters are percent-decoded per RFC 3986.
public final class HttpParser {

    /// Request head terminator {@code CRLF CRLF}.
    public static final byte[] HEAD_TERMINATOR = {'\r', '\n', '\r', '\n'};

    private HttpParser() {
    }

    /// Locates the request head terminator within a byte sequence.
    ///
    /// @param data   raw bytes
    /// @param length valid length
    /// @return the starting index of the terminator; {@code -1} if not found
    public static int indexOfHeadEnd(byte[] data, int length) {
        outer:
        for (int i = 0; i <= length - 4; i++) {
            for (int j = 0; j < 4; j++) {
                if (data[i + j] != HEAD_TERMINATOR[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    /// Parses the request head (excluding the trailing {@code CRLF CRLF}).
    ///
    /// @param data   raw bytes
    /// @param length valid length
    /// @return the parsed request
    /// @throws HandshakeException if the request format is invalid
    public static HttpRequest parse(byte[] data, int length) throws HandshakeException {
        String raw = new String(data, 0, length, java.nio.charset.StandardCharsets.ISO_8859_1);
        String[] lines = raw.split("\r\n", -1);
        if (lines.length == 0 || lines[0].isEmpty()) {
            throw new HandshakeException(400, "empty HTTP request");
        }

        String[] requestLine = lines[0].split(" ");
        if (requestLine.length != 3) {
            throw new HandshakeException(400, "invalid request line: " + lines[0]);
        }
        String method = requestLine[0];
        String target = requestLine[1];
        String version = requestLine[2];
        if (!method.equals("GET")) {
            throw new HandshakeException(405, "only GET is allowed for WebSocket handshake");
        }
        if (!version.equals("HTTP/1.1")) {
            throw new HandshakeException(400, "only HTTP/1.1 handshake is supported: " + version);
        }
        if (target.isEmpty() || target.charAt(0) != '/') {
            throw new HandshakeException(400, "invalid request target: " + target);
        }

        Map<String, List<String>> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i];
            if (line.isEmpty()) {
                continue;
            }
            int colon = line.indexOf(':');
            if (colon <= 0) {
                throw new HandshakeException(400, "invalid header line: " + line);
            }
            String name = line.substring(0, colon).trim();
            String value = line.substring(colon + 1).trim();
            if (name.isEmpty()) {
                throw new HandshakeException(400, "empty header name");
            }
            headers.computeIfAbsent(name, key -> new ArrayList<>()).add(value);
        }

        int question = target.indexOf('?');
        String path = question < 0 ? target : target.substring(0, question);
        String queryString = question < 0 ? "" : target.substring(question + 1);
        Map<String, String> queryParameters = parseQuery(queryString);

        Map<String, List<String>> immutableHeaders =
                java.util.Collections.unmodifiableMap(headers);
        return new HttpRequest(method, target, path,
                java.util.Collections.unmodifiableMap(queryParameters), immutableHeaders);
    }

    private static Map<String, String> parseQuery(String queryString) throws HandshakeException {
        Map<String, String> result = new LinkedHashMap<>();
        if (queryString.isEmpty()) {
            return result;
        }
        for (String pair : queryString.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int equals = pair.indexOf('=');
            String name = equals < 0 ? pair : pair.substring(0, equals);
            String value = equals < 0 ? "" : pair.substring(equals + 1);
            result.putIfAbsent(percentDecode(name), percentDecode(value));
        }
        return result;
    }

    /// RFC 3986 percent-decoding; invalid escapes are rejected with 400.
    ///
    /// Consecutive escaped bytes are grouped and then decoded as UTF-8, so multibyte characters are
    /// restored correctly even when fully percent-encoded; {@code '+'} is not a space in the RFC 3986 query component and is left as-is.
    private static String percentDecode(String value) throws HandshakeException {
        if (value.indexOf('%') < 0) {
            return value;
        }
        StringBuilder builder = new StringBuilder(value.length());
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        for (int i = 0; i < value.length(); ) {
            char c = value.charAt(i);
            if (c == '%') {
                if (i + 2 >= value.length()) {
                    throw new HandshakeException(400, "invalid percent-encoding: " + value);
                }
                int high = Character.digit(value.charAt(i + 1), 16);
                int low = Character.digit(value.charAt(i + 2), 16);
                if (high < 0 || low < 0) {
                    throw new HandshakeException(400, "invalid percent-encoding: " + value);
                }
                bytes.write((high << 4) | low);
                i += 3;
            } else {
                flushEscaped(builder, bytes);
                builder.append(c);
                i++;
            }
        }
        flushEscaped(builder, bytes);
        return builder.toString();
    }

    private static void flushEscaped(StringBuilder builder, java.io.ByteArrayOutputStream bytes) {
        if (bytes.size() > 0) {
            builder.append(bytes.toString(java.nio.charset.StandardCharsets.UTF_8));
            bytes.reset();
        }
    }
}
