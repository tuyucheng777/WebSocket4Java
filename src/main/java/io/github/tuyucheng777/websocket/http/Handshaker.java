package io.github.tuyucheng777.websocket.http;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Locale;

/// WebSocket handshake handler (RFC 6455 Section 4.2).
///
/// Validates the mandatory headers of the upgrade request, computes {@code Sec-WebSocket-Accept},
/// and builds the {@code 101 Switching Protocols} response and ordinary HTTP error responses.
public final class Handshaker {

    /// Handshake GUID defined by RFC 6455, appended to the client nonce for the SHA-1 digest.
    public static final String GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    /// The only supported WebSocket protocol version.
    public static final String SUPPORTED_VERSION = "13";

    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();
    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

    private Handshaker() {
    }

    /// Computes the {@code Sec-WebSocket-Accept} response header.
    ///
    /// @param nonce the {@code Sec-WebSocket-Key} sent by the client
    /// @return Base64-encoded SHA-1 digest
    /// @throws IllegalArgumentException if nonce is not valid 16-byte Base64 data
    public static String acceptKey(String nonce) {
        byte[] decoded;
        try {
            decoded = BASE64_DECODER.decode(nonce);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Sec-WebSocket-Key is not valid Base64: " + nonce, e);
        }
        if (decoded.length != 16) {
            throw new IllegalArgumentException(
                    "Sec-WebSocket-Key must decode to 16 bytes, but was " + decoded.length + " bytes");
        }
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] digest = sha1.digest((nonce + GUID).getBytes(StandardCharsets.ISO_8859_1));
            return BASE64_ENCODER.encodeToString(digest);
        } catch (NoSuchAlgorithmException e) {
            // SHA-1 is an algorithm that every JDK is required to provide; theoretically unreachable.
            throw new IllegalStateException("current JDK does not support SHA-1", e);
        }
    }

    /// Validates the upgrade request per RFC 6455 Section 4.2.1.
    ///
    /// @param request the parsed HTTP request
    /// @throws HandshakeException if a required header is missing or has an invalid value
    public static void validate(HttpRequest request) throws HandshakeException {
        if (!containsToken(request.header("Upgrade"), "websocket")) {
            throw new HandshakeException(400, "missing or invalid Upgrade: websocket header");
        }
        if (!containsToken(request.header("Connection"), "upgrade")) {
            throw new HandshakeException(400, "missing or invalid Connection: Upgrade header");
        }
        String version = request.header("Sec-WebSocket-Version");
        if (version == null || !version.trim().equals(SUPPORTED_VERSION)) {
            throw new HandshakeException(426, "only WebSocket version 13 is supported");
        }
        String key = request.header("Sec-WebSocket-Key");
        if (key == null) {
            throw new HandshakeException(400, "missing Sec-WebSocket-Key header");
        }
        try {
            acceptKey(key.trim());
        } catch (IllegalArgumentException e) {
            throw new HandshakeException(400, e.getMessage());
        }
    }

    /// Checks whether the comma-separated token list in a header value contains the target token (case-insensitive).
    private static boolean containsToken(String headerValue, String token) {
        if (headerValue == null) {
            return false;
        }
        for (String part : headerValue.split(",")) {
            if (part.trim().toLowerCase(Locale.ROOT).equals(token)) {
                return true;
            }
        }
        return false;
    }

    /// Builds the bytes of the 101 upgrade response.
    ///
    /// @param request the validated handshake request
    /// @return response bytes ready to be written back to the client
    /// @throws HandshakeException if the request is invalid (theoretically {@link #validate} has already rejected it)
    public static byte[] upgradeResponse(HttpRequest request) throws HandshakeException {
        String key = request.header("Sec-WebSocket-Key").trim();
        String accept = acceptKey(key);
        return ("HTTP/1.1 101 Switching Protocols\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Accept: " + accept + "\r\n"
                + "\r\n").getBytes(StandardCharsets.ISO_8859_1);
    }

    /// Builds an ordinary HTTP error response; the connection should be closed immediately after writing it.
    ///
    /// @param statusCode HTTP status code
    /// @param reason     reason phrase
    /// @return response bytes ready to be written back to the client
    public static byte[] errorResponse(int statusCode, String reason) {
        String body = reason + "\n";
        byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        return ("HTTP/1.1 " + statusCode + " " + reason + "\r\n"
                + "Content-Type: text/plain; charset=utf-8\r\n"
                + "Content-Length: " + bodyBytes.length + "\r\n"
                + "Connection: close\r\n"
                + "\r\n").getBytes(StandardCharsets.ISO_8859_1);
    }
}
