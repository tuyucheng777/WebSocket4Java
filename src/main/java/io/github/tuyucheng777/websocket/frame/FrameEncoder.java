package io.github.tuyucheng777.websocket.frame;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

/// WebSocket frame encoder (RFC 6455, sections 5.2 and 5.3).
///
/// The encoder outputs two {@link ByteBuffer}s, **frame header + payload**, which can be handed directly to
/// {@link java.nio.channels.SocketChannel#write(ByteBuffer[])} for a gathering write.
/// Server sends are not masked and the payload is zero-copy; client sends are masked by copying the payload once and
/// XORing it in place, without modifying the buffer passed in by the caller.
public final class FrameEncoder {

    /// Random number generator used to generate client masking keys.
    private static final SecureRandom RANDOM = new SecureRandom();

    /// Maximum frame header length: 2-byte fixed header + 8-byte extended length + 4-byte masking key.
    public static final int MAX_HEADER_SIZE = 14;

    private FrameEncoder() {
    }

    /// Encodes an arbitrary frame.
    ///
    /// The position and limit of the payload buffer determine the effective payload range, and this method
    /// does not change its position (a slice is used internally).
    ///
    /// @param fin     whether this is the last fragment of the message
    /// @param opcode  opcode, see {@link Opcode}
    /// @param payload payload; an empty buffer is allowed
    /// @param masked  whether to write a mask (must be {@code true} for clients and {@code false} for servers)
    /// @return a buffer array of length 1 or 2: the frame header is always returned, followed by the payload when it is non-empty
    public static ByteBuffer[] encode(boolean fin, int opcode, ByteBuffer payload, boolean masked) {
        long length = payload == null ? 0 : payload.remaining();
        if (Opcode.isControl(opcode) && length > 125) {
            throw new IllegalArgumentException("control frame payload must not exceed 125 bytes: " + length);
        }
        int headerLength = 2 + (length <= 125 ? 0 : length <= 0xFFFF ? 2 : 8) + (masked ? 4 : 0);
        ByteBuffer header = ByteBuffer.allocate(headerLength);

        int firstByte = (fin ? 0x80 : 0x00) | (opcode & 0x0F);
        header.put((byte) firstByte);

        int maskBit = masked ? 0x80 : 0x00;
        if (length <= 125) {
            header.put((byte) (maskBit | (int) length));
        } else if (length <= 0xFFFF) {
            header.put((byte) (maskBit | 126));
            header.putShort((short) length);
        } else {
            header.put((byte) (maskBit | 127));
            header.putLong(length);
        }

        ByteBuffer body;
        if (masked) {
            byte[] key = new byte[4];
            RANDOM.nextBytes(key);
            header.put(key);
            byte[] data = new byte[(int) length];
            payload.duplicate().get(data);
            for (int i = 0; i < data.length; i++) {
                data[i] ^= key[i & 3];
            }
            body = ByteBuffer.wrap(data);
        } else {
            body = payload == null ? ByteBuffer.allocate(0) : payload.slice();
        }
        header.flip();

        return body.hasRemaining() ? new ByteBuffer[]{header, body} : new ByteBuffer[]{header};
    }

    /// Encodes a text frame (server side, not masked).
    ///
    /// @param text text content
    /// @return buffer array for a gathering write
    public static ByteBuffer[] text(String text) {
        return encode(true, Opcode.TEXT, ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)), false);
    }

    /// Encodes a binary frame (server side, not masked).
    ///
    /// @param data binary payload
    /// @return buffer array for a gathering write
    public static ByteBuffer[] binary(ByteBuffer data) {
        return encode(true, Opcode.BINARY, data, false);
    }

    /// Encodes a message fragment (server side, not masked).
    ///
    /// The fragment sequence must start with {@link Opcode#TEXT} or {@link Opcode#BINARY},
    /// use {@link Opcode#CONTINUATION} for every following fragment, and set {@code fin = true} on the last fragment.
    ///
    /// @param opcode  fragment opcode
    /// @param payload fragment payload
    /// @param fin     whether this is the last fragment
    /// @return buffer array for a gathering write
    public static ByteBuffer[] fragment(int opcode, ByteBuffer payload, boolean fin) {
        if (opcode != Opcode.TEXT && opcode != Opcode.BINARY && opcode != Opcode.CONTINUATION) {
            throw new IllegalArgumentException("invalid fragment opcode: " + Opcode.nameOf(opcode));
        }
        return encode(fin, opcode, payload, false);
    }

    /// Encodes a Ping frame (server side, not masked).
    ///
    /// @param payload payload; its length must not exceed 125
    /// @return buffer array for a gathering write
    public static ByteBuffer[] ping(ByteBuffer payload) {
        return encode(true, Opcode.PING, payload, false);
    }

    /// Encodes a Pong frame (server side, not masked).
    ///
    /// @param payload payload; its length must not exceed 125
    /// @return buffer array for a gathering write
    public static ByteBuffer[] pong(ByteBuffer payload) {
        return encode(true, Opcode.PONG, payload, false);
    }

    /// Encodes a close frame with a status code and reason (server side, not masked).
    ///
    /// @param statusCode close status code; it must be sendable on the wire, see {@link CloseStatus#isSendable}
    /// @param reason     close reason; may be {@code null}
    /// @return buffer array for a gathering write
    public static ByteBuffer[] close(int statusCode, String reason) {
        if (!CloseStatus.isSendable(statusCode)) {
            throw new IllegalArgumentException("invalid close status code: " + statusCode);
        }
        byte[] reasonBytes = reason == null ? new byte[0] : reason.getBytes(StandardCharsets.UTF_8);
        if (reasonBytes.length > 123) {
            throw new IllegalArgumentException("close reason is too long: " + reasonBytes.length);
        }
        ByteBuffer payload = ByteBuffer.allocate(2 + reasonBytes.length);
        payload.putShort((short) statusCode);
        payload.put(reasonBytes);
        payload.flip();
        return encode(true, Opcode.CLOSE, payload, false);
    }

    /// Encodes an empty close frame without a status code (server side, not masked).
    ///
    /// @return buffer array for a gathering write
    public static ByteBuffer[] closeEmpty() {
        return encode(true, Opcode.CLOSE, ByteBuffer.allocate(0), false);
    }
}
