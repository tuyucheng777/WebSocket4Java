package io.github.tuyucheng777.websocket.support;

import java.nio.ByteBuffer;
import java.security.SecureRandom;

/// Test helper: hand-crafts frame bytes in RFC 6455 wire format without depending on the encoder
/// under test, so it can cross-validate the encoder/decoder and deliberately build illegal frames.
public final class WireFrames {

    private static final SecureRandom RANDOM = new SecureRandom();

    private WireFrames() {
    }

    /// Builds a masked data frame (simulating a browser client) with a random masking key.
    ///
    /// @param opcode  opcode
    /// @param payload payload
    /// @param fin     whether this is the final frame
    /// @return wire bytes
    public static byte[] masked(int opcode, byte[] payload, boolean fin) {
        byte[] key = new byte[4];
        RANDOM.nextBytes(key);
        return frame(fin, false, opcode, payload, key);
    }

    /// Builds a frame with a specific masking key, convenient for assertions.
    public static byte[] maskedWithKey(byte[] key, int opcode, byte[] payload, boolean fin) {
        return frame(fin, false, opcode, payload, key);
    }

    /// Builds an unmasked frame (server-to-client direction; an illegal client frame for the decoder).
    public static byte[] unmasked(int opcode, byte[] payload, boolean fin) {
        return frame(fin, false, opcode, payload, null);
    }

    /// Generic frame builder.
    ///
    /// @param fin     FIN bit
    /// @param rsv1    RSV1 bit
    /// @param opcode  raw opcode value
    /// @param payload payload
    /// @param maskKey 4-byte masking key; {@code null} means unmasked
    /// @return wire bytes
    public static byte[] frame(boolean fin, boolean rsv1, int opcode, byte[] payload, byte[] maskKey) {
        int length = payload.length;
        int extended = length <= 125 ? 0 : length <= 0xFFFF ? 2 : 8;
        int total = 2 + extended + (maskKey == null ? 0 : 4) + length;
        ByteBuffer buffer = ByteBuffer.allocate(total);

        buffer.put((byte) ((fin ? 0x80 : 0x00) | (rsv1 ? 0x40 : 0x00) | (opcode & 0x0F)));
        int shortLength = length <= 125 ? length : length <= 0xFFFF ? 126 : 127;
        buffer.put((byte) ((maskKey == null ? 0x00 : 0x80) | shortLength));
        if (extended == 2) {
            buffer.putShort((short) length);
        } else if (extended == 8) {
            buffer.putLong(length);
        }
        if (maskKey != null) {
            buffer.put(maskKey);
            for (int i = 0; i < length; i++) {
                buffer.put((byte) (payload[i] ^ maskKey[i & 3]));
            }
        } else {
            buffer.put(payload);
        }
        return buffer.array();
    }

    /// Builds a raw 2-byte frame header (used for exceptional cases such as control frames with an illegal length).
    public static byte[] rawHeader(boolean fin, boolean mask, int opcode, int length7) {
        return new byte[]{
                (byte) ((fin ? 0x80 : 0x00) | (opcode & 0x0F)),
                (byte) ((mask ? 0x80 : 0x00) | (length7 & 0x7F))
        };
    }
}
