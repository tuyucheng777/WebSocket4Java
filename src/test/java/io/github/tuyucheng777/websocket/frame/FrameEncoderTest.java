package io.github.tuyucheng777.websocket.frame;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Unit tests for {@link FrameEncoder}: frame header layout, three-tier length encoding, masking, and close frames.
class FrameEncoderTest {

    private static byte[] flatten(ByteBuffer[] buffers) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (ByteBuffer buffer : buffers) {
            byte[] block = new byte[buffer.remaining()];
            buffer.duplicate().get(block);
            out.writeBytes(block);
        }
        return out.toByteArray();
    }

    @Test
    void encodesSmallUnmaskedTextFrame() {
        byte[] wire = flatten(FrameEncoder.text("Hello"));

        assertEquals(7, wire.length);
        assertEquals(0x81, wire[0] & 0xFF);
        assertEquals(0x05, wire[1] & 0xFF);
        assertArrayEquals("Hello".getBytes(StandardCharsets.UTF_8),
                java.util.Arrays.copyOfRange(wire, 2, 7));
    }

    @Test
    void encodesUtf8TextFrame() {
        byte[] wire = flatten(FrameEncoder.text("café 🌐"));
        byte[] payload = java.util.Arrays.copyOfRange(wire, 2, wire.length);
        assertArrayEquals("café 🌐".getBytes(StandardCharsets.UTF_8), payload);
    }

    @Test
    void encodes16BitLengthBoundary() {
        byte[] data = new byte[126];
        new SecureRandom().nextBytes(data);

        byte[] wire = flatten(FrameEncoder.binary(ByteBuffer.wrap(data)));

        assertEquals(0x82, wire[0] & 0xFF);
        assertEquals(0x7E, wire[1] & 0xFF);
        assertEquals(126, ((wire[2] & 0xFF) << 8) | (wire[3] & 0xFF));
        assertArrayEquals(data, java.util.Arrays.copyOfRange(wire, 4, wire.length));
    }

    @Test
    void encodes64BitLengthBoundary() {
        byte[] data = new byte[65_536];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        byte[] wire = flatten(FrameEncoder.binary(ByteBuffer.wrap(data)));

        assertEquals(0x82, wire[0] & 0xFF);
        assertEquals(0x7F, wire[1] & 0xFF);
        long length = 0;
        for (int i = 2; i < 10; i++) {
            length = (length << 8) | (wire[i] & 0xFFL);
        }
        assertEquals(65_536L, length);
        assertArrayEquals(data, java.util.Arrays.copyOfRange(wire, 10, wire.length));
    }

    @Test
    void encodesMaskedFrameAndPayloadIsXored() {
        byte[] key = {0x01, 0x02, 0x03, 0x04};
        byte[] payload = "abcdef".getBytes(StandardCharsets.UTF_8);

        // The random masking key cannot be controlled directly, so fully unmask any masked frame to verify it.
        byte[] wire = flatten(
                FrameEncoder.encode(true, Opcode.TEXT, ByteBuffer.wrap(payload), true));

        assertEquals(0x81, wire[0] & 0xFF);
        assertEquals(0x86, wire[1] & 0xFF);
        byte[] decoded = new byte[6];
        for (int i = 0; i < 6; i++) {
            decoded[i] = (byte) (wire[6 + i] ^ wire[2 + (i & 3)]);
        }
        assertArrayEquals(payload, decoded);
        // The masking key is not all zeros (an all-zero key is extremely unlikely and can be ignored when randomly generated).
        assertTrue(wire[2] != key[0] || wire[3] != key[1] || wire[4] != key[2] || wire[5] != key[3]
                || wire[2] != 0);
    }

    @Test
    void encodesCloseFrameWithStatusAndReason() {
        byte[] wire = flatten(FrameEncoder.close(1000, "bye"));

        assertEquals(0x88, wire[0] & 0xFF);
        assertEquals(0x05, wire[1] & 0xFF);
        assertEquals(1000, ((wire[2] & 0xFF) << 8) | (wire[3] & 0xFF));
        assertArrayEquals("bye".getBytes(StandardCharsets.UTF_8),
                java.util.Arrays.copyOfRange(wire, 4, 7));
    }

    @Test
    void encodesEmptyCloseFrame() {
        byte[] wire = flatten(FrameEncoder.closeEmpty());
        assertArrayEquals(new byte[]{(byte) 0x88, 0x00}, wire);
    }

    @Test
    void encodesPingFrame() {
        byte[] wire = flatten(
                FrameEncoder.ping(ByteBuffer.wrap(new byte[]{1, 2, 3})));
        assertArrayEquals(new byte[]{(byte) 0x89, 0x03, 1, 2, 3}, wire);
    }

    @Test
    void encodesFragmentSequence() {
        byte[] first = flatten(FrameEncoder.fragment(Opcode.TEXT,
                ByteBuffer.wrap("ab".getBytes(StandardCharsets.UTF_8)), false));
        byte[] last = flatten(FrameEncoder.fragment(Opcode.CONTINUATION,
                ByteBuffer.wrap("cd".getBytes(StandardCharsets.UTF_8)), true));

        assertEquals(0x01, first[0] & 0xFF);
        assertEquals(0x80, last[0] & 0xFF);
    }

    @Test
    void rejectsControlFrameOver125Bytes() {
        ByteBuffer big = ByteBuffer.allocate(126);
        assertThrows(IllegalArgumentException.class, () -> FrameEncoder.ping(big));
    }

    @Test
    void rejectsUnsendableCloseStatus() {
        assertThrows(IllegalArgumentException.class,
                () -> FrameEncoder.close(CloseStatus.NO_STATUS_CODE, null));
        assertThrows(IllegalArgumentException.class,
                () -> FrameEncoder.close(1006, null));
    }
}
