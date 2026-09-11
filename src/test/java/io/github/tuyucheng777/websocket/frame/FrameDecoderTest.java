package io.github.tuyucheng777.websocket.frame;

import io.github.tuyucheng777.websocket.support.WireFrames;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Unit tests for {@link FrameDecoder}: unmasking, fragment reassembly, cross-read boundaries,
/// interleaved control frames, and all key protocol violations.
class FrameDecoderTest {

    /// Test listener that records every callback.
    private static final class RecordingListener implements FrameListener {
        final List<String> texts = new ArrayList<>();
        final List<byte[]> binaries = new ArrayList<>();
        final List<byte[]> pings = new ArrayList<>();
        final List<byte[]> pongs = new ArrayList<>();
        final List<int[]> closes = new ArrayList<>();
        final List<String> closeReasons = new ArrayList<>();
        final List<Integer> errorCodes = new ArrayList<>();

        @Override
        public void onTextMessage(ByteBuffer payload) {
            byte[] data = new byte[payload.remaining()];
            payload.get(data);
            texts.add(new String(data, StandardCharsets.UTF_8));
        }

        @Override
        public void onBinaryMessage(ByteBuffer payload) {
            byte[] data = new byte[payload.remaining()];
            payload.get(data);
            binaries.add(data);
        }

        @Override
        public void onPing(ByteBuffer payload) {
            pings.add(copy(payload));
        }

        @Override
        public void onPong(ByteBuffer payload) {
            pongs.add(copy(payload));
        }

        @Override
        public void onClose(int statusCode, String reason) {
            closes.add(new int[]{statusCode});
            closeReasons.add(reason);
        }

        @Override
        public void onProtocolError(int closeStatus, String message) {
            errorCodes.add(closeStatus);
        }

        private static byte[] copy(ByteBuffer buffer) {
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            return data;
        }
    }

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static FrameDecoder decoder(RecordingListener listener) {
        return new FrameDecoder(listener, 1L << 20);
    }

    private static FrameDecoder decoder(RecordingListener listener, long maxPayload) {
        return new FrameDecoder(listener, maxPayload);
    }

    @Test
    void decodesMaskedTextFrame() {
        var listener = new RecordingListener();
        byte[] key = {0x37, (byte) 0xFA, 0x21, 0x3D};
        byte[] wire = WireFrames.maskedWithKey(key, Opcode.TEXT, utf8("Hello"), true);

        decoder(listener).feed(ByteBuffer.wrap(wire));

        assertEquals(List.of("Hello"), listener.texts);
    }

    @Test
    void decodesEmptyTextFrame() {
        var listener = new RecordingListener();
        decoder(listener).feed(ByteBuffer.wrap(
                WireFrames.masked(Opcode.TEXT, new byte[0], true)));

        assertEquals(List.of(""), listener.texts);
    }

    @Test
    void decodesBinaryFrame() {
        var listener = new RecordingListener();
        byte[] data = {1, 2, 3, 4, 5};
        decoder(listener).feed(ByteBuffer.wrap(
                WireFrames.masked(Opcode.BINARY, data, true)));

        assertEquals(1, listener.binaries.size());
        assertArrayEquals(data, listener.binaries.getFirst());
    }

    @Test
    void decodes16BitLengthPayload() {
        var listener = new RecordingListener();
        byte[] data = new byte[200];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        decoder(listener).feed(ByteBuffer.wrap(
                WireFrames.masked(Opcode.BINARY, data, true)));

        assertArrayEquals(data, listener.binaries.getFirst());
    }

    @Test
    void decodes64BitLengthPayload() {
        var listener = new RecordingListener();
        byte[] data = new byte[70_000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 251);
        }
        decoder(listener).feed(ByteBuffer.wrap(
                WireFrames.masked(Opcode.BINARY, data, true)));

        assertArrayEquals(data, listener.binaries.getFirst());
    }

    @Test
    void reassemblesFragmentedMessage() {
        var listener = new RecordingListener();
        FrameDecoder decoder = decoder(listener);

        decoder.feed(ByteBuffer.wrap(WireFrames.masked(Opcode.TEXT, utf8("Hel"), false)));
        decoder.feed(ByteBuffer.wrap(WireFrames.masked(Opcode.CONTINUATION, utf8("lo"), true)));

        assertEquals(List.of("Hello"), listener.texts);
    }

    @Test
    void reassemblesThreeFragmentsWithControlFrameInterleaved() {
        var listener = new RecordingListener();
        FrameDecoder decoder = decoder(listener);

        decoder.feed(ByteBuffer.wrap(WireFrames.masked(Opcode.TEXT, utf8("a"), false)));
        decoder.feed(ByteBuffer.wrap(WireFrames.masked(Opcode.PING, utf8("x"), true)));
        decoder.feed(ByteBuffer.wrap(WireFrames.masked(Opcode.CONTINUATION, utf8("b"), false)));
        decoder.feed(ByteBuffer.wrap(WireFrames.masked(Opcode.CONTINUATION, utf8("c"), true)));

        assertEquals(List.of("abc"), listener.texts);
        assertEquals(1, listener.pings.size());
        assertArrayEquals(utf8("x"), listener.pings.getFirst());
    }

    @Test
    void decodesTwoFramesInOneBuffer() {
        var listener = new RecordingListener();
        byte[] first = WireFrames.masked(Opcode.TEXT, utf8("one"), true);
        byte[] second = WireFrames.masked(Opcode.TEXT, utf8("two"), true);
        byte[] combined = new byte[first.length + second.length];
        System.arraycopy(first, 0, combined, 0, first.length);
        System.arraycopy(second, 0, combined, first.length, second.length);

        decoder(listener).feed(ByteBuffer.wrap(combined));

        assertEquals(List.of("one", "two"), listener.texts);
    }

    @Test
    void decodesFrameFedOneByteAtATime() {
        var listener = new RecordingListener();
        FrameDecoder decoder = decoder(listener);
        byte[] wire = WireFrames.masked(Opcode.BINARY, new byte[]{9, 8, 7}, true);

        for (byte b : wire) {
            decoder.feed(ByteBuffer.wrap(new byte[]{b}));
        }

        assertArrayEquals(new byte[]{9, 8, 7}, listener.binaries.getFirst());
    }

    @Test
    void decodesCloseWithStatusAndReason() {
        var listener = new RecordingListener();
        byte[] payload = ByteBuffer.allocate(5)
                .putShort((short) 1000).put(utf8("bye")).array();

        decoder(listener).feed(ByteBuffer.wrap(
                WireFrames.masked(Opcode.CLOSE, payload, true)));

        assertEquals(1000, listener.closes.getFirst()[0]);
        assertEquals("bye", listener.closeReasons.getFirst());
    }

    @Test
    void decodesCloseWithoutStatusAs1005() {
        var listener = new RecordingListener();
        decoder(listener).feed(ByteBuffer.wrap(
                WireFrames.masked(Opcode.CLOSE, new byte[0], true)));

        assertEquals(CloseStatus.NO_STATUS_CODE, listener.closes.getFirst()[0]);
        assertEquals("", listener.closeReasons.getFirst());
    }

    @Test
    void rejectsInvalidUtf8InTextFrame() {
        var listener = new RecordingListener();
        // 0xFF 0xFE cannot be the start of any UTF-8 sequence.
        byte[] bad = {(byte) 0xFF, (byte) 0xFE};
        decoder(listener).feed(ByteBuffer.wrap(
                WireFrames.masked(Opcode.TEXT, bad, true)));

        assertEquals(List.of(CloseStatus.INVALID_PAYLOAD_DATA), listener.errorCodes);
        assertTrue(listener.texts.isEmpty());
    }

    @Test
    void rejectsInvalidUtf8AcrossFragments() {
        var listener = new RecordingListener();
        FrameDecoder decoder = decoder(listener);
        byte[] first = {(byte) 0xC3}; // start of a two-byte sequence
        byte[] last = {'x'};          // illegal continuation byte

        decoder.feed(ByteBuffer.wrap(WireFrames.masked(Opcode.TEXT, first, false)));
        decoder.feed(ByteBuffer.wrap(WireFrames.masked(Opcode.CONTINUATION, last, true)));

        assertEquals(List.of(CloseStatus.INVALID_PAYLOAD_DATA), listener.errorCodes);
    }

    @Test
    void rejectsUnmaskedClientFrame() {
        var listener = new RecordingListener();
        decoder(listener).feed(ByteBuffer.wrap(
                WireFrames.unmasked(Opcode.TEXT, utf8("x"), true)));

        assertEquals(List.of(CloseStatus.PROTOCOL_ERROR), listener.errorCodes);
    }

    @Test
    void rejectsNonZeroRsvBits() {
        var listener = new RecordingListener();
        byte[] wire = WireFrames.frame(true, true, Opcode.TEXT, utf8("x"),
                new byte[]{1, 2, 3, 4});
        decoder(listener).feed(ByteBuffer.wrap(wire));

        assertEquals(List.of(CloseStatus.PROTOCOL_ERROR), listener.errorCodes);
    }

    @Test
    void rejectsReservedOpcode() {
        var listener = new RecordingListener();
        decoder(listener).feed(ByteBuffer.wrap(
                WireFrames.masked(0x3, utf8("x"), true)));

        assertEquals(List.of(CloseStatus.PROTOCOL_ERROR), listener.errorCodes);
    }

    @Test
    void rejectsFragmentedControlFrame() {
        var listener = new RecordingListener();
        decoder(listener).feed(ByteBuffer.wrap(
                WireFrames.masked(Opcode.PING, utf8("x"), false)));

        assertEquals(List.of(CloseStatus.PROTOCOL_ERROR), listener.errorCodes);
    }

    @Test
    void rejectsControlFrameWith16BitLengthMarker() {
        var listener = new RecordingListener();
        FrameDecoder decoder = decoder(listener);
        byte[] key = {1, 2, 3, 4};
        // Ping + masked + length marker 126 (only the 2-byte length and masking key follow).
        ByteBuffer head = ByteBuffer.allocate(4 + 4)
                .put(new byte[]{(byte) 0x89, (byte) (0x80 | 126)})
                .putShort((short) 200)
                .put(key);
        decoder.feed(ByteBuffer.wrap(head.array()));

        assertEquals(List.of(CloseStatus.PROTOCOL_ERROR), listener.errorCodes);
    }

    @Test
    void rejectsContinuationWithoutStartFrame() {
        var listener = new RecordingListener();
        decoder(listener).feed(ByteBuffer.wrap(
                WireFrames.masked(Opcode.CONTINUATION, utf8("x"), true)));

        assertEquals(List.of(CloseStatus.PROTOCOL_ERROR), listener.errorCodes);
    }

    @Test
    void rejectsNewStartBeforePreviousMessageFinishes() {
        var listener = new RecordingListener();
        FrameDecoder decoder = decoder(listener);
        decoder.feed(ByteBuffer.wrap(WireFrames.masked(Opcode.TEXT, utf8("a"), false)));
        decoder.feed(ByteBuffer.wrap(WireFrames.masked(Opcode.TEXT, utf8("b"), false)));

        assertEquals(List.of(CloseStatus.PROTOCOL_ERROR), listener.errorCodes);
    }

    @Test
    void rejectsCloseFrameWithOneBytePayload() {
        var listener = new RecordingListener();
        decoder(listener).feed(ByteBuffer.wrap(
                WireFrames.masked(Opcode.CLOSE, new byte[]{0x03}, true)));

        assertEquals(List.of(CloseStatus.PROTOCOL_ERROR), listener.errorCodes);
    }

    @Test
    void rejectsCloseFrameWithForbiddenStatusCode() {
        var listener = new RecordingListener();
        byte[] payload = ByteBuffer.allocate(2).putShort((short) 1005).array();
        decoder(listener).feed(ByteBuffer.wrap(
                WireFrames.masked(Opcode.CLOSE, payload, true)));

        assertEquals(List.of(CloseStatus.PROTOCOL_ERROR), listener.errorCodes);
    }

    @Test
    void rejectsOversizedPayloadWith1009() {
        var listener = new RecordingListener();
        FrameDecoder decoder = decoder(listener, 16);
        decoder.feed(ByteBuffer.wrap(
                WireFrames.masked(Opcode.BINARY, new byte[17], true)));

        assertEquals(List.of(CloseStatus.MESSAGE_TOO_BIG), listener.errorCodes);
    }

    @Test
    void stopsProcessingAfterProtocolError() {
        var listener = new RecordingListener();
        FrameDecoder decoder = decoder(listener);

        decoder.feed(ByteBuffer.wrap(
                WireFrames.unmasked(Opcode.TEXT, utf8("x"), true)));
        decoder.feed(ByteBuffer.wrap(
                WireFrames.masked(Opcode.TEXT, utf8("y"), true)));

        assertTrue(listener.texts.isEmpty());
        assertEquals(1, listener.errorCodes.size());
    }

    @Test
    void ignoresDataAfterCloseFrame() {
        var listener = new RecordingListener();
        FrameDecoder decoder = decoder(listener);
        byte[] close = WireFrames.masked(Opcode.CLOSE, new byte[0], true);
        byte[] text = WireFrames.masked(Opcode.TEXT, utf8("late"), true);
        byte[] combined = new byte[close.length + text.length];
        System.arraycopy(close, 0, combined, 0, close.length);
        System.arraycopy(text, 0, combined, close.length, text.length);

        decoder.feed(ByteBuffer.wrap(combined));

        assertEquals(1, listener.closes.size());
        assertTrue(listener.texts.isEmpty());
    }
}
