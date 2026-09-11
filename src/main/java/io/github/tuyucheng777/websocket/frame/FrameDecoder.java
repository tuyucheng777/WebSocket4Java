package io.github.tuyucheng777.websocket.frame;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/// Stateful WebSocket frame decoder (RFC 6455, sections 5 and 7).
///
/// Each connection holds one instance. The decoder is responsible for:
///
/// - handling frame headers that arrive across multiple TCP reads (internal state machine);
/// - validating protocol rules such as the client mask, RSV bits, opcodes, and control frame boundaries;
/// - undoing the payload mask in place;
/// - reassembling cross-frame fragments; Ping/Pong frames may be interleaved between fragmented messages and delivered immediately;
/// - strictly validating text frame UTF-8 and close frame status codes;
/// - rejecting payloads that exceed the limit with {@link CloseStatus#MESSAGE_TOO_BIG}.
///
/// Decoding events are delivered via {@link FrameListener} callbacks, and violations are reported through
/// {@link FrameListener#onProtocolError}; after that the decoder is permanently disabled.
public final class FrameDecoder {

    private enum Stage {
        /// Read the 2-byte fixed frame header.
        HEAD,
        /// Read the 2-byte extended length (7-bit length 126).
        LENGTH16,
        /// Read the 8-byte extended length (7-bit length 127).
        LENGTH64,
        /// Read the 4-byte masking key.
        MASK,
        /// Read the payload.
        PAYLOAD
    }

    private final FrameListener listener;
    private final long maxPayloadSize;

    private Stage stage = Stage.HEAD;
    private final byte[] headerScratch = new byte[8];
    private int headerFilled;

    private boolean fin;
    private int opcode;
    private long payloadLength;
    private final byte[] maskingKey = new byte[4];
    private long payloadReceived;

    /// Whether a fragmented message is currently being assembled.
    private boolean messageActive;
    /// Opcode of the current message's initial frame (TEXT or BINARY).
    private int messageOpcode;
    private final MessageAccumulator message = new MessageAccumulator();

    /// A control frame payload never exceeds 125 bytes, so a fixed-length array is used directly.
    private byte[] controlPayload;

    /// Permanently stops parsing after a protocol error occurs.
    private boolean broken;
    /// After a close frame is received, all subsequent data is ignored (RFC 6455, section 5.5.1).
    private boolean closeReceived;

    /// Constructs a decoder.
    ///
    /// @param listener       listener for decoding events
    /// @param maxPayloadSize maximum payload size in bytes for a single message
    public FrameDecoder(FrameListener listener, long maxPayloadSize) {
        if (maxPayloadSize <= 0 || maxPayloadSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("maxPayloadSize must be between 1 and Integer.MAX_VALUE");
        }
        this.listener = listener;
        this.maxPayloadSize = maxPayloadSize;
    }

    /// Feeds in network bytes and parses as many frames as possible.
    ///
    /// The method consumes all parseable bytes of the buffer; incomplete headers/frames are kept in internal state
    /// to await the next feed.
    ///
    /// @param in network data (read mode)
    public void feed(ByteBuffer in) {
        while (in.hasRemaining() && !broken && !closeReceived) {
            switch (stage) {
                case HEAD -> readHead(in);
                case LENGTH16 -> readExtendedLength(in, 2);
                case LENGTH64 -> readExtendedLength(in, 8);
                case MASK -> readMask(in);
                case PAYLOAD -> readPayload(in);
            }
        }
    }

    private void readHead(ByteBuffer in) {
        while (headerFilled < 2 && in.hasRemaining()) {
            headerScratch[headerFilled++] = in.get();
        }
        if (headerFilled < 2) {
            return;
        }
        int byte0 = headerScratch[0] & 0xFF;
        int byte1 = headerScratch[1] & 0xFF;
        headerFilled = 0;

        fin = (byte0 & 0x80) != 0;
        if ((byte0 & 0x70) != 0) {
            fail(CloseStatus.PROTOCOL_ERROR, "RSV bits are non-zero but no extension was negotiated");
            return;
        }
        opcode = byte0 & 0x0F;
        if (!Opcode.isKnown(opcode)) {
            fail(CloseStatus.PROTOCOL_ERROR, "reserved opcode, cannot be processed: " + Opcode.nameOf(opcode));
            return;
        }
        if (Opcode.isControl(opcode) && !fin) {
            fail(CloseStatus.PROTOCOL_ERROR, "control frames must not be fragmented");
            return;
        }
        if ((byte1 & 0x80) == 0) {
            fail(CloseStatus.PROTOCOL_ERROR, "frames sent by the client must be masked");
            return;
        }
        int shortLength = byte1 & 0x7F;
        if (Opcode.isControl(opcode) && shortLength > 125) {
            fail(CloseStatus.PROTOCOL_ERROR, "control frame payload must not exceed 125 bytes");
            return;
        }
        if (shortLength < 126) {
            payloadLength = shortLength;
            enterMaskStage();
        } else if (shortLength == 126) {
            stage = Stage.LENGTH16;
        } else {
            stage = Stage.LENGTH64;
        }
    }

    private void readExtendedLength(ByteBuffer in, int lengthBytes) {
        while (headerFilled < lengthBytes && in.hasRemaining()) {
            headerScratch[headerFilled++] = in.get();
        }
        if (headerFilled < lengthBytes) {
            return;
        }
        long value = 0;
        for (int i = 0; i < lengthBytes; i++) {
            value = (value << 8) | (headerScratch[i] & 0xFFL);
        }
        headerFilled = 0;
        if (lengthBytes == 8 && value < 0) {
            fail(CloseStatus.PROTOCOL_ERROR, "the most significant bit of the 64-bit payload length is invalidly set to 1");
            return;
        }
        payloadLength = value;
        enterMaskStage();
    }

    private void enterMaskStage() {
        if (payloadLength > maxPayloadSize) {
            fail(CloseStatus.MESSAGE_TOO_BIG,
                    "message payload is " + payloadLength + " bytes, which exceeds the limit of " + maxPayloadSize + " bytes");
            return;
        }
        stage = Stage.MASK;
    }

    private void readMask(ByteBuffer in) {
        while (headerFilled < 4 && in.hasRemaining()) {
            maskingKey[headerFilled++] = in.get();
        }
        if (headerFilled < 4) {
            return;
        }
        headerFilled = 0;
        payloadReceived = 0;
        if (Opcode.isControl(opcode)) {
            controlPayload = new byte[(int) payloadLength];
        }
        stage = Stage.PAYLOAD;
        // A zero-payload frame does not depend on further network bytes; it is complete once the masking key is read.
        if (payloadLength == 0) {
            stage = Stage.HEAD;
            completeFrame();
        }
    }

    private void readPayload(ByteBuffer in) {
        long missing = payloadLength - payloadReceived;
        int available = (int) Math.min(in.remaining(), missing);
        int offset = (int) payloadReceived;

        if (Opcode.isControl(opcode)) {
            in.get(controlPayload, offset, available);
            for (int i = 0; i < available; i++) {
                int index = offset + i;
                controlPayload[index] ^= maskingKey[index & 3];
            }
        } else {
            byte[] block = new byte[available];
            in.get(block);
            for (int i = 0; i < available; i++) {
                long index = payloadReceived + i;
                block[i] ^= maskingKey[(int) (index & 3)];
            }
            message.append(block);
        }
        payloadReceived += available;
        if (payloadReceived == payloadLength) {
            stage = Stage.HEAD;
            completeFrame();
        }
    }

    private void completeFrame() {
        if (opcode == Opcode.PING) {
            listener.onPing(ByteBuffer.wrap(controlPayload));
            return;
        }
        if (opcode == Opcode.PONG) {
            listener.onPong(ByteBuffer.wrap(controlPayload));
            return;
        }
        if (opcode == Opcode.CLOSE) {
            handleClose();
            return;
        }
        handleDataFrame();
    }

    private void handleDataFrame() {
        if (opcode == Opcode.TEXT || opcode == Opcode.BINARY) {
            if (messageActive) {
                fail(CloseStatus.PROTOCOL_ERROR, "a new initial frame was received before the previous message ended");
                return;
            }
            messageActive = true;
            messageOpcode = opcode;
        } else if (!messageActive) {
            fail(CloseStatus.PROTOCOL_ERROR, "a continuation frame was received without an initial frame");
            return;
        }

        if (fin) {
            byte[] data = message.drain();
            messageActive = false;
            if (messageOpcode == Opcode.BINARY) {
                listener.onBinaryMessage(ByteBuffer.wrap(data));
            } else if (!Utf8.isValid(data)) {
                fail(CloseStatus.INVALID_PAYLOAD_DATA, "text frame contains an invalid UTF-8 sequence");
            } else {
                listener.onTextMessage(ByteBuffer.wrap(data));
            }
        }
    }

    private void handleClose() {
        byte[] payload = controlPayload;
        if (payload.length == 0) {
            closeReceived = true;
            listener.onClose(CloseStatus.NO_STATUS_CODE, "");
            return;
        }
        if (payload.length == 1) {
            fail(CloseStatus.PROTOCOL_ERROR, "the close frame payload length must not be 1");
            return;
        }
        int status = ((payload[0] & 0xFF) << 8) | (payload[1] & 0xFF);
        if (!CloseStatus.isSendable(status)) {
            fail(CloseStatus.PROTOCOL_ERROR, "close frame carries an invalid status code: " + status);
            return;
        }
        byte[] reasonBytes = new byte[payload.length - 2];
        System.arraycopy(payload, 2, reasonBytes, 0, reasonBytes.length);
        if (!Utf8.isValid(reasonBytes)) {
            fail(CloseStatus.INVALID_PAYLOAD_DATA, "close reason contains an invalid UTF-8 sequence");
            return;
        }
        closeReceived = true;
        listener.onClose(status, new String(reasonBytes, StandardCharsets.UTF_8));
    }

    private void fail(int closeStatus, String errorMessage) {
        broken = true;
        listener.onProtocolError(closeStatus, errorMessage);
    }
}
