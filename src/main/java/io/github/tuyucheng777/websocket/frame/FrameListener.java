package io.github.tuyucheng777.websocket.frame;

import java.nio.ByteBuffer;

/// Listener for frame decoding events.
///
/// After {@link FrameDecoder} finishes fragment reassembly and validation, it delivers logical messages and
/// control frames to the I/O layer through this interface. All methods are invoked on the selector thread that
/// owns the decoder, and implementations must not perform blocking operations in the callbacks.
public interface FrameListener {

    /// A complete text message was received.
    ///
    /// @param payload message payload that has passed UTF-8 validation (read mode, consumed once)
    void onTextMessage(ByteBuffer payload);

    /// A complete binary message was received.
    ///
    /// @param payload message payload (read mode, consumed once)
    void onBinaryMessage(ByteBuffer payload);

    /// A Ping control frame was received; the framework has automatically sent back a Pong.
    ///
    /// @param payload Ping payload; its length does not exceed 125
    void onPing(ByteBuffer payload);

    /// A Pong control frame was received.
    ///
    /// @param payload Pong payload; its length does not exceed 125
    void onPong(ByteBuffer payload);

    /// A close frame was received, marking the start of the closing handshake.
    ///
    /// @param statusCode status code; {@link CloseStatus#NO_STATUS_CODE} when the peer did not carry one
    /// @param reason     close reason text; may be an empty string
    void onClose(int statusCode, String reason);

    /// Data violating the protocol was received; the decoder enters an unusable state after the callback.
    ///
    /// @param closeStatus the close status code suggested to send back
    /// @param message     description of the violation
    void onProtocolError(int closeStatus, String message);
}
