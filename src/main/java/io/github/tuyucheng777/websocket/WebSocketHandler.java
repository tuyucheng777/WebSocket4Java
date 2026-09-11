package io.github.tuyucheng777.websocket;

import java.nio.ByteBuffer;

/// WebSocket connection lifecycle and message handler.
///
/// All methods have empty default implementations, so business code only needs
/// to override the callbacks it cares about. Callbacks run on a **virtual
/// thread dedicated to each connection**, which means blocking operations (such
/// as database access or waiting on a blocking queue) are allowed and will not
/// block I/O and event processing of other connections.
///
/// Callbacks on the same connection are strictly serial in the following order:
///
/// 1. {@link #onOpen} exactly once;
/// 2. zero or more message/Ping/Pong callbacks;
/// 3. {@link #onClose} exactly once.
///
/// {@link #onError} may be interleaved before closing.
public interface WebSocketHandler {

    /// Invoked when the handshake completes and the connection enters the
    /// WebSocket state.
    ///
    /// @param session the newly established session
    default void onOpen(WebSocketSession session) {
    }

    /// Invoked when a complete text message is received.
    ///
    /// @param session the owning session
    /// @param message the fully UTF-8 decoded message text
    default void onTextMessage(WebSocketSession session, String message) {
    }

    /// Invoked when a complete binary message is received.
    ///
    /// @param session the owning session
    /// @param payload the message payload; it may be recycled after the
    ///                callback returns, so copy it yourself for asynchronous use
    default void onBinaryMessage(WebSocketSession session, ByteBuffer payload) {
    }

    /// Invoked when a Ping frame is received; the framework automatically sends
    /// back the corresponding Pong before the callback.
    ///
    /// @param session the owning session
    /// @param payload the Ping payload, no longer than 125 bytes
    default void onPing(WebSocketSession session, ByteBuffer payload) {
    }

    /// Invoked when a Pong frame is received.
    ///
    /// @param session the owning session
    /// @param payload the Pong payload, no longer than 125 bytes
    default void onPong(WebSocketSession session, ByteBuffer payload) {
    }

    /// Invoked when the close handshake completes and the connection is
    /// terminated; called exactly once on this session.
    ///
    /// @param session    the owning session
    /// @param statusCode the final close status code; for scenarios without a
    ///                   close frame such as network errors it is
    ///                   {@link io.github.tuyucheng777.websocket.frame.CloseStatus#CLOSED_ABNORMALLY}
    /// @param reason     the close reason, which may be an empty string
    default void onClose(WebSocketSession session, int statusCode, String reason) {
    }

    /// Invoked when an I/O exception, protocol error, or an exception thrown by
    /// a business callback occurs.
    ///
    /// @param session the owning session (never {@code null} for errors after
    ///                the handshake)
    /// @param error   the exception
    default void onError(WebSocketSession session, Throwable error) {
    }
}
