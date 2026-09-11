package io.github.tuyucheng777.websocket;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/// Session operation interface for a single WebSocket connection.
///
/// The session is created by the framework after a successful handshake and
/// passed into each {@link WebSocketHandler} callback. Send methods may be
/// called from any thread: data first enters a lock-free per-connection send
/// queue and is uniformly flushed by the single I/O selector thread, which
/// guarantees that frames never interleave. When the peer reads too slowly and
/// the backlog exceeds the high water mark, the send call suspends with
/// backpressure on the virtual thread (it never occupies a platform thread for
/// long).
public interface WebSocketSession {

    /// Returns the unique session identifier.
    ///
    /// @return the identifier string
    String id();

    /// Returns the request path (excluding the query string).
    ///
    /// @return the request path
    String path();

    /// Returns the path variables resolved when matching the path pattern.
    ///
    /// @return an immutable map of variables
    Map<String, String> pathVariables();

    /// Reads a path variable.
    ///
    /// @param name the variable name
    /// @return the variable value, or {@code null} if absent
    String pathVariable(String name);

    /// Returns all query parameters (first-value view).
    ///
    /// @return an immutable map of query parameters
    Map<String, String> queryParameters();

    /// Reads a query parameter.
    ///
    /// @param name the parameter name
    /// @return the parameter value, or {@code null} if absent
    String queryParameter(String name);

    /// Returns all headers of the handshake request (case-insensitive).
    ///
    /// @return an immutable map of headers
    Map<String, List<String>> headers();

    /// Reads the first value of a handshake request header.
    ///
    /// @param name the header name, case-insensitive
    /// @return the header value, or {@code null} if absent
    String header(String name);

    /// Returns the remote address.
    ///
    /// @return the remote address, possibly {@code null} if unavailable
    InetSocketAddress remoteAddress();

    /// Returns the user-defined attribute map for sharing state between
    /// callbacks.
    ///
    /// @return the mutable attribute map
    Map<String, Object> attributes();

    /// Whether the session can still send data (has not entered the closing
    /// process).
    ///
    /// @return {@code true} if open
    boolean isOpen();

    /// Sends a complete text message.
    ///
    /// @param text the text content
    /// @throws WebSocketException if the session is closed or the backlog
    ///                            backpressure wait is interrupted
    void sendText(String text);

    /// Sends a complete binary message.
    ///
    /// @param payload the payload; the framework reads its remaining bytes
    ///                during sending, so the caller need not retain it
    /// @throws WebSocketException if the session is closed or the backlog
    ///                            backpressure wait is interrupted
    void sendBinary(ByteBuffer payload);

    /// Sends a text message fragment.
    ///
    /// The first call starts the message with the TEXT opcode, subsequent calls
    /// use CONTINUATION continuation frames, and {@code last = true} ends the
    /// message; no other message may be interleaved while a fragment sequence is
    /// in progress.
    ///
    /// @param fragment the fragment text
    /// @param last     whether this is the final fragment
    /// @throws WebSocketException if the session is closed
    void sendTextFragment(String fragment, boolean last);

    /// Sends a binary message fragment, with the same semantics as
    /// {@link #sendTextFragment}.
    ///
    /// @param fragment the fragment payload
    /// @param last     whether this is the final fragment
    /// @throws WebSocketException if the session is closed
    void sendBinaryFragment(ByteBuffer fragment, boolean last);

    /// Sends a Ping frame; the peer should respond with a Pong.
    ///
    /// @param payload the payload, no longer than 125 bytes, may be
    ///                {@code null}
    /// @throws WebSocketException if the session is closed
    void sendPing(ByteBuffer payload);

    /// Sends a Pong frame (also usable as a one-way heartbeat).
    ///
    /// @param payload the payload, no longer than 125 bytes, may be
    ///                {@code null}
    /// @throws WebSocketException if the session is closed
    void sendPong(ByteBuffer payload);

    /// Initiates a normal close handshake with
    /// {@link io.github.tuyucheng777.websocket.frame.CloseStatus#NORMAL}.
    void close();

    /// Initiates a close handshake with the specified status code and reason.
    ///
    /// Repeated calls are ignored; after the close frame is flushed, the
    /// underlying TCP connection is closed immediately.
    ///
    /// @param statusCode a status code that can be sent on the wire
    /// @param reason     the close reason, may be {@code null}
    void close(int statusCode, String reason);
}
