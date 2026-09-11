package io.github.tuyucheng777.websocket.http;

/// Exception indicating a failed WebSocket handshake.
///
/// Carries the HTTP status code to send back (e.g. {@code 400}, {@code 404});
/// the I/O layer converts it into an ordinary HTTP error response and closes the TCP connection.
public class HandshakeException extends Exception {

    private static final long serialVersionUID = 1L;

    /// The HTTP status code suggested to send back.
    private final transient int httpStatus;

    /// Constructs a handshake exception.
    ///
    /// @param httpStatus HTTP status code
    /// @param message    failure reason
    public HandshakeException(int httpStatus, String message) {
        super(message);
        this.httpStatus = httpStatus;
    }

    /// Returns the HTTP status code suggested to send back.
    ///
    /// @return the HTTP status code
    public int httpStatus() {
        return httpStatus;
    }
}
