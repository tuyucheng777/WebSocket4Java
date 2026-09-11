package io.github.tuyucheng777.websocket;

/// General framework runtime exception.
///
/// Thrown in scenarios such as server startup failure or continuing to send
/// after the session has been closed.
public class WebSocketException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /// Constructs an exception.
    ///
    /// @param message the error description
    public WebSocketException(String message) {
        super(message);
    }

    /// Constructs an exception with a cause.
    ///
    /// @param message the error description
    /// @param cause   the original cause
    public WebSocketException(String message, Throwable cause) {
        super(message, cause);
    }
}
