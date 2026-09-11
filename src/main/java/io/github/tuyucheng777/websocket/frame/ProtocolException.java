package io.github.tuyucheng777.websocket.frame;

/// Represents an exception caused by a WebSocket protocol violation.
///
/// This exception carries the close status code suggested for sending back to the peer. The frame decoder
/// throws it when it encounters invalid data, and the I/O layer converts it into a close frame and terminates the connection.
public class ProtocolException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /// The suggested close status code; see {@link CloseStatus} for possible values.
    private final transient int closeStatus;

    /// Constructs a protocol exception.
    ///
    /// @param closeStatus the close status code suggested to send back
    /// @param message     the error description
    public ProtocolException(int closeStatus, String message) {
        super(message);
        this.closeStatus = closeStatus;
    }

    /// Returns the close status code suggested to send back to the peer.
    ///
    /// @return the close status code
    public int closeStatus() {
        return closeStatus;
    }
}
