package io.github.tuyucheng777.websocket.frame;

/// WebSocket close status codes (RFC 6455, section 7.4).
///
/// The status code is carried in 2-byte big-endian byte order in the close frame payload. {@code 1004}–{@code 1006} and
/// {@code 1015} only represent connection state locally and must never appear on the wire.
public final class CloseStatus {

    /// Normal closure.
    public static final int NORMAL = 1000;

    /// Endpoint is "going away", such as a server shutdown or a browser page navigation.
    public static final int GOING_AWAY = 1001;

    /// Protocol error.
    public static final int PROTOCOL_ERROR = 1002;

    /// Unsupported data type ({@link #INVALID_PAYLOAD_DATA} may also be used, for example, when a text frame contains invalid UTF-8).
    public static final int UNSUPPORTED_DATA = 1003;

    /// Reserved value: no status code on the wire (local semantics, must not be sent).
    public static final int NO_STATUS_CODE = 1005;

    /// Reserved value: the connection closed abnormally without completing the closing handshake (local semantics, must not be sent).
    public static final int CLOSED_ABNORMALLY = 1006;

    /// Invalid payload data, typically when a text frame is not valid UTF-8.
    public static final int INVALID_PAYLOAD_DATA = 1007;

    /// Endpoint policy violation.
    public static final int POLICY_VIOLATION = 1008;

    /// Message too large for the endpoint to process.
    public static final int MESSAGE_TOO_BIG = 1009;

    /// The client expected extension negotiation but the server did not accept it.
    public static final int MANDATORY_EXTENSION = 1010;

    /// The server encountered an unexpected internal error.
    public static final int INTERNAL_ERROR = 1011;

    /// Reserved value: TLS handshake failure (local semantics, must not be sent).
    public static final int TLS_HANDSHAKE_FAILURE = 1015;

    private CloseStatus() {
    }

    /// Determines whether the status code is allowed to appear in a close frame (RFC 6455 / RFC 7936).
    ///
    /// The allowed range includes the standard codes {@code 1000}–{@code 1011} except {@code 1015},
    /// as well as the private range {@code 3000}–{@code 4999}.
    ///
    /// @param code the status code to check
    /// @return {@code true} if it may be sent on the wire
    public static boolean isSendable(int code) {
        if (code >= 3000 && code <= 4999) {
            return true;
        }
        if (code < 1000 || code > 1011) {
            return false;
        }
        return code != 1004 && code != NO_STATUS_CODE && code != CLOSED_ABNORMALLY;
    }
}
