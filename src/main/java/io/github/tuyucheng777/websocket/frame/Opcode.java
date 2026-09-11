package io.github.tuyucheng777.websocket.frame;

/// WebSocket frame opcodes (RFC 6455, section 5.2).
///
/// The opcode is represented by the low 4 bits of the first frame byte and is used to
/// distinguish data frames, control frames, and fragmented continuation frames.
/// This class provides constants only and cannot be instantiated.
public final class Opcode {

    /// Continuation frame: a subsequent fragment belonging to the previous data frame.
    public static final int CONTINUATION = 0x0;

    /// Text frame: the payload is UTF-8 encoded text.
    public static final int TEXT = 0x1;

    /// Binary frame: the payload is an opaque byte sequence.
    public static final int BINARY = 0x2;

    /// Close frame: initiates or acknowledges the closing handshake.
    public static final int CLOSE = 0x8;

    /// Ping frame: heartbeat probe; the receiver must reply with a Pong as soon as possible.
    public static final int PING = 0x9;

    /// Pong frame: the reply to a Ping.
    public static final int PONG = 0xA;

    private Opcode() {
    }

    /// Determines whether the opcode is a control frame ({@code 0x8}–{@code 0xF}).
    ///
    /// @param opcode raw opcode
    /// @return {@code true} for a control frame
    public static boolean isControl(int opcode) {
        return (opcode & 0x08) != 0;
    }

    /// Determines whether the opcode is a known opcode reserved by the protocol.
    ///
    /// @param opcode raw opcode
    /// @return {@code true} for a known opcode; {@code false} for an unknown reserved opcode
    public static boolean isKnown(int opcode) {
        return opcode == CONTINUATION || opcode == TEXT || opcode == BINARY
                || opcode == CLOSE || opcode == PING || opcode == PONG;
    }

    /// Returns a human-readable name for the opcode, mainly for logging.
    ///
    /// @param opcode raw opcode
    /// @return opcode name
    public static String nameOf(int opcode) {
        return switch (opcode) {
            case CONTINUATION -> "CONTINUATION";
            case TEXT -> "TEXT";
            case BINARY -> "BINARY";
            case CLOSE -> "CLOSE";
            case PING -> "PING";
            case PONG -> "PONG";
            default -> "UNKNOWN(0x" + Integer.toHexString(opcode) + ")";
        };
    }
}
