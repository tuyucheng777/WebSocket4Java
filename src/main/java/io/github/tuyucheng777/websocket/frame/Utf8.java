package io.github.tuyucheng777.websocket.frame;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/// Strict UTF-8 validation and decoding utilities.
///
/// RFC 6455 requires the payload of a text frame to be valid UTF-8, and invalid encoding must
/// close the connection with the status code {@link CloseStatus#INVALID_PAYLOAD_DATA}. The JDK's
/// {@link java.nio.charset.CharsetDecoder} strictly enforces Unicode 12 and RFC 3629 rules in
/// {@link CodingErrorAction#REPORT} mode; it is reused directly to avoid mistakes in a hand-written state machine.
public final class Utf8 {

    private Utf8() {
    }

    /// Determines whether the byte sequence is valid UTF-8.
    ///
    /// @param data the bytes to validate
    /// @return {@code true} if valid
    public static boolean isValid(byte[] data) {
        try {
            decode(data);
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    /// Strictly decodes UTF-8 bytes.
    ///
    /// @param data the byte sequence
    /// @return the decoded string
    /// @throws CharacterCodingException if an invalid UTF-8 sequence is present
    public static String decode(byte[] data) throws CharacterCodingException {
        var decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        return decoder.decode(ByteBuffer.wrap(data)).toString();
    }
}
