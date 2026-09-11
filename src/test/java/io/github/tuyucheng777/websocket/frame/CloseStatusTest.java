package io.github.tuyucheng777.websocket.frame;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Tests for the validity of {@link CloseStatus} status codes.
class CloseStatusTest {

    @Test
    void standardCodesAreSendable() {
        assertTrue(CloseStatus.isSendable(CloseStatus.NORMAL));
        assertTrue(CloseStatus.isSendable(CloseStatus.GOING_AWAY));
        assertTrue(CloseStatus.isSendable(CloseStatus.PROTOCOL_ERROR));
        assertTrue(CloseStatus.isSendable(CloseStatus.MESSAGE_TOO_BIG));
        assertTrue(CloseStatus.isSendable(CloseStatus.INTERNAL_ERROR));
    }

    @Test
    void reservedCodesAreNotSendable() {
        assertFalse(CloseStatus.isSendable(CloseStatus.NO_STATUS_CODE));
        assertFalse(CloseStatus.isSendable(CloseStatus.CLOSED_ABNORMALLY));
        assertFalse(CloseStatus.isSendable(CloseStatus.TLS_HANDSHAKE_FAILURE));
        assertFalse(CloseStatus.isSendable(1004));
    }

    @Test
    void privateRangeIsSendable() {
        assertTrue(CloseStatus.isSendable(3000));
        assertTrue(CloseStatus.isSendable(4999));
        assertFalse(CloseStatus.isSendable(2999));
        assertFalse(CloseStatus.isSendable(5000));
    }
}
