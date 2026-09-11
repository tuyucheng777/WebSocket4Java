package io.github.tuyucheng777.websocket.integration;

import io.github.tuyucheng777.websocket.WebSocketHandler;
import io.github.tuyucheng777.websocket.WebSocketServer;
import io.github.tuyucheng777.websocket.WebSocketSession;
import io.github.tuyucheng777.websocket.frame.CloseStatus;
import io.github.tuyucheng777.websocket.support.WsTestClient;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Tests for server lifecycle, configuration validation, and backpressure behavior.
class ServerLifecycleIntegrationTest {

    @Test
    void portZeroAssignsEphemeralPort() throws Exception {
        WebSocketServer server = WebSocketServer.builder()
                .port(0).path("/echo", new WebSocketHandler() { }).build();
        server.start();
        try {
            assertTrue(server.getBoundPort() > 0);
            try (WsTestClient client = WsTestClient.connect(server.getBoundPort(), "/echo")) {
                assertNotNull(client);
            }
        } finally {
            server.close();
        }
    }

    @Test
    void startTwiceIsRejected() throws Exception {
        WebSocketServer server = WebSocketServer.builder()
                .port(0).build();
        server.start();
        try {
            assertThrows(IllegalStateException.class, server::start);
        } finally {
            server.close();
        }
    }

    @Test
    void invalidPortIsRejectedAtBuildTime() {
        assertThrows(IllegalArgumentException.class,
                () -> WebSocketServer.builder().port(70_000).build());
    }

    @Test
    void invalidWaterMarksAreRejectedAtBuildTime() {
        assertThrows(IllegalArgumentException.class,
                () -> WebSocketServer.builder()
                        .writeBufferWaterMark(1_000, 2_000).build());
    }

    @Test
    void invalidPathPatternIsRejectedAtRegistration() {
        assertThrows(IllegalArgumentException.class,
                () -> WebSocketServer.builder().path("no-leading-slash", new WebSocketHandler() { }));
    }

    @Test
    void shutdownSendsGoingAwayAndClosesSessions() throws Exception {
        WebSocketServer server = WebSocketServer.builder()
                .port(0).path("/echo", new WebSocketHandler() { }).build();
        server.start();
        int port = server.getBoundPort();
        WsTestClient client = WsTestClient.connect(port, "/echo");

        server.close();

        assertEquals(CloseStatus.GOING_AWAY, client.recvCloseStatus());
        client.disconnect();
        assertEquals(0, server.connectionCount());
    }

    @Test
    void sessionBecomesNotOpenAfterClose() throws Exception {
        final boolean[] observedOpen = new boolean[1];
        WebSocketServer server = WebSocketServer.builder()
                .port(0)
                .path("/echo", new WebSocketHandler() {
                    @Override
                    public void onTextMessage(WebSocketSession session, String message) {
                        observedOpen[0] = session.isOpen();
                        session.close(CloseStatus.NORMAL, "done");
                        assertFalse(session.isOpen());
                    }
                })
                .build();
        try (server) {
            server.start();
            int port = server.getBoundPort();
            try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
                client.sendText("go");
                assertEquals(CloseStatus.NORMAL, client.recvCloseStatus());
            }
            assertTrue(observedOpen[0]);
        }
    }

    @Test
    void attributesAreSharedAcrossCallbacks() throws Exception {
        WebSocketServer server = WebSocketServer.builder()
                .port(0)
                .path("/echo", new WebSocketHandler() {
                    @Override
                    public void onOpen(WebSocketSession session) {
                        session.attributes().put("counter", new int[]{10});
                    }

                    @Override
                    public void onTextMessage(WebSocketSession session, String message) {
                        int[] counter = (int[]) session.attributes().get("counter");
                        counter[0]++;
                        session.sendText(String.valueOf(counter[0]));
                    }
                })
                .build();
        try (server) {
            server.start();
            int port = server.getBoundPort();
            try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
                client.sendText("inc");
                WsTestClient.Frame frame = client.recvMessage();
                assertEquals("11", new String(frame.payload(),
                        java.nio.charset.StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void sendingOnClosedSessionThrows() throws Exception {
        final Throwable[] failure = new Throwable[1];
        WebSocketServer server = WebSocketServer.builder()
                .port(0)
                .path("/echo", new WebSocketHandler() {
                    @Override
                    public void onTextMessage(WebSocketSession session, String message) {
                        session.close();
                        try {
                            session.sendText("late");
                        } catch (RuntimeException e) {
                            failure[0] = e;
                        }
                    }
                })
                .build();
        try (server) {
            server.start();
            int port = server.getBoundPort();
            try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
                client.sendText("go");
                client.recvCloseStatus();
                long deadline = System.currentTimeMillis() + 5_000;
                while (failure[0] == null && System.currentTimeMillis() < deadline) {
                    Thread.sleep(10);
                }
                assertNotNull(failure[0], "expected send after close to throw");
            }
        }
    }

    @Test
    void interleavingFragmentWithCompleteMessageIsRejected() throws Exception {
        final Throwable[] failure = new Throwable[1];
        WebSocketServer server = WebSocketServer.builder()
                .port(0)
                .path("/echo", new WebSocketHandler() {
                    @Override
                    public void onOpen(WebSocketSession session) {
                        try {
                            session.sendTextFragment("a", false);
                            session.sendText("b");
                        } catch (IllegalStateException e) {
                            failure[0] = e;
                        }
                    }
                })
                .build();
        try (server) {
            server.start();
            int port = server.getBoundPort();
            try (WsTestClient ignored = WsTestClient.connect(port, "/echo")) {
                long deadline = System.currentTimeMillis() + 5_000;
                while (failure[0] == null && System.currentTimeMillis() < deadline) {
                    Thread.sleep(10);
                }
                assertInstanceOf(IllegalStateException.class, failure[0]);
            }
        }
    }

    @Test
    void backpressureDrainsWithoutDeadlockWhenClientReads() throws Exception {
        // 1 MB far exceeds the default 256 KB high water mark; the client keeps reading, so the sender virtual thread should be woken normally.
        WebSocketServer server = WebSocketServer.builder()
                .port(0)
                .writeBufferWaterMark(64 * 1024, 16 * 1024)
                .path("/bulk", new WebSocketHandler() {
                    @Override
                    public void onTextMessage(WebSocketSession session, String message) {
                        byte[] payload = new byte[1 << 20];
                        for (int i = 0; i < payload.length; i++) {
                            payload[i] = (byte) i;
                        }
                        session.sendBinary(ByteBuffer.wrap(payload));
                        session.sendText("done");
                    }
                })
                .build();
        try (server) {
            server.start();
            int port = server.getBoundPort();
            try (WsTestClient client = WsTestClient.connect(port, "/bulk")) {
                client.sendText("go");
                int total = 0;
                while (true) {
                    WsTestClient.Frame frame = client.recvMessage();
                    if (frame.opcode() == 0x1) {
                        assertEquals("done", new String(frame.payload(),
                                java.nio.charset.StandardCharsets.UTF_8));
                        break;
                    }
                    total += frame.payload().length;
                }
                assertEquals(1 << 20, total);
            }
        }
    }

    @Test
    void sessionsSnapshotIsImmutable() throws Exception {
        WebSocketServer server = WebSocketServer.builder()
                .port(0).path("/echo", new WebSocketHandler() { }).build();
        server.start();
        try {
            int port = server.getBoundPort();
            try (WsTestClient ignored = WsTestClient.connect(port, "/echo")) {
                assertThrows(UnsupportedOperationException.class,
                        () -> server.sessions().clear());
                assertFalse(server.sessions().isEmpty());
            }
        } finally {
            server.close();
        }
    }

    @Test
    void serverWithNoRoutesRejectsAllPaths() throws Exception {
        WebSocketServer server = WebSocketServer.builder().port(0).build();
        try (server) {
            server.start();
            int port = server.getBoundPort();
            assertThrows(java.io.IOException.class,
                    () -> WsTestClient.connect(port, "/anything", Map.of()));
        }
    }
}
