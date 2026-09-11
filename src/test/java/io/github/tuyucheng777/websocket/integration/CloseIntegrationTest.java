package io.github.tuyucheng777.websocket.integration;

import io.github.tuyucheng777.websocket.WebSocketHandler;
import io.github.tuyucheng777.websocket.WebSocketServer;
import io.github.tuyucheng777.websocket.WebSocketSession;
import io.github.tuyucheng777.websocket.frame.CloseStatus;
import io.github.tuyucheng777.websocket.support.WsTestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Close handshake integration tests: client-initiated, server-initiated, callback exceptions, abrupt TCP disconnects, and status code propagation.
class CloseIntegrationTest {

    /// Handler that records the final close status code.
    private static final class RecordingHandler implements WebSocketHandler {
        final CountDownLatch closed = new CountDownLatch(1);
        final AtomicInteger statusCode = new AtomicInteger(-1);
        final AtomicReference<String> reason = new AtomicReference<>();

        @Override
        public void onClose(WebSocketSession session, int code, String closeReason) {
            statusCode.set(code);
            reason.set(closeReason);
            closed.countDown();
        }
    }

    private WebSocketServer server;
    private int port;

    @BeforeEach
    void startServer() {
        server = WebSocketServer.builder()
                .port(0)
                .path("/close", new WebSocketHandler() {
                    @Override
                    public void onTextMessage(WebSocketSession session, String message) {
                        session.close(CloseStatus.GOING_AWAY, "vacation");
                    }
                })
                .path("/throw", new WebSocketHandler() {
                    @Override
                    public void onTextMessage(WebSocketSession session, String message) {
                        throw new IllegalStateException("boom");
                    }
                })
                .build();
        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        port = server.getBoundPort();
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    @Test
    void clientInitiatedCloseIsEchoedAndObservedOnServer() throws Exception {
        RecordingHandler observer = new RecordingHandler();
        WebSocketServer customServer = WebSocketServer.builder()
                .port(0).path("/observe", observer).build();
        try (customServer) {
            customServer.start();
            int customPort = customServer.getBoundPort();
            WsTestClient client = WsTestClient.connect(customPort, "/observe");

            client.sendClose(1000, "bye");
            assertEquals(1000, client.recvCloseStatus());

            assertTrue(observer.closed.await(5, TimeUnit.SECONDS));
            assertEquals(1000, observer.statusCode.get());
            assertEquals("bye", observer.reason.get());
            client.disconnect();
        }
    }

    @Test
    void serverInitiatedCloseReachesClient() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/close")) {
            client.sendText("go");
            assertEquals(CloseStatus.GOING_AWAY, client.recvCloseStatus());
        }
    }

    @Test
    void handlerExceptionClosesWith1011() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/throw")) {
            client.sendText("trigger");
            assertEquals(CloseStatus.INTERNAL_ERROR, client.recvCloseStatus());
        }
    }

    @Test
    void abruptTcpCloseReports1006OnServer() throws Exception {
        RecordingHandler observer = new RecordingHandler();
        WebSocketServer customServer = WebSocketServer.builder()
                .port(0).path("/observe", observer).build();
        try (customServer) {
            customServer.start();
            int customPort = customServer.getBoundPort();
            WsTestClient client = WsTestClient.connect(customPort, "/observe");
            client.disconnect();

            assertTrue(observer.closed.await(5, TimeUnit.SECONDS));
            assertEquals(CloseStatus.CLOSED_ABNORMALLY, observer.statusCode.get());
        }
    }

    @Test
    void closeCallIsIdempotent() throws Exception {
        RecordingHandler observer = new RecordingHandler();
        WebSocketServer customServer = WebSocketServer.builder()
                .port(0).path("/observe", new WebSocketHandler() {
                    @Override
                    public void onTextMessage(WebSocketSession session, String message) {
                        session.close(1000, "first");
                        session.close(1011, "second");
                    }
                })
                .build();
        try (customServer) {
            customServer.start();
            int customPort = customServer.getBoundPort();
            try (WsTestClient client = WsTestClient.connect(customPort, "/observe")) {
                client.sendText("go");
                assertEquals(1000, client.recvCloseStatus());
            }
        }
    }
}
