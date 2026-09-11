package io.github.tuyucheng777.websocket.integration;

import io.github.tuyucheng777.websocket.WebSocketHandler;
import io.github.tuyucheng777.websocket.WebSocketServer;
import io.github.tuyucheng777.websocket.WebSocketSession;
import io.github.tuyucheng777.websocket.support.WsTestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// End-to-end tests: text/binary echo, large messages, bidirectional fragmentation, heartbeats, and session metadata.
class EchoIntegrationTest {

    private WebSocketServer server;
    private int port;

    @BeforeEach
    void startServer() throws Exception {
        server = WebSocketServer.builder()
                .port(0)
                .path("/echo", new WebSocketHandler() {
                    @Override
                    public void onTextMessage(WebSocketSession session, String message) {
                        if ("ping-me".equals(message)) {
                            session.sendPing(ByteBuffer.wrap(new byte[]{1, 2, 3}));
                        } else {
                            session.sendText(message);
                        }
                    }

                    @Override
                    public void onBinaryMessage(WebSocketSession session, ByteBuffer payload) {
                        session.sendBinary(payload);
                    }
                })
                .path("/fragment", new WebSocketHandler() {
                    @Override
                    public void onTextMessage(WebSocketSession session, String message) {
                        session.sendTextFragment("Hel", false);
                        session.sendTextFragment("lo-", false);
                        session.sendTextFragment(message, true);
                    }
                })
                .path("/chat/{room}", new WebSocketHandler() {
                    @Override
                    public void onTextMessage(WebSocketSession session, String message) {
                        session.sendText("path=" + session.path()
                                + "|room=" + session.pathVariable("room")
                                + "|user=" + session.queryParameter("user")
                                + "|version=" + session.header("Sec-WebSocket-Version"));
                    }
                })
                .build();
        server.start();
        port = server.getBoundPort();
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    @Test
    void echoesTextMessage() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
            client.sendText("hello websocket");
            WsTestClient.Frame frame = client.recvMessage();
            assertEquals(0x1, frame.opcode());
            assertEquals("hello websocket", new String(frame.payload(), java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @Test
    void echoesUnicodeTextMessage() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
            client.sendText("Héllo, WebSocket 🌐");
            WsTestClient.Frame frame = client.recvMessage();
            assertEquals("Héllo, WebSocket 🌐",
                    new String(frame.payload(), java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @Test
    void echoesBinaryMessage() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
            byte[] data = new byte[1_024];
            new SecureRandom().nextBytes(data);
            client.sendBinary(data);
            WsTestClient.Frame frame = client.recvMessage();
            assertEquals(0x2, frame.opcode());
            assertArrayEquals(data, frame.payload());
        }
    }

    @Test
    void echoesOneMegabyteMessage() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
            byte[] data = new byte[1 << 20];
            new SecureRandom().nextBytes(data);
            client.sendBinary(data);

            WsTestClient.Frame frame = client.recvMessage();
            assertArrayEquals(data, frame.payload());
        }
    }

    @Test
    void serverReceivesFragmentedMessageAssembled() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
            client.sendFragment(0x1, "frag".getBytes(java.nio.charset.StandardCharsets.UTF_8), false);
            client.sendFragment(0x0, "ment".getBytes(java.nio.charset.StandardCharsets.UTF_8), true);

            WsTestClient.Frame frame = client.recvMessage();
            assertEquals("fragment", new String(frame.payload(), java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @Test
    void clientReassemblesFragmentedServerMessage() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/fragment")) {
            client.sendText("ws");
            WsTestClient.Frame frame = client.recvMessage();
            assertTrue(frame.fin());
            assertEquals("Hello-ws", new String(frame.payload(), java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @Test
    void serverAutomaticallyRepliesPong() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
            client.sendPing(new byte[]{9, 8, 7});
            WsTestClient.Frame frame = client.recvFrame();
            assertEquals(0xA, frame.opcode());
            assertArrayEquals(new byte[]{9, 8, 7}, frame.payload());
        }
    }

    @Test
    void serverCanSendPingProactively() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
            client.sendText("ping-me");
            WsTestClient.Frame frame = client.recvFrame();
            assertEquals(0x9, frame.opcode());
            assertArrayEquals(new byte[]{1, 2, 3}, frame.payload());
        }
    }

    @Test
    void sessionExposesPathVariablesQueryAndHeaders() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/chat/java?user=alice")) {
            client.sendText("info");
            WsTestClient.Frame frame = client.recvMessage();
            String response = new String(frame.payload(), java.nio.charset.StandardCharsets.UTF_8);
            assertEquals("path=/chat/java|room=java|user=alice|version=13", response);
        }
    }

    @Test
    void survivesFiveHundredSequentialMessages() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
            for (int i = 0; i < 500; i++) {
                String message = "message-" + i;
                client.sendText(message);
                WsTestClient.Frame frame = client.recvMessage();
                assertEquals(message,
                        new String(frame.payload(), java.nio.charset.StandardCharsets.UTF_8));
            }
        }
        // Give the server a moment to finish closing, then assert no connections remain.
        TimeUnit.MILLISECONDS.sleep(100);
        assertEquals(0, server.connectionCount());
    }
}
