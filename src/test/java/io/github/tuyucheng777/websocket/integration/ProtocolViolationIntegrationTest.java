package io.github.tuyucheng777.websocket.integration;

import io.github.tuyucheng777.websocket.WebSocketHandler;
import io.github.tuyucheng777.websocket.WebSocketServer;
import io.github.tuyucheng777.websocket.frame.CloseStatus;
import io.github.tuyucheng777.websocket.frame.Opcode;
import io.github.tuyucheng777.websocket.support.WireFrames;
import io.github.tuyucheng777.websocket.support.WsTestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// End-to-end rejection tests for illegal protocol data and invalid handshakes.
class ProtocolViolationIntegrationTest {

    private WebSocketServer server;
    private int port;

    @BeforeEach
    void startServer() {
        server = WebSocketServer.builder()
                .port(0)
                .maxFramePayloadSize(1_024)
                .path("/echo", new WebSocketHandler() { })
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
    void unmaskedFrameIsRejectedWith1002() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
            byte[] illegal = WireFrames.unmasked(Opcode.TEXT,
                    "hello".getBytes(StandardCharsets.UTF_8), true);
            client.sendRaw(illegal);
            assertEquals(CloseStatus.PROTOCOL_ERROR, client.recvCloseStatus());
        }
    }

    @Test
    void invalidUtf8TextIsRejectedWith1007() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
            byte[] bad = {(byte) 0xFF, (byte) 0xFE, (byte) 0xFD};
            client.sendRaw(WireFrames.masked(Opcode.TEXT, bad, true));
            assertEquals(CloseStatus.INVALID_PAYLOAD_DATA, client.recvCloseStatus());
        }
    }

    @Test
    void oversizedMessageIsRejected() throws Exception {
        try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
            client.sendRaw(WireFrames.masked(Opcode.BINARY, new byte[2_048], true));
            // The kernel may buffer the unread payload and send RST, so either a 1009 close frame or a connection error is acceptable.
            try {
                int status = client.recvCloseStatus();
                assertEquals(CloseStatus.MESSAGE_TOO_BIG, status);
            } catch (Exception acceptable) {
                // The OS may return RST directly for a close with unread received bytes.
            }
        }
    }

    @Test
    void unknownPathReturns404() throws Exception {
        try (Socket socket = WsTestClient.rawConnection(port)) {
            OutputStream output = socket.getOutputStream();
            WsTestClient.writeHandshake(output, "127.0.0.1", port, "/missing", Map.of());
            String response = WsTestClient.readHead(socket.getInputStream());
            assertTrue(response.startsWith("HTTP/1.1 404 "), response);
        }
    }

    @Test
    void wrongWebSocketVersionReturns426() throws Exception {
        try (Socket socket = WsTestClient.rawConnection(port)) {
            WsTestClient.writeHandshake(socket.getOutputStream(), "127.0.0.1", port,
                    "/echo", Map.of("Sec-WebSocket-Version", "8"));
            String response = WsTestClient.readHead(socket.getInputStream());
            assertTrue(response.startsWith("HTTP/1.1 426 "), response);
        }
    }

    @Test
    void missingKeyReturns400() throws Exception {
        try (Socket socket = WsTestClient.rawConnection(port)) {
            // HashMap allows null values, which semantically removes a default header; Map.of does not allow null.
            Map<String, String> overrides = new java.util.HashMap<>();
            overrides.put("Sec-WebSocket-Key", null);
            WsTestClient.writeHandshake(socket.getOutputStream(), "127.0.0.1", port,
                    "/echo", overrides);
            String response = WsTestClient.readHead(socket.getInputStream());
            assertTrue(response.startsWith("HTTP/1.1 400 "), response);
        }
    }

    @Test
    void connectionIsGoneAfterRejectedHandshake() throws Exception {
        Socket socket = WsTestClient.rawConnection(port);
        WsTestClient.writeHandshake(socket.getOutputStream(), "127.0.0.1", port,
                "/missing", Map.of());
        String response = WsTestClient.readHead(socket.getInputStream());
        assertTrue(response.contains("Connection: close"));
        // The server should close the TCP connection after writing the error response.
        int next = socket.getInputStream().read();
        if (next != -1) {
            fail("expected closed connection but read byte: " + next);
        }
        socket.close();
    }
}
