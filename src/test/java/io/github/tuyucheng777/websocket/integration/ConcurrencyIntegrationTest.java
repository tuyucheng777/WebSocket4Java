package io.github.tuyucheng777.websocket.integration;

import io.github.tuyucheng777.websocket.WebSocketHandler;
import io.github.tuyucheng777.websocket.WebSocketServer;
import io.github.tuyucheng777.websocket.WebSocketSession;
import io.github.tuyucheng777.websocket.support.WsTestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Integration tests for high-concurrency connections, broadcast, and connection counting.
class ConcurrencyIntegrationTest {

    private static final int CLIENT_COUNT = 200;

    private WebSocketServer server;
    private int port;
    private CountDownLatch openLatch;

    @BeforeEach
    void startServer() {
        openLatch = new CountDownLatch(CLIENT_COUNT);
        server = WebSocketServer.builder()
                .port(0)
                .path("/echo", new WebSocketHandler() {
                    @Override
                    public void onTextMessage(WebSocketSession session, String message) {
                        session.sendText(message);
                    }
                })
                .path("/room", new WebSocketHandler() {
                    @Override
                    public void onOpen(WebSocketSession session) {
                        openLatch.countDown();
                    }

                    @Override
                    public void onTextMessage(WebSocketSession session, String message) {
                        // Unicast reply; broadcast is triggered by the test thread via server.broadcastText.
                        session.sendText("ack:" + message);
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
    void twoHundredVirtualThreadsExchangeEchoMessages() throws Exception {
        CountDownLatch done = new CountDownLatch(CLIENT_COUNT);
        AtomicInteger failures = new AtomicInteger();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < CLIENT_COUNT; i++) {
                int index = i;
                executor.submit(() -> {
                    try (WsTestClient client = WsTestClient.connect(port, "/echo")) {
                        String message = "hello-" + index;
                        client.sendText(message);
                        WsTestClient.Frame frame = client.recvMessage();
                        String reply = new String(frame.payload(), StandardCharsets.UTF_8);
                        if (!message.equals(reply)) {
                            failures.incrementAndGet();
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(30, TimeUnit.SECONDS), "not all concurrent echoes completed within 30s");
        }
        assertEquals(0, failures.get(), "some concurrent clients failed");
    }

    @Test
    void broadcastReachesEveryConnectedSession() throws Exception {
        WsTestClient[] clients = new WsTestClient[CLIENT_COUNT];
        try {
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                CountDownLatch connected = new CountDownLatch(CLIENT_COUNT);
                for (int i = 0; i < CLIENT_COUNT; i++) {
                    int index = i;
                    executor.submit(() -> {
                        try {
                            clients[index] = WsTestClient.connect(port, "/room");
                            connected.countDown();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
                assertTrue(connected.await(15, TimeUnit.SECONDS), "not all clients connected");
            }
            assertTrue(openLatch.await(5, TimeUnit.SECONDS), "onOpen was not fired for all clients");
            assertEquals(CLIENT_COUNT, server.connectionCount());

            server.broadcastText("attention");

            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                CountDownLatch received = new CountDownLatch(CLIENT_COUNT);
                AtomicInteger failures = new AtomicInteger();
                for (WsTestClient client : clients) {
                    executor.submit(() -> {
                        try {
                            WsTestClient.Frame frame = client.recvMessage();
                            if (!"attention".equals(
                                    new String(frame.payload(), StandardCharsets.UTF_8))) {
                                failures.incrementAndGet();
                            }
                            received.countDown();
                        } catch (Exception e) {
                            failures.incrementAndGet();
                        }
                    });
                }
                assertTrue(received.await(15, TimeUnit.SECONDS), "broadcast was not delivered in time");
                assertEquals(0, failures.get());
            }
        } finally {
            for (WsTestClient client : clients) {
                if (client != null) {
                    try {
                        client.close();
                    } catch (Exception ignored) {
                        // Ignore close races.
                    }
                }
            }
        }
    }

    @Test
    void connectionCountTracksConnectsAndDisconnects() throws Exception {
        WsTestClient a = WsTestClient.connect(port, "/echo");
        WsTestClient b = WsTestClient.connect(port, "/echo");
        a.sendText("x");
        a.recvMessage();
        b.sendText("x");
        b.recvMessage();
        assertEquals(2, server.connectionCount());

        a.close();
        long deadline = System.currentTimeMillis() + 5_000;
        while (server.connectionCount() != 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(1, server.connectionCount());
        b.close();
        deadline = System.currentTimeMillis() + 5_000;
        while (server.connectionCount() != 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(0, server.connectionCount());
    }
}
