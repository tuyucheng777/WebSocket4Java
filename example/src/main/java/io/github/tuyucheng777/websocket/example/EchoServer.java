import io.github.tuyucheng777.websocket.WebSocketHandler;
import io.github.tuyucheng777.websocket.WebSocketServer;
import io.github.tuyucheng777.websocket.WebSocketSession;

/// Minimal echo server.
///
/// Sends back every received text and binary message unchanged, and logs
/// lifecycle events to stdout. Run with {@code mvn -pl example exec:java}
/// (the default {@code mainClass} in {@code example/pom.xml} points here).
///
/// Connect with any WebSocket client, for example:
///
/// ```
    /// websocat ws://localhost:8080/echo
    /// ```
void main(String[] args) throws Exception {
    // The builder uses fluent setters; port 0 lets the OS pick a free
    // port, here we bind explicitly to 8080 for the example.
    WebSocketServer server = WebSocketServer.builder()
            .host("0.0.0.0")
            .port(8080)
            .path("/echo", new EchoHandler())
            .build();

    server.start();
    IO.println("Echo server listening on ws://0.0.0.0:8080/echo");

    // Register a JVM shutdown hook for a graceful close; close() sends a
    // 1001 frame to every session and waits briefly for the flush.
    Runtime.getRuntime().addShutdownHook(new Thread(server::close));

    // Keep the main thread alive; the server itself runs on a daemon
    // reactor thread plus per-connection virtual threads.
    Thread.currentThread().join();
}

/// Echoes text and binary messages back to the sender.
private static final class EchoHandler implements WebSocketHandler {

    @Override
    public void onOpen(WebSocketSession session) {
        IO.println("[" + session.id() + "] connected from "
                + session.remoteAddress() + " on path " + session.path());
    }

    @Override
    public void onTextMessage(WebSocketSession session, String message) {
        IO.println("[" + session.id() + "] text: " + message);
        // Callbacks run on the connection's virtual thread, so blocking
        // sends are fine and never stall other connections.
        session.sendText(message);
    }

    @Override
    public void onBinaryMessage(WebSocketSession session, ByteBuffer payload) {
        IO.println("[" + session.id() + "] binary: " + payload.remaining() + " bytes");
        // The payload buffer may be recycled after the callback returns,
        // so send it directly within the callback.
        session.sendBinary(payload);
    }

    @Override
    public void onClose(WebSocketSession session, int statusCode, String reason) {
        IO.println("[" + session.id() + "] closed with status "
                + statusCode + " (" + reason + ")");
    }

    @Override
    public void onError(WebSocketSession session, Throwable error) {
        IO.println("[" + session.id() + "] error: " + error.getMessage());
    }
}
