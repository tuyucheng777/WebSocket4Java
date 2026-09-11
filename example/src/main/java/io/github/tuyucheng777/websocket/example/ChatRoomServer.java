import io.github.tuyucheng777.websocket.WebSocketHandler;
import io.github.tuyucheng777.websocket.WebSocketServer;
import io.github.tuyucheng777.websocket.WebSocketSession;

/// Minimal chat room server.
///
/// Demonstrates path variables ({@code /chat/{room}}), query parameters,
/// per-session attributes, and room-scoped broadcast. The framework does not
/// provide a built-in router for arbitrary topics, so a small in-memory room
/// registry is used here.
///
/// Connect with:
///
/// ```
    /// websocat ws://localhost:8080/chat/general?name=alice
    /// websocat ws://localhost:8080/chat/general?name=bob
    /// ```
///
/// Messages from one client are relayed to every other client in the same room,
/// prefixed with the sender's {@code name} query parameter.
void main() throws Exception {
    // One registry per room name. A CopyOnWriteArrayList keeps iteration
    // cheap and lock-free; for high-throughput rooms a concurrent map or
    // striped lock would be more appropriate.
    Map<String, List<WebSocketSession>> rooms = new ConcurrentHashMap<>();

    WebSocketServer server = WebSocketServer.builder()
            .host("0.0.0.0")
            .port(8080)
            .path("/chat/{room}", new ChatHandler(rooms))
            .build();

    server.start();
    IO.println("Chat server listening on ws://0.0.0.0:8080/chat/{room}?name=...");

    Runtime.getRuntime().addShutdownHook(new Thread(server::close));
    Thread.currentThread().join();
}

/// Broadcasts text messages within the resolved room.
private static final class ChatHandler implements WebSocketHandler {

    private final Map<String, List<WebSocketSession>> rooms;

    ChatHandler(Map<String, List<WebSocketSession>> rooms) {
        this.rooms = rooms;
    }

    @Override
    public void onOpen(WebSocketSession session) {
        String room = session.pathVariable("room");
        String name = session.queryParameter("name");
        if (name == null || name.isBlank()) {
            name = "guest-" + session.id();
        }
        // attributes() is a mutable map shared between callbacks of the
        // same session; it is the recommended way to carry connection state.
        session.attributes().put("room", room);
        session.attributes().put("name", name);

        rooms.computeIfAbsent(room, _ -> new CopyOnWriteArrayList<>())
                .add(session);

        broadcast(room, "* " + name + " joined the room (" + room + ")");
        IO.println("[" + session.id() + "] " + name + " joined " + room);
    }

    @Override
    public void onTextMessage(WebSocketSession session, String message) {
        String room = (String) session.attributes().get("room");
        String name = (String) session.attributes().get("name");
        broadcast(room, name + ": " + message);
    }

    @Override
    public void onClose(WebSocketSession session, int statusCode, String reason) {
        String room = (String) session.attributes().get("room");
        String name = (String) session.attributes().get("name");
        List<WebSocketSession> members = rooms.get(room);
        if (members != null) {
            members.remove(session);
        }
        broadcast(room, "* " + name + " left the room");
        IO.println("[" + session.id() + "] " + name + " left " + room);
    }

    private void broadcast(String room, String text) {
        List<WebSocketSession> members = rooms.get(room);
        if (members == null) {
            return;
        }
        for (WebSocketSession session : members) {
            try {
                session.sendText(text);
            } catch (RuntimeException _) {
                // Session closed concurrently; the next onClose will clean it up.
            }
        }
    }
}
