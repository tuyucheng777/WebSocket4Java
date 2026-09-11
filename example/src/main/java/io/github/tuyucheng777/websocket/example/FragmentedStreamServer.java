import io.github.tuyucheng777.websocket.WebSocketHandler;
import io.github.tuyucheng777.websocket.WebSocketServer;
import io.github.tuyucheng777.websocket.WebSocketSession;

/// Server that streams a large text message back to the client using
/// WebSocket fragmentation.
///
/// Demonstrates the {@link WebSocketSession#sendTextFragment(String, boolean)}
/// API: the first fragment starts the message, intermediate fragments are
/// continuation frames, and the final fragment ends it. While a fragment
/// sequence is in progress on a session, no other message may be sent on that
/// same session.
///
/// Connect and send any text message; the server replies with a long string
/// delivered in fixed-size fragments:
///
/// ```
/// websocat ws://localhost:8080/stream
/// ```

/// Size of each text fragment sent on the wire.
private static final int FRAGMENT_SIZE = 16;

void main() throws Exception {
    WebSocketServer server = WebSocketServer.builder()
            .host("0.0.0.0")
            .port(8080)
            .path("/stream", new StreamHandler())
            .build();

    server.start();
    IO.println("Fragmented stream server listening on ws://0.0.0.0:8080/stream");

    Runtime.getRuntime().addShutdownHook(new Thread(server::close));
    Thread.currentThread().join();
}

/// Replies to every text message with a large payload sent in fragments.
private static final class StreamHandler implements WebSocketHandler {

    @Override
    public void onTextMessage(WebSocketSession session, String message) {
        // Build a large ASCII payload by repeating the incoming message.
        StringBuilder body = new StringBuilder();
        while (body.length() < 256) {
            body.append(message).append(' ');
        }
        String payload = body.toString();

        // Send the payload in FRAGMENT_SIZE chunks. The framework marks
        // the first frame with the TEXT opcode and the following frames
        // with CONTINUATION; only the last frame has the FIN bit set.
        int offset = 0;
        while (offset < payload.length()) {
            int end = Math.min(offset + FRAGMENT_SIZE, payload.length());
            boolean last = end == payload.length();
            session.sendTextFragment(payload.substring(offset, end), last);
            offset = end;
        }

        // Binary fragmentation works the same way via sendBinaryFragment.
        ByteBuffer binary = StandardCharsets.UTF_8.encode("(binary trailer)");
        session.sendBinary(binary);
    }
}