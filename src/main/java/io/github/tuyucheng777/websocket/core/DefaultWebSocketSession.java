package io.github.tuyucheng777.websocket.core;

import io.github.tuyucheng777.websocket.WebSocketSession;
import io.github.tuyucheng777.websocket.frame.CloseStatus;
import io.github.tuyucheng777.websocket.http.HttpRequest;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/// Default implementation of {@link WebSocketSession}, purely forwarding to
/// {@link Connection}.
///
/// The session object is created after a successful handshake and shares the
/// connection's lifecycle; the attribute map uses a concurrent map, allowing
/// business threads to read and write freely.
final class DefaultWebSocketSession implements WebSocketSession {

    private final Connection connection;
    private final String id;
    private final String path;
    private final Map<String, String> pathVariables;
    private final InetSocketAddress remoteAddress;
    private final ConcurrentHashMap<String, Object> attributes = new ConcurrentHashMap<>();

    /// Constructs a session view.
    ///
    /// @param connection the owning connection
    DefaultWebSocketSession(Connection connection) {
        this.connection = connection;
        this.id = connection.id;
        HttpRequest request = connection.request();
        this.path = request.path();
        this.pathVariables = connection.pathVariables();
        this.remoteAddress = connection.remoteAddress();
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String path() {
        return path;
    }

    @Override
    public Map<String, String> pathVariables() {
        return pathVariables;
    }

    @Override
    public String pathVariable(String name) {
        return pathVariables.get(name);
    }

    @Override
    public Map<String, String> queryParameters() {
        return connection.request().queryParameters();
    }

    @Override
    public String queryParameter(String name) {
        return connection.request().query(name);
    }

    @Override
    public Map<String, List<String>> headers() {
        return connection.request().headers();
    }

    @Override
    public String header(String name) {
        return connection.request().header(name);
    }

    @Override
    public InetSocketAddress remoteAddress() {
        return remoteAddress;
    }

    @Override
    public Map<String, Object> attributes() {
        return attributes;
    }

    @Override
    public boolean isOpen() {
        return connection.isOpen();
    }

    @Override
    public void sendText(String text) {
        connection.sendText(text);
    }

    @Override
    public void sendBinary(ByteBuffer payload) {
        connection.sendBinary(payload);
    }

    @Override
    public void sendTextFragment(String fragment, boolean last) {
        connection.sendTextFragment(fragment, last);
    }

    @Override
    public void sendBinaryFragment(ByteBuffer fragment, boolean last) {
        connection.sendBinaryFragment(fragment, last);
    }

    @Override
    public void sendPing(ByteBuffer payload) {
        connection.sendPing(payload);
    }

    @Override
    public void sendPong(ByteBuffer payload) {
        connection.sendPong(payload);
    }

    @Override
    public void close() {
        connection.initiateClose(CloseStatus.NORMAL, "");
    }

    @Override
    public void close(int statusCode, String reason) {
        connection.initiateClose(statusCode, reason);
    }
}
