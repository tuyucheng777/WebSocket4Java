package io.github.tuyucheng777.websocket;

import io.github.tuyucheng777.websocket.core.Connection;
import io.github.tuyucheng777.websocket.core.NioReactor;
import io.github.tuyucheng777.websocket.core.ServerConfig;
import io.github.tuyucheng777.websocket.frame.CloseStatus;
import io.github.tuyucheng777.websocket.route.PathPattern;
import io.github.tuyucheng777.websocket.route.Route;
import io.github.tuyucheng777.websocket.route.Router;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/// High-performance WebSocket server based on JDK NIO and virtual threads.
///
/// ## Architecture
///
/// - A single daemon reactor thread ({@code websocket-nio}) handles accept,
///   read-ready and write-ready events for all connections, using non-blocking
///   channels and gathering writes;
/// - Each connection has its own virtual thread that serially executes the
///   business {@link WebSocketHandler} callbacks, so blocking business logic
///   never slows down other connections;
/// - Outbound writes are flushed uniformly by the reactor from per-connection
///   queues, with high/low water mark backpressure;
/// - Zero third-party dependencies in runtime code.
///
/// ## Usage
///
/// ```java
/// WebSocketServer server = WebSocketServer.builder()
///         .port(8080)
///         .path("/echo", new EchoHandler())
///         .build();
/// server.start();
/// // ...
/// server.close();
/// ```
public final class WebSocketServer implements AutoCloseable {

    private final ServerConfig config;
    private final Router router;
    private final ConcurrentHashMap<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    private volatile ServerSocketChannel serverChannel;
    private volatile NioReactor reactor;
    private volatile boolean started;

    private WebSocketServer(ServerConfig config, Router router) {
        this.config = config;
        this.router = router;
    }

    /// Creates a builder.
    ///
    /// @return the server builder
    public static Builder builder() {
        return new Builder();
    }

    /// Binds the port and starts the reactor.
    ///
    /// This method returns immediately; I/O runs on a background daemon thread.
    /// When the port is {@code 0}, the actual port assigned by the operating
    /// system can be obtained via {@link #getBoundPort()}.
    ///
    /// @throws IOException if binding or startup fails
    public synchronized void start() throws IOException {
        if (started) {
            throw new IllegalStateException("server already started");
        }
        ServerSocketChannel channel = ServerSocketChannel.open();
        try {
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            channel.bind(new InetSocketAddress(config.host(), config.port()), config.backlog());
            channel.configureBlocking(false);
            NioReactor newReactor = new NioReactor(channel,
                    socketChannel -> new Connection(
                            reactor, config, router, sessions, socketChannel));
            this.serverChannel = channel;
            this.reactor = newReactor;
            newReactor.start();
            started = true;
        } catch (IOException | RuntimeException e) {
            try {
                channel.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /// Returns the actually bound port.
    ///
    /// @return the listening port
    /// @throws IllegalStateException if the server has not started yet
    public int getBoundPort() {
        ServerSocketChannel channel = serverChannel;
        if (channel == null) {
            throw new IllegalStateException("server not started yet");
        }
        try {
            return ((InetSocketAddress) channel.getLocalAddress()).getPort();
        } catch (IOException e) {
            throw new IllegalStateException("unable to obtain bound port", e);
        }
    }

    /// Returns a snapshot of the currently online sessions.
    ///
    /// @return an immutable collection of sessions
    public Collection<WebSocketSession> sessions() {
        return Collections.unmodifiableCollection(new ArrayList<>(sessions.values()));
    }

    /// Number of currently online connections.
    ///
    /// @return the connection count
    public int connectionCount() {
        return sessions.size();
    }

    /// Broadcasts a text message to all online sessions.
    ///
    /// A send failure for a single session (such as a closed session) is simply
    /// skipped and does not affect the remaining sessions.
    ///
    /// @param text the text message
    public void broadcastText(String text) {
        for (WebSocketSession session : sessions()) {
            try {
                session.sendText(text);
            } catch (RuntimeException _) {
                // Skip invalid sessions.
            }
        }
    }

    /// Broadcasts a binary message to all online sessions.
    ///
    /// Each session receives its own buffer duplicate so that consumption
    /// positions do not interfere with one another.
    ///
    /// @param payload the binary message
    public void broadcastBinary(ByteBuffer payload) {
        for (WebSocketSession session : sessions()) {
            try {
                session.sendBinary(payload.duplicate());
            } catch (RuntimeException _) {
                // Skip invalid sessions.
            }
        }
    }

    /// Graceful shutdown: first stops accepting new connections, sends a
    /// {@code 1001} close frame to all sessions, and terminates the reactor
    /// after waiting for the close handshake data to be flushed.
    @Override
    public synchronized void close() {
        if (!started) {
            return;
        }
        started = false;
        ServerSocketChannel channel = serverChannel;
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException _) {
                // Ignore exceptions from closing the server socket channel.
            }
        }
        for (WebSocketSession session : new ArrayList<>(sessions.values())) {
            try {
                session.close(CloseStatus.GOING_AWAY, "server shutdown");
            } catch (RuntimeException _) {
                // Ignore close races.
            }
        }
        // Wait up to 2 seconds for the reactor to flush close frames and finish
        // hard-closing each connection.
        long deadline = System.currentTimeMillis() + 2_000L;
        while (!sessions.isEmpty() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(5L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        NioReactor runningReactor = reactor;
        if (runningReactor != null) {
            runningReactor.shutdown();
            try {
                runningReactor.awaitTermination(2_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// Server builder.
    public static final class Builder {

        private String host = "0.0.0.0";
        private int port;
        private int backlog = 1_024;
        private long maxFramePayloadSize = 1L << 20;
        private int readBufferSize = 16 * 1024;
        private int maxHandshakeSize = 64 * 1024;
        private int writeBufferHighWaterMark = 256 * 1024;
        private int writeBufferLowWaterMark = 64 * 1024;
        private int eventQueueCapacity = 1_024;
        private final List<Route> routes = new ArrayList<>();

        private Builder() {
        }

        /// Sets the bind address; defaults to {@code 0.0.0.0}.
        ///
        /// @param host the bind address
        /// @return this builder
        public Builder host(String host) {
            this.host = host;
            return this;
        }

        /// Sets the bind port; defaults to {@code 0} (assigned by the OS).
        ///
        /// @param port the port
        /// @return this builder
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /// Sets the TCP accept queue length.
        ///
        /// @param backlog the queue length
        /// @return this builder
        public Builder backlog(int backlog) {
            this.backlog = backlog;
            return this;
        }

        /// Limits the maximum payload size in bytes of a single message;
        /// exceeding it closes the connection with {@code 1009}. Default 1 MB.
        ///
        /// @param bytes the maximum payload size
        /// @return this builder
        public Builder maxFramePayloadSize(long bytes) {
            this.maxFramePayloadSize = bytes;
            return this;
        }

        /// Sets the per-connection read buffer size; defaults to 16 KB.
        ///
        /// @param bytes the buffer size in bytes
        /// @return this builder
        public Builder readBufferSize(int bytes) {
            this.readBufferSize = bytes;
            return this;
        }

        /// Sets the handshake request header size limit; defaults to 64 KB.
        ///
        /// @param bytes the maximum number of bytes
        /// @return this builder
        public Builder maxHandshakeSize(int bytes) {
            this.maxHandshakeSize = bytes;
            return this;
        }

        /// Sets the send backpressure water marks.
        ///
        /// @param highWaterMark the high water mark; the sender suspends when
        ///                      queued bytes exceed this value
        /// @param lowWaterMark  the low water mark; the sender is woken up when
        ///                      queued bytes fall back to this value
        /// @return this builder
        public Builder writeBufferWaterMark(int highWaterMark, int lowWaterMark) {
            this.writeBufferHighWaterMark = highWaterMark;
            this.writeBufferLowWaterMark = lowWaterMark;
            return this;
        }

        /// Sets the per-connection business event queue capacity;
        /// defaults to 1024.
        ///
        /// @param capacity the queue capacity
        /// @return this builder
        public Builder eventQueueCapacity(int capacity) {
            this.eventQueueCapacity = capacity;
            return this;
        }

        /// Registers a route.
        ///
        /// Paths support exact matching ({@code /echo}) and path variables
        /// ({@code /chat/{room}}); exact paths take precedence over variable
        /// patterns.
        ///
        /// @param pattern the path pattern
        /// @param handler the handler
        /// @return this builder
        public Builder path(String pattern, WebSocketHandler handler) {
            if (handler == null) {
                throw new IllegalArgumentException("handler must not be null");
            }
            routes.add(new Route(PathPattern.compile(pattern), handler));
            return this;
        }

        /// Builds the server instance.
        ///
        /// @return the unstarted server
        public WebSocketServer build() {
            ServerConfig serverConfig = new ServerConfig(
                    host, port, backlog, maxFramePayloadSize, readBufferSize,
                    maxHandshakeSize, writeBufferHighWaterMark,
                    writeBufferLowWaterMark, eventQueueCapacity);
            return new WebSocketServer(serverConfig, new Router(routes));
        }
    }
}
