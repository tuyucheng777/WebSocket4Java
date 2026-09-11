package io.github.tuyucheng777.websocket.core;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/// Single-threaded NIO Reactor (internal implementation class, not part of
/// the public API compatibility promise).
///
/// Accept, read-ready and write-ready events for all connections are
/// dispatched on the single daemon platform thread ({@code websocket-nio}),
/// avoiding the synchronization cost and thundering herd of multiple
/// selectors. Business callbacks run on each connection's own virtual thread
/// and never block the reactor; outbound writes likewise execute only on the
/// reactor thread, while business threads request interestOps updates via the
/// dirty connection queue.
public final class NioReactor {

    /// Factory: used by the server to construct connection objects on accept.
    @FunctionalInterface
    public interface ConnectionFactory {
        /// Creates a connection for a newly accepted channel.
        ///
        /// @param channel the channel already configured as non-blocking
        /// @return the connection object
        Connection create(SocketChannel channel) throws IOException;
    }

    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final ConnectionFactory connectionFactory;
    private final ConcurrentLinkedQueue<Connection> dirtyConnections = new ConcurrentLinkedQueue<>();

    private volatile boolean running;
    private Thread thread;

    /// Constructs the reactor and registers the server socket channel with the
    /// selector.
    ///
    /// @param serverChannel     the already bound, non-blocking server socket
    ///                          channel
    /// @param connectionFactory the connection factory
    /// @throws IOException if creating the selector or registration fails
    public NioReactor(ServerSocketChannel serverChannel, ConnectionFactory connectionFactory)
            throws IOException {
        this.serverChannel = serverChannel;
        this.connectionFactory = connectionFactory;
        this.selector = SelectorProvider.provider().openSelector();
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    /// Starts the reactor thread.
    public void start() {
        if (thread != null) {
            throw new IllegalStateException("reactor already started");
        }
        running = true;
        thread = Thread.ofPlatform()
                .name("websocket-nio")
                .daemon(true)
                .start(this::runLoop);
    }

    /// Requests shutdown: wakes up the blocked selector; the selector is closed
    /// after the loop ends.
    public void shutdown() {
        running = false;
        selector.wakeup();
    }

    /// Waits for the reactor thread to terminate.
    ///
    /// @param timeoutMillis the maximum wait in milliseconds
    /// @throws InterruptedException if the wait is interrupted
    public void awaitTermination(long timeoutMillis) throws InterruptedException {
        if (thread != null) {
            thread.join(timeoutMillis);
        }
    }

    /// Wakes up a blocked {@link Selector#select()}.
    void wakeup() {
        selector.wakeup();
    }

    /// Marks that the connection's interestOps need to be recomputed on the
    /// reactor thread.
    ///
    /// @param connection the connection whose write queue changed
    void markDirty(Connection connection) {
        dirtyConnections.offer(connection);
        selector.wakeup();
    }

    private void runLoop() {
        while (running) {
            try {
                selector.select(1_000L);
            } catch (IOException e) {
                if (running) {
                    Thread.currentThread().interrupt();
                }
                break;
            }
            if (!running) {
                break;
            }
            processSelectedKeys();
            processDirtyConnections();
        }
        processDirtyConnections();
        try {
            selector.close();
        } catch (IOException ignored) {
            // Exceptions during shutdown are meaningless.
        }
    }

    private void processSelectedKeys() {
        Set<SelectionKey> keys = selector.selectedKeys();
        for (Iterator<SelectionKey> iterator = keys.iterator(); iterator.hasNext(); ) {
            SelectionKey key = iterator.next();
            iterator.remove();
            if (!key.isValid()) {
                continue;
            }
            try {
                if (key.isAcceptable()) {
                    acceptConnections();
                }
                Object attachment = key.attachment();
                if (attachment instanceof Connection connection) {
                    if (key.isReadable()) {
                        connection.onReadable();
                    }
                    if (key.isValid() && key.isWritable()) {
                        connection.onWritable();
                    }
                    if (key.isValid()) {
                        connection.rebuildInterest();
                    }
                }
            } catch (CancelledKeyException ignored) {
                // The connection was closed during processing and the key has
                // been cancelled.
            } catch (IOException e) {
                if (key.attachment() instanceof Connection connection) {
                    connection.handleIoError(e);
                }
            }
        }
    }

    private void acceptConnections() throws IOException {
        SocketChannel channel;
        while ((channel = serverChannel.accept()) != null) {
            try {
                channel.configureBlocking(false);
                channel.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
                Connection connection = connectionFactory.create(channel);
                SelectionKey key = channel.register(selector, SelectionKey.OP_READ, connection);
                connection.attachKey(key);
            } catch (IOException e) {
                closeQuietly(channel);
                throw e;
            }
        }
    }

    private void processDirtyConnections() {
        Connection connection;
        while ((connection = dirtyConnections.poll()) != null) {
            connection.rebuildInterest();
        }
    }

    private static void closeQuietly(SelectableChannel channel) {
        try {
            channel.close();
        } catch (IOException ignored) {
            // Ignore close exceptions.
        }
    }
}
