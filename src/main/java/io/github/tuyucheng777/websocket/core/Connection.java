package io.github.tuyucheng777.websocket.core;

import io.github.tuyucheng777.websocket.WebSocketException;
import io.github.tuyucheng777.websocket.WebSocketHandler;
import io.github.tuyucheng777.websocket.WebSocketSession;
import io.github.tuyucheng777.websocket.frame.CloseStatus;
import io.github.tuyucheng777.websocket.frame.FrameDecoder;
import io.github.tuyucheng777.websocket.frame.FrameEncoder;
import io.github.tuyucheng777.websocket.frame.FrameListener;
import io.github.tuyucheng777.websocket.frame.Opcode;
import io.github.tuyucheng777.websocket.frame.ProtocolException;
import io.github.tuyucheng777.websocket.http.HandshakeException;
import io.github.tuyucheng777.websocket.http.Handshaker;
import io.github.tuyucheng777.websocket.http.HttpParser;
import io.github.tuyucheng777.websocket.http.HttpRequest;
import io.github.tuyucheng777.websocket.route.Route;
import io.github.tuyucheng777.websocket.route.Router;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/// Complete state machine for a single TCP connection (internal implementation
/// class, not part of the public API compatibility promise).
///
/// A connection plays three roles at once:
///
/// - {@link FrameListener} on the reactor thread side: handshake parsing, frame
///   decode callbacks, and flushing outbound writes;
/// - send facade on the business virtual thread side: backpressure enqueueing,
///   fragment state maintenance, and close handshake initiation;
/// - backing implementation for {@link DefaultWebSocketSession}.
///
/// Threading model: all channel reads and writes happen only on the reactor
/// thread; business threads only touch {@link #writeQueue} (waiting on the
/// {@link #writeLock} monitor) and request interestOps updates via
/// {@link NioReactor#markDirty}.
public final class Connection implements FrameListener {

    private static final AtomicLong ID_SEQUENCE = new AtomicLong();

    /// Connection is open and can send and receive normally.
    private static final int STATE_OPEN = 0;
    /// Close frame has been sent or received; waiting for flush before hard
    /// close.
    private static final int STATE_CLOSING = 1;

    // ---- Infrastructure ----
    final String id = Long.toHexString(ID_SEQUENCE.incrementAndGet());
    private final NioReactor reactor;
    private final ServerConfig config;
    private final Router router;
    private final ConcurrentHashMap<String, WebSocketSession> sessionRegistry;
    private final SocketChannel channel;
    private final InetSocketAddress remoteAddress;
    private SelectionKey selectionKey;

    // ---- Handshake phase ----
    private boolean handshakeCompleted;
    private final ByteArrayOutputStream handshakeBuffer = new ByteArrayOutputStream();
    private HttpRequest request;

    // ---- WebSocket phase ----
    private WebSocketHandler handler;
    private Map<String, String> pathVariables = Map.of();
    private FrameDecoder decoder;
    private DefaultWebSocketSession session;
    private final ByteBuffer readBuffer;

    // ---- Send queue (reactor thread writes, business threads enqueue) ----
    private final Object writeLock = new Object();
    private final ArrayDeque<ByteBuffer> writeQueue = new ArrayDeque<>();
    private long queuedBytes;
    private boolean closeAfterFlush;

    // ---- Lifecycle ----
    private final AtomicBoolean hardClosed = new AtomicBoolean(false);
    private final AtomicBoolean closeCallbackFired = new AtomicBoolean(false);
    private volatile int state = STATE_OPEN;
    private volatile int finalCloseStatus = CloseStatus.CLOSED_ABNORMALLY;
    private volatile String finalCloseReason = "";

    // ---- Business event virtual thread ----
    private final LinkedBlockingQueue<Event> eventQueue;
    private Thread eventThread;

    // ---- Outbound fragment state ----
    private final Object fragmentLock = new Object();
    private int outgoingFragmentOpcode;

    /// Constructs a connection in the handshake phase.
    ///
    /// @param reactor         the owning reactor
    /// @param config          the server configuration
    /// @param router          the route table
    /// @param sessionRegistry the session registry
    /// @param channel         the accepted non-blocking channel
    public Connection(NioReactor reactor, ServerConfig config, Router router,
                      ConcurrentHashMap<String, WebSocketSession> sessionRegistry,
                      SocketChannel channel) throws IOException {
        this.reactor = reactor;
        this.config = config;
        this.router = router;
        this.sessionRegistry = sessionRegistry;
        this.channel = channel;
        this.readBuffer = ByteBuffer.allocate(config.readBufferSize());
        this.eventQueue = new LinkedBlockingQueue<>(config.eventQueueCapacity());
        InetSocketAddress address = null;
        try {
            address = (InetSocketAddress) channel.getRemoteAddress();
        } catch (IOException _) {
            // May be null when the remote address is unavailable.
        }
        this.remoteAddress = address;
    }

    /// Callback after the selection key is registered.
    ///
    /// @param key this connection's selection key
    void attachKey(SelectionKey key) {
        this.selectionKey = key;
    }

    // ===================================================================
    // Reactor thread: read path
    // ===================================================================

    /// Read-ready callback: drains the kernel receive buffer as much as
    /// possible and dispatches bytes according to the current phase.
    void onReadable() throws IOException {
        int n;
        do {
            readBuffer.clear();
            n = channel.read(readBuffer);
            if (n > 0) {
                readBuffer.flip();
                if (handshakeCompleted) {
                    decoder.feed(readBuffer);
                } else {
                    feedHandshake(readBuffer);
                }
            }
        } while (n > 0 && !hardClosed.get());
        if (n < 0 && hardClosed.compareAndSet(false, true)) {
            // Peer sent FIN directly without going through the close handshake.
            finishHardClose();
        }
    }

    /// Unified entry point for I/O exceptions (reactor callback).
    ///
    /// @param error the underlying I/O exception
    void handleIoError(IOException error) {
        if (handshakeCompleted) {
            dispatchError(error);
        }
        if (hardClosed.compareAndSet(false, true)) {
            finishHardClose();
        }
    }

    private void feedHandshake(ByteBuffer in) {
        byte[] chunk = new byte[in.remaining()];
        in.get(chunk);
        if (handshakeBuffer.size() + chunk.length > config.maxHandshakeSize()) {
            rejectHandshake(431);
            return;
        }
        handshakeBuffer.writeBytes(chunk);
        byte[] all = handshakeBuffer.toByteArray();
        int headEnd = HttpParser.indexOfHeadEnd(all, all.length);
        if (headEnd < 0) {
            return;
        }
        HttpRequest parsed;
        try {
            parsed = HttpParser.parse(all, headEnd);
        } catch (HandshakeException e) {
            rejectHandshake(e.httpStatus());
            return;
        }
        try {
            Handshaker.validate(parsed);
        } catch (HandshakeException e) {
            rejectHandshake(e.httpStatus());
            return;
        }
        Route.Match match = router.route(parsed.path());
        if (match == null) {
            rejectHandshake(404);
            return;
        }

        this.request = parsed;
        this.handler = match.handler();
        this.pathVariables = match.pathVariables();
        this.session = new DefaultWebSocketSession(this);
        this.decoder = new FrameDecoder(this, config.maxFramePayloadSize());

        try {
            enqueueIo(ByteBuffer.wrap(Handshaker.upgradeResponse(parsed)));
        } catch (HandshakeException e) {
            rejectHandshake(e.httpStatus());
            return;
        }
        handshakeCompleted = true;
        sessionRegistry.put(id, session);
        startEventThread();

        int leftoverStart = headEnd + HttpParser.HEAD_TERMINATOR.length;
        if (leftoverStart < all.length) {
            decoder.feed(ByteBuffer.wrap(all, leftoverStart, all.length - leftoverStart));
        }
        flushNow();
    }

    private void rejectHandshake(int httpStatus) {
        String reasonPhrase = switch (httpStatus) {
            case 400 -> "Bad Request";
            case 404 -> "Not Found";
            case 405 -> "Method Not Allowed";
            case 426 -> "Upgrade Required";
            case 431 -> "Request Header Fields Too Large";
            default -> "Error";
        };
        enqueueIo(ByteBuffer.wrap(Handshaker.errorResponse(httpStatus, reasonPhrase)));
        flushNow();
        if (hardClosed.compareAndSet(false, true)) {
            finishHardClose();
        }
    }

    // ===================================================================
    // Reactor thread: FrameListener callbacks
    // ===================================================================

    @Override
    public void onTextMessage(ByteBuffer payload) {
        if (state != STATE_OPEN) {
            return;
        }
        byte[] data = new byte[payload.remaining()];
        payload.get(data);
        offerEvent(new TextEvent(new String(data, StandardCharsets.UTF_8)));
    }

    @Override
    public void onBinaryMessage(ByteBuffer payload) {
        if (state != STATE_OPEN) {
            return;
        }
        byte[] data = new byte[payload.remaining()];
        payload.get(data);
        offerEvent(new BinaryEvent(ByteBuffer.wrap(data)));
    }

    @Override
    public void onPing(ByteBuffer payload) {
        if (state == STATE_OPEN) {
            // Automatically respond with a Pong, echoing the payload as-is
            // (RFC 6455 section 5.5.3).
            ByteBuffer copy = ByteBuffer.allocate(payload.remaining());
            copy.put(payload).flip();
            enqueueIo(FrameEncoder.pong(copy));
            flushNow();
            offerEvent(new PingEvent(copy));
        }
    }

    @Override
    public void onPong(ByteBuffer payload) {
        if (state != STATE_OPEN) {
            return;
        }
        ByteBuffer copy = ByteBuffer.allocate(payload.remaining());
        copy.put(payload).flip();
        offerEvent(new PongEvent(copy));
    }

    @Override
    public void onClose(int statusCode, String reason) {
        if (hardClosed.get()) {
            return;
        }
        synchronized (writeLock) {
            if (state == STATE_OPEN) {
                state = STATE_CLOSING;
                finalCloseStatus = statusCode;
                finalCloseReason = reason;
                if (statusCode == CloseStatus.NO_STATUS_CODE) {
                    enqueueIo(FrameEncoder.closeEmpty());
                } else {
                    enqueueIo(FrameEncoder.close(statusCode, ""));
                }
            }
            closeAfterFlush = true;
        }
        flushNow();
    }

    @Override
    public void onProtocolError(int closeStatus, String message) {
        dispatchError(new ProtocolException(closeStatus, message));
        initiateClose(closeStatus, "protocol error");
    }

    private void offerEvent(Event event) {
        if (!eventQueue.offer(event)) {
            // Business consumption is severely lagging; terminate as an
            // internal error.
            initiateClose(CloseStatus.INTERNAL_ERROR, "event queue overflow");
        }
    }

    // ===================================================================
    // Reactor thread: write path
    // ===================================================================

    /// Write-ready callback.
    void onWritable() {
        flushNow();
    }

    /// Immediately attempts to flush all queued data on the reactor thread.
    void flushNow() {
        boolean shouldHardClose;
        synchronized (writeLock) {
            while (!writeQueue.isEmpty()) {
                ByteBuffer head = writeQueue.peek();
                int written;
                try {
                    written = channel.write(head);
                } catch (IOException e) {
                    handleIoError(e);
                    return;
                }
                if (written == 0 || head.hasRemaining()) {
                    break;
                }
                writeQueue.poll();
                queuedBytes -= head.limit();
            }
            if (queuedBytes <= config.writeBufferLowWaterMark()) {
                writeLock.notifyAll();
            }
            shouldHardClose = closeAfterFlush && writeQueue.isEmpty();
        }
        if (shouldHardClose && hardClosed.compareAndSet(false, true)) {
            finishHardClose();
        }
    }

    /// Recomputes the selection key interestOps; called only on the reactor
    /// thread.
    void rebuildInterest() {
        SelectionKey key = selectionKey;
        if (key == null || !key.isValid()) {
            return;
        }
        int ops = SelectionKey.OP_READ;
        synchronized (writeLock) {
            if (!writeQueue.isEmpty()) {
                ops |= SelectionKey.OP_WRITE;
            }
        }
        if (key.interestOps() != ops) {
            key.interestOps(ops);
        }
    }

    private void enqueueIo(ByteBuffer[] buffers) {
        synchronized (writeLock) {
            for (ByteBuffer buffer : buffers) {
                if (buffer.remaining() > 0) {
                    writeQueue.addLast(buffer);
                    queuedBytes += buffer.remaining();
                }
            }
        }
    }

    private void enqueueIo(ByteBuffer buffer) {
        if (buffer.remaining() > 0) {
            synchronized (writeLock) {
                writeQueue.addLast(buffer);
                queuedBytes += buffer.remaining();
            }
        }
    }

    // ===================================================================
    // Business threads: send API (forwarded by DefaultWebSocketSession)
    // ===================================================================

    /// Enqueues a frame sequence, suspending on the virtual thread when the
    /// backlog exceeds the high water mark.
    ///
    /// The current frame is always enqueued first (a single frame must be
    /// sendable even if it is larger than the high water mark, otherwise it
    /// would self-deadlock), then waits for the backlog to fall below the high
    /// water mark, so the {@code while} loop blocks subsequent sends and forms
    /// backpressure against fast producers.
    ///
    /// @param buffers the frame header and payload
    void enqueueWithBackpressure(ByteBuffer[] buffers) {
        synchronized (writeLock) {
            if (state != STATE_OPEN || hardClosed.get()) {
                throw new WebSocketException("session closed, cannot send message");
            }
            for (ByteBuffer buffer : buffers) {
                if (buffer.remaining() > 0) {
                    writeQueue.addLast(buffer);
                    queuedBytes += buffer.remaining();
                }
            }
            // The reactor must be notified before suspending: the water mark
            // wait holds the write lock and blocks this thread the whole time;
            // if the wake-up were placed after the wait, the reactor would
            // never know there is new data to write.
            reactor.markDirty(this);
            boolean interrupted = false;
            while (state == STATE_OPEN && !hardClosed.get()
                    && queuedBytes > config.writeBufferHighWaterMark()) {
                try {
                    writeLock.wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                    break;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
                throw new WebSocketException("send wait interrupted");
            }
            if (state != STATE_OPEN || hardClosed.get()) {
                throw new WebSocketException("session closed, cannot send message");
            }
        }
    }

    /// Initiates the close handshake (idempotent, may be called from any
    /// thread).
    ///
    /// @param statusCode the close status code
    /// @param reason     the close reason
    void initiateClose(int statusCode, String reason) {
        synchronized (writeLock) {
            if (state != STATE_OPEN || hardClosed.get()) {
                return;
            }
            state = STATE_CLOSING;
            finalCloseStatus = statusCode;
            finalCloseReason = reason == null ? "" : reason;
            if (CloseStatus.isSendable(statusCode)) {
                enqueueIo(FrameEncoder.close(statusCode, reason));
            } else {
                enqueueIo(FrameEncoder.closeEmpty());
            }
            closeAfterFlush = true;
            writeLock.notifyAll();
        }
        reactor.markDirty(this);
    }

    void sendText(String text) {
        synchronized (fragmentLock) {
            if (outgoingFragmentOpcode != 0) {
                throw new IllegalStateException("fragmented message not finished, cannot insert a complete message");
            }
        }
        enqueueWithBackpressure(FrameEncoder.text(text));
    }

    void sendBinary(ByteBuffer payload) {
        synchronized (fragmentLock) {
            if (outgoingFragmentOpcode != 0) {
                throw new IllegalStateException("fragmented message not finished, cannot insert a complete message");
            }
        }
        enqueueWithBackpressure(FrameEncoder.binary(payload));
    }

    void sendTextFragment(String fragment, boolean last) {
        ByteBuffer payload = ByteBuffer.wrap(fragment.getBytes(StandardCharsets.UTF_8));
        synchronized (fragmentLock) {
            int opcode;
            if (outgoingFragmentOpcode == 0) {
                opcode = Opcode.TEXT;
                outgoingFragmentOpcode = Opcode.TEXT;
            } else {
                opcode = Opcode.CONTINUATION;
            }
            if (last) {
                outgoingFragmentOpcode = 0;
            }
            enqueueWithBackpressure(FrameEncoder.fragment(opcode, payload, last));
        }
    }

    void sendBinaryFragment(ByteBuffer fragment, boolean last) {
        synchronized (fragmentLock) {
            int opcode;
            if (outgoingFragmentOpcode == 0) {
                opcode = Opcode.BINARY;
                outgoingFragmentOpcode = Opcode.BINARY;
            } else {
                opcode = Opcode.CONTINUATION;
            }
            if (last) {
                outgoingFragmentOpcode = 0;
            }
            enqueueWithBackpressure(FrameEncoder.fragment(opcode, fragment, last));
        }
    }

    void sendPing(ByteBuffer payload) {
        ByteBuffer data = payload == null ? ByteBuffer.allocate(0) : payload;
        if (data.remaining() > 125) {
            throw new IllegalArgumentException("Ping payload must not exceed 125 bytes");
        }
        enqueueWithBackpressure(FrameEncoder.ping(data));
    }

    void sendPong(ByteBuffer payload) {
        ByteBuffer data = payload == null ? ByteBuffer.allocate(0) : payload;
        if (data.remaining() > 125) {
            throw new IllegalArgumentException("Pong payload must not exceed 125 bytes");
        }
        enqueueWithBackpressure(FrameEncoder.pong(data));
    }

    boolean isOpen() {
        return state == STATE_OPEN && !hardClosed.get();
    }

    // ===================================================================
    // Business virtual thread: event pump
    // ===================================================================

    private void startEventThread() {
        eventThread = Thread.ofVirtual()
                .name("ws-session-" + id)
                .start(this::runEventLoop);
    }

    private void runEventLoop() {
        try {
            handler.onOpen(session);
        } catch (Throwable error) {
            handleUserError(error);
        }
        try {
            while (true) {
                Event event;
                try {
                    event = eventQueue.take();
                } catch (InterruptedException e) {
                    break;
                }
                if (event == ShutdownEvent.INSTANCE) {
                    break;
                }
                dispatch(event);
            }
        } finally {
            fireCloseCallback();
        }
    }

    private void dispatch(Event event) {
        try {
            switch (event) {
                case TextEvent textEvent -> handler.onTextMessage(session, textEvent.text());
                case BinaryEvent binaryEvent -> handler.onBinaryMessage(session, binaryEvent.payload());
                case PingEvent pingEvent -> handler.onPing(session, pingEvent.payload());
                case PongEvent pongEvent -> handler.onPong(session, pongEvent.payload());
                case ErrorEvent errorEvent -> handler.onError(session, errorEvent.error());
                case ShutdownEvent _ -> {
                }
            }
        } catch (Throwable error) {
            handleUserError(error);
        }
    }

    private void handleUserError(Throwable error) {
        dispatchError(error);
        initiateClose(CloseStatus.INTERNAL_ERROR, "handler error");
    }

    private void dispatchError(Throwable error) {
        // Errors also go through the event queue to stay serial with message
        // callbacks; if the queue is full they are dropped as the connection
        // closes.
        eventQueue.offer(new ErrorEvent(error));
    }

    private void fireCloseCallback() {
        if (!closeCallbackFired.compareAndSet(false, true)) {
            return;
        }
        try {
            handler.onClose(session, finalCloseStatus, finalCloseReason);
        } catch (Throwable error) {
            try {
                handler.onError(session, error);
            } catch (Throwable suppressed) {
                // Swallow it if onError also fails, to ensure connection
                // reclamation completes.
            }
        }
    }

    /// Final resource reclamation; may be called from any thread.
    private void finishHardClose() {
        if (selectionKey != null) {
            selectionKey.cancel();
        }
        try {
            channel.close();
        } catch (IOException _) {
            // No need to handle channel close exceptions.
        }
        sessionRegistry.remove(id, session);
        synchronized (writeLock) {
            writeQueue.clear();
            queuedBytes = 0;
            writeLock.notifyAll();
        }
        if (eventThread != null) {
            if (!eventQueue.offer(ShutdownEvent.INSTANCE)) {
                eventQueue.clear();
                eventQueue.offer(ShutdownEvent.INSTANCE);
            }
            eventThread.interrupt();
        }
    }

    // ===================================================================
    // Views read by DefaultWebSocketSession
    // ===================================================================

    HttpRequest request() {
        return request;
    }

    Map<String, String> pathVariables() {
        return pathVariables;
    }

    InetSocketAddress remoteAddress() {
        return remoteAddress;
    }

    // ===================================================================
    // Event definitions
    // ===================================================================

    private sealed interface Event
            permits TextEvent, BinaryEvent, PingEvent, PongEvent, ErrorEvent, ShutdownEvent {
    }

    private record TextEvent(String text) implements Event {
    }

    private record BinaryEvent(ByteBuffer payload) implements Event {
    }

    private record PingEvent(ByteBuffer payload) implements Event {
    }

    private record PongEvent(ByteBuffer payload) implements Event {
    }

    private record ErrorEvent(Throwable error) implements Event {
    }

    private record ShutdownEvent() implements Event {
        static final ShutdownEvent INSTANCE = new ShutdownEvent();
    }
}
