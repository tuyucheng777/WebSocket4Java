package io.github.tuyucheng777.websocket.support;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/// Minimal WebSocket client used only by integration tests (blocking I/O, masked sends).
///
/// Depends on no third-party libraries. Capabilities: standard handshake, custom handshake
/// headers (error cases), sending text/binary/fragment/Ping/Pong/close frames, and reading
/// both raw frames and logical messages.
public final class WsTestClient implements AutoCloseable {

    /// A raw frame that was read.
    ///
    /// @param fin     whether this is the final frame
    /// @param opcode  opcode
    /// @param payload payload after the mask has been removed
    public record Frame(boolean fin, int opcode, byte[] payload) {
    }

    private static final SecureRandom RANDOM = new SecureRandom();

    private final Socket socket;
    private final InputStream input;
    private final OutputStream output;

    private WsTestClient(Socket socket) throws IOException {
        this.socket = socket;
        this.input = socket.getInputStream();
        this.output = socket.getOutputStream();
    }

    /// Connects to a local server with a standard 101 handshake.
    ///
    /// @param port server port
    /// @param path path (may include a query string)
    /// @return the connected client
    /// @throws IOException if the handshake fails or on I/O error
    public static WsTestClient connect(int port, String path) throws IOException {
        return connect("127.0.0.1", port, path, Map.of());
    }

    /// Connects to a local server with custom handshake headers.
    ///
    /// @param port          server port
    /// @param path          path
    /// @param customHeaders header overrides; a {@code null} value removes a default header
    /// @return the connected client
    /// @throws IOException if the handshake fails
    public static WsTestClient connect(int port, String path,
                                       Map<String, String> customHeaders) throws IOException {
        return connect("127.0.0.1", port, path, customHeaders);
    }

    /// Connects with custom handshake headers, used for invalid handshake cases.
    ///
    /// @param host          host
    /// @param port          port
    /// @param path          path
    /// @param customHeaders headers to override/append; passing keys such as
    ///                      {@code Sec-WebSocket-Version} can produce an invalid handshake
    /// @return the connected client
    /// @throws IOException if the handshake fails (thrown when the response is not 101)
    public static WsTestClient connect(String host, int port, String path,
                                       Map<String, String> customHeaders) throws IOException {
        Socket socket = new Socket();
        socket.setSoTimeout(5_000);
        socket.connect(new InetSocketAddress(host, port), 5_000);
        Map<String, String> headers = new LinkedHashMap<>(customHeaders);
        writeHandshake(socket.getOutputStream(), host, port, path, headers);
        String response = readHead(socket.getInputStream());
        if (!response.startsWith("HTTP/1.1 101 ")) {
            socket.close();
            throw new IOException("handshake failed, response: " + response.split("\r\n")[0]);
        }
        return new WsTestClient(socket);
    }

    /// Opens a raw TCP connection without sending any data.
    ///
    /// @param port port
    /// @return the TCP socket
    /// @throws IOException if the connection fails
    public static Socket rawConnection(int port) throws IOException {
        Socket socket = new Socket();
        socket.setSoTimeout(5_000);
        socket.connect(new InetSocketAddress("127.0.0.1", port), 5_000);
        return socket;
    }

    /// Writes a complete handshake request to the output stream.
    ///
    /// @param output    output stream
    /// @param host      host
    /// @param port      port
    /// @param path      path
    /// @param overrides header overrides
    /// @throws IOException if the write fails
    public static void writeHandshake(OutputStream output, String host, int port,
                                      String path, Map<String, String> overrides)
            throws IOException {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Upgrade", "websocket");
        headers.put("Connection", "Upgrade");
        headers.put("Sec-WebSocket-Key",
                Base64.getEncoder().encodeToString(randomBytes(16)));
        headers.put("Sec-WebSocket-Version", "13");
        // A null override removes that default header, used to build requests missing headers.
        overrides.forEach((key, value) -> {
            if (value == null) {
                headers.remove(key);
            } else {
                headers.put(key, value);
            }
        });

        StringBuilder builder = new StringBuilder();
        builder.append("GET ").append(path).append(" HTTP/1.1\r\n");
        builder.append("Host: ").append(host).append(':').append(port).append("\r\n");
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            builder.append(entry.getKey()).append(": ").append(entry.getValue()).append("\r\n");
        }
        builder.append("\r\n");
        output.write(builder.toString().getBytes(StandardCharsets.ISO_8859_1));
        output.flush();
    }

    /// Reads HTTP response headers up to {@code CRLF CRLF}.
    ///
    /// @param input input stream
    /// @return the response header text
    /// @throws IOException if the read fails or the stream ends early
    public static String readHead(InputStream input) throws IOException {
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        int matched = 0;
        byte[] terminator = {'\r', '\n', '\r', '\n'};
        int value;
        while (matched < 4 && (value = input.read()) != -1) {
            byte b = (byte) value;
            head.write(b);
            if (b == terminator[matched]) {
                matched++;
            } else {
                matched = (b == '\r') ? 1 : 0;
            }
        }
        if (matched < 4) {
            throw new IOException("incomplete response header");
        }
        return head.toString(StandardCharsets.ISO_8859_1);
    }

    // ---- sending ----

    /// Sends a text message.
    public void sendText(String text) throws IOException {
        sendFrame(true, 0x1, text.getBytes(StandardCharsets.UTF_8));
    }

    /// Sends a binary message.
    public void sendBinary(byte[] data) throws IOException {
        sendFrame(true, 0x2, data);
    }

    /// Sends a fragment.
    public void sendFragment(int opcode, byte[] payload, boolean last) throws IOException {
        sendFrame(last, opcode, payload);
    }

    /// Sends a Ping.
    public void sendPing(byte[] payload) throws IOException {
        sendFrame(true, 0x9, payload);
    }

    /// Sends a close frame with a status code.
    public void sendClose(int statusCode, String reason) throws IOException {
        byte[] reasonBytes = reason.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[2 + reasonBytes.length];
        payload[0] = (byte) ((statusCode >>> 8) & 0xFF);
        payload[1] = (byte) (statusCode & 0xFF);
        System.arraycopy(reasonBytes, 0, payload, 2, reasonBytes.length);
        sendFrame(true, 0x8, payload);
    }

    /// Writes raw bytes directly, used to send hand-crafted illegal frames.
    public void sendRaw(byte[] bytes) throws IOException {
        output.write(bytes);
        output.flush();
    }

    /// Sends a masked frame.
    public void sendFrame(boolean fin, int opcode, byte[] payload) throws IOException {
        output.write(WireFrames.masked(opcode, payload, fin));
        output.flush();
    }

    // ---- receiving ----

    /// Reads one raw frame with a 5-second timeout; returns {@code null} when the peer closes.
    ///
    /// @return the frame; {@code null} at end of stream
    /// @throws IOException if the read fails or the frame is malformed
    public Frame recvFrame() throws IOException {
        int first = input.read();
        if (first == -1) {
            return null;
        }
        int second = input.read();
        if (second == -1) {
            return null;
        }
        boolean fin = (first & 0x80) != 0;
        int opcode = first & 0x0F;
        boolean masked = (second & 0x80) != 0;
        int shortLength = second & 0x7F;
        long length;
        if (shortLength < 126) {
            length = shortLength;
        } else if (shortLength == 126) {
            length = ((long) readByte() << 8) | readByte();
        } else {
            length = 0;
            for (int i = 0; i < 8; i++) {
                length = (length << 8) | readByte();
            }
        }
        byte[] key = null;
        if (masked) {
            key = new byte[4];
            readFully(key);
        }
        byte[] payload = new byte[(int) length];
        readFully(payload);
        if (masked) {
            for (int i = 0; i < payload.length; i++) {
                payload[i] ^= key[i & 3];
            }
        }
        return new Frame(fin, opcode, payload);
    }

    /// Reads and reassembles one complete message (across multiple continuation frames); control frames are excluded.
    ///
    /// @return the message frame (opcode is the start frame type, payload is the assembled result); {@code null} on close
    /// @throws IOException if the read fails
    public Frame recvMessage() throws IOException {
        Frame frame = recvFrame();
        if (frame == null) {
            return null;
        }
        if (frame.opcode() == 0x8) {
            return frame;
        }
        if (frame.fin()) {
            return frame;
        }
        ByteArrayOutputStream assembled = new ByteArrayOutputStream();
        assembled.writeBytes(frame.payload());
        int opcode = frame.opcode();
        while (true) {
            frame = recvFrame();
            if (frame == null) {
                return null;
            }
            // Control frames may be interleaved inside a fragmented message; ignore them and keep waiting for continuation frames.
            if (frame.opcode() >= 0x8) {
                continue;
            }
            assembled.writeBytes(frame.payload());
            if (frame.fin()) {
                break;
            }
        }
        return new Frame(true, opcode, assembled.toByteArray());
    }

    /// Reads the close frame status code and reason.
    ///
    /// @return the status code; {@code 1005} for an empty close frame
    /// @throws IOException if the frame read is not a close frame
    public int recvCloseStatus() throws IOException {
        Frame frame = recvFrame();
        if (frame == null) {
            return 1006;
        }
        if (frame.opcode() != 0x8) {
            throw new IOException("expected close frame, got opcode=0x" + Integer.toHexString(frame.opcode()));
        }
        if (frame.payload().length < 2) {
            return 1005;
        }
        return ((frame.payload()[0] & 0xFF) << 8) | (frame.payload()[1] & 0xFF);
    }

    /// Closes the underlying TCP immediately without sending a close frame.
    public void disconnect() throws IOException {
        socket.close();
    }

    @Override
    public void close() throws IOException {
        try {
            sendClose(1000, "");
            try {
                socket.setSoTimeout(1_000);
                recvFrame();
            } catch (IOException ignored) {
                // Best-effort read of the peer's close frame; it does not matter if this fails.
            }
        } catch (IOException ignored) {
            // The socket may already be disconnected.
        } finally {
            socket.close();
        }
    }

    private int readByte() throws IOException {
        int value = input.read();
        if (value == -1) {
            throw new IOException("connection ended prematurely while reading frame");
        }
        return value;
    }

    private void readFully(byte[] buffer) throws IOException {
        int offset = 0;
        while (offset < buffer.length) {
            int read = input.read(buffer, offset, buffer.length - offset);
            if (read == -1) {
                throw new IOException("connection ended prematurely while reading frame payload");
            }
            offset += read;
        }
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        RANDOM.nextBytes(bytes);
        return bytes;
    }
}
