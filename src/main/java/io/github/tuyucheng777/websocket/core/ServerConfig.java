package io.github.tuyucheng777.websocket.core;

/// Server runtime configuration (immutable value object).
///
/// @param host                     the bind address
/// @param port                     the bind port; {@code 0} means assigned by
///                                 the operating system
/// @param backlog                  the TCP accept queue length
/// @param maxFramePayloadSize      maximum payload size in bytes of a single
///                                 WebSocket message
/// @param readBufferSize           per-connection read buffer size in bytes
/// @param maxHandshakeSize         maximum size in bytes of the HTTP
///                                 handshake request headers
/// @param writeBufferHighWaterMark send backlog high water mark; the sender
///                                 virtual thread suspends once exceeded
/// @param writeBufferLowWaterMark  the value the send backlog falls back to
///                                 before the sender is woken up
/// @param eventQueueCapacity       per-connection queue capacity for pending
///                                 business events
public record ServerConfig(
        String host,
        int port,
        int backlog,
        long maxFramePayloadSize,
        int readBufferSize,
        int maxHandshakeSize,
        int writeBufferHighWaterMark,
        int writeBufferLowWaterMark,
        int eventQueueCapacity
) {

    /// Compact constructor: validates all value ranges.
    public ServerConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be empty");
        }
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 0 and 65535: " + port);
        }
        if (backlog <= 0) {
            throw new IllegalArgumentException("backlog must be positive");
        }
        if (maxFramePayloadSize <= 0 || maxFramePayloadSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("maxFramePayloadSize out of allowed range");
        }
        if (readBufferSize <= 0) {
            throw new IllegalArgumentException("readBufferSize must be positive");
        }
        if (maxHandshakeSize <= 0) {
            throw new IllegalArgumentException("maxHandshakeSize must be positive");
        }
        if (writeBufferLowWaterMark <= 0
                || writeBufferHighWaterMark <= writeBufferLowWaterMark) {
            throw new IllegalArgumentException("invalid write water mark values");
        }
        if (eventQueueCapacity <= 0) {
            throw new IllegalArgumentException("eventQueueCapacity must be positive");
        }
    }
}
