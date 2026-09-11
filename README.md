<p align="center">
  <img src="logo.jpg" alt="WebSocket4Java Logo" width="180">
</p>

<h1 align="center">WebSocket4Java</h1>

<p align="center">
  <a href="https://central.sonatype.com/artifact/io.github.tuyucheng777/WebSocket4Java"><img src="https://img.shields.io/maven-central/v/io.github.tuyucheng777/WebSocket4Java?logo=apachemaven&color=blue" alt="Maven Central"></a>
  <a href="https://github.com/tuyucheng777/WebSocket4Java/actions/workflows/ci.yml"><img src="https://github.com/tuyucheng777/WebSocket4Java/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://sonarcloud.io/summary/new_code?id=tuyucheng777_WebSocket4Java"><img src="https://sonarcloud.io/api/project_badges/measure?project=tuyucheng777_WebSocket4Java&metric=coverage" alt="Coverage"></a>
  <img src="https://img.shields.io/badge/Java-26%2B-ED8B00?logo=openjdk&logoColor=white" alt="Java 26+">
  <a href="https://opensource.org/license/mit"><img src="https://img.shields.io/badge/License-MIT-green" alt="License: MIT"></a>
</p>

<p align="center">A high-performance WebSocket server framework built on JDK NIO and virtual threads, with a complete RFC 6455 implementation. The runtime code has zero third-party dependencies and uses only the JDK standard library. Java 26 or later is required.</p>

---

## Features

- Single-reactor threading model: one daemon thread handles accept, read, and write events for every connection, using non-blocking channels and gathering writes with no thread-switching or lock-contention overhead
- One virtual thread per connection: business callbacks run serially, and blocking logic (database access, blocking queues, etc.) can be written directly inside callbacks without stalling I/O for other connections
- Per-connection send queue with high/low water-mark backpressure: when a peer reads too slowly, the sender is suspended on its virtual thread and automatically woken up once the queued bytes drain
- Full RFC 6455 support: text/binary messages, message fragmentation (control frames may be interleaved), Ping/Pong heartbeats, close handshake, strict UTF-8 validation
- Protocol safety checks: mandatory client masking, RSV bit validation, control-frame payload limited to 125 bytes, message size limit (oversized messages are closed with 1009)
- HTTP upgrade handshake: SHA-1 + GUID verification (against the official RFC 6455 test vectors), request header size limit, malformed requests rejected with 400/404/426
- Path routing: exact paths (`/echo`) and path variables (`/chat/{room}`), with exact paths taking precedence; query parameters are accessible
- Session management: per-session attribute map, immutable snapshot of live sessions, server-wide text/binary broadcast
- Zero runtime dependencies: production code uses only `java.nio`, `java.lang` (virtual threads), `java.security` (SHA-1), and `java.util` (Base64)

## Requirements

- JDK 26 or later

## Installation

Maven:

```xml
<dependency>
    <groupId>io.github.tuyucheng777</groupId>
    <artifactId>WebSocket4Java</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Quick Start

Create and start a server through the builder:

```java
import io.github.tuyucheng777.websocket.WebSocketHandler;
import io.github.tuyucheng777.websocket.WebSocketServer;
import io.github.tuyucheng777.websocket.WebSocketSession;

void main() throws Exception {
    WebSocketServer server = WebSocketServer.builder()
            .host("0.0.0.0")
            .port(8080)
            .path("/echo", new WebSocketHandler() {
                @Override
                public void onTextMessage(WebSocketSession session, String message) {
                    session.sendText(message);
                }

                @Override
                public void onBinaryMessage(WebSocketSession session,
                                            java.nio.ByteBuffer payload) {
                    session.sendBinary(payload);
                }
            })
            .build();

    server.start();
    Runtime.getRuntime().addShutdownHook(new Thread(server::close));
}
```

Every callback in `WebSocketHandler` has a default empty implementation, so you only override the ones you need. Callbacks run on the virtual thread dedicated to that connection and are triggered strictly in order for the same connection: `onOpen` exactly once, zero or more message/heartbeat callbacks, then `onClose` exactly once.

## Path Variables and Query Parameters

```java
WebSocketServer server = WebSocketServer.builder()
        .port(8080)
        .path("/chat/{room}", new WebSocketHandler() {
            @Override
            public void onOpen(WebSocketSession session) {
                String room = session.pathVariable("room");
                String nickname = session.queryParameter("name");
                session.attributes().put("room", room);
            }

            @Override
            public void onTextMessage(WebSocketSession session, String message) {
                String room = (String) session.attributes().get("room");
                // Broadcast or dispatch by room...
            }
        })
        .build();
```

When a client connects to `ws://host:8080/chat/42?name=alice`, `room` is resolved to `42`.

## Broadcast

```java
server.broadcastText("server will restart in one minute");
server.broadcastBinary(java.nio.ByteBuffer.wrap(payload));
```

A send failure for a single session (for example, if it is already closed) is skipped automatically without affecting the other sessions. Use `server.sessions()` to obtain an immutable snapshot of live sessions and `server.connectionCount()` to obtain the current connection count.

## Heartbeat and Close

When a client Ping frame is received, the framework automatically sends back the matching Pong before invoking the callback. The server can also send heartbeats proactively:

```java
session.sendPing(null);
```

The close handshake is idempotent; repeated calls are ignored:

```java
session.close();                                          // 1000 normal closure
session.close(CloseStatus.GOING_AWAY, "server shutdown"); // explicit status code and reason
```

Calling `server.close()` stops accepting new connections, sends a 1001 close frame to every session, waits for the data to be flushed, and then shuts the reactor down. The class implements `AutoCloseable` and can be used with try-with-resources.

## Configuration

All configuration is done through `WebSocketServer.builder()`, with the following defaults:

| Method | Default | Description |
| --- | --- | --- |
| `host(String)` | `0.0.0.0` | Bind address |
| `port(int)` | `0` | Listen port; `0` lets the operating system assign one, retrievable after start via `getBoundPort()` |
| `backlog(int)` | `1024` | TCP accept queue length |
| `maxFramePayloadSize(long)` | `1048576` (1 MB) | Maximum payload of a single message; oversized messages are closed with 1009 |
| `readBufferSize(int)` | `16384` (16 KB) | Read buffer size per connection |
| `maxHandshakeSize(int)` | `65536` (64 KB) | Maximum HTTP handshake request header size |
| `writeBufferWaterMark(int, int)` | high 256 KB, low 64 KB | Send backpressure water marks; parameter order is high then low |
| `eventQueueCapacity(int)` | `1024` | Per-connection business event queue capacity; overflow closes the connection with 1011 |
| `path(String, WebSocketHandler)` | none | Registers a route; may be called multiple times |

The underlying sockets enable `TCP_NODELAY` and `SO_REUSEADDR` by default.

## Project Structure

```
src/main/java/io/github/tuyucheng777/websocket/
├── WebSocketServer.java        Entry point: builder configuration, start/stop, broadcast, session registry
├── WebSocketHandler.java       Business callback interface (all methods have default empty implementations)
├── WebSocketSession.java       Session operations interface
├── WebSocketException.java     Framework runtime exception
├── core/                       Reactor, connection state machine, session implementation, server config
├── frame/                      RFC 6455 frame encode/decode, UTF-8 validation, message reassembly
├── http/                       HTTP upgrade handshake parsing and validation
└── route/                      Path patterns and route matching
```

Threading model: a single daemon thread named `websocket-nio` performs all I/O; each connection owns a virtual thread named `ws-session-{id}` that runs business callbacks serially; events flow between the reactor and virtual threads through bounded event queues, while outbound writes are driven through the per-connection queue and `Selector.wakeup()`.