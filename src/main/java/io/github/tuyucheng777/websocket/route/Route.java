package io.github.tuyucheng777.websocket.route;

import io.github.tuyucheng777.websocket.WebSocketHandler;

import java.util.Map;

/// A registered route: the binding between a path pattern and its handler.
///
/// @param pattern   path pattern
/// @param handler   WebSocket event handler
public record Route(PathPattern pattern, WebSocketHandler handler) {

    /// The result of a successful route match.
    ///
    /// @param handler        the matched handler
    /// @param pathVariables  the resolved path variables
    public record Match(WebSocketHandler handler, Map<String, String> pathVariables) {
    }
}
