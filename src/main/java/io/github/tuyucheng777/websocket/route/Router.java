package io.github.tuyucheng777.websocket.route;

import java.util.List;
import java.util.Map;

/// Route table: holds all registered routes and performs matching.
///
/// Matching prefers exact paths first, then path variable patterns in registration order,
/// preventing patterns such as {@code /rooms/{id}} from matching before an exact path.
public final class Router {

    private final List<Route> routes;

    /// Constructs a route table.
    ///
    /// @param routes registered routes (in registration order)
    public Router(List<Route> routes) {
        this.routes = List.copyOf(routes);
    }

    /// Matches a route by request path.
    ///
    /// @param path the request path without the query string
    /// @return the match result, or {@code null} if no route matched
    public Route.Match route(String path) {
        // First pass: exact paths take priority.
        for (Route route : routes) {
            Map<String, String> variables = route.pattern().match(path);
            if (variables != null && variables.isEmpty()) {
                return new Route.Match(route.handler(), Map.of());
            }
        }
        // Second pass: path variable patterns, in registration order.
        for (Route route : routes) {
            Map<String, String> variables = route.pattern().match(path);
            if (variables != null) {
                return new Route.Match(route.handler(), Map.copyOf(variables));
            }
        }
        return null;
    }
}
