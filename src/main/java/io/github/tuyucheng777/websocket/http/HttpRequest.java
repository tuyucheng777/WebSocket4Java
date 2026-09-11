package io.github.tuyucheng777.websocket.http;

import java.util.List;
import java.util.Map;

/// A view of a parsed HTTP upgrade request.
///
/// The request line and headers are parsed once during the handshake and held in immutable form;
/// subsequent route matching and session reads all reuse this object.
public final class HttpRequest {

    private final String method;
    private final String target;
    private final String path;
    private final Map<String, String> queryParameters;
    private final Map<String, List<String>> headers;

    /// Constructs a request view.
    ///
    /// @param method          HTTP method
    /// @param target          raw request target (including the query string)
    /// @param path            path with the query string removed
    /// @param queryParameters query parameters (first-value view)
    /// @param headers         raw headers (case-insensitive)
    public HttpRequest(String method, String target, String path,
                       Map<String, String> queryParameters,
                       Map<String, List<String>> headers) {
        this.method = method;
        this.target = target;
        this.path = path;
        this.queryParameters = queryParameters;
        this.headers = headers;
    }

    /// Returns the HTTP method.
    ///
    /// @return the method, e.g. {@code GET}
    public String method() {
        return method;
    }

    /// Returns the raw request target.
    ///
    /// @return the request target, e.g. {@code /chat/room?u=1}
    public String target() {
        return target;
    }

    /// Returns the path without the query string.
    ///
    /// @return the path, e.g. {@code /chat/room}
    public String path() {
        return path;
    }

    /// Returns the query parameters (first-value view, immutable).
    ///
    /// @return the query parameter map
    public Map<String, String> queryParameters() {
        return queryParameters;
    }

    /// Reads the first value of a query parameter by name.
    ///
    /// @param name parameter name
    /// @return the parameter value, or {@code null} if absent
    public String query(String name) {
        return queryParameters.get(name);
    }

    /// Returns all raw headers (case-insensitive, immutable).
    ///
    /// @return the header map
    public Map<String, List<String>> headers() {
        return headers;
    }

    /// Reads the first value of a header by name; the name is case-insensitive.
    ///
    /// @param name header name
    /// @return the header value, or {@code null} if absent
    public String header(String name) {
        List<String> values = headers.get(name);
        return values == null || values.isEmpty() ? null : values.get(0);
    }
}
