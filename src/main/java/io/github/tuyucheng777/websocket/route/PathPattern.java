package io.github.tuyucheng777.websocket.route;

import java.util.LinkedHashMap;
import java.util.Map;

/// WebSocket path matching pattern.
///
/// Two forms are supported:
///
/// - exact match: {@code /echo}, {@code /chat/room}
/// - path variable: {@code /chat/{room}}, where {@code {room}} matches a single non-empty path
///   segment; after a match the variable value can be read within the session.
///
/// Patterns are matched segment by segment ({@code /}); cross-segment wildcards are not supported.
public final class PathPattern {

    private final String raw;
    private final String[] segments;
    private final String[] variableNames;

    private PathPattern(String raw, String[] segments, String[] variableNames) {
        this.raw = raw;
        this.segments = segments;
        this.variableNames = variableNames;
    }

    /// Compiles a path pattern.
    ///
    /// @param pattern the path pattern, which must start with {@code /}
    /// @return the compiled immutable pattern
    /// @throws IllegalArgumentException if the pattern syntax is invalid
    public static PathPattern compile(String pattern) {
        if (pattern == null || pattern.isEmpty() || pattern.charAt(0) != '/') {
            throw new IllegalArgumentException("path must start with /: " + pattern);
        }
        String normalized = pattern.length() > 1 && pattern.endsWith("/")
                ? pattern.substring(0, pattern.length() - 1)
                : pattern;
        String[] parts = normalized.substring(1).split("/", -1);
        String[] names = new String[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            if (part.isEmpty()) {
                throw new IllegalArgumentException("path contains an empty segment: " + pattern);
            }
            if (part.startsWith("{") && part.endsWith("}")) {
                String name = part.substring(1, part.length() - 1);
                if (name.isEmpty() || name.contains("{") || name.contains("}")) {
                    throw new IllegalArgumentException("invalid path variable: " + part);
                }
                names[i] = name;
            } else if (part.contains("{") || part.contains("}")) {
                throw new IllegalArgumentException("path variable must occupy the entire path segment: " + part);
            }
        }
        return new PathPattern(normalized, parts, names);
    }

    /// Attempts to match a concrete path.
    ///
    /// @param path the request path without the query string
    /// @return on a match, the path variable map (an empty map when there are no variables); {@code null} if no match
    public Map<String, String> match(String path) {
        if (path == null) {
            return null;
        }
        String normalized = path.length() > 1 && path.endsWith("/")
                ? path.substring(0, path.length() - 1)
                : path;
        String[] parts = normalized.substring(1).split("/", -1);
        if (parts.length != segments.length) {
            return null;
        }
        Map<String, String> variables = new LinkedHashMap<>();
        for (int i = 0; i < parts.length; i++) {
            String actual = parts[i];
            if (actual.isEmpty()) {
                return null;
            }
            if (variableNames[i] != null) {
                variables.put(variableNames[i], actual);
            } else if (!segments[i].equals(actual)) {
                return null;
            }
        }
        return variables;
    }

    /// Returns the raw pattern text.
    ///
    /// @return the pattern text
    public String raw() {
        return raw;
    }

    @Override
    public String toString() {
        return raw;
    }
}
