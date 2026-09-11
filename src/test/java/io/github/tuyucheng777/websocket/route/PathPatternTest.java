package io.github.tuyucheng777.websocket.route;

import io.github.tuyucheng777.websocket.WebSocketHandler;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Unit tests for {@link PathPattern} and {@link Router}.
class PathPatternTest {

    @Test
    void matchesExactPath() {
        PathPattern pattern = PathPattern.compile("/echo");
        assertEquals(Map.of(), pattern.match("/echo"));
    }

    @Test
    void treatsTrailingSlashLeniently() {
        PathPattern pattern = PathPattern.compile("/echo");
        assertEquals(Map.of(), pattern.match("/echo/"));
    }

    @Test
    void matchesSinglePathVariable() {
        PathPattern pattern = PathPattern.compile("/chat/{room}");
        assertEquals(Map.of("room", "java"), pattern.match("/chat/java"));
    }

    @Test
    void matchesMultiplePathVariables() {
        PathPattern pattern = PathPattern.compile("/users/{userId}/posts/{postId}");
        Map<String, String> variables = pattern.match("/users/42/posts/7");
        assertEquals("42", variables.get("userId"));
        assertEquals("7", variables.get("postId"));
    }

    @Test
    void doesNotMatchDifferentSegmentCount() {
        assertNull(PathPattern.compile("/chat/{room}").match("/chat"));
        assertNull(PathPattern.compile("/chat/{room}").match("/chat/java/extra"));
    }

    @Test
    void doesNotMatchLiteralMismatch() {
        assertNull(PathPattern.compile("/chat/{room}").match("/room/java"));
    }

    @Test
    void doesNotMatchEmptySegment() {
        assertNull(PathPattern.compile("/chat/{room}").match("/chat/"));
    }

    @Test
    void rejectsInvalidPatterns() {
        assertThrows(IllegalArgumentException.class, () -> PathPattern.compile("echo"));
        assertThrows(IllegalArgumentException.class, () -> PathPattern.compile("/chat//room"));
        assertThrows(IllegalArgumentException.class, () -> PathPattern.compile("/chat/{ro}om"));
        assertThrows(IllegalArgumentException.class, () -> PathPattern.compile("/chat/{}"));
    }

    private static WebSocketHandler handler() {
        return new WebSocketHandler() { };
    }

    @Test
    void routerPrefersExactPathOverVariablePattern() {
        WebSocketHandler exact = handler();
        WebSocketHandler variable = handler();
        Router router = new Router(java.util.List.of(
                new Route(PathPattern.compile("/rooms/{id}"), variable),
                new Route(PathPattern.compile("/rooms/special"), exact)));

        Route.Match match = router.route("/rooms/special");
        assertSame(match.handler(), exact);
        assertEquals(Map.of(), match.pathVariables());

        Route.Match variableMatch = router.route("/rooms/42");
        assertSame(variableMatch.handler(), variable);
        assertEquals("42", variableMatch.pathVariables().get("id"));
    }

    @Test
    void routerReturnsNullForUnknownPath() {
        Router router = new Router(java.util.List.of(
                new Route(PathPattern.compile("/echo"), handler())));
        assertNull(router.route("/missing"));
    }
}
