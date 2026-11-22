package com.remond.speechcompat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.WebSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class OpenAiTtsReuseTest {
    private final ScheduledExecutorService timers = Executors.newSingleThreadScheduledExecutor();
    private final OpenAi pool = new OpenAi(new ObjectMapper(), Runnable::run, timers, 1);

    @AfterEach
    void close() {
        pool.closeIdle().join();
        timers.shutdownNow();
    }

    @Test
    void failedTerminalWriteCannotBeReusedInEitherCompletionOrder() {
        for (boolean closeFirst : List.of(true, false)) {
            var staleEvent = new CompletableFuture<Void>();
            var first = pool.stream(false, event -> staleEvent, () -> {}, ignored -> {});
            WebSocket socket = connect(first);
            first.onText(socket, "{\"type\":\"response.done\"}", true);
            var terminalWrite = new CompletableFuture<Boolean>();
            var downstreamClosed = new CompletableFuture<Void>();
            var finished = pool.finishTts(first, terminalWrite, downstreamClosed);

            if (closeFirst) {
                downstreamClosed.complete(null);
            } else {
                terminalWrite.complete(false);
            }
            assertFalse(finished.isDone());
            assertEquals(0, pool.idleTtsCount());
            var secondFailures = new ArrayList<String>();
            var second = pool.stream(false, event -> CompletableFuture.completedFuture(null),
                    () -> {}, secondFailures::add);
            assertNotSame(first, second);

            terminalWrite.complete(false);
            downstreamClosed.complete(null);
            finished.join();
            staleEvent.completeExceptionally(new IllegalStateException("downstream write failed"));
            assertTrue(secondFailures.isEmpty());
            assertEquals(0, pool.idleTtsCount());
            verify(socket).abort();
        }
    }

    @Test
    void successfulWriteWaitsForCloseAndLateClientFailureCannotCrossLeases() {
        var staleEvent = new CompletableFuture<Void>();
        var first = pool.stream(false, event -> staleEvent, () -> {}, ignored -> {});
        WebSocket socket = connect(first);
        first.onText(socket, "{\"type\":\"response.done\"}", true);
        var downstreamClosed = new CompletableFuture<Void>();
        var finished = pool.finishTts(first, CompletableFuture.completedFuture(true), downstreamClosed);
        assertFalse(finished.isDone());
        downstreamClosed.complete(null);
        finished.join();
        var failures = new ArrayList<String>();
        var second = pool.stream(false, event -> CompletableFuture.completedFuture(null), () -> {}, failures::add);
        assertSame(first, second);

        staleEvent.completeExceptionally(new IllegalStateException("old client write failed"));
        assertTrue(failures.isEmpty());
        second.onError(socket, new IllegalStateException("current model transport failed"));
        assertEquals(List.of("model_disconnected"), failures);
    }

    private WebSocket connect(OpenAi.Stream stream) {
        WebSocket socket = mock(WebSocket.class);
        stream.onOpen(socket);
        ReflectionTestUtils.setField(stream, "openedAt", System.nanoTime());
        ReflectionTestUtils.setField(stream, "outbound", CompletableFuture.completedFuture(socket));
        return socket;
    }
}
