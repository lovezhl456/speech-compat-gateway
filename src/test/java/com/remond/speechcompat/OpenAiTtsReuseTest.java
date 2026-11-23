package com.remond.speechcompat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import java.net.http.WebSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class OpenAiTtsReuseTest {
    private final ScheduledExecutorService timers = Executors.newSingleThreadScheduledExecutor();
    private final OpenAi pool = new OpenAi(new ObjectMapper(), Runnable::run, timers, 1, 5000);

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

    @Test
    void idleErrorMarksTheStreamUnborrowableBeforeThePoolEvictsIt() {
        var pausedEviction = new AtomicReference<Runnable>();
        var controlled = new OpenAi(new ObjectMapper(), command -> {
            if (!pausedEviction.compareAndSet(null, command)) {
                command.run();
            }
        }, timers, 1, 5000);
        var first = controlled.stream(false, event -> CompletableFuture.completedFuture(null), () -> {}, ignored -> {});
        WebSocket socket = connect(first);
        park(controlled, first);
        first.onText(socket, "{\"type\":\"error\"}", true);
        assertEquals(1, controlled.idleTtsCount());

        var failures = new ArrayList<String>();
        var next = controlled.stream(false, event -> CompletableFuture.completedFuture(null), () -> {}, failures::add);
        assertNotSame(first, next);
        pausedEviction.get().run();
        verify(socket).abort();
        assertEquals(0, controlled.idleTtsCount());
        assertTrue(failures.isEmpty());
    }

    @Test
    void errorIsDeliveredToTheReceiverIfTheBorrowWinsDuringClassification() throws Exception {
        var readingType = new CountDownLatch(1);
        var resume = new CountDownLatch(1);
        String message = "{\"type\":\"error\",\"error\":{\"event_id\":\"A\"}}";
        var parser = mock(ObjectMapper.class);
        var event = mock(JsonNode.class);
        when(parser.readTree(message)).thenReturn(event);
        when(event.path("type")).thenAnswer(ignored -> {
            readingType.countDown();
            assertTrue(resume.await(2, TimeUnit.SECONDS));
            return TextNode.valueOf("error");
        });
        var controlled = new OpenAi(parser, Runnable::run, timers, 1, 5000);
        var first = controlled.stream(false, item -> CompletableFuture.completedFuture(null), () -> {}, ignored -> {});
        WebSocket socket = connect(first);
        park(controlled, first);

        var received = new ArrayList<JsonNode>();
        var failed = new ArrayList<String>();
        var handling = CompletableFuture.runAsync(() -> first.onText(socket, message, true));
        try {
            assertTrue(readingType.await(2, TimeUnit.SECONDS));
            var next = controlled.stream(false, item -> {
                received.add(item);
                return CompletableFuture.completedFuture(null);
            }, () -> {}, failed::add);
            assertSame(first, next);
        } finally {
            resume.countDown();
        }
        handling.get(2, TimeUnit.SECONDS);
        assertEquals(List.of(event), received);
        assertTrue(failed.isEmpty());
        first.onError(socket, new IllegalStateException("current connection failed"));
        assertEquals(List.of("model_disconnected"), failed);
    }

    private static void park(OpenAi owner, OpenAi.Stream stream) {
        owner.finishTts(stream, CompletableFuture.completedFuture(true), CompletableFuture.completedFuture(null)).join();
    }

    private WebSocket connect(OpenAi.Stream stream) {
        WebSocket socket = mock(WebSocket.class);
        stream.onOpen(socket);
        ReflectionTestUtils.setField(stream, "openedAt", System.nanoTime());
        ReflectionTestUtils.setField(stream, "outbound", CompletableFuture.completedFuture(socket));
        return socket;
    }
}
