package com.remond.speechcompat;

import jakarta.websocket.RemoteEndpoint;
import jakarta.websocket.Session;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.tomcat.websocket.Constants;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.adapter.NativeWebSocketSession;

/** Orders protocol frames and bounds a client that stops reading. */
final class ClientOutput {
    private static final int MAX_BYTES = 1_048_576;
    private static final int MAX_FRAMES = 32;
    private static final long SEND_TIMEOUT_MS = 3000;
    private static final long CLOSE_TIMEOUT_MS = 500;
    private static final CompletableFuture<Void> EMPTY = CompletableFuture.completedFuture(null);

    static final class Backpressure extends IOException {
        Backpressure() {
            super("Client output unavailable");
        }
    }

    private record Frame(String text, byte[] audio, int bytes, long queuedAt, CompletableFuture<Void> sent) {}

    private final WebSocketSession socket;
    private final RemoteEndpoint.Async remote;
    private final Executor writers;
    private final Executor closers;
    private final Runnable writeFailed;
    private final ArrayDeque<Frame> pending = new ArrayDeque<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private Frame writing;
    private int bytes;
    private boolean scheduled;
    private boolean finishing;
    private boolean closing;

    ClientOutput(WebSocketSession socket, Executor writers, Executor closers, Runnable writeFailed) {
        this.socket = socket;
        this.writers = writers;
        this.closers = closers;
        this.writeFailed = writeFailed;
        Session session = ((NativeWebSocketSession) socket).getNativeSession(Session.class);
        session.getUserProperties().put(Constants.BLOCKING_SEND_TIMEOUT_PROPERTY, CLOSE_TIMEOUT_MS);
        session.getUserProperties().put(Constants.SESSION_CLOSE_TIMEOUT_PROPERTY, 1000L);
        remote = session.getAsyncRemote();
        remote.setSendTimeout(SEND_TIMEOUT_MS);
    }

    void send(String text) throws Backpressure {
        enqueue(text, null, text.length() * 3);
    }

    void send(byte[] audio) throws Backpressure {
        if (audio.length > 0) {
            enqueue(null, audio, audio.length);
        }
    }

    private synchronized void enqueue(String text, byte[] audio, int size) throws Backpressure {
        if (finishing || pending.size() >= MAX_FRAMES || size > MAX_BYTES - bytes) {
            throw new Backpressure();
        }
        pending.addLast(new Frame(text, audio, size, System.nanoTime(), new CompletableFuture<>()));
        bytes += size;
    }

    void flush() {
        write(false);
    }

    synchronized CompletableFuture<Void> flushed() {
        Frame tail = pending.peekLast();
        if (tail == null) {
            tail = writing;
        }
        return tail == null ? EMPTY : tail.sent();
    }

    CompletableFuture<Void> closed() {
        return closed;
    }

    synchronized void finish() {
        finishing = true;
        scheduleWrite();
    }

    void abort() {
        ArrayDeque<Frame> discarded;
        synchronized (this) {
            discarded = new ArrayDeque<>(pending);
            pending.clear();
            if (writing != null) {
                discarded.addFirst(writing);
                writing = null;
            }
            bytes = 0;
            finishing = true;
            scheduleClose();
        }
        discarded.forEach(frame -> frame.sent().completeExceptionally(new Backpressure()));
    }

    private void scheduleWrite() {
        if (scheduled || writing != null || closing) {
            return;
        }
        if (!pending.isEmpty()) {
            scheduled = true;
            writers.execute(() -> write(true));
        } else if (finishing) {
            scheduleClose();
        }
    }

    private void write(boolean scheduledJob) {
        Frame frame;
        synchronized (this) {
            if (scheduledJob) {
                scheduled = false;
            }
            if (scheduled || closing || writing != null || pending.isEmpty()) {
                return;
            }
            frame = pending.removeFirst();
            writing = frame;
        }
        if (System.nanoTime() - frame.queuedAt() >= TimeUnit.MILLISECONDS.toNanos(SEND_TIMEOUT_MS)) {
            sent(frame, false);
            return;
        }
        try {
            if (frame.text() != null) {
                remote.sendText(frame.text(), result -> sent(frame, result.isOK()));
            } else {
                remote.sendBinary(ByteBuffer.wrap(frame.audio()), result -> sent(frame, result.isOK()));
            }
        } catch (RuntimeException failure) {
            sent(frame, false);
        }
    }

    private void sent(Frame frame, boolean success) {
        synchronized (this) {
            if (writing != frame) {
                return;
            }
            writing = null;
            bytes -= frame.bytes();
            if (success) {
                scheduleWrite();
            } else {
                finishing = true;
            }
        }
        if (success) {
            frame.sent().complete(null);
        } else {
            // Inform the session before the close callback can release admission.
            writeFailed.run();
            frame.sent().completeExceptionally(new Backpressure());
        }
    }

    private void scheduleClose() {
        if (!closing) {
            closing = true;
            closers.execute(this::close);
        }
    }

    private void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // The peer may already have disconnected.
        } finally {
            closed.complete(null);
        }
    }
}
