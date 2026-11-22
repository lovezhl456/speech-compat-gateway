package com.remond.speechcompat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

final class OpenAi {
    private static final String TRANSCRIPTION = "?intent=transcription";
    private static final String SYNTHESIS = "?model=gpt-realtime-2.1-mini";
    private static final int IDLE_TTS_SECONDS = 30;
    private static final long MAX_TTS_AGE_NANOS = TimeUnit.MINUTES.toNanos(50);

    private record Receiver(Function<JsonNode, CompletionStage<Void>> events, Runnable connected,
            Consumer<String> failure) {}

    private final ObjectMapper json;
    private final HttpClient http;
    private final Executor commands;
    private final ScheduledExecutorService timers;
    private final int idleLimit;
    private final ArrayDeque<Stream> idleTts = new ArrayDeque<>();
    private boolean stopping;
    private final String key = Auth.setting("OPENAI_API_KEY");
    private final String endpoint = System.getenv().getOrDefault("COMPAT_REALTIME_URL",
            "wss://api.openai.com/v1/realtime");

    OpenAi(ObjectMapper json, Executor commands, ScheduledExecutorService timers, int idleLimit) {
        this.json = json;
        this.commands = commands;
        this.timers = timers;
        this.idleLimit = idleLimit;
        var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(4));
        String proxy = System.getenv("OPENAI_LIVE_HTTP_PROXY");
        if (proxy != null && !proxy.isBlank()) {
            URI uri = URI.create(proxy);
            if (!"http".equals(uri.getScheme()) || uri.getHost() == null || uri.getPort() < 1
                    || uri.getRawUserInfo() != null) {
                throw new IllegalArgumentException("Invalid OpenAI proxy configuration");
            }
            client.proxy(ProxySelector.of(new InetSocketAddress(uri.getHost(), uri.getPort())));
        }
        URI uri = URI.create(endpoint);
        boolean fixture = "ws".equals(uri.getScheme()) && "127.0.0.1".equals(uri.getHost());
        if (!fixture && !"wss://api.openai.com/v1/realtime".equals(endpoint)) {
            throw new IllegalArgumentException("Invalid OpenAI endpoint");
        }
        http = client.build();
    }

    synchronized Stream stream(boolean asr, Function<JsonNode, CompletionStage<Void>> events, Runnable connected,
            Consumer<String> failure) {
        var receiver = new Receiver(events, connected, failure);
        if (!asr) {
            while (!idleTts.isEmpty()) {
                Stream stream = idleTts.removeLast();
                stream.idleExpiry.cancel(false);
                stream.idleExpiry = null;
                if (System.nanoTime() - stream.idleSince < TimeUnit.SECONDS.toNanos(IDLE_TTS_SECONDS)
                        && stream.borrow(receiver)) {
                    return stream;
                }
                stream.stop(null);
            }
        }
        return new Stream(asr ? TRANSCRIPTION : SYNTHESIS, receiver);
    }

    CompletableFuture<Void> finishTts(Stream stream, CompletableFuture<Boolean> delivered,
            CompletableFuture<Void> clientClosed) {
        return delivered.thenCombine(clientClosed, (sent, ignored) -> sent)
                .thenCompose(sent -> sent ? recycle(stream) : stream.stop(null));
    }

    private synchronized CompletableFuture<Void> recycle(Stream stream) {
        if (stopping || idleTts.size() >= idleLimit || !stream.park()) {
            return stream.stop(null);
        }
        long since = System.nanoTime();
        stream.idleSince = since;
        stream.idleExpiry = timers.schedule(() -> evict(stream, since), IDLE_TTS_SECONDS, TimeUnit.SECONDS);
        idleTts.addLast(stream);
        return CompletableFuture.completedFuture(null);
    }

    synchronized int idleTtsCount() {
        return idleTts.size();
    }

    private synchronized void evict(Stream stream, long idleSince) {
        if (stream.idleSince == idleSince && idleTts.remove(stream)) {
            closeParked(stream);
        }
    }

    private synchronized void evictFailed(Stream stream) {
        if (idleTts.remove(stream)) {
            closeParked(stream);
        }
    }

    private void closeParked(Stream stream) {
        stream.idleExpiry.cancel(false);
        stream.idleExpiry = null;
        stream.stop(null);
    }

    synchronized CompletableFuture<Void> closeIdle() {
        stopping = true;
        var closes = idleTts.stream().map(stream -> {
            stream.idleExpiry.cancel(false);
            stream.idleExpiry = null;
            return stream.stop(null);
        }).toArray(CompletableFuture[]::new);
        idleTts.clear();
        return CompletableFuture.allOf(closes);
    }

    final class Stream implements WebSocket.Listener {
        private static final int MAX_SENDS = 24;
        private static final int MAX_SEND_CHARS = 524_288;
        private static final long SEND_TIMEOUT_MS = 2000;
        private final String query;
        private volatile Receiver receiver;
        private boolean failed;
        private StringBuilder input = new StringBuilder();
        private CompletableFuture<WebSocket> opening;
        private CompletableFuture<WebSocket> outbound;
        private CompletableFuture<WebSocket> writing;
        private WebSocket socket;
        private long openedAt;
        private int queued;
        private int queuedChars;
        private volatile boolean stopped;
        // Idle entries and their expiry tasks are owned by the enclosing pool lock.
        private ScheduledFuture<?> idleExpiry;
        private volatile long idleSince;

        Stream(String query, Receiver receiver) {
            this.query = query;
            this.receiver = receiver;
        }

        synchronized boolean open(JsonNode config) {
            if (opening != null) {
                return true;
            }
            openedAt = System.nanoTime();
            opening = http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(4))
                    .header("Authorization", "Bearer " + key)
                    .buildAsync(URI.create(endpoint + query), this);
            outbound = opening.thenComposeAsync(ws -> write(ws, config.toString()), commands);
            outbound.whenComplete((ws, error) -> {
                if (error != null) {
                    failed("model_setup_failed");
                }
            });
            return false;
        }

        private synchronized boolean borrow(Receiver next) {
            if (receiver != null || !usable()) {
                return false;
            }
            receiver = next;
            return true;
        }

        private synchronized boolean park() {
            if (!usable()) {
                return false;
            }
            receiver = null;
            return true;
        }

        private boolean usable() {
            return System.nanoTime() - openedAt < MAX_TTS_AGE_NANOS && !failed && !stopped
                    && socket != null && !socket.isInputClosed() && !socket.isOutputClosed()
                    && outbound.isDone() && !outbound.isCompletedExceptionally();
        }

        synchronized void send(JsonNode event) {
            String text = event.toString();
            if (stopped || outbound == null || queued >= MAX_SENDS || text.length() > MAX_SEND_CHARS - queuedChars) {
                throw new Backpressure();
            }
            queued++;
            queuedChars += text.length();
            // JDK WebSocket sends only submit work. Keep the normal streaming path on that queue.
            outbound = outbound.thenCompose(ws -> write(ws, text))
                    .orTimeout(SEND_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            outbound.whenComplete((ws, error) -> {
                synchronized (this) {
                    queued--;
                    queuedChars -= text.length();
                }
                if (error != null) {
                    failed("model_write_failed");
                }
            });
        }

        private synchronized CompletableFuture<WebSocket> write(WebSocket ws, String text) {
            if (stopped) {
                return CompletableFuture.failedFuture(new IllegalStateException("Upstream closed"));
            }
            writing = ws.sendText(text, true);
            return writing;
        }

        CompletableFuture<Void> stop(String cancel) {
            WebSocket closing;
            CompletableFuture<WebSocket> lastWrite;
            synchronized (this) {
                stopped = true;
                receiver = null;
                closing = socket;
                lastWrite = writing;
            }
            return CompletableFuture.runAsync(() -> {
                if (closing == null && opening != null) {
                    opening.cancel(true);
                }
            }, commands).thenCompose(ignored -> {
                if (closing == null) {
                    return CompletableFuture.completedFuture(null);
                }
                if (cancel == null) {
                    closing.abort();
                    return CompletableFuture.completedFuture(null);
                }
                var ready = lastWrite == null ? CompletableFuture.completedFuture(closing) : lastWrite;
                return ready.thenCompose(ws -> ws.sendText(cancel, true)).orTimeout(300, TimeUnit.MILLISECONDS)
                        .handle((ws, error) -> {
                            closing.abort();
                            return null;
                        });
            });
        }

        private void failed(String reason) {
            failed(reason, null);
        }

        private void failed(String reason, Receiver owner) {
            Receiver target;
            synchronized (this) {
                if (stopped || failed || (owner != null && owner != receiver)) {
                    return;
                }
                failed = true;
                target = receiver;
            }
            commands.execute(() -> {
                if (target == null) {
                    evictFailed(this);
                } else if (!stopped) {
                    target.failure().accept(reason);
                }
            });
        }

        @Override
        public void onOpen(WebSocket ws) {
            synchronized (this) {
                if (!stopped) {
                    socket = ws;
                }
            }
            Receiver target = receiver;
            if (stopped || target == null) {
                ws.abort();
            } else {
                target.connected().run();
                ws.request(1);
            }
        }

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            try {
                if (input.length() + data.length() > 2_000_000) {
                    throw new IllegalArgumentException("Oversize model message");
                }
                input.append(data);
                if (last) {
                    JsonNode event = json.readTree(input.toString());
                    if (input.capacity() > 65_536) {
                        input = new StringBuilder();
                    } else {
                        input.setLength(0);
                    }
                    Receiver target = receiver;
                    if (target == null) {
                        if ("error".equals(event.path("type").asText())) {
                            failed("model_idle_error");
                        } else if (!stopped) {
                            ws.request(1);
                        }
                        return null;
                    }
                    target.events().apply(event).whenComplete((ignored, error) -> {
                        if (error != null) {
                            failed("client_write_failed", target);
                        } else if (!stopped) {
                            ws.request(1);
                        }
                    });
                } else if (!stopped) {
                    ws.request(1);
                }
            } catch (Exception error) {
                failed("model_protocol_error");
            }
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int status, String reason) {
            failed("model_disconnected");
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            failed("model_disconnected");
        }
    }

    static final class Backpressure extends IllegalStateException {
        Backpressure() {
            super("Upstream send unavailable");
        }
    }
}
