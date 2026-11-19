package com.remond.speechcompat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

final class OpenAi {
    private final ObjectMapper json;
    private final HttpClient http;
    private final String key = Auth.setting("OPENAI_API_KEY");
    private final String endpoint = System.getenv().getOrDefault("COMPAT_REALTIME_URL",
            "wss://api.openai.com/v1/realtime");

    OpenAi(ObjectMapper json) {
        this.json = json;
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

    Stream stream(boolean asr, Consumer<JsonNode> events, Runnable failure) {
        return new Stream(asr ? "?intent=transcription" : "?model=gpt-realtime-2.1-mini", events, failure);
    }

    final class Stream implements WebSocket.Listener {
        private final String query;
        private final Consumer<JsonNode> events;
        private final Runnable failure;
        private final StringBuilder input = new StringBuilder();
        private CompletableFuture<WebSocket> opening;
        private CompletableFuture<WebSocket> outbound;
        private WebSocket socket;
        private int queued;
        private boolean stopped;

        Stream(String query, Consumer<JsonNode> events, Runnable failure) {
            this.query = query;
            this.events = events;
            this.failure = failure;
        }

        synchronized void open(JsonNode config) {
            opening = http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(4))
                    .header("Authorization", "Bearer " + key)
                    .buildAsync(URI.create(endpoint + query), this);
            outbound = opening.thenCompose(ws -> ws.sendText(config.toString(), true));
            outbound.whenComplete((ws, error) -> {
                if (error != null) {
                    failure.run();
                }
            });
        }

        synchronized void send(JsonNode event) {
            if (stopped || outbound == null || queued >= 24) {
                throw new IllegalStateException("Upstream send unavailable");
            }
            queued++;
            outbound = outbound.thenCompose(ws -> ws.sendText(event.toString(), true));
            outbound.whenComplete((ws, error) -> {
                synchronized (this) {
                    queued--;
                }
                if (error != null) {
                    failure.run();
                }
            });
        }

        synchronized void stop(String cancel) {
            stopped = true;
            if (socket == null) {
                if (opening != null) {
                    opening.cancel(true);
                }
                return;
            }
            WebSocket closing = socket;
            CompletableFuture<WebSocket> tail = outbound;
            if (cancel == null) {
                closing.abort();
            } else {
                tail.thenCompose(ws -> ws.sendText(cancel, true)).orTimeout(300, java.util.concurrent.TimeUnit.MILLISECONDS)
                        .whenComplete((ws, error) -> closing.abort());
            }
        }

        @Override
        public synchronized void onOpen(WebSocket ws) {
            if (stopped) {
                ws.abort();
            } else {
                socket = ws;
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
                    input.setLength(0);
                    events.accept(event);
                }
                ws.request(1);
            } catch (Exception error) {
                failure.run();
            }
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int status, String reason) {
            failure.run();
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            failure.run();
        }
    }
}
