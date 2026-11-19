package com.remond.speechcompat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

@SpringBootApplication
@EnableWebSocket
public class Gateway implements WebSocketConfigurer {
    private static final Logger LOG = LoggerFactory.getLogger(Gateway.class);
    private final ObjectMapper json = new ObjectMapper();
    private final Auth auth = new Auth();
    private final OpenAi upstream = new OpenAi(json);
    private final Map<String, Call> calls = new ConcurrentHashMap<>();
    private final java.util.concurrent.Semaphore slots = new java.util.concurrent.Semaphore(64);
    private final Map<String, LongAdder> counts = new ConcurrentHashMap<>();
    private final java.util.concurrent.ScheduledThreadPoolExecutor timers =
            (java.util.concurrent.ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(2);

    public Gateway() {
        timers.setRemoveOnCancelPolicy(true);
    }

    public static void main(String[] args) {
        SpringApplication.run(Gateway.class, args);
    }

    @jakarta.annotation.PreDestroy
    void shutdown() {
        calls.values().forEach(Call::disconnect);
        timers.shutdownNow();
    }

    @RestController
    final class Access {
        @GetMapping("/healthz")
        Map<String, Object> health() {
            var events = new java.util.TreeMap<String, Long>();
            counts.forEach((name, count) -> events.put(name, count.sum()));
            return Map.of("status", "UP", "asrActive", calls.values().stream().filter(c -> c instanceof Asr).count(),
                    "ttsActive", calls.values().stream().filter(c -> c instanceof Tts).count(), "events", events);
        }

        @GetMapping(value = "/", params = "Action=CreateToken")
        ResponseEntity<?> token(jakarta.servlet.http.HttpServletRequest request) {
            try {
                if (auth.aliyun(Auth.query(request.getQueryString()))) {
                    long expires = Instant.now().plusSeconds(3600).getEpochSecond();
                    count("token_issued");
                    return ResponseEntity.ok().header("Cache-Control", "no-store")
                            .body(Map.of("Token", Map.of("Id", auth.token(expires), "ExpireTime", expires)));
                }
            } catch (RuntimeException ignored) {
                // A malformed or expired signing request is handled as invalid credentials.
            }
            count("auth_rejected");
            return ResponseEntity.status(403).body(Map.of("Code", "InvalidSignature", "Message", "Invalid credentials"));
        }
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(new Handler(true), "/asr/v2/*").addInterceptors(new Authenticate(true));
        registry.addHandler(new Handler(false), "/ws/v1").addInterceptors(new Authenticate(false));
    }

    private final class Authenticate implements HandshakeInterceptor {
        private final boolean asr;

        Authenticate(boolean asr) {
            this.asr = asr;
        }

        @Override
        public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler handler,
                Map<String, Object> attributes) {
            try {
                var fields = Auth.query(request.getURI().getRawQuery());
                if (asr && auth.tencent(request.getURI().getPath(), fields,
                        request.getHeaders().getFirst("Authorization"))) {
                    attributes.put("voiceId", fields.get("voice_id"));
                    return true;
                }
                String token = request.getHeaders().getFirst("X-NLS-Token");
                if (!asr && auth.validToken(token == null ? fields.get("token") : token)) {
                    return true;
                }
            } catch (RuntimeException ignored) {
                // Invalid requests share a single response without exposing authentication data.
            }
            count("auth_rejected");
            response.setStatusCode(HttpStatus.FORBIDDEN);
            return false;
        }

        @Override
        public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler handler,
                Exception exception) {}
    }

    private final class Handler extends AbstractWebSocketHandler {
        private final boolean asr;

        Handler(boolean asr) {
            this.asr = asr;
        }

        @Override
        public void afterConnectionEstablished(WebSocketSession socket) throws IOException {
            if (!slots.tryAcquire()) {
                count("busy");
                socket.close(new CloseStatus(1013, "Busy"));
                return;
            }
            socket.setBinaryMessageSizeLimit(16_384);
            socket.setTextMessageSizeLimit(16_384);
            Call call = asr ? new Asr(socket) : new Tts(socket);
            calls.put(socket.getId(), call);
            call.start();
        }

        @Override
        protected void handleTextMessage(WebSocketSession socket, TextMessage message) {
            Call call = calls.get(socket.getId());
            if (call != null) {
                call.receive(() -> call.text(json.readTree(message.getPayload())));
            }
        }

        @Override
        protected void handleBinaryMessage(WebSocketSession socket, BinaryMessage message) {
            Call call = calls.get(socket.getId());
            if (call != null) {
                call.receive(() -> call.audio(message.getPayload()));
            }
        }

        @Override
        public void handleTransportError(WebSocketSession socket, Throwable error) {
            afterConnectionClosed(socket, CloseStatus.SERVER_ERROR);
        }

        @Override
        public void afterConnectionClosed(WebSocketSession socket, CloseStatus status) {
            Call call = calls.get(socket.getId());
            if (call != null) {
                call.disconnect();
            }
        }
    }

    private interface Input {
        void run() throws IOException;
    }

    abstract class Call {
        final WebSocketSession socket;
        final String label;
        final long startNanos = System.nanoTime();
        OpenAi.Stream model;
        ScheduledFuture<?> timeout;
        boolean done;

        Call(WebSocketSession socket, String label) {
            this.socket = socket;
            this.label = label;
        }

        abstract void start();
        abstract void event(JsonNode event) throws IOException;
        abstract void error() throws IOException;

        void text(JsonNode data) throws IOException {
            throw new IllegalArgumentException("Unexpected message");
        }

        void audio(ByteBuffer data) throws IOException {
            throw new IllegalArgumentException("Unexpected audio");
        }

        synchronized void receive(Input input) {
            if (!done) {
                try {
                    input.run();
                } catch (Exception exception) {
                    fail("invalid_or_unavailable");
                }
            }
        }

        void open(boolean asr, ObjectNode config) {
            model = upstream.stream(asr, event -> receive(() -> {
                if ("error".equals(event.path("type").asText())) {
                    fail("model_rejected");
                } else {
                    event(event);
                }
            }), () -> fail("model_disconnected"));
            model.open(config);
        }

        void deadline(long milliseconds) {
            if (timeout != null) {
                timeout.cancel(false);
            }
            timeout = timers.schedule(() -> fail("timeout"), milliseconds, TimeUnit.MILLISECONDS);
        }

        void stage(String stage, long nanos) {
            LOG.info("path={} session={} stage={} elapsed_ms={}", label, socket.getId(), stage,
                    TimeUnit.NANOSECONDS.toMillis(nanos));
        }

        void send(ObjectNode message) throws IOException {
            socket.sendMessage(new TextMessage(message.toString()));
        }

        void send(byte[] pcm) throws IOException {
            if (pcm.length > 0) {
                socket.sendMessage(new BinaryMessage(pcm));
            }
        }

        synchronized void fail(String reason) {
            if (!done) {
                count(label + "_" + reason);
                try {
                    error();
                } catch (IOException ignored) {
                    // The client may already have disconnected.
                } finally {
                    end(false);
                }
            }
        }

        synchronized void disconnect() {
            if (!done) {
                count(label + "_disconnected");
                end(false);
            }
        }

        String cancel() {
            return null;
        }

        void end(boolean success) {
            done = true;
            if (timeout != null) {
                timeout.cancel(false);
            }
            if (model != null) {
                model.stop(success ? null : cancel());
            }
            try {
                socket.close();
            } catch (IOException ignored) {
                // Cleanup continues for a disconnected client.
            } finally {
                calls.remove(socket.getId());
                slots.release();
                LOG.info("path={} session={} success={} elapsed_ms={}", label, socket.getId(), success,
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
            }
        }
    }

    private final class Asr extends Call {
        private final String voiceId;
        private final AsrTurns turns;

        Asr(WebSocketSession socket) {
            super(socket, "asr");
            voiceId = (String) socket.getAttributes().get("voiceId");
            turns = new AsrTurns(json, voiceId, this::send,
                    nanos -> stage("commit_to_final", nanos));
        }

        @Override
        synchronized void start() {
            deadline(4400);
            var config = message("session.update");
            var session = config.putObject("session").put("type", "transcription");
            var audio = session.putObject("audio").putObject("input");
            audio.putObject("format").put("type", "audio/pcm").put("rate", 24000);
            audio.putObject("transcription").put("model", "gpt-live-transcribe").put("language", "zh");
            audio.putNull("turn_detection");
            open(true, config);
        }

        @Override
        void event(JsonNode event) throws IOException {
            switch (event.path("type").asText()) {
                case "session.updated", "transcription_session.updated" -> {
                    if (!turns.ready()) {
                        timeout.cancel(false);
                        send(json.createObjectNode().put("code", 0).put("voice_id", voiceId));
                        turns.start(model::send);
                        count("asr_ready");
                        stage("ready", System.nanoTime() - startNanos);
                    }
                }
                default -> turns.event(event);
            }
        }

        @Override
        void audio(ByteBuffer data) throws IOException {
            turns.audio(data);
        }

        @Override
        void text(JsonNode data) throws IOException {
            if (!"end".equals(data.path("type").asText())) {
                throw new IllegalArgumentException("Expected end");
            }
            send(json.createObjectNode().put("code", 0).put("voice_id", voiceId).put("final", 1));
            end(true);
        }

        @Override
        void error() throws IOException {
            send(json.createObjectNode().put("code", 4000).put("voice_id", voiceId).put("message", "ASR unavailable"));
        }
    }

    private final class Tts extends Call {
        private final Pcm.Down pcm = new Pcm.Down();
        private String task;
        private String text;
        private String responseId;
        private boolean requested;
        private boolean audible;

        Tts(WebSocketSession socket) {
            super(socket, "tts");
        }

        @Override
        synchronized void start() {
            deadline(3000);
        }

        @Override
        void text(JsonNode data) {
            var header = data.path("header");
            var payload = data.path("payload");
            String id = header.path("task_id").asText();
            String input = payload.path("text").asText();
            String voice = payload.path("voice").asText();
            if (task != null || !"StartSynthesis".equals(header.path("name").asText())
                    || !"SpeechSynthesizer".equals(header.path("namespace").asText())
                    || !id.matches("[a-fA-F0-9]{32}")) {
                throw new IllegalArgumentException("Invalid StartSynthesis");
            }
            task = id;
            if (!auth.appKey(header.path("appkey").asText()) || !"pcm".equals(payload.path("format").asText())
                    || payload.path("sample_rate").asInt() != 8000 || input.isBlank() || input.length() > 4096
                    || !("aixia".equals(voice) || "xiaoyun".equals(voice))) {
                throw new IllegalArgumentException("Unsupported synthesis settings");
            }
            text = input;
            deadline(10_500);
            var config = message("session.update");
            var session = config.putObject("session").put("type", "realtime");
            session.putArray("output_modalities").add("audio");
            var audio = session.putObject("audio");
            audio.putObject("input").putNull("turn_detection");
            var out = audio.putObject("output").put("voice", "marin");
            out.putObject("format").put("type", "audio/pcm").put("rate", 24000);
            open(false, config);
        }

        @Override
        void event(JsonNode event) throws IOException {
            switch (event.path("type").asText()) {
                case "session.updated" -> {
                    if (!requested) {
                        var command = message("response.create");
                        var response = command.putObject("response").put("conversation", "none")
                                .put("instructions", "Read the following text exactly as written, in its language. "
                                        + "Speak only the given text, including all numbers.\n\n" + text);
                        response.putArray("input");
                        response.putArray("output_modalities").add("audio");
                        requested = true;
                        model.send(command);
                    }
                }
                case "response.created" -> responseId = event.path("response").path("id").asText(null);
                case "response.output_audio.delta" -> {
                    byte[] converted = pcm.append(java.util.Base64.getDecoder().decode(event.path("delta").asText()));
                    if (converted.length > 0 && !audible) {
                        count("tts_first_audio");
                        stage("first_pcm", System.nanoTime() - startNanos);
                        audible = true;
                    }
                    send(converted);
                }
                case "response.done" -> {
                    if (!"completed".equals(event.path("response").path("status").asText()) || !audible) {
                        fail("incomplete");
                        return;
                    }
                    send(pcm.finish());
                    send(nls("SynthesisCompleted", 20000000));
                    count("tts_completed");
                    end(true);
                }
                default -> { }
            }
        }

        @Override
        String cancel() {
            if (!requested) {
                return null;
            }
            var cancel = message("response.cancel");
            if (responseId != null) {
                cancel.put("response_id", responseId);
            }
            count("tts_cancelled");
            return cancel.toString();
        }

        @Override
        void error() throws IOException {
            if (task != null) {
                send(nls("TaskFailed", 40000000));
            }
        }

        private ObjectNode nls(String name, int status) {
            var result = json.createObjectNode();
            result.putObject("header").put("namespace", "SpeechSynthesizer").put("name", name)
                    .put("task_id", task).put("status", status);
            return result;
        }
    }

    private ObjectNode message(String type) {
        return json.createObjectNode().put("type", type);
    }

    private void count(String name) {
        counts.computeIfAbsent(name, key -> new LongAdder()).increment();
    }
}
