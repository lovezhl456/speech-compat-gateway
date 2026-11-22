package com.remond.speechcompat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
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
@EnableConfigurationProperties(TtsTimeouts.class)
public class Gateway implements WebSocketConfigurer {
    private static final Logger LOG = LoggerFactory.getLogger(Gateway.class);
    private final ObjectMapper json = new ObjectMapper();
    private final Auth auth = new Auth();
    private final Object admission = new Object();
    private final Map<String, Call> calls = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> counts = new ConcurrentHashMap<>();
    private final ExecutorService writers = workers(4, "compat-client-write");
    private final ExecutorService closers = workers(8, "compat-client-close");
    private final ExecutorService commands = workers(4, "compat-model-write");
    private final ExecutorService stageLogs = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1024), work -> worker(work, "compat-stage-log"),
            (work, executor) -> count("stage_log_dropped"));
    private final ScheduledThreadPoolExecutor timers = new ScheduledThreadPoolExecutor(2);
    private final OpenAi upstream;
    private final Semaphore asrSlots;
    private final Semaphore ttsSlots;
    private final int asrLimit;
    private final int ttsLimit;
    private final long asrResultTimeoutNanos;
    private final TtsTimeouts ttsTimeouts;
    private boolean stopping;

    public Gateway(@Value("${compat.asr-limit}") int asrLimit,
            @Value("${compat.tts-limit}") int ttsLimit,
            @Value("${compat.tts-idle-limit}") int ttsIdleLimit,
            @Value("${compat.asr-result-timeout-ms}") long asrResultTimeoutMs,
            TtsTimeouts ttsTimeouts) {
        if (asrLimit < 1 || ttsLimit < 1 || ttsIdleLimit < 0 || asrResultTimeoutMs < 1) {
            throw new IllegalArgumentException("Invalid compatibility capacity or timeout");
        }
        this.asrLimit = asrLimit;
        this.ttsLimit = ttsLimit;
        this.asrSlots = new Semaphore(asrLimit);
        this.ttsSlots = new Semaphore(ttsLimit);
        this.asrResultTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(asrResultTimeoutMs);
        this.ttsTimeouts = ttsTimeouts;
        this.upstream = new OpenAi(json, commands, timers, Math.min(ttsIdleLimit, ttsLimit));
        timers.setRemoveOnCancelPolicy(true);
    }

    private static ExecutorService workers(int size, String name) {
        return Executors.newFixedThreadPool(size, work -> worker(work, name));
    }

    private static Thread worker(Runnable work, String name) {
        var thread = new Thread(work, name);
        thread.setDaemon(true);
        return thread;
    }

    public static void main(String[] args) {
        SpringApplication.run(Gateway.class, args);
    }

    @jakarta.annotation.PreDestroy
    void shutdown() {
        Call[] active;
        synchronized (admission) {
            stopping = true;
            active = calls.values().toArray(Call[]::new);
        }
        CompletableFuture<Void> idleClosed = upstream.closeIdle();
        for (Call call : active) {
            call.disconnect();
        }
        try {
            CompletableFuture.allOf(idleClosed, CompletableFuture.allOf(java.util.Arrays.stream(active)
                    .map(call -> call.closed).toArray(CompletableFuture[]::new))).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException error) {
            LOG.warn("Gateway shutdown wait elapsed", error);
        }
        timers.shutdownNow();
        writers.shutdown();
        closers.shutdown();
        commands.shutdown();
        stageLogs.shutdown();
    }

    @RestController
    final class Access {
        @GetMapping("/healthz")
        Map<String, Object> health() {
            var events = new java.util.TreeMap<String, Long>();
            counts.forEach((name, count) -> events.put(name, count.sum()));
            return Map.of("status", "UP", "asrActive", asrLimit - asrSlots.availablePermits(),
                    "ttsActive", ttsLimit - ttsSlots.availablePermits(),
                    "asrLimit", asrLimit, "ttsLimit", ttsLimit, "ttsUpstreamIdle", upstream.idleTtsCount(),
                    "events", events);
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
            Call call = admit(socket);
            if (call == null) {
                count("busy");
                count(asr ? "asr_busy" : "tts_busy");
                socket.close(new CloseStatus(1013, "Busy"));
            } else if (socket.isOpen()) {
                call.receive(call::start);
            } else {
                call.disconnect();
            }
        }

        private Call admit(WebSocketSession socket) {
            Semaphore slots = asr ? asrSlots : ttsSlots;
            synchronized (admission) {
                if (stopping || !slots.tryAcquire()) {
                    return null;
                }
                try {
                    socket.setBinaryMessageSizeLimit(16_384);
                    socket.setTextMessageSizeLimit(16_384);
                    Call call = asr ? new Asr(socket) : new Tts(socket);
                    calls.put(socket.getId(), call);
                    return call;
                } catch (RuntimeException unavailable) {
                    slots.release();
                    throw unavailable;
                }
            }
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
        final ClientOutput output;
        final Semaphore slots;
        final CompletableFuture<Void> closed = new CompletableFuture<>();
        OpenAi.Stream model;
        private ScheduledFuture<?> timeout;
        private long deadlineVersion;
        private boolean done;

        Call(WebSocketSession socket, String label, Semaphore slots) {
            this.socket = socket;
            this.label = label;
            this.slots = slots;
            this.output = new ClientOutput(socket, writers, closers, this::clientWriteFailed);
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

        void receive(Input input) {
            synchronized (this) {
                if (!done) {
                    try {
                        input.run();
                    } catch (ClientOutput.Backpressure blocked) {
                        clientWriteFailed();
                    } catch (OpenAi.Backpressure blocked) {
                        fail("model_backpressure");
                    } catch (Exception exception) {
                        fail("invalid_or_unavailable");
                    }
                }
            }
            output.flush();
        }

        boolean open(boolean asr, ObjectNode config) {
            long openingNanos = System.nanoTime();
            model = upstream.stream(asr, event -> {
                receive(() -> {
                    if ("error".equals(event.path("type").asText())) {
                        JsonNode error = event.path("error");
                        if (acceptsModelError(error)) {
                            if ("rate_limit_exceeded".equals(error.path("code").asText())) {
                                count(label + "_model_rate_limited");
                            }
                            fail("model_rejected");
                        }
                    } else {
                        event(event);
                    }
                });
                return modelEventFlushed();
            }, () -> stage("upstream_connected", System.nanoTime() - openingNanos), this::fail);
            return model.open(config);
        }

        CompletableFuture<Void> modelEventFlushed() {
            return output.flushed();
        }

        boolean acceptsModelError(JsonNode error) {
            return true;
        }

        void deadline(long milliseconds) {
            deadline(TimeUnit.MILLISECONDS.toNanos(milliseconds), "timeout");
        }

        void deadline(long nanos, String reason) {
            if (done) {
                return;
            }
            clearDeadline();
            long version = deadlineVersion;
            timeout = timers.schedule(() -> expire(version, reason), nanos, TimeUnit.NANOSECONDS);
        }

        void clearDeadline() {
            deadlineVersion++;
            if (timeout != null) {
                timeout.cancel(false);
                timeout = null;
            }
        }

        private synchronized void expire(long version, String reason) {
            if (version == deadlineVersion) {
                fail(reason);
            }
        }

        void stage(String stage, long nanos) {
            String sessionId = socket.getId();
            stageLogs.execute(() -> LOG.info("path={} session={} stage={} elapsed_ms={}", label, sessionId, stage,
                    TimeUnit.NANOSECONDS.toMillis(nanos)));
        }

        CompletableFuture<Void> send(ObjectNode message) throws IOException {
            return output.send(message.toString());
        }

        void send(byte[] pcm) throws IOException {
            output.send(pcm);
        }

        synchronized void fail(String reason) {
            if (!done) {
                count(label + "_" + reason);
                try {
                    error();
                    end(false);
                } catch (IOException blocked) {
                    clientWriteFailed();
                }
            }
        }

        synchronized void clientWriteFailed() {
            count(label + "_client_backpressure");
            if (done) {
                output.abort();
            } else {
                end(false, false);
            }
        }

        synchronized void disconnect() {
            if (done) {
                // A peer can disappear after the terminal frame was queued but before its send callback.
                output.abort();
            } else {
                count(label + "_disconnected");
                end(false, false);
            }
        }

        String cancel() {
            return null;
        }

        boolean canRecycleModel() {
            return false;
        }

        void end(boolean success) {
            end(success, true);
        }

        void endAfterTtsCompletion(CompletableFuture<Void> terminalWrite) {
            end(true, true, terminalWrite.handle((ignored, error) -> {
                if (error == null) {
                    count("tts_completed");
                }
                return error == null;
            }));
        }

        private void end(boolean success, boolean drain) {
            end(success, drain, CompletableFuture.completedFuture(success));
        }

        private void end(boolean success, boolean drain, CompletableFuture<Boolean> delivered) {
            done = true;
            clearDeadline();
            CompletableFuture<Void> stopped = stopModel(success, delivered);
            if (drain) {
                output.finish();
            } else {
                output.abort();
            }
            CompletableFuture.allOf(stopped, output.closed(), delivered).whenComplete((ignored, error) -> {
                String sessionId = socket.getId();
                calls.remove(sessionId);
                slots.release();
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                stageLogs.execute(() -> LOG.info("path={} session={} success={} elapsed_ms={}",
                        label, sessionId, delivered.getNow(false), elapsedMs));
                closed.complete(null);
            });
        }

        private CompletableFuture<Void> stopModel(boolean success, CompletableFuture<Boolean> delivered) {
            if (model == null) {
                return CompletableFuture.completedFuture(null);
            }
            if (success && canRecycleModel()) {
                return upstream.finishTts(model, delivered, output.closed());
            }
            return model.stop(success ? null : cancel());
        }
    }

    private final class Asr extends Call {
        private final String voiceId;
        private final AsrTurns turns;
        private long oldestPending;
        private boolean inputEnded;

        Asr(WebSocketSession socket) {
            super(socket, "asr", asrSlots);
            voiceId = (String) socket.getAttributes().get("voiceId");
            turns = new AsrTurns(json, voiceId, this::send,
                    nanos -> stage("commit_to_final", nanos));
        }

        @Override
        void start() {
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
                        clearDeadline();
                        send(json.createObjectNode().put("code", 0).put("voice_id", voiceId));
                        turns.start(model::send);
                        count("asr_ready");
                        stage("ready", System.nanoTime() - startNanos);
                        if (inputEnded) {
                            finishOrAwaitResults();
                        }
                    }
                }
                default -> {
                    turns.event(event);
                    finishOrAwaitResults();
                }
            }
        }

        @Override
        void audio(ByteBuffer data) throws IOException {
            if (inputEnded) {
                throw new IllegalArgumentException("Audio received after end");
            }
            turns.audio(data);
            finishOrAwaitResults();
        }

        private void finishOrAwaitResults() throws IOException {
            if (!turns.ready()) {
                return;
            }
            long pending = turns.oldestPendingNanos();
            if (inputEnded && pending == 0) {
                send(json.createObjectNode().put("code", 0).put("voice_id", voiceId).put("final", 1));
                end(true);
                return;
            }
            if (pending != oldestPending) {
                oldestPending = pending;
                if (pending == 0) {
                    clearDeadline();
                } else {
                    deadline(asrResultTimeoutNanos - (System.nanoTime() - pending), "result_timeout");
                }
            }
        }

        @Override
        void text(JsonNode data) throws IOException {
            if (!"end".equals(data.path("type").asText())) {
                throw new IllegalArgumentException("Expected end");
            }
            if (inputEnded) {
                return;
            }
            inputEnded = true;
            if (turns.ready()) {
                turns.finishInput();
                finishOrAwaitResults();
            }
        }

        @Override
        void error() throws IOException {
            send(json.createObjectNode().put("code", 4000).put("voice_id", voiceId).put("message", "ASR unavailable"));
        }
    }

    private final class Tts extends Call {
        private final Pcm.Down pcm = new Pcm.Down();
        private final String requestId;
        private String task;
        private String text;
        private String responseId;
        private boolean reused;
        private boolean correlated;
        private boolean requested;
        private boolean audible;
        private boolean writingAudio;
        private long synthesisNanos;
        private long requestNanos;

        Tts(WebSocketSession socket) {
            super(socket, "tts", ttsSlots);
            requestId = "tts_" + socket.getId();
        }

        @Override
        void start() {
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
            synthesisNanos = System.nanoTime();
            synthesisDeadline(ttsTimeouts.readyMs(), "ready_timeout");
            var config = message("session.update");
            var session = config.putObject("session").put("type", "realtime");
            session.putArray("output_modalities").add("audio");
            var audio = session.putObject("audio");
            audio.putObject("input").putNull("turn_detection");
            var out = audio.putObject("output").put("voice", "marin");
            out.putObject("format").put("type", "audio/pcm").put("rate", 24000);
            reused = open(false, config);
            if (reused) {
                count("tts_reused");
                requestSynthesis();
            }
        }

        private void requestSynthesis() {
            if (requested) {
                return;
            }
            stage("upstream_ready", System.nanoTime() - startNanos);
            var command = message("response.create").put("event_id", requestId);
            var response = command.putObject("response").put("conversation", "none")
                    .put("instructions", "Read the following text exactly as written, in its language. "
                            + "Speak only the given text, including all numbers.\n\n" + text);
            response.putObject("metadata").put("compat_request", requestId);
            response.putArray("input");
            response.putArray("output_modalities").add("audio");
            requested = true;
            requestNanos = System.nanoTime();
            synthesisDeadline(ttsTimeouts.firstAudioMs(), "first_audio_timeout");
            model.send(command);
        }

        private long remainingSynthesisNanos() {
            return TimeUnit.MILLISECONDS.toNanos(ttsTimeouts.totalMs()) - (System.nanoTime() - synthesisNanos);
        }

        private void synthesisDeadline(long stageMs, String reason) {
            long remaining = remainingSynthesisNanos();
            long stage = TimeUnit.MILLISECONDS.toNanos(stageMs);
            if (stage < remaining) {
                deadline(stage, reason);
            } else {
                deadline(remaining, "total_timeout");
            }
        }

        @Override
        synchronized CompletableFuture<Void> modelEventFlushed() {
            var flushed = super.modelEventFlushed();
            return writingAudio ? flushed.thenRun(this::awaitNextAudio) : flushed;
        }

        private synchronized void awaitNextAudio() {
            writingAudio = false;
            synthesisDeadline(ttsTimeouts.audioIdleMs(), "audio_idle_timeout");
        }

        private void responseCreated(JsonNode response) {
            String request = response.path("metadata").path("compat_request").asText();
            if (requestId.equals(request)) {
                responseId = response.path("id").asText(null);
                correlated = responseId != null;
            } else if (!reused && request.isEmpty() && responseId == null) {
                responseId = response.path("id").asText(null);
            }
        }

        private boolean isCurrentResponse(String id) {
            if (!id.isEmpty()) {
                return id.equals(responseId);
            }
            // A legacy upstream may omit identifiers. It can serve one task on a cold connection.
            correlated = false;
            return !reused;
        }

        @Override
        boolean acceptsModelError(JsonNode error) {
            String eventId = error.path("event_id").asText();
            return !reused || eventId.isEmpty() || requestId.equals(eventId);
        }

        @Override
        void event(JsonNode event) throws IOException {
            switch (event.path("type").asText()) {
                case "session.updated" -> requestSynthesis();
                case "response.created" -> responseCreated(event.path("response"));
                case "response.output_audio.delta" -> {
                    if (!isCurrentResponse(event.path("response_id").asText())) {
                        return;
                    }
                    byte[] converted = pcm.append(java.util.Base64.getDecoder().decode(event.path("delta").asText()));
                    if (converted.length == 0) {
                        return;
                    }
                    boolean first = !audible;
                    if (first) {
                        count("tts_first_audio");
                        stage("first_pcm", System.nanoTime() - startNanos);
                        stage("response_to_first_pcm", System.nanoTime() - requestNanos);
                        audible = true;
                    }
                    send(converted);
                    // No next upstream event is requested until this write completes.
                    writingAudio = true;
                    deadline(remainingSynthesisNanos(), "total_timeout");
                    if (first) {
                        output.flushed().thenRun(() -> stage("first_pcm_sent", System.nanoTime() - startNanos));
                    }
                }
                case "response.done" -> {
                    JsonNode response = event.path("response");
                    if (!isCurrentResponse(response.path("id").asText())) {
                        return;
                    }
                    if (!"completed".equals(response.path("status").asText()) || !audible) {
                        fail("incomplete");
                        return;
                    }
                    send(pcm.finish());
                    endAfterTtsCompletion(send(nls("SynthesisCompleted", 20000000)));
                }
                default -> { }
            }
        }

        @Override
        boolean canRecycleModel() {
            return correlated;
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
