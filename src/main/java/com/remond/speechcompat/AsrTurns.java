package com.remond.speechcompat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.function.LongConsumer;
import java.util.function.Consumer;

final class AsrTurns {
    private static final int MAX_PENDING = 8;
    private final ObjectMapper json;
    private final String voiceId;
    private final Deliver deliver;
    private final LongConsumer finalTiming;
    private final byte[] frame = new byte[Pcm.FRAME_BYTES];
    private final ArrayDeque<Turn> committed = new ArrayDeque<>();
    private final LinkedHashMap<String, Turn> byItem = new LinkedHashMap<>();
    private final LinkedHashSet<String> retired = new LinkedHashSet<>();
    private Consumer<JsonNode> send;
    private int offset;
    private long clock;
    private int index;
    private Turn current;

    interface Deliver {
        void send(ObjectNode message) throws IOException;
    }

    AsrTurns(ObjectMapper json, String voiceId, Deliver deliver, LongConsumer finalTiming) {
        this.json = json;
        this.voiceId = voiceId;
        this.deliver = deliver;
        this.finalTiming = finalTiming;
    }

    boolean ready() {
        return send != null;
    }

    void start(Consumer<JsonNode> upstream) {
        send = upstream;
    }

    void audio(ByteBuffer data) throws IOException {
        if (!ready()) {
            throw new IllegalStateException("Recognition not ready");
        }
        while (data.hasRemaining()) {
            int count = Math.min(data.remaining(), frame.length - offset);
            data.get(frame, offset, count);
            offset += count;
            if (offset == frame.length) {
                process();
                clock += 20;
                offset = 0;
            }
        }
        flushAudio();
    }

    private void process() throws IOException {
        boolean speech = Pcm.speech(frame);
        if (current == null) {
            if (!speech) {
                return;
            }
            current = new Turn(index++, clock);
            deliver.send(result(current, 0, ""));
        }
        if (speech) {
            current.lastVoice = clock + 20;
            current.voiced += 20;
        }
        current.audio.writeBytes(Pcm.up(frame));
        if (clock + 20 - current.lastVoice >= 700 || clock + 20 - current.start >= 25_000) {
            finish();
        }
    }

    private void flushAudio() {
        if (current != null && current.audio.size() > 0) {
            send.accept(message("input_audio_buffer.append")
                    .put("audio", Base64.getEncoder().encodeToString(current.audio.toByteArray())));
            current.audio.reset();
        }
    }

    private void finish() throws IOException {
        flushAudio();
        Turn turn = current;
        current = null;
        if (turn.voiced < 100) {
            send.accept(message("input_audio_buffer.clear"));
            deliver.send(result(turn, 2, ""));
        } else {
            if (committed.size() + byItem.size() >= MAX_PENDING) {
                throw new IllegalStateException("Recognition results are stalled");
            }
            turn.committedNanos = System.nanoTime();
            committed.addLast(turn);
            send.accept(message("input_audio_buffer.commit"));
        }
    }

    void event(JsonNode event) throws IOException {
        String id = event.path("item_id").asText();
        String type = event.path("type").asText();
        if ("input_audio_buffer.committed".equals(type)) {
            if (id.isBlank() || byItem.containsKey(id) || retired.contains(id)) {
                return;
            }
            Turn turn = committed.pollFirst();
            if (turn == null) {
                return;
            }
            byItem.put(id, turn);
        } else {
            Turn turn = byItem.get(id);
            if (turn == null) {
                return;
            }
            switch (type) {
                case "conversation.item.input_audio_transcription.delta" -> {
                    turn.partial.append(event.path("delta").asText());
                    if (turn.partial.length() > 16_384) {
                        throw new IllegalArgumentException("Oversize transcript");
                    }
                    deliver.send(result(turn, 1, turn.partial.toString()));
                }
                case "conversation.item.input_audio_transcription.completed" -> {
                    byItem.remove(id);
                    retired.add(id);
                    if (retired.size() > 64) {
                        var iterator = retired.iterator();
                        iterator.next();
                        iterator.remove();
                    }
                    finalTiming.accept(System.nanoTime() - turn.committedNanos);
                    deliver.send(result(turn, 2, event.path("transcript").asText()));
                }
                case "conversation.item.input_audio_transcription.failed" -> {
                    throw new IOException("Model transcription failed");
                }
                default -> { }
            }
        }
    }

    private ObjectNode result(Turn turn, int slice, String text) {
        var event = json.createObjectNode().put("code", 0).put("voice_id", voiceId);
        event.putObject("result").put("slice_type", slice).put("index", turn.index)
                .put("start_time", turn.start).put("end_time", turn.lastVoice).put("voice_text_str", text);
        return event;
    }

    long oldestPendingNanos() {
        // Acknowledgements take turns from committed in order, so any acknowledged turn is older.
        if (!byItem.isEmpty()) {
            return byItem.values().iterator().next().committedNanos;
        }
        Turn first = committed.peekFirst();
        return first == null ? 0 : first.committedNanos;
    }

    private ObjectNode message(String type) {
        return json.createObjectNode().put("type", type);
    }

    private static final class Turn {
        final int index;
        final long start;
        final StringBuilder partial = new StringBuilder();
        final java.io.ByteArrayOutputStream audio = new java.io.ByteArrayOutputStream();
        long lastVoice;
        int voiced;
        long committedNanos;

        Turn(int index, long start) {
            this.index = index;
            this.start = start;
            lastVoice = start + 20;
        }
    }
}
