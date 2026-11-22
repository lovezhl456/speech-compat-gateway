#!/usr/bin/env python3
"""Check Tencent-compatible end-of-input against a local, controlled transcription peer."""

import argparse
import base64
from contextlib import ExitStack
from pathlib import Path
from queue import Queue
import select
import socket
import struct
from threading import Event, Thread

import concurrency as wire


VOICE = struct.pack("<h", 1200) * 160


class AsrEndFixture(wire.OpenAiFixture):
    def recognize_model(self):
        self.read_event("session.update")
        self.commits = Queue()
        self.server.recognizers.put(self)
        assert self.server.ready.wait(5)
        self.event({"type": "session.updated"})
        buffered_bytes = 0
        index = 0
        while True:
            event = self.read_event()
            if event["type"] == "input_audio_buffer.append":
                buffered_bytes += len(base64.b64decode(event["audio"], validate=True))
            elif event["type"] == "input_audio_buffer.clear":
                buffered_bytes = 0
            else:
                assert event["type"] == "input_audio_buffer.commit", event
                item = f"item-{index}"
                index += 1
                self.event({"type": "input_audio_buffer.committed", "item_id": item})
                self.commits.put((item, buffered_bytes))
                buffered_bytes = 0

    def complete(self, item, transcript):
        self.event({"type": "conversation.item.input_audio_transcription.completed",
                    "item_id": item, "transcript": transcript})


def next_result(peer, slice_type, index, text):
    event = peer.event()
    assert event["code"] == 0 and "final" not in event, event
    result = event["result"]
    assert (result["slice_type"], result["index"], result["voice_text_str"]) == (slice_type, index, text), result
    return result


def terminal(peer, success=True):
    event = peer.event()
    if success:
        assert event["code"] == 0 and event["final"] == 1, event
    else:
        assert event["code"] != 0 and "final" not in event, event
    assert peer.close_code() == 1000


def expect_no_terminal(peer):
    readable, _, _ = select.select([peer.socket], [], [], 0.15)
    assert not readable, "ASR announced completion while a committed result was still pending"


def submitted_final(peer, model, end_first):
    wire.send_asr_turn(peer)
    next_result(peer, 0, 0, "")
    item, _ = model.commits.get(timeout=2)
    if end_first:
        peer.text({"type": "end"})
        expect_no_terminal(peer)
    model.complete(item, "末句")
    next_result(peer, 2, 0, "末句")
    if not end_first:
        peer.text({"type": "end"})
    terminal(peer)


def active_tail(peer, model):
    # Split a sample across WebSocket messages. The last 10 ms is still valid PCM.
    audio = VOICE * 5 + VOICE[:160]
    peer.send(2, audio[:319])
    peer.send(2, audio[319:])
    next_result(peer, 0, 0, "")
    peer.text({"type": "end"})
    item, size = model.commits.get(timeout=2)
    assert size == len(audio) * 3, size
    peer.text({"type": "end"})
    model.complete(item, "无需尾部静音")
    result = next_result(peer, 2, 0, "无需尾部静音")
    assert result["end_time"] == 110, result
    terminal(peer)
    assert model.commits.empty()


def multiple_pending(peer, model):
    for index in range(2):
        wire.send_asr_turn(peer)
        next_result(peer, 0, index, "")
    first, _ = model.commits.get(timeout=2)
    second, _ = model.commits.get(timeout=2)
    peer.text({"type": "end"})
    model.complete(second, "第二句先完成")
    next_result(peer, 2, 1, "第二句先完成")
    model.event({"type": "input_audio_buffer.committed", "item_id": second})
    model.complete(second, "重复结果")
    expect_no_terminal(peer)
    model.complete(first, "第一句")
    next_result(peer, 2, 0, "第一句")
    terminal(peer)


def short_tail(peer, model):
    peer.send(2, VOICE * 4 + VOICE[:160])
    next_result(peer, 0, 0, "")
    peer.text({"type": "end"})
    result = next_result(peer, 2, 0, "")
    assert result["end_time"] == 90, result
    terminal(peer)
    assert model.commits.empty()


def silence(peer, model):
    peer.send(2, bytes(640))
    peer.text({"type": "end"})
    terminal(peer)
    assert model.commits.empty()


def incomplete_sample(peer, model):
    peer.send(2, VOICE * 5 + b"\x00")
    next_result(peer, 0, 0, "")
    peer.text({"type": "end"})
    terminal(peer, success=False)
    assert model.commits.empty()


def failed_drain(peer, model, failure):
    wire.send_asr_turn(peer)
    next_result(peer, 0, 0, "")
    model.commits.get(timeout=2)
    peer.text({"type": "end"})
    if failure == "upstream-close":
        expect_no_terminal(peer)
        model.connection.shutdown(socket.SHUT_RDWR)
    if failure == "extra-audio":
        peer.send(2, VOICE)
    terminal(peer, success=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compat-jar", type=Path, default=wire.ROOT / "target/speech-compat-gateway-0.1.0-SNAPSHOT.jar")
    parser.add_argument("--output-dir", type=Path, default=wire.ROOT / ".local/test-asr-end")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    cases = (
        ("result-before-end", submitted_final, False),
        ("end-before-result", submitted_final, True),
        ("active-tail-repeated-end", active_tail),
        ("two-pending-reordered-and-duplicate", multiple_pending),
        ("short-tail", short_tail),
        ("silence", silence),
        ("incomplete-pcm-sample", incomplete_sample),
        ("pending-timeout", failed_drain, "timeout"),
        ("pending-upstream-close", failed_drain, "upstream-close"),
        ("audio-after-end", failed_drain, "extra-audio"),
    )
    with ExitStack() as stack:
        fixture = stack.enter_context(wire.FixtureServer(("127.0.0.1", 0), AsrEndFixture))
        fixture.recognizers = Queue()
        fixture.ready = Event()
        fixture.ready.set()
        Thread(target=fixture.serve_forever, daemon=True).start()
        stack.callback(fixture.shutdown)
        port = wire.launch(stack, args, fixture, idle_limit=0, log_name="gateway.log")
        for name, test, *extra in cases:
            with ExitStack() as client:
                peer = wire.open_asr(port, client, name)
                test(peer, fixture.recognizers.get(timeout=2), *extra)
            wire.wait_until(lambda: wire.no_active(port), "ASR slot was not released", 5)
            print(f"{name}: passed", flush=True)
        with ExitStack() as client:
            fixture.ready.clear()
            peer = wire.Peer(port, wire.asr_path("end-before-ready"))
            client.callback(peer.close)
            model = fixture.recognizers.get(timeout=2)
            peer.text({"type": "end"})
            expect_no_terminal(peer)
            fixture.ready.set()
            ready = peer.event()
            assert ready["code"] == 0 and "final" not in ready, ready
            terminal(peer)
            assert model.commits.empty()
        wire.wait_until(lambda: wire.no_active(port), "ASR slot was not released", 5)
        print("end-before-ready: passed", flush=True)
        metrics = wire.health(port)["events"]
        assert metrics.get("asr_result_timeout") == 1, metrics


if __name__ == "__main__":
    main()
