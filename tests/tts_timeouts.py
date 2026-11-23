#!/usr/bin/env python3
"""Verify TTS stage timeouts and exact PCM delivery on the compatibility WebSocket."""

import argparse
import base64
from contextlib import ExitStack
import json
from pathlib import Path
import struct
import threading
import time

import concurrency as c
from media import count, tone_frame


def downsample(pcm):
    return b"".join(struct.pack("<h", int(sum(samples) / 3)) for samples in struct.iter_unpack("<hhh", pcm))


class TimeoutFixture(c.Fixture):
    def do_GET(self):
        if self.server.upgrade_delay:
            time.sleep(self.server.upgrade_delay)
        super().do_GET()

    def synthesize_model(self):
        if self.server.stall_ready:
            self.read_event("session.update")
            self.read_event()
            return
        super().synthesize_model()

    def render_tts(self, command, response, previous):
        probe = command["response"]["instructions"].split("\n\n", 1)[-1]
        if probe not in {"long", "first", "idle", "total", "cancel", "reuse-idle"}:
            return super().render_tts(command, response, previous)
        self.event({"type": "response.created", "response": response})
        if probe == "long":
            first, last = tone_frame(800), tone_frame(1600)
            start = time.monotonic()
            for frame in range(125):
                self.audio(response, first if frame < 124 else last)
                time.sleep(max(0, start + (frame + 1) * 0.1 - time.monotonic()))
            self.event({"type": "response.done", "response": {**response, "status": "completed"}})
            return True
        if probe in {"idle", "cancel", "reuse-idle"}:
            self.audio(response, tone_frame(800))
        if probe == "cancel":
            cancelled = self.read_event("response.cancel")
            assert cancelled["response_id"] == response["id"]
            count("model_cancels")
            return False
        if probe == "reuse-idle":
            assert previous is not None
        # Empty or superseded deltas must not refresh the audio deadline.
        start = time.monotonic()
        current_tone, previous_tone = tone_frame(800), tone_frame(2100)
        for frame in range(40):
            if probe == "total":
                self.audio(response, current_tone)
            elif probe == "reuse-idle":
                self.audio(previous, previous_tone)
            else:
                self.audio(response, b"")
            time.sleep(max(0, start + (frame + 1) * 0.09 - time.monotonic()))
        return False

    def audio(self, response, pcm):
        self.event({"type": "response.output_audio.delta", "response_id": response["id"],
                    "delta": base64.b64encode(pcm).decode()})


def run_task(port, probe, expected_name, counter, bounds):
    before = c.health(port)["events"].get(counter, 0)
    with ExitStack() as stack:
        peer = c.Peer(port, c.tts_path())
        stack.callback(peer.close)
        start = time.monotonic()
        peer.start_tts(probe)
        audio = bytearray()
        frames = 0
        while True:
            opcode, payload = peer.recv()
            if opcode in (0, 2):
                audio.extend(payload)
                frames += opcode == 2
            else:
                assert opcode == 1, (probe, opcode)
                header = json.loads(payload)["header"]
                elapsed = time.monotonic() - start
                print(f"{probe}: {header['name']} {elapsed:.3f}s {frames} PCM frames", flush=True)
                assert header["name"] == expected_name, header
                assert header["task_id"] == "a" * 32 and header["namespace"] == "SpeechSynthesizer"
                assert header["status"] == (20000000 if expected_name == "SynthesisCompleted" else 40000000)
                assert bounds[0] <= elapsed <= bounds[1], elapsed
                # Any second terminal event or late audio frame would fail this assertion.
                assert peer.close_code() == 1000
                break
    c.wait_until(lambda: c.no_active(port), f"{probe} did not release its TTS slot", 6)
    assert c.health(port)["events"][counter] == before + 1
    return bytes(audio)


def test_long_synthesis(port):
    success = "SynthesisCompleted"
    short = run_task(port, "800。", success, "tts_completed", (0, 2))
    assert short == c.expected_pcm(800)
    expected = downsample(tone_frame(800) * 124 + tone_frame(1600))
    received = run_task(port, "long", success, "tts_completed", (12.3, 17))
    assert received == expected
    assert run_task(port, "1100。", success, "tts_completed", (0, 2)) == c.expected_pcm(1100)


def test_expiry(port, server):
    failed = "TaskFailed"
    server.stall_ready = True
    try:
        assert not run_task(port, "ready", failed, "tts_ready_timeout", (0.5, 1.4))
    finally:
        server.stall_ready = False
    assert not run_task(port, "first", failed, "tts_first_audio_timeout", (0.65, 1.55))
    assert run_task(port, "idle", failed, "tts_audio_idle_timeout", (0.5, 1.5)) == downsample(tone_frame(800))
    received = run_task(port, "total", failed, "tts_total_timeout", (2.25, 3.2))
    assert len(received) >= 20 * 320 and received == downsample(tone_frame(800)) * (len(received) // 320)
    with ExitStack() as stack:
        before = c.COUNTS["model_cancels"]
        peer = c.Peer(port, c.tts_path())
        stack.callback(peer.close)
        peer.start_tts("cancel")
        assert peer.recv() == (2, downsample(tone_frame(800)))
    c.wait_until(lambda: c.no_active(port) and c.COUNTS["model_cancels"] == before + 1,
                 "disconnect did not cancel TTS or release its slot", 5)
    assert run_task(port, "1100。", "SynthesisCompleted", "tts_completed", (0, 2)) == c.expected_pcm(1100)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compat-jar", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    defaults = {"COMPAT_TTS_READY_TIMEOUT_MS": "5000", "COMPAT_TTS_FIRST_AUDIO_TIMEOUT_MS": "6000",
                "COMPAT_TTS_AUDIO_IDLE_TIMEOUT_MS": "5000", "COMPAT_TTS_TOTAL_TIMEOUT_MS": "30000"}
    short = {"COMPAT_TTS_READY_TIMEOUT_MS": "600", "COMPAT_TTS_FIRST_AUDIO_TIMEOUT_MS": "750",
             "COMPAT_TTS_AUDIO_IDLE_TIMEOUT_MS": "650", "COMPAT_TTS_TOTAL_TIMEOUT_MS": "2400"}
    with ExitStack() as stack:
        fixture = stack.enter_context(c.FixtureServer(("127.0.0.1", 0), TimeoutFixture))
        fixture.lock = threading.Lock()
        fixture.upgrade_delay = 0
        fixture.stall_ready = False
        threading.Thread(target=fixture.serve_forever, daemon=True).start()
        stack.callback(fixture.shutdown)
        with ExitStack() as gateway:
            port = c.launch(gateway, args, fixture, 0, "slow-upgrade.log",
                            {**defaults, "COMPAT_TTS_READY_TIMEOUT_MS": "6500"})
            fixture.upgrade_delay = 4.3
            try:
                audio = run_task(port, "800。", "SynthesisCompleted", "tts_completed", (4, 6.5))
                assert audio == c.expected_pcm(800)
            finally:
                fixture.upgrade_delay = 0
        with ExitStack() as gateway:
            port = c.launch(gateway, args, fixture, 0, "progress.log", defaults)
            test_long_synthesis(port)
        with ExitStack() as gateway:
            port = c.launch(gateway, args, fixture, 0, "expiry.log", short)
            test_expiry(port, fixture)
        with ExitStack() as gateway:
            port = c.launch(gateway, args, fixture, 1, "reuse.log", short)
            run_task(port, "800。", "SynthesisCompleted", "tts_completed", (0, 2))
            assert c.health(port)["ttsUpstreamIdle"] == 1
            received = run_task(port, "reuse-idle", "TaskFailed", "tts_audio_idle_timeout", (0.5, 1.5))
            assert received == downsample(tone_frame(800))
            assert c.health(port)["events"]["tts_reused"] == 1
            assert c.health(port)["ttsUpstreamIdle"] == 0
        with ExitStack() as gateway:
            port = c.launch(gateway, args, fixture, 0, "backpressure.log",
                            {**defaults, "COMPAT_TTS_AUDIO_IDLE_TIMEOUT_MS": "250"})
            c.test_slow_readers(port, fixture)
            assert c.health(port)["events"].get("tts_audio_idle_timeout", 0) == 0


if __name__ == "__main__":
    main()
