#!/usr/bin/env python3
"""Exercise gateway capacity and slow peers with loopback WebSockets and a fake upstream."""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import hashlib
import hmac
from http.server import ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import struct
import threading
import time
from urllib.parse import urlencode

from media import COUNTS, KEY, ROOT, OpenAiFixture, health, no_active, route_tools, tone_frame, wait_until


ENV = {
    "TENCENT_ASR_APP_ID": "12345",
    "TENCENT_ASR_SECRET_ID": "fixture-id",
    "TENCENT_ASR_SECRET_KEY": "fixture-tencent",
    "ALIYUN_NLS_APP_KEY": "fixture-app",
    "ALIYUN_AK_ID": "fixture-id",
    "ALIYUN_AK_SECRET": "fixture-aliyun",
    "OPENAI_API_KEY": KEY,
    "OPENAI_LIVE_HTTP_PROXY": "",
}
ASR_LIMIT = 64
TTS_LIMIT = 64


def sign(key, value, digest):
    return base64.b64encode(hmac.digest(ENV[key].encode(), value.encode(), digest)).decode()


def asr_path(voice):
    path = "/asr/v2/12345"
    now = int(time.time())
    fields = {
        "engine_model_type": "8k_zh", "voice_format": "1", "needvad": "1",
        "vad_silence_time": "700", "timestamp": str(now), "expired": str(now + 60),
        "voice_id": voice, "secretid": "fixture-id",
    }
    canonical = "&".join(f"{key}={value}" for key, value in sorted(fields.items()))
    fields["signature"] = sign("TENCENT_ASR_SECRET_KEY", "asr.cloud.tencent.com" + path + "?" + canonical, "sha1")
    return path + "?" + urlencode(fields)


def tts_path():
    value = f"{int(time.time()) + 60}.fixture"
    token = value + "." + sign("ALIYUN_AK_SECRET", value, "sha256")
    return "/ws/v1?" + urlencode({"token": token})


class Peer:
    def __init__(self, port, path, receive_buffer=None):
        self.socket = socket.socket()
        self.socket.settimeout(8)
        if receive_buffer:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
        self.stream = self.socket.makefile("rb")
        try:
            self.socket.connect(("127.0.0.1", port))
            key = base64.b64encode(os.urandom(16)).decode()
            self.socket.sendall((
                f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\n"
                f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
            ).encode())
            status = self.stream.readline()
            assert status.startswith(b"HTTP/1.1 101 "), status
            while self.stream.readline() not in (b"\r\n", b""):
                pass
        except Exception:
            self.close()
            raise

    def close(self):
        self.stream.close()
        self.socket.close()

    def send(self, opcode, body):
        mask = os.urandom(4)
        size = bytes([len(body) | 128]) if len(body) < 126 else b"\xfe" + struct.pack("!H", len(body))
        masked = bytes(value ^ mask[i % 4] for i, value in enumerate(body))
        self.socket.sendall(bytes([0x80 | opcode]) + size + mask + masked)

    def text(self, event):
        self.send(1, json.dumps(event).encode())

    def recv(self):
        header = self.stream.read(2)
        assert len(header) == 2, header
        length = header[1] & 127
        if length in (126, 127):
            length = int.from_bytes(self.stream.read(2 if length == 126 else 8), "big")
        return header[0] & 15, self.stream.read(length)

    def event(self):
        opcode, data = self.recv()
        assert opcode == 1, (opcode, data)
        return json.loads(data)

    def close_code(self):
        opcode, data = self.recv()
        assert opcode == 8, (opcode, data)
        return int.from_bytes(data[:2], "big")

    def start_tts(self, text="隔离拨测。"):
        self.text({
            "header": {"name": "StartSynthesis", "namespace": "SpeechSynthesizer",
                       "task_id": "a" * 32, "appkey": ENV["ALIYUN_NLS_APP_KEY"]},
            "payload": {"text": text, "voice": "aixia", "format": "pcm", "sample_rate": 8000},
        })


class FixtureServer(ThreadingHTTPServer):
    request_queue_size = 128
    slow_tasks = 0
    stall_result = False
    tts_barrier = None


class Fixture(OpenAiFixture):
    def render_tts(self, command, result, previous):
        barrier = self.server.tts_barrier
        if barrier is not None:
            barrier.wait(6)
        return super().render_tts(command, result, previous)

    def synthesize_model(self):
        with self.server.lock:
            slow = self.server.slow_tasks > 0
            if slow:
                self.server.slow_tasks -= 1
        if not slow:
            return super().synthesize_model()
        self.read_event("session.update")
        self.event({"type": "session.updated"})
        self.read_event("response.create")
        self.event({"type": "response.created", "response": {"id": "slow-reader"}})
        delta = base64.b64encode(bytes(46_080)).decode()
        for _ in range(700):
            self.event({"type": "response.output_audio.delta", "delta": delta})

    def recognize_model(self):
        if not self.server.stall_result:
            return super().recognize_model()
        self.read_event("session.update")
        self.event({"type": "session.updated"})
        while self.read_event()["type"] != "input_audio_buffer.commit":
            pass
        self.event({"type": "input_audio_buffer.committed", "item_id": "pending"})
        self.read_event()


def open_asr(port, stack, voice):
    peer = Peer(port, asr_path(voice))
    stack.callback(peer.close)
    assert peer.event()["code"] == 0
    return peer


def read_tts(peer):
    audio = bytearray()
    while True:
        opcode, payload = peer.recv()
        if opcode in (0, 2):
            audio.extend(payload)
        else:
            assert opcode == 1 and json.loads(payload)["header"]["name"] == "SynthesisCompleted", payload
            assert len(audio) == 9600, len(audio)
            assert peer.close_code() == 1000
            return audio


def test_admission(port, fixture):
    with ExitStack() as stack, ThreadPoolExecutor(max_workers=16) as pool:
        active = list(pool.map(lambda i: open_asr(port, stack, f"load-{i}"), range(ASR_LIMIT)))
        rejected = Peer(port, asr_path("overload"))
        stack.callback(rejected.close)
        assert rejected.close_code() == 1013
        tasks = [Peer(port, tts_path()) for _ in range(TTS_LIMIT)]
        for peer in tasks:
            stack.callback(peer.close)
        rejected_tts = Peer(port, tts_path())
        stack.callback(rejected_tts.close)
        assert rejected_tts.close_code() == 1013
        value = health(port)
        assert (value["asrActive"], value["ttsActive"]) == (ASR_LIMIT, TTS_LIMIT), value
        active[0].text({"type": "end"})
        assert active[0].event()["final"] == 1
        wait_until(lambda: health(port)["asrActive"] == ASR_LIMIT - 1, "ASR permit not released", 4)
        active[0] = open_asr(port, stack, "replacement")
        fixture.tts_barrier = threading.Barrier(TTS_LIMIT)
        for peer in tasks:
            peer.start_tts()
        list(pool.map(read_tts, tasks))
        fixture.tts_barrier = None
        for peer in active:
            peer.text({"type": "end"})
    wait_until(lambda: no_active(port), "mixed saturation resources not drained", 8)
    print(f"{ASR_LIMIT} ASR held while {TTS_LIMIT} simultaneous TTS completed, independent rejection and recovery",
          flush=True)


def test_slow_readers(port, fixture):
    before = health(port)["events"].get("tts_first_audio", 0)
    with ExitStack() as stack:
        fixture.slow_tasks = 2
        for _ in range(2):
            slow = Peer(port, tts_path(), receive_buffer=4096)
            stack.callback(slow.close)
            slow.start_tts()
        wait_until(lambda: health(port)["events"].get("tts_first_audio", 0) >= before + 2, "slow tasks not started", 5)
        idle = Peer(port, tts_path())
        stack.callback(idle.close)
        start = time.monotonic()
        normal = Peer(port, tts_path())
        stack.callback(normal.close)
        normal.start_tts()
        read_tts(normal)
        assert time.monotonic() - start < 2
        assert idle.close_code() == 1000
        elapsed = time.monotonic() - start
        assert 2.7 < elapsed < 3.7, elapsed
        wait_until(lambda: no_active(port), "slow connections were not reclaimed", 6)
        assert health(port)["events"].get("tts_client_backpressure", 0) >= 2
    print(f"2 blocked readers reclaimed, unrelated 3 s deadline fired at {elapsed:.3f} s", flush=True)


def send_asr_turn(peer):
    pcm = b"".join(struct.pack("<h", 1200 if index % 16 < 8 else -1200) for index in range(160))
    peer.send(2, pcm * 5)
    for _ in range(7):
        peer.send(2, bytes(320 * 5))


def test_stalled_result(port, fixture):
    with ExitStack() as stack:
        fixture.stall_result = True
        peer = open_asr(port, stack, "stalled")
        send_asr_turn(peer)
        assert peer.event()["result"]["slice_type"] == 0
        assert peer.event()["code"] == 4000
        assert peer.close_code() == 1000
        assert health(port)["events"]["asr_result_timeout"] == 1
        wait_until(lambda: no_active(port), "stalled ASR resources not reclaimed", 6)
        fixture.stall_result = False
        peer = open_asr(port, stack, "recovery")
        send_asr_turn(peer)
        assert peer.event()["result"]["slice_type"] == 0
        assert peer.event()["result"]["slice_type"] == 1
        assert peer.event()["result"]["slice_type"] == 2
        peer.text({"type": "end"})
        assert peer.event()["final"] == 1
        assert peer.close_code() == 1000
    wait_until(lambda: no_active(port), "ASR replacement resources not drained", 6)
    print("stalled ASR result timed out, subsequent recognition completed", flush=True)


def expected_pcm(frequency):
    pcm = tone_frame(frequency) * 30
    return b"".join(struct.pack("<h", int(sum(samples) / 3)) for samples in struct.iter_unpack("<hhh", pcm))


def test_tts_reuse(port, fixture):
    before = COUNTS["model_tts_connections"]
    with ExitStack() as stack:
        def synthesize(frequency):
            peer = Peer(port, tts_path())
            stack.callback(peer.close)
            peer.start_tts(f"频率{frequency}。")
            assert read_tts(peer) == expected_pcm(frequency)
            wait_until(lambda: no_active(port), "reusable task did not drain", 5)
            assert health(port)["ttsUpstreamIdle"] == 1

        synthesize(800)
        synthesize(1100)
        assert COUNTS["model_tts_connections"] == before + 1
        assert health(port)["events"]["tts_reused"] == 1

        cancels = COUNTS["model_cancels"]
        fixture.cancel_next_tts = True
        peer = Peer(port, tts_path())
        stack.callback(peer.close)
        peer.start_tts()
        assert peer.recv()[0] == 2
        peer.close()
        wait_until(lambda: no_active(port) and COUNTS["model_cancels"] == cancels + 1,
                   "cancellation did not retire the reused connection", 5)
        assert health(port)["ttsUpstreamIdle"] == 0
        synthesize(1600)
        assert COUNTS["model_tts_connections"] == before + 2
        print("2 completed tasks used 1 connection, delayed output discarded, cancelled lease retired", flush=True)

        idle_at = time.monotonic()
        wait_until(lambda: no_active(port) and health(port)["ttsUpstreamIdle"] == 0,
                   "the idle connection did not expire", 34)
        assert time.monotonic() - idle_at >= 28
        print("idle upstream connection expired and drained", flush=True)


def launch(stack, args, fixture, idle_limit, log_name, extra_env=None):
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    env = {**os.environ, **ENV, "COMPAT_PORT": str(port),
           "COMPAT_REALTIME_URL": f"ws://127.0.0.1:{fixture.server_port}/v1/realtime",
           "COMPAT_ASR_LIMIT": str(ASR_LIMIT), "COMPAT_TTS_LIMIT": str(TTS_LIMIT),
           "COMPAT_TTS_IDLE_LIMIT": str(idle_limit), "COMPAT_ASR_RESULT_TIMEOUT_MS": "1000",
           **(extra_env or {})}
    route_tools["child"](stack, ["java", "-jar", str(args.compat_jar.resolve())],
                         env, args.output_dir / log_name, port)
    return port


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compat-jar", type=Path, default=ROOT / "target/speech-compat-gateway-0.1.0-SNAPSHOT.jar")
    parser.add_argument("--output-dir", type=Path, default=ROOT / ".local/test-concurrency")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    with ExitStack() as stack:
        fixture = stack.enter_context(FixtureServer(("127.0.0.1", 0), Fixture))
        fixture.lock = threading.Lock()
        threading.Thread(target=fixture.serve_forever, daemon=True).start()
        stack.callback(fixture.shutdown)
        with ExitStack() as gateway:
            port = launch(gateway, args, fixture, idle_limit=0, log_name="gateway.log")
            test_admission(port, fixture)
            test_slow_readers(port, fixture)
            test_stalled_result(port, fixture)
        with ExitStack() as gateway:
            port = launch(gateway, args, fixture, idle_limit=1, log_name="tts-reuse.log")
            test_tts_reuse(port, fixture)


if __name__ == "__main__":
    main()
