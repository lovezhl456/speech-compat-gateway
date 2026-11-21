#!/usr/bin/env python3
"""Real SDK/SIP/RTP regression against local OpenAI protocol fixtures."""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from http.server import ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET
import re
import math
import socket
import struct
import runpy
from threading import Thread
import time
from urllib.error import HTTPError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
MEDIA = ROOT.parent / "mjsip-media-gateway"
SITE = ROOT.parent / "mjsip-call-test"
import sys
sys.path[:0] = [str(MEDIA / "tests/media"), str(SITE)]

from live_client_fixture import LiveClientFixture
from phone import Phone, sessions, wait_until
from provider_fixture import tone_frame
from streaming_fixture import count, COUNTS

route_tools = runpy.run_path(str(SITE / "compat-routes.py"))
settings = runpy.run_path(str(ROOT / "scripts/start.py"))
echo = runpy.run_path(str(SITE / "business-echo.py"))
KEY = "fixture-openai-key"
LONG_TTS_TAIL = 2450


class OpenAiFixture(LiveClientFixture):
    def do_GET(self):
        if self.path == "/test/status":
            return self.body(dict(COUNTS))
        if self.path == "/v1/realtime?intent=transcription":
            assert self.headers.get("Authorization") == "Bearer " + KEY
            return self.upgrade(self.recognize_model, "model_asr_active")
        if self.path == "/v1/realtime?model=gpt-realtime-2.1-mini":
            assert self.headers.get("Authorization") == "Bearer " + KEY
            return self.upgrade(self.synthesize_model, "model_tts_active")
        return super().do_GET()

    def read_event(self, kind=None):
        opcode, data = self.frame()
        event = json.loads(data)
        assert opcode == 1
        if kind:
            assert event["type"] == kind, event["type"]
        return event

    def recognize_model(self):
        audio = self.read_event("session.update")["session"]["audio"]["input"]
        assert audio["format"] == {"type": "audio/pcm", "rate": 24000}
        assert audio["transcription"]["model"] == "gpt-live-transcribe" and audio["turn_detection"] is None
        connection = count("model_asr_started")
        if getattr(self.server, "reject_next_asr", False):
            self.server.reject_next_asr = False
            self.event({"type": "error", "error": {"code": "fixture_rejected"}})
            return
        self.event({"type": "session.updated"})
        recorded = bytearray()
        index = 0
        while True:
            event = self.read_event()
            if event["type"] == "input_audio_buffer.append":
                if getattr(self.server, "break_next_asr", False):
                    self.server.break_next_asr = False
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                recorded.extend(base64.b64decode(event["audio"], validate=True))
            elif event["type"] == "input_audio_buffer.clear":
                recorded.clear()
            else:
                assert event["type"] == "input_audio_buffer.commit"
                assert len(recorded) >= 4800 and len(recorded) % 960 == 0, len(recorded)
                samples = tuple(value for (value,) in struct.iter_unpack("<h", recorded[:960]))
                recorded.clear()
                index += 1
                count("model_commits")
                item = f"item-{connection}-{index}"
                self.event({"type": "input_audio_buffer.committed", "item_id": item})
                self.event({"type": "conversation.item.input_audio_transcription.delta",
                            "item_id": item, "delta": "九零"})
                result = {"type": "conversation.item.input_audio_transcription.completed",
                          "item_id": item, "transcript": f"九零七零四二。测试频率{650 + (connection % 8) * 150}。"}
                if getattr(self.server, "individual", False):
                    crossings = sum((a < 0) != (b < 0) for a, b in zip(samples, samples[1:]))
                    source = round(crossings * 25 / 100)
                    result["transcript"] = f"频率{650 + (source - 5) * 150}。"
                elif getattr(self.server, "order_recognition", False):
                    result["transcript"] = "九零七零四二"
                elif getattr(self.server, "single_response", False):
                    result["transcript"] = f"频率{650 + (connection % 8) * 150}。"
                self.event(result)
                self.event({"type": "input_audio_buffer.committed", "item_id": item})
                self.event(result)

    def synthesize_model(self):
        self.connection.settimeout(45)
        connection = count("model_tts_connections")
        session = self.read_event("session.update")["session"]
        assert session["output_modalities"] == ["audio"]
        assert session["audio"]["output"]["format"] == {"type": "audio/pcm", "rate": 24000}
        self.event({"type": "session.updated"})
        previous = None
        task = 0
        while True:
            command = self.read_event()
            if command["type"] == "response.cancel":
                count("model_cancels")
                return
            assert command["type"] == "response.create", command["type"]
            task += 1
            result = {"id": f"response-{connection}-{task}", "metadata": command["response"].get("metadata", {})}
            if not self.render_tts(command, result, previous):
                return
            previous = result

    def render_tts(self, command, result, previous):
        response = command["response"]
        assert response["output_modalities"] == ["audio"] and response["conversation"] == "none"
        assert response["input"] == []
        text = response["instructions"].split("\n\n", 1)[-1]
        if getattr(self.server, "fail_next_tts", False):
            self.server.fail_next_tts = False
            self.event({"type": "error", "error": {"code": "rate_limit_exceeded"}})
            count("model_rejected")
            return False
        match = re.search(r"([0-9]{3,4})[。]?$", text)
        frequency = int(match[1]) if match else (1400 if "查到了" in text else 500)
        self.event({"type": "response.created", "response": result})
        if previous is not None:
            # Deliver old events while a new request owns the same connection.
            self.event({"type": "response.created", "response": previous})
            self.event({"type": "response.output_audio.delta", "response_id": previous["id"],
                        "delta": base64.b64encode(tone_frame(2100)).decode()})
            self.event({"type": "response.done", "response": {**previous, "status": "completed"}})
        if getattr(self.server, "cancel_next_tts", False):
            self.server.cancel_next_tts = False
            self.event({"type": "response.output_audio.delta", "response_id": result["id"],
                        "delta": base64.b64encode(tone_frame(frequency) * 12).decode()})
            cancelled = self.read_event("response.cancel")
            assert cancelled.get("response_id") == result["id"]
            count("model_cancels")
            return False
        if getattr(self.server, "extend_next_tts", False):
            self.server.extend_next_tts = False
            for frame in range(125):
                marker = LONG_TTS_TAIL if frame == 124 else 1650
                pcm = tone_frame(frequency if frame < 115 else marker)
                self.event({"type": "response.output_audio.delta", "response_id": result["id"],
                            "delta": base64.b64encode(pcm).decode()})
                time.sleep(0.1)
            self.event({"type": "response.done", "response": {**result, "status": "completed"}})
            count("model_tts_finished")
            return True
        frames = 30
        pcm = tone_frame(frequency) * frames
        for offset in range(0, len(pcm), 2999):
            self.event({"type": "response.output_audio.delta", "response_id": result["id"],
                        "delta": base64.b64encode(pcm[offset:offset + 2999]).decode()})
        self.event({"type": "response.done", "response": {**result, "status": "completed"}})
        count("model_tts_finished")
        return True


def health(port):
    with urlopen(f"http://127.0.0.1:{port}/healthz", timeout=2) as response:
        return json.load(response)


def no_active(port):
    value = health(port)
    return (value["asrActive"] == value["ttsActive"] == 0
            and COUNTS["model_asr_active"] == COUNTS["live_active"] == 0
            and COUNTS["model_tts_active"] == value.get("ttsUpstreamIdle", 0))


def test_suite(switch_port, fixture, by_route, port, faults_only=False):
    def drained():
        return no_active(port)

    def dial(route, codec="PCMU", dtmf=False):
        target = switch_port or by_route[route][1]
        return Phone(("127.0.0.1", target), route, codec, dtmf=dtmf)

    def heard(phone, duration, frequency=None):
        tones = phone.hear(duration, frequency, 4 if frequency else None)
        assert tones and (frequency is None or any(abs(tone - frequency) <= 100 for tone in tones)), tones

    token_calls = health(port)["events"].get("token_issued", 0)
    with urlopen(f"http://127.0.0.1:{fixture.server_port}/test/status") as response:
        assert response.status == 200
    try:
        urlopen(f"http://127.0.0.1:{port}/?Action=CreateToken&Signature=invalid")
        raise AssertionError("unsigned request received a token")
    except HTTPError as error:
        assert error.code == 403 and health(port)["events"].get("token_issued", 0) == token_calls
    print("unsigned CreateToken rejected", flush=True)

    if not switch_port:
        fixture.reject_next_asr = True
        with Phone(("127.0.0.1", by_route["9910"][1]), "9910", answer_status=503):
            pass
        wait_until(drained, "rejected ASR was not reclaimed", 4)
        print("9910 initial ASR rejection maps to SIP 503", flush=True)

    fixture.single_response = True
    fixture.break_next_asr = True
    before = health(port)["events"].get("asr_ready", 0)
    with dial("9910") as call:
        call.speak()
        wait_until(lambda: health(port)["events"].get("asr_ready", 0) >= before + 2,
                   "ASR did not reconnect", 6)
        time.sleep(0.9)
        call.speak()
        heard(call, 4)
    wait_until(drained, "reopened ASR was not reclaimed", 4)
    print("9910 ASR disconnect, reconnect and reply recovery", flush=True)
    if faults_only:
        return
    for codec in ("PCMU", "PCMA"):
        before = COUNTS["model_commits"]
        with dial("9910", codec) as call:
            time.sleep(0.9)
            assert COUNTS["model_commits"] == before
            call.speak()
            heard(call, 3)
            call.speak()
            heard(call, 3)
        wait_until(drained, "sessions remained open", 4)
        assert COUNTS["model_commits"] == before + 2, (codec, before, COUNTS["model_commits"])
        print(f"9910 {codec}: silence, ordered duplicate suppression and two responses", flush=True)

    fixture.fail_next_tts = True
    with dial("9910") as call:
        call.speak()
        wait_until(lambda: COUNTS["model_rejected"] > 0, "missing model rejection", 4)
        time.sleep(0.8)
        call.speak()
        heard(call, 3)
    wait_until(drained, "failed synthesis did not release connection", 4)
    print("9910 provider limit and next-turn recovery", flush=True)

    fixture.cancel_next_tts = True
    before = health(port)["events"].get("tts_first_audio", 0)
    with dial("9910") as call:
        call.speak()
        wait_until(lambda: health(port)["events"].get("tts_first_audio", 0) > before,
                   "long reply did not start", 4)
        time.sleep(0.18)
        call.speak()
        wait_until(lambda: COUNTS["model_cancels"] >= 1, "barge-in did not cancel upstream synthesis", 4)
        heard(call, 3)
    wait_until(drained, "barge-in resources remained open", 4)
    print("9910 barge-in cancelled TTS and released resources", flush=True)

    fixture.single_response = False
    fixture.order_recognition = True
    with dial("9914", dtmf=True) as call:
        heard(call, 4, 500)
        time.sleep(2.6)
        call.speak()
        heard(call, 4, 1400)
        time.sleep(1.8)
        call.dial_digits("31*907042#")
        heard(call, 5, 1400)
    fixture.order_recognition = False
    wait_until(drained, "order resources remained open", 4)
    print("9914 speech and DTMF hit with clear and submit", flush=True)

    with dial("9913") as call:
        call.speak()
        heard(call, 4, 950)
    wait_until(drained, "live resources remained open", 5)
    print("9913 live delegation through new SDK TTS", flush=True)

    fixture.individual = True
    def batch(number):
        source = 500 + (number % 8) * 100
        expected = 650 + (number % 8) * 150
        with dial("9910", "PCMU" if number % 2 == 0 else "PCMA") as call:
            levels = (0x9F, 0x1F) if call.codec == "PCMU" else (0xAA, 0x2A)
            payload = bytes(levels[math.sin(i * 2 * math.pi * source / 8000) < 0] for i in range(160))
            last_voice = None

            def mark_sent(instant, audio):
                nonlocal last_voice
                if audio == payload:
                    last_voice = instant

            call.on_media_sent = mark_sent
            call.pending.extend((call.payload_type, payload, 0) for _ in range(23))
            tones = call.hear(6, expected, 6)
            confirmed = time.monotonic()
            assert len(tones) >= 6 and all(abs(tone - expected) <= 70 for tone in tones), (source, tones)
            assert last_voice is not None
            return round((confirmed - last_voice) * 1000)

    with ThreadPoolExecutor(max_workers=8) as pool:
        latencies = sorted(pool.map(batch, range(24)))
    print(json.dumps({"tail_to_sixth_matched_rtp_ms": {
        "n": len(latencies), "p95": latencies[math.ceil(len(latencies) * 0.95) - 1],
    }}), flush=True)
    wait_until(drained, "concurrent resources remained open", 5)
    for _, _, http, _ in by_route.values():
        assert not sessions(f"http://127.0.0.1:{http}")
    print("24 calls / peak requested 8, resources drained", flush=True)
    print(json.dumps(health(port), sort_keys=True), flush=True)


def start_switch(stack, routes, ports, run):
    browser, telephone, event = ports
    lab = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="switch.", dir=run)))
    for name in ("conf", "log", "run", "db", "storage", "lib"):
        (lab / name).mkdir()
    fs = Path(shutil.which("freeswitch"))
    module_dir = fs.parent / "../lib/freeswitch/mod"
    if sys.platform == "darwin":
        (lab / "lib/libfreeswitch.dylib").symlink_to((module_dir / "../../libfreeswitch.dylib").resolve())
    password = secrets.token_hex(24)
    text = (SITE / "freeswitch.xml").read_text().format(
        business_port=5070, event_port=event, password=password, media_ip="127.0.0.1")
    xml = ET.fromstring(text)
    for name, value in (("max-sessions", "32"), ("rtp-start-port", "37000"), ("rtp-end-port", "37159")):
        xml.find(f".//configuration[@name='switch.conf']/settings/param[@name='{name}']").set("value", value)
    browser_settings = xml.find(".//profile[@name='browser']/settings")
    browser_settings.find("param[@name='sip-port']").set("value", str(browser))
    # This fixture calls the bridge over SIP/UDP and does not need a WebSocket listener.
    browser_settings.remove(browser_settings.find("param[@name='ws-binding']"))
    xml.find(".//profile[@name='telephone']/settings/param[@name='sip-port']").set("value", str(telephone))
    context = xml.find(".//context[@name='lab']")
    for node in list(context):
        condition = node.find("condition")
        extension = condition.get("expression").strip("^$")
        if extension not in routes:
            context.remove(node)
        else:
            action = condition.find("action[@application='bridge']")
            action.set("data", f"sofia/telephone/speech-gateway@127.0.0.1:{routes[extension][1]}")
    ET.ElementTree(xml).write(lab / "conf/freeswitch.xml", encoding="utf-8")
    console = stack.enter_context((run / "switch.log").open("w"))
    process = subprocess.Popen([str(fs), "-nf", "-c", "-nonat", "-np", "-base", str(lab),
                                "-conf", str(lab / "conf"), "-log", str(lab / "log"),
                                "-run", str(lab / "run"), "-db", str(lab / "db"),
                                "-storage", str(lab / "storage"), "-mod", str(module_dir)],
                               stdin=subprocess.PIPE, stdout=console, stderr=subprocess.STDOUT)
    stack.callback(route_tools["stop"], process)
    def ready():
        try:
            status = subprocess.run(["fs_cli", "-H", "127.0.0.1", "-P", str(event), "-p", password,
                                     "-x", "sofia status"], capture_output=True, text=True, timeout=2)
        except subprocess.TimeoutExpired:
            return False
        return all(f"127.0.0.1:{port}" in status.stdout for port in (browser, telephone))
    wait_until(ready, "isolated FreeSWITCH failed to start", 30)
    return browser


def test_long_tts(switch_port, fixture, routes, compat_port):
    fixture.single_response = True
    for extension in ("9910", "9913"):
        completed = health(compat_port)["events"].get("tts_completed", 0)
        fixture.extend_next_tts = True
        target = switch_port or routes[extension][1]
        with Phone(("127.0.0.1", target), extension) as call:
            call.speak()
            tones = call.hear(19, frequency=LONG_TTS_TAIL, frames=1)
            tail = sum(abs(tone - 1650) <= 100 for tone in tones)
            assert (len(tones) >= 90 and tail >= 8 and abs(tones[-1] - LONG_TTS_TAIL) <= 100), (
                extension, len(tones), tail, tones[-5:])
            wait_until(lambda: health(compat_port)["events"].get("tts_completed", 0) == completed + 1,
                       "long task did not complete normally", 3)
        wait_until(lambda: no_active(compat_port), "long call left active resources", 5)
        assert not sessions(f"http://127.0.0.1:{routes[extension][2]}")
        print(f"{extension} long TTS: {len(tones)} voiced RTP frames observed through final tone, normal completion",
              flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through-switch", action="store_true")
    parser.add_argument("--ephemeral", action="store_true", help="use separate ports for direct SIP tests")
    parser.add_argument("--faults-only", action="store_true")
    parser.add_argument("--long-tts", action="store_true", help="also verify 12.5-second continuous synthesis")
    parser.add_argument("--serve", action="store_true", help="hold fixture-backed routes for UI checks")
    parser.add_argument("--compat-jar", type=Path, default=ROOT / "target/speech-compat-gateway-0.1.0-SNAPSHOT.jar")
    parser.add_argument("--media-jar", type=Path, default=MEDIA / "target/mjsip-media-gateway-0.1.0-SNAPSHOT.jar")
    parser.add_argument("--output-dir", type=Path, help="save logs separately from the default test run")
    args = parser.parse_args()
    routes = route_tools["ROUTES"]
    compat_port = 18120
    switch_ports = None
    if args.ephemeral:
        if args.serve:
            parser.error("--serve uses the ports in the public dial plan")
        with ExitStack() as reservations:
            def reserve(kind):
                sock = reservations.enter_context(socket.socket(socket.AF_INET, kind))
                sock.bind(("127.0.0.1", 0))
                return sock.getsockname()[1]
            compat_port = reserve(socket.SOCK_STREAM)
            routes = {route: (mode, reserve(socket.SOCK_DGRAM), reserve(socket.SOCK_STREAM), (43500 if args.through_switch else 43000) + index * 100)
                      for index, (route, (mode, _, _, _)) in enumerate(routes.items())}
            if args.through_switch:
                switch_ports = (reserve(socket.SOCK_DGRAM), reserve(socket.SOCK_DGRAM), reserve(socket.SOCK_STREAM))
    prep = route_tools["check_ports"]
    prep([compat_port] + [port for _, _, port, _ in routes.values()],
         [port for _, sip, _, rtp in routes.values() for port in (sip, *range(rtp, rtp + 40))])
    local = {name: value.replace(":18120", f":{compat_port}") for name, value in settings["local_settings"]().items()}
    run = args.output_dir or ROOT / ".local" / (
        "test-browser" if args.serve else "test-bridge" if args.through_switch else "test-direct"
    )
    run.mkdir(parents=True, exist_ok=True)
    with ExitStack() as stack:
        fixture = stack.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), OpenAiFixture))
        Thread(target=fixture.serve_forever, daemon=True).start()
        stack.callback(fixture.shutdown)
        reply_log = stack.enter_context((run / "reply.log").open("w"))
        server = stack.enter_context(echo["EchoServer"](reply_log))
        Thread(target=server.serve_forever, daemon=True).start()
        stack.callback(server.shutdown)
        child = route_tools["child"]
        base = {**os.environ, **local, "OPENAI_LIVE_HTTP_PROXY": "", "SPEECH_API_KEY": ""}
        child(stack, ["java", "-jar", str(args.compat_jar.resolve())],
              {**base, "OPENAI_API_KEY": KEY, "COMPAT_PORT": str(compat_port),
               "COMPAT_REALTIME_URL": f"ws://127.0.0.1:{fixture.server_port}/v1/realtime"},
              run / "compat.log", compat_port)
        for route, (mode, sip, http, rtp) in routes.items():
            env = {**base, "OPENAI_API_KEY": "", "SPEECH_MODE": mode, "SIP_PORT": str(sip),
                   "SIP_BIND_IP": "127.0.0.1", "SIP_ADVERTISE_IP": "127.0.0.1", "SIP_ANSWER_DELAY_MS": "0",
                   "HTTP_BIND_IP": "127.0.0.1", "HTTP_PORT": str(http), "RTP_BIND_IP": "127.0.0.1",
                   "RTP_ADVERTISE_IP": "127.0.0.1", "RTP_MIN_PORT": str(rtp), "RTP_MAX_PORT": str(rtp + 39),
                   "RTP_IDLE_KEEPALIVE": "true" if route != "9910" else "false",
                   "OPENAI_LIVE_URL": f"ws://127.0.0.1:{fixture.server_port}/v1/live/sessions",
                   "SPEECH_API_BASE": f"http://127.0.0.1:{fixture.server_port}/v1",
                   "SPEECH_REPLY_RPC_BASE": f"http://127.0.0.1:{server.server_port}",
                   "SPEECH_REPLY_RPC_TOKEN": ""}
            if route == "9913":
                env["ALIYUN_NLS_SYNTHESIS_TIMEOUT_MS"] = str(settings["client_tts_timeout_ms"]())
            child(stack, ["java", "-jar", str(args.media_jar.resolve())], env, run / (route + ".log"), http)
        print(f"fixture routes ready; logs {run}", flush=True)
        if args.serve:
            fixture.order_recognition = True
            for signum in (route_tools["signal"].SIGTERM, route_tools["signal"].SIGINT):
                route_tools["signal"].signal(signum, route_tools["command_exit"])
            while True:
                time.sleep(1)
        else:
            switch_port = start_switch(stack, routes, switch_ports, run) if switch_ports else (15060 if args.through_switch else None)
            test_suite(switch_port, fixture, routes, compat_port, args.faults_only)
            if args.long_tts:
                test_long_tts(switch_port, fixture, routes, compat_port)


if __name__ == "__main__":
    main()
