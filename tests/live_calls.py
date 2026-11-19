#!/usr/bin/env python3
"""Capture caller-side RTP for matched 9900/9910 latency measurements."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
from pathlib import Path
import socket
import struct
import subprocess
import sys
from threading import Event, Thread
from time import monotonic, sleep
import wave
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parent / "mjsip-media-gateway/tests/media"))
from phone import Phone

SAMPLES = ROOT.parent / "mjsip-call-test/site/samples"
FRAME = 160
INTERVAL = 0.02
QUIET = 0.28


def mulaw(code):
    value = ~code & 255
    magnitude = (((value & 15) << 3) + 132) << ((value >> 4) & 7)
    return 132 - magnitude if value & 128 else magnitude - 132


LINEAR = tuple(mulaw(code) for code in range(256))
QUIET_FRAME = bytes([255]) * FRAME


def voiced(frame):
    return sum(LINEAR[value] ** 2 for value in frame) >= FRAME * 350 ** 2


def audio(path):
    data = subprocess.check_output(["ffmpeg", "-loglevel", "error", "-i", str(path),
                                    "-ac", "1", "-ar", "8000", "-f", "mulaw", "pipe:1"])
    frames = [data[start:start + FRAME].ljust(FRAME, b"\xff")
              for start in range(0, len(data), FRAME)]
    active = [i for i, frame in enumerate(frames) if voiced(frame)]
    if not active:
        raise ValueError(f"No speech in {path.name}")
    return frames[max(0, active[0] - 2):active[-1] + 2]


class Tap:
    def __init__(self, rtp):
        self.rtp = rtp
        self.sent = []
        self.received = []

    def __getattr__(self, name):
        return getattr(self.rtp, name)

    def sendto(self, packet, address):
        instant = monotonic()
        result = self.rtp.sendto(packet, address)
        if len(packet) == 172 and packet[1] & 127 == 0:
            self.sent.append((instant, packet[12:]))
        return result


class RecordedPhone(Phone):
    def connect(self):
        self.rtp = Tap(self.rtp)
        super().connect()
        self.stop_recording = Event()
        self.rtp.settimeout(0.1)
        self.reader = Thread(target=self.capture, daemon=True)
        self.reader.start()

    def capture(self):
        while not self.stop_recording.is_set():
            try:
                packet, source = self.rtp.recvfrom(2048)
            except socket.timeout:
                continue
            except OSError:
                return
            if source == self.remote_rtp and len(packet) == 172 and packet[1] & 127 == 0:
                self.rtp.received.append((monotonic(), packet[12:]))

    def __exit__(self, *args):
        if hasattr(self, "reader"):
            self.stop_recording.set()
            self.reader.join(0.3)
        super().__exit__(*args)

    def inject(self, frames):
        start = monotonic()
        self.pending.extend((self.payload_type, frame, 0) for frame in frames)
        return start

    def downlink(self, since):
        return [instant for instant, data in self.rtp.received if instant >= since and voiced(data)]

    def uplink(self, since):
        return [instant for instant, data in self.rtp.sent if instant >= since and voiced(data)]


def record(path, tap):
    frames = [tap.sent, tap.received]
    start = min(pair[0][0] for pair in frames if pair)
    end = max(pair[-1][0] for pair in frames if pair)
    windows = [{round((instant - start) / INTERVAL): payload for instant, payload in channel}
               for channel in frames]
    with wave.open(str(path), "wb") as output:
        output.setparams((2, 2, 8000, 0, "NONE", "not compressed"))
        for index in range(round((end - start) / INTERVAL) + 1):
            left, right = (channel.get(index, QUIET_FRAME) for channel in windows)
            audio_bytes = b"".join(struct.pack("<hh", LINEAR[l], LINEAR[r]) for l, r in zip(left, right))
            output.writeframesraw(audio_bytes)


def wait_for(predicate, timeout):
    until = monotonic() + timeout
    while monotonic() < until:
        value = predicate()
        if value:
            return value
        sleep(0.02)
    return None


def order_idle(route, split_gateway):
    port = (18092 if route == "9904" else 18114) if split_gateway else 8088
    tags = "" if split_gateway else f'{{called_number="{route}"}}'
    idle_metric = f"speech_gateway_streaming_replies{tags} 0.0".encode()
    with urlopen(f"http://127.0.0.1:{port}/actuator/prometheus", timeout=2) as response:
        return idle_metric in response.read().splitlines()


def measure(route, number, directory, first, second=None, digits=None, split_gateway=False):
    path = directory / f"{route}-{number:02d}.wav"
    result = {"route": route, "round": number, "recording": path.name, "status": "error"}
    phone = RecordedPhone(("127.0.0.1", 15060), route, dtmf=bool(digits), response_timeout=12)
    try:
        with phone:
            if route in ("9904", "9914"):
                initial = wait_for(lambda: phone.downlink(0), 8)
                if not initial:
                    raise TimeoutError("welcome")
                if not wait_for(lambda: monotonic() - phone.downlink(0)[-1] > 0.5 and order_idle(route, split_gateway), 35):
                    raise TimeoutError("welcome did not finish")
            if digits:
                start = monotonic()
                phone.dial_digits(digits)
                reply = wait_for(lambda: phone.downlink(start), 20)
                if not reply:
                    raise TimeoutError("DTMF reply")
                if not wait_for(lambda: monotonic() - phone.downlink(start)[-1] > 0.5 and order_idle(route, split_gateway), 30):
                    raise TimeoutError("order reply did not finish")
                result["status"] = "ok"
                return result
            start = phone.inject(first)
            sleep(len(first) * INTERVAL + 0.15)
            sent = phone.uplink(start)
            if not sent:
                raise TimeoutError("uplink")
            onset = sent[0]
            speech_end = sent[-1] + INTERVAL
            reply = wait_for(lambda: phone.downlink(speech_end), 12)
            if not reply:
                raise TimeoutError("first response")
            result["first_ms"] = round((reply[0] - speech_end) * 1000)
            result["start_to_first_ms"] = round((reply[0] - onset) * 1000)
            if second is None:
                finished = wait_for(lambda: monotonic() - phone.downlink(speech_end)[-1] > 1.0
                                    and (route not in ("9904", "9914") or order_idle(route, split_gateway)), 30)
                if not finished:
                    raise TimeoutError("reply did not finish")
            else:
                if not wait_for(lambda: len(phone.downlink(speech_end)) >= 14, 4):
                    raise TimeoutError("long response did not sustain audio")
                interrupted = phone.inject(second)
                sleep(len(second) * INTERVAL + 0.15)
                incoming = phone.uplink(interrupted)
                if not incoming:
                    raise TimeoutError("interruption uplink")
                interrupt_at = incoming[0]
                next_earliest = incoming[-1] + INTERVAL + 0.35
                latest_audio = wait_for(lambda: phone.downlink(next_earliest), 12)
                if not latest_audio:
                    raise TimeoutError("next response")
                candidates = phone.downlink(speech_end)
                old_end = None
                for before, after in zip(candidates, candidates[1:]):
                    if after >= interrupt_at and after - before >= QUIET:
                        old_end = before + INTERVAL
                        break
                if old_end is None or old_end < interrupt_at - 0.10 or old_end >= next_earliest:
                    raise TimeoutError("old/new boundary ambiguous")
                result["barge_ms"] = max(0, round((old_end - interrupt_at) * 1000))
                result["next_first_ms"] = round((latest_audio[0] - incoming[-1] - INTERVAL) * 1000)
                wait_for(lambda: monotonic() - phone.downlink(next_earliest)[-1] > 1.0, 15)
            result["status"] = "ok"
    except (AssertionError, OSError, TimeoutError) as error:
        result["reason"] = str(error)
    finally:
        if hasattr(phone.rtp, "sent") and phone.rtp.sent:
            record(path, phone.rtp)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--routes", nargs="+", default=["9900", "9910"])
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--parallel", type=int, default=1)
    parser.add_argument("--sample", default="order-4")
    parser.add_argument("--file", type=Path)
    parser.add_argument("--barge", action="store_true")
    parser.add_argument("--digits")
    parser.add_argument("--split-gateway", action="store_true", help="Use the legacy per-extension HTTP ports.")
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=True)
    first = audio(args.file or SAMPLES / f"{args.sample}.wav") if not args.digits else []
    second = audio(SAMPLES / "order-3.wav") if args.barge else None
    jobs = [(route, index + 1) for index in range(args.rounds) for route in args.routes]

    def run(job):
        route, number = job
        result = measure(route, number, args.directory, first, second, args.digits, args.split_gateway)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return result

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        results = list(pool.map(run, jobs))
    for route in args.routes:
        rows = [result for result in results if result["route"] == route]
        stats = {"route": route, "total": len(rows), "success": sum(row["status"] == "ok" for row in rows)}
        for metric in ("first_ms", "barge_ms"):
            values = sorted(row[metric] for row in rows if metric in row)
            if values:
                stats[metric] = {"n": len(values), "p95": values[math.ceil(len(values) * 0.95) - 1]}
        print(json.dumps(stats), flush=True)
    if any(row["status"] != "ok" for row in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
