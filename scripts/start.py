#!/usr/bin/env python3
"""Launch the gateway using only the project's local configuration copy."""

import os
from pathlib import Path
import re
import secrets
import shlex

ROOT = Path(__file__).resolve().parents[1]
LOCAL = ROOT / ".local"
OPENAI_NAMES = {"OPENAI_API_KEY", "OPENAI_LIVE_HTTP_PROXY"}
COMPAT_NAMES = {
    "TENCENT_ASR_URL", "TENCENT_ASR_APP_ID", "TENCENT_ASR_SECRET_ID", "TENCENT_ASR_SECRET_KEY",
    "ALIYUN_NLS_URL", "ALIYUN_NLS_TOKEN_URL", "ALIYUN_NLS_APP_KEY", "ALIYUN_AK_ID",
    "ALIYUN_AK_SECRET", "ALIYUN_NLS_VOICE",
}


def read_settings(path, allowed):
    result = {}
    try:
        for number, line in enumerate(path.read_text().splitlines(), 1):
            match = re.fullmatch(r"\s*(?:export\s+)?([A-Z][A-Z0-9_]*)\s*=\s*(.*?)\s*", line)
            if match and match[1] in allowed:
                try:
                    values = shlex.split(match[2], comments=True)
                except ValueError:
                    raise SystemExit(f"配置第 {number} 行无效：{path}") from None
                if len(values) > 1 or match[1] in result:
                    raise SystemExit(f"配置第 {number} 行无效：{path}")
                result[match[1]] = values[0] if values else ""
    except FileNotFoundError:
        raise SystemExit(f"找不到配置文件：{path}") from None
    return result


def openai_settings():
    path = LOCAL / "openai-live.env"
    settings = read_settings(path, OPENAI_NAMES)
    key = settings.get("OPENAI_API_KEY", "")
    if not key or any(char.isspace() for char in key):
        raise SystemExit(f"OPENAI_API_KEY 无效：{path}")
    return {**dict.fromkeys(OPENAI_NAMES, ""), **settings}


def local_settings():
    path = LOCAL / "compat.env"
    try:
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as file:
            file.write("TENCENT_ASR_URL=ws://127.0.0.1:18120/asr/v2\n")
            file.write("TENCENT_ASR_APP_ID=12345\n")
            file.write("ALIYUN_NLS_URL=ws://127.0.0.1:18120/ws/v1\n")
            file.write("ALIYUN_NLS_TOKEN_URL=http://127.0.0.1:18120/\n")
            file.write("ALIYUN_NLS_VOICE=aixia\n")
            for name in ("TENCENT_ASR_SECRET_ID", "TENCENT_ASR_SECRET_KEY", "ALIYUN_NLS_APP_KEY",
                         "ALIYUN_AK_ID", "ALIYUN_AK_SECRET"):
                file.write(f"{name}={secrets.token_hex(24)}\n")
    except FileExistsError:
        pass
    result = read_settings(path, COMPAT_NAMES)
    if any(not result.get(name) for name in COMPAT_NAMES):
        raise SystemExit(f"兼容配置缺少必填值：{path}")
    return result


def main():
    jar = ROOT / "target" / "speech-compat-gateway-0.1.0-SNAPSHOT.jar"
    if not jar.is_file():
        raise SystemExit(f"请先构建：{ROOT / 'scripts/build.sh'} package")
    env = {**os.environ, **local_settings(), **openai_settings(), "COMPAT_PORT": "18120"}
    env.pop("COMPAT_REALTIME_URL", None)
    os.execvpe("java", ["java", "-jar", str(jar)], env)


if __name__ == "__main__":
    main()
