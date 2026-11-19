#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
source "$root/../mjsip-media-gateway/scripts/toolchain.sh"
exec python3 -B "$root/scripts/start.py"
