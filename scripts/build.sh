#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
source "$root/../mjsip-media-gateway/scripts/toolchain.sh"
exec mvn -B -ntp -f "$root/pom.xml" -Dmaven.repo.local="${MAVEN_REPO_LOCAL:-$root/../mjsip-media-gateway/.m2/repository}" "$@"
