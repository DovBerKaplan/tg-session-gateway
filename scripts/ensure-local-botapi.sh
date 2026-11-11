#!/usr/bin/env bash
# Bring up the internal telegram-bot-api companion for tggw failover / local mode.
# Run on the host or in CI — never mount the Docker socket into tggw.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -z "${TELEGRAM_API_ID:-${GW_API_ID:-}}" || -z "${TELEGRAM_API_HASH:-${GW_API_HASH:-}}" ]]; then
  echo "set TELEGRAM_API_ID/TELEGRAM_API_HASH (or GW_API_ID/GW_API_HASH)" >&2
  exit 1
fi

export GW_UPSTREAM_FALLBACK="${GW_UPSTREAM_FALLBACK:-http://telegram-bot-api:8081}"
export GW_UPSTREAM_FAILOVER="${GW_UPSTREAM_FAILOVER:-auto}"
export GW_UPSTREAM_KIND="${GW_UPSTREAM_KIND:-auto}"
export GW_ENV_DETECT="${GW_ENV_DETECT:-auto}"

docker compose --profile local-botapi up -d telegram-bot-api botapi
echo "companion up; fallback=$GW_UPSTREAM_FALLBACK failover=$GW_UPSTREAM_FAILOVER"
echo "clients still use: http://tggw:8080/tgapi"
