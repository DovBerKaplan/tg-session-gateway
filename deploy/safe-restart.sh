#!/usr/bin/env bash
# Controlled gateway restart (spec §9.2): drain → restart → verify.
# The check now FAILS (exit != 0) unless every bot returns to live —
# readyz alone used to pass with all sessions in error.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then set -a; . ./.env; set +a; fi
SECRET="${GW_ADMIN_SECRET:?GW_ADMIN_SECRET not set}"
API="http://127.0.0.1:${GW_PORT:-8080}"
TIMEOUT="${GW_RESTART_VERIFY_TIMEOUT:-60}"

echo "1/3 drain (pollers stop after the current batch)..."
for alias in $(curl -s "$API/admin/status" -H "Authorization: Bearer $SECRET" \
               | python3 -c 'import json,sys; print(" ".join(b["alias"] for b in json.load(sys.stdin)["bots"]))'); do
  curl -s -X POST "$API/admin/bots/$alias/drain" -H "Authorization: Bearer $SECRET" >/dev/null
done
sleep 3   # let in-flight getUpdates batches settle

echo "2/3 restart..."
docker compose restart gateway

echo "3/3 verify: readyz AND every bot live..."
deadline=$((SECONDS + TIMEOUT))
while true; do
  if curl -sf "$API/readyz" >/dev/null 2>&1; then
    NOT_LIVE=$(curl -s "$API/admin/status" -H "Authorization: Bearer $SECRET" | \
      python3 -c 'import json,sys
bad=[b["alias"] for b in json.load(sys.stdin)["bots"] if b["state"]!="live"]
print(",".join(bad))' 2>/dev/null || echo "?")
    if [ "$NOT_LIVE" = "" ]; then
      echo "gateway ready, all bots live (offsets restored from disk)"
      exit 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "bots not live after ${TIMEOUT}s: $NOT_LIVE — see docs/runbook.md" >&2
      exit 2
    fi
  elif [ "$SECONDS" -ge "$deadline" ]; then
    echo "readyz not ok after ${TIMEOUT}s" >&2
    exit 1
  fi
  sleep 2
done
