#!/usr/bin/env bash
# Controlled MTProto gateway restart (spec §9.2): drain → restart → verify.
# Asserts every session returns to live; non-zero exit otherwise.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then set -a; . ./.env; set +a; fi
SECRET="${GW_ADMIN_SECRET:?GW_ADMIN_SECRET not set}"
API="http://127.0.0.1:${GW_PORT:-8081}"
TIMEOUT="${GW_RESTART_VERIFY_TIMEOUT:-60}"

echo "1/3 drain (flush queues to consumers, stop new updates)..."
for alias in $(curl -s "$API/v1/admin/status" -H "Authorization: Bearer $SECRET" \
               | python3 -c 'import json,sys; print(" ".join(s["alias"] for s in json.load(sys.stdin)["sessions"]))'); do
  curl -s -X POST "$API/v1/admin/sessions/$alias/drain" \
       -H "Authorization: Bearer $SECRET" >/dev/null
done
sleep 3   # let in-flight Pyrogram calls settle

echo "2/3 restart..."
docker compose -f docker-compose.mtgateway.yml restart mtgateway

echo "3/3 verify: readyz AND every session live..."
deadline=$((SECONDS + TIMEOUT))
while true; do
  if curl -sf "$API/readyz" >/dev/null 2>&1; then
    NOT_LIVE=$(curl -s "$API/v1/admin/status" -H "Authorization: Bearer $SECRET" | \
      python3 -c 'import json,sys
bad=[s["alias"] for s in json.load(sys.stdin)["sessions"] if s["state"]!="live"]
print(",".join(bad))' 2>/dev/null || echo "?")
    if [ "$NOT_LIVE" = "" ]; then
      echo "gateway ready, all sessions live (restored from volume)"
      exit 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "sessions not live after ${TIMEOUT}s: $NOT_LIVE — see docs/runbook.md" >&2
      exit 2
    fi
  elif [ "$SECONDS" -ge "$deadline" ]; then
    echo "readyz not ok after ${TIMEOUT}s (safe mode? ack via /v1/admin/safe-mode/ack)" >&2
    exit 1
  fi
  sleep 2
done
