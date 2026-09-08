#!/usr/bin/env bash
# Controlled gateway restart (spec §9.2): drain → restart → resume.
# Consumers see readiness loss only; Telegram ownership resumes from disk.
set -euo pipefail
cd "$(dirname "$0")/.."

SECRET="${GW_ADMIN_SECRET:?}"
API="http://127.0.0.1:8080"

echo "1/3 drain (pollers stop after the current batch, queues flush to consumers)..."
for alias in $(curl -s "$API/admin/status" -H "Authorization: Bearer $SECRET" \
               | python3 -c 'import json,sys; print(" ".join(b["alias"] for b in json.load(sys.stdin)["bots"]))'); do
  curl -s -X POST "$API/admin/bots/$alias/drain" -H "Authorization: Bearer $SECRET" >/dev/null
done
sleep 3   # let in-flight getUpdates batches settle

echo "2/3 restart..."
docker compose restart gateway

echo "3/3 wait for readiness..."
for i in $(seq 1 30); do
  curl -sf "$API/readyz" >/dev/null 2>&1 && { echo "gateway ready (offsets restored from disk)"; exit 0; }
  sleep 1
done
echo "gateway did not become ready in 30s" >&2; exit 1
