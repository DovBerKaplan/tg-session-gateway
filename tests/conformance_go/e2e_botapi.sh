#!/usr/bin/env bash
# E2E against REAL Telegram (Bot API plane) — runs in CI when the
# secrets exist, locally with the same env. Skips loudly otherwise.
#
# Required secrets/env:
#   TG_TEST_BOT_TOKEN  a real bot token (a throwaway test bot)
#   TG_TEST_CHAT_ID    a chat the bot can write to (its own chat with
#                      you, or a private test group)
set -euo pipefail
cd "$(dirname "$0")/../.."

if [ -z "${TG_TEST_BOT_TOKEN:-}" ] || [ -z "${TG_TEST_CHAT_ID:-}" ]; then
    echo "SKIP: TG_TEST_BOT_TOKEN / TG_TEST_CHAT_ID not set — real-Telegram E2E skipped"
    exit 0
fi

TMP=$(mktemp -d)
trap 'kill "${GWPID:-}" 2>/dev/null || true; rm -rf "$TMP"' EXIT

CGO_ENABLED=0 go build -o "$TMP/tggw" ./cmd/tggw

GW_UPSTREAM=https://api.telegram.org \
GW_HOST=127.0.0.1 GW_PORT=18085 GW_DATA_DIR="$TMP/data" \
GW_FERNET_KEY=CfR-eL8QzhZJQb22PBP1zxH7CzJMFK4LwKvFACh1pfw= \
GW_ADMIN_SECRET=e2e-adm GW_APP_SECRET=e2e-app \
GW_ALLOW_EPHEMERAL_DATA=1 \
"$TMP/tggw" botapi & GWPID=$!

for _ in $(seq 1 50); do
    curl -sf "http://127.0.0.1:18085/healthz" >/dev/null && break
    sleep 0.2
done

echo "== register the real bot"
curl -sf -X POST http://127.0.0.1:18085/admin/bots \
    -H "Authorization: Bearer e2e-adm" -H "Content-Type: application/json" \
    -d "{\"token\":\"$TG_TEST_BOT_TOKEN\",\"alias\":\"e2e\"}"
echo

echo "== send through the gateway (must reach real Telegram)"
RESP=$(curl -sf -X POST "http://127.0.0.1:18085/tgapi/bot$TG_TEST_BOT_TOKEN/sendMessage" \
    -H "Authorization: Bearer e2e-app" -H "Content-Type: application/json" \
    -d "{\"chat_id\":$TG_TEST_CHAT_ID,\"text\":\"tggw e2e $(date +%s)\"}")
echo "$RESP"
echo "$RESP" | grep -q '"message_id"' || { echo "FAIL: no message_id in send response"; exit 1; }

echo "== drain the durable queue (one real update cycle)"
curl -sf -X POST "http://127.0.0.1:18085/tgapi/bot$TG_TEST_BOT_TOKEN/getUpdates" \
    -H "Authorization: Bearer e2e-app" -H "Content-Type: application/json" \
    -d '{"timeout":5,"offset":0}' | head -c 400
echo

echo "== SIGKILL + restart: acked state survives"
kill -9 $GWPID; wait $GWPID 2>/dev/null || true
GW_UPSTREAM=https://api.telegram.org \
GW_HOST=127.0.0.1 GW_PORT=18085 GW_DATA_DIR="$TMP/data" \
GW_FERNET_KEY=CfR-eL8QzhZJQb22PBP1zxH7CzJMFK4LwKvFACh1pfw= \
GW_ADMIN_SECRET=e2e-adm GW_APP_SECRET=e2e-app \
GW_ALLOW_EPHEMERAL_DATA=1 \
"$TMP/tggw" botapi & GWPID=$!
for _ in $(seq 1 50); do
    curl -sf "http://127.0.0.1:18085/healthz" >/dev/null && break
    sleep 0.2
done
curl -sf "http://127.0.0.1:18085/admin/status" -H "Authorization: Bearer e2e-adm" | grep -q '"live"' \
    || { echo "FAIL: bot not live after restart"; exit 1; }

echo "E2E botapi: PASS"
