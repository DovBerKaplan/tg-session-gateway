#!/usr/bin/env bash
# Compatibility smoke against a running tggw + fake upstream (or real local companion).
# Exercises the Bot API path shapes libraries use. No real Telegram required when
# GW_UPSTREAM points at the project's fake upstream.
set -euo pipefail
BASE="${TGGW_BASE:-http://127.0.0.1:8080}"
TOKEN="${BOT_TOKEN:?set BOT_TOKEN}"
ADMIN="${GW_ADMIN_SECRET:?set GW_ADMIN_SECRET}"
PREFIX="${GW_PATH_PREFIX:-/tgapi}"
[[ "$PREFIX" == "none" ]] && PREFIX=""

echo "== register =="
curl -sf -X POST "$BASE/admin/bots" \
  -H "Authorization: Bearer $ADMIN" \
  -H "Content-Type: application/json" \
  -d "{\"token\":\"$TOKEN\",\"alias\":\"matrix\"}" | tee /tmp/tggw-matrix-reg.json

echo "== paths: prefixed / bare / test DC / getWebhookInfo =="
curl -sf -X POST "$BASE${PREFIX}/bot${TOKEN}/getMe" -H "Content-Type: application/json" -d '{}' >/dev/null
curl -sf -X POST "$BASE/bot${TOKEN}/getMe" -H "Content-Type: application/json" -d '{}' >/dev/null || true
curl -sf -X POST "$BASE${PREFIX}/bot${TOKEN}/test/getMe" -H "Content-Type: application/json" -d '{}' >/dev/null || true
curl -sf -X POST "$BASE${PREFIX}/bot${TOKEN}/getWebhookInfo" -H "Content-Type: application/json" -d '{}' \
  | grep -q '"url":""'

echo "== getUpdates offset + sendMessage =="
curl -sf -X POST "$BASE${PREFIX}/bot${TOKEN}/getUpdates" \
  -H "Content-Type: application/json" \
  -d '{"timeout":0,"offset":0,"allowed_updates":["message"]}' >/dev/null
curl -sf -X POST "$BASE${PREFIX}/bot${TOKEN}/sendMessage" \
  -H "Content-Type: application/json" \
  -d '{"chat_id":1,"text":"matrix-ok"}' >/dev/null || true

echo "== admin upstream/env =="
curl -sf -H "Authorization: Bearer $ADMIN" "$BASE/admin/upstream" >/dev/null
curl -sf -H "Authorization: Bearer $ADMIN" "$BASE/admin/env" >/dev/null

echo "compat_matrix: OK (curl path). Library clients: see docs/compatibility.md"
