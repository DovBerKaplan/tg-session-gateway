#!/usr/bin/env bash
# Controlled MTProto gateway restart (spec §9.2): drain → restart → resume.
# Session files on the volume survive; auth_key is restored on boot.
set -euo pipefail
cd "$(dirname "$0")/.."

SECRET="${GW_ADMIN_SECRET:?}"
API="http://127.0.0.1:8081"

echo "1/3 drain (flush queues to consumers, stop new updates)..."
for alias in $(curl -s "$API/v1/admin/status" -H "Authorization: Bearer $SECRET" \
               | python3 -c 'import json,sys; print(" ".join(s["alias"] for s in json.load(sys.stdin)["sessions"]))'); do
  curl -s -X POST "$API/v1/admin/sessions/$alias/drain" \
       -H "Authorization: Bearer $SECRET" >/dev/null
done
sleep 3   # let in-flight Pyrogram calls settle

echo "2/3 restart..."
docker compose -f docker-compose.mtgateway.yml restart mtgateway

echo "3/3 wait for readiness..."
for i in $(seq 1 30); do
  curl -sf "$API/readyz" >/dev/null 2>&1 && {
    echo "gateway ready (sessions restored from volume)"
    # Verify sessions came back
    curl -s "$API/v1/admin/status" -H "Authorization: Bearer $SECRET" | \
      python3 -c 'import json,sys; d=json.load(sys.stdin); print("sessions:", ", ".join(s["alias"]+"="+s["state"] for s in d["sessions"]))'
    exit 0
  }
  sleep 1
done
echo "gateway did not become ready in 30s" >&2
exit 1
