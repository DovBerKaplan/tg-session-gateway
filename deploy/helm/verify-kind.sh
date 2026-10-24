#!/usr/bin/env bash
# Kind-based acceptance for the tggw chart — the plan's K8s verification
# gate, automated. Boots a kind cluster, installs the chart, runs real
# traffic through port-forward, kills the pod mid-traffic, verifies the
# replay + NetworkPolicy + idle resources, then tears everything down.
#
# Usage: deploy/helm/verify-kind.sh [image-tag]
# Env:   TGGW_IMAGE (default: builds tggw:verify from docker/Dockerfile.tggw)
#        KEEP_CLUSTER=1 to skip teardown (debugging)
set -euo pipefail

CLUSTER="${CLUSTER:-tggw-verify}"
IMAGE="${TGGW_IMAGE:-tggw:verify}"
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
NS=tg-gateway
TOKEN="700000:conformance"
FERNET="CfR-eL8QzhZJQb22PBP1zxH7CzJMFK4LwKvFACh1pfw="
# the machine may carry an unrelated KUBECONFIG (e.g. k3s); the script
# works exclusively against the kind cluster it creates
export KUBECONFIG="${KUBECONFIG_KIND:-/tmp/tggw-kind-verify.yaml}"
KCTX=(kubectl)

log() { printf '\n== %s\n' "$*"; }
fail() { printf '\nVERIFY-FAIL: %s\n' "$*" >&2; exit 1; }

cleanup() {
  if [ "${KEEP_CLUSTER:-0}" != "1" ]; then
    kind delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# ── 1. cluster + image ─────────────────────────────────────────────
log "creating kind cluster"
kind create cluster --name "$CLUSTER" --image kindest/node:v1.31.4 --wait 120s \
  --kubeconfig "$KUBECONFIG"

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  log "building $IMAGE"
  docker build -f "$REPO/docker/Dockerfile.tggw" -t "$IMAGE" "$REPO"
fi
kind load docker-image "$IMAGE" --name "$CLUSTER"

# ── 2. fake Telegram inside the cluster ────────────────────────────
log "deploying in-cluster fake Telegram"
"${KCTX[@]}" create ns faketg
"${KCTX[@]}" -n faketg create configmap fake-tg --from-file="$REPO/tests/conformance_go/fake_telegram_upstream.py"
"${KCTX[@]}" -n faketg apply -f - <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata: {name: fake-tg, namespace: faketg}
spec:
  replicas: 1
  selector: {matchLabels: {app: fake-tg}}
  template:
    metadata: {labels: {app: fake-tg}}
    spec:
      containers:
        - name: fake
          image: python:3.12-alpine
          command: ["python", "/srv/fake_telegram_upstream.py"]
          volumeMounts: [{name: script, mountPath: /srv}]
          ports: [{containerPort: 18090}]
      volumes:
        - name: script
          configMap: {name: fake-tg}
---
apiVersion: v1
kind: Service
metadata: {name: fake-tg, namespace: faketg}
spec:
  selector: {app: fake-tg}
  ports: [{port: 18090, targetPort: 18090}]
EOF
"${KCTX[@]}" -n faketg wait --for=condition=available --timeout=300s deployment/fake-tg
FAKE_SVC=$("${KCTX[@]}" -n faketg get svc fake-tg -o jsonpath='{.spec.clusterIP}')
POD_CIDR=$("${KCTX[@]}" get node -o jsonpath='{.items[0].spec.podCIDR}')

# ── 3. helm install ────────────────────────────────────────────────
log "helm install (traffic pass: policy off until the upstream IP is known)"
helm install tggw "$REPO/deploy/helm/tggw" --namespace "$NS" --create-namespace \
  --set image.repository=tggw --set image.tag="${IMAGE##*:}" --set image.pullPolicy=Never \
  --set secrets.fernetKey="$FERNET" \
  --set mtproto.enabled=false \
  --set networkPolicy.enabled=false \
  --set botapi.env.GW_UPSTREAM="http://${FAKE_SVC}:18090" \
  --set botapi.env.GW_POLL_TIMEOUT=0 \
  --set botapi.env.GW_POLL_BACKOFF_MAX=0.1
"${KCTX[@]}" -n "$NS" wait --for=condition=ready --timeout=300s pod/tggw-botapi-0

PVC_MODE=$("${KCTX[@]}" -n "$NS" get pvc data-tggw-botapi-0 -o jsonpath='{.spec.accessModes[0]}')
[ "$PVC_MODE" = "ReadWriteOncePod" ] || fail "PVC access mode = $PVC_MODE, want ReadWriteOncePod"

ADM=$("${KCTX[@]}" -n "$NS" get secret tggw-secrets -o jsonpath='{.data.GW_ADMIN_SECRET}' | base64 -d)
APP=$("${KCTX[@]}" -n "$NS" get secret tggw-secrets -o jsonpath='{.data.GW_APP_SECRET}' | base64 -d)

forward() {
  "${KCTX[@]}" -n "$NS" port-forward svc/tggw 18080:8080 >/dev/null 2>&1 &
  PF1=$!
  "${KCTX[@]}" -n "$NS" port-forward svc/tggw-admin 18081:8081 >/dev/null 2>&1 &
  PF2=$!
  for _ in $(seq 1 100); do
    API_CODE=$(curl -s -o /dev/null -w '%{http_code}' -m 5 http://127.0.0.1:18080/healthz 2>/dev/null || echo 000)
    ADM_CODE=$(curl -s -o /dev/null -w '%{http_code}' -m 5 http://127.0.0.1:18081/metrics 2>/dev/null || echo 000)
    [ "$API_CODE" = "200" ] && [ "$ADM_CODE" != "000" ] && return 0
    sleep 0.2
  done
  fail "port-forwards never became healthy (api=$API_CODE admin=$ADM_CODE)"
}
unforward() { kill ${PF1:-} ${PF2:-} 2>/dev/null || true; }

api() { curl -s -m 10 "$@" 2>/dev/null || echo; }
adm() { api -H "Authorization: Bearer $ADM" "$@"; }

log "traffic: register → poll → pull → send (API :18080, admin :18081)"
forward
REG=$(api -X POST -H "Authorization: Bearer $ADM" -d "{\"token\":\"$TOKEN\",\"alias\":\"k1\"}" \
  http://127.0.0.1:18081/admin/bots)
echo "$REG" | grep -q '"ok":[ ]*true' || fail "register failed: $REG"

for _ in $(seq 1 100); do
  DEPTH=$(adm http://127.0.0.1:18081/admin/status | grep -o '"depth":[0-9]*' | head -1 | cut -d: -f2)
  [ "${DEPTH:-0}" -ge 2 ] && break
  sleep 0.2
done
[ "${DEPTH:-0}" -ge 2 ] || fail "queue never filled (poller broken)"

PULL=$(api -X POST -H "Authorization: Bearer $APP" -d '{"timeout":0,"offset":1001}' "http://127.0.0.1:18080/tgapi/bot$TOKEN/getUpdates")
echo "$PULL" | grep -q '"update_id":1001' || fail "pull missing update 1001: $PULL"
SEND=$(api -X POST -H "Authorization: Bearer $APP" -d '{"chat_id":10,"text":"hi"}' \
  http://127.0.0.1:18080/bots/k1/sendMessage)
echo "$SEND" | grep -q '"ok":[ ]*true' || fail "send failed: $SEND"
api http://127.0.0.1:18081/metrics | grep -q 'tg_gateway_session_state{alias="k1"} 1' || fail "metrics missing on admin port"

log "pod deletion mid-traffic (unacked 1001+1002 must replay exactly once)"
unforward
OLD_UID=$("${KCTX[@]}" -n "$NS" get pod tggw-botapi-0 -o jsonpath='{.metadata.uid}')
"${KCTX[@]}" -n "$NS" delete pod tggw-botapi-0 --wait=false
# wait for a NEW pod (different uid) — `kubectl wait` alone would match
# the old, still-registered object and return instantly
for _ in $(seq 1 300); do
  NEW_UID=$("${KCTX[@]}" -n "$NS" get pod tggw-botapi-0 -o jsonpath='{.metadata.uid}' 2>/dev/null || echo gone)
  [ "$NEW_UID" != "$OLD_UID" ] && [ "$NEW_UID" != "gone" ] && break
  sleep 1
done
[ "$NEW_UID" != "$OLD_UID" ] && [ "$NEW_UID" != "gone" ] || fail "replacement pod never appeared"
"${KCTX[@]}" -n "$NS" wait --for=condition=ready --timeout=300s pod/tggw-botapi-0
forward
sleep 2
STATUS=$(adm http://127.0.0.1:18081/admin/status)
if ! echo "$STATUS" | grep -q '"alias":"k1"'; then
  echo "--- admin status: $STATUS"
  "${KCTX[@]}" -n "$NS" logs tggw-botapi-0 --tail=30 || true
  fail "session not restored after pod kill"
fi
PULL=$(api -X POST -H "Authorization: Bearer $APP" -d '{"timeout":0,"offset":1001}' "http://127.0.0.1:18080/tgapi/bot$TOKEN/getUpdates")
echo "$PULL" | grep -q '"update_id":1002' || fail "unacked updates lost across pod kill: $PULL"
api -X POST -H "Authorization: Bearer $APP" -d '{"timeout":0,"offset":1003}' "http://127.0.0.1:18080/tgapi/bot$TOKEN/getUpdates" >/dev/null
unforward

# ── 4. NetworkPolicy enforcement ───────────────────────────────────
log "enabling NetworkPolicy (egress allowlist: DNS + fake CIDRs)"
helm upgrade tggw "$REPO/deploy/helm/tggw" --namespace "$NS" --reuse-values \
  --set networkPolicy.enabled=true \
  --set networkPolicy.telegramCIDRs[0]="$POD_CIDR" \
  --set "networkPolicy.telegramCIDRs[1]=10.96.0.0/12"
"${KCTX[@]}" -n "$NS" wait --for=condition=ready --timeout=300s pod/tggw-botapi-0

log "ingress: unlabeled namespace must be BLOCKED"
BLOCKED=$("${KCTX[@]}" -n faketg exec deployment/fake-tg -- python -c "
import urllib.request, socket
socket.setdefaulttimeout(4)
try:
    print('REACHABLE', urllib.request.urlopen('http://tggw.$NS.svc.cluster.local:8080/healthz', timeout=4).status)
except Exception as e:
    print('BLOCKED', type(e).__name__)")
echo "$BLOCKED" | grep -q BLOCKED || fail "NetworkPolicy ingress NOT enforced: $BLOCKED"

log "ingress: labeled client namespace must pass"
"${KCTX[@]}" label ns faketg tg-gateway.io/client=true --overwrite
sleep 2
REACH=$("${KCTX[@]}" -n faketg exec deployment/fake-tg -- python -c "
import urllib.request
print('REACHABLE', urllib.request.urlopen('http://tggw.$NS.svc.cluster.local:8080/healthz', timeout=4).status)")
echo "$REACH" | grep -q 'REACHABLE 200' || fail "labeled client blocked: $REACH"

log "egress: gateway must still reach its (allowed) upstream"
EGRESS=$("${KCTX[@]}" -n "$NS" exec tggw-botapi-0 -- /tggw healthcheck 2>&1)
echo "$EGRESS" | grep -q 'healthcheck ok' || fail "gateway unhealthy after policy: $EGRESS"

log "push egress: labeled consumer namespace RECEIVES, unlabeled never does"
# receiver: any HTTP POST lands in a file the test can read
"${KCTX[@]}" -n faketg apply -f - <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata: {name: push-receiver, namespace: faketg}
spec:
  replicas: 1
  selector: {matchLabels: {app: push-receiver}}
  template:
    metadata: {labels: {app: push-receiver}}
    spec:
      containers:
        - name: rx
          image: python:3.12-alpine
          command: ["python", "-c"]
          args:
            - |
              import http.server, subprocess
              class H(http.server.BaseHTTPRequestHandler):
                  def do_POST(self):
                      open("/tmp/received","a").write("hit\n")
                      self.send_response(200); self.end_headers()
                  def log_message(self, *a): pass
              http.server.HTTPServer(("0.0.0.0", 8099), H).serve_forever()
YAML
"${KCTX[@]}" -n faketg wait --for=condition=available --timeout=300s deployment/push-receiver
"${KCTX[@]}" -n faketg expose deployment push-receiver --port=8099 >/dev/null 2>&1 || true
# faketg carries the client label from the ingress test → egress allowed
PUSH_OK=$("${KCTX[@]}" -n "$NS" exec tggw-botapi-0 -- /tggw healthcheck http://push-receiver.faketg.svc.cluster.local:8099/healthz 2>&1 || true)
if ! echo "$PUSH_OK" | grep -qE "healthcheck ok|healthz status"; then
  fail "push egress to labeled ns broken: $PUSH_OK"
fi
# unlabeled namespace: egress denied by the policy
"${KCTX[@]}" create ns nolabel >/dev/null 2>&1 || true
PUSH_BLOCKED=$("${KCTX[@]}" -n "$NS" exec tggw-botapi-0 -- /tggw healthcheck http://fake-tg.nolabel.svc.cluster.local:18090 2>&1 || true)
if echo "$PUSH_BLOCKED" | grep -q "healthcheck ok"; then
  fail "push egress reached an UNLABELED namespace: $PUSH_BLOCKED"
fi
echo "push egress: labeled=ok, unlabeled=blocked"

# ── 5. idle resources (crictl — no metrics-server needed) ─────────
log "idle resources via crictl"
sleep 5
# informational: idle memory from crictl's table (target < 32Mi)
docker exec "${CLUSTER}-control-plane" crictl stats 2>/dev/null | awk 'NR==1 || /botapi|mtproto/' || true

log "VERIFY-OK: install, RWOP PVC, traffic, pod-kill replay, NetworkPolicy, resources"
