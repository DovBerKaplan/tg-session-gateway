#!/usr/bin/env bash
# One-time gateway install (spec §5): creates the external network and
# starts the gateway under its OWN compose project, outside any consumer
# repo. Run from the repo root:  bash deploy/install.sh
set -euo pipefail

INSTALL_DIR="${TG_GATEWAY_HOME:-/opt/tg-gateway}"

if [ "$(id -u)" -ne 0 ]; then
  echo "run as root (sudo bash deploy/install.sh)"; exit 1
fi

mkdir -p "$INSTALL_DIR"
cp -r . "$INSTALL_DIR"/
cd "$INSTALL_DIR"

# external network — consumers join it, the gateway never joins theirs
docker network inspect tg-gateway-net >/dev/null 2>&1 || docker network create tg-gateway-net

# autoheal: Docker's `restart:` policy NEVER restarts an unhealthy
# container — only a crashed one. docker-autoheal closes that gap for
# anything labeled autoheal=true (both gateway planes carry the label).
if ! docker ps -a --filter name=tg-autoheal --format '{{.Names}}' | grep -q .; then
  docker run -d --name tg-autoheal \
    --restart=unless-stopped \
    -v /var/run/docker.sock:/var/run/docker.sock:ro \
    willfarrell/autoheal:latest \
    >/dev/null && echo "autoheal watchdog deployed (container: tg-autoheal)"
fi

# secrets
if [ ! -f .env ]; then
  ADMIN=$(openssl rand -hex 24)
  APP=$(openssl rand -hex 24)
  # urlsafe-base64 of 32 bytes — a valid Fernet key (GW_FERNET_KEY)
  FERNET=$(openssl rand -base64 32 | tr '+/' '-_')
  cat > .env <<EOF
GW_ADMIN_SECRET=$ADMIN
GW_APP_SECRET=$APP
GW_FERNET_KEY=$FERNET
EOF
  chmod 600 .env
  echo "generated secrets in $INSTALL_DIR/.env — save them:"
  echo "  GW_ADMIN_SECRET / GW_APP_SECRET / GW_FERNET_KEY"
  echo "(losing GW_FERNET_KEY loses every stored bot token)"
fi

docker compose up -d --build
docker compose ps

cat <<'EOF'

Gateway is up (project: tg-gateway, network: tg-gateway-net).

Register a bot:
  curl -H "Authorization: Bearer $GW_ADMIN_SECRET" \
       -H 'Content-Type: application/json' \
       -d '{"alias":"mybot","token":"123:ABC"}' \
       http://127.0.0.1:8080/admin/bots

Consumer (aiogram):  Bot(token, base_url="http://tg-gateway:8080/tgapi")
Consumer (PTB):      Telegram(token, base_url="http://tg-gateway:8080/tgapi")
EOF
