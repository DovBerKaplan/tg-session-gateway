# Security Policy

## Threat Model

### Assets

| Asset | Where | Risk if compromised |
|---|---|---|
| Bot tokens | SQLite (Fernet-encrypted via `GW_FERNET_KEY`) | Attacker can impersonate the bot |
| MTProto auth_key | `.session` files on the session volume | Attacker can use the bot's identity; harder to revoke than a token |
| Admin secret | `.env` / Docker environment | Full control of all sessions |
| App secret | `.env` / Docker environment | Can send messages as any registered bot |

### Attack surfaces

1. **Admin API** — bound to `127.0.0.1` only; requires `Authorization: Bearer $GW_ADMIN_SECRET`.
   Not exposed to the public internet by design (spec §5.1).
2. **Consumer API** — on the internal Docker network (`tg-gateway-net`); requires
   `Authorization: Bearer $GW_APP_SECRET`.
3. **Session files** — on a dedicated Docker volume (`tg-gateway-data` or
   `tg-mtgateway-data`); not baked into images; permissions `0700` on directory,
   `0600` on files (enforced by the sidecar at startup).
4. **Network** — the sidecar/gateway is the only process with TCP to Telegram.
   Consumers have no direct Telegram access.

### Known footgun: `/tgapi/bot{token}/...` is unauthenticated

The Bot API-compatible surface (`/tgapi/bot<token>/<method>` and
`/file/bot<token>/...`) authenticates by **possession of the token
itself** — exactly like `api.telegram.org`. There is no additional
secret. Consequences:

- Anyone on the gateway's Docker network who learns a bot token can
  send as that bot through the gateway.
- Treat the token with the same care as in direct mode: never in
  logs, never in app config committed to a repo.
- For a hardened setup, use the alias surface
  (`/bots/{alias}/{method}`, Bearer `GW_APP_SECRET`) so consumer
  processes never hold bot tokens at all — and rotate `GW_APP_SECRET`
  independently of bot tokens.

Admin actions (register, delete, pause, resume, push/paid settings)
are recorded in an append-only audit table in the store
(`admin_audit`) — check it when a session "disappears".

## Session File Security

Session files (`.session`) contain the raw MTProto `auth_key`. Anyone with
filesystem access to the session volume can impersonate the bot.

**Mitigations:**

- Session volume is a dedicated Docker volume, not a bind mount to the host
- Volume permissions: `chmod 700` on the directory, `chmod 600` on the
  `.session` files — both enforced by the sidecar itself at startup
- Session files are never logged (only alias names appear in logs)
- Tokens ARE encrypted at rest (Fernet); session files are filesystem-protected

**Known limitation:** session files are not encrypted at rest. The auth_key
inside them is only as safe as the Docker volume. If you need encryption
at rest for session files, use filesystem-level encryption (LUKS, eCryptfs)
on the Docker volume. Application-level session file encryption is on the
roadmap ([Phase 0.4](roadmap-10-of-10.md#04-session-file-encryption-at-rest)).

## Secrets policy (Fernet key)

`GW_FERNET_KEY` encrypts bot tokens in the store. Behavior:

- **Not set** → a key is auto-generated into `<data_dir>/.fernet_key`
  (0600) with a loud warning. Acceptable for a POC, **not for real
  traffic**: losing the file loses every stored token.
- **Production** → set `GW_FERNET_KEY` explicitly, store it in your
  secrets manager, and **back it up** — the encrypted tokens are
  unrecoverable without it.
- **Rotation** → set the new key, then re-register each bot
  (`POST /admin/bots` with the same alias and token — the store
  re-encrypts on upsert). The old key is not needed for rotation.

## Singleton Lock

Each session has an advisory file lock (`.lock` file next to the `.session`
file). The lock is acquired BEFORE any TCP connection to Telegram. A second
process attempting to use the same session:

1. Fails at the filesystem level (`O_CREAT | O_EXCL`)
2. Never opens a socket to Telegram
3. Therefore never triggers `AUTH_KEY_DUPLICATED`

The lock includes a PID and heartbeat timestamp. A stale lock (holder
process dead + no heartbeat for 60 seconds) is automatically reclaimed.

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Use GitHub's **private vulnerability reporting** on this repo
(Security tab → "Report a vulnerability"), or open a draft
GitHub Security Advisory.

- Response time: within 48 hours
- Disclosure: coordinated (we'll acknowledge, investigate, and credit you)
- Please include: steps to reproduce, impact assessment, suggested fix if any

## Hardening Checklist

- [ ] Set a strong `GW_ADMIN_SECRET` (32+ random bytes)
- [ ] Set a strong `GW_APP_SECRET` (32+ random bytes)
- [ ] Set `GW_FERNET_KEY` (don't rely on auto-generation; back it up)
- [ ] Keep the admin API on loopback only (default)
- [ ] Use a dedicated Docker volume for session files
- [ ] Restrict Docker network access to known consumer containers
- [ ] Monitor `/admin/status` for unexpected sessions
- [ ] Set up alerting on `tg_gateway_real_flood_wait_total > 0`
