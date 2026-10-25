# examples/

Working consumers for the gateway, one per language. All three do the
same thing: pull updates through the gateway's durable queue and echo
every text message back (with an Idempotency-Key, so a crash between
send and ack never double-sends).

## Run the whole demo stack (no Telegram credentials needed)

```bash
docker compose -f examples/docker-compose.demo.yml up --build
```

This starts a stdlib fake Telegram upstream, `tggw` pointed at it, a
one-shot bot registration, and all three consumers. The fake produces
one batch of two updates — watch each consumer echo them:

| Directory | What it shows |
|---|---|
| [go-consumer](go-consumer/) | the typed Go SDK (`pkg/client`): pull with offset ack, auto Idempotency-Key, typed 429/403/501 errors |
| [python-consumer](python-consumer/) | the raw wire contract with httpx: Bearer secret, envelope, offset ack, 429 retry_after |
| [node-consumer](node-consumer/) | the same contract with plain `fetch` (Node 18+, zero dependencies) |

## Against real Telegram

Point the gateway at the real API and register your actual token:

```bash
GW_UPSTREAM=https://api.telegram.org
# the consumer env stays the same; BOT_TOKEN becomes your real token
```

The consumer code never changes — that is the point of the gateway.
