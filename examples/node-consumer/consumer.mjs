// Node consumer example (Node 18+, no dependencies): raw fetch
// against the gateway — the Bot API wire contract, nothing else.
//
// Env:
//   TGGW_BASE       gateway base URL     (default http://127.0.0.1:8080)
//   TGGW_APP_SECRET GW_APP_SECRET value  (required)
//   BOT_TOKEN       the token registered in the gateway (required)

const base = (process.env.TGGW_BASE || "http://127.0.0.1:8080").replace(/\/$/, "");
const secret = process.env.TGGW_APP_SECRET;
const token = process.env.BOT_TOKEN;
if (!secret || !token) {
  console.error("TGGW_APP_SECRET and BOT_TOKEN are required");
  process.exit(1);
}
const headers = { "Content-Type": "application/json", Authorization: `Bearer ${secret}` };
let offset = 0;

async function getUpdates() {
  const body = { timeout: 25, limit: 100 };
  if (offset) body.offset = offset + 1;
  const r = await fetch(`${base}/tgapi/bot${token}/getUpdates`, {
    method: "POST", headers, body: JSON.stringify(body),
  });
  const data = await r.json();
  if (!data.ok) throw new Error(`getUpdates: ${data.description}`);
  for (const u of data.result || []) offset = Math.max(offset, u.update_id);
  return data.result || [];
}

async function send(chatId, text) {
  const r = await fetch(`${base}/tgapi/bot${token}/sendMessage`, {
    method: "POST",
    headers: { ...headers, "Idempotency-Key": `echo-${chatId}-${text}` },
    body: JSON.stringify({ chat_id: chatId, text }),
  });
  if (r.status === 429) {
    const data = await r.json();
    await new Promise((res) => setTimeout(res, (data.parameters?.retry_after || 1) * 1000));
  }
}

async function main() {
  while (true) {
    try {
      for (const u of await getUpdates()) {
        const msg = u.message;
        if (msg?.chat?.id && msg.text) await send(msg.chat.id, `echo: ${msg.text}`);
      }
    } catch (e) {
      console.error(`pull failed: ${e.message}`);
      await new Promise((res) => setTimeout(res, 1000));
    }
  }
}

main();
