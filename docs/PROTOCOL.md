# Kimi Claw bridge protocol (as implemented by this plugin)

Documented from the publicly distributed `kimi-claw` OpenClaw plugin v0.27.1 and
validated against production (2026-08), including a frame-by-frame comparison with
traffic from an official cloud-hosted Kimi Claw instance running verbose tracing.

## 0. Topology

```
Kimi app/web ── Kimi cloud ──(1) IM RPC: Connect-RPC over HTTPS)──▶ this plugin ──▶ Hermes gateway
                            ──(2) WS streaming replies          ──▶
                            ──(3) agent-ws channel (terminal)   ──▶  (server-side gated off)
```

- IM RPC base: `https://www.kimi.com/api-ws`
  (the configured `kimiapiHost` is `.../api-claw`; the plugin rewrites the path to `/api-ws`)
- Auth on **every** HTTP/WS request: header `X-Kimi-Bot-Token: km_b_prod_...`
- Self-reported identity headers (all client-asserted, never verified):
  `X-Kimi-Claw-Version` (plugin version), `X-Kimi-OpenClaw-Version`,
  `X-Kimi-OpenClaw-Skills`, `X-Kimi-OpenClaw-Plugins`
- Message bodies are **protojson** (lowerCamelCase fields, enums as name strings,
  int64 as strings, FieldMask as a single string)
- The official cloud instance runs `ingressMode: im_only` + `outboundTransport: im_rpc`
  — exactly the paths implemented here

## 1. Inbound: Subscribe (Connect server-streaming)

```
POST {im_base}/kimi.gateway.im.v1.IMService/Subscribe
content-type: application/connect+json; charset=utf-8
accept: application/connect+json
connect-protocol-version: 1
```

- Request body = ONE Connect envelope: `0x00 + uint32BE(len) + protojson(SubscribeRequest)`
  where `SubscribeRequest = { optional string since_id }` → `{}` or `{"sinceId": "<eventId>"}`
- Response = envelope stream (same framing). Flags: bit0 compressed (unused), bit1 EndStream
  (payload `null` or `{"error": {...}}`)
- Response headers carry `x-kimi-claw-default-chat` (primary DM chat id) and
  `x-kimi-claw-bot-region`
- Server pushes `{"ping": {}}` ~every 10s; treat 30s of silence as a dead connection
- `Event` = `{ id, oneof payload: ping | chatMessage | reconnect | typing | disband | botReport }`
  - `ping` → liveness only
  - `reconnect` → reconnect immediately
  - `botReport` → answer with `UpdateBotMeta`
  - `chatMessage` → dispatch only when `status == "STATUS_COMPLETED"` and `role == "user"`
    (the role filter is the loop guard — no self-replies)
- Resume: persist every `Event.id`; reconnect with `sinceId` = last seen.
  Backoff 1s ×2 up to 300s, unlimited attempts. HTTP 401/403 = auth failure, do not retry.

### ChatMessageEvent

```json
{"chatId": "...", "messageId": "...", "status": "STATUS_COMPLETED",
 "role": "user", "senderId": "...", "senderShortId": "...",
 "roomId": "...", "roomType": "GROUP|DIRECT", "mentioned": true}
```

The event has **no body** — fetch it:

```
POST .../IMService/ListMessages
{"chatId", "startMessageId": msgId, "endMessageId": msgId,
 "includeStartMessage": true, "includeEndMessage": true,
 "direction": "DIRECTION_BACKWARD", "pageSize": 20, "pageToken": ""}
```

→ `messages[*].message.blocks[]`: `text.content` for text, `file` for attachments,
`resourceLink.uri` for links.

### Inbound files

`block.file` is an inline `kimi.file.v1.File`: `{id, meta:{name,contentType,sizeBytes},
blob:{signUrl}}`. If `signUrl` is absent/expired:

```
GET {origin}/api-claw/files/{fileId}     (X-Kimi-Bot-Token header)
→ {id, meta, blob:{signUrl}}             (fallback: parseJob image previewUrl)
```

## 2. Outbound

### 2a. One-shot: unary SendMessage

```
POST .../IMService/SendMessage
{"chatId": "<uuidv8 from server>", "blocks": [{"id": "b1", "text": {"content": "..."}}]}
→ {"messageId": "..."}
```

File card: `{"id": "f1", "resourceLink": {"title": name, "uri": "kimi-file://<id>"}}`.

### 2b. Typewriter streaming: WS send-message-stream

```
WS wss://www.kimi.com/api-ws/im/send-message/ws
handshake header: X-Kimi-Bot-Token only
```

Each frame is one protojson `SendMessageStreamRequest`:

1. First frame binds the chat: `{"chatId": "..."}`
2. Block frames: `{"block": {"op": "append"|"set", "mask": "block.text.content",
   "block": {"id": "<seq>", "text": {"content": "..."}}}}`
3. Keepalive `{"ping": {}}` every 10s
4. Final `{"end": {}}`, then close; a normal close = success (no message_id returned)

**Block choreography** (captured from the official cloud instance, verbose trace):

```
frame 1      {"chatId": ...}
frames 2..N  think lane:  blockId "0", op=append, mask=block.think.content,
                          content prefixed with "Reasoning:\n"
tool start   tool lane:   blockId "1", op=set, NO mask,
                          block.tool = {toolCallId, name, args}
answer lane  text:        next free blockId ("3" in the capture),
                          op=append, mask=block.text.content, incremental deltas
last         {"end": {}}
```

Reconciliation: if the new snapshot does not extend the sent text (segment rewrite),
send one `op=set` full-snapshot frame.

**Production-verified rejection**: `ToolBlock.contents[].status` kills the connection
in BOTH encodings tried (`"running"`/`"done"` and `"STATUS_RUNNING"`/`"STATUS_DONE"`).
Only the bare `{toolCallId, name, args}` tool block is accepted; status transitions are
therefore not expressed.

### 2c. File upload (outbound files)

```
POST {kimiapiHost origin}/api-claw/files:upload   (sic: path is /files:upload on the api-claw host)
header: X-Kimi-Bot-Token
multipart field: "file" (1–5 files per call, sequential)
→ {"file": {"id": "...", "meta": {"name": "...", "contentType": "..."}}}
```

Important: uploading alone does NOT display anything. The official plugin converts the
result into `resourceLink` blocks in the outbound stream — this plugin sends a separate
SendMessage with a `resourceLink` block instead (same user-visible result).

### 2d. UpdateBotMeta

`POST .../IMService/UpdateBotMeta` with
`{"pluginVersion", "openclawVersion", "skills": [...], "platform": "linux"}`.
Sent in reply to `botReport` events. Extra fields are tolerated (HTTP 200) but have no
observed effect (e.g. `botType: "remote"` does not unlock cloud-instance UI features —
those are gated by the server-side instance record).

## 3. agent-ws channel (remote terminal — currently dormant server-side)

```
WS wss://www.kimi.com/api-claw/bots/agent-ws
headers: X-Kimi-Bot-Token, X-Kimi-Claw-Version
```

- No client first frame; server pushes. Keepalive: send `{"type":"ping"}` every 15s;
  answer text `ping` with `pong`. Close code 4001 / HTTP 401 = auth failure (no retry).
- Downstream: JSON-RPC 2.0 (`initialize`, `session/*`) + out-of-band terminal envelopes.
- `initialize` reply (capability advertisement):

```json
{"protocolVersion": 1,
 "agentCapabilities": {"loadSession": true,
   "promptCapabilities": {"embeddedContext": true, "image": true, "audio": false},
   "sessionCapabilities": {"list": {}, "web-ssh": true}},
 "agentInfo": {"name": "kimi-claw", "version": "0.27.1"},
 "_meta": {"instanceId": "...", "deviceId": "..."}}
```

- Terminal envelope (both directions):
  `{"type": "terminal", "terminalId": "term_...", "_clientId": "<echo>", "payload": {...}}`
  - inbound actions: `open{cols,rows,cwd?,shell?}` / `input{dataBase64|content}` /
    `resize{cols,rows}` / `close` / `heartbeat`
  - outbound: `opened{...}` / `{event:"stdout", dataBase64, seq}` /
    `{event:"exit", code, signal, reason?}` / `{event:"state", from, to}` /
    `{action:"error", code, message}` (codes: -32010 shellDisabled, -32011 notFound,
    -32012 closed/duplicate, -32013 quota, -32014 timeout, -32601 unknown, -32602 params)

**Status**: verified that the cloud accepts this connection but sends nothing — for
self-hosted instances the feature is unwired server-side (the cloud-hosted terminal is
plain SSH from Kimi's internal network to the VM they provision; it does not use this
protocol at all). This plugin ships a complete implementation (`terminal_bridge.py`,
stdlib `pty`), disabled by default.

## 4. Search-series tool API

Base `https://api.kimi.com/coding/v1` (the cloud plugin defaults to
`https://agent-gw.kimi.com/coding/v1`, which requires a Kimi-provisioned key;
a regular Kimi Code key works on `api.kimi.com` only). Auth: `Authorization: Bearer`.

| Tool | Endpoint | Body |
|---|---|---|
| kimi_search | `/search` | `{text_query, limit 1-20, enable_page_crawling, timeout_seconds}` → `search_results[]` |
| kimi_fetch | `/fetch` | `{url}`, `Accept: text/markdown` → markdown body |
| kimi_finance | `/tools` | `{method:"call_data_source_tool", params:{data_source_name:"stock_finance_data", api_name:"stock_finance_data_get_stock_realtime_price", params:{ticker,time?,type,file_path}}}` |
| kimi_datasource_get_desc | `/tools` | `{method:"get_data_source_desc", params:{name}}` |
| kimi_datasource_call | `/tools` | `{method:"call_data_source_tool", params:{data_source_name, api_name, params}}` |

Data sources: `yahoo_finance, arxiv, world_bank_open_data, imf, tianyancha, scholar,
yuandian_law, stock_finance_data`. Responses: `is_success`, assistant texts under
`result.assistant[*].text` (a JSON string containing `data_preview`/`notice`), files
under `files[{name, content}]`.

## 5. Hermes-side adaptation notes (v0.20.1)

Three version-specific gotchas this plugin works around:

1. `SUPPORTS_MESSAGE_EDITING = False` makes the gateway skip the ENTIRE stream
   consumer (anti-partial-duplication guard for QQ/WeChat). This adapter reports True
   and fails `edit_message` gracefully — the draft contract never produces partial
   duplicates.
2. The stream-event dispatcher (`render_message_event`/`format_tool_event`) exists but
   is not wired in production; tool-call interception uses a `pre_tool_call` plugin hook
   instead, and tool chrome text is disabled per-platform
   (`display.platforms.kimi-claw.tool_progress: false`).
3. Progress bubbles are delivered via plain `adapter.send()` — indistinguishable from a
   final answer. The `💬 `-prefixed thinking relay is intercepted and routed to think
   blocks; everything else finalizes the current stream (segment = bubble model).
