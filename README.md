# hermes-kimi-claw

A [Hermes Agent](https://github.com/NousResearch/hermes-agent) platform plugin that bridges your
self-hosted Hermes instance to **Kimi Claw** (kimi.com) — so you can talk to your Hermes
from the Kimi app / web UI, with streaming, tool cards, thinking blocks, and file transfer.

This is an interoperability re-implementation of the official `kimi-claw` OpenClaw connector
plugin (v0.27.1), ported to Hermes' platform/plugin architecture. The wire protocol was
documented from the publicly distributed plugin package and validated against production,
including a frame-by-frame comparison against traffic captured from an official cloud-hosted
Kimi Claw instance. [中文文档](README.zh-CN.md)

## Features

| Capability | Status |
|---|---|
| Text DM (inbound/outbound) | ✅ |
| Typewriter streaming replies (WS `send-message-stream`) | ✅ |
| Thinking blocks (`forwardThinking` equivalent) | ✅ |
| Native tool-call cards (`forwardToolCalls` equivalent) | ✅ |
| Inbound images/files (downloaded, routed to vision/file tools) | ✅ |
| Outbound files as Kimi file cards (`kimi_upload_file` tool + resourceLink) | ✅ |
| `kimi_search` / `kimi_fetch` / `kimi_finance` / `kimi_datasource_*` tools | ✅ |
| Kimi group chat / multi-agent (`kimiim-cli` + skill) | ✅ |
| Cron delivery to Kimi (`KIMI_CLAW_HOME_CHANNEL`) | ✅ |
| Pairing / authorization via Hermes pairing flow | ✅ |
| Resume after disconnect (`sinceId`), reconnect backoff | ✅ |
| Remote terminal (agent-ws `web-ssh`) | ⏸ implemented, dormant — Kimi cloud does not serve this to self-hosted instances today |

Known structural difference: Hermes finalizes a message bubble at each tool boundary
("one segment = one bubble"), so multi-tool tasks appear as several sequentially
typewritten bubbles instead of a single multi-block message. Content is unaffected.

## Requirements

- Hermes Agent v0.20+ (developed against v0.20.1)
- A Kimi account with Kimi Claw access
- Network access to `www.kimi.com` / `api.kimi.com`

## Install

1. Copy the `kimi-claw/` directory into `~/.hermes/plugins/`:

   ```bash
   git clone https://github.com/<your-org>/hermes-kimi-claw.git
   cp -r hermes-kimi-claw/kimi-claw ~/.hermes/plugins/
   ```

2. Get a bot token: kimi.com → Kimi Claw → 关联已有 OpenClaw → copy the
   `bash <(curl ...) --bot-token km_b_prod_...` command; you only need the token.

3. Add the token to `~/.hermes/.env`:

   ```
   KIMI_CLAW_BOT_TOKEN=km_b_prod_...
   ```

4. Enable the platform in `~/.hermes/config.yaml`:

   ```yaml
   platforms:
     kimi-claw:
       enabled: true
   streaming:
     enabled: true      # required for typewriter streaming
     transport: auto
   display:
     platforms:
       kimi-claw:
         tool_progress: false      # suppress text tool chrome (native cards instead)
         thinking_progress: true   # relay thinking into think blocks
   ```

5. Enable the plugin and restart the gateway:

   ```bash
   hermes plugins enable kimi-claw
   hermes plugins doctor kimi-claw   # optional sanity check
   systemctl restart hermes-gateway  # or however you run it
   ```

6. Back in the Kimi dialog, click **我已运行**, then send any message.
   On first contact Hermes replies with a pairing code — approve it:

   ```bash
   hermes pairing approve kimi-claw <CODE>
   ```

## Optional pieces

- **Group chat**: install `kimiim-cli` (see `docs/`) and the bundled skill notes;
  the CLI reads its bridge config from `~/.openclaw/openclaw.json` or `KIMI_BOT_TOKEN`.
- **Search tools** authenticate with `KIMI_PLUGIN_API_KEY`, falling back to your
  existing `HERMES_CUSTOM_API_KIMI_COM_API_KEY` (Kimi Code key) — verified working
  against `api.kimi.com/coding/v1/{search,fetch,tools}`.
- **Remote terminal**: `platforms.kimi-claw.extra.terminal_enabled: true` starts the
  agent-ws channel with a full pty bridge. ⚠️ This exposes a shell on your machine to
  whoever can drive the linked Kimi bot. It is off by default and currently receives
  no traffic from Kimi cloud (server-side gated).

## Configuration reference

| Key | Where | Default | Notes |
|---|---|---|---|
| `KIMI_CLAW_BOT_TOKEN` | env / `.env` | — | required |
| `KIMI_CLAW_KIMIAPI_HOST` | env | `https://www.kimi.com/api-claw` | |
| `KIMI_CLAW_WS_URL` | env | `wss://www.kimi.com/api-claw/bots/agent-ws` | terminal channel |
| `KIMI_PLUGIN_API_KEY` | env | falls back to Kimi Code key | search-series tools |
| `terminal_enabled` | `extra` | `false` | see warning above |
| `forward_thinking` / `forward_tool_calls` | `extra` | `true` | think/tool blocks |
| `dm_policy` / `group_policy` | `extra` | `open` | Hermes access policy |

State is persisted in `~/.hermes/state/kimi-claw/` (resume cursor, chat routing,
instance ids). Inbound files land in `~/.hermes/attachments/kimi-claw/`.

## How it works

See [docs/PROTOCOL.md](docs/PROTOCOL.md) for the full wire-protocol writeup
(Connect-RPC subscribe, WS streaming frames, block choreography, file transfer,
terminal envelopes) with byte-level notes on what the server accepts and rejects.

## Disclaimer

Unofficial community project, not affiliated with Moonshot AI. Interoperating with
the Kimi Claw service this way may be outside Kimi's supported usage — use your own
account, expect breakage on upstream changes, and rotate your bot token if it ever
leaks. The `kimi-claw` name and protocol belong to their respective owners.

## License

MIT (see LICENSE). Protocol documentation produced from the publicly distributed
plugin package for interoperability purposes.
