"""Kimi Claw IM bridge protocol client.

Python re-implementation of the kimi-claw OpenClaw plugin's IM bridge
(v0.27.1), reverse-engineered from the publicly distributed plugin package.

Protocol summary (see MAPPING.md / protocol report):
  - IM RPC base: https://www.kimi.com/api-ws  (kimiapiHost with /api-claw
    rewritten to /api-ws)
  - Auth header on every HTTP/WS request: X-Kimi-Bot-Token
  - Inbound: Connect-RPC server-streaming  POST /kimi.gateway.im.v1.IMService/Subscribe
    (envelope framing: 1-byte flags + uint32BE length + protojson payload)
  - Message body fetch: unary POST .../ListMessages
  - Outbound: unary POST .../SendMessage
  - Server pushes a ping event ~every 10s; 30s without any frame => reconnect
  - Resume via SubscribeRequest.sinceId = last received Event.id
"""

from __future__ import annotations

import asyncio
import json
import logging
import struct
from typing import Any, AsyncIterator, Callable, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)

DEFAULT_KIMIAPI_HOST = "https://www.kimi.com/api-claw"
IM_SERVICE = "kimi.gateway.im.v1.IMService"

# Self-reported identity headers (upstream collects these from the local
# openclaw CLI; we report the values Kimi Claw officially supports).
PLUGIN_VERSION = "0.27.1"
OPENCLAW_VERSION = "2026.4.14"

CONNECT_CONTENT_TYPE = "application/connect+json; charset=utf-8"
JSON_CONTENT_TYPE = "application/json; charset=utf-8"

PING_TIMEOUT_S = 30.0          # server pings ~every 10s; dead after 30s silence
RETRY_BASE_S = 1.0
RETRY_MAX_S = 300.0
REQUEST_TIMEOUT_S = 60.0

CONNECT_FLAG_COMPRESSED = 0x01
CONNECT_FLAG_END_STREAM = 0x02


class AuthError(Exception):
    """401/403 — bad bot token. Not retryable."""


class ReconnectRequested(Exception):
    """Server sent a reconnect event — reconnect immediately."""


class StreamClosed(Exception):
    """Stream ended (EOF / EndStream) — reconnect with backoff."""


def resolve_im_base(kimiapi_host: Optional[str]) -> str:
    host = (kimiapi_host or "").strip() or DEFAULT_KIMIAPI_HOST
    host = host.rstrip("/")
    # Upstream rewrites the /api-claw path to /api-ws for IM RPC traffic.
    if host.endswith("/api-claw"):
        host = host[: -len("/api-claw")] + "/api-ws"
    return host


def resolve_upload_host(kimiapi_host: Optional[str]) -> str:
    return ((kimiapi_host or "").strip() or DEFAULT_KIMIAPI_HOST).rstrip("/")


def encode_envelope(payload: bytes, flags: int = 0) -> bytes:
    return bytes([flags]) + struct.pack(">I", len(payload)) + payload


class KimiImClient:
    """Minimal text-capable Kimi Claw IM bridge client."""

    def __init__(self, token: str, kimiapi_host: Optional[str] = None,
                 extra_headers: Optional[Dict[str, str]] = None,
                 skills: Optional[list] = None):
        if not token:
            raise ValueError("bot token is required")
        self.token = token
        self.im_base = resolve_im_base(kimiapi_host)
        self.default_chat_id: Optional[str] = None
        self.bot_region: Optional[str] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._extra_headers = dict(extra_headers or {})
        self._skills = list(skills or [])

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    async def __aenter__(self) -> "KimiImClient":
        await self.open()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def open(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------
    # headers / errors
    # ------------------------------------------------------------------
    def _headers(self, content_type: str, streaming: bool = False) -> Dict[str, str]:
        h = {
            "X-Kimi-Bot-Token": self.token,
            "X-Kimi-Claw-Version": PLUGIN_VERSION,
            "X-Kimi-OpenClaw-Version": OPENCLAW_VERSION,
            "content-type": content_type,
        }
        if self._skills:
            h["X-Kimi-OpenClaw-Skills"] = json.dumps(
                self._skills, ensure_ascii=True)
        if streaming:
            h["accept"] = "application/connect+json"
            h["connect-protocol-version"] = "1"
        h.update(self._extra_headers)
        return h

    @staticmethod
    async def _raise_for_status(resp: aiohttp.ClientResponse, what: str) -> None:
        if resp.status in (401, 403):
            raise AuthError(f"{what}: auth failed (HTTP {resp.status})")
        if resp.status == 429:
            retry_after = resp.headers.get("Retry-After", "")
            raise StreamClosed(f"{what}: rate limited (429), retry-after={retry_after}")
        if resp.status >= 400:
            body = (await resp.text())[:300]
            raise StreamClosed(f"{what}: HTTP {resp.status} {body}")

    # ------------------------------------------------------------------
    # Subscribe (inbound, Connect server-streaming)
    # ------------------------------------------------------------------
    async def subscribe(self, since_id: Optional[str],
                        on_event: Callable[[Dict[str, Any]], Any]) -> None:
        """One subscribe connection. Returns on clean EndStream; raises
        StreamClosed/ReconnectRequested/AuthError otherwise.

        `on_event` may be sync or async; it is awaited if awaitable.
        """
        assert self._session is not None, "call open() first"
        url = f"{self.im_base}/{IM_SERVICE}/Subscribe"
        req: Dict[str, Any] = {}
        if since_id:
            req["sinceId"] = since_id
        body = encode_envelope(json.dumps(req).encode("utf-8"))

        async with self._session.post(
            url, data=body, headers=self._headers(CONNECT_CONTENT_TYPE, streaming=True),
            timeout=aiohttp.ClientTimeout(total=None),  # watchdog handles liveness
        ) as resp:
            await self._raise_for_status(resp, "subscribe")
            # Connection byproducts
            self.default_chat_id = resp.headers.get("x-kimi-claw-default-chat") or self.default_chat_id
            self.bot_region = resp.headers.get("x-kimi-claw-bot-region") or self.bot_region
            logger.info("[kimi-claw] subscribed. default_chat=%s region=%s since=%s",
                        self.default_chat_id, self.bot_region, since_id)

            stream = resp.content
            while True:
                try:
                    header = await asyncio.wait_for(
                        stream.readexactly(5), timeout=PING_TIMEOUT_S)
                except (asyncio.TimeoutError, asyncio.IncompleteReadError) as exc:
                    raise StreamClosed(f"subscribe: read failed ({exc!r})") from exc

                flags = header[0]
                (length,) = struct.unpack(">I", header[1:5])
                try:
                    payload = await asyncio.wait_for(
                        stream.readexactly(length), timeout=PING_TIMEOUT_S)
                except (asyncio.TimeoutError, asyncio.IncompleteReadError) as exc:
                    raise StreamClosed(f"subscribe: payload read failed ({exc!r})") from exc

                if flags & CONNECT_FLAG_COMPRESSED:
                    raise StreamClosed("subscribe: unexpected compressed frame")
                if flags & CONNECT_FLAG_END_STREAM:
                    error = None
                    if payload and payload != b"null":
                        try:
                            error = json.loads(payload)
                        except Exception:
                            error = {"raw": payload[:200].decode("utf-8", "replace")}
                    if error:
                        raise StreamClosed(f"subscribe: end-stream error {error}")
                    return  # clean end

                try:
                    event = json.loads(payload)
                except Exception:
                    logger.warning("[kimi-claw] undecodable event frame: %r", payload[:200])
                    continue

                if "reconnect" in event:
                    raise ReconnectRequested("server requested reconnect")
                # ping frames and everything else flow through the callback;
                # any received frame already proves liveness via readexactly.
                result = on_event(event)
                if asyncio.iscoroutine(result):
                    await result

    async def subscribe_forever(self, since_provider: Callable[[], Optional[str]],
                                on_event: Callable[[Dict[str, Any]], Any],
                                should_stop: Callable[[], bool]) -> None:
        """Reconnecting subscribe loop: exponential backoff 1s..300s,
        immediate reconnect on server request. Returns when should_stop()."""
        backoff = RETRY_BASE_S
        while not should_stop():
            try:
                await self.subscribe(since_provider(), on_event)
                backoff = RETRY_BASE_S  # clean end -> quick resume
            except ReconnectRequested:
                backoff = RETRY_BASE_S
                logger.info("[kimi-claw] server-requested reconnect")
                continue
            except AuthError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[kimi-claw] subscribe dropped: %s; retry in %.0fs",
                               exc, backoff)
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                raise
            backoff = min(backoff * 2, RETRY_MAX_S)

    # ------------------------------------------------------------------
    # Unary RPCs
    # ------------------------------------------------------------------
    async def _unary(self, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        assert self._session is not None, "call open() first"
        url = f"{self.im_base}/{IM_SERVICE}/{method}"
        async with self._session.post(
            url, json=payload, headers=self._headers(JSON_CONTENT_TYPE),
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_S),
        ) as resp:
            await self._raise_for_status(resp, method)
            return await resp.json()

    async def list_message_text(self, chat_id: str, message_id: str) -> Dict[str, Any]:
        """Fetch one message and extract text + sender info + file refs.

        Returns {"text": str, "sender_name": str|None, "sender_id": str|None,
                 "files": [{"id","name","content_type","sign_url"}]}.
        File blocks carry the kimi.file.v1.File message inline; blob.signUrl
        may be absent, in which case call get_file_metadata() for a fresh one.
        """
        data = await self._unary("ListMessages", {
            "chatId": chat_id,
            "startMessageId": message_id,
            "endMessageId": message_id,
            "includeStartMessage": True,
            "includeEndMessage": True,
            "direction": "DIRECTION_BACKWARD",
            "pageSize": 20,
            "pageToken": "",
        })
        messages = data.get("messages") or []
        if not messages:
            return {"text": "", "sender_name": None, "sender_id": None, "files": []}
        entry = messages[0]
        msg = entry.get("message") or {}
        texts, files = [], []
        for block in msg.get("blocks") or []:
            if "text" in block:
                content = (block["text"] or {}).get("content")
                if content:
                    texts.append(content)
            elif "file" in block:
                f = block["file"] or {}
                meta = f.get("meta") or {}
                blob = f.get("blob") or {}
                files.append({
                    "id": f.get("id"),
                    "name": meta.get("name") or "file",
                    "content_type": meta.get("contentType")
                    or meta.get("content_type") or "",
                    "sign_url": blob.get("signUrl") or blob.get("sign_url")
                    or blob.get("previewUrl") or blob.get("preview_url"),
                })
            elif "resourceLink" in block:
                link = block["resourceLink"] or {}
                if link.get("uri"):
                    texts.append(link["uri"])
        return {
            "text": "\n".join(texts).strip(),
            "sender_name": entry.get("senderName") or entry.get("sender_name"),
            "sender_id": entry.get("senderId") or entry.get("sender_id"),
            "files": files,
        }

    async def get_file_download_url(self, file_id: str) -> Optional[str]:
        """Resolve a fresh download URL for a kimi file id.

        GET {origin}/api-claw/files/{fileId} with the bot token; the download
        URL is blob.signUrl (fallback: image preview URL).
        """
        if not file_id:
            return None
        origin = self.im_base
        # im_base ends with /api-ws; metadata lives on the origin root.
        if "://" in origin:
            scheme, rest = origin.split("://", 1)
            origin = f"{scheme}://{rest.split('/', 1)[0]}"
        url = f"{origin}/api-claw/files/{file_id}"
        assert self._session is not None, "call open() first"
        async with self._session.get(
            url, headers={"X-Kimi-Bot-Token": self.token},
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_S),
        ) as resp:
            if resp.status != 200:
                logger.warning("[kimi-claw] file metadata %s -> HTTP %s",
                               file_id, resp.status)
                return None
            data = await resp.json()
        blob = data.get("blob") or {}
        url = blob.get("signUrl") or blob.get("sign_url")
        if not url:
            # image preview fallback (parseJob.result.image.thumbnail.previewUrl)
            try:
                url = (data["parseJob"]["result"]["image"]["thumbnail"]
                       ["previewUrl"])
            except Exception:
                url = None
        return url

    async def download_file(self, url: str, dest: "Path") -> bool:
        assert self._session is not None, "call open() first"
        try:
            async with self._session.get(
                url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_S)
            ) as resp:
                if resp.status != 200:
                    return False
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(dest, "wb") as fh:
                    async for chunk in resp.content.iter_chunked(1 << 16):
                        fh.write(chunk)
            return True
        except Exception as exc:
            logger.warning("[kimi-claw] download failed %s: %s", url[:80], exc)
            return False

    async def send_blocks(self, chat_id: str, blocks: list) -> Optional[str]:
        """Send arbitrary content blocks. Returns messageId on success."""
        data = await self._unary("SendMessage", {
            "chatId": chat_id,
            "blocks": blocks,
        })
        return data.get("messageId") or data.get("message_id")

    async def send_message(self, chat_id: str, text: str,
                           block_id: str = "b1") -> Optional[str]:
        """Send a plain-text message. Returns messageId on success."""
        return await self.send_blocks(
            chat_id, [{"id": block_id, "text": {"content": text}}])

    async def send_resource_link(self, chat_id: str, uri: str, title: str,
                                 block_id: str = "f1") -> Optional[str]:
        """Send a file card referencing a previously uploaded kimi-file:// URI."""
        return await self.send_blocks(chat_id, [{
            "id": block_id,
            "resourceLink": {"title": title, "uri": uri},
        }])

    async def update_bot_meta(self) -> None:
        """Answer botReport events (best-effort, matches upstream payload)."""
        try:
            await self._unary("UpdateBotMeta", {
                "pluginVersion": PLUGIN_VERSION,
                "openclawVersion": OPENCLAW_VERSION,
                "skills": self._skills,
                "platform": "linux",
                # Mimic the cloud-managed bot marker (server accepts this
                # field — verified 200 — though the official plugin never
                # sends it; effect on UI gating is unverified).
                "botType": "remote",
            })
        except Exception as exc:
            logger.debug("[kimi-claw] UpdateBotMeta failed (ignored): %s", exc)


class KimiStreamSender:
    """Typewriter-style streaming reply over WS /im/send-message/ws.

    Frame protocol (protojson text frames, auth via X-Kimi-Bot-Token header):
      1. first frame binds the chat: {"chatId": "..."}
      2. block updates: {"block": {"op": "append"|"set",
                          "mask": "block.text.content",
                          "block": {"id": "<seq>", "text": {"content": ...}}}}
      3. keepalive: {"ping": {}} every 10s
      4. finalize: {"end": {}} then close; a normal close = success
    """

    WS_PATH = "/im/send-message/ws"
    PING_INTERVAL_S = 10.0
    COMPLETION_TIMEOUT_S = 30.0
    IDLE_TIMEOUT_S = 180.0  # safety net if the final end never arrives

    def __init__(self, client: "KimiImClient", chat_id: str):
        self._client = client
        self.chat_id = chat_id
        self._ws = None
        self._sent_text = ""
        self._sent_think = ""
        # Block-id choreography calibrated from a cloud-instance traffic
        # capture: think lane uses "0", tool blocks count up from "1", the
        # answer text block takes the next free id once text starts.
        self._think_block_id = "0"
        self._tool_seq = 0
        self._block_id: Optional[str] = None  # allocated on first text frame
        self._frames_sent = 0
        self._ping_task: Optional[asyncio.Task] = None
        self._idle_task: Optional[asyncio.Task] = None
        self._closed = False

    def _text_block_id(self) -> str:
        if self._block_id is None:
            self._block_id = str(self._tool_seq + 1)
        return self._block_id

    async def start(self) -> None:
        session = self._client._session
        assert session is not None, "client not open"
        url = f"{self._client.im_base}{self.WS_PATH}"
        self._ws = await session.ws_connect(
            url, headers={"X-Kimi-Bot-Token": self._client.token},
            heartbeat=None,  # app-level ping frames instead
        )
        await self._ws.send_str(json.dumps({"chatId": self.chat_id}))
        self._ping_task = asyncio.create_task(self._ping_loop())
        self._idle_task = asyncio.create_task(self._idle_watchdog())

    async def update(self, content: str) -> None:
        """Send the latest full snapshot; emits append-delta when possible."""
        if self._closed or self._ws is None or self._ws.closed:
            raise StreamClosed("stream sender is closed")
        if content == self._sent_text:
            return
        block_id = self._text_block_id()
        if self._sent_text and content.startswith(self._sent_text):
            delta = content[len(self._sent_text):]
            frame = {"block": {"op": "append", "mask": "block.text.content",
                               "block": {"id": block_id,
                                         "text": {"content": delta}}}}
        elif not self._sent_text:
            frame = {"block": {"op": "append",
                               "block": {"id": block_id,
                                         "text": {"content": content}}}}
        else:
            # non-prefix change (segment break / rewrite): reconcile fully
            frame = {"block": {"op": "set", "mask": "block.text.content",
                               "block": {"id": block_id,
                                         "text": {"content": content}}}}
        await self._ws.send_str(json.dumps(frame))
        self._frames_sent += 1
        self._sent_text = content

    async def update_think(self, content: str) -> None:
        """Forward reasoning/narration as a collapsible think block
        (mask block.think.content, block id "0", "Reasoning:\n" prefix —
        all matching the captured upstream convention)."""
        if self._closed or self._ws is None or self._ws.closed:
            raise StreamClosed("stream sender is closed")
        if content and not content.startswith("Reasoning:\n"):
            content = "Reasoning:\n" + content
        if content == self._sent_think:
            return
        if self._sent_think and content.startswith(self._sent_think):
            frame = {"block": {"op": "append", "mask": "block.think.content",
                               "block": {"id": self._think_block_id,
                                         "think": {"content":
                                                   content[len(self._sent_think):]}}}}
        elif not self._sent_think:
            frame = {"block": {"op": "append",
                               "block": {"id": self._think_block_id,
                                         "think": {"content": content}}}}
        else:
            frame = {"block": {"op": "set", "mask": "block.think.content",
                               "block": {"id": self._think_block_id,
                                         "think": {"content": content}}}}
        await self._ws.send_str(json.dumps(frame))
        self._frames_sent += 1
        self._sent_think = content

    async def add_tool_block(self, tool_call_id: str, name: str,
                             args_text: str,
                             status: str = "running") -> None:
        """Emit a tool-call block (upstream forwardToolCalls equivalent).

        Calibrated against production: op "set", no mask, block ids count up
        from "1".  NOTE: ToolBlock.contents (status field) is rejected by the
        server in both lowercase and STATUS_* enum encodings (connection
        dies) — empirically only the bare toolCallId/name/args shape is
        accepted, so status transitions are not expressed.
        """
        if self._closed or self._ws is None or self._ws.closed:
            raise StreamClosed("stream sender is closed")
        if status != "running":
            return  # no safe wire encoding for status updates; skip
        self._tool_seq += 1
        block_id = str(self._tool_seq)
        tool: Dict[str, Any] = {
            "toolCallId": tool_call_id,
            "name": name,
        }
        if args_text:
            tool["args"] = args_text
        frame = {"block": {"op": "set",
                           "block": {"id": block_id, "tool": tool}}}
        await self._ws.send_str(json.dumps(frame))
        self._frames_sent += 1

    async def finish(self, final_content: Optional[str] = None) -> bool:
        """Reconcile final text (if given) and close the stream."""
        if self._closed:
            return True
        try:
            if final_content is not None and final_content != self._sent_text:
                await self.update(final_content)
            if self._ws is not None and not self._ws.closed:
                await self._ws.send_str(json.dumps({"end": {}}))
                try:
                    await asyncio.wait_for(self._ws.close(),
                                           timeout=self.COMPLETION_TIMEOUT_S)
                except asyncio.TimeoutError:
                    pass
            return True
        except Exception as exc:
            logger.warning("[kimi-claw] stream finish failed: %s", exc)
            return False
        finally:
            self._closed = True
            for task in (self._ping_task, self._idle_task):
                if task and not task.done():
                    task.cancel()

    async def abort(self) -> None:
        self._closed = True
        for task in (self._ping_task, self._idle_task):
            if task and not task.done():
                task.cancel()
        if self._ws is not None and not self._ws.closed:
            try:
                await self._ws.close()
            except Exception:
                pass

    async def _ping_loop(self) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(self.PING_INTERVAL_S)
                if self._ws is not None and not self._ws.closed:
                    await self._ws.send_str(json.dumps({"ping": {}}))
        except (asyncio.CancelledError, Exception):
            pass

    async def _idle_watchdog(self) -> None:
        try:
            await asyncio.sleep(self.IDLE_TIMEOUT_S)
            if not self._closed:
                logger.warning("[kimi-claw] stream idle timeout, finalizing")
                await self.finish()
        except asyncio.CancelledError:
            pass
