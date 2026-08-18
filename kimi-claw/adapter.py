"""Kimi Claw platform adapter for Hermes Agent.

Bridges kimi.com (Kimi Claw) conversations into the Hermes gateway session,
mirroring the official kimi-claw OpenClaw plugin's channel behavior.

Inbound : Subscribe event stream -> ListMessages -> MessageEvent -> handle_message
Outbound: send() -> SendMessage RPC
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.config import Platform

from .kimi_client import (
    AuthError,
    KimiImClient,
    KimiStreamSender,
    DEFAULT_KIMIAPI_HOST,
)
from .terminal_bridge import AgentWsTerminalBridge

logger = logging.getLogger(__name__)

STATE_DIR = Path.home() / ".hermes" / "state" / "kimi-claw"
STATE_FILE = STATE_DIR / "bridge_state.json"
ATTACH_DIR = Path.home() / ".hermes" / "attachments" / "kimi-claw"
SKILLS_DIR = Path.home() / ".hermes" / "skills"


def _collect_skill_names() -> list:
    """Hermes skills (category/<name>/SKILL.md) reported to Kimi cloud as the
    equivalent of the upstream X-Kimi-OpenClaw-Skills metadata."""
    try:
        return sorted(p.name for p in SKILLS_DIR.glob("*/*")
                      if (p / "SKILL.md").exists())
    except Exception:
        return []


async def send_kimi_claw_direct(*, extra: Dict[str, Any],
                                token: Optional[str], chat_id: str,
                                message: str,
                                media_files: Optional[list] = None
                                ) -> Dict[str, Any]:
    """One-shot send for cron delivery / send_message outside the gateway.

    Follows the standalone_sender_fn contract (same shape as
    send_weixin_direct).
    """
    from .kimi_client import KimiImClient, DEFAULT_KIMIAPI_HOST as _H

    bot_token = (token or os.getenv("KIMI_CLAW_BOT_TOKEN") or "").strip()
    if not bot_token:
        return {"success": False, "error": "KIMI_CLAW_BOT_TOKEN not set"}
    host = (extra or {}).get("kimiapi_host") or _H
    client = KimiImClient(bot_token, host, skills=_collect_skill_names())
    await client.open()
    try:
        mid = await client.send_message(chat_id, message)
        return {"success": True, "message_id": mid}
    except Exception as exc:
        return {"success": False, "error": str(exc)}
    finally:
        await client.close()

MAX_MESSAGE_LENGTH = 4000  # conservative chunk size for outbound text

# The live adapter instance, set at connect().  Lets process-wide plugin
# hooks (pre_tool_call) reach the active draft stream; hooks fire on agent
# worker threads, so all WS sends hop through the gateway loop.
_ACTIVE_ADAPTER: Optional["KimiClawAdapter"] = None


def pre_tool_call_hook(tool_name: str = "", args: Optional[Dict] = None,
                       tool_call_id: str = "", **_kw):
    """Observer hook: mirror tool calls into the Kimi draft stream as native
    tool blocks (forwardToolCalls).  Returns None — never blocks/approves."""
    adapter = _ACTIVE_ADAPTER
    if adapter is None or not adapter._forward_tool_calls:
        return None
    if tool_name in KimiClawAdapter._META_TOOL_CHROME:
        return None
    chat_id = adapter._last_chat_id or adapter._default_chat_id
    stream = adapter._streams.get(chat_id) if chat_id else None
    loop = adapter._loop
    if stream is None or stream._closed or loop is None:
        return None
    try:
        args_text = json.dumps(args or {}, ensure_ascii=False)[:1500]
    except Exception:
        args_text = ""
    try:
        asyncio.run_coroutine_threadsafe(
            stream.add_tool_block(tool_call_id or "0", tool_name, args_text,
                                  "running"),
            loop,
        )
    except Exception:
        pass
    return None


class KimiClawAdapter(BasePlatformAdapter):
    """Instantiated by the adapter_factory passed to register_platform()."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    # NOTE: must be True even though Kimi messages can't actually be edited.
    # gateway/run.py._build_stream_consumer_config skips the ENTIRE stream
    # consumer for adapters with SUPPORTS_MESSAGE_EDITING=False (designed for
    # QQ/WeChat where a partial first message could never be updated).  Our
    # draft path (WS stream + finalizing send) doesn't produce that
    # partial+final duplication, so we opt in and provide a graceful
    # edit_message failure for the never-should-happen edit fallback.
    SUPPORTS_MESSAGE_EDITING = True
    supports_code_blocks = True
    splits_long_messages = False  # we chunk ourselves in send()

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform("kimi-claw"))

        extra = getattr(config, "extra", {}) or {}
        self._token = (
            getattr(config, "token", None)
            or getattr(config, "api_key", None)
            or extra.get("bot_token")
            or os.getenv("KIMI_CLAW_BOT_TOKEN")
            or ""
        ).strip()
        self._kimiapi_host = (
            os.getenv("KIMI_CLAW_KIMIAPI_HOST")
            or extra.get("kimiapi_host")
            or DEFAULT_KIMIAPI_HOST
        )

        # Access policy: the bot token itself scopes access to the linked Kimi
        # account, so mirror the upstream channel's open DM policy.
        self._dm_policy = str(extra.get("dm_policy", "open")).lower()
        self._group_policy = str(extra.get("group_policy", "open")).lower()

        self._client: Optional[KimiImClient] = None
        self._subscribe_task: Optional[asyncio.Task] = None
        self._stopping = False
        self._streams: Dict[str, KimiStreamSender] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Remote terminal (agent-ws channel). Default ON per operator request;
        # set platforms.kimi-claw.extra.terminal_enabled: false to disable.
        # WARNING: this exposes a shell on this machine to the linked Kimi
        # account's UI.
        self._terminal_enabled = bool(extra.get("terminal_enabled", True))
        self._terminal_ws_url = (
            os.getenv("KIMI_CLAW_WS_URL") or extra.get("ws_url") or None
        )
        self._terminal_bridge: Optional[AgentWsTerminalBridge] = None
        self._terminal_task: Optional[asyncio.Task] = None

        # forwardThinking / forwardToolCalls equivalents: stream reasoning
        # narration as think blocks and tool calls as tool blocks. True
        # model reasoning is filtered upstream of Hermes adapters, so the
        # think block carries Hermes Commentary (interim narration).
        self._forward_thinking = bool(extra.get("forward_thinking", True))
        self._forward_tool_calls = bool(extra.get("forward_tool_calls", True))
        self._think_buffers: Dict[str, str] = {}
        # Last full text delivered per chat — lets edit_message answer the
        # gateway's stale-finalize reconciliation truthfully (no-op when the
        # message already says exactly this, avoiding duplicate resends).
        self._last_delivered: Dict[str, str] = {}

        # Resume / dedup state
        self._last_event_id: Optional[str] = None
        self._seen_message_ids: set[str] = set()
        self._default_chat_id: Optional[str] = None
        self._last_chat_id: Optional[str] = None
        self._instance_id: Optional[str] = None
        self._device_id: Optional[str] = None
        # Our own bot ids (long + short), resolved via GetMe at connect.
        # The loop guard matches on sender identity, NOT role: in group
        # rooms the human coordinator's messages arrive with role=assistant,
        # so a role-based filter silently drops legitimate group traffic.
        self._self_ids: set[str] = set()

    # ------------------------------------------------------------------
    # state persistence (resume via sinceId across restarts)
    # ------------------------------------------------------------------
    def _load_state(self) -> None:
        try:
            data = json.loads(STATE_FILE.read_text())
            self._last_event_id = data.get("last_event_id")
            self._default_chat_id = data.get("default_chat_id")
            self._last_chat_id = data.get("last_chat_id")
            self._instance_id = data.get("instance_id")
            self._device_id = data.get("device_id")
        except Exception:
            pass

    def _save_state(self) -> None:
        try:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            STATE_FILE.write_text(json.dumps({
                "last_event_id": self._last_event_id,
                "default_chat_id": self._default_chat_id
                or (self._client.default_chat_id if self._client else None),
                "last_chat_id": self._last_chat_id,
                "instance_id": self._instance_id,
                "device_id": self._device_id,
            }))
        except Exception as exc:
            logger.debug("[kimi-claw] state save failed: %s", exc)

    # ------------------------------------------------------------------
    # BasePlatformAdapter interface
    # ------------------------------------------------------------------
    async def connect(self, *, is_reconnect: bool = False) -> bool:
        global _ACTIVE_ADAPTER
        if not self._token:
            logger.error("[kimi-claw] no bot token configured "
                         "(KIMI_CLAW_BOT_TOKEN); platform cannot start")
            return False
        _ACTIVE_ADAPTER = self
        self._stopping = False
        self._loop = asyncio.get_running_loop()
        self._load_state()
        skills = _collect_skill_names()
        self._client = KimiImClient(self._token, self._kimiapi_host,
                                    skills=skills)
        await self._client.open()
        me = await self._client.get_me()
        if me:
            self._self_ids = {v for v in (me.get("id"), me.get("shortId")) if v}
            logger.info("[kimi-claw] bot identity: %s (%s)",
                        me.get("name"), me.get("shortId"))
        self._subscribe_task = asyncio.create_task(
            self._subscribe_loop(), name="kimi-claw-subscribe")
        if self._terminal_enabled:
            if not self._instance_id:
                self._instance_id = f"hermes-{uuid.uuid4().hex[:12]}"
                self._device_id = f"hermes-{uuid.uuid4().hex[:12]}"
            self._terminal_bridge = AgentWsTerminalBridge(
                self._token,
                ws_url=self._terminal_ws_url,
                shell_enabled=True,
                instance_id=self._instance_id,
                device_id=self._device_id,
                default_cwd=str(Path.home() / ".hermes" / "workspace"),
            )
            self._terminal_task = asyncio.create_task(
                self._terminal_bridge.run_forever(
                    self._client._session,
                    should_stop=lambda: self._stopping),
                name="kimi-claw-terminal",
            )
            logger.warning("[kimi-claw] remote terminal ENABLED — a shell on "
                           "this machine is exposed to the linked Kimi UI")
        self._mark_connected()
        logger.info("[kimi-claw] adapter connected (reconnect=%s)", is_reconnect)
        return True

    async def disconnect(self) -> None:
        global _ACTIVE_ADAPTER
        if _ACTIVE_ADAPTER is self:
            _ACTIVE_ADAPTER = None
        self._stopping = True
        for stream in self._streams.values():
            await stream.abort()
        self._streams.clear()
        if self._terminal_bridge is not None:
            try:
                await self._terminal_bridge.stop()
            except Exception:
                pass
        if self._terminal_task and not self._terminal_task.done():
            self._terminal_task.cancel()
            try:
                await self._terminal_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._subscribe_task and not self._subscribe_task.done():
            self._subscribe_task.cancel()
            try:
                await self._subscribe_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._client:
            await self._client.close()
        self._save_state()
        logger.info("[kimi-claw] adapter disconnected")

    async def send(self, chat_id: str, content: str,
                   reply_to: Optional[str] = None,
                   metadata: Optional[Dict[str, Any]] = None) -> SendResult:
        if not self._client:
            return SendResult(success=False, error="not connected", retryable=True)
        target = chat_id or self._default_chat_id
        if not target:
            return SendResult(success=False, error="chat_id unavailable",
                              retryable=False)
        # If a draft stream is open for this chat, this send() is the final
        # frame of a streamed response: reconcile + close instead of sending
        # a duplicate message.
        stream = self._streams.get(target)
        if stream is not None and content.startswith("💬 "):
            # thinking_progress relay (assistant scratch text between tool
            # calls) — forward as a think block, never as a message, and
            # crucially do NOT let it finalize the stream.
            asyncio.create_task(
                self._safe_think_update(target, stream, content[2:].strip()))
            return SendResult(success=True)
        stream = self._streams.pop(target, None)
        if stream is not None:
            self._think_buffers.pop(target, None)
            logger.info("[kimi-claw] finalizing draft stream chat=%s "
                        "frames=%d", target, stream._frames_sent)
            ok = await stream.finish(final_content=content)
            if ok:
                self._last_delivered[target] = content
            return SendResult(success=ok,
                              error=None if ok else "stream finish failed",
                              retryable=not ok)
        last_id = None
        try:
            for chunk in self._chunk(content):
                last_id = await self._client.send_message(target, chunk)
            self._last_delivered[target] = content
            return SendResult(success=True, message_id=last_id)
        except AuthError as exc:
            return SendResult(success=False, error=str(exc), retryable=False)
        except Exception as exc:
            logger.warning("[kimi-claw] send failed: %s", exc)
            return SendResult(success=False, error=str(exc), retryable=True)

    # ------------------------------------------------------------------
    # streaming drafts (typewriter via WS send-message-stream)
    # ------------------------------------------------------------------
    def supports_draft_streaming(self, chat_type=None, metadata=None) -> bool:
        logger.info("[kimi-claw] draft-streaming probe chat_type=%s -> True",
                    chat_type)
        return True

    async def send_draft(self, chat_id: str, draft_id: int, content: str,
                         metadata: Optional[Dict[str, Any]] = None) -> SendResult:
        if not self._client:
            return SendResult(success=False, error="not connected")
        target = chat_id or self._default_chat_id
        if not target:
            return SendResult(success=False, error="chat_id unavailable")
        stream = self._streams.get(target)
        if stream is not None and (stream._draft_id != draft_id
                                   or stream._closed):
            # a new response started before the old one finalized, or the
            # previous stream already closed (e.g. early segment finalize)
            await stream.abort()
            stream = None
        if stream is None:
            stream = KimiStreamSender(self._client, target)
            stream._draft_id = draft_id
            try:
                await stream.start()
            except Exception as exc:
                logger.warning("[kimi-claw] stream start failed: %s", exc)
                return SendResult(success=False, error=str(exc))
            self._streams[target] = stream
            logger.info("[kimi-claw] draft stream started chat=%s draft=%s",
                        target, draft_id)
        try:
            await stream.update(content)
            return SendResult(success=True)
        except Exception as exc:
            self._streams.pop(target, None)
            await stream.abort()
            return SendResult(success=False, error=str(exc))

    # ------------------------------------------------------------------
    # forwardThinking / forwardToolCalls (stream event interception)
    # ------------------------------------------------------------------
    def render_message_event(self, event: Any, sink: Any) -> None:
        from gateway.stream_events import Commentary

        if isinstance(event, Commentary) and self._forward_thinking:
            text = event.text or ""
            chat_id = getattr(sink, "chat_id", None)
            stream = self._streams.get(chat_id) if chat_id else None
            if text and stream is not None:
                buf = (self._think_buffers.get(chat_id, "") + text).strip()
                self._think_buffers[chat_id] = buf
                asyncio.create_task(self._safe_think_update(chat_id, stream,
                                                            buf))
                return  # rendered as think block, eaten from main text
            # no active stream: fall through so narration isn't lost
        super().render_message_event(event, sink)

    # Hermes-internal housekeeping tools (progressive tool disclosure etc.).
    # They are an implementation detail of the agent runtime, never something
    # the end user should see — eat their chrome on this platform.
    _META_TOOL_CHROME = frozenset({
        "tool_describe", "skills_list", "skill_view",
    })

    def format_tool_event(self, event: Any, *, mode: str = "all",
                          preview_max_len: int = 40) -> Optional[str]:
        from gateway.stream_events import ToolCallChunk, ToolCallFinished

        if getattr(event, "tool_name", None) in self._META_TOOL_CHROME:
            return None

        if self._forward_tool_calls:
            chat_id = self._last_chat_id or self._default_chat_id
            stream = self._streams.get(chat_id) if chat_id else None
            if stream is not None:
                if isinstance(event, ToolCallChunk):
                    args_text = ""
                    if event.args:
                        try:
                            args_text = json.dumps(event.args,
                                                   ensure_ascii=False)
                        except Exception:
                            args_text = str(event.args)
                    elif event.preview:
                        args_text = event.preview
                    asyncio.create_task(self._safe_tool_block(
                        stream, str(event.index), event.tool_name,
                        args_text, "running"))
                    return None  # rendered as tool block, no chrome text
                if isinstance(event, ToolCallFinished):
                    asyncio.create_task(self._safe_tool_block(
                        stream, str(event.index), event.tool_name, "",
                        "done" if event.ok else "error"))
                    return None
        return super().format_tool_event(event, mode=mode,
                                         preview_max_len=preview_max_len)

    async def _safe_think_update(self, chat_id: str,
                                 stream: KimiStreamSender, buf: str) -> None:
        try:
            await stream.update_think(buf)
        except Exception as exc:
            logger.debug("[kimi-claw] think block update failed: %s", exc)

    async def _safe_tool_block(self, stream: KimiStreamSender, call_id: str,
                               name: str, args_text: str,
                               status: str = "running") -> None:
        try:
            await stream.add_tool_block(call_id, name, args_text, status)
        except Exception as exc:
            logger.debug("[kimi-claw] tool block failed: %s", exc)

    # ------------------------------------------------------------------
    # inbound
    # ------------------------------------------------------------------
    async def _subscribe_loop(self) -> None:
        assert self._client is not None
        try:
            await self._client.subscribe_forever(
                since_provider=lambda: self._last_event_id,
                on_event=self._on_event,
                should_stop=lambda: self._stopping,
            )
        except AuthError as exc:
            logger.error("[kimi-claw] %s — check KIMI_CLAW_BOT_TOKEN", exc)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[kimi-claw] subscribe loop crashed")

    async def _on_event(self, event: Dict[str, Any]) -> None:
        event_id = event.get("id")
        if event_id:
            self._last_event_id = event_id

        if "ping" in event:
            return
        # Raw visibility for protocol edge cases (group rooms etc.)
        logger.info("[kimi-claw] event: %s",
                    json.dumps(event, ensure_ascii=False)[:600])
        if "botReport" in event:
            await self._client.update_bot_meta()
            return

        chat_msg = event.get("chatMessage")
        if not chat_msg:
            return
        if chat_msg.get("status") != "STATUS_COMPLETED":
            logger.info("[kimi-claw] skip chatMessage status=%s",
                        chat_msg.get("status"))
            return
        # Loop guard by sender identity, not role: group coordinators post
        # as role=assistant, so role filtering drops real user traffic.
        sender = chat_msg.get("senderId") or ""
        sender_short = chat_msg.get("senderShortId") or ""
        if self._self_ids and (sender in self._self_ids
                               or sender_short in self._self_ids):
            return  # our own message echoed back
        if not self._self_ids and chat_msg.get("role") != "user":
            # GetMe unavailable: conservative fallback (DM behavior)
            logger.info("[kimi-claw] skip chatMessage role=%s (no self ids)",
                        chat_msg.get("role"))
            return
        if chat_msg.get("role") not in ("user", "assistant"):
            logger.info("[kimi-claw] skip chatMessage role=%s",
                        chat_msg.get("role"))
            return

        chat_id = chat_msg.get("chatId")
        message_id = chat_msg.get("messageId")
        room_id = chat_msg.get("roomId") or None
        room_type = chat_msg.get("roomType")
        if not chat_id or not message_id:
            return
        if message_id in self._seen_message_ids:
            return

        try:
            fetched = await self._client.list_message_text(chat_id, message_id)
        except Exception as exc:
            logger.warning("[kimi-claw] failed to fetch message %s: %s",
                           message_id, exc)
            return

        text = fetched["text"]
        media_urls: list[str] = []
        media_types: list[str] = []
        failed_files: list[str] = []
        for finfo in fetched["files"]:
            local = await self._fetch_kimi_file(finfo, message_id)
            if local:
                media_urls.append(str(local))
                media_types.append(finfo.get("content_type") or "")
            else:
                failed_files.append(finfo.get("name") or "file")
        if failed_files:
            note = "\n".join(f"[文件下载失败: {name}]" for name in failed_files)
            text = f"{text}\n{note}".strip() if text else note
        if not text and not media_urls:
            return

        # Message type: photo when all media are images, document otherwise.
        if media_urls and all(t.startswith("image/") for t in media_types):
            msg_type = MessageType.PHOTO
        elif media_urls:
            msg_type = MessageType.DOCUMENT
        else:
            msg_type = MessageType.TEXT

        self._seen_message_ids.add(message_id)
        if len(self._seen_message_ids) > 500:
            self._seen_message_ids = set(list(self._seen_message_ids)[-250:])
        self._last_chat_id = str(chat_id)
        self._save_state()

        is_group = room_type == "GROUP" or room_type == "ROOM_TYPE_GROUP"
        sender_id = chat_msg.get("senderId") or fetched["sender_id"]
        sender_name = fetched["sender_name"] or chat_msg.get("senderShortId")

        source = self.build_source(
            chat_id=str(chat_id),
            chat_name=None,
            chat_type="group" if is_group else "dm",
            user_id=str(sender_id) if sender_id else None,
            user_name=sender_name,
            scope_id=str(room_id) if room_id else None,
            message_id=str(message_id),
        )
        event_obj = MessageEvent(
            text=text,
            message_type=msg_type,
            user_id=str(sender_id) if sender_id else None,
            user_name=sender_name,
            source=source,
            raw_message=chat_msg,
            message_id=str(message_id),
            media_urls=media_urls,
            media_types=media_types,
        )
        logger.info("[kimi-claw] inbound message chat=%s user=%s len=%d media=%d",
                    chat_id, sender_id, len(text), len(media_urls))
        await self.handle_message(event_obj)

    # ------------------------------------------------------------------
    # media download
    # ------------------------------------------------------------------
    async def _fetch_kimi_file(self, finfo: Dict[str, Any],
                               message_id: str) -> Optional[Path]:
        """Download an inbound file block to the local attachments dir."""
        if not self._client:
            return None
        url = finfo.get("sign_url")
        if not url:
            try:
                url = await self._client.get_file_download_url(finfo.get("id"))
            except Exception as exc:
                logger.warning("[kimi-claw] file metadata failed: %s", exc)
                return None
        if not url:
            return None
        name = re.sub(r"[^\w.\-]+", "_", str(finfo.get("name") or "file"))[:120]
        dest = ATTACH_DIR / f"{message_id}_{name}"
        ok = await self._client.download_file(url, dest)
        return dest if ok else None

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "dm"}

    async def edit_message(self, chat_id: str, message_id: str,
                           content: str, **kwargs) -> SendResult:
        # Kimi IM has no edit API.  The gateway's stale-finalize
        # reconciliation tries to edit the delivered message up to the
        # complete response; when what it wants the message to say is
        # exactly what we already delivered, report success (truthful
        # no-op) so it does NOT resend the whole response as a duplicate.
        # Only when the content genuinely differs do we fail — the
        # gateway then correctly resends the complete response fresh.
        last = self._last_delivered.get(chat_id)
        if last is not None and last.strip() == (content or "").strip():
            return SendResult(success=True, message_id=message_id)
        return SendResult(success=False,
                          error="kimi-claw does not support message editing",
                          retryable=False)

    @staticmethod
    def _chunk(text: str) -> list[str]:
        if len(text) <= MAX_MESSAGE_LENGTH:
            return [text]
        chunks, current = [], []
        current_len = 0
        for line in text.splitlines(keepends=True):
            if current_len + len(line) > MAX_MESSAGE_LENGTH and current:
                chunks.append("".join(current))
                current, current_len = [], 0
            current.append(line)
            current_len += len(line)
        if current:
            chunks.append("".join(current))
        # hard-split any still-oversized chunk
        final = []
        for c in chunks:
            while len(c) > MAX_MESSAGE_LENGTH:
                final.append(c[:MAX_MESSAGE_LENGTH])
                c = c[MAX_MESSAGE_LENGTH:]
            final.append(c)
        return final
