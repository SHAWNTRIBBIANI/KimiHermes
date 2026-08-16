"""Kimi Claw remote terminal bridge (agent-ws channel).

Implements the second connection of the official kimi-claw plugin:
  wss://www.kimi.com/api-claw/bots/agent-ws
    - headers: X-Kimi-Bot-Token, X-Kimi-Claw-Version
    - 15s {"type":"ping"} keepalive; text "ping" -> "pong"
    - 60s inbound-liveness watchdog -> reconnect; close 4001 / HTTP 401 = auth
      failure (no retry)
    - JSON-RPC 2.0 downstream: initialize (capabilities incl. web-ssh),
      session/* (acknowledged with -32601), plus out-of-band terminal
      envelopes {type:"terminal", terminalId, _clientId, payload}
    - terminal actions: open / input / resize / close / heartbeat
    - outbound frames: opened / stdout(event, dataBase64, seq) / exit /
      state / error

Security note: this exposes a root shell on this machine to whoever can drive
the linked Kimi bot. Controlled by platforms.kimi-claw.extra.terminal_enabled.
"""

from __future__ import annotations

import asyncio
import base64
import fcntl
import json
import logging
import os
import pty
import signal
import struct
import subprocess
import termios
import time
import uuid
from typing import Any, Dict, Optional

import aiohttp

from .kimi_client import PLUGIN_VERSION

logger = logging.getLogger(__name__)

DEFAULT_WS_URL = "wss://www.kimi.com/api-claw/bots/agent-ws"

PING_INTERVAL_S = 15.0
# The cloud never sends frames when idle (verified in production), so a
# short silence-based kill just causes a reconnect loop. 15 min is a sanity
# bound only; real failures surface as ws close/error frames.
LIVENESS_TIMEOUT_S = 900.0
RETRY_BASE_S = 1.0
RETRY_MAX_S = 120.0

MAX_SESSIONS = 10
HEARTBEAT_TIMEOUT_S = 60.0      # cloud stopped watching -> close session
IDLE_TIMEOUT_S = 12 * 3600.0    # session lifetime cap without activity
MAX_DURATION_S = 12 * 3600.0

# error codes (from upstream terminal-rpc.js / handler)
ERR_SHELL_DISABLED = -32010
ERR_TERMINAL_NOT_FOUND = -32011
ERR_TERMINAL_CLOSED = -32012
ERR_QUOTA_EXCEEDED = -32013
ERR_TERMINAL_TIMEOUT = -32014
ERR_UNKNOWN_ACTION = -32601
ERR_INVALID_PARAMS = -32602


class TerminalAuthError(Exception):
    pass


def _now_ms() -> int:
    return int(time.time() * 1000)


class PtySession:
    """One shell behind a pty."""

    def __init__(self, terminal_id: str, session_id: str, cols: int, rows: int,
                 cwd: Optional[str], shell: Optional[str],
                 on_stdout, on_exit):
        self.terminal_id = terminal_id
        self.session_id = session_id
        self.shell = shell or "/bin/bash"
        self.cwd = cwd or os.getcwd()
        self.cols = cols
        self.rows = rows
        self.state = "opening"
        self.started_at = _now_ms()
        self.last_active_at = self.started_at
        self.seq = 0
        self._on_stdout = on_stdout
        self._on_exit = on_exit
        self._proc: Optional[subprocess.Popen] = None
        self._master_fd: Optional[int] = None
        self._reader_task: Optional[asyncio.Task] = None

    def start(self) -> None:
        master, slave = pty.openpty()
        env = dict(os.environ, TERM="ansi")
        self._proc = subprocess.Popen(
            [self.shell], stdin=slave, stdout=slave, stderr=slave,
            cwd=self.cwd, env=env, preexec_fn=os.setsid, close_fds=True,
        )
        os.close(slave)
        self._master_fd = master
        self._set_winsize(self.cols, self.rows)
        self.state = "running"
        self._reader_task = asyncio.create_task(self._read_loop())

    def _set_winsize(self, cols: int, rows: int) -> None:
        if self._master_fd is None:
            return
        try:
            fcntl.ioctl(self._master_fd, termios.TIOCSWINSZ,
                        struct.pack("HHHH", rows, cols, 0, 0))
        except OSError:
            pass

    async def _read_loop(self) -> None:
        loop = asyncio.get_running_loop()
        fd = self._master_fd
        assert fd is not None and self._proc is not None
        try:
            while True:
                data = await loop.run_in_executor(None, self._read_chunk, fd)
                if not data:
                    break
                self.last_active_at = _now_ms()
                self.seq += 1
                await self._on_stdout(self, data)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("[kimi-claw] pty read loop error: %s", exc)
        finally:
            code = self._proc.poll()
            if code is None:
                try:
                    os.killpg(self._proc.pid, signal.SIGHUP)
                except OSError:
                    pass
                code = self._proc.wait()
            await self._on_exit(self, code, None, None)

    def _read_chunk(self, fd: int) -> bytes:
        """Blocking-with-poll read: returns b"" on EOF or when closing."""
        import select
        while self.state != "closed":
            try:
                ready, _, _ = select.select([fd], [], [], 0.5)
            except (OSError, ValueError):
                return b""
            if not ready:
                if self._proc is not None and self._proc.poll() is not None:
                    # process exited; drain whatever is left
                    try:
                        data = os.read(fd, 65536)
                        return data
                    except OSError:
                        return b""
                continue
            try:
                return os.read(fd, 65536)
            except OSError:
                return b""
        return b""

    def write(self, data: bytes) -> None:
        if self._master_fd is not None:
            try:
                os.write(self._master_fd, data)
                self.last_active_at = _now_ms()
            except OSError:
                pass

    def resize(self, cols: int, rows: int) -> None:
        self.cols, self.rows = cols, rows
        self._set_winsize(cols, rows)
        self.last_active_at = _now_ms()

    def touch(self) -> None:
        self.last_active_at = _now_ms()

    def close(self) -> None:
        if self.state in ("closing", "closed"):
            return
        self.state = "closing"
        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
        if self._proc and self._proc.poll() is None:
            try:
                os.killpg(self._proc.pid, signal.SIGTERM)
            except OSError:
                pass
        if self._master_fd is not None:
            try:
                os.close(self._master_fd)
            except OSError:
                pass
            self._master_fd = None
        self.state = "closed"


class AgentWsTerminalBridge:
    """agent-ws channel client + terminal envelope handler."""

    def __init__(self, token: str, ws_url: Optional[str] = None,
                 shell_enabled: bool = True,
                 instance_id: Optional[str] = None,
                 device_id: Optional[str] = None,
                 default_cwd: Optional[str] = None):
        self._token = token
        self._ws_url = (ws_url or DEFAULT_WS_URL).strip()
        self._shell_enabled = shell_enabled
        self._instance_id = instance_id or f"hermes-{uuid.uuid4().hex[:12]}"
        self._device_id = device_id or f"hermes-{uuid.uuid4().hex[:12]}"
        self._default_cwd = default_cwd
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws = None
        self._stopping = False
        self._last_inbound = time.monotonic()
        self._debug_index = 0
        self._sessions: Dict[str, PtySession] = {}
        self._client_ids: Dict[str, str] = {}  # terminalId -> _clientId
        self._ping_task: Optional[asyncio.Task] = None
        self._watchdog_task: Optional[asyncio.Task] = None
        self._reaper_task: Optional[asyncio.Task] = None
        self.auth_failed = False

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    async def run_forever(self, session: aiohttp.ClientSession,
                          should_stop) -> None:
        self._session = session
        backoff = RETRY_BASE_S
        while not should_stop():
            try:
                await self._run_once()
                backoff = RETRY_BASE_S
            except TerminalAuthError:
                self.auth_failed = True
                logger.error("[kimi-claw] agent-ws auth failed; not retrying")
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[kimi-claw] agent-ws dropped: %s; retry %.0fs",
                               exc, backoff)
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                raise
            backoff = min(backoff * 2, RETRY_MAX_S)
        await self._close_all_sessions()

    async def stop(self) -> None:
        self._stopping = True
        for task in (self._ping_task, self._watchdog_task, self._reaper_task):
            if task and not task.done():
                task.cancel()
        if self._ws is not None and not self._ws.closed:
            try:
                await self._ws.close()
            except Exception:
                pass
        await self._close_all_sessions()

    async def _run_once(self) -> None:
        assert self._session is not None
        headers = {
            "X-Kimi-Bot-Token": self._token,
            "X-Kimi-Claw-Version": PLUGIN_VERSION,
        }
        async with self._session.ws_connect(
            self._ws_url, headers=headers, heartbeat=None,
        ) as ws:
            self._ws = ws
            self._last_inbound = time.monotonic()
            self._ping_task = asyncio.create_task(self._ping_loop())
            self._watchdog_task = asyncio.create_task(self._liveness_loop())
            self._reaper_task = asyncio.create_task(self._reaper_loop())
            logger.info("[kimi-claw] agent-ws connected (terminal %s)",
                        "enabled" if self._shell_enabled else "disabled")
            try:
                async for msg in ws:
                    self._last_inbound = time.monotonic()
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._on_text(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        raise ConnectionError(f"ws error: {ws.exception()}")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                      aiohttp.WSMsgType.CLOSE):
                        break
            finally:
                for task in (self._ping_task, self._watchdog_task,
                             self._reaper_task):
                    if task and not task.done():
                        task.cancel()
                self._ws = None
            close_code = ws.close_code
            if close_code == 4001:
                raise TerminalAuthError("agent-ws closed with 4001")
            raise ConnectionError(f"agent-ws closed (code={close_code})")

    # ------------------------------------------------------------------
    # keepalive
    # ------------------------------------------------------------------
    async def _ping_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL_S)
                if self._ws is not None and not self._ws.closed:
                    await self._ws.send_str(json.dumps({"type": "ping"}))
        except (asyncio.CancelledError, Exception):
            pass

    async def _liveness_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(10)
                if time.monotonic() - self._last_inbound > LIVENESS_TIMEOUT_S:
                    logger.warning("[kimi-claw] agent-ws liveness timeout")
                    if self._ws is not None:
                        await self._ws.close(code=1001)
                    return
        except asyncio.CancelledError:
            pass

    async def _reaper_loop(self) -> None:
        """Enforce heartbeat timeout (cloud stopped watching) and lifetime."""
        try:
            while True:
                await asyncio.sleep(15)
                now = time.time()
                for tid, sess in list(self._sessions.items()):
                    idle = now - sess.last_active_at / 1000.0
                    alive_for = (now * 1000 - sess.started_at) / 1000.0
                    if idle > HEARTBEAT_TIMEOUT_S and \
                            alive_for < MAX_DURATION_S:
                        # heartbeat timeout is about cloud attention; the
                        # session's own last_active doubles as activity mark
                        reason = "idle_timeout"
                    elif alive_for >= MAX_DURATION_S:
                        reason = "max_duration_timeout"
                    else:
                        continue
                    logger.info("[kimi-claw] terminal %s closing: %s",
                                tid, reason)
                    await self._close_session(tid, reason=reason)
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # inbound dispatch
    # ------------------------------------------------------------------
    async def _on_text(self, data: str) -> None:
        if data == "ping":
            await self._send_raw("pong")
            return
        if data == "pong":
            return
        try:
            frame = json.loads(data)
        except Exception:
            return
        if not isinstance(frame, dict):
            return

        if frame.get("type") == "terminal":
            await self._on_terminal(frame)
            return

        # JSON-RPC request from the cloud
        method = frame.get("method")
        req_id = frame.get("id")
        if method is None:
            return
        logger.info("[kimi-claw] agent-ws rpc method=%s id=%s", method, req_id)
        if method == "initialize":
            await self._reply(req_id, {
                "protocolVersion": 1,
                "agentCapabilities": {
                    "loadSession": True,
                    "promptCapabilities": {
                        "embeddedContext": True,
                        "image": True,
                        "audio": False,
                    },
                    "sessionCapabilities": {
                        "list": {},
                        "web-ssh": bool(self._shell_enabled),
                    },
                },
                "agentInfo": {"name": "kimi-claw", "version": PLUGIN_VERSION},
                "_meta": {
                    "instanceId": self._instance_id,
                    "deviceId": self._device_id,
                },
            })
        elif method == "session/new":
            await self._reply(req_id, {
                "sessionId": f"agentws-{uuid.uuid4().hex[:12]}",
                "modes": {
                    "availableModes": [{"id": "default", "name": "default"}],
                    "currentModeId": "default",
                },
            })
        else:
            await self._reply_error(req_id, ERR_UNKNOWN_ACTION,
                                    f"unsupported method: {method}")

    # ------------------------------------------------------------------
    # JSON-RPC helpers
    # ------------------------------------------------------------------
    async def _send_raw(self, text: str) -> None:
        if self._ws is not None and not self._ws.closed:
            await self._ws.send_str(text)

    async def _reply(self, req_id, result: Dict[str, Any]) -> None:
        self._debug_index += 1
        result = dict(result)
        meta = dict(result.get("_meta") or {})
        meta.setdefault("requestId", req_id)
        meta["_debug_index"] = self._debug_index
        result["_meta"] = meta
        await self._send_raw(json.dumps({
            "jsonrpc": "2.0", "id": req_id, "result": result,
        }))

    async def _reply_error(self, req_id, code: int, message: str) -> None:
        self._debug_index += 1
        await self._send_raw(json.dumps({
            "jsonrpc": "2.0", "id": req_id,
            "error": {"code": code, "message": message,
                      "data": {"_meta": {"requestId": req_id,
                                         "_debug_index": self._debug_index}}},
        }))

    # ------------------------------------------------------------------
    # terminal envelopes
    # ------------------------------------------------------------------
    @staticmethod
    def _payload_action(payload: Dict[str, Any]) -> Optional[str]:
        action = payload.get("action") or payload.get("event")
        if not action:
            t = str(payload.get("type") or "").lower()
            action = {
                "start": "open", "open": "open",
                "stdin": "input", "input": "input",
                "resize": "resize",
                "stop": "close", "close": "close",
                "heartbeat": "heartbeat", "ping": "heartbeat",
            }.get(t, t or None)
        return action

    async def _on_terminal(self, frame: Dict[str, Any]) -> None:
        terminal_id = str(frame.get("terminalId") or "")
        client_id = str(frame.get("_clientId") or "")
        payload = frame.get("payload")
        if not terminal_id or not isinstance(payload, dict):
            return
        if client_id:
            self._client_ids[terminal_id] = client_id

        action = self._payload_action(payload)
        logger.info("[kimi-claw] terminal %s action=%s", terminal_id, action)

        if not self._shell_enabled:
            await self._send_terminal_error(terminal_id, ERR_SHELL_DISABLED,
                                            "shell disabled")
            return

        try:
            if action == "open":
                await self._t_open(terminal_id, payload)
            elif action == "input":
                await self._t_input(terminal_id, payload)
            elif action == "resize":
                await self._t_resize(terminal_id, payload)
            elif action == "close":
                await self._close_session(terminal_id, reason=None)
            elif action == "heartbeat":
                sess = self._sessions.get(terminal_id)
                if sess:
                    sess.touch()
            else:
                await self._send_terminal_error(
                    terminal_id, ERR_UNKNOWN_ACTION,
                    f"unknown action: {action}")
        except Exception as exc:
            logger.exception("[kimi-claw] terminal action %s failed", action)
            await self._send_terminal_error(terminal_id, -32000, str(exc))

    @staticmethod
    def _open_params(payload: Dict[str, Any]) -> Dict[str, Any]:
        params = dict(payload)
        content = payload.get("content")
        if isinstance(content, dict):
            params.update(content)
        return params

    async def _t_open(self, terminal_id: str, payload: Dict[str, Any]) -> None:
        if terminal_id in self._sessions:
            await self._send_terminal_error(terminal_id, ERR_TERMINAL_CLOSED,
                                            "terminal already exists")
            return
        params = self._open_params(payload)
        cols, rows = params.get("cols"), params.get("rows")
        if not (isinstance(cols, int) and cols > 0
                and isinstance(rows, int) and rows > 0):
            await self._send_terminal_error(terminal_id, ERR_INVALID_PARAMS,
                                            "cols/rows must be positive ints")
            return
        if len(self._sessions) >= MAX_SESSIONS:
            # evict oldest by lastActiveAt
            oldest = min(self._sessions.values(),
                         key=lambda s: s.last_active_at)
            await self._close_session(oldest.terminal_id, reason=None)
            if len(self._sessions) >= MAX_SESSIONS:
                await self._send_terminal_error(
                    terminal_id, ERR_QUOTA_EXCEEDED, "terminal quota exceeded")
                return
        session_id = str(params.get("sessionId") or terminal_id)
        sess = PtySession(
            terminal_id, session_id, cols, rows,
            cwd=params.get("cwd") or self._default_cwd,
            shell=params.get("shell"),
            on_stdout=self._on_pty_stdout,
            on_exit=self._on_pty_exit,
        )
        try:
            sess.start()
        except Exception as exc:
            await self._send_terminal_error(terminal_id, -32000,
                                            f"spawn failed: {exc}")
            return
        self._sessions[terminal_id] = sess
        await self._send_terminal(terminal_id, {
            "action": "opened",
            "terminalId": terminal_id,
            "sessionId": session_id,
            "shell": sess.shell,
            "cwd": sess.cwd,
            "cols": cols,
            "rows": rows,
            "state": sess.state,
            "startedAt": sess.started_at,
            "lastActiveAt": sess.last_active_at,
        })

    async def _t_input(self, terminal_id: str, payload: Dict[str, Any]) -> None:
        sess = self._sessions.get(terminal_id)
        if not sess:
            await self._send_terminal_error(terminal_id,
                                            ERR_TERMINAL_NOT_FOUND,
                                            "terminal not found")
            return
        data_b64 = payload.get("dataBase64") or payload.get("contentBase64")
        if data_b64:
            try:
                data = base64.b64decode(data_b64)
            except Exception:
                await self._send_terminal_error(terminal_id,
                                                ERR_INVALID_PARAMS,
                                                "invalid base64")
                return
        else:
            content = payload.get("content")
            if not isinstance(content, str):
                await self._send_terminal_error(
                    terminal_id, ERR_INVALID_PARAMS,
                    "dataBase64 or content required")
                return
            data = content.encode("utf-8")
        sess.write(data)

    async def _t_resize(self, terminal_id: str, payload: Dict[str, Any]) -> None:
        sess = self._sessions.get(terminal_id)
        if not sess:
            await self._send_terminal_error(terminal_id,
                                            ERR_TERMINAL_NOT_FOUND,
                                            "terminal not found")
            return
        cols, rows = payload.get("cols"), payload.get("rows")
        if not (isinstance(cols, int) and cols > 0
                and isinstance(rows, int) and rows > 0):
            await self._send_terminal_error(terminal_id, ERR_INVALID_PARAMS,
                                            "cols/rows must be positive ints")
            return
        sess.resize(cols, rows)

    # ------------------------------------------------------------------
    # outbound terminal frames
    # ------------------------------------------------------------------
    async def _send_terminal(self, terminal_id: str,
                             payload: Dict[str, Any]) -> None:
        frame = {
            "type": "terminal",
            "terminalId": terminal_id,
            "_clientId": self._client_ids.get(terminal_id, ""),
            "payload": payload,
        }
        await self._send_raw(json.dumps(frame))

    async def _send_terminal_error(self, terminal_id: str, code: int,
                                   message: str) -> None:
        await self._send_terminal(terminal_id, {
            "action": "error", "code": code, "message": message,
        })

    async def _on_pty_stdout(self, sess: PtySession, data: bytes) -> None:
        await self._send_terminal(sess.terminal_id, {
            "event": "stdout",
            "dataBase64": base64.b64encode(data).decode("ascii"),
            "seq": sess.seq,
        })

    async def _on_pty_exit(self, sess: PtySession, code, sig,
                           reason) -> None:
        self._sessions.pop(sess.terminal_id, None)
        payload: Dict[str, Any] = {
            "event": "exit",
            "code": code,
            "signal": sig,
        }
        if reason:
            payload["reason"] = reason
        await self._send_terminal(sess.terminal_id, payload)

    async def _close_session(self, terminal_id: str,
                             reason: Optional[str]) -> None:
        sess = self._sessions.pop(terminal_id, None)
        if not sess:
            return
        sess.close()
        payload: Dict[str, Any] = {
            "event": "exit", "code": None, "signal": "SIGHUP",
        }
        if reason:
            payload["reason"] = reason
        await self._send_terminal(terminal_id, payload)
        self._client_ids.pop(terminal_id, None)

    async def _close_all_sessions(self) -> None:
        for tid in list(self._sessions):
            try:
                await self._close_session(tid, reason=None)
            except Exception:
                pass
