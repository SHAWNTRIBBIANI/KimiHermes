"""kimi_upload_file tool — upload local files into the current Kimi conversation.

Ported from kimi-claw plugin `dist/src/kimi-upload-tool.js` (v0.27.1):
  POST {kimiapiHost}/files:upload
  Header: X-Kimi-Bot-Token: <bot token>
  Body: multipart/form-data, single field "file"
  Response: {"file": {"id": ..., "meta": {"name": ..., "contentType": ...}}}
"""

from __future__ import annotations

import logging
import mimetypes
import os
from pathlib import Path

import aiohttp

logger = logging.getLogger(__name__)

TOOL_NAME = "kimi_upload_file"
DEFAULT_KIMIAPI_HOST = "https://www.kimi.com/api-claw"
UPLOAD_PATH = "/files:upload"
MAX_PATHS = 5
MIN_PATHS = 1
REQUEST_TIMEOUT_S = 120

TOOL_SCHEMA = {
    "type": "object",
    "title": "Kimi file upload",
    "description": (
        "Upload local files from this machine to the current Kimi conversation. "
        "If the call succeeds, the files are already sent to the user in Kimi "
        "and no additional send tool is needed."
    ),
    "additionalProperties": False,
    "required": ["paths"],
    "properties": {
        "paths": {
            "type": "array",
            "minItems": MIN_PATHS,
            "maxItems": MAX_PATHS,
            "description": (
                "Local filesystem paths only (1-5 total). Any file type is "
                "accepted. Use absolute or workspace-relative local paths "
                "that are readable files."
            ),
            "items": {"type": "string", "minLength": 1},
        }
    },
}


def _resolve_upload_endpoint(host: str) -> str:
    host = (host or "").strip() or DEFAULT_KIMIAPI_HOST
    if host.endswith(UPLOAD_PATH):
        return host
    return host.rstrip("/") + UPLOAD_PATH


def _guess_mime(path: str) -> str:
    mime, _ = mimetypes.guess_type(path)
    return mime or "application/octet-stream"


async def _upload_one(session: aiohttp.ClientSession, url: str, token: str,
                      path: str) -> dict:
    p = Path(path).expanduser()
    if not p.is_file():
        raise FileNotFoundError(f"not a readable file: {path}")
    data = aiohttp.FormData()
    data.add_field(
        "file",
        p.read_bytes(),
        filename=p.name,
        content_type=_guess_mime(str(p)),
    )
    async with session.post(
        url,
        data=data,
        headers={"X-Kimi-Bot-Token": token},
        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_S),
    ) as resp:
        if resp.status != 200:
            text = (await resp.text())[:500]
            raise RuntimeError(f"upload failed for {p.name}: HTTP {resp.status} {text}")
        payload = await resp.json()
    file_obj = (payload or {}).get("file") or {}
    file_id = file_obj.get("id")
    if not file_id:
        raise RuntimeError(f"upload failed for {p.name}: missing file.id in response")
    meta = file_obj.get("meta") or {}
    return {
        "uri": f"kimi-file://{file_id}",
        "name": meta.get("name") or p.name,
        "mimeType": meta.get("contentType") or meta.get("content_type")
        or _guess_mime(str(p)),
    }


async def handle_kimi_upload_file(args, **_kwargs) -> str:
    paths = (args or {}).get("paths") or []
    if not (MIN_PATHS <= len(paths) <= MAX_PATHS):
        return f"Error: paths must contain {MIN_PATHS}-{MAX_PATHS} entries."

    token = os.getenv("KIMI_CLAW_BOT_TOKEN", "").strip()
    if not token:
        return "Error: KIMI_CLAW_BOT_TOKEN is not configured."
    host = os.getenv("KIMI_CLAW_KIMIAPI_HOST", "").strip() or DEFAULT_KIMIAPI_HOST
    url = _resolve_upload_endpoint(host)

    results, errors = [], []
    async with aiohttp.ClientSession() as session:
        for raw in paths:
            try:
                results.append(await _upload_one(session, url, token, str(raw)))
            except Exception as exc:  # keep going, report per-file
                errors.append(f"{raw}: {exc}")

    # Uploading alone does NOT display the file in the chat — the upstream
    # plugin converts the result into resourceLink blocks in the outbound
    # stream. Mirror that: send one resourceLink message per file so the
    # user actually receives a file card in Kimi.
    delivered, deliver_errors = [], []
    if results:
        try:
            from .kimi_client import KimiImClient
        except ImportError:
            from kimi_client import KimiImClient  # standalone/script import
        chat_id = _resolve_current_chat_id()
        if chat_id:
            client = KimiImClient(token, host)
            await client.open()
            try:
                for idx, r in enumerate(results):
                    try:
                        await client.send_resource_link(
                            chat_id, r["uri"], r["name"],
                            block_id=f"f{idx + 1}")
                        delivered.append(r["name"])
                    except Exception as exc:
                        deliver_errors.append(f"{r['name']}: {exc}")
            finally:
                await client.close()
        else:
            deliver_errors.append("no active Kimi chat known (send any "
                                  "message to the bot first)")

    lines = [f"Uploaded: {r['name']} -> {r['uri']} ({r['mimeType']})" for r in results]
    if delivered:
        lines.append(f"Delivered to chat: {', '.join(delivered)}")
    lines += [f"Failed: {e}" for e in errors]
    lines += [f"Delivery failed: {e}" for e in deliver_errors]
    if not results:
        return "Error: all uploads failed.\n" + "\n".join(lines)
    return "\n".join(lines)


def _resolve_current_chat_id() -> str:
    """Best-effort: last chat the user messaged us from, else the bot's
    default DM chat. Persisted by the adapter in bridge_state.json."""
    try:
        state_path = (Path.home() / ".hermes" / "state" / "kimi-claw"
                      / "bridge_state.json")
        data = __import__("json").loads(state_path.read_text())
        return data.get("last_chat_id") or data.get("default_chat_id") or ""
    except Exception:
        return ""


def register_kimi_tools(ctx) -> None:
    ctx.register_tool(
        TOOL_NAME,
        "kimi-claw",
        TOOL_SCHEMA,
        handle_kimi_upload_file,
        is_async=True,
        description=TOOL_SCHEMA["description"],
        emoji="📎",
    )
