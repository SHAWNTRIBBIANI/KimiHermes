"""kimi-claw gateway event hook: close draft streams at agent:end.

The Kimi UI shows the "generating/typing" indicator while a
send-message-stream WS is open.  Without a run-end signal the adapter can
only wait for its idle-grace timeout; this hook fires exactly when the
agent turn finishes and arms a short delayed close (6s) so any trailing
final-send frames still land first.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CLOSE_DELAY_S = 6.0


async def handle(event_type: str, context: dict) -> None:
    if event_type != "agent:end":
        return
    if context.get("platform") != "kimi-claw":
        return
    try:
        from hermes_plugins.kimi_claw import adapter as ad
    except Exception:
        return
    adapter = getattr(ad, "_ACTIVE_ADAPTER", None)
    if adapter is None:
        return
    chat_id = str(context.get("chat_id") or "")
    if not chat_id:
        return
    stream = adapter._streams.get(chat_id)
    if stream is not None and not stream._closed:
        stream.schedule_close(_CLOSE_DELAY_S)
        logger.info("[kimi-claw] agent:end -> stream close armed (%.0fs)",
                    _CLOSE_DELAY_S)
