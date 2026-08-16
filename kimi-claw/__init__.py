"""Kimi Claw bridge plugin for Hermes Agent.

Registers:
  - The ``kimi-claw`` platform adapter (inbound/outbound bridge to kimi.com).
  - The ``kimi_upload_file`` tool (upload local files into the Kimi chat).

Zero core edits — everything goes through the public PluginContext surface
(``ctx.register_platform`` + ``ctx.register_tool``).
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

__all__ = ["register", "check_requirements", "validate_config"]


def check_requirements() -> bool:
    """The adapter needs aiohttp/websockets, both shipped with Hermes."""
    try:
        import aiohttp  # noqa: F401
        import websockets  # noqa: F401
        return True
    except Exception:
        # Fall back to aiohttp-only operation; adapter re-checks at connect().
        try:
            import aiohttp  # noqa: F401
            return True
        except Exception:
            return False


def validate_config(config) -> bool:
    """A bot token is mandatory — without it the bridge cannot authenticate."""
    extra = getattr(config, "extra", {}) or {}
    token = (
        getattr(config, "token", None)
        or getattr(config, "api_key", None)
        or extra.get("bot_token")
        or os.getenv("KIMI_CLAW_BOT_TOKEN")
    )
    return bool(token)


def is_connected(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    return bool(extra.get("enabled"))


def register(ctx) -> None:
    from .adapter import KimiClawAdapter, send_kimi_claw_direct, \
        pre_tool_call_hook
    from .tools import register_kimi_tools
    from .search_tools import register_kimi_search_tools

    ctx.register_platform(
        "kimi-claw",
        "Kimi Claw",
        lambda config: KimiClawAdapter(config),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["KIMI_CLAW_BOT_TOKEN"],
        install_hint="Set KIMI_CLAW_BOT_TOKEN in ~/.hermes/.env",
        emoji="🦞",
        cron_deliver_env_var="KIMI_CLAW_HOME_CHANNEL",
        standalone_sender_fn=send_kimi_claw_direct,
    )
    register_kimi_tools(ctx)
    register_kimi_search_tools(ctx)
    # forwardToolCalls: mirror tool calls into the Kimi draft stream as
    # native tool blocks (the stream-event dispatcher is unused by this
    # Hermes version, so the hook is the live interception point).
    ctx.register_hook("pre_tool_call", pre_tool_call_hook)
    logger.info("[kimi-claw] plugin registered (platform + tools + hook)")
