"""Kimi search-series tools, ported from the cloud KimiClaw instance's
kimi-search plugin (v0.1.15, unminified dist).

Five tools against the Kimi tool API (default host https://api.kimi.com):
  - kimi_search             POST /coding/v1/search
  - kimi_fetch              POST /coding/v1/fetch
  - kimi_finance            POST /coding/v1/tools (call_data_source_tool /
                            stock_finance_data)
  - kimi_datasource_get_desc  POST /coding/v1/tools (get_data_source_desc)
  - kimi_datasource_call    POST /coding/v1/tools (call_data_source_tool)

Auth: Bearer key. Resolution order:
  KIMI_PLUGIN_API_KEY -> HERMES_CUSTOM_API_KIMI_COM_API_KEY (the existing
  Kimi Code key, verified working on api.kimi.com for all three endpoints).
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import aiohttp

logger = logging.getLogger(__name__)

DEFAULT_BASE = "https://api.kimi.com/coding/v1"
DATASOURCE_FILE_DIR = Path.home() / ".hermes" / "workspace" / "kimi-datasource"

DATA_SOURCE_NAMES = [
    "yahoo_finance",
    "arxiv",
    "world_bank_open_data",
    "imf",
    "tianyancha",
    "scholar",
    "yuandian_law",
    "stock_finance_data",
    # Added by Kimi after 0.1.15 (verified 2026-08):
    "wind",
    "gildata",
    "sec_edgar",
    "sp_data",
]

SEARCH_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "query": {"type": "string",
                  "description": "The query text to search for."},
        "limit": {"type": "integer", "minimum": 1, "maximum": 20,
                  "default": 5,
                  "description": "The number of results to return. Typically "
                                 "you do not need to set this value."},
        "include_content": {"type": "boolean", "default": False,
                            "description": "Whether to include page content "
                                           "in results. This can consume "
                                           "many tokens."},
    },
    "required": ["query"],
}

FETCH_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "url": {"type": "string",
                "description": "The URL to fetch content from."},
    },
    "required": ["url"],
}

FINANCE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "ticker": {"type": "string",
                   "description": "Stock code(s), comma-separated, maximum 3. "
                                  "Supports A-shares (.SH/.SZ/.BJ), HK stocks "
                                  "(.HK), and/or US stocks (.US). Mixed "
                                  "markets are supported (auto-split and "
                                  "merged). IMPORTANT: Always verify stock "
                                  "codes via web search before querying — "
                                  "wrong codes produce silent wrong results. "
                                  "e.g. '600223.SH', '000001.SZ,0700.HK', "
                                  "'AAPL.US', '000001.SZ,AAPL.US'"},
        "time": {"type": "string",
                 "description": "Query time in format YYYY-MM-DD HH:MM:SS "
                                "(seconds should be 00). Optional — if not "
                                "specified, the server will determine the "
                                "appropriate time and snap to the nearest "
                                "active trading time for each market."},
        "type": {"type": "string",
                 "enum": ["open_summary", "close_summary", "realtime_price",
                          "realtime_tech"],
                 "default": "realtime_price",
                 "description": "Query type: open_summary / close_summary "
                                "(A-shares & HK), realtime_price (A-shares, "
                                "HK, US), realtime_tech (A-shares only)"},
        "file_path": {"type": "string",
                      "description": "File path to save data in CSV format."},
    },
    "required": ["ticker", "file_path"],
}

DATASOURCE_GET_DESC_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "data_source_name": {"type": "string", "enum": DATA_SOURCE_NAMES,
                             "description": "Name of the data source. "
                                            "Required parameter."},
    },
    "required": ["data_source_name"],
}

DATASOURCE_CALL_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "data_source_name": {"type": "string", "enum": DATA_SOURCE_NAMES,
                             "description": "Name of the data source. "
                                            "Required parameter."},
        "api_name": {"type": "string",
                     "description": "Name of the API to call (for "
                                    "'yahoo_finance' data source, an example "
                                    "of the available API name is "
                                    "'get_historical_stock_prices'). "
                                    "Required parameter"},
        "params": {"type": "object",
                   "description": "Parameters for the API call (e.g., for "
                                  "'yahoo_finance' data source and its "
                                  "'get_historical_stock_prices' API, the "
                                  "parameters are {'ticker', 'period', "
                                  "'interval'})."},
    },
    "required": ["data_source_name", "api_name"],
}


def _resolve_key() -> str:
    for name in ("KIMI_PLUGIN_API_KEY", "HERMES_CUSTOM_API_KIMI_COM_API_KEY"):
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    # The gateway process env is fixed at startup; re-read .env at call time
    # so an API-key rotation does not require a gateway restart.
    try:
        env_path = Path.home() / ".hermes" / ".env"
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            if key.strip() in ("KIMI_PLUGIN_API_KEY",
                               "HERMES_CUSTOM_API_KIMI_COM_API_KEY"):
                value = value.strip().strip('"').strip("'")
                if value:
                    return value
    except Exception:
        pass
    return ""


def _base() -> str:
    return (os.getenv("KIMI_PLUGIN_BASE_URL") or DEFAULT_BASE).rstrip("/")


def _headers(extra=None):
    h = {
        "Authorization": f"Bearer {_resolve_key()}",
        "Content-Type": "application/json",
        "User-Agent": "hermes-kimi-claw-plugin/0.1",
    }
    if extra:
        h.update(extra)
    return h


async def _post(session, url, payload, extra_headers=None, text=False):
    if not _resolve_key():
        raise RuntimeError("no Kimi API key (KIMI_PLUGIN_API_KEY or "
                           "HERMES_CUSTOM_API_KIMI_COM_API_KEY)")
    async with session.post(
        url, json=payload, headers=_headers(extra_headers),
        timeout=aiohttp.ClientTimeout(total=45),
    ) as resp:
        if resp.status != 200:
            body = (await resp.text())[:300]
            raise RuntimeError(f"HTTP {resp.status}: {body}")
        return await resp.text() if text else await resp.json()


# ----------------------------------------------------------------------
# tool handlers
# ----------------------------------------------------------------------
async def handle_kimi_search(args, **_kw) -> str:
    args = args or {}
    query = str(args.get("query") or "").strip()
    if not query:
        return "Error: query is required."
    limit = args.get("limit") or 5
    try:
        limit = max(1, min(20, int(limit)))
    except (TypeError, ValueError):
        limit = 5
    async with aiohttp.ClientSession() as session:
        try:
            data = await _post(session, f"{_base()}/search", {
                "text_query": query,
                "limit": limit,
                "enable_page_crawling": bool(args.get("include_content")),
                "timeout_seconds": 30,
            })
        except Exception as exc:
            return f"Error: search failed: {exc}"
    results = data.get("search_results") or []
    if not results:
        return "No results."
    chunks = []
    for i, r in enumerate(results):
        if i:
            chunks.append("---\n")
        chunks.append(f"Title: {r.get('title', '')}\n"
                      f"Date: {r.get('date', '')}\n"
                      f"URL: {r.get('url', '')}\n"
                      f"Summary: {r.get('snippet', '')}\n")
        if r.get("content"):
            chunks.append(str(r["content"]) + "\n")
    return "\n".join(chunks)


async def handle_kimi_fetch(args, **_kw) -> str:
    url = str((args or {}).get("url") or "").strip()
    if not url:
        return "Error: url is required."
    async with aiohttp.ClientSession() as session:
        try:
            return await _post(session, f"{_base()}/fetch", {"url": url},
                               extra_headers={"Accept": "text/markdown"},
                               text=True)
        except Exception as exc:
            return f"Error: fetch failed: {exc}"


def _extract_texts(items) -> list:
    out = []
    for item in items or []:
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict) and isinstance(item.get("text"), str):
            out.append(item["text"])
    return [t for t in out if t]


def _save_datasource_files(files) -> str:
    saved = []
    for f in files or []:
        try:
            name = Path(str(f.get("name") or "data.csv")).name  # confine
            dest = DATASOURCE_FILE_DIR / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(str(f.get("content") or ""), encoding="utf-8")
            saved.append(str(dest))
        except Exception as exc:
            logger.warning("[kimi-claw] datasource file save failed: %s", exc)
    return f"Files saved: {', '.join(saved)}" if saved else ""


async def _call_tool_service(method: str, params: dict) -> str:
    async with aiohttp.ClientSession() as session:
        try:
            data = await _post(session, f"{_base()}/tools",
                               {"method": method, "params": params})
        except Exception as exc:
            return f"Error: tool service call failed: {exc}"
    if isinstance(data, dict) and data.get("is_success") is False:
        msgs = _extract_texts((data.get("error") or {}).get("assistant"))
        return "Error: " + ("\n".join(msgs) or "data source returned an error")
    if isinstance(data, str):
        return data
    msgs = _extract_texts((data.get("result") or {}).get("assistant"))
    file_msg = _save_datasource_files(data.get("files"))
    out = "\n".join(msgs) if msgs else json.dumps(data, ensure_ascii=False)[:4000]
    return f"{out}\n{file_msg}".strip()


async def handle_kimi_finance(args, **_kw) -> str:
    args = args or {}
    ticker = str(args.get("ticker") or "").strip()
    file_path = str(args.get("file_path") or "").strip()
    if not ticker or not file_path:
        return "Error: ticker and file_path are required."
    params = {
        "ticker": ticker,
        "type": args.get("type") or "realtime_price",
        "file_path": file_path,
    }
    if args.get("time"):
        params["time"] = str(args["time"])
    return await _call_tool_service("call_data_source_tool", {
        "data_source_name": "stock_finance_data",
        "api_name": "stock_finance_data_get_stock_realtime_price",
        "params": params,
    })


async def handle_kimi_datasource_get_desc(args, **_kw) -> str:
    name = str((args or {}).get("data_source_name") or "").strip()
    if not name:
        return "Error: data_source_name is required."
    return await _call_tool_service("get_data_source_desc", {"name": name})


async def handle_kimi_datasource_call(args, **_kw) -> str:
    args = args or {}
    name = str(args.get("data_source_name") or "").strip()
    api_name = str(args.get("api_name") or "").strip()
    if not name or not api_name:
        return "Error: data_source_name and api_name are required."
    return await _call_tool_service("call_data_source_tool", {
        "data_source_name": name,
        "api_name": api_name,
        "params": args.get("params") or {},
    })


# ----------------------------------------------------------------------
# Named alias tools — deterministic discovery for the post-0.1.15 sources.
# tool_search can't find "kimi_datasource_call" by source keywords, so give
# each new source its own searchable name.
# ----------------------------------------------------------------------
ALIAS_SOURCES = {
    "kimi_sec_edgar": (
        "sec_edgar",
        "SEC EDGAR filings for US-listed companies: 10-K/10-Q full financial "
        "statements (balance sheet, income statement, cash flow), XBRL "
        "metric time series, insider trades (Form 4), institutional 13F "
        "holdings, 8-K company events, filing list. Use for questions like "
        "'AAPL/MSFT/TSLA latest 10-K balance sheet'.",
    ),
    "kimi_wind": (
        "wind",
        "Wind financial data: A-share minute/daily OHLCV, funds, bonds, "
        "macro EDB (CPI/PPI/GDP/PMI/social financing/exports/unemployment), "
        "financial indicators (PE/PB/ROE/market cap), technicals, "
        "natural-language stock/fund screening.",
    ),
    "kimi_gildata": (
        "gildata",
        "Gildata (Juyuan) natural-language financial data: smart stock/fund/"
        "fund-manager screening, broker research reports, A-share "
        "announcements, news and public-opinion corpus.",
    ),
    "kimi_sp_data": (
        "sp_data",
        "S&P Capital IQ institutional US equity data: analyst consensus "
        "estimates (EPS/revenue/EBITDA forecasts), valuation ratios, "
        "competitors, top owners/executives, earnings-call transcripts, "
        "M&A transactions, topic tags.",
    ),
}


def _make_alias_handler(source_name):
    async def _handler(args, **_kw):
        args = dict(args or {})
        args["data_source_name"] = source_name
        return await handle_kimi_datasource_call(args)
    return _handler


def register_kimi_search_tools(ctx) -> None:
    ctx.register_tool(
        "kimi_search", "kimi-search", SEARCH_SCHEMA, handle_kimi_search,
        is_async=True, emoji="🔍",
        description="Search the web via Kimi search. Returns titles, URLs, "
                    "dates and snippets (optionally full page content).")
    ctx.register_tool(
        "kimi_fetch", "kimi-search", FETCH_SCHEMA, handle_kimi_fetch,
        is_async=True, emoji="📄",
        description="Fetch a URL and return its main content as markdown, "
                    "via Kimi's fetch service.")
    ctx.register_tool(
        "kimi_finance", "kimi-search", FINANCE_SCHEMA, handle_kimi_finance,
        is_async=True, emoji="📈",
        description="Query stock market data (A-shares/HK/US): realtime "
                    "price, open/close summary, technical indicators.")
    ctx.register_tool(
        "kimi_datasource_get_desc", "kimi-search", DATASOURCE_GET_DESC_SCHEMA,
        handle_kimi_datasource_get_desc, is_async=True, emoji="🗃️",
        description="Get the description and available APIs of a Kimi data "
                    "source. Available: yahoo_finance, arxiv, "
                    "world_bank_open_data, imf, tianyancha (中国企业工商), "
                    "scholar, yuandian_law (中国法律), stock_finance_data "
                    "(A股/港美实时行情), wind (A股分钟线/基金/债券/宏观EDB/自然语言选股), "
                    "gildata (聚源：智能选股选基金/研报/公告/新闻语料), "
                    "sec_edgar (美股 SEC 财报 10-K/10-Q/XBRL/内部人交易/13F持仓), "
                    "sp_data (S&P Capital IQ：美股一致预期/估值/股东/高管/电话会纪要).")
    ctx.register_tool(
        "kimi_datasource_call", "kimi-search", DATASOURCE_CALL_SCHEMA,
        handle_kimi_datasource_call, is_async=True, emoji="🗃️",
        description="Call an API of a Kimi data source. Use "
                    "kimi_datasource_get_desc first to discover available "
                    "API names and parameters. Routing hints: US SEC filings "
                    "and balance sheets -> sec_edgar; institutional US "
                    "fundamentals/estimates -> sp_data; A-share/funds/bonds/"
                    "macro and natural-language stock screening -> wind; "
                    "broker research/announcements/news -> gildata; CN "
                    "company registry -> tianyancha; CN law -> yuandian_law; "
                    "papers -> arxiv/scholar; global macro -> imf/"
                    "world_bank_open_data; realtime stock quotes -> "
                    "stock_finance_data.")
    for alias, (source, desc) in ALIAS_SOURCES.items():
        schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "api_name": {"type": "string",
                             "description": f"API name of the {source} data "
                                            "source (discover via "
                                            "kimi_datasource_get_desc)."},
                "params": {"type": "object",
                           "description": "Parameters for the API call."},
            },
            "required": ["api_name"],
        }
        ctx.register_tool(alias, "kimi-search", schema,
                          _make_alias_handler(source), is_async=True,
                          emoji="🗃️", description=desc)
