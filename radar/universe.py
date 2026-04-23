"""Build the scanning universe: S&P 500 + Russell 1000 + Nasdaq 100 + liquid ETFs.

Uses Wikipedia for index constituents (free, no key). ETF list is curated for
options liquidity (tight spreads, deep open interest, weekly options).
Cached to disk for 24h so Streamlit Cloud doesn't re-fetch every rerun.
"""
from __future__ import annotations

import io
import json
import logging
import os
import time
from typing import List

import pandas as pd
import requests

log = logging.getLogger("radar.universe")

_CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "data_cache")
_CACHE_PATH = os.path.join(_CACHE_DIR, "universe.json")
_TTL_SECONDS = 24 * 3600

# Most options-liquid ETFs (tight spreads, weekly expiries, deep OI).
# These often beat single names for PMCC because IV is low-to-moderate
# and they don't gap on earnings.
LIQUID_ETFS = [
    # Broad market
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "IVV", "MDY", "RSP",
    # Sector SPDRs
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC",
    # Tech / growth
    "SMH", "SOXX", "ARKK", "ARKG", "ARKW", "XBI", "IBB", "FDN", "IGV",
    # International
    "EEM", "EFA", "FXI", "EWZ", "EWJ", "INDA", "KWEB", "VWO",
    # Commodities
    "GLD", "SLV", "USO", "UNG", "GDX", "GDXJ", "URA", "CPER",
    # Fixed income
    "TLT", "HYG", "LQD", "IEF", "AGG", "BND", "TIP",
    # Volatility / inverse (caution but liquid)
    "UVXY", "VXX", "SQQQ", "TQQQ", "SPXL", "SOXL", "TMF",
    # Dividend / income
    "VYM", "SCHD", "DVY", "JEPI", "JEPQ", "QYLD", "XYLD",
    # Crypto-adjacent
    "IBIT", "FBTC", "BITO", "ETHA",
    # Thematic
    "ICLN", "LIT", "TAN", "JETS", "ITB", "XHB",
]


def _clean_tickers(raw_list) -> List[str]:
    """Clean a list of raw ticker strings: handle dots→dashes, strip suffixes."""
    out = set()
    for s in raw_list:
        s = str(s).strip().upper()
        if not s or not s.isascii():
            continue
        # Strip common Wikipedia reference footnotes like [1], †, *, etc
        for bad in ["[", "]", "†", "*", "‡"]:
            s = s.split(bad)[0].strip()
        # Yahoo uses BRK-B, BF-B (not BRK.B)
        s = s.replace(".", "-")
        if 1 <= len(s) <= 6 and s.replace("-", "").isalnum():
            out.add(s)
    return sorted(out)


def _find_ticker_tables(url: str, min_rows: int = 50) -> List[List[str]]:
    """Fetch a Wikipedia page and return all tables with a Ticker/Symbol column.

    pd.read_html sometimes returns tables with multi-level headers; we flatten
    columns so lookups by simple string name work.
    """
    headers = {"User-Agent": "pmcc-radar/1.0 (education; github.com)"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    # pd.read_html in newer pandas requires a file-like object, not raw string
    try:
        tables = pd.read_html(io.StringIO(resp.text))
    except ValueError as e:
        log.warning(f"No HTML tables in {url}: {e}")
        return []

    ticker_candidates = ["Ticker", "Symbol", "Ticker symbol", "Code", "TICKER", "SYMBOL"]
    results = []
    for t in tables:
        # Flatten multi-level columns if needed
        if isinstance(t.columns, pd.MultiIndex):
            t.columns = [" ".join([str(x) for x in col if str(x) != "nan"]).strip() for col in t.columns]
        cols = [str(c).strip() for c in t.columns]
        t.columns = cols
        if len(t) < min_rows:
            continue
        for cand in ticker_candidates:
            # Exact match or substring (e.g. "Ticker" matches "Ticker symbol")
            matching = [c for c in cols if c == cand or cand.lower() in c.lower()]
            if matching:
                col = matching[0]
                syms = _clean_tickers(t[col].tolist())
                if len(syms) >= min_rows // 2:  # sanity check
                    results.append(syms)
                    break
    return results


def _fetch_sp500() -> List[str]:
    try:
        tables = _find_ticker_tables(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            min_rows=400,
        )
        return tables[0] if tables else []
    except Exception as e:
        log.warning(f"S&P 500 fetch failed: {e}")
        return []


def _fetch_nasdaq100() -> List[str]:
    try:
        tables = _find_ticker_tables(
            "https://en.wikipedia.org/wiki/Nasdaq-100", min_rows=80
        )
        # Prefer the biggest matching table (the constituents list)
        if tables:
            return max(tables, key=len)
        return []
    except Exception as e:
        log.warning(f"Nasdaq 100 fetch failed: {e}")
        return []


def _fetch_russell1000() -> List[str]:
    try:
        tables = _find_ticker_tables(
            "https://en.wikipedia.org/wiki/Russell_1000_Index", min_rows=400
        )
        if tables:
            return max(tables, key=len)
        return []
    except Exception as e:
        log.warning(f"Russell 1000 fetch failed: {e}")
        return []


# High-options-volume retail favorites that may not be in any of the indices above
# (e.g. foreign ADRs, recent IPOs, high-short-interest names). Maintained manually.
EXTRA_LIQUID_STOCKS = [
    # Retail favorites / high-vol names
    "COIN", "MSTR", "SMCI", "PLTR", "SOFI", "HOOD", "AFRM", "RIVN",
    "NIO", "BABA", "JD", "BIDU", "LCID", "RIOT", "MARA", "PYPL",
    "DKNG", "RBLX", "U", "NET", "CRWD", "PANW", "ZS", "DDOG",
    "SNOW", "MRVL", "ON", "AMAT", "LRCX", "KLAC", "MU", "WDC",
    "BE", "FSLR", "ENPH", "RUN", "CHPT", "BLNK",
    "TLRY", "CGC", "SPCE", "PATH", "BBAI", "SOUN",
    "BYND", "PTON", "W", "ETSY", "CVNA",
    "T", "VZ", "F", "GM", "X", "CLF", "FCX", "NEM",
    "BAC", "C", "WFC", "JPM", "GS", "MS",
]


def _load_cache():
    if not os.path.exists(_CACHE_PATH):
        return None
    try:
        with open(_CACHE_PATH, "r") as f:
            data = json.load(f)
        if time.time() - data.get("ts", 0) > _TTL_SECONDS:
            return None
        return data
    except Exception:
        return None


def _save_cache(payload: dict) -> None:
    os.makedirs(_CACHE_DIR, exist_ok=True)
    try:
        with open(_CACHE_PATH, "w") as f:
            json.dump(payload, f)
    except Exception:
        pass


def build_universe(force_refresh: bool = False) -> List[str]:
    """Return sorted, de-duped ticker list for the scanner.

    Combines: S&P 500 + Russell 1000 + Nasdaq 100 + curated ETFs + retail favorites.
    Falls back gracefully if any index fetch fails.
    """
    if not force_refresh:
        cached = _load_cache()
        if cached and cached.get("tickers"):
            return cached["tickers"]

    sp = _fetch_sp500()
    nd = _fetch_nasdaq100()
    r1k = _fetch_russell1000()

    combined = sorted(set(sp + nd + r1k + LIQUID_ETFS + EXTRA_LIQUID_STOCKS))
    _save_cache({
        "ts": time.time(),
        "tickers": combined,
        "sp500_count": len(sp),
        "nasdaq100_count": len(nd),
        "russell1000_count": len(r1k),
        "etf_count": len(LIQUID_ETFS),
        "extras_count": len(EXTRA_LIQUID_STOCKS),
    })
    log.info(
        f"Universe built: {len(combined)} tickers "
        f"(S&P500={len(sp)}, Nasdaq100={len(nd)}, Russell1000={len(r1k)}, "
        f"ETFs={len(LIQUID_ETFS)}, Extras={len(EXTRA_LIQUID_STOCKS)})"
    )
    return combined


def cache_age_seconds() -> float | None:
    """How old is the universe cache (seconds)? None if no cache."""
    if not os.path.exists(_CACHE_PATH):
        return None
    try:
        with open(_CACHE_PATH, "r") as f:
            data = json.load(f)
        return time.time() - data.get("ts", 0)
    except Exception:
        return None


def cache_stats() -> dict:
    """Return breakdown of what's in the cached universe. Empty dict if no cache."""
    if not os.path.exists(_CACHE_PATH):
        return {}
    try:
        with open(_CACHE_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}
