"""Build the scanning universe: S&P 500 + Russell 1000 + Nasdaq 100 + liquid ETFs.

Uses Wikipedia for index constituents (free, no key). ETF list is curated for
options liquidity (tight spreads, deep open interest, weekly options).
Cached to disk for 24h so Streamlit Cloud doesn't re-fetch every rerun.
"""
from __future__ import annotations

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


def _fetch_wikipedia_table(url: str, table_index: int, symbol_col: str) -> List[str]:
    """Generic Wikipedia table fetcher that returns a clean ticker list."""
    headers = {"User-Agent": "pmcc-radar/1.0 (education; github.com)"}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    tables = pd.read_html(resp.text)
    df = tables[table_index]
    if symbol_col not in df.columns:
        # Try common alternative column names
        for alt in ["Ticker", "Symbol", "Code", "Ticker symbol"]:
            if alt in df.columns:
                symbol_col = alt
                break
    syms = df[symbol_col].astype(str).str.replace(".", "-", regex=False).tolist()
    # Drop anything weird; keep only clean ASCII tickers 1-6 chars
    return sorted({s.strip() for s in syms if s and s.isascii() and 1 <= len(s.strip()) <= 6})


def _fetch_sp500() -> List[str]:
    try:
        return _fetch_wikipedia_table(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            table_index=0,
            symbol_col="Symbol",
        )
    except Exception as e:
        log.warning(f"S&P 500 fetch failed: {e}")
        return []


def _fetch_nasdaq100() -> List[str]:
    try:
        # Nasdaq-100 Wikipedia page — table index varies, try a few
        headers = {"User-Agent": "pmcc-radar/1.0 (education; github.com)"}
        resp = requests.get(
            "https://en.wikipedia.org/wiki/Nasdaq-100", headers=headers, timeout=20
        )
        resp.raise_for_status()
        tables = pd.read_html(resp.text)
        # Find the table with a "Ticker" or "Symbol" column AND > 50 rows (the constituents)
        for t in tables:
            cols = [c for c in t.columns]
            for sym_col in ["Ticker", "Symbol"]:
                if sym_col in cols and len(t) >= 50:
                    syms = t[sym_col].astype(str).str.replace(".", "-", regex=False).tolist()
                    return sorted({s.strip() for s in syms if s and s.isascii() and 1 <= len(s.strip()) <= 6})
        return []
    except Exception as e:
        log.warning(f"Nasdaq 100 fetch failed: {e}")
        return []


def _fetch_russell1000() -> List[str]:
    try:
        headers = {"User-Agent": "pmcc-radar/1.0 (education; github.com)"}
        resp = requests.get(
            "https://en.wikipedia.org/wiki/Russell_1000_Index",
            headers=headers,
            timeout=20,
        )
        resp.raise_for_status()
        tables = pd.read_html(resp.text)
        # The Russell 1000 constituents table is the largest one with Ticker/Symbol
        best = None
        for t in tables:
            cols = list(t.columns)
            for sym_col in ["Ticker", "Symbol"]:
                if sym_col in cols and len(t) >= 500:
                    if best is None or len(t) > len(best):
                        best = t
                        best_col = sym_col
        if best is None:
            return []
        syms = best[best_col].astype(str).str.replace(".", "-", regex=False).tolist()
        return sorted({s.strip() for s in syms if s and s.isascii() and 1 <= len(s.strip()) <= 6})
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
