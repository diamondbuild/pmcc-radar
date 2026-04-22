"""Build the scanning universe: S&P 500 constituents + liquid ETFs.

Uses Wikipedia for S&P 500 (free, no key). ETF list is curated for
options liquidity (tight spreads, deep open interest, weekly options).
Cached to disk for 24h so Streamlit Cloud doesn't re-fetch every rerun.
"""
from __future__ import annotations

import json
import os
import time
from typing import List

import pandas as pd
import requests

_CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "data_cache")
_CACHE_PATH = os.path.join(_CACHE_DIR, "universe.json")
_TTL_SECONDS = 24 * 3600

# Most options-liquid ETFs (tight spreads, weekly expiries, deep OI).
# These often beat single names for PMCC because IV is low-to-moderate
# and they don't gap on earnings.
LIQUID_ETFS = [
    # Broad market
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "IVV",
    # Sector
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC",
    # Tech / growth
    "SMH", "SOXX", "ARKK", "XBI", "IBB", "FDN",
    # International
    "EEM", "EFA", "FXI", "EWZ", "EWJ", "INDA",
    # Commodities
    "GLD", "SLV", "USO", "UNG", "GDX", "GDXJ",
    # Fixed income
    "TLT", "HYG", "LQD", "IEF",
    # Volatility / inverse (caution but liquid)
    "UVXY", "SQQQ", "TQQQ",
    # Dividend / value
    "VYM", "SCHD", "DVY",
]


def _fetch_sp500() -> List[str]:
    """Pull S&P 500 tickers from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    # Wikipedia blocks default user-agent; send a polite one.
    headers = {"User-Agent": "pmcc-radar/1.0 (education; github.com)"}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    tables = pd.read_html(resp.text)
    df = tables[0]
    syms = df["Symbol"].astype(str).str.replace(".", "-", regex=False).tolist()
    # Drop anything weird
    return sorted({s for s in syms if s and s.isascii() and len(s) <= 6})


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
    """Return sorted, de-duped ticker list for the scanner."""
    if not force_refresh:
        cached = _load_cache()
        if cached and cached.get("tickers"):
            return cached["tickers"]

    try:
        sp = _fetch_sp500()
    except Exception:
        sp = []

    combined = sorted(set(sp + LIQUID_ETFS))
    _save_cache({"ts": time.time(), "tickers": combined, "sp500_count": len(sp)})
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
