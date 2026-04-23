"""IBKR client — talks to our VPS proxy which holds a persistent IB Gateway
connection.

All endpoints require the X-PMCC-Token header. Base URL + token come from env
vars (set in Streamlit Cloud secrets or .env).

This module intentionally mirrors the shape of radar.options so the pipeline
can swap between data sources transparently.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import requests

log = logging.getLogger("radar.ibkr")

PROXY_URL = os.environ.get("PMCC_PROXY_URL", "").rstrip("/")
PROXY_TOKEN = os.environ.get("PMCC_PROXY_TOKEN", "")
REQUEST_TIMEOUT = 20


class IbkrProxyError(Exception):
    pass


def _headers() -> dict:
    return {"X-PMCC-Token": PROXY_TOKEN}


def is_configured() -> bool:
    return bool(PROXY_URL and PROXY_TOKEN)


def health() -> dict:
    """Check proxy health. Returns full response dict."""
    if not PROXY_URL:
        return {"ok": False, "error": "PMCC_PROXY_URL not configured"}
    try:
        r = requests.get(f"{PROXY_URL}/health", timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_spot(symbol: str) -> Optional[dict]:
    """Returns {symbol, price, bid, ask, close, delayed} or None on failure."""
    if not is_configured():
        return None
    try:
        r = requests.get(
            f"{PROXY_URL}/spot/{symbol.upper()}",
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"get_spot({symbol}) failed: {e}")
        return None


def get_expiries(symbol: str) -> Optional[list[str]]:
    """Returns list of YYYYMMDD expiry strings or None."""
    if not is_configured():
        return None
    try:
        r = requests.get(
            f"{PROXY_URL}/chain/{symbol.upper()}",
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        return data.get("expirations") or data.get("expiries") or None
    except Exception as e:
        log.warning(f"get_expiries({symbol}) failed: {e}")
        return None


def get_chain(symbol: str, expiry: str) -> Optional[dict]:
    """Returns chain dict with calls/puts for a specific expiry.

    Expected shape:
      {
        "symbol": "AAPL",
        "expiry": "20260619",
        "spot": 273.17,
        "calls": [{strike, bid, ask, last, iv, delta, gamma, vega, theta, oi, vol}, ...],
        "puts":  [...]
      }
    """
    if not is_configured():
        return None
    try:
        r = requests.get(
            f"{PROXY_URL}/chain/{symbol.upper()}",
            params={"expiry": expiry},
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"get_chain({symbol}, {expiry}) failed: {e}")
        return None


def get_positions() -> Optional[list[dict]]:
    if not is_configured():
        return None
    try:
        r = requests.get(
            f"{PROXY_URL}/positions",
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        return r.json().get("positions", [])
    except Exception as e:
        log.warning(f"get_positions failed: {e}")
        return None


def get_account() -> Optional[dict]:
    if not is_configured():
        return None
    try:
        r = requests.get(
            f"{PROXY_URL}/account",
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"get_account failed: {e}")
        return None
