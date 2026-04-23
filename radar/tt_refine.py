"""Tastytrade refinement layer.

After the fast yfinance scan ranks all tickers, re-fetch the TOP N through
Tastytrade (live greeks + bid/ask via DXLink websocket) to replace the
Black-Scholes approximations with real market data.

Rows refined are marked source="tastytrade".
"""
from __future__ import annotations

import logging
import math
import time
from typing import Optional

import pandas as pd

from . import tastytrade as tt

log = logging.getLogger("radar.tt_refine")


def _yyyymmdd(date_str: str) -> str:
    return str(date_str).replace("-", "")


def _pick_row_at_strike(rows: list[dict], strike: float) -> Optional[dict]:
    if not rows:
        return None
    for r in rows:
        try:
            if abs(float(r.get("strike", 0)) - strike) < 0.01:
                return r
        except (TypeError, ValueError):
            continue
    return None


def _mid(row: dict) -> Optional[float]:
    bid = row.get("bid")
    ask = row.get("ask")
    last = row.get("last")
    try:
        if bid and ask and float(bid) > 0 and float(ask) > 0:
            return (float(bid) + float(ask)) / 2.0
        if last and float(last) > 0:
            return float(last)
    except (TypeError, ValueError):
        pass
    return None


def _refine_row(row: pd.Series) -> Optional[dict]:
    """Refine one scan row through Tastytrade. Returns patch dict or None."""
    ticker = row["ticker"]
    try:
        leap_exp = _yyyymmdd(row["leap_expiry"])
        short_exp = _yyyymmdd(row["short_expiry"])

        leap_chain = tt.get_chain(ticker, leap_exp)
        if not leap_chain or not leap_chain.get("calls"):
            return None
        short_chain = tt.get_chain(ticker, short_exp)
        if not short_chain or not short_chain.get("calls"):
            return None

        leap_row = _pick_row_at_strike(leap_chain["calls"], float(row["leap_strike"]))
        short_row = _pick_row_at_strike(short_chain["calls"], float(row["short_strike"]))
        if not leap_row or not short_row:
            return None

        leap_mid = _mid(leap_row)
        short_mid = _mid(short_row)
        if leap_mid is None or short_mid is None:
            return None

        spot = (
            float(leap_chain.get("spot") or 0)
            or float(short_chain.get("spot") or 0)
            or float(row["spot"])
        )

        leap_cost = leap_mid * 100.0
        short_prem = short_mid * 100.0
        net_debit = leap_cost - short_prem
        if net_debit <= 0:
            return None
        max_profit = (float(row["short_strike"]) - float(row["leap_strike"])) * 100.0 - net_debit
        max_loss = net_debit
        breakeven = float(row["leap_strike"]) + (net_debit / 100.0)
        static_yield = short_prem / net_debit
        short_dte = int(row.get("short_dte", 30)) or 30
        annualized = static_yield * (365.0 / short_dte)
        upside_cap = (float(row["short_strike"]) - spot) / spot if spot > 0 else float(row.get("upside_cap_pct", 0))

        def _or(val, fallback):
            try:
                if val is None:
                    return fallback
                v = float(val)
                if math.isnan(v) or math.isinf(v):
                    return fallback
                return v
            except (TypeError, ValueError):
                return fallback

        leap_iv = _or(leap_row.get("iv"), float(row.get("leap_iv", 0) or 0))
        leap_delta = _or(leap_row.get("delta"), float(row.get("leap_delta", 0) or 0))
        short_iv = _or(short_row.get("iv"), float(row.get("short_iv", 0) or 0))
        short_delta = _or(short_row.get("delta"), float(row.get("short_delta", 0) or 0))
        # Tastytrade chain has no OI; keep yfinance values
        leap_oi = int(row.get("leap_oi", 0) or 0)
        short_oi = int(row.get("short_oi", 0) or 0)

        ivr = max(0.0, min(100.0, (leap_iv - 0.15) / (0.80 - 0.15) * 100.0))

        return {
            "spot": spot,
            "leap_cost": leap_cost,
            "leap_iv": leap_iv,
            "leap_delta": leap_delta,
            "leap_oi": leap_oi,
            "short_premium": short_prem,
            "short_iv": short_iv,
            "short_delta": short_delta,
            "short_oi": short_oi,
            "net_debit": net_debit,
            "max_profit": max_profit,
            "max_loss": max_loss,
            "breakeven": breakeven,
            "static_yield": static_yield,
            "annualized_yield": annualized,
            "upside_cap_pct": upside_cap,
            "iv_rank": ivr,
            "source": "tastytrade",
        }
    except Exception as e:
        log.warning(f"Tastytrade refine failed for {ticker}: {e}")
        return None


def refine_top_n(
    df: pd.DataFrame,
    top_n: int = 5,
    progress_cb=None,
) -> pd.DataFrame:
    """Refine the top N rows of a ranked DataFrame through Tastytrade.

    Adds a `source` column. Rows not refined remain marked "yfinance".
    Re-scores and re-sorts at the end if any row was refined.
    """
    from . import scoring

    if df is None or df.empty:
        return df

    out = df.copy()
    if "source" not in out.columns:
        out["source"] = "yfinance"

    n = min(top_n, len(out))
    refined = 0
    for i in range(n):
        idx = out.index[i]
        ticker = out.at[idx, "ticker"]
        try:
            patch = _refine_row(out.iloc[i])
        except Exception as e:
            log.warning(f"refine_row exception for {ticker}: {e}")
            patch = None

        if patch:
            for k, v in patch.items():
                if k not in out.columns:
                    out[k] = None
                out.at[idx, k] = v
            refined += 1

        if progress_cb:
            try:
                progress_cb(i + 1, n)
            except Exception:
                pass

        # Gentle pacing (Tastytrade doesn't enforce strict rate limits but
        # DXLink streamer setup costs ~200ms regardless)
        time.sleep(0.05)

    if refined > 0:
        out = scoring.score_dataframe(out)
        out = out.sort_values("score", ascending=False).reset_index(drop=True)

    log.info(f"Tastytrade refine: {refined}/{n} rows updated")
    return out
