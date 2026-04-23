"""PMCC scoring engine.

Composite score 0-100 balancing:
  40%  Annualized yield (return on capital at risk)
  20%  Upside room (short strike buffer above spot)
  15%  Liquidity (OI + tight spreads on both legs)
  10%  IV sweet-spot (not too low, not too high — 30-60% ideal)
  10%  Earnings safety (no earnings between now and short expiry)
   5%  LEAP delta quality (closer to 85 is better)

Bonuses / penalties:
  +3  If upside cap > 10% (room to run)
  -5  If earnings falls before short expiry
  -5  If any leg has wide spread (>15%) or thin OI
"""
from __future__ import annotations

import math
import pandas as pd


def _clamp(x, lo, hi):
    try:
        x = float(x)
    except Exception:
        return lo
    if math.isnan(x):
        return lo
    return max(lo, min(hi, x))


def _score_yield(annualized: float) -> float:
    """Annualized yield component. 30% annualized = 100 points."""
    # Map 0% → 0, 10% → 33, 20% → 67, 30% → 100, cap at 40% → 100
    return _clamp(annualized * 100 / 0.30 * 100, 0, 100)


def _score_upside(cap_pct: float) -> float:
    """Upside room component. 15% = 100."""
    return _clamp(cap_pct / 0.15 * 100, 0, 100)


def _score_liquidity(leap_oi, short_oi, leap_spread, short_spread) -> float:
    """Liquidity composite: OI + spread on both legs."""
    leap_oi_score = _clamp(math.log1p(leap_oi or 0) / math.log1p(1000) * 100, 0, 100)
    short_oi_score = _clamp(math.log1p(short_oi or 0) / math.log1p(500) * 100, 0, 100)
    # Spread: 0% ideal, 20% awful
    leap_spr_score = _clamp(100 - (leap_spread or 0) * 500, 0, 100)
    short_spr_score = _clamp(100 - (short_spread or 0) * 500, 0, 100)
    return (leap_oi_score + short_oi_score + leap_spr_score + short_spr_score) / 4


def _score_iv(iv: float) -> float:
    """IV sweet-spot: 30-60% is ideal for PMCC.
    Too low = no premium; too high = underlying whipsaw."""
    iv = iv or 0
    if iv < 0.15:
        return 20
    if iv < 0.30:
        return 60 + (iv - 0.15) / 0.15 * 40  # ramp to 100
    if iv <= 0.60:
        return 100
    if iv <= 1.00:
        return 100 - (iv - 0.60) / 0.40 * 60  # decay to 40
    return 20


def _score_earnings(flag: bool) -> float:
    return 0.0 if flag else 100.0


def _score_leap_delta(delta: float) -> float:
    """Closest to 0.85 = best."""
    if not delta or math.isnan(delta):
        return 0
    gap = abs(delta - 0.85)
    return _clamp(100 - gap * 400, 0, 100)


def score_row(row: dict) -> float:
    y = _score_yield(row.get("annualized_yield", 0))
    u = _score_upside(row.get("upside_cap_pct", 0))
    l = _score_liquidity(
        row.get("leap_oi", 0),
        row.get("short_oi", 0),
        row.get("leap_spread", 0),
        row.get("short_spread", 0),
    )
    iv = _score_iv(row.get("short_iv", 0))
    e = _score_earnings(row.get("earnings_before_short_expiry", False))
    dq = _score_leap_delta(row.get("leap_delta", 0))

    composite = (
        0.40 * y
        + 0.20 * u
        + 0.15 * l
        + 0.10 * iv
        + 0.10 * e
        + 0.05 * dq
    )

    # Bonuses / penalties
    if row.get("upside_cap_pct", 0) > 0.10:
        composite += 3
    if row.get("earnings_before_short_expiry", False):
        composite -= 5
    warnings = row.get("warnings", "") or ""
    if "Wide" in warnings or "Thin" in warnings:
        composite -= 3
    # Penalize structurally-unprofitable-at-expiry trades: they need rolls to work
    max_profit = row.get("max_profit", 0)
    net_debit = row.get("net_debit", 0)
    if max_profit is not None and max_profit < 0:
        # Scale penalty by how deeply negative vs net debit
        if net_debit and net_debit > 0:
            ratio = abs(max_profit) / net_debit
            composite -= min(15, ratio * 20)
        else:
            composite -= 10

    # Hard floor for non-viable: annualized yield < 5% is disqualified
    if row.get("annualized_yield", 0) < 0.05:
        composite = min(composite, 25)

    return round(_clamp(composite, 0, 100), 2)


def score_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Add 'score' column to a DataFrame of PMCC rows."""
    if df.empty:
        return df
    df = df.copy()
    df["score"] = df.apply(lambda r: score_row(r.to_dict()), axis=1)
    return df.sort_values("score", ascending=False)
