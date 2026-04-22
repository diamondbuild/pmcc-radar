"""Options chain analysis for Poor Man's Covered Calls (PMCC).

For each ticker we:
  1. Pull the options chain from yfinance (free).
  2. Find the best deep-ITM LEAP (~80-90 delta, 12-18mo DTE, cost <= budget).
  3. Find the best short-call to sell against it (30-45 DTE, 20-30 delta).
  4. Compute economics: breakeven, max risk, static yield, annualized yield.

Delta is computed from Black-Scholes using IV from the chain (yfinance
doesn't expose greeks). This keeps us 100% free.
"""
from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf
from scipy.stats import norm

# Silence yfinance's noisy 404 prints for ETFs missing fundamentals endpoints.
logging.getLogger("yfinance").setLevel(logging.CRITICAL)


RISK_FREE_RATE = 0.045  # ~4.5% 1-yr T-bill, rough PMCC calc; not sensitive


# ---------------------------------------------------------------------- BS delta
def bs_call_delta(
    spot: float,
    strike: float,
    dte_days: float,
    iv: float,
    r: float = RISK_FREE_RATE,
) -> float:
    """Black-Scholes call delta. Returns NaN on invalid inputs."""
    try:
        if spot <= 0 or strike <= 0 or dte_days <= 0 or iv <= 0:
            return float("nan")
        T = dte_days / 365.0
        sigma = iv
        d1 = (math.log(spot / strike) + (r + 0.5 * sigma * sigma) * T) / (
            sigma * math.sqrt(T)
        )
        return float(norm.cdf(d1))
    except Exception:
        return float("nan")


# ------------------------------------------------------------------- Data pulls
def _spot_price(tk: yf.Ticker) -> float:
    """Latest price — fast path via fast_info, fallback to history."""
    try:
        p = tk.fast_info.get("last_price")
        if p and p > 0:
            return float(p)
    except Exception:
        pass
    try:
        h = tk.history(period="1d", interval="1d", auto_adjust=False)
        if not h.empty:
            return float(h["Close"].iloc[-1])
    except Exception:
        pass
    return float("nan")


def _earnings_date(tk: yf.Ticker) -> Optional[datetime]:
    """Next earnings date (UTC). None if unknown."""
    try:
        cal = tk.calendar
        if isinstance(cal, dict):
            ev = cal.get("Earnings Date")
            if isinstance(ev, list) and ev:
                return pd.Timestamp(ev[0]).to_pydatetime().replace(tzinfo=timezone.utc)
        if isinstance(cal, pd.DataFrame) and not cal.empty:
            for key in ("Earnings Date", "earningsDate"):
                if key in cal.index:
                    v = cal.loc[key].iloc[0]
                    return pd.Timestamp(v).to_pydatetime().replace(tzinfo=timezone.utc)
    except Exception:
        pass
    return None


def _get_chain(tk: yf.Ticker, expiry: str) -> Optional[pd.DataFrame]:
    """Pull one expiry's call chain, annotate with mid price and DTE."""
    try:
        chain = tk.option_chain(expiry)
    except Exception:
        return None
    calls = getattr(chain, "calls", None)
    if calls is None or calls.empty:
        return None
    df = calls.copy()
    # Mid between bid/ask; fall back to lastPrice if spread is closed
    bid = df.get("bid", pd.Series(dtype=float)).fillna(0)
    ask = df.get("ask", pd.Series(dtype=float)).fillna(0)
    last = df.get("lastPrice", pd.Series(dtype=float)).fillna(0)
    mid = (bid + ask) / 2
    df["mid"] = np.where((bid > 0) & (ask > 0) & (ask >= bid), mid, last)
    df["spread"] = np.where(ask > 0, (ask - bid) / ask, np.nan)
    df["expiry"] = expiry
    try:
        exp_dt = datetime.strptime(expiry, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        df["dte"] = max(0, (exp_dt - now).days)
    except Exception:
        df["dte"] = np.nan
    return df


# -------------------------------------------------------------------- PMCC pair
@dataclass
class PMCCResult:
    ticker: str
    spot: float
    # LEAP leg
    leap_expiry: str
    leap_dte: int
    leap_strike: float
    leap_cost: float  # premium * 100
    leap_delta: float
    leap_iv: float
    leap_oi: int
    leap_spread: float
    # Short call leg
    short_expiry: str
    short_dte: int
    short_strike: float
    short_premium: float  # premium * 100
    short_delta: float
    short_iv: float
    short_oi: int
    short_spread: float
    # Economics
    breakeven: float
    max_profit: float
    max_loss: float
    net_debit: float
    static_yield: float  # short prem / net debit
    annualized_yield: float  # static * 365/short_dte
    upside_cap_pct: float  # (short_strike - spot) / spot
    # Risk flags
    earnings_before_short_expiry: bool
    next_earnings: Optional[str]
    iv_rank: float  # 0-100, rough (leap IV vs 52-wk IV range proxy)
    # Meta
    score: float
    warnings: str


def _select_leap(
    chain: pd.DataFrame,
    spot: float,
    budget: float,
    target_delta_min: float = 0.80,
    target_delta_max: float = 0.92,
) -> Optional[pd.Series]:
    """Pick the deep-ITM LEAP closest to 85 delta that fits budget."""
    if chain is None or chain.empty:
        return None
    # Restrict to ITM calls within budget
    df = chain[chain["mid"] > 0].copy()
    df["cost"] = df["mid"] * 100.0
    df = df[df["cost"] <= budget]
    df = df[df["strike"] < spot]  # ITM only for LEAP
    if df.empty:
        return None
    # Compute delta for each row
    df["delta"] = df.apply(
        lambda r: bs_call_delta(
            spot, float(r["strike"]), float(r["dte"]), float(r.get("impliedVolatility", 0))
        ),
        axis=1,
    )
    # Prefer target band; else closest to midpoint
    mid_target = (target_delta_min + target_delta_max) / 2
    in_band = df[(df["delta"] >= target_delta_min) & (df["delta"] <= target_delta_max)]
    if not in_band.empty:
        # Among in-band, pick highest OI (liquidity)
        in_band = in_band.sort_values("openInterest", ascending=False)
        return in_band.iloc[0]
    # Otherwise closest delta to target, requiring at least 0.70
    df = df[df["delta"] >= 0.70]
    if df.empty:
        return None
    df["delta_gap"] = (df["delta"] - mid_target).abs()
    return df.sort_values("delta_gap").iloc[0]


def _select_short(
    chain: pd.DataFrame,
    spot: float,
    leap_strike: float,
    target_delta_min: float = 0.20,
    target_delta_max: float = 0.32,
) -> Optional[pd.Series]:
    """Pick the best short call: 20-30 delta, OTM, above LEAP strike."""
    if chain is None or chain.empty:
        return None
    df = chain[chain["mid"] > 0].copy()
    # Must be OTM and above the LEAP strike (so max loss is bounded)
    df = df[df["strike"] > spot]
    df = df[df["strike"] > leap_strike]
    if df.empty:
        return None
    df["delta"] = df.apply(
        lambda r: bs_call_delta(
            spot, float(r["strike"]), float(r["dte"]), float(r.get("impliedVolatility", 0))
        ),
        axis=1,
    )
    in_band = df[(df["delta"] >= target_delta_min) & (df["delta"] <= target_delta_max)]
    if not in_band.empty:
        # Best premium / liquidity tradeoff: highest mid among in-band with OI > 10
        liquid = in_band[in_band["openInterest"].fillna(0) > 10]
        pick = liquid if not liquid.empty else in_band
        return pick.sort_values("mid", ascending=False).iloc[0]
    # Fallback: closest to 25 delta
    df = df[df["delta"] > 0.10]
    if df.empty:
        return None
    df["gap"] = (df["delta"] - 0.25).abs()
    return df.sort_values("gap").iloc[0]


def _pick_leap_expiry(expirations: list[str], dte_min: int = 330, dte_max: int = 600) -> Optional[str]:
    """Pick a LEAP expiry in the 11-20 month window, preferring ~15mo."""
    now = datetime.now(timezone.utc)
    candidates = []
    for e in expirations:
        try:
            exp_dt = datetime.strptime(e, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            dte = (exp_dt - now).days
            if dte_min <= dte <= dte_max:
                candidates.append((e, dte))
        except Exception:
            continue
    if not candidates:
        return None
    # Prefer middle of window (~450 DTE / 15 months)
    candidates.sort(key=lambda x: abs(x[1] - 450))
    return candidates[0][0]


def _pick_short_expiry(expirations: list[str], dte_min: int = 25, dte_max: int = 50) -> Optional[str]:
    """Pick a short-call expiry in the 25-50 DTE window, preferring ~35."""
    now = datetime.now(timezone.utc)
    candidates = []
    for e in expirations:
        try:
            exp_dt = datetime.strptime(e, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            dte = (exp_dt - now).days
            if dte_min <= dte <= dte_max:
                candidates.append((e, dte))
        except Exception:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: abs(x[1] - 35))
    return candidates[0][0]


# ---------------------------------------------------------------------- Main fn
def analyze_ticker(
    ticker: str,
    budget: float = 3500.0,
) -> Optional[PMCCResult]:
    """Full PMCC analysis for one ticker. None if no viable pair."""
    warnings_list: list[str] = []
    try:
        tk = yf.Ticker(ticker)
        spot = _spot_price(tk)
        if not spot or math.isnan(spot) or spot <= 0:
            return None

        try:
            expirations = list(tk.options)
        except Exception:
            return None
        if not expirations:
            return None

        leap_exp = _pick_leap_expiry(expirations)
        short_exp = _pick_short_expiry(expirations)
        if not leap_exp or not short_exp:
            return None

        leap_chain = _get_chain(tk, leap_exp)
        short_chain = _get_chain(tk, short_exp)
        if leap_chain is None or short_chain is None:
            return None

        leap = _select_leap(leap_chain, spot, budget)
        if leap is None:
            return None

        short = _select_short(short_chain, spot, float(leap["strike"]))
        if short is None:
            return None

        # Economics
        leap_cost = float(leap["mid"]) * 100.0
        short_prem = float(short["mid"]) * 100.0
        net_debit = leap_cost - short_prem
        # Max profit: (short_strike - leap_strike) * 100 - net_debit
        max_profit = (float(short["strike"]) - float(leap["strike"])) * 100.0 - net_debit
        max_loss = net_debit  # if LEAP goes to zero
        breakeven = float(leap["strike"]) + (net_debit / 100.0)
        static_yield = short_prem / net_debit if net_debit > 0 else 0.0
        short_dte = int(short["dte"]) or 30
        annualized = static_yield * (365.0 / short_dte)
        upside_cap = (float(short["strike"]) - spot) / spot

        # Earnings flag — if earnings falls before short expiry, it's a red flag
        earn = _earnings_date(tk)
        earn_flag = False
        earn_str = None
        if earn:
            earn_str = earn.strftime("%Y-%m-%d")
            try:
                short_exp_dt = datetime.strptime(short_exp, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
                if datetime.now(timezone.utc) < earn < short_exp_dt:
                    earn_flag = True
                    warnings_list.append("Earnings before short expiry")
            except Exception:
                pass

        # Rough IV rank: LEAP IV percentile proxy. We use LEAP IV normalized to
        # a typical band (0.15-0.80). Real IVR needs 1-yr history; this is a proxy.
        leap_iv = float(leap.get("impliedVolatility", 0) or 0)
        ivr = max(0.0, min(100.0, (leap_iv - 0.15) / (0.80 - 0.15) * 100.0))

        # Liquidity warnings
        if float(leap.get("openInterest", 0) or 0) < 50:
            warnings_list.append("Thin LEAP OI")
        if float(short.get("openInterest", 0) or 0) < 20:
            warnings_list.append("Thin short OI")
        leap_spread = float(leap.get("spread", 0) or 0)
        short_spread = float(short.get("spread", 0) or 0)
        if leap_spread > 0.15:
            warnings_list.append("Wide LEAP spread")
        if short_spread > 0.20:
            warnings_list.append("Wide short spread")

        return PMCCResult(
            ticker=ticker,
            spot=spot,
            leap_expiry=leap_exp,
            leap_dte=int(leap["dte"]),
            leap_strike=float(leap["strike"]),
            leap_cost=leap_cost,
            leap_delta=float(leap["delta"]),
            leap_iv=leap_iv,
            leap_oi=int(leap.get("openInterest", 0) or 0),
            leap_spread=leap_spread,
            short_expiry=short_exp,
            short_dte=short_dte,
            short_strike=float(short["strike"]),
            short_premium=short_prem,
            short_delta=float(short["delta"]),
            short_iv=float(short.get("impliedVolatility", 0) or 0),
            short_oi=int(short.get("openInterest", 0) or 0),
            short_spread=short_spread,
            breakeven=breakeven,
            max_profit=max_profit,
            max_loss=max_loss,
            net_debit=net_debit,
            static_yield=static_yield,
            annualized_yield=annualized,
            upside_cap_pct=upside_cap,
            earnings_before_short_expiry=earn_flag,
            next_earnings=earn_str,
            iv_rank=ivr,
            score=0.0,  # filled by scoring.py
            warnings=", ".join(warnings_list),
        )
    except Exception:
        return None


def result_to_row(r: PMCCResult) -> dict:
    d = asdict(r)
    return d
