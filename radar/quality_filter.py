"""Quality filters for "Joey's PMCC Method".

Hard-gates a ticker before running the full options analysis. We check:

  1. Stock price >= $40
  2. Average daily volume >= 5M shares
  3. Last close above the 200-day moving average
  4. Weekly options chain available (>= 6 expiries with one within 14 days)
  5. No earnings inside the next 14 days

All checks are done off a single yfinance history pull + the ticker's option
expiries list, so the marginal cost vs. the regular analyze_ticker is small.

Returns a QualityResult dataclass with `passed` and a list of `reasons` for
any failures so the UI / logs can show why a ticker was rejected.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import yfinance as yf


log = logging.getLogger("radar.quality_filter")


# ----------------------------------------------------------------- thresholds
MIN_PRICE = 40.0
MIN_AVG_VOLUME = 5_000_000
EARNINGS_BLOCKOUT_DAYS = 14
MIN_WEEKLY_EXPIRIES = 6        # rough proxy: a name with weeklies has >= 6 expiries
WEEKLIES_NEAR_DAYS = 14         # at least one expiry inside 14 days


# ETFs are exempt from the earnings check (no earnings) and from the
# "long-term quality" assumption (we trust their construction).
KNOWN_ETF_PREFIXES = (
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "IVV", "MDY", "RSP",
    "XL", "SMH", "SOXX", "ARK", "XBI", "IBB", "FDN", "IGV",
    "EEM", "EFA", "FXI", "EWZ", "EWJ", "INDA", "KWEB", "VWO",
    "GLD", "SLV", "USO", "UNG", "GDX", "URA", "CPER",
    "TLT", "HYG", "LQD", "IEF", "AGG", "BND", "TIP",
    "UVXY", "VXX", "SQQQ", "TQQQ", "SPXL", "SOXL", "TMF",
    "VYM", "SCHD", "DVY", "JEPI", "JEPQ", "QYLD", "XYLD",
    "IBIT", "FBTC", "BITO", "ETHA",
    "ICLN", "LIT", "TAN", "JETS", "ITB", "XHB",
)


def _looks_like_etf(ticker: str) -> bool:
    return ticker in KNOWN_ETF_PREFIXES


@dataclass
class QualityResult:
    ticker: str
    passed: bool
    spot: float = 0.0
    avg_volume: float = 0.0
    above_200dma: Optional[bool] = None
    has_weeklies: Optional[bool] = None
    days_to_earnings: Optional[int] = None  # None if unknown
    reasons: list[str] = field(default_factory=list)


def check_quality(
    ticker: str,
    *,
    require_above_200dma: bool = True,
    skip_etf_earnings: bool = True,
) -> QualityResult:
    """Run all gates. Returns QualityResult; .passed is True only if all pass.

    Errors are treated as soft-fails (returned as a reason) so a single bad
    yfinance response doesn't crash the scan.
    """
    res = QualityResult(ticker=ticker, passed=False)
    is_etf = _looks_like_etf(ticker)

    try:
        tk = yf.Ticker(ticker)
    except Exception as e:
        res.reasons.append(f"ticker init failed: {e}")
        return res

    # 1. Pull ~1 year of history in one shot — gives us spot, avg vol, 200dma
    try:
        hist = tk.history(period="1y", interval="1d", auto_adjust=False)
    except Exception as e:
        res.reasons.append(f"history fetch failed: {e}")
        return res

    if hist is None or hist.empty:
        res.reasons.append("no price history")
        return res

    try:
        spot = float(hist["Close"].iloc[-1])
    except Exception:
        res.reasons.append("invalid close")
        return res
    res.spot = spot

    # 1a. Stock price gate
    if spot < MIN_PRICE:
        res.reasons.append(f"price ${spot:.2f} < ${MIN_PRICE:.0f}")

    # 2. Average volume (last 30 sessions, more responsive than 90-day)
    try:
        avg_vol = float(hist["Volume"].tail(30).mean())
    except Exception:
        avg_vol = 0.0
    res.avg_volume = avg_vol
    if avg_vol < MIN_AVG_VOLUME:
        res.reasons.append(
            f"avg vol {avg_vol/1e6:.1f}M < {MIN_AVG_VOLUME/1e6:.0f}M"
        )

    # 3. Above 200-day moving average
    if len(hist) >= 200:
        ma200 = float(hist["Close"].tail(200).mean())
        above = spot > ma200
        res.above_200dma = above
        if require_above_200dma and not above:
            res.reasons.append(
                f"below 200DMA (${spot:.2f} vs ${ma200:.2f})"
            )
    else:
        # Not enough history. Don't reject — flag as unknown.
        res.above_200dma = None

    # 4. Weekly options chain
    try:
        expiries = list(tk.options or [])
    except Exception:
        expiries = []
    if expiries:
        now = datetime.now(timezone.utc)
        near_count = 0
        for e in expiries:
            try:
                exp_dt = datetime.strptime(e, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                if 0 <= (exp_dt - now).days <= WEEKLIES_NEAR_DAYS:
                    near_count += 1
            except Exception:
                continue
        has_weeklies = (
            len(expiries) >= MIN_WEEKLY_EXPIRIES and near_count >= 1
        )
        res.has_weeklies = has_weeklies
        if not has_weeklies:
            res.reasons.append(
                f"no weeklies ({len(expiries)} expiries, {near_count} within {WEEKLIES_NEAR_DAYS}d)"
            )
    else:
        res.has_weeklies = False
        res.reasons.append("no option expiries")

    # 5. Earnings blackout (skip for ETFs)
    if is_etf and skip_etf_earnings:
        res.days_to_earnings = None
    else:
        earn = _next_earnings(tk)
        if earn is not None:
            now = datetime.now(timezone.utc)
            days = (earn - now).days
            res.days_to_earnings = days
            if 0 <= days <= EARNINGS_BLOCKOUT_DAYS:
                res.reasons.append(f"earnings in {days}d")

    res.passed = len(res.reasons) == 0
    return res


def _next_earnings(tk) -> Optional[datetime]:
    """Best-effort next earnings date in UTC. None if unknown."""
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


# ----------------------------------------------------------- Joey's whitelist
# These are the names you said you'd actually trade. Liquid mega-caps + the
# big indexes/ETFs. If a user picks "Joey's method", we restrict the universe
# to this list (plus they can still add custom tickers via the existing UI).
JOEY_WHITELIST: list[str] = [
    # Indexes / ETFs
    "SPY", "QQQ", "IWM", "DIA",
    # Mega-cap tech
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "AMD",
    "NFLX", "AVGO", "ORCL", "CRM", "ADBE",
    # Mega-cap blue chips
    "JPM", "COST", "XOM", "CVX", "WMT", "HD", "UNH", "V", "MA",
    "BAC", "GS", "DIS",
    # High-vol favorites
    "TSLA", "PLTR", "COIN",
]
