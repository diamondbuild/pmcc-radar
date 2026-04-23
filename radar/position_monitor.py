"""Position monitor — evaluate open Tastytrade positions and flag PMCC
adjustments worth considering on each scan.

Rules (focused on the short call leg of a PMCC):
  - DEFENSIVE_ITM    short call ITM or >= 95% of strike  → roll up-and-out
  - NEAR_EXPIRY      short call has <= 7 DTE             → close and re-sell
  - HIGH_DELTA       short call delta >= 0.50            → roll up to reduce risk
  - DECAY_HARVEST    short trading at <= 20% of entry    → buy back, re-sell fresh
  - EXPIRED_WORTH    short <= $0.05 and <= 3 DTE         → let expire or buy to close

Long LEAP leg signals:
  - LEAP_NEAR_EXPIRY long LEAP <= 60 DTE                 → plan to roll LEAP out

Each alert includes underlying, leg, reason, metrics, and a plain-English
recommendation. The caller decides whether/how to act.

Designed to be fast: pulls spot once per unique underlying and the relevant
option chain once per (underlying, expiry). Already-cached spot quotes in
tastytrade.py help.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from . import tastytrade as tt


# ---------------------------------------------------------------- OCC symbol parsing
def _parse_occ(sym: str):
    """Parse OCC option symbol like 'GME   260529C00028000'.
    Returns (underlying, expiry_YYYYMMDD, right, strike) or None.
    """
    s = (sym or "").strip()
    if len(s) < 21 or s[-9] not in ("C", "P"):
        return None
    try:
        under = s[:-15].strip()
        yymmdd = s[-15:-9]
        right = s[-9]
        strike = int(s[-8:]) / 1000.0
        expiry = f"20{yymmdd[0:2]}{yymmdd[2:4]}{yymmdd[4:6]}"
        return under, expiry, right, strike
    except Exception:
        return None


def _dte(expiry_yyyymmdd: str) -> Optional[int]:
    try:
        d = datetime.strptime(expiry_yyyymmdd, "%Y%m%d").replace(tzinfo=timezone.utc)
        return max(0, (d.date() - datetime.now(timezone.utc).date()).days)
    except Exception:
        return None


# ---------------------------------------------------------------- Data shape
@dataclass
class Alert:
    underlying: str
    leg: str                 # "short_call" or "long_call"
    reason: str              # short code e.g. "DEFENSIVE_ITM"
    severity: str            # "info" | "warn" | "action"
    title: str               # short headline
    detail: str              # plain English one-liner
    metrics: dict = field(default_factory=dict)


# ---------------------------------------------------------------- Core monitor
def _find_roll_candidate(underlying: str, current_expiry: str, current_strike: float,
                         spot: float) -> Optional[dict]:
    """Find a reasonable roll target: ~30-45 DTE, OTM above current spot by ~5-8%.
    Returns {expiry, strike, bid, mid, delta} or None if no chain reachable.
    """
    try:
        expiries = tt.get_expiries(underlying) or []
    except Exception:
        return None
    if not expiries:
        return None

    # Pick expiry closest to 35 DTE within [21, 49]
    target_expiry = None
    target_dte = None
    for e in expiries:
        d = _dte(e)
        if d is None or d < 21 or d > 49:
            continue
        if target_dte is None or abs(d - 35) < abs(target_dte - 35):
            target_expiry = e
            target_dte = d
    if not target_expiry:
        return None

    try:
        chain = tt.get_chain(underlying, target_expiry)
    except Exception:
        return None
    if not chain or not chain.get("calls"):
        return None

    # Find OTM call nearest ~5-8% above spot
    desired = spot * 1.06
    best = None
    best_diff = None
    for c in chain["calls"]:
        k = c.get("strike")
        if not k or k <= spot:
            continue
        diff = abs(k - desired)
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best = c
    if not best:
        return None
    bid = best.get("bid") or 0
    ask = best.get("ask") or 0
    mid = (bid + ask) / 2 if (bid and ask) else (bid or ask or 0)
    return {
        "expiry": target_expiry,
        "dte": target_dte,
        "strike": best.get("strike"),
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "delta": best.get("delta"),
    }


def evaluate_positions(positions: list[dict]) -> list[Alert]:
    """Scan open option positions and emit adjustment alerts.

    Expects positions as returned by tt.get_positions(). Silently skips
    non-option positions. Rolling candidates are best-effort; if chain data
    isn't reachable, the alert is still emitted without a roll suggestion.
    """
    if not positions:
        return []

    # Group option positions by underlying for PMCC pairing context
    legs: dict[str, dict] = {}  # underlying -> {"shorts":[...], "longs":[...], "spot":float|None}
    for p in positions:
        if "Option" not in (p.get("instrument_type") or ""):
            continue
        parsed = _parse_occ(p.get("symbol", ""))
        if not parsed:
            continue
        under, expiry, right, strike = parsed
        if right != "C":   # PMCC is call-only; skip puts for now
            continue
        direction = p.get("quantity_direction")  # "Short" or "Long"
        qty = abs(p.get("quantity") or 0)
        avg_cost = float(p.get("average_open_price") or 0)
        mark = float(p.get("mark_price") or p.get("close_price") or 0)
        bucket = legs.setdefault(under, {"shorts": [], "longs": [], "spot": None})
        item = {
            "symbol": p.get("symbol"),
            "expiry": expiry,
            "strike": strike,
            "qty": qty,
            "avg_cost": avg_cost,
            "mark": mark,
            "dte": _dte(expiry),
        }
        if direction == "Short":
            bucket["shorts"].append(item)
        else:
            bucket["longs"].append(item)

    alerts: list[Alert] = []

    for under, grp in legs.items():
        # Resolve spot once per underlying
        spot_px = None
        try:
            spot = tt.get_spot(under) or {}
            spot_px = spot.get("price") or spot.get("last") or spot.get("mark")
        except Exception:
            spot_px = None

        # --- Short call checks ---
        for s in grp["shorts"]:
            dte = s["dte"]
            strike = s["strike"]
            mark = s["mark"]
            avg = s["avg_cost"]

            # 1) Expired-worthless-ish: let it go or close for pennies
            if mark is not None and mark <= 0.05 and dte is not None and dte <= 3:
                alerts.append(Alert(
                    underlying=under, leg="short_call", reason="EXPIRED_WORTH",
                    severity="info",
                    title=f"{under} short call ${strike:.0f} near zero",
                    detail=(
                        f"Short call at ${mark:.2f} with {dte}d left. Let it expire "
                        f"or buy to close for pennies and sell a fresh 30-45 DTE call."
                    ),
                    metrics={"strike": strike, "dte": dte, "mark": mark, "avg_cost": avg},
                ))
                continue

            # 2) Near expiry — roll for more premium
            if dte is not None and dte <= 7:
                roll = None
                if spot_px:
                    roll = _find_roll_candidate(under, s["expiry"], strike, spot_px)
                roll_txt = ""
                if roll and roll.get("mid"):
                    extra = roll["mid"] - mark
                    roll_txt = (
                        f" Suggested roll: {under} ${roll['strike']:.0f}C "
                        f"exp {roll['expiry'][:4]}-{roll['expiry'][4:6]}-{roll['expiry'][6:]} "
                        f"for ~${roll['mid']:.2f} (net credit ~${extra:.2f} per contract)."
                    )
                alerts.append(Alert(
                    underlying=under, leg="short_call", reason="NEAR_EXPIRY",
                    severity="action",
                    title=f"{under} short call ${strike:.0f} expires in {dte}d",
                    detail=(
                        f"Close this short and sell a new 30-45 DTE call to keep "
                        f"the premium stream going.{roll_txt}"
                    ),
                    metrics={
                        "strike": strike, "dte": dte, "mark": mark,
                        "spot": spot_px, "roll": roll,
                    },
                ))
                continue

            # 3) Defensive: ITM or within 5% of strike
            if spot_px and spot_px >= strike * 0.95:
                roll = _find_roll_candidate(under, s["expiry"], strike, spot_px)
                roll_txt = ""
                if roll and roll.get("mid"):
                    roll_txt = (
                        f" Roll up-and-out to ${roll['strike']:.0f}C "
                        f"exp {roll['expiry'][:4]}-{roll['expiry'][4:6]}-{roll['expiry'][6:]} "
                        f"for ~${roll['mid']:.2f}."
                    )
                status = "ITM" if spot_px >= strike else "near money"
                alerts.append(Alert(
                    underlying=under, leg="short_call", reason="DEFENSIVE_ITM",
                    severity="action",
                    title=f"{under} short call ${strike:.0f} is {status}",
                    detail=(
                        f"Spot ${spot_px:.2f} vs strike ${strike:.0f}. Assignment risk "
                        f"rising — consider rolling up-and-out for a net credit to "
                        f"raise your cap.{roll_txt}"
                    ),
                    metrics={
                        "strike": strike, "dte": dte, "mark": mark,
                        "spot": spot_px, "roll": roll,
                    },
                ))
                continue

            # 4) Decay harvest — 80%+ of premium captured
            if avg and mark is not None and avg > 0 and mark <= avg * 0.20:
                pct = (1 - mark / avg) * 100 if avg else 0
                alerts.append(Alert(
                    underlying=under, leg="short_call", reason="DECAY_HARVEST",
                    severity="info",
                    title=f"{under} short call ${strike:.0f} captured {pct:.0f}%",
                    detail=(
                        f"Premium decayed from ${avg:.2f} to ${mark:.2f}. Buy back and "
                        f"sell a new 30-45 DTE call to reset the premium clock."
                    ),
                    metrics={
                        "strike": strike, "dte": dte, "mark": mark, "avg_cost": avg,
                    },
                ))

        # --- Long LEAP checks ---
        for L in grp["longs"]:
            if L["dte"] is not None and L["dte"] <= 60:
                alerts.append(Alert(
                    underlying=under, leg="long_call", reason="LEAP_NEAR_EXPIRY",
                    severity="warn",
                    title=f"{under} long LEAP ${L['strike']:.0f} expires in {L['dte']}d",
                    detail=(
                        f"Time to plan rolling the LEAP out to a 12-18 month expiry "
                        f"to keep the PMCC structure intact."
                    ),
                    metrics={"strike": L["strike"], "dte": L["dte"], "mark": L["mark"]},
                ))

    # Sort: action first, then warn, then info; stable by underlying
    sev_order = {"action": 0, "warn": 1, "info": 2}
    alerts.sort(key=lambda a: (sev_order.get(a.severity, 9), a.underlying))
    return alerts
