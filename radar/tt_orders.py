"""Order builders for PMCC entries and roll adjustments via Tastytrade.

Two public entry points wrap the SDK's NewOrder construction, pricing, and
place_order flow:
  build_pmcc_open(ticker, leap_expiry, leap_strike, leap_bid, leap_ask,
                  short_expiry, short_strike, short_bid, short_ask, qty=1)
      -> (NewOrder, details_dict)
  build_short_roll(under, close_symbol, close_bid, close_ask,
                   roll_expiry, roll_strike, roll_bid, roll_ask, qty=1)
      -> (NewOrder, details_dict)

Each returns a tuple of (order, details) where details is a dict with
  {legs:[...], net_mid, price_effect, order_type, time_in_force, description}
suitable for rendering a confirmation popup.

preview_order(order) -> dict performs a dry_run and returns cost + fees + BP.
submit_order(order)  -> dict performs the real order POST.

All functions use the same thread+fresh-loop pattern as radar.tastytrade.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from . import tastytrade as tt


# -------------------------------------------------------- OCC symbol construction
def _occ(root: str, expiry_yyyymmdd: str, right: str, strike: float) -> str:
    """Build an OCC-style option symbol. expiry accepts YYYYMMDD or YYYY-MM-DD."""
    e = expiry_yyyymmdd.replace("-", "")
    if len(e) != 8:
        raise ValueError(f"bad expiry: {expiry_yyyymmdd!r}")
    yymmdd = e[2:]
    right = right.upper()
    if right not in ("C", "P"):
        raise ValueError(f"right must be C or P, got {right!r}")
    strike_thou = int(round(strike * 1000))
    root_padded = f"{root:<6}"[:6]
    return f"{root_padded}{yymmdd}{right}{strike_thou:08d}"


def _mid(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    """Midpoint of bid/ask. Falls back to whichever is present, else None."""
    b = float(bid) if bid else 0.0
    a = float(ask) if ask else 0.0
    if b > 0 and a > 0:
        return round((b + a) / 2, 2)
    return round(b or a, 2) if (b or a) else None


def _round_cents(v: float) -> Decimal:
    """Round to nearest cent, Decimal for SDK."""
    return Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# -------------------------------------------------------- Order builders
def build_pmcc_open(
    ticker: str,
    leap_expiry: str,
    leap_strike: float,
    leap_mid: float,
    short_expiry: str,
    short_strike: float,
    short_mid: float,
    qty: int = 1,
):
    """Build a two-leg PMCC opening order.
      Leg 1: Buy-to-Open the LEAP call (debit)
      Leg 2: Sell-to-Open the short call (credit)
    Combo net = leap_mid - short_mid. Always a net debit for a valid PMCC.

    ``leap_mid`` and ``short_mid`` are per-contract mid prices in dollars
    (not cents-per-share). Tastytrade limit orders are in dollars per share.

    Returns (NewOrder, details_dict). Raises ValueError on bad inputs.
    """
    from tastytrade.order import (
        InstrumentType, Leg, NewOrder, OrderAction, OrderType, OrderTimeInForce,
    )

    if not leap_mid or not short_mid:
        raise ValueError("missing mid price for one or both legs")
    net = round(leap_mid - short_mid, 2)
    if net <= 0:
        # Shouldn't happen for a legit PMCC, but guard anyway
        raise ValueError(f"combo mid {net:.2f} is not a debit; check inputs")

    leap_sym = _occ(ticker, leap_expiry, "C", leap_strike)
    short_sym = _occ(ticker, short_expiry, "C", short_strike)

    legs = [
        Leg(
            instrument_type=InstrumentType.EQUITY_OPTION,
            symbol=leap_sym,
            action=OrderAction.BUY_TO_OPEN,
            quantity=Decimal(qty),
        ),
        Leg(
            instrument_type=InstrumentType.EQUITY_OPTION,
            symbol=short_sym,
            action=OrderAction.SELL_TO_OPEN,
            quantity=Decimal(qty),
        ),
    ]
    # SDK v12 infers DEBIT/CREDIT from the sign of price: NEGATIVE = debit, POSITIVE = credit.
    # A PMCC open is always a debit, so price is negative.
    order = NewOrder(
        time_in_force=OrderTimeInForce.DAY,
        order_type=OrderType.LIMIT,
        legs=legs,
        price=_round_cents(-net),  # negative = debit
    )
    details = {
        "kind": "pmcc_open",
        "ticker": ticker,
        "qty": qty,
        "legs": [
            {"action": "Buy to Open", "symbol": leap_sym, "expiry": leap_expiry,
             "strike": leap_strike, "mid": leap_mid, "right": "C"},
            {"action": "Sell to Open", "symbol": short_sym, "expiry": short_expiry,
             "strike": short_strike, "mid": short_mid, "right": "C"},
        ],
        "net_mid": net,
        "price_effect": "DEBIT",
        "est_cost": net * 100 * qty,     # per contract = 100 shares
        "order_type": "LIMIT",
        "time_in_force": "DAY",
        "description": (
            f"{ticker} PMCC: BTO {leap_expiry} {leap_strike:.0f}C / "
            f"STO {short_expiry} {short_strike:.0f}C, "
            f"LIMIT ${net:.2f} DEBIT, DAY x{qty}"
        ),
    }
    return order, details


def build_short_roll(
    under: str,
    close_symbol: str,
    close_mid: float,
    roll_expiry: str,
    roll_strike: float,
    roll_mid: float,
    qty: int = 1,
):
    """Build a two-leg diagonal roll of a short call:
      Leg 1: Buy-to-Close the current short (debit)
      Leg 2: Sell-to-Open the new short    (credit)
    Priced at combo mid. Usually a net credit when rolling for premium.

    ``close_symbol`` must be the existing short's OCC symbol (as stored on
    the position). Mids are per-share dollar prices for each leg.
    """
    from tastytrade.order import (
        InstrumentType, Leg, NewOrder, OrderAction, OrderType, OrderTimeInForce,
    )

    if not close_mid or not roll_mid:
        raise ValueError("missing mid price for one or both legs")

    # Combo net from our perspective: pay close_mid, receive roll_mid
    net = round(roll_mid - close_mid, 2)
    price_effect = "CREDIT" if net >= 0 else "DEBIT"
    price_val = abs(net)

    roll_sym = _occ(under, roll_expiry, "C", roll_strike)

    legs = [
        Leg(
            instrument_type=InstrumentType.EQUITY_OPTION,
            symbol=close_symbol.strip(),
            action=OrderAction.BUY_TO_CLOSE,
            quantity=Decimal(qty),
        ),
        Leg(
            instrument_type=InstrumentType.EQUITY_OPTION,
            symbol=roll_sym,
            action=OrderAction.SELL_TO_OPEN,
            quantity=Decimal(qty),
        ),
    ]
    # SDK v12 infers DEBIT/CREDIT from the sign of price: NEGATIVE = debit, POSITIVE = credit.
    # net = roll_mid - close_mid. If positive, credit (keep sign). If negative, debit.
    order = NewOrder(
        time_in_force=OrderTimeInForce.DAY,
        order_type=OrderType.LIMIT,
        legs=legs,
        price=_round_cents(net),
    )
    details = {
        "kind": "short_roll",
        "ticker": under,
        "qty": qty,
        "legs": [
            {"action": "Buy to Close", "symbol": close_symbol.strip(),
             "expiry": "", "strike": "", "mid": close_mid, "right": "C"},
            {"action": "Sell to Open", "symbol": roll_sym, "expiry": roll_expiry,
             "strike": roll_strike, "mid": roll_mid, "right": "C"},
        ],
        "net_mid": net,
        "price_effect": price_effect,
        "est_credit": net * 100 * qty,   # positive for credit, negative for debit
        "order_type": "LIMIT",
        "time_in_force": "DAY",
        "description": (
            f"{under} roll: BTC short / STO {roll_expiry} {roll_strike:.0f}C, "
            f"LIMIT ${price_val:.2f} {price_effect}, DAY x{qty}"
        ),
    }
    return order, details


# -------------------------------------------------------- Preview / submit
def _place(order, dry_run: bool) -> dict:
    """Call Account.place_order(..., dry_run=...) in a fresh thread+loop."""
    from tastytrade import Account

    def factory():
        async def inner():
            session = tt._make_session()
            await session.refresh(force=True)
            acct = await Account.get(session, tt.ACCOUNT_NUMBER)
            resp = await acct.place_order(session, order, dry_run=dry_run)
            return resp
        return inner()

    resp = tt._call_in_thread(factory, timeout=20.0)
    # Pydantic model → dict
    try:
        d = resp.model_dump(by_alias=False)
    except Exception:
        d = dict(resp.__dict__)
    return d


def preview_order(order) -> dict:
    """Dry-run the order to get fees, buying-power impact, and any warnings.
    Broker-side rejections (margin, concentration, invalid price) are caught
    and returned in the ``errors`` list so the UI can show them.
    """
    try:
        raw = _place(order, dry_run=True)
    except Exception as e:
        msg = str(e)
        return {
            "ok": False,
            "errors": [msg],
            "warnings": [],
            "order_id_preview": None,
            "price": None,
            "price_effect": None,
            "fees_total": None,
            "bp_change": None,
            "bp_change_effect": None,
            "raw": {"exception": msg},
        }
    # Pull out the friendly fields we care about
    po = raw.get("order") or {}
    bp = raw.get("buying_power_effect") or {}
    fee = raw.get("fee_calculation") or {}
    warnings = raw.get("warnings") or []
    errors = raw.get("errors") or []
    return {
        "ok": not errors,
        "errors": [e.get("message") if isinstance(e, dict) else str(e) for e in errors],
        "warnings": [w.get("message") if isinstance(w, dict) else str(w) for w in warnings],
        "order_id_preview": po.get("id"),
        "price": po.get("price"),
        "price_effect": po.get("price_effect"),
        "fees_total": fee.get("total_fees") or fee.get("regulatory_fees"),
        "bp_change": bp.get("change_in_buying_power"),
        "bp_change_effect": bp.get("change_in_buying_power_effect"),
        "raw": raw,
    }


def submit_order(order) -> dict:
    """Submit the order for real. Returns the placed order response."""
    try:
        raw = _place(order, dry_run=False)
    except Exception as e:
        return {
            "ok": False,
            "errors": [str(e)],
            "order_id": None,
            "status": None,
            "price": None,
            "price_effect": None,
            "raw": {"exception": str(e)},
        }
    po = raw.get("order") or {}
    return {
        "ok": not raw.get("errors"),
        "errors": [e.get("message") if isinstance(e, dict) else str(e)
                   for e in (raw.get("errors") or [])],
        "order_id": po.get("id"),
        "status": po.get("status"),
        "price": po.get("price"),
        "price_effect": po.get("price_effect"),
        "raw": raw,
    }
