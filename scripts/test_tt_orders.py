"""Smoke test tt_orders.py — builds a PMCC order and dry-runs it.
NEVER submits a real order.
"""
import sys
from radar import tastytrade as tt, tt_orders


def main():
    # Use a cheap, liquid symbol for preview (won't place real order)
    # Pick AAPL ~2027 LEAP deep ITM + ~35 DTE short — fetch live chain for fresh prices
    # Cheap symbol the small account can afford in dry-run
    under = "F"
    expiries = tt.get_expiries(under) or []
    print(f"AAPL expiries (first 10): {expiries[:10]}")
    if not expiries:
        print("FAIL: no expiries")
        sys.stdout.flush()
        return

    # Pick a LEAP-like expiry (>330 DTE) and a short (~30 DTE)
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).date()

    def dte(e):
        return (datetime.strptime(e, "%Y%m%d").date() - today).days

    leap_exp = next((e for e in expiries if dte(e) > 330), None)
    short_exp = next((e for e in expiries if 20 <= dte(e) <= 55), None)
    print(f"LEAP expiry: {leap_exp} ({dte(leap_exp)}d)")
    print(f"Short expiry: {short_exp} ({dte(short_exp)}d)")

    leap_chain = tt.get_chain(under, leap_exp)
    short_chain = tt.get_chain(under, short_exp)
    if not leap_chain or not short_chain:
        print("FAIL: missing chain")
        sys.stdout.flush()
        return

    spot = leap_chain.get("spot") or 0
    print(f"Spot AAPL: {spot}")

    # Pick a deep ITM LEAP (delta ~0.85, roughly 25-30% below spot)
    leap_strike_target = spot * 0.75
    leap = min(leap_chain["calls"], key=lambda c: abs((c.get("strike") or 0) - leap_strike_target))
    # Pick OTM short (~5-8% above spot)
    short_strike_target = spot * 1.05
    short = min(
        (c for c in short_chain["calls"] if c.get("strike", 0) > spot),
        key=lambda c: abs(c.get("strike") - short_strike_target),
    )
    print(f"LEAP: strike={leap['strike']} bid={leap.get('bid')} ask={leap.get('ask')}")
    print(f"Short: strike={short['strike']} bid={short.get('bid')} ask={short.get('ask')}")

    def mid(c):
        b, a = c.get("bid") or 0, c.get("ask") or 0
        return round((b + a) / 2, 2) if (b and a) else (b or a)
    order, details = tt_orders.build_pmcc_open(
        ticker=under,
        leap_expiry=leap_exp, leap_strike=leap["strike"], leap_mid=mid(leap),
        short_expiry=short_exp, short_strike=short["strike"], short_mid=mid(short),
        qty=1,
    )
    print()
    print("Order description:", details["description"])
    print("Combo mid:", details["net_mid"])
    print("Est cost:", details["est_cost"])
    print()
    print("Calling dry_run preview...")
    preview = tt_orders.preview_order(order)
    print("Preview OK:", preview["ok"])
    print("Price:", preview["price"], preview["price_effect"])
    print("Fees total:", preview["fees_total"])
    print("BP change:", preview["bp_change"], preview["bp_change_effect"])
    if preview["warnings"]:
        print("WARNINGS:")
        for w in preview["warnings"]:
            print(" -", w)
    if preview["errors"]:
        print("ERRORS:")
        for e in preview["errors"]:
            print(" -", e)
    sys.stdout.flush()


if __name__ == "__main__":
    main()
