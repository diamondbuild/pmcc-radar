"""Diagnose why PMCC scanner finds no opportunities.

Traces a few known-liquid tickers through the full pipeline and reports
exactly where each one is rejected (no LEAP, no short, quality gate, etc.).
"""
from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import math
from radar import options as opt
import yfinance as yf

TEST_TICKERS = ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA", "GLD", "XLF", "AMD", "F"]
BUDGETS = [3500, 7500, 15000]


def trace(ticker: str, budget: float):
    print(f"\n{'='*70}")
    print(f"TICKER: {ticker}  |  BUDGET: ${budget}")
    print('='*70)
    tk = yf.Ticker(ticker)

    try:
        spot = opt._spot_price(tk)
    except Exception as e:
        print(f"  ❌ spot_price failed: {e}")
        return
    print(f"  spot: ${spot:.2f}")

    try:
        expirations = list(tk.options)
    except Exception as e:
        print(f"  ❌ options list failed: {e}")
        return
    print(f"  expirations: {len(expirations)} available")
    if not expirations:
        print("  ❌ no expirations")
        return

    leap_exp = opt._pick_leap_expiry(expirations)
    short_exp = opt._pick_short_expiry(expirations)
    print(f"  LEAP expiry: {leap_exp}")
    print(f"  short expiry: {short_exp}")
    if not leap_exp or not short_exp:
        print("  ❌ couldn't pick expiries")
        return

    try:
        leap_chain = opt._get_chain(tk, leap_exp)
        short_chain = opt._get_chain(tk, short_exp)
    except Exception as e:
        print(f"  ❌ chain fetch failed: {e}")
        return

    if leap_chain is None or leap_chain.empty:
        print(f"  ❌ LEAP chain empty")
        return
    if short_chain is None or short_chain.empty:
        print(f"  ❌ short chain empty")
        return

    print(f"  LEAP chain rows: {len(leap_chain)}")
    print(f"  short chain rows: {len(short_chain)}")

    # IV sanity
    iv_col = leap_chain.get("impliedVolatility")
    if iv_col is not None:
        ivs = iv_col.dropna().tolist()
        if ivs:
            print(f"  LEAP IV range: {min(ivs):.2f} – {max(ivs):.2f}  (sane if 0.05–3.0)")
            saneish = [v for v in ivs if 0.05 <= v <= 3.0]
            print(f"  LEAP rows with sane IV: {len(saneish)}/{len(ivs)}")
        else:
            print(f"  ❌ LEAP has no IV data at all")

    iv_col2 = short_chain.get("impliedVolatility")
    if iv_col2 is not None:
        ivs2 = iv_col2.dropna().tolist()
        if ivs2:
            print(f"  short IV range: {min(ivs2):.2f} – {max(ivs2):.2f}  (sane if 0.05–3.0)")
            saneish2 = [v for v in ivs2 if 0.05 <= v <= 3.0]
            print(f"  short rows with sane IV: {len(saneish2)}/{len(ivs2)}")
        else:
            print(f"  ❌ short has no IV data at all")

    # Try LEAP selection
    leap = opt._select_leap(leap_chain, spot, budget)
    if leap is None:
        # Peek why
        ci = leap_chain.copy()
        ci = ci[ci["mid"] > 0]
        print(f"  LEAP filter: {len(ci)} with mid>0")
        ci = ci[ci["mid"] * 100 <= budget]
        print(f"  LEAP filter: {len(ci)} within ${budget} budget")
        ci = ci[ci["strike"] < spot]
        print(f"  LEAP filter: {len(ci)} ITM (strike<spot)")
        if not ci.empty:
            ci2 = ci[ci["impliedVolatility"].apply(opt._sane_iv)]
            print(f"  LEAP filter: {len(ci2)} with sane IV (5%-300%)")
            if not ci2.empty:
                ci2 = ci2.copy()
                ci2["delta"] = ci2.apply(
                    lambda r: opt.bs_call_delta(
                        spot, float(r["strike"]), float(r["dte"]),
                        float(r["impliedVolatility"]),
                    ), axis=1)
                print(f"  LEAP delta range among survivors: {ci2['delta'].min():.2f} – {ci2['delta'].max():.2f}")
                print(f"  LEAP rows with delta in 0.80–0.92 band: {((ci2['delta'] >= 0.80) & (ci2['delta'] <= 0.92)).sum()}")
                print(f"  LEAP rows with delta in 0.70–0.95 fallback band: {((ci2['delta'] >= 0.70) & (ci2['delta'] <= 0.95)).sum()}")
        print("  ❌ LEAP selection returned None")
        return

    print(f"  ✓ LEAP picked: strike ${leap['strike']}, mid ${leap['mid']:.2f}, "
          f"cost ${leap['mid']*100:.0f}, IV {leap['impliedVolatility']:.2f}")

    # Try short selection
    short = opt._select_short(short_chain, spot, float(leap["strike"]))
    if short is None:
        ci = short_chain.copy()
        ci = ci[ci["mid"] > 0]
        print(f"  short filter: {len(ci)} with mid>0")
        ci = ci[ci["strike"] > spot]
        print(f"  short filter: {len(ci)} OTM (strike>spot)")
        ci = ci[ci["strike"] > float(leap["strike"])]
        print(f"  short filter: {len(ci)} above LEAP strike ${leap['strike']}")
        if not ci.empty:
            ci2 = ci[ci["impliedVolatility"].apply(opt._sane_iv)]
            print(f"  short filter: {len(ci2)} with sane IV")
            if not ci2.empty:
                ci2 = ci2.copy()
                ci2["delta"] = ci2.apply(
                    lambda r: opt.bs_call_delta(
                        spot, float(r["strike"]), float(r["dte"]),
                        float(r["impliedVolatility"]),
                    ), axis=1)
                print(f"  short delta range: {ci2['delta'].min():.3f} – {ci2['delta'].max():.3f}")
                print(f"  short rows in 0.20–0.32 band: {((ci2['delta'] >= 0.20) & (ci2['delta'] <= 0.32)).sum()}")
                print(f"  short rows in 0.10–0.45 fallback: {((ci2['delta'] >= 0.10) & (ci2['delta'] <= 0.45)).sum()}")
        print("  ❌ short selection returned None")
        return

    print(f"  ✓ short picked: strike ${short['strike']}, mid ${short['mid']:.2f}, "
          f"IV {short['impliedVolatility']:.2f}")

    # Economics
    leap_cost = float(leap["mid"]) * 100.0
    short_prem = float(short["mid"]) * 100.0
    net_debit = leap_cost - short_prem
    max_profit = (float(short["strike"]) - float(leap["strike"])) * 100.0 - net_debit
    upside_cap = (float(short["strike"]) - spot) / spot
    static = short_prem / net_debit if net_debit > 0 else 0
    annualized = static * (365.0 / float(short["dte"]))

    print(f"  net_debit: ${net_debit:.0f}  (gate: >0)")
    print(f"  max_profit: ${max_profit:.0f}  (gate: >0)")
    print(f"  upside_cap: {upside_cap*100:.1f}%  (gate: >=1%)")
    print(f"  annualized: {annualized*100:.1f}%  (gate: <=200%)")

    gates = []
    if net_debit <= 0: gates.append("net_debit<=0")
    if max_profit <= 0: gates.append("max_profit<=0")
    if upside_cap < 0.01: gates.append("upside_cap<1%")
    if annualized > 2.0: gates.append("annualized>200%")

    if gates:
        print(f"  ❌ REJECTED by gates: {', '.join(gates)}")
    else:
        print(f"  ✅ PASSES all gates")


if __name__ == "__main__":
    budget = float(sys.argv[1]) if len(sys.argv) > 1 else 3500.0
    for t in TEST_TICKERS:
        trace(t, budget)
