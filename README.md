# 📈 PMCC Radar

Scans S&P 500 + liquid ETFs for the best **Poor Man's Covered Call** setups and ranks them by annualized yield, upside room, liquidity, IV, and earnings safety.

Sister app to [Squeeze Radar](https://github.com/diamondbuild/equity-scanner) — same stack, same design language.

## What it does

For each ticker it tries to build a PMCC pair:
- **LEAP leg:** deep-ITM call, 12–18 months out, ~0.85 delta, cost ≤ your budget
- **Short leg:** OTM call, 25–50 DTE, ~0.25 delta, above the LEAP strike

It then computes full economics (net debit, breakeven, max profit/loss, static yield, annualized yield, upside cap) and ranks by a composite 0–100 score.

## Scoring weights

| Component | Weight |
|---|---|
| Annualized yield | 40% |
| Upside room | 20% |
| Liquidity (OI + spreads) | 15% |
| IV sweet-spot (30–60%) | 10% |
| Earnings safety | 10% |
| LEAP delta quality | 5% |

Plus bonuses for upside > 10%, penalties for earnings-inside-short-window, wide spreads, or thin OI. Hard floor: yield < 5% caps score at 25.

## Stack

- Streamlit (Cloud auto-deploy)
- yfinance (free options chains)
- scipy (Black-Scholes delta — yfinance doesn't expose greeks)
- pandas / numpy

Everything is free. No paid APIs.

## Running locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Disclaimer

Not financial advice. Verify every trade with your own research before entering.
