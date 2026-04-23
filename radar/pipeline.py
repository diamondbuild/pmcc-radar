"""Scan pipeline: universe → options analysis → scoring.

Parallel fetch using ThreadPoolExecutor (yfinance is I/O bound).
Progress callback signature is (done:int, total:int) — do not pass strings.

Optional IBKR refinement: after the fast yfinance scan ranks all tickers, the
top N rows can be re-fetched through the IBKR proxy for real greeks + live
prices. IBKR-only full scans aren't practical (rate limits + subscription
latency), so hybrid = fast breadth + accurate top picks.
"""
from __future__ import annotations

import concurrent.futures as cf
from datetime import datetime, timezone
from typing import Callable, Optional

import pandas as pd

from . import options, scoring, universe


def _safe_analyze(ticker: str, budget: float):
    try:
        return options.analyze_ticker(ticker, budget=budget)
    except Exception:
        return None


def run_scan(
    budget: float = 3500.0,
    max_workers: int = 16,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    limit: Optional[int] = None,
    force_refresh_universe: bool = False,
    use_tastytrade: bool = False,
    refine_top_n: int = 5,
    refine_progress_cb: Optional[Callable[[int, int], None]] = None,
    # Legacy aliases (IBKR path kept for backwards compat but routes to tastytrade)
    use_ibkr: bool = False,
    ibkr_top_n: Optional[int] = None,
) -> pd.DataFrame:
    """Run a full PMCC scan. Returns ranked DataFrame.

    If ``use_tastytrade`` is True and credentials are configured, the top
    ``refine_top_n`` ranked rows are refined with real greeks + live prices
    from Tastytrade, then the DataFrame is re-scored and re-sorted.
    """
    # Back-compat: old callers may pass use_ibkr / ibkr_top_n
    if use_ibkr and not use_tastytrade:
        use_tastytrade = True
    if ibkr_top_n is not None:
        refine_top_n = ibkr_top_n
    tickers = universe.build_universe(force_refresh=force_refresh_universe)
    if limit:
        tickers = tickers[:limit]
    total = len(tickers)

    rows: list[dict] = []
    done = 0
    if progress_cb:
        try:
            progress_cb(done, total)
        except Exception:
            pass

    with cf.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_safe_analyze, t, budget): t for t in tickers}
        for fut in cf.as_completed(futs):
            done += 1
            res = fut.result()
            if res is not None:
                rows.append(options.result_to_row(res))
            if progress_cb and (done % 2 == 0 or done == total):
                try:
                    progress_cb(done, total)
                except Exception:
                    pass

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = scoring.score_dataframe(df)
    df["source"] = "yfinance"
    df["scanned_at"] = datetime.now(timezone.utc).isoformat()
    df = df.reset_index(drop=True)

    # Optional Tastytrade refinement of top N rows
    if use_tastytrade:
        try:
            from . import tastytrade as tt, tt_refine
            if tt.is_configured():
                df = tt_refine.refine_top_n(
                    df, top_n=refine_top_n, progress_cb=refine_progress_cb
                )
        except Exception as e:
            # Never let refinement break the scan
            import logging
            logging.getLogger("radar.pipeline").warning(
                f"Tastytrade refinement skipped due to error: {e}"
            )

    return df
