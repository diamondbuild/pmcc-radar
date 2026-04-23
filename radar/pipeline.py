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
    use_ibkr: bool = False,
    ibkr_top_n: int = 20,
    refine_progress_cb: Optional[Callable[[int, int], None]] = None,
) -> pd.DataFrame:
    """Run a full PMCC scan. Returns ranked DataFrame.

    If ``use_ibkr`` is True and the IBKR proxy is configured+healthy, the top
    ``ibkr_top_n`` ranked rows are refined with real IBKR greeks + live prices,
    then the DataFrame is re-scored and re-sorted.
    """
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

    # Optional IBKR refinement of top N rows
    if use_ibkr:
        try:
            from . import ibkr, ibkr_refine
            if ibkr.is_configured():
                health = ibkr.health()
                if health.get("ok") and health.get("gateway_connected"):
                    df = ibkr_refine.refine_top_n(
                        df, top_n=ibkr_top_n, progress_cb=refine_progress_cb
                    )
        except Exception as e:
            # Never let IBKR refinement break the scan
            import logging
            logging.getLogger("radar.pipeline").warning(
                f"IBKR refinement skipped due to error: {e}"
            )

    return df
