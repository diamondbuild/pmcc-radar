"""Scan pipeline: universe → options analysis → scoring.

Parallel fetch using ThreadPoolExecutor (yfinance is I/O bound).
Progress callback signature is (done:int, total:int) — do not pass strings.
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
) -> pd.DataFrame:
    """Run a full PMCC scan. Returns ranked DataFrame."""
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
    df["scanned_at"] = datetime.now(timezone.utc).isoformat()
    return df.reset_index(drop=True)
