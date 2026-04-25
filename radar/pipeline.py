"""Scan pipeline: universe → options analysis → scoring.

Parallel fetch using ThreadPoolExecutor (yfinance is I/O bound).
Progress callback signature is (done:int, total:int) — do not pass strings.

Optional Tastytrade refinement: after the fast yfinance scan ranks all tickers,
the top N rows can be re-fetched through the Tastytrade API for real greeks +
live prices. Full Tastytrade-only scans aren't practical (rate limits + streamer
latency), so hybrid = fast breadth + accurate top picks.
"""
from __future__ import annotations

import concurrent.futures as cf
from datetime import datetime, timezone
from typing import Callable, Optional

import pandas as pd

from . import options, scoring, universe
from .quality_filter import JOEY_WHITELIST, check_quality


def _safe_analyze(ticker: str, budget: float, joey_method: bool = False):
    try:
        return options.analyze_ticker(
            ticker, budget=budget, joey_method=joey_method
        )
    except Exception:
        return None


def _safe_quality_check(ticker: str):
    try:
        return ticker, check_quality(ticker)
    except Exception:
        return ticker, None


def run_scan(
    budget: float = 3500.0,
    max_workers: int = 16,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    limit: Optional[int] = None,
    force_refresh_universe: bool = False,
    use_tastytrade: bool = False,
    refine_top_n: int = 5,
    refine_progress_cb: Optional[Callable[[int, int], None]] = None,
    joey_method: bool = False,
    quality_progress_cb: Optional[Callable[[int, int], None]] = None,
) -> pd.DataFrame:
    """Run a full PMCC scan. Returns ranked DataFrame.

    If ``use_tastytrade`` is True and credentials are configured, the top
    ``refine_top_n`` ranked rows are refined with real greeks + live prices
    from Tastytrade, then the DataFrame is re-scored and re-sorted.

    If ``joey_method`` is True:
      - Universe is restricted to the curated whitelist (liquid mega-caps + ETFs)
      - Each ticker is gated through `quality_filter.check_quality` first:
        $40+ price, 5M+ avg volume, above 200DMA, weeklies, no earnings in 14d
      - `options.analyze_ticker` uses tighter LEAP delta (0.70-0.85), requires
        short strike above LEAP breakeven, and a higher yield floor (~18% ann)
    """
    if joey_method:
        tickers = list(JOEY_WHITELIST)
    else:
        tickers = universe.build_universe(force_refresh=force_refresh_universe)
    if limit:
        tickers = tickers[:limit]

    # Quality pre-gate (Joey's method only): drop tickers that fail hard rules
    # before we spend time pulling option chains for them.
    quality_results: dict[str, object] = {}
    if joey_method:
        passed: list[str] = []
        q_done = 0
        q_total = len(tickers)
        if quality_progress_cb:
            try:
                quality_progress_cb(q_done, q_total)
            except Exception:
                pass
        with cf.ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(_safe_quality_check, t): t for t in tickers}
            for fut in cf.as_completed(futs):
                q_done += 1
                t, qr = fut.result()
                if qr is not None:
                    quality_results[t] = qr
                    if qr.passed:
                        passed.append(t)
                if quality_progress_cb and (q_done % 2 == 0 or q_done == q_total):
                    try:
                        quality_progress_cb(q_done, q_total)
                    except Exception:
                        pass
        tickers = passed

    total = len(tickers)

    rows: list[dict] = []
    done = 0
    if progress_cb:
        try:
            progress_cb(done, total)
        except Exception:
            pass

    with cf.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {
            ex.submit(_safe_analyze, t, budget, joey_method): t
            for t in tickers
        }
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
        # If Joey's method was on, attach a diagnostic frame so the UI
        # can explain why nothing surfaced.
        if joey_method and quality_results:
            from collections import Counter
            reasons: Counter = Counter()
            for qr in quality_results.values():
                for r in (qr.reasons or []):
                    # Bucket reasons into short labels
                    if "price" in r:
                        reasons["price < $40"] += 1
                    elif "avg vol" in r:
                        reasons["avg vol < 5M"] += 1
                    elif "200DMA" in r:
                        reasons["below 200DMA"] += 1
                    elif "earnings" in r:
                        reasons["earnings within 14d"] += 1
                    elif "weeklies" in r or "option expiries" in r:
                        reasons["no weeklies"] += 1
                    else:
                        reasons["other"] += 1
            empty = pd.DataFrame()
            empty.attrs["joey_diagnostics"] = {
                "checked": len(quality_results),
                "passed_quality": sum(
                    1 for qr in quality_results.values() if qr.passed
                ),
                "reasons": dict(reasons),
            }
            return empty
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
