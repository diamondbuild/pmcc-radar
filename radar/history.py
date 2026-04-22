"""Snapshot storage + aggregate for tracking PMCC picks over time."""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd

HIST_DIR = os.path.join(os.path.dirname(__file__), "..", "history")
AGGREGATE_PATH = os.path.join(HIST_DIR, "aggregate.csv")

SNAPSHOT_COLS = [
    "ticker", "spot", "score", "annualized_yield", "static_yield",
    "leap_expiry", "leap_dte", "leap_strike", "leap_cost", "leap_delta",
    "leap_iv", "leap_oi", "leap_spread",
    "short_expiry", "short_dte", "short_strike", "short_premium",
    "short_delta", "short_iv", "short_oi", "short_spread",
    "breakeven", "max_profit", "max_loss", "net_debit", "upside_cap_pct",
    "iv_rank", "earnings_before_short_expiry", "next_earnings", "warnings",
    "scanned_at",
]


def _ensure_dir():
    os.makedirs(HIST_DIR, exist_ok=True)


def save_snapshot(df: pd.DataFrame) -> str:
    """Save a ranked snapshot. Returns file path."""
    _ensure_dir()
    if df.empty:
        return ""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(HIST_DIR, f"snap_{ts}.csv")
    out = df.copy()
    for col in SNAPSHOT_COLS:
        if col not in out.columns:
            out[col] = None
    out = out[SNAPSHOT_COLS]
    out.to_csv(path, index=False)
    _append_aggregate(out)
    return path


def _append_aggregate(df: pd.DataFrame) -> None:
    try:
        if os.path.exists(AGGREGATE_PATH):
            prior = pd.read_csv(AGGREGATE_PATH)
            # Parse timestamp BEFORE concat to avoid mixed-format NaT (the bug
            # we hit on Squeeze Radar).
            for frame in (prior, df):
                if "scanned_at" in frame.columns:
                    frame["scanned_at"] = pd.to_datetime(
                        frame["scanned_at"], errors="coerce", utc=True
                    )
            combined = pd.concat([prior, df], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["ticker", "scanned_at"], keep="last"
            )
        else:
            combined = df.copy()
            combined["scanned_at"] = pd.to_datetime(
                combined["scanned_at"], errors="coerce", utc=True
            )
        combined = combined.sort_values("scanned_at")
        combined.to_csv(AGGREGATE_PATH, index=False)
    except Exception:
        pass


def load_aggregate() -> pd.DataFrame:
    if not os.path.exists(AGGREGATE_PATH):
        return pd.DataFrame()
    try:
        df = pd.read_csv(AGGREGATE_PATH)
        if "scanned_at" in df.columns:
            df["scanned_at"] = pd.to_datetime(df["scanned_at"], errors="coerce", utc=True)
        return df
    except Exception:
        return pd.DataFrame()


def latest_snapshot() -> pd.DataFrame:
    """Most recent ranked snapshot (from aggregate)."""
    agg = load_aggregate()
    if agg.empty:
        return pd.DataFrame()
    latest_ts = agg["scanned_at"].max()
    return agg[agg["scanned_at"] == latest_ts].sort_values("score", ascending=False)


def list_snapshots() -> list[str]:
    if not os.path.exists(HIST_DIR):
        return []
    return sorted(
        [f for f in os.listdir(HIST_DIR) if f.startswith("snap_") and f.endswith(".csv")]
    )
