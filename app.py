"""PMCC Radar — Poor Man's Covered Call scanner.

Finds the best LEAP + short-call pairs across S&P 500 + liquid ETFs.
Ranks by annualized yield, upside room, liquidity, IV, and earnings safety.

Version: 2.1 (hybrid yfinance + IBKR refinement)
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

# Make local package importable when run from Streamlit Cloud
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from radar import history, ibkr, pipeline, ui, universe


def _safe_int(v, default=0):
    try:
        if v is None:
            return default
        f = float(v)
        if pd.isna(f):
            return default
        return int(f)
    except Exception:
        return default


def _safe_float(v, default=float("nan")):
    try:
        if v is None:
            return default
        f = float(v)
        return default if pd.isna(f) else f
    except Exception:
        return default


def _dte_from_expiry(expiry_str: str) -> int:
    """Compute days-to-expiry from ISO date string. Returns 0 on failure."""
    try:
        from datetime import datetime as _dt, timezone as _tz
        exp = _dt.strptime(str(expiry_str), "%Y-%m-%d").replace(tzinfo=_tz.utc)
        return max(0, (exp - _dt.now(_tz.utc)).days)
    except Exception:
        return 0


def _get(row, key, default=None):
    """Safe getter for pandas Series — returns default if key absent/NaN."""
    try:
        v = row[key] if key in row.index else default
    except Exception:
        v = default
    if v is None:
        return default
    try:
        if isinstance(v, float) and pd.isna(v):
            return default
    except Exception:
        pass
    return v


# --------------------------------------------------------- Page + global CSS
st.set_page_config(
    page_title="PMCC Radar",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    f"""
    <style>
    .stApp {{ background: {ui.BG}; color: {ui.TEXT}; }}
    /* Hide Streamlit's top toolbar & deploy button — they overlap the title on iPhone */
    header[data-testid="stHeader"] {{ display: none !important; }}
    #MainMenu {{ display: none !important; }}
    footer {{ display: none !important; }}
    .block-container {{ padding-top: 1.5rem; padding-bottom: 2rem; max-width: 1400px; }}
    section[data-testid="stSidebar"] {{ background: {ui.SURFACE}; }}
    h1, h2, h3 {{ color: {ui.TEXT}; font-weight: 700; }}
    .stButton > button {{
        background: {ui.SURFACE}; color: {ui.TEXT};
        border: 1px solid {ui.BORDER}; border-radius: 6px;
        font-weight: 600; padding: 8px 16px;
    }}
    .stButton > button:hover {{ background: {ui.SURFACE_HOVER}; border-color: {ui.ACCENT}; }}
    .stButton > button[kind="primary"] {{
        background: {ui.ACCENT}; color: #000; border: none;
    }}
    .pmcc-title {{
        display: flex; align-items: baseline; gap: 12px; margin-bottom: 4px;
    }}
    .pmcc-title h1 {{ margin: 0; font-size: 24px; }}
    .pmcc-title .tag {{
        background: {ui.ACCENT}; color: #000; padding: 2px 8px;
        border-radius: 4px; font-size: 10px; font-weight: 700;
        letter-spacing: 0.5px; text-transform: uppercase;
    }}
    .subtitle {{ color: {ui.MUTED}; font-size: 12px; margin-bottom: 16px; }}
    .metric-card {{
        background: {ui.SURFACE}; border: 1px solid {ui.BORDER};
        border-radius: 8px; padding: 12px 14px;
    }}
    .metric-label {{ color: {ui.MUTED}; font-size: 10px; text-transform: uppercase;
                    letter-spacing: 0.5px; font-weight: 600; }}
    .metric-value {{ color: {ui.TEXT}; font-size: 18px; font-weight: 700; margin-top: 4px; }}
    @media (max-width: 640px) {{
        .block-container {{ padding: 0.75rem 0.5rem 1rem 0.5rem; }}
        .metric-value {{ font-size: 14px; }}
        .pmcc-title h1 {{ font-size: 20px; }}
        .pmcc-title .tag {{ font-size: 9px; padding: 2px 6px; }}
    }}
    </style>
    """,
    unsafe_allow_html=True,
)


# -------------------------------------------------------------- Session state
if "scan_df" not in st.session_state:
    latest = history.latest_snapshot()
    st.session_state["scan_df"] = latest
if "last_scan_ts" not in st.session_state:
    st.session_state["last_scan_ts"] = None


# ---------------------------------------------------------------------- Header
st.markdown(
    '<div class="pmcc-title"><h1>📈 PMCC Radar</h1>'
    '<span class="tag">Poor Man\'s Covered Calls</span></div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="subtitle">'
    "Ranks the best LEAP + short-call pairs across S&P 500 and liquid ETFs. "
    "Income-focused: deep ITM LEAPs (~0.85 delta) + ~30-day short calls (~0.25 delta)."
    "</div>",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------- Sidebar
with st.sidebar:
    st.markdown("### Settings")
    budget = st.number_input(
        "Max LEAP cost ($)", min_value=200, max_value=20000, value=3500, step=100,
        help="Filters out LEAPs that cost more than this."
    )
    limit = st.number_input(
        "Universe limit", min_value=20, max_value=600, value=550, step=10,
        help="Max tickers to scan. Lower = faster. Full universe ≈ 540."
    )
    max_workers = st.slider(
        "Parallel workers", 4, 24, 12,
        help="More = faster but riskier of rate-limit from yfinance."
    )
    st.markdown("---")
    st.markdown("### Universe cache")
    age = universe.cache_age_seconds()
    if age is None:
        st.caption("Not cached yet")
    else:
        mins = int(age // 60)
        st.caption(f"{mins}m old" if mins < 60 else f"{mins//60}h {mins%60}m old")
    force_refresh = st.checkbox("Force refresh universe on next scan", value=False)


# -------------------------------------------------------- Data source (on-page)
ibkr_available = ibkr.is_configured()
if ibkr_available:
    _h = ibkr.health()
    _ib_ok = bool(_h.get("ok") and _h.get("gateway_connected"))
    _badge = "🟢 live" if _ib_ok else "🔴 offline"
    ds_col1, ds_col2 = st.columns([3, 2])
    with ds_col1:
        use_ibkr = st.toggle(
            "Use IBKR data (real greeks + portfolio)",
            value=_ib_ok,
            disabled=not _ib_ok,
            help="IBKR provides real greeks from the chain. yfinance uses Black-Scholes approximation.",
            key="use_ibkr_toggle",
        )
    with ds_col2:
        st.markdown(
            f'<div style="color:{ui.MUTED};font-size:11px;padding-top:10px;text-align:right;">'
            f'IBKR proxy: {_badge}'
            f'</div>',
            unsafe_allow_html=True,
        )
else:
    use_ibkr = False
st.session_state["use_ibkr"] = use_ibkr


# ------------------------------------------------------------ Scan / load logic
run_clicked = st.button("▶ Run scan", type="primary", use_container_width=True)


if run_clicked:
    prog = st.progress(0.0, text="Starting scan…")
    refine_prog = None
    status = st.empty()
    t0 = time.time()

    def _cb(done: int, total: int):
        # IMPORTANT: signature (done:int, total:int). Never pass strings.
        pct = done / max(total, 1)
        prog.progress(pct, text=f"Scanning {done}/{total} tickers…")

    def _refine_cb(done: int, total: int):
        nonlocal_holder["refine_prog"].progress(
            done / max(total, 1),
            text=f"Refining top {total} via IBKR— {done}/{total}…",
        )

    nonlocal_holder = {"refine_prog": None}

    use_ibkr_flag = bool(st.session_state.get("use_ibkr", False))

    # Show refine bar placeholder before calling pipeline
    if use_ibkr_flag:
        refine_prog = st.progress(0.0, text="Awaiting IBKR refinement…")
        nonlocal_holder["refine_prog"] = refine_prog

    try:
        df = pipeline.run_scan(
            budget=float(budget),
            max_workers=int(max_workers),
            progress_cb=_cb,
            limit=int(limit),
            force_refresh_universe=force_refresh,
            use_ibkr=use_ibkr_flag,
            ibkr_top_n=5,
            refine_progress_cb=_refine_cb if use_ibkr_flag else None,
        )
    except Exception as e:
        st.error(f"Scan failed: {e}")
        df = pd.DataFrame()

    prog.empty()
    if refine_prog is not None:
        refine_prog.empty()
    status.empty()
    if df.empty:
        st.warning("No PMCC candidates found with current settings.")
    else:
        path = history.save_snapshot(df)
        st.session_state["scan_df"] = df
        st.session_state["last_scan_ts"] = datetime.now(timezone.utc).isoformat()
        refined_count = int((df.get("source") == "IBKR").sum()) if "source" in df.columns else 0
        refine_note = f" · {refined_count} top picks refined via IBKR" if refined_count else ""
        st.success(
            f"Scan complete: {len(df)} candidates in {time.time()-t0:.1f}s."
            f"{refine_note} Snapshot saved."
        )


# ------------------------------------------------------------------- Main table
df = st.session_state.get("scan_df", pd.DataFrame())

tab_labels = ["🏆 Leaderboard", "🔍 Detail", "📜 Legend"]
if ibkr.is_configured():
    tab_labels.insert(2, "💼 Portfolio")
tabs = st.tabs(tab_labels)

with tabs[0]:
    if df.empty:
        st.info("No scan loaded yet — click Run scan.")
    else:
        ts = None
        if "scanned_at" in df.columns and len(df):
            raw = df["scanned_at"].iloc[0]
            try:
                ts = pd.to_datetime(raw, utc=True).strftime("%Y-%m-%d %H:%M UTC")
            except Exception:
                ts = str(raw)
        st.caption(f"Showing top {min(50, len(df))} of {len(df)} candidates · scanned {ts}")

        # Summary strip
        c1, c2, c3, c4 = st.columns(4)
        top = df.iloc[0]
        with c1:
            st.markdown(
                f'<div class="metric-card"><div class="metric-label">Top Pick</div>'
                f'<div class="metric-value">{top["ticker"]}</div></div>',
                unsafe_allow_html=True,
            )
        with c2:
            st.markdown(
                f'<div class="metric-card"><div class="metric-label">Top Score</div>'
                f'<div class="metric-value">{top["score"]:.1f}</div></div>',
                unsafe_allow_html=True,
            )
        with c3:
            median_yield = df["annualized_yield"].median()
            st.markdown(
                f'<div class="metric-card"><div class="metric-label">Median Ann. Yield</div>'
                f'<div class="metric-value">{median_yield*100:.1f}%</div></div>',
                unsafe_allow_html=True,
            )
        with c4:
            high_yield_count = (df["annualized_yield"] > 0.20).sum()
            st.markdown(
                f'<div class="metric-card"><div class="metric-label">≥20% Yield</div>'
                f'<div class="metric-value">{high_yield_count}</div></div>',
                unsafe_allow_html=True,
            )

        st.markdown("<br/>", unsafe_allow_html=True)
        st.markdown(ui.render_table(df, max_rows=50), unsafe_allow_html=True)

        # CSV export
        with st.expander("⬇ Export"):
            st.download_button(
                "Download CSV",
                df.to_csv(index=False).encode("utf-8"),
                file_name=f"pmcc_scan_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )

with tabs[1]:
    if df.empty:
        st.info("Run a scan first.")
    else:
        tickers = df["ticker"].tolist()
        pick = st.selectbox("Ticker", tickers, index=0)
        row = df[df["ticker"] == pick].iloc[0]

        # Safe field reads (old snapshots may miss columns)
        spot = _safe_float(_get(row, "spot"))
        score = _safe_float(_get(row, "score"), 0.0)
        leap_expiry = _get(row, "leap_expiry", "") or ""
        leap_dte = _safe_int(_get(row, "leap_dte")) or _dte_from_expiry(leap_expiry)
        leap_strike = _safe_float(_get(row, "leap_strike"))
        leap_cost = _safe_float(_get(row, "leap_cost"))
        leap_delta = _safe_float(_get(row, "leap_delta"))
        leap_iv = _safe_float(_get(row, "leap_iv"))
        leap_oi = _safe_int(_get(row, "leap_oi"))
        short_expiry = _get(row, "short_expiry", "") or ""
        short_dte = _safe_int(_get(row, "short_dte")) or _dte_from_expiry(short_expiry)
        short_strike = _safe_float(_get(row, "short_strike"))
        short_premium = _safe_float(_get(row, "short_premium"))
        short_delta = _safe_float(_get(row, "short_delta"))
        short_iv = _safe_float(_get(row, "short_iv"))
        short_oi = _safe_int(_get(row, "short_oi"))
        net_debit = _safe_float(_get(row, "net_debit"))
        max_profit = _safe_float(_get(row, "max_profit"))
        max_loss = _safe_float(_get(row, "max_loss"))
        static_yield = _safe_float(_get(row, "static_yield"), 0.0)
        annualized = _safe_float(_get(row, "annualized_yield"), 0.0)
        breakeven = _safe_float(_get(row, "breakeven"))
        upside_cap = _safe_float(_get(row, "upside_cap_pct"), 0.0)

        row_source = _get(row, "source", "yfinance") or "yfinance"
        source_badge = (
            f'<span style="background:{ui.ACCENT};color:#000;padding:2px 6px;'
            f'border-radius:4px;font-size:9px;font-weight:700;letter-spacing:0.5px;'
            f'margin-left:8px;vertical-align:middle;">IBKR</span>'
            if row_source == "IBKR"
            else f'<span style="background:{ui.BORDER};color:{ui.MUTED};padding:2px 6px;'
                 f'border-radius:4px;font-size:9px;font-weight:700;letter-spacing:0.5px;'
                 f'margin-left:8px;vertical-align:middle;">yfinance</span>'
        )
        st.markdown(
            f'### {pick} — ${spot:.2f} {source_badge}',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div style="color:{ui.MUTED};font-size:12px;margin-bottom:12px;">'
            f'PMCC Score: <b style="color:{ui.ACCENT}">{score:.1f}</b> / 100'
            f'</div>',
            unsafe_allow_html=True,
        )

        cols = st.columns(2)
        with cols[0]:
            st.markdown("#### 🛒 Buy (LEAP)")
            st.markdown(
                f'**Expiry:** {leap_expiry} ({leap_dte} DTE)  \n'
                f'**Strike:** ${leap_strike:g}  \n'
                f'**Cost:** ${leap_cost:,.0f}  \n'
                f'**Delta:** {leap_delta:.2f}  \n'
                f'**IV:** {leap_iv*100:.1f}%  \n'
                f'**OI:** {leap_oi}'
            )
        with cols[1]:
            st.markdown("#### 💰 Sell (Short Call)")
            st.markdown(
                f'**Expiry:** {short_expiry} ({short_dte} DTE)  \n'
                f'**Strike:** ${short_strike:g}  \n'
                f'**Premium:** ${short_premium:,.0f}  \n'
                f'**Delta:** {short_delta:.2f}  \n'
                f'**IV:** {short_iv*100:.1f}%  \n'
                f'**OI:** {short_oi}'
            )

        st.markdown("#### 📊 Economics")
        e1, e2, e3, e4 = st.columns(4)
        with e1:
            st.metric("Net Debit", f"${net_debit:,.0f}")
        with e2:
            st.metric("Max Profit", f"${max_profit:,.0f}")
        with e3:
            st.metric("Static Yield", f"{static_yield*100:.1f}%")
        with e4:
            st.metric("Annualized", f"{annualized*100:.1f}%")

        f1, f2, f3 = st.columns(3)
        with f1:
            st.metric("Breakeven", f"${breakeven:.2f}")
        with f2:
            st.metric("Upside Cap", f"{upside_cap*100:.1f}%")
        with f3:
            st.metric("Max Loss", f"${max_loss:,.0f}")

        warnings_str = _get(row, "warnings", "") or ""
        earn_flag = bool(_get(row, "earnings_before_short_expiry", False))
        if warnings_str or earn_flag:
            st.markdown("#### ⚠ Flags")
            if earn_flag:
                st.warning(
                    f"**Earnings risk**: next earnings {_get(row, 'next_earnings', 'TBD')} "
                    f"falls before short expiry ({short_expiry})"
                )
            if warnings_str:
                st.warning(f"**Liquidity**: {warnings_str}")
        else:
            st.success("Clean setup — no liquidity or earnings red flags.")

        st.markdown("#### 📋 Order Ticket")
        st.code(
            f"BUY  +1  {pick}  {leap_expiry}  {leap_strike:g}C  LMT ~${leap_cost/100:.2f}\n"
            f"SELL -1  {pick}  {short_expiry}  {short_strike:g}C  LMT ~${short_premium/100:.2f}\n"
            f"Net debit: ~${net_debit:,.0f}",
            language="text",
        )

# Portfolio tab (only present when IBKR is configured)
if ibkr.is_configured():
    with tabs[2]:
        st.markdown("### 💼 IBKR Paper Portfolio")
        h = ibkr.health()
        if not h.get("ok") or not h.get("gateway_connected"):
            st.error("IBKR proxy is offline. Start the VPS gateway and refresh.")
        else:
            acct = ibkr.get_account() or {}
            positions = ibkr.get_positions() or []

            # Account summary
            if acct:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Net Liq", f"${float(acct.get('net_liquidation', 0)):,.0f}")
                c2.metric("Buying Power", f"${float(acct.get('buying_power', 0)):,.0f}")
                c3.metric("Cash", f"${float(acct.get('total_cash', 0)):,.0f}")
                c4.metric("Realized P/L", f"${float(acct.get('realized_pnl', 0)):,.0f}")

            st.markdown("---")

            if not positions:
                st.info("No open positions in the paper account yet.")
            else:
                # Split equity vs option positions
                rows = []
                for p in positions:
                    rows.append({
                        "Symbol": p.get("symbol", ""),
                        "Type": p.get("sec_type", ""),
                        "Expiry": p.get("expiry", "") or "",
                        "Strike": p.get("strike", "") or "",
                        "Right": p.get("right", "") or "",
                        "Qty": p.get("position", 0),
                        "Avg Cost": p.get("avg_cost", 0),
                        "Mkt Price": p.get("market_price", 0),
                        "Mkt Value": p.get("market_value", 0),
                        "Unrealized P/L": p.get("unrealized_pnl", 0),
                    })
                pos_df = pd.DataFrame(rows)
                st.dataframe(pos_df, use_container_width=True, hide_index=True)

            st.caption(
                f"Data via IBKR paper account (read-only API). "
                f"Proxy health: {h.get('server_time', '')}"
            )

_legend_idx = 3 if ibkr.is_configured() else 2
with tabs[_legend_idx]:
    st.markdown("### How scoring works")
    st.markdown(
        f"""
<div style="color:{ui.MUTED};font-size:12px;line-height:1.6;">
Composite 0–100 weighted as:
<ul>
<li><b style="color:{ui.TEXT}">40%</b> Annualized yield (premium ÷ net debit × 365/DTE)</li>
<li><b style="color:{ui.TEXT}">20%</b> Upside room (short strike buffer above spot)</li>
<li><b style="color:{ui.TEXT}">15%</b> Liquidity (OI + tight bid-ask on both legs)</li>
<li><b style="color:{ui.TEXT}">10%</b> IV sweet-spot (30–60% ideal)</li>
<li><b style="color:{ui.TEXT}">10%</b> Earnings safety (no earnings before short expiry)</li>
<li><b style="color:{ui.TEXT}">5%</b>  LEAP delta quality (closer to 0.85)</li>
</ul>
Hard floor: annualized yield &lt; 5% caps score at 25 regardless.
</div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("#### Table legend")
    st.markdown(ui.legend_html(), unsafe_allow_html=True)

    st.markdown("#### What is a PMCC?")
    st.markdown(
        f"""
<div style="color:{ui.MUTED};font-size:12px;line-height:1.6;">
A <b style="color:{ui.TEXT}">Poor Man's Covered Call</b> is a two-leg trade that mimics
a traditional covered call at a fraction of the capital:
<ol>
<li><b style="color:{ui.BLUE}">Buy a deep-ITM LEAP</b> (long-dated call, ~0.85 delta,
12–18 months out) — this acts as a stock proxy.</li>
<li><b style="color:{ui.WARN}">Sell a short-dated OTM call</b> (30–45 days, ~0.25 delta)
to collect premium.</li>
<li>Repeat step 2 every month until the LEAP expires.</li>
</ol>
<b style="color:{ui.TEXT}">Why it works:</b> the LEAP gives you ~85 cents of movement
per $1 of stock for ~30-40% of the cost. The short call funds the time-decay on
the LEAP and generates steady income. Best on stable, moderately bullish underlyings.
<br/><br/>
<b style="color:{ui.DANGER}">Risks:</b> the LEAP can lose value if the underlying
drops; short call can get assigned if the stock rockets past the strike; earnings
gaps can blow up either leg.
</div>
        """,
        unsafe_allow_html=True,
    )

st.markdown(
    f'<div style="text-align:center;color:{ui.MUTED};font-size:10px;margin-top:32px;">'
    f'Not financial advice · yfinance for breadth · IBKR for live greeks on top picks'
    f'</div>',
    unsafe_allow_html=True,
)
