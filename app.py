"""PMCC Radar — Poor Man's Covered Call scanner.

Finds the best LEAP + short-call pairs across S&P 500 + liquid ETFs.
Ranks by annualized yield, upside room, liquidity, IV, and earnings safety.
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

from radar import history, pipeline, ui, universe


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
    .block-container {{ padding-top: 1rem; padding-bottom: 2rem; max-width: 1400px; }}
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
        .block-container {{ padding: 0.5rem; }}
        .metric-value {{ font-size: 14px; }}
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


# ------------------------------------------------------------ Scan / load logic
col_a, col_b = st.columns([1, 1])
with col_a:
    run_clicked = st.button("▶ Run scan", type="primary", use_container_width=True)
with col_b:
    refresh_clicked = st.button("↻ Reload latest", use_container_width=True)

if refresh_clicked:
    latest = history.latest_snapshot()
    if latest.empty:
        st.warning("No saved scans yet — run one.")
    else:
        st.session_state["scan_df"] = latest
        st.session_state["last_scan_ts"] = latest["scanned_at"].iloc[0] if "scanned_at" in latest.columns else None
        st.rerun()


if run_clicked:
    prog = st.progress(0.0, text="Starting scan…")
    status = st.empty()
    t0 = time.time()

    def _cb(done: int, total: int):
        # IMPORTANT: signature (done:int, total:int). Never pass strings.
        pct = done / max(total, 1)
        prog.progress(pct, text=f"Scanning {done}/{total} tickers…")

    try:
        df = pipeline.run_scan(
            budget=float(budget),
            max_workers=int(max_workers),
            progress_cb=_cb,
            limit=int(limit),
            force_refresh_universe=force_refresh,
        )
    except Exception as e:
        st.error(f"Scan failed: {e}")
        df = pd.DataFrame()

    prog.empty()
    status.empty()
    if df.empty:
        st.warning("No PMCC candidates found with current settings.")
    else:
        path = history.save_snapshot(df)
        st.session_state["scan_df"] = df
        st.session_state["last_scan_ts"] = datetime.now(timezone.utc).isoformat()
        st.success(
            f"Scan complete: {len(df)} candidates in {time.time()-t0:.1f}s. "
            f"Snapshot saved."
        )


# ------------------------------------------------------------------- Main table
df = st.session_state.get("scan_df", pd.DataFrame())

tabs = st.tabs(["🏆 Leaderboard", "🔍 Detail", "📜 Legend"])

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

        st.markdown(f"### {pick} — ${row['spot']:.2f}")
        st.markdown(
            f'<div style="color:{ui.MUTED};font-size:12px;margin-bottom:12px;">'
            f'PMCC Score: <b style="color:{ui.ACCENT}">{row["score"]:.1f}</b> / 100'
            f'</div>',
            unsafe_allow_html=True,
        )

        cols = st.columns(2)
        with cols[0]:
            st.markdown("#### 🛒 Buy (LEAP)")
            st.markdown(
                f'**Expiry:** {row["leap_expiry"]} ({int(row["leap_dte"])} DTE)  \n'
                f'**Strike:** ${row["leap_strike"]:g}  \n'
                f'**Cost:** ${row["leap_cost"]:,.0f}  \n'
                f'**Delta:** {row["leap_delta"]:.2f}  \n'
                f'**IV:** {row["leap_iv"]*100:.1f}%  \n'
                f'**OI:** {int(row["leap_oi"])}'
            )
        with cols[1]:
            st.markdown("#### 💰 Sell (Short Call)")
            st.markdown(
                f'**Expiry:** {row["short_expiry"]} ({int(row["short_dte"])} DTE)  \n'
                f'**Strike:** ${row["short_strike"]:g}  \n'
                f'**Premium:** ${row["short_premium"]:,.0f}  \n'
                f'**Delta:** {row["short_delta"]:.2f}  \n'
                f'**IV:** {row["short_iv"]*100:.1f}%  \n'
                f'**OI:** {int(row["short_oi"])}'
            )

        st.markdown("#### 📊 Economics")
        e1, e2, e3, e4 = st.columns(4)
        with e1:
            st.metric("Net Debit", f"${row['net_debit']:,.0f}")
        with e2:
            st.metric("Max Profit", f"${row['max_profit']:,.0f}")
        with e3:
            st.metric("Static Yield", f"{row['static_yield']*100:.1f}%")
        with e4:
            st.metric("Annualized", f"{row['annualized_yield']*100:.1f}%")

        f1, f2, f3 = st.columns(3)
        with f1:
            st.metric("Breakeven", f"${row['breakeven']:.2f}")
        with f2:
            st.metric("Upside Cap", f"{row['upside_cap_pct']*100:.1f}%")
        with f3:
            st.metric("Max Loss", f"${row['max_loss']:,.0f}")

        if row.get("warnings") or row.get("earnings_before_short_expiry"):
            st.markdown("#### ⚠ Flags")
            flags = []
            if row.get("earnings_before_short_expiry"):
                flags.append(f"**Earnings risk**: next earnings {row.get('next_earnings', 'TBD')} "
                            f"falls before short expiry ({row['short_expiry']})")
            if row.get("warnings"):
                flags.append(f"**Liquidity**: {row['warnings']}")
            for f in flags:
                st.warning(f)
        else:
            st.success("Clean setup — no liquidity or earnings red flags.")

        st.markdown("#### 📋 Order Ticket")
        st.code(
            f"BUY  +1  {pick}  {row['leap_expiry']}  {row['leap_strike']:g}C  LMT ~${row['leap_cost']/100:.2f}\n"
            f"SELL -1  {pick}  {row['short_expiry']}  {row['short_strike']:g}C  LMT ~${row['short_premium']/100:.2f}\n"
            f"Net debit: ~${row['net_debit']:,.0f}",
            language="text",
        )

with tabs[2]:
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
    f'Not financial advice · Data from yfinance · Delta computed from Black-Scholes'
    f'</div>',
    unsafe_allow_html=True,
)
