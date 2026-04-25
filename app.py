"""PMCC Radar — Poor Man's Covered Call scanner.

Finds the best LEAP + short-call pairs across S&P 500 + liquid ETFs.
Ranks by annualized yield, upside room, liquidity, IV, and earnings safety.

Version: 3.0 (hybrid yfinance + Tastytrade refinement)
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

from radar import history, pipeline, position_monitor, tastytrade as tt, tt_orders, ui, universe

# Streamlit Cloud injects st.secrets into the environment at startup.
# Map TT_* secrets to env vars so radar.tastytrade picks them up.
try:
    for _k in ("TT_CLIENT_SECRET", "TT_REFRESH_TOKEN", "TT_ACCOUNT_NUMBER"):
        if _k in st.secrets and _k not in os.environ:
            os.environ[_k] = st.secrets[_k]
except Exception:
    pass


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


# ------------------------------------------------------------ Trade dialogs
@st.dialog("Confirm PMCC order")
def _confirm_pmcc_dialog(row_key: str):
    """Show dry-run preview and a Submit button for a PMCC opening order.
    row_key is the session_state key holding the pre-built (order, details) pair.
    """
    stash = st.session_state.get(row_key)
    if not stash:
        st.error("Trade details missing. Close and re-tap Trade.")
        return
    order, details = stash

    st.markdown(
        f"**{details['ticker']} PMCC** · qty {details['qty']} · "
        f"LIMIT **${abs(details['net_mid']):.2f} {details['price_effect']}** · DAY"
    )
    for L in details["legs"]:
        action_color = ui.BLUE if "Buy" in L["action"] else ui.WARN
        st.markdown(
            f'<div style="padding:6px 10px;background:rgba(255,255,255,0.04);'
            f'border-radius:4px;margin:4px 0;font-size:13px;">'
            f'<span style="color:{action_color};font-weight:600;">{L["action"]}</span> '
            f'<span style="color:{ui.TEXT};">{L["symbol"].strip()}</span> '
            f'<span style="color:{ui.MUTED};">@ mid ${L["mid"]:.2f}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    est_cost = details.get("est_cost") or 0
    st.markdown(
        f'<div style="color:{ui.MUTED};font-size:12px;margin-top:6px;">'
        f'Estimated net debit: <b style="color:{ui.TEXT}">${est_cost:,.2f}</b> '
        f'(per contract × qty)</div>',
        unsafe_allow_html=True,
    )

    preview_key = f"{row_key}_preview"
    if preview_key not in st.session_state:
        with st.spinner("Running dry-run check against Tastytrade…"):
            st.session_state[preview_key] = tt_orders.preview_order(order)
    prev = st.session_state[preview_key]

    if prev["warnings"]:
        for w in prev["warnings"]:
            st.warning(w)
    if prev["errors"]:
        for e in prev["errors"]:
            st.error(e)
    else:
        bp = prev.get("bp_change")
        fees = prev.get("fees_total")
        lines = []
        if bp is not None:
            lines.append(f"Buying-power effect: ${float(bp):,.2f}")
        if fees is not None:
            lines.append(f"Estimated fees: ${float(fees):,.2f}")
        if lines:
            st.markdown(
                f'<div style="color:{ui.MUTED};font-size:12px;">'
                + " · ".join(lines) + "</div>",
                unsafe_allow_html=True,
            )

    # Submit / cancel row
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Cancel", use_container_width=True, key=f"{row_key}_cancel"):
            st.session_state.pop(preview_key, None)
            st.session_state.pop(row_key, None)
            st.rerun()
    with c2:
        disabled = not prev["ok"]
        if st.button("Submit to Tastytrade", type="primary",
                     use_container_width=True, disabled=disabled,
                     key=f"{row_key}_submit"):
            with st.spinner("Submitting order…"):
                result = tt_orders.submit_order(order)
            if result["ok"]:
                st.success(
                    f"Order submitted. ID {result.get('order_id')} · "
                    f"status {result.get('status')}. Review in Tastytrade."
                )
                st.session_state.pop(preview_key, None)
                st.session_state.pop(row_key, None)
            else:
                for e in result["errors"]:
                    st.error(e)


@st.dialog("Confirm roll")
def _confirm_roll_dialog(row_key: str):
    """Show dry-run preview and a Submit button for a short-call roll."""
    stash = st.session_state.get(row_key)
    if not stash:
        st.error("Trade details missing. Close and re-tap Roll.")
        return
    order, details = stash

    effect = details["price_effect"]
    net = abs(details["net_mid"])
    st.markdown(
        f"**{details['ticker']} roll** · qty {details['qty']} · "
        f"LIMIT **${net:.2f} {effect}** · DAY"
    )
    for L in details["legs"]:
        action_color = ui.DANGER if "Close" in L["action"] else ui.WARN
        st.markdown(
            f'<div style="padding:6px 10px;background:rgba(255,255,255,0.04);'
            f'border-radius:4px;margin:4px 0;font-size:13px;">'
            f'<span style="color:{action_color};font-weight:600;">{L["action"]}</span> '
            f'<span style="color:{ui.TEXT};">{L["symbol"].strip()}</span> '
            f'<span style="color:{ui.MUTED};">@ mid ${L["mid"]:.2f}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    est = details.get("est_credit") or 0
    est_label = "Estimated net credit" if est >= 0 else "Estimated net debit"
    st.markdown(
        f'<div style="color:{ui.MUTED};font-size:12px;margin-top:6px;">'
        f'{est_label}: <b style="color:{ui.TEXT}">${abs(est):,.2f}</b></div>',
        unsafe_allow_html=True,
    )

    preview_key = f"{row_key}_preview"
    if preview_key not in st.session_state:
        with st.spinner("Running dry-run check against Tastytrade…"):
            st.session_state[preview_key] = tt_orders.preview_order(order)
    prev = st.session_state[preview_key]

    if prev["warnings"]:
        for w in prev["warnings"]:
            st.warning(w)
    if prev["errors"]:
        for e in prev["errors"]:
            st.error(e)

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Cancel", use_container_width=True, key=f"{row_key}_cancel"):
            st.session_state.pop(preview_key, None)
            st.session_state.pop(row_key, None)
            st.rerun()
    with c2:
        disabled = not prev["ok"]
        if st.button("Submit to Tastytrade", type="primary",
                     use_container_width=True, disabled=disabled,
                     key=f"{row_key}_submit"):
            with st.spinner("Submitting roll…"):
                result = tt_orders.submit_order(order)
            if result["ok"]:
                st.success(
                    f"Roll submitted. ID {result.get('order_id')} · "
                    f"status {result.get('status')}. Review in Tastytrade."
                )
                st.session_state.pop(preview_key, None)
                st.session_state.pop(row_key, None)
            else:
                for e in result["errors"]:
                    st.error(e)


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
    "Ranks the best LEAP + short-call pairs across S&P 500, Russell 1000, Nasdaq 100, "
    "and liquid ETFs. Income-focused: deep ITM LEAPs (~0.85 delta) + ~30-day short calls (~0.25 delta)."
    "</div>",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------- Sidebar
with st.sidebar:
    st.markdown("### Advanced")
    limit = st.number_input(
        "Universe limit", min_value=20, max_value=1500, value=1100, step=10,
        help="Max tickers to scan. Lower = faster. Full universe ≈ 1,050."
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
    stats = universe.cache_stats()
    if stats:
        st.caption(
            f"S&P 500: {stats.get('sp500_count', 0)} · "
            f"Nasdaq 100: {stats.get('nasdaq100_count', 0)} · "
            f"Russell 1000: {stats.get('russell1000_count', 0)} · "
            f"ETFs: {stats.get('etf_count', 0)} · "
            f"Extras: {stats.get('extras_count', 0)}"
        )
    force_refresh = st.checkbox("Force refresh universe on next scan", value=False)


# ---------------------------------------------------------- Budget (on-page)
budget = st.slider(
    "Max LEAP cost per contract",
    min_value=500, max_value=50000, value=15000, step=500,
    format="$%d",
    help="Filters out LEAPs above this cost. "
    "Raise to cover big names (SPY ~$13k, AAPL ~$12k, NVDA ~$8k). "
    "Lower for small-account scans.",
)


# -------------------------------------------------------- Data source (on-page)
tt_available = tt.is_configured()
if tt_available:
    _h = tt.health()
    _tt_ok = bool(_h.get("ok"))
    _badge = "🟢 live" if _tt_ok else "🔴 offline"
    ds_col1, ds_col2 = st.columns([3, 2])
    with ds_col1:
        use_tt = st.toggle(
            "Refine top 5 via Tastytrade (real greeks)",
            value=_tt_ok,
            disabled=not _tt_ok,
            help="Tastytrade provides live greeks + bid/ask for the top 5 picks. yfinance uses Black-Scholes approximations.",
            key="use_tt_toggle",
        )
    with ds_col2:
        st.markdown(
            f'<div style="color:{ui.MUTED};font-size:11px;padding-top:10px;text-align:right;">'
            f'Tastytrade: {_badge}'
            f'</div>',
            unsafe_allow_html=True,
        )
else:
    use_tt = False
st.session_state["use_tt"] = use_tt

# Joey's PMCC Method preset: liquid mega-cap whitelist + hard quality gates
# (price >= $40, avg vol >= 5M, above 200DMA, weeklies, no earnings in 14d)
# plus tighter LEAP delta (0.70-0.85), short > breakeven, yield floor 18% ann.
use_joey = st.toggle(
    "Joey's PMCC Method (quality filters)",
    value=st.session_state.get("use_joey", False),
    help=(
        "Restricts the scan to a curated liquid mega-cap + ETF whitelist and "
        "applies hard PMCC quality gates: $40+ price, 5M+ avg volume, above "
        "200-day MA, weekly options, no earnings within 14 days. Tightens "
        "LEAP delta to 0.70-0.85, requires short strike above LEAP breakeven, "
        "and a 1.5%/mo (~18% annualized) yield floor."
    ),
    key="use_joey_toggle",
)
st.session_state["use_joey"] = use_joey


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
            text=f"Refining top {total} via Tastytrade — {done}/{total}…",
        )

    nonlocal_holder = {"refine_prog": None}

    use_tt_flag = bool(st.session_state.get("use_tt", False))
    use_joey_flag = bool(st.session_state.get("use_joey", False))

    # Show refine bar placeholder before calling pipeline
    if use_tt_flag:
        refine_prog = st.progress(0.0, text="Awaiting Tastytrade refinement…")
        nonlocal_holder["refine_prog"] = refine_prog

    # Quality-gate progress bar (Joey's method only)
    quality_prog = None
    if use_joey_flag:
        quality_prog = st.progress(0.0, text="Quality-checking whitelist…")

    def _quality_cb(done: int, total: int):
        if quality_prog is not None:
            quality_prog.progress(
                done / max(total, 1),
                text=f"Quality-checking {done}/{total}…",
            )

    try:
        df = pipeline.run_scan(
            budget=float(budget),
            max_workers=int(max_workers),
            progress_cb=_cb,
            limit=int(limit),
            force_refresh_universe=force_refresh,
            use_tastytrade=use_tt_flag,
            refine_top_n=5,
            refine_progress_cb=_refine_cb if use_tt_flag else None,
            joey_method=use_joey_flag,
            quality_progress_cb=_quality_cb if use_joey_flag else None,
        )
    except Exception as e:
        st.error(f"Scan failed: {e}")
        df = pd.DataFrame()

    prog.empty()
    if refine_prog is not None:
        refine_prog.empty()
    if quality_prog is not None:
        quality_prog.empty()
    status.empty()
    if df.empty:
        st.warning("No PMCC candidates found with current settings.")
    else:
        path = history.save_snapshot(df)
        st.session_state["scan_df"] = df
        st.session_state["last_scan_ts"] = datetime.now(timezone.utc).isoformat()
        refined_count = int((df.get("source") == "tastytrade").sum()) if "source" in df.columns else 0
        refine_note = f" · {refined_count} top picks refined via Tastytrade" if refined_count else ""
        # Evaluate open positions for adjustments (uses Tastytrade only)
        if use_tt_flag and tt.is_configured():
            try:
                open_positions = tt.get_positions() or []
                alerts = position_monitor.evaluate_positions(open_positions)
                st.session_state["position_alerts"] = alerts
            except Exception as e:
                st.session_state["position_alerts"] = []
                st.info(f"Position check skipped: {e}")
        st.success(
            f"Scan complete: {len(df)} candidates in {time.time()-t0:.1f}s."
            f"{refine_note} Snapshot saved."
        )


# ------------------------------------------------------------------- Main table
df = st.session_state.get("scan_df", pd.DataFrame())

tab_labels = ["🏆 Leaderboard", "🔍 Detail", "📜 Legend"]
if tt.is_configured():
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

        # ----- Position Adjustments panel (from position_monitor) -----
        _alerts = st.session_state.get("position_alerts") or []
        if _alerts:
            _sev_color = {"action": ui.DANGER, "warn": ui.WARN, "info": ui.BLUE}
            _sev_label = {"action": "ACTION", "warn": "WATCH", "info": "INFO"}
            _action_ct = sum(1 for a in _alerts if a.severity == "action")
            _head = f"{len(_alerts)} position alert{'s' if len(_alerts) != 1 else ''}"
            if _action_ct:
                _head += f" · {_action_ct} need attention"
            with st.expander(f"🔔 {_head}", expanded=bool(_action_ct)):
                for ai, a in enumerate(_alerts):
                    color = _sev_color.get(a.severity, ui.MUTED)
                    badge = _sev_label.get(a.severity, a.severity.upper())
                    roll = (a.metrics or {}).get("roll")
                    close_sym = (a.metrics or {}).get("symbol")
                    close_mark = (a.metrics or {}).get("mark")
                    qty = int((a.metrics or {}).get("qty") or 1)
                    can_roll = bool(
                        roll and roll.get("mid") and close_sym and close_mark
                        and a.leg == "short_call"
                    )
                    # Render alert box + optional roll button side-by-side
                    show_button = can_roll and tt.is_configured()
                    alert_html = (
                        f'<div style="border-left:3px solid {color};padding:8px 12px;'
                        f'margin:6px 0;background:rgba(255,255,255,0.02);border-radius:4px;">'
                        f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">'
                        f'<span style="background:{color};color:#000;padding:2px 6px;'
                        f'border-radius:3px;font-size:10px;font-weight:700;">{badge}</span>'
                        f'<span style="color:{ui.TEXT};font-weight:600;font-size:13px;">{a.title}</span>'
                        f'</div>'
                        f'<div style="color:{ui.MUTED};font-size:12px;line-height:1.5;">{a.detail}</div>'
                        f'</div>'
                    )
                    if show_button:
                        ac1, ac2 = st.columns([5, 1.3])
                        with ac1:
                            st.markdown(alert_html, unsafe_allow_html=True)
                        with ac2:
                            if st.button("Roll", key=f"roll_btn_{a.underlying}_{ai}",
                                         use_container_width=True):
                                try:
                                    order, details = tt_orders.build_short_roll(
                                        under=a.underlying,
                                        close_symbol=close_sym,
                                        close_mid=float(close_mark),
                                        roll_expiry=str(roll["expiry"]),
                                        roll_strike=float(roll["strike"]),
                                        roll_mid=float(roll["mid"]),
                                        qty=qty,
                                    )
                                    stash_key = f"roll_order_{a.underlying}_{ai}"
                                    st.session_state[stash_key] = (order, details)
                                    st.session_state.pop(f"{stash_key}_preview", None)
                                    _confirm_roll_dialog(stash_key)
                                except Exception as e:
                                    st.error(f"Could not build roll: {e}")
                    else:
                        st.markdown(alert_html, unsafe_allow_html=True)

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

        # ----- Top-5 trade cards (only when Tastytrade is configured) -----
        if tt.is_configured():
            st.markdown(
                f'<div style="color:{ui.MUTED};font-size:11px;'
                f'text-transform:uppercase;letter-spacing:0.5px;font-weight:600;'
                f'margin-bottom:6px;">Top 5 · tap to send to Tastytrade</div>',
                unsafe_allow_html=True,
            )
            top5 = df.head(5)
            for idx, row in top5.iterrows():
                t = row["ticker"]
                score_v = _safe_float(row.get("score"), 0.0)
                ann = _safe_float(row.get("annualized_yield"), 0.0) * 100
                leap_k = _safe_float(row.get("leap_strike"), 0.0)
                short_k = _safe_float(row.get("short_strike"), 0.0)
                leap_exp = str(row.get("leap_expiry", "") or "")
                short_exp = str(row.get("short_expiry", "") or "")
                leap_cost = _safe_float(row.get("leap_cost"), 0.0)
                short_prem = _safe_float(row.get("short_premium"), 0.0)
                leap_mid = round(leap_cost / 100, 2) if leap_cost else 0
                short_mid = round(short_prem / 100, 2) if short_prem else 0
                net_deb = round(leap_mid - short_mid, 2)
                src_badge = ""
                if (row.get("source") or "") == "tastytrade":
                    src_badge = (
                        f'<span style="background:{ui.GOOD};color:#000;'
                        f'padding:1px 6px;border-radius:3px;font-size:9px;'
                        f'font-weight:700;margin-left:6px;">LIVE</span>'
                    )

                # Two columns: card info on left, trade button on right
                cc1, cc2 = st.columns([5, 1.3])
                with cc1:
                    st.markdown(
                        f'<div style="padding:10px 14px;margin:4px 0;'
                        f'background:rgba(255,255,255,0.04);border-left:3px solid {ui.BLUE};'
                        f'border-radius:4px;">'
                        f'<div style="display:flex;justify-content:space-between;'
                        f'align-items:center;gap:8px;flex-wrap:wrap;">'
                        f'<div>'
                        f'<span style="color:{ui.TEXT};font-weight:700;font-size:15px;">{t}</span>'
                        f'{src_badge}'
                        f'<span style="color:{ui.MUTED};font-size:11px;margin-left:8px;">'
                        f'score {score_v:.1f} · ann {ann:.1f}%</span>'
                        f'</div>'
                        f'<div style="color:{ui.TEXT};font-size:12px;">'
                        f'LEAP ${leap_k:.0f}C {leap_exp} @ ${leap_mid:.2f} · '
                        f'Short ${short_k:.0f}C {short_exp} @ ${short_mid:.2f}'
                        f'</div></div>'
                        f'<div style="color:{ui.MUTED};font-size:11px;margin-top:2px;">'
                        f'Net debit ~${net_deb*100:.0f} per contract</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                with cc2:
                    btn_disabled = (net_deb <= 0) or (not leap_mid) or (not short_mid)
                    if st.button(
                        "Trade", key=f"trade_btn_{t}_{idx}",
                        disabled=btn_disabled, use_container_width=True,
                    ):
                        try:
                            order, details = tt_orders.build_pmcc_open(
                                ticker=t,
                                leap_expiry=leap_exp, leap_strike=leap_k,
                                leap_mid=leap_mid,
                                short_expiry=short_exp, short_strike=short_k,
                                short_mid=short_mid,
                                qty=1,
                            )
                            stash_key = f"pmcc_order_{t}_{idx}"
                            st.session_state[stash_key] = (order, details)
                            # Clear any stale preview
                            st.session_state.pop(f"{stash_key}_preview", None)
                            _confirm_pmcc_dialog(stash_key)
                        except Exception as e:
                            st.error(f"Could not build order: {e}")

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
            f'margin-left:8px;vertical-align:middle;">LIVE</span>'
            if row_source == "tastytrade"
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
                f'<div style="line-height:1.8;font-size:14px;">'
                f'<b>Expiry:</b> {leap_expiry} ({leap_dte} DTE)<br/>'
                f'<b>Strike:</b> ${leap_strike:g}<br/>'
                f'<b>Cost:</b> ${leap_cost:,.0f}<br/>'
                f'<b>Delta:</b> {leap_delta:.2f}<br/>'
                f'<b>IV:</b> {leap_iv*100:.1f}%<br/>'
                f'<b>OI:</b> {leap_oi}'
                f'</div>',
                unsafe_allow_html=True,
            )
        with cols[1]:
            st.markdown("#### 💰 Sell (Short Call)")
            st.markdown(
                f'<div style="line-height:1.8;font-size:14px;">'
                f'<b>Expiry:</b> {short_expiry} ({short_dte} DTE)<br/>'
                f'<b>Strike:</b> ${short_strike:g}<br/>'
                f'<b>Premium:</b> ${short_premium:,.0f}<br/>'
                f'<b>Delta:</b> {short_delta:.2f}<br/>'
                f'<b>IV:</b> {short_iv*100:.1f}%<br/>'
                f'<b>OI:</b> {short_oi}'
                f'</div>',
                unsafe_allow_html=True,
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

# Portfolio tab (only present when Tastytrade is configured)
if tt.is_configured():
    with tabs[2]:
        st.markdown("### 💼 Tastytrade Portfolio")
        h = tt.health()
        if not h.get("ok"):
            st.error(f"Tastytrade unavailable: {h.get('error', 'unknown error')}")
        else:
            acct = tt.get_account() or {}
            positions = tt.get_positions() or []

            # Account summary
            if acct:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Net Liq", f"${float(acct.get('NetLiquidation') or 0):,.2f}")
                c2.metric("Buying Power", f"${float(acct.get('BuyingPower') or 0):,.2f}")
                c3.metric("Cash", f"${float(acct.get('CashBalance') or 0):,.2f}")
                c4.metric("Equity BP", f"${float(acct.get('EquityBuyingPower') or 0):,.2f}")

            st.markdown("---")

            if not positions:
                st.info("No open positions in this account yet.")
            else:
                # Parse OCC-style option symbol: 'GME   260529C00028000'
                def _parse_occ(sym: str):
                    s = sym.strip()
                    if len(s) >= 21 and (s[-9] in ("C", "P")):
                        try:
                            under = s[:-15].strip()
                            yymmdd = s[-15:-9]
                            right = s[-9]
                            strike = int(s[-8:]) / 1000.0
                            expiry = f"20{yymmdd[0:2]}-{yymmdd[2:4]}-{yymmdd[4:6]}"
                            return under, expiry, right, strike
                        except Exception:
                            pass
                    return sym, "", "", ""

                # Fetch live option marks so unrealized P/L reflects current prices
                # (the positions API often returns stale mark == avg until refreshed).
                _opt_syms = [
                    p.get("symbol", "") for p in positions
                    if "Option" in (p.get("instrument_type", "") or "")
                ]
                try:
                    _live_quotes = tt.get_option_quotes(_opt_syms) if _opt_syms else {}
                except Exception:
                    _live_quotes = {}

                # Build per-position row dicts and group by underlying
                groups: dict[str, list[dict]] = {}
                for p in positions:
                    is_opt = "Option" in (p.get("instrument_type", "") or "")
                    under, expiry, right, strike = (
                        _parse_occ(p.get("symbol", "")) if is_opt
                        else (p.get("symbol", ""), "", "", "")
                    )
                    qty_signed = p.get("quantity", 0)
                    direction = p.get("quantity_direction")
                    if direction == "Short":
                        qty_signed = -abs(qty_signed)
                    mult = p.get("multiplier", 100) if is_opt else 1
                    # Prefer live mark from /market-data/by-type, then SDK fields
                    _lq = _live_quotes.get(p.get("symbol", "")) if is_opt else None
                    mkt_price = (
                        (_lq or {}).get("mark")
                        or p.get("mark_price")
                        or p.get("close_price")
                        or 0
                    )
                    mkt_value = mkt_price * qty_signed * mult
                    avg_cost = p.get("average_open_price", 0)
                    if direction == "Short":
                        unreal = (avg_cost - mkt_price) * abs(qty_signed) * mult
                    else:
                        unreal = (mkt_price - avg_cost) * abs(qty_signed) * mult
                    row = {
                        "is_opt": is_opt,
                        "direction": direction or "",
                        "right": right,
                        "expiry": expiry,
                        "strike": strike if strike else None,
                        "qty": qty_signed,
                        "avg_cost": avg_cost or 0,
                        "mkt_price": mkt_price or 0,
                        "mkt_value": mkt_value or 0,
                        "unreal": unreal or 0,
                        "mult": mult,
                    }
                    groups.setdefault(under or p.get("symbol", ""), []).append(row)

                # Portfolio totals
                total_mv = sum(r["mkt_value"] for g in groups.values() for r in g)
                total_unreal = sum(r["unreal"] for g in groups.values() for r in g)
                mc1, mc2, mc3 = st.columns(3)
                mc1.metric("Positions", f"{len(positions)}")
                mc2.metric("Net Market Value", f"${total_mv:,.2f}")
                pl_color = ui.GOOD if total_unreal >= 0 else ui.DANGER
                mc3.markdown(
                    f'<div style="padding:4px 0;">'
                    f'<div style="color:{ui.MUTED};font-size:12px;">Unrealized P/L</div>'
                    f'<div style="color:{pl_color};font-size:22px;font-weight:700;">'
                    f'${total_unreal:,.2f}</div></div>',
                    unsafe_allow_html=True,
                )
                st.markdown("")

                # One card per underlying with alternating row shading inside.
                # Sort groups so largest net MV first.
                sorted_groups = sorted(
                    groups.items(),
                    key=lambda kv: -sum(abs(r["mkt_value"]) for r in kv[1]),
                )
                shade_a = "rgba(255,255,255,0.03)"
                shade_b = "rgba(255,255,255,0.07)"
                for under, rows in sorted_groups:
                    # PMCC detection: one long call + one short call on same underlying
                    longs = [r for r in rows if r["is_opt"] and r["right"] == "C" and r["direction"] == "Long"]
                    shorts = [r for r in rows if r["is_opt"] and r["right"] == "C" and r["direction"] == "Short"]
                    is_pmcc = len(longs) >= 1 and len(shorts) >= 1
                    group_tag = "PMCC" if is_pmcc else "POS"
                    tag_color = ui.BLUE if is_pmcc else ui.MUTED
                    group_mv = sum(r["mkt_value"] for r in rows)
                    group_pl = sum(r["unreal"] for r in rows)
                    pl_c = ui.GOOD if group_pl >= 0 else ui.DANGER

                    # Sort rows inside group: longs first, then shorts, then stock; by expiry
                    def _rkey(r):
                        # 0 long call, 1 short call, 2 stock/other
                        if r["is_opt"] and r["right"] == "C" and r["direction"] == "Long":
                            o = 0
                        elif r["is_opt"] and r["right"] == "C" and r["direction"] == "Short":
                            o = 1
                        else:
                            o = 2
                        return (o, r["expiry"] or "")
                    rows_sorted = sorted(rows, key=_rkey)

                    # Group header card
                    st.markdown(
                        f'<div style="margin-top:14px;padding:10px 14px;'
                        f'background:rgba(96,165,250,0.08);border-left:3px solid {tag_color};'
                        f'border-radius:6px 6px 0 0;display:flex;align-items:center;'
                        f'justify-content:space-between;flex-wrap:wrap;gap:8px;">'
                        f'<div style="display:flex;align-items:center;gap:10px;">'
                        f'<span style="color:{ui.TEXT};font-weight:700;font-size:16px;">{under}</span>'
                        f'<span style="background:{tag_color};color:#000;padding:2px 7px;'
                        f'border-radius:3px;font-size:10px;font-weight:700;letter-spacing:0.5px;">'
                        f'{group_tag}</span>'
                        f'<span style="color:{ui.MUTED};font-size:11px;">'
                        f'{len(rows)} leg{"s" if len(rows) != 1 else ""}</span>'
                        f'</div>'
                        f'<div style="display:flex;gap:16px;align-items:center;">'
                        f'<span style="color:{ui.MUTED};font-size:11px;">MV</span>'
                        f'<span style="color:{ui.TEXT};font-size:13px;font-weight:600;">'
                        f'${group_mv:,.0f}</span>'
                        f'<span style="color:{ui.MUTED};font-size:11px;">P/L</span>'
                        f'<span style="color:{pl_c};font-size:13px;font-weight:700;">'
                        f'${group_pl:,.0f}</span>'
                        f'</div></div>',
                        unsafe_allow_html=True,
                    )

                    # Column header row
                    st.markdown(
                        f'<div style="display:grid;grid-template-columns:'
                        f'0.9fr 1.1fr 0.8fr 0.5fr 0.9fr 0.9fr 1fr 1fr;gap:6px;'
                        f'padding:6px 12px;background:rgba(255,255,255,0.04);'
                        f'color:{ui.MUTED};font-size:10px;text-transform:uppercase;'
                        f'letter-spacing:0.5px;font-weight:600;">'
                        f'<div>Leg</div><div>Expiry</div><div>Strike</div>'
                        f'<div>Qty</div><div>Avg</div><div>Mark</div>'
                        f'<div>Mkt Value</div><div>Unreal P/L</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

                    # Data rows with alternating shading
                    for i, r in enumerate(rows_sorted):
                        bg = shade_b if i % 2 else shade_a
                        if r["is_opt"]:
                            if r["direction"] == "Long":
                                leg_label = "Long Call"
                                leg_color = ui.BLUE
                            elif r["direction"] == "Short":
                                leg_label = "Short Call"
                                leg_color = ui.WARN
                            else:
                                leg_label = r["right"] or "OPT"
                                leg_color = ui.TEXT
                        else:
                            leg_label = "Stock"
                            leg_color = ui.TEXT
                        strike_s = f"${r['strike']:.2f}" if r["strike"] else "—"
                        avg_s = f"${r['avg_cost']:.2f}" if r["avg_cost"] else "—"
                        mark_s = f"${r['mkt_price']:.2f}" if r["mkt_price"] else "—"
                        pl_rc = ui.GOOD if r["unreal"] >= 0 else ui.DANGER
                        is_last = i == len(rows_sorted) - 1
                        radius = "0 0 6px 6px" if is_last else "0"
                        st.markdown(
                            f'<div style="display:grid;grid-template-columns:'
                            f'0.9fr 1.1fr 0.8fr 0.5fr 0.9fr 0.9fr 1fr 1fr;gap:6px;'
                            f'padding:8px 12px;background:{bg};color:{ui.TEXT};'
                            f'font-size:12px;border-radius:{radius};align-items:center;">'
                            f'<div style="color:{leg_color};font-weight:600;">{leg_label}</div>'
                            f'<div style="color:{ui.MUTED};">{r["expiry"] or "—"}</div>'
                            f'<div>{strike_s}</div>'
                            f'<div>{r["qty"]}</div>'
                            f'<div>{avg_s}</div>'
                            f'<div>{mark_s}</div>'
                            f'<div>${r["mkt_value"]:,.0f}</div>'
                            f'<div style="color:{pl_rc};font-weight:600;">'
                            f'${r["unreal"]:,.0f}</div>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )

                # Show any alerts specific to these positions
                _portfolio_alerts = st.session_state.get("position_alerts") or []
                if _portfolio_alerts:
                    st.markdown("&nbsp;", unsafe_allow_html=True)
                    st.markdown(
                        f'<div style="color:{ui.MUTED};font-size:11px;'
                        f'text-transform:uppercase;letter-spacing:0.5px;'
                        f'font-weight:600;margin-top:8px;">Adjustment alerts</div>',
                        unsafe_allow_html=True,
                    )
                    _sev_color = {"action": ui.DANGER, "warn": ui.WARN, "info": ui.BLUE}
                    for a in _portfolio_alerts:
                        color = _sev_color.get(a.severity, ui.MUTED)
                        st.markdown(
                            f'<div style="border-left:3px solid {color};padding:6px 10px;'
                            f'margin:4px 0;background:rgba(255,255,255,0.02);border-radius:3px;'
                            f'font-size:12px;">'
                            f'<span style="color:{ui.TEXT};font-weight:600;">{a.title}</span>'
                            f'<div style="color:{ui.MUTED};font-size:11px;margin-top:2px;">'
                            f'{a.detail}</div>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )
                else:
                    st.markdown(
                        f'<div style="color:{ui.MUTED};font-size:11px;margin-top:12px;">'
                        f'Run a scan to check for adjustment recommendations.</div>',
                        unsafe_allow_html=True,
                    )

            st.caption(
                f"Data via Tastytrade API (OAuth read-only). "
                f"Account: {h.get('account_number')} · {h.get('nickname')}"
            )

_legend_idx = 3 if tt.is_configured() else 2
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
    f'Not financial advice · yfinance for breadth · Tastytrade for live greeks on top picks'
    f'</div>',
    unsafe_allow_html=True,
)
