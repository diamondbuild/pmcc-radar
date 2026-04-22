"""Custom HTML table for PMCC scans — mobile-first, trader-terminal aesthetic.

Same design tokens as Squeeze Radar so they feel like a product family.
"""
from __future__ import annotations

import html
import math
from typing import Callable

import pandas as pd

# Design tokens
BG = "#0B0F1A"
SURFACE = "#141A29"
SURFACE_HOVER = "#1A2237"
BORDER = "#1F2937"
TEXT = "#E5E7EB"
MUTED = "#7A8699"
ACCENT = "#A3E635"
WARN = "#F59E0B"
DANGER = "#EF4444"
GOOD = "#34D399"
BLUE = "#60A5FA"


def _is_num(x) -> bool:
    return isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x))


def _safe_str(x) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and math.isnan(x):
        return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s


def _esc(x) -> str:
    s = _safe_str(x)
    return html.escape(s) if s else ""


def cell_score(v) -> str:
    if not _is_num(v):
        return f'<span style="color:{MUTED}">—</span>'
    pct = max(0, min(100, v))
    if v >= 70:
        color = ACCENT
    elif v >= 50:
        color = "#EAB308"
    elif v >= 30:
        color = WARN
    else:
        color = MUTED
    return (
        f'<div style="display:flex;align-items:center;gap:6px;">'
        f'<span style="color:{color};font-weight:700;font-size:13px;min-width:32px;">{v:.0f}</span>'
        f'<div style="flex:1;height:4px;background:{BORDER};border-radius:2px;overflow:hidden;max-width:48px;">'
        f'<div style="width:{pct}%;height:100%;background:{color};"></div></div></div>'
    )


def cell_ticker(t) -> str:
    s = _esc(t)
    return f'<span style="font-weight:700;color:{TEXT};font-size:13px;">{s}</span>'


def cell_dollar(v, decimals: int = 2) -> str:
    if not _is_num(v):
        return f'<span style="color:{MUTED}">—</span>'
    return f'<span style="color:{TEXT};font-size:12px;">${v:,.{decimals}f}</span>'


def cell_int_dollar(v) -> str:
    if not _is_num(v):
        return f'<span style="color:{MUTED}">—</span>'
    return f'<span style="color:{TEXT};font-size:12px;">${v:,.0f}</span>'


def cell_pct(v, highlight: bool = False) -> str:
    if not _is_num(v):
        return f'<span style="color:{MUTED}">—</span>'
    color = TEXT
    if highlight:
        if v >= 0.30:
            color = ACCENT
        elif v >= 0.15:
            color = GOOD
        elif v >= 0.05:
            color = WARN
        else:
            color = MUTED
    return f'<span style="color:{color};font-size:12px;font-weight:600;">{v*100:.1f}%</span>'


def cell_delta(v) -> str:
    if not _is_num(v):
        return f'<span style="color:{MUTED}">—</span>'
    return f'<span style="color:{MUTED};font-size:11px;">Δ{v:.2f}</span>'


def cell_strike_leg(strike, expiry, cost_or_prem, delta, is_leap: bool) -> str:
    if not _is_num(strike):
        return f'<span style="color:{MUTED}">—</span>'
    label = "LEAP" if is_leap else "SHORT"
    label_color = BLUE if is_leap else WARN
    exp_short = _esc(expiry)
    # shorten expiry: 2027-07-16 → Jul 16 '27  (day matters for option pricing)
    try:
        parts = exp_short.split("-")
        if len(parts) == 3:
            months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            mm = int(parts[1])
            dd = int(parts[2])
            exp_short = f"{months[mm-1]} {dd} '{parts[0][-2:]}"
    except Exception:
        pass
    cost_str = f"${cost_or_prem:,.0f}" if _is_num(cost_or_prem) else "—"
    delta_str = f"Δ{delta:.2f}" if _is_num(delta) else ""
    return (
        f'<div style="font-size:11px;line-height:1.3;">'
        f'<span style="color:{label_color};font-weight:700;">{label}</span> '
        f'<span style="color:{TEXT};">${strike:g}</span> '
        f'<span style="color:{MUTED};">{exp_short}</span><br/>'
        f'<span style="color:{MUTED};">{cost_str} · {delta_str}</span>'
        f'</div>'
    )


def cell_warnings(w, earnings_flag: bool) -> str:
    chips = []
    w = _safe_str(w)
    if earnings_flag:
        chips.append(f'<span style="background:{DANGER};color:#000;padding:1px 5px;border-radius:3px;font-size:10px;font-weight:700;">E</span>')
    if w:
        if "Thin" in w:
            chips.append(f'<span style="background:{WARN};color:#000;padding:1px 5px;border-radius:3px;font-size:10px;font-weight:700;">T</span>')
        if "Wide" in w:
            chips.append(f'<span style="background:{WARN};color:#000;padding:1px 5px;border-radius:3px;font-size:10px;font-weight:700;">W</span>')
    if not chips:
        return f'<span style="color:{GOOD};font-size:11px;">✓</span>'
    return " ".join(chips)


def render_table(df: pd.DataFrame, max_rows: int = 50) -> str:
    """Render the top PMCC candidates as a styled HTML table."""
    if df.empty:
        return f'<div style="color:{MUTED};padding:24px;text-align:center;">No candidates found</div>'

    df = df.head(max_rows)
    rows_html = []
    for _, r in df.iterrows():
        cells = [
            f'<td style="padding:10px 8px;border-bottom:1px solid {BORDER};">{cell_ticker(r.get("ticker"))}</td>',
            f'<td style="padding:10px 8px;border-bottom:1px solid {BORDER};">{cell_score(r.get("score"))}</td>',
            f'<td style="padding:10px 8px;border-bottom:1px solid {BORDER};">{cell_dollar(r.get("spot"))}</td>',
            f'<td style="padding:10px 8px;border-bottom:1px solid {BORDER};">'
            f'{cell_strike_leg(r.get("leap_strike"), r.get("leap_expiry"), r.get("leap_cost"), r.get("leap_delta"), True)}</td>',
            f'<td style="padding:10px 8px;border-bottom:1px solid {BORDER};">'
            f'{cell_strike_leg(r.get("short_strike"), r.get("short_expiry"), r.get("short_premium"), r.get("short_delta"), False)}</td>',
            f'<td style="padding:10px 8px;border-bottom:1px solid {BORDER};text-align:right;">{cell_int_dollar(r.get("net_debit"))}</td>',
            f'<td style="padding:10px 8px;border-bottom:1px solid {BORDER};text-align:right;">{cell_pct(r.get("annualized_yield"), highlight=True)}</td>',
            f'<td style="padding:10px 8px;border-bottom:1px solid {BORDER};text-align:right;">{cell_pct(r.get("upside_cap_pct"))}</td>',
            f'<td style="padding:10px 8px;border-bottom:1px solid {BORDER};text-align:center;">'
            f'{cell_warnings(r.get("warnings"), r.get("earnings_before_short_expiry", False))}</td>',
        ]
        rows_html.append(f'<tr>{"".join(cells)}</tr>')

    header_style = (
        f"padding:8px;text-align:left;color:{MUTED};font-size:10px;"
        f"font-weight:600;text-transform:uppercase;letter-spacing:0.5px;"
        f"border-bottom:1px solid {BORDER};background:{SURFACE};"
    )
    header_right = header_style + "text-align:right;"
    header_center = header_style + "text-align:center;"

    return (
        f'<div style="overflow-x:auto;background:{SURFACE};border-radius:8px;'
        f'border:1px solid {BORDER};">'
        f'<table style="width:100%;border-collapse:collapse;font-family:'
        f'-apple-system,BlinkMacSystemFont,sans-serif;color:{TEXT};">'
        f'<thead><tr>'
        f'<th style="{header_style}">Ticker</th>'
        f'<th style="{header_style}">Score</th>'
        f'<th style="{header_style}">Spot</th>'
        f'<th style="{header_style}">LEAP Leg</th>'
        f'<th style="{header_style}">Short Call</th>'
        f'<th style="{header_right}">Net Debit</th>'
        f'<th style="{header_right}">Ann. Yield</th>'
        f'<th style="{header_right}">Upside</th>'
        f'<th style="{header_center}">Flags</th>'
        f'</tr></thead>'
        f'<tbody>{"".join(rows_html)}</tbody></table></div>'
    )


def legend_html() -> str:
    items = [
        ("Score", f"Composite 0–100 — higher is better", ACCENT),
        ("LEAP", "Long deep-ITM call (12–18mo, ~0.85 delta)", BLUE),
        ("SHORT", "Short call (~35 DTE, ~0.25 delta)", WARN),
        ("Ann. Yield", "Annualized static yield (premium ÷ net debit)", GOOD),
        ("Upside", "Room between spot and short strike", TEXT),
        ("E", "Earnings falls before short expiry — caution", DANGER),
        ("T", "Thin open interest on a leg", WARN),
        ("W", "Wide bid-ask spread on a leg", WARN),
        ("✓", "No warnings", GOOD),
    ]
    rows = []
    for label, desc, color in items:
        rows.append(
            f'<div style="display:flex;align-items:flex-start;gap:10px;margin-bottom:6px;">'
            f'<span style="color:{color};font-weight:700;font-size:11px;min-width:72px;">{label}</span>'
            f'<span style="color:{MUTED};font-size:11px;">{desc}</span></div>'
        )
    return (
        f'<div style="background:{SURFACE};border:1px solid {BORDER};'
        f'border-radius:8px;padding:12px;">'
        + "".join(rows)
        + "</div>"
    )
