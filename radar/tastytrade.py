"""Tastytrade client — direct API access for chains, accounts, and positions.

Uses the official `tastytrade` Python SDK (v12.x) with OAuth refresh-token
flow. Credentials come from env vars / Streamlit secrets:
  TT_CLIENT_SECRET  — OAuth app client secret
  TT_REFRESH_TOKEN  — permanent refresh token from initial auth

Design notes
------------
The SDK is fully async and its underlying httpx clients are bound to whatever
event loop first creates them. Caching a Session across calls leads to
"Event loop is closed" errors in Streamlit (where each rerun uses a fresh
thread/loop). So we:

  1. For each public call, spawn a dedicated thread.
  2. Inside that thread, create a NEW event loop AND a NEW Session.
  3. Run the whole workflow in that one loop, then close.

Session creation is a single HTTP POST to refresh the access token — cheap.
Cache spot quotes at module level for a short TTL to avoid re-auth on every
ticker during a scan refinement.

Public API:
  is_configured()                -> bool
  health()                       -> dict
  get_spot(symbol)               -> dict | None
  get_expiries(symbol)           -> list[str] | None    (YYYYMMDD)
  get_chain(symbol, expiry)      -> dict | None
  get_positions()                -> list[dict] | None
  get_account()                  -> dict | None
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from datetime import datetime
from typing import Any, Callable, Optional

log = logging.getLogger("radar.tastytrade")

# --- config ---------------------------------------------------------------
CLIENT_SECRET = os.environ.get("TT_CLIENT_SECRET", "")
REFRESH_TOKEN = os.environ.get("TT_REFRESH_TOKEN", "")
ACCOUNT_NUMBER = os.environ.get("TT_ACCOUNT_NUMBER", "5WZ48203")
USER_AGENT = "pmcc-radar/1.0"

GREEKS_TIMEOUT = float(os.environ.get("TT_GREEKS_TIMEOUT", "6"))
CALL_TIMEOUT = float(os.environ.get("TT_CALL_TIMEOUT", "45"))


def is_configured() -> bool:
    return bool(CLIENT_SECRET and REFRESH_TOKEN)


def _f(x) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except (TypeError, ValueError):
        return None


def _call_in_thread(async_factory: Callable[[], Any], timeout: float = CALL_TIMEOUT):
    """Run a fresh coroutine on a fresh loop in a fresh thread.

    async_factory is a zero-arg callable that returns a coroutine. It MUST
    close over the session creation too, so that the Session's httpx clients
    are bound to the same loop that later awaits them.
    """
    result: dict = {}

    def runner():
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            coro = async_factory()
            result["v"] = loop.run_until_complete(coro)
        except BaseException as e:
            result["err"] = e
        finally:
            try:
                # Cancel any stragglers before closing so close() doesn't
                # warn about pending tasks.
                pending = asyncio.all_tasks(loop)
                for t in pending:
                    t.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            try:
                loop.close()
            except Exception:
                pass

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if t.is_alive():
        raise TimeoutError(f"Tastytrade call exceeded {timeout}s")
    if "err" in result:
        raise result["err"]
    return result.get("v")


def _make_session():
    """Create a fresh Session. MUST be called inside the target event loop
    (or before any async work — the httpx client is created lazily)."""
    from tastytrade import Session
    return Session(
        provider_secret=CLIENT_SECRET,
        refresh_token=REFRESH_TOKEN,
    )


# --- account --------------------------------------------------------------
def health() -> dict:
    if not is_configured():
        return {"ok": False, "error": "TT_CLIENT_SECRET / TT_REFRESH_TOKEN not set"}
    try:
        from tastytrade import Account

        def factory():
            async def inner():
                session = _make_session()
                a = await Account.get(session, account_number=ACCOUNT_NUMBER)
                return {
                    "ok": True,
                    "account_number": a.account_number,
                    "nickname": a.nickname,
                    "account_type": a.account_type_name,
                }
            return inner()

        return _call_in_thread(factory, timeout=20)
    except Exception as e:
        log.warning(f"health failed: {e}")
        return {"ok": False, "error": str(e)}


def get_account() -> Optional[dict]:
    if not is_configured():
        return None
    try:
        from tastytrade import Account

        def factory():
            async def inner():
                session = _make_session()
                a = await Account.get(session, account_number=ACCOUNT_NUMBER)
                bal = await a.get_balances(session)
                return {
                    "account_number": a.account_number,
                    "nickname": a.nickname,
                    "NetLiquidation": _f(bal.net_liquidating_value),
                    "CashBalance": _f(bal.cash_balance),
                    "BuyingPower": _f(bal.derivative_buying_power),
                    "EquityBuyingPower": _f(bal.equity_buying_power),
                    "AvailableFunds": _f(bal.equity_buying_power),
                    "MaintenanceMargin": _f(getattr(bal, "maintenance_requirement", None)),
                    "InitialMargin": _f(getattr(bal, "margin_equity", None)),
                    "GrossPositionValue": _f(getattr(bal, "long_equity_value", None)),
                    "UnrealizedPnL": _f(getattr(bal, "pending_cash", None)),
                    "source": "tastytrade",
                    "updated_at": datetime.utcnow().isoformat() + "Z",
                }
            return inner()

        return _call_in_thread(factory, timeout=25)
    except Exception as e:
        log.warning(f"get_account failed: {e}")
        return None


def get_positions() -> Optional[list[dict]]:
    if not is_configured():
        return None
    try:
        from tastytrade import Account

        def factory():
            async def inner():
                session = _make_session()
                a = await Account.get(session, account_number=ACCOUNT_NUMBER)
                positions = await a.get_positions(session)
                out = []
                for p in positions:
                    out.append({
                        "symbol": p.symbol,
                        "underlying_symbol": getattr(p, "underlying_symbol", None),
                        "instrument_type": str(getattr(p, "instrument_type", "")),
                        "quantity": float(p.quantity) if p.quantity is not None else 0.0,
                        "quantity_direction": str(getattr(p, "quantity_direction", "")),
                        "average_open_price": float(getattr(p, "average_open_price", 0) or 0),
                        "close_price": float(getattr(p, "close_price", 0) or 0),
                        "multiplier": int(getattr(p, "multiplier", 100) or 100),
                        "cost_effect": str(getattr(p, "cost_effect", "")),
                        "realized_day_gain": float(getattr(p, "realized_day_gain", 0) or 0),
                        "mark": float(getattr(p, "mark", 0) or 0),
                        "mark_price": float(getattr(p, "mark_price", 0) or 0),
                    })
                return out
            return inner()

        return _call_in_thread(factory, timeout=25)
    except Exception as e:
        log.warning(f"get_positions failed: {e}")
        return None


# --- market data ----------------------------------------------------------
_spot_cache: dict[str, tuple[float, dict]] = {}
_SPOT_TTL = 5.0  # seconds


def get_spot(symbol: str) -> Optional[dict]:
    """Fetch spot quote. Uses /market-data/by-type endpoint + short TTL cache."""
    if not is_configured():
        return None
    symbol = symbol.upper()
    now = time.time()
    cached = _spot_cache.get(symbol)
    if cached and (now - cached[0]) < _SPOT_TTL:
        return cached[1]

    try:
        import requests
        # Refresh first to populate session_token with real access token
        def factory():
            async def inner():
                session = _make_session()
                await session.refresh(force=True)
                return session.session_token
            return inner()
        token = _call_in_thread(factory, timeout=15)
        r = requests.get(
            "https://api.tastyworks.com/market-data/by-type",
            params={"equity": symbol},
            headers={
                "Authorization": f"Bearer {token}",
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
            },
            timeout=15,
        )
        r.raise_for_status()
        items = r.json().get("data", {}).get("items", [])
        if not items:
            return None
        q = items[0]
        bid = float(q.get("bid") or 0) or None
        ask = float(q.get("ask") or 0) or None
        last = float(q.get("last") or 0) or None
        mid = (bid + ask) / 2 if (bid and ask) else (last or bid or ask)
        result = {
            "symbol": symbol,
            "price": mid,
            "bid": bid,
            "ask": ask,
            "last": last,
            "close": float(q.get("close") or 0) or None,
            "source": "tastytrade",
        }
        _spot_cache[symbol] = (now, result)
        return result
    except Exception as e:
        log.warning(f"get_spot({symbol}) failed: {e}")
        return None


_opt_cache: dict[str, tuple[float, dict]] = {}
_OPT_TTL = 5.0  # seconds


def get_option_quotes(occ_symbols: list[str]) -> dict[str, dict]:
    """Batch-fetch live option quotes via /market-data/by-type.

    Input: list of OCC symbols as returned by Tastytrade positions
    (e.g. 'ETHA  260529C00020000' — space-padded to 6 chars at underlying).
    Returns: {occ_symbol: {bid, ask, last, mark, close}}
    Missing symbols simply won't be in the output dict.
    """
    if not is_configured() or not occ_symbols:
        return {}
    now = time.time()
    out: dict[str, dict] = {}
    need: list[str] = []
    for s in occ_symbols:
        c = _opt_cache.get(s)
        if c and (now - c[0]) < _OPT_TTL:
            out[s] = c[1]
        else:
            need.append(s)
    if not need:
        return out
    try:
        import requests

        def factory():
            async def inner():
                session = _make_session()
                await session.refresh(force=True)
                return session.session_token
            return inner()
        token = _call_in_thread(factory, timeout=15)
        # API accepts repeated equity-option params; chunk to be safe.
        CHUNK = 25
        for i in range(0, len(need), CHUNK):
            batch = need[i:i + CHUNK]
            params = [("equity-option", s) for s in batch]
            r = requests.get(
                "https://api.tastyworks.com/market-data/by-type",
                params=params,
                headers={
                    "Authorization": f"Bearer {token}",
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                },
                timeout=20,
            )
            r.raise_for_status()
            items = r.json().get("data", {}).get("items", [])
            for q in items:
                sym = q.get("symbol")
                if not sym:
                    continue
                bid = float(q.get("bid") or 0) or None
                ask = float(q.get("ask") or 0) or None
                last = float(q.get("last") or 0) or None
                close = float(q.get("close") or 0) or None
                mark = float(q.get("mark") or 0) or None
                if mark is None:
                    if bid and ask:
                        mark = (bid + ask) / 2
                    else:
                        mark = last or bid or ask or close
                entry = {
                    "bid": bid, "ask": ask, "last": last,
                    "mark": mark, "close": close,
                }
                _opt_cache[sym] = (now, entry)
                out[sym] = entry
        return out
    except Exception as e:
        log.warning(f"get_option_quotes failed: {e}")
        return out


def get_expiries(symbol: str) -> Optional[list[str]]:
    if not is_configured():
        return None
    try:
        from tastytrade.instruments import NestedOptionChain

        def factory():
            async def inner():
                session = _make_session()
                chains = await NestedOptionChain.get(session, symbol.upper())
                ch = chains[0] if isinstance(chains, list) else chains
                return [e.expiration_date.strftime("%Y%m%d") for e in ch.expirations]
            return inner()

        return _call_in_thread(factory, timeout=20)
    except Exception as e:
        log.warning(f"get_expiries({symbol}) failed: {e}")
        return None


def _parse_expiry(expiry: str):
    s = str(expiry).replace("-", "")
    return datetime.strptime(s, "%Y%m%d").date()


def get_chain(symbol: str, expiry: str) -> Optional[dict]:
    """Fetch full chain for one expiry with live greeks + quotes via DXLink.

    expiry: YYYYMMDD or YYYY-MM-DD
    Returns shape:
      {symbol, expiry, spot, calls, puts, source, greeks_hit_rate}
    Each row: {strike, bid, ask, last, iv, delta, gamma, theta, vega, oi, vol}
    """
    if not is_configured():
        return None
    try:
        from tastytrade import DXLinkStreamer
        from tastytrade.instruments import NestedOptionChain
        from tastytrade.dxfeed import Greeks, Quote

        target = _parse_expiry(expiry)
        sym = symbol.upper()

        def factory():
            async def inner():
                session = _make_session()
                chains = await NestedOptionChain.get(session, sym)
                ch = chains[0] if isinstance(chains, list) else chains
                exp = next((e for e in ch.expirations if e.expiration_date == target), None)
                if exp is None:
                    return None

                strike_by_call = {}
                strike_by_put = {}
                for s in exp.strikes:
                    if s.call_streamer_symbol:
                        strike_by_call[s.call_streamer_symbol] = s
                    if s.put_streamer_symbol:
                        strike_by_put[s.put_streamer_symbol] = s

                all_syms = list(strike_by_call.keys()) + list(strike_by_put.keys())
                greeks_by_sym = {}
                quotes_by_sym = {}

                async with DXLinkStreamer(session) as streamer:
                    await streamer.subscribe(Greeks, all_syms)
                    await streamer.subscribe(Quote, all_syms)

                    deadline = asyncio.get_event_loop().time() + GREEKS_TIMEOUT
                    target_count = len(all_syms)
                    while asyncio.get_event_loop().time() < deadline:
                        got_any = False
                        for _ in range(500):
                            g = streamer.get_event_nowait(Greeks)
                            if g is None:
                                break
                            greeks_by_sym[g.event_symbol] = g
                            got_any = True
                        for _ in range(500):
                            q = streamer.get_event_nowait(Quote)
                            if q is None:
                                break
                            quotes_by_sym[q.event_symbol] = q
                            got_any = True
                        if (len(greeks_by_sym) >= target_count * 0.9
                                and len(quotes_by_sym) >= target_count * 0.5):
                            break
                        await asyncio.sleep(0.15 if not got_any else 0.05)

                def _row(strike_obj, streamer_sym):
                    g = greeks_by_sym.get(streamer_sym)
                    q = quotes_by_sym.get(streamer_sym)
                    bid = _f(q.bid_price) if q else None
                    ask = _f(q.ask_price) if q else None
                    return {
                        "strike": float(strike_obj.strike_price),
                        "bid": bid,
                        "ask": ask,
                        "last": None,
                        "iv": _f(g.volatility) if g else None,
                        "delta": _f(g.delta) if g else None,
                        "gamma": _f(g.gamma) if g else None,
                        "theta": _f(g.theta) if g else None,
                        "vega": _f(g.vega) if g else None,
                        "oi": None,
                        "vol": None,
                    }

                calls = [_row(s, s.call_streamer_symbol)
                         for s in exp.strikes if s.call_streamer_symbol]
                puts = [_row(s, s.put_streamer_symbol)
                        for s in exp.strikes if s.put_streamer_symbol]

                return {
                    "symbol": sym,
                    "expiry": target.strftime("%Y%m%d"),
                    "spot": None,  # filled by caller via get_spot() if needed
                    "calls": calls,
                    "puts": puts,
                    "source": "tastytrade",
                    "greeks_hit_rate": len(greeks_by_sym) / max(1, len(all_syms)),
                }
            return inner()

        result = _call_in_thread(factory, timeout=GREEKS_TIMEOUT + 30)
        # Enrich with spot (separate call, uses cache)
        if result is not None and result.get("spot") is None:
            sp = get_spot(sym)
            if sp:
                result["spot"] = sp.get("price")
        return result
    except Exception as e:
        log.warning(f"get_chain({symbol}, {expiry}) failed: {e}")
        return None
