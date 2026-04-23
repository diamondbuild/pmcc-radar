"""Quick smoke test for radar/tastytrade.py."""
import os
import sys
import time

# Load creds from secrets file (workspace only; prod reads from Streamlit secrets)
try:
    with open('/home/user/workspace/secrets/tastytrade.env') as f:
        for line in f:
            if '=' in line:
                k, v = line.strip().split('=', 1)
                os.environ.setdefault(k, v)
except FileNotFoundError:
    pass

from radar import tastytrade as tt


def _p(*a, **k):
    print(*a, **k)
    sys.stdout.flush()


def main():
    _p('[1/5] health')
    h = tt.health()
    _p(' ', h)
    assert h.get('ok'), 'health failed'

    _p('[2/5] account')
    a = tt.get_account()
    _p(f"  net_liq={a.get('NetLiquidation')}  cash={a.get('CashBalance')}  bp={a.get('BuyingPower')}")
    assert a is not None

    _p('[3/5] positions')
    pos = tt.get_positions() or []
    for p in pos:
        _p(f"  {p['symbol']} | {p['quantity_direction']} {p['quantity']}")
    _p(f"  total={len(pos)}")

    _p('[4/5] spot AAPL')
    s = tt.get_spot('AAPL')
    _p(f"  {s}")

    _p('[5/5] chain AAPL 2026-06-18')
    t0 = time.time()
    ch = tt.get_chain('AAPL', '20260618')
    dt = time.time() - t0
    if ch:
        n_ok = sum(1 for c in ch['calls'] if c['delta'] is not None)
        n_quoted = sum(1 for c in ch['calls'] if c['bid'] is not None)
        _p(f"  spot={ch['spot']}  calls={len(ch['calls'])}  with_greeks={n_ok}  with_quote={n_quoted}  took={dt:.1f}s")
        spot = ch['spot'] or 270
        atm = min(ch['calls'], key=lambda c: abs(c['strike']-spot))
        _p(f"  ATM: strike={atm['strike']} bid={atm['bid']} ask={atm['ask']} delta={atm['delta']} iv={atm['iv']}")
        assert n_ok > 0, 'no greeks received'
    else:
        _p('  FAILED')
        sys.stdout.flush()
        os._exit(1)

    _p('\nAll tests passed.')
    sys.stdout.flush()
    os._exit(0)


if __name__ == '__main__':
    main()
