# -*- coding: utf-8 -*-
"""표준편차매매 프리셋 재최적화 (2010-03 ~ 최신) — 1단계 격자 탐색.

전체 구간 + 앞/뒤 절반을 각각 돌려 한 구간에만 맞는 조합을 걸러낼 수 있게 한다.
실행: python reports/sd_reopt_2010/grid.py   → reports/sd_reopt_2010/grid.csv
"""
import os, sys, itertools, pickle, time
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT); sys.path.insert(0, os.path.join(ROOT, "scripts"))
PX = os.path.join(HERE, "_px.pkl")
SPLIT = "2018-07-01"
_px = None


def _init():
    global _px
    _px = pickle.load(open(PX, "rb"))


def _one(a):
    import stdev_engine as E
    sp, kb, ks, sr, dv, rn = a
    s, e = str(_px.index[0].date()), str(_px.index[-1].date())
    out = [sp, kb, ks, sr, dv, rn]
    for (s1, e1) in ((s, e), (s, SPLIT), (SPLIT, e)):
        r = E.run_backtest_stdev_fast(_px, s1, e1, sigma_period=sp, k_buy=kb, k_sell=ks,
                                      sell_ratio=float(sr), divisions=dv, renewal=rn,
                                      initial_capital=10000.0)
        out += [round(r["cagr"] * 100, 2), round(r["mdd"] * 100, 2)] if r else [None, None]
    return out


if __name__ == "__main__":
    import warnings; warnings.filterwarnings("ignore")
    import multiprocessing as mp, pandas as pd
    import update_preset_stats as U
    px = U.load_adjusted("SOXL")[["Close"]]
    pickle.dump(px, open(PX, "wb"))
    print("period", px.index[0].date(), px.index[-1].date(), flush=True)
    K_BUY = [round(0.30 + 0.05 * i, 2) for i in range(15)]      # 0.30 ~ 1.00
    K_SELL = [round(0.20 + 0.05 * i, 2) for i in range(17)]     # 0.20 ~ 1.00
    combos = []
    for sp, kb, ks, sr, dv in itertools.product([2, 3, 5], K_BUY, K_SELL,
                                                [75, 80, 85, 90, 95, 100], [3, 4, 5, 6, 7, 8]):
        for rn in (dv, 2 * dv):
            combos.append((sp, kb, ks, sr, dv, rn))
    print("combos", len(combos), flush=True)
    t0 = time.time(); rows = []
    with mp.Pool(7, initializer=_init) as pool:
        for i, r in enumerate(pool.imap_unordered(_one, combos, chunksize=50)):
            rows.append(r)
            if i % 5000 == 0:
                print(i, f"{time.time() - t0:.0f}s", flush=True)
    df = pd.DataFrame(rows, columns=["sigma", "k_buy", "k_sell", "sell_ratio", "div", "renewal",
                                     "cagr", "mdd", "cagr_a", "mdd_a", "cagr_b", "mdd_b"])
    df.to_csv(os.path.join(HERE, "grid.csv"), index=False)
    print("DONE", len(df), f"{time.time() - t0:.0f}s", flush=True)
