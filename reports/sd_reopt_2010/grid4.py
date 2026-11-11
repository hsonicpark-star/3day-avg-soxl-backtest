# -*- coding: utf-8 -*-
"""4분할·갱신 4 (균형형) 주변 정밀 탐색 — 1차 격자의 끝(매도비율 75%, k_buy 1.00) 바깥까지 확인.

실행: python reports/sd_reopt_2010/grid4.py  → grid4.csv
"""
import os, sys, itertools, pickle
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
PX = os.path.join(HERE, "_px.pkl")
SPLIT = "2018-07-01"
_px = None


def _init():
    global _px
    _px = pickle.load(open(PX, "rb"))


def _one(a):
    import stdev_engine as E
    kb, ks, sr, dv = a
    s, e = str(_px.index[0].date()), str(_px.index[-1].date())
    out = [kb, ks, sr, dv]
    for (s1, e1) in ((s, e), (s, SPLIT), (SPLIT, e)):
        r = E.run_backtest_stdev_fast(_px, s1, e1, sigma_period=2, k_buy=kb, k_sell=ks,
                                      sell_ratio=float(sr), divisions=dv, renewal=dv, initial_capital=10000.0)
        out += [round(r["cagr"] * 100, 2), round(r["mdd"] * 100, 2), r["buy_count"], r["sell_count"]]
    return out


if __name__ == "__main__":
    import multiprocessing as mp, pandas as pd
    K_BUY = [round(0.50 + 0.05 * i, 2) for i in range(21)]      # 0.50 ~ 1.50
    K_SELL = [round(0.25 + 0.05 * i, 2) for i in range(13)]     # 0.25 ~ 0.85
    SR = list(range(50, 101, 5))                                 # 50 ~ 100
    combos = list(itertools.product(K_BUY, K_SELL, SR, [4]))
    print("combos", len(combos), flush=True)
    with mp.Pool(7, initializer=_init) as pool:
        rows = pool.map(_one, combos, chunksize=20)
    cols = ["k_buy", "k_sell", "sell_ratio", "div"]
    for sfx in ("", "_a", "_b"):
        cols += ["cagr" + sfx, "mdd" + sfx, "buys" + sfx, "sells" + sfx]
    pd.DataFrame(rows, columns=cols).to_csv(os.path.join(HERE, "grid4.csv"), index=False)
    print("DONE", flush=True)
