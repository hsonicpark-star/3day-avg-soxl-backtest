# -*- coding: utf-8 -*-
"""표준편차 프리셋별 월별 승률 (월간 수익률이 플러스인 달의 비율) — 2010-03 ~ 최신."""
import os, sys, warnings; warnings.filterwarnings("ignore")
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT); sys.path.insert(0, os.path.join(ROOT, "scripts"))
import numpy as np, pandas as pd, stdev_engine as E, update_preset_stats as U
px = U.load_adjusted("SOXL")[["Close"]]
s, e = str(px.index[0].date()), str(px.index[-1].date())
db = U.extract_literal("strategies/stdev.py", "_SD_PRESETS_DB")["SOXL"]
print("기간", s, "~", e)
mon = {}
for p in db:
    r = E.run_backtest_stdev(px, s, e, sigma_period=p["sigma_period"], k_buy=p["k_buy"], k_sell=p["k_sell"],
                             sell_ratio=p["sell_ratio"], divisions=p["divisions"], renewal=p["renewal"],
                             initial_capital=10000.0, return_history=False)
    a = pd.Series(np.asarray(r["assets"], float), index=pd.to_datetime(r["dates"]))
    m = a.resample("ME").last().pct_change().dropna()
    m = m[m.index < a.index[-1].to_period("M").to_timestamp()]      # 진행 중인 이번 달 제외
    mon[p["label"]] = m
bh = px["Close"].resample("ME").last().pct_change().dropna(); bh = bh[bh.index.isin(mon[db[0]["label"]].index)]
mon["SOXL 보유"] = bh
print("\n이름 | 달 수 | 승률 | 평균 이긴 달 | 평균 진 달 | 최고 | 최악 | 최장 연패 | -10% 이하인 달")
for n, m in mon.items():
    w = m > 0
    lose = (~w).astype(int); streak = lose.groupby((lose == 0).cumsum()).sum().max()
    print(f"{n:12s} | {len(m)} | {w.mean()*100:4.1f}% | {m[w].mean()*100:+5.1f}% | {m[~w].mean()*100:+5.1f}% | "
          f"{m.max()*100:+5.1f}% ({m.idxmax():%Y-%m}) | {m.min()*100:+5.1f}% ({m.idxmin():%Y-%m}) | {streak}달 | {(m <= -0.10).sum()}")
print("\n[구간별 승률] 2010~2018 / 2019~2026 / 최근 3년(2024~)")
for n, m in mon.items():
    print(f"{n:12s} | {(m[:'2018']>0).mean()*100:4.1f}% | {(m['2019':]>0).mean()*100:4.1f}% | {(m['2024':]>0).mean()*100:4.1f}%")
print("\n[달력 월별 승률 %]  (각 16~17회)")
print("이름         " + " ".join(f"{k:>4d}월" for k in range(1, 13)))
for n, m in mon.items():
    g = (m > 0).groupby(m.index.month).mean() * 100
    print(f"{n:12s} " + " ".join(f"{g.get(k, float('nan')):5.0f} " for k in range(1, 13)))
print("\n[달력 월별 평균 수익률 %]")
for n, m in mon.items():
    g = m.groupby(m.index.month).mean() * 100
    print(f"{n:12s} " + " ".join(f"{g.get(k, float('nan')):+5.1f} " for k in range(1, 13)))
print("\n[2026년 월별 수익률 %]")
for n, m in mon.items():
    print(f"{n:12s} " + " ".join(f"{v*100:+5.1f}" for v in m['2026':].values))
