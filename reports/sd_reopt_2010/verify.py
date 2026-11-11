# -*- coding: utf-8 -*-
"""후보 검증 — 2년 구간별 수익/낙폭, 최근 하락장, 분할수별 요약."""
import os, sys, warnings; warnings.filterwarnings("ignore")
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
import pickle, numpy as np, pandas as pd, stdev_engine as E
px = pickle.load(open(os.path.join(HERE, "_px.pkl"), "rb"))
s, e = str(px.index[0].date()), str(px.index[-1].date())
df = pd.read_csv(os.path.join(HERE, "grid_scored.csv"))
pd.set_option("display.width", 250)
g = df.groupby("div")
print("[분할수별 — 전 조합]")
print(pd.DataFrame({"CAGR중앙": g.cagr.median(), "MDD중앙": g.mdd.median(), "Calmar중앙": g.cal.median(),
                    "Calmar상위5%": g.cal.apply(lambda x: x[x >= x.quantile(.95)].mean()),
                    "MDD상위5%평균": g.apply(lambda x: x[x.cal >= x.cal.quantile(.95)].mdd.mean()),
                    "가장얕은MDD": g.mdd.max()}).round(2))
print("\n전체 격자 Calmar 분포:", df.cal.quantile([.5, .9, .99, 1]).round(2).to_dict(), "| MDD -35% 보다 얕은 조합 중 최고 CAGR:", df[df.mdd > -35].cagr.max())
C = {
 "현재 공격형 3분할": (2, .65, .45, 85, 3, 3), "현재 Sonic형 3분할": (2, .55, .45, 95, 3, 3),
 "현재 균형형 6분할": (2, .65, .55, 90, 6, 10), "현재 안정형 5분할": (2, .65, .45, 85, 5, 5),
 "현재 Ultra 7분할": (2, .55, .30, 95, 7, 7),
 "후보A 5분할/갱신10": (2, .70, .50, 95, 5, 10), "후보B 4분할": (2, .70, .50, 90, 4, 4),
 "후보C 5분할": (2, .90, .50, 90, 5, 5), "후보D 7분할": (2, .90, .55, 85, 7, 7),
 "3분할 최선": (2, .70, .65, 85, 3, 3),
}
print("\n이름 | CAGR / MDD / Calmar | 2026-07 이후 수익·낙폭 | 2년 구간 MDD (2011~)")
for n, (sg, kb, ks, sr, dv, rn) in C.items():
    r = E.run_backtest_stdev(px, s, e, sigma_period=sg, k_buy=kb, k_sell=ks, sell_ratio=float(sr),
                             divisions=dv, renewal=rn, initial_capital=10000.0, return_history=False)
    a = pd.Series(np.asarray(r["assets"], float), index=pd.to_datetime(r["dates"]))
    dd = a / a.cummax() - 1
    rec = a["2026-07":]
    blk = []
    worst_year = min((a[str(y)].iloc[-1] / a[str(y)].iloc[0] - 1) for y in range(2011, 2027))
    for y in range(2011, 2027, 2):
        seg = a[str(y):str(y + 1)]
        blk.append(f"{((seg / seg.cummax() - 1).min()) * 100:.0f}")
    und = (dd < 0).astype(int); longest = und.groupby((und == 0).cumsum()).sum().max()
    print(f"{n:18s} | {r['cagr']*100:5.1f} / {r['mdd']*100:6.1f} / {r['cagr']/abs(r['mdd']):.2f} | "
          f"{(rec.iloc[-1]/rec.iloc[0]-1)*100:+6.1f}% {((rec/rec.cummax()-1).min())*100:6.1f}% | {' '.join(blk)} | 최악연도 {worst_year*100:+.0f}% · 최장 회복 {longest}일")
