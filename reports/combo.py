# -*- coding: utf-8 -*-
"""이평선형(4일) + 중심주가형 50:50 합산 성과 분석."""
import os, sys, io, json, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, r"D:\04.backtest\02.종가평균매매")
warnings.filterwarnings('ignore')

import numpy as np, pandas as pd
from common.pricedb import load_prices
from common.analysis import monthly_perf_table
from manse_engine import run_backtest, build_mode_frame
from strategies.manse import _MANSE_PRESETS, preset_to_params

PR = {"SOXL": load_prices("SOXL"), "QQQ": load_prices("QQQ")}
EACH = 30000.0            # 계좌당 자본 (실제 세팅과 동일)
TOTAL = EACH * 2
END = "2026-08-20"
MA = _MANSE_PRESETS[0]    # 이평선형 (4일)
CT = _MANSE_PRESETS[2]    # 중심주가형


def run(pre, start, fee):
    p = preset_to_params(pre, "SOXL", EACH)
    p.fee = fee
    return run_backtest(PR, p, start=start, end=END,
                        mode_frame=build_mode_frame(p, PR))


def stats(eq, cap, trades=None):
    eq = eq.dropna()
    yrs = (eq.index[-1] - eq.index[0]).days / 365.25
    fin = float(eq.iloc[-1])
    dd = eq / eq.cummax() - 1
    mdd = float(dd.min())
    cagr = (fin / cap) ** (1 / yrs) - 1
    dr = eq.pct_change().dropna()
    sharpe = dr.mean() / dr.std() * np.sqrt(252) if dr.std() else np.nan
    neg = dr[dr < 0]
    sortino = dr.mean() / neg.std() * np.sqrt(252) if len(neg) > 1 and neg.std() else np.nan
    return {"최종자산": fin, "총수익률": fin / cap - 1, "CAGR": cagr, "MDD": mdd,
            "Calmar": abs(cagr / mdd) if mdd else np.nan,
            "Sharpe": sharpe, "Sortino": sortino,
            "최악일": float(dr.min()), "기간(년)": yrs}


OUT = {}
for pname, start in [("전체 (2011~)", "2011-01-03"),
                     ("2017년~", "2017-01-03"),
                     ("2020년~", "2020-01-02")]:
    for fname, fee in [("수수료 0%", 0.0), ("수수료 0.07%", 0.0007)]:
        a, b = run(MA, start, fee), run(CT, start, fee)
        ea, eb = a["df"]["총자산"], b["df"]["총자산"]
        idx = ea.index.intersection(eb.index)
        ea, eb = ea.reindex(idx), eb.reindex(idx)
        ec = ea + eb                                    # 50:50 합산
        ra, rb = ea.pct_change().dropna(), eb.pct_change().dropna()
        corr = float(ra.corr(rb))
        sa, sb, sc = stats(ea, EACH), stats(eb, EACH), stats(ec, TOTAL)
        key = f"{pname} / {fname}"
        OUT[key] = {"이평4일": sa, "중심주가": sb, "합산50:50": sc, "상관계수": corr,
                    "MDD_동시성": float(((ea / ea.cummax() - 1 < -0.10) &
                                      (eb / eb.cummax() - 1 < -0.10)).mean())}
        if pname.startswith("전체") and fee == 0.0007:
            OUT["_heat"] = ec
            OUT["_mode"] = a["df"]["모드"].reindex(idx)
            OUT["_ea"], OUT["_eb"] = ea, eb

print(f"{'='*104}")
print(f"이평선형(4일) + 중심주가형  50:50 합산 — 계좌당 ${EACH:,.0f} / 합계 ${TOTAL:,.0f}")
print(f"{'='*104}")
hdr = f"{'구간 / 수수료':<24}{'전략':<12}{'CAGR':>9}{'MDD':>9}{'Calmar':>9}{'Sharpe':>8}{'Sortino':>9}{'최악일':>8}"
for key, v in OUT.items():
    if key.startswith("_"):
        continue
    print(f"\n{key}   (일간수익률 상관 {v['상관계수']:.3f} · "
          f"동시 -10% 낙폭 구간 {v['MDD_동시성']*100:.0f}%)")
    print(hdr)
    for nm in ("이평4일", "중심주가", "합산50:50"):
        s = v[nm]
        print(f"{'':<24}{nm:<12}{s['CAGR']*100:8.1f}%{s['MDD']*100:8.1f}%"
              f"{s['Calmar']:9.2f}{s['Sharpe']:8.2f}{s['Sortino']:9.2f}"
              f"{s['최악일']*100:7.1f}%")

# 히트맵 + 원자료 저장
SP = os.path.dirname(os.path.abspath(__file__))
html = monthly_perf_table(OUT["_heat"], OUT["_mode"],
                          mode_short={"바닥": "바", "중간": "중", "천장": "천"})
io.open(SP + r"\combo_heat.html", "w", encoding="utf-8").write(html)
pd.DataFrame({"이평4일": OUT["_ea"], "중심주가": OUT["_eb"],
              "합산": OUT["_heat"]}).to_csv(SP + r"\combo_equity.csv",
                                          encoding="utf-8-sig")
json.dump({k: v for k, v in OUT.items() if not k.startswith("_")},
          io.open(SP + r"\combo_stats.json", "w", encoding="utf-8"),
          ensure_ascii=False, indent=1, default=float)
print("\n저장: combo_heat.html / combo_equity.csv / combo_stats.json")
