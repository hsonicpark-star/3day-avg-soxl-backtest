# -*- coding: utf-8 -*-
"""전략 계열을 넘나드는 조합 분석 — 만능 스위치 + 기존 4전략.

  기존 `strategies/portfolio.py` 는 종가평균/표준편차/DSS/듀얼스나이퍼 4개만 다루고
  만능 스위치가 빠져 있다. 여기서 만능 프리셋을 같은 축에 올려 함께 비교한다.

  ※ 프리셋 기본값이 아니라 **사용자가 실제 운용 중인 설정**을 쓴다
      종가평균 · 표준편차  : ~/.usd-avg/config.json  (SOXL / sd_SOXL)
      DSS                : ~/.dss/config.json       (기본계좌)
      듀얼스나이퍼         : 로케트셋 프리셋 + 모드 소스 '원본시트 자동'
      만능 스위치         : 이평선형(4일) · 중심주가형

  방식
    · 각 전략을 '독립 계좌'로 보고 자산곡선을 시작=1.0 으로 정규화한 뒤 합산한다
      (portfolio.py 와 동일한 선형 스케일 가정).
    · 만능은 정수 주식수 반올림 때문에 엄밀히는 비선형이지만 $3,000~$60,000 구간에서
      배수 차이가 0.04% 수준이라 비교 목적에는 충분하다.

  산출: cross_strategy.json
"""
import os, sys, io, json, itertools, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, r"D:\04.backtest\02.종가평균매매")
warnings.filterwarnings('ignore')

import numpy as np, pandas as pd

SP = os.path.dirname(os.path.abspath(__file__))
START, END = "2011-01-03", "2026-09-04"
HOME = os.environ["USERPROFILE"]


def _cfg(path):
    p = os.path.join(HOME, path)
    return json.load(io.open(p, encoding="utf-8")) if os.path.exists(p) else {}


UA = _cfg(r".usd-avg\config.json")
DS = _cfg(r".dss\config.json")

# ── 사용자 실제 설정 → portfolio.py 가 기대하는 preset dict ──
AV = dict(UA.get("SOXL") or {})
AVG_USER = {"a_buy": AV.get("a_buy", -0.0063), "a_sell": AV.get("a_sell", 0.0075),
            "sell_ratio": AV.get("sell_ratio", 100.0),
            "divisions": AV.get("divisions", 5), "n_days": AV.get("n_days", 2)}
SD = dict(UA.get("sd_SOXL") or {})
STDEV_USER = {"sigma_period": SD.get("sigma_period", 2), "k_buy": SD.get("k_buy", 0.65),
              "k_sell": SD.get("k_sell", 0.45), "sell_ratio": SD.get("sell_ratio", 85.0),
              "divisions": SD.get("divisions", 5), "renewal": SD.get("renewal", 5)}
DP = dict(((DS.get("accounts") or {}).get("기본계좌") or {}).get("params") or {})
DSS_USER = {k: DP[k] for k in ("sf_div", "sf_hold", "sf_buy", "sf_sell", "ag_div",
                               "ag_hold", "ag_buy", "ag_sell", "pcr", "lcr",
                               "renewal_period", "fee_rate")}

import strategies.portfolio as PF
DUAL_USER = PF.DUAL_PRESETS["🚀 공격형 (로케트셋)"]

print("사용 설정")
print("  종가평균 :", AVG_USER)
print("  표준편차 :", STDEV_USER)
print("  DSS      :", DSS_USER)
print("  듀얼     : 로케트셋 + 모드 '원본시트 자동'")

# ── 1) 기존 4전략 곡선 ────────────────────────────────────
soxl_C, qqq_C, soxl_l = PF._load_market_data()
curves = {
    "종가평균": PF._equity_avg(soxl_C, AVG_USER, START, END),
    "표준편차": PF._equity_stdev(soxl_C, STDEV_USER, START, END),
    "DSS": PF._equity_dss(soxl_C, qqq_C, DSS_USER, START, END),
    "듀얼스나이퍼": PF._equity_dual(soxl_l, DUAL_USER, "원본시트 자동", START, END),
}
for k, v in curves.items():
    print(f"  {k}: {len(v):,}일")

# ── 2) 만능 스위치 ────────────────────────────────────────
from common.pricedb import load_prices
from manse_engine import run_backtest, build_mode_frame
from strategies.manse import _MANSE_PRESETS, preset_to_params

PR = {"SOXL": load_prices("SOXL"), "QQQ": load_prices("QQQ")}
for i, lab in ((0, "만능-이평4일"), (2, "만능-중심주가")):
    p = preset_to_params(_MANSE_PRESETS[i], "SOXL", 30000.0)
    p.fee = 0.0007
    curves[lab] = run_backtest(PR, p, start=START, end=END,
                               mode_frame=build_mode_frame(p, PR))["df"]["총자산"]

allc = pd.concat(curves, axis=1).sort_index().dropna(how="any")
norm = allc / allc.iloc[0]
print(f"\n공통 구간 {norm.index[0].date()} ~ {norm.index[-1].date()} "
      f"({len(norm):,}일) · 전략 {norm.shape[1]}개")


def stats(cur):
    c = np.asarray(cur, dtype=float)
    yrs = (cur.index[-1] - cur.index[0]).days / 365.25
    cagr = (c[-1] / c[0]) ** (1 / yrs) - 1
    peak = np.maximum.accumulate(c)
    mdd = float(((c - peak) / peak).min())
    r = pd.Series(c, index=cur.index).pct_change().dropna()
    n = r[r < 0]
    return {"CAGR": float(cagr), "MDD": mdd, "MAR": abs(cagr / mdd),
            "Sharpe": float(r.mean() / r.std() * np.sqrt(252)),
            "Sortino": float(r.mean() / n.std() * np.sqrt(252)) if len(n) > 1 else np.nan,
            "최악일": float(r.min())}


IND = {c: stats(norm[c]) for c in norm.columns}
CORR = norm.pct_change().dropna().corr()

print(f"\n{'='*80}\n개별 전략 (2011~, 실제 운용 설정)\n{'='*80}")
print(f"{'전략':<14}{'CAGR':>9}{'MDD':>9}{'MAR':>7}{'Sharpe':>8}{'최악일':>9}")
for k in norm.columns:
    v = IND[k]
    print(f"{k:<14}{v['CAGR']*100:8.1f}%{v['MDD']*100:8.1f}%{v['MAR']:7.2f}"
          f"{v['Sharpe']:8.2f}{v['최악일']*100:8.1f}%")

print(f"\n{'='*80}\n일간 수익률 상관계수\n{'='*80}")
print(CORR.round(3).to_string())

rows = []
for a, b in itertools.combinations(norm.columns, 2):
    s = stats(norm[a] * 0.5 + norm[b] * 0.5)
    s.update({"조합": f"{a}+{b}", "상관": float(CORR.loc[a, b]),
              "개별최고MAR": max(IND[a]["MAR"], IND[b]["MAR"]),
              "개별최선MDD": max(IND[a]["MDD"], IND[b]["MDD"])})
    rows.append(s)
C2 = pd.DataFrame(rows).sort_values("MAR", ascending=False)

print(f"\n{'='*98}\n2개 조합 50:50 — MAR 내림차순 ({len(C2)}개)\n{'='*98}")
print(f"{'조합':<30}{'CAGR':>9}{'MDD':>8}{'MAR':>7}{'Sharpe':>8}{'최악일':>8}"
      f"{'상관':>7}{'개선':>6}")
for _, r in C2.iterrows():
    imp = "O" if (r["MAR"] > r["개별최고MAR"] and r["MDD"] > r["개별최선MDD"]) else "-"
    print(f"{r['조합'].replace('+',' + '):<30}{r['CAGR']*100:8.1f}%{r['MDD']*100:7.1f}%"
          f"{r['MAR']:7.2f}{r['Sharpe']:8.2f}{r['최악일']*100:7.1f}%"
          f"{r['상관']:7.3f}{imp:>6}")

# ── 3종 조합 상위 ─────────────────────────────────────────
r3 = []
for c in itertools.combinations(norm.columns, 3):
    s = stats(sum(norm[x] for x in c) / 3)
    s["조합"] = "+".join(c)
    s["개별최고MAR"] = max(IND[x]["MAR"] for x in c)
    r3.append(s)
C3 = pd.DataFrame(r3).sort_values("MAR", ascending=False)
print(f"\n{'='*98}\n3개 조합 균등 — 상위 8개 ({len(C3)}개 중)\n{'='*98}")
for _, r in C3.head(8).iterrows():
    print(f"{r['조합'].replace('+',' + '):<40}{r['CAGR']*100:8.1f}%{r['MDD']*100:7.1f}%"
          f"{r['MAR']:7.2f}{r['Sharpe']:8.2f}{r['최악일']*100:7.1f}%")

json.dump({"settings": {"종가평균": AVG_USER, "표준편차": STDEV_USER,
                        "DSS": DSS_USER, "듀얼": "로케트셋 + 원본시트 자동"},
           "individual": IND,
           "corr": {a: {b: float(CORR.loc[a, b]) for b in norm.columns} for a in norm.columns},
           "pairs": C2.to_dict("records"), "triples": C3.to_dict("records"),
           "period": [str(norm.index[0].date()), str(norm.index[-1].date())]},
          io.open(os.path.join(SP, "cross_strategy.json"), "w", encoding="utf-8"),
          ensure_ascii=False, indent=1, default=float)
print("\n저장: cross_strategy.json")
