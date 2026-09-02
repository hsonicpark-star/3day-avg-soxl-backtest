# -*- coding: utf-8 -*-
"""만능 스위치 프리셋 4종 — 모든 균등배분 조합 비교 + 순위 안정성 검증.

  조합   : 단독 4 + 2개 6 + 3개 4 + 4개 1 = 15가지 (계좌당 $30,000 균등)
  시나리오: 전체(2011~)·2017~ × 수수료 0.07%/0%,  IS(2017~2023) vs OOS(2024~2026)
  산출   : combo_all_stats.json / combo_all_equity.csv / best3_heat.html
"""
import os, sys, io, json, itertools, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, r"D:\04.backtest\02.종가평균매매")
warnings.filterwarnings('ignore')

import numpy as np, pandas as pd
from common.pricedb import load_prices
from common.analysis import monthly_perf_table
from manse_engine import run_backtest, build_mode_frame
from strategies.manse import _MANSE_PRESETS, preset_to_params

SP = os.path.dirname(os.path.abspath(__file__))
PR = {"SOXL": load_prices("SOXL"), "QQQ": load_prices("QQQ")}
EACH, END = 30000.0, "2026-08-20"
KEYS = ["이평4일", "이평1일", "중심주가", "RSI"]      # _MANSE_PRESETS 순서 고정
PRESET = dict(zip(KEYS, _MANSE_PRESETS))
BEST3 = ["이평4일", "이평1일", "중심주가"]            # 가장 안정적이었던 조합


def run(key, start, end, fee):
    p = preset_to_params(PRESET[key], "SOXL", EACH)
    p.fee = fee
    r = run_backtest(PR, p, start=start, end=end, mode_frame=build_mode_frame(p, PR))
    return r["df"]["총자산"], r["df"]["모드"]


def align(eqs):
    idx = eqs[KEYS[0]].index
    for k in KEYS[1:]:
        idx = idx.intersection(eqs[k].index)
    return {k: v.reindex(idx) for k, v in eqs.items()}


def stats(eq, cap):
    eq = eq.dropna()
    yrs = (eq.index[-1] - eq.index[0]).days / 365.25
    mdd = float((eq / eq.cummax() - 1).min())
    cagr = (float(eq.iloc[-1]) / cap) ** (1 / yrs) - 1
    dr = eq.pct_change().dropna()
    neg = dr[dr < 0]
    return {"CAGR": cagr, "MDD": mdd, "Calmar": abs(cagr / mdd) if mdd else np.nan,
            "Sharpe": float(dr.mean() / dr.std() * np.sqrt(252)) if dr.std() else np.nan,
            "Sortino": float(dr.mean() / neg.std() * np.sqrt(252))
                       if len(neg) > 1 and neg.std() else np.nan,
            "최악일": float(dr.min()), "배수": float(eq.iloc[-1]) / cap}


def all_combos(eqs):
    out = {}
    for n in (1, 2, 3, 4):
        for c in itertools.combinations(KEYS, n):
            out["+".join(c)] = (sum(eqs[k] for k in c), EACH * n)
    return out


# ── 1) 시나리오별 15개 조합 ─────────────────────────────────
SCEN = [("전체 (2011~) · 수수료 0.07%", "2011-01-03", END, 0.0007),
        ("2017~ · 수수료 0.07%",        "2017-01-03", END, 0.0007),
        ("전체 (2011~) · 수수료 0%",     "2011-01-03", END, 0.0),
        ("IS 2017~2023 · 수수료 0.07%",  "2017-01-03", "2023-12-29", 0.0007),
        ("OOS 2024~2026 · 수수료 0.07%", "2024-01-02", END, 0.0007)]

ALL, CORR, EQ_FULL = {}, None, None
for tag, s, e, fee in SCEN:
    eqs = align({k: run(k, s, e, fee)[0] for k in KEYS})
    ALL[tag] = {n: stats(eq, cap) for n, (eq, cap) in all_combos(eqs).items()}
    if tag.startswith("전체 (2011~) · 수수료 0.07"):
        CORR = pd.DataFrame({k: v.pct_change() for k, v in eqs.items()}).dropna().corr()
        EQ_FULL = eqs
    print(f"[{tag}] 15개 조합 완료")

# ── 2) 콘솔 요약 ──────────────────────────────────────────
main = "전체 (2011~) · 수수료 0.07%"
df = pd.DataFrame(ALL[main]).T.sort_values("Calmar", ascending=False)
df.insert(0, "개수", [len(i.split("+")) for i in df.index])
print(f"\n{'='*100}\n[{main}] Calmar 내림차순\n{'='*100}")
v = df.copy()
for c in ("CAGR", "MDD", "최악일"):
    v[c] = (v[c] * 100).map("{:6.1f}%".format)
for c in ("Calmar", "Sharpe", "Sortino"):
    v[c] = v[c].map("{:5.2f}".format)
v["배수"] = v["배수"].map("{:,.0f}x".format)
print(v.to_string())

print(f"\n{'='*100}\n조합 '개수'별 Calmar — 평균 / 최저 (최저 = 그 개수에서 최악의 선택)\n{'='*100}")
size = {}
for tag, r in ALL.items():
    g = pd.DataFrame(r).T
    g["개수"] = [len(i.split("+")) for i in g.index]
    size[tag] = g.groupby("개수")["Calmar"].agg(["mean", "min"])
sz = pd.concat(size, axis=1).round(2)
print(sz.to_string())

is_r = pd.Series({k: v["Calmar"] for k, v in ALL[SCEN[3][0]].items()}).rank(ascending=False)
oos_r = pd.Series({k: v["Calmar"] for k, v in ALL[SCEN[4][0]].items()}).rank(ascending=False)
rho = is_r.corr(oos_r, method="spearman")
top_is, top_oos = set(is_r.nsmallest(3).index), set(oos_r.nsmallest(3).index)
print(f"\n순위 안정성: Spearman(IS,OOS) = {rho:.3f} · 상위3 교집합 {len(top_is & top_oos)}/3")
print(f"  IS  상위3: {sorted(top_is)}")
print(f"  OOS 상위3: {sorted(top_oos)}")

print(f"\n{'='*100}\n일간수익률 상관계수 (전체 2011~ / 0.07%)\n{'='*100}")
print(CORR.round(3).to_string())

# ── 3) 산출물 저장 ────────────────────────────────────────
mode = run(BEST3[0], "2011-01-03", END, 0.0007)[1]
tot3 = sum(EQ_FULL[k] for k in BEST3)
io.open(os.path.join(SP, "best3_heat.html"), "w", encoding="utf-8").write(
    monthly_perf_table(tot3, mode.reindex(tot3.index),
                       mode_short={"바닥": "바", "중간": "중", "천장": "천"}))
json.dump({t: {k: {m: float(x) for m, x in s.items()} for k, s in r.items()}
           for t, r in ALL.items()},
          io.open(os.path.join(SP, "combo_all_stats.json"), "w", encoding="utf-8"),
          ensure_ascii=False, indent=1)
pd.DataFrame({**EQ_FULL, "합산3종": tot3,
              "합산2종": EQ_FULL["이평4일"] + EQ_FULL["중심주가"]}).to_csv(
    os.path.join(SP, "combo_all_equity.csv"), encoding="utf-8-sig")
CORR.round(4).to_csv(os.path.join(SP, "combo_all_corr.csv"), encoding="utf-8-sig")
print("\n저장: combo_all_stats.json / combo_all_equity.csv / combo_all_corr.csv / best3_heat.html")
