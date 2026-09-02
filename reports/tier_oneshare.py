# -*- coding: utf-8 -*-
"""1티어를 '1% 금액매수' 대신 '1주 매수'로 바꾸면?

  A 현재      : 1티어 seed_w 1% · 수량 = 사다리 계산 (보통 2~3주)
  B 1주고정   : 1티어 seed_w 1% · 수량 = 1주   (게이트는 A와 동일)
  C 1주+게이트: 1티어 seed_w 50% · 수량 = 1주  (주문가 상한 벽 제거)

  seed_w 는 수량을 고정한 뒤에도 '주문 자격'을 좌우한다.
    · odp(주문가) > seed 이면 주문 자체가 나가지 않는다  → 1% 는 $300 벽
    · seed >= 종가 여야 체결된다
  C 는 이 벽을 없애는 변형이다.

  엔진은 고정 수량 모드가 없으므로 calc_qty 를 분석 시점에만 가로챈다
  (manse_engine.py 는 수정하지 않음 — buy_amt = buy_px * qty 라 회계는 그대로 따라간다).

  산출: oneshare_stats.json / oneshare_qty.csv / oneshare_equity.csv
"""
import os, sys, io, json, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, r"D:\04.backtest\02.종가평균매매")
warnings.filterwarnings('ignore')

import numpy as np, pandas as pd
from common.pricedb import load_prices
import manse_engine as ME
from manse_engine import run_backtest, build_mode_frame
from strategies.manse import _MANSE_PRESETS, preset_to_params

SP = os.path.dirname(os.path.abspath(__file__))
J = lambda *a: os.path.join(SP, *a)
PR = {"SOXL": load_prices("SOXL"), "QQQ": load_prices("QQQ")}
EACH, FEE, END = 30000.0, 0.0007, "2026-08-20"
S = {"이평4일": _MANSE_PRESETS[0], "중심주가": _MANSE_PRESETS[2]}
SCEN = [("전체 2011~", "2011-01-03", END),
        ("IS 2017~2023", "2017-01-03", "2023-12-29"),
        ("OOS 2024~2026", "2024-01-02", END)]

# ── 1티어 수량 고정 패치 ─────────────────────────────────
_ORIG = ME.calc_qty
_ONE = {"on": False}


def _patched(seed, bid, close, tier, lp, p):
    if _ONE["on"] and tier == 1:
        return 1 if (bid > 0 and seed >= bid) else 0
    return _ORIG(seed, bid, close, tier, lp, p)


ME.calc_qty = _patched

VARIANTS = {"A 현재 (1% 금액)": (0.01, False),
            "B 1주 고정": (0.01, True),
            "C 1주 + 게이트확대": (0.50, True)}
_MF = {}


def run(name, w1, one, cap=EACH, s="2011-01-03", e=END):
    p = preset_to_params(S[name], "SOXL", cap)
    p.fee = FEE
    for L in p.levels.values():
        L.tiers[0].seed_w = w1
        L.tiers[1].seed_w = 0.99
    if name not in _MF:
        _MF[name] = build_mode_frame(p, PR)
    _ONE["on"] = one
    try:
        return run_backtest(PR, p, start=s, end=e, mode_frame=_MF[name])
    finally:
        _ONE["on"] = False


def stats(eq, cap):
    eq = eq.dropna()
    y = (eq.index[-1] - eq.index[0]).days / 365.25
    mdd = float((eq / eq.cummax() - 1).min())
    cagr = (float(eq.iloc[-1]) / cap) ** (1 / y) - 1
    dr = eq.pct_change().dropna()
    neg = dr[dr < 0]
    return {"CAGR": cagr, "MDD": mdd, "Calmar": abs(cagr / mdd) if mdd else np.nan,
            "Sharpe": float(dr.mean() / dr.std() * np.sqrt(252)) if dr.std() else np.nan,
            "Sortino": float(dr.mean() / neg.std() * np.sqrt(252))
                       if len(neg) > 1 and neg.std() else np.nan,
            "최악일": float(dr.min()), "배수": float(eq.iloc[-1]) / cap}


# ── 1) 변형별 성과 ────────────────────────────────────────
rows, EQ = [], {}
for tag, s, e in SCEN:
    for vname, (w1, one) in VARIANTS.items():
        eqs = {}
        for n in S:
            d = run(n, w1, one, EACH, s, e)["df"]
            eqs[n] = d["총자산"]
            rows.append({"시나리오": tag, "변형": vname, "대상": n,
                         **stats(d["총자산"], EACH)})
        idx = eqs["이평4일"].index.intersection(eqs["중심주가"].index)
        tot = eqs["이평4일"].reindex(idx) + eqs["중심주가"].reindex(idx)
        rows.append({"시나리오": tag, "변형": vname, "대상": "50:50 합산",
                     **stats(tot, EACH * 2)})
        if tag.startswith("전체"):
            EQ[vname] = tot
    print(f"[{tag}] {len(VARIANTS)}개 변형 완료")
R = pd.DataFrame(rows)

# ── 2) 1티어가 실제로 몇 주를 샀나 ───────────────────────
q = []
for n in S:
    for vname, (w1, one) in VARIANTS.items():
        d = run(n, w1, one)["df"]
        f = d[(d["티어"] == 1) & (d["수량"].fillna(0) > 0)]
        if not len(f):
            q.append({"전략": n, "변형": vname, "체결": 0})
            continue
        q.append({"전략": n, "변형": vname, "체결": len(f),
                  "평균수량": float(f["수량"].mean()),
                  "중앙수량": float(f["수량"].median()),
                  "최대수량": float(f["수량"].max()),
                  "평균금액": float((f["수량"] * f["종가"]).mean()),
                  "총투입": float((f["수량"] * f["종가"]).sum())})
Q = pd.DataFrame(q)
Q.to_csv(J("oneshare_qty.csv"), index=False, encoding="utf-8-sig")

# ── 3) 소액 계좌 / 고가 상황 내성 ────────────────────────
rob = []
for cap in (3000.0, 6000.0, 10000.0, 30000.0):
    for vname, (w1, one) in VARIANTS.items():
        for n in S:
            d = run(n, w1, one, cap, "2024-01-02")["df"]
            f = d[d["매수체결"].fillna(0).astype(bool)]
            vc = f["티어"].value_counts().to_dict()
            eq = d["총자산"].dropna()
            y = (eq.index[-1] - eq.index[0]).days / 365.25
            rob.append({"계좌": cap, "변형": vname, "전략": n,
                        "1티어체결": vc.get(1, 0), "2티어체결": vc.get(2, 0),
                        "CAGR": (float(eq.iloc[-1]) / cap) ** (1 / y) - 1})
RB = pd.DataFrame(rob)
RB.to_csv(J("oneshare_robust.csv"), index=False, encoding="utf-8-sig")

# ── 출력 ─────────────────────────────────────────────────
pd.set_option("display.width", 220)
print(f"\n{'='*104}\n50:50 합산 — 변형별 성과\n{'='*104}")
t = R[R["대상"] == "50:50 합산"].pivot_table(
    index="변형", columns="시나리오", values="Calmar", aggfunc="first")
print("Calmar\n" + t.round(3).to_string())
for m in ("CAGR", "MDD"):
    tt = R[R["대상"] == "50:50 합산"].pivot_table(
        index="변형", columns="시나리오", values=m, aggfunc="first")
    print(f"\n{m}\n" + (tt * 100).round(1).to_string())

print(f"\n{'='*104}\n1티어 실제 체결 수량\n{'='*104}")
print(Q.to_string(index=False))

print(f"\n{'='*104}\n소액 계좌 내성 (2024~, 2티어 체결 건수 · 정상 277/345)\n{'='*104}")
print(RB.pivot_table(index=["계좌", "전략"], columns="변형",
                     values="2티어체결", aggfunc="first").to_string())

R.to_csv(J("oneshare_stats.csv"), index=False, encoding="utf-8-sig")
pd.DataFrame(EQ).to_csv(J("oneshare_equity.csv"), encoding="utf-8-sig")
json.dump(R.to_dict("records"), io.open(J("oneshare_stats.json"), "w", encoding="utf-8"),
          ensure_ascii=False, indent=1, default=float)
print("\n저장: oneshare_stats.csv/.json · oneshare_qty.csv · "
      "oneshare_robust.csv · oneshare_equity.csv")
