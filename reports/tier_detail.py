# -*- coding: utf-8 -*-
"""티어 비율 분석 보조 — 티어별 기여 분해 + 1주 하한선 검증.

  산출: tier_bytier.csv / tier_floor.csv / tier_fine.csv
"""
import os, sys, io, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, r"D:\04.backtest\02.종가평균매매")
warnings.filterwarnings('ignore')

import numpy as np, pandas as pd
from common.pricedb import load_prices
from manse_engine import run_backtest, build_mode_frame
from strategies.manse import _MANSE_PRESETS, preset_to_params

SP = os.path.dirname(os.path.abspath(__file__))
J = lambda *a: os.path.join(SP, *a)
PR = {"SOXL": load_prices("SOXL"), "QQQ": load_prices("QQQ")}
FEE, END = 0.0007, "2026-08-20"
S = {"이평4일": _MANSE_PRESETS[0], "중심주가": _MANSE_PRESETS[2]}
_MF = {}


def run(n, r, cap=30000.0, s="2011-01-03", e=END):
    p = preset_to_params(S[n], "SOXL", cap)
    p.fee = FEE
    for L in p.levels.values():
        L.tiers[0].seed_w = r / 100.0
        L.tiers[1].seed_w = (100 - r) / 100.0
    if n not in _MF:
        _MF[n] = build_mode_frame(p, PR)
    return run_backtest(PR, p, start=s, end=e, mode_frame=_MF[n])


def stat(eq, cap):
    eq = eq.dropna()
    y = (eq.index[-1] - eq.index[0]).days / 365.25
    m = float((eq / eq.cummax() - 1).min())
    c = (float(eq.iloc[-1]) / cap) ** (1 / y) - 1
    return c, m, abs(c / m)


# ── 1) 티어별 기여 (거래 특성은 비율과 무관 — 1% 기준으로 산출) ──
bt = []
for n in S:
    t = run(n, 1)["by_tier"].copy()
    t.insert(0, "전략", n)
    bt.append(t)
BT = pd.concat(bt, ignore_index=True)
BT.to_csv(J("tier_bytier.csv"), index=False, encoding="utf-8-sig")
print("=" * 92)
print("티어별 거래 특성 (전체 기간) — 비율을 바꿔도 이 표는 변하지 않는다")
print("=" * 92)
print(BT.to_string(index=False))

# ── 2) 1% 미만 정밀 스윕 ──────────────────────────────────
fine = []
for r in (0.1, 0.25, 0.5, 1, 2, 3, 5):
    for n in S:
        c, m, cal = stat(run(n, r)["df"]["총자산"], 30000.0)
        fine.append({"1티어%": r, "전략": n, "CAGR": c, "MDD": m, "Calmar": cal})
FINE = pd.DataFrame(fine)
FINE.to_csv(J("tier_fine.csv"), index=False, encoding="utf-8-sig")
print(f"\n{'='*92}\n1% 미만 정밀 스윕\n{'='*92}")
print(FINE.pivot_table(index="1티어%", columns="전략",
                       values=["CAGR", "MDD", "Calmar"]).round(4).to_string())

# ── 3) 1주 하한선 — 스위치가 켜지는가 ─────────────────────
px = PR["SOXL"]["Close"]
floor = []
for cap in (3000.0, 6000.0, 10000.0, 30000.0, 60000.0, 100000.0):
    for r in (0.1, 0.5, 1, 2):
        for n in S:
            res = run(n, r, cap, "2024-01-02")
            d = res["df"]
            f = d[d["매수체결"].fillna(0).astype(bool)]
            vc = f["티어"].value_counts().to_dict()
            eq = d["총자산"].dropna()
            y = (eq.index[-1] - eq.index[0]).days / 365.25
            floor.append({"계좌": cap, "1티어%": r, "1티어금액": cap * r / 100,
                          "전략": n, "1티어체결": vc.get(1, 0), "2티어체결": vc.get(2, 0),
                          "CAGR": (float(eq.iloc[-1]) / cap) ** (1 / y) - 1})
FL = pd.DataFrame(floor)
FL.to_csv(J("tier_floor.csv"), index=False, encoding="utf-8-sig")
print(f"\n{'='*92}\n1주 하한선 — 2024~ 구간 체결 건수 (스위치가 안 켜지면 2티어가 사라진다)\n{'='*92}")
print(f"SOXL 현재 ${float(px.iloc[-1]):.2f} · 2024년 이후 최고 ${float(px['2024':].max()):.2f}")
p = FL[FL["전략"] == "이평4일"].pivot_table(
    index="계좌", columns="1티어%", values="2티어체결", aggfunc="first")
print("\n[이평4일] 2티어 체결 건수")
print(p.to_string())

# 안전 여유: 1티어 금액 / 현재가 = 살 수 있는 주식 수
now = float(px.iloc[-1])
print(f"\n계좌 $30,000 · 1티어 1% = $300 → 현재가 기준 {300/now:.1f}주")
print(f"  스위치가 꺼지는 주가 = $300 초과  (2024년 이후 최고가 ${float(px['2024':].max()):.2f})")
print("\n저장: tier_bytier.csv / tier_fine.csv / tier_floor.csv")
