# -*- coding: utf-8 -*-
"""듀얼스나이퍼 파라미터 최적화 (2016~) + 만능 스위치와의 조합 재평가.

  · 탐색 구간(IS) 2016-01 ~ 2022-12, 검증 구간(OOS) 2023-01 ~ 현재로 나눈다.
    (지금까지 반복 확인했듯 전 구간에 맞춘 최적값은 검증에서 무너지는 경우가 많다)
  · 로케트셋 프리셋 주변을 무작위 탐색하고, 이웃 교란에 견디는지(강건성)까지 본다.
  · 모드 소스는 '원본시트 자동' 고정.

  산출: dual_opt_2016.json
"""
import os, sys, io, json, copy, random, warnings, itertools
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, r"D:\04.backtest\02.종가평균매매")
warnings.filterwarnings('ignore')

import numpy as np, pandas as pd
import strategies.portfolio as PF

SP = os.path.dirname(os.path.abspath(__file__))
IS_S, IS_E = "2016-01-04", "2022-12-30"
OOS_S, OOS_E = "2023-01-03", "2026-09-11"
FULL_S, FULL_E = "2016-01-04", "2026-09-11"
BASE = PF.DUAL_PRESETS["🚀 공격형 (로케트셋)"]
rnd = random.Random(20260911)

soxl_C, qqq_C, soxl_l = PF._load_market_data()


def curve(pre, s, e):
    return PF._equity_dual(soxl_l, pre, "원본시트 자동", s, e)


def mar(c):
    a = np.asarray(c, float)
    if len(a) < 50 or a[0] <= 0:
        return -9, 0, 0
    y = (c.index[-1] - c.index[0]).days / 365.25
    g = (a[-1] / a[0]) ** (1 / y) - 1
    pk = np.maximum.accumulate(a)
    m = float(((a - pk) / pk).min())
    return (abs(g / m) if m else -9), g, m


def sample():
    p = dict(BASE)
    p["ag_div"] = max(3, BASE["ag_div"] + rnd.randint(-2, 2))
    p["ag_buy"] = round(BASE["ag_buy"] * (1 + rnd.uniform(-.5, .5)), 2)
    p["ag_sell_alpha"] = round(max(.05, BASE["ag_sell_alpha"] + rnd.uniform(-.25, .25)), 3)
    p["ag_hold_alpha"] = round(max(.5, BASE["ag_hold_alpha"] + rnd.uniform(-1, 1)), 2)
    p["sf_div"] = max(3, BASE["sf_div"] + rnd.randint(-1, 2))
    p["sf_buy1"] = round(BASE["sf_buy1"] + rnd.uniform(-1.2, 1.2), 2)
    p["sf_buy2"] = round(max(.5, BASE["sf_buy2"] + rnd.uniform(-2.5, 2.5)), 2)
    p["sf_sell"] = round(max(.1, BASE["sf_sell"] + rnd.uniform(-.5, .5)), 2)
    p["sf_hold"] = max(2, BASE["sf_hold"] + rnd.randint(-4, 6))
    p["sf_ma_base"] = max(1, BASE["sf_ma_base"] + rnd.randint(-1, 2))
    # 티어 가중치는 등차 형태를 유지한 채 기울기만 바꾼다
    n, st = p["sf_div"], rnd.choice([4, 6, 7, 8, 10])
    p["sf_weights"] = ", ".join(str(6 + st * i) for i in range(n))
    return p


def jitter(p, pct=.12):
    q = dict(p)
    for k in ("ag_buy", "ag_sell_alpha", "ag_hold_alpha", "sf_buy2", "sf_sell"):
        q[k] = round(q[k] * (1 + rnd.uniform(-pct, pct)), 3)
    q["sf_hold"] = max(2, int(round(q["sf_hold"] * (1 + rnd.uniform(-pct, pct)))))
    return q


N = 160
print(f"로케트셋 기준선 평가 ...")
b_is = mar(curve(BASE, IS_S, IS_E))
b_oos = mar(curve(BASE, OOS_S, OOS_E))
b_full = mar(curve(BASE, FULL_S, FULL_E))
print(f"  IS  MAR {b_is[0]:.2f} (CAGR {b_is[1]*100:.0f}% MDD {b_is[2]*100:.0f}%)")
print(f"  OOS MAR {b_oos[0]:.2f} (CAGR {b_oos[1]*100:.0f}% MDD {b_oos[2]*100:.0f}%)")
print(f"  전체 MAR {b_full[0]:.2f} (CAGR {b_full[1]*100:.0f}% MDD {b_full[2]*100:.0f}%)")

print(f"\n후보 {N}개 탐색 (IS 2016~2022) ...")
cands = []
for i in range(N):
    p = sample()
    try:
        s = mar(curve(p, IS_S, IS_E))[0]
    except Exception:
        continue
    if s > -9:
        cands.append((s, p))
    if (i + 1) % 40 == 0:
        print(f"  {i+1}/{N} · 현재 최고 IS MAR {max(c[0] for c in cands):.2f}")
cands.sort(key=lambda x: -x[0])

print("\n상위 6개 — 강건성(이웃 4개 중앙값) + OOS 검증")
rows = []
for s, p in cands[:6]:
    nb = [s] + [mar(curve(jitter(p), IS_S, IS_E))[0] for _ in range(4)]
    rob = float(np.median(nb))
    o = mar(curve(p, OOS_S, OOS_E))
    f = mar(curve(p, FULL_S, FULL_E))
    rows.append({"params": p, "IS": s, "강건": rob,
                 "OOS": o[0], "OOS_CAGR": o[1], "OOS_MDD": o[2],
                 "FULL": f[0], "FULL_CAGR": f[1], "FULL_MDD": f[2]})
    print(f"  IS {s:.2f} · 강건 {rob:.2f} · OOS {o[0]:.2f} · 전체 {f[0]:.2f} "
          f"(CAGR {f[1]*100:.0f}% MDD {f[2]*100:.0f}%)")

print(f"\n기준선(로케트셋) OOS {b_oos[0]:.2f} · 전체 {b_full[0]:.2f}")
best_oos = max(rows, key=lambda r: r["OOS"]) if rows else None
if best_oos:
    print(f"최적 후보 OOS 최고 {best_oos['OOS']:.2f} → "
          f"{'개선' if best_oos['OOS'] > b_oos[0] else '개선 실패'}")

json.dump({"base": {"params": BASE, "IS": b_is[0], "OOS": b_oos[0], "FULL": b_full[0],
                    "FULL_CAGR": b_full[1], "FULL_MDD": b_full[2]},
           "top": rows},
          io.open(os.path.join(SP, "dual_opt_2016.json"), "w", encoding="utf-8"),
          ensure_ascii=False, indent=1, default=float)
print("\n저장: dual_opt_2016.json")
