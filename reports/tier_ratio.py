# -*- coding: utf-8 -*-
"""만능 스위치 — 1티어 / 2티어 시드 비율 스윕.

  현재 세팅은 1티어 1% / 2티어 99%.
  tier_method='보유' 이므로 보유 0개면 1티어, 1개 보유 중이면 2티어 주문이 나간다.
  1티어는 장기보유 다리(손절 25~38일), 2티어는 1~4일 회전 주력.

  비율 r 을 바닥/중간/천장 세 레벨에 동일 적용 → (r, 100-r).
  산출: tier_sweep.csv / tier_grid.csv / tier_stats.json
"""
import os, sys, io, json, copy, warnings
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
EACH, FEE, END = 30000.0, 0.0007, "2026-08-20"
STRATS = {"이평4일": _MANSE_PRESETS[0], "중심주가": _MANSE_PRESETS[2]}
RATIOS = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 95]
CURRENT = 1
SCEN = [("전체 2011~", "2011-01-03", END),
        ("IS 2017~2023", "2017-01-03", "2023-12-29"),
        ("OOS 2024~2026", "2024-01-02", END)]

_MF = {}


def build(name, r):
    """1티어 r%, 2티어 (100-r)% 로 세 레벨 모두 설정."""
    p = preset_to_params(STRATS[name], "SOXL", EACH)
    p.fee = FEE
    for L in p.levels.values():
        L.tiers[0].seed_w = r / 100.0
        L.tiers[1].seed_w = (100 - r) / 100.0
    return p


def run(name, r, s, e):
    p = build(name, r)
    if name not in _MF:                       # mode_frame 은 seed_w 와 무관
        _MF[name] = build_mode_frame(p, PR)
    return run_backtest(PR, p, start=s, end=e, mode_frame=_MF[name])


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


# ── 1) 단독 스윕 ──────────────────────────────────────────
CURVE, rows = {}, []
for tag, s, e in SCEN:
    for name in STRATS:
        for r in RATIOS:
            res = run(name, r, s, e)
            eq = res["df"]["총자산"]
            CURVE[(tag, name, r)] = eq
            m = res.get("metrics", {}) or {}
            rows.append({"시나리오": tag, "전략": name, "1티어%": r,
                         **stats(eq, EACH),
                         "거래": m.get("거래횟수"), "승률": m.get("승률")})
    print(f"[{tag}] {len(STRATS)*len(RATIOS)}회 완료")
SW = pd.DataFrame(rows)
SW.to_csv(J("tier_sweep.csv"), index=False, encoding="utf-8-sig")

# ── 2) 두 전략 비율 조합 (2D 격자) ────────────────────────
grid = []
for tag, _, _ in SCEN:
    for ra in RATIOS:
        for rb in RATIOS:
            a = CURVE[(tag, "이평4일", ra)]
            b = CURVE[(tag, "중심주가", rb)]
            idx = a.index.intersection(b.index)
            grid.append({"시나리오": tag, "이평4일_1티어%": ra, "중심주가_1티어%": rb,
                         **stats(a.reindex(idx) + b.reindex(idx), EACH * 2)})
GD = pd.DataFrame(grid)
GD.to_csv(J("tier_grid.csv"), index=False, encoding="utf-8-sig")
print(f"2D 격자 {len(GD)}행 저장")

# ── 3) 콘솔 요약 ──────────────────────────────────────────
pd.set_option("display.width", 220)
for name in STRATS:
    print(f"\n{'='*100}\n[{name}] 1티어 비율별 — 전체 기간 2011~ · 수수료 0.07%\n{'='*100}")
    d = SW[(SW["전략"] == name) & (SW["시나리오"] == "전체 2011~")].set_index("1티어%")
    o = d[["CAGR", "MDD", "Calmar", "Sharpe", "Sortino", "최악일", "배수"]].copy()
    for c in ("CAGR", "MDD", "최악일"):
        o[c] = (o[c] * 100).map("{:7.1f}%".format)
    for c in ("Calmar", "Sharpe", "Sortino"):
        o[c] = o[c].map("{:5.2f}".format)
    o["배수"] = o["배수"].map("{:,.0f}x".format)
    print(o.to_string())

print(f"\n{'='*100}\n시나리오별 최적 1티어 비율 (Calmar 기준)\n{'='*100}")
best = SW.loc[SW.groupby(["전략", "시나리오"])["Calmar"].idxmax()]
print(best.pivot_table(index="전략", columns="시나리오",
                       values="1티어%", aggfunc="first").to_string())

print(f"\n{'='*100}\n현재(1%) 대비 Calmar 변화율 (%) — 주요 비율만\n{'='*100}")
show = [1, 10, 25, 50, 75 if 75 in RATIOS else 70, 90]
for tag, _, _ in SCEN:
    line = [f"[{tag}]"]
    for name in STRATS:
        d = SW[(SW["전략"] == name) & (SW["시나리오"] == tag)].set_index("1티어%")["Calmar"]
        base = d[CURRENT]
        line.append(f"{name}: " + " ".join(
            f"{r}%={((d[r]-base)/base*100):+.0f}" for r in show if r in d.index))
    print("  " + " | ".join(line))

print(f"\n{'='*100}\n50:50 합산 — 두 전략 모두 같은 비율일 때 (전체 기간)\n{'='*100}")
dg = GD[(GD["시나리오"] == "전체 2011~")]
same = dg[dg["이평4일_1티어%"] == dg["중심주가_1티어%"]].set_index("이평4일_1티어%")
o = same[["CAGR", "MDD", "Calmar", "Sharpe", "최악일"]].copy()
for c in ("CAGR", "MDD", "최악일"):
    o[c] = (o[c] * 100).map("{:7.1f}%".format)
for c in ("Calmar", "Sharpe"):
    o[c] = o[c].map("{:5.2f}".format)
print(o.to_string())

bb = dg.loc[dg["Calmar"].idxmax()]
print(f"\n2D 격자 최적 (전체): 이평4일 {bb['이평4일_1티어%']:.0f}% / "
      f"중심주가 {bb['중심주가_1티어%']:.0f}% → Calmar {bb['Calmar']:.2f}")
cur = dg[(dg["이평4일_1티어%"] == CURRENT) & (dg["중심주가_1티어%"] == CURRENT)].iloc[0]
print(f"현재 세팅 (1%/1%)              → Calmar {cur['Calmar']:.2f} "
      f"(격자 최적 대비 {(cur['Calmar']-bb['Calmar'])/bb['Calmar']*100:+.1f}%)")

json.dump({"ratios": RATIOS, "current": CURRENT,
           "sweep": SW.to_dict("records")},
          io.open(J("tier_stats.json"), "w", encoding="utf-8"),
          ensure_ascii=False, indent=1, default=float)
print("\n저장: tier_sweep.csv / tier_grid.csv / tier_stats.json")
