# -*- coding: utf-8 -*-
"""만능 스위치 프리셋 — 비대칭 배분(가중치) 전수 분석.

  총자본 $60,000 고정. 비중은 5% 단위.
  각 프리셋을 자본 5%~100% (20단계) 로 미리 백테스트해 두면
  임의 비중 포트폴리오는 그 곡선들의 합으로 정확히 구성된다.
    (정수 주식수 반올림까지 그대로 반영 — 스케일 가정 없음)

  산출: weights_stats.json / weights_sweep2.csv / weights_best.csv
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
J = lambda *a: os.path.join(SP, *a)

PR = {"SOXL": load_prices("SOXL"), "QQQ": load_prices("QQQ")}
TOTAL, FEE, STEP, END = 60000.0, 0.0007, 5, "2026-08-20"
KEYS = ["이평4일", "이평1일", "중심주가", "RSI"]
PRESET = dict(zip(KEYS, _MANSE_PRESETS))
STEPS = list(range(STEP, 101, STEP))                 # 5,10,...,100 (%)

SCEN = [("전체 2011~", "2011-01-03", END),
        ("IS 2017~2023", "2017-01-03", "2023-12-29"),
        ("OOS 2024~2026", "2024-01-02", END)]

_MF = {}


def curve(key, pct, start, end):
    """프리셋을 총자본의 pct% 로 운용했을 때의 자산곡선."""
    p = preset_to_params(PRESET[key], "SOXL", TOTAL * pct / 100.0)
    p.fee = FEE
    if key not in _MF:
        _MF[key] = build_mode_frame(p, PR)
    return run_backtest(PR, p, start=start, end=end,
                        mode_frame=_MF[key])["df"]["총자산"]


def stats(eq):
    eq = eq.dropna()
    yrs = (eq.index[-1] - eq.index[0]).days / 365.25
    mdd = float((eq / eq.cummax() - 1).min())
    cagr = (float(eq.iloc[-1]) / TOTAL) ** (1 / yrs) - 1
    dr = eq.pct_change().dropna()
    neg = dr[dr < 0]
    return {"CAGR": cagr, "MDD": mdd, "Calmar": abs(cagr / mdd) if mdd else np.nan,
            "Sharpe": float(dr.mean() / dr.std() * np.sqrt(252)) if dr.std() else np.nan,
            "Sortino": float(dr.mean() / neg.std() * np.sqrt(252))
                       if len(neg) > 1 and neg.std() else np.nan,
            "최악일": float(dr.min())}


# ── 1) 자본 단계별 곡선 미리 계산 ─────────────────────────
BANK = {}                       # BANK[시나리오][프리셋][pct] = 자산곡선
for tag, s, e in SCEN:
    B, idx = {}, None
    for k in KEYS:
        B[k] = {}
        for pct in STEPS:
            c = curve(k, pct, s, e)
            B[k][pct] = c
            idx = c.index if idx is None else idx.intersection(c.index)
    BANK[tag] = {k: {p: v.reindex(idx) for p, v in d.items()} for k, d in B.items()}
    print(f"[{tag}] {len(KEYS)*len(STEPS)}개 곡선 준비 완료")


def port(tag, w):
    """w = {프리셋: 비중%} → 합성 자산곡선"""
    return sum(BANK[tag][k][p] for k, p in w.items() if p > 0)


# ── 2) 2종 조합 비중 스윕 ─────────────────────────────────
sweep = []
for a, b in itertools.combinations(KEYS, 2):
    for tag, _, _ in SCEN:
        for wa in STEPS[:-1]:                       # 5~95
            st = stats(port(tag, {a: wa, b: 100 - wa}))
            sweep.append({"쌍": f"{a}+{b}", "A": a, "B": b, "A비중": wa,
                          "시나리오": tag, **st})
SW = pd.DataFrame(sweep)
SW.to_csv(J("weights_sweep2.csv"), index=False, encoding="utf-8-sig")
print(f"\n2종 스윕 {len(SW)}행 저장")

# ── 3) 3종·4종 최적 비중 (5% 단위 전수) ───────────────────
best = []


def comps(total_steps, n):
    """total_steps 를 n개 양수로 쪼개는 모든 조합 (5% 단위)"""
    if n == 1:
        yield (total_steps,)
        return
    for i in range(1, total_steps - n + 2):
        for rest in comps(total_steps - i, n - 1):
            yield (i,) + rest


NS = 100 // STEP
for n in (2, 3, 4):
    for combo in itertools.combinations(KEYS, n):
        for tag, _, _ in SCEN:
            rows = []
            for c in comps(NS, n):
                w = {k: v * STEP for k, v in zip(combo, c)}
                rows.append((w, stats(port(tag, w))))
            # 균등 비중 — 5% 격자에 최대잔여법으로 배분 (3종 -> 35/35/30)
            base, rem = divmod(NS, n)
            eq_w = {k: (base + (1 if i < rem else 0)) * STEP
                    for i, k in enumerate(combo)}
            eq_s = stats(port(tag, eq_w))
            top = max(rows, key=lambda r: r[1]["Calmar"])
            near = [r for r in rows if r[1]["Calmar"] >= top[1]["Calmar"] * 0.95]
            best.append({
                "조합": "+".join(combo), "개수": n, "시나리오": tag,
                "최적비중": "/".join(f"{top[0][k]}" for k in combo),
                "최적Calmar": top[1]["Calmar"], "최적CAGR": top[1]["CAGR"],
                "최적MDD": top[1]["MDD"],
                "균등비중": "/".join(f"{eq_w[k]}" for k in combo),
                "균등Calmar": eq_s["Calmar"], "균등CAGR": eq_s["CAGR"],
                "균등MDD": eq_s["MDD"],
                "손실%": (top[1]["Calmar"] - eq_s["Calmar"]) / top[1]["Calmar"] * 100,
                "고원비율": len(near) / len(rows) * 100, "격자수": len(rows)})
    print(f"{n}종 조합 최적화 완료")
BEST = pd.DataFrame(best)
BEST.to_csv(J("weights_best.csv"), index=False, encoding="utf-8-sig")

# ── 4) 콘솔 요약 ──────────────────────────────────────────
pd.set_option("display.width", 220)
print(f"\n{'='*104}\n최적 비중 vs 균등 비중 — 균등을 써서 잃는 Calmar (%)\n{'='*104}")
p = BEST.pivot_table(index=["개수", "조합"], columns="시나리오",
                     values="손실%", aggfunc="first").round(1)
print(p.to_string())

print(f"\n{'='*104}\n최적 비중의 구간 안정성 (전체 / IS / OOS)\n{'='*104}")
q = BEST.pivot_table(index=["개수", "조합"], columns="시나리오",
                     values="최적비중", aggfunc="first")
print(q.to_string())

print(f"\n{'='*104}\n2종 조합 — 시나리오별 최적 A비중 (%)\n{'='*104}")
opt2 = (SW.loc[SW.groupby(["쌍", "시나리오"])["Calmar"].idxmax()]
          .pivot_table(index="쌍", columns="시나리오", values="A비중", aggfunc="first"))
print(opt2.to_string())
print("\n(A = 쌍 이름의 앞쪽 프리셋)")

print(f"\n{'='*104}\n'고원 비율' — 최적 대비 95% 이상 Calmar 를 내는 격자점 비율 (%)\n{'='*104}")
print(BEST.pivot_table(index=["개수", "조합"], columns="시나리오",
                       values="고원비율", aggfunc="first").round(0).to_string())

json.dump({"total": TOTAL, "fee": FEE, "step": STEP,
           "best": BEST.to_dict("records")},
          io.open(J("weights_stats.json"), "w", encoding="utf-8"),
          ensure_ascii=False, indent=1, default=float)
print("\n저장: weights_stats.json / weights_sweep2.csv / weights_best.csv")
