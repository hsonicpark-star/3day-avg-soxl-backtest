# -*- coding: utf-8 -*-
"""결정적 검증 — 훈련 구간 최적 비중을 검증 구간에 적용하면 균등보다 나은가?

  IS  : 2017-01-03 ~ 2023-12-29 에서 조합별 최적 비중 탐색 (5% 격자 전수)
  OOS : 2024-01-02 ~ 2026-08-20 에 그 비중을 그대로 적용
  비교 : 같은 구간의 균등 비중 / 사후 최적 비중

  총자본 $60,000 고정. combo_weights.py 와 동일한 방식으로
  각 프리셋을 자본 5~100% 20단계로 미리 백테스트해 임의 비중을 정확히 합성한다.

  산출: weights_oos_test.csv  (mkreport_weights.py 가 읽는다)
"""
import os, sys, io, itertools, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, r"D:\04.backtest\02.종가평균매매")
warnings.filterwarnings('ignore')

import numpy as np, pandas as pd
from common.pricedb import load_prices
from manse_engine import run_backtest, build_mode_frame
from strategies.manse import _MANSE_PRESETS, preset_to_params

SP = os.path.dirname(os.path.abspath(__file__))
PR = {"SOXL": load_prices("SOXL"), "QQQ": load_prices("QQQ")}
TOTAL, FEE, STEP = 60000.0, 0.0007, 5
KEYS = ["이평4일", "이평1일", "중심주가", "RSI"]
PRESET = dict(zip(KEYS, _MANSE_PRESETS))
STEPS = list(range(STEP, 101, STEP))
NS = 100 // STEP
_MF = {}


def curve(key, pct, start, end):
    p = preset_to_params(PRESET[key], "SOXL", TOTAL * pct / 100.0)
    p.fee = FEE
    if key not in _MF:
        _MF[key] = build_mode_frame(p, PR)
    return run_backtest(PR, p, start=start, end=end,
                        mode_frame=_MF[key])["df"]["총자산"]


def bank(start, end):
    """프리셋 x 자본단계 자산곡선 (공통 거래일로 정렬)."""
    B, idx = {}, None
    for k in KEYS:
        B[k] = {}
        for pct in STEPS:
            c = curve(k, pct, start, end)
            B[k][pct] = c
            idx = c.index if idx is None else idx.intersection(c.index)
    return {k: {p: v.reindex(idx) for p, v in d.items()} for k, d in B.items()}


def calmar(eq):
    eq = eq.dropna()
    yrs = (eq.index[-1] - eq.index[0]).days / 365.25
    mdd = float((eq / eq.cummax() - 1).min())
    cagr = (float(eq.iloc[-1]) / TOTAL) ** (1 / yrs) - 1
    return abs(cagr / mdd) if mdd else np.nan


def comps(total_steps, n):
    """total_steps 를 n개 양수로 쪼개는 모든 조합."""
    if n == 1:
        yield (total_steps,)
        return
    for i in range(1, total_steps - n + 2):
        for rest in comps(total_steps - i, n - 1):
            yield (i,) + rest


IS = bank("2017-01-03", "2023-12-29")
OOS = bank("2024-01-02", "2026-08-20")
port = lambda B, w: sum(B[k][v] for k, v in w.items() if v > 0)

rows = []
for n in (2, 3, 4):
    for combo in itertools.combinations(KEYS, n):
        grid = [{k: v * STEP for k, v in zip(combo, c)} for c in comps(NS, n)]
        w_is = max(grid, key=lambda w: calmar(port(IS, w)))
        base, rem = divmod(NS, n)          # 균등 비중 (최대잔여법)
        w_eq = {k: (base + (1 if i < rem else 0)) * STEP
                for i, k in enumerate(combo)}
        c_opt, c_eq = calmar(port(OOS, w_is)), calmar(port(OOS, w_eq))
        c_best = max(calmar(port(OOS, w)) for w in grid)
        rows.append({"조합": "+".join(combo), "개수": n,
                     "IS최적비중": "/".join(str(w_is[k]) for k in combo),
                     "OOS(IS최적)": c_opt, "OOS(균등)": c_eq,
                     "OOS(사후최적)": c_best,
                     "균등대비": (c_opt - c_eq) / c_eq * 100})
        print(f"  {'+'.join(combo):28s} IS최적 {rows[-1]['IS최적비중']:12s} "
              f"→ OOS {c_opt:.2f} vs 균등 {c_eq:.2f} ({rows[-1]['균등대비']:+.1f}%)")

df = pd.DataFrame(rows)
pd.set_option("display.width", 200)
print(f"\n{'='*104}\n훈련 최적비중을 검증 구간에 적용 — 균등비중 대비 (Calmar)\n{'='*104}")
o = df.copy()
for c in ("OOS(IS최적)", "OOS(균등)", "OOS(사후최적)"):
    o[c] = o[c].map("{:.2f}".format)
o["균등대비"] = o["균등대비"].map("{:+.1f}%".format)
print(o.to_string(index=False))
w = int((df["균등대비"] > 0).sum())
print(f"\n훈련최적이 균등을 이긴 경우: {w}/{len(df)}")
print(f"평균 차이: {df['균등대비'].mean():+.1f}%  "
      f"(중앙값 {df['균등대비'].median():+.1f}%)")
print(f"사후최적 대비 회수율 — 훈련최적 "
      f"{(df['OOS(IS최적)']/df['OOS(사후최적)']).mean()*100:.0f}% · "
      f"균등 {(df['OOS(균등)']/df['OOS(사후최적)']).mean()*100:.0f}%")

df.to_csv(os.path.join(SP, "weights_oos_test.csv"), index=False,
          encoding="utf-8-sig")
print("\n저장: weights_oos_test.csv")
