# -*- coding: utf-8 -*-
"""모드 판단 기준 3종(중심주가·이평선·RSI) 해설 리포트용 데이터/통계 산출.

  · 월말 샘플 시계열 (차트용)
  · 구간 비율 · 드리프트 기울기 · 모드 전환 주기 · 분위수
  · 시트 RSI(단순평균) vs 표준 RSI(와일더) 대조
  · 중심주가 드리프트 상세 (연도별 최저 이격도)

  산출: modes_data.json
"""
import os, sys, io, json, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, r"D:\04.backtest\02.종가평균매매")
warnings.filterwarnings('ignore')

import numpy as np, pandas as pd
from common.pricedb import load_prices
from manse_engine import build_mode_frame, build_weekly_closes, simple_rsi
from strategies.manse import _MANSE_PRESETS, preset_to_params

SP = os.path.dirname(os.path.abspath(__file__))
PR = {"SOXL": load_prices("SOXL"), "QQQ": load_prices("QQQ")}

# 프리셋 인덱스: 0=이평4일, 1=이평1일, 2=중심주가, 3=RSI
SPECS = [
    ("center", "중심주가", 2, "중심주가"),
    ("ma",     "이평선",   0, "MA120"),
    ("rsi",    "RSI",      3, "wRSI"),
]
OUT = {}

for key, label, pi, refcol in SPECS:
    p = preset_to_params(_MANSE_PRESETS[pi], "SOXL", 30000.0)
    mf = build_mode_frame(p, PR)
    gapcol = "wRSI" if key == "rsi" else "이격도"
    keep = ["주봉종가", gapcol] + ([] if key == "rsi" else [refcol])
    mf = mf[keep + ["판정"]].dropna(subset=[gapcol])

    g = mf[gapcol]
    t = (mf.index - mf.index[0]).days / 365.25
    slope = float(np.polyfit(t, g, 1)[0])
    m = mf["판정"]
    switches = int((m != m.shift()).sum() - 1)
    vc = m.value_counts(normalize=True)

    if key == "rsi":
        low, high = float(p.rsi_low), float(p.rsi_high)
    elif key == "center":
        low, high = float(p.center_low), float(p.center_high)
    else:
        low, high = float(p.ma_low), float(p.ma_high)

    d = mf.resample("ME").last().dropna(subset=[gapcol])
    if key == "rsi":
        series = [[str(i.date())[:7].replace("-", ""), round(float(a), 2), round(float(b), 1)]
                  for i, a, b in zip(d.index, d["주봉종가"], d[gapcol])]
    else:
        series = [[str(i.date())[:7].replace("-", ""), round(float(a), 2), round(float(b), 2)]
                  for i, a, b in zip(d.index, d["주봉종가"], d[refcol])]

    eras = []
    for a, b in [(2010, 2013), (2014, 2017), (2018, 2021), (2022, 2026)]:
        s = mf[(mf.index.year >= a) & (mf.index.year <= b)]
        if not len(s):
            continue
        v = s["판정"].value_counts(normalize=True)
        eras.append({"기간": f"{a}~{b}", "평균": float(s[gapcol].mean()),
                     "바닥": float(v.get("바닥", 0)), "중간": float(v.get("중간", 0)),
                     "천장": float(v.get("천장", 0))})

    OUT[key] = {
        "label": label, "low": low, "high": high, "series": series,
        "n_weeks": int(len(mf)), "slope": slope, "switches": switches,
        "avg_run": float(len(m) / max(switches, 1)),
        "ratio": {k: float(vc.get(k, 0)) for k in ("바닥", "중간", "천장")},
        "pct": {str(q): float(np.percentile(g, q)) for q in (5, 25, 50, 75, 95)},
        "now": float(g.iloc[-1]), "now_mode": str(m.iloc[-1]),
        "min": float(g.min()), "max": float(g.max()),
        "start": str(mf.index[0].date()), "end": str(mf.index[-1].date()),
        "eras": eras,
        "tiers": [{"구간": lv, "티어": i + 1, "시드": t.seed_w,
                   "매수목표": t.buy_gap, "매도목표": t.sell_gap, "손절일수": t.stop_days}
                  for lv, L in p.levels.items() for i, t in enumerate(L.tiers)],
    }
    print(f"[{label}] 주봉 {len(mf)} · 기울기 {slope:+.3f}/년 · "
          f"전환 {switches}회 · 현재 {g.iloc[-1]:.2f} ({m.iloc[-1]})")

# ── 중심주가 드리프트 상세 ────────────────────────────────
p = preset_to_params(_MANSE_PRESETS[2], "SOXL", 30000.0)
mf = build_mode_frame(p, PR).dropna(subset=["중심주가"])
OUT["center"]["yearly_min"] = [
    {"연도": int(y), "최저": float(s.min()), "평균": float(s.mean()),
     "최고": float(s.max()), "바닥도달": bool(s.min() < p.center_low)}
    for y, s in mf["이격도"].groupby(mf.index.year) if y >= 2016]
q = PR["QQQ"]["Close"]
for a in (2010, 2016):
    s = q[str(a):]
    yrs = (s.index[-1] - s.index[0]).days / 365.25
    OUT["center"][f"qqq_cagr_{a}"] = float((float(s.iloc[-1]) / float(s.iloc[0])) ** (1 / yrs) - 1)
OUT["center"]["assumed_cagr"] = 1.0132 ** 12 - 1
OUT["center"]["base"], OUT["center"]["rate"] = 100.31, 1.0132

# ── RSI: 시트 방식 vs 표준(와일더) ────────────────────────
wk = build_weekly_closes(q)
sma = simple_rsi(wk, 14)
dd = wk.diff()
au = dd.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
ad = (-dd).clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
wil = 100 - 100 / (1 + au / ad.replace(0, np.nan))
c = pd.DataFrame({"sheet": sma, "wilder": wil}).dropna()
cls = lambda v: "바닥" if v < 36 else ("중간" if v <= 60 else "천장")
OUT["rsi"]["vs_wilder"] = {
    "corr": float(c.corr().iloc[0, 1]),
    "mean_diff": float((c["sheet"] - c["wilder"]).mean()),
    "max_diff": float((c["sheet"] - c["wilder"]).abs().max()),
    "now_sheet": float(c["sheet"].iloc[-1]), "now_wilder": float(c["wilder"].iloc[-1]),
    "disagree": int(sum(1 for a, b in zip(c["sheet"], c["wilder"]) if cls(a) != cls(b))),
    "total": int(len(c)),
}

# ── 상관행렬 (모드 기준끼리가 아니라 전략 수익률) ─────────
from manse_engine import run_backtest
eqs = {}
for pi, lab in ((0, "이평4일"), (2, "중심주가"), (3, "RSI")):
    pp = preset_to_params(_MANSE_PRESETS[pi], "SOXL", 30000.0)
    pp.fee = 0.0007
    eqs[lab] = run_backtest(PR, pp, start="2011-01-03", end="2026-08-20",
                            mode_frame=build_mode_frame(pp, PR))["df"]["총자산"]
rets = pd.DataFrame({k: v.pct_change() for k, v in eqs.items()}).dropna()
OUT["corr"] = {a: {b: float(rets.corr().loc[a, b]) for b in rets.columns}
               for a in rets.columns}

json.dump(OUT, io.open(os.path.join(SP, "modes_data.json"), "w", encoding="utf-8"),
          ensure_ascii=False, indent=1)
print("\n저장: modes_data.json")
print("전략 수익률 상관:",
      {f"{a}~{b}": round(OUT['corr'][a][b], 3)
       for i, a in enumerate(rets.columns) for b in list(rets.columns)[i + 1:]})
