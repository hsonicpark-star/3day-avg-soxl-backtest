"""
공격형 + 변동성 위험 레짐: 위험 달에 분할수 변경 (× 매수 배율) 효과 비교 (2026-11-24)
위험 레짐 = 월말 SOXX 20일 변동성 상승 & 3년 백분위 ≥ 기준 → 다음 달 적용
"""
import sys, os, itertools, pickle, tempfile
import warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import numpy as np
import pandas as pd
import multiprocessing as mp
import importlib.util

_spec = importlib.util.spec_from_file_location("vr", os.path.join(HERE, "reports", "dss_vol_regime_2026-11.py"))
VR = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(VR)
from dss_engine import load_price_data, build_weekly_rsi_series, build_mode_series, DSSParams, run_backtest_fast

_D = {}
SUBS = None


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))


def m(p, s, e, bm, dv):
    r = run_backtest_fast(p, _D["soxl"], _D["ms"], s, e, day_buy_mult=bm, day_divisions=dv)
    y = r["total_days"] / 252
    cg = (r["final_asset"] / 1e4) ** (1 / y) - 1
    md = abs(r["mdd"])
    return cg, md, cg / md


def evaluate(item):
    rp, div, mult = item
    p = DSSParams(**VR.PRESETS["공격형"])
    vr = _D["vr"][rp]
    risk_days = [d for d, r in vr.items() if r == "위험"]
    bm = {d: mult for d in risk_days} if mult != 1.0 else None
    dv = {d: div for d in risk_days} if div != 7 else None
    E = _D["end"]
    f = m(p, "2010-03-12", E, bm, dv)
    h1 = m(p, "2010-03-12", "2018-03-12", bm, dv)
    h2 = m(p, "2018-03-12", E, bm, dv)
    ho = m(p, "2024-01-02", E, bm, dv)
    subs = [m(p, s, e, bm, dv)[2] for s, e in (("2010-03-12", "2014-03-12"), ("2014-03-12", "2018-03-12"),
                                                ("2018-03-12", "2022-03-12"), ("2022-03-12", E))]
    return dict(risk_pct=rp, div=div, mult=mult, max_exposure=f"{100 * mult:.0f}%",
                CAGR=f[0] * 100, MDD=-f[1] * 100, Calmar=f[2], H1=h1[2], H2=h2[2], 봉인=ho[2],
                구간최소=min(subs))


def main():
    soxl = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    soxx = load_price_data("SOXX", "2006-12-01", "2026-12-31")["Close"].astype(float)
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    vr = {rp: VR.vol_regime_days(soxx, soxl.index, "M", rp) for rp in (0.6, 0.7, 0.8)}
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(dict(soxl=soxl, ms=ms, vr=vr, end=str(soxl.index[-1].date())), tmp)
    tmp.close()
    items = list(itertools.product((0.6, 0.7, 0.8), (7, 8, 10, 12, 15, 20), (1.0, 0.8, 0.6)))
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        df = pd.DataFrame(pool.map(evaluate, items))
    os.unlink(tmp.name)
    df.to_csv(os.path.join(HERE, "reports", "dss_vol_regime_div.csv"), index=False, encoding="utf-8-sig")
    f2 = lambda v: f"{v:.2f}"
    print("기준(위험 레짐 무시) = 7분할 × 100%")
    for rp in (0.6, 0.7, 0.8):
        sub = df[df["risk_pct"] == rp]
        print(f"\n=== 위험 기준 백분위 {rp:.0%} — 칼마 (행: 위험 시 분할수, 열: 매수 배율) ===")
        print(sub.pivot(index="div", columns="mult", values="Calmar").round(2).to_string())
    sub = df[df["risk_pct"] == 0.7].sort_values(["mult", "div"], ascending=[False, True])
    print("\n=== 위험 기준 70% 상세 ===")
    print(sub[["div", "mult", "CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소"]]
          .to_string(index=False, float_format=f2))


if __name__ == "__main__":
    main()
