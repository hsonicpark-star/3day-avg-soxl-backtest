"""
공격형 + R5(위험 레짐 매수 축소) 민감도 — 주변 값에서도 효과가 유지되는지 (고원 확인)
축소 배율 × 위험 백분위 기준 × 변동성 기간 × 판정 주기
"""
import sys, os, itertools, pickle, tempfile, time
import warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "reports"))
import numpy as np
import pandas as pd
import multiprocessing as mp
import importlib.util

_spec = importlib.util.spec_from_file_location("vr", os.path.join(HERE, "reports", "dss_vol_regime_2026-11.py"))
VR = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(VR)
from dss_engine import load_price_data, build_weekly_rsi_series, build_mode_series, DSSParams

_D = {}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))
    VR._D.update(_D)


def evaluate(item):
    fq, rp, vw, mult = item
    p = DSSParams(**VR.PRESETS["공격형"])
    vr = _D["vr"][(fq, rp, vw)]
    bm = {d: (mult if r == "위험" else 1.0) for d, r in vr.items()}
    E = _D["end"]
    f = VR.m(p, "2010-03-12", E, None, bm)
    h1 = VR.m(p, "2010-03-12", "2018-03-12", None, bm)
    h2 = VR.m(p, "2018-03-12", E, None, bm)
    ho = VR.m(p, "2024-01-02", E, None, bm)
    return dict(freq=fq, risk_pct=rp, vol_win=vw, mult=mult, CAGR=f[0] * 100, MDD=-f[1] * 100,
                Calmar=f[2], H1=h1[2], H2=h2[2], 봉인=ho[2])


def main():
    soxl = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    soxx = load_price_data("SOXX", "2006-12-01", "2026-12-31")["Close"].astype(float)
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    vr = {(fq, rp, vw): VR.vol_regime_days(soxx, soxl.index, fq, rp, vol_win=vw)
          for fq in ("M", "W") for rp in (0.5, 0.6, 0.7, 0.8) for vw in (10, 20, 30)}
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(dict(soxl=soxl, ms=ms, vr=vr, end=str(soxl.index[-1].date())), tmp)
    tmp.close()
    items = list(itertools.product(("M", "W"), (0.5, 0.6, 0.7, 0.8), (10, 20, 30), (0.25, 0.4, 0.5, 0.6, 0.75)))
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        df = pd.DataFrame(pool.map(evaluate, items))
    os.unlink(tmp.name)
    base = dict(Calmar=1.13, H1=1.32, H2=1.22, 봉인=1.47)
    df["전부통과"] = (df.H1 > base["H1"]) & (df.H2 > base["H2"]) & (df.봉인 > base["봉인"])
    df.to_csv(os.path.join(HERE, "reports", "dss_vol_regime_sens.csv"), index=False, encoding="utf-8-sig")
    print(f"총 {len(df)}개 · 칼마 > 기준(1.13): {(df.Calmar > 1.13).sum()}개 · "
          f"반기·봉인 모두 기준 초과: {df['전부통과'].sum()}개")
    print("\n축소 배율별 (모든 기준·기간·주기 평균):")
    print(df.groupby("mult")[["CAGR", "MDD", "Calmar"]].mean().round(2).to_string())
    print("\n판정 주기 × 위험 기준별 칼마 평균 (배율 0.4~0.6):")
    mid = df[df["mult"].between(0.4, 0.6)]
    print(mid.pivot_table(index="risk_pct", columns="freq", values="Calmar", aggfunc="mean").round(2).to_string())
    print("\n변동성 기간별 칼마 평균 (배율 0.4~0.6):")
    print(mid.groupby("vol_win")["Calmar"].mean().round(2).to_string())
    print("\n상위 10:")
    print(df.sort_values("Calmar", ascending=False).head(10).to_string(index=False, float_format=lambda v: f"{v:.2f}"))


if __name__ == "__main__":
    main()
