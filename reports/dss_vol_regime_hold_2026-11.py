"""
공격형V 후보(위험 레짐 달 12분할)에서 위험 달 신규 티어의 최대보유기간 최적화 (2026-11-24)
A. 모드 무관 단일 기한  B. 모드별(안전/공세) 기한
위험 레짐 = 월말 SOXX 20일 변동성 상승 & 3년 백분위 ≥ 기준 → 다음 달
"""
import sys, os, itertools, pickle, tempfile
import warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import pandas as pd
import multiprocessing as mp
import importlib.util

_spec = importlib.util.spec_from_file_location("vr", os.path.join(HERE, "reports", "dss_vol_regime_2026-11.py"))
VR = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(VR)
from dss_engine import (load_price_data, build_weekly_rsi_series, build_mode_series, DSSParams,
                        run_backtest_fast, get_week_mode_map)

_D = {}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))


def m(p, s, e, dv, dh):
    r = run_backtest_fast(p, _D["soxl"], _D["ms"], s, e, day_divisions=dv, day_hold=dh)
    y = r["total_days"] / 252
    cg = (r["final_asset"] / 1e4) ** (1 / y) - 1
    md = abs(r["mdd"])
    return cg, md, cg / md


def evaluate(item):
    rp, kind, sf_h, ag_h = item
    p = DSSParams(**VR.PRESETS["공격형"])
    risk = [d for d, r in _D["vr"][rp].items() if r == "위험"]
    dv = {d: 12 for d in risk}
    if kind == "모드기본":
        dh = None
    elif kind == "단일":
        dh = {d: sf_h for d in risk}
    else:
        dh = {d: (ag_h if _D["qmode"].get(d) == "AG" else sf_h) for d in risk}
    E = _D["end"]
    f = m(p, "2010-03-12", E, dv, dh)
    h1 = m(p, "2010-03-12", "2018-03-12", dv, dh)
    h2 = m(p, "2018-03-12", E, dv, dh)
    ho = m(p, "2024-01-02", E, dv, dh)
    subs = [m(p, s, e, dv, dh)[2] for s, e in (("2010-03-12", "2014-03-12"), ("2014-03-12", "2018-03-12"),
                                                ("2018-03-12", "2022-03-12"), ("2022-03-12", E))]
    return dict(risk_pct=rp, kind=kind, SF보유=sf_h, AG보유=ag_h, CAGR=f[0] * 100, MDD=-f[1] * 100,
                Calmar=f[2], H1=h1[2], H2=h2[2], 봉인=ho[2], 구간최소=min(subs))


def main():
    soxl = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    soxx = load_price_data("SOXX", "2006-12-01", "2026-12-31")["Close"].astype(float)
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    vr = {rp: VR.vol_regime_days(soxx, soxl.index, "M", rp) for rp in (0.6, 0.7, 0.8)}
    qmode = get_week_mode_map(ms, soxl.index)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(dict(soxl=soxl, ms=ms, vr=vr, qmode=qmode, end=str(soxl.index[-1].date())), tmp)
    tmp.close()
    items = []
    for rp in (0.6, 0.7, 0.8):
        items.append((rp, "모드기본", "38", "8"))
        items += [(rp, "단일", h, h) for h in (2, 3, 4, 5, 7, 10, 15, 20, 30, 38)]
        items += [(rp, "모드별", sf, ag) for sf, ag in itertools.product((5, 10, 20, 30, 38), (3, 5, 8, 12, 15))]
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        df = pd.DataFrame(pool.map(evaluate, items))
    os.unlink(tmp.name)
    df.to_csv(os.path.join(HERE, "reports", "dss_vol_regime_hold.csv"), index=False, encoding="utf-8-sig")
    f2 = lambda v: f"{v:.2f}"
    cols = ["kind", "SF보유", "AG보유", "CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소"]
    for rp in (0.6, 0.7, 0.8):
        sub = df[df["risk_pct"] == rp]
        print(f"\n######## 위험 기준 백분위 {rp:.0%} ########")
        print("A. 단일 기한 (+ 모드 기본 38/8):")
        print(sub[sub["kind"] != "모드별"][cols].to_string(index=False, float_format=f2))
        print("B. 모드별 기한 칼마 (행: 안전 SF, 열: 공세 AG):")
        print(sub[sub["kind"] == "모드별"].pivot(index="SF보유", columns="AG보유", values="Calmar")
              .round(2).to_string())


if __name__ == "__main__":
    main()
