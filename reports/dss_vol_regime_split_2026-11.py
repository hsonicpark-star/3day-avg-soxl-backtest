"""
공격형V 후보: 위험 레짐 달 12분할의 '1티어 금액' 계산 방식 비교 (2026-11-24)
  A  투자금 ÷ 12 (보유 총 12개까지, 예수금 한도)
  B1 (투자금 − 보유 매수금) ÷ 남은 칸 — 투자금 100%에서 멈춤
  B2 예수금 ÷ 남은 칸 — 예수금을 끝까지 고르게
× 공세모드 위험 달 보유일(8 기본 / 5) × 위험 기준(60/70/80%)
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
METHOD = {"A 투자금÷12": None, "B1 투자금잔여÷남은칸": "capital_left", "B2 예수금÷남은칸": "cash_left"}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))


def m(p, s, e, dv, dh, sm):
    r = run_backtest_fast(p, _D["soxl"], _D["ms"], s, e, day_divisions=dv, day_hold=dh, split_mode=sm)
    y = r["total_days"] / 252
    cg = (r["final_asset"] / 1e4) ** (1 / y) - 1
    md = abs(r["mdd"])
    return cg, md, cg / md


def evaluate(item):
    rp, meth, ag_h = item
    p = DSSParams(**VR.PRESETS["공격형"])
    risk = [d for d, r in _D["vr"][rp].items() if r == "위험"]
    dv = {d: 12 for d in risk}
    dh = None if ag_h == 8 else {d: ag_h for d in risk if _D["qmode"].get(d) == "AG"}
    sm = METHOD[meth]
    E = _D["end"]
    f = m(p, "2010-03-12", E, dv, dh, sm)
    h1 = m(p, "2010-03-12", "2018-03-12", dv, dh, sm)
    h2 = m(p, "2018-03-12", E, dv, dh, sm)
    ho = m(p, "2024-01-02", E, dv, dh, sm)
    subs = [m(p, s, e, dv, dh, sm)[2] for s, e in (("2010-03-12", "2014-03-12"), ("2014-03-12", "2018-03-12"),
                                                    ("2018-03-12", "2022-03-12"), ("2022-03-12", E))]
    return dict(위험기준=rp, 방식=meth, AG보유=ag_h, CAGR=f[0] * 100, MDD=-f[1] * 100, Calmar=f[2],
                H1=h1[2], H2=h2[2], 봉인=ho[2], 구간최소=min(subs))


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
    items = list(itertools.product((0.6, 0.7, 0.8), METHOD, (8, 5)))
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        df = pd.DataFrame(pool.map(evaluate, items))
    os.unlink(tmp.name)
    df.to_csv(os.path.join(HERE, "reports", "dss_vol_regime_split.csv"), index=False, encoding="utf-8-sig")
    print(df.to_string(index=False, float_format=lambda v: f"{v:.2f}"))


if __name__ == "__main__":
    main()
