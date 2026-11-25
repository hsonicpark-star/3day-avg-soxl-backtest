"""
D1 낙폭 레짐 견고성 점검 (2026-11-25) — d1_drawdown_regime_2026-11.py 의 1차 통과 후보

후보 (월 판정, 조정+약세 = 월말 SOXX 낙폭 −10 ~ −30% → 다음 달)
  표준편차 균형형 · 공격형 : 매도 100% · 비중 50% 상한 (V 와 같은 규칙, 적용 달만 다름)
  DSS 공격형               : 10분할 / 12분할 + 공세 보유 5일
점검
  ① 무작위 달 300회 — 같은 개수의 달을 아무렇게나 골라 같은 규칙 (Calmar · MDD 순위)
  ② 2025년 말까지 — 2026 폭락을 빼도 남는가
  ③ 경계 흔들기 — 위 경계 −8/−10/−12% × 아래 경계 −25/−30/−35%
  ④ V1(변동성) 달과 비교 — 같은 규칙을 V1 달에 / D1 달에 / 둘 중 하나라도 / 둘 다
"""
import sys, os, pickle, tempfile, time, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "scripts"))
import numpy as np
import pandas as pd
import multiprocessing as mp
import importlib.util

_sp = importlib.util.spec_from_file_location("d1", os.path.join(HERE, "reports", "d1_drawdown_regime_2026-11.py"))
D1 = importlib.util.module_from_spec(_sp)
_sp.loader.exec_module(D1)
from dss_engine import (load_price_data, build_weekly_rsi_series, build_mode_series, DSSParams,
                        run_backtest_fast, get_week_mode_map, load_regime_prices)

START = D1.START
N_RAND = 300
SD_RULE = (1.0, 100.0, 0.5)
DSS_CANDS = {"10분할": (10, None), "12분할+공세5일": (12, 5)}
_D = {}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))
    D1._D.update(_D)


def month_mask(idx, months):
    return np.array([d.to_period("M") in months for d in idx])


def d1_months(soxx, hi=-0.10, lo=-0.30):
    dd = (soxx / soxx.rolling(252).max() - 1.0).dropna()
    ends = dd.groupby(dd.index.to_period("M")).tail(1)
    return {d.to_period("M") + 1 for d, v in ends.items() if lo <= v <= hi}


def sd_cm(a, idx, mask, end=None):
    n = len(idx)
    i1 = n if end is None else idx.searchsorted(pd.Timestamp(end))
    arrs = D1.sd_arrays(n, None, mask, SD_RULE if mask is not None else None)
    eq = D1.sd_sim(a["c"][:i1], a["pc"][:i1], a["sg"][:i1], a["p"], *[x[:i1] for x in arrs])
    y = (idx[i1 - 1] - idx[0]).days / 365.25
    cg = (eq[-1] / 1e4) ** (1 / y) - 1
    md = abs((eq / np.maximum.accumulate(eq) - 1).min())
    return cg * 100, -md * 100, cg / md


def dss_cm(item):
    months, cand, end = item
    div, agh = DSS_CANDS[cand]
    qm = _D["qmode"]
    dv, dh = {}, {}
    for d in _D["dss_idx"]:
        if d.to_period("M") in months:
            dv[d] = div
            if agh and qm.get(d) == "AG":
                dh[d] = agh
    p = DSSParams(**D1.VR.PRESETS["공격형"])
    r = run_backtest_fast(p, _D["soxl"], _D["ms"], START, end or _D["end"], day_divisions=dv or None, day_hold=dh or None)
    y = r["total_days"] / 252
    cg = (r["final_asset"] / 1e4) ** (1 / y) - 1
    return cg * 100, r["mdd"] * 100 if r["mdd"] < 0 else -abs(r["mdd"]) * 100, cg / abs(r["mdd"])


def rank(val, arr):
    return f"무작위 {N_RAND}회 중 상위 {(np.array(arr) >= val).mean() * 100:.1f}%"


def main():
    t0 = time.time()
    import update_preset_stats as U
    os.chdir(HERE)
    soxx = load_regime_prices()
    _px = U.load_adjusted("SOXL")
    U.load_adjusted = lambda tk, *a, **k: _px
    D = D1.S1.prepare()
    idx = D["idx"]
    all_m = sorted({d.to_period("M") for d in idx})
    dm = d1_months(soxx) & set(all_m)
    vm = {d.to_period("M") for d, r in zip(idx, D["risk"][0.7]) if r}
    print(f"\n달 수: 전체 {len(all_m)} · D1(조정+약세) {len(dm)} · V1 위험 {len(vm)} · 겹침 {len(dm & vm)}")
    rng = np.random.default_rng(20261125)
    rand_sets = [set(rng.choice(all_m, size=len(dm), replace=False)) for _ in range(N_RAND)]

    soxl = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(dict(soxl=soxl, ms=ms, qmode=get_week_mode_map(ms, soxl.index), dss_idx=soxl.index,
                     end=str(soxl.index[-1].date())), tmp)
    tmp.close()
    f = lambda t: f"{t[0]:.1f}% / {t[1]:.1f}% / {t[2]:.2f}"

    # ── 표준편차 ──
    for lab in ("균형형", "공격형"):
        a = D["arr"][lab]
        base, cand = sd_cm(a, idx, None), sd_cm(a, idx, month_mask(idx, dm))
        rnd = [sd_cm(a, idx, month_mask(idx, s)) for s in rand_sets]
        print(f"\n━━ 표준편차 {lab} — 기준 {f(base)} → D1 {f(cand)}")
        print(f"   ① Calmar {rank(cand[2], [x[2] for x in rnd])} (무작위 중앙값 {np.median([x[2] for x in rnd]):.2f}) · "
              f"MDD {rank(cand[1], [x[1] for x in rnd])} · CAGR 무작위 중앙값 {np.median([x[0] for x in rnd]):.1f}%")
        b25, c25 = sd_cm(a, idx, None, "2026-01-01"), sd_cm(a, idx, month_mask(idx, dm), "2026-01-01")
        print(f"   ② ~2025년 말: 기준 {f(b25)} → D1 {f(c25)}")
        print("   ③ 경계 흔들기 (Calmar): " + " · ".join(
            f"{int(hi*100)}~{int(lo*100)}% {sd_cm(a, idx, month_mask(idx, d1_months(soxx, hi, lo)))[2]:.2f}"
            for hi in (-0.08, -0.10, -0.12) for lo in (-0.25, -0.30, -0.35)))
        for nm, mset in (("V1 달", vm), ("D1 달", dm), ("V1 ∪ D1", vm | dm), ("V1 ∩ D1", vm & dm), ("D1 − V1", dm - vm)):
            print(f"   ④ {nm:<8} ({len(mset):3d}개월): {f(sd_cm(a, idx, month_mask(idx, mset)))}"
                  f"  | ~2025: {f(sd_cm(a, idx, month_mask(idx, mset), '2026-01-01'))}")

    # ── DSS ──
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        base = dss_cm_main(pool, [(set(), "10분할", None), (set(), "10분할", "2025-12-31")])
        for cand in DSS_CANDS:
            sets = [(dm, cand, None), (dm, cand, "2025-12-31"), (vm, cand, None), (vm | dm, cand, None),
                    (vm & dm, cand, None), (dm - vm, cand, None)]
            edges = [(hi, lo) for hi in (-0.08, -0.10, -0.12) for lo in (-0.25, -0.30, -0.35)]
            sets += [(d1_months(soxx, hi, lo), cand, None) for hi, lo in edges]
            sets += [(s, cand, None) for s in rand_sets]
            res = dss_cm_main(pool, sets)
            c, c25, rv, ru, ri, rd = res[:6]
            ed, rnd = res[6:6 + len(edges)], res[6 + len(edges):]
            print(f"\n━━ DSS 공격형 [{cand}] — 기준 {f(base[0])} → D1 {f(c)}")
            print(f"   ① Calmar {rank(c[2], [x[2] for x in rnd])} (무작위 중앙값 {np.median([x[2] for x in rnd]):.2f}) · "
                  f"MDD {rank(c[1], [x[1] for x in rnd])} · CAGR 무작위 중앙값 {np.median([x[0] for x in rnd]):.1f}%")
            print(f"   ② ~2025년 말: 기준 {f(base[1])} → D1 {f(c25)}")
            print("   ③ 경계 흔들기 (Calmar): " + " · ".join(f"{int(hi*100)}~{int(lo*100)}% {x[2]:.2f}" for (hi, lo), x in zip(edges, ed)))
            print(f"   ④ V1 달 {f(rv)} · V1 ∪ D1 {f(ru)} · V1 ∩ D1 {f(ri)} · D1 − V1 {f(rd)}")
    os.unlink(tmp.name)
    print(f"\n{time.time() - t0:.0f}초")


def dss_cm_main(pool, items):
    return pool.map(dss_cm, items, chunksize=4)


if __name__ == "__main__":
    main()
