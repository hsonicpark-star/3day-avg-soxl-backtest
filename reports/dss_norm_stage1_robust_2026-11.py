"""
DSS 정규화 1단계 — 후보 견고성 (2026-11-26)

후보: 매수일의 'N일 수익률 순위 x(최근 252일, 전일 기준)' 로 분할수를 정한다
        분할수 = 7 × (1 − g(1 − 2x))  (4~14, V 위험 달은 12 기준 7~24),  g < 0 → 최근 수익률이 낮을수록 분할을 늘린다(작게 산다)
      1단계에서 N=20 · g=−0.5 가 공격형 · 공격형V 모두 전 항목 통과
점검: ① N 10~60일 × g −0.25~−0.75   ② 신호를 통째로 밀어 넣은 가짜 신호(순환 이동 100회)   ③ 연도별 수익률 차이
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


def _load(name, fn):
    sp = importlib.util.spec_from_file_location(name, os.path.join(HERE, "reports", fn))
    m = importlib.util.module_from_spec(sp)
    sp.loader.exec_module(m)
    return m


SB = _load("sb", "sd_norm_buy_2026-11.py")
D1 = _load("d1", "d1_drawdown_regime_2026-11.py")
from dss_engine import (load_price_data, build_weekly_rsi_series, build_mode_series, DSSParams,
                        run_backtest, run_backtest_fast, get_week_mode_map, load_regime_prices)

START = "2010-03-12"
PRESET = D1.VR.PRESETS["공격형"]
NS, GS = (10, 20, 30, 40, 60), (-0.25, -0.5, -0.75)
KEYS = ["Calmar", "H1", "H2", "봉인", "구간최소"]
_D = {}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))


def dicts(base, n, g, shift=0):
    qm, risk = _D["qmode"], (_D["risk"] if base == "V" else set())
    dv, dh = {}, {}
    for d in risk:
        dv[d] = 12
        if qm.get(d) == "AG":
            dh[d] = 5
    if n:
        x = _D["sig"][n]
        if shift:
            x = np.roll(x, shift)
        for d, xv in zip(_D["idx"], x):
            if xv == xv:
                b = 12 if d in risk else 7
                f = 1.0 + g * (1.0 - 2.0 * xv)
                dv[d] = int(min(b * 2, max(round(b * 4 / 7), round(b * (2.0 - f)))))
    return dv or None, dh or None


def one(a, s, e):
    r = run_backtest_fast(DSSParams(**PRESET), _D["df"], _D["ms"], s, e or _D["end"], day_divisions=a[0], day_hold=a[1])
    y = r["total_days"] / 252
    cg = (r["final_asset"] / 1e4) ** (1 / y) - 1
    return cg * 100, -abs(r["mdd"]) * 100, cg / abs(r["mdd"])


def evaluate(item):
    base, n, g, shift, full_only = item
    a = dicts(base, n, g, shift)
    f = one(a, START, None)
    out = dict(기준=base or "-", N=n, g=g, shift=shift, CAGR=f[0], MDD=f[1], Calmar=f[2])
    if not full_only:
        for k, (s, e) in D1.DSS_WIN.items():
            out[k] = one(a, s, e)[2]
        out["구간최소"] = min(one(a, s, e)[2] for s, e in D1.DSS_SUBS)
        out["~2025"] = one(a, START, "2025-12-31")[2]
    return out


def main():
    t0 = time.time()
    os.chdir(HERE)
    df = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    idx = df.index
    c = df["Close"].astype(float)
    sig = {n: SB.rank252(c.pct_change(n)).shift(1).values for n in NS}
    months = SB.A2.risk_months(load_regime_prices().pct_change().rolling(20).std() * np.sqrt(252), 0.7)
    D = dict(df=df, ms=ms, idx=idx, qmode=get_week_mode_map(ms, idx), risk={d for d in idx if d.to_period("M") in months},
             sig=sig, end=str(idx[-1].date()))
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(D, tmp)
    tmp.close()
    rng = np.random.default_rng(20261126)
    shifts = [int(k) for k in rng.integers(120, len(idx) - 120, size=100)]
    items = []
    for base in ("", "V"):
        items.append((base, 0, 0.0, 0, False))
        items += [(base, n, g, 0, False) for n in NS for g in GS]
        items += [(base, 20, -0.5, k, True) for k in shifts]
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        res = pd.DataFrame(pool.map(evaluate, items, chunksize=2))
    os.unlink(tmp.name)
    f2 = lambda v: f"{v:.2f}"
    for base in ("-", "V"):
        s = res[res["기준"] == base]
        b = s[s["N"] == 0].iloc[0]
        g = s[(s["N"] > 0) & (s["shift"] == 0)].copy()
        g["통과"] = ["✅" if all(r[k] > b[k] for k in KEYS) else "" for _, r in g.iterrows()]
        rnd = s[s["shift"] != 0]["Calmar"].values
        cand = g[(g["N"] == 20) & (g["g"] == -0.5)].iloc[0]
        print(f"\n━━ DSS 공격형{'V' if base == 'V' else ''} — 기준 {b['CAGR']:.1f}% / {b['MDD']:.1f}% / {b['Calmar']:.2f} "
              f"(H1 {b['H1']:.2f} H2 {b['H2']:.2f} 봉인 {b['봉인']:.2f} 최소 {b['구간최소']:.2f} ~2025 {b['~2025']:.2f})")
        print(g[["N", "g", "CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소", "~2025", "통과"]].to_string(index=False, float_format=f2))
        print(f"   ② 순환 이동 100회 (N=20, g=−0.5): 후보 {cand['Calmar']:.2f} — 가짜 신호 중앙값 {np.median(rnd):.2f} · 최고 {rnd.max():.2f}"
              f" · 후보 이상 {(rnd >= cand['Calmar']).mean()*100:.0f}% · 기준({b['Calmar']:.2f}) 초과 {(rnd > b['Calmar']).mean()*100:.0f}%")

    # ③ 연도별 (전체 엔진)
    _D.update(D)
    for base in ("", "V"):
        ys = []
        for n, g in ((0, 0.0), (20, -0.5)):
            a = dicts(base, n, g)
            bt = run_backtest(DSSParams(**PRESET), df, ms, START, D["end"], day_divisions=a[0], day_hold=a[1])
            eq = pd.Series(bt["총자산"].values.astype(float), index=pd.to_datetime(bt["날짜"]))
            ys.append(eq.groupby(eq.index.year).last().pct_change() * 100)
        d = (ys[1] - ys[0]).dropna()
        print(f"   ③ 공격형{base} 연도별 수익률 차이(%p): " + " ".join(f"{y}:{v:+.0f}" for y, v in d.items()) + f" | 나은 해 {(d > 0).sum()}/{len(d)}")
    print(f"\n{time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
