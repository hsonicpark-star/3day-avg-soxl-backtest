"""
DSS 모멘텀 분할(M) 후보 심층 점검 (2026-11-26)

후보 M: 매수일의 'N일 수익률 순위 x (최근 1년, 전일 기준)' 로 그날의 분할수를 정한다
        분할수 = 7 × (1 − g + 2g·x), 4~14 로 제한.  g = −0.5 → x=0(가장 약함) 10~11분할, x=1(가장 강함) 4분할

① V 와의 관계 — 공격형 / V / M / V+M (세 가지 결합 방식)
     scale : 위험 달은 12분할을 기준으로 같은 식 (7~24)
     v12   : 위험 달은 V 그대로 12분할 고정, 그 외 달만 M
     hold5 : 분할수는 전부 M, 위험 달 공세 보유 5일만 V 에서 가져옴
② 고정 분할수와 비교 — 분할수 상수만 바꿔도 같은 효과인가 (4~12분할)
③ 순위 창 126 / 504일, 분할수 범위, 계단식(3칸) 버전, 신호 종목(SOXL / SOXX / QQQ)
④ 주요 4종의 추가 지표 (샤프 · 울서 · 큰 낙폭 5건 평균 · 최악의 해 · 평균 노출)
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


D1 = _load("d1", "d1_drawdown_regime_2026-11.py")
A2 = _load("a2", "a2_tr_regime_2026-11.py")
from dss_engine import (load_price_data, build_weekly_rsi_series, build_mode_series, DSSParams,
                        run_backtest, run_backtest_fast, get_week_mode_map, load_regime_prices)

START = "2010-03-12"
PRESET = D1.VR.PRESETS["공격형"]
KEYS = ["Calmar", "H1", "H2", "봉인", "구간최소"]
_D = {}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))


def M(n=20, g=-0.5, lo=4, hi=14, win=252, basis="SOXL", step=None):
    return dict(n=n, g=g, lo=lo, hi=hi, win=win, basis=basis, step=step)


def dicts(cfg):
    """cfg: v(bool) · m(M dict|None) · combo('scale'|'v12'|'hold5') · fixed(int|None)"""
    qm, risk = _D["qmode"], _D["risk"]
    v, m, combo, fixed = cfg.get("v"), cfg.get("m"), cfg.get("combo", "scale"), cfg.get("fixed")
    x = _D["sig"][(m["basis"], m["n"], m["win"])] if m else None
    dv, dh = {}, {}
    for i, d in enumerate(_D["idx"]):
        rk = v and d in risk
        if rk and qm.get(d) == "AG":
            dh[d] = 5
        if rk and combo != "hold5":
            dv[d] = 12
        elif fixed:
            dv[d] = fixed
        if m is None or (rk and combo == "v12"):
            continue
        xv = x[i]
        if xv != xv:
            continue
        b = 12 if (rk and combo == "scale") else 7
        if m["step"]:
            weak, mid, strong = m["step"]
            k = weak if xv < 1 / 3 else (mid if xv < 2 / 3 else strong)
            dv[d] = int(round(k * b / 7))
        else:
            raw = b * (1.0 - m["g"] + 2.0 * m["g"] * xv)
            dv[d] = int(min(round(m["hi"] * b / 7), max(round(m["lo"] * b / 7), round(raw))))
    return dv or None, dh or None


def one(a, s, e):
    r = run_backtest_fast(DSSParams(**PRESET), _D["df"], _D["ms"], s, e or _D["end"], day_divisions=a[0], day_hold=a[1])
    y = r["total_days"] / 252
    cg = (r["final_asset"] / 1e4) ** (1 / y) - 1
    return cg * 100, -abs(r["mdd"]) * 100, cg / abs(r["mdd"])


def evaluate(item):
    name, cfg = item
    a = dicts(cfg)
    f = one(a, START, None)
    out = dict(이름=name, CAGR=f[0], MDD=f[1], Calmar=f[2])
    for k, (s, e) in D1.DSS_WIN.items():
        out[k] = one(a, s, e)[2]
    out["구간최소"] = min(one(a, s, e)[2] for s, e in D1.DSS_SUBS)
    out["~2025"] = one(a, START, "2025-12-31")[2]
    return out


def rank(s, w):
    return s.rolling(w, min_periods=60).apply(lambda z: (z[:-1] < z[-1]).mean(), raw=True)


def extra(name, cfg, df, ms, end):
    a = dicts(cfg)
    bt = run_backtest(DSSParams(**PRESET), df, ms, START, end, day_divisions=a[0], day_hold=a[1])
    eq = pd.Series(bt["총자산"].values.astype(float), index=pd.to_datetime(bt["날짜"]))
    expo = (pd.Series(bt["평가금"].values.astype(float), index=eq.index) / eq).mean() * 100
    r = eq.pct_change().dropna()
    dd = eq / eq.cummax() - 1
    s, tops = dd.copy(), []
    for _ in range(5):
        t = s.idxmin(); pk = eq[:t].idxmax()
        rec = dd[t:][dd[t:] > -1e-9]
        en = rec.index[0] if len(rec) else dd.index[-1]
        tops.append(dd[t] * 100); s[pk:en] = 0
    yr = eq.groupby(eq.index.year).last().pct_change().dropna() * 100
    mon = eq.groupby(eq.index.to_period("M")).last().pct_change().dropna()
    return dict(이름=name, 샤프=r.mean() / r.std() * np.sqrt(252), 울서=float(np.sqrt((dd ** 2).mean()) * 100),
                큰낙폭5평균=float(np.mean(tops)), 최악의해=float(yr.min()), 손실해=int((yr < 0).sum()),
                월승률=float((mon > 0).mean() * 100), 평균노출=expo, 평균분할=float(np.mean(list(a[0].values()))) if a[0] else 7.0), yr


def main():
    t0 = time.time()
    os.chdir(HERE)
    df = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    idx = df.index
    soxx = load_regime_prices()
    closes = {"SOXL": df["Close"].astype(float), "SOXX": soxx.reindex(idx).ffill(), "QQQ": qqq["Close"].astype(float).reindex(idx).ffill()}
    sig = {}
    for basis, n, w in [("SOXL", n, 252) for n in (10, 20, 30, 40, 60)] + [("SOXL", n, w) for n in (20, 40) for w in (126, 504)] \
            + [(b, n, 252) for b in ("SOXX", "QQQ") for n in (20, 40)]:
        sig[(basis, n, w)] = rank(closes[basis].pct_change(n), w).shift(1).values
    months = A2.risk_months(soxx.pct_change().rolling(20).std() * np.sqrt(252), 0.7)
    D = dict(df=df, ms=ms, idx=idx, qmode=get_week_mode_map(ms, idx), risk={d for d in idx if d.to_period("M") in months},
             sig=sig, end=str(idx[-1].date()))
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(D, tmp)
    tmp.close()

    groups = {}
    groups["① V 와의 관계 (M = 20일 · g −0.5 / 40일)"] = [
        ("공격형", {}), ("V", dict(v=True)),
        ("M20", dict(m=M(20))), ("M40", dict(m=M(40))),
        ("V+M20 scale", dict(v=True, m=M(20), combo="scale")), ("V+M20 v12", dict(v=True, m=M(20), combo="v12")),
        ("V+M20 hold5", dict(v=True, m=M(20), combo="hold5")),
        ("V+M40 scale", dict(v=True, m=M(40), combo="scale")), ("V+M40 v12", dict(v=True, m=M(40), combo="v12")),
        ("V+M40 hold5", dict(v=True, m=M(40), combo="hold5"))]
    groups["② 고정 분할수 (상수만 바꾼 경우)"] = \
        [(f"고정 {k}분할", dict(fixed=k)) for k in (4, 5, 6, 8, 9, 10, 12)] + \
        [(f"V + 평소 {k}분할", dict(v=True, fixed=k)) for k in (5, 6, 8, 9)]
    groups["③-1 순위 창"] = [(f"{'V+' if v else ''}M{n} 창{w}", dict(v=v, m=M(n, win=w), combo="v12"))
                          for v in (False, True) for n in (20, 40) for w in (126, 504)]
    groups["③-2 분할수 범위 (M20)"] = [(f"{'V+' if v else ''}범위 {lo}~{hi}", dict(v=v, m=M(20, lo=lo, hi=hi), combo="v12"))
                                 for v in (False, True) for lo, hi in ((3, 14), (4, 10), (5, 10), (5, 12), (6, 9))]
    groups["③-3 계단식 3칸 (약함/보통/강함 분할수, M20 · M40)"] = \
        [(f"{'V+' if v else ''}M{n} 계단 {s[0]}/{s[1]}/{s[2]}", dict(v=v, m=M(n, step=s), combo="v12"))
         for v in (False, True) for n in (20, 40) for s in ((10, 7, 5), (9, 7, 5), (10, 7, 4), (12, 7, 5), (9, 7, 6))]
    groups["③-4 신호 종목"] = [(f"{'V+' if v else ''}{b} {n}일", dict(v=v, m=M(n, basis=b), combo="v12"))
                           for v in (False, True) for b in ("SOXX", "QQQ") for n in (20, 40)]
    items = [it for g in groups.values() for it in g]
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        res = pd.DataFrame(pool.map(evaluate, items, chunksize=1)).set_index("이름")
    os.unlink(tmp.name)
    b0, bv = res.loc["공격형"], res.loc["V"]
    f2 = lambda v: f"{v:.2f}"
    res[">공격형"] = ["✅" if all(r[k] > b0[k] for k in KEYS) else "" for _, r in res.iterrows()]
    res[">V"] = ["✅" if all(r[k] > bv[k] for k in KEYS) else "" for _, r in res.iterrows()]
    for title, g in groups.items():
        print(f"\n━━ {title}")
        print(res.loc[[n for n, _ in g]].reset_index().to_string(index=False, float_format=f2))
    res.reset_index().to_csv(os.path.join(HERE, "reports", "dss_m_deep.csv"), index=False, encoding="utf-8-sig")

    _D.update(D)
    rows, yrs = [], {}
    for name, cfg in (("공격형", {}), ("V", dict(v=True)), ("M40", dict(m=M(40))), ("V+M40 v12", dict(v=True, m=M(40), combo="v12")),
                      ("V+M40 계단 10/7/5", dict(v=True, m=M(40, step=(10, 7, 5)), combo="v12"))):
        r, y = extra(name, cfg, df, ms, D["end"])
        rows.append(r); yrs[name] = y
    print("\n━━ ④ 추가 지표")
    print(pd.DataFrame(rows).to_string(index=False, float_format=f2))
    print("\n연도별 수익률 (%)")
    print(pd.DataFrame(yrs).round(0).T.to_string())
    print(f"\n{time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
