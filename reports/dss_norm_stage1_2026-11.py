"""
DSS 정규화 1단계 (2026-11-26) — 엔진 수정 없이 실험용 인자(day_buy_mult · day_hold · day_divisions)만 사용

매수일의 신호 x(0~1, 전일까지의 정보)에 따라 그날 매수분의 값을 바꾼다.   f = 1 + g × (1 − 2x)
    수량    매수 수량 × f                      (g>0 = 과매도일수록 더 삼)        g = ±0.5 · ±1
    보유일  최대 보유일 × f (최소 2일)          (g>0 = 과매도일수록 오래 들고 감)  g = ±0.5
    분할수  분할수 ÷ f 에 해당 (7 × (1 − g(1−2x)), 4~14)  (g>0 = 과매도일수록 분할 적게 = 크게 삼)  g = ±0.5
신호 x: RSI3 · RSI5 · RSI14 · IBS · σ순위 · 5일 · 20일 수익률순위 (그 종목 자체)

기준: 공격형 / 공격형V (V = 위험 달 12분할 · 공세 보유 5일)
판정: SOXL 전체 · 앞/뒤 절반 · 봉인(2024~) · 4구간 최소 모두 기준 초과 + ~2025년 말
교차: 3배 ETF 16종에 파라미터 그대로 (모드는 같은 QQQ 주봉 RSI, V 는 그 종목 자체 변동성)
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
A2 = SB.A2
from dss_engine import (load_price_data, build_weekly_rsi_series, build_mode_series, DSSParams,
                        run_backtest_fast, get_week_mode_map)

START = "2010-03-12"
TICKERS = SB.TICKERS
PRESET = D1.VR.PRESETS["공격형"]
KEYS = ["Calmar", "H1", "H2", "봉인", "구간최소"]
LEVERS = {"수량": (-1.0, -0.5, 0.5, 1.0), "보유일": (-0.5, 0.5), "분할수": (-0.5, 0.5)}
_D = {}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))


def dicts(tk, base, lever, sig, g):
    T = _D["tk"][tk]
    qm = T["qmode"]
    dv, dh, bm = {}, {}, {}
    risk = T["risk"] if base == "V" else set()
    for d in risk:
        dv[d] = 12
        if qm.get(d) == "AG":
            dh[d] = 5
    if lever:
        x = T["sig"][sig]
        for d, xv in zip(T["idx"], x):
            if xv != xv:
                continue
            f = 1.0 + g * (1.0 - 2.0 * xv)
            if lever == "수량":
                bm[d] = max(0.0, f)
            elif lever == "보유일":
                ag = qm.get(d) == "AG"
                b = (5 if d in risk else PRESET["ag_max_hold"]) if ag else PRESET["sf_max_hold"]
                dh[d] = max(2, int(round(b * f)))
            else:
                b = 12 if d in risk else 7
                dv[d] = int(min(b * 2, max(round(b * 4 / 7), round(b * (2.0 - f)))))
    return dv or None, dh or None, bm or None


def one(tk, a, s, e):
    T = _D["tk"][tk]
    r = run_backtest_fast(DSSParams(**PRESET), T["df"], _D["ms"], s or T["start"], e or T["end"],
                          day_divisions=a[0], day_hold=a[1], day_buy_mult=a[2])
    if not r or r["final_asset"] <= 0:
        return 0.0, -100.0, 0.0
    y = r["total_days"] / 252
    cg = (r["final_asset"] / 1e4) ** (1 / y) - 1
    md = abs(r["mdd"]) or 1e-9
    return cg * 100, -md * 100, cg / md


def evaluate(item):
    base, lever, sig, g = item
    a = dicts("SOXL", base, lever, sig, g)
    f = one("SOXL", a, START, None)
    out = dict(기준=base or "-", 레버=lever or "기준", 신호=sig or "-", g=g, CAGR=f[0], MDD=f[1], Calmar=f[2])
    for k, (s, e) in D1.DSS_WIN.items():
        out[k] = one("SOXL", a, s, e)[2]
    out["구간최소"] = min(one("SOXL", a, s, e)[2] for s, e in D1.DSS_SUBS)
    out["~2025"] = one("SOXL", a, START, "2025-12-31")[2]
    for tk in _D["tk"]:
        if tk != "SOXL":
            r = one(tk, dicts(tk, base, lever, sig, g), None, None)
            out["x_" + tk] = r[2]
            out["m_" + tk] = r[1]
    return out


def main():
    t0 = time.time()
    os.chdir(HERE)
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    tk = {}
    bh = {}
    for t in TICKERS:
        try:
            df = load_price_data(t, "2009-06-01", "2026-12-31")
        except Exception as e:      # noqa: BLE001
            print(f"{t}: 데이터 실패 {str(e)[:60]}")
            continue
        if len(df) < 600:
            continue
        idx = df.index
        st = max(pd.Timestamp(START), idx[30])
        own = df["Close"].astype(float).pct_change().rolling(20).std() * np.sqrt(252)
        if t == "SOXL":
            from dss_engine import load_regime_prices
            own = load_regime_prices().pct_change().rolling(20).std() * np.sqrt(252)
        months = A2.risk_months(own, 0.7)
        sig = {k: v.reindex(idx).values for k, v in SB.signals(df).items()}
        tk[t] = dict(df=df, idx=idx, qmode=get_week_mode_map(ms, idx), risk={d for d in idx if d.to_period("M") in months},
                     sig=sig, start=str(st.date()), end=str(idx[-1].date()))
        c = df["Close"].astype(float)[idx >= st]
        y = len(c) / 252
        cg = (c.iloc[-1] / c.iloc[0]) ** (1 / y) - 1
        md = abs((c / c.cummax() - 1).min())
        bh[t] = (cg / md, -md * 100)
    names = list(tk["SOXL"]["sig"])
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(dict(tk=tk, ms=ms), tmp)
    tmp.close()
    items = []
    for base in ("", "V"):
        items.append((base, None, None, 0.0))
        items += [(base, lv, s, g) for lv, gs in LEVERS.items() for s in names for g in gs]
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        df = pd.DataFrame(pool.map(evaluate, items, chunksize=2))
    os.unlink(tmp.name)
    xs = [c for c in df.columns if c.startswith("x_")]
    f2 = lambda v: f"{v:.2f}"
    out = []
    for base in ("-", "V"):
        sub = df[df["기준"] == base].copy()
        b = sub[sub["레버"] == "기준"].iloc[0]
        rest = sub[sub["레버"] != "기준"].copy()
        rest["통과"] = ["✅" if all(r[k] > b[k] for k in KEYS) else "" for _, r in rest.iterrows()]
        diff = rest[xs].values - b[xs].values.astype(float)
        rest["교차"] = [f"{int((d > 0).sum())}/{len(xs)}" for d in diff]
        rest["교차차이"] = np.median(diff, axis=1)
        name = "공격형" + ("V" if base == "V" else "")
        print(f"\n━━ DSS {name} — 기준 {b['CAGR']:.1f}% / {b['MDD']:.1f}% / {b['Calmar']:.2f} "
              f"(H1 {b['H1']:.2f} H2 {b['H2']:.2f} 봉인 {b['봉인']:.2f} 최소 {b['구간최소']:.2f} ~2025 {b['~2025']:.2f})")
        print("   교차 종목 기준 (Calmar / MDD / 그냥 보유 Calmar): " + " · ".join(
            f"{c[2:]} {b[c]:.2f}/{b['m_' + c[2:]]:.0f}%/{bh[c[2:]][0]:.2f}" for c in xs))
        bx = b[xs].values.astype(float)
        print(f"   → Calmar 중앙값 {np.median(bx):.2f} · 그냥 보유보다 나은 종목 {sum(b[c] > bh[c[2:]][0] for c in xs)}/{len(xs)}"
              f" · MDD 중앙값 {np.median([b['m_' + c[2:]] for c in xs]):.1f}% · MDD −80% 아래 {sum(b['m_' + c[2:]] < -80 for c in xs)}종목")
        cols = ["레버", "신호", "g", "CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소", "~2025", "통과", "교차", "교차차이"]
        for lv in LEVERS:
            s = rest[rest["레버"] == lv]
            print(f"   [{lv}] 전 항목 통과 {(s['통과'] == '✅').sum()}/{len(s)}")
            print("     " + s[cols].to_string(index=False, float_format=f2).replace("\n", "\n     "))
        rest.insert(0, "프리셋", name)
        out.append(rest)
    pd.concat(out).to_csv(os.path.join(HERE, "reports", "dss_norm_stage1.csv"), index=False, encoding="utf-8-sig")
    print(f"\n{len(df)} 조합, {time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
