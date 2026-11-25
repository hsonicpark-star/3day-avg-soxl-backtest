"""
X1 — VIX 레짐 × DSS · 표준편차매매 (2026-11-25) — 레짐_카탈로그.md 의 X1

VIX(S&P500 옵션 내재변동성)로 위험 구간을 정하고, 배포된 V 규칙을 그 구간에 적용해 V1(SOXX 실현변동성)과 비교한다.
  규칙: DSS 공격형 = 12분할 · 공세 보유 5일 / 표준편차 = 매도 100% · 비중 50% 상한

변형 (월 = 월말 판정 → 다음 달, 일 = 전일 판정 → 오늘)
  L60/70/80  VIX 를 V1 절차에 그대로 (월말 VIX 상승 & 756일 백분위 ≥ 기준)
  A25/A30    VIX 절대 수준 ≥ 25 / 30                         (월 · 일)
  T95/T100   기간구조 VIX ÷ VIX3M ≥ 0.95 / 1.00 (역전)        (월 · 일)
  TP70       VIX ÷ VIX3M 을 V1 절차에 (상승 & 백분위 ≥ 70%)   (월)
  N100       단기 역전 VIX9D ÷ VIX ≥ 1.00                    (월 · 일, 2011~)

각 변형에 대해: 단독 / V1 과 합집합(V 위에 추가) / X1 만 걸리는 날(X − V1)
통과 = 전체 · 앞/뒤 절반 · 봉인(2024~) · 4구간 최소 모두 비교 대상 초과
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
import yfinance as yf


def _load(name, fn):
    sp = importlib.util.spec_from_file_location(name, os.path.join(HERE, "reports", fn))
    m = importlib.util.module_from_spec(sp)
    sp.loader.exec_module(m)
    return m


D1 = _load("d1", "d1_drawdown_regime_2026-11.py")
A2 = _load("a2", "a2_tr_regime_2026-11.py")
from dss_engine import (load_price_data, build_weekly_rsi_series, build_mode_series, DSSParams,
                        run_backtest_fast, get_week_mode_map)

START = D1.START
SD_RULE = (1.0, 100.0, 0.5)
KEYS = ["Calmar", "H1", "H2", "봉인", "구간최소"]
_D = {}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))


def fetch(tk, last):
    df = yf.download(tk, start="2006-01-01", end="2027-01-01", auto_adjust=False, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    s = df["Close"].astype(float).dropna()
    s.index = pd.to_datetime(s.index)
    return s[s.index <= last]


def day_mask(state, idx):
    """일별 bool Series → 전일 판정으로 거래일 배열."""
    s = state.astype(float).shift(1).dropna()
    return s.reindex(idx, method="ffill").fillna(0.0).values == 1.0


def month_mask_from_state(state, idx):
    """월말 상태 → 다음 달."""
    ends = state.dropna().groupby(state.dropna().index.to_period("M")).tail(1)
    months = {d.to_period("M") + 1 for d, v in ends.items() if v}
    return A2.month_mask(idx, months)


def sd_all(a, idx, mask):
    use = mask is not None and mask.any()
    n = len(idx)
    arrs = D1.sd_arrays(n, None, mask if use else None, SD_RULE if use else None)

    def m(s, e):
        i0 = idx.searchsorted(pd.Timestamp(s))
        i1 = n if e is None else idx.searchsorted(pd.Timestamp(e))
        eq = D1.sd_sim(a["c"][i0:i1], a["pc"][i0:i1], a["sg"][i0:i1], a["p"], *[x[i0:i1] for x in arrs])
        y = (idx[i1 - 1] - idx[i0]).days / 365.25
        cg = (eq[-1] / 1e4) ** (1 / y) - 1
        md = abs((eq / np.maximum.accumulate(eq) - 1).min())
        return cg * 100, -md * 100, cg / md
    f = m(START, None)
    return dict(CAGR=f[0], MDD=f[1], Calmar=f[2], **{k: m(s, e)[2] for k, (s, e) in D1.SD_WIN.items()},
                구간최소=min(m(s, e)[2] for s, e in D1.SD_SUBS), **{"~2025": m(START, "2026-01-01")[2]})


def dss_one(mask, s, e):
    qm = _D["qmode"]
    dv, dh = {}, {}
    for d, on in zip(_D["dss_idx"], mask):
        if on:
            dv[d] = 12
            if qm.get(d) == "AG":
                dh[d] = 5
    r = run_backtest_fast(DSSParams(**D1.VR.PRESETS["공격형"]), _D["soxl"], _D["ms"], s, e or _D["end"],
                          day_divisions=dv or None, day_hold=dh or None)
    y = r["total_days"] / 252
    cg = (r["final_asset"] / 1e4) ** (1 / y) - 1
    return cg * 100, -abs(r["mdd"]) * 100, cg / abs(r["mdd"])


def dss_all(mask):
    f = dss_one(mask, START, None)
    return dict(CAGR=f[0], MDD=f[1], Calmar=f[2], **{k: dss_one(mask, s, e)[2] for k, (s, e) in D1.DSS_WIN.items()},
                구간최소=min(dss_one(mask, s, e)[2] for s, e in D1.DSS_SUBS), **{"~2025": dss_one(mask, START, "2025-12-31")[2]})


def report(title, names, masks, v1, res, days_ref):
    """res: {(name, mode): metrics}. mode ∈ 단독 / 합 / X만."""
    base, rv = res[("기준", "단독")], res[("V1", "단독")]
    f = lambda r: f"{r['CAGR']:5.1f}% / {r['MDD']:6.1f}% / {r['Calmar']:.2f} | H1 {r['H1']:.2f} H2 {r['H2']:.2f} 봉인 {r['봉인']:.2f} 최소 {r['구간최소']:.2f} ~2025 {r['~2025']:.2f}"
    print(f"\n━━ {title}")
    print(f"   기준                {f(base)}")
    print(f"   V1 (변동성 레짐)    {f(rv)}")
    rows = []
    for nm in names:
        a, u, x = res[(nm, "단독")], res[(nm, "합")], res[(nm, "X만")]
        p1 = all(a[k] > base[k] for k in KEYS)
        p2 = all(a[k] > rv[k] for k in KEYS)
        p3 = all(u[k] > rv[k] for k in KEYS)
        rows.append({"변형": nm, "적용일%": masks[nm].mean() * 100, "V1겹침%": (masks[nm] & v1).sum() / max(1, masks[nm].sum()) * 100,
                     "CAGR": a["CAGR"], "MDD": a["MDD"], "Calmar": a["Calmar"], "H1": a["H1"], "H2": a["H2"], "봉인": a["봉인"],
                     "최소": a["구간최소"], "~2025": a["~2025"], ">기준": "✅" if p1 else "", ">V1": "✅" if p2 else "",
                     "V1∪X": u["Calmar"], "∪~2025": u["~2025"], "∪>V1": "✅" if p3 else "", "X만": x["Calmar"]})
    df = pd.DataFrame(rows)
    print(df.to_string(index=False, float_format=lambda v: f"{v:.2f}"))
    df.insert(0, "전략", title)
    return df


def main():
    t0 = time.time()
    import update_preset_stats as U
    os.chdir(HERE)
    _px = U.load_adjusted("SOXL")
    U.load_adjusted = lambda tk, *a, **k: _px
    D = D1.S1.prepare()
    sd_idx, sd_v1 = D["idx"], D["risk"][0.7]
    soxl = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    dss_idx = soxl.index
    last = sd_idx[-1]

    vix, vix3m, vix9d = fetch("^VIX", last), fetch("^VIX3M", last), fetch("^VIX9D", last)
    ts = (vix / vix3m).dropna()
    ns = (vix9d / vix).dropna()
    print(f"\nVIX {vix.index[0].date()}~{vix.index[-1].date()} (현재 {vix.iloc[-1]:.1f}) · VIX3M {vix3m.index[0].date()}~ (현재 비율 {ts.iloc[-1]:.2f})"
          f" · VIX9D {vix9d.index[0].date()}~ (현재 비율 {ns.iloc[-1]:.2f})")
    from dss_engine import load_regime_prices
    soxx = load_regime_prices()
    sd20 = (soxx.pct_change().rolling(20).std() * np.sqrt(252)).dropna()
    j = pd.concat([sd20, vix, ts], axis=1, keys=["sd", "vix", "ts"]).dropna()
    print(f"일별 상관: SOXX 20일 변동성 vs VIX {j['sd'].corr(j['vix']):.2f} · vs VIX÷VIX3M {j['sd'].corr(j['ts']):.2f}")

    def variants(idx):
        m = {}
        for thr in (0.6, 0.7, 0.8):
            m[f"L{int(thr*100)} 월"] = A2.month_mask(idx, A2.risk_months(vix, thr))
        for lv in (25, 30):
            m[f"A{lv} 월"] = month_mask_from_state(vix >= lv, idx)
            m[f"A{lv} 일"] = day_mask(vix >= lv, idx)
        for r_, nm in ((0.95, "T95"), (1.0, "T100")):
            m[f"{nm} 월"] = month_mask_from_state(ts >= r_, idx)
            m[f"{nm} 일"] = day_mask(ts >= r_, idx)
        m["TP70 월"] = A2.month_mask(idx, A2.risk_months(ts, 0.7))
        m["N100 월"] = month_mask_from_state(ns >= 1.0, idx)
        m["N100 일"] = day_mask(ns >= 1.0, idx)
        return m

    sd_m, dss_m = variants(sd_idx), variants(dss_idx)
    dss_v1 = A2.month_mask(dss_idx, {d.to_period("M") for d, r in zip(sd_idx, sd_v1) if r})
    names = list(sd_m)

    def jobs(masks, v1):
        out = [(("기준", "단독"), np.zeros(len(v1), bool)), (("V1", "단독"), v1)]
        for nm in names:
            out += [((nm, "단독"), masks[nm]), ((nm, "합"), masks[nm] | v1), ((nm, "X만"), masks[nm] & ~v1)]
        return out

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(dict(soxl=soxl, ms=ms, qmode=get_week_mode_map(ms, dss_idx), dss_idx=dss_idx, end=str(dss_idx[-1].date())), tmp)
    tmp.close()
    out = []
    dj = jobs(dss_m, dss_v1)
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        dres = dict(zip([k for k, _ in dj], pool.map(dss_all, [m for _, m in dj], chunksize=2)))
    os.unlink(tmp.name)
    out.append(report("DSS 공격형 (12분할 · 공세 5일)", names, dss_m, dss_v1, dres, dss_idx))
    for lab in ("균형형", "공격형"):
        a = D["arr"][lab]
        sres = {k: sd_all(a, sd_idx, m) for k, m in jobs(sd_m, sd_v1)}
        out.append(report(f"표준편차 {lab} (매도 100% · 상한 50%)", names, sd_m, sd_v1, sres, sd_idx))
    pd.concat(out).to_csv(os.path.join(HERE, "reports", "x1_vix_regime.csv"), index=False, encoding="utf-8-sig")
    print(f"\n{time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
