"""
A2 — TR(ATR) 변동성 레짐 × DSS · 표준편차매매 (2026-11-25) — 레짐_카탈로그.md 의 A2

V1 의 '20일 수익률 표준편차' 자리에 ATR% 를 넣고 나머지는 그대로 둔다.
  TR     = max(고가−저가, |고가−전일종가|, |저가−전일종가|)      (SOXX, 무조정)
  ATR%   = N일 평균 TR ÷ 종가                                   (N = 14 · 20 · 30)
  백분위 = 최근 756거래일 중 오늘 값보다 낮은 날의 비율
  위험   = 월말 ATR% 가 직전 월말보다 상승 & 백분위 ≥ 기준(60 · 70 · 80%) → 다음 달

규칙은 배포된 V 그대로: DSS 공격형 = 위험 달 12분할 · 공세 보유 5일 / 표준편차 = 매도 100% · 비중 50% 상한
비교: 기준 · V1 · A2(9종) · V1∪A2 · V1∩A2 · A2만 · V1만 — 전체 · 앞/뒤 절반 · 봉인 · 4구간 최소 · ~2025년 말
      + 무작위 달 300회 (A2 20일·70%)
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
SD_RULE = (1.0, 100.0, 0.5)
N_RAND = 300
_D = {}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))


def risk_months(vol, thr, pct_win=756):
    """V1 과 같은 판정 절차 — vol(일별 변동성 지표) → 위험 적용 월 집합."""
    vol = vol.dropna()
    v = vol.values
    pct = pd.Series([np.mean(v[max(0, i - pct_win + 1): i + 1] < v[i]) for i in range(len(v))], index=vol.index)
    ends = vol.groupby(vol.index.to_period("M")).tail(1)
    out, prev = set(), None
    for d, val in ends.items():
        if prev is not None and val > prev and pct[d] >= thr:
            out.add(d.to_period("M") + 1)
        prev = val
    return out


def month_mask(idx, months):
    return np.array([d.to_period("M") in months for d in idx])


def sd_all(a, idx, months):
    mask = month_mask(idx, months) if months else None
    n = len(idx)
    arrs = D1.sd_arrays(n, None, mask, SD_RULE if mask is not None else None)

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


def dss_one(months, s, e):
    qm = _D["qmode"]
    dv, dh = {}, {}
    for d in _D["dss_idx"]:
        if d.to_period("M") in months:
            dv[d] = 12
            if qm.get(d) == "AG":
                dh[d] = 5
    r = run_backtest_fast(DSSParams(**D1.VR.PRESETS["공격형"]), _D["soxl"], _D["ms"], s, e or _D["end"],
                          day_divisions=dv or None, day_hold=dh or None)
    y = r["total_days"] / 252
    cg = (r["final_asset"] / 1e4) ** (1 / y) - 1
    return cg * 100, -abs(r["mdd"]) * 100, cg / abs(r["mdd"])


def dss_all(months):
    f = dss_one(months, START, None)
    return dict(CAGR=f[0], MDD=f[1], Calmar=f[2], **{k: dss_one(months, s, e)[2] for k, (s, e) in D1.DSS_WIN.items()},
                구간최소=min(dss_one(months, s, e)[2] for s, e in D1.DSS_SUBS), **{"~2025": dss_one(months, START, "2025-12-31")[2]})


def dss_full(months):
    return dss_one(months, START, None)


def main():
    t0 = time.time()
    import update_preset_stats as U
    os.chdir(HERE)
    close = load_regime_prices()
    ox = load_price_data("SOXX", "2006-12-01", "2026-12-31")
    h, l, c = ox["High"].astype(float), ox["Low"].astype(float), ox["Close"].astype(float)
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    sd20 = close.pct_change().rolling(20).std() * np.sqrt(252)

    _px = U.load_adjusted("SOXL")
    U.load_adjusted = lambda tk, *a, **k: _px
    D = D1.S1.prepare()
    idx = D["idx"]
    all_m = sorted({d.to_period("M") for d in idx})
    v1 = {d.to_period("M") for d, r in zip(idx, D["risk"][0.7]) if r}
    chk = risk_months(sd20, 0.7) & set(all_m)
    print(f"\n판정 절차 확인 — 표준편차로 다시 계산한 위험 달 {len(chk)} = V1 {len(v1)} (차이 {len(chk ^ v1)})")

    sets = {"기준": set(), "V1 (표준편차 20일·70%)": v1}
    a2 = {}
    for n in (14, 20, 30):
        atr = tr.rolling(n).mean() / c
        for thr in (0.6, 0.7, 0.8):
            a2[(n, thr)] = risk_months(atr, thr) & set(all_m)
            sets[f"A2 ATR{n}·{int(thr*100)}%"] = a2[(n, thr)]
    A = a2[(20, 0.7)]
    sets.update({"V1 ∪ A2": v1 | A, "V1 ∩ A2": v1 & A, "A2 만 (A2 − V1)": A - v1, "V1 만 (V1 − A2)": v1 - A})
    print(f"달 수: 전체 {len(all_m)} · V1 {len(v1)} · A2(20일·70%) {len(A)} · 겹침 {len(v1 & A)} · A2 만 {len(A - v1)} · V1 만 {len(v1 - A)}")
    print("A2 만:", " ".join(str(m) for m in sorted(A - v1)))
    print("V1 만:", " ".join(str(m) for m in sorted(v1 - A)))
    corr = pd.concat([sd20, tr.rolling(20).mean() / c], axis=1).dropna().corr().iloc[0, 1]
    print(f"일별 상관 (20일 표준편차 vs 20일 ATR%): {corr:.3f}")

    soxl = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(dict(soxl=soxl, ms=ms, qmode=get_week_mode_map(ms, soxl.index), dss_idx=soxl.index,
                     end=str(soxl.index[-1].date())), tmp)
    tmp.close()
    rng = np.random.default_rng(20261125)
    rand_sets = [set(rng.choice(all_m, size=len(A), replace=False)) for _ in range(N_RAND)]
    rows = []
    f2 = lambda v: f"{v:.2f}"
    cols = ["레짐", "달수", "CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소", "~2025"]
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        dres = pool.map(dss_all, list(sets.values()))
        drand = pool.map(dss_full, rand_sets, chunksize=8)
    df = pd.DataFrame([dict(전략="DSS 공격형", 레짐=k, 달수=len(v), **r) for (k, v), r in zip(sets.items(), dres)])
    rows.append(df)
    print("\n━━ DSS 공격형 (위험 달 12분할 · 공세 보유 5일)")
    print(df[cols].to_string(index=False, float_format=f2))
    cal = np.array([x[2] for x in drand])
    cA = df[df["레짐"] == "A2 ATR20·70%"]["Calmar"].iloc[0]
    cV = df[df["레짐"].str.startswith("V1 (")]["Calmar"].iloc[0]
    print(f"   무작위 {len(A)}개월 300회: 중앙값 {np.median(cal):.2f} · A2 상위 {(cal >= cA).mean()*100:.1f}% · V1 상위 {(cal >= cV).mean()*100:.1f}%")
    for lab in ("균형형", "공격형"):
        a = D["arr"][lab]
        df = pd.DataFrame([dict(전략=f"표준편차 {lab}", 레짐=k, 달수=len(v), **sd_all(a, idx, v)) for k, v in sets.items()])
        rows.append(df)
        print(f"\n━━ 표준편차 {lab} (위험 달 매도 100% · 비중 50% 상한)")
        print(df[cols].to_string(index=False, float_format=f2))
        cal = np.array([sd_all_full(a, idx, s) for s in rand_sets])
        cA = df[df["레짐"] == "A2 ATR20·70%"]["Calmar"].iloc[0]
        cV = df[df["레짐"].str.startswith("V1 (")]["Calmar"].iloc[0]
        print(f"   무작위 {len(A)}개월 300회: 중앙값 {np.median(cal):.2f} · A2 상위 {(cal >= cA).mean()*100:.1f}% · V1 상위 {(cal >= cV).mean()*100:.1f}%")
    os.unlink(tmp.name)
    pd.concat(rows).to_csv(os.path.join(HERE, "reports", "a2_tr_regime.csv"), index=False, encoding="utf-8-sig")
    print(f"\n{time.time() - t0:.0f}초")


def sd_all_full(a, idx, months):
    n = len(idx)
    arrs = D1.sd_arrays(n, None, month_mask(idx, months), SD_RULE)
    eq = D1.sd_sim(a["c"], a["pc"], a["sg"], a["p"], *arrs)
    y = (idx[-1] - idx[0]).days / 365.25
    return ((eq[-1] / 1e4) ** (1 / y) - 1) / abs((eq / np.maximum.accumulate(eq) - 1).min())


if __name__ == "__main__":
    main()
