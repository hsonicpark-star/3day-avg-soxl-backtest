"""
M1 — 거시 레짐 (금리 · 신용 · 달러) × DSS · 표준편차매매 (2026-11-25) — 레짐_카탈로그.md 의 M1

데이터: FRED (T10Y2Y · T10Y3M · DGS10 · BAA10Y · DTWEXBGS) + 야후 (HYG · IEF)
        ※ 하이일드 OAS(BAMLH0A0HYM2)는 FRED 가 최근 3년치만 제공 → Baa−10년 스프레드와 HYG÷IEF 로 대체
규칙은 배포된 V 그대로 (DSS 12분할 · 공세 5일 / 표준편차 매도 100% · 상한 50%), 적용 날짜만 바꾼다.
월 = 월말 판정 → 다음 달, 일 = 전일 판정 → 오늘

금리  YC2 / YC3     10년−2년 / 10년−3개월 < 0 (역전)
      RSTEEP        최근 252일 안에 역전이 있었고 지금은 정상화 (역전 해소 구간)
      RUP           10년 금리 63일 변화 ≥ +0.5%p (급등)
신용  CRP70         Baa−10년 스프레드를 V1 절차에 (상승 & 756일 백분위 ≥ 70%)
      CRMA          Baa−10년 스프레드 > 200일 평균 (확대 국면)
      HYGP70        −ln(HYG÷IEF) 를 V1 절차에
      HYGMA         HYG÷IEF < 200일 평균
달러  DXMA          달러지수(광의) > 200일 평균
      DXUP          달러지수 63일 변화 ≥ +3%
      DXP70         달러지수를 V1 절차에
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


X1 = _load("x1", "x1_vix_regime_2026-11.py")
D1, A2 = X1.D1, X1.A2
from dss_engine import load_price_data, build_weekly_rsi_series, build_mode_series, get_week_mode_map


def init_worker(pkl):
    with open(pkl, "rb") as f:
        X1._D.update(pickle.load(f))


def dss_all(mask):
    return X1.dss_all(mask)


def fred(sid, last):
    d = pd.read_csv(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}")
    d.columns = ["d", "v"]
    s = pd.Series(pd.to_numeric(d["v"], errors="coerce").values, index=pd.to_datetime(d["d"])).dropna()
    return s[(s.index >= "2005-01-01") & (s.index <= last)]


def yclose(tk, last):
    df = yf.download(tk, start="2006-01-01", end="2027-01-01", auto_adjust=True, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    s = df["Close"].astype(float).dropna()
    s.index = pd.to_datetime(s.index)
    return s[s.index <= last]


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

    yc2, yc3, y10 = fred("T10Y2Y", last), fred("T10Y3M", last), fred("DGS10", last)
    baa, dxy = fred("BAA10Y", last), fred("DTWEXBGS", last)
    hyg, ief = yclose("HYG", last), yclose("IEF", last)
    cr = (hyg / ief).dropna()
    crs = -np.log(cr)
    print(f"\n현재: 10년−2년 {yc2.iloc[-1]:+.2f} · 10년−3개월 {yc3.iloc[-1]:+.2f} · 10년 금리 {y10.iloc[-1]:.2f}% (63일 {y10.iloc[-1]-y10.iloc[-64]:+.2f}%p)"
          f" · Baa 스프레드 {baa.iloc[-1]:.2f} (200일 평균 {baa.rolling(200).mean().iloc[-1]:.2f}) · 달러 {dxy.iloc[-1]:.1f} (200일 평균 {dxy.rolling(200).mean().iloc[-1]:.1f})")
    from dss_engine import load_regime_prices
    soxx = load_regime_prices()
    sd20 = (soxx.pct_change().rolling(20).std() * np.sqrt(252)).dropna()
    j = pd.concat([sd20, baa, crs, dxy, yc2], axis=1, keys=["sd", "baa", "hyg", "dxy", "yc"]).dropna()
    print("SOXX 20일 변동성과의 일별 상관: " + " · ".join(f"{k} {j['sd'].corr(j[k]):+.2f}" for k in ("baa", "hyg", "dxy", "yc")))

    inv = yc2 < 0
    rsteep = (yc2 >= 0) & (inv.astype(float).rolling(252).max() == 1.0)
    states_m = {"YC2 월": inv, "YC3 월": yc3 < 0, "RSTEEP 월": rsteep, "RUP 월": (y10 - y10.shift(63)) >= 0.5,
                "CRMA 월": baa > baa.rolling(200).mean(), "HYGMA 월": cr < cr.rolling(200).mean(),
                "DXMA 월": dxy > dxy.rolling(200).mean(), "DXUP 월": (dxy / dxy.shift(63) - 1) >= 0.03}
    states_d = {"RUP 일": (y10 - y10.shift(63)) >= 0.5, "CRMA 일": baa > baa.rolling(200).mean(),
                "HYGMA 일": cr < cr.rolling(200).mean(), "DXMA 일": dxy > dxy.rolling(200).mean()}
    proc = {"CRP70 월": baa, "HYGP70 월": crs, "DXP70 월": dxy}

    def variants(idx):
        m = {k: X1.month_mask_from_state(v, idx) for k, v in states_m.items()}
        m.update({k: X1.day_mask(v, idx) for k, v in states_d.items()})
        m.update({k: A2.month_mask(idx, A2.risk_months(v, 0.7)) for k, v in proc.items()})
        order = ["YC2 월", "YC3 월", "RSTEEP 월", "RUP 월", "RUP 일", "CRP70 월", "CRMA 월", "CRMA 일",
                 "HYGP70 월", "HYGMA 월", "HYGMA 일", "DXMA 월", "DXMA 일", "DXUP 월", "DXP70 월"]
        return {k: m[k] for k in order}

    sd_m, dss_m = variants(sd_idx), variants(dss_idx)
    dss_v1 = A2.month_mask(dss_idx, {d.to_period("M") for d, r in zip(sd_idx, sd_v1) if r})
    names = list(sd_m)
    yrs = {nm: sorted({d.year for d, on in zip(sd_idx, sd_m[nm]) if on}) for nm in names}
    print("적용 연도: " + " | ".join(f"{nm}: {len(y)}개 연도" for nm, y in yrs.items()))

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
    out.append(X1.report("DSS 공격형 (12분할 · 공세 5일)", names, dss_m, dss_v1, dres, dss_idx))
    for lab in ("균형형", "공격형"):
        a = D["arr"][lab]
        sres = {k: X1.sd_all(a, sd_idx, m) for k, m in jobs(sd_m, sd_v1)}
        out.append(X1.report(f"표준편차 {lab} (매도 100% · 상한 50%)", names, sd_m, sd_v1, sres, sd_idx))
    pd.concat(out).to_csv(os.path.join(HERE, "reports", "m1_macro_regime.csv"), index=False, encoding="utf-8-sig")
    print(f"\n{time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
