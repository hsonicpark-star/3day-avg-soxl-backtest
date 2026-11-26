"""
DSS 공격형V — 평소 분할수 × 위험 달 분할수 (2026-11-26)

dss_m_deep_2026-11.py 에서 'V + 평소 6분할' 이 모멘텀 분할(M)과 같은 성과를 냈다 → 상수만 바꾼 쪽을 따로 확인.
평소 분할수 5 · 6 · 7 · 8 × 위험 달 분할수 10 · 12 · 14 (공세 보유 5일은 그대로)
"""
import sys, os, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import numpy as np
import pandas as pd
import importlib.util

_sp = importlib.util.spec_from_file_location("dm", os.path.join(HERE, "reports", "dss_m_deep_2026-11.py"))
DM = importlib.util.module_from_spec(_sp)
_sp.loader.exec_module(DM)
from dss_engine import (load_price_data, build_weekly_rsi_series, build_mode_series, DSSParams, run_backtest,
                        run_backtest_fast, get_week_mode_map, load_regime_prices)

START, D1, KEYS = DM.START, DM.D1, DM.KEYS


def main():
    os.chdir(HERE)
    df = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    idx = df.index
    months = DM.A2.risk_months(load_regime_prices().pct_change().rolling(20).std() * np.sqrt(252), 0.7)
    risk = {d for d in idx if d.to_period("M") in months}
    qm = get_week_mode_map(ms, idx)
    end = str(idx[-1].date())

    def dicts(base, rdiv):
        dv, dh = {}, {}
        for d in idx:
            if d in risk:
                dv[d] = rdiv
                if qm.get(d) == "AG":
                    dh[d] = 5
            elif base != 7:
                dv[d] = base
        return dv, dh

    def one(a, s, e):
        r = run_backtest_fast(DSSParams(**DM.PRESET), df, ms, s, e or end, day_divisions=a[0], day_hold=a[1])
        y = r["total_days"] / 252
        cg = (r["final_asset"] / 1e4) ** (1 / y) - 1
        return cg * 100, -abs(r["mdd"]) * 100, cg / abs(r["mdd"])

    rows = []
    for base in (5, 6, 7, 8):
        for rdiv in (10, 12, 14):
            a = dicts(base, rdiv)
            f = one(a, START, None)
            r = dict(평소=base, 위험달=rdiv, CAGR=f[0], MDD=f[1], Calmar=f[2])
            for k, (s, e) in D1.DSS_WIN.items():
                r[k] = one(a, s, e)[2]
            r["구간최소"] = min(one(a, s, e)[2] for s, e in D1.DSS_SUBS)
            r["~2025"] = one(a, START, "2025-12-31")[2]
            # 추가 지표
            bt = run_backtest(DSSParams(**DM.PRESET), df, ms, START, end, day_divisions=a[0], day_hold=a[1])
            eq = pd.Series(bt["총자산"].values.astype(float), index=pd.to_datetime(bt["날짜"]))
            ret = eq.pct_change().dropna()
            dd = eq / eq.cummax() - 1
            s_, tops = dd.copy(), []
            for _ in range(5):
                t = s_.idxmin(); pk = eq[:t].idxmax()
                rec = dd[t:][dd[t:] > -1e-9]
                en = rec.index[0] if len(rec) else dd.index[-1]
                tops.append(dd[t] * 100); s_[pk:en] = 0
            yr = eq.groupby(eq.index.year).last().pct_change().dropna() * 100
            r.update(샤프=ret.mean() / ret.std() * np.sqrt(252), 큰낙폭5평균=float(np.mean(tops)), 최악의해=float(yr.min()),
                     평균노출=float((pd.Series(bt["평가금"].values.astype(float), index=eq.index) / eq).mean() * 100))
            rows.append(r)
    res = pd.DataFrame(rows)
    b = res[(res["평소"] == 7) & (res["위험달"] == 12)].iloc[0]
    res[">V"] = ["✅" if all(r[k] > b[k] for k in KEYS) else "" for _, r in res.iterrows()]
    print(res.to_string(index=False, float_format=lambda v: f"{v:.2f}"))
    res.to_csv(os.path.join(HERE, "reports", "dss_v_basediv.csv"), index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
