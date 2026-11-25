"""
변동성 레짐 사전 점검 — 레짐별 일평균 수익 프로파일 (2026-11-25)

레짐 규칙을 전략에 붙이기 전에 '위험 달에 이 전략이 버는가, 잃는가'부터 본다 (가이드 8절).
대상: 표준편차매매 SOXL 프리셋 5종 + 비교용 DSS 공격형(효과 있었음) · 듀얼스나이퍼 로케트셋(효과 없었음).

출력
  ① 레짐별: 일수 · 일평균 수익 · 일변동 · 평균 노출 · 최악의 하루 · 월평균 수익 · 손실 달 비율
  ② 간이 추정: 위험 달의 일수익을 ×0.6 으로 줄였을 때 CAGR / MDD / Calmar (경로 의존 무시한 근사)
  ③ 낙폭 25% 이상 구간: 고점·저점 날짜, 고점 달 레짐, '첫 위험 판정일 전에 이미 빠진 폭'
"""
import sys, os, ast, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "scripts"))
import numpy as np
import pandas as pd

from dss_engine import vol_regime_table, load_regime_prices
START = "2010-03-12"


def regime_map(risk_pct=0.7):
    t = vol_regime_table(load_regime_prices(), risk_pct)
    return dict(zip(t["적용월"], t["레짐"]))


def profile(name, eq, expo, reg):
    eq = eq.astype(float)
    r = eq.pct_change().fillna(0.0)
    rg = pd.Series([reg.get(d.to_period("M"), "미판정") for d in eq.index], index=eq.index)
    y = len(eq) / 252
    cagr = (eq.iloc[-1] / eq.iloc[0]) ** (1 / y) - 1
    dd = eq / eq.cummax() - 1
    mdd = dd.min()
    print(f"\n━━ {name} — CAGR {cagr*100:.1f}% / MDD {mdd*100:.1f}% / Calmar {cagr/abs(mdd):.2f} "
          f"({eq.index[0].date()} ~ {eq.index[-1].date()})")
    mon = (1 + r).groupby(eq.index.to_period("M")).prod() - 1
    mreg = pd.Series({m: reg.get(m, "미판정") for m in mon.index})
    rows = []
    tot_log = np.log1p(r).sum()
    for k in ("위험", "승부", "안정"):
        m = rg == k
        if not m.any():
            continue
        mm = mon[mreg == k]
        rows.append({"레짐": k, "일수": int(m.sum()), "일평균(%)": r[m].mean() * 100, "일변동(%)": r[m].std() * 100,
                     "일샤프": r[m].mean() / r[m].std() if r[m].std() > 0 else 0,
                     "평균노출(%)": expo[m].mean() * 100 if expo is not None else np.nan,
                     "최악하루(%)": r[m].min() * 100, "월평균(%)": mm.mean() * 100,
                     "손실달(%)": (mm < 0).mean() * 100, "최악달(%)": mm.min() * 100,
                     "수익기여(%)": np.log1p(r[m]).sum() / tot_log * 100})
    print(pd.DataFrame(rows).to_string(index=False, float_format=lambda v: f"{v:.2f}"))
    # 간이 추정: 위험 달 일수익 ×0.6
    r2 = r.where(rg != "위험", r * 0.6)
    e2 = (1 + r2).cumprod()
    c2 = e2.iloc[-1] ** (1 / y) - 1
    m2 = (e2 / e2.cummax() - 1).min()
    print(f"   간이 추정 (위험 달 일수익 ×0.6): CAGR {c2*100:.1f}% / MDD {m2*100:.1f}% / Calmar {c2/abs(m2):.2f}"
          f"  (기준 {cagr/abs(mdd):.2f} → {'+' if c2/abs(m2) > cagr/abs(mdd) else ''}{c2/abs(m2) - cagr/abs(mdd):.2f})")
    # 낙폭 구간
    out, in_dd, pk = [], False, None
    for d, v in dd.items():
        if not in_dd and v < -0.25:
            in_dd, pk = True, eq[:d].idxmax()
        if in_dd and v > -1e-9:
            seg = dd[pk:d]
            out.append((pk, seg.idxmin(), seg.min()))
            in_dd = False
    if in_dd:
        seg = dd[pk:]
        out.append((pk, seg.idxmin(), seg.min()))
    for pk, tr, v in out:
        seg_r = rg[pk:tr]
        first_risk = seg_r[seg_r == "위험"].index
        before = dd[first_risk[0]] if len(first_risk) and first_risk[0] > pk else (0.0 if len(first_risk) else v)
        # 첫 위험일 '직전' 낙폭
        if len(first_risk) and first_risk[0] > pk:
            before = dd[:first_risk[0]].iloc[-2] if len(dd[:first_risk[0]]) > 1 else 0.0
        print(f"   낙폭 {v*100:6.1f}%  고점 {pk.date()} ({reg.get(pk.to_period('M'), '-')}) → 저점 {tr.date()} "
              f"({reg.get(tr.to_period('M'), '-')}) | 위험 판정 전 이미 {before*100:.1f}% | 구간 내 위험일 {int((seg_r == '위험').sum())}/{len(seg_r)}")
    return dict(전략=name, CAGR=cagr * 100, MDD=mdd * 100, Calmar=cagr / abs(mdd),
                위험_일평균=r[rg == "위험"].mean() * 100, 승부_일평균=r[rg == "승부"].mean() * 100,
                안정_일평균=r[rg == "안정"].mean() * 100,
                위험_일샤프=r[rg == "위험"].mean() / r[rg == "위험"].std(),
                그외_일샤프=r[rg != "위험"].mean() / r[rg != "위험"].std(),
                간이추정_Calmar=c2 / abs(m2))


def main():
    reg = regime_map(0.7)
    summary = []

    # ── 표준편차매매 ──
    import stdev_engine as E
    from update_preset_stats import load_adjusted, extract_literal
    os.chdir(HERE)
    db = extract_literal("strategies/stdev.py", "_SD_PRESETS_DB")
    px = load_adjusted("SOXL")[["Close"]]
    for p in db["SOXL"]:
        r = E.run_backtest_stdev(px, START, str(px.index[-1].date()),
                                 sigma_period=p["sigma_period"], k_buy=p["k_buy"], k_sell=p["k_sell"],
                                 sell_ratio=p["sell_ratio"], divisions=p["divisions"], renewal=p["renewal"],
                                 pcr=1.0, lcr=1.0, initial_capital=10000.0, return_history=False)
        eq = pd.Series(r["assets"], index=pd.to_datetime(r["dates"])).dropna()
        cash = pd.Series(r["cash_series"], index=pd.to_datetime(r["dates"])).reindex(eq.index)
        summary.append(profile(f"표준편차 {p['label']} ({p['divisions']}분할)", eq, 1 - cash / eq, reg))

    # ── 비교: DSS 공격형 ──
    from dss_engine import (load_price_data, build_weekly_rsi_series, build_mode_series, DSSParams, run_backtest)
    soxl = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    dp = DSSParams(sf_divisions=7, ag_divisions=7, sf_max_hold=38, sf_buy_pct=0.033, sf_sell_pct=0.018,
                   ag_max_hold=8, ag_buy_pct=0.0315, ag_sell_pct=0.0375, initial_capital=10000.0,
                   fee_rate=0.0004, renewal_period=10, pcr=0.75, lcr=0.20)
    bt = run_backtest(dp, soxl, ms, START, str(soxl.index[-1].date()))
    eq = pd.Series(bt["총자산"].values.astype(float), index=pd.to_datetime(bt["날짜"]))
    expo = pd.Series(bt["평가금"].values.astype(float), index=eq.index) / eq
    summary.append(profile("[비교] DSS 공격형 — 레짐 효과 있었음", eq, expo, reg))

    # ── 비교: 듀얼스나이퍼 로케트셋 ──
    import dual_sniper_engine as DS
    dpx = DS.load_price_data("SOXL", "2009-01-01", "2027-01-01")
    mm = DS.build_original_mode_map(dpx, extra_modes={})
    lg = DS.run_backtest(dpx, DS.DualSniperParams(), mode_map=mm, start_date="2016-01-04")
    eq = pd.Series(lg["총자산"].values.astype(float), index=pd.to_datetime(lg["날짜"]))
    expo = pd.Series(lg["평가금"].values.astype(float), index=eq.index) / eq
    summary.append(profile("[비교] 듀얼스나이퍼 로케트셋 — 레짐 효과 없었음", eq, expo, reg))

    df = pd.DataFrame(summary)
    df.to_csv(os.path.join(HERE, "reports", "regime_daily_profile.csv"), index=False, encoding="utf-8-sig")
    print("\n━━ 요약")
    print(df.to_string(index=False, float_format=lambda v: f"{v:.2f}"))


if __name__ == "__main__":
    main()
