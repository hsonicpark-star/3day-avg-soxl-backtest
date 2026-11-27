"""
DSS-N 실험용 시뮬레이터 (2026-11-27) — 배포 엔진(dss_engine)은 건드리지 않는 복사본

run_backtest_fast 와 같은 체결 규칙에, 날짜별로 달라지는 값(정규화)을 넣을 수 있게 한 것.
정규화 기준 v = 전일까지의 N일 일간 변동성 ÷ 기준 변동성(REF)  (v=1 이면 평소, 2 면 평소의 두 배로 출렁임)

  매수 조건 %  = 기본값 × v^a_buy      (0.25~4배로 제한)
  매도 조건 %  = 기본값 × v^a_sell
  최대 보유일  = 기본값 × v^a_hold     (1~90일)
  티어 비중    = 1 + s·(2u−1),  u = (티어−1)/(분할수−1)  → 분할수가 달라도 같은 모양 (s>0 뒤 티어를 크게)
                 s = s0 + s_v·ln(v)

a·s 가 모두 0 이고 V 가 켜져 있으면 공격형V 와 같은 결과 (check() 로 확인).
"""
import os, sys, math
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import numpy as np
import pandas as pd

REF = 0.045          # SOXL 일간 변동성 기준 (단위일 뿐 — 기본값에 흡수됨)
FEE, SEC = 0.0004, 0.0000278

BASE = dict(sf_div=7, sf_hold=38, sf_buy=0.033, sf_sell=0.018,
            ag_div=7, ag_hold=8, ag_buy=0.0315, ag_sell=0.0375,
            renew=10, pcr=0.75, lcr=0.20,
            v=1, risk_div=12, risk_ag_hold=5,
            win=20, a_buy_sf=0.0, a_buy_ag=0.0, a_sell_sf=0.0, a_sell_ag=0.0, a_hold_sf=0.0, a_hold_ag=0.0,
            s_sf=0.0, s_ag=0.0, sv_sf=0.0, sv_ag=0.0,
            # 2차 (2026-11-27, 공유받은 방식): ref=1 → v = 오늘 변동 ÷ 최근 1년 중앙값(종목 자체 기준)
            ref=0, q_exp=0.0, q_cap=2.0, q_floor=0.25,          # 수량 = 기본 × v^(−q_exp) (상한 q_cap 배)
            r_buy=0.0, r_sell=0.0, r_hold=0.0, r_qty=0.0,       # RSI14 (전일) — z = (50−RSI)/50
            prog=0.0,                                           # 보유 진행률: 수량 × (1 − prog × 보유수/분할수)
            tw_sf=None, tw_ag=None)                             # 티어 비중 직접 지정 (분할수와 길이가 같을 때만)


def make_ctx(df, mode_map, risk_days, wins=(10, 20, 40, 60)):
    """df: 종가 DataFrame(전체 기간) · mode_map: {날짜: 'AG'|'SF'} · risk_days: 위험 달 날짜 set"""
    idx = df.index
    c = df["Close"].astype(float)
    r = c.pct_change()
    vr, vrel = {}, {}
    for w in wins:
        sd = r.rolling(w).std()
        v = (sd.shift(1) / REF).values
        vr[w] = np.where(np.isfinite(v) & (v > 0), v, 1.0)
        v = (sd / sd.rolling(252, min_periods=60).median()).shift(1).values
        vrel[w] = np.where(np.isfinite(v) & (v > 0), v, 1.0)
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    rsi = (100 - 100 / (1 + up / dn)).shift(1).values
    rz = np.clip((50.0 - np.where(np.isfinite(rsi), rsi, 50.0)) / 50.0, -1, 1)
    return dict(idx=idx, closes=np.round(c.values.astype(np.float64), 2),
                mode=np.array([1 if mode_map.get(d, "AG") == "AG" else 0 for d in idx], dtype=np.int8),
                risk=np.array([d in risk_days for d in idx], dtype=bool), vr=vr, vrel=vrel, rz=rz)


def day_arrays(ctx, P):
    P = dict(BASE, **P)
    v = (ctx["vrel"] if P["ref"] else ctx["vr"])[int(P["win"])]
    rz = ctx["rz"]
    ag = ctx["mode"] == 1
    lv = np.log(v)

    def sc(base_sf, base_ag, a_sf, a_ag, lo, hi):
        f = np.exp(np.where(ag, a_ag, a_sf) * lv)
        return np.where(ag, base_ag, base_sf) * np.clip(f, lo, hi)

    buy = sc(P["sf_buy"], P["ag_buy"], P["a_buy_sf"], P["a_buy_ag"], 0.25, 4.0)
    sell = sc(P["sf_sell"], P["ag_sell"], P["a_sell_sf"], P["a_sell_ag"], 0.25, 4.0)
    hold = sc(P["sf_hold"], P["ag_hold"], P["a_hold_sf"], P["a_hold_ag"], 0.1, 10.0)
    if P["r_buy"]:
        buy = buy * np.clip(1 + P["r_buy"] * rz, 0.25, 4.0)
    if P["r_sell"]:
        sell = sell * np.clip(1 + P["r_sell"] * rz, 0.25, 4.0)
    if P["r_hold"]:
        hold = hold * np.clip(1 + P["r_hold"] * rz, 0.25, 4.0)
    hold = np.clip(np.rint(hold), 1, 90).astype(np.int64)
    qm = np.ones(len(v))
    if P["q_exp"]:
        qm = np.clip(np.exp(-P["q_exp"] * lv), P["q_floor"], P["q_cap"])
    if P["r_qty"]:
        qm = qm * np.clip(1 - P["r_qty"] * rz, 0.25, 2.0)
    div = np.where(ag, int(P["ag_div"]), int(P["sf_div"])).astype(np.int64)
    if P["v"]:
        rk = ctx["risk"]
        div = np.where(rk, int(P["risk_div"]), div)
        if P["risk_ag_hold"]:
            hold = np.where(rk & ag, int(P["risk_ag_hold"]), hold)
    slope = np.where(ag, P["s_ag"], P["s_sf"]) + np.where(ag, P["sv_ag"], P["sv_sf"]) * lv
    return buy, sell, hold, div, slope, qm


def sim(ctx, P, i0, i1, curve=False):
    """i0~i1 (포함) 구간 시뮬레이션. 반환: (최종자산/초기자본, MDD(음수), 거래일수[, 자산곡선])"""
    P = dict(BASE, **P)
    closes = ctx["closes"]
    buy_a, sell_a, hold_a, div_a, slope_a, qm_a = day_arrays(ctx, P)
    use_q = bool(P["q_exp"] or P["r_qty"])
    prog = P["prog"]
    tw = (tuple(P["tw_sf"] or ()), tuple(P["tw_ag"] or ()))
    tws = (sum(tw[0]), sum(tw[1]))
    mode_a = ctx["mode"]
    n_all = len(closes)
    renew, pcr, lcr = int(P["renew"]), P["pcr"], P["lcr"]
    use_w = bool(P["s_sf"] or P["s_ag"] or P["sv_sf"] or P["sv_ag"])
    floor = math.floor
    capital = cash = 10000.0
    positions = []
    dsr, pnl_acc = 0, 0.0
    peak, mdd, total = 10000.0, 0.0, 10000.0
    prev = closes[i0 - 1] if i0 > 0 else None
    eq = [] if curve else None
    for i in range(i0, i1 + 1):
        close = closes[i]
        div = div_a[i]
        n_open = len(positions)
        cash_open = cash
        cap_open = capital
        if n_open:
            keep = []
            for p in positions:
                if close >= p[0] or i >= p[1]:
                    amt = p[2] * close
                    net = amt - amt * (FEE + SEC)
                    cash += net
                    pnl_acc += net - (p[3] + p[4])
                else:
                    keep.append(p)
            positions = keep
        dsr += 1
        if dsr >= renew:
            if pnl_acc != 0:
                capital += pnl_acc * (pcr if pnl_acc > 0 else lcr)
            dsr, pnl_acc = 0, 0.0
        if prev is not None and n_open < div and len(positions) < div:
            bo = floor(prev * (1 + buy_a[i]) * 100) / 100
            if close <= bo:
                t = tw[mode_a[i]]
                if t and len(t) == div:
                    q = int(cap_open * t[n_open] / tws[mode_a[i]] / bo)
                    if q < 1:
                        q = 1
                elif use_w and div > 1:
                    s = slope_a[i]
                    tot = 0.0
                    wk = 1.0
                    for k in range(div):
                        w = 1.0 + s * (2.0 * k / (div - 1) - 1.0)
                        if w < 0.05:
                            w = 0.05
                        tot += w
                        if k == n_open:
                            wk = w
                    q = int(cap_open * wk / tot / bo)
                    if q < 1:
                        q = 1
                else:
                    q = int(cap_open / div / bo)
                if use_q:
                    q = int(q * qm_a[i])
                if prog:
                    q = int(q * (1.0 - prog * n_open / div))
                cap = int(max(0.0, cash_open) / (bo * (1 + FEE)))
                if q > cap:
                    q = cap
                if q > 0:
                    amt = q * close
                    fee = amt * FEE
                    if cash >= amt + fee:
                        cash -= amt + fee
                        st = i + hold_a[i]
                        if st >= n_all:
                            st = n_all - 1
                        positions.append((round(close * (1 + sell_a[i]), 2), st, q, amt, fee))
        hv = 0.0
        for p in positions:
            hv += p[2]
        total = cash + hv * close
        if total > peak:
            peak = total
        dd = (total - peak) / peak
        if dd < mdd:
            mdd = dd
        if curve:
            eq.append(total)
        prev = close
    out = (total / 10000.0, mdd, i1 - i0 + 1)
    return out + (np.array(eq),) if curve else out


def metric(ctx, P, s, e=None):
    """(CAGR %, MDD %, Calmar) — 엔진과 같은 정의 (년 = 거래일/252)"""
    idx = ctx["idx"]
    i0 = int(idx.searchsorted(pd.Timestamp(s)))
    i1 = (int(idx.searchsorted(pd.Timestamp(e), side="right")) - 1) if e else len(idx) - 1
    g, mdd, n = sim(ctx, P, i0, i1)
    cg = g ** (252.0 / n) - 1
    return cg * 100, mdd * 100, (cg / abs(mdd) if mdd else 0.0)


def yearly(ctx, P, s, e=None):
    """연도별 수익률(전년 말 대비) · 연중 MDD"""
    idx = ctx["idx"]
    i0 = int(idx.searchsorted(pd.Timestamp(s)))
    i1 = (int(idx.searchsorted(pd.Timestamp(e), side="right")) - 1) if e else len(idx) - 1
    eq = pd.Series(sim(ctx, P, i0, i1, curve=True)[3], index=idx[i0:i1 + 1])
    last = eq.groupby(eq.index.year).last()
    ret = last.pct_change()
    ret.iloc[0] = last.iloc[0] / 10000.0 - 1
    dd = eq / eq.cummax() - 1
    return pd.DataFrame({"수익률": ret * 100, "MDD": dd.groupby(dd.index.year).min() * 100})


def load_soxl():
    """SOXL 컨텍스트 (QQQ 주간 RSI 모드 · SOXX 변동성 레짐 = 배포 엔진과 같은 함수)"""
    from dss_engine import (load_price_data, build_weekly_rsi_series, build_mode_series, DSSParams,
                            get_week_mode_map, regime_day_map, load_regime_prices)
    os.chdir(HERE)
    df = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    rm = regime_day_map(df.index, DSSParams(vol_regime=True), load_regime_prices())
    risk = {d for d, r in rm.items() if r == "위험"}
    return make_ctx(df, get_week_mode_map(ms, df.index), risk), df, ms


def check():
    from dss_engine import DSSParams, run_backtest_fast
    ctx, df, ms = load_soxl()
    end = str(df.index[-1].date())
    kw = dict(sf_divisions=7, sf_max_hold=38, sf_buy_pct=0.033, sf_sell_pct=0.018, ag_divisions=7, ag_max_hold=8,
              ag_buy_pct=0.0315, ag_sell_pct=0.0375, renewal_period=10, pcr=0.75, lcr=0.20)
    for v in (0, 1):
        for s, e in (("2010-03-12", end), ("2011-01-03", end), ("2018-03-12", end), ("2010-03-12", "2018-03-12")):
            r = run_backtest_fast(DSSParams(vol_regime=bool(v), **kw), df, ms, s, e)
            a = r["final_asset"] / 1e4, r["mdd"]
            b = metric(ctx, dict(BASE, v=v), s, e)
            g = sim(ctx, dict(BASE, v=v), int(df.index.searchsorted(pd.Timestamp(s))),
                    int(df.index.searchsorted(pd.Timestamp(e), side="right")) - 1)
            print(f"V={v} {s}~{e}  엔진 {a[0]:.6f} / {a[1]*100:.4f}%   복사본 {g[0]:.6f} / {g[1]*100:.4f}%   "
                  f"차이 {abs(a[0]-g[0]):.2e}   CAGR {b[0]:.1f}% Calmar {b[2]:.2f}")


if __name__ == "__main__":
    check()
