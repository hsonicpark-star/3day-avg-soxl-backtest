"""
표준편차매매 — ① 매도 비율 정규화  ② 교차 종목 검증 (2026-11-26)

① 매도 비율을 상수(80%) 대신 보유 진행률로 정한다
     매도 비율 = 최소매도율 + 진행률 × (1 − 최소매도율)
     진행률 A(예수금) = 1 − 예수금 ÷ 사이클 투자금        (대화에서 나온 정의)
     진행률 B(비중)   = 보유 평가금(전일 종가) ÷ 전일 총자산
   비교 대상: 고정 매도 비율 60~100% (상수만 바꿔도 같은 효과인지), V(변동성 레짐)
   SOXL 5개 프리셋 + 균형형V · 공격형V. 전체 · 앞/뒤 절반 · 봉인(2024~) · 4구간 최소 · ~2025년 말

② 파라미터를 그대로 두고 다른 3배 ETF 에 돌린다 (가격 타점이 σ 로 정규화돼 있어 가능)
     기본 프리셋 / V(그 종목 자체 20일 변동성으로 같은 절차) / 매도 비율 정규화
"""
import sys, os, math, time, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "scripts"))
import numpy as np
import pandas as pd
import importlib.util


def _load(name, fn):
    sp = importlib.util.spec_from_file_location(name, os.path.join(HERE, "reports", fn))
    m = importlib.util.module_from_spec(sp)
    sp.loader.exec_module(m)
    return m


A2 = _load("a2", "a2_tr_regime_2026-11.py")
START = "2010-03-12"
WIN = {"H1": (START, "2018-07-02"), "H2": ("2018-07-02", None), "봉인": ("2024-01-02", None)}
SUBS = [(START, "2014-05-01"), ("2014-05-01", "2018-07-02"), ("2018-07-02", "2022-09-01"), ("2022-09-01", None)]
TICKERS = ["SOXL", "TQQQ", "TECL", "UPRO", "TNA", "UDOW", "FAS", "LABU", "WEBL", "NAIL", "DFEN", "CURE", "DPST",
           "YINN", "KORU", "DRN", "RETL"]


def sim(c, pc, sg, p, vmask=None, mode=None, m=None, sr_fix=None, cap0=10000.0):
    """stdev_engine.run_backtest_stdev_fast 와 같은 순서·산식 + 매도 비율 정규화.
    mode None = 고정 매도 비율(sr_fix 또는 프리셋), 'cash' / 'wt' = 진행률 A / B.  vmask = V 위험일(매도 100% · 상한 50%)."""
    div, ren = p["divisions"], p["renewal"]
    k_buy, k_sell = p["k_buy"], p["k_sell"]
    sr = (sr_fix if sr_fix is not None else p["sell_ratio"]) / 100.0
    n = len(c)
    cash, hold, avg, cum, tot, tier = cap0, 0, 0.0, 0.0, cap0, 0
    buf = np.empty(n + 1); bi = 0
    out = np.empty(n); prev_asset = cap0
    for i in range(n):
        cl, s = c[i], sg[i]
        if s != s:
            out[i] = prev_asset = cash + hold * cl
            buf[bi] = cum; bi += 1
            continue
        p0 = pc[i]
        b_loc = p0 * (1.0 + s * k_buy)
        s_loc = p0 * (1.0 + s * k_sell)
        tier = (tier % div) + 1
        if tier == 1 and bi > 0:
            lb = (ren + 1) if bi > 2 * div else ren
            tot += cum - (buf[bi - lb] if bi >= lb else 0.0)
        daily = tot / div
        rk = vmask is not None and vmask[i]
        sq = 0
        if hold > 0 and cl >= s_loc:
            if rk:
                ratio = 1.0
            elif mode is None:
                ratio = sr
            else:
                prog = (1.0 - cash / tot) if mode == "cash" else (hold * p0 / prev_asset)
                prog = min(1.0, max(0.0, prog))
                ratio = m + prog * (1.0 - m)
            sq = int(round(float(hold) * ratio))
        bq = 0
        if cl <= b_loc:
            avail = min(daily, cash)
            if rk:
                avail = min(avail, max(0.0, 0.5 * prev_asset - hold * p0))
            bq = math.floor(avail / b_loc)
        if sq > 0:
            cum += cl * sq - avg * sq
        rem = hold - sq
        nh = rem + bq
        if nh > 0:
            avg = (rem * avg + cl * bq) / nh if bq > 0 else avg
        else:
            avg = 0.0
        hold = nh
        cash = cash - cl * bq + cl * sq
        out[i] = prev_asset = cash + hold * cl
        buf[bi] = cum; bi += 1
    return out


def prep(px, sp=2):
    c = px["Close"].values.astype(float)
    r = np.full(len(c), np.nan); r[1:] = c[1:] / c[:-1] - 1
    sg = np.full(len(c), np.nan)
    for i in range(sp, len(c)):
        w = r[i - sp:i]
        if not np.any(np.isnan(w)):
            sg[i] = np.std(w, ddof=0)
    return c, sg


def cm(eq, idx):
    y = (idx[-1] - idx[0]).days / 365.25
    cg = (eq[-1] / 1e4) ** (1 / y) - 1
    md = abs((eq / np.maximum.accumulate(eq) - 1).min()) or 1e-9
    return cg * 100, -md * 100, cg / md


class Run:
    def __init__(self, px, start):
        self.c_all, self.sg_all = prep(px)
        self.px = px
        self.off = max(px.index.searchsorted(pd.Timestamp(start)), 1)
        self.idx = px.index[self.off:]
        own = (px["Close"].pct_change().rolling(20).std() * np.sqrt(252))
        self.vown = A2.month_mask(self.idx, A2.risk_months(own, 0.7))

    def go(self, p, s=None, e=None, vmask=None, **kw):
        i0 = self.off if s is None else max(self.off, self.px.index.searchsorted(pd.Timestamp(s)))
        i1 = len(self.c_all) if e is None else self.px.index.searchsorted(pd.Timestamp(e))
        vm = None if vmask is None else vmask[i0 - self.off:i1 - self.off]
        eq = sim(self.c_all[i0:i1], self.c_all[i0 - 1:i1 - 1], self.sg_all[i0:i1], p, vm, **kw)
        return cm(eq, self.px.index[i0:i1])

    def all(self, p, vmask=None, **kw):
        f = self.go(p, vmask=vmask, **kw)
        d = dict(CAGR=f[0], MDD=f[1], Calmar=f[2])
        for k, (s, e) in WIN.items():
            d[k] = self.go(p, s, e, vmask, **kw)[2]
        d["구간최소"] = min(self.go(p, s, e, vmask, **kw)[2] for s, e in SUBS)
        d["~2025"] = self.go(p, START, "2026-01-01", vmask, **kw)[2]
        return d


def main():
    t0 = time.time()
    import stdev_engine as E
    import update_preset_stats as U
    from dss_engine import load_regime_prices, vol_regime_table
    os.chdir(HERE)
    db = {p["label"]: p for p in U.extract_literal("strategies/stdev.py", "_SD_PRESETS_DB")["SOXL"]}
    px = U.load_adjusted("SOXL")[["Close"]]
    R = Run(px, START)
    t = vol_regime_table(load_regime_prices(), 0.7)
    v1 = A2.month_mask(R.idx, {m for m, g in zip(t["적용월"], t["레짐"]) if g == "위험"})
    # 엔진 일치 확인
    for lab in ("균형형", "공격형"):
        p = db[lab]
        kw = dict(sigma_period=2, k_buy=p["k_buy"], k_sell=p["k_sell"], sell_ratio=p["sell_ratio"], divisions=p["divisions"],
                  renewal=p["renewal"], initial_capital=10000.0)
        e0 = E.run_backtest_stdev_fast(px, START, str(px.index[-1].date()), **kw)["final_asset"]
        e1 = E.run_backtest_stdev_fast(px, START, str(px.index[-1].date()), vreg=E.sd_vreg("1"), **kw)["final_asset"]
        m0 = sim(R.c_all[R.off:], R.c_all[R.off - 1:-1], R.sg_all[R.off:], p)[-1]
        m1 = sim(R.c_all[R.off:], R.c_all[R.off - 1:-1], R.sg_all[R.off:], p, v1)[-1]
        print(f"엔진 일치 [{lab}] 기본 차이 {abs(e0 - m0):.6f} · V 차이 {abs(e1 - m1):.6f}")

    # ── ① 매도 비율 정규화 ──
    f2 = lambda v: f"{v:.2f}"
    cols = ["규칙", "CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소", "~2025", "통과"]
    keys = ["Calmar", "H1", "H2", "봉인", "구간최소"]
    rows_all = []
    for lab, vm in (("균형형", None), ("공격형", None), ("안정형", None), ("Ultra-Safe형", None), ("Sonic형", None),
                    ("균형형", v1), ("공격형", v1)):
        p = db[lab]
        name = lab + ("V" if vm is not None else "")
        base = R.all(p, vm)
        rows = [dict(규칙=f"기준 (고정 {p['sell_ratio']:.0f}%)", **base, 통과="")]
        for sr in (50, 60, 70, 80, 90, 100):
            if sr == p["sell_ratio"]:
                continue
            r = R.all(p, vm, sr_fix=float(sr))
            rows.append(dict(규칙=f"고정 {sr}%", **r, 통과="✅" if all(r[k] > base[k] for k in keys) else ""))
        for mode, nm in (("cash", "A 예수금"), ("wt", "B 비중")):
            for m in (0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8):
                r = R.all(p, vm, mode=mode, m=m)
                rows.append(dict(규칙=f"진행률 {nm} · 최소 {int(m*100)}%", **r,
                                 통과="✅" if all(r[k] > base[k] for k in keys) else ""))
        df = pd.DataFrame(rows)
        print(f"\n━━ ① {name}")
        print(df[cols].to_string(index=False, float_format=f2))
        df.insert(0, "프리셋", name)
        rows_all.append(df)
    pd.concat(rows_all).to_csv(os.path.join(HERE, "reports", "sd_norm_sell.csv"), index=False, encoding="utf-8-sig")

    # ── ② 교차 종목 ──
    variants = [("기본", {}, False), ("V(자기 변동성)", {}, True),
                ("고정 100%", dict(sr_fix=100.0), False),
                ("진행률A 최소30", dict(mode="cash", m=0.3), False), ("진행률A 최소50", dict(mode="cash", m=0.5), False),
                ("진행률A 최소70", dict(mode="cash", m=0.7), False), ("진행률B 최소50", dict(mode="wt", m=0.5), False),
                ("V + 진행률A 최소50", dict(mode="cash", m=0.5), True)]
    out = []
    for tk in TICKERS:
        try:
            d = px if tk == "SOXL" else U.load_adjusted(tk)[["Close"]]
        except Exception as e:      # noqa: BLE001
            print(f"{tk}: 데이터 실패 {str(e)[:60]}")
            continue
        st = max(pd.Timestamp(START), d.index[1])
        r = Run(d, st)
        bh = d["Close"].values[r.off:]
        bh_c = cm(bh / bh[0] * 1e4, r.idx)
        for lab in ("균형형", "공격형"):
            row = dict(종목=tk, 시작=str(r.idx[0].date()), 프리셋=lab, BH=bh_c[2], BH_MDD=bh_c[1])
            for nm, kw, usev in variants:
                f = r.go(db[lab], vmask=r.vown if usev else None, **kw)
                row[nm] = f[2]; row[nm + "_CAGR"] = f[0]; row[nm + "_MDD"] = f[1]
            out.append(row)
    cx = pd.DataFrame(out)
    cx.to_csv(os.path.join(HERE, "reports", "sd_cross_ticker.csv"), index=False, encoding="utf-8-sig")
    vn = [v[0] for v in variants]
    for lab in ("균형형", "공격형"):
        s = cx[cx["프리셋"] == lab]
        print(f"\n━━ ② 교차 종목 — {lab} (SOXL 파라미터 그대로) · Calmar")
        print(s[["종목", "시작", "BH", "BH_MDD", "기본_CAGR", "기본_MDD"] + vn].to_string(index=False, float_format=f2))
        o = s[s["종목"] != "SOXL"]
        print(f"   SOXL 제외 {len(o)}종목 요약:")
        print(f"     기본: Calmar 중앙값 {o['기본'].median():.2f} · B&H 보다 나은 종목 {(o['기본'] > o['BH']).sum()}/{len(o)}"
              f" · MDD 중앙값 {o['기본_MDD'].median():.1f}% (B&H {o['BH_MDD'].median():.1f}%) · MDD −80% 아래 {(o['기본_MDD'] < -80).sum()}종목")
        for nm in vn[1:]:
            print(f"     {nm:<20} 기본보다 나은 종목 {(o[nm] > o['기본']).sum()}/{len(o)} · Calmar 중앙값 {o[nm].median():.2f}"
                  f" · 차이 중앙값 {(o[nm] - o['기본']).median():+.2f} · MDD 중앙값 {o[nm + '_MDD'].median():.1f}%")
    print(f"\n{time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
