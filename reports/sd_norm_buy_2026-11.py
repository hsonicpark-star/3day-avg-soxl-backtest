"""
표준편차매매 — ③ 매수 수량 정규화 (2026-11-26)

1회 매수 한도(사이클 투자금 ÷ 분할수)에 신호에 따른 배율을 곱한다 (예수금 한도는 그대로).
    배율 = 1 + g × (1 − 2x)        x = 0~1 로 맞춘 신호 (전일 종가까지의 정보)
    g > 0 : 신호가 낮을수록(과매도) 더 산다 · g < 0 : 신호가 낮을수록 덜 산다     (g = ±0.5 → 0.5~1.5배, ±1 → 0~2배)

신호 x
    RSI3 · RSI5 · RSI14   전일 RSI ÷ 100
    IBS                    전일 (종가 − 저가) ÷ (고가 − 저가)
    σ순위                  오늘 주문에 쓰는 2일 σ 의 최근 252일 순위 (높으면 변동성 큼)
    5일 · 20일 수익률순위  최근 252일 순위

판정
    SOXL: 전체 · 앞/뒤 절반 · 봉인(2024~) · 4구간 최소 모두 기준 초과 + ~2025년 말
    교차 종목: 3배 ETF 16종에 같은 값 그대로 — 기본보다 Calmar 가 나은 종목 수 · 차이 중앙값
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
SN = _load("sn", "sd_norm_sell_2026-11.py")
START, WIN, SUBS, TICKERS = SN.START, SN.WIN, SN.SUBS, SN.TICKERS
GS = (-1.0, -0.5, 0.5, 1.0)
KEYS = ["Calmar", "H1", "H2", "봉인", "구간최소"]


def sim(c, pc, sg, p, vmask=None, bm=None, cap0=10000.0):
    """stdev_engine.run_backtest_stdev_fast 와 같은 산식 + 날짜별 매수 한도 배율 bm (None = 1)."""
    div, ren = p["divisions"], p["renewal"]
    k_buy, k_sell, sr = p["k_buy"], p["k_sell"], p["sell_ratio"] / 100.0
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
            sq = int(round(float(hold) * (1.0 if rk else sr)))
        bq = 0
        if cl <= b_loc:
            f = 1.0 if bm is None else bm[i]
            avail = min(daily * (f if f == f else 1.0), cash)
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


def rsi(close, n):
    d = close.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))


def rank252(s):
    return s.rolling(252, min_periods=60).apply(lambda w: (w[:-1] < w[-1]).mean(), raw=True)


def signals(df):
    """{이름: x(0~1) Series} — 값은 '그날 주문 전에 아는 정보' (전일까지) 로 맞춘다."""
    c = df["Close"].astype(float)
    r = c.pct_change()
    sig2 = r.rolling(2).std(ddof=0)                       # 당일 포함 2일 σ → 다음 날 주문에 쓰임
    ibs = ((c - df["Low"]) / (df["High"] - df["Low"]).replace(0, np.nan)).clip(0, 1)
    raw = {"RSI3": rsi(c, 3) / 100, "RSI5": rsi(c, 5) / 100, "RSI14": rsi(c, 14) / 100, "IBS": ibs,
           "σ순위": rank252(sig2), "5일수익순위": rank252(c.pct_change(5)), "20일수익순위": rank252(c.pct_change(20))}
    return {k: v.shift(1) for k, v in raw.items()}


class Run:
    def __init__(self, df, start):
        px = df[["Close"]]
        self.px = px
        self.c_all, self.sg_all = SN.prep(px)
        self.off = max(px.index.searchsorted(pd.Timestamp(start)), 1)
        self.idx = px.index[self.off:]
        own = px["Close"].pct_change().rolling(20).std() * np.sqrt(252)
        self.vown = A2.month_mask(self.idx, A2.risk_months(own, 0.7))
        self.sig = {k: v.values for k, v in signals(df).items()}

    def bm(self, name, g):
        x = self.sig[name]
        return np.clip(1.0 + g * (1.0 - 2.0 * x), 0.0, None)

    def go(self, p, s=None, e=None, vmask=None, bm=None):
        i0 = self.off if s is None else max(self.off, self.px.index.searchsorted(pd.Timestamp(s)))
        i1 = len(self.c_all) if e is None else self.px.index.searchsorted(pd.Timestamp(e))
        vm = None if vmask is None else vmask[i0 - self.off:i1 - self.off]
        eq = sim(self.c_all[i0:i1], self.c_all[i0 - 1:i1 - 1], self.sg_all[i0:i1], p, vm, None if bm is None else bm[i0:i1])
        return SN.cm(eq, self.px.index[i0:i1])

    def all(self, p, vmask=None, bm=None):
        f = self.go(p, vmask=vmask, bm=bm)
        d = dict(CAGR=f[0], MDD=f[1], Calmar=f[2])
        for k, (s, e) in WIN.items():
            d[k] = self.go(p, s, e, vmask, bm)[2]
        d["구간최소"] = min(self.go(p, s, e, vmask, bm)[2] for s, e in SUBS)
        d["~2025"] = self.go(p, START, "2026-01-01", vmask, bm)[2]
        return d


def main():
    t0 = time.time()
    import update_preset_stats as U
    from dss_engine import load_regime_prices, vol_regime_table
    os.chdir(HERE)
    db = {p["label"]: p for p in U.extract_literal("strategies/stdev.py", "_SD_PRESETS_DB")["SOXL"]}
    data = {}
    for tk in TICKERS:
        try:
            data[tk] = U.load_adjusted(tk)
        except Exception as e:      # noqa: BLE001
            print(f"{tk}: 데이터 실패 {str(e)[:60]}")
    R = Run(data["SOXL"], START)
    t = vol_regime_table(load_regime_prices(), 0.7)
    v1 = A2.month_mask(R.idx, {m for m, g in zip(t["적용월"], t["레짐"]) if g == "위험"})
    chk = SN.sim(R.c_all[R.off:], R.c_all[R.off - 1:-1], R.sg_all[R.off:], db["균형형"])[-1]
    print(f"배율 없음 = 이전 검증 루프와 차이 {abs(chk - sim(R.c_all[R.off:], R.c_all[R.off - 1:-1], R.sg_all[R.off:], db['균형형'])[-1]):.6f}")
    others = {tk: Run(d, max(pd.Timestamp(START), d.index[1])) for tk, d in data.items() if tk != "SOXL"}
    names = list(R.sig)
    f2 = lambda v: f"{v:.2f}"
    rows_all = []
    for lab, usev in (("균형형", False), ("공격형", False), ("균형형", True), ("공격형", True)):
        p = db[lab]
        name = lab + ("V" if usev else "")
        vm = v1 if usev else None
        base = R.all(p, vm)
        ob = {tk: r.go(p, vmask=r.vown if usev else None)[2] for tk, r in others.items()}
        rows = [dict(신호="기준", g=0.0, **base, 통과="", 교차="-", 교차차이=0.0)]
        for nm in names:
            for g in GS:
                r = R.all(p, vm, R.bm(nm, g))
                oc = {tk: o.go(p, vmask=o.vown if usev else None, bm=o.bm(nm, g))[2] for tk, o in others.items()}
                diff = np.array([oc[tk] - ob[tk] for tk in ob])
                rows.append(dict(신호=nm, g=g, **r, 통과="✅" if all(r[k] > base[k] for k in KEYS) else "",
                                 교차=f"{int((diff > 0).sum())}/{len(diff)}", 교차차이=float(np.median(diff))))
        df = pd.DataFrame(rows)
        print(f"\n━━ ③ {name}  (g>0 = 신호 낮을수록 더 삼, 교차 = 16종목 중 기본보다 나은 수 · 차이 중앙값)")
        print(df[["신호", "g", "CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소", "~2025", "통과", "교차", "교차차이"]]
              .to_string(index=False, float_format=f2))
        df.insert(0, "프리셋", name)
        rows_all.append(df)
    pd.concat(rows_all).to_csv(os.path.join(HERE, "reports", "sd_norm_buy.csv"), index=False, encoding="utf-8-sig")
    print(f"\n{time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
