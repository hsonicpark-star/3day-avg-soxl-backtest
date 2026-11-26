"""
표준편차매매 — ④ 매도 · 매수 가격 정규화 (2026-11-26)

LOC 가격 = 전일 종가 × (1 + σ × k) 의 k 를 신호에 따라 움직인다.
    k' = k + h × (1 − 2x)          x = 0~1 로 맞춘 신호 (전일 종가까지의 정보)
    매도가: h > 0 = 과매도(x 낮음)일수록 매도가를 높이고(작은 반등에 안 팖), 과매수일수록 낮춘다   ← 대화에서 나온 방향
    매수가: h > 0 = 과매도일수록 매수가를 높이고(더 쉽게 삼), 과매수일수록 낮춘다
    h = ±0.25 · ±0.5 · ±1.0   (기본 k_sell 0.5, k_buy 0.7~0.9)

신호 x: RSI3 · RSI5 · RSI14 · IBS · σ순위 · 5일 · 20일 수익률순위 (sd_norm_buy_2026-11.py 와 같음)
판정: SOXL 전체 · 앞/뒤 절반 · 봉인 · 4구간 최소 모두 기준 초과 + ~2025 / 교차 16종목
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


SB = _load("sb", "sd_norm_buy_2026-11.py")
SN, A2 = SB.SN, SB.A2
START, WIN, SUBS, TICKERS, KEYS = SB.START, SB.WIN, SB.SUBS, SB.TICKERS, SB.KEYS
HS = (-1.0, -0.5, -0.25, 0.25, 0.5, 1.0)


def sim(c, pc, sg, p, vmask=None, kb=None, ks=None, cap0=10000.0):
    """stdev_engine.run_backtest_stdev_fast 와 같은 산식 + 날짜별 k 가감 (kb / ks, None = 0)."""
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
        db_ = 0.0 if kb is None or kb[i] != kb[i] else kb[i]
        ds_ = 0.0 if ks is None or ks[i] != ks[i] else ks[i]
        b_loc = p0 * (1.0 + s * (k_buy + db_))
        s_loc = p0 * (1.0 + s * (k_sell + ds_))
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
        if cl <= b_loc and b_loc > 0:
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


class Run(SB.Run):
    def adj(self, name, h):
        return h * (1.0 - 2.0 * self.sig[name])

    def go(self, p, s=None, e=None, vmask=None, kb=None, ks=None):
        i0 = self.off if s is None else max(self.off, self.px.index.searchsorted(pd.Timestamp(s)))
        i1 = len(self.c_all) if e is None else self.px.index.searchsorted(pd.Timestamp(e))
        vm = None if vmask is None else vmask[i0 - self.off:i1 - self.off]
        eq = sim(self.c_all[i0:i1], self.c_all[i0 - 1:i1 - 1], self.sg_all[i0:i1], p, vm,
                 None if kb is None else kb[i0:i1], None if ks is None else ks[i0:i1])
        return SN.cm(eq, self.px.index[i0:i1])

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
    a = (R.c_all[R.off:], R.c_all[R.off - 1:-1], R.sg_all[R.off:], db["균형형"])
    print(f"가감 없음 = 이전 검증 루프와 차이 {abs(SN.sim(*a)[-1] - sim(*a)[-1]):.6f}")
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
        rows = [dict(대상="기준", 신호="-", h=0.0, **base, 통과="", 교차="-", 교차차이=0.0)]
        for tgt, key in (("매도가", "ks"), ("매수가", "kb")):
            for nm in names:
                for h in HS:
                    r = R.all(p, vm, **{key: R.adj(nm, h)})
                    oc = {tk: o.go(p, vmask=o.vown if usev else None, **{key: o.adj(nm, h)})[2] for tk, o in others.items()}
                    diff = np.array([oc[tk] - ob[tk] for tk in ob])
                    rows.append(dict(대상=tgt, 신호=nm, h=h, **r, 통과="✅" if all(r[k] > base[k] for k in KEYS) else "",
                                     교차=f"{int((diff > 0).sum())}/{len(diff)}", 교차차이=float(np.median(diff))))
        df = pd.DataFrame(rows)
        df.insert(0, "프리셋", name)
        rows_all.append(df)
        b = df.iloc[0]
        print(f"\n━━ ④ {name} — 기준 {b['CAGR']:.1f}% / {b['MDD']:.1f}% / {b['Calmar']:.2f} "
              f"(H1 {b['H1']:.2f} H2 {b['H2']:.2f} 봉인 {b['봉인']:.2f} 최소 {b['구간최소']:.2f} ~2025 {b['~2025']:.2f})")
        cols = ["대상", "신호", "h", "CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소", "~2025", "통과", "교차", "교차차이"]
        for tgt in ("매도가", "매수가"):
            s = df[df["대상"] == tgt]
            ok = s[s["통과"] == "✅"]
            print(f"   [{tgt}] 전 항목 통과 {len(ok)}/{len(s)} · Calmar 범위 {s['Calmar'].min():.2f} ~ {s['Calmar'].max():.2f}")
            # 신호 × 방향 요약 (Calmar)
            pv = s.pivot(index="신호", columns="h", values="Calmar").reindex(names)
            print("     " + pv.to_string(float_format=f2).replace("\n", "\n     "))
            if len(ok):
                print("     통과:")
                print("     " + ok[cols].to_string(index=False, float_format=f2).replace("\n", "\n     "))
    pd.concat(rows_all).to_csv(os.path.join(HERE, "reports", "sd_norm_price.csv"), index=False, encoding="utf-8-sig")
    print(f"\n{time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
