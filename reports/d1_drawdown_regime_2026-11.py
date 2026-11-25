"""
D1 낙폭 레짐 × DSS · 표준편차매매 (2026-11-25) — 레짐_카탈로그.md 의 D1

레짐: 낙폭 = SOXX 종가 ÷ 최근 252거래일 최고 종가 − 1
      칸 0 정상(> −10%) · 1 조정(−10~−20%) · 2 약세(−20~−30%) · 3 폭락(< −30%)
      판정 '일' = 전일 칸 → 오늘 / '월' = 월말 칸 → 다음 달

① 프로파일: 칸별 일수 · 일평균 수익 · 일샤프 · 평균 노출 (기준 전략 그대로 돌린 결과를 칸으로 나눠 봄)
② 본 검증: 적용 구간(S10 −10% 아래 전부 · S20 · S30 · B1 조정만 · B2 약세만 · B12) × 판정(일/월) × 규칙
            기준 = 기본 프리셋 / V 프리셋(변동성 레짐 위에 D1 을 더 얹음)
   통과 = 전체 · 앞/뒤 절반 · 봉인(2024~) · 4구간 최소 가 모두 기준 초과
"""
import sys, os, math, itertools, pickle, tempfile, time, warnings
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


VR = _load("vr", "dss_vol_regime_2026-11.py")
S1 = _load("sd1", "sd_vol_regime_2026-11.py")
from dss_engine import (load_price_data, build_weekly_rsi_series, build_mode_series, DSSParams,
                        run_backtest, run_backtest_fast, get_week_mode_map, load_regime_prices, vol_regime_table)

START = "2010-03-12"
BAND_NAMES = ["정상 >-10%", "조정 -10~-20%", "약세 -20~-30%", "폭락 <-30%"]
SETS = {"S10": (1, 2, 3), "S20": (2, 3), "S30": (3,), "B1": (1,), "B2": (2,), "B12": (1, 2)}
FREQS = ("일", "월")

# 표준편차: (매수 한도 배율, 매도 비율, 비중 상한)
SD_RULES = [r for r in itertools.product((1.0, 0.5, 0.25, 0.0, 1.5), (None, 100.0), (None, 0.5))
            if r != (1.0, None, None)]
SD_V = (1.0, 100.0, 0.5)
SD_WIN = {"H1": (START, "2018-07-02"), "H2": ("2018-07-02", None), "봉인": ("2024-01-02", None)}
SD_SUBS = [(START, "2014-05-01"), ("2014-05-01", "2018-07-02"), ("2018-07-02", "2022-09-01"), ("2022-09-01", None)]

# DSS: (분할수, 매수 배율, 공세 보유일)
DSS_RULES = [(12, 1.0, None), (10, 1.0, None), (12, 1.0, 5), (None, 0.6, None), (None, 0.0, None), (5, 1.0, None)]
DSS_WIN = {"H1": (START, "2018-03-12"), "H2": ("2018-03-12", None), "봉인": ("2024-01-02", None)}
DSS_SUBS = [(START, "2014-03-12"), ("2014-03-12", "2018-03-12"), ("2018-03-12", "2022-03-12"), ("2022-03-12", None)]

_D = {}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))


# ── 레짐 ──────────────────────────────────────────────
def d1_bands(soxx, win=252):
    dd = soxx / soxx.rolling(win).max() - 1.0
    b = pd.Series(np.select([dd > -0.10, dd > -0.20, dd > -0.30], [0, 1, 2], 3), index=soxx.index).where(dd.notna())
    return dd, b


def d1_labels(band, trade_idx, freq):
    """거래일 → 칸 (판정 이후에만 적용). NaN = 미판정."""
    band = band.dropna()
    if freq == "일":
        prev = band.shift(1).dropna()
        return prev.reindex(trade_idx, method="ffill").values
    ends = band.groupby(band.index.to_period("M")).tail(1)
    mp_ = {d.to_period("M") + 1: v for d, v in ends.items()}
    return np.array([mp_.get(d.to_period("M"), np.nan) for d in trade_idx])


# ── 표준편차 시뮬 (S1.sim 과 같은 산식, 날짜별 배열) ──────
def sd_sim(closes, prevs, sigmas, p, mult, srv, capv, cap0=10000.0):
    div, ren = p["divisions"], p["renewal"]
    k_buy, k_sell, sr = p["k_buy"], p["k_sell"], p["sell_ratio"] / 100.0
    n = len(closes)
    cash, hold, avg, cum, tot, tier = cap0, 0, 0.0, 0.0, cap0, 0
    buf = np.empty(n + 1)
    bi = 0
    out = np.empty(n)
    prev_asset = cap0
    for i in range(n):
        c, s = closes[i], sigmas[i]
        if s != s:
            out[i] = prev_asset = cash + hold * c
            buf[bi] = cum; bi += 1
            continue
        pc = prevs[i]
        b_loc = pc * (1.0 + s * k_buy)
        s_loc = pc * (1.0 + s * k_sell)
        tier = (tier % div) + 1
        if tier == 1 and bi > 0:
            lb = (ren + 1) if bi > 2 * div else ren
            pcum = buf[bi - lb] if bi >= lb else 0.0
            tot += cum - pcum
        daily = tot / div
        sq = 0
        if hold > 0 and c >= s_loc:
            sv = srv[i]
            sq = int(round(float(hold) * (sr if sv != sv else sv)))
        bq = 0
        if c <= b_loc:
            avail = min(daily * mult[i], cash)
            cv = capv[i]
            if cv == cv:
                avail = min(avail, max(0.0, cv * prev_asset - hold * pc))
            bq = math.floor(avail / b_loc)
        if sq > 0:
            cum += c * sq - avg * sq
        rem = hold - sq
        nh = rem + bq
        if nh > 0:
            avg = (rem * avg + c * bq) / nh if bq > 0 else avg
        else:
            avg = 0.0
        hold = nh
        cash = cash - c * bq + c * sq
        out[i] = prev_asset = cash + hold * c
        buf[bi] = cum; bi += 1
    return out


def sd_arrays(n, vmask, dmask, rule):
    mult = np.ones(n); srv = np.full(n, np.nan); capv = np.full(n, np.nan)
    if vmask is not None:
        srv[vmask] = SD_V[1] / 100.0; capv[vmask] = SD_V[2]
    if rule is not None:
        m, sr, cap = rule
        mult[dmask] = m
        if sr is not None:
            srv[dmask] = sr / 100.0
        if cap is not None:
            capv[dmask] = np.fmin(capv[dmask], cap)
    return mult, srv, capv


def sd_metric(pk, s, e, arrs):
    a, idx = _D["sd"][pk], _D["sd_idx"]
    i0 = idx.searchsorted(pd.Timestamp(s))
    i1 = len(idx) if e is None else idx.searchsorted(pd.Timestamp(e))
    eq = sd_sim(a["c"][i0:i1], a["pc"][i0:i1], a["sg"][i0:i1], a["p"], *[x[i0:i1] for x in arrs])
    y = (idx[i1 - 1] - idx[i0]).days / 365.25
    cg = (eq[-1] / 1e4) ** (1 / y) - 1
    md = abs((eq / np.maximum.accumulate(eq) - 1).min()) or 1e-9
    return cg, md, cg / md


def sd_eval(item):
    pk, base, freq, sname, rule = item
    n = len(_D["sd_idx"])
    vmask = _D["sd_v"] if base == "V" else None
    dmask = np.isin(_D["sd_lab"][freq], SETS[sname]) if rule is not None else None
    arrs = sd_arrays(n, vmask, dmask, rule)
    cg, md, cm = sd_metric(pk, START, None, arrs)
    out = dict(전략="표준편차", 프리셋=pk + ("V" if base == "V" else ""), 판정=freq, 구간=sname,
               규칙="기준" if rule is None else f"배율{rule[0]}·매도{rule[1] or '-'}·상한{rule[2] or '-'}",
               기준=rule is None, CAGR=cg * 100, MDD=-md * 100, Calmar=cm)
    for k, (s, e) in SD_WIN.items():
        out[k] = sd_metric(pk, s, e, arrs)[2]
    out["구간최소"] = min(sd_metric(pk, s, e, arrs)[2] for s, e in SD_SUBS)
    return out


# ── DSS ───────────────────────────────────────────────
def dss_dicts(base, freq, sname, rule):
    dv, dh, bm = {}, {}, {}
    qm = _D["qmode"]
    if base == "V":
        for d in _D["dss_v"]:
            dv[d] = 12
            if qm.get(d) == "AG":
                dh[d] = 5
    if rule is not None:
        div, mult, agh = rule
        lab = _D["dss_lab"][freq]
        for d, b in zip(_D["dss_idx"], lab):
            if b in SETS[sname]:
                if div is not None:
                    dv[d] = div
                if mult != 1.0:
                    bm[d] = mult
                if agh is not None and qm.get(d) == "AG":
                    dh[d] = agh
    return dv or None, dh or None, bm or None


def dss_metric(s, e, dv, dh, bm):
    p = DSSParams(**VR.PRESETS["공격형"])
    r = run_backtest_fast(p, _D["soxl"], _D["ms"], s, e or _D["end"], day_divisions=dv, day_hold=dh, day_buy_mult=bm)
    y = r["total_days"] / 252
    cg = (r["final_asset"] / 1e4) ** (1 / y) - 1
    md = abs(r["mdd"])
    return cg, md, cg / md


def dss_eval(item):
    base, freq, sname, rule = item
    dv, dh, bm = dss_dicts(base, freq, sname, rule)
    cg, md, cm = dss_metric(START, None, dv, dh, bm)
    out = dict(전략="DSS", 프리셋="공격형" + ("V" if base == "V" else ""), 판정=freq, 구간=sname,
               규칙="기준" if rule is None else f"분할{rule[0] or '-'}·배율{rule[1]}·공세보유{rule[2] or '-'}",
               기준=rule is None, CAGR=cg * 100, MDD=-md * 100, Calmar=cm)
    for k, (s, e) in DSS_WIN.items():
        out[k] = dss_metric(s, e, dv, dh, bm)[2]
    out["구간최소"] = min(dss_metric(s, e, dv, dh, bm)[2] for s, e in DSS_SUBS)
    return out


def evaluate(item):
    return sd_eval(item[1:]) if item[0] == "SD" else dss_eval(item[1:])


# ── 프로파일 ──────────────────────────────────────────
def profile(name, eq, expo, lab):
    r = eq.pct_change().fillna(0.0)
    y = len(eq) / 252
    cagr = (eq.iloc[-1] / eq.iloc[0]) ** (1 / y) - 1
    mdd = (eq / eq.cummax() - 1).min()
    print(f"\n━━ {name} — CAGR {cagr*100:.1f}% / MDD {mdd*100:.1f}% / Calmar {cagr/abs(mdd):.2f}")
    rows = []
    for b in range(4):
        m = lab == b
        if m.sum() < 5:
            continue
        rows.append({"칸": BAND_NAMES[b], "일수": int(m.sum()), "비율(%)": m.mean() * 100, "일평균(%)": r[m].mean() * 100,
                     "일변동(%)": r[m].std() * 100, "일샤프": r[m].mean() / r[m].std(),
                     "평균노출(%)": expo[m].mean() * 100, "최악하루(%)": r[m].min() * 100})
    print(pd.DataFrame(rows).to_string(index=False, float_format=lambda v: f"{v:.2f}"))
    return [dict(전략=name, **x) for x in rows]


def main():
    t0 = time.time()
    import stdev_engine as E
    import update_preset_stats as U
    os.chdir(HERE)
    soxx = load_regime_prices()
    dd, band = d1_bands(soxx)

    # 표준편차 데이터 (S1.prepare 와 같은 가격 고정)
    _px = U.load_adjusted("SOXL")
    U.load_adjusted = lambda tk, *a, **k: _px
    D = S1.prepare()
    sd_idx = D["idx"]
    sd_lab = {f: d1_labels(band, sd_idx, f) for f in FREQS}
    sd_v = D["risk"][0.7]

    # DSS 데이터
    soxl = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    qmode = get_week_mode_map(ms, soxl.index)
    vrd = VR.vol_regime_days(soxx, soxl.index, "M", 0.7)
    dss_v = [d for d, r in vrd.items() if r == "위험"]
    dss_idx = soxl.index
    dss_lab = {f: d1_labels(band, dss_idx, f) for f in FREQS}
    end = str(soxl.index[-1].date())

    # 레짐 개요
    lab_d = pd.Series(sd_lab["일"], index=sd_idx)
    lab_m = pd.Series(sd_lab["월"], index=sd_idx)
    print(f"\n━━ D1 개요 ({sd_idx[0].date()} ~ {sd_idx[-1].date()}, SOXX 252일 고점 대비) — 현재 낙폭 {dd.iloc[-1]*100:.1f}%")
    for b in range(4):
        md_ = lab_d == b
        runs = (md_ != md_.shift()).cumsum()[md_]
        print(f"   {BAND_NAMES[b]:<14} 일 판정 {md_.mean()*100:5.1f}% (평균 {md_.sum()/max(1, runs.nunique()):.0f}일 연속) · "
              f"월 판정 {(lab_m == b).mean()*100:5.1f}% · 그중 V1 위험일 {sd_v[md_.values].mean()*100:.0f}%")
    print(f"   V1 위험일 중 D1 칸 분포(일 판정): " + " · ".join(
        f"{BAND_NAMES[b].split()[0]} {(lab_d[sd_v] == b).mean()*100:.0f}%" for b in range(4)))

    # ① 프로파일
    prof = []
    for lab in ("균형형", "공격형"):
        a = D["arr"][lab]
        n = len(sd_idx)
        for base, vm in (("", None), ("V", sd_v)):
            eq = pd.Series(sd_sim(a["c"], a["pc"], a["sg"], a["p"], *sd_arrays(n, vm, None, None)), index=sd_idx)
            if base == "":
                assert abs(eq.iloc[-1] - S1.sim(a["c"], a["pc"], a["sg"], None, a["p"], None)[-1]) < 1e-6
            else:
                ref = S1.sim(a["c"], a["pc"], a["sg"], sd_v, a["p"], (1.0, 100.0, 0.0, 0.0, 0.5))
                assert abs(eq.iloc[-1] - ref[-1]) < 1e-6, (eq.iloc[-1], ref[-1])
            # 노출: 표준편차 엔진으로 다시 돌려 예수금 확보
            r = E.run_backtest_stdev(_px[["Close"]], START, str(_px.index[-1].date()), sigma_period=2,
                                     k_buy=a["p"]["k_buy"], k_sell=a["p"]["k_sell"], sell_ratio=a["p"]["sell_ratio"],
                                     divisions=a["p"]["divisions"], renewal=a["p"]["renewal"], initial_capital=10000.0,
                                     vreg=E.sd_vreg("1") if base else None)
            cash = pd.Series(r["cash_series"], index=pd.to_datetime(r["dates"])).reindex(sd_idx)
            asset = pd.Series(r["assets"], index=pd.to_datetime(r["dates"])).reindex(sd_idx)
            prof += profile(f"표준편차 {lab}{base} (일 판정)", eq, 1 - cash / asset, lab_d.values)
    for base in ("", "V"):
        p = DSSParams(**VR.PRESETS["공격형"])
        _D.update(dict(qmode=qmode, dss_v=dss_v, dss_idx=dss_idx, dss_lab=dss_lab))
        dv, dh, _ = dss_dicts("V" if base else "", "일", "S10", None)
        bt = run_backtest(p, soxl, ms, START, end, day_divisions=dv, day_hold=dh)
        eq = pd.Series(bt["총자산"].values.astype(float), index=pd.to_datetime(bt["날짜"]))
        expo = pd.Series(bt["평가금"].values.astype(float), index=eq.index) / eq
        lab_x = pd.Series(dss_lab["일"], index=dss_idx).reindex(eq.index).values
        prof += profile(f"DSS 공격형{base} (일 판정)", eq, expo, lab_x)
    pd.DataFrame(prof).to_csv(os.path.join(HERE, "reports", "d1_drawdown_profile.csv"), index=False, encoding="utf-8-sig")

    # ② 본 검증
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(dict(sd=D["arr"], sd_idx=sd_idx, sd_lab=sd_lab, sd_v=sd_v, soxl=soxl, ms=ms, qmode=qmode,
                     dss_v=dss_v, dss_idx=dss_idx, dss_lab=dss_lab, end=end), tmp)
    tmp.close()
    items = []
    for base in ("", "V"):
        items.append(("DSS", base, "-", "S10", None))
        items += [("DSS", base, f, s, r) for f in FREQS for s in SETS for r in DSS_RULES]
        for pk in ("균형형", "공격형"):
            items.append(("SD", pk, base, "-", "S10", None))
            items += [("SD", pk, base, f, s, r) for f in FREQS for s in SETS for r in SD_RULES]
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        df = pd.DataFrame(pool.map(evaluate, items, chunksize=4))
    os.unlink(tmp.name)
    df.to_csv(os.path.join(HERE, "reports", "d1_drawdown_grid.csv"), index=False, encoding="utf-8-sig")
    f2 = lambda v: f"{v:.2f}"
    cols = ["판정", "구간", "규칙", "CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소"]
    keys = ["Calmar", "H1", "H2", "봉인", "구간최소"]
    for (st, pr), sub in df.groupby(["전략", "프리셋"], sort=False):
        b = sub[sub["기준"]].iloc[0]
        rest = sub[~sub["기준"]]
        ok = rest[np.logical_and.reduce([rest[k] > b[k] for k in keys])]
        print(f"\n##### {st} {pr} — 기준 {b['CAGR']:.1f}% / {b['MDD']:.1f}% / {b['Calmar']:.2f} "
              f"(H1 {b['H1']:.2f} H2 {b['H2']:.2f} 봉인 {b['봉인']:.2f} 구간최소 {b['구간최소']:.2f})"
              f" — 전 검증 통과 {len(ok)}/{len(rest)} · Calmar 만 초과 {int((rest['Calmar'] > b['Calmar']).sum())}/{len(rest)}")
        print("Calmar 상위 8:")
        print(rest.sort_values("Calmar", ascending=False)[cols].head(8).to_string(index=False, float_format=f2))
        if len(ok):
            print(f"[전 검증 통과 — 판정×구간별 {ok.groupby(['판정', '구간']).size().to_dict()}]")
            print(ok.sort_values("Calmar", ascending=False)[cols].head(10).to_string(index=False, float_format=f2))
    print(f"\n{len(df)} 조합, {time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
