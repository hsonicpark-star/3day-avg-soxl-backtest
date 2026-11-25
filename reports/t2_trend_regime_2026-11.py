"""
T2 추세 레짐 (200일선 이격도 구간, TQ200 판정) × DSS · 표준편차매매 (2026-11-25) — 레짐_카탈로그.md 의 T2

이격도 = 종가 ÷ 200일 이동평균 − 1,  기울기 = 200MA − 5일 전 200MA
기준 종목 3종
  SOXL (3배, TQ200 원래 경계)  : 이탈 < −1% · 경계 −1~+1% · 진입 +1~+10% · 확장 +10~+50% · 과열 > +50%
  TQQQ (3배, TQ200 원신호 그대로): 같은 경계
  SOXX (1배, 경계를 줄임)        : 이탈 < −1% · 경계 −1~+1% · 진입 +1~+5% · 확장 +5~+15% · 과열 > +15%
적용 구간 (그 상태인 날에만 규칙 적용)
  DN 이탈 · DNB 이탈+경계 · IN 진입 · EX 확장+과열 · OH 과열 · SLN 기울기 음수
  NOHOLD / HOLD — TQ200 모델(백테스트)이 비보유 / 보유 중 (SOXL · TQQQ 만)
판정 '일' = 전일 상태 → 오늘 / '월' = 월말 상태 → 다음 달

① 프로파일: 칸별 일평균 수익 · 일샤프 · 평균 노출
② 본 검증: 기본 프리셋 / V 프리셋 위 — 전체 · 앞/뒤 절반 · 봉인(2024~) · 4구간 최소 모두 기준 초과면 통과
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
                        run_backtest, run_backtest_fast, get_week_mode_map, load_regime_prices)
import tq200_engine as TQ

START = D1.START
EDGES = {"SOXL": (-0.01, 0.01, 0.10, 0.50), "TQQQ": (-0.01, 0.01, 0.10, 0.50), "SOXX": (-0.01, 0.01, 0.05, 0.15)}
BANDS = ["이탈", "경계", "진입", "확장", "과열"]
BSETS = {"DN": (0,), "DNB": (0, 1), "IN": (2,), "EX": (3, 4), "OH": (4,)}
FREQS = ("일", "월")
SD_RULES, DSS_RULES = D1.SD_RULES, D1.DSS_RULES
_D = {}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))
    D1._D.update(_D)


def to_idx(state, trade_idx, freq):
    """상태 Series(기준 종목 날짜) → 거래일 배열. 판정 이후에만 적용, 미판정 = NaN."""
    state = state.dropna()
    if freq == "일":
        return state.shift(1).dropna().reindex(trade_idx, method="ffill").values.astype(float)
    ends = state.groupby(state.index.to_period("M")).tail(1)
    mp_ = {d.to_period("M") + 1: float(v) for d, v in ends.items()}
    return np.array([mp_.get(d.to_period("M"), np.nan) for d in trade_idx])


def build_states(close, edges, ohlc=None):
    """{이름: 상태 Series} — band(0~4), slope_neg(0/1), hold(0/1, TQ200 모델)."""
    ma = close.rolling(200).mean()
    disp = close / ma - 1.0
    slope = ma - ma.shift(5)
    e = edges
    band = pd.Series(np.select([disp < e[0], disp < e[1], disp < e[2], disp <= e[3]], [0, 1, 2, 3], 4),
                     index=close.index, dtype=float).where(disp.notna())
    out = {"band": band, "sln": (slope < 0).astype(float).where(slope.notna())}
    if ohlc is not None:
        lg = TQ.run_backtest(ohlc[["Open", "Close"]], 10000.0)["log"]
        out["hold"] = (lg["보유수량"] > 0).astype(float)
    return out, disp


def masks_for(states, trade_idx):
    """{(구간, 판정): bool 배열}"""
    out = {}
    for f in FREQS:
        b = to_idx(states["band"], trade_idx, f)
        for k, v in BSETS.items():
            out[(k, f)] = np.isin(b, v)
        out[("SLN", f)] = to_idx(states["sln"], trade_idx, f) == 1.0
        if "hold" in states:
            h = to_idx(states["hold"], trade_idx, f)
            out[("NOHOLD", f)] = h == 0.0
            out[("HOLD", f)] = h == 1.0
    return out


# ── 평가 ──
def sd_eval(item):
    pk, base, key, rule = item
    n = len(_D["sd_idx"])
    arrs = D1.sd_arrays(n, _D["sd_v"] if base == "V" else None, _D["sd_mask"][key] if rule is not None else None, rule)
    cg, md, cm = D1.sd_metric(pk, START, None, arrs)
    out = dict(전략="표준편차", 프리셋=pk + base, 기준종목=key[0] if rule else "-", 구간=key[1] if rule else "-",
               판정=key[2] if rule else "-",
               규칙="기준" if rule is None else f"배율{rule[0]}·매도{rule[1] or '-'}·상한{rule[2] or '-'}",
               기준=rule is None, CAGR=cg * 100, MDD=-md * 100, Calmar=cm)
    for k, (s, e) in D1.SD_WIN.items():
        out[k] = D1.sd_metric(pk, s, e, arrs)[2]
    out["구간최소"] = min(D1.sd_metric(pk, s, e, arrs)[2] for s, e in D1.SD_SUBS)
    return out


def dss_eval(item):
    base, key, rule = item
    dv, dh, bm = {}, {}, {}
    qm = _D["qmode"]
    if base == "V":
        for d in _D["dss_v"]:
            dv[d] = 12
            if qm.get(d) == "AG":
                dh[d] = 5
    if rule is not None:
        div, mult, agh = rule
        for d, on in zip(_D["dss_idx"], _D["dss_mask"][key]):
            if on:
                if div is not None:
                    dv[d] = div
                if mult != 1.0:
                    bm[d] = mult
                if agh is not None and qm.get(d) == "AG":
                    dh[d] = agh
    a = (dv or None, dh or None, bm or None)
    cg, md, cm = D1.dss_metric(START, None, *a)
    out = dict(전략="DSS", 프리셋="공격형" + base, 기준종목=key[0] if rule else "-", 구간=key[1] if rule else "-",
               판정=key[2] if rule else "-",
               규칙="기준" if rule is None else f"분할{rule[0] or '-'}·배율{rule[1]}·공세보유{rule[2] or '-'}",
               기준=rule is None, CAGR=cg * 100, MDD=-md * 100, Calmar=cm)
    for k, (s, e) in D1.DSS_WIN.items():
        out[k] = D1.dss_metric(s, e, *a)[2]
    out["구간최소"] = min(D1.dss_metric(s, e, *a)[2] for s, e in D1.DSS_SUBS)
    return out


def evaluate(item):
    return sd_eval(item[1:]) if item[0] == "SD" else dss_eval(item[1:])


def profile(name, eq, expo, lab, names):
    r = eq.pct_change().fillna(0.0)
    rows = []
    for b, nm in enumerate(names):
        m = lab == b
        if m.sum() < 20:
            continue
        rows.append({"칸": nm, "일수": int(m.sum()), "비율(%)": m.mean() * 100, "일평균(%)": r[m].mean() * 100,
                     "일변동(%)": r[m].std() * 100, "일샤프": r[m].mean() / r[m].std(), "평균노출(%)": expo[m].mean() * 100})
    print(f"   · {name}")
    print("     " + pd.DataFrame(rows).to_string(index=False, float_format=lambda v: f"{v:.2f}").replace("\n", "\n     "))
    return [dict(전략=name, **x) for x in rows]


def main():
    t0 = time.time()
    import update_preset_stats as U
    os.chdir(HERE)
    soxx = load_regime_prices()
    _px = U.load_adjusted("SOXL")
    U.load_adjusted = lambda tk, *a, **k: _px
    D = D1.S1.prepare()
    sd_idx, sd_v = D["idx"], D["risk"][0.7]

    soxl = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    tqqq = load_price_data("TQQQ", "2009-06-01", "2026-12-31")
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    qmode = get_week_mode_map(ms, soxl.index)
    dss_idx = soxl.index
    dss_v = [d for d, r in D1.VR.vol_regime_days(soxx, dss_idx, "M", 0.7).items() if r == "위험"]
    end = str(dss_idx[-1].date())

    states, disp = {}, {}
    states["SOXL"], disp["SOXL"] = build_states(soxl["Close"].astype(float), EDGES["SOXL"], soxl)
    states["TQQQ"], disp["TQQQ"] = build_states(tqqq["Close"].astype(float), EDGES["TQQQ"], tqqq)
    states["SOXX"], disp["SOXX"] = build_states(soxx, EDGES["SOXX"])
    sd_mask = {(bs,) + k: v for bs in states for k, v in masks_for(states[bs], sd_idx).items()}
    dss_mask = {(bs,) + k: v for bs in states for k, v in masks_for(states[bs], dss_idx).items()}

    print(f"\n━━ T2 개요 ({sd_idx[0].date()} ~ {sd_idx[-1].date()})")
    for bs in states:
        b = to_idx(states[bs]["band"], sd_idx, "일")
        ok = ~np.isnan(b)
        line = " · ".join(f"{BANDS[i]} {(b[ok] == i).mean()*100:.0f}%" for i in range(5))
        extra = ""
        if "hold" in states[bs]:
            h = to_idx(states[bs]["hold"], sd_idx, "일")
            extra = f" | TQ200 모델 보유일 {np.nanmean(h)*100:.0f}%"
        ch = int((pd.Series(b[ok]).diff() != 0).sum())
        print(f"   {bs}: {line} | 칸 평균 {ok.sum()/max(1, ch):.0f}일 지속 | 현재 이격도 {disp[bs].iloc[-1]*100:.1f}%"
              f" | V1 위험일 중 이탈 {(b[ok & sd_v] == 0).mean()*100:.0f}%{extra}")

    # ① 프로파일
    prof = []
    eqs = {}
    n = len(sd_idx)
    for lab in ("균형형", "공격형"):
        a = D["arr"][lab]
        eq = pd.Series(D1.sd_sim(a["c"], a["pc"], a["sg"], a["p"], *D1.sd_arrays(n, None, None, None)), index=sd_idx)
        import stdev_engine as E
        r = E.run_backtest_stdev(_px[["Close"]], START, str(_px.index[-1].date()), sigma_period=2, k_buy=a["p"]["k_buy"],
                                 k_sell=a["p"]["k_sell"], sell_ratio=a["p"]["sell_ratio"], divisions=a["p"]["divisions"],
                                 renewal=a["p"]["renewal"], initial_capital=10000.0)
        cash = pd.Series(r["cash_series"], index=pd.to_datetime(r["dates"])).reindex(sd_idx)
        asset = pd.Series(r["assets"], index=pd.to_datetime(r["dates"])).reindex(sd_idx)
        eqs[f"표준편차 {lab}"] = (eq, 1 - cash / asset)
    bt = run_backtest(DSSParams(**D1.VR.PRESETS["공격형"]), soxl, ms, START, end)
    eq = pd.Series(bt["총자산"].values.astype(float), index=pd.to_datetime(bt["날짜"]))
    eqs["DSS 공격형"] = (eq, pd.Series(bt["평가금"].values.astype(float), index=eq.index) / eq)
    for bs in states:
        print(f"\n━━ 프로파일 — 기준 {bs} (일 판정)")
        for nm, (eq, expo) in eqs.items():
            b = to_idx(states[bs]["band"], eq.index, "일")
            prof += profile(f"{nm} | {bs} 이격도 칸", eq, expo, b, BANDS)
            if "hold" in states[bs]:
                h = to_idx(states[bs]["hold"], eq.index, "일")
                prof += profile(f"{nm} | {bs} TQ200 모델", eq, expo, h, ["비보유", "보유"])
    pd.DataFrame(prof).to_csv(os.path.join(HERE, "reports", "t2_trend_profile.csv"), index=False, encoding="utf-8-sig")

    # ② 본 검증
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(dict(sd=D["arr"], sd_idx=sd_idx, sd_v=sd_v, sd_mask=sd_mask, soxl=soxl, ms=ms, qmode=qmode,
                     dss_v=dss_v, dss_idx=dss_idx, dss_mask=dss_mask, end=end), tmp)
    tmp.close()
    keys = [k for k in sd_mask if sd_mask[k].mean() >= 0.02]      # 거의 안 뜨는 구간 제외
    skipped = [k for k in sd_mask if k not in keys]
    items = []
    for base in ("", "V"):
        items.append(("DSS", base, None, None))
        items += [("DSS", base, k, r) for k in keys for r in DSS_RULES]
        for pk in ("균형형", "공격형"):
            items.append(("SD", pk, base, None, None))
            items += [("SD", pk, base, k, r) for k in keys for r in SD_RULES]
    print(f"\n적용 구간 {len(keys)}종 (일수 2% 미만이라 제외: {skipped}) · {len(items)} 조합 실행")
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        df = pd.DataFrame(pool.map(evaluate, items, chunksize=4))
    os.unlink(tmp.name)
    df["적용일(%)"] = [np.nan if r["기준"] else sd_mask[(r["기준종목"], r["구간"], r["판정"])].mean() * 100 for _, r in df.iterrows()]
    df.to_csv(os.path.join(HERE, "reports", "t2_trend_grid.csv"), index=False, encoding="utf-8-sig")
    f2 = lambda v: f"{v:.2f}"
    cols = ["기준종목", "구간", "판정", "적용일(%)", "규칙", "CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소"]
    ks = ["Calmar", "H1", "H2", "봉인", "구간최소"]
    for (st, pr), sub in df.groupby(["전략", "프리셋"], sort=False):
        b = sub[sub["기준"]].iloc[0]
        rest = sub[~sub["기준"]]
        ok = rest[np.logical_and.reduce([rest[k] > b[k] for k in ks])]
        print(f"\n##### {st} {pr} — 기준 {b['CAGR']:.1f}% / {b['MDD']:.1f}% / {b['Calmar']:.2f} "
              f"(H1 {b['H1']:.2f} H2 {b['H2']:.2f} 봉인 {b['봉인']:.2f} 구간최소 {b['구간최소']:.2f})"
              f" — 전 검증 통과 {len(ok)}/{len(rest)} · Calmar 만 초과 {int((rest['Calmar'] > b['Calmar']).sum())}/{len(rest)}")
        print("Calmar 상위 6:")
        print(rest.sort_values("Calmar", ascending=False)[cols].head(6).to_string(index=False, float_format=f2))
        if len(ok):
            print(f"[전 검증 통과 — 기준종목×구간×판정별 {ok.groupby(['기준종목', '구간', '판정']).size().to_dict()}]")
            print(ok.sort_values("Calmar", ascending=False)[cols].head(10).to_string(index=False, float_format=f2))
    print(f"\n{len(df)} 조합, {time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
