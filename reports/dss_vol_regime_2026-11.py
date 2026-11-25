"""
DSS 모드 판정에 SOXX 변동성 레짐 결합 실험 (2026-11-24)

변동성 레짐: SOXX 20일 변동성 × √252,
최근 756거래일 백분위 + 직전 판정 대비 방향.
  상승 & 백분위 ≥ 위험기준 → 위험 / 하락 & 백분위 < 50% → 안정 / 그 외 → 승부
판정 주기: 월간(월말 → 다음 달) / 주간(금요일 → 다음 주). 장 전 확정 정보만 사용.

결합 규칙
  R1 위험이면 안전(SF) 강제, 나머지는 QQQ 모드
  R2 R1 + 안정이면 공세(AG) 강제
  R3 변동성만 사용: 위험→SF, 안정·승부→AG
  R4 변동성만 사용: 승부→AG, 안정·위험→SF
  R5 모드는 QQQ 그대로, 위험이면 신규 매수 배율 (0 = 중단, 0.5 = 절반)
"""
import sys, os, io, pickle, tempfile, itertools, time
import warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import numpy as np
import pandas as pd
import multiprocessing as mp
from dss_engine import (load_price_data, build_weekly_rsi_series, build_mode_series,
                        run_backtest_fast, DSSParams, parse_tier_weights, get_week_mode_map)

AGG = dict(sf_max_hold=38, sf_buy_pct=0.033, sf_sell_pct=0.018, ag_max_hold=8,
           ag_buy_pct=0.0315, ag_sell_pct=0.0375, initial_capital=10000.0,
           fee_rate=0.0004, renewal_period=10, pcr=0.75, lcr=0.20)
PRESETS = {
    "공격형": dict(AGG, sf_divisions=7, ag_divisions=7),
    "보초형W": dict(AGG, sf_divisions=8, ag_divisions=8,
                  sf_tier_weights=parse_tier_weights("0,4,1.25,1.25,1.25,1.25,1.25,1"),
                  ag_tier_weights=parse_tier_weights("0,4,1.25,1.25,1.25,1.25,1.25,1")),
    "양면형W": dict(AGG, sf_divisions=8, ag_divisions=7,
                  sf_tier_weights=parse_tier_weights("0,2.5,1,1.188,1.375,1.562,1.75,0.5"),
                  ag_tier_weights=parse_tier_weights("0,4.75,0.25,0.417,0.583,0.75,2.25")),
    "철벽형W": dict(sf_divisions=8, sf_max_hold=42, sf_buy_pct=0.028, sf_sell_pct=0.019,
                  ag_divisions=7, ag_max_hold=6, ag_buy_pct=0.0355, ag_sell_pct=0.049,
                  initial_capital=10000.0, fee_rate=0.0004, renewal_period=17, pcr=0.65, lcr=0.20,
                  sf_tier_weights=parse_tier_weights("0,3.5,1.5,1.562,1.625,1.688,1.75,0.5"),
                  ag_tier_weights=parse_tier_weights("0,4,0.5,0.667,0.833,1,2.5")),
}
_D = {}


def vol_regime_days(soxx, trade_idx, freq, risk_pct, safe_pct=0.50, vol_win=20, pct_win=756):
    """거래일 → 변동성 레짐 (판정 시점 이후 다음 기간에 적용)."""
    vol = (soxx.pct_change().rolling(vol_win).std() * np.sqrt(252)).dropna()
    v = vol.values
    pct = pd.Series([np.mean(v[max(0, i - pct_win + 1): i + 1] < v[i]) for i in range(len(v))],
                    index=vol.index)
    key = vol.index.to_period("M") if freq == "M" else vol.index.to_period("W-FRI")
    ends = vol.groupby(key).tail(1)
    reg_by_period, prev = {}, None
    for d, val in ends.items():
        if prev is not None:
            up, p = val > prev, pct[d]
            r = "위험" if (up and p >= risk_pct) else ("안정" if ((not up) and p < safe_pct) else "승부")
            per = (d.to_period("M") + 1) if freq == "M" else (d.to_period("W-FRI") + 1)
            reg_by_period[per] = r
        prev = val
    out = {}
    for d in trade_idx:
        per = d.to_period("M") if freq == "M" else d.to_period("W-FRI")
        if per in reg_by_period:
            out[d] = reg_by_period[per]
    return out


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))


def m(p, s, e, dm, bm):
    r = run_backtest_fast(p, _D["soxl"], _D["ms"], s, e, day_modes=dm, day_buy_mult=bm)
    y = r["total_days"] / 252
    cg = (r["final_asset"] / 1e4) ** (1 / y) - 1
    md = abs(r["mdd"])
    return cg, md, cg / md


def evaluate(item):
    preset, freq, rule, rp, mult = item
    p = DSSParams(**PRESETS[preset])
    dm = bm = None
    if rule != "기준":
        vr = _D["vr"][(freq, rp)]
        qm = _D["qmode"]
        if rule == "R1":
            dm = {d: ("SF" if r == "위험" else qm[d]) for d, r in vr.items()}
        elif rule == "R2":
            dm = {d: ("SF" if r == "위험" else "AG" if r == "안정" else qm[d]) for d, r in vr.items()}
        elif rule == "R3":
            dm = {d: ("SF" if r == "위험" else "AG") for d, r in vr.items()}
        elif rule == "R4":
            dm = {d: ("AG" if r == "승부" else "SF") for d, r in vr.items()}
        elif rule == "R5":
            bm = {d: (mult if r == "위험" else 1.0) for d, r in vr.items()}
    E = _D["end"]
    f = m(p, "2010-03-12", E, dm, bm)
    h1 = m(p, "2010-03-12", "2018-03-12", dm, bm)
    h2 = m(p, "2018-03-12", E, dm, bm)
    ho = m(p, "2024-01-02", E, dm, bm)
    subs = [m(p, s, e, dm, bm)[2] for s, e in (("2010-03-12", "2014-03-12"), ("2014-03-12", "2018-03-12"),
                                                ("2018-03-12", "2022-03-12"), ("2022-03-12", E))]
    return {"preset": preset, "freq": freq if rule != "기준" else "-", "rule": rule,
            "risk_pct": rp if rule != "기준" else "-", "mult": mult if rule == "R5" else "-",
            "CAGR": f[0] * 100, "MDD": -f[1] * 100, "Calmar": f[2], "H1": h1[2], "H2": h2[2],
            "봉인": ho[2], "구간최소": min(subs)}


def main():
    soxl = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    soxx = load_price_data("SOXX", "2006-12-01", "2026-12-31")["Close"].astype(float)
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    idx = soxl.index
    qmode = get_week_mode_map(ms, idx)
    vr = {(fq, rp): vol_regime_days(soxx, idx, fq, rp) for fq in ("M", "W") for rp in (0.3, 0.5, 0.7)}
    data = dict(soxl=soxl, ms=ms, qmode=qmode, vr=vr, end=str(idx[-1].date()))
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(data, tmp)
    tmp.close()
    items = []
    for pr in PRESETS:
        items.append((pr, "-", "기준", None, None))
        for fq, rp in itertools.product(("M", "W"), (0.3, 0.5, 0.7)):
            for rule in ("R1", "R2", "R3", "R4"):
                items.append((pr, fq, rule, rp, None))
            for mult in (0.0, 0.5):
                items.append((pr, fq, "R5", rp, mult))
    t0 = time.time()
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        df = pd.DataFrame(pool.map(evaluate, items))
    os.unlink(tmp.name)
    df.to_csv(os.path.join(HERE, "reports", "dss_vol_regime_grid.csv"), index=False, encoding="utf-8-sig")
    out = [f"# DSS × 변동성 레짐 ({len(items)}개 조합, {time.time() - t0:.0f}초, 데이터 ~{data['end']})", ""]
    f2 = lambda v: f"{v:.2f}"
    cols = ["preset", "freq", "rule", "risk_pct", "mult", "CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소"]
    for pr in PRESETS:
        sub = df[df["preset"] == pr]
        ref = sub[sub["rule"] == "기준"].iloc[0]
        beat = sub[(sub["rule"] != "기준") & (sub["H1"] > ref["H1"]) & (sub["H2"] > ref["H2"])
                   & (sub["봉인"] > ref["봉인"]) & (sub["구간최소"] > ref["구간최소"])]
        out += [f"## {pr} — 기준: CAGR {ref.CAGR:.1f}% / MDD {ref.MDD:.1f}% / Calmar {ref.Calmar:.2f} "
                f"(반기 {ref.H1:.2f}/{ref.H2:.2f}, 봉인 {ref.봉인:.2f}, 구간최소 {ref.구간최소:.2f})",
                f"- 반기 둘 다·봉인·구간최소 모두 기준 초과: {len(beat)} / {len(sub) - 1}", "```",
                sub.sort_values("Calmar", ascending=False)[cols].head(8).to_string(index=False, float_format=f2),
                "```", ""]
        if len(beat):
            out += ["모든 검증 통과 조합:", "```", beat[cols].to_string(index=False, float_format=f2), "```", ""]
    txt = "\n".join(out)
    with open(os.path.join(HERE, "reports", "dss_vol_regime_result.md"), "w", encoding="utf-8") as fh:
        fh.write(txt)
    sys.stdout.buffer.write(txt.encode("utf-8"))


if __name__ == "__main__":
    main()
