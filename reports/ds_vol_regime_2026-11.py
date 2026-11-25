"""
듀얼스나이퍼 × 변동성 레짐 1단계 탐색 (2026-11-25)

레짐 = DSS 와 같은 정의 (dss_engine.vol_regime_table: 월말 SOXX 20일 변동성 상승 & 3년 백분위 ≥ 기준 → 다음 달 위험).
모드(공격/방어)는 원전략 실제 모드 그대로, 위험 달의 '매수 크기'만 바꾼다.

규칙 (위험 달에만 적용)
  M   매수 할당금 × 배율 (공격·방어 모두)
  MA  공격모드 날만 × 배율        MS  방어모드 날만 × 배율
  D   공격 분할수 변경 (균등)      SW  방어 티어 비중을 더 잘게 (7·8 티어 등차)
  D+SW 둘 다                       H   보유기한 (방어 보유일 / 공격 보유일 상한)
검증: 전체 · 반기 2개 · 봉인(2024~) · 구간 4개 최소 — 각각 새로 시작한 백테스트.
"""
import sys, os, itertools, pickle, tempfile, time
import warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import pandas as pd
import multiprocessing as mp

from dual_sniper_engine import (load_price_data, DualSniperParams, build_original_mode_map,
                                load_original_modes, run_backtest)

BASE_SF = dict(sf_divisions=5, sf_hold=8, sf_buy_pct1=-0.6, sf_sell_pct=0.7, sf_ma_base=3)
PRESETS = {
    "로케트셋": dict(BASE_SF, ag_divisions=6, ag_buy_pct=8.0, ag_sell_alpha=0.4, ag_hold_alpha=2.0,
                  sf_buy_pct2=5.5, sf_tier_weights=(6, 13, 20, 27, 34)),
    "전차셋": dict(BASE_SF, ag_divisions=5, ag_buy_pct=0.5, ag_sell_alpha=0.4, ag_hold_alpha=2.0,
                 sf_buy_pct2=5.5, sf_tier_weights=(6, 13, 20, 27, 34)),
    "헬기셋": dict(BASE_SF, ag_divisions=6, ag_buy_pct=0.5, ag_sell_alpha=0.4, ag_hold_alpha=2.0,
                 sf_buy_pct2=5.5, sf_tier_weights=(6, 13, 20, 27, 34)),
    "헬기셋개선": dict(BASE_SF, ag_divisions=8, ag_buy_pct=8.0, ag_sell_alpha=0.35, ag_hold_alpha=2.0,
                   sf_buy_pct2=6.0, sf_tier_weights=(3, 11, 20, 29, 37)),
    "최적안정": dict(BASE_SF, ag_divisions=10, ag_buy_pct=8.0, ag_sell_alpha=0.4, ag_hold_alpha=2.0,
                  sf_buy_pct2=5.5, sf_tier_weights=(3, 11, 20, 29, 37)),
}
SF_W = {7: (2, 6, 10, 14, 18, 23, 27), 8: (2, 5, 8, 11, 14, 17, 20, 23), 10: (1, 3, 5, 7, 9, 11, 13, 15, 17, 19)}
START = "2016-01-04"
WINDOWS = {"전체": (START, None), "H1": (START, "2021-06-01"), "H2": ("2021-06-01", None), "봉인": ("2024-01-02", None)}
SUBS = [(START, "2018-09-01"), ("2018-09-01", "2021-06-01"), ("2021-06-01", "2024-01-02"), ("2024-01-02", None)]

RULES = [("기준", None)]
RULES += [("M", m) for m in (0.0, 0.4, 0.5, 0.6, 0.75)]
RULES += [("MA", m) for m in (0.0, 0.5, 0.75)]
RULES += [("MS", m) for m in (0.0, 0.5, 0.75)]
RULES += [("D", d) for d in (8, 10, 12, 16)]
RULES += [("SW", k) for k in (7, 8, 10)]
RULES += [("D+SW", (10, 8)), ("D+SW", (12, 8)), ("D+SW", (12, 10)), ("D+SW", (16, 10))]
RULES += [("H", ("sf", 4)), ("H", ("sf", 12)), ("H", ("ag", 10)), ("H", ("ag", 15))]

_D = {}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))


def metric(p, s, e, kw):
    px = _D["px"] if e is None else _D["px"][_D["px"].index < pd.Timestamp(e)]
    r = run_backtest(px, p, mode_map=_D["mm"], start_date=s, light=True, **kw)
    eq = r["총자산"].values.astype(float)
    y = len(eq) / 252
    cg = (eq[-1] / 1e4) ** (1 / y) - 1
    peak, md = eq[0], 0.0
    for v in eq:
        peak = max(peak, v)
        md = min(md, v / peak - 1)
    md = abs(md) if md < 0 else 1e-9
    return cg, md, cg / md


def build_kw(rule, val, risk, mm):
    if rule == "기준":
        return {}
    if rule == "M":
        return {"day_buy_mult": {d: val for d in risk}}
    if rule == "MA":
        return {"day_buy_mult": {d: val for d in risk if mm.get(d) == "공격"}}
    if rule == "MS":
        return {"day_buy_mult": {d: val for d in risk if mm.get(d) == "방어"}}
    if rule == "D":
        return {"day_ag_div": {d: val for d in risk}}
    if rule == "SW":
        return {"day_sf_weights": {d: SF_W[val] for d in risk}}
    if rule == "D+SW":
        return {"day_ag_div": {d: val[0] for d in risk}, "day_sf_weights": {d: SF_W[val[1]] for d in risk}}
    if rule == "H":
        return {"day_sf_hold": {d: val[1] for d in risk}} if val[0] == "sf" \
            else {"day_ag_hold_cap": {d: val[1] for d in risk}}
    raise ValueError(rule)


def evaluate(item):
    preset, rp, rule, val = item
    p = DualSniperParams(**PRESETS[preset])
    risk = _D["risk"][rp]
    kw = build_kw(rule, val, risk, _D["mm"])
    if rule == "D" and val <= p.ag_divisions:
        return None
    out = dict(preset=preset, risk_pct=rp, rule=rule, val=str(val))
    for name, (s, e) in WINDOWS.items():
        cg, md, cm = metric(p, s, e, kw)
        if name == "전체":
            out.update(CAGR=cg * 100, MDD=-md * 100, Calmar=cm)
        else:
            out[name] = cm
    out["구간최소"] = min(metric(p, s, e, kw)[2] for s, e in SUBS)
    return out


def main():
    from dss_engine import vol_regime_table, load_regime_prices
    t0 = time.time()
    px = load_price_data("SOXL", "2009-01-01", "2027-01-01")
    mm = build_original_mode_map(px, extra_modes={})
    soxx = load_regime_prices()
    risk = {}
    for rp in (0.6, 0.7, 0.8):
        t = vol_regime_table(soxx, rp)
        months = {m for m, r in zip(t["적용월"], t["레짐"]) if r == "위험"}
        risk[rp] = [d for d in px.index if d.to_period("M") in months]
    n_m = {rp: len({d.to_period("M") for d in v if d >= pd.Timestamp(START)}) for rp, v in risk.items()}
    print("위험 달 수 (2016~):", n_m, "| 데이터 ~", px.index[-1].date())
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(dict(px=px, mm=mm, risk=risk), tmp)
    tmp.close()
    items = [(pr, rp, rule, val) for pr in PRESETS for rp in (0.6, 0.7, 0.8) for rule, val in RULES
             if not (rule == "기준" and rp != 0.7)]
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        rows = [r for r in pool.map(evaluate, items, chunksize=4) if r]
    os.unlink(tmp.name)
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(HERE, "reports", "ds_vol_regime_grid.csv"), index=False, encoding="utf-8-sig")
    f2 = lambda v: f"{v:.2f}"
    cols = ["rule", "val", "risk_pct", "CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소"]
    for pr in PRESETS:
        sub = df[df["preset"] == pr]
        b = sub[sub["rule"] == "기준"].iloc[0]
        ok = sub[(sub["rule"] != "기준") & (sub["Calmar"] > b["Calmar"]) & (sub["H1"] > b["H1"])
                 & (sub["H2"] > b["H2"]) & (sub["봉인"] > b["봉인"]) & (sub["구간최소"] > b["구간최소"])]
        print(f"\n##### {pr} — 기준 {b['CAGR']:.1f}% / {b['MDD']:.1f}% / {b['Calmar']:.2f} "
              f"(H1 {b['H1']:.2f} H2 {b['H2']:.2f} 봉인 {b['봉인']:.2f} 구간최소 {b['구간최소']:.2f})"
              f" — 전 검증 통과 {len(ok)}/{len(sub) - 1}")
        print(sub[sub["risk_pct"] == 0.7].sort_values("Calmar", ascending=False)[cols].head(14)
              .to_string(index=False, float_format=f2))
        if len(ok):
            print("  [전 검증 통과]")
            print(ok.sort_values("Calmar", ascending=False)[cols].head(10).to_string(index=False, float_format=f2))
    print(f"\n{len(df)} 조합, {time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
