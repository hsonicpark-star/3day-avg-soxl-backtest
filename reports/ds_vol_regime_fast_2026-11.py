"""
듀얼스나이퍼 × 변동성 레짐 2단계 — 더 빠른 신호 (2026-11-25)

1단계(월간 판정)는 효과 없음 → 판정 주기를 줄여 본다. 전부 '장 전 확정 정보'만 사용.
  M   월간 (월말 판정 → 다음 달)                      ← 1단계와 동일, 비교용
  W   주간 (금요일 판정 → 다음 주): 변동성 상승 & 3년 백분위 ≥ 기준
  D   일간 수준: 전일 변동성 백분위 ≥ 기준
  DR  일간 수준+상승: 전일 백분위 ≥ 기준 & 전일 변동성 > 5거래일 전 변동성
  S5  단기 변동성: 전일 5일 변동성의 3년 백분위 ≥ 기준 (더 빠른 반응)
위험 날의 규칙: M(전체 배율) / MS(방어모드 날만 배율) × 0 · 0.5 · 0.75
"""
import sys, os, pickle, tempfile, time
import warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import numpy as np
import pandas as pd
import multiprocessing as mp
import importlib.util

_spec = importlib.util.spec_from_file_location("ds1", os.path.join(HERE, "reports", "ds_vol_regime_2026-11.py"))
S1 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(S1)
from dual_sniper_engine import load_price_data, DualSniperParams, build_original_mode_map

PRESETS = {k: S1.PRESETS[k] for k in ("로케트셋", "헬기셋개선", "최적안정")}
_D = S1._D


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))


def pct_rank(v, win=756):
    return np.array([np.mean(v[max(0, i - win + 1): i + 1] < v[i]) for i in range(len(v))])


def build_signals(soxx, idx):
    """{(신호, 기준): 위험 거래일 리스트} — 모두 전일(또는 직전 판정일)까지의 정보."""
    ret = soxx.pct_change()
    out = {}
    vol20 = (ret.rolling(20).std() * np.sqrt(252)).dropna()
    p20 = pd.Series(pct_rank(vol20.values), index=vol20.index)
    vol5 = (ret.rolling(5).std() * np.sqrt(252)).dropna()
    p5 = pd.Series(pct_rank(vol5.values), index=vol5.index)
    for thr in (0.6, 0.7, 0.8, 0.9):
        # 월간 / 주간
        for freq, key in (("M", "M"), ("W", "W-FRI")):
            ends = vol20.groupby(vol20.index.to_period(key)).tail(1)
            risk_per, prev = set(), None
            for d, val in ends.items():
                if prev is not None and val > prev and p20[d] >= thr:
                    risk_per.add(d.to_period(key) + 1)
                prev = val
            out[(freq, thr)] = [d for d in idx if d.to_period(key) in risk_per]
        # 일간 (전일 값)
        lvl = (p20 >= thr).shift(1)
        rising = (vol20 > vol20.shift(5)).shift(1)
        s5 = (p5 >= thr).shift(1)
        out[("D", thr)] = [d for d in idx if bool(lvl.get(d, False))]
        out[("DR", thr)] = [d for d in idx if bool(lvl.get(d, False)) and bool(rising.get(d, False))]
        out[("S5", thr)] = [d for d in idx if bool(s5.get(d, False))]
    return out


def evaluate(item):
    preset, sig, thr, rule, val = item
    p = DualSniperParams(**PRESETS[preset])
    risk = _D["sig"][(sig, thr)] if sig != "-" else []
    kw = S1.build_kw(rule, val, risk, _D["mm"])
    out = dict(preset=preset, sig=sig, thr=thr, rule=rule, val=str(val), 위험일비율=len(risk) / _D["n_days"])
    for name, (s, e) in S1.WINDOWS.items():
        cg, md, cm = S1.metric(p, s, e, kw)
        if name == "전체":
            out.update(CAGR=cg * 100, MDD=-md * 100, Calmar=cm)
        else:
            out[name] = cm
    out["구간최소"] = min(S1.metric(p, s, e, kw)[2] for s, e in S1.SUBS)
    return out


def main():
    from dss_engine import load_regime_prices
    t0 = time.time()
    px = load_price_data("SOXL", "2009-01-01", "2027-01-01")
    mm = build_original_mode_map(px, extra_modes={})
    idx = px.index[px.index >= pd.Timestamp(S1.START)]
    sig = build_signals(load_regime_prices(), idx)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(dict(px=px, mm=mm, sig=sig, n_days=len(idx)), tmp)
    tmp.close()
    items = [(pr, "-", 0, "기준", None) for pr in PRESETS]
    items += [(pr, s, thr, rule, val) for pr in PRESETS for s in ("M", "W", "D", "DR", "S5")
              for thr in (0.6, 0.7, 0.8, 0.9) for rule in ("M", "MS") for val in (0.0, 0.5, 0.75)]
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        df = pd.DataFrame(pool.map(evaluate, items, chunksize=4))
    os.unlink(tmp.name)
    df.to_csv(os.path.join(HERE, "reports", "ds_vol_regime_fast.csv"), index=False, encoding="utf-8-sig")
    f2 = lambda v: f"{v:.2f}"
    cols = ["sig", "thr", "rule", "val", "위험일비율", "CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소"]
    for pr in PRESETS:
        sub = df[df["preset"] == pr]
        b = sub[sub["rule"] == "기준"].iloc[0]
        ok = sub[(sub["rule"] != "기준") & (sub["Calmar"] > b["Calmar"]) & (sub["H1"] > b["H1"])
                 & (sub["H2"] > b["H2"]) & (sub["봉인"] > b["봉인"]) & (sub["구간최소"] > b["구간최소"])]
        print(f"\n##### {pr} — 기준 {b['CAGR']:.1f}% / {b['MDD']:.1f}% / {b['Calmar']:.2f} "
              f"(H1 {b['H1']:.2f} H2 {b['H2']:.2f} 봉인 {b['봉인']:.2f} 구간최소 {b['구간최소']:.2f})"
              f" — 전 검증 통과 {len(ok)}/{len(sub) - 1}")
        print("Calmar 상위 10:")
        print(sub.sort_values("Calmar", ascending=False)[cols].head(10).to_string(index=False, float_format=f2))
        print("신호별 최고 Calmar / 그때 MDD:")
        g = sub[sub["rule"] != "기준"].sort_values("Calmar", ascending=False).groupby("sig").head(1)
        print(g[cols].to_string(index=False, float_format=f2))
        if len(ok):
            print("[전 검증 통과]")
            print(ok.sort_values("Calmar", ascending=False)[cols].head(8).to_string(index=False, float_format=f2))
    print(f"\n{len(df)} 조합, {time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
