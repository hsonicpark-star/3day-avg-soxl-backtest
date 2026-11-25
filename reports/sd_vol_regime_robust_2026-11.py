"""
표준편차매매 × 변동성 레짐 2단계 — 후보 규칙의 견고성 (2026-11-25)

1단계 공통 후보: 위험 달에 '매도 비율 100% + 보유 평가금이 전일 총자산의 cap 을 넘으면 신규 매수 안 함'.
  ① 주변값: cap 0.3~0.8 × 매도비율(프리셋 그대로/90/100) × 위험 기준 60/70/80%
  ② 상시 적용 대조: 같은 규칙을 '매일' 적용하면? (레짐 신호 없이도 같은 효과면 신호 가치 없음)
  ③ 무작위 달 대조: 위험 달과 같은 수의 달을 무작위로 뽑아 같은 규칙 적용 (300회)
  ④ 연도별 수익률·낙폭 비교 (기준 vs 후보)
"""
import sys, os, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import numpy as np
import pandas as pd
import importlib.util

_spec = importlib.util.spec_from_file_location("sd1", os.path.join(HERE, "reports", "sd_vol_regime_2026-11.py"))
S1 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(S1)
f2 = lambda v: f"{v:.2f}"


def row(pk, thr, rule):
    r = S1.evaluate((pk, thr, rule))
    return {k: r[k] for k in ("CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소")}


def passed(r, b):
    return all(r[k] > b[k] for k in ("Calmar", "H1", "H2", "봉인", "구간최소"))


def main():
    S1._D.update(S1.prepare())
    D = S1._D
    idx = D["idx"]
    presets = list(D["arr"])
    base = {pk: row(pk, 0.7, None) for pk in presets}

    # ① 주변값
    print("\n━━ ① 주변값 — Calmar (통과 = *)  행: 위험기준·매도비율 / 열: cap")
    caps = (0.3, 0.4, 0.5, 0.6, 0.7, 0.8, None)
    rows = []
    for pk in presets:
        b = base[pk]
        print(f"\n[{pk}] 기준 {b['CAGR']:.1f}% / {b['MDD']:.1f}% / {b['Calmar']:.2f}")
        for thr in (0.6, 0.7, 0.8):
            for sr in (None, 90.0, 100.0):
                cells = []
                for cap in caps:
                    if sr is None and cap is None:
                        cells.append("  기준 ")
                        continue
                    r = row(pk, thr, (1.0, sr, 0.0, 0.0, cap))
                    rows.append(dict(preset=pk, thr=thr, sr=sr, cap=cap, **r, 통과=passed(r, b)))
                    cells.append(f"{r['Calmar']:.2f}{'*' if passed(r, b) else ' '} ")
                print(f"  {int(thr*100)}% · 매도 {'그대로' if sr is None else int(sr):>4} | " + " ".join(cells)
                      + ("   ← cap " + " ".join(str(c) for c in caps) if thr == 0.6 and sr is None else ""))
    nb = pd.DataFrame(rows)
    nb.to_csv(os.path.join(HERE, "reports", "sd_vol_regime_neighbors.csv"), index=False, encoding="utf-8-sig")

    CAND = (1.0, 100.0, 0.0, 0.0, 0.5)
    print("\n━━ 후보 (위험 70% · 매도 100% · cap 0.5) vs 기준")
    out = []
    for pk in presets:
        r = row(pk, 0.7, CAND)
        out.append(dict(프리셋=pk, 구분="기준", **base[pk]))
        out.append(dict(프리셋=pk, 구분="후보", **r, 통과=passed(r, base[pk])))
    print(pd.DataFrame(out).to_string(index=False, float_format=f2))

    # ② 상시 적용
    print("\n━━ ② 같은 규칙을 매일 적용 (레짐 신호 없음)")
    risk_bak = D["risk"][0.7]
    D["risk"][0.7] = np.ones(len(idx), dtype=bool)
    for pk in presets:
        r = row(pk, 0.7, CAND)
        print(f"  {pk:12s} {r['CAGR']:.1f}% / {r['MDD']:.1f}% / Calmar {r['Calmar']:.2f}  (기준 {base[pk]['Calmar']:.2f})")
    D["risk"][0.7] = risk_bak

    # ③ 무작위 달
    print("\n━━ ③ 무작위 달 대조 (같은 달 수, 300회) — 후보 규칙의 전체 Calmar / MDD")
    months = idx.to_period("M")
    uniq = months.unique()
    n_risk = len({m for m, k in zip(months, risk_bak) if k})
    rng = np.random.default_rng(20261125)
    sets = [set(rng.choice(len(uniq), n_risk, replace=False)) for _ in range(300)]
    code = pd.Series(range(len(uniq)), index=uniq).reindex(months).values
    for pk in presets:
        real = row(pk, 0.7, CAND)
        cal, mdd = [], []
        for s in sets:
            D["risk"][0.7] = np.isin(code, list(s))
            cg, md, cm = S1.metric(pk, 0.7, S1.START, None, CAND)
            cal.append(cm); mdd.append(-md * 100)
        D["risk"][0.7] = risk_bak
        cal, mdd = np.array(cal), np.array(mdd)
        print(f"  {pk:12s} 실제 Calmar {real['Calmar']:.2f} (무작위 중앙 {np.median(cal):.2f}, 상위 5% {np.quantile(cal, .95):.2f}, "
              f"실제보다 좋은 비율 {np.mean(cal >= real['Calmar'])*100:.1f}%) | 실제 MDD {real['MDD']:.1f}% "
              f"(무작위 중앙 {np.median(mdd):.1f}%, 실제보다 얕은 비율 {np.mean(mdd >= real['MDD'])*100:.1f}%)")

    # ④ 연도별
    print("\n━━ ④ 연도별 수익률(%) / 연중 최대 낙폭(%) — 기준 → 후보")
    for pk in ("균형형", "공격형"):
        a = D["arr"][pk]
        eqs = {}
        for name, rule in (("기준", None), ("후보", CAND)):
            eqs[name] = pd.Series(S1.sim(a["c"], a["pc"], a["sg"], risk_bak, a["p"], rule), index=idx)
        yr = {}
        for name, eq in eqs.items():
            ye = eq.groupby(eq.index.year).last()
            ret = ye / ye.shift(1).fillna(1e4) - 1
            dd = (eq / eq.cummax() - 1).groupby(eq.index.year).min()
            yr[name + " 수익"] = ret * 100
            yr[name + " 낙폭"] = dd * 100
        t = pd.DataFrame(yr)
        t["위험달수"] = pd.Series(risk_bak, index=idx).groupby([idx.year, idx.month]).first().groupby(level=0).sum()
        t["수익차"] = t["후보 수익"] - t["기준 수익"]
        print(f"\n[{pk}] 후보가 나은 해 {int((t['수익차'] > 0).sum())}/{len(t)} · 낙폭이 얕아진 해 "
              f"{int((t['후보 낙폭'] > t['기준 낙폭'] + 0.05).sum())}/{len(t)}")
        print(t.to_string(float_format=lambda v: f"{v:.1f}"))

    # ⑤ 한 번의 폭락(2026-10~11)에 기대지 않는 지표
    print("\n━━ ⑤ 최대 낙폭 한 건에 덜 좌우되는 지표 — 기준 → 후보 (괄호: 무작위 300회 중 후보보다 좋은 비율)")

    def stats(eq):
        r = eq.pct_change().dropna()
        dd = eq / eq.cummax() - 1
        ep, cur = [], 0.0
        for v in dd.values:
            if v < -1e-9:
                cur = min(cur, v)
            elif cur < 0:
                ep.append(cur); cur = 0.0
        if cur < 0:
            ep.append(cur)
        return dict(샤프=r.mean() / r.std() * np.sqrt(252), 울서=np.sqrt((dd ** 2).mean()) * 100,
                    낙폭5평균=np.mean(sorted(ep)[:5]) * 100)

    cut = idx.searchsorted(pd.Timestamp("2026-01-01"))
    for pk in presets:
        a = D["arr"][pk]
        run = lambda rk, rule, n=None: pd.Series(S1.sim(a["c"][:n], a["pc"][:n], a["sg"][:n], rk[:n], a["p"], rule), index=idx[:n])
        sb, sc = stats(run(risk_bak, None)), stats(run(risk_bak, CAND))
        rnd = [stats(run(np.isin(code, list(s)), CAND)) for s in sets[:300]]
        p_sh = np.mean([x["샤프"] >= sc["샤프"] for x in rnd]) * 100
        p_ul = np.mean([x["울서"] <= sc["울서"] for x in rnd]) * 100
        p_d5 = np.mean([x["낙폭5평균"] >= sc["낙폭5평균"] for x in rnd]) * 100
        pre = []
        for rule in (None, CAND):
            eq = run(risk_bak, rule, cut)
            y = (eq.index[-1] - eq.index[0]).days / 365.25
            cg = (eq.iloc[-1] / 1e4) ** (1 / y) - 1
            md = (eq / eq.cummax() - 1).min()
            pre.append(f"{cg*100:.1f}%/{md*100:.1f}%/{cg/abs(md):.2f}")
        print(f"  {pk:12s} 샤프 {sb['샤프']:.2f}→{sc['샤프']:.2f} ({p_sh:.1f}%) · 울서지수 {sb['울서']:.1f}→{sc['울서']:.1f} ({p_ul:.1f}%)"
              f" · 큰 낙폭 5건 평균 {sb['낙폭5평균']:.1f}→{sc['낙폭5평균']:.1f} ({p_d5:.1f}%) | ~2025년 말: {pre[0]} → {pre[1]}")


if __name__ == "__main__":
    main()
