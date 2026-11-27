"""
DSS-N — 2025년까지만 맞추고 2026년을 처음 보는 구간으로 (2026-11-27)

dssn_opt_2026-11.py 와 같은 탐색(같은 손잡이 · 같은 목적 함수 · 같은 탐색량)을 2011-01-03 ~ 2025-12-31 에만 적용.
2026-01-02 ~ 현재는 탐색이 한 번도 보지 못한 구간.
비교: 공격형 · 공격형V · 2026년까지 통으로 맞춘 값(dssn_opt_params.json 의 N-전체 · P-전체)
"""
import os, sys, time, json, pickle, tempfile, importlib.util, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "reports"))
import numpy as np
import pandas as pd
import multiprocessing as mp
import dssn_sim as S

_sp = importlib.util.spec_from_file_location("dssn_opt", os.path.join(HERE, "reports", "dssn_opt_2026-11.py"))
OPT = importlib.util.module_from_spec(_sp)
sys.modules["dssn_opt"] = OPT
_sp.loader.exec_module(OPT)
T0, FIT_END, TEST = "2011-01-03", "2025-12-31", "2026-01-02"


def row(ctx, name, P):
    idx = ctx["idx"]
    fit = S.metric(ctx, P, T0, FIT_END)
    full = S.metric(ctx, P, T0)
    i0 = int(idx.searchsorted(pd.Timestamp(TEST)))
    g, mdd, n = S.sim(ctx, P, i0, len(idx) - 1)
    yr = S.yearly(ctx, P, T0)
    return dict(이름=name, **{"맞춘구간 CAGR": fit[0], "맞춘구간 MDD": fit[1], "맞춘구간 Calmar": fit[2],
                "2026 수익률(단독)": (g - 1) * 100, "2026 MDD(단독)": mdd * 100,
                "2026 수익률(이어서)": float(yr.loc[2026, "수익률"]), "2026 MDD(이어서)": float(yr.loc[2026, "MDD"]),
                "전체 CAGR": full[0], "전체 MDD": full[1], "전체 Calmar": full[2]})


def main():
    t0 = time.time()
    ctx, df, ms = S.load_soxl()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(ctx, tmp); tmp.close()
    runs = {}
    with mp.Pool(7, initializer=OPT.init_worker, initargs=(tmp.name,)) as pool:
        for name, space in (("N-2025", OPT.SPACE_P + OPT.SPACE_N), ("P-2025", OPT.SPACE_P)):
            for i, seed in enumerate((1, 2, 3)):
                P, sc = OPT.search(pool, space, T0, FIT_END, seed)
                runs[f"{name}#{i + 1}"] = P
                print(f"   {name}#{i + 1} 점수 {sc:.1f}  {time.time() - t0:.0f}초", flush=True)
    os.unlink(tmp.name)
    with open(os.path.join(HERE, "reports", "dssn_opt_2025_params.json"), "w", encoding="utf-8") as f:
        json.dump(runs, f, ensure_ascii=False, indent=1)
    old = json.load(open(os.path.join(HERE, "reports", "dssn_opt_params.json"), encoding="utf-8"))
    rows = [row(ctx, "공격형", dict(S.BASE, v=0)), row(ctx, "공격형V", dict(S.BASE))]
    rows += [row(ctx, n + " (2026까지 맞춤)", P) for n, P in old.items() if "전체" in n]
    rows += [row(ctx, n + " (2025까지 맞춤)", P) for n, P in runs.items()]
    d = pd.DataFrame(rows)
    print(d.to_string(index=False, float_format=lambda v: f"{v:.2f}"))
    d.to_csv(os.path.join(HERE, "reports", "dssn_opt_2025.csv"), index=False, encoding="utf-8-sig")
    ys = {r["이름"]: S.yearly(ctx, P, T0)["수익률"] for r, P in zip(rows, [dict(S.BASE, v=0), dict(S.BASE)] + [P for n, P in old.items() if "전체" in n] + list(runs.values()))}
    print("\n연도별 수익률 (%)")
    print(pd.DataFrame(ys).round(0).T.to_string())
    # 2026년 월별 (공격형V vs 2025까지 맞춘 정규화)
    idx = ctx["idx"]
    i0 = int(idx.searchsorted(pd.Timestamp(T0)))
    mm = {}
    for n, P in [("공격형V", dict(S.BASE))] + [(k, v) for k, v in runs.items()]:
        eq = pd.Series(S.sim(ctx, P, i0, len(idx) - 1, curve=True)[3], index=idx[i0:])
        m = eq.groupby(eq.index.to_period("M")).last().pct_change() * 100
        mm[n] = m[m.index.year == 2026]
    print("\n2026년 월별 수익률 (%)")
    print(pd.DataFrame(mm).round(1).T.to_string())
    print("\n파라미터:")
    for n, P in runs.items():
        print(n, {k: (round(v, 4) if isinstance(v, float) else v) for k, v in P.items() if v != S.BASE.get(k)})
    print(f"\n{time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
