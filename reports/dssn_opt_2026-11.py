"""
DSS-N (정규화 동파) 최적화 — 목표 기준점: 2011~2026 CAGR 85.11% · MDD −42.05% (2026-11-27)

시뮬레이터: reports/dssn_sim.py (배포 엔진과 차이 0 확인)
목적 함수 : CAGR − 3 × max(0, |MDD| − 42)   → 'MDD 42% 이내에서 CAGR 최대'

① 정규화 손잡이를 하나씩만 (공격형V 기본값 그대로)
② 전체 탐색 (기본값 + 정규화) — 세 가지를 같은 탐색량으로
     N-전체  : 2011~2026 전체에 맞춤 (목표와 같은 조건, 표본 내)
     P-전체  : 정규화 없이 기본값만 전체에 맞춤 (정규화의 몫을 가려내는 대조군)
     N-앞    : 2011~2019 에만 맞춤 → 2020~2026 은 처음 보는 구간 (표본 외)
     P-앞    : 정규화 없이 2011~2019 에만 맞춤
③ 결과 점검: 구간별 · 연도별 · 값을 ±10% 흔들었을 때
"""
import os, sys, time, pickle, tempfile, json, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "reports"))
import numpy as np
import pandas as pd
import multiprocessing as mp
import dssn_sim as S

T0, SPLIT = "2011-01-03", "2019-12-31"
TARGET = dict(CAGR=85.11, MDD=-42.05)
TARGET_Y = {2011: 0.52, 2012: 26.33, 2013: 120.48, 2014: 92.30, 2015: 52.40, 2016: 104.15, 2017: 80.77, 2018: 23.11,
            2019: 62.88, 2020: 218.76, 2021: 145.01, 2022: 68.04, 2023: 89.00, 2024: 131.12, 2025: 186.36, 2026: 21.93}
WIN = {"H1": ("2010-03-12", "2018-03-12"), "H2": ("2018-03-12", None), "봉인": ("2024-01-02", None)}
SUBS = [("2010-03-12", "2014-03-12"), ("2014-03-12", "2018-03-12"), ("2018-03-12", "2022-03-12"), ("2022-03-12", None)]

# (이름, 최소, 최대, 종류)  종류: f 실수 · i 정수 · c 선택
SPACE_P = [("sf_buy", .010, .060, "f"), ("sf_sell", .004, .050, "f"), ("sf_hold", 10, 60, "i"),
           ("ag_buy", .010, .070, "f"), ("ag_sell", .010, .080, "f"), ("ag_hold", 3, 20, "i"),
           ("div", 5, 9, "i"), ("renew", 5, 20, "i"), ("pcr", .5, 1.0, "f"), ("lcr", 0.0, .5, "f"),
           ("risk_div", 9, 16, "i"), ("risk_ag_hold", 3, 8, "i")]
SPACE_N = [("win", 0, 3, "c"),
           ("a_buy_sf", -1.0, 1.5, "f"), ("a_buy_ag", -1.0, 1.5, "f"), ("a_sell_sf", -1.0, 1.5, "f"), ("a_sell_ag", -1.0, 1.5, "f"),
           ("a_hold_sf", -1.5, 1.0, "f"), ("a_hold_ag", -1.5, 1.0, "f"),
           ("s_sf", -.9, .9, "f"), ("s_ag", -.9, .9, "f"), ("sv_sf", -.8, .8, "f"), ("sv_ag", -.8, .8, "f")]
WINS = (10, 20, 40, 60)
_C = {}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _C["ctx"] = pickle.load(f)


def to_P(x, space):
    P = dict(S.BASE)
    for (k, lo, hi, t), v in zip(space, x):
        v = min(hi, max(lo, v))
        if k == "div":
            P["sf_div"] = P["ag_div"] = int(round(v))
        elif k == "win":
            P["win"] = WINS[int(round(v))]
        elif t == "i":
            P[k] = int(round(v))
        else:
            P[k] = float(v)
    return P


def score(m):
    return m[0] - 3.0 * max(0.0, abs(m[1]) - 42.0)


def ev(job):
    P, s, e = job
    return S.metric(_C["ctx"], P, s, e)


def search(pool, space, s, e, seed, pop=140, keep=20, gens=40):
    rng = np.random.default_rng(seed)
    lo = np.array([a for _, a, _, _ in space], float)
    hi = np.array([b for _, _, b, _ in space], float)
    base = dict(S.BASE, div=7)
    x0 = np.array([float(base.get(k, 1 if k == "win" else 0.0)) if k != "win" else 1.0 for k, *_ in space])
    X = np.vstack([x0, lo + rng.random((pop - 1, len(space))) * (hi - lo)])
    best = []
    for g in range(gens):
        res = pool.map(ev, [(to_P(x, space), s, e) for x in X], chunksize=4)
        best = sorted(best + [(score(m), tuple(x)) for m, x in zip(res, X)], key=lambda t: -t[0])
        uniq, seen = [], set()
        for sc_, x in best:
            key = json.dumps(to_P(x, space), sort_keys=True)
            if key not in seen:
                seen.add(key); uniq.append((sc_, x))
            if len(uniq) >= keep:
                break
        best = uniq
        sig = (hi - lo) * (0.20 * (1 - g / gens) + 0.02)
        el = np.array([x for _, x in best])
        par = el[rng.integers(0, len(el), pop)]
        par2 = el[rng.integers(0, len(el), pop)]
        mix = rng.random((pop, len(space))) < 0.5
        X = np.clip(np.where(mix, par, par2) + rng.normal(0, 1, (pop, len(space))) * sig * (rng.random((pop, len(space))) < 0.4), lo, hi)
    return to_P(best[0][1], space), best[0][0]


def report(pool, name, P, fit_end=None):
    jobs = [(P, T0, None), (P, "2010-03-12", None), (P, T0, SPLIT), ("2020", None, None)]
    jobs[3] = (P, "2020-01-02", None)
    jobs += [(P, s, e) for s, e in WIN.values()] + [(P, s, e) for s, e in SUBS] + [(P, "2010-03-12", "2025-12-31")]
    r = pool.map(ev, jobs)
    row = dict(이름=name, CAGR=r[0][0], MDD=r[0][1], Calmar=r[0][2], **{"2010~": r[1][2], "앞(11~19)": r[2][0], "앞MDD": r[2][1],
               "뒤(20~26)": r[3][0], "뒤MDD": r[3][1], "뒤Calmar": r[3][2]},
               H1=r[4][2], H2=r[5][2], 봉인=r[6][2], 구간최소=min(x[2] for x in r[7:11]), **{"~2025": r[11][2]})
    return row


def jitter(pool, P, n=200, seed=7):
    rng = np.random.default_rng(seed)
    jobs = []
    for _ in range(n):
        Q = dict(P)
        for k, v in P.items():
            if k in ("v", "win", "sf_div", "ag_div", "risk_div", "renew"):
                continue
            f = 1 + rng.uniform(-0.10, 0.10)
            if isinstance(v, int):
                Q[k] = max(1, int(round(v * f)))
            elif k.startswith(("a_", "s_", "sv_")):
                Q[k] = v + rng.uniform(-0.10, 0.10)
            else:
                Q[k] = v * f
        jobs.append((Q, T0, None))
    r = np.array(pool.map(ev, jobs, chunksize=4))
    return dict(CAGR중앙=np.median(r[:, 0]), MDD중앙=np.median(r[:, 1]), MDD최악=r[:, 1].min(), Calmar중앙=np.median(r[:, 2]),
                Calmar하위5=np.percentile(r[:, 2], 5))


def main():
    t0 = time.time()
    ctx, df, ms = S.load_soxl()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(ctx, tmp); tmp.close()
    f2 = lambda v: f"{v:.2f}"
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        # ① 하나씩
        V = dict(S.BASE)
        rows = [report(pool, "공격형", dict(V, v=0)), report(pool, "공격형V", V)]
        for label, keys, vals in (("매수% a", ("a_buy_sf", "a_buy_ag"), (-0.5, 0.25, 0.5, 0.75, 1.0)),
                                  ("매도% a", ("a_sell_sf", "a_sell_ag"), (-0.5, 0.25, 0.5, 0.75, 1.0)),
                                  ("보유일 a", ("a_hold_sf", "a_hold_ag"), (-1.0, -0.5, -0.25, 0.25, 0.5)),
                                  ("티어 기울기 s", ("s_sf", "s_ag"), (-0.6, -0.3, 0.3, 0.6)),
                                  ("티어 기울기×변동성 sv", ("sv_sf", "sv_ag"), (-0.5, -0.25, 0.25, 0.5))):
            for v in vals:
                rows.append(report(pool, f"{label}={v:+.2f}", dict(V, **{k: v for k in keys})))
        for v in (0.5, 1.0):
            rows.append(report(pool, f"매수·매도 a={v:+.2f}", dict(V, a_buy_sf=v, a_buy_ag=v, a_sell_sf=v, a_sell_ag=v)))
            rows.append(report(pool, f"매수·매도 a={v:+.2f} · 보유 {-v/2:+.2f}",
                               dict(V, a_buy_sf=v, a_buy_ag=v, a_sell_sf=v, a_sell_ag=v, a_hold_sf=-v / 2, a_hold_ag=-v / 2)))
        d1 = pd.DataFrame(rows)
        print("━━ ① 정규화 손잡이 하나씩 (공격형V 기본값, 2011-01-03~) — 목표 85.11% / −42.05% / 2.02")
        print(d1.to_string(index=False, float_format=f2))
        d1.to_csv(os.path.join(HERE, "reports", "dssn_opt_single.csv"), index=False, encoding="utf-8-sig")
        print(f"   {time.time() - t0:.0f}초", flush=True)

        # ② 탐색
        runs = {}
        for name, space, e in (("N-전체", SPACE_P + SPACE_N, None), ("P-전체", SPACE_P, None),
                               ("N-앞", SPACE_P + SPACE_N, SPLIT), ("P-앞", SPACE_P, SPLIT)):
            cands = [search(pool, space, T0, e, seed) for seed in (1, 2, 3)]
            for i, (P, sc_) in enumerate(cands):
                runs[f"{name}#{i + 1}"] = P
            print(f"   {name} 탐색 끝 — 점수 {[round(c[1], 1) for c in cands]}  {time.time() - t0:.0f}초", flush=True)
        rows = [report(pool, "공격형V", V)] + [report(pool, n, P) for n, P in runs.items()]
        d2 = pd.DataFrame(rows)
        print("\n━━ ② 탐색 결과 (N = 정규화 포함 · P = 기본값만 / 전체 = 2011~2026 에 맞춤 · 앞 = 2011~2019 에만 맞춤)")
        print(d2.to_string(index=False, float_format=f2))
        d2.to_csv(os.path.join(HERE, "reports", "dssn_opt_search.csv"), index=False, encoding="utf-8-sig")
        with open(os.path.join(HERE, "reports", "dssn_opt_params.json"), "w", encoding="utf-8") as f:
            json.dump(runs, f, ensure_ascii=False, indent=1)

        # ③ 흔들기 · 연도별
        print("\n━━ ③ 값을 ±10% 흔들었을 때 (200회, 2011-01-03~)")
        jr = [dict(이름=n, **jitter(pool, P)) for n, P in [("공격형V", V)] + list(runs.items())]
        print(pd.DataFrame(jr).to_string(index=False, float_format=f2))
    os.unlink(tmp.name)
    ys = {"목표": pd.Series(TARGET_Y), "공격형V": S.yearly(ctx, V, T0)["수익률"]}
    for n in ("N-전체", "P-전체", "N-앞", "P-앞"):
        b = max((k for k in runs if k.startswith(n)), key=lambda k: float(d2[d2["이름"] == k]["Calmar"].iloc[0]) if "전체" in n
                else float(d2[d2["이름"] == k]["앞(11~19)"].iloc[0]))
        ys[b] = S.yearly(ctx, runs[b], T0)["수익률"]
    print("\n━━ 연도별 수익률 (%)")
    print(pd.DataFrame(ys).round(1).T.to_string())
    print("\n파라미터:")
    for n, P in runs.items():
        print(n, {k: (round(v, 4) if isinstance(v, float) else v) for k, v in P.items() if v != S.BASE.get(k)})
    print(f"\n{time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
