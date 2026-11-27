"""
DSS-N 2차 — 공유받은 정규화 방식 그대로 (2026-11-27)

  가격 정규화 지수 0.5 : 매수% · 매도% = 원래 % × (오늘 변동 ÷ 평소 변동)^0.5
  수량 정규화 지수 1.0 : 칸 크기 = 원래 × (평소 변동 ÷ 오늘 변동)^1.0  (상한 2배)
  RSI                  : RSI 낮으면 매수% 크게 · 익절 멀리 · 더 오래 들기 / 오를 때 크게 사기
  보유 진행률          : 칸이 찰수록 작게
  '평소 변동' = 최근 1년 중앙값 (종목 자체 기준 → 다른 종목에도 그대로 적용 가능)

① 정해진 값 그대로 — 공격형 · 공격형V · 보초형W 위에, SOXL 과 TQQQ(교차 종목)
② 손잡이 7개만 무작위 600조합 — 전체 기간 1등 / 2011~2019 1등이 2020~2026 과 TQQQ 에서 어떤가
"""
import os, sys, time, pickle, tempfile, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "reports"))
import numpy as np
import pandas as pd
import multiprocessing as mp
import dssn_sim as S

T0, SPLIT = "2011-01-03", "2019-12-31"
WIN = {"H1": ("2010-03-12", "2018-03-12"), "H2": ("2018-03-12", None), "봉인": ("2024-01-02", None)}
SUBS = [("2010-03-12", "2014-03-12"), ("2014-03-12", "2018-03-12"), ("2018-03-12", "2022-03-12"), ("2022-03-12", None)]
W8 = (0, 4, 1.25, 1.25, 1.25, 1.25, 1.25, 1)
BASES = {"공격형": dict(v=0), "공격형V": dict(v=1),
         "보초형W": dict(v=0, sf_div=8, ag_div=8, tw_sf=W8, tw_ag=W8), "보초형W+V": dict(v=1, sf_div=8, ag_div=8, tw_sf=W8, tw_ag=W8)}
PR = dict(ref=1, a_buy_sf=.5, a_buy_ag=.5, a_sell_sf=.5, a_sell_ag=.5)
RECIPES = {"순정": {}, "가격 0.5": PR, "수량 1.0": dict(ref=1, q_exp=1.0), "가격 0.5 + 수량 1.0": dict(PR, q_exp=1.0),
           "가격 0.5 + 수량 0.5": dict(PR, q_exp=0.5),
           "+ RSI (매수 · 매도 · 보유일 0.5)": dict(PR, q_exp=1.0, r_buy=.5, r_sell=.5, r_hold=.5),
           "+ RSI 수량 0.5 (오를 때 크게)": dict(PR, q_exp=1.0, r_qty=.5),
           "+ 진행률 0.5 (찰수록 작게)": dict(PR, q_exp=1.0, prog=.5),
           "전부": dict(PR, q_exp=1.0, r_buy=.5, r_sell=.5, r_hold=.5, r_qty=.5, prog=.5)}
KNOBS = [("a", 0.0, 1.0), ("q_exp", 0.0, 1.5), ("r_buy", -1.0, 1.0), ("r_sell", -1.0, 1.0), ("r_hold", -1.0, 1.0),
         ("r_qty", -1.0, 1.0), ("prog", 0.0, 0.8)]
_C = {}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _C.update(pickle.load(f))


def ev(job):
    tk, P, s, e = job
    return S.metric(_C[tk], dict(S.BASE, **P), s, e)


def knob_P(x):
    d = dict(zip([k for k, *_ in KNOBS], x))
    a = d.pop("a")
    return dict(ref=1, a_buy_sf=a, a_buy_ag=a, a_sell_sf=a, a_sell_ag=a, **{k: float(v) for k, v in d.items()})


def full_row(pool, name, P):
    jobs = [("SOXL", P, T0, None), ("SOXL", P, T0, SPLIT), ("SOXL", P, "2020-01-02", None)]
    jobs += [("SOXL", P, s, e) for s, e in WIN.values()] + [("SOXL", P, s, e) for s, e in SUBS] + [("SOXL", P, "2010-03-12", "2025-12-31")]
    jobs += [("TQQQ", dict(P, v=0), T0, None)]
    r = pool.map(ev, jobs)
    return dict(이름=name, CAGR=r[0][0], MDD=r[0][1], Calmar=r[0][2], **{"앞CAGR": r[1][0], "앞MDD": r[1][1], "뒤CAGR": r[2][0], "뒤MDD": r[2][1], "뒤Calmar": r[2][2]},
                H1=r[3][2], H2=r[4][2], 봉인=r[5][2], 구간최소=min(x[2] for x in r[6:10]), **{"~2025": r[10][2], "TQQQ CAGR": r[11][0], "TQQQ MDD": r[11][1], "TQQQ Calmar": r[11][2]})


def main():
    t0 = time.time()
    ctx, df, ms = S.load_soxl()
    from dss_engine import load_price_data, get_week_mode_map
    tq = load_price_data("TQQQ", "2010-02-11", "2026-12-31")
    C = {"SOXL": ctx, "TQQQ": S.make_ctx(tq, get_week_mode_map(ms, tq.index), set())}
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(C, tmp); tmp.close()
    f2 = lambda v: f"{v:.2f}"
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        rows = []
        for bn, B in BASES.items():
            for rn, Rc in RECIPES.items():
                rows.append(dict(기본=bn, **full_row(pool, rn, dict(B, **Rc))))
        d1 = pd.DataFrame(rows)
        print("━━ ① 정해진 값 그대로 (SOXL 2011-01-03~ · TQQQ 는 V 없이 같은 값)")
        print(d1.to_string(index=False, float_format=f2))
        d1.to_csv(os.path.join(HERE, "reports", "dssn_recipe.csv"), index=False, encoding="utf-8-sig")
        print(f"   {time.time() - t0:.0f}초", flush=True)

        # ② 손잡이 7개 무작위
        rng = np.random.default_rng(20261127)
        lo = np.array([a for _, a, _ in KNOBS]); hi = np.array([b for _, _, b in KNOBS])
        X = lo + rng.random((600, len(KNOBS))) * (hi - lo)
        out = []
        for bn in ("공격형", "공격형V"):
            B = BASES[bn]
            Ps = [dict(B, **knob_P(x)) for x in X]
            jobs = []
            for P in Ps:
                jobs += [("SOXL", P, T0, None), ("SOXL", P, T0, SPLIT), ("SOXL", P, "2020-01-02", None), ("TQQQ", dict(P, v=0), T0, None)]
            r = np.array(pool.map(ev, jobs, chunksize=8)).reshape(len(Ps), 4, 3)
            b = np.array(pool.map(ev, [("SOXL", B, T0, None), ("SOXL", B, T0, SPLIT), ("SOXL", B, "2020-01-02", None), ("TQQQ", dict(B, v=0), T0, None)]))
            res = pd.DataFrame(X, columns=[k for k, *_ in KNOBS])
            for j, nm in enumerate(("전체", "앞", "뒤", "TQQQ")):
                res[nm + "CAGR"], res[nm + "MDD"], res[nm + "Calmar"] = r[:, j, 0], r[:, j, 1], r[:, j, 2]
            res.to_csv(os.path.join(HERE, "reports", f"dssn_recipe_random_{bn}.csv"), index=False, encoding="utf-8-sig")
            print(f"\n━━ ② {bn} + 손잡이 7개 무작위 600조합  (순정: 전체 {b[0][0]:.1f}%/{b[0][1]:.1f}%/{b[0][2]:.2f} · 앞 {b[1][2]:.2f} · 뒤 {b[2][2]:.2f} · TQQQ {b[3][0]:.1f}%/{b[3][1]:.1f}%/{b[3][2]:.2f})")
            print(f"   순정보다 Calmar 높은 조합: 전체 {(res['전체Calmar'] > b[0][2]).mean()*100:.0f}% · 앞 {(res['앞Calmar'] > b[1][2]).mean()*100:.0f}% · 뒤 {(res['뒤Calmar'] > b[2][2]).mean()*100:.0f}%"
                  f" · TQQQ {(res['TQQQCalmar'] > b[3][2]).mean()*100:.0f}% · 넷 모두 {((res['전체Calmar'] > b[0][2]) & (res['앞Calmar'] > b[1][2]) & (res['뒤Calmar'] > b[2][2]) & (res['TQQQCalmar'] > b[3][2])).mean()*100:.0f}%")
            print(f"   앞 Calmar 와 뒤 Calmar 의 상관: {np.corrcoef(res['앞Calmar'], res['뒤Calmar'])[0, 1]:+.2f} · SOXL 전체와 TQQQ 의 상관: {np.corrcoef(res['전체Calmar'], res['TQQQCalmar'])[0, 1]:+.2f}")
            top = res.nlargest(30, "앞Calmar")
            print(f"   앞 구간 상위 30개의 뒤 구간: Calmar 중앙 {top['뒤Calmar'].median():.2f} (전체 600개 중앙 {res['뒤Calmar'].median():.2f}) · MDD 중앙 {top['뒤MDD'].median():.1f}%")
            cols = [k for k, *_ in KNOBS] + ["전체CAGR", "전체MDD", "전체Calmar", "앞Calmar", "뒤CAGR", "뒤MDD", "뒤Calmar", "TQQQCAGR", "TQQQMDD", "TQQQCalmar"]
            for label, key in (("전체 기간 1~3등", "전체Calmar"), ("앞 구간(2011~2019) 1~3등", "앞Calmar")):
                print(f"   {label}")
                print(res.nlargest(3, key)[cols].to_string(index=False, float_format=f2))
            allpass = res[(res['전체Calmar'] > b[0][2]) & (res['앞Calmar'] > b[1][2]) & (res['뒤Calmar'] > b[2][2]) & (res['TQQQCalmar'] > b[3][2])]
            if len(allpass):
                print(f"   넷 모두 통과한 {len(allpass)}개의 손잡이 평균: " + " ".join(f"{k}={allpass[k].mean():+.2f}" for k, *_ in KNOBS))
                best = allpass.assign(m=allpass[["전체Calmar", "앞Calmar", "뒤Calmar"]].min(axis=1)).nlargest(1, "m").iloc[0]
                out.append((f"{bn} · 넷 모두 통과 중 최고", dict(B, **knob_P([best[k] for k, *_ in KNOBS]))))
        if out:
            print("\n━━ 넷 모두 통과한 후보의 전체 점검")
            print(pd.DataFrame([full_row(pool, n, P) for n, P in [("공격형V 순정", BASES["공격형V"])] + out]).to_string(index=False, float_format=f2))
            for n, P in out:
                print(n, {k: (round(v, 3) if isinstance(v, float) else v) for k, v in P.items()})
    os.unlink(tmp.name)
    print(f"\n{time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
