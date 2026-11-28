"""
DSS-N 4차 — 7분할은 그대로 두고 '열쇠 네 가지'를 따로 · 묶어서 시험 (2026-11-28)

기본값(7분할 · 매수/매도 % · 보유일)은 손대지 않는다. 얹는 장치만 바꾼다.

  A 반등 때 크게 사기 : 고점 대비 크게 빠진 상태(전일 종가 기준)에서 반등이 확인되면 그날 매수 수량 × m
                        낙폭 자 = SOXL 자기 낙폭(1년 고점 대비) 또는 QQQ 낙폭
                        반등 확인 = 없음 / 전일 상승 / 전일 +3% 이상 / 전일 종가 > 5일선 / 10일 저점 대비 +10%
  B 파는 쪽 정규화    : 매도 % (또는 보유일) × 자^a — 자 = SOXL 변동비 · QQQ 변동비 · SOXL 백분위 · QQQ 백분위
  C 칸 수             : 위험 달 칸 수 9 · 10 · 14 (V) / 변동성 백분위가 높으면 그날 칸 수를 9 · 12 로
  D 현금 남기기       : 늦추기(매수 뒤 g일 쉬기) · 오른 날 조금씩 팔기(종가가 전일 +u% 이상이면 보유 칸마다 f 매도)
  E 묶음              : A × (C · D)

모든 신호는 전일 종가까지의 값만 쓴다 (주문표를 만드는 시점에 알 수 있음).
검증: 2011~ 전체 · 앞/뒤 절반 · 봉인 · 4구간 최소 · ~2025 모두 기준(공격형V)을 넘어야 통과, TQQQ(V 없이)는 참고.
"""
import os, sys, math, time, pickle, tempfile, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "reports"))
import numpy as np
import pandas as pd
import multiprocessing as mp
import dssn_sim as S

FEE, SEC = S.FEE, S.SEC
T0, SPLIT = "2011-01-03", "2019-12-31"
WIN = {"H1": ("2010-03-12", "2018-03-12"), "H2": ("2018-03-12", None), "봉인": ("2024-01-02", None)}
SUBS = [("2010-03-12", "2014-03-12"), ("2014-03-12", "2018-03-12"), ("2018-03-12", "2022-03-12"), ("2022-03-12", None)]
KEYS = ["Calmar", "H1", "H2", "봉인", "구간최소", "~2025"]
O0 = dict(rb_src="", rb_dd=0.4, rb_cf=0, rb_m=1.0,        # A
          sn_src="", sn_a=0.0, hn_a=0.0,                  # B
          dv_src="", dv_thr=0.8, dv_n=12,                 # C (risk_div 는 P 로)
          gap=0, gap_sc=0, tr_u=0.0, tr_f=0.0, tr_sc=0)   # D  (범위 0 항상 · 1 위험 달 · 2 SOXL 낙폭 −30% 이하)
_C = {}


def add_rulers(ctx, close, qqq):
    """전일 종가까지로 만든 자(ruler)들. close: 종목 종가, qqq: QQQ 종가 (둘 다 Series)"""
    idx = ctx["idx"]
    c = close.astype(float)
    q = qqq.astype(float).reindex(idx).ffill()

    def vol(x):
        sd = x.pct_change().rolling(20).std()
        rel = (sd / sd.rolling(252, min_periods=60).median()).shift(1)
        pct = sd.rolling(252, min_periods=60).rank(pct=True).shift(1)
        return (np.nan_to_num(rel.values, nan=1.0).clip(0.2, 5.0), np.power(2.0, 2.0 * (np.nan_to_num(pct.values, nan=0.5) - 0.5)),
                np.nan_to_num(pct.values, nan=0.5))

    def dd(x):
        return np.nan_to_num((x / x.rolling(252, min_periods=20).max() - 1).shift(1).values, nan=0.0)
    s_rel, s_px, s_p = vol(c)
    q_rel, q_px, q_p = vol(q)
    r1 = np.nan_to_num(c.pct_change().shift(1).values, nan=0.0)
    ma5 = np.nan_to_num((c > c.rolling(5).mean()).shift(1).values.astype(float), nan=0.0) > 0
    low10 = np.nan_to_num((c / c.rolling(10).min() - 1).shift(1).values, nan=0.0)
    ctx.update(R=dict(S=s_rel, Q=q_rel, SP=s_px, QP=q_px), PCT=dict(SP=s_p, QP=q_p), DD=dict(S=dd(c), Q=dd(q)),
               CF=[np.ones(len(idx), bool), r1 > 0, r1 >= 0.03, ma5, low10 >= 0.10])
    return ctx


def sim3(ctx, P, O, i0, i1):
    P = dict(S.BASE, **P)
    O = dict(O0, **O)
    closes = ctx["closes"]
    buy_a, sell_a, hold_a, div_a, _, _ = S.day_arrays(ctx, P)
    n_all = len(closes)
    if O["sn_src"]:
        lr = np.log(ctx["R"][O["sn_src"]])
        if O["sn_a"]:
            sell_a = sell_a * np.clip(np.exp(O["sn_a"] * lr), 0.25, 4.0)
        if O["hn_a"]:
            hold_a = np.clip(np.rint(hold_a * np.clip(np.exp(O["hn_a"] * lr), 0.25, 4.0)), 1, 90).astype(np.int64)
    if O["dv_src"]:
        div_a = np.where(ctx["PCT"][O["dv_src"]] >= O["dv_thr"], np.maximum(div_a, int(O["dv_n"])), div_a)
    rb_m = O["rb_m"]
    rb_on = (ctx["DD"][O["rb_src"]] <= -O["rb_dd"]) & ctx["CF"][int(O["rb_cf"])] if (O["rb_src"] and rb_m != 1.0) else None
    risk, dd30 = ctx["risk"], ctx["DD"]["S"] <= -0.30
    scope = (None, risk, dd30)
    gap, gap_on = int(O["gap"]), scope[int(O["gap_sc"])]
    tr_u, tr_f, tr_on = O["tr_u"], O["tr_f"], scope[int(O["tr_sc"])]
    renew, pcr, lcr = int(P["renew"]), P["pcr"], P["lcr"]
    floor = math.floor
    capital = cash = 10000.0
    pos = []   # [목표가, 손절idx, 수량, 매수금액, 수수료]
    dsr, pnl_acc = 0, 0.0
    peak, mdd, total = 10000.0, 0.0, 10000.0
    prev = closes[i0 - 1] if i0 > 0 else None
    last_buy = -99
    for i in range(i0, i1 + 1):
        close = closes[i]
        div = div_a[i]
        n_open = len(pos)
        cash_open, cap_open = cash, capital
        if n_open:
            keep = []
            for p in pos:
                if close >= p[0] or i >= p[1]:
                    amt = p[2] * close
                    net = amt - amt * (FEE + SEC)
                    cash += net
                    pnl_acc += net - (p[3] + p[4])
                else:
                    keep.append(p)
            if tr_f and keep and prev is not None and close >= prev * (1 + tr_u) and (tr_on is None or tr_on[i]):
                nk = []
                for p in keep:
                    q = int(p[2] * tr_f)
                    if q:
                        amt = q * close
                        net = amt - amt * (FEE + SEC)
                        cash += net
                        pnl_acc += net - (p[3] + p[4]) * q / p[2]
                        r = 1 - q / p[2]
                        p[3] *= r; p[4] *= r; p[2] -= q
                    if p[2] > 0:
                        nk.append(p)
                keep = nk
            pos = keep
        dsr += 1
        if dsr >= renew:
            if pnl_acc != 0:
                capital += pnl_acc * (pcr if pnl_acc > 0 else lcr)
            dsr, pnl_acc = 0, 0.0
        if prev is not None and n_open < div and len(pos) < div:
            if not (gap and i - last_buy <= gap and (gap_on is None or gap_on[i])):
                bo = floor(prev * (1 + buy_a[i]) * 100) / 100
                if close <= bo:
                    q = int(cap_open / div / bo)
                    if rb_on is not None and rb_on[i]:
                        q = int(q * rb_m)
                    cap = int(max(0.0, cash_open) / (bo * (1 + FEE)))
                    if q > cap:
                        q = cap
                    if q > 0:
                        amt = q * close
                        fee = amt * FEE
                        if cash >= amt + fee:
                            cash -= amt + fee
                            st = i + hold_a[i]
                            if st >= n_all:
                                st = n_all - 1
                            pos.append([round(close * (1 + sell_a[i]), 2), st, q, amt, fee])
                            last_buy = i
        hv = 0.0
        for p in pos:
            hv += p[2]
        total = cash + hv * close
        if total > peak:
            peak = total
        dd = (total - peak) / peak
        if dd < mdd:
            mdd = dd
        prev = close
    return total / 10000.0, mdd, i1 - i0 + 1


def metric3(ctx, P, O, s, e=None):
    idx = ctx["idx"]
    i0 = int(idx.searchsorted(pd.Timestamp(s)))
    i1 = (int(idx.searchsorted(pd.Timestamp(e), side="right")) - 1) if e else len(idx) - 1
    g, mdd, n = sim3(ctx, P, O, i0, i1)
    cg = g ** (252.0 / n) - 1
    return cg * 100, mdd * 100, (cg / abs(mdd) if mdd else 0.0)


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _C.update(pickle.load(f))


def full_row(job):
    name, grp, P, O = job
    P = dict(P)
    Ps = {k: P.pop(k) for k in list(P) if k == "risk_div"}          # risk_div 는 SOXL 에만
    PS = dict(P, **Ps)
    m = lambda tk, PP, s, e=None: metric3(_C[tk], PP, O, s, e)
    r = [m("SOXL", PS, T0), m("SOXL", PS, T0, SPLIT), m("SOXL", PS, "2020-01-02")]
    r += [m("SOXL", PS, s, e) for s, e in WIN.values()] + [m("SOXL", PS, s, e) for s, e in SUBS]
    r += [m("SOXL", PS, "2010-03-12", "2025-12-31"), m("TQQQ", dict(P, v=0), T0)]
    return dict(이름=name, 묶음=grp, CAGR=r[0][0], MDD=r[0][1], Calmar=r[0][2], 앞Calmar=r[1][2], 뒤Calmar=r[2][2], 앞CAGR=r[1][0], 뒤CAGR=r[2][0],
                앞MDD=r[1][1], 뒤MDD=r[2][1], H1=r[3][2], H2=r[4][2], 봉인=r[5][2], 구간최소=min(x[2] for x in r[6:10]),
                **{"~2025": r[10][2], "TQQQ CAGR": r[11][0], "TQQQ MDD": r[11][1], "TQQQ Calmar": r[11][2]})


CFN = ["확인 없음", "전일 상승", "전일 +3%↑", "5일선 위", "10일 저점 +10%"]
SRC = dict(S="SOXL 변동비", Q="QQQ 변동비", SP="SOXL 백분위", QP="QQQ 백분위")
SCN = ["항상", "위험 달", "낙폭 −30%↓"]


def variants():
    v = [("순정", "", {}, {})]
    for src, dds in (("S", (.3, .4, .5)), ("Q", (.10, .15, .20))):
        for d in dds:
            for cf in range(5):
                for m in (1.5, 2.0, 3.0):
                    v.append((f"반등 확대 {'SOXL' if src == 'S' else 'QQQ'} −{int(d*100)}%↓ · {CFN[cf]} ×{m}", "A", {}, dict(rb_src=src, rb_dd=d, rb_cf=cf, rb_m=m)))
    for src in SRC:
        v += [(f"매도% × {SRC[src]}^{a}", "B", {}, dict(sn_src=src, sn_a=a)) for a in (-.5, .25, .5, 1.0)]
        v += [(f"보유일 × {SRC[src]}^{a}", "B", {}, dict(sn_src=src, hn_a=a)) for a in (-.5, .5)]
        v += [(f"매도% · 보유일 × {SRC[src]}^0.5", "B", {}, dict(sn_src=src, sn_a=.5, hn_a=.5))]
    v += [(f"위험 달 {n}칸", "C", dict(risk_div=n), {}) for n in (9, 10, 14)]
    v += [(f"{SRC[s]} {int(t*100)}%↑ 이면 {n}칸", "C", {}, dict(dv_src=s, dv_thr=t, dv_n=n)) for s in ("SP", "QP") for t in (.7, .8, .9) for n in (9, 12)]
    v += [(f"늦추기 {g}일 · {SCN[sc]}", "D", {}, dict(gap=g, gap_sc=sc)) for g in (1, 2, 3) for sc in range(3)]
    v += [(f"오른 날(+{int(u*100)}%↑) {int(f*100)}% 팔기 · {SCN[sc]}", "D", {}, dict(tr_u=u, tr_f=f, tr_sc=sc)) for u in (.03, .05, .08) for f in (.1, .25) for sc in range(3)]
    cash = [("위험 달 9칸", dict(risk_div=9), {}), ("늦추기 1일(위험 달)", {}, dict(gap=1, gap_sc=1)),
            ("오른 날 +5% 25% 팔기(위험 달)", {}, dict(tr_u=.05, tr_f=.25, tr_sc=1)),
            ("위험 달 9칸 + 오른 날 팔기", dict(risk_div=9), dict(tr_u=.05, tr_f=.25, tr_sc=1))]
    for src, d in (("S", .4), ("Q", .15)):
        for cf in (1, 2, 3):
            for m in (2.0, 3.0):
                for cn, cp, co in cash:
                    v.append((f"[{'SOXL' if src == 'S' else 'QQQ'} −{int(d*100)}%↓ · {CFN[cf]} ×{m}] + {cn}", "E", cp, dict(co, rb_src=src, rb_dd=d, rb_cf=cf, rb_m=m)))
    return v


def main():
    t0 = time.time()
    ctx, df, ms = S.load_soxl()
    from dss_engine import load_price_data, get_week_mode_map
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")["Close"]
    tq = load_price_data("TQQQ", "2010-02-11", "2026-12-31")
    add_rulers(ctx, df["Close"], qqq)
    C = {"SOXL": ctx, "TQQQ": add_rulers(S.make_ctx(tq, get_week_mode_map(ms, tq.index), set()), tq["Close"], qqq)}
    for v in (0, 1):
        a = S.metric(ctx, dict(S.BASE, v=v), "2010-03-12")
        b = metric3(ctx, dict(v=v), {}, "2010-03-12")
        print(f"일치 확인 V={v}: {a[0]:.6f} / {a[1]:.6f}  vs  {b[0]:.6f} / {b[1]:.6f}  차이 {abs(a[0]-b[0]) + abs(a[1]-b[1]):.1e}")
    on = (ctx["DD"]["S"] <= -0.4)
    i0 = int(ctx["idx"].searchsorted(pd.Timestamp(T0)))
    print(f"SOXL 낙폭 −40% 이하인 날: {on[i0:].mean()*100:.1f}%  (그중 전일 상승 {(on & ctx['CF'][1])[i0:].mean()*100:.1f}%)  "
          f"QQQ −15% 이하: {(ctx['DD']['Q'] <= -0.15)[i0:].mean()*100:.1f}%")
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(C, tmp); tmp.close()
    f2 = lambda v: f"{v:.2f}"
    cols = ["이름", "CAGR", "MDD", "Calmar", "앞Calmar", "뒤Calmar", "H1", "H2", "봉인", "구간최소", "~2025", "TQQQ Calmar", "통과", "TQQQ"]
    allrows = []
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        for bn, B in (("공격형V", dict(v=1)), ("공격형", dict(v=0))):
            jobs = [(n, g, dict(B, **p), o) for n, g, p, o in variants() if not (bn == "공격형" and ("위험 달" in n))]
            d = pd.DataFrame(pool.map(full_row, jobs, chunksize=2))
            b = d.iloc[0]
            d["통과"] = ["" if i == 0 else ("✅" if all(r[k] > b[k] for k in KEYS) else "") for i, r in d.iterrows()]
            d["TQQQ"] = ["" if i == 0 else ("↑" if r["TQQQ Calmar"] > b["TQQQ Calmar"] else "↓") for i, r in d.iterrows()]
            print(f"\n━━ {bn} + 열쇠 (SOXL {T0}~, TQQQ 는 V 없이)   변형 {len(d) - 1}개 · 통과 {(d['통과'] == '✅').sum()}개")
            print(d[cols].to_string(index=False, float_format=f2))
            x = d.iloc[1:]
            for g in "ABCDE":
                y = x[x["묶음"] == g]
                if len(y):
                    print(f"   {g}: {len(y)}개 · 통과 {(y['통과'] == '✅').sum()} · 전체 Calmar {y['Calmar'].min():.2f}~{y['Calmar'].max():.2f} (기준 {b['Calmar']:.2f}) · "
                          f"앞 개선 {(y['앞Calmar'] > b['앞Calmar']).mean()*100:.0f}% · 뒤 개선 {(y['뒤Calmar'] > b['뒤Calmar']).mean()*100:.0f}% · "
                          f"앞·뒤 상관 {np.corrcoef(y['앞Calmar'], y['뒤Calmar'])[0, 1]:+.2f} · TQQQ 개선 {(y['TQQQ Calmar'] > b['TQQQ Calmar']).mean()*100:.0f}%")
            allrows.append(d.assign(기본=bn))
            print(f"   {time.time() - t0:.0f}초", flush=True)
    os.unlink(tmp.name)
    pd.concat(allrows).to_csv(os.path.join(HERE, "reports", "dssn_keys.csv"), index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
