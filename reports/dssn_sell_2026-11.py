"""
DSS 매도 구조 실험 (2026-11-27) — 정규화가 아니라 '파는 방식' 자체를 바꿔 본다

지금 DSS: 칸마다 목표가에 닿으면 그 칸 전량 매도, 최대보유일이 되면 그 칸 전량 매도(손절).
시험한 다섯 가지 (각 손잡이 1~3개, 매수 규칙 · 모드 · V 는 그대로)

  분할 익절   : 목표가에 닿으면 칸의 f 만 팔고, 나머지는 목표 수익률의 m 배에서 판다 (손절일은 그대로)
  손절 연장   : 손절일에 손실 중이면 frac 만 팔고 나머지는 n 일 더 들고 간다 (한 번만)
  목표 낮추기 : 보유일이 지날수록 목표가를 낮춘다 (d=1 이면 손절일에 본전)
  짝 매도     : 익절이 나온 날, 가장 오래된 손실 칸을 그 이익의 r 배 한도 안에서 같이 정리
  묶음 매도   : 종가가 전체 평단보다 g% 높으면 모든 칸의 F 를 한꺼번에 판다 (표준편차매매식)
  평단 기준만 : 칸별 목표가를 없애고 '전체 평단 + g%' 에서만 F 씩 판다 (손절일은 그대로)
  급락일 수량 : 하루 낙폭이 x% 이상인 날의 매수 수량을 m 배 (0.5 = 덜 사기, 2 = 더 사기)

검증: 2011~ 전체 · 앞/뒤 절반 · 봉인 · 4구간 최소 · ~2025 모두 공격형V 를 넘어야 통과, TQQQ 는 참고.
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
O0 = dict(pf=0.0, pm=2.0, sx=0, sx_frac=0.5, decay=0.0, pr=0.0, bg=0.0, bF=1.0, bk=3, notgt=0, bd=0.0, bdm=2.0)
_C = {}


def sim2(ctx, P, O, i0, i1):
    P = dict(S.BASE, **P)
    O = dict(O0, **O)
    closes = ctx["closes"]
    buy_a, sell_a, hold_a, div_a, _, _ = S.day_arrays(ctx, P)
    n_all = len(closes)
    renew, pcr, lcr = int(P["renew"]), P["pcr"], P["lcr"]
    pf, pm, sx, sxf, dec, pr, bg, bF, bk = O["pf"], O["pm"], int(O["sx"]), O["sx_frac"], O["decay"], O["pr"], O["bg"], O["bF"], int(O["bk"])
    notgt, bd, bdm = O["notgt"], O["bd"], O["bdm"]
    floor = math.floor
    capital = cash = 10000.0
    pos = []   # [목표가, 손절idx, 수량, 매수금액, 수수료, 매수가, 매수idx, 매도%, 보유일, 익절단계, 연장여부]
    dsr, pnl_acc = 0, 0.0
    peak, mdd, total = 10000.0, 0.0, 10000.0
    prev = closes[i0 - 1] if i0 > 0 else None
    for i in range(i0, i1 + 1):
        close = closes[i]
        div = div_a[i]
        n_open = len(pos)
        cash_open, cap_open = cash, capital
        if n_open:
            keep, win_profit = [], 0.0
            for p in pos:
                tgt = p[0]
                if dec and p[9] == 0:
                    age = (i - p[6]) / p[8]
                    tgt = round(p[5] * (1 + p[7] * (1 - dec * (age if age < 1 else 1.0))), 2)
                q = 0
                if close >= tgt and not notgt:
                    q = p[2]
                    if pf and p[9] == 0:
                        qq = int(p[2] * pf)
                        if 0 < qq < p[2]:
                            q = qq
                            p[9] = 1
                            p[0] = round(p[5] * (1 + p[7] * pm), 2)
                    is_win = True
                elif i >= p[1]:
                    q = p[2]
                    if sx and not p[10] and close < p[5]:
                        q = int(p[2] * sxf)
                        p[10] = 1
                        p[1] = min(i + sx, n_all - 1)
                    is_win = False
                if q:
                    amt = q * close
                    net = amt - amt * (FEE + SEC)
                    cost = (p[3] + p[4]) * q / p[2]
                    cash += net
                    pnl_acc += net - cost
                    if is_win:
                        win_profit += net - cost
                    if q < p[2]:
                        r = 1 - q / p[2]
                        p[3] *= r; p[4] *= r; p[2] -= q
                        keep.append(p)
                else:
                    keep.append(p)
            if pr and win_profit > 0 and keep:
                p = keep[0]
                amt = p[2] * close
                net = amt - amt * (FEE + SEC)
                loss = net - (p[3] + p[4])
                if loss < 0 and -loss <= pr * win_profit:
                    cash += net
                    pnl_acc += loss
                    keep = keep[1:]
            if bg and len(keep) >= bk:
                tq = sum(p[2] for p in keep)
                if close >= sum(p[3] for p in keep) / tq * (1 + bg):
                    nk = []
                    for p in keep:
                        q = p[2] if bF >= 1 else int(p[2] * bF)
                        if q:
                            amt = q * close
                            net = amt - amt * (FEE + SEC)
                            cost = (p[3] + p[4]) * q / p[2]
                            cash += net
                            pnl_acc += net - cost
                        if q < p[2]:
                            r = 1 - q / p[2]
                            p[3] *= r; p[4] *= r; p[2] -= q
                            nk.append(p)
                    keep = nk
            pos = keep
        dsr += 1
        if dsr >= renew:
            if pnl_acc != 0:
                capital += pnl_acc * (pcr if pnl_acc > 0 else lcr)
            dsr, pnl_acc = 0, 0.0
        if prev is not None and n_open < div and len(pos) < div:
            bo = floor(prev * (1 + buy_a[i]) * 100) / 100
            if close <= bo:
                q = int(cap_open / div / bo)
                if bd and close <= prev * (1 - bd):
                    q = int(q * bdm)
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
                        pos.append([round(close * (1 + sell_a[i]), 2), st, q, amt, fee, close, i, sell_a[i], hold_a[i], 0, 0])
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


def metric2(ctx, P, O, s, e=None):
    idx = ctx["idx"]
    i0 = int(idx.searchsorted(pd.Timestamp(s)))
    i1 = (int(idx.searchsorted(pd.Timestamp(e), side="right")) - 1) if e else len(idx) - 1
    g, mdd, n = sim2(ctx, P, O, i0, i1)
    cg = g ** (252.0 / n) - 1
    return cg * 100, mdd * 100, (cg / abs(mdd) if mdd else 0.0)


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _C.update(pickle.load(f))


def ev(job):
    tk, P, O, s, e = job
    return metric2(_C[tk], P, O, s, e)


def full_row(pool, name, P, O):
    jobs = [("SOXL", P, O, T0, None), ("SOXL", P, O, T0, SPLIT), ("SOXL", P, O, "2020-01-02", None)]
    jobs += [("SOXL", P, O, s, e) for s, e in WIN.values()] + [("SOXL", P, O, s, e) for s, e in SUBS]
    jobs += [("SOXL", P, O, "2010-03-12", "2025-12-31"), ("TQQQ", dict(P, v=0), O, T0, None)]
    r = pool.map(ev, jobs)
    return dict(이름=name, CAGR=r[0][0], MDD=r[0][1], Calmar=r[0][2], 앞Calmar=r[1][2], 뒤Calmar=r[2][2], H1=r[3][2], H2=r[4][2], 봉인=r[5][2],
                구간최소=min(x[2] for x in r[6:10]), **{"~2025": r[10][2], "TQQQ CAGR": r[11][0], "TQQQ MDD": r[11][1], "TQQQ Calmar": r[11][2]})


def variants():
    v = [("순정", {})]
    v += [(f"분할 익절 f={f} m={m}", dict(pf=f, pm=m)) for f, m in ((.5, 1.5), (.5, 2), (.5, 3), (.3, 2), (.7, 2), (.7, 3))]
    v += [(f"손절 연장 {n}일 · {int(fr*100)}% 먼저 매도", dict(sx=n, sx_frac=fr)) for n in (5, 10, 20) for fr in (0.0, 0.5)]
    v += [(f"목표 낮추기 d={d}", dict(decay=d)) for d in (.25, .5, .75, 1.0)]
    v += [(f"짝 매도 r={r}", dict(pr=r)) for r in (.5, 1.0, 2.0)]
    v += [(f"묶음 매도 평단+{int(g*100)}% · {int(F*100)}%", dict(bg=g, bF=F)) for g in (.03, .05, .08, .12) for F in (.5, 1.0)]
    v += [(f"평단 기준만 (칸별 목표 없음) +{g*100:.0f}% · {int(F*100)}%", dict(notgt=1, bk=1, bg=g or 1e-9, bF=F))
          for g in (0.0, .01, .02, .03, .05) for F in (.25, .5, 1.0)]
    v += [(f"급락일 더 사기 −{int(x*100)}% 이하 ×{m}", dict(bd=x, bdm=m)) for x in (.05, .08, .12) for m in (0.5, 1.5, 2.0)]
    return v


def main():
    t0 = time.time()
    ctx, df, ms = S.load_soxl()
    from dss_engine import load_price_data, get_week_mode_map
    tq = load_price_data("TQQQ", "2010-02-11", "2026-12-31")
    C = {"SOXL": ctx, "TQQQ": S.make_ctx(tq, get_week_mode_map(ms, tq.index), set())}
    # 복사본 일치 확인 (손잡이 전부 끔 = dssn_sim 과 같아야 함)
    for v in (0, 1):
        a = S.metric(ctx, dict(S.BASE, v=v), "2010-03-12")
        b = metric2(ctx, dict(v=v), {}, "2010-03-12")
        print(f"일치 확인 V={v}: {a[0]:.6f} / {a[1]:.6f}  vs  {b[0]:.6f} / {b[1]:.6f}  차이 {abs(a[0]-b[0]) + abs(a[1]-b[1]):.1e}")
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(C, tmp); tmp.close()
    f2 = lambda v: f"{v:.2f}"
    allrows = []
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        for bn, B in (("공격형V", dict(v=1)), ("공격형", dict(v=0))):
            rows = [full_row(pool, n, B, O) for n, O in variants()]
            d = pd.DataFrame(rows)
            b = d.iloc[0]
            d["통과"] = ["" if i == 0 else ("✅" if all(r[k] > b[k] for k in KEYS) else "") for i, r in d.iterrows()]
            d["TQQQ"] = ["" if i == 0 else ("↑" if r["TQQQ Calmar"] > b["TQQQ Calmar"] else "↓") for i, r in d.iterrows()]
            print(f"\n━━ {bn} + 매도 구조 (SOXL 2011-01-03~, TQQQ 는 V 없이)")
            print(d.to_string(index=False, float_format=f2))
            allrows.append(d.assign(기본=bn))
            print(f"   {time.time() - t0:.0f}초", flush=True)
    os.unlink(tmp.name)
    pd.concat(allrows).to_csv(os.path.join(HERE, "reports", "dssn_sell.csv"), index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
