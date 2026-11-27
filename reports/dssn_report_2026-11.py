"""
DSS-N (정규화 동파) 검토 정리 문서 생성 → reports/dssn_report.html (2026-11-27)

dssn_opt_2026-11.py 가 남긴 dssn_opt_search.csv · dssn_opt_single.csv · dssn_opt_params.json 을 읽고,
자산 곡선 · 연도별 수익률 · 변동성 배율만 다시 계산해 한 장짜리 HTML 로 만든다.
"""
import os, sys, json, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "reports"))
import numpy as np
import pandas as pd
import dssn_sim as S

T0 = "2011-01-03"
R = os.path.join(HERE, "reports")
TARGET_Y = {2011: 0.52, 2012: 26.33, 2013: 120.48, 2014: 92.30, 2015: 52.40, 2016: 104.15, 2017: 80.77, 2018: 23.11,
            2019: 62.88, 2020: 218.76, 2021: 145.01, 2022: 68.04, 2023: 89.00, 2024: 131.12, 2025: 186.36, 2026: 21.93}
C = ["#2a78d6", "#eb6834", "#1a9e8f", "#8a5bd1"]

PNAME = [("sf_div", "안전 분할수", "d"), ("sf_hold", "안전 최대보유일", "d"), ("sf_buy", "안전 매수조건", "%"), ("sf_sell", "안전 매도조건", "%"),
         ("ag_div", "공세 분할수", "d"), ("ag_hold", "공세 최대보유일", "d"), ("ag_buy", "공세 매수조건", "%"), ("ag_sell", "공세 매도조건", "%"),
         ("renew", "투자금 갱신주기(일)", "d"), ("pcr", "이익복리율", "%0"), ("lcr", "손실복리율", "%0"),
         ("risk_div", "위험 달 분할수 (V)", "d"), ("risk_ag_hold", "위험 달 공세 보유일 (V)", "d")]
NNAME = [("win", "변동성을 재는 기간(일)", "d"), ("a_buy_sf", "매수조건 지수 a · 안전", "a"), ("a_buy_ag", "매수조건 지수 a · 공세", "a"),
         ("a_sell_sf", "매도조건 지수 a · 안전", "a"), ("a_sell_ag", "매도조건 지수 a · 공세", "a"),
         ("a_hold_sf", "보유일 지수 a · 안전", "a"), ("a_hold_ag", "보유일 지수 a · 공세", "a"),
         ("s_sf", "티어 기울기 s · 안전", "a"), ("s_ag", "티어 기울기 s · 공세", "a"),
         ("sv_sf", "티어 기울기의 변동성 반응 · 안전", "a"), ("sv_ag", "티어 기울기의 변동성 반응 · 공세", "a")]


def fmt(v, t):
    if t == "d":
        return f"{int(v)}"
    if t == "%":
        return f"{v * 100:.2f}%"
    if t == "%0":
        return f"{v * 100:.0f}%"
    return f"{v:+.2f}"


def table(head, rows, cls=""):
    h = "".join(f"<th>{x}</th>" for x in head)
    b = "".join("<tr>" + "".join(f"<td{' class=k' if i == 0 else ''}>{x}</td>" for i, x in enumerate(r)) + "</tr>" for r in rows)
    return f"<div class='tw'><table class='{cls}'><thead><tr>{h}</tr></thead><tbody>{b}</tbody></table></div>"


def pct(v, d=1, sign=False):
    return f"{v:+.{d}f}%" if sign else f"{v:.{d}f}%"


def main():
    ctx, df, ms = S.load_soxl()
    idx = ctx["idx"]
    runs = json.load(open(os.path.join(R, "dssn_opt_params.json"), encoding="utf-8"))
    d2 = pd.read_csv(os.path.join(R, "dssn_opt_search.csv")).set_index("이름")
    d1 = pd.read_csv(os.path.join(R, "dssn_opt_single.csv")).set_index("이름")
    V = dict(S.BASE)
    i0 = int(idx.searchsorted(pd.Timestamp(T0)))
    i1 = len(idx) - 1
    end = str(idx[-1].date())

    # ── 자산 곡선 (주 1회로 줄임)
    show = [("공격형V (현재)", V), ("정규화 · 전체 기간에 맞춤", runs["N-전체#1"]),
            ("정규화 · 2011~2019 에만 맞춤", runs["N-앞#1"]), ("정규화 없이 · 2011~2019 에만 맞춤", runs["P-앞#1"])]
    step = list(range(0, i1 - i0 + 1, 5))
    if step[-1] != i1 - i0:
        step.append(i1 - i0)
    dates = [str(idx[i0 + k].date()) for k in step]
    curves = []
    for (name, P), col in zip(show, C):
        eq = S.sim(ctx, P, i0, i1, curve=True)[3] / 10000.0
        curves.append(dict(name=name, color=col, vals=[round(float(eq[k]), 4) for k in step]))
    split_i = next(i for i, d in enumerate(dates) if d >= "2020-01-01")

    # ── 변동성 배율 v
    v20, v60 = ctx["vr"][20][i0:], ctx["vr"][60][i0:]
    vser = [dict(name="v (20일 기준)", color=C[0], vals=[round(float(v20[k]), 3) for k in step])]
    vq = np.percentile(v20, [5, 50, 95])
    vmax_i = int(np.argmax(v20)); vmin_i = int(np.argmin(v20))
    vnow = float(v20[-1])

    # ── 연도별
    ys = {"목표 (캡처)": pd.Series(TARGET_Y)}
    for name, P in show:
        ys[name] = S.yearly(ctx, P, T0)["수익률"]
    ydf = pd.DataFrame(ys)

    def ycell(v):
        if v != v:
            return ""
        cls = "neg" if v < 0 else ""
        return f"<span class='{cls}'>{v:+.0f}%</span>"
    yrows = [[n] + [ycell(ydf.loc[y, n]) for y in ydf.index] for n in ydf.columns]
    ytab = table(["연도"] + [str(y) for y in ydf.index], yrows, "yr")

    # ── 결과 표
    def rng(names, col, d=1):
        v = [float(d2.loc[n, col]) for n in names]
        return f"{min(v):.{d}f} ~ {max(v):.{d}f}" if len(v) > 1 else f"{v[0]:.{d}f}"
    grp = [("공격형V (현재)", ["공격형V"], ""),
           ("정규화 포함 · 전체 기간에 맞춤", [f"N-전체#{i}" for i in (1, 2, 3)], "목표와 같은 조건"),
           ("정규화 없이 · 전체 기간에 맞춤", [f"P-전체#{i}" for i in (1, 2, 3)], "대조군"),
           ("정규화 포함 · 2011~2019 에만 맞춤", [f"N-앞#{i}" for i in (1, 2, 3)], "2020~ 은 처음 보는 구간"),
           ("정규화 없이 · 2011~2019 에만 맞춤", [f"P-앞#{i}" for i in (1, 2, 3)], "대조군")]
    rrows = [["목표 (캡처)", "85.1", "−42.1", "2.02", "", "", "", ""]]
    for g, names, note in grp:
        rrows.append([g + (f"<small>{note}</small>" if note else ""), rng(names, "CAGR"), rng(names, "MDD"), rng(names, "Calmar", 2),
                      rng(names, "앞(11~19)"), rng(names, "앞MDD"), rng(names, "뒤(20~26)"), rng(names, "뒤MDD")])
    rtab = table(["", "CAGR %", "MDD %", "Calmar", "2011~2019 CAGR %", "2011~2019 MDD %", "2020~2026 CAGR %", "2020~2026 MDD %"], rrows, "num")

    # ── 손잡이 하나씩
    pick = [("공격형V", "기준 (정규화 없음)"), ("매수% a=+0.50", "매수조건만 정규화 (a = +0.5)"), ("매수% a=+1.00", "매수조건만 정규화 (a = +1.0)"),
            ("매도% a=+0.50", "매도조건만 정규화 (a = +0.5)"), ("매도% a=-0.50", "매도조건만 정규화 (a = −0.5)"),
            ("보유일 a=-0.50", "보유일만 정규화 (a = −0.5, 출렁일수록 짧게)"), ("보유일 a=+0.25", "보유일만 정규화 (a = +0.25, 출렁일수록 길게)"),
            ("티어 기울기 s=-0.30", "티어 비중: 앞 티어를 크게 (s = −0.3)"), ("티어 기울기 s=+0.30", "티어 비중: 뒤 티어를 크게 (s = +0.3)"),
            ("티어 기울기×변동성 sv=+0.50", "티어 비중: 출렁일수록 뒤 티어를 크게 (sv = +0.5)"),
            ("매수·매도 a=+1.00 · 보유 -0.50", "매수 · 매도 · 보유일 모두 (a = +1, 보유 −0.5)")]
    srows = []
    for k, label in pick:
        r = d1.loc[k]
        srows.append([label, f"{r['CAGR']:.1f}", f"{r['MDD']:.1f}", f"{r['Calmar']:.2f}", f"{r['H1']:.2f}", f"{r['H2']:.2f}", f"{r['구간최소']:.2f}"])
    stab = table(["켠 손잡이", "CAGR %", "MDD %", "Calmar", "앞 절반 Calmar", "뒤 절반 Calmar", "4구간 최소"], srows, "num")

    # ── 파라미터 표
    cols = [("공격형V", V), ("정규화 · 전체 맞춤 ①", runs["N-전체#1"]), ("정규화 · 전체 맞춤 ③", runs["N-전체#3"]),
            ("정규화 · 앞 구간 맞춤 ①", runs["N-앞#1"]), ("정규화 없이 · 전체 맞춤 ①", runs["P-전체#1"])]
    prow = [[lab] + [fmt(P[k], t) for _, P in cols] for k, lab, t in PNAME]
    nrow = [[lab] + [fmt(P[k], t) if (t == "d" or abs(P[k]) > 1e-9) else "<span class='mut'>0</span>" for _, P in cols] for k, lab, t in NNAME]
    ptab = table(["기본값"] + [n for n, _ in cols], prow, "num")
    ntab = table(["정규화 손잡이"] + [n for n, _ in cols], nrow, "num")

    # ── 예시: 정규화가 실제 조건을 얼마나 바꾸나
    ex = []
    for a in (0.0, 0.5, 1.0, -0.5):
        ex.append([f"a = {a:+.1f}" + (" (고정)" if a == 0 else ""), *[f"{3.3 * v ** a:.2f}%" for v in (0.5, 1.0, 2.0)]])
    extab = table(["지수 a", "잔잔한 날 (v = 0.5)", "평소 (v = 1.0)", "출렁이는 날 (v = 2.0)"], ex, "num")
    P1 = runs["N-전체#1"]
    b_, s_, h_, dv_, sl_, _qm = S.day_arrays(ctx, P1)
    vv = ctx["vr"][int(P1["win"])]
    ag = ctx["mode"] == 1
    real = []
    for lab, m in (("안전모드", ~ag), ("공세모드", ag)):
        mk = m.copy(); mk[:i0] = False
        lo = mk & (vv <= np.percentile(vv[i0:], 20)); hi = mk & (vv >= np.percentile(vv[i0:], 80))
        real.append([lab, f"{b_[lo].mean() * 100:.2f}% → {b_[hi].mean() * 100:.2f}%", f"{s_[lo].mean() * 100:.2f}% → {s_[hi].mean() * 100:.2f}%",
                     f"{h_[lo].mean():.0f}일 → {h_[hi].mean():.0f}일"])
    realtab = table(["", "매수조건 (잔잔 → 출렁)", "매도조건 (잔잔 → 출렁)", "최대보유일 (잔잔 → 출렁)"], real, "num")

    # ── 2차: 공유받은 방식 그대로
    d3 = pd.read_csv(os.path.join(R, "dssn_recipe.csv"))
    rec = [("순정", "정규화 없음"), ("가격 0.5", "가격 지수 0.5 만"), ("수량 1.0", "수량 지수 1.0 만"),
           ("가격 0.5 + 수량 1.0", "<b>가격 0.5 + 수량 1.0 (공유받은 값)</b>"), ("전부", "가격 · 수량 + RSI + 보유 진행률 전부")]
    r3 = []
    for bn in ("공격형", "공격형V"):
        for k, lab in rec:
            x = d3[(d3["기본"] == bn) & (d3["이름"] == k)].iloc[0]
            r3.append([f"{bn} · {lab}", f"{x['CAGR']:.1f}", f"{x['MDD']:.1f}", f"{x['Calmar']:.2f}", f"{x['H1']:.2f}", f"{x['H2']:.2f}",
                       f"{x['구간최소']:.2f}", f"{x['TQQQ CAGR']:.1f}", f"{x['TQQQ MDD']:.1f}", f"{x['TQQQ Calmar']:.2f}"])
    rectab = table(["", "CAGR %", "MDD %", "Calmar", "앞 절반 Calmar", "뒤 절반 Calmar", "4구간 최소", "TQQQ CAGR %", "TQQQ MDD %", "TQQQ Calmar"], r3, "num")

    # ── 3차: 2025년까지만 맞추고 2026년 보기
    r25 = json.load(open(os.path.join(R, "dssn_opt_2025_params.json"), encoding="utf-8"))
    k0 = int(idx.searchsorted(pd.Timestamp("2026-01-02")))

    def row5(P):
        fit, full = S.metric(ctx, P, T0, "2025-12-31"), S.metric(ctx, P, T0)
        g, mdd, _n = S.sim(ctx, P, k0, i1)
        yr = S.yearly(ctx, P, T0)
        return {"맞춘구간 CAGR": fit[0], "맞춘구간 MDD": fit[1], "2026 수익률(단독)": (g - 1) * 100, "2026 MDD(단독)": mdd * 100,
                "2026 수익률(이어서)": float(yr.loc[2026, "수익률"]), "2026 MDD(이어서)": float(yr.loc[2026, "MDD"]), "전체 Calmar": full[2]}
    all5 = {"공격형": dict(S.BASE, v=0), "공격형V": V}
    all5.update({f"{k} (2026까지 맞춤)": P for k, P in runs.items() if "전체" in k})
    all5.update({f"{k} (2025까지 맞춤)": P for k, P in r25.items()})
    d5 = pd.DataFrame({k: row5(P) for k, P in all5.items()}).T
    cn = "①②③"

    def r5(names, col, d=1):
        v = [float(d5.loc[n, col]) for n in names]
        return f"{v[0]:+.{d}f}" if len(v) == 1 else f"{min(v):+.{d}f} ~ {max(v):+.{d}f}"

    def u5(names, col, d=1):
        return r5(names, col, d).replace("+", "")
    g5 = [("공격형", ["공격형"]), ("<b>공격형V (현재)</b>", ["공격형V"]),
          ("정규화 · 2026년까지 통으로 맞춤 (3회)", [f"N-전체#{i} (2026까지 맞춤)" for i in (1, 2, 3)]),
          ("정규화 없이 · 2026년까지 통으로 맞춤 (3회)", [f"P-전체#{i} (2026까지 맞춤)" for i in (1, 2, 3)])]
    g5 += [(f"<b>정규화 · 2025년까지만 맞춤 {cn[i - 1]}</b>", [f"N-2025#{i} (2025까지 맞춤)"]) for i in (1, 2, 3)]
    g5 += [("정규화 없이 · 2025년까지만 맞춤 (3회)", [f"P-2025#{i} (2025까지 맞춤)" for i in (1, 2, 3)])]

    def neg(t):
        return f"<span class='neg'>{t}</span>" if t.startswith("-") else t
    t5rows = [[g, u5(n, "맞춘구간 CAGR"), u5(n, "맞춘구간 MDD"), neg(r5(n, "2026 수익률(이어서)")), u5(n, "2026 MDD(이어서)"),
               neg(r5(n, "2026 수익률(단독)")), u5(n, "2026 MDD(단독)"), u5(n, "전체 Calmar", 2)] for g, n in g5]
    t5 = table(["", "2011~2025 CAGR %", "2011~2025 MDD %", "2026년 수익률 % (이어서 운용)", "2026년 연중 MDD % (이어서 운용)",
                "2026년 수익률 % (연초에 새로 시작)", "2026년 MDD % (연초에 새로 시작)", "전체 기간 Calmar"], t5rows, "num")
    n25 = [d5.loc[f"N-2025#{i} (2025까지 맞춤)"] for i in (1, 2, 3)]
    p25 = [d5.loc[f"P-2025#{i} (2025까지 맞춤)"] for i in (1, 2, 3)]
    nf = [d5.loc[f"N-전체#{i} (2026까지 맞춤)"] for i in (1, 2, 3)]
    v5 = d5.loc["공격형V"]

    # 2026년 자산 곡선 (2025년 마지막 거래일 = 1) · 월별
    show5 = [("공격형V (현재)", V, C[0], ""), ("정규화 · 2026년까지 통으로 맞춤 ①", runs["N-전체#1"], C[3], "")]
    show5 += [(f"정규화 · 2025년까지만 맞춤 {cn[i]}", r25[f"N-2025#{i + 1}"], C[1], ["", "6 4", "2 3"][i]) for i in range(3)]
    j0 = int(idx.searchsorted(pd.Timestamp("2026-01-01"))) - 1
    dates26 = [str(d.date()) for d in idx[j0:]]
    cur26, mrows = [], []
    for name, P, col, dash in show5:
        eq = pd.Series(S.sim(ctx, P, i0, i1, curve=True)[3], index=idx[i0:])
        e26 = eq[idx[j0]:]
        cur26.append(dict(name=name, color=col, dash=dash, vals=[round(float(x), 4) for x in e26 / e26.iloc[0]]))
        m = eq.groupby(eq.index.to_period("M")).last().pct_change() * 100
        m = m[m.index.year == 2026]
        mrows.append([name] + [neg(f"{x:+.1f}") for x in m])
    mtab = table([""] + [f"{k.month}월" for k in m.index], mrows, "yr")
    leg26 = "".join("<span><i style='background:" + (c["color"] if not c["dash"] else f"repeating-linear-gradient(90deg,{c['color']} 0 4px,transparent 4px 7px)")
                    + f"'></i>{c['name']}</span>" for c in cur26)
    # 찾은 값 (2025 맞춤)
    cols5 = [("공격형V", V)] + [(f"2025 맞춤 {cn[i]}", r25[f"N-2025#{i + 1}"]) for i in range(3)]
    ptab5 = table(["기본값"] + [n for n, _ in cols5], [[lab] + [fmt(P[k], t) for _, P in cols5] for k, lab, t in PNAME], "num")

    data = dict(dates=dates, curves=curves, split=split_i, vser=vser, dates26=dates26, cur26=cur26)
    n1, n3 = d2.loc["N-전체#1"], d2.loc["N-전체#3"]
    na = [d2.loc[f"N-앞#{i}"] for i in (1, 2, 3)]
    y22 = float(ydf.loc[2022, "정규화 · 2011~2019 에만 맞춤"])

    html = f"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>DSS 정규화 검토</title>
<style>
:root{{--bg:#fbfaf7;--card:#fff;--ink:#1d1f23;--ink2:#555b66;--mut:#8a909b;--line:#e4e1da;--acc:#2a78d6;--bad:#c2372f;--good:#1f8a4c;--warnbg:#fff6e5;--warnln:#e8c787}}
@media (prefers-color-scheme:dark){{:root{{--bg:#15171a;--card:#1d2024;--ink:#e8e9ec;--ink2:#b3b8c2;--mut:#7f8691;--line:#30343a;--acc:#5a9cec;--bad:#ef6b62;--good:#4cc27e;--warnbg:#2c2618;--warnln:#6b5a2c}}}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:16px/1.7 "Pretendard","Malgun Gothic",system-ui,sans-serif}}
main{{max-width:980px;margin:0 auto;padding:32px 16px 80px}}
h1{{font-size:30px;line-height:1.3;margin:0 0 6px}}h2{{font-size:22px;margin:52px 0 10px;padding-top:18px;border-top:1px solid var(--line)}}h3{{font-size:17px;margin:26px 0 6px}}
p{{margin:8px 0}}.sub{{color:var(--ink2)}}small{{display:block;color:var(--mut);font-size:12.5px;line-height:1.4}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px 18px;margin:14px 0}}
.verdict{{border-left:5px solid var(--acc)}}.warn{{background:var(--warnbg);border-color:var(--warnln)}}
.kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px;margin:14px 0}}
.kpi{{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px 14px}}.kpi b{{display:block;font-size:24px;line-height:1.25;font-variant-numeric:tabular-nums}}.kpi span{{color:var(--ink2);font-size:13.5px}}
.tw{{overflow-x:auto;margin:10px 0}}table{{border-collapse:collapse;width:100%;font-size:14px}}
th,td{{padding:7px 9px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}}
th{{color:var(--ink2);font-weight:600;font-size:12.5px;white-space:normal;vertical-align:bottom}}td.k,th:first-child{{text-align:left;white-space:normal;min-width:150px}}
table.yr td,table.yr th{{padding:6px 5px;font-size:12.5px}}.neg{{color:var(--bad);font-weight:600}}.mut{{color:var(--mut)}}
.formula{{font-family:Consolas,monospace;background:var(--bg);border:1px solid var(--line);border-radius:8px;padding:10px 14px;margin:10px 0;overflow-x:auto;white-space:nowrap}}
.chart{{position:relative;margin:8px 0}}.chart svg{{display:block;width:100%;height:auto}}
.tip{{position:absolute;pointer-events:none;background:var(--card);border:1px solid var(--line);border-radius:8px;padding:6px 9px;font-size:12.5px;line-height:1.5;box-shadow:0 4px 14px rgba(0,0,0,.15);display:none;white-space:nowrap}}
.legend{{display:flex;flex-wrap:wrap;gap:6px 16px;font-size:13px;color:var(--ink2);margin:6px 0}}.legend i{{display:inline-block;width:14px;height:3px;border-radius:2px;vertical-align:middle;margin-right:6px}}
ol,ul{{padding-left:22px}}li{{margin:4px 0}}code{{font-family:Consolas,monospace;font-size:13.5px;background:var(--bg);border:1px solid var(--line);border-radius:5px;padding:1px 5px}}
</style></head><body><main>

<h1>DSS 정규화 검토 — 목표 CAGR 85%를 따라가 본 결과</h1>
<p class="sub">SOXL · {T0} ~ {end} · 작성 2026-11-27 · 배포 앱과 엔진은 바꾸지 않은 실험입니다</p>

<div class="card verdict"><b>결론: 목표 수치는 맞춰지지만, 실전에 쓸 수 있는 값은 아니었습니다.</b>
<p>과거 16년 전체에 값을 맞추면 CAGR {n1['CAGR']:.1f}% · MDD {n1['MDD']:.1f}%가 나와 목표(85.1% · −42.1%)에 도달합니다.
그런데 같은 방법으로 2011~2019년에만 맞춘 값을 2020~2026년에 그대로 쓰면 MDD가 {min(r['뒤MDD'] for r in na):.0f}%까지 무너졌습니다.
과거를 잘 설명하는 값을 찾은 것이지, 앞으로도 통하는 규칙을 찾은 것이 아니라는 뜻입니다.</p>
<p>2025년까지만 맞추고 2026년을 처음 보는 해로 두어도 같았습니다. 세 번의 결과가 {n25[0]['2026 수익률(이어서)']:+.0f}% · {n25[1]['2026 수익률(이어서)']:+.0f}% · {n25[2]['2026 수익률(이어서)']:+.0f}%로 흩어졌고, 연중 낙폭은 세 번 모두 공격형V({v5['2026 MDD(이어서)']:.0f}%)보다 컸습니다 (7번).</p></div>

<div class="kpis">
<div class="kpi"><b>62.5% / −43.7%</b><span>공격형V (현재 배포)</span></div>
<div class="kpi"><b>{n1['CAGR']:.1f}% / {n1['MDD']:.1f}%</b><span>정규화 · 전체 기간에 맞춤</span></div>
<div class="kpi"><b>{min(r['뒤(20~26)'] for r in na):.0f}~{max(r['뒤(20~26)'] for r in na):.0f}% / {min(r['뒤MDD'] for r in na):.0f}%</b><span>정규화 · 앞 9년에 맞춘 값의 2020~2026 성과</span></div>
<div class="kpi"><b>74.5% / −42.0%</b><span>공격형V 의 2020~2026 성과</span></div>
</div>

<h2>1. 정규화가 무엇인가</h2>
<p>지금 DSS 는 조건이 고정입니다. 공격형의 안전모드 매수조건은 시장이 잔잔하든 폭락 중이든 항상 "전일 종가 +3.3% 이하"입니다.</p>
<p>정규화는 <b>"오늘 시장이 평소보다 얼마나 출렁이는가"를 먼저 재고, 그 크기에 맞춰 조건을 늘이거나 줄이는 것</b>입니다.
하루에 2%씩 움직이는 시기의 3%와, 하루에 8%씩 움직이는 시기의 3%는 같은 3%가 아니라는 생각에서 나왔습니다.</p>

<h3>출렁임의 크기 v</h3>
<div class="formula">v = (전일까지 최근 N일의 SOXL 일간 수익률 표준편차) ÷ 4.5%</div>
<ul><li>v = 1 이면 평소, v = 2 면 평소의 두 배로 출렁이는 날, v = 0.5 면 절반만 움직이는 잔잔한 날입니다.</li>
<li>전일까지의 값만 쓰므로 주문표를 만드는 시점에 이미 알 수 있습니다 (미래 정보 없음).</li>
<li>2011년 이후 v(20일)는 대부분 {vq[0]:.2f} ~ {vq[2]:.2f} 사이였고, 가장 낮았던 날은 {v20[vmin_i]:.2f} ({str(idx[i0 + vmin_i].date())}), 가장 높았던 날은 {v20[vmax_i]:.2f} ({str(idx[i0 + vmax_i].date())}), 지금은 {vnow:.2f} 입니다.</li></ul>
<div class="chart" id="c_v"></div>

<h3>조건을 바꾸는 식</h3>
<div class="formula">그날의 값 = 기본값 × v<sup>a</sup></div>
<p>지수 a 가 "얼마나 따라 움직일지"를 정합니다. a = 0 이면 지금처럼 고정, a = 1 이면 출렁임에 정비례, 음수면 반대로 움직입니다. 매수조건 3.3%를 예로 들면:</p>
{extab}
<p>이 식을 네 군데에 넣었습니다. 받은 설명("매수 · 매도 · 손절일 · 티어 비중에 정규화 적용")과 같은 네 곳입니다.</p>
{table(["대상", "정규화하면 달라지는 것", "손잡이"], [
    ["매수조건 %", "출렁이는 날에는 더 넓게(혹은 좁게) 매수 주문가를 잡음", "a (안전 · 공세 따로)"],
    ["매도조건 %", "출렁이는 날에 산 물량은 목표 수익률을 더 크게(혹은 작게)", "a (안전 · 공세 따로)"],
    ["최대보유일 (손절일)", "출렁이는 날에 산 물량은 더 짧게(혹은 길게) 들고 감", "a (안전 · 공세 따로)"],
    ["티어 비중", "앞 티어와 뒤 티어 중 어디에 돈을 더 실을지, 그리고 출렁임에 따라 그 기울기를 바꿀지", "기울기 s · 변동성 반응 sv"]])}
<p class="sub">티어 비중은 <code>1 + s × (2u − 1)</code> (u = 0 첫 티어 ~ 1 마지막 티어) 모양으로 정했습니다. s 가 양수면 뒤 티어를 크게, 음수면 앞 티어를 크게 삽니다.
분할수가 7이든 12든 같은 모양이 되도록 한 것이 '정규화'에 해당합니다.</p>
<div class="card warn"><b>제가 가정한 부분</b><br>출렁임을 재는 기준으로 <b>SOXL 일간 변동성</b> 한 가지만 썼습니다. 캡처를 만든 분이 어떤 기준(ATR, 수익률 순위, RSI 등)을 썼는지는 모릅니다.
이후 대화방 자료로 실제 방식(가격 지수 0.5 · 수량 지수 1.0 · RSI · 보유 진행률)이 확인되어, 그 방식 그대로 돌린 결과를 6번에 따로 실었습니다.</div>

<h2>2. 어떻게 검토했나</h2>
<ol>
<li><b>실험용 복사본 만들기.</b> 배포 엔진은 그대로 두고, 날마다 조건을 바꿀 수 있는 복사본 시뮬레이터를 만들었습니다. 정규화를 끈 상태에서 공격형 · 공격형V 를 8개 구간으로 돌려 배포 엔진과 결과가 소수점까지 같은지 확인했습니다 (차이 0).</li>
<li><b>손잡이를 하나씩만 켜 보기.</b> 공격형V 의 값은 그대로 두고 정규화 손잡이 하나만 움직여, 정규화 자체에 효과가 있는지 봤습니다.</li>
<li><b>전부 함께 탐색하기.</b> 기본값 12개와 정규화 손잡이 11개를 한꺼번에 움직여 "MDD 42% 이내에서 CAGR 이 가장 높은 값"을 찾았습니다. 한 번에 약 5,600가지 조합을 시험하고, 출발점을 바꿔 세 번 반복했습니다.</li>
<li><b>대조군 두 가지.</b> ① 정규화 손잡이 없이 기본값만 같은 양만큼 탐색 (정규화의 몫을 가려냄). ② 2011~2019년 자료만 보고 값을 찾은 뒤 2020~2026년에 그대로 적용 (처음 보는 구간에서도 통하는지 확인).</li>
<li><b>흔들어 보기.</b> 찾은 값을 ±10% 범위에서 200번 흔들어 성과가 유지되는지 봤습니다.</li>
</ol>

<h2>3. 손잡이를 하나씩만 켰을 때</h2>
<p>정규화 자체가 좋은 아이디어라면 손잡이 하나만 켜도 어느 정도는 나아져야 합니다. 결과는 거의 제자리였습니다.</p>
{stab}
<ul><li>매수 · 매도 · 보유일 정규화는 공격형V 와 같거나 나빴습니다. 세 가지를 모두 켜면 MDD 가 −55%까지 커졌습니다.</li>
<li>"출렁일수록 뒤 티어를 크게"만 Calmar 1.43 → 1.47 로 조금 올랐는데, 앞 절반은 그대로이고 개선이 뒤 절반에만 있었습니다.</li></ul>

<h2>4. 전부 함께 탐색했을 때</h2>
<p>각 줄은 출발점을 바꿔 세 번 탐색한 결과의 범위입니다.</p>
{rtab}
<div class="legend">{"".join(f"<span><i style='background:{c['color']}'></i>{c['name']}</span>" for c in curves)}</div>
<div class="chart" id="c_eq"></div>
<p class="sub">자산 1 에서 시작한 배수, 세로축은 로그 눈금입니다. 세로 점선(2020년) 오른쪽이 "앞 구간에만 맞춘 값"이 처음 보는 구간입니다.</p>
<h3>읽는 법</h3>
<ul>
<li><b>전체 기간에 맞추면 목표에 도달합니다.</b> {n1['CAGR']:.1f}% / {n1['MDD']:.1f}%, {n3['CAGR']:.1f}% / {n3['MDD']:.1f}% 가 나왔습니다.</li>
<li><b>처음 보는 구간에서는 무너집니다.</b> 2011~2019년에 CAGR {na[0]['앞(11~19)']:.0f}%였던 값이 2022년 한 해에 {y22:.0f}%가 났습니다. 2024년과 2026년도 손실이었습니다.</li>
<li><b>손잡이가 많을수록 더 심하게 무너집니다.</b> 정규화 없이 맞춘 값은 뒤 구간 MDD −60%, 정규화까지 넣어 맞춘 값은 −88%였습니다. 손잡이 23개는 과거의 우연까지 외울 만큼 많습니다.</li>
<li><b>전체에 맞춘 값도 개선이 뒤쪽에만 있습니다.</b> 앞 절반 Calmar 는 1.37~1.53 으로 공격형V(1.44)와 같습니다.</li>
<li><b>±10% 흔들면</b> 전체 맞춤 값의 CAGR 은 80~87%로 유지되지만 MDD 는 중앙값 −45%, 최악 −53~−59%로 커집니다. −42% 라는 숫자는 딱 그 값에서만 나오는 숫자입니다.</li>
</ul>
<h3>연도별 수익률</h3>
{ytab}

<h2>5. 찾은 파라미터</h2>
<p>참고용입니다. 아래 값을 앱에 넣어 쓰는 것은 권하지 않습니다.</p>
{ptab}
{ntab}
<p>"전체 맞춤 ①"의 설정이 실제로 조건을 얼마나 바꿨는지, 잔잔한 날(v 하위 20%)과 출렁이는 날(v 상위 20%)의 평균으로 보면:</p>
{realtab}
<p class="sub">세 번의 탐색이 서로 다른 값(분할수 5 / 7 / 8, 변동성 기간 10 / 20 / 60일, 지수의 부호까지)을 찾았습니다. 뚜렷한 정답이 있었다면 비슷한 곳으로 모였을 것입니다.</p>

<h2>6. 공유받은 방식 그대로 돌려 보기 (2차)</h2>
<p>위 탐색을 마친 뒤 대화방 자료에서 실제로 쓴 방식이 확인되어, 그 방식 그대로 다시 돌렸습니다.</p>
{table(["대상", "방식", "값"], [
    ["매수가 · 매도가", "원래 % × (오늘 변동 ÷ 평소 변동)<sup>지수</sup>", "가격 정규화 지수 0.5"],
    ["물량 (칸 크기)", "원래 수량 × (평소 변동 ÷ 오늘 변동)<sup>지수</sup> — 출렁이면 작게, 잔잔하면 크게 (상한 2배)", "수량 정규화 지수 1.0"],
    ["RSI", "RSI 가 낮으면 매수 폭 크게 · 익절 멀리 · 더 오래 들기, 오를 때 크게 사기", "예시 (정확한 값 없음)"],
    ["보유 진행률", "칸이 찰수록 작게 사기", "예시 (정확한 값 없음)"]])}
<p class="sub">'평소 변동'은 그 종목의 최근 1년 중앙값으로 잡았습니다. 종목마다 자기 기준을 쓰므로 TQQQ 에도 같은 값을 그대로 넣을 수 있습니다. TQQQ 열은 V 없이 돌린 결과입니다.</p>
{rectab}
<h3>읽는 법</h3>
<ul>
<li><b>공격형 위에서는 수량 정규화가 효과가 있습니다.</b> 수량 지수 1.0 만 켜면 MDD −59.5% → −47.8%, Calmar 1.08 → 1.33 입니다. "출렁일 때 작게 산다"는 한 가지가 낙폭을 줄였습니다.</li>
<li><b>공격형V 위에서는 오히려 나빠집니다.</b> Calmar 1.43 → 1.29 (수량만), 1.10 (가격 + 수량). 공격형V 가 이미 "변동성이 큰 달에는 12분할로 작게 산다"를 하고 있어서, 같은 일을 두 번 하는 셈이 됩니다.</li>
<li><b>가격 정규화는 SOXL 에서 도움이 되지 않았습니다.</b> 공격형 1.08 → 0.97, 공격형V 1.43 → 1.27.</li>
<li><b>TQQQ 에서는 정규화가 통했습니다.</b> 순정 Calmar 0.52 가 0.60 ~ 0.81 로 올랐고, 손잡이 7개를 무작위로 600가지 넣어 봐도 93%가 순정보다 나았습니다. V 같은 장치가 없는 종목에서는 수량 정규화가 그 역할을 대신합니다.</li>
<li><b>손잡이 7개 무작위 600조합 중 공격형V 를 전체 기간에서 넘은 것은 0개</b>였습니다. 앞 구간 성적과 뒤 구간 성적의 상관도 +0.05 로, 앞에서 좋았던 값이 뒤에서도 좋다는 근거가 없었습니다.</li>
<li><b>84% / −46% 는 재현되지 않았습니다.</b> 그쪽은 '커스텀 티어'(티어별로 따로 맞춘 값)가 바탕이고 기본 성과도 55.1% / −55.5% 로 우리 공격형(64.5% / −59.5%)과 다릅니다. 티어별 값을 모르는 상태에서는 같은 숫자를 만들 수 없습니다.</li>
</ul>
<div class="card verdict"><b>정리:</b> 정규화의 알맹이는 "변동성이 클 때 작게 산다"이고, 이것은 공격형V 가 이미 하고 있습니다. 그래서 공격형V 에 얹으면 더할 것이 없습니다.
쓸모가 있다면 V 가 없는 다른 종목(TQQQ 등)에 DSS 를 적용할 때입니다.</div>

<h2>7. 2025년까지만 맞추고 2026년 보기 (3차)</h2>
<p>4번에서 2026년까지 통으로 맞춘 값은 2026년 10~11월 폭락까지 본 상태에서 고른 값입니다.
그래서 같은 탐색(같은 손잡이 23개 · 같은 목표 · 같은 탐색량)을 <b>2011-01-03 ~ 2025-12-31 에만</b> 적용하고, 2026년은 탐색이 한 번도 보지 못한 해로 남겼습니다.</p>
{t5}
<p class="sub">'이어서 운용'은 2011년부터 계속 굴려 온 계좌의 2026년, '연초에 새로 시작'은 2026-01-02 에 빈 계좌로 시작한 경우입니다. 2026년은 {end} 까지이고, 이 문서를 만든 날의 종가로 다시 계산한 값이라 탐색 당시에 저장한 <code>dssn_opt_2025.csv</code> 와는 조금 다를 수 있습니다.</p>
<div class="legend">{leg26}</div>
<div class="chart" id="c_26"></div>
<p class="sub">2025년 마지막 거래일의 자산을 1 로 둔 2026년 자산입니다 (이어서 운용 기준).</p>
<h3>2026년 월별 수익률 (%)</h3>
{mtab}
<h3>읽는 법</h3>
<ul>
<li><b>맞춘 구간에서는 MDD 가 모두 −42% 안팎이었는데, 2026년에는 {min(r['2026 MDD(이어서)'] for r in n25):.0f} ~ {max(r['2026 MDD(이어서)'] for r in n25):.0f}%가 났습니다.</b> 공격형V 는 {v5['2026 MDD(이어서)']:.1f}%로 원래 수준을 지켰습니다.</li>
<li><b>세 번의 결과가 흩어졌습니다.</b> 2025년까지는 셋 다 CAGR {min(r['맞춘구간 CAGR'] for r in n25):.0f}~{max(r['맞춘구간 CAGR'] for r in n25):.0f}%로 구분이 되지 않았는데, 2026년에는 {n25[0]['2026 수익률(이어서)']:+.0f}% · {n25[1]['2026 수익률(이어서)']:+.0f}% · {n25[2]['2026 수익률(이어서)']:+.0f}%였습니다. 어느 것이 좋을지 미리 고를 방법이 없었습니다.</li>
<li><b>통으로 맞춘 값의 2026년 {min(r['2026 수익률(이어서)'] for r in nf):+.0f} ~ {max(r['2026 수익률(이어서)'] for r in nf):+.0f}%는 폭락을 미리 보고 고른 결과입니다.</b> 같은 방법이 2026년을 보지 못하면 위 숫자가 됩니다.</li>
<li><b>①은 수익률만 보면 공격형V 보다 높습니다</b> ({n25[0]['2026 수익률(이어서)']:+.0f}% 대 {v5['2026 수익률(이어서)']:+.0f}%). 9월까지 더 공격적으로 벌어 둔 덕이고, 그 대가로 연중 낙폭이 {n25[0]['2026 MDD(이어서)']:.0f}%였습니다.</li>
<li><b>오를 때 더 벌고 내릴 때 더 잃었습니다.</b> 오른 달(1 · 2 · 4 · 5 · 6 · 9월)에는 매번 공격형V 보다 많이 벌었지만, 7월과 10월 하락에서 훨씬 크게 잃었습니다 (7월 −19 ~ −33% 대 −7%, 10월 −31 ~ −41% 대 −27%). 찾은 값이 셋 다 평소 6분할에 이익복리율이 100% 가까이로, 공격형V(7분할 · 75%)보다 한 번에 크게 사는 설정입니다.</li>
<li><b>정규화 없이 맞춘 값은 2026년에 더 나빴습니다</b> ({min(r['2026 수익률(이어서)'] for r in p25):+.0f} ~ {max(r['2026 수익률(이어서)'] for r in p25):+.0f}%, MDD {min(r['2026 MDD(이어서)'] for r in p25):.0f}%). 4번의 2011~2019년 맞춤에서는 정규화 쪽이 더 크게 무너졌으니, 어느 쪽이 덜 무너지는지는 구간마다 다릅니다. 둘 다 공격형V 보다 낙폭이 컸다는 점만 같습니다.</li>
</ul>
<h3>2025년까지 맞춰 찾은 기본값</h3>
{ptab5}
<div class="card verdict"><b>정리:</b> 처음 보는 구간을 7년(2020~2026)으로 잡든 1년(2026)으로 잡든 결과가 같습니다. 맞춘 구간의 MDD −42%는 처음 보는 구간에서 지켜지지 않았고, 공격형V 만 제 수준을 유지했습니다. 채택하지 않습니다.</div>

<h2>8. 한계와 다음에 확인할 것</h2>
<ul>
<li>'커스텀 티어'의 티어별 값을 모릅니다. 그 값이 있어야 84% / −46% 를 같은 조건으로 따져 볼 수 있습니다.</li>
<li>변동 기준으로 일간 수익률 표준편차를 썼습니다 (자료의 'TR 자리'와 같은 역할이지만 계산식은 다를 수 있습니다).</li>
<li>티어 비중은 "앞 ↔ 뒤 기울기" 한 가지 모양만 시험했습니다.</li>
<li>교차 종목은 TQQQ 하나만 봤습니다.</li>
<li>7번의 처음 보는 구간은 2026년 한 해(11개월)뿐입니다. 한 해의 결과이므로 4번의 2020~2026년 결과와 함께 봐야 합니다.</li>
</ul>
<p>캡처를 만든 분께 확인하면 좋은 것:</p>
<ol>
<li>커스텀 티어의 티어별 값 (매수 · 매도 · 손절일 · 비중).</li>
<li>값을 정할 때 전체 기간을 다 썼는지, 일부 기간을 떼어 두고 확인했는지.</li>
<li>모드 판정(QQQ 주간 RSI)과 수수료, 시작일이 우리와 같은지.</li>
</ol>

<h2>재현</h2>
<p class="sub">폴더 <code>D:\\04.backtest\\02.종가평균매매\\reports\\</code><br>
<code>dssn_sim.py</code> 실험용 시뮬레이터 · <code>dssn_opt_2026-11.py</code> 탐색 (약 36분) · <code>dssn_report_2026-11.py</code> 이 문서 생성<br>
<code>dssn_recipe_2026-11.py</code> 공유받은 방식 그대로 (2차, 약 2분)<br>
<code>dssn_opt_2025_2026-11.py</code> 2025년까지만 맞추고 2026년 보기 (3차, 약 20분) → <code>dssn_opt_2025.csv</code> · <code>dssn_opt_2025_params.json</code><br>
<code>dssn_opt_search.csv</code> · <code>dssn_opt_single.csv</code> · <code>dssn_opt_params.json</code> · <code>dssn_recipe.csv</code> · <code>dssn_recipe_random_공격형.csv</code> · <code>dssn_recipe_random_공격형V.csv</code> 결과</p>

</main>
<script>
const D = {json.dumps(data, ensure_ascii=False)};
function chart(id, o) {{
  const el = document.getElementById(id), W = 940, H = o.h || 380, L = 54, Rr = 14, T = 12, B = 30;
  const DT = o.dates || D.dates, n = DT.length, tr = v => o.log ? Math.log10(v) : v;
  let lo = Infinity, hi = -Infinity;
  o.series.forEach(s => s.vals.forEach(v => {{ lo = Math.min(lo, tr(v)); hi = Math.max(hi, tr(v)); }}));
  const st = o.step || 1;
  if (o.log) {{ lo = Math.floor(lo); hi = Math.ceil(hi); }} else {{ lo = 0; hi = Math.ceil(hi / st) * st; }}
  const x = i => L + (W - L - Rr) * i / (n - 1), y = v => T + (H - T - B) * (1 - (tr(v) - lo) / (hi - lo));
  let g = '';
  for (let k = lo; k <= hi + 1e-9; k += st) {{
    const yy = T + (H - T - B) * (1 - (k - lo) / (hi - lo));
    g += `<line x1="${{L}}" x2="${{W - Rr}}" y1="${{yy}}" y2="${{yy}}" stroke="var(--line)"/>`;
    g += `<text x="${{L - 8}}" y="${{yy + 4}}" text-anchor="end" font-size="12" fill="var(--mut)">${{o.log ? (10 ** k).toLocaleString() + '배' : k.toFixed(st < 1 ? 1 : 0) + (o.unit || '')}}</text>`;
  }}
  let last = '';
  if (o.monthly) DT.forEach((d, i) => {{ const m = d.slice(0, 7); if (m !== last && i) g += `<text x="${{x(i)}}" y="${{H - 9}}" text-anchor="middle" font-size="12" fill="var(--mut)">${{+d.slice(5, 7)}}월</text>`; last = m; }});
  else DT.forEach((d, i) => {{ const yr = d.slice(0, 4); if (yr !== last && (+yr % 2 === 1)) g += `<text x="${{x(i)}}" y="${{H - 9}}" text-anchor="middle" font-size="12" fill="var(--mut)">${{yr}}</text>`; last = yr; }});
  if (o.split != null) g += `<line x1="${{x(o.split)}}" x2="${{x(o.split)}}" y1="${{T}}" y2="${{H - B}}" stroke="var(--ink2)" stroke-dasharray="4 4"/><text x="${{x(o.split) + 6}}" y="${{T + 12}}" font-size="12" fill="var(--ink2)">2020년 → 처음 보는 구간</text>`;
  if (o.base != null) g += `<line x1="${{L}}" x2="${{W - Rr}}" y1="${{y(o.base)}}" y2="${{y(o.base)}}" stroke="var(--ink2)" stroke-dasharray="4 4"/><text x="${{W - Rr - 4}}" y="${{y(o.base) - 6}}" text-anchor="end" font-size="12" fill="var(--ink2)">${{o.baseLabel || '평소 (v = 1)'}}</text>`;
  o.series.forEach(s => {{ g += `<path d="${{s.vals.map((v, i) => (i ? 'L' : 'M') + x(i).toFixed(1) + ' ' + y(v).toFixed(1)).join('')}}" fill="none" stroke="${{s.color}}" stroke-width="2" stroke-linejoin="round"${{s.dash ? ` stroke-dasharray="${{s.dash}}"` : ''}}/>`; }});
  g += `<line class="cx" x1="0" x2="0" y1="${{T}}" y2="${{H - B}}" stroke="var(--ink2)" stroke-width="1" visibility="hidden"/>`;
  el.innerHTML = `<svg viewBox="0 0 ${{W}} ${{H}}" role="img" aria-label="${{o.label}}">${{g}}</svg><div class="tip"></div>`;
  const svg = el.querySelector('svg'), cx = el.querySelector('.cx'), tip = el.querySelector('.tip');
  svg.addEventListener('mousemove', e => {{
    const r = svg.getBoundingClientRect(), px = (e.clientX - r.left) * W / r.width;
    const i = Math.max(0, Math.min(n - 1, Math.round((px - L) / (W - L - Rr) * (n - 1))));
    cx.setAttribute('x1', x(i)); cx.setAttribute('x2', x(i)); cx.setAttribute('visibility', 'visible');
    tip.innerHTML = `<b>${{DT[i]}}</b><br>` + o.series.map(s => `<span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:${{s.color}};margin-right:5px"></span>${{s.name}}: <b>${{o.fmt(s.vals[i])}}</b>`).join('<br>');
    tip.style.display = 'block';
    const tx = x(i) * r.width / W;
    tip.style.left = (tx > r.width / 2 ? tx - tip.offsetWidth - 12 : tx + 12) + 'px'; tip.style.top = '8px';
  }});
  svg.addEventListener('mouseleave', () => {{ cx.setAttribute('visibility', 'hidden'); tip.style.display = 'none'; }});
}}
chart('c_eq', {{series: D.curves, log: true, split: D.split, label: '자산 곡선 비교', fmt: v => v.toLocaleString(undefined, {{maximumFractionDigits: 1}}) + '배'}});
chart('c_26', {{series: D.cur26, dates: D.dates26, h: 340, step: 0.5, unit: '배', monthly: true, base: 1, baseLabel: '2025년 말', label: '2026년 자산 비교', fmt: v => (v >= 1 ? '+' : '') + ((v - 1) * 100).toFixed(1) + '%'}});
chart('c_v', {{series: D.vser, h: 240, base: 1, label: '변동성 배율 v 추이', fmt: v => v.toFixed(2)}});
</script></body></html>"""
    out = os.path.join(R, "dssn_report.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    print(out, len(html))
    print("v20 pct", vq, "now", vnow, "y22", y22)
    print(ydf.round(0).T.to_string())
    print(real)


if __name__ == "__main__":
    main()
