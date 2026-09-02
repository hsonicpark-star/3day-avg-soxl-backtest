# -*- coding: utf-8 -*-
"""tier_ratio.py + tier_detail.py 결과 → 티어 비율 리포트 HTML 생성."""
import os, io
import pandas as pd

from _chart import line_chart

SP = os.path.dirname(os.path.abspath(__file__))
J = lambda *a: os.path.join(SP, *a)

FULL, IS_T, OOS_T = "전체 2011~", "IS 2017~2023", "OOS 2024~2026"
C_MA, C_CT, C_SUM = "#C0662B", "#3E8A6E", "var(--accent)"

SW = pd.read_csv(J("tier_sweep.csv"))
GD = pd.read_csv(J("tier_grid.csv"))
BT = pd.read_csv(J("tier_bytier.csv"))
FI = pd.read_csv(J("tier_fine.csv"))
FL = pd.read_csv(J("tier_floor.csv"))

sw = SW[SW["시나리오"] == FULL]
gd = GD[(GD["시나리오"] == FULL) & (GD["이평4일_1티어%"] == GD["중심주가_1티어%"])]


def sr(name):
    d = sw[sw["전략"] == name].sort_values("1티어%")
    return list(zip(d["1티어%"], d["Calmar"]))


CHART1 = line_chart(
    [("이평4일", sr("이평4일"), C_MA), ("중심주가", sr("중심주가"), C_CT),
     ("50:50 합산", list(zip(gd["이평4일_1티어%"], gd["Calmar"])), C_SUM)],
    ylab="Calmar", xlab="1티어 시드 비율",
    xticks=[1, 10, 20, 30, 40, 50, 60, 70, 80, 90])

fi = lambda n: list(zip(FI[FI["전략"] == n]["1티어%"], FI[FI["전략"] == n]["Calmar"]))
CHART2 = line_chart([("이평4일", fi("이평4일"), C_MA), ("중심주가", fi("중심주가"), C_CT)],
                    ylab="Calmar", xlab="1티어 시드 비율 (확대)",
                    xticks=[0.1, 0.5, 1, 2, 3, 5])

# ── 주요 비율 표 (합산 기준) ──────────────────────────────
base = gd[gd["이평4일_1티어%"] == 1].iloc[0]
rows = []
for _, r in gd.iterrows():
    v = r["이평4일_1티어%"]
    if v not in (1, 5, 10, 15, 20, 25, 30, 40, 50, 70, 90):
        continue
    d = (r["Calmar"] - base["Calmar"]) / base["Calmar"] * 100
    cur = ' class="sum"' if v == 1 else ""
    tag = ('<span style="font-size:10px;letter-spacing:.08em;color:var(--accent);'
           'font-weight:600"> 현재</span>' if v == 1 else "")
    rows.append(
        f'<tr{cur}><td class="n">{v:.0f}% / {100-v:.0f}%{tag}</td>'
        f'<td class="n">{r["CAGR"]*100:.1f}%</td>'
        f'<td class="n neg">{r["MDD"]*100:.1f}%</td>'
        f'<td class="n"><b>{r["Calmar"]:.2f}</b></td>'
        f'<td class="n">{r["Sharpe"]:.2f}</td>'
        f'<td class="n neg">{r["최악일"]*100:.1f}%</td>'
        f'<td class="n" style="color:{"var(--faint)" if v==1 else "var(--neg)"};'
        f'font-weight:600">{d:+.0f}%</td></tr>')
RTAB = "\n".join(rows)

# ── 티어별 특성 ──────────────────────────────────────────
rows = []
for n in ("이평4일", "중심주가"):
    for tier in (1, 2):
        d = BT[(BT["전략"] == n) & (BT["티어"] == tier)]
        tot = d["거래횟수"].sum()
        wr = (d["승률"] * d["거래횟수"]).sum() / tot
        hold = (d["평균보유일"] * d["거래횟수"]).sum() / tot
        pl = (d["손익비"] * d["거래횟수"]).sum() / tot
        hi = ' class="sum"' if tier == 2 else ""
        rows.append(
            f'<tr{hi}><td>{n}</td><td class="n">{tier}티어</td>'
            f'<td class="n">{tot:,.0f}</td><td class="n">{wr*100:.1f}%</td>'
            f'<td class="n">{hold:.1f}일</td>'
            f'<td class="n" style="color:{"var(--neg)" if pl<0.5 else "var(--pos)"};'
            f'font-weight:600">{pl:.2f}</td></tr>')
BTAB = "\n".join(rows)

# ── 하한선 ───────────────────────────────────────────────
p = FL[FL["전략"] == "이평4일"].pivot_table(
    index="계좌", columns="1티어%", values="2티어체결", aggfunc="first")
cols = list(p.columns)
head = "".join(f"<th>{c:g}%</th>" for c in cols)
rows = []
for cap in p.index:
    cells = []
    for c in cols:
        v = int(p.loc[cap, c])
        ok = v >= 277
        cells.append(f'<td class="n" style="color:{"var(--pos)" if ok else "var(--neg)"};'
                     f'font-weight:{600 if not ok else 400}">{v}</td>')
    mark = ' class="sum"' if cap == 30000 else ""
    rows.append(f'<tr{mark}><td class="n">${cap:,.0f}</td>' + "".join(cells) + "</tr>")
FTAB = f'<thead><tr><th>계좌 규모</th>{head}</tr></thead><tbody>' \
       + "\n".join(rows) + "</tbody>"

r25 = gd[gd["이평4일_1티어%"] == 25].iloc[0]
V = {"__CUR_CAGR__": f"{base['CAGR']*100:.1f}", "__CUR_MDD__": f"{base['MDD']*100:.1f}",
     "__CUR_CAL__": f"{base['Calmar']:.2f}", "__C25_CAGR__": f"{r25['CAGR']*100:.1f}",
     "__C25_MDD__": f"{r25['MDD']*100:.1f}", "__C25_CAL__": f"{r25['Calmar']:.2f}",
     "__C25_D__": f"{(r25['Calmar']-base['Calmar'])/base['Calmar']*100:.0f}"}

head_css = io.open(J("_css.txt"), encoding="utf-8").read().replace(
    "<title>반반 투자 성과 분석</title>", "<title>티어 비율을 바꾸면</title>")
src = io.open(J("report_tier_src.html"), encoding="utf-8").read()
for k, v in [("__HEAD__", head_css), ("__CHART1__", CHART1), ("__CHART2__", CHART2),
             ("__RTAB__", RTAB), ("__BTAB__", BTAB), ("__FTAB__", FTAB), *V.items()]:
    src = src.replace(k, v)
out = J("260902_만능스위치_티어비율_분석.html")
io.open(out, "w", encoding="utf-8").write(src)
print(f"생성: {os.path.basename(out)} ({len(src):,} bytes)")
print(f"  현재 1%: Calmar {base['Calmar']:.2f} · 25%: {r25['Calmar']:.2f} "
      f"({V['__C25_D__']}%)")
