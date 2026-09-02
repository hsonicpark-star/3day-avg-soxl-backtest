# -*- coding: utf-8 -*-
"""tier_oneshare.py 결과 → '1% 금액매수 vs 1주 매수' 비교 리포트."""
import os, io
import pandas as pd

from _chart import line_chart

SP = os.path.dirname(os.path.abspath(__file__))
J = lambda *a: os.path.join(SP, *a)
FULL, IS_T, OOS_T = "전체 2011~", "IS 2017~2023", "OOS 2024~2026"
A, B, C = "A 현재 (1% 금액)", "B 1주 고정", "C 1주 + 게이트확대"

R = pd.read_csv(J("oneshare_stats.csv"))
Q = pd.read_csv(J("oneshare_qty.csv"))
RB = pd.read_csv(J("oneshare_robust.csv"))
G = pd.read_csv(J("oneshare_gate.csv"))

# ── 게이트 차트 ──────────────────────────────────────────
CHART = line_chart(
    [("현재 방식 (1티어 금액매수)", list(zip(G["seed_w%"], G["A 금액매수"])), "#B3352C"),
     ("1주 고정", list(zip(G["seed_w%"], G["B 1주고정"])), "var(--accent)")],
    ylab="Calmar (50:50 합산)", xlab="1티어 seed_w",
    xticks=[1, 10, 20, 30, 50, 70, 90], mark_max=False)

# ── 성과 비교 ────────────────────────────────────────────
rows = []
for tgt in ("50:50 합산", "이평4일", "중심주가"):
    for i, v in enumerate((A, B)):
        d = R[(R["대상"] == tgt) & (R["변형"] == v)].set_index("시나리오")
        first = ('<td rowspan="2" style="vertical-align:middle">'
                 f'<b>{tgt}</b></td>' if i == 0 else "")
        mark = ' class="sum"' if v == B else ""
        cells = ""
        for s in (FULL, IS_T, OOS_T):
            r = d.loc[s]
            cells += (f'<td class="n">{r["Calmar"]:.2f}</td>'
                      f'<td class="n" style="color:var(--muted)">{r["CAGR"]*100:.0f}%</td>'
                      f'<td class="n neg">{r["MDD"]*100:.1f}%</td>')
        rows.append(f'<tr{mark}>{first}<td>{v}</td>{cells}</tr>')
PTAB = "\n".join(rows)

# ── 투입 자본 ────────────────────────────────────────────
rows = []
for n in ("이평4일", "중심주가"):
    a = Q[(Q["전략"] == n) & (Q["변형"] == A)].iloc[0]
    b = Q[(Q["전략"] == n) & (Q["변형"] == B)].iloc[0]
    rows.append(
        f'<tr><td rowspan="2" style="vertical-align:middle"><b>{n}</b></td>'
        f'<td>{A}</td><td class="n">{a["체결"]:,.0f}</td>'
        f'<td class="n">{a["중앙수량"]:,.0f}주</td>'
        f'<td class="n">{a["최대수량"]:,.0f}주</td>'
        f'<td class="n">${a["총투입"]:,.0f}</td></tr>'
        f'<tr class="sum"><td>{B}</td><td class="n">{b["체결"]:,.0f}</td>'
        f'<td class="n">1주</td><td class="n">1주</td>'
        f'<td class="n">${b["총투입"]:,.0f}</td></tr>')
QTAB = "\n".join(rows)

# ── 소액 계좌 내성 ───────────────────────────────────────
p = RB.pivot_table(index=["계좌", "전략"], columns="변형",
                   values="2티어체결", aggfunc="first")
NORM = {"이평4일": 277, "중심주가": 345}
rows = []
for (cap, n) in p.index:
    cells = ""
    for v in (A, B, C):
        val = int(p.loc[(cap, n), v])
        ok = val >= NORM[n]
        cells += (f'<td class="n" style="color:{"var(--pos)" if ok else "var(--neg)"};'
                  f'font-weight:{400 if ok else 600}">{val}</td>')
    mark = ' class="sum"' if cap == 30000 else ""
    rows.append(f'<tr{mark}><td class="n">${cap:,.0f}</td><td>{n}</td>{cells}'
                f'<td class="n" style="color:var(--faint)">{NORM[n]}</td></tr>')
RTAB = "\n".join(rows)

g1, g90 = G.iloc[0], G.iloc[-1]
V = {"__A_FULL__": f"{R[(R['대상']=='50:50 합산')&(R['변형']==A)&(R['시나리오']==FULL)]['Calmar'].iloc[0]:.3f}",
     "__B_FULL__": f"{R[(R['대상']=='50:50 합산')&(R['변형']==B)&(R['시나리오']==FULL)]['Calmar'].iloc[0]:.3f}",
     "__A90__": f"{g90['A 금액매수']:.2f}", "__A90D__": f"{g90['변화 A']:.0f}",
     "__CAP_A__": f"{Q[(Q['전략']=='이평4일')&(Q['변형']==A)]['총투입'].iloc[0]:,.0f}",
     "__CAP_B__": f"{Q[(Q['전략']=='이평4일')&(Q['변형']==B)]['총투입'].iloc[0]:,.0f}"}

head = io.open(J("_css.txt"), encoding="utf-8").read().replace(
    "<title>반반 투자 성과 분석</title>", "<title>1주 매수로 바꾸면</title>")
src = io.open(J("report_oneshare_src.html"), encoding="utf-8").read()
for k, v in [("__HEAD__", head), ("__CHART__", CHART), ("__PTAB__", PTAB),
             ("__QTAB__", QTAB), ("__RTAB__", RTAB), *V.items()]:
    src = src.replace(k, v)
out = J("260902_만능스위치_1주매수_비교.html")
io.open(out, "w", encoding="utf-8").write(src)
print(f"생성: {os.path.basename(out)} ({len(src):,} bytes)")
print(f"  A {V['__A_FULL__']} vs B {V['__B_FULL__']} · "
      f"게이트 90%: A {V['__A90__']} ({V['__A90D__']}%) vs B 변화 0%")
