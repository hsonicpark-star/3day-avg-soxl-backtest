# -*- coding: utf-8 -*-
import os, io, pandas as pd, numpy as np

SP = os.path.dirname(os.path.abspath(__file__))
eq = pd.read_csv(SP + r"\combo_equity.csv", index_col=0, parse_dates=True)

def year_rows(s):
    """연도별 수익률/MDD — 기준값은 전년 말 총자산."""
    out = {}
    for y, g in s.groupby(s.index.year):
        i0 = s.index.get_loc(g.index[0])
        base = float(s.iloc[i0 - 1]) if i0 > 0 else float(g.iloc[0])
        out[y] = (float(g.iloc[-1]) / base - 1,
                  float((g / np.maximum.accumulate(g) - 1).min()))
    return out

A, B, C = (year_rows(eq[c]) for c in ("이평4일", "중심주가", "합산"))
rows, wins = [], 0
for y in sorted(C):
    a, b, c = A[y], B[y], C[y]
    worse = min(a[0], b[0])
    if c[0] > worse:
        wins += 1
    def cell(v, mdd=False):
        cls = "neg" if v < 0 else ("" if mdd else "pos")
        return f'<td class="n {cls}">{v*100:+.1f}%</td>' if not mdd else \
               f'<td class="n" style="color:var(--faint)">{v*100:.1f}%</td>'
    rows.append(f'<tr><td class="n">{y}</td>'
                + cell(a[0]) + cell(a[1], True)
                + cell(b[0]) + cell(b[1], True)
                + f'<td class="n" style="background:var(--accent-soft);font-weight:600;'
                  f'color:var(--{"neg" if c[0]<0 else "pos"})">{c[0]*100:+.1f}%</td>'
                + f'<td class="n" style="background:var(--accent-soft);color:var(--muted)">{c[1]*100:.1f}%</td>'
                + '</tr>')
print(f"합산이 못한쪽보다 우위: {wins}/{len(C)}년")

heat = io.open(SP + r"\combo_heat.html", encoding="utf-8").read()
src = io.open(SP + r"\report_src.html", encoding="utf-8").read()
src = src.replace("__YEARLY__", "\n".join(rows)).replace("__HEATMAP__", heat)
io.open(SP + r"\260901_만능스위치_반반투자_성과분석.html", "w", encoding="utf-8").write(src)
print("report.html 생성:", len(src), "bytes")
