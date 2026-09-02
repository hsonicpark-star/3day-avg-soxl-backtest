# -*- coding: utf-8 -*-
"""combo_weights.py + weights_oos_test.csv → 비대칭 배분 리포트 HTML 생성."""
import os, io, json
import pandas as pd

SP = os.path.dirname(os.path.abspath(__file__))
J = lambda *a: os.path.join(SP, *a)

KEYS = ["이평4일", "이평1일", "중심주가", "RSI"]
FULL, IS_T, OOS_T = "전체 2011~", "IS 2017~2023", "OOS 2024~2026"
CUR = "이평4일+중심주가"
LINE = ["var(--accent)", "#C0662B", "#3E8A6E", "#8A5BA6", "#B3352C", "#4C7FA8"]

SW = pd.read_csv(J("weights_sweep2.csv"))
BEST = pd.read_csv(J("weights_best.csv"))
OOST = pd.read_csv(J("weights_oos_test.csv"))


# ── SVG 라인차트 ─────────────────────────────────────────
def chart(series, ylab="Calmar", W=760, H=330, pad=(52, 16, 40, 56)):
    """series = [(label, [(x,y)...], color)] · x 는 0~100 비중"""
    t, r, b, l = pad
    iw, ih = W - l - r, H - t - b
    ys = [y for _, pts, _ in series for _, y in pts]
    lo, hi = min(ys), max(ys)
    m = (hi - lo) * 0.12 or 0.1
    lo, hi = lo - m, hi + m
    X = lambda v: l + iw * v / 100.0
    Y = lambda v: t + ih * (1 - (v - lo) / (hi - lo))

    g = [f'<svg viewBox="0 0 {W} {H}" width="100%" role="img" '
         f'style="display:block;overflow:visible">']
    # y 그리드
    for i in range(5):
        v = lo + (hi - lo) * i / 4
        y = Y(v)
        g.append(f'<line x1="{l}" y1="{y:.1f}" x2="{l+iw}" y2="{y:.1f}" '
                 f'stroke="var(--rule)" stroke-width="1"/>')
        g.append(f'<text x="{l-10}" y="{y+4:.1f}" text-anchor="end" font-size="11" '
                 f'fill="var(--faint)" font-family="IBM Plex Mono,monospace">{v:.1f}</text>')
    # x 눈금
    for v in range(0, 101, 20):
        g.append(f'<text x="{X(v):.1f}" y="{t+ih+22}" text-anchor="middle" font-size="11" '
                 f'fill="var(--faint)" font-family="IBM Plex Mono,monospace">{v}%</text>')
    g.append(f'<text x="{l+iw/2:.0f}" y="{H-4}" text-anchor="middle" font-size="11" '
             f'fill="var(--muted)">첫 번째 프리셋 비중</text>')
    g.append(f'<text x="14" y="{t-6}" font-size="11" fill="var(--muted)">{ylab}</text>')
    # 라인 + 최고점
    for lab, pts, col in series:
        d = " ".join(("M" if i == 0 else "L") + f"{X(x):.1f},{Y(y):.1f}"
                     for i, (x, y) in enumerate(pts))
        g.append(f'<path d="{d}" fill="none" stroke="{col}" stroke-width="2" '
                 f'stroke-linejoin="round" stroke-linecap="round"/>')
        bx, by = max(pts, key=lambda p: p[1])
        g.append(f'<circle cx="{X(bx):.1f}" cy="{Y(by):.1f}" r="4" fill="{col}" '
                 f'stroke="var(--surface)" stroke-width="1.5"/>')
    g.append("</svg>")
    key = "".join(
        f'<span style="display:inline-flex;align-items:center;gap:6px;font-size:12px;'
        f'color:var(--muted)"><i style="width:14px;height:2px;background:{c};'
        f'display:inline-block"></i>{lab}</span>'
        for lab, _, c in series)
    return ("".join(g) + f'<div style="display:flex;flex-wrap:wrap;gap:8px 18px;'
            f'margin-top:14px;justify-content:center">{key}</div>')


def pts(pair, scen):
    d = SW[(SW["쌍"] == pair) & (SW["시나리오"] == scen)].sort_values("A비중")
    return list(zip(d["A비중"], d["Calmar"]))


pairs = list(dict.fromkeys(SW["쌍"]))
CHART1 = chart([(p.replace("+", " → "), pts(p, FULL), LINE[i])
                for i, p in enumerate(pairs)])
CHART2 = chart([(s, pts(CUR, s), c) for s, c in
                zip([FULL, IS_T, OOS_T], ["var(--accent)", "#C0662B", "#3E8A6E"])])


# ── 표 ───────────────────────────────────────────────────
def opt_row(r):
    d = r["균등대비"]
    col = "var(--pos)" if d > 0 else "var(--neg)"
    bad = ' style="background:var(--accent-soft)"' if d < -20 else ""
    return (f'<tr{bad}><td>{r["조합"].replace("+", " + ")}</td>'
            f'<td class="n">{r["개수"]}</td>'
            f'<td class="n" style="color:var(--muted)">{r["IS최적비중"]}</td>'
            f'<td class="n">{r["OOS(IS최적)"]:.2f}</td>'
            f'<td class="n"><b>{r["OOS(균등)"]:.2f}</b></td>'
            f'<td class="n" style="color:var(--faint)">{r["OOS(사후최적)"]:.2f}</td>'
            f'<td class="n" style="color:{col};font-weight:600">{d:+.1f}%</td></tr>')


OOSTAB = "\n".join(opt_row(r) for _, r in OOST.iterrows())

piv_w = BEST.pivot_table(index=["개수", "조합"], columns="시나리오",
                         values="최적비중", aggfunc="first")
piv_l = BEST.pivot_table(index=["개수", "조합"], columns="시나리오",
                         values="손실%", aggfunc="first")
piv_p = BEST.pivot_table(index=["개수", "조합"], columns="시나리오",
                         values="고원비율", aggfunc="first")
rows = []
for (n, c) in piv_w.index:
    mark = ' class="sum"' if c == CUR else ""
    rows.append(
        f'<tr{mark}><td>{c.replace("+", " + ")}</td><td class="n">{n}</td>'
        + "".join(f'<td class="n" style="color:var(--muted)">{piv_w.loc[(n, c), s]}</td>'
                  for s in (FULL, IS_T, OOS_T))
        + f'<td class="n">{piv_l.loc[(n, c), FULL]:.1f}%</td>'
        + f'<td class="n" style="color:var(--faint)">{piv_p.loc[(n, c), FULL]:.0f}%</td></tr>')
WTAB = "\n".join(rows)

WINS = int((OOST["균등대비"] > 0).sum())
MEAN = OOST["균등대비"].mean()
CAP_O = (OOST["OOS(IS최적)"] / OOST["OOS(사후최적)"]).mean() * 100
CAP_E = (OOST["OOS(균등)"] / OOST["OOS(사후최적)"]).mean() * 100
CUR_L = piv_l.loc[(2, CUR), FULL]

head = io.open(J("_css.txt"), encoding="utf-8").read().replace(
    "<title>반반 투자 성과 분석</title>", "<title>비중을 바꾸면 나아지나</title>")
src = io.open(J("report_weights_src.html"), encoding="utf-8").read()
for k, v in [("__HEAD__", head), ("__CHART1__", CHART1), ("__CHART2__", CHART2),
             ("__OOSTAB__", OOSTAB), ("__WTAB__", WTAB),
             ("__WINS__", str(WINS)), ("__N__", str(len(OOST))),
             ("__MEAN__", f"{MEAN:+.1f}"), ("__CAPO__", f"{CAP_O:.0f}"),
             ("__CAPE__", f"{CAP_E:.0f}"), ("__CURL__", f"{CUR_L:.1f}")]:
    src = src.replace(k, v)
out = J("260901_만능스위치_비대칭배분_분석.html")
io.open(out, "w", encoding="utf-8").write(src)
print(f"생성: {os.path.basename(out)} ({len(src):,} bytes)")
print(f"  균등 승 {len(OOST)-WINS}/{len(OOST)} · 평균 {MEAN:+.1f}% · "
      f"회수율 훈련최적 {CAP_O:.0f}% vs 균등 {CAP_E:.0f}%")
