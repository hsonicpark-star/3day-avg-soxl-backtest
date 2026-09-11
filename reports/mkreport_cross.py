# -*- coding: utf-8 -*-
"""cross_2016.json + dual_opt_2016.json → 전략 계열 교차 조합 리포트."""
import os, io, json
from datetime import date

SP = os.path.dirname(os.path.abspath(__file__))
J = lambda *a: os.path.join(SP, *a)

C = json.load(io.open(J("cross_2016.json"), encoding="utf-8"))
D = json.load(io.open(J("dual_opt_2016.json"), encoding="utf-8"))
IND, PAIRS, CORR = C["individual"], C["pairs"], C["corr"]
CUR = "만능-이평4일+만능-중심주가"
ALT = "듀얼스나이퍼+만능-이평4일"

# ── 개별 ──────────────────────────────────────────────────
order = sorted(IND, key=lambda k: -IND[k]["MAR"])
rows = []
for k in order:
    v = IND[k]
    hi = ' class="sum"' if "만능" in k or k == "듀얼스나이퍼" else ""
    rows.append(
        f'<tr{hi}><td><b>{k}</b></td>'
        f'<td class="n">{v["CAGR"]*100:.1f}%</td>'
        f'<td class="n neg">{v["MDD"]*100:.1f}%</td>'
        f'<td class="n"><b>{v["MAR"]:.2f}</b></td>'
        f'<td class="n">{v["Sharpe"]:.2f}</td>'
        f'<td class="n neg">{v["최악일"]*100:.1f}%</td></tr>')
INDH = "\n".join(rows)

# ── 상관행렬 ──────────────────────────────────────────────
K = list(CORR.keys())
head = "".join(f'<th>{k.replace("만능-", "만능<br>")}</th>' for k in K)
body = []
for a in K:
    cells = []
    for b in K:
        if a == b:
            cells.append('<td class="n" style="color:var(--faint)">—</td>')
        else:
            v = CORR[a][b]
            lo = v < 0.60
            cells.append(f'<td class="n"'
                         f'{" style=\"color:var(--accent);font-weight:600\"" if lo else ""}'
                         f'>{v:.3f}</td>')
    body.append(f'<tr><td><b>{a}</b></td>' + "".join(cells) + "</tr>")
CORRH = (f'<thead><tr><th></th>{head}</tr></thead><tbody>'
         + "".join(body) + "</tbody>")

# ── 조합 ──────────────────────────────────────────────────
rows = []
for r in PAIRS:
    nm = r["조합"]
    mark = ' class="sum"' if nm in (CUR, ALT) else ""
    tag = ""
    if nm == CUR:
        tag = ('<span style="font-size:10px;letter-spacing:.08em;color:var(--accent);'
               'font-weight:600"> 현재</span>')
    elif nm == ALT:
        tag = ('<span style="font-size:10px;letter-spacing:.08em;color:var(--pos);'
               'font-weight:600"> 대안</span>')
    rows.append(
        f'<tr{mark}><td>{nm.replace("+", " + ")}{tag}</td>'
        f'<td class="n">{r["CAGR"]*100:.1f}%</td>'
        f'<td class="n neg">{r["MDD"]*100:.1f}%</td>'
        f'<td class="n"><b>{r["MAR"]:.2f}</b></td>'
        f'<td class="n">{r["Sharpe"]:.2f}</td>'
        f'<td class="n neg">{r["최악일"]*100:.1f}%</td>'
        f'<td class="n" style="color:var(--faint)">{r["상관"]:.3f}</td></tr>')
PAIRSH = "\n".join(rows)

# ── 듀얼 최적화 ───────────────────────────────────────────
b = D["base"]
rows = [f'<tr class="sum"><td><b>로케트셋 (현재)</b></td>'
        f'<td class="n"><b>{b["IS"]:.2f}</b></td>'
        f'<td class="n"><b>{b["OOS"]:.2f}</b></td>'
        f'<td class="n"><b>{b["FULL"]:.2f}</b></td></tr>']
for i, t in enumerate(D["top"], 1):
    rows.append(f'<tr><td style="color:var(--muted)">탐색 {i}위 후보</td>'
                f'<td class="n">{t["IS"]:.2f}</td>'
                f'<td class="n">{t["OOS"]:.2f}</td>'
                f'<td class="n">{t["FULL"]:.2f}</td></tr>')
DUALH = "\n".join(rows)

head_css = io.open(J("_css.txt"), encoding="utf-8").read().replace(
    "<title>반반 투자 성과 분석</title>",
    "<title>만능 스위치 밖에서 짝을 찾는다면</title>")
src = io.open(J("report_cross_src.html"), encoding="utf-8").read()
for k, v in [("__HEAD__", head_css), ("__IND__", INDH), ("__CORR__", CORRH),
             ("__PAIRS__", PAIRSH), ("__DUAL__", DUALH),
             ("__PERIOD__", "2016-01-06 ~ 2026-08-31"),
             ("__TODAY__", str(date.today()))]:
    src = src.replace(k, v)
out = J("260911_전략교차조합_분석.html")
io.open(out, "w", encoding="utf-8").write(src)
import re
left = re.findall(r"__[A-Z0-9_]+__", src)
print(f"생성: {os.path.basename(out)} ({len(src):,} bytes) · 미치환 {left or '없음'}")
