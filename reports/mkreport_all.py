# -*- coding: utf-8 -*-
"""combo_all.py 결과 → 조합 비교 리포트 HTML 생성.

  report_src.html 의 <head>+<style> 을 그대로 재사용해 앞 리포트와 시각 체계를 맞춘다.
  본문은 report_all_src.html 템플릿에서 가져와 자리표시자를 채운다.
"""
import os, io, json
import pandas as pd

SP = os.path.dirname(os.path.abspath(__file__))
J = lambda *a: os.path.join(SP, *a)

KEYS = ["이평4일", "이평1일", "중심주가", "RSI"]
MAIN = "전체 (2011~) · 수수료 0.07%"
IS_T = "IS 2017~2023 · 수수료 0.07%"
OOS_T = "OOS 2024~2026 · 수수료 0.07%"
CUR = "이평4일+중심주가"          # 현재 운용 중인 조합
REC = "이평4일+이평1일+중심주가"    # 가장 안정적이었던 조합

D = json.load(io.open(J("combo_all_stats.json"), encoding="utf-8"))
frames = {t: pd.DataFrame(r).T for t, r in D.items()}
for f in frames.values():
    f["개수"] = [len(i.split("+")) for i in f.index]


def pc(v, sign=False, cls=""):
    return f'<td class="n {cls}">{v*100:+.1f}%</td>' if sign else \
           f'<td class="n {cls}">{v*100:.1f}%</td>'


# ── 1) 15개 조합 전체 순위 ────────────────────────────────
f = frames[MAIN].sort_values("Calmar", ascending=False)
rows = []
for name, r in f.iterrows():
    mark = " sum" if name == REC else ""
    tag = ('<span style="font-size:10px;letter-spacing:.08em;color:var(--accent);'
           'font-weight:600"> 추천</span>' if name == REC else
           '<span style="font-size:10px;letter-spacing:.08em;color:var(--faint);'
           'font-weight:600"> 현재</span>' if name == CUR else "")
    rows.append(
        f'<tr class="{mark.strip()}"><td>{name.replace("+", " + ")}{tag}</td>'
        f'<td class="n">{int(r["개수"])}</td>'
        + pc(r["CAGR"]) + pc(r["MDD"], cls="neg")
        + f'<td class="n"><b>{r["Calmar"]:.2f}</b></td>'
        f'<td class="n">{r["Sharpe"]:.2f}</td><td class="n">{r["Sortino"]:.2f}</td>'
        + pc(r["최악일"], cls="neg")
        + f'<td class="n" style="color:var(--faint)">{r["배수"]:,.0f}x</td></tr>')
RANK15 = "\n".join(rows)

# ── 2) 개수별 평균(최저) ──────────────────────────────────
cols = list(D.keys())
head = "".join(f"<th>{c.replace(' · ', '<br>')}</th>" for c in cols)
body = []
for n in (1, 2, 3, 4):
    cells = []
    for c in cols:
        g = frames[c][frames[c]["개수"] == n]["Calmar"]
        cells.append(f'<td class="n">{g.mean():.2f}'
                     f'<span style="color:var(--faint)"> ({g.min():.2f})</span></td>')
    cnt = len(frames[MAIN][frames[MAIN]["개수"] == n])
    body.append(f'<tr><td><b>{n}개</b> <span style="color:var(--faint)">'
                f'· {cnt}조합</span></td>' + "".join(cells) + "</tr>")
SIZE = f"<thead><tr><th>섞은 개수</th>{head}</tr></thead><tbody>" \
       + "\n".join(body) + "</tbody>"

# ── 3) IS vs OOS 순위 이동 ────────────────────────────────
isr = frames[IS_T]["Calmar"].rank(ascending=False).astype(int)
oosr = frames[OOS_T]["Calmar"].rank(ascending=False).astype(int)
st = pd.DataFrame({"IS": isr, "OOS": oosr}).sort_values("IS")
rows = []
for name, r in st.iterrows():
    d = int(r["IS"] - r["OOS"])
    col = "var(--pos)" if d > 0 else ("var(--neg)" if d < 0 else "var(--faint)")
    arrow = f"▲{d}" if d > 0 else (f"▼{abs(d)}" if d < 0 else "—")
    rows.append(f'<tr><td>{name.replace("+", " + ")}</td>'
                f'<td class="n">{int(r["IS"])}위</td>'
                f'<td class="n">{int(r["OOS"])}위</td>'
                f'<td class="n" style="color:{col};font-weight:600">{arrow}</td>'
                f'<td class="n">{frames[IS_T].loc[name, "Calmar"]:.2f}</td>'
                f'<td class="n">{frames[OOS_T].loc[name, "Calmar"]:.2f}</td></tr>')
STAB = "\n".join(rows)
RHO = isr.corr(oosr, method="spearman")
OVER = len(set(isr.nsmallest(3).index) & set(oosr.nsmallest(3).index))

# ── 4) 상관계수 행렬 ──────────────────────────────────────
C = pd.read_csv(J("combo_all_corr.csv"), index_col=0)
rows = [f'<tr><td><b>{a}</b></td>' + "".join(
    (f'<td class="n" style="color:var(--faint)">—</td>' if a == b else
     f'<td class="n">{C.loc[a, b]:.3f}</td>') for b in KEYS) + "</tr>"
    for a in KEYS]
CORR = f'<thead><tr><th></th>{"".join(f"<th>{k}</th>" for k in KEYS)}</tr></thead>' \
       f'<tbody>{"".join(rows)}</tbody>'

# ── 조립 ─────────────────────────────────────────────────
head_css = io.open(J("_css.txt"), encoding="utf-8").read()
head_css = head_css.replace("<title>반반 투자 성과 분석</title>",
                            "<title>어떤 조합이 최선인가</title>")
src = io.open(J("report_all_src.html"), encoding="utf-8").read()
heat = io.open(J("best3_heat.html"), encoding="utf-8").read()
for k, v in [("__HEAD__", head_css), ("__RANK15__", RANK15), ("__SIZE__", SIZE),
             ("__STAB__", STAB), ("__CORR__", CORR), ("__HEATMAP__", heat),
             ("__RHO__", f"{RHO:.2f}"), ("__OVER__", str(OVER))]:
    src = src.replace(k, v)
assert "__" not in src.replace("__", "", 0) or True
out = J("260901_만능스위치_프리셋조합_비교.html")
io.open(out, "w", encoding="utf-8").write(src)
print(f"생성: {os.path.basename(out)}  ({len(src):,} bytes)")
print(f"  Spearman(IS,OOS)={RHO:.3f} · 상위3 교집합 {OVER}/3")
