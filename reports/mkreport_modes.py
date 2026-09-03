# -*- coding: utf-8 -*-
"""modes_data.py 결과 → 모드 판단 기준 해설 리포트 HTML 생성."""
import os, io, json
from datetime import date

SP = os.path.dirname(os.path.abspath(__file__))
J = lambda *a: os.path.join(SP, *a)

D = json.load(io.open(J("modes_data.json"), encoding="utf-8"))
C, M, R = D["center"], D["ma"], D["rsi"]

# ── 세 기준 비교표 ────────────────────────────────────────
def pc(v):
    return f"{v*100:.1f}%"


rows = [
    ("기준선", "2016년 고정 추세선", "QQQ 120일 이동평균", "없음 (오실레이터)"),
    ("재는 것", "장기 추세 대비 이격도", "최근 넉 달 평균 대비 이격도", "14주 상승/하락 힘의 비율"),
    ("바닥 경계", f"&lt; {C['low']*100:g}%", f"&lt; {M['low']*100:g}%", f"&lt; {R['low']:g}"),
    ("천장 경계", f"&gt; {C['high']*100:g}%", f"&gt; {M['high']*100:g}%", f"&gt; {R['high']:g}"),
    ("드리프트 (연)", f"<b>+{C['slope']*100:.2f}%p</b>",
     f"+{M['slope']*100:.2f}%p", f"{R['slope']:+.2f}pt"),
    ("바닥 비율", pc(C["ratio"]["바닥"]), pc(M["ratio"]["바닥"]),
     f"<b>{pc(R['ratio']['바닥'])}</b>"),
    ("중간 비율", pc(C["ratio"]["중간"]), pc(M["ratio"]["중간"]), pc(R["ratio"]["중간"])),
    ("천장 비율", pc(C["ratio"]["천장"]), pc(M["ratio"]["천장"]), pc(R["ratio"]["천장"])),
    ("모드 지속", f"{C['avg_run']:.1f}주", f"<b>{M['avg_run']:.1f}주</b>",
     f"{R['avg_run']:.1f}주"),
    ("현재값", f"{C['now']*100:+.1f}% · {C['now_mode']}",
     f"{M['now']*100:+.1f}% · {M['now_mode']}",
     f"{R['now']:.1f} · {R['now_mode']}"),
]
CMP = "\n".join(
    f'<tr><td style="color:var(--muted)">{a}</td>'
    f'<td class="n">{b}</td><td class="n">{c}</td><td class="n">{d}</td></tr>'
    for a, b, c, d in rows)

# ── 중심주가 연도별 ───────────────────────────────────────
C_Y = "\n".join(
    f'<tr><td class="n">{r["연도"]}</td>'
    f'<td class="n {"neg" if r["최저"] < 0 else ""}">{r["최저"]*100:+.1f}%</td>'
    f'<td class="n">{r["평균"]*100:+.1f}%</td>'
    f'<td class="n" style="color:var(--faint)">{r["최고"]*100:+.1f}%</td>'
    f'<td class="n" style="color:{"var(--pos)" if r["바닥도달"] else "var(--faint)"};'
    f'font-weight:{600 if r["바닥도달"] else 400}">'
    f'{"도달" if r["바닥도달"] else "—"}</td></tr>'
    for r in C["yearly_min"])

# ── 상관행렬 ──────────────────────────────────────────────
K = list(D["corr"].keys())
head = "".join(f"<th>{k}</th>" for k in K)
body = "".join(
    f'<tr><td><b>{a}</b></td>' + "".join(
        ('<td class="n" style="color:var(--faint)">—</td>' if a == b else
         f'<td class="n">{D["corr"][a][b]:.3f}</td>') for b in K) + "</tr>"
    for a in K)
CORR = f'<thead><tr><th></th>{head}</tr></thead><tbody>{body}</tbody>'

W = R["vs_wilder"]
V = {
    "__PERIOD__": f"{C['start']} ~ {C['end']}",
    "__TODAY__": str(date.today()),
    "__CMP__": CMP, "__C_YEARLY__": C_Y, "__CORR__": CORR,
    "__C_BASE__": f"{C['base']:.2f}", "__C_RATE__": "1.32",
    "__C_RATEF__": f"{C['rate']:g}",
    "__C_ACAGR__": f"{C['assumed_cagr']*100:.1f}",
    "__C_REAL__": f"{C['qqq_cagr_2016']*100:.2f}",
    "__C_SLOPE__": f"{C['slope']*100:.2f}",
    "__C_LOW__": f"{C['low']*100:g}", "__C_HIGH__": f"{C['high']*100:g}",
    "__C_RUN__": f"{C['avg_run']:.1f}",
    "__M_SLOPE__": f"{M['slope']*100:.2f}",
    "__M_LOW__": f"{M['low']*100:g}", "__M_HIGH__": f"{M['high']*100:g}",
    "__M_RUN__": f"{M['avg_run']:.1f}",
    "__R_LOW__": f"{R['low']:g}", "__R_HIGH__": f"{R['high']:g}",
    "__R_RUN__": f"{R['avg_run']:.1f}",
    "__R_FLOOR__": f"{R['ratio']['바닥']*100:.0f}",
    "__R_CORR__": f"{W['corr']:.2f}", "__R_MAXD__": f"{W['max_diff']:.1f}",
    "__R_DIS__": f"{W['disagree']:,}", "__R_TOT__": f"{W['total']:,}",
    "__R_DISP__": f"{W['disagree']/W['total']*100:.0f}",
    "__R_NOWS__": f"{W['now_sheet']:.1f}", "__R_NOWW__": f"{W['now_wilder']:.1f}",
    "__DATA__": json.dumps(
        {k: {"series": D[k]["series"], "low": D[k]["low"], "high": D[k]["high"]}
         for k in ("center", "ma", "rsi")}, ensure_ascii=False, separators=(",", ":")),
}

head_css = io.open(J("_css.txt"), encoding="utf-8").read().replace(
    "<title>반반 투자 성과 분석</title>", "<title>바닥·중간·천장은 어떻게 정해지나</title>")
src = io.open(J("report_modes_src.html"), encoding="utf-8").read()
src = src.replace("__HEAD__", head_css, 1)
for k, v in V.items():
    src = src.replace(k, v)

out = J("260903_만능스위치_모드판단기준_해설.html")
io.open(out, "w", encoding="utf-8").write(src)
left = [t for t in ("__PERIOD__", "__CMP__", "__DATA__", "__C_", "__M_", "__R_")
        if t in src]
print(f"생성: {os.path.basename(out)} ({len(src):,} bytes)")
print("미치환 자리표시자:", left or "없음")
