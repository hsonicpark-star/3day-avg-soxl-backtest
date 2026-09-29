# -*- coding: utf-8 -*-
"""tq_opt/final.json (1차) + tq_opt/run2_full_is/final.json (2차) → TQ 모멘텀 5T 최적화 리포트."""
import os, io, json
from datetime import date

SP = os.path.dirname(os.path.abspath(__file__))
J = lambda *a: os.path.join(SP, *a)

R1 = json.load(io.open(J("tq_opt", "final.json"), encoding="utf-8"))
R2 = json.load(io.open(J("tq_opt", "run2_full_is", "final.json"), encoding="utf-8"))
PRE = json.load(io.open(J("tq_opt", "preset_opt26.json"), encoding="utf-8"))
BASE = json.load(io.open(J("tq_opt", "base_params.json"), encoding="utf-8"))
LV = ("바닥", "중간", "천장")


def c(x, d=2):
    return "-" if x is None else f"{x:.{d}f}"


def pct(x, d=1):
    return "-" if x is None else f"{x*100:.{d}f}%"


def cls(v, ref):
    if v is None or ref is None:
        return ""
    return ' class="n pos"' if v > ref + 1e-9 else (' class="n neg"' if v < ref - 1e-9 else ' class="n"')


# ── 1차: 학습 2015~22 / 검증 2023~ ─────────────────────────
b1 = R1["baseline"]
C1 = R1["candidates"]
by_is = sorted(C1, key=lambda r: -r["IS"]["Calmar"])
pick1 = [("현재 프리셋", b1, True), ("최적화 1위 (#%d)" % C1[0]["idx"], C1[0], False),
         ("학습 구간 최고 (#%d)" % by_is[0]["idx"], by_is[0], False)]
rows = []
for nm, r, cur in pick1:
    hi = ' class="sum"' if cur else ""
    rows.append(
        f'<tr{hi}><td><b>{nm}</b></td>'
        f'<td{cls(r["IS"]["Calmar"], b1["IS"]["Calmar"]) if not cur else " class=\"n\""}>{c(r["IS"]["Calmar"])}</td>'
        f'<td{cls(r["OOS"]["Calmar"], b1["OOS"]["Calmar"]) if not cur else " class=\"n\""}><b>{c(r["OOS"]["Calmar"])}</b></td>'
        f'<td class="n neg">{pct(r["OOS"]["MDD"])}</td>'
        f'<td{cls(r["PRE"]["Calmar"], b1["PRE"]["Calmar"]) if not cur else " class=\"n\""}>{c(r["PRE"]["Calmar"])}</td>'
        f'<td class="n">{c(r["FULL"]["Calmar"])}</td>'
        f'<td class="n neg">{pct(r["FULL"]["MDD"])}</td></tr>')
T1 = "\n".join(rows)
n1_oos = sum(1 for r in C1 if (r["OOS"]["Calmar"] or 0) > b1["OOS"]["Calmar"])
n1_pre = sum(1 for r in C1 if (r["PRE"]["Calmar"] or 0) > b1["PRE"]["Calmar"])
med1 = sorted(r["OOS"]["Calmar"] for r in C1)[len(C1) // 2]

# ── 2차: 학습 2015~26 / 검증 2011~14 ─────────────────────────
b2 = R2["baseline"]
C2 = R2["candidates"]
groups = {}
for r in C2:
    k = (round(r["IS"]["Calmar"], 2), round(r["OOS"]["Calmar"], 2))
    groups.setdefault(k, []).append(r)
glist = sorted(groups.items(), key=lambda kv: -kv[0][1])
rows, rank = [], 1
placed = False
for (is_, oos), rs in glist:
    if not placed and oos < b2["OOS"]["Calmar"] - 1e-9:
        rows.append(("현재 프리셋", b2, True, 1, rank))
        rank += 1
        placed = True
    rows.append((", ".join(f"#{r['idx']}" for r in rs[:4]) + (" 외" if len(rs) > 4 else ""),
                 rs[0], False, len(rs), rank))
    rank += len(rs)
if not placed:
    rows.append(("현재 프리셋", b2, True, 1, rank))
html = []
for nm, r, cur, n, rk in rows:
    hi = ' class="sum"' if cur else ""
    tag = "" if cur else f' <span style="color:var(--faint);font-size:11px">({n}개)</span>'
    ref = lambda k: b2[k]["Calmar"]
    html.append(
        f'<tr{hi}><td class="n">{rk}</td><td><b>{nm}</b>{tag}</td>'
        f'<td{cls(r["OOS"]["Calmar"], ref("OOS")) if not cur else " class=\"n\""}><b>{c(r["OOS"]["Calmar"])}</b></td>'
        f'<td class="n">{c(r["IS"]["Calmar"])}</td>'
        f'<td class="n">{pct(r["IS"]["CAGR"])}</td><td class="n neg">{pct(r["IS"]["MDD"])}</td>'
        f'<td class="n">{c(r["15_18"]["Calmar"])}</td><td class="n">{c(r["19_22"]["Calmar"])}</td>'
        f'<td class="n">{c(r["23_26"]["Calmar"])}</td>'
        f'<td class="n">{c(r["robust_med"])}</td><td class="n">{c(r["FULL_fee7"]["Calmar"])}</td></tr>')
T2 = "\n".join(html)
n2_oos = sum(1 for r in C2 if (r["OOS"]["Calmar"] or 0) > b2["OOS"]["Calmar"])
A = [r for r in C2 if r["idx"] == 26][0]

# ── 바뀐 파라미터 (시드 비중) ─────────────────────────────
w_rows = []
for lv in LV:
    cur = [t["seed_w"] for t in BASE["levels"][lv]["tiers"]]
    new = [t[0] for t in PRE["levels"][lv][4]]
    fmt = lambda ws: " · ".join(f"{w*100:.0f}" if w >= 0.01 else "0" for w in ws)
    w_rows.append(f'<tr><td><b>{lv}</b></td><td class="n">{fmt(cur)}</td>'
                  f'<td class="n"><b>{fmt(new)}</b></td></tr>')
TW = "\n".join(w_rows)
n_changed = 43

head_css = io.open(J("_css.txt"), encoding="utf-8").read().replace(
    "<title>반반 투자 성과 분석</title>", "<title>TQ 모멘텀 최적화 검증</title>")

body = f"""
<div class="wrap">

<header>
  <div class="eyebrow">TQQQ · 만능 스위치 모멘텀 5T · 정량매수</div>
  <h1>Calmar 를 더 올릴 수 있을까</h1>
  <p class="dek">모드 기준 3개와 바닥·중간·천장 × 5티어의 시드비중·매수목표·매도목표·손절일수,
  체크박스까지 <b>약 70개 파라미터를 전부 풀어서</b> 두 번 최적화했습니다.
  결론부터: <b>과거 한 구간에서는 두 배까지 오르지만, 처음 보는 구간에서 유지되는 값은 없었습니다.</b></p>
  <div class="setup">
    <span>대상 <b>TQ 모멘텀 5T (정량)</b></span>
    <span>탐색 <b>1차 약 3.9만 · 2차 약 3.1만 회</b></span>
    <span>고정 <b>TQQQ · $50,000 · 복리 100% · 5분할</b></span>
    <span>기준일 <b>2026-09-28</b></span>
  </div>
</header>

<section style="margin-top:0">
  <div class="verdict">
    <div class="line">실운용은 지금 프리셋 그대로. 2차 1위는 '실험 프리셋'으로 성과만 추적합니다.</div>
    <p>학습 기간 Calmar 는 3.59 → 7.0 (1차), 3.45 → 4.77 (2차) 까지 올랐습니다.
    하지만 1차에서 <b>2023년 이후를 처음 보여주자 후보 {len(C1)}개 모두 지금 프리셋보다 나빴고</b>
    (검증 Calmar 중앙값 {c(med1)} vs 현재 {c(b1['OOS']['Calmar'])}),
    2차의 개선도 미사용 구간(2011~14)에서는 {c(A['OOS']['Calmar'])} vs {c(b2['OOS']['Calmar'])} 로 작았습니다.</p>
  </div>
</section>

<section>
  <div class="kicker">방법</div>
  <h2>과최적화를 걸러내는 장치</h2>
  <div class="prose">
    <p>파라미터가 70개면 과거를 '외우는' 값이 1등으로 나오기 쉽습니다. 그래서 세 단계를 거쳤습니다.</p>
    <p><b>① TPE 탐색</b>(Optuna) — 기준선 주변을 넓게 훑는다.
    <b>② 국소 탐색</b> — 기준선에서 출발해 값을 몇 개씩 조금씩 바꿔 가며 개선한다.
    <b>③ 검증</b> — 상위 35개를 탐색에 쓰지 않은 기간, 파라미터를 ±5%·±10% 흔든 이웃 20개,
    수수료 0.07% 로 다시 평가한다.</p>
    <p>①은 두 번 모두 <b>기준선을 한 번도 넘지 못했습니다.</b> 원본 시트 작성자가 이미 잘 맞춘 값이라는 뜻입니다.
    개선은 전부 ②에서 나왔습니다.</p>
  </div>
</section>

<section>
  <div class="kicker">1차 · 학습 2015~2022 / 검증 2023~2026</div>
  <h2>처음 보는 3년에서 무너졌다</h2>
  <div class="scroll">
    <table>
      <thead><tr><th>후보</th><th>학습 Calmar</th><th>검증 Calmar</th><th>검증 MDD</th>
        <th>2011~14</th><th>전체 Calmar</th><th>전체 MDD</th></tr></thead>
      <tbody>{T1}</tbody>
    </table>
  </div>
  <div class="prose" style="margin-top:22px">
    <p>검증 구간에서 현재를 넘은 후보 <b>{n1_oos} / {len(C1)}</b>, 2011~14 에서 넘은 후보 <b>{n1_pre} / {len(C1)}</b>.
    학습 구간의 하락장만 피하도록 맞춰진 값이라, 2023년 이후 새 하락장에서
    <b>MDD 가 −28% → −56% 로 두 배</b>가 됐습니다.</p>
    <p>이 결과가 이번 리포트에서 가장 중요합니다. <b>이 탐색 방식은 '앞으로'를 맞히지 못한다</b>는 것을
    직접 보여주는 실험이기 때문입니다.</p>
  </div>
</section>

<section>
  <div class="kicker">2차 · 학습 2015~2026 / 검증 2011~2014</div>
  <h2>순위 — 미사용 구간(2011~14) Calmar 순</h2>
  <div class="scroll">
    <table>
      <thead><tr><th>#</th><th>후보</th><th>검증 2011~14</th><th>학습 2015~26</th><th>CAGR</th><th>MDD</th>
        <th>15~18</th><th>19~22</th><th>23~26</th><th>흔들기 ±5%</th><th>수수료 0.07%</th></tr></thead>
      <tbody>{T2}</tbody>
    </table>
  </div>
  <div class="prose" style="margin-top:22px">
    <p>2011~14 에서 현재를 넘은 후보는 <b>{n2_oos} / {len(C2)}</b> 이고 차이는
    {c(A['OOS']['Calmar'])} vs {c(b2['OOS']['Calmar'])} 입니다.
    학습 구간 최고 묶음은 오히려 현재보다 낮았습니다.
    세 소구간(15~18 · 19~22 · 23~26)이 모두 좋아진 것은 <b>그 기간에 맞춰 찾은 값이라 당연한 결과</b>입니다.</p>
  </div>
</section>

<section>
  <div class="kicker">2차 1위 — 무엇이 바뀌었나</div>
  <h2>시드를 한두 티어에 몰았다</h2>
  <div class="scroll">
    <table>
      <thead><tr><th>구간</th><th>현재 시드 비중 % (T1~T5)</th><th>실험 프리셋 % (T1~T5)</th></tr></thead>
      <tbody>{TW}</tbody>
    </table>
  </div>
  <div class="prose" style="margin-top:22px">
    <p>70개 중 <b>{n_changed}개</b>가 바뀌었습니다. 쓰지 않는 티어는 비중 0 에 가까운 '스위치'로 두고,
    바닥은 5티어에 <b>99.5%</b>, 중간은 1·5티어, 천장은 2·4티어에 몰았습니다 —
    SOXL 2분할의 1%/99% 구조와 닮았습니다. 그 밖에 바닥 1티어 매수목표 +10% → +20%,
    중간 5티어 +7% → +12.75%, 바닥·천장 'MOC날 매수' 끔. 모드 기준(ROC80 · 1.1% · 9%)은 그대로입니다.</p>
  </div>
</section>

<section>
  <div class="kicker">흔들기</div>
  <h2>지금 프리셋도 뾰족한 봉우리 위에 있다</h2>
  <div class="prose">
    <p>파라미터 전부를 동시에 ±5% 흔든 이웃 20개의 학습 Calmar 중앙값은
    현재 프리셋 <b>{c(b1['robust_med'])}</b> (원래 {c(b1['IS']['Calmar'])}), 실험 프리셋 <b>{c(A['robust_med'])}</b>
    (원래 {c(A['IS']['Calmar'])}). 둘 다 조금만 어긋나도 성과가 크게 떨어집니다.
    실운용에서 값을 손으로 옮기다 틀리지 않도록 <b>프리셋 그대로 쓰는 것이 중요</b>합니다.</p>
    <p>또 하나 — 실험 프리셋의 시드 비중을 <b>소수 여섯째 자리에서 반올림만 해도</b>
    2011~14 Calmar 가 1.24 → 1.08 로 바뀌었습니다 (2015~26 은 4.77 그대로).
    당시 TQQQ 가 $0.2~1 대라 1센트 반올림이 수량을 좌우하기 때문입니다.
    <b>2011~14 검증은 참고 수준</b>으로 보세요.</p>
  </div>
</section>

<section>
  <div class="kicker">결정</div>
  <h2>앞으로가 진짜 검증이다</h2>
  <div class="prose">
    <p>앱에 <b>🧪 TQ 5T 최적화 (실험)</b> 프리셋을 추가했습니다. 실운용 계좌는 기존
    <b>🚀 TQ 모멘텀 5T</b> 로 두고, 실험 프리셋은 백테스트·포트폴리오 합산에서 나란히 비교하며
    분기마다 2026-09-28 이후 성과를 확인합니다. 실험 프리셋이 몇 분기 연속 앞서고
    MDD 도 비슷하게 유지될 때 교체를 검토합니다.</p>
  </div>
  <div class="notes" style="margin-top:22px">
    <h3>읽을 때 주의</h3>
    <ul>
      <li><b>원본 프리셋의 성적도 부풀려져 있을 수 있습니다.</b> 원본 시트 작성자가 2015~2026 을 보고 맞췄을 가능성이 높아
      1차의 '검증 {c(b1['OOS']['Calmar'])}' 은 작성자 입장에서는 학습 구간입니다. 작성자도 안 봤을 2011~14 에서는 1.08 입니다.</li>
      <li>수수료 0 · SEC FEE 기본값, 정량매수 기준. 수수료 0.07% 에서도 순위는 같았습니다.</li>
      <li>분할수(5)·복리율·추가주문 설정과 모드 종목(QQQ)은 고정했습니다.</li>
    </ul>
  </div>
</section>

<footer>
  만능 스위치 엔진 (원본 시트 1:1) · 계산 기준일 {date.today()} ·
  재현: <code>cd tq_opt &amp;&amp; python tq_optimize.py</code> (2차는 IS_S/IS_E/OOS_S/OOS_E/OUTDIR 환경변수) →
  <code>python mkreport_tqopt.py</code>
</footer>

</div>
"""

out = J("260929_TQ모멘텀_최적화_분석.html")
io.open(out, "w", encoding="utf-8").write(head_css + body)
print(f"생성: {os.path.basename(out)} ({len(head_css + body):,} bytes)")
