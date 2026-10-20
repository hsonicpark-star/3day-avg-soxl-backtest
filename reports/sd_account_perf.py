# -*- coding: utf-8 -*-
"""표준편차매매 실계좌(원장) 성과 분석 — DD/MDD + 6/2 이후 실적.
입력: _sd_ledger_snapshot.csv (sd_SOXL_매매기록 스냅샷)
출력: YYMMDD_표준편차_실계좌_성과.html + 콘솔 요약
"""
import os, sys, io, base64, json, math
from datetime import datetime
import numpy as np, pandas as pd
SP = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(SP); sys.path.insert(0, ROOT)
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
plt.rcParams["font.family"] = "Malgun Gothic"; plt.rcParams["axes.unicode_minus"] = False
from stdev_engine import run_backtest_stdev

L = pd.read_csv(os.path.join(SP, "_sd_ledger_snapshot.csv"), encoding="utf-8-sig")
num = lambda s: pd.to_numeric(s, errors="coerce")
L["종가n"] = num(L["종가"]); L["총자산n"] = num(L["총자산"]); L["보유n"] = num(L["보유량"]).fillna(0)
L["매도n"] = num(L["매도량"]).fillna(0); L["매수n"] = num(L["매수량"]).fillna(0)
L["실현n"] = num(L["실현손익"]).fillna(0); L["손익률n"] = num(L["실현손익률(%)"])
L["평단n"] = num(L["평단가"]).fillna(0); L["예수금n"] = num(L["예수금"])
L["d"] = pd.to_datetime(L["날짜"])
S = L[L["종가n"].notna()].copy().reset_index(drop=True)      # 정산 완료 행만 (유령/예정 제외)
ghost = L[L["종가n"].isna() & (L["d"] < L["d"].max())]
pend = L.iloc[-1] if pd.isna(L.iloc[-1]["종가n"]) else None

# ── 입금(자본조정) 탐지: Δ총자산 − 보유×Δ종가 잔차가 큰 날 ──
S["dEq"] = S["총자산n"].diff()
S["dPx"] = S["종가n"].diff()
S["prevH"] = S["보유n"].shift(1)
S["resid"] = S["dEq"] - S["prevH"] * S["dPx"]
dep = S[S["resid"].abs() > 5000][["d", "resid"]].copy()
dep["resid"] = dep["resid"].round(0)
print("입금 탐지:", [(d.date(), int(v)) for d, v in zip(dep["d"], dep["resid"])])
deposits = dict(zip(dep["d"], dep["resid"]))
S["cumdep"] = S["d"].map(lambda d: sum(v for k, v in deposits.items() if k <= d))
BASE = float(S["총자산n"].iloc[0]) - S["cumdep"].iloc[0]       # 시작 자본 (첫 행 총자산)
# 시간가중 수익률 (TWR): 입금일의 입금분을 제외한 일별 수익률을 연결.
# (총자산 − 누적입금 '수준'으로 DD를 재면 입금이 자산 대비 클 때 %가 폭발 → 사용 금지)
S["ret"] = (S["dEq"] - S["d"].map(lambda d: deposits.get(d, 0))) / S["총자산n"].shift(1)
S["twr"] = (1 + S["ret"].fillna(0)).cumprod()
S["pnl_curve"] = BASE * S["twr"]                                   # 입금 없었다면의 자산 궤적
S["peak"] = S["pnl_curve"].cummax(); S["dd"] = S["twr"] / S["twr"].cummax() - 1
S["twr_dd"] = S["dd"]

last = S.iloc[-1]
total_in = BASE + sum(deposits.values())
equity = float(last["총자산n"]); net = equity - total_in
realized = float(S["실현n"].sum()); unreal = float(last["보유n"] * (last["종가n"] - last["평단n"]))
mdd = float(S["dd"].min()); mdd_i = int(S["dd"].idxmin())
cur_dd = float(last["dd"]); peak_date = S.loc[S["twr"].idxmax(), "d"].date()
since_peak = int((S["d"] > pd.Timestamp(peak_date)).sum())
twr_total = float(last["twr"] - 1); twr_mdd = mdd; twr_cur = cur_dd
peak_eq_actual = float(S.loc[S["twr"].idxmax(), "총자산n"])      # 그날의 실제 총자산

# 거래 통계
sells = S[S["매도n"] > 0]
wins = sells[sells["실현n"] > 0]; losses = sells[sells["실현n"] < 0]
top_loss = sells.nsmallest(3, "실현n")[["d", "종가n", "매도n", "평단n", "실현n", "손익률n"]]
top_win = sells.nlargest(3, "실현n")[["d", "종가n", "매도n", "평단n", "실현n", "손익률n"]]
realized_ex_last = realized - float(sells.iloc[-1]["실현n"])

# ── SOXL 동일 입금 보유(DCA) 비교 + Sonic 시뮬(TWR) 비교 ──
import yfinance as yf
d = yf.download("SOXL", start="2026-02-01", progress=False, auto_adjust=True)
if isinstance(d.columns, pd.MultiIndex): d.columns = d.columns.get_level_values(0)
px = d[["Close"]].dropna(); px.index = pd.to_datetime(px.index).tz_localize(None)
now_et = pd.Timestamp.now(tz="America/New_York")
if now_et < now_et.replace(hour=16, minute=30, second=0, microsecond=0):
    px = px[px.index.normalize() < pd.Timestamp(now_et.date())]
c = px["Close"]
start_d = S["d"].iloc[0]; end_d = S["d"].iloc[-1]
cc = c.loc[start_d:end_d]
# DCA: 시작자본은 첫날 종가, 입금은 입금 탐지일 종가로 매수
shares = BASE / cc.iloc[0]
bh_curve = []
for dt, p in cc.items():
    if dt in deposits: shares += deposits[dt] / p
    bh_curve.append(shares * p)
bh = pd.Series(bh_curve, index=cc.index)
bh_end = float(bh.iloc[-1]); bh_net = bh_end - total_in
bh_pure = cc / cc.iloc[0] * BASE                                  # SOXL 가격 지수 × 시작자본
bh_mdd = float((cc / cc.cummax() - 1).min())
soxl_ret = float(cc.iloc[-1] / cc.iloc[0] - 1)
# Sonic 시뮬 (6/2 시작, 50k, 입금 없음 → TWR 비교)
P = dict(sigma_period=2, k_buy=0.55, k_sell=0.45, sell_ratio=95.0, divisions=3, renewal=3, pcr=1.0, lcr=1.0)
res = run_backtest_stdev(px, str(start_d.date()), str(end_d.date()), initial_capital=BASE, return_history=True, **P)
H = res["history"]; H["d"] = pd.to_datetime(H["날짜"])
sim_twr = float(H["총자산"].iloc[-1] / BASE - 1); sim_mdd = float(res["mdd"])
sim_sells = int(res["sell_count"])
# 실계좌 vs 시뮬 일별 비교(TWR 지수)
cmp = pd.DataFrame({"실계좌": S.set_index("d")["twr"], "시뮬": H.set_index("d")["총자산"] / BASE}).dropna()

# ── 차트 ──
def b64(fig):
    buf = io.BytesIO(); fig.savefig(buf, format="png", dpi=110, bbox_inches="tight"); plt.close(fig); return base64.b64encode(buf.getvalue()).decode()
C_BLUE, C_RED, C_AQUA, C_ORG, C_GRAY = "#2a78d6", "#e34948", "#1baf7a", "#eb6834", "#8a897f"
fig, ax = plt.subplots(figsize=(10, 4.4))
ax.plot(S["d"], S["pnl_curve"], color=C_BLUE, lw=1.8, label="실계좌 — 입금이 없었다면의 궤적 (시간가중 × 시작자본)")
ax.plot(S["d"], S["peak"], color=C_BLUE, lw=.8, ls=":", label="전고점")
ax.plot(bh_pure.index, bh_pure.values, color=C_GRAY, lw=1.2, label="SOXL 보유 (같은 기준)")
for dt in deposits: ax.axvline(dt, color=C_AQUA, lw=.8, ls="--")
sl = S[S["매도n"] > 0]; ax.scatter(sl["d"], sl["pnl_curve"], marker="v", s=28, color=C_RED, zorder=5, label="매도")
ax.axhline(BASE, color="#bbb", lw=.7); ax.set_title("순수 수익 곡선 (입금 타이밍 효과 제거) — 초록 점선 = 입금일", fontsize=12)
ax.legend(fontsize=8.5); ax.grid(alpha=.25); ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"${v/1000:.0f}k"))
img_eq = b64(fig)
fig, ax = plt.subplots(figsize=(10, 2.8))
ax.fill_between(S["d"], S["dd"] * 100, 0, color=C_RED, alpha=.35); ax.plot(S["d"], S["dd"] * 100, color=C_RED, lw=1)
ax.axhline(mdd * 100, color=C_RED, ls="--", lw=.8); ax.text(S["d"].iloc[2], mdd * 100 + 1, f"MDD {mdd*100:.1f}%", color=C_RED, fontsize=9)
ax.set_title("전고점 대비 낙폭 (DD)", fontsize=12); ax.grid(alpha=.25); ax.set_ylabel("%")
img_dd = b64(fig)
fig, ax = plt.subplots(figsize=(10, 3.6))
ax.plot(cmp.index, (cmp["실계좌"] - 1) * 100, color=C_BLUE, lw=1.8, label="실계좌 (시간가중)")
ax.plot(cmp.index, (cmp["시뮬"] - 1) * 100, color=C_ORG, lw=1.4, label="Sonic 시뮬 (6/2 $50k)")
ax.plot(cc.index, (cc / cc.iloc[0] - 1) * 100, color=C_GRAY, lw=1, label="SOXL")
ax.axhline(0, color="#bbb", lw=.7); ax.set_title("6/2 이후 누적 수익률 — 실계좌 vs 시뮬 vs SOXL", fontsize=12); ax.legend(fontsize=8.5); ax.grid(alpha=.25); ax.set_ylabel("%")
img_cmp = b64(fig)

def pct(x, d=1): return f"{x*100:+.{d}f}%"
def money(x): return f"${x:,.0f}"
def trow(r): return f"<tr><td>{r['d'].date()}</td><td class='r'>${r['종가n']:.2f}</td><td class='r'>{int(r['매도n']):,}주</td><td class='r'>${r['평단n']:.2f}</td><td class='r {'pos' if r['실현n']>0 else 'neg'}'>{money(r['실현n'])}</td><td class='r {'pos' if r['실현n']>0 else 'neg'}'>{r['손익률n']:+.1f}%</td></tr>"
dep_rows = "".join(f"<tr><td>{k.date()}</td><td class='r'>{money(v)}</td></tr>" for k, v in deposits.items())
ghost_txt = (f"<p class='meta'>참고: 원장에 종가 없는 행 {len(ghost)}개({', '.join(str(x.date()) for x in ghost['d'])}) — 휴장일 유령 예정 행(8월 수정 전 잔재). 곡선 계산에서 제외했습니다.</p>" if len(ghost) else "")
TODAY = datetime.today().date()
html = f"""<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>표준편차 실계좌 성과 — {TODAY}</title>
<style>:root{{--ink:#0b0b0b;--ink2:#52514e;--ink3:#8a897f;--surf:#fcfcfb;--card:#fff;--line:#e6e5e2;--blue:#2a78d6;--orange:#eb6834;--aqua:#1baf7a;--red:#e34948;--blue-bg:#eef4fc;--red-bg:#fdeeee;--aqua-bg:#e9f7f1}}
*{{box-sizing:border-box;margin:0;padding:0}} body{{font-family:'Malgun Gothic',sans-serif;background:var(--surf);color:var(--ink);line-height:1.75;font-size:15px}} .wrap{{max-width:900px;margin:0 auto;padding:40px 22px 80px}}
h1{{font-size:26px;line-height:1.4}} h2{{font-size:20px;margin:48px 0 14px;padding-top:18px;border-top:2px solid var(--ink)}} h2 .no{{color:var(--blue);margin-right:8px}} h3{{font-size:16px;margin:24px 0 8px}} p{{margin:10px 0}} .meta{{color:var(--ink3);font-size:13px}}
table{{border-collapse:collapse;width:100%;margin:14px 0;font-size:13.5px;background:var(--card)}} th{{background:#f4f3f1;color:var(--ink2);font-weight:600;text-align:left;padding:7px 10px;border:1px solid var(--line)}} td{{padding:7px 10px;border:1px solid var(--line);white-space:nowrap}} td.r,th.r{{text-align:right}}
.pos{{color:var(--aqua);font-weight:600}} .neg{{color:var(--red);font-weight:600}} .tiles{{display:flex;gap:12px;flex-wrap:wrap;margin:18px 0}} .tile{{flex:1 1 150px;background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px}} .tile .k{{font-size:12.5px;color:var(--ink2)}} .tile .v{{font-size:23px;font-weight:700}} .tile .d{{font-size:12px;color:var(--ink3)}}
.v.blue{{color:var(--blue)}} .v.red{{color:var(--red)}} .v.aqua{{color:var(--aqua)}} .v.orange{{color:var(--orange)}} figure{{margin:20px 0;background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px}} figure img{{max-width:100%;display:block;margin:0 auto}} figcaption{{font-size:12.5px;color:var(--ink3);margin-top:8px;text-align:center}}
.note{{background:var(--blue-bg);border-radius:8px;padding:13px 16px;margin:16px 0;font-size:14px}} .warn{{background:var(--red-bg);border-radius:8px;padding:13px 16px;margin:16px 0;font-size:14px}} .good{{background:var(--aqua-bg);border-radius:8px;padding:13px 16px;margin:16px 0;font-size:14px}} ul{{margin:10px 0 10px 22px}} .foot{{margin-top:50px;padding-top:16px;border-top:1px solid var(--line);font-size:12.5px;color:var(--ink3)}}</style></head><body><div class="wrap">
<p class="meta">표준편차매매 · 실계좌 원장(sd_SOXL_매매기록) 기준 · {start_d.date()} ~ {end_d.date()} 정산 {len(S)}거래일</p>
<h1>6/2 이후 실계좌 성과와 지금의 낙폭</h1>
<div class="tiles">
<div class="tile"><div class="k">투입 원금</div><div class="v">{money(total_in)}</div><div class="d">시작 {money(BASE)} + 입금 {money(sum(deposits.values()))}</div></div>
<div class="tile"><div class="k">현재 자산</div><div class="v {'aqua' if net>=0 else 'red'}">{money(equity)}</div><div class="d">순손익 {money(net)} ({pct(net/total_in)})</div></div>
<div class="tile"><div class="k">현재 DD</div><div class="v red">{pct(cur_dd)}</div><div class="d">전고점 {peak_date} 이후 {since_peak}거래일</div></div>
<div class="tile"><div class="k">MDD</div><div class="v red">{pct(mdd)}</div><div class="d">{S.loc[mdd_i,'d'].date()} 기록</div></div></div>
<h3>입금(자본조정) — 원장 점프로 탐지</h3><table><tr><th>날짜</th><th class="r">금액</th></tr>{dep_rows}</table>{ghost_txt}

<h2><span class="no">1</span>DD와 MDD — 숫자의 뜻</h2>
<p><b>DD(현재 낙폭)</b>는 "수익 곡선의 최고점에서 지금 얼마나 내려와 있나", <b>MDD(최대 낙폭)</b>는 "그 곡선이 역사상 가장 깊게 내려갔던 폭"입니다.
입금은 수익이 아니므로 <b>시간가중(TWR)</b> 방식으로 뺐습니다 — 입금일의 입금분을 제외한 '하루 수익률'들을 이어 붙인 곡선이라,
돈이 언제 들어왔든 운용 실력만 남습니다. (총자산에서 누적 입금을 그냥 빼는 방식은 입금이 자산 대비 크면 %가 폭발해서 쓰지 않았습니다.)</p>
<div class="tiles">
<div class="tile"><div class="k">전고점 날짜</div><div class="v blue">{peak_date}</div><div class="d">그날 실제 총자산 {money(peak_eq_actual)}</div></div>
<div class="tile"><div class="k">금액가중 순손익</div><div class="v {'aqua' if net>=0 else 'red'}">{pct(net/total_in)}</div><div class="d">내 돈 기준 {money(net)}</div></div>
<div class="tile"><div class="k">시간가중 수익률</div><div class="v {'aqua' if twr_total>=0 else 'red'}">{pct(twr_total)}</div><div class="d">입금 타이밍 효과 제거 · MDD {pct(twr_mdd)}</div></div>
<div class="tile"><div class="k">SOXL 같은 기간</div><div class="v {'aqua' if soxl_ret>=0 else 'red'}">{pct(soxl_ret)}</div><div class="d">${cc.iloc[0]:.2f} → ${cc.iloc[-1]:.2f}</div></div></div>
<figure><img src="data:image/png;base64,{img_eq}"><figcaption>파랑 = 실계좌 순수 수익 곡선, 회색 = 같은 입금으로 SOXL을 그냥 샀을 때, ▼ = 매도일</figcaption></figure>
<figure><img src="data:image/png;base64,{img_dd}"><figcaption>전고점 대비 낙폭. 지금 {pct(cur_dd)}, 최대 {pct(mdd)}</figcaption></figure>
<div class="warn"><b>10/19 매도가 한 일.</b> 1,097주를 ${float(sells.iloc[-1]['종가n']):.2f}에 팔아 <b>{money(float(sells.iloc[-1]['실현n']))} ({float(sells.iloc[-1]['손익률n']):+.1f}%)</b>를 확정했습니다.
그 전까지의 누적 실현은 <b>{money(realized_ex_last)}</b>였고, 이 한 번으로 누적 실현이 {money(realized)}가 됐습니다.
다만 자산 곡선 자체는 매도로 떨어진 게 아닙니다 — 10/5~10/16 하락 동안 평가손실로 이미 내려와 있던 것을 '실현'으로 바꾼 것이라,
10/16 → 10/19 자산은 {money(float(S.iloc[-2]['총자산n']))} → {money(equity)}로 오히려 소폭 늘었습니다(반등일 +5.3%).</div>

<h2><span class="no">2</span>6/2 이후 실적</h2>
<div class="tiles">
<div class="tile"><div class="k">누적 실현손익</div><div class="v {'aqua' if realized>=0 else 'red'}">{money(realized)}</div><div class="d">매도 {len(sells)}회</div></div>
<div class="tile"><div class="k">미실현</div><div class="v {'aqua' if unreal>=0 else 'red'}">{money(unreal)}</div><div class="d">보유 {int(last['보유n'])}주 @ ${last['평단n']:.2f}</div></div>
<div class="tile"><div class="k">승률</div><div class="v">{len(wins)/max(1,len(sells))*100:.0f}%</div><div class="d">{len(wins)}승 {len(losses)}패 · 평균 {sells['손익률n'].mean():+.1f}%</div></div>
<div class="tile"><div class="k">현금 비중</div><div class="v blue">{float(last['예수금n'])/equity*100:.0f}%</div><div class="d">{money(float(last['예수금n']))}</div></div></div>
<table><tr><th></th><th class="r">실계좌</th><th class="r">SOXL 동일 입금 보유</th><th class="r">Sonic 시뮬 (6/2, 입금 없음)</th></tr>
<tr><td>순손익 (원금 대비)</td><td class="r {'pos' if net>=0 else 'neg'}">{money(net)} ({pct(net/total_in)})</td><td class="r {'pos' if bh_net>=0 else 'neg'}">{money(bh_net)} ({pct(bh_net/total_in)})</td><td class="r">-</td></tr>
<tr><td>시간가중 수익률</td><td class="r {'pos' if twr_total>=0 else 'neg'}">{pct(twr_total)}</td><td class="r {'pos' if soxl_ret>=0 else 'neg'}">{pct(soxl_ret)}</td><td class="r {'pos' if sim_twr>=0 else 'neg'}">{pct(sim_twr)}</td></tr>
<tr><td>MDD (입금 제거)</td><td class="r neg">{pct(mdd)}</td><td class="r neg">{pct(bh_mdd)}</td><td class="r neg">{pct(sim_mdd)}</td></tr>
<tr><td>매도 횟수</td><td class="r">{len(sells)}</td><td class="r">-</td><td class="r">{sim_sells}</td></tr></table>
<figure><img src="data:image/png;base64,{img_cmp}"><figcaption>누적 수익률 비교 (시간가중 = 입금 타이밍 효과 제거)</figcaption></figure>
<h3>가장 컸던 손실 3건</h3><table><tr><th>날짜</th><th class="r">매도가</th><th class="r">수량</th><th class="r">평단</th><th class="r">실현</th><th class="r">손익률</th></tr>{''.join(trow(r) for _, r in top_loss.iterrows())}</table>
<h3>가장 컸던 이익 3건</h3><table><tr><th>날짜</th><th class="r">매도가</th><th class="r">수량</th><th class="r">평단</th><th class="r">실현</th><th class="r">손익률</th></tr>{''.join(trow(r) for _, r in top_win.iterrows())}</table>
<div class="note">실계좌와 시뮬의 차이는 (1) 입금 타이밍과 보정으로 생긴 상태 차이, (2) 수동 주문·체결 차이(수량 불일치 사고들), (3) 7~8월 원장 전환기의 앵커 조정이 섞인 결과입니다. 같은 파라미터라도 '언제 얼마가 들어갔는가'가 결과를 크게 바꿉니다.</div>
<div class="foot">생성 {datetime.now().strftime('%Y-%m-%d %H:%M')} · 재현: reports/sd_account_perf.py (입력 _sd_ledger_snapshot.csv)</div></div></body></html>"""
out = os.path.join(SP, f"{TODAY.strftime('%y%m%d')}_표준편차_실계좌_성과.html")
open(out, "w", encoding="utf-8").write(html); print("저장:", out)
print(f"\n원금 {money(total_in)} (시작 {money(BASE)} + 입금 {money(sum(deposits.values()))}) → 자산 {money(equity)} 순손익 {money(net)} ({pct(net/total_in)})")
print(f"실현 {money(realized)} (10/19 제외 {money(realized_ex_last)}) / 미실현 {money(unreal)} / 매도 {len(sells)}회 승률 {len(wins)/len(sells)*100:.0f}% 평균 {sells['손익률n'].mean():+.1f}%")
print(f"전고점 {peak_date} {money(float(S['peak'].iloc[-1]))} → 지금 {money(float(last['pnl_curve']))} | DD {pct(cur_dd)} ({since_peak}일) | MDD {pct(mdd)} ({S.loc[mdd_i,'d'].date()})")
print(f"TWR 실계좌 {pct(twr_total)} (MDD {pct(twr_mdd)}) / SOXL {pct(soxl_ret)} (DCA MDD {pct(bh_mdd)}, DCA 순손익 {money(bh_net)}) / 시뮬 {pct(sim_twr)} (MDD {pct(sim_mdd)}, 매도 {sim_sells})")
print(f"10/16→10/19 자산 {money(float(S.iloc[-2]['총자산n']))} → {money(equity)} | 현금비중 {float(last['예수금n'])/equity*100:.0f}%")
