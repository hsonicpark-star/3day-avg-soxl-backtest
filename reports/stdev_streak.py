# -*- coding: utf-8 -*-
"""표준편차매매(Sonic형) — SOXL 급락(연속 하락) 구간 분석 리포트 생성기.

실행: python stdev_streak.py  (이 폴더에서)
출력: YYMMDD_표준편차_연속하락_분석.html (+ stdev_streak_episodes.csv)

데이터: yfinance SOXL 조정 종가(auto_adjust=True) — 표준편차 웹/발송과 동일 기준.
엔진 : stdev_engine.run_backtest_stdev / run_stdev_ordersheet (앱과 동일 코드).

유사 사례 정의: 연속 하락 '길이'로는 9일이 사상 최초라 표본이 없음 →
  7월 DSS 리포트와 같이 '깊이'로 정의: 9거래일 수익률이 -35% 이하로 처음 진입한 날
  (20거래일 쿨다운으로 한 급락을 한 번만 셈).
"""
import os, sys, io, base64, math
from datetime import datetime
import numpy as np
import pandas as pd

SP = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(SP)
sys.path.insert(0, ROOT)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False

from stdev_engine import run_backtest_stdev, run_stdev_ordersheet

# ── Sonic형 파라미터 (앱 프리셋과 동일) ──
P = dict(sigma_period=2, k_buy=0.55, k_sell=0.45, sell_ratio=95.0,
         divisions=3, renewal=3, pcr=1.0, lcr=1.0)
CAPITAL = 10000.0
LOOK = 9            # 낙폭 측정 창 (거래일) — 지금의 연속 하락 길이와 맞춤
DEPTH_TH = -0.35    # 유사 사례: 9일 수익률 ≤ -35%
COOLDOWN = 20       # 같은 급락 중복 집계 방지
TODAY = datetime.today().date()

# ══════════════════════════════════════════════════════════
# 1. 데이터
# ══════════════════════════════════════════════════════════
def load_soxl():
    import yfinance as yf
    df = yf.download("SOXL", start="2010-03-01", progress=False, auto_adjust=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Close"]].dropna()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    now_et = pd.Timestamp.now(tz="America/New_York")
    if now_et < now_et.replace(hour=16, minute=30, second=0, microsecond=0):
        df = df[df.index.normalize() < pd.Timestamp(now_et.date())]
    df["Close"] = df["Close"].astype(float)
    return df

px = load_soxl()
closes = px["Close"]; dates = px.index; n = len(px)
LAST = dates[-1].date()
print(f"데이터 {dates[0].date()} ~ {LAST}, {n} 거래일")

# ══════════════════════════════════════════════════════════
# 2. 백테스트 (전 기간) + 오늘 주문 상태
# ══════════════════════════════════════════════════════════
START = "2010-03-16"
res = run_backtest_stdev(px, START, str(LAST), initial_capital=CAPITAL, return_history=True, **P)
H = res["history"].copy(); H["날짜"] = pd.to_datetime(H["날짜"]); H = H.set_index("날짜")
eq = H["총자산"].astype(float); peak = eq.cummax()
H["dd"] = eq / peak - 1
H["cash_ratio"] = H["예수금"].astype(float) / eq
OS = run_stdev_ordersheet(px, START, initial_capital=CAPITAL, **P)
print(f"전체 성과 CAGR {res['cagr']*100:.1f}% · MDD {res['mdd']*100:.1f}% · 매도 {res['sell_count']}회")

# ══════════════════════════════════════════════════════════
# 3. 연속 하락 streak (맥락용) + 깊이 기준 유사 사례
# ══════════════════════════════════════════════════════════
ret = closes.pct_change()
down = (ret < 0).astype(int)
streak = pd.Series(0, index=dates); run = 0
for i, d in enumerate(down.values):
    run = run + 1 if d == 1 else 0; streak.iloc[i] = run
ends = [(i, int(streak.iloc[i])) for i in range(n)
        if streak.iloc[i] >= 1 and (i == n - 1 or streak.iloc[i + 1] == 0)]
LENS = pd.Series([k for _, k in ends])
len_counts = {k: int((LENS >= k).sum()) for k in range(4, 11)}
max_len = int(LENS.max()); cur_len = int(streak.iloc[-1])

r9 = closes.pct_change(LOOK)
cond = r9 <= DEPTH_TH
trig = []
last_i = -10**9
for i in range(n):
    if cond.iloc[i] and (i - last_i) > COOLDOWN:
        trig.append(i); last_i = i
print(f"현재 연속 하락 {cur_len}일 · {LOOK}일 수익률 {r9.iloc[-1]*100:+.1f}% · 유사 사례 진입일 {len(trig)}건")

# ══════════════════════════════════════════════════════════
# 4. 유사 사례 전진 분석
# ══════════════════════════════════════════════════════════
def fwd(series, i, k):
    return series.iloc[i + k] / series.iloc[i] - 1 if i + k < len(series) else np.nan

rows = []
for i in trig:
    d = dates[i]
    st = H.loc[d]
    ongoing = (n - 1 - i) < 60     # 60거래일 전진 창이 아직 안 찬 사례
    after = H.loc[d:].iloc[1:]
    sells = after[after["매도량"].astype(float) > 0]
    if len(sells):
        fs = sells.index[0]
        days_to_sell = int((after.index <= fs).sum())
        sell_day_ret = closes.loc[fs] / closes.loc[:fs].iloc[-2] - 1
        sell_pnl = sells.iloc[0]["실현손익률(%)"]
        extra_drop = closes.loc[d:fs].min() / closes.loc[d] - 1
    else:
        fs = None; days_to_sell = np.nan; sell_day_ret = np.nan; sell_pnl = np.nan; extra_drop = np.nan
    pk = peak.loc[d]
    rec = after[after["총자산"].astype(float) >= pk]
    days_to_recover = int((after.index <= rec.index[0]).sum()) if len(rec) else np.nan
    p_before = closes.iloc[i - LOOK]
    prc_after = closes.iloc[i + 1:]
    prc_rec = prc_after[prc_after >= p_before]
    days_price_recover = int(prc_after.index.get_loc(prc_rec.index[0]) + 1) if len(prc_rec) else np.nan
    seg_end = rec.index[0] if len(rec) else H.index[-1]
    eq_min_after = eq.loc[d:seg_end].min() / eq.loc[d] - 1
    price_min_after = closes.loc[d:seg_end].min() / closes.loc[d] - 1
    rows.append(dict(
        진입일=d.date(), 연속하락=int(streak.iloc[i]), 낙폭9일=float(r9.iloc[i]),
        상승일=int((ret.iloc[i - LOOK + 1:i + 1] > 0).sum()),
        고점대비=float(closes.iloc[i] / closes.iloc[:i + 1].max() - 1),
        현금비중=float(st["cash_ratio"]), 자산DD=float(st["dd"]),
        평가손익률=(float(st["종가"]) / float(st["평단가"]) - 1) if float(st["평단가"]) > 0 else np.nan,
        매도까지_일=days_to_sell, 매도일_등락=sell_day_ret, 매도_손익률=sell_pnl,
        매도전_추가하락=extra_drop, 주가_추가최저=price_min_after, 자산_추가최저=eq_min_after,
        자산회복_일=days_to_recover, 주가회복_일=days_price_recover,
        주가_5일=fwd(closes, i, 5), 주가_20일=fwd(closes, i, 20), 주가_60일=fwd(closes, i, 60),
        자산_20일=fwd(eq, i, 20), 자산_60일=fwd(eq, i, 60), 자산_120일=fwd(eq, i, 120),
        진행중=ongoing))
SIM = pd.DataFrame(rows)
SIM.to_csv(os.path.join(SP, "stdev_streak_episodes.csv"), index=False, encoding="utf-8-sig")
done = SIM[~SIM["진행중"]]; n_sim = len(done)
print(f"완결 사례 {n_sim}건 / 진행중 {int(SIM['진행중'].sum())}건")

# ══════════════════════════════════════════════════════════
# 5. 현재 상태
# ══════════════════════════════════════════════════════════
now = H.iloc[-1]
sig_now = float(OS["sigma_next"]); need_up = sig_now * P["k_sell"]
cur = dict(close=float(now["종가"]), sell_loc=float(OS["next_sell_loc"]), buy_loc=float(OS["next_buy_loc"]),
           holdings=int(now["보유량"]), avg=float(now["평단가"]), cash_ratio=float(now["cash_ratio"]),
           dd=float(now["dd"]), pnl=(float(now["종가"]) / float(now["평단가"]) - 1) if float(now["평단가"]) > 0 else 0)
cur_drop = float(r9.iloc[-1]); hi_date = closes.idxmax().date(); hi_val = float(closes.max())
cur_dd_price = float(closes.iloc[-1] / hi_val - 1)
rank_pct = float((r9.dropna() <= cur_drop).mean() * 100)
first_sell_days = done["매도일_등락"].dropna()
in_cond_now = bool(cond.iloc[-1])
cur_ups = int((ret.iloc[-LOOK:] > 0).sum())        # 지금 9일 창의 상승일 수
ups_min_past = int(done["상승일"].min()) if n_sim else 0
entry_cur = pd.Timestamp(SIM[SIM["진행중"]].iloc[-1]["진입일"]) if SIM["진행중"].any() else None
days_since_entry = int((dates > entry_cur).sum()) if entry_cur is not None else None

# 매도 공백: 보유 중인데 매도가 없는 연속 거래일 (역대 기록과 비교)
_sell = H["매도량"].astype(float) > 0; _hold = H["보유량"].astype(float) > 0
droughts = []; _s = None
for dt, s_, h_ in zip(H.index, _sell, _hold):
    if h_ and not s_:
        if _s is None: _s = dt
    elif _s is not None:
        droughts.append((_s, dt, int(((H.index >= _s) & (H.index < dt)).sum()))); _s = None
if _s is not None:
    droughts.append((_s, H.index[-1], int((H.index >= _s).sum())))
droughts.sort(key=lambda x: -x[2])
last_sell_date = H[_sell].index[-1]
drought_cur = int((H.index > last_sell_date).sum())
drought_rank = 1 + sum(1 for _, _, k in droughts if k > drought_cur)
drought_rows = "".join(
    f"<tr><td>{s.date()} ~ {e.date()}{' <b>(지금)</b>' if e == H.index[-1] else ''}</td><td class='r'>{k}일</td>"
    f"<td class='r neg'>{(closes.loc[s:e].iloc[-1]/closes.loc[s:e].iloc[0]-1)*100:+.0f}%</td>"
    f"<td class='r neg'>{(closes.loc[s:e].min()/closes.loc[s:e].iloc[0]-1)*100:+.0f}%</td></tr>"
    for s, e, k in droughts[:6])

# k_sell 민감도 (전 기간, 참고용)
sens = []
for ks in (0.45, 0.35, 0.25, 0.15):
    r2 = run_backtest_stdev(px, START, str(LAST), initial_capital=CAPITAL, return_history=False, **{**P, "k_sell": ks})
    sens.append(dict(k_sell=ks, cagr=r2["cagr"], mdd=r2["mdd"], calmar=r2["calmar"], sells=r2["sell_count"], avg_pnl=r2["avg_pnl"]))
SENS = pd.DataFrame(sens)

# ══════════════════════════════════════════════════════════
# 6. 차트
# ══════════════════════════════════════════════════════════
def b64(fig):
    buf = io.BytesIO(); fig.savefig(buf, format="png", dpi=110, bbox_inches="tight"); plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()
C_BLUE, C_RED, C_AQUA, C_ORG, C_GRAY = "#2a78d6", "#e34948", "#1baf7a", "#eb6834", "#8a897f"

seg = H.iloc[-60:]
fig, ax = plt.subplots(figsize=(10, 4.6))
ax.plot(seg.index, seg["종가"], color="#0b0b0b", lw=1.8, label="SOXL 종가")
ax.plot(seg.index, seg["매도LOC"], color=C_BLUE, lw=1, ls="--", label="매도 LOC = 전일종가×(1+σ×0.45)")
ax.plot(seg.index, seg["매수LOC"], color=C_RED, lw=1, ls=":", label="매수 LOC")
b = seg[seg["매수량"].astype(float) > 0]; s = seg[seg["매도량"].astype(float) > 0]
ax.scatter(b.index, b["종가"], marker="^", color=C_RED, s=55, zorder=5, label="매수 체결")
ax.scatter(s.index, s["종가"], marker="v", color=C_BLUE, s=55, zorder=5, label="매도 체결")
if cur_len:
    ax.axvspan(dates[-cur_len], dates[-1], color=C_RED, alpha=0.07)
ax.set_title("최근 60거래일 — 종가와 매도 문턱(파란 점선)이 벌어진 모습", fontsize=12)
ax.legend(fontsize=8.5, loc="upper left"); ax.grid(alpha=.25)
img_recent = b64(fig)

fig, ax = plt.subplots(figsize=(8, 3.6))
vc = LENS.value_counts().sort_index()
ax.bar(vc.index, vc.values, color=[C_RED if k >= 7 else C_GRAY for k in vc.index])
for k, v in vc.items(): ax.text(k, v, str(v), ha="center", va="bottom", fontsize=9)
ax.set_xlabel("연속 하락 거래일 수"); ax.set_ylabel("에피소드 수 (로그)"); ax.set_yscale("log")
ax.set_title("2010~ SOXL 연속 하락 에피소드 — 9일은 지금이 처음", fontsize=12); ax.grid(alpha=.25, axis="y")
img_len = b64(fig)

fig, ax = plt.subplots(figsize=(8, 4.2))
d2 = done.dropna(subset=["주가_60일"])
ax.scatter(d2["낙폭9일"] * 100, d2["주가_60일"] * 100, color=C_GRAY, s=42, label="SOXL 60일 후")
ax.scatter(d2["낙폭9일"] * 100, d2["자산_60일"] * 100, color=C_BLUE, s=42, label="전략 자산 60일 후")
ax.axhline(0, color="#999", lw=.8)
ax.axvline(cur_drop * 100, color=C_ORG, ls="--"); ax.text(cur_drop * 100 + .4, ax.get_ylim()[1] * .85, "지금", color=C_ORG)
ax.set_xlabel(f"진입일의 {LOOK}일 낙폭 (%)"); ax.set_ylabel("60거래일 후 등락 (%)")
ax.set_title(f"{LOOK}일 낙폭 {DEPTH_TH*100:.0f}% 이하 진입 후 60일", fontsize=12); ax.legend(fontsize=9); ax.grid(alpha=.25)
img_fwd = b64(fig)

fig, ax = plt.subplots(figsize=(8, 3.4))
dts = done["매도까지_일"].dropna().astype(int)
ax.hist(dts, bins=range(1, int(dts.max()) + 2), color=C_BLUE, alpha=.85)
ax.set_xlabel("진입 후 첫 매도까지 거래일"); ax.set_ylabel("사례 수")
ax.set_title("매도 기회는 언제 왔나", fontsize=12); ax.grid(alpha=.25, axis="y")
img_sell = b64(fig)

worst = done.sort_values("자산_추가최저").head(5)
fig, axes = plt.subplots(1, 5, figsize=(13, 3.2))
for ax, (_, w) in zip(axes, worst.iterrows()):
    d0 = pd.Timestamp(w["진입일"]); i0 = dates.get_loc(d0)
    sl = slice(max(0, i0 - 20), min(n, i0 + 120))
    ax.plot(closes.index[sl], closes.iloc[sl] / closes.iloc[i0] * 100, color=C_GRAY, lw=1.2, label="SOXL")
    ax.plot(eq.index[sl], eq.iloc[sl] / eq.iloc[i0] * 100, color=C_BLUE, lw=1.6, label="전략")
    ax.axvline(d0, color=C_RED, lw=.8, ls="--"); ax.axhline(100, color="#bbb", lw=.6)
    ax.set_title(f"{w['진입일']} ({w['낙폭9일']*100:.0f}%)", fontsize=9)
    ax.tick_params(labelsize=7); ax.xaxis.set_major_locator(plt.MaxNLocator(3))
axes[0].legend(fontsize=8)
fig.suptitle("진입 후 자산이 가장 많이 더 빠졌던 5개 사례 — 진입일=100, 이후 120거래일", fontsize=11)
img_worst = b64(fig)

# ══════════════════════════════════════════════════════════
# 7. HTML
# ══════════════════════════════════════════════════════════
def isnan(x): return x is None or (isinstance(x, float) and math.isnan(x))
def pct(x, d=1, sign=True):
    if isnan(x): return "-"
    return f"{x*100:+.{d}f}%" if sign else f"{x*100:.{d}f}%"
def dfmt(v): return "-" if isnan(v) else f"{int(v)}일"
def med(s): s = s.dropna(); return float(s.median()) if len(s) else float("nan")
def share_pos(s): s = s.dropna(); return float((s > 0).mean()) if len(s) else float("nan")
def colr(v, txt): return f'<td class="r {"pos" if v > 0 else "neg" if v < 0 else ""}">{txt}</td>' if not isnan(v) else f'<td class="r">{txt}</td>'

case_rows = []
for _, r in done.sort_values("진입일", ascending=False).iterrows():
    case_rows.append("<tr>"
        f"<td>{r['진입일']}</td><td class='c'>{int(r['연속하락'])}일</td><td class='c'>{int(r['상승일'])}/{LOOK}</td>" + colr(r["낙폭9일"], pct(r["낙폭9일"])) + colr(r["고점대비"], pct(r["고점대비"]))
        + f"<td class='r'>{pct(r['현금비중'],0,False)}</td>" + colr(r["자산DD"], pct(r["자산DD"]))
        + f"<td class='r'>{dfmt(r['매도까지_일'])}</td>" + colr(r["매도일_등락"], pct(r["매도일_등락"]))
        + colr(r["매도전_추가하락"], pct(r["매도전_추가하락"])) + colr(r["자산_추가최저"], pct(r["자산_추가최저"]))
        + f"<td class='r'>{dfmt(r['자산회복_일'])}</td><td class='r'>{dfmt(r['주가회복_일'])}</td>"
        + colr(r["자산_60일"], pct(r["자산_60일"])) + "</tr>")
case_html = ("<table><tr><th>진입일</th><th class='c'>연속하락</th><th class='c'>상승일</th><th class='r'>9일 낙폭</th><th class='r'>고점 대비</th>"
             "<th class='r'>현금비중</th><th class='r'>자산 DD</th><th class='r'>첫 매도까지</th><th class='r'>매도일 등락</th>"
             "<th class='r'>매도 전 추가하락</th><th class='r'>자산 추가최저</th><th class='r'>자산 회복</th><th class='r'>주가 회복</th>"
             "<th class='r'>자산 60일</th></tr>" + "".join(case_rows) + "</table>")

sens_rows = "".join(
    f"<tr><td class='c'>{r.k_sell:.2f}{' <b>(현재)</b>' if abs(r.k_sell-0.45)<1e-9 else ''}</td>"
    f"<td class='r'>{r.cagr*100:.1f}%</td><td class='r neg'>{r.mdd*100:.1f}%</td><td class='r'>{r.calmar:.2f}</td>"
    f"<td class='r'>{int(r.sells)}</td><td class='r'>{r.avg_pnl:+.2f}%</td></tr>" for r in SENS.itertuples())

n_recov = int(done["자산회복_일"].notna().sum())
both_mask = done["자산회복_일"].notna() & done["주가회복_일"].notna()
both = int(both_mask.sum()); beat_price = int((done.loc[both_mask, "자산회복_일"] < done.loc[both_mask, "주가회복_일"]).sum())
cur_in_txt = (f"지금({LAST})도 이 조건 안에 있습니다." if in_cond_now else "지금은 이 조건을 막 벗어난 상태입니다.")
n_le40 = int((done["낙폭9일"] <= -0.40).sum())

html = f"""<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>표준편차매매 급락 리포트 — {TODAY}</title>
<style>
:root{{--ink:#0b0b0b;--ink2:#52514e;--ink3:#8a897f;--surf:#fcfcfb;--card:#fff;--line:#e6e5e2;--blue:#2a78d6;--orange:#eb6834;--aqua:#1baf7a;--red:#e34948;--blue-bg:#eef4fc;--red-bg:#fdeeee;--aqua-bg:#e9f7f1}}
*{{box-sizing:border-box;margin:0;padding:0}} body{{font-family:'Malgun Gothic','Apple SD Gothic Neo',sans-serif;background:var(--surf);color:var(--ink);line-height:1.75;font-size:15px}}
.wrap{{max-width:900px;margin:0 auto;padding:40px 22px 80px}} h1{{font-size:26px;line-height:1.4;margin:6px 0 4px}}
h2{{font-size:20px;margin:56px 0 14px;padding-top:18px;border-top:2px solid var(--ink)}} h2 .no{{color:var(--blue);margin-right:8px}} h3{{font-size:16px;margin:26px 0 8px}}
p{{margin:10px 0}} .sub{{color:var(--ink2)}} .meta{{color:var(--ink3);font-size:13px;margin-top:8px}}
.disc{{background:var(--card);border:1px solid var(--line);border-left:4px solid var(--ink3);padding:12px 16px;font-size:13px;color:var(--ink2);margin:22px 0;border-radius:6px}}
table{{border-collapse:collapse;width:100%;margin:14px 0;font-size:13.5px;background:var(--card)}}
th{{background:#f4f3f1;color:var(--ink2);font-weight:600;text-align:left;padding:7px 10px;border:1px solid var(--line);white-space:nowrap}}
td{{padding:7px 10px;border:1px solid var(--line);white-space:nowrap}} td.r,th.r{{text-align:right}} td.c,th.c{{text-align:center}}
.pos{{color:var(--aqua);font-weight:600}} .neg{{color:var(--red);font-weight:600}}
.tiles{{display:flex;gap:12px;flex-wrap:wrap;margin:18px 0}} .tile{{flex:1 1 150px;background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px;min-width:150px}}
.tile .k{{font-size:12.5px;color:var(--ink2)}} .tile .v{{font-size:23px;font-weight:700;letter-spacing:-.5px}} .tile .d{{font-size:12px;color:var(--ink3)}}
.v.blue{{color:var(--blue)}} .v.red{{color:var(--red)}} .v.aqua{{color:var(--aqua)}} .v.orange{{color:var(--orange)}}
figure{{margin:20px 0;background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px;overflow-x:auto}} figure img{{max-width:100%;height:auto;display:block;margin:0 auto}}
figcaption{{font-size:12.5px;color:var(--ink3);margin-top:8px;text-align:center}}
.note{{background:var(--blue-bg);border-radius:8px;padding:13px 16px;margin:16px 0;font-size:14px}} .warn{{background:var(--red-bg);border-radius:8px;padding:13px 16px;margin:16px 0;font-size:14px}}
.good{{background:var(--aqua-bg);border-radius:8px;padding:13px 16px;margin:16px 0;font-size:14px}} .scroll{{overflow-x:auto}} ul{{margin:10px 0 10px 22px}} li{{margin:5px 0}}
.foot{{margin-top:60px;padding-top:16px;border-top:1px solid var(--line);font-size:12.5px;color:var(--ink3)}} @media (max-width:600px){{.tile{{flex:1 1 44%}} h1{{font-size:22px}}}}
</style></head><body><div class="wrap">

<p class="meta">표준편차매매 · Sonic형 파라미터 · {LAST} 종가 기준</p>
<h1>{cur_len}일 연속 하락 — 매도 문턱은 왜 멀어지고, 과거의 급락은 어떻게 끝났나</h1>
<p class="sub">하락에 맞춰 팔고 싶은데 기회가 오지 않는 지금, Sonic형 표준편차매매가 과거 {n_sim}번의 비슷한 급락에서 겪은 일</p>
<div class="disc">💡 투자 참고용 자료이며 손익 책임은 지지 않습니다. 백테스트는 과거 데이터 기반 분석일 뿐 미래 수익을 보장하지 않습니다.
7월 DSS 풀매수 리포트와 같은 프레임으로, <b>우리 앱의 Sonic형 파라미터·엔진 코드 그대로</b> 전 기간을 재계산했습니다.</div>

<h3>분석의 조건</h3>
<table><tr><th style="width:130px">항목</th><th>내용</th></tr>
<tr><td>대상</td><td>SOXL 조정 종가 (yfinance auto_adjust — 표준편차 웹·자동발송과 동일 기준)</td></tr>
<tr><td>기간</td><td>{dates[0].date()} ~ {LAST} ({(dates[-1]-dates[0]).days/365.25:.1f}년, {n:,}거래일)</td></tr>
<tr><td>파라미터</td><td>Sonic형 — σ 2일 · k_buy 0.55 · k_sell 0.45 · 매도 95% · 3분할 · 갱신 3 · 복리 100%</td></tr>
<tr><td>체결 가정</td><td>일봉 종가(LOC) 기준. 슬리피지·수수료 미반영</td></tr>
<tr><td>유사 사례 정의</td><td><b>{LOOK}거래일 수익률이 {DEPTH_TH*100:.0f}% 이하로 처음 들어간 날</b> (같은 급락은 {COOLDOWN}거래일 쿨다운으로 한 번만). 연속 하락 '길이'는 9일이 사상 최초라 표본이 없어, DSS 리포트처럼 '깊이'로 정의</td></tr>
<tr><td>회복 정의</td><td>진입일의 <b>자산 전고점</b>을 다시 넘는 첫 날 (진입일부터 거래일 수)</td></tr>
<tr><td>전체 성과</td><td>CAGR {res['cagr']*100:.1f}% · 최대 자산 낙폭 {res['mdd']*100:.1f}% · 매도 {res['sell_count']}회 (초기 $10,000)</td></tr></table>

<h2><span class="no">1</span>지금 벌어지고 있는 일</h2>
<div class="tiles">
<div class="tile"><div class="k">연속 하락</div><div class="v red">{cur_len}거래일</div><div class="d">사상 최장 (종전 최장 {max_len if cur_len < max_len else int(LENS.drop(LENS.idxmax()).max())}일)</div></div>
<div class="tile"><div class="k">{LOOK}거래일 낙폭</div><div class="v red">{pct(cur_drop)}</div><div class="d">역대 {LOOK}일 수익률 하위 {rank_pct:.1f}%</div></div>
<div class="tile"><div class="k">고점 대비 주가</div><div class="v orange">{pct(cur_dd_price)}</div><div class="d">고점 ${hi_val:,.2f} ({hi_date})</div></div>
<div class="tile"><div class="k">전략 자산 낙폭</div><div class="v blue">{pct(cur['dd'])}</div><div class="d">현금비중 {cur['cash_ratio']*100:.0f}% · 평가손익 {pct(cur['pnl'])}</div></div>
<div class="tile"><div class="k">매도 없는 날</div><div class="v orange">{drought_cur}거래일</div><div class="d">마지막 매도 {last_sell_date.date()} · 역대 {drought_rank}위</div></div></div>
<p><b>길이로는 전례가 없습니다.</b> 2010년 이후 SOXL이 {cur_len}일 연속 떨어진 적은 지금이 처음이고, 8일도 없었습니다(7일 {len_counts[7]}번, 6일 {len_counts[6]}번).
그래서 '길이'로는 비교할 과거가 없고, <b>'깊이'</b>로 봐야 합니다. {LOOK}거래일에 {DEPTH_TH*100:.0f}% 이상 빠진 급락은 <b>{n_sim}번</b> 있었고,
그중 지금처럼 −40%를 넘긴 것은 {n_le40}번입니다. {cur_in_txt}</p>
<figure><img src="data:image/png;base64,{img_len}"><figcaption>연속 하락 길이별 에피소드 수 — 오른쪽 끝 9일이 지금</figcaption></figure>

<h2><span class="no">2</span>왜 매도 기회가 안 오나 — 문턱이 같이 올라간다</h2>
<p>표준편차매매의 매도 LOC는 <b>전일 종가 × (1 + σ × 0.45)</b>입니다. σ는 최근 2일 수익률의 표준편차라서 급락이 이어지면 σ가 커지고,
<b>매도에 필요한 반등 폭도 같이 커집니다.</b> 떨어지는 날엔 당연히 못 팔고, 반등하는 날에도 σ×0.45만큼 못 오르면 못 팝니다.
전략이 고장 난 게 아니라, 큰 반등일에만 팔도록 설계된 것이 지금 체감되는 것입니다.</p>
<div class="tiles">
<div class="tile"><div class="k">오늘 σ</div><div class="v">{sig_now*100:.2f}%</div><div class="d">2일 수익률 표준편차</div></div>
<div class="tile"><div class="k">매도에 필요한 상승</div><div class="v orange">{need_up*100:+.2f}%</div><div class="d">매도 LOC ${cur['sell_loc']:,.2f}</div></div>
<div class="tile"><div class="k">매수 LOC</div><div class="v">${cur['buy_loc']:,.2f}</div><div class="d">현금 {cur['cash_ratio']*100:.0f}% — 추가 매수 여력</div></div>
<div class="tile"><div class="k">보유 평단</div><div class="v">${cur['avg']:,.2f}</div><div class="d">종가 ${cur['close']:,.2f} ({pct(cur['pnl'])})</div></div></div>
<figure><img src="data:image/png;base64,{img_recent}"><figcaption>최근 60거래일. 파란 점선(매도 문턱)이 종가 위에 떠서 내려오지 않는 구간이 지금입니다</figcaption></figure>
<div class="note">과거 {n_sim}번의 급락에서 <b>첫 매도가 체결된 날의 등락 중앙값은 {pct(med(first_sell_days))}</b>였습니다.
문턱을 넘는 날은 '조금 오른 날'이 아니라 <b>큰 반등일</b>이고, 그런 날은 진입 후 중앙값 <b>{med(done['매도까지_일']):.0f}거래일</b> 안에 왔습니다 (3절).</div>

<h2><span class="no">3</span>과거 {n_sim}번의 급락은 어떻게 끝났나</h2>
<div class="tiles">
<div class="tile"><div class="k">첫 매도까지 (중앙값)</div><div class="v blue">{med(done['매도까지_일']):.0f}거래일</div><div class="d">최대 {done['매도까지_일'].max():.0f}일</div></div>
<div class="tile"><div class="k">매도 전 추가 하락 (중앙값)</div><div class="v red">{pct(med(done['매도전_추가하락']))}</div><div class="d">최악 {pct(done['매도전_추가하락'].min())}</div></div>
<div class="tile"><div class="k">주가 60일 후 상승 비율</div><div class="v">{share_pos(done['주가_60일'])*100:.0f}%</div><div class="d">중앙값 {pct(med(done['주가_60일']))}</div></div>
<div class="tile"><div class="k">자산 60일 후 상승 비율</div><div class="v aqua">{share_pos(done['자산_60일'])*100:.0f}%</div><div class="d">중앙값 {pct(med(done['자산_60일']))}</div></div></div>
<div class="warn"><b>단, 이 숫자를 그대로 믿으면 안 되는 이유.</b> 과거 {n_sim}번은 {LOOK}일 창 안에 <b>상승일이 {ups_min_past}~{int(done['상승일'].max())}일</b> 섞여 있었습니다
(사례 표의 '상승일' 열). 그 반등일에 문턱을 넘어 1~2일 만에 팔린 것입니다. 지금은 <b>상승일 {cur_ups}/{LOOK}</b> — 단 하루도 오르지 않은 모양은
과거에 없었습니다. 그래서 "과거엔 1~2일 만에 팔렸다"는 <b>'반등일이 한 번은 온다'</b>는 전제 위의 통계이고,
현재 사례는 −{abs(DEPTH_TH)*100:.0f}% 진입일({entry_cur.date() if entry_cur is not None else '-'}) 기준 경과 {days_since_entry}거래일이라 '늦은 것'이 아니라 '아직'입니다.</div>
<figure><img src="data:image/png;base64,{img_sell}"><figcaption>진입 후 첫 매도까지 걸린 거래일</figcaption></figure>
<h3>매도 없이 버틴 기간 — 역대 기록과 비교</h3>
<p>'보유 중인데 매도가 한 번도 없는 연속 거래일'로 보면, 지금의 {drought_cur}거래일은 역대 {drought_rank}위입니다.
길이만 보면 전례가 있지만, <b>그 사이 낙폭이 {pct(cur_drop, 0)}인 것은 처음</b>입니다 (종전 기록들은 −4~−18%).</p>
<table><tr><th>구간</th><th class="r">거래일</th><th class="r">구간 주가</th><th class="r">구간 최저</th></tr>{drought_rows}</table>
<figure><img src="data:image/png;base64,{img_fwd}"><figcaption>진입일의 낙폭이 깊을수록 60일 뒤가 어땠나 — 회색 주가, 파랑 전략 자산</figcaption></figure>

<h3>자산은 돌아왔나</h3>
<div class="{'good' if n_recov == n_sim else 'note'}">완결된 {n_sim}번 중 <b>{n_recov}번</b>이 자산 전고점을 회복했습니다
(중앙값 <b>{med(done['자산회복_일']):.0f}거래일</b>, 최장 {done['자산회복_일'].max():.0f}일).
회복 전 자산이 진입일보다 더 빠진 폭은 중앙값 {pct(med(done['자산_추가최저']))}, 최악 {pct(done['자산_추가최저'].min())} —
같은 기간 주가는 중앙값 {pct(med(done['주가_추가최저']))}, 최악 {pct(done['주가_추가최저'].min())} 더 빠졌습니다.</div>
<p>주가가 급락 전 가격으로 돌아온 날과 자산이 전고점으로 돌아온 날을 둘 다 아는 {both}번 중 <b>{beat_price}번은 자산이 주가보다 먼저</b> 돌아왔습니다.
떨어지며 사 모은 물량이 반등 초입에 평단 위로 올라오기 때문입니다.</p>
<figure><img src="data:image/png;base64,{img_worst}"><figcaption>진입 후 자산이 가장 많이 더 빠졌던 5개 사례 — 진입일=100, 이후 120거래일 (회색 주가, 파랑 전략)</figcaption></figure>

<h3>사례 전체 (최근순)</h3>
<div class="scroll">{case_html}</div>
<p class="meta">첫 매도까지 = 진입일 다음 거래일부터 첫 매도 체결까지 · 자산/주가 회복 = 전고점 / 급락 전 가격 회복까지 거래일 · 60일 = 진입일 대비 60거래일 후 · '-' = 아직 미달성</p>

<h2><span class="no">4</span>"그럼 매도 문턱을 낮추면?" — 참고로 돌려본 것</h2>
<table><tr><th class="c">k_sell</th><th class="r">CAGR</th><th class="r">MDD</th><th class="r">Calmar</th><th class="r">매도 횟수</th><th class="r">평균 매도 손익률</th></tr>{sens_rows}</table>
<div class="warn">문턱을 낮추면 자주 팔리지만 평균 매도 손익률이 낮아지고 장기 성과도 대체로 밀립니다. 이 표는 <b>파라미터를 바꾸라는 뜻이 아니라</b>
"안 팔리는 답답함"이 설계된 대기임을 보여주기 위한 참고입니다. 변경은 백테스트 탭에서 전체 기간으로 검증한 뒤 결정하세요.</div>

<h2><span class="no">5</span>정리</h2>
<ul>
<li><b>길이로는 사상 처음, 깊이로는 {n_sim}번째.</b> {LOOK}일 {DEPTH_TH*100:.0f}% 이하 급락은 과거 {n_sim}번 있었고 모두 끝났습니다. −40% 이상은 {n_le40}번.</li>
<li><b>매도가 안 오는 건 설계대로.</b> σ가 커지면 문턱도 커지고, 과거의 첫 매도는 중앙값 {pct(med(first_sell_days))}짜리 반등일에 왔습니다 — 단 과거엔 그런 반등일이 {LOOK}일 안에 최소 {ups_min_past}번은 있었고, 지금은 {cur_ups}번입니다. 하루라도 반등이 와야 비교가 성립합니다.</li>
<li><b>매도 공백 {drought_cur}거래일은 역대 {drought_rank}위.</b> 길이는 전례가 있지만 그 사이 낙폭 {pct(cur_drop, 0)}는 처음입니다.</li>
<li><b>현금은 없습니다 (시뮬 기준 {cur['cash_ratio']*100:.0f}%).</b> 3분할이 하락 초입 3일에 모두 소진되어 지금은 반등 대기만 남은 상태입니다. 과거 사례의 매도 전 추가 하락은 중앙값 {pct(med(done['매도전_추가하락']))}, 최악 {pct(done['매도전_추가하락'].min())}였습니다. (실제 계좌 상태는 원장 기준으로 확인)</li>
<li><b>자산은 주가보다 먼저 돌아오는 경향</b> ({both}번 중 {beat_price}번). 전고점 회복 중앙값 {med(done['자산회복_일']):.0f}거래일, 60일 후 자산 상승 비율 {share_pos(done['자산_60일'])*100:.0f}%.</li>
</ul>
<div class="foot">생성 {datetime.now().strftime('%Y-%m-%d %H:%M')} · 재현: reports/stdev_streak.py · 사례 원자료: stdev_streak_episodes.csv</div>
</div></body></html>"""

out = os.path.join(SP, f"{TODAY.strftime('%y%m%d')}_표준편차_연속하락_분석.html")
with open(out, "w", encoding="utf-8") as f:
    f.write(html)
print("저장:", out)
print("\n=== 요약 ===")
print(f"현재 {cur_len}일 연속 / {LOOK}일 {pct(cur_drop)} / 고점대비 {pct(cur_dd_price)} / 자산DD {pct(cur['dd'])} / 현금 {cur['cash_ratio']*100:.0f}% / σ {sig_now*100:.2f}% / 매도필요 {need_up*100:+.2f}% / 평단 {cur['avg']:.2f} 손익 {pct(cur['pnl'])}")
print(f"유사사례 {n_sim}건(−40%이하 {n_le40}): 첫매도 중앙값 {med(done['매도까지_일']):.0f}일(최대 {done['매도까지_일'].max():.0f}) / 매도일등락 중앙값 {pct(med(first_sell_days))} / 매도전 추가하락 중앙값 {pct(med(done['매도전_추가하락']))} 최악 {pct(done['매도전_추가하락'].min())}")
print(f"자산 60일 상승비율 {share_pos(done['자산_60일'])*100:.0f}% 중앙값 {pct(med(done['자산_60일']))} / 주가 60일 {share_pos(done['주가_60일'])*100:.0f}% {pct(med(done['주가_60일']))}")
print(f"자산 전고점 회복 {n_recov}/{n_sim} 중앙값 {med(done['자산회복_일']):.0f}일 최장 {done['자산회복_일'].max():.0f} / 자산이 주가보다 먼저 {beat_price}/{both} / 자산 추가최저 중앙값 {pct(med(done['자산_추가최저']))} 최악 {pct(done['자산_추가최저'].min())}")
print(SENS.to_string(index=False))
print(done.sort_values("진입일", ascending=False)[["진입일","연속하락","낙폭9일","자산DD","매도까지_일","매도일_등락","매도전_추가하락","자산_추가최저","자산회복_일","자산_60일"]].to_string(index=False))
