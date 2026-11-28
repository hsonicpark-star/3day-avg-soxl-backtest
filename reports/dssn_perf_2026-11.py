"""
성과 분석 장표 (2026-11-28) — 실험용 시뮬레이터(dssn_sim) 결과를 한 장짜리 HTML 로

  perf_공격형V.html          공격형V (현재 배포, 배포 엔진과 차이 0 인 복사본)
  perf_정규화_2025맞춤3.html  정규화 · 2025년까지만 맞춤 ③ (dssn_opt_2025_params.json 의 N-2025#3)

2011-01-03 부터 이어서 운용 · 초기 자산 $10,000 · 종가 기준 · 수수료 0.04% + SEC 0.00278%
연 수익률은 전년 말 대비, CAGR 은 엔진과 같은 정의(거래일 ÷ 252).
"""
import os, sys, json, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "reports"))
import numpy as np
import pandas as pd
import dssn_sim as S

T0, CAP = "2011-01-03", 10000.0
R = os.path.join(HERE, "reports")


def money(v):
    return f"${v:,.0f}"


def stats(ctx, P):
    idx = ctx["idx"]
    i0, i1 = int(idx.searchsorted(pd.Timestamp(T0))), len(idx) - 1
    log = dict(trades=[], npos=[], buys=0, fees=0.0)
    g, mdd, n, eqa = S.sim(ctx, P, i0, i1, curve=True, log=log)
    dates = idx[i0:]
    eq = pd.Series(eqa, index=dates)
    ret = eq.pct_change(); ret.iloc[0] = eq.iloc[0] / CAP - 1
    peak = np.maximum(eq.cummax(), CAP)
    dd = eq / peak - 1
    uw = np.zeros(n, dtype=int)
    for k in range(n):
        uw[k] = 0 if dd.iloc[k] >= 0 else (uw[k - 1] + 1 if k else 1)
    npos = np.array(log["npos"])
    cagr = g ** (252.0 / n) - 1
    dsd = np.sqrt((np.minimum(ret, 0) ** 2).mean()) * np.sqrt(252)
    ml = eq.groupby(eq.index.to_period("M")).last()
    mret = ml.pct_change(); mret.iloc[0] = ml.iloc[0] / CAP - 1
    yl = eq.groupby(eq.index.year).last()
    yret = yl.pct_change(); yret.iloc[0] = yl.iloc[0] / CAP - 1
    tr = pd.DataFrame(log["trades"], columns=["b", "s", "q", "cost", "net"])
    tr["pnl"] = tr["net"] - tr["cost"]; tr["r"] = tr["net"] / tr["cost"] - 1
    tr["days"] = [(idx[s] - idx[b]).days for b, s in zip(tr["b"], tr["s"])]
    win, los = tr[tr["pnl"] > 0], tr[tr["pnl"] <= 0]
    ipk = int(np.argmax(eqa)); itr = int(np.argmin(dd.values))
    ipk_mdd = int(np.argmax(eqa[:itr + 1]))
    last_c = float(ctx["closes"][i1])
    yrows = []
    for y in yl.index:
        m = ret[ret.index.year == y]; d = dd[dd.index.year == y]; mm = mret[mret.index.year == y]
        sd = np.sqrt((np.minimum(m, 0) ** 2).mean()) * np.sqrt(252)
        yrows.append(dict(y=int(y), ret=float(yret[y]) * 100, mdd=float(d.min()) * 100, add=float(d.mean()) * 100,
                          sharpe=float(m.mean() / m.std() * np.sqrt(252)) if m.std() else 0.0, sortino=float(m.mean() * 252 / sd) if sd else 0.0,
                          dwin=float((m > 0).mean()) * 100, mwin=f"{int((mm > 0).sum())}/{len(mm)}", end=float(yl[y]), part=len(mm) < 12))
    mtab = {}
    for per, v in mret.items():
        mtab.setdefault(per.year, {})[per.month] = float(v) * 100
    return dict(dates=[str(d.date()) for d in dates], eq=[round(float(x), 2) for x in eqa], dd=[round(float(x) * 100, 2) for x in dd.values],
                uw=[int(x) for x in uw], n=n, g=g, cagr=cagr * 100, mdd=mdd * 100, calmar=cagr / abs(mdd),
                sharpe=float(ret.mean() / ret.std() * np.sqrt(252)), sortino=float(ret.mean() * 252 / dsd), dsd=dsd * 100,
                add=float(dd.mean()) * 100, uwmax=int(uw.max()), uwmax_i=int(np.argmax(uw)), uwnow=int(uw[-1]),
                mavg=float(mret.mean()) * 100, mwin=int((mret > 0).sum()), mn=len(mret), mwavg=float(mret[mret > 0].mean()) * 100, mlavg=float(mret[mret <= 0].mean()) * 100,
                mbest=(str(mret.idxmax()), float(mret.max()) * 100), mworst=(str(mret.idxmin()), float(mret.min()) * 100),
                dwin=int((ret > 0).sum()), uwdays=int((dd < 0).sum()), hold=int((npos > 0).sum()),
                pf=float(win["pnl"].sum() / -los["pnl"].sum()), payoff=float(win["r"].mean() / -los["r"].mean()),
                wavg=float(win["r"].mean()) * 100, lavg=float(los["r"].mean()) * 100, twin=float(len(win) / len(tr)) * 100, tdays=float(tr["days"].mean()),
                buys=log["buys"], sells=len(tr), fees=log["fees"], yrows=yrows, mtab=mtab,
                pk=(str(dates[ipk].date()), float(eqa[ipk])), mddpk=(str(dates[ipk_mdd].date()), float(eqa[ipk_mdd])), tr=(str(dates[itr].date()), float(eqa[itr])),
                ddnow=float(dd.iloc[-1]) * 100, stock=float(eqa[-1]) * 0 + 0, end=str(dates[-1].date()), last=float(eqa[-1]),
                mddi=itr, pki=ipk)


def heat(v, lim=20.0):
    if v is None:
        return ""
    a = min(abs(v) / lim, 1.0) * 0.55 + 0.08
    return f"background:rgba({'31,138,76' if v > 0 else '194,55,47'},{a:.2f})" if abs(v) >= 0.05 else ""


def knobs_html(ctx, P):
    """정규화 손잡이 11개 설명 (값 · 식 · 변동성 수준별 실제 값) — 다른 사람이 따라 계산할 수 있게"""
    P = dict(S.BASE, **P)
    idx = ctx["idx"]
    i0 = int(idx.searchsorted(pd.Timestamp(T0)))
    win = int(P["win"])
    v = ctx["vr"][win][i0:]
    qs = [float(np.percentile(v, q)) for q in (10, 50, 90)]
    vn = float(v[-1])
    cols = [("잔잔한 날", qs[0]), ("보통", qs[1]), ("출렁이는 날", qs[2])]
    th = "".join(f"<th>{n}<br><small>v = {x:.2f}</small></th>" for n, x in cols)
    f = lambda a, x, lo, hi: min(max(x ** a, lo), hi)
    pct = lambda base, a, x: f"{base * f(a, x, 0.25, 4.0) * 100:+.2f}%"
    day = lambda base, a, x: f"{int(min(max(round(base * f(a, x, 0.1, 10.0)), 1), 90))}일"
    rows = [
        ("안전", "매수 조건", "a_buy_sf", P["sf_buy"], P["a_buy_sf"], pct, "출렁일수록 매수 주문가를 전일 종가보다 더 높게 (더 잘 체결됨)"),
        ("공세", "매수 조건", "a_buy_ag", P["ag_buy"], P["a_buy_ag"], pct, "같은 방향, 더 강하게"),
        ("안전", "매도 조건", "a_sell_sf", P["sf_sell"], P["a_sell_sf"], pct, "출렁일수록 익절 폭을 좁게 (빨리 팔고 나옴)"),
        ("공세", "매도 조건", "a_sell_ag", P["ag_sell"], P["a_sell_ag"], pct, "거의 그대로 (조금 좁아짐)"),
        ("안전", "최대 보유일", "a_hold_sf", P["sf_hold"], P["a_hold_sf"], day, "출렁일수록 더 오래 들고 기다림"),
        ("공세", "최대 보유일", "a_hold_ag", P["ag_hold"], P["a_hold_ag"], day, "거의 그대로 (조금 짧아짐) · 위험 달에는 3일로 고정"),
    ]
    rb = ""
    for mode, what, key, base, a, fmt, memo in rows:
        b = f"{base * 100:+.2f}%" if fmt is pct else f"{int(base)}일"
        rb += (f"<tr><td class='k'>{mode} · {what}</td><td class='k'><code>{key}</code></td><td>{a:+.2f}</td><td>{b}</td>"
               + "".join(f"<td>{fmt(base, a, x)}</td>" for _, x in cols) + f"<td class='l'>{memo}</td></tr>")

    def shares(s0, sv, x, div):
        sl = s0 + sv * np.log(x)
        w = [max(1.0 + sl * (2.0 * k / (div - 1) - 1.0), 0.05) for k in range(div)]
        return sl, [q / sum(w) * 100 for q in w]

    div = int(P["sf_div"])
    tb = ""
    for mode, k0, kv in (("안전", "s_sf", "sv_sf"), ("공세", "s_ag", "sv_ag")):
        for n, x in cols:
            sl, sh = shares(P[k0], P[kv], x, div)
            tb += (f"<tr><td class='k'>{mode} · {n} <small>(v {x:.2f})</small></td><td>{sl:+.2f}</td>"
                   + "".join(f"<td>{q:.1f}%</td>" for q in sh) + "</tr>")
    base = [("분할 수 (안전 · 공세 공통)", f"{div}분할"), ("안전 매수 / 매도 / 보유", f"+{P['sf_buy'] * 100:.2f}% / +{P['sf_sell'] * 100:.2f}% / {int(P['sf_hold'])}일"),
            ("공세 매수 / 매도 / 보유", f"+{P['ag_buy'] * 100:.2f}% / +{P['ag_sell'] * 100:.2f}% / {int(P['ag_hold'])}일"),
            ("투자금 갱신 주기", f"{int(P['renew'])}거래일"), ("이익복리율 / 손실복리율", f"{P['pcr'] * 100:.0f}% / {P['lcr'] * 100:.0f}%"),
            ("위험 달 (변동성 레짐 V)", f"{int(P['risk_div'])}분할 · 공세 보유 {int(P['risk_ag_hold'])}일")]
    bb = "".join(f"<tr><td class='k'>{a}</td><td>{b}</td></tr>" for a, b in base)
    return f"""
<h2 id="knobs">설정값 — 정규화 손잡이 11개</h2>
<p class="sum">DSS(동파법)의 매수 조건 · 매도 조건 · 보유일 · 티어 비중을 고정값으로 두지 않고, <b>그날의 변동성에 맞춰 매일 다시 계산</b>하는 방식입니다.
 변동성을 재는 창 1개 + 조건을 늘리고 줄이는 지수 6개 + 티어 비중 기울기 4개 = <b>11개</b>이고, 여기에 기본값 12개가 더해져 모두 23개를 2011~2025년에 맞췄습니다.</p>

<h3>① 변동성 비율 v <small>손잡이 1개 — <code>win</code> = {win}일</small></h3>
<p class="sum"><b>v = (전일까지 {win}거래일의 일간 수익률 표준편차) ÷ {S.REF * 100:.1f}%</b>. 당일 종가는 쓰지 않습니다(전일까지만).
 v = 1 이면 SOXL 이 하루 {S.REF * 100:.1f}%쯤 출렁이는 상태, 2 면 그 두 배입니다.
 2011년 이후 v 는 하위 10% <b>{qs[0]:.2f}</b> · 중앙값 <b>{qs[1]:.2f}</b> · 상위 10% <b>{qs[2]:.2f}</b> (최소 {v.min():.2f} · 최대 {v.max():.2f}) 이고, 기말 값은 <b>{vn:.2f}</b> 입니다.
 창은 10 · 20 · 40 · 60일 중에서 60일이 골라졌습니다.</p>

<h3>② 조건을 늘리고 줄이는 지수 <small>손잡이 6개 — 실제 값 = 기본값 × v<sup>지수</sup></small></h3>
<p class="sum">지수가 <b>+</b> 면 출렁일수록 값이 커지고, <b>−</b> 면 작아지고, 0 이면 고정입니다. 매수 · 매도 조건은 기본값의 0.25~4배, 보유일은 1~90일 안에서만 움직입니다.</p>
<div class="tw"><table class="yt"><thead><tr><th>대상</th><th>손잡이</th><th>지수</th><th>기본값<br><small>v = 1</small></th>{th}<th class="l">뜻</th></tr></thead><tbody>{rb}</tbody></table></div>
<p class="cap">매수 주문가 = 전일 종가 × (1 + 매수 조건) 을 센트 아래 버림, 종가가 그 이하면 종가에 체결 · 매도 목표가 = 매수한 종가 × (1 + 매도 조건) · 조건은 매수하는 날의 v 로 정해져 그 칸에 고정됩니다.</p>

<h3>③ 티어 비중 기울기 <small>손잡이 4개 — 기울기 s = s0 + sv × ln(v)</small></h3>
<p class="sum">칸마다 돈을 똑같이 나누지 않고 <b>비중 = 1 + s × (2u − 1)</b>, u = (티어 − 1) ÷ (분할 수 − 1) 로 기울입니다(최소 0.05). s 가 + 면 뒤 티어(더 내려갔을 때 사는 칸)에 돈을 더 싣고, − 면 앞 티어에 더 싣습니다.
 s0 는 평소 기울기, sv 는 변동성에 따라 기울기가 바뀌는 정도입니다. 티어는 '장 시작 때 보유 칸 수 + 1' 이고, 1회 매수 금액 = 투자금 × 그 티어 비중 ÷ 비중 합계입니다.</p>
<div class="tw"><table class="yt"><thead><tr><th>손잡이</th><th>안전</th><th>공세</th><th class="l">뜻</th></tr></thead><tbody>
<tr><td class="k">평소 기울기 <code>s_sf</code> · <code>s_ag</code></td><td>{P['s_sf']:+.2f}</td><td>{P['s_ag']:+.2f}</td><td class="l">두 모드 모두 뒤 티어를 더 크게</td></tr>
<tr><td class="k">변동성 반응 <code>sv_sf</code> · <code>sv_ag</code></td><td>{P['sv_sf']:+.2f}</td><td>{P['sv_ag']:+.2f}</td><td class="l">안전은 출렁일수록 뒤 티어 쏠림이 커지고, 공세는 반대로 평평해짐</td></tr>
</tbody></table></div>
<div class="tw"><table class="yt"><thead><tr><th>{div}분할일 때 티어별 비중</th><th>기울기 s</th>{''.join(f'<th>{k + 1}티어</th>' for k in range(div))}</tr></thead><tbody>{tb}</tbody></table></div>
<p class="cap">균등 분할이면 칸마다 {100 / div:.1f}% · 위험 달의 12분할에도 같은 식을 씁니다(u 가 0~1 이라 분할 수가 달라도 모양이 같음).</p>

<h3>함께 맞춘 기본값 12개</h3>
<div class="tw"><table class="yt" style="max-width:560px"><tbody>{bb}</tbody></table></div>
<p class="cap">모드(안전 · 공세) 판정과 위험 달 판정은 공격형V 와 같습니다 — 정규화는 조건 · 보유일 · 수량에만 쓰고, 모드 판정에는 쓰지 않았습니다.
 탐색 범위: 지수 −1.0~+1.5 (보유일은 −1.5~+1.0) · 기울기 ±0.9 · 변동성 반응 ±0.8 · 목적은 CAGR − 3 × (MDD 가 42% 를 넘는 만큼).</p>
"""


def build(name, sub, note, s, out, extra=""):
    kp = lambda lab, val, cap, cls="": f"<div class='kp'><span>{lab}</span><b class='{cls}'>{val}</b><small>{cap}</small></div>"
    row = lambda title, cells: f"<div class='band'>{title}</div><div class='kps'>{''.join(cells)}</div>"
    k = "".join([
        row("대표 지표", [kp("기간 수익률", f"{(s['g'] - 1) * 100:+,.1f}%", "수수료 차감 후 누적"), kp("CAGR", f"{s['cagr']:+.2f}%", "거래일 ÷ 252 기준 연복리"),
                      kp("Calmar", f"{s['calmar']:.2f}", "최대낙폭 대비 수익"), kp("Sharpe", f"{s['sharpe']:.2f}", "전체 일수익 변동 기준"),
                      kp("Sortino", f"{s['sortino']:.2f}", f"목표 0% · 연 하방편차 {s['dsd']:.1f}%")]),
        row("낙폭과 회복", [kp("최대낙폭 · MDD", f"{s['mdd']:.2f}%", f"{s['mddpk'][0]} 고점 → {s['tr'][0]} 저점", "neg"), kp("평균 DD", f"{s['add']:.2f}%", "모든 거래일 종가 낙폭 평균", "neg"),
                        kp("기말 낙폭", f"{s['ddnow']:.2f}%", "최근 고점 대비 현재", "neg" if s['ddnow'] < 0 else ""),
                        kp("최장 미회복", f"{s['uwmax']}거래일", "고점 아래에 연속으로 머문 날"), kp("기말 미회복", f"{s['uwnow']}거래일", "지금 이어지는 미회복 일수")]),
        row("수익 빈도와 시장 노출", [kp("월평균 수익률", f"{s['mavg']:+.2f}%", "관측 월 수익률의 산술평균"), kp("월승률", f"{s['mwin'] / s['mn'] * 100:.1f}%", f"수익 월 {s['mwin']} / {s['mn']}개월"),
                              kp("일승률", f"{s['dwin'] / s['n'] * 100:.1f}%", f"수익 일 {s['dwin']:,} / {s['n']:,}거래일"),
                              kp("언더워터 비율", f"{s['uwdays'] / s['n'] * 100:.1f}%", f"전고점 미달 {s['uwdays']:,} / {s['n']:,}거래일", "neg"),
                              kp("보유일 비율", f"{s['hold'] / s['n'] * 100:.1f}%", f"종가 보유 {s['hold']:,} / {s['n']:,}거래일")]),
        row("거래 효율", [kp("실현 수익 팩터", f"{s['pf']:.2f}", "청산한 칸 · 달러 기준"), kp("손익비", f"{s['payoff']:.2f}", f"청산한 칸 · 수익률 기준 (승률 {s['twin']:.1f}%)"),
                      kp("평균 이익 거래", f"{s['wavg']:+.2f}%", "이익 거래 수익률 평균", "pos"), kp("평균 손실 거래", f"{s['lavg']:.2f}%", "손실 거래 수익률 평균", "neg"),
                      kp("평균 보유 기간", f"{s['tdays']:.1f}일", "청산한 칸 · 달력일")])])
    # 월별 표
    ys = {r["y"]: r for r in s["yrows"]}
    mh = "<tr><th>연도</th>" + "".join(f"<th>{m}월</th>" for m in range(1, 13)) + "<th>Year</th><th>MDD</th></tr>"
    mb = ""
    for y, r in ys.items():
        mb += f"<tr><td class='k'>{y}{'*' if r['part'] else ''}</td>"
        for m in range(1, 13):
            v = s["mtab"][y].get(m)
            mb += f"<td style='{heat(v)}'>{'—' if v is None else f'{v:+.1f}'}</td>"
        mb += f"<td style='{heat(r['ret'], 150)}'><b>{r['ret']:+.1f}</b></td><td style='{heat(r['mdd'], 45)}'><b>{r['mdd']:.1f}</b></td></tr>"
    mavg = [np.mean([s["mtab"][y][m] for y in ys if m in s["mtab"][y]]) for m in range(1, 13)]
    mb += "<tr class='avg'><td class='k'>월평균</td>" + "".join(f"<td style='{heat(v)}'>{v:+.1f}</td>" for v in mavg) + "<td></td><td></td></tr>"
    full = [r for r in s["yrows"] if not r["part"]]
    best, worst = max(full, key=lambda r: r["ret"]), min(full, key=lambda r: r["ret"])
    yb = "".join(f"<tr><td class='k'>{r['y']}{'*' if r['part'] else ''}</td><td class='{'neg' if r['ret'] < 0 else 'pos'}'><b>{r['ret']:+.2f}%</b></td><td class='neg'>{r['mdd']:.2f}%</td>"
                 f"<td>{r['add']:.2f}%</td><td>{r['sharpe']:.2f}</td><td>{r['sortino']:.2f}</td><td>{r['dwin']:.1f}%</td><td>{r['mwin']}</td><td>{money(r['end'])}</td></tr>" for r in s["yrows"])
    data = dict(dates=s["dates"], eq=s["eq"], dd=s["dd"], uw=s["uw"], mddi=s["mddi"], pki=s["pki"], uwi=s["uwmax_i"])
    html = f"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{name} 성과 분석</title>
<style>
:root{{--bg:#fbfaf7;--card:#fff;--ink:#1d1f23;--ink2:#555b66;--mut:#8a909b;--line:#e4e1da;--acc:#2f3e8f;--area:rgba(47,62,143,.10);--bad:#c2372f;--badarea:rgba(194,55,47,.14);--good:#1f8a4c;--band:#f1efe9}}
@media (prefers-color-scheme:dark){{:root{{--bg:#15171a;--card:#1d2024;--ink:#e8e9ec;--ink2:#b3b8c2;--mut:#7f8691;--line:#30343a;--acc:#8fa0f5;--area:rgba(143,160,245,.14);--bad:#ef6b62;--badarea:rgba(239,107,98,.18);--good:#4cc27e;--band:#24282d}}}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:15px/1.6 "Pretendard","Malgun Gothic",system-ui,sans-serif}}
main{{max-width:1040px;margin:0 auto;padding:28px 16px 70px}}
header{{display:flex;justify-content:space-between;align-items:flex-end;gap:12px;flex-wrap:wrap;border-bottom:2px solid var(--ink);padding-bottom:8px}}
h1{{font-size:30px;margin:0;line-height:1.2}}header span{{color:var(--acc);font-weight:700}}.meta{{color:var(--ink2);font-size:13px;margin:6px 0 0}}
h2{{font-size:19px;margin:34px 0 8px}}h3{{font-size:14.5px;margin:20px 0 0;color:var(--acc)}}h3.r{{color:var(--bad)}}h3 small{{color:var(--ink2);font-weight:400;margin-left:10px}}
.hd{{display:flex;justify-content:space-between;align-items:baseline;gap:10px;flex-wrap:wrap}}.hd b{{font-size:14.5px;color:var(--acc)}}.hd b.r{{color:var(--bad)}}
.cap{{color:var(--mut);font-size:12px;margin:0}}.note{{background:var(--card);border:1px solid var(--line);border-left:5px solid var(--acc);border-radius:10px;padding:12px 16px;margin:14px 0}}
.band{{background:var(--band);font-weight:700;font-size:13px;padding:5px 12px;margin-top:10px}}
.kps{{display:grid;grid-template-columns:repeat(5,1fr);gap:0 10px;padding:8px 12px 4px}}@media (max-width:720px){{.kps{{grid-template-columns:repeat(2,1fr);gap:8px 10px}}}}
.kp span{{display:block;font-size:12px;color:var(--ink2)}}.kp b{{display:block;font-size:21px;line-height:1.25;font-variant-numeric:tabular-nums}}.kp small{{display:block;font-size:11px;color:var(--mut);line-height:1.35}}
.neg{{color:var(--bad)}}.pos{{color:var(--good)}}
.tw{{overflow-x:auto}}table{{border-collapse:separate;border-spacing:2px;width:100%;font-size:12.5px}}th,td{{padding:4px 6px;text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}}
th{{color:var(--ink2);font-weight:600;font-size:12px;border-bottom:1px solid var(--line)}}td.k,th:first-child{{text-align:left}}table.hm td{{text-align:center;border-radius:3px}}table.hm td.k{{text-align:left}}
tr.avg td{{border-top:1px solid var(--line);font-weight:700}}table.yt{{border-spacing:0}}table.yt td{{border-bottom:1px solid var(--line);padding:6px 8px}}
.chart{{position:relative}}.chart svg{{display:block;width:100%;height:auto}}
.tip{{position:absolute;pointer-events:none;background:var(--card);border:1px solid var(--line);border-radius:8px;padding:5px 9px;font-size:12.5px;line-height:1.5;box-shadow:0 4px 14px rgba(0,0,0,.15);display:none;white-space:nowrap}}
.sum{{font-size:13.5px;color:var(--ink2)}}.sum b{{color:var(--ink)}}
td.l,th.l{{text-align:left;white-space:normal;color:var(--ink2)}}th small,td small{{font-weight:400;color:var(--mut)}}code{{font:12px ui-monospace,Consolas,monospace;background:var(--band);padding:1px 5px;border-radius:4px}}
</style></head><body><main>
<header><h1>SOXL · {name}</h1><span>전체 기간 백테스트 성과</span></header>
<p class="meta">{T0} — {s['end']} · {s['n']:,}거래일 · 초기 자산 {money(CAP)} · 2011년부터 이어서 운용 · 종가 기준 · 수수료 0.04% + SEC 0.00278%</p>
<p class="meta">{sub}</p>
{note}

<h2>자산과 고점 회복</h2>
<div class="hd"><h3>자산 곡선<small>계좌의 종가 평가액 · 로그 눈금 (한 칸 = 10배)</small></h3><b>시작 {money(CAP)} → 기말 {money(s['last'])}</b></div>
<div class="chart" id="c_eq"></div>
<div class="hd"><h3 class="r">고점 대비 낙폭<small>0% 는 종가 고점에 도달한 날</small></h3><b class="r">최대낙폭 {s['mdd']:.2f}% · {s['tr'][0]}</b></div>
<div class="chart" id="c_dd"></div>
<div class="hd"><h3>고점 미회복 기간<small>고점 아래에서 하루가 지나면 1씩 늘고, 고점을 넘으면 0</small></h3><b>최장 {s['uwmax']}거래일 · 기말 {s['uwnow']}거래일</b></div>
<div class="chart" id="c_uw"></div>
{k}

<h2>월별 수익률</h2>
<p class="sum">월승률 <b>{s['mwin'] / s['mn'] * 100:.1f}% ({s['mwin']}/{s['mn']})</b> · 월평균 <b>{s['mavg']:+.2f}%</b> · 이익 월 평균 <b class="pos">{s['mwavg']:+.2f}%</b> · 손실 월 평균 <b class="neg">{s['mlavg']:.2f}%</b>
 · 최고 월 <b class="pos">{s['mbest'][0]} {s['mbest'][1]:+.2f}%</b> · 최저 월 <b class="neg">{s['mworst'][0]} {s['mworst'][1]:.2f}%</b></p>
<div class="tw"><table class="hm"><thead>{mh}</thead><tbody>{mb}</tbody></table></div>
<p class="cap">단위 % · Year 는 전년 말 대비 연간 수익률, MDD 는 그해의 최대 낙폭(직전 고점 대비) · * 는 {s['end']} 까지</p>

<h2>연도별 성과와 연내 낙폭</h2>
<p class="sum">완결 연도 <b>{len(full)}개</b> · 수익 연도 <b>{sum(r['ret'] > 0 for r in full)}개</b> · 최고 <b class="pos">{best['y']}년 {best['ret']:+.2f}%</b> · 최저 <b>{worst['y']}년 {worst['ret']:+.2f}%</b>
 · 평균 연내 MDD <b class="neg">{np.mean([r['mdd'] for r in full]):.2f}%</b></p>
<div class="tw"><table class="yt"><thead><tr><th>연도</th><th>연 수익률</th><th>연내 MDD</th><th>평균 DD</th><th>Sharpe</th><th>Sortino</th><th>일승률</th><th>수익 월</th><th>기말 자산</th></tr></thead><tbody>{yb}</tbody></table></div>

<h2>최고점 이후 회복 상태</h2>
<p class="sum">최대낙폭 <b>{s['mddpk'][0]} {money(s['mddpk'][1])}</b> → <b>{s['tr'][0]} {money(s['tr'][1])}</b> ({s['mdd']:.2f}%)
 · 최근 최고점 <b>{s['pk'][0]} {money(s['pk'][1])}</b> · 기말 고점 대비 <b class="{'neg' if s['ddnow'] < 0 else ''}">{s['ddnow']:.2f}%</b> · {'미회복 (진행 중) ' + str(s['uwnow']) + '거래일' if s['uwnow'] else '고점 갱신 중'}</p>

<h2>거래 요약</h2>
<p class="sum">매수 <b>{s['buys']:,}회</b> · 매도 <b>{s['sells']:,}회</b> · 칸 승률 <b>{s['twin']:.1f}%</b> · 누적 수수료 <b>{money(s['fees'])}</b></p>
{extra}
<p class="cap">과거 종가로 계산한 백테스트입니다. 세금 · 체결 오차 · 호가 영향은 빠져 있어 실전에서 기대할 값이 아닙니다. 배포 엔진은 바꾸지 않은 실험용 시뮬레이터 결과입니다.</p>
</main>
<script>
const D = {json.dumps(data)};
function chart(id, o) {{
  const el = document.getElementById(id), W = 1000, H = o.h, L = 62, Rr = 12, T = 14, B = 24, n = D.dates.length, v = o.vals;
  const tr = x => o.log ? Math.log10(x) : x;
  let lo = Infinity, hi = -Infinity; v.forEach(x => {{ lo = Math.min(lo, tr(x)); hi = Math.max(hi, tr(x)); }});
  if (o.log) {{ lo = Math.floor(lo); hi = Math.ceil(hi); }} else if (o.neg) {{ hi = 0; lo = Math.floor(lo / o.step) * o.step; }} else {{ lo = 0; hi = Math.ceil(hi / o.step) * o.step; }}
  const x = i => L + (W - L - Rr) * i / (n - 1), y = q => T + (H - T - B) * (1 - (tr(q) - lo) / (hi - lo)), st = o.log ? 1 : o.step;
  let g = '';
  for (let k = lo; k <= hi + 1e-9; k += st) {{
    const yy = T + (H - T - B) * (1 - (k - lo) / (hi - lo));
    g += `<line x1="${{L}}" x2="${{W - Rr}}" y1="${{yy}}" y2="${{yy}}" stroke="var(--line)"/><text x="${{L - 7}}" y="${{yy + 4}}" text-anchor="end" font-size="11" fill="var(--mut)">${{o.tick(o.log ? 10 ** k : k)}}</text>`;
  }}
  let last = '';
  D.dates.forEach((d, i) => {{ const yr = d.slice(0, 4); if (yr !== last && (+yr % 2 === 1)) g += `<text x="${{x(i)}}" y="${{H - 6}}" text-anchor="middle" font-size="11" fill="var(--mut)">${{yr}}</text>`; last = yr; }});
  const base = y(o.log ? 10 ** lo : 0), pts = v.map((q, i) => x(i).toFixed(1) + ' ' + y(q).toFixed(1));
  g += `<path d="M${{x(0)}} ${{base}}L${{pts.join('L')}}L${{x(n - 1)}} ${{base}}Z" fill="${{o.area}}"/><path d="M${{pts.join('L')}}" fill="none" stroke="${{o.color}}" stroke-width="${{o.sw || 1.6}}" stroke-linejoin="round"/>`;
  (o.marks || []).forEach(m => {{ g += `<circle cx="${{x(m.i)}}" cy="${{y(v[m.i])}}" r="4" fill="${{o.color}}" stroke="var(--bg)" stroke-width="2"/><text x="${{x(m.i) + (m.i > n * .9 ? -8 : 8)}}" y="${{y(v[m.i]) + (m.dy || -8)}}" text-anchor="${{m.i > n * .9 ? 'end' : 'start'}}" font-size="11.5" font-weight="700" fill="${{o.color}}">${{m.t}}</text>`; }});
  g += `<line class="cx" x1="0" x2="0" y1="${{T}}" y2="${{H - B}}" stroke="var(--ink2)" visibility="hidden"/>`;
  el.innerHTML = `<svg viewBox="0 0 ${{W}} ${{H}}" role="img" aria-label="${{o.label}}">${{g}}</svg><div class="tip"></div>`;
  const svg = el.querySelector('svg'), cx = el.querySelector('.cx'), tip = el.querySelector('.tip');
  svg.addEventListener('mousemove', e => {{
    const r = svg.getBoundingClientRect(), px = (e.clientX - r.left) * W / r.width, i = Math.max(0, Math.min(n - 1, Math.round((px - L) / (W - L - Rr) * (n - 1))));
    cx.setAttribute('x1', x(i)); cx.setAttribute('x2', x(i)); cx.setAttribute('visibility', 'visible');
    tip.innerHTML = `<b>${{D.dates[i]}}</b><br>${{o.label}}: <b>${{o.fmt(v[i])}}</b>`; tip.style.display = 'block';
    const tx = x(i) * r.width / W; tip.style.left = (tx > r.width / 2 ? tx - tip.offsetWidth - 12 : tx + 12) + 'px'; tip.style.top = '6px';
  }});
  svg.addEventListener('mouseleave', () => {{ cx.setAttribute('visibility', 'hidden'); tip.style.display = 'none'; }});
}}
const usd = v => '$' + Math.round(v).toLocaleString();
chart('c_eq', {{vals: D.eq, h: 300, log: true, color: 'var(--acc)', area: 'var(--area)', label: '자산', tick: usd, fmt: usd, sw: 2, marks: [{{i: D.pki, t: '최고점'}}, {{i: D.eq.length - 1, t: '기말', dy: 16}}]}});
chart('c_dd', {{vals: D.dd, h: 200, neg: true, step: 10, color: 'var(--bad)', area: 'var(--badarea)', label: '낙폭', tick: v => v.toFixed(0) + '%', fmt: v => v.toFixed(2) + '%', sw: 1.2, marks: [{{i: D.mddi, t: '최대낙폭', dy: 14}}]}});
chart('c_uw', {{vals: D.uw, h: 190, step: 100, color: 'var(--acc)', area: 'var(--area)', label: '미회복', tick: v => v.toFixed(0), fmt: v => v + '거래일', sw: 1.2, marks: [{{i: D.uwi, t: '최장 구간'}}]}});
</script></body></html>"""
    with open(os.path.join(R, out), "w", encoding="utf-8") as f:
        f.write(html)
    print(out, len(html))


def main():
    ctx, df, ms = S.load_soxl()
    r25 = json.load(open(os.path.join(R, "dssn_opt_2025_params.json"), encoding="utf-8"))
    P3 = r25["N-2025#3"]
    a = stats(ctx, dict(S.BASE))
    b = stats(ctx, P3)
    for nm, s in (("공격형V", a), ("N-2025#3", b)):
        print(nm, f"CAGR {s['cagr']:.2f} MDD {s['mdd']:.2f} Calmar {s['calmar']:.2f} Sharpe {s['sharpe']:.2f} Sortino {s['sortino']:.2f} PF {s['pf']:.2f} 손익비 {s['payoff']:.2f} "
              f"칸승률 {s['twin']:.1f} 보유 {s['tdays']:.1f}일 월승률 {s['mwin']}/{s['mn']} 최장미회복 {s['uwmax']} 기말DD {s['ddnow']:.1f} 2026 {s['yrows'][-1]['ret']:.1f}/{s['yrows'][-1]['mdd']:.1f}")
    build("공격형V", "7분할 · 안전 +3.3% / +1.8% / 38일 · 공세 +3.15% / +3.75% / 8일 · 갱신 10일 · 이익복리 75% · 손실복리 20% · 변동성 레짐(V): 위험 달 12분할 · 공세 5일",
          "<div class='note'><b>현재 배포 중인 프리셋입니다.</b> 값을 정한 뒤 손대지 않은 설정이라, 전 구간이 같은 조건의 결과입니다.</div>", a, "perf_공격형V.html")
    build("정규화 · 2025년까지만 맞춤 ③", "6분할 · 안전 +5.39% / +0.55% / 24일 · 공세 +2.82% / +4.73% / 11일 · 갱신 5일 · 이익복리 100% · 손실복리 16% · V: 위험 달 12분할 · 공세 3일 · 정규화 손잡이 11개 (변동성 60일)",
          f"<div class='note'><b>읽을 때 주의:</b> 이 설정은 2011~2025년 성과가 가장 좋게 나오도록 손잡이 23개를 맞춘 값입니다. 그래서 2011~2025년 숫자는 '맞춘 결과'이고, <b>처음 보는 구간은 2026년 한 해뿐</b>입니다. "
          f"2026년은 {b['yrows'][-1]['ret']:+.1f}% / 연중 MDD {b['yrows'][-1]['mdd']:.1f}% (공격형V {a['yrows'][-1]['ret']:+.1f}% / {a['yrows'][-1]['mdd']:.1f}%)였습니다. "
          "같은 방법으로 찾은 ①·②는 2026년에 +37% · −40%로 갈렸습니다. 손잡이 설명은 <a href='#knobs'>맨 아래 '정규화 손잡이 11개'</a>에 있습니다.</div>", b, "perf_정규화_2025맞춤3.html",
          extra=knobs_html(ctx, P3))


if __name__ == "__main__":
    main()
