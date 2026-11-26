"""
공격형 vs 공격형V 월간·연간 성과 비교표 (HTML) — 2026-11-24
공격형V = 공격형 + 위험 레짐 달(월말 SOXX 20일 변동성 상승 & 3년 백분위 ≥ 70% → 다음 달)
          12분할(투자금÷12) · 공세모드 신규 매수 보유 5일
출력: reports/dss_v_monthly_compare.html
"""
import sys, os, importlib.util, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import pandas as pd

_spec = importlib.util.spec_from_file_location("vr", os.path.join(HERE, "reports", "dss_vol_regime_2026-11.py"))
VR = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(VR)
from dss_engine import (load_price_data, build_weekly_rsi_series, build_mode_series, DSSParams,
                        run_backtest, get_week_mode_map)

START = "2010-03-12"


def stats(eq):
    y = len(eq) / 252
    cg = (eq.iloc[-1] / 1e4) ** (1 / y) - 1
    md = (eq / eq.cummax() - 1).min()
    return cg, md


ANNUAL_CSS = """
:root{--s1:#2a78d6;--s2:#eb6834;--card:#f5f5f7}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--s1:#3987e5;--s2:#d95926;--card:#222226}}
.ann-sum{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:12px;margin:12px 0}
.ann-card{background:var(--card);border-radius:10px;padding:12px 14px;font-size:13px;line-height:1.9}
.ann-card h3{margin:0 0 2px;font-size:14px}
.ann-card .dot{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:6px}
.ann-card span{color:var(--mut);margin-right:4px} .ann-card b{margin-right:14px;font-variant-numeric:tabular-nums}
.chart-legend{display:flex;gap:16px;font-size:12px;color:var(--mut);margin:14px 0 2px;justify-content:space-between;flex-wrap:wrap}
.chart-legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px}
svg.ann{width:100%;height:auto;display:block}
svg.ann text{fill:var(--mut);font-size:11px;font-family:inherit}
svg.ann text.val{fill:var(--fg);font-weight:700}
svg.ann line{stroke:var(--line)} svg.ann line.zero{stroke:var(--mut)}
svg.ann rect.s1{fill:var(--s1)} svg.ann rect.s2{fill:var(--s2)}
svg.ann rect:hover{opacity:.75}
table.ann td,table.ann th{padding:7px 8px;font-size:12.5px}
table.ann th:first-child,table.ann td:first-child{text-align:left}
table.ann tr.avg td,table.ann tr.avg th{font-weight:700;border-top:2px solid var(--mut)}
table.ann caption{caption-side:top;text-align:left;font-weight:700;font-size:14px;padding:18px 0 6px}
"""


def annual_stats(bt):
    """연도별 지표 DataFrame (index=연도). 낙폭은 총자산 기준."""
    d = bt.copy()
    d["날짜"] = pd.to_datetime(d["날짜"])
    d = d.set_index("날짜")
    eq = d["총자산"].astype(float)
    dd_all = eq / eq.cummax() - 1
    cost = d["평가금"].astype(float) - d["평가손익"].astype(float)
    hold_ret = (d["평가손익"].astype(float) / cost).where(cost > 0)
    prev_pos = d["보유포지션수"].shift(1).fillna(0)
    sells = d["매도내역"].apply(lambda x: x if isinstance(x, list) else [])
    rows, prev_end = {}, 1e4
    for y, g in d.groupby(d.index.year):
        e = eq[g.index]
        x = pd.concat([pd.Series([prev_end]), e.reset_index(drop=True)])
        uw = (dd_all[g.index] < -1e-9).values
        runs, n = [], 0
        for f in uw:
            if f:
                n += 1
            elif n:
                runs.append(n); n = 0
        if n:
            runs.append(n)
        sl = [t for lst in sells[g.index] for t in lst]
        n_buy = int((g["수량"] > 0).sum())
        buy_miss = int((g["매수주문가"].notna() & ~(g["수량"] > 0)).sum())
        sell_miss = int(prev_pos[g.index].sum() - len(sl))
        hr = hold_ret[g.index]
        rows[y] = dict(수익률=e.iloc[-1] / prev_end - 1, 보유MDD=float(hr.min()) if hr.notna().any() else 0.0,
                       최대낙폭=float((x / x.cummax() - 1).min()), 평균낙폭=float(dd_all[g.index].mean()),
                       미회복=float(sum(runs) / len(runs)) if runs else 0.0, 거래수=n_buy + len(sl),
                       회전수=sum(t["qty"] * t["sell_price"] for t in sl) / e.mean(), 미체결=buy_miss + max(0, sell_miss),
                       현금비중=float((g["예수금"].astype(float) / e).mean()),
                       승률=(sum(1 for t in sl if t["pnl"] > 0) / len(sl)) if sl else float("nan"),
                       일수=len(g))
        prev_end = e.iloc[-1]
    return pd.DataFrame(rows).T


def annual_section(bts):
    st = {k: annual_stats(v) for k, v in bts.items()}
    names = list(st)
    years = [int(y) for y in st[names[0]].index]
    first, last = years[0], years[-1]
    partial = {y for y in (first, last) if st[names[0]].loc[y, "일수"] < 240}
    full = [y for y in years if y not in partial]

    def lab(y):
        if y in partial:
            return f"{y} (3월~)" if y == first else f"{y} (YTD)"
        return str(y)

    def sg(v, d=1):
        return f"{v * 100:+.{d}f}%"

    # 요약 카드
    cards = []
    for i, k in enumerate(names):
        r = st[k].loc[full, "수익률"]
        win, loss = r[r > 0], r[r <= 0]
        cards.append(
            f'<div class="ann-card"><h3><i class="dot" style="background:var(--s{i + 1})"></i>{k}</h3>'
            f'<span>연 승률</span><b>{len(win) / len(r) * 100:.1f}%</b>'
            f'<span>연평균</span><b class="{"pos" if r.mean() > 0 else "neg"}">{sg(r.mean())}</b>'
            f'<span>연평균이익</span><b class="pos">{sg(win.mean()) if len(win) else "–"}</b>'
            f'<span>연평균손실</span><b class="neg">{sg(loss.mean()) if len(loss) else "–"}</b><br>'
            f'<span>최고 연도</span><b class="pos">{sg(r.max())} · {r.idxmax()}</b>'
            f'<span>최저 연도</span><b class="{"pos" if r.min() > 0 else "neg"}">{sg(r.min())} · {r.idxmin()}</b></div>')

    # 막대 차트 (연도별 묶음 막대)
    W, H, L, R_, T, B = 1100, 300, 44, 10, 22, 30
    vals = [st[k].loc[y, "수익률"] * 100 for k in names for y in years]
    hi = max(50, (int(max(vals)) // 50 + 1) * 50)
    lo = (int(min(vals)) // 50) * 50 if min(vals) < 0 else 0

    def ys(v):
        return T + (hi - v) / (hi - lo) * (H - T - B)

    gw = (W - L - R_) / len(years)
    bw = min(22, (gw - 12) / 2)
    parts = [f'<svg class="ann" viewBox="0 0 {W} {H}" role="img" aria-label="연도별 수익률 막대 차트 — 공격형과 공격형V">']
    for t in range(int(lo), int(hi) + 1, 50):
        parts.append(f'<line class="{"zero" if t == 0 else ""}" x1="{L}" x2="{W - R_}" y1="{ys(t):.1f}" y2="{ys(t):.1f}"/>'
                     f'<text x="{L - 6}" y="{ys(t) + 4:.1f}" text-anchor="end">{t}%</text>')
    for gi, y in enumerate(years):
        cx = L + gw * (gi + 0.5)
        parts.append(f'<text x="{cx:.1f}" y="{H - 10}" text-anchor="middle">{lab(y)}</text>')
        for si, k in enumerate(names):
            v = st[k].loc[y, "수익률"] * 100
            x = cx - bw - 1 + si * (bw + 2)
            y0, y1 = ys(max(v, 0)), ys(min(v, 0))
            h = max(1.0, y1 - y0)
            parts.append(f'<rect class="s{si + 1}" x="{x:.1f}" y="{y0:.1f}" width="{bw:.1f}" height="{h:.1f}" rx="3">'
                         f'<title>{lab(y)} · {k} {v:+.1f}%</title></rect>')
            r = st[k].loc[full, "수익률"]
            if y in (r.idxmax(), r.idxmin(), last):
                ty = y0 - 5 if v >= 0 else y1 + 13
                anc, tx = ("end", x + bw / 2 + 5) if si == 0 else ("start", x + bw / 2 - 5)
                parts.append(f'<text class="val" x="{tx:.1f}" y="{ty:.1f}" text-anchor="{anc}">{v:+.0f}%</text>')
    parts.append("</svg>")
    legend = ('<div class="chart-legend"><span>'
              + "".join(f'<i style="background:var(--s{i + 1})"></i>{k} &nbsp; ' for i, k in enumerate(names))
              + '</span><span>라벨은 최고 · 최저 · 올해 · 막대에 마우스를 올리면 값 표시</span></div>')

    # 표
    def heat(v, scale, var):
        a = max(0.0, min(1.0, abs(v) / scale)) * 45
        return f' style="background:color-mix(in srgb,var(--{var}) {a:.0f}%,transparent)"' if a >= 4 else ""

    tables = []
    for k in names:
        s = st[k]
        body = []
        for y in years:
            r = s.loc[y]
            body.append(
                f"<tr><th>{lab(y)}</th>"
                f"<td{heat(r['수익률'], 1.5, 'pos' if r['수익률'] > 0 else 'neg')}>{sg(r['수익률'])}</td>"
                f"<td{heat(r['보유MDD'], 0.6, 'neg')}>{r['보유MDD'] * 100:.1f}%</td>"
                f"<td{heat(r['최대낙폭'], 0.6, 'neg')}>{r['최대낙폭'] * 100:.1f}%</td>"
                f"<td{heat(r['평균낙폭'], 0.3, 'neg')}>{r['평균낙폭'] * 100:.1f}%</td>"
                f"<td>{r['미회복']:.0f}거래일</td><td>{r['거래수']:.0f}</td><td>{r['회전수']:.1f}</td><td>{r['미체결']:.0f}</td>"
                f"<td>{r['현금비중'] * 100:.1f}%</td><td>{r['승률'] * 100:.1f}%</td></tr>")
        m = s.loc[full].mean()
        body.append(f"<tr class='avg'><th>평균</th><td>{sg(m['수익률'])}</td><td>{m['보유MDD'] * 100:.1f}%</td>"
                    f"<td>{m['최대낙폭'] * 100:.1f}%</td><td>{m['평균낙폭'] * 100:.1f}%</td><td>{m['미회복']:.1f}거래일</td>"
                    f"<td>{m['거래수']:.1f}</td><td>{m['회전수']:.1f}</td><td>{m['미체결']:.1f}</td>"
                    f"<td>{m['현금비중'] * 100:.1f}%</td><td>{m['승률'] * 100:.1f}%</td></tr>")
        tables.append(f'<div class="scroll"><table class="ann"><caption>{k}</caption><thead><tr><th>연도</th><th>수익률</th>'
                      f'<th>보유 MDD</th><th>최대 낙폭</th><th>평균 낙폭</th><th>평균 미회복 기간</th><th>거래 수</th><th>회전 수</th>'
                      f'<th>미체결 수</th><th>평균 현금 비중</th><th>승률</th></tr></thead><tbody>{"".join(body)}</tbody></table></div>')
    note = (f'<div class="sub" style="margin-top:10px">{" · ".join(lab(y) for y in sorted(partial))} 은 한 해가 다 차지 않아 요약 통계와 평균에서 뺐다.<br>'
            '수익률 = 전년 말 총자산 대비 · 보유 MDD = 보유 주식의 평가손익률(매수 원금 대비) 최저 · '
            '최대 낙폭 = 그해 안에서 총자산의 고점 대비 최대 하락 · 평균 낙폭 = 전고점 대비 낙폭의 일평균 · '
            '평균 미회복 기간 = 전고점을 회복하지 못한 연속 구간의 평균 길이(그해 안에서 센 거래일) · '
            '거래 수 = 매수 + 매도 체결 건수 · 회전 수 = 연간 매도 대금 ÷ 평균 총자산 · '
            '미체결 수 = 체결되지 않은 매수 LOC + 매도 LOC 주문 수 · 승률 = 매도 건 중 이익 비율</div>')
    return ('<h2>연도별 수익률</h2><div class="ann-sum">' + "".join(cards) + "</div>" + legend + "".join(parts)
            + "".join(tables) + note)


def main():
    soxl = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    soxx = load_price_data("SOXX", "2006-12-01", "2026-12-31")["Close"].astype(float)
    qqq = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    ms = build_mode_series(build_weekly_rsi_series(qqq))
    vr = VR.vol_regime_days(soxx, soxl.index, "M", 0.7)
    qmode = get_week_mode_map(ms, soxl.index)
    END = str(soxl.index[-1].date())
    risk = [d for d, r in vr.items() if r == "위험"]
    dv = {d: 12 for d in risk}
    dh = {d: 5 for d in risk if qmode.get(d) == "AG"}

    p = DSSParams(**VR.PRESETS["공격형"])
    bt_b = run_backtest(p, soxl, ms, START, END)
    bt_v = run_backtest(p, soxl, ms, START, END, day_divisions=dv, day_hold=dh)
    base = bt_b.set_index("날짜")["총자산"]
    v = bt_v.set_index("날짜")["총자산"]
    base.index = pd.to_datetime(base.index)
    v.index = pd.to_datetime(v.index)

    # 월별 레짐 (그 달 첫 거래일 기준)
    reg = pd.Series(vr)
    reg.index = pd.to_datetime(reg.index)
    reg = reg[reg.index >= START]
    mreg = reg.groupby(reg.index.to_period("M")).first()

    def monthly(eq):
        me = eq.groupby(eq.index.to_period("M")).last()
        prev = me.shift(1)
        prev.iloc[0] = 1e4
        return me / prev - 1

    def yearly(eq):
        ye = eq.groupby(eq.index.year).last()
        prev = ye.shift(1)
        prev.iloc[0] = 1e4
        ret = ye / prev - 1
        mdd = eq.groupby(eq.index.year).apply(lambda s: (s / s.cummax() - 1).min())
        return ret, mdd

    def monthly_mdd(eq):
        out, prev = {}, 1e4
        for per, s in eq.groupby(eq.index.to_period("M")):
            x = pd.concat([pd.Series([prev]), s.reset_index(drop=True)])
            out[per] = (x / x.cummax() - 1).min()
            prev = s.iloc[-1]
        return pd.Series(out)

    mb, mv = monthly(base), monthly(v)
    mdb, mdv = monthly_mdd(base), monthly_mdd(v)
    yb, ydb = yearly(base)
    yv, ydv = yearly(v)
    cb, db = stats(base)
    cv, dvv = stats(v)
    n_risk = int((mreg == "위험").sum())

    def pct(x, digits=1):
        return "" if pd.isna(x) else f"{x * 100:+.{digits}f}%"

    def cls(x):
        return "" if pd.isna(x) else ("pos" if x > 0 else "neg" if x < 0 else "")

    def n(x):
        s = f"{x * 100:.1f}"
        s = s[:-2] if s.endswith(".0") else s
        return "0" if s in ("-0", "-0.0") else s

    def pair(a, b, da, db_):
        # 1줄: 수익률 공격형/V · 2줄: MDD 공격형/V — V가 나은 쪽이면 ▲ 색
        rb = "up" if b - a > 0.0005 else "down" if b - a < -0.0005 else ""
        mb_ = "up" if db_ - da > 0.0005 else "down" if db_ - da < -0.0005 else ""
        return (f'<div class="r"><span class="a {cls(a)}">{n(a)}</span>/<span class="b {cls(b)}">{n(b)}</span></div>'
                f'<div class="m"><span class="a">{n(da)}</span>/<span class="b {mb_}">{n(db_)}</span></div>')

    years = sorted(set(mb.index.year))
    # 월간 표
    rows = []
    for yr in years:
        cells = [f'<th class="yr">{yr}</th>']
        for mo in range(1, 13):
            per = pd.Period(f"{yr}-{mo:02d}", "M")
            if per not in mb.index:
                cells.append('<td class="empty"></td>')
                continue
            r = mreg.get(per, "")
            c = {"위험": "risk", "안정": "safe"}.get(r, "")
            cells.append(f'<td class="{c}" title="{yr}-{mo:02d} · {r or "-"} 레짐 · 공격형/공격형V">'
                         + pair(mb[per], mv[per], mdb[per], mdv[per]) + '</td>')
        cells.append('<td class="tot">' + pair(yb[yr], yv[yr], ydb[yr], ydv[yr]) + '</td>')
        rows.append("<tr>" + "".join(cells) + "</tr>")
    month_table = "\n".join(rows)

    # 연간 표
    yrows = []
    for yr in years:
        rm = int((mreg[mreg.index.year == yr] == "위험").sum())
        d_ret = yv[yr] - yb[yr]
        d_mdd = ydv[yr] - ydb[yr]
        yrows.append(
            f"<tr><th>{yr}</th><td class='c'>{'●' * rm if rm else '–'}</td>"
            f"<td class='{cls(yb[yr])}'>{pct(yb[yr])}</td><td class='{cls(yv[yr])}'>{pct(yv[yr])}</td>"
            f"<td class='{'up' if d_ret > 0.0005 else 'down' if d_ret < -0.0005 else ''}'>{pct(d_ret)}p</td>"
            f"<td class='neg'>{pct(ydb[yr])}</td><td class='neg'>{pct(ydv[yr])}</td>"
            f"<td class='{'up' if d_mdd > 0.0005 else 'down' if d_mdd < -0.0005 else ''}'>{pct(d_mdd)}p</td></tr>")
    year_table = "\n".join(yrows)

    annual_html = annual_section({"공격형": bt_b, "공격형V": bt_v})

    html = f"""<!doctype html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>공격형 vs 공격형V</title>
<style>
:root{{--bg:#fff;--fg:#1d1d1f;--mut:#6e6e73;--line:#e5e5ea;--pos:#c62828;--neg:#1565c0;
--risk:#ffe0d6;--riskb:#f4511e;--safe:#e3f4e6;--tot:#f5f5f7;--up:#2e7d32;--down:#b71c1c}}
@media (prefers-color-scheme:dark){{:root:not([data-theme="light"]){{--bg:#161618;--fg:#f2f2f7;--mut:#98989f;
--line:#2c2c30;--pos:#ff6b6b;--neg:#64b5f6;--risk:#4a2418;--riskb:#ff7043;--safe:#183323;--tot:#222226;
--up:#81c784;--down:#ef9a9a}}}}
body{{background:var(--bg);color:var(--fg);font-family:-apple-system,"Segoe UI","Malgun Gothic",sans-serif;
margin:0;padding:24px 16px;}}
.wrap{{max-width:1180px;margin:0 auto}}
h1{{font-size:22px;margin:0 0 4px}} h2{{font-size:17px;margin:32px 0 8px}}
.sub{{color:var(--mut);font-size:13px;line-height:1.6}}
.kpi{{display:flex;gap:12px;flex-wrap:wrap;margin:16px 0}}
.k{{border:1px solid var(--line);border-radius:10px;padding:10px 14px;min-width:220px}}
.k b{{display:block;font-size:13px;color:var(--mut);font-weight:600;margin-bottom:4px}}
.k span{{font-size:18px;font-weight:700;font-variant-numeric:tabular-nums}}
.scroll{{overflow-x:auto}}
table{{border-collapse:collapse;font-variant-numeric:tabular-nums;font-size:12px;width:100%}}
th,td{{border:1px solid var(--line);padding:4px 5px;text-align:right;white-space:nowrap}}
thead th{{background:var(--tot);text-align:center;position:sticky;top:0}}
th.yr{{text-align:center;background:var(--tot)}}
td.risk{{background:var(--risk);box-shadow:inset 3px 0 0 var(--riskb)}}
td.safe{{background:var(--safe)}}
td.tot{{background:var(--tot);font-weight:700}}
td .a{{opacity:.7}} td .b{{font-weight:700}} .pos{{color:var(--pos)}} .neg{{color:var(--neg)}}
td .m{{color:var(--mut);font-size:11px}} td .m .b.up{{color:var(--up)}} td .m .b.down{{color:var(--down)}}
td .r,td .m{{line-height:1.35}}
td.up{{color:var(--up);font-weight:700}} td.down{{color:var(--down);font-weight:700}} td.c{{text-align:center;color:var(--riskb)}}
.legend{{display:flex;gap:16px;flex-wrap:wrap;font-size:12px;color:var(--mut);margin:8px 0}}
.sw{{display:inline-block;width:14px;height:14px;border-radius:3px;vertical-align:-3px;margin-right:4px;border:1px solid var(--line)}}
{ANNUAL_CSS}
</style></head><body><div class="wrap">
<h1>공격형 vs 공격형V — 월간·연간 성과</h1>
<div class="sub">기간 {START} ~ {END} · 시작 $10,000 · 엔진 v2(시가 기준 티어 + 예수금 한도)<br>
공격형V = 공격형 + <b>위험 레짐 달</b>(월말 SOXX 20일 변동성 상승 &amp; 3년 백분위 ≥ 70% → 다음 달)에
12분할(투자금÷12) · 공세모드 신규 매수 보유 5일. 위험 달 {n_risk}개월.</div>
<div class="kpi">
<div class="k"><b>공격형</b><span>CAGR {cb * 100:.1f}% · MDD {db * 100:.1f}% · 칼마 {cb / abs(db):.2f}</span></div>
<div class="k"><b>공격형V</b><span>CAGR {cv * 100:.1f}% · MDD {dvv * 100:.1f}% · 칼마 {cv / abs(dvv):.2f}</span></div>
</div>

{annual_html}

<h2>월간 수익률</h2>
<div class="legend"><span><i class="sw" style="background:var(--risk);border-left:3px solid var(--riskb)"></i>위험 레짐 달</span>
<span><i class="sw" style="background:var(--safe)"></i>안정 레짐 달</span><span><i class="sw"></i>승부 레짐 달</span>
<span>각 칸 = <b>공격형/공격형V</b> · 1줄 수익률(%) · 2줄 MDD(%) — MDD 초록=V가 얕음, 빨강=V가 깊음</span></div>
<div class="scroll"><table><thead><tr><th>연도</th>{''.join(f'<th>{m}월</th>' for m in range(1, 13))}<th>연간</th></tr></thead>
<tbody>{month_table}</tbody></table></div>

<h2>연간 비교</h2>
<div class="scroll"><table><thead><tr><th>연도</th><th>위험 달</th><th>공격형 수익</th><th>공격형V 수익</th><th>차이</th>
<th>공격형 MDD</th><th>공격형V MDD</th><th>MDD 차이</th></tr></thead><tbody>{year_table}</tbody></table></div>
<div class="sub" style="margin-top:12px">연간 MDD는 그해 안에서의 고점 대비 최대 낙폭. 위험 달 ● 개수 = 그해 위험 레짐 개월 수.</div>
</div></body></html>"""
    out = os.path.join(HERE, "reports", "dss_v_monthly_compare.html")
    open(out, "w", encoding="utf-8").write(html)
    print(f"공격형  {cb*100:.1f} / {db*100:.1f} / {cb/abs(db):.2f}")
    print(f"공격형V {cv*100:.1f} / {dvv*100:.1f} / {cv/abs(dvv):.2f}  위험달 {n_risk}")
    print(pd.DataFrame({"공격형": yb * 100, "V": yv * 100, "MDD": ydb * 100, "V_MDD": ydv * 100}).round(1).to_string())
    print(out)


if __name__ == "__main__":
    main()
