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
    base = run_backtest(p, soxl, ms, START, END).set_index("날짜")["총자산"]
    v = run_backtest(p, soxl, ms, START, END, day_divisions=dv, day_hold=dh).set_index("날짜")["총자산"]
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
</style></head><body><div class="wrap">
<h1>공격형 vs 공격형V — 월간·연간 성과</h1>
<div class="sub">기간 {START} ~ {END} · 시작 $10,000 · 엔진 v2(시가 기준 티어 + 예수금 한도)<br>
공격형V = 공격형 + <b>위험 레짐 달</b>(월말 SOXX 20일 변동성 상승 &amp; 3년 백분위 ≥ 70% → 다음 달)에
12분할(투자금÷12) · 공세모드 신규 매수 보유 5일. 위험 달 {n_risk}개월.</div>
<div class="kpi">
<div class="k"><b>공격형</b><span>CAGR {cb * 100:.1f}% · MDD {db * 100:.1f}% · 칼마 {cb / abs(db):.2f}</span></div>
<div class="k"><b>공격형V</b><span>CAGR {cv * 100:.1f}% · MDD {dvv * 100:.1f}% · 칼마 {cv / abs(dvv):.2f}</span></div>
</div>

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
