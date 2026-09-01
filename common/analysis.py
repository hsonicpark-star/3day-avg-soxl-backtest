from __future__ import annotations

import pandas as pd
import numpy as np


def compute_annual_stats(history_df, initial_capital):
    df = history_df.copy()
    df["날짜"] = pd.to_datetime(df["날짜"])
    df["Year"] = df["날짜"].dt.year
    rows = []
    prev_end = float(initial_capital)
    for yr in sorted(df["Year"].unique()):
        assets = df[df["Year"] == yr]["총자산($)"].values.astype(float)
        end_asset = float(assets[-1])
        annual_ret = (end_asset / prev_end - 1) * 100 if prev_end > 0 else 0.0
        all_a = np.concatenate([[prev_end], assets])
        peak  = np.maximum.accumulate(all_a)
        mdd   = float(((all_a - peak) / peak).min() * 100)
        rows.append({"연도": yr, "연간수익률(%)": round(annual_ret, 2), "MDD(%)": round(mdd, 2)})
        prev_end = end_asset
    return pd.DataFrame(rows)


def compute_monthly_pivot(history_df, initial_capital):
    df = history_df.copy()
    df["날짜"] = pd.to_datetime(df["날짜"])
    df["YM"] = df["날짜"].dt.to_period("M")
    monthly = []
    prev = float(initial_capital)
    for ym in sorted(df["YM"].unique()):
        end = float(df[df["YM"] == ym]["총자산($)"].iloc[-1])
        ret = (end / prev - 1) * 100 if prev > 0 else 0.0
        monthly.append({"Year": ym.year, "Month": ym.month, "Return": round(ret, 2)})
        prev = end
    mdf = pd.DataFrame(monthly)
    pivot = mdf.pivot(index="Year", columns="Month", values="Return")
    month_kr = {1:"1월",2:"2월",3:"3월",4:"4월",5:"5월",6:"6월",
                7:"7월",8:"8월",9:"9월",10:"10월",11:"11월",12:"12월"}
    pivot.columns = [month_kr.get(c, c) for c in pivot.columns]
    return pivot


def compute_sharpe_sortino(assets, risk_free_annual=0.04):
    """샤프 비율 & 소르티노 비율 (연환산)."""
    if len(assets) < 2:
        return 0.0, 0.0
    daily_ret = np.diff(assets) / assets[:-1]
    rf_daily  = risk_free_annual / 252
    excess    = daily_ret - rf_daily
    std_all   = np.std(excess, ddof=1)
    sharpe    = np.mean(excess) / std_all * np.sqrt(252) if std_all > 0 else 0.0
    downside  = excess[excess < 0]
    std_down  = np.std(downside, ddof=1) if len(downside) > 1 else 0.0
    sortino   = np.mean(excess) / std_down * np.sqrt(252) if std_down > 0 else 0.0
    return round(sharpe, 3), round(sortino, 3)


def compute_rolling_perf(assets, window_days=252):
    """롤링 CAGR(%) 및 MDD(%) 계산. 첫 window_days 구간은 NaN."""
    n = len(assets)
    rolling_cagr = np.full(n, np.nan)
    rolling_mdd  = np.full(n, np.nan)
    years = window_days / 252.0
    for i in range(window_days, n):
        sub  = assets[i - window_days: i + 1]
        cagr = (sub[-1] / sub[0]) ** (1.0 / years) - 1.0
        peak = np.maximum.accumulate(sub)
        mdd  = ((sub - peak) / peak).min()
        rolling_cagr[i] = round(cagr * 100, 2)
        rolling_mdd[i]  = round(mdd * 100, 2)
    return rolling_cagr, rolling_mdd


def compute_bnh(price_df, start_date, end_date, initial_capital):
    """Buy & Hold 자산 시계열 반환."""
    sub = price_df.loc[pd.to_datetime(start_date):pd.to_datetime(end_date), "Close"].dropna()
    if sub.empty:
        return np.array([]), pd.DatetimeIndex([])
    shares_bnh = initial_capital / float(sub.iloc[0])
    assets_bnh = sub.values.astype(float) * shares_bnh
    return assets_bnh, sub.index


def recalc_adj_history(adj_history, base_capital):
    """자본 조정 이력을 날짜순 정렬 + 누적자본금 재계산.

    Args:
        adj_history: [{"날짜": str, "조정금액": float, ...}, ...]
        base_capital: 시작 자본 (모든 조정 이전의 base 금액, 변경되지 않음)

    Returns:
        (정렬된 이력 list, base + sum(조정) 최종 자본금 float)
    """
    if not adj_history:
        return [], float(base_capital)
    sorted_history = sorted(adj_history, key=lambda x: str(x.get("날짜", "")))
    cumulative = float(base_capital)
    for item in sorted_history:
        cumulative += float(item.get("조정금액", 0))
        item["누적자본금"] = float(cumulative)
    return sorted_history, float(cumulative)


# ══════════════════════════════════════════════════════════════
# 월별 수익률 히트맵 (전 전략 공용)
# ══════════════════════════════════════════════════════════════
# 각 셀 = 월수익률 / 월 MDD / (선택) 모드 비율,  우측 끝 = YEAR · MDD
# plotly imshow 대비 장점: 한 셀에 3개 지표를 담고 연간 요약 열을 붙일 수 있다.
#
#   from common.analysis import monthly_perf_table
#   st.markdown(monthly_perf_table(equity_series, mode_series), unsafe_allow_html=True)

_HEAT_BG   = "#2A2E39"
_HEAT_HEAD = "#1E2229"
_HEAT_SIDE = "#171A20"
_HEAT_LINE = "#3A3F4B"


def ret_heat_color(v, vmax: float = 12.0) -> str:
    """수익률(%) → 히트맵 배경색 (음수 빨강 ↔ 0 중립 ↔ 양수 초록)."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return _HEAT_BG
    t = max(-1.0, min(1.0, float(v) / vmax))
    if t >= 0:
        r = int(42 + (16 - 42) * t); g = int(46 + (150 - 46) * t); b = int(57 + (70 - 57) * t)
    else:
        u = -t
        r = int(42 + (150 - 42) * u); g = int(46 + (40 - 46) * u); b = int(57 + (60 - 57) * u)
    return f"rgb({r},{g},{b})"


def monthly_perf_table(equity, mode=None, vmax: float = 12.0,
                       mode_short=None) -> str:
    """월별 수익률 히트맵 HTML 테이블.

    Args:
        equity: 총자산 시계열 (pd.Series, DatetimeIndex). 결측 없이 일별 정렬.
        mode  : 같은 인덱스의 모드/구간 라벨 Series (선택). 주면 셀에 비율 표시.
        vmax  : 색상 포화 기준 수익률(%) — ±vmax 에서 최대 채도.
        mode_short: {라벨: 축약문자} dict (선택). 미지정 시 라벨 첫 글자.

    Returns: HTML 문자열 (st.markdown(..., unsafe_allow_html=True) 로 렌더)

    ※ 월수익률의 기준값은 **전월 말 총자산**(그 달 첫 행 직전 값)이다.
       그 달 첫날 값을 기준으로 하면 월초 갭이 누락되어 월별 누적이
       연간 수익률과 어긋난다.
    """
    eq = pd.Series(equity).dropna()
    if eq.empty:
        return "<div>데이터 없음</div>"
    eq = eq.sort_index()
    idx = pd.DatetimeIndex(eq.index)
    md = pd.Series(mode).reindex(eq.index) if mode is not None else None

    def _base_at(pos, fallback):
        return float(eq.iloc[pos - 1]) if pos > 0 else float(fallback)

    cells, ydata = {}, {}
    years = sorted(set(idx.year))
    for y in years:
        ymask = idx.year == y
        for m in sorted(set(idx[ymask].month)):
            sel = ymask & (idx.month == m)
            g = eq[sel]
            pos = eq.index.get_loc(g.index[0])
            base = _base_at(pos, g.iloc[0])
            ret = (float(g.iloc[-1]) / base - 1) * 100 if base > 0 else 0.0
            peak = g.cummax()
            mdd = float((g / peak - 1).min()) * 100
            mix = ""
            if md is not None:
                vc = md[sel].dropna()
                vc = vc[vc.astype(str) != ""].value_counts()
                tot = int(vc.sum())
                if tot:
                    mix = " / ".join(
                        f"{(mode_short or {}).get(k, str(k)[:1])}"
                        f"{int(v) / tot * 100:.0f}%"
                        for k, v in vc.items())
            cells[(y, m)] = (ret, mdd, mix)
        g = eq[ymask]
        pos = eq.index.get_loc(g.index[0])
        base = _base_at(pos, g.iloc[0])
        peak = g.cummax()
        ydata[y] = ((float(g.iloc[-1]) / base - 1) * 100 if base > 0 else 0.0,
                    float((g / peak - 1).min()) * 100)

    th = (f"padding:6px 4px;border:1px solid {_HEAT_LINE};background:{_HEAT_HEAD};"
          "color:#C8CDD6;font-size:0.72em;font-weight:700;text-align:center")
    h = ['<div style="overflow-x:auto"><table style="border-collapse:collapse;'
         'width:100%;min-width:1100px;font-family:inherit">',
         f'<tr><th style="{th}">연도</th>']
    for m in range(1, 13):
        h.append(f'<th style="{th}">{m}월</th>')
    h.append(f'<th style="{th};background:{_HEAT_SIDE}">YEAR</th>')
    h.append(f'<th style="{th};background:{_HEAT_SIDE}">MDD</th></tr>')

    for y in years:
        h.append(f'<tr><td style="{th};background:{_HEAT_SIDE}">{y}</td>')
        for m in range(1, 13):
            c = cells.get((y, m))
            if c is None:
                h.append(f'<td style="padding:6px 4px;border:1px solid {_HEAT_LINE};'
                         f'background:{_HEAT_BG};color:#666;text-align:center">-</td>')
                continue
            ret, mdd, mix = c
            sub = (f'<div style="color:#AEB4BF;font-size:0.60em">{mix}</div>'
                   if mix else "")
            h.append(
                f'<td style="padding:5px 4px;border:1px solid {_HEAT_LINE};'
                f'background:{ret_heat_color(ret, vmax)};text-align:center;'
                f'line-height:1.35">'
                f'<div style="color:#fff;font-weight:700;font-size:0.82em">{ret:+.1f}%</div>'
                f'<div style="color:#D5D9E0;font-size:0.64em">MDD {mdd:.1f}%</div>'
                f'{sub}</td>')
        yret, ymdd = ydata[y]
        yc = "#4ADE80" if yret >= 0 else "#F87171"
        h.append(f'<td style="padding:6px 4px;border:1px solid {_HEAT_LINE};'
                 f'background:{_HEAT_SIDE};color:{yc};font-weight:700;'
                 f'text-align:center;font-size:0.85em">{yret:+.1f}%</td>')
        h.append(f'<td style="padding:6px 4px;border:1px solid {_HEAT_LINE};'
                 f'background:{_HEAT_SIDE};color:#F87171;font-weight:700;'
                 f'text-align:center;font-size:0.85em">{ymdd:.1f}%</td></tr>')
    h.append("</table></div>")
    legend = ("셀 = 월수익률 / 월 MDD"
              + (" / 모드 비율" if md is not None else "")
              + " · 우측 = 연간 수익률 · 연간 MDD")
    h.append(f'<div style="margin-top:6px;font-size:0.7em;color:#888">{legend}</div>')
    return "".join(h)
