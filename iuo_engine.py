"""
IUO 매매법 백테스트 엔진
- 사이클 기반 변동성 수확 전략 (SOXL)
- 30% 초기 매수 → 12.5%씩 물타기 → 마지막매수종가 대비 +4.3% 전량 매도
- 시간청산(MOC) 18거래일
- QQQ GROWTH 추세 기반 첫매수비율 조정 (옵션)
"""

import math
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd
import numpy as np


# ──────────────────────────────────────────────
# 0. 파라미터
# ──────────────────────────────────────────────

@dataclass
class IUOParams:
    initial_capital: float = 10000.0
    first_buy_ratio: float = 0.33       # 첫매수비율 (33%)
    buy0_pct: float = 0.00              # 첫매수 LOC: 전일종가 × (1 + buy0_pct)
    buy1_pct: float = -0.018            # 추가매수 Level1: 전일종가 × (1 - 1.8%)
    buy2_pct: float = -0.10             # 추가매수 Level2: 전일종가 × (1 - 10%)
    sell_pct: float = 0.043             # 매도: 마지막매수종가 × (1 + 4.3%)
    moc_days: int = 18                  # 시간청산 거래일
    max_additional_buys: int = 7        # 추가매수 제한
    divisions: int = 8                  # 분할수 (추가매수 금액 = 투자금/divisions)
    fee_rate: float = 0.0               # 수수료율
    slippage: float = 0.0               # 슬리피지
    # QQQ 추세 기능
    use_qqq_trend: bool = False
    trend_period: str = "260"           # "260" (260주) 또는 "all" (전기간)
    trend_thresholds: list = field(default_factory=lambda: [0.10, 0.05, 0.0, -0.06, -0.12])
    trend_ratios: list = field(default_factory=lambda: [0.20, 0.25, 0.33, 0.35, 0.40])


# ──────────────────────────────────────────────
# 1. QQQ GROWTH 추세 계산
# ──────────────────────────────────────────────

def calc_qqq_weekly_growth(qqq_daily: pd.DataFrame,
                           period: str = "260") -> pd.DataFrame:
    """
    QQQ 주간 종가에 지수회귀(GROWTH)로 추세선 계산.
    period: "260" (260주 롤링) 또는 "all" (전기간)
    Returns: DataFrame (week_end, close, trend, deviation)
    """
    qqq = qqq_daily.copy()
    qqq.index = pd.to_datetime(qqq.index)
    weekly = qqq['Close'].resample('W-FRI').last().dropna()

    records = []
    for i in range(len(weekly)):
        date = weekly.index[i]
        close = float(weekly.iloc[i])

        if period == "all":
            window = weekly.iloc[:i + 1]
        else:
            n = int(period)
            start = max(0, i - n + 1)
            window = weekly.iloc[start:i + 1]

        if len(window) < 10:
            records.append({'week_end': date, 'close': close,
                            'trend': None, 'deviation': None})
            continue

        # 지수회귀: ln(y) = a*x + b → y = exp(b) * exp(a*x)
        x = np.arange(len(window), dtype=np.float64)
        y = np.log(window.values.astype(np.float64))
        try:
            coeffs = np.polyfit(x, y, 1)
            trend_val = np.exp(coeffs[1] + coeffs[0] * (len(window) - 1))
            dev = (close - trend_val) / trend_val
        except Exception:
            trend_val = close
            dev = 0.0

        records.append({'week_end': date, 'close': close,
                        'trend': trend_val, 'deviation': dev})

    return pd.DataFrame(records)


def get_trend_buy_ratio(deviation: float, params: IUOParams) -> float:
    """이격도에 따른 첫매수비율 반환."""
    if deviation is None:
        return params.first_buy_ratio

    thresholds = params.trend_thresholds  # [+10%, +5%, 0%, -6%, -12%]
    ratios = params.trend_ratios          # [20%, 25%, 33%, 35%, 40%]

    # 초고평가 (이격도 > 최고 임계값)
    if deviation >= thresholds[0]:
        return ratios[0]
    # 고평가
    elif deviation >= thresholds[1]:
        return ratios[1]
    # 중립
    elif deviation >= thresholds[3]:
        return ratios[2]
    # 저평가
    elif deviation >= thresholds[4]:
        return ratios[3]
    # 초저평가
    else:
        return ratios[4]


def _get_weekly_deviation_map(qqq_growth_df: pd.DataFrame,
                              trading_days: pd.DatetimeIndex) -> dict:
    """각 거래일에 해당 주의 이격도를 매핑."""
    dev_map = {}
    for _, row in qqq_growth_df.iterrows():
        if row['deviation'] is None:
            continue
        friday = pd.Timestamp(row['week_end'])
        monday = friday - pd.Timedelta(days=4)
        week_days = trading_days[(trading_days >= monday) & (trading_days <= friday)]
        for d in week_days:
            dev_map[d] = row['deviation']
    return dev_map


# ──────────────────────────────────────────────
# 2. 백테스트 엔진
# ──────────────────────────────────────────────

def run_backtest(params: IUOParams,
                 price_df: pd.DataFrame,
                 qqq_df: pd.DataFrame = None,
                 start_date: str = "2015-12-31",
                 end_date: str = "2026-04-10") -> dict:
    """
    IUO 매매법 전체 백테스트.

    Returns: dict with
        - daily_log: list of dict (일별 기록)
        - sell_records: list of dict (매도 기록)
        - metrics: dict (성과 지표)
    """
    df = price_df.copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)

    df = df[(df.index >= start) & (df.index <= end)]

    # 시작일은 초기 상태 설정일 → 첫 거래일의 종가를 prev_close로 사용
    # 실제 매매는 두 번째 거래일부터 시작
    prev_close = None  # 첫 날은 매매 없이 종가만 기록
    trading_days = df.index

    # QQQ 추세 맵 (옵션)
    dev_map = {}
    if params.use_qqq_trend and qqq_df is not None:
        qqq_growth = calc_qqq_weekly_growth(qqq_df, params.trend_period)
        dev_map = _get_weekly_deviation_map(qqq_growth, trading_days)

    # 상태 변수
    cash = params.initial_capital
    cycle_base = params.initial_capital   # 매수기준액 (사이클 시작 총자산)
    shares = 0                            # 보유수량
    avg_cost = 0.0                        # 평단가
    last_buy_close = 0.0                  # 마지막 매수일 종가
    additional_buy_count = 0              # 추가매수 횟수
    cycle_day = 0                         # 사이클 내 거래일
    cum_realized = 0.0                    # 누적 실현손익
    peak_asset = params.initial_capital   # 최고 총자산 (MDD 계산)

    daily_log = []
    sell_records = []

    for date in trading_days:
        close = df.loc[date, 'Close']
        if isinstance(close, pd.Series):
            close = close.iloc[0]
        close = round(float(close), 2)

        day_buy_qty = 0
        day_sell_qty = 0
        day_realized = None
        day_realized_pct = None
        sold = False

        # ── 매도 체크 ──
        if shares > 0:
            cycle_day += 1
            sell_target = round(last_buy_close * (1 + params.sell_pct), 2)
            time_liquidation = (cycle_day >= params.moc_days)

            if close >= sell_target or time_liquidation:
                # 전량 매도
                revenue = shares * close
                cost_basis = cycle_base - cash  # 총 투자 금액
                fee = revenue * params.fee_rate
                net_revenue = revenue - fee

                day_realized = net_revenue - cost_basis
                day_realized_pct = (day_realized / cost_basis * 100) if cost_basis > 0 else 0.0
                cum_realized += day_realized

                day_sell_qty = shares
                cash += net_revenue
                total_asset = cash

                sell_records.append({
                    '매도일': date.strftime('%Y-%m-%d'),
                    '매도종가': close,
                    '매도수량': day_sell_qty,
                    '평단가': avg_cost,
                    '실현손익': round(day_realized, 2),
                    '손익률(%)': round(day_realized_pct, 2),
                    '매도유형': '시간청산' if time_liquidation and close < sell_target else '익절',
                    '사이클거래일': cycle_day,
                    '추가매수횟수': additional_buy_count,
                    '매수기준액': cycle_base,
                })

                # 사이클 리셋
                shares = 0
                avg_cost = 0.0
                last_buy_close = 0.0
                cycle_base = total_asset  # 다음 사이클 기준액
                additional_buy_count = 0
                cycle_day = 0
                sold = True

        # ── 매수 체크 (매도일에는 매수 안 함) ──
        if not sold and prev_close is not None:
            if shares == 0:
                # ── 새 사이클 첫매수 ──
                loc_price = round(prev_close * (1 + params.buy0_pct), 2)
                if close <= loc_price:
                    # QQQ 추세에 따른 첫매수비율 결정
                    fbr = params.first_buy_ratio
                    if params.use_qqq_trend:
                        dev = dev_map.get(date)
                        if dev is not None:
                            fbr = get_trend_buy_ratio(dev, params)

                    buy_amount = cycle_base * fbr
                    qty = int(buy_amount / prev_close)  # LOC가 = prev_close (buy0=0%)
                    if qty > 0:
                        cost = qty * close
                        fee = cost * params.fee_rate
                        if cash >= cost + fee:
                            cash -= (cost + fee)
                            shares = qty
                            avg_cost = close
                            last_buy_close = close
                            cycle_day = 1
                            day_buy_qty = qty

            elif additional_buy_count < params.max_additional_buys:
                # ── 추가매수 ──
                total_invested = cycle_base - cash
                buy_amount = total_invested / params.divisions

                # Level 1: buy1_pct
                loc1 = round(prev_close * (1 + params.buy1_pct), 2)
                if close <= loc1 and additional_buy_count < params.max_additional_buys:
                    qty1 = int(buy_amount / loc1)
                    if qty1 > 0:
                        cost1 = qty1 * close
                        fee1 = cost1 * params.fee_rate
                        if cash >= cost1 + fee1:
                            cash -= (cost1 + fee1)
                            total_cost = shares * avg_cost + cost1
                            shares += qty1
                            avg_cost = total_cost / shares
                            last_buy_close = close
                            additional_buy_count += 1
                            day_buy_qty += qty1

                            # 재계산: Level 2용 buy_amount 갱신
                            total_invested = cycle_base - cash
                            buy_amount = total_invested / params.divisions

                # Level 2: buy2_pct
                loc2 = round(prev_close * (1 + params.buy2_pct), 2)
                if close <= loc2 and additional_buy_count < params.max_additional_buys:
                    qty2 = int(buy_amount / loc2)
                    if qty2 > 0:
                        cost2 = qty2 * close
                        fee2 = cost2 * params.fee_rate
                        if cash >= cost2 + fee2:
                            cash -= (cost2 + fee2)
                            total_cost = shares * avg_cost + cost2
                            shares += qty2
                            avg_cost = total_cost / shares
                            last_buy_close = close
                            additional_buy_count += 1
                            day_buy_qty += qty2

        # ── 일일 평가 ──
        holding_value = shares * close
        total_asset = cash + holding_value
        eval_pnl = holding_value - (shares * avg_cost) if shares > 0 else 0.0

        if total_asset > peak_asset:
            peak_asset = total_asset
        dd = (total_asset - peak_asset) / peak_asset if peak_asset > 0 else 0.0
        cash_ratio = cash / total_asset if total_asset > 0 else 1.0

        daily_log.append({
            '날짜': date.strftime('%Y-%m-%d'),
            '종가': close,
            '매수수량': day_buy_qty,
            '매도수량': -day_sell_qty if day_sell_qty > 0 else 0,
            '보유수량': shares,
            '평단가': round(avg_cost, 2),
            '마지막매수종가': last_buy_close,
            '추가매수횟수': additional_buy_count,
            '진행일': cycle_day,
            '예수금': round(cash, 2),
            '평가액': round(holding_value, 2),
            '총자산': round(total_asset, 2),
            '실현손익': round(day_realized, 2) if day_realized is not None else None,
            '실현손익률(%)': round(day_realized_pct, 2) if day_realized_pct is not None else None,
            '평가손익': round(eval_pnl, 2),
            '누적실현손익': round(cum_realized, 2),
            'DD(%)': round(dd * 100, 2),
            '매수기준액': round(cycle_base, 2),
            '현금비율(%)': round(cash_ratio * 100, 2),
        })

        prev_close = close

    # ── 성과 지표 계산 ──
    metrics = _calc_metrics(daily_log, sell_records, params.initial_capital)

    return {
        'daily_log': daily_log,
        'sell_records': sell_records,
        'metrics': metrics,
    }


def _calc_metrics(daily_log: list, sell_records: list,
                  initial_capital: float) -> dict:
    """백테스트 결과에서 성과 지표 계산."""
    if not daily_log:
        return {}

    final_asset = daily_log[-1]['총자산']
    total_days = len(daily_log)
    years = total_days / 252

    total_return = (final_asset / initial_capital - 1) * 100
    cagr = ((final_asset / initial_capital) ** (1 / years) - 1) * 100 if years > 0 else 0

    # MDD
    dd_list = [r['DD(%)'] for r in daily_log]
    mdd = min(dd_list) if dd_list else 0

    # 승률
    wins = sum(1 for s in sell_records if s['실현손익'] > 0)
    losses = sum(1 for s in sell_records if s['실현손익'] <= 0)
    total_sells = len(sell_records)
    win_rate = (wins / total_sells * 100) if total_sells > 0 else 0

    # 일간 수익률 (Sharpe/Sortino)
    assets = [initial_capital] + [r['총자산'] for r in daily_log]
    daily_returns = np.diff(assets) / assets[:-1]
    avg_ret = np.mean(daily_returns)
    std_ret = np.std(daily_returns, ddof=1) if len(daily_returns) > 1 else 0
    downside = daily_returns[daily_returns < 0]
    std_down = np.std(downside, ddof=1) if len(downside) > 1 else 0

    rf = 0.0  # 무위험수익률 (연 0%)
    rf_daily = rf / 252
    sharpe = ((avg_ret - rf_daily) / std_ret * np.sqrt(252)) if std_ret > 0 else 0
    sortino = ((avg_ret - rf_daily) / std_down * np.sqrt(252)) if std_down > 0 else 0
    calmar = abs(cagr / mdd) if mdd != 0 else 0

    # 평균 보유기간
    hold_days = [s['사이클거래일'] for s in sell_records]
    avg_hold = np.mean(hold_days) if hold_days else 0

    # 평균 현금비율
    cash_ratios = [r['현금비율(%)'] for r in daily_log]
    avg_cash = np.mean(cash_ratios) if cash_ratios else 0

    # 추가매수 횟수 분포
    buy_count_dist = {}
    for s in sell_records:
        bc = s['추가매수횟수']
        buy_count_dist[bc] = buy_count_dist.get(bc, 0) + 1

    return {
        '최종자산': round(final_asset, 2),
        '총수익률(%)': round(total_return, 2),
        'CAGR(%)': round(cagr, 2),
        'MDD(%)': round(mdd, 2),
        '총매도횟수': total_sells,
        '익절': wins,
        '손절': losses,
        '승률(%)': round(win_rate, 2),
        'Sharpe': round(sharpe, 3),
        'Sortino': round(sortino, 3),
        'Calmar': round(calmar, 3),
        '평균보유기간': round(avg_hold, 1),
        '평균현금비율(%)': round(avg_cash, 2),
        '누적실현손익': round(daily_log[-1]['누적실현손익'], 2),
        '추가매수분포': buy_count_dist,
    }


# ──────────────────────────────────────────────
# 3. 경량 백테스트 (최적화용)
# ──────────────────────────────────────────────

def run_backtest_fast(params: IUOParams,
                      price_df: pd.DataFrame,
                      qqq_df: pd.DataFrame = None,
                      start_date: str = "2015-12-31",
                      end_date: str = "2026-04-10") -> Optional[dict]:
    """
    최적화 전용 경량 백테스트.
    daily_log 생성 없이 핵심 지표만 반환.
    """
    idx = price_df.index
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)

    range_mask = (idx >= start_ts) & (idx <= end_ts)
    prev_close = None  # 첫 날은 매매 없이 종가만 기록
    trade_dates = idx[range_mask]
    n_days = len(trade_dates)
    if n_days == 0:
        return None

    close_col = price_df.columns.get_loc('Close')
    mask_bool = np.asarray(range_mask)
    closes = np.round(
        price_df.values[mask_bool, close_col].astype(np.float64), 2
    )

    # QQQ 추세 맵
    dev_arr = np.full(n_days, np.nan)
    if params.use_qqq_trend and qqq_df is not None:
        qqq_growth = calc_qqq_weekly_growth(qqq_df, params.trend_period)
        dev_map = _get_weekly_deviation_map(qqq_growth, trade_dates)
        for i, d in enumerate(trade_dates):
            v = dev_map.get(d)
            if v is not None:
                dev_arr[i] = v

    # 로컬 파라미터
    fbr = params.first_buy_ratio
    buy0 = params.buy0_pct
    buy1 = params.buy1_pct
    buy2 = params.buy2_pct
    sell_pct = params.sell_pct
    moc = params.moc_days
    max_add = params.max_additional_buys
    div = params.divisions
    fee = params.fee_rate
    init_cap = params.initial_capital
    use_trend = params.use_qqq_trend

    cash = init_cap
    cycle_base = init_cap
    shares = 0
    avg_cost = 0.0
    last_buy_close = 0.0
    add_count = 0
    cycle_day = 0
    cum_realized = 0.0
    peak = init_cap
    max_dd = 0.0
    sell_count = 0
    win_count = 0

    for i in range(n_days):
        close = closes[i]
        sold = False

        # 매도
        if shares > 0:
            cycle_day += 1
            sell_target = last_buy_close * (1 + sell_pct)
            if close >= sell_target or cycle_day >= moc:
                revenue = shares * close
                cost_basis = cycle_base - cash
                net = revenue * (1 - fee)
                pnl = net - cost_basis
                cum_realized += pnl
                sell_count += 1
                if pnl > 0:
                    win_count += 1
                cash += net
                cycle_base = cash
                shares = 0
                avg_cost = 0.0
                last_buy_close = 0.0
                add_count = 0
                cycle_day = 0
                sold = True

        # 매수
        if not sold and prev_close is not None:
            if shares == 0:
                loc0 = prev_close * (1 + buy0)
                if close <= loc0:
                    ratio = fbr
                    if use_trend and not np.isnan(dev_arr[i]):
                        ratio = get_trend_buy_ratio(dev_arr[i], params)
                    buy_amt = cycle_base * ratio
                    qty = int(buy_amt / prev_close)
                    if qty > 0:
                        cost = qty * close
                        f = cost * fee
                        if cash >= cost + f:
                            cash -= (cost + f)
                            shares = qty
                            avg_cost = close
                            last_buy_close = close
                            cycle_day = 1
            elif add_count < max_add:
                invested = cycle_base - cash
                buy_amt = invested / div

                loc1 = prev_close * (1 + buy1)
                if close <= loc1 and add_count < max_add:
                    qty1 = int(buy_amt / loc1)
                    if qty1 > 0:
                        cost1 = qty1 * close
                        f1 = cost1 * fee
                        if cash >= cost1 + f1:
                            cash -= (cost1 + f1)
                            shares += qty1
                            last_buy_close = close
                            add_count += 1
                            invested = cycle_base - cash
                            buy_amt = invested / div

                loc2 = prev_close * (1 + buy2)
                if close <= loc2 and add_count < max_add:
                    qty2 = int(buy_amt / loc2)
                    if qty2 > 0:
                        cost2 = qty2 * close
                        f2 = cost2 * fee
                        if cash >= cost2 + f2:
                            cash -= (cost2 + f2)
                            shares += qty2
                            last_buy_close = close
                            add_count += 1

        total = cash + shares * close
        if total > peak:
            peak = total
        dd = (total - peak) / peak if peak > 0 else 0
        if dd < max_dd:
            max_dd = dd

        prev_close = close

    final = cash + shares * closes[-1] if n_days > 0 else init_cap

    return {
        'final_asset': final,
        'total_days': n_days,
        'mdd': max_dd,
        'sell_count': sell_count,
        'win_count': win_count,
        'cum_realized': cum_realized,
    }
