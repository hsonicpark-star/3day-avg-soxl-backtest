"""
DSS 동파법(동적파도타기법) 백테스트 엔진
- QQQ 주간 RSI 기반 모드 전환 (AG/SF)
- SOXL LOC 매수/매도
- 시드분할, 손절, 투자금갱신(복리)
"""

import math
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd
import numpy as np
import yfinance as yf


# ──────────────────────────────────────────────
# 0. 데이터 로드
# ──────────────────────────────────────────────

def load_price_data(ticker: str, start: str = "2009-01-01",
                    end: str = "2026-12-31") -> pd.DataFrame:
    """yfinance에서 미조정(unadjusted) 종가 로드.
    미국장이 종료되지 않은 당일 intraday 데이터는 제외."""
    df = yf.download(ticker, start=start, end=end, auto_adjust=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.index = pd.to_datetime(df.index)
    # US 장 마감(16:30 ET) 이전이면 당일 intraday 데이터 제거
    try:
        now_est = pd.Timestamp.now(tz="America/New_York")
        market_close_buffer = now_est.replace(hour=16, minute=30,
                                              second=0, microsecond=0)
        if now_est < market_close_buffer:
            us_today = pd.Timestamp(now_est.date())
            df = df[df.index.normalize() < us_today]
    except Exception:
        pass
    return df


# ──────────────────────────────────────────────
# 주문 시트 rows 생성 (공통 — 웹앱 + 자동 발송 스크립트 모두 사용)
# ──────────────────────────────────────────────

def build_order_rows(os_result: dict, today=None) -> list:
    """DSS 주문표를 L/M/N/O 4열 포맷 rows로 변환.

    컬럼 순서: [구분(매수/매도), 거래방법(LOC/MOC), 가격($, 숫자), 수량(주, 정수)]
    MOC 매도(손절일 도래)는 가격 공란.

    Args:
        os_result: `_build_os_result_from_backtest()` 반환값 또는 유사 dict.
                   required keys: open_positions, n_pos, cur_divisions,
                                  next_buy_order, buy_qty_est
        today: 기준일(Timestamp). None이면 오늘 날짜 사용.

    Returns:
        list of 4-element rows, 시트 L4:O{N}에 기록될 포맷.
    """
    from datetime import datetime as _dt
    if today is None:
        today_ts = pd.Timestamp(_dt.today().date())
    else:
        today_ts = pd.Timestamp(today)

    rows = []
    # 매도 주문 (보유 포지션별)
    for pos in os_result.get("open_positions", []) or []:
        if pos.get("sell_target") is None:
            continue
        _stop = pos.get("stop_date")
        _is_moc = False
        if _stop is not None:
            try:
                _stop_ts = pd.Timestamp(_stop)
                _is_moc = (_stop_ts <= today_ts)
            except Exception:
                _is_moc = False
        if _is_moc:
            rows.append(["매도", "MOC", "", int(pos.get("qty", 0))])
        else:
            rows.append(["매도", "LOC", round(float(pos["sell_target"]), 2),
                         int(pos.get("qty", 0))])

    # 매수 주문 (빈 슬롯 있을 때만)
    if os_result.get("n_pos", 0) < os_result.get("cur_divisions", 0):
        rows.append(["매수", "LOC",
                     round(float(os_result.get("next_buy_order", 0)), 2),
                     int(os_result.get("buy_qty_est", 0))])
    return rows


# ──────────────────────────────────────────────
# 1. 주간 RSI 계산 (KB증권 단순평균 방식, 14주)
# ──────────────────────────────────────────────

def get_weekly_closes(daily_data: pd.DataFrame) -> pd.DataFrame:
    """일봉 → 주봉 종가 변환. 금요일(또는 해당 주 마지막 거래일) 종가 사용."""
    df = daily_data.copy()
    df.index = pd.to_datetime(df.index)
    # 주 단위 리샘플링: 금요일 기준, 마지막 종가
    weekly = df['Close'].resample('W-FRI').last().dropna()
    return weekly


def calc_weekly_rsi(weekly_closes: pd.Series, date: pd.Timestamp) -> Optional[float]:
    """
    KB증권 단순평균 방식 14주 RSI 계산.
    date: 해당 주의 마지막 거래일(금요일)
    weekly_closes: 주봉 종가 시리즈 (index=금요일 날짜)

    수식:
      Change[i] = Close[i] - Close[i+1]  (최근→과거)
      AvgUp = sum(Change > 0) / 14
      AvgDn = -sum(Change < 0) / 14
      RS = AvgUp / AvgDn
      RSI = RS / (1+RS) * 100
    """
    # date 이전의 주봉 종가 15개 필요 (14개 변화량)
    available = weekly_closes[weekly_closes.index <= date]
    if len(available) < 15:
        return None

    # 최근 15개 종가 (내림차순 = 최근→과거)
    closes_15 = available.iloc[-15:].values[::-1]  # [newest, ..., oldest]

    # 14개 변화량 계산
    changes = closes_15[:-1] - closes_15[1:]  # close[i] - close[i+1]

    ups = changes[changes > 0]
    downs = changes[changes < 0]

    avg_up = ups.sum() / 14 if len(ups) > 0 else 0
    avg_dn = -downs.sum() / 14 if len(downs) > 0 else 0

    if avg_dn == 0:
        return 100.0 if avg_up > 0 else 50.0

    rs = avg_up / avg_dn
    rsi = rs / (1 + rs) * 100
    return round(rsi, 8)


def build_weekly_rsi_series(qqq_daily: pd.DataFrame) -> pd.DataFrame:
    """QQQ 일봉 데이터로 주간 RSI 시리즈 생성."""
    weekly_closes = get_weekly_closes(qqq_daily)
    records = []
    for i in range(len(weekly_closes)):
        date = weekly_closes.index[i]
        rsi = calc_weekly_rsi(weekly_closes, date)
        if rsi is not None:
            records.append({'week_end': date, 'close': weekly_closes.iloc[i], 'rsi': rsi})
    return pd.DataFrame(records)


# ──────────────────────────────────────────────
# 2. 모드 전환 로직
# ──────────────────────────────────────────────

def determine_mode(rr: float, r: float, prev_mode: str) -> str:
    """
    모드 전환 규칙.
    rr: 2주 전 RSI
    r:  1주 전 RSI
    prev_mode: 직전 모드 ("AG" or "SF")
    """
    if rr <= 35 and rr < r:
        return "AG"   # RSI ≤35에서 상승
    elif rr >= 40 and rr < 50 and rr > r:
        return "SF"   # RSI 40~50에서 하락
    elif rr <= 50 and r > 50:
        return "AG"   # 50 상향돌파
    elif rr >= 50 and r < 50:
        return "SF"   # 50 하향돌파
    elif rr >= 50 and rr < 60 and rr < r:
        return "AG"   # RSI 50~60에서 상승
    elif rr > 65 and rr > r:
        return "SF"   # RSI >65에서 하락
    else:
        return prev_mode  # 유지


def build_mode_series(weekly_rsi_df: pd.DataFrame, initial_mode: str = "AG") -> pd.DataFrame:
    """주간 RSI 시리즈에서 모드 전환 시리즈 생성."""
    df = weekly_rsi_df.copy()
    modes = []
    for i in range(len(df)):
        if i < 2:
            modes.append(initial_mode)
        else:
            rr = df.iloc[i - 2]['rsi']  # 2주 전
            r = df.iloc[i - 1]['rsi']   # 1주 전
            prev = modes[i - 1]
            modes.append(determine_mode(rr, r, prev))
    df['mode'] = modes
    return df


# ──────────────────────────────────────────────
# 3. 거래일/영업일 유틸리티
# ──────────────────────────────────────────────

def get_trading_days(daily_data: pd.DataFrame) -> pd.DatetimeIndex:
    """일봉 데이터에서 실제 거래일 추출."""
    return pd.to_datetime(daily_data.index)


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int):
    """year/month의 n번째 weekday(0=월요일)를 반환."""
    from datetime import date as _d, timedelta as _td
    first = _d(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + _td(days=offset + 7 * (n - 1))


def _last_weekday_of_month(year: int, month: int, weekday: int):
    """year/month의 마지막 weekday를 반환."""
    from datetime import date as _d, timedelta as _td
    if month == 12:
        last = _d(year + 1, 1, 1) - _td(days=1)
    else:
        last = _d(year, month + 1, 1) - _td(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - _td(days=offset)


def _easter_date(year: int):
    """Anonymous Gregorian algorithm으로 부활절 일자 반환."""
    from datetime import date as _d
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return _d(year, month, day + 1)


def _us_market_holidays_year(year: int):
    """해당 연도의 NYSE 휴장일 집합 반환."""
    from datetime import date as _d, timedelta as _td
    hs = set()
    hs.add(_d(year, 1, 1))                              # New Year's Day
    hs.add(_nth_weekday_of_month(year, 1, 0, 3))        # MLK Day (1월 셋째 월)
    hs.add(_nth_weekday_of_month(year, 2, 0, 3))        # Presidents' Day (2월 셋째 월)
    hs.add(_easter_date(year) - _td(days=2))            # Good Friday
    hs.add(_last_weekday_of_month(year, 5, 0))          # Memorial Day (5월 마지막 월)
    hs.add(_d(year, 6, 19))                              # Juneteenth
    hs.add(_d(year, 7, 4))                               # Independence Day
    hs.add(_nth_weekday_of_month(year, 9, 0, 1))        # Labor Day (9월 첫째 월)
    hs.add(_nth_weekday_of_month(year, 11, 3, 4))       # Thanksgiving (11월 넷째 목)
    hs.add(_d(year, 12, 25))                             # Christmas
    # 주말과 겹치는 휴일의 observed 처리
    observed = set()
    for h in hs:
        if h.weekday() == 5:    # 토요일 → 금요일
            observed.add(h - _td(days=1))
        elif h.weekday() == 6:  # 일요일 → 월요일
            observed.add(h + _td(days=1))
    return hs | observed


def _is_us_trading_day(d) -> bool:
    """주말/NYSE 휴장일이 아닌 거래일이면 True."""
    d = d.date() if hasattr(d, 'date') else d
    if d.weekday() >= 5:  # 주말
        return False
    return d not in _us_market_holidays_year(d.year)


def next_us_trading_days(start_date, n_days: int) -> pd.DatetimeIndex:
    """start_date 다음 영업일부터 n개의 NYSE 거래일 반환.
    Memorial Day, Good Friday 등 NYSE 휴장일 자동 제외.

    Args:
        start_date: 기준일 (이 날짜 다음부터 카운트 시작)
        n_days: 반환할 거래일 개수
    """
    result = []
    current = pd.Timestamp(start_date) + pd.Timedelta(days=1)
    safety = 0
    while len(result) < n_days and safety < n_days * 3 + 30:
        if _is_us_trading_day(current):
            result.append(current)
        current += pd.Timedelta(days=1)
        safety += 1
    return pd.DatetimeIndex(result)


def workday_offset(start_date: pd.Timestamp, n_days: int,
                   trading_days: pd.DatetimeIndex) -> pd.Timestamp:
    """start_date로부터 n_days 거래일 이후의 날짜 반환.
    데이터 범위를 넘어서면 NYSE 휴장일을 제외한 거래일 계산으로 보정.
    (Memorial Day, Good Friday 등 휴장일을 정확히 처리)"""
    future = trading_days[trading_days > start_date]
    if len(future) >= n_days:
        return future[n_days - 1]
    # 데이터 부족 시: 마지막 known 거래일부터 NYSE 거래일 기준 day-by-day 진행
    covered = len(future)
    remaining = n_days - covered
    last_known = future[-1] if covered > 0 else start_date

    current = pd.Timestamp(last_known) + pd.Timedelta(days=1)
    count = 0
    while count < remaining:
        if _is_us_trading_day(current):
            count += 1
            if count == remaining:
                return current
        current += pd.Timedelta(days=1)
        # 안전장치: 무한루프 방지 (1년 이상 가면 중단)
        if (current - pd.Timestamp(last_known)).days > 400:
            break
    return current


# ──────────────────────────────────────────────
# 4. 매매 시뮬레이션 엔진
# ──────────────────────────────────────────────

@dataclass
class Position:
    buy_date: pd.Timestamp
    buy_price: float
    qty: int
    buy_order_price: float
    sell_target: float
    stop_date: pd.Timestamp
    mode: str
    buy_amount: float = 0.0
    buy_fee: float = 0.0


@dataclass
class DSSParams:
    # 안전모드
    sf_divisions: int = 7
    sf_max_hold: int = 30
    sf_buy_pct: float = 0.03      # 3%
    sf_sell_pct: float = 0.002    # 0.2%
    # 공세모드
    ag_divisions: int = 7
    ag_max_hold: int = 7
    ag_buy_pct: float = 0.05     # 5%
    ag_sell_pct: float = 0.025   # 2.5%
    # 공통
    initial_capital: float = 10000.0
    fee_rate: float = 0.0004     # 0.04%
    sec_fee_rate: float = 0.0000278  # SEC fee (매도 시에만)
    renewal_period: int = 10     # 투자금갱신주기
    pcr: float = 0.80            # 이익복리율
    lcr: float = 0.30            # 손실복리율


def get_mode_for_date(date: pd.Timestamp, mode_series: pd.DataFrame) -> str:
    """특정 날짜가 속한 주의 모드를 반환."""
    # date 이전의 가장 최근 주 모드
    before = mode_series[mode_series['week_end'] <= date + pd.Timedelta(days=6)]
    if len(before) == 0:
        return "AG"
    # date가 속한 주의 금요일 찾기
    # date의 주 시작(월요일) ~ 종료(금요일) 범위에 맞는 주봉 찾기
    week_friday = date + pd.Timedelta(days=(4 - date.weekday()) % 7)
    if date.weekday() > 4:  # 주말
        week_friday = date + pd.Timedelta(days=(4 - date.weekday()) % 7)

    # 해당 주 또는 그 이전 주의 모드
    mask = mode_series['week_end'] >= date - pd.Timedelta(days=6)
    mask &= mode_series['week_end'] <= date + pd.Timedelta(days=6)
    if mask.any():
        return mode_series.loc[mask].iloc[0]['mode']

    # fallback: 가장 최근 모드
    past = mode_series[mode_series['week_end'] <= date]
    if len(past) > 0:
        return past.iloc[-1]['mode']
    return "AG"


def get_week_mode_map(mode_series: pd.DataFrame, trading_days: pd.DatetimeIndex) -> dict:
    """
    각 거래일에 대해 해당 주의 모드를 매핑.
    mode_series의 week_end(금요일)를 기준으로 해당 주 월~금에 모드 할당.
    """
    mode_map = {}
    for _, row in mode_series.iterrows():
        friday = pd.Timestamp(row['week_end'])
        monday = friday - pd.Timedelta(days=4)
        mode = row['mode']
        # 해당 주의 모든 거래일에 모드 할당
        week_days = trading_days[(trading_days >= monday) & (trading_days <= friday)]
        for d in week_days:
            mode_map[d] = mode
    return mode_map


def run_backtest(params: DSSParams,
                 soxl_daily: pd.DataFrame,
                 mode_series: pd.DataFrame,
                 start_date: str = "2024-01-02",
                 end_date: str = "2026-04-10",
                 capital_adj_history: list = None) -> pd.DataFrame:
    """
    DSS 동파법 백테스트 실행.

    Args:
        capital_adj_history: 자본 조정 이력 [{날짜, 조정금액, ...}, ...]
            - 백테스트 도중 해당 날짜에 capital/cash 에 즉시 합산
            - 다음 거래일부터 시드/매수수량에 반영됨
            - 시트 동작과 일치: 조정 발생 다음 거래일부터 새 시드 적용

    Returns: DataFrame with daily trading log.
    """
    soxl = soxl_daily.copy()
    soxl.index = pd.to_datetime(soxl.index)
    soxl = soxl.sort_index()

    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)

    all_trading_days = soxl.index  # 전체 거래일 (손절일 계산용)

    # 시작일 이전 종가를 prev_close로 사용
    before_start = soxl[soxl.index < start]
    prev_close = float(before_start.iloc[-1]['Close']) if len(before_start) > 0 else None

    soxl = soxl[(soxl.index >= start) & (soxl.index <= end)]
    trading_days = soxl.index

    # 모드 맵 생성
    mode_map = get_week_mode_map(mode_series, all_trading_days)

    # 자본 조정 이력 정렬 (날짜 오름차순)
    pending_adjs = []
    if capital_adj_history:
        for _adj in capital_adj_history:
            try:
                _dt = pd.Timestamp(_adj.get("날짜"))
                _amt = float(_adj.get("조정금액", 0))
                if _amt != 0:
                    pending_adjs.append((_dt, _amt))
            except Exception:
                continue
        pending_adjs.sort(key=lambda x: x[0])
    _adj_idx = 0  # 다음 적용 대기 중인 조정의 인덱스

    # 상태 변수
    capital = params.initial_capital          # 현재 투자금 (갱신 대상)
    cash = params.initial_capital             # 예수금
    positions: list[Position] = []            # 보유 포지션
    sell_count = 0                            # 누적 매도 건수
    days_since_renewal = 0                    # 갱신 이후 거래일수 (10거래일마다 갱신)
    realized_pnl_since_renewal: list[float] = []  # 갱신 주기 내 실현손익
    cumulative_realized = 0.0                 # 누적 실현손익

    # 백테스트 시작일 이전 발생 조정은 시작 전에 적용 (초기 자본에 반영)
    if len(trading_days) > 0:
        first_td = trading_days[0]
        while _adj_idx < len(pending_adjs) and pending_adjs[_adj_idx][0] < first_td:
            _, _amt = pending_adjs[_adj_idx]
            capital += _amt
            cash += _amt
            _adj_idx += 1

    records = []

    for date in trading_days:
        close = soxl.loc[date, 'Close']
        if isinstance(close, pd.Series):
            close = close.iloc[0]
        close = round(float(close), 2)  # yfinance float32 정밀도 보정

        mode = mode_map.get(date, "AG")

        # 모드별 파라미터
        if mode == "AG":
            divisions = params.ag_divisions
            buy_pct = params.ag_buy_pct
            sell_pct = params.ag_sell_pct
            max_hold = params.ag_max_hold
        else:
            divisions = params.sf_divisions
            buy_pct = params.sf_buy_pct
            sell_pct = params.sf_sell_pct
            max_hold = params.sf_max_hold

        seed_per_trade = capital / divisions

        # ── 매도 처리 ──
        sold_positions = []
        daily_realized = 0.0
        remaining_positions = []

        for pos in positions:
            sell = False
            # 1) 매도목표가 도달
            if close >= pos.sell_target:
                sell = True
            # 2) 손절일 도래
            elif date >= pos.stop_date:
                sell = True

            if sell:
                sell_amount = pos.qty * close
                sell_fee = sell_amount * (params.fee_rate + params.sec_fee_rate)
                net_sell = sell_amount - sell_fee
                net_buy = pos.buy_amount + pos.buy_fee
                pnl = net_sell - net_buy

                sold_positions.append({
                    'buy_date': pos.buy_date,
                    'buy_price': pos.buy_price,
                    'qty': pos.qty,
                    'sell_date': date,
                    'sell_price': close,
                    'sell_target': pos.sell_target,
                    'stop_date': pos.stop_date,
                    'pnl': pnl,
                    'mode': pos.mode,
                })

                cash += net_sell
                daily_realized += pnl
                cumulative_realized += pnl
                sell_count += 1
                realized_pnl_since_renewal.append(pnl)
            else:
                remaining_positions.append(pos)

        positions = remaining_positions

        # ── 투자금 갱신 (복리) — 매 N 거래일마다 ──
        # (시트 RECORD 검증: 거래일수 10일째에 갱신, 매도 횟수와 무관)
        days_since_renewal += 1
        renewal_happened = False
        renewal_amount = 0.0
        if days_since_renewal >= params.renewal_period:
            # 직전 N거래일 동안의 실현손익 합산 (매도 0회면 갱신 0)
            total_pnl = sum(realized_pnl_since_renewal) if realized_pnl_since_renewal else 0.0
            if total_pnl > 0:
                renewal_amount = total_pnl * params.pcr
            elif total_pnl < 0:
                renewal_amount = total_pnl * params.lcr
            # else: total_pnl == 0 → renewal_amount = 0 (그래도 카운터 리셋)
            if renewal_amount != 0:
                capital += renewal_amount
                renewal_happened = True
            days_since_renewal = 0
            realized_pnl_since_renewal = []

        # ── 매수 처리 ──
        bought = False
        buy_order_price = None
        buy_qty = 0

        if prev_close is not None and len(positions) < divisions:
            buy_order_price = math.floor(prev_close * (1 + buy_pct) * 100) / 100  # ROUNDDOWN 2자리

            if close <= buy_order_price:
                buy_qty = int(seed_per_trade / buy_order_price)
                if buy_qty > 0:
                    buy_amount = buy_qty * close
                    buy_fee = buy_amount * params.fee_rate
                    if cash >= buy_amount + buy_fee:
                        cash -= (buy_amount + buy_fee)

                        sell_target = round(close * (1 + sell_pct), 2)
                        stop_date = workday_offset(date, max_hold, all_trading_days)

                        pos = Position(
                            buy_date=date,
                            buy_price=close,
                            qty=buy_qty,
                            buy_order_price=buy_order_price,
                            sell_target=sell_target,
                            stop_date=stop_date,
                            mode=mode,
                            buy_amount=buy_amount,
                            buy_fee=buy_fee,
                        )
                        positions.append(pos)
                        bought = True

        # ── 평가 ──
        holding_value = sum(p.qty * close for p in positions)
        total_asset = cash + holding_value
        eval_pnl = sum(p.qty * close - p.buy_amount - p.buy_fee for p in positions)

        # ── 자본 조정 적용 (당일 날짜의 조정을 일중 매매 처리 후 반영) ──
        # 시트 동작: 4/17 시드증액 → 4/17 매매는 옛 시드 사용, 4/20 첫 매수부터 새 시드
        # 우리 엔진: 4/17 매매 후 (records.append 전에) 조정 적용 → 다음 날 자동 반영
        # 또한 백테스트 마지막 날이 adj_date 인 경우도 정상 반영
        _adj_today = 0.0
        while _adj_idx < len(pending_adjs) and pending_adjs[_adj_idx][0] <= date:
            _, _amt = pending_adjs[_adj_idx]
            capital += _amt
            cash += _amt
            _adj_today += _amt
            _adj_idx += 1
        # capital/cash/total_asset 모두 조정 반영된 값으로 기록
        if _adj_today != 0:
            total_asset += _adj_today

        records.append({
            '날짜': date,
            '종가': close,
            '모드': mode,
            '매수주문가': buy_order_price,
            '매수체결': close if bought else None,
            '수량': buy_qty if bought else 0,
            '매도목표가': positions[-1].sell_target if bought else None,
            '손절예정일': positions[-1].stop_date if bought else None,
            '보유포지션수': len(positions),
            '분할수': divisions,
            '1회시드': seed_per_trade,
            '투자금': capital,
            '예수금': cash,
            '평가금': holding_value,
            '총자산': total_asset,
            '평가손익': eval_pnl,
            '당일실현': daily_realized,
            '누적실현': cumulative_realized,
            '갱신': renewal_happened,
            '갱신금액': renewal_amount if renewal_happened else 0,
            '누적매도': sell_count,
            '매도내역': sold_positions if sold_positions else None,
            '자본조정': _adj_today if _adj_today != 0 else 0,
        })

        prev_close = close

    return pd.DataFrame(records)


# ──────────────────────────────────────────────
# 5. 최적화 전용 경량 백테스트
# ──────────────────────────────────────────────

def run_backtest_fast(params: DSSParams,
                      soxl_daily: pd.DataFrame,
                      mode_series: pd.DataFrame,
                      start_date: str = "2024-01-02",
                      end_date: str = "2026-04-10",
                      capital_adj_history: list = None) -> dict:
    """
    최적화 전용 경량 백테스트.
    UI 출력용 데이터를 생성하지 않고, 핵심 지표만 반환.
    run_backtest 대비 2~3배 빠름.

    Args:
        capital_adj_history: 자본 조정 이력 (run_backtest와 동일)

    Returns: dict with 핵심 지표 or None
    """
    idx = soxl_daily.index
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)

    # 시작일 이전 종가
    before_idx = idx[idx < start_ts]
    if len(before_idx) > 0:
        prev_close = round(float(soxl_daily.loc[before_idx[-1], 'Close']), 2)
    else:
        prev_close = None

    # 거래일 범위 내 종가 배열 (numpy)
    range_mask = (idx >= start_ts) & (idx <= end_ts)
    trade_dates = idx[range_mask]
    n_days = len(trade_dates)
    if n_days == 0:
        return None

    # 종가를 numpy 배열로 사전 추출 (핵심 최적화)
    close_col = soxl_daily.columns.get_loc('Close')
    mask_bool = np.asarray(range_mask)
    closes = np.round(
        soxl_daily.values[mask_bool, close_col].astype(np.float64), 2
    )

    # 모드 맵: 0=SF, 1=AG (int 배열)
    mode_map = get_week_mode_map(mode_series, idx)
    mode_arr = np.array(
        [1 if mode_map.get(d, "AG") == "AG" else 0 for d in trade_dates],
        dtype=np.int8
    )

    # 손절일용: 거래일 → 인덱스 룩업
    all_dates_list = list(idx)
    date_to_idx = {}
    for ii, d in enumerate(all_dates_list):
        date_to_idx[d] = ii
    n_all = len(all_dates_list)

    # 파라미터 로컬 변수화 (속성 접근 오버헤드 제거)
    sf_div = params.sf_divisions
    sf_hold = params.sf_max_hold
    sf_buy = params.sf_buy_pct
    sf_sell = params.sf_sell_pct
    ag_div = params.ag_divisions
    ag_hold = params.ag_max_hold
    ag_buy = params.ag_buy_pct
    ag_sell = params.ag_sell_pct
    fee_r = params.fee_rate
    sec_fee = params.sec_fee_rate
    renew_period = params.renewal_period
    pcr_val = params.pcr
    lcr_val = params.lcr
    init_cap = params.initial_capital

    # 포지션: 리스트 of [sell_target, stop_idx, qty, buy_amt, buy_fee]
    positions = []

    capital = init_cap
    cash = init_cap
    sell_count = 0
    days_since_renew = 0  # 거래일수 카운터 (10거래일마다 갱신)
    pnl_since_renew = []
    cum_realized = 0.0

    # 자본 조정 이력 정렬
    pending_adjs = []
    if capital_adj_history:
        for _adj in capital_adj_history:
            try:
                _dt = pd.Timestamp(_adj.get("날짜"))
                _amt = float(_adj.get("조정금액", 0))
                if _amt != 0:
                    pending_adjs.append((_dt, _amt))
            except Exception:
                continue
        pending_adjs.sort(key=lambda x: x[0])
    _adj_idx = 0

    # 백테스트 시작일 이전 발생 조정은 미리 반영
    if n_days > 0:
        first_td = trade_dates[0]
        while _adj_idx < len(pending_adjs) and pending_adjs[_adj_idx][0] < first_td:
            _, _amt = pending_adjs[_adj_idx]
            capital += _amt
            cash += _amt
            _adj_idx += 1

    # MDD 계산용
    peak_asset = init_cap
    max_dd = 0.0
    final_asset = init_cap

    for i in range(n_days):
        cur_date = trade_dates[i]
        close = closes[i]
        is_ag = mode_arr[i]

        if is_ag:
            divisions = ag_div
            buy_pct = ag_buy
            sell_pct = ag_sell
            max_hold = ag_hold
        else:
            divisions = sf_div
            buy_pct = sf_buy
            sell_pct = sf_sell
            max_hold = sf_hold

        seed = capital / divisions
        cur_idx = date_to_idx.get(trade_dates[i])

        # ── 매도 처리 ──
        new_pos = []
        for pos in positions:
            if close >= pos[0] or (cur_idx is not None and cur_idx >= pos[1]):
                sell_amt = pos[2] * close
                pnl = (sell_amt - sell_amt * (fee_r + sec_fee)) - (pos[3] + pos[4])
                cash += sell_amt - sell_amt * (fee_r + sec_fee)
                cum_realized += pnl
                sell_count += 1
                pnl_since_renew.append(pnl)
            else:
                new_pos.append(pos)
        positions = new_pos

        # ── 투자금 갱신 (매 N 거래일마다, 매도 횟수 무관) ──
        days_since_renew += 1
        if days_since_renew >= renew_period:
            total_pnl = sum(pnl_since_renew) if pnl_since_renew else 0.0
            if total_pnl != 0:
                capital += total_pnl * (pcr_val if total_pnl > 0 else lcr_val)
            days_since_renew = 0
            pnl_since_renew = []

        # ── 매수 처리 ──
        if prev_close is not None and len(positions) < divisions:
            buy_order = math.floor(prev_close * (1 + buy_pct) * 100) / 100
            if close <= buy_order:
                buy_qty = int(seed / buy_order)
                if buy_qty > 0:
                    buy_amt = buy_qty * close
                    buy_fee = buy_amt * fee_r
                    if cash >= buy_amt + buy_fee:
                        cash -= (buy_amt + buy_fee)
                        sell_tgt = round(close * (1 + sell_pct), 2)
                        if cur_idx is not None and cur_idx + max_hold < n_all:
                            stop_idx = cur_idx + max_hold
                        else:
                            stop_idx = n_all - 1
                        positions.append([sell_tgt, stop_idx, buy_qty, buy_amt, buy_fee])

        # ── 총자산 & MDD ──
        holding = sum(p[2] * close for p in positions)
        total = cash + holding

        # ── 당일 자본 조정 (매매 후 반영) ──
        while _adj_idx < len(pending_adjs) and pending_adjs[_adj_idx][0] <= cur_date:
            _, _amt = pending_adjs[_adj_idx]
            capital += _amt
            cash += _amt
            total += _amt
            _adj_idx += 1

        if total > peak_asset:
            peak_asset = total
        dd = (total - peak_asset) / peak_asset if peak_asset > 0 else 0
        if dd < max_dd:
            max_dd = dd
        final_asset = total
        prev_close = close

    return {
        'final_asset': final_asset,
        'total_days': n_days,
        'mdd': max_dd,        # 음수 (예: -0.15 = -15%)
        'sell_count': sell_count,
        'cum_realized': cum_realized,
    }
