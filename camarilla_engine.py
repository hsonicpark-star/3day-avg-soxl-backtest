"""
카마릴라 피봇 매매법(추세추종 돌파) 백테스트 엔진
─────────────────────────────────────────────
전략 개요 (출처: 완두콩님 카마릴라 피봇 매매법 시트)
  - 저항선 = 전일종가 + (전일고가 - 전일저가) × 계수
      · 계수 0.55 = 1.1/2  → 카마릴라 4차 저항선(R4, 기본값)
      · 계수 0.275 = 1.1/4 → 3차 저항선(R3)
      · 계수 0.1833 = 1.1/6 → 2차 저항선(R2)
      · 계수 0.0917 = 1.1/12 → 1차 저항선(R1)
  - 매수: 장중 고가가 저항선을 돌파하면 그 순간(저항선 가격)에 매수.
          단, 시가가 이미 저항선 이상이면(장전 돌파/갭상승) 당일 시가(MOO)에 매수.
  - 매도: 다음 거래일 시가(MOO)로 보유수량 전부 매도.
  - 보유기간: 정확히 1거래일(돌파일 → 익일 시가). 매도 후 같은 날 재돌파 시 재매수 가능.
  - 자금: 매 회차 가용자금 전액 투입(복리), 정수 주식 단위.

피보나치 피봇 변형(옵션):
  - 저항선 = (전일고가+전일저가+전일종가)/3 + (전일고가-전일저가) × 계수
      · 계수 0.618 = 피보 2차(기본), 1.0 = 3차, 0.382 = 1차
"""

import math
from dataclasses import dataclass
from typing import Optional
import pandas as pd
import numpy as np
import yfinance as yf


# ──────────────────────────────────────────────
# 0. 데이터 로드
# ──────────────────────────────────────────────

def load_price_data(ticker: str, start: str = "2009-01-01",
                    end: str = "2026-12-31",
                    auto_adjust: bool = True) -> pd.DataFrame:
    """yfinance OHLC 로드.

    카마릴라는 SOXL 상장(2010-03) 이후 장기 데이터를 사용하며, 다수의
    분할(reverse split)을 거쳤으므로 분할조정된 연속가격(auto_adjust=True)이
    필요하다. 전략 수익률은 가격 스케일에 불변이므로 조정 시점이 달라도
    결과는 동일 수준으로 재현된다.

    미국장이 종료되지 않은 당일 intraday 데이터(확정되지 않은 종가)는 제외한다.
    """
    df = yf.download(ticker, start=start, end=end,
                     auto_adjust=auto_adjust, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.index = pd.to_datetime(df.index)
    # Close가 NaN인 행 제거 (yfinance가 장 마감 직후 NaN 반환하는 경우 방어)
    for col in ('Open', 'High', 'Low', 'Close'):
        if col in df.columns:
            df = df[df[col].notna()]
    if 'Volume' not in df.columns:
        df['Volume'] = 0.0
    df['Volume'] = df['Volume'].fillna(0.0)
    # 당일 미확정 intraday 데이터 제외
    try:
        now_est = pd.Timestamp.now(tz="America/New_York")
        market_close_buffer = now_est.replace(hour=16, minute=30,
                                              second=0, microsecond=0)
        if now_est < market_close_buffer:
            us_today = pd.Timestamp(now_est.date())
            df = df[df.index.normalize() < us_today]
    except Exception:
        pass
    return df[['Open', 'High', 'Low', 'Close', 'Volume']].sort_index()


# ──────────────────────────────────────────────
# 1. 파라미터
# ──────────────────────────────────────────────

# 카마릴라 저항선 계수 프리셋 (1.1/n)
CAMARILLA_LEVELS = {
    "R4 (1.1/2)": 1.1 / 2,    # 0.550  — 기본(돌파 추세추종)
    "R3 (1.1/4)": 1.1 / 4,    # 0.275
    "R2 (1.1/6)": 1.1 / 6,    # 0.1833
    "R1 (1.1/12)": 1.1 / 12,  # 0.0917
}
# 피보나치 피봇 계수 프리셋
FIB_LEVELS = {
    "피보 3차 (100%)": 1.000,
    "피보 2차 (61.8%)": 0.618,   # 기본
    "피보 1차 (38.2%)": 0.382,
}


@dataclass
class CamarillaParams:
    coef: float = 1.1 / 2            # 저항선 계수 (기본 R4 = 0.55)
    pivot_type: str = "camarilla"    # "camarilla" | "fib"
    initial_capital: float = 100000.0
    fee_rate: float = 0.0            # 매수/매도 수수료율
    sec_fee_rate: float = 0.0        # SEC fee (매도 시에만)
    position_pct: float = 1.0        # 회차당 투입 비중 (1.0 = 전액)
    # ── 최적화용 추가 파라미터 (기본값 = 원전략과 동일 동작) ──
    hold_days: int = 1               # 보유일수 (1 = 익일 시가 매도)
    profit_target_pct: float = 0.0   # 익절 % (보유 중 목표 도달 시, 0 = 미사용)
    stop_loss_pct: float = 0.0       # 손절 % (보유 중 이탈 시, 0 = 미사용)
    ma_filter_period: int = 0        # 추세필터: 전일종가 ≥ SMA(n) 일 때만 진입 (0 = 미사용)
    # ── 신호 기반 매수비중 조절 (애드온 오버레이용) ──
    signal: str = "none"             # "none" | "ma" | "fi" | "vol" | "vol3"
    signal_ma: int = 200             # ma 신호: SMA 기간
    fi_period: int = 13              # fi 신호: Force Index EMA 기간
    vol_period: int = 20             # vol 신호: 변동성 측정 기간
    vol_target: float = 0.50         # vol 신호: 연환산 목표 변동성
    vol3_period: int = 3             # vol3 신호: 표준편차식 σ 기간(거래일)
    vol3_target: float = 0.030       # vol3 신호: 일간 목표 σ(비연환산)
    risk_off_mult: float = 0.0       # 신호 위험구간 비중 배수 (0=스킵, 0.5=절반)

    def resistance(self, prev_high: float, prev_low: float,
                   prev_close: float) -> float:
        """전일 OHLC로 당일 저항선(돌파 기준가) 계산."""
        rng = prev_high - prev_low
        if self.pivot_type == "fib":
            pivot = (prev_high + prev_low + prev_close) / 3.0
            return pivot + rng * self.coef
        # camarilla
        return prev_close + rng * self.coef


def build_signal_mult(df: pd.DataFrame, params: 'CamarillaParams') -> Optional[pd.Series]:
    """전일 정보 기준 매수비중 배수(0~1) 시리즈 생성.

    - ma : 전일종가 ≥ 전일 SMA → 1.0, 아니면 risk_off_mult
    - fi : 전일 Force Index EMA > 0 → 1.0, 아니면 risk_off_mult
    - vol: 목표변동성/실현변동성(전일) 로 연속 축소 (상한 1.0). 20일 연환산(×√252, ddof=1)
    - vol3: 표준편차매매식 σ — 직전 N거래일 일간수익률 표준편차(ddof=0, 비연환산)로 축소
    NaN(데이터 부족) 구간은 1.0(중립)으로 둔다. signal="none"이면 None.
    """
    sig = params.signal
    if sig == "none":
        return None
    close = df['Close']
    ro = params.risk_off_mult
    if sig == "ma":
        sma = close.rolling(params.signal_ma).mean()
        m = (close >= sma).map(lambda x: 1.0 if x else ro)
        m = m.where(sma.notna(), 1.0)
        return m.shift(1)
    if sig == "fi":
        fi_raw = (close - close.shift(1)) * df['Volume']
        fi = fi_raw.ewm(span=params.fi_period, adjust=False).mean()
        m = (fi > 0).map(lambda x: 1.0 if x else ro)
        return m.shift(1)
    if sig == "vol":
        ret = close.pct_change()
        rv = ret.rolling(params.vol_period).std() * np.sqrt(252)
        m = (params.vol_target / rv).clip(upper=1.0)
        m = m.where(rv.notna() & (rv > 0), 1.0)
        # risk_off_mult을 하한으로 사용(완전 0 방지 옵션)
        if ro > 0:
            m = m.clip(lower=ro)
        return m.shift(1)
    if sig == "vol3":
        # 표준편차매매와 동일 로직: 직전 N거래일 일간수익률 σ(ddof=0, 비연환산)
        # rolling(N).std(ddof=0)는 오늘 포함 창 → shift(1)로 '전일까지 N일' 정렬
        ret = close.pct_change()
        rv = ret.rolling(params.vol3_period).std(ddof=0)
        m = (params.vol3_target / rv).clip(upper=1.0)
        m = m.where(rv.notna() & (rv > 0), 1.0)
        if ro > 0:
            m = m.clip(lower=ro)
        return m.shift(1)
    return None


# ──────────────────────────────────────────────
# 2. 백테스트 엔진 (일별 매매 로그)
# ──────────────────────────────────────────────

def run_backtest(params: CamarillaParams,
                 price: pd.DataFrame,
                 start_date: str = "2010-03-12",
                 end_date: str = "2026-12-31",
                 capital_adj_history: list = None) -> pd.DataFrame:
    """카마릴라 피봇 돌파 매매 백테스트.

    Returns: 일별 매매 로그 DataFrame.
    """
    df = price.copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)

    # 추세필터용 SMA(전일 기준) 사전계산
    use_ma = params.ma_filter_period and params.ma_filter_period > 1
    sma_prev = (df['Close'].rolling(params.ma_filter_period).mean().shift(1)
                if use_ma else None)
    sig_mult = build_signal_mult(df, params)  # 매수비중 배수 (None=신호 미사용)
    hold_days = max(1, int(params.hold_days))
    pt = params.profit_target_pct
    sl = params.stop_loss_pct

    # 시작일 이전 마지막 봉을 첫 저항선 계산용 prev로 사용
    before = df[df.index < start]
    prev_o = prev_h = prev_l = prev_c = None
    if len(before) > 0:
        last = before.iloc[-1]
        prev_h, prev_l, prev_c = float(last['High']), float(last['Low']), float(last['Close'])

    win = df[(df.index >= start) & (df.index <= end)]
    trading_days = win.index

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

    # 상태
    cash = params.initial_capital
    shares = 0
    entry_price = 0.0          # 보유 포지션 평균 매수가
    entry_date = None
    entry_invest = 0.0         # 매수에 투입한 금액(매수대금+수수료)
    days_held = 0              # 보유 경과 거래일수
    cumulative_realized = 0.0
    sell_count = 0
    win_count = 0

    records = []

    # 시작일 이전 조정 선반영
    if len(trading_days) > 0:
        first_td = trading_days[0]
        while _adj_idx < len(pending_adjs) and pending_adjs[_adj_idx][0] < first_td:
            cash += pending_adjs[_adj_idx][1]
            _adj_idx += 1

    for date in trading_days:
        o = float(win.loc[date, 'Open'])
        h = float(win.loc[date, 'High'])
        l = float(win.loc[date, 'Low'])
        c = float(win.loc[date, 'Close'])

        daily_realized = 0.0
        sold_qty = 0
        sell_price = None
        exit_reason = None

        # ── 1) 보유분 매도 판단 ──
        # 보유일수 도달 → 시가(MOO) 매도 / 그 전에 손절·익절 도달 시 청산
        if shares > 0:
            days_held += 1
            do_sell = False
            if days_held >= hold_days:
                sell_price = o
                exit_reason = "MOO"
                do_sell = True
            elif sl > 0 and l <= entry_price * (1 - sl):
                sell_price = min(o, entry_price * (1 - sl))  # 갭하락 시 시가 체결(현실적)
                exit_reason = "손절"
                do_sell = True
            elif pt > 0 and h >= entry_price * (1 + pt):
                sell_price = max(o, entry_price * (1 + pt))  # 갭상승 시 시가 체결
                exit_reason = "익절"
                do_sell = True

            if do_sell:
                gross = shares * sell_price
                sell_fee = gross * (params.fee_rate + params.sec_fee_rate)
                net_sell = gross - sell_fee
                pnl = net_sell - entry_invest
                cash += net_sell
                daily_realized += pnl
                cumulative_realized += pnl
                sell_count += 1
                if pnl > 0:
                    win_count += 1
                sold_qty = shares
                shares = 0
                entry_invest = 0.0
                days_held = 0

        # ── 2) 저항선 계산 ──
        resistance = None
        if prev_c is not None:
            resistance = params.resistance(prev_h, prev_l, prev_c)

        # 추세필터: 전일종가 ≥ 전일 SMA
        ma_ok = True
        if use_ma:
            _sp = sma_prev.loc[date] if date in sma_prev.index else None
            ma_ok = (_sp is not None) and (not pd.isna(_sp)) and (prev_c is not None) and (prev_c >= _sp)

        # ── 3) 매수 판단 (보유 중이 아닐 때만) ──
        bought = False
        buy_price = None
        buy_qty = 0
        if shares == 0 and resistance is not None and ma_ok:
            if o >= resistance:
                buy_price = o          # 장전(갭) 돌파 → 시가 매수
            elif h >= resistance:
                buy_price = resistance  # 장중 돌파 → 저항선 가격 매수
            if buy_price is not None and buy_price > 0:
                _mult = 1.0
                if sig_mult is not None and date in sig_mult.index:
                    _mv = sig_mult.loc[date]
                    _mult = float(_mv) if pd.notna(_mv) else 1.0
                budget = cash * params.position_pct * _mult
                buy_qty = int(budget / (buy_price * (1 + params.fee_rate)))
                if buy_qty > 0:
                    gross = buy_qty * buy_price
                    buy_fee = gross * params.fee_rate
                    cost = gross + buy_fee
                    if cost <= cash + 1e-9:
                        cash -= cost
                        shares = buy_qty
                        entry_price = buy_price
                        entry_date = date
                        entry_invest = cost
                        days_held = 0
                        bought = True

        # ── 4) 평가 ──
        holding_value = shares * c
        total_asset = cash + holding_value

        # ── 5) 당일 자본 조정 (매매 후 반영) ──
        _adj_today = 0.0
        while _adj_idx < len(pending_adjs) and pending_adjs[_adj_idx][0] <= date:
            cash += pending_adjs[_adj_idx][1]
            _adj_today += pending_adjs[_adj_idx][1]
            _adj_idx += 1
        if _adj_today != 0:
            total_asset += _adj_today

        records.append({
            '날짜': date,
            '시가': o, '고가': h, '저가': l, '종가': c,
            '저항선': resistance,
            '매도가': sell_price,
            '매도수량': sold_qty,
            '청산사유': exit_reason,
            '매수체결': buy_price if bought else None,
            '매수수량': buy_qty if bought else 0,
            '보유수량': shares,
            '예수금': cash,
            '평가금': holding_value,
            '총자산': total_asset,
            '당일실현': daily_realized,
            '누적실현': cumulative_realized,
            '누적매도': sell_count,
            '자본조정': _adj_today if _adj_today != 0 else 0,
        })

        prev_o, prev_h, prev_l, prev_c = o, h, l, c

    out = pd.DataFrame(records)
    if len(out):
        out.attrs['sell_count'] = sell_count
        out.attrs['win_count'] = win_count
    return out


# ──────────────────────────────────────────────
# 3. 최적화 전용 경량 백테스트
# ──────────────────────────────────────────────

def run_backtest_fast(params: CamarillaParams,
                      price: pd.DataFrame,
                      start_date: str = "2010-03-12",
                      end_date: str = "2026-12-31") -> Optional[dict]:
    """최적화 전용 경량 백테스트 — 핵심 지표만 반환."""
    idx = price.index
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)

    before = idx[idx < start_ts]
    if len(before) > 0:
        last = price.loc[before[-1]]
        prev_h, prev_l, prev_c = float(last['High']), float(last['Low']), float(last['Close'])
    else:
        prev_h = prev_l = prev_c = None

    mask = (idx >= start_ts) & (idx <= end_ts)
    mask_bool = np.asarray(mask)
    n = int(mask_bool.sum())
    if n == 0:
        return None

    o_col = price.columns.get_loc('Open')
    h_col = price.columns.get_loc('High')
    l_col = price.columns.get_loc('Low')
    c_col = price.columns.get_loc('Close')
    vals = price.values
    opens = vals[mask_bool, o_col].astype(np.float64)
    highs = vals[mask_bool, h_col].astype(np.float64)
    lows = vals[mask_bool, l_col].astype(np.float64)
    closes = vals[mask_bool, c_col].astype(np.float64)
    dates = idx[mask_bool]

    coef = params.coef
    is_fib = params.pivot_type == "fib"
    fee_r = params.fee_rate
    sec_fee = params.sec_fee_rate
    pos_pct = params.position_pct
    hold_days = max(1, int(params.hold_days))
    pt = params.profit_target_pct
    sl = params.stop_loss_pct

    use_ma = params.ma_filter_period and params.ma_filter_period > 1
    if use_ma:
        sma_prev_arr = price['Close'].rolling(
            params.ma_filter_period).mean().shift(1).values[mask_bool].astype(np.float64)
    else:
        sma_prev_arr = None

    _sig = build_signal_mult(price, params)
    if _sig is not None:
        sig_arr = _sig.values[mask_bool].astype(np.float64)
    else:
        sig_arr = None

    cash = params.initial_capital
    shares = 0
    entry_price = 0.0
    entry_invest = 0.0
    days_held = 0
    cum_realized = 0.0
    sell_count = 0
    win_count = 0

    peak = params.initial_capital
    max_dd = 0.0
    final_asset = params.initial_capital
    # 연도별 자산 스냅샷(연말)용
    year_end_asset = {}
    prev_year = None

    for i in range(n):
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]

        # 매도
        if shares > 0:
            days_held += 1
            sell_price = None
            if days_held >= hold_days:
                sell_price = o
            elif sl > 0 and l <= entry_price * (1 - sl):
                sell_price = min(o, entry_price * (1 - sl))  # 갭하락 시 시가 체결
            elif pt > 0 and h >= entry_price * (1 + pt):
                sell_price = max(o, entry_price * (1 + pt))  # 갭상승 시 시가 체결
            if sell_price is not None:
                gross = shares * sell_price
                net_sell = gross - gross * (fee_r + sec_fee)
                pnl = net_sell - entry_invest
                cash += net_sell
                cum_realized += pnl
                sell_count += 1
                if pnl > 0:
                    win_count += 1
                shares = 0
                entry_invest = 0.0
                days_held = 0

        # 저항선
        resistance = None
        if prev_c is not None:
            rng = prev_h - prev_l
            if is_fib:
                resistance = (prev_h + prev_l + prev_c) / 3.0 + rng * coef
            else:
                resistance = prev_c + rng * coef

        # 추세필터
        ma_ok = True
        if use_ma:
            _sp = sma_prev_arr[i]
            ma_ok = (not np.isnan(_sp)) and (prev_c is not None) and (prev_c >= _sp)

        # 매수 (보유 중이 아닐 때만)
        if shares == 0 and resistance is not None and ma_ok:
            buy_price = None
            if o >= resistance:
                buy_price = o
            elif h >= resistance:
                buy_price = resistance
            if buy_price is not None and buy_price > 0:
                _mult = 1.0
                if sig_arr is not None:
                    _mv = sig_arr[i]
                    _mult = _mv if not np.isnan(_mv) else 1.0
                budget = cash * pos_pct * _mult
                qty = int(budget / (buy_price * (1 + fee_r)))
                if qty > 0:
                    gross = qty * buy_price
                    cost = gross + gross * fee_r
                    if cost <= cash + 1e-9:
                        cash -= cost
                        shares = qty
                        entry_price = buy_price
                        entry_invest = cost
                        days_held = 0

        total = cash + shares * c
        if total > peak:
            peak = total
        dd = (total - peak) / peak if peak > 0 else 0.0
        if dd < max_dd:
            max_dd = dd
        final_asset = total

        yr = dates[i].year
        if prev_year is not None and yr != prev_year:
            year_end_asset[prev_year] = total
        prev_year = yr
        prev_h, prev_l, prev_c = h, l, c

    if prev_year is not None:
        year_end_asset[prev_year] = final_asset

    return {
        'final_asset': final_asset,
        'total_days': n,
        'mdd': max_dd,
        'sell_count': sell_count,
        'win_count': win_count,
        'win_rate': (win_count / sell_count) if sell_count else 0.0,
        'cum_realized': cum_realized,
    }


# ──────────────────────────────────────────────
# 4. 성과지표 유틸
# ──────────────────────────────────────────────

def compute_metrics(bt: pd.DataFrame, initial_capital: float,
                    trading_days_per_year: int = 252) -> dict:
    """일별 매매 로그에서 종합 성과지표 산출."""
    if bt is None or len(bt) == 0:
        return {}
    asset = bt['총자산'].astype(float).values
    final = asset[-1]
    n = len(asset)
    years = n / trading_days_per_year if n else 0

    total_return = final / initial_capital - 1.0
    cagr = (final / initial_capital) ** (1 / years) - 1.0 if years > 0 and final > 0 else 0.0

    # MDD
    peak = np.maximum.accumulate(asset)
    dd = (asset - peak) / peak
    mdd = dd.min() if len(dd) else 0.0

    # 일간 수익률
    rets = np.diff(asset) / asset[:-1]
    rets = rets[np.isfinite(rets)]
    ann_factor = trading_days_per_year
    sharpe = (rets.mean() / rets.std() * np.sqrt(ann_factor)) if len(rets) > 1 and rets.std() > 0 else 0.0
    downside = rets[rets < 0]
    sortino = (rets.mean() / downside.std() * np.sqrt(ann_factor)) if len(downside) > 1 and downside.std() > 0 else 0.0

    calmar = (cagr / abs(mdd)) if mdd < 0 else 0.0

    sell_count = int(bt.attrs.get('sell_count', (bt['매도수량'] > 0).sum()))
    win_count = int(bt.attrs.get('win_count', 0))
    win_rate = (win_count / sell_count) if sell_count else 0.0

    return {
        'final_asset': final,
        'total_return': total_return,
        'cagr': cagr,
        'mdd': mdd,
        'calmar': calmar,
        'sharpe': sharpe,
        'sortino': sortino,
        'sell_count': sell_count,
        'win_count': win_count,
        'win_rate': win_rate,
        'years': years,
    }


def yearly_returns(bt: pd.DataFrame, initial_capital: float) -> pd.DataFrame:
    """연도별 수익률(연말 총자산 기준 복리)."""
    if bt is None or len(bt) == 0:
        return pd.DataFrame()
    s = bt[['날짜', '총자산']].copy()
    s['날짜'] = pd.to_datetime(s['날짜'])
    s = s.set_index('날짜')
    yearly = s['총자산'].resample('YE').last()
    rows = []
    prev = initial_capital
    for ts, val in yearly.items():
        rows.append({'연도': ts.year, '수익률': val / prev - 1.0, '연말자산': val})
        prev = val
    return pd.DataFrame(rows)
