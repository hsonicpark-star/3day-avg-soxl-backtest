"""
Dual Sniper Pro 백테스트 엔진
- 공격/방어 2모드 슬롯(티어) 기반 그리드 매매
- 공격: FI 분기 LOC 매수, RSI 정규화 익절/보유기간, 슬롯별 독립 관리
- 방어: MA+종가 min LOC 매수, MA 돌파 전량청산, 고정 보유기간, 역피라미딩 비중
- 모드 전환: 주간 RSI(14, Wilder) 기반 (정확 규칙은 원전략 비공개 → 근사 규칙)

종목: SOXL

설계 메모 (스프레드시트 역설계 결과):
  · MA(n) = MA(3),  MA(5) = 5일 단순이평
  · 일RSI = RSI(14) Wilder,  wRSI = 주간종가 RSI(14) Wilder
  · 공격 매수조건 K = floor(전일종가 × (1+매수%), 2)   [전일 FI>0]
                    = floor(전일종가 × 0.999, 2)        [전일 FI≤0]
  · 공격 매도% = 0.1 + 2.9×(1-x)^(1/α_sell),  x=clip((매수RSI-35)/30, 0, 1)
  · 공격 보유일 = round(7 + 23×(1-x)^(1/α_hold))
  · 방어 매수조건 K = round(min(조건1, 조건2), 2)
        조건1 = (f1×Σ직전 MA기준-1일 종가) / (MA기준 - f1),  f1 = 1+(매수조건1%/100)
        조건2 = 전일종가 × (1 + 매수조건2%/100)
  · 방어 매도조건 = round((fs×Σ직전 MA기준-1일 종가) / (MA기준 - fs), 2),  fs = 1+(매도%/100)
  · 수량 = floor(할당금 / 주문가)
  · 할당금 = 현재현금 × (해당티어 비중 / 남은티어 비중합)
"""

import math
import os
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd
import numpy as np

# 번들된 원전략 실제 모드 (2016~2026, 로케트셋 역설계)
_MODES_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dual_sniper_modes.csv')


def load_original_modes() -> pd.DataFrame:
    """번들된 원전략 실제 모드 DataFrame[date, mode]. 없으면 빈 DF."""
    if not os.path.exists(_MODES_CSV):
        return pd.DataFrame(columns=['date', 'mode'])
    df = pd.read_csv(_MODES_CSV)
    df['date'] = pd.to_datetime(df['date'])
    return df


def build_original_mode_map(prices: pd.DataFrame, extra_modes: dict = None) -> dict:
    """원전략 실제 모드(번들) + 추가 일별모드(공유) → 거래일별 mode_map.
    번들에 없는 날(번들 종료 이후)은 직전 알려진 모드를 carry-forward."""
    df = load_original_modes()
    by_date = {pd.Timestamp(d).normalize(): m for d, m in zip(df['date'], df['mode'])}
    if extra_modes:
        for d, m in extra_modes.items():
            if m in ('공격', '방어'):
                by_date[pd.Timestamp(d).normalize()] = m
    dates = pd.to_datetime(prices.index)
    mode_map = {}
    last = '방어'
    for d in dates:
        dn = pd.Timestamp(d).normalize()
        if dn in by_date:
            last = by_date[dn]
        mode_map[d] = last
    return mode_map


# ──────────────────────────────────────────────
# 데이터 로드 (yfinance, 미조정 raw — 스프레드시트와 일치)
# ──────────────────────────────────────────────

_SHARE_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dual_sniper_share.json')


def _load_db_closes():
    """보조 종가 DB(구글시트 DB탭, E=날짜 'YY.MM.DD(요일)' / F=종가) → Series[date→close].
    야후 일봉 지연/누락 보충용. 인증 실패·미설정 시 None."""
    try:
        import json as _json
        with open(_SHARE_JSON, encoding='utf-8') as f:
            cfg = _json.load(f)
        url = cfg.get('price_db_url', '').strip()
        tab = cfg.get('price_db_tab', 'DB').strip() or 'DB'
        if not url:
            return None
        gc = None
        # 1) GitHub Actions 등 env 인증
        try:
            import json as _j, gspread
            from google.oauth2.service_account import Credentials
            info = _j.loads(os.environ['GCP_SERVICE_ACCOUNT_JSON'])
            creds = Credentials.from_service_account_info(
                info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
            gc = gspread.authorize(creds)
        except Exception:
            pass
        # 2) 앱 공용 클라이언트 (Cloud st.secrets / 로컬 service_account.json)
        if gc is None:
            try:
                from common.config import _get_gspread_client
                gc = _get_gspread_client()
            except Exception:
                return None
        vals = gc.open_by_url(url).worksheet(tab).get_all_values()
        out = {}
        for r in vals:
            if len(r) < 6:
                continue
            d, c = r[4].strip(), r[5].strip()
            if not d or not c:
                continue
            try:
                dt = pd.to_datetime('20' + d.split('(')[0].strip(), format='%Y.%m.%d')
                out[dt.normalize()] = float(c.replace(',', ''))
            except Exception:
                continue
        return pd.Series(out).sort_index() if out else None
    except Exception:
        return None


def load_price_data(ticker: str = "SOXL", start: str = "2009-01-01",
                    end: str = "2027-01-01") -> pd.DataFrame:
    """yfinance 미조정(auto_adjust=False) 종가 로드 → DataFrame[index=date, 'close'].
    당일 장중(intraday) 미확정 데이터는 제외 (확정 종가만 사용)."""
    import yfinance as yf
    df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.index = pd.to_datetime(df.index)
    # 미국장 마감(16:30 ET) 이전이면 당일 intraday 제거
    try:
        now_est = pd.Timestamp.now(tz="America/New_York")
        close_buf = now_est.replace(hour=16, minute=30, second=0, microsecond=0)
        if now_est < close_buf:
            df = df[df.index.normalize() < pd.Timestamp(now_est.date())]
        today_est = now_est.date()
    except Exception:
        today_est = None
    out = df[['Close']].rename(columns={'Close': 'close'}).copy()
    # ── 야후 일봉 지연/누락 보정 (마감된 지난 거래일만 대상 — 오늘 행은 위에서 제거됨) ──
    # 1순위: 보조 종가 DB(구글시트) → 2순위: yf fast_info 최종가
    try:
        if ticker == "SOXL" and today_est is not None and len(out):
            _tail_nan = pd.isna(out['close'].iloc[-1]) and out.index[-1].date() < today_est
            _nd = next_trading_days(out.index[-1], 1)
            _row_missing = len(_nd) > 0 and _nd[0].date() < today_est  # 마감된 세션 행 자체가 없음
            if _tail_nan or _row_missing:
                db = _load_db_closes()
                if db is not None:
                    # NaN 채움 + 야후에 없는 마감 거래일 행 추가 (오늘 이전만)
                    for d, c in db.items():
                        if d.date() >= today_est:
                            continue
                        if d in out.index:
                            if pd.isna(out.loc[d, 'close']):
                                out.loc[d, 'close'] = c
                        elif d > out.index[-1]:
                            out.loc[d] = c
                    out = out.sort_index()
                elif _tail_nan:   # DB 불가 시 최후 폴백
                    lp = yf.Ticker(ticker).fast_info.last_price
                    if lp and float(lp) > 0:
                        out.iloc[-1, out.columns.get_loc('close')] = float(lp)
    except Exception:
        pass
    out = out[out['close'].notna()]   # 남은 NaN(미확정) 종가 행 제거
    return out


# ──────────────────────────────────────────────
# 0. 파라미터
# ──────────────────────────────────────────────

@dataclass
class DualSniperParams:
    # ── 공격모드 ──
    ag_divisions: int = 6
    ag_buy_pct: float = 8.0          # 매수조건(종가%): FI>0일 때 전일종가 대비 +%
    ag_sell_alpha: float = 0.4       # 매도조건 정규화 계수 α
    ag_hold_alpha: float = 2.0       # 보유기간 정규화 계수 α
    # ── 방어모드 ──
    sf_divisions: int = 5
    sf_buy_pct1: float = -0.6        # 매수조건1(MA%)
    sf_buy_pct2: float = 5.5         # 매수조건2(종가%)
    sf_sell_pct: float = 0.7         # 매도조건(MA%)
    sf_ma_base: int = 3              # MA기준 (방어 매수/매도 MA 일수)
    sf_hold: int = 8                 # 방어 보유기간(거래일)
    sf_tier_weights: tuple = (6, 13, 20, 27, 34)  # 티어별 매수비중(%) - 오름차순
    # ── 공통 ──
    initial_capital: float = 10000.0
    fee_rate: float = 0.0004         # 매수/매도 수수료 0.04%
    sec_fee_rate: float = 0.0000278  # SEC fee (매도 시에만)
    # 공격 매수 체결 부등호: True면 close <= K, False면 close < K
    ag_buy_inclusive: bool = True
    sf_buy_inclusive: bool = True


# ──────────────────────────────────────────────
# 1. 지표 계산
# ──────────────────────────────────────────────

def calc_ma(closes: np.ndarray, period: int) -> np.ndarray:
    """단순이동평균. 데이터 부족 구간은 NaN."""
    out = np.full(len(closes), np.nan)
    csum = np.cumsum(np.insert(closes, 0, 0.0))
    for i in range(period - 1, len(closes)):
        out[i] = (csum[i + 1] - csum[i + 1 - period]) / period
    return out


def calc_rsi_wilder(closes: np.ndarray, period: int = 14) -> np.ndarray:
    """RSI(period), Wilder 지수평활 방식. 워밍업 구간은 NaN."""
    n = len(closes)
    out = np.full(n, np.nan)
    if n < period + 1:
        return out
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    avg_gain = gains[:period].mean()
    avg_loss = losses[:period].mean()
    # out[period] = RSI of the (period)-th close
    if avg_loss == 0:
        out[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        out[period] = 100 - 100 / (1 + rs)

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            out[i + 1] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[i + 1] = 100 - 100 / (1 + rs)
    return out


def calc_weekly_rsi_series(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """일봉 → 주봉(금요일 마지막 종가) RSI(14) Wilder.
    반환: DataFrame[week_end, close, rsi]"""
    s = df['close'].copy()
    s.index = pd.to_datetime(df.index)
    weekly = s.resample('W-FRI').last().dropna()
    rsi = calc_rsi_wilder(weekly.values.astype(float), period)
    return pd.DataFrame({
        'week_end': weekly.index,
        'close': weekly.values,
        'rsi': rsi,
    })


# ──────────────────────────────────────────────
# 2. 모드 전환 (근사 규칙 — 원전략 비공개)
# ──────────────────────────────────────────────

def determine_mode_wrsi(rr: float, r: float, prev_mode: str) -> str:
    """주간 RSI 기반 모드 판정 (DSS 차용 근사 규칙).
    rr: 2주 전 wRSI,  r: 1주 전 wRSI,  prev_mode: '공격'/'방어'
    원전략 실제 모드와 약 84% 일치 (전환 시점 100%)."""
    if rr <= 35 and rr < r:
        return '공격'
    elif rr >= 40 and rr < 50 and rr > r:
        return '방어'
    elif rr <= 50 and r > 50:
        return '공격'
    elif rr >= 50 and r < 50:
        return '방어'
    elif rr >= 50 and rr < 60 and rr < r:
        return '공격'
    elif rr > 65 and rr > r:
        return '방어'
    else:
        return prev_mode


def build_auto_mode_map(prices: pd.DataFrame,
                        ma_weeks: int = 36, peak_thr: float = 66.0,
                        dn: float = 42.0, rsi_period: int = 14,
                        init_mode: str = '방어') -> dict:
    """자체 설계 모드 규칙 → 일별 mode_map. (원전략 비공개 규칙 대체용)

    하이브리드 규칙 (전주 확정 주봉 기준, 룩어헤드 없음):
      · 추세필터: 주봉종가 > N주 이동평균 → 공격 후보
      · 방어 오버라이드(우선): wRSI < dn (레벨 이탈) OR
                              (wRSI 하락 AND 직전 wRSI > peak_thr) (천장 신호)
      · 최종: 방어신호면 방어, 아니고 추세상승이면 공격, 그 외 방어

    백테스트(2016~2026, SOXL): CAGR 76% / MDD -29% / Calmar 2.64
    (train≈test 2.68≈2.74로 견고. 원전략 실제모드 Calmar 3.69에는 못 미침 —
     위기연도 정밀 타이밍은 원전략 비공개 규칙의 고유 강점.)
    """
    df = prices.copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    wclose = df['close'].resample('W-FRI').last().dropna()
    wvals = wclose.values.astype(float)
    wrsi = calc_rsi_wilder(wvals, rsi_period)
    wma = pd.Series(wvals).rolling(ma_weeks).mean().values

    modes = []
    m = init_mode
    for i in range(len(wvals)):
        j = i - 1 if i >= 1 else 0          # 전주 확정값 사용
        v = wrsi[i - 1] if i >= 1 else wrsi[0]
        vp = wrsi[i - 2] if i >= 2 else v
        c, mm = wvals[j], wma[j]
        trend_up = (not np.isnan(mm)) and c > mm
        crash = ((not np.isnan(v) and v < dn)
                 or (not np.isnan(v) and not np.isnan(vp)
                     and v < vp and vp > peak_thr))
        if np.isnan(v):
            pass                             # 워밍업: 직전 모드 유지
        elif crash:
            m = '방어'
        elif trend_up:
            m = '공격'
        else:
            m = '방어'
        modes.append(m)

    ms = pd.DataFrame({'week_end': wclose.index, 'mode': modes})
    return map_modes_to_days(ms, df.index)


def forward_mode(prices: pd.DataFrame, ma_weeks: int = 36, peak_thr: float = 66.0,
                 dn: float = 42.0, prev_mode: str = '방어', rsi_period: int = 14) -> str:
    """다음 거래 세션(다가오는 주)의 모드를 '직전 확정 주봉' 기준으로 판정.

    백테스트의 주별 모드는 1주 지연(전주 기준)이 정상이나, 실거래 주문표는
    가장 최근 확정된 주봉(지난주)과 그 전주(지지난주)로 다음 세션 모드를 산출해야
    불필요한 추가 지연이 없다. (룩어헤드 아님 — 모두 확정된 과거 주봉 사용)"""
    df = prices.copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    wclose = df['close'].resample('W-FRI').last().dropna()
    # 미완성 현재 주봉 제외 (장중·주중 실행 시 마지막 W-FRI 라벨이 미래면 미완성)
    if len(wclose) > 0 and wclose.index[-1].date() > df.index[-1].date():
        wclose = wclose.iloc[:-1]
    wv = wclose.values.astype(float)
    if len(wv) < 2:
        return prev_mode
    wrsi = calc_rsi_wilder(wv, rsi_period)
    wma = pd.Series(wv).rolling(ma_weeks).mean().values
    v, vp = wrsi[-1], wrsi[-2]      # 지난주, 지지난주
    c, m = wv[-1], wma[-1]
    if np.isnan(v):
        return prev_mode
    trend_up = (not np.isnan(m)) and c > m
    crash = (v < dn) or (not np.isnan(vp) and v < vp and vp > peak_thr)
    if crash:
        return '방어'
    if trend_up:
        return '공격'
    return '방어'


def build_mode_series(weekly_rsi_df: pd.DataFrame,
                      initial_mode: str = '방어') -> pd.DataFrame:
    """주간 RSI 시리즈 → 주별 모드 시리즈."""
    df = weekly_rsi_df.copy().reset_index(drop=True)
    modes = []
    for i in range(len(df)):
        if i < 2 or pd.isna(df.loc[i - 2, 'rsi']) or pd.isna(df.loc[i - 1, 'rsi']):
            modes.append(initial_mode)
        else:
            modes.append(determine_mode_wrsi(
                df.loc[i - 2, 'rsi'], df.loc[i - 1, 'rsi'], modes[i - 1]))
    df['mode'] = modes
    return df


def map_modes_to_days(mode_series: pd.DataFrame,
                      dates: pd.DatetimeIndex) -> dict:
    """주별 모드를 각 거래일에 매핑. week_end(금) 확정 모드를 다음 주 월~금에 적용.

    원전략: 금요일 주봉 확정 → 그 주의 (rr,r)로 판정 → 다음 주 적용.
    여기서는 week_end 행의 mode를 '그 주 거래일'에 적용 (build 시 i-1,i-2 사용으로
    이미 1주 시프트 반영됨)."""
    mode_map = {}
    rows = mode_series.to_dict('records')
    for rec in rows:
        friday = pd.Timestamp(rec['week_end'])
        monday = friday - pd.Timedelta(days=4)
        for d in dates[(dates >= monday) & (dates <= friday)]:
            mode_map[d] = rec['mode']
    return mode_map


# ──────────────────────────────────────────────
# 3. 포지션
# ──────────────────────────────────────────────

@dataclass
class Position:
    mode: str               # '공격' / '방어'
    tier: int               # 슬롯 번호 (1..divisions)
    buy_date: pd.Timestamp
    buy_price: float        # 실제 체결가(종가)
    order_price: float      # 매수주문가 K
    qty: int
    buy_amount: float       # qty × buy_price
    buy_fee: float
    sell_target: Optional[float]  # 공격: 고정 익절가 / 방어: None(MA 그룹청산)
    buy_rsi: float
    stop_idx: int           # 손절(MOC) 거래일 인덱스
    hold_days: int


# ──────────────────────────────────────────────
# 4. 할당금 / 주문가 / 정규화 헬퍼
# ──────────────────────────────────────────────

def _alloc_amount(cash: float, weights: list, occupied_tiers: set,
                  target_tier: int) -> float:
    """할당금 = 현재현금 × (target 티어 비중 / 남은(미보유) 티어 비중합).
    occupied_tiers: 현재 보유 중인 티어 번호 집합 (1-based).
    target_tier: 이번에 매수할 티어 번호 (1-based)."""
    remaining = sum(weights[t - 1] for t in range(1, len(weights) + 1)
                    if t not in occupied_tiers)
    if remaining <= 0:
        return 0.0
    return cash * (weights[target_tier - 1] / remaining)


def _ag_sell_pct(rsi: float, alpha: float) -> float:
    """공격 매도% (소수, 예: 0.003 = 0.3%)."""
    x = max(0.0, min(1.0, (rsi - 35) / 30))
    return (0.1 + 2.9 * (1 - x) ** (1 / alpha)) / 100.0


def _ag_hold_days(rsi: float, alpha: float) -> int:
    """공격 보유기간(거래일, 반올림). 2026-06-30 스펙: x=(RSI-35)/35 (매도조건 /30과 분모 다름)."""
    x = max(0.0, min(1.0, (rsi - 35) / 35))
    return int(round(7 + 23 * (1 - x) ** (1 / alpha)))


def _lowest_empty_tier(occupied: set, divisions: int) -> Optional[int]:
    """가장 낮은 빈 슬롯 번호 (1..divisions). 없으면 None."""
    for t in range(1, divisions + 1):
        if t not in occupied:
            return t
    return None


# ──────────────────────────────────────────────
# 5. 백테스트 엔진
# ──────────────────────────────────────────────

def run_backtest(prices: pd.DataFrame,
                 params: DualSniperParams,
                 mode_map: dict = None,
                 mode_override: dict = None,
                 indicators: dict = None,
                 start_date=None,
                 skip_buy_dates: set = None,
                 light: bool = False,
                 return_trades: bool = False,
                 return_state: bool = False,
                 liquidate_at_end: bool = True,
                 capital_injections: dict = None) -> pd.DataFrame:
    """Dual Sniper Pro 백테스트.

    Args:
        prices: DataFrame[index=date, 'close'], 거래일 정렬. (지표 워밍업 위해
                백테스트 시작 이전 데이터도 포함되어 있어야 함)
        params: 전략 파라미터
        mode_map: {date: '공격'/'방어'} 일별 모드. None이면 wRSI 규칙 자동 생성.
        mode_override: {date: 모드} 수동 오버라이드 (mode_map보다 우선)
        indicators: {'drsi':arr, 'ma_n':arr, 'ma5':arr} 사전계산 지표 (검증용).
                    제공 시 내부 계산 대신 사용 (워밍업 격리).

    Returns: DataFrame 일별 매매로그
    """
    df = prices.copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    dates = df.index
    closes = df['close'].values.astype(float)
    n = len(closes)

    # 지표 (사전계산 우선)
    if indicators is not None:
        ma_n = indicators['ma_n']
        ma5 = indicators['ma5']
        drsi = indicators['drsi']
    else:
        ma_n = calc_ma(closes, params.sf_ma_base)   # 방어 MA기준 (=MA3)
        ma5 = calc_ma(closes, 5)
        drsi = calc_rsi_wilder(closes, 14)

    # 모드 맵 준비 (미지정 시 자체 설계 하이브리드 규칙 사용)
    if mode_map is None:
        mode_map = build_auto_mode_map(df)
    if mode_override:
        mode_override = {pd.Timestamp(k): v for k, v in mode_override.items()}

    weights = list(params.sf_tier_weights)
    ag_weights = [1] * params.ag_divisions  # 공격: 균등 비중

    cash = params.initial_capital
    positions: list[Position] = []
    cum_realized = 0.0
    sell_count = 0
    trades = []

    # 좌당가(기준가) 방식 손익률: 시작 좌당가 1.0, 좌수 = 시작자본. 증액/감액 시 당시 좌당가로 좌수 가감.
    units = float(params.initial_capital) if params.initial_capital else 1.0
    prev_total_asset = float(params.initial_capital)

    start_ts = pd.Timestamp(start_date) if start_date is not None else None

    # 자본 조정(증액/감액): '미국장 종료 후' 기준 → 조정일 다음 거래일부터 가용 (post-close)
    inject_on = {}
    if capital_injections:
        for d, amt in capital_injections.items():
            dt = pd.Timestamp(d).normalize()
            cand = dates[dates > dt]                       # 종료 후 → 다음 거래일부터
            if start_ts is not None:
                cand = cand[cand >= start_ts]
            if len(cand):
                inject_on[cand[0]] = inject_on.get(cand[0], 0.0) + float(amt)

    records = []
    prev_close = None
    prev_fi = None       # 전일 FI (+/-)
    prev_ma5 = None

    for i in range(n):
        date = dates[i]
        close = round(float(closes[i]), 6)

        # 워밍업: start_date 이전이면 지표/전일값만 갱신, 매매·기록 없음
        trading = (start_ts is None) or (date >= start_ts)

        # 자본 조정 현금 투입/회수 (장 시작 시점) + 좌수 가감 (직전일 종가 기준 좌당가)
        if date in inject_on:
            _amt = inject_on[date]
            _up = (prev_total_asset / units) if units > 0 else 1.0
            units += _amt / _up if _up > 0 else 0.0
            cash += _amt

        mode = (mode_override.get(date) if mode_override else None) \
            or mode_map.get(date, '방어')

        cur_ma_n = ma_n[i]
        cur_ma5 = ma5[i]
        cur_rsi = drsi[i]

        # FI: 당일 등락 부호 (오늘 종가 vs 전일 종가)
        if prev_close is None:
            fi = None
        else:
            fi = '+' if close > prev_close else '-'

        # ── 매수주문가 K 계산 (전일 데이터 기준, 당일 모드) ──
        order_price = None
        if prev_close is not None:
            if mode == '공격':
                if prev_fi == '+':
                    order_price = math.floor(prev_close * (1 + params.ag_buy_pct / 100) * 100) / 100
                else:  # prev_fi '-' or None
                    order_price = math.floor(prev_close * 0.999 * 100) / 100
            else:  # 방어
                # 조건1: MA 기반, 조건2: 종가 기반 → min
                if i >= params.sf_ma_base - 1:
                    f1 = 1 + params.sf_buy_pct1 / 100
                    prev_sum = closes[i - (params.sf_ma_base - 1): i].sum()  # 직전 (MA기준-1)일
                    cond1 = (f1 * prev_sum) / (params.sf_ma_base - f1)
                    cond2 = prev_close * (1 + params.sf_buy_pct2 / 100)
                    order_price = round(min(cond1, cond2), 2)

        if not trading:
            prev_close = close
            prev_fi = fi
            prev_ma5 = cur_ma5
            continue

        # ── 매도 처리 ──
        # 1) 각 포지션 매도 사유 판정
        #    - MOC: i >= stop_idx
        #    - 공격 LOC: close >= sell_target
        #    - 방어 LOC: close >= 방어 매도조건 (그룹 전량)
        moc_today = any(i >= p.stop_idx for p in positions)

        # 방어 그룹 매도가 (전체 정밀도 — 스프레드시트는 표시만 반올림)
        sf_sell_price = None
        if i >= params.sf_ma_base - 1 and prev_close is not None:
            fs = 1 + params.sf_sell_pct / 100
            prev_sum = closes[i - (params.sf_ma_base - 1): i].sum()
            sf_sell_price = (fs * prev_sum) / (params.sf_ma_base - fs)

        # 1티어 보류 조건: 전일종가 > 전일MA(5) → 공격 1티어 LOC 매도 보류 (MOC 제외)
        ag_hold_tier1 = (prev_close is not None and prev_ma5 is not None
                         and prev_close > prev_ma5)

        # 매수 티어/할당금은 '장 시작(매도 처리 전)' 상태 기준 (LOC 주문 장전 제출)
        occupied_before = set(p.tier for p in positions if p.mode == mode)
        buy_tier_planned = _lowest_empty_tier(
            occupied_before,
            params.ag_divisions if mode == '공격' else params.sf_divisions)
        cash_start = cash  # 당일 매도 대금 반영 전 현금

        sold = []
        remaining = []
        daily_realized = 0.0
        sell_reasons = []

        for p in positions:
            is_moc = i >= p.stop_idx
            do_sell = False
            reason = None

            if is_moc:
                do_sell = True
                reason = 'MOC'
            elif moc_today:
                # MOC 매도 존재 → 다른 LOC 매도 보류
                do_sell = False
            elif p.mode == '공격':
                if p.sell_target is not None and close >= p.sell_target:
                    # 1티어 보류: 전일종가>전일MA5 이면 슬롯1번(tier==1)의 LOC 매도 보류
                    if ag_hold_tier1 and p.tier == 1:
                        do_sell = False
                    else:
                        do_sell = True
                        reason = f'T{p.tier}'
            else:  # 방어
                if sf_sell_price is not None and close >= sf_sell_price:
                    # 공통사항(2026-06-30 스펙): 전일종가>전일MA5면 슬롯1 매도 보류 (MOC 제외)
                    if ag_hold_tier1 and p.tier == 1:
                        do_sell = False
                    else:
                        do_sell = True
                        reason = 'MA'

            if do_sell:
                sell_amount = p.qty * close
                sell_fee = sell_amount * (params.fee_rate + params.sec_fee_rate)
                net_sell = sell_amount - sell_fee
                pnl = net_sell - (p.buy_amount + p.buy_fee)
                cash += net_sell
                daily_realized += pnl
                cum_realized += pnl
                sell_count += 1
                sold.append(p)
                sell_reasons.append(reason)
                trades.append({
                    '매수일': p.buy_date, '매도일': date, '모드': p.mode,
                    '티어': p.tier, '매수가': round(p.buy_price, 4),
                    '매도가': round(close, 4), '수량': p.qty,
                    '실현손익': round(pnl, 2),
                    '수익률(%)': round((close / p.buy_price - 1) * 100, 2),
                    '사유': reason,
                    '보유일': int((date - p.buy_date).days),
                })
            else:
                remaining.append(p)

        positions = remaining

        # ── 매수 처리 (1티어/일) ──
        bought = None
        if order_price is not None:
            if mode == '공격':
                wts = ag_weights
                inclusive = params.ag_buy_inclusive
            else:
                wts = weights
                inclusive = params.sf_buy_inclusive

            # 티어는 장 시작 기준(occupied_before)으로 사전 결정
            tier = buy_tier_planned

            fill = (close <= order_price) if inclusive else (close < order_price)
            if skip_buy_dates and date in skip_buy_dates:
                fill = False
            if tier is not None and fill:
                # 할당금: 장 시작 현금 × (티어비중 / 남은 비중합), 둘 다 장 시작 기준
                alloc = _alloc_amount(cash_start, wts, occupied_before, tier)
                qty = int(math.floor(alloc / order_price)) if order_price > 0 else 0
                if qty > 0:
                    buy_amount = qty * close
                    buy_fee = buy_amount * params.fee_rate
                    if cash >= buy_amount + buy_fee:
                        cash -= (buy_amount + buy_fee)
                        if mode == '공격':
                            spct = _ag_sell_pct(cur_rsi, params.ag_sell_alpha) \
                                if not np.isnan(cur_rsi) else 0.001
                            sell_target = round(close * (1 + spct), 2)
                            hd = _ag_hold_days(cur_rsi, params.ag_hold_alpha) \
                                if not np.isnan(cur_rsi) else 7
                        else:
                            sell_target = None  # MA 그룹청산
                            hd = params.sf_hold
                        # 끝에서 강제청산(MOC)할지: 성과 백테스트=True(마지막날 평가청산),
                        # 현재 보유상태 carry(build_today_orders)=False → 미만기 포지션 유지
                        stop_idx = min(i + hd, n - 1) if liquidate_at_end else (i + hd)
                        p = Position(
                            mode=mode, tier=tier, buy_date=date,
                            buy_price=close, order_price=order_price, qty=qty,
                            buy_amount=buy_amount, buy_fee=buy_fee,
                            sell_target=sell_target, buy_rsi=cur_rsi,
                            stop_idx=stop_idx, hold_days=hd,
                        )
                        positions.append(p)
                        bought = p

        # ── 평가 ──
        hold_value = sum(p.qty * close for p in positions)
        total_asset = cash + hold_value
        if trading:
            prev_total_asset = total_asset   # 다음날 증액 시 좌당가 산정 기준
        unit_price = (total_asset / units) if units > 0 else 1.0
        ret_pct = (unit_price - 1.0) * 100   # 좌당가 기준 누적 손익률 (증액/감액 영향 제거)

        if light:
            records.append((date, total_asset))
            prev_close = close
            prev_fi = fi
            prev_ma5 = cur_ma5
            continue

        hold_qty = sum(p.qty for p in positions)
        _avg = round(sum(p.buy_amount for p in positions) / hold_qty, 4) if hold_qty else 0.0
        if mode == '방어':
            _sell_loc = round(sf_sell_price, 2) if sf_sell_price else None
        else:
            _tgts = [p.sell_target for p in positions if p.sell_target]
            _sell_loc = round(min(_tgts), 2) if _tgts else None

        records.append({
            '날짜': date,
            '종가': round(close, 2),
            '모드': mode,
            'FI': fi,
            '일RSI': round(cur_rsi, 1) if not np.isnan(cur_rsi) else None,
            'MA3': round(cur_ma_n, 2) if not np.isnan(cur_ma_n) else None,
            'MA5': round(cur_ma5, 2) if not np.isnan(cur_ma5) else None,
            '매수주문가': order_price,
            '매도주문가': _sell_loc,
            '매수티어': f'{"공" if mode=="공격" else "방"}T{bought.tier}' if bought else '',
            '매수량': bought.qty if bought else 0,
            '매수가': bought.buy_price if bought else None,
            '평단가': _avg,
            '보유일': bought.hold_days if bought else None,
            '매도사유': ','.join(f'T{p.tier}' if r and r.startswith('T') else (r or '')
                              for p, r in zip(sold, sell_reasons)) if sold else '',
            '매도량': sum(p.qty for p in sold) if sold else 0,
            '당일실현': round(daily_realized, 2),
            '보유수량': hold_qty,
            '보유티어수': len(positions),
            '평가금': round(hold_value, 2),
            '현금': round(cash, 2),
            '총자산': round(total_asset, 2),
            '누적실현': round(cum_realized, 2),
            '좌당가': round(unit_price, 6),
            '손익률(%)': round(ret_pct, 2),
        })

        prev_close = close
        prev_fi = fi
        prev_ma5 = cur_ma5

    if return_state:
        last_close = float(closes[-1]) if n > 0 else None
        state = {
            'positions': positions,
            'cash': cash,
            'last_date': dates[-1] if n > 0 else None,
            'last_close': last_close,
            'c1': last_close,
            'c2': float(closes[-2]) if n > 1 else None,
            'prev_fi': prev_fi,
            'last_ma5': float(ma5[-1]) if n > 0 and not np.isnan(ma5[-1]) else None,
            'last_mode': mode_map.get(dates[-1], '방어') if n > 0 else '방어',
            'cum_realized': cum_realized,
            'total_asset': cash + sum(p.qty * last_close for p in positions) if n > 0 else cash,
            'trades': trades,           # 체결 내역 (매수일/매도일/모드/티어/수량/실현손익)
            'sell_count': sell_count,
            'units': units,             # 좌수 (좌당가 손익률용)
            'unit_price': (cash + sum(p.qty * last_close for p in positions)) / units
            if units > 0 and n > 0 else 1.0,
        }
        log_df = pd.DataFrame(records)
        return log_df, state

    if light:
        return pd.DataFrame(records, columns=['날짜', '총자산'])
    log_df = pd.DataFrame(records)
    if return_trades:
        return log_df, pd.DataFrame(trades)
    return log_df


# ──────────────────────────────────────────────
# 6. 성과 지표
# ──────────────────────────────────────────────

def compute_metrics(log_df: pd.DataFrame, trades_df: pd.DataFrame = None,
                    initial_capital: float = None,
                    periods_per_year: int = 252) -> dict:
    """일별 로그(+거래기록)에서 성과지표 산출."""
    if log_df is None or len(log_df) == 0:
        return {}
    ta = log_df['총자산'].values.astype(float)
    init = float(initial_capital) if initial_capital else ta[0]
    final = ta[-1]
    n = len(ta)
    yrs = n / periods_per_year
    cagr = (final / init) ** (1 / yrs) - 1 if yrs > 0 and init > 0 else 0.0

    peak = np.maximum.accumulate(ta)
    dd = (ta - peak) / peak
    mdd = dd.min()
    calmar = cagr / abs(mdd) if mdd < 0 else float('nan')

    # 일간 수익률 → Sharpe/Sortino (연율화)
    rets = np.diff(ta) / ta[:-1]
    rets = rets[np.isfinite(rets)]
    if len(rets) > 1 and rets.std() > 0:
        sharpe = rets.mean() / rets.std() * np.sqrt(periods_per_year)
        downside = rets[rets < 0]
        sortino = (rets.mean() / downside.std() * np.sqrt(periods_per_year)
                   if len(downside) > 0 and downside.std() > 0 else float('nan'))
    else:
        sharpe = sortino = float('nan')

    m = {
        '최종자산': final, '초기자본': init,
        '총수익률(%)': (final / init - 1) * 100 if init > 0 else 0,
        'CAGR(%)': cagr * 100, 'MDD(%)': mdd * 100, 'Calmar': calmar,
        'Sharpe': sharpe, 'Sortino': sortino, '거래일수': n, '투자기간(년)': yrs,
    }
    if trades_df is not None and len(trades_df) > 0:
        wins = (trades_df['실현손익'] > 0).sum()
        m.update({
            '총매도횟수': len(trades_df),
            '승률(%)': wins / len(trades_df) * 100,
            '평균손익': trades_df['실현손익'].mean(),
            '평균보유일': trades_df['보유일'].mean(),
        })
    else:
        m.update({'총매도횟수': 0, '승률(%)': 0, '평균손익': 0, '평균보유일': 0})
    return m


# ──────────────────────────────────────────────
# 7. 거래일 캘린더 + 내일 주문 생성 (주문표용)
# ──────────────────────────────────────────────

def _nth_weekday_of_month(year, month, weekday, n):
    from datetime import date as _d, timedelta as _td
    first = _d(year, month, 1)
    off = (weekday - first.weekday()) % 7
    return first + _td(days=off + 7 * (n - 1))


def _last_weekday_of_month(year, month, weekday):
    from datetime import date as _d, timedelta as _td
    last = (_d(year + 1, 1, 1) if month == 12 else _d(year, month + 1, 1)) - _td(days=1)
    off = (last.weekday() - weekday) % 7
    return last - _td(days=off)


def _easter_date(year):
    from datetime import date as _d
    a = year % 19; b, c = divmod(year, 100); d, e = divmod(b, 4)
    f = (b + 8) // 25; g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30; i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    mth = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * mth + 114, 31)
    return _d(year, month, day + 1)


def _us_holidays(year):
    from datetime import date as _d, timedelta as _td
    hs = {_d(year, 1, 1), _nth_weekday_of_month(year, 1, 0, 3),
          _nth_weekday_of_month(year, 2, 0, 3), _easter_date(year) - _td(days=2),
          _last_weekday_of_month(year, 5, 0), _d(year, 6, 19), _d(year, 7, 4),
          _nth_weekday_of_month(year, 9, 0, 1), _nth_weekday_of_month(year, 11, 3, 4),
          _d(year, 12, 25)}
    obs = set()
    for h in hs:
        if h.weekday() == 5:
            obs.add(h - _td(days=1))
        elif h.weekday() == 6:
            obs.add(h + _td(days=1))
    return hs | obs


def _is_trading_day(d):
    d = d.date() if hasattr(d, 'date') else d
    return d.weekday() < 5 and d not in _us_holidays(d.year)


def next_trading_days(start_date, n_days):
    """start_date 다음 영업일부터 n개의 NYSE 거래일."""
    out = []
    cur = pd.Timestamp(start_date) + pd.Timedelta(days=1)
    safety = 0
    while len(out) < n_days and safety < n_days * 3 + 40:
        if _is_trading_day(cur):
            out.append(cur)
        cur += pd.Timedelta(days=1)
        safety += 1
    return pd.DatetimeIndex(out)


def build_today_orders(prices, params, mode_map=None, start_date=None, mode_rule=None,
                       forced_mode=None, capital_injections=None):
    """백테스트 최종 상태 → 다음 거래일 LOC/MOC 주문표.

    forced_mode: '공격'/'방어' 지정 시 다음 세션 매수 모드를 강제 (원전략 수동 모드용).
    mode_rule: {'ma_weeks','peak_thr','dn'} 제공 시, 다음 세션 모드를 '직전 확정 주봉
               (지난주/지지난주)' 기준으로 산출 (실거래 주문표 권장 — 1주 추가지연 제거).
               미제공 시 백테스트 마지막 주 모드를 그대로 carry.

    Returns dict: order_date, last_date, last_close, next_mode, cash, total_asset,
                  n_pos, divisions, positions(list), orders(list).
    orders 항목: {구분(매수/매도), 거래방법(LOC/MOC), 가격, 수량, 사유, 모드, 티어}
    """
    log, state = run_backtest(prices, params, mode_map=mode_map,
                              start_date=start_date, return_state=True,
                              liquidate_at_end=False, capital_injections=capital_injections)
    positions = state['positions']
    last_date = state['last_date']
    c1, c2 = state['c1'], state['c2']
    prev_fi = state['prev_fi']
    last_ma5 = state['last_ma5']
    cash = state['cash']
    if forced_mode in ('공격', '방어'):
        mode = forced_mode                  # 수동 지정 (원전략 모드 따라가기)
    elif mode_rule:
        mode = forward_mode(prices, prev_mode=state['last_mode'], **mode_rule)
    else:
        mode = state['last_mode']           # 다음 세션 모드 (carry)

    nd = next_trading_days(last_date, 1)
    order_date = nd[0] if len(nd) else pd.Timestamp(last_date) + pd.Timedelta(days=1)

    # 주간 RSI (헤더 표시용) — 금요일 종가 기준
    _wrsi_now = _wrsi_prev = None
    try:
        _wk = prices['close'].resample('W-FRI').last().dropna()
        _wr = calc_rsi_wilder(_wk.values, 14)
        if len(_wr) and not np.isnan(_wr[-1]):
            _wrsi_now = round(float(_wr[-1]), 2)
        if len(_wr) > 1 and not np.isnan(_wr[-2]):
            _wrsi_prev = round(float(_wr[-2]), 2)
    except Exception:
        pass

    # '미국장 종료 후' 자본 조정 중, 이번 주문일(=다음 거래일)부터 가용해지는 분을 주문 현금에 반영.
    # D일 종료 후 입금 → next_trading_day(D)=주문일. 즉 last_date <= D < order_date.
    units = state.get('units', 0.0)
    if capital_injections:
        _ld = pd.Timestamp(last_date).normalize()
        _od = pd.Timestamp(order_date).normalize()
        for d, amt in capital_injections.items():
            _dn = pd.Timestamp(d).normalize()
            if _ld <= _dn < _od:
                _up = state.get('unit_price', 1.0) or 1.0
                units += float(amt) / _up if _up > 0 else 0.0
                cash += float(amt)
                state['total_asset'] = state.get('total_asset', cash) + float(amt)
    unit_price_now = (state['total_asset'] / units) if units > 0 else state.get('unit_price', 1.0)
    ret_pct_now = (unit_price_now - 1.0) * 100

    # 다음날 방어 MA 매도가 / 매수 K
    sf_sell_price = sf_buy_K = None
    if c1 is not None and c2 is not None:
        prev_sum = c1 + c2
        fs = 1 + params.sf_sell_pct / 100
        f1 = 1 + params.sf_buy_pct1 / 100
        sf_sell_price = (fs * prev_sum) / (params.sf_ma_base - fs)
        cond1 = (f1 * prev_sum) / (params.sf_ma_base - f1)
        cond2 = c1 * (1 + params.sf_buy_pct2 / 100)
        sf_buy_K = round(min(cond1, cond2), 2)
    # 다음날 공격 매수 K
    ag_buy_K = None
    if c1 is not None:
        if prev_fi == '+':
            ag_buy_K = math.floor(c1 * (1 + params.ag_buy_pct / 100) * 100) / 100
        else:
            ag_buy_K = math.floor(c1 * 0.999 * 100) / 100

    def stop_of(p):
        sd = next_trading_days(p.buy_date, p.hold_days)
        return sd[-1] if len(sd) else pd.Timestamp(p.buy_date)

    moc_today = any(order_date >= stop_of(p) for p in positions)
    ag_hold_tier1 = (c1 is not None and last_ma5 is not None and c1 > last_ma5)

    orders = []
    pos_view = []
    for p in positions:
        sdt = stop_of(p)
        pos_view.append({'모드': p.mode, '티어': p.tier, '매수일': pd.Timestamp(p.buy_date),
                         '매수가': round(p.buy_price, 4), '수량': p.qty,
                         '매도목표': round(p.sell_target, 2) if p.sell_target else None,
                         '손절예정일': sdt})
        if order_date >= sdt:
            hd = p.hold_days
            note = f"보유 {hd}일 만기({pd.Timestamp(sdt).strftime('%m/%d')} 도래) → 시장가 종가 청산"
            orders.append({'구분': '매도', '거래방법': 'MOC', '가격': None, '수량': p.qty,
                           '사유': 'MOC', '모드': p.mode, '티어': p.tier, '비고': note})
            continue
        if moc_today:
            continue                     # MOC 우선 → 다른 LOC 매도 보류
        if p.mode == '공격' and p.sell_target is not None:
            if ag_hold_tier1 and p.tier == 1:
                continue                 # 1티어 보류
            npct = (p.sell_target / p.buy_price - 1) * 100 if p.buy_price else 0
            note = (f"매수가 ${p.buy_price:.2f} × (1+{npct:.2f}%) = ${p.sell_target:.2f} "
                    f"[매수RSI {p.buy_rsi:.0f} 정규화] 이상 종가면 익절")
            orders.append({'구분': '매도', '거래방법': 'LOC', '가격': round(p.sell_target, 2),
                           '수량': p.qty, '사유': f'T{p.tier}', '모드': '공격', '티어': p.tier,
                           '비고': note})
    # 방어 그룹 MA 전량 매도 (MOC 없을 때) — 공통사항: 전일종가>전일MA5면 슬롯1 매도 보류
    if not moc_today and sf_sell_price is not None:
        sf_pos = [p for p in positions if p.mode == '방어'
                  and not (ag_hold_tier1 and p.tier == 1)]
        sf_qty = sum(p.qty for p in sf_pos)
        if sf_qty > 0:
            _held1 = " (슬롯1 보류 제외)" if (ag_hold_tier1 and any(
                p.mode == '방어' and p.tier == 1 for p in positions)) else ""
            note = (f"3일MA 기준 (1+{params.sf_sell_pct}%) = ${sf_sell_price:.2f} "
                    f"이상 종가면 방어분 청산{_held1}")
            orders.append({'구분': '매도', '거래방법': 'LOC', '가격': round(sf_sell_price, 2),
                           '수량': sf_qty, '사유': 'MA', '모드': '방어', '티어': None, '비고': note})

    # 매수 주문 (빈 슬롯)
    if mode == '공격':
        divisions, K, wts = params.ag_divisions, ag_buy_K, [1] * params.ag_divisions
    else:
        divisions, K, wts = params.sf_divisions, sf_buy_K, list(params.sf_tier_weights)
    occupied = set(p.tier for p in positions if p.mode == mode)
    tier = next((t for t in range(1, divisions + 1) if t not in occupied), None)
    if tier is not None and K and K > 0:
        remaining = sum(wts[t - 1] for t in range(1, len(wts) + 1) if t not in occupied)
        alloc = cash * (wts[tier - 1] / remaining) if remaining > 0 else 0
        buy_qty = int(alloc / K)
        if buy_qty > 0:
            if mode == '공격':
                if prev_fi == '+':
                    note = (f"전일↑(FI+): 전일종가 ${c1:.2f} × (1+{params.ag_buy_pct}%) "
                            f"= ${K:.2f} 이하 종가면 체결 · 할당 ${alloc:,.0f}÷${K:.2f}")
                else:
                    note = (f"전일↓(FI−): 전일종가 ${c1:.2f} × (1−0.1%) "
                            f"= ${K:.2f} 이하 종가면 체결 · 할당 ${alloc:,.0f}÷${K:.2f}")
            else:
                note = (f"min(MA−{abs(params.sf_buy_pct1)}%, 전일종가×(1+{params.sf_buy_pct2}%)) "
                        f"= ${K:.2f} 이하 종가면 체결 · 티어{tier} 비중 할당 ${alloc:,.0f}÷${K:.2f}")
            orders.append({'구분': '매수', '거래방법': 'LOC', '가격': round(K, 2), '수량': buy_qty,
                           '사유': f'{"공" if mode == "공격" else "방"}T{tier}',
                           '모드': mode, '티어': tier, '비고': note})

    return {
        'order_date': order_date, 'last_date': last_date, 'last_close': c1,
        'next_mode': mode, 'cash': cash, 'total_asset': state['total_asset'],
        'n_pos': len(positions), 'divisions': divisions,
        'cum_realized': state['cum_realized'],
        'positions': pos_view, 'orders': orders,
        'trades': state.get('trades', []),    # 시작일~현재 체결 내역
        'daily_log': log,                     # 일별 매매로그 DataFrame
        'unit_price': round(unit_price_now, 6),   # 좌당가
        'return_pct': round(ret_pct_now, 2),      # 좌당가 기준 누적 손익률(%)
        'wrsi_now': _wrsi_now, 'wrsi_prev': _wrsi_prev,   # 주간 RSI (헤더 표시용)
    }
