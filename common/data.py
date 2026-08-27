from __future__ import annotations

import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, date


def next_trading_date(d: date | None = None) -> date:
    """다음 거래일 반환. 주말이면 다음 월요일, 평일이면 그대로 반환.
    미국 주요 공휴일(뉴욕증시 휴장일)도 건너뜀."""
    if d is None:
        d = datetime.today().date()
    # 미국 주요 공휴일 (고정일 + 근사치, 매년 갱신 필요 시 추가)
    year = d.year
    us_holidays = set()
    # New Year's Day
    us_holidays.add(date(year, 1, 1))
    # MLK Day (1월 셋째 월요일)
    us_holidays.add(_nth_weekday(year, 1, 0, 3))
    # Presidents' Day (2월 셋째 월요일)
    us_holidays.add(_nth_weekday(year, 2, 0, 3))
    # Good Friday (부활절 -2일, 근사 계산)
    _easter = _easter_date(year)
    us_holidays.add(_easter - timedelta(days=2))
    # Memorial Day (5월 마지막 월요일)
    us_holidays.add(_last_weekday(year, 5, 0))
    # Juneteenth
    us_holidays.add(date(year, 6, 19))
    # Independence Day
    us_holidays.add(date(year, 7, 4))
    # Labor Day (9월 첫째 월요일)
    us_holidays.add(_nth_weekday(year, 9, 0, 1))
    # Thanksgiving (11월 넷째 목요일)
    us_holidays.add(_nth_weekday(year, 11, 3, 4))
    # Christmas
    us_holidays.add(date(year, 12, 25))
    # 공휴일이 주말이면 관측일(observed) 추가
    observed = set()
    for h in us_holidays:
        if h.weekday() == 5:  # 토요일 → 금요일
            observed.add(h - timedelta(days=1))
        elif h.weekday() == 6:  # 일요일 → 월요일
            observed.add(h + timedelta(days=1))
    us_holidays |= observed

    while d.weekday() >= 5 or d in us_holidays:
        d += timedelta(days=1)
    return d


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """year/month의 n번째 weekday (0=월요일) 반환."""
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """year/month의 마지막 weekday 반환."""
    if month == 12:
        last = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - timedelta(days=offset)


def _easter_date(year: int) -> date:
    """Anonymous Gregorian algorithm으로 부활절 계산."""
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
    return date(year, month, day + 1)


def filter_incomplete_today(df: pd.DataFrame) -> pd.DataFrame:
    """미국 장이 아직 종료되지 않은 당일 intraday 데이터 제거.

    yfinance는 장 중에도 '오늘' 행의 Close에 현재가를 채워서 반환한다.
    이를 진짜 종가처럼 쓰면 백테스트가 미래를 당겨 쓰는 격이 되므로,
    16:30 ET(종가 확정 + 안전버퍼) 이전에는 US-오늘 날짜의 행을 제거.
    """
    if df is None or df.empty:
        return df
    try:
        # 뉴욕 기준 현재시각 (pandas가 IANA zoneinfo 사용)
        now_est = pd.Timestamp.now(tz="America/New_York")
        market_close_buffer = now_est.replace(hour=16, minute=30,
                                              second=0, microsecond=0)
        if now_est < market_close_buffer:
            us_today = pd.Timestamp(now_est.date())
            return df[df.index.normalize() < us_today]
    except Exception:
        pass
    return df


def _maybe_patch_soxl_backup(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """SOXL 한정: yfinance 확정 종가 누락 시 백업 구글시트(DB)로 보충.
    경고는 df.attrs['data_warning']에 저장 (호출자가 UI 표시)."""
    if df is None or df.empty or ticker.upper() != "SOXL":
        if df is not None:
            df.attrs['data_warning'] = None
        return df
    try:
        import sys as _sys, os as _os
        _root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
        if _root not in _sys.path:
            _sys.path.insert(0, _root)
        from backup_close import check_and_patch_soxl
        # gspread client: Cloud는 st.secrets, 로컬은 service_account.json
        gc = None
        try:
            from common.config import _get_gspread_client
            gc = _get_gspread_client()
        except Exception:
            gc = None  # backup_close가 자체 인증 시도
        df, warn, resolved = check_and_patch_soxl(df, gc=gc)
        df.attrs['data_warning'] = warn
        df.attrs['data_stale'] = not resolved
    except Exception:
        df.attrs['data_warning'] = None
        df.attrs['data_stale'] = False
    return df


@st.cache_data(show_spinner=False)
def _download_price(ticker: str, start_str: str, end_str: str) -> pd.DataFrame:
    start = pd.to_datetime(start_str).date()
    end   = pd.to_datetime(end_str).date()

    def _to_close_df(raw):
        if raw is None or raw.empty:
            return pd.DataFrame()
        if isinstance(raw.columns, pd.MultiIndex):
            try:    raw = raw.xs(ticker, axis=1, level="Ticker")
            except: raw.columns = raw.columns.droplevel(1)
        if "Close" not in raw.columns:
            return pd.DataFrame()
        df = raw[["Close"]].copy()
        df.index = pd.to_datetime(df.index)
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        return df.dropna()

    # 야후 순간 장애/레이트리밋(429) 대비 — 지연을 두고 최대 3회 재시도.
    # (한 번 실패로 바로 "가격 데이터를 불러오지 못했습니다"가 뜨던 문제 완화.
    #  같은 시각에도 요청별로 성공/실패가 갈리는 야후 CDN 특성 때문에
    #  재시도만으로 대부분 해결됨)
    import time as _time
    for _attempt in range(3):
        # 방법 1: yf.download
        try:
            raw = yf.download(
                ticker,
                start=start - timedelta(days=15),
                end=end + timedelta(days=2),
                progress=False, auto_adjust=True,
            )
            df = _to_close_df(raw)
            if not df.empty:
                return _maybe_patch_soxl_backup(filter_incomplete_today(df), ticker)
        except Exception:
            pass

        # 방법 2: yf.Ticker.history (fallback)
        try:
            t = yf.Ticker(ticker)
            raw2 = t.history(
                start=start - timedelta(days=15),
                end=end + timedelta(days=2),
                auto_adjust=True,
            )
            if not raw2.empty and "Close" in raw2.columns:
                df2 = raw2[["Close"]].copy()
                df2.index = pd.to_datetime(df2.index).tz_localize(None)
                df2["Close"] = pd.to_numeric(df2["Close"], errors="coerce")
                return _maybe_patch_soxl_backup(
                    filter_incomplete_today(df2.dropna()), ticker)
        except Exception:
            pass

        if _attempt < 2:
            _time.sleep(2 + 2 * _attempt)   # 2초 → 4초 백오프

    return pd.DataFrame()


def halt_if_stale(df, context: str = "주문표") -> bool:
    """df.attrs['data_stale']이면 st.error 표시 후 True 반환 (호출자는 생성 중단).

    정책: 전일 종가 미확보(yfinance + 백업 모두 실패) 시 낡은 데이터로
    주문표를 만들면 안 됨 → 생성 차단 + 에러 안내."""
    if df is None:
        return False
    try:
        stale = df.attrs.get('data_stale', False)
        warn = df.attrs.get('data_warning')
    except Exception:
        return False
    if stale:
        st.error(f"{warn or '⛔ 전일 종가 미확보'}\n\n"
                 f"최신 전일 종가를 확보하지 못해 **{context}를 생성하지 않습니다.** "
                 f"낡은 데이터 기준의 잘못된 주문을 방지하기 위함입니다. "
                 f"잠시 후 다시 시도해주세요.")
        return True
    if warn:
        st.warning(f"{warn}\n\n{context}는 백업 데이터를 반영하여 계산되었습니다.")
    return False


def load_price_data(ticker, start, end, data_source, excel_file):
    if data_source == "엑셀 Daily_Close 시트" and excel_file is not None:
        xl = pd.ExcelFile(excel_file)
        df = xl.parse("Daily_Close")
        df.columns = ["Date", "Close"]
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date").sort_index()
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        return df.dropna()
    return _download_price(ticker, str(start), str(end))
