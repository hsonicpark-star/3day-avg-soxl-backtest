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
            return df
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
            return df2.dropna()
    except Exception:
        pass

    return pd.DataFrame()


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
