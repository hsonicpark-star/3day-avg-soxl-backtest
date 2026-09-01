"""
common/pricedb.py — 로컬 파일 기반 종가 DB (야후 파이낸스 장애 대비 백업)

목적
----
yfinance 가 죽거나 레이트리밋(429)에 걸려도 백테스트를 계속 돌릴 수 있도록
과거 확정 종가를 로컬 CSV 로 보관한다.

저장 위치
---------
    <앱루트>/pricedb/{TICKER}.csv     (Date,Close — Date 오름차순, ISO 날짜)

원칙 (B방식 — 과거 기록 불변)
-----------------------------
- 이미 저장된 (날짜, 종가) 는 절대 덮어쓰지 않는다.
- 새 날짜만 뒤에 누적한다.
- overwrite=True 를 명시한 경우에만 기존 값을 갱신한다 (수동 보정용).

사용 예
-------
    from common.pricedb import load_prices, sync_from_yfinance, db_status
    df = load_prices("SOXL")                    # 로컬 DB 조회
    n  = sync_from_yfinance("SOXL")             # 야후에서 신규분만 추가
"""

from __future__ import annotations

import os
import re
import glob
from datetime import datetime, timedelta

import pandas as pd

# ── 경로 ────────────────────────────────────────────────────
_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_DIR = os.path.join(_APP_ROOT, "pricedb")


def _path(ticker: str) -> str:
    return os.path.join(DB_DIR, f"{ticker.strip().upper()}.csv")


def ensure_dir():
    os.makedirs(DB_DIR, exist_ok=True)


# ── 조회 ────────────────────────────────────────────────────
def list_tickers() -> list:
    """로컬 DB에 저장된 티커 목록."""
    if not os.path.isdir(DB_DIR):
        return []
    out = []
    for p in glob.glob(os.path.join(DB_DIR, "*.csv")):
        out.append(os.path.splitext(os.path.basename(p))[0].upper())
    return sorted(out)


def has_ticker(ticker: str) -> bool:
    return os.path.exists(_path(ticker))


def load_prices(ticker: str, start=None, end=None) -> pd.DataFrame:
    """로컬 DB에서 종가 시계열 로드. 없으면 빈 DataFrame.

    반환: index=DatetimeIndex(Date), columns=['Close']
    """
    p = _path(ticker)
    if not os.path.exists(p):
        return pd.DataFrame()
    try:
        df = pd.read_csv(p)
    except Exception:
        return pd.DataFrame()
    if df.empty or "Date" not in df.columns or "Close" not in df.columns:
        return pd.DataFrame()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    df = df.dropna(subset=["Date", "Close"])
    df = df.drop_duplicates(subset=["Date"], keep="last")
    df = df.set_index("Date").sort_index()[["Close"]]
    if start is not None:
        df = df[df.index >= pd.Timestamp(start)]
    if end is not None:
        df = df[df.index <= pd.Timestamp(end)]
    return df


def db_status() -> pd.DataFrame:
    """DB 전체 현황 (티커 / 행수 / 시작일 / 종료일 / 파일크기)."""
    rows = []
    for tk in list_tickers():
        df = load_prices(tk)
        p = _path(tk)
        rows.append({
            "티커": tk,
            "행수": len(df),
            "시작일": df.index.min().date() if len(df) else None,
            "종료일": df.index.max().date() if len(df) else None,
            "KB": round(os.path.getsize(p) / 1024, 1),
        })
    return pd.DataFrame(rows)


# ── 저장 (B방식 누적) ───────────────────────────────────────
def save_prices(ticker: str, new_df, overwrite: bool = False) -> int:
    """새 종가를 로컬 DB에 병합 저장. 반환값 = 실제로 추가된 행 수.

    new_df: index=Date(또는 'Date' 컬럼) + 'Close' 컬럼
    overwrite=False → 기존 날짜는 유지(불변), 새 날짜만 추가
    """
    if new_df is None or len(new_df) == 0:
        return 0
    ensure_dir()

    nd = new_df.copy()
    if isinstance(nd, pd.Series):
        nd = nd.to_frame("Close")
    if "Date" in nd.columns:
        nd = nd.set_index("Date")
    if "Close" not in nd.columns:
        if nd.shape[1] == 1:
            nd.columns = ["Close"]
        else:
            return 0
    nd.index = pd.to_datetime(nd.index, errors="coerce")
    nd = nd[["Close"]]
    nd["Close"] = pd.to_numeric(nd["Close"], errors="coerce")
    nd = nd[~nd.index.isna()].dropna()
    if nd.empty:
        return 0
    # 시간 성분 제거 (일봉)
    nd.index = pd.DatetimeIndex(nd.index).normalize()
    nd = nd[~nd.index.duplicated(keep="last")].sort_index()

    old = load_prices(ticker)
    if old.empty:
        merged = nd
        added = len(nd)
    elif overwrite:
        merged = pd.concat([old, nd])
        merged = merged[~merged.index.duplicated(keep="last")].sort_index()
        added = len(merged) - len(old)
    else:
        fresh = nd[~nd.index.isin(old.index)]
        added = len(fresh)
        if added == 0:
            return 0
        merged = pd.concat([old, fresh]).sort_index()

    out = merged.reset_index()
    out.columns = ["Date", "Close"]
    out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m-%d")
    out["Close"] = out["Close"].astype(float).round(4)
    out.to_csv(_path(ticker), index=False, encoding="utf-8")
    return added


def delete_ticker(ticker: str) -> bool:
    p = _path(ticker)
    if os.path.exists(p):
        os.remove(p)
        return True
    return False


# ── 야후 동기화 ─────────────────────────────────────────────
def _filter_incomplete_today(df: pd.DataFrame) -> pd.DataFrame:
    """미국 장 마감(16:30 ET) 전이면 '오늘' 행(intraday)을 제거.

    common.data 에도 같은 함수가 있으나, 이 모듈은 streamlit 의존 없이
    cron/스크립트에서도 쓰이므로 자체 구현을 둔다.
    """
    if df is None or df.empty:
        return df
    try:
        now_est = pd.Timestamp.now(tz="America/New_York")
        cutoff = now_est.replace(hour=16, minute=30, second=0, microsecond=0)
        if now_est < cutoff:
            return df[df.index.normalize() < pd.Timestamp(now_est.date())]
    except Exception:
        pass
    return df


def fetch_yfinance(ticker: str, start: str = "2009-01-01", end=None,
                   auto_adjust: bool = True) -> pd.DataFrame:
    """야후에서 종가 시계열을 받아 (Date index, Close) 로 정규화. 실패 시 빈 DF."""
    import yfinance as yf

    if end is None:
        end = (datetime.today() + timedelta(days=2)).strftime("%Y-%m-%d")

    raw = None
    try:
        raw = yf.download(ticker, start=start, end=end,
                          progress=False, auto_adjust=auto_adjust)
    except Exception:
        raw = None
    if raw is None or raw.empty:
        try:
            raw = yf.Ticker(ticker).history(start=start, end=end,
                                            auto_adjust=auto_adjust)
        except Exception:
            raw = None
    if raw is None or raw.empty:
        return pd.DataFrame()

    if isinstance(raw.columns, pd.MultiIndex):
        try:
            raw = raw.xs(ticker, axis=1, level="Ticker")
        except Exception:
            raw.columns = raw.columns.droplevel(1)
    if "Close" not in raw.columns:
        return pd.DataFrame()

    df = raw[["Close"]].copy()
    df.index = pd.to_datetime(df.index)
    try:
        df.index = df.index.tz_localize(None)
    except Exception:
        pass
    df.index = pd.DatetimeIndex(df.index).normalize()
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    return _filter_incomplete_today(df.dropna())


def sync_from_yfinance(ticker: str, start: str = "2009-01-01",
                       end=None) -> int:
    """야후에서 받아 로컬 DB에 신규분만 누적. 반환 = 추가된 행 수."""
    return save_prices(ticker, fetch_yfinance(ticker, start, end))


def load_prices_hybrid(ticker: str, start=None, end=None,
                       allow_remote: bool = True) -> pd.DataFrame:
    """로컬 DB(과거 확정 종가) + 야후 최신분 이어붙이기.

    만능시트 재현에 필요한 과거 무조정 종가는 로컬 DB 를 그대로 쓰고,
    DB 마지막 날짜 이후 구간만 야후에서 받아 덧붙인다.
    (최근 구간은 배당 조정계수가 1.0 이라 이어붙여도 불연속이 없다.)

    DB 가 비어 있으면 야후 전체를, 야후가 실패하면 DB 만 반환한다.
    반환 df 의 attrs:
        from_pricedb : 로컬 DB 를 기반으로 사용했는가
        appended     : 야후에서 덧붙인 행 수
        stale        : 최신분을 못 받아 DB 가 그대로인가
    """
    db = load_prices(ticker)
    if db.empty:
        out = fetch_yfinance(ticker) if allow_remote else pd.DataFrame()
        out.attrs.update({"from_pricedb": False, "appended": len(out),
                          "stale": out.empty})
        return _slice(out, start, end)

    appended = 0
    stale = True
    if allow_remote:
        last = db.index[-1]
        tail = fetch_yfinance(
            ticker, start=(last + timedelta(days=1)).strftime("%Y-%m-%d"))
        if not tail.empty:
            tail = tail[tail.index > last]
            if not tail.empty:
                db = pd.concat([db, tail]).sort_index()
                appended = len(tail)
        stale = (appended == 0)
    db.attrs.update({"from_pricedb": True, "appended": appended, "stale": stale})
    return _slice(db, start, end)


def _slice(df: pd.DataFrame, start, end) -> pd.DataFrame:
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    attrs = dict(df.attrs)
    if start is not None:
        df = df[df.index >= pd.Timestamp(start)]
    if end is not None:
        df = df[df.index <= pd.Timestamp(end)]
    df.attrs.update(attrs)
    return df


# ── 구글시트 CSV(만능시트 DB 탭) 임포트 ──────────────────────
_KDATE = re.compile(r"^\s*(\d{2})\.(\d{2})\.(\d{2})\.?\s*\(.\)\s*$")


def parse_kdate(s):
    """'26.08.28(금)' / '26.08.28.(금)' → Timestamp. 실패 시 None."""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    m = _KDATE.match(s)
    if m:
        yy, mm, dd = (int(x) for x in m.groups())
        try:
            return pd.Timestamp(2000 + yy, mm, dd)
        except ValueError:
            return None
    try:
        v = pd.to_datetime(s, errors="coerce")
        return None if pd.isna(v) else v.normalize()
    except Exception:
        return None


def import_pairs_from_csv(csv_path: str, date_col: int, close_col: int,
                          header_rows: int = 4) -> pd.DataFrame:
    """구글시트 export CSV에서 (날짜, 종가) 열 한 쌍을 뽑아 DataFrame 으로.

    만능시트 'DB' 탭은 머리글이 4행이고, 여러 종목의 시계열이
    가로로 나란히 배치되어 있어 열 인덱스를 직접 지정한다.
    """
    import csv as _csv

    rows = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        for i, r in enumerate(_csv.reader(f)):
            if i < header_rows:
                continue
            if len(r) <= max(date_col, close_col):
                continue
            d = parse_kdate(r[date_col])
            if d is None:
                continue
            c = str(r[close_col]).replace("$", "").replace(",", "").strip()
            if not c:
                continue
            try:
                c = float(c)
            except ValueError:
                continue
            rows.append((d, c))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["Date", "Close"]).set_index("Date")
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df


# ══════════════════════════════════════════════════════════════
# 구글시트 백업 (야후 장애 + 로컬 파일 소실 대비 3차 방어선)
# ══════════════════════════════════════════════════════════════
# Streamlit Cloud 는 파일시스템이 임시라 pricedb/*.csv 갱신이 재시작 시 사라진다.
# 따라서 클라우드에서 '영구 저장소' 역할은 구글시트가 맡는다.
#
# 조회 우선순위 (load_prices_resilient):
#     1) 로컬 CSV (과거 확정 종가, 가장 빠름)
#     2) + 야후 최신분 이어붙이기
#     3) 야후 실패 & 로컬이 낡음 → 사용자 스프레드시트 pricedb_{TICKER} 탭
#     4) 그래도 없으면 → 원본 만능시트 DB 탭 (읽기 전용)
#
# 기록 (push_to_gsheet): 야후에서 받은 신규 종가를 사용자 시트에 B방식으로 누적.
#
# ⚠️ 이 모듈은 streamlit 을 import 하지 않는다 (GitHub Actions 러너에 미설치).
#    gspread client 는 호출자가 넘기거나 backup_close 의 인증을 재사용한다.

GSHEET_TAB_PREFIX = "pricedb_"

# 원본 만능시트 DB 탭의 티커별 (날짜열, 종가열) — backup_close.BACKUP_SHEET_URL
SOURCE_DB_COLS = {
    "SOXL": ("E", "F"),
    "QQQ": ("N", "O"),
}


def gsheet_tab(ticker: str) -> str:
    return f"{GSHEET_TAB_PREFIX}{ticker.strip().upper()}"


def _resolve_gc(gc=None):
    """gspread client 확보. 호출자 → common.config(앱) → backup_close(env/로컬)."""
    if gc is not None:
        return gc
    try:
        from common.config import _get_gspread_client
        return _get_gspread_client()
    except Exception:
        pass
    from backup_close import _default_gspread_client
    return _default_gspread_client()


def _rows_to_df(rows) -> pd.DataFrame:
    """[[날짜, 종가], ...] → DataFrame(index=Date, Close). 한국식 날짜도 허용."""
    dates, closes = [], []
    for r in rows:
        if not r or len(r) < 2:
            continue
        d = parse_kdate(r[0])
        if d is None:
            continue
        c = str(r[1]).replace("$", "").replace(",", "").strip()
        if not c:
            continue
        try:
            closes.append(float(c))
        except ValueError:
            continue
        dates.append(d)
    if not dates:
        return pd.DataFrame()
    df = pd.DataFrame({"Close": closes}, index=pd.DatetimeIndex(dates))
    df.index.name = "Date"
    return df[~df.index.duplicated(keep="last")].sort_index()


def pull_from_gsheet(gs_url: str, ticker: str, gc=None) -> pd.DataFrame:
    """사용자 스프레드시트의 pricedb_{TICKER} 탭에서 종가 로드. 없으면 빈 DF."""
    if not gs_url:
        return pd.DataFrame()
    try:
        gc = _resolve_gc(gc)
        ws = gc.open_by_url(gs_url).worksheet(gsheet_tab(ticker))
        return _rows_to_df(ws.get_values("A2:B"))
    except Exception:
        return pd.DataFrame()


def push_to_gsheet(gs_url: str, ticker: str, df: pd.DataFrame = None,
                   gc=None) -> int:
    """종가를 사용자 스프레드시트에 B방식(신규 날짜만 append)으로 기록.

    df 미지정 시 로컬 DB 전체를 기록 대상으로 삼는다.
    반환 = 실제로 추가된 행 수. 탭이 없으면 만든다.
    """
    if not gs_url:
        return 0
    src = df if df is not None else load_prices(ticker)
    if src is None or src.empty:
        return 0

    gc = _resolve_gc(gc)
    sh = gc.open_by_url(gs_url)
    tab = gsheet_tab(ticker)
    try:
        ws = sh.worksheet(tab)
    except Exception:
        ws = sh.add_worksheet(title=tab, rows=1, cols=2)
        ws.update(range_name="A1", values=[["Date", "Close"]])

    have = _rows_to_df(ws.get_values("A2:B"))
    fresh = src if have.empty else src[~src.index.isin(have.index)]
    if fresh.empty:
        return 0
    rows = [[d.strftime("%Y-%m-%d"), float(round(c, 4))]
            for d, c in fresh["Close"].items()]
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    return len(rows)


def fetch_from_source_sheet(ticker: str, gc=None) -> pd.DataFrame:
    """원본 만능시트 DB 탭에서 종가 로드 (읽기 전용).

    backup_close.BACKUP_SHEET_URL / BACKUP_DB_TAB 을 재사용한다.
    지원 티커는 SOURCE_DB_COLS 에 정의된 것만.
    """
    tk = ticker.strip().upper()
    cols = SOURCE_DB_COLS.get(tk)
    if not cols:
        return pd.DataFrame()
    try:
        from backup_close import BACKUP_SHEET_URL, BACKUP_DB_TAB
        gc = _resolve_gc(gc)
        ws = gc.open_by_url(BACKUP_SHEET_URL).worksheet(BACKUP_DB_TAB)
        rng = f"{cols[0]}5:{cols[1]}"
        return _rows_to_df(ws.get_values(rng))
    except Exception:
        return pd.DataFrame()


def sync_all(ticker: str, gs_url: str = "", gc=None) -> dict:
    """야후 → 로컬 CSV → 구글시트 순으로 신규 종가를 누적한다.

    반환: {"yahoo": 받은 행수, "local": 로컬 추가 행수, "sheet": 시트 추가 행수}
    """
    out = {"yahoo": 0, "local": 0, "sheet": 0, "error": ""}
    try:
        fresh = fetch_yfinance(ticker)
        out["yahoo"] = len(fresh)
        if not fresh.empty:
            out["local"] = save_prices(ticker, fresh)
    except Exception as e:
        out["error"] = f"야후 수집 실패: {e}"
    if gs_url:
        try:
            out["sheet"] = push_to_gsheet(gs_url, ticker, gc=gc)
        except Exception as e:
            out["error"] = (out["error"] + " / " if out["error"] else "") \
                           + f"시트 기록 실패: {e}"
    return out


def load_prices_resilient(ticker: str, gs_url: str = "", gc=None,
                          allow_remote: bool = True,
                          start=None, end=None) -> pd.DataFrame:
    """로컬 CSV + 야후 최신분, 실패 시 구글시트로 폴백하는 최종 로더.

    attrs:
        source   : 사용한 경로 요약 문자열
        appended : 야후에서 이어붙인 행 수
        stale    : 최신분을 확보하지 못했는가
    """
    df = load_prices_hybrid(ticker, allow_remote=allow_remote)
    used = []
    if not df.empty:
        used.append("로컬DB")
        if df.attrs.get("appended"):
            used.append(f"야후+{df.attrs['appended']}행")

    # 야후 최신분을 못 받았거나 로컬이 아예 비어 있으면 시트로 보충
    if df.empty or df.attrs.get("stale"):
        for label, getter in (("사용자시트",
                               lambda: pull_from_gsheet(gs_url, ticker, gc)),
                              ("원본만능시트",
                               lambda: fetch_from_source_sheet(ticker, gc))):
            try:
                sheet_df = getter()
            except Exception:
                sheet_df = pd.DataFrame()
            if sheet_df is None or sheet_df.empty:
                continue
            if df.empty:
                df = sheet_df
                used.append(label)
            else:
                add = sheet_df[sheet_df.index > df.index[-1]]
                if not add.empty:
                    df = pd.concat([df, add]).sort_index()
                    used.append(f"{label}+{len(add)}행")
                else:
                    continue
            break

    if df is None or df.empty:
        return pd.DataFrame()
    attrs = dict(df.attrs)
    attrs["source"] = " → ".join(used) if used else "없음"
    attrs.setdefault("appended", 0)
    df = df.copy()
    df.attrs.update(attrs)
    return _slice(df, start, end)
