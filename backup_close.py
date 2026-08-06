"""
yfinance 장애 대비 — 구글시트 DB 백업으로 SOXL 종가 보충

yfinance가 확정 거래일의 종가를 NaN/누락으로 반환하는 경우
(예: 2026-06-09 Close=NaN 사건), '쪼꼬야옹 만능 스위치' 시트의
DB 탭(E열=날짜, F열=SOXL 일별종가)에서 누락 종가를 가져와 보충한다.

streamlit 의존 없음 — 웹앱 / GitHub Actions 모두 사용 가능.
인증 우선순위:
  1) 호출자가 넘긴 gspread client
  2) 환경변수 GCP_SERVICE_ACCOUNT_JSON (GitHub Actions)
  3) 로컬 service_account.json
"""
import os
import re
import json
import pandas as pd

BACKUP_SHEET_URL = ("https://docs.google.com/spreadsheets/d/"
                    "1GY1LvAPrqvHEC47Flt-atcikwx1gd8wH-mMnnnFwciM/edit")
BACKUP_DB_TAB = "DB"

_GS_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]


def _default_gspread_client():
    """환경변수 또는 로컬 service_account.json으로 gspread 인증."""
    import gspread
    from google.oauth2.service_account import Credentials
    sa_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
    if sa_json:
        info = json.loads(sa_json)
        creds = Credentials.from_service_account_info(info, scopes=_GS_SCOPES)
        return gspread.authorize(creds)
    here = os.path.dirname(os.path.abspath(__file__))
    sa_path = os.path.join(here, "service_account.json")
    if os.path.exists(sa_path):
        creds = Credentials.from_service_account_file(sa_path, scopes=_GS_SCOPES)
        return gspread.authorize(creds)
    raise RuntimeError("gspread 인증 수단 없음 (env/service_account.json)")


def _parse_db_date(s: str):
    """'26.06.09(화)' → Timestamp('2026-06-09'). 실패 시 None."""
    m = re.match(r"\s*(\d{2})\.(\d{2})\.(\d{2})", str(s))
    if not m:
        return None
    yy, mm, dd = m.groups()
    try:
        return pd.Timestamp(year=2000 + int(yy), month=int(mm), day=int(dd))
    except Exception:
        return None


_BACKUP_CACHE = {"series": None, "ts": None}
_CACHE_TTL_SEC = 300  # 5분 — 같은 실행 내 계좌 여러 개가 연속 호출해도 시트는 1번만 읽음


def fetch_backup_soxl_closes(gc=None) -> pd.Series:
    """백업 시트 DB 탭에서 SOXL 일별 종가 시리즈 반환 (index=날짜). 5분 캐시."""
    import time as _time
    now = _time.time()
    if (_BACKUP_CACHE["series"] is not None and _BACKUP_CACHE["ts"] is not None
            and now - _BACKUP_CACHE["ts"] < _CACHE_TTL_SEC):
        return _BACKUP_CACHE["series"]
    if gc is None:
        gc = _default_gspread_client()
    sh = gc.open_by_url(BACKUP_SHEET_URL)
    ws = sh.worksheet(BACKUP_DB_TAB)
    # E열(5)=날짜, F열(6)=종가 — 행5부터 데이터
    vals = ws.get_values("E5:F")
    dates, closes = [], []
    for row in vals:
        if len(row) < 2:
            continue
        d = _parse_db_date(row[0])
        if d is None:
            continue
        c = str(row[1]).replace("$", "").replace(",", "").strip()
        try:
            closes.append(float(c))
            dates.append(d)
        except Exception:
            continue
    series = pd.Series(closes, index=pd.DatetimeIndex(dates), name="Close")
    _BACKUP_CACHE["series"] = series
    _BACKUP_CACHE["ts"] = now
    return series


def expected_latest_trading_date(now_est=None) -> pd.Timestamp:
    """현 시각 기준 '종가가 확정됐어야 하는' 최신 미국 거래일.

    16:30 ET(마감+버퍼) 이후면 오늘(거래일인 경우), 이전이면 직전 거래일.
    """
    from dss_engine import _is_us_trading_day  # 지연 import (순환 방지)
    if now_est is None:
        now_est = pd.Timestamp.now(tz="America/New_York")
    market_close = now_est.replace(hour=16, minute=30, second=0, microsecond=0)
    d = pd.Timestamp(now_est.date())
    if now_est < market_close:
        d -= pd.Timedelta(days=1)  # 오늘 종가는 아직 미확정 → 어제부터 탐색
    for _ in range(10):
        if _is_us_trading_day(d):
            return d
        d -= pd.Timedelta(days=1)
    return d


def check_and_patch_soxl(df: pd.DataFrame, gc=None):
    """yfinance SOXL 데이터의 신선도 확인 + 누락 시 백업 시트로 보충.

    Args:
        df: yfinance 일봉 DataFrame (Close 컬럼 필수, NaN 행은 사전 제거 권장)
        gc: gspread client (None이면 자체 인증 시도)

    Returns:
        (df, warning_msg, resolved)
        - 정상(최신):  (원본 df, None, True)
        - 보충 성공:   (보충된 df, "⚠️ ... 백업으로 보충", True)
        - 보충 실패:   (원본 df, "⛔ ... 실패" 메시지, False)
          → resolved=False 면 최신 전일 종가 미확보 상태.
            호출자는 주문표 생성/발송을 중단하고 에러로 처리해야 함.
    """
    if df is None or df.empty or "Close" not in df.columns:
        return df, None, True
    try:
        expected = expected_latest_trading_date()
    except Exception:
        return df, None, True

    valid = df[df["Close"].notna()]
    if valid.empty:
        return df, "⛔ yfinance 데이터 전체가 비정상 (유효 종가 없음)", False
    last_valid = pd.Timestamp(valid.index[-1]).normalize()

    if last_valid >= expected:
        # 마지막 날이 최신이어도 '중간 구멍'이 있을 수 있음
        # (2026-08-06 사고: yfinance가 8/4 하루만 빼고 반환 → 마지막 날
        #  검사는 통과 → 백테스트 왜곡 → 텔레그램 매수수량 오발송 400 vs 404)
        return _patch_mid_holes(df, gc)

    # stale: expected까지의 누락 거래일을 백업에서 보충
    missing_desc = f"{last_valid.date()} 이후 ~ {expected.date()}"
    try:
        backup = fetch_backup_soxl_closes(gc)
    except Exception as e:
        return df, (f"⛔ 전일 종가 미확보: yfinance 누락({missing_desc}) "
                    f"+ 백업 시트 접근 실패({e})"), False

    patched = df.copy()
    added = []
    d = last_valid + pd.Timedelta(days=1)
    while d <= expected:
        if d in backup.index:
            c = float(backup.loc[d])
            patched.loc[d, "Close"] = c
            added.append(f"{d.strftime('%m/%d')}=${c:,.2f}")
        d += pd.Timedelta(days=1)
    patched = patched.sort_index()

    # 보충 후 최신 종가(expected)가 실제로 확보됐는지 재확인
    patched_valid = patched[patched["Close"].notna()]
    patched_last = pd.Timestamp(patched_valid.index[-1]).normalize() if not patched_valid.empty else last_valid

    if patched_last >= expected:
        # 꼬리 보충 성공 → 중간 구멍도 이어서 검사
        patched2, hole_warn, hole_ok = _patch_mid_holes(patched, gc)
        tail_warn = (f"⚠️ yfinance 종가 누락 감지 → 백업 시트(DB)로 보충: "
                     f"{', '.join(added)}")
        warn = tail_warn + (f" / {hole_warn}" if hole_warn else "")
        return patched2, warn, hole_ok
    if added:
        return patched, (f"⛔ 전일 종가 미확보: 백업에서 일부만 보충"
                         f"({', '.join(added)}) — 최신 거래일({expected.date()}) "
                         f"종가는 백업에도 없음"), False
    return df, (f"⛔ 전일 종가 미확보: yfinance 누락({missing_desc}) "
                f"+ 백업 시트에도 해당 날짜 없음"), False


def _patch_mid_holes(df: pd.DataFrame, gc=None, window_days: int = 45):
    """최근 구간의 '중간 구멍'(마지막 날은 있지만 사이에 빠진 거래일) 감지·보충.

    yfinance가 간헐적으로 중간 하루를 빼고 반환하거나 NaN 행(사전 제거됨)으로
    주는 경우, 마지막 날짜 검사만으로는 잡히지 않는다. 주말/휴장일 오탐을
    피하기 위해 1차로 bdate_range 후보를 뽑고, 후보가 있을 때만 백업 DB의
    실제 거래일 목록과 대조해 진짜 누락만 보충한다 (휴장일은 DB에도 없음).

    Returns: (df, warning_msg|None, resolved)  — 보충 실패한 진짜 누락이
             남으면 resolved=False (호출자는 주문 생성/발송 중단).
    """
    try:
        last_d = pd.Timestamp(df.index[-1]).normalize()
        start_d = last_d - pd.Timedelta(days=window_days)
        bdays = pd.bdate_range(start_d, last_d)
        have = set(pd.Timestamp(d).normalize() for d in df.index)
        candidates = [d for d in bdays if d not in have]
    except Exception:
        return df, None, True
    if not candidates:
        return df, None, True

    # 후보가 있을 때만 백업 조회 (휴장일이면 백업에도 없어 자연 제외)
    try:
        backup = fetch_backup_soxl_closes(gc)
    except Exception:
        # 백업 접근 불가 → 후보가 휴장일인지 판별 불가. 차단은 과잉이므로
        # 경고만 남긴다 (마지막 종가는 확보된 상태).
        return df, (f"⚠️ 최근 데이터에 빈 날짜 후보 "
                    f"{[d.strftime('%m/%d') for d in candidates]} — "
                    f"백업 시트 접근 실패로 검증 불가"), True

    real_missing = [d for d in candidates if d in backup.index]
    if not real_missing:
        return df, None, True  # 전부 휴장일 — 정상

    patched = df.copy()
    added = []
    for d in real_missing:
        patched.loc[d, "Close"] = float(backup.loc[d])
        added.append(f"{d.strftime('%m/%d')}=${float(backup.loc[d]):,.2f}")
    patched = patched.sort_index()
    return patched, (f"⚠️ yfinance 중간 누락 감지 → 백업 시트(DB)로 보충: "
                     f"{', '.join(added)}"), True
