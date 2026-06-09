"""
dual_sniper_mode_update.py — 듀얼스나이퍼 오늘 모드 자동 업데이트 (스케줄 실행)

매일 아침(GitHub Actions cron, 10:00 KST) 실행:
  1) 원본 ASTRA/Rocket 시트(L4:O 주문)를 읽어
  2) 주문가에서 공격/방어 모드를 역산하고
  3) 중앙 공유 시트(모드공유 탭)에 (날짜, 모드) 기록.
→ 앱의 '오늘 원전략 모드' 배너 + 원전략 소스 계좌가 이 값을 자동 사용.

필요 환경변수 (GitHub Secrets):
  GCP_SERVICE_ACCOUNT_JSON  — 서비스 계정 JSON (문자열)
  DS_ORIGIN_SHEET_URL       — 원본 Rocket 시트 URL
  DS_ORIGIN_TAB             — 원본 탭 이름 (기본 Rocket)
  DS_SHARE_SHEET_URL        — 중앙 공유 시트 URL (서비스계정 쓰기권한 필요)
  DS_SHARE_TAB              — 공유 탭 이름 (기본 모드공유)
"""
import os
import json
import math
import sys
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials
import yfinance as yf
import pandas as pd

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _nth_weekday(y, mo, wd, n):
    from datetime import date as D, timedelta as T
    first = D(y, mo, 1)
    return first + T(days=(wd - first.weekday()) % 7 + 7 * (n - 1))


def _last_weekday(y, mo, wd):
    from datetime import date as D, timedelta as T
    last = (D(y + 1, 1, 1) if mo == 12 else D(y, mo + 1, 1)) - T(days=1)
    return last - T(days=(last.weekday() - wd) % 7)


def _easter(y):
    from datetime import date as D
    a = y % 19; b, c = divmod(y, 100); d, e = divmod(b, 4)
    f = (b + 8) // 25; g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30; i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    mo, da = divmod(h + l - 7 * m + 114, 31)
    return D(y, mo, da + 1)


def _us_holidays(y):
    from datetime import date as D, timedelta as T
    hs = {D(y, 1, 1), _nth_weekday(y, 1, 0, 3), _nth_weekday(y, 2, 0, 3),
          _easter(y) - T(days=2), _last_weekday(y, 5, 0), D(y, 6, 19), D(y, 7, 4),
          _nth_weekday(y, 9, 0, 1), _nth_weekday(y, 11, 3, 4), D(y, 12, 25)}
    obs = set()
    for h in hs:
        if h.weekday() == 5:
            obs.add(h - T(days=1))
        elif h.weekday() == 6:
            obs.add(h + T(days=1))
    return hs | obs


def _next_trading_day(d):
    """d 다음 NYSE 거래일."""
    from datetime import timedelta as T
    cur = d + T(days=1)
    for _ in range(15):
        if cur.weekday() < 5 and cur not in _us_holidays(cur.year):
            return cur
        cur += T(days=1)
    return cur

# 원전략 기본 파라미터 (모드 역산용 — 표준 듀얼스나이퍼 값)
AG_BUY_PCT = 8.0
SF_BUY1 = -0.6
SF_BUY2 = 5.5
SF_MA_BASE = 3
SF_SELL = 0.7


def _gc():
    info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(info, scopes=_SCOPES)
    return gspread.authorize(creds)


def _read_origin_orders(gc, url, tab):
    ws = gc.open_by_url(url).worksheet(tab)
    vals = ws.get_all_values()
    rows = []
    for r in range(3, min(len(vals), 20)):
        row = vals[r]
        g = row[11].strip() if len(row) > 11 else ""
        m = row[12].strip() if len(row) > 12 else ""
        pr = row[13].strip() if len(row) > 13 else ""
        if g in ("매수", "매도"):
            rows.append((g, m, pr))
    return rows


def _soxl_last2():
    df = yf.download("SOXL", period="1mo", auto_adjust=False, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    closes = df["Close"].dropna()
    return float(closes.iloc[-1]), float(closes.iloc[-2]), closes.index[-1].date()


def _infer_mode(rows, c1, c2):
    def f(x):
        try:
            return float(str(x).replace(",", ""))
        except Exception:
            return None
    buys = [f(p) for (g, m, p) in rows if g == "매수" and m != "MOC" and f(p)]
    sells = [f(p) for (g, m, p) in rows if g == "매도" and m != "MOC" and f(p)]
    ag_plus = math.floor(c1 * (1 + AG_BUY_PCT / 100) * 100) / 100
    ag_minus = math.floor(c1 * 0.999 * 100) / 100
    f1 = 1 + SF_BUY1 / 100
    cond1 = (f1 * (c1 + c2)) / (SF_MA_BASE - f1)
    cond2 = c1 * (1 + SF_BUY2 / 100)
    sf_k = round(min(cond1, cond2), 2)
    if buys:
        bp = buys[0]
        cands = [("공격", ag_plus), ("공격", ag_minus), ("방어", sf_k)]
        best = min(cands, key=lambda c: abs(c[1] - bp))
        if abs(best[1] - bp) < 0.15:
            return best[0], {"buy": bp, "ag+": ag_plus, "ag-": ag_minus, "sf": sf_k}
    if len(sells) >= 2:
        return ("방어" if (max(sells) - min(sells) < 0.02) else "공격"), {"sells": sells}
    if len(sells) == 1:
        fs = 1 + SF_SELL / 100
        sf_sell = (fs * (c1 + c2)) / (SF_MA_BASE - fs)
        return ("방어" if abs(sells[0] - sf_sell) < 0.15 else "공격"), {"sell": sells[0], "sf_sell": sf_sell}
    return None, {}


def _append_shared(gc, url, tab, date_str, mode):
    sh = gc.open_by_url(url)
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab, rows=3000, cols=4)
        ws.append_row(["date", "mode", "기록시각"])
    existing = {r[0] for r in ws.get_all_values()[1:] if r}
    if date_str in existing:
        print(f"[skip] {date_str} 이미 기록됨")
        return False
    ws.append_row([date_str, mode, datetime.now().strftime("%Y-%m-%d %H:%M")])
    print(f"[ok] {date_str} → {mode} 기록 완료")
    return True


def _append_bundle_csv(date_str, mode):
    """로컬 번들 CSV(dual_sniper_modes.csv)에 (날짜,모드) 누적 → 2016~오늘 연속 연결 유지.
    중복 날짜면 skip. 정렬 보장."""
    csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "dual_sniper_modes.csv")
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        df = pd.DataFrame(columns=["date", "mode"])
    if (df["date"].astype(str) == date_str).any():
        print(f"[bundle skip] {date_str} 이미 번들에 있음")
        return False
    df = pd.concat([df, pd.DataFrame([{"date": date_str, "mode": mode}])], ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates("date", keep="last")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    df.to_csv(csv_path, index=False)
    print(f"[bundle ok] {date_str} → {mode} 번들 CSV 누적 (총 {len(df)}행)")
    return True


def main():
    origin_url = os.environ.get("DS_ORIGIN_SHEET_URL", "").strip()
    origin_tab = os.environ.get("DS_ORIGIN_TAB", "Rocket").strip() or "Rocket"
    share_url = os.environ.get("DS_SHARE_SHEET_URL", "").strip()
    share_tab = os.environ.get("DS_SHARE_TAB", "모드공유").strip() or "모드공유"
    if not origin_url or not share_url:
        print("[error] DS_ORIGIN_SHEET_URL / DS_SHARE_SHEET_URL 미설정")
        sys.exit(1)

    gc = _gc()
    rows = _read_origin_orders(gc, origin_url, origin_tab)
    print(f"원본 주문: {rows}")
    c1, c2, last_date = _soxl_last2()
    mode, detail = _infer_mode(rows, c1, c2)
    apply_date = _next_trading_day(last_date)   # 모드가 적용되는 다음 거래일
    print(f"종가기준일 {last_date} (종가 {c1:.2f}/{c2:.2f}) → 적용거래일 {apply_date} "
          f"모드={mode} 근거={detail}")
    if mode is None:
        print("[error] 모드 역산 실패")
        sys.exit(2)
    _append_shared(gc, share_url, share_tab, str(apply_date), mode)
    _append_bundle_csv(str(apply_date), mode)   # 로컬 번들 CSV에도 누적 (2016~오늘 연속)


if __name__ == "__main__":
    main()
