"""
매일 15:00 KST에 GitHub Actions로 실행되는 텔레그램 자동 알림 스크립트.
Google Sheets users 탭의 모든 사용자에게
각자 등록된 ticker_settings 기준으로 ticker별 LOC 주문을 발송.

전략별 발송:
  1. 종가평균매매 (tg_chat_id / tg_token / ticker_settings)
  2. 표준편차매매  (sd_tg_chat_id / sd_tg_token / sd_ticker_settings)
  3. DSS 동파법   (dss_config 내 tg_chat_id / tg_token + 계좌별 발송)
  4. Sigma매매법  (sigma_tg_chat_id / sigma_tg_token / sigma_ticker_settings)
  5. IUO 매매법   (iuo_config 내 tg_chat_id / tg_token + 계좌별 발송)
"""

import os, sys, json, math, time, requests
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# dss_engine.py를 import할 수 있도록 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── 기본값 (ticker_settings에 값 없을 때 fallback) ────────────
DEFAULT_A_BUY      = -0.005
DEFAULT_A_SELL     =  0.009
DEFAULT_SELL_RATIO = 100.0
DEFAULT_DIVISIONS  = 5
DEFAULT_CAPITAL    = 10000.0
DEFAULT_OS_START   = "2024-01-01"

SD_DEFAULT_K_BUY      = 0.65
SD_DEFAULT_K_SELL     = 0.65
SD_DEFAULT_SIGMA_PERIOD = 2
SD_DEFAULT_SELL_RATIO = 75.0
SD_DEFAULT_DIVISIONS  = 5
SD_DEFAULT_RENEWAL    = 5
SD_DEFAULT_CAPITAL    = 20000.0

GS_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

# ── Google Sheets 연결 ────────────────────────────────────────
def get_gspread_client():
    sa_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
    if not sa_json:
        raise RuntimeError("GCP_SERVICE_ACCOUNT_JSON 환경변수가 없습니다.")
    info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(info, scopes=GS_SCOPES)
    return gspread.authorize(creds)

def get_users(client, sheet_url: str, max_retries: int = 5) -> list:
    """Google Sheets에서 사용자 목록 로드 (503 등 일시 오류 시 재시도)."""
    for attempt in range(1, max_retries + 1):
        try:
            sh = client.open_by_url(sheet_url)
            ws = sh.worksheet("users")
            return ws.get_all_records()
        except gspread.exceptions.APIError as e:
            status = e.response.status_code if hasattr(e, 'response') else 0
            if status in (429, 500, 502, 503) and attempt < max_retries:
                wait = 30 * attempt  # 30초, 60초, 90초, 120초
                print(f"  ⚠️ GSheets API {status} 오류 — {wait}초 후 재시도 ({attempt}/{max_retries})")
                time.sleep(wait)
            else:
                raise


# ── 중복 발송 방지 (외부 cron + 백업 cron 둘 다 도는 경우 대비) ──
_SEND_LOG_TAB = "_send_log"


def already_sent_today(client, sheet_url: str) -> bool:
    """오늘(UTC 날짜) 이미 발송했으면 True. _send_log 탭 A1에 마지막 발송일 기록.

    외부 cron(cron-job.org)이 정시 트리거하고, GitHub 내장 백업 cron이
    수시간 뒤 또 실행될 때 중복 발송을 막는다. FILTER_USER(테스트) 모드는
    가드 무시 (호출 측에서 처리)."""
    from datetime import datetime as _dt
    today = _dt.utcnow().strftime("%Y-%m-%d")
    try:
        sh = client.open_by_url(sheet_url)
        try:
            ws = sh.worksheet(_SEND_LOG_TAB)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=_SEND_LOG_TAB, rows=10, cols=2)
            ws.update("A1", [["last_sent_date"]])
            return False
        last = str(ws.acell("A2").value or "").strip()
        return last == today
    except Exception as _e:
        print(f"  ⚠️ 발송 로그 확인 실패 (가드 무시하고 진행): {_e}")
        return False


def mark_sent_today(client, sheet_url: str):
    """오늘 발송 완료 기록 (_send_log A2)."""
    from datetime import datetime as _dt
    today = _dt.utcnow().strftime("%Y-%m-%d")
    now_kst = (_dt.utcnow() + timedelta(hours=9)).strftime("%H:%M")
    try:
        sh = client.open_by_url(sheet_url)
        ws = sh.worksheet(_SEND_LOG_TAB)
        ws.update("A2:B2", [[today, f"KST {now_kst}"]])
    except Exception as _e:
        print(f"  ⚠️ 발송 로그 기록 실패: {_e}")


# ── 가격 데이터 ────────────────────────────────────────────────
def _filter_incomplete_today(df: pd.DataFrame) -> pd.DataFrame:
    """미국 장이 아직 종료되지 않은 당일 intraday 데이터 제거."""
    if df is None or df.empty:
        return df
    try:
        now_est = pd.Timestamp.now(tz="America/New_York")
        market_close_buffer = now_est.replace(hour=16, minute=30,
                                              second=0, microsecond=0)
        if now_est < market_close_buffer:
            us_today = pd.Timestamp(now_est.date())
            return df[df.index.normalize() < us_today]
    except Exception:
        pass
    return df


def fetch_prices(ticker: str, start_date: str) -> pd.DataFrame:
    end = datetime.today() + timedelta(days=1)
    df = yf.download(ticker, start=start_date,
                     end=end.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Close"]].dropna()
    df["Close"] = df["Close"].astype(float)
    df = _filter_incomplete_today(df)
    # SOXL: yfinance 확정 종가 누락 시 백업 구글시트(DB)로 보충
    # 경고는 df.attrs['data_warning'] → 호출자가 user_warnings에 추가
    # ⛔ 백업까지 실패해 최신 전일 종가가 없으면 raise — 낡은 데이터로
    #    주문표를 만들어 발송하면 안 되므로 해당 발송을 차단한다.
    if ticker.upper() == "SOXL":
        from backup_close import check_and_patch_soxl
        df, _warn, _resolved = check_and_patch_soxl(df, gc=None)  # env 인증 (Actions)
        df.attrs['data_warning'] = _warn
        if _warn:
            print(f"    {_warn}")
        if not _resolved:
            raise RuntimeError(_warn or "⛔ 전일 종가 미확보 (yfinance+백업 모두 실패)")
    return df

# ══════════════════════════════════════════════════════════════
# 종가평균매매 관련
# ══════════════════════════════════════════════════════════════
# avg_close_engine 사용 (웹앱과 동일 엔진)
from avg_close_engine import (
    run_portfolio_for_ordersheet as _avg_run_ordersheet,
    buy_limit_price,
)


def calc_today_order(ticker: str, os_start: str,
                     a_buy: float, a_sell: float,
                     sell_ratio: float, divisions: int,
                     capital: float, n_days: int = 2) -> dict:
    """종가평균매매 오늘의 주문 계산.
    웹앱(strategies/avg_close.py:run_portfolio_for_ordersheet)과 동일 로직.
    n_days 파라미터 지원 (사용자 설정 그대로 사용).
    """
    df = fetch_prices(ticker, os_start)
    if df is None or df.empty or len(df) < n_days + 1:
        return {}

    res = _avg_run_ordersheet(
        price_df=df, start_date=os_start, ticker_name=ticker,
        a_buy=a_buy, a_sell=a_sell,
        sell_ratio=sell_ratio, divisions=divisions,
        initial_capital=capital, n_days=n_days,
    )
    if res is None:
        return {}

    # 자동발송 메시지/GSheet에서 사용하는 필드로 변환
    pending = res.get("pending_buys") or [{}]
    buy_qty = int(pending[0].get("수량", 0)) if pending else 0
    shares = int(res.get("shares", 0))
    sell_qty = math.floor(shares * (sell_ratio / 100.0)) if shares > 0 else 0

    return {
        "p1":       float(res.get("p1_now", 0)),
        "p2":       float(res.get("p2_now", 0)),
        "tb":       round(float(res.get("next_buy_primary", 0)), 2),
        "ts":       round(float(res.get("next_sell_target", 0)), 2),
        "shares":   shares,
        "buy_qty":  buy_qty,
        "sell_qty": sell_qty,
        "cash":     float(res.get("cash", 0)),
        "avg_cost": float(res.get("avg_cost", 0)),
    }

def build_avg_message(res: dict, ticker: str) -> str:
    today = datetime.today().strftime("%Y-%m-%d")
    lines = [
        f"📋 *종가평균 주문* ({ticker})",
        f"기준일: {today}",
        f"기준가: p1={res['p1']:.2f} / p2={res['p2']:.2f}",
        "",
    ]
    has_order = False
    if res["buy_qty"] > 0:
        lines.append(f"🔴 매수 LOC {res['buy_qty']}주  ${res['tb']:.2f}")
        has_order = True
    if res["shares"] > 0 and res["sell_qty"] > 0:
        lines.append(f"🔵 매도 LOC {res['sell_qty']}주  ${res['ts']:.2f}")
        has_order = True
    if not has_order:
        lines.append("⬜ 오늘은 주문 없음")
    if res["shares"] > 0:
        lines.append(f"\n📦 보유: {res['shares']}주  |  평단 ${res['avg_cost']:.2f}")
    else:
        lines.append("\n📦 보유 없음 (전량 현금)")
    return "\n".join(lines)

def parse_ticker_settings(user: dict) -> dict:
    raw = str(user.get("ticker_settings", "")).strip()
    if raw:
        try:
            ts = json.loads(raw)
            if isinstance(ts, dict) and ts:
                return ts
        except Exception:
            pass
    if user.get("a_buy"):
        return {
            "SOXL": {
                "a_buy":      float(user.get("a_buy",      DEFAULT_A_BUY)),
                "a_sell":     float(user.get("a_sell",     DEFAULT_A_SELL)),
                "sell_ratio": float(user.get("sell_ratio", DEFAULT_SELL_RATIO)),
                "divisions":  int(float(user.get("divisions", DEFAULT_DIVISIONS))),
                "os_start":   str(user.get("os_start",    DEFAULT_OS_START)).strip() or DEFAULT_OS_START,
                "os_capital": float(user.get("os_capital", DEFAULT_CAPITAL)),
            }
        }
    return {}

# ══════════════════════════════════════════════════════════════
# 표준편차매매 관련
# ══════════════════════════════════════════════════════════════

from stdev_engine import run_stdev_ordersheet as _sd_run_ordersheet


def calc_sd_today_order(ticker: str, os_start: str,
                        k_buy: float, k_sell: float,
                        sigma_period: int, sell_ratio: float,
                        divisions: int, renewal: int,
                        capital: float,
                        pcr: float = 1.0, lcr: float = 1.0) -> dict | None:
    """표준편차매매 오늘의 주문 계산.

    stdev_engine.run_stdev_ordersheet (웹앱과 동일 엔진) 사용.
    """
    buf_start = (pd.to_datetime(os_start) - pd.DateOffset(days=90)).strftime("%Y-%m-%d")
    df = fetch_prices(ticker, buf_start)
    if df is None or df.empty:
        return None

    res = _sd_run_ordersheet(
        price_df=df, start_date=os_start,
        sigma_period=sigma_period, k_buy=k_buy, k_sell=k_sell,
        sell_ratio=sell_ratio, divisions=divisions, renewal=renewal,
        pcr=pcr, lcr=lcr, initial_capital=capital,
    )
    if res is None:
        return None

    return {
        "ticker":         ticker,
        "last_close":     float(res.get("last_close", 0)),
        "sigma_next":     float(res.get("sigma_next", 0)),
        "next_buy_loc":   float(res.get("next_buy_loc", 0)),
        "next_sell_loc":  float(res.get("next_sell_loc", 0)),
        "holdings":       int(res.get("holdings", 0)),
        "avg_cost":       float(res.get("avg_cost", 0)),
        "cash":           float(res.get("cash", 0)),
        "est_buy_qty":    int(res.get("est_buy_qty", 0)),
        "est_sell_qty":   int(res.get("est_sell_qty", 0)),
        "total_invest":   float(res.get("total_invest", capital)),
    }


def build_sd_message(r: dict) -> str:
    today = datetime.today().strftime("%Y-%m-%d")
    pnl   = (r["last_close"] / r["avg_cost"] - 1) * 100 if r["avg_cost"] > 0 else 0
    lines = [
        f"📐 <b>표준편차매매 주문표</b> ({today})",
        f"종목: {r['ticker']}",
        f"직전 종가: ${r['last_close']:,.2f}  |  σ = {r['sigma_next']*100:.3f}%",
        "━━━━━━━━━━━━━━━",
        f"🔴 매수 LOC  ${r['next_buy_loc']:,.2f}  (예상 {r['est_buy_qty']:,}주)",
    ]
    if r["holdings"] > 0:
        lines.append(f"🔵 매도 LOC  ${r['next_sell_loc']:,.2f}  (예상 {r['est_sell_qty']:,}주)")
        lines += [
            "━━━━━━━━━━━━━━━",
            f"📦 보유: {r['holdings']:,}주  |  평단: ${r['avg_cost']:.2f}",
            f"   현재가: ${r['last_close']:.2f}  ({pnl:+.2f}%)  |  현금: ${r['cash']:,.2f}",
        ]
    else:
        lines.append(f"📦 보유주식 없음  |  현금: ${r['cash']:,.2f}")
    lines.append("※ 종가 LOC 주문 기준입니다.")
    return "\n".join(lines)

def parse_sd_ticker_settings(user: dict) -> dict:
    raw = str(user.get("sd_ticker_settings", "")).strip()
    if raw:
        try:
            ts = json.loads(raw)
            if isinstance(ts, dict) and ts:
                return ts
        except Exception:
            pass
    return {}

# ══════════════════════════════════════════════════════════════
# DSS 동파법 관련
# ══════════════════════════════════════════════════════════════

DSS_DEFAULT_PARAMS = {
    "sf_div": 7, "sf_hold": 36, "sf_buy": 4.91, "sf_sell": 0.9,
    "ag_div": 7, "ag_hold": 8, "ag_buy": 2.77, "ag_sell": 3.06,
    "pcr": 70, "lcr": 20, "renewal_period": 10, "fee_rate": 0.04,
}

def calc_dss_order(acct_data: dict) -> dict | None:
    """DSS 계좌 1개의 주문표 데이터 계산."""
    try:
        from dss_engine import (DSSParams, run_backtest, build_weekly_rsi_series,
                                build_mode_series, get_week_mode_map,
                                get_current_week_mode, next_us_trading_days)
    except ImportError as e:
        print(f"    ⚠️ dss_engine import 실패: {e}")
        return None

    # 파라미터 로드
    ap = acct_data.get("params", {})
    sf_div  = ap.get("sf_div",  DSS_DEFAULT_PARAMS["sf_div"])
    sf_hold = ap.get("sf_hold", DSS_DEFAULT_PARAMS["sf_hold"])
    sf_buy  = ap.get("sf_buy",  DSS_DEFAULT_PARAMS["sf_buy"])
    sf_sell = ap.get("sf_sell", DSS_DEFAULT_PARAMS["sf_sell"])
    ag_div  = ap.get("ag_div",  DSS_DEFAULT_PARAMS["ag_div"])
    ag_hold = ap.get("ag_hold", DSS_DEFAULT_PARAMS["ag_hold"])
    ag_buy  = ap.get("ag_buy",  DSS_DEFAULT_PARAMS["ag_buy"])
    ag_sell = ap.get("ag_sell", DSS_DEFAULT_PARAMS["ag_sell"])
    pcr     = ap.get("pcr",     DSS_DEFAULT_PARAMS["pcr"])
    lcr     = ap.get("lcr",     DSS_DEFAULT_PARAMS["lcr"])
    renew   = ap.get("renewal_period", DSS_DEFAULT_PARAMS["renewal_period"])
    fee     = ap.get("fee_rate", DSS_DEFAULT_PARAMS["fee_rate"])

    os_start  = str(acct_data.get("os_start", "2024-01-01"))
    os_capital = float(acct_data.get("os_capital", 100000))

    # 데이터 로드 — 실패(전일 종가 미확보 포함) 시 예외 전파
    # → main에서 해당 계좌 발송 차단 + 사용자 에러 알림
    soxl = fetch_prices("SOXL", "2009-06-01")
    qqq  = fetch_prices("QQQ",  "2009-01-01")

    if soxl.empty or qqq.empty:
        raise RuntimeError("가격 데이터 비어있음 (yfinance)")

    # yfinance 이상 → 백업 보충 경고 (fetch_prices가 attrs에 저장)
    _data_warning = soxl.attrs.get('data_warning')

    # RSI/모드 계산
    weekly_rsi_df = build_weekly_rsi_series(qqq)
    mode_series_df = build_mode_series(weekly_rsi_df)

    # 백테스트
    dss_params = DSSParams(
        sf_divisions=sf_div, sf_max_hold=sf_hold,
        sf_buy_pct=sf_buy / 100, sf_sell_pct=sf_sell / 100,
        ag_divisions=ag_div, ag_max_hold=ag_hold,
        ag_buy_pct=ag_buy / 100, ag_sell_pct=ag_sell / 100,
        initial_capital=os_capital,
        fee_rate=fee / 100, renewal_period=renew,
        pcr=pcr / 100, lcr=lcr / 100,
    )

    today_str = datetime.today().strftime("%Y-%m-%d")
    # 자본 조정 이력 추출 (엔진이 백테스트 도중 직접 적용)
    _adj_hist = acct_data.get("capital_adj_history", [])
    if not isinstance(_adj_hist, list):
        _adj_hist = []
    bt_df = run_backtest(dss_params, soxl, mode_series_df, os_start, today_str,
                          capital_adj_history=_adj_hist)

    # 최신 데이터 — 엔진(dss_engine.run_backtest)과 동일하게 round(2) 적용
    # yfinance float32 정밀도 차이로 매수주문가 1¢ 어긋남 방지
    prev_close = round(float(soxl.iloc[-1]['Close']), 2)
    last_date = soxl.index[-1]
    # 다음 거래일(오늘의 주문 적용일) 모드 결정.
    # mode_map에 있으면 그 값, 없으면(새 주 시작) 2주치 RSI로 직접 판정.
    _next_td = next_us_trading_days(last_date, 1)
    _next_date = _next_td[0] if len(_next_td) > 0 else last_date
    mode_map = get_week_mode_map(mode_series_df, soxl.index)
    if _next_date in mode_map:
        last_mode = mode_map[_next_date]
    else:
        last_mode = get_current_week_mode(mode_series_df)

    if last_mode == "AG":
        cur_div = ag_div; cur_buy_pct = ag_buy / 100; cur_sell_pct = ag_sell / 100; cur_hold = ag_hold
    else:
        cur_div = sf_div; cur_buy_pct = sf_buy / 100; cur_sell_pct = sf_sell / 100; cur_hold = sf_hold

    # RSI 정보
    latest_rsi = float(weekly_rsi_df.iloc[-1]['rsi']) if len(weekly_rsi_df) > 0 else None
    prev_rsi = float(weekly_rsi_df.iloc[-2]['rsi']) if len(weekly_rsi_df) > 1 else None

    # 백테스트에서 포지션 추출
    open_positions = []
    n_pos = 0
    cash = os_capital
    total_asset = os_capital
    capital = os_capital  # 엔진 현재 투자금 (PCR/LCR로 갱신된 값)
    cum_realized = 0
    sell_count = 0
    holding_value = 0
    engine_last_date = None

    if bt_df is not None and not bt_df.empty:
        last_row = bt_df.iloc[-1]
        cash = float(last_row['예수금'])
        total_asset = float(last_row['총자산'])
        capital = float(last_row['투자금'])  # ← 웹앱 UI와 동일: 엔진 갱신된 투자금
        cum_realized = float(last_row['누적실현'])
        sell_count = int(last_row['누적매도'])
        try:
            engine_last_date = pd.Timestamp(last_row['날짜'])
        except Exception:
            engine_last_date = pd.Timestamp(last_date)

        # 미체결 포지션 수집
        _all_sold = set()
        for _, row in bt_df.iterrows():
            if row['매도내역'] is not None:
                for sr in row['매도내역']:
                    _all_sold.add((pd.Timestamp(sr['buy_date']), sr['buy_price'], sr['qty']))

        for _, row in bt_df.iterrows():
            if row['매수체결'] and row['수량'] > 0:
                key = (pd.Timestamp(row['날짜']), float(row['매수체결']), int(row['수량']))
                if key not in _all_sold:
                    open_positions.append({
                        'buy_date': pd.Timestamp(row['날짜']),
                        'buy_price': float(row['매수체결']),
                        'qty': int(row['수량']),
                        'sell_target': float(row['매도목표가']) if row['매도목표가'] is not None else None,
                        'stop_date': pd.Timestamp(row['손절예정일']) if row['손절예정일'] is not None else None,
                        'mode': row['모드'],
                    })

        n_pos = len(open_positions)
        holding_value = sum(p['qty'] * prev_close for p in open_positions)

    # 자본 조정 합계 (UI 표시용 — 엔진이 이미 적용했으므로 여기서 더하지 않음)
    adj_applied = 0.0
    if isinstance(_adj_hist, list) and engine_last_date is not None:
        for _item in _adj_hist:
            try:
                _dt = pd.Timestamp(_item.get("날짜"))
                if _dt <= engine_last_date:
                    adj_applied += float(_item.get("조정금액", 0))
            except Exception:
                continue

    next_buy_order = math.floor(prev_close * (1 + cur_buy_pct) * 100) / 100
    # 엔진의 capital(투자금)은 PCR 갱신 + 자본 조정 모두 이미 반영된 값
    seed_per_trade = capital / cur_div if cur_div > 0 else capital
    buy_qty_est = int(seed_per_trade / next_buy_order) if next_buy_order > 0 else 0

    return {
        "prev_close": prev_close, "last_date": last_date, "last_mode": last_mode,
        "n_pos": n_pos, "total_asset": total_asset, "cash": cash,
        "holding_value": holding_value, "cum_realized": cum_realized,
        "sell_count": sell_count, "open_positions": open_positions,
        "next_buy_order": next_buy_order, "seed_per_trade": seed_per_trade,
        "buy_qty_est": buy_qty_est, "cur_divisions": cur_div,
        "cur_buy_pct": cur_buy_pct, "cur_sell_pct": cur_sell_pct,
        "cur_max_hold": cur_hold, "os_capital": os_capital,
        "latest_rsi": latest_rsi, "prev_rsi": prev_rsi,
        "data_warning": _data_warning,
    }


def build_dss_message(os_result: dict, acct_name: str = "") -> str:
    """DSS 주문표 텔레그램 메시지 생성."""
    _o = os_result
    mode_icon = "🟢" if _o["last_mode"] == "AG" else "🔵"
    mode_label = "공세" if _o["last_mode"] == "AG" else "안전"
    _today_str = datetime.today().strftime('%Y-%m-%d')
    _acct_label = f" [{acct_name}]" if acct_name else ""

    lines = [
        f"<b>📋 DSS 동파법 — SOXL 주문표{_acct_label}</b>",
        f"📅 {_today_str}  {mode_icon} {mode_label}모드",
        f"",
        f"전일종가: <b>${_o['prev_close']:,.2f}</b>",
        f"총자산: <b>${_o['total_asset']:,.0f}</b>  (현금 ${_o['cash']:,.0f})",
        f"보유: {_o['n_pos']}/{_o['cur_divisions']}시드",
    ]

    _today_ts = pd.Timestamp(datetime.today().date())
    _tdays = pd.DatetimeIndex([])
    try:
        from dss_engine import next_us_trading_days
        _soxl = fetch_prices("SOXL", "2024-01-01")
        _raw_idx = _soxl.index
        # NYSE 휴장일 제외한 미래 60거래일 추가 (Memorial Day 등 제외)
        _extra_bdays = next_us_trading_days(_raw_idx[-1], 60)
        _tdays = _raw_idx.append(_extra_bdays)
    except Exception:
        pass

    # 포지션 데이터 수집
    _pos_data = []
    for i, pos in enumerate(_o['open_positions']):
        if pos['sell_target'] is None:
            continue
        _stop = pos.get('stop_date')
        _is_stop = False
        _remain = None
        _rdate = ""
        if _stop is not None:
            _stop_ts = pd.Timestamp(_stop)
            _is_stop = (_stop_ts <= _today_ts)
            if not _is_stop and len(_tdays) > 0:
                _future = _tdays[(_tdays > _today_ts) & (_tdays <= _stop_ts)]
                _remain = len(_future)
                _before = _tdays[_tdays < _stop_ts]
                if len(_before) > 0:
                    _rdate = _before[-1].strftime('%m/%d').replace('/0', '/').lstrip('0')
        _pos_data.append((pos, i, _is_stop, _remain, _rdate))

    # ★ 마커: 전 거래일 매수 포지션만 (오늘 새로 예약해야 하는 것)
    _latest_td = pd.Timestamp(_o['last_date'])
    def _is_new_buy(pos):
        try:
            return pd.Timestamp(pos.get('buy_date')) == _latest_td
        except Exception:
            return False
    _reserve_tiers = {i + 1 for pos, i, _is_stop, _, _ in _pos_data
                      if not _is_stop and _is_new_buy(pos)}

    # 오늘의 주문 (전체 매도/매수)
    lines.append(f"")
    lines.append(f"── 오늘의 주문 ──")
    for pos, i, _is_stop, _remain, _rdate in _pos_data:
        _tier = i + 1
        _star = "★" if _tier in _reserve_tiers else ""
        if _is_stop:
            lines.append(f"🔴 MOC매도 티어{_tier}: 시장가 × {pos['qty']}주 (손절일)")
        else:
            pnl_pct = (pos['sell_target'] / pos['buy_price'] - 1) * 100
            lines.append(
                f"📈 LOC매도 {_star}티어{_tier}: ${pos['sell_target']:,.2f} "
                f"× {pos['qty']}주 ({pnl_pct:+.1f}%)")
    if _o['n_pos'] < _o['cur_divisions']:
        lines.append(
            f"📉 LOC매수 티어{_o['n_pos']+1}: ${_o['next_buy_order']:,.2f} "
            f"× {_o['buy_qty_est']}주")
    else:
        lines.append(f"⚠️ 전 슬롯 사용 중 — 매수 없음")

    # 오늘 새로 예약할 매도 (전 거래일 매수분만 — 이전 예약은 증권사에 살아있음)
    _reserve_lines = []
    for pos, i, _is_stop, _remain, _rdate in _pos_data:
        if _is_stop:
            continue
        if not _is_new_buy(pos):
            continue  # 전 거래일 매수분만
        _tier = i + 1
        _deadline = f"예약~{_rdate} (잔여 {_remain}일)" if _remain is not None and _rdate else ""
        _reserve_lines.append(f" ★티어{_tier}: ${pos['sell_target']:,.2f} {_deadline}")
    if _reserve_lines:
        lines.append(f"")
        lines.append(f"── 오늘 새로 예약 ──")
        lines.extend(_reserve_lines)

    if _o.get('latest_rsi'):
        lines.append(f"")
        lines.append(f"QQQ RSI: {_o['latest_rsi']:.1f}")
    return "\n".join(lines)


def parse_dss_config(user: dict) -> dict:
    """users 행에서 dss_config JSON 파싱."""
    raw = str(user.get("dss_config", "")).strip()
    if raw:
        try:
            cfg = json.loads(raw)
            if isinstance(cfg, dict):
                return cfg
        except Exception:
            pass
    return {}


def _parse_sheet_number(val):
    """시트 셀 값에서 숫자 추출 ('$188.56', '1,234', 188.56 등 → float).
    실패 시 None."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace("$", "").replace(",", "").replace("%", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def verify_dss_against_board(client, os_result: dict, verify_url: str,
                             verify_sheet: str, acct_name: str = "") -> list:
    """앱 계산 주문표를 알고리C 원본 시트 BOARD 탭과 대조.

    BOARD 탭 고정 셀:
      B3  = 모드 ("공세모드"/"안전모드")
      B7  = 당일 매수가,  C7 = 매수 수량

    매수가/수량은 '$188.56', '343' 등 문자열일 수 있어 _parse_sheet_number로 정규화.

    Returns: 불일치 메시지 리스트 (일치하면 빈 리스트)
    """
    issues = []
    try:
        sh = client.open_by_url(verify_url)
        try:
            ws = sh.worksheet(verify_sheet)
        except Exception:
            return [f"⚠️ 검증시트 탭 '{verify_sheet}' 없음"]

        board_mode_raw = str(ws.cell(3, 2).value or "").strip()
        board_buy_price = _parse_sheet_number(ws.cell(7, 2).value)
        board_buy_qty = _parse_sheet_number(ws.cell(7, 3).value)

        # 시트 모드 → AG/SF 변환
        board_mode = "AG" if "공세" in board_mode_raw else ("SF" if "안전" in board_mode_raw else "?")
        app_mode = os_result.get("last_mode", "?")

        # 1. 모드 비교
        if board_mode in ("AG", "SF") and app_mode != board_mode:
            issues.append(
                f"모드 불일치: 앱={app_mode} vs 시트={board_mode}({board_mode_raw})")

        # 2. 매수가 비교 (시트에 매수 주문이 있을 때만)
        app_buy = os_result.get("next_buy_order")
        can_buy = os_result.get("n_pos", 0) < os_result.get("cur_divisions", 0)
        if board_buy_price is not None and can_buy and app_buy is not None:
            bp = round(board_buy_price, 2)
            ap = round(float(app_buy), 2)
            if abs(bp - ap) > 0.01:
                issues.append(f"매수가 불일치: 앱=${ap:.2f} vs 시트=${bp:.2f}")

        # 3. 매수 수량 비교
        app_qty = os_result.get("buy_qty_est")
        if board_buy_qty is not None and can_buy and app_qty is not None:
            bq = int(board_buy_qty)
            aq = int(app_qty)
            if bq != aq:
                issues.append(f"매수수량 불일치: 앱={aq}주 vs 시트={bq}주")

    except Exception as e:
        issues.append(f"검증 실패: {e}")
    return issues


def parse_dss_verify_sheets(dss_cfg: dict) -> dict:
    """dss_config에서 계좌별 검증시트 매핑 반환.
    신구조: verify_sheets = {계좌명: {"url": ..., "sheet": ...}}
    레거시: verify_url(공용) + verify_sheets = {계좌명: 탭이름(문자열)}
    Returns: {계좌명: {"url": ..., "sheet": ...}}
    """
    legacy_url = str(dss_cfg.get("verify_url", "")).strip()
    raw = dss_cfg.get("verify_sheets", {})
    if not isinstance(raw, dict):
        return {}
    result = {}
    for acct, val in raw.items():
        if isinstance(val, dict):
            url = str(val.get("url", "")).strip()
            sheet = str(val.get("sheet", "")).strip()
        else:
            # 레거시: 값이 탭이름 문자열 → 공용 url 사용
            url = legacy_url
            sheet = str(val).strip()
        if url and sheet:
            result[acct] = {"url": url, "sheet": sheet}
    return result


# ══════════════════════════════════════════════════════════════
# Sigma매매법 관련
# ══════════════════════════════════════════════════════════════

SIGMA_DEFAULT_PERIOD   = 252
SIGMA_DEFAULT_CAPITAL  = 100000.0
SIGMA_DEFAULT_DIVISIONS = 20

def calc_sigma_order(ticker: str, sigma_period: int = 252) -> dict | None:
    """Sigma매매법 오늘의 주문 데이터 계산."""
    buf_days = sigma_period + 90
    start = (datetime.today() - timedelta(days=int(buf_days * 1.5))).strftime("%Y-%m-%d")
    df = fetch_prices(ticker, start)
    if df is None or df.empty or len(df) < sigma_period + 2:
        return None

    closes = df["Close"].values.astype(float)
    prev_close = float(closes[-1])

    # σ, μ 계산 (전일까지의 period 기간)
    end_idx = len(closes) - 1
    start_idx = max(0, end_idx - sigma_period + 1)
    window = closes[start_idx:end_idx + 1]
    if len(window) < max(10, sigma_period // 4):
        return None
    returns = np.diff(window) / window[:-1]
    if len(returns) < 2:
        return None
    mu = float(np.mean(returns))
    sigma = float(np.std(returns, ddof=0))
    rolling_max = float(np.max(window))

    buy_loc_1 = prev_close * (1 + mu - 1 * sigma)
    buy_loc_2 = prev_close * (1 + mu - 2 * sigma)
    buy_loc_3 = prev_close * (1 + mu - 3 * sigma)

    return {
        "ticker": ticker, "prev_close": prev_close,
        "mu_pct": mu * 100, "sigma_pct": sigma * 100,
        "rolling_max": rolling_max,
        "buy_loc_1": buy_loc_1, "buy_loc_2": buy_loc_2, "buy_loc_3": buy_loc_3,
        "sigma_period": sigma_period,
        "as_of": df.index[-1].strftime("%Y-%m-%d"),
    }


def build_sigma_message(od: dict, capital: float, divisions: int) -> str:
    """Sigma매매법 텔레그램 메시지 생성."""
    today = datetime.today().strftime("%Y-%m-%d")
    amount_per_trade = capital / max(divisions, 1)
    qty1 = math.floor(amount_per_trade / od["buy_loc_1"]) if od["buy_loc_1"] > 0 else 0

    lines = [
        f"📐 <b>Sigma매매 주문표</b> ({od['ticker']})",
        f"📅 {today}  |  기준: {od['as_of']}",
        f"전일종가: <b>${od['prev_close']:.2f}</b>",
        f"μ: {od['mu_pct']:+.4f}%  |  σ: {od['sigma_pct']:.4f}%",
        f"━━━━━━━━━━━━━━━",
        f"🔴 <b>매수 LOC (1σ 기준)</b>",
        f"  <b>${od['buy_loc_1']:.2f}</b>  {qty1}주  (≈${qty1 * od['buy_loc_1']:,.0f})",
        f"  2σ: ${od['buy_loc_2']:.2f}  |  3σ: ${od['buy_loc_3']:.2f}",
        f"ℹ️ σ 계산 기간: {od['sigma_period']}거래일",
    ]
    return "\n".join(lines)


def parse_sigma_ticker_settings(user: dict) -> dict:
    """users 행에서 sigma_ticker_settings JSON 파싱."""
    raw = str(user.get("sigma_ticker_settings", "")).strip()
    if raw:
        try:
            ts = json.loads(raw)
            if isinstance(ts, dict) and ts:
                return ts
        except Exception:
            pass
    return {}


# ══════════════════════════════════════════════════════════════
# IUO 매매법 관련
# ══════════════════════════════════════════════════════════════

IUO_DEFAULT_PARAMS = {
    "first_buy_ratio": 33, "buy0_pct": 0.0, "buy1_pct": -1.8, "buy2_pct": -10.0,
    "sell_pct": 4.3, "moc_days": 18, "max_add_buys": 7, "divisions": 8,
    "fee_rate": 0.0,
}

def calc_iuo_order(acct_data: dict) -> dict | None:
    """IUO 계좌 1개의 주문표 데이터 계산."""
    try:
        from iuo_engine import IUOParams, run_backtest
    except ImportError as e:
        print(f"    ⚠️ iuo_engine import 실패: {e}")
        return None

    ap = acct_data.get("params", {})
    ticker    = str(acct_data.get("ticker", "SOXL"))
    os_start  = str(acct_data.get("os_start", "2024-01-01"))
    os_capital = float(acct_data.get("os_capital", 10000))

    fbr = ap.get("first_buy_ratio", IUO_DEFAULT_PARAMS["first_buy_ratio"]) / 100
    b0  = ap.get("buy0_pct",  IUO_DEFAULT_PARAMS["buy0_pct"]) / 100
    b1  = ap.get("buy1_pct",  IUO_DEFAULT_PARAMS["buy1_pct"]) / 100
    b2  = ap.get("buy2_pct",  IUO_DEFAULT_PARAMS["buy2_pct"]) / 100
    sp  = ap.get("sell_pct",  IUO_DEFAULT_PARAMS["sell_pct"]) / 100
    moc = ap.get("moc_days",  IUO_DEFAULT_PARAMS["moc_days"])
    max_add = ap.get("max_add_buys", IUO_DEFAULT_PARAMS["max_add_buys"])
    div = ap.get("divisions", IUO_DEFAULT_PARAMS["divisions"])
    fee = ap.get("fee_rate",  IUO_DEFAULT_PARAMS["fee_rate"]) / 100

    # 데이터 로드 — 실패(전일 종가 미확보 포함) 시 예외 전파
    # → main에서 해당 발송 차단 + 사용자 에러 알림
    price_df = fetch_prices(ticker, "2009-01-01")
    if price_df.empty:
        raise RuntimeError(f"{ticker} 가격 데이터 비어있음 (yfinance)")

    params = IUOParams(
        initial_capital=os_capital,
        first_buy_ratio=fbr, buy0_pct=b0, buy1_pct=b1, buy2_pct=b2,
        sell_pct=sp, moc_days=moc, max_additional_buys=max_add,
        divisions=div, fee_rate=fee,
    )

    today_str = datetime.today().strftime("%Y-%m-%d")
    result = run_backtest(params, price_df, None, os_start, today_str)
    if not result or not result.get("daily_log"):
        return None

    last_row = result["daily_log"][-1]
    last_close = float(last_row["종가"])
    cur_shares = int(last_row["보유수량"])
    avg_cost   = float(last_row["평단가"])
    last_buy_close = float(last_row.get("마지막매수종가", 0))
    add_count  = int(last_row.get("추가매수횟수", 0))
    cycle_day  = int(last_row.get("진행일", 0))
    cash       = float(last_row["예수금"])
    total_asset = float(last_row["총자산"])
    cum_realized = float(last_row["누적실현손익"])
    cycle_base = float(last_row.get("매수기준액", os_capital))

    buy0_loc = round(last_close * (1 + b0), 2)
    buy1_loc = round(last_close * (1 + b1), 2)
    buy2_loc = round(last_close * (1 + b2), 2)
    sell_loc = round(last_buy_close * (1 + sp), 2) if last_buy_close > 0 else None

    remaining = max_add - add_count

    return {
        "ticker": ticker, "last_close": last_close,
        "cur_shares": cur_shares, "avg_cost": avg_cost,
        "last_buy_close": last_buy_close, "add_count": add_count,
        "cycle_day": cycle_day, "remaining": remaining,
        "cash": cash, "total_asset": total_asset,
        "cum_realized": cum_realized, "cycle_base": cycle_base,
        "buy0_loc": buy0_loc, "buy1_loc": buy1_loc, "buy2_loc": buy2_loc,
        "sell_loc": sell_loc,
        "fbr": fbr, "b0": b0, "b1": b1, "b2": b2, "sp": sp,
        "moc_days": moc, "max_add": max_add, "div": div,
        "sell_count": result["metrics"]["총매도횟수"],
    }


def build_iuo_message(o: dict, acct_name: str = "") -> str:
    """IUO 주문표 텔레그램 메시지 생성."""
    today = datetime.today().strftime("%Y-%m-%d")
    _acct = f" [{acct_name}]" if acct_name else ""

    lines = [
        f"<b>📊 IUO 매매법 — {o['ticker']} 주문표{_acct}</b>",
        f"📅 {today}",
        f"전일종가: <b>${o['last_close']:,.2f}</b>",
        f"총자산: <b>${o['total_asset']:,.0f}</b>  (현금 ${o['cash']:,.0f})",
        f"━━━━━━━━━━━━━━━",
    ]

    if o["cur_shares"] == 0:
        # 첫매수
        first_amt = o["cycle_base"] * o["fbr"]
        qty0 = math.floor(first_amt / o["buy0_loc"]) if o["buy0_loc"] > 0 else 0
        lines.append(f"🟢 첫매수 LOC: <b>${o['buy0_loc']:,.2f}</b>  {qty0:,}주")
        lines.append(f"   매수기준액 ${o['cycle_base']:,.0f} × {o['fbr']*100:.0f}%")
    else:
        # 매도
        if o["sell_loc"]:
            pnl = (o["sell_loc"] / o["avg_cost"] - 1) * 100 if o["avg_cost"] > 0 else 0
            lines.append(
                f"📈 매도 LOC: <b>${o['sell_loc']:,.2f}</b>  "
                f"{o['cur_shares']:,}주 전량 ({pnl:+.1f}%)")
            if o["cycle_day"] >= o["moc_days"]:
                lines.append(f"⚠️ 시간청산 도래 ({o['cycle_day']}일) — MOC 매도 대비")
        # 추가매수
        if o["remaining"] > 0:
            buy_amt = (o["cycle_base"] - o["cash"]) / o["div"] if o["div"] > 0 else 0
            if buy_amt <= 0:
                buy_amt = o["cycle_base"] / o["div"] if o["div"] > 0 else 0
            qty1 = math.floor(buy_amt / o["buy1_loc"]) if o["buy1_loc"] > 0 else 0
            qty2 = math.floor(buy_amt / o["buy2_loc"]) if o["buy2_loc"] > 0 else 0
            lines.append(
                f"🔴 추가매수1 LOC: <b>${o['buy1_loc']:,.2f}</b>  "
                f"{qty1:,}주 ({o['b1']*100:+.1f}%)")
            if o["remaining"] >= 2:
                lines.append(
                    f"🔴 추가매수2 LOC: <b>${o['buy2_loc']:,.2f}</b>  "
                    f"{qty2:,}주 ({o['b2']*100:+.1f}%)")
            lines.append(f"   잔여 {o['remaining']}회 │ 진행일 {o['cycle_day']}일")
        else:
            lines.append(f"⚠️ 추가매수 한도 도달 ({o['max_add']}회)")

    if o["cur_shares"] > 0:
        eval_pnl = (o["last_close"] / o["avg_cost"] - 1) * 100 if o["avg_cost"] > 0 else 0
        lines.append(f"━━━━━━━━━━━━━━━")
        lines.append(
            f"📦 보유: {o['cur_shares']:,}주  |  평단 ${o['avg_cost']:.2f}  "
            f"({eval_pnl:+.1f}%)")

    return "\n".join(lines)


def parse_iuo_config(user: dict) -> dict:
    """users 행에서 iuo_config JSON 파싱."""
    raw = str(user.get("iuo_config", "")).strip()
    if raw:
        try:
            cfg = json.loads(raw)
            if isinstance(cfg, dict):
                return cfg
        except Exception:
            pass
    return {}


# ══════════════════════════════════════════════════════════════
# 🔍 이상치 감지 (Sanity Check) — 모든 전략 공통
# ══════════════════════════════════════════════════════════════

def _check_qty_sanity(qty, label: str = "수량") -> list:
    """수량이 비정상적인지 체크."""
    msgs = []
    try:
        q = int(qty)
        if q < 0:
            msgs.append(f"⚠️ {label}이 음수({q})")
        elif q > 10000:
            msgs.append(f"⚠️ {label} 과다({q:,}주) — 계산 오류 가능성")
    except Exception:
        msgs.append(f"⚠️ {label} 파싱 실패 ({qty})")
    return msgs


def _check_price_sanity(price, prev_close, label: str = "주문가") -> list:
    """가격이 전일종가 대비 ±20% 넘으면 경고."""
    msgs = []
    try:
        p = float(price)
        pc = float(prev_close)
        if pc > 0 and p > 0:
            diff = abs(p / pc - 1)
            if diff > 0.20:
                msgs.append(
                    f"⚠️ {label} ${p:.2f} 가 전일종가 ${pc:.2f} 대비 "
                    f"{diff*100:.0f}% 차이 — 데이터 이상 가능성"
                )
    except Exception:
        pass
    return msgs


def sanity_check_avg(res: dict, tk: str) -> list:
    """종가평균매매 이상치 감지."""
    if not res:
        return []
    issues = []
    issues.extend(_check_qty_sanity(res.get("buy_qty", 0), "매수수량"))
    if res.get("shares", 0) > 0:
        issues.extend(_check_qty_sanity(res.get("sell_qty", 0), "매도수량"))
    issues.extend(_check_price_sanity(res.get("tb", 0), res.get("p1", 0), "매수LOC"))
    issues.extend(_check_price_sanity(res.get("ts", 0), res.get("p1", 0), "매도LOC"))
    return issues


def sanity_check_sd(r: dict, tk: str) -> list:
    """표준편차매매 이상치 감지."""
    if not r:
        return []
    issues = []
    sigma = r.get("sigma_next", 0)
    if sigma is not None:
        try:
            s = float(sigma)
            if s < 0.0005:
                issues.append(f"⚠️ σ 값 비정상 낮음 ({s*100:.4f}%) — 데이터 부족 가능성")
            elif s > 0.20:
                issues.append(f"⚠️ σ 값 비정상 높음 ({s*100:.2f}%) — 극단 변동성")
        except Exception:
            pass
    issues.extend(_check_qty_sanity(r.get("est_buy_qty", 0), "매수수량"))
    issues.extend(_check_qty_sanity(r.get("est_sell_qty", 0), "매도수량"))
    issues.extend(_check_price_sanity(r.get("next_buy_loc", 0),
                                      r.get("last_close", 0), "매수LOC"))
    issues.extend(_check_price_sanity(r.get("next_sell_loc", 0),
                                      r.get("last_close", 0), "매도LOC"))
    return issues


def sanity_check_sigma(od: dict) -> list:
    """Sigma매매법 이상치 감지."""
    if not od:
        return []
    issues = []
    sigma_pct = od.get("sigma_pct", 0)
    try:
        s = float(sigma_pct)
        if s < 0.05:
            issues.append(f"⚠️ σ {s:.3f}% 비정상 낮음 — 데이터 부족 가능성")
        elif s > 20:
            issues.append(f"⚠️ σ {s:.2f}% 비정상 높음 — 극단 변동성")
    except Exception:
        pass
    issues.extend(_check_price_sanity(od.get("buy_loc_1", 0),
                                      od.get("prev_close", 0), "1σ 매수LOC"))
    return issues


def sanity_check_dss(os_result: dict, acct_name: str, acct_data: dict) -> list:
    """DSS 이상치 감지 (가장 중요 — 과거 버그가 많았음)."""
    if not os_result:
        return []
    issues = []

    # 1. last_date 가 오늘이면 intraday 오염 의심
    try:
        last_date = pd.Timestamp(os_result.get("last_date"))
        today = pd.Timestamp(datetime.today().date())
        if last_date >= today:
            # 미국장은 KST 기준 늦게 마감되므로, US 기준 오늘 데이터가 들어오면 이상
            import pytz
            now_est = pd.Timestamp.now(tz='America/New_York').tz_localize(None)
            if now_est.replace(hour=16, minute=30, second=0) > now_est:
                # 장 마감 전인데 오늘 날짜 존재 → intraday 오염
                if last_date.date() >= now_est.date():
                    issues.append(
                        f"⚠️ 엔진 last_date={last_date.date()} 가 오늘 이후 "
                        f"(미국장 마감 전) — intraday 데이터 오염 의심"
                    )
    except Exception:
        pass

    # 2. 자본 조정 미반영 체크 — 가장 중요!
    #    adj_history 있는데 1회시드가 (os_capital / divisions) 와 거의 동일하면 의심
    adj_history = acct_data.get("capital_adj_history", []) or []
    if isinstance(adj_history, list) and adj_history:
        os_cap = float(acct_data.get("os_capital", 0))
        divisions = os_result.get("cur_divisions", 0)
        if os_cap > 0 and divisions > 0:
            expected_no_adj = os_cap / divisions
            actual_seed = float(os_result.get("seed_per_trade", 0))
            if abs(actual_seed - expected_no_adj) < 1.0:
                issues.append(
                    f"⚠️ 자본조정 이력 {len(adj_history)}건 있으나 "
                    f"1회시드 ${actual_seed:,.0f}가 조정 미반영 값과 동일 "
                    f"(${expected_no_adj:,.0f}) — 조정 누락 의심"
                )

    # 3. 매수주문가/매도목표가 sanity
    prev_close = os_result.get("prev_close", 0)
    issues.extend(_check_price_sanity(
        os_result.get("next_buy_order", 0), prev_close, "매수주문가"))
    for i, pos in enumerate(os_result.get("open_positions", []) or []):
        if pos.get("sell_target"):
            issues.extend(_check_price_sanity(
                pos["sell_target"], pos.get("buy_price", prev_close),
                f"티어{i+1} 매도목표"))

    # 4. 수량 sanity
    if os_result.get("n_pos", 0) < os_result.get("cur_divisions", 0):
        issues.extend(_check_qty_sanity(os_result.get("buy_qty_est", 0), "매수수량"))

    # 5. 손절예정일 < 오늘 (workday_offset 버그 재발)
    today = pd.Timestamp(datetime.today().date())
    for i, pos in enumerate(os_result.get("open_positions", []) or []):
        stop = pos.get("stop_date")
        if stop is not None:
            try:
                stop_ts = pd.Timestamp(stop)
                buy_ts = pd.Timestamp(pos.get("buy_date"))
                # 매수일로부터 stop_date가 최소 7일 뒤여야 정상 (AG max_hold=8)
                if (stop_ts - buy_ts).days < 5 and stop_ts.date() != today.date():
                    # 손절일이 매수일과 너무 가까우면 의심 (단, 오늘이 실제 손절일인 경우 제외)
                    issues.append(
                        f"⚠️ 티어{i+1} 손절예정일 {stop_ts.date()} 가 "
                        f"매수일 {buy_ts.date()} 대비 너무 가까움 — "
                        f"workday_offset 버그 재발 가능성"
                    )
            except Exception:
                pass

    return issues


def sanity_check_iuo(o: dict, acct_name: str, acct_data: dict) -> list:
    """IUO 매매법 이상치 감지."""
    if not o:
        return []
    issues = []

    # 1. cycle_day 비정상
    cycle_day = o.get("cycle_day", 0)
    moc_days = o.get("moc_days", 18)
    try:
        cd = int(cycle_day)
        md = int(moc_days)
        if cd < 0:
            issues.append(f"⚠️ 사이클 진행일 음수({cd})")
        elif cd > md + 3:
            issues.append(f"⚠️ 사이클 진행일 {cd} > MOC기한 {md} 초과 — 시간청산 실패 가능성")
    except Exception:
        pass

    # 2. 자본 조정 미반영 체크 (DSS와 동일 패턴)
    adj_raw = acct_data.get("capital_adj_history", "[]")
    try:
        adj_list = json.loads(adj_raw) if isinstance(adj_raw, str) else adj_raw
        if not isinstance(adj_list, list):
            adj_list = []
    except Exception:
        adj_list = []
    # IUO는 cash/total_asset에 조정 반영 체크 — 이미 합산됨이 정답

    # 3. 가격 sanity
    lc = o.get("last_close", 0)
    for k, label in [("buy0_loc", "첫매수LOC"), ("buy1_loc", "추가매수1LOC"),
                     ("buy2_loc", "추가매수2LOC")]:
        v = o.get(k, 0)
        if v:
            issues.extend(_check_price_sanity(v, lc, label))
    if o.get("sell_loc"):
        issues.extend(_check_price_sanity(
            o["sell_loc"], o.get("last_buy_close", lc), "매도LOC"))

    return issues


def build_admin_alert(username: str, warnings: list) -> str:
    """관리자(유저 본인) 알림 메시지 생성."""
    today = datetime.today().strftime("%Y-%m-%d")
    lines = [
        f"<b>🚨 주문표 이상치 감지 — {username}</b>",
        f"📅 {today}",
        f"",
    ]
    for w in warnings:
        lines.append(w)
    lines.append("")
    lines.append("※ 정상이라고 판단되면 무시하세요.")
    lines.append("※ 반복되면 앱 설정 / 자본 조정 이력 / 데이터 로딩을 점검해주세요.")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════
# 공통: 텔레그램 발송
# ══════════════════════════════════════════════════════════════

def send_telegram(chat_id: str, token: str, text: str,
                  parse_mode: str = "Markdown"):
    url  = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, json={
        "chat_id": chat_id, "text": text, "parse_mode": parse_mode,
    }, timeout=10)
    return resp.ok, resp.text


# ══════════════════════════════════════════════════════════════
# 공통: 구글시트에 주문표 기록 (덮어쓰기 방식)
# L4:O13 영역을 clear하고 새 주문표를 L4부터 작성
# 자동매매 프로그램이 이 시트를 읽어 주문 실행
# ══════════════════════════════════════════════════════════════

def write_orders_to_gsheet(client, gs_url: str, gs_sheet: str,
                            rows: list, label: str = "") -> bool:
    """주문표 rows를 구글시트 지정 탭의 L4부터 기록 (완전 덮어쓰기).

    동작 순서:
      1. L4:O30 범위를 batch_clear로 모두 비움 (27행 방어 마진)
      2. L4부터 새 rows만 작성 (나머지는 빈 상태 유지)
    → 이전 발송 기록은 완전히 사라지고, 오늘 발송분만 남음

    Parameters
    ----------
    client    : gspread 클라이언트
    gs_url    : 스프레드시트 URL
    gs_sheet  : 탭(워크시트) 이름
    rows      : 2D 리스트 [[셀1, 셀2, 셀3, 셀4], ...]
    label     : 로그용 라벨 (예: "종가평균/SOXL")

    Returns
    -------
    bool : 성공 여부 (실패해도 텔레그램 발송에 영향 없음 — 로그만 출력)
    """
    if not gs_url or not gs_sheet or not rows:
        return False
    try:
        sh = client.open_by_url(gs_url)
        try:
            ws = sh.worksheet(gs_sheet)
        except gspread.exceptions.WorksheetNotFound:
            print(f"      ⚠️ [{label}] 시트 탭 '{gs_sheet}' 없음 — 건너뜀")
            return False
        # ① 먼저 넓은 범위를 clear → 이전 기록 완전 제거 (최대 전략 8행 대비 27행 여유)
        ws.batch_clear(["L4:O30"])
        # ② 그 다음 L4부터 새 rows만 작성 → 오늘 발송분만 표시됨
        ws.update(range_name="L4", values=rows)
        # ③ B11 업데이트 시각 (KST) — 주문표가 언제 갱신됐는지 확인용
        ws.update(range_name="B11",
                  values=[[pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")]])
        print(f"      📊 [{label}] GSheet '{gs_sheet}' L4에 {len(rows)}건 기록 (이전 기록 clear 완료)")
        return True
    except Exception as e:
        print(f"      ⚠️ [{label}] GSheet 기록 실패: {e}")
        return False


def build_avg_order_rows(res: dict) -> list:
    """종가평균매매 주문 rows 생성 (웹앱 _write_orders_to_sheet와 동일 포맷)."""
    rows = []
    if res.get("buy_qty", 0) > 0:
        rows.append(["매수", "LOC", round(res["tb"], 2), int(res["buy_qty"])])
    if res.get("shares", 0) > 0 and res.get("sell_qty", 0) > 0:
        rows.append(["매도", "LOC", round(res["ts"], 2), int(res["sell_qty"])])
    return rows


def build_sd_order_rows(r: dict) -> list:
    """표준편차매매 주문 rows 생성 (웹앱 stdev.py line 2862-2866과 동일 포맷)."""
    rows = [["매수", "LOC", round(r["next_buy_loc"], 2), int(r["est_buy_qty"])]]
    if r["holdings"] > 0:
        rows.append(["매도", "LOC", round(r["next_sell_loc"], 2), int(r["est_sell_qty"])])
    return rows


def build_sigma_order_rows(od: dict, qty: int) -> list:
    """Sigma매매법 주문 rows 생성 (웹앱 sigma.py line 1856-1861과 동일 포맷)."""
    return [
        ["1σ 매수 LOC", round(od["buy_loc_1"], 4), int(qty),
         f"μ:{od['mu_pct']:+.4f}% σ:{od['sigma_pct']:.4f}%"],
        ["2σ 참고", round(od["buy_loc_2"], 4), "", ""],
        ["3σ 참고", round(od["buy_loc_3"], 4), "", ""],
    ]


def build_dss_order_rows(os_result: dict) -> list:
    """DSS 동파법 주문 rows 생성 — dss_engine.build_order_rows 재사용 (SSOT).

    웹앱의 _write_dss_orders_to_sheet 와 완전히 동일한 함수를 사용하므로
    포맷 불일치로 인한 자동매매 실패 방지.
    """
    from dss_engine import build_order_rows
    return build_order_rows(os_result)


def build_no_order_row(strategy_label: str) -> list:
    """오늘 주문 0건일 때 작성하는 sentinel 행.
    자동매매 프로그램은 L4의 "NO_ORDER" 키워드로 "오늘 주문 없음"을 판단.
    """
    today = datetime.today().strftime("%Y-%m-%d")
    return [["NO_ORDER", today, strategy_label, "오늘 주문 없음"]]


def build_error_row(strategy_label: str, reason: str) -> list:
    """계산 실패(데이터 로드 실패 등) 시 작성하는 sentinel 행.
    자동매매 프로그램은 L4의 "ERROR" 키워드로 "주문 실행 보류"를 판단.
    """
    today = datetime.today().strftime("%Y-%m-%d")
    reason_short = (reason or "계산 실패")[:60]
    return [["ERROR", today, strategy_label, reason_short]]


def write_gsheet_with_status(client, gs_url: str, gs_sheet: str,
                              normal_rows: list, strategy_label: str,
                              status: str = "OK", error_reason: str = "") -> bool:
    """주문표 또는 상태 sentinel 행 작성 (구글시트 L4부터 덮어쓰기).

    status:
      - "OK"       : normal_rows가 있으면 그대로 기록, 없으면 NO_ORDER sentinel
      - "NO_ORDER" : 주문 0건 명시 → NO_ORDER sentinel 행 작성
      - "ERROR"    : 계산 실패 → ERROR sentinel 행 작성 (error_reason 포함)
    """
    if not gs_url or not gs_sheet:
        return False
    if status == "ERROR":
        rows = build_error_row(strategy_label, error_reason)
    elif status == "NO_ORDER" or not normal_rows:
        rows = build_no_order_row(strategy_label)
    else:
        rows = normal_rows
    return write_orders_to_gsheet(client, gs_url, gs_sheet, rows, label=strategy_label)


def build_iuo_order_rows(o: dict) -> list:
    """IUO 매매법 주문 rows 생성 (신규 — 첫매수 / 추가매수 / 매도 LOC 4열 포맷)."""
    rows = []
    if o["cur_shares"] == 0:
        # 첫매수
        first_amt = o["cycle_base"] * o["fbr"]
        qty0 = math.floor(first_amt / o["buy0_loc"]) if o["buy0_loc"] > 0 else 0
        rows.append(["첫매수 LOC", round(o["buy0_loc"], 2), int(qty0),
                     f"기준액 ${o['cycle_base']:,.0f}×{o['fbr']*100:.0f}%"])
    else:
        # 매도 LOC
        if o.get("sell_loc"):
            pnl = (o["sell_loc"] / o["avg_cost"] - 1) * 100 if o["avg_cost"] > 0 else 0
            rows.append(["매도 LOC", round(o["sell_loc"], 2), int(o["cur_shares"]),
                         f"{pnl:+.1f}% / 진행{o['cycle_day']}일"])
        # 추가매수 LOC
        if o["remaining"] > 0:
            buy_amt = (o["cycle_base"] - o["cash"]) / o["div"] if o["div"] > 0 else 0
            if buy_amt <= 0:
                buy_amt = o["cycle_base"] / o["div"] if o["div"] > 0 else 0
            qty1 = math.floor(buy_amt / o["buy1_loc"]) if o["buy1_loc"] > 0 else 0
            rows.append(["추가매수1 LOC", round(o["buy1_loc"], 2), int(qty1),
                         f"{o['b1']*100:+.1f}%"])
            if o["remaining"] >= 2:
                qty2 = math.floor(buy_amt / o["buy2_loc"]) if o["buy2_loc"] > 0 else 0
                rows.append(["추가매수2 LOC", round(o["buy2_loc"], 2), int(qty2),
                             f"{o['b2']*100:+.1f}%"])
    return rows

# ══════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════

def main():
    sheet_url = os.environ.get("ADMIN_SHEET_URL", "")
    if not sheet_url:
        print("❌ ADMIN_SHEET_URL 환경변수가 없습니다.")
        return

    # ── 미국 휴장일 체크: NYSE 휴장이면 발송 스킵 ──
    # (Memorial Day, Independence Day, Thanksgiving, Christmas 등)
    # FORCE_RUN=1 환경변수 설정 시 휴장일에도 발송 (테스트용)
    try:
        from dss_engine import _is_us_trading_day
        from datetime import datetime as _dt
        # GitHub Actions runner는 UTC. trigger time 03:00 UTC = KST 12:00.
        # 이 시점 UTC 날짜 = 오늘의 KST 날짜 = 오늘의 US 시장 개장일
        today_date = _dt.utcnow().date()
        if not _is_us_trading_day(today_date) and not os.environ.get("FORCE_RUN", "").strip():
            print(f"🏖️ {today_date} 는 NYSE 휴장일 — 발송 스킵 (FORCE_RUN=1 로 강제 실행 가능)")
            return
    except Exception as _e:
        # 휴장일 체크 실패해도 발송은 진행 (보수적)
        print(f"⚠️ 휴장일 체크 실패 (계속 진행): {_e}")

    print("👥 사용자 목록 로드 중...")
    client = get_gspread_client()

    filter_user = os.environ.get("FILTER_USER", "").strip()
    force_run = os.environ.get("FORCE_RUN", "").strip()

    # ── 중복 발송 방지 ──
    # 외부 cron(정시)과 GitHub 백업 cron(지연)이 같은 날 둘 다 실행될 때
    # 두 번째 실행은 스킵. 단 테스트(FILTER_USER)/강제(FORCE_RUN)는 통과.
    if not filter_user and not force_run:
        if already_sent_today(client, sheet_url):
            print("✅ 오늘 이미 발송 완료 — 중복 실행 스킵 "
                  "(FORCE_RUN=1 로 강제 재발송 가능)")
            return

    users  = get_users(client, sheet_url)
    print(f"✅ {len(users)}명 로드")

    # 특정 유저만 필터링 (테스트 모드)
    if filter_user:
        _before = len(users)
        users = [u for u in users if str(u.get("username", "")).strip() == filter_user]
        print(f"🎯 필터 적용: '{filter_user}' → {len(users)}/{_before}명 선택")
        if not users:
            print(f"❌ username='{filter_user}' 사용자를 찾을 수 없습니다.")
            return

    ok_count = skip_count = fail_count = 0

    for user in users:
        username = user.get("username", "")
        # 종가평균/표준편차/Sigma 공용 gs_url (사용자 레벨)
        shared_gs_url = str(user.get("gs_url", "")).strip()
        # 이상치 알림 수집 버킷 (유저별)
        user_warnings = []

        # ── 1. 종가평균매매 발송 ──────────────────────────────
        avg_chat_id = str(user.get("tg_chat_id", "")).strip()
        avg_token   = str(user.get("tg_token",   "")).strip()
        avg_settings = parse_ticker_settings(user)

        if avg_chat_id and avg_token and avg_settings:
            print(f"  👤 {username} [종가평균]: {list(avg_settings.keys())} 처리 중...")
            for tk, cfg in avg_settings.items():
                a_buy      = float(cfg.get("a_buy",      DEFAULT_A_BUY))
                a_sell     = float(cfg.get("a_sell",     DEFAULT_A_SELL))
                sell_ratio = float(cfg.get("sell_ratio", DEFAULT_SELL_RATIO))
                divisions  = int(float(cfg.get("divisions", DEFAULT_DIVISIONS)))
                n_days     = int(float(cfg.get("n_days",   2)))   # 사용자 N일 평균 설정
                capital    = float(cfg.get("os_capital", DEFAULT_CAPITAL))
                os_start   = str(cfg.get("os_start", DEFAULT_OS_START)).strip() or DEFAULT_OS_START
                _gs_sheet  = str(cfg.get("gs_sheet", tk)).strip() or tk
                _label     = f"종가평균/{tk}"

                try:
                    res = calc_today_order(
                        ticker=tk, os_start=os_start,
                        a_buy=a_buy, a_sell=a_sell,
                        sell_ratio=sell_ratio, divisions=divisions,
                        capital=capital, n_days=n_days,
                    )
                except Exception as e:
                    print(f"    ❌ [{tk}] 계산 오류 → {e}")
                    fail_count += 1
                    user_warnings.append(f"[종가평균/{tk}] ❌ 주문표 미발송: {e}")
                    if shared_gs_url:
                        write_gsheet_with_status(client, shared_gs_url, _gs_sheet, None,
                                                  _label, status="ERROR", error_reason=f"계산 오류: {e}")
                    continue

                if not res:
                    print(f"    ❌ [{tk}] 데이터 부족")
                    fail_count += 1
                    if shared_gs_url:
                        write_gsheet_with_status(client, shared_gs_url, _gs_sheet, None,
                                                  _label, status="ERROR", error_reason="데이터 부족")
                    continue

                # 이상치 감지
                _issues = sanity_check_avg(res, tk)
                user_warnings.extend([f"[종가평균/{tk}] {m}" for m in _issues])

                msg = build_avg_message(res, tk)
                ok, resp = send_telegram(avg_chat_id, avg_token, msg, parse_mode="Markdown")
                if ok:
                    print(f"    ✅ [종가평균/{tk}] 발송 성공")
                    ok_count += 1
                else:
                    print(f"    ❌ [종가평균/{tk}] 발송 실패 → {resp}")
                    fail_count += 1

                # ── 구글시트 주문표 기록 (텔레그램 결과와 독립) ──
                # 주문 0건이면 NO_ORDER sentinel 자동 작성
                if shared_gs_url:
                    _rows = build_avg_order_rows(res)
                    write_gsheet_with_status(client, shared_gs_url, _gs_sheet, _rows,
                                              _label, status="OK")
        else:
            print(f"  ⏭️  {username} [종가평균]: 미설정 → 건너뜀")
            skip_count += 1

        # ── 2. 표준편차매매 발송 ──────────────────────────────
        sd_chat_id  = str(user.get("sd_tg_chat_id", "")).strip()
        sd_token    = str(user.get("sd_tg_token",   "")).strip()
        sd_settings = parse_sd_ticker_settings(user)

        if sd_chat_id and sd_token and sd_settings:
            print(f"  👤 {username} [표준편차]: {list(sd_settings.keys())} 처리 중...")
            for tk, cfg in sd_settings.items():
                k_buy        = float(cfg.get("k_buy",        SD_DEFAULT_K_BUY))
                k_sell       = float(cfg.get("k_sell",       SD_DEFAULT_K_SELL))
                sigma_period = int(float(cfg.get("sigma_period", SD_DEFAULT_SIGMA_PERIOD)))
                sell_ratio   = float(cfg.get("sell_ratio",   SD_DEFAULT_SELL_RATIO))
                divisions    = int(float(cfg.get("divisions",    SD_DEFAULT_DIVISIONS)))
                renewal      = int(float(cfg.get("renewal",      SD_DEFAULT_RENEWAL)))
                pcr          = float(cfg.get("pcr",          1.0))
                lcr          = float(cfg.get("lcr",          1.0))
                capital      = float(cfg.get("os_capital",   SD_DEFAULT_CAPITAL))
                os_start     = str(cfg.get("os_start", DEFAULT_OS_START)).strip() or DEFAULT_OS_START
                _gs_sheet    = str(cfg.get("gs_sheet", tk)).strip() or tk
                _label       = f"표준편차/{tk}"

                try:
                    r = calc_sd_today_order(
                        ticker=tk, os_start=os_start,
                        k_buy=k_buy, k_sell=k_sell,
                        sigma_period=sigma_period,
                        sell_ratio=sell_ratio,
                        divisions=divisions, renewal=renewal,
                        capital=capital, pcr=pcr, lcr=lcr,
                    )
                except Exception as e:
                    print(f"    ❌ [표준편차/{tk}] 계산 오류 → {e}")
                    fail_count += 1
                    user_warnings.append(f"[표준편차/{tk}] ❌ 주문표 미발송: {e}")
                    if shared_gs_url:
                        write_gsheet_with_status(client, shared_gs_url, _gs_sheet, None,
                                                  _label, status="ERROR", error_reason=f"계산 오류: {e}")
                    continue

                if not r:
                    print(f"    ❌ [표준편차/{tk}] 데이터 부족")
                    fail_count += 1
                    if shared_gs_url:
                        write_gsheet_with_status(client, shared_gs_url, _gs_sheet, None,
                                                  _label, status="ERROR", error_reason="데이터 부족")
                    continue

                # 이상치 감지
                _issues = sanity_check_sd(r, tk)
                user_warnings.extend([f"[표준편차/{tk}] {m}" for m in _issues])

                msg = build_sd_message(r)
                ok, resp = send_telegram(sd_chat_id, sd_token, msg, parse_mode="HTML")
                if ok:
                    print(f"    ✅ [표준편차/{tk}] 발송 성공")
                    ok_count += 1
                else:
                    print(f"    ❌ [표준편차/{tk}] 발송 실패 → {resp}")
                    fail_count += 1

                # ── 구글시트 주문표 기록 (텔레그램 결과와 독립) ──
                # 주문 0건이면 NO_ORDER sentinel 자동 작성
                if shared_gs_url:
                    _rows = build_sd_order_rows(r)
                    write_gsheet_with_status(client, shared_gs_url, _gs_sheet, _rows,
                                              _label, status="OK")
        else:
            print(f"  ⏭️  {username} [표준편차]: 미설정 → 건너뜀")
            skip_count += 1

        # ── 3. DSS 동파법 발송 ──────────────────────────────────
        dss_cfg = parse_dss_config(user)
        dss_chat_id = str(dss_cfg.get("tg_chat_id", "")).strip()
        dss_token   = str(dss_cfg.get("tg_token",   "")).strip()
        dss_accounts = dss_cfg.get("accounts", {})
        dss_gs_url  = str(dss_cfg.get("gs_url", "")).strip()
        # 계좌별 워크시트 매핑 (dss_config["gs_sheets"]) · 없으면 레거시 gs_sheet 필드 fallback
        dss_gs_sheets = dss_cfg.get("gs_sheets", {}) or {}
        if not isinstance(dss_gs_sheets, dict):
            dss_gs_sheets = {}
        dss_gs_sheet_legacy = str(dss_cfg.get("gs_sheet", "")).strip()
        # 검증용 알고리C 시트 (verify_url + 계좌별 verify_sheets)
        dss_verify = parse_dss_verify_sheets(dss_cfg)

        if dss_chat_id and dss_token and dss_accounts:
            print(f"  👤 {username} [DSS]: {list(dss_accounts.keys())} 처리 중...")
            for acct_name, acct_data in dss_accounts.items():
                # 계좌별 gs_sheets 매핑 우선 → 레거시 acct_data.gs_sheet → 레거시 dss_config.gs_sheet
                _gs_sheet_dss = (
                    str(dss_gs_sheets.get(acct_name, "")).strip()
                    or str(acct_data.get("gs_sheet", "")).strip()
                    or dss_gs_sheet_legacy
                )
                _label = f"DSS/{acct_name}"

                try:
                    os_result = calc_dss_order(acct_data)
                except Exception as e:
                    print(f"    ❌ [DSS/{acct_name}] 계산 오류 → {e}")
                    fail_count += 1
                    user_warnings.append(f"[DSS/{acct_name}] ❌ 주문표 미발송: {e}")
                    if dss_gs_url and _gs_sheet_dss:
                        write_gsheet_with_status(client, dss_gs_url, _gs_sheet_dss, None,
                                                  _label, status="ERROR", error_reason=f"계산 오류: {e}")
                    continue

                if not os_result:
                    print(f"    ❌ [DSS/{acct_name}] 데이터 부족")
                    fail_count += 1
                    if dss_gs_url and _gs_sheet_dss:
                        write_gsheet_with_status(client, dss_gs_url, _gs_sheet_dss, None,
                                                  _label, status="ERROR", error_reason="데이터 부족")
                    continue

                # 이상치 감지 (DSS는 중점 체크)
                _issues = sanity_check_dss(os_result, acct_name, acct_data)
                user_warnings.extend([f"[DSS/{acct_name}] {m}" for m in _issues])

                # yfinance 데이터 이상 → 백업 보충 알림 (유저당 1회만)
                _dw = os_result.get("data_warning")
                if _dw:
                    _dw_msg = f"[DSS] {_dw} (주문표는 백업 데이터 반영)"
                    if _dw_msg not in user_warnings:
                        user_warnings.append(_dw_msg)

                # 알고리C 원본 시트(BOARD)와 대조 검증 (계좌별 URL)
                _vinfo = dss_verify.get(acct_name)
                if _vinfo:
                    _vissues = verify_dss_against_board(
                        client, os_result, _vinfo["url"], _vinfo["sheet"], acct_name)
                    if _vissues:
                        user_warnings.extend(
                            [f"[DSS/{acct_name}📋시트대조] {m}" for m in _vissues])
                        print(f"    ⚠️ [DSS/{acct_name}] 시트 불일치 {len(_vissues)}건")

                msg = build_dss_message(os_result, acct_name)
                ok, resp = send_telegram(dss_chat_id, dss_token, msg, parse_mode="HTML")
                if ok:
                    print(f"    ✅ [DSS/{acct_name}] 발송 성공")
                    ok_count += 1
                else:
                    print(f"    ❌ [DSS/{acct_name}] 발송 실패 → {resp}")
                    fail_count += 1

                # ── 구글시트 주문표 기록 (텔레그램 결과와 독립) ──
                # 주문 0건(전 슬롯 보유 + 매도목표 없음)이면 NO_ORDER sentinel 자동 작성
                if dss_gs_url and _gs_sheet_dss:
                    _rows = build_dss_order_rows(os_result)
                    write_gsheet_with_status(client, dss_gs_url, _gs_sheet_dss, _rows,
                                              _label, status="OK")
        else:
            print(f"  ⏭️  {username} [DSS]: 미설정 → 건너뜀")
            skip_count += 1

        # ── 4. Sigma매매법 발송 ──────────────────────────────────
        sg_chat_id  = str(user.get("sigma_tg_chat_id", "")).strip()
        sg_token    = str(user.get("sigma_tg_token",   "")).strip()
        sg_settings = parse_sigma_ticker_settings(user)

        if sg_chat_id and sg_token and sg_settings:
            print(f"  👤 {username} [Sigma]: {list(sg_settings.keys())} 처리 중...")
            for tk, cfg in sg_settings.items():
                sigma_period = int(float(cfg.get("sigma_period", SIGMA_DEFAULT_PERIOD)))
                capital      = float(cfg.get("os_capital",   SIGMA_DEFAULT_CAPITAL))
                divisions    = int(float(cfg.get("divisions",    SIGMA_DEFAULT_DIVISIONS)))
                _gs_sheet_sg = str(cfg.get("gs_sheet", f"sigma_{tk}")).strip() or f"sigma_{tk}"
                _label       = f"Sigma/{tk}"

                try:
                    od = calc_sigma_order(tk, sigma_period)
                except Exception as e:
                    print(f"    ❌ [Sigma/{tk}] 계산 오류 → {e}")
                    fail_count += 1
                    user_warnings.append(f"[Sigma/{tk}] ❌ 주문표 미발송: {e}")
                    if shared_gs_url:
                        write_gsheet_with_status(client, shared_gs_url, _gs_sheet_sg, None,
                                                  _label, status="ERROR", error_reason=f"계산 오류: {e}")
                    continue

                if not od:
                    print(f"    ❌ [Sigma/{tk}] 데이터 부족")
                    fail_count += 1
                    if shared_gs_url:
                        write_gsheet_with_status(client, shared_gs_url, _gs_sheet_sg, None,
                                                  _label, status="ERROR", error_reason="데이터 부족")
                    continue

                # 이상치 감지
                _issues = sanity_check_sigma(od)
                user_warnings.extend([f"[Sigma/{tk}] {m}" for m in _issues])

                msg = build_sigma_message(od, capital, divisions)
                ok, resp = send_telegram(sg_chat_id, sg_token, msg, parse_mode="HTML")
                if ok:
                    print(f"    ✅ [Sigma/{tk}] 발송 성공")
                    ok_count += 1
                else:
                    print(f"    ❌ [Sigma/{tk}] 발송 실패 → {resp}")
                    fail_count += 1

                # ── 구글시트 주문표 기록 (텔레그램 결과와 독립) ──
                # Sigma는 1σ/2σ/3σ 참고 포함 항상 3행 → NO_ORDER 거의 없음
                if shared_gs_url:
                    amount_per_trade = capital / max(divisions, 1)
                    qty1 = math.floor(amount_per_trade / od["buy_loc_1"]) if od["buy_loc_1"] > 0 else 0
                    _rows = build_sigma_order_rows(od, qty1)
                    write_gsheet_with_status(client, shared_gs_url, _gs_sheet_sg, _rows,
                                              _label, status="OK")
        else:
            print(f"  ⏭️  {username} [Sigma]: 미설정 → 건너뜀")
            skip_count += 1

        # ── 5. IUO 매매법 발송 ──────────────────────────────────
        iuo_cfg = parse_iuo_config(user)
        iuo_chat_id  = str(iuo_cfg.get("tg_chat_id", "")).strip()
        iuo_token    = str(iuo_cfg.get("tg_token",   "")).strip()
        iuo_accounts = iuo_cfg.get("accounts", {})
        iuo_gs_url   = str(iuo_cfg.get("gs_url", "")).strip()

        if iuo_chat_id and iuo_token and iuo_accounts:
            print(f"  👤 {username} [IUO]: {list(iuo_accounts.keys())} 처리 중...")
            for acct_name, acct_data in iuo_accounts.items():
                # IUO 시트명: iuo_config["gs_sheet_{ticker}"] 플랫 키 우선 → acct 내 → 기본
                _iuo_tk_hint = str(acct_data.get("ticker", "")).strip()
                _gs_sheet_iuo = str(
                    iuo_cfg.get(f"gs_sheet_{_iuo_tk_hint}", "")
                    or acct_data.get("gs_sheet", "")
                    or f"iuo_{acct_name}"
                ).strip()
                _label = f"IUO/{acct_name}"

                try:
                    iuo_result = calc_iuo_order(acct_data)
                except Exception as e:
                    print(f"    ❌ [IUO/{acct_name}] 계산 오류 → {e}")
                    fail_count += 1
                    user_warnings.append(f"[IUO/{acct_name}] ❌ 주문표 미발송: {e}")
                    if iuo_gs_url and _gs_sheet_iuo:
                        write_gsheet_with_status(client, iuo_gs_url, _gs_sheet_iuo, None,
                                                  _label, status="ERROR", error_reason=f"계산 오류: {e}")
                    continue

                if not iuo_result:
                    print(f"    ❌ [IUO/{acct_name}] 데이터 부족")
                    fail_count += 1
                    if iuo_gs_url and _gs_sheet_iuo:
                        write_gsheet_with_status(client, iuo_gs_url, _gs_sheet_iuo, None,
                                                  _label, status="ERROR", error_reason="데이터 부족")
                    continue

                # 이상치 감지
                _issues = sanity_check_iuo(iuo_result, acct_name, acct_data)
                user_warnings.extend([f"[IUO/{acct_name}] {m}" for m in _issues])

                msg = build_iuo_message(iuo_result, acct_name)
                ok, resp = send_telegram(iuo_chat_id, iuo_token, msg, parse_mode="HTML")
                if ok:
                    print(f"    ✅ [IUO/{acct_name}] 발송 성공")
                    ok_count += 1
                else:
                    print(f"    ❌ [IUO/{acct_name}] 발송 실패 → {resp}")
                    fail_count += 1

                # ── 구글시트 주문표 기록 (텔레그램 결과와 독립) ──
                # iuo_result가 확정되면 ticker 기반 시트명 재계산 (acct_data보다 우선)
                _iuo_tk = iuo_result.get("ticker", _iuo_tk_hint)
                _gs_sheet_iuo_final = str(
                    iuo_cfg.get(f"gs_sheet_{_iuo_tk}", "")
                    or _gs_sheet_iuo
                ).strip()
                # 주문 0건이면 NO_ORDER sentinel 자동 작성
                if iuo_gs_url and _gs_sheet_iuo_final:
                    _rows = build_iuo_order_rows(iuo_result)
                    write_gsheet_with_status(client, iuo_gs_url, _gs_sheet_iuo_final, _rows,
                                              _label, status="OK")
        else:
            print(f"  ⏭️  {username} [IUO]: 미설정 → 건너뜀")
            skip_count += 1

        # ── 6. 이상치 알림 (유저 본인에게 발송) ─────────────────
        if user_warnings:
            # 텔레그램 채널: 등록된 전략 중 하나 선택
            # 우선순위: 종가평균 → 표준편차 → DSS → Sigma → IUO
            _candidates = [
                (avg_chat_id, avg_token),
                (sd_chat_id, sd_token),
                (dss_chat_id, dss_token),
                (sg_chat_id, sg_token),
                (iuo_chat_id, iuo_token),
            ]
            _alert_chat_id = ""
            _alert_token = ""
            for _cid, _tok in _candidates:
                if _cid and _tok:
                    _alert_chat_id = _cid
                    _alert_token = _tok
                    break
            if _alert_chat_id and _alert_token:
                _alert_msg = build_admin_alert(username, user_warnings)
                ok, resp = send_telegram(_alert_chat_id, _alert_token,
                                         _alert_msg, parse_mode="HTML")
                if ok:
                    print(f"  🚨 {username} 이상치 {len(user_warnings)}건 알림 발송 성공")
                else:
                    print(f"  ❌ {username} 이상치 알림 발송 실패 → {resp}")
            else:
                print(f"  ⚠️ {username} 이상치 {len(user_warnings)}건이나 텔레그램 채널 없음:")
                for w in user_warnings:
                    print(f"     - {w}")

    print(f"\n🏁 완료: 성공 {ok_count}건 / 건너뜀 {skip_count}명 / 실패 {fail_count}건")

    # 발송 완료 기록 (중복 실행 방지용). 테스트/강제 모드는 기록 안 함.
    if not filter_user and not force_run and ok_count > 0:
        mark_sent_today(client, sheet_url)
        print("📝 오늘 발송 완료 기록됨 (_send_log)")

if __name__ == "__main__":
    main()
