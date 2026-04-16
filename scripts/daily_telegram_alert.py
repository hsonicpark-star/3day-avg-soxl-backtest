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

# ── 가격 데이터 ────────────────────────────────────────────────
def fetch_prices(ticker: str, start_date: str) -> pd.DataFrame:
    end = datetime.today() + timedelta(days=1)
    df = yf.download(ticker, start=start_date,
                     end=end.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Close"]].dropna()
    df["Close"] = df["Close"].astype(float)
    return df

# ══════════════════════════════════════════════════════════════
# 종가평균매매 관련
# ══════════════════════════════════════════════════════════════

def buy_limit_price(p1: float, p2: float, a: float) -> float:
    return (p1 + p2) * (1 + a) / (2 - a)

def calc_today_order(df: pd.DataFrame,
                     a_buy: float, a_sell: float,
                     sell_ratio: float, divisions: int,
                     capital: float) -> dict:
    closes = df["Close"].values
    if len(closes) < 2:
        return {}

    shares, cash, avg_cost = 0, capital, 0.0
    open_tiers = []
    prev_asset = capital

    for i in range(2, len(closes)):
        x   = float(closes[i])
        p1  = float(closes[i - 1])
        p2  = float(closes[i - 2])
        tb  = buy_limit_price(p1, p2, a_buy)
        ts  = buy_limit_price(p1, p2, a_sell)
        chunk = prev_asset / divisions

        if shares > 0 and x >= ts:
            sell_qty = math.floor(shares * (sell_ratio / 100.0))
            if sell_qty > 0:
                cash  += sell_qty * x
                shares -= sell_qty
                remaining = sell_qty
                while remaining > 0 and open_tiers:
                    if open_tiers[0]["qty"] <= remaining:
                        remaining -= open_tiers[0]["qty"]
                        open_tiers.pop(0)
                    else:
                        open_tiers[0]["qty"] -= remaining
                        remaining = 0
                if shares > 0 and open_tiers:
                    total_inv = sum(t["price"] * t["qty"] for t in open_tiers)
                    total_qty = sum(t["qty"] for t in open_tiers)
                    avg_cost  = total_inv / total_qty if total_qty > 0 else 0.0
                else:
                    avg_cost, open_tiers = 0.0, []
        elif x <= tb:
            buy_qty = min(
                math.floor(chunk / tb + 1e-9),
                math.floor(cash / tb + 1e-9),
            )
            if buy_qty > 0:
                total_inv = avg_cost * shares + x * buy_qty
                shares   += buy_qty
                avg_cost  = total_inv / shares
                cash     -= buy_qty * x
                open_tiers.append({"price": x, "qty": buy_qty})

        prev_asset = cash + shares * x

    p1_now = float(closes[-1])
    p2_now = float(closes[-2])
    tb_next = buy_limit_price(p1_now, p2_now, a_buy)
    ts_next = buy_limit_price(p1_now, p2_now, a_sell)
    current_asset = cash + shares * p1_now
    chunk_now = current_asset / divisions

    buy_qty_next  = min(
        math.floor(chunk_now / tb_next + 1e-9),
        math.floor(cash / tb_next + 1e-9),
    ) if cash > 0 else 0
    sell_qty_next = math.floor(shares * (sell_ratio / 100.0)) if shares > 0 else 0

    return {
        "p1": p1_now, "p2": p2_now,
        "tb": round(tb_next, 2), "ts": round(ts_next, 2),
        "shares": shares, "buy_qty": buy_qty_next,
        "sell_qty": sell_qty_next, "cash": cash, "avg_cost": avg_cost,
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

def calc_sd_today_order(ticker: str, os_start: str,
                        k_buy: float, k_sell: float,
                        sigma_period: int, sell_ratio: float,
                        divisions: int, renewal: int,
                        capital: float) -> dict | None:
    """표준편차매매 오늘의 주문 계산."""
    buf_start = (pd.to_datetime(os_start) - pd.DateOffset(days=90)).strftime("%Y-%m-%d")
    df = fetch_prices(ticker, buf_start)
    if df is None or df.empty:
        return None

    closes_all = df["Close"].values.astype(float)
    if len(closes_all) < sigma_period + 1:
        return None

    # σ 계산 (최근 sigma_period 수익률)
    rets_last = [(closes_all[j] - closes_all[j-1]) / closes_all[j-1]
                 for j in range(len(closes_all) - sigma_period, len(closes_all))]
    sigma_next = float(np.std(rets_last, ddof=0))
    last_close = float(closes_all[-1])

    next_buy_loc  = round(last_close * (1.0 + sigma_next * k_buy),  2)
    next_sell_loc = round(last_close * (1.0 + sigma_next * k_sell), 2)

    # 포트폴리오 시뮬 (start_date 이후)
    today = datetime.today().date()
    portfolio = df.loc[pd.to_datetime(os_start):pd.to_datetime(today)]

    shares, cash, avg_cost = 0, float(capital), 0.0
    cur_invest = float(capital)

    if not portfolio.empty:
        port_closes = portfolio["Close"].values.astype(float)
        for i in range(1, len(port_closes)):
            x       = port_closes[i]
            x_prev  = port_closes[i - 1]

            # σ 계산: 전체 데이터 기준 (해당 시점까지)
            port_idx = df.index.get_loc(portfolio.index[i])
            if port_idx >= sigma_period:
                w = closes_all[port_idx - sigma_period: port_idx]
                rets = [(w[j] - w[j-1]) / w[j-1] for j in range(1, len(w))]
                sig  = float(np.std(rets, ddof=0)) if rets else sigma_next
            else:
                sig = sigma_next

            b_loc = round(x_prev * (1.0 + sig * k_buy),  2)
            s_loc = round(x_prev * (1.0 + sig * k_sell), 2)

            daily_inv = cur_invest / divisions if divisions > 0 else cur_invest
            avail     = min(daily_inv, cash)

            if shares > 0 and x >= s_loc:
                sell_qty = int(round(shares * sell_ratio / 100.0))
                if sell_qty > 0:
                    cash   += sell_qty * x
                    shares -= sell_qty
                    if shares == 0:
                        avg_cost = 0.0
            elif x <= b_loc:
                buy_qty = math.floor(avail / b_loc + 1e-9) if b_loc > 0 else 0
                if buy_qty > 0 and cash >= buy_qty * x:
                    total_inv = avg_cost * shares + x * buy_qty
                    shares   += buy_qty
                    avg_cost  = total_inv / shares
                    cash     -= buy_qty * x

    daily_inv  = cur_invest / divisions if divisions > 0 else cur_invest
    avail_next = min(daily_inv, cash)
    est_buy_qty  = math.floor(avail_next / next_buy_loc + 1e-9) if next_buy_loc > 0 else 0
    est_sell_qty = int(round(shares * sell_ratio / 100.0)) if shares > 0 else 0

    return {
        "ticker":         ticker,
        "last_close":     last_close,
        "sigma_next":     sigma_next,
        "next_buy_loc":   next_buy_loc,
        "next_sell_loc":  next_sell_loc,
        "holdings":       shares,
        "avg_cost":       avg_cost,
        "cash":           cash,
        "est_buy_qty":    est_buy_qty,
        "est_sell_qty":   est_sell_qty,
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
                                build_mode_series, get_week_mode_map)
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

    # 데이터 로드
    try:
        soxl = fetch_prices("SOXL", "2009-06-01")
        qqq  = fetch_prices("QQQ",  "2009-01-01")
    except Exception as e:
        print(f"    ⚠️ 데이터 로드 실패: {e}")
        return None

    if soxl.empty or qqq.empty:
        return None

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
    bt_df = run_backtest(dss_params, soxl, mode_series_df, os_start, today_str)

    # 최신 데이터
    prev_close = float(soxl.iloc[-1]['Close'])
    last_date = soxl.index[-1]
    mode_map = get_week_mode_map(mode_series_df, soxl.index)
    last_mode = mode_map.get(last_date, "AG")

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
    cum_realized = 0
    sell_count = 0
    holding_value = 0

    if bt_df is not None and not bt_df.empty:
        last_row = bt_df.iloc[-1]
        cash = float(last_row['예수금'])
        total_asset = float(last_row['총자산'])
        cum_realized = float(last_row['누적실현'])
        sell_count = int(last_row['누적매도'])

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

    next_buy_order = math.floor(prev_close * (1 + cur_buy_pct) * 100) / 100
    seed_per_trade = os_capital / cur_div if cur_div > 0 else os_capital
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
        _soxl = fetch_prices("SOXL", "2024-01-01")
        _raw_idx = _soxl.index
        _extra_bdays = pd.bdate_range(_raw_idx[-1] + pd.Timedelta(days=1), periods=60)
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

    _reserve_tiers = {i + 1 for pos, i, _is_stop, _, _ in _pos_data if not _is_stop}

    # 오늘의 주문
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

    # 예약 현황
    _reserve_lines = []
    for pos, i, _is_stop, _remain, _rdate in _pos_data:
        if _is_stop:
            continue
        _tier = i + 1
        _deadline = f"예약~{_rdate} (잔여 {_remain}일)" if _remain is not None and _rdate else ""
        _reserve_lines.append(f" ★티어{_tier}: ${pos['sell_target']:,.2f} {_deadline}")
    if _reserve_lines:
        lines.append(f"")
        lines.append(f"── 예약 현황 ──")
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

    try:
        price_df = fetch_prices(ticker, "2009-01-01")
    except Exception as e:
        print(f"    ⚠️ {ticker} 데이터 로드 실패: {e}")
        return None
    if price_df.empty:
        return None

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
# 메인
# ══════════════════════════════════════════════════════════════

def main():
    sheet_url = os.environ.get("ADMIN_SHEET_URL", "")
    if not sheet_url:
        print("❌ ADMIN_SHEET_URL 환경변수가 없습니다.")
        return

    print("👥 사용자 목록 로드 중...")
    client = get_gspread_client()
    users  = get_users(client, sheet_url)
    print(f"✅ {len(users)}명 로드")

    ok_count = skip_count = fail_count = 0

    for user in users:
        username = user.get("username", "")

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
                capital    = float(cfg.get("os_capital", DEFAULT_CAPITAL))
                os_start   = str(cfg.get("os_start", DEFAULT_OS_START)).strip() or DEFAULT_OS_START

                try:
                    df = fetch_prices(tk, os_start)
                except Exception as e:
                    print(f"    ❌ [{tk}] 데이터 로드 실패 → {e}")
                    fail_count += 1
                    continue

                if df.empty or len(df) < 2:
                    print(f"    ❌ [{tk}] 데이터 부족")
                    fail_count += 1
                    continue

                res = calc_today_order(df, a_buy, a_sell, sell_ratio, divisions, capital)
                if not res:
                    fail_count += 1
                    continue

                msg = build_avg_message(res, tk)
                ok, resp = send_telegram(avg_chat_id, avg_token, msg, parse_mode="Markdown")
                if ok:
                    print(f"    ✅ [종가평균/{tk}] 발송 성공")
                    ok_count += 1
                else:
                    print(f"    ❌ [종가평균/{tk}] 발송 실패 → {resp}")
                    fail_count += 1
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
                capital      = float(cfg.get("os_capital",   SD_DEFAULT_CAPITAL))
                os_start     = str(cfg.get("os_start", DEFAULT_OS_START)).strip() or DEFAULT_OS_START

                try:
                    r = calc_sd_today_order(
                        ticker=tk, os_start=os_start,
                        k_buy=k_buy, k_sell=k_sell,
                        sigma_period=sigma_period,
                        sell_ratio=sell_ratio,
                        divisions=divisions, renewal=renewal,
                        capital=capital,
                    )
                except Exception as e:
                    print(f"    ❌ [표준편차/{tk}] 계산 오류 → {e}")
                    fail_count += 1
                    continue

                if not r:
                    print(f"    ❌ [표준편차/{tk}] 데이터 부족")
                    fail_count += 1
                    continue

                msg = build_sd_message(r)
                ok, resp = send_telegram(sd_chat_id, sd_token, msg, parse_mode="HTML")
                if ok:
                    print(f"    ✅ [표준편차/{tk}] 발송 성공")
                    ok_count += 1
                else:
                    print(f"    ❌ [표준편차/{tk}] 발송 실패 → {resp}")
                    fail_count += 1
        else:
            print(f"  ⏭️  {username} [표준편차]: 미설정 → 건너뜀")
            skip_count += 1

        # ── 3. DSS 동파법 발송 ──────────────────────────────────
        dss_cfg = parse_dss_config(user)
        dss_chat_id = str(dss_cfg.get("tg_chat_id", "")).strip()
        dss_token   = str(dss_cfg.get("tg_token",   "")).strip()
        dss_accounts = dss_cfg.get("accounts", {})

        if dss_chat_id and dss_token and dss_accounts:
            print(f"  👤 {username} [DSS]: {list(dss_accounts.keys())} 처리 중...")
            for acct_name, acct_data in dss_accounts.items():
                try:
                    os_result = calc_dss_order(acct_data)
                except Exception as e:
                    print(f"    ❌ [DSS/{acct_name}] 계산 오류 → {e}")
                    fail_count += 1
                    continue

                if not os_result:
                    print(f"    ❌ [DSS/{acct_name}] 데이터 부족")
                    fail_count += 1
                    continue

                msg = build_dss_message(os_result, acct_name)
                ok, resp = send_telegram(dss_chat_id, dss_token, msg, parse_mode="HTML")
                if ok:
                    print(f"    ✅ [DSS/{acct_name}] 발송 성공")
                    ok_count += 1
                else:
                    print(f"    ❌ [DSS/{acct_name}] 발송 실패 → {resp}")
                    fail_count += 1
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

                try:
                    od = calc_sigma_order(tk, sigma_period)
                except Exception as e:
                    print(f"    ❌ [Sigma/{tk}] 계산 오류 → {e}")
                    fail_count += 1
                    continue

                if not od:
                    print(f"    ❌ [Sigma/{tk}] 데이터 부족")
                    fail_count += 1
                    continue

                msg = build_sigma_message(od, capital, divisions)
                ok, resp = send_telegram(sg_chat_id, sg_token, msg, parse_mode="HTML")
                if ok:
                    print(f"    ✅ [Sigma/{tk}] 발송 성공")
                    ok_count += 1
                else:
                    print(f"    ❌ [Sigma/{tk}] 발송 실패 → {resp}")
                    fail_count += 1
        else:
            print(f"  ⏭️  {username} [Sigma]: 미설정 → 건너뜀")
            skip_count += 1

        # ── 5. IUO 매매법 발송 ──────────────────────────────────
        iuo_cfg = parse_iuo_config(user)
        iuo_chat_id  = str(iuo_cfg.get("tg_chat_id", "")).strip()
        iuo_token    = str(iuo_cfg.get("tg_token",   "")).strip()
        iuo_accounts = iuo_cfg.get("accounts", {})

        if iuo_chat_id and iuo_token and iuo_accounts:
            print(f"  👤 {username} [IUO]: {list(iuo_accounts.keys())} 처리 중...")
            for acct_name, acct_data in iuo_accounts.items():
                try:
                    iuo_result = calc_iuo_order(acct_data)
                except Exception as e:
                    print(f"    ❌ [IUO/{acct_name}] 계산 오류 → {e}")
                    fail_count += 1
                    continue

                if not iuo_result:
                    print(f"    ❌ [IUO/{acct_name}] 데이터 부족")
                    fail_count += 1
                    continue

                msg = build_iuo_message(iuo_result, acct_name)
                ok, resp = send_telegram(iuo_chat_id, iuo_token, msg, parse_mode="HTML")
                if ok:
                    print(f"    ✅ [IUO/{acct_name}] 발송 성공")
                    ok_count += 1
                else:
                    print(f"    ❌ [IUO/{acct_name}] 발송 실패 → {resp}")
                    fail_count += 1
        else:
            print(f"  ⏭️  {username} [IUO]: 미설정 → 건너뜀")
            skip_count += 1

    print(f"\n🏁 완료: 성공 {ok_count}건 / 건너뜀 {skip_count}명 / 실패 {fail_count}건")

if __name__ == "__main__":
    main()
