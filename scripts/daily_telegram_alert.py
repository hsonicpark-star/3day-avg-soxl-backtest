"""
매일 15:00 KST에 GitHub Actions로 실행되는 텔레그램 자동 알림 스크립트.
Google Sheets users 탭의 모든 사용자에게
각자 등록된 ticker_settings 기준으로 ticker별 LOC 주문을 발송.

전략별 발송:
  1. 종가평균매매 (tg_chat_id / tg_token / ticker_settings)
  2. 표준편차매매  (sd_tg_chat_id / sd_tg_token / sd_ticker_settings)
  3. DSS 동파법   (dss_config 내 tg_chat_id / tg_token + 계좌별 발송)
"""

import os, sys, json, math, requests
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

def get_users(client, sheet_url: str) -> list:
    sh = client.open_by_url(sheet_url)
    ws = sh.worksheet("users")
    return ws.get_all_records()

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
        _tdays = _soxl.index
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

    print(f"\n🏁 완료: 성공 {ok_count}건 / 건너뜀 {skip_count}명 / 실패 {fail_count}건")

if __name__ == "__main__":
    main()
