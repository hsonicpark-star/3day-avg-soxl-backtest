"""
매일 15:00 KST에 GitHub Actions로 실행되는 텔레그램 자동 알림 스크립트.
Google Sheets users 탭의 모든 사용자에게
각자 등록된 ticker_settings 기준으로 ticker별 LOC 주문을 발송.

전략별 발송:
  1. 종가평균매매 (tg_chat_id / tg_token / ticker_settings)
  2. 표준편차매매  (sd_tg_chat_id / sd_tg_token / sd_ticker_settings)
"""

import os, json, math, requests
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

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

    print(f"\n🏁 완료: 성공 {ok_count}건 / 건너뜀 {skip_count}명 / 실패 {fail_count}건")

if __name__ == "__main__":
    main()
