"""
매일 15:00 KST에 GitHub Actions로 실행되는 텔레그램 자동 알림 스크립트.
Google Sheets users 탭의 모든 사용자에게 각자 설정 기준으로 LOC 주문을 발송.
"""

import os, json, math, requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# ── 상수 ───────────────────────────────────────────────
TICKER             = "SOXL"
DEFAULT_A_BUY      = -0.005
DEFAULT_A_SELL     =  0.009
DEFAULT_SELL_RATIO = 100.0
DEFAULT_DIVISIONS  = 5
DEFAULT_CAPITAL    = 10000.0
DEFAULT_OS_START   = "2024-01-01"

GS_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

# ── Google Sheets 연결 ─────────────────────────────────
def get_gspread_client():
    sa_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
    if not sa_json:
        raise RuntimeError("GCP_SERVICE_ACCOUNT_JSON 환경변수가 없습니다.")
    info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(info, scopes=GS_SCOPES)
    return gspread.authorize(creds)

def get_users(client, sheet_url: str) -> list[dict]:
    sh = client.open_by_url(sheet_url)
    ws = sh.worksheet("users")
    return ws.get_all_records()

# ── 가격 데이터 ────────────────────────────────────────
def fetch_prices(ticker: str, start_date: str) -> pd.DataFrame:
    """start_date부터 오늘까지 종가 데이터 로드."""
    end = datetime.today() + timedelta(days=1)  # yfinance end는 exclusive
    df = yf.download(ticker, start=start_date,
                     end=end.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
    # yfinance 최신버전 멀티컬럼 대응
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Close"]].dropna()
    df["Close"] = df["Close"].astype(float)
    return df

# ── LOC 경계가 계산 ────────────────────────────────────
def buy_limit_price(p1: float, p2: float, a: float) -> float:
    return (p1 + p2) * (1 + a) / (2 - a)

# ── 포트폴리오 시뮬레이션 (os_start부터 전체 시뮬레이션) ─────
def calc_today_order(df: pd.DataFrame,
                     a_buy: float, a_sell: float,
                     sell_ratio: float, divisions: int,
                     capital: float) -> dict:
    """앱과 동일하게 os_start부터 전체 시뮬레이션하여 오늘 주문 산출."""
    closes = df["Close"].values
    if len(closes) < 3:
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
                math.floor(chunk / x + 1e-9),
                math.floor(cash / x + 1e-9),
            )
            if buy_qty > 0:
                total_inv = avg_cost * shares + x * buy_qty
                shares   += buy_qty
                avg_cost  = total_inv / shares
                cash     -= buy_qty * x
                open_tiers.append({"price": x, "qty": buy_qty})

        prev_asset = cash + shares * x

    # ── 다음 주문 계산 (마지막 2일 종가로) ──
    p1_now = float(closes[-1])
    p2_now = float(closes[-2])
    tb_next = buy_limit_price(p1_now, p2_now, a_buy)
    ts_next = buy_limit_price(p1_now, p2_now, a_sell)
    current_asset = cash + shares * p1_now
    chunk_now = current_asset / divisions

    buy_qty_next = min(
        math.floor(chunk_now / tb_next + 1e-9),
        math.floor(cash / tb_next + 1e-9),
    ) if cash > 0 else 0

    sell_qty_next = math.floor(shares * (sell_ratio / 100.0)) if shares > 0 else 0

    return {
        "p1": p1_now,
        "p2": p2_now,
        "tb": round(tb_next, 2),
        "ts": round(ts_next, 2),
        "shares": shares,
        "buy_qty": buy_qty_next,
        "sell_qty": sell_qty_next,
        "cash": cash,
        "avg_cost": avg_cost,
    }

# ── 텔레그램 메시지 생성 ────────────────────────────────
def build_message(res: dict, ticker: str) -> str:
    today = datetime.today().strftime("%Y-%m-%d")
    lines = [f"📋 *종가평균 주문* ({ticker})"]
    lines.append(f"기준일: {today}")
    lines.append(f"기준가: p1={res['p1']:.2f} / p2={res['p2']:.2f}")
    lines.append("")

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

# ── 텔레그램 발송 ──────────────────────────────────────
def send_telegram(chat_id: str, token: str, text: str):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, json={
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
    }, timeout=10)
    return resp.ok, resp.text

# ── 메인 ───────────────────────────────────────────────
def main():
    sheet_url = os.environ.get("ADMIN_SHEET_URL", "")
    if not sheet_url:
        print("❌ ADMIN_SHEET_URL 환경변수가 없습니다.")
        return

    print("👥 사용자 목록 로드 중...")
    client = get_gspread_client()
    users  = get_users(client, sheet_url)
    print(f"✅ {len(users)}명 로드")

    ok_count, skip_count, fail_count = 0, 0, 0

    for user in users:
        username = user.get("username", "")
        chat_id  = str(user.get("tg_chat_id", "")).strip()
        token    = str(user.get("tg_token",   "")).strip()

        if not chat_id or not token:
            print(f"  ⏭️  {username}: 텔레그램 미설정 → 건너뜀")
            skip_count += 1
            continue

        # 사용자별 파라미터
        a_buy      = float(user.get("a_buy",      DEFAULT_A_BUY))      if user.get("a_buy")      else DEFAULT_A_BUY
        a_sell     = float(user.get("a_sell",     DEFAULT_A_SELL))     if user.get("a_sell")     else DEFAULT_A_SELL
        sell_ratio = float(user.get("sell_ratio", DEFAULT_SELL_RATIO)) if user.get("sell_ratio") else DEFAULT_SELL_RATIO
        divisions  = int(float(user.get("divisions", DEFAULT_DIVISIONS))) if user.get("divisions") else DEFAULT_DIVISIONS
        capital    = float(user.get("os_capital", DEFAULT_CAPITAL))    if user.get("os_capital") else DEFAULT_CAPITAL
        os_start   = str(user.get("os_start",    DEFAULT_OS_START)).strip() or DEFAULT_OS_START

        # os_start부터 전체 가격 데이터 로드 (앱과 동일한 방식)
        print(f"  📊 {username}: {os_start}부터 데이터 로드 중...")
        try:
            df = fetch_prices(TICKER, os_start)
        except Exception as e:
            print(f"  ❌ {username}: 데이터 로드 실패 → {e}")
            fail_count += 1
            continue

        if df.empty or len(df) < 3:
            print(f"  ❌ {username}: 데이터 부족")
            fail_count += 1
            continue

        print(f"     최근 종가: {float(df['Close'].iloc[-1]):.2f} (p1={float(df['Close'].iloc[-2]):.2f})")

        res = calc_today_order(df, a_buy, a_sell, sell_ratio, divisions, capital)
        if not res:
            print(f"  ❌ {username}: 주문 계산 실패")
            fail_count += 1
            continue

        msg = build_message(res, TICKER)
        ok, resp = send_telegram(chat_id, token, msg)
        if ok:
            print(f"  ✅ {username}: 발송 성공 (매수 {res['buy_qty']}주 ${res['tb']:.2f})")
            ok_count += 1
        else:
            print(f"  ❌ {username}: 발송 실패 → {resp}")
            fail_count += 1

    print(f"\n🏁 완료: 성공 {ok_count}명 / 건너뜀 {skip_count}명 / 실패 {fail_count}명")

if __name__ == "__main__":
    main()
