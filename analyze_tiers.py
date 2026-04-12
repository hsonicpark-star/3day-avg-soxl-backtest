"""
5티어 완전 투자 구간 분석
- 2014-01-01 ~ 오늘
- 기본 파라미터: a_buy=-0.005, a_sell=0.009, 분할수=5, 매도비율=100%
"""
import yfinance as yf
import pandas as pd
import numpy as np
import math
from datetime import datetime, timedelta

# ── 파라미터 ──────────────────────────────────
TICKER     = "SOXL"
START      = "2014-01-01"
A_BUY      = -0.005
A_SELL     =  0.009
SELL_RATIO = 100.0
DIVISIONS  = 5
CAPITAL    = 10000.0

# ── 데이터 다운로드 ───────────────────────────
print("SOXL 데이터 다운로드 중...")
raw = yf.download(TICKER, start="2013-12-15", end=datetime.today().strftime("%Y-%m-%d"),
                  progress=False, auto_adjust=True)
if isinstance(raw.columns, pd.MultiIndex):
    raw = raw.xs(TICKER, axis=1, level="Ticker")
df = raw[["Close"]].copy()
df.index = pd.to_datetime(df.index)
df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
df = df.dropna()

sim = df.loc[pd.to_datetime(START):].copy()
sim["p1"] = sim["Close"].shift(1)
sim["p2"] = sim["Close"].shift(2)
sim = sim.dropna()

closes   = sim["Close"].values.astype(float)
p1s      = sim["p1"].values.astype(float)
p2s      = sim["p2"].values.astype(float)
tgt_buy  = (p1s + p2s) * (1 + A_BUY)  / (2 - A_BUY)
tgt_sell = (p1s + p2s) * (1 + A_SELL) / (2 - A_SELL)
dates    = sim.index

# ── 시뮬레이션 ────────────────────────────────
cash       = CAPITAL
shares     = 0
prev_asset = CAPITAL
avg_cost   = 0.0
open_tiers = []  # {'date', 'price', 'qty'}

full_invest_events = []  # (5번째 매수일, 매도일 or None, 보유일수)
fifth_buy_date = None

for i in range(len(closes)):
    x  = closes[i]
    tb = tgt_buy[i]
    ts = tgt_sell[i]
    date = dates[i]
    current_chunk = prev_asset / DIVISIONS

    if shares > 0 and x >= ts:
        sell_qty = math.floor(shares * (SELL_RATIO / 100.0))
        if sell_qty > 0:
            # 매도 발생 → 5티어 구간 종료
            if fifth_buy_date is not None:
                holding = (date - fifth_buy_date).days
                full_invest_events.append({
                    "5번째_매수일": fifth_buy_date.date(),
                    "매도일":       date.date(),
                    "보유일수":     holding,
                    "5번째_매수가": fifth_buy_price,
                    "매도가":       x,
                    "손익률(%)":   round((x / avg_cost - 1) * 100, 2),
                })
                fifth_buy_date = None

            cash   += sell_qty * x
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
                avg_cost = 0.0; open_tiers = []

    elif x <= tb:
        buy_qty = min(
            math.floor(current_chunk / x + 1e-9),
            math.floor(cash / x + 1e-9)
        )
        if buy_qty > 0:
            total_inv = avg_cost * shares + buy_qty * x
            shares   += buy_qty
            avg_cost  = total_inv / shares
            cash     -= buy_qty * x
            open_tiers.append({"date": date, "price": x, "qty": buy_qty})

            # 5번째 티어 체결 시점 기록
            if len(open_tiers) == DIVISIONS and fifth_buy_date is None:
                fifth_buy_date  = date
                fifth_buy_price = x

    asset      = cash + shares * x
    prev_asset = asset

# 현재도 5티어 상태이면 미청산 구간
if fifth_buy_date is not None:
    today   = dates[-1]
    holding = (today - fifth_buy_date).days
    full_invest_events.append({
        "5번째_매수일": fifth_buy_date.date(),
        "매도일":       "미청산(보유중)",
        "보유일수":     holding,
        "5번째_매수가": fifth_buy_price,
        "매도가":       closes[-1],
        "손익률(%)":   round((closes[-1] / avg_cost - 1) * 100, 2),
    })

# ── 결과 출력 ─────────────────────────────────
result_df = pd.DataFrame(full_invest_events)
print(f"\n{'='*60}")
print(f"5티어 완전 투자 발생 횟수: {len(full_invest_events)}회")
print(f"{'='*60}\n")

top10 = result_df.sort_values("보유일수", ascending=False).head(10).reset_index(drop=True)
top10.index += 1
print("[ TOP 10 - 5티어 완전 투자 후 가장 긴 보유 기간 ]")
print(top10.to_string())
print()

# 전체 목록도 출력
print(f"\n[ 전체 {len(result_df)}회 목록 - 시간순 ]")
all_sorted = result_df.sort_values("5번째_매수일").reset_index(drop=True)
all_sorted.index += 1
print(all_sorted.to_string())
