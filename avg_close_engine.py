"""
avg_close_engine.py — 종가평균매매 (LOC Average Close) 엔진 (pure Python, no Streamlit)

웹앱(strategies/avg_close.py)과 자동발송 스크립트(scripts/daily_telegram_alert.py)가
공통으로 사용하는 백테스트/주문표 엔진.

Streamlit · common.* 의존성이 없어야 함 — GitHub Actions 환경에서도 import 가능.
의존성: numpy, pandas, math, datetime
"""
from __future__ import annotations

import math
import numpy as np
import pandas as pd
from datetime import datetime


# ══════════════════════════════════════════════════════════════
# 경계가 계산 헬퍼
# ══════════════════════════════════════════════════════════════

def buy_limit_price(p1, p2, a):
    """2일 이동평균 LOC 경계가 (legacy: n_days=2 호환)."""
    return (p1 + p2) * (1 + a) / (2 - a)


def calc_boundary(prices, a):
    """N일 종가 리스트 [p1,p2,...,pN]로 LOC 경계가 계산. N=2이면 buy_limit_price와 동일."""
    n = len(prices)
    return sum(prices) * (1 + a) / (n - a)


def _prep_sim(sim_raw, n_days, a_buy, a_sell):
    """N일 이동평균 기반 매수/매도 경계가 벡터 계산.
    Returns: (sim_df, closes_arr, tgt_buy_arr, tgt_sell_arr)"""
    for k in range(1, n_days + 1):
        sim_raw[f"p{k}"] = sim_raw["Close"].shift(k)
    sim = sim_raw.dropna(subset=[f"p{n_days}"])
    if sim.empty:
        return sim, np.array([]), np.array([]), np.array([])
    psum = sum(sim[f"p{k}"].values.astype(float) for k in range(1, n_days + 1))
    closes   = sim["Close"].values.astype(float)
    tgt_buy  = psum * (1 + a_buy)  / (n_days - a_buy)
    tgt_sell = psum * (1 + a_sell) / (n_days - a_sell)
    return sim, closes, tgt_buy, tgt_sell


def scalar(v):
    if isinstance(v, (pd.Series, np.ndarray)):
        return float(v.iloc[0] if isinstance(v, pd.Series) else v.flat[0])
    return float(v)


# ══════════════════════════════════════════════════════════════
# 백테스트 엔진 (full): 히스토리 + 모든 지표 반환
# ══════════════════════════════════════════════════════════════

def run_backtest(
    price_df, start_date, end_date,
    a_buy, a_sell, sell_ratio, divisions, initial_capital,
    return_history=False, n_days=2,
):
    sim_raw = price_df.loc[pd.to_datetime(start_date):pd.to_datetime(end_date)].copy()
    sim, closes, tgt_buy, tgt_sell = _prep_sim(sim_raw, n_days, a_buy, a_sell)
    if sim.empty:
        return None

    cash       = float(initial_capital)
    shares     = 0
    avg_cost   = 0.0
    prev_asset = float(initial_capital)
    assets     = np.empty(len(closes))
    cash_arr   = np.empty(len(closes))
    buy_count  = sell_count = 0
    sell_pnls  = []          # 매도별 손익률(%) 기록
    history    = [] if return_history else None

    for i in range(len(closes)):
        x  = closes[i]
        tb = tgt_buy[i]
        ts = tgt_sell[i]
        current_chunk = prev_asset / divisions
        action = "-"; trade_shares = 0; trade_amount = 0.0

        if shares > 0 and x >= ts:
            sell_qty = math.floor(shares * (sell_ratio / 100.0))
            if sell_qty > 0:
                pnl_pct = (x / avg_cost - 1) * 100 if avg_cost > 0 else 0.0
                sell_pnls.append(pnl_pct)
                action = "SELL"; trade_shares = -sell_qty; trade_amount = sell_qty * x
                cash += trade_amount; shares -= sell_qty; sell_count += 1
                if shares == 0:
                    avg_cost = 0.0
        elif x <= tb:
            # LOC 주문: 수량은 기준가(tb) 기준, 체결은 실제 종가(x)
            buy_qty = min(math.floor(current_chunk / tb + 1e-9), math.floor(cash / tb + 1e-9))
            if buy_qty > 0:
                avg_cost  = (avg_cost * shares + x * buy_qty) / (shares + buy_qty)
                action = "BUY"; trade_shares = buy_qty; trade_amount = buy_qty * x
                cash -= trade_amount; shares += buy_qty; buy_count += 1

        asset = cash + shares * x
        prev_asset = asset
        assets[i]  = asset
        cash_arr[i] = cash

        if return_history:
            history.append({
                "날짜": sim.index[i].date(), "종가(x)": x,
                "전날(p1)": float(sim["p1"].values[i]),
                "전전날(p2)": float(sim["p2"].values[i]) if "p2" in sim.columns else float("nan"),
                "매수경계가": tb, "매도경계가": ts,
                "매매": action, "거래주수": trade_shares,
                "거래금액($)": trade_amount, "보유주수": shares,
                "현금($)": cash, "총자산($)": asset,
            })

    final_asset  = float(assets[-1])
    peak         = np.maximum.accumulate(assets)
    mdd          = float(((assets - peak) / peak).min())
    years        = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days / 365.25
    total_return = (final_asset / initial_capital) - 1.0
    cagr         = ((final_asset / initial_capital) ** (1.0 / years) - 1.0) if years > 0 else 0.0
    calmar       = cagr / abs(mdd) if mdd != 0 else 0.0

    _win_cnt  = sum(1 for p in sell_pnls if p > 0)
    _avg_pnl  = sum(sell_pnls) / len(sell_pnls) if sell_pnls else 0.0
    _max_pnl  = max(sell_pnls) if sell_pnls else 0.0
    _min_pnl  = min(sell_pnls) if sell_pnls else 0.0

    out = dict(
        final_asset=final_asset, total_return=total_return,
        cagr=cagr, mdd=mdd, calmar=calmar,
        buy_count=buy_count, sell_count=sell_count,
        win_count=_win_cnt, avg_pnl=_avg_pnl,
        max_pnl=_max_pnl, min_pnl=_min_pnl,
        assets=assets, dates=sim.index,
        sell_pnls_list=sell_pnls,
        cash_series=cash_arr,
    )
    if return_history:
        out["history"] = pd.DataFrame(history)
    return out


# ══════════════════════════════════════════════════════════════
# 백테스트 엔진 (fast): 최적화 전용, history 없음
# ══════════════════════════════════════════════════════════════

def run_backtest_fast(
    price_df, start_date, end_date,
    a_buy, a_sell, sell_ratio, divisions, initial_capital,
    n_days=2,
):
    """run_backtest 경량 버전 -- 최적화 전용. history/assets 배열 생성 없이 최종 지표만 반환."""
    sim_raw = price_df.loc[pd.to_datetime(start_date):pd.to_datetime(end_date)].copy()
    # inline _prep_sim to avoid extra overhead
    for k in range(1, n_days + 1):
        sim_raw[f"p{k}"] = sim_raw["Close"].shift(k)
    sim_raw = sim_raw.dropna(subset=[f"p{n_days}"])
    if sim_raw.empty:
        return None

    psum = np.zeros(len(sim_raw), dtype=np.float64)
    for k in range(1, n_days + 1):
        psum += sim_raw[f"p{k}"].values.astype(np.float64)
    closes  = sim_raw["Close"].values.astype(np.float64)
    tgt_buy  = psum * (1.0 + a_buy)  / (n_days - a_buy)
    tgt_sell = psum * (1.0 + a_sell) / (n_days - a_sell)

    cash      = float(initial_capital)
    shares    = 0
    avg_cost  = 0.0
    prev_asset = float(initial_capital)
    peak      = float(initial_capital)
    max_dd    = 0.0
    buy_count = 0
    sell_count = 0
    sell_ratio_f = sell_ratio / 100.0
    n = len(closes)

    for i in range(n):
        x  = closes[i]
        tb = tgt_buy[i]
        ts = tgt_sell[i]
        current_chunk = prev_asset / divisions

        if shares > 0 and x >= ts:
            sell_qty = int(shares * sell_ratio_f)
            if sell_qty > 0:
                cash += sell_qty * x
                shares -= sell_qty
                sell_count += 1
                if shares == 0:
                    avg_cost = 0.0
        elif x <= tb:
            buy_qty = min(int(current_chunk / tb + 1e-9), int(cash / tb + 1e-9))
            if buy_qty > 0:
                avg_cost = (avg_cost * shares + x * buy_qty) / (shares + buy_qty)
                cash -= buy_qty * x
                shares += buy_qty
                buy_count += 1

        asset = cash + shares * x
        prev_asset = asset
        if asset > peak:
            peak = asset
        dd = (asset - peak) / peak
        if dd < max_dd:
            max_dd = dd

    final_asset = cash + shares * closes[-1] if n > 0 else float(initial_capital)
    years = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days / 365.25
    total_return = (final_asset / initial_capital) - 1.0
    cagr = ((final_asset / initial_capital) ** (1.0 / years) - 1.0) if years > 0 else 0.0
    calmar = cagr / abs(max_dd) if max_dd != 0.0 else 0.0

    return dict(
        final_asset=final_asset, cagr=cagr, mdd=max_dd, calmar=calmar,
        total_return=total_return, buy_count=buy_count, sell_count=sell_count,
    )


# ══════════════════════════════════════════════════════════════
# 오늘의 주문표 — 시뮬 후 다음 거래일 LOC 기준가 + 예상수량 반환
# ══════════════════════════════════════════════════════════════

def run_portfolio_for_ordersheet(
    price_df, start_date, ticker_name,
    a_buy, a_sell, sell_ratio, divisions, initial_capital,
    n_days=2,
):
    """백테스트를 오늘까지 실행하며 평균단가·티어·매도이력을 추적."""
    today = datetime.today().date()
    sim_raw = price_df.loc[pd.to_datetime(start_date):pd.to_datetime(today)].copy()
    all_closes = sim_raw["Close"].dropna().values.astype(float)
    # N일 계산에는 최소 N개 종가 필요
    if len(all_closes) < n_days:
        return None

    cash        = float(initial_capital)
    shares      = 0
    prev_asset  = float(initial_capital)
    peak_asset  = float(initial_capital)
    avg_cost    = 0.0
    open_tiers  = []   # [{'date': Timestamp, 'price': float, 'qty': int}]
    sell_trades = []
    buy_trades  = []
    daily_log   = []
    _hold_tday  = 0    # 거래일 기준 보유기간 카운터 (첫 매수일 = 0)

    if not sim_raw.empty:
        sim, closes, tgt_buy, tgt_sell = _prep_sim(sim_raw.copy(), n_days, a_buy, a_sell)
    else:
        sim    = sim_raw
        closes = np.array([])
        tgt_buy = tgt_sell = np.array([])

    for i in range(len(closes)):
        x    = closes[i]
        tb   = tgt_buy[i]
        ts   = tgt_sell[i]
        date = sim.index[i]
        current_chunk = prev_asset / divisions
        _day_action  = "-"
        _day_qty     = 0
        _day_amt     = 0.0
        _day_pnl_amt = None   # 실현손익($) — SELL 시에만 기록
        _day_pnl_pct = None   # 실현손익률(%) — SELL 시에만 기록

        if shares > 0 and x >= ts:
            sell_qty = math.floor(shares * (sell_ratio / 100.0))
            if sell_qty > 0:
                holding_days = _hold_tday  # 거래일 기준 보유기간
                _day_pnl_amt = round((x - avg_cost) * sell_qty, 2) if avg_cost > 0 else 0.0
                _day_pnl_pct = round((x / avg_cost - 1) * 100, 2)  if avg_cost > 0 else 0.0

                _date_val = date.date() if hasattr(date, "date") else date
                sell_trades.append({
                    "날짜":    str(_date_val),
                    "구분":    "매도",
                    "티커":    ticker_name,
                    "체결가":  x,
                    "avg_cost": avg_cost,
                    "수량":    sell_qty,
                    "금액($)": round(sell_qty * x, 2),
                    "보유기간(일)": holding_days,
                    "비고":    f"평단 ${avg_cost:.2f} → 수익률 {(x/avg_cost-1)*100:+.2f}%",
                })

                cash   += sell_qty * x
                shares -= sell_qty
                _day_action = "SELL"
                _day_qty    = -sell_qty   # 음수로 표시 (백테스트와 동일)
                _day_amt    = round(sell_qty * x, 2)

                # FIFO 티어 차감
                remaining = sell_qty
                while remaining > 0 and open_tiers:
                    if open_tiers[0]["qty"] <= remaining:
                        remaining -= open_tiers[0]["qty"]
                        open_tiers.pop(0)
                    else:
                        open_tiers[0]["qty"] -= remaining
                        remaining = 0

                # 평균단가 재계산
                if shares > 0 and open_tiers:
                    total_inv = sum(t["price"] * t["qty"] for t in open_tiers)
                    total_qty = sum(t["qty"] for t in open_tiers)
                    avg_cost  = total_inv / total_qty if total_qty > 0 else 0.0
                else:
                    avg_cost   = 0.0
                    open_tiers = []

        elif x <= tb:
            # LOC 주문: 수량은 기준가(tb) 기준, 체결은 실제 종가(x)
            buy_qty = min(
                math.floor(current_chunk / tb + 1e-9),
                math.floor(cash / tb + 1e-9),
            )
            if buy_qty > 0:
                total_inv = avg_cost * shares + x * buy_qty
                shares   += buy_qty
                avg_cost  = total_inv / shares
                cash     -= buy_qty * x
                open_tiers.append({"date": date, "price": x, "qty": buy_qty})
                _date_val = date.date() if hasattr(date, "date") else date
                buy_trades.append({
                    "날짜":    str(_date_val),
                    "구분":    "매수",
                    "티커":    ticker_name,
                    "체결가":  x,
                    "수량":    buy_qty,
                    "금액($)": round(buy_qty * x, 2),
                    "비고":    f"평단 ${avg_cost:.2f} | 보유 {shares}주",
                })
                _day_action = "BUY"
                _day_qty    = buy_qty
                _day_amt    = round(buy_qty * x, 2)

        asset      = cash + shares * x
        prev_asset = asset
        peak_asset = max(peak_asset, asset)

        # 전체 날짜 기록
        _date_val2 = date.date() if hasattr(date, "date") else date
        if shares > 0:
            _hdays = _hold_tday
            _hold_tday += 1
        else:
            _hdays = "-"
            _hold_tday = 0
        daily_log.append({
            "날짜":          str(_date_val2),
            "종가(x)":       round(x, 4),
            "전날(p1)":      round(float(sim["p1"].values[i]), 4),
            "전전날(p2)":    round(float(sim["p2"].values[i]), 4) if "p2" in sim.columns else float("nan"),
            "매수경계가":    round(tb, 4),
            "매도경계가":    round(ts, 4),
            "매매":          _day_action,
            "거래주수":      _day_qty,
            "거래금액($)":   _day_amt,
            "실현손익($)":   _day_pnl_amt,
            "실현손익률(%)": _day_pnl_pct,
            "보유주수":      shares,
            "보유기간":      _hdays if shares > 0 else "-",
            "현금($)":       round(cash, 2),
            "총자산($)":     round(asset, 2),
        })

    latest_price  = float(all_closes[-1])
    current_asset = cash + shares * latest_price
    total_return  = (current_asset - initial_capital) / initial_capital
    current_dd    = (current_asset - peak_asset) / peak_asset  # <= 0
    stock_weight  = (shares * latest_price / current_asset) if current_asset > 0 else 0.0
    years         = (today - pd.to_datetime(start_date).date()).days / 365.25
    cagr          = ((current_asset / initial_capital) ** (1.0 / years) - 1.0) if years > 0 else 0.0

    # 오늘 LOC 기준: 가장 최근 N개 종가
    p_now = [float(all_closes[-(k)]) for k in range(1, n_days + 1)]
    p1_now = p_now[0]
    p2_now = p_now[1] if len(p_now) > 1 else p_now[0]
    next_buy_primary   = calc_boundary(p_now, a_buy)
    next_buy_secondary = next_buy_primary * 0.95
    next_sell_target   = calc_boundary(p_now, a_sell)

    min_tier_price = min(t["price"] for t in open_tiers) if open_tiers else 0.0
    chunk_now      = current_asset / divisions
    qty_primary    = math.floor(chunk_now / next_buy_primary + 1e-9) if next_buy_primary > 0 else 0
    min_str        = f"{min_tier_price:.2f}" if open_tiers else "-"

    pending_buys = [
        {
            "구분":   "매수", "티커": ticker_name,
            "주문가": next_buy_primary,
            "수량":   qty_primary,
            "금액":   qty_primary * next_buy_primary,
            "비고":   (f"LOC {next_buy_primary:.2f} - "
                       f"보유 티어 최저가({min_str}) "
                       f"목표매도가({next_sell_target:.2f})"),
        },
    ]

    return {
        "initial_capital":    initial_capital,
        "current_asset":      current_asset,
        "total_return":       total_return,
        "current_dd":         current_dd,
        "stock_weight":       stock_weight,
        "avg_cost":           avg_cost,
        "shares":             shares,
        "cash":               cash,
        "sell_trades":        sell_trades,
        "trade_history":      sorted(buy_trades + sell_trades, key=lambda r: r["날짜"]),
        "daily_log":          daily_log,
        "pending_buys":       pending_buys,
        "open_tiers":         open_tiers,
        "latest_price":       latest_price,
        "p1_now":             p1_now,
        "p2_now":             p2_now,
        "next_sell_target": next_sell_target,
        "next_buy_primary": next_buy_primary,
        "cagr":             cagr,
        "start_date":       start_date,
        "end_date":         today,
    }


# ══════════════════════════════════════════════════════════════
# 5-tier Analysis Functions
# ══════════════════════════════════════════════════════════════

def run_5tier_analysis(price_df, start_date, end_date, a_buy, a_sell, sell_ratio, divisions, initial_capital, n_days=2):
    """분할수(N) 이상 매수 후 매도된 사이클(N티어 완전 투자) 이벤트 추출."""
    sim_raw = price_df.loc[pd.to_datetime(start_date):pd.to_datetime(end_date)].copy()
    sim, closes, tgt_buy, tgt_sell = _prep_sim(sim_raw, n_days, a_buy, a_sell)
    if sim.empty:
        return []

    cash       = float(initial_capital)
    shares     = 0
    avg_cost   = 0.0
    open_tiers = []
    prev_asset = float(initial_capital)
    cycle_buys = []   # 현재 사이클의 매수 목록
    events     = []

    # 거래일 인덱스 매핑 (Timestamp → 거래일 순번)
    _tday_idx = {ts: idx for idx, ts in enumerate(sim.index)}

    for i in range(len(closes)):
        x    = closes[i]
        tb   = tgt_buy[i]
        ts   = tgt_sell[i]
        date = sim.index[i]
        current_chunk = prev_asset / divisions

        if shares > 0 and x >= ts:
            sell_qty = math.floor(shares * (sell_ratio / 100.0))
            if sell_qty > 0:
                # N티어 이상 매수 사이클이면 이벤트 기록
                if len(cycle_buys) >= divisions:
                    nth  = cycle_buys[divisions - 1]
                    hold = _tday_idx[date] - _tday_idx[nth["date"]]
                    events.append({
                        "No":           len(events) + 1,
                        "5번째 매수일": str(nth["date"].date()),
                        "매도일":       str(date.date()),
                        "보유일수":     hold,
                        "5번째 매수가": round(nth["price"], 2),
                        "평균단가":     round(avg_cost, 2),
                        "매도가":       round(x, 2),
                        "손익률":       round((x / avg_cost - 1) * 100, 2) if avg_cost > 0 else 0,
                    })

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
                    avg_cost   = 0.0
                    open_tiers = []
                    cycle_buys = []   # 포지션 청산 → 사이클 초기화

        elif x <= tb:
            # LOC 주문: 수량은 기준가(tb) 기준, 체결은 실제 종가(x)
            buy_qty = min(
                math.floor(current_chunk / tb + 1e-9),
                math.floor(cash / tb + 1e-9),
            )
            if buy_qty > 0:
                total_inv = avg_cost * shares + x * buy_qty
                shares   += buy_qty
                avg_cost  = total_inv / shares
                cash     -= buy_qty * x
                open_tiers.append({"date": date, "price": x, "qty": buy_qty})
                cycle_buys.append({"date": date, "price": x})

        asset      = cash + shares * x
        prev_asset = asset

    return events


def run_tier_breakdown_analysis(price_df, start_date, end_date, a_buy, a_sell, sell_ratio, divisions, initial_capital, n_days=2):
    """티어별 완전 청산 사이클 분석.
    포지션이 0이 될 때까지 추적하여, 각 사이클에서 몇 티어까지 매수됐는지 기록.
    1티어만 사고 매도, 2티어 사고 매도, ... N티어 사고 매도 각각 통계 산출.
    """
    sim_raw = price_df.loc[pd.to_datetime(start_date):pd.to_datetime(end_date)].copy()
    sim, closes, tgt_buy, tgt_sell = _prep_sim(sim_raw, n_days, a_buy, a_sell)
    if sim.empty:
        return []

    cash       = float(initial_capital)
    shares     = 0
    avg_cost   = 0.0
    open_tiers = []
    prev_asset = float(initial_capital)

    # 사이클 추적
    cycle_buys            = []   # 이번 사이클의 매수 내역
    cycle_total_invested  = 0.0  # 이번 사이클 총 투자금
    cycle_total_received  = 0.0  # 이번 사이클 총 매도 수익
    cycle_start_date      = None

    events = []

    # 거래일 인덱스 매핑 (Timestamp → 거래일 순번)
    _tday_idx = {ts: idx for idx, ts in enumerate(sim.index)}

    for i in range(len(closes)):
        x    = closes[i]
        tb   = tgt_buy[i]
        ts   = tgt_sell[i]
        date = sim.index[i]
        current_chunk = prev_asset / divisions

        if shares > 0 and x >= ts:
            sell_qty = math.floor(shares * (sell_ratio / 100.0))
            if sell_qty > 0:
                proceeds               = sell_qty * x
                cash                  += proceeds
                shares                -= sell_qty
                cycle_total_received  += proceeds

                # FIFO 티어 소진
                remaining = sell_qty
                while remaining > 0 and open_tiers:
                    if open_tiers[0]["qty"] <= remaining:
                        remaining -= open_tiers[0]["qty"]
                        open_tiers.pop(0)
                    else:
                        open_tiers[0]["qty"] -= remaining
                        remaining = 0

                if shares == 0:
                    # 포지션 완전 청산 → 사이클 기록
                    n_tiers   = len(cycle_buys)
                    hold_days = (_tday_idx[date] - _tday_idx[cycle_start_date]) if cycle_start_date else 0
                    pnl       = (cycle_total_received - cycle_total_invested) / cycle_total_invested * 100 \
                                if cycle_total_invested > 0 else 0.0
                    events.append({
                        "티어수":     n_tiers,
                        "시작일":     str(cycle_start_date.date()) if cycle_start_date else "",
                        "매도완료일": str(date.date()),
                        "보유일수":   hold_days,
                        "평균단가":   round(cycle_total_invested / sum(b["qty"] for b in cycle_buys), 2)
                                      if cycle_buys and sum(b["qty"] for b in cycle_buys) > 0 else 0.0,
                        "최종매도가": round(x, 2),
                        "손익률":     round(pnl, 2),
                    })
                    # 사이클 초기화
                    cycle_buys           = []
                    cycle_total_invested = 0.0
                    cycle_total_received = 0.0
                    cycle_start_date     = None
                    avg_cost             = 0.0
                    open_tiers           = []
                else:
                    # 부분 매도 → avg_cost 재계산
                    if open_tiers:
                        total_inv = sum(t["price"] * t["qty"] for t in open_tiers)
                        total_qty = sum(t["qty"] for t in open_tiers)
                        avg_cost  = total_inv / total_qty if total_qty > 0 else 0.0

        elif x <= tb:
            # LOC 주문: 수량은 기준가(tb) 기준, 체결은 실제 종가(x)
            buy_qty = min(
                math.floor(current_chunk / tb + 1e-9),
                math.floor(cash / tb + 1e-9),
            )
            if buy_qty > 0:
                total_inv             = avg_cost * shares + x * buy_qty
                shares               += buy_qty
                avg_cost              = total_inv / shares
                cash                 -= buy_qty * x
                cycle_total_invested += buy_qty * x
                open_tiers.append({"date": date, "price": x, "qty": buy_qty})
                cycle_buys.append({"date": date, "price": x, "qty": buy_qty})
                if cycle_start_date is None:
                    cycle_start_date = date

        prev_asset = cash + shares * x

    return events


# ═══════════════════════════════════════════════════════════════
# 실전 원장(매매기록) 기반 주문 계산 · 예정 주문 정산
# ═══════════════════════════════════════════════════════════════
# 원칙 (매매법 룰 그대로 — 새 룰 아님):
#   1. 오늘 주문 수량은 "전일까지의 실제 상태(원장)" + 엔진 룰로 계산.
#      1회매수금 = 전일 총자산 ÷ 분할수 · 매수 = min(1회매수금÷tb, 현금÷tb)
#      매도 = 보유 × sell_ratio.  (백테스트 엔진 for-loop과 동일 산식)
#   2. 발송된 주문은 "예정(B{매수}/S{매도})" 행으로 원장에 저장 (체결 전 상태).
#   3. 다음날 그날의 실제 종가로 체결/미체결만 확정(정산). 수량 재계산 절대 없음.
#      → 미래 종가가 과거 주문 수량을 바꾸는 일이 구조적으로 불가능.
import re as _re

AVG_HIST_COLS = ["날짜", "종가(x)", "전날(p1)", "전전날(p2)",
                 "매수경계가", "매도경계가", "매매", "거래주수",
                 "거래금액($)", "실현손익($)", "실현손익률(%)",
                 "보유주수", "보유기간", "현금($)", "총자산($)"]


def _rec_num(v, default=0.0):
    """원장 셀 값 → float. '-', '', None은 default."""
    try:
        s = str(v).replace(",", "").strip()
        if s in ("", "-", "None", "nan"):
            return default
        return float(s)
    except Exception:
        return default


def build_avg_pending_label(buy_qty: int, sell_qty: int) -> str:
    return f"예정(B{int(buy_qty)}/S{int(sell_qty)})"


def parse_avg_pending_label(s):
    """'예정(B29/S160)' → (29, 160). 예정 행 아니면 None."""
    m = _re.match(r"예정\(B(\d+)/S(\d+)\)", str(s).strip())
    return (int(m.group(1)), int(m.group(2))) if m else None


def replay_avg_cost(rows) -> float:
    """원장 rows(날짜 오름차순)에서 현재 평단가 재구성.
    BUY 행의 체결가(거래금액/거래주수)로 평단 갱신, 전량 매도 시 리셋."""
    avg, H = 0.0, 0
    for r in rows:
        q = int(_rec_num(r.get("거래주수"), 0))
        if q > 0:
            amt = _rec_num(r.get("거래금액($)"), 0)
            px = (amt / q) if amt > 0 else _rec_num(r.get("종가(x)"), 0)
            if px > 0:
                avg = (avg * H + px * q) / (H + q)
            H += q
        elif q < 0:
            H += q
        H_col = _rec_num(r.get("보유주수"), None)
        if H_col is not None:
            H = int(H_col)          # 원장 컬럼이 진실 (사용자 수동 정정 반영)
        if H <= 0:
            avg, H = 0.0, max(H, 0)
    return avg


def settle_avg_pending_rows(rows, close_by_date: dict):
    """예정 행을 그날의 실제 종가로 체결/미체결 정산 (in-place 수정).

    rows: dict 리스트 (원장 전체, 날짜 오름차순). 예정 행 포맷:
          매매="예정(B../S..)", 보유주수/현금($) = 주문 시점(체결 전) 상태.
    close_by_date: {"YYYY-MM-DD": 확정종가}. 없는 날짜는 보류(다음 기회에 정산).
    체결 룰 = 백테스트 엔진과 동일: 매도 우선 (x≥ts → SELL), elif x≤tb → BUY.
    체결가 = 그날 종가 x. 수량은 예정 그대로 (재계산 없음).

    Returns: 변경된 row 인덱스 리스트
    """
    changed = []
    for idx, row in enumerate(rows):
        pq = parse_avg_pending_label(row.get("매매", ""))
        if pq is None:
            continue
        d = str(row.get("날짜", "")).strip()
        x = close_by_date.get(d)
        if x is None:
            continue                      # 종가 미확정 → 보류
        x = float(x)
        bq, sq = pq
        H  = int(_rec_num(row.get("보유주수"), 0))
        C  = _rec_num(row.get("현금($)"), 0)
        tb = _rec_num(row.get("매수경계가"), 0)
        ts = _rec_num(row.get("매도경계가"), 0)
        avg = replay_avg_cost(rows[:idx])

        if H > 0 and sq > 0 and ts > 0 and x >= ts:
            qty = min(sq, H)
            amt = qty * x
            H2, C2 = H - qty, C + amt
            row["매매"] = "SELL"
            row["거래주수"] = -qty
            row["거래금액($)"] = round(amt, 2)
            if avg > 0:
                row["실현손익($)"]   = round((x - avg) * qty, 2)
                row["실현손익률(%)"] = round((x / avg - 1) * 100, 2)
        elif bq > 0 and tb > 0 and x <= tb:
            cost = bq * x                 # LOC 체결가 = 종가 (수량은 tb 기준 확정분)
            H2, C2 = H + bq, C - cost
            row["매매"] = "BUY"
            row["거래주수"] = bq
            row["거래금액($)"] = round(cost, 2)
        else:
            H2, C2 = H, C                 # 미체결 — 상태 불변
            row["매매"] = "미체결"
            row["거래주수"] = 0
            row["거래금액($)"] = 0

        row["종가(x)"]  = round(x, 4)
        row["보유주수"] = H2
        row["현금($)"]  = round(C2, 2)
        row["총자산($)"] = round(C2 + H2 * x, 2)
        changed.append(idx)
    return changed


def calc_avg_record_state(rows):
    """정산된 원장 rows(날짜 오름차순, 오늘 이전분)에서 현재 상태 추출.
    Returns: {"shares","cash","avg_cost","last_date"} 또는 None(원장 비었음)"""
    if not rows:
        return None
    last = rows[-1]
    return {
        "shares":    int(_rec_num(last.get("보유주수"), 0)),
        "cash":      _rec_num(last.get("현금($)"), 0),
        "avg_cost":  replay_avg_cost(rows),
        "last_date": str(last.get("날짜", "")).strip(),
    }


def calc_avg_order_from_state(shares: int, cash: float, p1: float,
                              tb: float, ts: float,
                              divisions: int, sell_ratio: float) -> dict:
    """원장 상태 + 엔진 룰로 오늘 주문 수량 계산 (백테스트 for-loop과 동일 산식).
    prev_asset = 현금 + 보유 × 전일종가 → 1회매수금 = prev_asset ÷ 분할수."""
    prev_asset = cash + shares * p1
    chunk = prev_asset / divisions if divisions > 0 else prev_asset
    buy_qty = 0
    if tb > 0:
        buy_qty = min(math.floor(chunk / tb + 1e-9),
                      math.floor(max(cash, 0.0) / tb + 1e-9))
        buy_qty = max(0, buy_qty)
    sell_qty = math.floor(shares * (sell_ratio / 100.0)) if shares > 0 else 0
    return {"prev_asset": prev_asset, "chunk": chunk,
            "buy_qty": int(buy_qty), "sell_qty": int(sell_qty)}


def build_avg_pending_row(date_str: str, p1: float, p2: float,
                          tb: float, ts: float,
                          buy_qty: int, sell_qty: int,
                          shares: int, cash: float) -> dict:
    """오늘 발송 주문의 '예정' 원장 행 (체결 전 상태 저장)."""
    return {
        "날짜": date_str,
        "종가(x)": "",                       # 오늘 종가 미확정 — 정산 시 기입
        "전날(p1)": round(p1, 4),
        "전전날(p2)": round(p2, 4),
        "매수경계가": round(tb, 4),
        "매도경계가": round(ts, 4),
        "매매": build_avg_pending_label(buy_qty, sell_qty),
        "거래주수": "",
        "거래금액($)": "",
        "실현손익($)": "",
        "실현손익률(%)": "",
        "보유주수": int(shares),              # 체결 전 보유
        "보유기간": "-",
        "현금($)": round(cash, 2),            # 체결 전 현금
        "총자산($)": round(cash + shares * p1, 2),
    }
