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
