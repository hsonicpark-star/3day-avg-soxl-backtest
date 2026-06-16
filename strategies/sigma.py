"""
Sigma 매매법 전략 모듈
=====================
app.py 에서 추출한 Sigma매매 전용 코드:
  - 엔진 함수 (백테스트, 주문표 계산)
  - 사이드바 파라미터 렌더링
  - 탭 1~5 UI 렌더링
"""
from __future__ import annotations

import streamlit as st
import pandas as pd
import numpy as np
import math
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import json
import requests
import yfinance as yf

from common.data import next_trading_date
from common.config import (
    _IS_CLOUD, _CONFIG, load_config, save_config, _load_full_config,
    get_ticker_settings, save_ticker_setting, delete_ticker_setting,
    _get_gspread_client, _parse_ticker_settings_json,
    _get_ticker_history_file, _load_ticker_daily_history, _save_ticker_daily_history,
)

# ══════════════════════════════════════════════════════════════
# Sigma 계좌 접두사
# ══════════════════════════════════════════════════════════════
_SIGMA_PFX = "sigma_"


# ══════════════════════════════════════════════════════════════
# Sigma 계좌 설정 CRUD  (common.config 통합 함수 래퍼)
# ══════════════════════════════════════════════════════════════
def _get_sigma_ticker_settings() -> dict:
    return get_ticker_settings(prefix=_SIGMA_PFX, settings_key="sigma_ticker_settings")


def _save_sigma_ticker_setting(tk: str, data: dict) -> str:
    return save_ticker_setting(tk, data, prefix=_SIGMA_PFX, settings_key="sigma_ticker_settings")


def _delete_sigma_ticker_setting(tk: str) -> str:
    return delete_ticker_setting(tk, prefix=_SIGMA_PFX, settings_key="sigma_ticker_settings",
                                 history_prefix="sigma_")


# ══════════════════════════════════════════════════════════════
# 가격 다운로드 유틸 — common.data 통합 함수에 위임
# (intraday 필터 + SOXL 백업시트 보충 + 캐시 자동 적용)
# 이전: 자체 복제본 사용 → intraday/백업 누락으로 6/9 NaN 사건 시
#       전날 데이터로 계산되는 문제 발생
# ══════════════════════════════════════════════════════════════
def _download_price(ticker: str, start_str: str, end_str: str) -> pd.DataFrame:
    from common.data import _download_price as _common_dl
    return _common_dl(ticker, start_str, end_str)


# ══════════════════════════════════════════════════════════════
# Sigma 매매법 핵심 엔진 함수
# ══════════════════════════════════════════════════════════════

def _calc_sigma_params(closes: np.ndarray, end_idx: int, period_days: int):
    """전날까지의 period_days 기간 mu, sigma, rolling_max 계산."""
    start_idx = max(0, end_idx - period_days + 1)
    window = closes[start_idx:end_idx + 1]
    if len(window) < max(10, period_days // 4):
        return None, None, None
    returns = np.diff(window) / window[:-1]
    if len(returns) < 2:
        return None, None, None
    mu = float(np.mean(returns))
    sigma = float(np.std(returns, ddof=0))
    rolling_max = float(np.max(window))
    return mu, sigma, rolling_max


def run_sigma_backtest(
    price_df,
    start_date,
    end_date,
    sigma_period_days: int = 252,
    initial_capital: float = 100000.0,
    amount_per_trade: float = 5000.0,
    sell_pct_levels: list = None,
    sell_fixed_levels: list = None,
    profit_sell_small: list = None,
    profit_sell_large: list = None,
    profit_sell_small_pct: float = 10.0,
    profit_sell_large_pct: float = 50.0,
    trailing_activate_pct: float = None,
    trailing_stop_pct: float = 20.0,
    auto_inject: bool = True,
    dynamic_amount: bool = False,
):
    df = price_df.loc[:pd.to_datetime(end_date)].copy()
    closes_all = df["Close"].values.astype(float)
    dates_all  = df.index
    n = len(closes_all)

    sim_start_idx = df.index.searchsorted(pd.to_datetime(start_date))
    if sim_start_idx >= n or sim_start_idx == 0:
        return None

    cash            = float(initial_capital)
    shares          = 0
    avg_cost        = 0.0
    daily_log       = []
    total_injected  = 0.0
    triggered_profits: set = set()
    peak_profit: float = 0.0
    trailing_active: bool = False
    n_splits = max(1, round(initial_capital / amount_per_trade)) if dynamic_amount else 0

    for i in range(sim_start_idx, n):
        today_close = closes_all[i]
        date_str    = dates_all[i].strftime("%Y-%m-%d")

        mu, sigma, rolling_max = _calc_sigma_params(closes_all, i - 1, sigma_period_days)

        if mu is None:
            daily_log.append({
                "날짜": date_str, "종가": round(today_close, 4),
                "μ(%)": None, "σ(%)": None,
                "1σ매수가": None, "2σ매수가": None, "3σ매수가": None,
                "1yr최고가": None,
                "매도LOC1": None, "매도LOC2": None, "매도LOC3": None,
                "매수": "-", "매수수량": 0, "σ트리거": "",
                "매도": "-", "매도수량": 0, "매도트리거": "",
                "보유수량": shares, "평단가": round(avg_cost, 4),
                "예수금": round(cash, 2), "총자산": round(cash + shares * today_close, 2),
            })
            continue

        prev_close = closes_all[i - 1]
        buy_loc_1 = prev_close * (1 + mu - 1 * sigma)
        buy_loc_2 = prev_close * (1 + mu - 2 * sigma)
        buy_loc_3 = prev_close * (1 + mu - 3 * sigma)

        if sell_fixed_levels:
            sell_checks = sorted([(p, pct) for p, pct in sell_fixed_levels if p > 0 and pct > 0])
        elif sell_pct_levels:
            sell_checks = sorted([(rolling_max * (1 + off / 100), pct) for off, pct in sell_pct_levels if pct > 0])
        elif profit_sell_small or profit_sell_large:
            sell_checks = []
        elif trailing_activate_pct is not None:
            sell_checks = []
        else:
            sell_checks = sorted([(rolling_max * 0.93, 10.0), (rolling_max * 0.95, 30.0), (rolling_max * 0.97, 60.0)])

        sell_locs_log = [p for p, _ in sell_checks] + [None] * 3
        sloc1, sloc2, sloc3 = sell_locs_log[0], sell_locs_log[1], sell_locs_log[2]

        buy_qty_total   = 0
        sigma_triggered = []
        sell_qty_total  = 0
        sell_triggered  = []
        shares_before_buy = shares

        if dynamic_amount:
            prev_total = cash + shares * closes_all[i - 1] if i > sim_start_idx else initial_capital
            cur_trade_amount = max(amount_per_trade, prev_total / n_splits)
        else:
            cur_trade_amount = amount_per_trade

        day_injected = 0.0
        if today_close <= buy_loc_1:
            if auto_inject and cash < cur_trade_amount:
                day_injected    = cur_trade_amount
                cash           += cur_trade_amount
                total_injected += cur_trade_amount
            if cash >= today_close:
                qty = math.floor(min(cur_trade_amount, cash) / today_close)
                if qty > 0:
                    avg_cost = (avg_cost * shares + today_close * qty) / (shares + qty)
                    shares += qty
                    cash   -= qty * today_close
                    buy_qty_total += qty
                    if today_close <= buy_loc_3:   sigma_triggered.append(3)
                    elif today_close <= buy_loc_2: sigma_triggered.append(2)
                    else:                          sigma_triggered.append(1)

        if shares_before_buy > 0 and sell_checks:
            shares_available = shares_before_buy
            for sell_loc, sell_pct in sell_checks:
                if today_close >= sell_loc and sell_pct > 0 and shares_available > 0:
                    qty = math.floor(shares_available * sell_pct / 100.0)
                    if qty > 0:
                        cash   += qty * today_close
                        shares -= qty
                        shares_available -= qty
                        sell_qty_total += qty
                        sell_triggered.append(f"${sell_loc:.2f}")

        if shares_before_buy > 0 and avg_cost > 0 and (profit_sell_small or profit_sell_large):
            cur_profit = (today_close / avg_cost - 1) * 100
            _large_fired = False
            if profit_sell_large:
                _new_large = [t for t in sorted(profit_sell_large) if cur_profit >= t and t not in triggered_profits]
                if _new_large:
                    for t in _new_large: triggered_profits.add(t)
                    _t_fire = max(_new_large)
                    qty = math.floor(shares_before_buy * profit_sell_large_pct / 100.0)
                    if qty > 0 and shares > 0:
                        qty = min(qty, shares)
                        cash += qty * today_close
                        shares -= qty
                        sell_qty_total += qty
                        sell_triggered.append(f"+{_t_fire:.0f}%(대)")
                    _large_fired = True
            if profit_sell_small and not _large_fired:
                _new_small = [t for t in sorted(profit_sell_small) if cur_profit >= t and t not in triggered_profits]
                if _new_small:
                    for t in _new_small: triggered_profits.add(t)
                    _t_fire = max(_new_small)
                    qty = math.floor(shares_before_buy * profit_sell_small_pct / 100.0)
                    if qty > 0 and shares > 0:
                        qty = min(qty, shares)
                        cash += qty * today_close
                        shares -= qty
                        sell_qty_total += qty
                        sell_triggered.append(f"+{_t_fire:.0f}%(소)")

        if shares_before_buy > 0 and avg_cost > 0 and trailing_activate_pct is not None:
            cur_profit_ts = (today_close / avg_cost - 1) * 100
            if cur_profit_ts >= trailing_activate_pct:
                trailing_active = True
            if trailing_active:
                peak_profit = max(peak_profit, cur_profit_ts)
                drop = peak_profit - cur_profit_ts
                if drop >= trailing_stop_pct:
                    qty = min(shares_before_buy, shares)
                    if qty > 0:
                        cash += qty * today_close
                        shares -= qty
                        sell_qty_total += qty
                        sell_triggered.append(f"TS +{peak_profit:.0f}%->+{cur_profit_ts:.0f}%")
                    trailing_active = False
                    peak_profit = 0.0

        if shares == 0:
            avg_cost = 0.0
            triggered_profits = set()
            trailing_active = False
            peak_profit = 0.0

        total_assets = cash + shares * today_close
        daily_log.append({
            "날짜": date_str, "종가": round(today_close, 4),
            "μ(%)": round(mu * 100, 4), "σ(%)": round(sigma * 100, 4),
            "1σ매수가": round(buy_loc_1, 4), "2σ매수가": round(buy_loc_2, 4), "3σ매수가": round(buy_loc_3, 4),
            "1yr최고가": round(rolling_max, 4),
            "매도LOC1": round(sloc1, 4) if sloc1 else None,
            "매도LOC2": round(sloc2, 4) if sloc2 else None,
            "매도LOC3": round(sloc3, 4) if sloc3 else None,
            "매수": "BUY" if buy_qty_total > 0 else "-", "매수수량": buy_qty_total,
            "σ트리거": str(max(sigma_triggered)) if sigma_triggered else "",
            "추가투입": round(day_injected, 2) if day_injected > 0 else 0,
            "매도": "SELL" if sell_qty_total > 0 else "-", "매도수량": sell_qty_total,
            "매도트리거": sell_triggered[-1] if sell_triggered else "",
            "보유수량": shares, "평단가": round(avg_cost, 4) if avg_cost > 0 else 0,
            "예수금": round(cash, 2), "총자산": round(total_assets, 2),
        })

    if not daily_log:
        return None

    hist_df = pd.DataFrame(daily_log)
    assets   = hist_df["총자산"].values.astype(float)
    peak     = np.maximum.accumulate(assets)
    drawdown = np.where(peak > 0, (assets - peak) / peak * 100, 0.0)
    mdd      = float(drawdown.min())
    days     = max(1, (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days)
    years    = days / 365.25
    final    = float(hist_df["총자산"].iloc[-1])
    cagr     = ((final / initial_capital) ** (1 / years) - 1) * 100 if years > 0 and initial_capital > 0 else 0.0
    total_invested = initial_capital + total_injected
    return {
        "history": hist_df, "total_return": (final / total_invested - 1) * 100,
        "cagr": cagr, "mdd": mdd,
        "buy_count": int((hist_df["매수"] == "BUY").sum()),
        "sell_count": int((hist_df["매도"] == "SELL").sum()),
        "final_assets": final, "shares": shares, "cash": cash,
        "avg_cost": avg_cost, "total_injected": total_injected, "total_invested": total_invested,
    }


@st.cache_data(show_spinner=False)
def run_sigma_ordersheet(ticker: str, sigma_period_days: int = 252, _as_of: str = ""):
    today    = datetime.today().date()
    buf_days = sigma_period_days + 90
    start    = today - timedelta(days=int(buf_days * 1.5))
    df = _download_price(ticker, str(start), str(today))
    if df.empty or len(df) < sigma_period_days + 2:
        return None
    closes     = df["Close"].values.astype(float)
    prev_close = closes[-1]
    mu, sigma, rolling_max = _calc_sigma_params(closes, len(closes) - 1, sigma_period_days)
    if mu is None:
        return None
    buy_loc_1 = prev_close * (1 + mu - 1 * sigma)
    buy_loc_2 = prev_close * (1 + mu - 2 * sigma)
    buy_loc_3 = prev_close * (1 + mu - 3 * sigma)
    return {
        "ticker": ticker, "prev_close": prev_close,
        "mu_pct": mu * 100, "sigma_pct": sigma * 100,
        "rolling_max": rolling_max,
        "buy_loc_1": buy_loc_1, "buy_loc_2": buy_loc_2, "buy_loc_3": buy_loc_3,
        "sigma_period": sigma_period_days,
        "as_of": df.index[-1].strftime("%Y-%m-%d"),
        # 전일 종가 미확보 정책용 (yfinance+백업 실패 시 주문표 차단)
        "data_stale": bool(df.attrs.get("data_stale", False)),
        "data_warning": df.attrs.get("data_warning"),
    }


# ══════════════════════════════════════════════════════════════
# Sigma 헬퍼 함수
# ══════════════════════════════════════════════════════════════
def _range_to_list(mn, mx, step):
    if step <= 0 or mn > mx:
        return []
    vals, v = [], mn
    while v <= mx + step * 0.001:
        vals.append(round(v, 4))
        v += step
    return sorted(set(vals))


def _render_sigma_account_tab(tk: str, cfg: dict, key_sfx: str):
    """종목별 계좌 탭 렌더링 (종가평균매매 패턴 적용)"""
    sigma_period     = int(cfg.get("sigma_period", 252))
    os_capital       = float(cfg.get("os_capital", 100000.0))
    divisions        = int(cfg.get("divisions", 20))
    amount_per_trade = float(cfg.get("amount_per_trade", os_capital / max(divisions, 1)))
    os_start         = cfg.get("os_start", "-")

    # -- 삭제 버튼 --
    _del_col, _ = st.columns([1, 5])
    if _del_col.button(f"🗑️ {tk} 삭제", key=f"del_{key_sfx}"):
        _err = _delete_sigma_ticker_setting(tk)
        if _err:
            st.error(_err)
        else:
            st.success(f"{tk} 계좌가 삭제되었습니다.")
            st.rerun()

    # -- 파라미터 요약 + 수정 expander --
    with st.container(border=True):
        _pm1, _pm2, _pm3, _pm4 = st.columns(4)
        _pm1.metric("σ 계산 기간",  f"{sigma_period}일")
        _pm2.metric("분할수",        str(divisions))
        _pm3.metric("1회 매수금",    f"${amount_per_trade:,.0f}")
        _pm4.metric("시작일",        os_start)

        with st.expander("✏️ 파라미터 수정"):
            with st.form(f"edit_form_{key_sfx}"):
                _ef1, _ef2 = st.columns(2)
                try:
                    _start_val = pd.to_datetime(os_start).date()
                except Exception:
                    _start_val = datetime.today().date()
                new_start   = _ef1.date_input("운영 시작일", value=_start_val,
                                               key=f"edit_start_{key_sfx}")
                new_capital = _ef2.number_input("초기 투자금 ($)", value=os_capital,
                                                 step=1000.0, key=f"edit_cap_{key_sfx}")
                _ef3, _ef4 = st.columns(2)
                _sp_opts = [63, 126, 252, 504]
                _sp_idx  = _sp_opts.index(sigma_period) if sigma_period in _sp_opts else 2
                new_sigma_period = _ef3.selectbox(
                    "σ 계산 기간", _sp_opts, index=_sp_idx,
                    format_func=lambda x: {63:"3개월(63일)",126:"6개월(126일)",
                                            252:"1년(252일)",504:"2년(504일)"}[x],
                    key=f"edit_sp_{key_sfx}",
                )
                new_divisions = _ef4.number_input("분할수", value=divisions, min_value=1,
                                                   step=1, key=f"edit_div_{key_sfx}")
                new_amount = st.number_input(
                    "1회 매수금 ($)  (0 = 투자금/분할수 자동)",
                    value=amount_per_trade, step=100.0, key=f"edit_amt_{key_sfx}",
                )
                if st.form_submit_button("💾 저장", use_container_width=True):
                    _auto = new_capital / new_divisions if new_divisions > 0 else new_capital
                    _save_amt = new_amount if new_amount > 0 else _auto
                    _err = _save_sigma_ticker_setting(tk, {
                        "os_start":          str(new_start),
                        "os_capital":        new_capital,
                        "sigma_period":      new_sigma_period,
                        "divisions":         int(new_divisions),
                        "amount_per_trade":  _save_amt,
                    })
                    if _err:
                        st.error(f"저장 실패: {_err}")
                    else:
                        st.success("파라미터가 저장되었습니다.")
                        st.rerun()

    # -- 자본 조정 expander --
    with st.expander("💰 자본 조정"):
        with st.form(f"cap_form_{key_sfx}"):
            _ca1, _ca2 = st.columns(2)
            _adj_reason = _ca1.text_input("조정 사유 (메모)", key=f"adj_reason_{key_sfx}")
            _adj_amount = _ca2.number_input("조정 금액 ($)  (+입금 / -출금)",
                                             value=0.0, step=1000.0, key=f"adj_amt_{key_sfx}")
            if st.form_submit_button("💾 자본 조정 저장", use_container_width=True):
                _new_cap = os_capital + _adj_amount
                _err = _save_sigma_ticker_setting(tk, {"os_capital": _new_cap})
                if _err:
                    st.error(f"저장 실패: {_err}")
                else:
                    st.success(f"자본금이 ${_new_cap:,.0f}로 조정되었습니다.")
                    st.rerun()

    st.markdown("---")

    # -- 현재 보유 현황 입력 --
    st.markdown("##### 💼 현재 보유 현황")
    _hc1, _hc2, _hc3 = st.columns(3)
    _t_shares   = _hc1.number_input("보유 주수",    value=0,    min_value=0, step=1,
                                     key=f"sh_{key_sfx}")
    _t_avg_cost = _hc2.number_input("평균 단가 ($)", value=0.0, step=0.01, format="%.4f",
                                     key=f"avg_{key_sfx}")
    _t_cash     = _hc3.number_input("예수금 ($)",   value=0.0, step=100.0,
                                     key=f"cash_{key_sfx}")

    # -- 주문표 로드 버튼 --
    if st.button("📋 주문표 로드", type="primary", key=f"load_order_{key_sfx}",
                  use_container_width=True):
        with st.spinner(f"{tk} 데이터 로드 중..."):
            _od = run_sigma_ordersheet(tk, sigma_period, _as_of=str(datetime.today().date()))
        if _od is None:
            st.error("데이터 로드 실패 또는 σ 계산 데이터가 부족합니다.")
        elif _od.get("data_stale"):
            # 전일 종가 미확보(yfinance+백업 실패) → 주문표 생성 차단
            st.error(f"{_od.get('data_warning') or '⛔ 전일 종가 미확보'}\n\n"
                     f"최신 전일 종가를 확보하지 못해 **주문표를 생성하지 않습니다.** "
                     f"잠시 후 다시 시도해주세요.")
        else:
            if _od.get("data_warning"):
                st.warning(f"{_od['data_warning']}\n\n주문표는 백업 데이터를 반영하여 계산되었습니다.")
            st.session_state[f"order_data_{key_sfx}"]   = _od
            st.session_state[f"order_shares_{key_sfx}"] = _t_shares
            st.session_state[f"order_avg_{key_sfx}"]    = _t_avg_cost
            st.session_state[f"order_cash_{key_sfx}"]   = _t_cash
            st.session_state[f"order_amt_{key_sfx}"]    = amount_per_trade

    # -- 주문 결과 표시 --
    if f"order_data_{key_sfx}" in st.session_state:
        _od  = st.session_state[f"order_data_{key_sfx}"]
        _sh  = st.session_state.get(f"order_shares_{key_sfx}", 0)
        _av  = st.session_state.get(f"order_avg_{key_sfx}", 0.0)
        _ca  = st.session_state.get(f"order_cash_{key_sfx}", 0.0)
        _am  = st.session_state.get(f"order_amt_{key_sfx}", amount_per_trade)

        with st.container(border=True):
            st.markdown(
                f"<b>📊 오늘의 Sigma LOC 주문</b> · {_od['ticker']} · 주문일: {next_trading_date().strftime('%Y-%m-%d')} (데이터 기준: {_od['as_of']})<br>"
                f"전일 종가: <b>&#36;{_od['prev_close']:.4f}</b> &nbsp;|&nbsp; "
                f"μ: {_od['mu_pct']:.4f}% &nbsp;|&nbsp; "
                f"σ: {_od['sigma_pct']:.4f}% &nbsp;|&nbsp; "
                f"1yr 고점: &#36;{_od['rolling_max']:.4f}",
                unsafe_allow_html=True,
            )
            _qty1 = math.floor(_am / _od["buy_loc_1"]) if _od["buy_loc_1"] > 0 else 0
            _qty2 = math.floor(_am / _od["buy_loc_2"]) if _od["buy_loc_2"] > 0 else 0
            _qty3 = math.floor(_am / _od["buy_loc_3"]) if _od["buy_loc_3"] > 0 else 0
            _buy_df = pd.DataFrame([
                {"레벨": "1σ 매수 ⭐", "LOC 가격": f"${_od['buy_loc_1']:.4f}",
                 "예상 수량": _qty1, "예상 금액": f"${_qty1 * _od['buy_loc_1']:,.2f}",
                 "비고": "LOC 주문 대상"},
                {"레벨": "2σ (참고)",   "LOC 가격": f"${_od['buy_loc_2']:.4f}",
                 "예상 수량": _qty2, "예상 금액": f"${_qty2 * _od['buy_loc_2']:,.2f}",
                 "비고": "2σ 이탈 시 가격대"},
                {"레벨": "3σ (참고)",   "LOC 가격": f"${_od['buy_loc_3']:.4f}",
                 "예상 수량": _qty3, "예상 금액": f"${_qty3 * _od['buy_loc_3']:,.2f}",
                 "비고": "3σ 이탈 시 가격대"},
            ])
            st.dataframe(_buy_df, hide_index=True, use_container_width=True)
            st.caption("⭐ 실제 LOC 주문은 **1σ** 기준으로만 냅니다. 2σ·3σ는 참고용입니다.")
            st.info("💡 매도 전략 및 목표 가격은 **📈 매도 전략 가이드** 탭을 참고하세요.")

        # 보유 현황 메트릭
        if _sh > 0 or _ca > 0:
            _stock_val   = _sh * _od["prev_close"]
            _total_val   = _stock_val + _ca
            _pnl_pct     = (_av > 0 and (_od["prev_close"] / _av - 1) * 100) or 0.0
            _pnl_val     = (_av > 0 and (_sh * (_od["prev_close"] - _av))) or 0.0
            _stk_weight  = _stock_val / _total_val * 100 if _total_val > 0 else 0.0
            _hh1, _hh2, _hh3, _hh4 = st.columns(4)
            _hh1.metric("보유 주수",   f"{_sh:,}주")
            _hh2.metric("평균 단가",   f"${_av:.4f}")
            _hh3.metric("평가손익",    f"${_pnl_val:+,.2f}", f"{_pnl_pct:+.2f}%")
            _hh4.metric("주식 비중",   f"{_stk_weight:.1f}%")
            _hh5, _hh6, _hh7 = st.columns(3)
            _hh5.metric("평가금액",    f"${_stock_val:,.2f}")
            _hh6.metric("예수금",      f"${_ca:,.2f}")
            _hh7.metric("총 평가금액", f"${_total_val:,.2f}")


# ══════════════════════════════════════════════════════════════
# 사이드바 렌더링
# ══════════════════════════════════════════════════════════════
def render_sidebar() -> dict:
    """Sigma 매매법 사이드바 파라미터를 렌더링하고 dict 로 반환."""
    def _sfloat_sg(v, d):
        try: return float(v) if v not in ("", None) else d
        except: return d
    def _sint_sg(v, d):
        try: return int(float(v)) if v not in ("", None) else d
        except: return d

    bt_ticker       = st.text_input("종목코드", "SOXL", key="sg_bt_ticker").strip().upper()
    bt_start_date   = st.date_input("시작일", datetime(2015, 1, 1).date(), key="sg_bt_start")
    bt_end_date     = st.date_input("종료일", datetime.today().date(), key="sg_bt_end")
    bt_sigma_period = st.selectbox(
        "σ 계산 기간",
        [63, 126, 252, 504], index=2,
        format_func=lambda x: {63:"3개월(63일)",126:"6개월(126일)",252:"1년(252일)",504:"2년(504일)"}[x],
        key="sg_sigma_period",
    )
    bt_initial_capital  = st.number_input("초기 투자금 ($)", value=100000.0, step=1000.0, key="sg_capital")
    bt_divisions        = st.number_input("분할수", value=20, min_value=1, step=1, key="sg_divisions")
    bt_amount_per_trade = bt_initial_capital / bt_divisions if bt_divisions > 0 else bt_initial_capital
    bt_dynamic_amount   = st.toggle("동적 매수금 (총자산 비례)", value=False, key="sg_dynamic")
    bt_auto_inject      = st.toggle("예수금 부족 시 자동 투입", value=True, key="sg_auto_inject")
    st.markdown("---")
    st.markdown("**매도 모드**")
    _sell_mode = st.radio("매도 기준", [
        "🎯 트레일링 스탑",
        "💹 평단가 기준 익절",
        "📊 1년 고점 기준 %",
        "💰 직접 가격 입력",
    ], key="sg_sell_mode")
    bt_trailing_activate = None
    bt_trailing_stop     = 20.0
    bt_profit_small      = None
    bt_profit_large      = None
    bt_profit_small_pct  = 10.0
    bt_profit_large_pct  = 40.0
    bt_sell_pct_levels   = None
    bt_sell_fixed_levels = None
    if _sell_mode == "🎯 트레일링 스탑":
        bt_trailing_activate = st.number_input("TS 활성화 기준 (%)", value=100.0, step=10.0, key="sg_ts_act")
        bt_trailing_stop     = st.number_input("TS 하락폭 (%)", value=20.0, step=5.0, key="sg_ts_drop")
    elif _sell_mode == "💹 평단가 기준 익절":
        bt_trailing_activate = st.number_input("TS 활성화 기준 (%)", value=100.0, step=10.0, key="sg_ts_act2")
        bt_trailing_stop     = st.number_input("TS 하락폭 (%)", value=20.0, step=5.0, key="sg_ts_drop2")
        st.caption("소익절")
        bt_profit_small_pct = st.number_input("소익절 매도 비율 (%)", value=10.0, step=5.0, key="sg_sm_pct")
        bt_profit_small     = [
            st.number_input("소익절 레벨1 (%)", value=10.0, step=5.0, key="sg_sm1"),
            st.number_input("소익절 레벨2 (%)", value=25.0, step=5.0, key="sg_sm2"),
            st.number_input("소익절 레벨3 (%)", value=50.0, step=5.0, key="sg_sm3"),
        ]
        st.caption("대익절")
        bt_profit_large_pct = st.number_input("대익절 매도 비율 (%)", value=40.0, step=5.0, key="sg_lg_pct")
        bt_profit_large     = [
            st.number_input("대익절 레벨1 (%)", value=100.0, step=25.0, key="sg_lg1"),
            st.number_input("대익절 레벨2 (%)", value=150.0, step=25.0, key="sg_lg2"),
            st.number_input("대익절 레벨3 (%)", value=200.0, step=25.0, key="sg_lg3"),
        ]
    elif _sell_mode == "📊 1년 고점 기준 %":
        bt_sell_pct_levels = [
            (st.number_input("레벨1 고점대비 (%)", value=-3.0, step=1.0, format="%.0f", key="sg_pct1"),
             st.number_input("레벨1 매도비율 (%)", value=60.0, step=10.0, key="sg_pct1r")),
            (st.number_input("레벨2 고점대비 (%)", value=-5.0, step=1.0, format="%.0f", key="sg_pct2"),
             st.number_input("레벨2 매도비율 (%)", value=30.0, step=10.0, key="sg_pct2r")),
            (st.number_input("레벨3 고점대비 (%)", value=-7.0, step=1.0, format="%.0f", key="sg_pct3"),
             st.number_input("레벨3 매도비율 (%)", value=10.0, step=10.0, key="sg_pct3r")),
        ]
    elif _sell_mode == "💰 직접 가격 입력":
        bt_sell_fixed_levels = [
            (st.number_input("매도가1 ($)", value=0.0, step=1.0, key="sg_fix1"),
             st.number_input("매도비율1 (%)", value=50.0, step=10.0, key="sg_fix1r")),
            (st.number_input("매도가2 ($)", value=0.0, step=1.0, key="sg_fix2"),
             st.number_input("매도비율2 (%)", value=50.0, step=10.0, key="sg_fix2r")),
        ]
    _run_bt = st.button("▶ 백테스트 실행", type="primary", key="sg_run_bt")

    return {
        "bt_ticker": bt_ticker,
        "bt_start_date": bt_start_date,
        "bt_end_date": bt_end_date,
        "bt_sigma_period": bt_sigma_period,
        "bt_initial_capital": bt_initial_capital,
        "bt_divisions": bt_divisions,
        "bt_amount_per_trade": bt_amount_per_trade,
        "bt_dynamic_amount": bt_dynamic_amount,
        "bt_auto_inject": bt_auto_inject,
        "sell_mode": _sell_mode,
        "bt_trailing_activate": bt_trailing_activate,
        "bt_trailing_stop": bt_trailing_stop,
        "bt_profit_small": bt_profit_small,
        "bt_profit_large": bt_profit_large,
        "bt_profit_small_pct": bt_profit_small_pct,
        "bt_profit_large_pct": bt_profit_large_pct,
        "bt_sell_pct_levels": bt_sell_pct_levels,
        "bt_sell_fixed_levels": bt_sell_fixed_levels,
        "_run_bt": _run_bt,
    }


# ══════════════════════════════════════════════════════════════
# TAB 1 -- 백테스트
# ══════════════════════════════════════════════════════════════
def render_backtest_tab(params: dict):
    """Sigma 매매법 백테스트 탭 렌더링."""
    st.subheader("📊 Sigma 매매법 백테스트")

    bt_ticker          = params["bt_ticker"]
    bt_start_date      = params["bt_start_date"]
    bt_end_date        = params["bt_end_date"]
    bt_sigma_period    = params["bt_sigma_period"]
    bt_initial_capital = params["bt_initial_capital"]
    bt_amount_per_trade = params["bt_amount_per_trade"]
    bt_auto_inject     = params["bt_auto_inject"]
    bt_dynamic_amount  = params["bt_dynamic_amount"]
    _sell_mode         = params["sell_mode"]
    bt_trailing_activate = params["bt_trailing_activate"]
    bt_trailing_stop   = params["bt_trailing_stop"]
    bt_profit_small    = params["bt_profit_small"]
    bt_profit_large    = params["bt_profit_large"]
    bt_profit_small_pct = params["bt_profit_small_pct"]
    bt_profit_large_pct = params["bt_profit_large_pct"]
    bt_sell_pct_levels = params["bt_sell_pct_levels"]
    bt_sell_fixed_levels = params["bt_sell_fixed_levels"]
    _run_bt            = params["_run_bt"]

    if _run_bt:
        if not bt_ticker:
            st.warning("종목코드를 입력해주세요.")
        else:
            with st.spinner(f"{bt_ticker} 데이터 로드 중..."):
                buf_start = bt_start_date - timedelta(days=int(bt_sigma_period * 1.5 + 60))
                price_df_bt = _download_price(bt_ticker, str(buf_start), str(bt_end_date))

            if price_df_bt is None or price_df_bt.empty:
                st.error("데이터 로드 실패. 종목코드와 날짜를 확인해주세요.")
            else:
                with st.spinner("시뮬레이션 실행 중..."):
                    _is_profit_mode   = _sell_mode == "💹 평단가 기준 익절"
                    _is_trailing_mode = _sell_mode == "🎯 트레일링 스탑"
                    res = run_sigma_backtest(
                        price_df          = price_df_bt,
                        start_date        = bt_start_date,
                        end_date          = bt_end_date,
                        sigma_period_days = bt_sigma_period,
                        initial_capital   = bt_initial_capital,
                        amount_per_trade  = bt_amount_per_trade,
                        sell_pct_levels      = bt_sell_pct_levels if _sell_mode == "📊 1년 고점 기준 %" else None,
                        sell_fixed_levels    = bt_sell_fixed_levels if _sell_mode == "💰 직접 가격 입력" else None,
                        profit_sell_small    = bt_profit_small if (_is_profit_mode or _is_trailing_mode) else None,
                        profit_sell_large    = bt_profit_large if _is_profit_mode else None,
                        profit_sell_small_pct= bt_profit_small_pct,
                        profit_sell_large_pct= bt_profit_large_pct,
                        trailing_activate_pct= bt_trailing_activate if _is_trailing_mode else None,
                        trailing_stop_pct    = bt_trailing_stop if _is_trailing_mode else 20.0,
                        auto_inject          = bt_auto_inject,
                        dynamic_amount       = bt_dynamic_amount,
                    )
                if res is None:
                    st.error("시뮬레이션 실패. 데이터가 부족하거나 날짜 범위를 확인해주세요.")
                else:
                    st.session_state["sigma_bt_res"]     = res
                    st.session_state["sigma_bt_ticker"]  = bt_ticker
                    st.session_state["sigma_bt_capital"] = bt_initial_capital

    if "sigma_bt_res" in st.session_state:
        res     = st.session_state["sigma_bt_res"]
        tk_lbl  = st.session_state.get("sigma_bt_ticker", "")
        cap_lbl = st.session_state.get("sigma_bt_capital", bt_initial_capital)

        st.markdown("---")
        st.subheader("📈 성과 요약")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("최종 자산",    f"${res['final_assets']:,.2f}")
        m2.metric("총 수익률",    f"{res['total_return']:+.2f}%")
        m3.metric("CAGR",         f"{res['cagr']:+.2f}%")
        m4.metric("MDD",          f"{res['mdd']:.2f}%")

        m5, m6, m7, m8 = st.columns(4)
        m5.metric("매수 횟수",    f"{res['buy_count']:,}회")
        m6.metric("매도 횟수",    f"{res['sell_count']:,}회")
        m7.metric("보유 주수",    f"{res['shares']:,}주")
        _avg_c = res['avg_cost']
        m8.metric("평균 단가",    f"${_avg_c:.4f}" if _avg_c > 0 else "-")

        if res["total_injected"] > 0:
            mi1, mi2, mi3, _ = st.columns(4)
            mi1.metric("초기 자본",       f"${bt_initial_capital:,.0f}")
            mi2.metric("추가 투입 합계",  f"${res['total_injected']:,.0f}")
            mi3.metric("총 투입 원금",    f"${res['total_invested']:,.0f}")

        hist_df_raw = res["history"].dropna(subset=["σ(%)"])
        if not hist_df_raw.empty:
            _mu_mean    = hist_df_raw["μ(%)"].mean()
            _sigma_mean = hist_df_raw["σ(%)"].mean()
            _sigma_last = hist_df_raw["σ(%)"].iloc[-1]
            _sigma_min  = hist_df_raw["σ(%)"].min()
            _sigma_max  = hist_df_raw["σ(%)"].max()
            st.markdown("---")
            st.subheader("📐 σ (표준편차) 통계")
            s1, s2, s3, s4, s5 = st.columns(5)
            s1.metric("최근 σ",   f"{_sigma_last:.2f}%")
            s2.metric("평균 σ",   f"{_sigma_mean:.2f}%")
            s3.metric("최소 σ",   f"{_sigma_min:.2f}%")
            s4.metric("최대 σ",   f"{_sigma_max:.2f}%")
            s5.metric("평균 μ",   f"{_mu_mean:+.4f}%")

        st.subheader("📈 자산 곡선")
        hist_df = res["history"].copy()
        hist_df["날짜"] = pd.to_datetime(hist_df["날짜"])

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=hist_df["날짜"], y=hist_df["총자산"],
            name=f"{tk_lbl} Sigma매매",
            line=dict(color="#1976D2", width=2),
        ))
        buy_pts  = hist_df[hist_df["매수"] == "BUY"]
        sell_pts = hist_df[hist_df["매도"] == "SELL"]
        if not buy_pts.empty:
            fig.add_trace(go.Scatter(
                x=buy_pts["날짜"], y=buy_pts["총자산"],
                mode="markers", name="매수",
                marker=dict(color="red", size=6, symbol="triangle-up"),
            ))
        if not sell_pts.empty:
            fig.add_trace(go.Scatter(
                x=sell_pts["날짜"], y=sell_pts["총자산"],
                mode="markers", name="매도",
                marker=dict(color="green", size=6, symbol="triangle-down"),
            ))
        fig.update_layout(
            xaxis_title="날짜", yaxis_title="총자산 ($)",
            hovermode="x unified", height=450,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            template="plotly_white",
        )
        st.plotly_chart(fig, use_container_width=True)

        _price_fmt = {
            "종가":      "${:,.4f}",
            "1σ매수가":  "${:,.4f}",
            "2σ매수가":  "${:,.4f}",
            "3σ매수가":  "${:,.4f}",
            "1yr최고가": "${:,.4f}",
            "평단가":    "${:,.4f}",
            "예수금":    "${:,.2f}",
            "총자산":    "${:,.2f}",
            "추가투입":  lambda v: f"${v:,.0f}" if v and float(v) > 0 else "-",
            "μ(%)":      "{:.4f}%",
            "σ(%)":      "{:.4f}%",
        }

        def _row_color_sg(row):
            if row["매수"] == "BUY":
                return ["background-color: #fff3f3"] * len(row)
            if row["매도"] == "SELL":
                return ["background-color: #f3fff3"] * len(row)
            return [""] * len(row)

        with st.expander("📋 일별 매매 상세 내역 (전체)", expanded=False):
            disp_df = hist_df.copy()
            disp_df["날짜"] = disp_df["날짜"].dt.strftime("%Y-%m-%d")
            styled = (disp_df.style
                      .apply(_row_color_sg, axis=1)
                      .format(_price_fmt, na_rep="-"))
            st.dataframe(styled, use_container_width=True, height=500)
            csv_bytes = disp_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button("💾 CSV 다운로드", csv_bytes,
                               f"sigma_backtest_{tk_lbl}.csv", "text/csv", key="sg_bt_csv_dl")

        with st.expander(f"🔴 매수 발생일만 보기 ({res['buy_count']:,}건)", expanded=False):
            buy_only = hist_df[hist_df["매수"] == "BUY"].copy()
            buy_only["날짜"] = buy_only["날짜"].dt.strftime("%Y-%m-%d")
            buy_cols = ["날짜", "종가", "σ(%)", "μ(%)", "σ트리거",
                        "1σ매수가", "2σ매수가", "3σ매수가",
                        "매수수량", "보유수량", "평단가", "예수금", "총자산"]
            buy_cols = [c for c in buy_cols if c in buy_only.columns]
            if buy_only.empty:
                st.info("백테스트 기간 내 매수 발생 없음")
            else:
                st.dataframe(
                    buy_only[buy_cols].style
                        .apply(lambda r: ["background-color: #fff3f3"] * len(r), axis=1)
                        .format(_price_fmt, na_rep="-"),
                    use_container_width=True, height=400,
                )
                buy_csv = buy_only[buy_cols].to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                st.download_button("💾 매수 내역 CSV", buy_csv,
                                   f"sigma_buy_{tk_lbl}.csv", "text/csv", key="sg_bt_buy_csv")

        with st.expander("📅 연도별 성과", expanded=False):
            ydf = hist_df.copy()
            ydf["연도"] = ydf["날짜"].dt.year
            annual_rows = []
            prev_ast    = float(cap_lbl)
            for yr in sorted(ydf["연도"].unique()):
                end_ast  = float(ydf[ydf["연도"] == yr]["총자산"].iloc[-1])
                ret      = (end_ast / prev_ast - 1) * 100 if prev_ast > 0 else 0.0
                annual_rows.append({"연도": yr, "연말자산($)": f"${end_ast:,.0f}", "연간수익률(%)": ret})
                prev_ast = end_ast
            annual_df = pd.DataFrame(annual_rows)
            def _color_ret_sg(val):
                try:
                    v = float(val)
                    if v > 0: return "color: #1565C0; font-weight:bold"
                    if v < 0: return "color: #c62828; font-weight:bold"
                except: pass
                return ""
            st.dataframe(
                annual_df.style.map(_color_ret_sg, subset=["연간수익률(%)"]).format({"연간수익률(%)": "{:+.2f}%"}),
                hide_index=True, use_container_width=True,
            )


# ══════════════════════════════════════════════════════════════
# TAB 2 -- 매도 전략 가이드
# ══════════════════════════════════════════════════════════════
def render_optimization_tab(params: dict):
    """Sigma 매도 전략 가이드 탭 렌더링."""
    st.subheader("📈 매도 전략 가이드")
    st.caption("보유 포지션을 기준으로 매도 참고 가격을 보여줍니다. 이런 가격대가 오면 매도를 고민해보세요!")

    st.markdown("#### 📌 현재 포지션")
    _sg1, _sg2, _sg3 = st.columns(3)
    sg_avg    = _sg1.number_input("평균 단가 ($)", value=0.0, step=0.01,
                                   format="%.4f", key="sg_avg_cost")
    sg_qty    = _sg2.number_input("보유 수량 (주)", value=0, min_value=0,
                                   step=1, key="sg_qty")
    sg_ticker_guide = _sg3.text_input("종목코드 (현재가 조회)",
                                 value=st.session_state.get("sg_bt_ticker", "SOXL"),
                                 key="sg_ticker_guide").strip().upper()

    _sg_cur = None
    if sg_ticker_guide:
        try:
            _sg_raw = yf.download(sg_ticker_guide, period="3d", progress=False, auto_adjust=True)
            if not _sg_raw.empty:
                if isinstance(_sg_raw.columns, pd.MultiIndex):
                    _sg_raw.columns = _sg_raw.columns.get_level_values(0)
                _sg_cur = float(_sg_raw["Close"].iloc[-1])
        except Exception:
            pass

    if sg_avg > 0:
        _sg_profit_pct = (_sg_cur / sg_avg - 1) * 100 if _sg_cur else None
        _m1, _m2, _m3, _m4 = st.columns(4)
        _m1.metric("평균 단가",  f"${sg_avg:,.4f}")
        _m2.metric("보유 수량",  f"{sg_qty:,}주")
        _m3.metric("현재가",
                   f"${_sg_cur:,.4f}" if _sg_cur else "조회 중",
                   delta=f"{_sg_profit_pct:+.2f}%" if _sg_profit_pct is not None else None)
        _m4.metric("평가 손익",
                   f"${(_sg_cur - sg_avg) * sg_qty:+,.0f}" if (_sg_cur and sg_qty) else "-",
                   delta=f"{_sg_profit_pct:+.2f}%" if _sg_profit_pct is not None else None)

        st.markdown("---")
        sg_ts_drop = st.number_input(
            "🛑 트레일링 스탑 하락폭 (%)",
            value=10.0, step=1.0, min_value=1.0, max_value=50.0, key="sg_ts_drop_guide",
            help="활성화 기준 돌파 후, 최고점 대비 이 비율만큼 하락 시 전량 매도 고려",
        )

        def _sg_status(tgt_price):
            if _sg_cur is None:
                return "미확인"
            if _sg_cur >= tgt_price:
                return "달성"
            gap = tgt_price - _sg_cur
            return f"+${gap:,.2f} 남음"

        st.markdown("---")
        st.markdown("#### 소익절 가격대")
        st.caption("목표 수익률 도달 시 보유량의 일부를 매도해 수익을 부분 실현하는 기준가입니다.")
        _small_rows = []
        for _pct in [10, 25, 50]:
            _tgt = sg_avg * (1 + _pct / 100)
            _qty_ref = math.floor(sg_qty * 0.15) if sg_qty > 0 else 0
            _small_rows.append({
                "수익률 기준": f"+{_pct}%",
                "목표 가격 ($)": f"${_tgt:,.4f}",
                "매도 주수 참고 (보유량의 15%)": f"{_qty_ref}주",
                "현재 상태": _sg_status(_tgt),
            })
        st.dataframe(pd.DataFrame(_small_rows), hide_index=True, use_container_width=True)

        st.markdown("#### 대익절 가격대")
        st.caption("큰 수익 구간에서 비중을 대폭 줄이는 기준가입니다.")
        _large_rows = []
        for _pct in [100, 150, 200]:
            _tgt = sg_avg * (1 + _pct / 100)
            _qty_ref = math.floor(sg_qty * 0.40) if sg_qty > 0 else 0
            _large_rows.append({
                "수익률 기준": f"+{_pct}%",
                "목표 가격 ($)": f"${_tgt:,.4f}",
                "매도 주수 참고 (보유량의 40%)": f"{_qty_ref}주",
                "현재 상태": _sg_status(_tgt),
            })
        st.dataframe(pd.DataFrame(_large_rows), hide_index=True, use_container_width=True)

        st.markdown(f"#### 트레일링 스탑 기준가  *(활성화 후 -{sg_ts_drop:.0f}% 하락 시 전량 매도 고려)*")
        st.caption(f"아래 활성화 가격 돌파 이후, 최고점 대비 -{sg_ts_drop:.0f}% 내려오면 전량 매도를 고민해보세요.")
        _ts_rows = []
        for _act_pct in [100, 150, 200]:
            _act_price     = sg_avg * (1 + _act_pct / 100)
            _trigger_price = _act_price * (1 - sg_ts_drop / 100)
            _is_activated  = _sg_cur is not None and _sg_cur >= _act_price
            _ts_rows.append({
                "활성화 기준": f"+{_act_pct}%",
                "활성화 가격 ($)": f"${_act_price:,.4f}",
                f"매도 트리거가 (-{sg_ts_drop:.0f}%) ($)": f"${_trigger_price:,.4f}",
                "활성화 여부": "활성화됨" if _is_activated else _sg_status(_act_price),
            })
        st.dataframe(pd.DataFrame(_ts_rows), hide_index=True, use_container_width=True)

        st.info("""
💡 **매도 전략 활용 가이드**
- **소익절** : 목표가 도달 시 보유량의 **10~20%** 부분 매도
- **대익절** : 큰 수익 구간에서 보유량의 **30~50%** 비중 축소
- **트레일링** : 활성화 가격 돌파 후 최고점 대비 하락 시 **전량 매도** 고려
        """)
    else:
        st.info("👆 평균 단가를 입력하면 매도 가격 현황판이 표시됩니다.")


# ══════════════════════════════════════════════════════════════
# TAB 3 -- 주문표 & 계좌관리
# ══════════════════════════════════════════════════════════════
def render_ordersheet_tab(params: dict):
    """Sigma 매매법 주문표 & 계좌관리 탭 렌더링."""
    st.subheader("📋 주문표 & 계좌관리")
    st.caption("종목별 운영 계좌를 관리하고 오늘의 Sigma LOC 주문을 확인합니다.")

    with st.expander("➕ 계좌 추가"):
        with st.form("add_sigma_account"):
            _af1, _af2 = st.columns(2)
            _form_ticker  = _af1.text_input("종목코드", "SOXL",
                                             key="add_sigma_ticker").strip().upper()
            _form_start   = _af2.date_input("운영 시작일", datetime.today().date(),
                                             key="add_sigma_start")
            _af3, _af4 = st.columns(2)
            _form_capital  = _af3.number_input("초기 투자금 ($)", value=100000.0,
                                                step=1000.0, key="add_sigma_capital")
            _form_sigma_pd = _af4.selectbox(
                "σ 계산 기간", [63, 126, 252, 504], index=2,
                format_func=lambda x: {63:"3개월(63일)",126:"6개월(126일)",
                                        252:"1년(252일)",504:"2년(504일)"}[x],
                key="add_sigma_pd",
            )
            _af5, _af6 = st.columns(2)
            _form_divisions = _af5.number_input("분할수", value=20, min_value=1,
                                                 step=1, key="add_sigma_div")
            _form_amount    = _af6.number_input(
                "1회 매수금 ($)  (0 = 자동계산)", value=0.0, step=100.0,
                key="add_sigma_amt", help="0 입력 시 초기투자금 / 분할수로 자동계산",
            )
            if st.form_submit_button("💾 계좌 추가", type="primary", use_container_width=True):
                if not _form_ticker:
                    st.error("종목코드를 입력해주세요.")
                else:
                    _auto_amt = _form_capital / _form_divisions if _form_divisions > 0 else _form_capital
                    _save_amt = _form_amount if _form_amount > 0 else _auto_amt
                    _err = _save_sigma_ticker_setting(_form_ticker, {
                        "os_start":          str(_form_start),
                        "os_capital":        _form_capital,
                        "sigma_period":      _form_sigma_pd,
                        "divisions":         int(_form_divisions),
                        "amount_per_trade":  _save_amt,
                    })
                    if _err:
                        st.error(f"저장 실패: {_err}")
                    else:
                        st.success(f"{_form_ticker} 계좌가 추가되었습니다.")
                        st.rerun()

    _sigma_settings = _get_sigma_ticker_settings()
    if not _sigma_settings:
        st.info("등록된 계좌가 없습니다. 위의 **➕ 계좌 추가** 를 눌러 계좌를 등록하세요.")
    else:
        _acct_tickers = list(_sigma_settings.keys())
        if len(_acct_tickers) == 1:
            _render_sigma_account_tab(
                _acct_tickers[0], _sigma_settings[_acct_tickers[0]], _acct_tickers[0]
            )
        else:
            _acct_tabs = st.tabs([f"📌 {_tk}" for _tk in _acct_tickers])
            for _a_tab, _tk in zip(_acct_tabs, _acct_tickers):
                with _a_tab:
                    _render_sigma_account_tab(_tk, _sigma_settings[_tk], _tk)

    st.markdown("---")
    st.markdown("#### 📅 최근 1년 매수 신호 내역")
    _sig_settings  = _get_sigma_ticker_settings()
    _default_tk_sg = list(_sig_settings.keys())[0] if _sig_settings else "SOXL"
    _default_sp_sg = int(_sig_settings[_default_tk_sg].get("sigma_period", 252)) if _sig_settings else 252
    _sc1, _sc2 = st.columns(2)
    _sig_tk = _sc1.text_input("종목코드", value=_default_tk_sg, key="sig_hist_ticker").strip().upper()
    _sig_sp = _sc2.selectbox(
        "σ 계산 기간",
        [63, 126, 252, 504],
        index=[63,126,252,504].index(_default_sp_sg) if _default_sp_sg in [63,126,252,504] else 2,
        format_func=lambda x: {63:"3개월(63일)",126:"6개월(126일)",252:"1년(252일)",504:"2년(504일)"}[x],
        key="sig_hist_period",
    )
    st.caption(f"{_sig_tk} · σ 기간 {_sig_sp}일 · 오늘 기준 252 거래일")
    with st.spinner("신호 계산 중..."):
        _sig_today = datetime.today().date()
        _sig_buf   = _sig_today - timedelta(days=int(_sig_sp * 1.5 + 400))
        _sig_df    = _download_price(_sig_tk, str(_sig_buf), str(_sig_today + timedelta(days=2)))
    if _sig_df is not None and not _sig_df.empty:
        _sig_closes = _sig_df["Close"].values.astype(float)
        _sig_dates  = _sig_df.index
        _sig_n      = len(_sig_closes)
        _cur_mu, _cur_sg, _cur_rm = _calc_sigma_params(_sig_closes, _sig_n - 1, _sig_sp)
        if _cur_mu is not None:
            _prev_close = _sig_closes[-1]
            _buy1 = _prev_close * (1 + _cur_mu - 1 * _cur_sg)
            _buy2 = _prev_close * (1 + _cur_mu - 2 * _cur_sg)
            _buy3 = _prev_close * (1 + _cur_mu - 3 * _cur_sg)
            _sm1, _sm2, _sm3, _sm4, _sm5 = st.columns(5)
            _sm1.metric("μ (일평균 수익률)",  f"{_cur_mu*100:+.4f}%")
            _sm2.metric("σ (일간 표준편차)",  f"{_cur_sg*100:.4f}%")
            _sm3.metric("연환산 σ",           f"{_cur_sg*100*math.sqrt(252):.2f}%")
            _sm4.metric("1yr 고점",           f"${_cur_rm:,.2f}")
            _sm5.metric("전일 종가",           f"${_prev_close:,.2f}")
            with st.container(border=True):
                st.caption("📐 오늘의 매수 LOC 참고가 (전일 종가 기준)")
                _bc1, _bc2, _bc3 = st.columns(3)
                _bc1.metric("1σ 매수가 ⭐", f"${_buy1:,.4f}", f"{(_buy1/_prev_close-1)*100:+.2f}%")
                _bc2.metric("2σ 매수가 (참고)", f"${_buy2:,.4f}", f"{(_buy2/_prev_close-1)*100:+.2f}%")
                _bc3.metric("3σ 매수가 (참고)", f"${_buy3:,.4f}", f"{(_buy3/_prev_close-1)*100:+.2f}%")
        _one_yr_ago = _sig_today - timedelta(days=380)
        _sig_start_idx  = max(_sig_sp + 2, int(np.searchsorted(_sig_df.index, pd.Timestamp(_one_yr_ago))))
        _sig_rows = []
        for _si in range(_sig_start_idx, _sig_n):
            _mu_s, _sg_s, _rm_s = _calc_sigma_params(_sig_closes, _si - 1, _sig_sp)
            if _mu_s is None:
                continue
            _pc_s = _sig_closes[_si - 1]
            _cl_s = _sig_closes[_si]
            _bl1  = _pc_s * (1 + _mu_s - 1 * _sg_s)
            _bl2  = _pc_s * (1 + _mu_s - 2 * _sg_s)
            _bl3  = _pc_s * (1 + _mu_s - 3 * _sg_s)
            _lvl  = (3 if _cl_s <= _bl3 else 2 if _cl_s <= _bl2 else 1 if _cl_s <= _bl1 else 0)
            _chg_pct = (_cl_s / _pc_s - 1) * 100 if _pc_s > 0 else 0.0
            _sig_rows.append({
                "날짜":     _sig_dates[_si],
                "종가":     round(_cl_s, 2),
                "전일대비": round(_chg_pct, 2),
                "σ레벨":    _lvl,
                "1σ매수가": round(_bl1, 4),
                "2σ매수가": round(_bl2, 4),
                "3σ매수가": round(_bl3, 4),
            })
        _sig_all = pd.DataFrame(_sig_rows)
        _sig_buy = _sig_all[_sig_all["σ레벨"] > 0].copy() if not _sig_all.empty else pd.DataFrame()
        _fig_sig = go.Figure()
        if not _sig_all.empty:
            _fig_sig.add_trace(go.Scatter(
                x=_sig_all["날짜"], y=_sig_all["종가"],
                mode="lines", name="종가",
                line=dict(color="#4A90D9", width=1.5),
            ))
            _lvl_style = {
                1: ("1σ 매수", "#F5C518", "triangle-up", 10),
                2: ("2σ 매수", "#E07B00", "triangle-up", 13),
                3: ("3σ 매수", "#C0392B", "triangle-up", 16),
            }
            for _lv, (_lbl, _col, _sym, _sz) in _lvl_style.items():
                _lv_df = _sig_buy[_sig_buy["σ레벨"] == _lv] if not _sig_buy.empty else pd.DataFrame()
                if not _lv_df.empty:
                    _fig_sig.add_trace(go.Scatter(
                        x=_lv_df["날짜"], y=_lv_df["종가"],
                        mode="markers", name=_lbl,
                        marker=dict(color=_col, symbol=_sym, size=_sz,
                                    line=dict(color="white", width=1)),
                    ))
        _fig_sig.update_layout(
            height=360, margin=dict(l=40, r=20, t=30, b=40),
            plot_bgcolor="white", paper_bgcolor="white",
            xaxis=dict(title="날짜", showgrid=True, gridcolor="#f0f0f0"),
            yaxis=dict(title="가격 ($)", showgrid=True, gridcolor="#f0f0f0"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            hovermode="x unified",
        )
        st.plotly_chart(_fig_sig, use_container_width=True)
        if not _sig_buy.empty:
            st.caption(
                f"총 **{len(_sig_buy)}회** 매수 신호 "
                f"(1σ: {((_sig_buy['σ레벨']==1).sum())}회 · "
                f"2σ: {((_sig_buy['σ레벨']==2).sum())}회 · "
                f"3σ: {((_sig_buy['σ레벨']==3).sum())}회)"
            )
            _disp_buy = _sig_buy.copy()
            _disp_buy["날짜"]     = _disp_buy["날짜"].dt.strftime("%Y-%m-%d")
            _disp_buy["종가"]     = _disp_buy.apply(
                lambda r: f"${r['종가']:,.2f}  ({r['전일대비']:+.2f}%)", axis=1
            )
            _disp_buy["σ트리거"]  = _disp_buy["σ레벨"].apply(lambda x: f"{x}σ")
            _disp_buy["1σ매수가"] = _disp_buy["1σ매수가"].apply(lambda x: f"${x:,.4f}")
            _disp_buy["2σ매수가"] = _disp_buy["2σ매수가"].apply(lambda x: f"${x:,.4f}")
            _disp_buy["3σ매수가"] = _disp_buy["3σ매수가"].apply(lambda x: f"${x:,.4f}")
            st.dataframe(
                _disp_buy[["날짜", "종가", "σ트리거", "1σ매수가", "2σ매수가", "3σ매수가"]].iloc[::-1],
                hide_index=True, use_container_width=True,
            )
        else:
            st.info("최근 1년간 매수 신호가 없었습니다.")
    else:
        st.warning("가격 데이터를 불러올 수 없습니다.")


# ══════════════════════════════════════════════════════════════
# TAB 4 -- 전략 소개
# ══════════════════════════════════════════════════════════════
def render_intro_tab():
    """Sigma 매매법 전략 소개 탭 렌더링."""
    st.title("📖 Sigma 매매법 전략 소개")
    st.caption("표준편차(σ) 기반 과매도 역추세 매수 전략")

    st.markdown("---")
    st.subheader("🎯 전략 개요")
    st.markdown("""
**Sigma 매매법**은 주가가 평균 대비 얼마나 이탈했는지를 **표준편차(σ)** 로 측정해
과매도 구간을 판단하고 분할 매수하는 **역추세 전략**입니다.

> 볼린저밴드가 `이동평균 ± 2σ` 로 변동 범위를 시각화하는 것처럼,
> Sigma 매매법은 그 '이탈 원리'를 실제 매매 신호로 활용합니다.
""")

    col_l, col_r = st.columns(2)
    with col_l:
        st.info("""
**📌 핵심 아이디어**

주가의 일간 수익률은 장기적으로 정규분포에 가깝게 분포합니다.

- 68.26% 확률: μ ± 1σ 이내에서 움직임
- 95.44% 확률: μ ± 2σ 이내에서 움직임
- 99.74% 확률: μ ± 3σ 이내에서 움직임

→ 1σ 이탈은 약 16% 확률의 이례적 하락
→ 2σ 이탈은 약 2.3% 확률의 극단적 하락
→ 3σ 이탈은 약 0.13% 확률의 희귀 급락
""")
    with col_r:
        st.success("""
**📌 전략의 강점**

- **자동 적응**: σ는 시장 변동성에 따라 자동으로 조정
  → 조용한 장: 좁은 기준, 출렁이는 장: 넓은 기준
- **분할 매수**: 1σ / 2σ / 3σ 각 단계별 독립 진입
  → 하락이 깊을수록 더 적극적으로 매수
- **통계적 근거**: 정규분포 이론 기반의 명확한 진입 기준
""")

    st.markdown("---")
    st.subheader("🔴 매수 규칙")
    st.markdown("""
#### 1. σ와 μ 계산 (매일 업데이트)
전날까지 **N일(기본 1년=252 거래일)** 간의 일간 수익률 데이터로 계산합니다.

```
일간 수익률 r(t) = (종가(t) - 종가(t-1)) / 종가(t-1)

μ (뮤)  = 평균(r(1), r(2), ..., r(N))   ← 평균 일간 수익률
σ (시그마) = 표준편차(r(1), ..., r(N))  ← 일간 수익률의 변동성
```

#### 2. LOC 매수 기준가 계산
```
1σ 매수 LOC = 전일 종가 × (1 + μ - 1×σ)
2σ 매수 LOC = 전일 종가 × (1 + μ - 2×σ)
3σ 매수 LOC = 전일 종가 × (1 + μ - 3×σ)
```

#### 3. 체결 조건
당일 종가가 매수 LOC **이하**이면 체결 (LOC = Limit On Close)
→ 체결가는 당일 종가 (한도가 아닌 종가에 체결)

**실제 LOC 주문은 1σ 기준 하나만** 냅니다.
(LOC는 하루에 한 번 종가에 체결되므로 여러 가격에 동시 주문 불가)
""")

    # -- mu(뮤) 설명 --
    st.markdown("---")
    st.subheader("💡 왜 μ(뮤)를 기준선으로 쓰는가?")
    col_a, col_b = st.columns(2)
    with col_a:
        st.error("""
**❌ μ를 쓰지 않으면**

```
1σ 매수가 = 전일종가 × (1 - σ)
```

→ 일간 수익률 평균이 **0%** 라고 가정하는 것

SOXL처럼 장기 우상향하는 종목은
평균 일간 수익률이 **+0.22%** 수준
→ 아무 일 없이도 매일 평균 0.22% 오르는데
  0% 기준으로 이탈을 측정하면 **기준이 틀림**
""")
    with col_b:
        st.success("""
**✅ μ를 기준선으로 쓰면**

```
1σ 매수가 = 전일종가 × (1 + μ - σ)
```

→ "평균적인 일상 등락을 기준으로,
   거기서 1σ 이상 이탈했을 때만 매수"

반 평균 성적이 70점일 때
'이상한 점수'의 기준은 70점이지 0점이 아닌 것과 같은 원리

→ **통계적으로 더 올바른 과매도 판단**
""")
    st.caption("※ μ = +0.22% 는 매우 작아 실제 매수가 차이는 미미하지만, 개념적으로 더 정확한 접근입니다.")

    # -- 매도 규칙 --
    st.markdown("---")
    st.subheader("🔵 매도 전략 가이드")
    st.info("""
💡 **Sigma 매매법에서 매도는 정해진 정답이 없습니다.**
매수 기준(σ 이탈)은 통계적으로 명확하지만, 매도는 개인의 목표 수익률·리스크 허용 범위에 따라 달라집니다.
아래 3가지 방식 중 자신에게 맞는 방식을 선택하거나 조합해서 활용하세요.
""")

    st.markdown("#### 1️⃣ 소익절 — 평단가 대비 단계별 부분 매도")
    st.markdown("""
평균 단가 대비 수익률이 일정 수준에 도달하면 **소량씩 익절**하는 방식입니다.
보유 수량을 조금씩 줄이며 리스크를 낮추고, 나머지는 계속 보유합니다.
""")
    _sell_tbl1 = pd.DataFrame([
        {"수익률 기준": "+10%",  "의미": "본전 부근 소량 정리",   "참고 매도 비율": "보유분의 약 15%"},
        {"수익률 기준": "+25%",  "의미": "중간 이익 실현",        "참고 매도 비율": "보유분의 약 15%"},
        {"수익률 기준": "+50%",  "의미": "절반 수준 이익 구간",   "참고 매도 비율": "보유분의 약 15%"},
    ])
    st.dataframe(_sell_tbl1, hide_index=True, use_container_width=True)
    st.caption("※ 비율은 참고용입니다. 본인의 목표 수익에 맞게 조정하세요.")

    st.markdown("#### 2️⃣ 대익절 — 큰 수익 구간에서 의미 있는 비중 축소")
    st.markdown("""
평균 단가 대비 **100% 이상 수익** 구간에서 보유 비중을 크게 줄이는 방식입니다.
레버리지 ETF(SOXL, TQQQ 등)처럼 큰 변동성을 가진 종목에 특히 적합합니다.
""")
    _sell_tbl2 = pd.DataFrame([
        {"수익률 기준": "+100%", "의미": "2배 도달 — 원금 회수 가능 구간", "참고 매도 비율": "보유분의 약 40%"},
        {"수익률 기준": "+150%", "의미": "2.5배 — 추가 수익 구간",         "참고 매도 비율": "보유분의 약 40%"},
        {"수익률 기준": "+200%", "의미": "3배 — 대규모 차익 실현",         "참고 매도 비율": "보유분의 약 40%"},
    ])
    st.dataframe(_sell_tbl2, hide_index=True, use_container_width=True)
    st.caption("※ 40%씩 3번 매도하면 보유분의 약 78%가 정리됩니다. 잔여분은 장기 보유 또는 TS로 처리합니다.")

    st.markdown("#### 3️⃣ 트레일링 스탑 (TS) — 고점 대비 하락 시 전량 청산")
    st.markdown("""
목표 수익률에 도달한 이후, 고점에서 일정 비율 이상 하락하면 **전량 매도**하는 방식입니다.
수익을 최대한 지키면서 추세 전환 시 손실을 제한하는 데 효과적입니다.
""")
    col_ts1, col_ts2 = st.columns(2)
    with col_ts1:
        st.success("""
**작동 방식**

1. 평단가 대비 **+100%** (기본) 이상 수익 달성 → TS 활성화
2. 이후 매일 보유 기간 중 고점 수익률 추적
3. 고점 대비 수익률이 **-10%** (기본) 이상 하락하면 전량 매도

```
예시)
  평단가 $30, 현재 수익 +120% → TS 활성화
  이후 고점 수익 +140% 기록
  수익률이 +130%로 하락 (-10%p)
  → 전량 LOC 매도 실행
```
""")
    with col_ts2:
        st.warning("""
**TS 파라미터 조정 기준**

| 설정 | 효과 |
|------|------|
| TS 활성 기준 낮춤 | 일찍 보호 시작 (수익 적게 남김) |
| TS 활성 기준 높임 | 더 오를 때까지 자유롭게 보유 |
| TS 하락폭 좁힘 | 빠른 청산 (고점 근처에서 탈출) |
| TS 하락폭 넓힘 | 일시적 조정 허용 (더 오래 보유) |

**백테스트 권장값:** 활성 100% / 하락폭 10~20%
""")

    st.markdown("#### 🗂️ 매도 전략 비교")
    _sg_strategy_cmp = pd.DataFrame([
        {"전략": "소익절",       "특징": "소량씩 단계별 이익 실현",        "장점": "심리적 안정, 리스크 분산",   "단점": "큰 상승 시 일찍 털어냄",       "적합 상황": "단기~중기 보유 / 변동성 큰 종목"},
        {"전략": "대익절",       "특징": "2배 이상 수익 구간에서 큰 비중 정리", "장점": "원금 회수 + 수익 확정", "단점": "추가 상승 기회 일부 포기",     "적합 상황": "레버리지 ETF / 장기 보유 후 목표 달성 시"},
        {"전략": "트레일링 스탑", "특징": "고점 대비 하락폭으로 청산",       "장점": "추세 끝까지 수익 극대화",    "단점": "일시적 급락에 조기 청산 가능", "적합 상황": "강한 상승 추세 / 목표가 불명확할 때"},
    ])
    st.dataframe(_sg_strategy_cmp, hide_index=True, use_container_width=True)

    st.markdown("""
> **💡 실전 조합 예시**
> 평단가 +10%, +25%에서 소익절(15%씩) → +100% 도달 시 트레일링 스탑 활성화
> → 고점 대비 -15% 하락하면 잔여 전량 청산
""")

    # -- 파라미터 정리 --
    st.markdown("---")
    st.subheader("⚙️ 파라미터 정리")
    _sg_param_data = {
        "파라미터": ["σ 계산 기간", "초기 투자금", "분할수", "1회 매수금액",
                   "TS 활성화 기준", "TS 하락폭",
                   "소익절 기준가", "대익절 기준가"],
        "기본값": ["1년 (252거래일)", "-", "20", "투자금 / 분할수",
                  "평단가 +100%", "고점 대비 -10~20%",
                  "평단가 +10% / +25% / +50%", "평단가 +100% / +150% / +200%"],
        "설명": [
            "μ, σ, 1년 최고가를 계산하는 lookback 기간. 길수록 안정적, 짧을수록 최근 변동성 반영",
            "백테스트 시작 시점의 현금",
            "자본을 몇 등분할지. 20이면 1회 매수금 = 총자금의 5%",
            "각 σ 레벨 체결 시 투입할 금액 (고정 금액)",
            "이 수익률 달성 시 트레일링 스탑 감시 시작",
            "TS 활성화 이후 고점 대비 이 비율 하락 시 전량 청산",
            "평단가 대비 해당 수익률 도달 시 보유분 일부 익절 (참고 15%)",
            "평단가 대비 해당 수익률 도달 시 보유분 큰 비중 익절 (참고 40%)",
        ],
    }
    st.table(_sg_param_data)

    # -- 정규분포 차트 --
    st.markdown("---")
    st.subheader("📊 정규분포(가우시안)와 σ 매수 구간")

    _x = np.linspace(-4.2, 4.2, 800)
    _y = (1 / np.sqrt(2 * np.pi)) * np.exp(-0.5 * _x ** 2)

    _fig_gauss = go.Figure()
    _fig_gauss.add_trace(go.Scatter(
        x=_x, y=_y, mode="lines",
        line=dict(color="#4A90D9", width=2.5),
        showlegend=False,
    ))

    _shade_regions = [
        (-4.2, -3.0, "rgba(180,0,0,0.25)",    "3σ↓ 이탈 (0.13%)"),
        (-3.0, -2.0, "rgba(220,80,0,0.25)",   "2σ↓ 이탈 (2.28%)"),
        (-2.0, -1.0, "rgba(255,200,0,0.25)",  "1σ↓ 이탈 (15.87%)"),
        (-1.0,  1.0, "rgba(70,180,70,0.20)",  "μ±1σ 이내 (68.27%)"),
        ( 1.0,  4.2, "rgba(120,180,255,0.15)", "μ+1σ 이상 (상승)"),
    ]
    for _x0, _x1, _color, _name in _shade_regions:
        _mask = (_x >= _x0) & (_x <= _x1)
        _fig_gauss.add_trace(go.Scatter(
            x=np.concatenate([[_x0], _x[_mask], [_x1]]),
            y=np.concatenate([[0], _y[_mask], [0]]),
            fill="tozeroy", mode="lines",
            line=dict(width=0), fillcolor=_color,
            name=_name, legendgroup=_name,
        ))

    _buy_lines = [
        (-1.0, "#F5C518", "1σ 매수\n(16% 확률)",   0.13),
        (-2.0, "#E07B00", "2σ 매수\n(2.3% 확률)",  0.06),
        (-3.0, "#C0392B", "3σ 매수\n(0.13% 확률)", 0.02),
    ]
    for _xv, _col, _lbl, _yann in _buy_lines:
        _fig_gauss.add_vline(x=_xv, line_dash="dash", line_color=_col, line_width=1.8)
        _fig_gauss.add_annotation(
            x=_xv, y=_yann + 0.005, text=_lbl, showarrow=False,
            font=dict(size=11, color=_col), align="center",
            bgcolor="rgba(255,255,255,0.7)",
            bordercolor=_col, borderwidth=1, borderpad=3,
        )

    _fig_gauss.add_vline(x=0, line_dash="dot", line_color="#4A90D9", line_width=1.5)
    _fig_gauss.add_annotation(
        x=0, y=0.42, text="μ (평균\n일간 수익률)", showarrow=False,
        font=dict(size=11, color="#4A90D9"),
        bgcolor="rgba(255,255,255,0.8)",
        bordercolor="#4A90D9", borderwidth=1, borderpad=3,
    )
    _fig_gauss.update_layout(
        xaxis=dict(
            tickvals=[-3, -2, -1, 0, 1, 2, 3],
            ticktext=["μ-3σ", "μ-2σ", "μ-1σ", "μ", "μ+1σ", "μ+2σ", "μ+3σ"],
            title="일간 수익률 (표준편차 단위)",
        ),
        yaxis=dict(title="확률 밀도", showgrid=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1, font=dict(size=11)),
        height=380, margin=dict(l=40, r=20, t=40, b=40),
        plot_bgcolor="white", paper_bgcolor="white",
        title=dict(text="정규분포와 Sigma 매수 트리거 구간",
                   font=dict(size=14), x=0.5, xanchor="center"),
    )
    st.plotly_chart(_fig_gauss, use_container_width=True)
    st.caption("""
    🔴 **붉은 구간**: 3σ 이탈 — 약 1,000 거래일(4년)에 1~2번 수준의 극단적 하락
    🟠 **주황 구간**: 2σ 이탈 — 약 44 거래일 중 1번 수준의 강한 하락
    🟡 **노란 구간**: 1σ 이탈 — 약 6.3 거래일 중 1번 수준의 이례적 하락
    🟢 **초록 구간**: μ±1σ 이내 — 정상 등락 범위 (전체의 68.27%)
    """)

    # -- 주의사항 --
    st.markdown("---")
    st.subheader("⚠️ 주의사항")
    st.warning("""
- **과거 성과 ≠ 미래 보장**: 백테스트 결과는 참고용이며 실제 수익을 보장하지 않습니다.
- **σ 계산 기간**: 기간이 짧으면 최근 변동성에 민감, 길면 장기 평균에 수렴합니다.
- **거래 수수료 미반영**: 실제 매매 시 수수료/슬리피지가 발생합니다.
- **극단적 하락 시**: 3σ 이탈이 연속으로 발생하면 3회분 자금이 동시 투입될 수 있습니다.
- **LOC 주문 특성**: 장 마감 직전 지정가 주문으로, 체결은 종가 기준입니다.
""")


# ══════════════════════════════════════════════════════════════
# TAB 5 -- 개인 설정
# ══════════════════════════════════════════════════════════════
def render_settings_tab():
    """Sigma 매매법 개인 설정 탭 렌더링."""
    st.subheader("⚙️ 개인 설정")

    if _IS_CLOUD and not st.session_state.get("logged_in"):
        st.info("로그인 후 설정을 변경할 수 있습니다.")
    else:
        _us_sg = (st.session_state.get("user_settings", {})
               if (_IS_CLOUD and st.session_state.get("logged_in"))
               else _load_full_config())

        if _IS_CLOUD:
            st.info(f"☁️ **{st.session_state.get('username','')}** 으로 로그인 중 — 설정을 저장하면 다음 로그인 시 자동으로 불러옵니다.")
        else:
            st.success(f"🖥️ **로컬 PC 실행 중** — 설정이 `{_CONFIG}` 에 저장됩니다.")

        with st.container(border=True):
            _tg_col1_sg, _tg_col2_sg = st.columns([3, 1])
            with _tg_col1_sg:
                st.markdown("#### 💬 텔레그램 알림 설정")
                st.caption("Sigma 매매법 주문 신호를 텔레그램으로 받을 수 있습니다.")
            with _tg_col2_sg:
                with st.popover("❓ Chat ID & Bot Token 확인 방법", use_container_width=True):
                    st.markdown("""
<style>
.tg-help-section { margin-bottom: 20px; }
.tg-help-title {
    display: flex; align-items: center; gap: 10px;
    font-size: 17px; font-weight: 700; color: #1a1a2e; margin-bottom: 10px;
}
.tg-help-badge {
    background: #4A90D9; color: white;
    border-radius: 50%; width: 28px; height: 28px;
    display: inline-flex; align-items: center; justify-content: center;
    font-size: 14px; font-weight: 700; flex-shrink: 0;
}
.tg-help-box {
    background: #EEF4FB; border-radius: 10px;
    padding: 14px 18px; font-size: 14px; line-height: 2;
}
.tg-help-box ol { margin: 0; padding-left: 20px; }
.tg-help-box li { margin-bottom: 2px; }
.tg-tag {
    background: #D0E8FF; color: #1a5fa8;
    border-radius: 5px; padding: 1px 7px;
    font-family: monospace; font-size: 13px;
}
.tg-code-box {
    background: #1e2533; color: #7dd3fc;
    border-radius: 8px; padding: 10px 14px; margin-top: 8px;
    font-family: monospace; font-size: 12px; word-break: break-all;
    line-height: 1.7;
}
.tg-example-box {
    background: white; border: 1px solid #CBD5E1; border-radius: 8px;
    padding: 12px 16px; margin-top: 10px; font-size: 13px; color: #555;
}
.tg-example-val { color: #4A90D9; font-family: monospace; font-size: 13px; }
.tg-warn-box {
    background: #FFFBEB; border: 1px solid #F59E0B;
    border-radius: 10px; padding: 14px 18px; font-size: 14px; line-height: 2;
}
.tg-warn-title { font-weight: 700; color: #92400E; margin-bottom: 4px; }
.tg-sub-title { font-weight: 700; color: #1a5fa8; margin: 10px 0 4px 0; }
.tg-tip-box {
    background: #F0FDF4; border: 1px solid #86EFAC;
    border-radius: 8px; padding: 10px 14px; margin-top: 8px;
    font-size: 13px; color: #166534;
}
</style>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">1</span> Bot Token 생성하기</div>
  <div class="tg-help-box">
    <ol>
      <li>텔레그램 앱에서 검색창에 <span class="tg-tag">@BotFather</span> 를 검색합니다.</li>
      <li>파란 체크 공식 계정을 선택하고 <span class="tg-tag">/start</span> 를 눌러 대화를 시작합니다.</li>
      <li><span class="tg-tag">/newbot</span> 을 입력합니다.</li>
      <li><strong>봇 표시 이름</strong>을 입력합니다. (예: <span class="tg-tag">Sigma 알림봇</span>) — 한글 가능, 자유롭게 설정</li>
      <li><strong>봇 username</strong>을 입력합니다. — 영문+숫자만 가능, 반드시 <span class="tg-tag">bot</span> 으로 끝나야 함<br>
          &nbsp;&nbsp;예: <span class="tg-tag">sigma_alert_bot</span> &nbsp;/&nbsp; <span class="tg-tag">my_soxl_sigma_bot</span></li>
      <li>성공 시 <strong>HTTP API Token</strong>이 발급됩니다. 이것이 <strong>Bot Token</strong>입니다.</li>
    </ol>
    <div class="tg-example-box">
      <div style="color:#888; font-size:12px; margin-bottom:4px;">Bot Token 예시 (발급 후 복사해서 아래 입력창에 붙여넣기):</div>
      <div class="tg-example-val">1234567890:ABCdefGHIjklMNOpqrSTUvwxYZ</div>
    </div>
  </div>
</div>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">2</span> 내 봇 시작하기 (필수!)</div>
  <div class="tg-warn-box">
    <div class="tg-warn-title">⚠ 봇을 먼저 시작해야 Chat ID를 확인하고 메시지를 받을 수 있습니다!</div>
    <ol>
      <li>텔레그램 검색창에서 내가 만든 봇 username을 검색합니다. (예: <span class="tg-tag">@sigma_alert_bot</span>)</li>
      <li>봇 대화창에서 <span class="tg-tag">/start</span> 를 눌러 봇을 활성화합니다.</li>
      <li>봇에게 아무 메시지나 한 번 보냅니다. (Chat ID 확인을 위해 필요)</li>
    </ol>
  </div>
</div>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">3</span> Chat ID 확인하기</div>
  <div class="tg-help-box">
    <div class="tg-sub-title">✅ 방법 1: getUpdates API 사용 (가장 확실한 방법)</div>
    <ol>
      <li>위 2단계에서 봇에게 메시지를 보낸 후, 아래 주소를 브라우저에 입력합니다.</li>
      <li><span class="tg-tag">{토큰값}</span> 부분을 발급받은 Bot Token으로 교체합니다.</li>
    </ol>
    <div class="tg-code-box">https://api.telegram.org/bot<span style="color:#fde047;">{토큰값}</span>/getUpdates</div>
    <ol start="3">
      <li>JSON 응답에서 <span class="tg-tag">"id"</span> 값을 찾습니다. 이것이 <strong>Chat ID</strong>입니다.</li>
    </ol>
    <div class="tg-example-box">
      <div style="color:#888; font-size:12px; margin-bottom:6px;">응답 예시:</div>
      <div style="font-family:monospace; font-size:12px; color:#333; line-height:1.8;">
        {"ok":true,"result":[{"message":{"chat":{<strong style="color:#e11d48;">"id": 123456789</strong>,"first_name":"홍길동"}}}]}
      </div>
    </div>
    <div class="tg-sub-title">방법 2: @userinfobot 사용 (간편)</div>
    <ol>
      <li>텔레그램에서 <span class="tg-tag">@userinfobot</span> 을 검색합니다.</li>
      <li><span class="tg-tag">/start</span> 를 누르면 자동으로 내 Chat ID가 표시됩니다.</li>
    </ol>
    <div class="tg-sub-title">방법 3: @RawDataBot 사용</div>
    <ol>
      <li>텔레그램에서 <span class="tg-tag">@RawDataBot</span> 을 검색합니다.</li>
      <li>아무 메시지나 보내면 JSON 형식으로 정보가 표시되며, <span class="tg-tag">"id"</span> 값이 Chat ID입니다.</li>
    </ol>
    <div class="tg-example-box">
      <div style="color:#888; font-size:12px; margin-bottom:4px;">Chat ID 예시 (숫자만):</div>
      <div class="tg-example-val">123456789</div>
    </div>
  </div>
</div>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">4</span> 연결 테스트</div>
  <div class="tg-tip-box">
    💡 Bot Token과 Chat ID를 입력한 후 아래 <strong>📨 주문표 테스트 발송</strong> 버튼을 눌러보세요.<br>
    메시지가 정상적으로 수신되면 설정 완료입니다! ✅
  </div>
</div>
""", unsafe_allow_html=True)

            _tg_c1_sg, _tg_c2_sg = st.columns(2)
            _tg_chat_id_sg = _tg_c1_sg.text_input(
                "텔레그램 Chat ID",
                value=str(_us_sg.get("sigma_tg_chat_id", "")),
                placeholder="예: 1234567890",
                key="sigma_tg_chat_id_input",
            )
            _tg_token_sg = _tg_c2_sg.text_input(
                "Bot Token",
                value=str(_us_sg.get("sigma_tg_token", "")),
                placeholder="예: 123456789:AAF...",
                type="password",
                key="sigma_tg_token_input",
            )
            st.caption("📅 주문표는 매주 월~금 오후 3:00 (KST)에 텔레그램으로 자동 발송됩니다")

            _btn_tg1_sg, _btn_tg2_sg, _ = st.columns([1, 1, 4])
            with _btn_tg1_sg:
                if st.button("📨 주문표 테스트 발송", use_container_width=True, key="sigma_tg_test"):
                    if not _tg_chat_id_sg or not _tg_token_sg:
                        st.warning("Chat ID와 Bot Token을 먼저 입력해주세요.")
                    else:
                        _test_settings_sg = _get_sigma_ticker_settings()
                        if not _test_settings_sg:
                            st.warning("등록된 Sigma 계좌가 없습니다. 주문표 탭에서 계좌를 먼저 등록해주세요.")
                        else:
                            for _test_tk_sg, _test_cfg_sg in _test_settings_sg.items():
                                with st.spinner(f"{_test_tk_sg} 주문 계산 & 발송 중..."):
                                    try:
                                        _test_od_sg = run_sigma_ordersheet(
                                            _test_tk_sg,
                                            int(_test_cfg_sg.get("sigma_period", 252)),
                                            _as_of=str(datetime.today().date()),
                                        )
                                        if _test_od_sg is None:
                                            st.error(f"❌ {_test_tk_sg}: 데이터 부족")
                                            continue
                                        _test_capital_sg   = float(_test_cfg_sg.get("os_capital", 100000))
                                        _test_divisions_sg = int(_test_cfg_sg.get("divisions", 20))
                                        _test_amt_sg       = float(_test_cfg_sg.get(
                                            "amount_per_trade", _test_capital_sg / _test_divisions_sg))
                                        _test_qty_sg = (math.floor(_test_amt_sg / _test_od_sg["buy_loc_1"])
                                                     if _test_od_sg["buy_loc_1"] > 0 else 0)
                                        _test_msg_sg = (
                                            f"📐 <b>Sigma매매 주문표</b> ({_test_tk_sg})\n"
                                            f"📅 {datetime.today().strftime('%Y-%m-%d')}  |  기준: {_test_od_sg['as_of']}\n"
                                            f"전일종가: <b>${_test_od_sg['prev_close']:.4f}</b>\n"
                                            f"μ: {_test_od_sg['mu_pct']:+.4f}%  |  σ: {_test_od_sg['sigma_pct']:.4f}%\n"
                                            f"━━━━━━━━━━━━━━━━\n"
                                            f"🔴 <b>매수 LOC (1σ 기준)</b>\n"
                                            f"  <b>${_test_od_sg['buy_loc_1']:.4f}</b>  {_test_qty_sg}주  "
                                            f"(≈${_test_qty_sg * _test_od_sg['buy_loc_1']:,.0f})\n"
                                            f"  2σ: ${_test_od_sg['buy_loc_2']:.4f}  |  3σ: ${_test_od_sg['buy_loc_3']:.4f}\n"
                                            f"ℹ️ σ 계산 기간: {_test_od_sg['sigma_period']}거래일"
                                        )
                                        _resp_sg = requests.post(
                                            f"https://api.telegram.org/bot{_tg_token_sg}/sendMessage",
                                            json={"chat_id": _tg_chat_id_sg, "text": _test_msg_sg,
                                                  "parse_mode": "HTML"},
                                            timeout=10,
                                        )
                                        if _resp_sg.ok:
                                            st.success(f"✅ {_test_tk_sg} 발송 성공!")
                                        else:
                                            st.error(f"❌ {_test_tk_sg} 발송 실패: {_resp_sg.text}")
                                    except Exception as _e_sg:
                                        st.error(f"❌ {_test_tk_sg} 오류: {_e_sg}")

            with _btn_tg2_sg:
                if st.button("💾 저장하기", use_container_width=True, key="sigma_tg_save", type="primary"):
                    if not _tg_chat_id_sg or not _tg_token_sg:
                        st.warning("Chat ID와 Bot Token을 모두 입력해주세요.")
                    else:
                        _tg_save_sg = {"sigma_tg_chat_id": _tg_chat_id_sg, "sigma_tg_token": _tg_token_sg}
                        save_config(_tg_save_sg)
                        if _IS_CLOUD and st.session_state.get("logged_in"):
                            try:
                                from common.auth import _save_user_settings_to_sheet
                                _save_user_settings_to_sheet(st.session_state.username, _tg_save_sg)
                                if "user_settings" not in st.session_state:
                                    st.session_state.user_settings = {}
                                st.session_state.user_settings.update(_tg_save_sg)
                            except Exception as _e_sg:
                                st.error(f"GSheets 저장 오류: {_e_sg}")
                        st.success("✅ 텔레그램 설정이 저장되었습니다.")

        st.write("")

        with st.container(border=True):
            _gs_hdr1_sg, _gs_hdr2_sg = st.columns([3, 1])
            with _gs_hdr1_sg:
                st.markdown("#### 🗂️ 구글 스프레드시트 연동")
                st.caption("포트폴리오 정보와 주문 신호를 구글 스프레드시트로 전송합니다.")
            with _gs_hdr2_sg:
                with st.popover("❓ 구글 스프레드시트 URL 확인 & 권한 부여", use_container_width=True):
                    st.markdown("""
<style>
.gs-help-section { margin-bottom: 20px; }
.gs-help-title {
    display: flex; align-items: center; gap: 10px;
    font-size: 17px; font-weight: 700; color: #1a1a2e; margin-bottom: 10px;
}
.gs-help-badge {
    background: #2EAA5E; color: white;
    border-radius: 50%; width: 28px; height: 28px;
    display: inline-flex; align-items: center; justify-content: center;
    font-size: 14px; font-weight: 700; flex-shrink: 0;
}
.gs-help-box {
    background: #EDF7F0; border-radius: 10px;
    padding: 14px 18px; font-size: 14px; line-height: 2;
}
.gs-help-box ol { margin: 0; padding-left: 20px; }
.gs-help-box li { margin-bottom: 2px; }
.gs-tag {
    background: #D4EFE0; color: #1a6e3c;
    border-radius: 5px; padding: 1px 7px;
    font-family: monospace; font-size: 13px;
}
.gs-example-box {
    background: white; border: 1px solid #CBD5E1; border-radius: 8px;
    padding: 12px 16px; margin-top: 10px; font-size: 13px; color: #555;
}
.gs-example-val { color: #2EAA5E; font-family: monospace; font-size: 13px; }
.gs-warn-box {
    background: #FFFBEB; border: 1px solid #F59E0B;
    border-radius: 10px; padding: 14px 18px; font-size: 14px; line-height: 2;
}
.gs-warn-title { font-weight: 700; color: #92400E; margin-bottom: 6px; }
.gs-email-box {
    background: white; border: 1px solid #CBD5E1; border-radius: 8px;
    padding: 10px 14px; margin: 8px 0 12px 0; font-size: 13px; color: #555;
}
.gs-email-val { color: #2EAA5E; font-family: monospace; font-size: 13px; font-weight: 700; }
.gs-security-box {
    background: #F1F5F9; border-radius: 10px;
    padding: 14px 18px; font-size: 13px; color: #475569; line-height: 1.7;
    margin-bottom: 10px;
}
</style>

<div class="gs-help-section">
  <div class="gs-help-title"><span class="gs-help-badge">1</span> 새 스프레드시트 만들기</div>
  <div class="gs-help-box">
    <ol>
      <li><a href="https://sheets.google.com" target="_blank">Google Sheets</a>에 접속합니다.</li>
      <li><span class="gs-tag">+ 새로 만들기</span> 또는 <span class="gs-tag">빈 스프레드시트</span> 를 클릭합니다.</li>
      <li>스프레드시트 이름을 지정합니다. (예: Sigma 포트폴리오)</li>
    </ol>
  </div>
</div>

<div class="gs-help-section">
  <div class="gs-help-title"><span class="gs-help-badge">2</span> 스프레드시트 URL 확인하기</div>
  <div class="gs-help-box">
    <div>브라우저 주소창에 표시된 URL을 복사합니다.</div>
    <div class="gs-example-box">
      <div style="color:#888; font-size:12px; margin-bottom:4px;">URL 형식:</div>
      <div class="gs-example-val">https://docs.google.com/spreadsheets/d/1ABC...XYZ/edit</div>
    </div>
    <div style="font-size:13px; color:#64748B; margin-top:8px;">
      * 전체 URL을 복사하면 됩니다. 뒤에 <span class="gs-tag">/edit</span> 가 있어도 괜찮습니다.
    </div>
  </div>
</div>

<div class="gs-help-section">
  <div class="gs-help-title"><span class="gs-help-badge">3</span> 서비스 계정에 편집 권한 부여 (중요!)</div>
  <div class="gs-warn-box">
    <div class="gs-warn-title">⚠ 앱이 스프레드시트에 데이터를 기록하려면 아래 이메일에 편집 권한을 부여해야 합니다.</div>
    <div class="gs-email-box">
      <div style="color:#888; font-size:12px; margin-bottom:4px;">서비스 계정 이메일:</div>
      <div class="gs-email-val">connectspreadsheet@sodium-gateway-485307-f3.iam.gserviceaccount.com</div>
    </div>
    <ol>
      <li>스프레드시트 우측 상단의 <span class="gs-tag">공유</span> 버튼을 클릭합니다.</li>
      <li>"사용자 및 그룹 추가" 입력란에 위 서비스 계정 이메일을 붙여넣습니다.</li>
      <li>권한을 <span class="gs-tag">편집자</span> 로 설정합니다.</li>
      <li><span class="gs-tag">보내기</span> 를 클릭합니다.</li>
    </ol>
  </div>
</div>

<div class="gs-security-box">
  <strong>보안 참고사항:</strong> 서비스 계정은 이 앱 전용 계정으로, 공유된 스프레드시트에만 접근할 수 있습니다.
  스프레드시트를 "링크가 있는 모든 사용자"로 공개할 필요 없이, 서비스 계정에만 권한을 부여하면 됩니다.
</div>
""", unsafe_allow_html=True)

            _gs_url_sg = st.text_input(
                "스프레드시트 URL",
                value=str(_us_sg.get("gs_url", "")),
                placeholder="https://docs.google.com/spreadsheets/d/...",
                key="sigma_gs_url_input",
            )
            st.caption("* 스프레드시트에 서비스 계정 이메일을 편집자로 공유해주세요. (우측 상단 도움말 참고)")

            _gs_sigma_settings_sg = _get_sigma_ticker_settings()
            _gs_sheet_map_sg = {}
            if _gs_sigma_settings_sg:
                st.markdown("##### 📋 종목별 시트 이름 매핑")
                st.caption("각 종목 매매 기록을 저장할 구글시트의 탭(시트) 이름을 입력하세요.")
                for _gs_tk_sg, _gs_cfg_sg in _gs_sigma_settings_sg.items():
                    _default_sheet_sg = _gs_cfg_sg.get("gs_sheet", f"sigma_{_gs_tk_sg}")
                    _gs_sheet_map_sg[_gs_tk_sg] = st.text_input(
                        f"{_gs_tk_sg} 시트 이름",
                        value=_default_sheet_sg,
                        key=f"gs_sheet_sigma_{_gs_tk_sg}",
                    )
            else:
                st.info("📭 등록된 Sigma 계좌가 없습니다. 주문표 탭에서 계좌를 먼저 등록해주세요.")

            st.write("")
            _btn_gs1_sg, _btn_gs2_sg, _btn_gs3_sg = st.columns([1, 1, 1])
            with _btn_gs1_sg:
                if st.button("🔗 시트 연결 테스트", use_container_width=True, key="sigma_gs_test"):
                    if not _gs_url_sg:
                        st.warning("스프레드시트 URL을 먼저 입력해주세요.")
                    else:
                        try:
                            _gc_sg = _get_gspread_client()
                            _sh_sg = _gc_sg.open_by_url(_gs_url_sg)
                            st.success(f"✅ 연결 성공! 스프레드시트: **{_sh_sg.title}**")
                        except Exception as _e_sg:
                            st.error(f"❌ 연결 실패: {_e_sg}")

            with _btn_gs2_sg:
                if st.button("📋 주문 시트 전송", use_container_width=True, key="sigma_gs_send", type="primary"):
                    if not _gs_url_sg:
                        st.warning("스프레드시트 URL을 먼저 입력해주세요.")
                    elif not _gs_sigma_settings_sg:
                        st.warning("등록된 Sigma 계좌가 없습니다.")
                    else:
                        for _gs_tk_sg2, _gs_cfg_sg2 in _gs_sigma_settings_sg.items():
                            _sheet_nm_sg2 = _gs_sheet_map_sg.get(_gs_tk_sg2, f"sigma_{_gs_tk_sg2}")
                            with st.spinner(f"{_gs_tk_sg2} → '{_sheet_nm_sg2}' 전송 중..."):
                                try:
                                    _od_sg2 = run_sigma_ordersheet(
                                        _gs_tk_sg2,
                                        int(_gs_cfg_sg2.get("sigma_period", 252)),
                                        _as_of=str(datetime.today().date()),
                                    )
                                    if _od_sg2 is None:
                                        st.error(f"❌ {_gs_tk_sg2}: 데이터 부족")
                                        continue
                                    _amt_sg2  = float(_gs_cfg_sg2.get("amount_per_trade",
                                        float(_gs_cfg_sg2.get("os_capital", 100000)) /
                                        int(_gs_cfg_sg2.get("divisions", 20))))
                                    _qty_sg2  = math.floor(_amt_sg2 / _od_sg2["buy_loc_1"]) if _od_sg2["buy_loc_1"] > 0 else 0
                                    _gc2      = _get_gspread_client()
                                    _sh2      = _gc2.open_by_url(_gs_url_sg)
                                    _ws2      = _sh2.worksheet(_sheet_nm_sg2)
                                    _ws2.batch_clear(["L4:O13"])
                                    _ws2.update(range_name="L4", values=[
                                        ["1σ 매수 LOC", round(_od_sg2["buy_loc_1"], 4), _qty_sg2,
                                         f"μ:{_od_sg2['mu_pct']:+.4f}% σ:{_od_sg2['sigma_pct']:.4f}%"],
                                        ["2σ 참고",     round(_od_sg2["buy_loc_2"], 4), "", ""],
                                        ["3σ 참고",     round(_od_sg2["buy_loc_3"], 4), "", ""],
                                    ])
                                    # B11 업데이트 시각 (KST)
                                    _ws2.update(range_name="B11", values=[[
                                        pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")]])
                                    st.success(f"✅ {_gs_tk_sg2} → '{_sheet_nm_sg2}' 탭 L4에 전송 완료!")
                                except Exception as _e_gs2:
                                    st.error(f"❌ {_gs_tk_sg2} 전송 실패: {_e_gs2}")

            with _btn_gs3_sg:
                if st.button("💾 저장하기", use_container_width=True, key="sigma_gs_save", type="primary"):
                    if not _gs_url_sg:
                        st.warning("스프레드시트 URL을 입력해주세요.")
                    else:
                        _gs_save_sg = {"gs_url": _gs_url_sg}
                        save_config(_gs_save_sg)
                        if _IS_CLOUD and st.session_state.get("logged_in"):
                            try:
                                from common.auth import _save_user_settings_to_sheet
                                _save_user_settings_to_sheet(st.session_state.username, _gs_save_sg)
                                if "user_settings" not in st.session_state:
                                    st.session_state.user_settings = {}
                                st.session_state.user_settings.update(_gs_save_sg)
                            except Exception as _e_sg:
                                st.error(f"GSheets 저장 오류: {_e_sg}")
                        for _gs_tk2_sg, _sheet_nm_sg in _gs_sheet_map_sg.items():
                            _save_sigma_ticker_setting(_gs_tk2_sg, {"gs_sheet": _sheet_nm_sg})
                        st.success("✅ URL 및 종목별 시트 이름이 저장되었습니다.")

        st.write("")

        with st.expander("🔧 관리자 도구 — 비밀번호 해시 생성 (users 시트 등록용)"):
            st.caption("새 사용자를 추가할 때 비밀번호를 bcrypt 해시로 변환하여 Google Sheets에 붙여넣으세요.")
            _admin_pw_sg = st.text_input("등록할 비밀번호 입력", type="password", key="sigma_admin_pw")
            if st.button("🔑 해시 생성", key="sigma_gen_hash"):
                if _admin_pw_sg:
                    import bcrypt
                    _hash = bcrypt.hashpw(_admin_pw_sg.encode(), bcrypt.gensalt()).decode()
                    st.code(_hash, language=None)
                    st.caption("👆 위 해시를 복사해서 users 시트의 password_hash 컬럼에 붙여넣으세요.")
                else:
                    st.warning("비밀번호를 입력해주세요.")
