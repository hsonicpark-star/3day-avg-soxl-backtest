# -*- coding: utf-8 -*-
"""TQ200 (TQQQ 200일선 이격도 매매) — 전략 모듈

200일 이동평균 대비 이격도로 진입·청산하는 추세추종 스윙 전략.
- 매수: 이격도 1% 초과 10% 미만 + 200MA 기울기(5일) > 0 → 다음 거래일 시가 전량 매수 (MOO)
- 매도: 이격도 −1% 미만 이탈 또는 50% 초과(과열)        → 다음 거래일 시가 전량 매도 (MOO)
        (과열 익절 기본값은 원본 시트 55% 에서 50% 로 조정 — tq200_engine.DEFAULTS 주석 참고)
- 원본: 14.TQ200/TQQQ200일선.xlsx — 무조정 종가 기준 3,986일·26사이클 1:1 일치
        (재현: 14.TQ200/verify_tq200.py)
- 설정 저장: Cloud = users.tq200_config JSON 컬럼 / 로컬 = ~/.tq200/config.json
"""
from __future__ import annotations

import json
import os
import time
from datetime import date, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf
from plotly.subplots import make_subplots

from common.analysis import compute_rolling_perf, monthly_perf_table
from common.config import _IS_CLOUD
from common.data import filter_incomplete_today, next_trading_date
from common.telegram import _send_telegram, render_telegram_help_popover
from tq200_engine import (DEFAULTS, EXIT_BREAK, EXIT_TP, SHEET_PARAMS,
                          compute_stats, latest_signal, run_backtest,
                          yearly_returns)
from tq200_engine import build_order_plan as _order_plan
from tq200_engine import build_order_text as _order_text
from tq200_engine import engine_kwargs as _engine_kwargs

# ──────────────────────────────────────────────
# 설정 저장 (Cloud: users.tq200_config / 로컬: ~/.tq200/config.json)
# ──────────────────────────────────────────────
_TQ_CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".tq200")
_TQ_CONFIG_PATH = os.path.join(_TQ_CONFIG_DIR, "config.json")

PRICE_RAW = "무조정 종가 (원본 시트 동일)"
PRICE_ADJ = "배당 수정주가"

_COL_STRAT = "#e45756"
_COL_BH = "#888888"
_COL_MA = "#f2a93b"
_COL_BUY = "#22c55e"
_COL_SELL = "#ef4444"


def _load_cfg() -> dict:
    if _IS_CLOUD and st.session_state.get("logged_in"):
        raw = st.session_state.get("user_settings", {}).get("tq200_config", "")
        if raw:
            try:
                cfg = json.loads(raw) if isinstance(raw, str) else raw
                return cfg if isinstance(cfg, dict) else {}
            except Exception:
                pass
        return {}
    try:
        with open(_TQ_CONFIG_PATH, encoding="utf-8") as f:
            cfg = json.load(f)
            return cfg if isinstance(cfg, dict) else {}
    except Exception:
        return {}


def _save_cfg(cfg: dict):
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            cfg_json = json.dumps(cfg, ensure_ascii=False)
            st.session_state.setdefault("user_settings", {})
            st.session_state.user_settings["tq200_config"] = cfg_json
            from common.auth import _save_user_settings_to_sheet
            _save_user_settings_to_sheet(st.session_state.username,
                                         {"tq200_config": cfg_json})
        except Exception as e:
            st.warning(f"⚠️ Cloud 저장 실패: {e}")
        return
    try:
        os.makedirs(_TQ_CONFIG_DIR, exist_ok=True)
        with open(_TQ_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.warning(f"⚠️ 로컬 저장 실패: {e}")


# 사이드바 위젯 key ↔ 저장 파라미터 key ↔ 기본값
_WIDGETS = [
    ("tq_ma", "ma_window", DEFAULTS["ma_window"]),
    ("tq_slope_n", "slope_window", DEFAULTS["slope_window"]),
    ("tq_buy_lo", "buy_lo", DEFAULTS["buy_lo"]),
    ("tq_buy_hi", "buy_hi", DEFAULTS["buy_hi"]),
    ("tq_exit", "exit_disp", DEFAULTS["exit_disp"]),
    ("tq_tp", "tp_disp", DEFAULTS["tp_disp"]),
    ("tq_use_slope", "use_slope", DEFAULTS["use_slope"]),
    ("tq_use_tp", "use_tp", DEFAULTS["use_tp"]),
    ("tq_fee", "fee", DEFAULTS["fee"]),
    ("tq_capital", "capital", 10000.0),
    ("tq_price_mode", "price_mode", PRICE_RAW),
]
_INT_KEYS = {"ma_window", "slope_window"}
_BOOL_KEYS = {"use_slope", "use_tp"}


def _coerce(pkey: str, val, default):
    try:
        if pkey in _INT_KEYS:
            return int(float(val))
        if pkey in _BOOL_KEYS:
            return bool(val)
        if pkey == "price_mode":
            return val if val in (PRICE_RAW, PRICE_ADJ) else default
        return float(val)
    except (TypeError, ValueError):
        return default


# ──────────────────────────────────────────────
# 데이터 (OHLC — 신호는 종가, 체결은 다음날 시가라 시가 필요)
# ──────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=1800)
def _load_ohlc(ticker: str, end_str: str, adjusted: bool) -> pd.DataFrame:
    """상장 이후 전체 OHLC. 200MA 워밍업 때문에 시작일과 무관하게 전 구간 로드.

    adjusted=False(기본) = 무조정 종가 — 원본 시트와 같은 기준.
    야후 순간 장애 대비 3회 재시도. 실패 시 빈 DF.
    (로컬 pricedb 는 종가만 있어 시가 체결 전략의 폴백으로 쓸 수 없다)
    """
    end = pd.to_datetime(end_str).date()
    for attempt in range(3):
        try:
            raw = yf.download(ticker, start="2009-01-01",
                              end=end + timedelta(days=2),
                              progress=False, auto_adjust=adjusted)
            if raw is not None and not raw.empty:
                if isinstance(raw.columns, pd.MultiIndex):
                    try:
                        raw = raw.xs(ticker, axis=1, level="Ticker")
                    except Exception:
                        raw.columns = raw.columns.droplevel(1)
                df = raw[["Open", "High", "Low", "Close"]].copy()
                df.index = pd.to_datetime(df.index)
                try:
                    df.index = df.index.tz_localize(None)
                except Exception:
                    pass
                df = df.apply(pd.to_numeric, errors="coerce").dropna()
                df = filter_incomplete_today(df)
                return df[df.index <= pd.Timestamp(end)]
        except Exception:
            pass
        if attempt < 2:
            time.sleep(2 + 2 * attempt)
    return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=3600)
def _cached_backtest(df, capital, start_str, end_str, ma_window, slope_window,
                     buy_lo, buy_hi, exit_disp, tp_disp, use_slope, use_tp,
                     fee):
    """백테스트 결과 캐시 — 다른 위젯 조작 시 재계산 방지."""
    return run_backtest(df, initial_capital=capital,
                        start=start_str, end=end_str,
                        ma_window=ma_window, slope_window=slope_window,
                        buy_lo=buy_lo / 100, buy_hi=buy_hi / 100,
                        exit_disp=exit_disp / 100, tp_disp=tp_disp / 100,
                        use_slope=use_slope, use_tp=use_tp,
                        fee_rate=fee / 100)


def _run(params: dict, df: pd.DataFrame) -> dict:
    return _cached_backtest(
        df, params["capital"], str(params["bt_start_date"]),
        str(params["bt_end_date"]), params["ma_window"],
        params["slope_window"], params["buy_lo"], params["buy_hi"],
        params["exit_disp"], params["tp_disp"], params["use_slope"],
        params["use_tp"], params["fee"])


def _get_data(params: dict, end=None) -> pd.DataFrame:
    return _load_ohlc(params["bt_ticker"],
                      str(end or params["bt_end_date"]),
                      params["price_mode"] == PRICE_ADJ)


# ──────────────────────────────────────────────
# 카드 UI
# ──────────────────────────────────────────────
def _stat_cards(items: list, tone: str = "gray") -> str:
    if tone == "blue":
        border, bg, lbl = ("rgba(59,130,246,.4)", "rgba(59,130,246,.08)",
                           "#3b82f6")
    else:
        border, bg, lbl = ("rgba(128,128,128,.28)", "rgba(128,128,128,.05)",
                           "#888")
    boxes = []
    for label, value, sub, color in items:
        sub_html = (f'<div style="font-size:12px; margin-top:5px; '
                    f'color:{color};">{sub}</div>' if sub else "")
        boxes.append(
            '<div style="flex:1 1 150px; min-width:140px; '
            f'border:1px solid {border}; border-radius:10px; '
            'padding:13px 8px 11px; text-align:center; '
            f'background:{bg};">'
            f'<div style="font-size:12.5px; color:{lbl}; '
            f'margin-bottom:6px;">{label}</div>'
            f'<div style="font-size:22px; font-weight:700; '
            f'line-height:1.15;">{value}</div>{sub_html}</div>')
    return ('<div style="display:flex; gap:10px; flex-wrap:wrap; '
            'margin:4px 0 12px 0;">' + "".join(boxes) + "</div>")


def _rule_text(P: dict) -> str:
    """현재 파라미터를 한 줄 규칙으로."""
    buy = f"이격도 {P['buy_lo']:g}%\\~{P['buy_hi']:g}%"
    if P["use_slope"]:
        buy += f" + {P['ma_window']}MA 기울기({P['slope_window']}일) > 0"
    sell = f"이격도 {P['exit_disp']:g}% 이탈"
    if P["use_tp"]:
        sell += f" 또는 {P['tp_disp']:g}% 초과"
    return f"**매수** {buy} → 익일 시가(MOO) · **매도** {sell} → 익일 시가(MOO)"


# ══════════════════════════════════════════════
# 사이드바
# ══════════════════════════════════════════════
def render_sidebar() -> dict:
    # 위젯 session_state 시드 — 저장된 기본값(개인 설정 탭) 우선.
    # 위젯은 key 만 쓰고 value= 미지정 (session_state 충돌 방지).
    # 다른 전략으로 갔다 오면 Streamlit 이 위젯 key 를 지우므로 '없는 key' 만 다시 채운다.
    # 초기화 요청은 위젯 생성 전에만 반영 가능 → 개인 설정 탭이 플래그만 세우고 rerun.
    if st.session_state.pop("_tq_reset", False):
        for wkey, _pkey, default in _WIDGETS:
            st.session_state[wkey] = default
    _missing = [w for w in _WIDGETS if w[0] not in st.session_state]
    if _missing:
        saved = _load_cfg().get("params", {})
        for wkey, pkey, default in _missing:
            st.session_state[wkey] = _coerce(
                pkey, saved.get(pkey, default), default)

    st.subheader("📌 종목")
    _tk_presets = ["TQQQ", "SOXL", "UPRO", "QLD", "직접입력"]
    _sel = st.selectbox("종목코드 (Ticker)", _tk_presets, index=0,
                        key="tq_tk_sel")
    if _sel == "직접입력":
        ticker = st.text_input("티커 직접 입력", key="tq_tk_custom",
                               placeholder="예: QQQ, SPY, NVDA").strip().upper()
    else:
        ticker = _sel

    st.markdown("---")
    st.subheader("전략 파라미터")
    c1, c2 = st.columns(2)
    ma_window = c1.number_input("이동평균 (일)", min_value=20, max_value=400,
                                step=10, key="tq_ma")
    slope_window = c2.number_input("기울기 기간 (일)", min_value=1,
                                   max_value=60, step=1, key="tq_slope_n")

    st.caption("🟢 **매수** — 이격도가 하한 초과 · 상한 미만이면 익일 시가(MOO) 매수")
    c3, c4 = st.columns(2)
    buy_lo = c3.number_input("이격도 하한 (%)", min_value=-20.0,
                             max_value=50.0, step=0.5, format="%.1f",
                             key="tq_buy_lo")
    buy_hi = c4.number_input("이격도 상한 (%)", min_value=-20.0,
                             max_value=100.0, step=0.5, format="%.1f",
                             key="tq_buy_hi",
                             help="이 값 이상이면 추격 매수하지 않고 눌림을 기다립니다")
    use_slope = st.checkbox("200MA 기울기 > 0 일 때만 매수", key="tq_use_slope",
                            help="이동평균이 하락 중이면 이격도가 맞아도 매수하지 않음 "
                                 "(하락 추세 속 데드캣 반등 회피)")

    st.caption("🔴 **매도** — 조건 충족 시 익일 시가(MOO) 전량 매도")
    exit_disp = st.number_input("청산 이격도 (%) — 미만으로 이탈", min_value=-30.0,
                                max_value=20.0, step=0.5, format="%.1f",
                                key="tq_exit")
    use_tp = st.checkbox("과열 익절 사용", key="tq_use_tp",
                         help="이격도가 과도하게 벌어지면 익절 후 눌림에서 재진입")
    tp_disp = st.number_input("익절 이격도 (%) — 초과", min_value=5.0,
                              max_value=300.0, step=5.0, format="%.1f",
                              key="tq_tp", disabled=not use_tp)
    if buy_lo >= buy_hi:
        st.warning("⚠️ 이격도 하한이 상한보다 크거나 같아 매수 신호가 나오지 않습니다.")
    if exit_disp >= buy_lo:
        st.warning("⚠️ 청산 이격도가 매수 하한 이상이면 매수 직후 바로 청산됩니다.")

    fee = st.number_input("수수료+슬리피지 (%, 편도)", min_value=0.0,
                          step=0.01, format="%.2f", key="tq_fee")

    st.markdown("---")
    st.subheader("백테스트 설정")
    capital = st.number_input("초기 투자금 ($)", min_value=100.0, step=1000.0,
                              format="%.0f", key="tq_capital")
    b1, b2 = st.columns(2)
    bt_start = b1.date_input("시작 일", date(2011, 1, 3), key="tq_start")
    bt_end = b2.date_input("종료 일", date.today(), key="tq_end")
    price_mode = st.radio("📂 가격 기준", [PRICE_RAW, PRICE_ADJ],
                          key="tq_price_mode",
                          help="무조정: 증권사 차트의 200일선과 같은 기준(원본 엑셀과 "
                               "일치). 수정주가: 배당을 재투자한 것으로 환산 — "
                               "과거 200일선이 미세하게 달라져 일부 신호가 바뀝니다.")

    params = {
        "bt_ticker": ticker, "bt_start_date": bt_start, "bt_end_date": bt_end,
        "bt_initial_capital": float(capital), "capital": float(capital),
        "ma_window": int(ma_window), "slope_window": int(slope_window),
        "buy_lo": float(buy_lo), "buy_hi": float(buy_hi),
        "exit_disp": float(exit_disp), "tp_disp": float(tp_disp),
        "use_slope": bool(use_slope), "use_tp": bool(use_tp),
        "fee": float(fee), "price_mode": price_mode,
    }
    # 개인 설정 탭(인자 없음)의 '주문표 테스트 발송'이 현재 세팅을 쓰도록 보관
    st.session_state["tq_last_params"] = params
    return params


# ══════════════════════════════════════════════
# 백테스트 탭
# ══════════════════════════════════════════════
def _trade_points(log: pd.DataFrame):
    buys = log[log["매매"] == "시가 매수"]
    sells = log[log["매매"] == "시가 매도"]
    return buys, sells


def _holding_spans(log: pd.DataFrame) -> list:
    """보유 구간 [(시작, 끝), ...] — 차트 음영용."""
    held = (log["보유수량"] > 0).to_numpy()
    idx = log.index
    spans, start = [], None
    for i, h in enumerate(held):
        if h and start is None:
            start = idx[i]
        elif not h and start is not None:
            spans.append((start, idx[i]))
            start = None
    if start is not None:
        spans.append((start, idx[-1]))
    return spans


def _fig_equity(log, capital, name, log_scale):
    bh = capital * log["종가"] / log["종가"].iloc[0]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=log.index, y=log["평가금"], name=name,
                             line=dict(color=_COL_STRAT, width=2)))
    fig.add_trace(go.Scatter(x=bh.index, y=bh, name="Buy&Hold",
                             line=dict(color=_COL_BH, width=1.2, dash="dot")))
    fig.update_layout(height=420, margin=dict(l=10, r=10, t=30, b=10),
                      yaxis_title="평가금 ($)", legend=dict(x=0.01, y=0.99),
                      hovermode="x unified")
    if log_scale:
        fig.update_yaxes(type="log")
    return fig


def _fig_price(log, P, log_scale):
    buys, sells = _trade_points(log)
    fig = go.Figure()
    for s, e in _holding_spans(log):
        fig.add_vrect(x0=s, x1=e, fillcolor=_COL_BUY, opacity=0.08,
                      line_width=0, layer="below")
    fig.add_trace(go.Scatter(x=log.index, y=log["종가"], name="종가",
                             line=dict(color="#4c78a8", width=1.3)))
    fig.add_trace(go.Scatter(x=log.index, y=log["200MA"],
                             name=f"{P['ma_window']}MA",
                             line=dict(color=_COL_MA, width=1.6)))
    fig.add_trace(go.Scatter(
        x=buys.index, y=buys["시가"], name="시가 매수", mode="markers",
        marker=dict(symbol="triangle-up", size=10, color=_COL_BUY,
                    line=dict(color="#fff", width=1))))
    fig.add_trace(go.Scatter(
        x=sells.index, y=sells["시가"], name="시가 매도", mode="markers",
        marker=dict(symbol="triangle-down", size=10, color=_COL_SELL,
                    line=dict(color="#fff", width=1))))
    fig.update_layout(height=420, margin=dict(l=10, r=10, t=40, b=10),
                      title_text="가격 · 이동평균 · 매매 시점 (초록 음영 = 보유 구간)",
                      yaxis_title="가격 ($)", hovermode="x unified",
                      legend=dict(x=0.01, y=0.99))
    if log_scale:
        fig.update_yaxes(type="log")
    return fig


def _fig_disp(log, P):
    d = log["이격도"] * 100
    fig = go.Figure()
    fig.add_hrect(y0=P["buy_lo"], y1=P["buy_hi"], fillcolor=_COL_BUY,
                  opacity=0.10, line_width=0, layer="below")
    fig.add_trace(go.Scatter(x=d.index, y=d, name="이격도",
                             line=dict(color="#4c78a8", width=1.2)))
    fig.add_hline(y=P["exit_disp"], line=dict(color=_COL_SELL, width=1.2,
                                              dash="dash"),
                  annotation_text=f"청산 {P['exit_disp']:g}%",
                  annotation_position="bottom left")
    if P["use_tp"]:
        fig.add_hline(y=P["tp_disp"], line=dict(color=_COL_MA, width=1.2,
                                                dash="dash"),
                      annotation_text=f"과열 익절 {P['tp_disp']:g}%",
                      annotation_position="top left")
    fig.add_hline(y=0, line=dict(color="#888", width=0.8))
    fig.update_layout(height=280, margin=dict(l=10, r=10, t=40, b=10),
                      title_text="이격도 추이 (초록 띠 = 매수 구간)",
                      yaxis_title="이격도 (%)", showlegend=False,
                      hovermode="x unified")
    return fig


def _fig_drawdown(log, capital):
    eq = np.concatenate([[capital], log["평가금"].to_numpy()])
    dd = (eq / np.maximum.accumulate(eq) - 1.0)[1:] * 100
    bh = log["종가"] / log["종가"].cummax() - 1.0
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=log.index, y=bh * 100, name="Buy&Hold",
                             line=dict(color=_COL_BH, width=1, dash="dot")))
    fig.add_trace(go.Scatter(x=log.index, y=dd, name="TQ200", fill="tozeroy",
                             line=dict(color=_COL_STRAT, width=1.2)))
    fig.update_layout(height=260, margin=dict(l=10, r=10, t=40, b=10),
                      title_text="고점 대비 낙폭 (Drawdown)",
                      yaxis_title="낙폭 (%)", hovermode="x unified",
                      legend=dict(x=0.01, y=0.02))
    return fig


def render_backtest_tab(params: dict):
    ticker = params["bt_ticker"]
    if not ticker:
        st.warning("티커를 입력해주세요.")
        return
    with st.spinner(f"{ticker} 가격 데이터 로드 중..."):
        df = _get_data(params)
    if df.empty:
        st.error(f"{ticker} 가격 데이터를 불러오지 못했습니다. "
                 "야후 파이낸스 일시 장애일 수 있으니 잠시 후 다시 시도해주세요.")
        return

    result = _run(params, df)
    log = result["log"]
    if log.empty:
        st.error(f"백테스트 구간에 데이터가 없습니다. {params['ma_window']}일 이동평균 "
                 f"계산에 상장 후 {params['ma_window'] + params['slope_window']} "
                 "거래일이 필요합니다. 시작일/종료일을 확인하세요.")
        return
    capital = params["capital"]
    stats = compute_stats(result, capital)
    cyc = result["cycles"]

    st.caption(
        f"데이터: {log.index[0].date()} ~ {log.index[-1].date()} "
        f"({len(log):,} 거래일 · yfinance {params['price_mode']}) · "
        + _rule_text(params))
    if log.index[0].date() > params["bt_start_date"] + timedelta(days=7):
        st.info(f"ℹ️ {params['ma_window']}일 이동평균 워밍업 때문에 실제 백테스트는 "
                f"**{log.index[0].date()}** 부터 시작합니다.")

    _n_cyc = stats["완료 사이클"]
    st.markdown(_stat_cards([
        ("총수익률", f"{stats['총수익률(%)']:,.0f}%",
         f"B&H {stats['B&H 수익률(%)']:,.0f}%", "#888"),
        ("CAGR", f"{stats['CAGR(%)']:.1f}%",
         f"B&H {stats['B&H CAGR(%)']:.1f}%", "#888"),
        ("MDD", f"{stats['MDD(%)']:.1f}%",
         f"B&H {stats['B&H MDD(%)']:.1f}%", "#888"),
        ("최종평가금", f"${stats['최종평가금']:,.0f}",
         f"Calmar {stats['Calmar']:.2f} · Sharpe {stats['Sharpe']:.2f}", "#888"),
        ("완료 사이클", f"{_n_cyc}회",
         f"승률 {stats['승률(%)']:.0f}%" if _n_cyc else "", "#888"),
        ("보유 비중", f"{stats['보유비중(%)']:.0f}%", "나머지는 현금", "#888"),
    ], tone="blue"), unsafe_allow_html=True)

    op = result["open_position"]
    if op:
        st.warning(f"📌 기간말 보유 중: {op['시작일'].date()} 시가 {op['매수가']:.2f} "
                   f"매수 · {op['보유일']}거래일째 · 현재가 {op['현재가']:.2f} · "
                   f"평가손익 {op['평가손익']:+,.0f}$ ({op['평가손익률(%)']:+.2f}%)")
    if result["pending"] == "buy":
        st.success(f"🟢 마지막 날({log.index[-1].date()}) 종가 기준 **매수 신호** "
                   "— 다음 거래일 시가 매수")
    elif result["pending"] == "sell":
        st.error(f"🔴 마지막 날({log.index[-1].date()}) 종가 기준 **매도 신호** "
                 f"({result['pending_reason']}) — 다음 거래일 시가 매도")

    log_scale = st.toggle("로그 스케일", value=True, key="tq_logscale",
                          help="장기 복리 곡선은 로그 스케일이 구간별 성과 비교에 적합합니다")
    st.plotly_chart(_fig_equity(log, capital, f"TQ200 ({ticker})", log_scale),
                    use_container_width=True)
    st.plotly_chart(_fig_price(log, params, log_scale),
                    use_container_width=True)
    st.plotly_chart(_fig_disp(log, params), use_container_width=True)
    st.plotly_chart(_fig_drawdown(log, capital), use_container_width=True)

    # ── 사이클 통계 ──
    if stats["완료 사이클"]:
        st.subheader("🔁 사이클 통계")
        st.markdown(_stat_cards([
            ("승률", f"{stats['승률(%)']:.1f}%",
             f"{stats['완료 사이클']}회 중 "
             f"{int(round(stats['승률(%)'] * stats['완료 사이클'] / 100))}승", "#888"),
            ("평균 수익", f"{stats['평균 수익(%)']:+.1f}%",
             f"최대 {stats['최대 수익(%)']:+.1f}%", _COL_BUY),
            ("평균 손실", f"{stats['평균 손실(%)']:+.1f}%",
             f"최대 {stats['최대 손실(%)']:+.1f}%", _COL_SELL),
            ("평균 보유", f"{stats['평균 보유일']:.0f}일",
             f"최장 {stats['최장 보유일']}일", "#888"),
            ("과열 익절", f"{stats['과열 익절 횟수']}회",
             f"이탈 청산 {stats['완료 사이클'] - stats['과열 익절 횟수']}회", "#888"),
        ]), unsafe_allow_html=True)
        st.caption("💡 추세추종 전략의 전형 — 손실은 작게 자주, 수익은 크게 가끔 납니다. "
                   "짧은 손절이 연속되는 구간(횡보장)을 견디는 것이 핵심입니다.")

    c1, c2 = st.columns([1, 2])
    with c1:
        st.subheader("연도별 수익률")
        st.dataframe(yearly_returns(log["평가금"], capital, log["종가"]),
                     hide_index=True, use_container_width=True, height=420)
    with c2:
        st.subheader("사이클 내역")
        if len(cyc):
            show = cyc.copy()
            show["시작일"] = show["시작일"].dt.date
            show["종료일"] = show["종료일"].dt.date
            st.dataframe(
                show, hide_index=True, use_container_width=True, height=420,
                column_config={
                    "수익률(%)": st.column_config.NumberColumn(format="%.2f"),
                    "누적수익률(%)": st.column_config.NumberColumn(format="%.1f"),
                    "MDD(%)": st.column_config.NumberColumn(format="%.2f"),
                    "매수가": st.column_config.NumberColumn(format="%.2f"),
                    "매도가": st.column_config.NumberColumn(format="%.2f"),
                })
        else:
            st.info("완료된 사이클이 없습니다.")

    st.subheader("📅 월별 수익률")
    _mode = pd.Series(np.where(log["보유수량"] > 0, "보유", "현금"),
                      index=log.index)
    st.markdown(monthly_perf_table(log["평가금"], _mode, vmax=20.0,
                                   mode_short={"보유": "보유", "현금": "현금"},
                                   idle_label="현금"),
                unsafe_allow_html=True)

    with st.expander("📜 일별 로그 (원본 시트 '최근로그' 형식 — 최신순)"):
        show = log.iloc[::-1].copy()
        show.index = show.index.date
        show["이격도"] = show["이격도"] * 100
        show = show.rename(columns={"이격도": "이격도(%)",
                                    "200MA": f"{params['ma_window']}MA"})
        st.dataframe(
            show, use_container_width=True, height=420,
            column_config={
                "시가": st.column_config.NumberColumn(format="%.2f"),
                "종가": st.column_config.NumberColumn(format="%.2f"),
                f"{params['ma_window']}MA": st.column_config.NumberColumn(format="%.2f"),
                "이격도(%)": st.column_config.NumberColumn(format="%.2f"),
                "Slope": st.column_config.NumberColumn(format="%.4f"),
                "보유수량": st.column_config.NumberColumn(format="%.2f"),
                "현금": st.column_config.NumberColumn(format="%.0f"),
                "평가금": st.column_config.NumberColumn(format="%.0f"),
            })
        st.download_button(
            "⬇️ 일별 로그 CSV", show.to_csv().encode("utf-8-sig"),
            file_name=f"TQ200_{ticker}_log.csv", mime="text/csv",
            key="tq_dl_log")


# ══════════════════════════════════════════════
# 파라미터 최적화 탭
# ══════════════════════════════════════════════
_TP_OFF = "없음"


def render_optimization_tab(params: dict):
    ticker = params["bt_ticker"]
    st.subheader("🔍 파라미터 그리드 탐색")
    st.caption("기간·초기 투자금·수수료·가격 기준·기울기 필터는 사이드바를 따릅니다. "
               "후보를 고르면 모든 조합을 백테스트해 비교합니다.")
    if not ticker:
        st.warning("티커를 입력해주세요.")
        return
    df = _get_data(params)
    if df.empty:
        st.error(f"{ticker} 가격 데이터를 불러오지 못했습니다.")
        return

    c1, c2, c3 = st.columns(3)
    mas = c1.multiselect("이동평균 (일)", [100, 120, 150, 180, 200, 220, 250],
                         default=[150, 200, 250], key="tq_opt_ma")
    los = c2.multiselect("매수 이격도 하한 (%)", [0.0, 0.5, 1.0, 2.0, 3.0, 5.0],
                         default=[0.0, 1.0, 2.0], key="tq_opt_lo")
    his = c3.multiselect("매수 이격도 상한 (%)",
                         [5.0, 8.0, 10.0, 12.0, 15.0, 20.0, 30.0],
                         default=[8.0, 10.0, 15.0], key="tq_opt_hi")
    c4, c5, c6 = st.columns(3)
    exits = c4.multiselect("청산 이격도 (%)", [-5.0, -3.0, -2.0, -1.0, 0.0],
                           default=[-3.0, -1.0, 0.0], key="tq_opt_exit")
    tps = c5.multiselect("익절 이격도 (%)",
                         [30.0, 40.0, 50.0, 55.0, 60.0, 70.0, _TP_OFF],
                         default=[40.0, 50.0, 55.0, 70.0, _TP_OFF],
                         key="tq_opt_tp")
    sort_key = c6.selectbox("정렬 기준", ["Calmar", "CAGR(%)", "MDD(%)",
                                       "총수익률(%)", "Sharpe"],
                            key="tq_opt_sort")

    combos = [(ma, lo, hi, ex, tp) for ma in mas for lo in los for hi in his
              for ex in exits for tp in tps if lo < hi and ex < lo]
    st.caption(f"조합 수: **{len(combos):,}개** "
               "(하한 ≥ 상한, 청산 ≥ 하한인 조합은 제외)")
    if len(combos) > 3000:
        st.warning("조합이 3,000개를 넘습니다. 후보를 줄여주세요.")
        return

    if st.button("▶️ 그리드 실행", key="tq_opt_run", type="primary") and combos:
        rows = []
        prog = st.progress(0.0, text="백테스트 실행 중...")
        step = max(len(combos) // 100, 1)
        for i, (ma, lo, hi, ex, tp) in enumerate(combos):
            r = run_backtest(
                df, initial_capital=params["capital"],
                start=str(params["bt_start_date"]),
                end=str(params["bt_end_date"]),
                ma_window=int(ma), slope_window=params["slope_window"],
                buy_lo=lo / 100, buy_hi=hi / 100, exit_disp=ex / 100,
                tp_disp=(0.0 if tp == _TP_OFF else tp / 100),
                use_slope=params["use_slope"], use_tp=(tp != _TP_OFF),
                fee_rate=params["fee"] / 100)
            s = compute_stats(r, params["capital"])
            if not s:
                continue
            rows.append({
                "MA": int(ma), "하한(%)": lo, "상한(%)": hi, "청산(%)": ex,
                "익절(%)": tp if tp == _TP_OFF else f"{tp:g}",
                "총수익률(%)": s["총수익률(%)"], "CAGR(%)": s["CAGR(%)"],
                "MDD(%)": s["MDD(%)"], "Calmar": s["Calmar"],
                "Sharpe": s["Sharpe"], "사이클": s["완료 사이클"],
                "승률(%)": s.get("승률(%)", 0.0),
                "보유비중(%)": s["보유비중(%)"],
            })
            if i % step == 0:
                prog.progress((i + 1) / len(combos),
                              text=f"백테스트 실행 중... {i + 1:,}/{len(combos):,}")
        prog.empty()
        st.session_state["tq_opt_result"] = pd.DataFrame(rows)
        st.session_state["tq_opt_meta"] = (
            f"{ticker} · {params['bt_start_date']} ~ {params['bt_end_date']} · "
            f"{params['price_mode']}")

    out = st.session_state.get("tq_opt_result")
    if out is None or out.empty:
        return
    st.caption(f"결과 기준: {st.session_state.get('tq_opt_meta', '')}")
    # MDD 는 음수 → 큰 값(0 에 가까운 값)이 좋으므로 전부 내림차순
    out = out.sort_values(sort_key, ascending=False).reset_index(drop=True)

    cur = out[(out["MA"] == params["ma_window"])
              & (out["하한(%)"] == params["buy_lo"])
              & (out["상한(%)"] == params["buy_hi"])
              & (out["청산(%)"] == params["exit_disp"])
              & (out["익절(%)"] == (f"{params['tp_disp']:g}"
                                   if params["use_tp"] else _TP_OFF))]
    if len(cur):
        st.info(f"📍 현재 사이드바 세팅은 {len(out):,}개 중 **{cur.index[0] + 1}위** "
                f"({sort_key} 기준) — CAGR {cur['CAGR(%)'].iloc[0]:.1f}% · "
                f"MDD {cur['MDD(%)'].iloc[0]:.1f}%")

    fig = go.Figure(go.Scatter(
        x=out["MDD(%)"], y=out["CAGR(%)"], mode="markers",
        marker=dict(size=7, color=out["Calmar"], colorscale="Viridis",
                    showscale=True, colorbar=dict(title="Calmar")),
        text=[f"MA{ma} · 매수 {lo:g}~{hi:g}% · 청산 {ex:g}% · 익절 {tp}"
              for ma, lo, hi, ex, tp in zip(out["MA"], out["하한(%)"],
                                            out["상한(%)"], out["청산(%)"],
                                            out["익절(%)"])],
        hovertemplate="%{text}<br>MDD %{x:.1f}% · CAGR %{y:.1f}%<extra></extra>"))
    if len(cur):
        fig.add_trace(go.Scatter(
            x=cur["MDD(%)"], y=cur["CAGR(%)"], mode="markers", name="현재 세팅",
            marker=dict(size=16, color="rgba(0,0,0,0)",
                        line=dict(color=_COL_STRAT, width=3)),
            hoverinfo="skip"))
    fig.update_layout(height=380, margin=dict(l=10, r=10, t=40, b=10),
                      title_text="조합별 CAGR vs MDD (우상단일수록 우수)",
                      xaxis_title="MDD (%)", yaxis_title="CAGR (%)",
                      showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(out, use_container_width=True, height=460)
    st.download_button("⬇️ 결과 CSV", out.to_csv(index=False).encode("utf-8-sig"),
                       file_name=f"TQ200_{ticker}_grid.csv", mime="text/csv",
                       key="tq_dl_grid")
    st.caption("⚠️ 사이클이 20~30회뿐인 전략이라 상위 조합은 과최적화일 가능성이 "
               "높습니다. 1위 한 점보다 **주변 조합도 고르게 좋은 영역**을 고르세요.")


# ══════════════════════════════════════════════
# 오늘의 신호 탭
# ══════════════════════════════════════════════
def _tg_creds(cfg: dict):
    """TQ200 전용 텔레그램 (개인 설정 탭에서 저장 — tq200_config 안에 보관)."""
    return (str(cfg.get("tg_token", "")).strip(),
            str(cfg.get("tg_chat_id", "")).strip())


def render_ordersheet_tab(params: dict):
    ticker = params["bt_ticker"]
    st.subheader(f"📋 오늘의 신호 — {ticker}")
    if not ticker:
        st.warning("티커를 입력해주세요.")
        return
    cfg = _load_cfg()
    pos_all = cfg.get("positions", {})
    pos = pos_all.get(ticker, {})

    # 신호는 사이드바 종료일과 무관하게 항상 최신 확정 종가 기준
    df = _get_data(params, end=date.today())
    if df.empty:
        st.error(f"{ticker} 가격 데이터를 불러오지 못했습니다. 최신 종가 없이 신호를 "
                 "만들면 잘못된 주문이 나올 수 있어 표시하지 않습니다.")
        return

    # ── 내 포지션 ──
    st.markdown("**① 내 실제 포지션**")
    p1, p2, p3, p4, p5 = st.columns([1.5, 1, 1, 1, 1])
    holding = p1.radio("현재 상태", ["무포지션 (현금)", "보유 중"],
                       index=1 if pos.get("holding") else 0, horizontal=True,
                       key=f"tq_pos_state_{ticker}") == "보유 중"
    shares = p2.number_input("보유 수량 (주)", min_value=0, step=1,
                             value=int(pos.get("shares", 0)),
                             key=f"tq_pos_sh_{ticker}", disabled=not holding)
    avg_px = p3.number_input("매수 단가 ($)", min_value=0.0, step=0.01,
                             value=float(pos.get("avg_price", 0.0)),
                             format="%.2f", key=f"tq_pos_px_{ticker}",
                             disabled=not holding)
    cash = p4.number_input("예수금 ($)", min_value=0.0, step=100.0,
                           value=float(pos.get("cash", params["capital"])),
                           format="%.2f", key=f"tq_pos_cash_{ticker}")
    gap_buf = p5.number_input(
        "MOO 갭 여유 (%)", min_value=0.0, max_value=20.0, step=0.5,
        value=float(pos.get("gap_buf", 3.0)), format="%.1f",
        key=f"tq_pos_gap_{ticker}",
        help="MOO 는 시가에 체결되므로 전일 종가보다 높게 시작하면 예수금이 모자랄 수 "
             "있습니다. 종가 × (1 + 여유%) 기준으로 수량을 계산해 미수·주문 거부를 "
             "방지합니다. 0 이면 종가 기준 최대 수량.")
    if st.button("💾 포지션 저장", key=f"tq_pos_save_{ticker}"):
        pos_all[ticker] = {"holding": bool(holding), "shares": int(shares),
                           "avg_price": float(avg_px), "cash": float(cash),
                           "gap_buf": float(gap_buf)}
        cfg["positions"] = pos_all
        _save_cfg(cfg)
        st.success("저장 완료")

    sig = latest_signal(df, holding=holding, **_engine_kwargs(params))
    if not sig:
        st.error("이동평균 계산에 필요한 데이터가 부족합니다.")
        return
    base_day = sig["기준일"].date()
    next_day = next_trading_date(base_day + timedelta(days=1))
    close = sig["종가"]

    st.divider()
    st.markdown(f"**② 신호** — 기준 종가 **{base_day}** · 주문일 **{next_day}** "
                f"(개장 전 MOO 주문 → 시가 체결)")
    _stale_days = (date.today() - base_day).days
    if _stale_days > 5:
        st.warning(f"⚠️ 마지막 확정 종가가 {_stale_days}일 전({base_day})입니다. "
                   "데이터가 최신인지 확인하세요.")

    d_pct = sig["이격도"] * 100
    st.markdown(_stat_cards([
        ("종가", f"${close:,.2f}", str(base_day), "#888"),
        (f"{params['ma_window']}MA", f"${sig['200MA']:,.2f}", "", "#888"),
        ("이격도", f"{d_pct:+.2f}%",
         "매수 구간 ✓" if sig["매수조건"] else "매수 구간 밖",
         _COL_BUY if sig["매수조건"] else "#888"),
        (f"MA 기울기 ({params['slope_window']}일)", f"{sig['Slope']:+.4f}",
         ("상승 ✓" if sig["Slope"] > 0 else "하락/횡보")
         + ("" if params["use_slope"] else " (필터 OFF)"),
         _COL_BUY if sig["Slope"] > 0 else _COL_SELL),
    ], tone="blue"), unsafe_allow_html=True)

    # 화면에 입력된 값(저장 전이어도) 기준으로 주문 계획 계산
    cur_pos = {"holding": bool(holding), "shares": int(shares),
               "avg_price": float(avg_px), "cash": float(cash),
               "gap_buf": float(gap_buf)}
    plan = _order_plan(ticker, sig, cur_pos, next_day)
    action, order_rows = plan["action"], plan["rows"]
    _box = {"buy": st.success, "sell": st.error}.get(action, st.info)
    _box(f"### {plan['headline']}\n{plan['detail']}")

    st.markdown(f"**📋 {next_day} 주문표**")
    if order_rows:
        st.dataframe(pd.DataFrame(order_rows), hide_index=True,
                     use_container_width=True)
        if action == "sell" and int(shares) <= 0:
            st.warning("⚠️ 보유 수량이 0 입니다. ① 에 실제 보유 수량을 입력하세요.")
        st.caption("MOO(Market On Open) = 개장 동시호가에 시장가로 체결되는 주문. "
                   "정규장 개장(한국시간 23:30, 서머타임 22:30) 전에 접수해야 합니다.")
    else:
        st.dataframe(pd.DataFrame([{
            "구분": "—", "종목": ticker, "수량": "0",
            "기준가(전일 종가)": f"${close:,.2f}", "예상 금액": "—",
            "비고": "주문 없음 — " + sig["reason"]}]),
            hide_index=True, use_container_width=True)

    # ── 다음 종가 트리거 ──
    st.markdown(f"**③ {next_day} 종가가 얼마면 신호가 바뀌나** "
                "(그날 종가가 이동평균에 반영되는 것까지 계산한 값)")
    rows = []
    # 주문이 체결된 뒤의 포지션 기준 (매수 체결 → 보유, 매도 체결 → 무포지션)
    will_hold = (action == "buy") or (holding and action != "sell")
    if will_hold:
        rows.append({"신호": f"🔴 청산 (이격도 < {params['exit_disp']:g}%)",
                     "종가 조건": f"${sig['다음_청산가']:,.2f} 미만",
                     "현재가 대비": f"{(sig['다음_청산가'] / close - 1) * 100:+.2f}%"})
        if params["use_tp"]:
            rows.append({"신호": f"🟠 과열 익절 (이격도 > {params['tp_disp']:g}%)",
                         "종가 조건": f"${sig['다음_익절가']:,.2f} 초과",
                         "현재가 대비": f"{(sig['다음_익절가'] / close - 1) * 100:+.2f}%"})
    else:
        rows.append({
            "신호": f"🟢 매수 구간 (이격도 {params['buy_lo']:g}%~{params['buy_hi']:g}%)",
            "종가 조건": f"${sig['다음_매수하한']:,.2f} 초과 ~ "
                       f"${sig['다음_매수상한']:,.2f} 미만",
            "현재가 대비": f"{(sig['다음_매수하한'] / close - 1) * 100:+.2f}% ~ "
                        f"{(sig['다음_매수상한'] / close - 1) * 100:+.2f}%"})
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
    if not will_hold and params["use_slope"] and sig["Slope"] <= 0:
        st.caption("※ 현재 이동평균 기울기가 0 이하라, 종가가 매수 구간에 들어와도 "
                   "기울기가 양수로 돌아서기 전에는 매수하지 않습니다.")

    # ── 백테스트 모델 상태와 비교 ──
    model = run_backtest(df, initial_capital=params["capital"],
                         start=str(params["bt_start_date"]),
                         fee_rate=params["fee"] / 100, **_engine_kwargs(params))
    mlog = model["log"]
    if not mlog.empty:
        m_hold = bool(mlog["보유수량"].iloc[-1] > 0)
        if m_hold != holding:
            st.warning(
                "⚠️ 백테스트 모델은 현재 **"
                + ("보유 중" if m_hold else "무포지션")
                + "** 인데 입력한 실제 포지션은 **"
                + ("보유 중" if holding else "무포지션")
                + "** 입니다. 위 신호는 **실제 포지션 기준**으로 계산했습니다. "
                  "(모델과 다르게 진입/청산했다면 정상)")
        with st.expander("📜 최근 15거래일 로그 (백테스트 모델 기준)"):
            recent = mlog.iloc[-15:][::-1][["시가", "종가", "200MA", "이격도",
                                            "Slope", "매수조건", "이탈", "상태",
                                            "매매"]].copy()
            recent.index = recent.index.date
            recent["이격도"] = recent["이격도"] * 100
            st.dataframe(recent, use_container_width=True, column_config={
                "시가": st.column_config.NumberColumn(format="%.2f"),
                "종가": st.column_config.NumberColumn(format="%.2f"),
                "200MA": st.column_config.NumberColumn(
                    f"{params['ma_window']}MA", format="%.2f"),
                "이격도": st.column_config.NumberColumn("이격도(%)", format="%.2f"),
                "Slope": st.column_config.NumberColumn(format="%.4f"),
            })

    # ── 텔레그램 ──
    st.divider()
    tok, chat = _tg_creds(cfg)
    msg = _order_text(ticker, params, sig, plan, cur_pos, next_day)
    if tok and chat:
        if st.button("📨 텔레그램으로 주문표 전송", key="tq_tg_send"):
            r = _send_telegram(tok, chat, msg)
            if r.get("ok"):
                st.success("전송 완료")
            else:
                st.error(f"전송 실패: {r.get('description', '알 수 없는 오류')}")
    else:
        st.caption("📨 텔레그램 전송은 [⚙️ 개인 설정] 탭의 **텔레그램 알림 설정**에서 "
                   "Chat ID · Bot Token 을 저장하면 사용할 수 있습니다.")


# ══════════════════════════════════════════════
# 전략 소개 탭
# ══════════════════════════════════════════════
def render_intro_tab(params: dict):
    st.subheader("📖 TQ200 — 200일선 이격도 매매란")
    st.markdown("""
**3배 레버리지 ETF(TQQQ)를 200일 이동평균 위 상승 추세에서만 보유하고, 추세가 꺾이면
현금으로 물러나는 추세추종 전략.** 하루 한 번 종가만 확인하고, 신호가 나온 날에만
다음 날 개장 전 **MOO(시가 시장가)** 주문을 한 번 내면 끝나는 단순한 구조입니다.
판단 기준은 **TQQQ 자체의 200일선**입니다 (나스닥·QQQ 의 이동평균이 아님).

#### 지표
| 지표 | 계산 | 의미 |
|---|---|---|
| **200MA** | 종가 200일 단순이동평균 | 장기 추세선 |
| **이격도** | 종가 ÷ 200MA − 1 | 추세선에서 얼마나 떨어져 있나 |
| **Slope** | (200MA − 5거래일 전 200MA) ÷ 5 | 추세선이 오르는 중인가 |

#### 규칙 (기본값)
| 구분 | 조건 (종가 확정 후 판단) | 실행 |
|---|---|---|
| 🟢 **매수** | 무포지션 + 이격도 **1% 초과 10% 미만** + Slope **> 0** | 다음 거래일 **MOO 전량 매수** |
| 🔴 **청산** | 보유 중 + 이격도 **−1% 미만**으로 이탈 | 다음 거래일 **MOO 전량 매도** |
| 🟠 **과열 익절** | 보유 중 + 이격도 **50% 초과** | 다음 거래일 **MOO 전량 매도** |

예) 200일선이 $100 이면 — 종가 $101 초과 ~ $110 미만에서 매수, $99 미만이면 청산,
$150 초과면 과열 익절. 매도는 구간에 머무는 동안 매일 나오는 게 아니라 경계선을
**처음 넘는 날 한 번** 발생합니다.

#### 왜 이렇게 설계됐나
- **이격도 1% 초과** — 200일선을 살짝 넘은 정도의 가짜 돌파(휩쏘)를 걸러냅니다.
- **이격도 10% 미만** — 이미 많이 오른 뒤의 추격 매수를 막습니다. 과열 익절 후에는
  가격이 200일선 근처로 **눌릴 때까지 기다렸다가** 재진입하게 됩니다.
- **Slope > 0** — 200일선 자체가 하락 중이면 이격도가 맞아도 사지 않습니다
  (하락 추세 속 반등에 물리는 것 방지).
- **−1% 이탈 청산** — 200일선 아래로 확실히 내려가면 추세 종료로 보고 물러납니다.
  3배 레버리지는 하락장에서 −80% 이상 녹을 수 있어 이 손절이 전략의 핵심입니다.
- **50% 과열 익절** — 200일선과 50% 넘게 벌어지면 평균 회귀 위험이 커 일단 수익을
  확정합니다. 원본 시트는 55% 였는데, 50~55% 가 성과가 비슷한 구간이고 55% 는 그
  위쪽 끝(57.5% 부터 성과 급락)이라 안쪽인 50% 로 옮겼습니다. 16년간 익절은 5~7번뿐이라
  50% 와 55% 의 성과 차이 자체는 의미 있는 수준이 아닙니다.

#### 성격 — 꼭 알고 시작할 것
- **승률은 절반 정도입니다.** 횡보장에서는 −4~−8% 손절이 여러 번 연속됩니다.
  대신 큰 추세 한 번(+100~200%)이 그 손실을 모두 덮는 구조입니다.
- **보유 중 낙폭도 큽니다.** 200일선 위에서도 TQQQ는 −20~−30% 조정이 흔합니다.
- 신호가 나오면 **예외 없이 실행**해야 통계가 성립합니다. 손절 몇 번 뒤 다음 매수
  신호를 건너뛰면, 그게 큰 추세의 시작일 수 있습니다.
""")

    st.divider()
    st.subheader("🧪 이 앱의 백테스트 가정")
    st.markdown("""
- 신호는 **당일 종가 확정 후** 판단, 체결은 **다음 거래일 시가 = MOO 체결가** (전량, 소수점 주식 허용 · 복리)
- 시가 매도한 날 종가에 매수 조건이 맞으면 그날 매수 신호 → 다음 날 재진입
- 시가 매수한 날 종가가 청산선을 이탈하면 그날 매도 신호 → 다음 날 청산 (1일 보유 가능)
- 현금 보유 구간의 이자는 반영하지 않음 (실제로는 MMF·단기채 수익이 추가됨)
- **가격 기준 = 무조정 종가**(기본) — 증권사 차트의 200일선과 동일하며 원본 엑셀과 일치.
  분할은 반영, 배당은 미반영 (TQQQ 배당은 연 1% 안팎으로 영향이 작음)
- 수수료·슬리피지는 사이드바에서 편도 %로 지정 (기본 0)
""")

    st.divider()
    st.subheader("✅ 원본 엑셀과 대조 검증")
    st.markdown("""
원본 `TQQQ200일선.xlsx` (2011-01-03 ~ 2026-11-06) 와 이 앱의 엔진을 무조정 종가 ·
**원본 시트 값(과열 익절 55%)** 으로 대조한 결과:

| 항목 | 결과 |
|---|---|
| 일별 상태·매매 (3,986거래일) | **불일치 0건** |
| 사이클 시작일·종료일·청산 사유 (26사이클) | **전부 일치** |
| 사이클 수익률 · MDD | 최대 오차 0.03%p (1달러 미만 구간의 가격 소수점 차이) |
| 최종 누적수익률 | 엑셀 31,880.65% / 엔진 31,895.24% |

재현: `14.TQ200/verify_tq200.py --raw`

외부 백테스트 서비스 결과와도 대조했습니다 (익절 55% 기준) — CAGR 43.9% · MDD −31.1% ·
Calmar 1.41 · Sharpe 1.17 · 승률 53.8% · 평균 이익 +67.4% / 평균 손실 −5.3% ·
평균 보유 94.6일, 연도별·월별 수익률까지 동일합니다.

#### 앱 기본값(익절 50%)과 원본(55%) 비교 — 같은 기간
| 항목 | 원본 55% | 앱 기본값 50% |
|---|---|---|
| CAGR | 43.9% | 45.3% |
| MDD | −31.1% | −31.1% |
| Calmar / Sharpe | 1.41 / 1.17 | 1.46 / 1.23 |
| 사이클 / 과열 익절 | 26회 / 5회 | 28회 / 7회 |

원본과 똑같이 돌리려면 사이드바 **익절 이격도**를 55 로 바꾸면 됩니다.
""")
    st.caption("현재 사이드바 세팅 — " + _rule_text(params))

    st.divider()
    _render_perf_analysis(params)


# ══════════════════════════════════════════════
# 전략 성과 분석 (전략 소개 탭 하단 — DSS '전략 성과 분석'과 같은 구성)
# ══════════════════════════════════════════════
_PERF_SRC = ["📐 사이드바 설정값", "🎯 앱 기본값 (익절 50%)", "📜 원본 시트 (익절 55%)"]
_STRAT_KEYS = ("ma_window", "slope_window", "buy_lo", "buy_hi", "exit_disp",
               "tp_disp", "use_slope", "use_tp")
# 민감도 분석 축: 라벨 → (세로축 키, 가로축 키)
_SENS_AXES = {
    "청산 이격도 × 익절 이격도": ("exit_disp", "tp_disp"),
    "매수 하한 × 매수 상한": ("buy_lo", "buy_hi"),
    "이동평균 × 익절 이격도": ("ma_window", "tp_disp"),
}
_SENS_STEP = {"exit_disp": 0.5, "tp_disp": 2.5, "buy_lo": 0.5, "buy_hi": 1.0,
              "ma_window": 10}
_SENS_NAME = {"exit_disp": "청산 이격도(%)", "tp_disp": "익절 이격도(%)",
              "buy_lo": "매수 하한(%)", "buy_hi": "매수 상한(%)",
              "ma_window": "이동평균(일)"}


def _perf_params(params: dict, src_idx: int) -> dict:
    """파라미터 소스 선택 → 분석에 쓸 params (기간·자본·수수료·가격 기준은 사이드바 유지)."""
    P = dict(params)
    if src_idx == 1:
        P.update({k: DEFAULTS[k] for k in _STRAT_KEYS})
    elif src_idx == 2:
        P.update({k: SHEET_PARAMS[k] for k in _STRAT_KEYS})
    return P


def _bt_for(df: pd.DataFrame, P: dict, start=None, end=None) -> dict:
    return run_backtest(df, initial_capital=P["capital"], start=start, end=end,
                        fee_rate=P["fee"] / 100, **_engine_kwargs(P))


def _ret_mdd(assets: np.ndarray, base: float):
    """(총수익률 %, MDD %) — 시작 자본을 첫 고점으로 포함."""
    full = np.concatenate([[base], assets])
    mdd = float((full / np.maximum.accumulate(full) - 1.0).min()) * 100
    return (float(assets[-1]) / base - 1.0) * 100, mdd


def _dd_episodes(dates, assets: np.ndarray, threshold: float) -> list:
    """고점 대비 threshold(%) 이상 하락한 에피소드 — 고점·저점·회복 시점."""
    recs = []
    peak_v, peak_i = float(assets[0]), 0
    in_dd, trough_v, trough_i = False, peak_v, 0

    def _rec(end_i, recovered):
        return {
            "고점": str(dates[peak_i].date()), "고점 자산": f"${peak_v:,.0f}",
            "최대하락 시점": str(dates[trough_i].date()),
            "저점 자산": f"${trough_v:,.0f}",
            "하락율": f"{(trough_v / peak_v - 1) * 100:.1f}%",
            "회복 시점": str(dates[end_i].date()) if recovered else "미회복 ⏳",
            "기간(일)": (dates[end_i] - dates[peak_i]).days,
        }

    for i in range(1, len(assets)):
        cur = float(assets[i])
        if not in_dd:
            if cur > peak_v:
                peak_v, peak_i = cur, i
            elif (cur / peak_v - 1) * 100 <= -threshold:
                in_dd, trough_v, trough_i = True, cur, i
        else:
            if cur < trough_v:
                trough_v, trough_i = cur, i
            if cur >= peak_v:
                recs.append(_rec(i, True))
                in_dd, peak_v, peak_i = False, cur, i
                trough_v, trough_i = cur, i
    if in_dd:
        recs.append(_rec(len(assets) - 1, False))
    return recs


def _underwater_periods(dd: pd.Series) -> pd.DataFrame:
    """낙폭 시계열(%) → 고점~회복 구간 목록 (미회복 구간 포함)."""
    rows, start = [], None
    for d, v in dd.items():
        if v < 0 and start is None:
            start = d
        elif v >= 0 and start is not None:
            rows.append({"시작일": str(start.date()), "회복일": str(d.date()),
                         "기간(일)": (d - start).days,
                         "최대낙폭(%)": round(float(dd[start:d].min()), 2)})
            start = None
    if start is not None:
        rows.append({"시작일": str(start.date()), "회복일": "미회복 ⏳",
                     "기간(일)": (dd.index[-1] - start).days,
                     "최대낙폭(%)": round(float(dd[start:].min()), 2)})
    return pd.DataFrame(rows)


def _flat_spells(log: pd.DataFrame) -> list:
    """현금(무포지션) 구간의 길이(거래일) 목록."""
    held = (log["보유수량"] > 0).to_numpy()
    spells, n = [], 0
    for h in held:
        if not h:
            n += 1
        elif n:
            spells.append(n)
            n = 0
    if n:
        spells.append(n)
    return spells


def _usd(v: float) -> str:
    """부호 있는 달러 금액 — +$1,234 / -$1,234."""
    return f"{'+' if v >= 0 else '-'}${abs(v):,.0f}"


def _color_pm(val):
    if isinstance(val, (int, float)) and not pd.isna(val):
        if val > 0:
            return "color: #2E7D32; font-weight:bold"
        if val < 0:
            return "color: #C62828; font-weight:bold"
    return ""


def _render_perf_analysis(params: dict):
    """전략 소개 탭 하단의 성과 분석. 버튼을 눌러야 계산한다 (탭 전환 때마다 재계산 방지)."""
    st.subheader("📊 전략 성과 분석")
    st.markdown("사이드바의 파라미터와 기간 설정을 기반으로 성과를 분석합니다.")
    ticker = params["bt_ticker"]
    if not ticker:
        st.warning("티커를 입력해주세요.")
        return

    src_sel = st.radio("파라미터 소스", _PERF_SRC, index=0, horizontal=True,
                       key="tq_intro_param_src")
    P = _perf_params(params, _PERF_SRC.index(src_sel))
    capital = float(P["capital"])
    start_s, end_s = str(P["bt_start_date"]), str(P["bt_end_date"])

    with st.expander("🔍 적용 파라미터 확인", expanded=False):
        pv1, pv2 = st.columns(2)
        pv1.markdown(
            f"**🟢 매수** — 이격도 {P['buy_lo']:g}% 초과 · {P['buy_hi']:g}% 미만"
            + (f" · {P['ma_window']}MA 기울기({P['slope_window']}일) > 0"
               if P["use_slope"] else " · 기울기 필터 OFF"))
        pv2.markdown(
            f"**🔴 매도** — 이격도 {P['exit_disp']:g}% 미만 이탈"
            + (f" · {P['tp_disp']:g}% 초과 과열 익절" if P["use_tp"]
               else " · 과열 익절 OFF"))
        st.caption(f"{ticker} · 이동평균 {P['ma_window']}일 · 수수료 {P['fee']:g}% · "
                   f"{P['price_mode']} · 기간 {start_s} ~ {end_s} · "
                   f"자본 ${capital:,.0f}")

    if st.button("▶ 성과 분석 실행", type="primary", key="tq_run_intro_perf"):
        with st.spinner("데이터 로드 및 분석 중..."):
            df = _get_data(P)
            if df.empty:
                st.error(f"⚠️ {ticker} 가격 데이터를 불러오지 못했습니다.")
                return
            res = _bt_for(df, P, start_s, end_s)
        if res["log"].empty:
            st.error("분석 구간에 데이터가 없습니다. 시작일/종료일을 확인하세요.")
            return
        st.session_state["tq_intro_bt"] = {"res": res, "P": P, "label": src_sel,
                                           "ticker": ticker}
        # 이전 분석의 민감도·강건성 결과는 기준이 달라졌으므로 지운다
        st.session_state.pop("tq_sens", None)
        st.session_state.pop("tq_mc_result", None)

    saved = st.session_state.get("tq_intro_bt")
    if not saved:
        st.info("👆 '성과 분석 실행' 버튼을 클릭하면 현재 파라미터 기준의 성과 분석 결과를 "
                "확인할 수 있습니다.\n\n"
                "💡 사이드바 설정값, 앱 기본값(익절 50%), 원본 시트(익절 55%) 중에서 "
                "골라 비교 분석할 수 있습니다.")
        return

    res, P, ticker = saved["res"], saved["P"], saved["ticker"]
    log, cyc = res["log"], res["cycles"]
    capital = float(P["capital"])
    st.caption(f"📌 분석 기준: **{saved['label']}** · {ticker} · "
               f"{log.index[0].date()} ~ {log.index[-1].date()} "
               f"({len(log):,} 거래일) · " + _rule_text(P))

    # ── 핵심 지표 ──
    dates = log.index
    assets = log["평가금"].to_numpy(dtype=float)
    close = log["종가"].to_numpy(dtype=float)
    final = float(assets[-1])
    years = max((dates[-1] - dates[0]).days / 365.25, 1e-9)
    total_ret = final / capital - 1
    cagr = (final / capital) ** (1 / years) - 1 if final > 0 else -1.0
    full = np.concatenate([[capital], assets])
    dd_arr = (full / np.maximum.accumulate(full) - 1.0)[1:] * 100
    mdd = float(dd_arr.min()) / 100
    calmar = abs(cagr / mdd) if mdd != 0 else 0.0
    rets = np.diff(full) / full[:-1]
    sharpe = (rets.mean() / rets.std() * np.sqrt(252)) if rets.std() > 0 else 0.0
    neg = rets[rets < 0]
    sortino = (rets.mean() / neg.std() * np.sqrt(252)) \
        if len(neg) > 1 and neg.std() > 0 else 0.0

    bh_assets = capital * close / close[0]
    bh_final = float(bh_assets[-1])
    bh_ret = bh_final / capital - 1
    bh_cagr = (bh_final / capital) ** (1 / years) - 1
    bh_dd = (bh_assets / np.maximum.accumulate(bh_assets) - 1.0) * 100
    bh_mdd = float(bh_dd.min()) / 100

    _c_ret = _COL_BUY if total_ret >= 0 else _COL_SELL
    st.markdown(_stat_cards([
        ("CAGR", f"{cagr * 100:.2f}%", f"B&H {bh_cagr * 100:.1f}%", "#888"),
        ("총 수익률", f"{total_ret * 100:+,.1f}%", f"B&H {bh_ret * 100:+,.0f}%", _c_ret),
        ("최대 MDD", f"{mdd * 100:.2f}%", f"B&H {bh_mdd * 100:.1f}%", _COL_SELL),
        ("Calmar Ratio", f"{calmar:.3f}", "CAGR ÷ |MDD|", "#888"),
        ("Sharpe Ratio", f"{sharpe:.3f}", "", "#888"),
        ("Sortino Ratio", f"{sortino:.3f}", "", "#888"),
    ], tone="blue"), unsafe_allow_html=True)
    st.divider()

    # ── 연도별 성과 ──
    st.subheader("📅 연도별 성과")
    df_year = yearly_returns(log["평가금"], capital, log["종가"]).rename(
        columns={"수익률(%)": "연간수익률(%)", "B&H(%)": "B&H 수익률(%)"})
    df_year["초과수익(%p)"] = (df_year["연간수익률(%)"]
                            - df_year["B&H 수익률(%)"]).round(2)
    _held = (log["보유수량"] > 0)
    df_year["보유비중(%)"] = [
        round(float(_held[_held.index.year == y].mean()) * 100, 0)
        for y in df_year["연도"]]
    st.dataframe(
        df_year.style
        .map(_color_pm, subset=["연간수익률(%)", "B&H 수익률(%)", "초과수익(%p)"])
        .format({"연간수익률(%)": "{:+.2f}%", "MDD(%)": "{:.2f}%",
                 "B&H 수익률(%)": "{:+.2f}%", "초과수익(%p)": "{:+.2f}",
                 "보유비중(%)": "{:.0f}%"}),
        hide_index=True, use_container_width=True,
        height=min(38 + 35 * len(df_year), 640))
    _n_up = int((df_year["연간수익률(%)"] > 0).sum())
    _n_beat = int((df_year["초과수익(%p)"] > 0).sum())
    st.caption(f"{len(df_year)}개 연도 중 플러스 {_n_up}회 · B&H 초과 {_n_beat}회. "
               "강세장에서는 B&H 에 뒤지고, 하락장을 피한 해에 크게 앞서는 것이 "
               "추세추종의 전형적인 모습입니다.")
    st.divider()

    # ── 월별 수익률 히트맵 ──
    st.subheader("🗓️ 월별 수익률 히트맵")
    _mode = pd.Series(np.where(_held, "보유", "현금"), index=dates)
    st.markdown(monthly_perf_table(log["평가금"], _mode, vmax=20.0,
                                   mode_short={"보유": "보유", "현금": "현금"},
                                   idle_label="현금"),
                unsafe_allow_html=True)
    st.divider()

    # ── 총자산 추이 & 낙폭 ──
    st.subheader("📈 총자산 추이 & 낙폭")
    fig_asset = make_subplots(rows=2, cols=1, shared_xaxes=True,
                              row_heights=[0.7, 0.3], vertical_spacing=0.05)
    fig_asset.add_trace(go.Scatter(x=dates, y=assets, mode="lines", name="총자산",
                                   line=dict(color=_COL_STRAT, width=1.5)),
                        row=1, col=1)
    fig_asset.add_trace(go.Scatter(x=dates, y=dd_arr, mode="lines", name="낙폭(%)",
                                   fill="tozeroy",
                                   fillcolor="rgba(198,40,40,0.15)",
                                   line=dict(color="#C62828", width=1)),
                        row=2, col=1)
    fig_asset.update_layout(height=500, showlegend=True,
                            legend=dict(orientation="h", y=1.02),
                            margin=dict(l=60, r=20, t=40, b=30),
                            hovermode="x unified")
    fig_asset.update_yaxes(title_text="총자산 ($, 로그)", type="log", row=1, col=1)
    fig_asset.update_yaxes(title_text="낙폭 (%)", row=2, col=1)
    st.plotly_chart(fig_asset, use_container_width=True)
    st.divider()

    # ── 이격도 & 포지션 변화 (DSS 의 'RSI & 모드 변화'에 해당) ──
    st.subheader("🔄 이격도 & 포지션 변화")
    st.caption("전략이 보는 유일한 지표인 200일선 이격도와, 그에 따라 보유/현금이 "
               "어떻게 바뀌었는지 보여줍니다.")
    disp = log["이격도"] * 100
    fig_pos = go.Figure()
    fig_pos.add_trace(go.Scatter(x=dates, y=disp, mode="lines", name="이격도",
                                 line=dict(color="#777", width=1)))
    fig_pos.add_trace(go.Scatter(x=dates[_held.to_numpy()], y=disp[_held],
                                 mode="markers", name="보유",
                                 marker=dict(color=_COL_BUY, size=3, opacity=0.7)))
    fig_pos.add_trace(go.Scatter(x=dates[~_held.to_numpy()], y=disp[~_held],
                                 mode="markers", name="현금",
                                 marker=dict(color="#1565C0", size=3, opacity=0.5)))
    # 청산선과 매수 하한은 붙어 있어 라벨이 겹친다 → 하나는 아래, 하나는 위로
    _lvls = [(P["exit_disp"], _COL_SELL, f"청산 {P['exit_disp']:g}%", "bottom right"),
             (P["buy_lo"], _COL_BUY, f"매수 하한 {P['buy_lo']:g}%", "top right"),
             (P["buy_hi"], _COL_BUY, f"매수 상한 {P['buy_hi']:g}%", "top right")]
    if P["use_tp"]:
        _lvls.append((P["tp_disp"], _COL_MA, f"익절 {P['tp_disp']:g}%", "top right"))
    for lvl, clr, nm, pos in _lvls:
        fig_pos.add_hline(y=lvl, line_dash="dot", line_color=clr, opacity=0.6,
                          annotation_text=nm, annotation_position=pos)
    fig_pos.update_layout(height=380, showlegend=True,
                          legend=dict(orientation="h", y=1.05),
                          margin=dict(l=50, r=90, t=40, b=30),
                          yaxis_title="이격도 (%)")
    st.plotly_chart(fig_pos, use_container_width=True)
    st.divider()

    # ── 승률 & 손익비 분석 (사이클 단위) ──
    st.subheader("🎯 승률 & 손익비 분석")
    n_cyc = len(cyc)
    pnl_pct = cyc["수익률(%)"].to_numpy(dtype=float) if n_cyc else np.array([])
    if n_cyc:
        # 사이클 손익($) = 매도 직후 현금 − 그 사이클에 투입한 금액
        proceeds = np.array([float(log.at[d, "현금"]) for d in cyc["종료일"]])
        pnl_usd = proceeds - proceeds / (1 + pnl_pct / 100)
        wins, losses = pnl_pct[pnl_pct > 0], pnl_pct[pnl_pct <= 0]
        win_rate = len(wins) / n_cyc * 100
        avg_win = float(wins.mean()) if len(wins) else 0.0
        avg_loss = abs(float(losses.mean())) if len(losses) else 0.0
        payoff = avg_win / avg_loss if avg_loss > 0 else float("inf")
        total_pnl = float(pnl_usd.sum())
        st.markdown(_stat_cards([
            ("총 사이클", f"{n_cyc}회", f"{len(wins)}승 {len(losses)}패", "#888"),
            ("승률", f"{win_rate:.1f}%", "", _COL_BUY),
            ("평균 익절", f"{avg_win:+.1f}%", f"최대 {pnl_pct.max():+.1f}%", _COL_BUY),
            ("평균 손절", f"-{avg_loss:.1f}%", f"최대 {pnl_pct.min():+.1f}%", _COL_SELL),
            ("손익비", f"{payoff:.2f}", "평균 익절 ÷ 평균 손절", "#888"),
            ("누적 실현손익", _usd(total_pnl), "",
             _COL_BUY if total_pnl >= 0 else _COL_SELL),
        ]), unsafe_allow_html=True)

        fig_cyc = go.Figure(go.Bar(
            x=[f"#{int(n)}" for n in cyc["사이클"]], y=pnl_pct,
            marker_color=[_COL_BUY if v > 0 else _COL_SELL for v in pnl_pct],
            customdata=np.stack([cyc["시작일"].dt.strftime("%Y-%m-%d"),
                                 cyc["종료일"].dt.strftime("%Y-%m-%d"),
                                 cyc["보유일"], cyc["청산사유"]], axis=-1),
            hovertemplate="%{x} · %{customdata[0]} ~ %{customdata[1]}<br>"
                          "수익률 %{y:+.2f}% · 보유 %{customdata[2]}일 · "
                          "%{customdata[3]}<extra></extra>"))
        fig_cyc.add_hline(y=0, line_color="#888", line_width=1)
        fig_cyc.update_layout(height=320, showlegend=False,
                              title_text="사이클별 수익률 (시간순)",
                              xaxis_title="사이클", yaxis_title="수익률 (%)",
                              margin=dict(l=50, r=20, t=50, b=30))
        st.plotly_chart(fig_cyc, use_container_width=True)
        st.caption("손실은 작고 잦게, 수익은 크고 드물게 납니다. 승률보다 손익비가 "
                   "성과를 만드는 구조라, 연속 손절 구간을 건너뛰지 않는 것이 중요합니다.")
    else:
        st.info("완료된 사이클이 없어 승률 분석을 표시할 수 없습니다.")
    st.divider()

    # ── TQ200 vs Buy & Hold ──
    st.subheader("📉 TQ200 vs Buy & Hold 비교")
    st.caption(f"같은 기간 {ticker}를 단순 보유했을 때와 TQ200 전략 성과를 비교합니다.")
    fig_bh = go.Figure()
    fig_bh.add_trace(go.Scatter(x=dates, y=assets, name="TQ200",
                                line=dict(color=_COL_STRAT, width=2)))
    fig_bh.add_trace(go.Scatter(x=dates, y=bh_assets, name=f"Buy & Hold ({ticker})",
                                line=dict(color=_COL_BH, width=2, dash="dot")))
    fig_bh.add_hline(y=capital, line_dash="dash", line_color="#aaa",
                     annotation_text="시작 자본")
    fig_bh.update_layout(
        title=f"전략 수익 {total_ret * 100:+,.1f}% vs B&H {bh_ret * 100:+,.1f}%",
        yaxis_title="자산 ($, 로그)", height=400, hovermode="x unified",
        legend=dict(orientation="h", y=1.08),
        margin=dict(l=60, r=20, t=60, b=30))
    fig_bh.update_yaxes(type="log")
    st.plotly_chart(fig_bh, use_container_width=True)
    st.markdown(_stat_cards([
        ("TQ200", f"${final:,.0f}",
         f"수익률 {total_ret * 100:+,.1f}% · CAGR {cagr * 100:.1f}% · "
         f"MDD {mdd * 100:.1f}%", _COL_STRAT),
        (f"Buy & Hold ({ticker})", f"${bh_final:,.0f}",
         f"수익률 {bh_ret * 100:+,.1f}% · CAGR {bh_cagr * 100:.1f}% · "
         f"MDD {bh_mdd * 100:.1f}%", "#888"),
    ]), unsafe_allow_html=True)
    st.divider()

    # ── 회복력 분석 ──
    st.subheader("🛡️ 회복력 분석 (15% 이상 하락 에피소드)")
    st.caption("고점 대비 15% 이상 하락한 구간별로, 어디까지 빠지고 얼마 만에 "
               "회복했는지 분석합니다. (3배 레버리지라 10% 조정은 너무 잦아 15% 기준)")
    episodes = _dd_episodes(dates, assets, 15.0)
    if episodes:
        def _clr_recovery(val):
            s = str(val)
            if (s.startswith("-") and "%" in s) or "미회복" in s:
                return "color:#C62828;font-weight:bold"
            return ""
        st.dataframe(pd.DataFrame(episodes).style.map(_clr_recovery),
                     hide_index=True, use_container_width=True)
        _done = [e["기간(일)"] for e in episodes if "미회복" not in e["회복 시점"]]
        if _done:
            st.caption(f"회복 완료 {len(_done)}건 — 고점에서 회복까지 평균 "
                       f"{np.mean(_done):.0f}일 · 최장 {max(_done)}일.")
    else:
        st.info("15% 이상 하락 에피소드가 없습니다.")
    st.divider()

    # ── 청산 사유별 성과 (DSS 의 '모드별 매매 성과'에 해당) ──
    st.subheader("🚪 청산 사유별 성과 (이탈 청산 vs 과열 익절)")
    st.caption("사이클이 어떻게 끝났는지에 따라 수익/손실을 비교합니다.")
    if n_cyc:
        def _reason_stats(mask, name):
            v = pnl_pct[mask]
            if not len(v):
                return {"청산 사유": name, "횟수": 0, "승률": "-", "평균 수익률": "-",
                        "평균 보유일": "-", "총 손익($)": "-"}
            return {"청산 사유": name, "횟수": int(len(v)),
                    "승률": f"{(v > 0).mean() * 100:.1f}%",
                    "평균 수익률": f"{v.mean():+.2f}%",
                    "평균 보유일": f"{cyc['보유일'].to_numpy()[mask].mean():.0f}일",
                    "총 손익($)": _usd(pnl_usd[mask].sum())}
        m_brk = (cyc["청산사유"] == EXIT_BREAK).to_numpy()
        m_tp = (cyc["청산사유"] == EXIT_TP).to_numpy()
        st.dataframe(pd.DataFrame([
            _reason_stats(m_brk, "🔴 이격도 이탈 (추세 종료)"),
            _reason_stats(m_tp, "🟠 이격도 과열 (익절)"),
            _reason_stats(np.ones(n_cyc, dtype=bool), "📊 전체"),
        ]), hide_index=True, use_container_width=True)
        if m_brk.any() and m_tp.any():
            fig_box = go.Figure()
            fig_box.add_trace(go.Box(y=pnl_pct[m_brk], name="이격도 이탈",
                                     marker_color=_COL_SELL, boxmean=True,
                                     boxpoints="all", jitter=0.4))
            fig_box.add_trace(go.Box(y=pnl_pct[m_tp], name="이격도 과열",
                                     marker_color=_COL_MA, boxmean=True,
                                     boxpoints="all", jitter=0.4))
            fig_box.add_hline(y=0, line_dash="dash", line_color="gray")
            fig_box.update_layout(height=350, yaxis_title="사이클 수익률 (%)",
                                  showlegend=False,
                                  margin=dict(l=60, r=20, t=30, b=30))
            st.plotly_chart(fig_box, use_container_width=True)
    else:
        st.info("완료된 사이클이 없습니다.")
    st.divider()

    # ── 드로다운 (Underwater) ──
    st.subheader("🌊 드로다운 (Underwater) 분석")
    st.caption("고점 대비 현재 손실 비율 추이. 얼마나 깊이, 얼마나 오래 손실 구간에 "
               "있었는지 보여줍니다.")
    fig_uw = go.Figure()
    fig_uw.add_trace(go.Scatter(x=dates, y=bh_dd, name="Buy & Hold",
                                line=dict(color=_COL_BH, width=1, dash="dot")))
    fig_uw.add_trace(go.Scatter(x=dates, y=dd_arr, fill="tozeroy", name="TQ200",
                                line=dict(color="#EF5350", width=1),
                                fillcolor="rgba(239,83,80,0.25)"))
    fig_uw.add_hline(y=0, line_color="#888", line_width=1)
    fig_uw.update_layout(yaxis_title="드로다운 (%)", height=300,
                         yaxis=dict(tickformat=".1f"), hovermode="x unified",
                         legend=dict(orientation="h", y=1.1),
                         margin=dict(l=60, r=20, t=30, b=30))
    st.plotly_chart(fig_uw, use_container_width=True)
    uw = _underwater_periods(pd.Series(dd_arr, index=dates))
    if len(uw):
        top5 = uw.nsmallest(5, "최대낙폭(%)").reset_index(drop=True)
        top5.index += 1
        st.markdown("**Top 5 최대 낙폭 구간**")
        st.dataframe(top5.style.format({"최대낙폭(%)": "{:.2f}%"}),
                     hide_index=False, use_container_width=True)
        _uw_days = int((dd_arr < 0).sum())
        st.caption(f"전체 {len(dd_arr):,} 거래일 중 {_uw_days:,}일"
                   f"({_uw_days / len(dd_arr) * 100:.0f}%)이 고점 아래였고, "
                   f"가장 긴 구간은 {int(uw['기간(일)'].max())}일입니다.")
    st.divider()

    # ── 롤링 성과 ──
    st.subheader("📉 롤링 성과 분석")
    st.caption("구간별 성과 추이. 특정 시기에만 좋은 게 아닌지 검증합니다.")
    for rwin, rtab in zip([252, 504, 756],
                          st.tabs(["1년 롤링", "2년 롤링", "3년 롤링"])):
        with rtab:
            if len(assets) <= rwin:
                st.info(f"분석 기간이 {rwin // 252}년보다 짧아 롤링 분석이 불가합니다.")
                continue
            rc, rm = compute_rolling_perf(assets, rwin)
            bc, _bm = compute_rolling_perf(bh_assets, rwin)
            ok = ~np.isnan(rc)
            fig_roll = go.Figure()
            fig_roll.add_trace(go.Scatter(
                x=dates[ok], y=rc[ok], name="TQ200 롤링 CAGR(%)",
                line=dict(color=_COL_STRAT, width=2), yaxis="y1"))
            fig_roll.add_trace(go.Scatter(
                x=dates[ok], y=bc[ok], name="B&H 롤링 CAGR(%)",
                line=dict(color=_COL_BH, width=1, dash="dot"), yaxis="y1"))
            fig_roll.add_trace(go.Scatter(
                x=dates[ok], y=rm[ok], name="TQ200 롤링 MDD(%)",
                line=dict(color="#1565C0", width=1.2, dash="dash"), yaxis="y2"))
            fig_roll.add_hline(y=0, line_dash="dash", line_color="#aaa", yref="y1")
            fig_roll.update_layout(
                yaxis=dict(title="롤링 CAGR (%)", side="left"),
                yaxis2=dict(title="롤링 MDD (%)", side="right", overlaying="y",
                            showgrid=False),
                legend=dict(orientation="h", y=1.12), height=360,
                hovermode="x unified", margin=dict(l=60, r=60, t=40, b=30))
            st.plotly_chart(fig_roll, use_container_width=True)
            r1, r2, r3, r4 = st.columns(4)
            r1.metric("평균 CAGR", f"{np.nanmean(rc):+.1f}%")
            r2.metric("최고 CAGR", f"{np.nanmax(rc):+.1f}%")
            r3.metric("최저 CAGR", f"{np.nanmin(rc):+.1f}%")
            r4.metric("플러스 구간 비율", f"{(rc[ok] > 0).mean() * 100:.0f}%",
                      help="임의의 날에 시작해 이 기간을 운용했을 때 수익으로 끝난 비율")
    st.divider()

    # ── 사이클 손익률 분포 ──
    st.subheader("📊 사이클 손익률 분포")
    st.caption("사이클마다 발생한 손익률의 분포. 수익/손실의 패턴을 분석합니다.")
    if n_cyc >= 2:
        fig_pnl = go.Figure()
        fig_pnl.add_trace(go.Histogram(x=pnl_pct[pnl_pct <= 0], name="손실",
                                       marker_color="#EF5350",
                                       xbins=dict(size=10)))
        fig_pnl.add_trace(go.Histogram(x=pnl_pct[pnl_pct > 0], name="수익",
                                       marker_color="#43A047",
                                       xbins=dict(size=10)))
        fig_pnl.add_vline(x=0, line_dash="dash", line_color="#333")
        fig_pnl.add_vline(x=float(pnl_pct.mean()), line_dash="dot",
                          line_color="#1565C0",
                          annotation_text=f"평균 {pnl_pct.mean():+.2f}%",
                          annotation_position="top right")
        fig_pnl.update_layout(xaxis_title="사이클 손익률 (%)", yaxis_title="빈도 (회)",
                              height=320, barmode="overlay", showlegend=False,
                              margin=dict(l=60, r=20, t=30, b=30))
        st.plotly_chart(fig_pnl, use_container_width=True)
        _s = pd.Series(pnl_pct)
        pd1, pd2, pd3, pd4 = st.columns(4)
        pd1.metric("평균 손익률", f"{pnl_pct.mean():+.2f}%")
        pd2.metric("중앙값", f"{np.median(pnl_pct):+.2f}%")
        pd3.metric("왜도 (Skew)", f"{float(_s.skew()):.3f}",
                   help="양수=우측 꼬리(큰 수익 가끔), 음수=좌측 꼬리(큰 손실 가끔)")
        pd4.metric("첨도 (Kurt)", f"{float(_s.kurtosis()):.3f}",
                   help="높을수록 극단값(큰 수익/손실) 빈도 높음")
        st.caption("평균이 중앙값보다 훨씬 크고 왜도가 양수면, 소수의 큰 수익이 전체 "
                   "성과를 끌어올리는 구조입니다.")
    else:
        st.info("사이클이 2회 미만이라 분포 분석이 불가합니다.")
    st.divider()

    # ── 보유 비중 & 매매 타이밍 ──
    st.subheader("💵 보유 비중 & 매매 타이밍 패턴")
    st.caption("이 전략은 전량 보유 아니면 전량 현금입니다. 얼마나 자주 시장에 "
               "들어가 있었는지와 매매가 언제 몰렸는지 보여줍니다.")
    held_arr = _held.to_numpy()
    spells = _flat_spells(log)
    cu1, cu2, cu3, cu4 = st.columns(4)
    cu1.metric("보유 비중", f"{held_arr.mean() * 100:.1f}%",
               help="전체 거래일 중 주식을 들고 있던 날의 비율")
    cu2.metric("현금 비중", f"{(1 - held_arr.mean()) * 100:.1f}%")
    cu3.metric("평균 보유 기간", f"{cyc['보유일'].mean():.0f}일" if n_cyc else "-",
               help="사이클 하나의 평균 길이 (거래일)")
    cu4.metric("최장 현금 대기", f"{max(spells)}일" if spells else "-",
               help="매수 신호 없이 현금으로 기다린 가장 긴 기간 (거래일)")
    _exp = pd.Series(held_arr.astype(float) * 100, index=dates)
    _exp_roll = _exp.rolling(252, min_periods=60).mean()
    fig_cu = go.Figure()
    fig_cu.add_trace(go.Scatter(
        x=dates, y=_exp, name="보유(100) / 현금(0)", mode="lines",
        line=dict(width=0), fill="tozeroy", fillcolor="rgba(34,197,94,0.25)",
        hoverinfo="skip"))
    fig_cu.add_trace(go.Scatter(
        x=dates, y=_exp_roll, name="최근 1년 보유 비중", mode="lines",
        line=dict(color="rgba(180,80,0,0.9)", width=1.8),
        hovertemplate="%{x|%Y-%m-%d}<br>최근 1년 보유 비중 %{y:.0f}%<extra></extra>"))
    fig_cu.update_layout(yaxis_title="비율 (%)", yaxis=dict(range=[0, 100]),
                         height=300, hovermode="x unified",
                         legend=dict(orientation="h", x=1.0, y=1.02,
                                     xanchor="right", yanchor="bottom"),
                         margin=dict(l=60, r=20, t=20, b=40))
    st.plotly_chart(fig_cu, use_container_width=True)

    buys = log.index[log["매매"] == "시가 매수"]
    sells = log.index[log["매매"] == "시가 매도"]
    if len(buys):
        dow_labels = ["월", "화", "수", "목", "금"]
        b_dow = pd.Series(buys.dayofweek).value_counts().reindex(range(5), fill_value=0)
        s_dow = pd.Series(sells.dayofweek).value_counts().reindex(range(5), fill_value=0)
        fig_dow = go.Figure()
        fig_dow.add_trace(go.Bar(x=dow_labels, y=b_dow.to_numpy(), name="매수",
                                 marker_color=_COL_BUY))
        fig_dow.add_trace(go.Bar(x=dow_labels, y=s_dow.to_numpy(), name="매도",
                                 marker_color=_COL_SELL))
        fig_dow.update_layout(barmode="group", title="요일별 매매 빈도 (체결일)",
                              yaxis_title="횟수", height=300)
        b_mon = pd.Series(buys.month).value_counts().reindex(range(1, 13), fill_value=0)
        s_mon = pd.Series(sells.month).value_counts().reindex(range(1, 13), fill_value=0)
        mon_labels = [f"{m}월" for m in range(1, 13)]
        fig_mon = go.Figure()
        fig_mon.add_trace(go.Bar(x=mon_labels, y=b_mon.to_numpy(), name="매수",
                                 marker_color=_COL_BUY))
        fig_mon.add_trace(go.Bar(x=mon_labels, y=s_mon.to_numpy(), name="매도",
                                 marker_color=_COL_SELL))
        fig_mon.update_layout(barmode="group", title="월별 매매 빈도 (체결일)",
                              yaxis_title="횟수", height=300)
        tc1, tc2 = st.columns(2)
        tc1.plotly_chart(fig_dow, use_container_width=True)
        tc2.plotly_chart(fig_mon, use_container_width=True)
        st.caption(f"매매 횟수가 매수 {len(buys)}회 · 매도 {len(sells)}회뿐이라 "
                   "요일·월 쏠림은 우연일 가능성이 높습니다. 참고용으로만 보세요.")
    st.divider()

    # ── 파라미터 민감도 ──
    st.subheader("🎛️ 파라미터 민감도 분석")
    st.caption("현재 파라미터 주변의 Calmar Ratio 분포. 과최적화 여부를 확인합니다.")
    # 버튼을 누르면 rerun 되면서 펼침이 닫히므로, 결과가 있으면 펼친 채로 그린다
    with st.expander("🔍 민감도 히트맵 보기 (클릭하여 실행)",
                     expanded=st.session_state.get("tq_sens") is not None):
        ax_sel = st.radio("분석 대상 축", list(_SENS_AXES), horizontal=True,
                          key="tq_sens_axes")
        ky, kx = _SENS_AXES[ax_sel]

        def _axis_vals(k):
            cur, step = float(P[k]), _SENS_STEP[k]
            vals = [cur + step * j for j in range(-2, 3)]
            return [int(round(v)) for v in vals] if k == "ma_window" else vals
        y_vals, x_vals = _axis_vals(ky), _axis_vals(kx)
        if st.button("▶ 민감도 분석 실행", key="tq_run_sensitivity"):
            with st.spinner("민감도 분석 중... (25회 시뮬레이션)"):
                df_s = _get_data(P)
                if df_s.empty:
                    st.error("⚠️ 데이터 로드 실패")
                    return
                heat = np.full((5, 5), np.nan)
                for yi, yv in enumerate(y_vals):
                    for xi, xv in enumerate(x_vals):
                        Ps = dict(P)
                        Ps[ky], Ps[kx] = yv, xv
                        if "tp_disp" in (ky, kx):
                            Ps["use_tp"] = True
                        # 성립하지 않는 조합은 비워 둔다
                        if (Ps["buy_lo"] >= Ps["buy_hi"]
                                or Ps["exit_disp"] >= Ps["buy_lo"]
                                or Ps["ma_window"] < 20
                                or (Ps["use_tp"] and Ps["tp_disp"] <= Ps["buy_hi"])):
                            continue
                        lg = _bt_for(df_s, Ps, str(P["bt_start_date"]),
                                     str(P["bt_end_date"]))["log"]
                        if len(lg) < 60:
                            continue
                        a_s = lg["평가금"].to_numpy(dtype=float)
                        yrs = max((lg.index[-1] - lg.index[0]).days / 365.25, 1e-9)
                        c_s = (a_s[-1] / capital) ** (1 / yrs) - 1 if a_s[-1] > 0 else -1
                        _r, m_s = _ret_mdd(a_s, capital)
                        heat[yi][xi] = abs(c_s / (m_s / 100)) if m_s != 0 else 0.0
            st.session_state["tq_sens"] = {
                "heat": heat, "y": y_vals, "x": x_vals, "ky": ky, "kx": kx,
                "cur": (P[ky], P[kx]), "label": ax_sel}
            st.rerun()

        sens = st.session_state.get("tq_sens")
        if sens is not None:
            def _lab(k, v):
                return f"{int(v)}" if k == "ma_window" else f"{v:g}%"
            y_lab = [_lab(sens["ky"], v) for v in sens["y"]]
            x_lab = [_lab(sens["kx"], v) for v in sens["x"]]
            fig_heat = px.imshow(
                sens["heat"], x=x_lab, y=y_lab, color_continuous_scale="RdYlGn",
                labels={"x": _SENS_NAME[sens["kx"]], "y": _SENS_NAME[sens["ky"]],
                        "color": "Calmar"},
                text_auto=".2f", aspect="auto",
                title=f"Calmar Ratio 히트맵 — {sens['label']}")
            fig_heat.add_annotation(
                x=_lab(sens["kx"], sens["cur"][1]), y=_lab(sens["ky"], sens["cur"][0]),
                text="★", showarrow=False, xshift=-30,   # 값 왼쪽, 같은 칸 안에
                font=dict(color="white", size=15, family="Arial Black"))
            fig_heat.update_xaxes(type="category")
            fig_heat.update_yaxes(type="category")
            fig_heat.update_layout(height=380)
            st.plotly_chart(fig_heat, use_container_width=True)
            _h = sens["heat"]
            _valid = _h[~np.isnan(_h)]
            if len(_valid):
                st.caption(
                    f"25칸 중 유효 {len(_valid)}칸 — Calmar 최소 {_valid.min():.2f} · "
                    f"중앙값 {np.median(_valid):.2f} · 최대 {_valid.max():.2f}. "
                    "녹색일수록 Calmar Ratio가 높습니다. 현재 파라미터(★) 주변이 고르게 "
                    "녹색이면 과최적화 위험이 낮고, ★ 바로 옆에서 색이 급변하면 "
                    "경계에 걸친 값입니다.")
    st.divider()

    # ── 무작위 기간 강건성 ──
    st.subheader("🎲 무작위 기간 강건성 분석")
    st.caption(
        "전체 데이터에서 1년(252 거래일) 구간 100개를 무작위 추출하여 백테스트를 "
        "반복합니다. 시작 시점과 무관하게 전략이 일관된 성과를 내는지 확인합니다.")
    with st.expander("🔍 강건성 분석 실행 (클릭)",
                     expanded=bool(st.session_state.get("tq_mc_result"))):
        if st.button("▶ 무작위 100구간 분석 시작", key="tq_mc_run"):
            st.session_state["tq_mc_result"] = None
            with st.spinner("분석 중..."):
                df_mc = _get_data(P)
                if df_mc.empty:
                    st.error("⚠️ 데이터 로드 실패")
                    return
                idx_all = _bt_for(df_mc, P)["log"].index   # 지표가 유효한 전 구간
                WINDOW = 252
                if len(idx_all) - WINDOW < 100:
                    st.warning("데이터가 100구간 분석에 충분하지 않습니다.")
                else:
                    chosen = np.random.default_rng().choice(
                        len(idx_all) - WINDOW, size=100, replace=False)
                    mc = {"strat_ret": [], "strat_mdd": [], "bnh_ret": [],
                          "bnh_mdd": [], "periods": []}
                    prog = st.progress(0, text="시뮬레이션 중...")
                    for ci, si in enumerate(sorted(int(v) for v in chosen)):
                        s_dt, e_dt = idx_all[si], idx_all[si + WINDOW - 1]
                        lg = _bt_for(df_mc, P, s_dt, e_dt)["log"]
                        if len(lg):
                            a_mc = lg["평가금"].to_numpy(dtype=float)
                            tr, md = _ret_mdd(a_mc, capital)
                            c_mc = lg["종가"].to_numpy(dtype=float)
                            b_tr, b_md = _ret_mdd(capital * c_mc / c_mc[0], capital)
                            mc["strat_ret"].append(round(tr, 2))
                            mc["strat_mdd"].append(round(abs(md), 2))
                            mc["bnh_ret"].append(round(b_tr, 2))
                            mc["bnh_mdd"].append(round(abs(b_md), 2))
                            mc["periods"].append((str(s_dt.date()), str(e_dt.date())))
                        prog.progress((ci + 1) / 100,
                                      text=f"시뮬레이션 중... {ci + 1}/100")
                    prog.empty()
                    st.session_state["tq_mc_result"] = mc
                    st.rerun()

        mc = st.session_state.get("tq_mc_result")
        if mc and mc["strat_ret"]:
            sr, sm = np.array(mc["strat_ret"]), np.array(mc["strat_mdd"])
            br, bm = np.array(mc["bnh_ret"]), np.array(mc["bnh_mdd"])

            def _mc_stats(arr, label, signed=True):
                f = "{:+.1f}%" if signed else "{:.1f}%"
                return {"구분": label, "평균": f.format(np.mean(arr)),
                        "중앙값": f.format(np.median(arr)),
                        "표준편차": f"{np.std(arr):.1f}%",
                        "최솟값": f.format(np.min(arr)),
                        "최댓값": f.format(np.max(arr)),
                        "양(+) 비율": (f"{(arr > 0).mean() * 100:.0f}%"
                                     if signed else "-")}
            st.markdown(f"**📋 요약 통계 (n={len(sr)})**")
            st.dataframe(pd.DataFrame([
                _mc_stats(sr, "TQ200 전략 (1년 수익률)"),
                _mc_stats(br, f"{ticker} B&H (1년 수익률)"),
                _mc_stats(sm, "TQ200 전략 (MDD)", signed=False),
                _mc_stats(bm, f"{ticker} B&H (MDD)", signed=False),
            ]), hide_index=True, use_container_width=True)
            st.caption(f"전략이 B&H 보다 수익률이 높았던 구간 "
                       f"{(sr > br).mean() * 100:.0f}% · MDD 가 작았던 구간 "
                       f"{(sm < bm).mean() * 100:.0f}%. 1년 단위로는 강세장에서 B&H 에 "
                       "뒤지는 구간이 많은 것이 정상이고, 낙폭을 얼마나 줄였는지가 "
                       "이 전략의 핵심입니다.")

            with st.expander("📄 100구간 상세 결과 보기"):
                det = pd.DataFrame({
                    "#": range(1, len(sr) + 1),
                    "시작일": [p_[0] for p_ in mc["periods"]],
                    "종료일": [p_[1] for p_ in mc["periods"]],
                    "전략 수익률(%)": sr, "전략 MDD(%)": sm,
                    "B&H 수익률(%)": br, "B&H MDD(%)": bm})

                def _hl_det(row):
                    clr = ("background-color: rgba(46,125,50,0.15)"
                           if row["전략 수익률(%)"] > 0
                           else "background-color: rgba(198,40,40,0.15)")
                    return [clr] * len(row)
                st.dataframe(
                    det.style.apply(_hl_det, axis=1).format({
                        "전략 수익률(%)": "{:+.1f}%", "전략 MDD(%)": "{:.1f}%",
                        "B&H 수익률(%)": "{:+.1f}%", "B&H MDD(%)": "{:.1f}%"}),
                    hide_index=True, use_container_width=True,
                    height=min(38 + 35 * len(det), 500))

            fig_mc = make_subplots(
                rows=1, cols=2, horizontal_spacing=0.12,
                subplot_titles=["수익률 분포 (1년)", "최대 낙폭(MDD) 분포"])
            for arr, color, name, col, lg, show in (
                    (br, "#FB8C00", f"{ticker} B&H", 1, "bnh", True),
                    (bm, "#FB8C00", f"{ticker} B&H", 2, "bnh", False),
                    (sr, "#1565C0", "TQ200 전략", 1, "strat", True),
                    (sm, "#1565C0", "TQ200 전략", 2, "strat", False)):
                fig_mc.add_trace(go.Histogram(
                    x=arr, nbinsx=20, name=name, opacity=0.55, marker_color=color,
                    legendgroup=lg, showlegend=show), row=1, col=col)
            fig_mc.add_vline(x=0, line_dash="dash", line_color="#555", row=1, col=1)
            fig_mc.update_xaxes(title_text="1년 수익률 (%)", row=1, col=1)
            fig_mc.update_xaxes(title_text="MDD (%)", row=1, col=2)
            fig_mc.update_yaxes(title_text="빈도 (구간수)", row=1, col=1)
            fig_mc.update_yaxes(title_text="빈도 (구간수)", row=1, col=2)
            fig_mc.update_layout(height=420, barmode="overlay",
                                 legend=dict(orientation="v", x=1.02, y=1.0,
                                             xanchor="left"),
                                 margin=dict(r=140))
            st.plotly_chart(fig_mc, use_container_width=True)
            st.caption("수익률 분포가 B&H 보다 좁을수록(큰 손실 구간이 줄어듦), MDD 분포가 "
                       "왼쪽에 몰릴수록(낮은 손실) 시작 시점에 덜 좌우되는 전략입니다.")

            st.markdown("**📦 박스플롯 — 분포 요약 (중앙값 · 사분위 · 이상치)**")
            fig_box2 = make_subplots(
                rows=1, cols=2, horizontal_spacing=0.12,
                subplot_titles=["수익률 박스플롯", "최대 낙폭(MDD) 박스플롯"])
            for arr, color, name, col, lg, show in (
                    (br, "#FB8C00", f"{ticker} B&H", 1, "bnh_box", True),
                    (sr, "#1565C0", "TQ200 전략", 1, "strat_box", True),
                    (bm, "#FB8C00", f"{ticker} B&H", 2, "bnh_box", False),
                    (sm, "#1565C0", "TQ200 전략", 2, "strat_box", False)):
                fig_box2.add_trace(go.Box(
                    y=arr, name=name, marker_color=color, boxmean=True,
                    legendgroup=lg, showlegend=show), row=1, col=col)
            fig_box2.add_hline(y=0, line_dash="dash", line_color="#888", row=1, col=1)
            fig_box2.update_yaxes(title_text="수익률 (%)", row=1, col=1)
            fig_box2.update_yaxes(title_text="MDD (%)", row=1, col=2)
            fig_box2.update_layout(height=420,
                                   legend=dict(orientation="v", x=1.02, y=1.0,
                                               xanchor="left"),
                                   margin=dict(r=140))
            st.plotly_chart(fig_box2, use_container_width=True)
            st.caption("박스: 25~75 백분위 구간 / 선: 중앙값 / 점선: 평균 / 점: 이상치")
    st.divider()

    # ── 전략 인사이트 ──
    st.subheader("💡 전략 인사이트 & 맥락 참고")
    st.warning(f"**다음 내용은 {ticker} TQ200 백테스트 결과 해석입니다. "
               "과거 성과가 미래 수익을 보장하지 않습니다.**")
    with st.container(border=True):
        st.markdown("""
**왜 200일선 이격도 전략이 3배 레버리지 ETF 에서 작동하나?**
- **하락장을 통째로 피한다**: 3배 레버리지는 하락·횡보 구간에서 변동성 손실이 누적되어
  단순 보유 시 −80% 안팎까지 빠집니다. 200일선 아래 구간에 현금으로 있는 것만으로 낙폭이 크게 줄어듭니다.
- **추격 매수를 막는다**: 이격도 상한(기본 10%) 덕분에 이미 많이 오른 뒤에는 사지 않고 눌림을 기다립니다.
- **과열에서 수익을 확정한다**: 200일선과 크게 벌어진 뒤의 급락을 피하고, 다시 200일선 근처로
  내려왔을 때 재진입합니다. 과열 익절이 없으면 CAGR·MDD 모두 뚜렷이 나빠집니다.
- **기울기 필터**: 200일선이 하락 중일 때의 반등(데드캣 바운스)에 물리는 것을 줄입니다.

**주요 지표 해석**
- **Calmar 1.0 이상**: 우수 / **2.0 이상**: 최상급
- **승률은 절반 안팎**이 정상입니다. 손익비(평균 익절 ÷ 평균 손절)가 성과를 만듭니다.
- **보유 중 낙폭**: 200일선 위에서도 −20~−30% 조정은 흔합니다. 감내 가능한 수준인지 확인하세요.

**주의사항**
- **사이클 수가 적습니다**(16년에 20~30회). 지표 하나하나의 통계적 신뢰도는 높지 않고,
  한두 번의 큰 사이클이 결과를 좌우합니다.
- **검증 구간의 한계**: TQQQ 는 2010년 상장이라 2000~2002년·2008년 같은 장기 약세장이
  데이터에 없습니다. 나스닥이 장기간 200일선을 오르내리는 횡보장에서는 손절이 연속될 수 있습니다.
- **시가 갭 위험**: 신호는 종가로 판단하고 다음 날 시가에 체결하므로, 밤사이 급락하면
  청산선보다 훨씬 낮은 가격에 팔릴 수 있습니다.
- 현금 구간의 이자 수익은 반영하지 않았고, 실제 거래에서는 **슬리피지, 수수료, 세금**이 수익률을 낮춥니다.
- 전략 파라미터를 너무 자주 바꾸면 과최적화(overfitting) 위험이 있습니다.
        """)
    st.divider()

    # ── 종합 성과 요약 ──
    st.subheader("📋 종합 성과 요약")
    _rows = [
        ("시작 자본", f"${capital:,.0f}"), ("최종 자산", f"${final:,.0f}"),
        ("총 수익률", f"{total_ret * 100:+,.2f}%"),
        ("CAGR (연복리)", f"{cagr * 100:.2f}%"), ("MDD", f"{mdd * 100:.2f}%"),
        ("Calmar Ratio", f"{calmar:.3f}"), ("Sharpe Ratio", f"{sharpe:.3f}"),
        ("Sortino Ratio", f"{sortino:.3f}"),
        ("B&H 총 수익률 / CAGR / MDD",
         f"{bh_ret * 100:+,.1f}% / {bh_cagr * 100:.1f}% / {bh_mdd * 100:.1f}%"),
        ("총 사이클", f"{n_cyc}회"),
    ]
    if n_cyc:
        _rows += [
            ("승률", f"{win_rate:.1f}% ({len(wins)}승 {len(losses)}패)"),
            ("평균 손익률", f"{pnl_pct.mean():+.2f}%"),
            ("손익비", f"{payoff:.2f}"),
            # 수익률 기준 최고/최저 사이클 — 금액은 같은 사이클의 것
            ("최대 단일 수익 (수익률 기준)",
             f"{pnl_pct.max():+.2f}% ({_usd(pnl_usd[pnl_pct.argmax()])})"),
            ("최대 단일 손실 (수익률 기준)",
             f"{pnl_pct.min():+.2f}% ({_usd(pnl_usd[pnl_pct.argmin()])})"),
            ("최대 단일 손익 (금액 기준)",
             f"{_usd(pnl_usd.max())} / {_usd(pnl_usd.min())}"),
            ("과열 익절 / 이탈 청산", f"{int(m_tp.sum())}회 / {int(m_brk.sum())}회"),
            ("평균 보유 기간", f"{cyc['보유일'].mean():.0f} 거래일"),
        ]
    _rows.append(("보유 비중", f"{held_arr.mean() * 100:.1f}%"))
    if res["open_position"]:
        op = res["open_position"]
        _rows.append(("기간말 보유 포지션",
                      f"{op['시작일'].date()} 매수 · 평가손익 {op['평가손익률(%)']:+.2f}%"))
    st.dataframe(pd.DataFrame(_rows, columns=["항목", "수치"]),
                 hide_index=True, use_container_width=True,
                 height=38 + 35 * len(_rows))


# ══════════════════════════════════════════════
# 개인 설정 탭
# ══════════════════════════════════════════════
def _send_test_ordersheet(tg_token: str, tg_chat_id: str, cfg: dict):
    """현재 사이드바 세팅 + 저장된 포지션으로 주문표를 만들어 발송.

    Returns (ok: bool, 메시지: str)."""
    params = st.session_state.get("tq_last_params")
    if not params or not params.get("bt_ticker"):
        return False, "사이드바를 한 번 불러온 뒤 다시 시도해주세요."
    ticker = params["bt_ticker"]
    df = _get_data(params, end=date.today())
    if df.empty:
        return False, f"{ticker} 가격 데이터를 불러오지 못했습니다."
    pos = dict(cfg.get("positions", {}).get(ticker, {}))
    pos.setdefault("cash", params["capital"])
    sig = latest_signal(df, holding=bool(pos.get("holding")),
                        **_engine_kwargs(params))
    if not sig:
        return False, "이동평균 계산에 필요한 데이터가 부족합니다."
    next_day = next_trading_date(sig["기준일"].date() + timedelta(days=1))
    plan = _order_plan(ticker, sig, pos, next_day)
    res = _send_telegram(tg_token, tg_chat_id,
                         _order_text(ticker, params, sig, plan, pos, next_day))
    if res.get("ok"):
        return True, f"{ticker} 주문표 발송 성공!"
    return False, f"발송 실패: {res.get('description', '알 수 없는 오류')}"


def _render_telegram_settings(cfg: dict):
    """텔레그램 알림 설정 (DSS·만능 스위치 등 기존 전략과 동일한 구성).

    Chat ID · Bot Token 은 tq200_config 안에 저장 —
    Cloud = users.tq200_config / 로컬 = ~/.tq200/config.json
    """
    with st.container(border=True):
        col_title, col_help = st.columns([3, 1])
        with col_title:
            st.markdown("#### 💬 텔레그램 알림 설정")
            st.caption("TQ200 주문표(오늘의 신호)를 텔레그램으로 받을 수 있습니다.")
        with col_help:
            with st.popover("❓ Chat ID & Bot Token 확인 방법",
                            use_container_width=True):
                render_telegram_help_popover(
                    strategy_name="TQ200",
                    example_bot_display="TQ200 알림봇",
                    example_bot_username="tq200_alert_bot",
                    example_bot_username2="my_tqqq_bot",
                    test_button_label="📨 주문표 테스트 발송")

        c1, c2 = st.columns(2)
        tg_chat_id = c1.text_input(
            "텔레그램 Chat ID", value=cfg.get("tg_chat_id", ""),
            placeholder="예: 1234567890", key="tq_tg_chat_id").strip()
        tg_token = c2.text_input(
            "Bot Token", value=cfg.get("tg_token", ""),
            placeholder="예: 123456789:AAF...", type="password",
            key="tq_tg_token").strip()

        st.caption("📅 저장해 두면 **매수·매도 신호가 난 날에만** 월~금 낮 12:00 (KST)에 "
                   "주문표가 자동 발송됩니다. 신호가 없는 날은 보내지 않습니다.")

        btn1, btn2, _sp = st.columns([1, 1, 4])
        with btn1:
            if st.button("📨 주문표 테스트 발송", use_container_width=True,
                         key="tq_tg_test"):
                if not tg_chat_id or not tg_token:
                    st.warning("Chat ID와 Bot Token을 먼저 입력해주세요.")
                else:
                    with st.spinner("주문표 생성 & 발송 중..."):
                        try:
                            ok, text = _send_test_ordersheet(
                                tg_token, tg_chat_id, cfg)
                        except Exception as e:
                            ok, text = False, f"발송 실패: {e}"
                    (st.success if ok else st.error)(
                        ("✅ " if ok else "") + text)
        with btn2:
            if st.button("💾 저장하기", use_container_width=True,
                         key="tq_tg_save", type="primary"):
                if not tg_chat_id or not tg_token:
                    st.warning("Chat ID와 Bot Token을 모두 입력해주세요.")
                else:
                    cfg["tg_chat_id"] = tg_chat_id
                    cfg["tg_token"] = tg_token
                    _save_cfg(cfg)
                    _loc = ("Google Sheets"
                            if (_IS_CLOUD and st.session_state.get("logged_in"))
                            else _TQ_CONFIG_PATH)
                    st.success(f"✅ 저장 완료! `{_loc}`")


def render_settings_tab():
    st.subheader("⚙️ TQ200 개인 설정")
    cfg = _load_cfg()
    where = ("Google Sheets `users.tq200_config`" if _IS_CLOUD
             else "`~/.tq200/config.json`")
    st.caption(f"저장 위치: {where}")

    _render_telegram_settings(cfg)
    with st.expander("ℹ️ 자동 발송은 어떻게 동작하나요?"):
        st.markdown("""
- **언제**: 미국 장 다음 날 낮 12:00 (KST, 월~금·미국 휴장일 제외). 전일 종가로 판단한
  주문을 그날 밤 개장 전에 MOO 로 넣을 수 있게 미리 보냅니다.
- **어떤 날**: 백테스트와 같은 기준으로 **매수 신호 / 매도 신호가 난 날에만** 보냅니다
  (백테스트 일별 로그의 '매수 신호'·'매도 신호' 행과 같은 날). 한 신호당 한 번입니다.
- **어떤 종목**: [📋 오늘의 신호] 탭에서 포지션을 저장한 종목들. 저장한 종목이 없으면 TQQQ.
- **어떤 파라미터**: 아래 '전략 파라미터 기본값'에 저장한 값 (없으면 앱 기본값).
- **수량**: 저장된 포지션의 예수금(매수)·보유 수량(매도)으로 계산합니다. 체결 후 포지션을
  저장해 두면 다음 주문 수량이 정확해집니다.
- Cloud(로그인) 에서 저장한 설정만 자동 발송됩니다. 로컬 PC 에 저장한 값은 수동 발송용입니다.
""")
    st.write("")

    st.markdown("**전략 파라미터 기본값**")
    saved = cfg.get("params", {})
    cur = {pkey: st.session_state.get(wkey, default)
           for wkey, pkey, default in _WIDGETS}
    _labels = {"ma_window": "이동평균(일)", "slope_window": "기울기 기간(일)",
               "buy_lo": "매수 하한(%)", "buy_hi": "매수 상한(%)",
               "exit_disp": "청산 이격도(%)", "tp_disp": "익절 이격도(%)",
               "use_slope": "기울기 필터", "use_tp": "과열 익절",
               "fee": "수수료(%)", "capital": "초기 투자금($)",
               "price_mode": "가격 기준"}
    st.dataframe(pd.DataFrame([
        {"항목": _labels[pkey], "현재 사이드바": str(cur[pkey]),
         "저장된 기본값": str(saved.get(pkey, "—")),
         "앱 기본값": str(default)}
        for _wkey, pkey, default in _WIDGETS]),
        hide_index=True, use_container_width=True)
    s1, s2 = st.columns(2)
    if s1.button("💾 현재 사이드바 값을 기본값으로 저장", key="tq_set_save",
                 use_container_width=True):
        cfg["params"] = cur
        _save_cfg(cfg)
        st.success("저장 완료 — 다음 접속부터 이 값으로 시작합니다.")
    if s2.button("↩️ 앱 기본값으로 초기화", key="tq_set_reset",
                 use_container_width=True):
        cfg.pop("params", None)
        _save_cfg(cfg)
        st.session_state["_tq_reset"] = True   # 다음 실행의 사이드바가 반영
        st.rerun()

    st.write("")
    st.markdown("**저장된 포지션**")
    positions = cfg.get("positions", {})
    if not positions:
        st.info("저장된 포지션이 없습니다. [📋 오늘의 신호] 탭에서 입력·저장하세요.")
    else:
        for tk, p in positions.items():
            if p.get("holding"):
                st.caption(f"📈 **{tk}** — 보유 {int(p.get('shares', 0)):,}주 · "
                           f"단가 ${float(p.get('avg_price', 0)):,.2f} · "
                           f"예수금 ${float(p.get('cash', 0)):,.2f}")
            else:
                st.caption(f"💵 **{tk}** — 무포지션 · "
                           f"예수금 ${float(p.get('cash', 0)):,.2f}")
