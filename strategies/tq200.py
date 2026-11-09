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
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

from common.analysis import monthly_perf_table
from common.config import _IS_CLOUD
from common.data import filter_incomplete_today, next_trading_date
from common.telegram import _send_telegram, render_telegram_help_popover
from tq200_engine import (DEFAULTS, compute_stats, latest_signal,
                          run_backtest, yearly_returns)
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
                                   mode_short={"보유": "보유", "현금": "현금"}),
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
