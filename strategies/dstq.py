# -*- coding: utf-8 -*-
"""듀얼스나이퍼 TQ (TQQQ 200일선 홀딩 + 듀얼스나이퍼 방어) — 전략 모듈

추세(200일선 위 = 홀딩) + 역추세(그 외 = 듀얼스나이퍼 방어 LOC) 혼합 전략.
- 엔진: dstq_engine.py — 원본 08.듀얼스나이퍼/항공모함셋_TQQQ.xlsx 재현
        (2011-01-03~2026-11-10 모드·매수티어 3,988일 100% 일치, 최종자산 오차 −0.05%)
- 가격: 무조정 OHLC (auto_adjust=False) — 홀딩 진입·청산이 시가 체결이라 Open 필수
- 설정 저장: Cloud = users.dstq_config JSON 컬럼 / 로컬 = ~/.dstq/config.json
"""
from __future__ import annotations

import itertools
import json
import os
import time
from datetime import date, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf
from plotly.subplots import make_subplots

from common.analysis import monthly_perf_table, recalc_adj_history
from common.config import _IS_CLOUD
from common.data import filter_incomplete_today, next_trading_date
from common.telegram import _send_telegram, render_telegram_help_popover
from dstq_engine import (DEFAULTS, EXIT_BREAK, EXIT_TP, MODE_DEF, MODE_HOLD, account_adjustments,
                         account_params, build_order_text, can_tungchigi,
                         compute_account_orders, compute_stats, daily_view,
                         normalize_accounts, order_rows, run_backtest,
                         yearly_returns)

_CFG_DIR = os.path.join(os.path.expanduser("~"), ".dstq")
_CFG_PATH = os.path.join(_CFG_DIR, "config.json")
_CFG_COL = "dstq_config"

_COL_STRAT, _COL_BH, _COL_MA = "#e45756", "#888888", "#f2a93b"
_COL_BUY, _COL_SELL = "#22c55e", "#ef4444"
_W_STR = ", ".join(f"{w:g}" for w in DEFAULTS["sf_weights"])


# ──────────────────────────────────────────────
# 설정 저장
# ──────────────────────────────────────────────
def _load_cfg() -> dict:
    if _IS_CLOUD and st.session_state.get("logged_in"):
        raw = st.session_state.get("user_settings", {}).get(_CFG_COL, "")
        if raw:
            try:
                cfg = json.loads(raw) if isinstance(raw, str) else raw
                return cfg if isinstance(cfg, dict) else {}
            except Exception:
                pass
        return {}
    try:
        with open(_CFG_PATH, encoding="utf-8") as f:
            cfg = json.load(f)
            return cfg if isinstance(cfg, dict) else {}
    except Exception:
        return {}


def _save_cfg(cfg: dict):
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            cfg_json = json.dumps(cfg, ensure_ascii=False)
            st.session_state.setdefault("user_settings", {})
            st.session_state.user_settings[_CFG_COL] = cfg_json
            from common.auth import _save_user_settings_to_sheet
            _save_user_settings_to_sheet(st.session_state.username, {_CFG_COL: cfg_json})
        except Exception as e:
            st.warning(f"⚠️ Cloud 저장 실패: {e}")
        return
    try:
        os.makedirs(_CFG_DIR, exist_ok=True)
        with open(_CFG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.warning(f"⚠️ 로컬 저장 실패: {e}")


# 사이드바 위젯 key ↔ 파라미터 key ↔ 기본값(항공모함셋)
_WIDGETS = [
    ("dq_buy_lo", "buy_lo", DEFAULTS["buy_lo"]), ("dq_buy_hi", "buy_hi", DEFAULTS["buy_hi"]),
    ("dq_exit", "exit_disp", DEFAULTS["exit_disp"]), ("dq_tp", "tp_disp", DEFAULTS["tp_disp"]),
    ("dq_div", "sf_div", DEFAULTS["sf_div"]), ("dq_hold", "sf_hold", DEFAULTS["sf_hold"]),
    ("dq_b1", "sf_buy1", DEFAULTS["sf_buy1"]), ("dq_b2", "sf_buy2", DEFAULTS["sf_buy2"]),
    ("dq_sell", "sf_sell", DEFAULTS["sf_sell"]),
    ("dq_ratio", "sf_sell_ratio", DEFAULTS["sf_sell_ratio"]),
    ("dq_ma", "sf_ma_base", DEFAULTS["sf_ma_base"]), ("dq_w", "sf_weights", _W_STR),
    ("dq_fee", "fee", DEFAULTS["fee"]), ("dq_capital", "capital", 10000.0),
]
_INT_KEYS = {"sf_div", "sf_hold", "sf_ma_base"}
_STRAT_KEYS = ("buy_lo", "buy_hi", "exit_disp", "tp_disp", "sf_div", "sf_hold", "sf_buy1",
               "sf_buy2", "sf_sell", "sf_sell_ratio", "sf_ma_base", "sf_weights", "fee")


def _coerce(pkey, val, default):
    try:
        if pkey == "sf_weights":
            if isinstance(val, (list, tuple)):
                return ", ".join(f"{float(w):g}" for w in val)
            return str(val)
        return int(float(val)) if pkey in _INT_KEYS else float(val)
    except (TypeError, ValueError):
        return default


def _parse_weights(text: str, div: int):
    """'6, 13, 20, 27, 34' → tuple. 개수 불일치·오름차순 아님이면 (None, 오류문구)."""
    try:
        w = tuple(float(x) for x in str(text).replace("/", ",").split(",") if x.strip())
    except ValueError:
        return None, "티어별 매수비중은 숫자를 쉼표로 구분해 입력하세요."
    if len(w) != int(div):
        return None, f"티어별 매수비중 개수({len(w)})가 방어 분할수({int(div)})와 다릅니다."
    if any(x <= 0 for x in w) or any(b < a for a, b in zip(w, w[1:])):
        return None, "티어별 매수비중은 0보다 큰 오름차순만 허용합니다."
    return w, ""


def _engine_params(params: dict) -> dict:
    return {k: params[k] for k in _STRAT_KEYS}


# ──────────────────────────────────────────────
# 데이터
# ──────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=1800)
def _load_ohlc(ticker: str, end_str: str) -> pd.DataFrame:
    """상장 이후 전체 무조정 OHLC (200MA 워밍업 때문에 시작일과 무관하게 전 구간)."""
    end = pd.to_datetime(end_str).date()
    for attempt in range(3):
        try:
            raw = yf.download(ticker, start="2009-01-01", end=end + timedelta(days=2),
                              progress=False, auto_adjust=False)
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
def _cached_bt(df, p_json: str, capital: float, start_str: str, end_str: str):
    r = run_backtest(df, json.loads(p_json), capital, start=start_str, end=end_str)
    return r["log"], r["trades"]


def _run(params: dict, df: pd.DataFrame, start=None, end=None):
    return _cached_bt(df, json.dumps(_engine_params(params), sort_keys=True),
                      float(params["capital"]),
                      str(start or params["bt_start_date"]), str(end or params["bt_end_date"]))


def _stat_cards(items: list, tone: str = "gray") -> str:
    if tone == "blue":
        border, bg, lbl = "rgba(59,130,246,.4)", "rgba(59,130,246,.08)", "#3b82f6"
    else:
        border, bg, lbl = "rgba(128,128,128,.28)", "rgba(128,128,128,.05)", "#888"
    boxes = []
    for label, value, sub, color in items:
        sub_html = (f'<div style="font-size:12px; margin-top:5px; color:{color};">{sub}</div>'
                    if sub else "")
        boxes.append(
            '<div style="flex:1 1 150px; min-width:140px; '
            f'border:1px solid {border}; border-radius:10px; padding:13px 8px 11px; '
            f'text-align:center; background:{bg};">'
            f'<div style="font-size:12.5px; color:{lbl}; margin-bottom:6px;">{label}</div>'
            f'<div style="font-size:22px; font-weight:700; line-height:1.15;">{value}</div>'
            f'{sub_html}</div>')
    return ('<div style="display:flex; gap:10px; flex-wrap:wrap; margin:4px 0 12px 0;">'
            + "".join(boxes) + "</div>")


def _bh_stats(log: pd.DataFrame) -> tuple:
    px = log["종가"].to_numpy(dtype=float)
    years = (pd.Timestamp(log["날짜"].iloc[-1]) - pd.Timestamp(log["날짜"].iloc[0])).days / 365.25
    cagr = (px[-1] / px[0]) ** (1 / years) - 1 if years > 0 else 0.0
    return cagr * 100, (px / np.maximum.accumulate(px) - 1).min() * 100


# ══════════════════════════════════════════════
# 사이드바
# ══════════════════════════════════════════════
def render_sidebar() -> dict:
    # 위젯은 key 만 쓰고 value= 미지정 — 없는 key 만 저장값/기본값으로 시드
    if st.session_state.pop("_dq_reset", False):
        for wkey, _pkey, default in _WIDGETS:
            if wkey != "dq_capital":
                st.session_state[wkey] = default
    _missing = [w for w in _WIDGETS if w[0] not in st.session_state]
    if _missing:
        saved = _load_cfg().get("params", {})
        for wkey, pkey, default in _missing:
            st.session_state[wkey] = _coerce(pkey, saved.get(pkey, default), default)

    st.subheader("📌 종목")
    ticker = st.selectbox("종목코드 (Ticker)", ["TQQQ", "SOXL", "UPRO", "QLD"], key="dq_tk")

    st.markdown("---")
    st.subheader("⚙️ 프리셋")
    if st.button("🛳️ 항공모함셋 (원본)", key="dq_preset", use_container_width=True,
                 help="원본 시트 값으로 되돌립니다.\n"
                      "홀딩: 매수 이격도 1~8% · 매도 −1% / 50%\n"
                      "방어: 분할5 · 보유8일 · 매수 −1.5%/5.5% · 매도 1.5% · MA3 · 비중 6/13/20/27/34\n"
                      "TQQQ 2011-01~2026-11: CAGR 60.8% / MDD −32.0%"):
        st.session_state["_dq_reset"] = True
        st.rerun()

    st.markdown("#### 🟧 홀딩모드 (200일선 이격도)")
    c1, c2 = st.columns(2)
    buy_lo = c1.number_input("매수 이격도 하한(%)", min_value=-20.0, max_value=50.0, step=0.5,
                             format="%.1f", key="dq_buy_lo")
    buy_hi = c2.number_input("매수 이격도 상한(%)", min_value=-20.0, max_value=100.0, step=0.5,
                             format="%.1f", key="dq_buy_hi",
                             help="이 값 이상이면 추격 매수하지 않고 방어 모드를 유지합니다")
    c3, c4 = st.columns(2)
    exit_disp = c3.number_input("매도 이격도 하한(%)", min_value=-30.0, max_value=20.0, step=0.5,
                                format="%.1f", key="dq_exit",
                                help="이격도가 이 값 미만으로 이탈하면 다음날 시가 전량 매도")
    tp_disp = c4.number_input("매도 이격도 상한(%)", min_value=5.0, max_value=300.0, step=5.0,
                              format="%.1f", key="dq_tp",
                              help="이격도가 이 값을 넘으면(과열) 다음날 시가 전량 매도")

    st.markdown("#### 🟩 방어모드 (듀얼스나이퍼 LOC)")
    d1, d2 = st.columns(2)
    sf_div = d1.number_input("방어 분할수", min_value=1, max_value=10, step=1, key="dq_div")
    sf_hold = d2.number_input("보유기간", min_value=1, max_value=60, step=1, key="dq_hold")
    d3, d4 = st.columns(2)
    sf_b1 = d3.number_input("매수조건1(MA%)", min_value=-20.0, max_value=20.0, step=0.1,
                            format="%.1f", key="dq_b1")
    sf_b2 = d4.number_input("매수조건2(종가%)", min_value=-20.0, max_value=30.0, step=0.5,
                            format="%.1f", key="dq_b2")
    st.caption("매수조건 1, 2 중에 낮은 값")
    d5, d6 = st.columns(2)
    sf_sell = d5.number_input("매도조건(MA%)", min_value=-10.0, max_value=30.0, step=0.1,
                              format="%.1f", key="dq_sell")
    sf_ratio = d6.number_input("매도 비율(%)", min_value=1.0, max_value=100.0, step=5.0,
                               format="%.1f", key="dq_ratio")
    sf_ma = st.number_input("MA기준", min_value=2, max_value=20, step=1, key="dq_ma")
    w_text = st.text_input("티어별 매수비중(%)", key="dq_w", help="오름차순만 허용")
    weights, w_err = _parse_weights(w_text, sf_div)
    if w_err:
        st.error(w_err + " — 기본값으로 계산합니다.")
        weights, sf_div = tuple(DEFAULTS["sf_weights"]), DEFAULTS["sf_div"]
    if buy_lo >= buy_hi:
        st.warning("⚠️ 매수 이격도 하한이 상한 이상이라 홀딩 진입 신호가 나오지 않습니다.")

    fee = st.number_input("수수료 (%, 편도)", min_value=0.0, step=0.01, format="%.2f",
                          key="dq_fee")

    st.markdown("---")
    st.subheader("백테스트 설정")
    capital = st.number_input("초기 투자금 ($)", min_value=100.0, step=1000.0, format="%.0f",
                              key="dq_capital")
    b1, b2 = st.columns(2)
    bt_start = b1.date_input("시작 일", date(2011, 1, 3), key="dq_start")
    bt_end = b2.date_input("종료 일", date.today(), key="dq_end")

    params = {
        "bt_ticker": ticker, "bt_start_date": bt_start, "bt_end_date": bt_end,
        "bt_initial_capital": float(capital), "capital": float(capital),
        "buy_lo": float(buy_lo), "buy_hi": float(buy_hi),
        "exit_disp": float(exit_disp), "tp_disp": float(tp_disp),
        "sf_div": int(sf_div), "sf_hold": int(sf_hold),
        "sf_buy1": float(sf_b1), "sf_buy2": float(sf_b2),
        "sf_sell": float(sf_sell), "sf_sell_ratio": float(sf_ratio),
        "sf_ma_base": int(sf_ma), "sf_weights": list(weights), "fee": float(fee),
    }
    st.session_state["dq_last_params"] = params
    return params


# ══════════════════════════════════════════════
# 차트
# ══════════════════════════════════════════════
def _hold_spans(log: pd.DataFrame) -> list:
    m = (log["모드"] == MODE_HOLD).to_numpy()
    d = pd.to_datetime(log["날짜"]).to_numpy()
    spans, s = [], None
    for i, v in enumerate(m):
        if v and s is None:
            s = d[i]
        elif not v and s is not None:
            spans.append((s, d[i])); s = None
    if s is not None:
        spans.append((s, d[-1]))
    return spans


def _fig_equity(log: pd.DataFrame, ticker: str, log_scale: bool) -> go.Figure:
    d = pd.to_datetime(log["날짜"])
    base = float(log["총자산"].iloc[0] / log["좌당가"].iloc[0])
    bh = log["종가"] / float(log["종가"].iloc[0]) * base
    fig = go.Figure()
    for s, e in _hold_spans(log):
        fig.add_vrect(x0=s, x1=e, fillcolor="rgba(242,169,59,0.13)", line_width=0, layer="below")
    fig.add_trace(go.Scatter(x=d, y=log["좌당가"] * base, name="듀얼스나이퍼 TQ",
                             line=dict(color=_COL_STRAT, width=2)))
    fig.add_trace(go.Scatter(x=d, y=bh, name=f"{ticker} 보유(B&H)",
                             line=dict(color=_COL_BH, width=1.2, dash="dot")))
    fig.update_layout(title="자산 곡선 (주황 배경 = 홀딩 모드, 흰 배경 = 방어 모드)",
                      height=420, margin=dict(l=10, r=10, t=45, b=10),
                      yaxis_type="log" if log_scale else "linear",
                      legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"))
    return fig


def _fig_price(log: pd.DataFrame, trades: pd.DataFrame, log_scale: bool) -> go.Figure:
    d = pd.to_datetime(log["날짜"])
    fig = go.Figure()
    for s, e in _hold_spans(log):
        fig.add_vrect(x0=s, x1=e, fillcolor="rgba(242,169,59,0.13)", line_width=0, layer="below")
    fig.add_trace(go.Scatter(x=d, y=log["종가"], name="종가", line=dict(color="#4c78a8", width=1.2)))
    fig.add_trace(go.Scatter(x=d, y=log["200MA"], name="200MA", line=dict(color=_COL_MA, width=1.5)))
    if trades is not None and len(trades):
        h = trades[trades["모드"] == MODE_HOLD]
        for side, col, sym in (("매수", _COL_BUY, "triangle-up"), ("매도", _COL_SELL, "triangle-down")):
            t = h[h["구분"] == side]
            fig.add_trace(go.Scatter(x=t["날짜"], y=t["가격"], mode="markers", name=f"홀딩 {side}",
                                     marker=dict(color=col, size=9, symbol=sym)))
    fig.update_layout(title="가격 · 200일선 · 홀딩 진입/청산", height=380,
                      margin=dict(l=10, r=10, t=45, b=10),
                      yaxis_type="log" if log_scale else "linear",
                      legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"))
    return fig


def _fig_dd(log: pd.DataFrame) -> go.Figure:
    up = log["좌당가"].to_numpy(dtype=float)
    dd = (up / np.maximum.accumulate(up) - 1) * 100
    px = log["종가"].to_numpy(dtype=float)
    bdd = (px / np.maximum.accumulate(px) - 1) * 100
    d = pd.to_datetime(log["날짜"])
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=d, y=bdd, name="B&H", line=dict(color=_COL_BH, width=1)))
    fig.add_trace(go.Scatter(x=d, y=dd, name="전략", fill="tozeroy",
                             line=dict(color=_COL_STRAT, width=1.2)))
    fig.update_layout(title="낙폭 (Drawdown, %)", height=280, margin=dict(l=10, r=10, t=45, b=10),
                      legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"))
    return fig


def _summary_cards(log: pd.DataFrame, trades: pd.DataFrame):
    s = compute_stats(log)
    bh_cagr, bh_mdd = _bh_stats(log)
    n_hold = int(((trades["모드"] == MODE_HOLD) & (trades["구분"] == "매도")).sum()) if len(trades) else 0
    n_def = int(((trades["모드"] == MODE_DEF) & (trades["구분"] == "매도")).sum()) if len(trades) else 0
    st.markdown(_stat_cards([
        ("CAGR", f"{s['CAGR']:.1f}%", f"B&H {bh_cagr:.1f}%", "#888"),
        ("MDD", f"{s['MDD']:.1f}%", f"B&H {bh_mdd:.1f}%", "#888"),
        ("Calmar", f"{s['Calmar']:.2f}", "CAGR ÷ |MDD|", "#888"),
        ("총수익률", f"{s['수익률']:,.0f}%", f"최종 ${s['최종자산']:,.0f}", "#888"),
        ("홀딩 모드 비중", f"{s['홀딩일비중']:.0f}%", f"홀딩 청산 {n_hold}회", "#888"),
        ("방어 매도", f"{n_def}회", "MA 일괄 · MOC 포함", "#888"),
    ], tone="blue"), unsafe_allow_html=True)
    return s


# ══════════════════════════════════════════════
# 탭1: 백테스트
# ══════════════════════════════════════════════
def render_backtest_tab(params: dict):
    ticker = params["bt_ticker"]
    with st.spinner(f"{ticker} 가격 데이터 로드 중..."):
        df = _load_ohlc(ticker, str(params["bt_end_date"]))
    if df.empty:
        st.error(f"{ticker} 가격 데이터를 불러오지 못했습니다. 잠시 후 다시 시도해주세요.")
        return
    log, trades = _run(params, df)
    if log.empty:
        st.error("백테스트 구간에 데이터가 없습니다. 시작일/종료일을 확인하세요.")
        return
    st.caption(f"데이터: {pd.Timestamp(log['날짜'].iloc[0]).date()} ~ "
               f"{pd.Timestamp(log['날짜'].iloc[-1]).date()} ({len(log):,} 거래일 · 무조정 OHLC) · "
               f"홀딩 매수 이격도 {params['buy_lo']:g}\\~{params['buy_hi']:g}% · "
               f"매도 {params['exit_disp']:g}% / {params['tp_disp']:g}% · "
               f"방어 {params['sf_div']}분할 {params['sf_hold']}일")
    _summary_cards(log, trades)

    log_scale = st.toggle("로그 스케일", value=True, key="dq_logscale")
    st.plotly_chart(_fig_equity(log, ticker, log_scale), use_container_width=True, key="dq_ch_eq")
    st.plotly_chart(_fig_price(log, trades, log_scale), use_container_width=True, key="dq_ch_px")
    st.plotly_chart(_fig_dd(log), use_container_width=True, key="dq_ch_dd")

    c1, c2 = st.columns([1, 1])
    with c1:
        st.subheader("연도별 수익률")
        st.dataframe(yearly_returns(log), hide_index=True, use_container_width=True, height=420,
                     key="dq_tbl_year")
    with c2:
        st.subheader("모드별 실현손익")
        if len(trades):
            sells = trades[trades["구분"] == "매도"].copy()
            sells["사유"] = sells["사유"].replace({"MA": "방어 MA 매도", "MOC": "방어 MOC(만기)"})
            g = sells.groupby("사유")["실현손익"].agg(["count", "sum", "mean"]).reset_index()
            g.columns = ["사유", "횟수", "실현손익 합($)", "평균($)"]
            st.dataframe(g, hide_index=True, use_container_width=True,
                         column_config={"실현손익 합($)": st.column_config.NumberColumn(format="%.0f"),
                                        "평균($)": st.column_config.NumberColumn(format="%.0f")})
            st.caption("홀딩 청산 손익에는 홀딩으로 전환된 방어 물량의 손익도 포함됩니다.")

    st.subheader("📅 월별 수익률")
    eq = pd.Series(log["좌당가"].to_numpy(), index=pd.to_datetime(log["날짜"]))
    mode = pd.Series(log["모드"].to_numpy(), index=eq.index)
    st.markdown(monthly_perf_table(eq, mode, vmax=20.0,
                                   mode_short={MODE_HOLD: "홀", MODE_DEF: "방"}, card=True),
                unsafe_allow_html=True)

    with st.expander("📜 체결 내역 (최신순)"):
        if len(trades):
            t = trades.iloc[::-1].copy()
            t["날짜"] = pd.to_datetime(t["날짜"]).dt.date
            st.dataframe(t, hide_index=True, use_container_width=True, height=420,
                         column_config={"가격": st.column_config.NumberColumn(format="%.4f"),
                                        "금액": st.column_config.NumberColumn(format="%.0f"),
                                        "실현손익": st.column_config.NumberColumn(format="%.0f")})
    with st.expander("📜 일별 로그 (최신순)"):
        show = log.iloc[::-1].copy()
        show["날짜"] = pd.to_datetime(show["날짜"]).dt.date
        show["이격도"] = show["이격도"] * 100
        st.dataframe(show.rename(columns={"이격도": "이격도(%)"}), hide_index=True,
                     use_container_width=True, height=420)
        st.download_button("⬇️ 일별 로그 CSV", show.to_csv(index=False).encode("utf-8-sig"),
                           file_name=f"DSTQ_{ticker}_log.csv", mime="text/csv", key="dq_dl_log")


# ══════════════════════════════════════════════
# 탭2: 파라미터 최적화 (2축 격자)
# ══════════════════════════════════════════════
_OPT_AXES = {
    "매수 이격도 상한(%)": ("buy_hi", [5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 12.0]),
    "매수 이격도 하한(%)": ("buy_lo", [0.0, 0.5, 1.0, 1.5, 2.0, 3.0]),
    "매도 이격도 하한(%)": ("exit_disp", [-3.0, -2.0, -1.5, -1.0, -0.5, 0.0]),
    "매도 이격도 상한(%)": ("tp_disp", [40.0, 45.0, 50.0, 55.0, 60.0, 70.0]),
    "방어 보유기간": ("sf_hold", [5, 6, 7, 8, 9, 10, 12]),
    "방어 매수조건1(MA%)": ("sf_buy1", [-2.5, -2.0, -1.5, -1.0, -0.6, 0.0]),
    "방어 매수조건2(종가%)": ("sf_buy2", [3.0, 4.0, 5.5, 7.0, 9.0]),
    "방어 매도조건(MA%)": ("sf_sell", [0.5, 0.7, 1.0, 1.5, 2.0, 2.5]),
    "방어 MA기준": ("sf_ma_base", [2, 3, 4, 5]),
}


def render_optimization_tab(params: dict):
    st.subheader("🔍 파라미터 최적화 — 두 파라미터 격자 탐색")
    st.caption("사이드바 설정을 기준으로 두 파라미터만 바꿔 가며 성과를 비교합니다. "
               "한 점만 튀는 값보다 **주변까지 고르게 좋은 구간**을 고르세요 (과최적화 방지).")
    names = list(_OPT_AXES)
    c1, c2, c3 = st.columns([1.2, 1.2, 1])
    ax = c1.selectbox("가로축", names, index=0, key="dq_opt_x")
    ay = c2.selectbox("세로축", names, index=2, key="dq_opt_y")
    metric = c3.selectbox("지표", ["Calmar", "CAGR", "MDD"], key="dq_opt_m")
    if ax == ay:
        st.warning("가로축과 세로축을 다르게 선택하세요.")
        return
    kx, vx = _OPT_AXES[ax]
    ky, vy = _OPT_AXES[ay]
    st.caption(f"{ax}: {vx} · {ay}: {vy} → {len(vx) * len(vy)}회 백테스트")
    if st.button("▶ 탐색 실행", type="primary", key="dq_opt_run"):
        df = _load_ohlc(params["bt_ticker"], str(params["bt_end_date"]))
        if df.empty:
            st.error("가격 데이터를 불러오지 못했습니다.")
            return
        base = _engine_params(params)
        rows, prog = [], st.progress(0.0)
        combos = list(itertools.product(vx, vy))
        for n, (x, y) in enumerate(combos, 1):
            p = {**base, kx: x, ky: y}
            r = run_backtest(df, p, params["capital"], start=str(params["bt_start_date"]),
                             end=str(params["bt_end_date"]))
            s = compute_stats(r["log"])
            rows.append({"x": x, "y": y, "CAGR": s.get("CAGR", np.nan),
                         "MDD": s.get("MDD", np.nan), "Calmar": s.get("Calmar", np.nan)})
            prog.progress(n / len(combos))
        prog.empty()
        st.session_state["dq_opt_res"] = (ax, ay, kx, ky, pd.DataFrame(rows))
    res = st.session_state.get("dq_opt_res")
    if not res:
        return
    rax, ray, rkx, rky, rdf = res
    piv = rdf.pivot(index="y", columns="x", values=metric)
    fig = go.Figure(go.Heatmap(z=piv.values, x=[str(c) for c in piv.columns],
                               y=[str(i) for i in piv.index], colorscale="RdYlGn",
                               text=np.round(piv.values, 2), texttemplate="%{text}",
                               colorbar=dict(title=metric)))
    fig.update_layout(title=f"{metric} — {rax} × {ray}", xaxis_title=rax, yaxis_title=ray,
                      height=420, margin=dict(l=10, r=10, t=45, b=10))
    st.plotly_chart(fig, use_container_width=True, key="dq_ch_opt")
    cur = (params.get(rkx), params.get(rky))
    st.caption(f"현재 사이드바 값: {rax} = {cur[0]} · {ray} = {cur[1]}")
    top = rdf.sort_values("Calmar", ascending=False).head(10).rename(columns={"x": rax, "y": ray})
    st.dataframe(top, hide_index=True, use_container_width=True,
                 column_config={c: st.column_config.NumberColumn(format="%.2f")
                                for c in ("CAGR", "MDD", "Calmar")})


# ══════════════════════════════════════════════
# 계좌 기록 저장 (B방식: 과거 날짜 불변, 새 날짜만 추가 · gs_url 있으면 GSheets 동기화)
# ══════════════════════════════════════════════
def _safe(name: str) -> str:
    return (name or "기본계좌").replace(" ", "_").replace("/", "_").replace("\\", "_")


def _rec_path(kind: str, acct: str) -> str:
    return os.path.join(_CFG_DIR, f"{kind}_{_safe(acct)}.csv")


def _load_rec(kind: str, acct: str) -> pd.DataFrame:
    path = _rec_path(kind, acct)
    if os.path.exists(path):
        try:
            return pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            pass
    return pd.DataFrame()


def _save_rec(kind: str, acct: str, new_df: pd.DataFrame, gs_suffix: str) -> pd.DataFrame:
    """기존 날짜는 그대로 두고 새 날짜만 덧붙여 저장 → 병합본 반환."""
    os.makedirs(_CFG_DIR, exist_ok=True)
    new_df = new_df.copy()
    new_df["날짜"] = new_df["날짜"].astype(str)
    path = _rec_path(kind, acct)
    old = _load_rec(kind, acct)
    if not old.empty:
        old["날짜"] = old["날짜"].astype(str)
        add = new_df[~new_df["날짜"].isin(set(old["날짜"]))]
        merged = pd.concat([old, add], ignore_index=True) if not add.empty else old
    else:
        add, merged = new_df, new_df
    merged = merged.sort_values("날짜").reset_index(drop=True)
    merged.to_csv(path, index=False, encoding="utf-8-sig")

    gs_url = str(_load_cfg().get("gs_url", "")).strip()
    if gs_url and not add.empty:
        try:
            import gspread as _gs
            from common.config import _get_gspread_client
            ws_name = f"dstq_{_safe(acct)}_{gs_suffix}"
            sh = _get_gspread_client().open_by_url(gs_url)
            try:
                ws = sh.worksheet(ws_name)
                cells = ws.get_all_values()
                gs_dates = set()
                if len(cells) > 1 and "날짜" in cells[0]:
                    di = cells[0].index("날짜")
                    gs_dates = {r[di] for r in cells[1:] if len(r) > di}
                add_gs = add[~add["날짜"].isin(gs_dates)]
            except _gs.WorksheetNotFound:
                ws = sh.add_worksheet(title=ws_name, rows=5000, cols=20)
                ws.append_row(new_df.columns.tolist())
                add_gs = add
            if not add_gs.empty:
                ws.append_rows([["" if pd.isna(v) else str(v) for v in r]
                                for r in add_gs.values.tolist()], value_input_option="RAW")
        except Exception as e:
            st.warning(f"⚠️ GSheets 동기화 실패 ({gs_suffix}): {e}")
    return merged


def _move_recs(old: str, new: str | None):
    """계좌 이름 변경(new) 또는 삭제(new=None) 시 로컬 기록 파일 정리."""
    for kind in ("daily", "history"):
        op = _rec_path(kind, old)
        if os.path.exists(op):
            try:
                os.rename(op, _rec_path(kind, new)) if new else os.remove(op)
            except OSError:
                pass


def _cfg_accounts(cfg: dict) -> dict:
    """cfg 안의 계좌 dict (예전 단일 계좌 형식이면 '기본계좌'로 옮겨 담는다)."""
    accts = normalize_accounts(cfg)
    cfg["accounts"] = accts
    cfg.pop("account", None)
    return accts


def _tung_rows(rows: list) -> list:
    try:
        from dss_engine import rows_to_tungchigi_rows
    except ImportError:
        import importlib
        import dss_engine as _de
        rows_to_tungchigi_rows = importlib.reload(_de).rows_to_tungchigi_rows
    return rows_to_tungchigi_rows(rows)


def _card(label: str, value: str, sub: str = "", sub_col: str = "#888", bg: str = "#FAFAFA",
          fg: str = "#333", flex: float = 1) -> str:
    sub_html = (f'<div style="font-size:0.68em;color:{sub_col};font-weight:600">{sub}</div>'
                if sub else "")
    return (f'<div style="flex:{flex};background:{bg};border:1px solid #EEE;border-radius:10px;'
            'padding:12px 16px;text-align:center">'
            f'<div style="font-size:0.72em;color:#888;margin-bottom:2px">{label}</div>'
            f'<div style="font-size:1.1em;font-weight:700;color:{fg}">{value}</div>{sub_html}</div>')


def _is_default_params(P: dict) -> bool:
    for k in _STRAT_KEYS:
        if k == "fee":
            continue
        a, b = P.get(k), DEFAULTS[k]
        if k == "sf_weights":
            if tuple(float(x) for x in a) != tuple(float(x) for x in b):
                return False
        elif abs(float(a) - float(b)) > 1e-9:
            return False
    return True


# ══════════════════════════════════════════════
# 탭3: 오늘의 주문표 (계좌별)
# ══════════════════════════════════════════════
def render_ordersheet_tab(params: dict):
    cfg = _load_cfg()
    st.subheader(f"📋 오늘의 주문표 — {date.today()}")
    st.caption("계좌별 포트폴리오를 추적하고, 다음 거래일 주문(LOO·MOO·LOC·MOC)을 산출합니다.")
    accounts = _cfg_accounts(cfg)

    with st.expander("➕ 계좌 추가", expanded=(len(accounts) == 0)):
        a1, a2 = st.columns(2)
        add_name = a1.text_input("계좌 이름", "", key="dq_add_name", placeholder="예: PJH, 연습용")
        add_start = a2.date_input("시작일", date.today(), min_value=date(2010, 12, 1),
                                  key="dq_add_start",
                                  help="이 날부터 규칙대로 매매했다고 보고 보유·현금을 계산합니다. "
                                       "시작할 때 200일선 위로 멀리 떠 있으면 방어 모드로 시작합니다.")
        a3, a4 = st.columns(2)
        add_cap = a3.number_input("시작 자본 ($)", value=10000.0, min_value=100.0, step=1000.0,
                                  key="dq_add_cap")
        a4.text_input("종목", params["bt_ticker"], disabled=True, key="dq_add_tk",
                      help="사이드바에서 고른 종목과 현재 파라미터로 등록됩니다.")
        if st.button("✅ 계좌 등록", type="primary", key="dq_add_btn", use_container_width=True):
            nm = add_name.strip()
            if not nm:
                st.error("계좌 이름을 입력하세요.")
            elif nm in accounts:
                st.error(f"'{nm}' 계좌가 이미 존재합니다.")
            else:
                accounts[nm] = {"ticker": params["bt_ticker"], "os_start": str(add_start),
                                "os_capital": float(add_cap), "params": _engine_params(params)}
                _save_cfg(cfg)
                st.success(f"✅ '{nm}' 계좌 등록 완료.")
                st.rerun()

    keys = list(accounts.keys())
    if not keys:
        st.info("등록된 계좌가 없습니다. 위에서 계좌를 추가하세요.")
        return
    for i, (tab, key) in enumerate(zip(st.tabs([f"📊 {k}" for k in keys]), keys)):
        with tab:
            _render_account(key, accounts[key], cfg, i)


def _render_account(acct_key: str, acct: dict, cfg: dict, idx: int):
    sfx = f"a{idx}"
    ticker = str(acct.get("ticker", "TQQQ"))
    os_start = str(acct.get("os_start", date.today()))
    os_capital = float(acct.get("os_capital", 10000))
    P = account_params(acct)

    def _save():
        cfg["accounts"][acct_key] = acct
        _save_cfg(cfg)

    # ── 1) 파라미터 표시 + 수정 ──
    with st.container(border=True):
        if _is_default_params(P):
            st.markdown('<div style="margin-bottom:8px"><span style="display:inline-block;'
                        'background:linear-gradient(90deg,#FFF3E0,#FFE0B2);color:#E65100;'
                        'border:1px solid #FFB74D;border-radius:14px;padding:3px 14px;'
                        'font-size:0.9em;font-weight:700">🏷️ 🛳️ 항공모함셋 프리셋 적용 중'
                        '</span></div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="margin-bottom:8px"><span style="display:inline-block;'
                        'background:#ECEFF1;color:#546E7A;border:1px solid #B0BEC5;'
                        'border-radius:14px;padding:3px 14px;font-size:0.9em;font-weight:700">'
                        '🛠️ 커스텀 파라미터</span></div>', unsafe_allow_html=True)
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("종목", ticker)
        m2.metric("홀딩 매수 이격도", f"{P['buy_lo']:g}~{P['buy_hi']:g}%")
        m3.metric("홀딩 매도 이격도", f"{P['exit_disp']:g} / {P['tp_disp']:g}%")
        m4.metric("방어(분할/보유)", f"{P['sf_div']} / {P['sf_hold']}일")
        m5.metric("방어(매수1/2/매도)", f"{P['sf_buy1']:g}/{P['sf_buy2']:g}/{P['sf_sell']:g}")
        with st.expander("✏️ 파라미터 수정"):
            seeds = [("blo", "buy_lo"), ("bhi", "buy_hi"), ("ex", "exit_disp"), ("tp", "tp_disp"),
                     ("div", "sf_div"), ("hold", "sf_hold"), ("b1", "sf_buy1"), ("b2", "sf_buy2"),
                     ("sell", "sf_sell"), ("ratio", "sf_sell_ratio"), ("ma", "sf_ma_base")]
            for wk, pk in seeds:        # 위젯은 key 만 — 저장값으로 1회 시드
                st.session_state.setdefault(
                    f"dq_e_{wk}{sfx}", int(P[pk]) if pk in _INT_KEYS else float(P[pk]))
            st.session_state.setdefault(f"dq_e_w{sfx}", ", ".join(f"{w:g}" for w in P["sf_weights"]))
            st.caption("💡 프리셋 — 버튼을 누르면 아래 입력칸이 원본 값으로 채워집니다 (저장은 따로).")
            if st.button("🛳️ 항공모함셋 (원본)", key=f"dq_e_preset{sfx}"):
                for wk, pk in seeds:
                    st.session_state[f"dq_e_{wk}{sfx}"] = (
                        int(DEFAULTS[pk]) if pk in _INT_KEYS else float(DEFAULTS[pk]))
                st.session_state[f"dq_e_w{sfx}"] = _W_STR
                st.rerun()
            e1, e2, e3, e4 = st.columns(4)
            v_blo = e1.number_input("매수 이격도 하한(%)", step=0.5, format="%.1f", key=f"dq_e_blo{sfx}")
            v_bhi = e2.number_input("매수 이격도 상한(%)", step=0.5, format="%.1f", key=f"dq_e_bhi{sfx}")
            v_ex = e3.number_input("매도 이격도 하한(%)", step=0.5, format="%.1f", key=f"dq_e_ex{sfx}")
            v_tp = e4.number_input("매도 이격도 상한(%)", step=5.0, format="%.1f", key=f"dq_e_tp{sfx}")
            f1, f2, f3, f4 = st.columns(4)
            v_div = f1.number_input("방분할", min_value=1, max_value=10, step=1, key=f"dq_e_div{sfx}")
            v_hold = f2.number_input("방보유", min_value=1, max_value=60, step=1, key=f"dq_e_hold{sfx}")
            v_b1 = f3.number_input("방매수1(MA%)", step=0.1, format="%.1f", key=f"dq_e_b1{sfx}")
            v_b2 = f4.number_input("방매수2(종가%)", step=0.5, format="%.1f", key=f"dq_e_b2{sfx}")
            g1, g2, g3, g4 = st.columns(4)
            v_sell = g1.number_input("방매도(MA%)", step=0.1, format="%.1f", key=f"dq_e_sell{sfx}")
            v_ratio = g2.number_input("매도 비율(%)", min_value=1.0, max_value=100.0, step=5.0,
                                      format="%.1f", key=f"dq_e_ratio{sfx}")
            v_ma = g3.number_input("MA기준", min_value=2, max_value=20, step=1, key=f"dq_e_ma{sfx}")
            v_w = g4.text_input("티어비중%", key=f"dq_e_w{sfx}")
            if st.button("💾 파라미터 저장", type="primary", key=f"dq_save_p{sfx}"):
                w, werr = _parse_weights(v_w, v_div)
                if werr:
                    st.error(werr)
                else:
                    acct["params"] = {
                        "buy_lo": float(v_blo), "buy_hi": float(v_bhi), "exit_disp": float(v_ex),
                        "tp_disp": float(v_tp), "sf_div": int(v_div), "sf_hold": int(v_hold),
                        "sf_buy1": float(v_b1), "sf_buy2": float(v_b2), "sf_sell": float(v_sell),
                        "sf_sell_ratio": float(v_ratio), "sf_ma_base": int(v_ma),
                        "sf_weights": list(w), "fee": float(P["fee"])}
                    _save()
                    st.session_state.pop(f"dq_os{sfx}", None)
                    st.success("✅ 저장되었습니다. 주문표를 다시 로드하세요.")
                    st.rerun()

    # ── 2) 이름 변경 / 삭제 ──
    mg1, mg2, _ = st.columns([1, 1, 4])
    if mg1.button("✏️ 이름 변경", key=f"dq_rn_btn{sfx}"):
        st.session_state[f"dq_rn{sfx}"] = True
    if mg2.button("🗑️ 계좌 삭제", key=f"dq_del_btn{sfx}"):
        st.session_state[f"dq_del{sfx}"] = True
    if st.session_state.get(f"dq_rn{sfx}"):
        r1, r2, r3, _ = st.columns([2, 1, 1, 3])
        new_nm = r1.text_input("새 이름", acct_key, key=f"dq_newnm{sfx}")
        if r2.button("✅", key=f"dq_rn_ok{sfx}"):
            nm = new_nm.strip()
            if nm and nm not in cfg["accounts"]:
                cfg["accounts"] = {(nm if k == acct_key else k): v
                                   for k, v in cfg["accounts"].items()}
                _save_cfg(cfg)
                _move_recs(acct_key, nm)
                st.session_state.pop(f"dq_rn{sfx}", None)
                st.rerun()
        if r3.button("❌", key=f"dq_rn_no{sfx}"):
            st.session_state.pop(f"dq_rn{sfx}", None)
            st.rerun()
    if st.session_state.get(f"dq_del{sfx}"):
        st.warning(f"⚠️ **{acct_key}** 계좌를 삭제하시겠습니까? (이 기기의 기록 파일 포함)")
        d1, d2, _ = st.columns([1, 1, 4])
        if d1.button("✅ 삭제", type="primary", key=f"dq_del_ok{sfx}"):
            del cfg["accounts"][acct_key]
            _save_cfg(cfg)
            _move_recs(acct_key, None)
            for k in (f"dq_del{sfx}", f"dq_os{sfx}"):
                st.session_state.pop(k, None)
            st.rerun()
        if d2.button("❌ 취소", key=f"dq_del_no{sfx}"):
            st.session_state.pop(f"dq_del{sfx}", None)
            st.rerun()

    # ── 3) 시작일 / 자본 ──
    s1, s2, s3 = st.columns([2, 2, 1])
    in_start = s1.date_input("시작일", pd.to_datetime(os_start).date(),
                             min_value=date(2010, 12, 1), key=f"dq_start{sfx}")
    in_cap = s2.number_input("시작 자본 ($)", value=os_capital, min_value=100.0, step=1000.0,
                             key=f"dq_cap{sfx}")
    if s3.button("💾 저장", key=f"dq_savesc{sfx}"):
        acct["os_start"], acct["os_capital"] = str(in_start), float(in_cap)
        _save()
        st.session_state.pop(f"dq_os{sfx}", None)
        st.success("저장됨")
        st.rerun()

    # ── 4) 자본 조정 (시작 자본 os_capital 은 불변 — 조정은 별도 이력) ──
    with st.expander("💰 자본 조정 (증액 / 감액)"):
        st.caption("증액: 양수 · 감액: 음수. **날짜 = 미국장 종료일 기준** → 그 날 종료 후 입금/출금으로 "
                   "보아 **다음 거래일부터** 반영됩니다. (손익률은 좌당가 방식으로 조정분 제외)")
        adj = account_adjustments(acct)
        ad1, ad2 = st.columns([2, 1])
        adj_date = ad1.date_input("적용 날짜", date.today(), min_value=date(2010, 12, 1),
                                  key=f"dq_adj_d{sfx}")
        adj_amt = ad1.number_input("조정 금액 ($)", value=0.0, step=500.0, key=f"dq_adj_a{sfx}")
        adj_memo = ad1.text_input("메모 (선택)", value="", key=f"dq_adj_m{sfx}",
                                  placeholder="예: 11월 적립 / 일부 출금")
        if ad2.button("💰 적용", key=f"dq_adj_btn{sfx}", disabled=(adj_amt == 0)):
            adj.append({"날짜": adj_date.strftime("%Y-%m-%d"), "조정금액": float(adj_amt),
                        "누적자본금": 0.0,
                        "메모": adj_memo.strip() or ("증액" if adj_amt > 0 else "감액")})
            adj, _fc = recalc_adj_history(adj, os_capital)
            acct["capital_adj_history"] = json.dumps(adj, ensure_ascii=False)
            _save()
            st.session_state.pop(f"dq_os{sfx}", None)
            st.rerun()
        if adj:
            dfa = pd.DataFrame(adj)
            ds1, ds2 = st.columns([3, 1])
            ds1.dataframe(dfa[[c for c in ("날짜", "조정금액", "누적자본금", "메모") if c in dfa.columns]],
                          use_container_width=True, hide_index=True)
            opts = [f"{a['날짜']} {a['조정금액']:+,.0f} ({a.get('메모', '')})" for a in adj]
            sel = ds2.selectbox("삭제할 항목", ["—"] + opts, key=f"dq_adj_del_sel{sfx}")
            if sel != "—" and ds2.button("🗑️ 삭제", key=f"dq_adj_del{sfx}"):
                adj.pop(opts.index(sel))
                adj, _fc = recalc_adj_history(adj, os_capital) if adj else ([], os_capital)
                acct["capital_adj_history"] = json.dumps(adj, ensure_ascii=False)
                _save()
                st.session_state.pop(f"dq_os{sfx}", None)
                st.rerun()

    # ── 5) 주문표 로드 ──
    if st.button("📋 주문표 로드", type="primary", key=f"dq_load{sfx}", use_container_width=True):
        try:
            df = _load_ohlc(ticker, str(date.today()))
            if df.empty:
                raise RuntimeError(f"{ticker} 가격 데이터를 불러오지 못했습니다 (야후 일시 장애 가능)")
            _nd = next_trading_date(pd.Timestamp(df.index[-1]).date() + timedelta(days=1))
            r = compute_account_orders(df, acct, next_day=_nd)
            if r.get("error"):
                raise RuntimeError(r["error"])
            if not r["log"].empty:
                try:    # B방식 누적: 이번 로드의 일별 행을 계좌 기록에 추가 (기존 날짜 불변)
                    _save_rec("daily", acct_key, daily_view(r["log"]), "일별매매")
                except Exception:
                    pass
        except Exception as e:
            st.error(f"주문표 생성 실패: {e}")
            return
        st.session_state[f"dq_os{sfx}"] = r

    r = st.session_state.get(f"dq_os{sfx}")
    if r is None:
        st.info("👆 '주문표 로드'를 클릭하면 다음 거래일 주문이 생성됩니다.")
        return

    # ── 요약 카드 ──
    base_day = pd.Timestamp(r["base_date"]).date()
    next_day = next_trading_date(base_day + timedelta(days=1))
    if (date.today() - base_day).days > 5:
        st.warning(f"⚠️ 마지막 확정 종가가 {base_day} 입니다. 데이터가 최신인지 확인하세요.")
    if r.get("first_day"):
        st.info(f"🚩 **운용 첫날** — 시작일({os_start})의 첫 주문입니다. 전액 현금·방어 모드에서 시작하며, "
                "홀딩 진입 여부는 오늘 종가부터 판단합니다.")
    will_hold = r["signal"] == "buy" or (r["mode"] == MODE_HOLD and r["signal"] != "sell")
    is_hold = r["mode"] == MODE_HOLD
    buy_px = next((o["가격"] for o in r["orders"] if o["구분"] == "매수"), None)
    sell_px = next((o["가격"] for o in r["orders"] if o["구분"] == "매도" and o["가격"]), None)
    cond = (f"${buy_px:,.2f}" if buy_px else "-") + " / " + (f"${sell_px:,.2f}" if sell_px else "-")
    n_hold = "홀딩" if r["hold_qty"] else f"{len(r['lots'])} / {P['sf_div']}"
    st.markdown(f"<div style='font-size:0.82em;color:#888;margin-bottom:4px'>데이터 기준: "
                f"<b>{base_day}</b> (${r['close']:.2f})</div>", unsafe_allow_html=True)
    st.markdown(
        '<div style="display:flex;gap:10px;margin-bottom:10px">'
        + _card("현재 모드", ("🟧 홀딩모드" if is_hold else "🟩 방어모드"),
                {"buy": "내일 시가 홀딩 진입", "sell": "내일 시가 전량 매도"}.get(r["signal"], ""),
                "#C62828", bg=("#FFF3E0" if is_hold else "#E8F5E9"),
                fg=("#E65100" if is_hold else "#2E7D32"), flex=1.2)
        + _card("200일선 이격도", f"{r['disp'] * 100:+.2f}%", f"200MA ${r['ma']:,.2f}")
        + _card("보유 티어 / 분할수", n_hold)
        + _card("매수가 / 매도가", cond) + "</div>", unsafe_allow_html=True)

    held = ([(r["hold_cost"] / r["hold_qty"], r["hold_qty"])] if r["hold_qty"]
            else [(l["매수가"], l["수량"]) for l in r["lots"]])
    ev_pnl = sum((r["close"] - bp) * q for bp, q in held)
    g, rd = "#2E7D32", "#C62828"
    st.markdown(
        '<div style="display:flex;gap:10px;margin-bottom:8px">'
        + _card("시작 자본", f"${os_capital:,.0f}",
                f"+ 조정 ${r['net_adj']:+,.0f}" if abs(r["net_adj"]) > 0.01 else "", "#0B7A3E")
        + _card("총자산", f"${r['total']:,.0f}", f"{r['return_pct']:+.2f}% (좌당가)",
                g if r["return_pct"] >= 0 else rd)
        + _card("평가손익", f"${ev_pnl:+,.0f}", fg=g if ev_pnl >= 0 else rd)
        + _card("누적실현손익", f"${r['cum_realized']:+,.0f}", f"매도 {r['n_sells']}회",
                fg=g if r["cum_realized"] >= 0 else rd)
        + _card("예수금", f"${r['cash']:,.0f}") + "</div>", unsafe_allow_html=True)

    # ── 주문 ──
    st.divider()
    st.subheader(f"📑 오늘의 주문  ({next_day})")
    st.markdown(f"<div style='font-size:0.85em;color:#888;margin-bottom:8px'>"
                f"기준종가({base_day}) = <b>${r['close']:,.2f}</b>&ensp;·&ensp;"
                f"모드 <b>{r['mode']}</b>&ensp;·&ensp;매수가 / 매도가 <b>{cond}</b></div>",
                unsafe_allow_html=True)
    raw_rows = order_rows(r)
    if raw_rows:
        rows = []
        for o in r["orders"]:
            if int(o["수량"]) <= 0:
                continue
            icon = ("🔴" if o["방법"] in ("MOC", "MOO") else "🔵") if o["구분"] == "매도" else "🟠"
            rows.append({"구분": f"{icon} {o['방법']}{o['구분']}", "사유": o["티어"],
                         "주문가": ("시장가(시가)" if o["방법"] == "MOO" else "시장가(종가)")
                         if o["가격"] is None else f"${o['가격']:,.2f}",
                         "수량": f"{o['수량']:,}주", "예상금액": f"${o['예상금액']:,.0f}",
                         "비고 (계산 근거)": o["비고"]})

        def _style(row):
            s = [""] * len(row)
            v = row["구분"]
            s[0] = ("color:#C62828;font-weight:bold" if "🔴" in v else
                    "color:#1565C0;font-weight:bold" if "🔵" in v else
                    "color:#E65100;font-weight:bold")
            return s
        t1, t2 = st.tabs(["📋 매일 주문", "🔄 퉁치기 주문 (자전거래 회피)"])
        with t1:
            st.dataframe(pd.DataFrame(rows).style.apply(_style, axis=1), use_container_width=True,
                         hide_index=True, height=38 + 35 * len(rows))
            st.caption("💡 **LOO** = 개장 지정가(시가 체결) · **MOO** = 개장 시장가 · **LOC** = 종가 지정가 "
                       "(매수는 주문가 이하, 매도는 주문가 이상 종가에 체결) · **MOC** = 만기 도래분 종가 청산. "
                       "홀딩 진입·청산은 시가, 방어 매매는 종가 체결입니다. MOC 가 있는 날은 LOC 매도가 보류됩니다.")
            if r["signal"] == "sell":
                st.info("ℹ️ 시가 매도 대금이 들어온 뒤 현금이 확정되므로, 방어 LOC 매수 수량은 **예상치**입니다. "
                        "개장 후 실제 예수금 × 티어 비중 ÷ 매수가로 다시 계산해 주문하세요.")
            if (not is_hold and not r["signal"] and any(l["티어"] == "방T1" for l in r["lots"])
                    and r["close"] > r["ma5"]
                    and not any(o["구분"] == "매도" and "방T1" in o["티어"] for o in r["orders"])):
                st.info(f"ℹ️ **전일 종가가 MA(5)보다 크면 방T1 은 매도하지 않습니다.** "
                        f"(전일종가 ${r['close']:,.2f} > MA5 ${r['ma5']:,.2f})")
        with t2:
            if not can_tungchigi(raw_rows):
                st.info("오늘 주문에는 **시가 주문(LOO/MOO)** 이 있어 퉁치기를 적용하지 않습니다. "
                        "매일 주문 그대로 사용하세요.")
            else:
                try:
                    tng = _tung_rows(raw_rows)
                    same = sorted(map(tuple, tng)) == sorted(map(tuple, raw_rows))
                    if same:
                        st.success("✅ 매일 주문과 퉁치기 결과가 동일 — 어느 증권사든 매일 주문 그대로 "
                                   "사용 가능합니다.")
                    else:
                        st.warning("⚠️ 매일 주문에 매수/매도 상계 구간 있음 (자전거래 위험) — "
                                   "자전거래 거부 증권사는 아래 퉁치기 주문을 사용하세요.")
                    st.dataframe(pd.DataFrame([{
                        "구분": ("🔴 MOC매도" if t[1] == "MOC" else
                                 "🔵 LOC매도" if t[0] == "매도" else "🟠 LOC매수"),
                        "주문가": "시장가(종가)" if t[1] == "MOC" else f"${float(t[2]):,.2f}",
                        "수량": f"{int(t[3]):,}주"} for t in tng]),
                        use_container_width=True, hide_index=True)
                    st.caption("※ 어떤 종가에도 순 체결 결과는 매일 주문과 동일합니다 (상계 제거).")
                except Exception as e:
                    st.error(f"퉁치기 계산 실패: {e}")
    else:
        st.info("💤 주문 없음 — " + ("홀딩 유지 (청산 신호가 날 때까지 그대로 보유)"
                                 if is_hold else "관망 (매수 대기 또는 현금 부족)"))

    # ── 다음 종가 트리거 ──
    t = r["trig"]
    st.markdown(f"##### 🎯 {next_day} 종가가 얼마면 모드가 바뀌나")
    if will_hold:
        trows = [{"신호": f"🔴 홀딩 청산 (이격도 < {P['exit_disp']:g}%)",
                  "종가 조건": f"${t['exit']:,.2f} 미만",
                  "현재가 대비": f"{(t['exit'] / r['close'] - 1) * 100:+.2f}%"},
                 {"신호": f"🟠 과열 청산 (이격도 > {P['tp_disp']:g}%)",
                  "종가 조건": f"${t['tp']:,.2f} 초과",
                  "현재가 대비": f"{(t['tp'] / r['close'] - 1) * 100:+.2f}%"}]
    else:
        trows = [{"신호": f"🟢 홀딩 진입 (이격도 {P['buy_lo']:g}%~{P['buy_hi']:g}%)",
                  "종가 조건": f"${t['buy_lo']:,.2f} 초과 ~ ${t['buy_hi']:,.2f} 미만",
                  "현재가 대비": f"{(t['buy_lo'] / r['close'] - 1) * 100:+.2f}% ~ "
                              f"{(t['buy_hi'] / r['close'] - 1) * 100:+.2f}%"}]
    st.dataframe(pd.DataFrame(trows), hide_index=True, use_container_width=True)
    if not will_hold and r["slope"] <= 0:
        st.caption("※ 현재 200일선 기울기가 0 이하라, 종가가 진입 구간에 들어와도 "
                   "기울기가 양수로 돌아서기 전에는 홀딩 진입하지 않습니다.")

    # ── 보유 ──
    st.markdown("##### 📦 현재 보유")
    if r["hold_qty"]:
        avg = r["hold_cost"] / r["hold_qty"]
        st.dataframe(pd.DataFrame([{
            "모드": "홀딩", "진입일": pd.Timestamp(r["hold_start"]).date(),
            "수량": f"{r['hold_qty']:,}주", "평단": f"${avg:,.2f}", "매도": "이격도 청산 신호 시 시가",
            "평가손익": f"{(r['close'] / avg - 1) * 100:+.1f}%"}]),
            hide_index=True, use_container_width=True)
    elif r["lots"]:
        st.dataframe(pd.DataFrame([{
            "모드": "방어", "티어": l["티어"], "매수일": pd.Timestamp(l["매수일"]).date(),
            "매수가": f"${l['매수가']:,.2f}", "수량": f"{l['수량']:,}주",
            "만기(MOC)까지": f"{l['남은일']}거래일",
            "평가손익": f"{(r['close'] / l['매수가'] - 1) * 100:+.1f}%"} for l in r["lots"]]),
            hide_index=True, use_container_width=True)
    else:
        st.info("현재 보유 없음 (전량 현금).")

    # ── 일별 매매 상세표 (B방식 누적 기록) ──
    st.markdown("##### 📑 일별 매매 상세표")
    daily = _load_rec("daily", acct_key)
    if daily.empty:
        daily = daily_view(r["log"])
    if not daily.empty:
        daily = daily.copy()
        daily["날짜"] = daily["날짜"].astype(str)
        nb = int((pd.to_numeric(daily.get("매수량"), errors="coerce") > 0).sum())
        ns = int((pd.to_numeric(daily.get("매도량"), errors="coerce") > 0).sum())
        st.caption(f"기록 {daily['날짜'].iloc[0]} ~ {daily['날짜'].iloc[-1]} · "
                   f"총 {len(daily)}일 (매수 {nb}회 / 매도 {ns}회)")
        st.info("ℹ️ 이 기록은 **주문표 로드 시점마다 누적 저장**된 데이터입니다 (B방식). "
                "파라미터를 변경해도 **과거 기록은 변경되지 않습니다.**")
        show = daily.iloc[::-1].reset_index(drop=True)
        st.dataframe(show, use_container_width=True, hide_index=True,
                     height=min(38 + 35 * len(show), 480))
        dl1, dl2 = st.columns(2)
        dl1.download_button("📥 CSV 다운로드", show.to_csv(index=False).encode("utf-8-sig"),
                            f"dstq_daily_{acct_key}.csv", "text/csv", key=f"dq_dldaily{sfx}")
        try:
            import io as _io
            buf = _io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as w:
                show.to_excel(w, index=False, sheet_name="일별매매")
            dl2.download_button("📊 엑셀 다운로드", buf.getvalue(), f"dstq_daily_{acct_key}.xlsx",
                                key=f"dq_dlxls{sfx}",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception:
            pass

    # ── 일일 주문표 스냅샷 ──
    h1, h2, _ = st.columns([1, 1, 2])
    if h1.button("💾 오늘 주문 기록 저장", key=f"dq_savehist{sfx}"):
        snap = {"날짜": str(next_day), "기준종가": round(r["close"], 2), "모드": r["mode"],
                "신호": r["signal"] or "", "보유수량": r["hold_qty"] + sum(l["수량"] for l in r["lots"]),
                "매수주문": sum(1 for o in r["orders"] if o["구분"] == "매수"),
                "매도주문": sum(1 for o in r["orders"] if o["구분"] == "매도"),
                "현금": round(r["cash"]), "총자산": round(r["total"]),
                "누적실현": round(r["cum_realized"])}
        _save_rec("history", acct_key, pd.DataFrame([snap]), "매매기록")
        st.success(f"✅ {next_day} 기록 저장됨")
    tok, chat = str(cfg.get("tg_token", "")).strip(), str(cfg.get("tg_chat_id", "")).strip()
    if h2.button("📨 텔레그램 전송", key=f"dq_tg_send{sfx}", disabled=not (tok and chat),
                 help=None if tok and chat else "개인 설정 탭에서 텔레그램을 먼저 저장하세요"):
        res = _send_telegram(tok, chat, build_order_text(ticker, r, next_day, acct_key))
        (st.success if res.get("ok") else st.error)(
            "전송 완료" if res.get("ok") else f"전송 실패: {res.get('description', '알 수 없는 오류')}")
    hist = _load_rec("history", acct_key)
    if not hist.empty:
        with st.expander(f"📋 누적 기록 ({len(hist)}건)"):
            st.dataframe(hist.iloc[::-1].reset_index(drop=True), use_container_width=True,
                         hide_index=True, height=min(38 + 35 * len(hist), 400))
            st.download_button("📥 기록 CSV", hist.to_csv(index=False).encode("utf-8-sig"),
                               f"dstq_{acct_key}.csv", "text/csv", key=f"dq_dlhist{sfx}")
    st.caption("⚠️ 보유·현금은 **시작일부터 규칙대로 매매했다고 가정한 모델** 기준입니다. "
               "실제 체결(시가·수량)이 다르면 조금씩 차이가 날 수 있습니다.")


# ══════════════════════════════════════════════
# 탭4: 전략 소개 & 성과
# ══════════════════════════════════════════════
def render_intro_tab(params: dict):
    st.subheader("🛳️ 듀얼스나이퍼 TQ — 200일선 홀딩 + 방어 스나이퍼")
    st.markdown(f"""
**한 줄 요약** — TQQQ가 **200일선 위로 막 올라설 때는 몰빵해서 들고 가고(홀딩)**,
그 외에는 **듀얼스나이퍼 방어 로직으로 떨어질 때만 조금씩 사서 반등에 파는(방어)** 혼합 전략입니다.
추세 구간은 통째로 먹고, 추세가 없는 구간은 현금 위주로 버티며 잔수익을 쌓습니다.

---
#### 🧭 모드는 어떻게 정해지나
| | 조건 (종가 확정 후 판단) | 다음 거래일 행동 |
|---|---|---|
| 🟧 **홀딩 진입** | 방어 중 · 이격도 **{params['buy_lo']:g}% 초과 ~ {params['buy_hi']:g}% 미만** · 200일선 기울기(5일) > 0 | **시가**에 남은 현금 전부 매수 (LOO, 전일종가 +10% 지정가) |
| 🟩 **방어 전환** | 홀딩 중 · 이격도 **{params['exit_disp']:g}% 미만** 이탈 또는 **{params['tp_disp']:g}% 초과** 과열 | **시가**에 전량 매도 (MOO) → 그날 종가부터 방어 매매 |

- **이격도** = 종가 ÷ 200일 이동평균 − 1
- 홀딩 진입 때 들고 있던 **방어 물량은 팔지 않고 그대로 홀딩분으로 합칩니다.**
- LOO 수량 = 현금 ÷ (전일종가 × 1.10) 이라, 몰빵해도 예수금이 조금 남습니다.
- 처음 시작할 때 이미 200일선 위로 멀리({params['buy_hi']:g}% 이상) 떠 있으면 진입 조건이 안 맞아
  **방어 모드로 시작**합니다. 그래서 시작 시점에 따라 초반 모드가 사람마다 다를 수 있습니다.

---
#### 🟩 방어 모드 (종가 LOC)
| 항목 | 규칙 |
|---|---|
| 매수가 | min( 종가가 {params['sf_ma_base']}일선보다 **{params['sf_buy1']:g}%** 인 가격, 전일종가 × (1 + **{params['sf_buy2']:g}%**) ) — 종가가 이 값 이하면 체결 |
| 매수 수량 | 하루 1티어 · 할당금 = 현금 × (티어 비중 ÷ 남은 티어 비중합) · 비중 **{' / '.join(f'{w:g}' for w in params['sf_weights'])}** |
| 매도 | 종가가 {params['sf_ma_base']}일선보다 **+{params['sf_sell']:g}%** 이상이면 방어분 **일괄 매도** |
| 1티어 보류 | 전일종가 > 전일 5일선이면 방T1 은 팔지 않고 보류 |
| 만기 | 매수 후 **{params['sf_hold']}거래일**째 그 티어만 종가 청산(MOC) · 이 날은 MA 매도 보류 |

---
#### ✅ 원본 대조 검증 (항공모함셋_TQQQ.xlsx · 2011-01-03 ~ 2026-11-10)
| 항목 | 결과 |
|---|---|
| 일별 모드 (홀딩/방어) | **3,988일 100% 일치** |
| 매수 티어 | **100% 일치** (방어 475건 + 홀딩 진입 28건) |
| 매수·매도 수량 | 99.8% 일치 (차이는 시가 데이터 소스 차이로 1~수십 주) |
| 최종 자산 | 원본 $18,625,446 vs 엔진 $18,615,251 (**−0.05%**) |
| CAGR / MDD | **60.8% / −32.0%** (원본과 동일) |
""")
    st.divider()
    st.subheader("📊 전략 성과 분석")
    st.caption("파라미터를 고르고 실행하면 핵심 지표부터 회복력·롤링·모드별 성과·민감도·강건성까지 분석합니다.")
    src = st.radio("파라미터", ["🧭 사이드바 설정값", "🛳️ 항공모함셋 (원본)"], horizontal=True,
                   key="dq_intro_src")
    P = dict(params)
    if src.startswith("🛳️"):
        P.update({k: (list(v) if isinstance(v, tuple) else v) for k, v in DEFAULTS.items()
                  if k in _STRAT_KEYS})
    with st.expander("🔎 적용 파라미터 확인"):
        st.markdown(
            f"- **종목 / 기간**: {P['bt_ticker']} · {P['bt_start_date']} ~ {P['bt_end_date']} · "
            f"초기자본 ${P['capital']:,.0f}\n"
            f"- **홀딩**: 매수 이격도 {P['buy_lo']:g}%\\~{P['buy_hi']:g}% · "
            f"매도 {P['exit_disp']:g}% 미만 / {P['tp_disp']:g}% 초과\n"
            f"- **방어**: 분할 {P['sf_div']} · 보유 {P['sf_hold']}일 · 매수1 {P['sf_buy1']:g}% · "
            f"매수2 {P['sf_buy2']:g}% · 매도 {P['sf_sell']:g}% · MA{P['sf_ma_base']}\n"
            f"- **티어비중**: {', '.join(f'{w:g}' for w in P['sf_weights'])}")
    if st.button("▶ 성과 분석 실행", type="primary", key="dq_intro_run", use_container_width=True):
        df = _load_ohlc(P["bt_ticker"], str(P["bt_end_date"]))
        if df.empty:
            st.error("가격 데이터를 불러오지 못했습니다. 잠시 후 다시 시도해주세요.")
            return
        with st.spinner("백테스트 실행 중..."):
            log, trades = _run(P, df)
        if log.empty:
            st.warning("백테스트 구간에 데이터가 없습니다. 시작일/종료일을 확인하세요.")
            return
        st.session_state["dq_perf"] = (log, trades, P)
        for k in ("dq_sens", "dq_mc"):
            st.session_state.pop(k, None)
    perf = st.session_state.get("dq_perf")
    if not perf:
        st.info("👆 '성과 분석 실행' 버튼을 클릭하면 종합 성과를 확인할 수 있습니다.")
        return
    _render_perf(*perf)


def _episodes(dates, assets, thr: float = -10.0) -> list:
    """고점 대비 thr% 이상 하락한 에피소드 (고점·저점·회복 시점)."""
    rec = []
    pv, pi, indd, tv, ti = float(assets[0]), 0, False, float(assets[0]), 0
    for i in range(1, len(assets)):
        cur = float(assets[i])
        if not indd:
            if cur > pv:
                pv, pi = cur, i
            elif (cur - pv) / pv * 100 <= thr:
                indd, tv, ti = True, cur, i
        else:
            if cur < tv:
                tv, ti = cur, i
            if cur >= pv:
                rec.append((pi, ti, i, pv, tv))
                indd, pv, pi, tv, ti = False, cur, i, cur, i
    if indd:
        rec.append((pi, ti, None, pv, tv))
    return [{"고점": str(dates[a].date()), "고점 자산": f"${p_:,.0f}",
             "최대하락 시점": str(dates[b].date()), "저점 자산": f"${t_:,.0f}",
             "하락율": f"{(t_ - p_) / p_ * 100:.1f}%",
             "회복 시점": str(dates[c].date()) if c is not None else "미회복 ⏳",
             "기간(일)": ((dates[c] if c is not None else dates[-1]) - dates[a]).days}
            for a, b, c, p_, t_ in rec]


def _render_perf(log: pd.DataFrame, trades: pd.DataFrame, P: dict):
    from common.analysis import compute_sharpe_sortino
    ticker = P["bt_ticker"]
    cap = float(P["capital"])
    dates = pd.to_datetime(log["날짜"]).reset_index(drop=True)
    assets = log["총자산"].to_numpy(dtype=float)
    px = log["종가"].to_numpy(dtype=float)
    bh = px / px[0] * cap
    s = compute_stats(log)
    bh_cagr, bh_mdd = _bh_stats(log)
    sharpe, sortino = compute_sharpe_sortino(assets)
    sells = trades[trades["구분"] == "매도"].copy() if len(trades) else pd.DataFrame()
    hold_s = sells[sells["모드"] == MODE_HOLD] if len(sells) else sells
    def_s = sells[sells["모드"] == MODE_DEF] if len(sells) else sells
    n_days = len(log)
    win = float((sells["실현손익"] > 0).mean() * 100) if len(sells) else 0.0

    # ── 요약 인사이트 ──
    dd_all = assets / np.maximum.accumulate(assets) - 1
    worst_i = int(np.argmin(dd_all))
    yr = yearly_returns(log)
    neg_years = int((yr["수익률(%)"] < 0).sum()) if len(yr) else 0
    hold_pnl = float(hold_s["실현손익"].sum()) if len(hold_s) else 0.0
    def_pnl = float(def_s["실현손익"].sum()) if len(def_s) else 0.0
    tot_pnl = hold_pnl + def_pnl
    st.markdown(f"""
> **💡 한눈에 보기**
> - 연평균 **{s['CAGR']:.1f}%** · 최대 낙폭 **{s['MDD']:.1f}%** (가장 깊었던 날 {dates[worst_i].date()}) —
>   같은 기간 {ticker} 보유는 연 {bh_cagr:.1f}% · 낙폭 {bh_mdd:.1f}%
> - {len(yr)}개 연도 중 손실 연도 **{neg_years}회** · 전체 기간의 **{s['홀딩일비중']:.0f}%** 를 홀딩 모드로 보냄
> - 실현손익의 **{(hold_pnl / tot_pnl * 100 if tot_pnl else 0):.0f}%** 는 홀딩(추세), **{(def_pnl / tot_pnl * 100 if tot_pnl else 0):.0f}%** 는 방어(역추세)에서 나옴
""")

    # ── 핵심 지표 ──
    st.markdown("##### 📈 핵심 지표")
    c = st.columns(5)
    c[0].metric("최종 자산", f"${s['최종자산']:,.0f}")
    c[1].metric("총수익률", f"{s['수익률']:,.0f}%")
    c[2].metric("CAGR", f"{s['CAGR']:.1f}%", delta=f"B&H 대비 {s['CAGR'] - bh_cagr:+.1f}%p")
    c[3].metric("MDD", f"{s['MDD']:.1f}%", delta=f"B&H {bh_mdd:.1f}%", delta_color="off")
    c[4].metric("Calmar", f"{s['Calmar']:.2f}")
    c = st.columns(5)
    c[0].metric("Sharpe", f"{sharpe:.2f}")
    c[1].metric("Sortino", f"{sortino:.2f}")
    c[2].metric("승률 (매도 기준)", f"{win:.1f}%")
    c[3].metric("매도횟수", f"{len(sells):,}회")
    c[4].metric("평균보유", f"{sells['보유일'].mean():.1f}일" if len(sells) else "-")
    c = st.columns(5)
    c[0].metric("거래일수", f"{n_days:,}일")
    c[1].metric("투자기간", f"{s['기간(년)']:.1f}년")
    c[2].metric("홀딩일 비중", f"{s['홀딩일비중']:.0f}%")
    c[3].metric("평균손익/건", f"${sells['실현손익'].mean():,.0f}" if len(sells) else "-")
    bh_ret = (bh[-1] / cap - 1) * 100
    c[4].metric("B&H 수익률", f"{bh_ret:,.0f}%", delta=f"{s['수익률'] - bh_ret:+,.0f}%p")

    # ── 자산 곡선 · 드로다운 ──
    st.markdown("##### 💰 자산 곡선 (로그 스케일)")
    st.plotly_chart(_fig_equity(log, ticker, True), use_container_width=True, key="dq_pf_eq")
    st.markdown("##### 📉 드로다운")
    st.plotly_chart(_fig_dd(log), use_container_width=True, key="dq_pf_dd")

    # ── 연도별 ──
    st.markdown("##### 📅 연도별 성과")
    ca1, ca2 = st.columns([1, 1])
    h_ = min(38 + 35 * len(yr), 600)
    ca1.dataframe(yr, use_container_width=True, hide_index=True, height=h_, key="dq_pf_year")
    figY = go.Figure()
    figY.add_trace(go.Bar(x=yr["연도"], y=yr["수익률(%)"], name="전략",
                          marker_color=[_COL_BUY if v >= 0 else _COL_SELL for v in yr["수익률(%)"]],
                          text=[f"{v:.0f}" for v in yr["수익률(%)"]], textposition="outside"))
    figY.add_trace(go.Scatter(x=yr["연도"], y=yr["종목(%)"], name=f"{ticker} 보유", mode="lines+markers",
                              line=dict(color=_COL_BH, dash="dot")))
    figY.add_hline(y=0, line_dash="dash", line_color="gray")
    figY.update_layout(height=h_, margin=dict(l=0, r=0, t=10, b=0),
                       legend=dict(orientation="h", y=1.05))
    ca2.plotly_chart(figY, use_container_width=True, key="dq_pf_ybar")

    # ── 월별 히트맵 ──
    st.markdown("##### 🗓️ 월별 수익률 히트맵")
    eq = pd.Series(assets, index=dates)
    md = pd.Series(log["모드"].to_numpy(), index=dates)
    st.markdown(monthly_perf_table(eq, md, vmax=20.0, mode_short={MODE_HOLD: "홀", MODE_DEF: "방"},
                                   card=True), unsafe_allow_html=True)

    # ── 거래 통계 ──
    if len(sells):
        st.markdown("##### 🧾 거래 통계")
        wins, losses = sells[sells["실현손익"] > 0], sells[sells["실현손익"] <= 0]
        t = st.columns(4)
        t[0].metric("익절 / 손절", f"{len(wins)} / {len(losses)}")
        t[1].metric("평균 익절 / 손절",
                    f"${wins['실현손익'].mean():,.0f} / ${losses['실현손익'].mean():,.0f}"
                    if len(wins) and len(losses) else "-")
        t[2].metric("홀딩 청산", f"{len(hold_s):,}회")
        t[3].metric("방어 매도", f"{len(def_s):,}건")
        with st.expander(f"📋 전체 체결 기록 ({len(trades):,}건)"):
            td = trades.copy()
            for col in ("날짜", "매수일"):
                td[col] = pd.to_datetime(td[col]).dt.strftime("%Y-%m-%d")
            st.dataframe(td, use_container_width=True, hide_index=True, height=400,
                         column_config={"가격": st.column_config.NumberColumn(format="%.4f"),
                                        "금액": st.column_config.NumberColumn(format="%.0f"),
                                        "실현손익": st.column_config.NumberColumn(format="%.0f"),
                                        "수익률(%)": st.column_config.NumberColumn(format="%.2f")})
            st.download_button("📥 체결기록 CSV", td.to_csv(index=False).encode("utf-8-sig"),
                               f"dstq_{ticker}_trades.csv", "text/csv", key="dq_pf_dl")

    # ── 회복력 ──
    st.divider()
    st.markdown("##### 🛡️ 회복력 분석 (10% 이상 하락 에피소드)")
    st.caption("고점 대비 10% 이상 하락한 구간별로, 어디까지 빠지고 얼마 만에 회복했는지 분석합니다.")
    rec = _episodes(list(dates), assets)
    if rec:
        def _clr(v):
            sv = str(v)
            return ("color:#C62828;font-weight:bold"
                    if (("%" in sv and sv.startswith("-")) or "미회복" in sv) else "")
        rdf = pd.DataFrame(rec)
        done = rdf[rdf["회복 시점"] != "미회복 ⏳"]
        q = st.columns(4)
        q[0].metric("하락 에피소드", f"{len(rdf)}회")
        q[1].metric("평균 회복 기간", f"{done['기간(일)'].mean():.0f}일" if len(done) else "-")
        q[2].metric("최장 회복 기간", f"{done['기간(일)'].max():.0f}일" if len(done) else "-")
        q[3].metric("가장 깊은 하락", f"{s['MDD']:.1f}%")
        st.dataframe(rdf.style.map(_clr), hide_index=True, use_container_width=True,
                     height=min(38 + 35 * len(rdf), 420))
        top = rdf.assign(_v=rdf["하락율"].str.rstrip("%").astype(float)).nsmallest(5, "_v").drop(columns="_v")
        st.markdown("**Top 5 최대 낙폭 구간**")
        st.dataframe(top.reset_index(drop=True).style.map(_clr), hide_index=True, use_container_width=True)
    else:
        st.info("10% 이상 하락 에피소드가 없습니다.")

    # ── 롤링 성과 ──
    st.divider()
    st.markdown("##### 🔄 롤링 성과 분석")
    st.caption("구간별 성과 추이. 특정 시기에만 좋은 게 아닌지 검증합니다.")
    for win_n, rt in zip([252, 504, 756], st.tabs(["1년 롤링", "2년 롤링", "3년 롤링"])):
        with rt:
            if len(assets) <= win_n:
                st.info(f"분석 기간이 {win_n // 252}년보다 짧습니다.")
                continue
            yrs = win_n / 252
            ser = pd.Series(assets)
            rc = ((ser / ser.shift(win_n)) ** (1 / yrs) - 1).to_numpy() * 100
            rm = np.full(len(assets), np.nan)
            for ri in range(win_n, len(assets)):
                seg = assets[ri - win_n: ri + 1]
                rm[ri] = float((seg / np.maximum.accumulate(seg) - 1).min()) * 100
            vm = ~np.isnan(rc)
            figr = make_subplots(specs=[[{"secondary_y": True}]])
            figr.add_trace(go.Scatter(x=dates[vm], y=rc[vm], name="롤링 CAGR(%)",
                                      line=dict(color="#1565C0", width=2)), secondary_y=False)
            figr.add_trace(go.Scatter(x=dates[vm], y=rm[vm], name="롤링 MDD(%)",
                                      line=dict(color="#EF5350", width=1.5, dash="dot")),
                           secondary_y=True)
            figr.add_hline(y=0, line_dash="dash", line_color="#aaa")
            figr.update_layout(height=320, legend=dict(orientation="h", y=1.1),
                               margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(figr, use_container_width=True, key=f"dq_pf_roll{win_n}")
            rr = st.columns(4)
            rr[0].metric("평균 CAGR", f"{np.nanmean(rc):+.1f}%")
            rr[1].metric("최고 CAGR", f"{np.nanmax(rc):+.1f}%")
            rr[2].metric("최저 CAGR", f"{np.nanmin(rc):+.1f}%")
            rr[3].metric("손실 구간 비율", f"{float((rc[vm] < 0).mean() * 100):.1f}%",
                         help=f"임의 시점에 시작해 {win_n // 252}년 뒤 손실이었던 비율")

    # ── 모드별 성과 ──
    if len(sells):
        st.divider()
        st.markdown("##### 🔄 홀딩 vs 방어 매매 성과")

        def _ms(d, nm):
            if not len(d):
                return {"구분": nm, "매도 횟수": 0, "승률": "-", "평균 수익률": "-",
                        "평균손익": "-", "총손익": "-", "평균 보유일": "-"}
            return {"구분": nm, "매도 횟수": len(d), "승률": f"{(d['실현손익'] > 0).mean() * 100:.1f}%",
                    "평균 수익률": f"{d['수익률(%)'].mean():+.2f}%",
                    "평균손익": f"${d['실현손익'].mean():+,.0f}",
                    "총손익": f"${d['실현손익'].sum():+,.0f}",
                    "평균 보유일": f"{d['보유일'].mean():.1f}일"}
        rows_ = [_ms(hold_s, "🟧 홀딩 (전체)")]
        for rs in (EXIT_BREAK, EXIT_TP):
            rows_.append(_ms(hold_s[hold_s["사유"] == rs], f"　└ {rs} 청산"))
        rows_.append(_ms(def_s, "🟩 방어 (전체)"))
        rows_.append(_ms(def_s[def_s["사유"] == "MA"], "　└ MA 일괄 매도"))
        rows_.append(_ms(def_s[def_s["사유"] == "MOC"], "　└ 만기 MOC"))
        rows_.append(_ms(sells, "📊 전체"))
        st.dataframe(pd.DataFrame(rows_), hide_index=True, use_container_width=True)
        st.caption("홀딩 청산 손익에는 홀딩으로 전환된 방어 물량의 손익도 포함됩니다. "
                   "홀딩은 '손실은 작게 자주, 수익은 크게 가끔', 방어는 '작은 수익을 자주' 내는 구조입니다.")

        if len(hold_s):
            st.markdown("##### 🟧 홀딩 사이클 내역")
            hc = hold_s[["매수일", "날짜", "보유일", "가격", "수익률(%)", "실현손익", "사유"]].copy()
            hc.columns = ["진입일", "청산일", "보유일(달력)", "청산가", "수익률(%)", "실현손익($)", "청산 사유"]
            hc["진입일"] = pd.to_datetime(hc["진입일"]).dt.date
            hc["청산일"] = pd.to_datetime(hc["청산일"]).dt.date
            hc.insert(0, "사이클", range(1, len(hc) + 1))

            def _pm(v):
                return f"color:{'#2E7D32' if v >= 0 else '#C62828'};font-weight:600"
            st.dataframe(hc.style.map(_pm, subset=["수익률(%)", "실현손익($)"])
                         .format({"청산가": "{:.2f}", "수익률(%)": "{:+.1f}", "실현손익($)": "{:+,.0f}",
                                  "보유일(달력)": "{:.0f}"}),
                         hide_index=True, use_container_width=True,
                         height=min(38 + 35 * len(hc), 420))

        st.markdown("##### 📊 매도 수익률 분포")
        dsel = st.radio("대상", ["전체", "홀딩", "방어"], horizontal=True, key="dq_pf_dist")
        dd_ = {"전체": sells, "홀딩": hold_s, "방어": def_s}[dsel]
        pr = dd_["수익률(%)"].to_numpy(dtype=float)
        if len(pr) >= 2:
            figh = go.Figure(go.Histogram(x=pr.tolist(), nbinsx=40, marker_color="#3498DB"))
            figh.add_vline(x=0, line_dash="dash", line_color="#333")
            figh.add_vline(x=float(np.mean(pr)), line_dash="dot", line_color="#1565C0",
                           annotation_text=f"평균 {np.mean(pr):+.2f}%")
            figh.update_layout(height=300, xaxis_title="수익률(%)", yaxis_title="빈도(회)",
                               margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(figh, use_container_width=True, key="dq_pf_hist")
            pq = st.columns(4)
            pq[0].metric("평균 수익률", f"{np.mean(pr):+.2f}%")
            pq[1].metric("중앙값", f"{np.median(pr):+.2f}%")
            pq[2].metric("왜도(Skew)", f"{float(pd.Series(pr).skew()):.2f}", help="양수=가끔 큰 수익")
            pq[3].metric("첨도(Kurt)", f"{float(pd.Series(pr).kurtosis()):.2f}",
                         help="높을수록 극단값 빈번")
        else:
            st.info("분포를 그리기에 매도 건수가 부족합니다.")

    # ── 현금 활용률 ──
    st.divider()
    st.markdown("##### 💵 현금 활용률 (투자 비율)")
    invr = np.clip((1 - log["현금"].to_numpy(dtype=float) / assets) * 100, 0, 100)
    is_def = (log["모드"] == MODE_DEF).to_numpy()
    cu = st.columns(4)
    cu[0].metric("평균 투자비율", f"{np.mean(invr):.1f}%")
    cu[1].metric("방어 모드 평균", f"{np.mean(invr[is_def]):.1f}%" if is_def.any() else "-")
    cu[2].metric("홀딩 모드 평균", f"{np.mean(invr[~is_def]):.1f}%" if (~is_def).any() else "-")
    cu[3].metric("평균 현금비율", f"{100 - np.mean(invr):.1f}%")
    figc = go.Figure(go.Scatter(x=dates, y=invr, fill="tozeroy",
                                line=dict(color="rgba(255,179,0,0.9)", width=1),
                                fillcolor="rgba(255,179,0,0.4)"))
    figc.update_layout(height=240, yaxis=dict(range=[0, 100], title="투자비율(%)"),
                       margin=dict(l=0, r=0, t=10, b=0), hovermode="x unified")
    st.plotly_chart(figc, use_container_width=True, key="dq_pf_cash")

    # ── 파라미터 민감도 ──
    base = _engine_params(P)
    start_s, end_s = str(P["bt_start_date"]), str(P["bt_end_date"])
    st.divider()
    st.markdown("##### 🎛️ 파라미터 민감도 분석")
    st.caption("현재 값 주변의 Calmar 분포. 현재값 주변이 고르게 녹색이면 과최적화 위험이 낮습니다.")
    with st.expander("🔍 민감도 히트맵 보기 (클릭하여 실행)"):
        smode = st.radio("분석 대상", ["🟧 홀딩 (매수 상한 × 매도 하한)", "🟩 방어 (매수1 × 매도)"],
                         horizontal=True, key="dq_sens_mode")
        if st.button("▶ 민감도 분석 실행 (25회)", key="dq_sens_run"):
            df = _load_ohlc(ticker, end_s)
            if smode.startswith("🟧"):
                ky, kx, ny, nx = "buy_hi", "exit_disp", "매수 이격도 상한(%)", "매도 이격도 하한(%)"
                yr_ = np.round(np.linspace(max(base["buy_lo"] + 1, base["buy_hi"] - 4), base["buy_hi"] + 4, 5), 1)
                xr_ = np.round(np.linspace(base["exit_disp"] - 2, min(base["buy_lo"] - 0.5, base["exit_disp"] + 2), 5), 2)
            else:
                ky, kx, ny, nx = "sf_buy1", "sf_sell", "방어 매수조건1(MA%)", "방어 매도조건(MA%)"
                yr_ = np.round(np.linspace(base["sf_buy1"] - 1, base["sf_buy1"] + 1, 5), 2)
                xr_ = np.round(np.linspace(max(0.1, base["sf_sell"] - 1), base["sf_sell"] + 1, 5), 2)
            heat, prog = np.zeros((5, 5)), st.progress(0.0)
            for yi, yv in enumerate(yr_):
                for xi, xv in enumerate(xr_):
                    lg = run_backtest(df, {**base, ky: float(yv), kx: float(xv)}, cap,
                                      start=start_s, end=end_s)["log"]
                    cs = compute_stats(lg)
                    heat[yi][xi] = cs.get("Calmar", 0) if np.isfinite(cs.get("Calmar", 0)) else 0
                    prog.progress((yi * 5 + xi + 1) / 25)
            prog.empty()
            st.session_state["dq_sens"] = (heat, yr_, xr_, ny, nx, base[ky], base[kx])
        sd = st.session_state.get("dq_sens")
        if sd:
            heat, yr_, xr_, ny, nx, cy, cx = sd
            fh = go.Figure(go.Heatmap(z=heat, x=[f"{v:g}" for v in xr_], y=[f"{v:g}" for v in yr_],
                                      colorscale="RdYlGn", text=np.round(heat, 2),
                                      texttemplate="%{text}", colorbar=dict(title="Calmar")))
            fh.update_layout(height=380, xaxis_title=nx, yaxis_title=ny,
                             xaxis_type="category", yaxis_type="category",
                             margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fh, use_container_width=True, key="dq_pf_sens")
            st.caption(f"현재값: {ny} = {cy:g} · {nx} = {cx:g} (가운데 칸)")

    # ── 무작위 기간 강건성 ──
    st.divider()
    st.markdown("##### 🎲 무작위 기간 강건성 분석")
    st.caption("분석 기간 안에서 1년(252거래일) 구간 100개를 무작위로 뽑아, 그때 시작했다면 어땠을지 "
               f"전략과 {ticker} 보유를 비교합니다.")
    with st.expander("🔍 강건성 분석 실행 (클릭)"):
        if st.button("▶ 무작위 100구간 분석 시작", key="dq_mc_run"):
            import random
            df = _load_ohlc(ticker, end_s)
            idx = df.index
            WIN = 252
            lo = int(np.searchsorted(idx, pd.Timestamp(dates.iloc[0])))
            valid = list(range(lo, len(idx) - WIN))
            if len(valid) < 100:
                st.warning("분석 기간이 짧아 100구간을 뽑을 수 없습니다 (약 1년 반 이상 필요).")
            else:
                random.seed(7)
                out, prog = [], st.progress(0.0)
                for ci, si in enumerate(random.sample(valid, 100)):
                    sdt, edt = idx[si], idx[si + WIN - 1]
                    lg = run_backtest(df, base, cap, start=str(sdt.date()), end=str(edt.date()))["log"]
                    if len(lg) > 50:
                        a = lg["총자산"].to_numpy(dtype=float)
                        b = lg["종가"].to_numpy(dtype=float)
                        out.append((str(sdt.date()), str(edt.date()), (a[-1] / cap - 1) * 100,
                                    abs((a / np.maximum.accumulate(a) - 1).min()) * 100,
                                    (b[-1] / b[0] - 1) * 100,
                                    abs((b / np.maximum.accumulate(b) - 1).min()) * 100))
                    prog.progress((ci + 1) / 100)
                prog.empty()
                st.session_state["dq_mc"] = pd.DataFrame(
                    out, columns=["시작일", "종료일", "전략 수익률(%)", "전략 MDD(%)",
                                  "B&H 수익률(%)", "B&H MDD(%)"])
        mc = st.session_state.get("dq_mc")
        if mc is not None and len(mc):
            sr, sm = mc["전략 수익률(%)"].to_numpy(), mc["전략 MDD(%)"].to_numpy()
            br, bm = mc["B&H 수익률(%)"].to_numpy(), mc["B&H MDD(%)"].to_numpy()

            def _row(a, lbl, pos=True):
                sg = "+" if pos else ""      # MDD 는 크기(양수)라 부호를 붙이지 않는다
                return {"구분": lbl, "평균": f"{np.mean(a):{sg}.1f}%", "중앙값": f"{np.median(a):{sg}.1f}%",
                        "표준편차": f"{np.std(a):.1f}%", "최솟값": f"{np.min(a):{sg}.1f}%",
                        "최댓값": f"{np.max(a):{sg}.1f}%",
                        "양(+) 비율": f"{(a > 0).mean() * 100:.0f}%" if pos else "-"}
            st.markdown(f"**📋 요약 통계 (n={len(mc)})**")
            st.dataframe(pd.DataFrame([
                _row(sr, "전략 (1년 수익률)"), _row(br, f"{ticker} B&H (1년 수익률)"),
                _row(sm, "전략 (MDD)", False), _row(bm, f"{ticker} B&H (MDD)", False)]),
                hide_index=True, use_container_width=True)
            figm = make_subplots(rows=1, cols=2, subplot_titles=["수익률 분포(1년)", "MDD 분포"],
                                 horizontal_spacing=0.12)
            for col_i, (b_, s_) in enumerate(((br, sr), (bm, sm)), 1):
                figm.add_trace(go.Histogram(x=b_.tolist(), nbinsx=20, name=f"{ticker} B&H", opacity=0.55,
                                            marker_color="#FB8C00", showlegend=(col_i == 1)), row=1, col=col_i)
                figm.add_trace(go.Histogram(x=s_.tolist(), nbinsx=20, name="전략", opacity=0.6,
                                            marker_color="#1565C0", showlegend=(col_i == 1)), row=1, col=col_i)
            figm.update_layout(height=380, barmode="overlay", margin=dict(l=0, r=0, t=40, b=0),
                               legend=dict(orientation="h", y=1.15))
            st.plotly_chart(figm, use_container_width=True, key="dq_pf_mc")
            st.caption("전략 수익률 분포가 오른쪽에, MDD 분포가 왼쪽(낮은 손실)에 모일수록 강건합니다.")
            det = mc.copy()
            det["전략-B&H(%p)"] = det["전략 수익률(%)"] - det["B&H 수익률(%)"]
            sort_opt = st.selectbox("정렬 기준", ["시작일", "전략 수익률(%)", "전략 MDD(%)", "전략-B&H(%p)"],
                                    key="dq_mc_sort")
            det = det.sort_values(sort_opt, ascending=(sort_opt == "시작일")).reset_index(drop=True).round(1)
            st.markdown(f"**📄 100구간 상세 결과** — 전략이 수익이었던 구간 "
                        f"{int((det['전략 수익률(%)'] > 0).sum())}/{len(det)} · "
                        f"B&H보다 나았던 구간 {int((det['전략-B&H(%p)'] > 0).sum())}/{len(det)}")

            def _hl(row):
                col_ = "#e8f5e9" if row["전략 수익률(%)"] > 0 else "#ffebee"
                return [f"background-color:{col_};color:#222"] * len(row)
            st.dataframe(det.style.apply(_hl, axis=1).format(precision=1), use_container_width=True,
                         hide_index=True, height=min(38 + 35 * len(det), 560))
            st.download_button("📥 100구간 결과 CSV", det.to_csv(index=False).encode("utf-8-sig"),
                               f"dstq_{ticker}_robustness.csv", "text/csv", key="dq_mc_dl")
    st.caption("⚠️ 과거 성과가 미래를 보장하지 않습니다. 3배 레버리지 ETF 는 장기 횡보·급락 시 손실이 큽니다.")


# ══════════════════════════════════════════════
# 탭5: 개인 설정
# ══════════════════════════════════════════════
def _write_orders_to_sheet(sh, sheet_name: str, rows: list):
    """계좌 탭 L4 에 주문, B11 에 갱신 시각(KST) 기록 — 다른 매매법과 같은 자리."""
    ws = sh.worksheet(sheet_name)
    ws.batch_clear(["L4:O13"])
    if rows:
        ws.update(range_name="L4", values=rows)
    now_kst = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
    ws.update(range_name="B11", values=[[now_kst]])
    return now_kst


def render_settings_tab():
    st.subheader("⚙️ 개인 설정 — 듀얼스나이퍼 TQ")
    cfg = _load_cfg()
    accounts = _cfg_accounts(cfg)
    if _IS_CLOUD:
        if st.session_state.get("logged_in"):
            st.success("☁️ **Streamlit Cloud 실행 중** — 설정이 Google Sheets에 영구 저장됩니다.")
        else:
            st.warning("☁️ **Streamlit Cloud 실행 중** — 로그인 후 설정이 영구 저장됩니다.")
    else:
        st.success(f"🖥️ **로컬 PC 실행 중** — 설정이 `{_CFG_PATH}` 에 저장됩니다.")

    # ── 텔레그램 ──
    with st.container(border=True):
        ct, ch = st.columns([3, 1])
        with ct:
            st.markdown("#### 💬 텔레그램 알림 설정")
            st.caption("저장하면 **매일 낮 12시(한국시간, 미국 휴장일 제외)** 에 계좌별 다음 거래일 주문표가 "
                       "자동 발송됩니다. 주문이 없는 날도 현황(모드·보유·모드가 바뀌는 종가)을 보냅니다.")
        with ch:
            with st.popover("❓ Chat ID & Bot Token 확인 방법", use_container_width=True):
                render_telegram_help_popover(
                    strategy_name="듀얼스나이퍼 TQ", example_bot_display="듀얼스나이퍼TQ 알림봇",
                    example_bot_username="dstq_alert_bot", example_bot_username2="my_tqqq_bot")
        c1, c2 = st.columns(2)
        chat = c1.text_input("텔레그램 Chat ID", value=str(cfg.get("tg_chat_id", "")),
                             placeholder="예: 1234567890", key="dq_tg_chat")
        tok = c2.text_input("Bot Token", value=str(cfg.get("tg_token", "")), type="password",
                            placeholder="예: 123456789:AAF...", key="dq_tg_tok")
        b1, b2, _ = st.columns([1, 1, 4])
        if b1.button("📨 주문표 테스트 발송", use_container_width=True, key="dq_tg_test"):
            if not (chat.strip() and tok.strip()):
                st.warning("Chat ID와 Bot Token을 먼저 입력하세요.")
            elif not accounts:
                st.warning("등록된 계좌가 없습니다. 오늘의 주문표 탭에서 계좌를 먼저 등록하세요.")
            else:
                ok = 0
                for name, acct in accounts.items():
                    try:
                        df = _load_ohlc(str(acct.get("ticker", "TQQQ")), str(date.today()))
                        nd = next_trading_date(pd.Timestamp(df.index[-1]).date() + timedelta(days=1))
                        od = compute_account_orders(df, acct, next_day=nd)
                        if od.get("error"):
                            raise RuntimeError(od["error"])
                        msg = build_order_text(str(acct.get("ticker", "TQQQ")), od, nd, name)
                        ok += bool(_send_telegram(tok.strip(), chat.strip(), msg).get("ok"))
                    except Exception as e:
                        st.error(f"❌ {name}: {e}")
                st.success(f"✅ {ok}/{len(accounts)}개 계좌 발송 완료")
        if b2.button("💾 저장하기", use_container_width=True, key="dq_tg_save", type="primary"):
            cfg["tg_chat_id"], cfg["tg_token"] = chat.strip(), tok.strip()
            _save_cfg(cfg)
            st.success("✅ 텔레그램 설정 저장 완료")

    st.write("")

    # ── 구글 스프레드시트 ──
    with st.container(border=True):
        st.markdown("#### 🗂️ 구글 스프레드시트 연동")
        st.caption("① 매매 기록 동기화 — 계좌별 `dstq_{계좌}_일별매매` · `dstq_{계좌}_매매기록` 탭 자동 생성  "
                   "② 주문 전송 — 계좌별 탭의 **L4** 에 다음 거래일 주문, **B11** 에 갱신 시각 기록 "
                   "(매일 12시 자동 발송 때도 함께 기록)")
        gs_url = st.text_input("스프레드시트 URL", value=str(cfg.get("gs_url", "")),
                               placeholder="https://docs.google.com/spreadsheets/d/...", key="dq_gs_url")
        st.caption("* 스프레드시트에 서비스 계정 이메일 "
                   "`connectspreadsheet@sodium-gateway-485307-f3.iam.gserviceaccount.com` 을 "
                   "**편집자**로 공유해주세요.")
        use_tung = st.checkbox(
            "🔄 시트 전송 시 퉁치기 주문으로 전송 (자전거래 거부 증권사용)",
            value=bool(cfg.get("use_tungchigi_gsheet", False)), key="dq_use_tung",
            help="매수/매도 상계(퉁치기) 적용 주문을 시트에 기록합니다. 순 체결 결과는 원 주문과 같습니다. "
                 "시가 주문(LOO/MOO)이 있는 날은 퉁치기 없이 원 주문 그대로 기록합니다.")
        sheet_map = {}
        if accounts:
            st.markdown("##### 🗂️ 계좌별 시트(탭) 이름 매핑")
            st.caption("각 계좌의 주문을 기록할 탭 이름입니다. 탭은 미리 만들어 두어야 합니다.")
            for ak, ad in accounts.items():
                sheet_map[ak] = st.text_input(f"**{ak}** 시트 이름", value=ad.get("gs_sheet", ak),
                                              key=f"dq_gssheet_{ak}")
        else:
            st.info("등록된 계좌가 없습니다. 오늘의 주문표 탭에서 계좌를 먼저 등록하세요.")

        g1, g2, g3 = st.columns(3)
        if g1.button("🔗 시트 연결 테스트", use_container_width=True, key="dq_gs_test"):
            if not gs_url:
                st.warning("URL을 먼저 입력하세요.")
            else:
                try:
                    from common.config import _get_gspread_client
                    sh = _get_gspread_client().open_by_url(gs_url)
                    st.success(f"✅ 연결 성공: **{sh.title}** · 탭: {[w.title for w in sh.worksheets()]}")
                except Exception as e:
                    st.error(f"❌ 연결 실패: {e}")
        if g2.button("📤 주문 시트 전송", use_container_width=True, key="dq_gs_send", type="primary"):
            if not gs_url:
                st.warning("URL을 먼저 입력하세요.")
            elif not accounts:
                st.warning("등록된 계좌가 없습니다.")
            else:
                try:
                    from common.config import _get_gspread_client
                    sh = _get_gspread_client().open_by_url(gs_url)
                    for ak, ad in accounts.items():
                        snm = sheet_map.get(ak, ak)
                        with st.spinner(f"{ak} → '{snm}' 전송 중..."):
                            try:
                                df = _load_ohlc(str(ad.get("ticker", "TQQQ")), str(date.today()))
                                nd = next_trading_date(
                                    pd.Timestamp(df.index[-1]).date() + timedelta(days=1))
                                od = compute_account_orders(df, ad, next_day=nd)
                                if od.get("error"):
                                    raise RuntimeError(od["error"])
                                rows = order_rows(od)
                                if use_tung and can_tungchigi(rows):
                                    rows = _tung_rows(rows)
                                now_kst = _write_orders_to_sheet(sh, snm, rows)
                                st.success(f"✅ {ak} → '{snm}' L4: 주문 {len(rows)}건 "
                                           f"(모드 {od['mode']}) · B11 {now_kst} 전송 완료")
                            except Exception as e2:
                                st.error(f"❌ {ak} → '{snm}' 실패: {e2}")
                except Exception as e:
                    st.error(f"❌ 전송 실패: {e}")
        if g3.button("💾 저장하기", use_container_width=True, key="dq_gs_save", type="primary"):
            cfg["gs_url"] = gs_url.strip()
            cfg["use_tungchigi_gsheet"] = bool(use_tung)
            for ak, snm in sheet_map.items():
                if ak in cfg.get("accounts", {}):
                    cfg["accounts"][ak]["gs_sheet"] = snm.strip() or ak
            _save_cfg(cfg)
            st.success("✅ URL + 계좌별 시트 이름 + 퉁치기 설정 저장 완료")

    st.write("")

    # ── 기본값 ──
    with st.container(border=True):
        st.markdown("#### 🧰 사이드바 파라미터 기본값")
        st.caption("사이드바 값은 백테스트·새 계좌 등록에 쓰입니다. 등록된 계좌의 파라미터는 "
                   "주문표 탭의 '파라미터 수정'에서 계좌별로 바꿉니다.")
        p = st.session_state.get("dq_last_params")
        c1, c2 = st.columns(2)
        if c1.button("💾 현재 사이드바 값을 기본값으로 저장", key="dq_save_params",
                     use_container_width=True) and p:
            cfg["params"] = {**_engine_params(p), "capital": p["capital"]}
            _save_cfg(cfg)
            st.success("✅ 기본값 저장 완료")
        if c2.button("↩️ 사이드바를 항공모함셋(원본)으로 되돌리기", key="dq_reset_btn",
                     use_container_width=True):
            cfg.pop("params", None)
            _save_cfg(cfg)
            st.session_state["_dq_reset"] = True
            st.rerun()
