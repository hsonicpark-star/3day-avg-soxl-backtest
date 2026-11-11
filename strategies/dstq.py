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

from common.analysis import monthly_perf_table
from common.config import _IS_CLOUD
from common.data import filter_incomplete_today, next_trading_date
from common.telegram import _send_telegram
from dstq_engine import (DEFAULTS, MODE_DEF, MODE_HOLD, build_order_text,
                         build_today_orders, compute_stats, run_backtest,
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
# 탭3: 오늘의 주문
# ══════════════════════════════════════════════
def render_ordersheet_tab(params: dict):
    ticker = params["bt_ticker"]
    cfg = _load_cfg()
    acct = cfg.get("account", {})
    st.subheader(f"📋 오늘의 주문 — {ticker}")

    with st.expander("① 내 계좌 설정 (운용 시작일 · 시작 자본)", expanded=not acct):
        a1, a2 = st.columns(2)
        try:
            _sd = date.fromisoformat(str(acct.get("start", "")))
        except ValueError:
            _sd = date.today()
        os_start = a1.date_input("운용 시작일", _sd, key="dq_os_start",
                                 help="이 날부터 규칙대로 매매했다고 보고 현재 보유·현금을 계산합니다. "
                                      "시작할 때 200일선 위로 멀리 떠 있으면 방어 모드로 시작합니다.")
        os_cap = a2.number_input("시작 자본 ($)", min_value=100.0, step=1000.0, format="%.0f",
                                 value=float(acct.get("capital", params["capital"])),
                                 key="dq_os_cap")
        if st.button("💾 계좌 · 현재 파라미터 저장", key="dq_os_save"):
            cfg["account"] = {"start": str(os_start), "capital": float(os_cap), "ticker": ticker}
            cfg["params"] = {**_engine_params(params), "capital": params["capital"]}
            _save_cfg(cfg)
            st.success("저장 완료 — 다음부터 이 계좌·파라미터로 주문을 계산합니다.")
            acct = cfg["account"]
    if not acct:
        st.info("운용 시작일과 시작 자본을 저장하면 오늘의 주문이 표시됩니다.")
        return

    df = _load_ohlc(ticker, str(date.today()))
    if df.empty:
        st.error(f"{ticker} 가격 데이터를 불러오지 못했습니다. 최신 종가 없이 주문을 만들면 "
                 "잘못된 주문이 나올 수 있어 표시하지 않습니다.")
        return
    od = build_today_orders(df, _engine_params(params), float(acct["capital"]),
                            start=str(acct["start"]))
    if od.get("error") or od["log"].empty:
        st.warning("운용 시작일 이후 확정 종가가 아직 없습니다. 첫 종가가 나오면 주문이 표시됩니다.")
        return
    base_day = pd.Timestamp(od["base_date"]).date()
    next_day = next_trading_date(base_day + timedelta(days=1))
    if (date.today() - base_day).days > 5:
        st.warning(f"⚠️ 마지막 확정 종가가 {base_day} 입니다. 데이터가 최신인지 확인하세요.")

    mode_col = _COL_MA if od["mode"] == MODE_HOLD else _COL_BUY
    st.markdown(f"**주문일 {next_day}** · 기준종가({base_day}) = **${od['close']:,.2f}**")
    st.markdown(_stat_cards([
        ("현재 모드", od["mode"], {"buy": "내일 시가 홀딩 진입", "sell": "내일 시가 전량 매도"}
         .get(od["signal"], "유지"), mode_col),
        ("이격도", f"{od['disp'] * 100:+.2f}%", f"200MA ${od['ma']:,.2f}", "#888"),
        ("총자산", f"${od['total']:,.0f}", f"손익률 {od['return_pct']:+.2f}%",
         _COL_BUY if od["return_pct"] >= 0 else _COL_SELL),
        ("현금", f"${od['cash']:,.0f}", f"현금 비중 {od['cash'] / od['total'] * 100:.0f}%", "#888"),
        ("보유 수량", f"{od['hold_qty'] + sum(l['수량'] for l in od['lots']):,}주",
         f"방어 {len(od['lots'])}티어" if od["lots"] else ("홀딩" if od["hold_qty"] else "없음"), "#888"),
    ], tone="blue"), unsafe_allow_html=True)

    st.markdown(f"**② {next_day} 주문표**")
    if od["orders"]:
        rows = [{"구분": ("🔴 " if o["구분"] == "매도" else "🟢 ") + o["방법"] + o["구분"],
                 "티어": o["티어"],
                 "주문가": "시장가" if o["가격"] is None else f"${o['가격']:,.2f}",
                 "수량": f"{o['수량']:,}주", "예상금액": f"${o['예상금액']:,.0f}",
                 "비고 (계산 근거)": o["비고"]} for o in od["orders"]]
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
    else:
        st.info("주문 없음 — " + ("홀딩 유지 (청산 신호가 날 때까지 그대로 보유)"
                              if od["mode"] == MODE_HOLD else "관망"))
    st.caption("💡 LOO = 개장 지정가(시가 체결) · MOO = 개장 시장가 · LOC = 종가 지정가 "
               "(매수는 주문가 이하, 매도는 주문가 이상 종가에 체결) · MOC = 종가 시장가. "
               "홀딩 진입·청산은 **시가**, 방어 매매는 **종가** 체결입니다.")

    t = od["trig"]
    st.markdown(f"**③ {next_day} 종가가 얼마면 신호가 바뀌나**")
    will_hold = od["signal"] == "buy" or (od["mode"] == MODE_HOLD and od["signal"] != "sell")
    if will_hold:
        trows = [{"신호": f"🔴 홀딩 청산 (이격도 < {params['exit_disp']:g}%)",
                  "종가 조건": f"${t['exit']:,.2f} 미만",
                  "현재가 대비": f"{(t['exit'] / od['close'] - 1) * 100:+.2f}%"},
                 {"신호": f"🟠 과열 청산 (이격도 > {params['tp_disp']:g}%)",
                  "종가 조건": f"${t['tp']:,.2f} 초과",
                  "현재가 대비": f"{(t['tp'] / od['close'] - 1) * 100:+.2f}%"}]
    else:
        trows = [{"신호": f"🟢 홀딩 진입 (이격도 {params['buy_lo']:g}%~{params['buy_hi']:g}%)",
                  "종가 조건": f"${t['buy_lo']:,.2f} 초과 ~ ${t['buy_hi']:,.2f} 미만",
                  "현재가 대비": f"{(t['buy_lo'] / od['close'] - 1) * 100:+.2f}% ~ "
                              f"{(t['buy_hi'] / od['close'] - 1) * 100:+.2f}%"}]
    st.dataframe(pd.DataFrame(trows), hide_index=True, use_container_width=True)
    if not will_hold and od["slope"] <= 0:
        st.caption("※ 현재 200일선 기울기가 0 이하라, 종가가 진입 구간에 들어와도 "
                   "기울기가 양수로 돌아서기 전에는 홀딩 진입하지 않습니다.")

    st.markdown("**④ 현재 보유**")
    if od["hold_qty"]:
        avg = od["hold_cost"] / od["hold_qty"]
        st.dataframe(pd.DataFrame([{
            "구분": "홀딩", "진입일": pd.Timestamp(od["hold_start"]).date(),
            "수량": f"{od['hold_qty']:,}주", "평단": f"${avg:,.2f}",
            "평가손익": f"{(od['close'] / avg - 1) * 100:+.1f}%"}]),
            hide_index=True, use_container_width=True)
    elif od["lots"]:
        st.dataframe(pd.DataFrame([{
            "티어": l["티어"], "매수일": pd.Timestamp(l["매수일"]).date(),
            "매수가": f"${l['매수가']:,.2f}", "수량": f"{l['수량']:,}주",
            "만기(MOC)까지": f"{l['남은일']}거래일",
            "평가손익": f"{(od['close'] / l['매수가'] - 1) * 100:+.1f}%"} for l in od["lots"]]),
            hide_index=True, use_container_width=True)
    else:
        st.caption("보유 없음 (전액 현금)")

    with st.expander("📜 최근 20거래일 (모델 기준)"):
        cols = ["날짜", "종가", "모드", "이격도", "매수가", "매도가", "매수티어", "매수량",
                "매도사유", "매도량", "보유수량", "현금", "총자산", "손익률(%)"]
        rec = od["log"].iloc[-20:][::-1][cols].copy()
        rec["날짜"] = pd.to_datetime(rec["날짜"]).dt.date
        rec["이격도"] = rec["이격도"] * 100
        st.dataframe(rec.rename(columns={"이격도": "이격도(%)", "매수가": "매수LOC", "매도가": "매도LOC"}),
                     hide_index=True, use_container_width=True,
                     column_config={c: st.column_config.NumberColumn(format="%.2f")
                                    for c in ("종가", "이격도(%)", "매수LOC", "매도LOC", "손익률(%)")}
                     | {c: st.column_config.NumberColumn(format="%.0f") for c in ("현금", "총자산")})
    st.caption("⚠️ 보유·현금은 **운용 시작일부터 규칙대로 매매했다고 가정한 모델** 기준입니다. "
               "실제 체결(시가·수량)이 다르면 조금씩 차이가 날 수 있습니다.")

    st.divider()
    tok, chat = str(cfg.get("tg_token", "")).strip(), str(cfg.get("tg_chat_id", "")).strip()
    if tok and chat:
        if st.button("📨 텔레그램으로 주문표 전송", key="dq_tg_send"):
            r = _send_telegram(tok, chat, build_order_text(ticker, od, next_day))
            st.success("전송 완료") if r.get("ok") else st.error(
                f"전송 실패: {r.get('description', '알 수 없는 오류')}")
    else:
        st.caption("📨 텔레그램 전송은 [⚙️ 개인 설정] 탭에서 Chat ID · Bot Token 을 저장하면 쓸 수 있습니다.")


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
    st.subheader("📊 성과 분석")
    src = st.radio("파라미터", ["🧭 사이드바 설정값", "🛳️ 항공모함셋 (원본)"], horizontal=True,
                   key="dq_intro_src")
    P = dict(params)
    if src.startswith("🛳️"):
        P.update({k: (list(v) if isinstance(v, tuple) else v) for k, v in DEFAULTS.items()
                  if k in _STRAT_KEYS})
    df = _load_ohlc(P["bt_ticker"], str(P["bt_end_date"]))
    if df.empty:
        st.error("가격 데이터를 불러오지 못했습니다.")
        return
    log, trades = _run(P, df)
    if log.empty:
        st.warning("백테스트 구간에 데이터가 없습니다.")
        return
    _summary_cards(log, trades)
    st.plotly_chart(_fig_equity(log, P["bt_ticker"], True), use_container_width=True,
                    key="dq_ch_intro_eq")

    st.markdown("**구간별 성과** — 어느 시기에 시작했어도 통했는지")
    rows = []
    last = pd.Timestamp(log["날짜"].iloc[-1])
    for s_ in ("2011-01-03", "2016-01-04", "2019-01-02", "2021-01-04", "2023-01-03"):
        if pd.Timestamp(s_) < pd.Timestamp(log["날짜"].iloc[0]) or pd.Timestamp(s_) >= last:
            continue
        lg, _t = _run(P, df, start=s_)
        if lg.empty:
            continue
        s = compute_stats(lg)
        bc, bm = _bh_stats(lg)
        rows.append({"시작": s_, "CAGR(%)": round(s["CAGR"], 1), "MDD(%)": round(s["MDD"], 1),
                     "Calmar": round(s["Calmar"], 2), "B&H CAGR(%)": round(bc, 1),
                     "B&H MDD(%)": round(bm, 1), "홀딩일(%)": round(s["홀딩일비중"], 0)})
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
    st.dataframe(yearly_returns(log), hide_index=True, use_container_width=True,
                 key="dq_tbl_intro_year")
    st.caption("⚠️ 과거 성과가 미래를 보장하지 않습니다. 3배 레버리지 ETF 는 장기 횡보·급락 시 손실이 큽니다.")


# ══════════════════════════════════════════════
# 탭5: 개인 설정
# ══════════════════════════════════════════════
def render_settings_tab():
    st.subheader("⚙️ 개인 설정 — 듀얼스나이퍼 TQ")
    cfg = _load_cfg()
    acct = cfg.get("account")
    if acct:
        st.markdown(f"**저장된 계좌** — {acct.get('ticker', 'TQQQ')} · 시작일 {acct.get('start')} · "
                    f"시작 자본 ${float(acct.get('capital', 0)):,.0f}")
    else:
        st.caption("저장된 계좌 없음 — [📋 오늘의 주문] 탭에서 운용 시작일·자본을 저장하세요.")

    st.markdown("#### 📨 텔레그램")
    t1, t2 = st.columns(2)
    chat = t1.text_input("Chat ID", value=str(cfg.get("tg_chat_id", "")), key="dq_tg_chat")
    tok = t2.text_input("Bot Token", value=str(cfg.get("tg_token", "")), type="password",
                        key="dq_tg_tok")
    b1, b2 = st.columns(2)
    if b1.button("💾 텔레그램 저장", key="dq_tg_save"):
        cfg["tg_chat_id"], cfg["tg_token"] = chat.strip(), tok.strip()
        _save_cfg(cfg)
        st.success("저장 완료")
    if b2.button("📨 테스트 메시지", key="dq_tg_test"):
        if chat.strip() and tok.strip():
            r = _send_telegram(tok.strip(), chat.strip(), "🛳️ 듀얼스나이퍼 TQ 텔레그램 연결 테스트")
            st.success("전송 완료") if r.get("ok") else st.error(
                f"전송 실패: {r.get('description', '알 수 없는 오류')}")
        else:
            st.warning("Chat ID 와 Bot Token 을 입력하세요.")
    st.caption("저장하면 **매일 낮 12시(한국시간, 미국 휴장일 제외)** 에 다음 거래일 주문표가 자동 발송됩니다. "
               "주문이 없는 날도 현황(모드·보유·신호가 바뀌는 종가)을 보냅니다. "
               "자동 발송은 [📋 오늘의 주문] 탭에서 **저장한 계좌·파라미터** 기준입니다 — "
               "사이드바 값을 바꿨다면 거기서 다시 저장하세요.")

    st.markdown("#### 🔄 초기화")
    p = st.session_state.get("dq_last_params")
    c1, c2 = st.columns(2)
    if c1.button("💾 현재 사이드바 파라미터를 기본값으로 저장", key="dq_save_params") and p:
        cfg["params"] = {**_engine_params(p), "capital": p["capital"]}
        _save_cfg(cfg)
        st.success("저장 완료")
    if c2.button("↩️ 항공모함셋(원본)으로 되돌리기", key="dq_reset_btn"):
        cfg.pop("params", None)
        _save_cfg(cfg)
        st.session_state["_dq_reset"] = True
        st.rerun()
