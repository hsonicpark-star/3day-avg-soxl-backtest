# -*- coding: utf-8 -*-
"""평단법 (평균단가법) — 전략 모듈

실시간 그리드 매수 + 평단 기준 일괄매도 전략.
- 사이클: 시작가 아래로 갭 간격 지정가 사다리 → 하락 시 티어 체결 →
  평단 × (1+이익률%) 도달 시 전량 매도 → 재시작
- 매도 방식 3종: ①목표가 지정 ②평단 도달 부분매도 ③종가 만족 전량매도
- 설정 저장: Cloud = users.pdan_config JSON 컬럼 / 로컬 = ~/.pdan/config.json
  (로컬 파일은 11.평단법 독립 앱과 공유 — 계좌·애드온 동일)
- 실시간 텔레그램 모니터·감시 파일은 로컬 전용 도구(11.평단법)로 제공
"""
from __future__ import annotations

import json
import os
from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

from common.config import _IS_CLOUD, _get_gspread_client
from common.data import filter_incomplete_today
from pdan_engine import (SELL_MODES, build_order_table, compute_stats,
                         ladder_prices, run_backtest, yearly_returns)

# ──────────────────────────────────────────────
# 설정 저장 (Cloud: users.pdan_config / 로컬: ~/.pdan/config.json)
# ──────────────────────────────────────────────
_PDAN_CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".pdan")
_PDAN_CONFIG_PATH = os.path.join(_PDAN_CONFIG_DIR, "config.json")


def _load_cfg() -> dict:
    if _IS_CLOUD and st.session_state.get("logged_in"):
        raw = st.session_state.get("user_settings", {}).get("pdan_config", "")
        if raw:
            try:
                cfg = json.loads(raw) if isinstance(raw, str) else raw
                return cfg if isinstance(cfg, dict) else {}
            except Exception:
                pass
        return {}
    try:
        with open(_PDAN_CONFIG_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_cfg(cfg: dict):
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            cfg_json = json.dumps(cfg, ensure_ascii=False)
            st.session_state.setdefault("user_settings", {})
            st.session_state.user_settings["pdan_config"] = cfg_json
            from common.auth import _save_user_settings_to_sheet
            _save_user_settings_to_sheet(st.session_state.username,
                                         {"pdan_config": cfg_json})
        except Exception as e:
            st.warning(f"⚠️ Cloud 저장 실패: {e}")
        return
    try:
        os.makedirs(_PDAN_CONFIG_DIR, exist_ok=True)
        with open(_PDAN_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.warning(f"⚠️ 로컬 저장 실패: {e}")


PARAM_DEFAULTS = {
    "ticker": "SOXL", "seed": 30000.0, "splits": 50,
    "tiered": True, "buy_gap": 1.0,
    "g1": 1.0, "t1": 20, "g2": 0.7, "t2": 40, "g3": 0.5,
    "sell_mode": "target",
    "thr_target": 5.0, "thr_partial": 1.0, "thr_close": 5.0,
    "compound": True, "reentry": True, "fee": 0.0,
    "gs_url": "",
    "weighted": False, "w1": 1.0, "w2": 1.5, "w3": 3.0,  # 하락가중
}
_INT_KEYS = {"splits", "t1", "t2"}


def _gap_of(P: dict):
    """계좌/파라미터 dict → (gap_param, gap_desc)."""
    splits = int(P["splits"])
    if P.get("tiered"):
        t1, t2 = int(P["t1"]), int(P["t2"])
        gp = [(t1, float(P["g1"]) / 100), (t2, float(P["g2"]) / 100),
              (splits, float(P["g3"]) / 100)]
        gd = (f"{P['g1']}%\\~{t1}T · {P['g2']}%\\~{t2}T · {P['g3']}%(그 이후)")
        return gp, gd
    return float(P["buy_gap"]) / 100, f"{float(P['buy_gap']):.1f}%"


def _weights_of(P: dict):
    """하락가중 설정 → qty_weights 파라미터 (미사용 시 None).

    구간 경계는 매수갭 3구간의 t1/t2를 그대로 사용."""
    if not P.get("weighted"):
        return None
    return [(int(P["t1"]), float(P["w1"])),
            (int(P["t2"]), float(P["w2"])),
            (int(P["splits"]), float(P["w3"]))]


def _thr_of(P: dict) -> float:
    m = P.get("sell_mode", "target")
    return float(P["thr_target"] if m == "target" else
                 P["thr_partial"] if m == "partial" else P["thr_close"])


# ──────────────────────────────────────────────
# 데이터 (OHLC — 평단법은 장중 레벨 체결이라 시고저종 필요)
# ──────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=1800)
def _load_ohlc(ticker: str, start_str: str, end_str: str) -> pd.DataFrame:
    start = pd.to_datetime(start_str).date()
    end = pd.to_datetime(end_str).date()
    try:
        raw = yf.download(ticker, start=start, end=end + timedelta(days=2),
                          progress=False, auto_adjust=True)
        if raw is None or raw.empty:
            return pd.DataFrame()
        if isinstance(raw.columns, pd.MultiIndex):
            try:
                raw = raw.xs(ticker, axis=1, level="Ticker")
            except Exception:
                raw.columns = raw.columns.droplevel(1)
        df = raw[["Open", "High", "Low", "Close"]].copy()
        df.index = pd.to_datetime(df.index)
        df = df.apply(pd.to_numeric, errors="coerce").dropna()
        return filter_incomplete_today(df)
    except Exception:
        return pd.DataFrame()


# ──────────────────────────────────────────────
# ASTRA 구글시트 주문 전송 (공통 gspread 클라이언트 사용)
# ──────────────────────────────────────────────
def _push_orders_to_astra(gs_url, order_df, ws_name="ASTRA", start_row=4,
                          sell=None) -> int:
    """L열~: [구분, 거래방법, 가격, 수량]. 기존 영역 비우고 매수 사다리
    (+선택 매도 1행) 기록. ASTRA는 시트 전체를 다시 주문하는 방식."""
    client = _get_gspread_client()
    ws = client.open_by_url(gs_url).worksheet(ws_name)
    rows = [["매수", "지정가", float(r["매수가"]), int(r["회차수량"])]
            for _, r in order_df.iterrows() if int(r["회차수량"]) > 0]
    if sell:
        rows.append(["매도", "지정가", round(float(sell[0]), 2), int(sell[1])])
    ws.batch_clear([f"L{start_row}:O{start_row + max(len(rows), 500)}"])
    if rows:
        ws.update(rows, f"L{start_row}", value_input_option="USER_ENTERED")
    return len(rows)


def _replace_sell_order(gs_url, price, qty, ws_name="ASTRA",
                        start_row=4) -> int:
    """주문 영역을 전부 비우고 매도 지정가 1행만 기록 (매수 중복 제출 방지)."""
    client = _get_gspread_client()
    ws = client.open_by_url(gs_url).worksheet(ws_name)
    last = max(len(ws.col_values(12)) + 5, start_row + 5)
    ws.batch_clear([f"L{start_row}:O{last}"])
    ws.update([["매도", "지정가", round(float(price), 2), int(qty)]],
              f"L{start_row}", value_input_option="USER_ENTERED")
    return start_row


def _eff_gs_url(P: dict, cfg: dict) -> str:
    """계좌 전용 gs_url > 평단법 공통 > (Cloud) 개인설정 gs_url."""
    url = str(P.get("gs_url", "")).strip() or str(cfg.get("gs_url", "")).strip()
    if not url and _IS_CLOUD:
        url = str(st.session_state.get("user_settings", {})
                  .get("gs_url", "")).strip()
    return url


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


# ══════════════════════════════════════════════
# 사이드바
# ══════════════════════════════════════════════
# 파라미터 프리셋 — 검증된 세팅 후보군 (설계 지표는 균등금액 기준 계산값)
_PDAN_PRESETS = [
    {"label": "🎯 표준 50분할 · 1/0.7/0.5 · ①5%",
     "help": "현재 운용형. 커버 -31.7% · 풀티어 평가손실 -14.7% · 필요반등 +23%",
     "splits": 50, "tiered": True, "g1": 1.0, "t1": 20, "g2": 0.7, "t2": 40,
     "g3": 0.5, "gap": 1.0, "mode": "target", "thr_t": 5.0},
    {"label": "🪜 90분할 연장 · 1/0.7/0.5 · ①5%",
     "help": "표준형의 0.5% 구간을 90티어까지 연장. 커버 -44.1% · 평가손실 -20.9% · 필요반등 +33%",
     "splits": 90, "tiered": True, "g1": 1.0, "t1": 20, "g2": 0.7, "t2": 40,
     "g3": 0.5, "gap": 1.0, "mode": "target", "thr_t": 5.0},
    {"label": "🌊 딥커버 90분할 · 1.6/0.8/0.4 · ①5%",
     "help": "커버 -50% + 평가손실 -20%를 동시에 맞춘 조합. 필요반등 +31%. 초반 갭이 넓어 얕은 조정 회전은 감소",
     "splits": 90, "tiered": True, "g1": 1.6, "t1": 20, "g2": 0.8, "t2": 45,
     "g3": 0.4, "gap": 1.0, "mode": "target", "thr_t": 5.0},
    {"label": "📏 균일 90분할 · 0.8% · ①10%",
     "help": "단일 갭으로 세팅 단순. 커버 -51% · 평가손실 -28.5%. 이익률 10%로 깊은 반등 수확형",
     "splits": 90, "tiered": False, "g1": 1.0, "t1": 20, "g2": 0.7, "t2": 40,
     "g3": 0.5, "gap": 0.8, "mode": "target", "thr_t": 10.0},
    {"label": "♻️ 회전형 50분할 · 1/0.7/0.5 · ②트리거1%",
     "help": "평단 도달 부분매도(②) — 싸게 산 티어만 익절하고 재하락 시 재매수. 백테스트(SOXL 5.5년)에서 물림 깊이·수익 균형 최상",
     "splits": 50, "tiered": True, "g1": 1.0, "t1": 20, "g2": 0.7, "t2": 40,
     "g3": 0.5, "gap": 1.0, "mode": "partial", "thr_p": 1.0},
    {"label": "🏋️ 하락가중 90분할 · 1/0.7/0.5 · 가중 1/1.5/3 · ①5%",
     "help": "C 구조 + 아래 구간일수록 자금 가중 (배분 10%/15%/75%). "
             "커버 -44.1% · 풀티어 평가손실 -16% · 필요반등 +25% — "
             "폭락 방어 최적. 대신 자금 대부분이 깊은 구간에 대기해 "
             "얕은 조정 수익은 작음",
     "splits": 90, "tiered": True, "g1": 1.0, "t1": 20, "g2": 0.7, "t2": 40,
     "g3": 0.5, "gap": 1.0, "mode": "target", "thr_t": 5.0,
     "weighted": True, "w1": 1.0, "w2": 1.5, "w3": 3.0},
    {"label": "🌱 입문 10분할 · 1% · ①5%",
     "help": "커버 -8.6% — 소액 연습·구조 이해용 (하락장 취약)",
     "splits": 10, "tiered": False, "g1": 1.0, "t1": 5, "g2": 0.7, "t2": 8,
     "g3": 0.5, "gap": 1.0, "mode": "target", "thr_t": 5.0},
]


def _preset_params(pk: dict) -> dict:
    """프리셋 정의 → 계좌 파라미터 dict 조각."""
    return {"splits": int(pk["splits"]), "tiered": bool(pk["tiered"]),
            "g1": float(pk["g1"]), "t1": int(pk["t1"]),
            "g2": float(pk["g2"]), "t2": int(pk["t2"]),
            "g3": float(pk["g3"]), "buy_gap": float(pk["gap"]),
            "sell_mode": pk["mode"],
            "thr_target": float(pk.get("thr_t", 5.0)),
            "thr_partial": float(pk.get("thr_p", 1.0)),
            "thr_close": float(pk.get("thr_c", 5.0)),
            "weighted": bool(pk.get("weighted", False)),
            "w1": float(pk.get("w1", 1.0)), "w2": float(pk.get("w2", 1.5)),
            "w3": float(pk.get("w3", 3.0))}


def _match_preset(P: dict):
    """계좌 파라미터가 프리셋과 일치하면 라벨 반환 (배지 표시용)."""
    for pk in _PDAN_PRESETS:
        pp = _preset_params(pk)
        keys = ["splits", "tiered", "thr_target", "thr_partial", "thr_close"]
        keys += (["g1", "t1", "g2", "t2", "g3"] if pp["tiered"]
                 else ["buy_gap"])
        if pp["weighted"]:
            keys += ["w1", "w2", "w3"]
        try:
            if (P.get("sell_mode") == pp["sell_mode"]
                    and bool(P.get("weighted", False)) == pp["weighted"]
                    and all(abs(float(P.get(k, -1)) - float(pp[k])) < 1e-9
                            for k in keys)):
                return pk["label"]
        except (TypeError, ValueError):
            continue
    return None


def render_sidebar() -> dict:
    # 위젯 session_state 1회 시드 — 위젯은 key만 쓰고 value= 미지정 →
    # 프리셋 선택이 session_state를 직접 덮어쓰면 즉시 반영됨 (경고 없음)
    for _k, _v in (("pd_seed", 30000.0), ("pd_splits", 50),
                   ("pd_tiered", True), ("pd_g1", 1.0), ("pd_t1", 20),
                   ("pd_g2", 0.7), ("pd_t2", 40), ("pd_g3", 0.5),
                   ("pd_gap", 1.0), ("pd_mode", "target"),
                   ("pd_thr_t", 5.0), ("pd_thr_p", 1.0), ("pd_thr_c", 5.0),
                   ("pd_comp", True), ("pd_re", True), ("pd_fee", 0.0),
                   ("pd_wt", False), ("pd_w1", 1.0), ("pd_w2", 1.5),
                   ("pd_w3", 3.0)):
        st.session_state.setdefault(_k, _v)

    st.subheader("📌 종목")
    _tk_presets = ["SOXL", "TQQQ", "USD", "직접입력"]
    _sel = st.selectbox("종목코드 (Ticker)", _tk_presets, index=0,
                        key="pd_tk_sel")
    if _sel == "직접입력":
        ticker = st.text_input("티커 직접 입력", key="pd_tk_custom",
                               placeholder="예: NVDA, SPY").strip().upper()
    else:
        ticker = _sel

    st.markdown("---")
    st.subheader("전략 파라미터")
    st.caption("💡 검증된 프리셋은 주문표 탭의 [계좌 추가]·[파라미터 수정]"
               "에서 선택할 수 있습니다.")
    seed = st.number_input("Seed / 초기 투자금 ($)", min_value=1000.0,
                           step=1000.0, format="%.0f", key="pd_seed")
    splits = st.number_input("분할수 (티어)", min_value=2, step=1,
                             key="pd_splits")
    # 구간 경계가 분할수/서로를 넘지 않도록 위젯 생성 전에 보정
    st.session_state["pd_t1"] = max(2, min(int(st.session_state["pd_t1"]),
                                           max(int(splits) - 1, 2)))
    st.session_state["pd_t2"] = max(int(st.session_state["pd_t1"]) + 1,
                                    min(int(st.session_state["pd_t2"]),
                                        int(splits)))
    tiered = st.checkbox("구간별 매수갭 (3구간)", key="pd_tiered")
    if tiered:
        c1, c2 = st.columns(2)
        g1 = c1.number_input("1구간 갭 (%)", min_value=0.1, step=0.1,
                             format="%.1f", key="pd_g1")
        t1 = c2.number_input("~티어", min_value=2, step=1,
                             max_value=max(int(splits) - 1, 2), key="pd_t1")
        c3, c4 = st.columns(2)
        g2 = c3.number_input("2구간 갭 (%)", min_value=0.1, step=0.1,
                             format="%.1f", key="pd_g2")
        t2 = c4.number_input("~티어 ", min_value=int(t1) + 1, step=1,
                             max_value=int(splits), key="pd_t2")
        g3 = st.number_input(f"3구간 갭 (%) — {int(t2) + 1}티어부터 끝까지",
                             min_value=0.1, step=0.1, format="%.1f",
                             key="pd_g3")
        gap_param = [(int(t1), g1 / 100), (int(t2), g2 / 100),
                     (int(splits), g3 / 100)]
        gap_desc = f"{g1}%\\~{int(t1)}T · {g2}%\\~{int(t2)}T · {g3}%(그 이후)"
        buy_gap = float(st.session_state["pd_gap"])
    else:
        buy_gap = st.number_input("매수갭 (%)", min_value=0.1, step=0.1,
                                  format="%.1f", key="pd_gap")
        gap_param, gap_desc = buy_gap / 100, f"{buy_gap:.1f}%"
        g1 = float(st.session_state["pd_g1"])
        t1 = int(st.session_state["pd_t1"])
        g2 = float(st.session_state["pd_g2"])
        t2 = int(st.session_state["pd_t2"])
        g3 = float(st.session_state["pd_g3"])

    weighted = st.checkbox("하락가중 (구간별 자금 가중)", key="pd_wt",
                           help="아래 구간일수록 티어당 자금을 크게 배분 — "
                                "폭락 시 평단이 빨리 따라 내려옴. "
                                "구간 경계는 위 3구간(~티어)을 따름")
    if weighted:
        wc1, wc2, wc3 = st.columns(3)
        w1 = wc1.number_input("1구간 가중", min_value=0.1, step=0.1,
                              format="%.1f", key="pd_w1")
        w2 = wc2.number_input("2구간 가중", min_value=0.1, step=0.1,
                              format="%.1f", key="pd_w2")
        w3 = wc3.number_input("3구간 가중", min_value=0.1, step=0.1,
                              format="%.1f", key="pd_w3")
    else:
        w1 = float(st.session_state["pd_w1"])
        w2 = float(st.session_state["pd_w2"])
        w3 = float(st.session_state["pd_w3"])

    st.markdown("---")
    sell_mode = st.radio("매도 방식", options=list(SELL_MODES),
                         format_func=lambda k: SELL_MODES[k], key="pd_mode")
    if sell_mode == "target":
        thr = st.number_input("평단이익률 (%)", min_value=0.5, step=0.5,
                              format="%.1f", key="pd_thr_t")
    elif sell_mode == "partial":
        thr = st.number_input("평단 부근 트리거 (%)", min_value=0.0,
                              step=0.5, format="%.1f", key="pd_thr_p")
    else:
        thr = st.number_input("만족 수익률 (%)", min_value=0.5, step=0.5,
                              format="%.1f", key="pd_thr_c")

    _pr = ladder_prices(100.0, int(splits), gap_param)
    st.caption(f"1회금액 = {seed / splits:,.2f}$ · "
               f"최대 커버 ≈ {(1 - _pr[-1] / _pr[0]) * 100:.1f}%")

    st.markdown("---")
    compound = st.checkbox("복리 (사이클마다 1회금액 재계산)", value=True,
                           key="pd_comp")
    reentry = st.checkbox("매도 당일 종가 재진입", value=True, key="pd_re")
    fee = st.number_input("수수료+슬리피지 (%, 편도)", min_value=0.0,
                          value=0.0, step=0.01, format="%.2f", key="pd_fee")

    st.markdown("---")
    st.subheader("백테스트 설정")
    b1, b2 = st.columns(2)
    bt_start = b1.date_input("시작 일", date(2021, 1, 4), key="pd_start")
    bt_end = b2.date_input("종료 일", date.today(), key="pd_end")

    return {
        "bt_ticker": ticker, "bt_start_date": bt_start, "bt_end_date": bt_end,
        "bt_initial_capital": float(seed),
        "seed": float(seed), "splits": int(splits),
        "tiered": bool(tiered), "buy_gap": float(buy_gap),
        "g1": float(g1), "t1": int(t1), "g2": float(g2), "t2": int(t2),
        "g3": float(g3),
        "gap_param": gap_param, "gap_desc": gap_desc,
        "sell_mode": sell_mode, "thr": float(thr),
        "compound": bool(compound), "reentry": bool(reentry),
        "fee": float(fee),
        "weighted": bool(weighted), "w1": float(w1), "w2": float(w2),
        "w3": float(w3),
        "qty_weights": ([(int(t1), float(w1)), (int(t2), float(w2)),
                         (int(splits), float(w3))] if weighted else None),
    }


# ══════════════════════════════════════════════
# 백테스트 탭
# ══════════════════════════════════════════════
def render_backtest_tab(params: dict):
    ticker = params["bt_ticker"]
    if not ticker:
        st.warning("티커를 입력해주세요.")
        return
    df = _load_ohlc(ticker, str(params["bt_start_date"]),
                    str(params["bt_end_date"]))
    if df.empty:
        st.error(f"{ticker} 데이터를 불러오지 못했습니다.")
        return
    st.caption(f"데이터: {df.index[0].date()} ~ {df.index[-1].date()} "
               f"({len(df)} 거래일, yfinance 수정주가 OHLC)")

    seed = params["seed"]
    result = run_backtest(df, seed=seed, splits=params["splits"],
                          buy_gap=params["gap_param"],
                          target_pct=params["thr"] / 100,
                          compound=params["compound"],
                          fee_rate=params["fee"] / 100,
                          reentry_same_day=params["reentry"],
                          sell_mode=params["sell_mode"],
                          qty_weights=params.get("qty_weights"))
    stats = compute_stats(result, df, seed)
    eq = result["equity"]

    m = st.columns(6)
    m[0].metric("총수익률", f"{stats['총수익률(%)']:.1f}%",
                delta=f"B&H {stats['B&H 수익률(%)']:.1f}%")
    m[1].metric("CAGR", f"{stats['CAGR(%)']:.1f}%")
    m[2].metric("MDD", f"{stats['MDD(%)']:.1f}%",
                delta=f"B&H {stats['B&H MDD(%)']:.1f}%", delta_color="off")
    m[3].metric("최종평가금", f"${stats['최종평가금']:,.0f}")
    m[4].metric("완료 사이클", stats["완료 사이클"],
                delta=(f"부분매도 {stats['부분매도 횟수']}회"
                       if stats.get("부분매도 횟수") else None),
                delta_color="off")
    m[5].metric("최대 사용티어", stats.get("최대 사용회차", 0),
                delta=f"평균 {stats.get('평균 최대회차', 0)}")

    if result["open_position"]:
        op = result["open_position"]
        st.warning(f"⚠️ 기간말 미청산 포지션: {op['시작일'].date()} 진입 · "
                   f"{op['회차']}티어 / {op['수량']}주 · 평단 {op['평단']} · "
                   f"목표가 {op['목표가']} · 현재가 {op['현재가']} · "
                   f"평가손익 {op['평가손익']:,.0f}$ "
                   f"({op['평가손익률(%)']}%)")

    bh = seed * df["Close"] / df["Close"].iloc[0]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=eq.index, y=eq["평가금"], name="평단법",
                             line=dict(color="#e45756", width=2)))
    fig.add_trace(go.Scatter(x=bh.index, y=bh, name="Buy&Hold",
                             line=dict(color="#888", width=1.2, dash="dot")))
    fig.update_layout(height=420, margin=dict(l=10, r=10, t=30, b=10),
                      yaxis_title="평가금 ($)", legend=dict(x=0.01, y=0.99))
    st.plotly_chart(fig, use_container_width=True)

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=eq.index, y=eq["회차"], name="사용 티어",
                              fill="tozeroy",
                              line=dict(color="#4c78a8", width=1)))
    fig2.update_layout(height=200, margin=dict(l=10, r=10, t=30, b=10),
                       yaxis_title="티어",
                       title_text="사용 티어 추이 (물림 깊이)")
    st.plotly_chart(fig2, use_container_width=True)

    c1, c2 = st.columns([1, 2])
    with c1:
        st.subheader("연도별 수익률")
        st.dataframe(yearly_returns(eq), hide_index=True,
                     use_container_width=True)
    with c2:
        st.subheader("사이클 내역")
        cyc = result["cycles"]
        if len(cyc):
            show = cyc[["시작일", "종료일", "기간(일)", "최대회차", "부분매도",
                        "진입가", "매도가", "투입금", "수익",
                        "수익률(%)"]].copy()
            show["시작일"] = show["시작일"].dt.date
            show["종료일"] = show["종료일"].dt.date
            st.dataframe(show, hide_index=True, use_container_width=True,
                         height=330)
        else:
            st.info("완료된 사이클이 없습니다.")

    with st.expander("전체 체결 내역"):
        fills = result["fills"]
        if len(fills):
            fills = fills.copy()
            fills["일자"] = fills["일자"].dt.date
            st.dataframe(fills, hide_index=True, use_container_width=True,
                         height=400)


# ══════════════════════════════════════════════
# 파라미터 최적화 탭
# ══════════════════════════════════════════════
def render_optimization_tab(params: dict):
    ticker = params["bt_ticker"]
    st.subheader("🔍 세팅 비교 & 그리드 탐색")
    st.caption("기간·Seed·복리 설정은 사이드바를 따릅니다. "
               "매도 방식은 현재 선택된 방식으로 통일해 비교합니다.")

    df = _load_ohlc(ticker, str(params["bt_start_date"]),
                    str(params["bt_end_date"]))
    if df.empty:
        st.error(f"{ticker} 데이터를 불러오지 못했습니다.")
        return
    seed = params["seed"]
    common = dict(compound=params["compound"], fee_rate=params["fee"] / 100,
                  reentry_same_day=params["reentry"],
                  sell_mode=params["sell_mode"])

    st.markdown("**① 대표 세팅 후보군 비교** — 가이드의 A~D 후보 + 현재 세팅")
    if st.button("후보군 백테스트 실행", key="pd_opt_presets"):
        presets = [
            ("현재 세팅", params["splits"], params["gap_param"],
             params["thr"], params.get("qty_weights")),
            ("A. 입문형 10분할·1%", 10, 0.01, 5.0, None),
            ("B. 균일 90분할·0.8%", 90, 0.008, params["thr"], None),
            ("C. 구간갭 90분할 1/0.7/0.5", 90,
             [(20, 0.01), (40, 0.007), (90, 0.005)], params["thr"], None),
            ("D. 구간갭 50분할 1/0.7/0.5", 50,
             [(20, 0.01), (40, 0.007), (50, 0.005)], params["thr"], None),
            ("E. 하락가중 90분할 1/0.7/0.5 ×1/1.5/3", 90,
             [(20, 0.01), (40, 0.007), (90, 0.005)], params["thr"],
             [(20, 1.0), (40, 1.5), (90, 3.0)]),
        ]
        rows = []
        prog = st.progress(0.0)
        for i, (name, sp, gp, thr, qw) in enumerate(presets):
            r = run_backtest(df, seed=seed, splits=sp, buy_gap=gp,
                             target_pct=thr / 100, qty_weights=qw, **common)
            s = compute_stats(r, df, seed)
            pr = ladder_prices(100.0, sp, gp)
            rows.append({
                "세팅": name, "분할": sp,
                "커버(%)": round((pr[-1] / pr[0] - 1) * 100, 1),
                "총수익률(%)": s["총수익률(%)"], "CAGR(%)": s["CAGR(%)"],
                "MDD(%)": s["MDD(%)"], "사이클": s["완료 사이클"],
                "최장물림(일)": s.get("최장 사이클(일)", 0),
            })
            prog.progress((i + 1) / len(presets))
        prog.empty()
        st.dataframe(pd.DataFrame(rows), hide_index=True,
                     use_container_width=True)

    st.markdown("---")
    st.markdown("**② 단일갭 × 이익률 그리드 탐색** — 현재 분할수·매도방식 기준")
    c1, c2 = st.columns(2)
    gaps = c1.multiselect("매수갭 후보 (%)", [0.5, 0.7, 1.0, 1.5, 2.0, 3.0],
                          default=[0.5, 1.0, 2.0, 3.0], key="pd_opt_gaps")
    thrs = c2.multiselect("이익률 후보 (%)", [3.0, 5.0, 7.0, 10.0, 15.0],
                          default=[3.0, 5.0, 10.0], key="pd_opt_thrs")
    if st.button("그리드 실행", key="pd_opt_grid") and gaps and thrs:
        rows = []
        total = len(gaps) * len(thrs)
        prog = st.progress(0.0)
        n = 0
        for g in gaps:
            for t in thrs:
                r = run_backtest(df, seed=seed, splits=params["splits"],
                                 buy_gap=g / 100, target_pct=t / 100,
                                 qty_weights=params.get("qty_weights"),
                                 **common)
                s = compute_stats(r, df, seed)
                rows.append({
                    "갭(%)": g, "이익률(%)": t,
                    "총수익률(%)": s["총수익률(%)"], "CAGR(%)": s["CAGR(%)"],
                    "MDD(%)": s["MDD(%)"], "사이클": s["완료 사이클"],
                    "최장물림(일)": s.get("최장 사이클(일)", 0),
                })
                n += 1
                prog.progress(n / total)
        prog.empty()
        out = pd.DataFrame(rows).sort_values("총수익률(%)", ascending=False)
        st.dataframe(out, hide_index=True, use_container_width=True)


# ══════════════════════════════════════════════
# 주문표 & 계좌관리 탭
# ══════════════════════════════════════════════
def _render_order_panel(P: dict, keyfx: str, cfg: dict,
                        acct_name: str | None = None):
    ticker = str(P["ticker"]).strip().upper()
    seed = float(P["seed"])
    addons = list(P.get("addons", []))
    addon_sum = float(sum(float(a.get("amount", 0)) for a in addons))
    # 운용 자금(본 시드) 입력 세션값 선반영 — 총 투입금 = 운용 자금 + 애드온
    try:
        seed_in = float(st.session_state.get(f"pd_seedin_{keyfx}", seed))
    except (TypeError, ValueError):
        seed_in = seed
    seed_eff = seed_in + addon_sum
    splits = int(P["splits"])
    gap_param, gap_desc = _gap_of(P)
    qw = _weights_of(P)
    sell_mode = P["sell_mode"]
    target_pct = _thr_of(P)

    _prx = ladder_prices(100.0, splits, gap_param)
    _seed_txt = (f"운용 ${seed_in:,.0f} + 애드온 ${addon_sum:,.0f} = "
                 f"총 ${seed_eff:,.0f}" if addon_sum else
                 f"운용 ${seed_in:,.0f}")
    _wt_txt = (f" · 하락가중 {P['w1']}/{P['w2']}/{P['w3']}" if qw else "")
    st.caption(f"**{ticker}** · {_seed_txt} · {splits}분할 · "
               f"매수갭 {gap_desc}{_wt_txt} · "
               f"{SELL_MODES[sell_mode]} {target_pct}% · "
               f"최대 커버 ≈{(1 - _prx[-1] / _prx[0]) * 100:.1f}%")

    eff_gs = _eff_gs_url(P, cfg)
    gs_src = ("계좌 전용 시트" if str(P.get("gs_url", "")).strip()
              else "공통 시트")

    # 최근 확정 종가
    df_last = _load_ohlc(ticker, str(date.today() - timedelta(days=14)),
                         str(date.today()))
    last_close = (float(df_last["Close"].iloc[-1])
                  if not df_last.empty else 100.0)

    saved_px = float(P.get("first_price", 0) or 0)
    default_px = round(saved_px if saved_px > 0 else last_close, 2)
    saved_filled = set(int(n) for n in P.get("filled", []))

    sim_area = st.container()  # 설계 시뮬레이션 전광판 자리

    c1, c2 = st.columns([1, 3])
    with c1:
        first_price = st.number_input(
            "최초매수가 ($)", min_value=0.01, value=default_px, step=0.01,
            format="%.2f", key=f"pd_px_{keyfx}",
            help="기본값 = 저장된 최초매수가 (없으면 최근 확정 종가)")
        st.caption(f"최근 확정 종가 {last_close:,.2f}")
        seed_in = st.number_input(
            "운용 자금 ($)", min_value=0.0,
            value=float(seed), step=500.0, format="%.0f",
            key=f"pd_seedin_{keyfx}",
            help="본 시드(내 자금)만 입력. 총 투입금 = 운용 자금 + 애드온 "
                 "합계로 사다리가 계산됩니다. [💾 상태 저장] 시 계좌 Seed로 "
                 "저장")
        seed_eff = float(seed_in) + addon_sum
        ot = build_order_table(seed_eff, splits, first_price, gap_param,
                               target_pct / 100, qty_weights=qw)
        st.metric("총 투입금", f"${ot['총금액'].iloc[-1]:,.2f}",
                  delta=(f"운용 ${float(seed_in):,.0f} + 애드온 "
                         f"${addon_sum:,.0f} · 1회 평균 ${seed_eff / splits:,.0f}"),
                  delta_color="off")
        st.metric("최종티어 매수가", f"${ot['매수가'].iloc[-1]:,.2f}",
                  delta=f"{(ot['매수가'].iloc[-1] / first_price - 1) * 100:.1f}%")
        st.download_button("⬇️ CSV 다운로드",
                           ot.to_csv(index=False).encode("utf-8-sig"),
                           file_name=f"평단법_주문표_{ticker}.csv",
                           mime="text/csv", key=f"pd_dl_{keyfx}")

        # 애드온 자금 (계좌 전용)
        if acct_name:
            st.divider()
            st.markdown("**💰 끌어온 자금** (타 전략 예수금 애드온)")
            _srcs = ["DSS", "표준편차", "종가평균", "Sigma", "IUO",
                     "듀얼스나이퍼"]
            _dirty = False
            for _i, a in enumerate(addons):
                if "id" not in a:
                    a["id"] = _i + 1
                    _dirty = True
            for a in list(addons):
                opts = _srcs + ([a["source"]] if a["source"] not in _srcs
                                else [])
                r1, r2, r3 = st.columns([2, 2, 0.6])
                new_src = r1.selectbox("출처", opts,
                                       index=opts.index(a["source"]),
                                       key=f"pd_aes_{keyfx}_{a['id']}")
                new_amt = r2.number_input(
                    "금액 ($)", min_value=0.0, step=1000.0, format="%.0f",
                    value=float(a["amount"]),
                    key=f"pd_aea_{keyfx}_{a['id']}",
                    help=f"끌어온 날짜: {a.get('date', '?')} · 0 = 전액 반납")
                r3.markdown("<div style='height:1.75em'></div>",
                            unsafe_allow_html=True)
                if r3.button("✕", key=f"pd_aed_{keyfx}_{a['id']}",
                             help="항목 완전 삭제"):
                    addons.remove(a)
                    _dirty = True
                    continue
                if (new_src != a["source"]
                        or float(new_amt) != float(a["amount"])):
                    a["source"], a["amount"] = new_src, float(new_amt)
                    _dirty = True
            if _dirty:
                cfg["accounts"][acct_name]["addons"] = addons
                _save_cfg(cfg)
                st.rerun()
            if addons:
                _sums = {}
                for a in addons:
                    _sums[a["source"]] = (_sums.get(a["source"], 0.0)
                                          + float(a["amount"]))
                st.metric("애드온 합계", f"${addon_sum:,.0f}",
                          delta=f"운용 자금 ${seed_eff:,.0f}",
                          delta_color="off")
                st.caption("반납 기준 출처별: "
                           + " · ".join(f"**{s}** ${v:,.0f}"
                                        for s, v in _sums.items()))
            with st.expander("➕ 새 항목 추가"):
                asrc_sel = st.selectbox("출처", _srcs + ["기타 (직접 입력)"],
                                        key=f"pd_asrc_{keyfx}")
                if asrc_sel == "기타 (직접 입력)":
                    asrc = st.text_input("출처 직접 입력",
                                         key=f"pd_asrctxt_{keyfx}")
                else:
                    asrc = asrc_sel
                aamt = st.number_input("금액 ($)", min_value=0.0,
                                       step=1000.0, format="%.0f",
                                       key=f"pd_aamt_{keyfx}")
                if st.button("➕ 자금 추가", key=f"pd_aadd_{keyfx}",
                             use_container_width=True, disabled=aamt <= 0):
                    next_id = max([a.get("id", 0) for a in addons],
                                  default=0) + 1
                    addons.append({"id": next_id,
                                   "date": date.today().isoformat(),
                                   "source": (asrc or "기타").strip(),
                                   "amount": float(aamt)})
                    cfg["accounts"][acct_name]["addons"] = addons
                    _save_cfg(cfg)
                    st.rerun()

    with c2:
        ot_edit = ot.copy()
        ot_edit.insert(0, "매수✓", ot_edit["회차"].isin(saved_filled))
        edited = st.data_editor(
            ot_edit, hide_index=True, use_container_width=True, height=560,
            disabled=[c for c in ot_edit.columns if c != "매수✓"],
            key=f"pd_ed_{keyfx}_{ticker}_{first_price}_{splits}_{gap_desc}_{seed_eff}",
            column_config={"매수✓": st.column_config.CheckboxColumn(
                "매수✓", help="체결된 티어를 수동으로 체크")})
        checked = edited[edited["매수✓"]]

    # 설계 시뮬레이션 전광판
    with sim_area:
        st.markdown("**📐 설계 시뮬레이션** — 이 파라미터로 마지막 티어까지 "
                    "매수됐다고 가정하면")
        _last = ot.iloc[-1]
        _bot, _avgf, _tgtf = (float(_last["매수가"]), float(_last["평단"]),
                              float(_last["매도희망"]))
        st.markdown(_stat_cards([
            ("최대 커버 (풀티어 도달가)", f"{_bot / first_price - 1:+.1%}",
             f"{splits}티어 ${_bot:,.2f}", "#888"),
            ("풀티어 평단가", f"${_avgf:,.2f}",
             f"시작가 대비 {_avgf / first_price - 1:+.1%}", "#888"),
            ("풀티어 평가손실", f"{_bot / _avgf - 1:+.1%}",
             "도달가 vs 평단 (물린 정도)", "#dc2626"),
            ("풀티어 매도목표", f"${_tgtf:,.2f}",
             f"시작가 대비 {_tgtf / first_price - 1:+.1%}", "#888"),
            ("바닥에서 필요 반등", f"{_tgtf / _bot - 1:+.1%}",
             "도달가 → 목표가", "#d97706"),
            ("총 투입 / 수량", f"${float(_last['총금액']):,.0f}",
             f"{int(_last['총수량'])}주 · 1회 평균 ${seed_eff / splits:,.0f}",
             "#888"),
        ], tone="blue"), unsafe_allow_html=True)
        with st.expander("단계별 상세 — 티어별 도달가·평단·평가손실·필요반등"):
            _step = max(splits // 9, 1)
            _ns = sorted(set([1] + list(range(_step, splits, _step))
                             + [splits]))
            _rows = []
            for _n in _ns:
                _r = ot[ot["회차"] == _n].iloc[0]
                _px, _av, _tg = (float(_r["매수가"]), float(_r["평단"]),
                                 float(_r["매도희망"]))
                _rows.append({
                    "티어": _n, "도달가": f"{_px:,.2f}",
                    "하락": f"{_px / first_price - 1:+.1%}",
                    "평단가": f"{_av:,.2f}",
                    "평가손실(평단比)": f"{_px / _av - 1:+.1%}",
                    "목표가": f"{_tg:,.2f}",
                    "필요반등": f"{_tg / _px - 1:+.1%}",
                    "누적투입": f"${float(_r['총금액']):,.0f}",
                })
            st.dataframe(pd.DataFrame(_rows), hide_index=True,
                         use_container_width=True)

    # 저장 안 된 변경사항 경고 + 현황판 — 주문테이블 아래(오른쪽 컬럼)에
    # 배치해 매매 리스트와 현황판을 한 화면에서 함께 보도록
    ui_nos = (set(checked["회차"].astype(int)) if len(checked) else set())
    with c2:
        if acct_name and (abs(first_price - (saved_px or first_price)) > 0.004
                          or ui_nos != saved_filled
                          or abs(float(seed_in) - seed) > 0.5):
            st.warning("⚠️ 최초매수가/운용 자금/매수✓ 변경사항이 저장되지 "
                       "않았습니다 — 새로고침하면 사라집니다 → 아래 "
                       "**[💾 상태 저장]**을 누르세요.")

        st.markdown("**📟 현황판** (매수✓ 기준)")
        sell_qty, tgt = 0, 0.0
        if checked.empty or int(checked["회차수량"].sum()) <= 0:
            st.caption("체크된 티어가 없습니다. 위 표에서 체결된 티어를 "
                       "체크하세요.")
        else:
            q = int(checked["회차수량"].sum())
            amt = float((checked["매수가"] * checked["회차수량"]).sum())
            avg = amt / q
            tgt = avg * (1 + target_pct / 100)
            total_amt = float(ot["총금액"].iloc[-1])
            if sell_mode == "partial":
                sell_qty = int(checked.loc[checked["매수가"] <= tgt,
                                           "회차수량"].sum())
                qty_note = f"트리거 이하 매수분 (전체 {q}주)"
            else:
                sell_qty = q
                qty_note = "전량"
            below = ot[ot["매수가"] <= avg]
            be_txt = (f"{int(below['회차'].iloc[0])}티어 "
                      f"(${below['매수가'].iloc[0]:,.2f})") if len(below) \
                else "-"
            unfilled_rows = ot[~ot["회차"].isin(ui_nos)]
            nxt = unfilled_rows.iloc[0] if len(unfilled_rows) else None
            pnl = (last_close - avg) * q
            pnl_pct = (last_close / avg - 1) * 100
            pnl_color = "#16a34a" if pnl >= 0 else "#dc2626"
            st.markdown(_stat_cards([
                ("매수 체결", f"{len(checked)}티어 · {q}주",
                 f"진행률 {len(checked)}/{splits}", "#888"),
                ("투입금", f"${amt:,.0f}",
                 (f"총 예정 대비 {amt / total_amt * 100:.0f}%"
                  if total_amt > 0 else ""), "#888"),
                ("손익 기준 평단가 (본전)", f"${avg:,.2f}",
                 f"손익 기준 {be_txt}", "#888"),
                ("매도 목표가", f"${tgt:,.2f}",
                 f"평단 +{target_pct:.1f}%", "#888"),
                ("매도할 수량", f"{sell_qty}주", qty_note, "#888"),
            ]), unsafe_allow_html=True)
            st.markdown(_stat_cards([
                ("최근 종가", f"${last_close:,.2f}",
                 f"평단 대비 {pnl_pct:+.2f}%", pnl_color),
                ("평가손익 (종가 기준)", f"${pnl:,.0f}",
                 f"{pnl_pct:+.2f}%", pnl_color),
                ("목표까지", f"{(tgt / last_close - 1) * 100:+.2f}%",
                 "종가 → 매도 목표가", "#888"),
                ("다음 매수 레벨",
                 f"${float(nxt['매수가']):,.2f}" if nxt is not None
                 else "없음",
                 (f"{int(nxt['회차'])}티어 · 종가 대비 "
                  f"{(float(nxt['매수가']) / last_close - 1) * 100:+.2f}%")
                 if nxt is not None else "사다리 소진", "#888"),
                ("목표 매도 시 실현이익", f"${(tgt - avg) * sell_qty:,.0f}",
                 f"매도 {sell_qty}주 기준", "#16a34a"),
            ]), unsafe_allow_html=True)

        bt1, bt2 = st.columns(2)
        with bt1:
            if (not checked.empty and eff_gs and sell_mode != "close"
                    and sell_qty > 0):
                if st.button(f"📤 매도 주문 시트 반영 — 지정가 "
                             f"{tgt:,.2f} × {sell_qty}주",
                             use_container_width=True,
                             key=f"pd_sell_{keyfx}"):
                    try:
                        row = _replace_sell_order(
                            eff_gs, tgt, sell_qty,
                            ws_name=cfg.get("astra_ws", "ASTRA"))
                        st.success(f"매도 1행만 기록 완료 — L{row} "
                                   f"({gs_src}). 매수 행은 제거했습니다 "
                                   "(중복 매수 방지). 자동주문 프로그램을 "
                                   "실행하면 매도만 제출됩니다.")
                    except Exception as e:
                        st.error(f"기록 실패: {e}")
        with bt2:
            if acct_name:
                if st.button("💾 상태 저장 (운용 자금 + 최초매수가 + 매수✓)",
                             use_container_width=True,
                             key=f"pd_wsave_{keyfx}"):
                    cfg["accounts"][acct_name]["first_price"] = \
                        float(first_price)
                    cfg["accounts"][acct_name]["filled"] = sorted(ui_nos)
                    cfg["accounts"][acct_name]["seed"] = float(seed_in)
                    _save_cfg(cfg)
                    st.success(f"저장 완료 — 운용 ${float(seed_in):,.0f} + "
                               f"애드온 ${addon_sum:,.0f} = "
                               f"총 ${seed_eff:,.0f} · "
                               f"체결 {len(ui_nos)}티어. 새로고침해도 "
                               "유지됩니다.")

    # ASTRA 전송
    st.divider()
    b1, b2 = st.columns(2)
    with b1:
        st.markdown("**🤖 ASTRA 주문 전송** — L4열부터 "
                    "[구분·거래방법·가격·수량], 지정가 매수")
        if not eff_gs:
            st.info("계좌 파라미터 또는 개인 설정에서 구글시트 URL을 먼저 "
                    "저장하세요.")
        else:
            unfilled = (ot[~ot["회차"].isin(ui_nos)] if ui_nos else ot)
            scope = st.radio(
                "전송 범위",
                ["미체결 전체", "티어 범위 지정", "전체 (1티어부터)"],
                horizontal=True, key=f"pd_scope_{keyfx}",
                help="매수✓ 체크된 티어는 제외 — 세션 리셋 후 재전송해도 "
                     "중복 매수하지 않습니다")
            if scope == "티어 범위 지정":
                _first_open = (int(unfilled["회차"].iloc[0])
                               if len(unfilled) else 1)
                rc1, rc2 = st.columns(2)
                r_from = rc1.number_input("시작 티어", min_value=1,
                                          max_value=splits,
                                          value=_first_open, step=1,
                                          key=f"pd_rf_{keyfx}")
                r_to = rc2.number_input("끝 티어", min_value=int(r_from),
                                        max_value=splits,
                                        value=min(int(r_from) + 9, splits),
                                        step=1, key=f"pd_rt_{keyfx}")
                send_df = unfilled[(unfilled["회차"] >= int(r_from))
                                   & (unfilled["회차"] <= int(r_to))]
            elif scope == "미체결 전체":
                send_df = unfilled
            else:
                send_df = ot
            include_sell = False
            if sell_qty > 0 and sell_mode != "close":
                include_sell = st.checkbox(
                    f"매도 주문 포함 — 지정가 {tgt:,.2f} × {sell_qty}주",
                    value=True, key=f"pd_incsell_{keyfx}",
                    help="세션 리셋으로 매수·매도가 모두 사라졌을 때 한 번에 "
                         "복원")
            n_orders = int((send_df["회차수량"] > 0).sum())
            if send_df.empty:
                st.info("전송할 미체결 티어가 없습니다.")
            else:
                st.caption(f"전송될 주문: 지정가 매수 {n_orders}건 · "
                           f"{int(send_df['회차'].min())}\\~"
                           f"{int(send_df['회차'].max())}티어"
                           + (" + 매도 1건" if include_sell else "")
                           + f" · {gs_src} · 기존 L4\\~O 영역은 지워집니다")
                if st.button("ASTRA 시트로 주문 전송", type="primary",
                             use_container_width=True,
                             key=f"pd_astra_{keyfx}"):
                    try:
                        with st.spinner("전송 중..."):
                            n = _push_orders_to_astra(
                                eff_gs, send_df,
                                ws_name=cfg.get("astra_ws", "ASTRA"),
                                sell=((tgt, sell_qty) if include_sell
                                      else None))
                        st.success(f"전송 완료 → L4:O{3 + n} · "
                                   f"매수 {n_orders}건"
                                   + (" + 매도 1건" if include_sell else ""))
                        st.warning("⚠️ 자동주문 프로그램의 실시간 감시가 "
                                   "켜져 있으면 주문이 실행될 수 있습니다. "
                                   "시트 확인 후 감시를 켜세요.")
                    except Exception as e:
                        st.error(f"전송 실패: {e}")
    with b2:
        st.markdown("**📡 실시간 모니터**")
        if _IS_CLOUD:
            st.caption("가격 감시·텔레그램 알림·매도 명령·자동 매도행 갱신은 "
                       "로컬 PC에서 도는 별도 도구(11.평단법 폴더의 "
                       "monitor.py)로 제공됩니다. 웹에서 누른 [상태 저장]은 "
                       "클라우드 계정에 저장되며, 모니터를 쓰려면 로컬 02 "
                       "앱에서 저장하세요.")
        else:
            _mon_dir = os.path.normpath(os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "..", "11.평단법"))
            _mon_ok = os.path.exists(os.path.join(_mon_dir, "monitor.py"))
            _ready = float(P.get("first_price", 0) or 0) > 0
            st.caption("모니터는 이 앱의 **[💾 상태 저장]** 내용"
                       "(~/.pdan/config.json)을 읽어 감시합니다. "
                       + ("✅ 이 계좌는 저장돼 있어 바로 감시 가능"
                          if _ready else
                          "⚠️ 아직 [💾 상태 저장]을 안 눌러 이 계좌는 "
                          "감시 대상에서 제외됩니다"))
            if _mon_ok:
                if st.button("▶ 모니터 실행 (전체 계좌, 새 콘솔 창)",
                             use_container_width=True,
                             key=f"pd_mon_{keyfx}"):
                    import subprocess
                    import sys as _sys
                    subprocess.Popen(
                        [_sys.executable, "monitor.py", "--interval", "30"],
                        cwd=_mon_dir,
                        creationflags=subprocess.CREATE_NEW_CONSOLE)
                    st.success("모니터를 새 콘솔 창에서 실행했습니다. "
                               "텔레그램 🟢 시작 메시지를 확인하세요.")
                st.caption("⚠️ 모니터 창이 이미 떠 있으면 그 창을 먼저 닫고 "
                           "실행하세요 (중복 실행 시 텔레그램 수신 충돌). "
                           "감시(한국시간): 주간거래 09:00\\~16:50 → "
                           "프리장 17:00 → 정규장 22:30 → 애프터장 "
                           "\\~익일 09:00. 텔레그램 명령: [계좌] 매도 1 · "
                           "매도 2 [가격] · 매도 3 가격 · 상태 · 동기화")
            else:
                st.caption(f"monitor.py를 찾을 수 없습니다: `{_mon_dir}`")


def render_ordersheet_tab(params: dict):
    st.subheader("📋 오늘의 주문표")
    cfg = _load_cfg()
    cfg.setdefault("accounts", {})
    accounts = cfg["accounts"]

    with st.expander("➕ 계좌 추가"):
        nc1, nc2 = st.columns([2, 2])
        new_name = nc1.text_input("계좌 이름", key="pd_new_acct",
                                  placeholder="예: PJH, 본계좌")
        _add_opts = (["현재 사이드바 파라미터"]
                     + [p["label"] for p in _PDAN_PRESETS])
        _add_sel = nc2.selectbox(
            "파라미터 프리셋", _add_opts, key="pd_add_preset",
            help="\n\n".join(f"• {p['label']}:\n{p['help']}"
                             for p in _PDAN_PRESETS))
        if st.button("계좌 등록", key="pd_add_acct",
                     use_container_width=True,
                     disabled=not new_name.strip()):
            _nm = new_name.strip()
            if _nm in accounts:
                st.error(f"'{_nm}' 계좌가 이미 있습니다.")
            else:
                snap = {k: params.get(k, v) for k, v in
                        PARAM_DEFAULTS.items()}
                snap["ticker"] = params.get("bt_ticker") or "SOXL"
                snap["thr_" + ("target" if params["sell_mode"] == "target"
                               else "partial" if params["sell_mode"]
                               == "partial" else "close")] = params["thr"]
                if _add_sel != "현재 사이드바 파라미터":
                    _pk = next(p for p in _PDAN_PRESETS
                               if p["label"] == _add_sel)
                    snap.update(_preset_params(_pk))
                accounts[_nm] = snap
                _save_cfg(cfg)
                st.rerun()
        st.caption("프리셋(또는 사이드바 파라미터)을 초기값으로 계좌가 "
                   "생성됩니다. 생성 후에는 계좌 탭 안의 "
                   "[✏️ 파라미터 수정]에서 관리하세요.")

    names = sorted(accounts)
    if names:
        for nm, atab in zip(names, st.tabs([f"📈 {n}" for n in names])):
            with atab:
                P = {**PARAM_DEFAULTS, **accounts[nm]}
                _render_account_editor(nm, P, cfg)
                _render_order_panel(P, keyfx=nm, cfg=cfg, acct_name=nm)
    else:
        st.info("등록된 계좌가 없습니다. [➕ 계좌 추가]로 만들어주세요 — "
                "아래는 사이드바 파라미터 기준 미리보기입니다.")
        P = {k: params.get(k, v) for k, v in PARAM_DEFAULTS.items()}
        P["ticker"] = params.get("bt_ticker") or "SOXL"
        P["thr_" + ("target" if params["sell_mode"] == "target" else
                    "partial" if params["sell_mode"] == "partial"
                    else "close")] = params["thr"]
        _render_order_panel(P, keyfx="side", cfg=cfg)


def _render_account_editor(name: str, P: dict, cfg: dict):
    _badge = _match_preset(P)
    with st.expander("✏️ 파라미터 수정 / 계좌 삭제"):
        if _badge:
            st.markdown(
                f'<div style="margin-bottom:8px"><span style="display:inline-block;'
                f'background:linear-gradient(90deg,#FFF3E0,#FFE0B2);color:#E65100;'
                f'border:1px solid #FFB74D;border-radius:14px;padding:3px 14px;'
                f'font-size:0.9em;font-weight:700">🏷️ {_badge} 프리셋 적용 중'
                f'</span></div>', unsafe_allow_html=True)
        else:
            st.markdown(
                '<div style="margin-bottom:8px"><span style="display:inline-block;'
                'background:#ECEFF1;color:#546E7A;border:1px solid #B0BEC5;'
                'border-radius:14px;padding:3px 14px;font-size:0.9em;font-weight:700">'
                '🛠️ 커스텀 파라미터</span></div>', unsafe_allow_html=True)

        # ── 편집 위젯 session 시드 (key만 사용 — 프리셋 버튼이 직접 덮어씀) ──
        _edit_defaults = {
            f"pd_etkr_{name}": str(P["ticker"]),
            f"pd_esd_{name}": float(P["seed"]),
            f"pd_esp_{name}": int(P["splits"]),
            f"pd_eti_{name}": bool(P["tiered"]),
            f"pd_eg1_{name}": float(P["g1"]), f"pd_et1_{name}": int(P["t1"]),
            f"pd_eg2_{name}": float(P["g2"]), f"pd_et2_{name}": int(P["t2"]),
            f"pd_eg3_{name}": float(P["g3"]),
            f"pd_ebg_{name}": float(P["buy_gap"]),
            f"pd_emd_{name}": P["sell_mode"],
            f"pd_ett_{name}": float(P["thr_target"]),
            f"pd_etp_{name}": float(P["thr_partial"]),
            f"pd_etc_{name}": float(P["thr_close"]),
            f"pd_egs_{name}": str(P.get("gs_url", "")),
            f"pd_ewt_{name}": bool(P.get("weighted", False)),
            f"pd_ew1_{name}": float(P.get("w1", 1.0)),
            f"pd_ew2_{name}": float(P.get("w2", 1.5)),
            f"pd_ew3_{name}": float(P.get("w3", 3.0)),
        }
        for _k, _v in _edit_defaults.items():
            st.session_state.setdefault(_k, _v)

        # ── 추천 프리셋 버튼 (클릭 → 편집값 적용, [💾 저장]으로 확정) ──
        st.caption("💡 추천 프리셋 — 클릭하면 아래 편집값에 적용됩니다. "
                   "[💾 저장]을 눌러야 확정돼요.")
        for _row_start in range(0, len(_PDAN_PRESETS), 3):
            _pcols = st.columns(3)
            for _pcol, _pr in zip(_pcols,
                                  _PDAN_PRESETS[_row_start:_row_start + 3]):
                if _pcol.button(_pr["label"],
                                key=f"pd_pre_{name}_{_pr['label']}",
                                help=_pr["help"], use_container_width=True):
                    _pp = _preset_params(_pr)
                    st.session_state[f"pd_esp_{name}"] = _pp["splits"]
                    st.session_state[f"pd_eti_{name}"] = _pp["tiered"]
                    st.session_state[f"pd_eg1_{name}"] = _pp["g1"]
                    st.session_state[f"pd_et1_{name}"] = _pp["t1"]
                    st.session_state[f"pd_eg2_{name}"] = _pp["g2"]
                    st.session_state[f"pd_et2_{name}"] = _pp["t2"]
                    st.session_state[f"pd_eg3_{name}"] = _pp["g3"]
                    st.session_state[f"pd_ebg_{name}"] = _pp["buy_gap"]
                    st.session_state[f"pd_emd_{name}"] = _pp["sell_mode"]
                    st.session_state[f"pd_ett_{name}"] = _pp["thr_target"]
                    st.session_state[f"pd_etp_{name}"] = _pp["thr_partial"]
                    st.session_state[f"pd_etc_{name}"] = _pp["thr_close"]
                    st.session_state[f"pd_ewt_{name}"] = _pp["weighted"]
                    st.session_state[f"pd_ew1_{name}"] = _pp["w1"]
                    st.session_state[f"pd_ew2_{name}"] = _pp["w2"]
                    st.session_state[f"pd_ew3_{name}"] = _pp["w3"]
                    st.rerun()
        st.divider()

        e1, e2, e3 = st.columns(3)
        e_tkr = e1.text_input("티커", key=f"pd_etkr_{name}")
        e_seed = e2.number_input("Seed ($)", min_value=0.0, step=1000.0,
                                 format="%.0f", key=f"pd_esd_{name}",
                                 help="0 가능 — 운용 자금이 전부 애드온일 때")
        e_sp = e3.number_input("분할수", min_value=2, step=1,
                               key=f"pd_esp_{name}")
        e_tier = st.checkbox("구간별 매수갭 (3구간)", key=f"pd_eti_{name}")
        if e_tier:
            r1 = st.columns(5)
            e_g1 = r1[0].number_input("1구간 갭%", min_value=0.1, step=0.1,
                                      format="%.1f", key=f"pd_eg1_{name}")
            e_t1 = r1[1].number_input("~티어", min_value=2, step=1,
                                      key=f"pd_et1_{name}")
            e_g2 = r1[2].number_input("2구간 갭%", min_value=0.1, step=0.1,
                                      format="%.1f", key=f"pd_eg2_{name}")
            e_t2 = r1[3].number_input("~티어 ", min_value=3, step=1,
                                      key=f"pd_et2_{name}")
            e_g3 = r1[4].number_input("3구간 갭% (이후)", min_value=0.1,
                                      step=0.1, format="%.1f",
                                      key=f"pd_eg3_{name}")
            e_bg = float(st.session_state[f"pd_ebg_{name}"])
        else:
            e_bg = st.number_input("매수갭 (%)", min_value=0.1, step=0.1,
                                   format="%.1f", key=f"pd_ebg_{name}")
            e_g1 = float(st.session_state[f"pd_eg1_{name}"])
            e_t1 = int(st.session_state[f"pd_et1_{name}"])
            e_g2 = float(st.session_state[f"pd_eg2_{name}"])
            e_t2 = int(st.session_state[f"pd_et2_{name}"])
            e_g3 = float(st.session_state[f"pd_eg3_{name}"])
        m1, m2 = st.columns([1, 2])
        e_md = m1.selectbox("매도 방식", list(SELL_MODES),
                            format_func=lambda k: SELL_MODES[k],
                            key=f"pd_emd_{name}")
        r2 = m2.columns(3)
        e_tt = r2[0].number_input("①목표%", min_value=0.5, step=0.5,
                                  format="%.1f", key=f"pd_ett_{name}")
        e_tp = r2[1].number_input("②트리거%", min_value=0.0, step=0.5,
                                  format="%.1f", key=f"pd_etp_{name}")
        e_tc = r2[2].number_input("③만족%", min_value=0.5, step=0.5,
                                  format="%.1f", key=f"pd_etc_{name}")
        e_wt = st.checkbox("하락가중 (구간별 자금 가중 — 경계는 위 3구간 티어)",
                           key=f"pd_ewt_{name}")
        if e_wt:
            w1c, w2c, w3c = st.columns(3)
            e_w1 = w1c.number_input("1구간 가중", min_value=0.1, step=0.1,
                                    format="%.1f", key=f"pd_ew1_{name}")
            e_w2 = w2c.number_input("2구간 가중", min_value=0.1, step=0.1,
                                    format="%.1f", key=f"pd_ew2_{name}")
            e_w3 = w3c.number_input("3구간 가중", min_value=0.1, step=0.1,
                                    format="%.1f", key=f"pd_ew3_{name}")
        else:
            e_w1 = float(st.session_state[f"pd_ew1_{name}"])
            e_w2 = float(st.session_state[f"pd_ew2_{name}"])
            e_w3 = float(st.session_state[f"pd_ew3_{name}"])
        e_gs = st.text_input(
            "구글시트 URL (계좌 전용 — 비우면 공통/개인설정 사용)",
            key=f"pd_egs_{name}")
        b1, b2 = st.columns(2)
        if b1.button("💾 저장", use_container_width=True,
                     key=f"pd_esv_{name}"):
            sp_i = int(e_sp)
            t1_i = max(2, min(int(e_t1), max(sp_i - 1, 2)))
            t2_i = max(t1_i + 1, min(int(e_t2), sp_i))
            cfg["accounts"][name] = {
                **P, "ticker": e_tkr.strip().upper(), "seed": float(e_seed),
                "splits": sp_i, "tiered": bool(e_tier),
                "buy_gap": float(e_bg), "g1": float(e_g1), "t1": t1_i,
                "g2": float(e_g2), "t2": t2_i, "g3": float(e_g3),
                "sell_mode": e_md, "thr_target": float(e_tt),
                "thr_partial": float(e_tp), "thr_close": float(e_tc),
                "weighted": bool(e_wt), "w1": float(e_w1),
                "w2": float(e_w2), "w3": float(e_w3),
                "gs_url": e_gs.strip()}
            _save_cfg(cfg)
            st.session_state.pop(f"pd_seedin_{name}", None)
            st.rerun()
        if b2.button("🗑 계좌 삭제", use_container_width=True,
                     key=f"pd_edel_{name}"):
            cfg["accounts"].pop(name, None)
            _save_cfg(cfg)
            st.rerun()


# ══════════════════════════════════════════════
# 전략 소개 탭
# ══════════════════════════════════════════════
@st.cache_data(show_spinner=False)
def _load_guide(path: str, mtime: float) -> str | None:
    import re
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except OSError:
        return None
    return re.sub(r"(?m)^(#{1,4}) ",
                  lambda m: "#" * min(len(m.group(1)) + 2, 6) + " ", text)


def render_intro_tab(params: dict):
    st.subheader("📖 평단 트레이딩이란")
    st.markdown("**정해둔 갭으로 지정가 매수를 사다리처럼 깔아 하락할 때마다 "
                "모으고, 평단 대비 목표수익률 도달 시 전량 매도 후 재시작하는 "
                "실시간 그리드 전략.**")
    _guide_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "평단법_설명.md")
    _mtime = os.path.getmtime(_guide_path) if os.path.exists(_guide_path) \
        else 0.0
    if st.toggle("📚 종합 가이드 전체 보기 (용어·규칙·세팅 후보군·설계 검증·"
                 "백테스트·운영 노하우)", value=True, key="pd_show_guide"):
        _guide_md = _load_guide(_guide_path, _mtime)
        if _guide_md:
            st.markdown(_guide_md)
        else:
            st.info("가이드 문서(평단법_설명.md)를 찾을 수 없습니다.")

    st.divider()
    st.subheader("🧪 이 앱의 백테스트 체결 가정 (일봉 OHLC, 보수적)")
    st.markdown("""
- ①②는 당일 시작 시점의 평단 기준 지정가 체결 (당일 매수로 낮아진 목표로는 당일 매도 안 함)
- ③은 당일 매수 반영 후의 평단으로 종가에 판단
- 매도가 체결된 날은 추가매수 없음 (장중 순서 불명 → 보수적 처리)
- 갭하락 시 레벨가 대신 시가로 체결 (유리한 체결가 반영)
- 손절 규칙이 없는 전략이므로 MDD와 최장 사이클 기간을 반드시 확인하세요.
""")


# ══════════════════════════════════════════════
# 개인 설정 탭
# ══════════════════════════════════════════════
def render_settings_tab():
    st.subheader("⚙️ 평단 트레이딩 개인 설정")
    cfg = _load_cfg()
    where = ("Google Sheets `users.pdan_config`" if _IS_CLOUD
             else "`~/.pdan/config.json` (11.평단법 독립 앱과 공유)")
    st.caption(f"저장 위치: {where}")

    c1, c2 = st.columns(2)
    astra_ws = c1.text_input("ASTRA 워크시트 이름",
                             value=cfg.get("astra_ws", "ASTRA"),
                             key="pd_set_ws")
    gs_url = c2.text_input(
        "공통 구글시트 URL (계좌 전용 URL이 없을 때 사용)",
        value=cfg.get("gs_url", ""), key="pd_set_gs",
        help="비우면 (Cloud) 공통 개인설정의 gs_url을 사용합니다")
    if st.button("💾 설정 저장", key="pd_set_save"):
        cfg["astra_ws"] = astra_ws.strip() or "ASTRA"
        cfg["gs_url"] = gs_url.strip()
        _save_cfg(cfg)
        st.success("저장 완료")

    if not _IS_CLOUD:
        st.divider()
        st.markdown("**텔레그램 (로컬 모니터용)**")
        tc1, tc2 = st.columns(2)
        tg_token = tc1.text_input("Bot Token", value=cfg.get("tg_token", ""),
                                  type="password", key="pd_set_tgt",
                                  help="@BotFather 에서 발급")
        tg_chat = tc2.text_input("Chat ID", value=cfg.get("tg_chat_id", ""),
                                 key="pd_set_tgc",
                                 help="@userinfobot 에게 말 걸면 확인 가능")
        tb1, tb2 = st.columns(2)
        if tb1.button("💾 텔레그램 저장", key="pd_set_tgsave",
                      use_container_width=True):
            cfg["tg_token"] = tg_token.strip()
            cfg["tg_chat_id"] = tg_chat.strip()
            _save_cfg(cfg)
            st.success("저장 완료")
        if tb2.button("📨 테스트 전송", key="pd_set_tgtest",
                      use_container_width=True):
            import requests
            try:
                r = requests.post(
                    f"https://api.telegram.org/bot{tg_token.strip()}"
                    "/sendMessage",
                    json={"chat_id": tg_chat.strip(),
                          "text": "✅ [평단 트레이딩] 텔레그램 연결 테스트"},
                    timeout=10).json()
                if r.get("ok"):
                    st.success("전송 성공 — 텔레그램을 확인하세요")
                else:
                    st.error(f"전송 실패: {r.get('description')}")
            except Exception as e:
                st.error(f"전송 실패: {e}")

    st.divider()
    st.markdown("**등록된 계좌**")
    accounts = cfg.get("accounts", {})
    if not accounts:
        st.info("계좌가 없습니다. 주문표 탭에서 [➕ 계좌 추가]로 만드세요.")
    else:
        for nm, P in accounts.items():
            Pm = {**PARAM_DEFAULTS, **P}
            _, gd = _gap_of(Pm)
            st.caption(f"📈 **{nm}** — {Pm['ticker']} · "
                       f"Seed ${float(Pm['seed']):,.0f} · "
                       f"{int(Pm['splits'])}분할 · {gd} · "
                       f"{SELL_MODES[Pm['sell_mode']]} {_thr_of(Pm)}% · "
                       f"체결 {len(Pm.get('filled', []))}티어 저장됨")

    st.divider()
    st.markdown("""
**로컬 전용 도구 안내** — 실시간 가격 감시 · 텔레그램 알림/매도 명령 ·
매도 주문 자동 갱신은 로컬 PC에서 도는 별도 모니터(`11.평단법/monitor.py`)로
제공됩니다. 웹 앱은 백테스트 · 설계 시뮬레이션 · 주문표/계좌 관리 ·
ASTRA 시트 전송을 담당합니다.
""")
