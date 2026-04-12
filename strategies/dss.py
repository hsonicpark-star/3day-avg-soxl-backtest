"""
strategies/dss.py — DSS 동파법(동적파도타기법) 전략 모듈

종목: SOXL (3x 레버리지 ETF)
모드: QQQ 주간 RSI 기반 안전모드(SF)/공세모드(AG) 자동 전환
매매: LOC(Limit on Close), 시드분할, 복리 관리

인터페이스: Sigma와 동일 패턴 (자체 사이드바 사용)
- render_sidebar() → params dict
- render_backtest_tab(params)
- render_optimization_tab(params)
- render_ordersheet_tab(params)
- render_intro_tab(params)  — 통합앱에서는 render_intro_tab() 호출
- render_settings_tab()
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import math
import json
import itertools
import random
import os
import requests
import io as _io
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

from dss_engine import (
    load_price_data, get_weekly_closes, calc_weekly_rsi,
    build_weekly_rsi_series, build_mode_series, determine_mode,
    run_backtest, run_backtest_fast, DSSParams
)

from common.config import _IS_CLOUD, _CONFIG, load_config, save_config, \
    get_ticker_settings, save_ticker_setting

# ──────────────────────────────────────────────
# DSS 설정 경로 (로컬: ~/.dss/, 클라우드: session_state)
# ──────────────────────────────────────────────
_DSS_CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".dss")
_DSS_CONFIG_PATH = os.path.join(_DSS_CONFIG_DIR, "config.json")


def _load_dss_config() -> dict:
    if os.path.exists(_DSS_CONFIG_PATH):
        try:
            with open(_DSS_CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_dss_config(cfg: dict):
    os.makedirs(_DSS_CONFIG_DIR, exist_ok=True)
    with open(_DSS_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


# ──────────────────────────────────────────────
# 텔레그램
# ──────────────────────────────────────────────

def _send_telegram(token: str, chat_id: str, text: str) -> dict:
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        return resp.json()
    except Exception as e:
        return {"ok": False, "description": str(e)}


# ──────────────────────────────────────────────
# 히스토리 (B방식)
# ──────────────────────────────────────────────

def _get_history_path() -> str:
    return os.path.join(_DSS_CONFIG_DIR, "history_SOXL.csv")


def _load_dss_history() -> pd.DataFrame:
    path = _get_history_path()
    if os.path.exists(path):
        try:
            return pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def _save_dss_history(bt_df: pd.DataFrame):
    if bt_df is None or bt_df.empty:
        return
    rows = []
    for _, row in bt_df.iterrows():
        date_str = str(row['날짜'])[:10]
        close = float(row['종가'])
        mode = row['모드']
        buy_order = row['매수주문가']
        sell_target = row['매도목표가']
        action = "-"
        trade_qty = 0
        trade_amt = 0.0
        realized_pnl = None
        realized_pct = None
        bought = row['매수체결'] is not None and not (isinstance(row['매수체결'], float) and np.isnan(row['매수체결']))
        sold = row['매도내역'] is not None
        if sold:
            sell_details = row['매도내역']
            total_sell_qty = sum(s['qty'] for s in sell_details)
            total_sell_amt = sum(s['qty'] * s['sell_price'] for s in sell_details)
            total_pnl = sum(s['pnl'] for s in sell_details)
            total_buy_amt = sum(s['qty'] * s['buy_price'] for s in sell_details)
            pnl_pct = (total_pnl / total_buy_amt * 100) if total_buy_amt > 0 else 0
            action = f"SELL (${close:.2f})"
            trade_qty = -total_sell_qty
            trade_amt = total_sell_amt
            realized_pnl = total_pnl
            realized_pct = pnl_pct
        if bought:
            buy_price = float(row['매수체결'])
            qty = int(row['수량'])
            if action == "-":
                action = f"BUY (${buy_price:.2f})"
                trade_qty = qty
                trade_amt = buy_price * qty
            else:
                action += f" / BUY (${buy_price:.2f})"
                trade_qty += qty
                trade_amt += buy_price * qty
        rows.append({
            "날짜": date_str, "종가": round(close, 4), "모드": mode,
            "매수주문가": round(float(buy_order), 4) if buy_order is not None else "-",
            "매도경계가": round(float(sell_target), 4) if sell_target is not None and not (isinstance(sell_target, float) and np.isnan(sell_target)) else "-",
            "매매": action, "거래주수": trade_qty if trade_qty != 0 else "-",
            "거래금액($)": round(trade_amt, 2) if trade_amt > 0 else "-",
            "실현손익($)": f"+${realized_pnl:,.2f}" if realized_pnl is not None and realized_pnl >= 0 else f"-${abs(realized_pnl):,.2f}" if realized_pnl is not None else "-",
            "실현손익률(%)": f"+{realized_pct:.2f}%" if realized_pct is not None and realized_pct >= 0 else f"{realized_pct:.2f}%" if realized_pct is not None else "-",
            "보유시드": int(row['보유포지션수']), "보유기간": "-",
            "현금($)": f"${float(row['예수금']):,.2f}",
            "총자산($)": f"${float(row['총자산']):,.2f}",
        })
    df_new = pd.DataFrame(rows)
    df_existing = _load_dss_history()
    if not df_existing.empty and "날짜" in df_existing.columns:
        existing_dates = set(df_existing["날짜"].astype(str))
        df_add = df_new[~df_new["날짜"].astype(str).isin(existing_dates)].copy()
        if df_add.empty:
            return
        df_merged = pd.concat([df_existing, df_add], ignore_index=True)
    else:
        df_merged = df_new
    os.makedirs(_DSS_CONFIG_DIR, exist_ok=True)
    df_merged.to_csv(_get_history_path(), index=False, encoding="utf-8-sig")


# ──────────────────────────────────────────────
# 다음 거래일 (미국 휴장일 제외)
# ──────────────────────────────────────────────

def _nth_weekday(year, month, weekday, n):
    first = datetime(year, month, 1).date()
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))

def _last_weekday(year, month, weekday):
    if month == 12:
        last = datetime(year + 1, 1, 1).date() - timedelta(days=1)
    else:
        last = datetime(year, month + 1, 1).date() - timedelta(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - timedelta(days=offset)

def _easter_date(year):
    a = year % 19; b, c = divmod(year, 100); d, e = divmod(b, 4)
    f = (b + 8) // 25; g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30; i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return datetime(year, month, day + 1).date()

def next_trading_date(d=None):
    if d is None:
        d = datetime.today().date()
    year = d.year
    us_holidays = set()
    us_holidays.add(datetime(year, 1, 1).date())
    us_holidays.add(_nth_weekday(year, 1, 0, 3))
    us_holidays.add(_nth_weekday(year, 2, 0, 3))
    us_holidays.add(_easter_date(year) - timedelta(days=2))
    us_holidays.add(_last_weekday(year, 5, 0))
    us_holidays.add(datetime(year, 6, 19).date())
    us_holidays.add(datetime(year, 7, 4).date())
    us_holidays.add(_nth_weekday(year, 9, 0, 1))
    us_holidays.add(_nth_weekday(year, 11, 3, 4))
    us_holidays.add(datetime(year, 12, 25).date())
    observed = set()
    for h in us_holidays:
        if h.weekday() == 5: observed.add(h - timedelta(days=1))
        elif h.weekday() == 6: observed.add(h + timedelta(days=1))
    all_holidays = us_holidays | observed
    candidate = d + timedelta(days=1)
    for _ in range(30):
        if candidate.weekday() < 5 and candidate not in all_holidays:
            return candidate
        candidate += timedelta(days=1)
    return candidate


# ──────────────────────────────────────────────
# 데이터 캐싱
# ──────────────────────────────────────────────

@st.cache_data(show_spinner="SOXL 데이터 로딩...")
def get_soxl_data():
    return load_price_data("SOXL", "2009-06-01", "2026-12-31")

@st.cache_data(show_spinner="QQQ 데이터 로딩...")
def get_qqq_data():
    return load_price_data("QQQ", "2009-01-01", "2026-12-31")

@st.cache_data(show_spinner="주간 RSI 계산 중...")
def get_mode_series(_qqq_hash):
    qqq = get_qqq_data()
    weekly_rsi = build_weekly_rsi_series(qqq)
    return build_mode_series(weekly_rsi)


# ──────────────────────────────────────────────
# 사이드바 (Sigma와 동일 패턴: 자체 사이드바)
# ──────────────────────────────────────────────

def render_sidebar():
    """DSS 사이드바 렌더링. params dict 반환."""
    st.sidebar.markdown("### 🔄 공통 설정")
    col1, col2, col3 = st.sidebar.columns(3)
    pcr = col1.number_input("이익복리율(%)", min_value=0, max_value=100, value=80, step=1, key="dss_pcr")
    lcr = col2.number_input("손실복리율(%)", min_value=0, max_value=100, value=30, step=1, key="dss_lcr")
    renewal_period = col3.number_input("갱신주기", min_value=1, max_value=100, value=10, step=1, key="dss_renew")

    st.sidebar.markdown("### 🔵 안전모드 (SF)")
    sc1, sc2, sc3, sc4 = st.sidebar.columns(4)
    sf_div = sc1.number_input("분할수", min_value=1, max_value=50, value=7, step=1, key="dss_sf_div")
    sf_hold = sc2.number_input("최대보유", min_value=1, max_value=100, value=30, step=1, key="dss_sf_hold")
    sf_buy = sc3.number_input("매수%", min_value=0.0, max_value=30.0, value=3.0, step=0.1, key="dss_sf_buy")
    sf_sell = sc4.number_input("매도%", min_value=0.0, max_value=30.0, value=0.2, step=0.1, key="dss_sf_sell")

    st.sidebar.markdown("### 🟢 공세모드 (AG)")
    ac1, ac2, ac3, ac4 = st.sidebar.columns(4)
    ag_div = ac1.number_input("분할수", min_value=1, max_value=50, value=7, step=1, key="dss_ag_div")
    ag_hold = ac2.number_input("최대보유", min_value=1, max_value=100, value=7, step=1, key="dss_ag_hold")
    ag_buy = ac3.number_input("매수%", min_value=0.0, max_value=30.0, value=5.0, step=0.1, key="dss_ag_buy")
    ag_sell = ac4.number_input("매도%", min_value=0.0, max_value=30.0, value=2.5, step=0.1, key="dss_ag_sell")

    st.sidebar.markdown("### ⚙️ 백테스트 설정")
    bc1, bc2 = st.sidebar.columns(2)
    initial_capital = bc1.number_input("초기투자금", min_value=1000, max_value=10000000, value=10000, step=1000, key="dss_cap")
    fee_rate = bc2.number_input("수수료(%)", min_value=0.0, max_value=1.0, value=0.04, step=0.001, format="%.3f", key="dss_fee")

    dc1, dc2 = st.sidebar.columns(2)
    start_date = dc1.date_input("투자시작일", value=pd.Timestamp("2010-03-12"), key="dss_start")
    end_date = dc2.date_input("투자종료일", value=pd.Timestamp("2026-04-10"), key="dss_end")

    return {
        "sf_div": sf_div, "sf_hold": sf_hold, "sf_buy": sf_buy, "sf_sell": sf_sell,
        "ag_div": ag_div, "ag_hold": ag_hold, "ag_buy": ag_buy, "ag_sell": ag_sell,
        "pcr": pcr, "lcr": lcr, "renewal_period": renewal_period,
        "initial_capital": initial_capital, "fee_rate": fee_rate,
        "start_date": start_date, "end_date": end_date,
        "bt_ticker": "SOXL",
        "bt_start_date": start_date, "bt_end_date": end_date,
        "bt_initial_capital": initial_capital,
    }


def _make_params(p):
    """params dict → DSSParams 변환."""
    return DSSParams(
        sf_divisions=p["sf_div"], sf_max_hold=p["sf_hold"],
        sf_buy_pct=p["sf_buy"] / 100, sf_sell_pct=p["sf_sell"] / 100,
        ag_divisions=p["ag_div"], ag_max_hold=p["ag_hold"],
        ag_buy_pct=p["ag_buy"] / 100, ag_sell_pct=p["ag_sell"] / 100,
        initial_capital=float(p["initial_capital"]),
        fee_rate=p["fee_rate"] / 100,
        renewal_period=p["renewal_period"],
        pcr=p["pcr"] / 100, lcr=p["lcr"] / 100,
    )


# ══════════════════════════════════════════════
# 탭 1: 백테스트
# ══════════════════════════════════════════════

def render_backtest_tab(params):
    """백테스트 탭."""
    p = params
    st.subheader("📊 DSS 동파법 백테스트")

    if st.button("▶ 백테스트 실행", type="primary", key="dss_bt_run"):
        soxl = get_soxl_data()
        qqq = get_qqq_data()
        ms = get_mode_series(len(qqq))
        dss_p = _make_params(p)

        with st.spinner("백테스트 실행 중..."):
            bt = run_backtest(dss_p, soxl, ms, str(p["start_date"]), str(p["end_date"]))

        if bt is not None and not bt.empty:
            st.session_state["dss_bt_result"] = bt
        else:
            st.warning("결과가 없습니다. 날짜 범위를 확인하세요.")

    bt = st.session_state.get("dss_bt_result")
    if bt is None or (isinstance(bt, pd.DataFrame) and bt.empty):
        st.info("👆 '백테스트 실행' 버튼을 클릭하세요.")
        return

    # ── 핵심 지표 ──
    _assets = bt['총자산'].values.astype(float)
    _final = float(_assets[-1])
    _days = len(bt)
    _years = _days / 252
    _init = float(p["initial_capital"])
    _total_ret = (_final / _init - 1)
    _cagr = (_final / _init) ** (1 / _years) - 1 if _years > 0 else 0
    _peak = np.maximum.accumulate(_assets)
    _dd = (_assets - _peak) / _peak
    _mdd = float(_dd.min())
    _calmar = abs(_cagr / _mdd) if _mdd != 0 else 0
    _daily_rets = np.diff(_assets) / _assets[:-1]
    _sharpe = (np.mean(_daily_rets) / np.std(_daily_rets) * np.sqrt(252)) if np.std(_daily_rets) > 0 else 0
    _neg = _daily_rets[_daily_rets < 0]
    _sortino = (np.mean(_daily_rets) / np.std(_neg) * np.sqrt(252)) if len(_neg) > 0 and np.std(_neg) > 0 else 0

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("총 수익률", f"{_total_ret*100:+,.1f}%")
    m2.metric("CAGR", f"{_cagr*100:.2f}%")
    m3.metric("MDD", f"{_mdd*100:.2f}%")
    m4.metric("Calmar", f"{_calmar:.3f}")
    m5.metric("Sharpe", f"{_sharpe:.3f}")
    m6.metric("최종자산", f"${_final:,.0f}")

    # ── 자산 추이 + 낙폭 차트 ──
    _dates = pd.to_datetime(bt['날짜'])
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05)
    fig.add_trace(go.Scatter(x=_dates, y=_assets, mode='lines', name='총자산',
                             line=dict(color='#1565C0', width=1.5)), row=1, col=1)
    fig.add_trace(go.Scatter(x=_dates, y=_dd * 100, mode='lines', name='낙폭(%)',
                             fill='tozeroy', fillcolor='rgba(198,40,40,0.15)',
                             line=dict(color='#C62828', width=1)), row=2, col=1)
    fig.update_layout(height=500, showlegend=True, legend=dict(orientation='h', y=1.02))
    fig.update_yaxes(title_text="총자산 ($)", row=1, col=1)
    fig.update_yaxes(title_text="낙폭 (%)", row=2, col=1)
    st.plotly_chart(fig, use_container_width=True)

    # ── 매매 기록 테이블 ──
    with st.expander("📋 일별 매매 기록", expanded=False):
        _show_cols = ['날짜', '종가', '모드', '매수주문가', '매수체결', '수량',
                      '매도목표가', '보유포지션수', '예수금', '총자산', '당일실현', '누적실현']
        _bt_show = bt[_show_cols].copy()
        _bt_show['날짜'] = _bt_show['날짜'].astype(str).str[:10]
        st.dataframe(_bt_show, hide_index=True, use_container_width=True,
                     height=min(38 + 35 * len(_bt_show), 600))


# ══════════════════════════════════════════════
# 탭 2: 최적화
# ══════════════════════════════════════════════

def render_optimization_tab(params):
    """파라미터 최적화 탭."""
    p = params
    st.subheader("🔍 파라미터 최적화")

    opt_mode = st.radio("최적화 방식", ["그리드 서치", "랜덤 서치"], horizontal=True, key="dss_opt_mode")

    with st.form("dss_opt_form"):
        st.markdown("**탐색 범위 설정**")
        oc1, oc2 = st.columns(2)
        with oc1:
            st.markdown("🔵 **안전모드 (SF)**")
            sf_buy_range = st.slider("SF 매수%", 0.5, 10.0, (2.0, 5.0), 0.5, key="dss_opt_sfb")
            sf_sell_range = st.slider("SF 매도%", 0.1, 5.0, (0.1, 1.0), 0.1, key="dss_opt_sfs")
        with oc2:
            st.markdown("🟢 **공세모드 (AG)**")
            ag_buy_range = st.slider("AG 매수%", 1.0, 15.0, (3.0, 8.0), 0.5, key="dss_opt_agb")
            ag_sell_range = st.slider("AG 매도%", 0.5, 10.0, (1.5, 5.0), 0.5, key="dss_opt_ags")

        step = st.number_input("탐색 스텝", 0.5, 5.0, 1.0, 0.5, key="dss_opt_step")
        n_random = st.number_input("랜덤 서치 횟수", 50, 5000, 500, 50, key="dss_opt_n") if opt_mode == "랜덤 서치" else 0
        sort_col = st.selectbox("정렬 기준", ["Calmar", "CAGR(%)", "수익률(%)", "MDD(%)"], key="dss_opt_sort")
        submitted = st.form_submit_button("▶ 최적화 실행", type="primary")

    if submitted:
        soxl = get_soxl_data()
        qqq = get_qqq_data()
        ms = get_mode_series(len(qqq))

        sf_buys = np.arange(sf_buy_range[0], sf_buy_range[1] + step, step)
        sf_sells = np.arange(sf_sell_range[0], sf_sell_range[1] + step, step)
        ag_buys = np.arange(ag_buy_range[0], ag_buy_range[1] + step, step)
        ag_sells = np.arange(ag_sell_range[0], ag_sell_range[1] + step, step)

        if opt_mode == "그리드 서치":
            combos = list(itertools.product(sf_buys, sf_sells, ag_buys, ag_sells))
        else:
            all_combos = list(itertools.product(sf_buys, sf_sells, ag_buys, ag_sells))
            combos = random.sample(all_combos, min(int(n_random), len(all_combos)))

        st.info(f"총 {len(combos)}개 조합 실행 중...")
        prog = st.progress(0)
        results = []
        for i, (sb, ss, ab, as_) in enumerate(combos):
            dss_p = DSSParams(
                sf_divisions=p["sf_div"], sf_max_hold=p["sf_hold"],
                sf_buy_pct=sb / 100, sf_sell_pct=ss / 100,
                ag_divisions=p["ag_div"], ag_max_hold=p["ag_hold"],
                ag_buy_pct=ab / 100, ag_sell_pct=as_ / 100,
                initial_capital=float(p["initial_capital"]),
                fee_rate=p["fee_rate"] / 100,
                renewal_period=p["renewal_period"],
                pcr=p["pcr"] / 100, lcr=p["lcr"] / 100,
            )
            r = run_backtest_fast(dss_p, soxl, ms, str(p["start_date"]), str(p["end_date"]))
            if r:
                final = r['final_asset']
                days = r['total_days']
                yrs = days / 252
                ret = (final / float(p["initial_capital"]) - 1) * 100
                cagr = ((final / float(p["initial_capital"])) ** (1 / yrs) - 1) * 100 if yrs > 0 else 0
                mdd = r['mdd'] * 100
                calmar = abs(cagr / mdd) if mdd != 0 else 0
                results.append({
                    'SF매수%': round(sb, 2), 'SF매도%': round(ss, 2),
                    'AG매수%': round(ab, 2), 'AG매도%': round(as_, 2),
                    '수익률(%)': round(ret, 2), 'CAGR(%)': round(cagr, 2),
                    'MDD(%)': round(mdd, 2), 'Calmar': round(calmar, 3),
                    '최종자산($)': round(final, 0),
                })
            prog.progress((i + 1) / len(combos))
        prog.empty()

        if results:
            df_opt = pd.DataFrame(results)
            ascending = True if sort_col == "MDD(%)" else False
            df_opt = df_opt.sort_values(sort_col, ascending=ascending).reset_index(drop=True)
            st.session_state["dss_opt_result"] = df_opt

    df_opt = st.session_state.get("dss_opt_result")
    if df_opt is not None and not df_opt.empty:
        st.subheader(f"상위 20개 결과")
        st.dataframe(df_opt.head(20), hide_index=True, use_container_width=True)

        # 리스크-수익 분포
        st.subheader("리스크-수익 분포 (CAGR vs MDD)")
        fig_scatter = px.scatter(
            df_opt, x="MDD(%)", y="CAGR(%)", color="Calmar",
            color_continuous_scale="RdYlGn", hover_data=["SF매수%", "SF매도%", "AG매수%", "AG매도%"],
            title="CAGR vs MDD")
        fig_scatter.update_layout(height=450)
        st.plotly_chart(fig_scatter, use_container_width=True)


# ══════════════════════════════════════════════
# 탭 3: 주문표
# ══════════════════════════════════════════════

def render_ordersheet_tab(params):
    """주문표 & 계좌관리 탭."""
    p = params
    st.subheader("📋 DSS 동파법 주문표")
    st.caption("포트폴리오를 추적하여 현황과 내일 LOC 주문을 표시합니다.")

    _cfg = _load_dss_config()
    _saved = _cfg.get("params", {})

    # 파라미터 소스 선택
    _use_saved = False
    if _saved:
        _use_saved = st.checkbox("저장된 파라미터 사용", value=True, key="dss_use_saved")
        if _use_saved:
            st.caption(f"SF: 분할{_saved.get('sf_div',7)} 보유{_saved.get('sf_hold',30)} "
                       f"매수{_saved.get('sf_buy',3.0)}% 매도{_saved.get('sf_sell',0.2)}% | "
                       f"AG: 분할{_saved.get('ag_div',7)} 보유{_saved.get('ag_hold',7)} "
                       f"매수{_saved.get('ag_buy',5.0)}% 매도{_saved.get('ag_sell',2.5)}%")

    # 시작일/자본금
    os_col1, os_col2 = st.columns(2)
    os_start = os_col1.date_input("시작일", value=datetime.strptime(
        _cfg.get("os_start", "2024-01-01"), "%Y-%m-%d").date(), key="dss_os_start")
    os_capital = os_col2.number_input("투자 자본금", value=float(_cfg.get("os_capital", 10000)),
                                      min_value=1000.0, step=1000.0, key="dss_os_cap")

    _os_btn_label = "🔄 새로고침" if st.session_state.get("dss_os_result") else "📋 주문표 로드"
    if st.button(_os_btn_label, type="primary", key="dss_run_os"):
        _cfg["os_start"] = str(os_start)
        _cfg["os_capital"] = float(os_capital)
        _save_dss_config(_cfg)

        soxl = get_soxl_data()
        qqq = get_qqq_data()
        ms = get_mode_series(len(qqq))

        if _use_saved and _saved:
            _cur = _saved
        else:
            _cur = p

        dss_p = DSSParams(
            sf_divisions=int(_cur.get("sf_div", p["sf_div"])),
            sf_max_hold=int(_cur.get("sf_hold", p["sf_hold"])),
            sf_buy_pct=float(_cur.get("sf_buy", p["sf_buy"])) / 100,
            sf_sell_pct=float(_cur.get("sf_sell", p["sf_sell"])) / 100,
            ag_divisions=int(_cur.get("ag_div", p["ag_div"])),
            ag_max_hold=int(_cur.get("ag_hold", p["ag_hold"])),
            ag_buy_pct=float(_cur.get("ag_buy", p["ag_buy"])) / 100,
            ag_sell_pct=float(_cur.get("ag_sell", p["ag_sell"])) / 100,
            initial_capital=float(os_capital),
            fee_rate=float(_cur.get("fee_rate", p["fee_rate"])) / 100,
            renewal_period=int(_cur.get("renewal_period", p["renewal_period"])),
            pcr=float(_cur.get("pcr", p["pcr"])) / 100,
            lcr=float(_cur.get("lcr", p["lcr"])) / 100,
        )

        today_str = pd.Timestamp.today().strftime("%Y-%m-%d")
        with st.spinner("포트폴리오 시뮬레이션 중..."):
            bt_df = run_backtest(dss_p, soxl, ms, str(os_start), today_str)

        if bt_df is None or bt_df.empty:
            st.warning("시뮬레이션 결과가 없습니다.")
        else:
            _save_dss_history(bt_df)
            last = bt_df.iloc[-1]
            prev_close = float(last['종가'])
            last_date = pd.Timestamp(last['날짜'])
            last_mode = last['모드']
            n_pos = int(last['보유포지션수'])

            if last_mode == "AG":
                cur_div = dss_p.ag_divisions
                cur_buy = dss_p.ag_buy_pct
                cur_sell = dss_p.ag_sell_pct
                cur_hold = dss_p.ag_max_hold
            else:
                cur_div = dss_p.sf_divisions
                cur_buy = dss_p.sf_buy_pct
                cur_sell = dss_p.sf_sell_pct
                cur_hold = dss_p.sf_max_hold

            open_positions = []
            _all_sold = set()
            for _, row in bt_df.iterrows():
                if row['매도내역'] is not None:
                    for sr in row['매도내역']:
                        _all_sold.add((pd.Timestamp(sr['buy_date']), sr['buy_price'], sr['qty']))
            for _, row in bt_df.iterrows():
                if row['매수체결'] is not None and row['수량'] > 0:
                    bk = (pd.Timestamp(row['날짜']), float(row['매수체결']), int(row['수량']))
                    if bk not in _all_sold:
                        open_positions.append({
                            'buy_date': pd.Timestamp(row['날짜']),
                            'buy_price': float(row['매수체결']),
                            'qty': int(row['수량']),
                            'sell_target': float(row['매도목표가']) if row['매도목표가'] is not None else None,
                            'stop_date': pd.Timestamp(row['손절예정일']) if row['손절예정일'] is not None else None,
                            'mode': row['모드'],
                        })

            capital = float(last['투자금'])
            next_buy = math.floor(prev_close * (1 + cur_buy) * 100) / 100
            seed = capital / cur_div
            buy_qty_est = int(seed / next_buy) if next_buy > 0 else 0

            weekly_rsi_df = build_weekly_rsi_series(qqq)
            latest_rsi = float(weekly_rsi_df.iloc[-1]['rsi']) if len(weekly_rsi_df) > 0 else None
            prev_rsi = float(weekly_rsi_df.iloc[-2]['rsi']) if len(weekly_rsi_df) > 1 else None

            st.session_state["dss_os_result"] = {
                "bt_df": bt_df, "prev_close": prev_close, "last_date": last_date,
                "last_mode": last_mode, "n_pos": n_pos,
                "total_asset": float(last['총자산']), "cash": float(last['예수금']),
                "capital": capital, "holding_value": float(last['평가금']),
                "eval_pnl": float(last['평가손익']), "cum_realized": float(last['누적실현']),
                "sell_count": int(last['누적매도']), "open_positions": open_positions,
                "next_buy_order": next_buy, "seed_per_trade": seed,
                "buy_qty_est": buy_qty_est, "cur_divisions": cur_div,
                "cur_buy_pct": cur_buy, "cur_sell_pct": cur_sell, "cur_max_hold": cur_hold,
                "latest_rsi": latest_rsi, "prev_rsi": prev_rsi,
                "latest_rsi_date": str(weekly_rsi_df.iloc[-1]['week_end'].date()) if len(weekly_rsi_df) > 0 else None,
                "os_capital": float(os_capital),
            }

    _os = st.session_state.get("dss_os_result")
    if _os is not None:
        # ── 모드 & 포트폴리오 현황 (HTML 카드) ──
        _mode_icon = "🟢" if _os["last_mode"] == "AG" else "🔵"
        _mode_label = "공세모드 (AG)" if _os["last_mode"] == "AG" else "안전모드 (SF)"
        _mode_bg = "#E8F5E9" if _os["last_mode"] == "AG" else "#E3F2FD"
        _mode_fg = "#2E7D32" if _os["last_mode"] == "AG" else "#1565C0"

        st.markdown(f"""
        <div style="display:flex;gap:10px;margin:12px 0">
          <div style="flex:1.2;background:{_mode_bg};border-radius:10px;padding:14px;text-align:center">
            <div style="font-size:1.3em;font-weight:700;color:{_mode_fg}">{_mode_icon} {_mode_label}</div>
            <div style="font-size:0.7em;color:#888;margin-top:4px">
            데이터 기준: <b>{_os['last_date'].strftime('%Y-%m-%d')}</b></div>
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
            <div style="font-size:0.72em;color:#888">보유시드</div>
            <div style="font-size:1.15em;font-weight:700">{_os['n_pos']} / {_os['cur_divisions']}</div>
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
            <div style="font-size:0.72em;color:#888">총자산</div>
            <div style="font-size:1.1em;font-weight:700">${_os['total_asset']:,.0f}</div>
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
            <div style="font-size:0.72em;color:#888">현금</div>
            <div style="font-size:1.1em;font-weight:700">${_os['cash']:,.0f}</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # ── LOC 주문 ──
        st.divider()
        _next_td = next_trading_date()
        st.subheader(f"📑 오늘의 LOC 주문  ({_next_td.strftime('%Y-%m-%d')})")
        st.markdown(
            f"<div style='font-size:0.85em;color:#888;margin-bottom:8px'>"
            f"전일종가 = <b>${_os['prev_close']:,.2f}</b>&ensp;·&ensp;"
            f"매수주문가 = <b>${_os['next_buy_order']:,.2f}</b>&ensp;·&ensp;"
            f"1회시드 = <b>${_os['seed_per_trade']:,.0f}</b></div>",
            unsafe_allow_html=True)

        today_orders = []
        for i, pos in enumerate(_os['open_positions']):
            if pos['sell_target'] is not None:
                today_orders.append({
                    "구분": "매도", "시드": f"티어{i+1}",
                    "LOC 기준가": f"${pos['sell_target']:,.2f}",
                    "수량": f"{pos['qty']:,}주",
                })
        if _os['n_pos'] < _os['cur_divisions']:
            today_orders.append({
                "구분": "매수", "시드": f"티어{_os['n_pos']+1}",
                "LOC 기준가": f"${_os['next_buy_order']:,.2f}",
                "수량": f"{_os['buy_qty_est']:,}주",
            })
        else:
            st.info(f"⚠️ 모든 슬롯({_os['cur_divisions']}개) 사용 중 — 매수 없음")

        if today_orders:
            st.dataframe(pd.DataFrame(today_orders), use_container_width=True, hide_index=True)

        # ── 보유 현황 ──
        if _os['open_positions']:
            st.divider()
            st.subheader("📦 현재 보유 시드")
            pos_rows = []
            for i, pos in enumerate(_os['open_positions']):
                pnl_pct = (_os['prev_close'] / pos['buy_price'] - 1) * 100 if pos['buy_price'] > 0 else 0
                pos_rows.append({
                    "시드": f"티어{i+1}", "모드": pos['mode'],
                    "매수가": f"${pos['buy_price']:.2f}", "수량": f"{pos['qty']:,}주",
                    "매도목표": f"${pos['sell_target']:.2f}" if pos['sell_target'] else "-",
                    "평가손익": f"{pnl_pct:+.1f}%",
                })
            st.dataframe(pd.DataFrame(pos_rows), use_container_width=True, hide_index=True)

        # ── 히스토리 ──
        st.divider()
        with st.expander("📊 일별 매매 히스토리", expanded=False):
            _hist = _load_dss_history()
            if not _hist.empty:
                st.dataframe(_hist.tail(100), hide_index=True, use_container_width=True,
                             height=min(38 + 35 * 30, 500))
            else:
                st.info("아직 히스토리가 없습니다.")


# ══════════════════════════════════════════════
# 탭 5: DB 조회
# ══════════════════════════════════════════════

def render_db_tab(params=None):
    """DB 조회 탭 — 일별/주별 누적 기록 표시."""
    p = params or {}
    st.markdown("### DB — 일별/주별 누적 기록")
    st.caption("백테스트 결과를 원본 DB 시트 형식으로 표시합니다.")

    if st.button("DB 생성", type="primary", key="dss_run_db"):
        soxl = get_soxl_data()
        qqq = get_qqq_data()
        mode_series_df = get_mode_series(len(qqq))
        weekly_rsi_df = build_weekly_rsi_series(qqq)

        bt_params = _make_params(p)
        s_date = str(p.get("bt_start_date", "2010-01-01"))
        e_date = str(p.get("bt_end_date", datetime.today().strftime("%Y-%m-%d")))
        result = run_backtest(bt_params, soxl, mode_series_df,
                              start_date=s_date, end_date=e_date)

        st.session_state['dss_db_result'] = result
        st.session_state['dss_db_mode_series'] = mode_series_df
        st.session_state['dss_db_weekly_rsi'] = weekly_rsi_df

    if 'dss_db_result' not in st.session_state:
        st.info("🔘 위 버튼을 눌러 DB를 생성하세요.")
        return

    result = st.session_state['dss_db_result']
    mode_series_df = st.session_state['dss_db_mode_series']
    weekly_rsi_df = st.session_state['dss_db_weekly_rsi']

    # ── 최신 데이터 요약 ──
    latest = result.iloc[-1]
    latest_date = latest['날짜']
    latest_mode = latest['모드']

    weekly_in_range = mode_series_df[
        mode_series_df['week_end'] <= latest_date + pd.Timedelta(days=6)
    ]
    latest_week = weekly_in_range.iloc[-1] if len(weekly_in_range) > 0 else None
    latest_rsi = latest_week['rsi'] if latest_week is not None else 0
    week_num = len(weekly_in_range)

    mode_icon = "🔴" if latest_mode == "AG" else "🔵"
    st.markdown(
        f"#### 최신 현황 — {latest_date.strftime('%Y-%m-%d')} | "
        f"{mode_icon} **{latest_mode}** | RSI {latest_rsi:.2f} (W{week_num})"
    )
    lc1, lc2, lc3, lc4, lc5 = st.columns(5)
    lc1.metric("종가", f"${latest['종가']:.2f}")
    lc2.metric("보유포지션", f"{int(latest['보유포지션수'])}/{int(latest['분할수'])}")
    lc3.metric("총자산", f"${latest['총자산']:,.0f}")
    lc4.metric("투자금", f"${latest['투자금']:,.0f}")
    lc5.metric("예수금", f"${latest['예수금']:,.0f}")

    lc6, lc7, lc8 = st.columns(3)
    lc6.metric("누적실현", f"${latest['누적실현']:,.0f}")
    lc7.metric("누적매도", f"{int(latest['누적매도'])}회")
    lc8.metric("1회시드", f"${latest['1회시드']:,.0f}")

    # ── 일별 + 주별 기록 ──
    st.markdown("---")
    db_col1, db_col2 = st.columns([3, 2])

    with db_col1:
        st.markdown("#### 일별 SOXL 기록")
        daily_log = result[['날짜', '종가', '모드', '매수주문가', '매수체결', '수량',
                            '매도목표가', '보유포지션수', '분할수', '1회시드',
                            '투자금', '예수금', '평가금', '총자산',
                            '당일실현', '누적실현', '누적매도']].copy()
        daily_log['날짜'] = daily_log['날짜'].dt.strftime('%y.%m.%d')
        for col in ['종가', '매수주문가', '매수체결', '매도목표가', '1회시드']:
            daily_log[col] = daily_log[col].apply(
                lambda x: f"${x:.2f}" if pd.notna(x) else "")
        for col in ['투자금', '예수금', '평가금', '총자산', '당일실현', '누적실현']:
            daily_log[col] = daily_log[col].apply(lambda x: f"${x:,.0f}" if x != 0 else "")
        daily_log['수량'] = daily_log['수량'].apply(lambda x: str(x) if x > 0 else "")
        daily_log['누적매도'] = daily_log['누적매도'].astype(int)
        st.dataframe(daily_log, use_container_width=True, height=600)
        st.caption(f"총 {len(daily_log)}거래일 기록")

    with db_col2:
        st.markdown("#### 주별 RSI / 모드")
        s_date = str(p.get("bt_start_date", "2010-01-01"))
        e_date = str(p.get("bt_end_date", datetime.today().strftime("%Y-%m-%d")))
        weekly_in_range2 = mode_series_df[
            (mode_series_df['week_end'] >= pd.Timestamp(s_date) - pd.Timedelta(days=7)) &
            (mode_series_df['week_end'] <= pd.Timestamp(e_date) + pd.Timedelta(days=7))
        ].copy()

        weekly_display = pd.DataFrame()
        weekly_display['주차'] = range(1, len(weekly_in_range2) + 1)
        weekly_display['모드'] = weekly_in_range2['mode'].values
        weekly_display['시작'] = (weekly_in_range2['week_end'].values
                                 - pd.Timedelta(days=4)).astype('datetime64[ns]')
        weekly_display['종료'] = weekly_in_range2['week_end'].values
        weekly_display['시작'] = pd.to_datetime(weekly_display['시작']).dt.strftime('%y.%m.%d')
        weekly_display['종료'] = pd.to_datetime(weekly_display['종료']).dt.strftime('%y.%m.%d')
        weekly_display['RSI'] = weekly_in_range2['rsi'].values.round(2)

        def _color_mode(val):
            if val == "AG":
                return 'background-color: #ffe0e0'
            elif val == "SF":
                return 'background-color: #e0e0ff'
            return ''

        styled = weekly_display.style.map(_color_mode, subset=['모드'])
        st.dataframe(styled, use_container_width=True, height=600)

    # ── 모드 가이드 차트 ──
    st.markdown("---")
    st.markdown("#### 모드 가이드 차트")

    fig_rsi = go.Figure()
    for _, row in weekly_in_range2.iterrows():
        color = 'red' if row['mode'] == 'AG' else 'blue'
        fig_rsi.add_trace(go.Scatter(
            x=[row['week_end']], y=[row['rsi']],
            mode='markers', marker=dict(color=color, size=6),
            showlegend=False,
        ))
    fig_rsi.add_trace(go.Scatter(
        x=weekly_in_range2['week_end'], y=weekly_in_range2['rsi'],
        mode='lines', line=dict(color='gray', width=1), name='RSI',
    ))
    thresholds = [
        (35, 'green', 'dash', 'RSI 35'), (40, 'orange', 'dot', 'RSI 40'),
        (50, 'black', 'solid', 'RSI 50'), (60, 'orange', 'dot', 'RSI 60'),
        (65, 'red', 'dash', 'RSI 65'),
    ]
    for level, color, dash, name in thresholds:
        fig_rsi.add_hline(y=level, line=dict(color=color, dash=dash, width=1),
                          annotation_text=name, annotation_position="bottom right")

    prev_mode = None
    span_start = None
    for _, row in weekly_in_range2.iterrows():
        if row['mode'] != prev_mode:
            if prev_mode is not None and span_start is not None:
                fill_color = 'rgba(255,200,200,0.2)' if prev_mode == 'AG' else 'rgba(200,200,255,0.2)'
                fig_rsi.add_vrect(x0=span_start, x1=row['week_end'],
                                  fillcolor=fill_color, line_width=0, layer='below')
            span_start = row['week_end']
            prev_mode = row['mode']
    if prev_mode and span_start is not None:
        fill_color = 'rgba(255,200,200,0.2)' if prev_mode == 'AG' else 'rgba(200,200,255,0.2)'
        fig_rsi.add_vrect(x0=span_start, x1=weekly_in_range2['week_end'].iloc[-1],
                          fillcolor=fill_color, line_width=0, layer='below')
    fig_rsi.update_layout(height=350, yaxis_title="QQQ Weekly RSI", xaxis_title="",
                          margin=dict(t=30, b=30), legend=dict(orientation='h', y=1.05))
    st.plotly_chart(fig_rsi, use_container_width=True)

    # ── Historical RSI ──
    st.markdown("---")
    with st.expander("Historical RSI (전체 기간)", expanded=False):
        hist_rsi = weekly_rsi_df.copy()
        hist_rsi = hist_rsi.merge(mode_series_df[['week_end', 'mode']], on='week_end', how='left')
        hist_display = pd.DataFrame()
        hist_display['주차'] = range(1, len(hist_rsi) + 1)
        hist_display['시작'] = (hist_rsi['week_end'].values - pd.Timedelta(days=4)).astype('datetime64[ns]')
        hist_display['종료'] = hist_rsi['week_end'].values
        hist_display['시작'] = pd.to_datetime(hist_display['시작']).dt.strftime('%y.%m.%d')
        hist_display['종료'] = pd.to_datetime(hist_display['종료']).dt.strftime('%y.%m.%d')
        hist_display['RSI'] = hist_rsi['rsi'].values.round(2)
        hist_display['모드'] = hist_rsi['mode'].values
        st.dataframe(hist_display, use_container_width=True, height=500)
        st.caption(f"총 {len(hist_display)}주 기록 (2010~)")

        fig_hist = go.Figure()
        fig_hist.add_trace(go.Scatter(
            x=hist_rsi['week_end'], y=hist_rsi['rsi'],
            mode='lines', line=dict(color='gray', width=1), name='RSI',
        ))
        for level, color, dash, name in thresholds:
            fig_hist.add_hline(y=level, line=dict(color=color, dash=dash, width=1))
        fig_hist.update_layout(height=300, margin=dict(t=20, b=20), yaxis_title="QQQ Weekly RSI")
        st.plotly_chart(fig_hist, use_container_width=True)

    # ── 매도 내역 누적 기록 ──
    with st.expander("매도 내역 누적 기록"):
        all_sells = []
        for _, row in result.iterrows():
            if row['매도내역']:
                for s in row['매도내역']:
                    all_sells.append(s)
        if all_sells:
            sells_df = pd.DataFrame(all_sells)
            sells_df['buy_date'] = sells_df['buy_date'].dt.strftime('%y.%m.%d')
            sells_df['sell_date'] = sells_df['sell_date'].dt.strftime('%y.%m.%d')
            sells_df['stop_date'] = sells_df['stop_date'].dt.strftime('%y.%m.%d')
            sells_df.columns = ['매수일', '매수가', '수량', '매도일', '매도가',
                                '매도목표', '손절일', '손익', '모드']
            sells_df['손익'] = sells_df['손익'].round(2)
            sells_df.insert(0, '회차', range(1, len(sells_df) + 1))
            st.dataframe(sells_df, use_container_width=True, height=400)
            st.caption(f"총 {len(sells_df)}회 매도 완료")
        else:
            st.info("매도 기록이 없습니다.")


# ══════════════════════════════════════════════
# 탭 4: 소개 & 성과
# ══════════════════════════════════════════════

def render_intro_tab(params=None):
    """전략 소개 & 성과 분석 탭."""
    p = params or {}

    # ── 전략 설명 ──
    st.subheader("📖 DSS 동파법 (동적파도타기법) 이란?")
    st.markdown("""
DSS 동파법은 **SOXL(3배 레버리지 반도체 ETF)**을 대상으로,
**QQQ 주간 RSI**를 기반으로 시장 상황을 판단하여
**안전모드(SF)**와 **공세모드(AG)**를 자동 전환하며 매매하는 전략입니다.

- **LOC 주문**: 장 마감 직전 종가 기준 조건부 주문
- **시드 분할 관리**: 각 시드별 독립적 매수-매도-손절 사이클
- **자동 복리**: 매도 N회마다 실현손익을 투자금에 반영
    """)

    st.subheader("📋 매매 규칙")
    col_sf, col_ag = st.columns(2)
    with col_sf:
        st.markdown("""
**🔵 안전모드 (SF)**
- 매수조건: 전일종가 × (1 + 매수%) 이하
- 매도조건: 매수가 × (1 + 매도%)
- 손절: 보유기간 초과 시 강제 매도
        """)
    with col_ag:
        st.markdown("""
**🟢 공세모드 (AG)**
- 매수조건: 전일종가 × (1 + 매수%) 이하
- 매도조건: 매수가 × (1 + 매도%)
- 손절: 보유기간 초과 시 강제 매도
        """)

    # ── 성과 분석 실행 ──
    st.divider()
    st.subheader("📊 전략 성과 분석")

    if st.button("▶ 성과 분석 실행", type="primary", key="dss_intro_perf"):
        with st.spinner("데이터 로드 및 분석 중..."):
            soxl = get_soxl_data()
            qqq = get_qqq_data()
            ms = get_mode_series(len(qqq))
            dss_p = _make_params(p)
            bt = run_backtest(dss_p, soxl, ms, str(p["start_date"]), str(p["end_date"]))
        if bt is not None and not bt.empty:
            st.session_state["dss_intro_bt"] = bt

    bt = st.session_state.get("dss_intro_bt")
    if bt is None or (isinstance(bt, pd.DataFrame) and bt.empty):
        st.info("👆 '성과 분석 실행' 버튼을 클릭하면 성과 분석 결과를 확인할 수 있습니다.")
        return

    _init = float(p.get("initial_capital", 10000))
    _assets = bt['총자산'].values.astype(float)
    _final = float(_assets[-1])
    _days = len(bt)
    _years = _days / 252
    _total_ret = (_final / _init - 1)
    _cagr = (_final / _init) ** (1 / _years) - 1 if _years > 0 else 0
    _peak = np.maximum.accumulate(_assets)
    _dd = (_assets - _peak) / _peak
    _mdd = float(_dd.min())
    _calmar = abs(_cagr / _mdd) if _mdd != 0 else 0
    _daily_rets = np.diff(_assets) / _assets[:-1]
    _sharpe = (np.mean(_daily_rets) / np.std(_daily_rets) * np.sqrt(252)) if np.std(_daily_rets) > 0 else 0
    _neg = _daily_rets[_daily_rets < 0]
    _sortino = (np.mean(_daily_rets) / np.std(_neg) * np.sqrt(252)) if len(_neg) > 0 and np.std(_neg) > 0 else 0

    # 핵심 지표 카드
    st.markdown(f"""
    <div style="display:flex;gap:10px;margin:12px 0">
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:0.72em;color:#888">CAGR</div>
        <div style="font-size:1.15em;font-weight:700">{_cagr*100:.2f}%</div>
      </div>
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:0.72em;color:#888">총 수익률</div>
        <div style="font-size:1.15em;font-weight:700;color:{'#2E7D32' if _total_ret>=0 else '#C62828'}">{_total_ret*100:+,.1f}%</div>
      </div>
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:0.72em;color:#888">MDD</div>
        <div style="font-size:1.15em;font-weight:700;color:#C62828">{_mdd*100:.2f}%</div>
      </div>
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:0.72em;color:#888">Calmar</div>
        <div style="font-size:1.15em;font-weight:700">{_calmar:.3f}</div>
      </div>
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:0.72em;color:#888">Sharpe</div>
        <div style="font-size:1.15em;font-weight:700">{_sharpe:.3f}</div>
      </div>
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:0.72em;color:#888">Sortino</div>
        <div style="font-size:1.15em;font-weight:700">{_sortino:.3f}</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    _dates = pd.to_datetime(bt['날짜'])

    # ── 연도별 성과 ──
    st.divider()
    st.subheader("📅 연도별 성과")
    _bt_y = bt.copy()
    _bt_y['날짜'] = pd.to_datetime(_bt_y['날짜'])
    _bt_y['연도'] = _bt_y['날짜'].dt.year
    yr_rows = []
    for year, grp in _bt_y.groupby('연도'):
        sv = float(grp.iloc[0]['총자산']); ev = float(grp.iloc[-1]['총자산'])
        yr_ret = (ev / sv - 1) * 100
        pk = grp['총자산'].cummax(); dd = (grp['총자산'] - pk) / pk
        yr_rows.append({"연도": int(year), "연간수익률(%)": yr_ret, "MDD(%)": float(dd.min()) * 100})
    df_yr = pd.DataFrame(yr_rows)
    def _clr(val):
        if isinstance(val, (int, float)):
            if val > 0: return "color:#2E7D32;font-weight:bold"
            if val < 0: return "color:#C62828;font-weight:bold"
        return ""
    st.dataframe(df_yr.style.map(_clr, subset=["연간수익률(%)"]).format(
        {"연간수익률(%)": "{:+.2f}%", "MDD(%)": "{:.2f}%"}),
        hide_index=True, use_container_width=True)

    # ── 월별 히트맵 ──
    st.divider()
    st.subheader("🗓️ 월별 수익률 히트맵")
    _bt_m = bt.copy()
    _bt_m['날짜'] = pd.to_datetime(_bt_m['날짜'])
    _bt_m['연도'] = _bt_m['날짜'].dt.year; _bt_m['월'] = _bt_m['날짜'].dt.month
    mon_rows = []
    for (y, m), grp in _bt_m.groupby(['연도', '월']):
        sv = float(grp.iloc[0]['총자산']); ev = float(grp.iloc[-1]['총자산'])
        mon_rows.append({"연도": int(y), "월": int(m), "수익률(%)": round((ev/sv-1)*100, 2)})
    df_mon = pd.DataFrame(mon_rows)
    if not df_mon.empty:
        _mp = df_mon.pivot(index="연도", columns="월", values="수익률(%)")
        _mp.columns = [f"{m}월" for m in _mp.columns]
        fig_h = px.imshow(_mp, color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
                          text_auto=".1f", aspect="auto")
        fig_h.update_layout(height=max(320, len(_mp) * 38 + 120))
        st.plotly_chart(fig_h, use_container_width=True)

    # ── 총자산 추이 & 낙폭 ──
    st.divider()
    st.subheader("📈 총자산 추이 & 낙폭")
    fig_a = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05)
    fig_a.add_trace(go.Scatter(x=_dates, y=_assets, mode='lines', name='총자산',
                               line=dict(color='#1565C0', width=1.5)), row=1, col=1)
    fig_a.add_trace(go.Scatter(x=_dates, y=_dd*100, mode='lines', name='낙폭(%)',
                               fill='tozeroy', fillcolor='rgba(198,40,40,0.15)',
                               line=dict(color='#C62828', width=1)), row=2, col=1)
    fig_a.update_layout(height=500, showlegend=True, legend=dict(orientation='h', y=1.02))
    st.plotly_chart(fig_a, use_container_width=True)

    # ── B&H 비교 ──
    st.divider()
    st.subheader("📉 DSS vs Buy & Hold")
    _bh_p0 = float(bt.iloc[0]['종가'])
    _bh_prices = bt['종가'].values.astype(float)
    _bh_sh = int(_init / _bh_p0)
    _bh_lo = _init - _bh_sh * _bh_p0
    _bh_a = _bh_sh * _bh_prices + _bh_lo
    _bh_ret = (_bh_a[-1] / _init - 1) * 100

    fig_bh = go.Figure()
    fig_bh.add_trace(go.Scatter(x=_dates, y=_assets, name="DSS 동파법", line=dict(color='#1565C0', width=2)))
    fig_bh.add_trace(go.Scatter(x=_dates, y=_bh_a, name="Buy & Hold", line=dict(color='#EF5350', width=2, dash='dot')))
    fig_bh.update_layout(title=f"DSS {_total_ret*100:+,.1f}% vs B&H {_bh_ret:+,.1f}%",
                         yaxis_title="자산 ($)", height=400)
    st.plotly_chart(fig_bh, use_container_width=True)

    # ── 종합 요약 ──
    st.divider()
    st.subheader("📋 종합 성과 요약")
    _sell_recs = []
    for _, row in bt.iterrows():
        if row['매도내역'] is not None:
            _sell_recs.extend(row['매도내역'])
    _ts = len(_sell_recs)
    _tw = len([s for s in _sell_recs if s['pnl'] >= 0])
    st.dataframe(pd.DataFrame({
        "항목": ["시작 자본", "최종 자산", "총 수익률", "CAGR", "MDD", "Calmar", "Sharpe", "Sortino",
                 "총 매도", "승률"],
        "수치": [f"${_init:,.0f}", f"${_final:,.0f}", f"{_total_ret*100:+.2f}%",
                 f"{_cagr*100:.2f}%", f"{_mdd*100:.2f}%", f"{_calmar:.3f}",
                 f"{_sharpe:.3f}", f"{_sortino:.3f}", f"{_ts}회",
                 f"{_tw/_ts*100:.1f}% ({_tw}승 {_ts-_tw}패)" if _ts > 0 else "-"],
    }), hide_index=True, use_container_width=True)


# ══════════════════════════════════════════════
# 탭 5 (or 6): 설정
# ══════════════════════════════════════════════

def render_settings_tab():
    """개인 설정 탭."""
    st.subheader("⚙️ DSS 동파법 개인 설정")

    _cfg = _load_dss_config()
    st.success(f"🖥️ 설정 파일: `{_DSS_CONFIG_PATH}`")

    # ── 텔레그램 ──
    with st.container(border=True):
        st.markdown("#### 💬 텔레그램 알림 설정")
        st.caption("DSS 동파법 주문표를 텔레그램으로 받을 수 있습니다.")

        c1, c2 = st.columns(2)
        tg_cid = c1.text_input("Chat ID", value=_cfg.get("tg_chat_id", ""),
                               placeholder="예: 1234567890", key="dss_s_tg_cid")
        tg_tok = c2.text_input("Bot Token", value=_cfg.get("tg_token", ""),
                               placeholder="예: 123456789:AAF...", type="password", key="dss_s_tg_tok")

        b1, b2, _ = st.columns([1, 1, 4])
        with b1:
            if st.button("📨 테스트 발송", use_container_width=True, key="dss_s_tg_test"):
                if not tg_cid or not tg_tok:
                    st.warning("Chat ID와 Bot Token을 입력해주세요.")
                else:
                    _tg_os = st.session_state.get("dss_os_result")
                    if _tg_os is None:
                        st.warning("주문표 탭에서 먼저 '주문표 로드'를 실행해주세요.")
                    else:
                        _o = _tg_os
                        mode_icon = "🟢" if _o["last_mode"] == "AG" else "🔵"
                        lines = [
                            f"<b>📋 DSS 동파법 — SOXL 주문표</b>",
                            f"📅 {next_trading_date().strftime('%Y-%m-%d')}  {mode_icon} {'공세' if _o['last_mode']=='AG' else '안전'}모드",
                            f"", f"전일종가: <b>${_o['prev_close']:,.2f}</b>",
                            f"총자산: <b>${_o['total_asset']:,.0f}</b>  (현금 ${_o['cash']:,.0f})",
                            f"보유: {_o['n_pos']}/{_o['cur_divisions']}시드", f"",
                        ]
                        for i, pos in enumerate(_o['open_positions']):
                            if pos['sell_target']:
                                lines.append(f"📈 매도 티어{i+1}: LOC ${pos['sell_target']:,.2f} × {pos['qty']}주")
                        if _o['n_pos'] < _o['cur_divisions']:
                            lines.append(f"📉 매수 티어{_o['n_pos']+1}: LOC ${_o['next_buy_order']:,.2f} × {_o['buy_qty_est']}주")
                        result = _send_telegram(tg_tok, tg_cid, "\n".join(lines))
                        if result.get("ok"):
                            st.success("✅ 발송 성공!")
                        else:
                            st.error(f"❌ 실패: {result.get('description')}")
        with b2:
            if st.button("💾 저장", use_container_width=True, key="dss_s_tg_save", type="primary"):
                _cfg["tg_chat_id"] = tg_cid
                _cfg["tg_token"] = tg_tok
                _save_dss_config(_cfg)
                st.success("✅ 저장 완료!")

    st.write("")

    # ── 파라미터 관리 ──
    with st.container(border=True):
        st.markdown("#### 📋 저장된 파라미터")
        _saved = _cfg.get("params", {})
        if _saved:
            st.json(_saved)
        else:
            st.info("주문표 탭에서 파라미터를 저장하면 여기에 표시됩니다.")
