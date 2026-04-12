"""
app.py — 전략 백테스터 라우터 (thin entry point)

모든 전략 로직은 strategies/ 패키지에, 공통 유틸은 common/ 패키지에 위치.
이 파일은 페이지 설정, 로그인, 사이드바 공통 UI, 탭 라우팅만 담당한다.
"""

import streamlit as st
from datetime import datetime
import sys

# ── 페이지 설정 (반드시 첫 Streamlit 호출) ───────────────────
st.set_page_config(page_title="📊 전략 백테스터", layout="wide")

# ── 디버그: 앱 시작 확인 ─────────────────────────────────────
print(f"[DEBUG] app.py started, Python {sys.version}", flush=True)

# ── 공통 모듈 import ─────────────────────────────────────────
from common.config import _IS_CLOUD, _CONFIG, load_config, get_ticker_settings
print("[DEBUG] common.config loaded", flush=True)
from common.auth import render_login_gate, _cookie_mgr
print("[DEBUG] common.auth loaded", flush=True)

# ── 클라우드: 로그인 게이트 ──────────────────────────────────
if _IS_CLOUD:
    print("[DEBUG] rendering login gate...", flush=True)
    render_login_gate()
    print("[DEBUG] login gate done", flush=True)

# ── 전략 모듈 import ─────────────────────────────────────────
try:
    print("[DEBUG] importing strategies...", flush=True)
    from strategies import avg_close, stdev, sigma
    print("[DEBUG] strategies loaded OK", flush=True)
except Exception as _import_err:
    st.error(f"⚠️ 전략 모듈 로드 실패: {_import_err}")
    import traceback
    st.code(traceback.format_exc())
    st.stop()

# ── 전략 목록 ────────────────────────────────────────────────
_STRATEGIES = ["📐 표준편차매매", "📈 종가평균매매", "📐 Sigma매매", "🌊 DSS 동파법"]

# ── 전략 판별 ────────────────────────────────────────────────
_strategy_title = st.session_state.get("strategy_radio", "📐 표준편차매매")
_is_stdev = (_strategy_title == "📐 표준편차매매")
_is_sigma = (_strategy_title == "📐 Sigma매매")
_is_dss = (_strategy_title == "🌊 DSS 동파법")

# ── DSS lazy import (선택 시에만 로드) ───────────────────────
dss = None
if _is_dss:
    try:
        from strategies import dss
    except Exception as _dss_err:
        st.error(f"⚠️ DSS 모듈 로드 실패: {_dss_err}")
        import traceback
        st.code(traceback.format_exc())

# ── 타이틀 ───────────────────────────────────────────────────
if _is_dss:
    st.title("🌊 DSS 동파법 백테스터")
elif _is_stdev:
    st.title("📐 표준편차매매")
elif _is_sigma:
    st.title("📐 Sigma매매법 백테스터")
else:
    st.title("📈 종가평균매매")


# ══════════════════════════════════════════════════════════════
# 사이드바
# ══════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("📊 전략 백테스터")

    # ── 전략 선택 드롭다운 ────────────────────────
    st.selectbox("전략 선택", _STRATEGIES, key="strategy_radio")
    st.markdown("---")

    # ── 공통 종목 선택 (Sigma·DSS 제외 — 자체 사이드바 사용) ──
    if not _is_sigma and not _is_dss:
        st.subheader("📌 종목")
        _PRESET_TICKERS = ["SOXL", "USD", "TQQQ", "직접입력"]
        _ticker_select = st.selectbox("종목코드 (Ticker)", _PRESET_TICKERS, index=0)
        if _ticker_select == "직접입력":
            ticker = st.text_input("티커 직접 입력", placeholder="예: NVDA, SPY, QQQ, TSLA").strip().upper()
            if not ticker:
                st.warning("티커를 입력해주세요.")
        else:
            ticker = _ticker_select

        st.markdown("---")
        st.subheader("전략 파라미터")

        # ── 저장된 파라미터 기본값 로드 ──
        def _sfloat(v, d):
            try: return float(v) if v not in ("", None) else d
            except: return d
        def _sint(v, d):
            try: return int(float(v)) if v not in ("", None) else d
            except: return d

        _cfg_sb = load_config(ticker)
        if _IS_CLOUD and st.session_state.get("logged_in"):
            _all_tk_cfg = get_ticker_settings()
            _usercfg_sb = _all_tk_cfg.get(ticker, st.session_state.get("user_settings", {}))
        else:
            _usercfg_sb = _cfg_sb

        # ── 전략별 파라미터 렌더링 ────────────────
        if _is_stdev:
            params = stdev.render_sidebar(_usercfg_sb, _sfloat, _sint)
        else:
            params = avg_close.render_sidebar(_usercfg_sb, _sfloat, _sint)

        st.caption("📌 파라미터 저장은 오늘의 주문표 탭에서 할 수 있습니다.")

        # ── 백테스트 설정 (공통) ──────────────────
        st.markdown("---")
        st.subheader("백테스트 설정")
        col1, col2 = st.columns(2)
        start_date = col1.date_input(
            "시작 일",
            datetime(2010, 3, 15).date() if _is_stdev else datetime(2014, 1, 1).date(),
        )
        end_date = col2.date_input("종료 일", datetime.today().date())
        initial_capital = st.number_input(
            "초기 투자금 ($)",
            value=20000.0 if _is_stdev else 10000.0,
            step=1000.0,
        )
        divisions = params.get("divisions", 5)
        st.info(f"1회 분할 금액: ${initial_capital / divisions:,.2f}")

        # ── 데이터 소스 선택 ─────────────────────
        st.markdown("---")
        data_source = st.radio(
            "📂 종가 데이터 소스",
            ["야후 파이낸스 (yfinance)", "엑셀 Daily_Close 시트"],
            index=0,
        )
        excel_file = None
        if data_source == "엑셀 Daily_Close 시트":
            excel_file = st.file_uploader("엑셀 파일 업로드 (.xlsx)", type=["xlsx"])
            st.caption("엑셀 내 **Daily_Close** 시트의 날짜/종가 두 컬럼이 사용됩니다.")

    elif _is_dss and dss:
        # DSS: 자체 사이드바 (SOXL 전용, QQQ RSI 모드 전환)
        params = dss.render_sidebar()
        ticker = params.get("bt_ticker", "SOXL")
        start_date = params.get("bt_start_date")
        end_date = params.get("bt_end_date")
        initial_capital = params.get("bt_initial_capital", 100000.0)
        data_source = "야후 파이낸스 (yfinance)"
        excel_file = None

    elif _is_dss and not dss:
        # DSS 로드 실패 시 최소 변수 설정
        st.warning("DSS 모듈 로드 실패. 위 에러를 확인하세요.")
        params = {}
        ticker = "SOXL"
        start_date = datetime(2010, 1, 1).date()
        end_date = datetime.today().date()
        initial_capital = 100000.0
        data_source = "야후 파이낸스 (yfinance)"
        excel_file = None

    else:
        # Sigma: 자체 사이드바 (종목, 날짜, 파라미터 포함)
        params = sigma.render_sidebar()
        ticker = params.get("bt_ticker", "SOXL")
        start_date = params.get("bt_start_date")
        end_date = params.get("bt_end_date")
        initial_capital = params.get("bt_initial_capital", 100000.0)
        data_source = "야후 파이낸스 (yfinance)"
        excel_file = None

    # ── 사용자 정보 & 로그아웃 (클라우드 로그인 시) ──
    if _IS_CLOUD and st.session_state.get("logged_in"):
        st.markdown("---")
        st.caption(f"👤 **{st.session_state.username}** 으로 로그인 중")
        if st.button("🚪 로그아웃", use_container_width=True):
            try:
                _cookie_mgr.remove("usd_avg_user")
            except Exception:
                pass
            for k in ("logged_in", "username", "user_settings"):
                st.session_state.pop(k, None)
            st.rerun()


# ══════════════════════════════════════════════════════════════
# 탭 구성 & 라우팅
# ══════════════════════════════════════════════════════════════
if _is_dss and dss:
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 백테스트", "🔍 파라미터 최적화", "📋 오늘의 주문표",
        "📖 전략 소개 & 성과", "📂 DB 조회", "⚙️ 개인 설정",
    ])
elif _is_dss and not dss:
    st.warning("⚠️ DSS 모듈을 로드하지 못했습니다.")
    st.stop()
elif _is_sigma:
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 백테스트", "📈 매도 전략 가이드", "📋 주문표 & 계좌관리",
        "📖 전략 소개", "⚙️ 개인설정",
    ])
elif _is_stdev:
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 백테스트", "🔍 파라미터 최적화", "📋 오늘의 주문표",
        "📖 전략 소개 & 성과", "⚙️ 개인 설정",
    ])
else:
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 백테스트", "🔍 파라미터 최적화", "📋 오늘의 주문표",
        "📖 전략 소개 & 성과", "⚙️ 개인 설정",
    ])


with tab1:
    if _is_dss and dss:
        dss.render_backtest_tab(params)
    elif _is_sigma:
        sigma.render_backtest_tab(params)
    elif _is_stdev:
        stdev.render_backtest_tab(ticker, params, data_source, excel_file,
                                  start_date, end_date, initial_capital)
    else:
        avg_close.render_backtest_tab(ticker, params, data_source, excel_file,
                                      start_date, end_date, initial_capital)

with tab2:
    if _is_dss and dss:
        dss.render_optimization_tab(params)
    elif _is_sigma:
        sigma.render_optimization_tab(params)
    elif _is_stdev:
        stdev.render_optimization_tab(ticker, params, start_date, end_date,
                                      initial_capital, data_source, excel_file)
    else:
        avg_close.render_optimization_tab(ticker, params, start_date, end_date,
                                          initial_capital, data_source, excel_file)

with tab3:
    if _is_dss and dss:
        dss.render_ordersheet_tab(params)
    elif _is_sigma:
        sigma.render_ordersheet_tab(params)
    elif _is_stdev:
        stdev.render_ordersheet_tab(ticker, params, initial_capital,
                                    data_source, excel_file)
    else:
        avg_close.render_ordersheet_tab(ticker, params, initial_capital,
                                        data_source, excel_file)

with tab4:
    if _is_dss and dss:
        dss.render_intro_tab(params)
    elif _is_sigma:
        sigma.render_intro_tab()
    elif _is_stdev:
        stdev.render_intro_tab(ticker, params, data_source, excel_file,
                               start_date, end_date, initial_capital)
    else:
        avg_close.render_intro_tab(ticker, params, data_source, excel_file,
                                   start_date, end_date, initial_capital)

with tab5:
    if _is_dss and dss:
        dss.render_db_tab(params)
    elif _is_sigma:
        sigma.render_settings_tab()
    elif _is_stdev:
        stdev.render_settings_tab()
    else:
        avg_close.render_settings_tab()

# DSS 6번째 탭 (설정)
if _is_dss and dss:
    with tab6:
        dss.render_settings_tab()
