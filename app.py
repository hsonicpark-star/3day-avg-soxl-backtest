"""
app.py — 전략 백테스터 라우터 (thin entry point)

모든 전략 로직은 strategies/ 패키지에, 공통 유틸은 common/ 패키지에 위치.
이 파일은 페이지 설정, 로그인, 사이드바 공통 UI, 탭 라우팅만 담당한다.
"""

import streamlit as st
from datetime import datetime, date, timedelta

# ── 페이지 설정 (반드시 첫 Streamlit 호출) ───────────────────
st.set_page_config(page_title="📊 전략 백테스터", layout="wide")

# ── date_input 선택 범위 전역 확장 ───────────────────────────
# Streamlit 기본값은 value ± 10년으로 제한됨 → 시작일 기본이 2014년이면
# 달력에서 2024년까지만 선택 가능한 문제 발생.
# min/max 미지정 호출에 전역 기본치(2000-01-01 ~ 2035-12-31)를 적용.
# 전략 모듈 전체(37곳)에 일괄 적용 — 명시적으로 min/max를 넘기면 그 값 우선.
# NOTE: 2035년이 가까워지면 상한을 다시 연장할 것.
# 두 단계 패치 필수:
#  1) DeltaGenerator 클래스 메서드 — col1.date_input(...), st.sidebar.date_input(...)
#     등 컨테이너 객체 경유 호출 (앱 대부분의 호출이 이 형태)
#  2) st.date_input 모듈 어트리뷰트 — import 시점에 이미 바인딩된 직접 호출
from streamlit.delta_generator import DeltaGenerator as _DG

if not getattr(_DG.date_input, "_wide_range_patched", False):
    _orig_dg_date_input = _DG.date_input

    def _dg_date_input_wide(self, *args, **kwargs):
        kwargs.setdefault("min_value", date(2000, 1, 1))
        kwargs.setdefault("max_value", date(2035, 12, 31))
        return _orig_dg_date_input(self, *args, **kwargs)

    _dg_date_input_wide._wide_range_patched = True
    _DG.date_input = _dg_date_input_wide

if not getattr(st.date_input, "_wide_range_patched", False):
    _orig_date_input = st.date_input

    def _date_input_wide(*args, **kwargs):
        kwargs.setdefault("min_value", date(2000, 1, 1))
        kwargs.setdefault("max_value", date(2035, 12, 31))
        return _orig_date_input(*args, **kwargs)

    _date_input_wide._wide_range_patched = True
    st.date_input = _date_input_wide

# ── 공통 모듈 import ─────────────────────────────────────────
from common.config import _IS_CLOUD, _CONFIG, load_config, get_ticker_settings
from common.auth import render_login_gate, _cookie_mgr

# ── 클라우드: 로그인 게이트 ──────────────────────────────────
if _IS_CLOUD:
    render_login_gate()

# ── 전략 모듈 import ─────────────────────────────────────────
try:
    from strategies import avg_close, stdev, sigma
except Exception as _import_err:
    st.error(f"⚠️ 전략 모듈 로드 실패: {_import_err}")
    import traceback
    st.code(traceback.format_exc())
    st.stop()

# ── 전략 목록 ────────────────────────────────────────────────
_STRATEGIES = ["📐 표준편차매매", "📈 종가평균매매", "📐 Sigma매매", "🌊 DSS 동파법", "📊 IUO 매매법", "🎯 듀얼스나이퍼", "🎲 카마릴라 돌파", "🪜 평단법", "🧩 포트폴리오 합산"]

# ── 전략 판별 ────────────────────────────────────────────────
_strategy_title = st.session_state.get("strategy_radio", "📐 표준편차매매")
_is_stdev = (_strategy_title == "📐 표준편차매매")
_is_sigma = (_strategy_title == "📐 Sigma매매")
_is_dss = (_strategy_title == "🌊 DSS 동파법")
_is_iuo = (_strategy_title == "📊 IUO 매매법")
_is_dual = (_strategy_title == "🎯 듀얼스나이퍼")
_is_cam = (_strategy_title == "🎲 카마릴라 돌파")
_is_pdan = (_strategy_title == "🪜 평단법")
_is_portfolio = (_strategy_title == "🧩 포트폴리오 합산")

# ── 포트폴리오 합산 lazy import (선택 시에만 로드) ─────────────
portfolio = None
if _is_portfolio:
    try:
        import importlib
        from strategies import portfolio
        importlib.reload(portfolio)
    except Exception as _pf_err:
        st.error(f"⚠️ 포트폴리오 모듈 로드 실패: {_pf_err}")
        import traceback
        st.code(traceback.format_exc())

# ── DSS lazy import (선택 시에만 로드) ───────────────────────
dss = None
if _is_dss:
    try:
        from strategies import dss
        import importlib
        importlib.reload(dss)
    except Exception as _dss_err:
        st.error(f"⚠️ DSS 모듈 로드 실패: {_dss_err}")
        import traceback
        st.code(traceback.format_exc())

# ── IUO lazy import (선택 시에만 로드) ───────────────────────
iuo = None
if _is_iuo:
    try:
        from strategies import iuo
        import importlib
        importlib.reload(iuo)
    except Exception as _iuo_err:
        st.error(f"⚠️ IUO 모듈 로드 실패: {_iuo_err}")
        import traceback
        st.code(traceback.format_exc())

# ── 카마릴라 lazy import (선택 시에만 로드) ───────────────────
cam = None
if _is_cam:
    try:
        from strategies import camarilla as cam
        import importlib
        importlib.reload(cam)
    except Exception as _cam_err:
        st.error(f"⚠️ 카마릴라 모듈 로드 실패: {_cam_err}")
        import traceback
        st.code(traceback.format_exc())

# ── 평단법 lazy import (선택 시에만 로드) ─────────────────────
pdan = None
if _is_pdan:
    try:
        import importlib
        import pdan_engine as _pd_engine
        importlib.reload(_pd_engine)
        from strategies import pdan
        importlib.reload(pdan)
    except Exception as _pdan_err:
        st.error(f"⚠️ 평단법 모듈 로드 실패: {_pdan_err}")
        import traceback
        st.code(traceback.format_exc())

# ── 듀얼스나이퍼 lazy import (선택 시에만 로드) ───────────────
dual = None
if _is_dual:
    try:
        import importlib
        # 엔진 모듈 먼저 reload (build_today_orders 등 신규 심볼 stale 캐시 방지)
        import dual_sniper_engine as _ds_engine
        importlib.reload(_ds_engine)
        from strategies import dual_sniper as dual
        importlib.reload(dual)
    except Exception as _dual_err:
        st.error(f"⚠️ 듀얼스나이퍼 모듈 로드 실패: {_dual_err}")
        import traceback
        st.code(traceback.format_exc())

# ── 타이틀 ───────────────────────────────────────────────────
if _is_pdan:
    st.title("🪜 평단법 백테스터")
elif _is_dual:
    st.title("🎯 Dual Sniper Pro 백테스터")
elif _is_cam:
    st.title("🎲 카마릴라 피봇 돌파 백테스터")
elif _is_iuo:
    st.title("📊 IUO 매매법 백테스터")
elif _is_dss:
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

    # ── 공통 종목 선택 (Sigma·DSS·IUO·듀얼·카마릴라·평단법·포트폴리오 제외 — 자체 사이드바 사용) ──
    if not _is_sigma and not _is_dss and not _is_iuo and not _is_dual and not _is_cam and not _is_pdan and not _is_portfolio:
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
            datetime(2014, 1, 2).date(),
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

    elif _is_pdan and pdan:
        # 평단법: 자체 사이드바 (티어 사다리 + 매도방식)
        params = pdan.render_sidebar()
        ticker = params.get("bt_ticker", "SOXL")
        start_date = params.get("bt_start_date")
        end_date = params.get("bt_end_date")
        initial_capital = params.get("bt_initial_capital", 30000.0)
        data_source = "야후 파이낸스 (yfinance)"
        excel_file = None

    elif _is_pdan and not pdan:
        st.warning("평단법 모듈 로드 실패. 위 에러를 확인하세요.")
        params = {}; ticker = "SOXL"
        start_date = datetime(2021, 1, 4).date(); end_date = datetime.today().date()
        initial_capital = 30000.0; data_source = "야후 파이낸스 (yfinance)"; excel_file = None

    elif _is_dss and dss:
        # DSS: 자체 사이드바 (SOXL 전용, QQQ RSI 모드 전환)
        params = dss.render_sidebar()
        ticker = params.get("bt_ticker", "SOXL")
        start_date = params.get("bt_start_date")
        end_date = params.get("bt_end_date")
        initial_capital = params.get("bt_initial_capital", 100000.0)
        data_source = "야후 파이낸스 (yfinance)"
        excel_file = None

    elif _is_cam and cam:
        # 카마릴라: 자체 사이드바 (돌파 + 변동성 타게팅)
        params = cam.render_sidebar()
        ticker = params.get("bt_ticker", "SOXL")
        start_date = params.get("bt_start_date")
        end_date = params.get("bt_end_date")
        initial_capital = params.get("bt_initial_capital", 100000.0)
        data_source = "야후 파이낸스 (yfinance)"
        excel_file = None

    elif _is_cam and not cam:
        st.warning("카마릴라 모듈 로드 실패. 위 에러를 확인하세요.")
        params = {}; ticker = "SOXL"
        start_date = datetime(2010, 3, 12).date(); end_date = datetime.today().date()
        initial_capital = 100000.0; data_source = "야후 파이낸스 (yfinance)"; excel_file = None

    elif _is_iuo and iuo:
        # IUO: 자체 사이드바 (SOXL 전용, QQQ 추세 옵션)
        params = iuo.render_sidebar()
        ticker = params.get("bt_ticker", "SOXL")
        start_date = params.get("bt_start_date")
        end_date = params.get("bt_end_date")
        initial_capital = params.get("bt_initial_capital", 10000.0)
        data_source = "야후 파이낸스 (yfinance)"
        excel_file = None

    elif _is_iuo and not iuo:
        # IUO 로드 실패 시 최소 변수 설정
        st.warning("IUO 모듈 로드 실패. 위 에러를 확인하세요.")
        params = {}
        ticker = "SOXL"
        start_date = datetime(2015, 12, 31).date()
        end_date = datetime.today().date()
        initial_capital = 10000.0
        data_source = "야후 파이낸스 (yfinance)"
        excel_file = None

    elif _is_dual and dual:
        # 듀얼스나이퍼: 자체 사이드바 (SOXL 전용, 하이브리드 모드)
        params = dual.render_sidebar()
        ticker = params.get("bt_ticker", "SOXL")
        start_date = params.get("bt_start_date")
        end_date = params.get("bt_end_date")
        initial_capital = params.get("bt_initial_capital", 10000.0)
        data_source = "야후 파이낸스 (yfinance)"
        excel_file = None

    elif _is_dual and not dual:
        st.warning("듀얼스나이퍼 모듈 로드 실패. 위 에러를 확인하세요.")
        params = {}
        ticker = "SOXL"
        start_date = datetime(2016, 1, 4).date()
        end_date = datetime.today().date()
        initial_capital = 10000.0
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

    elif _is_portfolio:
        # 포트폴리오 합산: 사이드바/본문 모두 portfolio.render() 가 자체 처리
        params = {}
        ticker = "SOXL"
        start_date = datetime(2016, 1, 4).date()
        end_date = datetime.today().date()
        initial_capital = 0.0
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
# 포트폴리오 합산 — 전용 페이지 (표준 탭 레이아웃 우회)
# ══════════════════════════════════════════════════════════════
if _is_portfolio:
    if portfolio:
        portfolio.render()
    else:
        st.warning("⚠️ 포트폴리오 모듈을 로드하지 못했습니다.")
    st.stop()


# ══════════════════════════════════════════════════════════════
# 탭 구성 & 라우팅
# ══════════════════════════════════════════════════════════════
if _is_pdan and pdan:
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 백테스트", "🔍 파라미터 최적화", "📋 주문표 & 계좌관리",
        "📖 전략 소개", "⚙️ 개인 설정",
    ])
elif _is_pdan and not pdan:
    st.warning("⚠️ 평단법 모듈을 로드하지 못했습니다.")
    st.stop()
elif _is_dual and dual:
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 백테스트", "🔍 모드 규칙 최적화", "📋 오늘의 주문표",
        "📖 전략 소개", "📂 DB 조회", "⚙️ 개인 설정",
    ])
elif _is_dual and not dual:
    st.warning("⚠️ 듀얼스나이퍼 모듈을 로드하지 못했습니다.")
    st.stop()
elif _is_iuo and iuo:
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 백테스트", "🔍 파라미터 최적화", "📋 주문표 & 계좌관리",
        "📖 전략 소개 & 성과", "📂 DB 조회", "⚙️ 개인 설정",
    ])
elif _is_iuo and not iuo:
    st.warning("⚠️ IUO 모듈을 로드하지 못했습니다.")
    st.stop()
elif _is_dss and dss:
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 백테스트", "🔍 파라미터 최적화", "📋 오늘의 주문표",
        "📖 전략 소개 & 성과", "📂 DB 조회", "⚙️ 개인 설정",
    ])
elif _is_dss and not dss:
    st.warning("⚠️ DSS 모듈을 로드하지 못했습니다.")
    st.stop()
elif _is_cam and cam:
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 백테스트", "🔍 파라미터 최적화", "📋 오늘의 주문표",
        "📖 전략 소개 & 성과", "📂 DB 조회", "⚙️ 개인 설정",
    ])
elif _is_cam and not cam:
    st.warning("⚠️ 카마릴라 모듈을 로드하지 못했습니다.")
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
    if _is_pdan and pdan:
        pdan.render_backtest_tab(params)
    elif _is_dual and dual:
        dual.render_backtest_tab(params)
    elif _is_cam and cam:
        cam.render_backtest_tab(params)
    elif _is_iuo and iuo:
        iuo.render_backtest_tab(params)
    elif _is_dss and dss:
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
    if _is_pdan and pdan:
        pdan.render_optimization_tab(params)
    elif _is_dual and dual:
        dual.render_optimization_tab(params)
    elif _is_cam and cam:
        cam.render_optimization_tab(params)
    elif _is_iuo and iuo:
        iuo.render_optimization_tab(params)
    elif _is_dss and dss:
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
    if _is_pdan and pdan:
        pdan.render_ordersheet_tab(params)
    elif _is_dual and dual:
        dual.render_ordersheet_tab(params)
    elif _is_cam and cam:
        cam.render_ordersheet_tab(params)
    elif _is_iuo and iuo:
        iuo.render_ordersheet_tab(params)
    elif _is_dss and dss:
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
    if _is_pdan and pdan:
        pdan.render_intro_tab(params)
    elif _is_dual and dual:
        dual.render_intro_tab(params)
    elif _is_cam and cam:
        cam.render_intro_tab(params)
    elif _is_iuo and iuo:
        iuo.render_intro_tab(params)
    elif _is_dss and dss:
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
    if _is_pdan and pdan:
        pdan.render_settings_tab()
    elif _is_dual and dual:
        dual.render_db_tab(params)
    elif _is_cam and cam:
        cam.render_db_tab(params)
    elif _is_iuo and iuo:
        iuo.render_db_tab(params)
    elif _is_dss and dss:
        dss.render_db_tab(params)
    elif _is_sigma:
        sigma.render_settings_tab()
    elif _is_stdev:
        stdev.render_settings_tab()
    else:
        avg_close.render_settings_tab()

# 6번째 탭 (설정) — IUO / DSS / 듀얼 / 카마릴라
if _is_dual and dual:
    with tab6:
        dual.render_settings_tab()
elif _is_cam and cam:
    with tab6:
        cam.render_settings_tab()
elif _is_iuo and iuo:
    with tab6:
        iuo.render_settings_tab()
elif _is_dss and dss:
    with tab6:
        dss.render_settings_tab()
