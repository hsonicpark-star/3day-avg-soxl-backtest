"""
strategies/iuo.py — IUO 매매법 전략 모듈

사이클 기반 변동성 수확 전략:
- 30% 초기 매수 → 12.5%씩 물타기 → 마지막매수종가 +4.3% 전량 매도
- 시간청산(MOC) 18거래일
- QQQ GROWTH 추세 기반 첫매수비율 조정 (옵션)

인터페이스: DSS/Sigma 패턴 (자체 사이드바)
- render_sidebar() → params dict
- render_backtest_tab(params)
- render_optimization_tab(params)
- render_ordersheet_tab(params)
- render_intro_tab(params)
- render_db_tab(params)
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
import sys
import requests
import io as _io
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

_root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _root_dir not in sys.path:
    sys.path.insert(0, _root_dir)

from iuo_engine import (
    IUOParams, run_backtest, run_backtest_fast,
    calc_qqq_weekly_growth, get_trend_buy_ratio,
)
from common.config import _IS_CLOUD, _CONFIG, load_config, save_config

# ──────────────────────────────────────────────
# IUO 설정 경로 (로컬: ~/.iuo/)
# ──────────────────────────────────────────────
_IUO_CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".iuo")
_IUO_CONFIG_PATH = os.path.join(_IUO_CONFIG_DIR, "config.json")


def _load_iuo_config() -> dict:
    """IUO 설정 로드. Cloud: GSheets(session_state) / 로컬: ~/.iuo/config.json"""
    if _IS_CLOUD and st.session_state.get("logged_in"):
        raw = st.session_state.get("user_settings", {}).get("iuo_config", "")
        if raw:
            try:
                cfg = json.loads(raw) if isinstance(raw, str) else raw
                return cfg if isinstance(cfg, dict) else {}
            except Exception:
                pass
        return {}
    # 로컬
    if os.path.exists(_IUO_CONFIG_PATH):
        try:
            with open(_IUO_CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_iuo_config(cfg: dict):
    """IUO 설정 저장. Cloud: session_state + GSheets / 로컬: ~/.iuo/config.json"""
    # 로컬 파일 저장
    try:
        os.makedirs(_IUO_CONFIG_DIR, exist_ok=True)
        with open(_IUO_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # Cloud: Google Sheets에 사용자별 영구 저장
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            cfg_json = json.dumps(cfg, ensure_ascii=False)
            if "user_settings" not in st.session_state:
                st.session_state.user_settings = {}
            st.session_state.user_settings["iuo_config"] = cfg_json
            from common.auth import _save_user_settings_to_sheet
            _save_user_settings_to_sheet(st.session_state.username, {"iuo_config": cfg_json})
        except Exception:
            pass


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
# 다음 거래일 유틸
# ──────────────────────────────────────────────

def _nth_weekday(year, month, weekday, n):
    first = datetime(year, month, 1).date()
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))

def _easter_date(year):
    a = year % 19; b, c = divmod(year, 100); d, e = divmod(b, 4)
    f = (b + 8) // 25; g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30; i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return datetime(year, month, day + 1).date()

def _last_weekday(year, month, weekday):
    if month == 12:
        last = datetime(year + 1, 1, 1).date() - timedelta(days=1)
    else:
        last = datetime(year, month + 1, 1).date() - timedelta(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - timedelta(days=offset)

def _next_trading_date(d=None):
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
    # 오늘이 거래일이면 오늘 (LOC = 당일 장마감 주문)
    candidate = d
    for _ in range(30):
        if candidate.weekday() < 5 and candidate not in all_holidays:
            return candidate
        candidate += timedelta(days=1)
    return candidate


# ──────────────────────────────────────────────
# 데이터 캐싱
# ──────────────────────────────────────────────

@st.cache_data(ttl=3600)
def _download_soxl(start: str = "2009-01-01", end: str = "2027-01-01"):
    import yfinance as yf
    df = yf.download("SOXL", start=start, end=end, auto_adjust=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.index = pd.to_datetime(df.index)
    return df


@st.cache_data(ttl=3600)
def _download_qqq(start: str = "2009-01-01", end: str = "2027-01-01"):
    import yfinance as yf
    df = yf.download("QQQ", start=start, end=end, auto_adjust=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.index = pd.to_datetime(df.index)
    return df


@st.cache_data(ttl=3600)
def _download_ticker(ticker: str, start: str = "2009-01-01", end: str = "2027-01-01"):
    import yfinance as yf
    df = yf.download(ticker, start=start, end=end, auto_adjust=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.index = pd.to_datetime(df.index)
    return df


# ══════════════════════════════════════════════
# 사이드바
# ══════════════════════════════════════════════

def render_sidebar() -> dict:
    """IUO 매매법 사이드바 (자체 관리). Returns params dict."""
    cfg = _load_iuo_config()

    st.subheader("📊 IUO 매매법")

    # 종목 선택
    st.markdown("##### 종목")
    _TICKERS = ["SOXL", "TQQQ", "직접입력"]
    _tk_sel = st.selectbox("종목코드", _TICKERS, index=0, key="iuo_ticker_sel")
    if _tk_sel == "직접입력":
        ticker = st.text_input("티커 직접 입력", placeholder="예: SOXL", key="iuo_ticker_input").strip().upper()
        if not ticker:
            ticker = "SOXL"
    else:
        ticker = _tk_sel

    st.markdown("---")

    # 매매 파라미터
    st.markdown("##### 매매 파라미터")
    tk_cfg = cfg.get(ticker, {})

    first_buy_ratio = st.number_input(
        "첫매수비율 (%)", value=float(tk_cfg.get("first_buy_ratio", 33.0)),
        min_value=5.0, max_value=80.0, step=1.0, key="iuo_fbr",
    )
    col1, col2 = st.columns(2)
    buy1_pct = col1.number_input(
        "매수1 (%)", value=float(tk_cfg.get("buy1_pct", -1.8)),
        min_value=-30.0, max_value=0.0, step=0.1, key="iuo_b1",
    )
    buy2_pct = col2.number_input(
        "매수2 (%)", value=float(tk_cfg.get("buy2_pct", -10.0)),
        min_value=-50.0, max_value=0.0, step=0.5, key="iuo_b2",
    )
    sell_pct = st.number_input(
        "매도 (%)", value=float(tk_cfg.get("sell_pct", 4.3)),
        min_value=0.1, max_value=30.0, step=0.1, key="iuo_sp",
    )
    col3, col4 = st.columns(2)
    moc_days = col3.number_input(
        "시간청산 (거래일)", value=int(tk_cfg.get("moc_days", 18)),
        min_value=3, max_value=60, step=1, key="iuo_moc",
    )
    max_add_buys = col4.number_input(
        "매수제한 (회)", value=int(tk_cfg.get("max_add_buys", 7)),
        min_value=1, max_value=20, step=1, key="iuo_maxb",
    )
    divisions = st.number_input(
        "분할수", value=int(tk_cfg.get("divisions", 8)),
        min_value=2, max_value=20, step=1, key="iuo_div",
    )
    buy0_pct = st.number_input(
        "첫매수 LOC (%)", value=float(tk_cfg.get("buy0_pct", 0.0)),
        min_value=-10.0, max_value=10.0, step=0.1, key="iuo_b0",
        help="전일종가 대비. 0% = 전일종가 이하에서 매수",
    )

    st.markdown("---")

    # QQQ 추세 설정
    st.markdown("##### QQQ 추세 (옵션)")
    use_qqq = st.checkbox("QQQ 추세별 첫매수비율 조정", value=False, key="iuo_use_qqq")

    st.markdown("---")

    # 백테스트 설정
    st.markdown("##### 백테스트 설정")
    col5, col6 = st.columns(2)
    start_date = col5.date_input(
        "시작일", datetime(2015, 12, 31).date(), key="iuo_start",
    )
    end_date = col6.date_input("종료일", datetime.today().date(), key="iuo_end")
    initial_capital = st.number_input(
        "초기 투자금 ($)", value=float(tk_cfg.get("initial_capital", 10000.0)),
        step=1000.0, key="iuo_cap",
    )

    add_buy_amt = initial_capital * (first_buy_ratio / 100) / divisions if divisions > 0 else 0
    st.info(f"첫매수: ${initial_capital * first_buy_ratio / 100:,.0f} | "
            f"추가매수 1회: ~${add_buy_amt:,.0f}")

    return {
        "bt_ticker": ticker,
        "bt_start_date": start_date,
        "bt_end_date": end_date,
        "bt_initial_capital": initial_capital,
        "first_buy_ratio": first_buy_ratio / 100,
        "buy0_pct": buy0_pct / 100,
        "buy1_pct": buy1_pct / 100,
        "buy2_pct": buy2_pct / 100,
        "sell_pct": sell_pct / 100,
        "moc_days": moc_days,
        "max_additional_buys": max_add_buys,
        "divisions": divisions,
        "use_qqq_trend": use_qqq,
    }


# ══════════════════════════════════════════════
# 탭1: 백테스트
# ══════════════════════════════════════════════

def render_backtest_tab(params: dict):
    ticker = params["bt_ticker"]
    start = str(params["bt_start_date"])
    end = str(params["bt_end_date"])
    cap = params["bt_initial_capital"]

    if st.button("📊 백테스트 실행", key="iuo_run_bt", use_container_width=True):
        with st.spinner("데이터 로드 중..."):
            price_df = _download_ticker(ticker)
            qqq_df = _download_qqq() if params["use_qqq_trend"] else None

        iuo_params = IUOParams(
            initial_capital=cap,
            first_buy_ratio=params["first_buy_ratio"],
            buy0_pct=params["buy0_pct"],
            buy1_pct=params["buy1_pct"],
            buy2_pct=params["buy2_pct"],
            sell_pct=params["sell_pct"],
            moc_days=params["moc_days"],
            max_additional_buys=params["max_additional_buys"],
            divisions=params["divisions"],
            use_qqq_trend=params["use_qqq_trend"],
        )

        with st.spinner("백테스트 실행 중..."):
            result = run_backtest(iuo_params, price_df, qqq_df, start, end)

        st.session_state["iuo_bt_result"] = result
        st.session_state["iuo_bt_params"] = params

    result = st.session_state.get("iuo_bt_result")
    if result is None:
        st.info("위 버튼을 눌러 백테스트를 실행하세요.")
        return

    log = result["daily_log"]
    sells = result["sell_records"]
    m = result["metrics"]

    # ── 핵심 지표 카드 ──
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("최종 자산", f"${m['최종자산']:,.0f}")
    c2.metric("CAGR", f"{m['CAGR(%)']:.2f}%")
    c3.metric("MDD", f"{m['MDD(%)']:.2f}%")
    c4.metric("승률", f"{m['승률(%)']:.1f}%")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Sharpe", f"{m['Sharpe']:.3f}")
    c6.metric("Sortino", f"{m['Sortino']:.3f}")
    c7.metric("Calmar", f"{m['Calmar']:.3f}")
    c8.metric("총 매도", f"{m['총매도횟수']}회")

    c9, c10, c11, c12 = st.columns(4)
    c9.metric("익절", f"{m['익절']}회")
    c10.metric("손절", f"{m['손절']}회")
    c11.metric("평균 보유기간", f"{m['평균보유기간']:.1f}일")
    c12.metric("평균 현금비율", f"{m['평균현금비율(%)']:.1f}%")

    df = pd.DataFrame(log)
    df["날짜"] = pd.to_datetime(df["날짜"])

    # ── 자산 추이 차트 ──
    st.subheader("총자산 추이")
    fig_asset = go.Figure()
    fig_asset.add_trace(go.Scatter(
        x=df["날짜"], y=df["총자산"], mode="lines", name="IUO 총자산",
        line=dict(color="#2E86C1", width=1.5),
    ))

    # B&H 비교
    from common.analysis import compute_bnh
    try:
        price_df = _download_ticker(ticker)
        bnh = compute_bnh(price_df, str(params["bt_start_date"]),
                          str(params["bt_end_date"]), cap)
        if bnh is not None and "dates" in bnh:
            fig_asset.add_trace(go.Scatter(
                x=bnh["dates"], y=bnh["values"], mode="lines",
                name=f"B&H {ticker}", line=dict(color="gray", dash="dot"),
            ))
    except Exception:
        pass

    fig_asset.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0),
                            yaxis_title="총자산 ($)", hovermode="x unified")
    st.plotly_chart(fig_asset, use_container_width=True)

    # ── 드로다운 차트 ──
    st.subheader("드로다운 (DD)")
    fig_dd = go.Figure()
    fig_dd.add_trace(go.Scatter(
        x=df["날짜"], y=df["DD(%)"], mode="lines", name="DD(%)",
        fill="tozeroy", line=dict(color="#E74C3C", width=1),
    ))
    fig_dd.update_layout(height=250, margin=dict(l=0, r=0, t=30, b=0),
                         yaxis_title="DD (%)", hovermode="x unified")
    st.plotly_chart(fig_dd, use_container_width=True)

    # ── 추가매수 횟수 분포 ──
    if m.get("추가매수분포"):
        st.subheader("추가매수 횟수 분포")
        dist = m["추가매수분포"]
        dist_df = pd.DataFrame([
            {"추가매수횟수": k, "매도건수": v}
            for k, v in sorted(dist.items())
        ])
        fig_dist = px.bar(dist_df, x="추가매수횟수", y="매도건수",
                          text="매도건수", color_discrete_sequence=["#3498DB"])
        fig_dist.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig_dist, use_container_width=True)

    # ── 연도별 성과 ──
    st.subheader("연도별 성과")
    df["연도"] = df["날짜"].dt.year
    yearly = []
    for year, grp in df.groupby("연도"):
        first_asset = grp.iloc[0]["총자산"]
        last_asset = grp.iloc[-1]["총자산"]
        ret = (last_asset / first_asset - 1) * 100
        mdd_y = grp["DD(%)"].min()
        sells_y = [s for s in sells if s["매도일"].startswith(str(year))]
        wins_y = sum(1 for s in sells_y if s["실현손익"] > 0)
        total_y = len(sells_y)
        wr_y = (wins_y / total_y * 100) if total_y > 0 else 0
        yearly.append({
            "연도": year, "수익률(%)": round(ret, 2), "MDD(%)": round(mdd_y, 2),
            "매도건수": total_y, "승률(%)": round(wr_y, 1),
        })
    st.dataframe(pd.DataFrame(yearly), use_container_width=True, hide_index=True)

    # ── 매도 기록 ──
    with st.expander("📋 매도 기록 상세"):
        sell_df = pd.DataFrame(sells)
        st.dataframe(sell_df, use_container_width=True, hide_index=True)

    # ── 일별 매매 기록 ──
    with st.expander("📋 일별 매매 기록"):
        st.dataframe(df.drop(columns=["연도"], errors="ignore"),
                     use_container_width=True, hide_index=True)

    # ── CSV 다운로드 ──
    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        csv_log = df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("📥 일별 기록 CSV", csv_log, f"iuo_{ticker}_daily.csv",
                           "text/csv", key="iuo_dl_daily")
    with col_dl2:
        if sells:
            csv_sell = pd.DataFrame(sells).to_csv(index=False).encode("utf-8-sig")
            st.download_button("📥 매도 기록 CSV", csv_sell, f"iuo_{ticker}_sells.csv",
                               "text/csv", key="iuo_dl_sells")


# ══════════════════════════════════════════════
# 탭2: 파라미터 최적화
# ══════════════════════════════════════════════

def render_optimization_tab(params: dict):
    ticker = params["bt_ticker"]
    start = str(params["bt_start_date"])
    end = str(params["bt_end_date"])
    cap = params["bt_initial_capital"]

    st.subheader("파라미터 최적화")

    opt_method = st.selectbox(
        "최적화 방법", ["그리드 서치", "랜덤 서치", "워크포워드", "베이지안(Optuna)"],
        key="iuo_opt_method",
    )

    st.markdown("##### 파라미터 범위")
    col1, col2 = st.columns(2)
    with col1:
        fbr_range = st.slider("첫매수비율 (%)", 10.0, 60.0, (25.0, 40.0), 5.0, key="iuo_opt_fbr")
        b1_range = st.slider("매수1 (%)", -5.0, 0.0, (-3.0, -1.0), 0.5, key="iuo_opt_b1")
        sp_range = st.slider("매도 (%)", 1.0, 10.0, (3.0, 6.0), 0.5, key="iuo_opt_sp")
    with col2:
        b2_range = st.slider("매수2 (%)", -20.0, -3.0, (-15.0, -5.0), 1.0, key="iuo_opt_b2")
        moc_range = st.slider("시간청산 (일)", 5, 30, (12, 24), 3, key="iuo_opt_moc")
        div_range = st.slider("분할수", 4, 16, (6, 10), 1, key="iuo_opt_div")

    max_add = params["max_additional_buys"]

    if st.button("🚀 최적화 실행", key="iuo_run_opt", use_container_width=True):
        with st.spinner("데이터 로드 중..."):
            price_df = _download_ticker(ticker)
            qqq_df = None

        # 파라미터 조합 생성
        fbrs = np.arange(fbr_range[0], fbr_range[1] + 0.1, 5.0)
        b1s = np.arange(b1_range[0], b1_range[1] + 0.1, 0.5)
        b2s = np.arange(b2_range[0], b2_range[1] + 0.1, 2.0)
        sps = np.arange(sp_range[0], sp_range[1] + 0.1, 0.5)
        mocs = list(range(moc_range[0], moc_range[1] + 1, 3))
        divs = list(range(div_range[0], div_range[1] + 1, 2))

        if opt_method == "그리드 서치":
            combos = list(itertools.product(fbrs, b1s, b2s, sps, mocs, [max_add], divs))
        elif opt_method == "랜덤 서치":
            n_samples = st.session_state.get("iuo_n_samples", 200)
            combos = []
            for _ in range(n_samples):
                combos.append((
                    random.choice(fbrs.tolist()),
                    random.choice(b1s.tolist()),
                    random.choice(b2s.tolist()),
                    random.choice(sps.tolist()),
                    random.choice(mocs),
                    max_add,
                    random.choice(divs),
                ))
        else:
            combos = list(itertools.product(fbrs, b1s, b2s, sps, mocs, [max_add], divs))

        args_list = [
            (fbr, b1, b2, sp, moc, maxb, div, start, end, cap)
            for fbr, b1, b2, sp, moc, maxb, div in combos
        ]

        total = len(args_list)
        st.info(f"총 {total}개 조합 실행 중...")

        # 멀티프로세싱
        import pickle, tempfile
        data_path = os.path.join(tempfile.gettempdir(), "iuo_opt_data.pkl")
        with open(data_path, "wb") as f:
            pickle.dump((price_df, qqq_df), f)

        n_workers = max(1, mp.cpu_count() - 1)
        results = []
        progress = st.progress(0)

        try:
            from opt_worker_iuo import init_worker, run_single_bt
            with mp.Pool(n_workers, initializer=init_worker, initargs=(data_path,)) as pool:
                done = 0
                for r in pool.imap_unordered(run_single_bt, args_list, chunksize=max(1, total // (n_workers * 4))):
                    done += 1
                    if r is not None:
                        results.append(r)
                    if done % max(1, total // 20) == 0:
                        progress.progress(min(done / total, 1.0))
        except Exception as e:
            st.error(f"최적화 오류: {e}")
            return
        finally:
            progress.progress(1.0)
            try:
                os.remove(data_path)
            except Exception:
                pass

        if not results:
            st.warning("결과가 없습니다.")
            return

        res_df = pd.DataFrame(results).sort_values("Calmar", ascending=False)
        st.session_state["iuo_opt_results"] = res_df

    res_df = st.session_state.get("iuo_opt_results")
    if res_df is None:
        return

    st.success(f"최적화 완료: {len(res_df)}개 결과")

    # Top 20
    st.subheader("Top 20 파라미터")
    st.dataframe(res_df.head(20), use_container_width=True, hide_index=True)

    # CAGR vs MDD 산점도
    st.subheader("CAGR vs MDD")
    fig_scatter = px.scatter(
        res_df, x="MDD(%)", y="CAGR(%)", color="Calmar",
        hover_data=["첫매수%", "매수1%", "매수2%", "매도%", "MOC", "분할수"],
        color_continuous_scale="RdYlGn",
    )
    fig_scatter.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig_scatter, use_container_width=True)

    # CSV 다운로드
    csv_opt = res_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("📥 최적화 결과 CSV", csv_opt, "iuo_optimization.csv",
                       "text/csv", key="iuo_dl_opt")


# ══════════════════════════════════════════════
# 탭3: 주문표 & 계좌관리
# ══════════════════════════════════════════════

def _get_iuo_history_path(ticker: str, acct_name: str = "") -> str:
    """IUO 히스토리 CSV 파일 경로."""
    safe_name = acct_name.replace(" ", "_").replace("/", "_").replace("\\", "_")
    if safe_name:
        return os.path.join(_IUO_CONFIG_DIR, f"history_{ticker}_{safe_name}.csv")
    return os.path.join(_IUO_CONFIG_DIR, f"history_{ticker}.csv")


def _load_iuo_history(ticker: str, acct_name: str = "") -> pd.DataFrame:
    """IUO 히스토리 CSV 로드."""
    path = _get_iuo_history_path(ticker, acct_name)
    if os.path.exists(path):
        try:
            return pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def _save_iuo_history(ticker: str, daily_log: list, acct_name: str = ""):
    """IUO 히스토리 CSV 저장 (B방식: 기존 날짜 보존, 새 날짜만 추가)."""
    if not daily_log:
        return
    os.makedirs(_IUO_CONFIG_DIR, exist_ok=True)
    new_df = pd.DataFrame(daily_log)
    path = _get_iuo_history_path(ticker, acct_name)
    if os.path.exists(path):
        old_df = pd.read_csv(path, encoding="utf-8-sig")
        old_dates = set(old_df["날짜"].astype(str))
        append_df = new_df[~new_df["날짜"].astype(str).isin(old_dates)]
        if not append_df.empty:
            combined = pd.concat([old_df, append_df], ignore_index=True)
            combined.to_csv(path, index=False, encoding="utf-8-sig")
    else:
        new_df.to_csv(path, index=False, encoding="utf-8-sig")


def render_ordersheet_tab(params: dict):
    ticker = params["bt_ticker"]
    cfg = _load_iuo_config()
    today_str = datetime.today().strftime("%Y-%m-%d")

    st.subheader(f"📋 오늘의 주문표 — {today_str}")
    st.caption("종목별 포트폴리오를 추적하고, 오늘의 LOC 주문 기준가와 계좌를 관리합니다.")

    # ── 등록된 계좌 로드 ──
    accounts = cfg.get("accounts", {})

    # ── 계좌 추가 ──
    _ADD_PRESETS = [
        {"label": "🚀 공격형", "first_buy_ratio": 40, "buy1_pct": -1.0, "buy2_pct": -11.0,
         "sell_pct": 6.0, "moc_days": 24, "max_add_buys": 7, "divisions": 6},
        {"label": "⚖️ 균형형", "first_buy_ratio": 35, "buy1_pct": -1.0, "buy2_pct": -11.0,
         "sell_pct": 6.0, "moc_days": 24, "max_add_buys": 7, "divisions": 6},
        {"label": "🛡️ 안정형", "first_buy_ratio": 25, "buy1_pct": -1.0, "buy2_pct": -11.0,
         "sell_pct": 6.0, "moc_days": 24, "max_add_buys": 7, "divisions": 6},
    ]
    _preset_labels = [pr["label"] for pr in _ADD_PRESETS]

    with st.expander("➕ 계좌 추가", expanded=False):
        add_col1, add_col2 = st.columns(2)
        add_name = add_col1.text_input("계좌 이름", value="", key="iuo_add_name",
                                       placeholder="예: PJH, 연습용")
        _preset_idx = add_col2.selectbox("파라미터 프리셋", range(len(_ADD_PRESETS)),
                                          format_func=lambda i: _preset_labels[i],
                                          index=2, key="iuo_add_preset")
        add_col3, add_col4 = st.columns(2)
        add_start = add_col3.date_input("시작일", datetime.today().date(), key="iuo_add_start")
        add_cap = add_col4.number_input("시작 자본 ($)", value=10000.0, step=1000.0, key="iuo_add_cap")

        if st.button("✅ 계좌 등록", type="primary", key="iuo_add_acct", use_container_width=True):
            _nm = add_name.strip()
            if not _nm:
                st.error("계좌 이름을 입력하세요.")
            elif _nm in accounts:
                st.error(f"'{_nm}' 계좌가 이미 존재합니다.")
            else:
                _sel_preset = _ADD_PRESETS[_preset_idx]
                accounts[_nm] = {
                    "ticker": ticker,
                    "params": {
                        "first_buy_ratio": _sel_preset["first_buy_ratio"],
                        "buy0_pct": params["buy0_pct"] * 100,
                        "buy1_pct": _sel_preset["buy1_pct"],
                        "buy2_pct": _sel_preset["buy2_pct"],
                        "sell_pct": _sel_preset["sell_pct"],
                        "moc_days": _sel_preset["moc_days"],
                        "max_add_buys": _sel_preset["max_add_buys"],
                        "divisions": _sel_preset["divisions"],
                    },
                    "os_start": str(add_start),
                    "os_capital": add_cap,
                }
                cfg["accounts"] = accounts
                _save_iuo_config(cfg)
                st.success(f"✅ '{_nm}' 계좌가 등록되었습니다. (프리셋: {_sel_preset['label']})")
                st.rerun()

    # ── 계좌별 탭 렌더링 ──
    acct_keys = list(accounts.keys())
    if not acct_keys:
        st.info("등록된 계좌가 없습니다. 위에서 계좌를 추가하세요.")
        return

    tabs = st.tabs([f"📊 {k}" for k in acct_keys])
    for i, (tab, key) in enumerate(zip(tabs, acct_keys)):
        with tab:
            _render_iuo_account(key, accounts[key], cfg, params, i)


def _render_iuo_account(acct_key: str, acct_data: dict, cfg: dict, params: dict, idx: int):
    """개별 IUO 계좌 렌더링 — avg_close 레이아웃 순서 준수."""
    sfx = f"_{idx}"  # session_state key suffix
    tk_params = acct_data.get("params", {})
    acct_ticker = acct_data.get("ticker", acct_key.split("_")[0])
    os_start = acct_data.get("os_start", "2015-12-31")
    os_capital = float(acct_data.get("os_capital", 10000))

    # 파라미터 값 (계좌 저장값 우선, 없으면 사이드바 값)
    fbr = float(tk_params.get("first_buy_ratio", params["first_buy_ratio"] * 100)) / 100
    b0 = float(tk_params.get("buy0_pct", params["buy0_pct"] * 100)) / 100
    b1 = float(tk_params.get("buy1_pct", params["buy1_pct"] * 100)) / 100
    b2 = float(tk_params.get("buy2_pct", params["buy2_pct"] * 100)) / 100
    sp = float(tk_params.get("sell_pct", params["sell_pct"] * 100)) / 100
    moc = int(tk_params.get("moc_days", params["moc_days"]))
    maxb = int(tk_params.get("max_add_buys", params["max_additional_buys"]))
    div = int(tk_params.get("divisions", params["divisions"]))

    # ── 1) 파라미터 표시 + 수정 (border container) ──
    with st.container(border=True):
        _p1, _p2, _p3, _p4, _p5 = st.columns(5)
        _p1.metric("첫매수비율", f"{fbr*100:.0f}%")
        _p2.metric("매수1 / 매수2", f"{b1*100:.1f}% / {b2*100:.1f}%")
        _p3.metric("매도기준", f"{sp*100:.1f}%")
        _p4.metric("분할수", f"{div}회")
        _p5.metric("MOC / 매수제한", f"{moc}일 / {maxb}회")

        with st.expander("✏️ 파라미터 수정"):
            # ── 추천 프리셋 ──
            _IUO_PRESETS = [
                {"label": "🚀 공격형",    "first_buy_ratio": 40, "buy1_pct": -1.0, "buy2_pct": -11.0,
                 "sell_pct": 6.0, "moc_days": 24, "max_add_buys": 7, "divisions": 6,
                 "help": "CAGR 69.51%  |  MDD -44.12%  |  Calmar 1.576  |  승률 78.1%\n높은 수익률 추구, 변동성 감수"},
                {"label": "⚖️ 균형형",   "first_buy_ratio": 35, "buy1_pct": -1.0, "buy2_pct": -11.0,
                 "sell_pct": 6.0, "moc_days": 24, "max_add_buys": 7, "divisions": 6,
                 "help": "CAGR 63.07%  |  MDD -38.82%  |  Calmar 1.625  |  승률 77.5%\n수익률과 안정성의 중간"},
                {"label": "🛡️ 안정형 ⭐", "first_buy_ratio": 25, "buy1_pct": -1.0, "buy2_pct": -11.0,
                 "sell_pct": 6.0, "moc_days": 24, "max_add_buys": 7, "divisions": 6,
                 "help": "CAGR 42.35%  |  MDD -28.00%  |  Calmar 1.512  |  승률 77.2%\n최저 MDD — 안정적 운용 추천"},
            ]
            st.caption("💡 추천 프리셋 — 버튼 위에 마우스를 올리면 성과 지표를 확인할 수 있습니다.")
            _pc1, _pc2, _pc3 = st.columns(3)
            for _pi, (_pcol, _pr) in enumerate(zip([_pc1, _pc2, _pc3], _IUO_PRESETS)):
                if _pcol.button(_pr["label"], key=f"iuo_preset_{_pi}{sfx}",
                                help=_pr["help"], use_container_width=True):
                    st.session_state[f"iuo_e_fbr{sfx}"] = float(_pr["first_buy_ratio"])
                    st.session_state[f"iuo_e_b1{sfx}"]  = float(_pr["buy1_pct"])
                    st.session_state[f"iuo_e_b2{sfx}"]  = float(_pr["buy2_pct"])
                    st.session_state[f"iuo_e_sp{sfx}"]  = float(_pr["sell_pct"])
                    st.session_state[f"iuo_e_moc{sfx}"] = int(_pr["moc_days"])
                    st.session_state[f"iuo_e_maxb{sfx}"] = int(_pr["max_add_buys"])
                    st.session_state[f"iuo_e_div{sfx}"] = int(_pr["divisions"])
                    st.rerun()
            st.divider()

            ec1, ec2, ec3, ec4 = st.columns(4)
            e_fbr = ec1.number_input("첫매수비율(%)", value=fbr * 100, step=1.0,
                                     key=f"iuo_e_fbr{sfx}")
            e_b1 = ec2.number_input("매수1(%)", value=b1 * 100, step=0.1,
                                    key=f"iuo_e_b1{sfx}")
            e_b2 = ec3.number_input("매수2(%)", value=b2 * 100, step=0.5,
                                    key=f"iuo_e_b2{sfx}")
            e_sp = ec4.number_input("매도(%)", value=sp * 100, step=0.1,
                                    key=f"iuo_e_sp{sfx}")
            ec5, ec6, ec7 = st.columns(3)
            e_moc = ec5.number_input("시간청산(일)", value=moc, step=1,
                                     key=f"iuo_e_moc{sfx}")
            e_maxb = ec6.number_input("매수제한(회)", value=maxb, step=1,
                                      key=f"iuo_e_maxb{sfx}")
            e_div = ec7.number_input("분할수", value=div, step=1,
                                     key=f"iuo_e_div{sfx}")

            if st.button("💾 파라미터 저장", key=f"iuo_save_p{sfx}", type="primary",
                         use_container_width=True):
                acct_data["params"] = {
                    "first_buy_ratio": e_fbr, "buy0_pct": b0 * 100,
                    "buy1_pct": e_b1, "buy2_pct": e_b2,
                    "sell_pct": e_sp, "moc_days": e_moc,
                    "max_add_buys": e_maxb, "divisions": e_div,
                }
                cfg["accounts"][acct_key] = acct_data
                _save_iuo_config(cfg)
                st.success(f"✅ {acct_key} 파라미터가 저장되었습니다!")
                st.rerun()

    # ── 2) 계좌 관리 (이름 변경 / 삭제) ──
    _mgr1, _mgr2, _ = st.columns([1, 1, 4])
    if _mgr1.button("✏️ 이름 변경", key=f"iuo_rename_btn{sfx}", type="secondary"):
        st.session_state[f"iuo_renaming{sfx}"] = True
    if _mgr2.button("🗑️ 계좌 삭제", key=f"iuo_del_acct{sfx}", type="secondary"):
        st.session_state[f"iuo_confirm_del{sfx}"] = True

    # ── 이름 변경 ──
    if st.session_state.get(f"iuo_renaming{sfx}", False):
        _rn1, _rn2, _rn3, _ = st.columns([2, 1, 1, 3])
        _new_nm = _rn1.text_input("새 계좌 이름", value=acct_key, key=f"iuo_new_name{sfx}")
        if _rn2.button("✅ 변경", key=f"iuo_rename_ok{sfx}", type="primary"):
            _nm = _new_nm.strip()
            accounts = cfg.get("accounts", {})
            if not _nm:
                st.warning("계좌 이름을 입력하세요.")
            elif _nm == acct_key:
                st.session_state[f"iuo_renaming{sfx}"] = False
                st.rerun()
            elif _nm in accounts:
                st.warning(f"'{_nm}' 계좌가 이미 존재합니다.")
            else:
                new_accounts = {}
                for k, v in accounts.items():
                    new_accounts[_nm if k == acct_key else k] = v
                cfg["accounts"] = new_accounts
                _save_iuo_config(cfg)
                old_hp = _get_iuo_history_path(acct_ticker, acct_key)
                new_hp = _get_iuo_history_path(acct_ticker, _nm)
                if os.path.exists(old_hp):
                    os.rename(old_hp, new_hp)
                st.session_state.pop(f"iuo_renaming{sfx}", None)
                st.session_state.pop(f"iuo_os_result{sfx}", None)
                st.success(f"✅ '{acct_key}' → '{_nm}' 변경 완료!")
                st.rerun()
        if _rn3.button("❌ 취소", key=f"iuo_rename_cancel{sfx}"):
            st.session_state[f"iuo_renaming{sfx}"] = False
            st.rerun()

    # ── 계좌 삭제 확인 ──
    if st.session_state.get(f"iuo_confirm_del{sfx}", False):
        st.warning(f"⚠️ **{acct_key}** 계좌를 삭제하시겠습니까? 저장된 설정 및 매매 히스토리가 모두 삭제됩니다.")
        dc1, dc2, _ = st.columns([1, 1, 4])
        if dc1.button("✅ 삭제", key=f"iuo_confirm_yes{sfx}", type="primary"):
            del cfg["accounts"][acct_key]
            _save_iuo_config(cfg)
            hist_path = _get_iuo_history_path(acct_ticker, acct_key)
            if os.path.exists(hist_path):
                os.remove(hist_path)
            st.session_state.pop(f"iuo_confirm_del{sfx}", None)
            st.success(f"'{acct_key}' 계좌가 삭제되었습니다.")
            st.rerun()
        if dc2.button("❌ 취소", key=f"iuo_confirm_no{sfx}"):
            st.session_state[f"iuo_confirm_del{sfx}"] = False
            st.rerun()

    # ── 3) 시작일 / 자본금 ──
    mc1, mc2 = st.columns(2)
    os_start_input = mc1.date_input("시작일", pd.to_datetime(os_start).date(),
                                    min_value=datetime(2000, 1, 1).date(),
                                    max_value=datetime.today().date(),
                                    key=f"iuo_e_start{sfx}")
    os_capital_input = mc2.number_input("시작 자본 ($)", value=os_capital, step=1000.0,
                                        key=f"iuo_e_cap{sfx}")

    # ── 4) 자본 조정 (증액 / 감액) ──
    with st.expander("💰 자본 조정 (증액 / 감액)"):
        st.caption("현재 자본금에 추가하거나 차감할 금액을 입력하세요.")
        _adj_history_raw = acct_data.get("capital_adj_history", "[]")
        try:
            _adj_history = json.loads(_adj_history_raw) if isinstance(_adj_history_raw, str) else _adj_history_raw
            if not isinstance(_adj_history, list): _adj_history = []
        except: _adj_history = []

        _adj_c1, _adj_c2 = st.columns([2, 1])
        _adj_amount = _adj_c1.number_input("조정 금액 ($)", value=0.0, step=500.0,
                                            help="증액: 양수 · 감액: 음수",
                                            key=f"iuo_adj_input{sfx}")
        _adj_c1.caption(
            f"적용 후 자본금: **${os_capital + _adj_amount:,.0f}** "
            f"({'↑' if _adj_amount > 0 else '↓' if _adj_amount < 0 else '='} "
            f"${abs(_adj_amount):,.0f})"
        )
        _adj_memo = _adj_c1.text_input("메모 (선택)", placeholder="예: 3월 추가 입금",
                                        key=f"iuo_adj_memo{sfx}")
        if _adj_c2.button("💰 적용", use_container_width=True,
                          key=f"iuo_adj_apply{sfx}", disabled=(_adj_amount == 0)):
            _new_capital = os_capital + _adj_amount
            if _new_capital <= 0:
                st.error("자본금은 0보다 커야 합니다.")
            else:
                _adj_history.append({
                    "날짜": datetime.today().strftime("%Y-%m-%d"),
                    "조정금액": float(_adj_amount),
                    "누적자본금": float(_new_capital),
                    "메모": _adj_memo or ("증액" if _adj_amount > 0 else "감액"),
                })
                acct_data["os_capital"] = _new_capital
                acct_data["capital_adj_history"] = json.dumps(_adj_history, ensure_ascii=False)
                cfg["accounts"][acct_key] = acct_data
                _save_iuo_config(cfg)
                st.success(f"✅ 자본금이 **${_new_capital:,.0f}**으로 업데이트되었습니다.")
                st.rerun()

        if _adj_history:
            st.markdown("---")
            st.markdown("**📋 자본 조정 이력**")
            _df_adj = pd.DataFrame(_adj_history)
            _df_adj["조정금액"] = _df_adj["조정금액"].apply(lambda x: f"{'↑' if x>0 else '↓'} ${abs(x):,.0f}")
            _df_adj["누적자본금"] = _df_adj["누적자본금"].apply(lambda x: f"${x:,.0f}")
            st.dataframe(_df_adj[["날짜","조정금액","누적자본금","메모"]],
                         use_container_width=True, hide_index=True)
        else:
            st.info("아직 자본 조정 이력이 없습니다.")

        # 전체 초기화
        st.markdown("---")
        st.markdown("**🔄 전체 초기화**")
        st.caption("시작일·자본금·조정 이력을 모두 초기화합니다.")
        _rc1, _rc2, _rc3 = st.columns(3)
        _reset_start = _rc1.date_input("새 시작일", value=datetime.today().date(),
                                        key=f"iuo_reset_start{sfx}")
        _reset_capital = _rc2.number_input("새 시작 자본 ($)", value=os_capital,
                                            step=1000.0, key=f"iuo_reset_cap{sfx}")
        if _rc3.button("🔄 초기화", use_container_width=True,
                       key=f"iuo_do_reset{sfx}", type="secondary"):
            st.session_state[f"iuo_reset_confirmed{sfx}"] = True
        if st.session_state.get(f"iuo_reset_confirmed{sfx}", False):
            st.warning(f"⚠️ **정말 초기화하시겠습니까?**  \n"
                       f"시작일: {_reset_start} / 자본금: ${_reset_capital:,.0f} / 조정 이력 전체 삭제")
            _conf_c1, _conf_c2 = st.columns(2)
            if _conf_c1.button("✅ 확인 (초기화)", type="primary", key=f"iuo_confirm_reset{sfx}"):
                acct_data["os_start"] = str(_reset_start)
                acct_data["os_capital"] = float(_reset_capital)
                acct_data["capital_adj_history"] = "[]"
                cfg["accounts"][acct_key] = acct_data
                _save_iuo_config(cfg)
                st.session_state[f"iuo_reset_confirmed{sfx}"] = False
                st.success(f"✅ 초기화 완료! 시작일: {_reset_start} / 자본금: ${_reset_capital:,.0f}")
                st.rerun()
            if _conf_c2.button("❌ 취소", key=f"iuo_cancel_reset{sfx}"):
                st.session_state[f"iuo_reset_confirmed{sfx}"] = False
                st.rerun()

    # ── 5) 새로고침 / 주문표 로드 버튼 ──
    # 가격 데이터 로드
    try:
        price_df = _download_ticker(acct_ticker)
        last_close = round(float(price_df.iloc[-1]["Close"]), 2)
        last_date = price_df.index[-1].strftime("%Y-%m-%d")
    except Exception:
        st.error("가격 데이터를 로드할 수 없습니다.")
        return

    _ss_key = f"iuo_os_result{sfx}"
    _use_start = str(os_start_input)
    _use_capital = float(os_capital_input)
    _btn_label = "🔄 새로고침" if st.session_state.get(_ss_key) else "📋 주문표 로드"
    if st.button(_btn_label, type="primary", key=f"iuo_load_os{sfx}", use_container_width=True):
        # 시작일/자본 저장
        acct_data["os_start"] = _use_start
        acct_data["os_capital"] = _use_capital
        cfg["accounts"][acct_key] = acct_data
        _save_iuo_config(cfg)

        iuo_p = IUOParams(
            initial_capital=_use_capital,
            first_buy_ratio=fbr, buy0_pct=b0, buy1_pct=b1, buy2_pct=b2,
            sell_pct=sp, moc_days=moc, max_additional_buys=maxb, divisions=div,
        )
        with st.spinner("백테스트 실행 중..."):
            result = run_backtest(iuo_p, price_df, None, _use_start, last_date)
        if result:
            st.session_state[_ss_key] = result
            _save_iuo_history(acct_ticker, result["daily_log"], acct_key)
        else:
            st.warning("백테스트 결과가 없습니다. 시작일을 확인하세요.")

    result = st.session_state.get(_ss_key)

    next_td = _next_trading_date()

    # 현재 보유 상태 파악 (백테스트 결과에서)
    has_position = False
    last_buy_close = None
    add_buy_count = 0
    cycle_day = 0
    cur_shares = 0
    avg_cost = 0
    cash = _use_capital
    total_asset = _use_capital
    cycle_base = _use_capital

    if result:
        log = result["daily_log"]
        if log:
            last_row = log[-1]
            cur_shares = last_row.get("보유수량", 0)
            has_position = cur_shares > 0
            last_buy_close = last_row.get("마지막매수종가", None)
            add_buy_count = last_row.get("추가매수횟수", 0)
            cycle_day = last_row.get("진행일", 0)
            avg_cost = last_row.get("평단가", 0)
            cash = last_row.get("예수금", _use_capital)
            total_asset = last_row.get("총자산", _use_capital)
            cycle_base = last_row.get("매수기준액", _use_capital)

    # ── 6) 포트폴리오 현황 ──
    if result:
        st.caption(f"{_use_start} ~ {last_date}")
        cum_realized = sum(s.get("실현손익", 0) for s in result.get("sell_records", []))
        dd_val = result.get("metrics", {}).get("mdd", 0)
        stock_ratio = (cur_shares * last_close / total_asset * 100) if total_asset > 0 else 0

        p1, p2, p3, p4, p5 = st.columns(5)
        p1.metric("시작 자본", f"${_use_capital:,.0f}")
        p2.metric("평가 자산", f"${total_asset:,.0f}",
                  f"📈 CAGR {result.get('metrics',{}).get('cagr',0):.1f}%")
        p3.metric("실현손익", f"${cum_realized:+,.2f}",
                  f"{'✅ 수익' if cum_realized > 0 else '❌ 손실'}")
        p4.metric("현재 DD", f"{dd_val:.2f}%",
                  f"{'📉' if dd_val < -5 else '📈'} {dd_val:+.2f}%")
        p5.metric("주식 비중", f"{stock_ratio:.1f}%")

        if has_position:
            eval_amt = cur_shares * last_close
            unreal_pnl = eval_amt - (cur_shares * avg_cost) if avg_cost > 0 else 0
            unreal_pct = (last_close / avg_cost - 1) * 100 if avg_cost > 0 else 0

            st.markdown("**보유 현황**")
            hold_data = {
                "종목": [acct_ticker], "보유수량": [f"{cur_shares:,}주"],
                "평단가": [f"${avg_cost:,.2f}"], "현재가": [f"${last_close:,.2f}"],
                "평가금액": [f"${eval_amt:,.0f}"],
                "평가손익": [f"${unreal_pnl:+,.0f} ({unreal_pct:+.1f}%)"],
                "추가매수": [f"{add_buy_count}/{maxb}회"],
                "진행일": [f"{cycle_day}/{moc}일"],
                "마지막매수종가": [f"${last_buy_close:,.2f}" if last_buy_close else "-"],
            }
            st.dataframe(pd.DataFrame(hold_data), use_container_width=True, hide_index=True)
        else:
            st.info("현재 보유 포지션이 없습니다. 다음 사이클 첫매수를 기다리는 중입니다.")
    else:
        st.caption("'📋 주문표 로드' 버튼을 눌러 포트폴리오 현황을 확인하세요.")

    # ── 7) 오늘의 LOC 주문 ──
    st.divider()
    st.subheader(f"📑 오늘의 LOC 주문 — {next_td.strftime('%Y-%m-%d')}")
    st.caption(f"기준가 (전일종가): ${last_close:,.2f} ({last_date})")

    buy0_loc = round(last_close * (1 + b0), 2)
    buy1_loc = round(last_close * (1 + b1), 2)
    buy2_loc = round(last_close * (1 + b2), 2)

    today_orders = []
    if has_position:
        if last_buy_close and last_buy_close > 0:
            sell_loc = round(last_buy_close * (1 + sp), 2)
            today_orders.append({
                "구분": "📈 매도", "LOC 기준가": f"${sell_loc:,.2f}",
                "수량": f"{cur_shares:,}주 (전량)",
                "예상금액": f"${cur_shares * sell_loc:,.2f}",
                "전일종가 대비": f"{(sell_loc / last_close - 1) * 100:+.2f}%",
                "비고": f"마지막매수종가 ${last_buy_close:,.2f} × (1+{sp*100:.1f}%)",
            })
        remaining = maxb - add_buy_count
        if remaining > 0:
            buy_amt = cycle_base / div if div > 0 else 0
            qty1 = math.floor(buy_amt / buy1_loc) if buy1_loc > 0 else 0
            today_orders.append({
                "구분": "🔴 추가매수1", "LOC 기준가": f"${buy1_loc:,.2f}",
                "수량": f"{qty1:,}주",
                "예상금액": f"${qty1 * buy1_loc:,.2f}",
                "전일종가 대비": f"{b1 * 100:+.1f}%",
                "비고": f"잔여 {remaining}회 │ 진행일 {cycle_day}일",
            })
            if remaining >= 2:
                qty2 = math.floor(buy_amt / buy2_loc) if buy2_loc > 0 else 0
                today_orders.append({
                    "구분": "🔴 추가매수2", "LOC 기준가": f"${buy2_loc:,.2f}",
                    "수량": f"{qty2:,}주",
                    "예상금액": f"${qty2 * buy2_loc:,.2f}",
                    "전일종가 대비": f"{b2 * 100:+.1f}%",
                    "비고": f"잔여 {remaining}회 │ 하루 2건 체결 가능",
                })
    else:
        first_amt = cycle_base * fbr
        qty0 = math.floor(first_amt / buy0_loc) if buy0_loc > 0 else 0
        today_orders.append({
            "구분": "🟢 첫매수", "LOC 기준가": f"${buy0_loc:,.2f}",
            "수량": f"{qty0:,}주",
            "예상금액": f"${qty0 * buy0_loc:,.2f}",
            "전일종가 대비": f"{b0 * 100:+.1f}%",
            "비고": f"매수기준액 ${cycle_base:,.0f} × {fbr*100:.0f}%",
        })

    if today_orders:
        order_df = pd.DataFrame(today_orders)

        def _style_order(row):
            if "매도" in str(row["구분"]):
                return ["color: #1565C0; font-weight: bold"] * len(row)
            return ["color: #C62828; font-weight: bold"] * len(row)

        st.dataframe(order_df.style.apply(_style_order, axis=1),
                     use_container_width=True, hide_index=True,
                     height=38 + 35 * len(today_orders))
    else:
        st.info("주문표를 로드하면 오늘의 LOC 주문이 표시됩니다.")

    # ── 8) 최근 매도 이력 ──
    if result and result.get("sell_records"):
        st.divider()
        st.subheader("📈 최근 매도 이력")
        sells = result["sell_records"]
        sell_df = pd.DataFrame(sells[-20:])
        if not sell_df.empty:
            wins = sum(1 for s in sells if s.get("실현손익", 0) > 0)
            losses = len(sells) - wins
            sc1, sc2, sc3 = st.columns(3)
            sc1.metric("총 매도", f"{len(sells)}회")
            sc2.metric("승률", f"{wins/len(sells)*100:.1f}%" if sells else "-")
            sc3.metric("승/패", f"{wins}승 {losses}패")
            st.dataframe(sell_df.iloc[::-1], use_container_width=True, hide_index=True)

    # ── 9) 일별 매매 히스토리 ──
    st.divider()
    with st.expander("📋 일별 매매 상세표", expanded=False):
        hist_df = _load_iuo_history(acct_ticker, acct_key)
        if hist_df.empty and result:
            hist_df = pd.DataFrame(result["daily_log"])
        if not hist_df.empty:
            buy_count = (pd.to_numeric(hist_df.get("매수수량", pd.Series(dtype=float)),
                                       errors="coerce").fillna(0) > 0).sum()
            sell_count = (pd.to_numeric(hist_df.get("매도수량", pd.Series(dtype=float)),
                                        errors="coerce").fillna(0) < 0).sum()
            first_date = hist_df.iloc[0].get("날짜", "")
            last_date_h = hist_df.iloc[-1].get("날짜", "")
            st.caption(f"기록 {first_date} ~ {last_date_h} │ "
                       f"총 {buy_count + sell_count}건 (매수 {buy_count}회 · 매도 {sell_count}회)")

            display_df = hist_df.copy()

            def _style_trade_row(row):
                buy_q = pd.to_numeric(row.get("매수수량", 0), errors="coerce") or 0
                sell_q = pd.to_numeric(row.get("매도수량", 0), errors="coerce") or 0
                if buy_q > 0:
                    return ["background-color: #FFF0F0"] * len(row)
                elif sell_q < 0:
                    return ["background-color: #F0FFF4"] * len(row)
                return [""] * len(row)

            st.dataframe(display_df.iloc[::-1].style.apply(_style_trade_row, axis=1),
                         use_container_width=True, hide_index=True)

            dl1, dl2 = st.columns(2)
            with dl1:
                csv_data = display_df.to_csv(index=False, encoding="utf-8-sig")
                st.download_button(
                    "📥 CSV 다운로드", csv_data,
                    f"IUO_{acct_key}_{datetime.today().strftime('%Y%m%d')}.csv",
                    "text/csv", key=f"iuo_dl_csv{sfx}")
            with dl2:
                try:
                    buf = _io.BytesIO()
                    display_df.to_excel(buf, index=False, sheet_name="일별매매상세",
                                        engine="openpyxl")
                    st.download_button(
                        "📥 Excel 다운로드", buf.getvalue(),
                        f"IUO_{acct_key}_{datetime.today().strftime('%Y%m%d')}.xlsx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"iuo_dl_xlsx{sfx}")
                except Exception:
                    pass
        else:
            st.caption("히스토리가 없습니다. 주문표를 로드하면 자동 생성됩니다.")


# ──────────────────────────────────────────────
# 회복력 분석 헬퍼
# ──────────────────────────────────────────────

def _compute_recovery_table(assets, dates, threshold=10.0):
    """고점 대비 threshold% 이상 하락 에피소드별 회복력 분석 테이블 반환."""
    records = []
    n = len(assets)
    if n == 0:
        return records
    peak_val = float(assets[0])
    peak_idx = 0
    in_dd = False
    trough_val = peak_val
    trough_idx = 0
    for i in range(1, n):
        curr = float(assets[i])
        dd_pct = (curr - peak_val) / peak_val * 100
        if not in_dd:
            if curr > peak_val:
                peak_val = curr
                peak_idx = i
            elif dd_pct <= -threshold:
                in_dd = True
                trough_val = curr
                trough_idx = i
        else:
            if curr < trough_val:
                trough_val = curr
                trough_idx = i
            if curr >= peak_val:
                drop_rate = (trough_val - peak_val) / peak_val * 100
                records.append({
                    "고점": str(pd.Timestamp(dates[peak_idx]).date()),
                    "고점 평가액": round(peak_val),
                    "최대하락 시점": str(pd.Timestamp(dates[trough_idx]).date()),
                    "저점 평가액": round(trough_val),
                    "하락율(%)": round(drop_rate, 2),
                    "회복 시점": str(pd.Timestamp(dates[i]).date()),
                    "기간(일)": (pd.Timestamp(dates[i]) - pd.Timestamp(dates[peak_idx])).days,
                })
                in_dd = False
                peak_val = curr
                peak_idx = i
                trough_val = curr
                trough_idx = i
    if in_dd:
        drop_rate = (trough_val - peak_val) / peak_val * 100
        records.append({
            "고점": str(pd.Timestamp(dates[peak_idx]).date()),
            "고점 평가액": round(peak_val),
            "최대하락 시점": str(pd.Timestamp(dates[trough_idx]).date()),
            "저점 평가액": round(trough_val),
            "하락율(%)": round(drop_rate, 2),
            "회복 시점": "미회복",
            "기간(일)": (pd.Timestamp(dates[-1]) - pd.Timestamp(dates[peak_idx])).days,
        })
    return records


# ══════════════════════════════════════════════
# 탭4: 전략 소개 & 성과분석
# ══════════════════════════════════════════════

def render_intro_tab(params: dict):
    st.subheader("📖 IUO 매매법 소개")

    left, right = st.columns([3, 2])
    with left:
        st.markdown("""
#### 전략 개요
IUO 매매법은 **SOXL(3x 레버리지 반도체 ETF)** 의 V자 반등 변동성을 수확하는 **사이클 기반 전략**입니다.

---

#### 매수 규칙
1. **첫매수**: 사이클 시작 시 매수기준액(=전 사이클 종료 자산)의 **33%** 를 투입
   - 전일종가 이하에서 LOC 매수
2. **추가매수**: 매일 2개 LOC 주문
   - Level 1: 전일종가 × (1 - 1.8%) 이하 → 총투자금의 12.5%(1/8) 매수
   - Level 2: 전일종가 × (1 - 10%) 이하 → 총투자금의 12.5%(1/8) 매수
3. **매수 제한**: 추가매수 최대 7회 (최소 현금 ~30% 유지)

#### 매도 규칙
1. **익절**: 종가 ≥ **마지막 매수일 종가** × (1 + 4.3%) → 전량 매도
   - 핵심: 평단가가 아닌 마지막 매수일 종가 기준!
2. **시간청산**: 18거래일 무매도 시 강제 전량 매도
3. **매도일 매수 금지**: 매도 발생일에는 매수 없음

#### 파라미터
| 파라미터 | 기본값 | 설명 |
|---------|-------|------|
| 첫매수비율 | 33% | 사이클 시작 시 투입 비율 |
| 매수1 | -1.8% | 추가매수 Level 1 (전일종가 대비) |
| 매수2 | -10% | 추가매수 Level 2 (전일종가 대비) |
| 매도 | +4.3% | 마지막매수일 종가 대비 익절 |
| 시간청산 | 18일 | 강제 청산 거래일 |
| 분할수 | 8 | 추가매수 1회 = 투자금/8 |
| 매수제한 | 7회 | 추가매수 최대 횟수 |
""")
    with right:
        st.info("""
**장점**
- V자 반등에서 후기 매수금이 커져 유연한 대응
- 회전율이 빨라 하락장에서도 짧게 치고 빠짐
- 최소 30% 현금 유지로 리스크 관리
""")
        st.warning("""
**단점**
- 변동성이 작아지면 수익 감소
- 시간청산 발동 시 큰 손실 가능
- 연속 손절 사이클 누적 위험
""")
        st.info("""
**QQQ 추세 기능 (옵션)**

QQQ 주간종가에 GROWTH 함수(지수회귀)로 추세선 계산.
이격도에 따라 첫매수비율 조정:
초고평가(20%) ~ 초저평가(40%)
""")

    st.divider()

    # ── 성과 분석 ──
    st.subheader("📊 전략 성과 분석")

    # ── 파라미터 소스 선택 (사이드바 or 프리셋) ──
    _IUO_INTRO_PRESETS = [
        {"label": "🚀 공격형", "first_buy_ratio": 40, "buy1_pct": -1.0, "buy2_pct": -11.0,
         "sell_pct": 6.0, "moc_days": 24, "max_add_buys": 7, "divisions": 6},
        {"label": "⚖️ 균형형", "first_buy_ratio": 35, "buy1_pct": -1.0, "buy2_pct": -11.0,
         "sell_pct": 6.0, "moc_days": 24, "max_add_buys": 7, "divisions": 6},
        {"label": "🛡️ 안정형", "first_buy_ratio": 25, "buy1_pct": -1.0, "buy2_pct": -11.0,
         "sell_pct": 6.0, "moc_days": 24, "max_add_buys": 7, "divisions": 6},
    ]
    _src_options = ["📐 사이드바 설정값"] + [pr["label"] for pr in _IUO_INTRO_PRESETS]
    _src_sel = st.radio("파라미터 소스", _src_options, index=0, horizontal=True,
                        key="iuo_intro_param_src")
    _src_idx = _src_options.index(_src_sel)

    if _src_idx == 0:
        _use_p = dict(params)
    else:
        _pr = _IUO_INTRO_PRESETS[_src_idx - 1]
        _use_p = dict(params)
        _use_p["first_buy_ratio"] = _pr["first_buy_ratio"] / 100
        _use_p["buy1_pct"] = _pr["buy1_pct"] / 100
        _use_p["buy2_pct"] = _pr["buy2_pct"] / 100
        _use_p["sell_pct"] = _pr["sell_pct"] / 100
        _use_p["moc_days"] = _pr["moc_days"]
        _use_p["max_additional_buys"] = _pr["max_add_buys"]
        _use_p["divisions"] = _pr["divisions"]

    with st.expander("🔍 적용 파라미터 확인", expanded=False):
        st.markdown(
            f"첫매수 **{_use_p['first_buy_ratio']*100:.0f}%** · "
            f"매수1 **{_use_p['buy1_pct']*100:.1f}%** · "
            f"매수2 **{_use_p['buy2_pct']*100:.1f}%** · "
            f"매도 **{_use_p['sell_pct']*100:.1f}%** · "
            f"MOC **{_use_p['moc_days']}일** · "
            f"매수제한 **{_use_p['max_additional_buys']}회** · "
            f"분할 **{_use_p['divisions']}**")
        st.caption(
            f"기간 {_use_p['bt_start_date']} ~ {_use_p['bt_end_date']} · "
            f"자본 ${_use_p['bt_initial_capital']:,.0f}")

    _intro_ss_key = f"iuo_intro_result_{_src_idx}"
    if st.button("▶ 성과 분석 실행", type="primary", key="iuo_run_intro_perf"):
        with st.spinner("데이터 로드 및 분석 중..."):
            try:
                _intro_price = _download_ticker(_use_p["bt_ticker"])
            except Exception as _e:
                st.error(f"⚠️ 데이터 로드 실패: {_e}")
                return
            _intro_params = IUOParams(
                initial_capital=_use_p["bt_initial_capital"],
                first_buy_ratio=_use_p["first_buy_ratio"],
                buy0_pct=_use_p["buy0_pct"],
                buy1_pct=_use_p["buy1_pct"],
                buy2_pct=_use_p["buy2_pct"],
                sell_pct=_use_p["sell_pct"],
                moc_days=_use_p["moc_days"],
                max_additional_buys=_use_p["max_additional_buys"],
                divisions=_use_p["divisions"],
            )
            _intro_result = run_backtest(
                _intro_params, _intro_price, None,
                str(_use_p["bt_start_date"]), str(_use_p["bt_end_date"]),
            )
            if _intro_result:
                st.session_state[_intro_ss_key] = _intro_result
            else:
                st.warning("백테스트 결과가 없습니다.")
                return

    result = st.session_state.get(_intro_ss_key)
    if result is None:
        st.info("💡 사이드바 설정값 또는 추천 프리셋을 선택하여 '▶ 성과 분석 실행' 버튼을 눌러주세요.")
        return

    log = result["daily_log"]
    sells = result["sell_records"]
    m = result["metrics"]
    cap = _use_p["bt_initial_capital"]
    ticker = _use_p["bt_ticker"]
    df = pd.DataFrame(log)
    df["날짜"] = pd.to_datetime(df["날짜"])
    assets = df["총자산"].values.astype(float)
    dates = df["날짜"]

    # ── 핵심 지표 카드 (6열) ──
    sm1, sm2, sm3, sm4, sm5, sm6 = st.columns(6)
    sm1.metric("CAGR", f"{m['CAGR(%)']:.2f}%")
    sm2.metric("총 수익률", f"{(m['최종자산'] / cap - 1) * 100:+.2f}%")
    sm3.metric("최대 MDD", f"{m['MDD(%)']:.2f}%")
    sm4.metric("Calmar Ratio", f"{m['Calmar']:.3f}")
    sm5.metric("Sharpe Ratio", f"{m['Sharpe']:.3f}")
    sm6.metric("Sortino Ratio", f"{m['Sortino']:.3f}")
    st.divider()

    # ── 연도별 성과 ──
    st.subheader("📅 연도별 성과")
    df["연도"] = df["날짜"].dt.year
    yearly = []
    for year, grp in df.groupby("연도"):
        first_asset = grp.iloc[0]["총자산"]
        last_asset = grp.iloc[-1]["총자산"]
        ret = (last_asset / first_asset - 1) * 100
        mdd_y = grp["DD(%)"].min()
        sells_y = [s for s in sells if s["매도일"].startswith(str(year))]
        wins_y = sum(1 for s in sells_y if s["실현손익"] > 0)
        total_y = len(sells_y)
        wr_y = (wins_y / total_y * 100) if total_y > 0 else 0
        yearly.append({
            "연도": year, "수익률(%)": round(ret, 2), "MDD(%)": round(mdd_y, 2),
            "매도건수": total_y, "승률(%)": round(wr_y, 1),
        })
    yearly_df = pd.DataFrame(yearly)

    def _color_ret(val):
        if isinstance(val, (int, float)):
            if val > 0:
                return "color: #2e7d32; font-weight:bold"
            if val < 0:
                return "color: #c62828; font-weight:bold"
        return ""
    st.dataframe(
        yearly_df.style.map(_color_ret, subset=["수익률(%)"])
                       .format({"수익률(%)": "{:+.2f}%", "MDD(%)": "{:.2f}%"}),
        hide_index=True, use_container_width=True)
    st.divider()

    # ── 월별 히트맵 ──
    st.subheader("🗓️ 월별 수익률 히트맵")
    df["월"] = df["날짜"].dt.month
    monthly = []
    prev_asset = cap
    for ym, grp in df.groupby(df["날짜"].dt.to_period("M")):
        end_a = float(grp.iloc[-1]["총자산"])
        ret = (end_a / prev_asset - 1) * 100
        monthly.append({"Year": ym.year, "Month": ym.month, "Return": round(ret, 2)})
        prev_asset = end_a
    if monthly:
        m_df = pd.DataFrame(monthly)
        month_kr = {1:"1월",2:"2월",3:"3월",4:"4월",5:"5월",6:"6월",
                    7:"7월",8:"8월",9:"9월",10:"10월",11:"11월",12:"12월"}
        pivot = m_df.pivot(index="Year", columns="Month", values="Return")
        pivot.columns = [month_kr.get(c, c) for c in pivot.columns]
        fig_heat = px.imshow(
            pivot, text_auto=".1f", color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0, aspect="auto",
            labels={"x": "월", "y": "연도", "color": "수익률(%)"},
        )
        fig_heat.update_layout(height=max(320, len(pivot) * 38 + 120),
                               coloraxis_colorbar=dict(title="수익률(%)"))
        st.plotly_chart(fig_heat, use_container_width=True)
    st.divider()

    # ── 종합 성과 요약 ──
    st.subheader("📋 종합 성과 요약")
    sc, wc = m["총매도횟수"], m["익절"]
    sell_pnls = [s["손익률(%)"] for s in sells] if sells else []
    st.dataframe(pd.DataFrame({
        "항목": ["시작 자본", "최종 자산", "총 수익률", "CAGR (연복리)",
                 "MDD", "Calmar Ratio", "총 매도 횟수", "승률",
                 "평균 손익률", "최대 단일 수익", "최대 단일 손실"],
        "수치": [
            f"${cap:,.0f}", f"${m['최종자산']:,.0f}",
            f"{(m['최종자산'] / cap - 1) * 100:+.2f}%", f"{m['CAGR(%)']:.1f}%",
            f"{m['MDD(%)']:.1f}%", f"{m['Calmar']:.3f}",
            f"{sc}회",
            f"{m['승률(%)']:.1f}%  ({wc}승 {sc - wc}패)" if sc > 0 else "-",
            f"{np.mean(sell_pnls):+.2f}%" if sell_pnls else "-",
            f"{max(sell_pnls):+.2f}%" if sell_pnls else "-",
            f"{min(sell_pnls):+.2f}%" if sell_pnls else "-",
        ],
    }), hide_index=True, use_container_width=True)
    st.divider()

    # ── Buy & Hold 비교 ──
    st.subheader("📈 Buy & Hold 비교")
    st.caption("같은 기간 종목을 단순 보유했을 때와 전략 성과를 비교합니다.")
    from common.analysis import compute_bnh, compute_rolling_perf
    try:
        price_df = _download_ticker(ticker)
        bnh_result = compute_bnh(price_df, str(params["bt_start_date"]),
                                 str(params["bt_end_date"]), cap)
        bnh_assets, bnh_dates = (bnh_result if isinstance(bnh_result, tuple)
                                 else (np.array([]), pd.DatetimeIndex([])))
    except Exception:
        bnh_assets, bnh_dates = np.array([]), pd.DatetimeIndex([])

    if len(bnh_assets) > 0:
        fig_bnh = go.Figure()
        fig_bnh.add_trace(go.Scatter(
            x=dates, y=assets.tolist(),
            name="IUO 전략", line=dict(color="#1565C0", width=2),
        ))
        fig_bnh.add_trace(go.Scatter(
            x=bnh_dates, y=bnh_assets.tolist(),
            name="Buy & Hold", line=dict(color="#EF5350", width=2, dash="dot"),
        ))
        fig_bnh.add_hline(y=cap, line_dash="dash", line_color="#aaa",
                          annotation_text="시작 자본")
        strat_ret = (assets[-1] / assets[0] - 1) * 100
        bnh_ret = (bnh_assets[-1] / bnh_assets[0] - 1) * 100
        bnh_yrs = (pd.to_datetime(params["bt_end_date"]) -
                   pd.to_datetime(params["bt_start_date"])).days / 365.25
        bnh_cagr = ((bnh_assets[-1] / bnh_assets[0]) ** (1 / bnh_yrs) - 1) * 100 if bnh_yrs > 0 else 0
        fig_bnh.update_layout(
            title=f"전략 vs Buy&Hold │ 전략 {strat_ret:+.1f}% vs B&H {bnh_ret:+.1f}%",
            yaxis_title="자산 ($)", height=380,
            legend=dict(orientation="h", y=1.08),
        )
        st.plotly_chart(fig_bnh, use_container_width=True)
        bc1, bc2, bc3, bc4 = st.columns(4)
        bc1.metric("전략 총수익", f"{strat_ret:+.1f}%")
        bc2.metric("B&H 총수익", f"{bnh_ret:+.1f}%")
        bc3.metric("전략 CAGR", f"{m['CAGR(%)']:.1f}%")
        bc4.metric("B&H CAGR", f"{bnh_cagr:.1f}%")
    st.divider()

    # ── 드로다운 (Underwater) 분석 ──
    st.subheader("🌊 드로다운 (Underwater) 분석")
    st.caption("고점 대비 현재 손실 비율 추이. 얼마나 깊이, 얼마나 오래 손실 구간에 있었는지 보여줍니다.")
    peak_arr = np.maximum.accumulate(assets)
    dd_arr = (assets - peak_arr) / peak_arr * 100
    fig_dd = go.Figure()
    fig_dd.add_trace(go.Scatter(
        x=dates, y=dd_arr.tolist(),
        fill="tozeroy", name="드로다운(%)",
        line=dict(color="#EF5350", width=1),
        fillcolor="rgba(239,83,80,0.25)",
    ))
    fig_dd.add_hline(y=0, line_color="#888", line_width=1)
    fig_dd.update_layout(yaxis_title="드로다운 (%)", height=300,
                         yaxis=dict(tickformat=".1f"))
    st.plotly_chart(fig_dd, use_container_width=True)

    # 드로다운 구간 TOP5
    dd_series = pd.Series(dd_arr, index=dates.values)
    in_dd = False
    dd_start = None
    dd_periods = []
    for di, (ddate, dval) in enumerate(dd_series.items()):
        if dval < 0 and not in_dd:
            in_dd = True
            dd_start = ddate
        elif dval == 0 and in_dd:
            in_dd = False
            sub_dd = dd_series[dd_start:ddate]
            dd_periods.append({
                "시작일": str(pd.Timestamp(dd_start).date()),
                "회복일": str(pd.Timestamp(ddate).date()),
                "기간(일)": (pd.Timestamp(ddate) - pd.Timestamp(dd_start)).days,
                "최대낙폭(%)": round(float(sub_dd.min()), 2),
            })
    if dd_periods:
        dd_df = pd.DataFrame(dd_periods).nsmallest(5, "최대낙폭(%)").reset_index(drop=True)
        dd_df.index += 1
        st.markdown("**Top 5 최대 낙폭 구간**")
        st.dataframe(dd_df.style.format({"최대낙폭(%)": "{:.2f}%"}),
                     hide_index=False, use_container_width=True)
    st.divider()

    # ── 고점 회복력 분석 ──
    st.subheader("🔄 고점 회복력 분석")
    st.caption("고점 대비 10% 이상 하락이 발생한 모든 에피소드와 회복까지 걸린 기간을 정리합니다.")
    rec_records = _compute_recovery_table(assets, dates.values, threshold=10.0)
    if rec_records:
        rec_df = pd.DataFrame(rec_records).reset_index(drop=True)
        rec_df.index += 1
        rec_show = rec_df.copy()
        rec_show["고점 평가액"] = rec_show["고점 평가액"].apply(lambda v: f"${v:,}")
        rec_show["저점 평가액"] = rec_show["저점 평가액"].apply(lambda v: f"${v:,}")
        rec_show["하락율(%)"] = rec_show["하락율(%)"].apply(lambda v: f"{abs(v):.2f}%")
        rec_show["기간(일)"] = rec_show["기간(일)"].apply(
            lambda v: f"{v}일" if isinstance(v, (int, float)) else str(v))

        def _highlight_unrecovered(row):
            return ["background-color: #fff3e0"] * len(row) \
                if row["회복 시점"] == "미회복" else [""] * len(row)
        st.dataframe(rec_show.style.apply(_highlight_unrecovered, axis=1),
                     hide_index=False, use_container_width=True)

        rc1, rc2, rc3, rc4 = st.columns(4)
        completed = [r for r in rec_records if r["회복 시점"] != "미회복"]
        avg_days = int(np.mean([r["기간(일)"] for r in completed])) if completed else 0
        max_days = max([r["기간(일)"] for r in completed], default=0)
        max_drop = min([r["하락율(%)"] for r in rec_records])
        rc1.metric("총 에피소드", f"{len(rec_records)}회")
        rc2.metric("평균 회복 기간", f"{avg_days}일" if completed else "-")
        rc3.metric("최장 회복 기간", f"{max_days}일" if completed else "-")
        rc4.metric("최대 낙폭", f"{abs(max_drop):.2f}%")

        # 회복력 차트
        st.markdown("**📊 고점 회복 구간 시각화**")
        st.caption("노란 음영: 10% 이상 하락 구간 / 초록 점: 고점 / 빨간 점: 저점")
        fig_rec = go.Figure()
        fig_rec.add_trace(go.Scatter(
            x=dates, y=assets.tolist(), name="IUO 전략",
            line=dict(color="#1565C0", width=2),
        ))
        if len(bnh_assets) > 0:
            fig_rec.add_trace(go.Scatter(
                x=bnh_dates, y=bnh_assets.tolist(), name="Buy & Hold",
                line=dict(color="#FB8C00", width=1.5, dash="dot"),
            ))
        date_map = {str(pd.Timestamp(d).date()): i for i, d in enumerate(dates.values)}
        for ep in rec_records:
            xs = ep["고점"]
            xe = ep["회복 시점"] if ep["회복 시점"] != "미회복" else str(dates.iloc[-1].date())
            fig_rec.add_vrect(x0=xs, x1=xe,
                              fillcolor="rgba(255,236,153,0.35)", layer="below", line_width=0)
            pi = date_map.get(xs)
            if pi is not None:
                fig_rec.add_trace(go.Scatter(
                    x=[xs], y=[float(assets[pi])], mode="markers",
                    marker=dict(color="#43A047", size=8), showlegend=False,
                ))
            ti = date_map.get(ep["최대하락 시점"])
            if ti is not None:
                fig_rec.add_trace(go.Scatter(
                    x=[ep["최대하락 시점"]], y=[float(assets[ti])], mode="markers",
                    marker=dict(color="#E53935", size=8), showlegend=False,
                ))
        fig_rec.update_layout(yaxis_title="자산 ($)", height=420,
                              legend=dict(orientation="h", y=1.08), hovermode="x unified")
        st.plotly_chart(fig_rec, use_container_width=True)
    else:
        st.info("분석 기간 중 10% 이상 하락 에피소드가 없습니다.")
    st.divider()

    # ── 롤링 성과 분석 ──
    st.subheader("📉 롤링 성과 분석")
    st.caption("구간별 성과 추이. 특정 시기에만 좋은 게 아닌지 검증합니다.")
    roll_tabs = st.tabs(["1년 롤링", "2년 롤링", "3년 롤링"])
    for rwin, rtab in zip([252, 504, 756], roll_tabs):
        with rtab:
            rc_arr, rm_arr = compute_rolling_perf(assets, rwin)
            valid = ~np.isnan(rc_arr)
            if valid.sum() > 0:
                rdates = dates[valid]
                fig_roll = go.Figure()
                fig_roll.add_trace(go.Scatter(
                    x=rdates, y=rc_arr[valid].tolist(),
                    name="롤링 CAGR(%)", line=dict(color="#1565C0", width=2), yaxis="y1",
                ))
                fig_roll.add_trace(go.Scatter(
                    x=rdates, y=rm_arr[valid].tolist(),
                    name="롤링 MDD(%)", line=dict(color="#EF5350", width=1.5, dash="dot"), yaxis="y2",
                ))
                fig_roll.add_hline(y=0, line_dash="dash", line_color="#aaa", yref="y1")
                fig_roll.update_layout(
                    yaxis=dict(title="롤링 CAGR (%)", side="left"),
                    yaxis2=dict(title="롤링 MDD (%)", side="right", overlaying="y"),
                    legend=dict(orientation="h", y=1.08), height=340,
                )
                st.plotly_chart(fig_roll, use_container_width=True)
                r1, r2, r3 = st.columns(3)
                r1.metric("평균 CAGR", f"{np.nanmean(rc_arr):+.1f}%")
                r2.metric("최고 CAGR", f"{np.nanmax(rc_arr):+.1f}%")
                r3.metric("최저 CAGR", f"{np.nanmin(rc_arr):+.1f}%")
            else:
                st.info(f"분석 기간이 {rwin // 252}년보다 짧아 롤링 분석이 불가합니다.")
    st.divider()

    # ── 매도 손익률 분포 ──
    st.subheader("📊 매도 손익률 분포")
    st.caption("매도 시마다 발생한 손익률의 분포. 수익/손실의 패턴을 분석합니다.")
    if sell_pnls:
        pnl_arr = np.array(sell_pnls)
        skew = float(pd.Series(pnl_arr).skew())
        kurt = float(pd.Series(pnl_arr).kurtosis())
        fig_pnl = go.Figure()
        fig_pnl.add_trace(go.Histogram(
            x=pnl_arr.tolist(), nbinsx=30,
            marker_color=["#EF5350" if v < 0 else "#43A047" for v in pnl_arr],
            name="손익률 빈도",
        ))
        fig_pnl.add_vline(x=0, line_dash="dash", line_color="#333")
        fig_pnl.add_vline(x=float(np.mean(pnl_arr)), line_dash="dot",
                          line_color="#1565C0",
                          annotation_text=f"평균 {np.mean(pnl_arr):+.2f}%",
                          annotation_position="top right")
        fig_pnl.update_layout(xaxis_title="손익률 (%)", yaxis_title="빈도 (회)", height=320)
        st.plotly_chart(fig_pnl, use_container_width=True)
        pd1, pd2, pd3, pd4 = st.columns(4)
        pd1.metric("평균 손익률", f"{np.mean(pnl_arr):+.2f}%")
        pd2.metric("중앙값", f"{np.median(pnl_arr):+.2f}%")
        pd3.metric("왜도 (Skew)", f"{skew:.3f}",
                    help="양수=우측 꼬리(큰 수익 가끔), 음수=좌측 꼬리(큰 손실 가끔)")
        pd4.metric("첨도 (Kurt)", f"{kurt:.3f}",
                    help="높을수록 극단값(큰 수익/손실) 빈도 높음")
    else:
        st.info("매도 이력이 없어 분포 분석이 불가합니다.")
    st.divider()

    # ── 현금 활용률 & 매매 타이밍 ──
    st.subheader("💵 현금 활용률 & 매매 타이밍 패턴")
    inv_ratio = (1 - df["예수금"].values / assets) * 100
    cu1, cu2, cu3 = st.columns(3)
    cu1.metric("평균 투자 비율", f"{np.mean(inv_ratio):.1f}%",
               help="현금이 아닌 주식에 투자된 비율의 평균")
    cu2.metric("최대 투자 비율", f"{np.max(inv_ratio):.1f}%")
    cu3.metric("현금 보유 비율", f"{100 - np.mean(inv_ratio):.1f}%")

    # 스택 영역 (주식 vs 현금)
    ratio_s = pd.Series(inv_ratio, index=dates.values)
    total_days = (pd.Timestamp(ratio_s.index[-1]) - pd.Timestamp(ratio_s.index[0])).days
    win = 5 if total_days > 365 else 0
    trend_s = ratio_s.rolling(win, min_periods=1).mean() if win > 0 else None

    fig_cu = go.Figure()
    fig_cu.add_trace(go.Scatter(
        x=dates, y=[100.0] * len(dates), name="현금",
        mode="lines", line=dict(width=0), fill="tozeroy",
        fillcolor="rgba(200,200,200,0.6)", hoverinfo="skip",
    ))
    fig_cu.add_trace(go.Scatter(
        x=dates, y=inv_ratio.tolist(), name="주식(ETF)",
        mode="lines", line=dict(width=0), fill="tozeroy",
        fillcolor="rgba(255,179,0,0.75)",
        hovertemplate="%{x}<br>주식(ETF): %{y:.1f}%<extra></extra>",
    ))
    if trend_s is not None:
        fig_cu.add_trace(go.Scatter(
            x=dates, y=trend_s.tolist(), name="5일 이동평균 추세선",
            mode="lines", line=dict(color="rgba(180,80,0,0.85)", width=1.5),
            hoverinfo="skip",
        ))
    fig_cu.update_layout(
        yaxis_title="비율 (%)",
        yaxis=dict(range=[0, 100], tickvals=[0, 20, 40, 60, 80, 100],
                   ticktext=["0%", "20%", "40%", "60%", "80%", "100%"]),
        height=300, legend=dict(orientation="h", x=1.0, y=1.02, xanchor="right", yanchor="bottom"),
        hovermode="x unified", margin=dict(l=60, r=20, t=20, b=40),
    )
    st.plotly_chart(fig_cu, use_container_width=True)

    # 요일별 / 월별 매매 빈도
    buy_df = df[df["매수수량"] > 0].copy()
    sell_df = df[df["매도수량"] < 0].copy()
    if not buy_df.empty:
        buy_df["요일"] = buy_df["날짜"].dt.day_name()
        sell_df["요일"] = sell_df["날짜"].dt.day_name()
        buy_df["월num"] = buy_df["날짜"].dt.month
        sell_df["월num"] = sell_df["날짜"].dt.month
        dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        dow_labels = ["월", "화", "수", "목", "금"]
        buy_dow = buy_df["요일"].value_counts().reindex(dow_order, fill_value=0)
        sell_dow = sell_df["요일"].value_counts().reindex(dow_order, fill_value=0)
        fig_dow = go.Figure()
        fig_dow.add_trace(go.Bar(x=dow_labels, y=buy_dow.values.tolist(),
                                 name="매수", marker_color="#EF5350"))
        fig_dow.add_trace(go.Bar(x=dow_labels, y=sell_dow.values.tolist(),
                                 name="매도", marker_color="#43A047"))
        fig_dow.update_layout(barmode="group", title="요일별 매매 빈도",
                              yaxis_title="횟수", height=300)
        buy_mon = buy_df["월num"].value_counts().sort_index()
        sell_mon = sell_df["월num"].value_counts().sort_index()
        fig_mon = go.Figure()
        fig_mon.add_trace(go.Bar(x=[f"{m_}월" for m_ in buy_mon.index],
                                 y=buy_mon.values.tolist(),
                                 name="매수", marker_color="#EF5350"))
        fig_mon.add_trace(go.Bar(x=[f"{m_}월" for m_ in sell_mon.index],
                                 y=sell_mon.values.tolist(),
                                 name="매도", marker_color="#43A047"))
        fig_mon.update_layout(barmode="group", title="월별 매매 빈도",
                              yaxis_title="횟수", height=300)
        tc1, tc2 = st.columns(2)
        with tc1:
            st.plotly_chart(fig_dow, use_container_width=True)
        with tc2:
            st.plotly_chart(fig_mon, use_container_width=True)
    st.divider()

    # ── 파라미터 민감도 히트맵 ──
    st.subheader("🎛️ 파라미터 민감도 분석")
    st.caption("현재 매수1% · 매도% 주변의 Calmar Ratio 분포. 과최적화 여부를 확인합니다.")
    with st.expander("🔍 민감도 히트맵 보기 (클릭하여 실행)", expanded=False):
        n_steps = 5
        b1_center = params["buy1_pct"]
        sp_center = params["sell_pct"]
        b1_range_s = np.linspace(b1_center - 0.01, b1_center + 0.01, n_steps)
        sp_range_s = np.linspace(sp_center - 0.015, sp_center + 0.015, n_steps)
        heat = np.zeros((n_steps, n_steps))
        with st.spinner("민감도 분석 중... (25회 시뮬레이션)"):
            price_df_s = _download_ticker(ticker)
            qqq_df_s = None
            for bi, bv in enumerate(b1_range_s):
                for si, sv in enumerate(sp_range_s):
                    p = IUOParams(
                        initial_capital=cap,
                        first_buy_ratio=params["first_buy_ratio"],
                        buy0_pct=params["buy0_pct"],
                        buy1_pct=bv, buy2_pct=params["buy2_pct"],
                        sell_pct=sv, moc_days=params["moc_days"],
                        max_additional_buys=params["max_additional_buys"],
                        divisions=params["divisions"],
                    )
                    r = run_backtest_fast(p, price_df_s, qqq_df_s,
                                         str(params["bt_start_date"]),
                                         str(params["bt_end_date"]))
                    if r:
                        final = r["final_asset"]
                        days = r["total_days"]
                        mdd_v = r["mdd"] * 100
                        yrs = days / 252
                        cagr_v = ((final / cap) ** (1 / yrs) - 1) * 100 if yrs > 0 else 0
                        heat[bi][si] = abs(cagr_v / mdd_v) if mdd_v != 0 else 0
        b1_labels = [f"{v*100:.2f}%" for v in b1_range_s]
        sp_labels = [f"{v*100:.2f}%" for v in sp_range_s]
        fig_sens = px.imshow(
            heat, x=sp_labels, y=b1_labels,
            color_continuous_scale="RdYlGn",
            labels={"x": "매도%", "y": "매수1%", "color": "Calmar"},
            text_auto=".2f", aspect="auto",
            title="Calmar Ratio 히트맵 (매수1% x 매도%)",
        )
        fig_sens.add_annotation(
            x=f"{sp_center*100:.2f}%", y=f"{b1_center*100:.2f}%",
            text="★ 현재", showarrow=True, arrowhead=2,
            font=dict(color="white", size=13, family="Arial Black"),
        )
        fig_sens.update_layout(height=380)
        st.plotly_chart(fig_sens, use_container_width=True)
        st.caption("녹색일수록 Calmar Ratio가 높습니다. 현재 파라미터(★) 주변이 고르게 녹색이면 과최적화 위험이 낮습니다.")
    st.divider()

    # ── 무작위 기간 강건성 분석 ──
    st.subheader("🎲 무작위 기간 강건성 분석")
    st.caption("1년(252 거래일) 구간 100개를 무작위 추출하여 백테스트를 반복합니다. "
               "시작 시점과 무관하게 전략이 일관된 성과를 내는지 확인합니다.")
    with st.expander("🔍 강건성 분석 실행 (클릭)", expanded=False):
        if st.button("▶ 무작위 100구간 분석 시작", key="iuo_mc_run"):
            st.session_state["iuo_mc_result"] = None
            with st.spinner("전체 가격 데이터 로드 중..."):
                mc_pdf = _download_ticker(ticker)
            mc_closes = mc_pdf["Close"].dropna()
            mc_idx = mc_closes.index
            WINDOW = 252
            mc_valid = [i for i in range(len(mc_idx) - WINDOW)]
            if len(mc_valid) < 100:
                st.warning("데이터가 100구간 분석에 충분하지 않습니다.")
            else:
                import random as _rand
                _rand.seed(None)
                mc_chosen = _rand.sample(mc_valid, 100)
                mc_strat_ret, mc_strat_mdd = [], []
                mc_bnh_ret, mc_bnh_mdd = [], []
                mc_prog = st.progress(0, text="시뮬레이션 중...")
                for ci, si in enumerate(mc_chosen):
                    s_dt = str(mc_idx[si].date())
                    e_dt = str(mc_idx[si + WINDOW - 1].date())
                    p = IUOParams(
                        initial_capital=cap,
                        first_buy_ratio=_use_p["first_buy_ratio"],
                        buy0_pct=_use_p["buy0_pct"],
                        buy1_pct=_use_p["buy1_pct"],
                        buy2_pct=_use_p["buy2_pct"],
                        sell_pct=_use_p["sell_pct"],
                        moc_days=_use_p["moc_days"],
                        max_additional_buys=_use_p["max_additional_buys"],
                        divisions=_use_p["divisions"],
                    )
                    r = run_backtest_fast(p, mc_pdf, None, s_dt, e_dt)
                    if r:
                        mc_strat_ret.append(round((r["final_asset"] / cap - 1) * 100, 2))
                        mdd_v = abs(r["mdd"] * 100)
                        mc_strat_mdd.append(round(mdd_v, 2))
                        ba, _ = compute_bnh(mc_pdf, s_dt, e_dt, cap)
                        if len(ba) > 0:
                            bnh_tr = (ba[-1] / ba[0] - 1) * 100
                            bnh_pk = np.maximum.accumulate(ba)
                            bnh_md = abs(float(((ba - bnh_pk) / bnh_pk).min())) * 100
                            mc_bnh_ret.append(round(bnh_tr, 2))
                            mc_bnh_mdd.append(round(bnh_md, 2))
                    mc_prog.progress((ci + 1) / 100, text=f"시뮬레이션 중... {ci+1}/100")
                mc_prog.empty()
                st.session_state["iuo_mc_result"] = {
                    "strat_ret": mc_strat_ret, "strat_mdd": mc_strat_mdd,
                    "bnh_ret": mc_bnh_ret, "bnh_mdd": mc_bnh_mdd,
                }

        mc_res = st.session_state.get("iuo_mc_result")
        if mc_res:
            sr_arr = np.array(mc_res["strat_ret"])
            sm_arr = np.array(mc_res["strat_mdd"])
            br_arr = np.array(mc_res["bnh_ret"]) if mc_res["bnh_ret"] else None

            def _mc_stats(arr, label):
                return {
                    "구분": label, "평균": f"{np.mean(arr):+.1f}%",
                    "중앙값": f"{np.median(arr):+.1f}%", "표준편차": f"{np.std(arr):.1f}%",
                    "최솟값": f"{np.min(arr):+.1f}%", "최댓값": f"{np.max(arr):+.1f}%",
                    "양(+) 비율": f"{(arr > 0).sum() / len(arr) * 100:.0f}%",
                }
            stat_rows = [_mc_stats(sr_arr, "IUO 전략 (1년 수익률)")]
            if br_arr is not None:
                stat_rows.append(_mc_stats(br_arr, f"{ticker} B&H (1년 수익률)"))
            stat_rows.append({
                "구분": "IUO 전략 (MDD)", "평균": f"{np.mean(sm_arr):.1f}%",
                "중앙값": f"{np.median(sm_arr):.1f}%", "표준편차": f"{np.std(sm_arr):.1f}%",
                "최솟값": f"{np.min(sm_arr):.1f}%", "최댓값": f"{np.max(sm_arr):.1f}%",
                "양(+) 비율": "-",
            })
            st.dataframe(pd.DataFrame(stat_rows), hide_index=True, use_container_width=True)

            # 히스토그램 + 박스플롯
            fig_mc = make_subplots(rows=1, cols=2, horizontal_spacing=0.12,
                                   subplot_titles=["1년 수익률 분포", "MDD 분포"])
            fig_mc.add_trace(go.Histogram(x=sr_arr.tolist(), name="IUO 전략",
                                          marker_color="#1565C0", opacity=0.7), row=1, col=1)
            if br_arr is not None:
                fig_mc.add_trace(go.Histogram(x=br_arr.tolist(), name="B&H",
                                              marker_color="#EF5350", opacity=0.5), row=1, col=1)
            bm_arr = np.array(mc_res["bnh_mdd"]) if mc_res["bnh_mdd"] else None
            fig_mc.add_trace(go.Box(y=sm_arr.tolist(), name="IUO MDD",
                                    marker_color="#1565C0"), row=1, col=2)
            if bm_arr is not None:
                fig_mc.add_trace(go.Box(y=bm_arr.tolist(), name="B&H MDD",
                                        marker_color="#EF5350"), row=1, col=2)
            fig_mc.update_layout(height=400, showlegend=True,
                                 margin=dict(t=50),
                                 legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"))
            fig_mc.update_xaxes(title_text="수익률 (%)", row=1, col=1)
            fig_mc.update_yaxes(title_text="빈도", row=1, col=1)
            fig_mc.update_yaxes(title_text="MDD (%)", row=1, col=2)
            st.plotly_chart(fig_mc, use_container_width=True)

            # 100구간 상세 데이터 테이블
            with st.expander("📋 100구간 상세 데이터", expanded=False):
                mc_detail_rows = []
                for i in range(len(sr_arr)):
                    row = {"#": i + 1, "IUO 수익률": f"{sr_arr[i]:+.2f}%",
                           "IUO MDD": f"{sm_arr[i]:.2f}%"}
                    if br_arr is not None and i < len(br_arr):
                        row["B&H 수익률"] = f"{br_arr[i]:+.2f}%"
                    if bm_arr is not None and i < len(bm_arr):
                        row["B&H MDD"] = f"{bm_arr[i]:.2f}%"
                    if br_arr is not None and i < len(br_arr):
                        row["초과수익"] = f"{sr_arr[i] - br_arr[i]:+.2f}%"
                    mc_detail_rows.append(row)
                mc_detail_df = pd.DataFrame(mc_detail_rows)

                def _style_mc_row(row):
                    val = float(str(row.get("IUO 수익률", "0")).replace("%", "").replace("+", ""))
                    if val < 0:
                        return ["background-color: #FFF0F0; color: #C62828"] * len(row)
                    return [""] * len(row)

                st.dataframe(mc_detail_df.style.apply(_style_mc_row, axis=1),
                             use_container_width=True, hide_index=True, height=400)
    st.divider()

    # ── 전략 인사이트 ──
    st.subheader("💡 전략 인사이트")
    avg_pnl = np.mean(sell_pnls) if sell_pnls else 0
    win_rate = m["승률(%)"]
    insights = []
    if win_rate >= 70:
        insights.append(f"승률 {win_rate:.1f}%로 높은 편입니다. 사이클 매매의 특성상 빈번한 소액 익절이 누적됩니다.")
    elif win_rate >= 50:
        insights.append(f"승률 {win_rate:.1f}%로 보통 수준입니다.")
    else:
        insights.append(f"승률 {win_rate:.1f}%로 다소 낮습니다. 시간청산 비중이 높을 수 있습니다.")
    if m["Calmar"] > 1.0:
        insights.append(f"Calmar {m['Calmar']:.2f}로 위험 대비 수익이 양호합니다.")
    else:
        insights.append(f"Calmar {m['Calmar']:.2f}로 MDD 대비 수익이 낮은 편입니다. 파라미터 조정을 검토하세요.")
    if avg_pnl > 0:
        insights.append(f"평균 매도 손익률 {avg_pnl:+.2f}%입니다.")
    cash_ratio = m["평균현금비율(%)"]
    insights.append(f"평균 현금비율 {cash_ratio:.1f}% — {'안정적인 현금 관리' if cash_ratio > 30 else '적극적인 투자 비중'}입니다.")
    for ins in insights:
        st.markdown(f"- {ins}")


# ══════════════════════════════════════════════
# 탭5: DB 조회
# ══════════════════════════════════════════════

def render_db_tab(params: dict):
    st.subheader("📂 일별 매매 로그 조회")

    result = st.session_state.get("iuo_bt_result")
    if result is None:
        st.info("백테스트를 실행하면 일별 로그를 조회할 수 있습니다.")
        return

    log = result["daily_log"]
    df = pd.DataFrame(log)
    df["날짜"] = pd.to_datetime(df["날짜"])

    # 필터
    col1, col2 = st.columns(2)
    filter_type = col1.selectbox("필터", ["전체", "매수일만", "매도일만", "보유 중"],
                                 key="iuo_db_filter")
    if filter_type == "매수일만":
        df = df[df["매수수량"] > 0]
    elif filter_type == "매도일만":
        df = df[df["매도수량"] < 0]
    elif filter_type == "보유 중":
        df = df[df["보유수량"] > 0]

    st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption(f"총 {len(df)}행")

    # QQQ 추세 차트
    if params.get("use_qqq_trend"):
        st.markdown("---")
        st.subheader("QQQ 추세 & 이격도")
        try:
            qqq_df = _download_qqq()
            growth_df = calc_qqq_weekly_growth(qqq_df, params.get("trend_period", "260"))
            growth_df = growth_df.dropna(subset=["trend"])

            fig_qqq = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                    row_heights=[0.6, 0.4],
                                    vertical_spacing=0.05)
            fig_qqq.add_trace(go.Scatter(
                x=growth_df["week_end"], y=growth_df["close"],
                mode="lines", name="QQQ 종가",
            ), row=1, col=1)
            fig_qqq.add_trace(go.Scatter(
                x=growth_df["week_end"], y=growth_df["trend"],
                mode="lines", name="GROWTH 추세선", line=dict(dash="dash"),
            ), row=1, col=1)
            fig_qqq.add_trace(go.Scatter(
                x=growth_df["week_end"],
                y=[d * 100 if d is not None else None for d in growth_df["deviation"]],
                mode="lines", name="이격도 (%)", fill="tozeroy",
            ), row=2, col=1)
            fig_qqq.update_layout(height=500, margin=dict(l=0, r=0, t=30, b=0),
                                  hovermode="x unified")
            st.plotly_chart(fig_qqq, use_container_width=True)
        except Exception as e:
            st.warning(f"QQQ 추세 차트 로드 실패: {e}")


# ══════════════════════════════════════════════
# 탭6: 개인설정
# ══════════════════════════════════════════════

def render_settings_tab():
    st.subheader("⚙️ IUO 매매법 개인 설정")

    cfg = _load_iuo_config()

    # ── 텔레그램 알림 설정 ──
    with st.container(border=True):
        st.markdown("##### 💬 텔레그램 알림 설정")

        with st.popover("❓ 텔레그램 설정 방법"):
            from common.telegram import render_telegram_help_popover
            render_telegram_help_popover(
                strategy_name="IUO",
                example_bot_display="IUO 알림봇",
                example_bot_username="iuo_alert_bot",
                example_bot_username2="my_iuo_bot",
                test_button_label="📨 주문표 테스트 발송",
            )

        tg_token = st.text_input("Bot Token", value=cfg.get("tg_token", ""),
                                 type="password", key="iuo_set_token")
        tg_chat_id = st.text_input("Chat ID", value=cfg.get("tg_chat_id", ""),
                                   key="iuo_set_chatid")
        st.caption("매주 월~금 오후 3시에 자동 발송됩니다 (GitHub Actions)")

        col_tg1, col_tg2 = st.columns(2)
        with col_tg1:
            if st.button("📨 주문표 테스트 발송", key="iuo_test_tg", use_container_width=True):
                if tg_token and tg_chat_id:
                    tickers = [k for k in cfg if isinstance(cfg[k], dict)]
                    if not tickers:
                        tickers = ["SOXL"]
                    msgs = []
                    for tk in tickers:
                        try:
                            pdf = _download_ticker(tk)
                            lc = round(float(pdf.iloc[-1]["Close"]), 2)
                            ld = pdf.index[-1].strftime("%Y-%m-%d")
                            tk_cfg = cfg.get(tk, {})
                            b0 = float(tk_cfg.get("buy0_pct", 0)) / 100
                            b1 = float(tk_cfg.get("buy1_pct", -1.8)) / 100
                            b2 = float(tk_cfg.get("buy2_pct", -10)) / 100
                            msg = (
                                f"<b>📊 IUO 매매법 — {tk} 주문표</b>\n"
                                f"📅 {_next_trading_date().strftime('%Y-%m-%d')}\n\n"
                                f"전일종가: <b>${lc:,.2f}</b> ({ld})\n"
                                f"첫매수 LOC: ${round(lc*(1+b0),2):,.2f}\n"
                                f"추가매수1 LOC: ${round(lc*(1+b1),2):,.2f}\n"
                                f"추가매수2 LOC: ${round(lc*(1+b2),2):,.2f}"
                            )
                            msgs.append(msg)
                        except Exception:
                            pass
                    if msgs:
                        resp = _send_telegram(tg_token, tg_chat_id, "\n\n".join(msgs))
                        if resp.get("ok"):
                            st.success("테스트 발송 성공!")
                        else:
                            st.error(f"발송 실패: {resp.get('description')}")
                    else:
                        st.warning("발송할 종목이 없습니다.")
                else:
                    st.warning("Bot Token과 Chat ID를 입력하세요.")
        with col_tg2:
            if st.button("💾 저장하기", key="iuo_save_tg", use_container_width=True):
                cfg["tg_token"] = tg_token
                cfg["tg_chat_id"] = tg_chat_id
                _save_iuo_config(cfg)
                st.success("텔레그램 설정이 저장되었습니다.")

    # ── 구글 스프레드시트 연동 ──
    with st.container(border=True):
        st.markdown("##### 🗂️ 구글 스프레드시트 연동")

        with st.popover("❓ 구글시트 설정 방법"):
            st.markdown("""
**1단계**: Google Sheets에서 새 스프레드시트를 만드세요.

**2단계**: 스프레드시트 URL을 복사하세요.
- 주소창의 `https://docs.google.com/spreadsheets/d/...` 전체를 복사

**3단계**: 스프레드시트를 서비스 계정과 공유하세요.
- 공유 버튼 → 이메일 입력:
`connectspreadsheet@sodium-gateway-485307-f3.iam.gserviceaccount.com`
- 권한: **편집자**

**4단계**: 아래에 URL을 붙여넣고 저장하세요.
""")

        gs_url = st.text_input("스프레드시트 URL", value=cfg.get("gs_url", ""),
                               key="iuo_gs_url",
                               placeholder="https://docs.google.com/spreadsheets/d/...")

        # 종목별 시트 매핑
        tickers_cfg = [k for k in cfg if isinstance(cfg[k], dict)]
        if tickers_cfg:
            st.markdown("**종목별 시트명 매핑**")
            sheet_map = {}
            for tk in tickers_cfg:
                default_sheet = cfg.get(f"gs_sheet_{tk}", f"iuo_{tk}_매매기록")
                sheet_map[tk] = st.text_input(f"{tk} 시트명", value=default_sheet,
                                              key=f"iuo_gs_sheet_{tk}")
        else:
            st.info("등록된 종목이 없습니다. 주문표 탭에서 먼저 계좌를 등록하세요.")
            sheet_map = {}

        col_gs1, col_gs2 = st.columns(2)
        with col_gs1:
            if st.button("🔗 시트 연결 테스트", key="iuo_gs_test", use_container_width=True):
                if gs_url:
                    try:
                        from common.config import _get_gspread_client
                        gc = _get_gspread_client()
                        wb = gc.open_by_url(gs_url)
                        st.success(f"연결 성공! 시트 수: {len(wb.worksheets())}")
                    except Exception as e:
                        st.error(f"연결 실패: {e}")
                else:
                    st.warning("URL을 입력하세요.")
        with col_gs2:
            if st.button("💾 저장하기", key="iuo_save_gs", use_container_width=True):
                cfg["gs_url"] = gs_url
                for tk, sn in sheet_map.items():
                    cfg[f"gs_sheet_{tk}"] = sn
                _save_iuo_config(cfg)
                st.success("구글시트 설정이 저장되었습니다.")

    # ── 등록된 계좌 목록 ──
    with st.container(border=True):
        st.markdown("##### 📋 등록된 계좌")
        tickers = [k for k in cfg if isinstance(cfg[k], dict)]
        if tickers:
            for tk in tickers:
                with st.expander(f"📌 {tk}"):
                    tk_cfg = cfg[tk]
                    st.json(tk_cfg)
                    if st.button(f"🗑️ {tk} 삭제", key=f"iuo_del_{tk}"):
                        del cfg[tk]
                        _save_iuo_config(cfg)
                        st.success(f"{tk} 계좌가 삭제되었습니다.")
                        st.rerun()
        else:
            st.caption("등록된 계좌가 없습니다. 주문표 탭에서 파라미터를 저장하세요.")

    # ── 추천 프리셋 ──
    with st.container(border=True):
        st.markdown("##### 🎯 추천 프리셋")
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📌 기본 (IUO 원본)", key="iuo_preset_default", use_container_width=True):
                st.session_state["iuo_fbr"] = 33.0
                st.session_state["iuo_b0"] = 0.0
                st.session_state["iuo_b1"] = -1.8
                st.session_state["iuo_b2"] = -10.0
                st.session_state["iuo_sp"] = 4.3
                st.session_state["iuo_moc"] = 18
                st.session_state["iuo_maxb"] = 7
                st.session_state["iuo_div"] = 8
                st.success("기본 프리셋 적용!")
                st.rerun()
        with col2:
            if st.button("📌 공격형", key="iuo_preset_agg", use_container_width=True):
                st.session_state["iuo_fbr"] = 40.0
                st.session_state["iuo_b0"] = 0.0
                st.session_state["iuo_b1"] = -1.5
                st.session_state["iuo_b2"] = -8.0
                st.session_state["iuo_sp"] = 3.5
                st.session_state["iuo_moc"] = 15
                st.session_state["iuo_maxb"] = 7
                st.session_state["iuo_div"] = 6
                st.success("공격형 프리셋 적용!")
                st.rerun()
        with col3:
            if st.button("📌 안정형", key="iuo_preset_safe", use_container_width=True):
                st.session_state["iuo_fbr"] = 25.0
                st.session_state["iuo_b0"] = 0.0
                st.session_state["iuo_b1"] = -2.0
                st.session_state["iuo_b2"] = -12.0
                st.session_state["iuo_sp"] = 5.0
                st.session_state["iuo_moc"] = 20
                st.session_state["iuo_maxb"] = 7
                st.session_state["iuo_div"] = 10
                st.success("안정형 프리셋 적용!")
                st.rerun()

    # ── 관리자 도구 ──
    with st.expander("🔧 관리자 도구 — 비밀번호 해시 생성"):
        st.caption("새 사용자 추가 시 bcrypt 해시 생성용")
        new_pw = st.text_input("비밀번호 입력", type="password", key="iuo_admin_pw")
        if st.button("🔑 해시 생성", key="iuo_gen_hash"):
            if new_pw:
                try:
                    from common.auth import _hash_password
                    hashed = _hash_password(new_pw)
                    st.code(hashed, language=None)
                    st.caption("위 해시를 users 시트 password_hash 컬럼에 붙여넣으세요.")
                except Exception as e:
                    st.error(f"해시 생성 실패: {e}")
            else:
                st.warning("비밀번호를 입력하세요.")
