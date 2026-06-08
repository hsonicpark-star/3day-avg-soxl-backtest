"""
strategies/dual_sniper.py — Dual Sniper Pro 전략 모듈

종목: SOXL (3x 레버리지 ETF)
구조: 공격/방어 2모드 슬롯(티어) 기반 그리드 매매 (스프레드시트 역설계, 엔진 검증 완료)
모드: 주간 wRSI 기반 자체 하이브리드 규칙 (원전략 전환규칙은 비공개) + 수동 오버라이드

인터페이스: DSS 패턴 (자체 사이드바)
- render_sidebar() → params dict
- render_backtest_tab(params)
- render_optimization_tab(params)
- render_ordersheet_tab(params)
- render_intro_tab(params)  — 통합앱은 render_intro_tab() 호출
- render_db_tab(params)
- render_settings_tab()
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import json
import os
import sys
import itertools
from datetime import datetime, timedelta

_root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _root_dir not in sys.path:
    sys.path.insert(0, _root_dir)

from dual_sniper_engine import (
    DualSniperParams, run_backtest, compute_metrics,
    load_price_data, build_auto_mode_map,
)
from common.config import _IS_CLOUD

# ──────────────────────────────────────────────
# 설정 경로 (로컬: ~/.dual_sniper/)
# ──────────────────────────────────────────────
_DS_CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".dual_sniper")
_DS_CONFIG_PATH = os.path.join(_DS_CONFIG_DIR, "config.json")


def _load_ds_config() -> dict:
    if _IS_CLOUD and st.session_state.get("logged_in"):
        raw = st.session_state.get("user_settings", {}).get("dual_sniper_config", "")
        if raw:
            try:
                cfg = json.loads(raw) if isinstance(raw, str) else raw
                return cfg if isinstance(cfg, dict) else {}
            except Exception:
                pass
        return {}
    if os.path.exists(_DS_CONFIG_PATH):
        try:
            with open(_DS_CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_ds_config(cfg: dict):
    try:
        os.makedirs(_DS_CONFIG_DIR, exist_ok=True)
        with open(_DS_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            cfg_json = json.dumps(cfg, ensure_ascii=False)
            st.session_state.setdefault("user_settings", {})["dual_sniper_config"] = cfg_json
            from common.auth import _save_user_settings_to_sheet
            _save_user_settings_to_sheet(st.session_state.username,
                                         {"dual_sniper_config": cfg_json})
        except Exception:
            pass


# ──────────────────────────────────────────────
# 데이터 캐싱 (auto_adjust=False raw — 검증과 일치)
# ──────────────────────────────────────────────

@st.cache_data(show_spinner="SOXL 데이터 로딩...", ttl=300)
def get_soxl_data():
    df = load_price_data("SOXL", "2009-01-01", "2027-01-01")
    if df is None or df.empty:
        raise RuntimeError("SOXL 데이터 로드 실패 (yfinance)")
    return df


def clear_ds_data_cache():
    try:
        get_soxl_data.clear()
    except Exception:
        pass


# ──────────────────────────────────────────────
# 모드맵 (자동 하이브리드)
# ──────────────────────────────────────────────

@st.cache_data(show_spinner="모드 계산 중...", ttl=300)
def get_auto_mode_map(_price_hash, ma_weeks, peak_thr, dn):
    px_df = get_soxl_data()
    return build_auto_mode_map(px_df, ma_weeks=ma_weeks, peak_thr=peak_thr, dn=dn)


# ══════════════════════════════════════════════
# 정규화 표 (공격모드 자세히)
# ══════════════════════════════════════════════

def _render_norm_tables(hold_alpha, sell_alpha, buy_pct):
    """공격모드 보유기간/매도조건 정규화 표 + FI 매수 분기 규칙."""
    rsis = [70, 65, 60, 55, 50, 45, 40, 35]
    def hold(r):
        x = max(0.0, min(1.0, (r - 35) / 30))
        return round(7 + 23 * (1 - x) ** (1 / hold_alpha))
    def sellp(r):
        x = max(0.0, min(1.0, (r - 35) / 30))
        return round(0.1 + 2.9 * (1 - x) ** (1 / sell_alpha), 2)
    st.markdown(f"**보유기간 정규화 (α={hold_alpha})**")
    st.dataframe(pd.DataFrame({"매수RSI": rsis, "보유일": [hold(r) for r in rsis]}),
                 hide_index=True, use_container_width=True)
    st.caption("보유일 = 7 + 23×(1-x)^(1/α), x=(매수RSI-35)/30")
    st.markdown(f"**매도조건 정규화 (α={sell_alpha})**")
    st.dataframe(pd.DataFrame({"매수RSI": rsis, "매도%": [f"{sellp(r):.2f}%" for r in rsis]}),
                 hide_index=True, use_container_width=True)
    st.caption("매도% = 0.1 + 2.9×(1-x)^(1/α)")
    st.markdown("**매수조건 FI 분기 규칙**")
    st.dataframe(pd.DataFrame({"전일 FI": ["FI > 0", "FI ≤ 0"],
                              "매수주문가": [f"전일종가 +{buy_pct:.1f}%", "전일종가 -0.1% (고정)"]}),
                 hide_index=True, use_container_width=True)


# ══════════════════════════════════════════════
# 사이드바
# ══════════════════════════════════════════════

def render_sidebar():
    cfg = _load_ds_config()
    sp = cfg.get("strategy", {})
    mr = cfg.get("mode_rule", {})

    st.sidebar.markdown("### 🎯 Dual Sniper Pro")
    st.sidebar.caption("SOXL · 공격/방어 2모드 슬롯 그리드")

    # ── 모드 규칙 (자체 하이브리드) ──
    with st.sidebar.expander("🧭 모드 규칙 (자동)", expanded=False):
        st.caption("주봉 > N주MA → 공격, 천장하락/이탈 → 방어")
        ma_weeks = st.number_input("추세 MA(주)", 5, 60, int(mr.get("ma_weeks", 36)), 1, key="ds_ma")
        peak_thr = st.number_input("천장 wRSI", 50.0, 80.0, float(mr.get("peak_thr", 66.0)), 1.0, key="ds_peak")
        dn = st.number_input("방어 이탈 wRSI", 30.0, 50.0, float(mr.get("dn", 42.0)), 1.0, key="ds_dn")
        st.caption("기본값: 36 / 66 / 42 (Calmar 2.64)")

    # ══ 공격모드 (원전략 패널과 동일 레이아웃) ══
    st.sidebar.markdown("#### 🟥 공격모드")
    c1, c2 = st.sidebar.columns(2)
    ag_div = c1.number_input("공격 분할수", 1, 12, int(sp.get("ag_div", 6)), 1, key="ds_agdiv")
    ag_ha = c2.number_input("보유기간(정규화 α)", 0.1, 5.0,
                            float(sp.get("ag_hold_alpha", 2.0)), 0.1, key="ds_agha")
    c3, c4 = st.sidebar.columns(2)
    ag_buy = c3.number_input("매수조건(종가%)", 0.0, 30.0,
                             float(sp.get("ag_buy", 8.0)), 0.5, key="ds_agbuy")
    ag_sa = c4.number_input("매도조건(정규화 α)", 0.1, 3.0,
                            float(sp.get("ag_sell_alpha", 0.4)), 0.1, key="ds_agsa")
    with st.sidebar.expander("📊 자세히 (정규화 표 · FI 규칙)"):
        _render_norm_tables(ag_ha, ag_sa, ag_buy)

    # ══ 방어모드 (원전략 패널과 동일 레이아웃) ══
    st.sidebar.markdown("#### 🟦 방어모드")
    d1, d2 = st.sidebar.columns(2)
    sf_div = d1.number_input("방어 분할수", 1, 12, int(sp.get("sf_div", 5)), 1, key="ds_sfdiv")
    sf_hold = d2.number_input("보유기간", 1, 60, int(sp.get("sf_hold", 8)), 1, key="ds_sfhold")
    e1, e2 = st.sidebar.columns(2)
    sf_b1 = e1.number_input("매수조건1(MA%)", -10.0, 10.0,
                            float(sp.get("sf_buy1", -0.6)), 0.1, key="ds_sfb1")
    sf_b2 = e2.number_input("매수조건2(종가%)", 0.0, 30.0,
                            float(sp.get("sf_buy2", 5.5)), 0.1, key="ds_sfb2")
    st.sidebar.caption("↳ 매수조건 1, 2 중에 낮은 값")
    f1, f2 = st.sidebar.columns(2)
    sf_sell = f1.number_input("매도조건(MA%)", 0.0, 10.0,
                              float(sp.get("sf_sell", 0.7)), 0.1, key="ds_sfsell")
    sf_ma = f2.number_input("MA기준", 2, 10, int(sp.get("sf_ma_base", 3)), 1, key="ds_sfma")
    sf_w = st.sidebar.text_input("티어별 매수비중(%)",
                                 sp.get("sf_weights", "6, 13, 20, 27, 34"), key="ds_sfw")
    st.sidebar.caption("↳ 오름차순(등차수열) 권장 · 합계 100")

    st.sidebar.markdown("### ⚙️ 백테스트 설정")
    b1, b2 = st.sidebar.columns(2)
    initial_capital = b1.number_input("초기투자금", 1000, 100000000,
                                      int(cfg.get("initial_capital", 10000)), 1000, key="ds_cap")
    fee_rate = b2.number_input("수수료%", 0.0, 1.0, float(cfg.get("fee_rate", 0.0)),
                               0.001, format="%.3f", key="ds_fee")
    g1, g2 = st.sidebar.columns(2)
    start_date = g1.date_input("투자시작일", value=pd.Timestamp(cfg.get("start_date", "2016-01-04")),
                               key="ds_start")
    end_date = g2.date_input("투자종료일", value=datetime.today().date(), key="ds_end")
    st.sidebar.caption("⚠️ 2011~2015 포함 시 성과 하락(반도체 약세장). 2016 권장.")

    # 티어비중 파싱
    try:
        weights = tuple(float(x.strip()) for x in sf_w.split(",") if x.strip())
        if len(weights) != sf_div:
            weights = tuple([6, 13, 20, 27, 34][:sf_div]) or (100.0,)
    except Exception:
        weights = (6, 13, 20, 27, 34)

    return {
        "bt_ticker": "SOXL",
        "bt_start_date": start_date, "bt_end_date": end_date,
        "bt_initial_capital": initial_capital,
        "initial_capital": initial_capital, "fee_rate": fee_rate,
        "start_date": start_date, "end_date": end_date,
        "ma_weeks": int(ma_weeks), "peak_thr": float(peak_thr), "dn": float(dn),
        "ag_div": int(ag_div), "ag_buy": float(ag_buy),
        "ag_sell_alpha": float(ag_sa), "ag_hold_alpha": float(ag_ha),
        "sf_div": int(sf_div), "sf_hold": int(sf_hold),
        "sf_buy1": float(sf_b1), "sf_buy2": float(sf_b2),
        "sf_sell": float(sf_sell), "sf_ma_base": int(sf_ma),
        "sf_weights": weights, "sf_weights_str": sf_w,
    }


def _make_params(p):
    return DualSniperParams(
        ag_divisions=p["ag_div"], ag_buy_pct=p["ag_buy"],
        ag_sell_alpha=p["ag_sell_alpha"], ag_hold_alpha=p["ag_hold_alpha"],
        sf_divisions=p["sf_div"], sf_buy_pct1=p["sf_buy1"], sf_buy_pct2=p["sf_buy2"],
        sf_sell_pct=p["sf_sell"], sf_ma_base=p["sf_ma_base"], sf_hold=p["sf_hold"],
        sf_tier_weights=p["sf_weights"],
        initial_capital=float(p["initial_capital"]),
        fee_rate=p["fee_rate"] / 100, sec_fee_rate=0.0,
        ag_buy_inclusive=False, sf_buy_inclusive=False,
    )


# ══════════════════════════════════════════════
# 탭1: 백테스트
# ══════════════════════════════════════════════

def render_backtest_tab(params):
    p = params
    st.subheader("📊 Dual Sniper Pro 백테스트")

    if st.button("▶ 백테스트 실행", type="primary", key="ds_bt_run"):
        try:
            px_df = get_soxl_data()
            mode_map = build_auto_mode_map(px_df, ma_weeks=p["ma_weeks"],
                                           peak_thr=p["peak_thr"], dn=p["dn"])
        except Exception as e:
            st.error(f"⚠️ 데이터/모드 로드 실패: {e}")
            return
        ds_p = _make_params(p)
        with st.spinner("백테스트 실행 중..."):
            log, trades = run_backtest(px_df, ds_p, mode_map=mode_map,
                                       start_date=str(p["start_date"]),
                                       return_trades=True)
        log = log[(log['날짜'] >= pd.Timestamp(p["start_date"])) &
                  (log['날짜'] <= pd.Timestamp(p["end_date"]))].reset_index(drop=True)
        st.session_state["ds_bt_log"] = log
        st.session_state["ds_bt_trades"] = trades
        st.session_state["ds_bt_cap"] = float(p["initial_capital"])

    if "ds_bt_log" not in st.session_state:
        st.info("👆 '백테스트 실행' 버튼을 클릭하세요.")
        return

    log = st.session_state["ds_bt_log"]
    trades = st.session_state["ds_bt_trades"]
    cap = st.session_state["ds_bt_cap"]
    if log.empty:
        st.warning("결과가 없습니다. 날짜 범위를 확인하세요.")
        return

    m = compute_metrics(log, trades, cap)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("최종자산", f"${m['최종자산']:,.0f}")
    c2.metric("총수익률", f"{m['총수익률(%)']:.0f}%")
    c3.metric("CAGR", f"{m['CAGR(%)']:.1f}%")
    c4.metric("MDD", f"{m['MDD(%)']:.1f}%")
    c5.metric("Calmar", f"{m['Calmar']:.2f}")
    c6.metric("승률", f"{m['승률(%)']:.1f}%")
    c7, c8, c9, c10 = st.columns(4)
    c7.metric("Sharpe", f"{m['Sharpe']:.2f}")
    c8.metric("Sortino", f"{m['Sortino']:.2f}")
    c9.metric("매도횟수", f"{m['총매도횟수']}회")
    c10.metric("평균보유", f"{m['평균보유일']:.1f}일")

    # ── 차트 (총자산 / 종가-모드 / 일별실현) ──
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.05,
                        row_heights=[0.5, 0.25, 0.25],
                        subplot_titles=("총자산 추이", "SOXL 종가 (모드별)", "일별 실현손익"))
    fig.add_trace(go.Scatter(x=log['날짜'], y=log['총자산'], name='총자산',
                             fill='tozeroy', fillcolor='rgba(0,100,200,0.1)',
                             line=dict(color='royalblue', width=1.5)), row=1, col=1)
    ag = log[log['모드'] == '공격']; sf = log[log['모드'] == '방어']
    fig.add_trace(go.Scatter(x=ag['날짜'], y=ag['종가'], mode='markers', name='공격',
                             marker=dict(color='red', size=3)), row=2, col=1)
    fig.add_trace(go.Scatter(x=sf['날짜'], y=sf['종가'], mode='markers', name='방어',
                             marker=dict(color='royalblue', size=3)), row=2, col=1)
    dpnl = log[log['당일실현'] != 0]
    colors = ['green' if x > 0 else 'red' for x in dpnl['당일실현']]
    fig.add_trace(go.Bar(x=dpnl['날짜'], y=dpnl['당일실현'], name='일별실현',
                         marker_color=colors), row=3, col=1)
    fig.update_layout(height=780, showlegend=True, legend=dict(orientation='h', y=1.02))
    st.plotly_chart(fig, use_container_width=True)

    # ── 연도별 성과 ──
    st.subheader("연도별 성과")
    log2 = log.copy(); log2['연도'] = log2['날짜'].dt.year
    yearly = []
    for yr, g in log2.groupby('연도'):
        ret = (g.iloc[-1]['총자산'] / g.iloc[0]['총자산'] - 1) * 100
        peak = g['총자산'].cummax(); mdd_y = ((g['총자산'] - peak) / peak).min() * 100
        ty = trades[(pd.to_datetime(trades['매도일']).dt.year == yr)] if len(trades) else trades
        wr = (ty['실현손익'] > 0).mean() * 100 if len(ty) else 0
        ag_days = (g['모드'] == '공격').mean() * 100
        yearly.append({'연도': yr, '수익률(%)': round(ret, 1), 'MDD(%)': round(mdd_y, 1),
                       '매도건수': len(ty), '승률(%)': round(wr, 1), '공격일비중(%)': round(ag_days, 0)})
    st.dataframe(pd.DataFrame(yearly), use_container_width=True, hide_index=True)

    with st.expander(f"📋 거래 기록 ({len(trades)}건)"):
        if len(trades):
            td = trades.copy()
            td['매수일'] = pd.to_datetime(td['매수일']).dt.strftime('%Y-%m-%d')
            td['매도일'] = pd.to_datetime(td['매도일']).dt.strftime('%Y-%m-%d')
            st.dataframe(td, use_container_width=True, height=400, hide_index=True)
            st.download_button("📥 거래기록 CSV", td.to_csv(index=False).encode('utf-8-sig'),
                               "dual_sniper_trades.csv", "text/csv", key="ds_dl_tr")

    with st.expander("📋 일별 기록"):
        ld = log.copy(); ld['날짜'] = ld['날짜'].dt.strftime('%Y-%m-%d')
        st.dataframe(ld, use_container_width=True, height=400, hide_index=True)


# ══════════════════════════════════════════════
# 탭2: 파라미터 최적화 (전략 파라미터 / 모드 규칙)
# ══════════════════════════════════════════════

def render_optimization_tab(params):
    p = params
    st.subheader("🔍 파라미터 최적화")
    target = st.radio("최적화 대상", ["🎯 전략 파라미터", "🧭 모드 규칙"],
                      horizontal=True, key="ds_opt_target")
    sort_col = st.selectbox("정렬 기준", ["Calmar", "CAGR(%)", "최종자산"], key="ds_opt_sort")

    if target == "🎯 전략 파라미터":
        _opt_strategy_params(p, sort_col)
    else:
        _opt_mode_rule(p, sort_col)


def _opt_strategy_params(p, sort_col):
    st.caption("공격/방어 매매 파라미터를 탐색합니다. 모드는 사이드바 규칙으로 고정. (랜덤 서치)")
    st.markdown("**🟥 공격모드 범위**")
    a1, a2, a3 = st.columns(3)
    agbuy_r = a1.slider("매수조건(종가%)", 0.0, 15.0, (4.0, 10.0), 0.5, key="ds_o_agbuy")
    agsa_r = a2.slider("매도 α", 0.2, 1.5, (0.3, 0.7), 0.1, key="ds_o_agsa")
    agha_r = a3.slider("보유 α", 0.5, 4.0, (1.5, 3.0), 0.5, key="ds_o_agha")
    st.markdown("**🟦 방어모드 범위**")
    b1, b2, b3 = st.columns(3)
    sfb1_r = b1.slider("매수조건1(MA%)", -3.0, 1.0, (-1.5, 0.0), 0.1, key="ds_o_sfb1")
    sfb2_r = b2.slider("매수조건2(종가%)", 1.0, 12.0, (3.0, 8.0), 0.5, key="ds_o_sfb2")
    sfsell_r = b3.slider("매도조건(MA%)", 0.1, 3.0, (0.4, 1.2), 0.1, key="ds_o_sfsell")
    n_samples = st.slider("랜덤 샘플 수", 50, 800, 300, 50, key="ds_o_n")

    if st.button("🚀 최적화 실행", type="primary", key="ds_run_opt_sp", use_container_width=True):
        import random
        try:
            px_df = get_soxl_data()
            mode_map = build_auto_mode_map(px_df, ma_weeks=p["ma_weeks"],
                                           peak_thr=p["peak_thr"], dn=p["dn"])
        except Exception as e:
            st.error(f"데이터/모드 로드 실패: {e}")
            return
        start = str(p["start_date"]); end = str(p["end_date"]); cap = float(p["initial_capital"])

        def rng(lo, hi, step):
            n = int(round((hi - lo) / step))
            return [round(lo + k * step, 3) for k in range(n + 1)]
        AGB, AGS, AGH = rng(*agbuy_r, 0.5), rng(*agsa_r, 0.1), rng(*agha_r, 0.5)
        SB1, SB2, SS = rng(*sfb1_r, 0.1), rng(*sfb2_r, 0.5), rng(*sfsell_r, 0.1)

        prog = st.progress(0)
        results = []
        seen = set()
        for i in range(n_samples):
            combo = (random.choice(AGB), random.choice(AGS), random.choice(AGH),
                     random.choice(SB1), random.choice(SB2), random.choice(SS))
            if combo in seen:
                continue
            seen.add(combo)
            agb, ags, agh, sb1, sb2, ss = combo
            ds_p = DualSniperParams(
                ag_divisions=p["ag_div"], ag_buy_pct=agb,
                ag_sell_alpha=ags, ag_hold_alpha=agh,
                sf_divisions=p["sf_div"], sf_buy_pct1=sb1, sf_buy_pct2=sb2,
                sf_sell_pct=ss, sf_ma_base=p["sf_ma_base"], sf_hold=p["sf_hold"],
                sf_tier_weights=p["sf_weights"], initial_capital=cap,
                fee_rate=p["fee_rate"] / 100, sec_fee_rate=0.0,
                ag_buy_inclusive=False, sf_buy_inclusive=False)
            log = run_backtest(px_df, ds_p, mode_map=mode_map, start_date=start, light=True)
            log = log[(log['날짜'] >= pd.Timestamp(start)) & (log['날짜'] <= pd.Timestamp(end))]
            if len(log) < 50:
                continue
            mm = compute_metrics(log, None, cap)
            results.append({'매수%': agb, '매도α': ags, '보유α': agh,
                            '방매수1': sb1, '방매수2': sb2, '방매도': ss,
                            'CAGR(%)': round(mm['CAGR(%)'], 1), 'MDD(%)': round(mm['MDD(%)'], 1),
                            'Calmar': round(mm['Calmar'], 2), '최종자산': round(mm['최종자산'])})
            if i % max(1, n_samples // 20) == 0:
                prog.progress(min((i + 1) / n_samples, 1.0))
        prog.progress(1.0)
        if results:
            st.session_state["ds_opt_sp_res"] = pd.DataFrame(results).sort_values(
                sort_col, ascending=False).reset_index(drop=True)

    res = st.session_state.get("ds_opt_sp_res")
    if res is None:
        return
    st.success(f"완료: {len(res)}개 결과 (정렬: {sort_col})")
    st.dataframe(res.sort_values(sort_col, ascending=False).head(25),
                 use_container_width=True, hide_index=True)
    fig = px.scatter(res, x="MDD(%)", y="CAGR(%)", color="Calmar",
                     hover_data=['매수%', '매도α', '보유α', '방매수1', '방매수2', '방매도'],
                     color_continuous_scale="RdYlGn")
    fig.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig, use_container_width=True)
    st.download_button("📥 결과 CSV", res.to_csv(index=False).encode('utf-8-sig'),
                       "dual_sniper_opt_strategy.csv", "text/csv", key="ds_dl_opt_sp")


def _opt_mode_rule(p, sort_col):
    st.caption("자체 하이브리드 모드 규칙의 추세MA·천장·이탈을 탐색합니다. 전략 파라미터는 사이드바 값 고정.")
    col1, col2, col3 = st.columns(3)
    ma_range = col1.slider("추세 MA(주)", 5, 52, (20, 40), 2, key="ds_opt_ma")
    pk_range = col2.slider("천장 wRSI", 58, 74, (60, 70), 2, key="ds_opt_pk")
    dn_range = col3.slider("이탈 wRSI", 38, 50, (40, 46), 1, key="ds_opt_dn")

    if st.button("🚀 최적화 실행", type="primary", key="ds_run_opt", use_container_width=True):
        try:
            px_df = get_soxl_data()
        except Exception as e:
            st.error(f"데이터 로드 실패: {e}")
            return
        ds_p = _make_params(p)
        start = str(p["start_date"]); end = str(p["end_date"]); cap = float(p["initial_capital"])
        mas = list(range(ma_range[0], ma_range[1] + 1, 2))
        pks = list(range(pk_range[0], pk_range[1] + 1, 2))
        dns = list(range(dn_range[0], dn_range[1] + 1, 1))
        combos = list(itertools.product(mas, pks, dns))
        st.info(f"총 {len(combos)}개 조합 실행 중...")
        prog = st.progress(0)
        results = []
        for i, (ma, pk, dn) in enumerate(combos):
            mode_map = build_auto_mode_map(px_df, ma_weeks=ma, peak_thr=pk, dn=dn)
            log = run_backtest(px_df, ds_p, mode_map=mode_map, start_date=start, light=True)
            log = log[(log['날짜'] >= pd.Timestamp(start)) & (log['날짜'] <= pd.Timestamp(end))]
            if len(log) < 50:
                continue
            mm = compute_metrics(log, None, cap)
            results.append({'MA(주)': ma, '천장': pk, '이탈': dn,
                            'CAGR(%)': round(mm['CAGR(%)'], 1), 'MDD(%)': round(mm['MDD(%)'], 1),
                            'Calmar': round(mm['Calmar'], 2), '최종자산': round(mm['최종자산'])})
            if i % max(1, len(combos) // 20) == 0:
                prog.progress(min((i + 1) / len(combos), 1.0))
        prog.progress(1.0)
        if results:
            st.session_state["ds_opt_results"] = pd.DataFrame(results).sort_values(
                sort_col, ascending=False).reset_index(drop=True)

    res = st.session_state.get("ds_opt_results")
    if res is None:
        return
    st.success(f"완료: {len(res)}개 결과 (정렬: {sort_col})")
    st.dataframe(res.sort_values(sort_col, ascending=False).head(25),
                 use_container_width=True, hide_index=True)
    fig = px.scatter(res, x="MDD(%)", y="CAGR(%)", color="Calmar",
                     hover_data=["MA(주)", "천장", "이탈"], color_continuous_scale="RdYlGn")
    fig.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════
# 탭3: 오늘의 주문표 (기본)
# ══════════════════════════════════════════════

def render_ordersheet_tab(params):
    p = params
    st.subheader("📋 오늘의 주문표")
    st.caption("현재 보유 슬롯과 다음 거래일 LOC/MOC 주문 기준가를 산출합니다. (자동 모드 기준)")

    if st.button("🔄 주문표 생성", type="primary", key="ds_os_run"):
        try:
            px_df = get_soxl_data()
            mode_map = build_auto_mode_map(px_df, ma_weeks=p["ma_weeks"],
                                           peak_thr=p["peak_thr"], dn=p["dn"])
        except Exception as e:
            st.error(f"데이터 로드 실패: {e}")
            return
        ds_p = _make_params(p)
        log, trades = run_backtest(px_df, ds_p, mode_map=mode_map,
                                   start_date=str(p["start_date"]), return_trades=True)
        st.session_state["ds_os_log"] = log
        last = log.iloc[-1]
        st.session_state["ds_os_last"] = last

    if "ds_os_last" not in st.session_state:
        st.info("👆 버튼을 눌러 주문표를 생성하세요.")
        return

    last = st.session_state["ds_os_last"]
    st.markdown(f"**기준일**: {pd.Timestamp(last['날짜']).strftime('%Y-%m-%d')} · "
                f"**종가** ${last['종가']:.2f} · **모드** {last['모드']} · "
                f"**보유수량** {int(last['보유수량']):,}주 · **현금** ${last['현금']:,.0f}")
    c1, c2, c3 = st.columns(3)
    c1.metric("총자산", f"${last['총자산']:,.0f}")
    c2.metric("보유티어수", f"{int(last['보유티어수'])}")
    c3.metric("현재 모드", last['모드'])
    st.info("💡 v1: 현재 상태 요약. 슬롯별 매도목표가·계좌관리는 추후 추가 예정입니다.")
    with st.expander("최근 20거래일 로그"):
        log = st.session_state["ds_os_log"]
        ld = log.tail(20).copy(); ld['날짜'] = pd.to_datetime(ld['날짜']).dt.strftime('%Y-%m-%d')
        st.dataframe(ld, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════
# 탭4: 전략 소개
# ══════════════════════════════════════════════

def render_intro_tab(params=None):
    st.subheader("📖 Dual Sniper Pro — 전략 소개")
    st.markdown("""
**SOXL 대상 공격/방어 2모드 슬롯(티어) 기반 그리드 매매 전략.**
원전략 스프레드시트(10.5년)를 역설계하여 엔진을 구축했으며, **실제 모드 입력 시 원전략을
사실상 완벽 재현**합니다 (CAGR 96.5% vs 96.86%, MDD -26.1% vs -26.14%, 매수티어 98.5% 일치).

#### 🟥 공격모드 (추세 상승)
- 슬롯 6개. 매일 1개 티어 LOC 매수 (전일 FI>0: 전일종가+8% / FI≤0: -0.1%)
- 슬롯별 독립 익절: 매수RSI 정규화 (RSI 낮을수록 익절폭↑, 0.1~3.0%)
- 보유기간: 매수RSI 정규화 (7~30일), 만기 MOC 청산
- 1티어 보류: 전일종가>전일MA(5)면 슬롯1 익절 보류

#### 🟦 방어모드 (추세 하락/횡보)
- 슬롯 5개. 매수가 = min(MA조건, 종가조건) LOC
- **역피라미딩 비중** (6→34%): 하락할수록 큰 물량
- MA 돌파 시 **전량 청산**, 보유 8일 만기 MOC

#### 🧭 모드 전환 (자체 하이브리드 규칙)
원전략의 모드 전환 규칙은 **비공개**입니다. 대신 자체 설계한 견고한 규칙을 사용합니다:
- 주봉종가 > **36주 이동평균** → 공격
- wRSI < **42**(이탈) 또는 (wRSI 하락 & 직전 wRSI > **66**)(천장) → 방어
- 백테스트(2016~): **CAGR 76% / MDD -29% / Calmar 2.64** (train≈test 견고, 과최적화 없음)

#### ⚠️ 유의사항
- 2011~2015(반도체 약세장) 포함 시 Calmar 1.1로 하락 — 레버리지 추세전략의 한계.
- 최고 성과를 원하면 **수동 모드 입력**(원전략 시그널 추종)이 자동모드보다 우수합니다.
""")
    st.caption("상세 로직: `08.듀얼스나이퍼/듀얼스나이퍼_로직정리.md` 참조")


# ══════════════════════════════════════════════
# 탭5: DB 조회 (기본)
# ══════════════════════════════════════════════

def render_db_tab(params=None):
    st.subheader("📂 DB 조회")
    st.info("v1: 거래 히스토리 DB 연동은 추후 추가 예정입니다. "
            "현재는 백테스트 탭에서 CSV 다운로드를 이용하세요.")


# ══════════════════════════════════════════════
# 탭6: 개인 설정
# ══════════════════════════════════════════════

def render_settings_tab():
    st.subheader("⚙️ 개인 설정")
    cfg = _load_ds_config()

    st.markdown("##### 기본값 저장")
    st.caption("사이드바 파라미터를 기본값으로 저장합니다.")
    if st.button("💾 현재 사이드바 값 저장", key="ds_save_cfg"):
        cfg["mode_rule"] = {
            "ma_weeks": st.session_state.get("ds_ma", 36),
            "peak_thr": st.session_state.get("ds_peak", 66.0),
            "dn": st.session_state.get("ds_dn", 42.0),
        }
        cfg["strategy"] = {
            "ag_div": st.session_state.get("ds_agdiv", 6),
            "ag_buy": st.session_state.get("ds_agbuy", 8.0),
            "ag_sell_alpha": st.session_state.get("ds_agsa", 0.4),
            "ag_hold_alpha": st.session_state.get("ds_agha", 2.0),
            "sf_div": st.session_state.get("ds_sfdiv", 5),
            "sf_hold": st.session_state.get("ds_sfhold", 8),
            "sf_buy1": st.session_state.get("ds_sfb1", -0.6),
            "sf_buy2": st.session_state.get("ds_sfb2", 5.5),
            "sf_sell": st.session_state.get("ds_sfsell", 0.7),
            "sf_ma_base": st.session_state.get("ds_sfma", 3),
            "sf_weights": st.session_state.get("ds_sfw", "6, 13, 20, 27, 34"),
        }
        cfg["initial_capital"] = st.session_state.get("ds_cap", 10000)
        cfg["fee_rate"] = st.session_state.get("ds_fee", 0.0)
        _save_ds_config(cfg)
        st.success("✅ 저장되었습니다.")

    st.markdown("---")
    st.markdown("##### 데이터 캐시")
    if st.button("🔄 가격/모드 캐시 초기화", key="ds_clear_cache"):
        clear_ds_data_cache()
        try:
            get_auto_mode_map.clear()
        except Exception:
            pass
        st.success("캐시를 초기화했습니다. 다음 실행 시 최신 데이터를 받습니다.")
