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
import requests
from datetime import datetime, timedelta

_root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _root_dir not in sys.path:
    sys.path.insert(0, _root_dir)

from dual_sniper_engine import (
    DualSniperParams, run_backtest, compute_metrics,
    load_price_data, build_auto_mode_map, build_today_orders,
    build_original_mode_map, load_original_modes, next_trading_days,
)
from common.config import _IS_CLOUD

# ──────────────────────────────────────────────
# 설정 경로 (로컬: ~/.dual_sniper/)
# ──────────────────────────────────────────────
_DS_CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".dual_sniper")
_DS_CONFIG_PATH = os.path.join(_DS_CONFIG_DIR, "config.json")

# 모드 공유 중앙 시트 설정 (committed: 모든 유저 공통). 관리자만 쓰기, 전체 읽기.
_SHARE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "dual_sniper_share.json")


def _share_cfg():
    try:
        with open(_SHARE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


@st.cache_data(ttl=300, show_spinner=False)
def _load_shared_modes():
    """중앙 공유 시트의 일별 모드(번들 이후) → {date_str: mode}. 미설정/실패 시 {}."""
    cfg = _share_cfg()
    url, tab = cfg.get("url", ""), cfg.get("tab", "모드공유")
    if not url:
        return {}
    try:
        from common.config import _get_gspread_client
        ws = _get_gspread_client().open_by_url(url).worksheet(tab)
        out = {}
        for row in ws.get_all_values()[1:]:
            if len(row) >= 2 and row[0].strip() and row[1].strip() in ("공격", "방어"):
                out[row[0].strip()] = row[1].strip()
        return out
    except Exception:
        return {}


def _save_shared_mode(date_str, mode):
    """중앙 공유 시트에 오늘 모드 기록 (관리자). (성공여부, 메시지)."""
    cfg = _share_cfg()
    url, tab = cfg.get("url", ""), cfg.get("tab", "모드공유")
    if not url:
        return False, "공유 시트가 설정되지 않았습니다 (dual_sniper_share.json)."
    try:
        from common.config import _get_gspread_client
        sh = _get_gspread_client().open_by_url(url)
        try:
            ws = sh.worksheet(tab)
        except Exception:
            ws = sh.add_worksheet(title=tab, rows=3000, cols=4)
            ws.append_row(["date", "mode", "기록시각"])
        existing = {r[0] for r in ws.get_all_values()[1:] if r}
        if date_str in existing:
            return True, "이미 기록됨"
        from datetime import datetime as _dt
        ws.append_row([date_str, mode, _dt.now().strftime("%Y-%m-%d %H:%M")])
        try:
            _load_shared_modes.clear()
        except Exception:
            pass
        return True, "기록 완료"
    except Exception as e:
        return False, str(e)


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
    sp = dict(cfg.get("strategy", {}))
    mr = dict(cfg.get("mode_rule", {}))

    # 로케트셋 불러오기 요청 시: 위젯 키 비우고 기본값 소스를 로케트셋으로 덮어씀 (경고 없이 리셋)
    if st.session_state.pop("_ds_force_rocket", False):
        _rk = _DS_PRESETS[0]
        sp = {"ag_div": _rk["ag_div"], "ag_buy": _rk["ag_buy"],
              "ag_sell_alpha": _rk["ag_sell_alpha"], "ag_hold_alpha": _rk["ag_hold_alpha"],
              "sf_div": _rk["sf_div"], "sf_hold": _rk["sf_hold"], "sf_buy1": _rk["sf_buy1"],
              "sf_buy2": _rk["sf_buy2"], "sf_sell": _rk["sf_sell"], "sf_ma_base": _rk["sf_ma_base"],
              "sf_weights": _rk["sf_weights"]}
        mr = {"ma_weeks": 36, "peak_thr": 66.0, "dn": 42.0}

    st.sidebar.markdown("### 🎯 Dual Sniper Pro")
    st.sidebar.caption("SOXL · 공격/방어 2모드 슬롯 그리드")

    # ── 모드 규칙 (자체 하이브리드) ──
    with st.sidebar.expander("🧭 모드 규칙 (자동)", expanded=False):
        st.caption("주봉 > N주MA → 공격, 천장하락/이탈 → 방어")
        ma_weeks = st.number_input("추세 MA(주)", 5, 60, int(mr.get("ma_weeks", 36)), 1, key="ds_ma")
        peak_thr = st.number_input("천장 wRSI", 50.0, 80.0, float(mr.get("peak_thr", 66.0)), 1.0, key="ds_peak")
        dn = st.number_input("방어 이탈 wRSI", 30.0, 50.0, float(mr.get("dn", 42.0)), 1.0, key="ds_dn")
        st.caption("기본값: 36 / 66 / 42 (Calmar 2.64)")

    # ══ 모드 소스 (원전략 따라가기 vs 자동) ══
    _SRC_OPTS = ["🤖 자동 (하이브리드)", "🔗 원본시트 자동", "✋ 수동 공격", "✋ 수동 방어"]
    _src_saved = cfg.get("mode_source", _SRC_OPTS[0])
    _src_idx = _SRC_OPTS.index(_src_saved) if _src_saved in _SRC_OPTS else 0
    mode_source = st.sidebar.selectbox("🧭 모드 소스 (운용 방식)", _SRC_OPTS, index=_src_idx,
                                       key="ds_mode_source",
                                       help="자동=우리 하이브리드 규칙 / 원본시트=원전략 구글시트에서 모드 역산 / "
                                            "수동=직접 지정. 계좌 추가 시 이 값이 기본값으로 들어갑니다.")
    st.sidebar.caption("↳ 계좌별로 따로 저장됩니다. 주문표 탭에서 계좌마다 변경 가능.")

    # ══ 로케트셋 기본값 불러오기 ══
    def _load_rocket():
        for _k in ("ds_agdiv", "ds_agbuy", "ds_agsa", "ds_agha", "ds_sfdiv", "ds_sfhold",
                   "ds_sfb1", "ds_sfb2", "ds_sfsell", "ds_sfma", "ds_sfw",
                   "ds_ma", "ds_peak", "ds_dn"):
            st.session_state.pop(_k, None)     # 위젯 키 비움 → value= 기본값이 적용됨
        st.session_state["_ds_force_rocket"] = True
    st.sidebar.button("🚀 로케트셋 기본값 불러오기", on_click=_load_rocket,
                      use_container_width=True,
                      help="공격/방어 파라미터 + 모드규칙을 원전략(로케트셋) 기본값으로 일괄 초기화")

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
    _date_max = max(datetime.today().date(), datetime(2026, 12, 31).date())
    start_date = g1.date_input("투자시작일", value=pd.Timestamp(cfg.get("start_date", "2016-01-04")),
                               min_value=datetime(2010, 3, 11).date(), max_value=_date_max,
                               key="ds_start")
    end_date = g2.date_input("투자종료일", value=datetime.today().date(),
                             min_value=datetime(2010, 3, 11).date(), max_value=_date_max,
                             key="ds_end")
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
        "mode_source": mode_source,
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

    _msrc = _normalize_src(p.get("mode_source", "자동"))
    if _msrc == "자동":
        st.caption("🤖 모드: **자동 하이브리드** (사이드바 모드 소스 기준)")
    else:
        st.caption(f"📜 모드: **원전략 실제모드(2016~)** — 사이드바 '{_SRC_LBL.get(_msrc, _msrc)}' 선택됨 "
                   "→ 원전략 그대로 백테스트")

    if st.button("▶ 백테스트 실행", type="primary", key="ds_bt_run"):
        try:
            px_df = get_soxl_data()
            if _msrc == "자동":
                mode_map = build_auto_mode_map(px_df, ma_weeks=p["ma_weeks"],
                                               peak_thr=p["peak_thr"], dn=p["dn"])
            else:
                mode_map = build_original_mode_map(px_df, extra_modes=dict(_load_shared_modes()))
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
# 탭2: 파라미터 최적화 (DSS 4종 방식 — 그리드/랜덤/워크포워드/베이지안)
# ══════════════════════════════════════════════

_OPT_METRICS = {
    "Calmar Ratio (CAGR / MDD)": ("Calmar", False),
    "CAGR (%)": ("CAGR(%)", False),
    "총수익률 (%)": ("수익률(%)", False),
    "MDD 최소화 (작을수록 좋음)": ("MDD(%)", False),
    "Sharpe Ratio": ("Sharpe", False),
}


def _eval_dual(combo, px_df, mode_map, start, end, cap, fixed):
    """combo=(공분할,공보유α,공매수%,공매도α,방분할,방보유,방매수1,방매수2,방매도) → 결과 dict or None."""
    ag_div, ag_ha, ag_buy, ag_sa, sf_div, sf_hold, sf_b1, sf_b2, sf_sell = combo
    sf_ma_base, sf_weights, fee_rate = fixed
    w = sf_weights
    if len(w) != sf_div:
        base = [6, 13, 20, 27, 34, 40, 46, 52]
        w = tuple(base[:sf_div]) if sf_div <= len(base) else tuple([round(100/sf_div, 1)] * sf_div)
    ds_p = DualSniperParams(
        ag_divisions=int(ag_div), ag_buy_pct=ag_buy, ag_sell_alpha=ag_sa, ag_hold_alpha=ag_ha,
        sf_divisions=int(sf_div), sf_buy_pct1=sf_b1, sf_buy_pct2=sf_b2, sf_sell_pct=sf_sell,
        sf_ma_base=int(sf_ma_base), sf_hold=int(sf_hold), sf_tier_weights=w,
        initial_capital=cap, fee_rate=fee_rate / 100, sec_fee_rate=0.0,
        ag_buy_inclusive=False, sf_buy_inclusive=False)
    log = run_backtest(px_df, ds_p, mode_map=mode_map, start_date=start, light=True)
    log = log[(log['날짜'] >= pd.Timestamp(start)) & (log['날짜'] <= pd.Timestamp(end))]
    if len(log) < 50:
        return None
    m = compute_metrics(log, None, cap)
    if not np.isfinite(m.get('Calmar', float('nan'))):
        return None
    return {'공분할': int(ag_div), '공보유α': ag_ha, '공매수%': ag_buy, '공매도α': ag_sa,
            '방분할': int(sf_div), '방보유': int(sf_hold), '방매수1': sf_b1, '방매수2': sf_b2, '방매도': sf_sell,
            'CAGR(%)': round(m['CAGR(%)'], 1), 'MDD(%)': round(m['MDD(%)'], 1),
            '수익률(%)': round(m['총수익률(%)'], 1),
            'Sharpe': round(m['Sharpe'], 2) if np.isfinite(m['Sharpe']) else 0.0,
            'Calmar': round(m['Calmar'], 2), '최종자산': round(m['최종자산'])}


def _show_ds_opt(res_df, sort_col, key_sfx):
    st.success(f"완료: {len(res_df)}개 결과 (정렬: {sort_col})")
    st.dataframe(res_df.head(25), use_container_width=True, hide_index=True)
    fig = px.scatter(res_df, x="MDD(%)", y="CAGR(%)", color="Calmar",
                     hover_data=['공매수%', '공매도α', '공보유α', '방매수1', '방매수2', '방매도'],
                     color_continuous_scale="RdYlGn")
    fig.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig, use_container_width=True)
    st.download_button("📥 결과 CSV", res_df.to_csv(index=False).encode('utf-8-sig'),
                       f"dual_sniper_opt_{key_sfx}.csv", "text/csv", key=f"ds_dl_{key_sfx}")


def render_optimization_tab(params):
    p = params
    st.subheader("🔍 파라미터 최적화")
    method = st.radio("최적화 방식",
                      ["📊 그리드 탐색", "🎲 랜덤 탐색", "📈 워크포워드", "🧠 베이지안"],
                      horizontal=True, key="ds_opt_method")
    _desc = {
        "📊 그리드 탐색": "모든 파라미터 조합을 완전 탐색합니다. 조합이 적을 때 가장 정확합니다.",
        "🎲 랜덤 탐색": "무작위로 N개 조합을 샘플링합니다. 탐색 공간이 클 때 빠르게 좋은 값을 찾습니다.",
        "📈 워크포워드": "전체 기간을 IS(최적화)·OOS(검증) 윈도우로 분할해 과적합을 방지합니다.",
        "🧠 베이지안": "Optuna TPE로 적은 시도로 최적값에 빠르게 수렴합니다.",
    }
    _msrc = _normalize_src(p.get("mode_source", "자동"))
    _mode_label = ("원전략 실제모드(2016~) 고정" if _msrc != "자동"
                   else "자동 하이브리드 규칙 고정")
    st.caption(_desc[method] + f"  ·  모드는 **{_mode_label}** (전략 파라미터만 탐색)")

    # ── 파라미터 범위 ──
    with st.expander("파라미터 범위 설정", expanded=True):
        st.markdown("**🟥 공격모드**")
        a1, a2, a3, a4 = st.columns(4)
        agdv = a1.slider("분할수", 3, 10, (6, 6), key="dso_agdv")
        agha = a2.slider("보유 α", 0.5, 4.0, (2.0, 2.0), 0.5, key="dso_agha")
        agha_s = a2.number_input("보유α 간격", 0.5, 2.0, 1.0, 0.5, key="dso_agha_s")
        agby = a3.slider("매수조건%", 0.0, 15.0, (6.0, 10.0), 0.5, key="dso_agby")
        agby_s = a3.number_input("매수% 간격", 0.5, 5.0, 2.0, 0.5, key="dso_agby_s")
        agsa = a4.slider("매도 α", 0.2, 1.5, (0.3, 0.5), 0.1, key="dso_agsa")
        agsa_s = a4.number_input("매도α 간격", 0.1, 0.5, 0.1, 0.1, key="dso_agsa_s")
        st.markdown("**🟦 방어모드**")
        b1, b2, b3, b4, b5 = st.columns(5)
        sfdv = b1.slider("분할수", 3, 8, (5, 5), key="dso_sfdv")
        sfhd = b2.slider("보유기간", 4, 20, (8, 8), key="dso_sfhd")
        sfb1 = b3.slider("매수1 MA%", -3.0, 1.0, (-1.0, 0.0), 0.1, key="dso_sfb1")
        sfb1_s = b3.number_input("매수1 간격", 0.1, 1.0, 0.5, 0.1, key="dso_sfb1_s")
        sfb2 = b4.slider("매수2 종가%", 1.0, 12.0, (4.0, 8.0), 0.5, key="dso_sfb2")
        sfb2_s = b4.number_input("매수2 간격", 0.5, 3.0, 2.0, 0.5, key="dso_sfb2_s")
        sfsl = b5.slider("매도 MA%", 0.1, 3.0, (0.5, 1.0), 0.1, key="dso_sfsl")
        sfsl_s = b5.number_input("매도 간격", 0.1, 1.0, 0.5, 0.1, key="dso_sfsl_s")
        metric_label = st.selectbox("최적화 기준 지표", list(_OPT_METRICS.keys()), key="ds_opt_metric")
    sort_col, sort_asc = _OPT_METRICS[metric_label]

    def _vint(rng, step=1):
        return list(range(rng[0], rng[1] + 1, max(1, int(step))))

    def _vflt(rng, step):
        n = int(round((rng[1] - rng[0]) / step)) if step > 0 else 0
        return [round(rng[0] + k * step, 3) for k in range(n + 1)]

    AGDV, AGHA = _vint(agdv), _vflt(agha, agha_s)
    AGBY, AGSA = _vflt(agby, agby_s), _vflt(agsa, agsa_s)
    SFDV, SFHD = _vint(sfdv), _vint(sfhd)
    SFB1, SFB2, SFSL = _vflt(sfb1, sfb1_s), _vflt(sfb2, sfb2_s), _vflt(sfsl, sfsl_s)
    n_total = (len(AGDV) * len(AGHA) * len(AGBY) * len(AGSA) *
               len(SFDV) * len(SFHD) * len(SFB1) * len(SFB2) * len(SFSL))

    start, end = str(p["start_date"]), str(p["end_date"])
    cap = float(p["initial_capital"])
    fixed = (p["sf_ma_base"], p["sf_weights"], p["fee_rate"])

    def _load():
        px_df = get_soxl_data()
        if _msrc == "자동":
            mode_map = build_auto_mode_map(px_df, ma_weeks=p["ma_weeks"],
                                           peak_thr=p["peak_thr"], dn=p["dn"])
        else:
            mode_map = build_original_mode_map(px_df, extra_modes=dict(_load_shared_modes()))
        return px_df, mode_map

    # ── ① 그리드 ──
    if method == "📊 그리드 탐색":
        msg = f"예상 조합 수: **{n_total:,}개**"
        (st.error if n_total > 30000 else st.warning if n_total > 4000 else st.info)(msg)
        if st.button("▶ 그리드 탐색 실행", type="primary", key="ds_grid",
                     disabled=(n_total == 0 or n_total > 30000)):
            px_df, mode_map = _load()
            combos = list(itertools.product(AGDV, AGHA, AGBY, AGSA, SFDV, SFHD, SFB1, SFB2, SFSL))
            prog = st.progress(0.0, text="그리드 탐색 중...")
            rows = []
            for i, c in enumerate(combos):
                r = _eval_dual(c, px_df, mode_map, start, end, cap, fixed)
                if r:
                    rows.append(r)
                if i % max(1, len(combos) // 50) == 0:
                    prog.progress(min((i + 1) / len(combos), 1.0))
            prog.progress(1.0)
            if rows:
                st.session_state["ds_opt_res"] = pd.DataFrame(rows).sort_values(
                    sort_col, ascending=sort_asc).reset_index(drop=True)
        if "ds_opt_res" in st.session_state:
            _show_ds_opt(st.session_state["ds_opt_res"], sort_col, "grid")

    # ── ② 랜덤 ──
    elif method == "🎲 랜덤 탐색":
        import random
        n_s = st.number_input("샘플 수", 50, 5000, 400, 50, key="ds_ns")
        st.info(f"랜덤 **{int(n_s):,}개** 조합 샘플링 (그리드 {n_total:,}개 중 무작위)")
        if st.button("▶ 랜덤 탐색 실행", type="primary", key="ds_rand"):
            px_df, mode_map = _load()
            random.seed(42)
            prog = st.progress(0.0, text="랜덤 탐색 중...")
            rows, seen = [], set()
            for i in range(int(n_s)):
                c = (random.choice(AGDV), random.choice(AGHA), random.choice(AGBY), random.choice(AGSA),
                     random.choice(SFDV), random.choice(SFHD), random.choice(SFB1), random.choice(SFB2),
                     random.choice(SFSL))
                if c in seen:
                    continue
                seen.add(c)
                r = _eval_dual(c, px_df, mode_map, start, end, cap, fixed)
                if r:
                    rows.append(r)
                if i % max(1, int(n_s) // 50) == 0:
                    prog.progress(min((i + 1) / int(n_s), 1.0))
            prog.progress(1.0)
            if rows:
                st.session_state["ds_opt_res"] = pd.DataFrame(rows).sort_values(
                    sort_col, ascending=sort_asc).reset_index(drop=True)
        if "ds_opt_res" in st.session_state:
            _show_ds_opt(st.session_state["ds_opt_res"], sort_col, "random")

    # ── ③ 워크포워드 ──
    elif method == "📈 워크포워드":
        w1, w2 = st.columns(2)
        is_y = w1.number_input("IS(최적화) 기간(년)", 1, 10, 3, key="ds_wf_is")
        oos_y = w2.number_input("OOS(검증) 기간(년)", 1, 5, 1, key="ds_wf_oos")
        st.info(f"IS **{is_y}년** 최적화 → OOS **{oos_y}년** 검증을 슬라이딩 반복. "
                f"그리드 **{n_total:,}개** × 윈도우 수만큼 실행됩니다.")
        if n_total > 2000:
            st.warning(f"조합 {n_total:,}개로 많습니다. 범위/간격을 줄이면 빨라집니다.")
        if st.button("▶ 워크포워드 실행", type="primary", key="ds_wf"):
            px_df, mode_map = _load()
            ts, te = pd.Timestamp(start).date(), pd.Timestamp(end).date()
            wins, cur = [], ts
            while True:
                ie = cur + timedelta(days=int(is_y * 365.25))
                oe = ie + timedelta(days=int(oos_y * 365.25))
                if oe > te:
                    break
                wins.append((cur, ie, oe))
                cur = ie
            if not wins:
                st.error("데이터 기간이 짧아 윈도우를 만들 수 없습니다.")
            else:
                combos = list(itertools.product(AGDV, AGHA, AGBY, AGSA, SFDV, SFHD, SFB1, SFB2, SFSL))
                prog = st.progress(0.0, text="워크포워드 중...")
                wrows, ccap = [], cap
                for wi, (is_s, is_e, oos_e) in enumerate(wins):
                    best = None
                    for c in combos:
                        r = _eval_dual(c, px_df, mode_map, str(is_s), str(is_e), cap, fixed)
                        if r and (best is None or r[sort_col] > best[0]):
                            best = (r[sort_col], c)
                    prog.progress((wi + 0.5) / len(wins),
                                  text=f"윈도우 {wi+1}/{len(wins)} OOS 검증 중...")
                    if not best:
                        continue
                    oos = _eval_dual(best[1], px_df, mode_map, str(is_e), str(oos_e), ccap, fixed)
                    if not oos:
                        continue
                    c = best[1]
                    wrows.append({'윈도우': wi + 1, 'IS': f"{is_s}~{is_e}", 'OOS': f"{is_e}~{oos_e}",
                                  '공(분/보α/매수/매도α)': f"{c[0]}/{c[1]}/{c[2]}/{c[3]}",
                                  '방(분/보/매1/매2/매도)': f"{c[4]}/{c[5]}/{c[6]}/{c[7]}/{c[8]}",
                                  f'IS {sort_col}': round(best[0], 2),
                                  'OOS CAGR(%)': oos['CAGR(%)'], 'OOS MDD(%)': oos['MDD(%)'],
                                  'OOS Calmar': oos['Calmar'],
                                  '시작($)': round(ccap), '종료($)': oos['최종자산']})
                    ccap = oos['최종자산']
                prog.progress(1.0)
                if not wrows:
                    st.error("유효한 OOS 결과가 없습니다.")
                else:
                    wdf = pd.DataFrame(wrows)
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("시작 자본", f"${cap:,.0f}")
                    m2.metric("최종 자본(OOS)", f"${ccap:,.0f}")
                    m3.metric("OOS 총수익", f"{(ccap/cap-1)*100:+.1f}%")
                    m4.metric("윈도우 수", f"{len(wrows)}개")
                    st.dataframe(wdf, use_container_width=True, hide_index=True)
                    figw = px.bar(wdf, x="윈도우", y="OOS CAGR(%)", color="OOS CAGR(%)",
                                  color_continuous_scale="RdYlGn", text_auto=".0f")
                    figw.add_hline(y=0, line_dash="dash", line_color="gray")
                    figw.update_layout(height=380)
                    st.plotly_chart(figw, use_container_width=True)
                    st.download_button("📥 워크포워드 CSV", wdf.to_csv(index=False).encode('utf-8-sig'),
                                       "dual_sniper_wfo.csv", "text/csv", key="ds_dl_wfo")

    # ── ④ 베이지안 ──
    else:
        try:
            import optuna as _optuna
            _ok = True
        except ImportError:
            _ok = False
        if not _ok:
            st.error("`optuna` 패키지가 없습니다. `pip install optuna` 후 재시작하세요.")
        else:
            n_t = st.number_input("탐색 횟수(trials)", 50, 2000, 300, 50, key="ds_nt")
            st.info(f"Optuna TPE로 **{int(n_t)}회** 스마트 탐색합니다.")
            if st.button("▶ 베이지안 최적화 실행", type="primary", key="ds_bayes"):
                px_df, mode_map = _load()
                _optuna.logging.set_verbosity(_optuna.logging.WARNING)
                prog = st.progress(0.0, text="베이지안 탐색 중...")
                rows, tc = [], [0]

                def _obj(tr):
                    c = (tr.suggest_int("공분할", agdv[0], agdv[1]),
                         round(tr.suggest_float("공보유α", agha[0], agha[1]), 2),
                         round(tr.suggest_float("공매수%", agby[0], agby[1]), 2),
                         round(tr.suggest_float("공매도α", agsa[0], agsa[1]), 2),
                         tr.suggest_int("방분할", sfdv[0], sfdv[1]),
                         tr.suggest_int("방보유", sfhd[0], sfhd[1]),
                         round(tr.suggest_float("방매수1", sfb1[0], sfb1[1]), 2),
                         round(tr.suggest_float("방매수2", sfb2[0], sfb2[1]), 2),
                         round(tr.suggest_float("방매도", sfsl[0], sfsl[1]), 2))
                    r = _eval_dual(c, px_df, mode_map, start, end, cap, fixed)
                    if not r:
                        return -999.0
                    rows.append(r)
                    tc[0] += 1
                    if tc[0] % max(1, int(n_t) // 50) == 0:
                        prog.progress(min(tc[0] / int(n_t), 1.0), text=f"탐색 {tc[0]}/{int(n_t)}")
                    return r[sort_col]

                study = _optuna.create_study(direction="maximize",
                                             sampler=_optuna.samplers.TPESampler(seed=42))
                study.optimize(_obj, n_trials=int(n_t))
                prog.progress(1.0)
                if not rows:
                    st.error("유효한 결과가 없습니다.")
                else:
                    bp = study.best_params
                    st.success(f"**최적:** 공(분{bp.get('공분할')}/보α{bp.get('공보유α',0):.2f}/"
                               f"매수{bp.get('공매수%',0):.1f}%/매도α{bp.get('공매도α',0):.2f}) · "
                               f"방(분{bp.get('방분할')}/보{bp.get('방보유')}/매1{bp.get('방매수1',0):.1f}/"
                               f"매2{bp.get('방매수2',0):.1f}/매도{bp.get('방매도',0):.1f})")
                    st.session_state["ds_opt_res"] = pd.DataFrame(rows).sort_values(
                        sort_col, ascending=sort_asc).reset_index(drop=True)
                    _show_ds_opt(st.session_state["ds_opt_res"], sort_col, "bayes")
                    vals = [t.value for t in study.trials if t.value is not None and t.value > -900]
                    bc = [max(vals[:i + 1]) for i in range(len(vals))]
                    figc = px.line(y=bc, labels={"y": f"Best {sort_col}", "index": "Trial"},
                                   title="베이지안 수렴 곡선")
                    figc.update_layout(height=350)
                    st.plotly_chart(figc, use_container_width=True)


# ══════════════════════════════════════════════
# 탭3: 오늘의 주문표 & 계좌관리 (DSS 패턴)
# ══════════════════════════════════════════════

_DS_PRESETS = [
    {"label": "🚀 로케트셋 (원전략 기본)", "ag_div": 6, "ag_buy": 8.0, "ag_sell_alpha": 0.4,
     "ag_hold_alpha": 2.0, "sf_div": 5, "sf_hold": 8, "sf_buy1": -0.6, "sf_buy2": 5.5,
     "sf_sell": 0.7, "sf_ma_base": 3, "sf_weights": "6, 13, 20, 27, 34",
     "help": ("원전략(로케트셋) 기본 파라미터\n"
              "공격: 분할6 · 매수8.0% · 매도α0.4 · 보유α2.0\n"
              "방어: 분할5 · 보유8일 · 매수1 -0.6% · 매수2 5.5% · 매도0.7% · MA3 · 비중6/13/20/27/34\n"
              "원전략 실제모드 기준 CAGR 96.8% / MDD -26.1% / Calmar 3.70")},
]


def _ds_history_path(acct_name=""):
    safe = (acct_name or "").replace(" ", "_").replace("/", "_").replace("\\", "_")
    fn = f"history_SOXL_{safe}.csv" if safe else "history_SOXL.csv"
    return os.path.join(_DS_CONFIG_DIR, fn)


def _load_ds_history(acct_name=""):
    path = _ds_history_path(acct_name)
    if os.path.exists(path):
        try:
            return pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def _save_ds_history(acct_name, snapshot_row):
    """B방식: 기존 날짜 보존, 새 날짜만 추가. Cloud: gs_url 있으면 GSheets 동기화."""
    os.makedirs(_DS_CONFIG_DIR, exist_ok=True)
    new_df = pd.DataFrame([snapshot_row])
    new_df["날짜"] = new_df["날짜"].astype(str)
    path = _ds_history_path(acct_name)
    if os.path.exists(path):
        old = pd.read_csv(path, encoding="utf-8-sig")
        old_dates = set(old["날짜"].astype(str))
        add = new_df[~new_df["날짜"].astype(str).isin(old_dates)]
        if not add.empty:
            pd.concat([old, add], ignore_index=True).to_csv(path, index=False, encoding="utf-8-sig")
    else:
        new_df.to_csv(path, index=False, encoding="utf-8-sig")

    cfg = _load_ds_config()
    gs_url = cfg.get("gs_url", "")
    if gs_url:
        try:
            import gspread as _gs
            from common.config import _get_gspread_client
            safe = (acct_name or "기본계좌").replace(" ", "_").replace("/", "_").replace("\\", "_")
            ws_name = f"ds_{safe}_매매기록"
            client = _get_gspread_client()
            sh = client.open_by_url(gs_url)
            try:
                ws = sh.worksheet(ws_name)
                cells = ws.get_all_values()
                gs_dates = set()
                if len(cells) > 1 and "날짜" in cells[0]:
                    di = cells[0].index("날짜")
                    gs_dates = {r[di] for r in cells[1:] if len(r) > di}
                add_gs = new_df[~new_df["날짜"].astype(str).isin(gs_dates)]
            except _gs.WorksheetNotFound:
                ws = sh.add_worksheet(title=ws_name, rows=5000, cols=20)
                ws.append_row(new_df.columns.tolist())
                add_gs = new_df
            if not add_gs.empty:
                ws.append_rows([[str(v) for v in r] for r in add_gs.values.tolist()],
                               value_input_option="RAW")
                st.toast(f"📊 '{ws_name}' 동기화", icon="✅")
        except Exception as _e:
            st.warning(f"⚠️ GSheets 동기화 실패: {_e}")


def _read_algoc_orders(url, tab="Rocket"):
    """원본(알고리C/ASTRA) 시트 L4:O 주문 영역 읽기 → [(구분,거래방법,가격,수량), ...]."""
    from common.config import _get_gspread_client
    gc = _get_gspread_client()
    ws = gc.open_by_url(url).worksheet(tab)
    vals = ws.get_all_values()
    rows = []
    for r in range(3, min(len(vals), 20)):       # 4행(인덱스3)부터
        row = vals[r]
        g = row[11].strip() if len(row) > 11 else ""   # L
        m = row[12].strip() if len(row) > 12 else ""   # M
        pr = row[13].strip() if len(row) > 13 else ""  # N
        q = row[14].strip() if len(row) > 14 else ""   # O
        if g in ("매수", "매도"):
            rows.append((g, m, pr, q))
    return rows


def _infer_mode_from_orders(rows, c1, c2, ds_p):
    """원본 주문가 → 모드 역산. c1=전일종가, c2=전전일종가."""
    import math
    def _f(x):
        try:
            return float(str(x).replace(",", ""))
        except Exception:
            return None
    buys = [_f(p) for (g, m, p, q) in rows if g == "매수" and m != "MOC" and _f(p)]
    sells = [_f(p) for (g, m, p, q) in rows if g == "매도" and m != "MOC" and _f(p)]
    # 후보 매수가
    ag_plus = math.floor(c1 * (1 + ds_p.ag_buy_pct / 100) * 100) / 100
    ag_minus = math.floor(c1 * 0.999 * 100) / 100
    f1 = 1 + ds_p.sf_buy_pct1 / 100
    cond1 = (f1 * (c1 + c2)) / (ds_p.sf_ma_base - f1)
    cond2 = c1 * (1 + ds_p.sf_buy_pct2 / 100)
    sf_k = round(min(cond1, cond2), 2)
    detail = {"원본매수가": buys[0] if buys else None,
              "후보_공격FI+": ag_plus, "후보_공격FI-": ag_minus, "후보_방어": sf_k}
    if buys:
        bp = buys[0]
        cands = [("공격", ag_plus), ("공격", ag_minus), ("방어", sf_k)]
        best = min(cands, key=lambda c: abs(c[1] - bp))
        if abs(best[1] - bp) < 0.15:
            return best[0], detail
    # 매수 없으면 매도가 균일성으로 판정 (방어=동일 MA가, 공격=슬롯별 상이)
    if len(sells) >= 2:
        return ("방어" if (max(sells) - min(sells) < 0.02) else "공격"), detail
    if len(sells) == 1:
        fs = 1 + ds_p.sf_sell_pct / 100
        sf_sell = (fs * (c1 + c2)) / (ds_p.sf_ma_base - fs)
        return ("방어" if abs(sells[0] - sf_sell) < 0.15 else "공격"), detail
    return None, detail


_SRC_VALS = ["자동", "원본시트", "공격", "방어"]
_SRC_LBL = {"자동": "🤖 자동 (하이브리드)", "원본시트": "🔗 원본시트 자동",
            "공격": "✋ 수동 공격", "방어": "✋ 수동 방어"}


def _normalize_src(s):
    """사이드바 라벨/내부값 → 내부값."""
    s = str(s)
    if s in _SRC_VALS:
        return s
    for v, lbl in _SRC_LBL.items():       # 정확 라벨 매칭 우선
        if s == lbl:
            return v
    if "원본" in s:                        # 키워드 (구체적인 것 먼저)
        return "원본시트"
    if "공격" in s:
        return "공격"
    if "방어" in s:
        return "방어"
    return "자동"


def _render_ds_source_box(acct_key, acct_data, cfg, sfx):
    """계좌별 모드 소스 선택 + 저장 (원본시트 URL/탭 포함). 저장된 내부값 반환."""
    cur = _normalize_src(acct_data.get("mode_source", "자동"))
    with st.container(border=True):
        st.markdown("##### 🧭 모드 소스 (운용 방식)")
        sel = st.selectbox("소스 선택", _SRC_VALS,
                           index=_SRC_VALS.index(cur), format_func=lambda x: _SRC_LBL[x],
                           key=f"ds_src{sfx}", label_visibility="collapsed")
        au = acct_data.get("algoc_url", "")
        at = acct_data.get("algoc_tab", "Rocket")
        if sel == "원본시트":
            a1, a2 = st.columns([3, 1])
            au = a1.text_input("원본 시트 URL (ASTRA/Rocket)", value=au,
                               placeholder="https://docs.google.com/spreadsheets/d/...",
                               key=f"ds_au{sfx}")
            at = a2.text_input("탭 이름", value=at, key=f"ds_at{sfx}")
            st.caption("⚠️ 원본 시트를 서비스 계정(읽기)에 공유: "
                       "`connectspreadsheet@sodium-gateway-485307-f3.iam.gserviceaccount.com` · "
                       "매일 9시경 원본 주문가로 공격/방어 모드를 자동 역산합니다.")
        elif sel in ("공격", "방어"):
            st.caption(f"✋ 항상 **{sel}** 모드로 주문을 산출합니다. (원전략 모드를 직접 확인하며 변경)")
        else:
            st.caption("🤖 자체 하이브리드 규칙(36주MA + 천장방어)으로 모드를 자동 판정합니다.")
        if st.button("💾 모드 소스 저장", key=f"ds_srcsave{sfx}", use_container_width=True):
            acct_data["mode_source"] = sel
            if sel == "원본시트":
                acct_data["algoc_url"] = au.strip()
                acct_data["algoc_tab"] = (at.strip() or "Rocket")
            cfg["accounts"][acct_key] = acct_data
            _save_ds_config(cfg)
            st.success(f"✅ 모드 소스 '{_SRC_LBL[sel]}' 저장됨")
            st.rerun()
    return _normalize_src(acct_data.get("mode_source", "자동"))


def _acct_params(acct_data, p):
    """계좌 저장 파라미터(없으면 사이드바 p) → (DualSniperParams, mode_rule dict)."""
    ap = acct_data.get("params", {})
    def g(k, d):
        return ap.get(k, p.get(k, d))
    sf_w = g("sf_weights", "6, 13, 20, 27, 34")
    try:
        weights = tuple(float(x.strip()) for x in str(sf_w).split(",") if x.strip())
    except Exception:
        weights = (6, 13, 20, 27, 34)
    sf_div = int(g("sf_div", 5))
    if len(weights) != sf_div:
        weights = tuple([6, 13, 20, 27, 34, 40, 46, 52][:sf_div])
    ds_p = DualSniperParams(
        ag_divisions=int(g("ag_div", 6)), ag_buy_pct=float(g("ag_buy", 8.0)),
        ag_sell_alpha=float(g("ag_sell_alpha", 0.4)), ag_hold_alpha=float(g("ag_hold_alpha", 2.0)),
        sf_divisions=sf_div, sf_buy_pct1=float(g("sf_buy1", -0.6)),
        sf_buy_pct2=float(g("sf_buy2", 5.5)), sf_sell_pct=float(g("sf_sell", 0.7)),
        sf_ma_base=int(g("sf_ma_base", 3)), sf_hold=int(g("sf_hold", 8)),
        sf_tier_weights=weights,
        initial_capital=10000.0, fee_rate=float(p.get("fee_rate", 0.0)) / 100, sec_fee_rate=0.0,
        ag_buy_inclusive=False, sf_buy_inclusive=False)
    mode_rule = {"ma_weeks": int(g("ma_weeks", p.get("ma_weeks", 36))),
                 "peak_thr": float(g("peak_thr", p.get("peak_thr", 66.0))),
                 "dn": float(g("dn", p.get("dn", 42.0)))}
    return ds_p, mode_rule


def render_ordersheet_tab(params):
    p = params
    cfg = _load_ds_config()
    today_str = datetime.today().strftime("%Y-%m-%d")
    st.subheader(f"📋 오늘의 주문표 — {today_str}")
    st.caption("계좌별 포트폴리오를 추적하고, 다음 거래일 LOC/MOC 주문을 산출합니다.")

    # ── 📢 오늘 원전략 모드 (전체 유저 공유) ──
    _shared = _load_shared_modes()
    _orig = load_original_modes()
    _last_bundle = (str(_orig['date'].iloc[-1].date()), _orig['mode'].iloc[-1]) if len(_orig) else (None, None)
    if _shared:
        _ld = sorted(_shared.keys())[-1]
        st.info(f"📢 **원전략 모드: `{_shared[_ld]}`** (적용 거래일 {_ld}) — "
                f"원전략을 따라가려면 계좌 모드 소스를 '수동: {_shared[_ld]}'로 두면 됩니다.")
    elif _last_bundle[0]:
        st.caption(f"📜 원전략 실제모드 번들: {_orig['date'].iloc[0].date()} ~ {_last_bundle[0]} "
                   f"(마지막 {_last_bundle[1]}) · 이후 모드는 관리자 공유 시 표시됩니다.")

    accounts = cfg.get("accounts", {})

    # ── 계좌 추가 ──
    with st.expander("➕ 계좌 추가", expanded=(len(accounts) == 0)):
        ac1, ac2 = st.columns(2)
        add_name = ac1.text_input("계좌 이름", "", key="ds_add_name", placeholder="예: PJH, 연습용")
        _dmax = max(datetime.today().date(), datetime(2026, 12, 31).date())
        add_start = ac2.date_input("시작일", pd.to_datetime(p.get("start_date", "2016-01-04")),
                                   min_value=datetime(2010, 3, 11).date(), max_value=_dmax,
                                   key="ds_add_start")
        ac3, _ = st.columns(2)
        add_cap = ac3.number_input("시작 자본 ($)", value=10000.0, step=1000.0, key="ds_add_cap")
        if st.button("✅ 계좌 등록", type="primary", key="ds_add_btn", use_container_width=True):
            nm = add_name.strip()
            if not nm:
                st.error("계좌 이름을 입력하세요.")
            elif nm in accounts:
                st.error(f"'{nm}' 계좌가 이미 존재합니다.")
            else:
                accounts[nm] = {
                    "params": {k: p.get(k) for k in (
                        "ag_div", "ag_buy", "ag_sell_alpha", "ag_hold_alpha", "sf_div", "sf_hold",
                        "sf_buy1", "sf_buy2", "sf_sell", "sf_ma_base", "sf_weights_str",
                        "ma_weeks", "peak_thr", "dn")},
                    "os_start": str(add_start), "os_capital": float(add_cap),
                    "mode_source": _normalize_src(p.get("mode_source", "자동")),
                }
                # sf_weights_str → sf_weights 키 정규화
                accounts[nm]["params"]["sf_weights"] = p.get("sf_weights_str", "6, 13, 20, 27, 34")
                cfg["accounts"] = accounts
                _save_ds_config(cfg)
                st.success(f"✅ '{nm}' 계좌 등록 완료.")
                st.rerun()

    keys = list(accounts.keys())
    if not keys:
        st.info("등록된 계좌가 없습니다. 위에서 계좌를 추가하세요.")
        return

    tabs = st.tabs([f"📊 {k}" for k in keys])
    for i, (tab, key) in enumerate(zip(tabs, keys)):
        with tab:
            _render_ds_account(key, accounts[key], cfg, p, i)


def _render_ds_account(acct_key, acct_data, cfg, p, idx):
    sfx = f"a{idx}"
    os_start = acct_data.get("os_start", "2016-01-04")
    os_capital = float(acct_data.get("os_capital", 10000))
    ds_p, mode_rule = _acct_params(acct_data, p)

    # ── 1) 파라미터 표시 + 수정 ──
    with st.container(border=True):
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("공격(분할/매수%)", f"{ds_p.ag_divisions} / {ds_p.ag_buy_pct:.0f}%")
        m2.metric("공격(매도α/보유α)", f"{ds_p.ag_sell_alpha} / {ds_p.ag_hold_alpha}")
        m3.metric("방어(분할/보유)", f"{ds_p.sf_divisions} / {ds_p.sf_hold}일")
        m4.metric("방어(매수1/2/매도)", f"{ds_p.sf_buy_pct1}/{ds_p.sf_buy_pct2}/{ds_p.sf_sell_pct}")
        m5.metric("모드(MA/천장/이탈)", f"{mode_rule['ma_weeks']}/{int(mode_rule['peak_thr'])}/{int(mode_rule['dn'])}")
        with st.expander("✏️ 파라미터 수정"):
            st.caption("💡 프리셋 — 버튼에 마우스를 올리면 파라미터/성과를 볼 수 있습니다.")
            _pcols = st.columns(max(len(_DS_PRESETS), 2))
            for _pi, pc in enumerate(_DS_PRESETS):
                if _pcols[_pi].button(pc["label"], key=f"ds_preset_{_pi}{sfx}", help=pc["help"],
                                      use_container_width=True):
                    for k in ("ag_div", "ag_buy", "ag_sell_alpha", "ag_hold_alpha", "sf_div",
                              "sf_hold", "sf_buy1", "sf_buy2", "sf_sell", "sf_ma_base", "sf_weights"):
                        acct_data.setdefault("params", {})[k] = pc[k]
                    cfg["accounts"][acct_key] = acct_data
                    _save_ds_config(cfg)
                    st.success(f"✅ '{pc['label']}' 적용됨")
                    st.rerun()
            e1, e2, e3, e4 = st.columns(4)
            v_agdiv = e1.number_input("공분할", 1, 12, ds_p.ag_divisions, key=f"ds_e_agdiv{sfx}")
            v_agbuy = e2.number_input("공매수%", 0.0, 30.0, ds_p.ag_buy_pct, 0.5, key=f"ds_e_agbuy{sfx}")
            v_agsa = e3.number_input("공매도α", 0.1, 3.0, ds_p.ag_sell_alpha, 0.1, key=f"ds_e_agsa{sfx}")
            v_agha = e4.number_input("공보유α", 0.1, 5.0, ds_p.ag_hold_alpha, 0.1, key=f"ds_e_agha{sfx}")
            f1, f2, f3, f4 = st.columns(4)
            v_sfdiv = f1.number_input("방분할", 1, 12, ds_p.sf_divisions, key=f"ds_e_sfdiv{sfx}")
            v_sfhold = f2.number_input("방보유", 1, 60, ds_p.sf_hold, key=f"ds_e_sfhold{sfx}")
            v_sfb1 = f3.number_input("방매수1", -10.0, 10.0, ds_p.sf_buy_pct1, 0.1, key=f"ds_e_sfb1{sfx}")
            v_sfb2 = f4.number_input("방매수2", 0.0, 30.0, ds_p.sf_buy_pct2, 0.1, key=f"ds_e_sfb2{sfx}")
            g1, g2, g3 = st.columns(3)
            v_sfsell = g1.number_input("방매도", 0.0, 10.0, ds_p.sf_sell_pct, 0.1, key=f"ds_e_sfsell{sfx}")
            v_sfma = g2.number_input("MA기준", 2, 10, ds_p.sf_ma_base, key=f"ds_e_sfma{sfx}")
            v_sfw = g3.text_input("티어비중%", ", ".join(str(int(x)) for x in ds_p.sf_tier_weights),
                                  key=f"ds_e_sfw{sfx}")
            h1, h2, h3 = st.columns(3)
            v_ma = h1.number_input("추세MA(주)", 5, 60, mode_rule["ma_weeks"], key=f"ds_e_ma{sfx}")
            v_pk = h2.number_input("천장wRSI", 50.0, 80.0, mode_rule["peak_thr"], 1.0, key=f"ds_e_pk{sfx}")
            v_dn = h3.number_input("이탈wRSI", 30.0, 50.0, mode_rule["dn"], 1.0, key=f"ds_e_dn{sfx}")
            if st.button("💾 파라미터 저장", type="primary", key=f"ds_save_p{sfx}"):
                acct_data["params"] = {
                    "ag_div": v_agdiv, "ag_buy": v_agbuy, "ag_sell_alpha": v_agsa, "ag_hold_alpha": v_agha,
                    "sf_div": v_sfdiv, "sf_hold": v_sfhold, "sf_buy1": v_sfb1, "sf_buy2": v_sfb2,
                    "sf_sell": v_sfsell, "sf_ma_base": v_sfma, "sf_weights": v_sfw,
                    "ma_weeks": v_ma, "peak_thr": v_pk, "dn": v_dn}
                cfg["accounts"][acct_key] = acct_data
                _save_ds_config(cfg)
                st.success("✅ 저장되었습니다.")
                st.rerun()

    # ── 1.5) 모드 소스 (계좌별 저장) ──
    saved_src = _render_ds_source_box(acct_key, acct_data, cfg, sfx)

    # ── 2) 이름변경 / 삭제 ──
    mg1, mg2, _ = st.columns([1, 1, 4])
    if mg1.button("✏️ 이름 변경", key=f"ds_rn_btn{sfx}"):
        st.session_state[f"ds_rn{sfx}"] = True
    if mg2.button("🗑️ 계좌 삭제", key=f"ds_del_btn{sfx}"):
        st.session_state[f"ds_del{sfx}"] = True
    if st.session_state.get(f"ds_rn{sfx}"):
        r1, r2, r3, _ = st.columns([2, 1, 1, 3])
        new_nm = r1.text_input("새 이름", acct_key, key=f"ds_newnm{sfx}")
        if r2.button("✅", key=f"ds_rn_ok{sfx}"):
            nm = new_nm.strip()
            if nm and nm not in cfg["accounts"]:
                cfg["accounts"] = {(nm if k == acct_key else k): v for k, v in cfg["accounts"].items()}
                _save_ds_config(cfg)
                op, npth = _ds_history_path(acct_key), _ds_history_path(nm)
                if os.path.exists(op):
                    os.rename(op, npth)
                st.session_state.pop(f"ds_rn{sfx}", None)
                st.rerun()
        if r3.button("❌", key=f"ds_rn_no{sfx}"):
            st.session_state.pop(f"ds_rn{sfx}", None)
            st.rerun()
    if st.session_state.get(f"ds_del{sfx}"):
        st.warning(f"⚠️ **{acct_key}** 계좌를 삭제하시겠습니까? (히스토리 포함)")
        d1, d2, _ = st.columns([1, 1, 4])
        if d1.button("✅ 삭제", type="primary", key=f"ds_del_ok{sfx}"):
            del cfg["accounts"][acct_key]
            _save_ds_config(cfg)
            hp = _ds_history_path(acct_key)
            if os.path.exists(hp):
                os.remove(hp)
            st.session_state.pop(f"ds_del{sfx}", None)
            st.rerun()
        if d2.button("❌ 취소", key=f"ds_del_no{sfx}"):
            st.session_state.pop(f"ds_del{sfx}", None)
            st.rerun()

    # ── 3) 시작일 / 자본 ──
    s1, s2, s3 = st.columns([2, 2, 1])
    in_start = s1.date_input("시작일", pd.to_datetime(os_start).date(),
                             min_value=datetime(2010, 3, 11).date(),
                             max_value=max(datetime.today().date(), datetime(2026, 12, 31).date()),
                             key=f"ds_start{sfx}")
    in_cap = s2.number_input("시작 자본 ($)", value=os_capital, step=1000.0, key=f"ds_cap{sfx}")
    if s3.button("💾 저장", key=f"ds_savesc{sfx}"):
        acct_data["os_start"] = str(in_start)
        acct_data["os_capital"] = float(in_cap)
        cfg["accounts"][acct_key] = acct_data
        _save_ds_config(cfg)
        st.success("저장됨")
        st.rerun()

    # ── 4) 자본 조정 ──
    from common.analysis import recalc_adj_history as _recalc
    with st.expander("💰 자본 조정 (증액 / 감액)"):
        st.caption("증액: 양수 · 감액: 음수. (v1: 조정 합계를 시작 자본에 반영)")
        raw = acct_data.get("capital_adj_history", "[]")
        try:
            adj = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(adj, list):
                adj = []
        except Exception:
            adj = []
        ad1, ad2 = st.columns([2, 1])
        adj_date = ad1.date_input("적용 날짜", datetime.today().date(),
                                  min_value=datetime(2010, 3, 11).date(),
                                  max_value=max(datetime.today().date(), datetime(2026, 12, 31).date()),
                                  key=f"ds_adj_d{sfx}")
        adj_amt = ad1.number_input("조정 금액 ($)", value=0.0, step=500.0, key=f"ds_adj_a{sfx}")
        if ad2.button("💰 적용", key=f"ds_adj_btn{sfx}", disabled=(adj_amt == 0)):
            adj.append({"날짜": adj_date.strftime("%Y-%m-%d"), "조정금액": float(adj_amt),
                        "누적자본금": 0.0, "메모": "증액" if adj_amt > 0 else "감액"})
            adj, _fc = _recalc(adj, os_capital)
            acct_data["capital_adj_history"] = json.dumps(adj, ensure_ascii=False)
            cfg["accounts"][acct_key] = acct_data
            _save_ds_config(cfg)
            st.rerun()
        if adj:
            dfa = pd.DataFrame(adj)
            st.dataframe(dfa[["날짜", "조정금액", "메모"]], use_container_width=True, hide_index=True)

    # 유효 자본 (v1: 시작자본 + today 이하 조정 합계)
    net_adj = 0.0
    raw = acct_data.get("capital_adj_history", "[]")
    try:
        for it in (json.loads(raw) if isinstance(raw, str) else raw):
            if pd.Timestamp(it.get("날짜")) <= pd.Timestamp(datetime.today().date()):
                net_adj += float(it.get("조정금액", 0))
    except Exception:
        pass
    eff_capital = os_capital + net_adj
    ds_p.initial_capital = eff_capital

    # ── 5) 주문표 로드 (저장된 모드 소스 사용) ──
    forced = saved_src if saved_src in ("공격", "방어") else None
    use_algoc = (saved_src == "원본시트")
    st.markdown(f"**현재 모드 소스**: {_SRC_LBL[saved_src]}"
                + (f" · 원본탭 `{acct_data.get('algoc_tab','Rocket')}`" if use_algoc else ""))
    if saved_src != "자동":
        st.caption("📜 과거 보유 슬롯은 **원전략 실제모드(2016~2026 번들)** 로 재현됩니다. "
                   "→ 원전략을 과거부터 돌린 것과 동일한 현재 상태.")
    if st.button("📋 주문표 로드", type="primary", key=f"ds_load{sfx}", use_container_width=True):
        try:
            px_df = get_soxl_data()
            algoc_info = None
            shared_today = None
            if use_algoc:
                au = acct_data.get("algoc_url", "").strip()
                at = (acct_data.get("algoc_tab", "Rocket") or "Rocket").strip()
                if not au:
                    st.error("원본 시트 URL을 모드 소스 박스에서 입력·저장하세요.")
                    return
                rows = _read_algoc_orders(au, at)
                c1 = float(px_df['close'].iloc[-1]); c2 = float(px_df['close'].iloc[-2])
                m_inf, det = _infer_mode_from_orders(rows, c1, c2, ds_p)
                if m_inf is None:
                    st.error(f"모드 역산 실패 — 원본 주문: {rows}")
                    return
                forced = m_inf
                shared_today = m_inf
                algoc_info = {"mode": m_inf, "detail": det, "orders": rows}
            # 모드맵: 자동=하이브리드 / 그 외=원전략 실제모드(번들+공유)
            if saved_src == "자동":
                mode_map = build_auto_mode_map(px_df, ma_weeks=mode_rule["ma_weeks"],
                                               peak_thr=mode_rule["peak_thr"], dn=mode_rule["dn"])
            else:
                extra = dict(_load_shared_modes())   # 공유 일별 모드 (번들 이후)
                if shared_today:
                    _nd = next_trading_days(px_df.index[-1], 1)
                    _kd = (_nd[0] if len(_nd) else px_df.index[-1]).date()
                    extra[str(_kd)] = shared_today
                mode_map = build_original_mode_map(px_df, extra_modes=extra)
            r = build_today_orders(px_df, ds_p, mode_map=mode_map, start_date=str(in_start),
                                   mode_rule=mode_rule, forced_mode=forced)
            r["algoc"] = algoc_info
        except Exception as e:
            st.error(f"주문표 생성 실패: {e}")
            return
        st.session_state[f"ds_os{sfx}"] = r

    r = st.session_state.get(f"ds_os{sfx}")
    if r is None:
        st.info("👆 '주문표 로드'를 클릭하면 다음 거래일 주문이 생성됩니다.")
        return

    # ── 원본시트 모드 역산 결과 ──
    ai = r.get("algoc")
    if ai:
        d = ai["detail"]
        st.success(f"🔗 **원본시트 자동 모드 = {ai['mode']}** "
                   f"(원본 매수가 ${d.get('원본매수가')} ↔ 방어후보 ${d.get('후보_방어')} / "
                   f"공격후보 ${d.get('후보_공격FI+')}·${d.get('후보_공격FI-')})")
        with st.expander("📥 원본 시트에서 읽은 주문"):
            st.dataframe(pd.DataFrame(ai["orders"], columns=["구분", "거래방법", "가격", "수량"]),
                         hide_index=True, use_container_width=True)
        # 관리자: 다른 유저에게 오늘 모드 공유
        _today_str = str(pd.Timestamp(r["last_date"]).date())
        if st.button(f"📢 오늘({_today_str}) 모드 '{ai['mode']}' 다른 유저에게 공유 저장",
                     key=f"ds_share{sfx}"):
            ok, msg = _save_shared_mode(_today_str, ai["mode"])
            (st.success if ok else st.error)(f"공유 {'완료' if ok else '실패'}: {msg}")

    # ── 요약 ──
    od = pd.Timestamp(r["order_date"]).strftime("%Y-%m-%d")
    ld = pd.Timestamp(r["last_date"]).strftime("%Y-%m-%d")
    _src = _SRC_LBL.get(saved_src, saved_src)
    st.markdown(f"**주문일** {od} · **기준종가**({ld}) ${r['last_close']:.2f} · "
                f"**모드** `{r['next_mode']}` ({_src}) · **보유** {r['n_pos']}/{r['divisions']}슬롯")
    sm1, sm2, sm3, sm4 = st.columns(4)
    sm1.metric("총자산", f"${r['total_asset']:,.0f}")
    sm2.metric("현금", f"${r['cash']:,.0f}")
    sm3.metric("누적실현", f"${r['cum_realized']:,.0f}")
    sm4.metric("유효자본", f"${eff_capital:,.0f}", delta=f"{net_adj:+,.0f}" if net_adj else None)

    # ── 내일 주문 ──
    st.markdown("##### 📌 다음 거래일 주문")
    if r["orders"]:
        rows = []
        for o in r["orders"]:
            gubun = ("🔴 MOC매도" if o["거래방법"] == "MOC" else
                     "🔵 LOC매도" if o["구분"] == "매도" else "🟠 LOC매수")
            price = "시장가(종가)" if o["가격"] is None else f"${o['가격']:,.2f}"
            amt = (o["수량"] * (o["가격"] or r["last_close"]))
            rows.append({"구분": gubun, "사유": o["사유"], "주문가": price,
                         "수량": f"{o['수량']:,}주", "예상금액": f"${amt:,.0f}",
                         "비고 (계산 근거)": o.get("비고", "")})

        def _style(row):
            s = [""] * len(row)
            ix = list(row.index).index("구분")
            v = row["구분"]
            s[ix] = ("color:#C62828;font-weight:bold" if "MOC" in v else
                     "color:#1565C0;font-weight:bold" if "매도" in v else
                     "color:#E65100;font-weight:bold")
            return s
        st.dataframe(pd.DataFrame(rows).style.apply(_style, axis=1),
                     use_container_width=True, hide_index=True, height=38 + 35 * len(rows))
        st.caption("💡 매도 LOC는 목표가 이상 종가 시 체결 · 매수 LOC는 주문가 이하 종가 시 체결 · "
                   "MOC는 손절일 도래분(시장가 종가 청산). MOC가 있는 날은 다른 LOC 매도가 보류됩니다.")
    else:
        st.info("다음 거래일 주문 없음 (조건 미충족).")

    # ── 보유 슬롯 ──
    st.markdown("##### 📦 현재 보유 슬롯")
    if r["positions"]:
        pr = []
        for pv in r["positions"]:
            pnl = (r["last_close"] / pv["매수가"] - 1) * 100
            pr.append({"모드": pv["모드"], "티어": f"T{pv['티어']}",
                       "매수일": pd.Timestamp(pv["매수일"]).strftime("%Y-%m-%d"),
                       "매수가": f"${pv['매수가']:.2f}", "수량": f"{pv['수량']:,}주",
                       "매도목표": f"${pv['매도목표']:.2f}" if pv["매도목표"] else "MA청산",
                       "손절예정일": pd.Timestamp(pv["손절예정일"]).strftime("%Y-%m-%d"),
                       "평가손익": f"{pnl:+.1f}%"})
        st.dataframe(pd.DataFrame(pr), use_container_width=True, hide_index=True,
                     height=38 + 35 * len(pr))
    else:
        st.info("현재 보유 슬롯 없음 (전량 현금).")

    # ── 히스토리 저장 ──
    hc1, hc2 = st.columns([1, 3])
    if hc1.button("💾 오늘 주문 기록 저장", key=f"ds_savehist{sfx}"):
        n_buy = sum(1 for o in r["orders"] if o["구분"] == "매수")
        n_sell = sum(1 for o in r["orders"] if o["구분"] == "매도")
        snap = {"날짜": od, "기준종가": round(r["last_close"], 2), "모드": r["next_mode"],
                "모드소스": saved_src,
                "보유슬롯": r["n_pos"], "매수주문": n_buy, "매도주문": n_sell,
                "현금": round(r["cash"]), "총자산": round(r["total_asset"]),
                "누적실현": round(r["cum_realized"])}
        _save_ds_history(acct_key, snap)
        st.success(f"✅ {od} 기록 저장됨")

    hist = _load_ds_history(acct_key)
    if not hist.empty:
        with st.expander(f"📋 누적 기록 ({len(hist)}건)"):
            st.dataframe(hist.iloc[::-1].reset_index(drop=True), use_container_width=True,
                         hide_index=True, height=min(38 + 35 * len(hist), 400))
            st.download_button("📥 기록 CSV", hist.to_csv(index=False).encode("utf-8-sig"),
                               f"dual_sniper_{acct_key}.csv", "text/csv", key=f"ds_dlhist{sfx}")



# ══════════════════════════════════════════════
# 탭4: 전략 소개 & 성과 분석
# ══════════════════════════════════════════════

def _render_norm_viz():
    """공격모드 매도조건/보유기간 정규화 — α 슬라이더 + 표 + 곡선 차트."""
    s1, s2 = st.columns(2)
    a_sell = s1.slider("매도조건 α", 0.2, 1.5, 0.4, 0.1, key="ds_viz_sa",
                       help="작을수록 곡선이 가팔라짐 (저RSI에서 익절폭 급증)")
    a_hold = s2.slider("보유기간 α", 0.5, 4.0, 2.0, 0.5, key="ds_viz_ha")

    def _clip(r):
        return max(0.0, min(1.0, (r - 35) / 30))
    rs_sell = [65, 62, 59, 56, 53, 50, 47, 44, 41, 38, 35]
    sell = [round(0.1 + 2.9 * (1 - _clip(r)) ** (1 / a_sell), 2) for r in rs_sell]
    rs_hold = [70, 65, 60, 55, 50, 45, 40, 35]
    hold = [int(round(7 + 23 * (1 - _clip(r)) ** (1 / a_hold))) for r in rs_hold]

    g1, g2 = st.columns(2)
    with g1:
        st.caption(f"📈 매도조건 정규화 표 (α = {a_sell})")
        fig = go.Figure(go.Scatter(x=rs_sell, y=sell, mode="lines+markers",
                                   fill="tozeroy", line=dict(color="#4FC3F7", width=2),
                                   marker=dict(size=7), fillcolor="rgba(79,195,247,0.15)"))
        fig.update_layout(height=240, margin=dict(l=0, r=0, t=6, b=0),
                          xaxis=dict(autorange="reversed", title="매수RSI", dtick=3),
                          yaxis=dict(title="매도%", ticksuffix="%"))
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(pd.DataFrame({"매수RSI": rs_sell, "매도기준": [f"{v:.2f}%" for v in sell]}),
                     hide_index=True, use_container_width=True, height=240)
        st.caption("매도기준(%) = 0.1 + 2.9 × (1−x)^(1/α),  x=(매수RSI−35)/30")
    with g2:
        st.caption(f"📈 보유기간 정규화 표 (α = {a_hold})")
        fig2 = go.Figure(go.Scatter(x=rs_hold, y=hold, mode="lines+markers",
                                    fill="tozeroy", line=dict(color="#4FC3F7", width=2),
                                    marker=dict(size=7), fillcolor="rgba(79,195,247,0.15)"))
        fig2.update_layout(height=240, margin=dict(l=0, r=0, t=6, b=0),
                           xaxis=dict(autorange="reversed", title="매수RSI", dtick=5),
                           yaxis=dict(title="보유일", ticksuffix="일"))
        st.plotly_chart(fig2, use_container_width=True)
        st.dataframe(pd.DataFrame({"매수RSI": rs_hold, "보유일": [f"{v}일" for v in hold]}),
                     hide_index=True, use_container_width=True, height=240)
        st.caption("보유일 = 7 + 23 × (1−x)^(1/α),  x=(매수RSI−35)/30")


def _render_mode_now(ma_weeks=36, peak_thr=66.0, dn=42.0):
    """이번 주/지난 주 모드 판정 근거를 실제 수치·계산식으로 표시."""
    from dual_sniper_engine import calc_rsi_wilder
    try:
        pxf = get_soxl_data()
    except Exception as e:
        st.caption(f"(데이터 로드 실패: {e})")
        return
    wc = pxf['close'].resample('W-FRI').last().dropna()
    # 미완성 현재 주봉 제외 (주중 실행 시)
    if len(wc) > 0 and wc.index[-1].date() > pxf.index[-1].date():
        wc = wc.iloc[:-1]
    wv = wc.values.astype(float)
    if len(wv) < ma_weeks + 3:
        st.caption("(주봉 데이터 부족)")
        return
    wrsi = calc_rsi_wilder(wv, 14)
    wma = pd.Series(wv).rolling(ma_weeks).mean().values

    # 최근 6주 표
    rows = []
    for i in range(len(wv) - 6, len(wv)):
        rows.append({"주말(금)": wc.index[i].strftime("%Y-%m-%d"),
                     "주봉종가": f"${wv[i]:.2f}", f"{ma_weeks}주MA": f"${wma[i]:.2f}",
                     "추세(종가>MA)": "✅ 상승" if wv[i] > wma[i] else "❌ 하락",
                     "wRSI(14)": f"{wrsi[i]:.1f}"})
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

    def _decide(v, vp, c, m):
        trend_up = c > m
        crash_lvl = v < dn
        crash_top = (v < vp) and (vp > peak_thr)
        if crash_lvl or crash_top:
            mode = "🟦 방어"
        elif trend_up:
            mode = "🟥 공격"
        else:
            mode = "🟦 방어"
        return mode, trend_up, crash_lvl, crash_top

    # 이번 주(다가오는 세션): 지난주(-1)·지지난주(-2)
    v1, vp1, c1, m1 = wrsi[-1], wrsi[-2], wv[-1], wma[-1]
    d1 = wc.index[-1].strftime("%m/%d"); d2 = wc.index[-2].strftime("%m/%d")
    mode1, t1, cl1, ct1 = _decide(v1, vp1, c1, m1)
    # 지난 주: 지지난주(-2)·지지지난주(-3)
    v2, vp2, c2, m2 = wrsi[-2], wrsi[-3], wv[-2], wma[-2]
    d3 = wc.index[-3].strftime("%m/%d")
    mode2, t2, cl2, ct2 = _decide(v2, vp2, c2, m2)

    cc1, cc2 = st.columns(2)
    with cc1:
        st.markdown(f"**📅 이번 주(다가오는 세션) → {mode1}**")
        st.markdown(f"""
판단 데이터: 지난주({d1})·지지난주({d2}) 확정 주봉
1. **추세필터**: 주봉종가 ${c1:.2f} {'>' if t1 else '≤'} {ma_weeks}주MA ${m1:.2f} → 추세상승 {'✅' if t1 else '❌'}
2. **이탈 경보**(wRSI<{int(dn)}): {v1:.1f} < {int(dn)}? {'🔔 발동' if cl1 else '❌'}
3. **천장 경보**(꺾임): wRSI {v1:.1f} < 직전 {vp1:.1f}(하락) **AND** {vp1:.1f} > {int(peak_thr)}? {'🔔 발동' if ct1 else '❌'}
→ {'방어 경보 발동 → ' if (cl1 or ct1) else ('추세상승 → ' if t1 else '추세하락 → ')}**{mode1}**
""")
    with cc2:
        st.markdown(f"**📅 지난 주 → {mode2}**")
        st.markdown(f"""
판단 데이터: {d2}·{d3} 확정 주봉
1. **추세필터**: 주봉종가 ${c2:.2f} {'>' if t2 else '≤'} {ma_weeks}주MA ${m2:.2f} → 추세상승 {'✅' if t2 else '❌'}
2. **이탈 경보**(wRSI<{int(dn)}): {v2:.1f} < {int(dn)}? {'🔔 발동' if cl2 else '❌'}
3. **천장 경보**(꺾임): wRSI {v2:.1f} < 직전 {vp2:.1f}? **AND** {vp2:.1f} > {int(peak_thr)}? {'🔔 발동' if ct2 else '❌'}
→ {'방어 경보 발동 → ' if (cl2 or ct2) else ('추세상승 → ' if t2 else '추세하락 → ')}**{mode2}**
""")
    st.caption("※ 룩어헤드 없음 — 각 주의 모드는 직전 확정 주봉(지난주/지지난주)만 사용. "
               "백테스트는 추가로 1주 더 지연(전주 기준)되며, 실거래 주문표는 위 '이번 주' 값을 사용합니다.")


def render_intro_tab(params=None):
    st.subheader("📖 Dual Sniper Pro — 전략 소개")

    st.markdown("""
**SOXL** 대상 **공격/방어 2모드 슬롯(티어) 기반 그리드 매매 전략**입니다.
원전략 스프레드시트(10.5년, 2,621거래일)와 PDF를 역설계하여 백테스트 엔진을 구축했고,
**실제 모드를 입력하면 원전략을 사실상 완벽 재현**합니다.

| 검증 항목 | 엔진(실제모드) | 원전략 | 자동모드(하이브리드) |
|---|---|---|---|
| CAGR | 96.5% | 96.86% | **76%** |
| MDD | -26.1% | -26.14% | **-29%** |
| Calmar | 3.70 | 3.69 | **2.64** |
| 매수티어 일치 | 98.5% | — | — |
| 매도사유 일치 | 97.1% | — | — |

> · **엔진(실제모드)**: 원전략의 모드를 그대로 입력 → 원전략 재현 (모드 정확도 검증용)
> · **자동모드(하이브리드)**: 모드 전환이 비공개라 자체 설계한 규칙(36주MA+천장방어)으로 운용 — 실제로 우리가 쓰는 값. (2016~ 기준, train≈test로 과최적화 없음)
""")

    with st.expander("🟥 공격모드 로직 (추세 상승 구간)", expanded=True):
        st.markdown("""
**컨셉**: 빈번한 소액 회전. 슬롯(티어)별 독립 관리.

**📍 FI (Follow Indicator) 란?**
- **전일 종가의 등락 방향**을 나타내는 추세 추종 신호입니다.
- `FI > 0` = 전일 **상승 마감** (종가 > 전전일 종가) → 모멘텀 ON
- `FI ≤ 0` = 전일 **하락/보합 마감** → 모멘텀 OFF
- 즉 "어제 시장이 올랐나 내렸나"로 오늘 매수 공격성을 결정합니다.

**매수** — 매일 1개 티어 LOC 매수주문 (전일 FI 부호로 분기)
- 전일 **상승**(FI>0): `전일종가 × (1 + 8.0%)` → 모멘텀 타고 **천장을 넓게** 열어 적극 매수 (8% 이내면 체결)
- 전일 **하락**(FI≤0): `전일종가 × (1 − 0.1%)` → 떨어지는 칼날 주의, **0.1% 더 빠지면** 신중 진입
- 슬롯 6개, 가장 낮은 빈 슬롯을 채움

**매수RSI 정규화** — 매도 익절폭과 보유기간을 매수 시점 RSI로 연속 조절. **α를 바꾸면 곡선이 즉시 갱신**됩니다.
""")
        _render_norm_viz()
        st.markdown("""
→ RSI 낮은(공포) 구간 매수일수록 **익절폭을 크게, 보유를 길게**. (반등 여력이 크니 넉넉히 기다림)

**1티어 보류** — 전일종가 > 전일 MA(5)면 슬롯1의 LOC 매도를 보류(MOC 제외) → 상승추세에서 첫 슬롯을 끌고 감.
""")

    with st.expander("🟦 방어모드 로직 (추세 하락/횡보 구간)", expanded=True):
        st.markdown("""
**컨셉**: 보수적. **역피라미딩**(하락할수록 큰 물량) 후 MA 반등 시 전량 익절.

**매수** — 두 조건 중 더 낮은 가격으로 LOC
- 조건1(MA): `(0.994 × 직전 2일 종가합) / (3 − 0.994)`  *(MA −0.6%)*
- 조건2(종가): `전일종가 × (1 + 5.5%)`
- 슬롯 5개, **티어별 매수비중 6 → 13 → 20 → 27 → 34%**

**매도** — MA 돌파 시 **전량 청산** (LOC)
- `매도가 = (1.007 × 직전 2일 종가합) / (3 − 1.007)`  *(3일 MA +0.7%)*

**보유기간** — 8거래일 고정, 만기 MOC 청산.
""")

    with st.expander("🧭 모드 전환 규칙 (자체 하이브리드)", expanded=True):
        st.markdown("""
원전략의 모드 전환 규칙은 **비공개**라, 우리는 **두 개의 질문**으로 모드를 정하는 직관적 규칙을 씁니다.
매주 금요일 주봉이 확정되면, 그 데이터로 다음 주 모드를 결정합니다 (룩어헤드 없음).

#### 🟥 언제 공격? — "추세가 살아있을 때"
- 주간 종가가 **36주(약 9개월) 이동평균 위**에 있으면 = 큰 상승 추세 → **공격**
- 비유: 강물(장기추세)이 위로 흐르면 배도 위로.

#### 🟦 언제 방어? — "천장에서 꺾이거나, 바닥으로 무너질 때" (이게 우선!)
방어는 **두 가지 경보** 중 하나만 울려도 발동합니다:
1. **🔔 천장 경보**: 주간 RSI가 **고점(66 초과)에서 꺾여 내려오기 시작** → 과열 후 하락 전조
2. **🔔 이탈 경보**: 주간 RSI가 **42 아래로 추락** → 추세 자체가 무너짐

→ **방어 경보가 우선.** 추세가 아무리 좋아 보여도 천장에서 꺾이면 즉시 방어로 전환해 폭락을 피합니다.

**왜 이렇게?** 공격은 "추세 추종"으로 큰 상승을 먹고, 방어는 "천장·붕괴 감지"로 큰 낙폭을 피합니다.
방어를 우선시해 **MDD를 지키는 것**이 이 규칙의 핵심입니다.

**백테스트(2016~)**: CAGR 76% / MDD -29% / **Calmar 2.64** · train≈test(2.68≈2.74)로 과최적화 없음.

> 지표: 주간 RSI(14, Wilder) · 36주 단순이동평균 · 종가 = yfinance 미조정(raw)
""")
        st.markdown("---")
        st.markdown("##### 🔍 지금 모드는 어떻게 정해졌나? (실시간 계산)")
        _render_mode_now()

    with st.expander("⚠️ 유의사항 & 한계", expanded=True):
        st.markdown("""
- **2011~2015(반도체 약세장) 포함 시 Calmar 1.1로 하락** — 좋은 숫자는 부분적으로 반도체 secular 강세장 덕. 레버리지 추세전략의 정직한 한계.
- 원전략 실제모드(Calmar 3.69)의 진짜 강점은 **위기연도(2020 +143%, 2022 +124%) 정밀 타이밍** — 비공개 규칙의 고유 엣지. 자체 자동모드는 여기서 한발 늦음.
- **최고 성과를 원하면 수동 모드 입력**(원전략 시그널 추종)이 자동모드보다 우수합니다.
- 실거래 주문표의 '다음 세션 모드'는 가장 최근 확정 주봉(지난주/지지난주)으로 산출 — 백테스트의 1주 지연과는 분리.
""")

    st.divider()

    # ══════════════════════════════════════════════
    # 성과 분석
    # ══════════════════════════════════════════════
    st.subheader("📊 전략 성과 분석")
    st.caption("사이드바의 파라미터·기간 설정 또는 기본 프리셋으로 종합 성과를 분석합니다.")

    if params is None:
        st.info("성과 분석은 사이드바에서 듀얼스나이퍼를 선택한 상태에서 이용하세요.")
        return

    src = st.radio("파라미터 / 모드 소스",
                   ["🧭 사이드바 설정값", "⚖️ 기본 (자동모드)", "📜 원전략 실제모드 (2016~)"],
                   horizontal=True, key="ds_intro_src",
                   help="자동모드=우리 하이브리드 규칙 / 원전략 실제모드=로케트셋 실제 모드(번들)로 원전략 그대로 재현")
    use_orig_modes = (src == "📜 원전략 실제모드 (2016~)")

    if src in ("⚖️ 기본 (자동모드)", "📜 원전략 실제모드 (2016~)"):
        ds_p = DualSniperParams(
            ag_buy_inclusive=False, sf_buy_inclusive=False,
            fee_rate=float(params.get("fee_rate", 0.0)) / 100, sec_fee_rate=0.0)
        mr = {"ma_weeks": 36, "peak_thr": 66.0, "dn": 42.0}
        cap = float(params.get("initial_capital", 10000))
        start, end = str(params.get("start_date", "2016-01-04")), str(params.get("end_date"))
    else:
        ds_p = _make_params(params)
        mr = {"ma_weeks": params["ma_weeks"], "peak_thr": params["peak_thr"], "dn": params["dn"]}
        cap = float(params["initial_capital"])
        start, end = str(params["start_date"]), str(params["end_date"])

    with st.expander("🔎 적용 파라미터 확인"):
        st.markdown(f"""
- **기간**: {start} ~ {end} · **초기자본** ${cap:,.0f}
- **공격**: 분할 {ds_p.ag_divisions} · 매수 {ds_p.ag_buy_pct}% · 매도α {ds_p.ag_sell_alpha} · 보유α {ds_p.ag_hold_alpha}
- **방어**: 분할 {ds_p.sf_divisions} · 보유 {ds_p.sf_hold}일 · 매수1 {ds_p.sf_buy_pct1}% · 매수2 {ds_p.sf_buy_pct2}% · 매도 {ds_p.sf_sell_pct}%
- **티어비중**: {', '.join(str(int(x)) for x in ds_p.sf_tier_weights)}
- **모드규칙**: 추세MA {mr['ma_weeks']}주 · 천장 {int(mr['peak_thr'])} · 이탈 {int(mr['dn'])}
""")

    if st.button("▶ 성과 분석 실행", type="primary", key="ds_intro_run", use_container_width=True):
        try:
            px_df = get_soxl_data()
            if use_orig_modes:
                mode_map = build_original_mode_map(px_df, extra_modes=dict(_load_shared_modes()))
            else:
                mode_map = build_auto_mode_map(px_df, **mr)
        except Exception as e:
            st.error(f"데이터/모드 로드 실패: {e}")
            return
        with st.spinner("백테스트 실행 중..."):
            log, trades = run_backtest(px_df, ds_p, mode_map=mode_map,
                                       start_date=start, return_trades=True)
        log = log[(log['날짜'] >= pd.Timestamp(start)) & (log['날짜'] <= pd.Timestamp(end))].reset_index(drop=True)
        st.session_state["ds_intro_log"] = log
        st.session_state["ds_intro_trades"] = trades
        st.session_state["ds_intro_cap"] = cap
        st.session_state["ds_intro_range"] = (start, end)
        st.session_state["ds_intro_dsp"] = ds_p
        st.session_state["ds_intro_mr"] = mr
        st.session_state["ds_intro_orig"] = use_orig_modes

    log = st.session_state.get("ds_intro_log")
    if log is None or log.empty:
        st.info("👆 '성과 분석 실행' 버튼을 클릭하면 종합 성과를 확인할 수 있습니다.")
        return
    trades = st.session_state["ds_intro_trades"]
    cap = st.session_state["ds_intro_cap"]
    start, end = st.session_state["ds_intro_range"]
    _render_ds_performance(log, trades, cap, start, end,
                           st.session_state.get("ds_intro_dsp"),
                           st.session_state.get("ds_intro_mr"),
                           st.session_state.get("ds_intro_orig", False))


def _render_ds_performance(log, trades, cap, start, end, ds_p=None, mr=None, use_orig=False):
    from common.analysis import (compute_annual_stats, compute_monthly_pivot,
                                  compute_sharpe_sortino, compute_rolling_perf, compute_bnh)
    assets = log['총자산'].values.astype(float)
    m = compute_metrics(log, trades, cap)
    sharpe, sortino = compute_sharpe_sortino(assets)

    # ── 핵심 지표 ──
    st.markdown("##### 📈 핵심 지표")
    c = st.columns(5)
    c[0].metric("최종 자산", f"${m['최종자산']:,.0f}")
    c[1].metric("총수익률", f"{m['총수익률(%)']:,.0f}%")
    c[2].metric("CAGR", f"{m['CAGR(%)']:.1f}%")
    c[3].metric("MDD", f"{m['MDD(%)']:.1f}%")
    c[4].metric("Calmar", f"{m['Calmar']:.2f}")
    c = st.columns(5)
    c[0].metric("Sharpe", f"{sharpe:.2f}")
    c[1].metric("Sortino", f"{sortino:.2f}")
    c[2].metric("승률", f"{m['승률(%)']:.1f}%")
    c[3].metric("매도횟수", f"{m['총매도횟수']:,}회")
    c[4].metric("평균보유", f"{m['평균보유일']:.1f}일")

    days = len(log)
    ag_ratio = (log['모드'] == '공격').mean() * 100
    bh_assets, bh_idx = compute_bnh(
        get_soxl_data().rename(columns={'close': 'Close'}), start, end, cap)
    c = st.columns(5)
    c[0].metric("거래일수", f"{days:,}일")
    c[1].metric("투자기간", f"{days/252:.1f}년")
    c[2].metric("공격일 비중", f"{ag_ratio:.0f}%")
    c[3].metric("평균손익/건", f"${m['평균손익']:,.0f}")
    if len(bh_assets):
        bh_ret = (bh_assets[-1] / cap - 1) * 100
        c[4].metric("B&H 수익률", f"{bh_ret:,.0f}%",
                    delta=f"{m['총수익률(%)']-bh_ret:+,.0f}%p")

    # ── 자산 곡선 + B&H (로그) ──
    st.markdown("##### 💰 자산 곡선 (로그 스케일)")
    figA = go.Figure()
    figA.add_trace(go.Scatter(x=log['날짜'], y=assets, name="전략 총자산",
                              line=dict(color="#2E86C1", width=1.8)))
    if len(bh_assets):
        figA.add_trace(go.Scatter(x=bh_idx, y=bh_assets, name="B&H (SOXL)",
                                  line=dict(color="gray", dash="dot", width=1.2)))
    figA.update_layout(height=380, yaxis_type="log", hovermode="x unified",
                       margin=dict(l=0, r=0, t=20, b=0), legend=dict(orientation="h", y=1.02))
    st.plotly_chart(figA, use_container_width=True)

    # ── 드로다운 ──
    peak = np.maximum.accumulate(assets)
    dd = (assets - peak) / peak * 100
    st.markdown("##### 📉 드로다운")
    figD = go.Figure(go.Scatter(x=log['날짜'], y=dd, fill="tozeroy",
                                line=dict(color="#E74C3C", width=1)))
    figD.update_layout(height=230, margin=dict(l=0, r=0, t=10, b=0),
                       yaxis_title="DD (%)", hovermode="x unified")
    st.plotly_chart(figD, use_container_width=True)

    # ── 연도별 ──
    hist = log[['날짜', '총자산']].rename(columns={'총자산': '총자산($)'})
    st.markdown("##### 📅 연도별 성과")
    annual = compute_annual_stats(hist, cap)
    # 연도별 공격일 비중 추가
    lg = log.copy(); lg['연도'] = lg['날짜'].dt.year
    agy = lg.groupby('연도').apply(lambda g: round((g['모드'] == '공격').mean() * 100)).to_dict()
    annual['공격일(%)'] = annual['연도'].map(agy)
    ca1, ca2 = st.columns([1, 1])
    ca1.dataframe(annual, use_container_width=True, hide_index=True, height=min(38+35*len(annual), 430))
    figY = px.bar(annual, x="연도", y="연간수익률(%)", color="연간수익률(%)",
                  color_continuous_scale="RdYlGn", text_auto=".0f")
    figY.add_hline(y=0, line_dash="dash", line_color="gray")
    figY.update_layout(height=min(38+35*len(annual), 430), margin=dict(l=0, r=0, t=10, b=0),
                       showlegend=False)
    ca2.plotly_chart(figY, use_container_width=True)

    # ── 월별 히트맵 ──
    st.markdown("##### 🗓️ 월별 수익률 (%)")
    try:
        pivot = compute_monthly_pivot(hist, cap)
        figM = px.imshow(pivot, text_auto=".1f", aspect="auto",
                         color_continuous_scale="RdYlGn", color_continuous_midpoint=0)
        figM.update_layout(height=min(60+30*len(pivot), 480), margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(figM, use_container_width=True)
    except Exception:
        st.caption("월별 데이터 부족")

    # ── 롤링 1년 ──
    if days > 260:
        st.markdown("##### 🔄 롤링 1년 (252거래일) CAGR / MDD")
        rc, rm = compute_rolling_perf(assets, 252)
        figR = make_subplots(specs=[[{"secondary_y": True}]])
        figR.add_trace(go.Scatter(x=log['날짜'], y=rc, name="롤링 CAGR(%)",
                                  line=dict(color="#27AE60")), secondary_y=False)
        figR.add_trace(go.Scatter(x=log['날짜'], y=rm, name="롤링 MDD(%)",
                                  line=dict(color="#E74C3C")), secondary_y=True)
        figR.update_layout(height=300, hovermode="x unified", margin=dict(l=0, r=0, t=10, b=0),
                           legend=dict(orientation="h", y=1.02))
        st.plotly_chart(figR, use_container_width=True)

    # ── 거래 통계 ──
    if len(trades):
        st.markdown("##### 🧾 거래 통계")
        wins = trades[trades['실현손익'] > 0]
        losses = trades[trades['실현손익'] <= 0]
        ag_tr = trades[trades['모드'] == '공격']
        sf_tr = trades[trades['모드'] == '방어']
        t = st.columns(4)
        t[0].metric("익절 / 손절", f"{len(wins)} / {len(losses)}")
        t[1].metric("평균 익절 / 손절",
                    f"${wins['실현손익'].mean():,.0f} / ${losses['실현손익'].mean():,.0f}"
                    if len(wins) and len(losses) else "-")
        t[2].metric("공격 거래", f"{len(ag_tr):,}건")
        t[3].metric("방어 거래", f"{len(sf_tr):,}건")
        with st.expander(f"📋 전체 거래 기록 ({len(trades):,}건)"):
            td = trades.copy()
            td['매수일'] = pd.to_datetime(td['매수일']).dt.strftime('%Y-%m-%d')
            td['매도일'] = pd.to_datetime(td['매도일']).dt.strftime('%Y-%m-%d')
            st.dataframe(td, use_container_width=True, hide_index=True, height=400)
            st.download_button("📥 거래기록 CSV", td.to_csv(index=False).encode('utf-8-sig'),
                               "dual_sniper_perf_trades.csv", "text/csv", key="ds_intro_dl")

    dates = pd.to_datetime(log['날짜']).tolist()

    # ── 회복력 분석 (10% 이상 하락 에피소드) ──
    st.divider()
    st.markdown("##### 🛡️ 회복력 분석 (10% 이상 하락 에피소드)")
    st.caption("고점 대비 10% 이상 하락한 구간별로, 어디까지 빠지고 얼마 만에 회복했는지 분석합니다.")
    rec = []
    pv = float(assets[0]); pi = 0; indd = False; tv = pv; ti = 0
    for i in range(1, len(assets)):
        cur = float(assets[i]); ddp = (cur - pv) / pv * 100
        if not indd:
            if cur > pv:
                pv, pi = cur, i
            elif ddp <= -10:
                indd = True; tv, ti = cur, i
        else:
            if cur < tv:
                tv, ti = cur, i
            if cur >= pv:
                rec.append({"고점": str(dates[pi].date()), "고점 자산": f"${pv:,.0f}",
                            "최대하락 시점": str(dates[ti].date()), "저점 자산": f"${tv:,.0f}",
                            "하락율": f"{(tv-pv)/pv*100:.1f}%", "회복 시점": str(dates[i].date()),
                            "기간(일)": (dates[i]-dates[pi]).days})
                indd = False; pv, pi = cur, i; tv, ti = cur, i
    if indd:
        rec.append({"고점": str(dates[pi].date()), "고점 자산": f"${pv:,.0f}",
                    "최대하락 시점": str(dates[ti].date()), "저점 자산": f"${tv:,.0f}",
                    "하락율": f"{(tv-pv)/pv*100:.1f}%", "회복 시점": "미회복 ⏳",
                    "기간(일)": (dates[-1]-dates[pi]).days})
    if rec:
        def _clr(v):
            s = str(v)
            return "color:#C62828;font-weight:bold" if (("%" in s and s.startswith("-")) or "미회복" in s) else ""
        st.dataframe(pd.DataFrame(rec).style.map(_clr), hide_index=True, use_container_width=True)
    else:
        st.info("10% 이상 하락 에피소드가 없습니다.")

    # ── Top 5 최대 낙폭 구간 ──
    peak2 = np.maximum.accumulate(assets)
    dd_s = pd.Series((assets - peak2) / peak2 * 100, index=pd.to_datetime(log['날짜']))
    indd2 = False; st2 = None; pers = []
    for dt2, dv2 in dd_s.items():
        if dv2 < 0 and not indd2:
            indd2 = True; st2 = dt2
        elif dv2 == 0 and indd2:
            indd2 = False
            sub = dd_s[st2:dt2]
            pers.append({"시작일": str(st2.date()), "회복일": str(dt2.date()),
                         "기간(일)": (dt2 - st2).days, "최대낙폭(%)": round(float(sub.min()), 2)})
    if pers:
        ddf = pd.DataFrame(pers).nsmallest(5, "최대낙폭(%)").reset_index(drop=True)
        ddf.index += 1
        st.markdown("**Top 5 최대 낙폭 구간**")
        st.dataframe(ddf.style.format({"최대낙폭(%)": "{:.2f}%"}), use_container_width=True)

    # ── 롤링 성과 (1/2/3년) ──
    st.divider()
    st.markdown("##### 🔄 롤링 성과 분석")
    st.caption("구간별 성과 추이. 특정 시기에만 좋은 게 아닌지 검증합니다.")
    rtabs = st.tabs(["1년 롤링", "2년 롤링", "3년 롤링"])
    for win, rt in zip([252, 504, 756], rtabs):
        with rt:
            if len(assets) > win:
                rc = np.full(len(assets), np.nan); rm = np.full(len(assets), np.nan)
                yrs = win / 252
                for ri in range(win, len(assets)):
                    seg = assets[ri-win:ri+1]
                    rc[ri] = ((seg[-1]/seg[0])**(1/yrs)-1)*100
                    sp = np.maximum.accumulate(seg)
                    rm[ri] = float(((seg-sp)/sp).min())*100
                vmask = ~np.isnan(rc)
                figr = make_subplots(specs=[[{"secondary_y": True}]])
                figr.add_trace(go.Scatter(x=log['날짜'][vmask], y=rc[vmask], name="롤링 CAGR(%)",
                                          line=dict(color="#1565C0", width=2)), secondary_y=False)
                figr.add_trace(go.Scatter(x=log['날짜'][vmask], y=rm[vmask], name="롤링 MDD(%)",
                                          line=dict(color="#EF5350", width=1.5, dash="dot")), secondary_y=True)
                figr.add_hline(y=0, line_dash="dash", line_color="#aaa")
                figr.update_layout(height=320, legend=dict(orientation="h", y=1.1),
                                   margin=dict(l=0, r=0, t=10, b=0))
                st.plotly_chart(figr, use_container_width=True)
                rr = st.columns(3)
                rr[0].metric("평균 CAGR", f"{np.nanmean(rc):+.1f}%")
                rr[1].metric("최고 CAGR", f"{np.nanmax(rc):+.1f}%")
                rr[2].metric("최저 CAGR", f"{np.nanmin(rc):+.1f}%")
            else:
                st.info(f"분석 기간이 {win//252}년보다 짧습니다.")

    # ── 모드별 성과 & 손익률 분포 ──
    if len(trades):
        st.divider()
        st.markdown("##### 🔄 공격 vs 방어 매매 성과")
        def _ms(df, nm):
            if not len(df):
                return {"모드": nm, "매도 횟수": 0, "승률": "-", "평균손익": "-", "총손익": "-"}
            w = (df['실현손익'] > 0).mean()*100
            return {"모드": nm, "매도 횟수": len(df), "승률": f"{w:.1f}%",
                    "평균손익": f"${df['실현손익'].mean():+,.0f}", "총손익": f"${df['실현손익'].sum():+,.0f}"}
        agt = trades[trades['모드'] == '공격']; sft = trades[trades['모드'] == '방어']
        st.dataframe(pd.DataFrame([_ms(agt, "🟥 공격"), _ms(sft, "🟦 방어"), _ms(trades, "📊 전체")]),
                     hide_index=True, use_container_width=True)
        # 손익률 분포 (왜도/첨도)
        st.markdown("##### 📊 매도 수익률 분포")
        pr = trades['수익률(%)'].values.astype(float)
        sk = float(pd.Series(pr).skew()); ku = float(pd.Series(pr).kurtosis())
        figh = go.Figure(go.Histogram(x=pr.tolist(), nbinsx=40,
                                      marker_color="#3498DB"))
        figh.add_vline(x=0, line_dash="dash", line_color="#333")
        figh.add_vline(x=float(np.mean(pr)), line_dash="dot", line_color="#1565C0",
                       annotation_text=f"평균 {np.mean(pr):+.2f}%")
        figh.update_layout(height=300, xaxis_title="수익률(%)", yaxis_title="빈도(회)",
                           margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(figh, use_container_width=True)
        pq = st.columns(4)
        pq[0].metric("평균 수익률", f"{np.mean(pr):+.2f}%")
        pq[1].metric("중앙값", f"{np.median(pr):+.2f}%")
        pq[2].metric("왜도(Skew)", f"{sk:.2f}", help="양수=가끔 큰 수익")
        pq[3].metric("첨도(Kurt)", f"{ku:.2f}", help="높을수록 극단값 빈번")

    # ── 현금 활용률 ──
    if '현금' in log.columns:
        st.divider()
        st.markdown("##### 💵 현금 활용률 (투자 비율)")
        cash = log['현금'].values.astype(float)
        invr = np.clip((1 - cash/assets)*100, 0, 100)
        cu = st.columns(3)
        cu[0].metric("평균 투자비율", f"{np.mean(invr):.1f}%")
        cu[1].metric("최대 투자비율", f"{np.max(invr):.1f}%")
        cu[2].metric("평균 현금비율", f"{100-np.mean(invr):.1f}%")
        figc = go.Figure(go.Scatter(x=log['날짜'], y=invr, fill="tozeroy",
                                    line=dict(color="rgba(255,179,0,0.9)", width=1),
                                    fillcolor="rgba(255,179,0,0.4)"))
        figc.update_layout(height=240, yaxis=dict(range=[0, 100], title="투자비율(%)"),
                           margin=dict(l=0, r=0, t=10, b=0), hovermode="x unified")
        st.plotly_chart(figc, use_container_width=True)

    # ── 파라미터 민감도 분석 ──
    if ds_p is not None and mr is not None:
        st.divider()
        st.markdown("##### 🎛️ 파라미터 민감도 분석")
        st.caption("현재 매수/매도 조건 주변의 Calmar 분포. 과최적화 여부를 확인합니다.")
        with st.expander("🔍 민감도 히트맵 보기 (클릭하여 실행)"):
            smode = st.radio("분석 대상 모드", ["🟦 방어모드", "🟥 공격모드"], horizontal=True,
                             key="ds_sens_mode")
            if st.button("▶ 민감도 분석 실행 (25회)", key="ds_sens_run"):
                try:
                    pxf = get_soxl_data()
                    mm = (build_original_mode_map(pxf, extra_modes=dict(_load_shared_modes()))
                          if use_orig else build_auto_mode_map(pxf, **mr))
                except Exception as e:
                    st.error(f"데이터 로드 실패: {e}")
                else:
                    if smode == "🟦 방어모드":
                        bc, sc = ds_p.sf_buy_pct2, ds_p.sf_sell_pct
                        brange = np.round(np.linspace(max(0.5, bc-3), bc+3, 5), 2)
                        srange = np.round(np.linspace(max(0.1, sc-0.6), sc+0.6, 5), 2)
                    else:
                        bc, sc = ds_p.ag_buy_pct, ds_p.ag_sell_alpha
                        brange = np.round(np.linspace(max(1.0, bc-4), bc+4, 5), 2)
                        srange = np.round(np.linspace(max(0.2, sc-0.2), sc+0.2, 5), 2)
                    heat = np.zeros((5, 5))
                    prog = st.progress(0.0)
                    for bi, bv in enumerate(brange):
                        for si, sv in enumerate(srange):
                            import copy as _copy
                            pp = _copy.copy(ds_p)
                            if smode == "🟦 방어모드":
                                pp.sf_buy_pct2 = float(bv); pp.sf_sell_pct = float(sv)
                            else:
                                pp.ag_buy_pct = float(bv); pp.ag_sell_alpha = float(sv)
                            lg = run_backtest(pxf, pp, mode_map=mm, start_date=start, light=True)
                            lg = lg[(lg['날짜'] >= pd.Timestamp(start)) & (lg['날짜'] <= pd.Timestamp(end))]
                            if len(lg) > 10:
                                mm2 = compute_metrics(lg, None, cap)
                                heat[bi][si] = mm2['Calmar'] if np.isfinite(mm2['Calmar']) else 0
                            prog.progress(min((bi*5+si+1)/25, 1.0))
                    st.session_state["ds_sens"] = (heat, brange, srange, smode, bc, sc)
            sd = st.session_state.get("ds_sens")
            if sd:
                heat, brange, srange, smode, bc, sc = sd
                fh = px.imshow(heat, x=[f"{v}" for v in srange], y=[f"{v}" for v in brange],
                               color_continuous_scale="RdYlGn", text_auto=".2f", aspect="auto",
                               labels={"x": "매도조건", "y": "매수조건", "color": "Calmar"},
                               title=f"Calmar 히트맵 — {smode}")
                fh.update_layout(height=380)
                st.plotly_chart(fh, use_container_width=True)
                st.caption("현재값(매수≈%.2f, 매도≈%.2f) 주변이 고르게 녹색이면 과최적화 위험이 낮습니다." % (bc, sc))

    # ── 무작위 기간 강건성 분석 ──
    if ds_p is not None and mr is not None:
        st.divider()
        st.markdown("##### 🎲 무작위 기간 강건성 분석")
        st.caption("2011~현재 1년(252거래일) 구간 100개를 무작위 추출해 전략 vs B&H를 반복 검증합니다.")
        with st.expander("🔍 강건성 분석 실행 (클릭)"):
            if st.button("▶ 무작위 100구간 분석 시작", key="ds_mc_run"):
                import random
                try:
                    pxf = get_soxl_data()
                    mm = (build_original_mode_map(pxf, extra_modes=dict(_load_shared_modes()))
                          if use_orig else build_auto_mode_map(pxf, **mr))
                except Exception as e:
                    st.error(f"데이터 로드 실패: {e}")
                else:
                    closes = pxf['close'].dropna()
                    idx = closes.index
                    WIN = 252
                    base = idx[idx >= pd.Timestamp("2011-01-01")]
                    valid = [i for i in range(len(idx)) if idx[i] in set(base) and i + WIN < len(idx)]
                    if len(valid) < 100:
                        st.warning("데이터가 100구간에 부족합니다.")
                    else:
                        random.seed(7)
                        chosen = random.sample(valid, 100)
                        sr = []; sm = []; br = []; bm = []; pers = []
                        prog = st.progress(0.0)
                        for ci, si in enumerate(chosen):
                            sdt = str(idx[si].date()); edt = str(idx[si+WIN-1].date())
                            lg = run_backtest(pxf, ds_p, mode_map=mm, start_date=sdt, light=True)
                            lg = lg[(lg['날짜'] >= pd.Timestamp(sdt)) & (lg['날짜'] <= pd.Timestamp(edt))]
                            if len(lg) > 50:
                                a = lg['총자산'].values.astype(float)
                                sr.append((a[-1]/a[0]-1)*100)
                                pk = np.maximum.accumulate(a); sm.append(abs(((a-pk)/pk).min())*100)
                                bp = closes.loc[pd.Timestamp(sdt):pd.Timestamp(edt)].values.astype(float)
                                ba = cap/bp[0]*bp
                                br.append((ba[-1]/ba[0]-1)*100)
                                bpk = np.maximum.accumulate(ba); bm.append(abs(((ba-bpk)/bpk).min())*100)
                                pers.append((sdt, edt))
                            prog.progress(min((ci+1)/100, 1.0))
                        st.session_state["ds_mc"] = (np.array(sr), np.array(sm),
                                                     np.array(br), np.array(bm), pers)
            mc = st.session_state.get("ds_mc")
            if mc:
                sr, sm, br, bm = mc[0], mc[1], mc[2], mc[3]
                pers = mc[4] if len(mc) > 4 else [("", "")] * len(sr)
                def _ms2(a, lbl, pos=True):
                    d = {"구분": lbl, "평균": f"{np.mean(a):+.1f}%", "중앙값": f"{np.median(a):+.1f}%",
                         "표준편차": f"{np.std(a):.1f}%", "최솟값": f"{np.min(a):+.1f}%",
                         "최댓값": f"{np.max(a):+.1f}%", "양(+) 비율": f"{(a>0).mean()*100:.0f}%" if pos else "-"}
                    return d
                st.markdown(f"**📋 요약 통계 (n={len(sr)})**")
                st.dataframe(pd.DataFrame([
                    _ms2(sr, "전략 (1년 수익률)"), _ms2(br, "SOXL B&H (1년 수익률)"),
                    _ms2(sm, "전략 (MDD)", False), _ms2(bm, "SOXL B&H (MDD)", False)]),
                    hide_index=True, use_container_width=True)
                figm = make_subplots(rows=1, cols=2, subplot_titles=["수익률 분포(1년)", "MDD 분포"],
                                     horizontal_spacing=0.12)
                figm.add_trace(go.Histogram(x=br.tolist(), nbinsx=20, name="SOXL B&H",
                                            opacity=0.55, marker_color="#FB8C00"), row=1, col=1)
                figm.add_trace(go.Histogram(x=sr.tolist(), nbinsx=20, name="전략",
                                            opacity=0.6, marker_color="#1565C0"), row=1, col=1)
                figm.add_trace(go.Histogram(x=bm.tolist(), nbinsx=20, name="SOXL B&H",
                                            opacity=0.55, marker_color="#FB8C00", showlegend=False), row=1, col=2)
                figm.add_trace(go.Histogram(x=sm.tolist(), nbinsx=20, name="전략",
                                            opacity=0.6, marker_color="#1565C0", showlegend=False), row=1, col=2)
                figm.update_layout(height=380, barmode="overlay", margin=dict(r=120),
                                   legend=dict(orientation="v", x=1.02, y=1.0))
                st.plotly_chart(figm, use_container_width=True)
                st.caption("전략 수익률 분포가 오른쪽에 모이고 MDD 분포가 왼쪽(낮은 손실)에 모일수록 강건합니다.")

                # ── 100구간 상세 결과 리스트 ──
                det = pd.DataFrame({
                    "시작일": [s for s, _ in pers], "종료일": [e for _, e in pers],
                    "전략 수익률(%)": np.round(sr, 1), "전략 MDD(%)": np.round(sm, 1),
                    "B&H 수익률(%)": np.round(br, 1), "B&H MDD(%)": np.round(bm, 1),
                    "전략-B&H(%p)": np.round(sr - br, 1),
                })
                _sort_opt = st.selectbox("정렬 기준", ["시작일", "전략 수익률(%)", "전략 MDD(%)",
                                                      "전략-B&H(%p)"], key="ds_mc_sort")
                det = det.sort_values(_sort_opt,
                                      ascending=(_sort_opt == "시작일")).reset_index(drop=True)
                det.index += 1

                def _hl(row):
                    c = "#e8f5e9" if row["전략 수익률(%)"] > 0 else "#ffebee"
                    return [f"background-color:{c}"] * len(row)
                st.markdown(f"**📄 100구간 상세 결과** (전략이 B&H보다 나은 구간: "
                            f"{int((det['전략-B&H(%p)'] > 0).sum())}/{len(det)})")
                st.dataframe(det.style.apply(_hl, axis=1),
                             use_container_width=True, height=min(38 + 35 * len(det), 560))
                st.download_button("📥 100구간 결과 CSV", det.to_csv().encode("utf-8-sig"),
                                   "dual_sniper_robustness.csv", "text/csv", key="ds_mc_dl")





# ══════════════════════════════════════════════
# 탭5: DB 조회 (기본)
# ══════════════════════════════════════════════

def render_db_tab(params=None):
    st.subheader("📂 DB 조회")
    st.info("v1: 거래 히스토리 DB 연동은 추후 추가 예정입니다. "
            "현재는 백테스트 탭에서 CSV 다운로드를 이용하세요.")


# ══════════════════════════════════════════════
# 탭6: 개인 설정 (텔레그램 / 구글시트 — DSS 패턴)
# ══════════════════════════════════════════════

def _send_telegram(token, chat_id, text):
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=10)
        return resp.json()
    except Exception as e:
        return {"ok": False, "description": str(e)}


def _build_ds_order_text(r, acct_name=""):
    """주문표 result → 텔레그램 메시지 텍스트."""
    od = pd.Timestamp(r["order_date"]).strftime("%Y-%m-%d")
    lines = [f"<b>🎯 Dual Sniper — {acct_name}</b>",
             f"주문일 {od} · 모드 <b>{r['next_mode']}</b> · 보유 {r['n_pos']}/{r['divisions']}슬롯",
             f"기준종가 ${r['last_close']:.2f} · 총자산 ${r['total_asset']:,.0f}", ""]
    if r["orders"]:
        for o in r["orders"]:
            price = "시장가(종가)" if o["가격"] is None else f"${o['가격']:,.2f}"
            tag = "🔴매도" if o["거래방법"] == "MOC" else ("🔵매도" if o["구분"] == "매도" else "🟠매수")
            lines.append(f"{tag} {o['거래방법']} {price} × {o['수량']:,}주 ({o['사유']})")
    else:
        lines.append("주문 없음")
    return "\n".join(lines)


def render_settings_tab():
    st.subheader("⚙️ 개인 설정")
    cfg = _load_ds_config()

    if _IS_CLOUD:
        if st.session_state.get("logged_in"):
            st.success("☁️ **Streamlit Cloud 실행 중** — 설정이 Google Sheets에 영구 저장됩니다.")
        else:
            st.warning("☁️ **Streamlit Cloud 실행 중** — 로그인 후 설정이 영구 저장됩니다.")
    else:
        st.success(f"🖥️ **로컬 PC 실행 중** — 설정이 `{_DS_CONFIG_PATH}` 에 저장됩니다.")

    # ── 텔레그램 알림 설정 ──
    with st.container(border=True):
        ct, ch = st.columns([3, 1])
        with ct:
            st.markdown("#### 💬 텔레그램 알림 설정")
            st.caption("듀얼스나이퍼 주문표를 텔레그램으로 받을 수 있습니다.")
        with ch:
            with st.popover("❓ Chat ID & Bot Token 확인 방법", use_container_width=True):
                st.markdown("""
<style>
.tg-help-section { margin-bottom: 20px; }
.tg-help-title {
    display: flex; align-items: center; gap: 10px;
    font-size: 17px; font-weight: 700; color: #1a1a2e; margin-bottom: 10px;
}
.tg-help-badge {
    background: #4A90D9; color: white;
    border-radius: 50%; width: 28px; height: 28px;
    display: inline-flex; align-items: center; justify-content: center;
    font-size: 14px; font-weight: 700; flex-shrink: 0;
}
.tg-help-box {
    background: #EEF4FB; border-radius: 10px;
    padding: 14px 18px; font-size: 14px; line-height: 2;
}
.tg-help-box ol { margin: 0; padding-left: 20px; }
.tg-tag {
    background: #D0E8FF; color: #1a5fa8;
    border-radius: 5px; padding: 1px 7px;
    font-family: monospace; font-size: 13px;
}
.tg-code-box {
    background: #1e2533; color: #7dd3fc;
    border-radius: 8px; padding: 10px 14px; margin-top: 8px;
    font-family: monospace; font-size: 12px; word-break: break-all;
    line-height: 1.7;
}
.tg-example-box {
    background: white; border: 1px solid #CBD5E1; border-radius: 8px;
    padding: 12px 16px; margin-top: 10px; font-size: 13px; color: #555;
}
.tg-example-val { color: #4A90D9; font-family: monospace; font-size: 13px; }
.tg-warn-box {
    background: #FFFBEB; border: 1px solid #F59E0B;
    border-radius: 10px; padding: 14px 18px; font-size: 14px; line-height: 2;
}
.tg-warn-title { font-weight: 700; color: #92400E; margin-bottom: 4px; }
.tg-sub-title { font-weight: 700; color: #1a5fa8; margin: 10px 0 4px 0; }
.tg-tip-box {
    background: #F0FDF4; border: 1px solid #86EFAC;
    border-radius: 8px; padding: 10px 14px; margin-top: 8px;
    font-size: 13px; color: #166534;
}
</style>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">1</span> Bot Token 생성하기</div>
  <div class="tg-help-box">
    <ol>
      <li>텔레그램 앱에서 <span class="tg-tag">@BotFather</span> 를 검색합니다.</li>
      <li><span class="tg-tag">/start</span> → <span class="tg-tag">/newbot</span> 입력</li>
      <li><strong>봇 표시 이름</strong> 입력 (예: <span class="tg-tag">듀얼스나이퍼 알림봇</span>)</li>
      <li><strong>봇 username</strong> 입력 — 영문+숫자, <span class="tg-tag">bot</span> 으로 끝나야 함 (예: <span class="tg-tag">dual_sniper_bot</span>)</li>
      <li>성공 시 <strong>HTTP API Token</strong> 발급 → 이것이 <strong>Bot Token</strong></li>
    </ol>
    <div class="tg-example-box">
      <div style="color:#888; font-size:12px; margin-bottom:4px;">Bot Token 예시:</div>
      <div class="tg-example-val">1234567890:ABCdefGHIjklMNOpqrSTUvwxYZ</div>
    </div>
  </div>
</div>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">2</span> 내 봇 시작하기 (필수!)</div>
  <div class="tg-warn-box">
    <div class="tg-warn-title">⚠ 봇을 먼저 시작해야 Chat ID를 확인하고 메시지를 받을 수 있습니다!</div>
    <ol>
      <li>텔레그램에서 내 봇 username 검색 (예: <span class="tg-tag">@dual_sniper_bot</span>)</li>
      <li><span class="tg-tag">/start</span> 클릭 → 아무 메시지 보내기</li>
    </ol>
  </div>
</div>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">3</span> Chat ID 확인하기</div>
  <div class="tg-help-box">
    <div class="tg-sub-title">✅ 방법 1: getUpdates API</div>
    <ol>
      <li>봇에게 메시지 보낸 후 브라우저에 입력:</li>
    </ol>
    <div class="tg-code-box">https://api.telegram.org/bot<span style="color:#fde047;">{토큰값}</span>/getUpdates</div>
    <ol start="2">
      <li>JSON 응답에서 <span class="tg-tag">"id"</span> 값 = <strong>Chat ID</strong></li>
    </ol>
    <div class="tg-sub-title">방법 2: @userinfobot 사용</div>
    <ol><li><span class="tg-tag">@userinfobot</span> 검색 → <span class="tg-tag">/start</span> → Chat ID 확인</li></ol>
  </div>
</div>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">4</span> 연결 테스트</div>
  <div class="tg-tip-box">
    💡 Bot Token과 Chat ID를 입력한 후 <strong>📨 주문표 테스트 발송</strong> 버튼을 눌러보세요.<br>
    메시지가 정상적으로 수신되면 설정 완료입니다! ✅
  </div>
</div>
""", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        tg_chat = c1.text_input("텔레그램 Chat ID", value=cfg.get("tg_chat_id", ""),
                                placeholder="예: 1234567890", key="ds_tg_chat")
        tg_tok = c2.text_input("Bot Token", value=cfg.get("tg_token", ""),
                               placeholder="예: 123456789:AAF...", type="password", key="ds_tg_tok")
        b1, b2, _ = st.columns([1, 1, 4])
        if b1.button("📨 주문표 테스트 발송", use_container_width=True, key="ds_tg_test"):
            if not tg_chat or not tg_tok:
                st.warning("Chat ID와 Bot Token을 먼저 입력하세요.")
            else:
                accts = list(cfg.get("accounts", {}).keys())
                sent = [(a, st.session_state.get(f"ds_osa{i}"))
                        for i, a in enumerate(accts) if st.session_state.get(f"ds_osa{i}")]
                if not sent:
                    st.warning("⚠️ 주문표 탭에서 먼저 '주문표 로드'를 실행하세요.")
                else:
                    ok = 0
                    for a, r in sent:
                        if _send_telegram(tg_tok, tg_chat, _build_ds_order_text(r, a)).get("ok"):
                            ok += 1
                    st.success(f"✅ {ok}/{len(sent)}개 계좌 발송 완료")
        if b2.button("💾 저장하기", use_container_width=True, key="ds_tg_save", type="primary"):
            cfg["tg_chat_id"] = tg_chat
            cfg["tg_token"] = tg_tok
            _save_ds_config(cfg)
            st.success("✅ 텔레그램 설정 저장 완료")

    st.write("")

    # ── 구글 스프레드시트 연동 ──
    with st.container(border=True):
        cg, cgh = st.columns([3, 1])
        with cg:
            st.markdown("#### 🗂️ 구글 스프레드시트 연동")
            st.caption("주문 기록을 구글 스프레드시트로 동기화합니다. (계좌별 `ds_{계좌}_매매기록` 시트 자동 생성)")
        with cgh:
            with st.popover("❓ 구글 스프레드시트 설정 방법", use_container_width=True):
                st.markdown("""
<style>
.gs-help-section { margin-bottom: 20px; }
.gs-help-title {
    display: flex; align-items: center; gap: 10px;
    font-size: 17px; font-weight: 700; color: #1a1a2e; margin-bottom: 10px;
}
.gs-help-badge {
    background: #2EAA5E; color: white;
    border-radius: 50%; width: 28px; height: 28px;
    display: inline-flex; align-items: center; justify-content: center;
    font-size: 14px; font-weight: 700; flex-shrink: 0;
}
.gs-help-box {
    background: #EDF7F0; border-radius: 10px;
    padding: 14px 18px; font-size: 14px; line-height: 2;
}
.gs-help-box ol { margin: 0; padding-left: 20px; }
.gs-tag {
    background: #D4EFE0; color: #1a6e3c;
    border-radius: 5px; padding: 1px 7px;
    font-family: monospace; font-size: 13px;
}
.gs-example-box {
    background: white; border: 1px solid #CBD5E1; border-radius: 8px;
    padding: 12px 16px; margin-top: 10px; font-size: 13px; color: #555;
}
.gs-example-val { color: #2EAA5E; font-family: monospace; font-size: 13px; }
.gs-warn-box {
    background: #FFFBEB; border: 1px solid #F59E0B;
    border-radius: 10px; padding: 14px 18px; font-size: 14px; line-height: 2;
}
.gs-warn-title { font-weight: 700; color: #92400E; margin-bottom: 6px; }
.gs-email-box {
    background: white; border: 1px solid #CBD5E1; border-radius: 8px;
    padding: 10px 14px; margin: 8px 0 12px 0; font-size: 13px; color: #555;
}
.gs-email-val { color: #2EAA5E; font-family: monospace; font-size: 13px; font-weight: 700; }
</style>

<div class="gs-help-section">
  <div class="gs-help-title"><span class="gs-help-badge">1</span> 새 스프레드시트 만들기</div>
  <div class="gs-help-box">
    <ol>
      <li><a href="https://sheets.google.com" target="_blank">Google Sheets</a>에서 새 시트 생성</li>
      <li>시트 이름 지정 (예: 듀얼스나이퍼 포트폴리오)</li>
    </ol>
  </div>
</div>

<div class="gs-help-section">
  <div class="gs-help-title"><span class="gs-help-badge">2</span> URL 복사</div>
  <div class="gs-help-box">
    <div>브라우저 주소창의 URL을 복사합니다.</div>
    <div class="gs-example-box">
      <div class="gs-example-val">https://docs.google.com/spreadsheets/d/1ABC...XYZ/edit</div>
    </div>
  </div>
</div>

<div class="gs-help-section">
  <div class="gs-help-title"><span class="gs-help-badge">3</span> 서비스 계정 권한 부여 (중요!)</div>
  <div class="gs-warn-box">
    <div class="gs-warn-title">⚠ 아래 이메일에 편집 권한을 부여해야 합니다.</div>
    <div class="gs-email-box">
      <div style="color:#888; font-size:12px; margin-bottom:4px;">서비스 계정 이메일:</div>
      <div class="gs-email-val">connectspreadsheet@sodium-gateway-485307-f3.iam.gserviceaccount.com</div>
    </div>
    <ol>
      <li>스프레드시트 우측 상단 <span class="gs-tag">공유</span> 클릭</li>
      <li>위 이메일을 <span class="gs-tag">편집자</span> 로 추가</li>
    </ol>
  </div>
</div>
""", unsafe_allow_html=True)
        gs_url = st.text_input("스프레드시트 URL", value=cfg.get("gs_url", ""),
                               placeholder="https://docs.google.com/spreadsheets/d/...", key="ds_gs_url")
        st.caption("* 스프레드시트에 서비스 계정 이메일을 편집자로 공유해주세요. (우측 상단 도움말 참고)")
        g1, g2, _ = st.columns([1, 1, 4])
        if g1.button("🔗 시트 연결 테스트", use_container_width=True, key="ds_gs_test"):
            if not gs_url:
                st.warning("URL을 먼저 입력하세요.")
            else:
                try:
                    from common.config import _get_gspread_client
                    sh = _get_gspread_client().open_by_url(gs_url)
                    st.success(f"✅ 연결 성공: **{sh.title}**")
                except Exception as e:
                    st.error(f"❌ 연결 실패: {e}")
        if g2.button("💾 저장하기 ", use_container_width=True, key="ds_gs_save", type="primary"):
            if not gs_url:
                st.warning("URL을 입력하세요.")
            else:
                cfg["gs_url"] = gs_url
                _save_ds_config(cfg)
                st.success("✅ 구글 스프레드시트 설정 저장 완료")

    st.write("")

    # ── 기본값 / 캐시 ──
    with st.container(border=True):
        st.markdown("#### 🧰 파라미터 기본값 & 캐시")
        bc1, bc2 = st.columns(2)
        if bc1.button("💾 현재 사이드바 값을 기본값으로 저장", key="ds_save_cfg", use_container_width=True):
            cfg["mode_rule"] = {"ma_weeks": st.session_state.get("ds_ma", 36),
                                "peak_thr": st.session_state.get("ds_peak", 66.0),
                                "dn": st.session_state.get("ds_dn", 42.0)}
            cfg["strategy"] = {
                "ag_div": st.session_state.get("ds_agdiv", 6), "ag_buy": st.session_state.get("ds_agbuy", 8.0),
                "ag_sell_alpha": st.session_state.get("ds_agsa", 0.4), "ag_hold_alpha": st.session_state.get("ds_agha", 2.0),
                "sf_div": st.session_state.get("ds_sfdiv", 5), "sf_hold": st.session_state.get("ds_sfhold", 8),
                "sf_buy1": st.session_state.get("ds_sfb1", -0.6), "sf_buy2": st.session_state.get("ds_sfb2", 5.5),
                "sf_sell": st.session_state.get("ds_sfsell", 0.7), "sf_ma_base": st.session_state.get("ds_sfma", 3),
                "sf_weights": st.session_state.get("ds_sfw", "6, 13, 20, 27, 34")}
            cfg["initial_capital"] = st.session_state.get("ds_cap", 10000)
            cfg["fee_rate"] = st.session_state.get("ds_fee", 0.0)
            _save_ds_config(cfg)
            st.success("✅ 기본값 저장 완료")
        if bc2.button("🔄 가격/모드 캐시 초기화", key="ds_clear_cache", use_container_width=True):
            clear_ds_data_cache()
            try:
                get_auto_mode_map.clear()
            except Exception:
                pass
            st.success("✅ 캐시 초기화 완료")
