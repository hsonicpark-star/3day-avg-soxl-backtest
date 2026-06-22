"""
카마릴라 피봇 돌파 매매법 — 02 통합 모듈
- 진입: 카마릴라 4차 저항선 돌파 (계수 0.70 권장)
- 청산: 익일 시가 MOO (검증상 최강·갭에 robust)
- 위험관리: 변동성 타게팅 (급락기 자동 디레버리징)
- 운용: DSS 유휴 예수금 애드온 오버레이 + 워터폴 자금관리
주문표는 DSS 계좌(common dss_config)를 선택해 예수금/투자금/분할수/보유를 읽어온다.
"""
import os
import json
import itertools
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from common.config import _IS_CLOUD
from camarilla_engine import (
    load_price_data, CamarillaParams, run_backtest, run_backtest_fast,
    compute_metrics, yearly_returns, build_signal_mult,
    CAMARILLA_LEVELS, FIB_LEVELS,
)

_CAM_CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".camarilla")
_CAM_CONFIG_PATH = os.path.join(_CAM_CONFIG_DIR, "config.json")

_CAM_DEFAULTS = {
    "ov_ticker": "SOXL",
    "ov_coef": 0.70,
    "ov_vol_period": 20,
    "ov_vol_target": 0.70,
    "ov_reserve_tiers": 1,
    "ov_inject_frac": 0.70,
    "ov_slippage": 0.001,
}


# ──────────────────────────────────────────────
# 설정 (Cloud: user_settings["cam_config"] / 로컬: ~/.camarilla/config.json)
# ──────────────────────────────────────────────

def _load_cam_config() -> dict:
    cfg = {}
    if _IS_CLOUD and st.session_state.get("logged_in"):
        raw = st.session_state.get("user_settings", {}).get("cam_config", "")
        if raw:
            try:
                cfg = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                cfg = {}
    elif os.path.exists(_CAM_CONFIG_PATH):
        try:
            with open(_CAM_CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}
    if not isinstance(cfg, dict):
        cfg = {}
    for k, v in _CAM_DEFAULTS.items():
        cfg.setdefault(k, v)
    return cfg


def _save_cam_config(cfg: dict):
    try:
        os.makedirs(_CAM_CONFIG_DIR, exist_ok=True)
        with open(_CAM_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            cfg_json = json.dumps(cfg, ensure_ascii=False)
            st.session_state.setdefault("user_settings", {})
            st.session_state.user_settings["cam_config"] = cfg_json
            from common.auth import _save_user_settings_to_sheet
            _save_user_settings_to_sheet(st.session_state.username, {"cam_config": cfg_json})
        except Exception as e:
            st.warning(f"⚠️ Cloud 저장 실패 (로컬엔 저장됨): {e}")


# ──────────────────────────────────────────────
# 데이터 / 유틸
# ──────────────────────────────────────────────

@st.cache_data(show_spinner="가격 데이터 로딩...")
def _get_price(ticker: str):
    df = load_price_data(ticker, "2009-06-01", "2026-12-31")
    # SOXL: yfinance 확정 종가 누락 시 백업 구글시트(DB)로 보충 — DSS/듀얼/표준편차와 동일 백업
    if str(ticker).upper() == "SOXL" and df is not None and len(df):
        try:
            from common.data import _maybe_patch_soxl_backup
            df = _maybe_patch_soxl_backup(df, ticker)
            # 백업으로 추가된 행은 Close만 존재(O/H/L=NaN) → Close로 채워 평평한 봉 처리
            for _c in ("Open", "High", "Low"):
                if _c in df.columns:
                    df[_c] = df[_c].fillna(df["Close"])
        except Exception:
            pass
    return df


def _vol_mult(price, period: int, target: float) -> float:
    ret = price['Close'].pct_change().dropna()
    if len(ret) < period:
        return 1.0
    rv = float(ret.iloc[-period:].std() * np.sqrt(252))
    return float(min(1.0, target / rv)) if rv > 0 else 1.0


def _send_telegram(token: str, chat_id: str, text: str) -> dict:
    try:
        import requests
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                          timeout=10)
        return r.json()
    except Exception as e:
        return {"ok": False, "description": str(e)}


def _nth_weekday(y, m, wd, n):
    first = datetime(y, m, 1).date()
    off = (wd - first.weekday()) % 7
    return first + timedelta(days=off + 7 * (n - 1))


def _last_weekday(y, m, wd):
    nxt = datetime(y + 1, 1, 1).date() if m == 12 else datetime(y, m + 1, 1).date()
    last = nxt - timedelta(days=1)
    while last.weekday() != wd:
        last -= timedelta(days=1)
    return last


def _us_holidays(y):
    h = {datetime(y, 1, 1).date(), _nth_weekday(y, 1, 0, 3), _nth_weekday(y, 2, 0, 3),
         _last_weekday(y, 5, 0), datetime(y, 6, 19).date(), datetime(y, 7, 4).date(),
         _nth_weekday(y, 9, 0, 1), _nth_weekday(y, 11, 3, 4), datetime(y, 12, 25).date()}
    return h


def _next_trading_date(d=None):
    d = d or datetime.now().date()
    nxt = d + timedelta(days=1)
    while nxt.weekday() >= 5 or nxt in _us_holidays(nxt.year):
        nxt += timedelta(days=1)
    return nxt


@st.cache_data(show_spinner="DSS 계좌 상태 계산 중...")
def _dss_account_state(acct_name, acct_json, today_str):
    """선택한 DSS 계좌의 현재 예수금/투자금/분할수/보유를 DSS 엔진으로 산출."""
    from strategies import dss
    acct = json.loads(acct_json)
    ap = acct.get("params", {})
    soxl = dss.get_soxl_data()
    ms = dss.get_mode_series(len(dss.get_qqq_data()))
    p = dss.DSSParams(
        sf_divisions=int(ap.get("sf_div", 7)), sf_max_hold=int(ap.get("sf_hold", 30)),
        sf_buy_pct=float(ap.get("sf_buy", 3.0)) / 100, sf_sell_pct=float(ap.get("sf_sell", 0.2)) / 100,
        ag_divisions=int(ap.get("ag_div", 7)), ag_max_hold=int(ap.get("ag_hold", 7)),
        ag_buy_pct=float(ap.get("ag_buy", 5.0)) / 100, ag_sell_pct=float(ap.get("ag_sell", 2.5)) / 100,
        initial_capital=float(acct.get("os_capital", 10000.0)),
        fee_rate=float(ap.get("fee_rate", 0.04)) / 100,
        renewal_period=int(ap.get("renewal_period", 10)),
        pcr=float(ap.get("pcr", 80)) / 100, lcr=float(ap.get("lcr", 30)) / 100,
    )
    bt = dss.run_backtest(p, soxl, ms, str(acct.get("os_start", "2024-01-01")),
                          today_str, capital_adj_history=acct.get("capital_adj_history", []))
    last = bt.iloc[-1]
    return {
        "cash": float(last["예수금"]), "capital": float(last["투자금"]),
        "divisions": int(last["분할수"]), "tier": float(last["1회시드"]),
        "n_pos": int(last["보유포지션수"]), "total": float(last["총자산"]),
        "date": str(last["날짜"])[:10],
    }


# ──────────────────────────────────────────────
# 사이드바
# ──────────────────────────────────────────────

def render_sidebar():
    cfg = _load_cam_config()
    st.sidebar.markdown("### 🎯 카마릴라 설정")
    ticker = st.sidebar.text_input("종목", value=cfg.get("ov_ticker", "SOXL"),
                                   key="cam_ticker").strip().upper()

    sel = st.sidebar.selectbox("저항선 레벨",
                               ["R4 (계수 0.55)", "추천 (계수 0.70)", "사용자 지정"],
                               index=1, key="cam_level")
    if sel == "R4 (계수 0.55)":
        coef = 0.55
    elif sel == "추천 (계수 0.70)":
        coef = 0.70
    else:
        coef = st.sidebar.number_input("계수", 0.1, 2.0, value=float(cfg.get("ov_coef", 0.70)),
                                       step=0.05, format="%.2f", key="cam_coef")
    st.sidebar.caption(f"저항선 = 전일종가 + (고가−저가)×{coef:.2f}")

    st.sidebar.markdown("**변동성 타게팅** (위험 손잡이)")
    sig_on = st.sidebar.checkbox("변동성 타게팅 사용", value=True, key="cam_sig_on")
    vol_target = st.sidebar.slider("변동성 목표", 0.2, 1.2, float(cfg.get("ov_vol_target", 0.70)),
                                   0.05, key="cam_vt") if sig_on else 0.7
    vol_period = int(cfg.get("ov_vol_period", 20))

    st.sidebar.markdown("### ⚙️ 백테스트 설정")
    bc1, bc2 = st.sidebar.columns(2)
    initial_capital = bc1.number_input("초기투자금", 1000, 1_000_000_000, 100000, 1000, key="cam_cap")
    pos_pct = bc2.slider("투입비중(%)", 10, 100, 100, 5, key="cam_pos") / 100.0
    slippage = st.sidebar.number_input(
        "슬리피지 (왕복, %)", 0.0, 2.0, float(cfg.get("ov_slippage_pct", 0.1)), 0.05,
        format="%.2f", key="cam_slip",
        help="돌파매매 체결 슬리피지(왕복 기준). 기본 0.1% 권장 — 1일 보유라 거래가 잦아 성과에 영향이 큽니다.")
    dc1, dc2 = st.sidebar.columns(2)
    start_date = dc1.date_input("시작일", value=pd.Timestamp("2010-03-12"), key="cam_start")
    end_date = dc2.date_input("종료일", value=datetime.today().date(), key="cam_end")

    return {
        "ticker": ticker, "coef": coef, "signal": "vol" if sig_on else "none",
        "vol_target": vol_target, "vol_period": vol_period,
        "initial_capital": float(initial_capital), "position_pct": pos_pct,
        "slippage": float(slippage),
        "start_date": start_date, "end_date": end_date,
        "bt_ticker": ticker, "bt_start_date": start_date, "bt_end_date": end_date,
        "bt_initial_capital": float(initial_capital),
    }


def _make_params(p, **over):
    kw = dict(coef=p["coef"], initial_capital=p["initial_capital"],
              position_pct=p.get("position_pct", 1.0), hold_days=1,
              signal=p.get("signal", "none"), vol_period=p.get("vol_period", 20),
              vol_target=p.get("vol_target", 0.70))
    kw.update(over)
    return CamarillaParams(**kw)


# ══════════════════════════════════════════════
# 탭 1: 백테스트
# ══════════════════════════════════════════════

@st.cache_data(show_spinner="백테스트 실행 중...")
def _run_bt(ticker, coef, cap, pos, signal, vt, vp, sd, ed, slip=0.1):
    # slip = 왕복 슬리피지(%) → fee_rate(편도) = slip/100/2 = slip/200
    p = CamarillaParams(coef=coef, initial_capital=cap, position_pct=pos, hold_days=1,
                        signal=signal, vol_target=vt, vol_period=vp, fee_rate=slip / 200.0)
    return run_backtest(p, _get_price(ticker), str(sd), str(ed))


def render_backtest_tab(params):
    p = params
    _slip = float(p.get("slippage", 0.1))
    st.subheader(f"📊 {p['ticker']} · 카마릴라 피봇 돌파 백테스트")
    bt = _run_bt(p["ticker"], p["coef"], p["initial_capital"], p["position_pct"],
                 p["signal"], p["vol_target"], p["vol_period"], p["start_date"], p["end_date"], _slip)
    if not len(bt):
        st.warning("데이터가 부족합니다.")
        return
    m = compute_metrics(bt, p["initial_capital"])
    _sig = f" · 변동성타겟 {p['vol_target']}" if p["signal"] == "vol" else ""
    st.caption(f"계수 {p['coef']:.2f}{_sig} · 익일시가 MOO · **슬리피지 {_slip:.2f}% 적용** · "
               f"{bt['날짜'].iloc[0].date()} ~ {bt['날짜'].iloc[-1].date()} ({len(bt):,} 거래일)")

    c = st.columns(6)
    c[0].metric("최종 자산", f"${m['final_asset']:,.0f}")
    c[1].metric("총 수익률", f"{m['total_return']*100:,.0f}%")
    c[2].metric("CAGR", f"{m['cagr']*100:.1f}%")
    c[3].metric("MDD", f"{m['mdd']*100:.1f}%")
    c[4].metric("CALMAR", f"{m['calmar']:.2f}")
    c[5].metric("승률", f"{m['win_rate']*100:.1f}%")
    c2 = st.columns(4)
    c2[0].metric("Sharpe", f"{m['sharpe']:.2f}")
    c2[1].metric("Sortino", f"{m['sortino']:.2f}")
    c2[2].metric("매도 건수", f"{m['sell_count']:,}")
    c2[3].metric("승/패", f"{m['win_count']:,}/{m['sell_count']-m['win_count']:,}")

    # 자산추이 & 낙폭
    st.subheader("📈 총자산 추이 & 낙폭")
    asset = bt['총자산'].values
    dd = (asset - np.maximum.accumulate(asset)) / np.maximum.accumulate(asset) * 100
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3],
                        vertical_spacing=0.05, subplot_titles=("총자산 (로그)", "낙폭 (%)"))
    fig.add_trace(go.Scatter(x=bt['날짜'], y=asset, line=dict(color="#2563eb")), row=1, col=1)
    fig.add_trace(go.Scatter(x=bt['날짜'], y=dd, fill="tozeroy", line=dict(color="#dc2626")), row=2, col=1)
    fig.update_yaxes(type="log", row=1, col=1)
    fig.update_layout(height=520, showlegend=False, margin=dict(t=40, b=20))
    st.plotly_chart(fig, use_container_width=True, key="cam_bt_eq")

    # 연도별
    st.subheader("📅 연도별 수익률")
    yr = yearly_returns(bt, p["initial_capital"])
    if len(yr):
        colors = ["#16a34a" if v >= 0 else "#dc2626" for v in yr['수익률']]
        figy = go.Figure(go.Bar(x=yr['연도'].astype(str), y=yr['수익률']*100, marker_color=colors,
                                text=[f"{v*100:+.0f}%" for v in yr['수익률']], textposition="outside"))
        figy.update_layout(height=320, yaxis_title="%", margin=dict(t=20, b=20))
        st.plotly_chart(figy, use_container_width=True, key="cam_bt_yr")

    # B&H 비교
    st.subheader("📉 카마릴라 vs Buy & Hold")
    pw = _get_price(p["ticker"])
    pw = pw[(pw.index >= bt['날짜'].iloc[0]) & (pw.index <= bt['날짜'].iloc[-1])]
    bh = p["initial_capital"] * (pw['Close'] / pw['Close'].iloc[0])
    figc = go.Figure()
    figc.add_trace(go.Scatter(x=bt['날짜'], y=asset, name="카마릴라", line=dict(color="#2563eb")))
    figc.add_trace(go.Scatter(x=pw.index, y=bh.values, name="Buy & Hold",
                              line=dict(color="#9ca3af", dash="dot")))
    figc.update_yaxes(type="log")
    figc.update_layout(height=360, margin=dict(t=20, b=20), legend=dict(orientation="h", y=1.1))
    st.plotly_chart(figc, use_container_width=True, key="cam_bt_bh")

    # 최근 매매
    st.subheader("📑 최근 매매 내역 (30건)")
    tr = bt[(bt['매수체결'].notna()) | (bt['매도수량'] > 0)].tail(30)[
        ['날짜', '시가', '저항선', '고가', '종가', '매수체결', '매수수량', '매도가', '매도수량', '당일실현', '총자산']].copy()
    tr['날짜'] = tr['날짜'].dt.strftime('%Y-%m-%d')
    for col in ['시가', '저항선', '고가', '종가', '매수체결', '매도가']:
        tr[col] = tr[col].map(lambda v: f"${v:,.2f}" if pd.notna(v) else "")
    for col in ['매수수량', '매도수량']:
        tr[col] = tr[col].map(lambda v: f"{int(v):,}" if v else "")
    tr['당일실현'] = tr['당일실현'].map(lambda v: f"${v:,.0f}" if v else "")
    tr['총자산'] = tr['총자산'].map(lambda v: f"${v:,.0f}")
    st.dataframe(tr, use_container_width=True, hide_index=True)
    st.download_button("전체 CSV 다운로드", bt.to_csv(index=False).encode('utf-8-sig'),
                       file_name=f"camarilla_{p['ticker']}.csv", key="cam_bt_dl")


# ══════════════════════════════════════════════
# 탭 2: 최적화
# ══════════════════════════════════════════════

_OPT_SORT = {"CALMAR": ("CALMAR", False), "CAGR": ("CAGR", False),
             "최종자산": ("최종자산", False), "MDD 최소화": ("MDD", False), "승률": ("승률", False)}


def _eval(pxd, cf, pos, vt, sd, ed, cap, slip=0.1):
    sig = "vol" if vt > 0 else "none"
    p = CamarillaParams(coef=float(cf), initial_capital=cap, position_pct=pos/100.0,
                        hold_days=1, signal=sig, vol_period=20, vol_target=(vt if vt > 0 else 0.7),
                        fee_rate=slip / 200.0)
    r = run_backtest_fast(p, pxd, str(sd), str(ed))
    if not r or r['total_days'] < 20:
        return None
    yrs = r['total_days'] / 252
    cagr = (r['final_asset']/cap)**(1/yrs) - 1 if r['final_asset'] > 0 else -1
    calmar = cagr/abs(r['mdd']) if r['mdd'] < 0 else 0
    return {"계수": round(float(cf), 3), "투입%": pos, "변동성타겟": vt,
            "CAGR": cagr, "MDD": r['mdd'], "CALMAR": calmar,
            "최종자산": r['final_asset'], "승률": r['win_rate'], "매도건수": r['sell_count']}


def render_optimization_tab(params):
    p = params
    st.subheader("🔍 파라미터 최적화")
    method = st.radio("최적화 방식", ["📊 그리드 탐색", "🎲 랜덤 탐색", "📈 워크포워드", "🧠 베이지안"],
                      horizontal=True, key="cam_opt_method")
    _desc = {"📊 그리드 탐색": "모든 조합 완전 탐색. 조합이 적을 때 가장 정확.",
             "🎲 랜덤 탐색": "무작위 N개 샘플링. 공간이 클 때 빠름.",
             "📈 워크포워드": "IS(최적화)·OOS(검증) 분할 반복. 과적합 방지.",
             "🧠 베이지안": "Optuna TPE 스마트 탐색."}
    st.caption(_desc[method])

    with st.expander("파라미터 범위", expanded=True):
        r1, r2, r3 = st.columns(3)
        cmin = r1.number_input("계수 최소", 0.1, 2.0, 0.40, 0.05, key="cam_o_cmin")
        cmax = r2.number_input("계수 최대", 0.1, 2.0, 0.90, 0.05, key="cam_o_cmax")
        cstep = r3.number_input("계수 간격", 0.01, 0.5, 0.05, 0.01, key="cam_o_cstep")
        m1, m2 = st.columns(2)
        opt_pos = m1.multiselect("투입비중(%)", [100, 75, 50], default=[100], key="cam_o_pos")
        opt_vt = m2.multiselect("변동성타겟 (0=미사용)", [0, 0.4, 0.5, 0.7, 1.0],
                                default=[0, 0.7], key="cam_o_vt")
        metric = st.selectbox("최적화 기준", list(_OPT_SORT.keys()), key="cam_o_metric")
    coefs = list(np.round(np.arange(cmin, cmax + 1e-9, cstep), 3))
    opt_pos = opt_pos or [100]
    opt_vt = opt_vt or [0]
    sort_col, asc = _OPT_SORT[metric]
    n_total = len(coefs) * len(opt_pos) * len(opt_vt)
    cap = p["initial_capital"]
    _oslip = float(p.get("slippage", 0.1))

    def _show(res, sfx):
        d = res.copy()
        d['CAGR'] = d['CAGR'].map(lambda v: f"{v*100:.1f}%")
        d['MDD'] = d['MDD'].map(lambda v: f"{v*100:.1f}%")
        d['CALMAR'] = d['CALMAR'].map(lambda v: f"{v:.2f}")
        d['최종자산'] = d['최종자산'].map(lambda v: f"${v:,.0f}")
        d['승률'] = d['승률'].map(lambda v: f"{v*100:.1f}%")
        st.dataframe(d.head(30), use_container_width=True, hide_index=True)
        figs = go.Figure(go.Scatter(
            x=res['MDD']*100, y=res['CAGR']*100, mode="markers",
            marker=dict(size=9, color=res['CALMAR'], colorscale="Viridis", showscale=True,
                        colorbar=dict(title="CALMAR")),
            text=[f"계수{c:.2f}·{pp}%·vt{v}" for c, pp, v in zip(res['계수'], res['투입%'], res['변동성타겟'])]))
        figs.update_layout(height=400, xaxis_title="MDD (%)", yaxis_title="CAGR (%)", margin=dict(t=20, b=20))
        st.plotly_chart(figs, use_container_width=True, key=f"cam_opt_sc_{sfx}")
        st.download_button("CSV", res.to_csv(index=False).encode('utf-8-sig'),
                           file_name="camarilla_opt.csv", key=f"cam_opt_dl_{sfx}")

    if method == "📊 그리드 탐색":
        st.info(f"예상 조합: {n_total:,}개")
        if st.button("▶ 그리드 실행", type="primary", key="cam_run_grid"):
            pxd = _get_price(p["ticker"])
            combos = list(itertools.product(coefs, opt_pos, opt_vt))
            prog = st.progress(0.0)
            rows = []
            for i, (cf, pp, vt) in enumerate(combos):
                r = _eval(pxd, cf, pp, vt, p["start_date"], p["end_date"], cap, _oslip)
                if r:
                    rows.append(r)
                if i % max(1, len(combos)//50) == 0:
                    prog.progress((i+1)/len(combos))
            prog.empty()
            if rows:
                st.session_state['cam_opt_res'] = pd.DataFrame(rows).sort_values(sort_col, ascending=asc).reset_index(drop=True)

    elif method == "🎲 랜덤 탐색":
        n_s = st.number_input("샘플 수", 20, 2000, 200, 20, key="cam_n_s")
        if st.button("▶ 랜덤 실행", type="primary", key="cam_run_rand"):
            import random
            random.seed(42)
            full = list(itertools.product(coefs, opt_pos, opt_vt))
            sample = random.sample(full, min(int(n_s), len(full)))
            pxd = _get_price(p["ticker"])
            prog = st.progress(0.0)
            rows = []
            for i, (cf, pp, vt) in enumerate(sample):
                r = _eval(pxd, cf, pp, vt, p["start_date"], p["end_date"], cap, _oslip)
                if r:
                    rows.append(r)
                if i % max(1, len(sample)//50) == 0:
                    prog.progress((i+1)/len(sample))
            prog.empty()
            if rows:
                st.session_state['cam_opt_res'] = pd.DataFrame(rows).sort_values(sort_col, ascending=asc).reset_index(drop=True)

    elif method == "📈 워크포워드":
        w1, w2 = st.columns(2)
        isy = w1.number_input("IS 기간(년)", 1, 10, 3, key="cam_wf_is")
        oosy = w2.number_input("OOS 기간(년)", 1, 5, 1, key="cam_wf_oos")
        if st.button("▶ 워크포워드 실행", type="primary", key="cam_run_wfo"):
            pxd = _get_price(p["ticker"])
            ts, te = pd.Timestamp(str(p["start_date"])), pd.Timestamp(str(p["end_date"]))
            wins, cur = [], ts
            while True:
                ie = cur + pd.Timedelta(days=int(isy*365.25))
                oe = ie + pd.Timedelta(days=int(oosy*365.25))
                if oe > te:
                    break
                wins.append((cur, ie, ie, oe)); cur = ie
            if not wins:
                st.error("기간이 짧습니다.")
            else:
                combos = list(itertools.product(coefs, opt_pos, opt_vt))
                prog = st.progress(0.0)
                rows = []
                for wi, (iss, ise, os_, oe) in enumerate(wins):
                    best, bs = None, -1e18
                    for cf, pp, vt in combos:
                        r = _eval(pxd, cf, pp, vt, iss.date(), ise.date(), cap, _oslip)
                        if r:
                            sc = (-abs(r["MDD"]) if sort_col == "MDD" else r[sort_col])
                            if sc > bs:
                                bs, best = sc, (cf, pp, vt)
                    prog.progress((wi+1)/len(wins))
                    if not best:
                        continue
                    oos = _eval(pxd, *best, os_.date(), oe.date(), cap, _oslip)
                    if oos:
                        rows.append({"윈도우": wi+1, "IS": f"{iss.date()}~{ise.date()}",
                                     "OOS": f"{os_.date()}~{oe.date()}",
                                     "최적(계수/투입/타겟)": f"{best[0]:.2f}/{best[1]}/{best[2]}",
                                     "OOS CAGR": oos["CAGR"], "OOS MDD": oos["MDD"], "OOS CALMAR": oos["CALMAR"]})
                prog.empty()
                if rows:
                    wdf = pd.DataFrame(rows)
                    a = st.columns(3)
                    a[0].metric("윈도우", f"{len(wdf)}개")
                    a[1].metric("OOS 평균 CAGR", f"{wdf['OOS CAGR'].mean()*100:.1f}%")
                    a[2].metric("OOS 평균 MDD", f"{wdf['OOS MDD'].mean()*100:.1f}%")
                    disp = wdf.copy()
                    for c in ['OOS CAGR', 'OOS MDD']:
                        disp[c] = disp[c].map(lambda v: f"{v*100:.1f}%")
                    disp['OOS CALMAR'] = disp['OOS CALMAR'].map(lambda v: f"{v:.2f}")
                    st.dataframe(disp, use_container_width=True, hide_index=True)

    else:  # 베이지안
        try:
            import optuna
            ok = True
        except ImportError:
            ok = False
        if not ok:
            st.error("`optuna` 미설치. `pip install optuna` 후 재시작.")
        else:
            n_tr = st.number_input("탐색 횟수", 30, 1000, 150, 10, key="cam_n_tr")
            if st.button("▶ 베이지안 실행", type="primary", key="cam_run_bayes"):
                pxd = _get_price(p["ticker"])
                optuna.logging.set_verbosity(optuna.logging.WARNING)
                prog = st.progress(0.0)
                rows, tc = [], [0]

                def _obj(trial):
                    cf = trial.suggest_float("계수", float(cmin), float(cmax))
                    pp = trial.suggest_categorical("투입%", opt_pos)
                    vt = trial.suggest_categorical("변동성타겟", opt_vt)
                    r = _eval(pxd, cf, pp, vt, p["start_date"], p["end_date"], cap, _oslip)
                    if not r:
                        return -1e9
                    rows.append(r); tc[0] += 1
                    if tc[0] % max(1, int(n_tr)//40) == 0:
                        prog.progress(min(tc[0]/int(n_tr), 1.0))
                    return (-abs(r["MDD"]) if sort_col == "MDD" else r[sort_col])
                study = optuna.create_study(direction="maximize",
                                            sampler=optuna.samplers.TPESampler(seed=42))
                study.optimize(_obj, n_trials=int(n_tr))
                prog.empty()
                if rows:
                    bp = study.best_params
                    st.success(f"최적: 계수={bp.get('계수', 0):.3f} · 투입={bp.get('투입%')}% · 타겟={bp.get('변동성타겟')}")
                    st.session_state['cam_opt_res'] = pd.DataFrame(rows).sort_values(sort_col, ascending=asc).reset_index(drop=True)

    if 'cam_opt_res' in st.session_state and method != "📈 워크포워드":
        st.markdown(f"**상위 결과 ({len(st.session_state['cam_opt_res'])}개)**")
        _show(st.session_state['cam_opt_res'], method[:2])


# ══════════════════════════════════════════════
# 탭 3: 오늘의 주문표 (DSS 계좌 연동)
# ══════════════════════════════════════════════

# ──────────────────────────────────────────────
# 유휴자금 출처 어댑터 (전략별) — 자동 예수금 산출
# ──────────────────────────────────────────────

_SRC_STRATEGIES = ["DSS", "종가평균", "표준편차", "Sigma", "IUO", "듀얼스나이퍼", "직접 입력"]


def _cap_adj_sum(cfg_dict) -> float:
    """capital_adj_history(증액/감액 이력)의 순합. 종가평균·표준편차용(백테스트 후 합산)."""
    raw = cfg_dict.get("capital_adj_history", "[]")
    try:
        hist = json.loads(raw) if isinstance(raw, str) else (raw or [])
        return float(sum(float(it.get("조정금액", 0)) for it in hist))
    except Exception:
        return 0.0


def _src_telegram(strategy):
    """출처 전략에 설정된 텔레그램 (token, chat_id). 미설정·미구현이면 ('', '')."""
    try:
        if strategy == "DSS":
            from strategies import dss
            c = dss._load_dss_config()
            return c.get("tg_token", ""), c.get("tg_chat_id", "")
        if strategy == "듀얼스나이퍼":
            from strategies import dual_sniper as dual
            c = dual._load_ds_config()
            return c.get("tg_token", ""), c.get("tg_chat_id", "")
        if strategy == "IUO":
            from strategies import iuo
            c = iuo._load_iuo_config()
            return c.get("tg_token", ""), c.get("tg_chat_id", "")
        if strategy in ("종가평균", "표준편차", "Sigma"):
            us = dict(st.session_state.get("user_settings", {}) or {})
            if not _IS_CLOUD:
                try:
                    from common.config import load_config
                    us = {**(load_config() or {}), **us}
                except Exception:
                    pass
            km = {"종가평균": ("tg_token", "tg_chat_id"),
                  "표준편차": ("sd_tg_token", "sd_tg_chat_id"),
                  "Sigma": ("sigma_tg_token", "sigma_tg_chat_id")}[strategy]
            return us.get(km[0], "") or "", us.get(km[1], "") or ""
    except Exception:
        pass
    return "", ""


def _src_list_accounts(strategy):
    """해당 전략의 계좌/티커 목록. 실패 시 빈 리스트."""
    try:
        if strategy == "DSS":
            from strategies import dss
            return list(dss._load_dss_config().get("accounts", {}).keys())
        if strategy == "듀얼스나이퍼":
            from strategies import dual_sniper as dual
            return list(dual._load_ds_config().get("accounts", {}).keys())
        if strategy == "IUO":
            from strategies import iuo
            return list(iuo._load_iuo_config().keys())
        from common.config import get_ticker_settings
        if strategy == "종가평균":
            return list(get_ticker_settings(prefix="", settings_key="ticker_settings").keys())
        if strategy == "표준편차":
            return list(get_ticker_settings(prefix="sd_", settings_key="sd_ticker_settings").keys())
        if strategy == "Sigma":
            return list(get_ticker_settings(prefix="sigma_", settings_key="sigma_ticker_settings").keys())
    except Exception:
        return []
    return []


@st.cache_data(show_spinner="계좌 예수금 산출 중...", ttl=120)
def _src_get_state(strategy, account, today_str):
    """전략·계좌의 현재 예수금/투자금/분할수/보유 산출.
    반환 {cash, capital, div, n_pos, detail}. 미지원/실패 시 예외 발생."""
    from common.data import load_price_data
    from common.config import get_ticker_settings
    today = datetime.today().date()

    if strategy == "DSS":
        from strategies import dss
        accounts = dss._load_dss_config().get("accounts", {})
        if account not in accounts:
            raise ValueError(f"DSS 계좌 '{account}' 없음")
        stt = _dss_account_state(account, json.dumps(accounts[account], ensure_ascii=False), today_str)
        return {"cash": stt["cash"], "capital": stt["capital"], "div": stt["divisions"],
                "n_pos": stt["n_pos"], "detail": f"DSS {account} (기준 {stt['date']})"}

    if strategy == "종가평균":
        from avg_close_engine import run_portfolio_for_ordersheet
        c = get_ticker_settings(prefix="", settings_key="ticker_settings")[account]
        os_start = c.get("os_start", "2024-01-01"); cap = float(c.get("os_capital", 10000))
        pdf = load_price_data(account, os_start, today, "Yahoo Finance", None)
        res = run_portfolio_for_ordersheet(
            pdf, os_start, account, float(c.get("a_buy", -0.005)), float(c.get("a_sell", 0.009)),
            float(c.get("sell_ratio", 100)), int(c.get("divisions", 5)), cap,
            n_days=int(c.get("n_days", 2)))
        adj = _cap_adj_sum(c)   # 증액/감액 반영 (그쪽 주문표와 동일 방식)
        return {"cash": res["cash"] + adj, "capital": res["current_asset"] + adj,
                "div": int(c.get("divisions", 5)),
                "n_pos": len(res.get("open_tiers", [])),
                "detail": f"종가평균 {account}" + (f" (조정 {adj:+,.0f})" if adj else "")}

    if strategy == "표준편차":
        from stdev_engine import run_stdev_ordersheet
        c = get_ticker_settings(prefix="sd_", settings_key="sd_ticker_settings")[account]
        os_start = c.get("os_start", "2024-01-01"); cap = float(c.get("os_capital", 20000))
        buf = (pd.to_datetime(str(os_start)) - pd.DateOffset(days=90)).strftime("%Y-%m-%d")
        pdf = load_price_data(account, buf, str(today), "야후파이낸스 (yfinance)", None)
        res = run_stdev_ordersheet(
            pdf, str(os_start), sigma_period=int(c.get("sd_sigma_period", 2)),
            k_buy=float(c.get("sd_k_buy", 0.65)), k_sell=float(c.get("sd_k_sell", 0.45)),
            sell_ratio=float(c.get("sell_ratio", 85)), divisions=int(c.get("divisions", 5)),
            renewal=int(c.get("sd_renewal", 5)), initial_capital=cap)
        adj = _cap_adj_sum(c)   # 증액/감액 반영
        return {"cash": res["cash"] + adj, "capital": res.get("total_invest", res["final_asset"]) + adj,
                "div": int(c.get("divisions", 5)), "n_pos": max(0, int(res.get("next_tier", 1)) - 1),
                "detail": f"표준편차 {account}" + (f" (조정 {adj:+,.0f})" if adj else "")}

    if strategy == "듀얼스나이퍼":
        from strategies import dual_sniper as dual
        accounts = dual._load_ds_config().get("accounts", {})
        if account not in accounts:
            raise ValueError(f"듀얼 계좌 '{account}' 없음")
        ad = accounts[account]
        os_start = ad.get("os_start", "2016-01-04")
        os_capital = float(ad.get("os_capital", 10000))
        ds_p, mode_rule = dual._acct_params(ad, {})
        ds_p.initial_capital = os_capital   # 시뮬 시작자본 = base (render와 동일)
        raw = ad.get("capital_adj_history", "[]")
        try:
            adj = json.loads(raw) if isinstance(raw, str) else (raw or [])
        except Exception:
            adj = []
        inj = {a["날짜"]: float(a["조정금액"]) for a in adj if a.get("날짜")}
        px_df = dual.get_soxl_data()
        # ★ 계좌의 모드 소스에 맞춰 모드맵 선택 (render_ds_account와 동일)
        m_src = ad.get("mode_source", "자동")
        try:
            m_src = dual._normalize_src(m_src)
        except Exception:
            pass
        if m_src == "자동":
            mode_map = dual.build_auto_mode_map(px_df, ma_weeks=mode_rule["ma_weeks"],
                                                peak_thr=mode_rule["peak_thr"], dn=mode_rule["dn"])
            _mr, forced = mode_rule, None
        else:   # 원본시트 / 공격 / 방어 → 원전략 실제모드(번들+공유)
            try:
                extra = dict(dual._load_shared_modes())
            except Exception:
                extra = {}
            mode_map = dual.build_original_mode_map(px_df, extra_modes=extra)
            _mr = None
            forced = m_src if m_src in ("공격", "방어") else None
        r = dual.build_today_orders(px_df, ds_p, mode_map=mode_map, start_date=str(os_start),
                                    mode_rule=_mr, forced_mode=forced,
                                    capital_injections=inj or None)
        return {"cash": float(r["cash"]), "capital": float(r["total_asset"]),
                "div": int(r["divisions"]), "n_pos": int(r["n_pos"]),
                "detail": f"듀얼스나이퍼 {account} (모드 {r.get('next_mode', '')}·출처 {m_src})"}

    raise NotImplementedError(f"'{strategy}' 자동 예수금 산출은 아직 미지원입니다. 수동 입력을 사용하세요.")


def _render_cam_account(name, acct, cfg):
    """카마릴라 계좌 1개의 오늘 주문 렌더링."""
    sfx = name.replace(" ", "_").replace("/", "_")
    coef = float(acct.get("coef", 0.70))
    vt = float(acct.get("vol_target", 0.70))
    vp = int(acct.get("vol_period", 20))
    res_tiers = int(acct.get("reserve_tiers", 1))
    ov_ticker = (acct.get("ov_ticker") or "SOXL").strip().upper()
    # 레거시 마이그레이션 (source="dss:NAME" → source_strategy/account)
    if "source_strategy" not in acct:
        old = acct.get("source", "manual")
        if old.startswith("dss:"):
            acct["source_strategy"], acct["source_account"] = "DSS", old[4:]
        else:
            acct["source_strategy"], acct["source_account"] = "직접 입력", ""
    src_strat = acct.get("source_strategy", "직접 입력")
    src_acct = acct.get("source_account", "")

    # ── 유휴자금 출처 상태 ──
    cash = float(acct.get("cash", 0)); capital = float(acct.get("capital", 0)); div = int(acct.get("div", 7))
    if src_strat == "직접 입력":
        st.caption("✍️ 유휴자금 직접 입력")
        m1, m2, m3 = st.columns(3)
        cash = m1.number_input("예수금 ($)", 0, 10_000_000_000, int(cash), 1000, key=f"cam_mc_{sfx}")
        capital = m2.number_input("투자금 ($)", 0, 10_000_000_000, int(capital), 1000, key=f"cam_mcap_{sfx}")
        div = m3.number_input("분할수", 1, 50, int(div) if div else 7, key=f"cam_mdiv_{sfx}")
        if (int(cash), int(capital), int(div)) != (int(acct.get("cash", 0)), int(acct.get("capital", 0)), int(acct.get("div", 7))):
            acct.update(cash=int(cash), capital=int(capital), div=int(div))
            cfg["accounts"][name] = acct; _save_cam_config(cfg)
    else:
        sc1, sc2 = st.columns([3, 1])
        sc1.markdown(f"**출처: {src_strat} · {src_acct}** — 저장된 예수금 ${cash:,.0f} · 투자금 ${capital:,.0f} · {div}분할")
        if sc2.button("🔄 자동 불러오기", key=f"cam_pull_{sfx}"):
            try:
                stt = _src_get_state(src_strat, src_acct, datetime.today().strftime("%Y-%m-%d"))
                acct.update(cash=float(stt["cash"]), capital=float(stt["capital"]), div=int(stt["div"]))
                cfg["accounts"][name] = acct; _save_cam_config(cfg)
                cash, capital, div = stt["cash"], stt["capital"], stt["div"]
                st.success(f"✅ {stt['detail']}: 예수금 ${cash:,.0f} · 투자금 ${capital:,.0f} · {div}분할 · 보유 {stt['n_pos']}/{div}")
                st.rerun()
            except Exception as e:
                st.error(f"자동 산출 실패 ({e}). 아래 계좌 설정에서 출처를 확인하거나 '직접 입력'으로 바꿔 수동 관리하세요.")
        if cash <= 0:
            st.caption("※ '🔄 자동 불러오기'를 눌러 현재 예수금을 가져오세요.")

    # ── 오버레이 계산 ──
    ov_px = _get_price(ov_ticker)
    if ov_px is None or len(ov_px) == 0:
        st.error(f"오버레이 종목 **'{ov_ticker or '(빈값)'}'** 가격 데이터를 불러오지 못했습니다. "
                 f"아래 **⚙️ 계좌 설정**에서 종목(예: SOXL)을 확인하거나, 개인설정에서 "
                 f"**가격 데이터 캐시 초기화** 후 다시 시도하세요.")
        with st.expander("⚙️ 계좌 설정 수정 / 삭제"):
            _nt = st.text_input("오버레이 종목", value=ov_ticker, key=f"cam_fixtkr_{sfx}")
            if st.button("💾 종목 저장", key=f"cam_fixsave_{sfx}", type="primary"):
                acct["ov_ticker"] = _nt.strip().upper() or "SOXL"
                cfg["accounts"][name] = acct
                _save_cam_config(cfg)
                st.cache_data.clear()
                st.rerun()
        return
    last = ov_px.iloc[-1]
    last_date = ov_px.index[-1].date()
    ntd = _next_trading_date(last_date)
    ph, pl, pc = float(last['High']), float(last['Low']), float(last['Close'])
    resistance = pc + (ph - pl) * coef
    vmult = _vol_mult(ov_px, vp, vt)
    tier = capital / div if div else 0
    reserve = res_tiers * tier
    borrowable = max(0.0, cash - reserve)
    deployable = borrowable * vmult
    buy_shares = int(deployable / resistance) if resistance > 0 else 0

    st.markdown(f"**오버레이 계산** — `{ov_ticker}` · 계수 {coef:.2f} · 타겟 {vt} · "
                f"{last_date} → 적용 {ntd}")
    b = st.columns(4)
    b[0].metric("1티어", f"${tier:,.0f}")
    b[1].metric(f"유보 ({res_tiers}티어)", f"${reserve:,.0f}")
    b[2].metric("차입가능 유휴", f"${borrowable:,.0f}")
    b[3].metric("변동성 비중", f"{vmult*100:.0f}%",
                help=f"최근 {vp}일 변동성 기준, 목표 {vt} 대비 자동 축소")

    held = st.number_input("오버레이 보유수량 (어제 매수분, 있으면 시가 매도)", 0, 100_000_000,
                           int(acct.get("held_qty", 0)), 1, key=f"cam_held_{sfx}")
    if int(held) != acct.get("held_qty", 0):
        acct["held_qty"] = int(held)
        cfg["accounts"][name] = acct
        _save_cam_config(cfg)

    # ── 오늘 주문 ──
    if held > 0:
        st.warning(f"💰 **매도**: 보유 {held:,}주 → 시가(MOO) 전량 매도 (≈ ${held*pc:,.0f})")
    if buy_shares > 0:
        st.success(
            f"🎯 **매수 — 자동감시주문(역지정가)** ({ov_ticker})\n\n"
            f"- 감시조건: **현재가 ≥ ${resistance:,.2f} 이상** 도달 시\n"
            f"- 주문유형: **시장가** · 수량: **{buy_shares:,}주**\n"
            f"- 유지기간: ⭐ **다음 거래일 1일만** (갭상승·장중돌파 모두 자동 체결)\n"
            f"- 투입 ≈ **${deployable:,.0f}** (차입 ${borrowable:,.0f} × 변동성 {vmult*100:.0f}%)")
        st.caption("💡 매도는 체결된 다음날 **MOO(개장 시가)** 전량 매도. 상세는 소개 탭의 **🛒 실전 주문법** 참고.")
    else:
        st.info("매수 가능 금액 없음 (변동성↑로 비중 축소 또는 유휴현금 부족)")

    o = st.columns(4)
    o[0].metric("전일 종가", f"${pc:,.2f}")
    o[1].metric("전일 고가", f"${ph:,.2f}")
    o[2].metric("전일 저가", f"${pl:,.2f}")
    o[3].metric("돌파 기준가", f"${resistance:,.4f}")

    # ── 텔레그램 (계좌 전용 우선 → 없으면 출처 전략) ──
    _own_tok = (acct.get("tg_token", "") or "").strip()
    _own_chat = (acct.get("tg_chat_id", "") or "").strip()
    if _own_tok and _own_chat:
        tg_token, tg_chat, _tg_from = _own_tok, _own_chat, "이 계좌 전용"
    else:
        tg_token, tg_chat = _src_telegram(src_strat)
        _tg_from = f"출처 전략 {src_strat}"
    txt = (f"<b>📋 카마릴라 오버레이 — {name} ({ntd})</b>\n"
           f"출처: {src_strat} {src_acct} · 종목: {ov_ticker}\n"
           f"━━━━━\n🎯 매수: ${resistance:,.4f} 돌파 시 {buy_shares:,}주 (투입 ${deployable:,.0f})\n"
           f"   변동성 비중 {vmult*100:.0f}%\n"
           f"💰 매도: {'보유 '+format(int(held),',')+'주 시가 매도' if held>0 else '없음'}")
    cb1, cb2 = st.columns([1, 3])
    _tg_ok = bool(tg_token and tg_chat)
    if cb1.button("📨 텔레그램 발송", disabled=not _tg_ok, key=f"cam_tg_{sfx}"):
        r = _send_telegram(tg_token, tg_chat, txt)
        st.success("✅ 발송 완료!") if r.get("ok") else st.error(f"실패: {r.get('description')}")
    if _tg_ok:
        cb2.caption(f"✉️ **{_tg_from}** 텔레그램으로 발송됩니다.")
    else:
        cb2.caption(f"※ **{_tg_from}** 텔레그램 미설정 — 아래 '계좌 설정'에서 "
                    f"**이 계좌 전용 텔레그램**을 등록하거나, 해당 전략 개인설정에 등록하세요.")

    # ── 계좌 설정 수정 / 삭제 ──
    with st.expander("⚙️ 계좌 설정 수정 / 삭제"):
        e1, e2 = st.columns(2)
        si = _SRC_STRATEGIES.index(src_strat) if src_strat in _SRC_STRATEGIES else len(_SRC_STRATEGIES) - 1
        new_strat = e1.selectbox("유휴자금 출처 전략", _SRC_STRATEGIES, index=si, key=f"cam_estrat_{sfx}")
        _accts = _src_list_accounts(new_strat) if new_strat != "직접 입력" else []
        if new_strat == "직접 입력":
            new_acct = ""
            e2.caption("직접 입력은 계좌 탭에서 예수금을 수동 관리")
        elif _accts:
            _ai = _accts.index(src_acct) if src_acct in _accts else 0
            new_acct = e2.selectbox("출처 계좌/티커", _accts, index=_ai, key=f"cam_eacct_{sfx}")
        else:
            new_acct = e2.text_input("출처 계좌/티커 (목록 없음 — 직접)", value=src_acct, key=f"cam_eacctt_{sfx}")
        e3, e4, e5, e6 = st.columns(4)
        new_tkr = e3.text_input("오버레이 종목", value=ov_ticker, key=f"cam_etkr_{sfx}")
        new_coef = e4.number_input("계수", 0.1, 2.0, coef, 0.05, key=f"cam_ecoef_{sfx}")
        new_vt = e5.number_input("변동성 타겟", 0.2, 1.5, vt, 0.05, key=f"cam_evt_{sfx}")
        new_res = e6.number_input("유보 티어수", 1, 5, res_tiers, key=f"cam_eres_{sfx}")

        st.markdown("**📨 이 계좌 전용 텔레그램 (선택)** — 입력하면 출처 전략 대신 **이 주소로** 발송, "
                    "비우면 출처 전략을 따라갑니다.")
        tg1, tg2 = st.columns(2)
        new_tgc = tg1.text_input("Chat ID", value=acct.get("tg_chat_id", ""),
                                 placeholder="비우면 출처 전략 따라감", key=f"cam_etgc_{sfx}")
        new_tgt = tg2.text_input("Bot Token", value=acct.get("tg_token", ""),
                                 type="password", key=f"cam_etgt_{sfx}")

        sv, dl = st.columns([1, 1])
        if sv.button("💾 저장", key=f"cam_esave_{sfx}", type="primary"):
            acct.update(source_strategy=new_strat, source_account=new_acct,
                        ov_ticker=new_tkr.strip().upper(), coef=float(new_coef),
                        vol_target=float(new_vt), reserve_tiers=int(new_res),
                        tg_chat_id=new_tgc.strip(), tg_token=new_tgt.strip())
            cfg["accounts"][name] = acct
            _save_cam_config(cfg)
            st.success("저장되었습니다. '🔄 자동 불러오기'로 예수금을 갱신하세요.")
            st.rerun()
        if dl.button("🗑️ 이 계좌 삭제", key=f"cam_edel_{sfx}"):
            cfg["accounts"].pop(name, None)
            _save_cam_config(cfg)
            st.warning(f"'{name}' 삭제됨.")
            st.rerun()


def render_ordersheet_tab(params):
    cfg = _load_cam_config()
    st.subheader(f"📋 오늘의 오버레이 주문표  ({datetime.today().strftime('%Y-%m-%d')})")
    st.caption("카마릴라 계좌마다 **어느 전략(DSS·종가평균·표준편차 등)의 유휴 예수금**을 활용할지 설정해, "
               "계좌별로 오늘 주문을 받습니다.")

    accounts = cfg.setdefault("accounts", {})

    # ── 계좌 추가 ──
    with st.expander("➕ 카마릴라 계좌 추가", expanded=not accounts):
        c1, c2 = st.columns(2)
        new_name = c1.text_input("계좌 이름", placeholder="예: 메인 오버레이, ISA 오버레이", key="cam_new_name")
        new_strat = c2.selectbox("유휴자금 출처 전략", _SRC_STRATEGIES, key="cam_new_strat")
        _accts = _src_list_accounts(new_strat) if new_strat != "직접 입력" else []
        if new_strat == "직접 입력":
            new_acct = ""
            st.caption("직접 입력: 계좌 탭에서 예수금을 수동 관리합니다.")
        elif _accts:
            new_acct = st.selectbox("출처 계좌/티커", _accts, key="cam_new_acct")
        else:
            new_acct = st.text_input(f"'{new_strat}' 계좌 목록이 없습니다 — 티커/계좌명 직접 입력", key="cam_new_acctt")
        c3, c4, c5 = st.columns(3)
        new_tkr = c3.text_input("오버레이 종목", value=cfg.get("ov_ticker", "SOXL"), key="cam_new_tkr")
        new_coef = c4.number_input("계수", 0.1, 2.0, float(cfg.get("ov_coef", 0.70)), 0.05, key="cam_new_coef")
        new_vt = c5.number_input("변동성 타겟", 0.2, 1.5, float(cfg.get("ov_vol_target", 0.70)), 0.05, key="cam_new_vt")
        st.caption("권장: 계수 0.70 · 타겟 0.70 (홀드아웃 검증된 정직 설정). "
                   "DSS·종가평균·표준편차·듀얼스나이퍼는 예수금 자동 산출, Sigma·IUO는 수동 입력.")
        if st.button("✅ 계좌 등록", type="primary", key="cam_add_acct", use_container_width=True):
            nm = new_name.strip()
            if not nm:
                st.warning("계좌 이름을 입력하세요.")
            elif nm in accounts:
                st.warning(f"'{nm}' 계좌가 이미 존재합니다.")
            else:
                accounts[nm] = {
                    "source_strategy": new_strat, "source_account": new_acct,
                    "ov_ticker": new_tkr.strip().upper(), "coef": float(new_coef),
                    "vol_target": float(new_vt), "vol_period": int(cfg.get("ov_vol_period", 20)),
                    "reserve_tiers": int(cfg.get("ov_reserve_tiers", 1)),
                    "held_qty": 0, "cash": 0, "capital": 0, "div": 7,
                }
                cfg["accounts"] = accounts
                _save_cam_config(cfg)
                st.success(f"✅ '{nm}' 등록 완료 (출처: {new_strat} {new_acct})")
                st.rerun()

    if not accounts:
        st.info("등록된 카마릴라 계좌가 없습니다. 위 **➕ 카마릴라 계좌 추가**에서 만들어 주세요.")
        return

    # ── 계좌별 탭 ──
    names = list(accounts.keys())
    tabs = st.tabs([f"📊 {n}" for n in names])
    for nm, tb in zip(names, tabs):
        with tb:
            _render_cam_account(nm, accounts[nm], cfg)


# ══════════════════════════════════════════════
# 탭 4: 전략 소개 & 성과
# ══════════════════════════════════════════════

def render_intro_tab(params=None):
    st.subheader("📖 전략 소개 & 성과")
    t = st.tabs(["① 무엇인가", "② 매매 규칙", "🛒 실전 주문법", "③ 왜 이 설정",
                 "④ 애드온 운용", "⑤ 검증 성과", "⑥ 주의사항"])
    with t[0]:
        st.markdown("""
### 카마릴라 피봇 돌파 매매법
전일 **고가·저가·종가**만으로 오늘의 저항선을 계산해, **강한 상승 돌파가 나오면 사서 다음날 아침에 판다.**
완두콩님 원전략에 **변동성 위험관리 + 유휴현금 애드온 + 정직한 체결 검증**을 더했습니다.
""")
        st.code("저항선 = 전일종가 + (전일고가 − 전일저가) × 계수\n"
                "계수 0.55 = R4(원전략) / 0.70 = 우리 운용값(강한 돌파 선별)", language="text")
    with t[1]:
        st.markdown("""
### 매일 하는 일 (한국 시간 기준)
미국 장은 **한국 새벽에 마감**되고 **한국 밤에 개장**합니다. 그래서:

**🌅 새벽 (미국 마감, 5~6시경) — 저항선 확정**
직전 미국봉(고가·저가·종가)이 확정되면 저항선이 정해집니다.
```
저항선 = 직전 종가 + (직전 고가 − 직전 저가) × 0.70
```
돌파가격이 **미리 확정**되므로, 아침에 확인 후 stop-buy 주문을 걸어둘 수 있습니다. (손매매 친화적)

**☀️ 낮 — 투입비중 결정**
```
매수비중 = min(100%, 목표0.7 ÷ 최근20일 변동성)
```
변동성이 치솟은 날은 비중을 자동 축소 → 위기 때 발 빼기.

**🌙 밤 (미국 개장, 10:30~11:30시경) — 주문 실행**
- 보유분 있으면 → **개장 시가(MOO)로 전량 매도**
- 장중 저항선 돌파 → 그 가격에 매수 (갭상승이면 시가 매수)

**🔁 다음날 새벽 다시 매도 → 반복** — 보유기간 딱 **하룻밤(1거래일)**

> ※ 서머타임(3~11월): 개장 ≈ 밤 10:30 / 마감 ≈ 새벽 5:00. 겨울(11~3월): 개장 ≈ 밤 11:30 / 마감 ≈ 새벽 6:00.
""")
    with t[2]:
        st.markdown("""
### 🛒 실전 주문 방법 (메리츠증권 기준 예시)

이 전략은 **두 주문이 한 쌍**입니다 — ① 자동감시 매수(역지정가) + ② MOO 매도.

#### ① 매수 — 자동감시주문 (역지정가 / STOP)
"가격이 **저항선 이상**으로 오르면 시장가 매수"를 개장 전에 미리 걸어둡니다.
- **감시조건**: `현재가 ≥ 저항선($X) 이상` 도달 시 실행
- **주문유형**: `시장가`
- **주문수량**: 주문표가 계산한 **오늘 수량**
- **유지기간**: ⭐ **다음 거래일 1일만** (5일/30일/90일 누르지 마세요!)

→ 이 **한 주문이 갭상승·장중돌파 둘 다** 처리합니다:
- 시가가 이미 저항선 위로 **갭상승** → 개장 즉시 체결
- 장중에 저항선 **터치** → 그 순간 체결

#### ② 매도 — MOO (개장 시장가)
매수가 체결되면 **다음 거래일 개장 시가에 전량 매도** — 메리츠 **MOO 매도** 주문 활용.

#### ⭐ 매일 갱신이 핵심
저항선·수량은 **매일 바뀝니다**. 어제 값을 그대로 두면 안 됩니다.
- 매일 주문표에서 **새 저항선·새 수량** 확인 → 자동감시 매수 새로 설정
- 안 터진 어제 주문은 폐기 (유지기간을 당일로 둬서 자동 만료)

#### 💡 참고
- 다른 증권사도 **"자동감시 / 스탑 / 역지정가"** 메뉴를 찾으면 동일하게 가능.
- 시장가라 3배 ETF에선 약간의 슬리피지(체결가 ≈ 저항선 또는 약간 위) — 정상 범위.
- 갭상승만 잡으면 안 되는 이유: 장중돌파가 거래의 **76%·수익의 대부분** →
  갭상승만 하면 CAGR 68%→16%로 급락. 그래서 자동감시(역지정가) 매수가 **필수**입니다.
""")
        st.markdown("##### 📷 메리츠 자동매수 감시주문 설정 예시")
        _img = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets", "meritz_autobuy.png")
        if os.path.exists(_img):
            st.image(_img, width=400,
                     caption="①현재가 ≥ 저항선  ②시장가  ③유지=당일 — 위 화면 그대로 따라 입력")
        else:
            st.info("이미지 로딩 중이거나 아직 배포 반영 전입니다. 새로고침(Ctrl+Shift+R) 후 다시 확인해 주세요.")
    with t[3]:
        st.markdown("""
### 검증으로 도달한 설정
- **청산 = 익일 시가 MOO**: 당일종가·익일종가·트레일링·손절 전부 비교 → 3구간 홀드아웃 모두 CALMAR 1위.
  익일종가(MOC)는 되돌림에 MDD −78%, 트레일링은 휩쏘로 마이너스.
- **계수 0.70 + 변동성타겟 0.7**: 손절 없어 갭 가정이 없는 **정직한 수치**. 3구간 모두 CAGR~68%·MDD−28%.
- **버린 것**: 손절 기반 고CAGR은 "갭 무시 −5% 체결" 가정으로 부풀려진 것(보정 시 CALMAR 2.86→1.15).
  타인의 CAGR 198%/Calmar 6.93은 과적합 트릭(거의 항상보유 + 잔량누적 + 종가MDD).
""")
    with t[4]:
        st.markdown("""
### 유휴 예수금 애드온 + 워터폴
- DSS는 평소 자산의 절반 이상을 예수금으로 보유 → 그 유휴현금에 오버레이
- 오버레이는 **유휴현금만** 사용, DSS가 살 수 있게 **1티어는 유보**
- 손익 → 별도 수익계좌 / 쌓이면 연말 일정%를 DSS 증액
- 공격형 DSS는 MDD가 증액과 무관히 ~−50% 고정 → **증액 70%가 CALMAR 정점**
""")
        st.info("주문표 탭에서 DSS 계좌를 선택하면 예수금·투자금·분할수를 자동으로 읽어와 오늘 주문을 계산합니다.")
    with t[5]:
        st.markdown("""
### 백테스트 성과 (SOXL 2010~2026, 홀드아웃 검증)
**추천: 계수0.70 · MOO · 변동성타겟0.7**

| 구간 | CAGR (슬리피지 0%) | CAGR (0.1% 적용) | MDD | CALMAR(0%/0.1%) |
|------|:----:|:----:|:----:|:----:|
| 전체 | 68.6% | **56.0%** | −28~30% | 2.45 / 1.86 |
| 전반(10~18) | 69.8% | ~58% | −27.5% | 2.53 / ~2.0 |
| 후반(19~26) | 67.2% | ~54% | −28% | 2.40 / ~1.8 |

→ 두 반기 거의 동일 = **robust(과적합 아님)**

⚠️ **슬리피지 주의**: 1일 보유라 거래가 잦아(연 ~93회) 슬리피지 민감도가 큽니다.
기본값 **0.1% 적용 시 CAGR ~56%·CALMAR ~1.86** 가 현실적 기대치입니다 (사이드바에서 조절 가능).

**합산(공격형 DSS + 오버레이 + 증액70%, 2015~26, 슬리피지0.1%)**: CAGR~96% / MDD~−50% / 자산 +132%
""")
    with t[6]:
        st.markdown("""
### ⚠️ 위험
1. **SOXL = 3배 레버리지 단일종목** (실제 B&H MDD −90%). DSS와 동시급락 위험 — 오버레이는 위험분산 X, 수익만 추가
2. **돌파 슬리피지** — 백테스트 0.1% 반영, 실거래는 더 클 수 있음
3. **MDD −28~50% 실재** — 변동성타게팅이 낮추지만 0 아님
4. **과적합 경계** — 실거래 데이터 쌓이면 주기적 재점검
5. **워터폴 버퍼** — 증액 많을수록 손실보전 버퍼 더 필요, 처음엔 작게 시작
""")


# ══════════════════════════════════════════════
# 탭 5: DB 조회 (간단 매매기록)
# ══════════════════════════════════════════════

def render_db_tab(params):
    p = params
    st.subheader("📂 DB 조회 — 일별 매매 기록")
    st.caption(f"백테스트 결과를 일자별로 조회합니다. (슬리피지 {float(p.get('slippage', 0.1)):.2f}% 적용)")
    bt = _run_bt(p["ticker"], p["coef"], p["initial_capital"], p["position_pct"],
                 p["signal"], p["vol_target"], p["vol_period"], p["start_date"], p["end_date"],
                 float(p.get("slippage", 0.1)))
    if not len(bt):
        st.warning("데이터 없음")
        return
    show = bt[['날짜', '시가', '고가', '저가', '종가', '저항선', '매수체결', '매수수량',
               '매도가', '매도수량', '보유수량', '예수금', '총자산', '당일실현']].copy()
    show['날짜'] = show['날짜'].dt.strftime('%Y-%m-%d')
    st.dataframe(show, use_container_width=True, hide_index=True, height=560)
    st.download_button("CSV 다운로드", show.to_csv(index=False).encode('utf-8-sig'),
                       file_name=f"camarilla_db_{p['ticker']}.csv", key="cam_db_dl")


# ══════════════════════════════════════════════
# 탭 6: 개인 설정
# ══════════════════════════════════════════════

def render_settings_tab():
    st.subheader("⚙️ 개인 설정")
    cfg = _load_cam_config()
    st.success(f"🖥️ 설정 저장 위치: 로컬 `{_CAM_CONFIG_PATH}`" +
               (" + 클라우드 동기화" if _IS_CLOUD else ""))

    with st.container(border=True):
        st.markdown("#### 🎯 오버레이 운용 설정 (확정 기본값)")
        v = st.columns(3)
        o_tkr = v[0].text_input("오버레이 종목", value=cfg.get("ov_ticker", "SOXL"), key="cam_s_tkr")
        o_coef = v[1].number_input("저항선 계수", 0.1, 2.0, float(cfg.get("ov_coef", 0.70)),
                                   0.05, format="%.2f", key="cam_s_coef")
        o_vt = v[2].number_input("변동성 타겟", 0.2, 1.5, float(cfg.get("ov_vol_target", 0.70)),
                                 0.05, format="%.2f", key="cam_s_vt")
        v2 = st.columns(3)
        o_vp = v2[0].number_input("변동성 측정기간", 5, 120, int(cfg.get("ov_vol_period", 20)), key="cam_s_vp")
        o_res = v2[1].number_input("유보 티어수", 1, 5, int(cfg.get("ov_reserve_tiers", 1)), key="cam_s_res")
        o_inj = v2[2].number_input("DSS 증액 비율 (%)", 0, 100,
                                   int(cfg.get("ov_inject_frac", 0.70)*100), 5, key="cam_s_inj")
        st.caption("권장: 계수 0.70 · 타겟 0.70 · 유보 1티어 · 증액 70%(공격형 DSS) — 홀드아웃 검증된 정직 설정")
        if st.button("💾 오버레이 설정 저장", type="primary", key="cam_s_save"):
            cfg.update({"ov_ticker": o_tkr.strip().upper(), "ov_coef": float(o_coef),
                        "ov_vol_target": float(o_vt), "ov_vol_period": int(o_vp),
                        "ov_reserve_tiers": int(o_res), "ov_inject_frac": o_inj/100.0})
            _save_cam_config(cfg)
            st.success("저장되었습니다.")

    with st.container(border=True):
        st.markdown("#### 💬 텔레그램")
        st.info("카마릴라 주문표 텔레그램 우선순위: **① 계좌 전용 텔레그램**(주문표 탭의 "
                "⚙️ 계좌 설정에서 입력) → 없으면 **② 출처 전략의 텔레그램**(DSS·표준편차 등). "
                "둘 다 없으면 발송 버튼이 비활성화됩니다. "
                "→ 듀얼스나이퍼처럼 별도 채널로 받고 싶으면 계좌 전용 텔레그램에 입력하세요.")

    with st.container(border=True):
        st.markdown("#### 🔄 데이터")
        if st.button("가격 데이터 캐시 초기화", key="cam_s_cache"):
            st.cache_data.clear()
            st.success("캐시를 비웠습니다.")
