"""strategies/portfolio.py — 4개 전략 합산 포트폴리오 시뮬레이터.

종가평균 / 표준편차 / DSS / 듀얼스나이퍼 의 백테스트 자산곡선을 투자금액 비례로
합산하여 포트폴리오 CAGR/MDD/연·월 수익을 계산하고, 목표 CAGR/MDD 를 만족하는
금액 배분을 역산한다.

핵심 가정 (선형 스케일링)
  - 각 전략을 '독립 계좌'로 운용한다고 본다 (전략끼리 현금/포지션 공유 없음).
  - 각 전략의 자산곡선은 투자금액에 비례한다고 가정 → 1달러당 정규화 곡선을
    먼저 만들고, 투자금액을 곱해 합산한다.
  - 따라서 합산 CAGR/MDD(%)는 '투자금액의 비율'에만 의존하고 총액에는 무관하다.
  - 정수주 매매로 인한 비선형(소액일수록 체결 단위 오차)은 무시한다.

이 모듈은 app.py 에서 '🧩 포트폴리오 합산' 전략 선택 시 render() 가 호출된다.
"""
from __future__ import annotations

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime

# ── 엔진 import ──────────────────────────────────────────────
import avg_close_engine
import stdev_engine
from dss_engine import (
    load_price_data as _load_px_C,          # 'Close' 컬럼 (avg/stdev/dss 용)
    build_weekly_rsi_series, build_mode_series,
    DSSParams, run_backtest as _dss_run,
)
from dual_sniper_engine import (
    load_price_data as _load_px_lower,       # 'close' 소문자 컬럼 (듀얼 전용)
    DualSniperParams, build_original_mode_map, build_auto_mode_map,
    run_backtest as _dual_run,
)
from common.analysis import compute_annual_stats, compute_monthly_pivot, compute_sharpe_sortino


# ══════════════════════════════════════════════════════════════
# 프리셋 정의 (각 전략 모듈의 SOXL 프리셋과 동일)
# ══════════════════════════════════════════════════════════════
AVG_PRESETS = {
    "🚀 공격형":      dict(a_buy=-0.0055,  a_sell=0.00875, sell_ratio=100.0, divisions=3, n_days=2),
    "⚖️ 균형형":      dict(a_buy=-0.0050,  a_sell=0.0095,  sell_ratio=100.0, divisions=4, n_days=2),
    "🛡️ 안정형":      dict(a_buy=-0.0065,  a_sell=0.0075,  sell_ratio=100.0, divisions=5, n_days=2),
    "🪨 Ultra-Safe형": dict(a_buy=-0.01275, a_sell=0.0075,  sell_ratio=100.0, divisions=7, n_days=2),
}

STDEV_PRESETS = {
    "공격형":      dict(sigma_period=2, k_buy=0.65, k_sell=0.45, sell_ratio=85.0, divisions=3, renewal=3),
    "균형형":      dict(sigma_period=2, k_buy=0.65, k_sell=0.55, sell_ratio=90.0, divisions=6, renewal=10),
    "안정형":      dict(sigma_period=2, k_buy=0.65, k_sell=0.45, sell_ratio=85.0, divisions=5, renewal=5),
    "Ultra-Safe형": dict(sigma_period=2, k_buy=0.55, k_sell=0.30, sell_ratio=95.0, divisions=7, renewal=7),
    "Sonic형":     dict(sigma_period=2, k_buy=0.55, k_sell=0.45, sell_ratio=95.0, divisions=3, renewal=3),
}

DSS_PRESETS = {
    "🚀 공격형": dict(sf_div=7, sf_hold=38, sf_buy=3.3, sf_sell=1.8,
                    ag_div=7, ag_hold=8, ag_buy=3.15, ag_sell=3.75,
                    pcr=75, lcr=20, renewal_period=10, fee_rate=0.04),
    "⚖️ 균형형": dict(sf_div=7, sf_hold=35, sf_buy=3.5, sf_sell=1.8,
                    ag_div=8, ag_hold=7, ag_buy=3.6, ag_sell=6.0,
                    pcr=72, lcr=21, renewal_period=10, fee_rate=0.04),
    "🛡️ 안정형": dict(sf_div=7, sf_hold=36, sf_buy=4.91, sf_sell=0.9,
                    ag_div=7, ag_hold=8, ag_buy=2.77, ag_sell=3.06,
                    pcr=70, lcr=20, renewal_period=10, fee_rate=0.04),
    "🔬 sonic형": dict(sf_div=7, sf_hold=36, sf_buy=3.3, sf_sell=1.8,
                     ag_div=7, ag_hold=6, ag_buy=2.9, ag_sell=6.5,
                     pcr=65, lcr=20, renewal_period=12, fee_rate=0.04),
}

DUAL_PRESETS = {
    "🚀 공격형 (로케트셋)": dict(ag_div=6, ag_buy=8.0, ag_sell_alpha=0.4, ag_hold_alpha=2.0,
                            sf_div=5, sf_hold=8, sf_buy1=-0.6, sf_buy2=5.5,
                            sf_sell=0.7, sf_ma_base=3, sf_weights="6, 13, 20, 27, 34"),
    "⚖️ 균형형 (전차셋)":  dict(ag_div=5, ag_buy=0.5, ag_sell_alpha=0.4, ag_hold_alpha=2.0,
                            sf_div=5, sf_hold=8, sf_buy1=-0.6, sf_buy2=5.5,
                            sf_sell=0.7, sf_ma_base=3, sf_weights="6, 13, 20, 27, 34"),
    "🛡️ 안정형 (헬기셋)":  dict(ag_div=6, ag_buy=0.5, ag_sell_alpha=0.4, ag_hold_alpha=2.0,
                            sf_div=5, sf_hold=8, sf_buy1=-0.6, sf_buy2=5.5,
                            sf_sell=0.7, sf_ma_base=3, sf_weights="6, 13, 20, 27, 34"),
}

DUAL_MODE_OPTS = ["원본시트 자동", "자동 하이브리드"]

# 전략 키 (표시순서) 와 사용자가 현재 쓰는 기본 프리셋
STRAT_KEYS = ["종가평균", "표준편차", "DSS", "듀얼스나이퍼"]
DEFAULT_PRESET = {
    "종가평균": "🛡️ 안정형",
    "표준편차": "Sonic형",
    "DSS": "🚀 공격형",
    "듀얼스나이퍼": "🚀 공격형 (로케트셋)",
}
DEFAULT_AMOUNT = {"종가평균": 10000.0, "표준편차": 10000.0, "DSS": 10000.0, "듀얼스나이퍼": 10000.0}


# ══════════════════════════════════════════════════════════════
# 가격 데이터 로딩
# ══════════════════════════════════════════════════════════════
@st.cache_data(show_spinner="SOXL/QQQ 데이터 로딩...", ttl=600)
def _load_market_data():
    """avg/stdev/dss 용 SOXL('Close'), 듀얼 용 SOXL('close'), DSS 용 QQQ('Close') 반환."""
    soxl_C = _load_px_C("SOXL", "2009-06-01", "2027-01-01")
    qqq_C = _load_px_C("QQQ", "2009-01-01", "2027-01-01")
    soxl_l = _load_px_lower("SOXL", "2009-01-01", "2027-01-01")
    return soxl_C, qqq_C, soxl_l


def _parse_weights(s, div):
    """'6, 13, 20, 27, 34' 문자열 → div 길이 tuple (불일치 시 fallback)."""
    try:
        w = tuple(float(x) for x in str(s).replace(" ", "").split(",") if x != "")
    except Exception:
        w = ()
    if len(w) != div:
        base = [6, 13, 20, 27, 34, 40, 46, 52]
        w = tuple(base[:div]) if div <= len(base) else tuple([round(100.0 / div, 1)] * div)
    return w


# ══════════════════════════════════════════════════════════════
# 전략별 자산곡선 (참조자본 기준 → 호출측에서 정규화)
# ══════════════════════════════════════════════════════════════
_REF_CAP = 10000.0   # 정규화 기준 자본 (선형이므로 값 자체는 무의미)


def _equity_avg(soxl_C, preset, start, end):
    r = avg_close_engine.run_backtest(
        price_df=soxl_C, start_date=str(start), end_date=str(end),
        a_buy=preset["a_buy"], a_sell=preset["a_sell"], sell_ratio=preset["sell_ratio"],
        divisions=preset["divisions"], initial_capital=_REF_CAP,
        return_history=False, n_days=preset["n_days"],
    )
    return pd.Series(r["assets"], index=pd.to_datetime(r["dates"]))


def _equity_stdev(soxl_C, preset, start, end):
    r = stdev_engine.run_backtest_stdev(
        price_df=soxl_C, start_date=str(start), end_date=str(end),
        sigma_period=preset["sigma_period"], k_buy=preset["k_buy"], k_sell=preset["k_sell"],
        sell_ratio=preset["sell_ratio"], divisions=preset["divisions"], renewal=preset["renewal"],
        pcr=1.0, lcr=1.0, initial_capital=_REF_CAP, return_history=False,
    )
    return pd.Series(r["assets"], index=pd.to_datetime(r["dates"]))


def _equity_dss(soxl_C, qqq_C, preset, start, end):
    mode_series = build_mode_series(build_weekly_rsi_series(qqq_C))
    params = DSSParams(
        sf_divisions=preset["sf_div"], sf_max_hold=preset["sf_hold"],
        sf_buy_pct=preset["sf_buy"] / 100, sf_sell_pct=preset["sf_sell"] / 100,
        ag_divisions=preset["ag_div"], ag_max_hold=preset["ag_hold"],
        ag_buy_pct=preset["ag_buy"] / 100, ag_sell_pct=preset["ag_sell"] / 100,
        initial_capital=_REF_CAP, fee_rate=preset["fee_rate"] / 100,
        renewal_period=preset["renewal_period"], pcr=preset["pcr"] / 100, lcr=preset["lcr"] / 100,
    )
    log = _dss_run(params=params, soxl_daily=soxl_C, mode_series=mode_series,
                   start_date=str(start), end_date=str(end))
    s = pd.Series(log["총자산"].values.astype(float), index=pd.to_datetime(log["날짜"]))
    return s


def _equity_dual(soxl_l, preset, mode_source, start, end):
    weights = _parse_weights(preset["sf_weights"], preset["sf_div"])
    params = DualSniperParams(
        ag_divisions=preset["ag_div"], ag_buy_pct=preset["ag_buy"],
        ag_sell_alpha=preset["ag_sell_alpha"], ag_hold_alpha=preset["ag_hold_alpha"],
        sf_divisions=preset["sf_div"], sf_buy_pct1=preset["sf_buy1"], sf_buy_pct2=preset["sf_buy2"],
        sf_sell_pct=preset["sf_sell"], sf_ma_base=preset["sf_ma_base"], sf_hold=preset["sf_hold"],
        sf_tier_weights=weights, initial_capital=_REF_CAP,
    )
    if mode_source == "자동 하이브리드":
        mode_map = build_auto_mode_map(soxl_l, ma_weeks=36, peak_thr=66.0, dn=42.0)
    else:
        try:
            from strategies.dual_sniper import _load_shared_modes
            extra = dict(_load_shared_modes())
        except Exception:
            extra = {}
        mode_map = build_original_mode_map(soxl_l, extra_modes=extra)
    out = _dual_run(soxl_l, params, mode_map=mode_map, start_date=str(start), return_trades=True)
    log = out[0] if isinstance(out, tuple) else out
    log = log[(log["날짜"] >= pd.Timestamp(start)) & (log["날짜"] <= pd.Timestamp(end))]
    s = pd.Series(log["총자산"].values.astype(float), index=pd.to_datetime(log["날짜"]))
    return s


@st.cache_data(show_spinner="4개 전략 백테스트 실행 중...", ttl=600)
def compute_norm_matrix(av_key, sd_key, dss_key, dual_key, dual_mode, start, end):
    """4전략 자산곡선을 공통 거래일로 정렬 후 시작=1.0 으로 정규화한 DataFrame 반환.

    columns = ['종가평균','표준편차','DSS','듀얼스나이퍼'], index = 공통 거래일.
    """
    soxl_C, qqq_C, soxl_l = _load_market_data()
    curves = {
        "종가평균": _equity_avg(soxl_C, AVG_PRESETS[av_key], start, end),
        "표준편차": _equity_stdev(soxl_C, STDEV_PRESETS[sd_key], start, end),
        "DSS": _equity_dss(soxl_C, qqq_C, DSS_PRESETS[dss_key], start, end),
        "듀얼스나이퍼": _equity_dual(soxl_l, DUAL_PRESETS[dual_key], dual_mode, start, end),
    }
    df = pd.concat(curves, axis=1)
    df = df.sort_index().dropna(how="any")
    if df.empty or len(df) < 2:
        return df
    norm = df / df.iloc[0]      # 각 전략 시작=1.0
    return norm


# ══════════════════════════════════════════════════════════════
# 지표 계산
# ══════════════════════════════════════════════════════════════
def _metrics(equity, index):
    """자산배열 → (cagr, mdd, years). cagr/mdd 는 소수(예: 0.45, -0.30)."""
    equity = np.asarray(equity, dtype=float)
    if len(equity) < 2 or equity[0] <= 0:
        return 0.0, 0.0, 0.0
    years = (index[-1] - index[0]).days / 365.25
    cagr = (equity[-1] / equity[0]) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    peak = np.maximum.accumulate(equity)
    mdd = float(((equity - peak) / peak).min())
    return float(cagr), mdd, years


def combine(norm: pd.DataFrame, amounts: dict):
    """정규화곡선 × 투자금액 → 합산 자산곡선 Series."""
    w = pd.Series({k: float(amounts.get(k, 0.0)) for k in norm.columns})
    combined = (norm * w).sum(axis=1)
    return combined


def portfolio_metrics_table(norm: pd.DataFrame, amounts: dict):
    """전략별 개별 지표 + 합산 지표 테이블 (DataFrame) 반환."""
    rows = []
    idx = norm.index
    for k in norm.columns:
        amt = float(amounts.get(k, 0.0))
        c, m, _ = _metrics(norm[k].values * amt if amt > 0 else norm[k].values, idx)
        final = float(norm[k].iloc[-1] * amt)
        rows.append({"구분": k, "투자금액($)": amt, "CAGR(%)": round(c * 100, 2),
                     "MDD(%)": round(m * 100, 2), "최종자산($)": round(final, 0)})
    combined = combine(norm, amounts)
    cc, cm, _ = _metrics(combined.values, idx)
    total_amt = sum(float(amounts.get(k, 0.0)) for k in norm.columns)
    rows.append({"구분": "📊 합산 포트폴리오", "투자금액($)": total_amt,
                 "CAGR(%)": round(cc * 100, 2), "MDD(%)": round(cm * 100, 2),
                 "최종자산($)": round(float(combined.iloc[-1]), 0)})
    return pd.DataFrame(rows), combined, (cc, cm)


# ══════════════════════════════════════════════════════════════
# 전략별 연/월 상세 + 다운로드
# ══════════════════════════════════════════════════════════════
def _scaled_curves(norm: pd.DataFrame, amounts: dict):
    """(라벨, 자산곡선 Series, 시작자본) 리스트. 금액 0 전략은 제외, 마지막에 합산."""
    items = []
    for k in norm.columns:
        amt = float(amounts.get(k, 0.0))
        if amt > 0:
            items.append((k, norm[k] * amt, amt))
    total = sum(float(amounts.get(k, 0.0)) for k in norm.columns)
    if total > 0:
        items.append(("📊 합산", combine(norm, amounts), total))
    return items


def annual_breakdown(norm: pd.DataFrame, amounts: dict):
    """연도별 전략별 수익률%/MDD% 와이드 테이블."""
    frames = []
    for label, eq, cap in _scaled_curves(norm, amounts):
        h = pd.DataFrame({"날짜": eq.index, "총자산($)": eq.values})
        a = compute_annual_stats(h, cap).rename(columns={
            "연간수익률(%)": f"{label} 수익률%", "MDD(%)": f"{label} MDD%"})
        frames.append(a.set_index("연도"))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=1).reset_index()


def monthly_for(norm: pd.DataFrame, amounts: dict, label: str):
    """특정 전략(또는 '📊 합산')의 월별 수익률 pivot."""
    for lab, eq, cap in _scaled_curves(norm, amounts):
        if lab == label:
            h = pd.DataFrame({"날짜": eq.index, "총자산($)": eq.values})
            return compute_monthly_pivot(h, cap)
    return pd.DataFrame()


def _sheet_name(s):
    """엑셀 시트명 정리 (특수문자·이모지 제거, 31자 제한)."""
    import re
    s = re.sub(r"[\[\]\:\*\?\/\\]", "", s)
    s = "".join(ch for ch in s if ord(ch) < 0x80 or 0xAC00 <= ord(ch) <= 0xD7A3)
    return (s.strip() or "sheet")[:31]


def build_excel(norm: pd.DataFrame, amounts: dict):
    """전 표를 다중 시트 엑셀(bytes)로. 시트: 전략별성과, 연도별, 월별_<전략>들."""
    import io
    buf = io.BytesIO()
    metrics_tbl, _, _ = portfolio_metrics_table(norm, amounts)
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        metrics_tbl.to_excel(w, sheet_name="전략별성과", index=False)
        annual_breakdown(norm, amounts).to_excel(w, sheet_name="연도별", index=False)
        for label, _, _ in _scaled_curves(norm, amounts):
            mp = monthly_for(norm, amounts, label)
            mp.to_excel(w, sheet_name=_sheet_name(f"월별_{label}"))
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════
# 역방향: 목표 CAGR/MDD 를 만족하는 배분 탐색 (벡터화)
# ══════════════════════════════════════════════════════════════
def _sample_simplex(n, k, bounds=None, seed=42):
    """합=1 가중치 n×k 샘플. bounds=[(lo,hi),...] (비율) 충족분만 반환."""
    rng = np.random.default_rng(seed)
    W = rng.dirichlet(np.ones(k), size=n)
    if bounds:
        lo = np.array([b[0] for b in bounds])
        hi = np.array([b[1] for b in bounds])
        mask = np.all((W >= lo - 1e-9) & (W <= hi + 1e-9), axis=1)
        W = W[mask]
    return W


def frontier(norm: pd.DataFrame, n_samples=20000, bounds=None):
    """가중치 샘플별 (cagr, mdd, weights) 계산. 비율에만 의존 (총액 무관)."""
    N = norm.values                      # (T, k) 시작 1.0
    idx = norm.index
    years = (idx[-1] - idx[0]).days / 365.25
    k = N.shape[1]
    W = _sample_simplex(n_samples, k, bounds)
    if len(W) == 0:
        return np.array([]), np.array([]), np.empty((0, k))
    C = N @ W.T                          # (T, m) 각 열 시작 1.0
    final = C[-1]
    cagr = final ** (1.0 / years) - 1.0 if years > 0 else np.zeros_like(final)
    peak = np.maximum.accumulate(C, axis=0)
    mdd = ((C - peak) / peak).min(axis=0)   # (m,) 음수
    return cagr, mdd, W


def recommend(norm, objective, target_cagr=None, target_mdd=None, bounds=None, n_samples=20000):
    """목표에 맞는 추천 가중치(비율) 반환.

    objective:
      'max_cagr_under_mdd' : MDD <= target_mdd 중 CAGR 최대
      'min_mdd_over_cagr'  : CAGR >= target_cagr 중 MDD 최소(절댓값)
      'both'               : 두 목표 동시 만족점(없으면 정규화 거리 최소)
    반환: dict(weights, cagr, mdd, feasible) 또는 None
    """
    cagr, mdd, W = frontier(norm, n_samples=n_samples, bounds=bounds)
    if len(W) == 0:
        return None
    if objective == "max_cagr_under_mdd":
        feas = mdd >= -abs(target_mdd)
        if not feas.any():
            i = np.argmax(mdd)          # 한도 못맞추면 MDD가 가장 얕은 것
            return dict(weights=W[i], cagr=cagr[i], mdd=mdd[i], feasible=False)
        cand = np.where(feas)[0]
        i = cand[np.argmax(cagr[cand])]
        return dict(weights=W[i], cagr=cagr[i], mdd=mdd[i], feasible=True)
    if objective == "min_mdd_over_cagr":
        feas = cagr >= target_cagr
        if not feas.any():
            i = np.argmax(cagr)
            return dict(weights=W[i], cagr=cagr[i], mdd=mdd[i], feasible=False)
        cand = np.where(feas)[0]
        i = cand[np.argmin(np.abs(mdd[cand]))]
        return dict(weights=W[i], cagr=cagr[i], mdd=mdd[i], feasible=True)
    # both
    feas = (cagr >= target_cagr) & (mdd >= -abs(target_mdd))
    if feas.any():
        cand = np.where(feas)[0]
        i = cand[np.argmax(cagr[cand])]
        return dict(weights=W[i], cagr=cagr[i], mdd=mdd[i], feasible=True)
    # 정규화 거리 최소 (목표 미달)
    dc = (target_cagr - cagr) / max(abs(target_cagr), 1e-6)
    dm = (-abs(target_mdd) - mdd) / max(abs(target_mdd), 1e-6)
    dist = np.sqrt(np.clip(dc, 0, None) ** 2 + np.clip(dm, 0, None) ** 2)
    i = np.argmin(dist)
    return dict(weights=W[i], cagr=cagr[i], mdd=mdd[i], feasible=False)


# ══════════════════════════════════════════════════════════════
# UI
# ══════════════════════════════════════════════════════════════
def render():
    st.title("🧩 포트폴리오 합산 시뮬레이터")
    st.caption("종가평균 · 표준편차 · DSS · 듀얼스나이퍼 4개 전략을 **독립 계좌**로 합산했을 때의 "
               "CAGR/MDD 와 연·월 수익을 계산하고, 목표치를 만족하는 투자금액 배분을 역산합니다.")

    # ── 사이드바: 전략 프리셋 + 기간 ──
    with st.sidebar:
        st.markdown("### ⚙️ 전략 프리셋 선택")
        av_key = st.selectbox("종가평균", list(AVG_PRESETS.keys()),
                              index=list(AVG_PRESETS).index(DEFAULT_PRESET["종가평균"]))
        sd_key = st.selectbox("표준편차", list(STDEV_PRESETS.keys()),
                              index=list(STDEV_PRESETS).index(DEFAULT_PRESET["표준편차"]))
        dss_key = st.selectbox("DSS", list(DSS_PRESETS.keys()),
                               index=list(DSS_PRESETS).index(DEFAULT_PRESET["DSS"]))
        dual_key = st.selectbox("듀얼스나이퍼", list(DUAL_PRESETS.keys()),
                                index=list(DUAL_PRESETS).index(DEFAULT_PRESET["듀얼스나이퍼"]))
        dual_mode = st.selectbox("듀얼 모드 소스", DUAL_MODE_OPTS, index=0)

        st.markdown("### 📅 백테스트 기간")
        c1, c2 = st.columns(2)
        start_date = c1.date_input("시작", datetime(2016, 1, 4).date())
        end_date = c2.date_input("종료", datetime.today().date())
        st.caption("4개 전략 모두 유효한 구간은 듀얼 원전략 모드(2016~) 기준입니다.")

    # ── 백테스트 실행 (캐시) ──
    try:
        norm = compute_norm_matrix(av_key, sd_key, dss_key, dual_key, dual_mode,
                                   start_date, end_date)
    except Exception as e:
        st.error(f"⚠️ 백테스트 실행 실패: {e}")
        import traceback
        st.code(traceback.format_exc())
        return

    if norm is None or norm.empty or len(norm) < 2:
        st.warning("공통 거래일이 부족합니다. 기간을 넓혀주세요.")
        return

    st.success(f"✅ 공통 백테스트 구간: **{norm.index[0].date()} ~ {norm.index[-1].date()}** "
               f"({len(norm):,} 거래일)")

    tab_fwd, tab_rev = st.tabs(["➡️ 순방향: 금액 → CAGR/MDD", "🎯 역방향: 목표 → 금액 배분"])

    with tab_fwd:
        _render_forward(norm)
    with tab_rev:
        _render_reverse(norm)


def _render_forward(norm):
    st.markdown("#### 전략별 투자금액 입력")
    cols = st.columns(4)
    amounts = {}
    for i, k in enumerate(norm.columns):
        amounts[k] = cols[i].number_input(f"{k} ($)", min_value=0.0,
                                          value=DEFAULT_AMOUNT.get(k, 10000.0),
                                          step=1000.0, key=f"pf_amt_{k}")
    total = sum(amounts.values())
    if total <= 0:
        st.info("투자금액을 입력하세요.")
        return

    table, combined, (cc, cm) = portfolio_metrics_table(norm, amounts)

    # ── 핵심 지표 ──
    final = float(combined.iloc[-1])
    years = (norm.index[-1] - norm.index[0]).days / 365.25
    sharpe, sortino = compute_sharpe_sortino(combined.values)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("총 투자금", f"${total:,.0f}")
    m2.metric("합산 CAGR", f"{cc*100:.2f}%")
    m3.metric("합산 MDD", f"{cm*100:.2f}%")
    m4.metric("최종 자산", f"${final:,.0f}", f"{(final/total-1)*100:+.1f}%")

    # ── 분산 효과 ──
    wavg_mdd = sum(abs(table.loc[table["구분"] == k, "MDD(%)"].iloc[0]) * amounts[k]
                   for k in norm.columns) / total
    diversify = wavg_mdd - abs(cm * 100)
    st.caption(f"📉 개별 MDD 가중평균 **{wavg_mdd:.2f}%** → 합산 MDD **{abs(cm*100):.2f}%** "
               f"(분산효과 {diversify:+.2f}%p · Calmar {cc/abs(cm) if cm else 0:.2f} · "
               f"Sharpe {sharpe:.2f} · Sortino {sortino:.2f})")

    st.dataframe(table.style.format({
        "투자금액($)": "${:,.0f}", "CAGR(%)": "{:.2f}", "MDD(%)": "{:.2f}",
        "최종자산($)": "${:,.0f}"}), use_container_width=True, hide_index=True)

    # ── 자산곡선 차트 ──
    fig = go.Figure()
    for k in norm.columns:
        if amounts[k] > 0:
            fig.add_trace(go.Scatter(x=norm.index, y=norm[k] * amounts[k],
                                     name=k, mode="lines", line=dict(width=1), opacity=0.55))
    fig.add_trace(go.Scatter(x=combined.index, y=combined.values, name="📊 합산",
                             mode="lines", line=dict(width=3, color="#111")))
    fig.update_layout(height=420, hovermode="x unified", yaxis_title="자산($)",
                      legend=dict(orientation="h", y=1.02, yanchor="bottom"),
                      margin=dict(t=30, b=10))
    st.plotly_chart(fig, use_container_width=True)

    # ── 합산 연/월 수익 ──
    hist = pd.DataFrame({"날짜": combined.index, "총자산($)": combined.values})
    cA, cB = st.columns([1, 1.4])
    with cA:
        st.markdown("##### 📅 합산 연도별 수익률 / MDD")
        annual = compute_annual_stats(hist, total)
        st.dataframe(annual.style.format({"연간수익률(%)": "{:.2f}", "MDD(%)": "{:.2f}"}),
                     use_container_width=True, hide_index=True)
    with cB:
        st.markdown("##### 🗓️ 합산 월별 수익률 (%)")
        pivot = compute_monthly_pivot(hist, total)
        st.dataframe(pivot.style.format("{:.1f}").background_gradient(cmap="RdYlGn", axis=None),
                     use_container_width=True)

    # ── 합산 그래프 (연도별 막대 + 월별 히트맵) ──
    gA, gB = st.columns([1, 1.2])
    with gA:
        st.markdown("##### 📊 합산 연도별 수익률 / MDD")
        ya = annual["연도"].astype(str)
        fig_y = go.Figure()
        fig_y.add_bar(x=ya, y=annual["연간수익률(%)"], name="연간수익률(%)",
                      marker_color=["#2E7D32" if v >= 0 else "#C62828" for v in annual["연간수익률(%)"]],
                      text=annual["연간수익률(%)"].round(1), textposition="outside")
        fig_y.add_bar(x=ya, y=annual["MDD(%)"], name="MDD(%)", marker_color="#EF9A9A")
        fig_y.update_layout(barmode="group", height=360, hovermode="x unified",
                            yaxis_title="%", legend=dict(orientation="h", y=1.02, yanchor="bottom"),
                            margin=dict(t=30, b=10))
        st.plotly_chart(fig_y, use_container_width=True)
    with gB:
        st.markdown("##### 🌡️ 합산 월별 수익률 히트맵 (%)")
        fig_m = go.Figure(go.Heatmap(
            z=pivot.values, x=list(pivot.columns), y=[str(i) for i in pivot.index],
            colorscale="RdYlGn", zmid=0, text=pivot.values,
            texttemplate="%{text:.1f}", textfont=dict(size=10),
            colorbar=dict(title="%")))
        fig_m.update_layout(height=360, yaxis=dict(autorange="reversed", title="연도"),
                            margin=dict(t=30, b=10))
        st.plotly_chart(fig_m, use_container_width=True)

    # ── 전략별 상세 (연도별 수익률/MDD, 월별) ──
    st.markdown("---")
    st.markdown("#### 📂 전략별 상세 (각 매매법 연도/월별)")

    ann_bd = annual_breakdown(norm, amounts)
    st.markdown("##### 📅 연도별 — 전략별 수익률% / MDD%")
    _fmt = {c: "{:.2f}" for c in ann_bd.columns if c != "연도"}
    st.dataframe(ann_bd.style.format(_fmt), use_container_width=True, hide_index=True)

    # ── 전략별 연도 수익률/MDD 비교 막대 ──
    _strats = [lab for lab, _, _ in _scaled_curves(norm, amounts)]
    _yx = ann_bd["연도"].astype(str)
    cR, cM = st.columns(2)
    with cR:
        st.markdown("###### 📈 전략별 연간 수익률(%) 비교")
        fig_r = go.Figure()
        for s in _strats:
            fig_r.add_bar(x=_yx, y=ann_bd[f"{s} 수익률%"], name=s)
        fig_r.update_layout(barmode="group", height=340, hovermode="x unified",
                            legend=dict(orientation="h", y=1.02, yanchor="bottom"),
                            margin=dict(t=30, b=10))
        st.plotly_chart(fig_r, use_container_width=True)
    with cM:
        st.markdown("###### 📉 전략별 연간 MDD(%) 비교")
        fig_d = go.Figure()
        for s in _strats:
            fig_d.add_bar(x=_yx, y=ann_bd[f"{s} MDD%"], name=s)
        fig_d.update_layout(barmode="group", height=340, hovermode="x unified",
                            legend=dict(orientation="h", y=1.02, yanchor="bottom"),
                            margin=dict(t=30, b=10))
        st.plotly_chart(fig_d, use_container_width=True)

    st.markdown("##### 🗓️ 월별 — 전략 선택")
    labels = [lab for lab, _, _ in _scaled_curves(norm, amounts)]
    sel = st.selectbox("전략", labels, index=len(labels) - 1, key="pf_monthly_sel")
    mp = monthly_for(norm, amounts, sel)
    mcol1, mcol2 = st.columns([1, 1])
    with mcol1:
        st.dataframe(mp.style.format("{:.1f}").background_gradient(cmap="RdYlGn", axis=None),
                     use_container_width=True)
    with mcol2:
        if not mp.empty:
            fig_ms = go.Figure(go.Heatmap(
                z=mp.values, x=list(mp.columns), y=[str(i) for i in mp.index],
                colorscale="RdYlGn", zmid=0, text=mp.values,
                texttemplate="%{text:.1f}", textfont=dict(size=10), colorbar=dict(title="%")))
            fig_ms.update_layout(height=360, yaxis=dict(autorange="reversed", title="연도"),
                                 title=f"{sel} 월별 히트맵", margin=dict(t=40, b=10))
            st.plotly_chart(fig_ms, use_container_width=True)

    # ── 다운로드 (CSV / Excel) ──
    st.markdown("##### 📥 다운로드")
    _tag = norm.index[-1].strftime("%Y%m%d")
    d1, d2, d3, d4 = st.columns(4)
    d1.download_button("연도별 CSV", ann_bd.to_csv(index=False).encode("utf-8-sig"),
                       file_name=f"portfolio_annual_{_tag}.csv", mime="text/csv",
                       use_container_width=True)
    d2.download_button(f"월별({sel}) CSV", mp.to_csv().encode("utf-8-sig"),
                       file_name=f"portfolio_monthly_{_sheet_name(sel)}_{_tag}.csv", mime="text/csv",
                       use_container_width=True)
    d3.download_button("전략별성과 CSV", table.to_csv(index=False).encode("utf-8-sig"),
                       file_name=f"portfolio_metrics_{_tag}.csv", mime="text/csv",
                       use_container_width=True)
    try:
        xls = build_excel(norm, amounts)
        d4.download_button("📊 전체 Excel", xls, file_name=f"portfolio_{_tag}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)
    except Exception as _xe:
        d4.caption(f"Excel 생성 실패: {_xe}")


def _render_reverse(norm):
    st.markdown("#### 목표 지표 → 추천 금액 배분")
    st.caption("CAGR/MDD(%)는 **금액의 비율**에만 의존하므로, 먼저 비율을 찾고 총 투자금을 곱해 금액을 산출합니다.")

    c1, c2, c3 = st.columns(3)
    objective = c1.selectbox("최적화 목표", [
        "MDD 한도 내 CAGR 최대화", "목표 CAGR 이상에서 MDD 최소화", "목표 CAGR·MDD 동시 만족"])
    target_cagr = c2.number_input("목표 CAGR (%)", value=50.0, step=5.0) / 100
    target_mdd = c3.number_input("허용 MDD (%, 절댓값)", value=30.0, step=5.0)

    total_budget = st.number_input("총 투자금 ($)", min_value=0.0, value=40000.0, step=5000.0)

    with st.expander("전략별 비중 제약 (선택)"):
        bcols = st.columns(4)
        bounds = []
        for i, k in enumerate(norm.columns):
            lo = bcols[i].number_input(f"{k} 최소%", 0, 100, 0, 5, key=f"pf_lo_{k}") / 100
            hi = bcols[i].number_input(f"{k} 최대%", 0, 100, 100, 5, key=f"pf_hi_{k}") / 100
            bounds.append((lo, hi))
        use_bounds = st.checkbox("비중 제약 적용", value=False)

    obj_map = {"MDD 한도 내 CAGR 최대화": "max_cagr_under_mdd",
               "목표 CAGR 이상에서 MDD 최소화": "min_mdd_over_cagr",
               "목표 CAGR·MDD 동시 만족": "both"}

    if st.button("🔍 추천 배분 탐색", type="primary"):
        rec = recommend(norm, obj_map[objective],
                        target_cagr=target_cagr, target_mdd=target_mdd,
                        bounds=bounds if use_bounds else None, n_samples=30000)
        if rec is None:
            st.error("제약을 만족하는 배분을 찾지 못했습니다. 제약을 완화하세요.")
            return
        st.session_state["pf_rec"] = rec

    rec = st.session_state.get("pf_rec")
    if not rec:
        st.info("목표를 설정하고 '추천 배분 탐색'을 누르세요.")
        return

    w = rec["weights"]
    if not rec["feasible"]:
        st.warning("⚠️ 목표를 완전히 만족하는 배분이 없어 **가장 근접한** 배분을 제시합니다.")

    r1, r2, r3 = st.columns(3)
    r1.metric("예상 CAGR", f"{rec['cagr']*100:.2f}%")
    r2.metric("예상 MDD", f"{rec['mdd']*100:.2f}%")
    r3.metric("Calmar", f"{rec['cagr']/abs(rec['mdd']) if rec['mdd'] else 0:.2f}")

    alloc = pd.DataFrame({
        "전략": list(norm.columns),
        "비중(%)": [round(x * 100, 1) for x in w],
        "추천 금액($)": [round(x * total_budget, 0) for x in w],
    })
    st.dataframe(alloc.style.format({"비중(%)": "{:.1f}", "추천 금액($)": "${:,.0f}"}),
                 use_container_width=True, hide_index=True)

    # ── 효율적 프론티어 ──
    cagr_all, mdd_all, _ = frontier(norm, n_samples=4000,
                                    bounds=(bounds if use_bounds else None))
    if len(cagr_all):
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=-mdd_all * 100, y=cagr_all * 100, mode="markers",
                                 marker=dict(size=4, color=cagr_all / np.where(mdd_all == 0, 1, -mdd_all),
                                             colorscale="Viridis", showscale=True,
                                             colorbar=dict(title="Calmar")),
                                 name="배분 조합", opacity=0.5))
        fig.add_trace(go.Scatter(x=[-rec["mdd"] * 100], y=[rec["cagr"] * 100], mode="markers",
                                 marker=dict(size=16, color="red", symbol="star"), name="추천"))
        fig.add_vline(x=target_mdd, line_dash="dash", line_color="gray")
        fig.add_hline(y=target_cagr * 100, line_dash="dash", line_color="gray")
        fig.update_layout(height=420, xaxis_title="MDD (%, 작을수록 좋음)", yaxis_title="CAGR (%)",
                          title="효율적 프론티어 (점=배분 조합 / ★=추천)", margin=dict(t=40))
        st.plotly_chart(fig, use_container_width=True)

    st.caption("※ 선형 스케일 가정: 각 전략 자산곡선이 투자금액에 비례한다고 보고 합산. "
               "정수주 체결 오차는 무시됩니다.")
