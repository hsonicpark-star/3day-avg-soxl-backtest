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


_MONTH_ORDER = ["1월", "2월", "3월", "4월", "5월", "6월", "7월", "8월",
                "9월", "10월", "11월", "12월"]


def monthly_breakdown(norm: pd.DataFrame, amounts: dict):
    """월별 전략별 수익률% 와이드 테이블 (행=연도·월, 열=전략 + 합산)."""
    labels = []
    frames = []
    for label, eq, cap in _scaled_curves(norm, amounts):
        labels.append(label)
        h = pd.DataFrame({"날짜": eq.index, "총자산($)": eq.values})
        piv = compute_monthly_pivot(h, cap)   # index=Year, cols=월
        long = piv.reset_index().melt(id_vars="Year", var_name="월", value_name=label)
        frames.append(long.set_index(["Year", "월"]))
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, axis=1).reset_index()
    out = out.dropna(subset=labels, how="all")
    out["__m"] = out["월"].map({m: i for i, m in enumerate(_MONTH_ORDER, 1)})
    out = out.sort_values(["Year", "__m"]).drop(columns="__m")
    return out.rename(columns={"Year": "연도"}).reset_index(drop=True)


def monthly_for(norm: pd.DataFrame, amounts: dict, label: str):
    """특정 전략(또는 '📊 합산')의 월별 수익률 pivot."""
    for lab, eq, cap in _scaled_curves(norm, amounts):
        if lab == label:
            h = pd.DataFrame({"날짜": eq.index, "총자산($)": eq.values})
            return compute_monthly_pivot(h, cap)
    return pd.DataFrame()


def _bg_pct(v, lo=-20.0, hi=20.0):
    """수익률(%) → RdYlGn 배경색 (matplotlib 비의존). 음수=빨강, 0=노랑, 양수=초록."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    t = (float(v) - lo) / (hi - lo)
    t = max(0.0, min(1.0, t))
    if t < 0.5:
        f = t / 0.5
        r, g, b = 215 + (255 - 215) * f, 48 + (255 - 48) * f, 39 + (191 - 39) * f
    else:
        f = (t - 0.5) / 0.5
        r, g, b = 255 + (26 - 255) * f, 255 + (152 - 255) * f, 191 + (80 - 191) * f
    return f"background-color: rgb({int(r)},{int(g)},{int(b)}); color:#111"


def _style_pct(df):
    """월별 수익률 DataFrame → 색상 입힌 Styler (matplotlib 불필요)."""
    sty = df.style.format("{:.1f}")
    try:
        return sty.map(_bg_pct)            # pandas >= 2.1
    except AttributeError:
        return sty.applymap(_bg_pct)       # 구버전 fallback


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
        monthly_breakdown(norm, amounts).to_excel(w, sheet_name="월별전체", index=False)
        for label, _, _ in _scaled_curves(norm, amounts):
            mp = monthly_for(norm, amounts, label)
            mp.to_excel(w, sheet_name=_sheet_name(f"월별_{label}"))
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════
# 역방향: 목표 CAGR/MDD 를 만족하는 배분 탐색 (벡터화)
# ══════════════════════════════════════════════════════════════
def _sample_simplex(n, k, bounds=None, seed=42):
    """합=1 가중치 n×k 샘플.

    bounds=[(lo,hi),...] (비율). 하한은 'Dirichlet 여유분 분배'로 수학적으로 보장하고
    상한만 기각필터 → 빡빡한 제약에서도 표본이 잘 확보됨. 불가능 제약이면 빈 배열.
    """
    rng = np.random.default_rng(seed)
    if not bounds:
        return rng.dirichlet(np.ones(k), size=n)
    lo = np.array([b[0] for b in bounds], dtype=float)
    hi = np.array([b[1] for b in bounds], dtype=float)
    if lo.sum() > 1 + 1e-9 or hi.sum() < 1 - 1e-9:
        return np.empty((0, k))           # 합=100% 불가능
    residual = 1.0 - lo.sum()
    if residual <= 1e-12:
        W = np.tile(lo, (n, 1))           # 하한 합이 100% → 고정
    else:
        W = lo + rng.dirichlet(np.ones(k), size=n) * residual
    mask = np.all(W <= hi + 1e-9, axis=1)   # 상한 필터
    return W[mask]


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

    tab_fwd, tab_rev, tab_cmp = st.tabs([
        "➡️ 순방향: 금액 → CAGR/MDD", "🎯 역방향: 목표 → 금액 배분",
        "🔬 비교 분석: 합산 vs 단일 전략"])

    with tab_fwd:
        _render_forward(norm)
    with tab_rev:
        _render_reverse(norm)
    with tab_cmp:
        _render_compare(norm)


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
        st.dataframe(_style_pct(pivot), use_container_width=True)

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

    # ── 월별 — 전략별 한눈에 (와이드 통합 표) ──
    st.markdown("##### 🗓️ 월별 — 전략별 한눈에 (수익률 %)")
    mon_bd = monthly_breakdown(norm, amounts)
    _mvcols = [c for c in mon_bd.columns if c not in ("연도", "월")]
    _msty = mon_bd.style.format({c: "{:.1f}" for c in _mvcols})
    try:
        _msty = _msty.map(_bg_pct, subset=_mvcols)
    except AttributeError:
        _msty = _msty.applymap(_bg_pct, subset=_mvcols)
    st.dataframe(_msty, use_container_width=True, hide_index=True, height=400)

    st.markdown("##### 🗓️ 월별 — 전략 선택 (히트맵)")
    labels = [lab for lab, _, _ in _scaled_curves(norm, amounts)]
    sel = st.selectbox("전략", labels, index=len(labels) - 1, key="pf_monthly_sel")
    mp = monthly_for(norm, amounts, sel)
    mcol1, mcol2 = st.columns([1, 1])
    with mcol1:
        st.dataframe(_style_pct(mp), use_container_width=True)
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
    d1.download_button("연도별 전략별 CSV", ann_bd.to_csv(index=False).encode("utf-8-sig"),
                       file_name=f"portfolio_annual_{_tag}.csv", mime="text/csv",
                       use_container_width=True)
    d2.download_button("월별 전략별 CSV", mon_bd.to_csv(index=False).encode("utf-8-sig"),
                       file_name=f"portfolio_monthly_all_{_tag}.csv", mime="text/csv",
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


def _monthly_ret_series(curve):
    """일별 자산곡선 → 월별 수익률(%) Series (인덱스=월말)."""
    m = curve.resample("ME").last()
    return m.pct_change().dropna() * 100


def _annual_of(curve):
    """per-dollar 자산곡선 → 연도별 수익률/MDD 표."""
    h = pd.DataFrame({"날짜": curve.index, "총자산($)": curve.values})
    return compute_annual_stats(h, float(curve.iloc[0]))


def _render_compare(norm):
    st.markdown("#### 🔬 합산 포트폴리오 vs 단일 전략 비교")
    st.caption("순방향 탭의 투자금액 배분을 그대로 사용합니다. 단일 전략이 합산을 **보완**하는지, "
               "아니면 **함께 등락**하는지 연·월별 성과와 상관관계로 확인합니다.")

    amounts = {k: float(st.session_state.get(f"pf_amt_{k}", DEFAULT_AMOUNT.get(k, 10000.0)))
               for k in norm.columns}
    total = sum(amounts.values())
    if total <= 0:
        st.info("순방향 탭에서 투자금액을 입력하세요.")
        return

    sel = st.selectbox("비교할 단일 전략", list(norm.columns), key="pf_cmp_sel")

    # per-dollar (시작=1.0) 곡선 — 수익률 비교는 금액 무관
    comb_pd = combine(norm, amounts) / total
    sel_pd = norm[sel]

    c_cb, m_cb, _ = _metrics(comb_pd.values, comb_pd.index)
    c_sel, m_sel, _ = _metrics(sel_pd.values, sel_pd.index)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric(f"{sel} CAGR", f"{c_sel*100:.2f}%", f"{(c_sel-c_cb)*100:+.2f}%p vs 합산")
    k2.metric("합산 CAGR", f"{c_cb*100:.2f}%")
    k3.metric(f"{sel} MDD", f"{m_sel*100:.2f}%", f"{(abs(m_sel)-abs(m_cb))*-100:+.2f}%p vs 합산",
              delta_color="inverse")
    k4.metric("합산 MDD", f"{m_cb*100:.2f}%")

    # ── 1) 연도별 비교 ──
    st.markdown("##### 📅 연도별 수익률 비교 (선택 전략 vs 합산)")
    a_sel = _annual_of(sel_pd).rename(columns={"연간수익률(%)": "선택 수익률%", "MDD(%)": "선택 MDD%"})
    a_cb = _annual_of(comb_pd).rename(columns={"연간수익률(%)": "합산 수익률%", "MDD(%)": "합산 MDD%"})
    ann = a_sel.merge(a_cb, on="연도")
    ann["수익 차이(선택-합산)%"] = (ann["선택 수익률%"] - ann["합산 수익률%"]).round(2)
    yx = ann["연도"].astype(str)

    g1, g2 = st.columns([1.1, 1])
    with g1:
        fig = go.Figure()
        fig.add_bar(x=yx, y=ann["선택 수익률%"], name=sel, marker_color="#1565C0")
        fig.add_bar(x=yx, y=ann["합산 수익률%"], name="합산", marker_color="#9E9E9E")
        fig.update_layout(barmode="group", height=340, hovermode="x unified", yaxis_title="%",
                          legend=dict(orientation="h", y=1.02, yanchor="bottom"), margin=dict(t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)
    with g2:
        figd = go.Figure()
        figd.add_bar(x=yx, y=ann["수익 차이(선택-합산)%"],
                     marker_color=["#2E7D32" if v >= 0 else "#C62828" for v in ann["수익 차이(선택-합산)%"]],
                     text=ann["수익 차이(선택-합산)%"], textposition="outside")
        figd.update_layout(height=340, title="차이 (양수=선택 우위 / 음수=합산 우위)",
                           yaxis_title="%p", margin=dict(t=40, b=10))
        st.plotly_chart(figd, use_container_width=True)

    st.dataframe(ann.style.format({c: "{:.2f}" for c in ann.columns if c != "연도"}),
                 use_container_width=True, hide_index=True)

    # ── 2) 월별 차이 히트맵 ──
    st.markdown("##### 🌡️ 월별 수익률 차이 히트맵 (선택 − 합산)")
    st.caption("초록 = 그 달에 선택 전략이 합산보다 우수(보완 효과) / 빨강 = 합산이 더 우수")
    mp_sel = monthly_for(norm, amounts, sel)
    mp_cb = monthly_for(norm, amounts, "📊 합산")
    common_yr = mp_sel.index.intersection(mp_cb.index)
    diff = (mp_sel.loc[common_yr] - mp_cb.loc[common_yr])
    fig_h = go.Figure(go.Heatmap(
        z=diff.values, x=list(diff.columns), y=[str(i) for i in diff.index],
        colorscale="RdYlGn", zmid=0, text=diff.values, texttemplate="%{text:.1f}",
        textfont=dict(size=10), colorbar=dict(title="%p")))
    fig_h.update_layout(height=360, yaxis=dict(autorange="reversed", title="연도"), margin=dict(t=20, b=10))
    st.plotly_chart(fig_h, use_container_width=True)

    # ── 3) 상관관계 분석 ──
    st.markdown("##### 🔗 상관관계 분석 (상호 보완 vs 동반 등락)")
    rets = norm.pct_change().dropna()
    comb_ret = comb_pd.pct_change().reindex(rets.index)
    rets_all = rets.copy()
    rets_all["📊 합산"] = comb_ret
    corr_d = rets_all.corr()

    # 월별 상관
    mser = {k: _monthly_ret_series(norm[k]) for k in norm.columns}
    mser["📊 합산"] = _monthly_ret_series(comb_pd)
    corr_m = pd.DataFrame(mser).corr()

    cc1, cc2 = st.columns(2)
    for col, corr, title in [(cc1, corr_d, "일간 수익률 상관계수"), (cc2, corr_m, "월간 수익률 상관계수")]:
        with col:
            fig_c = go.Figure(go.Heatmap(
                z=corr.values, x=list(corr.columns), y=list(corr.index),
                colorscale="RdBu_r", zmid=0, zmin=-1, zmax=1,
                text=corr.round(2).values, texttemplate="%{text}", textfont=dict(size=11),
                colorbar=dict(title="r")))
            fig_c.update_layout(height=360, title=title, yaxis=dict(autorange="reversed"),
                                margin=dict(t=40, b=10))
            st.plotly_chart(fig_c, use_container_width=True)

    avg_off = (corr_d.values[~np.eye(len(corr_d), dtype=bool)]).mean()
    st.caption(f"📌 전략 간 평균 일간 상관 **{avg_off:.2f}** — "
               + ("1에 가까울수록 **다같이 등락**(분산효과 작음), " if avg_off > 0.7 else "")
               + "0에 가깝거나 낮을수록 **상호 보완적**입니다. (모두 SOXL 기반이라 상관은 대체로 높은 편)")

    # ── 4) 하방 보완 지표 + 산점도 + 롤링 상관 ──
    st.markdown("##### 🛡️ 하방 보완 & 동조성")
    sm = _monthly_ret_series(sel_pd)
    cm = _monthly_ret_series(comb_pd)
    idx = sm.index.intersection(cm.index)
    sm, cm = sm.loc[idx], cm.loc[idx]
    down = cm < 0
    help_rate = float((sm[down] > cm[down]).mean()) * 100 if down.any() else 0.0
    cushion = float((sm[down] > 0).mean()) * 100 if down.any() else 0.0

    d1, d2, d3 = st.columns(3)
    d1.metric("합산 손실月에 선택이 더 나은 비율", f"{help_rate:.0f}%",
              help="합산이 마이너스인 달 중, 선택 전략이 합산보다 높았던 비율")
    d2.metric("합산 손실月에 선택이 플러스인 비율", f"{cushion:.0f}%",
              help="합산이 마이너스인 달 중, 선택 전략은 오히려 수익이었던 비율 (방어력)")
    d3.metric("월간 상관계수", f"{sm.corr(cm):.2f}")

    s1, s2 = st.columns(2)
    with s1:
        fig_s = go.Figure(go.Scatter(x=cm, y=sm, mode="markers",
                                     marker=dict(size=6, color=np.where(down, "#C62828", "#2E7D32"), opacity=0.6)))
        _lim = float(max(abs(cm).max(), abs(sm).max())) * 1.05
        fig_s.add_trace(go.Scatter(x=[-_lim, _lim], y=[-_lim, _lim], mode="lines",
                                   line=dict(dash="dash", color="gray"), showlegend=False))
        fig_s.update_layout(height=340, title=f"월간 수익률 산점도 ({sel} vs 합산)",
                            xaxis_title="합산 월수익률(%)", yaxis_title=f"{sel} 월수익률(%)", margin=dict(t=40))
        st.plotly_chart(fig_s, use_container_width=True)
    with s2:
        roll = rets[sel].rolling(126).corr(comb_ret).dropna()
        fig_r = go.Figure(go.Scatter(x=roll.index, y=roll.values, mode="lines", line=dict(color="#6A1B9A")))
        fig_r.update_layout(height=340, title=f"6개월 롤링 상관 ({sel} vs 합산)",
                            yaxis_title="r", yaxis_range=[-1, 1], margin=dict(t=40))
        fig_r.add_hline(y=avg_off, line_dash="dot", line_color="gray")
        st.plotly_chart(fig_r, use_container_width=True)

    st.caption("롤링 상관이 낮아지는 구간 = 그 시기에 선택 전략이 합산과 **다르게 움직여 분산에 기여**한 시점입니다.")


def _render_reverse(norm):
    st.markdown("#### 목표 지표 → 추천 금액 배분")
    st.caption("CAGR/MDD(%)는 **금액의 비율**에만 의존하므로, 먼저 비율을 찾고 총 투자금을 곱해 금액을 산출합니다.")

    c1, c2, c3 = st.columns(3)
    objective = c1.selectbox("최적화 목표", [
        "MDD 한도 내 CAGR 최대화", "목표 CAGR 이상에서 MDD 최소화", "목표 CAGR·MDD 동시 만족"])
    target_cagr = c2.number_input("목표 CAGR (%)", value=50.0, step=5.0) / 100
    target_mdd = c3.number_input("허용 MDD (%, 절댓값)", value=30.0, step=5.0)

    total_budget = st.number_input("총 투자금 ($)", min_value=0.0, value=40000.0, step=5000.0)

    with st.expander("전략별 비중 제약 (최소%·최대% — 항상 적용됨)", expanded=True):
        st.caption("최소/최대 비중을 지정하면 추천 탐색에 바로 반영됩니다. 기본값(0~100%)은 제약 없음과 동일.")
        bcols = st.columns(4)
        bounds = []
        for i, k in enumerate(norm.columns):
            lo = bcols[i].number_input(f"{k} 최소%", 0, 100, 0, 5, key=f"pf_lo_{k}") / 100
            hi = bcols[i].number_input(f"{k} 최대%", 0, 100, 100, 5, key=f"pf_hi_{k}") / 100
            bounds.append((lo, hi))
        _lo_sum = sum(b[0] for b in bounds) * 100
        _hi_sum = sum(b[1] for b in bounds) * 100
        if _lo_sum > 100:
            st.warning(f"⚠️ 최소% 합계가 {_lo_sum:.0f}% — 100%를 초과해 불가능합니다. 최소%를 낮추세요.")
        if _hi_sum < 100:
            st.warning(f"⚠️ 최대% 합계가 {_hi_sum:.0f}% — 100%에 못 미쳐 불가능합니다. 최대%를 높이세요.")
        for i, k in enumerate(norm.columns):
            if bounds[i][0] > bounds[i][1]:
                st.warning(f"⚠️ {k}: 최소%가 최대%보다 큽니다.")

    obj_map = {"MDD 한도 내 CAGR 최대화": "max_cagr_under_mdd",
               "목표 CAGR 이상에서 MDD 최소화": "min_mdd_over_cagr",
               "목표 CAGR·MDD 동시 만족": "both"}

    if st.button("🔍 추천 배분 탐색", type="primary"):
        rec = recommend(norm, obj_map[objective],
                        target_cagr=target_cagr, target_mdd=target_mdd / 100.0,
                        bounds=bounds, n_samples=30000)
        if rec is None:
            st.error("제약(최소/최대 비중)을 만족하는 배분을 찾지 못했습니다. 비중 제약을 완화하세요.")
            st.session_state.pop("pf_rec", None)
            return
        st.session_state["pf_rec"] = rec
        st.session_state["pf_rec_bounds"] = bounds

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

    # ── 효율적 프론티어 (추천 탐색에 쓴 제약 그대로) ──
    cagr_all, mdd_all, _ = frontier(norm, n_samples=4000,
                                    bounds=st.session_state.get("pf_rec_bounds"))
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
