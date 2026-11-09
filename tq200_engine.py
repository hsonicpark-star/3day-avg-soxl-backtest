# -*- coding: utf-8 -*-
"""TQ200 (TQQQ 200일선 이격도 매매) — 백테스트 엔진

원본: 14.TQ200/TQQQ200일선.xlsx (최근로그 · 사이클 시트) 규칙 1:1 이식.

규칙 (모든 판단은 종가 확정 후, 체결은 다음 거래일 시가)
----------------------------------------------------------
- 200MA   = 종가 200일 단순이동평균
- 이격도  = 종가 / 200MA − 1
- Slope   = (200MA − 5거래일 전 200MA) / 5      (USD/day)
- 매수 신호 (무포지션): buy_lo < 이격도 < buy_hi (1% 초과 10% 미만)  AND  Slope > 0
                        → 다음 거래일 **시가 전량 매수** (MOO)
- 매도 신호 (보유 중) : 이격도 < exit_disp (−1% 미만으로 이탈)
                        또는 이격도 > tp_disp (55% 초과 과열 익절)
                        → 다음 거래일 **시가 전량 매도** (MOO)
- 경계값은 모두 '초과/미만' (정확히 1.00%·−1.00%·55.00% 는 신호 아님).
  원본 엑셀 구간에는 경계에 정확히 걸린 날이 없어 '이상/이하'와 결과가 같다.
- 시가 매도한 날 종가에 매수 조건이 맞으면 그날 바로 매수 신호 (다음날 재진입)
- 시가 매수한 날 종가가 −1% 를 이탈하면 그날 바로 매도 신호 (다음날 청산)

Streamlit 비의존 — 순수 pandas/numpy. 검증: 14.TQ200/verify_tq200.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

DEFAULTS = {
    "ma_window": 200,      # 이동평균 기간 (거래일)
    "slope_window": 5,     # 기울기 측정 기간 (거래일)
    "buy_lo": 1.0,         # 매수 이격도 하한 (%)
    "buy_hi": 10.0,        # 매수 이격도 상한 (%)
    "exit_disp": -1.0,     # 청산 이격도 (%) — 미만으로 이탈 시 매도
    "tp_disp": 55.0,       # 과열 익절 이격도 (%) — 초과 시 매도
    "use_slope": True,     # 200MA 기울기 > 0 필터 사용
    "use_tp": True,        # 과열 익절 사용
    "fee": 0.0,            # 수수료+슬리피지 (%, 편도)
}

EXIT_BREAK = "이격도 이탈"
EXIT_TP = "이격도 과열"


# ──────────────────────────────────────────────
# 지표
# ──────────────────────────────────────────────
def compute_indicators(df: pd.DataFrame, ma_window: int = 200,
                       slope_window: int = 5) -> pd.DataFrame:
    """OHLC(최소 Open/Close) → MA·이격도·Slope 컬럼 추가본 반환.

    워밍업(ma_window + slope_window 거래일)이 지나기 전 행은 NaN.
    """
    out = df[["Open", "Close"]].astype(float).copy()
    out["MA"] = out["Close"].rolling(int(ma_window)).mean()
    out["이격도"] = out["Close"] / out["MA"] - 1.0
    out["Slope"] = (out["MA"] - out["MA"].shift(int(slope_window))) \
        / float(slope_window)
    return out


def trigger_closes(closes, ma_window: int, level: float) -> float:
    """다음 거래일 종가가 얼마면 이격도가 정확히 `level` 이 되는가.

    내일 종가 C 가 들어오면 MA' = (최근 (n−1)일 합 + C) / n 이므로
        C / MA' − 1 = level  ⇒  C = (1+level)·S / (n − (1+level))
    (S = 내일 기준 최근 n−1일 종가 합). 데이터 부족 시 NaN.
    """
    n = int(ma_window)
    arr = np.asarray(closes, dtype=float)
    if len(arr) < n - 1:
        return float("nan")
    s = float(arr[-(n - 1):].sum()) if n > 1 else 0.0
    k = 1.0 + float(level)
    return k * s / (n - k) if n > k else float("nan")


# ──────────────────────────────────────────────
# 백테스트
# ──────────────────────────────────────────────
def run_backtest(df: pd.DataFrame, initial_capital: float = 10000.0,
                 start=None, end=None,
                 ma_window: int = 200, slope_window: int = 5,
                 buy_lo: float = 0.01, buy_hi: float = 0.10,
                 exit_disp: float = -0.01, tp_disp: float = 0.55,
                 use_slope: bool = True, use_tp: bool = True,
                 fee_rate: float = 0.0) -> dict:
    """TQ200 백테스트.

    Args:
        df: DatetimeIndex, Open/Close 컬럼. **start 이전 워밍업 포함**해서 넘길 것
            (지표는 df 전체로 계산하고, 매매는 [start, end] 구간에서만 수행).
        buy_lo/buy_hi/exit_disp/tp_disp: 이격도 소수 (0.01 = 1%).
        fee_rate: 편도 수수료+슬리피지 소수 (0.001 = 0.1%).

    Returns dict:
        log      — 일별 로그 DataFrame (index=날짜)
                   [시가, 종가, 200MA, 이격도, Slope, 매수조건, 이탈, 상태, 매매,
                    보유수량, 현금, 평가금]
        cycles   — 완료 사이클 DataFrame
                   [사이클, 시작일, 종료일, 매수가, 매도가, 수익률(%),
                    누적수익률(%), MDD(%), 보유일, 청산사유]
        equity   — 평가금 Series (종가 기준)
        open_position — 기간말 미청산 포지션 dict | None
        pending  — 마지막 날 종가 기준 '다음 거래일 시가' 주문 ("buy"/"sell"/None)
        pending_reason — pending == "sell" 일 때 청산 사유
    """
    ind = compute_indicators(df, ma_window, slope_window)
    ind = ind.dropna(subset=["MA", "Slope"]) if use_slope \
        else ind.dropna(subset=["MA"])
    if start is not None:
        ind = ind[ind.index >= pd.Timestamp(start)]
    if end is not None:
        ind = ind[ind.index <= pd.Timestamp(end)]
    if ind.empty:
        return {"log": pd.DataFrame(), "cycles": pd.DataFrame(),
                "equity": pd.Series(dtype=float), "open_position": None,
                "pending": None, "pending_reason": None}

    o = ind["Open"].to_numpy()
    c = ind["Close"].to_numpy()
    ma = ind["MA"].to_numpy()
    disp = ind["이격도"].to_numpy()
    slope = ind["Slope"].to_numpy()
    n = len(ind)

    buy_ok = (disp > buy_lo) & (disp < buy_hi)
    slope_ok = (slope > 0) if use_slope else np.ones(n, dtype=bool)
    prev_disp = np.concatenate([[np.nan], disp[:-1]])
    # 시트의 '이격도 −1% 이탈' 열 = 위에서 아래로 뚫는 날만 표시 (교차)
    cross_dn = (disp < exit_disp) & ~(prev_disp < exit_disp)

    cash = float(initial_capital)
    shares = 0.0
    pending = None            # 다음 시가 주문: "buy" / "sell" / None
    pending_reason = None
    states, trades = [], []
    sh_arr = np.zeros(n)
    cash_arr = np.zeros(n)
    eq_arr = np.zeros(n)

    cycles = []
    cyc = None                # 진행 중 사이클
    cum = 1.0                 # 사이클 누적 배수 (수수료 반영)

    for i in range(n):
        trade = "—"
        # ── 시가: 전일 신호 체결 ──
        if pending == "buy":
            px = o[i]
            shares = cash * (1.0 - fee_rate) / px
            cyc = {"시작일": ind.index[i], "매수가": px, "i0": i,
                   "투입금": cash, "peak": px, "mdd": 0.0}
            cash = 0.0
            trade = "시가 매수"
        elif pending == "sell":
            px = o[i]
            cash = shares * px * (1.0 - fee_rate)
            shares = 0.0
            trade = "시가 매도"
            # 사이클 MDD: 매도가까지 포함 (진입가 → 종가들 → 매도가의 고점 대비 낙폭)
            cyc["mdd"] = min(cyc["mdd"], px / cyc["peak"] - 1.0)
            ret = cash / cyc["투입금"] - 1.0
            cum *= (1.0 + ret)
            cycles.append({
                "사이클": len(cycles) + 1,
                "시작일": cyc["시작일"], "종료일": ind.index[i],
                "매수가": round(cyc["매수가"], 4), "매도가": round(px, 4),
                "수익률(%)": round(ret * 100, 2),
                "누적수익률(%)": round((cum - 1.0) * 100, 2),
                "MDD(%)": round(cyc["mdd"] * 100, 2),
                "보유일": i - cyc["i0"],
                "청산사유": pending_reason,
            })
            cyc = None
        pending, pending_reason = None, None

        # ── 종가: 신호 판단 ──
        if shares > 0:
            cyc["peak"] = max(cyc["peak"], c[i])
            cyc["mdd"] = min(cyc["mdd"], c[i] / cyc["peak"] - 1.0)
            if disp[i] < exit_disp:
                pending, pending_reason = "sell", EXIT_BREAK
            elif use_tp and disp[i] > tp_disp:
                pending, pending_reason = "sell", EXIT_TP
            state = "매도 신호" if pending == "sell" else "홀딩"
        else:
            if buy_ok[i] and slope_ok[i]:
                pending = "buy"
            state = "매수 신호" if pending == "buy" else "무포지션"

        states.append(state)
        trades.append(trade)
        sh_arr[i] = shares
        cash_arr[i] = cash
        eq_arr[i] = cash + shares * c[i]

    log = pd.DataFrame({
        "시가": o, "종가": c, "200MA": ma, "이격도": disp, "Slope": slope,
        "매수조건": np.where(buy_ok, "O", "X"),
        "이탈": np.where(cross_dn, "O", "X"),
        "상태": states, "매매": trades,
        "보유수량": sh_arr, "현금": cash_arr, "평가금": eq_arr,
    }, index=ind.index)
    log.index.name = "날짜"

    open_position = None
    if shares > 0 and cyc is not None:
        cur = c[-1]
        open_position = {
            "시작일": cyc["시작일"], "매수가": round(cyc["매수가"], 4),
            "수량": shares, "현재가": round(cur, 4),
            "평가손익": shares * cur - cyc["투입금"],
            "평가손익률(%)": round((shares * cur / cyc["투입금"] - 1) * 100, 2),
            "보유일": n - 1 - cyc["i0"],
            "MDD(%)": round(cyc["mdd"] * 100, 2),
        }

    return {"log": log, "cycles": pd.DataFrame(cycles),
            "equity": log["평가금"], "open_position": open_position,
            "pending": pending, "pending_reason": pending_reason}


# ──────────────────────────────────────────────
# 통계
# ──────────────────────────────────────────────
def compute_stats(result: dict, initial_capital: float) -> dict:
    """요약 통계 + 같은 기간 Buy&Hold 비교."""
    log = result["log"]
    if log is None or log.empty:
        return {}
    eq = log["평가금"]
    final = float(eq.iloc[-1])
    years = max((eq.index[-1] - eq.index[0]).days / 365.25, 1e-9)
    total = final / initial_capital - 1.0
    cagr = (final / initial_capital) ** (1 / years) - 1 if final > 0 else -1.0
    # 첫날 시가 매수 직후 낙폭도 잡히도록 시작 자본을 고점 기준에 포함
    full = np.concatenate([[float(initial_capital)], eq.to_numpy()])
    mdd = float((full / np.maximum.accumulate(full) - 1.0).min())

    close = log["종가"]
    bh_total = float(close.iloc[-1] / close.iloc[0] - 1.0)
    bh_cagr = (1 + bh_total) ** (1 / years) - 1
    bh_mdd = float((close / close.cummax() - 1.0).min())

    daily = eq.pct_change().dropna()
    sharpe = float(daily.mean() / daily.std() * np.sqrt(252)) \
        if len(daily) > 1 and daily.std() > 0 else 0.0

    stats = {
        "총수익률(%)": round(total * 100, 2),
        "CAGR(%)": round(cagr * 100, 2),
        "MDD(%)": round(mdd * 100, 2),
        "Calmar": round(cagr / abs(mdd), 2) if mdd < 0 else 0.0,
        "Sharpe": round(sharpe, 2),
        "최종평가금": round(final, 2),
        "B&H 수익률(%)": round(bh_total * 100, 2),
        "B&H CAGR(%)": round(bh_cagr * 100, 2),
        "B&H MDD(%)": round(bh_mdd * 100, 2),
        "보유비중(%)": round(float((log["보유수량"] > 0).mean()) * 100, 1),
        "완료 사이클": 0,
    }
    cyc = result["cycles"]
    if cyc is not None and len(cyc):
        r = cyc["수익률(%)"]
        win, loss = r[r > 0], r[r <= 0]
        stats.update({
            "완료 사이클": int(len(cyc)),
            "승률(%)": round(len(win) / len(cyc) * 100, 1),
            "평균 수익(%)": round(float(win.mean()), 2) if len(win) else 0.0,
            "평균 손실(%)": round(float(loss.mean()), 2) if len(loss) else 0.0,
            "최대 수익(%)": round(float(r.max()), 2),
            "최대 손실(%)": round(float(r.min()), 2),
            "평균 보유일": round(float(cyc["보유일"].mean()), 1),
            "최장 보유일": int(cyc["보유일"].max()),
            "과열 익절 횟수": int((cyc["청산사유"] == EXIT_TP).sum()),
        })
    return stats


def yearly_returns(equity: pd.Series, initial_capital: float,
                   close: pd.Series | None = None) -> pd.DataFrame:
    """연도별 수익률·MDD (평가금 기준) + 선택적으로 B&H 수익률."""
    rows = []
    prev = float(initial_capital)
    prev_c = float(close.iloc[0]) if close is not None and len(close) else None
    for year, s in equity.groupby(equity.index.year):
        arr = np.concatenate([[prev], s.to_numpy()])
        mdd = float((arr / np.maximum.accumulate(arr) - 1.0).min())
        row = {"연도": int(year),
               "수익률(%)": round((float(s.iloc[-1]) / prev - 1) * 100, 2),
               "MDD(%)": round(mdd * 100, 2)}
        if close is not None:
            cy = close[close.index.year == year]
            row["B&H(%)"] = round((float(cy.iloc[-1]) / prev_c - 1) * 100, 2)
            prev_c = float(cy.iloc[-1])
        rows.append(row)
        prev = float(s.iloc[-1])
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────
# 오늘의 신호 (주문표용)
# ──────────────────────────────────────────────
def latest_signal(df: pd.DataFrame, holding: bool,
                  ma_window: int = 200, slope_window: int = 5,
                  buy_lo: float = 0.01, buy_hi: float = 0.10,
                  exit_disp: float = -0.01, tp_disp: float = 0.55,
                  use_slope: bool = True, use_tp: bool = True) -> dict:
    """마지막 확정 종가 기준 — 실제 보유 여부(holding)에 따른 다음 거래일 주문.

    Returns dict: 기준일·종가·200MA·이격도·Slope·action("buy"/"sell"/"hold"/"wait")
                  ·reason·다음 종가 기준 트리거 가격들.
    """
    ind = compute_indicators(df, ma_window, slope_window).dropna(
        subset=["MA", "Slope"] if use_slope else ["MA"])
    if ind.empty:
        return {}
    last = ind.iloc[-1]
    d, sl = float(last["이격도"]), float(last["Slope"])
    in_zone = buy_lo < d < buy_hi
    slope_ok = (sl > 0) if use_slope else True

    if holding:
        if d < exit_disp:
            action, reason = "sell", f"이격도 {exit_disp * 100:+.1f}% 이탈"
        elif use_tp and d > tp_disp:
            action, reason = "sell", f"이격도 {tp_disp * 100:.0f}% 초과 (과열 익절)"
        else:
            action, reason = "hold", "청산 조건 미충족 — 보유 유지"
    else:
        if in_zone and slope_ok:
            action, reason = "buy", "매수 이격도 충족 + 200MA 상승"
        elif in_zone:
            action, reason = "wait", "이격도는 충족했으나 200MA 기울기 ≤ 0"
        elif d >= buy_hi:
            action, reason = "wait", (f"이격도 {buy_hi * 100:.0f}% 이상 — "
                                      "추격 매수 금지, 눌림 대기")
        else:
            action, reason = "wait", f"이격도 {buy_lo * 100:.0f}% 이하 — 200일선 회복 대기"

    closes = df["Close"].astype(float).to_numpy()
    return {
        "기준일": ind.index[-1], "종가": float(last["Close"]),
        "200MA": float(last["MA"]), "이격도": d, "Slope": sl,
        "매수조건": in_zone, "기울기조건": slope_ok,
        "action": action, "reason": reason,
        "다음_청산가": trigger_closes(closes, ma_window, exit_disp),
        "다음_익절가": trigger_closes(closes, ma_window, tp_disp),
        "다음_매수하한": trigger_closes(closes, ma_window, buy_lo),
        "다음_매수상한": trigger_closes(closes, ma_window, buy_hi),
    }
