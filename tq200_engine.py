# -*- coding: utf-8 -*-
"""TQ200 (TQQQ 200일선 이격도 매매) — 백테스트 엔진

원본: 14.TQ200/TQQQ200일선.xlsx (최근로그 · 사이클 시트) 규칙 1:1 이식.
운용 기본값(DEFAULTS)은 과열 익절만 원본 55% → **50%** 로 조정 (2026-11-09).
원본 시트 값 그대로는 SHEET_PARAMS — 엑셀 대조 검증은 이 값으로 한다.

규칙 (모든 판단은 종가 확정 후, 체결은 다음 거래일 시가)
----------------------------------------------------------
- 200MA   = 종가 200일 단순이동평균
- 이격도  = 종가 / 200MA − 1
- Slope   = (200MA − 5거래일 전 200MA) / 5      (USD/day)
- 매수 신호 (무포지션): buy_lo < 이격도 < buy_hi (1% 초과 10% 미만)  AND  Slope > 0
                        → 다음 거래일 **시가 전량 매수** (MOO)
- 매도 신호 (보유 중) : 이격도 < exit_disp (−1% 미만으로 이탈)
                        또는 이격도 > tp_disp (기본 50% 초과 과열 익절 · 원본 55%)
                        → 다음 거래일 **시가 전량 매도** (MOO)
- 경계값은 모두 '초과/미만' (정확히 1.00%·−1.00%·50.00% 는 신호 아님).
  원본 엑셀 구간에는 경계에 정확히 걸린 날이 없어 '이상/이하'와 결과가 같다.
- 시가 매도한 날 종가에 매수 조건이 맞으면 그날 바로 매수 신호 (다음날 재진입)
- 시가 매수한 날 종가가 −1% 를 이탈하면 그날 바로 매도 신호 (다음날 청산)

Streamlit 비의존 — 순수 pandas/numpy. 검증: 14.TQ200/verify_tq200.py
일일 자동 발송(scripts/daily_telegram_alert.py)도 이 모듈의 daily_signal() 을 쓴다.
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
    # 과열 익절 이격도 (%) — 초과 시 매도. 원본 시트는 55.
    # 50~55% 는 성과가 비슷한 구간인데 55 는 그 위쪽 끝이라(57.5 부터 CAGR 39.7%·
    # MDD −36.9% 로 급락 — 2020-02 익절이 이격도 55.68% 에서 겨우 걸림) 안쪽으로 옮겼다.
    # 50 기준 TQQQ 2011-01~2026-11: CAGR 45.3% · MDD −31.1% (55: 43.9% · −31.1%).
    "tp_disp": 50.0,
    "use_slope": True,     # 200MA 기울기 > 0 필터 사용
    "use_tp": True,        # 과열 익절 사용
    "fee": 0.0,            # 수수료+슬리피지 (%, 편도)
}

# 원본 엑셀(TQQQ200일선.xlsx)의 값 — 14.TQ200/verify_tq200.py 대조 검증용
SHEET_PARAMS = {**DEFAULTS, "tp_disp": 55.0}

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
                 exit_disp: float = -0.01, tp_disp: float = 0.50,
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
                  exit_disp: float = -0.01, tp_disp: float = 0.50,
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


# ──────────────────────────────────────────────
# 주문표 (앱 '오늘의 신호' 탭 · 텔레그램 · 일일 자동 발송 공용 — streamlit 비의존)
# ──────────────────────────────────────────────
def engine_kwargs(P: dict) -> dict:
    """저장/사이드바 params(% 단위) → 엔진 인자(소수 단위). 빠진 키는 DEFAULTS."""
    g = lambda k: P.get(k, DEFAULTS[k])   # noqa: E731
    return dict(ma_window=int(g("ma_window")),
                slope_window=int(g("slope_window")),
                buy_lo=float(g("buy_lo")) / 100,
                buy_hi=float(g("buy_hi")) / 100,
                exit_disp=float(g("exit_disp")) / 100,
                tp_disp=float(g("tp_disp")) / 100,
                use_slope=bool(g("use_slope")), use_tp=bool(g("use_tp")))


def build_order_plan(ticker: str, sig: dict, pos: dict, next_day) -> dict:
    """신호 + 포지션 → 다음 거래일 MOO 주문 계획.

    pos: {"holding", "shares", "avg_price", "cash", "gap_buf"}
    Returns: action · headline(한 줄 요약) · detail(설명 문장) · rows(주문표 행, 없으면 [])
    """
    close = sig["종가"]
    shares = int(pos.get("shares", 0) or 0)
    avg_px = float(pos.get("avg_price", 0.0) or 0.0)
    cash = float(pos.get("cash", 0.0) or 0.0)
    gap_buf = float(pos.get("gap_buf", 3.0) or 0.0)
    pnl = ((close / avg_px - 1) * 100) if avg_px > 0 else None
    action, rows = sig["action"], []

    if action == "buy":
        ref_px = close * (1 + gap_buf / 100)
        qty = int(cash // ref_px) if ref_px > 0 else 0
        headline = f"🟢 {next_day} MOO 매수 — {ticker} {qty:,}주"
        detail = (f"{sig['reason']}. 예수금 ${cash:,.0f} ÷ (종가 ${close:,.2f} "
                  f"× 갭 여유 {gap_buf:g}%) = **{qty:,}주**. "
                  "개장 전에 **MOO(시가 시장가)** 로 주문하세요.")
        rows.append({
            "구분": "🟢 MOO매수", "종목": ticker, "수량": f"{qty:,}",
            "기준가(전일 종가)": f"${close:,.2f}",
            "예상 금액": f"${qty * close:,.0f}",
            "비고": f"시가 ${ref_px:,.2f} 까지 예수금 내 체결"})
    elif action == "sell":
        qty_txt = f"{shares:,}주 전량" if shares > 0 else "보유 전량"
        headline = f"🔴 {next_day} MOO 매도 — {ticker} {qty_txt}"
        detail = (f"{sig['reason']}. 개장 전에 **MOO(시가 시장가)** 로 전량 매도 "
                  "주문하세요."
                  + (f" 종가 기준 손익 {pnl:+.2f}%." if pnl is not None else ""))
        rows.append({
            "구분": "🔴 MOO매도", "종목": ticker, "수량": f"{shares:,}",
            "기준가(전일 종가)": f"${close:,.2f}",
            "예상 금액": f"${shares * close:,.0f}",
            "비고": "전량 · " + sig["reason"]})
    elif action == "hold":
        headline = f"🟦 홀딩 유지 — {ticker} 주문 없음"
        detail = (f"{sig['reason']}."
                  + (f" 종가 기준 평가손익 {pnl:+.2f}%." if pnl is not None else ""))
    else:
        headline = f"⚪ 관망 — {ticker} 주문 없음"
        detail = f"{sig['reason']}."
    return {"action": action, "headline": headline, "detail": detail,
            "rows": rows}


def build_order_text(ticker: str, P: dict, sig: dict, plan: dict, pos: dict,
                     next_day, notes=None) -> str:
    """텔레그램용 주문표 메시지 (HTML parse_mode). notes = 끝에 붙일 안내 줄 목록."""
    close = sig["종가"]
    holding = bool(pos.get("holding"))
    ma_n = int(P.get("ma_window", DEFAULTS["ma_window"]))
    sl_n = int(P.get("slope_window", DEFAULTS["slope_window"]))
    use_tp = bool(P.get("use_tp", DEFAULTS["use_tp"]))
    use_slope = bool(P.get("use_slope", DEFAULTS["use_slope"]))
    if holding:
        shares = int(pos.get("shares", 0) or 0)
        avg_px = float(pos.get("avg_price", 0) or 0)
        pos_line = ("포지션: 보유" + (f" {shares:,}주" if shares > 0 else " 중")
                    + (f" (단가 ${avg_px:,.2f})" if avg_px > 0 else ""))
    else:
        pos_line = f"포지션: 무포지션 (예수금 ${float(pos.get('cash', 0) or 0):,.0f})"
    slope_txt = "상승" if sig["Slope"] > 0 else "하락/횡보"
    lines = [
        f"<b>📋 TQ200 — {ticker} 주문표</b>",
        f"📅 {next_day} 주문 (기준 종가 {sig['기준일'].date()})",
        "",
        f"전일종가: <b>${close:,.2f}</b>",
        f"{ma_n}MA: ${sig['200MA']:,.2f} · 이격도 "
        f"<b>{sig['이격도'] * 100:+.2f}%</b>",
        f"MA 기울기({sl_n}일): {sig['Slope']:+.4f} ({slope_txt})",
        pos_line,
        "",
        f"<b>{plan['headline']}</b>",
        f"사유: {sig['reason']}",
    ]
    if plan["rows"]:
        lines.append("주문: 개장 전 MOO(시가 시장가) 접수")
    # 주문이 체결된 뒤의 포지션 기준 (매수 체결 → 보유, 매도 체결 → 무포지션)
    will_hold = (plan["action"] == "buy") or (holding and plan["action"] != "sell")
    lines += ["", f"<b>{next_day} 종가 트리거</b>"]
    if will_hold:
        lines.append(f"• 청산: ${sig['다음_청산가']:,.2f} 미만 "
                     f"({(sig['다음_청산가'] / close - 1) * 100:+.1f}%)")
        if use_tp:
            lines.append(f"• 과열 익절: ${sig['다음_익절가']:,.2f} 초과 "
                         f"({(sig['다음_익절가'] / close - 1) * 100:+.1f}%)")
    else:
        lines.append(f"• 매수 구간: ${sig['다음_매수하한']:,.2f} 초과 ~ "
                     f"${sig['다음_매수상한']:,.2f} 미만")
        if use_slope and sig["Slope"] <= 0:
            lines.append("  (MA 기울기가 양수로 돌아서야 매수)")
    if notes:
        lines += [""] + list(notes)
    return "\n".join(lines)


def daily_signal(df: pd.DataFrame, P: dict, saved_pos: dict | None,
                 ticker: str, next_day, signal_only: bool = False) -> dict:
    """일일 자동 발송용 — **매 거래일** send=True (신호가 드물어 매일 현황을 보낸다).

    · 매수/매도 신호가 난 날 : 맨 위에 '주문이 있는 날' 표시 + MOO 주문표
    · 신호가 없는 날         : 홀딩 유지 / 관망 현황 + 다음 종가 트리거 가격

    판단 기준은 전체 구간 백테스트 모델이다. 신호 여부는 마지막 날의
    run_backtest()["pending"] (= 원본 시트의 '매수 신호'/'매도 신호' 와 같은 날, 한 신호당
    한 번), 신호가 없는 날의 보유 여부는 모델의 마지막 날 보유 상태.
    수량은 저장된 실제 포지션(saved_pos)으로 계산하고, 저장된 포지션이 모델과
    다르면(예: 모델은 보유인데 저장은 무포지션) 안내 줄을 붙인다.

    P: params(% 단위, 빠진 키는 DEFAULTS) + 선택 "capital"/"fee".
    signal_only: True 면 예전 방식 — 신호가 난 날에만 send=True.
    Returns: {"send": bool, "text": str, "action": "buy"/"sell"/None(신호 없음),
              "status": "buy"/"sell"/"hold"/"wait", "base_day": Timestamp|None,
              "reason": str}
    """
    kw = engine_kwargs(P)
    model = run_backtest(df, **kw)
    log = model["log"]
    if log is None or log.empty:
        return {"send": False, "text": "", "action": None, "status": None,
                "base_day": None, "reason": "이동평균 계산에 필요한 데이터 부족"}
    pending = model["pending"]
    base_day = log.index[-1]
    is_signal = pending in ("buy", "sell")
    if signal_only and not is_signal:
        return {"send": False, "text": "", "action": None, "status": None,
                "base_day": base_day,
                "reason": f"신호 없음 (모델 상태: {log['상태'].iloc[-1]})"}

    if is_signal:       # 매도 신호 = 모델 보유 중, 매수 신호 = 모델 무포지션
        model_hold = (pending == "sell")
    else:
        model_hold = bool(log["보유수량"].iloc[-1] > 0)
    sig = latest_signal(df, holding=model_hold, **kw)
    pos = dict(saved_pos or {})
    pos.setdefault("cash", float(P.get("capital", 10000.0)))
    saved_hold = bool(pos.get("holding")) if saved_pos else None
    pos["holding"] = model_hold
    plan = build_order_plan(ticker, sig, pos, next_day)

    notes = []
    if saved_pos is not None and saved_hold != model_hold:
        notes.append("⚠️ 저장된 포지션("
                     + ("보유 중" if saved_hold else "무포지션")
                     + ")이 백테스트 모델("
                     + ("보유 중" if model_hold else "무포지션")
                     + ")과 다릅니다. "
                     + ("실제 계좌 상태를 확인하고 주문하세요." if is_signal
                        else "이 메시지는 모델 기준입니다."))
    if is_signal:
        if saved_pos is None:
            notes.append("ℹ️ 저장된 포지션이 없어 "
                         + (f"초기 투자금 ${pos['cash']:,.0f} 기준으로 수량을 계산했습니다."
                            if pending == "buy" else "수량 없이 '보유 전량'으로 안내합니다."))
        notes.append("체결 후 앱 [오늘의 신호] 탭에서 포지션을 저장해 두면 다음 주문 수량이 "
                     "정확해집니다.")
    text = build_order_text(ticker, P, sig, plan, pos, next_day, notes)
    if is_signal:       # 매일 오는 현황 메시지 속에서 주문 있는 날이 묻히지 않게
        text = ("🚨 <b>매수 신호 — 오늘 밤 개장 전 주문</b>" if pending == "buy"
                else "🚨 <b>매도 신호 — 오늘 밤 개장 전 주문</b>") + "\n\n" + text
    return {"send": True, "action": pending if is_signal else None,
            "status": plan["action"], "base_day": base_day,
            "reason": sig["reason"], "text": text}
