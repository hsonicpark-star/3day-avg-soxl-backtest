# -*- coding: utf-8 -*-
"""평단법 백테스트 엔진

전략 (평단법시트_v01.xlsx 기반):
- 사이클 시작: 무포지션 상태에서 당일 종가에 1회차 매수 (최초매수가 = 진입 종가)
- 추가매수 레벨: 직전 레벨가 * (1 - 매수갭%) — 분할수만큼 사다리 생성
- 회차수량: INT(1회금액 / 레벨가격), 1회금액 = Seed / 분할수 (복리 시 현재자본 / 분할수)

매도 방식 (sell_mode):
- "target"  ① 목표가 지정매도: 평단 * (1 + threshold) 도달 시 전량 지정가 매도
- "partial" ② 평단 도달 부분매도: 평단 * (1 + threshold) 도달 시 그 가격 이하에
             매수한 회차만 전량 매도. 남은 회차는 유지, 판 레벨은 재매수 가능
- "close"   ③ 종가 만족 전량매도: 종가가 평단 * (1 + threshold) 이상이면
             당일 종가에 전량 매도 (시장가 가정)

일봉 OHLC 체결 가정 (보수적):
- 지정가 매도(①②): 시가 >= 트리거면 시가 체결, 아니면 고가 >= 트리거면 트리거가 체결.
  트리거는 당일 시작 시점 평단 기준 (당일 추가매수로 낮아진 트리거로는 당일 매도 안 함)
- 매도가 발생한 날은 추가매수 없음 (장중 순서를 알 수 없으므로 보수적 처리)
- 매수: 저가 <= 레벨가면 체결. 갭하락(시가 < 레벨가) 시 시가 체결
- ③은 종가 시점 판단이므로 당일 매수 반영 후의 평단으로 평가
"""
from __future__ import annotations

import pandas as pd

SELL_MODES = {
    "target": "① 목표가 지정매도",
    "partial": "② 평단 도달 부분매도",
    "close": "③ 종가 만족 전량매도",
}


def ladder_prices(first_price: float, splits: int, buy_gap) -> list[float]:
    """회차별 매수가 사다리 생성.

    buy_gap: 비율 스칼라 (0.01) 또는 구간 리스트 [(끝회차, 비율갭), ...]
             예) [(20, 0.01), (30, 0.007), (40, 0.005)]
             → 2~20회차 1%, 21~30회차 0.7%, 31~40회차 0.5% 간격
    """
    if isinstance(buy_gap, (int, float)):
        tiers = [(splits, float(buy_gap))]
    else:
        tiers = sorted((int(t), float(g)) for t, g in buy_gap)
    prices, price = [], float(first_price)
    for i in range(1, splits + 1):
        if i > 1:
            gap = next((g for lim, g in tiers if i <= lim), tiers[-1][1])
            price *= (1 - gap)
        prices.append(price)
    return prices


def build_order_table(seed: float, splits: int, first_price: float,
                      buy_gap, target_pct: float) -> pd.DataFrame:
    """주문테이블 생성 — 시트의 회차/매수가/수량/평단/매도희망 재현.

    buy_gap: 비율 스칼라 또는 구간 리스트 (ladder_prices 참조)
    target_pct는 비율 (예: 0.05)
    """
    per_amt = seed / splits
    rows = []
    tot_qty, tot_amt = 0, 0.0
    for i, price in enumerate(ladder_prices(first_price, splits, buy_gap),
                              start=1):
        qty = int(per_amt // price) if price > 0 else 0
        amt = qty * price
        tot_qty += qty
        tot_amt += amt
        avg = tot_amt / tot_qty if tot_qty else 0.0
        rows.append({
            "회차": i, "매수가": round(price, 2), "회차수량": qty,
            "회차금액": round(amt, 2), "총수량": tot_qty,
            "총금액": round(tot_amt, 2), "평단": round(avg, 2),
            "매도희망": round(avg * (1 + target_pct), 2),
        })
    return pd.DataFrame(rows)


def run_backtest(df: pd.DataFrame, seed: float, splits: int,
                 buy_gap, target_pct: float,
                 compound: bool = True, fee_rate: float = 0.0,
                 reentry_same_day: bool = True,
                 sell_mode: str = "target") -> dict:
    """일봉 OHLC 기반 평단법 백테스트.

    df: DatetimeIndex + [Open, High, Low, Close]
    buy_gap: 비율 스칼라 또는 구간 리스트 (ladder_prices 참조)
    target_pct, fee_rate는 비율 (0.01 = 1%)
    target_pct: 매도 방식별 임계값 (①목표이익률 ②평단 프리미엄 ③만족 수익률)
    반환: {"equity", "cycles", "fills", "open_position", "skipped_starts"}
    """
    if sell_mode not in SELL_MODES:
        raise ValueError(f"sell_mode must be one of {list(SELL_MODES)}")

    cash = float(seed)
    levels: list[dict] = []   # {no, price, qty, filled, fill_px}
    cycle: dict | None = None
    cycles, fills, equity_rows = [], [], []
    skipped_starts = 0

    def held():
        return [lv for lv in levels if lv["filled"]]

    def position():
        rows = held()
        shares = sum(lv["qty"] for lv in rows)
        cost = sum(lv["qty"] * lv["fill_px"] for lv in rows)
        return shares, cost

    def start_cycle(date, close_px):
        nonlocal cash, levels, cycle, skipped_starts
        capital = cash if compound else min(cash, seed)
        per_amt = capital / splits
        qty1 = int(per_amt // close_px)
        if qty1 <= 0 or cash < qty1 * close_px * (1 + fee_rate):
            skipped_starts += 1
            return
        levels = [{"no": i, "price": price, "qty": int(per_amt // price),
                   "filled": False, "fill_px": 0.0}
                  for i, price in enumerate(
                      ladder_prices(close_px, splits, buy_gap), start=1)]
        cycle = {"시작일": date, "진입가": close_px, "투입금": 0.0,
                 "회수금": 0.0, "최대회차": 0, "부분매도": 0}
        fill_level(date, levels[0], close_px)

    def fill_level(date, lv, px):
        nonlocal cash
        cash -= lv["qty"] * px * (1 + fee_rate)
        lv["filled"], lv["fill_px"] = True, px
        cycle["투입금"] += lv["qty"] * px * (1 + fee_rate)
        cycle["최대회차"] = max(cycle["최대회차"], lv["no"])
        shares, cost = position()
        fills.append({"일자": date, "구분": "매수", "회차": lv["no"],
                      "가격": round(px, 2), "수량": lv["qty"],
                      "금액": round(lv["qty"] * px, 2),
                      "평단": round(cost / shares, 2),
                      "목표가": round(cost / shares * (1 + target_pct), 2)})

    def sell_rounds(date, rows, px, kind):
        """rows의 회차들을 px에 매도. 레벨은 미체결 상태로 리셋 (재매수 가능)."""
        nonlocal cash
        qty = sum(lv["qty"] for lv in rows)
        cost = sum(lv["qty"] * lv["fill_px"] for lv in rows)
        proceeds = qty * px * (1 - fee_rate)
        cash += proceeds
        cycle["회수금"] += proceeds
        shares_all, cost_all = position()
        fills.append({"일자": date, "구분": kind,
                      "회차": max(lv["no"] for lv in rows),
                      "가격": round(px, 2), "수량": qty,
                      "금액": round(qty * px, 2),
                      "평단": round(cost_all / shares_all, 2),
                      "목표가": round(cost / qty, 2)})
        for lv in rows:
            lv["filled"], lv["fill_px"] = False, 0.0

    def close_cycle(date, sell_px):
        nonlocal levels, cycle
        profit = cycle["회수금"] - cycle["투입금"]
        cycles.append({"시작일": cycle["시작일"], "진입가": cycle["진입가"],
                       "종료일": date,
                       "기간(일)": (date - cycle["시작일"]).days,
                       "최대회차": cycle["최대회차"],
                       "부분매도": cycle["부분매도"],
                       "매도가": round(sell_px, 2),
                       "투입금": round(cycle["투입금"], 2),
                       "수익": round(profit, 2),
                       "수익률(%)": round(profit / cycle["투입금"] * 100, 2)})
        levels, cycle = [], None

    for date, row in df.iterrows():
        o, h, l, c = row["Open"], row["High"], row["Low"], row["Close"]
        sold_today = False        # 전량 청산 발생
        sell_event_today = False  # 매도 체결 발생 (부분 포함) → 당일 매수 금지

        shares, cost = position() if cycle else (0, 0.0)

        # ── 지정가 매도 (①목표가 / ②부분매도) — 당일 시작 시점 평단 기준
        if shares > 0 and sell_mode in ("target", "partial"):
            avg = cost / shares
            trigger = avg * (1 + target_pct)
            sell_px = o if o >= trigger else (trigger if h >= trigger else None)
            if sell_px is not None:
                if sell_mode == "target":
                    sell_rounds(date, held(), sell_px, "전량매도")
                    close_cycle(date, sell_px)
                    sold_today = True
                else:
                    rows = [lv for lv in held() if lv["fill_px"] <= sell_px]
                    if rows:
                        full = len(rows) == len(held())
                        sell_rounds(date, rows, sell_px,
                                    "전량매도" if full else "부분매도")
                        if full:
                            close_cycle(date, sell_px)
                            sold_today = True
                        else:
                            cycle["부분매도"] += 1
                sell_event_today = True

        # ── 추가매수 (매도 체결일은 건너뜀 — 장중 순서 불명, 보수적)
        #    부분매도로 리셋된 레벨도 다시 하락하면 재매수된다
        if cycle is not None and not sell_event_today:
            for lv in levels:
                if lv["filled"] or lv["qty"] <= 0:
                    continue
                if l <= lv["price"]:
                    px = min(lv["price"], o)
                    if cash >= lv["qty"] * px * (1 + fee_rate):
                        fill_level(date, lv, px)

        # ── ③ 종가 만족 전량매도 — 당일 매수 반영 후 평단으로 종가 판단
        if cycle is not None and sell_mode == "close":
            shares, cost = position()
            if shares > 0 and c >= (cost / shares) * (1 + target_pct):
                sell_rounds(date, held(), c, "전량매도")
                close_cycle(date, c)
                sold_today = True

        # ── 재진입
        if cycle is None and (not sold_today or reentry_same_day):
            start_cycle(date, c)

        shares, _ = position() if cycle else (0, 0.0)
        equity_rows.append({"일자": date, "평가금": cash + shares * c,
                            "현금": cash, "주식수": shares,
                            "회차": cycle["최대회차"] if cycle else 0})

    equity = pd.DataFrame(equity_rows).set_index("일자")
    open_position = None
    if cycle is not None:
        shares, cost = position()
        if shares > 0:
            last_close = df["Close"].iloc[-1]
            avg = cost / shares
            net_invested = cycle["투입금"] - cycle["회수금"]
            open_position = {
                "시작일": cycle["시작일"], "회차": cycle["최대회차"],
                "수량": shares, "평단": round(avg, 2),
                "목표가": round(avg * (1 + target_pct), 2),
                "현재가": round(last_close, 2),
                "평가손익": round(shares * last_close - net_invested, 2),
                "평가손익률(%)": round(
                    (shares * last_close / net_invested - 1) * 100, 2)
                if net_invested > 0 else 0.0,
                "투입금": round(net_invested, 2),
            }
    return {"equity": equity,
            "cycles": pd.DataFrame(cycles),
            "fills": pd.DataFrame(fills),
            "open_position": open_position,
            "skipped_starts": skipped_starts}


def compute_stats(result: dict, df: pd.DataFrame, seed: float) -> dict:
    """백테스트 결과 요약 통계 + Buy&Hold 비교."""
    eq = result["equity"]["평가금"]
    final = eq.iloc[-1]
    n_days = (eq.index[-1] - eq.index[0]).days
    years = max(n_days / 365.25, 1e-9)
    total_ret = final / seed - 1
    cagr = (final / seed) ** (1 / years) - 1 if final > 0 else -1.0
    peak = eq.cummax()
    mdd = (eq / peak - 1).min()

    close = df["Close"]
    bh_ret = close.iloc[-1] / close.iloc[0] - 1
    bh_mdd = (close / close.cummax() - 1).min()

    cyc = result["cycles"]
    stats = {
        "총수익률(%)": round(total_ret * 100, 2),
        "CAGR(%)": round(cagr * 100, 2),
        "MDD(%)": round(mdd * 100, 2),
        "최종평가금": round(final, 2),
        "완료 사이클": len(cyc),
        "B&H 수익률(%)": round(bh_ret * 100, 2),
        "B&H MDD(%)": round(bh_mdd * 100, 2),
    }
    if len(cyc):
        stats.update({
            "평균 사이클(일)": round(cyc["기간(일)"].mean(), 1),
            "최장 사이클(일)": int(cyc["기간(일)"].max()),
            "평균 최대회차": round(cyc["최대회차"].mean(), 1),
            "최대 사용회차": int(cyc["최대회차"].max()),
            "사이클 평균수익(%)": round(cyc["수익률(%)"].mean(), 2),
            "부분매도 횟수": int(cyc["부분매도"].sum()),
        })
    return stats


def yearly_returns(equity: pd.DataFrame) -> pd.DataFrame:
    """연도별 수익률 (평가금 기준)."""
    eq = equity["평가금"]
    rows = []
    for year, s in eq.groupby(eq.index.year):
        prev_years = eq[eq.index.year < year]
        base = prev_years.iloc[-1] if len(prev_years) else s.iloc[0]
        rows.append({"연도": year,
                     "수익률(%)": round((s.iloc[-1] / base - 1) * 100, 2)})
    return pd.DataFrame(rows)
