# -*- coding: utf-8 -*-
"""듀얼스나이퍼 TQ (TQQQ 200일선 홀딩 + 듀얼스나이퍼 방어) — 백테스트 엔진

원본: 08.듀얼스나이퍼/항공모함셋_TQQQ.xlsx (2011-01-03 ~ 2026-11-10) 규칙 이식.
검증: 08.듀얼스나이퍼/validate_dstq.py

규칙
----
추세(200일선 위) + 역추세(200일선 아래) 혼합. 모드는 **홀딩 / 방어** 두 가지.

■ 모드 판정 (TQ200 과 같은 신호 — 종가 확정 후 판단, 다음 거래일 시가 체결)
- 200MA = 종가 200일 단순이동평균 · 이격도 = 종가/200MA − 1
- Slope = (200MA − 5거래일 전 200MA) / 5
- 방어 중 **매수 신호**: buy_lo < 이격도 < buy_hi (1%~8%) AND Slope > 0
    → 다음 거래일 시가에 남은 현금 전부 매수 (LOO, 지정가 = 전일종가 × (1+loo_pct%))
      수량 = floor(현금 / 지정가) 라서 몰빵해도 예수금이 조금 남는다.
      보유 중이던 방어 포지션은 팔지 않고 **홀딩분으로 전환** (만기·MA 매도 해제).
- 홀딩 중 **매도 신호**: 이격도 < exit_disp (−1%) 또는 이격도 > tp_disp (50%)
    → 다음 거래일 시가 전량 매도 (MOO) → 그날부터 방어 (그날 종가 LOC 매수 가능).
- 시작할 때 이미 200일선 위로 멀리(8% 이상) 떠 있으면 매수 신호가 없으므로 방어로 시작.

■ 방어 모드 (듀얼스나이퍼 방어 로직, LOC)
- 매수가 K = min( MA기준 조건1, 전일종가×(1+buy_pct2%) )
    조건1 = 오늘 종가가 들어왔을 때 종가/MA(n) − 1 = buy_pct1% 가 되는 종가
- 매일 가장 낮은 빈 티어 1개 매수 · 종가 ≤ K 면 체결(종가 체결)
- 할당금 = 장 시작 현금 × (티어비중 / 남은 티어 비중합) · 수량 = floor(할당금 / K)
- 매도: 종가 ≥ MA기준 (1+sell_pct%) 가격이면 방어분 **일괄 매도**
    (단, 전일종가 > 전일 MA5 면 방T1 은 보류 — 듀얼스나이퍼 공통 규칙)
- 보유 hold 거래일 만기 → 그 티어만 MOC. MOC 가 있는 날은 MA 매도 보류.

Streamlit 비의존 — 순수 pandas/numpy.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

MODE_HOLD = "홀딩"
MODE_DEF = "방어"
EXIT_BREAK = "이격도 이탈"
EXIT_TP = "이격도 과열"

# 항공모함셋 (원본 시트 값)
DEFAULTS = {
    "ma_window": 200, "slope_window": 5,
    "buy_lo": 1.0, "buy_hi": 8.0,        # 홀딩 매수 이격도 하한/상한 (%)
    "exit_disp": -1.0, "tp_disp": 50.0,  # 홀딩 매도 이격도 하한/상한 (%)
    "use_slope": True,
    "loo_pct": 10.0,                     # 홀딩 시가 매수 지정가 = 전일종가 × (1+%)
    "sf_div": 5, "sf_hold": 8,
    "sf_buy1": -1.5, "sf_buy2": 5.5,
    "sf_sell": 1.5, "sf_sell_ratio": 100.0,
    "sf_ma_base": 3,
    "tier1_hold": True,                  # 전일종가 > 전일 MA5 면 방T1 MA 매도 보류 (MOC 제외)
    "sf_weights": (6, 13, 20, 27, 34),
    "fee": 0.0,                          # 수수료 (%, 편도)
}


@dataclass
class Lot:
    tier: int
    buy_date: pd.Timestamp
    buy_price: float
    order_price: float
    qty: int
    stop_idx: int


@dataclass
class State:
    mode: str = MODE_DEF
    cash: float = 0.0
    hold_qty: int = 0                 # 홀딩 모드 보유 수량 (전환된 방어분 포함)
    hold_cost: float = 0.0            # 홀딩분 매수 원가 합
    hold_start: pd.Timestamp | None = None
    lots: list = field(default_factory=list)   # 방어 포지션
    pending: str | None = None        # 다음 거래일 시가 주문: "buy" / "sell"
    pending_reason: str | None = None


def compute_indicators(df: pd.DataFrame, ma_window: int = 200,
                       slope_window: int = 5) -> pd.DataFrame:
    out = df[["Open", "Close"]].astype(float).copy()
    out["Open"] = out["Open"].round(4)
    out["Close"] = out["Close"].round(4)      # 원본 시트와 같은 정밀도
    out["MA"] = out["Close"].rolling(int(ma_window)).mean()
    out["이격도"] = out["Close"] / out["MA"] - 1.0
    out["Slope"] = (out["MA"] - out["MA"].shift(int(slope_window))) / float(slope_window)
    out["MA5"] = out["Close"].rolling(5).mean()
    return out


def ma_target_close(prev_closes, n: int, pct: float) -> float:
    """오늘 종가 C 가 들어왔을 때 C / MA(n) − 1 = pct% 가 되는 C.
    prev_closes = 직전 (n−1)일 종가."""
    f = 1.0 + pct / 100.0
    s = float(np.sum(prev_closes))
    return f * s / (n - f)


def _ndec(x: float) -> int:
    """가격의 소수 자릿수 기준: 2자리 이하면 2, 아니면 4 (분할 조정된 과거 가격)."""
    return 2 if abs(round(x, 2) - x) < 1e-9 else 4


def floor_px(x: float, d: int = 2) -> float:
    return math.floor(x * 10 ** d + 1e-9) / 10 ** d


def ceil_px(x: float, d: int = 2) -> float:
    return math.ceil(x * 10 ** d - 1e-9) / 10 ** d


def defense_prices(prev_closes, prev_close: float, p: dict, d: int = 2):
    """방어 LOC 매수가 K(내림) · 매도가 S(올림). prev_closes = 직전 (MA기준−1)일 종가."""
    n = int(p["sf_ma_base"])
    K = min(ma_target_close(prev_closes, n, p["sf_buy1"]),
            prev_close * (1 + p["sf_buy2"] / 100.0))
    S = ma_target_close(prev_closes, n, p["sf_sell"])
    return floor_px(K, d), ceil_px(S, d)


def norm_params(P: dict | None = None) -> dict:
    p = {**DEFAULTS, **(P or {})}
    w = p["sf_weights"]
    if isinstance(w, str):
        w = [float(x) for x in w.split(",") if x.strip()]
    p["sf_weights"] = tuple(float(x) for x in w)
    p["sf_div"] = int(p["sf_div"])
    if len(p["sf_weights"]) != p["sf_div"]:
        raise ValueError("티어별 매수비중 개수가 방어 분할수와 다릅니다.")
    return p


def run_backtest(df: pd.DataFrame, P: dict | None = None,
                 initial_capital: float = 10000.0, start=None, end=None,
                 return_state: bool = False,
                 capital_injections: dict | None = None) -> dict:
    """df: DatetimeIndex + Open/Close (무조정). start 이전 워밍업 포함해서 넘길 것.

    Returns dict(log, trades, equity, state). log 는 일별, trades 는 체결 단위.
    capital_injections: {날짜: 금액} — 장 종료 후 기준(다음 거래일 장 시작부터 가용).
    """
    p = norm_params(P)
    n_ma, hold_days, div = int(p["sf_ma_base"]), int(p["sf_hold"]), p["sf_div"]
    wts = list(p["sf_weights"])
    fee = float(p["fee"]) / 100.0
    buy_lo, buy_hi = p["buy_lo"] / 100.0, p["buy_hi"] / 100.0
    exit_d, tp_d = p["exit_disp"] / 100.0, p["tp_disp"] / 100.0
    sell_ratio = float(p["sf_sell_ratio"]) / 100.0

    ind = compute_indicators(df, p["ma_window"], p["slope_window"]).sort_index()
    if end is not None:
        ind = ind[ind.index <= pd.Timestamp(end)]
    dates = ind.index
    o = ind["Open"].to_numpy(); c = ind["Close"].to_numpy()
    ma = ind["MA"].to_numpy(); disp = ind["이격도"].to_numpy(); slope = ind["Slope"].to_numpy()
    ma5 = ind["MA5"].to_numpy()
    n = len(ind)
    i0 = 0 if start is None else int(np.searchsorted(dates, pd.Timestamp(start)))
    i0 = max(i0, n_ma)

    inject_on = {}
    for d, amt in (capital_injections or {}).items():
        cand = dates[dates > pd.Timestamp(d).normalize()]
        cand = cand[cand >= dates[i0]] if len(cand) and i0 < n else cand
        if len(cand):
            inject_on[cand[0]] = inject_on.get(cand[0], 0.0) + float(amt)

    st = State(cash=float(initial_capital))
    units = float(initial_capital) or 1.0
    prev_total = float(initial_capital)
    rows, trades = [], []

    for i in range(i0, n):
        date = dates[i]
        if date in inject_on:
            up = prev_total / units if units > 0 else 1.0
            units += inject_on[date] / up if up > 0 else 0.0
            st.cash += inject_on[date]

        row = {"날짜": date, "시가": o[i], "종가": c[i], "200MA": ma[i],
               "이격도": disp[i], "Slope": slope[i], "매수가": None, "매도가": None,
               "매수티어": "", "매수량": 0, "매수금": 0.0,
               "매도사유": "", "매도량": 0, "매도금": 0.0, "실현손익": 0.0}

        # ── 시가: 전일 신호 체결 ──
        loo_miss = False
        if st.pending == "buy":
            limit = floor_px(c[i - 1] * (1 + p["loo_pct"] / 100.0), _ndec(c[i - 1]))
            qty = int(math.floor(st.cash / (limit * (1 + fee)))) if limit > 0 else 0
            if o[i] <= limit:
                amt = qty * o[i]
                st.cash -= amt * (1 + fee)
                conv = sum(l.qty for l in st.lots)
                st.hold_cost = sum(l.qty * l.buy_price for l in st.lots) + amt
                st.hold_qty = conv + qty
                st.lots = []
                st.mode, st.hold_start = MODE_HOLD, date
                row.update(매수티어=MODE_HOLD, 매수량=qty, 매수금=amt, 매수가=o[i])
                if qty > 0:
                    trades.append({"날짜": date, "구분": "매수", "모드": MODE_HOLD, "티어": MODE_HOLD,
                                   "가격": o[i], "수량": qty, "금액": amt, "사유": "매수 조건 충족",
                                   "실현손익": 0.0, "전환수량": conv})
            else:
                loo_miss = True      # 시가가 지정가(+loo_pct%)를 넘는 갭 상승 → 미체결, 그날은 쉼
        elif st.pending == "sell" and st.hold_qty > 0:
            amt = st.hold_qty * o[i]
            pnl = amt * (1 - fee) - st.hold_cost
            st.cash += amt * (1 - fee)
            row.update(매도사유=st.pending_reason, 매도량=st.hold_qty, 매도금=amt,
                       매도가=o[i], 실현손익=pnl)
            trades.append({"날짜": date, "구분": "매도", "모드": MODE_HOLD, "티어": MODE_HOLD,
                           "가격": o[i], "수량": st.hold_qty, "금액": amt,
                           "사유": st.pending_reason, "실현손익": pnl, "전환수량": 0})
            st.hold_qty, st.hold_cost, st.hold_start = 0, 0.0, None
            st.mode = MODE_DEF
        st.pending, st.pending_reason = None, None

        # ── 종가: 방어 모드 LOC ──
        if st.mode == MODE_DEF and not loo_miss:
            prev = c[i - (n_ma - 1): i]
            K, S = defense_prices(prev, c[i - 1], p, _ndec(c[i]))
            row["매수가"], row["매도가"] = K, S
            occupied = {l.tier for l in st.lots}
            tier = next((t for t in range(1, div + 1) if t not in occupied), None)
            cash_start = st.cash

            moc = [l for l in st.lots if i >= l.stop_idx]
            sold, reason = [], ""
            if moc:
                sold = moc
                reason = "+".join(f"방T{l.tier}MOC" for l in moc)
            elif st.lots and c[i] >= S:
                hold1 = bool(p["tier1_hold"]) and c[i - 1] > ma5[i - 1]
                sold = [l for l in st.lots if not (hold1 and l.tier == 1)]
                reason = "+".join("MA" for _ in sold)
            if sold:
                ratio = 1.0 if moc else sell_ratio
                q_tot = a_tot = pnl_tot = 0.0
                keep = [l for l in st.lots if l not in sold]
                for l in sold:
                    q = l.qty if ratio >= 1 else int(math.floor(l.qty * ratio))
                    if q <= 0:
                        keep.append(l); continue
                    amt = q * c[i]
                    pnl = amt * (1 - fee) - q * l.buy_price * (1 + fee)
                    st.cash += amt * (1 - fee)
                    q_tot += q; a_tot += amt; pnl_tot += pnl
                    trades.append({"날짜": date, "구분": "매도", "모드": MODE_DEF, "티어": f"방T{l.tier}",
                                   "가격": c[i], "수량": q, "금액": amt,
                                   "사유": "MOC" if moc else "MA", "실현손익": pnl, "전환수량": 0})
                    if q < l.qty:
                        l.qty -= q; keep.append(l)
                st.lots = sorted(keep, key=lambda l: l.tier)
                row.update(매도사유=(row["매도사유"] + " · " if row["매도사유"] else "") + reason,
                           매도량=row["매도량"] + int(q_tot), 매도금=row["매도금"] + a_tot,
                           실현손익=row["실현손익"] + pnl_tot)

            if tier is not None and c[i] <= K:
                remaining = sum(wts[t - 1] for t in range(1, div + 1) if t not in occupied)
                alloc = math.floor(cash_start * wts[tier - 1] / remaining) if remaining > 0 else 0.0
                qty = int(math.floor(alloc / K)) if K > 0 else 0
                amt = qty * c[i]
                if qty > 0 and st.cash >= amt * (1 + fee):
                    st.cash -= amt * (1 + fee)
                    st.lots.append(Lot(tier, date, c[i], K, qty, i + hold_days))
                    st.lots.sort(key=lambda l: l.tier)
                    row.update(매수티어=f"방T{tier}", 매수량=qty, 매수금=amt)
                    trades.append({"날짜": date, "구분": "매수", "모드": MODE_DEF, "티어": f"방T{tier}",
                                   "가격": c[i], "수량": qty, "금액": amt, "사유": "LOC",
                                   "실현손익": 0.0, "전환수량": 0})

        # ── 종가: 다음 거래일 시가 신호 ──
        if st.mode == MODE_HOLD:
            if disp[i] < exit_d:
                st.pending, st.pending_reason = "sell", EXIT_BREAK
            elif disp[i] > tp_d:
                st.pending, st.pending_reason = "sell", EXIT_TP
        elif (not np.isnan(disp[i]) and buy_lo < disp[i] < buy_hi
              and (not p["use_slope"] or slope[i] > 0)):
            st.pending = "buy"

        qty_all = st.hold_qty + sum(l.qty for l in st.lots)
        total = st.cash + qty_all * c[i]
        prev_total = total
        up = total / units if units > 0 else 1.0
        row.update(모드=st.mode, 보유수량=qty_all, 보유티어수=len(st.lots),
                   평가금=qty_all * c[i], 현금=st.cash, 총자산=total,
                   좌당가=up, **{"손익률(%)": (up - 1) * 100},
                   신호={"buy": "매수 신호", "sell": "매도 신호"}.get(st.pending, ""))
        rows.append(row)

    log = pd.DataFrame(rows)
    out = {"log": log, "trades": pd.DataFrame(trades),
           "equity": log.set_index("날짜")["총자산"] if len(log) else pd.Series(dtype=float)}
    if return_state:
        out["state"] = st
        out["units"] = units
        out["ind"] = ind
        out["last_i"] = n - 1
    return out


def compute_stats(log: pd.DataFrame) -> dict:
    if log is None or log.empty:
        return {}
    ta = log["총자산"].to_numpy(dtype=float)
    up = log["좌당가"].to_numpy(dtype=float)
    years = (pd.Timestamp(log["날짜"].iloc[-1]) - pd.Timestamp(log["날짜"].iloc[0])).days / 365.25
    cagr = (up[-1] / 1.0) ** (1 / years) - 1 if years > 0 and up[-1] > 0 else 0.0
    dd = up / np.maximum.accumulate(up) - 1
    return {"CAGR": cagr * 100, "MDD": dd.min() * 100,
            "Calmar": cagr / abs(dd.min()) if dd.min() < 0 else float("nan"),
            "최종자산": ta[-1], "수익률": (up[-1] - 1) * 100, "기간(년)": years,
            "홀딩일비중": float((log["모드"] == MODE_HOLD).mean() * 100)}


def yearly_returns(log: pd.DataFrame) -> pd.DataFrame:
    """연도별 수익률(좌당가 기준) · 연중 MDD · 같은 기간 종목 수익률 · 홀딩일 비중."""
    if log is None or log.empty:
        return pd.DataFrame()
    d = log.copy()
    d["연도"] = pd.to_datetime(d["날짜"]).dt.year
    rows, prev_up, prev_px = [], 1.0, float(d["종가"].iloc[0])
    for y, g in d.groupby("연도"):
        up = g["좌당가"].to_numpy(dtype=float)
        path = np.concatenate([[prev_up], up])
        rows.append({"연도": int(y),
                     "수익률(%)": round((up[-1] / prev_up - 1) * 100, 1),
                     "MDD(%)": round((path / np.maximum.accumulate(path) - 1).min() * 100, 1),
                     "종목(%)": round((float(g["종가"].iloc[-1]) / prev_px - 1) * 100, 1),
                     "홀딩일(%)": round(float((g["모드"] == MODE_HOLD).mean() * 100), 0),
                     "연말 자산": round(float(g["총자산"].iloc[-1]), 0)})
        prev_up, prev_px = up[-1], float(g["종가"].iloc[-1])
    return pd.DataFrame(rows)


def trigger_close(closes, ma_window: int, level_pct: float) -> float:
    """다음 거래일 종가가 얼마면 이격도가 정확히 level_pct% 가 되는가."""
    n = int(ma_window)
    arr = np.asarray(closes, dtype=float)
    if len(arr) < n - 1:
        return float("nan")
    return ma_target_close(arr[-(n - 1):], n, level_pct)


def build_today_orders(df: pd.DataFrame, P: dict | None = None,
                       initial_capital: float = 10000.0, start=None,
                       capital_injections: dict | None = None) -> dict:
    """계좌 시작일부터 최신 확정 종가까지 모델을 굴린 뒤 **다음 거래일 주문**을 만든다.

    Returns dict:
        base_date, close, mode, disp, slope, ma — 기준일 지표
        orders — [{구분, 방법, 티어, 가격, 수량, 예상금액, 비고}]
        lots   — 방어 보유 티어 · hold_qty/hold_cost — 홀딩 보유
        cash, total, unit_price, return_pct
        signal — 다음 시가 주문 ("buy"/"sell"/None)
        trig   — 다음 종가 트리거 (exit/tp/buy_lo/buy_hi)
        log, trades
    """
    p = norm_params(P)
    r = run_backtest(df, p, initial_capital, start=start, return_state=True,
                     capital_injections=capital_injections)
    log = r["log"]
    if log.empty:
        return {"orders": [], "log": log, "trades": r["trades"], "error": "데이터 부족"}
    st: State = r["state"]
    ind, i = r["ind"], r["last_i"]
    c = ind["Close"].to_numpy()
    close = float(c[i])
    n_ma, div, wts = int(p["sf_ma_base"]), p["sf_div"], list(p["sf_weights"])
    cash = float(st.cash)
    for d, amt in (capital_injections or {}).items():     # 마지막 종가 이후(장 종료 후) 조정분
        if pd.Timestamp(d).normalize() >= ind.index[i]:
            cash += float(amt)
    orders = []
    prev = c[i - (n_ma - 2): i + 1]                        # 내일 기준 직전 (MA기준−1)일 종가

    def def_buy(cash_base: float, occupied: set, est: bool = False):
        tier = next((t for t in range(1, div + 1) if t not in occupied), None)
        if tier is None:
            return
        K, _S = defense_prices(prev, close, p, 2)
        remaining = sum(wts[t - 1] for t in range(1, div + 1) if t not in occupied)
        alloc = math.floor(cash_base * wts[tier - 1] / remaining) if remaining > 0 else 0
        qty = int(math.floor(alloc / K)) if K > 0 else 0
        if qty <= 0:
            return
        note = (f"min({n_ma}일선 {p['sf_buy1']:+g}%, 전일종가 {p['sf_buy2']:+g}%) = ${K:,.2f} "
                f"이하 종가면 체결 · 티어{tier} 비중 {wts[tier - 1]:g}/{remaining:g} "
                f"할당 ${alloc:,.0f} ÷ ${K:,.2f}")
        if est:
            note += " · ⚠️ 시가 매도 체결 후 실제 현금으로 수량 재계산"
        orders.append({"구분": "매수", "방법": "LOC", "티어": f"방T{tier}", "가격": K,
                       "수량": qty, "예상금액": qty * K, "비고": note})

    if st.pending == "buy":
        limit = floor_px(close * (1 + p["loo_pct"] / 100.0), 2)
        qty = int(math.floor(cash / limit)) if limit > 0 else 0
        conv = sum(l.qty for l in st.lots)
        note = (f"매수 조건 충족(이격도 {ind['이격도'].iloc[i] * 100:+.2f}%) → 시가 전량 매수 · "
                f"지정가 = 전일종가 ${close:,.2f} × (1+{p['loo_pct']:g}%) · "
                f"현금 ${cash:,.0f} ÷ ${limit:,.2f}")
        if conv:
            note += f" · 방어 {conv:,}주는 팔지 않고 홀딩으로 전환"
        orders.append({"구분": "매수", "방법": "LOO", "티어": MODE_HOLD, "가격": limit,
                       "수량": qty, "예상금액": qty * close, "비고": note})
    elif st.pending == "sell":
        orders.append({"구분": "매도", "방법": "MOO", "티어": MODE_HOLD, "가격": None,
                       "수량": int(st.hold_qty), "예상금액": st.hold_qty * close,
                       "비고": f"{st.pending_reason} → 시가 전량 매도 후 방어 모드"})
        def_buy(cash + st.hold_qty * close, set(), est=True)
    elif st.mode == MODE_DEF:
        _K, S = defense_prices(prev, close, p, 2)
        moc = [l for l in st.lots if i + 1 >= l.stop_idx]
        if moc:
            for l in moc:
                orders.append({"구분": "매도", "방법": "MOC", "티어": f"방T{l.tier}", "가격": None,
                               "수량": l.qty, "예상금액": l.qty * close,
                               "비고": f"보유 {p['sf_hold']}일 만기 → 종가 청산 (이 날은 MA 매도 보류)"})
        elif st.lots:
            hold1 = bool(p["tier1_hold"]) and close > float(ind["MA5"].iloc[i])
            sell = [l for l in st.lots if not (hold1 and l.tier == 1)]
            q = sum(l.qty for l in sell)
            if q > 0:
                note = (f"{n_ma}일선 {p['sf_sell']:+g}% = ${S:,.2f} "
                        "이상 종가면 방어분 일괄 매도")
                if hold1 and any(l.tier == 1 for l in st.lots):
                    note += " (방T1 보류: 전일종가 > MA5)"
                orders.append({"구분": "매도", "방법": "LOC",
                               "티어": "+".join(f"방T{l.tier}" for l in sell),
                               "가격": S, "수량": q, "예상금액": q * S, "비고": note})
        def_buy(cash, {l.tier for l in st.lots})

    closes = c[: i + 1]
    mw = int(p["ma_window"])
    trig = {"exit": trigger_close(closes, mw, p["exit_disp"]),
            "tp": trigger_close(closes, mw, p["tp_disp"]),
            "buy_lo": trigger_close(closes, mw, p["buy_lo"]),
            "buy_hi": trigger_close(closes, mw, p["buy_hi"])}
    qty_all = st.hold_qty + sum(l.qty for l in st.lots)
    total = cash + qty_all * close
    return {
        "base_date": ind.index[i], "close": close,
        "mode": st.mode, "signal": st.pending, "signal_reason": st.pending_reason,
        "disp": float(ind["이격도"].iloc[i]), "slope": float(ind["Slope"].iloc[i]),
        "ma": float(ind["MA"].iloc[i]), "ma5": float(ind["MA5"].iloc[i]),
        "orders": orders, "trig": trig,
        "lots": [{"티어": f"방T{l.tier}", "매수일": l.buy_date, "매수가": l.buy_price,
                  "수량": l.qty, "남은일": l.stop_idx - i} for l in st.lots],
        "hold_qty": int(st.hold_qty), "hold_cost": float(st.hold_cost),
        "hold_start": st.hold_start,
        "cash": cash, "total": total,
        "unit_price": float(log["좌당가"].iloc[-1]),
        "return_pct": float(log["손익률(%)"].iloc[-1]),
        "log": log, "trades": r["trades"],
    }


def build_order_text(ticker: str, od: dict, next_day, acct: str = "") -> str:
    """텔레그램용 주문표 텍스트."""
    head = f"🛳️ 듀얼스나이퍼 TQ — {ticker}" + (f" [{acct}]" if acct else "")
    lines = [head,
             f"주문일 {next_day} · 기준종가 {pd.Timestamp(od['base_date']).date()} ${od['close']:,.2f}",
             f"모드 {od['mode']} · 이격도 {od['disp'] * 100:+.2f}% · 200MA ${od['ma']:,.2f}", ""]
    if od["orders"]:
        for o in od["orders"]:
            px = "시장가" if o["가격"] is None else f"${o['가격']:,.2f}"
            lines.append(f"{'🔴' if o['구분'] == '매도' else '🟢'} {o['방법']} {o['구분']} "
                         f"{o['티어']} · {px} · {o['수량']:,}주")
    else:
        lines.append("주문 없음 — " + ("홀딩 유지" if od["mode"] == MODE_HOLD else "관망"))
    lines += ["", f"총자산 ${od['total']:,.0f} · 현금 ${od['cash']:,.0f} · "
                  f"손익률 {od['return_pct']:+.2f}%"]
    return "\n".join(lines)
