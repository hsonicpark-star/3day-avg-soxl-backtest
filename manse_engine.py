"""
manse_engine.py — 쪼꼬야옹 만능 스위치(만능시트) 백테스트 엔진

원본: "쪼꼬야옹 만능 스위치 BT 버전 v2.1" 구글시트 (RECORD / DB 탭)
시트의 수식을 1:1로 이식한 결정론적 시뮬레이터.

── 전략 개요 ───────────────────────────────────────────────
1) 주(週) 단위로 시장 "모드"를 바닥 / 중간 / 천장 3구간으로 판정한다.
   판정 기준(모드 판단 기준)은 3가지 중 택1:
     · 중심주가 : 지표종목 주봉종가 / 중심주가(월복리 추세선) - 1  → 이격도
     · 이평선   : 지표종목 주봉종가 / N일 이동평균 - 1             → 이격도
     · RSI      : 지표종목 주봉 14기간 RSI
   ※ N주차의 모드는 N-1주차 지표로 결정 (미래참조 없음)

2) 구간(바닥/중간/천장)마다 독립적인 파라미터 세트를 갖는다.
     · 시드분할수 / 정량매수 / 매도날 매수X / MOC날 매수
     · 티어별: 시드 비중 · 매수목표 · 매도목표 · 손절일수

3) 매일:
     티어 = 현재 보유 포지션 수 기반으로 결정 (보유 / 빈자리 방식)
     매수주문가 = ROUNDDOWN(전일종가 x (1 + 매수목표), 2)
     체결       = 주문가 >= 당일종가 이면 당일종가로 체결 (LOC)
     매도목표가 = ROUNDUP(체결가 x (1 + 매도목표), 2)
     매도       = 손절예정일 전에 종가가 매도목표 도달 → 그날 매도
                  미도달 → 손절예정일(= 매수일 + 손절일수 영업일)에 MOC 청산

4) 투자금은 실현손익에 이익/손실 복리율을 곱해 주기적으로 갱신한다.

── 공개 API ────────────────────────────────────────────────
    ManseParams / LevelParam / TierParam      파라미터 컨테이너
    build_weekly_closes(daily)                일봉 → 주봉(주간 마지막 거래일 종가)
    build_mode_frame(params, prices)          주차별 지표·모드 테이블
    build_mode_by_week(...)                   (isoyear, isoweek) → 모드 dict
    run_backtest(prices, params, ...)         백테스트 실행
    build_order_plan(...)                     오늘의 주문 계산
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field, asdict

import numpy as np
import pandas as pd

LEVELS = ("바닥", "중간", "천장")
MODE_BASES = ("중심주가", "이평선", "RSI")
TIER_METHODS = ("보유", "빈자리")
ORDER_TYPES = ("추가 주문 건수 고정", "추가 매수 갯수 고정")

# 중심주가 추세선 프리셋: (2016-01 기준가, 월복리율)
CENTER_PRESETS = {
    "SOXX": (31.224157, 1.018129),
    "QQQ": (100.31, 1.0132),
}
CENTER_BASE_YEAR = 2016
CENTER_BASE_MONTH = 1


# ══════════════════════════════════════════════════════════
# 반올림 헬퍼 (구글시트 ROUNDDOWN / ROUNDUP / INT 대응)
# ══════════════════════════════════════════════════════════
def _g15(x: float) -> float:
    """구글시트는 연산 결과를 유효숫자 15자리로 정규화한 뒤 INT/ROUND 를 적용한다.

    이 정규화가 없으면 20813.28/1.31 이 double 오차로 15887.999999999998 이 되어
    INT() 결과가 시트(15888)와 1 씩 어긋난다 — 수량이 어긋나면 예수금이
    갈라지며 이후 전체 시뮬레이션이 조금씩 벌어진다.
    """
    try:
        return float(f"{x:.15g}")
    except (ValueError, OverflowError):
        return x


def sheet_int(x: float) -> int:
    """구글시트 INT() (음수는 내림)."""
    return int(math.floor(_g15(x)))


def floor2(x: float) -> float:
    return math.floor(_g15(x * 100.0)) / 100.0


def ceil2(x: float) -> float:
    return math.ceil(_g15(x * 100.0)) / 100.0


def round2(x: float) -> float:
    return float(np.round(x, 2))


# ══════════════════════════════════════════════════════════
# 파라미터
# ══════════════════════════════════════════════════════════
@dataclass
class TierParam:
    """구간 내 티어 1개의 파라미터."""
    seed_w: float = 1.0        # 시드 비중 (0~1)
    buy_gap: float = -0.01     # 매수목표 (전일종가 대비 %)
    sell_gap: float = 0.02     # 매도목표 (체결가 대비 %)
    stop_days: int = 20        # 손절일수 (영업일)


@dataclass
class LevelParam:
    """바닥/중간/천장 한 구간의 파라미터 세트."""
    split: int = 2                     # 시드분할수 (= 최대 티어 수)
    fixed_qty: bool = False            # 정량매수 (분할주문 없이 SEED/주문가)
    no_buy_on_sell_day: bool = False   # 매도날 매수X
    moc_day_buy: bool = True           # MOC(손절)날 매수 허용
    tiers: list = field(default_factory=list)   # List[TierParam]

    def tier(self, n: int):
        """1-based 티어 번호 → TierParam (없으면 None)."""
        if 1 <= n <= len(self.tiers):
            return self.tiers[n - 1]
        return None


@dataclass
class ManseParams:
    # ── 투자 정보 입력 ──
    ticker: str = "SOXL"
    principal: float = 10000.0     # 최초 원금
    profit_comp: float = 0.67      # 이익 복리율
    loss_comp: float = 0.44        # 손실 복리율
    renew_cycle: int = 1           # 투자금 갱신 주기 (거래일)
    fee: float = 0.0               # 수수료율
    sec_fee: float = 0.0000278     # SEC FEE (매도시)
    extra_range: float = -0.20     # 추가 매수 범위 (주문가 대비 하단)
    order_type: str = "추가 주문 건수 고정"
    extra_step: int = 2            # 추가 매수 간격 (갯수 고정 방식)
    extra_count: int = 4           # 추가 주문 건수 (건수 고정 방식)

    # ── 스위치 세팅 ──
    mode_basis: str = "이평선"     # 중심주가 / 이평선 / RSI
    center_ticker: str = "QQQ"     # 중심주가 종목
    center_low: float = 0.055      # 바닥 범위
    center_high: float = 0.17      # 천장 범위
    ma_ticker: str = "QQQ"         # 이평선 종목
    ma_days: int = 120             # 이평선 일수
    ma_low: float = -0.0125        # 바닥 범위
    ma_high: float = 0.0575        # 천장 범위
    rsi_ticker: str = "QQQ"        # RSI 종목
    rsi_period: int = 14
    rsi_low: float = 40.0          # 바닥 RSI
    rsi_high: float = 65.0         # 천장 RSI
    tier_method: str = "보유"      # 티어계산 방식: 보유 / 빈자리

    # ── 구간별 파라미터 ──
    levels: dict = field(default_factory=dict)   # {"바닥": LevelParam, ...}

    # ── 파생 ──
    def level(self, name: str):
        return self.levels.get(name)

    def indicator_ticker(self) -> str:
        return {"중심주가": self.center_ticker,
                "이평선": self.ma_ticker,
                "RSI": self.rsi_ticker}.get(self.mode_basis, self.ma_ticker)

    def needed_tickers(self) -> list:
        tks = {self.ticker.upper(), self.indicator_ticker().upper()}
        return sorted(tks)


def default_params() -> ManseParams:
    """시트 기본값(이평선-2티어) 그대로의 파라미터."""
    return ManseParams(
        levels={
            "바닥": LevelParam(split=2, tiers=[
                TierParam(0.01, -0.008, 0.015, 36),
                TierParam(0.99, 0.079, 0.032, 1),
            ]),
            "중간": LevelParam(split=2, tiers=[
                TierParam(0.01, 0.084, 0.027, 38),
                TierParam(0.99, 0.028, 0.027, 4),
            ]),
            "천장": LevelParam(split=2, tiers=[
                TierParam(0.01, -0.011, 0.006, 25),
                TierParam(0.99, 0.076, 0.006, 1),
            ]),
        }
    )


def params_to_dict(p: ManseParams) -> dict:
    d = asdict(p)
    d["levels"] = {
        k: {"split": v.split, "fixed_qty": v.fixed_qty,
            "no_buy_on_sell_day": v.no_buy_on_sell_day,
            "moc_day_buy": v.moc_day_buy,
            "tiers": [asdict(t) for t in v.tiers]}
        for k, v in p.levels.items()
    }
    return d


def params_from_dict(d: dict) -> ManseParams:
    d = dict(d or {})
    lv = d.pop("levels", {}) or {}
    p = ManseParams(**{k: v for k, v in d.items()
                       if k in ManseParams.__dataclass_fields__})
    p.levels = {}
    for name in LEVELS:
        raw = lv.get(name) or {}
        tiers = [TierParam(**{k: t[k] for k in ("seed_w", "buy_gap", "sell_gap", "stop_days")
                              if k in t})
                 for t in (raw.get("tiers") or [])]
        p.levels[name] = LevelParam(
            split=int(raw.get("split", 2)),
            fixed_qty=bool(raw.get("fixed_qty", False)),
            no_buy_on_sell_day=bool(raw.get("no_buy_on_sell_day", False)),
            moc_day_buy=bool(raw.get("moc_day_buy", True)),
            tiers=tiers,
        )
    if not any(p.levels[n].tiers for n in LEVELS):
        return default_params()
    return p


# ══════════════════════════════════════════════════════════
# 지표 계산
# ══════════════════════════════════════════════════════════
def build_weekly_closes(daily: pd.Series) -> pd.Series:
    """일봉 종가 → 주봉 종가 (각 주의 마지막 거래일 종가, 인덱스=그 거래일).

    시트의 '주말(AE)' = 해당 주의 마지막 영업일, 주봉종가 = 그 날의 종가와 동일.
    """
    s = pd.Series(daily).dropna().sort_index()
    if s.empty:
        return s
    iso = s.index.isocalendar()
    key = list(zip(iso["year"].astype(int), iso["week"].astype(int)))
    tmp = pd.DataFrame({"close": s.values, "key": key}, index=s.index)
    last = tmp.groupby("key", sort=False).tail(1)
    return pd.Series(last["close"].values, index=last.index, name="weekly_close")


def center_price(ts, ticker: str = "QQQ", base: float = None,
                 rate: float = None) -> float:
    """중심주가(월복리 추세선) 값. 시트: base * rate^(12*(Y-2016)+(M-1))"""
    if base is None or rate is None:
        preset = CENTER_PRESETS.get(str(ticker).upper())
        if preset is None:
            return float("nan")
        base, rate = preset
    ts = pd.Timestamp(ts)
    n = 12 * (ts.year - CENTER_BASE_YEAR) + (ts.month - CENTER_BASE_MONTH)
    return base * (rate ** n)


def simple_rsi(weekly_close: pd.Series, period: int = 14) -> pd.Series:
    """시트와 동일한 단순평균(SMA) 기반 RSI.

        상승폭 = max(diff, 0), 하락폭 = min(diff, 0)   (부호 유지)
        RSI    = 상승평균 / (상승평균 - 하락평균) * 100
    """
    s = pd.Series(weekly_close).astype(float)
    d = s.diff()
    up = d.clip(lower=0.0)
    dn = d.clip(upper=0.0)
    au = up.rolling(period).mean()
    ad = dn.rolling(period).mean()
    denom = au - ad
    rsi = np.where(denom.abs() < 1e-12, np.nan, au / denom * 100.0)
    return pd.Series(rsi, index=s.index)


def _classify(v, low, high) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)) or pd.isna(v):
        return ""
    if v < low:
        return "바닥"
    if v <= high:
        return "중간"
    return "천장"


def build_mode_frame(p: ManseParams, prices: dict) -> pd.DataFrame:
    """주차별 지표/모드 테이블.

    prices: {"SOXL": DataFrame(Close), "QQQ": DataFrame(Close), ...}
    반환 컬럼: 주말, 주봉종가, 기준값, 이격도(또는 RSI), 판정, 모드
              · '판정'  = 그 주 지표로 매긴 구간
              · '모드'  = 실제 적용 모드 (= 직전 주의 판정, 1주 shift)
    """
    tk = p.indicator_ticker().upper()
    src = prices.get(tk)
    if src is None or len(src) == 0:
        return pd.DataFrame()
    close = src["Close"] if "Close" in getattr(src, "columns", []) else pd.Series(src)
    wk = build_weekly_closes(close)
    if wk.empty:
        return pd.DataFrame()

    out = pd.DataFrame({"주봉종가": wk.round(2)})
    out.index.name = "주말"

    if p.mode_basis == "중심주가":
        ref = pd.Series([center_price(d, tk) for d in wk.index], index=wk.index)
        gap = out["주봉종가"] / ref - 1.0
        low, high = p.center_low, p.center_high
        ref_name, gap_name = "중심주가", "이격도"
    elif p.mode_basis == "RSI":
        ref = pd.Series(np.nan, index=wk.index)
        gap = simple_rsi(wk, p.rsi_period)
        low, high = p.rsi_low, p.rsi_high
        ref_name, gap_name = "-", "wRSI"
    else:  # 이평선
        ma_daily = close.rolling(p.ma_days).mean().round(2)
        ref = ma_daily.reindex(wk.index)
        gap = out["주봉종가"] / ref - 1.0
        low, high = p.ma_low, p.ma_high
        ref_name, gap_name = f"MA{p.ma_days}", "이격도"

    out[ref_name] = ref.values
    out[gap_name] = gap.values
    out["판정"] = [_classify(v, low, high) for v in gap.values]
    out["모드"] = out["판정"].shift(1).fillna("")
    out.attrs["ref_name"] = ref_name
    out.attrs["gap_name"] = gap_name
    return out


def build_mode_by_week(mode_frame: pd.DataFrame) -> dict:
    """주말 인덱스 기반 mode_frame → {(isoyear, isoweek): 모드} dict."""
    if mode_frame is None or len(mode_frame) == 0:
        return {}
    iso = mode_frame.index.isocalendar()
    keys = list(zip(iso["year"].astype(int), iso["week"].astype(int)))
    return {k: m for k, m in zip(keys, mode_frame["모드"].tolist())}


def week_key(ts) -> tuple:
    ts = pd.Timestamp(ts)
    iso = ts.isocalendar()
    return (int(iso[0]), int(iso[1]))


# ══════════════════════════════════════════════════════════
# 수량 계산 (시트 AJ열)
# ══════════════════════════════════════════════════════════
def calc_qty(seed: float, bid: float, close: float, tier: int,
             lp: LevelParam, p: ManseParams) -> int:
    """분할 주문 체결 수량.

    최대 티어(=시드분할수)이거나 '정량매수'이면 SEED/주문가 단순 계산.
    그 외에는 주문가에서 '추가 매수 범위'까지 사다리 주문을 깔고,
    평균단가가 당일 종가 이상으로 유지되는 구간까지만 체결된 것으로 본다.
    """
    if bid <= 0 or seed <= 0:
        return 0
    q0 = sheet_int(seed / bid)
    if q0 <= 0:
        return 0
    if tier >= lp.split or lp.fixed_qty:
        return q0

    bot = floor2(bid * (1.0 + p.extra_range))
    if bot <= 0:
        return q0

    if p.order_type == "추가 주문 건수 고정":
        lim = min(50, int(p.extra_count))
        if lim <= 0:
            return q0
        step = sheet_int((seed / bot - seed / bid) / max(int(p.extra_count), 1))
    else:  # 추가 매수 갯수 고정
        lim = 50
        step = int(p.extra_step)
        if step <= 1:
            return sheet_int(seed / close) if close > 0 else 0

    if step <= 0:
        return q0

    qty = q0
    for k in range(1, lim):
        q = q0 + k * step
        px = seed / q
        if px >= bot and px >= close:
            qty += step
        else:
            break
    return qty


# ══════════════════════════════════════════════════════════
# 백테스트
# ══════════════════════════════════════════════════════════
def run_backtest(prices: dict, p: ManseParams, start=None, end=None,
                 mode_frame: pd.DataFrame = None,
                 cash_flows: dict = None, light: bool = False) -> dict:
    """만능시트 백테스트.

    prices     : {티커: DataFrame(index=Date, columns=['Close'])}
    p          : ManseParams
    start, end : 백테스트 기간 (미지정 시 전체)
    mode_frame : 사전 계산된 모드 테이블 (없으면 내부에서 생성)
    cash_flows : {Timestamp: {"seed_change": x, "deposit": y}} — 시드증감/입출금
    light      : True 면 일별 로그/매매기록 DataFrame 을 만들지 않고
                 요약 지표만 반환 (최적화용 — 약 4배 빠름)

    반환 dict:
        df       : 일별 시뮬레이션 로그 (시트 RECORD 탭 재현)
        trades   : 체결 단위 매매 기록
        metrics  : 요약 성과 지표
        by_level / by_tier : 구간별·티어별 통계
        mode_frame
    """
    tk = p.ticker.upper()
    src = prices.get(tk)
    if src is None or len(src) == 0:
        return {"error": f"{tk} 가격 데이터가 없습니다."}

    if mode_frame is None:
        mode_frame = build_mode_frame(p, prices)
    mode_by_week = build_mode_by_week(mode_frame)

    close_s = (src["Close"] if "Close" in getattr(src, "columns", []) else pd.Series(src))
    close_s = pd.Series(close_s).dropna().sort_index().round(2)

    full_idx = close_s.index
    if start is not None:
        sel = full_idx >= pd.Timestamp(start)
    else:
        sel = np.ones(len(full_idx), dtype=bool)
    if end is not None:
        sel = sel & (full_idx <= pd.Timestamp(end))
    idx_positions = np.flatnonzero(sel)
    if len(idx_positions) == 0:
        return {"error": "선택한 기간에 거래일이 없습니다."}

    first_pos = int(idx_positions[0])
    dates = full_idx[idx_positions]
    closes = close_s.values[idx_positions]
    n = len(dates)

    # 시작 전일 종가 (PRECLOSE)
    prev_close0 = float(close_s.values[first_pos - 1]) if first_pos > 0 else float(closes[0])

    cf = cash_flows or {}

    # ── 상태 ──
    cash = float(p.principal)
    seed_base = float(p.principal)     # BE 투자금갱신
    holdings = 0
    active = []                        # 보유 중 포지션
    positions = []                     # 전체 포지션 기록
    daily_realized = np.zeros(n)
    total_arr = np.zeros(n)

    rows = []

    for i in range(n):
        d = dates[i]
        c = float(closes[i])
        c_prev = prev_close0 if i == 0 else float(closes[i - 1])
        mode = mode_by_week.get(week_key(d), "")
        lp = p.levels.get(mode)

        flow = cf.get(pd.Timestamp(d).normalize(), {})
        seed_change = float(flow.get("seed_change", 0.0))
        deposit = float(flow.get("deposit", 0.0))

        # ── 티어 결정 (전일 기준 보유 포지션) ──
        held_tiers = [ps["tier"] for ps in active]
        if lp is None or lp.split <= 0:
            tier = None
        elif p.tier_method == "보유":
            tier = len(held_tiers) + 1
        else:  # 빈자리
            occupied = set(held_tiers)
            free = [t for t in range(1, lp.split + 1) if t not in occupied]
            tier = free[0] if free else len(held_tiers) + 1

        tp = lp.tier(tier) if (lp is not None and tier is not None) else None
        over_split = (lp is not None and tier is not None and tier > lp.split)

        # ── 1회 시드 (AB) ──
        seed = None
        if tp is not None and not over_split:
            seed = min(seed_base * tp.seed_w, cash)

        # ── 매수 주문가 (AC) ──
        bid = None
        if seed is not None and seed > 0:
            odp = floor2(c_prev * (1.0 + tp.buy_gap))
            if odp > seed:
                bid = None
            elif lp.no_buy_on_sell_day:
                wt = [ps["sell_target"] for ps in active]
                mocd = sum(1 for ps in active if ps["stop_i"] == i)
                if not wt and mocd == 0:
                    bid = odp
                elif (wt and mocd == 0) or (lp.moc_day_buy and mocd > 0):
                    bid = min(odp, min(wt) - 0.01) if wt else odp
                else:
                    bid = None
            else:
                bid = odp

        # ── 체결 (AI / AJ) ──
        buy_px = None
        qty = 0
        if bid is not None and seed is not None and seed >= c and bid >= c and c > 0:
            buy_px = c
            qty = calc_qty(seed, bid, c, tier, lp, p)
            if qty <= 0:
                buy_px = None
                qty = 0

        buy_amt = buy_fee = 0.0
        new_pos = None
        if buy_px is not None:
            buy_amt = buy_px * qty
            buy_fee = buy_amt * p.fee
            sell_target = ceil2(buy_px * (1.0 + tp.sell_gap))
            stop_i = i + int(tp.stop_days)
            sell_i = None
            hi = min(stop_i, n)
            for j in range(i + 1, hi):
                if closes[j] >= sell_target:
                    sell_i = j
                    break
            if sell_i is None and stop_i < n:
                sell_i = stop_i
            new_pos = {
                "tier": tier, "level": mode,
                "buy_i": i, "buy_date": d, "buy_px": buy_px, "qty": qty,
                "buy_amt": buy_amt, "buy_fee": buy_fee,
                "sell_target": sell_target,
                "stop_i": stop_i,
                # 손절일이 데이터 끝을 넘어가도 날짜를 계산해 둔다.
                # None 으로 두면 주문표에서 MOC 청산 판정이 실패한다
                # (마지막 날 매수 + 손절일수 1 → 다음 거래일이 곧 손절일).
                "stop_date": (dates[stop_i] if stop_i < n
                              else _extend_trading_day(dates[-1],
                                                       stop_i - (n - 1))),
                "sell_i": sell_i,
                "sell_date": dates[sell_i] if sell_i is not None else None,
                "sell_px": float(closes[sell_i]) if sell_i is not None else None,
            }
            if sell_i is not None:
                sp = new_pos["sell_px"]
                new_pos["sell_amt"] = sp * qty
                new_pos["sell_fee"] = new_pos["sell_amt"] * (p.fee + p.sec_fee)
                new_pos["pnl"] = round(new_pos["sell_amt"] - new_pos["sell_fee"]
                                       - (buy_amt + buy_fee), 2)
                new_pos["ret"] = sp / buy_px - 1.0
                new_pos["exit"] = "목표" if sell_i < stop_i else "MOC"
            else:
                new_pos["sell_amt"] = new_pos["sell_fee"] = None
                new_pos["pnl"] = None
                new_pos["ret"] = None
                new_pos["exit"] = "보유중"
            positions.append(new_pos)
            active.append(new_pos)
            holdings += qty

        # ── 당일 매도 청산 ──
        sold_amt = 0.0
        sold_qty = 0
        realized = 0.0
        still = []
        for ps in active:
            if ps["sell_i"] == i:
                sold_amt += ps["sell_amt"] - ps["sell_fee"]
                sold_qty += ps["qty"]
                realized += ps["pnl"]
            else:
                still.append(ps)
        active = still
        holdings -= sold_qty
        realized = round(realized, 2)
        daily_realized[i] = realized

        # ── 현금 / 자산 ──
        cash = round(cash - buy_amt - buy_fee + sold_amt + seed_change + deposit, 2)
        equity = round(holdings * c, 2)
        total = round(equity + cash, 2)
        total_arr[i] = total

        # ── 투자금 갱신 (BC/BD/BE) ──
        cycle = max(int(p.renew_cycle), 1)
        renew = ((i + 1) % cycle == 0)
        comp_amt = 0.0
        if i == 0:
            seed_base = p.principal + realized * p.profit_comp + seed_change
        else:
            if renew:
                lo = max(0, i - cycle + 1)
                bfs = float(daily_realized[lo:i + 1].sum())
                comp_amt = bfs * (p.loss_comp if bfs < 0 else p.profit_comp)
            seed_base = seed_base + comp_amt + seed_change

        if light:
            continue

        rows.append({
            "날짜": d, "모드": mode, "티어": tier,
            "매수목표": tp.buy_gap if tp else None,
            "매도목표": tp.sell_gap if tp else None,
            "손절일수": tp.stop_days if tp else None,
            "1회시드": seed, "매수주문가": bid, "종가": c,
            "등락률": (c / c_prev - 1.0) if i > 0 or first_pos > 0 else None,
            "매수체결": buy_px, "수량": qty if buy_px is not None else None,
            "매수대금": buy_amt if buy_px is not None else None,
            "매도목표가": new_pos["sell_target"] if new_pos else None,
            "손절예정": new_pos["stop_date"] if new_pos else None,
            "매도일": new_pos["sell_date"] if new_pos else None,
            "매도체결": new_pos["sell_px"] if new_pos else None,
            "실현손익": new_pos["pnl"] if new_pos else None,
            "당일실현": realized if realized else None,
            "보유": holdings, "평가금": equity, "예수금": cash, "총자산": total,
            "복리금액": comp_amt if renew else None,
            "투자금갱신": round(seed_base, 2),
        })

    if light:
        return _light_metrics(total_arr, positions, p, dates)

    df = pd.DataFrame(rows).set_index("날짜")

    # ── 매매 기록 ──
    tr = []
    for ps in positions:
        tr.append({
            "매수일": ps["buy_date"], "구간": ps["level"], "티어": ps["tier"],
            "매수가": ps["buy_px"], "수량": ps["qty"], "매수대금": ps["buy_amt"],
            "매도목표가": ps["sell_target"], "손절예정": ps["stop_date"],
            "매도일": ps["sell_date"], "매도가": ps["sell_px"],
            "청산": ps["exit"], "보유일": (
                (ps["sell_i"] - ps["buy_i"]) if ps["sell_i"] is not None else None),
            "실현손익": ps["pnl"], "수익률": ps["ret"],
        })
    trades = pd.DataFrame(tr)

    metrics = _calc_metrics(df, trades, p, dates)
    by_level, by_tier = _calc_breakdown(trades)

    return {"df": df, "trades": trades, "metrics": metrics,
            "by_level": by_level, "by_tier": by_tier,
            "mode_frame": mode_frame, "params": p}


def _light_metrics(total_arr, positions, p: ManseParams, dates) -> dict:
    """최적화용 경량 지표 (DataFrame 생성 없음)."""
    final = float(total_arr[-1])
    principal = float(p.principal)
    years = max((dates[-1] - dates[0]).days / 365.25, 1e-9)
    cagr = (final / principal) ** (1 / years) - 1 if principal > 0 and final > 0 else np.nan
    peak = np.maximum.accumulate(total_arr)
    mdd = float((total_arr / np.where(peak == 0, np.nan, peak) - 1.0).min())

    pnl = [ps["pnl"] for ps in positions if ps["pnl"] is not None]
    cnt = len(pnl)
    wins = [x for x in pnl if x > 0]
    loss = [x for x in pnl if x < 0]
    aw = float(np.mean(wins)) if wins else 0.0
    al = float(np.mean(loss)) if loss else 0.0
    calmar = abs(cagr / mdd) if (mdd and not np.isnan(cagr)) else np.nan
    return {
        "최종자산": final,
        "총수익률": final / principal - 1 if principal else np.nan,
        "CAGR": cagr,
        "MDD": mdd,
        "Calmar": calmar,
        "거래횟수": cnt,
        "승률": (len(wins) / cnt) if cnt else 0.0,
        "손익비": abs(aw / al) if al else np.nan,
        "누적실현": float(sum(pnl)),
    }


def _calc_metrics(df: pd.DataFrame, trades: pd.DataFrame,
                  p: ManseParams, dates) -> dict:
    total = df["총자산"]
    final = float(total.iloc[-1])
    principal = float(p.principal)
    years = max((dates[-1] - dates[0]).days / 365.25, 1e-9)
    cagr = (final / principal) ** (1 / years) - 1 if principal > 0 and final > 0 else np.nan
    peak = total.cummax()
    dd = total / peak - 1.0
    mdd = float(dd.min()) if len(dd) else 0.0

    closed = trades[trades["실현손익"].notna()] if len(trades) else trades
    cnt = len(closed)
    wins = closed[closed["실현손익"] > 0] if cnt else closed
    loss = closed[closed["실현손익"] < 0] if cnt else closed
    winrate = len(wins) / cnt if cnt else 0.0
    avg_w = float(wins["실현손익"].mean()) if len(wins) else 0.0
    avg_l = float(loss["실현손익"].mean()) if len(loss) else 0.0
    pl_ratio = abs(avg_w / avg_l) if avg_l else np.nan

    # 일간 수익률 기반 샤프
    dr = total.pct_change().dropna()
    sharpe = (dr.mean() / dr.std() * np.sqrt(252)) if len(dr) > 1 and dr.std() else np.nan

    return {
        "최종자산": final,
        "총수익률": final / principal - 1 if principal else np.nan,
        "CAGR": cagr,
        "MDD": mdd,
        "샤프": sharpe,
        "누적실현": float(df["실현손익"].sum(skipna=True)),
        "거래횟수": int(cnt),
        "승률": winrate,
        "평균수익률": float(closed["수익률"].mean()) if cnt else 0.0,
        "손익비": pl_ratio,
        "기간(년)": years,
        "미청산": int(len(trades) - cnt) if len(trades) else 0,
    }


def _calc_breakdown(trades: pd.DataFrame):
    if trades is None or len(trades) == 0:
        return pd.DataFrame(), pd.DataFrame()
    closed = trades[trades["실현손익"].notna()]
    if closed.empty:
        return pd.DataFrame(), pd.DataFrame()

    def _agg(g):
        w = g[g["실현손익"] > 0]
        l = g[g["실현손익"] < 0]
        aw = w["실현손익"].mean() if len(w) else 0.0
        al = l["실현손익"].mean() if len(l) else 0.0
        return pd.Series({
            "거래횟수": len(g),
            "승률": len(w) / len(g) if len(g) else 0.0,
            "평균수익률": g["수익률"].mean(),
            "손익합": g["실현손익"].sum(),
            "손익비": abs(aw / al) if al else np.nan,
            "평균보유일": g["보유일"].mean(),
        })

    by_level = closed.groupby("구간", sort=False).apply(
        _agg, include_groups=False).reset_index()
    by_tier = closed.groupby(["구간", "티어"], sort=False).apply(
        _agg, include_groups=False).reset_index()
    return by_level, by_tier


# ══════════════════════════════════════════════════════════
# 오늘의 주문표
# ══════════════════════════════════════════════════════════
def _extend_trading_day(last_date, steps: int) -> pd.Timestamp:
    """마지막 거래일에서 steps 거래일 뒤 날짜 (데이터 밖 구간 연장용)."""
    d = pd.Timestamp(last_date)
    for _ in range(max(int(steps), 0)):
        d = _next_trading_day(d)
    return d


def _next_trading_day(last_date) -> pd.Timestamp:
    """마지막 거래일 다음 미국 영업일(공휴일 제외).

    streamlit 없이도 동작해야 한다 — 텔레그램 자동발송(GitHub Actions)은
    streamlit 을 설치하지 않으므로 dss_engine 의 NYSE 달력을 먼저 쓰고,
    없으면 common.data, 그것도 없으면 주말만 건너뛴다.
    """
    d = pd.Timestamp(last_date).normalize() + pd.Timedelta(days=1)
    try:
        from dss_engine import _is_us_trading_day
        for _ in range(30):
            if _is_us_trading_day(d.date()):
                return d
            d += pd.Timedelta(days=1)
        return d
    except Exception:
        pass
    try:
        from common.data import next_trading_date
        return pd.Timestamp(next_trading_date(d.date()))
    except Exception:
        while d.weekday() >= 5:
            d += pd.Timedelta(days=1)
        return d



def build_order_plan(prices: dict, p: ManseParams, bt_result: dict = None,
                     capital: float = None, held: list = None) -> dict:
    """다음 거래일 주문 계산.

    bt_result 가 있으면 백테스트 마지막 상태(투자금·예수금·보유 포지션)를 이어받고,
    없으면 capital / held 인자로 직접 상태를 지정한다.

    held: [{"tier": 1, "sell_target": 12.34, "stop_date": Timestamp}, ...]
    """
    tk = p.ticker.upper()
    src = prices.get(tk)
    if src is None or len(src) == 0:
        return {"error": f"{tk} 가격 데이터가 없습니다."}
    close_s = (src["Close"] if "Close" in getattr(src, "columns", []) else pd.Series(src))
    close_s = pd.Series(close_s).dropna().sort_index().round(2)
    last_date = close_s.index[-1]
    last_close = float(close_s.values[-1])

    mode_frame = (bt_result or {}).get("mode_frame")
    if mode_frame is None:
        mode_frame = build_mode_frame(p, prices)
    mode_by_week = build_mode_by_week(mode_frame)

    nxt = _next_trading_day(last_date)

    # 다음 거래일이 속한 주의 모드 — 아직 확정 전이면 마지막 판정을 이어씀
    mode = mode_by_week.get(week_key(nxt), "")
    if not mode and len(mode_frame):
        mode = str(mode_frame["판정"].iloc[-1])

    # 상태 복원
    if bt_result and "df" in bt_result:
        bdf = bt_result["df"]
        cash = float(bdf["예수금"].iloc[-1])
        seed_base = float(bdf["투자금갱신"].iloc[-1])
        tr = bt_result.get("trades")
        open_pos = []
        if tr is not None and len(tr):
            op = tr[tr["매도일"].isna()]
            for _, r in op.iterrows():
                open_pos.append({"tier": int(r["티어"]),
                                 "qty": int(r["수량"]),
                                 "buy_px": float(r["매수가"]),
                                 "sell_target": float(r["매도목표가"]),
                                 "stop_date": r["손절예정"]})
        held = open_pos if held is None else held
    else:
        cash = float(capital if capital is not None else p.principal)
        seed_base = cash
        held = held or []

    lp = p.levels.get(mode)
    out = {"기준일": last_date, "전일종가": last_close, "주문일": nxt,
           "모드": mode, "예수금": cash, "투자금": seed_base,
           "보유": held, "orders": [], "note": "", "message": ""}

    def _stop_ts(h):
        """손절예정일 → Timestamp (미지정/NaT 이면 None)."""
        v = h.get("stop_date")
        if v is None or pd.isna(v):
            return None
        try:
            return pd.Timestamp(v).normalize()
        except (TypeError, ValueError):
            return None

    # ── 보유 포지션 매도 주문 (모드와 무관하게 항상 유지) ──
    def _add_sell_orders():
        """보유 포지션의 다음 거래일 매도 주문.

        시트의 매도일 탐색 구간은 `매수일 < S < 손절예정일` 이다.
          · 손절예정일이 곧 다음 거래일  → 탐색 구간이 비어 있음
            → 목표가 도달 여부와 무관하게 **그날 종가로 청산(MOC)**
          · 손절예정일이 더 뒤          → 목표가 도달 시 청산이므로
            **LOC 매도**(종가가 매도목표가 이상이면 체결)
        특히 손절일수 = 1 인 티어는 항상 다음날 MOC 다.
        """
        nd = nxt.normalize()
        for h in held:
            stop = _stop_ts(h)
            is_moc = stop is not None and stop <= nd
            tier = h.get("tier")
            qty = h.get("qty")
            out["orders"].append({
                "구분": (f"매도 MOC (티어{tier})" if is_moc
                        else f"매도 LOC (티어{tier})"),
                "티어": tier,
                # MOC 는 시장가 청산이라 지정가가 없다 (참고용 목표가는 별도 표기)
                "주문가": None if is_moc else h["sell_target"],
                "수량": qty,
                "금액": (None if is_moc or not qty
                        else round(h["sell_target"] * qty, 2)),
                "손절예정": stop,
                "매도목표가": h["sell_target"],
            })

    if lp is None:
        out["message"] = f"모드('{mode}') 파라미터가 없어 신규 매수를 계산할 수 없습니다."
        _add_sell_orders()
        return out

    held_tiers = [h["tier"] for h in held]
    if p.tier_method == "보유":
        tier = len(held_tiers) + 1
    else:
        occupied = set(held_tiers)
        free = [t for t in range(1, lp.split + 1) if t not in occupied]
        tier = free[0] if free else len(held_tiers) + 1

    tp = lp.tier(tier)
    out["티어"] = tier
    if tp is None or tier > lp.split:
        out["message"] = f"티어 {tier} 는 시드분할수({lp.split})를 초과 — 신규 매수 없음"
        _add_sell_orders()
        return out

    seed = min(seed_base * tp.seed_w, cash)
    odp = floor2(last_close * (1.0 + tp.buy_gap))
    bid = odp
    note = ""
    if odp > seed:
        bid = None
        note = "1회 시드로 1주도 살 수 없음"
    elif lp.no_buy_on_sell_day:
        wt = [h["sell_target"] for h in held]
        mocd = sum(1 for h in held if _stop_ts(h) == nxt.normalize())
        if not wt and mocd == 0:
            bid = odp
        elif (wt and mocd == 0) or (lp.moc_day_buy and mocd > 0):
            bid = min(odp, min(wt) - 0.01)
            note = "매도날 매수X → 최저 매도목표 -0.01 로 제한"
        else:
            bid = None
            note = "MOC(손절)일 매수 금지"

    out["1회시드"] = seed
    out["매수주문가"] = bid
    out["note"] = note
    if bid is not None:
        est_qty = calc_qty(seed, bid, bid, tier, lp, p)
        out["예상수량"] = est_qty
        out["orders"].append({
            "구분": f"매수 LOC (티어{tier})", "티어": tier, "주문가": bid,
            "수량": est_qty, "금액": round(bid * est_qty, 2),
            "손절예정": None,
        })
    _add_sell_orders()
    return out
