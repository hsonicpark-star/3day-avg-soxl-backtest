"""
stdev_engine.py — 표준편차매매 (σ-LOC) 엔진 (pure Python, no Streamlit)

웹앱(strategies/stdev.py)과 자동발송 스크립트(scripts/daily_telegram_alert.py)가
공통으로 사용하는 백테스트/주문표 엔진.

Streamlit · common.* 의존성이 없어야 함 — GitHub Actions 환경에서도 import 가능.
의존성: numpy, pandas, math, datetime
"""
from __future__ import annotations

import math
import numpy as np
import pandas as pd
from datetime import datetime


# ══════════════════════════════════════════════════════════════
# 백테스트 엔진 (full): 히스토리 + 모든 지표 반환
# ══════════════════════════════════════════════════════════════

def run_backtest_stdev(
    price_df, start_date, end_date,
    sigma_period=2, k_buy=0.55, k_sell=0.55,
    sell_ratio=75.0, divisions=5, renewal=5,
    pcr=1.0, lcr=1.0,
    initial_capital=20000.0,
    return_history=False,
):
    """
    표준편차매매 백테스트 엔진 (verify.py 검증 완료 로직 기반)

    매수: close <= prev_close*(1+sigma*k_buy)  -> floor(min(daily_invest, cash)/LOC) 주 매수
    매도: close >= prev_close*(1+sigma*k_sell) -> round(holdings*sell_ratio/100) 주 매도
    티어=1마다 총투자금 갱신 (RENEWAL 사이클 전 누적실현 기준)
    """
    df = price_df.copy()
    # sigma 워밍업: price_df 전체(start_date 이전 버퍼 포함)로 sigma 사전 계산
    # 포트폴리오 시뮬은 start_date 이후만 수행
    _df_sigma = df.loc[:pd.to_datetime(end_date)].copy()
    df        = _df_sigma.loc[pd.to_datetime(start_date):].copy()
    if df.empty or len(_df_sigma) < sigma_period + 2:
        return None

    # sigma 사전 계산 (버퍼 포함 전체 구간 -> 시작일부터 sigma 유효)
    _c_all = _df_sigma["Close"].values.astype(float)
    _n_all = len(_c_all)
    _r_all = np.full(_n_all, np.nan)
    for i in range(1, _n_all):
        if _c_all[i-1] > 0:
            _r_all[i] = (_c_all[i] - _c_all[i-1]) / _c_all[i-1]
    _s_all = np.full(_n_all, np.nan)
    for i in range(sigma_period, _n_all):
        w = _r_all[i-sigma_period:i]
        if not np.any(np.isnan(w)):
            _s_all[i] = np.std(w, ddof=0)

    # 시뮬레이션 구간에 해당하는 sigma 추출
    _sim_offset = _df_sigma.index.searchsorted(df.index[0])
    closes = df["Close"].values.astype(float)
    n      = len(closes)
    sigmas = _s_all[_sim_offset:_sim_offset + n]

    # prev_close 배열: 시뮬 첫날은 버퍼의 직전 종가 사용
    if _sim_offset > 0:
        _prev_c = _c_all[_sim_offset - 1 : _sim_offset - 1 + n]
    else:
        _prev_c = np.concatenate([[np.nan], closes[:-1]])

    # 시뮬레이션 상태
    cash          = float(initial_capital)
    holdings      = 0
    avg_cost      = 0.0
    cum_realized  = 0.0
    total_invest  = float(initial_capital)
    tier          = 0
    cum_realiz_hist = []   # 매 거래일 cumR 누적

    assets_arr = np.full(n, np.nan)
    cash_arr   = np.full(n, np.nan)
    history    = [] if return_history else None
    buy_count  = sell_count = 0
    sell_pnls  = []

    for i in range(n):
        close = closes[i]
        sigma = sigmas[i]

        # sigma 미확정 구간 (초기 sigma_period일) - 자산만 기록
        if np.isnan(sigma):
            assets_arr[i] = cash + holdings * close
            cash_arr[i]   = cash
            cum_realiz_hist.append(cum_realized)
            continue

        prev_close = _prev_c[i]   # _prev_c 배열로 첫날 prev_close 버그 수정
        buy_loc    = prev_close * (1.0 + sigma * k_buy)
        sell_loc   = prev_close * (1.0 + sigma * k_sell)

        # -- 티어 순환 (1 -> divisions -> 1 -> ...)
        tier = (tier % divisions) + 1

        # -- 총투자금 갱신 (tier=1, 첫 번째 이후부터)
        if tier == 1 and len(cum_realiz_hist) > 0:
            lookback = (renewal + 1) if len(cum_realiz_hist) > 2 * divisions else renewal
            prev_cum = cum_realiz_hist[-lookback] if len(cum_realiz_hist) >= lookback else 0.0
            delta    = cum_realized - prev_cum
            total_invest += delta * (pcr if delta >= 0 else lcr)

        daily_invest = total_invest / divisions
        prev_avg     = avg_cost
        prev_hold    = holdings

        # -- 매도 판정
        sell_qty = 0
        if holdings > 0 and close >= sell_loc:
            sell_qty = int(round(float(holdings) * sell_ratio / 100.0))

        # -- 매수 판정
        buy_qty = 0
        if close <= buy_loc:
            available = min(daily_invest, cash)
            buy_qty   = math.floor(available / buy_loc)

        # -- 체결 처리
        sell_amt = close * sell_qty
        buy_amt  = close * buy_qty

        # 실현손익
        if sell_qty > 0:
            realized      = sell_amt - prev_avg * sell_qty
            cum_realized += realized
            if prev_avg > 0:
                sell_pnls.append((close / prev_avg - 1) * 100)
            sell_count += 1

        # 잔여주 / 평단가 갱신
        remaining = prev_hold - sell_qty
        new_hold  = remaining + buy_qty
        if new_hold > 0:
            avg_cost = ((remaining * prev_avg + close * buy_qty) / new_hold
                        if buy_qty > 0 else prev_avg)
        else:
            avg_cost = 0.0

        if buy_qty > 0:
            buy_count += 1

        holdings      = new_hold
        cash          = cash - buy_amt + sell_amt
        total_asset   = cash + holdings * close

        cum_realiz_hist.append(cum_realized)
        assets_arr[i] = total_asset
        cash_arr[i]   = cash

        if return_history:
            # 매도 시 손익률 계산에 사용할 "매도 직전 평단가" 별도 기록
            # (sell_ratio=100% 전량 매도 시 avg_cost가 0이 되어 손익률 계산이 망가지는 문제 해결)
            _sell_avg_cost = round(prev_avg, 4) if sell_qty > 0 and prev_avg > 0 else None
            _pnl_pct = round((close / prev_avg - 1) * 100, 4) if sell_qty > 0 and prev_avg > 0 else None
            history.append({
                "날짜":       df.index[i].date(),
                "티어":       tier,
                "종가":       close,
                "σ(%)":      round(sigma * 100, 4),
                "매수LOC":   round(buy_loc,  4),
                "매도LOC":   round(sell_loc, 4),
                "매수량":    buy_qty,
                "매도량":    sell_qty,
                "보유량":    holdings,
                "평단가":    round(avg_cost, 4),
                "매도전평단가": _sell_avg_cost,
                "실현손익":  round(sell_amt - prev_avg * sell_qty if sell_qty > 0 else 0, 2),
                "실현손익률(%)": _pnl_pct,
                "누적실현":  round(cum_realized, 2),
                "총투자금":  round(total_invest, 2),
                "예수금":    round(cash, 2),
                "총자산":    round(total_asset, 2),
            })

    # -- 성과 지표
    valid = assets_arr[~np.isnan(assets_arr)]
    if len(valid) == 0:
        return None

    final_asset  = float(assets_arr[-1])
    assets_clean = assets_arr[~np.isnan(assets_arr)]
    peak_clean   = np.maximum.accumulate(assets_clean)
    mdd          = float(((assets_clean - peak_clean) / peak_clean).min())

    years        = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days / 365.25
    total_return = (final_asset / initial_capital) - 1.0
    cagr         = ((final_asset / initial_capital) ** (1.0 / years) - 1.0) if years > 0 else 0.0
    calmar       = cagr / abs(mdd) if mdd != 0 else 0.0

    out = dict(
        final_asset=final_asset, total_return=total_return,
        cagr=cagr, mdd=mdd, calmar=calmar,
        buy_count=buy_count, sell_count=sell_count,
        win_count=sum(1 for p in sell_pnls if p > 0),
        avg_pnl=float(np.mean(sell_pnls)) if sell_pnls else 0.0,
        max_pnl=max(sell_pnls) if sell_pnls else 0.0,
        min_pnl=min(sell_pnls) if sell_pnls else 0.0,
        assets=assets_arr, dates=df.index,
        sell_pnls_list=sell_pnls,
        cash_series=cash_arr,
        # ordersheet 의 next-day tier=1 갱신 계산에 사용 (NaN 일 포함 전체 길이)
        cum_realiz_hist=list(cum_realiz_hist),
    )
    if return_history:
        out["history"] = pd.DataFrame(history)
    return out


# ══════════════════════════════════════════════════════════════
# 백테스트 엔진 (fast): 최적화 전용, history 없음
# ══════════════════════════════════════════════════════════════

def run_backtest_stdev_fast(
    price_df, start_date, end_date,
    sigma_period=2, k_buy=0.55, k_sell=0.55,
    sell_ratio=75.0, divisions=5, renewal=5,
    pcr=1.0, lcr=1.0,
    initial_capital=20000.0,
):
    """
    run_backtest_stdev 경량 버전 -- 최적화 전용.
    history/DataFrame 생성 없이 핵심 지표만 반환.
    """
    # sigma 워밍업
    _df_sigma = price_df.loc[:pd.to_datetime(end_date)]
    df_idx    = _df_sigma.loc[pd.to_datetime(start_date):].index
    if len(df_idx) == 0 or len(_df_sigma) < sigma_period + 2:
        return None

    # numpy 배열 사전 추출
    _c_all = _df_sigma["Close"].values.astype(np.float64)
    _n_all = len(_c_all)

    # 수익률 + sigma 사전 계산
    _r_all = np.empty(_n_all, dtype=np.float64)
    _r_all[0] = np.nan
    _r_all[1:] = np.where(_c_all[:-1] > 0, (_c_all[1:] - _c_all[:-1]) / _c_all[:-1], np.nan)

    _s_all = np.full(_n_all, np.nan, dtype=np.float64)
    for i in range(sigma_period, _n_all):
        w = _r_all[i - sigma_period:i]
        if not np.any(np.isnan(w)):
            _s_all[i] = np.std(w, ddof=0)

    # 시뮬레이션 구간 오프셋
    _sim_offset = _df_sigma.index.searchsorted(df_idx[0])
    n = len(df_idx)
    closes = _c_all[_sim_offset:_sim_offset + n]
    sigmas = _s_all[_sim_offset:_sim_offset + n]

    if _sim_offset > 0:
        prev_closes = _c_all[_sim_offset - 1:_sim_offset - 1 + n]
    else:
        prev_closes = np.empty(n, dtype=np.float64)
        prev_closes[0] = np.nan
        prev_closes[1:] = closes[:-1]

    # 시뮬레이션 상태 (스칼라)
    cash         = float(initial_capital)
    holdings     = 0
    avg_cost     = 0.0
    cum_realized = 0.0
    total_invest = float(initial_capital)
    tier         = 0
    buy_count    = 0
    sell_count   = 0

    # MDD 실시간 추적
    peak_asset   = 0.0
    mdd          = 0.0

    # cum_realized 히스토리 (갱신 로직에 필요)
    cum_r_buf    = np.empty(n + 1, dtype=np.float64)
    cum_r_idx    = 0

    sell_ratio_f = sell_ratio / 100.0

    for i in range(n):
        close = closes[i]
        sigma = sigmas[i]

        if np.isnan(sigma):
            total_asset = cash + holdings * close
            cum_r_buf[cum_r_idx] = cum_realized
            cum_r_idx += 1
            if total_asset > peak_asset:
                peak_asset = total_asset
            if peak_asset > 0.0:
                dd = (total_asset - peak_asset) / peak_asset
                if dd < mdd:
                    mdd = dd
            continue

        prev_close = prev_closes[i]
        buy_loc    = prev_close * (1.0 + sigma * k_buy)
        sell_loc   = prev_close * (1.0 + sigma * k_sell)

        tier = (tier % divisions) + 1

        if tier == 1 and cum_r_idx > 0:
            lookback = (renewal + 1) if cum_r_idx > 2 * divisions else renewal
            prev_cum = cum_r_buf[cum_r_idx - lookback] if cum_r_idx >= lookback else 0.0
            delta    = cum_realized - prev_cum
            total_invest += delta * (pcr if delta >= 0 else lcr)

        daily_invest = total_invest / divisions
        prev_avg     = avg_cost
        prev_hold    = holdings

        # 매도
        sell_qty = 0
        if holdings > 0 and close >= sell_loc:
            sell_qty = int(round(float(holdings) * sell_ratio_f))

        # 매수
        buy_qty = 0
        if close <= buy_loc:
            available = min(daily_invest, cash)
            buy_qty   = math.floor(available / buy_loc)

        # 체결
        sell_amt = close * sell_qty
        buy_amt  = close * buy_qty

        if sell_qty > 0:
            cum_realized += sell_amt - prev_avg * sell_qty
            sell_count += 1

        remaining = prev_hold - sell_qty
        new_hold  = remaining + buy_qty
        if new_hold > 0:
            avg_cost = ((remaining * prev_avg + close * buy_qty) / new_hold
                        if buy_qty > 0 else prev_avg)
        else:
            avg_cost = 0.0

        if buy_qty > 0:
            buy_count += 1

        holdings    = new_hold
        cash        = cash - buy_amt + sell_amt
        total_asset = cash + holdings * close

        cum_r_buf[cum_r_idx] = cum_realized
        cum_r_idx += 1

        # MDD 실시간 갱신
        if total_asset > peak_asset:
            peak_asset = total_asset
        if peak_asset > 0.0:
            dd = (total_asset - peak_asset) / peak_asset
            if dd < mdd:
                mdd = dd

    if cum_r_idx == 0:
        return None

    final_asset  = cash + holdings * closes[-1]
    years        = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days / 365.25
    total_return = (final_asset / initial_capital) - 1.0
    cagr         = ((final_asset / initial_capital) ** (1.0 / years) - 1.0) if years > 0 else 0.0
    calmar       = cagr / abs(mdd) if mdd != 0 else 0.0

    return dict(
        final_asset=final_asset, total_return=total_return,
        cagr=cagr, mdd=mdd, calmar=calmar,
        buy_count=buy_count, sell_count=sell_count,
    )


# ══════════════════════════════════════════════════════════════
# 매도 이벤트 단위 분석 (티어별 손익률)
# ══════════════════════════════════════════════════════════════

def run_stdev_tier_analysis(hist_df, div4: int) -> list:
    """
    표준편차매매: 매도 이벤트 단위의 분할 매수 횟수별 성과 분석.

    표준편차매매는 sell_ratio<100% 부분 매도가 대부분이라 완전 청산(보유량=0)이
    드물게 발생한다.  그래서 '완전 청산 사이클' 대신 '매도 이벤트' 단위로 분석한다.

    각 매도 발생 시:
      - 직전 매도(또는 포지션 완전 청산) 이후 몇 번 매수가 있었는지 = 티어수
      - 티어수는 1 ~ div4 로 캡 (div4 이상은 div4 로 기록)
      - pnl = (매도가 / 평단가 - 1) * 100
      - 보유일수 = 직전 매도 이후 첫 매수일 ~ 이번 매도일

    hist_df 필수 컬럼: 날짜, 매수량, 매도량, 보유량, 종가, 평단가
    """
    events = []
    buys_since_last_sell  = 0
    first_buy_idx_since   = None   # 이번 '누적 구간'의 첫 매수 row index

    rows = hist_df.reset_index(drop=True)

    for idx, row in rows.iterrows():
        buy_qty  = int(row.get("매수량", 0) or 0)
        sell_qty = int(row.get("매도량", 0) or 0)
        holdings = float(row.get("보유량", 0) or 0)
        close    = float(row.get("종가", 0) or 0)
        avg_cost = float(row.get("평단가", 0) or 0)
        date     = str(row.get("날짜", ""))

        # -- 매도 처리 (엔진 순서: 매도 -> 매수)
        if sell_qty > 0:
            n_tier    = max(1, min(buys_since_last_sell, div4))
            # 매도 손익률: "매도전평단가" 우선 사용 (sell_ratio=100% 호환).
            # 폴백: 행의 평단가 (구버전 history 호환).
            # 두 경우 모두 None/0이면 "실현손익률(%)" 컬럼 사용.
            _sell_avg = row.get("매도전평단가")
            try:
                _sell_avg_f = float(_sell_avg) if _sell_avg is not None and not pd.isna(_sell_avg) else 0.0
            except Exception:
                _sell_avg_f = 0.0
            if _sell_avg_f <= 0:
                # 폴백: 행의 평단가 (구 데이터 호환)
                _sell_avg_f = avg_cost
            if _sell_avg_f > 0:
                pnl = (close / _sell_avg_f - 1) * 100
            else:
                # 그래도 안 되면 실현손익률 컬럼 시도
                _pnl_col = row.get("실현손익률(%)")
                try:
                    pnl = float(_pnl_col) if _pnl_col is not None and not pd.isna(_pnl_col) else 0.0
                except Exception:
                    pnl = 0.0
            hold_days = (idx - first_buy_idx_since) if first_buy_idx_since is not None else 0

            events.append({
                "티어수":   n_tier,
                "매도일":   date,
                "보유일수": hold_days,
                "손익률":   round(pnl, 2),
            })

            # 매도 후 buy 카운터 리셋
            buys_since_last_sell = 0
            first_buy_idx_since  = None

        # -- 매수 처리
        if buy_qty > 0:
            buys_since_last_sell += 1
            if first_buy_idx_since is None:
                first_buy_idx_since = idx

    return events


# ══════════════════════════════════════════════════════════════
# 오늘의 주문표 — 시뮬 후 다음 거래일 LOC 기준가 + 예상수량 반환
# ══════════════════════════════════════════════════════════════

def run_stdev_ordersheet(
    price_df, start_date,
    sigma_period=2, k_buy=0.55, k_sell=0.55,
    sell_ratio=75.0, divisions=5, renewal=5,
    pcr=1.0, lcr=1.0, initial_capital=20000.0,
):
    """오늘까지 시뮬레이션 -> 현재 상태(티어/보유량/예수금/평단가) + 내일 주문가 반환.

    price_df 는 start_date 이전 버퍼(90일)를 포함해 로드된 전체 데이터.
    - sigma 계산: price_df 전체의 최근 sigma_period 수익률 사용 (항상 최신 가격 기준)
    - 포트폴리오 시뮬: start_date 이후 데이터만 사용
    - start_date 이후 거래일이 없으면 초기 상태(0보유/전액현금) 반환
    """
    today = datetime.today().date()
    df_all = price_df.copy()

    # -- sigma 계산: 전체 price_df의 최근 데이터 사용 ------
    closes_all = df_all["Close"].dropna().values.astype(float)
    if len(closes_all) < sigma_period + 1:
        return None   # sigma 계산 불가 (데이터 자체가 너무 짧음)

    rets_last = [(closes_all[j] - closes_all[j-1]) / closes_all[j-1]
                 for j in range(len(closes_all) - sigma_period, len(closes_all))]
    sigma_next = float(np.std(rets_last, ddof=0))
    last_close = float(closes_all[-1])

    next_buy_loc  = round(last_close * (1.0 + sigma_next * k_buy),  2)
    next_sell_loc = round(last_close * (1.0 + sigma_next * k_sell), 2)

    # -- 포트폴리오 시뮬: start_date 이후 데이터 -------
    portfolio_df = df_all.loc[pd.to_datetime(start_date):pd.to_datetime(today)]

    if portfolio_df.empty:
        # start_date가 아직 미래이거나 거래일 없음 -> 초기 상태
        cur_cash     = float(initial_capital)
        cur_invest   = float(initial_capital)
        cur_holdings = 0
        avg_cost     = 0.0
        next_tier    = 1
        cagr         = 0.0
        mdd          = 0.0
        final_asset  = float(initial_capital)
        hist         = pd.DataFrame()
    else:
        res = run_backtest_stdev(
            price_df=df_all,
            start_date=start_date,
            end_date=str(today),
            sigma_period=sigma_period, k_buy=k_buy, k_sell=k_sell,
            sell_ratio=sell_ratio, divisions=divisions, renewal=renewal,
            pcr=pcr, lcr=lcr, initial_capital=initial_capital,
            return_history=True,
        )
        if res is None or res["history"].empty:
            # 시뮬 결과 없음 -> 초기 상태로 fallback
            cur_cash     = float(initial_capital)
            cur_invest   = float(initial_capital)
            cur_holdings = 0
            avg_cost     = 0.0
            next_tier    = 1
            cagr         = 0.0
            mdd          = 0.0
            final_asset  = float(initial_capital)
            hist         = pd.DataFrame()
        else:
            hist      = res["history"]
            last      = hist.iloc[-1]
            cur_cash  = float(last["예수금"])
            cur_invest = float(last["총투자금"])
            cur_holdings = int(last["보유량"])
            avg_cost  = float(last["평단가"])
            next_tier = (int(last["티어"]) % divisions) + 1
            cagr      = res["cagr"]
            mdd       = res["mdd"]
            final_asset = res["final_asset"]

            # ── 내일 tier=1로 진입 시: renewal 갱신을 미리 적용 ──
            # 엔진(run_backtest_stdev)은 다음 거래일 처리 시 tier가 1이 되면
            # total_invest를 renewal 주기 동안의 누적실현 delta로 갱신한다.
            # ordersheet는 미리 이를 적용해야 실제 백테스트와 동일한 daily_invest 산출 가능.
            # (이전 버그: ordersheet는 갱신 전 total_invest를 사용 → est_buy_qty 불일치)
            cum_realiz_hist = res.get("cum_realiz_hist", [])
            if next_tier == 1 and len(cum_realiz_hist) > 0:
                lookback = (renewal + 1) if len(cum_realiz_hist) > 2 * divisions else renewal
                prev_cum = cum_realiz_hist[-lookback] if len(cum_realiz_hist) >= lookback else 0.0
                cur_cum_realized = cum_realiz_hist[-1]
                delta = cur_cum_realized - prev_cum
                cur_invest += delta * (pcr if delta >= 0 else lcr)

    # -- 내일 예상 수량 --------
    daily_invest = cur_invest / divisions if divisions > 0 else cur_invest
    available    = min(daily_invest, cur_cash)
    est_buy_qty  = math.floor(available / next_buy_loc) if next_buy_loc > 0 else 0
    est_sell_qty = int(round(cur_holdings * sell_ratio / 100.0))

    return {
        "last_close":    last_close,
        "sigma_next":    sigma_next,
        "next_buy_loc":  next_buy_loc,
        "next_sell_loc": next_sell_loc,
        "next_tier":     next_tier,
        "holdings":      cur_holdings,
        "avg_cost":      avg_cost,
        "cash":          cur_cash,
        "total_invest":  cur_invest,
        "est_buy_qty":   est_buy_qty,
        "est_sell_qty":  est_sell_qty,
        "cagr":          cagr,
        "mdd":           mdd,
        "final_asset":   final_asset,
        "hist":          hist,
    }


# ═══════════════════════════════════════════════════════════════
# 실전 원장(매매기록) 기반 주문 계산 · 예정 주문 정산 (표준편차)
# ═══════════════════════════════════════════════════════════════
# 원칙 (매매법 룰 = 백테스트 엔진 그대로, 새 룰 아님):
#   · 티어 매일 순환: next_tier = (tier % divisions) + 1
#   · renewal: next_tier==1 진입 시 누적실현 delta로 총투자금 갱신
#   · 동시 체결: 매도(x>=매도LOC) AND 매수(x<=매수LOC) 같은 날 둘 다 가능
#   · 매수 = floor(min(총투자금/분할, 예수금)/매수LOC) · 매도 = round(보유×비율)
# 원장 = 매 거래일 1행. 최근 행(들)은 '예정'(종가 빈칸) → 다음날 실제 종가로 정산.
# 원장의 누적실현 컬럼이 엔진 cum_realiz_hist와 정렬 (정상 버퍼 시 nan일 없음).

SD_HIST_COLS = ["날짜", "티어", "종가", "σ(%)", "매수LOC", "매도LOC",
                "매수량", "매도량", "보유량", "평단가", "매도전평단가",
                "실현손익", "실현손익률(%)", "누적실현", "총투자금",
                "예수금", "총자산"]


def _sd_num(v, default=0.0):
    """원장 셀 → float. '', '-', None, nan은 default."""
    try:
        s = str(v).replace(",", "").strip()
        if s in ("", "-", "None", "nan", "NaN"):
            return default
        return float(s)
    except Exception:
        return default


def sd_row_is_pending(row) -> bool:
    """예정 행 여부 = 종가 미확정(빈칸)."""
    return str(row.get("종가", "")).strip() in ("", "-", "None", "nan")


def calc_sd_record_state(rows):
    """정산된 원장 rows(날짜 오름차순)에서 현재 상태 추출.
    Returns dict(tier,total_invest,cash,holdings,avg_cost,cum_realized,cum_hist,last_date)
    또는 None(원장 비었음)."""
    if not rows:
        return None
    last = rows[-1]
    cum_hist = [_sd_num(r.get("누적실현"), 0.0) for r in rows]
    return {
        "tier":         int(_sd_num(last.get("티어"), 0)),
        "total_invest": _sd_num(last.get("총투자금"), 0.0),
        "cash":         _sd_num(last.get("예수금"), 0.0),
        "holdings":     int(_sd_num(last.get("보유량"), 0)),
        "avg_cost":     _sd_num(last.get("평단가"), 0.0),
        "cum_realized": _sd_num(last.get("누적실현"), 0.0),
        "cum_hist":     cum_hist,
        "last_date":    str(last.get("날짜", "")).strip(),
    }


def calc_sd_order_from_state(state, buy_loc, sell_loc,
                             divisions, sell_ratio, renewal,
                             pcr=1.0, lcr=1.0):
    """원장 상태 + 엔진 룰로 다음 거래일 티어/renewal/주문 수량 계산.
    (run_stdev_ordersheet의 내일 계산 로직과 동일 산식)"""
    tier = (state["tier"] % divisions) + 1
    total_invest = state["total_invest"]
    cum_hist = state["cum_hist"]
    cum_realized = state["cum_realized"]
    # renewal: 티어 1 진입 시 누적실현 delta 반영 (엔진 line 104-108과 동일)
    if tier == 1 and len(cum_hist) > 0:
        lookback = (renewal + 1) if len(cum_hist) > 2 * divisions else renewal
        prev_cum = cum_hist[-lookback] if len(cum_hist) >= lookback else 0.0
        delta = cum_realized - prev_cum
        total_invest += delta * (pcr if delta >= 0 else lcr)
    daily_invest = total_invest / divisions if divisions > 0 else total_invest
    available = min(daily_invest, max(state["cash"], 0.0))
    buy_qty = math.floor(available / buy_loc) if buy_loc > 0 else 0
    buy_qty = max(0, int(buy_qty))
    sell_qty = (int(round(state["holdings"] * sell_ratio / 100.0))
                if state["holdings"] > 0 else 0)
    return {"next_tier": tier, "total_invest": total_invest,
            "daily_invest": daily_invest,
            "buy_qty": buy_qty, "sell_qty": sell_qty}


def settle_sd_pending_rows(rows, close_by_date):
    """예정 행(종가 빈칸)을 그날 실제 종가로 체결/미체결 정산 (in-place).
    엔진 체결 룰 동일: 매도(보유>0 & x>=매도LOC) AND 매수(x<=매수LOC) 동시 가능.
    수량은 예정 그대로 (재계산 없음). Returns 변경된 인덱스 리스트."""
    changed = []
    for idx, row in enumerate(rows):
        if not sd_row_is_pending(row):
            continue
        d = str(row.get("날짜", "")).strip()
        x = close_by_date.get(d)
        if x is None:
            continue                       # 종가 미확정 → 보류
        x = float(x)
        prev_hold = int(_sd_num(row.get("보유량"), 0))     # 체결 전 보유
        prev_avg  = _sd_num(row.get("평단가"), 0.0)
        prev_cash = _sd_num(row.get("예수금"), 0.0)
        prev_cum  = _sd_num(row.get("누적실현"), 0.0)
        buy_loc   = _sd_num(row.get("매수LOC"), 0.0)
        sell_loc  = _sd_num(row.get("매도LOC"), 0.0)
        plan_bq   = int(_sd_num(row.get("매수량"), 0))
        plan_sq   = int(_sd_num(row.get("매도량"), 0))
        # 체결 조건 (엔진 룰) — 매도/매수 독립 판정 (동시 가능)
        sell_exec = prev_hold > 0 and sell_loc > 0 and x >= sell_loc
        buy_exec  = buy_loc > 0 and x <= buy_loc
        sq = plan_sq if sell_exec else 0
        bq = plan_bq if buy_exec else 0
        # 엔진 체결 산식 (line 126-151)
        sell_amt = x * sq
        realized = (sell_amt - prev_avg * sq) if sq > 0 else 0.0
        cum = prev_cum + realized
        remaining = prev_hold - sq
        new_hold = remaining + bq
        if new_hold > 0:
            avg = ((remaining * prev_avg + x * bq) / new_hold) if bq > 0 else prev_avg
        else:
            avg = 0.0
        cash = prev_cash - x * bq + sell_amt
        # 기입
        row["종가"]     = round(x, 4)
        row["매수량"]   = bq
        row["매도량"]   = sq
        row["보유량"]   = new_hold
        row["평단가"]   = round(avg, 4)
        row["매도전평단가"] = round(prev_avg, 4) if (sq > 0 and prev_avg > 0) else ""
        row["실현손익"] = round(realized, 2) if sq > 0 else 0
        row["실현손익률(%)"] = round((x / prev_avg - 1) * 100, 4) if (sq > 0 and prev_avg > 0) else ""
        row["누적실현"] = round(cum, 2)
        # 총투자금·티어는 계획 시점 값 유지 (정산에서 불변)
        row["예수금"]   = round(cash, 2)
        row["총자산"]   = round(cash + new_hold * x, 2)
        changed.append(idx)
    return changed


def build_sd_pending_row(date_str, tier, sigma_next, buy_loc, sell_loc,
                         buy_qty, sell_qty, total_invest,
                         holdings, cash, avg_cost, cum_realized,
                         last_close):
    """오늘 발송 주문의 '예정' 원장 행 (체결 전 상태 저장, 종가 빈칸)."""
    return {
        "날짜": date_str,
        "티어": int(tier),
        "종가": "",                                    # 미확정 → 정산 시 기입
        "σ(%)": round(sigma_next * 100, 4),
        "매수LOC": round(buy_loc, 4),
        "매도LOC": round(sell_loc, 4),
        "매수량": int(buy_qty),                        # 계획 수량
        "매도량": int(sell_qty),                       # 계획 수량
        "보유량": int(holdings),                       # 체결 전 보유
        "평단가": round(avg_cost, 4),
        "매도전평단가": "",
        "실현손익": "",
        "실현손익률(%)": "",
        "누적실현": round(cum_realized, 2),            # 체결 전 (= 전일 누적)
        "총투자금": round(total_invest, 2),            # 오늘 renewal 반영값
        "예수금": round(cash, 2),                      # 체결 전 예수금
        "총자산": round(cash + holdings * last_close, 2),
    }
