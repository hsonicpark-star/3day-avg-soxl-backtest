"""
strategies/stdev.py
표준편차매매 (σ-LOC) 전략 모듈

engine functions, UI rendering (tabs 1-5), helpers
extracted from app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import math
import json
import os
import multiprocessing as mp
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from pathlib import Path

from common.config import (
    _IS_CLOUD, _CONFIG, load_config, save_config, _load_full_config,
    get_ticker_settings, save_ticker_setting, delete_ticker_setting,
    _get_gspread_client, _parse_ticker_settings_json,
)
from common.data import _download_price, load_price_data, next_trading_date
from common.telegram import _send_telegram, render_telegram_help_popover
from common.analysis import (
    compute_annual_stats, compute_monthly_pivot,
    compute_sharpe_sortino, compute_rolling_perf, compute_bnh,
    recalc_adj_history as _recalc_adj_history,
)
from common.auth import _save_user_settings_to_sheet, _hash_password

# 엔진 함수는 stdev_engine.py에 분리 (streamlit 의존성 없는 pure 모듈)
# 자동발송 스크립트(scripts/daily_telegram_alert.py)와 공유 사용
from stdev_engine import (
    run_backtest_stdev,
    run_backtest_stdev_fast,
    run_stdev_tier_analysis,
    run_stdev_ordersheet,
    SD_HIST_COLS,
    settle_sd_pending_rows,
    calc_sd_record_state,
    calc_sd_order_from_state,
    build_sd_pending_row,
    sd_row_is_pending,
)


def _get_avg_close_run_backtest():
    """종가평균매매 엔진 lazy import (성과 비교 탭에��만 사용)."""
    from strategies.avg_close import run_backtest as _run_bt
    return _run_bt


# ════════════════════════════════════════���═════
# Prefix for stdev accounts
# ══════════════════════════════════════════════
_SD_PFX = "sd_"


# ══════════════════════════════════════════════
# 표준편차매매 계좌 설정 CRUD  (common.config 통합 함수 래퍼)
# ══════════════════════════════════════════════
def _get_sd_ticker_settings() -> dict:
    return get_ticker_settings(prefix=_SD_PFX, settings_key="sd_ticker_settings")


def _save_sd_ticker_setting(tk: str, data: dict) -> str:
    return save_ticker_setting(tk, data, prefix=_SD_PFX, settings_key="sd_ticker_settings")


def _delete_sd_ticker_setting(tk: str) -> str:
    return delete_ticker_setting(tk, prefix=_SD_PFX, settings_key="sd_ticker_settings",
                                 history_prefix="sd_")



# ══════════════════════════════════════════════
# SD History helpers
# ══════════════════════════════════════════════
def _get_sd_history_file(tk: str) -> Path:
    if _IS_CLOUD:
        return Path(__file__).parent.parent / f"sd_history_{tk}.csv"
    return Path.home() / ".usd-avg" / f"sd_history_{tk}.csv"


def _load_sd_ledger(tk: str):
    """원장 로드 (엄격 모드): (df, status) 반환. status:
      "ok"     — 원장 정상 (빈 원장 포함 → 시드 가능)
      "no_url" — Cloud에서 gs_url 미설정 → 원장 미사용 모드
                 (시뮬 기준 표시만, 원장 기록/정산/보정 비활성)
      "error"  — GSheets 접근 실패 (일시 장애) → 원장 처리 전부 스킵
    Cloud에서 로컬 CSV는 재배포 시 초기화되는 휘발성이라 fallback으로 쓰면
    '빈 원장'으로 오인 → 시뮬 재시드 → 원장 오염이 발생함 (실사고 원인).
    """
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            import gspread as _gs
            gs_url = st.session_state.get("user_settings", {}).get("gs_url", "")
            if not gs_url:
                return pd.DataFrame(), "no_url"
            client = _get_gspread_client()
            sh = client.open_by_url(gs_url)
            try:
                ws = sh.worksheet(f"sd_{tk}_매매기록")
            except _gs.WorksheetNotFound:
                return pd.DataFrame(), "ok"    # 진짜 없음 → 빈 원장 (시드 가능)
            records = ws.get_all_records()
            return (pd.DataFrame(records) if records else pd.DataFrame()), "ok"
        except Exception:
            return pd.DataFrame(), "error"     # 접근 실패 — CSV fallback 금지
    # 로컬 모드: CSV가 원본 (영구 저장이므로 원장 사용 가능)
    f = _get_sd_history_file(tk)
    if f.exists():
        try:
            return pd.read_csv(f, encoding="utf-8-sig"), "ok"
        except Exception:
            return pd.DataFrame(), "error"
    return pd.DataFrame(), "ok"


def _load_sd_daily_history(tk: str) -> pd.DataFrame:
    """Cloud: GSheets 우선 읽기. 로컬: CSV 읽기. (표시 전용 — 관대 모드)"""
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            import gspread as _gs
            gs_url = st.session_state.get("user_settings", {}).get("gs_url", "")
            if gs_url:
                client = _get_gspread_client()
                sh = client.open_by_url(gs_url)
                ws_name = f"sd_{tk}_매매기록"
                try:
                    ws = sh.worksheet(ws_name)
                    records = ws.get_all_records()
                    if records:
                        return pd.DataFrame(records)
                except _gs.WorksheetNotFound:
                    return pd.DataFrame()
        except Exception:
            pass
    f = _get_sd_history_file(tk)
    if f.exists():
        try:
            return pd.read_csv(f, encoding="utf-8-sig")
        except Exception:
            pass
    return pd.DataFrame()


def _clear_sd_daily_history(tk: str):
    """매매기록 완전 삭제 (로컬 CSV + Cloud GSheets 워크시트).
    Returns: (local_deleted: bool, cloud_deleted: bool, error: str)
    """
    local_deleted = False
    cloud_deleted = False
    error_msg = ""

    # 로컬 CSV 삭제
    try:
        f = _get_sd_history_file(tk)
        if f.exists():
            f.unlink()
            local_deleted = True
    except Exception as e:
        error_msg = f"로컬 CSV 삭제 실패: {e}"

    # Cloud GSheets 워크시트 삭제
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            import gspread as _gs
            gs_url = st.session_state.get("user_settings", {}).get("gs_url", "")
            if gs_url:
                client = _get_gspread_client()
                sh = client.open_by_url(gs_url)
                ws_name = f"sd_{tk}_매매기록"
                try:
                    ws = sh.worksheet(ws_name)
                    sh.del_worksheet(ws)
                    cloud_deleted = True
                except _gs.WorksheetNotFound:
                    cloud_deleted = True   # 이미 없음 → 성공으로 간주
        except Exception as e:
            error_msg += f" / GSheets 삭제 실패: {e}"

    return local_deleted, cloud_deleted, error_msg


def _save_sd_daily_history(tk: str, hist_df: pd.DataFrame):
    """시뮬레이션 히스토리 중 새 날짜만 누적 저장 (B방식).
    로컬: CSV 저장. Cloud: CSV + Google Sheets 'sd_{tk}_매매기록' 워크시트 동기."""
    if hist_df is None or hist_df.empty:
        return
    df_new = hist_df.copy()
    df_new["날짜"] = df_new["날짜"].astype(str)
    df_existing = _load_sd_daily_history(tk)
    if not df_existing.empty and "날짜" in df_existing.columns:
        existing_dates = set(df_existing["날짜"].astype(str))
        df_add = df_new[~df_new["날짜"].isin(existing_dates)].copy()
    else:
        df_add = df_new.copy()
    if df_add.empty:
        return
    # 로컬 CSV 저장
    f = _get_sd_history_file(tk)
    f.parent.mkdir(parents=True, exist_ok=True)
    df_combined = pd.concat([df_existing, df_add], ignore_index=True) \
                  if not df_existing.empty else df_add.copy()
    df_combined = df_combined.sort_values("날짜").reset_index(drop=True)
    df_combined.to_csv(f, index=False, encoding="utf-8-sig")
    # Cloud: Google Sheets 워크시트에도 저장
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            import gspread as _gs
            gs_url = st.session_state.get("user_settings", {}).get("gs_url", "")
            if gs_url:
                client = _get_gspread_client()
                sh = client.open_by_url(gs_url)
                ws_name = f"sd_{tk}_매매기록"
                try:
                    ws = sh.worksheet(ws_name)
                except _gs.WorksheetNotFound:
                    ws = sh.add_worksheet(title=ws_name, rows=5000, cols=25)
                    ws.append_row(df_add.columns.tolist())  # 헤더 추가
                rows_to_add = [[str(v) for v in row] for row in df_add.values.tolist()]
                if rows_to_add:
                    ws.append_rows(rows_to_add, value_input_option="RAW")
        except Exception:
            pass


def _rewrite_sd_daily_history(tk: str, rows: list) -> tuple:
    """원장 전체를 rows로 교체 (행 삭제/보정 포함). rows: dict 리스트 (날짜 오름차순).
    현재 상태 보정 전용 — 잘못된 예정 행 제거 + 앵커 행 수정에 사용.
    Returns: (ok: bool, err: str) — Cloud에서 GSheets 기록 실패 시 ok=False.
    실패를 조용히 삼키면 사용자는 보정됐다고 믿는데 원장은 옛 상태 그대로
    남아 다음날 잘못된 수량이 재등장함 (실사고 원인)."""
    df = (pd.DataFrame(rows, columns=SD_HIST_COLS) if rows
          else pd.DataFrame(columns=SD_HIST_COLS))
    # 로컬 CSV 전체 재기록
    try:
        f = _get_sd_history_file(tk)
        f.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(f, index=False, encoding="utf-8-sig")
    except Exception:
        pass
    # Cloud: GSheets clear 후 전체 재기록 — 여기가 진짜 원장, 실패는 보고
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            gs_url = st.session_state.get("user_settings", {}).get("gs_url", "")
            if not gs_url:
                return False, "GSheets URL 미설정"
            client = _get_gspread_client()
            sh = client.open_by_url(gs_url)
            try:
                ws = sh.worksheet(f"sd_{tk}_매매기록")
            except Exception:
                ws = sh.add_worksheet(title=f"sd_{tk}_매매기록", rows=5000, cols=25)
            ws.clear()
            _data = [list(SD_HIST_COLS)] + [
                [str(r.get(c, "")) for c in SD_HIST_COLS] for r in rows]
            ws.update(values=_data, range_name="A1", value_input_option="RAW")
        except Exception as _e:
            return False, str(_e)
    return True, ""


def _update_sd_daily_history_rows(tk: str, rows: list, changed_indices: list):
    """예정 행 정산 결과 반영 (예정 → 체결/미체결 확정).
    B방식의 유일한 예외: 예정 행은 미확정 주문이므로 그날 종가로 1회 확정만 허용.
    rows: 로드 순서 그대로의 dict 리스트 (cloud=시트 순서, local=CSV 순서)"""
    if not rows or not changed_indices:
        return
    df = pd.DataFrame(rows)
    # 로컬 CSV 전체 재기록 (행 순서 유지)
    try:
        f = _get_sd_history_file(tk)
        f.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(f, index=False, encoding="utf-8-sig")
    except Exception:
        pass
    # Cloud: GSheets 해당 행만 업데이트
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            gs_url = st.session_state.get("user_settings", {}).get("gs_url", "")
            if gs_url:
                client = _get_gspread_client()
                sh = client.open_by_url(gs_url)
                ws = sh.worksheet(f"sd_{tk}_매매기록")
                cols = list(df.columns)
                col_end = chr(ord('A') + len(cols) - 1)
                for ci in changed_indices:
                    vals = [[str(df.iloc[ci][c]) for c in cols]]
                    ws.update(values=vals,
                              range_name=f"A{ci + 2}:{col_end}{ci + 2}",
                              value_input_option="RAW")
        except Exception:
            pass


def _bake_adj_into_sd_ledger(tk: str, amount: float):
    """자본 조정(입출금)을 원장에 즉시 반영.

    마지막 확정 행부터 이후 모든 행(예정 포함)의 예수금/총자산/총투자금에
    조정액을 가산. 표준편차는 1회매수금 = 총투자금 ÷ 분할수이므로 입금이
    총투자금까지 반영되어야 다음 주문부터 매수량이 커짐.
    예정 행의 계획 수량은 불변 (발송 수량 불변 원칙 — 오늘 주문은 그대로,
    내일 주문부터 증액 반영).
    Returns: (ok: bool, status: str)"""
    df, status = _load_sd_ledger(tk)
    if status != "ok" or df.empty:
        return False, status
    rows = df.to_dict("records")
    last_conf = -1
    for i, r in enumerate(rows):
        if not sd_row_is_pending(r):
            last_conf = i
    if last_conf < 0:
        return False, "no_confirmed"
    changed = []
    for i in range(last_conf, len(rows)):
        for col in ("예수금", "총자산", "총투자금"):
            try:
                v = float(str(rows[i].get(col, 0)).replace(",", "").strip() or 0)
                rows[i][col] = round(v + amount, 2)
            except Exception:
                pass
        changed.append(i)
    _update_sd_daily_history_rows(tk, rows, changed)
    return True, "ok"


# ══════════════════════════════════════════════
# Telegram text builder
# ══════════════════════════════════════════════
def _build_sd_order_text(ticker_name: str, k_buy: float, k_sell: float,
                         sigma_period: int, sell_ratio: float, divisions: int,
                         renewal: int, _os_start=None, _os_capital: float = 20000.0,
                         capital_adj_history=None) -> str:
    """표준편차매매 오늘의 주문표를 텔레그램 텍스트로 변환.

    capital_adj_history: JSON 문자열 또는 list [{날짜, 조정금액}, ...]
      사용자 입금/출금 조정 합산하여 cash 표시 보정 (웹 ordersheet와 일관성).
    """
    try:
        today    = datetime.today().date()
        buf_s    = (pd.to_datetime(str(_os_start)) - pd.DateOffset(days=90)).strftime("%Y-%m-%d")
        pdf_tg   = load_price_data(ticker_name, buf_s, str(today), "야후파이낸스 (yfinance)", None)
        if pdf_tg is None or pdf_tg.empty:
            return "가격 데이터를 불러오지 못했습니다."
        res = run_stdev_ordersheet(
            pdf_tg, str(_os_start),
            sigma_period=sigma_period, k_buy=k_buy, k_sell=k_sell,
            sell_ratio=sell_ratio, divisions=divisions, renewal=renewal,
            initial_capital=_os_capital,
        )
        if res is None:
            return "시뮬레이션 데이터가 없습니다."

        # 자본 조정 이력 합산 (웹 ordersheet와 일관성)
        if capital_adj_history:
            try:
                _adj_list = (json.loads(capital_adj_history)
                              if isinstance(capital_adj_history, str)
                              else capital_adj_history)
                if isinstance(_adj_list, list):
                    _today_ts = pd.Timestamp(today)
                    _adj_total = 0.0
                    for _it in _adj_list:
                        try:
                            _dt = pd.Timestamp(_it.get("날짜"))
                            if _dt <= _today_ts:
                                _adj_total += float(_it.get("조정금액", 0))
                        except Exception:
                            continue
                    if _adj_total != 0:
                        res["cash"] = float(res.get("cash", 0)) + _adj_total
                        if "final_asset" in res:
                            res["final_asset"] = float(res.get("final_asset", 0)) + _adj_total
                        # total_invest도 반영 (1회매수금 계산 정확화)
                        if "total_invest" in res:
                            res["total_invest"] = float(res.get("total_invest", 0)) + _adj_total
                        # est_buy_qty 재계산 (엔진 값은 시뮬 기준)
                        _daily_inv_txt = ((res["total_invest"] / divisions)
                                            if divisions > 0 else res["total_invest"])
                        _avail_txt = max(0.0, min(_daily_inv_txt, res["cash"]))
                        _nbl_txt = float(res.get("next_buy_loc", 0))
                        if _nbl_txt > 0:
                            res["est_buy_qty"] = int(math.floor(_avail_txt / _nbl_txt))
            except Exception:
                pass

        # ── 원장(매매기록) 기준 override — 표시 일관성 (조회만, 쓰기 없음) ──
        # 웹 주문표·자동발송과 동일하게 실제 보유/체결 기준 수량 표시
        try:
            _today_led = today.strftime("%Y-%m-%d")
            _closes_led = {}
            _h_led = res.get("hist")
            if _h_led is not None and not _h_led.empty:
                for _, _hr in _h_led.iterrows():
                    _closes_led[str(_hr["날짜"])] = float(_hr["종가"])
            _df_led, _led_st_txt = _load_sd_ledger(ticker_name)
            if _led_st_txt == "ok" and not _df_led.empty:
                _rows_led = _df_led.to_dict("records")
                settle_sd_pending_rows(
                    _rows_led, {d: c for d, c in _closes_led.items() if d < _today_led})
                _prev_led = sorted(
                    [r0 for r0 in _rows_led if str(r0.get("날짜", "")).strip() < _today_led],
                    key=lambda r0: str(r0.get("날짜", "")))
                _stg_led = calc_sd_record_state(_prev_led)
                if _stg_led:
                    # 마지막 기록 이후 입출금만 현금에 가산
                    _extra_led = 0.0
                    if capital_adj_history:
                        try:
                            _al = (json.loads(capital_adj_history)
                                   if isinstance(capital_adj_history, str) else capital_adj_history)
                            for _it in (_al if isinstance(_al, list) else []):
                                if _it.get("원장반영"):
                                    continue   # 이미 원장에 가산됨
                                _da = str(pd.Timestamp(_it.get("날짜")).date())
                                if _stg_led["last_date"] < _da <= _today_led:
                                    _extra_led += float(_it.get("조정금액", 0))
                        except Exception:
                            pass
                    _stg_led["cash"] += _extra_led
                    _og_led = calc_sd_order_from_state(
                        _stg_led, float(res.get("next_buy_loc", 0)),
                        float(res.get("next_sell_loc", 0)),
                        divisions, sell_ratio, renewal, 1.0, 1.0)
                    _tr_led = next((r0 for r0 in _rows_led
                                    if str(r0.get("날짜", "")).strip() == _today_led), None)
                    if _tr_led is not None and sd_row_is_pending(_tr_led):
                        _og_led["buy_qty"] = int(float(_tr_led.get("매수량", 0) or 0))
                        _og_led["sell_qty"] = int(float(_tr_led.get("매도량", 0) or 0))
                    res["holdings"]     = _stg_led["holdings"]
                    res["cash"]         = round(_stg_led["cash"], 2)
                    res["est_buy_qty"]  = _og_led["buy_qty"]
                    res["est_sell_qty"] = _og_led["sell_qty"]
                    if _stg_led["avg_cost"] > 0:
                        res["avg_cost"] = round(_stg_led["avg_cost"], 4)
                    # 총자산도 원장 기준 (시뮬 final_asset이 표시되지 않도록)
                    res["final_asset"] = round(
                        _stg_led["cash"] + _stg_led["holdings"]
                        * float(res.get("last_close", 0)), 2)
        except Exception:
            pass

        lp        = res["last_close"]
        sigma     = res["sigma_next"]
        today_str = today.strftime("%Y-%m-%d")
        # 총자산 계산 (final_asset 우선, 없으면 cash + holdings × last_close)
        _total_asset = float(res.get("final_asset", 0)) or (
            float(res.get("cash", 0)) + int(res.get("holdings", 0)) * lp)
        lines = [
            f"📐 <b>표준편차매매 주문표</b> ({today_str})",
            f"종목: {ticker_name}",
            f"직전 종가: ${lp:,.2f}  |  σ = {sigma*100:.3f}%",
            f"총자산: <b>${_total_asset:,.0f}</b>  (현금 ${res['cash']:,.0f})",
            "━━━━━━━━━━━━━━━",
            f"🔴 매수 LOC  ${res['next_buy_loc']:,.2f}  (예상 {res['est_buy_qty']:,}주)",
        ]
        if res["holdings"] > 0:
            lines.append(f"🔵 매도 LOC  ${res['next_sell_loc']:,.2f}  (예상 {res['est_sell_qty']:,}주)")
            pnl = (lp / res["avg_cost"] - 1) * 100 if res["avg_cost"] > 0 else 0
            lines += [
                "━━━━━━━━━━━━━━━",
                f"📦 보유: {res['holdings']:,}주  |  평단: ${res['avg_cost']:.2f}",
                f"   현재가: ${lp:.2f}  ({pnl:+.2f}%)",
            ]
        else:
            lines.append(f"📦 보유주식 없음 (전량 현금)")
        # 퉁치기 안내 (원 주문과 결과가 다를 때만 — 자전거래 거부 증권사용)
        # 자동발송(build_sd_message)과 동일 로직
        try:
            from dss_engine import tungchigi_message_lines
            _tng_rows = []
            if int(res.get("est_buy_qty", 0)) > 0:
                _tng_rows.append(["매수", "LOC",
                                   round(float(res["next_buy_loc"]), 2),
                                   int(res["est_buy_qty"])])
            if int(res.get("holdings", 0)) > 0 and int(res.get("est_sell_qty", 0)) > 0:
                _tng_rows.append(["매도", "LOC",
                                   round(float(res["next_sell_loc"]), 2),
                                   int(res["est_sell_qty"])])
            lines.extend(tungchigi_message_lines(_tng_rows))
        except Exception:
            pass
        lines.append("※ 종가 LOC 주문 기준입니다.")
        return "\n".join(lines)
    except Exception as e:
        return f"주문표 생성 오류: {e}"


# ══════════════════════════════════════════════
# Presets DB
# ══════════════════════════════════════════════
_SD_PRESETS_DB = {
    "SOXL": [
        {"label": "공격형",
         "sigma_period": 2, "k_buy": 0.65, "k_sell": 0.45, "sell_ratio": 85.0,
         "divisions": 3, "renewal": 3,
         "help": "CAGR 83.59%  |  MDD 41.06%  |  Calmar 2.04\n3분할 + 85% 매도로 매우 공격적. CAGR 최고\n(σ기간=2일, 갱신주기=3 자동 적용)"},
        {"label": "균형형",
         "sigma_period": 2, "k_buy": 0.65, "k_sell": 0.55, "sell_ratio": 90.0,
         "divisions": 6, "renewal": 10,
         "help": "CAGR 65.88%  |  MDD 31.29%  |  Calmar 2.11\n6분할 + 90% 매도로 균형. 다른 프리셋과 다른 매매 패턴\n(σ기간=2일, 갱신주기=10 자동 적용)"},
        {"label": "안정형",
         "sigma_period": 2, "k_buy": 0.65, "k_sell": 0.45, "sell_ratio": 85.0,
         "divisions": 5, "renewal": 5,
         "help": "CAGR 57.40%  |  MDD 28.02%  |  Calmar 2.05\n안정적 운용 추천 (검증된 클래식)\n(σ기간=2일, 갱신주기=5 자동 적용)"},
        {"label": "Ultra-Safe형",
         "sigma_period": 2, "k_buy": 0.55, "k_sell": 0.30, "sell_ratio": 95.0,
         "divisions": 7, "renewal": 7,
         "help": "CAGR 35.81%  |  MDD 17.40%  |  Calmar 2.06\n7분할 + 95% 매도로 매우 보수적. MDD 17% 이내\n변동성 22% (안정형 35%)로 심리 안정\n(σ기간=2일, 갱신주기=7 자동 적용)"},
        {"label": "Sonic형",
         "sigma_period": 2, "k_buy": 0.55, "k_sell": 0.45, "sell_ratio": 95.0,
         "divisions": 3, "renewal": 3,
         "help": "CAGR 77.25%  |  MDD 35.80%  |  Calmar 2.16\n3분할 + 95% 매도. 19,200개 중 Calmar 1위\n(σ기간=2일, 갱신주기=3 자동 적용)"},
    ],
}


# ══════════════════════════════════════════════
# Sidebar rendering
# ══════════════════════════════════════════════
def render_sidebar(usercfg, sfloat, sint):
    """표준편차매매 사이드바 파라미터 렌더링.

    Returns dict with: sd_sigma_period, sd_k_buy, sd_k_sell,
                       sell_ratio, divisions, sd_renewal
    """
    # 안정형 표준 파라미터: k_buy=0.65, k_sell=0.45, sell_ratio=85%, divisions=5, sigma=2일
    # (MDD -30% 이내 최고 Calmar 기준 최적화 결과)
    sd_sigma_period = st.number_input("σ 계산 기간 (일)", value=sint(usercfg.get("sd_sigma_period"), 2),
                                      min_value=1, max_value=20, step=1)
    sd_k_buy  = st.number_input("k_buy  (매수 계수)", value=sfloat(usercfg.get("sd_k_buy"),  0.65),
                                min_value=0.0, max_value=5.0, step=0.05, format="%.2f")
    sd_k_sell = st.number_input("k_sell (매도 계수)", value=sfloat(usercfg.get("sd_k_sell"), 0.45),
                                min_value=0.0, max_value=5.0, step=0.05, format="%.2f")
    sell_ratio = st.number_input("매도비율 (%)", value=sfloat(usercfg.get("sd_sell_ratio"), 85.0),
                                 step=5.0, min_value=0.0, max_value=100.0)
    divisions  = st.number_input("분할수 (티어 수)", value=sint(usercfg.get("sd_divisions"), 5),
                                 min_value=1, step=1)
    sd_renewal = st.number_input("갱신 주기 (RENEWAL)", value=sint(usercfg.get("sd_renewal"), 5),
                                 min_value=1, step=1)
    return {
        "sd_sigma_period": sd_sigma_period,
        "sd_k_buy": sd_k_buy,
        "sd_k_sell": sd_k_sell,
        "sell_ratio": sell_ratio,
        "divisions": divisions,
        "sd_renewal": sd_renewal,
    }


# ══════════════════════════════════════════════
# TAB 1 -- Backtest
# ══════════════════════════════════════════════
def render_backtest_tab(ticker, params, data_source, excel_file, start_date, end_date, initial_capital):
    """표준편차매매 백테스트 탭 렌더링."""
    sd_sigma_period = params["sd_sigma_period"]
    sd_k_buy  = params["sd_k_buy"]
    sd_k_sell = params["sd_k_sell"]
    sell_ratio = params["sell_ratio"]
    divisions  = params["divisions"]
    sd_renewal = params["sd_renewal"]

    if st.button("▶ 백테스트 실행", type="primary", key="run_bt_sd"):
        with st.spinner("데이터 로드 및 시뮬레이션 중..."):
            price_df = load_price_data(ticker, str(start_date), str(end_date), "야후 파이낸스 (yfinance)", None)
        if price_df is None or price_df.empty:
            st.error("데이터 로드 실패. 티커/날짜를 확인해주세요.")
        else:
            res = run_backtest_stdev(
                price_df=price_df,
                start_date=str(start_date), end_date=str(end_date),
                sigma_period=sd_sigma_period, k_buy=sd_k_buy, k_sell=sd_k_sell,
                sell_ratio=sell_ratio, divisions=divisions, renewal=sd_renewal,
                initial_capital=initial_capital,
                return_history=True,
            )
            if res is None:
                st.error("시뮬레이션 실패. 데이터가 부족합니다.")
            else:
                st.session_state["sd_bt_res"]    = res
                st.session_state["sd_bt_ticker"] = ticker

    if "sd_bt_res" in st.session_state:
        res    = st.session_state["sd_bt_res"]
        tk_lbl = st.session_state.get("sd_bt_ticker", ticker)

        # -- 핵심 지표 --
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("최종 자산",  f"${res['final_asset']:,.0f}")
        c2.metric("CAGR",       f"{res['cagr']*100:.2f}%")
        c3.metric("MDD",        f"{res['mdd']*100:.2f}%")
        c4.metric("Calmar",     f"{res['calmar']:.2f}")

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("총 수익률",  f"{res['total_return']*100:.1f}%")
        c6.metric("매수 횟수",  f"{res['buy_count']:,}회")
        c7.metric("매도 횟수",  f"{res['sell_count']:,}회")
        c8.metric("승률",       f"{res['win_count']/res['sell_count']*100:.1f}%" if res['sell_count'] else "N/A")

        # -- 자산 곡선 --
        dates_valid  = res["dates"]
        assets_valid = res["assets"]
        mask = ~np.isnan(assets_valid)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates_valid[mask], y=assets_valid[mask],
            name=f"{tk_lbl} 표준편차매매",
            line=dict(color="#1976D2", width=2),
        ))
        fig.update_layout(
            title=f"{tk_lbl} 표준편차매매 자산 곡선",
            xaxis_title="날짜", yaxis_title="총자산 ($)",
            height=400, template="plotly_white",
        )
        st.plotly_chart(fig, use_container_width=True)

        # -- 거래 히스토리 테이블 --
        with st.expander("거래 상세 내역", expanded=False):
            hist_df = res["history"].copy()
            hist_df["날짜"] = hist_df["날짜"].astype(str)
            st.dataframe(hist_df, hide_index=True, use_container_width=True, height=400)
            csv_bytes = hist_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button("CSV 다운로드", csv_bytes, f"{tk_lbl}_stdev_backtest.csv", "text/csv")

        # -- 연도별 수익률 --
        with st.expander("연도별 성과", expanded=False):
            hist_df2 = res["history"].copy()
            hist_df2["날짜"] = pd.to_datetime(hist_df2["날짜"])
            hist_df2 = hist_df2.set_index("날짜")
            annual_rows = []
            prev_asset  = float(initial_capital)
            for yr in sorted(hist_df2.index.year.unique()):
                yr_df    = hist_df2[hist_df2.index.year == yr]
                end_ast  = float(yr_df["총자산"].iloc[-1])
                ret      = (end_ast / prev_asset - 1) * 100
                annual_rows.append({"연도": yr, "연말자산($)": f"${end_ast:,.0f}", "연간수익률(%)": ret})
                prev_asset = end_ast
            annual_df = pd.DataFrame(annual_rows)
            def _color_ret_sd(val):
                try:
                    v = float(val)
                    if v > 0:  return "color: #1565C0; font-weight:bold"
                    if v < 0:  return "color: #c62828; font-weight:bold"
                except: pass
                return ""
            st.dataframe(
                annual_df.style
                    .map(_color_ret_sd, subset=["연간수익률(%)"])
                    .format({"연간수익률(%)": "{:+.2f}%"}),
                hide_index=True, use_container_width=True,
            )


# ══════════════════════════════════════════════
# TAB 2 -- Optimization
# ══════════════════════════════════════════════
def _show_stdev_opt_results(res_df, sort_col, kb_vals, ks_vals, ticker, key_sfx):
    """표준편차매매 최적화 결과 공통 표시."""
    st.subheader(f"상위 20개 결과  ({sort_col} 기준)")
    _fmt = {
        "k_buy":"{:.2f}", "k_sell":"{:.2f}",
        "매도비율(%)":"{:.0f}", "분할수":"{:.0f}",
        "CAGR(%)":"{:.2f}%", "MDD(%)":"{:.2f}%",
        "Calmar":"{:.4f}", "총수익(%)":"{:.2f}%", "최종자산($)":"${:,.0f}",
    }
    st.dataframe(
        res_df.head(20).style.format({k: v for k, v in _fmt.items() if k in res_df.columns}),
        use_container_width=True
    )
    if kb_vals and ks_vals and len(kb_vals) * len(ks_vals) <= 2500:
        st.subheader(f"히트맵: k_buy x k_sell  ->  {sort_col}")
        _hdf = res_df.groupby(["k_buy", "k_sell"])[sort_col].max().reset_index()
        _hpiv = _hdf.pivot(index="k_sell", columns="k_buy", values=sort_col)
        _show_txt = len(kb_vals) * len(ks_vals) <= 400
        fig_hmap = px.imshow(_hpiv, color_continuous_scale="RdYlGn",
                              labels={"x": "k_buy", "y": "k_sell", "color": sort_col},
                              aspect="auto", text_auto=".2f" if _show_txt else False)
        fig_hmap.update_layout(height=500)
        st.plotly_chart(fig_hmap, use_container_width=True)
    st.subheader("리스크-수익 분포  (CAGR vs MDD)")
    _hover = [c for c in ["k_buy","k_sell","분할수","매도비율(%)","Calmar"] if c in res_df.columns]
    fig_sc = px.scatter(res_df, x="MDD(%)", y="CAGR(%)", color=sort_col,
                         hover_data=_hover, color_continuous_scale="RdYlGn")
    fig_sc.update_layout(height=420)
    st.plotly_chart(fig_sc, use_container_width=True)
    _ocsv = res_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("최적화 결과 CSV", data=_ocsv,
                       file_name=f"opt_stdev_{ticker}_{key_sfx}.csv", mime="text/csv",
                       key=f"dl_sd_{key_sfx}")


def _sd_make_row(kb, ks, sr, dv, r):
    return {
        "k_buy": round(float(kb), 3), "k_sell": round(float(ks), 3),
        "매도비율(%)": int(sr), "분할수": int(dv),
        "CAGR(%)":   round(r["cagr"]         * 100, 2),
        "MDD(%)":    round(r["mdd"]          * 100, 2),
        "Calmar":    round(r["calmar"],             4),
        "총수익(%)": round(r["total_return"] * 100, 2),
        "최종자산($)": round(r["final_asset"],     0),
    }


_NUM_WORKERS = max(1, (os.cpu_count() or 1) - 1)

def _run_parallel_opt_stdev(combos, price_df, progress_bar):
    import pickle, tempfile
    tmp_path = os.path.join(tempfile.gettempdir(), f'stdev_opt_{os.getpid()}.pkl')
    with open(tmp_path, 'wb') as f:
        pickle.dump(price_df, f, protocol=pickle.HIGHEST_PROTOCOL)
    total = len(combos)
    rows = []
    from opt_worker_stdev import init_worker, run_single_bt
    try:
        with mp.Pool(_NUM_WORKERS, initializer=init_worker, initargs=(tmp_path,)) as pool:
            count = 0
            for r in pool.imap_unordered(run_single_bt, combos, chunksize=max(1, total // _NUM_WORKERS)):
                if r is not None:
                    rows.append(r)
                count += 1
                if count % max(1, total // 50) == 0:
                    progress_bar.progress(min(count / total, 1.0), text=f"실행 중... {count:,} / {total:,} ({_NUM_WORKERS}코어)")
    finally:
        try: os.unlink(tmp_path)
        except OSError: pass
    progress_bar.progress(1.0, text="완료!")
    return rows


def render_optimization_tab(ticker, params, start_date, end_date, initial_capital, data_source, excel_file):
    """표준편차매매 파라미터 최적화 탭 렌더링."""
    sd_sigma_period = params["sd_sigma_period"]
    sd_renewal      = params["sd_renewal"]

    st.subheader("표준편차매매 파라미터 최적화")

    sd_opt_method = st.radio(
        "최적화 방식",
        ["그리드 탐색", "랜덤 탐색", "워크포워드", "베이지안"],
        horizontal=True, key="sd_opt_method",
    )
    _sd_method_desc = {
        "그리드 탐색": "k_buy x k_sell x 매도비율 x 분할수 모든 조합을 완전 탐색합니다. 조합이 적을 때 가장 정확합니다.",
        "랜덤 탐색":   "무작위로 N개 조합을 샘플링합니다. 탐색 공간이 클 때 빠르게 좋은 파라미터를 찾습니다.",
        "워크포워드":  "전체 기간을 IS(최적화)/OOS(검증) 윈도우로 분할해 과적합을 방지합니다. 실전에 가장 가까운 검증 방식입니다.",
        "베이지안":    "Optuna TPE 알고리즘으로 스마트하게 탐색합니다. 적은 시도로 최적값에 빠르게 수렴합니다.",
    }
    st.caption(_sd_method_desc[sd_opt_method])

    # -- 공통 파라미터 범위 설정 ------
    with st.expander("파라미터 범위 설정", expanded=True):
        _sd_kb_same = st.checkbox("k_buy = k_sell 동일하게 설정", value=True, key="sd_kb_same")

        st.markdown("**k_buy 범위**  *(매수 sigma 계수 -- LOC = 전일종가 x (1 + sigma x k_buy))*")
        _skc1, _skc2, _skc3 = st.columns(3)
        sd_kb_min  = _skc1.number_input("최솟값", value=0.20, step=0.05, format="%.2f", key="sd_kb_min")
        sd_kb_max  = _skc2.number_input("최댓값", value=1.50, step=0.05, format="%.2f", key="sd_kb_max")
        sd_kb_step = _skc3.number_input("간격 (스텝)",
                        value=0.05, min_value=0.01, step=0.05, format="%.2f", key="sd_kb_step",
                        help="k값을 이 간격으로 나눠서 탐색합니다. 예) 0.05 -> 0.20, 0.25, 0.30 ... 순서로 탐색")

        if not _sd_kb_same:
            st.markdown("**k_sell 범위**  *(매도 sigma 계수)*")
            _skc4, _skc5, _skc6 = st.columns(3)
            sd_ks_min  = _skc4.number_input("최솟값", value=0.20, step=0.05, format="%.2f", key="sd_ks_min")
            sd_ks_max  = _skc5.number_input("최댓값", value=1.50, step=0.05, format="%.2f", key="sd_ks_max")
            sd_ks_step = _skc6.number_input("간격",   value=0.05, min_value=0.01, step=0.05, format="%.2f", key="sd_ks_step")
        else:
            sd_ks_min = sd_kb_min; sd_ks_max = sd_kb_max; sd_ks_step = sd_kb_step

        st.markdown("**분할수 목록**")
        _sdc1, _ = st.columns(2)
        sd_dv_list = _sdc1.multiselect("분할수 선택", [3, 4, 5, 6, 7, 8, 10], default=[5], key="sd_dv_list")

        st.markdown("**매도비율 범위 (%)**  *(5% 단위로 설정)*")
        _ssc1, _ssc2, _ssc3 = st.columns(3)
        sd_sr_min  = _ssc1.number_input("최솟값(%)", value=50,  min_value=5, max_value=100, step=5,  key="sd_sr_min")
        sd_sr_max  = _ssc2.number_input("최댓값(%)", value=100, min_value=5, max_value=100, step=5,  key="sd_sr_max")
        sd_sr_step = _ssc3.number_input("간격(%)",   value=25,  min_value=5, max_value=50,  step=5,  key="sd_sr_step")

        sd_metric_key = st.selectbox("최적화 기준 지표", [
            "Calmar Ratio (CAGR / MDD)",
            "CAGR (%)",
            "총수익률 (%)",
            "MDD 최소화 (작을수록 좋음)",
        ], key="sd_metric_key")

    # 탐색 범위 배열 생성
    sd_kb_vals = np.round(np.arange(sd_kb_min, sd_kb_max + sd_kb_step * 0.5, sd_kb_step), 4).tolist()
    sd_ks_vals = sd_kb_vals if _sd_kb_same else np.round(np.arange(sd_ks_min, sd_ks_max + sd_ks_step * 0.5, sd_ks_step), 4).tolist()
    sd_dv_vals = sd_dv_list if sd_dv_list else [5]
    sd_sr_vals = list(range(int(sd_sr_min), int(sd_sr_max) + 1, int(sd_sr_step)))
    if not sd_sr_vals: sd_sr_vals = [int(sd_sr_min)]
    sd_n_total = len(sd_kb_vals) * len(sd_ks_vals) * len(sd_dv_vals) * len(sd_sr_vals)

    if "Calmar" in sd_metric_key:    _sd_sort_col, _sd_sort_asc = "Calmar",    False
    elif "CAGR" in sd_metric_key:    _sd_sort_col, _sd_sort_asc = "CAGR(%)",   False
    elif "총수익률" in sd_metric_key: _sd_sort_col, _sd_sort_asc = "총수익(%)", False
    else:                             _sd_sort_col, _sd_sort_asc = "MDD(%)",    True

    # -- 1) 그리드 탐색 ---
    if sd_opt_method == "그리드 탐색":
        _sd_info = (f"예상 조합 수: **{sd_n_total:,}개** "
                    f"(k_buy {len(sd_kb_vals)} x k_sell {len(sd_ks_vals)} "
                    f"x 분할수 {len(sd_dv_vals)} x 매도비율 {len(sd_sr_vals)})")
        if sd_n_total > 10000:
            st.error(_sd_info + "  \n조합이 10,000개를 초과합니다. 범위를 줄이거나 간격을 늘려주세요.")
        elif sd_n_total > 3000:
            st.warning(_sd_info + "  \n조합이 많아 다소 시간이 걸릴 수 있습니다.")
        else:
            st.info(_sd_info)

        if st.button("▶ 그리드 탐색 실행", type="primary", key="sd_run_grid",
                     disabled=(sd_n_total > 10000 or sd_n_total == 0)):
            with st.spinner("데이터 로드 중..."):
                price_df_opt = load_price_data(ticker, str(start_date), str(end_date), "야후 파이낸스 (yfinance)", None)
            if price_df_opt is None or price_df_opt.empty:
                st.error("데이터 로드 실패.")
                st.stop()
            progress = st.progress(0.0, text="그리드 탐색 실행 중...")
            combos = [
                (kb, ks, sr, dv, str(start_date), str(end_date), initial_capital, sd_sigma_period, sd_renewal)
                for kb in sd_kb_vals for ks in sd_ks_vals for dv in sd_dv_vals for sr in sd_sr_vals
            ]
            rows = _run_parallel_opt_stdev(combos, price_df_opt, progress)
            if not rows: st.error("유효한 결과가 없습니다."); st.stop()
            res_df = pd.DataFrame(rows).sort_values(_sd_sort_col, ascending=_sd_sort_asc).reset_index(drop=True)
            _show_stdev_opt_results(res_df, _sd_sort_col, sd_kb_vals,
                                    None if _sd_kb_same else sd_ks_vals, ticker, "grid")

    # -- 2) 랜덤 탐색 ----
    elif sd_opt_method == "랜덤 탐색":
        import random as _random
        sd_n_samples = st.number_input("샘플 수", min_value=50, max_value=5000,
                                        value=300, step=50, key="sd_n_samples")
        st.info(f"랜덤으로 **{sd_n_samples:,}개** 조합을 샘플링합니다.")

        if st.button("▶ 랜덤 탐색 실행", type="primary", key="sd_run_random"):
            with st.spinner("데이터 로드 중..."):
                price_df_opt = load_price_data(ticker, str(start_date), str(end_date), "야후 파이낸스 (yfinance)", None)
            if price_df_opt is None or price_df_opt.empty:
                st.error("데이터 로드 실패."); st.stop()
            _random.seed(42)
            sampled = [
                (round(_random.uniform(sd_kb_min, sd_kb_max), 3),
                 round(_random.uniform(sd_kb_min, sd_kb_max), 3) if _sd_kb_same
                       else round(_random.uniform(sd_ks_min, sd_ks_max), 3),
                 _random.choice(sd_dv_vals),
                 _random.choice(sd_sr_vals))
                for _ in range(int(sd_n_samples))
            ]
            progress = st.progress(0.0, text="랜덤 탐색 실행 중...")
            combos = [
                (kb, ks, sr, dv, str(start_date), str(end_date), initial_capital, sd_sigma_period, sd_renewal)
                for kb, ks, dv, sr in sampled
            ]
            rows = _run_parallel_opt_stdev(combos, price_df_opt, progress)
            if not rows: st.error("유효한 결과가 없습니다."); st.stop()
            res_df = pd.DataFrame(rows).sort_values(_sd_sort_col, ascending=_sd_sort_asc).reset_index(drop=True)
            _show_stdev_opt_results(res_df, _sd_sort_col, None, None, ticker, "random")

    # -- 3) 워크포워드 ---
    elif sd_opt_method == "워크포워드":
        wf1, wf2 = st.columns(2)
        sd_is_years  = wf1.number_input("IS(최적화) 기간 (년)", min_value=1, max_value=10, value=3, key="sd_wf_is")
        sd_oos_years = wf2.number_input("OOS(검증) 기간 (년)",  min_value=1, max_value=5,  value=1, key="sd_wf_oos")
        st.info(
            f"IS **{sd_is_years}년** 최적화 -> OOS **{sd_oos_years}년** 검증을 슬라이딩 반복합니다.\n\n"
            f"그리드 조합 **{sd_n_total:,}개** x 윈도우 수 만큼 백테스트가 실행됩니다."
        )

        if st.button("▶ 워크포워드 실행", type="primary", key="sd_run_wfo"):
            with st.spinner("데이터 로드 중..."):
                price_df_opt = load_price_data(ticker, str(start_date), str(end_date), "야후 파이낸스 (yfinance)", None)
            if price_df_opt is None or price_df_opt.empty:
                st.error("데이터 로드 실패."); st.stop()
            dates       = price_df_opt.index
            total_start = dates[0].date()
            total_end   = dates[-1].date()
            windows = []
            cur = total_start
            while True:
                is_s  = cur
                is_e  = is_s  + timedelta(days=int(sd_is_years  * 365.25))
                oos_s = is_e
                oos_e = oos_s + timedelta(days=int(sd_oos_years * 365.25))
                if oos_e > total_end: break
                windows.append((is_s, is_e, oos_s, oos_e))
                cur = oos_s
            if not windows:
                st.error("데이터 기간이 너무 짧아 윈도우를 생성할 수 없습니다."); st.stop()
            st.info(f"총 **{len(windows)}개** 윈도우 생성됨")
            progress    = st.progress(0.0, text="워크포워드 실행 중...")
            wfo_rows    = []
            cur_capital = initial_capital
            for wi, (is_s, is_e, oos_s, oos_e) in enumerate(windows):
                # IS 구간 병렬 탐색
                is_combos = [
                    (kb, ks, sr, dv, str(is_s), str(is_e), initial_capital, sd_sigma_period, sd_renewal)
                    for kb in sd_kb_vals for ks in sd_ks_vals for dv in sd_dv_vals for sr in sd_sr_vals
                ]
                is_progress_text = f"윈도우 {wi+1}/{len(windows)} IS 최적화 중..."
                progress.progress(min((wi) / len(windows), 0.99), text=is_progress_text)
                is_rows = _run_parallel_opt_stdev(is_combos, price_df_opt, progress)
                # best score 추출
                best_score, best_params = -999.0, None
                for row in is_rows:
                    if "Calmar"   in sd_metric_key: score = row["Calmar"]
                    elif "CAGR"   in sd_metric_key: score = row["CAGR(%)"]
                    elif "총수익률" in sd_metric_key: score = row["총수익(%)"]
                    else:                            score = -abs(row["MDD(%)"])
                    if score > best_score:
                        best_score  = score
                        best_params = (row["k_buy"], row["k_sell"], row["분할수"], row["매도비율(%)"])
                if best_params is None: continue
                kb_b, ks_b, dv_b, sr_b = best_params
                # OOS 단일 실행 (run_backtest_stdev_fast)
                oos_r = run_backtest_stdev_fast(
                    price_df_opt, str(oos_s), str(oos_e),
                    sigma_period=sd_sigma_period, k_buy=kb_b, k_sell=ks_b,
                    sell_ratio=float(sr_b), divisions=int(dv_b), renewal=sd_renewal,
                    initial_capital=cur_capital,
                )
                if oos_r is None: continue
                wfo_rows.append({
                    "윈도우":      wi + 1,
                    "IS 기간":     f"{is_s} ~ {is_e}",
                    "OOS 기간":    f"{oos_s} ~ {oos_e}",
                    "Best k_buy":  round(kb_b, 3),
                    "Best k_sell": round(ks_b, 3),
                    "매도비율(%)": sr_b,
                    "분할수":      dv_b,
                    f"IS {_sd_sort_col}": round(best_score, 3),
                    "OOS Calmar":  round(oos_r["calmar"],      3),
                    "OOS CAGR(%)": round(oos_r["cagr"] * 100, 2),
                    "OOS MDD(%)":  round(oos_r["mdd"]  * 100, 2),
                    "시작($)":     round(cur_capital,          2),
                    "종료($)":     round(oos_r["final_asset"], 2),
                })
                cur_capital = oos_r["final_asset"]
            progress.progress(1.0, text="완료!")
            if not wfo_rows: st.error("유효한 OOS 결과가 없습니다."); st.stop()
            wfo_df    = pd.DataFrame(wfo_rows)
            total_ret = (cur_capital - initial_capital) / initial_capital
            st.subheader("워크포워드 종합 성과")
            wc1, wc2, wc3, wc4 = st.columns(4)
            wc1.metric("시작 자본",       f"${initial_capital:,.0f}")
            wc2.metric("최종 자본 (OOS)", f"${cur_capital:,.0f}")
            wc3.metric("OOS 총 수익률",   f"{total_ret*100:+.2f}%")
            wc4.metric("윈도우 수",       f"{len(wfo_rows)}개")
            st.subheader("윈도우별 결과")
            st.dataframe(wfo_df.style.format({
                "Best k_buy":  "{:.3f}", "Best k_sell": "{:.3f}",
                "매도비율(%)": "{:.0f}%",
                "OOS Calmar":  "{:.3f}", "OOS CAGR(%)": "{:.2f}%",
                "OOS MDD(%)":  "{:.2f}%",
                "시작($)":     "${:,.2f}", "종료($)": "${:,.2f}",
            }), use_container_width=True)
            fig_wfo = px.bar(wfo_df, x="윈도우", y="OOS CAGR(%)", color="OOS CAGR(%)",
                             color_continuous_scale="RdYlGn", text_auto=".1f",
                             title="윈도우별 OOS CAGR (%)")
            fig_wfo.add_hline(y=0, line_dash="dash", line_color="gray")
            fig_wfo.update_layout(height=400)
            st.plotly_chart(fig_wfo, use_container_width=True)
            fig_cap = px.line(wfo_df, x="윈도우", y="종료($)",
                              title="OOS 자본 변화 (윈도우별 종료 자산)", markers=True)
            fig_cap.update_layout(height=380)
            st.plotly_chart(fig_cap, use_container_width=True)
            wfo_csv = wfo_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button("워크포워드 결과 CSV", data=wfo_csv,
                               file_name=f"wfo_stdev_{ticker}.csv", mime="text/csv", key="sd_dl_wfo")

    # -- 4) 베이지안 (Optuna) -----
    elif sd_opt_method == "베이지안":
        try:
            import optuna as _optuna
            _sd_optuna_ok = True
        except ImportError:
            _sd_optuna_ok = False

        if not _sd_optuna_ok:
            st.error("`optuna` 패키지가 설치되지 않았습니다. `requirements.txt`에 `optuna>=3.6.0` 추가 후 재배포하세요.")
        else:
            bc1, _ = st.columns(2)
            sd_n_trials = bc1.number_input("탐색 횟수 (trials)", min_value=50,
                                            max_value=2000, value=300, step=50, key="sd_n_trials")
            st.info(
                f"Optuna TPE 알고리즘으로 **{sd_n_trials}회** 스마트 탐색합니다.\n\n"
                f"그리드 탐색({sd_n_total:,}개) 대비 적은 시도로 최적값에 근접합니다."
            )
            if st.button("▶ 베이지안 최적화 실행", type="primary", key="sd_run_bayes"):
                with st.spinner("데이터 로드 중..."):
                    price_df_opt = load_price_data(ticker, str(start_date), str(end_date), "야후 파이낸스 (yfinance)", None)
                if price_df_opt is None or price_df_opt.empty:
                    st.error("데이터 로드 실패."); st.stop()
                _optuna.logging.set_verbosity(_optuna.logging.WARNING)
                progress   = st.progress(0.0, text="베이지안 탐색 실행 중...")
                trial_rows = []
                _tc        = [0]
                def _sd_objective(trial):
                    kb  = trial.suggest_float("k_buy", sd_kb_min, sd_kb_max)
                    ks  = kb if _sd_kb_same else trial.suggest_float("k_sell", sd_ks_min, sd_ks_max)
                    dv  = trial.suggest_categorical("분할수", [int(x) for x in sd_dv_vals])
                    sr  = trial.suggest_int("매도비율", int(sd_sr_min), int(sd_sr_max), step=int(sd_sr_step))
                    r   = run_backtest_stdev_fast(
                        price_df_opt, str(start_date), str(end_date),
                        sigma_period=sd_sigma_period, k_buy=kb, k_sell=ks,
                        sell_ratio=float(sr), divisions=int(dv), renewal=sd_renewal,
                        initial_capital=initial_capital,
                    )
                    if r is None: return -999.0
                    if "Calmar"   in sd_metric_key: score = r["calmar"]
                    elif "CAGR"   in sd_metric_key: score = r["cagr"] * 100
                    elif "총수익률" in sd_metric_key: score = r["total_return"] * 100
                    else:                            score = -abs(r["mdd"] * 100)
                    trial_rows.append(_sd_make_row(round(kb,3), round(ks,3), sr, dv, r))
                    _tc[0] += 1
                    if _tc[0] % max(1, int(sd_n_trials)//50) == 0:
                        progress.progress(min(_tc[0]/int(sd_n_trials),1.0),
                                          text=f"베이지안 탐색 중... {_tc[0]:,}/{int(sd_n_trials):,}")
                    return score
                study = _optuna.create_study(direction="maximize",
                                              sampler=_optuna.samplers.TPESampler(seed=42))
                study.optimize(_sd_objective, n_trials=int(sd_n_trials))
                progress.progress(1.0, text="완료!")
                if not trial_rows: st.error("유효한 결과가 없습니다."); st.stop()
                res_df = pd.DataFrame(trial_rows).sort_values(_sd_sort_col, ascending=_sd_sort_asc).reset_index(drop=True)
                best = study.best_params
                st.success(
                    f"최적 파라미터: k_buy=**{best['k_buy']:.3f}**, "
                    f"k_sell=**{best.get('k_sell', best['k_buy']):.3f}**, "
                    f"분할수=**{best.get('분할수', sd_dv_vals[0])}**, "
                    f"매도비율=**{best.get('매도비율', int(sd_sr_min))}%**"
                )
                _show_stdev_opt_results(res_df, _sd_sort_col, None, None, ticker, "bayes")
                st.subheader("탐색 수렴 과정")
                _vals     = [t.value for t in study.trials if t.value is not None and t.value > -900]
                _best_cur = [max(_vals[:i+1]) for i in range(len(_vals))]
                fig_conv  = px.line(y=_best_cur,
                                    labels={"y": f"Best {_sd_sort_col}", "index": "Trial"},
                                    title="베이지안 최적화 수렴 곡선")
                fig_conv.update_layout(height=380)
                st.plotly_chart(fig_conv, use_container_width=True)


# ══════════════════════════════════════════════
# TAB 3 -- Order Sheet & Account Management
# ══════════════════════════════════════════════
def _render_sd_account_tab(tk: str, tk_cfg: dict, key_sfx: str):
    """표준편차매매 ticker별 주문표 탭 렌더링."""
    _kb   = float(tk_cfg.get("k_buy",       0.65))
    _ks   = float(tk_cfg.get("k_sell",      0.65))
    _sr   = float(tk_cfg.get("sell_ratio",  75.0))
    _div  = int  (tk_cfg.get("divisions",   5))
    _sp   = int  (tk_cfg.get("sigma_period", 2))
    _rn   = int  (tk_cfg.get("renewal",     5))
    _pcr  = float(tk_cfg.get("pcr", 1.0))
    _lcr  = float(tk_cfg.get("lcr", 1.0))

    _raw_start   = tk_cfg.get("os_start",   "2020-01-01")
    _raw_capital = tk_cfg.get("os_capital", 20000.0)
    try:    _def_start = datetime.strptime(str(_raw_start), "%Y-%m-%d").date()
    except: _def_start = datetime(2020, 1, 1).date()
    try:    _def_cap = float(_raw_capital)
    except: _def_cap = 20000.0

    # -- 계좌 삭제 ------
    _del_c, _ = st.columns([1, 5])
    if _del_c.button(f"{tk} 계좌 삭제", key=f"sd_del_{key_sfx}", type="secondary"):
        st.session_state[f"sd_del_confirm_{key_sfx}"] = True
    if st.session_state.get(f"sd_del_confirm_{key_sfx}", False):
        st.warning(f"**{tk} 계좌를 삭제하시겠습니까?** 저장된 설정 및 매매 히스토리가 모두 삭제됩니다.")
        _dc1, _dc2, _ = st.columns([1, 1, 4])
        if _dc1.button("삭제", key=f"sd_del_ok_{key_sfx}", type="primary"):
            _delete_sd_ticker_setting(tk)
            st.session_state.pop(f"sd_del_confirm_{key_sfx}", None)
            st.rerun()
        if _dc2.button("취소", key=f"sd_del_cancel_{key_sfx}"):
            st.session_state[f"sd_del_confirm_{key_sfx}"] = False
            st.rerun()

    # -- 파라미터 표시 + 수정 ----
    with st.container(border=True):
        _p1,_p2,_p3,_p4,_p5,_p6 = st.columns(6)
        _p1.metric("k_buy",    f"{_kb:.2f}")
        _p2.metric("k_sell",   f"{_ks:.2f}")
        _p3.metric("매도비율",  f"{_sr:.0f}%")
        _p4.metric("분할수",    f"{_div}회")
        _p5.metric("sigma 기간",   f"{_sp}일")
        _p6.metric("갱신 주기", f"{_rn}주기")

        with st.expander("파라미터 수정"):
            _param_presets = _SD_PRESETS_DB.get(tk, [])
            if _param_presets:
                st.caption("추천 프리셋 -- 버튼 위에 마우스를 올리면 성과 지표를 확인할 수 있습니다.")
                # 프리셋 개수에 맞춰 동적으로 컬럼 생성 (3개 또는 4개 등)
                _n_presets = len(_param_presets)
                _preset_cols = st.columns(_n_presets)
                for _pi, (_ppc, _pp) in enumerate(zip(_preset_cols, _param_presets)):
                    if _ppc.button(_pp["label"], key=f"sd_preset_{_pi}_{key_sfx}",
                                   help=_pp["help"], use_container_width=True):
                        st.session_state[f"sd_ekb_{key_sfx}"]  = _pp["k_buy"]
                        st.session_state[f"sd_eks_{key_sfx}"]  = _pp["k_sell"]
                        st.session_state[f"sd_esr_{key_sfx}"]  = _pp["sell_ratio"]
                        st.session_state[f"sd_edv_{key_sfx}"]  = _pp["divisions"]
                        # sigma_period / renewal도 함께 강제 적용 (프리셋 정확성 보장)
                        if "sigma_period" in _pp:
                            st.session_state[f"sd_esp_{key_sfx}"] = _pp["sigma_period"]
                        if "renewal" in _pp:
                            st.session_state[f"sd_ern_{key_sfx}"] = _pp["renewal"]
                        st.rerun()
                st.divider()
            else:
                st.caption("이 종목은 추천 프리셋이 없습니다. 직접 파라미터를 입력해 주세요.")
                st.divider()
            for _sk2, _sv2 in [(f"sd_ekb_{key_sfx}", _kb),  (f"sd_eks_{key_sfx}", _ks),
                                (f"sd_esr_{key_sfx}", _sr),  (f"sd_edv_{key_sfx}", _div),
                                (f"sd_esp_{key_sfx}", _sp),  (f"sd_ern_{key_sfx}", _rn)]:
                if _sk2 not in st.session_state:
                    st.session_state[_sk2] = _sv2
            _ep1, _ep2 = st.columns(2)
            _new_kb  = _ep1.number_input("k_buy",        step=0.05, format="%.2f", key=f"sd_ekb_{key_sfx}")
            _new_ks  = _ep2.number_input("k_sell",       step=0.05, format="%.2f", key=f"sd_eks_{key_sfx}")
            _new_sr  = _ep1.number_input("매도비율 (%)", step=5.0, min_value=5.0, max_value=100.0,
                                          key=f"sd_esr_{key_sfx}")
            _new_div = _ep2.number_input("분할수",        min_value=1, step=1,  key=f"sd_edv_{key_sfx}")
            _new_sp  = _ep1.number_input("sigma 계산 기간",  min_value=1, max_value=20, step=1,
                                          key=f"sd_esp_{key_sfx}")
            _new_rn  = _ep2.number_input("갱신 주기",    min_value=1, step=1,  key=f"sd_ern_{key_sfx}")
            if st.button("파라미터 저장", key=f"sd_save_{key_sfx}", type="primary",
                         use_container_width=True):
                _save_sd_ticker_setting(tk, {
                    "k_buy": float(_new_kb), "k_sell": float(_new_ks),
                    "sell_ratio": float(_new_sr), "divisions": int(_new_div),
                    "sigma_period": int(_new_sp), "renewal": int(_new_rn),
                })
                for _sk3 in [f"sd_ekb_{key_sfx}", f"sd_eks_{key_sfx}", f"sd_esr_{key_sfx}",
                              f"sd_edv_{key_sfx}", f"sd_esp_{key_sfx}", f"sd_ern_{key_sfx}"]:
                    st.session_state.pop(_sk3, None)
                st.success(f"{tk} 파라미터가 저장되었습니다!")
                st.rerun()

    # -- 시작일 / 자본금 -----
    _sc1, _sc2 = st.columns(2)
    _os_start = _sc1.date_input("시작일", value=_def_start,
                                 min_value=datetime(2000, 1, 1).date(),
                                 max_value=datetime.today().date(),
                                 key=f"sd_os_start_{key_sfx}")
    _os_cap   = _sc2.number_input("시작 자본 ($)", value=_def_cap,
                                   step=1000.0, key=f"sd_os_cap_{key_sfx}")

    # -- 자본 조정 (증액 / 감액) -----
    with st.expander("자본 조정 (증액 / 감액)"):
        st.caption("현재 자본금에 추가하거나 차감할 금액을 입력하세요. 날짜를 선택해 과거 항목도 입력 가능합니다.")
        _sd_adj_raw = tk_cfg.get("capital_adj_history", "[]")
        try:
            _sd_adj_hist = json.loads(_sd_adj_raw) if isinstance(_sd_adj_raw, str) else _sd_adj_raw
            if not isinstance(_sd_adj_hist, list): _sd_adj_hist = []
        except: _sd_adj_hist = []

        _cur_adj_sum = sum(float(it.get("조정금액", 0)) for it in _sd_adj_hist)
        _cur_total = _def_cap + _cur_adj_sum

        _sadj_c1, _sadj_c2 = st.columns([2, 1])
        _sadj_date = _sadj_c1.date_input("적용 날짜", value=datetime.today().date(),
                                          key=f"sd_adj_date_{key_sfx}",
                                          help="실제 입금/출금이 일어난 날짜")
        _sadj_amt = _sadj_c1.number_input("조정 금액 ($)", value=0.0, step=500.0,
                                           help="증액: 양수 / 감액: 음수",
                                           key=f"sd_adj_inp_{key_sfx}")
        _sadj_c1.caption(
            f"현재 자본금: **${_cur_total:,.0f}** → 적용 후: **${_cur_total + _sadj_amt:,.0f}** "
            f"({'up' if _sadj_amt > 0 else 'down' if _sadj_amt < 0 else '='} "
            f"${abs(_sadj_amt):,.0f})"
        )
        _sadj_memo = _sadj_c1.text_input("메모 (선택)", placeholder="예: 3월 추가 입금",
                                          key=f"sd_adj_memo_{key_sfx}")
        # 중복 적용 가드 — 같은 날짜·금액이 이미 있으면 실수 재클릭일 가능성
        _dup_exists_sd = any(
            str(_e.get("날짜", "")) == _sadj_date.strftime("%Y-%m-%d")
            and abs(float(_e.get("조정금액", 0) or 0) - float(_sadj_amt)) < 0.01
            for _e in _sd_adj_hist) if _sadj_amt != 0 else False
        _dup_ok_sd = True
        if _dup_exists_sd:
            _dup_ok_sd = _sadj_c1.checkbox(
                "⚠️ 같은 날짜·금액 이력이 이미 있습니다 — 실제로 한 번 더 "
                "입출금한 경우에만 체크 후 적용하세요",
                value=False, key=f"sd_dup_ok_{key_sfx}")
        if _sadj_c2.button("적용", use_container_width=True,
                            key=f"sd_apply_adj_{key_sfx}", disabled=(_sadj_amt == 0)):
            if _cur_total + _sadj_amt <= 0:
                st.error("자본금은 0보다 커야 합니다.")
            elif _dup_exists_sd and not _dup_ok_sd:
                st.error("⛔ 같은 날짜·금액 이력이 이미 있어 적용하지 않았습니다. "
                         "(이미 반영된 입출금의 중복 클릭 방지) 실제 추가 입출금이면 "
                         "위 체크박스를 켠 뒤 다시 적용하세요.")
            else:
                _new_entry = {
                    "날짜": _sadj_date.strftime("%Y-%m-%d"),
                    "조정금액": float(_sadj_amt),
                    "누적자본금": 0.0,
                    "메모": _sadj_memo or ("증액" if _sadj_amt > 0 else "감액"),
                }
                # 원장 활성 시 즉시 반영 — 예수금·총자산·총투자금 가산
                # (원장반영 플래그로 주문 계산의 날짜 필터 이중 합산 방지)
                _baked, _bk_st = _bake_adj_into_sd_ledger(tk, float(_sadj_amt))
                if _baked:
                    _new_entry["원장반영"] = True
                _sd_adj_hist.append(_new_entry)
                _sd_adj_hist, _final_cap = _recalc_adj_history(_sd_adj_hist, _def_cap)
                _save_sd_ticker_setting(tk, {
                    "capital_adj_history": json.dumps(_sd_adj_hist, ensure_ascii=False),
                })
                st.session_state.pop(f"sd_os_res_{key_sfx}", None)  # 캐시 갱신
                if _baked:
                    st.success(f"✅ {_sadj_date} 자본 조정 완료 — **원장에 즉시 반영**되었습니다. "
                               f"다음 주문부터 1회매수금·매수량에 적용됩니다. "
                               f"(오늘 이미 발송된 주문 수량은 그대로)")
                else:
                    st.success(f"✅ {_sadj_date} 자본 조정 완료. 현재 자본금: **${_final_cap:,.0f}**")
                st.rerun()

        if _sd_adj_hist:
            st.markdown("---")
            st.markdown("**자본 조정 이력** (직접 수정 가능 — 날짜/금액/메모 편집 · 행 삭제)")
            _df_sadj_edit = pd.DataFrame(_sd_adj_hist)
            _df_sadj_edit["날짜"] = pd.to_datetime(_df_sadj_edit["날짜"]).dt.date
            _df_sadj_edit["조정금액"] = _df_sadj_edit["조정금액"].astype(float)
            # 원장반영 플래그 왕복 보존 (편집 저장 시 유실되면 이중 합산 발생)
            if "원장반영" not in _df_sadj_edit.columns:
                _df_sadj_edit["원장반영"] = False
            _df_sadj_edit["원장반영"] = (_df_sadj_edit["원장반영"]
                                          .fillna(False).astype(bool))
            _sd_edited = st.data_editor(
                _df_sadj_edit[["날짜", "조정금액", "메모", "원장반영"]],
                column_config={
                    "날짜": st.column_config.DateColumn("날짜", format="YYYY-MM-DD", required=True),
                    "조정금액": st.column_config.NumberColumn("조정금액 ($)",
                                                              format="$%.0f", required=True),
                    "메모": st.column_config.TextColumn("메모"),
                    "원장반영": st.column_config.CheckboxColumn(
                        "원장반영", disabled=True,
                        help="적용 시 원장(매매기록)에 이미 가산된 항목 표시 — 수정 불가"),
                },
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True,
                key=f"sd_adj_editor_{key_sfx}",
            )
            _preview_list = []
            for _, _r in _sd_edited.iterrows():
                if pd.isna(_r.get("날짜")) or pd.isna(_r.get("조정금액")):
                    continue
                _preview_list.append({
                    "날짜": pd.Timestamp(_r["날짜"]).strftime("%Y-%m-%d"),
                    "조정금액": float(_r["조정금액"]),
                    "누적자본금": 0.0,
                    "메모": str(_r.get("메모") or ""),
                    "원장반영": bool(_r.get("원장반영", False)),
                })
            _preview_list, _preview_cap = _recalc_adj_history(
                _preview_list, _def_cap)
            if _preview_list:
                _df_preview = pd.DataFrame(_preview_list)
                _df_preview_show = _df_preview.copy()
                _df_preview_show["조정금액"] = _df_preview["조정금액"].apply(
                    lambda x: f"{'up' if x > 0 else 'down'} ${abs(x):,.0f}")
                _df_preview_show["누적자본금"] = _df_preview["누적자본금"].apply(
                    lambda x: f"${x:,.0f}")
                st.caption(f"📊 **미리보기** (저장 후 반영됩니다) — 최종 자본금: **${_preview_cap:,.0f}**")
                st.dataframe(_df_preview_show[["날짜", "조정금액", "누적자본금", "메모"]],
                             use_container_width=True, hide_index=True)

            if st.button("💾 변경사항 저장", key=f"sd_save_adj_edit_{key_sfx}",
                         type="primary"):
                if _preview_cap <= 0:
                    st.error(f"현재 자본금이 0 이하가 됩니다 (${_preview_cap:,.0f}). 수정 불가.")
                else:
                    # 원장반영 항목의 삭제·금액수정 → 원장에도 대칭 반영 (되돌리기)
                    # 적용이 원장에 더했으니, 삭제하면 빼고 수정하면 차액만 반영
                    _bake_delta = 0.0
                    _valid_kept = set()
                    for _ix, _r in _sd_edited.iterrows():
                        if pd.isna(_r.get("날짜")) or pd.isna(_r.get("조정금액")):
                            continue
                        if isinstance(_ix, int) and _ix < len(_sd_adj_hist):
                            _valid_kept.add(_ix)
                            _old_e = _sd_adj_hist[_ix]
                            if _old_e.get("원장반영"):
                                _bake_delta += (float(_r["조정금액"])
                                                 - float(_old_e.get("조정금액", 0) or 0))
                    for _oix, _old_e in enumerate(_sd_adj_hist):
                        if _oix not in _valid_kept and _old_e.get("원장반영"):
                            _bake_delta -= float(_old_e.get("조정금액", 0) or 0)
                    _unbake_msg = ""
                    if abs(_bake_delta) > 0.005:
                        _bk_ok2, _ = _bake_adj_into_sd_ledger(tk, _bake_delta)
                        if not _bk_ok2:
                            st.error("⛔ 원장 반영 실패 — 이력 변경을 저장하지 않았습니다. "
                                     "잠시 후 다시 시도해주세요.")
                            st.stop()
                        _unbake_msg = (f"  ·  원장에도 반영: "
                                       f"{'+' if _bake_delta > 0 else ''}${_bake_delta:,.0f} "
                                       f"(삭제/수정분 되돌림)")
                    _save_sd_ticker_setting(tk, {
                        "capital_adj_history": json.dumps(_preview_list, ensure_ascii=False),
                    })
                    st.session_state.pop(f"sd_os_res_{key_sfx}", None)  # 캐시 갱신
                    st.success(f"✅ 이력 업데이트 완료. 현재 자본금: **${_preview_cap:,.0f}**{_unbake_msg}")
                    st.rerun()
        else:
            st.info("아직 자본 조정 이력이 없습니다.")

        # 전체 초기화
        st.markdown("---")
        st.markdown("**전체 초기화**")
        st.caption("시작일/자본금/조정 이력을 모두 초기화합니다.")
        # 옵션: 매매기록 함께 삭제
        _sreset_wipe_hist = st.checkbox(
            "🗑️ 매매기록(GSheet 워크시트 + 로컬 CSV)도 완전 삭제",
            value=False, key=f"sd_reset_wipe_{key_sfx}",
            help="체크 시 'sd_{ticker}_매매기록' GSheet 시트와 로컬 history CSV가 모두 삭제됩니다.\n"
                 "(파라미터 변경 + 완전 새 시작 시 권장)",
        )
        _src1, _src2, _src3 = st.columns(3)
        _sreset_start   = _src1.date_input("새 시작일", value=datetime.today().date(),
                                            key=f"sd_reset_start_{key_sfx}")
        _sreset_capital = _src2.number_input("새 시작 자본 ($)", value=_def_cap,
                                              step=1000.0, key=f"sd_reset_cap_{key_sfx}")
        if _src3.button("초기화", use_container_width=True,
                        key=f"sd_do_reset_{key_sfx}", type="secondary"):
            st.session_state[f"sd_reset_confirm_{key_sfx}"] = True
        if st.session_state.get(f"sd_reset_confirm_{key_sfx}", False):
            _wipe_msg = " / **매매기록 전체 삭제**" if _sreset_wipe_hist else ""
            st.warning(f"**정말 초기화하시겠습니까?**  \n"
                       f"시작일: {_sreset_start} / 자본금: ${_sreset_capital:,.0f} / 조정 이력 전체 삭제{_wipe_msg}")
            _sc1r, _sc2r = st.columns(2)
            if _sc1r.button("확인 (초기화)", type="primary", key=f"sd_confirm_reset_{key_sfx}"):
                _save_sd_ticker_setting(tk, {
                    "os_start": str(_sreset_start),
                    "os_capital": float(_sreset_capital),
                    "capital_adj_history": "[]",
                })
                # 매매기록 삭제 (옵션)
                _hist_msg = ""
                if _sreset_wipe_hist:
                    _local_ok, _cloud_ok, _err = _clear_sd_daily_history(tk)
                    if _err:
                        _hist_msg = f"\n⚠️ 매매기록 삭제 일부 실패: {_err}"
                    else:
                        _hist_msg = "\n🗑️ 매매기록 완전 삭제 완료 (GSheet 워크시트 + 로컬 CSV)"
                # 주문표 캐시도 초기화
                st.session_state.pop(f"sd_os_res_{key_sfx}", None)
                st.session_state[f"sd_reset_confirm_{key_sfx}"] = False
                st.success(f"초기화 완료! 시작일: {_sreset_start} / 자본금: ${_sreset_capital:,.0f}{_hist_msg}")
                st.rerun()
            if _sc2r.button("취소", key=f"sd_cancel_reset_{key_sfx}"):
                st.session_state[f"sd_reset_confirm_{key_sfx}"] = False
                st.rerun()

    _sd_ss  = f"sd_os_res_{key_sfx}"
    # -- 현재 보유 상태 보정 (실제 계좌와 원장 맞추기) ------
    with st.expander("🔧 현재 보유 상태 보정 (실제 계좌와 맞추기)"):
        st.caption("실제 증권사 계좌의 **현재 보유주수 · 예수금 · 평단가**를 입력하면 매매기록 앵커를 "
                   "그 값으로 바로잡습니다. 잘못된 '예정' 행은 제거되고, 다음 주문부터 실제 보유 기준으로 "
                   "매수·매도 수량이 계산됩니다. (총투자금/티어 등 전략 상태는 그대로 이어집니다.)")
        _fix_led = _load_sd_daily_history(tk)
        _pf_h, _pf_c, _pf_a, _pf_cx, _pf_ti = 0, 0.0, 0.0, 0.0, 0.0
        if not _fix_led.empty and "종가" in _fix_led.columns:
            _conf = _fix_led[_fix_led["종가"].apply(
                lambda v: str(v).strip() not in ("", "-", "None", "nan"))]
            if not _conf.empty:
                _lastc = _conf.iloc[-1]
                _pf_h = int(pd.to_numeric(_lastc.get("보유량"), errors="coerce") or 0)
                _pf_c = float(pd.to_numeric(_lastc.get("예수금"), errors="coerce") or 0)
                _pf_a = float(pd.to_numeric(_lastc.get("평단가"), errors="coerce") or 0)
                _pf_cx = float(pd.to_numeric(_lastc.get("종가"), errors="coerce") or 0)
                _pf_ti = float(pd.to_numeric(_lastc.get("총투자금"), errors="coerce") or 0)
        _fx1, _fx2, _fx3 = st.columns(3)
        _in_h = _fx1.number_input("실제 보유주수", value=_pf_h, min_value=0, step=1,
                                   key=f"sd_fix_h_{key_sfx}")
        _in_c = _fx2.number_input("실제 예수금 ($)", value=_pf_c, step=100.0,
                                   key=f"sd_fix_c_{key_sfx}")
        _in_a = _fx3.number_input("실제 평단가 ($)", value=_pf_a, step=0.01,
                                   format="%.4f", key=f"sd_fix_a_{key_sfx}")
        st.markdown("**복리 기준금액(총투자금) 설정** — 표준편차는 `1회매수금 = 총투자금 ÷ 분할수`")
        _divN = int(_div) if _div > 0 else 1
        _ti_mode = st.radio(
            "총투자금 처리 방식", label_visibility="collapsed",
            options=["1회매수금 직접 입력", "현재 총자산으로 재설정", "기존 값 유지"],
            index=0, horizontal=True, key=f"sd_fix_timode_{key_sfx}",
            help="· 1회매수금 직접 입력: 텔레그램 주문표의 1회매수금과 똑같이 맞추고 싶을 때 (권장)\n"
                 "· 현재 총자산으로 재설정: 계좌 전액을 분할 배분하고 싶을 때 (총투자금=총자산)\n"
                 "· 기존 값 유지: 총투자금은 그대로, 보유·현금만 보정")
        _in_chunk = 0.0
        if _ti_mode == "1회매수금 직접 입력":
            _in_chunk = st.number_input(
                f"1회매수금 ($) — 텔레그램 주문표의 '1회매수금'과 동일하게 (분할수 {_divN}회)",
                value=round(_pf_ti / _divN, 2) if _pf_ti > 0 else 0.0,
                min_value=0.0, step=100.0, key=f"sd_fix_chunk_{key_sfx}")

        # ── 보정 결과 실시간 미리보기 (자릿수 오입력 방지) ──
        _pv_close = _pf_cx if _pf_cx > 0 else float(_in_a or 0)
        _pv_tot = float(_in_c) + int(_in_h) * _pv_close
        if _ti_mode == "1회매수금 직접 입력":
            _pv_ti = float(_in_chunk) * _divN
        elif _ti_mode == "현재 총자산으로 재설정":
            _pv_ti = _pv_tot
        else:
            _pv_ti = _pf_ti if _pf_ti > 0 else _pv_tot
        _pv_sell = int(round(int(_in_h) * _sr / 100.0)) if int(_in_h) > 0 else 0
        st.info(f"**보정 후 예상** → 총자산 **${_pv_tot:,.0f}** · 총투자금 **${_pv_ti:,.0f}** · "
                f"1회매수금 **${_pv_ti/_divN:,.0f}** · 오늘 매도 예상 **{_pv_sell}주** ({_sr:.0f}%)")
        if _pf_c > 0 and (float(_in_c) > _pf_c * 5 or (float(_in_c) > 0 and float(_in_c) < _pf_c / 5)):
            st.warning(f"⚠️ 입력한 예수금(${_in_c:,.0f})이 기존 기록(${_pf_c:,.0f})과 크게 다릅니다. "
                       f"**자릿수(0 개수)를 확인하세요!**")

        _fix_clicked = st.button("✅ 현재 상태로 보정", type="secondary",
                                  key=f"sd_do_fix_{key_sfx}")
        if _fix_clicked and _ti_mode == "1회매수금 직접 입력" and float(_in_chunk) <= 0:
            st.error("⛔ 1회매수금을 입력하세요 (0보다 커야 합니다).")
            _fix_clicked = False
        if _fix_clicked:
            # 🔒 엄격 재로드: 보정은 최신 원장 위에서만 (실패 시 중단)
            _fix_df2, _fix_st = _load_sd_ledger(tk)
            if _fix_st == "no_url":
                st.error("⛔ 보정은 원장(구글시트) 저장이 필요합니다 — 설정 탭에서 "
                         "'스프레드시트 URL'을 먼저 등록해주세요. "
                         "(주문표 보기만 하려면 보정 없이 그대로 쓰시면 됩니다)")
                st.stop()
            elif _fix_st != "ok":
                st.error("⛔ 원장(GSheets) 접근 실패 — 보정을 진행하지 않았습니다. "
                         "잠시 후 다시 시도해주세요.")
                st.stop()
            _rows_fix = _fix_df2.to_dict("records") if not _fix_df2.empty else []
            # 1) 예정(미확정) 행 제거
            _rows_fix = [r for r in _rows_fix
                         if str(r.get("종가", "")).strip() not in ("", "-", "None", "nan")]
            # 1-2) 중복 날짜 제거 (오염으로 같은 날짜가 여러 번 저장된 경우 첫 행 유지)
            _seen_d = set()
            _rows_fix = [r for r in _rows_fix
                         if str(r.get("날짜", "")).strip() not in _seen_d
                         and not _seen_d.add(str(r.get("날짜", "")).strip())]
            _new_ti = None
            _divN = int(_div) if _div > 0 else 1
            _reset_clock = _ti_mode in ("1회매수금 직접 입력", "현재 총자산으로 재설정")
            if _rows_fix:
                # 2) 마지막 확정 행 보정 (티어 유지)
                _rows_fix.sort(key=lambda r: str(r.get("날짜", "")))
                _lf = _rows_fix[-1]
                _cx = pd.to_numeric(_lf.get("종가"), errors="coerce")
                _cx = float(_cx) if pd.notna(_cx) else float(_in_a)
                _tot_asset = round(float(_in_c) + int(_in_h) * _cx, 2)
                _lf["보유량"] = int(_in_h)
                _lf["예수금"] = round(float(_in_c), 2)
                _lf["평단가"] = round(float(_in_a), 4)
                _lf["총자산"] = _tot_asset
                if _ti_mode == "1회매수금 직접 입력":
                    _new_ti = round(float(_in_chunk) * _divN, 2)
                    _lf["총투자금"] = _new_ti
                elif _ti_mode == "현재 총자산으로 재설정":
                    _new_ti = _tot_asset
                    _lf["총투자금"] = _tot_asset
                else:  # 기존 값 유지
                    _new_ti = pd.to_numeric(_lf.get("총투자금"), errors="coerce")
                    _new_ti = float(_new_ti) if pd.notna(_new_ti) else _tot_asset
                # 총투자금을 바꿨으면 누적실현 클럭 리셋 (renewal 이중계산 방지)
                if _reset_clock:
                    for _r in _rows_fix:
                        _r["누적실현"] = 0
            else:
                # 확정 행 없음 — 어제 날짜로 앵커 신규 생성
                _anchor_d = (datetime.today().date() - timedelta(days=1)).strftime("%Y-%m-%d")
                _tot_asset = round(float(_in_c) + int(_in_h) * float(_in_a), 2)
                if _ti_mode == "1회매수금 직접 입력":
                    _new_ti = round(float(_in_chunk) * _divN, 2)
                elif _ti_mode == "현재 총자산으로 재설정":
                    _new_ti = _tot_asset
                else:
                    _new_ti = round(float(_os_cap), 2)
                _row0 = {c: "" for c in SD_HIST_COLS}
                _row0.update({"날짜": _anchor_d, "티어": 1, "종가": round(float(_in_a), 4),
                              "매수량": 0, "매도량": 0, "보유량": int(_in_h),
                              "평단가": round(float(_in_a), 4), "누적실현": 0,
                              "총투자금": round(float(_new_ti), 2),
                              "예수금": round(float(_in_c), 2),
                              "총자산": _tot_asset})
                _rows_fix = [_row0]
            _rw_ok, _rw_err = _rewrite_sd_daily_history(tk, _rows_fix)
            if not _rw_ok:
                st.error(f"⛔ 보정 저장 실패 — 원장(GSheets)에 기록되지 않았습니다. "
                         f"다시 시도해주세요. (원인: {_rw_err})")
                st.stop()
            st.session_state.pop(_sd_ss, None)   # 세션 캐시 제거 → 재계산
            _div_msg = int(_div) if _div > 0 else 1
            st.success(f"✅ 보정 완료: 보유 {int(_in_h):,}주 / 예수금 ${_in_c:,.0f} / "
                       f"평단 ${_in_a:.2f}"
                       + (f" · 총투자금 ${_new_ti:,.0f} → 1회매수금 ${_new_ti/_div_msg:,.0f}"
                          if _new_ti else "")
                       + "  ·  '주문표 로드'를 다시 눌러주세요.")
            st.rerun()

    # -- 주문표 로드 ------
    _sd_lbl = "새로고침" if st.session_state.get(_sd_ss) else "주문표 로드"
    if st.button(_sd_lbl, type="primary", key=f"sd_run_os_{key_sfx}"):
        _save_sd_ticker_setting(tk, {"os_start": str(_os_start), "os_capital": _os_cap})
        with st.spinner("데이터 로드 및 시뮬레이션 중..."):
            _buf_start = (_os_start - timedelta(days=90))
            _pdf = load_price_data(tk, str(_buf_start), str(datetime.today().date()),
                                   "야후 파이낸스 (yfinance)", None)
        # 전일 종가 미확보(yfinance+백업 실패) 시 주문표 생성 차단
        from common.data import halt_if_stale
        if _pdf is None or _pdf.empty:
            st.error("가격 데이터를 불러오지 못했습니다.")
        elif halt_if_stale(_pdf):
            pass  # halt_if_stale이 에러 표시 — 주문표 미생성
        else:
            _sd_r = run_stdev_ordersheet(
                price_df=_pdf, start_date=str(_os_start),
                sigma_period=_sp, k_buy=_kb, k_sell=_ks,
                sell_ratio=_sr, divisions=_div, renewal=_rn,
                pcr=_pcr, lcr=_lcr,
                initial_capital=_os_cap,
            )
            if _sd_r is None:
                st.error("주문표 계산 실패. 데이터 기간을 늘려보세요.")
            else:
                # 자본 조정 반영 (엔진은 os_capital로만 시뮬, 조정은 별도 합산)
                _adj_raw_sd = tk_cfg.get("capital_adj_history", "[]")
                try:
                    _adj_list_sd = json.loads(_adj_raw_sd) if isinstance(_adj_raw_sd, str) else _adj_raw_sd
                    if not isinstance(_adj_list_sd, list):
                        _adj_list_sd = []
                except Exception:
                    _adj_list_sd = []
                _end_ts_sd = pd.Timestamp(datetime.today().date())
                _adj_applied_sd = 0.0
                for _it in _adj_list_sd:
                    try:
                        _dt = pd.Timestamp(_it.get("날짜"))
                        if _dt <= _end_ts_sd:
                            _adj_applied_sd += float(_it.get("조정금액", 0))
                    except Exception:
                        continue
                _sd_r["cash"] = float(_sd_r.get("cash", 0)) + _adj_applied_sd
                _sd_r["final_asset"] = float(_sd_r.get("final_asset", 0)) + _adj_applied_sd
                # total_invest도 조정 합산 → daily_invest (1회매수금) 정확화
                # 사용자 입금 시 실제 매수 가능 금액도 증가해야 함
                _sd_r["total_invest"] = float(_sd_r.get("total_invest", 0)) + _adj_applied_sd
                _sd_r["adj_applied"] = _adj_applied_sd
                # est_buy_qty 재계산 (조정 반영된 total_invest/cash 기준)
                # 엔진의 est_buy_qty는 시뮬 값 → 조정 반영 시 예상수량도 증가
                _daily_inv_adj_sd = (_sd_r["total_invest"] / _div) if _div > 0 else _sd_r["total_invest"]
                _avail_adj_sd = max(0.0, min(_daily_inv_adj_sd, _sd_r["cash"]))
                _nbl_sd = float(_sd_r.get("next_buy_loc", 0))
                if _nbl_sd > 0:
                    _sd_r["est_buy_qty"] = int(math.floor(_avail_adj_sd / _nbl_sd))
                # ══ 원장(매매기록) 처리 — 기록이 진실, 시뮬은 참고 ══
                # 1) 원장 비면 과거 시뮬 이력으로 시드 (오늘 제외)
                # 2) 예정 행을 그날 실제 종가로 체결/미체결 정산 (동시체결, 수량 불변)
                # 3) 원장 상태(티어/총투자금/보유/현금/누적실현) + 엔진 룰 → 오늘 주문
                # 4) 오늘 '예정' 행 저장 (자동발송이 이미 저장했으면 그 수량 사용)
                _today_sd = datetime.today().strftime("%Y-%m-%d")
                _closes_sd_map = {}
                _hist_sd = _sd_r.get("hist")
                if _hist_sd is not None and not _hist_sd.empty:
                    for _, _hr in _hist_sd.iterrows():
                        _closes_sd_map[str(_hr["날짜"])] = float(_hr["종가"])

                # 🔒 엄격 로드: 원장 소스 상태에 따라 처리 분기.
                # (실패를 빈 원장으로 오인 → 시뮬 재시드 → 원장 오염 사고 방지)
                _df_led0, _led_st = _load_sd_ledger(tk)
                _led_ok = (_led_st == "ok")
                if _led_st == "no_url":
                    st.info("📄 **원장 미사용 모드** — 구글시트 URL이 설정되지 않아 주문표를 "
                            "시뮬레이션 기준으로만 표시합니다 (기록/정산/보정 비활성). "
                            "실제 매매를 운용하시면 설정 탭에서 스프레드시트 URL을 등록하세요 — "
                            "실제 체결 기준으로 수량이 관리되어 훨씬 정확합니다.")
                elif _led_st == "error":
                    st.error("⛔ 원장(매매기록 GSheets) 접근 실패 — 이번 로드는 시뮬 참고값만 "
                             "표시하며 **원장 기록/정산/주문 수량 반영을 하지 않습니다.** "
                             "잠시 후 '새로고침'을 다시 눌러주세요. (주문은 원장 반영된 값으로만!)")
                if _led_ok and _df_led0.empty and _hist_sd is not None and not _hist_sd.empty:
                    # 원장 신규 시작 — 과거 시뮬 이력 시드 (오늘 제외)
                    # (엄격 로드 성공 + 진짜 빈 원장일 때만)
                    _seed = _hist_sd[_hist_sd["날짜"].astype(str) != _today_sd].copy()
                    _save_sd_daily_history(tk, _seed)
                    _df_led0, _led_st = _load_sd_ledger(tk)
                    _led_ok = (_led_st == "ok")
                _rows_led = (_df_led0.to_dict("records")
                             if _led_ok and not _df_led0.empty else [])

                # 2) 예정 정산 (오늘 이전분만)
                if _led_ok:
                    try:
                        _cc = {d: c for d, c in _closes_sd_map.items() if d < _today_sd}
                        _chg = settle_sd_pending_rows(_rows_led, _cc)
                        if _chg:
                            _update_sd_daily_history_rows(tk, _rows_led, _chg)
                            for _ci in _chg:
                                st.info(f"🧾 {_rows_led[_ci].get('날짜')} 예정 정산 → "
                                        f"매수 {_rows_led[_ci].get('매수량')} / "
                                        f"매도 {_rows_led[_ci].get('매도량')}주")
                    except Exception as _stl:
                        st.warning(f"예정 정산 실패 (다음 로드 때 재시도): {_stl}")

                # 3) 원장 상태 → 오늘 주문 수량 (엔진 룰: 티어/renewal 포함)
                _sd_ledger = None
                try:
                    _prev_led = sorted([r0 for r0 in _rows_led
                                        if str(r0.get("날짜", "")).strip() < _today_sd],
                                       key=lambda r0: str(r0.get("날짜", "")))
                    _stg = calc_sd_record_state(_prev_led)
                    if _stg:
                        # 마지막 기록 이후 입출금(자본 조정)만 현금에 반영
                        # ('원장반영' 플래그 항목은 이미 원장에 가산됨 → 스킵)
                        _extra = 0.0
                        for _it in _adj_list_sd:
                            try:
                                if _it.get("원장반영"):
                                    continue
                                _da = str(pd.Timestamp(_it.get("날짜")).date())
                                if _stg["last_date"] < _da <= _today_sd:
                                    _extra += float(_it.get("조정금액", 0))
                            except Exception:
                                continue
                        _stg["cash"] += _extra
                        _og = calc_sd_order_from_state(
                            _stg, float(_sd_r.get("next_buy_loc", 0)),
                            float(_sd_r.get("next_sell_loc", 0)),
                            _div, _sr, _rn, _pcr, _lcr)
                        _sd_ledger = {**_stg, **_og}
                except Exception:
                    _sd_ledger = None

                # 오늘 예정 행이 이미 있으면 (자동발송분) 그 수량이 오늘 주문
                _today_row_led = next((r0 for r0 in _rows_led
                                       if str(r0.get("날짜", "")).strip() == _today_sd), None)
                if _today_row_led is not None and _sd_ledger and sd_row_is_pending(_today_row_led):
                    try:
                        _sd_ledger["buy_qty"] = int(float(_today_row_led.get("매수량", 0)))
                        _sd_ledger["sell_qty"] = int(float(_today_row_led.get("매도량", 0)))
                    except Exception:
                        pass

                # res 오버라이드 → 이후 화면(주문표·보유현황)이 원장 값 사용
                if _sd_ledger:
                    _sim_h = int(_sd_r.get("holdings", 0))
                    if _sim_h != _sd_ledger["holdings"]:
                        st.caption(f"📒 원장 기준 적용: 시뮬 보유 {_sim_h:,}주 → "
                                   f"매매기록 보유 {_sd_ledger['holdings']:,}주 "
                                   f"(실제 계좌와 다르면 매매기록 시트를 수정하세요)")
                    _sd_r["holdings"]     = _sd_ledger["holdings"]
                    _sd_r["cash"]         = round(_sd_ledger["cash"], 2)
                    _sd_r["total_invest"] = round(_sd_ledger["total_invest"], 2)
                    _sd_r["next_tier"]    = _sd_ledger["next_tier"]
                    _sd_r["est_buy_qty"]  = _sd_ledger["buy_qty"]
                    _sd_r["est_sell_qty"] = _sd_ledger["sell_qty"]
                    if _sd_ledger.get("avg_cost", 0) > 0:
                        _sd_r["avg_cost"] = round(_sd_ledger["avg_cost"], 4)
                    # 총자산도 원장 기준으로 갱신 (시뮬 final_asset이 아니라 실제 현금+보유평가)
                    _lc_disp = float(_sd_r.get("last_close", 0))
                    _sd_r["final_asset"] = round(
                        _sd_ledger["cash"] + _sd_ledger["holdings"] * _lc_disp, 2)

                # 4) 오늘 '예정' 행 저장 (주문 있고 + 오늘 행 없을 때만, B방식)
                if _sd_ledger and _today_row_led is None \
                        and (_sd_ledger["buy_qty"] > 0 or _sd_ledger["sell_qty"] > 0):
                    _prow = build_sd_pending_row(
                        _today_sd, int(_sd_r.get("next_tier", 1)),
                        float(_sd_r.get("sigma_next", 0)),
                        float(_sd_r.get("next_buy_loc", 0)),
                        float(_sd_r.get("next_sell_loc", 0)),
                        _sd_ledger["buy_qty"], _sd_ledger["sell_qty"],
                        float(_sd_r.get("total_invest", 0)),
                        _sd_ledger["holdings"], _sd_ledger["cash"],
                        _sd_ledger.get("avg_cost", 0), _sd_ledger["cum_realized"],
                        float(_sd_r.get("last_close", 0)))
                    _save_sd_daily_history(tk, pd.DataFrame([_prow]))

                st.session_state[_sd_ss] = _sd_r

    _sd_res = st.session_state.get(_sd_ss)
    if _sd_res is None:
        return

    # -- 기간 표시 ------
    st.markdown(f"**{_os_start} ~ {datetime.today().date()}**")

    # -- 성과 요약 (5 metrics) ----
    _final_a = _sd_res["final_asset"]
    _cagr    = _sd_res["cagr"]
    _mdd     = _sd_res["mdd"]
    _lc      = _sd_res["last_close"]
    _holdings = _sd_res["holdings"]
    _avg_c   = _sd_res["avg_cost"]
    _cash    = _sd_res["cash"]

    # 실현손익 합산 (hist에서)
    _hdf = _sd_res.get("hist")
    _realized_pnl = 0.0
    if _hdf is not None and not (hasattr(_hdf, "empty") and _hdf.empty) and "실현손익" in _hdf.columns:
        _realized_pnl = float(_hdf["실현손익"].sum())
    _realized_pct = (_realized_pnl / _os_cap * 100) if _os_cap else 0.0

    # ── 선택한 파라미터 카드 (4장, DSS 스타일) ──
    _sig_next_pct = _sd_res.get("sigma_next", 0) * 100
    _nxt_t_for_card = _sd_res.get("next_tier", 1)
    st.markdown(f"""
    <div style="display:flex;gap:10px;margin-bottom:8px">
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:14px 18px;text-align:center">
        <div style="font-size:0.72em;color:#888;margin-bottom:2px">σ (오늘 예상)</div>
        <div style="font-size:1.15em;font-weight:700;color:#333">{_sig_next_pct:.4f}%</div>
      </div>
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:14px 18px;text-align:center">
        <div style="font-size:0.72em;color:#888;margin-bottom:2px">σ 기간 / 갱신주기</div>
        <div style="font-size:1.15em;font-weight:700;color:#333">{_sp}일 / {_rn}</div>
      </div>
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:14px 18px;text-align:center">
        <div style="font-size:0.72em;color:#888;margin-bottom:2px">분할수 / 매도비율</div>
        <div style="font-size:1.15em;font-weight:700;color:#333">{_div} / {_sr:.0f}%</div>
      </div>
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:14px 18px;text-align:center">
        <div style="font-size:0.72em;color:#888;margin-bottom:2px">k_buy / k_sell</div>
        <div style="font-size:1.15em;font-weight:700;color:#333">{_kb:.2f} / {_ks:.2f}</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    _sd_adj = float(_sd_res.get("adj_applied", 0.0))
    _eval_pnl_sd = (_lc - _avg_c) * _holdings if _holdings > 0 and _avg_c > 0 else 0.0
    # 수익률은 자본 조정 제외한 순수 매매 성과 기준
    _trading_asset_sd = _final_a - _sd_adj
    _ret_pct_sd = (_trading_asset_sd / _os_cap - 1) * 100 if _os_cap > 0 else 0.0
    _sell_cnt_sd = int(_sd_res.get("sell_count",
                                    int((_hdf["매도량"] > 0).sum()) if _hdf is not None and not (hasattr(_hdf, "empty") and _hdf.empty) and "매도량" in _hdf.columns else 0))
    _ret_color_sd  = "#2E7D32" if _ret_pct_sd  >= 0 else "#C62828"
    _eval_color_sd = "#2E7D32" if _eval_pnl_sd >= 0 else "#C62828"
    _real_color_sd = "#2E7D32" if _realized_pnl >= 0 else "#C62828"
    _adj_caption_sd = (f'<div style="font-size:0.68em;color:#0B7A3E;font-weight:600">'
                        f'+ 조정 ${_sd_adj:+,.0f}</div>') if abs(_sd_adj) > 0.01 else ''
    st.markdown(f"""
    <div style="display:flex;gap:10px;margin-bottom:8px">
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px 16px;text-align:center">
        <div style="font-size:0.72em;color:#888;margin-bottom:2px">시작 자본</div>
        <div style="font-size:1.1em;font-weight:700;color:#333">${_os_cap:,.0f}</div>
        {_adj_caption_sd}
      </div>
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px 16px;text-align:center">
        <div style="font-size:0.72em;color:#888;margin-bottom:2px">총자산</div>
        <div style="font-size:1.1em;font-weight:700;color:#333">${_final_a:,.0f}</div>
        <div style="font-size:0.68em;color:{_ret_color_sd};font-weight:600">{_ret_pct_sd:+.1f}%</div>
      </div>
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px 16px;text-align:center">
        <div style="font-size:0.72em;color:#888;margin-bottom:2px">평가손익</div>
        <div style="font-size:1.1em;font-weight:700;color:{_eval_color_sd}">${_eval_pnl_sd:+,.0f}</div>
      </div>
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px 16px;text-align:center">
        <div style="font-size:0.72em;color:#888;margin-bottom:2px">누적실현손익</div>
        <div style="font-size:1.1em;font-weight:700;color:{_real_color_sd}">${_realized_pnl:+,.0f}</div>
        <div style="font-size:0.68em;color:#888">매도 {_sell_cnt_sd}회</div>
      </div>
      <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px 16px;text-align:center">
        <div style="font-size:0.72em;color:#888;margin-bottom:2px">예수금</div>
        <div style="font-size:1.1em;font-weight:700;color:#333">${_cash:,.0f}</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # -- 내일 LOC 주문 ------
    _buy_loc  = _sd_res["next_buy_loc"]
    _sell_loc = _sd_res["next_sell_loc"]
    _buy_qty  = _sd_res["est_buy_qty"]
    _sell_qty = _sd_res["est_sell_qty"]
    _nxt_t    = _sd_res["next_tier"]

    st.subheader(f"오늘의 LOC 주문  ({next_trading_date().strftime('%Y-%m-%d')})")
    st.caption(f"최근 종가: **${_lc:.2f}**  |  sigma(오늘 예상): **{_sd_res['sigma_next']*100:.4f}%**  |  "
               f"다음 티어: **T{_nxt_t}**")

    _orders = []
    # 매도: 보유량 + 매도수량 둘 다 > 0일 때만 표시 (0주 노이즈 제거)
    if _holdings > 0 and _sell_qty > 0:
        _orders.append({
            "구분": "매도", "티커": tk,
            "LOC 기준가":    f"${_sell_loc:.2f}",
            "1회매수금":     "-",
            "예상수량":      f"{_sell_qty:,}주",
            "예상금액":      f"${_sell_qty * _sell_loc:,.0f}",
            "전일종가 대비": f"{(_sell_loc/_lc-1)*100:+.2f}%" if _lc > 0 else "-",
            "비고": f"보유 {_holdings:,}주 x {_sr:.0f}% | 평단 ${_avg_c:.2f}",
        })
    # 매수: 수량 > 0일 때만 표시 (현금 부족으로 0주면 실행 불가 → 노이즈 제거)
    if _buy_qty > 0:
        _orders.append({
            "구분": "매수", "티커": tk,
            "LOC 기준가":    f"${_buy_loc:.2f}",
            "1회매수금":     f"${_sd_res['total_invest']/_div:,.0f}",
            "예상수량":      f"{_buy_qty:,}주",
            "예상금액":      f"${_buy_qty * _buy_loc:,.0f}",
            "전일종가 대비": f"{(_buy_loc/_lc-1)*100:+.2f}%" if _lc > 0 else "-",
            "비고": f"T{_nxt_t} / 1회 매수금 ${_sd_res['total_invest']/_div:,.0f}",
        })

    def _sd_style(row):
        s = [""] * len(row)
        if "구분" in row.index:
            i = list(row.index).index("구분")
            s[i] = ("color:#1565C0;font-weight:bold" if row["구분"] == "매도"
                    else "color:#C62828;font-weight:bold")
        return s

    _ord_tab1_sd, _ord_tab2_sd = st.tabs(["📋 매일 주문", "🔄 퉁치기 주문 (자전거래 회피)"])
    with _ord_tab1_sd:
        if _orders:
            st.dataframe(pd.DataFrame(_orders).style.apply(_sd_style, axis=1),
                         use_container_width=True, hide_index=True,
                         height=38 + 35 * len(_orders))
        else:
            st.info("💤 오늘은 매수/매도 주문 없음 (매수 대기 or 현금 부족)")

    with _ord_tab2_sd:
        # 진단 기준: '원 주문 vs 퉁치기 결과가 실제로 다른가' (orders_differ)
        try:
            try:
                from dss_engine import build_tungchigi_orders as _bto_sd, \
                    orders_differ as _od_sd
            except ImportError:
                import importlib, dss_engine as _de
                _de = importlib.reload(_de)
                _bto_sd, _od_sd = _de.build_tungchigi_orders, _de.orders_differ
            _raw_sd = []
            if _buy_qty > 0:
                _raw_sd.append(["매수", "LOC", round(_buy_loc, 2), int(_buy_qty)])
            if _holdings > 0 and _sell_qty > 0:
                _raw_sd.append(["매도", "LOC", round(_sell_loc, 2), int(_sell_qty)])
            if not _raw_sd:
                st.info("오늘 주문이 없습니다.")
            else:
                _tng_sd = _bto_sd(_raw_sd)
                if _tng_sd and _od_sd(_raw_sd, _tng_sd):
                    st.warning("매일 주문에 매수/매도 상계 구간 있음 (자전거래 위험) -- "
                               "자전거래 거부 증권사는 아래 퉁치기 주문을 사용하세요.")
                else:
                    st.success("매일 주문과 퉁치기 결과가 동일 -- 어느 증권사든 "
                               "매일 주문 그대로 사용 가능합니다.")
                if _tng_sd:
                    _tng_rows_sd = [{
                        "구분": ("MOC매도" if t["방법"] == "MOC" else
                                 ("LOC매도" if t["구분"] == "매도" else "LOC매수")),
                        "주문가": "시장가(종가)" if t["방법"] == "MOC" else f"${t['가격']:,.2f}",
                        "수량": f"{t['수량']:,}주",
                    } for t in _tng_sd]
                    st.dataframe(pd.DataFrame(_tng_rows_sd), use_container_width=True,
                                 hide_index=True, height=38 + 35 * len(_tng_rows_sd))
                    st.caption("※ 어떤 종가에도 순 체결 결과는 매일 주문과 동일합니다 (상계 제거).")
        except Exception as _e_tng_sd:
            st.error(f"퉁치기 계산 실패: {_e_tng_sd}")

    # -- 현재 보유 현황 ---
    st.subheader("현재 보유 현황")
    if _holdings > 0:
        _hcols = st.columns(6)
        _hcols[0].metric("보유주수",  f"{_holdings:,}주")
        _hcols[1].metric("평균단가",  f"${_avg_c:.2f}")
        _hcols[2].metric("현재가",    f"${_lc:.2f}")
        _hcols[3].metric("평가금액",  f"${_holdings*_lc:,.2f}")
        _hcols[4].metric("평가손익",  f"${(_lc-_avg_c)*_holdings:,.2f}",
                          delta=f"{(_lc/_avg_c-1)*100:+.2f}%" if _avg_c > 0 else "")
        _hcols[5].metric("보유현금",  f"${_cash:,.2f}")
    else:
        st.info("현재 보유 주식 없음 (전량 현금)")
        st.metric("보유현금", f"${_cash:,.2f}")

    # -- 일별 매매 상세표 -----
    st.divider()
    st.subheader("일별 매매 상세표")
    _sd_hist_df = _load_sd_daily_history(tk)
    # 히스토리 파일 없으면 현재 시뮬 결과 fallback
    if _sd_hist_df.empty and _hdf is not None and not (hasattr(_hdf, "empty") and _hdf.empty):
        _sd_hist_df = _hdf.copy()
        _sd_hist_df["날짜"] = _sd_hist_df["날짜"].astype(str)

    if not _sd_hist_df.empty:
        _sd_daily = _sd_hist_df.sort_values("날짜", ascending=False).reset_index(drop=True)
        # 예정 행(종가 빈칸) 여부 — 미체결/미확정 주문
        _is_pending_col = (_sd_daily["종가"].apply(
            lambda v: str(v).strip() in ("", "-", "None", "nan"))
            if "종가" in _sd_daily.columns else pd.Series([False]*len(_sd_daily)))
        # 매매 유형 파악 (매수량/매도량 컬럼 기준) — 문자열/빈칸 안전 변환
        _bq_num = (pd.to_numeric(_sd_daily["매수량"], errors="coerce").fillna(0)
                   if "매수량" in _sd_daily.columns else pd.Series([0]*len(_sd_daily)))
        _sq_num = (pd.to_numeric(_sd_daily["매도량"], errors="coerce").fillna(0)
                   if "매도량" in _sd_daily.columns else pd.Series([0]*len(_sd_daily)))
        # 체결 완료(예정 아님)만 카운트
        _bc = int(((_bq_num > 0) & (~_is_pending_col)).sum())
        _sc2 = int(((_sq_num > 0) & (~_is_pending_col)).sum())
        _pend_cnt = int(_is_pending_col.sum())
        _hist_s = _sd_daily["날짜"].iloc[-1]
        _hist_e = _sd_daily["날짜"].iloc[0]
        _pend_txt = f" · 예정 {_pend_cnt}건" if _pend_cnt else ""
        st.caption(f"기록 {_hist_s} ~ {_hist_e} | 총 {_bc+_sc2}건 (매수 {_bc}회 / 매도 {_sc2}회){_pend_txt}")
        st.info("이 기록은 실제 주문표 로드 시점에 누적 저장된 데이터입니다. 파라미터를 변경해도 과거 기록은 변경되지 않습니다. "
                "'예정' 행은 발송된 주문이 아직 체결 확정 전인 상태로, 다음 거래일에 실제 종가로 정산됩니다.", icon="ℹ️")

        _sd_show = _sd_daily.copy()
        # 숫자 컬럼 포맷 (빈칸/문자열 → NaN → '-')
        for _col in ["종가", "매수LOC", "매도LOC"]:
            if _col in _sd_show.columns:
                _num = pd.to_numeric(_sd_show[_col], errors="coerce")
                _sd_show[_col] = _num.apply(lambda v: f"${v:,.4f}" if pd.notna(v) else "-")
        for _col in ["예수금", "총자산"]:
            if _col in _sd_show.columns:
                _num = pd.to_numeric(_sd_show[_col], errors="coerce")
                _sd_show[_col] = _num.apply(lambda v: f"${v:,.2f}" if pd.notna(v) else "-")
        for _col in ["매수량", "매도량", "보유량"]:
            if _col in _sd_show.columns:
                _num = pd.to_numeric(_sd_show[_col], errors="coerce")
                _sd_show[_col] = _num.apply(lambda v: f"{int(v):,}" if pd.notna(v) else "-")

        # 실현손익 포맷 + 색상용 원본 보존
        _pnl_raw = pd.to_numeric(_sd_daily["실현손익"], errors="coerce") if "실현손익" in _sd_daily.columns else pd.Series([None]*len(_sd_daily))
        _sd_show["실현손익"] = _pnl_raw.apply(
            lambda v: f"+${v:,.2f}" if (not pd.isna(v) and v > 0)
               else (f"-${abs(v):,.2f}" if (not pd.isna(v) and v < 0)
               else "-")
        )
        # 매매 컬럼: BUY/SELL/BUY+SELL 표시
        # 표준편차매매는 buy_loc/sell_loc 둘 다 트리거 가능 (k_sell < k_buy 인 경우)
        # → 같은 날 매수+매도 동시 발생 가능 → BUY+SELL 라벨로 명시
        def _fmt_trade(row):
            b = int(pd.to_numeric(row.get("매수량", 0), errors="coerce") or 0)
            s = int(pd.to_numeric(row.get("매도량", 0), errors="coerce") or 0)
            # 예정 행(종가 빈칸) — 아직 체결 확정 전
            if str(row.get("종가", "")).strip() in ("", "-", "None", "nan"):
                _p = []
                if b > 0: _p.append(f"매수 {b}")
                if s > 0: _p.append(f"매도 {s}")
                return "🕓 예정 (" + " / ".join(_p) + ")" if _p else "🕓 예정"
            c = float(pd.to_numeric(row.get("종가", 0), errors="coerce") or 0)
            if b > 0 and s > 0: return f"BUY+SELL (${c:.2f})"
            if b > 0: return f"BUY (${c:.2f})"
            if s > 0: return f"SELL (${c:.2f})"
            return "-"
        _sd_show["매매"] = _sd_daily.apply(_fmt_trade, axis=1)

        def _style_sd_daily(row):
            v = str(row.get("매매",""))
            if v.startswith("🕓"): return ["background-color:#F5F5F5"]*len(row)  # 예정: 회색
            # BUY+SELL 동시 발생: 노란색 (혼합 표시)
            if v.startswith("BUY+SELL"): return ["background-color:#FFF8E1"]*len(row)
            if v.startswith("BUY"):  return ["background-color:#FFF0F0"]*len(row)
            if v.startswith("SELL"): return ["background-color:#F0FFF4"]*len(row)
            return [""]*len(row)
        def _style_sd_action(val):
            v = str(val)
            if v.startswith("🕓"): return "color:#F57C00;font-weight:bold"  # 예정: 주황
            if v.startswith("BUY+SELL"): return "color:#9A6700;font-weight:bold"  # 갈색/주황
            if v.startswith("BUY"):  return "color:#C62828;font-weight:bold"
            if v.startswith("SELL"): return "color:#1565C0;font-weight:bold"
            return "color:#999"
        def _style_sd_pnl(val):
            if isinstance(val, str) and val.startswith("+"): return "color:#1565C0;font-weight:bold"
            if isinstance(val, str) and val.startswith("-"): return "color:#C62828;font-weight:bold"
            return "color:#999"

        # 표시 컬럼 순서 정리
        _display_cols = ["날짜", "매매", "종가", "σ(%)", "매수LOC", "매도LOC",
                         "매수량", "매도량", "보유량", "평단가", "실현손익", "예수금", "총자산", "티어"]
        _display_cols = [c for c in _display_cols if c in _sd_show.columns]

        st.dataframe(
            _sd_show[_display_cols].style
                .apply(_style_sd_daily, axis=1)
                .map(_style_sd_action, subset=["매매"] if "매매" in _display_cols else [])
                .map(_style_sd_pnl,    subset=["실현손익"] if "실현손익" in _display_cols else []),
            hide_index=True, use_container_width=True,
            height=min(38 + 35 * len(_sd_show), 600)
        )

        import io as _io
        _today_dl = str(datetime.today().date()).replace("-", "")
        _sdl1, _sdl2, _ = st.columns([1, 1, 4])
        _csv_data = _sd_daily.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        _sdl1.download_button("CSV 다운로드", data=_csv_data,
                               file_name=f"{tk}_sd_history_{_today_dl}.csv",
                               mime="text/csv", key=f"sd_dl_csv_{key_sfx}", use_container_width=True)
        _buf = _io.BytesIO()
        with pd.ExcelWriter(_buf, engine="openpyxl") as _writer:
            _sd_daily.to_excel(_writer, index=False, sheet_name="표준편차매매기록")
        _sdl2.download_button("엑셀 다운로드", data=_buf.getvalue(),
                               file_name=f"{tk}_sd_history_{_today_dl}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               key=f"sd_dl_xlsx_{key_sfx}", use_container_width=True)
    else:
        st.info("아직 기록된 매매 데이터가 없습니다. 주문표 로드 후 데이터가 누적됩니다.")


def render_ordersheet_tab(ticker, params, initial_capital, data_source, excel_file):
    """표준편차매매 멀티계좌 주문표 탭 렌더링."""
    st.subheader("표준편차매매 -- 오늘의 주문표")
    st.caption("종목별 계좌를 등록하고 오늘 LOC 매수/매도 주문가를 확인합니다.")

    _sd_all        = _get_sd_ticker_settings()
    _sd_registered = list(_sd_all.keys())

    # -- 계좌 추가 ------
    with st.expander("계좌 추가"):
        _sd_preset_tickers = list(_SD_PRESETS_DB.keys()) + ["직접입력"]
        _sd_add_sel = st.selectbox("종목 선택", _sd_preset_tickers, key="sd_add_sel")
        _sd_add_tk  = (_sd_add_sel if _sd_add_sel != "직접입력"
                       else st.text_input("직접 입력 (예: TQQQ)", key="sd_add_custom").upper().strip())

        # 프리셋 버튼
        _sd_ap = _SD_PRESETS_DB.get(_sd_add_tk, [])
        if _sd_ap:
            st.caption("추천 프리셋")
            _sap_cols = st.columns(len(_sd_ap))
            for _sai, (_sapc, _sapp) in enumerate(zip(_sap_cols, _sd_ap)):
                if _sapc.button(_sapp["label"], key=f"sd_addp_{_sai}",
                                help=_sapp["help"], use_container_width=True):
                    st.session_state["sd_add_kb_inp"]  = _sapp["k_buy"]
                    st.session_state["sd_add_ks_inp"]  = _sapp["k_sell"]
                    st.session_state["sd_add_sr_inp"]  = _sapp["sell_ratio"]
                    st.session_state["sd_add_dv_inp"]  = _sapp["divisions"]
                    # sigma_period / renewal도 함께 강제 적용 (프리셋 정확성 보장)
                    if "sigma_period" in _sapp:
                        st.session_state["sd_add_sp_inp"] = int(_sapp["sigma_period"])
                    if "renewal" in _sapp:
                        st.session_state["sd_add_rn_inp"] = int(_sapp["renewal"])
                    st.rerun()
            st.divider()

        # 파라미터 입력
        _add_c1, _add_c2 = st.columns(2)
        _sd_add_kb  = _add_c1.number_input("k_buy",       value=st.session_state.get("sd_add_kb",  0.65),
                                            step=0.05, format="%.2f", key="sd_add_kb_inp")
        _sd_add_ks  = _add_c2.number_input("k_sell",      value=st.session_state.get("sd_add_ks",  0.65),
                                            step=0.05, format="%.2f", key="sd_add_ks_inp")
        _sd_add_sr  = _add_c1.number_input("매도비율 (%)", value=st.session_state.get("sd_add_sr", 75.0),
                                            step=5.0, min_value=5.0, max_value=100.0, key="sd_add_sr_inp")
        _sd_add_dv  = _add_c2.number_input("분할수",       value=st.session_state.get("sd_add_dv",  5),
                                            min_value=1, step=1, key="sd_add_dv_inp")
        _sd_add_sp  = _add_c1.number_input("sigma 계산 기간", value=2, min_value=1, max_value=20, step=1,
                                            key="sd_add_sp_inp")
        _sd_add_rn  = _add_c2.number_input("갱신 주기",   value=5, min_value=1, step=1,
                                            key="sd_add_rn_inp")

        if st.button("계좌 등록", type="primary", use_container_width=True, key="sd_add_reg"):
            if not _sd_add_tk:
                st.warning("종목 티커를 입력하세요.")
            else:
                _save_sd_ticker_setting(_sd_add_tk, {
                    "k_buy": float(_sd_add_kb), "k_sell": float(_sd_add_ks),
                    "sell_ratio": float(_sd_add_sr), "divisions": int(_sd_add_dv),
                    "sigma_period": int(_sd_add_sp), "renewal": int(_sd_add_rn),
                })
                for _k in ["sd_add_kb", "sd_add_ks", "sd_add_sr", "sd_add_dv"]:
                    st.session_state.pop(_k, None)
                st.success(f"{_sd_add_tk} 계좌가 등록되었습니다!")
                st.rerun()

    # -- 등록된 계좌 렌더링 -----
    if not _sd_registered:
        st.info("등록된 계좌가 없습니다. 위 '계좌 추가'에서 종목을 등록하세요.")
    elif len(_sd_registered) == 1:
        _render_sd_account_tab(_sd_registered[0], _sd_all[_sd_registered[0]], _sd_registered[0])
    else:
        _sd_tabs = st.tabs([f"{_t}" for _t in _sd_registered])
        for _si, _stk in enumerate(_sd_registered):
            with _sd_tabs[_si]:
                _render_sd_account_tab(_stk, _sd_all[_stk], _stk)


# ══════════════════════════════════════════════
# TAB 4 -- Strategy Intro & Performance
# ══════════════════════════════════════════════
def _sd_compute_recovery_table(assets, dates, threshold=10.0):
    records = []
    n = len(assets)
    if n == 0: return records
    peak_val = float(assets[0]); peak_idx = 0
    in_dd = False; trough_val = peak_val; trough_idx = 0
    for i in range(1, n):
        curr   = float(assets[i])
        dd_pct = (curr - peak_val) / peak_val * 100
        if not in_dd:
            if curr > peak_val:   peak_val = curr; peak_idx = i
            elif dd_pct <= -threshold:
                in_dd = True; trough_val = curr; trough_idx = i
        else:
            if curr < trough_val: trough_val = curr; trough_idx = i
            if curr >= peak_val:
                records.append({
                    "고점":         str(dates[peak_idx].date()),
                    "고점 평가액":   round(peak_val),
                    "최대하락 시점": str(dates[trough_idx].date()),
                    "저점 평가액":  round(trough_val),
                    "하락율(%)":    round((trough_val - peak_val) / peak_val * 100, 2),
                    "회복 시점":    str(dates[i].date()),
                    "기간(일)":     (dates[i] - dates[peak_idx]).days,
                })
                in_dd = False; peak_val = curr; peak_idx = i
                trough_val = curr; trough_idx = i
    if in_dd:
        records.append({
            "고점":         str(dates[peak_idx].date()),
            "고점 평가액":   round(peak_val),
            "최대하락 시점": str(dates[trough_idx].date()),
            "저점 평가액":  round(trough_val),
            "하락율(%)":    round((trough_val - peak_val) / peak_val * 100, 2),
            "회복 시점":    "미회복",
            "기간(일)":     (dates[-1] - dates[peak_idx]).days,
        })
    return records


def _render_sd_perf_analysis(tk, kb, ks, sp, rn, sr4, div4, init_cap4, s_date4, e_date4):
    with st.spinner(f"{tk} 데이터 로드 및 분석 중..."):
        _pdf4 = load_price_data(tk, s_date4, e_date4, "야후파이낸스 (yfinance)", None)
    if _pdf4 is None or _pdf4.empty:
        st.error(f"{tk}: 가격 데이터를 불러오지 못했습니다.")
        return

    _r4 = run_backtest_stdev(_pdf4, s_date4, e_date4,
                              sigma_period=sp, k_buy=kb, k_sell=ks,
                              sell_ratio=sr4, divisions=div4, renewal=rn,
                              initial_capital=init_cap4, return_history=True)
    if _r4 is None:
        st.warning(f"{tk}: 선택된 기간 내 데이터가 부족합니다.")
        return

    _hist4 = _r4["history"].copy()
    # compute_annual_stats / compute_monthly_pivot 은 "총자산($)" 컬럼을 기대함
    _col_map = {"총자산": "총자산($)", "예수금": "현금($)"}
    _hist4.rename(columns={k: v for k, v in _col_map.items() if k in _hist4.columns}, inplace=True)
    _hist4["날짜"] = _hist4["날짜"].astype(str)

    # -- 6대 지표 ------
    _sharpe4, _sortino4 = compute_sharpe_sortino(_r4["assets"])
    _s1,_s2,_s3,_s4,_s5,_s6 = st.columns(6)
    _s1.metric("전체 CAGR",     f"{_r4['cagr']*100:.2f}%")
    _s2.metric("전체 수익률",   f"{_r4['total_return']*100:+.2f}%")
    _s3.metric("최대 MDD",      f"{_r4['mdd']*100:.2f}%")
    _s4.metric("Calmar Ratio",  f"{_r4['calmar']:.3f}")
    _s5.metric("Sharpe Ratio",  f"{_sharpe4:.3f}")
    _s6.metric("Sortino Ratio", f"{_sortino4:.3f}")
    st.divider()

    # -- 연도별 성과 ---
    st.subheader("연도별 성과")
    _ann4 = compute_annual_stats(_hist4, init_cap4)
    def _color_ret4(val):
        if isinstance(val, (int, float)):
            if val > 0: return "color:#2e7d32;font-weight:bold"
            if val < 0: return "color:#c62828;font-weight:bold"
        return ""
    st.dataframe(
        _ann4.style.map(_color_ret4, subset=["연간수익률(%)"])
                   .format({"연간수익률(%)": "{:+.2f}%", "MDD(%)": "{:.2f}%"}),
        hide_index=True, use_container_width=True)
    st.divider()

    # -- 월별 히트맵 ---
    st.subheader("월별 수익률 히트맵")
    _mp4 = compute_monthly_pivot(_hist4, init_cap4)
    _fig_m4 = px.imshow(_mp4, color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
                        text_auto=".1f", labels={"x":"월","y":"연도","color":"수익률(%)"},
                        aspect="auto")
    _fig_m4.update_layout(height=max(320, len(_mp4)*38+120),
                           coloraxis_colorbar=dict(title="수익률(%)"))
    st.plotly_chart(_fig_m4, use_container_width=True)
    st.divider()

    # -- 종합 성과 요약 ---
    st.subheader("종합 성과 요약")
    _fa4 = _r4.get("final_asset", init_cap4)
    _sc4, _wc4 = _r4["sell_count"], _r4["win_count"]
    st.dataframe(pd.DataFrame({
        "항목": ["시작 자본","최종 자산","총 수익률","CAGR (연복리)",
                 "MDD","Calmar Ratio","총 매도 횟수","승률",
                 "평균 손익률","최대 단일 수익","최대 단일 손실"],
        "수치": [
            f"${init_cap4:,.0f}", f"${_fa4:,.0f}",
            f"{_r4['total_return']*100:+.2f}%", f"{_r4['cagr']*100:.1f}%",
            f"{_r4['mdd']*100:.1f}%", f"{_r4['calmar']:.3f}",
            f"{_sc4}회",
            f"{_wc4/_sc4*100:.1f}%  ({_wc4}승 {_sc4-_wc4}패)" if _sc4>0 else "-",
            f"{_r4['avg_pnl']:+.2f}%", f"{_r4['max_pnl']:+.2f}%",
            f"{_r4['min_pnl']:+.2f}%",
        ],
    }), hide_index=True, use_container_width=True)
    st.divider()

    # -- Buy & Hold 비교 ------
    st.subheader("Buy & Hold 비교")
    st.caption("같은 기간 종목을 단순 보유했을 때와 전략 성과를 비교합니다.")
    _bnh_a4, _bnh_d4 = compute_bnh(_pdf4, s_date4, e_date4, init_cap4)
    if len(_bnh_a4) > 0:
        _str_d4    = [str(d.date()) for d in _r4["dates"]]
        _bnh_ds4   = [str(d.date()) for d in _bnh_d4]
        _valid_idx = ~np.isnan(_r4["assets"])
        _valid_assets = np.where(_valid_idx, _r4["assets"],
                                 np.interp(np.arange(len(_r4["assets"])),
                                           np.where(_valid_idx)[0], _r4["assets"][_valid_idx]))
        _fig_bnh4 = go.Figure()
        _fig_bnh4.add_trace(go.Scatter(x=_str_d4, y=_valid_assets.tolist(),
                                        name="표준편차매매 전략", line=dict(color="#1565C0", width=2)))
        _fig_bnh4.add_trace(go.Scatter(x=_bnh_ds4, y=_bnh_a4.tolist(),
                                        name="Buy & Hold", line=dict(color="#EF5350", width=2, dash="dot")))
        _fig_bnh4.add_hline(y=init_cap4, line_dash="dash", line_color="#aaa", annotation_text="시작 자본")
        _bnh_ret4  = (_bnh_a4[-1]/_bnh_a4[0]-1)*100
        _bnh_yrs4  = (pd.to_datetime(e_date4)-pd.to_datetime(s_date4)).days/365.25
        _bnh_cagr4 = ((_bnh_a4[-1]/_bnh_a4[0])**(1/_bnh_yrs4)-1)*100 if _bnh_yrs4>0 else 0
        _fig_bnh4.update_layout(
            title=f"전략 vs Buy&Hold | 전략 {_r4['total_return']*100:+.1f}% vs B&H {_bnh_ret4:+.1f}%",
            yaxis_title="자산 ($)", height=380, legend=dict(orientation="h", y=1.08))
        st.plotly_chart(_fig_bnh4, use_container_width=True)
        _bc1,_bc2,_bc3,_bc4 = st.columns(4)
        _bc1.metric("전략 총수익",  f"{_r4['total_return']*100:+.1f}%")
        _bc2.metric("B&H 총수익",   f"{_bnh_ret4:+.1f}%")
        _bc3.metric("전략 CAGR",    f"{_r4['cagr']*100:.1f}%")
        _bc4.metric("B&H CAGR",     f"{_bnh_cagr4:.1f}%")
    st.divider()

    # -- 드로다운 분석 ------
    st.subheader("드로다운 (Underwater) 분석")
    st.caption("고점 대비 현재 손실 비율 추이. 얼마나 깊이, 얼마나 오래 손실 구간에 있었는지 보여줍니다.")
    _assets_clean4 = _r4["assets"][~np.isnan(_r4["assets"])]
    _dates_clean4  = _r4["dates"][~np.isnan(_r4["assets"])]
    _peak4  = np.maximum.accumulate(_assets_clean4)
    _dd4    = (_assets_clean4 - _peak4) / _peak4 * 100
    _strd4  = [str(d.date()) for d in _dates_clean4]
    _fig_dd4 = go.Figure()
    _fig_dd4.add_trace(go.Scatter(x=_strd4, y=_dd4.tolist(),
                                   fill="tozeroy", name="드로다운(%)",
                                   line=dict(color="#EF5350", width=1),
                                   fillcolor="rgba(239,83,80,0.25)"))
    _fig_dd4.add_hline(y=0, line_color="#888", line_width=1)
    _fig_dd4.update_layout(yaxis_title="드로다운 (%)", height=300,
                            yaxis=dict(tickformat=".1f"))
    st.plotly_chart(_fig_dd4, use_container_width=True)
    _dd_s4  = pd.Series(_dd4, index=_dates_clean4)
    _in_dd4 = False; _dd_s4_start = None; _dd_prds4 = []
    for _ddi, (_dddate, _ddval) in enumerate(_dd_s4.items()):
        if _ddval < 0 and not _in_dd4:
            _in_dd4 = True; _dd_s4_start = _dddate
        elif _ddval == 0 and _in_dd4:
            _in_dd4 = False
            _sub4 = _dd_s4[_dd_s4_start:_dddate]
            _dd_prds4.append({"시작일": str(_dd_s4_start.date()), "회복일": str(_dddate.date()),
                                "기간(일)": (_dddate-_dd_s4_start).days,
                                "최대낙폭(%)": round(float(_sub4.min()), 2)})
    if _dd_prds4:
        _dd_df4 = pd.DataFrame(_dd_prds4).nsmallest(5,"최대낙폭(%)").reset_index(drop=True)
        _dd_df4.index += 1
        st.markdown("**Top 5 최대 낙폭 구간**")
        st.dataframe(_dd_df4.style.format({"최대낙폭(%)":"{:.2f}%"}),
                     hide_index=False, use_container_width=True)
    st.divider()

    # -- 고점 회복력 분석 -----
    st.subheader("고점 회복력 분석")
    st.caption("고점 대비 10% 이상 하락이 발생한 모든 에피소드와 회복까지 걸린 기간을 정리합니다.")
    _rec4 = _sd_compute_recovery_table(_assets_clean4, _dates_clean4, threshold=10.0)
    if _rec4:
        _rec_df4 = pd.DataFrame(_rec4).reset_index(drop=True)
        _rec_df4.index += 1
        _rec_show4 = _rec_df4.copy()
        _rec_show4["고점 평가액"] = _rec_show4["고점 평가액"].apply(lambda v: f"${v:,}")
        _rec_show4["저점 평가액"] = _rec_show4["저점 평가액"].apply(lambda v: f"${v:,}")
        _rec_show4["하락율(%)"]   = _rec_show4["하락율(%)"].apply(lambda v: f"{abs(v):.2f}%")
        _rec_show4["기간(일)"]    = _rec_show4["기간(일)"].apply(lambda v: f"{v}일" if isinstance(v,(int,float)) else str(v))
        def _hl_unrec4(row):
            return ["background-color:#fff3e0"]*len(row) if row["회복 시점"]=="미회복" else [""]*len(row)
        st.dataframe(_rec_show4.style.apply(_hl_unrec4, axis=1), hide_index=False, use_container_width=True)
        _completed4 = [r for r in _rec4 if r["회복 시점"] != "미회복"]
        _rcc1,_rcc2,_rcc3,_rcc4 = st.columns(4)
        _rcc1.metric("총 에피소드",   f"{len(_rec4)}회")
        _rcc2.metric("평균 회복 기간", f"{int(np.mean([r['기간(일)'] for r in _completed4]))}일" if _completed4 else "-")
        _rcc3.metric("최장 회복 기간", f"{max([r['기간(일)'] for r in _completed4], default=0)}일" if _completed4 else "-")
        _rcc4.metric("최대 낙폭",     f"{abs(min([r['하락율(%)'] for r in _rec4])):.2f}%")
        st.divider()

        # 회복력 차트
        st.markdown("**고점 회복 구간 시각화**")
        st.caption("노란 음영: 10% 이상 하락 구간 / 초록 점: 고점 / 빨간 점: 저점")
        _fig_rec4 = go.Figure()
        _fig_rec4.add_trace(go.Scatter(x=_strd4, y=_assets_clean4.tolist(),
                                        name="표준편차매매 전략", line=dict(color="#1565C0", width=2)))
        if len(_bnh_a4) > 0:
            _fig_rec4.add_trace(go.Scatter(x=[str(d.date()) for d in _bnh_d4], y=_bnh_a4.tolist(),
                                            name="Buy & Hold", line=dict(color="#FB8C00", width=1.5, dash="dot")))
        _dm4 = {str(d.date()): i for i, d in enumerate(_dates_clean4)}
        for _ep4 in _rec4:
            _xe4 = _ep4["회복 시점"] if _ep4["회복 시점"] != "미회복" else _strd4[-1]
            _fig_rec4.add_vrect(x0=_ep4["고점"], x1=_xe4,
                                 fillcolor="rgba(255,236,153,0.35)", layer="below", line_width=0)
            _pi4 = _dm4.get(_ep4["고점"])
            if _pi4 is not None:
                _fig_rec4.add_trace(go.Scatter(x=[_ep4["고점"]], y=[float(_assets_clean4[_pi4])],
                                                mode="markers", marker=dict(color="#43A047", size=8, symbol="circle"),
                                                showlegend=False))
            _ti4 = _dm4.get(_ep4["최대하락 시점"])
            if _ti4 is not None:
                _fig_rec4.add_trace(go.Scatter(x=[_ep4["최대하락 시점"]], y=[float(_assets_clean4[_ti4])],
                                                mode="markers", marker=dict(color="#E53935", size=8, symbol="circle"),
                                                showlegend=False))
        _fig_rec4.add_trace(go.Scatter(x=[None], y=[None], mode="markers", marker=dict(color="#43A047", size=8), name="고점"))
        _fig_rec4.add_trace(go.Scatter(x=[None], y=[None], mode="markers", marker=dict(color="#E53935", size=8), name="저점"))
        _fig_rec4.update_layout(yaxis_title="자산 ($)", height=420,
                                 legend=dict(orientation="h", y=1.08), hovermode="x unified")
        st.plotly_chart(_fig_rec4, use_container_width=True)
    else:
        st.info("분석 기간 중 10% 이상 하락 에피소드가 없습니다.")
    st.divider()

    # -- 롤링 성과 분석 ---
    st.subheader("롤링 성과 분석")
    st.caption("구간별 성과 추이. 특정 시기에만 좋은 게 아닌지 검증합니다.")
    _roll_tabs4 = st.tabs(["1년 롤링", "2년 롤링", "3년 롤링"])
    for _rwin4, _rtab4 in zip([252, 504, 756], _roll_tabs4):
        with _rtab4:
            _rc4w, _rm4w = compute_rolling_perf(_assets_clean4, _rwin4)
            _valid4 = ~np.isnan(_rc4w)
            if _valid4.sum() > 0:
                _rdates4 = [str(d.date()) for d, v in zip(_dates_clean4, _valid4) if v]
                _fig_r4 = go.Figure()
                _fig_r4.add_trace(go.Scatter(x=_rdates4, y=_rc4w[_valid4].tolist(),
                                              name="롤링 CAGR(%)", line=dict(color="#1565C0", width=2), yaxis="y1"))
                _fig_r4.add_trace(go.Scatter(x=_rdates4, y=_rm4w[_valid4].tolist(),
                                              name="롤링 MDD(%)", line=dict(color="#EF5350", width=1.5, dash="dot"), yaxis="y2"))
                _fig_r4.add_hline(y=0, line_dash="dash", line_color="#aaa", yref="y1")
                _fig_r4.update_layout(
                    yaxis=dict(title="롤링 CAGR (%)", side="left"),
                    yaxis2=dict(title="롤링 MDD (%)", side="right", overlaying="y"),
                    legend=dict(orientation="h", y=1.08), height=340)
                st.plotly_chart(_fig_r4, use_container_width=True)
                _r41,_r42,_r43 = st.columns(3)
                _r41.metric("평균 CAGR", f"{np.nanmean(_rc4w):+.1f}%")
                _r42.metric("최고 CAGR", f"{np.nanmax(_rc4w):+.1f}%")
                _r43.metric("최저 CAGR", f"{np.nanmin(_rc4w):+.1f}%")
            else:
                st.info(f"분석 기간이 {_rwin4//252}년보다 짧아 롤링 분석이 불가합니다.")
    st.divider()

    # -- 매도 손익률 분포 -----
    st.subheader("매도 손익률 분포")
    st.caption("매도 시마다 발생한 손익률의 분포. 수익/손실의 패턴을 분석합니다.")
    _pnl4 = _r4.get("sell_pnls_list", [])
    if _pnl4:
        _pnl_arr4 = np.array(_pnl4)
        _skew4    = float(pd.Series(_pnl_arr4).skew())
        _kurt4    = float(pd.Series(_pnl_arr4).kurtosis())
        _fig_pnl4 = go.Figure()
        _fig_pnl4.add_trace(go.Histogram(x=_pnl_arr4.tolist(), nbinsx=30,
                                          marker_color=["#EF5350" if v<0 else "#43A047" for v in _pnl_arr4],
                                          name="손익률 빈도"))
        _fig_pnl4.add_vline(x=0, line_dash="dash", line_color="#333")
        _fig_pnl4.add_vline(x=float(np.mean(_pnl_arr4)), line_dash="dot", line_color="#1565C0",
                             annotation_text=f"평균 {np.mean(_pnl_arr4):+.2f}%", annotation_position="top right")
        _fig_pnl4.update_layout(xaxis_title="손익률 (%)", yaxis_title="빈도 (회)", height=320)
        st.plotly_chart(_fig_pnl4, use_container_width=True)
        _pd1,_pd2,_pd3,_pd4 = st.columns(4)
        _pd1.metric("평균 손익률", f"{np.mean(_pnl_arr4):+.2f}%")
        _pd2.metric("중앙값",      f"{np.median(_pnl_arr4):+.2f}%")
        _pd3.metric("왜도 (Skew)", f"{_skew4:.3f}", help="양수=우측 꼬리(큰 수익 가끔), 음수=좌측 꼬리(큰 손실 가끔)")
        _pd4.metric("첨도 (Kurt)", f"{_kurt4:.3f}", help="높을수록 극단값 빈도 높음")
    else:
        st.info("매도 이력이 없어 분포 분석이 불가합니다.")
    st.divider()

    # -- 현금 활용률 ------
    st.subheader("현금 활용률 & 매매 타이밍 패턴")
    _cash4 = _r4.get("cash_series", np.array([]))
    if len(_cash4) > 0 and len(_r4["assets"]) > 0:
        _assets_f4  = _r4["assets"].copy()
        _assets_f4[np.isnan(_assets_f4)] = np.nanmean(_assets_f4)
        _inv4  = (1 - _cash4 / np.where(_assets_f4 == 0, 1, _assets_f4)) * 100
        _cu1,_cu2,_cu3 = st.columns(3)
        _cu1.metric("평균 투자 비율", f"{np.nanmean(_inv4):.1f}%")
        _cu2.metric("최대 투자 비율", f"{np.nanmax(_inv4):.1f}%")
        _cu3.metric("현금 보유 비율", f"{100-np.nanmean(_inv4):.1f}%")
        _x_d4   = [str(d.date()) for d in _r4["dates"]]
        _inv_l4 = np.clip(_inv4, 0, 100).tolist()
        _fig_cu4 = go.Figure()
        _fig_cu4.add_trace(go.Scatter(x=_x_d4, y=[100.0]*len(_x_d4), mode="lines",
                                       line=dict(width=0), fill="tozeroy",
                                       fillcolor="rgba(200,200,200,0.6)", name="현금", hoverinfo="skip"))
        _fig_cu4.add_trace(go.Scatter(x=_x_d4, y=_inv_l4, mode="lines",
                                       line=dict(width=0), fill="tozeroy",
                                       fillcolor="rgba(255,179,0,0.75)", name="주식(ETF)",
                                       hovertemplate="%{x}<br>주식(ETF): %{y:.1f}%<extra></extra>"))
        _fig_cu4.update_layout(yaxis_title="비율 (%)", height=280,
                                yaxis=dict(range=[0,100]), hovermode="x unified",
                                legend=dict(orientation="h", y=1.08))
        st.plotly_chart(_fig_cu4, use_container_width=True)

    # 요일별/월별 매매 빈도
    if not _hist4.empty and "매수량" in _hist4.columns:
        _buy_h4  = _hist4[_hist4["매수량"] > 0].copy()
        _sell_h4 = _hist4[_hist4["매도량"] > 0].copy()
        if not _buy_h4.empty:
            _buy_h4["요일"]  = pd.to_datetime(_buy_h4["날짜"].astype(str)).dt.day_name()
            _sell_h4["요일"] = pd.to_datetime(_sell_h4["날짜"].astype(str)).dt.day_name()
            _buy_h4["월"]    = pd.to_datetime(_buy_h4["날짜"].astype(str)).dt.month
            _sell_h4["월"]   = pd.to_datetime(_sell_h4["날짜"].astype(str)).dt.month
            _dow_order4      = ["Monday","Tuesday","Wednesday","Thursday","Friday"]
            _buy_dow4  = _buy_h4["요일"].value_counts().reindex(_dow_order4, fill_value=0)
            _sell_dow4 = _sell_h4["요일"].value_counts().reindex(_dow_order4, fill_value=0)
            _fig_dow4  = go.Figure()
            _fig_dow4.add_trace(go.Bar(x=["월","화","수","목","금"], y=_buy_dow4.values.tolist(), name="매수", marker_color="#EF5350"))
            _fig_dow4.add_trace(go.Bar(x=["월","화","수","목","금"], y=_sell_dow4.values.tolist(), name="매도", marker_color="#43A047"))
            _fig_dow4.update_layout(barmode="group", title="요일별 매매 빈도", yaxis_title="횟수", height=300)
            _buy_mon4  = _buy_h4["월"].value_counts().sort_index()
            _sell_mon4 = _sell_h4["월"].value_counts().sort_index()
            _fig_mon4  = go.Figure()
            _fig_mon4.add_trace(go.Bar(x=[f"{m}월" for m in _buy_mon4.index], y=_buy_mon4.values.tolist(), name="매수", marker_color="#EF5350"))
            _fig_mon4.add_trace(go.Bar(x=[f"{m}월" for m in _sell_mon4.index], y=_sell_mon4.values.tolist(), name="매도", marker_color="#43A047"))
            _fig_mon4.update_layout(barmode="group", title="월별 매매 빈도", yaxis_title="횟수", height=300)
            _tcc1, _tcc2 = st.columns(2)
            with _tcc1: st.plotly_chart(_fig_dow4, use_container_width=True)
            with _tcc2: st.plotly_chart(_fig_mon4, use_container_width=True)
    st.divider()

    # -- 티어별 매수 사이클 분석 ------
    st.subheader("티어별 매수 사이클 분석")
    st.caption(
        f"각 매도 이벤트를 '직전 매도 이후 매수 횟수'로 분류합니다.  \n"
        f"1티어 = 매수 1회 후 즉시 매도(빠른 반등), "
        f"{div4}티어 = 매수 {div4}회 이상 누적 후 매도(깊은 분할매수). "
        f"표준편차매매은 부분 매도(sell_ratio%)이므로 매도 이벤트 단위로 집계합니다."
    )
    _tier_ev4 = run_stdev_tier_analysis(_r4["history"], div4)
    if _tier_ev4:
        _tier_df4 = pd.DataFrame(_tier_ev4)

        # -- 티어별 요약 집계 (1 ~ div4, 마지막 티어는 >= div4 포함) --
        _tier_sum4 = []
        for _t4 in range(1, div4 + 1):
            _sub4 = _tier_df4[_tier_df4["티어수"] == _t4]
            _lbl4 = f"{_t4}티어" if _t4 < div4 else f"{_t4}티어+"
            if len(_sub4) == 0:
                _tier_sum4.append({
                    "티어": _lbl4,
                    "발생횟수": 0, "승수": 0, "패수": 0, "승률(%)": "-",
                    "평균보유일": "-", "평균손익률(%)": "-",
                    "최대수익(%)": "-", "최대손실(%)": "-",
                })
            else:
                _tw4 = int((_sub4["손익률"] > 0).sum())
                _tl4 = len(_sub4) - _tw4
                _tier_sum4.append({
                    "티어": _lbl4,
                    "발생횟수": len(_sub4),
                    "승수": _tw4,
                    "패수": _tl4,
                    "승률(%)":       f"{_tw4/len(_sub4)*100:.1f}%",
                    "평균보유일":    f"{_sub4['보유일수'].mean():.1f}일",
                    "평균손익률(%)": f"{_sub4['손익률'].mean():+.2f}%",
                    "최대수익(%)":   f"{_sub4['손익률'].max():+.2f}%",
                    "최대손실(%)":   f"{_sub4['손익률'].min():+.2f}%",
                })

        st.dataframe(pd.DataFrame(_tier_sum4), hide_index=True, use_container_width=True)

        # -- 발생횟수 + 평균손익률 이중 축 차트 --
        _tc_tiers   = [f"{_t4}티어" if _t4 < div4 else f"{_t4}티어+"
                       for _t4 in range(1, div4 + 1)]
        _tc_counts  = [len(_tier_df4[_tier_df4["티어수"] == _t4]) for _t4 in range(1, div4 + 1)]
        _tc_pnl_avg = [
            round(_tier_df4[_tier_df4["티어수"] == _t4]["손익률"].mean(), 2)
            if len(_tier_df4[_tier_df4["티어수"] == _t4]) > 0 else 0.0
            for _t4 in range(1, div4 + 1)
        ]
        _fig_tier4 = go.Figure()
        _fig_tier4.add_trace(go.Bar(
            x=_tc_tiers, y=_tc_counts,
            name="발생횟수", marker_color="#5C6BC0", yaxis="y1",
        ))
        _fig_tier4.add_trace(go.Scatter(
            x=_tc_tiers, y=_tc_pnl_avg,
            name="평균손익률(%)",
            mode="lines+markers+text",
            text=[f"{v:+.2f}%" for v in _tc_pnl_avg],
            textposition="top center",
            marker=dict(size=10, color="#EF5350"),
            line=dict(color="#EF5350", width=2),
            yaxis="y2",
        ))
        _fig_tier4.update_layout(
            title="티어별 발생 횟수 & 평균 손익률 (매도 이벤트 기준)",
            yaxis=dict(title="발생횟수 (회)", side="left"),
            yaxis2=dict(title="평균 손익률 (%)", side="right", overlaying="y",
                        zeroline=True, zerolinecolor="#aaa"),
            legend=dict(orientation="h", y=1.1),
            height=360, bargap=0.3,
        )
        st.plotly_chart(_fig_tier4, use_container_width=True)

        # -- 전체 매도 이벤트 상세 내역 ------
        with st.expander("전체 매도 이벤트 상세 내역 보기"):
            def _style_tier4(row):
                try:
                    pnl_v = float(row["손익률"])
                    color = ("color:#2e7d32;font-weight:bold" if pnl_v > 0
                             else "color:#c62828;font-weight:bold" if pnl_v < 0
                             else "")
                except Exception:
                    color = ""
                return [color] * len(row)

            _tier_disp4 = _tier_df4.copy()
            _tier_disp4["티어수"] = _tier_disp4["티어수"].apply(
                lambda x: f"{x}티어" if x < div4 else f"{x}티어+"
            )
            st.dataframe(
                _tier_disp4.style.apply(_style_tier4, axis=1)
                           .format({"손익률": "{:+.2f}%"}),
                hide_index=True, use_container_width=True,
                height=min(38 + 35 * len(_tier_disp4), 500),
            )
    else:
        st.info("선택 기간 내 매도 이벤트가 없습니다.")
    st.divider()

    # -- 파라미터 민감도 히트맵 ------
    st.subheader("파라미터 민감도 분석")
    st.caption("현재 k_buy / k_sell 주변의 Calmar Ratio 분포. 과최적화 여부를 확인합니다.")
    with st.expander("민감도 히트맵 보기 (클릭하여 실행)", expanded=False):
        _n_steps4    = 5
        _buy_rng4    = np.linspace(kb - 0.2, kb + 0.2, _n_steps4)
        _sell_rng4   = np.linspace(ks - 0.2, ks + 0.2, _n_steps4)
        _heat4       = np.zeros((_n_steps4, _n_steps4))
        with st.spinner("민감도 분석 중... (25회 시뮬레이션)"):
            for _bi4, _bv4 in enumerate(_buy_rng4):
                for _si4, _sv4 in enumerate(_sell_rng4):
                    _hr4 = run_backtest_stdev(_pdf4, s_date4, e_date4,
                                               sigma_period=sp, k_buy=_bv4, k_sell=_sv4,
                                               sell_ratio=sr4, divisions=div4, renewal=rn,
                                               initial_capital=init_cap4)
                    _heat4[_bi4][_si4] = _hr4["calmar"] if _hr4 else 0.0
        _buy_lbls4  = [f"{v:.2f}" for v in _buy_rng4]
        _sell_lbls4 = [f"{v:.2f}" for v in _sell_rng4]
        _fig_heat4  = px.imshow(_heat4, x=_sell_lbls4, y=_buy_lbls4,
                                 color_continuous_scale="RdYlGn",
                                 labels={"x":"k_sell","y":"k_buy","color":"Calmar"},
                                 text_auto=".2f", aspect="auto",
                                 title="Calmar Ratio 히트맵 (k_buy x k_sell)")
        _fig_heat4.add_annotation(x=f"{ks:.2f}", y=f"{kb:.2f}",
                                   text="현재", showarrow=True, arrowhead=2,
                                   font=dict(color="white", size=13, family="Arial Black"))
        _fig_heat4.update_layout(height=380)
        st.plotly_chart(_fig_heat4, use_container_width=True)
        st.caption("녹색일수록 Calmar Ratio가 높습니다. 현재 파라미터 주변이 고르게 녹색이면 과최적화 위험이 낮습니다.")
    st.divider()

    # -- 무작위 기간 강건성 분석 -----
    st.subheader("무작위 기간 강건성 분석")
    st.caption("2014~현재까지 1년(252 거래일) 구간 100개를 무작위 추출하여 백테스트를 반복합니다.")
    with st.expander("강건성 분석 실행 (클릭)", expanded=False):
        if st.button("▶ 무작위 100구간 분석 시작", key=f"sd_mc_run_{tk}"):
            st.session_state[f"sd_mc_res_{tk}"] = None
            with st.spinner("전체 가격 데이터 로드 중..."):
                _mc_pdf4 = load_price_data(tk, "2014-01-01", str(pd.Timestamp.today().date()),
                                           "야후파이낸스 (yfinance)", None)
            if _mc_pdf4 is None or _mc_pdf4.empty:
                st.error("가격 데이터를 불러오지 못했습니다.")
            else:
                _mc_idx4   = _mc_pdf4["Close"].dropna().index
                _WINDOW4   = 252
                _mc_starts4 = [i for i in range(len(_mc_idx4) - _WINDOW4)]
                if len(_mc_starts4) < 100:
                    st.warning("데이터가 100구간 분석에 충분하지 않습니다.")
                else:
                    import random as _rand4
                    _rand4.seed(None)
                    _mc_chosen4  = _rand4.sample(_mc_starts4, 100)
                    _mc_sr4 = []; _mc_sm4 = []; _mc_br4 = []; _mc_bm4 = []; _mc_pd4 = []
                    _mc_pg4 = st.progress(0, text="시뮬레이션 중...")
                    for _ci4, _si4 in enumerate(_mc_chosen4):
                        _sd4 = str(_mc_idx4[_si4].date())
                        _ed4 = str(_mc_idx4[_si4+_WINDOW4-1].date())
                        _rr4 = run_backtest_stdev(_mc_pdf4, _sd4, _ed4,
                                                   sigma_period=sp, k_buy=kb, k_sell=ks,
                                                   sell_ratio=sr4, divisions=div4, renewal=rn,
                                                   initial_capital=init_cap4)
                        if _rr4:
                            _mc_sr4.append(round(_rr4["total_return"]*100, 2))
                            _mc_sm4.append(round(abs(_rr4["mdd"])*100, 2))
                            _ba4, _ = compute_bnh(_mc_pdf4, _sd4, _ed4, init_cap4)
                            if len(_ba4) > 0:
                                _mc_br4.append(round((_ba4[-1]/_ba4[0]-1)*100, 2))
                                _bpk4 = np.maximum.accumulate(_ba4)
                                _mc_bm4.append(round(abs(float(((np.array(_ba4)-_bpk4)/_bpk4).min()))*100, 2))
                            _mc_pd4.append((_sd4, _ed4))
                        _mc_pg4.progress((_ci4+1)/100, text=f"시뮬레이션 중... {_ci4+1}/100")
                    _mc_pg4.empty()
                    st.session_state[f"sd_mc_res_{tk}"] = {
                        "strat_ret": _mc_sr4, "strat_mdd": _mc_sm4,
                        "bnh_ret": _mc_br4, "bnh_mdd": _mc_bm4, "periods": _mc_pd4,
                    }

        _mc_r4 = st.session_state.get(f"sd_mc_res_{tk}")
        if _mc_r4:
            _sr_a4 = np.array(_mc_r4["strat_ret"])
            _sm_a4 = np.array(_mc_r4["strat_mdd"])
            _br_a4 = np.array(_mc_r4["bnh_ret"]) if _mc_r4["bnh_ret"] else None
            _bm_a4 = np.array(_mc_r4["bnh_mdd"]) if _mc_r4["bnh_mdd"] else None
            _n_mc4 = len(_sr_a4)
            _sl4   = "표준편차매매 전략"
            _bl4   = f"{tk} B&H"

            def _mcs4(arr, lbl):
                return {"구분": lbl, "평균": f"{np.mean(arr):+.1f}%", "중앙값": f"{np.median(arr):+.1f}%",
                        "표준편차": f"{np.std(arr):.1f}%", "최솟값": f"{np.min(arr):+.1f}%",
                        "최댓값": f"{np.max(arr):+.1f}%", "양(+) 비율": f"{(arr>0).sum()/len(arr)*100:.0f}%"}
            _mcs_rows = [_mcs4(_sr_a4, f"{_sl4} (1년 수익률)")]
            if _br_a4 is not None: _mcs_rows.append(_mcs4(_br_a4, f"{_bl4} (1년 수익률)"))
            _mcs_rows.append({"구분": f"{_sl4} (MDD)", "평균": f"{np.mean(_sm_a4):.1f}%",
                               "중앙값": f"{np.median(_sm_a4):.1f}%", "표준편차": f"{np.std(_sm_a4):.1f}%",
                               "최솟값": f"{np.min(_sm_a4):.1f}%", "최댓값": f"{np.max(_sm_a4):.1f}%", "양(+) 비율": "-"})
            st.markdown(f"**요약 통계 (n={_n_mc4})**")
            st.dataframe(pd.DataFrame(_mcs_rows), hide_index=True, use_container_width=True)

            # -- 100구간 상세 결과 표 --
            with st.expander("100구간 상세 결과 보기"):
                _detail_rows4 = []
                for _di4, (_psd4, _ped4) in enumerate(_mc_r4["periods"]):
                    _row4 = {
                        "#": _di4 + 1,
                        "시작일": _psd4, "종료일": _ped4,
                        "전략 수익률(%)": f"{_mc_r4['strat_ret'][_di4]:+.1f}%",
                        "전략 MDD(%)":    f"{_mc_r4['strat_mdd'][_di4]:.1f}%",
                    }
                    if _br_a4 is not None and _di4 < len(_mc_r4["bnh_ret"]):
                        _row4[f"{tk} B&H 수익률(%)"] = f"{_mc_r4['bnh_ret'][_di4]:+.1f}%"
                        _row4[f"{tk} B&H MDD(%)"]    = f"{_mc_r4['bnh_mdd'][_di4]:.1f}%"
                    _detail_rows4.append(_row4)
                _detail_df4 = pd.DataFrame(_detail_rows4)

                def _hl_detail4(row):
                    try:
                        ret = float(row["전략 수익률(%)"].replace("%","").replace("+",""))
                        color = "background-color:#e8f5e9" if ret > 0 else "background-color:#ffebee"
                        return [color] * len(row)
                    except Exception:
                        return [""] * len(row)

                st.dataframe(
                    _detail_df4.style.apply(_hl_detail4, axis=1),
                    hide_index=True, use_container_width=True,
                    height=min(38 + 35 * len(_detail_df4), 500),
                )

            from scipy.stats import gaussian_kde as _kde4
            _fig_mc4 = make_subplots(rows=1, cols=2,
                                      subplot_titles=["수익률 분포 (1년)", "최대 낙폭(MDD) 분포"],
                                      horizontal_spacing=0.12)
            def _add_hk4(fig, arr, color, name, row, col, lg, sl):
                fig.add_trace(go.Histogram(x=arr.tolist(), nbinsx=20, name=name,
                                           opacity=0.55, marker_color=color,
                                           legendgroup=lg, showlegend=sl), row=row, col=col)
                if len(arr) > 5:
                    _kd4 = _kde4(arr)
                    _xr4 = np.linspace(arr.min()-arr.std(), arr.max()+arr.std(), 200)
                    _yr4 = _kd4(_xr4)*len(arr)*(arr.max()-arr.min())/20
                    fig.add_trace(go.Scatter(x=_xr4.tolist(), y=_yr4.tolist(), name=name,
                                              line=dict(color=color, width=2.5),
                                              legendgroup=lg, showlegend=False), row=row, col=col)
            if _br_a4 is not None: _add_hk4(_fig_mc4, _br_a4, "#FB8C00", _bl4, 1, 1, "bnh4", True)
            if _bm_a4 is not None: _add_hk4(_fig_mc4, _bm_a4, "#FB8C00", _bl4, 1, 2, "bnh4", False)
            _add_hk4(_fig_mc4, _sr_a4, "#1565C0", _sl4, 1, 1, "str4", True)
            _add_hk4(_fig_mc4, _sm_a4, "#1565C0", _sl4, 1, 2, "str4", False)
            _fig_mc4.add_vline(x=0, line_dash="dash", line_color="#555", row=1, col=1)
            _fig_mc4.update_xaxes(title_text="1년 수익률 (%)", row=1, col=1)
            _fig_mc4.update_xaxes(title_text="MDD (%)", row=1, col=2)
            _fig_mc4.update_layout(height=460, barmode="overlay",
                                    legend=dict(orientation="v", x=1.02, y=1.0,
                                                xanchor="left", yanchor="top",
                                                bgcolor="rgba(255,255,255,0.85)",
                                                bordercolor="#ccc", borderwidth=1),
                                    margin=dict(r=140))
            st.plotly_chart(_fig_mc4, use_container_width=True)


def render_intro_tab(ticker, params, data_source, excel_file, start_date, end_date, initial_capital):
    """표준편차매매 전략 소개 & 성과 분석 탭 렌더링."""
    sd_k_buy   = params["sd_k_buy"]
    sd_k_sell  = params["sd_k_sell"]
    sd_sigma_period = params["sd_sigma_period"]
    sd_renewal = params["sd_renewal"]
    sell_ratio = params["sell_ratio"]
    divisions  = params["divisions"]

    # -- 전략 소개 ------
    st.subheader("표준편차매매 (sigma-LOC 전략) 이란?")

    _sdl, _sdr = st.columns([3, 2])
    with _sdl:
        st.markdown("""
#### 전략 개요
**표준편차매매**는 직전 N거래일의 **일간 수익률 표준편차(sigma)**를 기준으로
당일 매수/매도 **LOC(Limit-On-Close)** 주문 기준가를 계산하는 퀀트 전략입니다.

주가의 **변동성(sigma)** 자체를 매매 기준으로 삼아,
변동이 클 때는 기준가가 벌어지고, 변동이 작을 때는 좁혀지는 **자동 적응형** 전략입니다.

---

#### 매수 룰
- 당일 종가 **<= 매수LOC** 이면 LOC 매수 체결
- 매수LOC = 전일 종가 x (1 + sigma x k_buy)  -- **k_buy < 0이면 하락 시 매수**
- 1회 매수금 = 총투자금 / 분할수(N)

#### 매도 룰
- 보유 중이고 당일 종가 **>= 매도LOC** 이면 LOC 매도 체결
- 매도LOC = 전일 종가 x (1 + sigma x k_sell)  -- **k_sell > 0이면 상승 시 매도**
- 매도 수량 = 보유량 x 매도비율(%)

#### 티어 시스템
| 파라미터 | 설명 |
|---|---|
| k_buy | 매수 LOC 배율 (음수 -> 하락폭의 배수만큼 내려가야 매수) |
| k_sell | 매도 LOC 배율 (양수 -> 상승폭의 배수만큼 올라야 매도) |
| sigma 기간 | 표준편차 계산에 사용할 직전 N일 수익률 |
| 분할수 N | 자산을 N등분하여 1회 매수 금액 결정 |
| 갱신 주기 | 총투자금 갱신 기준 사이클 수 |
| 매도비율 | 보유량 중 매도 비율 (100% = 전량 매도) |
        """)

    with _sdr:
        st.info("""
**LOC 기준가 공식**

```
sigma   = stdevp(직전 N일 일간수익률)

매수LOC = 전일종가 x (1 + sigma x k_buy)
매도LOC = 전일종가 x (1 + sigma x k_sell)
```

- k_buy = -0.65 -> sigma의 0.65배 하락 시 매수
- k_sell = 0.50 -> sigma의 0.50배 상승 시 매도
- sigma가 클수록(변동성 up) LOC 범위가 확대됨
        """)
        st.info("""
**종가평균매매와의 차이**

| 구분 | 종가평균 | 표준편차 |
|---|---|---|
| 기준 | 2일 이동평균 | 변동성(sigma) |
| 매도 | 전량 청산 | 비율 청산 |
| 적응성 | 고정 | 자동 조정 |
| 변동성 장세 | 불리 | 유리 |
        """)
        st.success("""
**핵심 장점**

변동성이 높을 때(공포장/급등장)
자동으로 기준가 범위가 넓어져
자연스럽게 매수 기회를 포착합니다.
        """)

    st.divider()

    # -- 전략 인사이트 & 맥락 참고 ------
    st.subheader("전략 인사이트 & 맥락 참고")
    st.warning("**다음 내용은 표준편차매매 전략 해석입니다. 과거 성과가 미래 수익을 보장하지 않습니다.**")
    with st.container(border=True):
        st.markdown("""
**왜 sigma(표준편차)를 기준으로 매매하나?**
- 변동성은 **시장마다 다르고, 시기마다 다릅니다.** 고정된 % 기준가는 급락장에선 너무 작고, 저변동기엔 너무 큽니다.
- sigma를 기준으로 삼으면 **공포장에서는 기준가가 넓어지고, 안정장에서는 좁아지는** 자동 조정이 일어납니다.
- 즉, 시장이 소리를 지를수록 매수 기회가 자동으로 더 깊어지고, 조용할 때는 가볍게 매매합니다.

**k_buy / k_sell 부호의 직관적 해석**
- `k_buy < 0` : 전일 종가보다 **하락**해야 매수 (하락 시 추가매수 -> 리스크 완화)
- `k_buy > 0` : 전일 종가보다 약간 **상승해도** 매수 (모멘텀 추종)
- `k_sell > 0` : 전일 종가보다 **상승**해야 매도 (상승분 익절)
- k_buy 와 k_sell 이 같은 절댓값이면, 매수 후 같은 방향으로 반등만 해도 즉시 매도 가능

**변동성이 '돈'이 되는 메커니즘**
```
변동성 up  ->  LOC 범위 확대  ->  매수/매도 기회 증가  ->  실현 손익 누적
변동성 down  ->  LOC 범위 축소  ->  거래 소강  ->  현금 유지 (방어)
```
SOXL 같은 3x 레버리지 ETF는 일간 변동이 크기 때문에, 이 전략의 **자동 적응 능력이 가장 극대화**됩니다.

**이 전략이 유독 강한 구간**
- **박스권/등락 반복 구간** : 오르면 팔고 내리면 사는 싸이클이 반복 -> 수익 누적
- **급락 후 빠른 반등 구간** : LOC가 자동 확대되어 저점 매수 -> 반등 시 큰 수익
- **높은 변동성 + 우상향** : 레버리지 ETF의 전형적 패턴, 최적 조합

**이 전략이 고통받는 구간**
- **단방향 지속 하락** : 계속 매수하지만 반등 없이 하락 -> MDD 확대
- **초저변동 횡보** : sigma가 너무 작아 LOC 범위가 너무 좁음 -> 매매 거의 없음
- **갭하락 충격** : 전일 sigma와 오늘 실제 변동이 심하게 어긋나면 예상 기준가 무의미

**주요 지표 해석**
| 지표 | 기준 | 해석 |
|---|---|---|
| Calmar | > 1.0 우수, > 2.0 최상 | 연복리 / MDD, 위험 대비 수익 |
| Sharpe | > 1.0 우수 | 변동성 대비 초과 수익 |
| Sortino | > 1.5 우수 | 하락 변동성만 기준 -> 더 엄격 |
| MDD | 레버리지 ETF는 -50% 이상도 발생 | 심리적으로 견딜 수 있는지 확인 |
| 티어별 평균보유일 | 짧을수록 회전율 up | 1티어 매도가 길면 반등이 느린 것 |

**실전 운용 주의사항**
- 분할수(N) 모두 체결되면 **현금이 소진** -> 추가 하락 시 매수 불가 (가장 큰 리스크)
- LOC 주문 미체결 시 다음 날 다시 계산 (당일 종가 기준 확인 필수)
- sigma 계산은 **직전 N일 종가 수익률** 기준 -> 오전 중 예측값과 마감 실제값이 다를 수 있음
- **파라미터를 자주 바꾸면 과최적화(overfitting)** -> 최소 3개월 이상 유지 권장
        """)

    st.divider()

    # -- 성과 분석 섹션 ------
    st.subheader("📊 전략 성과 분석")
    st.markdown("사이드바의 파라미터와 기간 설정을 기반으로 성과를 분석합니다.")

    # -- 분석 종목 선택: 등록된 계좌 우선 ------
    _sd_all_for_perf  = _get_sd_ticker_settings()
    _sd_reg_tickers   = list(_sd_all_for_perf.keys())
    _perf_candidates  = _sd_reg_tickers if _sd_reg_tickers else [ticker]
    for _xt in ["SOXL","TQQQ","FNGU"]:
        if _xt not in _perf_candidates:
            _perf_candidates.append(_xt)

    # 분석 종목 선택 (멀티 ticker 지원 위해 유지)
    _act_tk = st.selectbox(
        "분석 종목", _perf_candidates, key="sd_pf_tk",
        help="주문표 탭에 등록된 계좌가 목록 상단에 표시됩니다.",
    )

    # ── 파라미터 소스 선택 (사이드바 or 프리셋 or 등록 계좌) ──
    _sd_presets_for_tk = _SD_PRESETS_DB.get(_act_tk, [])
    _act_cfg = _sd_all_for_perf.get(_act_tk, {})

    _src_options = ["📐 사이드바 설정값"]
    if _act_cfg:
        _src_options.append(f"📂 등록 계좌 ({_act_tk})")
    _src_options.extend([f"{pr['label']}" if pr['label'].startswith(('🚀','⚖️','🛡️'))
                          else f"🎯 {pr['label']}" for pr in _sd_presets_for_tk])

    _src_sel = st.radio(
        "파라미터 소스", _src_options, index=0, horizontal=True,
        key="sd_intro_param_src",
    )
    _src_idx = _src_options.index(_src_sel)

    # 소스별 파라미터 결정
    _has_acct = bool(_act_cfg)
    if _src_idx == 0:
        # 사이드바 설정값
        _use_kb  = float(sd_k_buy)
        _use_ks  = float(sd_k_sell)
        _use_sp  = int(sd_sigma_period)
        _use_rn  = int(sd_renewal)
        _use_sr  = float(sell_ratio)
        _use_div = int(divisions)
    elif _has_acct and _src_idx == 1:
        # 등록 계좌 값
        _use_kb  = float(_act_cfg.get("k_buy",        sd_k_buy))
        _use_ks  = float(_act_cfg.get("k_sell",       sd_k_sell))
        _use_sp  = int  (_act_cfg.get("sigma_period", sd_sigma_period))
        _use_rn  = int  (_act_cfg.get("renewal",      sd_renewal))
        _use_sr  = float(_act_cfg.get("sell_ratio",   sell_ratio))
        _use_div = int  (_act_cfg.get("divisions",    divisions))
    else:
        # 프리셋 값 (sigma_period/renewal도 프리셋에 정의된 값 강제 적용)
        _preset_idx = _src_idx - (2 if _has_acct else 1)
        _pr = _sd_presets_for_tk[_preset_idx]
        _use_kb  = float(_pr["k_buy"])
        _use_ks  = float(_pr["k_sell"])
        # 프리셋에 sigma_period/renewal 키가 있으면 우선 사용, 없으면 사이드바 값 (구버전 호환)
        _use_sp  = int(_pr.get("sigma_period", sd_sigma_period))
        _use_rn  = int(_pr.get("renewal",      sd_renewal))
        _use_sr  = float(_pr["sell_ratio"])
        _use_div = int(_pr["divisions"])

    # 적용 파라미터 미리보기
    with st.expander("🔍 적용 파라미터 확인", expanded=False):
        _pv1, _pv2, _pv3 = st.columns(3)
        _pv1.markdown(f"**k_buy**: `{_use_kb:.2f}`")
        _pv2.markdown(f"**k_sell**: `{_use_ks:.2f}`")
        _pv3.markdown(f"**σ 기간**: `{_use_sp}일`")
        _pv4, _pv5, _pv6 = st.columns(3)
        _pv4.markdown(f"**매도비율**: `{_use_sr:.0f}%`")
        _pv5.markdown(f"**분할수**: `{_use_div}회`")
        _pv6.markdown(f"**갱신주기**: `{_use_rn}일`")
        st.caption(
            f"대상 종목 **{_act_tk}** · 기간 {start_date} ~ {end_date} · "
            f"자본 ${initial_capital:,.0f}"
        )

    if st.button("▶ 성과 분석 실행", type="primary",
                  key="sd_run_intro_perf", use_container_width=False):
        st.session_state["sd_perf_done"] = True
        st.session_state["sd_perf_run_params"] = {
            "act_tk": _act_tk,
            "kb": _use_kb, "ks": _use_ks,
            "sp": _use_sp, "rn": _use_rn,
            "sr": _use_sr, "div": _use_div,
            "cap": float(initial_capital),
            "s_date": str(start_date), "e_date": str(end_date),
            "source_label": _src_sel,
        }

    if st.session_state.get("sd_perf_done"):
        _prm = st.session_state.get("sd_perf_run_params", {})
        if _prm:
            _act_tk  = _prm["act_tk"]
            _act_kb  = _prm["kb"]
            _act_ks  = _prm["ks"]
            _act_sp  = _prm["sp"]
            _act_rn  = _prm["rn"]
            _act_sr  = _prm["sr"]
            _act_div = _prm["div"]
            _act_cap = _prm["cap"]
            _pf_s    = _prm["s_date"]
            _pf_e    = _prm["e_date"]
            st.success(f"✅ **{_prm['source_label']}** 기준 분석 결과 — {_act_tk}")
        else:
            _act_kb, _act_ks, _act_sp, _act_rn = _use_kb, _use_ks, _use_sp, _use_rn
            _act_sr, _act_div, _act_cap = _use_sr, _use_div, float(initial_capital)
            _pf_s, _pf_e = str(start_date), str(end_date)

        _render_sd_perf_analysis(
            _act_tk, _act_kb, _act_ks, _act_sp, _act_rn,
            _act_sr, _act_div, _act_cap, str(_pf_s), str(_pf_e),
        )

        # -- 매매법 간 성과 비교 (자동 실행) ------
        st.divider()
        st.subheader("매매법 간 성과 비교")

        _cmp_ab  = -0.0063
        _cmp_as  =  0.0075
        _cmp_sr  = 100.0
        _cmp_div = 5
        _cmp_n   = 2
        st.caption(
            f"**표준편차매매**: 현재 설정 파라미터 (k_buy={_act_kb:.4f} / k_sell={_act_ks:.4f} / sigma{_act_sp}일)  \n"
            f"**종가평균매매**: 안정형 표준 파라미터 (a_buy=-0.63% / a_sell=+0.75% / 3일 이동평균 / 5분할)  \n"
            f"각 전략의 고유 파라미터 기준으로 **2014-01-01 ~ 오늘** 동일 종목/기간/자본($10,000)을 비교합니다."
        )

        _cmp_s = "2014-01-01"
        _cmp_e = str(datetime.today().date())
        _cmp_cap = 10000.0

        _pdf_cmp = load_price_data(_act_tk, _cmp_s, _cmp_e, "야후파이낸스 (yfinance)", None)
        if _pdf_cmp is None or _pdf_cmp.empty:
            st.warning(f"{_act_tk} 가격 데이터를 불러오지 못해 비교 분석을 생략합니다.")
        else:
            with st.spinner("매매법 비교 분석 중..."):
                _cmp_r_sd = run_backtest_stdev(
                    _pdf_cmp, _cmp_s, _cmp_e,
                    sigma_period=_act_sp, k_buy=_act_kb, k_sell=_act_ks,
                    sell_ratio=_act_sr, divisions=_act_div, renewal=_act_rn,
                    initial_capital=_cmp_cap,
                )
                _cmp_r_avg = _get_avg_close_run_backtest()(
                    _pdf_cmp, _cmp_s, _cmp_e,
                    _cmp_ab, _cmp_as, _cmp_sr, _cmp_div, _cmp_cap, n_days=_cmp_n,
                )
                _cmp_bnh_a, _cmp_bnh_d = compute_bnh(_pdf_cmp, _cmp_s, _cmp_e, _cmp_cap)

            _fig_cmp = go.Figure()
            _cmp_metrics = []

            if _cmp_r_sd:
                _sd_norm = _cmp_r_sd["assets"] / _cmp_cap * 100
                _fig_cmp.add_trace(go.Scatter(
                    x=[str(d.date()) for d in _cmp_r_sd["dates"]], y=_sd_norm.tolist(),
                    name="표준편차매매", line=dict(color="#1565C0", width=2.5),
                ))
                _sh_sd, _so_sd = compute_sharpe_sortino(_cmp_r_sd["assets"])
                _cmp_metrics.append({
                    "전략": "표준편차매매",
                    "파라미터": f"k_buy={_act_kb:.2f} / k_sell={_act_ks:.2f} / sigma{_act_sp}일",
                    "총수익률": f"{_cmp_r_sd['total_return']*100:+.2f}%",
                    "CAGR":    f"{_cmp_r_sd['cagr']*100:.2f}%",
                    "MDD":     f"{_cmp_r_sd['mdd']*100:.2f}%",
                    "Calmar":  f"{_cmp_r_sd['calmar']:.3f}",
                    "Sharpe":  f"{_sh_sd:.3f}",
                    "Sortino": f"{_so_sd:.3f}",
                    "승률":    f"{_cmp_r_sd['win_count']/_cmp_r_sd['sell_count']*100:.1f}%" if _cmp_r_sd['sell_count'] > 0 else "-",
                })

            if _cmp_r_avg:
                _avg_norm = _cmp_r_avg["assets"] / _cmp_cap * 100
                _fig_cmp.add_trace(go.Scatter(
                    x=[str(d.date()) for d in _cmp_r_avg["dates"]], y=_avg_norm.tolist(),
                    name="종가평균매매", line=dict(color="#E53935", width=2, dash="dash"),
                ))
                _sh_avg, _so_avg = compute_sharpe_sortino(_cmp_r_avg["assets"])
                _cmp_metrics.append({
                    "전략": "종가평균매매",
                    "파라미터": f"a_buy={_cmp_ab*100:.2f}% / a_sell={_cmp_as*100:.2f}% / {_cmp_n+1}일",
                    "총수익률": f"{_cmp_r_avg['total_return']*100:+.2f}%",
                    "CAGR":    f"{_cmp_r_avg['cagr']*100:.2f}%",
                    "MDD":     f"{_cmp_r_avg['mdd']*100:.2f}%",
                    "Calmar":  f"{_cmp_r_avg['calmar']:.3f}",
                    "Sharpe":  f"{_sh_avg:.3f}",
                    "Sortino": f"{_so_avg:.3f}",
                    "승률":    f"{_cmp_r_avg['win_count']/_cmp_r_avg['sell_count']*100:.1f}%" if _cmp_r_avg.get('sell_count',0) > 0 else "-",
                })

            if len(_cmp_bnh_a) > 0:
                _bnh_norm = _cmp_bnh_a / _cmp_cap * 100
                _fig_cmp.add_trace(go.Scatter(
                    x=_cmp_bnh_d, y=_bnh_norm.tolist(),
                    name="Buy & Hold", line=dict(color="#FB8C00", width=1.5, dash="dot"),
                ))
                _bnh_yrs    = (pd.to_datetime(_cmp_e) - pd.to_datetime(_cmp_s)).days / 365.25
                _bnh_tot    = _cmp_bnh_a[-1] / _cmp_bnh_a[0] - 1
                _bnh_cagr   = ((_cmp_bnh_a[-1] / _cmp_bnh_a[0]) ** (1 / _bnh_yrs) - 1) if _bnh_yrs > 0 else 0
                _bnh_peak   = np.maximum.accumulate(_cmp_bnh_a)
                _bnh_mdd    = float(((np.array(_cmp_bnh_a) - _bnh_peak) / _bnh_peak).min())
                _bnh_calmar = _bnh_cagr / abs(_bnh_mdd) if _bnh_mdd != 0 else 0
                _cmp_metrics.append({
                    "전략": "Buy & Hold",
                    "파라미터": "-",
                    "총수익률": f"{_bnh_tot*100:+.2f}%",
                    "CAGR":    f"{_bnh_cagr*100:.2f}%",
                    "MDD":     f"{_bnh_mdd*100:.2f}%",
                    "Calmar":  f"{_bnh_calmar:.3f}",
                    "Sharpe":  "-", "Sortino": "-", "승률": "-",
                })

            _fig_cmp.add_hline(y=100, line_dash="dash", line_color="#aaa", annotation_text="시작(100)")
            _fig_cmp.update_layout(
                title=f"{_act_tk} | 매매법별 정규화 수익 곡선 (시작=100, {_cmp_s}~{_cmp_e})",
                yaxis_title="수익 지수 (시작=100)",
                height=420,
                legend=dict(orientation="h", y=1.08),
                hovermode="x unified",
            )
            st.plotly_chart(_fig_cmp, use_container_width=True)

            if _cmp_metrics:
                _cmp_df = pd.DataFrame(_cmp_metrics)

                def _hl_best_cmp(col_s):
                    try:
                        _nums = col_s.str.replace("%","").str.replace("+","").replace("-", "nan").astype(float)
                        best_i = _nums.idxmax()
                        return ["background-color:#e8f5e9;font-weight:bold" if i == best_i else ""
                                for i in range(len(col_s))]
                    except Exception:
                        return [""] * len(col_s)

                st.dataframe(
                    _cmp_df.style.apply(_hl_best_cmp, subset=["총수익률", "CAGR", "Calmar", "Sharpe", "Sortino"]),
                    hide_index=True, use_container_width=True,
                )

                if _cmp_r_sd and _cmp_r_avg:
                    _sd_calmar  = _cmp_r_sd["calmar"]
                    _avg_calmar = _cmp_r_avg["calmar"]
                    _sd_ret     = _cmp_r_sd["total_return"] * 100
                    _avg_ret    = _cmp_r_avg["total_return"] * 100
                    _bnh_ret_v  = (_cmp_bnh_a[-1] / _cmp_bnh_a[0] - 1) * 100 if len(_cmp_bnh_a) > 0 else 0

                    if _sd_calmar > _avg_calmar:
                        st.success(
                            f"**표준편차매매** Calmar 우위 ({_sd_calmar:.2f} vs {_avg_calmar:.2f})  \n"
                            f"-> 이 구간은 변동성 장세가 많아 sigma 자동 적응 전략이 유리했습니다."
                        )
                    else:
                        st.info(
                            f"**종가평균매매** Calmar 우위 ({_avg_calmar:.2f} vs {_sd_calmar:.2f})  \n"
                            f"-> 이 구간은 추세가 안정적이어서 고정 기준가 전략이 더 잘 맞았습니다."
                        )
                    if _sd_ret > _bnh_ret_v and _avg_ret > _bnh_ret_v:
                        st.success("두 전략 모두 Buy & Hold 대비 초과 수익을 기록했습니다.")
                    elif _sd_ret < _bnh_ret_v and _avg_ret < _bnh_ret_v:
                        st.warning("두 전략 모두 Buy & Hold에 미치지 못했습니다. 강한 단방향 상승장 구간입니다.")


# ══════════════════════════════════════════════
# TAB 5 -- Personal Settings
# ══════════════════════════════════════════════
def render_settings_tab():
    """표준편차매매 개인 설정 탭 렌더링."""
    _IS_CLOUD_val = _IS_CLOUD

    st.subheader("개인 설정")

    _cfg5    = load_config()
    _usercfg = st.session_state.get("user_settings", {}) if _IS_CLOUD_val else {}

    if _IS_CLOUD_val:
        st.info(f"**{st.session_state.get('username','')}** 으로 로그인 중 -- 설정을 저장하면 다음 로그인 시 자동으로 불러옵니다.")
    else:
        st.success(f"**로컬 PC 실행 중** -- 설정이 `{_CONFIG}` 에 저장됩니다.")

    # -- 텔레그램 알림 설정 ------
    with st.container(border=True):
        col_title, col_help = st.columns([3, 1])
        with col_title:
            st.markdown("#### 텔레그램 알림 설정")
            st.caption("포트폴리오 알림 및 주문 신호를 텔레그램으로 받을 수 있습니다.")
        with col_help:
            with st.popover("Chat ID & Bot Token 확인 방법", use_container_width=True):
                st.markdown("""
**1. Bot Token 생성하기**
- 텔레그램에서 `@BotFather` 검색 -> `/start` -> `/newbot`
- 봇 이름과 username 입력 후 발급된 HTTP API Token이 Bot Token

**2. 내 봇 시작하기**
- 만든 봇을 검색 -> `/start` -> 아무 메시지 전송

**3. Chat ID 확인하기**
- 브라우저에 `https://api.telegram.org/bot{토큰}/getUpdates` 입력
- JSON 응답에서 `"id"` 값이 Chat ID
- 또는 `@userinfobot` 에서 `/start` 로 확인 가능
                """)

        c1, c2 = st.columns(2)
        sd_tg_chat_id = c1.text_input(
            "텔레그램 Chat ID",
            value=_cfg5.get("sd_tg_chat_id", "") if not _IS_CLOUD_val else _usercfg.get("sd_tg_chat_id", ""),
            placeholder="예: 1234567890",
            key="sd_tg_chat_id_input",
        )
        sd_tg_token = c2.text_input(
            "Bot Token",
            value=_cfg5.get("sd_tg_token", "") if not _IS_CLOUD_val else _usercfg.get("sd_tg_token", ""),
            placeholder="예: 123456789:AAF...",
            type="password",
            key="sd_tg_token_input",
        )
        st.caption("주문표는 매주 월~금 오후 3:00 (KST)에 텔레그램으로 자동 발송됩니다")

        btn_col1, btn_col2, spacer = st.columns([1, 1, 4])
        with btn_col1:
            if st.button("주문표 테스트 발송", use_container_width=True, key="sd_tg_test"):
                if not sd_tg_chat_id or not sd_tg_token:
                    st.warning("Chat ID와 Bot Token을 먼저 입력해주세요.")
                else:
                    _sd_settings = _get_sd_ticker_settings()
                    if not _sd_settings:
                        st.warning("등록된 표준편차 계좌가 없습니다. 오늘의 주문표 탭에서 계좌를 먼저 등록해주세요.")
                    else:
                        for _sd_tk, _sd_cfg in _sd_settings.items():
                            with st.spinner(f"{_sd_tk} 시뮬레이션 & 발송 중..."):
                                try:
                                    _sd_start_d = datetime.strptime(
                                        _sd_cfg.get("os_start", "2024-01-01"), "%Y-%m-%d").date()
                                except Exception:
                                    _sd_start_d = datetime(2024, 1, 1).date()
                                msg = _build_sd_order_text(
                                    _sd_tk,
                                    k_buy        = float(_sd_cfg.get("k_buy",        0.65)),
                                    k_sell       = float(_sd_cfg.get("k_sell",       0.65)),
                                    sigma_period = int  (_sd_cfg.get("sigma_period", 2)),
                                    sell_ratio   = float(_sd_cfg.get("sell_ratio",   75.0)),
                                    divisions    = int  (_sd_cfg.get("divisions",    5)),
                                    renewal      = int  (_sd_cfg.get("renewal",      5)),
                                    _os_start    = _sd_start_d,
                                    _os_capital  = float(_sd_cfg.get("os_capital",  20000.0)),
                                    capital_adj_history = _sd_cfg.get("capital_adj_history", "[]"),
                                )
                                result = _send_telegram(sd_tg_token, sd_tg_chat_id, msg)
                            if result.get("ok"):
                                st.success(f"{_sd_tk} 발송 성공!")
                            else:
                                st.error(f"{_sd_tk} 발송 실패: {result.get('description', '알 수 없는 오류')}")

        with btn_col2:
            if st.button("저장하기", use_container_width=True, key="sd_tg_save", type="primary"):
                if not sd_tg_chat_id or not sd_tg_token:
                    st.warning("Chat ID와 Bot Token을 모두 입력해주세요.")
                elif _IS_CLOUD_val:
                    with st.spinner("저장 중..."):
                        try:
                            _save_user_settings_to_sheet(
                                st.session_state.username,
                                {"sd_tg_chat_id": sd_tg_chat_id, "sd_tg_token": sd_tg_token})
                            st.session_state.user_settings.update(
                                {"sd_tg_chat_id": sd_tg_chat_id, "sd_tg_token": sd_tg_token})
                            st.success("Google Sheets에 저장 완료!")
                        except Exception as e:
                            st.error(f"저장 실패: {e}")
                else:
                    save_config({"sd_tg_chat_id": sd_tg_chat_id, "sd_tg_token": sd_tg_token}, sensitive=True)
                    st.success(f"저장 완료! `{_CONFIG}`")

    st.write("")

    # -- 구글 스프레드시트 연동 ------
    with st.container(border=True):
        col_gs1, col_gs2 = st.columns([3, 1])
        with col_gs1:
            st.markdown("#### 구글 스프레드시트 연동")
            st.caption("포트폴리오 정보와 주문 신호를 구글 스프레드시트로 전송합니다.")
        with col_gs2:
            with st.popover("구글 스프레드시트 URL 확인 & 권한 부여", use_container_width=True):
                st.markdown("""
**1. 새 스프레드시트 만들기**
- Google Sheets에서 새 스프레드시트를 만듭니다.

**2. URL 복사**
- 브라우저 주소창의 URL을 복사합니다.

**3. 서비스 계정에 편집 권한 부여**
- 스프레드시트 공유 -> 아래 이메일을 편집자로 추가
- `connectspreadsheet@sodium-gateway-485307-f3.iam.gserviceaccount.com`
                """)

        _cfg5_gs = load_config()
        gs_url_sd = st.text_input(
            "스프레드시트 URL",
            value=_cfg5_gs.get("gs_url", "") if not _IS_CLOUD_val else _usercfg.get("gs_url", ""),
            placeholder="https://docs.google.com/spreadsheets/d/...",
            key="gs_url_input_sd",
        )
        st.caption("* 스프레드시트에 서비스 계정 이메일을 편집자로 공유해주세요. (우측 상단 도움말 참고)")

        _tung_default_sd = str(
            _usercfg.get("sd_use_tungchigi", "") if _IS_CLOUD_val
            else _cfg5_gs.get("sd_use_tungchigi", "")
        ).strip().lower() in ("true", "1", "y", "yes", "on")
        use_tungchigi_sd = st.checkbox(
            "시트 전송 시 퉁치기 주문으로 전송 (자전거래 거부 증권사용)",
            value=_tung_default_sd, key="sd_use_tungchigi_ck",
            help="체크하면 매수/매도 상계(퉁치기) 적용 주문을 시트에 기록합니다. "
                 "순 체결 결과는 원 주문과 동일하며, LOC매도가 < LOC매수가 교차를 "
                 "거부하는 증권사에서 사용하세요. 텔레그램/자동발송에도 적용됩니다.")

        # 표준편차 계좌별 시트 이름 매핑
        _gs_sd_settings = _get_sd_ticker_settings()
        _gs_sd_sheet_map = {}
        if _gs_sd_settings:
            st.markdown("##### 종목별 시트 이름 매핑")
            st.caption("각 종목 데이터를 기록할 구글시트의 탭(시트) 이름을 입력하세요.")
            for _gs_sd_tk, _gs_sd_cfg in _gs_sd_settings.items():
                _default_sheet = _gs_sd_cfg.get("gs_sheet", _gs_sd_tk)
                _gs_sd_sheet_map[_gs_sd_tk] = st.text_input(
                    f"{_gs_sd_tk} 시트 이름",
                    value=_default_sheet,
                    key=f"gs_sheet_sd_{_gs_sd_tk}",
                )
        else:
            st.info("등록된 표준편차 계좌가 없습니다. 오늘의 주문표 탭에서 계좌를 먼저 등록해주세요.")

        st.write("")
        btn_gs1, btn_gs2, btn_gs3 = st.columns(3)
        with btn_gs1:
            if st.button("시트 연결 테스트", use_container_width=True, key="gs_test_sd"):
                if not gs_url_sd:
                    st.warning("스프레드시트 URL을 먼저 입력해주세요.")
                else:
                    try:
                        gc = _get_gspread_client()
                        sh = gc.open_by_url(gs_url_sd)
                        st.success(f"연결 성공! 스프레드시트: **{sh.title}**")
                    except Exception as e:
                        st.error(f"연결 실패: {e}")

        with btn_gs2:
            if st.button("주문 시트 전송", use_container_width=True, key="gs_send_sd", type="primary"):
                if not gs_url_sd:
                    st.warning("스프레드시트 URL을 먼저 입력해주세요.")
                elif not _gs_sd_settings:
                    st.warning("등록된 표준편차 계좌가 없습니다.")
                else:
                    for _gs_sd_tk, _gs_sd_cfg in _gs_sd_settings.items():
                        _sheet_nm = _gs_sd_sheet_map.get(_gs_sd_tk, _gs_sd_tk)
                        with st.spinner(f"{_gs_sd_tk} -> '{_sheet_nm}' 전송 중..."):
                            try:
                                try:    _gs_sd_start = datetime.strptime(_gs_sd_cfg.get("os_start", "2024-01-01"), "%Y-%m-%d").date()
                                except: _gs_sd_start = datetime(2024, 1, 1).date()
                                _gs_sd_cap = float(_gs_sd_cfg.get("os_capital", 20000.0))
                                _buf = (pd.to_datetime(str(_gs_sd_start)) - pd.DateOffset(days=90)).strftime("%Y-%m-%d")
                                _pdf_gs = load_price_data(_gs_sd_tk, _buf, str(datetime.today().date()), "야후파이낸스 (yfinance)", None)
                                _res_gs = run_stdev_ordersheet(
                                    _pdf_gs, str(_gs_sd_start),
                                    sigma_period = int  (_gs_sd_cfg.get("sigma_period", 2)),
                                    k_buy        = float(_gs_sd_cfg.get("k_buy",        0.65)),
                                    k_sell       = float(_gs_sd_cfg.get("k_sell",       0.65)),
                                    sell_ratio   = float(_gs_sd_cfg.get("sell_ratio",   75.0)),
                                    divisions    = int  (_gs_sd_cfg.get("divisions",    5)),
                                    renewal      = int  (_gs_sd_cfg.get("renewal",      5)),
                                    initial_capital = _gs_sd_cap,
                                )
                                if _res_gs is None:
                                    st.error(f"{_gs_sd_tk}: 시뮬레이션 데이터가 없습니다.")
                                else:
                                    gc = _get_gspread_client()
                                    sh = gc.open_by_url(gs_url_sd)
                                    ws = sh.worksheet(_sheet_nm)
                                    ws.batch_clear(["L4:O13"])
                                    rows_gs = [["매수", "LOC", round(_res_gs["next_buy_loc"], 2), _res_gs["est_buy_qty"]]]
                                    if _res_gs["holdings"] > 0:
                                        rows_gs.append(["매도", "LOC", round(_res_gs["next_sell_loc"], 2), _res_gs["est_sell_qty"]])
                                    if use_tungchigi_sd and rows_gs:
                                        try:
                                            from dss_engine import rows_to_tungchigi_rows as _rttr_sd
                                        except ImportError:
                                            import importlib, dss_engine as _de_gs
                                            _rttr_sd = importlib.reload(_de_gs).rows_to_tungchigi_rows
                                        rows_gs = _rttr_sd(rows_gs)
                                    ws.update(range_name="L4", values=rows_gs)
                                    # B11 업데이트 시각 (KST)
                                    ws.update(range_name="B11", values=[[
                                        pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")]])
                                    st.success(f"{_gs_sd_tk} -> '{_sheet_nm}' L4에 {len(rows_gs)}건 전송 완료!")
                            except Exception as e:
                                st.error(f"{_gs_sd_tk} 전송 실패: {e}")

        with btn_gs3:
            if st.button("저장하기 ", use_container_width=True, key="gs_save_sd", type="primary"):
                if not gs_url_sd:
                    st.warning("스프레드시트 URL을 입력해주세요.")
                else:
                    if _IS_CLOUD_val:
                        try:
                            _save_user_settings_to_sheet(st.session_state.username,
                                                         {"gs_url": gs_url_sd,
                                                          "sd_use_tungchigi": use_tungchigi_sd})
                            st.session_state.user_settings.update(
                                {"gs_url": gs_url_sd,
                                 "sd_use_tungchigi": str(use_tungchigi_sd)})
                        except Exception as e:
                            st.error(f"저장 실패: {e}")
                    else:
                        save_config({"gs_url": gs_url_sd}, sensitive=True)
                        save_config({"sd_use_tungchigi": str(use_tungchigi_sd)})
                    for _gs_sd_tk, _sheet_nm in _gs_sd_sheet_map.items():
                        _save_sd_ticker_setting(_gs_sd_tk, {"gs_sheet": _sheet_nm})
                    st.success("URL 및 종목별 시트 이름 저장 완료!")

    st.write("")

    # -- 관리자 도구: 비밀번호 해시 생성 -----
    with st.expander("관리자 도구 -- 비밀번호 해시 생성 (users 시트 등록용)"):
        st.caption("새 사용자를 추가할 때 비밀번호를 bcrypt 해시로 변환하여 Google Sheets에 붙여넣으세요.")
        _admin_pw_sd = st.text_input("등록할 비밀번호 입력", type="password", key="admin_pw_input_sd")
        if st.button("해시 생성", key="gen_hash_sd"):
            if _admin_pw_sd:
                st.code(_hash_password(_admin_pw_sd), language=None)
                st.caption("위 해시를 복사해서 users 시트의 password_hash 컬럼에 붙여넣으세요.")
            else:
                st.warning("비밀번호를 입력해주세요.")
