"""
strategies/dss.py — DSS 동파법(동적파도타기법) 전략 모듈

종목: SOXL (3x 레버리지 ETF)
모드: QQQ 주간 RSI 기반 안전모드(SF)/공세모드(AG) 자동 전환
매매: LOC(Limit on Close), 시드분할, 복리 관리

인터페이스: Sigma와 동일 패턴 (자체 사이드바 사용)
- render_sidebar() → params dict
- render_backtest_tab(params)
- render_optimization_tab(params)
- render_ordersheet_tab(params)
- render_intro_tab(params)  — 통합앱에서는 render_intro_tab() 호출
- render_settings_tab()
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import math
import json
import itertools
import random
import os
import sys
import requests
import io as _io
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

# dss_engine.py는 app.py와 같은 루트에 위치 — sys.path 보장
_root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _root_dir not in sys.path:
    sys.path.insert(0, _root_dir)

from dss_engine import (
    load_price_data, get_weekly_closes, calc_weekly_rsi,
    build_weekly_rsi_series, build_mode_series, determine_mode,
    get_week_mode_map,
    run_backtest, run_backtest_fast, DSSParams
)

from common.config import _IS_CLOUD, _CONFIG, load_config, save_config, \
    get_ticker_settings, save_ticker_setting

# ──────────────────────────────────────────────
# DSS 설정 경로 (로컬: ~/.dss/, 클라우드: session_state)
# ──────────────────────────────────────────────
_DSS_CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".dss")
_DSS_CONFIG_PATH = os.path.join(_DSS_CONFIG_DIR, "config.json")


def _load_dss_config() -> dict:
    """DSS 설정 로드. Cloud: GSheets(session_state) / 로컬: ~/.dss/config.json"""
    if _IS_CLOUD and st.session_state.get("logged_in"):
        raw = st.session_state.get("user_settings", {}).get("dss_config", "")
        if raw:
            try:
                cfg = json.loads(raw) if isinstance(raw, str) else raw
                return cfg if isinstance(cfg, dict) else {}
            except Exception:
                pass
        return {}
    # 로컬
    if os.path.exists(_DSS_CONFIG_PATH):
        try:
            with open(_DSS_CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_dss_config(cfg: dict):
    """DSS 설정 저장. Cloud: session_state + GSheets / 로컬: ~/.dss/config.json"""
    # 로컬 파일 저장 (로컬 PC용 + Cloud 임시 백업)
    try:
        os.makedirs(_DSS_CONFIG_DIR, exist_ok=True)
        with open(_DSS_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # Cloud: Google Sheets에 영구 저장
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            cfg_json = json.dumps(cfg, ensure_ascii=False)
            if "user_settings" not in st.session_state:
                st.session_state.user_settings = {}
            st.session_state.user_settings["dss_config"] = cfg_json
            from common.auth import _save_user_settings_to_sheet
            _save_user_settings_to_sheet(st.session_state.username, {"dss_config": cfg_json})
        except Exception as e:
            st.warning(f"⚠️ Cloud 저장 실패 (로컬에는 저장됨): {e}")


# ──────────────────────────────────────────────
# 텔레그램
# ──────────────────────────────────────────────

def _send_telegram(token: str, chat_id: str, text: str) -> dict:
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        return resp.json()
    except Exception as e:
        return {"ok": False, "description": str(e)}


def _build_dss_order_text(os_result: dict, acct_name: str = "") -> str:
    """주문표 결과 dict를 텔레그램 메시지로 포맷팅."""
    _o = os_result
    mode_icon = "🟢" if _o["last_mode"] == "AG" else "🔵"
    mode_label = "공세" if _o["last_mode"] == "AG" else "안전"
    _today_str = datetime.today().strftime('%Y-%m-%d')
    _acct_label = f" [{acct_name}]" if acct_name else ""
    lines = [
        f"<b>📋 DSS 동파법 — SOXL 주문표{_acct_label}</b>",
        f"📅 {_today_str}  {mode_icon} {mode_label}모드",
        f"",
        f"전일종가: <b>${_o['prev_close']:,.2f}</b>",
        f"총자산: <b>${_o['total_asset']:,.0f}</b>  (현금 ${_o['cash']:,.0f})",
        f"보유: {_o['n_pos']}/{_o['cur_divisions']}시드",
    ]

    # 거래일 인덱스 (예약매도 잔여일 계산) — 미래 영업일 보정 포함
    try:
        _raw_idx = get_soxl_data().index
        _extra_bdays = pd.bdate_range(_raw_idx[-1] + pd.Timedelta(days=1), periods=60)
        _tdays = _raw_idx.append(_extra_bdays)
    except Exception:
        _tdays = None
    _today_ts = pd.Timestamp(datetime.today().date())

    # ── 포지션별 데이터 수집 ──
    _pos_data = []  # (pos, idx, is_stop, remain, reserve_date_str)
    for i, pos in enumerate(_o['open_positions']):
        if pos['sell_target'] is None:
            continue
        _stop = pos.get('stop_date')
        _is_stop = False
        _remain = None
        _rdate = ""
        if _stop is not None:
            _stop_ts = pd.Timestamp(_stop)
            _is_stop = (_stop_ts <= _today_ts)
            if not _is_stop and _tdays is not None:
                _future = _tdays[(_tdays > _today_ts) & (_tdays <= _stop_ts)]
                _remain = len(_future)
                _before = _tdays[_tdays < _stop_ts]
                if len(_before) > 0:
                    _rdate = _before[-1].strftime('%m/%d').replace('/0', '/').lstrip('0')
                else:
                    _rdate = _stop_ts.strftime('%m/%d').replace('/0', '/').lstrip('0')
        _pos_data.append((pos, i, _is_stop, _remain, _rdate))

    # 예약 중인 티어 번호 (손절일 아닌 보유 포지션)
    _reserve_tiers = {i + 1 for pos, i, _is_stop, _remain, _rdate in _pos_data if not _is_stop}

    # ── 오늘의 주문 ──
    lines.append(f"")
    lines.append(f"── 오늘의 주문 ──")
    for pos, i, _is_stop, _remain, _rdate in _pos_data:
        _tier = i + 1
        _star = "★" if _tier in _reserve_tiers else ""
        if _is_stop:
            lines.append(
                f"🔴 MOC매도 티어{_tier}: 시장가 "
                f"× {pos['qty']}주 (손절일)"
            )
        else:
            pnl_pct = (pos['sell_target'] / pos['buy_price'] - 1) * 100
            lines.append(
                f"📈 LOC매도 {_star}티어{_tier}: ${pos['sell_target']:,.2f} "
                f"× {pos['qty']}주 ({pnl_pct:+.1f}%)"
            )
    if _o['n_pos'] < _o['cur_divisions']:
        lines.append(
            f"📉 LOC매수 티어{_o['n_pos']+1}: ${_o['next_buy_order']:,.2f} "
            f"× {_o['buy_qty_est']}주"
        )
    else:
        lines.append(f"⚠️ 전 슬롯 사용 중 — 매수 없음")

    # ── 예약 현황 (예약 중인 티어가 있을 때만) ──
    _reserve_lines = []
    for pos, i, _is_stop, _remain, _rdate in _pos_data:
        if _is_stop:
            continue
        _tier = i + 1
        _deadline = f"예약~{_rdate} (잔여 {_remain}일)" if _remain is not None and _rdate else ""
        _reserve_lines.append(f" ★티어{_tier}: ${pos['sell_target']:,.2f} {_deadline}")

    if _reserve_lines:
        lines.append(f"")
        lines.append(f"── 예약 현황 ──")
        lines.extend(_reserve_lines)

    if _o.get('latest_rsi'):
        lines.append(f"")
        lines.append(f"QQQ RSI: {_o['latest_rsi']:.1f}")
    return "\n".join(lines)


# ── 구글시트 연동 ──

def _get_gspread_client():
    """Streamlit Cloud(st.secrets) 또는 로컬(service_account.json)로 gspread 인증."""
    from common.config import _get_gspread_client as _common_gs_client
    return _common_gs_client()


def _write_dss_orders_to_sheet(gs_url: str, gs_sheet: str, os_result: dict,
                               template_sheet: str = "") -> int:
    """주문표 결과를 구글시트에 기록 (L:구분, M:거래방법, N:가격, O:수량).
    시트가 없으면 template을 복제해서 생성."""
    gc = _get_gspread_client()
    sh = gc.open_by_url(gs_url)
    try:
        ws = sh.worksheet(gs_sheet)
    except Exception:
        # 템플릿이 지정되어 있으면 복제
        if template_sheet:
            try:
                tpl_ws = sh.worksheet(template_sheet)
                ws = tpl_ws.duplicate(new_sheet_name=gs_sheet)
            except Exception:
                ws = sh.add_worksheet(title=gs_sheet, rows=100, cols=20)
        else:
            ws = sh.add_worksheet(title=gs_sheet, rows=100, cols=20)
    ws.batch_clear(["L4:O13"])
    # 공통 rows 생성 — dss_engine.build_order_rows 재사용 (스크립트와 SSOT)
    from dss_engine import build_order_rows
    rows = build_order_rows(os_result)
    if rows:
        ws.update(range_name="L4", values=rows)
    return len(rows)


# ── 자본 조정 이력 유틸 (common/analysis.py 재export) ──
from common.analysis import recalc_adj_history as _recalc_adj_history


# ── 일별 매매 히스토리 (B방식: 과거 불변, 새 날짜만 누적) ──

def _get_history_path(acct_name: str = "") -> str:
    if not acct_name or acct_name == "기본계좌":
        return os.path.join(_DSS_CONFIG_DIR, "history_SOXL.csv")
    safe = acct_name.replace(" ", "_").replace("/", "_").replace("\\", "_")
    return os.path.join(_DSS_CONFIG_DIR, f"history_SOXL_{safe}.csv")


def _load_dss_history(acct_name: str = "") -> pd.DataFrame:
    path = _get_history_path(acct_name)
    if os.path.exists(path):
        try:
            return pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def _save_dss_history(bt_df: pd.DataFrame, acct_name: str = ""):
    if bt_df is None or bt_df.empty:
        return
    rows = []
    for _, row in bt_df.iterrows():
        date_str = str(row['날짜'])[:10]
        close = float(row['종가'])
        mode = row['모드']
        buy_order = row['매수주문가']
        sell_target = row['매도목표가']
        action = "-"
        trade_qty = 0
        trade_amt = 0.0
        realized_pnl = None
        realized_pct = None
        bought = row['매수체결'] is not None and not (isinstance(row['매수체결'], float) and np.isnan(row['매수체결']))
        sold = row['매도내역'] is not None
        if sold:
            sell_details = row['매도내역']
            total_sell_qty = sum(s['qty'] for s in sell_details)
            total_sell_amt = sum(s['qty'] * s['sell_price'] for s in sell_details)
            total_pnl = sum(s['pnl'] for s in sell_details)
            total_buy_amt = sum(s['qty'] * s['buy_price'] for s in sell_details)
            pnl_pct = (total_pnl / total_buy_amt * 100) if total_buy_amt > 0 else 0
            action = f"SELL (${close:.2f})"
            trade_qty = -total_sell_qty
            trade_amt = total_sell_amt
            realized_pnl = total_pnl
            realized_pct = pnl_pct
        if bought:
            buy_price = float(row['매수체결'])
            qty = int(row['수량'])
            if action == "-":
                action = f"BUY (${buy_price:.2f})"
                trade_qty = qty
                trade_amt = buy_price * qty
            else:
                action += f" / BUY (${buy_price:.2f})"
                trade_qty += qty
                trade_amt += buy_price * qty
        rows.append({
            "날짜": date_str, "종가": round(close, 4), "모드": mode,
            "매수주문가": round(float(buy_order), 4) if buy_order is not None else "-",
            "매도경계가": round(float(sell_target), 4) if sell_target is not None and not (isinstance(sell_target, float) and np.isnan(sell_target)) else "-",
            "매매": action, "거래주수": trade_qty if trade_qty != 0 else "-",
            "거래금액($)": round(trade_amt, 2) if trade_amt > 0 else "-",
            "실현손익($)": f"+${realized_pnl:,.2f}" if realized_pnl is not None and realized_pnl >= 0 else f"-${abs(realized_pnl):,.2f}" if realized_pnl is not None else "-",
            "실현손익률(%)": f"+{realized_pct:.2f}%" if realized_pct is not None and realized_pct >= 0 else f"{realized_pct:.2f}%" if realized_pct is not None else "-",
            "보유시드": int(row['보유포지션수']), "보유기간": "-",
            "현금($)": f"${float(row['예수금']):,.2f}",
            "총자산($)": f"${float(row['총자산']):,.2f}",
        })
    df_new = pd.DataFrame(rows)

    # ── 오늘/미래 날짜(미국장 미마감)는 히스토리에서 제외 ──
    _cutoff = None
    try:
        _now_est = pd.Timestamp.now(tz="America/New_York")
        _market_close = _now_est.replace(hour=16, minute=30,
                                          second=0, microsecond=0)
        if _now_est < _market_close:
            _cutoff = str(_now_est.date())  # 'YYYY-MM-DD' (US 오늘)
            df_new = df_new[df_new["날짜"].astype(str) < _cutoff]
    except Exception:
        pass

    df_existing_raw = _load_dss_history(acct_name)
    df_existing = df_existing_raw.copy() if not df_existing_raw.empty else df_existing_raw
    _cleaned = False
    if not df_existing.empty and "날짜" in df_existing.columns and _cutoff:
        _before = len(df_existing)
        df_existing = df_existing[df_existing["날짜"].astype(str) < _cutoff]
        _cleaned = (len(df_existing) < _before)

    # ── 로컬 CSV 저장 ──
    if not df_existing.empty and "날짜" in df_existing.columns:
        existing_dates = set(df_existing["날짜"].astype(str))
        df_add_local = df_new[~df_new["날짜"].astype(str).isin(existing_dates)].copy()
        if not df_add_local.empty or _cleaned:
            df_merged = pd.concat([df_existing, df_add_local], ignore_index=True)
            os.makedirs(_DSS_CONFIG_DIR, exist_ok=True)
            df_merged.to_csv(_get_history_path(acct_name), index=False, encoding="utf-8-sig")
    else:
        os.makedirs(_DSS_CONFIG_DIR, exist_ok=True)
        df_new.to_csv(_get_history_path(acct_name), index=False, encoding="utf-8-sig")

    # ── Google Sheets 매매기록 워크시트 동기화 ──
    # (로컬 CSV 상태와 무관하게 GSheets 자체 기준으로 동기화)
    _cfg = _load_dss_config()
    _gs_url = _cfg.get("gs_url", "")
    if _gs_url and not df_new.empty:
        try:
            import gspread as _gs
            _safe_name = (acct_name or "기본계좌").replace(" ", "_").replace("/", "_").replace("\\", "_")
            _ws_name = f"dss_{_safe_name}_매매기록"
            client = _get_gspread_client()
            sh = client.open_by_url(_gs_url)
            _created_new = False
            try:
                ws = sh.worksheet(_ws_name)
                _existing_cells = ws.get_all_values()
                _gs_dates = set()
                if len(_existing_cells) > 1:
                    _header = _existing_cells[0]
                    if "날짜" in _header:
                        _date_idx = _header.index("날짜")
                        _gs_dates = {row[_date_idx] for row in _existing_cells[1:]
                                     if len(row) > _date_idx}
                _df_add_gs = df_new[~df_new["날짜"].astype(str).isin(_gs_dates)].copy()
            except _gs.WorksheetNotFound:
                ws = sh.add_worksheet(title=_ws_name, rows=5000, cols=20)
                ws.append_row(df_new.columns.tolist())
                _df_add_gs = df_new.copy()
                _created_new = True

            if not _df_add_gs.empty:
                rows_to_add = [[str(v) for v in row] for row in _df_add_gs.values.tolist()]
                ws.append_rows(rows_to_add, value_input_option="RAW")
                try:
                    if _created_new:
                        st.toast(f"📊 '{_ws_name}' 시트 신규 생성 + {len(rows_to_add)}건 동기화",
                                 icon="✅")
                    else:
                        st.toast(f"📊 '{_ws_name}'에 {len(rows_to_add)}건 추가",
                                 icon="✅")
                except Exception:
                    pass
        except Exception as _gs_err:
            try:
                st.warning(f"⚠️ GSheets 동기화 실패 [{acct_name}]: {_gs_err}")
            except Exception:
                pass


# ──────────────────────────────────────────────
# 다음 거래일 (미국 휴장일 제외)
# ──────────────────────────────────────────────

def _nth_weekday(year, month, weekday, n):
    first = datetime(year, month, 1).date()
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))

def _last_weekday(year, month, weekday):
    if month == 12:
        last = datetime(year + 1, 1, 1).date() - timedelta(days=1)
    else:
        last = datetime(year, month + 1, 1).date() - timedelta(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - timedelta(days=offset)

def _easter_date(year):
    a = year % 19; b, c = divmod(year, 100); d, e = divmod(b, 4)
    f = (b + 8) // 25; g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30; i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return datetime(year, month, day + 1).date()

def next_trading_date(d=None):
    if d is None:
        d = datetime.today().date()
    year = d.year
    us_holidays = set()
    us_holidays.add(datetime(year, 1, 1).date())
    us_holidays.add(_nth_weekday(year, 1, 0, 3))
    us_holidays.add(_nth_weekday(year, 2, 0, 3))
    us_holidays.add(_easter_date(year) - timedelta(days=2))
    us_holidays.add(_last_weekday(year, 5, 0))
    us_holidays.add(datetime(year, 6, 19).date())
    us_holidays.add(datetime(year, 7, 4).date())
    us_holidays.add(_nth_weekday(year, 9, 0, 1))
    us_holidays.add(_nth_weekday(year, 11, 3, 4))
    us_holidays.add(datetime(year, 12, 25).date())
    observed = set()
    for h in us_holidays:
        if h.weekday() == 5: observed.add(h - timedelta(days=1))
        elif h.weekday() == 6: observed.add(h + timedelta(days=1))
    all_holidays = us_holidays | observed
    candidate = d + timedelta(days=1)
    for _ in range(30):
        if candidate.weekday() < 5 and candidate not in all_holidays:
            return candidate
        candidate += timedelta(days=1)
    return candidate


# ──────────────────────────────────────────────
# 데이터 캐싱
# ──────────────────────────────────────────────

@st.cache_data(show_spinner="SOXL 데이터 로딩...", ttl=300)
def get_soxl_data():
    df = load_price_data("SOXL", "2009-06-01", "2026-12-31")
    if df is None or df.empty:
        raise RuntimeError("SOXL 데이터 로드 실패 (yfinance)")
    return df

@st.cache_data(show_spinner="QQQ 데이터 로딩...", ttl=300)
def get_qqq_data():
    df = load_price_data("QQQ", "2009-01-01", "2026-12-31")
    if df is None or df.empty:
        raise RuntimeError("QQQ 데이터 로드 실패 (yfinance)")
    return df

def clear_dss_data_cache():
    """DSS 가격 데이터 캐시 강제 초기화 (yfinance intraday 오염 복구용)."""
    try:
        get_soxl_data.clear()
        get_qqq_data.clear()
        get_mode_series.clear()
    except Exception:
        pass

@st.cache_data(show_spinner="주간 RSI 계산 중...")
def get_mode_series(_qqq_hash):
    qqq = get_qqq_data()
    weekly_rsi = build_weekly_rsi_series(qqq)
    return build_mode_series(weekly_rsi)


# ──────────────────────────────────────────────
# 사이드바 (Sigma와 동일 패턴: 자체 사이드바)
# ──────────────────────────────────────────────

def render_sidebar():
    """DSS 사이드바 렌더링. params dict 반환."""
    st.sidebar.markdown("### 🔄 공통 설정")
    col1, col2, col3 = st.sidebar.columns(3)
    pcr = col1.number_input("이익복리율\n(%)", min_value=0, max_value=100, value=80, step=1, key="dss_pcr")
    lcr = col2.number_input("손실복리율\n(%)", min_value=0, max_value=100, value=30, step=1, key="dss_lcr")
    renewal_period = col3.number_input("갱신주기\n(일)", min_value=1, max_value=100, value=10, step=1, key="dss_renew")

    st.sidebar.markdown("### 🔵 안전모드 (SF)")
    sc1, sc2, sc3, sc4 = st.sidebar.columns(4)
    sf_div = sc1.number_input("분할수", min_value=1, max_value=50, value=7, step=1, key="dss_sf_div")
    sf_hold = sc2.number_input("최대보유", min_value=1, max_value=100, value=30, step=1, key="dss_sf_hold")
    sf_buy = sc3.number_input("매수%", min_value=0.0, max_value=30.0, value=3.0, step=0.01, format="%.2f", key="dss_sf_buy")
    sf_sell = sc4.number_input("매도%", min_value=0.0, max_value=30.0, value=0.2, step=0.01, format="%.2f", key="dss_sf_sell")

    st.sidebar.markdown("### 🟢 공세모드 (AG)")
    ac1, ac2, ac3, ac4 = st.sidebar.columns(4)
    ag_div = ac1.number_input("분할수", min_value=1, max_value=50, value=7, step=1, key="dss_ag_div")
    ag_hold = ac2.number_input("최대보유", min_value=1, max_value=100, value=7, step=1, key="dss_ag_hold")
    ag_buy = ac3.number_input("매수%", min_value=0.0, max_value=30.0, value=5.0, step=0.01, format="%.2f", key="dss_ag_buy")
    ag_sell = ac4.number_input("매도%", min_value=0.0, max_value=30.0, value=2.5, step=0.01, format="%.2f", key="dss_ag_sell")

    st.sidebar.markdown("### ⚙️ 백테스트 설정")
    bc1, bc2 = st.sidebar.columns(2)
    initial_capital = bc1.number_input("초기투자금", min_value=1000, max_value=10000000, value=10000, step=1000, key="dss_cap")
    fee_rate = bc2.number_input("수수료(%)", min_value=0.0, max_value=1.0, value=0.04, step=0.001, format="%.3f", key="dss_fee")

    dc1, dc2 = st.sidebar.columns(2)
    start_date = dc1.date_input("투자시작일", value=pd.Timestamp("2010-03-12"), key="dss_start")
    end_date = dc2.date_input("투자종료일", value=pd.Timestamp("2026-04-10"), key="dss_end")

    return {
        "sf_div": sf_div, "sf_hold": sf_hold, "sf_buy": sf_buy, "sf_sell": sf_sell,
        "ag_div": ag_div, "ag_hold": ag_hold, "ag_buy": ag_buy, "ag_sell": ag_sell,
        "pcr": pcr, "lcr": lcr, "renewal_period": renewal_period,
        "initial_capital": initial_capital, "fee_rate": fee_rate,
        "start_date": start_date, "end_date": end_date,
        "bt_ticker": "SOXL",
        "bt_start_date": start_date, "bt_end_date": end_date,
        "bt_initial_capital": initial_capital,
    }


def _make_params(p):
    """params dict → DSSParams 변환."""
    return DSSParams(
        sf_divisions=p["sf_div"], sf_max_hold=p["sf_hold"],
        sf_buy_pct=p["sf_buy"] / 100, sf_sell_pct=p["sf_sell"] / 100,
        ag_divisions=p["ag_div"], ag_max_hold=p["ag_hold"],
        ag_buy_pct=p["ag_buy"] / 100, ag_sell_pct=p["ag_sell"] / 100,
        initial_capital=float(p["initial_capital"]),
        fee_rate=p["fee_rate"] / 100,
        renewal_period=p["renewal_period"],
        pcr=p["pcr"] / 100, lcr=p["lcr"] / 100,
    )


# ══════════════════════════════════════════════
# 탭 1: 백테스트
# ══════════════════════════════════════════════

def render_backtest_tab(params):
    """백테스트 탭."""
    p = params
    st.subheader("📊 DSS 동파법 백테스트")

    if st.button("▶ 백테스트 실행", type="primary", key="dss_bt_run"):
        try:
            soxl = get_soxl_data()
            qqq = get_qqq_data()
            ms = get_mode_series(len(qqq))
        except Exception as _e:
            st.error(f"⚠️ 가격 데이터 로드 실패: {_e}")
            return
        dss_p = _make_params(p)

        with st.spinner("백테스트 실행 중..."):
            bt = run_backtest(dss_p, soxl, ms, str(p["start_date"]), str(p["end_date"]))

        if bt is not None and not bt.empty:
            st.session_state["dss_bt_result"] = bt
            st.session_state["dss_bt_params"] = dss_p
        else:
            st.warning("결과가 없습니다. 날짜 범위를 확인하세요.")

    if "dss_bt_result" not in st.session_state:
        st.info("👆 '백테스트 실행' 버튼을 클릭하세요.")
        return

    result = st.session_state["dss_bt_result"]
    dss_p = st.session_state["dss_bt_params"]

    # ── 핵심 지표 ──
    total_days = len(result)
    final_total = float(result.iloc[-1]['총자산'])
    cum_realized = float(result.iloc[-1]['누적실현'])
    total_sells = int(result.iloc[-1]['누적매도'])
    _init = float(p["initial_capital"])
    total_return = (final_total / _init - 1) * 100

    # CAGR
    years = total_days / 252
    cagr = ((final_total / _init) ** (1 / years) - 1) * 100 if years > 0 else 0

    # MDD
    peak = result['총자산'].cummax()
    dd = (result['총자산'] - peak) / peak
    mdd = dd.min() * 100

    # 승률
    all_sells = []
    for _, row in result.iterrows():
        if row['매도내역']:
            all_sells.extend(row['매도내역'])

    if all_sells:
        wins = sum(1 for s in all_sells if s['pnl'] > 0)
        win_rate = wins / len(all_sells) * 100
        avg_pnl = sum(s['pnl'] for s in all_sells) / len(all_sells)
    else:
        win_rate = 0
        avg_pnl = 0

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("총자산", f"${final_total:,.0f}")
    col2.metric("총수익률", f"{total_return:.1f}%")
    col3.metric("CAGR", f"{cagr:.1f}%")
    col4.metric("MDD", f"{mdd:.1f}%")
    col5.metric("승률", f"{win_rate:.1f}%")
    col6.metric("매도횟수", f"{int(total_sells)}회")

    col7, col8, col9, col10 = st.columns(4)
    col7.metric("누적실현손익", f"${cum_realized:,.0f}")
    col8.metric("평균손익/건", f"${avg_pnl:.1f}")
    col9.metric("거래일수", f"{total_days}일")
    col10.metric("투자기간", f"{years:.1f}년")

    # ── 자산 추이 차트 (3행) ──
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.5, 0.25, 0.25],
        subplot_titles=("총자산 추이", "SOXL 종가", "일별 실현손익"),
    )

    fig.add_trace(
        go.Scatter(x=result['날짜'], y=result['총자산'],
                   name='총자산', fill='tozeroy',
                   fillcolor='rgba(0,100,200,0.1)',
                   line=dict(color='royalblue', width=1.5)),
        row=1, col=1
    )

    # 모드별 색상
    ag_mask = result['모드'] == 'AG'
    sf_mask = result['모드'] == 'SF'

    fig.add_trace(
        go.Scatter(x=result.loc[ag_mask, '날짜'], y=result.loc[ag_mask, '종가'],
                   mode='markers', name='종가 (AG)',
                   marker=dict(color='red', size=3)),
        row=2, col=1
    )
    fig.add_trace(
        go.Scatter(x=result.loc[sf_mask, '날짜'], y=result.loc[sf_mask, '종가'],
                   mode='markers', name='종가 (SF)',
                   marker=dict(color='blue', size=3)),
        row=2, col=1
    )

    # 일별 실현손익
    daily_pnl = result[result['당일실현'] != 0]
    colors = ['green' if x > 0 else 'red' for x in daily_pnl['당일실현']]
    fig.add_trace(
        go.Bar(x=daily_pnl['날짜'], y=daily_pnl['당일실현'],
               name='일별 실현', marker_color=colors),
        row=3, col=1
    )

    fig.update_layout(height=800, showlegend=True, legend=dict(orientation='h', y=1.02))
    fig.update_xaxes(rangeslider_visible=False)
    st.plotly_chart(fig, use_container_width=True)

    # ── 매매 기록 테이블 ──
    with st.expander("매매 기록 상세"):
        display_cols = ['날짜', '종가', '모드', '매수주문가', '매수체결', '수량',
                       '매도목표가', '보유포지션수', '1회시드', '투자금',
                       '예수금', '총자산', '당일실현', '누적실현', '갱신', '누적매도']
        df_display = result[display_cols].copy()
        df_display['날짜'] = df_display['날짜'].dt.strftime('%Y-%m-%d')
        st.dataframe(df_display, use_container_width=True, height=500)

    # ── 매도 내역 ──
    if all_sells:
        with st.expander(f"매도 내역 ({len(all_sells)}건)"):
            sells_df = pd.DataFrame(all_sells)
            sells_df['buy_date'] = sells_df['buy_date'].dt.strftime('%Y-%m-%d')
            sells_df['sell_date'] = sells_df['sell_date'].dt.strftime('%Y-%m-%d')
            sells_df['stop_date'] = sells_df['stop_date'].dt.strftime('%Y-%m-%d')
            sells_df.columns = ['매수일', '매수가', '수량', '매도일', '매도가',
                                '매도목표', '손절일', '손익', '모드']
            sells_df['손익'] = sells_df['손익'].round(2)
            st.dataframe(sells_df, use_container_width=True, height=400)


# ══════════════════════════════════════════════
# 최적화 — 유틸 함수
# ══════════════════════════════════════════════

_NUM_WORKERS = max(1, (os.cpu_count() or 1) - 1)


def _run_opt_bt(sd, sh, sb, ss, ad, ah, ab, as_, pcr_val, lcr_val,
                soxl, mode_series, s_date, e_date, cap, fee_r, renew_p):
    """최적화용 백테스트 1회 실행 → dict or None."""
    params = DSSParams(
        sf_divisions=sd, sf_max_hold=sh,
        sf_buy_pct=sb / 100, sf_sell_pct=ss / 100,
        ag_divisions=ad, ag_max_hold=ah,
        ag_buy_pct=ab / 100, ag_sell_pct=as_ / 100,
        initial_capital=cap,
        fee_rate=fee_r / 100,
        renewal_period=renew_p,
        pcr=pcr_val / 100, lcr=lcr_val / 100,
    )
    r = run_backtest_fast(params, soxl, mode_series, s_date, e_date)
    if r is None:
        return None

    final = r['final_asset']
    days = r['total_days']
    mdd_val = r['mdd'] * 100
    ret = (final / cap - 1) * 100
    yrs = days / 252
    cagr_val = ((final / cap) ** (1 / yrs) - 1) * 100 if yrs > 0 else 0
    calmar = abs(cagr_val / mdd_val) if mdd_val != 0 else 0

    return {
        'SF분할': sd, 'SF보유': sh, 'SF매수%': sb, 'SF매도%': ss,
        'AG분할': ad, 'AG보유': ah, 'AG매수%': ab, 'AG매도%': as_,
        'PCR%': pcr_val, 'LCR%': lcr_val,
        '최종자산($)': round(final, 2),
        '수익률(%)': round(ret, 2),
        'CAGR(%)': round(cagr_val, 2),
        'MDD(%)': round(mdd_val, 2),
        'Sharpe': 0,
        'Calmar': round(calmar, 3),
    }


def _run_parallel_opt(combos, soxl, mode_series, s_date, e_date, cap, progress_bar):
    """멀티프로세싱으로 백테스트 병렬 실행."""
    import pickle, tempfile

    tmp_path = os.path.join(tempfile.gettempdir(), f'dss_opt_{os.getpid()}.pkl')
    with open(tmp_path, 'wb') as f:
        pickle.dump((soxl, mode_series), f, protocol=pickle.HIGHEST_PROTOCOL)

    total = len(combos)
    rows = []

    from opt_worker_dss import init_worker, run_single_bt

    try:
        chunksize = max(1, total // _NUM_WORKERS)

        with mp.Pool(
            processes=_NUM_WORKERS,
            initializer=init_worker,
            initargs=(tmp_path,),
        ) as pool:
            count = 0
            for r in pool.imap_unordered(run_single_bt, combos, chunksize=chunksize):
                if r is not None:
                    rows.append(r)
                count += 1
                if count % max(1, total // 50) == 0:
                    progress_bar.progress(
                        min(count / total, 1.0),
                        text=f"실행 중... {count:,} / {total:,}  ({_NUM_WORKERS}코어 병렬)"
                    )
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    progress_bar.progress(1.0, text="완료!")
    return rows


def _show_opt_results(res_df, sort_col, key_sfx):
    """최적화 결과 공통 표시 (Top 20, 산점도, CSV)."""
    st.subheader(f"상위 20개 결과 ({sort_col} 기준)")
    st.dataframe(res_df.head(20).style.format({
        'SF매수%': '{:.1f}', 'SF매도%': '{:.1f}',
        'AG매수%': '{:.1f}', 'AG매도%': '{:.1f}',
        'CAGR(%)': '{:.2f}%', 'MDD(%)': '{:.2f}%',
        'Calmar': '{:.3f}', 'Sharpe': '{:.3f}',
        '수익률(%)': '{:.2f}%', '최종자산($)': '${:,.0f}',
    }), use_container_width=True)

    best = res_df.iloc[0]
    st.success(
        f"**최적 파라미터:** "
        f"안전(분할={int(best['SF분할'])}, 보유={int(best['SF보유'])}, "
        f"매수={best['SF매수%']:.1f}%, 매도={best['SF매도%']:.1f}%) | "
        f"공세(분할={int(best['AG분할'])}, 보유={int(best['AG보유'])}, "
        f"매수={best['AG매수%']:.1f}%, 매도={best['AG매도%']:.1f}%) | "
        f"PCR={int(best['PCR%'])}%, LCR={int(best['LCR%'])}% → "
        f"**CAGR={best['CAGR(%)']:.1f}% | MDD={best['MDD(%)']:.1f}% | "
        f"Calmar={best['Calmar']:.3f} | Sharpe={best['Sharpe']:.3f}**"
    )

    st.subheader("리스크-수익 분포 (CAGR vs MDD)")
    fig_sc = px.scatter(
        res_df, x="MDD(%)", y="CAGR(%)", color=sort_col,
        hover_data=['SF분할', 'SF보유', 'SF매수%', 'SF매도%',
                    'AG분할', 'AG보유', 'AG매수%', 'AG매도%',
                    'PCR%', 'LCR%', 'Calmar'],
        color_continuous_scale="RdYlGn",
    )
    fig_sc.update_layout(height=450)
    st.plotly_chart(fig_sc, use_container_width=True)

    opt_csv = res_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("CSV 다운로드", data=opt_csv,
                       file_name=f"opt_dss_{key_sfx}.csv", mime="text/csv",
                       key=f"dss_dl_opt_{key_sfx}")


# ══════════════════════════════════════════════
# 탭 2: 최적화
# ══════════════════════════════════════════════

def render_optimization_tab(params):
    """파라미터 최적화 탭 — 4가지 방식."""
    p = params
    st.subheader("🔍 파라미터 최적화")

    opt_method = st.radio(
        "최적화 방식",
        ["📊 그리드 탐색", "🎲 랜덤 탐색", "📈 워크포워드", "🧠 베이지안"],
        horizontal=True, key="dss_opt_method",
    )
    _method_desc = {
        "📊 그리드 탐색": "모든 파라미터 조합을 완전 탐색합니다. 조합이 적을 때 가장 정확합니다.",
        "🎲 랜덤 탐색": "무작위로 N개 조합을 샘플링합니다. 탐색 공간이 클 때 빠르게 좋은 파라미터를 찾습니다.",
        "📈 워크포워드": "전체 기간을 IS(최적화)·OOS(검증) 윈도우로 분할해 과적합을 방지합니다.",
        "🧠 베이지안": "Optuna TPE 알고리즘으로 스마트하게 탐색합니다. 적은 시도로 최적값에 빠르게 수렴합니다.",
    }
    st.caption(_method_desc[opt_method])

    # ── 공통 파라미터 범위 설정 ──
    with st.expander("파라미터 범위 설정", expanded=True):
        oc1, oc2 = st.columns(2)

        with oc1:
            st.markdown("**안전모드 (SF)**")
            sf_dv_range = st.slider("분할수", 1, 20, (5, 9), key="dss_o_sf_dv")
            sf_dv_step  = st.number_input("분할수 간격", min_value=1, max_value=10, value=1, key="dss_o_sf_dv_stp")
            sf_hd_range = st.slider("최대보유기간", 5, 60, (20, 40), key="dss_o_sf_hd")
            sf_hd_step  = st.number_input("보유기간 간격", min_value=1, max_value=20, value=5, key="dss_o_sf_hd_stp")
            sf_by_range = st.slider("매수조건 (%)", 0.5, 15.0, (2.0, 5.0), step=0.1, key="dss_o_sf_by")
            sf_by_step  = st.number_input("매수조건 간격", value=0.5, min_value=0.1, step=0.1, format="%.1f", key="dss_o_sf_by_stp")
            sf_sl_range = st.slider("매도조건 (%)", 0.1, 10.0, (0.1, 1.0), step=0.1, key="dss_o_sf_sl")
            sf_sl_step  = st.number_input("매도조건 간격", value=0.2, min_value=0.1, step=0.1, format="%.1f", key="dss_o_sf_sl_stp")

        with oc2:
            st.markdown("**공세모드 (AG)**")
            ag_dv_range = st.slider("분할수", 1, 20, (5, 9), key="dss_o_ag_dv")
            ag_dv_step  = st.number_input("분할수 간격", min_value=1, max_value=10, value=1, key="dss_o_ag_dv_stp")
            ag_hd_range = st.slider("최대보유기간", 1, 30, (5, 10), key="dss_o_ag_hd")
            ag_hd_step  = st.number_input("보유기간 간격", min_value=1, max_value=10, value=1, key="dss_o_ag_hd_stp")
            ag_by_range = st.slider("매수조건 (%)", 0.5, 15.0, (3.0, 7.0), step=0.1, key="dss_o_ag_by")
            ag_by_step  = st.number_input("매수조건 간격", value=0.5, min_value=0.1, step=0.1, format="%.1f", key="dss_o_ag_by_stp")
            ag_sl_range = st.slider("매도조건 (%)", 0.1, 10.0, (1.5, 4.0), step=0.1, key="dss_o_ag_sl")
            ag_sl_step  = st.number_input("매도조건 간격", value=0.5, min_value=0.1, step=0.1, format="%.1f", key="dss_o_ag_sl_stp")

        st.markdown("**공통**")
        cc1, cc2 = st.columns(2)
        with cc1:
            pcr_range = st.slider("이익복리율 (PCR %)", 0, 100, (60, 90), step=5, key="dss_o_pcr")
            pcr_step  = st.number_input("PCR 간격", min_value=5, max_value=50, value=10, step=5, key="dss_o_pcr_stp")
        with cc2:
            lcr_range = st.slider("손실복리율 (LCR %)", 0, 100, (20, 50), step=5, key="dss_o_lcr")
            lcr_step  = st.number_input("LCR 간격", min_value=5, max_value=50, value=10, step=5, key="dss_o_lcr_stp")

        metric_key = st.selectbox("최적화 기준 지표", [
            "Calmar Ratio (CAGR / MDD)",
            "CAGR (%)",
            "총수익률 (%)",
            "MDD 최소화 (작을수록 좋음)",
            "Sharpe Ratio",
        ], key="dss_opt_metric")

    # 파라미터 리스트 생성
    sf_dv_min, sf_dv_max = sf_dv_range
    sf_hd_min, sf_hd_max = sf_hd_range
    sf_by_min, sf_by_max = sf_by_range
    sf_sl_min, sf_sl_max = sf_sl_range
    ag_dv_min, ag_dv_max = ag_dv_range
    ag_hd_min, ag_hd_max = ag_hd_range
    ag_by_min, ag_by_max = ag_by_range
    ag_sl_min, ag_sl_max = ag_sl_range
    pcr_min, pcr_max = pcr_range
    lcr_min, lcr_max = lcr_range

    _sf_dvs = list(range(sf_dv_min, sf_dv_max + 1, sf_dv_step))
    _sf_hds = list(range(sf_hd_min, sf_hd_max + 1, sf_hd_step))
    _sf_bys = np.round(np.arange(sf_by_min, sf_by_max + sf_by_step * 0.5, sf_by_step), 4).tolist()
    _sf_sls = np.round(np.arange(sf_sl_min, sf_sl_max + sf_sl_step * 0.5, sf_sl_step), 4).tolist()
    _ag_dvs = list(range(ag_dv_min, ag_dv_max + 1, ag_dv_step))
    _ag_hds = list(range(ag_hd_min, ag_hd_max + 1, ag_hd_step))
    _ag_bys = np.round(np.arange(ag_by_min, ag_by_max + ag_by_step * 0.5, ag_by_step), 4).tolist()
    _ag_sls = np.round(np.arange(ag_sl_min, ag_sl_max + ag_sl_step * 0.5, ag_sl_step), 4).tolist()
    _pcrs = list(range(pcr_min, pcr_max + 1, pcr_step))
    _lcrs = list(range(lcr_min, lcr_max + 1, lcr_step))
    for _lst in [_sf_dvs, _sf_hds, _sf_bys, _sf_sls, _ag_dvs, _ag_hds, _ag_bys, _ag_sls, _pcrs, _lcrs]:
        if not _lst:
            _lst.append(_lst[0] if _lst else 1)

    n_total = (len(_sf_dvs) * len(_sf_hds) * len(_sf_bys) * len(_sf_sls) *
               len(_ag_dvs) * len(_ag_hds) * len(_ag_bys) * len(_ag_sls) *
               len(_pcrs) * len(_lcrs))

    _fee_r = p["fee_rate"]
    _renew_p = p["renewal_period"]
    _start_date = str(p["start_date"])
    _end_date = str(p["end_date"])
    _cap = float(p["initial_capital"])

    if "Calmar" in metric_key:   _sort_col, _sort_asc = "Calmar",    False
    elif "CAGR" in metric_key:   _sort_col, _sort_asc = "CAGR(%)",   False
    elif "총수익률" in metric_key: _sort_col, _sort_asc = "수익률(%)", False
    elif "Sharpe" in metric_key: _sort_col, _sort_asc = "Sharpe",    False
    else:                         _sort_col, _sort_asc = "MDD(%)",    False

    # ── ① 그리드 탐색 ──
    if opt_method == "📊 그리드 탐색":
        info_msg = (f"예상 조합 수: **{n_total:,}개** "
                    f"(SF {len(_sf_dvs)}×{len(_sf_hds)}×{len(_sf_bys)}×{len(_sf_sls)} "
                    f"× AG {len(_ag_dvs)}×{len(_ag_hds)}×{len(_ag_bys)}×{len(_ag_sls)} "
                    f"× PCR {len(_pcrs)} × LCR {len(_lcrs)})")
        if n_total > 50000:
            st.error(info_msg + "  \n조합이 50,000개를 초과합니다.")
        elif n_total > 10000:
            st.warning(info_msg + "  \n조합이 많아 상당한 시간이 걸릴 수 있습니다.")
        elif n_total > 3000:
            st.warning(info_msg + "  \n조합이 많아 다소 시간이 걸릴 수 있습니다.")
        else:
            st.info(info_msg)

        if st.button("▶ 그리드 탐색 실행", type="primary", key="dss_run_grid",
                     disabled=(n_total == 0)):
            try:
                soxl = get_soxl_data()
                mode_series = get_mode_series(len(get_qqq_data()))
            except Exception as _e:
                st.error(f"⚠️ 데이터 로드 실패: {_e}")
                return

            st.caption(f"💻 {_NUM_WORKERS}코어 병렬 처리")
            progress = st.progress(0.0, text="그리드 탐색 실행 중...")

            combos = [
                (sd, sh, sb, ss, ad, ah, ab, as_, pcr_v, lcr_v,
                 _start_date, _end_date, _cap,
                 _fee_r, _renew_p)
                for sd, sh, sb, ss, ad, ah, ab, as_, pcr_v, lcr_v
                in itertools.product(
                    _sf_dvs, _sf_hds, _sf_bys, _sf_sls,
                    _ag_dvs, _ag_hds, _ag_bys, _ag_sls,
                    _pcrs, _lcrs)
            ]

            rows = _run_parallel_opt(combos, soxl, mode_series,
                                      _start_date, _end_date, _cap, progress)
            if not rows:
                st.error("유효한 결과가 없습니다.")
            else:
                res_df = pd.DataFrame(rows).sort_values(_sort_col, ascending=_sort_asc).reset_index(drop=True)
                _show_opt_results(res_df, _sort_col, "grid")

    # ── ② 랜덤 탐색 ──
    elif opt_method == "🎲 랜덤 탐색":
        n_samples = st.number_input("샘플 수", min_value=50, max_value=5000,
                                    value=500, step=50, key="dss_n_samples")
        st.info(f"랜덤으로 **{n_samples:,}개** 조합을 샘플링합니다. "
                f"(그리드 전체 {n_total:,}개 중 무작위 선택)")

        if st.button("▶ 랜덤 탐색 실행", type="primary", key="dss_run_random"):
            try:
                soxl = get_soxl_data()
                mode_series = get_mode_series(len(get_qqq_data()))
            except Exception as _e:
                st.error(f"⚠️ 데이터 로드 실패: {_e}")
                return

            random.seed(42)
            combos = [
                (random.choice(_sf_dvs), random.choice(_sf_hds),
                 round(random.uniform(sf_by_min, sf_by_max), 2),
                 round(random.uniform(sf_sl_min, sf_sl_max), 2),
                 random.choice(_ag_dvs), random.choice(_ag_hds),
                 round(random.uniform(ag_by_min, ag_by_max), 2),
                 round(random.uniform(ag_sl_min, ag_sl_max), 2),
                 random.choice(_pcrs), random.choice(_lcrs),
                 _start_date, _end_date, _cap,
                 _fee_r, _renew_p)
                for _ in range(int(n_samples))
            ]

            st.caption(f"💻 {_NUM_WORKERS}코어 병렬 처리")
            progress = st.progress(0.0, text="랜덤 탐색 실행 중...")
            rows = _run_parallel_opt(combos, soxl, mode_series,
                                      _start_date, _end_date, _cap, progress)
            if not rows:
                st.error("유효한 결과가 없습니다.")
            else:
                res_df = pd.DataFrame(rows).sort_values(_sort_col, ascending=_sort_asc).reset_index(drop=True)
                _show_opt_results(res_df, _sort_col, "random")

    # ── ③ 워크포워드 ──
    elif opt_method == "📈 워크포워드":
        wf1, wf2 = st.columns(2)
        is_years  = wf1.number_input("IS(최적화) 기간 (년)", min_value=1, max_value=10, value=3, key="dss_wf_is")
        oos_years = wf2.number_input("OOS(검증) 기간 (년)",  min_value=1, max_value=5,  value=1, key="dss_wf_oos")
        st.info(
            f"IS **{is_years}년** 최적화 → OOS **{oos_years}년** 검증을 슬라이딩 반복합니다.\n\n"
            f"그리드 조합 **{n_total:,}개** × 윈도우 수 만큼 백테스트가 실행됩니다."
        )

        if n_total > 3000:
            st.warning(f"조합이 {n_total:,}개로 많습니다. 범위를 줄이면 더 빠릅니다.")

        if st.button("▶ 워크포워드 실행", type="primary", key="dss_run_wfo"):
            try:
                soxl = get_soxl_data()
                mode_series = get_mode_series(len(get_qqq_data()))
            except Exception as _e:
                st.error(f"⚠️ 데이터 로드 실패: {_e}")
                return

            total_start = pd.Timestamp(_start_date).date()
            total_end   = pd.Timestamp(_end_date).date()

            windows = []
            cur = total_start
            while True:
                is_s  = cur
                is_e  = is_s  + timedelta(days=int(is_years  * 365.25))
                oos_s = is_e
                oos_e = oos_s + timedelta(days=int(oos_years * 365.25))
                if oos_e > total_end:
                    break
                windows.append((is_s, is_e, oos_s, oos_e))
                cur = oos_s

            if not windows:
                st.error("데이터 기간이 너무 짧아 윈도우를 생성할 수 없습니다.")
            else:
                st.info(f"총 **{len(windows)}개** 윈도우 생성됨 | 💻 {_NUM_WORKERS}코어 병렬 처리")
                progress = st.progress(0.0, text="워크포워드 실행 중...")
                wfo_rows = []
                cur_capital = _cap

                for wi, (is_s, is_e, oos_s, oos_e) in enumerate(windows):
                    progress.progress(
                        wi / len(windows),
                        text=f"윈도우 {wi+1}/{len(windows)} IS 최적화 중... ({_NUM_WORKERS}코어)"
                    )

                    # IS 구간 병렬 그리드 탐색
                    is_combos = [
                        (sd, sh, sb, ss, ad, ah, ab, as_, pcr_v, lcr_v,
                         str(is_s), str(is_e), _cap,
                         _fee_r, _renew_p)
                        for sd, sh, sb, ss, ad, ah, ab, as_, pcr_v, lcr_v
                        in itertools.product(
                            _sf_dvs, _sf_hds, _sf_bys, _sf_sls,
                            _ag_dvs, _ag_hds, _ag_bys, _ag_sls,
                            _pcrs, _lcrs)
                    ]

                    import pickle, tempfile
                    from opt_worker_dss import init_worker, run_single_bt

                    tmp_wf = tempfile.NamedTemporaryFile(delete=False, suffix='.pkl')
                    with open(tmp_wf.name, 'wb') as f:
                        pickle.dump((soxl, mode_series), f, protocol=pickle.HIGHEST_PROTOCOL)

                    best_score, best_combo = -999.0, None
                    with ProcessPoolExecutor(
                        max_workers=_NUM_WORKERS,
                        initializer=init_worker,
                        initargs=(tmp_wf.name,),
                    ) as executor:
                        for r in executor.map(run_single_bt, is_combos, chunksize=max(1, len(is_combos) // (_NUM_WORKERS * 4))):
                            if r is not None:
                                if "Calmar" in metric_key:   score = r["Calmar"]
                                elif "CAGR" in metric_key:   score = r["CAGR(%)"]
                                elif "총수익률" in metric_key: score = r["수익률(%)"]
                                elif "Sharpe" in metric_key: score = r["Sharpe"]
                                else:                         score = -abs(r["MDD(%)"])
                                if score > best_score:
                                    best_score = score
                                    best_combo = (r['SF분할'], r['SF보유'], r['SF매수%'], r['SF매도%'],
                                                  r['AG분할'], r['AG보유'], r['AG매수%'], r['AG매도%'],
                                                  r['PCR%'], r['LCR%'])

                    os.unlink(tmp_wf.name)

                    if best_combo is None:
                        continue

                    # OOS 구간은 단일 실행
                    oos_r = _run_opt_bt(*best_combo, soxl, mode_series,
                                        str(oos_s), str(oos_e), cur_capital,
                                        _fee_r, _renew_p)
                    if oos_r is None:
                        continue

                    sd, sh, sb, ss, ad, ah, ab, as_, pcr_v, lcr_v = best_combo
                    wfo_rows.append({
                        '윈도우': wi + 1,
                        'IS 기간': f"{is_s} ~ {is_e}",
                        'OOS 기간': f"{oos_s} ~ {oos_e}",
                        'SF(분할/보유/매수/매도)': f"{sd}/{sh}/{sb}/{ss}",
                        'AG(분할/보유/매수/매도)': f"{ad}/{ah}/{ab}/{as_}",
                        f'IS {_sort_col}': round(best_score, 3),
                        'OOS CAGR(%)': oos_r['CAGR(%)'],
                        'OOS MDD(%)': oos_r['MDD(%)'],
                        'OOS Calmar': oos_r['Calmar'],
                        '시작($)': round(cur_capital, 2),
                        '종료($)': oos_r['최종자산($)'],
                    })
                    cur_capital = oos_r['최종자산($)']

                progress.progress(1.0, text="완료!")
                if not wfo_rows:
                    st.error("유효한 OOS 결과가 없습니다.")
                else:
                    wfo_df = pd.DataFrame(wfo_rows)
                    total_ret = (cur_capital - _cap) / _cap

                    st.subheader("워크포워드 종합 성과")
                    wc1, wc2, wc3, wc4 = st.columns(4)
                    wc1.metric("시작 자본", f"${_cap:,.0f}")
                    wc2.metric("최종 자본 (OOS)", f"${cur_capital:,.0f}")
                    wc3.metric("OOS 총 수익률", f"{total_ret*100:+.2f}%")
                    wc4.metric("윈도우 수", f"{len(wfo_rows)}개")

                    st.subheader("윈도우별 결과")
                    st.dataframe(wfo_df.style.format({
                        'OOS CAGR(%)': '{:.2f}%', 'OOS MDD(%)': '{:.2f}%',
                        'OOS Calmar': '{:.3f}',
                        '시작($)': '${:,.2f}', '종료($)': '${:,.2f}',
                    }), use_container_width=True)

                    fig_wfo = px.bar(wfo_df, x="윈도우", y="OOS CAGR(%)",
                                     color="OOS CAGR(%)", color_continuous_scale="RdYlGn",
                                     text_auto=".1f", title="윈도우별 OOS CAGR (%)")
                    fig_wfo.add_hline(y=0, line_dash="dash", line_color="gray")
                    fig_wfo.update_layout(height=400)
                    st.plotly_chart(fig_wfo, use_container_width=True)

                    fig_cap = px.line(wfo_df, x="윈도우", y="종료($)",
                                      title="OOS 자본 변화", markers=True)
                    fig_cap.update_layout(height=380)
                    st.plotly_chart(fig_cap, use_container_width=True)

                    wfo_csv = wfo_df.to_csv(index=False).encode("utf-8-sig")
                    st.download_button("CSV 다운로드", data=wfo_csv,
                                       file_name="wfo_dss.csv", mime="text/csv", key="dss_dl_wfo")

    # ── ④ 베이지안 (Optuna) ──
    elif opt_method == "🧠 베이지안":
        try:
            import optuna as _optuna
            _optuna_ok = True
        except ImportError:
            _optuna_ok = False

        if not _optuna_ok:
            st.error("`optuna` 패키지가 설치되지 않았습니다. `pip install optuna` 실행 후 재시작하세요.")
        else:
            bc1, _ = st.columns(2)
            n_trials = bc1.number_input("탐색 횟수 (trials)", min_value=50,
                                        max_value=2000, value=300, step=50, key="dss_n_trials")
            st.info(f"Optuna TPE 알고리즘으로 **{n_trials}회** 스마트 탐색합니다.\n\n"
                    f"그리드 탐색({n_total:,}개) 대비 적은 시도로 최적값에 근접합니다.")

            if st.button("▶ 베이지안 최적화 실행", type="primary", key="dss_run_bayes"):
                try:
                    soxl = get_soxl_data()
                    mode_series = get_mode_series(len(get_qqq_data()))
                except Exception as _e:
                    st.error(f"⚠️ 데이터 로드 실패: {_e}")
                    return

                _optuna.logging.set_verbosity(_optuna.logging.WARNING)
                progress = st.progress(0.0, text="베이지안 탐색 실행 중...")
                trial_rows = []
                _tc = [0]

                def _objective(trial):
                    sd = trial.suggest_int("SF분할", sf_dv_min, sf_dv_max)
                    sh = trial.suggest_int("SF보유", sf_hd_min, sf_hd_max)
                    sb = trial.suggest_float("SF매수%", sf_by_min, sf_by_max)
                    ss = trial.suggest_float("SF매도%", sf_sl_min, sf_sl_max)
                    ad = trial.suggest_int("AG분할", ag_dv_min, ag_dv_max)
                    ah = trial.suggest_int("AG보유", ag_hd_min, ag_hd_max)
                    ab = trial.suggest_float("AG매수%", ag_by_min, ag_by_max)
                    as_ = trial.suggest_float("AG매도%", ag_sl_min, ag_sl_max)
                    pcr_v = trial.suggest_int("PCR%", pcr_min, pcr_max, step=pcr_step) if pcr_min != pcr_max else pcr_min
                    lcr_v = trial.suggest_int("LCR%", lcr_min, lcr_max, step=lcr_step) if lcr_min != lcr_max else lcr_min

                    r = _run_opt_bt(sd, sh, round(sb, 2), round(ss, 2),
                                    ad, ah, round(ab, 2), round(as_, 2), pcr_v, lcr_v,
                                    soxl, mode_series, _start_date, _end_date,
                                    _cap, _fee_r, _renew_p)
                    if r is None:
                        return -999.0

                    trial_rows.append(r)
                    _tc[0] += 1
                    if _tc[0] % max(1, int(n_trials) // 50) == 0:
                        progress.progress(min(_tc[0] / int(n_trials), 1.0),
                                          text=f"베이지안 탐색 중... {_tc[0]:,} / {int(n_trials):,}")

                    if "Calmar" in metric_key:   return r["Calmar"]
                    elif "CAGR" in metric_key:   return r["CAGR(%)"]
                    elif "총수익률" in metric_key: return r["수익률(%)"]
                    elif "Sharpe" in metric_key: return r["Sharpe"]
                    else:                         return -abs(r["MDD(%)"])

                study = _optuna.create_study(
                    direction="maximize",
                    sampler=_optuna.samplers.TPESampler(seed=42)
                )
                study.optimize(_objective, n_trials=int(n_trials))
                progress.progress(1.0, text="완료!")

                if not trial_rows:
                    st.error("유효한 결과가 없습니다.")
                else:
                    res_df = pd.DataFrame(trial_rows).sort_values(
                        _sort_col, ascending=_sort_asc
                    ).reset_index(drop=True)

                    best = study.best_params
                    st.success(
                        f"**최적 파라미터:** "
                        f"SF(분할={best.get('SF분할')}, 보유={best.get('SF보유')}, "
                        f"매수={best.get('SF매수%', 0):.1f}%, 매도={best.get('SF매도%', 0):.1f}%) | "
                        f"AG(분할={best.get('AG분할')}, 보유={best.get('AG보유')}, "
                        f"매수={best.get('AG매수%', 0):.1f}%, 매도={best.get('AG매도%', 0):.1f}%) | "
                        f"PCR={best.get('PCR%', pcr_min)}%, LCR={best.get('LCR%', lcr_min)}%"
                    )
                    _show_opt_results(res_df, _sort_col, "bayes")

                    st.subheader("탐색 수렴 과정")
                    _vals = [t.value for t in study.trials if t.value is not None and t.value > -900]
                    _best_cur = [max(_vals[:i+1]) for i in range(len(_vals))]
                    fig_conv = px.line(y=_best_cur,
                                       labels={"y": f"Best {_sort_col}", "index": "Trial"},
                                       title="베이지안 최적화 수렴 곡선")
                    fig_conv.update_layout(height=380)
                    st.plotly_chart(fig_conv, use_container_width=True)


# ══════════════════════════════════════════════
# ══════════════════════════════════════════════
# 탭 3: 주문표 & 계좌관리
# ══════════════════════════════════════════════


def _build_os_result_from_backtest(bt_df, os_params, os_capital, qqq,
                                    adj_history=None):
    """백테스트 결과 DataFrame → 주문표 표시용 result dict.

    adj_history: 자본 조정 이력 리스트. 있으면 last_date 이하 날짜의 조정
    금액을 cash/total_asset에 합산 (엔진이 조정을 모델링하지 않으므로 보정)."""
    last = bt_df.iloc[-1]
    prev_close = float(last['종가'])
    last_date = pd.Timestamp(last['날짜'])
    last_mode = last['모드']

    n_pos = int(last['보유포지션수'])
    total_asset = float(last['총자산'])
    cash = float(last['예수금'])
    capital = float(last['투자금'])
    holding_value = float(last['평가금'])
    eval_pnl = float(last['평가손익'])
    cum_realized = float(last['누적실현'])
    sell_count = int(last['누적매도'])

    # 자본 조정 반영 (last_date 이하 날짜)
    adj_applied = 0.0
    if adj_history:
        for _item in adj_history:
            try:
                _dt = pd.Timestamp(_item.get("날짜"))
                if _dt <= last_date:
                    adj_applied += float(_item.get("조정금액", 0))
            except Exception:
                continue
    cash += adj_applied
    total_asset += adj_applied
    capital += adj_applied

    if last_mode == "AG":
        cur_divisions = os_params.ag_divisions
        cur_buy_pct = os_params.ag_buy_pct
        cur_sell_pct = os_params.ag_sell_pct
        cur_max_hold = os_params.ag_max_hold
    else:
        cur_divisions = os_params.sf_divisions
        cur_buy_pct = os_params.sf_buy_pct
        cur_sell_pct = os_params.sf_sell_pct
        cur_max_hold = os_params.sf_max_hold

    open_positions = []
    _all_sold = set()
    for _, row in bt_df.iterrows():
        if row['매도내역'] is not None:
            for sr in row['매도내역']:
                _all_sold.add((pd.Timestamp(sr['buy_date']), sr['buy_price'], sr['qty']))
    for _, row in bt_df.iterrows():
        if row['매수체결'] is not None and row['수량'] > 0:
            bk = (pd.Timestamp(row['날짜']), float(row['매수체결']), int(row['수량']))
            if bk not in _all_sold:
                open_positions.append({
                    'buy_date': pd.Timestamp(row['날짜']),
                    'buy_price': float(row['매수체결']),
                    'qty': int(row['수량']),
                    'sell_target': float(row['매도목표가']) if row['매도목표가'] is not None else None,
                    'stop_date': pd.Timestamp(row['손절예정일']) if row['손절예정일'] is not None else None,
                    'mode': row['모드'],
                })

    next_buy_order = math.floor(prev_close * (1 + cur_buy_pct) * 100) / 100
    seed_per_trade = capital / cur_divisions
    buy_qty_est = int(seed_per_trade / next_buy_order) if next_buy_order > 0 else 0

    weekly_rsi_df = build_weekly_rsi_series(qqq)
    latest_rsi_row = weekly_rsi_df.iloc[-1] if len(weekly_rsi_df) > 0 else None
    prev_rsi_row = weekly_rsi_df.iloc[-2] if len(weekly_rsi_df) > 1 else None

    return {
        "bt_df": bt_df,
        "prev_close": prev_close,
        "last_date": last_date,
        "last_mode": last_mode,
        "n_pos": n_pos,
        "total_asset": total_asset,
        "cash": cash,
        "capital": capital,
        "holding_value": holding_value,
        "eval_pnl": eval_pnl,
        "cum_realized": cum_realized,
        "sell_count": sell_count,
        "open_positions": open_positions,
        "next_buy_order": next_buy_order,
        "seed_per_trade": seed_per_trade,
        "buy_qty_est": buy_qty_est,
        "cur_divisions": cur_divisions,
        "cur_buy_pct": cur_buy_pct,
        "cur_sell_pct": cur_sell_pct,
        "cur_max_hold": cur_max_hold,
        "latest_rsi": float(latest_rsi_row['rsi']) if latest_rsi_row is not None else None,
        "prev_rsi": float(prev_rsi_row['rsi']) if prev_rsi_row is not None else None,
        "latest_rsi_date": str(latest_rsi_row['week_end'].date()) if latest_rsi_row is not None else None,
        "os_capital": float(os_capital),
        "adj_applied": float(adj_applied),
    }


def _build_os_result_fallback(os_params, os_capital, adj_history=None):
    """백테스트 결과 없을 때 (신규 계좌/오늘 시작) → 최신 시장 데이터로 주문표 생성.
    adj_history가 있으면 last_date 이하 날짜의 조정금액을 자본에 합산."""
    try:
        soxl = get_soxl_data()
        qqq = get_qqq_data()
    except Exception:
        return None

    if soxl is None or soxl.empty or qqq is None or qqq.empty:
        return None

    mode_series_df = get_mode_series(len(qqq))

    prev_close = float(soxl.iloc[-1]['Close'])
    last_date = soxl.index[-1]

    mode_map = get_week_mode_map(mode_series_df, soxl.index)
    last_mode = mode_map.get(last_date, "AG")

    if last_mode == "AG":
        cur_divisions = os_params.ag_divisions
        cur_buy_pct = os_params.ag_buy_pct
        cur_sell_pct = os_params.ag_sell_pct
        cur_max_hold = os_params.ag_max_hold
    else:
        cur_divisions = os_params.sf_divisions
        cur_buy_pct = os_params.sf_buy_pct
        cur_sell_pct = os_params.sf_sell_pct
        cur_max_hold = os_params.sf_max_hold

    capital = float(os_capital)
    # 자본 조정 반영
    adj_applied = 0.0
    if adj_history:
        for _item in adj_history:
            try:
                _dt = pd.Timestamp(_item.get("날짜"))
                if _dt <= last_date:
                    adj_applied += float(_item.get("조정금액", 0))
            except Exception:
                continue
    capital += adj_applied
    next_buy_order = math.floor(prev_close * (1 + cur_buy_pct) * 100) / 100
    seed_per_trade = capital / cur_divisions
    buy_qty_est = int(seed_per_trade / next_buy_order) if next_buy_order > 0 else 0

    weekly_rsi_df = build_weekly_rsi_series(qqq)
    latest_rsi_row = weekly_rsi_df.iloc[-1] if len(weekly_rsi_df) > 0 else None
    prev_rsi_row = weekly_rsi_df.iloc[-2] if len(weekly_rsi_df) > 1 else None

    return {
        "bt_df": pd.DataFrame(),
        "prev_close": prev_close,
        "last_date": last_date,
        "last_mode": last_mode,
        "n_pos": 0,
        "total_asset": capital,
        "cash": capital,
        "capital": capital,
        "holding_value": 0,
        "eval_pnl": 0,
        "cum_realized": 0,
        "sell_count": 0,
        "open_positions": [],
        "next_buy_order": next_buy_order,
        "seed_per_trade": seed_per_trade,
        "buy_qty_est": buy_qty_est,
        "cur_divisions": cur_divisions,
        "cur_buy_pct": cur_buy_pct,
        "cur_sell_pct": cur_sell_pct,
        "cur_max_hold": cur_max_hold,
        "latest_rsi": float(latest_rsi_row['rsi']) if latest_rsi_row is not None else None,
        "prev_rsi": float(prev_rsi_row['rsi']) if prev_rsi_row is not None else None,
        "latest_rsi_date": str(latest_rsi_row['week_end'].date()) if latest_rsi_row is not None else None,
        "os_capital": float(os_capital),
        "adj_applied": float(adj_applied),
    }


_DSS_PRESETS = [
    {"label": "🚀 공격형", "sf_div": 7, "sf_hold": 38, "sf_buy": 3.3, "sf_sell": 1.8,
     "ag_div": 7, "ag_hold": 8, "ag_buy": 3.15, "ag_sell": 3.75,
     "pcr": 75, "lcr": 20, "renewal_period": 10, "fee_rate": 0.04,
     "help": "공격형 파라미터\nCAGR 66.8% | MDD -45% | Calmar 1.485"},
    {"label": "⚖️ 균형형", "sf_div": 7, "sf_hold": 35, "sf_buy": 3.5, "sf_sell": 1.8,
     "ag_div": 8, "ag_hold": 7, "ag_buy": 3.6, "ag_sell": 6.0,
     "pcr": 72, "lcr": 21, "renewal_period": 10, "fee_rate": 0.04,
     "help": "균형형 파라미터\nCAGR 63.24% | MDD -43.77% | Calmar 1.445"},
    {"label": "🛡️ 안정형 ⭐", "sf_div": 7, "sf_hold": 36, "sf_buy": 4.91, "sf_sell": 0.9,
     "ag_div": 7, "ag_hold": 8, "ag_buy": 2.77, "ag_sell": 3.06,
     "pcr": 70, "lcr": 20, "renewal_period": 10, "fee_rate": 0.04,
     "help": "안정형 파라미터\nCAGR 58.07% | MDD -40.69% | Calmar 1.427"},
]

_DSS_PARAM_KEYS = ["sf_div","sf_hold","sf_buy","sf_sell",
                   "ag_div","ag_hold","ag_buy","ag_sell",
                   "pcr","lcr","renewal_period","fee_rate"]

_DSS_DEFAULT_PARAMS = _DSS_PRESETS[2]  # 안정형을 기본값으로


def _render_dss_account(acct_name, acct_data, cfg, p, idx):
    """개별 계좌 탭 렌더링."""
    sfx = f"a{idx}"

    # ── 계좌별 파라미터 로드 ──
    _ap = acct_data.get("params", {})
    cur_sf_div  = _ap.get("sf_div",  _DSS_DEFAULT_PARAMS["sf_div"])
    cur_sf_hold = _ap.get("sf_hold", _DSS_DEFAULT_PARAMS["sf_hold"])
    cur_sf_buy  = _ap.get("sf_buy",  _DSS_DEFAULT_PARAMS["sf_buy"])
    cur_sf_sell = _ap.get("sf_sell", _DSS_DEFAULT_PARAMS["sf_sell"])
    cur_ag_div  = _ap.get("ag_div",  _DSS_DEFAULT_PARAMS["ag_div"])
    cur_ag_hold = _ap.get("ag_hold", _DSS_DEFAULT_PARAMS["ag_hold"])
    cur_ag_buy  = _ap.get("ag_buy",  _DSS_DEFAULT_PARAMS["ag_buy"])
    cur_ag_sell = _ap.get("ag_sell", _DSS_DEFAULT_PARAMS["ag_sell"])
    cur_pcr     = _ap.get("pcr",     _DSS_DEFAULT_PARAMS["pcr"])
    cur_lcr     = _ap.get("lcr",     _DSS_DEFAULT_PARAMS["lcr"])
    cur_renew   = _ap.get("renewal_period", _DSS_DEFAULT_PARAMS["renewal_period"])
    cur_fee     = _ap.get("fee_rate", _DSS_DEFAULT_PARAMS["fee_rate"])

    # ── 파라미터 테이블 ──
    with st.container(border=True):
        st.markdown(
            f"""
            <table style="width:100%; border-collapse:collapse; font-size:14px;">
            <tr style="border-bottom:1px solid #eee;">
                <th style="text-align:left; padding:6px 10px; color:#888; font-weight:500; width:12%;">모드</th>
                <th style="text-align:center; padding:6px 8px; color:#888; font-weight:500;">분할수</th>
                <th style="text-align:center; padding:6px 8px; color:#888; font-weight:500;">보유기간</th>
                <th style="text-align:center; padding:6px 8px; color:#888; font-weight:500;">매수조건</th>
                <th style="text-align:center; padding:6px 8px; color:#888; font-weight:500;">매도조건</th>
                <th style="text-align:center; padding:6px 8px; color:#888; font-weight:500;">PCR</th>
                <th style="text-align:center; padding:6px 8px; color:#888; font-weight:500;">LCR</th>
                <th style="text-align:center; padding:6px 8px; color:#888; font-weight:500;">갱신주기</th>
                <th style="text-align:center; padding:6px 8px; color:#888; font-weight:500;">수수료</th>
            </tr>
            <tr style="border-bottom:1px solid #f0f0f0;">
                <td style="padding:8px 10px; font-weight:600; color:#1976D2;">🔵 안전(SF)</td>
                <td style="text-align:center; padding:8px;">{cur_sf_div}</td>
                <td style="text-align:center; padding:8px;">{cur_sf_hold}일</td>
                <td style="text-align:center; padding:8px;">{cur_sf_buy}%</td>
                <td style="text-align:center; padding:8px;">{cur_sf_sell}%</td>
                <td style="text-align:center; padding:8px;" rowspan="2">{cur_pcr}%</td>
                <td style="text-align:center; padding:8px;" rowspan="2">{cur_lcr}%</td>
                <td style="text-align:center; padding:8px;" rowspan="2">{cur_renew}일</td>
                <td style="text-align:center; padding:8px;" rowspan="2">{cur_fee}%</td>
            </tr>
            <tr>
                <td style="padding:8px 10px; font-weight:600; color:#2E7D32;">🟢 공세(AG)</td>
                <td style="text-align:center; padding:8px;">{cur_ag_div}</td>
                <td style="text-align:center; padding:8px;">{cur_ag_hold}일</td>
                <td style="text-align:center; padding:8px;">{cur_ag_buy}%</td>
                <td style="text-align:center; padding:8px;">{cur_ag_sell}%</td>
            </tr>
            </table>
            """,
            unsafe_allow_html=True,
        )

        # ── 파라미터 수정 (프리셋 + 에디터 + 저장) ──
        with st.expander("✏️ 파라미터 수정"):
            st.caption("💡 추천 프리셋")
            _pc1, _pc2, _pc3 = st.columns(3)
            for _pi, (_pcol, _pr) in enumerate(zip([_pc1, _pc2, _pc3], _DSS_PRESETS)):
                if _pcol.button(_pr["label"], key=f"dss_{sfx}_preset_{_pi}",
                                help=_pr["help"], use_container_width=True):
                    for _k in _DSS_PARAM_KEYS:
                        st.session_state[f"dss_{sfx}_edit_{_k}"] = _pr[_k]
                    st.rerun()
            st.divider()

            _edit_defaults = {
                "sf_div": cur_sf_div, "sf_hold": cur_sf_hold,
                "sf_buy": float(cur_sf_buy), "sf_sell": float(cur_sf_sell),
                "ag_div": cur_ag_div, "ag_hold": cur_ag_hold,
                "ag_buy": float(cur_ag_buy), "ag_sell": float(cur_ag_sell),
                "pcr": cur_pcr, "lcr": cur_lcr,
                "renewal_period": cur_renew, "fee_rate": float(cur_fee),
            }
            for _k, _v in _edit_defaults.items():
                _skey = f"dss_{sfx}_edit_{_k}"
                if _skey not in st.session_state:
                    st.session_state[_skey] = _v

            st.markdown("**안전모드 (SF)**")
            _es1, _es2, _es3, _es4 = st.columns(4)
            _es1.number_input("SF 분할수",      min_value=1, max_value=50, step=1, key=f"dss_{sfx}_edit_sf_div")
            _es2.number_input("SF 최대보유기간", min_value=1, max_value=100, step=1, key=f"dss_{sfx}_edit_sf_hold")
            _es3.number_input("SF 매수조건(%)", min_value=0.0, max_value=30.0, step=0.01, format="%.2f", key=f"dss_{sfx}_edit_sf_buy")
            _es4.number_input("SF 매도조건(%)", min_value=0.0, max_value=30.0, step=0.01, format="%.2f", key=f"dss_{sfx}_edit_sf_sell")

            st.markdown("**공세모드 (AG)**")
            _ea1, _ea2, _ea3, _ea4 = st.columns(4)
            _ea1.number_input("AG 분할수",      min_value=1, max_value=50, step=1, key=f"dss_{sfx}_edit_ag_div")
            _ea2.number_input("AG 최대보유기간", min_value=1, max_value=100, step=1, key=f"dss_{sfx}_edit_ag_hold")
            _ea3.number_input("AG 매수조건(%)", min_value=0.0, max_value=30.0, step=0.01, format="%.2f", key=f"dss_{sfx}_edit_ag_buy")
            _ea4.number_input("AG 매도조건(%)", min_value=0.0, max_value=30.0, step=0.01, format="%.2f", key=f"dss_{sfx}_edit_ag_sell")

            st.markdown("**공통**")
            _ec1, _ec2, _ec3, _ec4 = st.columns(4)
            _ec1.number_input("이익복리율 PCR(%)", min_value=0, max_value=100, step=1, key=f"dss_{sfx}_edit_pcr")
            _ec2.number_input("손실복리율 LCR(%)", min_value=0, max_value=100, step=1, key=f"dss_{sfx}_edit_lcr")
            _ec3.number_input("투자금갱신주기",     min_value=1, max_value=100, step=1, key=f"dss_{sfx}_edit_renewal_period")
            _ec4.number_input("수수료(%)",         min_value=0.0, max_value=1.0, step=0.001, format="%.3f", key=f"dss_{sfx}_edit_fee_rate")

            if st.button("💾 파라미터 저장", type="primary", key=f"dss_{sfx}_save_params",
                         use_container_width=True):
                _new_params = {}
                for _k in _edit_defaults:
                    _new_params[_k] = st.session_state[f"dss_{sfx}_edit_{_k}"]
                acct_data["params"] = _new_params
                cfg["accounts"][acct_name] = acct_data
                _save_dss_config(cfg)
                for _k in _edit_defaults:
                    st.session_state.pop(f"dss_{sfx}_edit_{_k}", None)
                st.session_state.pop(f"dss_{sfx}_result", None)
                st.success("✅ 파라미터가 저장되었습니다!")
                st.rerun()

    # ── 계좌 관리 (이름변경 / 삭제) ──
    _mgr1, _mgr2, _ = st.columns([1, 1, 4])
    if _mgr1.button(f"✏️ 이름 변경", key=f"dss_{sfx}_rename_btn", type="secondary"):
        st.session_state[f"dss_{sfx}_renaming"] = True
    if _mgr2.button(f"🗑️ 계좌 삭제", key=f"dss_{sfx}_del", type="secondary"):
        st.session_state[f"dss_{sfx}_del_confirm"] = True

    # ── 이름 변경 ──
    if st.session_state.get(f"dss_{sfx}_renaming", False):
        _rn1, _rn2, _rn3, _ = st.columns([2, 1, 1, 3])
        _new_nm = _rn1.text_input("새 계좌 이름", value=acct_name, key=f"dss_{sfx}_new_name")
        if _rn2.button("✅ 변경", key=f"dss_{sfx}_rename_ok", type="primary"):
            _nm = _new_nm.strip()
            accounts = cfg.get("accounts", {})
            if not _nm:
                st.warning("계좌 이름을 입력하세요.")
            elif _nm == acct_name:
                st.session_state[f"dss_{sfx}_renaming"] = False
                st.rerun()
            elif _nm in accounts:
                st.warning(f"'{_nm}' 계좌가 이미 존재합니다.")
            else:
                # config에서 키 변경 (순서 유지)
                new_accounts = {}
                for k, v in accounts.items():
                    new_accounts[_nm if k == acct_name else k] = v
                cfg["accounts"] = new_accounts
                _save_dss_config(cfg)
                # 히스토리 파일 이름도 변경
                _old_hp = _get_history_path(acct_name)
                _new_hp = _get_history_path(_nm)
                if os.path.exists(_old_hp):
                    os.rename(_old_hp, _new_hp)
                st.session_state.pop(f"dss_{sfx}_renaming", None)
                st.session_state.pop(f"dss_{sfx}_result", None)
                st.success(f"✅ '{acct_name}' → '{_nm}' 변경 완료!")
                st.rerun()
        if _rn3.button("❌ 취소", key=f"dss_{sfx}_rename_cancel"):
            st.session_state[f"dss_{sfx}_renaming"] = False
            st.rerun()

    # ── 계좌 삭제 확인 ──
    if st.session_state.get(f"dss_{sfx}_del_confirm", False):
        st.warning(f"⚠️ **{acct_name}** 계좌를 삭제하시겠습니까? 저장된 설정 및 매매 히스토리가 모두 삭제됩니다.")
        _dc1, _dc2, _ = st.columns([1, 1, 4])
        if _dc1.button("✅ 삭제", key=f"dss_{sfx}_del_ok", type="primary"):
            accounts = cfg.get("accounts", {})
            accounts.pop(acct_name, None)
            cfg["accounts"] = accounts
            _save_dss_config(cfg)
            _hp = _get_history_path(acct_name)
            if os.path.exists(_hp):
                os.remove(_hp)
            st.session_state.pop(f"dss_{sfx}_del_confirm", None)
            st.session_state.pop(f"dss_{sfx}_result", None)
            st.rerun()
        if _dc2.button("❌ 취소", key=f"dss_{sfx}_del_cancel"):
            st.session_state[f"dss_{sfx}_del_confirm"] = False
            st.rerun()

    # ── 시작일 / 시작 자본 ──
    _os_default_start = acct_data.get("os_start", "2024-01-01")
    _os_default_capital = acct_data.get("os_capital", 10000.0)
    try:
        _os_start_val = datetime.strptime(str(_os_default_start), "%Y-%m-%d").date()
    except Exception:
        _os_start_val = datetime(2024, 1, 1).date()
    try:
        _os_capital_val = float(_os_default_capital)
    except Exception:
        _os_capital_val = 10000.0

    _oc1, _oc2, _oc3 = st.columns([2, 2, 1])
    os_start = _oc1.date_input("시작일", value=_os_start_val,
                                min_value=datetime(2010, 1, 1).date(),
                                max_value=datetime.today().date(),
                                key=f"dss_{sfx}_start")
    os_capital = _oc2.number_input("시작 자본 ($)", value=_os_capital_val,
                                    step=1000.0, key=f"dss_{sfx}_capital")
    _oc3.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
    if _oc3.button("💾 계좌 저장", key=f"dss_{sfx}_save_acct", use_container_width=True):
        acct_data["os_start"] = str(os_start)
        acct_data["os_capital"] = float(os_capital)
        cfg["accounts"][acct_name] = acct_data
        _save_dss_config(cfg)
        st.success(f"✅ {acct_name} 저장 완료 (시작일: {os_start}, 자본: ${os_capital:,.0f})")
        st.rerun()

    # ── 자본 조정 (증액/감액) ──
    with st.expander("💰 자본 조정 (증액 / 감액)"):
        st.caption("현재 자본금에 추가하거나 차감할 금액을 입력하세요. 날짜를 선택해 과거 항목도 입력 가능합니다.")
        _adj_history = acct_data.get("capital_adj_history", [])
        if not isinstance(_adj_history, list):
            _adj_history = []

        # 현재 총자본 = 시작자본(os_capital, base) + 기존 조정 합계
        _cur_adj_sum = sum(float(it.get("조정금액", 0)) for it in _adj_history)
        _cur_total = _os_capital_val + _cur_adj_sum

        _adj_c1, _adj_c2 = st.columns([2, 1])
        _adj_date = _adj_c1.date_input("적용 날짜", value=datetime.today().date(),
                                        key=f"dss_{sfx}_adj_date",
                                        help="실제 입금/출금이 일어난 날짜")
        _adj_amount = _adj_c1.number_input("조정 금액 ($)", value=0.0, step=500.0,
                                            help="증액: 양수 · 감액: 음수",
                                            key=f"dss_{sfx}_adj_amount")
        _adj_c1.caption(
            f"현재 자본금: **${_cur_total:,.0f}** → 적용 후: **${_cur_total + _adj_amount:,.0f}** "
            f"({'↑' if _adj_amount > 0 else '↓' if _adj_amount < 0 else '='} "
            f"${abs(_adj_amount):,.0f})"
        )
        _adj_memo = _adj_c1.text_input("메모 (선택)", placeholder="예: 3월 추가 입금",
                                        key=f"dss_{sfx}_adj_memo")
        if _adj_c2.button("💰 적용", use_container_width=True,
                          key=f"dss_{sfx}_apply_adj", disabled=(_adj_amount == 0)):
            if _cur_total + _adj_amount <= 0:
                st.error("자본금은 0보다 커야 합니다.")
            else:
                _adj_history.append({
                    "날짜": _adj_date.strftime("%Y-%m-%d"),
                    "조정금액": float(_adj_amount),
                    "누적자본금": 0.0,
                    "메모": _adj_memo or ("증액" if _adj_amount > 0 else "감액"),
                })
                _adj_history, _final_cap = _recalc_adj_history(
                    _adj_history, _os_capital_val)
                # os_capital 은 변경하지 않음 (base 유지)
                acct_data["capital_adj_history"] = _adj_history
                cfg["accounts"][acct_name] = acct_data
                _save_dss_config(cfg)
                st.success(f"✅ {_adj_date} 자본 조정 완료. 현재 자본금: **${_final_cap:,.0f}**")
                st.rerun()

        if _adj_history:
            st.markdown("---")
            st.markdown("**📋 자본 조정 이력** (직접 수정 가능 — 날짜/금액/메모 편집 · 행 삭제)")
            _df_adj_edit = pd.DataFrame(_adj_history)
            _df_adj_edit["날짜"] = pd.to_datetime(_df_adj_edit["날짜"]).dt.date
            _df_adj_edit["조정금액"] = _df_adj_edit["조정금액"].astype(float)
            _edited = st.data_editor(
                _df_adj_edit[["날짜", "조정금액", "메모"]],
                column_config={
                    "날짜": st.column_config.DateColumn("날짜", format="YYYY-MM-DD", required=True),
                    "조정금액": st.column_config.NumberColumn("조정금액 ($)",
                                                              format="$%.0f", required=True,
                                                              help="증액: 양수 · 감액: 음수"),
                    "메모": st.column_config.TextColumn("메모"),
                },
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True,
                key=f"dss_{sfx}_adj_editor",
            )
            # 실시간 미리보기: 누적자본금 재계산
            _preview_list = []
            for _, _r in _edited.iterrows():
                if pd.isna(_r.get("날짜")) or pd.isna(_r.get("조정금액")):
                    continue
                _preview_list.append({
                    "날짜": pd.Timestamp(_r["날짜"]).strftime("%Y-%m-%d"),
                    "조정금액": float(_r["조정금액"]),
                    "누적자본금": 0.0,
                    "메모": str(_r.get("메모") or ""),
                })
            _preview_list, _preview_cap = _recalc_adj_history(
                _preview_list, _os_capital_val)
            if _preview_list:
                _df_preview = pd.DataFrame(_preview_list)
                _df_preview_show = _df_preview.copy()
                _df_preview_show["조정금액"] = _df_preview["조정금액"].apply(
                    lambda x: f"{'↑' if x > 0 else '↓'} ${abs(x):,.0f}")
                _df_preview_show["누적자본금"] = _df_preview["누적자본금"].apply(
                    lambda x: f"${x:,.0f}")
                st.caption(f"📊 **미리보기** (저장 후 반영됩니다) — 최종 자본금: **${_preview_cap:,.0f}**")
                st.dataframe(_df_preview_show[["날짜", "조정금액", "누적자본금", "메모"]],
                             use_container_width=True, hide_index=True)

            if st.button("💾 변경사항 저장", key=f"dss_{sfx}_save_adj_edit",
                         type="primary"):
                if _preview_cap <= 0:
                    st.error(f"현재 자본금이 0 이하가 됩니다 (${_preview_cap:,.0f}). 수정 불가.")
                else:
                    # os_capital 은 변경하지 않음 (base 유지)
                    acct_data["capital_adj_history"] = _preview_list
                    cfg["accounts"][acct_name] = acct_data
                    _save_dss_config(cfg)
                    st.success(f"✅ 이력 업데이트 완료. 현재 자본금: **${_preview_cap:,.0f}**")
                    st.rerun()
        else:
            st.info("아직 자본 조정 이력이 없습니다.")

        # 전체 초기화
        st.markdown("---")
        st.markdown("**🔄 전체 초기화**")
        st.caption("시작일·자본금·조정 이력을 모두 초기화합니다.")
        _rc1, _rc2, _rc3 = st.columns(3)
        _reset_start = _rc1.date_input("새 시작일", value=datetime.today().date(),
                                        key=f"dss_{sfx}_reset_start")
        _reset_capital = _rc2.number_input("새 시작 자본 ($)", value=_os_capital_val,
                                            step=1000.0, key=f"dss_{sfx}_reset_capital")
        if _rc3.button("🔄 초기화", use_container_width=True,
                       key=f"dss_{sfx}_do_reset", type="secondary"):
            st.session_state[f"dss_{sfx}_reset_confirmed"] = True
        if st.session_state.get(f"dss_{sfx}_reset_confirmed", False):
            st.warning(f"⚠️ **정말 초기화하시겠습니까?**  \n"
                       f"시작일: {_reset_start} / 자본금: ${_reset_capital:,.0f} / 조정 이력 전체 삭제")
            _conf1, _conf2 = st.columns(2)
            if _conf1.button("✅ 확인 (초기화)", type="primary", key=f"dss_{sfx}_confirm_reset"):
                acct_data["os_start"] = str(_reset_start)
                acct_data["os_capital"] = float(_reset_capital)
                acct_data["capital_adj_history"] = []
                cfg["accounts"][acct_name] = acct_data
                _save_dss_config(cfg)
                st.session_state[f"dss_{sfx}_reset_confirmed"] = False
                st.session_state.pop(f"dss_{sfx}_result", None)
                st.success("✅ 초기화 완료!")
                st.rerun()
            if _conf2.button("❌ 취소", key=f"dss_{sfx}_cancel_reset"):
                st.session_state[f"dss_{sfx}_reset_confirmed"] = False
                st.rerun()

    # ── 주문표 로드 버튼 ──
    _ss_key = f"dss_{sfx}_result"
    _os_btn_label = "🔄 새로고침" if st.session_state.get(_ss_key) else "📋 주문표 로드"
    if st.button(_os_btn_label, type="primary", key=f"dss_{sfx}_run_os"):
        acct_data["os_start"] = str(os_start)
        acct_data["os_capital"] = float(os_capital)
        cfg["accounts"][acct_name] = acct_data
        _save_dss_config(cfg)

        # 매번 로드 시 가격 데이터 캐시 갱신 (intraday 오염 방지)
        clear_dss_data_cache()

        try:
            soxl = get_soxl_data()
            qqq = get_qqq_data()
            mode_series = get_mode_series(len(qqq))
        except Exception as _data_err:
            st.error(f"⚠️ 가격 데이터 로드 실패: {_data_err}\n\n잠시 후 다시 시도해주세요.")
            soxl = None

        if soxl is not None:
            os_params = DSSParams(
                sf_divisions=cur_sf_div, sf_max_hold=cur_sf_hold,
                sf_buy_pct=cur_sf_buy / 100, sf_sell_pct=cur_sf_sell / 100,
                ag_divisions=cur_ag_div, ag_max_hold=cur_ag_hold,
                ag_buy_pct=cur_ag_buy / 100, ag_sell_pct=cur_ag_sell / 100,
                initial_capital=float(os_capital),
                fee_rate=cur_fee / 100,
                renewal_period=cur_renew,
                pcr=cur_pcr / 100, lcr=cur_lcr / 100,
            )

            today_str = pd.Timestamp.today().strftime("%Y-%m-%d")
            with st.spinner("포트폴리오 시뮬레이션 중..."):
                bt_df = run_backtest(
                    os_params, soxl, mode_series,
                    str(os_start), today_str,
                )

            _adj_hist_for_os = acct_data.get("capital_adj_history", [])
            if not isinstance(_adj_hist_for_os, list):
                _adj_hist_for_os = []

            if bt_df is not None and not bt_df.empty:
                _save_dss_history(bt_df, acct_name)
                st.session_state[_ss_key] = _build_os_result_from_backtest(
                    bt_df, os_params, os_capital, qqq,
                    adj_history=_adj_hist_for_os)
            else:
                # 시작일이 최근이어서 백테스트 결과 없음 → 최신 시장 데이터로 폴백
                _fallback = _build_os_result_fallback(
                    os_params, os_capital, adj_history=_adj_hist_for_os)
                if _fallback is not None:
                    st.session_state[_ss_key] = _fallback
                    st.info("ℹ️ 시작일이 최근이어서 매매 내역은 없지만, 현재 시장 데이터 기반으로 주문표를 생성했습니다.")
                else:
                    st.error("⚠️ 가격 데이터를 불러올 수 없습니다. 잠시 후 다시 시도해주세요.")

    # ── 결과 렌더링 ──
    _os = st.session_state.get(_ss_key)
    if _os is not None:
        _mode_icon = "🟢" if _os["last_mode"] == "AG" else "🔵"
        _mode_label = "공세모드 (AG)" if _os["last_mode"] == "AG" else "안전모드 (SF)"
        _mode_bg = "#E8F5E9" if _os["last_mode"] == "AG" else "#E3F2FD"
        _mode_fg = "#2E7D32" if _os["last_mode"] == "AG" else "#1565C0"

        _last_date_str = _os['last_date'].strftime('%Y-%m-%d') if hasattr(_os['last_date'], 'strftime') else str(_os['last_date'])[:10]
        st.markdown(f"<div style='font-size:0.82em;color:#888;margin-bottom:4px'>"
                    f"데이터 기준: <b>{_last_date_str}</b></div>",
                    unsafe_allow_html=True)

        # ── 모드 & 현황 카드 ──
        _rsi_val = f"{_os['latest_rsi']:.2f}" if _os['latest_rsi'] else "-"
        _rsi_prev = f"{_os['prev_rsi']:.2f}" if _os['prev_rsi'] else "-"
        _rsi_delta_color = "#2E7D32" if _os['latest_rsi'] and _os['prev_rsi'] and _os['latest_rsi'] > _os['prev_rsi'] else "#C62828"
        st.markdown(f"""
        <div style="display:flex;gap:10px;margin-bottom:12px">
          <div style="flex:1.2;background:{_mode_bg};border-radius:10px;padding:14px 18px;text-align:center">
            <div style="font-size:0.72em;color:#888;margin-bottom:2px">현재 모드</div>
            <div style="font-size:1.15em;font-weight:700;color:{_mode_fg}">{_mode_icon} {_mode_label}</div>
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:14px 18px;text-align:center">
            <div style="font-size:0.72em;color:#888;margin-bottom:2px">QQQ 주간 RSI</div>
            <div style="font-size:1.15em;font-weight:700;color:#333">{_rsi_val}</div>
            <div style="font-size:0.68em;color:{_rsi_delta_color}">전주 {_rsi_prev}</div>
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:14px 18px;text-align:center">
            <div style="font-size:0.72em;color:#888;margin-bottom:2px">분할수 / 보유</div>
            <div style="font-size:1.15em;font-weight:700;color:#333">{_os['n_pos']} / {_os['cur_divisions']}</div>
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:14px 18px;text-align:center">
            <div style="font-size:0.72em;color:#888;margin-bottom:2px">매수 / 매도 조건</div>
            <div style="font-size:1.15em;font-weight:700;color:#333">+{_os['cur_buy_pct']*100:.1f}% / +{_os['cur_sell_pct']*100:.1f}%</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # ── 포트폴리오 요약 카드 ──
        _os_init_cap = _os.get("os_capital", float(p["initial_capital"]))
        _os_adj_applied = _os.get("adj_applied", 0.0)
        # 수익률은 자본 조정 제외한 순수 매매 성과 기준
        _trading_asset = _os['total_asset'] - _os_adj_applied
        _ret_pct = (_trading_asset / _os_init_cap - 1) * 100 if _os_init_cap > 0 else 0
        _ret_color = "#2E7D32" if _ret_pct >= 0 else "#C62828"
        _eval_color = "#2E7D32" if _os['eval_pnl'] >= 0 else "#C62828"
        _real_color = "#2E7D32" if _os['cum_realized'] >= 0 else "#C62828"
        _adj_caption = (f'<div style="font-size:0.68em;color:#0B7A3E;font-weight:600">'
                        f'+ 조정 ${_os_adj_applied:+,.0f}</div>') if abs(_os_adj_applied) > 0.01 else ''
        st.markdown(f"""
        <div style="display:flex;gap:10px;margin-bottom:8px">
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px 16px;text-align:center">
            <div style="font-size:0.72em;color:#888;margin-bottom:2px">시작 자본</div>
            <div style="font-size:1.1em;font-weight:700;color:#333">${_os_init_cap:,.0f}</div>
            {_adj_caption}
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px 16px;text-align:center">
            <div style="font-size:0.72em;color:#888;margin-bottom:2px">총자산</div>
            <div style="font-size:1.1em;font-weight:700;color:#333">${_os['total_asset']:,.0f}</div>
            <div style="font-size:0.68em;color:{_ret_color};font-weight:600">{_ret_pct:+.1f}%</div>
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px 16px;text-align:center">
            <div style="font-size:0.72em;color:#888;margin-bottom:2px">평가손익</div>
            <div style="font-size:1.1em;font-weight:700;color:{_eval_color}">${_os['eval_pnl']:+,.0f}</div>
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px 16px;text-align:center">
            <div style="font-size:0.72em;color:#888;margin-bottom:2px">누적실현손익</div>
            <div style="font-size:1.1em;font-weight:700;color:{_real_color}">${_os['cum_realized']:+,.0f}</div>
            <div style="font-size:0.68em;color:#888">매도 {_os['sell_count']}회</div>
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px 16px;text-align:center">
            <div style="font-size:0.72em;color:#888;margin-bottom:2px">예수금</div>
            <div style="font-size:1.1em;font-weight:700;color:#333">${_os['cash']:,.0f}</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # ── 오늘의 LOC 주문 ──
        st.divider()
        _today_date_str = datetime.today().strftime('%Y-%m-%d')
        st.subheader(f"📑 오늘의 주문  ({_today_date_str})")
        st.markdown(
            f"<div style='font-size:0.85em;color:#888;margin-bottom:8px'>"
            f"전일종가 = <b>${_os['prev_close']:,.2f}</b>&ensp;·&ensp;"
            f"매수주문가 = 전일종가 × (1 + {_os['cur_buy_pct']*100:.1f}%) = "
            f"<b>${_os['next_buy_order']:,.2f}</b>&ensp;·&ensp;"
            f"1회시드 = <b>${_os['seed_per_trade']:,.0f}</b></div>",
            unsafe_allow_html=True,
        )

        # 거래일 인덱스 (잔여일 계산용) — 미래 영업일 보정 포함
        try:
            _raw_idx = get_soxl_data().index
            _extra_bdays = pd.bdate_range(_raw_idx[-1] + pd.Timedelta(days=1), periods=60)
            _trading_days_idx = _raw_idx.append(_extra_bdays)
        except Exception:
            _trading_days_idx = None
        _today_ts = pd.Timestamp(datetime.today().date())

        # ── 포지션별 공통 데이터 계산 ──
        _pos_info = []  # (pos, idx, is_stop_today, remain_days, reserve_date_str)
        for i, pos in enumerate(_os['open_positions']):
            if pos['sell_target'] is None:
                continue
            _stop = pos.get('stop_date')
            _is_stop_today = False
            _remain_days = None
            _reserve_date_str = ""

            if _stop is not None and _trading_days_idx is not None:
                _stop_ts = pd.Timestamp(_stop)
                _is_stop_today = (_stop_ts <= _today_ts)
                if not _is_stop_today:
                    _future = _trading_days_idx[
                        (_trading_days_idx > _today_ts) & (_trading_days_idx <= _stop_ts)]
                    _remain_days = len(_future)
                    _before_stop = _trading_days_idx[_trading_days_idx < _stop_ts]
                    if len(_before_stop) > 0:
                        _reserve_dt = _before_stop[-1]
                        _reserve_date_str = _reserve_dt.strftime('%m/%d').replace('/0', '/').lstrip('0')
                    else:
                        _reserve_date_str = _stop_ts.strftime('%m/%d').replace('/0', '/').lstrip('0')
            _pos_info.append((pos, i, _is_stop_today, _remain_days, _reserve_date_str))

        _can_buy = _os['n_pos'] < _os['cur_divisions']

        # ── 두 가지 주문 방식 탭 ──
        _order_tab1, _order_tab2 = st.tabs([
            "📋 매일 주문 (직접 LOC)",
            "📅 예약 주문 (세팅 후 대기)",
        ])

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 탭 1: 매일 주문 — 오늘 직접 넣을 LOC 주문
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━
        with _order_tab1:
            st.caption("매일 장 시작 전에 아래 주문을 직접 넣는 방식입니다.")
            _daily_orders = []
            for pos, i, _is_stop, _remain, _rdate in _pos_info:
                if _is_stop:
                    _daily_orders.append({
                        "구분": "🔴 MOC매도", "시드": f"티어{i+1}",
                        "주문가": "시장가(종가)",
                        "수량": f"{pos['qty']:,}주",
                        "예상금액": f"${pos['qty'] * _os['prev_close']:,.2f}",
                        "비고": (f"⏰ 손절일 도래 — 매수가 ${pos['buy_price']:.2f} "
                                 f"({(_os['prev_close']/pos['buy_price']-1)*100:+.1f}%)"),
                    })
                else:
                    _daily_orders.append({
                        "구분": "LOC매도", "시드": f"티어{i+1}",
                        "주문가": f"${pos['sell_target']:,.2f}",
                        "수량": f"{pos['qty']:,}주",
                        "예상금액": f"${pos['qty'] * pos['sell_target']:,.2f}",
                        "비고": (f"매수가 ${pos['buy_price']:.2f} → "
                                 f"목표 ${pos['sell_target']:.2f} "
                                 f"({(pos['sell_target']/pos['buy_price']-1)*100:+.2f}%)"),
                    })
            if _can_buy:
                _daily_orders.append({
                    "구분": "LOC매수", "시드": f"티어{_os['n_pos']+1}",
                    "주문가": f"${_os['next_buy_order']:,.2f}",
                    "수량": f"{_os['buy_qty_est']:,}주",
                    "예상금액": f"${_os['buy_qty_est'] * _os['next_buy_order']:,.2f}",
                    "비고": (f"종가 ≤ ${_os['next_buy_order']:,.2f} 이면 체결 · "
                             f"슬롯 {_os['n_pos']}/{_os['cur_divisions']}"),
                })
            else:
                st.info(f"⚠️ 모든 슬롯({_os['cur_divisions']}개)이 사용 중 — 매수 없음")

            if _daily_orders:
                def _style_daily(row):
                    s = [""] * len(row)
                    if "구분" in row.index:
                        ix = list(row.index).index("구분")
                        val = row["구분"]
                        if "MOC" in val:
                            s[ix] = "color: #C62828; font-weight: bold"
                        elif "매도" in val:
                            s[ix] = "color: #1565C0; font-weight: bold"
                        elif "매수" in val:
                            s[ix] = "color: #E65100; font-weight: bold"
                    return s
                st.dataframe(
                    pd.DataFrame(_daily_orders).style.apply(_style_daily, axis=1),
                    use_container_width=True, hide_index=True,
                    height=38 + 35 * len(_daily_orders),
                )
                st.info("💡 **매일 주문 방식**: 매일 장 시작 전에 위 LOC 주문을 넣고, "
                        "장 마감 시 조건 충족되면 체결됩니다. 미체결 시 자동 취소되므로 다음 날 다시 넣어주세요.")

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 탭 2: 예약 주문 — LOC 예약 세팅 후 대기
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━
        with _order_tab2:
            st.caption("LOC 예약 주문을 세팅해두면, 조건 도달 시 자동 체결됩니다. 손절일에는 MOC 매도로 전환하세요.")
            _reserve_orders = []
            for pos, i, _is_stop, _remain, _rdate in _pos_info:
                if _is_stop:
                    _reserve_orders.append({
                        "구분": "🔴 MOC매도", "시드": f"티어{i+1}",
                        "주문가": "시장가(종가)",
                        "수량": f"{pos['qty']:,}주",
                        "예약기한": "⏰ 오늘",
                        "비고": (f"손절일 도래 — 매수가 ${pos['buy_price']:.2f} "
                                 f"({(_os['prev_close']/pos['buy_price']-1)*100:+.1f}%)"),
                    })
                else:
                    _deadline = f"~{_rdate} (잔여 {_remain}일)" if _remain is not None and _rdate else "-"
                    _reserve_orders.append({
                        "구분": "예약LOC매도", "시드": f"티어{i+1}",
                        "주문가": f"${pos['sell_target']:,.2f}",
                        "수량": f"{pos['qty']:,}주",
                        "예약기한": _deadline,
                        "비고": (f"매수가 ${pos['buy_price']:.2f} → "
                                 f"목표 ${pos['sell_target']:.2f} "
                                 f"({(pos['sell_target']/pos['buy_price']-1)*100:+.2f}%)"),
                    })
            if _can_buy:
                _reserve_orders.append({
                    "구분": "LOC매수", "시드": f"티어{_os['n_pos']+1}",
                    "주문가": f"${_os['next_buy_order']:,.2f}",
                    "수량": f"{_os['buy_qty_est']:,}주",
                    "예약기한": "당일만",
                    "비고": (f"종가 ≤ ${_os['next_buy_order']:,.2f} 이면 체결 · "
                             f"슬롯 {_os['n_pos']}/{_os['cur_divisions']}"),
                })
            else:
                st.info(f"⚠️ 모든 슬롯({_os['cur_divisions']}개)이 사용 중 — 매수 없음")

            if _reserve_orders:
                def _style_reserve(row):
                    s = [""] * len(row)
                    if "구분" in row.index:
                        ix = list(row.index).index("구분")
                        val = row["구분"]
                        if "MOC" in val:
                            s[ix] = "color: #C62828; font-weight: bold"
                        elif "매도" in val:
                            s[ix] = "color: #1565C0; font-weight: bold"
                        elif "매수" in val:
                            s[ix] = "color: #E65100; font-weight: bold"
                    return s
                st.dataframe(
                    pd.DataFrame(_reserve_orders).style.apply(_style_reserve, axis=1),
                    use_container_width=True, hide_index=True,
                    height=38 + 35 * len(_reserve_orders),
                )
                st.info("💡 **예약 주문 방식**: 매도 LOC를 예약 세팅해두면 기한 내 목표가 도달 시 자동 체결됩니다.\n\n"
                        "⚠️ **예약기한 만료 시** 미체결된 예약은 취소하고, 해당 시드를 **MOC(시장가 종가) 매도**로 전환하세요.\n\n"
                        "📌 **매수는 매일 갱신** — 전일종가 기준이므로 매일 새로 주문을 넣어야 합니다.")

        # ── 현재 보유 현황 ──
        st.divider()
        st.subheader("📦 현재 보유 시드")

        if _os['open_positions']:
            pos_rows = []
            _trading_days_all = get_soxl_data().index
            for i, pos in enumerate(_os['open_positions']):
                buy_d = pos['buy_date']
                buy_d_str = buy_d.strftime('%Y-%m-%d') if hasattr(buy_d, 'strftime') else str(buy_d)
                stop_d = pos['stop_date']
                stop_d_str = stop_d.strftime('%Y-%m-%d') if stop_d is not None and hasattr(stop_d, 'strftime') else str(stop_d) if stop_d else "-"
                _held_days = len(_trading_days_all[
                    (_trading_days_all >= buy_d) & (_trading_days_all <= _os['last_date'])])
                _remain_days = len(_trading_days_all[
                    (_trading_days_all > _os['last_date']) & (_trading_days_all <= stop_d)
                ]) if stop_d is not None else None
                pnl_pct = (_os['prev_close'] / pos['buy_price'] - 1) * 100 if pos['buy_price'] > 0 else 0
                pnl_amt = (pos['qty'] * _os['prev_close']) - (pos['qty'] * pos['buy_price'])
                pos_rows.append({
                    "시드": f"티어{i+1}", "모드": pos['mode'],
                    "매수일": buy_d_str, "매수가": f"${pos['buy_price']:.2f}",
                    "수량": f"{pos['qty']:,}주", "매수금액": f"${pos['buy_price']*pos['qty']:,.2f}",
                    "매도목표가": f"${pos['sell_target']:.2f}" if pos['sell_target'] else "-",
                    "손절예정일": stop_d_str,
                    "보유일": f"{_held_days}일",
                    "잔여일": f"{_remain_days}일" if _remain_days is not None else "-",
                    "평가손익": f"${pnl_amt:+,.0f} ({pnl_pct:+.1f}%)",
                })

            def _style_pnl_pos(val):
                if isinstance(val, str):
                    if val.startswith("$+") or val.startswith("$0"):
                        return "color: #1565C0; font-weight: bold"
                    elif val.startswith("$-"):
                        return "color: #C62828; font-weight: bold"
                return ""
            df_pos = pd.DataFrame(pos_rows)
            st.dataframe(df_pos.style.map(_style_pnl_pos, subset=["평가손익"]),
                         use_container_width=True, hide_index=True,
                         height=38 + 35 * len(df_pos))

            _total_holding_qty = sum(pos['qty'] for pos in _os['open_positions'])
            _avg_buy_price = sum(pos['buy_price'] * pos['qty'] for pos in _os['open_positions']) / _total_holding_qty if _total_holding_qty > 0 else 0
            hc1, hc2, hc3, hc4 = st.columns(4)
            hc1.metric("총 보유주수", f"{_total_holding_qty:,}주")
            hc2.metric("평균매수가", f"${_avg_buy_price:.2f}")
            hc3.metric("현재가 (종가)", f"${_os['prev_close']:,.2f}")
            hc4.metric("평가금액", f"${_os['holding_value']:,.0f}",
                       delta=f"{(_os['prev_close']/_avg_buy_price-1)*100:+.2f}%" if _avg_buy_price > 0 else "")
        else:
            st.info("현재 보유 시드 없음 (전량 현금)")
            st.metric("보유현금", f"${_os['cash']:,.0f}")

        # ── 최근 매도 기록 ──
        st.divider()
        _bt_df = _os.get('bt_df')
        if _bt_df is not None and not _bt_df.empty:
            with st.expander("📊 최근 매도 기록 (최근 20건)"):
                _sell_records = []
                for _, row in _bt_df.iterrows():
                    if row['매도내역'] is not None:
                        for sr in row['매도내역']:
                            _sell_records.append({
                                "매수일": pd.Timestamp(sr['buy_date']).strftime('%Y-%m-%d'),
                                "매도일": pd.Timestamp(sr['sell_date']).strftime('%Y-%m-%d'),
                                "모드": sr['mode'],
                                "매수가": f"${sr['buy_price']:.2f}",
                                "매도가": f"${sr['sell_price']:.2f}",
                                "수량": f"{sr['qty']:,}주",
                                "매도목표": f"${sr['sell_target']:.2f}",
                                "손절일": pd.Timestamp(sr['stop_date']).strftime('%Y-%m-%d'),
                                "손익": f"${sr['pnl']:+,.2f}",
                                "결과": "✅ 익절" if sr['pnl'] >= 0 else "❌ 손절",
                            })
                if _sell_records:
                    _df_sells = pd.DataFrame(_sell_records)
                    _df_sells_show = _df_sells.tail(20).iloc[::-1].reset_index(drop=True)

                    def _style_sell_result(val):
                        if "익절" in str(val): return "color: #1565C0; font-weight: bold"
                        if "손절" in str(val): return "color: #C62828; font-weight: bold"
                        return ""
                    st.dataframe(
                        _df_sells_show.style.map(_style_sell_result, subset=["결과"]),
                        use_container_width=True, hide_index=True,
                        height=min(38 + 35 * len(_df_sells_show), 600))

                    _total_sells = len(_sell_records)
                    _win_sells = sum(1 for s in _sell_records if "익절" in s["결과"])
                    _loss_sells = _total_sells - _win_sells
                    sc1, sc2, sc3 = st.columns(3)
                    sc1.metric("총 매도", f"{_total_sells}회")
                    sc2.metric("익절", f"{_win_sells}회 ({_win_sells/_total_sells*100:.0f}%)" if _total_sells else "0회")
                    sc3.metric("손절", f"{_loss_sells}회 ({_loss_sells/_total_sells*100:.0f}%)" if _total_sells else "0회")
                else:
                    st.info("매도 기록이 없습니다.")

        # ── 일별 매매 상세표 ──
        st.divider()
        st.subheader("📋 일별 매매 상세표")

        _df_daily = _load_dss_history(acct_name)
        if not _df_daily.empty:
            _buy_count = _df_daily["매매"].astype(str).str.startswith("BUY").sum()
            _sell_count = _df_daily["매매"].astype(str).str.startswith("SELL").sum()
            _total_trades = _buy_count + _sell_count
            _first_date = _df_daily["날짜"].iloc[0] if len(_df_daily) > 0 else "-"
            _last_date_h = _df_daily["날짜"].iloc[-1] if len(_df_daily) > 0 else "-"
            st.markdown(
                f"기록 {_first_date} ~ {_last_date_h} | "
                f"총 **{_total_trades}건** (매수 {_buy_count}회 · 매도 {_sell_count}회)"
            )
            st.info("📌 이 기록은 실제 주문표 로드 시점에 누적 저장된 데이터입니다.")

            _df_show = _df_daily.iloc[::-1].reset_index(drop=True)

            def _style_daily_row(row):
                action = str(row.get("매매", ""))
                if action.startswith("BUY"):
                    return ["background-color: #FFF0F0"] * len(row)
                if action.startswith("SELL"):
                    return ["background-color: #F0FFF4"] * len(row)
                return [""] * len(row)

            def _style_action(val):
                s = str(val)
                if s.startswith("BUY"):
                    return "color: #C62828; font-weight: bold"
                if s.startswith("SELL"):
                    return "color: #1565C0; font-weight: bold"
                return "color: #999"

            def _style_pnl(val):
                s = str(val)
                if s.startswith("+"):
                    return "color: #1565C0; font-weight: bold"
                if s.startswith("-") and s != "-":
                    return "color: #C62828; font-weight: bold"
                return "color: #999"

            st.dataframe(
                _df_show.style
                    .apply(_style_daily_row, axis=1)
                    .map(_style_action, subset=["매매"])
                    .map(_style_pnl, subset=["실현손익($)", "실현손익률(%)"]),
                hide_index=True, use_container_width=True,
                height=min(38 + 35 * len(_df_show), 600),
            )

            # 다운로드 버튼
            _dl1, _dl2, _ = st.columns([1, 1, 3])
            _today_dl = pd.Timestamp.today().strftime("%Y%m%d")

            _csv_data = _df_daily.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            _dl1.download_button(
                "📥 CSV 다운로드", data=_csv_data,
                file_name=f"DSS_{acct_name}_{_today_dl}.csv",
                mime="text/csv", key=f"dss_{sfx}_dl_csv", use_container_width=True,
            )

            _buf = _io.BytesIO()
            with pd.ExcelWriter(_buf, engine="openpyxl") as _writer:
                _df_daily.to_excel(_writer, index=False, sheet_name="일별매매상세")
            _dl2.download_button(
                "📥 엑셀 다운로드", data=_buf.getvalue(),
                file_name=f"DSS_{acct_name}_{_today_dl}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dss_{sfx}_dl_xlsx", use_container_width=True,
            )
        else:
            st.info("아직 저장된 매매 기록이 없습니다. '주문표 로드' 버튼을 클릭하면 기록이 시작됩니다.")


def render_ordersheet_tab(params):
    """주문표 & 계좌관리 탭."""
    p = params
    _today_str_title = datetime.today().strftime("%Y-%m-%d")
    st.subheader(f"📋 오늘의 주문표  ({_today_str_title})")
    st.caption("종목별 포트폴리오를 추적하여 현황과 내일 LOC 주문을 표시합니다.")

    # ── 설정 로드 ──
    _cfg = _load_dss_config()

    # ══════════════════════════════════════════════
    # 계좌 관리
    # ══════════════════════════════════════════════
    _accounts = _cfg.get("accounts", {})

    # ── 레거시 마이그레이션: accounts 없으면 기존 config에서 생성 ──
    if not _accounts:
        _legacy_start = _cfg.get("os_start", "2024-01-01")
        _legacy_capital = _cfg.get("os_capital", 10000.0)
        _legacy_adj = _cfg.get("capital_adj_history", [])
        _accounts = {"기본계좌": {
            "os_start": str(_legacy_start),
            "os_capital": float(_legacy_capital),
            "capital_adj_history": _legacy_adj if isinstance(_legacy_adj, list) else [],
            "params": {k: _DSS_DEFAULT_PARAMS[k] for k in _DSS_PARAM_KEYS},
        }}
        _cfg["accounts"] = _accounts
        _save_dss_config(_cfg)

    # ── 계좌 추가 ──
    with st.expander("➕ 계좌 추가"):
        _ac1, _ac2, _ac3 = st.columns([2, 1, 1])
        _new_name = _ac1.text_input("계좌 이름", placeholder="예: 메인, ISA, 추가계좌",
                                     key="dss_new_acct_name")
        _new_start = _ac2.date_input("시작일", value=datetime.today().date(),
                                      key="dss_new_acct_start")
        _new_capital = _ac3.number_input("시작 자본 ($)", value=100000.0, step=1000.0,
                                          key="dss_new_acct_capital")
        # 프리셋 선택
        _preset_labels = [pr["label"] for pr in _DSS_PRESETS]
        _preset_idx = st.selectbox("파라미터 프리셋", range(len(_DSS_PRESETS)),
                                    format_func=lambda i: _preset_labels[i],
                                    index=2, key="dss_new_acct_preset")
        if st.button("✅ 계좌 등록", type="primary", key="dss_add_account_btn",
                     use_container_width=True):
            _nm = _new_name.strip()
            if not _nm:
                st.warning("계좌 이름을 입력하세요.")
            elif _nm in _accounts:
                st.warning(f"'{_nm}' 계좌가 이미 존재합니다.")
            else:
                _sel_preset = _DSS_PRESETS[_preset_idx]
                _accounts[_nm] = {
                    "os_start": str(_new_start),
                    "os_capital": float(_new_capital),
                    "capital_adj_history": [],
                    "params": {k: _sel_preset[k] for k in _DSS_PARAM_KEYS},
                }
                _cfg["accounts"] = _accounts
                _save_dss_config(_cfg)
                st.success(f"✅ '{_nm}' 계좌가 등록되었습니다. (프리셋: {_sel_preset['label']})")
                st.rerun()

    # ── 계좌 탭 ──
    _acct_names = list(_accounts.keys())
    if not _acct_names:
        st.info("등록된 계좌가 없습니다. 위에서 계좌를 추가하세요.")
        return

    _acct_tabs = st.tabs([f"📊 {n}" for n in _acct_names])

    for _ai, (_aname, _atab) in enumerate(zip(_acct_names, _acct_tabs)):
        with _atab:
            _render_dss_account(_aname, _accounts[_aname], _cfg, p, _ai)

# ══════════════════════════════════════════════
# 탭 5: DB — xlsx DB 시트 재현 (하루씩 누적 기록)
# ══════════════════════════════════════════════

def render_db_tab(params=None):
    """DB 탭 — 일별/주별 누적 기록."""
    p = params or {}

    st.markdown("### DB — 일별/주별 누적 기록")
    st.caption("백테스트 결과를 원본 DB 시트 형식으로 표시합니다. 하루하루 한 줄씩 늘어나는 기록입니다.")

    if st.button("DB 생성", type="primary", key="dss_run_db"):
        try:
            soxl = get_soxl_data()
            qqq = get_qqq_data()
            mode_series_df = get_mode_series(len(qqq))
            weekly_rsi_df = build_weekly_rsi_series(qqq)
        except Exception as _e:
            st.error(f"⚠️ 데이터 로드 실패: {_e}")
            return

        dss_p = _make_params(p)
        result = run_backtest(
            dss_p, soxl, mode_series_df,
            start_date=str(p.get("bt_start_date", "2010-01-01")),
            end_date=str(p.get("bt_end_date", datetime.today().strftime("%Y-%m-%d"))),
        )

        if result is None or result.empty:
            st.warning("결과가 없습니다. 날짜 범위를 확인하세요.")
        else:
            st.session_state['dss_db_result'] = result
            st.session_state['dss_db_mode_series'] = mode_series_df
            st.session_state['dss_db_weekly_rsi'] = weekly_rsi_df

    if 'dss_db_result' not in st.session_state:
        st.info("🔘 위 버튼을 눌러 DB를 생성하세요.")
        return

    result = st.session_state['dss_db_result']
    if result is None or result.empty:
        st.warning("저장된 결과가 비어 있습니다. 다시 실행하세요.")
        return

    result = st.session_state['dss_db_result']
    mode_series_df = st.session_state['dss_db_mode_series']
    weekly_rsi_df = st.session_state['dss_db_weekly_rsi']

    # ── 최신 데이터 요약 (맨 위) ──
    latest = result.iloc[-1]
    latest_date = latest['날짜']
    latest_mode = latest['모드']

    # 최신 주차 RSI
    weekly_in_range = mode_series_df[
        mode_series_df['week_end'] <= latest_date + pd.Timedelta(days=6)
    ]
    latest_week = weekly_in_range.iloc[-1] if len(weekly_in_range) > 0 else None
    latest_rsi = latest_week['rsi'] if latest_week is not None else 0
    latest_week_end = latest_week['week_end'] if latest_week is not None else latest_date
    latest_week_start = latest_week_end - pd.Timedelta(days=4)
    week_num = len(weekly_in_range)

    mode_icon = "🔴" if latest_mode == "AG" else "🔵"
    st.markdown(
        f"#### 최신 현황 — {latest_date.strftime('%Y-%m-%d')} | "
        f"{mode_icon} **{latest_mode}** | "
        f"RSI {latest_rsi:.2f} (W{week_num})"
    )
    lc1, lc2, lc3, lc4, lc5 = st.columns(5)
    lc1.metric("종가", f"${latest['종가']:.2f}")
    lc2.metric("보유포지션", f"{int(latest['보유포지션수'])}/{int(latest['분할수'])}")
    lc3.metric("총자산", f"${latest['총자산']:,.0f}")
    lc4.metric("투자금", f"${latest['투자금']:,.0f}")
    lc5.metric("예수금", f"${latest['예수금']:,.0f}")

    lc6, lc7, lc8 = st.columns(3)
    lc6.metric("누적실현", f"${latest['누적실현']:,.0f}")
    lc7.metric("누적매도", f"{int(latest['누적매도'])}회")
    lc8.metric("1회시드", f"${latest['1회시드']:,.0f}")

    # ── 섹션 1: 일별 종가 + 매매 기록 (좌측) ──
    st.markdown("---")
    db_col1, db_col2 = st.columns([3, 2])

    _start_date_str = str(p.get("bt_start_date", "2010-01-01"))
    _end_date_str = str(p.get("bt_end_date", datetime.today().strftime("%Y-%m-%d")))

    with db_col1:
        st.markdown("#### 일별 SOXL 기록")

        # 일별 로그: 날짜, 종가, 모드, 매수/매도, 포지션 등
        daily_log = result[['날짜', '종가', '모드', '매수주문가', '매수체결', '수량',
                            '매도목표가', '보유포지션수', '분할수', '1회시드',
                            '투자금', '예수금', '평가금', '총자산',
                            '당일실현', '누적실현', '누적매도']].copy()
        daily_log['날짜'] = daily_log['날짜'].dt.strftime('%y.%m.%d')

        # 숫자 포맷
        for col in ['종가', '매수주문가', '매수체결', '매도목표가', '1회시드']:
            daily_log[col] = daily_log[col].apply(
                lambda x: f"${x:.2f}" if pd.notna(x) else "")
        for col in ['투자금', '예수금', '평가금', '총자산', '당일실현', '누적실현']:
            daily_log[col] = daily_log[col].apply(lambda x: f"${x:,.0f}" if x != 0 else "")
        daily_log['수량'] = daily_log['수량'].apply(lambda x: str(x) if x > 0 else "")
        daily_log['누적매도'] = daily_log['누적매도'].astype(int)

        st.dataframe(daily_log, use_container_width=True, height=600)
        st.caption(f"총 {len(daily_log)}거래일 기록")

    with db_col2:
        st.markdown("#### 주별 RSI / 모드")

        # 주별 데이터: 주차, 모드, 시작~종료, RSI
        weekly_in_range = mode_series_df[
            (mode_series_df['week_end'] >= pd.Timestamp(_start_date_str) - pd.Timedelta(days=7)) &
            (mode_series_df['week_end'] <= pd.Timestamp(_end_date_str) + pd.Timedelta(days=7))
        ].copy()

        weekly_display = pd.DataFrame()
        weekly_display['주차'] = range(1, len(weekly_in_range) + 1)
        weekly_display['모드'] = weekly_in_range['mode'].values
        weekly_display['시작'] = (weekly_in_range['week_end'].values
                                 - pd.Timedelta(days=4)).astype('datetime64[ns]')
        weekly_display['종료'] = weekly_in_range['week_end'].values
        weekly_display['시작'] = pd.to_datetime(weekly_display['시작']).dt.strftime('%y.%m.%d')
        weekly_display['종료'] = pd.to_datetime(weekly_display['종료']).dt.strftime('%y.%m.%d')
        weekly_display['RSI'] = weekly_in_range['rsi'].values.round(2)

        # 모드별 색상을 위한 스타일
        def color_mode(val):
            if val == "AG":
                return 'background-color: #ffe0e0'
            elif val == "SF":
                return 'background-color: #e0e0ff'
            return ''

        styled = weekly_display.style.map(color_mode, subset=['모드'])
        st.dataframe(styled, use_container_width=True, height=600)

    # ── 섹션 2: 모드 가이드 차트 (RSI + 기준선) ──
    st.markdown("---")
    st.markdown("#### 모드 가이드 차트")

    # 투자 기간 내 주별 RSI 차트
    fig_rsi = go.Figure()

    # RSI 라인 (모드별 색상)
    for _, row in weekly_in_range.iterrows():
        color = 'red' if row['mode'] == 'AG' else 'blue'
        fig_rsi.add_trace(go.Scatter(
            x=[row['week_end']], y=[row['rsi']],
            mode='markers',
            marker=dict(color=color, size=6),
            showlegend=False,
        ))

    # RSI 전체 라인
    fig_rsi.add_trace(go.Scatter(
        x=weekly_in_range['week_end'],
        y=weekly_in_range['rsi'],
        mode='lines',
        line=dict(color='gray', width=1),
        name='RSI',
    ))

    # 기준선들 (35, 40, 50, 60, 65)
    thresholds = [
        (35, 'green', 'dash', 'RSI 35'),
        (40, 'orange', 'dot', 'RSI 40'),
        (50, 'black', 'solid', 'RSI 50'),
        (60, 'orange', 'dot', 'RSI 60'),
        (65, 'red', 'dash', 'RSI 65'),
    ]
    for level, color, dash, name in thresholds:
        fig_rsi.add_hline(y=level, line=dict(color=color, dash=dash, width=1),
                          annotation_text=name, annotation_position="bottom right")

    # AG/SF 배경색 (모드 전환 구간)
    prev_mode = None
    span_start = None
    for _, row in weekly_in_range.iterrows():
        if row['mode'] != prev_mode:
            if prev_mode is not None and span_start is not None:
                fill_color = 'rgba(255,200,200,0.2)' if prev_mode == 'AG' else 'rgba(200,200,255,0.2)'
                fig_rsi.add_vrect(x0=span_start, x1=row['week_end'],
                                  fillcolor=fill_color, line_width=0, layer='below')
            span_start = row['week_end']
            prev_mode = row['mode']
    # 마지막 구간
    if prev_mode and span_start is not None:
        fill_color = 'rgba(255,200,200,0.2)' if prev_mode == 'AG' else 'rgba(200,200,255,0.2)'
        fig_rsi.add_vrect(x0=span_start, x1=weekly_in_range['week_end'].iloc[-1],
                          fillcolor=fill_color, line_width=0, layer='below')

    fig_rsi.update_layout(
        height=350,
        yaxis_title="QQQ Weekly RSI",
        xaxis_title="",
        margin=dict(t=30, b=30),
        legend=dict(orientation='h', y=1.05),
    )
    st.plotly_chart(fig_rsi, use_container_width=True)

    # ── 섹션 3: Historical RSI (2010~) ──
    st.markdown("---")
    with st.expander("Historical RSI (전체 기간)", expanded=False):
        hist_rsi = weekly_rsi_df.copy()
        hist_rsi = hist_rsi.merge(
            mode_series_df[['week_end', 'mode']],
            on='week_end', how='left'
        )
        hist_display = pd.DataFrame()
        hist_display['주차'] = range(1, len(hist_rsi) + 1)
        hist_display['시작'] = (hist_rsi['week_end'].values
                                - pd.Timedelta(days=4)).astype('datetime64[ns]')
        hist_display['종료'] = hist_rsi['week_end'].values
        hist_display['시작'] = pd.to_datetime(hist_display['시작']).dt.strftime('%y.%m.%d')
        hist_display['종료'] = pd.to_datetime(hist_display['종료']).dt.strftime('%y.%m.%d')
        hist_display['RSI'] = hist_rsi['rsi'].values.round(2)
        hist_display['모드'] = hist_rsi['mode'].values

        st.dataframe(hist_display, use_container_width=True, height=500)
        st.caption(f"총 {len(hist_display)}주 기록 (2010~)")

        # Historical RSI 차트
        fig_hist = go.Figure()
        fig_hist.add_trace(go.Scatter(
            x=hist_rsi['week_end'], y=hist_rsi['rsi'],
            mode='lines', line=dict(color='gray', width=1), name='RSI',
        ))
        for level, color, dash, name in thresholds:
            fig_hist.add_hline(y=level, line=dict(color=color, dash=dash, width=1))
        fig_hist.update_layout(height=300, margin=dict(t=20, b=20),
                               yaxis_title="QQQ Weekly RSI")
        st.plotly_chart(fig_hist, use_container_width=True)

    # ── 섹션 4: 매도 내역 누적 기록 ──
    with st.expander("매도 내역 누적 기록"):
        all_sells = []
        for _, row in result.iterrows():
            if row['매도내역']:
                for s in row['매도내역']:
                    all_sells.append(s)
        if all_sells:
            sells_df = pd.DataFrame(all_sells)
            sells_df['buy_date'] = sells_df['buy_date'].dt.strftime('%y.%m.%d')
            sells_df['sell_date'] = sells_df['sell_date'].dt.strftime('%y.%m.%d')
            sells_df['stop_date'] = sells_df['stop_date'].dt.strftime('%y.%m.%d')
            sells_df.columns = ['매수일', '매수가', '수량', '매도일', '매도가',
                                '매도목표', '손절일', '손익', '모드']
            sells_df['손익'] = sells_df['손익'].round(2)
            sells_df.insert(0, '회차', range(1, len(sells_df) + 1))
            st.dataframe(sells_df, use_container_width=True, height=400)
            st.caption(f"총 {len(sells_df)}회 매도 완료")
        else:
            st.info("매도 기록이 없습니다.")


# ══════════════════════════════════════════════
# 탭 6: 전략 소개 & 성과 분석
# ══════════════════════════════════════════════

def render_intro_tab(params):
    """전략 소개 & 성과 분석 탭."""
    p = params

    # ──────────────────────────────────────────
    # 1. 전략 소개
    # ──────────────────────────────────────────
    st.subheader("📖 DSS 동파법 (동적파도타기법) 이란?")

    left, right = st.columns([3, 2])

    with left:
        st.markdown("""
#### 전략 개요
**DSS 동파법(동적파도타기법)**은 **알고리C**님이 개발한 SOXL(3x 반도체 레버리지 ETF) 전용 매매 전략입니다.

QQQ 주간 RSI를 기반으로 **안전모드(SF)**와 **공세모드(AG)**를 전환하며,
시장 국면에 따라 매수/매도 조건과 보유기간을 동적으로 조절합니다.

투자금을 N등분(시드 분할)하여 매일 1시드씩 매수를 시도하고,
각 시드별로 독립적인 매도 목표가와 손절일을 관리합니다.

---

#### 핵심 특징
- **듀얼 모드 시스템**: 시장 상황에 따른 안전/공세 모드 자동 전환
- **LOC 주문**: 장 마감 직전 종가 기준 조건부 주문
- **시드 분할 관리**: 각 시드별 독립적 매수-매도-손절 사이클
- **자동 복리**: 매도 N회마다 실현손익을 투자금에 반영
        """)

    with right:
        st.info("""
**매수주문가 공식**
```
매수주문가 = ROUNDDOWN(
  전일종가 × (1 + 매수조건%), 2)
```
당일 종가 ≤ 매수주문가 → **종가로 체결**

---

**매도목표가 공식**
```
매도목표가 = ROUND(
  매수체결가 × (1 + 매도조건%), 2)
```
종가 ≥ 매도목표가 → **종가로 체결**
        """)

        st.info("""
**LOC 주문이란?**

장 마감 직전 일정 가격 이하/이상이면
종가로 체결되는 조건부 시장가 주문입니다.

미국 기준 오후 3:55 이전에 주문합니다.
        """)

    st.divider()

    # ──────────────────────────────────────────
    # 2. 매매 규칙 상세
    # ──────────────────────────────────────────
    st.subheader("📋 매매 규칙 상세")

    r1, r2 = st.columns(2)
    with r1:
        st.markdown("""
##### 매수 룰
1. **매수주문가** = `ROUNDDOWN(전일종가 × (1 + 매수조건%), 2)`
2. 당일 종가 ≤ 매수주문가이면 → **당일 종가로 체결**
3. 수량 = `INT(1회시드 / 매수주문가)`
4. 빈 시드 슬롯이 있을 때만 매수 (최대 분할수만큼)
5. **하루에 1시드만** 매수 시도

##### 매도 룰
1. **매도목표가** = `ROUND(매수체결가 × (1 + 매도조건%), 2)`
2. 종가 ≥ 매도목표가 → **종가로 익절 매도**
3. 최대보유기간 초과 시 → **종가로 강제 손절**
4. 같은 날 여러 시드 동시 매도 가능
        """)
    with r2:
        st.markdown("##### 순정 파라미터")
        st.dataframe(pd.DataFrame({
            "파라미터": ["분할수", "매수조건(%)", "매도조건(%)", "최대보유기간(거래일)"],
            "안전모드 (SF)": [7, "+3.0%", "+0.2%", 30],
            "공세모드 (AG)": [7, "+5.0%", "+2.5%", 7],
        }), hide_index=True, use_container_width=True)

        st.markdown("##### 공통 파라미터")
        st.dataframe(pd.DataFrame({
            "파라미터": ["이익복리율 (PCR)", "손실복리율 (LCR)", "투자금갱신주기", "수수료"],
            "기본값": ["80%", "30%", "매도 10회마다", "0.04%"],
        }), hide_index=True, use_container_width=True)

    st.divider()

    # ──────────────────────────────────────────
    # 3. 모드 전환 규칙 + RSI 시각화
    # ──────────────────────────────────────────
    st.subheader("🔄 모드 전환 규칙")

    m1, m2 = st.columns([3, 2])
    with m1:
        st.markdown("""
QQQ 주간 RSI의 **2주 전(RR)**과 **1주 전(R)** 값으로 현재 주 모드를 결정합니다.
        """)
        st.dataframe(pd.DataFrame({
            "조건": [
                "RR ≤ 35  &  RR < R",
                "40 ≤ RR < 50  &  RR > R",
                "RR ≤ 50  &  R > 50",
                "RR ≥ 50  &  R < 50",
                "50 ≤ RR < 60  &  RR < R",
                "RR > 65  &  RR > R",
                "그 외",
            ],
            "모드": ["🟢 AG", "🔵 SF", "🟢 AG", "🔵 SF", "🟢 AG", "🔵 SF", "유지"],
            "해석": [
                "RSI 35 이하에서 반등 시작 → 공세 전환",
                "RSI 40~50에서 하락 → 안전 전환",
                "RSI 50 상향돌파 → 공세 전환",
                "RSI 50 하향돌파 → 안전 전환",
                "RSI 50~60에서 상승 추세 → 공세 전환",
                "RSI 65 이상 과열 후 하락 → 안전 전환",
                "명확한 신호 없음 → 직전 모드 유지",
            ],
        }), hide_index=True, use_container_width=True, height=300)

    with m2:
        st.markdown("""
##### 주간 RSI 계산 (KB증권 단순평균 방식)
```
14주분 주봉 종가 변화량:

Change[i] = Close[i] - Close[i-1]

AvgUp = sum(max(Change, 0)) / 14
AvgDn = sum(max(-Change, 0)) / 14
RS    = AvgUp / AvgDn
RSI   = RS / (1 + RS) × 100
```

> 일반적인 Wilder의 지수이동평균 RSI와 달리
> **KB증권 방식**은 단순평균(SMA)을 사용합니다.
        """)

    st.divider()

    # ──────────────────────────────────────────
    # 4. 투자금 갱신 (복리 시스템)
    # ──────────────────────────────────────────
    st.subheader("💰 투자금 갱신 (복리 시스템)")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("""
매도가 **N회** (기본 10회) 완료될 때마다 투자금이 자동 갱신됩니다.

**계산 방식:**
1. 직전 N회 매도의 실현손익을 합산
2. **이익일 때**: 투자금 += 합산이익 × 이익복리율 (PCR)
3. **손실일 때**: 투자금 += 합산손실 × 손실복리율 (LCR)

> 이익은 높은 비율(80%)로 재투자하고,
> 손실은 낮은 비율(30%)만 반영하여 **방어적 복리** 효과를 구현합니다.
        """)
    with c2:
        st.markdown("##### 복리 예시 (투자금 $100,000 기준)")
        st.dataframe(pd.DataFrame({
            "상황": ["10회 매도 합산 +$5,000", "10회 매도 합산 -$3,000"],
            "복리율": ["80% (PCR)", "30% (LCR)"],
            "갱신 금액": ["+$4,000", "-$900"],
            "갱신 후 투자금": ["$104,000", "$99,100"],
        }), hide_index=True, use_container_width=True)

    st.divider()

    # ──────────────────────────────────────────
    # 5. 모드별 전략 성격 비교
    # ──────────────────────────────────────────
    st.subheader("⚖️ 안전모드 vs 공세모드 비교")

    st.markdown("""
    <div style="display:flex;gap:16px;margin-bottom:16px">
      <div style="flex:1;background:#E3F2FD;border-radius:12px;padding:20px;border-left:4px solid #1565C0">
        <h4 style="color:#1565C0;margin:0 0 10px">🔵 안전모드 (SF)</h4>
        <ul style="font-size:0.9em;line-height:1.8;margin:0;padding-left:18px">
          <li><b>매수조건 +3%</b> — 전일 종가 대비 3% 상승까지 매수 허용</li>
          <li><b>매도조건 +0.2%</b> — 아주 작은 이익에도 매도 (빠른 회전)</li>
          <li><b>보유기간 30일</b> — 충분한 회복 시간 부여</li>
          <li>하락장에서 <b>손실 최소화</b>에 초점</li>
          <li>잦은 매매로 소폭 이익 누적</li>
        </ul>
      </div>
      <div style="flex:1;background:#E8F5E9;border-radius:12px;padding:20px;border-left:4px solid #2E7D32">
        <h4 style="color:#2E7D32;margin:0 0 10px">🟢 공세모드 (AG)</h4>
        <ul style="font-size:0.9em;line-height:1.8;margin:0;padding-left:18px">
          <li><b>매수조건 +5%</b> — 전일 종가 대비 5% 상승까지 매수 허용</li>
          <li><b>매도조건 +2.5%</b> — 충분한 이익 확보 후 매도</li>
          <li><b>보유기간 7일</b> — 빠른 손절로 리스크 관리</li>
          <li>상승장에서 <b>수익 극대화</b>에 초점</li>
          <li>높은 단건 수익률, 적은 매매 횟수</li>
        </ul>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    # ──────────────────────────────────────────
    # 6. 성과 분석 (동적 차트)
    # ──────────────────────────────────────────
    st.subheader("📊 전략 성과 분석")
    st.markdown("사이드바의 파라미터와 기간 설정을 기반으로 성과를 분석합니다.")

    _initial_capital = float(p["initial_capital"])
    _start_date = str(p["start_date"])
    _end_date = str(p["end_date"])

    # ── 파라미터 소스 선택 (사이드바 or 프리셋) ──
    _src_options = ["📐 사이드바 설정값"] + [pr["label"] for pr in _DSS_PRESETS]
    _src_sel = st.radio("파라미터 소스", _src_options, index=0, horizontal=True,
                        key="dss_intro_param_src")
    _src_idx = _src_options.index(_src_sel)

    if _src_idx == 0:
        # 사이드바 값 사용
        _use_p = dict(p)
    else:
        # 프리셋 값 사용 (사이드바 기간/자본은 유지)
        _pr = _DSS_PRESETS[_src_idx - 1]
        _use_p = dict(p)
        for _k in _DSS_PARAM_KEYS:
            _use_p[_k] = _pr[_k]

    # 선택된 파라미터 미리보기
    with st.expander("🔍 적용 파라미터 확인", expanded=False):
        _pv1, _pv2 = st.columns(2)
        _pv1.markdown(
            f"**🔵 안전(SF)** — 분할 {_use_p['sf_div']} · 보유 {_use_p['sf_hold']}일 · "
            f"매수 {_use_p['sf_buy']}% · 매도 {_use_p['sf_sell']}%")
        _pv2.markdown(
            f"**🟢 공세(AG)** — 분할 {_use_p['ag_div']} · 보유 {_use_p['ag_hold']}일 · "
            f"매수 {_use_p['ag_buy']}% · 매도 {_use_p['ag_sell']}%")
        st.caption(
            f"PCR {_use_p['pcr']}% · LCR {_use_p['lcr']}% · "
            f"갱신주기 {_use_p['renewal_period']}일 · 수수료 {_use_p['fee_rate']}% · "
            f"기간 {_start_date} ~ {_end_date} · 자본 ${_initial_capital:,.0f}")

    if st.button("▶ 성과 분석 실행", type="primary", key="dss_run_intro_perf"):
        with st.spinner("데이터 로드 및 분석 중..."):
            try:
                soxl_intro = get_soxl_data()
                qqq_intro = get_qqq_data()
                mode_series_intro = get_mode_series(len(qqq_intro))
            except Exception as _e:
                st.error(f"⚠️ 데이터 로드 실패: {_e}")
                return

            params_intro = DSSParams(
                sf_divisions=_use_p["sf_div"], sf_max_hold=_use_p["sf_hold"],
                sf_buy_pct=_use_p["sf_buy"]/100, sf_sell_pct=_use_p["sf_sell"]/100,
                ag_divisions=_use_p["ag_div"], ag_max_hold=_use_p["ag_hold"],
                ag_buy_pct=_use_p["ag_buy"]/100, ag_sell_pct=_use_p["ag_sell"]/100,
                initial_capital=_initial_capital,
                fee_rate=_use_p["fee_rate"]/100,
                renewal_period=_use_p["renewal_period"],
                pcr=_use_p["pcr"]/100, lcr=_use_p["lcr"]/100,
            )

            bt_intro = run_backtest(
                params_intro, soxl_intro, mode_series_intro,
                _start_date, _end_date,
            )

        if bt_intro is not None and not bt_intro.empty:
            st.session_state["dss_intro_bt"] = bt_intro
            st.session_state["dss_intro_bt_label"] = _src_sel

    _intro_bt = st.session_state.get("dss_intro_bt")
    if _intro_bt is not None and not _intro_bt.empty:
        bt = _intro_bt
        _bt_label = st.session_state.get("dss_intro_bt_label", "")
        if _bt_label:
            st.caption(f"📌 분석 기준: **{_bt_label}**")

        # 핵심 지표
        _final = float(bt.iloc[-1]['총자산'])
        _days = len(bt)
        _years = _days / 252
        _total_ret = (_final / _initial_capital - 1)
        _cagr = (_final / _initial_capital) ** (1 / _years) - 1 if _years > 0 else 0

        _peak = bt['총자산'].cummax()
        _dd = (bt['총자산'] - _peak) / _peak
        _mdd = float(_dd.min())
        _calmar = abs(_cagr / _mdd) if _mdd != 0 else 0

        # 일간 수익률 기반 Sharpe/Sortino
        _assets = bt['총자산'].values.astype(float)
        _daily_rets = np.diff(_assets) / _assets[:-1]
        _sharpe = (np.mean(_daily_rets) / np.std(_daily_rets) * np.sqrt(252)) if np.std(_daily_rets) > 0 else 0
        _neg_rets = _daily_rets[_daily_rets < 0]
        _sortino = (np.mean(_daily_rets) / np.std(_neg_rets) * np.sqrt(252)) if len(_neg_rets) > 0 and np.std(_neg_rets) > 0 else 0

        st.markdown(f"""
        <div style="display:flex;gap:10px;margin:12px 0">
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
            <div style="font-size:0.72em;color:#888">CAGR</div>
            <div style="font-size:1.15em;font-weight:700;color:#333">{_cagr*100:.2f}%</div>
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
            <div style="font-size:0.72em;color:#888">총 수익률</div>
            <div style="font-size:1.15em;font-weight:700;color:{'#2E7D32' if _total_ret >= 0 else '#C62828'}">{_total_ret*100:+,.1f}%</div>
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
            <div style="font-size:0.72em;color:#888">최대 MDD</div>
            <div style="font-size:1.15em;font-weight:700;color:#C62828">{_mdd*100:.2f}%</div>
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
            <div style="font-size:0.72em;color:#888">Calmar Ratio</div>
            <div style="font-size:1.15em;font-weight:700;color:#333">{_calmar:.3f}</div>
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
            <div style="font-size:0.72em;color:#888">Sharpe Ratio</div>
            <div style="font-size:1.15em;font-weight:700;color:#333">{_sharpe:.3f}</div>
          </div>
          <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:12px;text-align:center">
            <div style="font-size:0.72em;color:#888">Sortino Ratio</div>
            <div style="font-size:1.15em;font-weight:700;color:#333">{_sortino:.3f}</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        st.divider()

        # ── 연도별 성과 ──
        st.subheader("📅 연도별 성과")
        bt_yearly = bt.copy()
        bt_yearly['날짜'] = pd.to_datetime(bt_yearly['날짜'])
        bt_yearly['연도'] = bt_yearly['날짜'].dt.year

        yearly_rows = []
        for year, grp in bt_yearly.groupby('연도'):
            start_val = float(grp.iloc[0]['총자산'])
            end_val = float(grp.iloc[-1]['총자산'])
            yr_ret = (end_val / start_val - 1) * 100
            yr_peak = grp['총자산'].cummax()
            yr_dd = (grp['총자산'] - yr_peak) / yr_peak
            yr_mdd = float(yr_dd.min()) * 100
            yearly_rows.append({"연도": int(year), "연간수익률(%)": yr_ret, "MDD(%)": yr_mdd})
        df_yearly = pd.DataFrame(yearly_rows)

        def _color_yr(val):
            if isinstance(val, (int, float)):
                if val > 0: return "color: #2E7D32; font-weight:bold"
                if val < 0: return "color: #C62828; font-weight:bold"
            return ""
        st.dataframe(
            df_yearly.style
                .map(_color_yr, subset=["연간수익률(%)"])
                .format({"연간수익률(%)": "{:+.2f}%", "MDD(%)": "{:.2f}%"}),
            hide_index=True, use_container_width=True,
        )
        st.divider()

        # ── 월별 수익률 히트맵 ──
        st.subheader("🗓️ 월별 수익률 히트맵")
        bt_monthly = bt.copy()
        bt_monthly['날짜'] = pd.to_datetime(bt_monthly['날짜'])
        bt_monthly['연도'] = bt_monthly['날짜'].dt.year
        bt_monthly['월'] = bt_monthly['날짜'].dt.month

        monthly_rows = []
        for (year, month), grp in bt_monthly.groupby(['연도', '월']):
            sv = float(grp.iloc[0]['총자산'])
            ev = float(grp.iloc[-1]['총자산'])
            mr = (ev / sv - 1) * 100 if sv > 0 else 0
            monthly_rows.append({"연도": int(year), "월": int(month), "수익률(%)": round(mr, 2)})
        df_mon = pd.DataFrame(monthly_rows)
        if not df_mon.empty:
            _mp = df_mon.pivot(index="연도", columns="월", values="수익률(%)")
            _mp.columns = [f"{m}월" for m in _mp.columns]
            fig_heatmap = px.imshow(
                _mp, color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
                text_auto=".1f", labels={"x": "월", "y": "연도", "color": "수익률(%)"},
                aspect="auto",
            )
            fig_heatmap.update_layout(
                height=max(320, len(_mp) * 38 + 120),
                coloraxis_colorbar=dict(title="수익률(%)"),
            )
            st.plotly_chart(fig_heatmap, use_container_width=True)
        st.divider()

        # ── 총자산 추이 & 낙폭 차트 ──
        st.subheader("📈 총자산 추이 & 낙폭")
        _dates = pd.to_datetime(bt['날짜'])
        fig_asset = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                  row_heights=[0.7, 0.3], vertical_spacing=0.05)
        fig_asset.add_trace(go.Scatter(x=_dates, y=_assets,
                                       mode='lines', name='총자산',
                                       line=dict(color='#1565C0', width=1.5)), row=1, col=1)
        fig_asset.add_trace(go.Scatter(x=_dates, y=_dd.values * 100,
                                       mode='lines', name='낙폭(%)',
                                       fill='tozeroy', fillcolor='rgba(198,40,40,0.15)',
                                       line=dict(color='#C62828', width=1)), row=2, col=1)
        fig_asset.update_layout(height=500, showlegend=True,
                                legend=dict(orientation='h', y=1.02),
                                margin=dict(l=60, r=20, t=40, b=30))
        fig_asset.update_yaxes(title_text="총자산 ($)", row=1, col=1)
        fig_asset.update_yaxes(title_text="낙폭 (%)", row=2, col=1)
        st.plotly_chart(fig_asset, use_container_width=True)
        st.divider()

        # ── 모드별 RSI 추이 시각화 ──
        st.subheader("🔄 QQQ 주간 RSI & 모드 변화")
        try:
            qqq_intro2 = get_qqq_data()
            rsi_df = build_weekly_rsi_series(qqq_intro2)
            if len(rsi_df) > 0:
                mode_s = build_mode_series(len(qqq_intro2))
                rsi_df_plot = rsi_df.copy()
                rsi_df_plot['mode'] = rsi_df_plot.index.map(
                    lambda i: mode_s[i] if i < len(mode_s) else 'SF')
                fig_rsi = go.Figure()
                # RSI 라인
                fig_rsi.add_trace(go.Scatter(
                    x=rsi_df_plot['week_end'], y=rsi_df_plot['rsi'],
                    mode='lines', name='QQQ 주간 RSI',
                    line=dict(color='#555', width=1.5)))
                # 기준선
                for lvl, clr, nm in [(35, '#C62828', 'RSI 35'), (50, '#FF9800', 'RSI 50'), (65, '#2E7D32', 'RSI 65')]:
                    fig_rsi.add_hline(y=lvl, line_dash="dot",
                                      line_color=clr, opacity=0.5,
                                      annotation_text=nm, annotation_position="right")
                # AG/SF 배경
                ag_mask = rsi_df_plot['mode'] == 'AG'
                fig_rsi.add_trace(go.Scatter(
                    x=rsi_df_plot.loc[ag_mask, 'week_end'],
                    y=rsi_df_plot.loc[ag_mask, 'rsi'],
                    mode='markers', name='공세 (AG)',
                    marker=dict(color='#4CAF50', size=4, opacity=0.7)))
                sf_mask = rsi_df_plot['mode'] == 'SF'
                fig_rsi.add_trace(go.Scatter(
                    x=rsi_df_plot.loc[sf_mask, 'week_end'],
                    y=rsi_df_plot.loc[sf_mask, 'rsi'],
                    mode='markers', name='안전 (SF)',
                    marker=dict(color='#1565C0', size=4, opacity=0.7)))
                fig_rsi.update_layout(height=350, showlegend=True,
                                      legend=dict(orientation='h', y=1.05),
                                      margin=dict(l=50, r=20, t=40, b=30),
                                      yaxis_title="RSI")
                st.plotly_chart(fig_rsi, use_container_width=True)
        except Exception:
            st.info("RSI 차트를 생성할 수 없습니다.")
        st.divider()

        # ── 승률 & 손익비 분석 ──
        st.subheader("🎯 승률 & 손익비 분석")
        _sell_recs = []
        for _, row in bt.iterrows():
            if row['매도내역'] is not None:
                for sr in row['매도내역']:
                    _sell_recs.append(sr)
        if _sell_recs:
            _wins = [s for s in _sell_recs if s['pnl'] >= 0]
            _losses = [s for s in _sell_recs if s['pnl'] < 0]
            _win_rate = len(_wins) / len(_sell_recs) * 100
            _avg_win = np.mean([s['pnl'] for s in _wins]) if _wins else 0
            _avg_loss = abs(np.mean([s['pnl'] for s in _losses])) if _losses else 0
            _payoff = _avg_win / _avg_loss if _avg_loss > 0 else float('inf')
            _total_pnl = sum(s['pnl'] for s in _sell_recs)

            st.markdown(f"""
            <div style="display:flex;gap:10px;margin:8px 0">
              <div style="flex:1;background:#E8F5E9;border-radius:10px;padding:14px;text-align:center">
                <div style="font-size:0.72em;color:#888">총 매도</div>
                <div style="font-size:1.1em;font-weight:700">{len(_sell_recs)}회</div>
              </div>
              <div style="flex:1;background:#E8F5E9;border-radius:10px;padding:14px;text-align:center">
                <div style="font-size:0.72em;color:#888">승률</div>
                <div style="font-size:1.1em;font-weight:700;color:#2E7D32">{_win_rate:.1f}%</div>
              </div>
              <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:14px;text-align:center">
                <div style="font-size:0.72em;color:#888">평균 익절</div>
                <div style="font-size:1.1em;font-weight:700;color:#2E7D32">${_avg_win:+,.2f}</div>
              </div>
              <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:14px;text-align:center">
                <div style="font-size:0.72em;color:#888">평균 손절</div>
                <div style="font-size:1.1em;font-weight:700;color:#C62828">-${_avg_loss:,.2f}</div>
              </div>
              <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:14px;text-align:center">
                <div style="font-size:0.72em;color:#888">손익비</div>
                <div style="font-size:1.1em;font-weight:700">{_payoff:.2f}</div>
              </div>
              <div style="flex:1;background:#FAFAFA;border:1px solid #EEE;border-radius:10px;padding:14px;text-align:center">
                <div style="font-size:0.72em;color:#888">누적 실현손익</div>
                <div style="font-size:1.1em;font-weight:700;color:{'#2E7D32' if _total_pnl>=0 else '#C62828'}">${_total_pnl:+,.0f}</div>
              </div>
            </div>
            """, unsafe_allow_html=True)

            # 손익 분포 히스토그램
            _pnls = [s['pnl'] for s in _sell_recs]
            fig_hist = go.Figure()
            fig_hist.add_trace(go.Histogram(
                x=_pnls, nbinsx=50, name='실현손익 분포',
                marker_color=['#2E7D32' if p >= 0 else '#C62828' for p in sorted(_pnls)]))
            fig_hist.update_layout(height=300, showlegend=False,
                                    xaxis_title="실현손익 ($)", yaxis_title="빈도",
                                    margin=dict(l=50, r=20, t=30, b=30))
            fig_hist.add_vline(x=0, line_dash="dash", line_color="gray")
            st.plotly_chart(fig_hist, use_container_width=True)
        else:
            st.info("매도 기록이 없어 승률 분석을 표시할 수 없습니다.")

        st.divider()

        # ── DSS vs Buy & Hold 비교 ──
        st.subheader("📉 DSS 동파법 vs Buy & Hold 비교")
        st.caption("같은 기간 SOXL을 단순 보유했을 때와 DSS 전략 성과를 비교합니다.")
        _bh_start_price = float(bt.iloc[0]['종가'])
        _bh_prices = bt['종가'].values.astype(float)
        _bh_shares = int(_initial_capital / _bh_start_price)
        _bh_leftover = _initial_capital - _bh_shares * _bh_start_price
        _bh_assets = _bh_shares * _bh_prices + _bh_leftover
        _bh_final = float(_bh_assets[-1])
        _bh_ret = (_bh_final / _initial_capital - 1) * 100
        _bh_cagr = (_bh_final / _initial_capital) ** (1 / _years) - 1 if _years > 0 else 0

        fig_bh = go.Figure()
        fig_bh.add_trace(go.Scatter(
            x=_dates, y=_assets, name="DSS 동파법",
            line=dict(color='#1565C0', width=2)))
        fig_bh.add_trace(go.Scatter(
            x=_dates, y=_bh_assets, name="Buy & Hold (SOXL)",
            line=dict(color='#EF5350', width=2, dash='dot')))
        fig_bh.add_hline(y=_initial_capital, line_dash="dash", line_color="#aaa",
                          annotation_text="시작 자본")
        fig_bh.update_layout(
            title=f"전략 수익 {_total_ret*100:+,.1f}% vs B&H {_bh_ret:+,.1f}%",
            yaxis_title="자산 ($)", height=400,
            legend=dict(orientation='h', y=1.08),
            margin=dict(l=60, r=20, t=60, b=30))
        st.plotly_chart(fig_bh, use_container_width=True)

        st.markdown(f"""
        <div style="display:flex;gap:12px;margin:8px 0">
          <div style="flex:1;background:#E3F2FD;border-radius:10px;padding:16px;text-align:center">
            <div style="font-size:0.8em;color:#1565C0;font-weight:600">DSS 동파법</div>
            <div style="font-size:1.2em;font-weight:700;margin:4px 0">${_final:,.0f}</div>
            <div style="font-size:0.75em;color:#555">수익률 {_total_ret*100:+,.1f}% · CAGR {_cagr*100:.1f}% · MDD {_mdd*100:.1f}%</div>
          </div>
          <div style="flex:1;background:#FFF3E0;border-radius:10px;padding:16px;text-align:center">
            <div style="font-size:0.8em;color:#E65100;font-weight:600">Buy & Hold (SOXL)</div>
            <div style="font-size:1.2em;font-weight:700;margin:4px 0">${_bh_final:,.0f}</div>
            <div style="font-size:0.75em;color:#555">수익률 {_bh_ret:+,.1f}% · CAGR {_bh_cagr*100:.1f}%</div>
          </div>
        </div>
        """, unsafe_allow_html=True)
        st.divider()

        # ── 회복력 분석 ──
        st.subheader("🛡️ 회복력 분석 (10% 이상 하락 에피소드)")
        st.caption("고점 대비 10% 이상 하락한 구간별로, 어디까지 빠지고 얼마 만에 회복했는지 분석합니다.")
        _recovery_records = []
        _peak_val = float(_assets[0])
        _peak_idx = 0
        _in_dd = False
        _trough_val = _peak_val
        _trough_idx = 0
        _dates_list = pd.to_datetime(bt['날짜']).tolist()
        for i in range(1, len(_assets)):
            curr = float(_assets[i])
            dd_pct = (curr - _peak_val) / _peak_val * 100
            if not _in_dd:
                if curr > _peak_val:
                    _peak_val = curr; _peak_idx = i
                elif dd_pct <= -10:
                    _in_dd = True; _trough_val = curr; _trough_idx = i
            else:
                if curr < _trough_val:
                    _trough_val = curr; _trough_idx = i
                if curr >= _peak_val:
                    _recovery_records.append({
                        "고점": str(_dates_list[_peak_idx].date()),
                        "고점 자산": f"${_peak_val:,.0f}",
                        "최대하락 시점": str(_dates_list[_trough_idx].date()),
                        "저점 자산": f"${_trough_val:,.0f}",
                        "하락율": f"{(_trough_val - _peak_val) / _peak_val * 100:.1f}%",
                        "회복 시점": str(_dates_list[i].date()),
                        "기간(일)": (_dates_list[i] - _dates_list[_peak_idx]).days,
                    })
                    _in_dd = False; _peak_val = curr; _peak_idx = i
                    _trough_val = curr; _trough_idx = i
        if _in_dd:
            _recovery_records.append({
                "고점": str(_dates_list[_peak_idx].date()),
                "고점 자산": f"${_peak_val:,.0f}",
                "최대하락 시점": str(_dates_list[_trough_idx].date()),
                "저점 자산": f"${_trough_val:,.0f}",
                "하락율": f"{(_trough_val - _peak_val) / _peak_val * 100:.1f}%",
                "회복 시점": "미회복 ⏳",
                "기간(일)": (_dates_list[-1] - _dates_list[_peak_idx]).days,
            })
        if _recovery_records:
            def _clr_recovery(val):
                s = str(val)
                if s.startswith("-") and "%" in s: return "color:#C62828;font-weight:bold"
                if "미회복" in s: return "color:#C62828;font-weight:bold"
                return ""
            st.dataframe(
                pd.DataFrame(_recovery_records).style.map(_clr_recovery),
                hide_index=True, use_container_width=True)
        else:
            st.info("10% 이상 하락 에피소드가 없습니다.")
        st.divider()

        # ── 모드별 성과 비교 ──
        st.subheader("🔄 안전모드 vs 공세모드 매매 성과")
        st.caption("각 모드에서 발생한 매도 건의 수익/손실을 비교합니다.")
        if _sell_recs:
            _sf_sells = [s for s in _sell_recs if s.get('mode') == 'SF']
            _ag_sells = [s for s in _sell_recs if s.get('mode') == 'AG']

            def _mode_stats(sells, name):
                if not sells:
                    return {"모드": name, "매도 횟수": 0, "승률": "-",
                            "평균 손익($)": "-", "총 손익($)": "-"}
                wins = [s for s in sells if s['pnl'] >= 0]
                wr = len(wins) / len(sells) * 100
                avg_pnl = np.mean([s['pnl'] for s in sells])
                total_pnl = sum(s['pnl'] for s in sells)
                return {
                    "모드": name, "매도 횟수": len(sells),
                    "승률": f"{wr:.1f}%",
                    "평균 손익($)": f"${avg_pnl:+,.2f}",
                    "총 손익($)": f"${total_pnl:+,.0f}",
                }

            df_mode_cmp = pd.DataFrame([
                _mode_stats(_sf_sells, "🔵 안전모드 (SF)"),
                _mode_stats(_ag_sells, "🟢 공세모드 (AG)"),
                _mode_stats(_sell_recs, "📊 전체"),
            ])
            st.dataframe(df_mode_cmp, hide_index=True, use_container_width=True)

            # 모드별 손익 분포
            if _sf_sells and _ag_sells:
                fig_mode_box = go.Figure()
                fig_mode_box.add_trace(go.Box(
                    y=[s['pnl'] for s in _sf_sells], name='안전모드 (SF)',
                    marker_color='#1565C0', boxmean=True))
                fig_mode_box.add_trace(go.Box(
                    y=[s['pnl'] for s in _ag_sells], name='공세모드 (AG)',
                    marker_color='#4CAF50', boxmean=True))
                fig_mode_box.update_layout(
                    height=350, yaxis_title="실현손익 ($)",
                    showlegend=False, margin=dict(l=60, r=20, t=30, b=30))
                fig_mode_box.add_hline(y=0, line_dash="dash", line_color="gray")
                st.plotly_chart(fig_mode_box, use_container_width=True)

        st.divider()

        # ── 드로다운 (Underwater) 분석 ──
        st.subheader("🌊 드로다운 (Underwater) 분석")
        st.caption("고점 대비 현재 손실 비율 추이. 얼마나 깊이, 얼마나 오래 손실 구간에 있었는지 보여줍니다.")
        _peak_arr = np.maximum.accumulate(_assets)
        _dd_arr = (_assets - _peak_arr) / _peak_arr * 100
        _fig_uw = go.Figure()
        _fig_uw.add_trace(go.Scatter(
            x=_dates, y=_dd_arr.tolist(),
            fill="tozeroy", name="드로다운(%)",
            line=dict(color="#EF5350", width=1),
            fillcolor="rgba(239,83,80,0.25)",
        ))
        _fig_uw.add_hline(y=0, line_color="#888", line_width=1)
        _fig_uw.update_layout(yaxis_title="드로다운 (%)", height=300, yaxis=dict(tickformat=".1f"),
                              margin=dict(l=60, r=20, t=30, b=30))
        st.plotly_chart(_fig_uw, use_container_width=True)

        # 드로다운 구간 TOP5
        _dd_s = pd.Series(_dd_arr, index=_dates)
        _in_dd2 = False; _dd2_start = None; _dd2_periods = []
        for _di2, (_ddate2, _dval2) in enumerate(_dd_s.items()):
            if _dval2 < 0 and not _in_dd2:
                _in_dd2 = True; _dd2_start = _ddate2
            elif _dval2 == 0 and _in_dd2:
                _in_dd2 = False
                _sub_dd2 = _dd_s[_dd2_start:_ddate2]
                _dd2_periods.append({
                    "시작일": str(_dd2_start.date()) if hasattr(_dd2_start, 'date') else str(_dd2_start),
                    "회복일": str(_ddate2.date()) if hasattr(_ddate2, 'date') else str(_ddate2),
                    "기간(일)": (_ddate2 - _dd2_start).days,
                    "최대낙폭(%)": round(float(_sub_dd2.min()), 2),
                })
        if _dd2_periods:
            _dd2_df = pd.DataFrame(_dd2_periods).nsmallest(5, "최대낙폭(%)").reset_index(drop=True)
            _dd2_df.index += 1
            st.markdown("**Top 5 최대 낙폭 구간**")
            st.dataframe(_dd2_df.style.format({"최대낙폭(%)": "{:.2f}%"}),
                         hide_index=False, use_container_width=True)
        st.divider()

        # ── 롤링 성과 분석 ──
        st.subheader("📉 롤링 성과 분석")
        st.caption("구간별 성과 추이. 특정 시기에만 좋은 게 아닌지 검증합니다.")
        _roll_tabs = st.tabs(["1년 롤링", "2년 롤링", "3년 롤링"])
        for _rwin, _rtab in zip([252, 504, 756], _roll_tabs):
            with _rtab:
                if len(_assets) > _rwin:
                    _rc_arr = np.full(len(_assets), np.nan)
                    _rm_arr = np.full(len(_assets), np.nan)
                    for _ri in range(_rwin, len(_assets)):
                        _seg = _assets[_ri - _rwin:_ri + 1]
                        _yrs_r = _rwin / 252
                        _rc_arr[_ri] = ((_seg[-1] / _seg[0]) ** (1 / _yrs_r) - 1) * 100
                        _seg_peak = np.maximum.accumulate(_seg)
                        _rm_arr[_ri] = float(((_seg - _seg_peak) / _seg_peak).min()) * 100
                    _valid_r = ~np.isnan(_rc_arr)
                    if _valid_r.sum() > 0:
                        _rdates = _dates[_valid_r]
                        _fig_roll = go.Figure()
                        _fig_roll.add_trace(go.Scatter(
                            x=_rdates, y=_rc_arr[_valid_r].tolist(),
                            name="롤링 CAGR(%)", line=dict(color="#1565C0", width=2), yaxis="y1"))
                        _fig_roll.add_trace(go.Scatter(
                            x=_rdates, y=_rm_arr[_valid_r].tolist(),
                            name="롤링 MDD(%)", line=dict(color="#EF5350", width=1.5, dash="dot"), yaxis="y2"))
                        _fig_roll.add_hline(y=0, line_dash="dash", line_color="#aaa", yref="y1")
                        _fig_roll.update_layout(
                            yaxis=dict(title="롤링 CAGR (%)", side="left"),
                            yaxis2=dict(title="롤링 MDD (%)", side="right", overlaying="y"),
                            legend=dict(orientation="h", y=1.08), height=340)
                        st.plotly_chart(_fig_roll, use_container_width=True)
                        _r1, _r2, _r3 = st.columns(3)
                        _r1.metric("평균 CAGR", f"{np.nanmean(_rc_arr):+.1f}%")
                        _r2.metric("최고 CAGR", f"{np.nanmax(_rc_arr):+.1f}%")
                        _r3.metric("최저 CAGR", f"{np.nanmin(_rc_arr):+.1f}%")
                    else:
                        st.info(f"분석 기간이 {_rwin // 252}년보다 짧아 롤링 분석이 불가합니다.")
                else:
                    st.info(f"분석 기간이 {_rwin // 252}년보다 짧아 롤링 분석이 불가합니다.")
        st.divider()

        # ── 매도 손익률 분포 (왜도/첨도 포함) ──
        st.subheader("📊 매도 손익률 분포")
        st.caption("매도 시마다 발생한 손익률의 분포. 수익/손실의 패턴을 분석합니다.")
        if _sell_recs:
            _pnl_pcts = [(s['pnl'] / (s['buy_price'] * s['qty'])) * 100
                         for s in _sell_recs if s['buy_price'] * s['qty'] > 0]
            if _pnl_pcts:
                _pnl_arr2 = np.array(_pnl_pcts)
                _skew = float(pd.Series(_pnl_arr2).skew())
                _kurt = float(pd.Series(_pnl_arr2).kurtosis())
                _fig_pnl2 = go.Figure()
                _fig_pnl2.add_trace(go.Histogram(
                    x=_pnl_arr2.tolist(), nbinsx=40,
                    marker_color=["#EF5350" if v < 0 else "#43A047" for v in sorted(_pnl_arr2)],
                    name="손익률 빈도"))
                _fig_pnl2.add_vline(x=0, line_dash="dash", line_color="#333")
                _fig_pnl2.add_vline(x=float(np.mean(_pnl_arr2)), line_dash="dot",
                                    line_color="#1565C0",
                                    annotation_text=f"평균 {np.mean(_pnl_arr2):+.2f}%",
                                    annotation_position="top right")
                _fig_pnl2.update_layout(xaxis_title="손익률 (%)", yaxis_title="빈도 (회)", height=320,
                                        margin=dict(l=60, r=20, t=30, b=30))
                st.plotly_chart(_fig_pnl2, use_container_width=True)
                _pd1, _pd2, _pd3, _pd4 = st.columns(4)
                _pd1.metric("평균 손익률", f"{np.mean(_pnl_arr2):+.2f}%")
                _pd2.metric("중앙값", f"{np.median(_pnl_arr2):+.2f}%")
                _pd3.metric("왜도 (Skew)", f"{_skew:.3f}",
                            help="양수=우측 꼬리(큰 수익 가끔), 음수=좌측 꼬리(큰 손실 가끔)")
                _pd4.metric("첨도 (Kurt)", f"{_kurt:.3f}",
                            help="높을수록 극단값(큰 수익/손실) 빈도 높음")
        else:
            st.info("매도 이력이 없어 분포 분석이 불가합니다.")
        st.divider()

        # ── 현금 활용률 & 매매 타이밍 패턴 ──
        st.subheader("💵 현금 활용률 & 매매 타이밍 패턴")
        _cash_arr = bt['예수금'].values.astype(float)
        if len(_cash_arr) > 0 and len(_assets) > 0:
            _inv_ratio = (1 - _cash_arr / _assets) * 100
            _inv_ratio = np.clip(_inv_ratio, 0, 100)
            _cu1, _cu2, _cu3 = st.columns(3)
            _cu1.metric("평균 투자 비율", f"{np.mean(_inv_ratio):.1f}%",
                        help="현금이 아닌 주식에 투자된 비율의 평균")
            _cu2.metric("최대 투자 비율", f"{np.max(_inv_ratio):.1f}%")
            _cu3.metric("현금 보유 비율", f"{100 - np.mean(_inv_ratio):.1f}%")

            # 스택 영역 차트
            _ratio_s = pd.Series(_inv_ratio, index=_dates)
            _total_days_r = (_ratio_s.index[-1] - _ratio_s.index[0]).days
            _win_r = 5 if _total_days_r > 365 else 0
            _trend_s = _ratio_s.rolling(_win_r, min_periods=1).mean() if _win_r > 0 else None
            _x_dates_r = _dates.tolist()
            _fig_cu = go.Figure()
            _fig_cu.add_trace(go.Scatter(
                x=_x_dates_r, y=[100.0] * len(_x_dates_r), name="현금",
                mode="lines", line=dict(width=0), fill="tozeroy",
                fillcolor="rgba(200,200,200,0.6)", hoverinfo="skip"))
            _fig_cu.add_trace(go.Scatter(
                x=_x_dates_r, y=_ratio_s.tolist(), name="주식(ETF)",
                mode="lines", line=dict(width=0), fill="tozeroy",
                fillcolor="rgba(255,179,0,0.75)",
                hovertemplate="%{x}<br>주식(ETF): %{y:.1f}%<extra></extra>"))
            if _trend_s is not None:
                _fig_cu.add_trace(go.Scatter(
                    x=_x_dates_r, y=_trend_s.tolist(), name="5일 이동평균 추세선",
                    mode="lines", line=dict(color="rgba(180,80,0,0.85)", width=1.5),
                    hoverinfo="skip"))
            _fig_cu.update_layout(
                yaxis_title="비율 (%)", yaxis=dict(range=[0, 100]),
                height=300, hovermode="x unified",
                legend=dict(orientation="h", x=1.0, y=1.02, xanchor="right", yanchor="bottom",
                            bgcolor="rgba(255,255,255,0.85)", bordercolor="#ccc", borderwidth=1),
                margin=dict(l=60, r=20, t=20, b=40))
            st.plotly_chart(_fig_cu, use_container_width=True)
            if _win_r > 0:
                st.caption("※ 영역: 일별 실제 투자비율 | 주황 선: 5일 이동평균 추세선")

        # 매매 타이밍 패턴 (요일별/월별)
        if _sell_recs:
            _buy_dates_all = []
            _sell_dates_all = []
            for _, row in bt.iterrows():
                if row.get('매수체결') is not None:
                    _buy_dates_all.append(pd.to_datetime(row['날짜']))
                if row['매도내역'] is not None:
                    _sell_dates_all.append(pd.to_datetime(row['날짜']))
            if _buy_dates_all:
                _buy_dows = pd.Series([d.day_name() for d in _buy_dates_all])
                _sell_dows = pd.Series([d.day_name() for d in _sell_dates_all])
                _buy_months = pd.Series([d.month for d in _buy_dates_all])
                _sell_months = pd.Series([d.month for d in _sell_dates_all])
                _dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
                _dow_labels = ["월", "화", "수", "목", "금"]
                _buy_dow = _buy_dows.value_counts().reindex(_dow_order, fill_value=0)
                _sell_dow = _sell_dows.value_counts().reindex(_dow_order, fill_value=0)
                _fig_dow = go.Figure()
                _fig_dow.add_trace(go.Bar(x=_dow_labels, y=_buy_dow.values.tolist(),
                                          name="매수", marker_color="#EF5350"))
                _fig_dow.add_trace(go.Bar(x=_dow_labels, y=_sell_dow.values.tolist(),
                                          name="매도", marker_color="#43A047"))
                _fig_dow.update_layout(barmode="group", title="요일별 매매 빈도",
                                       yaxis_title="횟수", height=300)
                _buy_mon = _buy_months.value_counts().sort_index()
                _sell_mon = _sell_months.value_counts().sort_index()
                _fig_mon = go.Figure()
                _fig_mon.add_trace(go.Bar(x=[f"{m}월" for m in _buy_mon.index],
                                          y=_buy_mon.values.tolist(), name="매수", marker_color="#EF5350"))
                _fig_mon.add_trace(go.Bar(x=[f"{m}월" for m in _sell_mon.index],
                                          y=_sell_mon.values.tolist(), name="매도", marker_color="#43A047"))
                _fig_mon.update_layout(barmode="group", title="월별 매매 빈도",
                                       yaxis_title="횟수", height=300)
                _tc1, _tc2 = st.columns(2)
                with _tc1:
                    st.plotly_chart(_fig_dow, use_container_width=True)
                with _tc2:
                    st.plotly_chart(_fig_mon, use_container_width=True)
        st.divider()

        # ── 파라미터 민감도 분석 ──
        st.subheader("🎛️ 파라미터 민감도 분석")
        st.caption("현재 매수/매도 조건 주변의 Calmar Ratio 분포. 과최적화 여부를 확인합니다.")
        with st.expander("🔍 민감도 히트맵 보기 (클릭하여 실행)", expanded=False):
            _sens_mode = st.radio("분석 대상 모드", ["안전모드 (SF)", "공세모드 (AG)"],
                                  horizontal=True, key="dss_sens_mode_radio")
            if _sens_mode == "안전모드 (SF)":
                _sb_cur, _ss_cur = p["sf_buy"] / 100, p["sf_sell"] / 100
            else:
                _sb_cur, _ss_cur = p["ag_buy"] / 100, p["ag_sell"] / 100
            _n_sens = 5
            _b_range = np.linspace(max(0.001, _sb_cur - 0.015), _sb_cur + 0.015, _n_sens)
            _s_range = np.linspace(max(0.001, _ss_cur - 0.015), _ss_cur + 0.015, _n_sens)
            _heat = np.zeros((_n_sens, _n_sens))
            if st.button("▶ 민감도 분석 실행", key="dss_run_sensitivity"):
                with st.spinner("민감도 분석 중... (25회 시뮬레이션)"):
                    try:
                        soxl_s = get_soxl_data()
                        ms_s = get_mode_series(len(get_qqq_data()))
                    except Exception as _e:
                        st.error(f"⚠️ 데이터 로드 실패: {_e}")
                        return
                    for _bi, _bv in enumerate(_b_range):
                        for _si, _sv in enumerate(_s_range):
                            _p_s = DSSParams(
                                sf_divisions=p["sf_div"] if _sens_mode == "안전모드 (SF)" else p["sf_div"],
                                sf_max_hold=p["sf_hold"],
                                sf_buy_pct=_bv if _sens_mode == "안전모드 (SF)" else p["sf_buy"]/100,
                                sf_sell_pct=_sv if _sens_mode == "안전모드 (SF)" else p["sf_sell"]/100,
                                ag_divisions=p["ag_div"], ag_max_hold=p["ag_hold"],
                                ag_buy_pct=_bv if _sens_mode != "안전모드 (SF)" else p["ag_buy"]/100,
                                ag_sell_pct=_sv if _sens_mode != "안전모드 (SF)" else p["ag_sell"]/100,
                                initial_capital=_initial_capital, fee_rate=p["fee_rate"]/100,
                                renewal_period=p["renewal_period"], pcr=p["pcr"]/100, lcr=p["lcr"]/100,
                            )
                            _bt_s = run_backtest(_p_s, soxl_s, ms_s, _start_date, _end_date)
                            if _bt_s is not None and len(_bt_s) > 10:
                                _a_s = _bt_s['총자산'].values.astype(float)
                                _yrs_s = len(_bt_s) / 252
                                _cagr_s = (_a_s[-1] / _a_s[0]) ** (1 / _yrs_s) - 1 if _yrs_s > 0 else 0
                                _pk_s = np.maximum.accumulate(_a_s)
                                _mdd_s = float(((_a_s - _pk_s) / _pk_s).min())
                                _heat[_bi][_si] = abs(_cagr_s / _mdd_s) if _mdd_s != 0 else 0
                            else:
                                _heat[_bi][_si] = 0
                st.session_state["dss_sens_heat"] = _heat
                st.session_state["dss_sens_b_range"] = _b_range
                st.session_state["dss_sens_s_range"] = _s_range
                st.session_state["dss_sens_cur"] = (_sb_cur, _ss_cur)
                st.session_state["dss_sens_mode_label"] = _sens_mode

            if st.session_state.get("dss_sens_heat") is not None:
                _h = st.session_state["dss_sens_heat"]
                _br = st.session_state["dss_sens_b_range"]
                _sr2 = st.session_state["dss_sens_s_range"]
                _scur = st.session_state["dss_sens_cur"]
                _sml = st.session_state.get("dss_sens_mode_label", "")
                _b_labels = [f"{v*100:.2f}%" for v in _br]
                _s_labels = [f"{v*100:.2f}%" for v in _sr2]
                _fig_heat = px.imshow(
                    _h, x=_s_labels, y=_b_labels,
                    color_continuous_scale="RdYlGn",
                    labels={"x": "매도조건(%)", "y": "매수조건(%)", "color": "Calmar"},
                    text_auto=".2f", aspect="auto",
                    title=f"Calmar Ratio 히트맵 — {_sml} (매수% × 매도%)")
                _fig_heat.add_annotation(
                    x=f"{_scur[1]*100:.2f}%", y=f"{_scur[0]*100:.2f}%",
                    text="★ 현재", showarrow=True, arrowhead=2,
                    font=dict(color="white", size=13, family="Arial Black"))
                _fig_heat.update_layout(height=380)
                st.plotly_chart(_fig_heat, use_container_width=True)
                st.caption("녹색일수록 Calmar Ratio가 높습니다. 현재 파라미터(★) 주변이 고르게 녹색이면 과최적화 위험이 낮습니다.")
        st.divider()

        # ── 무작위 기간 강건성 분석 ──
        st.subheader("🎲 무작위 기간 강건성 분석")
        st.caption(
            "2010~현재까지 1년(252 거래일) 구간 100개를 무작위 추출하여 백테스트를 반복합니다. "
            "시작 시점과 무관하게 전략이 일관된 성과를 내는지 확인합니다.")
        with st.expander("🔍 강건성 분석 실행 (클릭)", expanded=False):
            if st.button("▶ 무작위 100구간 분석 시작", key="dss_mc_run"):
                st.session_state["dss_mc_result"] = None
                with st.spinner("분석 중..."):
                    try:
                        soxl_mc = get_soxl_data()
                        qqq_mc = get_qqq_data()
                    except Exception as _e:
                        st.error(f"⚠️ 데이터 로드 실패: {_e}")
                        return
                    ms_mc = get_mode_series(len(qqq_mc))
                    _mc_closes = soxl_mc['Close'].dropna()
                    _mc_idx = _mc_closes.index
                    _WINDOW = 252
                    _mc_valid = [i for i in range(len(_mc_idx) - _WINDOW)]
                    if len(_mc_valid) < 100:
                        st.warning("데이터가 100구간 분석에 충분하지 않습니다.")
                    else:
                        random.seed(None)
                        _mc_chosen = random.sample(_mc_valid, 100)
                        _mc_strat_ret = []; _mc_strat_mdd = []
                        _mc_bnh_ret = []; _mc_bnh_mdd = []
                        _mc_periods = []
                        _mc_prog = st.progress(0, text="시뮬레이션 중...")
                        _p_mc = DSSParams(
                            sf_divisions=p["sf_div"], sf_max_hold=p["sf_hold"],
                            sf_buy_pct=p["sf_buy"]/100, sf_sell_pct=p["sf_sell"]/100,
                            ag_divisions=p["ag_div"], ag_max_hold=p["ag_hold"],
                            ag_buy_pct=p["ag_buy"]/100, ag_sell_pct=p["ag_sell"]/100,
                            initial_capital=_initial_capital, fee_rate=p["fee_rate"]/100,
                            renewal_period=p["renewal_period"], pcr=p["pcr"]/100, lcr=p["lcr"]/100)
                        for _ci, _si in enumerate(_mc_chosen):
                            _s_dt = str(_mc_idx[_si].date())
                            _e_dt = str(_mc_idx[_si + _WINDOW - 1].date())
                            _bt_mc = run_backtest(_p_mc, soxl_mc, ms_mc, _s_dt, _e_dt)
                            if _bt_mc is not None and not _bt_mc.empty:
                                _a_mc = _bt_mc['총자산'].values.astype(float)
                                _tr_mc = (_a_mc[-1] / _a_mc[0] - 1) * 100
                                _pk_mc = np.maximum.accumulate(_a_mc)
                                _md_mc = abs(float(((_a_mc - _pk_mc) / _pk_mc).min())) * 100
                                _mc_strat_ret.append(round(_tr_mc, 2))
                                _mc_strat_mdd.append(round(_md_mc, 2))
                                # B&H
                                _bh_p = _bt_mc['종가'].values.astype(float)
                                _bh_sh = int(_initial_capital / _bh_p[0])
                                _bh_lo = _initial_capital - _bh_sh * _bh_p[0]
                                _bh_a = _bh_sh * _bh_p + _bh_lo
                                _bnh_tr = (_bh_a[-1] / _bh_a[0] - 1) * 100
                                _bnh_pk = np.maximum.accumulate(_bh_a)
                                _bnh_md = abs(float(((_bh_a - _bnh_pk) / _bnh_pk).min())) * 100
                                _mc_bnh_ret.append(round(_bnh_tr, 2))
                                _mc_bnh_mdd.append(round(_bnh_md, 2))
                                _mc_periods.append((_s_dt, _e_dt))
                            _mc_prog.progress((_ci + 1) / 100, text=f"시뮬레이션 중... {_ci+1}/100")
                        _mc_prog.empty()
                        st.session_state["dss_mc_result"] = {
                            "strat_ret": _mc_strat_ret, "strat_mdd": _mc_strat_mdd,
                            "bnh_ret": _mc_bnh_ret, "bnh_mdd": _mc_bnh_mdd,
                            "periods": _mc_periods}

            _mc_res = st.session_state.get("dss_mc_result")
            if _mc_res and _mc_res["strat_ret"]:
                _sr_arr = np.array(_mc_res["strat_ret"])
                _sm_arr = np.array(_mc_res["strat_mdd"])
                _br_arr = np.array(_mc_res["bnh_ret"]) if _mc_res["bnh_ret"] else None
                _bm_arr = np.array(_mc_res["bnh_mdd"]) if _mc_res["bnh_mdd"] else None
                _n_mc = len(_sr_arr)

                def _mc_stats(arr, label):
                    return {"구분": label, "평균": f"{np.mean(arr):+.1f}%",
                            "중앙값": f"{np.median(arr):+.1f}%", "표준편차": f"{np.std(arr):.1f}%",
                            "최솟값": f"{np.min(arr):+.1f}%", "최댓값": f"{np.max(arr):+.1f}%",
                            "양(+) 비율": f"{(arr > 0).sum() / len(arr) * 100:.0f}%"}

                _mc_rows = [_mc_stats(_sr_arr, "DSS 전략 (1년 수익률)")]
                if _br_arr is not None:
                    _mc_rows.append(_mc_stats(_br_arr, "SOXL B&H (1년 수익률)"))
                _mc_rows.append({"구분": "DSS 전략 (MDD)", "평균": f"{np.mean(_sm_arr):.1f}%",
                                 "중앙값": f"{np.median(_sm_arr):.1f}%", "표준편차": f"{np.std(_sm_arr):.1f}%",
                                 "최솟값": f"{np.min(_sm_arr):.1f}%", "최댓값": f"{np.max(_sm_arr):.1f}%",
                                 "양(+) 비율": "-"})
                if _bm_arr is not None:
                    _mc_rows.append({"구분": "SOXL B&H (MDD)", "평균": f"{np.mean(_bm_arr):.1f}%",
                                     "중앙값": f"{np.median(_bm_arr):.1f}%", "표준편차": f"{np.std(_bm_arr):.1f}%",
                                     "최솟값": f"{np.min(_bm_arr):.1f}%", "최댓값": f"{np.max(_bm_arr):.1f}%",
                                     "양(+) 비율": "-"})
                st.markdown(f"**📋 요약 통계 (n={_n_mc})**")
                st.dataframe(pd.DataFrame(_mc_rows), hide_index=True, use_container_width=True)

                # 100구간 상세 결과
                with st.expander("📄 100구간 상세 결과 보기"):
                    _det_rows = []
                    for _di, (_psd, _ped) in enumerate(_mc_res["periods"]):
                        _row = {"#": _di + 1, "시작일": _psd, "종료일": _ped,
                                "전략 수익률(%)": f"{_mc_res['strat_ret'][_di]:+.1f}%",
                                "전략 MDD(%)": f"{_mc_res['strat_mdd'][_di]:.1f}%"}
                        if _br_arr is not None and _di < len(_mc_res["bnh_ret"]):
                            _row["B&H 수익률(%)"] = f"{_mc_res['bnh_ret'][_di]:+.1f}%"
                            _row["B&H MDD(%)"] = f"{_mc_res['bnh_mdd'][_di]:.1f}%"
                        _det_rows.append(_row)
                    _det_df = pd.DataFrame(_det_rows)
                    def _hl_det(row):
                        try:
                            ret = float(row["전략 수익률(%)"].replace("%", "").replace("+", ""))
                            clr = "background-color: #e8f5e9" if ret > 0 else "background-color: #ffebee"
                            return [clr] * len(row)
                        except Exception:
                            return [""] * len(row)
                    st.dataframe(_det_df.style.apply(_hl_det, axis=1),
                                 hide_index=True, use_container_width=True,
                                 height=min(38 + 35 * len(_det_df), 500))

                # 차트: 수익률 분포 + MDD 분포
                _fig_mc = make_subplots(rows=1, cols=2,
                    subplot_titles=["수익률 분포 (1년)", "최대 낙폭(MDD) 분포"],
                    horizontal_spacing=0.12)
                def _add_mc_hist(fig, arr, color, name, row, col, lg, sl):
                    fig.add_trace(go.Histogram(x=arr.tolist(), nbinsx=20, name=name,
                                               opacity=0.55, marker_color=color,
                                               legendgroup=lg, showlegend=sl), row=row, col=col)
                if _br_arr is not None:
                    _add_mc_hist(_fig_mc, _br_arr, "#FB8C00", "SOXL B&H", 1, 1, "bnh", True)
                if _bm_arr is not None:
                    _add_mc_hist(_fig_mc, _bm_arr, "#FB8C00", "SOXL B&H", 1, 2, "bnh", False)
                _add_mc_hist(_fig_mc, _sr_arr, "#1565C0", "DSS 전략", 1, 1, "strat", True)
                _add_mc_hist(_fig_mc, _sm_arr, "#1565C0", "DSS 전략", 1, 2, "strat", False)
                _fig_mc.add_vline(x=0, line_dash="dash", line_color="#555", row=1, col=1)
                _fig_mc.update_xaxes(title_text="1년 수익률 (%)", row=1, col=1)
                _fig_mc.update_xaxes(title_text="MDD (%)", row=1, col=2)
                _fig_mc.update_yaxes(title_text="빈도 (구간수)", row=1, col=1)
                _fig_mc.update_yaxes(title_text="빈도 (구간수)", row=1, col=2)
                _fig_mc.update_layout(height=420, barmode="overlay",
                    legend=dict(orientation="v", x=1.02, y=1.0, xanchor="left",
                                bgcolor="rgba(255,255,255,0.85)", bordercolor="#ccc", borderwidth=1),
                    margin=dict(r=140))
                st.plotly_chart(_fig_mc, use_container_width=True)
                st.caption("전략이 B&H 대비 수익률 분포가 오른쪽에 집중(높은 수익)되고, "
                           "MDD 분포가 왼쪽에 집중(낮은 손실)될수록 강건한 전략입니다.")

                # 박스플롯
                st.markdown("**📦 박스플롯 — 분포 요약 (중앙값 · 사분위 · 이상치)**")
                _fig_box2 = make_subplots(rows=1, cols=2,
                    subplot_titles=["수익률 박스플롯", "최대 낙폭(MDD) 박스플롯"],
                    horizontal_spacing=0.12)
                if _br_arr is not None:
                    _fig_box2.add_trace(go.Box(y=_br_arr.tolist(), name="SOXL B&H",
                        marker_color="#FB8C00", boxmean=True, legendgroup="bnh_box", showlegend=True), row=1, col=1)
                _fig_box2.add_trace(go.Box(y=_sr_arr.tolist(), name="DSS 전략",
                    marker_color="#1565C0", boxmean=True, legendgroup="strat_box", showlegend=True), row=1, col=1)
                if _bm_arr is not None:
                    _fig_box2.add_trace(go.Box(y=_bm_arr.tolist(), name="SOXL B&H",
                        marker_color="#FB8C00", boxmean=True, legendgroup="bnh_box", showlegend=False), row=1, col=2)
                _fig_box2.add_trace(go.Box(y=_sm_arr.tolist(), name="DSS 전략",
                    marker_color="#1565C0", boxmean=True, legendgroup="strat_box", showlegend=False), row=1, col=2)
                _fig_box2.add_hline(y=0, line_dash="dash", line_color="#888", row=1, col=1)
                _fig_box2.update_yaxes(title_text="수익률 (%)", row=1, col=1)
                _fig_box2.update_yaxes(title_text="MDD (%)", row=1, col=2)
                _fig_box2.update_layout(height=420,
                    legend=dict(orientation="v", x=1.02, y=1.0, xanchor="left",
                                bgcolor="rgba(255,255,255,0.85)", bordercolor="#ccc", borderwidth=1),
                    margin=dict(r=140))
                st.plotly_chart(_fig_box2, use_container_width=True)
                st.caption("박스: 25~75 백분위 구간 / 선: 중앙값 / 삼각형(▲): 평균 / 점: 이상치")
        st.divider()

        # ── 전략 인사이트 ──
        st.subheader("💡 전략 인사이트 & 맥락 참고")
        st.warning("**다음 내용은 SOXL DSS 동파법 백테스트 결과 해석입니다. 과거 성과가 미래 수익을 보장하지 않습니다.**")
        with st.container(border=True):
            st.markdown("""
**왜 DSS 동파법이 SOXL에서 잘 작동하나?**
- **모드 전환 시스템**: QQQ 주간 RSI 기반으로 시장 상황을 자동 판단하여 안전/공세 모드 전환
- **공세모드(AG)**: RSI 저점 반등 시 활성화 — 큰 변동성에서 높은 매수조건으로 저가 매수 기회 포착
- **안전모드(SF)**: RSI 고점 하락 시 활성화 — 작은 매수조건 + 긴 보유로 안정적 수익 추구
- **투자금 갱신(복리)**: 이익의 80%, 손실의 30%만 반영하여 점진적 복리 + 급락 시 방어

**주요 지표 해석**
- **Calmar 1.0 이상**: 우수 / **2.0 이상**: 최상급
- **MDD**: 레버리지 3x ETF(SOXL)는 MDD가 크게 나올 수 있으므로 감내 가능한 수준인지 확인
- **승률**: 높은 승률도 손익비(평균 수익 vs 평균 손실)와 함께 고려 필요

**주의사항**
- 전체 시드가 투입된 상태에서 추가 하락 시 **매수 불가** (빈 시드 없음)
- 급락장(코로나, 금리 충격)에서는 **MDD가 일시적으로 크게 확대**될 수 있음
- 모드 전환은 **2주 지연** 적용 → 급변 시 늦은 대응 가능
- 실제 거래에서는 **슬리피지, 수수료, 세금** 등이 수익률에 영향
- 전략 파라미터를 너무 자주 바꾸면 과최적화(overfitting) 위험
            """)
        st.divider()

        # ── 종합 성과 요약 테이블 ──
        st.subheader("📋 종합 성과 요약")
        _total_sells = len(_sell_recs) if _sell_recs else 0
        _total_wins = len([s for s in _sell_recs if s['pnl'] >= 0]) if _sell_recs else 0
        _avg_pnl_pct = np.mean([(s['pnl'] / (s['buy_price'] * s['qty'])) * 100 for s in _sell_recs]) if _sell_recs else 0
        _max_pnl = max([s['pnl'] for s in _sell_recs]) if _sell_recs else 0
        _min_pnl = min([s['pnl'] for s in _sell_recs]) if _sell_recs else 0
        st.dataframe(pd.DataFrame({
            "항목": ["시작 자본", "최종 자산", "총 수익률", "CAGR (연복리)",
                     "MDD", "Calmar Ratio", "Sharpe Ratio", "Sortino Ratio",
                     "총 매도 횟수", "승률", "평균 손익률",
                     "최대 단일 수익", "최대 단일 손실"],
            "수치": [
                f"${_initial_capital:,.0f}", f"${_final:,.0f}",
                f"{_total_ret*100:+.2f}%", f"{_cagr*100:.2f}%",
                f"{_mdd*100:.2f}%", f"{_calmar:.3f}",
                f"{_sharpe:.3f}", f"{_sortino:.3f}",
                f"{_total_sells}회",
                f"{_total_wins/_total_sells*100:.1f}% ({_total_wins}승 {_total_sells-_total_wins}패)" if _total_sells > 0 else "-",
                f"{_avg_pnl_pct:+.2f}%",
                f"${_max_pnl:+,.2f}", f"${_min_pnl:+,.2f}",
            ],
        }), hide_index=True, use_container_width=True)

    elif st.session_state.get("dss_intro_bt") is None:
        st.info("👆 '성과 분석 실행' 버튼을 클릭하면 현재 파라미터 기준의 성과 분석 결과를 확인할 수 있습니다.\n\n"
               "💡 사이드바 설정값 또는 추천 프리셋을 선택하여 비교 분석할 수 있습니다.")


# ══════════════════════════════════════════════
# 탭 7: 설정
# ══════════════════════════════════════════════

def render_settings_tab():
    """설정 탭 — 텔레그램, 구글시트, 파라미터 관리."""
    st.subheader("⚙️ 개인 설정")

    _cfg_s = _load_dss_config()
    if _IS_CLOUD:
        if st.session_state.get("logged_in"):
            st.success("☁️ **Streamlit Cloud 실행 중** — 설정이 Google Sheets에 영구 저장됩니다.")
        else:
            st.warning("☁️ **Streamlit Cloud 실행 중** — 로그인 후 설정이 영구 저장됩니다.")
    else:
        st.success(f"🖥️ **로컬 PC 실행 중** — 설정이 `{_DSS_CONFIG_PATH}` 에 저장됩니다.")

    # ── 텔레그램 알림 설정 ─────────────────────────────────
    with st.container(border=True):
        col_title_tg, col_help_tg = st.columns([3, 1])
        with col_title_tg:
            st.markdown("#### 💬 텔레그램 알림 설정")
            st.caption("DSS 동파법 주문표를 텔레그램으로 받을 수 있습니다.")
        with col_help_tg:
            with st.popover("❓ Chat ID & Bot Token 확인 방법", use_container_width=True):
                st.markdown("""
<style>
.tg-help-section { margin-bottom: 20px; }
.tg-help-title {
    display: flex; align-items: center; gap: 10px;
    font-size: 17px; font-weight: 700; color: #1a1a2e; margin-bottom: 10px;
}
.tg-help-badge {
    background: #4A90D9; color: white;
    border-radius: 50%; width: 28px; height: 28px;
    display: inline-flex; align-items: center; justify-content: center;
    font-size: 14px; font-weight: 700; flex-shrink: 0;
}
.tg-help-box {
    background: #EEF4FB; border-radius: 10px;
    padding: 14px 18px; font-size: 14px; line-height: 2;
}
.tg-help-box ol { margin: 0; padding-left: 20px; }
.tg-tag {
    background: #D0E8FF; color: #1a5fa8;
    border-radius: 5px; padding: 1px 7px;
    font-family: monospace; font-size: 13px;
}
.tg-code-box {
    background: #1e2533; color: #7dd3fc;
    border-radius: 8px; padding: 10px 14px; margin-top: 8px;
    font-family: monospace; font-size: 12px; word-break: break-all;
    line-height: 1.7;
}
.tg-example-box {
    background: white; border: 1px solid #CBD5E1; border-radius: 8px;
    padding: 12px 16px; margin-top: 10px; font-size: 13px; color: #555;
}
.tg-example-val { color: #4A90D9; font-family: monospace; font-size: 13px; }
.tg-warn-box {
    background: #FFFBEB; border: 1px solid #F59E0B;
    border-radius: 10px; padding: 14px 18px; font-size: 14px; line-height: 2;
}
.tg-warn-title { font-weight: 700; color: #92400E; margin-bottom: 4px; }
.tg-sub-title { font-weight: 700; color: #1a5fa8; margin: 10px 0 4px 0; }
.tg-tip-box {
    background: #F0FDF4; border: 1px solid #86EFAC;
    border-radius: 8px; padding: 10px 14px; margin-top: 8px;
    font-size: 13px; color: #166534;
}
</style>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">1</span> Bot Token 생성하기</div>
  <div class="tg-help-box">
    <ol>
      <li>텔레그램 앱에서 <span class="tg-tag">@BotFather</span> 를 검색합니다.</li>
      <li><span class="tg-tag">/start</span> → <span class="tg-tag">/newbot</span> 입력</li>
      <li><strong>봇 표시 이름</strong> 입력 (예: <span class="tg-tag">DSS 동파법 알림봇</span>)</li>
      <li><strong>봇 username</strong> 입력 — 영문+숫자, <span class="tg-tag">bot</span> 으로 끝나야 함 (예: <span class="tg-tag">dss_soxl_bot</span>)</li>
      <li>성공 시 <strong>HTTP API Token</strong> 발급 → 이것이 <strong>Bot Token</strong></li>
    </ol>
    <div class="tg-example-box">
      <div style="color:#888; font-size:12px; margin-bottom:4px;">Bot Token 예시:</div>
      <div class="tg-example-val">1234567890:ABCdefGHIjklMNOpqrSTUvwxYZ</div>
    </div>
  </div>
</div>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">2</span> 내 봇 시작하기 (필수!)</div>
  <div class="tg-warn-box">
    <div class="tg-warn-title">⚠ 봇을 먼저 시작해야 Chat ID를 확인하고 메시지를 받을 수 있습니다!</div>
    <ol>
      <li>텔레그램에서 내 봇 username 검색 (예: <span class="tg-tag">@dss_soxl_bot</span>)</li>
      <li><span class="tg-tag">/start</span> 클릭 → 아무 메시지 보내기</li>
    </ol>
  </div>
</div>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">3</span> Chat ID 확인하기</div>
  <div class="tg-help-box">
    <div class="tg-sub-title">✅ 방법 1: getUpdates API</div>
    <ol>
      <li>봇에게 메시지 보낸 후 브라우저에 입력:</li>
    </ol>
    <div class="tg-code-box">https://api.telegram.org/bot<span style="color:#fde047;">{토큰값}</span>/getUpdates</div>
    <ol start="2">
      <li>JSON 응답에서 <span class="tg-tag">"id"</span> 값 = <strong>Chat ID</strong></li>
    </ol>
    <div class="tg-sub-title">방법 2: @userinfobot 사용</div>
    <ol><li><span class="tg-tag">@userinfobot</span> 검색 → <span class="tg-tag">/start</span> → Chat ID 확인</li></ol>
  </div>
</div>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">4</span> 연결 테스트</div>
  <div class="tg-tip-box">
    💡 Bot Token과 Chat ID를 입력한 후 <strong>📨 주문표 테스트 발송</strong> 버튼을 눌러보세요.<br>
    메시지가 정상적으로 수신되면 설정 완료입니다! ✅
  </div>
</div>
""", unsafe_allow_html=True)

        c1_tg, c2_tg = st.columns(2)
        tg_chat_id = c1_tg.text_input(
            "텔레그램 Chat ID",
            value=_cfg_s.get("tg_chat_id", ""),
            placeholder="예: 1234567890",
            key="dss_tg_chat_id",
        )
        tg_token = c2_tg.text_input(
            "Bot Token",
            value=_cfg_s.get("tg_token", ""),
            placeholder="예: 123456789:AAF...",
            type="password",
            key="dss_tg_token",
        )

        btn_tg1, btn_tg2, spacer_tg = st.columns([1, 1, 4])
        with btn_tg1:
            if st.button("📨 주문표 테스트 발송", use_container_width=True, key="dss_tg_test"):
                if not tg_chat_id or not tg_token:
                    st.warning("Chat ID와 Bot Token을 먼저 입력해주세요.")
                else:
                    # 모든 계좌의 주문표 result 수집
                    _tg_accounts = []
                    _cfg_tg = _load_dss_config()
                    _acct_names = list(_cfg_tg.get("accounts", {}).keys())
                    for _ai, _aname in enumerate(_acct_names):
                        _rkey = f"dss_a{_ai}_result"
                        _res = st.session_state.get(_rkey)
                        if _res is not None:
                            _tg_accounts.append((_aname, _res))

                    if not _tg_accounts:
                        st.warning("⚠️ 주문표 탭에서 먼저 '주문표 로드'를 실행해주세요.")
                    else:
                        _ok_cnt = 0
                        _fail_cnt = 0
                        with st.spinner(f"발송 중... ({len(_tg_accounts)}개 계좌)"):
                            for _aname, _ares in _tg_accounts:
                                msg = _build_dss_order_text(_ares, _aname)
                                result = _send_telegram(tg_token, tg_chat_id, msg)
                                if result.get("ok"):
                                    _ok_cnt += 1
                                else:
                                    _fail_cnt += 1
                        if _fail_cnt == 0:
                            st.success(f"✅ {_ok_cnt}개 계좌 발송 성공!")
                        else:
                            st.warning(f"발송 결과: 성공 {_ok_cnt}개 / 실패 {_fail_cnt}개")

        with btn_tg2:
            if st.button("💾 저장하기", use_container_width=True, key="dss_tg_save", type="primary"):
                if not tg_chat_id or not tg_token:
                    st.warning("Chat ID와 Bot Token을 모두 입력해주세요.")
                else:
                    _cfg_s["tg_chat_id"] = tg_chat_id
                    _cfg_s["tg_token"] = tg_token
                    _save_dss_config(_cfg_s)
                    _save_loc = "Google Sheets" if (_IS_CLOUD and st.session_state.get("logged_in")) else _DSS_CONFIG_PATH
                    st.success(f"✅ 저장 완료! `{_save_loc}`")

    st.write("")

    # ── 구글 스프레드시트 연동 ──────────────────────────────
    with st.container(border=True):
        col_title_gs, col_help_gs = st.columns([3, 1])
        with col_title_gs:
            st.markdown("#### 🗂️ 구글 스프레드시트 연동")
            st.caption("DSS 동파법 주문표를 구글 스프레드시트로 전송합니다.")
        with col_help_gs:
            with st.popover("❓ 구글 스프레드시트 설정 방법", use_container_width=True):
                st.markdown("""
<style>
.gs-help-section { margin-bottom: 20px; }
.gs-help-title {
    display: flex; align-items: center; gap: 10px;
    font-size: 17px; font-weight: 700; color: #1a1a2e; margin-bottom: 10px;
}
.gs-help-badge {
    background: #2EAA5E; color: white;
    border-radius: 50%; width: 28px; height: 28px;
    display: inline-flex; align-items: center; justify-content: center;
    font-size: 14px; font-weight: 700; flex-shrink: 0;
}
.gs-help-box {
    background: #EDF7F0; border-radius: 10px;
    padding: 14px 18px; font-size: 14px; line-height: 2;
}
.gs-help-box ol { margin: 0; padding-left: 20px; }
.gs-tag {
    background: #D4EFE0; color: #1a6e3c;
    border-radius: 5px; padding: 1px 7px;
    font-family: monospace; font-size: 13px;
}
.gs-example-box {
    background: white; border: 1px solid #CBD5E1; border-radius: 8px;
    padding: 12px 16px; margin-top: 10px; font-size: 13px; color: #555;
}
.gs-example-val { color: #2EAA5E; font-family: monospace; font-size: 13px; }
.gs-warn-box {
    background: #FFFBEB; border: 1px solid #F59E0B;
    border-radius: 10px; padding: 14px 18px; font-size: 14px; line-height: 2;
}
.gs-warn-title { font-weight: 700; color: #92400E; margin-bottom: 6px; }
.gs-email-box {
    background: white; border: 1px solid #CBD5E1; border-radius: 8px;
    padding: 10px 14px; margin: 8px 0 12px 0; font-size: 13px; color: #555;
}
.gs-email-val { color: #2EAA5E; font-family: monospace; font-size: 13px; font-weight: 700; }
</style>

<div class="gs-help-section">
  <div class="gs-help-title"><span class="gs-help-badge">1</span> 새 스프레드시트 만들기</div>
  <div class="gs-help-box">
    <ol>
      <li><a href="https://sheets.google.com" target="_blank">Google Sheets</a>에서 새 시트 생성</li>
      <li>시트 이름 지정 (예: DSS 동파법 포트폴리오)</li>
    </ol>
  </div>
</div>

<div class="gs-help-section">
  <div class="gs-help-title"><span class="gs-help-badge">2</span> URL 복사</div>
  <div class="gs-help-box">
    <div>브라우저 주소창의 URL을 복사합니다.</div>
    <div class="gs-example-box">
      <div class="gs-example-val">https://docs.google.com/spreadsheets/d/1ABC...XYZ/edit</div>
    </div>
  </div>
</div>

<div class="gs-help-section">
  <div class="gs-help-title"><span class="gs-help-badge">3</span> 서비스 계정 권한 부여 (중요!)</div>
  <div class="gs-warn-box">
    <div class="gs-warn-title">⚠ 아래 이메일에 편집 권한을 부여해야 합니다.</div>
    <div class="gs-email-box">
      <div style="color:#888; font-size:12px; margin-bottom:4px;">서비스 계정 이메일:</div>
      <div class="gs-email-val">connectspreadsheet@sodium-gateway-485307-f3.iam.gserviceaccount.com</div>
    </div>
    <ol>
      <li>스프레드시트 우측 상단 <span class="gs-tag">공유</span> 클릭</li>
      <li>위 이메일을 <span class="gs-tag">편집자</span> 로 추가</li>
    </ol>
  </div>
</div>
""", unsafe_allow_html=True)

        gs_url = st.text_input(
            "스프레드시트 URL",
            value=_cfg_s.get("gs_url", ""),
            placeholder="https://docs.google.com/spreadsheets/d/...",
            key="dss_gs_url",
        )
        st.caption("* 스프레드시트에 서비스 계정 이메일을 편집자로 공유해주세요. (우측 상단 도움말 참고)")

        gs_template = st.text_input(
            "📄 템플릿 워크시트 이름 (선택)",
            value=_cfg_s.get("gs_template", ""),
            placeholder="예: DSS-pjh",
            key="dss_gs_template",
            help="계좌 시트가 없을 때 이 템플릿을 복제해서 자동 생성합니다. 비워두면 빈 시트 생성.",
        )

        # ── 계좌별 워크시트 매핑 ──────────────────────────────
        _accounts_s = _cfg_s.get("accounts", {})
        _acct_names_s = list(_accounts_s.keys())

        # 레거시 gs_sheet 단일 필드 → gs_sheets 딕셔너리로 마이그레이션
        _gs_sheets = dict(_cfg_s.get("gs_sheets", {}))
        _legacy_sheet = _cfg_s.get("gs_sheet")
        if _legacy_sheet and not _gs_sheets and _acct_names_s:
            _gs_sheets[_acct_names_s[0]] = _legacy_sheet

        st.markdown("##### 📋 계좌별 워크시트 매핑")
        if not _acct_names_s:
            st.info("등록된 계좌가 없습니다. 주문표 탭에서 먼저 계좌를 추가하세요.")
            _gs_sheet_inputs = {}
        else:
            st.caption("각 계좌의 주문표를 전송할 워크시트(탭) 이름을 지정합니다. 없으면 자동 생성됩니다.")
            _gs_sheet_inputs = {}
            for _an in _acct_names_s:
                _default_sheet = _gs_sheets.get(_an, _an)
                _gs_sheet_inputs[_an] = st.text_input(
                    f"📊 {_an}",
                    value=_default_sheet,
                    placeholder=f"예: {_an}_SOXL",
                    key=f"dss_gs_sheet_{_an}",
                )

        btn_gs1, btn_gs2, btn_gs3 = st.columns(3)
        with btn_gs1:
            if st.button("🔗 시트 연결 테스트", use_container_width=True, key="dss_gs_test"):
                if not gs_url:
                    st.warning("스프레드시트 URL을 먼저 입력해주세요.")
                else:
                    try:
                        gc = _get_gspread_client()
                        sh = gc.open_by_url(gs_url)
                        st.success(f"✅ 연결 성공! 스프레드시트: **{sh.title}**")
                    except FileNotFoundError as e:
                        st.error(f"❌ {e}")
                    except Exception as e:
                        st.error(f"❌ 연결 실패: {e}")

        with btn_gs2:
            if st.button("📊 주문 시트 전송", use_container_width=True, key="dss_gs_send", type="primary"):
                if not gs_url:
                    st.warning("스프레드시트 URL을 먼저 입력해주세요.")
                elif not _gs_sheet_inputs:
                    st.warning("⚠️ 등록된 계좌가 없습니다.")
                else:
                    _sent = 0
                    _skipped = 0
                    _failed = []
                    with st.spinner("계좌별 주문표 전송 중..."):
                        for _ai_s, _an_s in enumerate(_acct_names_s):
                            _sheet_s = _gs_sheet_inputs.get(_an_s, "").strip()
                            if not _sheet_s:
                                _skipped += 1
                                continue
                            _rkey_s = f"dss_a{_ai_s}_result"
                            _gs_os = st.session_state.get(_rkey_s)
                            if _gs_os is None:
                                _skipped += 1
                                continue
                            try:
                                n = _write_dss_orders_to_sheet(gs_url, _sheet_s, _gs_os, gs_template)
                                st.success(f"✅ [{_an_s}] → '{_sheet_s}' 탭 L4에 {n}건 전송 완료")
                                _sent += 1
                            except FileNotFoundError as e:
                                st.error(f"❌ [{_an_s}]: {e}")
                                _failed.append(_an_s)
                            except Exception as e:
                                st.error(f"❌ [{_an_s}] 전송 실패: {e}")
                                _failed.append(_an_s)
                    if _sent == 0 and _skipped > 0 and not _failed:
                        st.warning("⚠️ 주문표 탭에서 계좌별 '주문표 로드'를 먼저 실행해주세요.")
                    elif _skipped > 0:
                        st.caption(f"ℹ️ 주문표가 없거나 시트명이 비어 있어 {_skipped}개 계좌는 건너뜀")

        with btn_gs3:
            if st.button("💾 저장하기 ", use_container_width=True, key="dss_gs_save", type="primary"):
                if not gs_url:
                    st.warning("스프레드시트 URL을 입력해주세요.")
                else:
                    _cfg_s["gs_url"] = gs_url
                    _cfg_s["gs_template"] = gs_template.strip()
                    _cfg_s["gs_sheets"] = {
                        _an: _sh.strip() for _an, _sh in _gs_sheet_inputs.items() if _sh.strip()
                    }
                    # 레거시 필드 정리
                    _cfg_s.pop("gs_sheet", None)
                    _save_dss_config(_cfg_s)
                    st.success(f"✅ URL / 템플릿 / 계좌별 시트 매핑 {len(_cfg_s['gs_sheets'])}건 저장 완료!")

    st.write("")

    # ── 파라미터 프리셋 내보내기/가져오기 ──────────────────────
    with st.container(border=True):
        st.markdown("#### 📋 파라미터 설정 관리")
        st.caption("현재 사이드바 파라미터를 JSON으로 내보내거나, 저장된 설정을 확인합니다.")

        _cur_params_dict = {
            "sf_div": st.session_state.get("dss_sf_div", 7),
            "sf_hold": st.session_state.get("dss_sf_hold", 30),
            "sf_buy": st.session_state.get("dss_sf_buy", 3.0),
            "sf_sell": st.session_state.get("dss_sf_sell", 0.2),
            "ag_div": st.session_state.get("dss_ag_div", 7),
            "ag_hold": st.session_state.get("dss_ag_hold", 7),
            "ag_buy": st.session_state.get("dss_ag_buy", 5.0),
            "ag_sell": st.session_state.get("dss_ag_sell", 2.5),
            "pcr": st.session_state.get("dss_pcr", 80),
            "lcr": st.session_state.get("dss_lcr", 30),
            "renewal_period": st.session_state.get("dss_renew", 10),
            "fee_rate": st.session_state.get("dss_fee", 0.04),
            "initial_capital": st.session_state.get("dss_capital", 100000),
        }

        _prm1, _prm2 = st.columns(2)
        with _prm1:
            st.markdown("**현재 사이드바 파라미터**")
            st.json(_cur_params_dict)
        with _prm2:
            st.markdown("**저장된 설정 (config.json)**")
            _saved = _cfg_s.get("params", {})
            if _saved:
                st.json(_saved)
            else:
                st.info("주문표 탭에서 파라미터를 저장하면 여기에 표시됩니다.")

        if st.button("📥 현재 파라미터를 설정 파일에 저장", key="dss_save_params_to_config"):
            _cfg_s["params"] = _cur_params_dict
            _save_dss_config(_cfg_s)
            st.success("✅ 현재 사이드바 파라미터가 config.json에 저장되었습니다.")
