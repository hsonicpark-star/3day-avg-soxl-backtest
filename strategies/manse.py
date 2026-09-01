"""
strategies/manse.py — 만능 스위치 매매 (쪼꼬야옹 만능시트 v2.1) 전략 모듈

원본 구글시트의 RECORD/DB 탭 수식을 그대로 이식한 manse_engine 을 UI로 감싼다.
(검증: 이평선·중심주가 두 세팅 모두 시트와 3,932 거래일 전 구간 일치)

인터페이스: DSS/IUO 와 동일 패턴 (자체 사이드바 사용)
    render_sidebar() → params dict
    render_backtest_tab(params)
    render_optimization_tab(params)
    render_ordersheet_tab(params)
    render_intro_tab(params)
    render_db_tab(params)
    render_settings_tab()
"""

from __future__ import annotations

import copy
import io as _io
import itertools
import json
import os
import random
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

_root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _root_dir not in sys.path:
    sys.path.insert(0, _root_dir)

from manse_engine import (  # noqa: E402
    LEVELS, MODE_BASES, TIER_METHODS, ORDER_TYPES, CENTER_PRESETS,
    ManseParams, LevelParam, TierParam,
    default_params, params_to_dict, params_from_dict,
    build_mode_frame, run_backtest, build_order_plan, center_price,
)
from common.config import (  # noqa: E402
    _IS_CLOUD, _CONFIG, load_config, save_config, _get_gspread_client,
)
from common.auth import _save_user_settings_to_sheet, _hash_password  # noqa: E402
from common.telegram import _send_telegram, render_telegram_help_popover  # noqa: E402
from common.analysis import (  # noqa: E402
    recalc_adj_history, monthly_perf_table,
)
from common import pricedb  # noqa: E402

_CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".manse")
_CONFIG_PATH = os.path.join(_CONFIG_DIR, "config.json")

DATA_SOURCES = (
    "로컬 DB + 야후 최신분 (권장)",
    "자동 (야후 → 로컬 DB)",
    "야후 파이낸스 (yfinance)",
    "로컬 DB (백업 파일)",
)


# ══════════════════════════════════════════════════════════
# 설정 저장/로드
# ══════════════════════════════════════════════════════════
def _load_cfg() -> dict:
    if _IS_CLOUD and st.session_state.get("logged_in"):
        raw = st.session_state.get("user_settings", {}).get("manse_config", "")
        if raw:
            try:
                cfg = json.loads(raw) if isinstance(raw, str) else raw
                return cfg if isinstance(cfg, dict) else {}
            except Exception:
                pass
        return {}
    if os.path.exists(_CONFIG_PATH):
        try:
            with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _get_tg_creds() -> tuple:
    """(token, chat_id) — Cloud: user_settings / 로컬: ~/.usd-avg/config.json"""
    if _IS_CLOUD and st.session_state.get("logged_in"):
        u = st.session_state.get("user_settings", {})
        return (str(u.get("ms_tg_token", "")).strip(),
                str(u.get("ms_tg_chat_id", "")).strip())
    c = load_config()
    return (str(c.get("ms_tg_token", "")).strip(),
            str(c.get("ms_tg_chat_id", "")).strip())


def _default_sheet_name(acct_name: str, ticker: str) -> str:
    """계좌별 기본 구글시트 탭 이름.

    ⚠️ 티커만 쓰면 같은 종목 계좌 2개가 **같은 탭에 서로 덮어쓴다**.
       계좌명을 붙여 고유하게 만든다.
    """
    return f"manse_{str(acct_name).strip()}" if acct_name else str(ticker).upper()


def _ms_accounts() -> dict:
    """만능 스위치 계좌 목록 → {계좌명: {ticker, gs_sheet}}."""
    cfg = _load_cfg()
    out = {}
    for name, a in (cfg.get("accounts") or {}).items():
        if not isinstance(a, dict):
            continue
        tk = str(a.get("ticker", "")).upper()
        out[name] = {"ticker": tk,
                     "gs_sheet": str(a.get("gs_sheet")
                                     or _default_sheet_name(name, tk))}
    return out


def _save_cfg(cfg: dict):
    try:
        os.makedirs(_CONFIG_DIR, exist_ok=True)
        with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            js = json.dumps(cfg, ensure_ascii=False)
            st.session_state.setdefault("user_settings", {})
            st.session_state.user_settings["manse_config"] = js
            from common.auth import _save_user_settings_to_sheet
            _save_user_settings_to_sheet(st.session_state.username,
                                         {"manse_config": js})
        except Exception as e:
            st.warning(f"⚠️ Cloud 저장 실패 (로컬에는 저장됨): {e}")


# ══════════════════════════════════════════════════════════
# 데이터 로드
# ══════════════════════════════════════════════════════════
@st.cache_data(ttl=1800, show_spinner=False)
def _load_one(ticker: str, source: str, gs_url: str = "") -> pd.DataFrame:
    """티커 1개의 종가 시계열. source 에 따라 야후/로컬 DB/구글시트를 선택.

    gs_url 이 있으면 야후 실패 + 로컬 DB 노후 시 구글시트로 한 번 더 폴백한다
    (클라우드는 파일시스템이 임시라 시트가 실질적인 영구 저장소).
    """
    tk = ticker.strip().upper()
    if source == "로컬 DB (백업 파일)":
        return pricedb.load_prices(tk)
    if source == "로컬 DB + 야후 최신분 (권장)":
        return pricedb.load_prices_resilient(tk, gs_url=gs_url)

    from common.data import _download_price
    df = pd.DataFrame()
    try:
        df = _download_price(tk, "2009-01-01",
                             (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d"))
    except Exception:
        df = pd.DataFrame()

    # common.data._download_price 는 야후 3회 실패 시 이미 로컬 DB로 폴백한다
    # (attrs['from_pricedb']). 그래도 비어 있으면 여기서 한 번 더 시도.
    if (df is None or df.empty) and source == "자동 (야후 → 로컬 DB)":
        db = pricedb.load_prices(tk)
        if not db.empty:
            db.attrs["from_pricedb"] = True
            return db
    if df is None:
        df = pd.DataFrame()
    return df


def _gs_url() -> str:
    """사용자 스프레드시트 URL — 클라우드는 user_settings, 로컬은 공통 config."""
    try:
        if _IS_CLOUD and st.session_state.get("logged_in"):
            return str(st.session_state.get("user_settings", {})
                       .get("gs_url", "")).strip()
        return str(load_config().get("gs_url", "")).strip()
    except Exception:
        return ""


def _load_prices(tickers, source: str) -> dict:
    gs_url = _gs_url()
    out = {}
    for tk in tickers:
        out[tk.upper()] = _load_one(tk, source, gs_url)
    return out


def _data_health(prices: dict) -> list:
    msgs = []
    for tk, df in prices.items():
        if df is None or df.empty:
            msgs.append(f"⛔ **{tk}** 가격 데이터를 불러오지 못했습니다.")
        elif df.attrs.get("from_pricedb") or df.attrs.get("source"):
            last = df.index[-1].date()
            src = df.attrs.get("source")
            app = df.attrs.get("appended", 0)
            exp = df.attrs.get("expected")
            if not src:
                src = f"로컬DB{f' → 야후+{app}행' if app else ''}"
            # ⚠️ attrs["stale"] 을 그대로 믿지 않는다.
            # _load_one 은 @st.cache_data(ttl=1800) 이라, 배포 직후에는
            # '옛 코드가 만든 attrs' 가 최대 30분간 그대로 반환될 수 있다.
            # 표시 시점에 마지막 날짜로 직접 다시 판정한다.
            try:
                stale = pricedb._is_stale(df.index[-1])
                exp = exp if exp is not None else pricedb.expected_latest_close_date()
            except Exception:
                stale = bool(df.attrs.get("stale"))
            if stale:
                # 확정 거래일보다 뒤처진 '진짜' 낡음
                _e = f"확정 거래일 {pd.Timestamp(exp).date()} 대비 " if exp is not None else ""
                msgs.append(f"⚠️ **{tk}** 최신 종가 미확보 — {_e}최종 {last} "
                            f"({src}). 주문표가 낡을 수 있습니다.")
            elif "시트" in src:
                msgs.append(f"⚠️ **{tk}** 야후 실패 → {src} (최종 {last}, 최신)")
            else:
                msgs.append(f"ℹ️ **{tk}** {src} — 최종 {last} (최신)")
    return msgs


def _show_health(msgs):
    """데이터 상태 메시지를 심각도에 맞는 위젯으로 표시.

    ⛔ = error, ⚠️ = warning, 그 외(ℹ️) = info.
    전부 st.warning 으로 그리면 '최신 확보' 같은 정상 안내까지
    노란 경고 박스로 보여 사용자가 문제로 오인한다.
    """
    for m in msgs or []:
        (st.error if m.startswith("⛔") else
         st.warning if m.startswith("⚠️") else st.info)(m)



# ══════════════════════════════════════════════════════════
# 파라미터 <-> 표
# ══════════════════════════════════════════════════════════
def _tier_df(lp: LevelParam) -> pd.DataFrame:
    rows = []
    for i in range(max(lp.split, len(lp.tiers))):
        t = lp.tiers[i] if i < len(lp.tiers) else TierParam(0.0, 0.0, 0.02, 20)
        rows.append({"티어": i + 1,
                     "시드비중(%)": round(t.seed_w * 100, 4),
                     "매수목표(%)": round(t.buy_gap * 100, 4),
                     "매도목표(%)": round(t.sell_gap * 100, 4),
                     "손절일수": int(t.stop_days)})
    return pd.DataFrame(rows)


def _df_to_tiers(df: pd.DataFrame) -> list:
    out = []
    for _, r in df.iterrows():
        try:
            out.append(TierParam(
                seed_w=float(r["시드비중(%)"]) / 100.0,
                buy_gap=float(r["매수목표(%)"]) / 100.0,
                sell_gap=float(r["매도목표(%)"]) / 100.0,
                stop_days=int(r["손절일수"]),
            ))
        except Exception:
            continue
    return out


# ══════════════════════════════════════════════════════════
# 사이드바
# ══════════════════════════════════════════════════════════
def render_sidebar() -> dict:
    cfg = _load_cfg()

    st.subheader("🎛️ 만능 스위치 매매")

    # ── 종목 ──
    _TK = ["SOXL", "TQQQ", "QLD", "USD", "직접입력"]
    sel = st.selectbox("투자 종목", _TK, index=0, key="ms_tk_sel")
    if sel == "직접입력":
        ticker = st.text_input("티커 직접 입력", value="SOXL",
                               key="ms_tk_in").strip().upper() or "SOXL"
    else:
        ticker = sel

    saved = cfg.get(ticker, {})
    p = params_from_dict(saved.get("params")) if saved.get("params") else default_params()
    p.ticker = ticker

    src = st.radio("📂 종가 데이터 소스", DATA_SOURCES,
                   index=DATA_SOURCES.index(
                       saved.get("data_source", DATA_SOURCES[0]))
                   if saved.get("data_source") in DATA_SOURCES else 0,
                   key="ms_src",
                   help="로컬 DB 는 만능시트 DB 탭에서 가져온 백업 파일입니다. "
                        "야후 장애 시 대안으로 쓰거나, 시트와 완전히 동일한 "
                        "결과를 재현할 때 사용하세요.")
    if src == "로컬 DB (백업 파일)":
        st.caption("✅ 시트와 동일한 무조정 종가 — 원본 결과를 그대로 재현합니다. "
                   "다만 DB를 갱신하지 않으면 최신 며칠이 비어 주문표가 낡을 수 있습니다.")
    elif src == "로컬 DB + 야후 최신분 (권장)":
        st.caption("✅ 과거는 로컬 DB(무조정, 시트와 동일) + DB 이후 구간만 야후에서 이어붙임. "
                   "최근 구간은 배당 조정계수가 1.0이라 불연속이 없습니다. "
                   "**주문표·자동발송은 이 소스를 쓰세요.**")
    else:
        st.caption("⚠️ 야후는 **배당조정 종가**라 시트(무조정)와 결과가 다릅니다. "
                   "시트 검증용으로는 로컬 DB 를 쓰세요.")

    st.markdown("---")

    # ── 스위치 세팅 ──
    st.markdown("##### 🔀 스위치 세팅 (모드 판정)")
    p.mode_basis = st.selectbox("모드 판단 기준", MODE_BASES,
                                index=MODE_BASES.index(p.mode_basis),
                                key="ms_basis")

    if p.mode_basis == "중심주가":
        p.center_ticker = st.selectbox("중심주가 종목", list(CENTER_PRESETS.keys()),
                                       index=(list(CENTER_PRESETS).index(p.center_ticker)
                                              if p.center_ticker in CENTER_PRESETS else 1),
                                       key="ms_ctk")
        c1, c2 = st.columns(2)
        p.center_low = c1.number_input("바닥 범위 (%)", value=p.center_low * 100,
                                       step=0.25, format="%.2f", key="ms_clo") / 100
        p.center_high = c2.number_input("천장 범위 (%)", value=p.center_high * 100,
                                        step=0.25, format="%.2f", key="ms_chi") / 100
        st.caption("주봉종가 ÷ 중심주가(월복리 추세선) − 1 = 이격도")
    elif p.mode_basis == "RSI":
        p.rsi_ticker = st.text_input("RSI 종목", value=p.rsi_ticker,
                                     key="ms_rtk").strip().upper() or "QQQ"
        p.rsi_period = int(st.number_input("RSI 기간 (주)", value=int(p.rsi_period),
                                           min_value=2, max_value=52, step=1,
                                           key="ms_rper"))
        c1, c2 = st.columns(2)
        p.rsi_low = c1.number_input("바닥 RSI", value=float(p.rsi_low),
                                    step=1.0, key="ms_rlo")
        p.rsi_high = c2.number_input("천장 RSI", value=float(p.rsi_high),
                                     step=1.0, key="ms_rhi")
        st.caption("주봉 종가 기준 단순평균 RSI (시트와 동일)")
    else:
        p.ma_ticker = st.text_input("이평선 종목", value=p.ma_ticker,
                                    key="ms_mtk").strip().upper() or "QQQ"
        p.ma_days = int(st.number_input("이평선 일수", value=int(p.ma_days),
                                        min_value=5, max_value=400, step=5,
                                        key="ms_mad"))
        c1, c2 = st.columns(2)
        p.ma_low = c1.number_input("바닥 범위 (%)", value=p.ma_low * 100,
                                   step=0.25, format="%.2f", key="ms_mlo") / 100
        p.ma_high = c2.number_input("천장 범위 (%)", value=p.ma_high * 100,
                                    step=0.25, format="%.2f", key="ms_mhi") / 100
        st.caption("주봉종가 ÷ N일 이동평균 − 1 = 이격도")

    p.tier_method = st.selectbox("티어계산 방식", TIER_METHODS,
                                 index=TIER_METHODS.index(p.tier_method),
                                 key="ms_tmeth",
                                 help="보유 = 현재 보유 건수 + 1 / "
                                      "빈자리 = 비어 있는 가장 낮은 티어")

    st.markdown("---")

    # ── 구간별 파라미터 ──
    st.markdown("##### 📐 구간별 파라미터")
    for lv in LEVELS:
        lp = p.levels.get(lv) or LevelParam()
        with st.expander(f"{lv} 구간", expanded=(lv == "바닥")):
            lp.split = int(st.number_input(
                "시드분할수", value=int(lp.split), min_value=1, max_value=10,
                step=1, key=f"ms_{lv}_split"))
            c1, c2, c3 = st.columns(3)
            lp.fixed_qty = c1.checkbox("정량매수", value=lp.fixed_qty,
                                       key=f"ms_{lv}_fq")
            lp.no_buy_on_sell_day = c2.checkbox("매도날 매수X",
                                                value=lp.no_buy_on_sell_day,
                                                key=f"ms_{lv}_nb")
            lp.moc_day_buy = c3.checkbox("MOC날 매수", value=lp.moc_day_buy,
                                         key=f"ms_{lv}_mb")
            edited = st.data_editor(
                _tier_df(lp), key=f"ms_{lv}_tiers", hide_index=True,
                num_rows="fixed", use_container_width=True,
                column_config={
                    "티어": st.column_config.NumberColumn(disabled=True, width="small"),
                    "시드비중(%)": st.column_config.NumberColumn(format="%.2f"),
                    "매수목표(%)": st.column_config.NumberColumn(format="%.3f"),
                    "매도목표(%)": st.column_config.NumberColumn(format="%.3f"),
                    "손절일수": st.column_config.NumberColumn(format="%d"),
                })
            lp.tiers = _df_to_tiers(edited)
            tot = sum(t.seed_w for t in lp.tiers) * 100
            (st.caption if abs(tot - 100) < 0.5 else st.warning)(
                f"시드비중 합계: {tot:.2f}%")
        p.levels[lv] = lp

    st.markdown("---")

    # ── 투자 정보 ──
    st.markdown("##### 💰 투자 정보")
    c1, c2 = st.columns(2)
    start_date = c1.date_input("시작일",
                               pd.to_datetime(saved.get("bt_start", "2011-01-03")).date(),
                               key="ms_start")
    end_date = c2.date_input("종료일", datetime.today().date(), key="ms_end")
    p.principal = st.number_input("최초 원금 ($)", value=float(p.principal),
                                  step=1000.0, key="ms_cap")
    c3, c4 = st.columns(2)
    p.profit_comp = c3.number_input("이익 복리율 (%)", value=p.profit_comp * 100,
                                    step=1.0, key="ms_pc") / 100
    p.loss_comp = c4.number_input("손실 복리율 (%)", value=p.loss_comp * 100,
                                  step=1.0, key="ms_lc") / 100
    p.renew_cycle = int(st.number_input("투자금 갱신 주기 (거래일)",
                                        value=int(p.renew_cycle), min_value=1,
                                        max_value=250, step=1, key="ms_renew"))

    c5, c6 = st.columns(2)
    p.fee = c5.number_input(
        "수수료 (%)", value=p.fee * 100, step=0.01, format="%.4f", key="ms_fee",
        help="매수·매도 각각에 부과되는 편도 수수료율. "
             "왕복 비용은 이 값의 2배입니다.") / 100
    p.sec_fee = c6.number_input("SEC FEE (%)", value=p.sec_fee * 100,
                                step=0.0001, format="%.5f", key="ms_sec",
                                help="매도 시에만 부과") / 100
    if p.fee <= 0:
        st.caption("💡 현재 수수료 0% — 2027년부터 0.07% 예정이면 미리 넣고 "
                   "비교해 보세요. 보유기간이 짧은 전략이라 결과가 크게 달라집니다.")
    else:
        st.caption(f"왕복 비용 ≈ {p.fee*2*100:.3f}% (+ 매도 SEC {p.sec_fee*100:.4f}%)")

    with st.expander("추가 주문 방식", expanded=False):
        p.extra_range = st.number_input("추가 매수 범위 (%)",
                                        value=p.extra_range * 100, step=1.0,
                                        format="%.2f", key="ms_ext") / 100
        p.order_type = st.selectbox("추가 주문 방식", ORDER_TYPES,
                                    index=ORDER_TYPES.index(p.order_type),
                                    key="ms_otype")
        if p.order_type == "추가 주문 건수 고정":
            p.extra_count = int(st.number_input("추가 주문 건수",
                                                value=int(p.extra_count),
                                                min_value=1, max_value=50,
                                                step=1, key="ms_ecnt"))
        else:
            p.extra_step = int(st.number_input("추가 매수 간격 (주)",
                                               value=int(p.extra_step),
                                               min_value=1, max_value=1000,
                                               step=1, key="ms_estep"))

    out = {
        "bt_ticker": ticker,
        "bt_start_date": start_date,
        "bt_end_date": end_date,
        "bt_initial_capital": p.principal,
        "data_source": src,
        "mp": p,
    }
    # 개인 설정 탭(인자 없이 호출됨)에서 현재 사이드바 값을 저장할 수 있도록 공유
    st.session_state["ms_last_params"] = out
    return out


# ══════════════════════════════════════════════════════════
# 공통 헬퍼
# ══════════════════════════════════════════════════════════
_MODE_COLOR = {"바닥": "#2E86DE", "중간": "#8E8E93", "천장": "#E74C3C", "": "#DDDDDD"}
_MODE_BG = {"바닥": "#E8F1FC", "중간": "#F2F2F3", "천장": "#FDECEA", "": "#FAFAFA"}
_POS, _NEG, _DIM = "#2E7D32", "#C62828", "#888"


def _cards(items: list, mb: int = 12):
    """DSS 주문표와 동일한 플렉스 카드 행 렌더링.

    items: [{"label", "value", "sub"(선택), "sub_color"(선택),
             "bg"(선택), "fg"(선택), "flex"(선택)}]
    """
    html = ['<div style="display:flex;gap:10px;margin-bottom:%dpx">' % mb]
    for it in items:
        bg = it.get("bg", "#FAFAFA")
        border = "none" if it.get("bg") else "1px solid #EEE"
        fg = it.get("fg", "#333")
        sub = ""
        if it.get("sub"):
            sub = ('<div style="font-size:0.68em;color:%s;font-weight:600">%s</div>'
                   % (it.get("sub_color", _DIM), it["sub"]))
        html.append(
            '<div style="flex:%s;background:%s;border:%s;border-radius:10px;'
            'padding:12px 14px;text-align:center;min-width:0">'
            '<div style="font-size:0.72em;color:%s;margin-bottom:2px;'
            'white-space:nowrap;overflow:hidden;text-overflow:ellipsis">%s</div>'
            '<div style="font-size:1.1em;font-weight:700;color:%s;'
            'white-space:nowrap;overflow:hidden;text-overflow:ellipsis">%s</div>'
            '%s</div>'
            % (it.get("flex", 1), bg, border, _DIM, it["label"], fg,
               it["value"], sub))
    html.append("</div>")
    st.markdown("".join(html), unsafe_allow_html=True)


def _sign_color(v) -> str:
    try:
        return _POS if float(v) >= 0 else _NEG
    except (TypeError, ValueError):
        return _DIM


def _run(params: dict, light: bool = False):
    p: ManseParams = params["mp"]
    prices = _load_prices(p.needed_tickers(), params["data_source"])
    warn = _data_health(prices)
    return prices, warn, run_backtest(prices, p,
                                      start=params["bt_start_date"],
                                      end=params["bt_end_date"], light=light)


def _fmt_pct(x, digits=2):
    return "-" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x*100:.{digits}f}%"


def _fmt_mult(x) -> str:
    """원금 대비 배수를 읽기 쉽게 (1,234배 / 12.3만배 / 1.2억배)."""
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
        return "-"
    if x >= 1e8:
        return f"{x/1e8:,.2f}억배"
    if x >= 1e4:
        return f"{x/1e4:,.1f}만배"
    if x >= 100:
        return f"{x:,.0f}배"
    return f"{x:,.2f}배"


def _metric_cards(m: dict, principal: float = None, years: float = None):
    """백테스트 요약 카드 (주문표 탭과 동일한 카드 언어)."""
    mult = (m["최종자산"] / principal) if principal else None
    yrs = years if years is not None else m.get("기간(년)")
    if yrs and yrs >= 1.0:
        perf = {"label": "CAGR (연환산)", "value": _fmt_pct(m["CAGR"]),
                "fg": _sign_color(m["CAGR"]), "sub": f"{yrs:.1f}년 기준"}
    else:
        # 1년 미만 구간의 연환산은 과장되므로 기간수익률로 대체
        perf = {"label": "기간 수익률", "value": _fmt_pct(m["총수익률"], 1),
                "fg": _sign_color(m["총수익률"]),
                "sub": "1년 미만 — 연환산 생략", "sub_color": "#B26A00"}
    _cards([
        {"label": "최종자산", "value": f"${m['최종자산']:,.0f}", "flex": 1.2,
         "sub": (f"원금 ${principal:,.0f}" if principal else "")},
        {"label": "원금 대비", "value": _fmt_mult(mult),
         "sub": _fmt_pct(m["총수익률"], 1),
         "sub_color": _sign_color(m["총수익률"])},
        perf,
        {"label": "MDD", "value": _fmt_pct(m["MDD"]), "fg": _NEG},
        {"label": "승률", "value": _fmt_pct(m["승률"]),
         "sub": f"평균 {m['평균수익률']*100:+.2f}%/회",
         "sub_color": _sign_color(m["평균수익률"])},
        {"label": "거래횟수", "value": f"{m['거래횟수']:,}",
         "sub": f"미청산 {m.get('미청산', 0)}건"},
    ])


# ══════════════════════════════════════════════════════════
# 탭1: 백테스트
# ══════════════════════════════════════════════════════════
def render_backtest_tab(params: dict):
    p: ManseParams = params["mp"]
    st.markdown(f"### 📊 백테스트 — {p.ticker} / 모드 기준 **{p.mode_basis}**")

    if st.button("📊 백테스트 실행", key="ms_run", use_container_width=True,
                 type="primary"):
        with st.spinner("데이터 로드 & 시뮬레이션 중..."):
            prices, warn, res = _run(params)
        st.session_state["ms_result"] = res
        st.session_state["ms_warn"] = warn

    res = st.session_state.get("ms_result")
    _show_health(st.session_state.get("ms_warn", []))
    if not res:
        st.info("⬆️ 사이드바에서 파라미터를 설정한 뒤 **백테스트 실행** 을 눌러주세요.")
        return
    if "error" in res:
        st.error(res["error"])
        return

    df, trades, m = res["df"], res["trades"], res["metrics"]
    _metric_cards(m, p.principal, m.get("기간(년)"))
    _vc = df["모드"].value_counts()
    _tot_d = max(len(df), 1)
    _cards([
        {"label": "운용 기간", "value": f"{len(df):,} 거래일",
         "sub": f"{df.index[0].date()} ~ {df.index[-1].date()}"},
        {"label": "샤프", "value": ("-" if np.isnan(m["샤프"])
                                  else f"{m['샤프']:.2f}")},
        {"label": "손익비", "value": ("-" if np.isnan(m["손익비"])
                                   else f"{m['손익비']:.2f}"),
         "fg": _sign_color(0 if np.isnan(m["손익비"]) else m["손익비"] - 1)},
        {"label": "누적 실현손익", "value": f"${m['누적실현']:+,.0f}",
         "fg": _sign_color(m["누적실현"])},
    ] + [
        {"label": f"{lv} 구간", "value": f"{int(_vc.get(lv, 0)):,}일",
         "bg": _MODE_BG[lv], "fg": _MODE_COLOR[lv],
         "sub": f"{_vc.get(lv, 0)/_tot_d*100:.1f}%"}
        for lv in LEVELS
    ])

    # ── 자산 곡선 + 모드 배경 ──
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.72, 0.28], vertical_spacing=0.05,
                        subplot_titles=("총자산 (로그 스케일)", "낙폭 (Drawdown)"))
    fig.add_trace(go.Scatter(x=df.index, y=df["총자산"], name="총자산",
                             line=dict(color="#1f77b4", width=1.6)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["평가금"], name="평가금",
                             line=dict(color="#ff7f0e", width=1, dash="dot")),
                  row=1, col=1)
    dd = df["총자산"] / df["총자산"].cummax() - 1
    fig.add_trace(go.Scatter(x=df.index, y=dd * 100, name="DD(%)", fill="tozeroy",
                             line=dict(color="#d62728", width=1)), row=2, col=1)
    fig.update_yaxes(type="log", row=1, col=1)
    fig.update_layout(height=620, hovermode="x unified",
                      legend=dict(orientation="h", y=1.06))
    st.plotly_chart(fig, use_container_width=True)

    # ── 모드 타임라인 ──
    with st.expander("🔀 모드 판정 추이", expanded=False):
        mf = res["mode_frame"]
        gap_name = mf.attrs.get("gap_name", "이격도")
        f2 = go.Figure()
        f2.add_trace(go.Scatter(x=mf.index, y=mf[gap_name], name=gap_name,
                                line=dict(color="#333", width=1.2)))
        if p.mode_basis == "RSI":
            lo, hi = p.rsi_low, p.rsi_high
        elif p.mode_basis == "중심주가":
            lo, hi = p.center_low, p.center_high
        else:
            lo, hi = p.ma_low, p.ma_high
        f2.add_hline(y=lo, line_dash="dash", line_color=_MODE_COLOR["바닥"],
                     annotation_text=f"바닥 경계 {lo}")
        f2.add_hline(y=hi, line_dash="dash", line_color=_MODE_COLOR["천장"],
                     annotation_text=f"천장 경계 {hi}")
        f2.update_layout(height=320, hovermode="x unified",
                         title=f"{p.indicator_ticker()} 주봉 {gap_name}")
        st.plotly_chart(f2, use_container_width=True)

        vc = df["모드"].value_counts()
        cc = st.columns(4)
        for i, lv in enumerate(LEVELS):
            cc[i].metric(f"{lv} 거래일", f"{int(vc.get(lv, 0)):,}일")
        cc[3].metric("모드 미정", f"{int(vc.get('', 0)):,}일")
        st.dataframe(mf.tail(30), use_container_width=True)

    # ── 구간별 / 티어별 통계 ──
    st.markdown("#### 📈 구간별 · 티어별 성과")
    c1, c2 = st.columns(2)
    if len(res["by_level"]):
        c1.markdown("**구간별**")
        c1.dataframe(res["by_level"].style.format({
            "승률": "{:.1%}", "평균수익률": "{:.2%}", "손익합": "${:,.0f}",
            "손익비": "{:.2f}", "평균보유일": "{:.1f}", "거래횟수": "{:.0f}"}),
            use_container_width=True, hide_index=True)
    if len(res["by_tier"]):
        c2.markdown("**구간 × 티어별**")
        c2.dataframe(res["by_tier"].style.format({
            "승률": "{:.1%}", "평균수익률": "{:.2%}", "손익합": "${:,.0f}",
            "손익비": "{:.2f}", "평균보유일": "{:.1f}", "거래횟수": "{:.0f}"}),
            use_container_width=True, hide_index=True)

    # ── 연도별 성과 ──
    with st.expander("📅 연도별 성과", expanded=False):
        yr = df["총자산"].resample("YE").last()
        yr0 = df["총자산"].resample("YE").first()
        ydf = pd.DataFrame({
            "연도": yr.index.year,
            "연말자산": yr.values,
            "연간수익률": (yr.values / yr0.values - 1),
            "실현손익": df["실현손익"].resample("YE").sum().values,
            "거래일": df["종가"].resample("YE").count().values,
        })
        st.dataframe(ydf.style.format({
            "연말자산": "${:,.0f}", "연간수익률": "{:.2%}", "실현손익": "${:,.0f}"}),
            use_container_width=True, hide_index=True)

    # ── 로그 ──
    with st.expander("📋 일별 시뮬레이션 로그 (시트 RECORD 재현)", expanded=False):
        show = df.copy()
        show.index = show.index.date
        st.dataframe(show.tail(400), use_container_width=True)
        st.download_button("⬇️ 전체 일별 로그 CSV",
                           df.to_csv().encode("utf-8-sig"),
                           file_name=f"manse_{p.ticker}_daily.csv",
                           mime="text/csv", key="ms_dl_daily")

    with st.expander(f"🧾 매매 기록 ({len(trades):,}건)", expanded=False):
        st.dataframe(trades.tail(400), use_container_width=True, hide_index=True)
        st.download_button("⬇️ 매매 기록 CSV",
                           trades.to_csv(index=False).encode("utf-8-sig"),
                           file_name=f"manse_{p.ticker}_trades.csv",
                           mime="text/csv", key="ms_dl_trades")

    # ── 강건성 점검 ──
    st.markdown("---")
    st.markdown("#### 🎲 파라미터 강건성 점검")
    render_robustness_panel(
        _load_prices(p.needed_tickers(), params["data_source"]), p,
        params["bt_start_date"], params["bt_end_date"], key="bt")

    st.caption("ℹ️ 원본 구글시트는 마지막 행(종료일)을 '주문일'로 보아 체결시키지 않습니다. "
               "본 엔진은 종료일도 정상 체결시키므로 마지막 하루만 시트와 다를 수 있습니다.")


# ══════════════════════════════════════════════════════════
# 탭2: 파라미터 최적화
# ══════════════════════════════════════════════════════════
def _frange(lo, hi, step):
    if step <= 0 or hi < lo:
        return [round(float(lo), 6)]
    n = int(round((hi - lo) / step)) + 1
    return [round(lo + i * step, 6) for i in range(max(n, 1))]


def _irange(lo, hi, step=1):
    lo, hi, step = int(lo), int(hi), max(int(step), 1)
    return list(range(lo, hi + 1, step)) or [lo]


# ── 탐색 항목 위젯 (체크박스 + 최소/최대/간격) ────────────────
def _dim_num(box, label, key, cur, lo, hi, step, fmt="%.3f", scale=1.0):
    """실수형 탐색 항목. 반환: 값 리스트 (엔진 단위). 미선택 시 [현재값]."""
    c = box.columns([1.7, 1, 1, 1])
    on = c[0].checkbox(label, value=False, key=f"o_{key}_on")
    a = c[1].number_input("최소", value=float(lo), step=float(step), format=fmt,
                          key=f"o_{key}_lo", label_visibility="collapsed")
    b = c[2].number_input("최대", value=float(hi), step=float(step), format=fmt,
                          key=f"o_{key}_hi", label_visibility="collapsed")
    s = c[3].number_input("간격", value=float(step), step=float(step), format=fmt,
                          min_value=1e-9, key=f"o_{key}_st",
                          label_visibility="collapsed")
    if not on:
        return None
    return [v * scale for v in _frange(a, b, s)]


def _dim_int(box, label, key, cur, lo, hi, step=1):
    c = box.columns([1.7, 1, 1, 1])
    on = c[0].checkbox(label, value=False, key=f"o_{key}_on")
    a = c[1].number_input("최소", value=int(lo), step=1, key=f"o_{key}_lo",
                          label_visibility="collapsed")
    b = c[2].number_input("최대", value=int(hi), step=1, key=f"o_{key}_hi",
                          label_visibility="collapsed")
    s = c[3].number_input("간격", value=int(step), min_value=1, step=1,
                          key=f"o_{key}_st", label_visibility="collapsed")
    return _irange(a, b, s) if on else None


def _dim_bool(box, label, key, cur):
    c = box.columns([1.7, 3])
    on = c[0].checkbox(label, value=False, key=f"o_{key}_on")
    c[1].caption("체크 시 True/False 두 경우 모두 탐색" if on else "고정: "
                 + ("True" if cur else "False"))
    return [False, True] if on else None


# ── 샘플 → ManseParams ───────────────────────────────────────
_SCALAR_KEYS = ("center_low", "center_high", "ma_low", "ma_high",
                "rsi_low", "rsi_high", "ma_days", "rsi_period",
                "profit_comp", "loss_comp", "renew_cycle",
                "extra_range", "extra_count", "extra_step", "tier_method")


def _apply_sample(p0: ManseParams, s: dict) -> ManseParams:
    """탐색 샘플(dict)을 파라미터 객체에 반영."""
    q = copy.deepcopy(p0)
    if "mode_basis" in s:
        q.mode_basis = s["mode_basis"]
    for k in _SCALAR_KEYS:
        if k in s:
            setattr(q, k, s[k])

    for lv in LEVELS:
        L = q.levels[lv]
        touched_w = False
        if f"{lv}_split" in s:
            L.split = int(s[f"{lv}_split"])
            touched_w = True
        for suf, attr in (("fq", "fixed_qty"), ("nb", "no_buy_on_sell_day"),
                          ("mb", "moc_day_buy")):
            k = f"{lv}_{suf}"
            if k in s:
                setattr(L, attr, s[k])
        # 시드분할수를 늘렸다면 마지막 티어를 복제해 채운다
        while len(L.tiers) < L.split:
            L.tiers.append(copy.deepcopy(L.tiers[-1]) if L.tiers
                           else TierParam(1.0, 0.0, 0.02, 20))
        for i in range(len(L.tiers)):
            for suf, attr in (("w", "seed_w"), ("b", "buy_gap"),
                              ("s", "sell_gap"), ("d", "stop_days")):
                k = f"{lv}_t{i+1}_{suf}"
                if k in s:
                    setattr(L.tiers[i], attr,
                            int(s[k]) if attr == "stop_days" else s[k])
                    if attr == "seed_w":
                        touched_w = True
        # 시드비중을 건드렸으면 사용되는 티어 합계를 100%로 정규화
        if touched_w:
            used = L.tiers[:L.split]
            tot = sum(t.seed_w for t in used)
            if tot > 0:
                for t in used:
                    t.seed_w = t.seed_w / tot
    return q


def _mf_key(q: ManseParams):
    return (q.mode_basis, q.center_ticker, q.center_low, q.center_high,
            q.ma_ticker, q.ma_days, q.ma_low, q.ma_high,
            q.rsi_ticker, q.rsi_period, q.rsi_low, q.rsi_high)


# ══════════════════════════════════════════════════════════
# 강건성 점검 (파라미터 ±N% 교란)
# ══════════════════════════════════════════════════════════
def robustness_check(prices: dict, p: ManseParams, start, end,
                     n: int = 30, pct: float = 0.10, seed: int = 7,
                     progress=None) -> dict:
    """티어 파라미터를 ±pct 범위로 무작위 교란해 결과 산포를 본다.

    최적화 결과가 '봉우리 꼭대기'인지 '고원'인지 구분하기 위한 검사.
    """
    rnd = random.Random(seed)
    base = run_backtest(prices, p, start=start, end=end, light=True)
    if "error" in base:
        return {"error": base["error"]}
    vals = []
    for i in range(n):
        q = copy.deepcopy(p)
        for L in q.levels.values():
            for t in L.tiers:
                t.buy_gap *= (1 + rnd.uniform(-pct, pct))
                t.sell_gap *= (1 + rnd.uniform(-pct, pct))
                t.stop_days = max(1, int(round(
                    t.stop_days * (1 + rnd.uniform(-pct, pct)))))
        m = run_backtest(prices, q, start=start, end=end, light=True)
        if "error" not in m:
            vals.append(m)
        if progress:
            progress((i + 1) / n)
    if not vals:
        return {"error": "교란 시뮬레이션 실패"}
    fin = np.array([v["최종자산"] for v in vals])
    b = base["최종자산"]
    return {
        "base": base, "n": len(fin),
        "median": float(np.median(fin)), "min": float(fin.min()),
        "max": float(fin.max()),
        "median_ratio": float(np.median(fin) / b) if b else np.nan,
        "min_ratio": float(fin.min() / b) if b else np.nan,
        "max_ratio": float(fin.max() / b) if b else np.nan,
        "spread": float(fin.max() / fin.min()) if fin.min() > 0 else np.inf,
        "worse": int((fin < b).sum()),
        "cagr": np.array([v["CAGR"] for v in vals]),
        "mdd": np.array([v["MDD"] for v in vals]),
        "finals": fin,
    }


def render_robustness_panel(prices, p, start, end, key: str):
    """강건성 점검 UI (백테스트 탭 / 최적화 결과 아래에서 재사용)."""
    c1, c2, c3 = st.columns([1, 1, 2])
    n = int(c1.number_input("교란 횟수", value=30, min_value=5, max_value=300,
                            step=5, key=f"rb_n_{key}"))
    pct = c2.number_input("교란 폭 (±%)", value=10.0, min_value=1.0,
                          max_value=50.0, step=1.0, key=f"rb_p_{key}") / 100
    c3.caption("매수목표 · 매도목표 · 손절일수를 무작위로 흔들어 "
               "결과가 얼마나 흩어지는지 봅니다. 산포가 크면 그 파라미터는 "
               "우연히 잘 맞은 봉우리일 가능성이 높습니다.")
    if st.button("🎲 강건성 점검 실행", key=f"rb_run_{key}",
                 use_container_width=True):
        bar = st.progress(0.0)
        r = robustness_check(prices, p, start, end, n=n, pct=pct,
                             progress=bar.progress)
        bar.empty()
        if "error" in r:
            st.error(r["error"])
            return
        # 결과 보관 — 위 입력 위젯을 만져도 표가 사라지지 않도록
        st.session_state[f"rb_res_{key}"] = r

    r = st.session_state.get(f"rb_res_{key}")
    if r is None:
        return

    b = r["base"]["최종자산"]
    m = st.columns(5)
    m[0].metric("기준 파라미터", f"${b:,.0f}")
    m[1].metric("교란 중앙값", f"${r['median']:,.0f}", f"{r['median_ratio']:.2f}배")
    m[2].metric("최소", f"${r['min']:,.0f}", f"{r['min_ratio']:.3f}배")
    m[3].metric("최대", f"${r['max']:,.0f}", f"{r['max_ratio']:.2f}배")
    m[4].metric("기준보다 나쁨", f"{r['worse']}/{r['n']}")

    fig = go.Figure(go.Histogram(x=r["finals"], nbinsx=25,
                                 marker_color="#8E8E93"))
    fig.add_vline(x=b, line_color="#E74C3C", line_width=2,
                  annotation_text="기준 파라미터")
    fig.update_layout(height=280, xaxis_type="log",
                      xaxis_title="최종자산 (로그)", yaxis_title="빈도",
                      title=f"±{pct*100:.0f}% 교란 {r['n']}회 최종자산 분포")
    st.plotly_chart(fig, use_container_width=True)

    ratio = r["worse"] / r["n"]
    if r["spread"] >= 5 or ratio >= 0.7:
        st.error(f"⚠️ **취약** — 최대/최소 {r['spread']:,.0f}배 산포, "
                 f"{r['worse']}/{r['n']}회가 기준보다 나쁨. "
                 f"이 파라미터는 봉우리 꼭대기에 앉아 있을 가능성이 큽니다. "
                 f"CAGR 중앙값 {np.median(r['cagr'])*100:.1f}% "
                 f"(기준 {r['base']['CAGR']*100:.1f}%)")
    elif r["spread"] >= 2.5:
        st.warning(f"⚠️ **보통** — 최대/최소 {r['spread']:.1f}배 산포. "
                   f"주변 값도 함께 확인하세요.")
    else:
        st.success(f"✅ **안정적** — 최대/최소 {r['spread']:.1f}배 산포. "
                   f"고원(plateau) 위에 있습니다.")


# ══════════════════════════════════════════════════════════
# 최적화 방식
# ══════════════════════════════════════════════════════════
# 그리드/랜덤/베이지안은 "가장 높은 봉우리"를 찾는다. 그런데 이 전략은
# ±10% 교란만으로 결과가 14배 흩어지는 것이 확인됐다 (전략 소개 탭 강건성 참고).
# → 봉우리가 아니라 고원을 찾는 🏔️ 강건 최적화, 미래 구간에서 검증하는
#   📈 워크포워드, 다른 역사에서도 통하는지 보는 🎲 몬테카를로가 실전에서 더 유용하다.
_OPT_METHODS = {
    "📊 그리드 탐색": "모든 파라미터 조합을 완전 탐색합니다. 조합이 적을 때 가장 정확합니다.",
    "🎲 랜덤 탐색": "조합 공간에서 무작위 추출해 탐색합니다. 넓은 범위를 빠르게 훑을 때 유리합니다.",
    "📈 워크포워드": "기간을 IS(최적화)·OOS(검증) 윈도우로 나눠 굴립니다. "
                 "**미래 구간 성적**으로 평가하므로 과적합을 걸러냅니다.",
    "🧠 베이지안": "Optuna TPE 로 유망한 영역을 집중 탐색합니다. 적은 시도로 빠르게 수렴합니다.",
    "🏔️ 강건 최적화": "점수를 후보 자신이 아니라 **주변 이웃들의 성적**으로 매깁니다. "
                  "봉우리 꼭대기 대신 고원을 찾아 실전 재현성이 높습니다. ⭐ 권장",
    "🎲 몬테카를로 (부트스트랩)": "가격 시계열을 블록 단위로 재표집해 **합성 역사** 여러 개를 만들고 "
                        "거기서도 통하는 파라미터를 고릅니다. '이 한 번의 역사'에만 "
                        "맞춘 값을 걸러냅니다.",
}


class _OptCtx:
    """최적화 실행에 필요한 공통 컨텍스트."""

    def __init__(self, p, prices, start, end, labels, keys):
        self.p, self.prices = p, prices
        self.start, self.end = start, end
        self.labels, self.keys = labels, keys
        self.mf_cache = {}

    def mode_frame(self, q, prices=None):
        pr = prices if prices is not None else self.prices
        if prices is not None:
            return build_mode_frame(q, pr)
        k = _mf_key(q)
        if k not in self.mf_cache:
            self.mf_cache[k] = build_mode_frame(q, pr)
        return self.mf_cache[k]

    def run(self, q, start=None, end=None, prices=None):
        pr = prices if prices is not None else self.prices
        return run_backtest(pr, q, start=start or self.start,
                            end=end or self.end,
                            mode_frame=self.mode_frame(q, prices), light=True)

    def row(self, s, m):
        row = {}
        for k in self.keys:
            v = s[k]
            row[self.labels[k]] = (round(v * 100, 4)
                                   if self.labels[k].endswith("%") else v)
        row.update({"최종자산": m["최종자산"],
                    "배수": m["최종자산"] / self.p.principal,
                    "CAGR": m["CAGR"], "MDD": m["MDD"], "Calmar": m["Calmar"],
                    "승률": m["승률"], "손익비": m["손익비"],
                    "거래횟수": m["거래횟수"]})
        return row


def _gen_combos(dims, keys, n_trials, seed, grid=False):
    """후보 조합 생성. grid=True 면 전수(앞에서부터 n_trials), 아니면 무작위."""
    total = 1
    for v in dims.values():
        total *= len(v)
    if grid or total <= n_trials:
        out = []
        for c in itertools.product(*[dims[k] for k in keys]):
            out.append(dict(zip(keys, c)))
            if len(out) >= n_trials:
                break
        return out
    rnd = random.Random(seed)
    seen, combos, guard = set(), [], 0
    while len(combos) < n_trials and guard < n_trials * 50:
        guard += 1
        c = tuple(rnd.choice(dims[k]) for k in keys)
        if c in seen:
            continue
        seen.add(c)
        combos.append(dict(zip(keys, c)))
    return combos


def _score_of(m, sort_by):
    """정렬 기준에 해당하는 스칼라 점수 (클수록 좋음)."""
    v = m.get(sort_by if sort_by != "최종자산" else "최종자산")
    if sort_by == "MDD":
        return v if v is not None else -1.0    # 0에 가까울수록 큼
    return v if v is not None and not (isinstance(v, float) and np.isnan(v)) else -1e18


def _opt_plain(ctx, combos, bar, txt):
    """그리드 / 랜덤 탐색."""
    rows = []
    for i, s in enumerate(combos):
        q = _apply_sample(ctx.p, s)
        m = ctx.run(q)
        if "error" not in m:
            rows.append((ctx.row(s, m), s))
        if i % 5 == 0 or i == len(combos) - 1:
            bar.progress((i + 1) / len(combos))
            txt.caption(f"{i+1:,}/{len(combos):,} 완료")
    return rows, []


def _opt_robust(ctx, dims, n_trials, seed, sort_by, neigh, pct, stat, bar, txt):
    """🏔️ 강건 최적화 — 이웃 성적으로 순위를 매겨 '고원'을 찾는다."""
    combos = _gen_combos(dims, ctx.keys, n_trials, seed)
    rnd = random.Random(seed + 1)
    rows = []
    for i, s in enumerate(combos):
        q = _apply_sample(ctx.p, s)
        m0 = ctx.run(q)
        if "error" in m0:
            continue
        scores, cagrs, mdds = [], [], []
        for _ in range(neigh):
            qn = copy.deepcopy(q)
            for L in qn.levels.values():
                for t in L.tiers:
                    t.buy_gap *= (1 + rnd.uniform(-pct, pct))
                    t.sell_gap *= (1 + rnd.uniform(-pct, pct))
                    t.stop_days = max(1, int(round(
                        t.stop_days * (1 + rnd.uniform(-pct, pct)))))
            mn = ctx.run(qn)
            if "error" in mn:
                continue
            scores.append(_score_of(mn, sort_by))
            cagrs.append(mn["CAGR"])
            mdds.append(mn["MDD"])
        if not scores:
            continue
        arr = np.array(scores, dtype=float)
        if stat == "최악값(5%)":
            agg = float(np.percentile(arr, 5))
        elif stat == "평균":
            agg = float(arr.mean())
        else:
            agg = float(np.median(arr))
        base = _score_of(m0, sort_by)
        row = ctx.row(s, m0)
        row["이웃점수"] = agg
        row["이웃/기준"] = (agg / base) if base not in (0, None) else np.nan
        row["이웃CAGR중앙"] = float(np.median(cagrs))
        row["이웃MDD최악"] = float(np.min(mdds))
        rows.append((row, s))
        bar.progress((i + 1) / len(combos))
        txt.caption(f"{i+1:,}/{len(combos):,} 후보 (이웃 {neigh}회씩)")
    return rows, ["이웃점수", "이웃/기준", "이웃CAGR중앙", "이웃MDD최악"]


def _opt_walkforward(ctx, dims, n_trials, seed, sort_by, is_y, oos_y, bar, txt):
    """📈 워크포워드 — IS 최적화 → OOS 검증을 슬라이딩 반복."""
    s0 = pd.Timestamp(ctx.start)
    e0 = pd.Timestamp(ctx.end)
    wins = []
    cur = s0
    while True:
        is_e = cur + pd.DateOffset(months=int(round(is_y * 12)))
        oos_e = is_e + pd.DateOffset(months=int(round(oos_y * 12)))
        if oos_e > e0:
            break
        wins.append((cur, is_e, is_e, oos_e))
        cur = is_e
    if not wins:
        raise RuntimeError("기간이 짧아 IS/OOS 윈도우를 만들 수 없습니다. "
                           "IS·OOS 기간을 줄이거나 백테스트 기간을 늘리세요.")

    combos = _gen_combos(dims, ctx.keys, n_trials, seed)
    tot = len(wins) * len(combos)
    done = 0
    wf, picks = [], []
    for wi, (a, b, c, d) in enumerate(wins):
        best, best_s, best_sc = None, None, -1e18
        for s in combos:
            q = _apply_sample(ctx.p, s)
            m = ctx.run(q, start=a, end=b)
            done += 1
            if "error" not in m:
                sc = _score_of(m, sort_by)
                if sc > best_sc:
                    best, best_s, best_sc = m, s, sc
            if done % 10 == 0:
                bar.progress(min(done / tot, 1.0))
                txt.caption(f"윈도우 {wi+1}/{len(wins)} · IS 최적화 "
                            f"{done:,}/{tot:,}")
        if best_s is None:
            continue
        qo = _apply_sample(ctx.p, best_s)
        mo = ctx.run(qo, start=c, end=d)
        if "error" in mo:
            continue
        wf.append({"윈도우": wi + 1,
                   "IS 시작": a.date(), "IS 종료": b.date(),
                   "OOS 시작": c.date(), "OOS 종료": d.date(),
                   "IS CAGR": best["CAGR"], "IS MDD": best["MDD"],
                   "OOS CAGR": mo["CAGR"], "OOS MDD": mo["MDD"],
                   "OOS 수익률": mo["총수익률"], "OOS 거래": mo["거래횟수"],
                   "효율(OOS/IS)": (mo["CAGR"] / best["CAGR"]
                                  if best["CAGR"] else np.nan)})
        picks.append((best_s, mo))
    if not wf:
        raise RuntimeError("워크포워드 결과가 없습니다.")
    st.session_state["ms_opt_wf"] = pd.DataFrame(wf)

    rows = []
    for s, mo in picks:
        row = ctx.row(s, mo)
        rows.append((row, s))
    return rows, []


def _bootstrap_paths(prices: dict, n_paths: int, block: int, seed: int) -> list:
    """블록 부트스트랩으로 합성 가격 경로 생성.

    ⚠️ 투자종목과 지표종목의 **동시성**을 보존해야 하므로 두 시계열을
       같은 블록 인덱스로 함께 재표집한다 (따로 뽑으면 모드-가격 관계가 깨진다).
    수익률을 블록 단위로 이어붙인 뒤 원래 시작가에서 누적하고 2자리로 반올림한다
    (센트 단위 반올림이 이 전략의 체결에 직접 영향을 주기 때문).
    """
    tks = sorted(prices.keys())
    base = {t: pd.Series(prices[t]["Close"]).dropna().sort_index() for t in tks}
    common = None
    for t in tks:
        common = base[t].index if common is None else common.intersection(base[t].index)
    if common is None or len(common) < block * 3:
        return []
    rets = {t: np.log(base[t].reindex(common)).diff().fillna(0.0).values
            for t in tks}
    n = len(common)
    nb = int(np.ceil(n / block))
    rnd = np.random.default_rng(seed)
    out = []
    for _ in range(n_paths):
        starts = rnd.integers(0, max(n - block, 1), size=nb)
        idx = np.concatenate([np.arange(s, min(s + block, n)) for s in starts])[:n]
        path = {}
        for t in tks:
            r = rets[t][idx]
            lv = float(base[t].reindex(common).iloc[0]) * np.exp(np.cumsum(r))
            df = pd.DataFrame({"Close": np.round(lv, 2)}, index=common)
            path[t] = df
        out.append(path)
    return out


def _opt_montecarlo(ctx, dims, n_trials, seed, sort_by, n_paths, block, bar, txt):
    """🎲 몬테카를로 — 합성 역사에서도 통하는 파라미터를 고른다."""
    paths = _bootstrap_paths(ctx.prices, n_paths, block, seed)
    if not paths:
        raise RuntimeError("합성 경로를 만들 수 없습니다 (데이터가 너무 짧음).")
    combos = _gen_combos(dims, ctx.keys, n_trials, seed)
    rows = []
    for i, s in enumerate(combos):
        q = _apply_sample(ctx.p, s)
        m0 = ctx.run(q)
        if "error" in m0:
            continue
        sc, cg, md = [], [], []
        for pth in paths:
            mm = ctx.run(q, prices=pth)
            if "error" in mm:
                continue
            sc.append(_score_of(mm, sort_by))
            cg.append(mm["CAGR"])
            md.append(mm["MDD"])
        if not sc:
            continue
        row = ctx.row(s, m0)
        row["MC점수중앙"] = float(np.median(sc))
        row["MC CAGR중앙"] = float(np.median(cg))
        row["MC MDD최악"] = float(np.min(md))
        row["MC 손실경로"] = float(np.mean(np.array(cg) < 0))
        rows.append((row, s))
        bar.progress((i + 1) / len(combos))
        txt.caption(f"{i+1:,}/{len(combos):,} 후보 (합성 {n_paths}경로씩)")
    return rows, ["MC점수중앙", "MC CAGR중앙", "MC MDD최악", "MC 손실경로"]


def _opt_bayes(ctx, dims, n_trials, seed, sort_by, bar, txt):
    """🧠 베이지안 — Optuna TPE 로 유망 영역 집중 탐색."""
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    rows, cnt = [], [0]

    def obj(trial):
        s = {k: trial.suggest_categorical(k, list(map(_to_native, v)))
             for k, v in dims.items()}
        q = _apply_sample(ctx.p, s)
        m = ctx.run(q)
        cnt[0] += 1
        bar.progress(min(cnt[0] / n_trials, 1.0))
        txt.caption(f"베이지안 탐색 {cnt[0]:,}/{n_trials:,}")
        if "error" in m:
            return -1e18
        rows.append((ctx.row(s, m), s))
        return _score_of(m, sort_by)

    study = optuna.create_study(
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=seed))
    study.optimize(obj, n_trials=int(n_trials), show_progress_bar=False)
    return rows, []


def _to_native(v):
    """optuna categorical 은 numpy 타입을 싫어한다 → 파이썬 기본형으로."""
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    return v


# ══════════════════════════════════════════════════════════
# 탭2: 파라미터 최적화 — 전체 파라미터 탐색
# ══════════════════════════════════════════════════════════
def render_optimization_tab(params: dict):
    p: ManseParams = params["mp"]
    st.markdown("### 🔍 파라미터 최적화")

    st.markdown("##### ⚙️ 최적화 방식")
    method = st.radio("최적화 방식", list(_OPT_METHODS), horizontal=True,
                      key="o_method", label_visibility="collapsed")
    st.caption(_OPT_METHODS[method])
    if method in ("🏔️ 강건 최적화", "📈 워크포워드", "🎲 몬테카를로 (부트스트랩)"):
        st.info("💡 이 방식은 **과적합 방지**가 목적입니다. 최고 수익률보다 "
                "낮게 나오는 것이 정상이며, 그 값이 실전에 더 가깝습니다.")
    st.markdown("---")
    st.caption("아래에서 체크한 항목만 탐색하고, 체크하지 않은 항목은 "
               "사이드바 설정값을 그대로 씁니다.")

    dims: dict = {}      # {키: [후보값...]}
    labels: dict = {}    # {키: 결과표 컬럼명}

    def put(key, values, label):
        if values:
            dims[key] = values
            labels[key] = label

    # ── 모드 판정 ──
    with st.expander("🔀 모드 판정 기준 · 경계값", expanded=True):
        bases = st.multiselect(
            "모드 판단 기준 (여러 개 선택 시 모두 비교)", list(MODE_BASES),
            default=[p.mode_basis], key="o_basis")
        if bases and bases != [p.mode_basis]:
            put("mode_basis", bases, "모드기준")
        elif not bases:
            bases = [p.mode_basis]

        if "중심주가" in bases:
            st.markdown("**중심주가 경계** (이격도 %)")
            put("center_low", _dim_num(st, "바닥 범위", "clo", p.center_low * 100,
                                       p.center_low * 100 - 3, p.center_low * 100 + 3,
                                       0.5, "%.2f", 0.01), "중심_바닥%")
            put("center_high", _dim_num(st, "천장 범위", "chi", p.center_high * 100,
                                        p.center_high * 100 - 3, p.center_high * 100 + 3,
                                        0.5, "%.2f", 0.01), "중심_천장%")
        if "이평선" in bases:
            st.markdown("**이평선 경계** (이격도 %)")
            put("ma_days", _dim_int(st, "이평선 일수", "mad", p.ma_days,
                                    max(5, p.ma_days - 60), p.ma_days + 60, 10),
                "MA일수")
            put("ma_low", _dim_num(st, "바닥 범위", "mlo", p.ma_low * 100,
                                   p.ma_low * 100 - 2, p.ma_low * 100 + 2,
                                   0.25, "%.2f", 0.01), "MA_바닥%")
            put("ma_high", _dim_num(st, "천장 범위", "mhi", p.ma_high * 100,
                                    p.ma_high * 100 - 2, p.ma_high * 100 + 2,
                                    0.25, "%.2f", 0.01), "MA_천장%")
        if "RSI" in bases:
            st.markdown("**RSI 경계**")
            put("rsi_period", _dim_int(st, "RSI 기간(주)", "rper", p.rsi_period,
                                       max(2, p.rsi_period - 6), p.rsi_period + 6, 2),
                "RSI기간")
            put("rsi_low", _dim_num(st, "바닥 RSI", "rlo", p.rsi_low,
                                    p.rsi_low - 8, p.rsi_low + 8, 2.0, "%.1f"),
                "RSI_바닥")
            put("rsi_high", _dim_num(st, "천장 RSI", "rhi", p.rsi_high,
                                     p.rsi_high - 8, p.rsi_high + 8, 2.0, "%.1f"),
                "RSI_천장")

        tms = st.multiselect("티어계산 방식", list(TIER_METHODS),
                             default=[p.tier_method], key="o_tm")
        if tms and tms != [p.tier_method]:
            put("tier_method", tms, "티어방식")

    # ── 자금 / 복리 / 추가주문 ──
    with st.expander("💰 복리 · 추가 주문", expanded=False):
        put("profit_comp", _dim_num(st, "이익 복리율 (%)", "pc", p.profit_comp * 100,
                                    max(0, p.profit_comp * 100 - 30),
                                    min(100, p.profit_comp * 100 + 30), 5.0,
                                    "%.1f", 0.01), "이익복리%")
        put("loss_comp", _dim_num(st, "손실 복리율 (%)", "lc", p.loss_comp * 100,
                                  max(0, p.loss_comp * 100 - 30),
                                  min(100, p.loss_comp * 100 + 30), 5.0,
                                  "%.1f", 0.01), "손실복리%")
        put("renew_cycle", _dim_int(st, "갱신 주기 (거래일)", "rc", p.renew_cycle,
                                    1, max(5, p.renew_cycle + 9), 1), "갱신주기")
        put("extra_range", _dim_num(st, "추가 매수 범위 (%)", "er",
                                    p.extra_range * 100, p.extra_range * 100 - 10,
                                    min(0.0, p.extra_range * 100 + 10), 2.5,
                                    "%.2f", 0.01), "추가범위%")
        if p.order_type == "추가 주문 건수 고정":
            put("extra_count", _dim_int(st, "추가 주문 건수", "ec", p.extra_count,
                                        1, 12, 1), "추가건수")
        else:
            put("extra_step", _dim_int(st, "추가 매수 간격 (주)", "es", p.extra_step,
                                       1, 20, 1), "추가간격")

    # ── 구간별 ──
    st.markdown("##### 📐 구간별 파라미터")
    for lv in LEVELS:
        L = p.levels[lv]
        with st.expander(f"{lv} 구간 (현재 분할 {L.split}, 티어 {len(L.tiers)}개)",
                         expanded=(lv == "바닥")):
            put(f"{lv}_split", _dim_int(st, "시드분할수", f"{lv}sp", L.split,
                                        1, max(4, L.split + 2), 1), f"{lv}_분할")
            put(f"{lv}_fq", _dim_bool(st, "정량매수", f"{lv}fq", L.fixed_qty),
                f"{lv}_정량")
            put(f"{lv}_nb", _dim_bool(st, "매도날 매수X", f"{lv}nb",
                                      L.no_buy_on_sell_day), f"{lv}_매도날X")
            put(f"{lv}_mb", _dim_bool(st, "MOC날 매수", f"{lv}mb", L.moc_day_buy),
                f"{lv}_MOC매수")
            for i, t in enumerate(L.tiers, start=1):
                st.markdown(f"**티어 {i}**")
                put(f"{lv}_t{i}_w",
                    _dim_num(st, f"T{i} 시드비중 (%)", f"{lv}t{i}w", t.seed_w * 100,
                             max(0.1, t.seed_w * 100 - 20),
                             min(100.0, t.seed_w * 100 + 20), 5.0, "%.2f", 0.01),
                    f"{lv}T{i}_비중%")
                put(f"{lv}_t{i}_b",
                    _dim_num(st, f"T{i} 매수목표 (%)", f"{lv}t{i}b", t.buy_gap * 100,
                             t.buy_gap * 100 - 2, t.buy_gap * 100 + 2, 0.5,
                             "%.3f", 0.01), f"{lv}T{i}_매수%")
                put(f"{lv}_t{i}_s",
                    _dim_num(st, f"T{i} 매도목표 (%)", f"{lv}t{i}s", t.sell_gap * 100,
                             max(0.01, t.sell_gap * 100 - 1.5),
                             t.sell_gap * 100 + 1.5, 0.3, "%.3f", 0.01),
                    f"{lv}T{i}_매도%")
                put(f"{lv}_t{i}_d",
                    _dim_int(st, f"T{i} 손절일수", f"{lv}t{i}d", t.stop_days,
                             max(1, t.stop_days - 5), t.stop_days + 5, 1),
                    f"{lv}T{i}_손절")

    # ══ 실행 ══
    st.markdown("---")
    total = 1
    for v in dims.values():
        total *= len(v)
    if not dims:
        st.info("탐색할 항목을 하나 이상 체크해주세요. "
                "(위 그룹을 펼쳐 원하는 파라미터의 체크박스를 켜세요)")
        return

    c1, c2, c3 = st.columns(3)
    n_trials = int(c1.number_input("시도 횟수", value=min(300, max(total, 1)),
                                   min_value=1, max_value=20000, step=50,
                                   key="o_n"))
    sort_by = c2.selectbox("정렬 기준",
                           ["Calmar", "CAGR", "최종자산", "MDD", "승률", "손익비"],
                           key="o_sort")
    seed = int(c3.number_input("랜덤 시드", value=42, step=1, key="o_seed"))

    # 방식별 추가 옵션
    extra = {}
    if method == "🏔️ 강건 최적화":
        e1, e2, e3 = st.columns(3)
        extra["neigh"] = int(e1.number_input(
            "이웃 표본 수", value=8, min_value=3, max_value=40, step=1,
            key="o_rb_k", help="후보마다 주변을 몇 번 흔들어 볼지"))
        extra["pct"] = e2.number_input(
            "교란 폭 (±%)", value=10.0, min_value=1.0, max_value=50.0,
            step=1.0, key="o_rb_p") / 100
        extra["stat"] = e3.selectbox(
            "이웃 점수 집계", ["중앙값", "최악값(5%)", "평균"], key="o_rb_s",
            help="이웃들의 성적을 어떻게 대표값으로 삼을지")
        st.caption(f"1개 후보당 {extra['neigh']+1}회 백테스트 → "
                   f"실행 ≈ {min(total, n_trials)*(extra['neigh']+1):,}회")
    elif method == "📈 워크포워드":
        e1, e2 = st.columns(2)
        extra["is_y"] = e1.number_input("IS(최적화) 기간 (년)", value=3.0,
                                        min_value=0.5, max_value=10.0,
                                        step=0.5, key="o_wf_is")
        extra["oos_y"] = e2.number_input("OOS(검증) 기간 (년)", value=1.0,
                                         min_value=0.25, max_value=5.0,
                                         step=0.25, key="o_wf_oos")
    elif method == "🎲 몬테카를로 (부트스트랩)":
        e1, e2 = st.columns(2)
        extra["paths"] = int(e1.number_input(
            "합성 경로 수", value=12, min_value=3, max_value=60, step=1,
            key="o_mc_p", help="가격 시계열을 블록 재표집해 만든 가짜 역사 개수"))
        extra["block"] = int(e2.number_input(
            "블록 길이 (거래일)", value=21, min_value=5, max_value=120, step=1,
            key="o_mc_b", help="추세·변동성 군집을 보존하려면 20~60일 권장"))
        st.caption(f"1개 후보당 {extra['paths']+1}회 백테스트 → "
                   f"실행 ≈ {min(total, n_trials)*(extra['paths']+1):,}회")
    elif method == "🧠 베이지안":
        try:
            import optuna  # noqa: F401
        except ImportError:
            st.error("`optuna` 패키지가 없습니다. `pip install optuna` 후 재시작하세요.")
            return

    if method == "📊 그리드 탐색":
        st.info(f"탐색 항목 **{len(dims)}개** · 전체 조합 **{total:,}개** → "
                f"실행 **{min(total, n_trials):,}회** "
                f"(1회당 약 0.1~0.2초, 예상 {min(total, n_trials)*0.15/60:.1f}분)")
        if total > n_trials:
            st.warning(f"조합 {total:,}개 > 시도 {n_trials:,}회 — "
                       f"앞에서부터 {n_trials:,}개만 실행합니다. "
                       f"전수 탐색하려면 시도 횟수를 늘리거나 범위를 줄이세요.")
    else:
        st.info(f"탐색 항목 **{len(dims)}개** · 전체 조합 **{total:,}개** → "
                f"후보 **{min(total, n_trials):,}개** 평가")

    if st.button("🚀 최적화 실행", key="o_run", use_container_width=True,
                 type="primary"):
        keys = list(dims.keys())
        # 탐색할 모든 모드 기준에 필요한 티커를 한 번에 로드
        need = {p.ticker.upper()}
        for b in (dims.get("mode_basis") or [p.mode_basis]):
            need.add({"중심주가": p.center_ticker, "이평선": p.ma_ticker,
                      "RSI": p.rsi_ticker}[b].upper())
        prices = _load_prices(sorted(need), params["data_source"])
        _show_health(_data_health(prices))

        s_date, e_date = params["bt_start_date"], params["bt_end_date"]
        bar, txt = st.progress(0.0), st.empty()
        ctx = _OptCtx(p, prices, s_date, e_date, labels, keys)

        try:
            if method == "🧠 베이지안":
                rows, extra_cols = _opt_bayes(ctx, dims, n_trials, seed,
                                              sort_by, bar, txt)
            elif method == "📈 워크포워드":
                rows, extra_cols = _opt_walkforward(
                    ctx, dims, n_trials, seed, sort_by,
                    extra["is_y"], extra["oos_y"], bar, txt)
            elif method == "🏔️ 강건 최적화":
                rows, extra_cols = _opt_robust(
                    ctx, dims, n_trials, seed, sort_by,
                    extra["neigh"], extra["pct"], extra["stat"], bar, txt)
            elif method == "🎲 몬테카를로 (부트스트랩)":
                rows, extra_cols = _opt_montecarlo(
                    ctx, dims, n_trials, seed, sort_by,
                    extra["paths"], extra["block"], bar, txt)
            else:   # 그리드 / 랜덤
                combos = _gen_combos(dims, keys, n_trials, seed,
                                     grid=(method == "📊 그리드 탐색"))
                rows, extra_cols = _opt_plain(ctx, combos, bar, txt)
        except Exception as _oe:
            bar.empty(); txt.empty()
            st.error(f"최적화 실패: {_oe}")
            return
        bar.empty(); txt.empty()

        if not rows:
            st.warning("유효한 결과가 없습니다. 범위를 넓혀보세요.")
            return
        st.session_state["ms_opt_res"] = pd.DataFrame([r for r, _ in rows])
        st.session_state["ms_opt_samples"] = [s for _, s in rows]
        st.session_state["ms_opt_prices"] = prices
        st.session_state["ms_opt_method"] = method
        st.session_state["ms_opt_extracols"] = extra_cols

    res = st.session_state.get("ms_opt_res")
    if res is None or res.empty:
        return

    ran_method = st.session_state.get("ms_opt_method", method)
    extra_cols = st.session_state.get("ms_opt_extracols") or []

    # 방식별로 순위 기준 컬럼이 다르다 (강건/MC 는 이웃·합성 점수가 본질)
    rank_col, rank_note = sort_by, ""
    if "이웃점수" in res.columns:
        rank_col = "이웃점수"
        rank_note = f"이웃 성적({sort_by}) 기준 — 봉우리가 아닌 고원 순"
    elif "MC점수중앙" in res.columns:
        rank_col = "MC점수중앙"
        rank_note = f"합성 경로 {sort_by} 중앙값 기준 — 다른 역사에서도 통한 순"

    order = res.sort_values(rank_col, ascending=False).index
    view = res.loc[order]

    # 워크포워드는 윈도우별 IS/OOS 표가 본체
    if ran_method == "📈 워크포워드":
        wf = st.session_state.get("ms_opt_wf")
        if wf is not None and len(wf):
            st.markdown("#### 📈 워크포워드 결과 (윈도우별 IS 최적화 → OOS 검증)")
            st.dataframe(wf.style.format({
                "IS CAGR": "{:.2%}", "IS MDD": "{:.2%}",
                "OOS CAGR": "{:.2%}", "OOS MDD": "{:.2%}",
                "OOS 수익률": "{:+.2%}", "효율(OOS/IS)": "{:.2f}"}, na_rep="-")
                .map(lambda v: ("color:#2E7D32;font-weight:700"
                                if isinstance(v, float) and v > 0
                                else ("color:#C62828;font-weight:700"
                                      if isinstance(v, float) and v < 0 else "")),
                     subset=["OOS CAGR", "OOS 수익률"]),
                use_container_width=True, hide_index=True)
            _eff = wf["효율(OOS/IS)"].replace([np.inf, -np.inf], np.nan).dropna()
            _pos = float((wf["OOS CAGR"] > 0).mean())
            _cards([
                {"label": "윈도우 수", "value": f"{len(wf)}개"},
                {"label": "OOS CAGR 중앙", "value": _fmt_pct(float(wf["OOS CAGR"].median())),
                 "fg": _sign_color(wf["OOS CAGR"].median())},
                {"label": "OOS 양수 비율", "value": f"{_pos*100:.0f}%",
                 "fg": (_POS if _pos >= 0.6 else _NEG)},
                {"label": "효율 중앙 (OOS/IS)",
                 "value": ("-" if _eff.empty else f"{_eff.median():.2f}"),
                 "sub": "1.0 이면 IS 성적이 그대로 재현"},
                {"label": "OOS MDD 최악", "value": _fmt_pct(float(wf["OOS MDD"].min())),
                 "fg": _NEG},
            ])
            if not _eff.empty and _eff.median() < 0.5:
                st.error("⚠️ OOS 효율이 0.5 미만 — IS 성적의 절반도 재현되지 않습니다. "
                         "과적합 신호입니다.")
            elif _pos < 0.6:
                st.warning(f"⚠️ OOS 구간 중 {(1-_pos)*100:.0f}% 가 손실입니다.")
            else:
                st.success("✅ OOS 구간 대부분에서 양의 성과 — 재현성이 있습니다.")
            st.download_button("⬇️ 워크포워드 CSV",
                               wf.to_csv(index=False).encode("utf-8-sig"),
                               file_name="manse_walkforward.csv",
                               mime="text/csv", key="o_wf_dl")
            st.markdown("---")

    _fmt = {"최종자산": "${:,.0f}", "배수": "{:,.1f}배", "CAGR": "{:.2%}",
            "MDD": "{:.2%}", "Calmar": "{:.2f}", "승률": "{:.1%}",
            "손익비": "{:.2f}"}
    for c in extra_cols:
        if "MDD" in c or "CAGR" in c or "손실경로" in c:
            _fmt[c] = "{:.2%}"
        elif c in ("이웃/기준",):
            _fmt[c] = "{:.2f}"
        elif "점수" in c:
            _fmt[c] = "{:,.4g}"

    st.markdown(f"#### 결과 — {len(res):,}개 후보 (상위 50, **{rank_col}** 내림차순)")
    if rank_note:
        st.caption(f"📌 {rank_note}")
    st.dataframe(view.head(50).style.format(_fmt, na_rep="-"),
                 use_container_width=True, hide_index=True)
    st.download_button("⬇️ 전체 결과 CSV",
                       view.to_csv(index=False).encode("utf-8-sig"),
                       file_name="manse_opt.csv", mime="text/csv", key="o_dl")

    best_i = int(order[0])
    best = res.loc[best_i]
    st.success(
        f"🏆 최적 ({rank_col}) — 최종자산 ${best['최종자산']:,.0f} "
        f"({_fmt_mult(best['배수'])}) · CAGR {best['CAGR']:.2%} · "
        f"MDD {best['MDD']:.2%} · Calmar {best['Calmar']:.2f}")
    if "이웃/기준" in res.columns and pd.notna(best.get("이웃/기준")):
        _r = float(best["이웃/기준"])
        (st.success if _r >= 0.8 else st.warning)(
            f"이웃 점수 / 기준 점수 = **{_r:.2f}** — "
            + ("주변을 흔들어도 성적이 유지됩니다 (고원)." if _r >= 0.8
               else "주변을 흔들면 성적이 떨어집니다. 더 넓은 고원을 찾아보세요."))
    if "MC 손실경로" in res.columns and pd.notna(best.get("MC 손실경로")):
        _l = float(best["MC 손실경로"])
        (st.success if _l <= 0.2 else st.warning)(
            f"합성 경로 중 손실 비율 = **{_l*100:.0f}%** — "
            + ("다른 역사에서도 대체로 통합니다." if _l <= 0.2
               else "이 역사에만 맞춰졌을 가능성이 있습니다."))
    samples = st.session_state.get("ms_opt_samples") or []
    if best_i < len(samples):
        bp = _apply_sample(p, samples[best_i])
        with st.expander("🏆 최적 파라미터 상세 (사이드바에 입력할 값)",
                         expanded=True):
            st.write({"모드 기준": bp.mode_basis, "티어계산": bp.tier_method,
                      "이익/손실 복리율": f"{bp.profit_comp:.0%} / {bp.loss_comp:.0%}",
                      "갱신주기": bp.renew_cycle,
                      "추가 매수 범위": f"{bp.extra_range:.2%}"})
            for lv in LEVELS:
                L = bp.levels[lv]
                st.markdown(f"**{lv}** — 분할 {L.split} · 정량매수 {L.fixed_qty} · "
                            f"매도날매수X {L.no_buy_on_sell_day} · "
                            f"MOC날매수 {L.moc_day_buy}")
                st.dataframe(_tier_df(L), use_container_width=True,
                             hide_index=True)

        st.markdown("---")
        st.markdown("#### 🎲 최적 파라미터 강건성 점검")
        st.caption("최적화 1등은 대체로 과최적화된 값입니다. "
                   "주변을 흔들어도 성과가 유지되는지 반드시 확인하세요.")
        render_robustness_panel(
            st.session_state.get("ms_opt_prices")
            or _load_prices(bp.needed_tickers(), params["data_source"]),
            bp, params["bt_start_date"], params["bt_end_date"], key="opt")


# ══════════════════════════════════════════════════════════
# 파라미터 프리셋 — 원본 만능시트 2종에서 그대로 추출
# (성과는 SOXL 2011-01-03 ~ 2026-08-21, 로컬 DB(무조정) 기준)
# ══════════════════════════════════════════════════════════
_MANSE_PRESETS = [
    {
        "label": "📈 이평선형 (4일) ⭐",
        "help": ("원본 시트 '이평-2티어_4일' 그대로 — 시트 전 구간 검증 완료\n"
                 "CAGR 121.98% | MDD -35.86% | Calmar 3.40 | 승률 62.2% | 거래 2,079\n"
                 "4종 중 Calmar 최고"),
        "mode_basis": "이평선",
        "profit_comp": 0.67, "loss_comp": 0.44, "renew_cycle": 1,
        "extra_range": -0.20, "order_type": "추가 주문 건수 고정",
        "extra_step": 2, "extra_count": 4,
        "ma_ticker": "QQQ", "ma_days": 120, "ma_low": -0.0125, "ma_high": 0.0575,
        "center_ticker": "QQQ", "center_low": 0.055, "center_high": 0.17,
        "rsi_ticker": "QQQ", "rsi_period": 14, "rsi_low": 40.0, "rsi_high": 65.0,
        "tier_method": "보유",
        "levels": {
            "바닥": (2, False, False, True,
                    [(0.01, -0.008, 0.015, 36), (0.99, 0.079, 0.032, 1)]),
            "중간": (2, False, False, True,
                    [(0.01, 0.084, 0.027, 38), (0.99, 0.028, 0.027, 4)]),
            "천장": (2, False, False, True,
                    [(0.01, -0.011, 0.006, 25), (0.99, 0.076, 0.006, 1)]),
        },
    },
    {
        "label": "📉 이평선형 (1일)",
        "help": ("원본 시트 '이평-2티어_1일' 그대로 — 시트 전 구간 검증 완료\n"
                 "4일형 대비 중간 구간 2티어 손절일수 4일 → 1일 + 전 파라미터 재튜닝\n"
                 "CAGR 123.50% | MDD -38.16% | Calmar 3.24 | 승률 58.7% | 거래 2,489\n"
                 "4종 중 최종자산 최대"),
        "mode_basis": "이평선",
        "profit_comp": 0.82, "loss_comp": 0.54, "renew_cycle": 1,
        "extra_range": -0.20, "order_type": "추가 주문 건수 고정",
        "extra_step": 2, "extra_count": 4,
        "ma_ticker": "QQQ", "ma_days": 120, "ma_low": -0.0175, "ma_high": 0.0575,
        "center_ticker": "QQQ", "center_low": 0.055, "center_high": 0.17,
        "rsi_ticker": "QQQ", "rsi_period": 14, "rsi_low": 40.0, "rsi_high": 65.0,
        "tier_method": "보유",
        "levels": {
            "바닥": (2, False, False, True,
                    [(0.01, -0.022, 0.029, 32), (0.99, 0.072, 0.017, 1)]),
            "중간": (2, False, False, True,
                    [(0.01, 0.069, 0.010, 48), (0.99, 0.042, 0.040, 1)]),
            "천장": (2, False, False, True,
                    [(0.01, -0.011, 0.007, 24), (0.99, 0.069, 0.013, 1)]),
        },
    },
    {
        "label": "📊 중심주가형",
        "help": ("원본 시트 '중심주가-2티어' 그대로 — 시트 전 구간 검증 완료\n"
                 "CAGR 98.42% | MDD -34.70% | Calmar 2.84 | 승률 57.1% | 거래 2,800\n"
                 "4종 중 MDD가 가장 낮음"),
        "mode_basis": "중심주가",
        "profit_comp": 0.72, "loss_comp": 0.50, "renew_cycle": 1,
        "extra_range": -0.20, "order_type": "추가 주문 건수 고정",
        "extra_step": 2, "extra_count": 4,
        "ma_ticker": "QQQ", "ma_days": 120, "ma_low": -0.10, "ma_high": 0.05,
        "center_ticker": "QQQ", "center_low": 0.055, "center_high": 0.17,
        "rsi_ticker": "QQQ", "rsi_period": 14, "rsi_low": 40.0, "rsi_high": 65.0,
        "tier_method": "보유",
        "levels": {
            "바닥": (2, False, False, True,
                    [(0.01, -0.01, 0.02, 27), (0.99, 0.045, 0.0001, 1)]),
            "중간": (2, False, False, True,
                    [(0.01, 0.05, 0.01, 24), (0.99, 0.07, 0.055, 1)]),
            "천장": (2, False, False, True,
                    [(0.01, -0.01, 0.0001, 25), (0.99, 0.07, 0.03, 1)]),
        },
    },
    {
        "label": "🌀 RSI형",
        "help": ("원본 시트 'RSI-2티어' 그대로 — 시트 전 구간 검증 완료\n"
                 "CAGR 110.48% | MDD -38.99% | Calmar 2.83 | 승률 55.4% | 거래 2,782\n"
                 "4종 중 손익비가 가장 높음 (1.40)"),
        "mode_basis": "RSI",
        "profit_comp": 0.81, "loss_comp": 0.59, "renew_cycle": 1,
        "extra_range": -0.20, "order_type": "추가 주문 건수 고정",
        "extra_step": 2, "extra_count": 4,
        "ma_ticker": "QQQ", "ma_days": 120, "ma_low": -0.0125, "ma_high": 0.0575,
        "center_ticker": "QQQ", "center_low": 0.055, "center_high": 0.17,
        "rsi_ticker": "QQQ", "rsi_period": 14, "rsi_low": 36.0, "rsi_high": 60.0,
        "tier_method": "보유",
        "levels": {
            "바닥": (2, False, False, True,
                    [(0.01, -0.003, 0.053, 22), (0.99, 0.101, -0.026, 1)]),
            "중간": (2, False, False, True,
                    [(0.01, 0.032, 0.072, 52), (0.99, 0.062, 0.059, 1)]),
            "천장": (2, False, False, True,
                    [(0.01, 0.051, -0.053, 20), (0.99, 0.069, 0.065, 1)]),
        },
    },
]


def preset_to_params(pre: dict, ticker: str = "SOXL",
                     principal: float = 10000.0) -> ManseParams:
    """프리셋 dict → ManseParams."""
    p = ManseParams(ticker=ticker, principal=float(principal))
    for k in ("mode_basis", "profit_comp", "loss_comp", "renew_cycle",
              "extra_range", "order_type", "extra_step", "extra_count",
              "ma_ticker", "ma_days", "ma_low", "ma_high",
              "center_ticker", "center_low", "center_high",
              "rsi_ticker", "rsi_period", "rsi_low", "rsi_high",
              "tier_method"):
        if k in pre:
            setattr(p, k, pre[k])
    p.levels = {}
    for lv, (split, fq, nb, mb, tiers) in pre["levels"].items():
        p.levels[lv] = LevelParam(
            split=split, fixed_qty=fq, no_buy_on_sell_day=nb, moc_day_buy=mb,
            tiers=[TierParam(*t) for t in tiers])
    return p


def _match_preset(p: ManseParams) -> str:
    """현재 파라미터가 어떤 프리셋과 일치하는지 → 라벨 (없으면 '')."""
    for pre in _MANSE_PRESETS:
        q = preset_to_params(pre, p.ticker, p.principal)
        if q.mode_basis != p.mode_basis or q.tier_method != p.tier_method:
            continue
        if (round(q.profit_comp, 6), round(q.loss_comp, 6), q.renew_cycle) != \
           (round(p.profit_comp, 6), round(p.loss_comp, 6), p.renew_cycle):
            continue
        ok = True
        for lv in LEVELS:
            a, b = q.levels[lv], p.levels.get(lv)
            if b is None or a.split != b.split or len(a.tiers) != len(b.tiers):
                ok = False
                break
            for x, y in zip(a.tiers, b.tiers):
                if (round(x.seed_w, 6), round(x.buy_gap, 6),
                        round(x.sell_gap, 6), x.stop_days) != \
                   (round(y.seed_w, 6), round(y.buy_gap, 6),
                        round(y.sell_gap, 6), y.stop_days):
                    ok = False
                    break
            if not ok:
                break
        if ok:
            return pre["label"]
    return ""


# ══════════════════════════════════════════════════════════
# 탭3: 오늘의 주문표 & 계좌관리
# ══════════════════════════════════════════════════════════
def _acct_params(acct: dict) -> ManseParams:
    raw = acct.get("params")
    p = params_from_dict(raw) if raw else preset_to_params(_MANSE_PRESETS[0])
    p.ticker = str(acct.get("ticker", p.ticker)).upper()
    p.principal = float(acct.get("os_capital", p.principal))
    return p


def _sell_card_sub(sells: list) -> str:
    """매도 주문 카드 부제 — MOC 건수와 LOC 최저가를 함께 표기."""
    if not sells:
        return ""
    moc = [o for o in sells if o.get("주문가") is None]
    loc = [o for o in sells if o.get("주문가") is not None]
    parts = []
    if moc:
        parts.append(f"MOC {len(moc)}건")
    if loc:
        parts.append(f"LOC 최저 ${min(o['주문가'] for o in loc):,.2f}")
    return " · ".join(parts)


def _order_text(plan: dict, p: ManseParams, acct_name: str = "") -> str:
    head = f"<b>🎛️ 만능 스위치 — {p.ticker}</b>"
    if acct_name:
        head += f"  [{acct_name}]"
    lines = [head,
             f"주문일: {pd.Timestamp(plan['주문일']).date()}"
             f" / 모드: {plan['모드'] or '-'} / 티어: {plan.get('티어', '-')}",
             f"전일({pd.Timestamp(plan['기준일']).date()}) 종가: "
             f"${plan['전일종가']:,.2f}", ""]
    if plan.get("1회시드") is not None:
        lines.append(f"1회 시드: ${plan['1회시드']:,.0f}")
    lines += [f"투자금(갱신): ${plan['투자금']:,.0f}",
              f"예수금: ${plan['예수금']:,.0f}", ""]
    if plan.get("orders"):
        lines.append("── 주문 ──")
        for o in plan["orders"]:
            icon = "🔵" if "매수" in str(o.get("구분", "")) else "🔴"
            q = o.get("수량")
            px = o.get("주문가")
            # MOC 는 지정가가 없다 → 시장가로 표기하고 목표가는 참고로 덧붙임
            if px is None:
                _tgt = o.get("매도목표가")
                lines.append(f" {icon} {o['구분']}: 시장가(MOC)"
                             + (f" × {int(q):,}주" if q else "")
                             + (f"  [목표가 ${float(_tgt):,.2f}]" if _tgt else ""))
            else:
                lines.append(f" {icon} {o['구분']}: ${float(px):,.2f}"
                             + (f" × {int(q):,}주" if q else ""))
    else:
        lines.append("오늘 주문 없음")
    if plan.get("message"):
        lines += ["", plan["message"]]
    if plan.get("note"):
        lines.append(f"※ {plan['note']}")
    return "\n".join(lines)


def _order_rows(plan: dict) -> list:
    """주문표 → 구글시트 4열 rows [[구분, 거래방법, 가격, 수량], ...]"""
    rows = []
    for o in plan.get("orders", []):
        px, qty = o.get("주문가"), o.get("수량")
        if not qty:
            continue
        gubun = "매수" if "매수" in str(o.get("구분", "")) else "매도"
        method = "MOC" if "MOC" in str(o.get("구분", "")) else "LOC"
        # MOC 는 가격 칸을 비운다 (자동매매 프로그램이 시장가로 인식)
        if method == "MOC":
            rows.append([gubun, "MOC", "", int(qty)])
        elif px is not None:
            rows.append([gubun, "LOC", round(float(px), 2), int(qty)])
    return rows


def render_ordersheet_tab(params: dict):
    """주문표 & 계좌관리 탭 (DSS/표준편차와 동일 레벨)."""
    today = datetime.today().strftime("%Y-%m-%d")
    st.subheader(f"📋 오늘의 주문표  ({today})")
    st.caption("계좌별로 시작일부터 오늘까지 시뮬레이션하여 현황과 "
               "다음 거래일 주문을 계산합니다.")

    cfg = _load_cfg()
    accounts = cfg.get("accounts", {})

    # ── 레거시 마이그레이션: 종목별 저장 → 계좌 구조 ──
    if not accounts:
        for tk, v in list(cfg.items()):
            if tk.startswith("_") or not isinstance(v, dict) or "params" not in v:
                continue
            accounts[tk] = {
                "ticker": tk,
                "os_start": str(v.get("bt_start", "2024-01-02")),
                "os_capital": float(
                    (v.get("params") or {}).get("principal", 10000.0)),
                "capital_adj_history": [],
                "data_source": v.get("data_source", DATA_SOURCES[0]),
                "gs_sheet": v.get("gs_sheet", tk),
                "params": v.get("params"),
            }
        if accounts:
            cfg["accounts"] = accounts
            _save_cfg(cfg)
            st.info(f"기존 종목별 설정 {len(accounts)}개를 계좌로 이전했습니다.")

    # ── 계좌 추가 ──
    with st.expander("➕ 계좌 추가", expanded=not accounts):
        c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
        new_name = c1.text_input("계좌 이름", placeholder="예: 메인, ISA",
                                 key="ms_new_acct_name")
        new_tk = c2.text_input("종목", value="SOXL",
                               key="ms_new_acct_tk").strip().upper()
        new_start = c3.date_input("시작일", value=datetime.today().date(),
                                  key="ms_new_acct_start")
        new_cap = c4.number_input("시작 자본 ($)", value=10000.0, step=1000.0,
                                  key="ms_new_acct_cap")
        labels = [pr["label"] for pr in _MANSE_PRESETS]
        pi = st.selectbox("파라미터 프리셋", range(len(_MANSE_PRESETS)),
                          format_func=lambda i: labels[i], index=0,
                          key="ms_new_acct_preset")
        st.caption(_MANSE_PRESETS[pi]["help"])
        if st.button("✅ 계좌 등록", type="primary", key="ms_add_acct",
                     use_container_width=True):
            nm = new_name.strip()
            if not nm:
                st.warning("계좌 이름을 입력하세요.")
            elif nm in accounts:
                st.warning(f"'{nm}' 계좌가 이미 존재합니다.")
            else:
                pre = _MANSE_PRESETS[pi]
                accounts[nm] = {
                    "ticker": new_tk or "SOXL",
                    "os_start": str(new_start),
                    "os_capital": float(new_cap),
                    "capital_adj_history": [],
                    "data_source": DATA_SOURCES[0],
                    "gs_sheet": _default_sheet_name(nm, new_tk or "SOXL"),
                    "params": params_to_dict(
                        preset_to_params(pre, new_tk or "SOXL", new_cap)),
                }
                cfg["accounts"] = accounts
                _save_cfg(cfg)
                st.success(f"✅ '{nm}' 계좌 등록 완료 (프리셋: {pre['label']})")
                st.rerun()

    names = list(accounts.keys())
    if not names:
        st.info("등록된 계좌가 없습니다. 위에서 계좌를 추가하세요.")
        return

    for i, (nm, tab) in enumerate(zip(names, st.tabs([f"📊 {n}" for n in names]))):
        with tab:
            _render_account(nm, accounts[nm], cfg, i)


def _render_account(name: str, acct: dict, cfg: dict, idx: int):
    sfx = f"a{idx}"
    p = _acct_params(acct)
    badge = _match_preset(p)

    # ══ 파라미터 요약 카드 ══
    if p.mode_basis == "이평선":
        _basis_sub = f"{p.ma_ticker} MA{p.ma_days}"
    elif p.mode_basis == "중심주가":
        _basis_sub = f"{p.center_ticker} 추세선"
    else:
        _basis_sub = f"{p.rsi_ticker} {p.rsi_period}주 RSI"
    _cards([
        {"label": "종목", "value": p.ticker},
        {"label": "모드 기준", "value": p.mode_basis, "sub": _basis_sub},
        {"label": "티어계산", "value": p.tier_method,
         "sub": " · ".join(f"{lv} {p.levels[lv].split}분할" for lv in LEVELS)},
        {"label": "복리 (이익/손실)",
         "value": f"{p.profit_comp:.0%} / {p.loss_comp:.0%}",
         "sub": f"{p.renew_cycle}거래일마다 갱신"},
        {"label": "수수료 (왕복)", "value": f"{p.fee*2*100:.3f}%",
         "sub": f"SEC {p.sec_fee*100:.4f}%"},
        {"label": "프리셋", "value": badge or "커스텀", "flex": 1.4,
         "fg": "#333" if badge else _DIM},
    ])

    with st.expander("⚙️ 파라미터 보기 / 수정", expanded=False):
        pre_labels = ["(직접 수정)"] + [x["label"] for x in _MANSE_PRESETS]
        sel = st.selectbox("프리셋 적용", range(len(pre_labels)),
                           format_func=lambda i: pre_labels[i], index=0,
                           key=f"ms_{sfx}_preset")
        if sel > 0:
            st.caption(_MANSE_PRESETS[sel - 1]["help"])
            if st.button(f"↩️ {pre_labels[sel]} 적용", key=f"ms_{sfx}_applypre",
                         use_container_width=True):
                acct["params"] = params_to_dict(preset_to_params(
                    _MANSE_PRESETS[sel - 1], p.ticker, p.principal))
                cfg["accounts"][name] = acct
                _save_cfg(cfg)
                st.success("프리셋을 적용했습니다.")
                st.rerun()

        st.markdown("##### 스위치 세팅")
        s1, s2, s3 = st.columns(3)
        p.mode_basis = s1.selectbox("모드 기준", MODE_BASES,
                                    index=MODE_BASES.index(p.mode_basis),
                                    key=f"ms_{sfx}_basis")
        p.tier_method = s2.selectbox("티어계산", TIER_METHODS,
                                     index=TIER_METHODS.index(p.tier_method),
                                     key=f"ms_{sfx}_tm")
        if p.mode_basis == "이평선":
            p.ma_ticker = s3.text_input("이평선 종목", p.ma_ticker,
                                        key=f"ms_{sfx}_mtk").upper()
            m1, m2, m3 = st.columns(3)
            p.ma_days = int(m1.number_input("이평 일수", value=int(p.ma_days),
                                            min_value=5, step=5,
                                            key=f"ms_{sfx}_mad"))
            p.ma_low = m2.number_input("바닥 경계 (%)", value=p.ma_low * 100,
                                       step=0.25, format="%.2f",
                                       key=f"ms_{sfx}_mlo") / 100
            p.ma_high = m3.number_input("천장 경계 (%)", value=p.ma_high * 100,
                                        step=0.25, format="%.2f",
                                        key=f"ms_{sfx}_mhi") / 100
        elif p.mode_basis == "중심주가":
            p.center_ticker = s3.selectbox(
                "중심주가 종목", list(CENTER_PRESETS),
                index=(list(CENTER_PRESETS).index(p.center_ticker)
                       if p.center_ticker in CENTER_PRESETS else 1),
                key=f"ms_{sfx}_ctk")
            m1, m2 = st.columns(2)
            p.center_low = m1.number_input("바닥 경계 (%)", value=p.center_low * 100,
                                           step=0.25, format="%.2f",
                                           key=f"ms_{sfx}_clo") / 100
            p.center_high = m2.number_input("천장 경계 (%)", value=p.center_high * 100,
                                            step=0.25, format="%.2f",
                                            key=f"ms_{sfx}_chi") / 100
        else:
            p.rsi_ticker = s3.text_input("RSI 종목", p.rsi_ticker,
                                         key=f"ms_{sfx}_rtk").upper()
            m1, m2, m3 = st.columns(3)
            p.rsi_period = int(m1.number_input("RSI 기간(주)", value=int(p.rsi_period),
                                               min_value=2, step=1,
                                               key=f"ms_{sfx}_rp"))
            p.rsi_low = m2.number_input("바닥 RSI", value=float(p.rsi_low),
                                        step=1.0, key=f"ms_{sfx}_rlo")
            p.rsi_high = m3.number_input("천장 RSI", value=float(p.rsi_high),
                                         step=1.0, key=f"ms_{sfx}_rhi")

        st.markdown("##### 자금 · 수수료")
        f1, f2, f3, f4 = st.columns(4)
        p.profit_comp = f1.number_input("이익 복리율 (%)", value=p.profit_comp * 100,
                                        step=1.0, key=f"ms_{sfx}_pc") / 100
        p.loss_comp = f2.number_input("손실 복리율 (%)", value=p.loss_comp * 100,
                                      step=1.0, key=f"ms_{sfx}_lc") / 100
        p.renew_cycle = int(f3.number_input("갱신 주기", value=int(p.renew_cycle),
                                            min_value=1, step=1,
                                            key=f"ms_{sfx}_rc"))
        p.fee = f4.number_input("수수료 (%)", value=p.fee * 100, step=0.01,
                                format="%.4f", key=f"ms_{sfx}_fee") / 100

        st.markdown("##### 구간별 티어")
        for lv in LEVELS:
            L = p.levels[lv]
            st.markdown(f"**{lv}**")
            g1, g2, g3, g4 = st.columns(4)
            L.split = int(g1.number_input("시드분할수", value=int(L.split),
                                          min_value=1, max_value=10, step=1,
                                          key=f"ms_{sfx}_{lv}_sp"))
            L.fixed_qty = g2.checkbox("정량매수", L.fixed_qty,
                                      key=f"ms_{sfx}_{lv}_fq")
            L.no_buy_on_sell_day = g3.checkbox("매도날 매수X", L.no_buy_on_sell_day,
                                               key=f"ms_{sfx}_{lv}_nb")
            L.moc_day_buy = g4.checkbox("MOC날 매수", L.moc_day_buy,
                                        key=f"ms_{sfx}_{lv}_mb")
            L.tiers = _df_to_tiers(st.data_editor(
                _tier_df(L), key=f"ms_{sfx}_{lv}_ed", hide_index=True,
                num_rows="fixed", use_container_width=True,
                column_config={
                    "티어": st.column_config.NumberColumn(disabled=True,
                                                        width="small"),
                    "시드비중(%)": st.column_config.NumberColumn(format="%.2f"),
                    "매수목표(%)": st.column_config.NumberColumn(format="%.3f"),
                    "매도목표(%)": st.column_config.NumberColumn(format="%.3f"),
                    "손절일수": st.column_config.NumberColumn(format="%d"),
                }))

        if st.button("💾 파라미터 저장", type="primary", key=f"ms_{sfx}_savep",
                     use_container_width=True):
            acct["params"] = params_to_dict(p)
            cfg["accounts"][name] = acct
            _save_cfg(cfg)
            st.success("저장했습니다.")
            st.rerun()

    # ══ 계좌 설정 ══
    with st.expander("🗂️ 계좌 설정 (시작일 · 자본 · 이름변경 · 삭제)", expanded=False):
        b1, b2, b3 = st.columns(3)
        new_start = b1.date_input(
            "시작일", pd.to_datetime(acct.get("os_start", "2024-01-02")).date(),
            key=f"ms_{sfx}_start")
        new_cap = b2.number_input("시작 자본 ($)",
                                  value=float(acct.get("os_capital", 10000.0)),
                                  step=1000.0, key=f"ms_{sfx}_cap")
        src_list = list(DATA_SOURCES)
        cur_src = acct.get("data_source", DATA_SOURCES[0])
        new_src = b3.selectbox("데이터 소스", src_list,
                               index=src_list.index(cur_src)
                               if cur_src in src_list else 0,
                               key=f"ms_{sfx}_src")
        new_sheet = st.text_input(
            "구글시트 탭 이름",
            value=acct.get("gs_sheet") or _default_sheet_name(name, p.ticker),
            key=f"ms_{sfx}_sheet",
            help="계좌마다 달라야 합니다. 같으면 나중에 보낸 주문이 앞의 것을 덮어씁니다.")
        _dups = [n for n, a in (cfg.get("accounts") or {}).items()
                 if n != name and str(a.get("gs_sheet", "")).strip()
                 == new_sheet.strip()]
        if _dups:
            st.warning(f"⚠️ 탭 이름 '{new_sheet}' 이 계좌 **{', '.join(_dups)}** "
                       f"와 겹칩니다 — 주문이 서로 덮어써집니다.")
        if st.button("💾 계좌 설정 저장", key=f"ms_{sfx}_savea",
                     use_container_width=True):
            acct.update({"os_start": str(new_start), "os_capital": float(new_cap),
                         "data_source": new_src, "gs_sheet": new_sheet.strip()})
            acct["params"] = params_to_dict(p)
            cfg["accounts"][name] = acct
            _save_cfg(cfg)
            st.success("저장했습니다.")
            st.rerun()

        st.markdown("---")
        r1, r2 = st.columns(2)
        rename = r1.text_input("새 계좌 이름", value=name, key=f"ms_{sfx}_rn")
        if r1.button("✏️ 이름 변경", key=f"ms_{sfx}_rnbtn",
                     use_container_width=True):
            nn = rename.strip()
            if not nn or nn == name:
                st.warning("다른 이름을 입력하세요.")
            elif nn in cfg["accounts"]:
                st.warning(f"'{nn}' 계좌가 이미 있습니다.")
            else:
                cfg["accounts"] = {(nn if k == name else k): v
                                   for k, v in cfg["accounts"].items()}
                _save_cfg(cfg)
                st.rerun()
        if r2.checkbox("삭제 확인", key=f"ms_{sfx}_delck"):
            if r2.button("🗑️ 이 계좌 삭제", key=f"ms_{sfx}_del",
                         use_container_width=True):
                cfg["accounts"].pop(name, None)
                _save_cfg(cfg)
                st.rerun()

    # ══ 자본 조정 ══
    adj_hist = acct.get("capital_adj_history", []) or []
    base_cap = float(acct.get("os_capital", 10000.0))
    adj_hist, cur_cap = recalc_adj_history(adj_hist, base_cap)
    with st.expander(f"💵 자본 조정 (현재 자본금 ${cur_cap:,.0f})", expanded=False):
        st.caption("시작 자본(base)은 바꾸지 않고 증액/감액 이력으로 추적합니다.")
        a1, a2, a3 = st.columns([1, 1, 2])
        adj_date = a1.date_input("조정일", datetime.today().date(),
                                 key=f"ms_{sfx}_adjd")
        adj_amt = a2.number_input("조정금액 ($, 감액은 음수)", value=0.0,
                                  step=1000.0, key=f"ms_{sfx}_adja")
        adj_memo = a3.text_input("메모", key=f"ms_{sfx}_adjm")
        if st.button("➕ 조정 추가", key=f"ms_{sfx}_adjadd",
                     use_container_width=True):
            if adj_amt == 0:
                st.warning("조정금액을 입력하세요.")
            else:
                adj_hist.append({"날짜": str(adj_date), "조정금액": float(adj_amt),
                                 "메모": adj_memo})
                acct["capital_adj_history"], _ = recalc_adj_history(
                    adj_hist, base_cap)
                cfg["accounts"][name] = acct
                _save_cfg(cfg)
                st.rerun()
        if adj_hist:
            adf = pd.DataFrame(adj_hist)
            st.dataframe(adf.style.format({"조정금액": "${:,.0f}",
                                           "누적자본금": "${:,.0f}"}),
                         use_container_width=True, hide_index=True)
            di = st.number_input("삭제할 행 번호 (0부터)", value=0, min_value=0,
                                 max_value=max(len(adj_hist) - 1, 0), step=1,
                                 key=f"ms_{sfx}_adjdi")
            if st.button("🗑️ 해당 조정 삭제", key=f"ms_{sfx}_adjdel"):
                adj_hist.pop(int(di))
                acct["capital_adj_history"], _ = recalc_adj_history(
                    adj_hist, base_cap)
                cfg["accounts"][name] = acct
                _save_cfg(cfg)
                st.rerun()

    # ══ 주문표 생성 ══
    st.markdown("---")
    if st.button("📋 주문표 생성 / 새로고침", type="primary",
                 key=f"ms_{sfx}_run", use_container_width=True):
        with st.spinner("시뮬레이션 중..."):
            try:
                src = acct.get("data_source", DATA_SOURCES[0])
                prices = _load_prices(p.needed_tickers(), src)
                warn = _data_health(prices)
                cash_flows = {}
                for h in adj_hist:
                    try:
                        cash_flows[pd.Timestamp(h["날짜"]).normalize()] = {
                            "deposit": float(h.get("조정금액", 0))}
                    except Exception:
                        pass
                res = run_backtest(prices, p, start=acct.get("os_start"),
                                   end=str(datetime.today().date()),
                                   cash_flows=cash_flows)
                if "error" in res:
                    st.error(res["error"])
                    return
                plan = build_order_plan(prices, p, bt_result=res)
                st.session_state[f"ms_os_{sfx}"] = {
                    "res": res, "plan": plan, "warn": warn}
            except Exception as e:
                st.error(f"생성 실패: {e}")
                return

    store = st.session_state.get(f"ms_os_{sfx}")
    if not store:
        st.info("⬆️ **주문표 생성** 을 눌러주세요.")
        return
    _show_health(store["warn"])
    res, plan = store["res"], store["plan"]
    if "error" in plan:
        st.error(plan["error"])
        return
    df, trades, m = res["df"], res["trades"], res["metrics"]

    # ── 모드 & 지표 카드 ──
    st.markdown("#### 📊 현황")
    mode = plan["모드"] or ""
    mfr = res["mode_frame"]
    gap_name = mfr.attrs.get("gap_name", "이격도")
    if p.mode_basis == "RSI":
        _lo, _hi = p.rsi_low, p.rsi_high
        _fv = lambda v: f"{v:.1f}"
    elif p.mode_basis == "중심주가":
        _lo, _hi = p.center_low, p.center_high
        _fv = lambda v: f"{v*100:+.2f}%"
    else:
        _lo, _hi = p.ma_low, p.ma_high
        _fv = lambda v: f"{v*100:+.2f}%"
    _gv = mfr[gap_name].dropna() if gap_name in mfr.columns else pd.Series(dtype=float)
    _gnow = float(_gv.iloc[-1]) if len(_gv) else None
    _gprev = float(_gv.iloc[-2]) if len(_gv) > 1 else None

    chg = df["등락률"].iloc[-1]
    tp_now = (p.levels[mode].tier(plan.get("티어")) if mode and mode in p.levels
              else None)
    _cards([
        {"label": "현재 모드", "value": mode or "-", "flex": 1.2,
         "bg": _MODE_BG.get(mode, "#FAFAFA"),
         "fg": _MODE_COLOR.get(mode, "#333"),
         "sub": f"바닥 &lt; {_fv(_lo)} &nbsp;·&nbsp; 천장 &gt; {_fv(_hi)}"},
        {"label": f"{p.indicator_ticker()} 주봉 {gap_name}",
         "value": _fv(_gnow) if _gnow is not None else "-",
         "sub": (f"전주 {_fv(_gprev)}" if _gprev is not None else ""),
         "sub_color": (_sign_color(_gnow - _gprev)
                       if _gnow is not None and _gprev is not None else _DIM)},
        {"label": "기준일 종가", "value": f"${plan['전일종가']:,.2f}",
         "sub": (f"{chg*100:+.2f}%" if pd.notna(chg) else ""),
         "sub_color": _sign_color(chg if pd.notna(chg) else 0)},
        {"label": "티어 / 분할수",
         "value": (f"{plan.get('티어', '-')} / "
                   f"{p.levels[mode].split if mode in p.levels else '-'}"),
         "sub": (f"매수 {tp_now.buy_gap*100:+.2f}% · "
                 f"매도 {tp_now.sell_gap*100:+.2f}% · "
                 f"손절 {tp_now.stop_days}일" if tp_now else "신규 매수 없음")},
    ])

    # ── 포트폴리오 요약 카드 ──
    total = float(df["총자산"].iloc[-1])
    equity = float(df["평가금"].iloc[-1])
    cash = float(df["예수금"].iloc[-1])
    shares = int(df["보유"].iloc[-1])
    adj_sum = float(cur_cap - base_cap)
    # 수익률은 자본 조정을 제외한 순수 매매 성과 기준 (DSS와 동일 원칙)
    trading_asset = total - adj_sum
    ret = (trading_asset / base_cap - 1) if base_cap > 0 else float("nan")
    realized = float(df["실현손익"].sum(skipna=True))
    eval_pnl = 0.0
    _op = trades[trades["매도일"].isna()] if len(trades) else pd.DataFrame()
    if len(_op):
        eval_pnl = float(((float(df["종가"].iloc[-1]) - _op["매수가"])
                          * _op["수량"]).sum())
    _cards([
        {"label": "시작 자본", "value": f"${base_cap:,.0f}",
         "sub": (f"조정 ${adj_sum:+,.0f}" if abs(adj_sum) > 0.01 else ""),
         "sub_color": "#0B7A3E"},
        {"label": "총자산", "value": f"${total:,.0f}",
         "sub": ("" if np.isnan(ret) else f"{ret*100:+.1f}%"),
         "sub_color": _sign_color(0 if np.isnan(ret) else ret)},
        {"label": "평가금", "value": f"${equity:,.0f}",
         "sub": f"{shares:,}주 · 평가손익 ${eval_pnl:+,.0f}",
         "sub_color": _sign_color(eval_pnl)},
        {"label": "예수금", "value": f"${cash:,.0f}",
         "sub": f"투자금(갱신) ${df['투자금갱신'].iloc[-1]:,.0f}"},
        {"label": "누적 실현손익", "value": f"${realized:+,.0f}",
         "fg": _sign_color(realized),
         "sub": f"매도 {int(m['거래횟수']):,}회 · 승률 {m['승률']*100:.1f}%"},
    ])

    # ── 성과 지표 카드 ──
    days = (df.index[-1] - df.index[0]).days
    yrs = days / 365.25
    if yrs >= 1.0:
        _perf = {"label": "CAGR (연환산)", "value": _fmt_pct(m["CAGR"]),
                 "fg": _sign_color(m["CAGR"]), "sub": f"{yrs:.1f}년 기준"}
    else:
        # 1년 미만 구간을 연환산하면 과장되므로 기간수익률로 대체
        _perf = {"label": "기간 수익률", "value": _fmt_pct(ret, 1),
                 "fg": _sign_color(0 if np.isnan(ret) else ret),
                 "sub": "1년 미만 — 연환산 생략", "sub_color": "#B26A00"}
    _cards([
        {"label": "운용 기간", "value": f"{days:,}일",
         "sub": f"{df.index[0].date()} ~ {df.index[-1].date()}"},
        _perf,
        {"label": "MDD", "value": _fmt_pct(m["MDD"]), "fg": _NEG},
        {"label": "원금 대비",
         "value": _fmt_mult(trading_asset / base_cap if base_cap else float("nan")),
         "sub": "자본 조정 제외"},
        {"label": "손익비",
         "value": ("-" if np.isnan(m["손익비"]) else f"{m['손익비']:.2f}"),
         "sub": f"평균 {m['평균수익률']*100:+.2f}%/회"},
        {"label": "미청산", "value": f"{int(m['미청산'])}건"},
    ])

    # ── 오늘의 주문 ──
    st.divider()
    st.markdown(f"#### 📑 다음 거래일 주문  "
                f"({pd.Timestamp(plan['주문일']).date()})")
    _buy = [o for o in (plan.get("orders") or []) if "매수" in str(o.get("구분"))]
    _sell = [o for o in (plan.get("orders") or []) if "매수" not in str(o.get("구분"))]
    _cards([
        {"label": "티어", "value": str(plan.get("티어", "-"))},
        {"label": "1회 시드",
         "value": (f"${plan['1회시드']:,.0f}"
                   if plan.get("1회시드") is not None else "-")},
        {"label": "매수 주문 (LOC)",
         "value": (f"${_buy[0]['주문가']:,.2f}" if _buy else "없음"),
         "fg": (_MODE_COLOR["바닥"] if _buy else _DIM),
         "sub": (f"{int(_buy[0]['수량']):,}주 · ${_buy[0]['금액']:,.0f}"
                 if _buy else "")},
        {"label": "매도 주문", "value": f"{len(_sell)}건",
         "fg": (_NEG if _sell else _DIM),
         "sub": _sell_card_sub(_sell)},
    ])
    if plan.get("message"):
        st.info(plan["message"])
    if plan.get("note"):
        st.caption(f"※ {plan['note']}")
    if plan.get("orders"):
        odf = pd.DataFrame(plan["orders"])
        if "주문가" in odf.columns:
            odf["주문가"] = odf["주문가"].map(
                lambda v: "시장가(MOC)" if pd.isna(v) else f"${float(v):,.2f}")
        st.dataframe(odf.style.format({"금액": "${:,.0f}",
                                       "매도목표가": "${:,.2f}"}, na_rep="-"),
                     use_container_width=True, hide_index=True)
    else:
        st.warning("다음 거래일 주문 없음")

    # ── 보유 포지션 ──
    st.markdown("#### 📦 현재 보유 포지션")
    op = trades[trades["매도일"].isna()] if len(trades) else pd.DataFrame()
    if len(op):
        show = op[["매수일", "구간", "티어", "매수가", "수량", "매수대금",
                   "매도목표가", "손절예정"]].copy()
        last_px = float(df["종가"].iloc[-1])
        show["현재가"] = last_px
        show["평가손익"] = (last_px - show["매수가"]) * show["수량"]
        show["수익률"] = last_px / show["매수가"] - 1
        st.dataframe(show.style.format({
            "매수가": "${:,.2f}", "매수대금": "${:,.0f}", "매도목표가": "${:,.2f}",
            "현재가": "${:,.2f}", "평가손익": "${:,.0f}", "수익률": "{:+.2%}"}),
            use_container_width=True, hide_index=True)
    else:
        st.caption("보유 중인 포지션이 없습니다.")

    # ── 최근 매도 / 일별 상세 ──
    with st.expander("🧾 최근 매도 기록 (20건)", expanded=False):
        cl = trades[trades["매도일"].notna()].tail(20) if len(trades) else pd.DataFrame()
        if len(cl):
            st.dataframe(cl.style.format({
                "매수가": "${:,.2f}", "매도가": "${:,.2f}", "매수대금": "${:,.0f}",
                "실현손익": "${:,.0f}", "수익률": "{:+.2%}", "매도목표가": "${:,.2f}"}),
                use_container_width=True, hide_index=True)
        else:
            st.caption("매도 기록이 없습니다.")

    with st.expander("📋 일별 매매 상세표 (60일)", expanded=False):
        d = df.tail(60).copy()
        d.index = d.index.date
        st.dataframe(d, use_container_width=True)
        st.download_button("⬇️ 전체 일별 로그 CSV",
                           df.to_csv().encode("utf-8-sig"),
                           file_name=f"manse_{name}_{p.ticker}_daily.csv",
                           mime="text/csv", key=f"ms_{sfx}_dl")

    # ── 전송 ══
    st.markdown("#### 📨 전송")
    token, chat_id = _get_tg_creds()
    gs_url = _gs_url()
    text = _order_text(plan, p, name)
    st.code(text.replace("<b>", "").replace("</b>", ""), language=None)
    t1, t2 = st.columns(2)
    with t1:
        if st.button("📨 텔레그램 전송", key=f"ms_{sfx}_tg",
                     use_container_width=True, disabled=not (token and chat_id)):
            r = _send_telegram(token, chat_id, text)
            st.success("✅ 전송 완료") if r.get("ok") else st.error(
                f"전송 실패: {r.get('description')}")
        if not (token and chat_id):
            st.caption("⚙️ 개인 설정 탭에서 텔레그램을 먼저 설정하세요.")
    with t2:
        sheet_nm = acct.get("gs_sheet") or _default_sheet_name(name, p.ticker)
        if st.button(f"📤 구글시트 전송 ('{sheet_nm}')", key=f"ms_{sfx}_gs",
                     use_container_width=True, disabled=not gs_url):
            try:
                rows = _order_rows(plan)
                tung = str((st.session_state.get("user_settings", {}) or {})
                           .get("ms_use_tungchigi", "")).strip().lower() \
                    in ("true", "1", "y", "yes", "on") or \
                    str(load_config().get("ms_use_tungchigi", "")).strip().lower() \
                    in ("true", "1", "y", "yes", "on")
                if rows and tung:
                    from dss_engine import rows_to_tungchigi_rows
                    rows = rows_to_tungchigi_rows(rows)
                gc = _get_gspread_client()
                _sh = gc.open_by_url(gs_url)
                ws = None
                try:
                    ws = _sh.worksheet(sheet_nm)
                except Exception:
                    # gspread WorksheetNotFound 는 메시지가 탭 이름뿐이라
                    # 그대로 보여주면 원인을 알 수 없다.
                    # ⚠️ 여기서 st.stop() 을 쓰면 뒤쪽 탭(개인 설정 등)이
                    #    렌더되지 않아 그 탭 위젯 상태가 날아간다 → 쓰지 않는다.
                    st.error(f"⛔ 구글시트에 **'{sheet_nm}'** 탭이 없습니다. "
                             f"시트에서 해당 이름의 탭을 만들어 주세요 "
                             f"(기존 주문 탭을 복사해 이름만 바꾸면 양식 유지). "
                             f"현재 탭 목록: "
                             f"{', '.join(w.title for w in _sh.worksheets())}")
                if ws is not None:
                    ws.batch_clear(["L4:O13"])
                    if rows:
                        ws.update(range_name="L4", values=rows)
                    ws.update(range_name="B11", values=[[
                        pd.Timestamp.now(tz="Asia/Seoul")
                        .strftime("%Y-%m-%d %H:%M:%S")]])
                    st.success(f"✅ '{sheet_nm}' L4에 {len(rows)}건 전송 완료")
            except Exception as e:
                st.error(f"전송 실패: {type(e).__name__}: {e}")
        if not gs_url:
            st.caption("⚙️ 개인 설정 탭에서 스프레드시트 URL을 먼저 저장하세요.")


# ══════════════════════════════════════════════════════════
# 탭4: 전략 소개
# ══════════════════════════════════════════════════════════
def render_intro_tab(params: dict):
    p: ManseParams = params["mp"]
    st.markdown("""
### 🎛️ 만능 스위치 매매법

주(週) 단위로 시장을 **바닥 / 중간 / 천장** 세 구간으로 나누고,
구간마다 완전히 다른 매매 파라미터 세트를 적용하는 스위칭 전략입니다.
원본은 쪼꼬야옹님의 *만능 스위치 BT 버전 v2.1* 구글시트이며,
본 엔진은 시트의 RECORD/DB 탭 수식을 1:1로 이식했습니다.

---

#### 1️⃣ 모드 판정 — 3가지 기준 중 택1

| 기준 | 지표 | 구간 판정 |
|---|---|---|
| **중심주가** | 지표종목 주봉종가 ÷ 중심주가(월복리 추세선) − 1 | 이격도 < 바닥범위 → 바닥 |
| **이평선** | 지표종목 주봉종가 ÷ N일 이동평균 − 1 | 바닥범위 ≤ 이격도 ≤ 천장범위 → 중간 |
| **RSI** | 지표종목 주봉 N기간 단순 RSI | 이격도 > 천장범위 → 천장 |

> **N주차의 모드는 N−1주차 지표로 결정**됩니다. 미래 데이터를 쓰지 않습니다.
> 중심주가는 2016년 1월을 기준으로 한 월복리 추세선입니다
> (SOXX: 31.224157 × 1.018129^m, QQQ: 100.31 × 1.0132^m).

---

#### 2️⃣ 티어 결정

| 방식 | 규칙 |
|---|---|
| **보유** | 현재 보유 포지션 수 + 1 |
| **빈자리** | 1~시드분할수 중 비어 있는 가장 낮은 번호 |

티어 번호가 **시드분할수를 넘으면 그날은 신규 매수를 하지 않습니다.**

---

#### 3️⃣ 하루의 흐름

```
매수주문가 = ROUNDDOWN(전일종가 × (1 + 매수목표), 2)
1회 시드   = MIN(투자금 × 시드비중, 예수금)

  체결 조건 : 매수주문가 ≥ 당일 종가   → 당일 종가로 체결 (LOC)
  수량      : 티어 = 시드분할수 이거나 '정량매수' → INT(시드 ÷ 주문가)
              그 외 → 주문가에서 '추가 매수 범위'까지 사다리 주문을 깔고,
                      평균단가가 당일 종가 이상인 구간까지 체결

매도목표가 = ROUNDUP(체결가 × (1 + 매도목표), 2)
  손절예정일(= 매수일 + 손절일수 영업일) **이전에** 종가가 목표 도달 → 그날 매도
  미도달 → 손절예정일에 MOC(종가) 청산
```

**매도날 매수X**를 켜면 그날 매도가 예정된 포지션이 있을 때
매수주문가를 `최저 매도목표가 − $0.01` 이하로 눌러 같은 날 사고파는 것을 막습니다.
**MOC날 매수**를 끄면 손절(MOC) 예정일에는 아예 매수하지 않습니다.

---

#### 4️⃣ 복리 관리

```
갱신 주기마다:  구간 실현손익 합계(BFS)
   BFS ≥ 0 → 투자금 += BFS × 이익 복리율
   BFS < 0 → 투자금 += BFS × 손실 복리율      (손실은 덜 반영 = 방어적)
```
투자금(갱신)은 다음 날 **1회 시드**의 기준이 됩니다. 예수금과는 별개입니다.
""")

    st.markdown("---")
    st.markdown("#### 📌 현재 설정 요약")
    c1, c2 = st.columns(2)
    with c1:
        st.write({
            "투자 종목": p.ticker,
            "모드 기준": p.mode_basis,
            "지표 종목": p.indicator_ticker(),
            "티어계산 방식": p.tier_method,
            "최초 원금": f"${p.principal:,.0f}",
            "이익/손실 복리율": f"{p.profit_comp:.0%} / {p.loss_comp:.0%}",
            "갱신 주기": f"{p.renew_cycle} 거래일",
        })
    with c2:
        st.write({
            "추가 주문 방식": p.order_type,
            "추가 매수 범위": f"{p.extra_range:.1%}",
            "추가 주문 건수" if p.order_type == "추가 주문 건수 고정"
            else "추가 매수 간격": (p.extra_count
                                if p.order_type == "추가 주문 건수 고정"
                                else p.extra_step),
            "수수료 / SEC": f"{p.fee:.4%} / {p.sec_fee:.5%}",
        })
    for lv in LEVELS:
        lp = p.levels[lv]
        with st.expander(f"{lv} 구간 파라미터 (분할 {lp.split}, "
                         f"정량매수 {lp.fixed_qty}, 매도날매수X "
                         f"{lp.no_buy_on_sell_day}, MOC날매수 {lp.moc_day_buy})"):
            st.dataframe(_tier_df(lp), use_container_width=True, hide_index=True)

    st.markdown("---")
    st.success("✅ **검증 완료**")
    st.markdown("""
| 대상 | 방법 | 결과 |
|---|---|---|
| **이평선 모드 (4일)** | 원본 시트(이평-2티어_4일) 전 구간 대조 | 2011-01-03 ~ 2026-08-20 **3,931 거래일** 모드·티어·주문가·수량·예수금·총자산·투자금 **전부 일치** |
| **이평선 모드 (1일)** | 원본 시트(이평-2티어_1일) 전 구간 대조 | 동일 구간 **3,931 거래일** 전부 일치 (최종자산 $2,872,676,172) |
| **중심주가 모드** | 원본 시트(중심주가-2티어) 전 구간 대조 | 동일 구간 **3,932 거래일 전부 일치** |
| **RSI 모드** | 원본 시트(RSI-2티어) 전 구간 대조 | 2011-01-03 ~ 2026-08-20 **3,931 거래일** 모드·티어·주문가·수량·보유 **0 오차**, 예수금·총자산·투자금 최대오차 $0.5(시트 정수 표시 한계) |
| **RSI 지표** | 시트 DB탭이 계산해 둔 `WRSI` 열과 대조 | **855주** 비교, 최대오차 0.005 (= 시트 표시 소수 2자리 반올림 한계) |

매매 로직(티어 결정·주문가·체결·사다리 수량·MOC 청산·복리 갱신)은 **모드와 무관하게 공통**이며,
세 가지 모드 기준(이평선·중심주가·RSI) 모두 원본 시트로 전 구간 검증되었습니다.
""")

    st.markdown("#### 📦 내장 프리셋 성과")
    st.caption("동일 조건 — SOXL · 원금 $10,000 · 2011-01-03 ~ 2026-08-20 · "
               "로컬 DB(무조정 종가)")
    st.markdown("""
| 프리셋 | 모드 | 최종자산 | CAGR | MDD | Calmar | 승률 | 거래 | 손익비 |
|---|---|---|---|---|---|---|---|---|
| 📈 이평선형 (4일) ⭐ | 이평선 | $2,583,055,279 | 121.98% | **-35.86%** | **3.40** | 62.2% | 2,079 | 1.11 |
| 📉 이평선형 (1일) | 이평선 | **$2,872,676,172** | **123.50%** | -38.16% | 3.24 | 58.7% | 2,489 | 1.20 |
| 📊 중심주가형 | 중심주가 | $447,106,358 | 98.42% | -34.70% | 2.84 | 57.1% | 2,800 | 1.09 |
| 🌀 RSI형 | RSI | $1,124,382,119 | 110.48% | -38.99% | 2.83 | 55.4% | 2,782 | **1.40** |

**4종 모두 원본 시트로 전 구간(3,931 거래일) 검증 완료**되었습니다.

> ⚠️ 이 수치는 **거래비용 0% 가정**입니다. 보유기간이 짧아 비용에 민감하니
> 사이드바에서 실제 수수료를 넣고 다시 확인하세요.
> 초기 저가 구간(2011~2015)의 센트 반올림 의존도가 커서 절대 수익률은 과장되어 있습니다.

> 📌 **거래횟수 정의** — 위 표와 구간별 통계는 **청산 완료 건수** 기준입니다.
> 원본 시트의 '거래횟수'는 **매수 체결 건수**(미청산 포함)라 몇 건 더 많게 표시됩니다.
> 승률·평균수익률·손익비는 시트도 청산 기준이라 동일합니다.
""")
    st.caption("※ 주봉 정렬 차이: 시트는 구글 파이낸스 주봉 871개(2010-01-04 시작 바 + "
               "2026-09-04 미래 진행중 바 포함), 엔진은 일봉 리샘플 869개. "
               "차이는 이 2개뿐이며 둘 다 백테스트 구간 밖입니다.")

    # ── 성과 분석 ──
    render_perf_analysis(params)


# ══════════════════════════════════════════════════════════
# 전략 성과 분석 (전략 소개 탭 하단)
# ══════════════════════════════════════════════════════════
def render_perf_analysis(params: dict):
    """전략 성과 분석 — 사이드바 값 또는 프리셋으로 백테스트 후 상세 분석."""
    st.markdown("---")
    st.subheader("📊 전략 성과 분석")
    st.markdown("사이드바의 파라미터와 기간 설정을 기반으로 성과를 분석합니다.")

    p0: ManseParams = params["mp"]
    src_opts = ["📐 사이드바 설정값"] + [pr["label"] for pr in _MANSE_PRESETS]
    sel = st.radio("파라미터 소스", src_opts, index=0, horizontal=True,
                   key="ms_perf_src")
    si = src_opts.index(sel)
    up = (copy.deepcopy(p0) if si == 0
          else preset_to_params(_MANSE_PRESETS[si - 1], p0.ticker, p0.principal))
    if si > 0:
        # 프리셋을 써도 기간·자본·수수료는 사이드바 설정을 유지
        up.fee, up.sec_fee = p0.fee, p0.sec_fee

    with st.expander("🔍 적용 파라미터 확인", expanded=False):
        if up.mode_basis == "이평선":
            sw = (f"이평선 {up.ma_ticker} MA{up.ma_days} · "
                  f"바닥 &lt; {up.ma_low*100:+.2f}% · 천장 &gt; {up.ma_high*100:+.2f}%")
        elif up.mode_basis == "중심주가":
            sw = (f"중심주가 {up.center_ticker} · "
                  f"바닥 &lt; {up.center_low*100:+.2f}% · 천장 &gt; {up.center_high*100:+.2f}%")
        else:
            sw = (f"RSI {up.rsi_ticker} {up.rsi_period}주 · "
                  f"바닥 &lt; {up.rsi_low:.0f} · 천장 &gt; {up.rsi_high:.0f}")
        st.markdown(f"**🔀 스위치** — {sw} · 티어계산 {up.tier_method}",
                    unsafe_allow_html=True)
        for lv in LEVELS:
            L = up.levels[lv]
            ts = " · ".join(
                f"T{i+1}(비중 {t.seed_w*100:.1f}% / 매수 {t.buy_gap*100:+.2f}% / "
                f"매도 {t.sell_gap*100:+.2f}% / 손절 {t.stop_days}일)"
                for i, t in enumerate(L.tiers[:L.split]))
            st.markdown(f"**{lv}** — 분할 {L.split} · {ts}")
        st.caption(
            f"복리 이익 {up.profit_comp:.0%} / 손실 {up.loss_comp:.0%} · "
            f"갱신주기 {up.renew_cycle}일 · 수수료 {up.fee*100:.4f}%(편도) · "
            f"추가 {up.order_type} {up.extra_count if up.order_type.endswith('건수 고정') else up.extra_step} · "
            f"기간 {params['bt_start_date']} ~ {params['bt_end_date']} · "
            f"원금 ${up.principal:,.0f}")

    if st.button("▶ 성과 분석 실행", type="primary", key="ms_perf_run",
                 use_container_width=True):
        with st.spinner("데이터 로드 및 분석 중..."):
            try:
                prices = _load_prices(up.needed_tickers(), params["data_source"])
                warn = _data_health(prices)
                r = run_backtest(prices, up, start=params["bt_start_date"],
                                 end=params["bt_end_date"])
            except Exception as e:
                st.error(f"⚠️ 분석 실패: {e}")
                return
        if "error" in r:
            st.error(r["error"])
            return
        st.session_state["ms_perf_res"] = r
        st.session_state["ms_perf_label"] = sel
        st.session_state["ms_perf_warn"] = warn
        st.session_state["ms_perf_p"] = up

    res = st.session_state.get("ms_perf_res")
    if res is None:
        st.info("👆 **'성과 분석 실행'** 버튼을 클릭하면 현재 파라미터 기준의 "
                "성과 분석 결과를 확인할 수 있습니다.\n\n"
                "💡 사이드바 설정값 또는 프리셋 4종을 선택하여 비교 분석할 수 있습니다.")
        return

    _show_health(st.session_state.get("ms_perf_warn", []))
    up = st.session_state.get("ms_perf_p", up)
    st.caption(f"📌 분석 기준: **{st.session_state.get('ms_perf_label', '')}**  ·  "
               f"{up.ticker} · 모드 {up.mode_basis}")

    df, trades, m = res["df"], res["trades"], res["metrics"]
    total = df["총자산"]
    dd = total / total.cummax() - 1
    dr = total.pct_change().dropna()
    neg = dr[dr < 0]
    sortino = (dr.mean() / neg.std() * np.sqrt(252)
               if len(neg) > 1 and neg.std() else np.nan)

    _cards([
        {"label": "CAGR", "value": _fmt_pct(m["CAGR"]),
         "fg": _sign_color(m["CAGR"])},
        {"label": "총 수익률", "value": _fmt_pct(m["총수익률"], 1),
         "fg": _sign_color(m["총수익률"]),
         "sub": _fmt_mult(m["최종자산"] / up.principal if up.principal else np.nan)},
        {"label": "최대 MDD", "value": _fmt_pct(m["MDD"]), "fg": _NEG},
        {"label": "Calmar", "value": ("-" if not m["MDD"]
                                      else f"{abs(m['CAGR']/m['MDD']):.3f}")},
        {"label": "Sharpe", "value": ("-" if np.isnan(m["샤프"])
                                      else f"{m['샤프']:.3f}")},
        {"label": "Sortino", "value": ("-" if np.isnan(sortino)
                                       else f"{sortino:.3f}")},
    ])

    # ── 연도별 성과 ──
    st.markdown("#### 📅 연도별 성과")
    rows = []
    for y, g in df.groupby(df.index.year):
        pos = df.index.get_loc(g.index[0])
        base = float(df["총자산"].iloc[pos - 1]) if pos > 0 else float(g["총자산"].iloc[0])
        ev = float(g["총자산"].iloc[-1])
        pk = g["총자산"].cummax()
        vc = g["모드"].value_counts()
        tot = max(int(vc.sum()), 1)
        cl = trades[pd.to_datetime(trades["매도일"]).dt.year == y] if len(trades) else pd.DataFrame()
        rows.append({
            "연도": int(y), "연간수익률": ev / base - 1 if base > 0 else np.nan,
            "연말자산": ev, "MDD": float((g["총자산"] / pk - 1).min()),
            "실현손익": float(g["실현손익"].sum(skipna=True)),
            "매도": int(len(cl)),
            "승률": (float((cl["실현손익"] > 0).mean()) if len(cl) else np.nan),
            "바닥%": vc.get("바닥", 0) / tot, "중간%": vc.get("중간", 0) / tot,
            "천장%": vc.get("천장", 0) / tot,
        })
    ydf = pd.DataFrame(rows)
    st.dataframe(ydf.style.format({
        "연간수익률": "{:+.2%}", "연말자산": "${:,.0f}", "MDD": "{:.2%}",
        "실현손익": "${:+,.0f}", "승률": "{:.1%}",
        "바닥%": "{:.0%}", "중간%": "{:.0%}", "천장%": "{:.0%}"}, na_rep="-")
        .map(lambda v: ("color:#2E7D32;font-weight:700" if isinstance(v, float) and v > 0
                        else ("color:#C62828;font-weight:700" if isinstance(v, float) and v < 0 else "")),
             subset=["연간수익률", "실현손익"]),
        use_container_width=True, hide_index=True)

    # ── 월별 수익률 히트맵 ──
    st.markdown("#### 🗓️ 월별 수익률 히트맵")
    st.markdown(monthly_perf_table(df["총자산"], df["모드"],
                                   mode_short={"바닥": "바", "중간": "중", "천장": "천"}),
                unsafe_allow_html=True)

    # ── 총자산 추이 & 낙폭 ──
    st.markdown("#### 📈 총자산 추이 & 낙폭")
    fg = make_subplots(rows=2, cols=1, shared_xaxes=True,
                       row_heights=[0.7, 0.3], vertical_spacing=0.05)
    fg.add_trace(go.Scatter(x=df.index, y=total, name="총자산",
                            line=dict(color="#1565C0", width=1.5)), row=1, col=1)
    fg.add_trace(go.Scatter(x=df.index, y=dd * 100, name="낙폭(%)", fill="tozeroy",
                            fillcolor="rgba(198,40,40,0.15)",
                            line=dict(color="#C62828", width=1)), row=2, col=1)
    fg.update_yaxes(type="log", title_text="총자산 ($, 로그)", row=1, col=1)
    fg.update_yaxes(title_text="낙폭 (%)", row=2, col=1)
    fg.update_layout(height=500, legend=dict(orientation="h", y=1.03),
                     margin=dict(l=60, r=20, t=30, b=30), hovermode="x unified")
    st.plotly_chart(fg, use_container_width=True)

    # ── 모드 지표 추이 ──
    mfr = res["mode_frame"]
    gap_name = mfr.attrs.get("gap_name", "이격도")
    st.markdown(f"#### 🔄 {up.indicator_ticker()} 주봉 {gap_name} & 구간 변화")
    if up.mode_basis == "RSI":
        lo, hi = up.rsi_low, up.rsi_high
    elif up.mode_basis == "중심주가":
        lo, hi = up.center_low, up.center_high
    else:
        lo, hi = up.ma_low, up.ma_high
    f2 = go.Figure()
    f2.add_trace(go.Scatter(x=mfr.index, y=mfr[gap_name], name=gap_name,
                            line=dict(color="#555", width=1.3)))
    for lv in LEVELS:
        sub = mfr[mfr["모드"] == lv]
        if len(sub):
            f2.add_trace(go.Scatter(x=sub.index, y=sub[gap_name], mode="markers",
                                    name=lv, marker=dict(color=_MODE_COLOR[lv],
                                                         size=4, opacity=0.75)))
    for v, nm in ((lo, "바닥 경계"), (hi, "천장 경계")):
        f2.add_hline(y=v, line_dash="dot", opacity=0.6,
                     line_color=_MODE_COLOR["바닥" if nm.startswith("바닥") else "천장"],
                     annotation_text=nm, annotation_position="right")
    f2.update_layout(height=360, legend=dict(orientation="h", y=1.05),
                     margin=dict(l=50, r=20, t=30, b=30), hovermode="x unified")
    st.plotly_chart(f2, use_container_width=True)

    # ── 승률 & 손익비 ──
    st.markdown("#### 🎯 승률 & 손익비 분석")
    cl = trades[trades["실현손익"].notna()] if len(trades) else pd.DataFrame()
    if len(cl):
        w = cl[cl["실현손익"] > 0]
        l = cl[cl["실현손익"] < 0]
        aw = float(w["실현손익"].mean()) if len(w) else 0.0
        al = abs(float(l["실현손익"].mean())) if len(l) else 0.0
        _cards([
            {"label": "총 매도", "value": f"{len(cl):,}회",
             "sub": f"미청산 {m['미청산']}건"},
            {"label": "승률", "value": f"{len(w)/len(cl)*100:.1f}%",
             "fg": _POS, "bg": "#E8F5E9",
             "sub": f"승 {len(w):,} / 패 {len(l):,}"},
            {"label": "평균 익절", "value": f"${aw:+,.2f}", "fg": _POS},
            {"label": "평균 손절", "value": f"-${al:,.2f}", "fg": _NEG},
            {"label": "손익비", "value": ("-" if not al else f"{aw/al:.2f}")},
            {"label": "평균 보유", "value": f"{cl['보유일'].mean():.2f}일",
             "sub": f"중앙값 {cl['보유일'].median():.0f}일"},
        ])
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**구간별**")
            st.dataframe(res["by_level"].style.format({
                "승률": "{:.1%}", "평균수익률": "{:.2%}", "손익합": "${:,.0f}",
                "손익비": "{:.2f}", "평균보유일": "{:.1f}", "거래횟수": "{:.0f}"}),
                use_container_width=True, hide_index=True)
        with c2:
            st.markdown("**구간 × 티어별**")
            st.dataframe(res["by_tier"].style.format({
                "승률": "{:.1%}", "평균수익률": "{:.2%}", "손익합": "${:,.0f}",
                "손익비": "{:.2f}", "평균보유일": "{:.1f}", "거래횟수": "{:.0f}"}),
                use_container_width=True, hide_index=True)

        # 수익률 분포
        f3 = go.Figure()
        f3.add_trace(go.Histogram(x=cl["수익률"] * 100, nbinsx=60,
                                  marker_color="#8E8E93", name="1회 수익률"))
        f3.add_vline(x=0, line_color="#C62828", line_width=1.5)
        f3.update_layout(height=280, xaxis_title="1회 수익률 (%)",
                         yaxis_title="빈도", margin=dict(l=50, r=20, t=30, b=30),
                         title="체결 단위 수익률 분포")
        st.plotly_chart(f3, use_container_width=True)
    else:
        st.info("청산된 매매가 없습니다.")

    # ── 롤링 성과 ──
    st.markdown("#### 📉 롤링 1년 성과")
    if len(df) > 260:
        roll = total / total.shift(252) - 1
        rmdd = dd.rolling(252).min()
        f4 = make_subplots(rows=2, cols=1, shared_xaxes=True,
                           row_heights=[0.5, 0.5], vertical_spacing=0.06,
                           subplot_titles=("롤링 1년 수익률", "롤링 1년 MDD"))
        f4.add_trace(go.Scatter(x=df.index, y=roll * 100, name="1년 수익률",
                                line=dict(color="#1565C0", width=1.2)), row=1, col=1)
        f4.add_hline(y=0, line_dash="dot", line_color="#888", row=1, col=1)
        f4.add_trace(go.Scatter(x=df.index, y=rmdd * 100, name="1년 MDD",
                                fill="tozeroy", fillcolor="rgba(198,40,40,0.15)",
                                line=dict(color="#C62828", width=1)), row=2, col=1)
        f4.update_layout(height=420, showlegend=False,
                         margin=dict(l=50, r=20, t=50, b=30), hovermode="x unified")
        st.plotly_chart(f4, use_container_width=True)
        pos_ratio = float((roll.dropna() > 0).mean())
        st.caption(f"롤링 1년 수익률이 양수인 비율: **{pos_ratio*100:.1f}%** · "
                   f"최저 {roll.min()*100:.1f}% · 최고 {roll.max()*100:.1f}%")
    else:
        st.caption("롤링 1년 분석은 거래일 260일 이상일 때 표시됩니다.")

    st.download_button("⬇️ 연도별 성과 CSV",
                       ydf.to_csv(index=False).encode("utf-8-sig"),
                       file_name=f"manse_{up.ticker}_yearly.csv",
                       mime="text/csv", key="ms_perf_dl")

    # ── 파라미터 강건성 ──
    st.markdown("---")
    st.markdown("#### 🎲 파라미터 강건성")
    st.caption("이 파라미터가 '봉우리 꼭대기'인지 '고원'인지 확인합니다. "
               "매수목표·매도목표·손절일수를 무작위로 흔들어 결과 산포를 봅니다.")
    render_robustness_panel(
        _load_prices(up.needed_tickers(), params["data_source"]), up,
        params["bt_start_date"], params["bt_end_date"], key="perf")

    # ── 랜덤 시작일 1년 성과 ──
    st.markdown("---")
    render_random_start_panel(params, up)


# ══════════════════════════════════════════════════════════
# 랜덤 시작일 1년 성과 (시작 시점 운 의존도 측정)
# ══════════════════════════════════════════════════════════
def random_start_study(prices: dict, p: ManseParams, n: int = 200,
                       months: int = 12, seed: int = 42,
                       start=None, end=None, progress=None) -> dict:
    """무작위 시작일에서 N개월 운용했을 때의 성과 분포.

    전체 기간 백테스트 한 번의 롤링 수익률이 아니라, **매번 원금부터
    새로 시작**해 실제로 재시뮬레이션한다 (투자금 갱신·티어 상태가
    시작 시점에 초기화되므로 롤링과 결과가 다르다).
    """
    tk = p.ticker.upper()
    src = prices.get(tk)
    if src is None or len(src) == 0:
        return {"error": f"{tk} 가격 데이터가 없습니다."}
    idx = pd.DatetimeIndex(src.index)
    if start is not None:
        idx = idx[idx >= pd.Timestamp(start)]
    if end is not None:
        idx = idx[idx <= pd.Timestamp(end)]
    if len(idx) < 40:
        return {"error": "기간이 너무 짧습니다."}

    horizon = pd.DateOffset(months=months)
    # N개월을 온전히 채울 수 있는 시작일만 후보로
    last = idx[-1]
    cands = [d for d in idx if d + horizon <= last]
    if not cands:
        return {"error": f"{months}개월을 채울 수 있는 시작일이 없습니다."}

    rnd = random.Random(seed)
    picks = (cands if len(cands) <= n else rnd.sample(cands, n))
    picks = sorted(picks)

    mf = build_mode_frame(p, prices)
    rows = []
    for i, d0 in enumerate(picks):
        d1 = d0 + horizon
        m = run_backtest(prices, p, start=d0, end=d1, mode_frame=mf, light=True)
        if "error" not in m:
            rows.append({"시작일": d0, "종료일": min(d1, last),
                         "수익률": m["총수익률"], "MDD": m["MDD"],
                         "최종자산": m["최종자산"], "거래": m["거래횟수"]})
        if progress:
            progress((i + 1) / len(picks))
    if not rows:
        return {"error": "시뮬레이션 결과가 없습니다."}
    return {"df": pd.DataFrame(rows), "n": len(rows),
            "candidates": len(cands), "months": months}


def render_random_start_panel(params: dict, p: ManseParams):
    """랜덤 시작일 성과 분포 UI."""
    st.markdown("#### 🎯 랜덤 시작일 성과 분포")
    st.caption("아무 날에나 시작했다면 어땠을까 — 무작위 시작일마다 **원금부터 새로** "
               "N개월 운용해 재시뮬레이션합니다. 전체기간 1회 백테스트가 "
               "'운 좋은 시작일' 덕을 봤는지 가려냅니다.")
    c1, c2, c3, c4 = st.columns(4)
    n = int(c1.number_input("표본 수", value=150, min_value=20, max_value=1000,
                            step=50, key="ms_rs_n"))
    months = int(c2.number_input("운용 개월", value=12, min_value=1, max_value=60,
                                 step=1, key="ms_rs_m"))
    seed = int(c3.number_input("랜덤 시드", value=42, step=1, key="ms_rs_seed"))
    c4.caption(f"1회당 약 {months/12*0.02:.2f}초 · "
               f"예상 {n*months/12*0.02:.0f}초")

    if st.button("🎯 랜덤 시작 분석 실행", key="ms_rs_run",
                 use_container_width=True):
        prices = _load_prices(p.needed_tickers(), params["data_source"])
        bar = st.progress(0.0)
        r = random_start_study(prices, p, n=n, months=months, seed=seed,
                               start=params["bt_start_date"],
                               end=params["bt_end_date"], progress=bar.progress)
        bar.empty()
        if "error" in r:
            st.error(r["error"])
            return
        # 결과를 보관해야 아래 정렬/필터 위젯을 조작해도 표가 유지된다
        st.session_state["ms_rs_res"] = r
        st.session_state["ms_rs_meta"] = (p.ticker, p.mode_basis, n, months, seed)

    r = st.session_state.get("ms_rs_res")
    if r is None:
        return
    _meta = st.session_state.get("ms_rs_meta")
    if _meta:
        _tk, _mb, _n, _mo, _sd = _meta
        st.caption(f"📌 실행 조건: {_tk} · 모드 {_mb} · 표본 {_n:,} · "
                   f"{_mo}개월 · 시드 {_sd}"
                   + ("  ⚠️ 위 설정을 바꿨습니다 — 다시 실행하세요"
                      if (_tk, _mb, _n, _mo, _sd)
                      != (p.ticker, p.mode_basis, n, months, seed) else ""))

    d = r["df"]
    ret, mdd = d["수익률"], d["MDD"]
    loss_p = float((ret < 0).mean())
    _cards([
        {"label": f"{r['months']}개월 수익률 중앙값",
         "value": _fmt_pct(float(ret.median()), 1),
         "fg": _sign_color(ret.median())},
        {"label": "평균", "value": _fmt_pct(float(ret.mean()), 1),
         "fg": _sign_color(ret.mean()),
         "sub": f"표준편차 {ret.std()*100:.1f}%p"},
        {"label": "손실 확률", "value": f"{loss_p*100:.1f}%",
         "fg": (_NEG if loss_p >= 0.3 else _POS),
         "sub": f"{int((ret < 0).sum())} / {r['n']} 표본"},
        {"label": "최저 ~ 최고",
         "value": f"{ret.min()*100:+.0f}% ~ {ret.max()*100:+.0f}%",
         "sub": f"후보 시작일 {r['candidates']:,}일 중 {r['n']:,}개 추출"},
        {"label": "MDD 중앙값", "value": _fmt_pct(float(mdd.median())),
         "fg": _NEG, "sub": f"최악 {mdd.min()*100:.1f}%"},
    ])

    q = ret.quantile([0.05, 0.25, 0.5, 0.75, 0.95])
    qm = mdd.quantile([0.05, 0.25, 0.5, 0.75, 0.95])
    st.markdown("**분위수**")
    st.dataframe(pd.DataFrame({
        "분위": ["최악 5%", "하위 25%", "중앙값", "상위 25%", "상위 5%"],
        "수익률": q.values, "MDD": qm.values[::-1],
    }).style.format({"수익률": "{:+.1%}", "MDD": "{:.1%}"}),
        use_container_width=True, hide_index=True)

    f1 = make_subplots(rows=1, cols=2,
                       subplot_titles=(f"{r['months']}개월 수익률 분포",
                                       "MDD 분포"))
    f1.add_trace(go.Histogram(x=ret * 100, nbinsx=40, marker_color="#1565C0",
                              name="수익률"), row=1, col=1)
    f1.add_vline(x=0, line_color="#C62828", line_width=1.5, row=1, col=1)
    f1.add_trace(go.Histogram(x=mdd * 100, nbinsx=40, marker_color="#C62828",
                              name="MDD"), row=1, col=2)
    f1.update_layout(height=320, showlegend=False,
                     margin=dict(l=50, r=20, t=50, b=30))
    f1.update_xaxes(title_text="수익률 (%)", row=1, col=1)
    f1.update_xaxes(title_text="MDD (%)", row=1, col=2)
    st.plotly_chart(f1, use_container_width=True)

    f2 = go.Figure()
    f2.add_trace(go.Scatter(x=d["시작일"], y=ret * 100, mode="markers",
                            marker=dict(size=5, color=ret * 100,
                                        colorscale="RdYlGn", cmid=0,
                                        showscale=True,
                                        colorbar=dict(title="수익률(%)")),
                            name="시작일별 수익률",
                            text=[f"{a.date()} → {b.date()}"
                                  for a, b in zip(d["시작일"], d["종료일"])]))
    f2.add_hline(y=0, line_dash="dot", line_color="#888")
    f2.update_layout(height=340, xaxis_title="시작일",
                     yaxis_title=f"{r['months']}개월 수익률 (%)",
                     margin=dict(l=50, r=20, t=30, b=30),
                     title="시작 시점에 따른 성과 편차")
    st.plotly_chart(f2, use_container_width=True)

    # ── 결과 목록 ──
    view = d.copy()
    view.insert(0, "#", range(1, len(view) + 1))
    view["시작일"] = pd.to_datetime(view["시작일"]).dt.strftime("%Y-%m-%d")
    view["종료일"] = pd.to_datetime(view["종료일"]).dt.strftime("%Y-%m-%d")
    view["연도"] = pd.to_datetime(d["시작일"]).dt.year
    view = view[["#", "시작일", "종료일", "연도", "수익률", "MDD",
                 "최종자산", "거래"]]
    fmt = {"수익률": "{:+.2%}", "MDD": "{:.2%}", "최종자산": "${:,.0f}"}

    def _sty(v):
        if isinstance(v, float):
            return ("color:#2E7D32;font-weight:700" if v > 0
                    else ("color:#C62828;font-weight:700" if v < 0 else ""))
        return ""

    st.markdown("##### 📋 전체 결과")
    fc1, fc2, fc3 = st.columns([1.2, 1.2, 2])
    sort_by = fc1.selectbox("정렬", ["시작일 순", "수익률 높은 순",
                                   "수익률 낮은 순", "MDD 나쁜 순"],
                            key="ms_rs_sort")
    only = fc2.selectbox("필터", ["전체", "손실난 표본만", "수익난 표본만"],
                         key="ms_rs_filter")
    yrs_all = sorted(view["연도"].unique())
    yr_pick = fc3.multiselect("시작 연도", yrs_all, default=yrs_all,
                              key="ms_rs_years")

    vv = view[view["연도"].isin(yr_pick)] if yr_pick else view
    if only == "손실난 표본만":
        vv = vv[vv["수익률"] < 0]
    elif only == "수익난 표본만":
        vv = vv[vv["수익률"] >= 0]
    if sort_by == "수익률 높은 순":
        vv = vv.sort_values("수익률", ascending=False)
    elif sort_by == "수익률 낮은 순":
        vv = vv.sort_values("수익률")
    elif sort_by == "MDD 나쁜 순":
        vv = vv.sort_values("MDD")
    else:
        vv = vv.sort_values("시작일")

    st.caption(f"표시 {len(vv):,} / 전체 {len(view):,} 표본 "
               f"· 표 머리글을 클릭해도 정렬됩니다")
    st.dataframe(vv.style.format(fmt).map(_sty, subset=["수익률"]),
                 use_container_width=True, hide_index=True,
                 height=min(560, 38 * (len(vv) + 1) + 3))

    w1, w2 = st.columns(2)
    with w1:
        with st.expander("📉 최악 10개 시작일", expanded=False):
            st.dataframe(view.nsmallest(10, "수익률").style.format(fmt)
                         .map(_sty, subset=["수익률"]),
                         use_container_width=True, hide_index=True)
    with w2:
        with st.expander("📈 최고 10개 시작일", expanded=False):
            st.dataframe(view.nlargest(10, "수익률").style.format(fmt)
                         .map(_sty, subset=["수익률"]),
                         use_container_width=True, hide_index=True)

    # ── 시작 연도별 요약 ──
    with st.expander("📅 시작 연도별 요약", expanded=False):
        ysum = view.groupby("연도").agg(
            표본=("수익률", "size"),
            수익률_중앙값=("수익률", "median"),
            수익률_평균=("수익률", "mean"),
            최저=("수익률", "min"), 최고=("수익률", "max"),
            손실비율=("수익률", lambda x: (x < 0).mean()),
            MDD_중앙값=("MDD", "median")).reset_index()
        st.dataframe(ysum.style.format({
            "수익률_중앙값": "{:+.1%}", "수익률_평균": "{:+.1%}",
            "최저": "{:+.1%}", "최고": "{:+.1%}",
            "손실비율": "{:.0%}", "MDD_중앙값": "{:.1%}"})
            .map(_sty, subset=["수익률_중앙값", "수익률_평균"]),
            use_container_width=True, hide_index=True)

    dl1, dl2 = st.columns(2)
    dl1.download_button("⬇️ 전체 결과 CSV",
                        view.to_csv(index=False).encode("utf-8-sig"),
                        file_name=f"manse_{p.ticker}_randomstart.csv",
                        mime="text/csv", key="ms_rs_dl",
                        use_container_width=True)
    dl2.download_button("⬇️ 현재 필터 결과 CSV",
                        vv.to_csv(index=False).encode("utf-8-sig"),
                        file_name=f"manse_{p.ticker}_randomstart_filtered.csv",
                        mime="text/csv", key="ms_rs_dl2",
                        use_container_width=True)

    if loss_p >= 0.35:
        st.error(f"⚠️ 표본의 {loss_p*100:.0f}% 가 손실 — 시작 시점 운에 크게 좌우됩니다.")
    elif loss_p >= 0.2:
        st.warning(f"⚠️ 표본의 {loss_p*100:.0f}% 가 손실 — 시작 시점 편차가 있습니다.")
    else:
        st.success(f"✅ 손실 확률 {loss_p*100:.0f}% — 시작 시점 의존도가 낮습니다.")


# ══════════════════════════════════════════════════════════
# 탭5: DB 조회 / 관리
# ══════════════════════════════════════════════════════════
def render_db_tab(params: dict = None):
    st.markdown("### 📂 로컬 종가 DB")
    st.caption("야후 파이낸스 장애·레이트리밋에 대비한 백업 종가 저장소입니다. "
               "원칙은 **B방식** — 이미 저장된 과거 종가는 덮어쓰지 않고 새 날짜만 누적합니다.")

    status = pricedb.db_status()
    if len(status):
        st.dataframe(status, use_container_width=True, hide_index=True)
    else:
        st.warning("아직 저장된 종가가 없습니다. 아래에서 추가하세요.")
    st.caption(f"저장 위치: `{pricedb.DB_DIR}`")

    if _IS_CLOUD:
        st.warning(
            "☁️ **클라우드에서는 이 폴더가 임시 저장소입니다.** 여기서 갱신한 내용은 "
            "앱이 재시작되면 사라지고, 리포지토리에 커밋된 `pricedb/*.csv` 상태로 되돌아갑니다.\n\n"
            "다만 데이터 소스를 **'로컬 DB + 야후 최신분 (권장)'** 으로 두면 DB 이후 구간을 "
            "매번 야후에서 받아오므로, DB가 조금 낡아도 주문표는 최신 종가로 계산됩니다. "
            "영구 반영은 로컬 PC에서 갱신 후 커밋·푸시하세요.")

    st.markdown("---")
    t1, t4, t2, t3 = st.tabs(["🔄 야후에서 갱신", "☁️ 구글시트 백업",
                              "📥 시트 CSV 가져오기", "🔎 조회 · 내보내기"])

    # ── 야후 동기화 ──
    with t1:
        tickers = st.text_input("갱신할 티커 (쉼표 구분)",
                                value=",".join(pricedb.list_tickers()) or "SOXL,QQQ",
                                key="ms_db_sync_tk")
        gs_url = _gs_url()
        also_sheet = st.checkbox(
            "구글시트에도 함께 기록 (영구 백업)", value=bool(gs_url),
            disabled=not gs_url, key="ms_db_sync_sheet",
            help="개인 설정 탭에 스프레드시트 URL을 저장하면 활성화됩니다.")
        if not gs_url:
            st.caption("⚙️ 개인 설정 탭에서 스프레드시트 URL을 저장하면 "
                       "야후 데이터를 시트에도 영구 보관할 수 있습니다.")
        if st.button("🔄 야후에서 신규분 받아 누적", key="ms_db_sync",
                     use_container_width=True):
            for tk in [x.strip().upper() for x in tickers.split(",") if x.strip()]:
                with st.spinner(f"{tk} 동기화 중..."):
                    try:
                        r = pricedb.sync_all(
                            tk, gs_url=(gs_url if also_sheet else ""))
                        parts = [f"야후 {r['yahoo']:,}행 수신",
                                 f"로컬 +{r['local']:,}행"]
                        if also_sheet and gs_url:
                            parts.append(f"시트 +{r['sheet']:,}행")
                        msg = f"{tk}: " + " · ".join(parts)
                        if r["error"]:
                            st.warning(f"{msg}  ({r['error']})")
                        elif r["local"] or r["sheet"]:
                            st.success(msg)
                        else:
                            st.info(f"{tk}: 새 데이터 없음 (이미 최신)")
                    except Exception as e:
                        st.error(f"{tk} 실패: {e}")
            _load_one.clear()

    # ── 구글시트 백업 ──
    with t4:
        st.markdown("야후가 막혔을 때를 대비해 종가를 **구글시트에 영구 보관**합니다. "
                    "클라우드는 파일시스템이 임시라 시트가 실질적인 저장소입니다.")
        st.markdown("""
| 순서 | 소스 | 설명 |
|---|---|---|
| 1 | 로컬 DB `pricedb/*.csv` | 과거 확정 종가 (가장 빠름) |
| 2 | 야후 파이낸스 | DB 이후 구간만 이어붙임 |
| 3 | 사용자 시트 `pricedb_{티커}` 탭 | 야후 실패 + DB 노후 시 |
| 4 | 원본 만능시트 DB 탭 | 최후 수단 (읽기 전용, SOXL·QQQ) |
""")
        gs_url2 = _gs_url()
        if not gs_url2:
            st.warning("⚙️ 개인 설정 탭에서 **스프레드시트 URL** 을 먼저 저장해주세요.")
        else:
            st.caption(f"대상 시트: `{gs_url2[:60]}...`  "
                       f"탭 이름: `{pricedb.GSHEET_TAB_PREFIX}{{티커}}`")
        tk_b = st.text_input("티커 (쉼표 구분)",
                             value=",".join(pricedb.list_tickers()) or "SOXL,QQQ",
                             key="ms_db_gs_tk")
        _tks_b = [x.strip().upper() for x in tk_b.split(",") if x.strip()]

        b1, b2, b3 = st.columns(3)
        with b1:
            if st.button("⬆️ 로컬 DB → 시트 기록", key="ms_db_push",
                         use_container_width=True, disabled=not gs_url2):
                for tk in _tks_b:
                    with st.spinner(f"{tk} 시트 기록 중..."):
                        try:
                            n = pricedb.push_to_gsheet(gs_url2, tk)
                            st.success(f"{tk}: 시트에 {n:,}행 추가") if n else                                 st.info(f"{tk}: 시트가 이미 최신")
                        except Exception as e:
                            st.error(f"{tk} 실패: {e}")
        with b2:
            if st.button("⬇️ 시트 → 로컬 DB", key="ms_db_pull",
                         use_container_width=True, disabled=not gs_url2):
                for tk in _tks_b:
                    with st.spinner(f"{tk} 시트에서 로드 중..."):
                        try:
                            df = pricedb.pull_from_gsheet(gs_url2, tk)
                            if df.empty:
                                st.warning(f"{tk}: 시트에 데이터가 없습니다 "
                                           f"(탭 `{pricedb.gsheet_tab(tk)}`)")
                            else:
                                n = pricedb.save_prices(tk, df)
                                st.success(f"{tk}: {len(df):,}행 중 {n:,}행 신규 적재")
                        except Exception as e:
                            st.error(f"{tk} 실패: {e}")
                _load_one.clear()
        with b3:
            if st.button("📗 원본 만능시트에서 가져오기", key="ms_db_src",
                         use_container_width=True):
                for tk in _tks_b:
                    if tk not in pricedb.SOURCE_DB_COLS:
                        st.warning(f"{tk}: 원본 시트에 매핑된 열이 없습니다 "
                                   f"(지원: {', '.join(pricedb.SOURCE_DB_COLS)})")
                        continue
                    with st.spinner(f"{tk} 원본 시트에서 로드 중..."):
                        try:
                            df = pricedb.fetch_from_source_sheet(tk)
                            if df.empty:
                                st.warning(f"{tk}: 원본 시트에서 읽지 못했습니다 "
                                           f"(권한/네트워크 확인)")
                            else:
                                n = pricedb.save_prices(tk, df)
                                st.success(f"{tk}: {len(df):,}행 중 {n:,}행 신규 적재 "
                                           f"({df.index.min().date()} ~ "
                                           f"{df.index.max().date()})")
                        except Exception as e:
                            st.error(f"{tk} 실패: {e}")
                _load_one.clear()

        if gs_url2 and st.button("🔍 시트 백업 현황 조회", key="ms_db_gs_stat",
                                 use_container_width=True):
            rows = []
            for tk in _tks_b:
                try:
                    df = pricedb.pull_from_gsheet(gs_url2, tk)
                except Exception:
                    df = pd.DataFrame()
                rows.append({"티커": tk, "시트 행수": len(df),
                             "시트 최종일": df.index[-1].date() if len(df) else None,
                             "로컬 행수": len(pricedb.load_prices(tk))})
            st.dataframe(pd.DataFrame(rows), use_container_width=True,
                         hide_index=True)

    # ── 시트 CSV 임포트 ──
    with t2:
        st.markdown("만능시트의 **DB 탭**을 CSV로 내보낸 파일을 올리면 "
                    "지정한 열 쌍(날짜/종가)을 읽어 누적합니다.")
        up = st.file_uploader("DB 탭 CSV 업로드", type=["csv"], key="ms_db_up")
        c1, c2, c3 = st.columns(3)
        imp_tk = c1.text_input("티커", value="SOXL", key="ms_db_imp_tk")
        dcol = int(c2.number_input("날짜 열 (0-based)", value=4, min_value=0,
                                   step=1, key="ms_db_dc"))
        ccol = int(c3.number_input("종가 열 (0-based)", value=5, min_value=0,
                                   step=1, key="ms_db_cc"))
        st.caption("만능시트 기본: 투자종목 일별 = 4,5 · 지표종목(QQQ) 일별 = 13,14")
        if up is not None and st.button("📥 가져오기", key="ms_db_imp",
                                        use_container_width=True):
            tmp = os.path.join(_CONFIG_DIR, "_upload.csv")
            os.makedirs(_CONFIG_DIR, exist_ok=True)
            with open(tmp, "wb") as f:
                f.write(up.getbuffer())
            try:
                df = pricedb.import_pairs_from_csv(tmp, dcol, ccol)
                if df.empty:
                    st.error("해당 열에서 (날짜, 종가) 를 찾지 못했습니다.")
                else:
                    n = pricedb.save_prices(imp_tk.strip().upper(), df)
                    st.success(f"{imp_tk.upper()}: {len(df):,}행 중 {n:,}행 신규 적재 "
                               f"({df.index.min().date()} ~ {df.index.max().date()})")
                    _load_one.clear()
            except Exception as e:
                st.error(f"실패: {e}")
            finally:
                try:
                    os.remove(tmp)
                except Exception:
                    pass

    # ── 조회 ──
    with t3:
        tks = pricedb.list_tickers()
        if not tks:
            st.info("저장된 티커가 없습니다.")
            return
        tk = st.selectbox("티커", tks, key="ms_db_view_tk")
        df = pricedb.load_prices(tk)
        c1, c2 = st.columns(2)
        sd = c1.date_input("시작", df.index.min().date(), key="ms_db_sd")
        ed = c2.date_input("종료", df.index.max().date(), key="ms_db_ed")
        v = df[(df.index >= pd.Timestamp(sd)) & (df.index <= pd.Timestamp(ed))]
        fig = go.Figure(go.Scatter(x=v.index, y=v["Close"], name=tk,
                                   line=dict(width=1.2)))
        fig.update_layout(height=340, title=f"{tk} 종가", hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(v.tail(200), use_container_width=True)
        st.download_button(f"⬇️ {tk} 전체 CSV",
                           df.to_csv().encode("utf-8-sig"),
                           file_name=f"{tk}.csv", mime="text/csv",
                           key="ms_db_dl")


# ══════════════════════════════════════════════════════════
# 탭6: 개인 설정
# ══════════════════════════════════════════════════════════
def render_settings_tab():
    """만능 스위치 개인 설정 탭 (표준편차매매와 동일 구성)."""
    _IS_CLOUD_val = _IS_CLOUD

    st.subheader("개인 설정")

    _cfg = load_config()
    _usercfg = st.session_state.get("user_settings", {}) if _IS_CLOUD_val else {}
    _mcfg = _load_cfg()

    if _IS_CLOUD_val:
        st.info(f"**{st.session_state.get('username','')}** 으로 로그인 중 -- "
                f"설정을 저장하면 다음 로그인 시 자동으로 불러옵니다.")
    else:
        st.success(f"**로컬 PC 실행 중** -- 공통 설정은 `{_CONFIG}`, "
                   f"전략 파라미터는 `{_CONFIG_PATH}` 에 저장됩니다.")

    # ══ 텔레그램 알림 설정 ═══════════════════════════════════
    with st.container(border=True):
        col_title, col_help = st.columns([3, 1])
        with col_title:
            st.markdown("#### 텔레그램 알림 설정")
            st.caption("포트폴리오 알림 및 주문 신호를 텔레그램으로 받을 수 있습니다.")
        with col_help:
            with st.popover("Chat ID & Bot Token 확인 방법",
                            use_container_width=True):
                render_telegram_help_popover(
                    strategy_name="만능 스위치",
                    example_bot_display="만능 스위치 알림봇",
                    example_bot_username="manse_alert_bot",
                    example_bot_username2="my_soxl_bot",
                    test_button_label="📨 주문표 테스트 발송")

        c1, c2 = st.columns(2)
        ms_tg_chat_id = c1.text_input(
            "텔레그램 Chat ID",
            value=(_usercfg.get("ms_tg_chat_id", "") if _IS_CLOUD_val
                   else _cfg.get("ms_tg_chat_id", "")),
            placeholder="예: 1234567890", key="ms_tg_chat_id_input")
        ms_tg_token = c2.text_input(
            "Bot Token",
            value=(_usercfg.get("ms_tg_token", "") if _IS_CLOUD_val
                   else _cfg.get("ms_tg_token", "")),
            placeholder="예: 123456789:AAF...", type="password",
            key="ms_tg_token_input")
        st.caption("주문표는 매주 월~금 오후 3:00 (KST)에 텔레그램으로 자동 발송됩니다")

        btn1, btn2, _sp = st.columns([1, 1, 4])
        with btn1:
            if st.button("주문표 테스트 발송", use_container_width=True,
                         key="ms_tg_test"):
                if not ms_tg_chat_id or not ms_tg_token:
                    st.warning("Chat ID와 Bot Token을 먼저 입력해주세요.")
                else:
                    _params = st.session_state.get("ms_last_params")
                    if not _params:
                        st.warning("사이드바를 한 번 불러온 뒤 다시 시도해주세요.")
                    else:
                        _p = _params["mp"]
                        with st.spinner(f"{_p.ticker} 주문표 생성 & 발송 중..."):
                            try:
                                _pr = _load_prices(_p.needed_tickers(),
                                                   _params["data_source"])
                                _bt = st.session_state.get("ms_result")
                                _plan = build_order_plan(
                                    _pr, _p,
                                    bt_result=(_bt if _bt and "error" not in _bt
                                               else None),
                                    capital=(None if _bt and "error" not in _bt
                                             else _p.principal))
                                if "error" in _plan:
                                    st.error(_plan["error"])
                                else:
                                    _res = _send_telegram(
                                        ms_tg_token, ms_tg_chat_id,
                                        _order_text(_plan, _p))
                                    if _res.get("ok"):
                                        st.success(f"{_p.ticker} 발송 성공!")
                                    else:
                                        st.error(f"{_p.ticker} 발송 실패: "
                                                 f"{_res.get('description', '알 수 없는 오류')}")
                            except Exception as e:
                                st.error(f"발송 실패: {e}")

        with btn2:
            if st.button("저장하기", use_container_width=True, key="ms_tg_save",
                         type="primary"):
                if not ms_tg_chat_id or not ms_tg_token:
                    st.warning("Chat ID와 Bot Token을 모두 입력해주세요.")
                elif _IS_CLOUD_val:
                    with st.spinner("저장 중..."):
                        try:
                            _save_user_settings_to_sheet(
                                st.session_state.username,
                                {"ms_tg_chat_id": ms_tg_chat_id,
                                 "ms_tg_token": ms_tg_token})
                            st.session_state.user_settings.update(
                                {"ms_tg_chat_id": ms_tg_chat_id,
                                 "ms_tg_token": ms_tg_token})
                            st.success("Google Sheets에 저장 완료!")
                        except Exception as e:
                            st.error(f"저장 실패: {e}")
                else:
                    save_config({"ms_tg_chat_id": ms_tg_chat_id,
                                 "ms_tg_token": ms_tg_token}, sensitive=True)
                    st.success(f"저장 완료! `{_CONFIG}`")

    st.write("")

    # ══ 구글 스프레드시트 연동 ═══════════════════════════════
    with st.container(border=True):
        col_gs1, col_gs2 = st.columns([3, 1])
        with col_gs1:
            st.markdown("#### 구글 스프레드시트 연동")
            st.caption("포트폴리오 정보와 주문 신호를 구글 스프레드시트로 전송합니다.")
        with col_gs2:
            with st.popover("구글 스프레드시트 URL 확인 & 권한 부여",
                            use_container_width=True):
                st.markdown("""
**1. 새 스프레드시트 만들기**
- Google Sheets에서 새 스프레드시트를 만듭니다.

**2. URL 복사**
- 브라우저 주소창의 URL을 복사합니다.

**3. 서비스 계정에 편집 권한 부여**
- 스프레드시트 공유 -> 아래 이메일을 편집자로 추가
- `connectspreadsheet@sodium-gateway-485307-f3.iam.gserviceaccount.com`
                """)

        gs_url = st.text_input(
            "스프레드시트 URL",
            value=(_usercfg.get("gs_url", "") if _IS_CLOUD_val
                   else _cfg.get("gs_url", "")),
            placeholder="https://docs.google.com/spreadsheets/d/...",
            key="gs_url_input_ms")
        st.caption("* 스프레드시트에 서비스 계정 이메일을 편집자로 공유해주세요. "
                   "(우측 상단 도움말 참고)")

        _tung_default = str(
            _usercfg.get("ms_use_tungchigi", "") if _IS_CLOUD_val
            else _cfg.get("ms_use_tungchigi", "")
        ).strip().lower() in ("true", "1", "y", "yes", "on")
        use_tungchigi = st.checkbox(
            "시트 전송 시 퉁치기 주문으로 전송 (자전거래 거부 증권사용)",
            value=_tung_default, key="ms_use_tungchigi_ck",
            help="체크하면 매수/매도 상계(퉁치기) 적용 주문을 시트에 기록합니다. "
                 "순 체결 결과는 원 주문과 동일하며, LOC매도가 < LOC매수가 교차를 "
                 "거부하는 증권사에서 사용하세요.")

        # ── 계좌별 시트 이름 매핑 ──
        # ⚠️ 종목이 아니라 **계좌** 기준이어야 한다.
        #    같은 종목 계좌가 둘이면 탭이 겹쳐 서로 덮어쓴다.
        _accts = _ms_accounts()
        _sheet_map = {}
        if _accts:
            st.markdown("##### 계좌별 시트 이름 매핑")
            st.caption("계좌마다 주문을 기록할 구글시트 탭(시트) 이름입니다. "
                       "**계좌끼리 겹치면 안 됩니다.**")
            for _an, _ai in _accts.items():
                # ⚠️ value= 와 key= 를 함께 쓰면 session_state 가 우선이라
                #    계좌 설정에서 바꾼 값이 여기 반영되지 않을 수 있다.
                #    → value= 를 없애고, config 값이 바뀐 순간에만 위젯 상태를 갱신.
                _k = f"gs_sheet_ms_{_an}"
                _seen = f"_cfgseen_{_k}"
                # ⚠️ Streamlit 은 '이번 실행에서 렌더되지 않은 위젯'의
                #    session_state 키를 정리한다. 반면 _cfgseen_ 은 위젯 키가
                #    아니라 살아남으므로, 마커만 보고 판단하면 위젯이 빈 칸이
                #    된다 → 키 존재 여부도 함께 확인한다.
                if (_k not in st.session_state
                        or st.session_state.get(_seen) != _ai["gs_sheet"]):
                    st.session_state[_seen] = _ai["gs_sheet"]
                    st.session_state[_k] = _ai["gs_sheet"]
                _sheet_map[_an] = st.text_input(
                    f"{_an} ({_ai['ticker']}) 탭 이름", key=_k)
            _vals = [v.strip() for v in _sheet_map.values() if v.strip()]
            _dup = {v for v in _vals if _vals.count(v) > 1}
            if _dup:
                st.error(f"⛔ 탭 이름 중복: **{', '.join(sorted(_dup))}** — "
                         f"해당 계좌들의 주문이 서로 덮어써집니다. 다르게 지정하세요.")
        else:
            st.info("등록된 계좌가 없습니다. "
                    "**오늘의 주문표** 탭에서 계좌를 먼저 추가해주세요.")

        st.write("")
        bg1, bg2, bg3 = st.columns(3)
        with bg1:
            if st.button("시트 연결 테스트", use_container_width=True,
                         key="gs_test_ms"):
                if not gs_url:
                    st.warning("스프레드시트 URL을 먼저 입력해주세요.")
                else:
                    try:
                        gc = _get_gspread_client()
                        sh = gc.open_by_url(gs_url)
                        st.success(f"연결 성공! 스프레드시트: **{sh.title}**")
                    except Exception as e:
                        st.error(f"연결 실패: {e}")

        with bg2:
            if st.button("주문 시트 전송", use_container_width=True,
                         key="gs_send_ms", type="primary"):
                # ⚠️ 계좌마다 파라미터·시작일·자본·탭이 다르므로 계좌 단위로 보낸다.
                #    (사이드바 값 하나로 보내면 어느 계좌 주문인지 알 수 없다)
                _accs_send = (_mcfg.get("accounts") or {})
                if not gs_url:
                    st.warning("스프레드시트 URL을 먼저 입력해주세요.")
                elif not _accs_send:
                    st.warning("등록된 계좌가 없습니다. "
                               "**오늘의 주문표** 탭에서 계좌를 먼저 추가해주세요.")
                else:
                    _ok_cnt = 0
                    for _an, _acct in _accs_send.items():
                        _ap = _acct_params(_acct)
                        _nm2 = (_sheet_map.get(_an) or _acct.get("gs_sheet")
                                or _default_sheet_name(_an, _ap.ticker)).strip()
                        with st.spinner(f"{_an} -> '{_nm2}' 전송 중..."):
                            try:
                                _pr = _load_prices(
                                    _ap.needed_tickers(),
                                    _acct.get("data_source", DATA_SOURCES[0]))
                                _adj, _ = recalc_adj_history(
                                    _acct.get("capital_adj_history", []) or [],
                                    float(_acct.get("os_capital", 10000.0)))
                                _cf = {}
                                for _h in _adj:
                                    try:
                                        _cf[pd.Timestamp(_h["날짜"]).normalize()] = {
                                            "deposit": float(_h.get("조정금액", 0))}
                                    except Exception:
                                        pass
                                _res2 = run_backtest(
                                    _pr, _ap, start=_acct.get("os_start"),
                                    end=str(datetime.today().date()),
                                    cash_flows=_cf)
                                if "error" in _res2:
                                    st.error(f"{_an}: {_res2['error']}")
                                    continue
                                _plan = build_order_plan(_pr, _ap,
                                                         bt_result=_res2)
                                if "error" in _plan:
                                    st.error(f"{_an}: {_plan['error']}")
                                    continue
                                rows = _order_rows(_plan)
                                if use_tungchigi and rows:
                                    try:
                                        from dss_engine import rows_to_tungchigi_rows as _rttr
                                    except ImportError:
                                        import importlib
                                        import dss_engine as _de
                                        _rttr = importlib.reload(_de).rows_to_tungchigi_rows
                                    rows = _rttr(rows)
                                gc = _get_gspread_client()
                                sh = gc.open_by_url(gs_url)
                                try:
                                    ws = sh.worksheet(_nm2)
                                except Exception:
                                    # gspread WorksheetNotFound 는 메시지가 탭 이름뿐이라
                                    # 그대로 보여주면 원인을 알 수 없다
                                    st.error(
                                        f"⛔ {_an}: 구글시트에 **'{_nm2}'** 탭이 "
                                        f"없습니다. 시트에서 해당 이름의 탭을 만들어 "
                                        f"주세요 (기존 주문 탭을 복사해 이름만 변경하면 "
                                        f"양식이 유지됩니다).")
                                    continue
                                ws.batch_clear(["L4:O13"])
                                if rows:
                                    ws.update(range_name="L4", values=rows)
                                ws.update(range_name="B11", values=[[
                                    pd.Timestamp.now(tz="Asia/Seoul")
                                    .strftime("%Y-%m-%d %H:%M:%S")]])
                                st.success(f"✅ {_an} -> '{_nm2}' L4에 "
                                           f"{len(rows)}건 전송 완료")
                                _ok_cnt += 1
                            except Exception as e:
                                st.error(f"{_an} 전송 실패: "
                                         f"{type(e).__name__}: {e}")
                    if _ok_cnt:
                        st.caption(f"총 {_ok_cnt}/{len(_accs_send)} 계좌 전송 완료")

        with bg3:
            if st.button("저장하기 ", use_container_width=True, key="gs_save_ms",
                         type="primary"):
                if not gs_url:
                    st.warning("스프레드시트 URL을 입력해주세요.")
                else:
                    if _IS_CLOUD_val:
                        try:
                            _save_user_settings_to_sheet(
                                st.session_state.username,
                                {"gs_url": gs_url,
                                 "ms_use_tungchigi": use_tungchigi})
                            st.session_state.user_settings.update(
                                {"gs_url": gs_url,
                                 "ms_use_tungchigi": str(use_tungchigi)})
                        except Exception as e:
                            st.error(f"저장 실패: {e}")
                    else:
                        save_config({"gs_url": gs_url}, sensitive=True)
                        save_config({"ms_use_tungchigi": str(use_tungchigi)})
                    # 주문표가 읽는 위치(accounts)에 저장해야 실제로 반영된다
                    _accs = _mcfg.setdefault("accounts", {})
                    for _an, _nm in _sheet_map.items():
                        if _an in _accs and isinstance(_accs[_an], dict):
                            _accs[_an]["gs_sheet"] = _nm.strip()
                    _save_cfg(_mcfg)
                    st.success("URL 및 계좌별 시트 이름 저장 완료!")

    st.write("")

    # ══ 전략 파라미터 저장 / 백업 ════════════════════════════
    with st.container(border=True):
        st.markdown("#### 🧭 사이드바 설정 (백테스트용)")
        st.info("이 설정은 **사이드바 기본값**입니다 — 백테스트 · 최적화 · 성과 분석에만 "
                "쓰입니다.\n\n"
                "실제 주문표와 자동 발송은 **오늘의 주문표 탭의 계좌**가 자기 "
                "파라미터를 따로 들고 있으므로, 여기 저장해도 주문에는 영향이 없습니다.")
        _params = st.session_state.get("ms_last_params")
        if _params:
            _p = _params["mp"]
            _c1, _c2 = st.columns([1, 2])
            with _c1:
                if st.button(f"💾 {_p.ticker} 사이드바 설정 저장",
                             use_container_width=True,
                             key="ms_param_save", type="primary"):
                    _mcfg.setdefault(_p.ticker, {}).update({
                        "params": params_to_dict(_p),
                        "data_source": _params["data_source"],
                        "bt_start": str(_params["bt_start_date"]),
                    })
                    _save_cfg(_mcfg)
                    st.success(f"{_p.ticker} 사이드바 설정을 저장했습니다. "
                               f"다음 접속 시 자동으로 불러옵니다.")
            _c2.caption(
                f"현재 사이드바: {_p.mode_basis} 기준 · "
                f"복리 {_p.profit_comp:.0%}/{_p.loss_comp:.0%} · "
                f"기간 {_params['bt_start_date']} ~ {_params['bt_end_date']} · "
                f"원금 ${_p.principal:,.0f}")
            _saved_tks = [k for k, v in _mcfg.items()
                          if not str(k).startswith("_") and k != "accounts"
                          and isinstance(v, dict) and "params" in v]
            if _saved_tks:
                st.caption("저장된 종목: " + ", ".join(sorted(_saved_tks)))
        else:
            st.info("사이드바를 한 번 불러온 뒤 저장할 수 있습니다.")

    st.write("")

    # ══ 백업 / 복원 ══════════════════════════════════════════
    with st.container(border=True):
        st.markdown("#### 💾 전체 설정 백업 / 복원")
        st.caption("**계좌 정보(파라미터 · 시작일 · 자본 · 자본조정 이력 · 시트 탭 이름)와 "
                   "사이드바 설정이 모두 포함**됩니다. "
                   "클라우드는 파일시스템이 임시라, 내려받은 파일이 확실한 사본입니다.")
        _n_acc = len(_mcfg.get("accounts") or {})
        _b1, _b2 = st.columns(2)
        with _b1:
            st.download_button(
                f"⬇️ 설정 JSON 내보내기 (계좌 {_n_acc}개)",
                json.dumps(_mcfg, ensure_ascii=False, indent=2).encode("utf-8"),
                file_name=f"manse_config_{datetime.today().strftime('%Y%m%d')}.json",
                mime="application/json", use_container_width=True,
                key="ms_cfg_dl", type="primary")
        _b2.caption("정기적으로 받아두시면 사고 시 그대로 되살릴 수 있습니다.")

        up = st.file_uploader("설정 JSON 복원", type=["json"], key="ms_cfg_up")
        if up is not None:
            try:
                _incoming = json.loads(up.getvalue().decode("utf-8"))
                _in_acc = list((_incoming.get("accounts") or {}).keys())
                st.warning(f"⚠️ 복원하면 **현재 설정 전체를 덮어씁니다.** "
                           f"(현재 계좌 {_n_acc}개 → 파일의 계좌 {len(_in_acc)}개"
                           + (f": {', '.join(_in_acc)}" if _in_acc else "") + ")")
                if st.button("🔁 덮어쓰고 복원", key="ms_cfg_restore"):
                    _save_cfg(_incoming)
                    st.success("복원 완료 — 새로고침하세요.")
            except Exception as e:
                st.error(f"파일을 읽을 수 없습니다: {e}")

    st.write("")

    # ══ 데이터 캐시 ══════════════════════════════════════════
    with st.container(border=True):
        st.markdown("#### 🧹 데이터 캐시")
        st.caption("가격 데이터는 30분간 캐시됩니다. 최신 종가를 강제로 다시 받거나, "
                   "백테스트·최적화 결과를 초기화할 때 사용하세요.")
        _k1, _k2 = st.columns([1, 2])
        with _k1:
            if st.button("🧹 가격 캐시 비우기", use_container_width=True,
                         key="ms_clear"):
                _load_one.clear()
                for k in ("ms_result", "ms_plan", "ms_opt_res",
                          "ms_opt_samples", "ms_opt_prices", "ms_perf_res",
                          "ms_rs_res"):
                    st.session_state.pop(k, None)
                st.success("캐시를 비웠습니다. 다시 실행하면 새 데이터를 받습니다.")
        _k2.caption("설정·계좌는 지워지지 않습니다. 캐시된 가격과 실행 결과만 비웁니다.")

    st.write("")

    # ══ 등록된 계좌 요약 ═════════════════════════════════════
    with st.container(border=True):
        st.markdown("#### 📊 등록된 계좌")
        st.caption("계좌 추가·수정·삭제는 **오늘의 주문표** 탭에서 합니다.")
        _accs_full = _mcfg.get("accounts") or {}
        if _accs_full:
            _rows = []
            for _n, _a in _accs_full.items():
                _ap = _acct_params(_a)
                _rows.append({
                    "계좌": _n, "종목": _ap.ticker, "모드": _ap.mode_basis,
                    "프리셋": _match_preset(_ap) or "커스텀",
                    "시작일": str(_a.get("os_start", "")),
                    "시작자본": float(_a.get("os_capital", 0) or 0),
                    "시트 탭": (_a.get("gs_sheet")
                              or _default_sheet_name(_n, _ap.ticker)),
                })
            _adf = pd.DataFrame(_rows)
            st.dataframe(_adf.style.format({"시작자본": "${:,.0f}"}),
                         use_container_width=True, hide_index=True)
            _tabs = [r["시트 탭"] for _rr, r in enumerate(_rows)]
            _dupt = {t for t in _tabs if _tabs.count(t) > 1}
            if _dupt:
                st.error(f"⛔ 시트 탭 중복: **{', '.join(sorted(_dupt))}** — "
                         f"해당 계좌의 주문이 서로 덮어써집니다.")
        else:
            st.info("등록된 계좌가 없습니다. "
                    "**오늘의 주문표** 탭에서 계좌를 추가해주세요.")

    st.write("")

    # ══ 관리자 도구 ══════════════════════════════════════════
    with st.expander("관리자 도구 -- 비밀번호 해시 생성 (users 시트 등록용)"):
        st.caption("새 사용자를 추가할 때 비밀번호를 bcrypt 해시로 변환하여 "
                   "Google Sheets에 붙여넣으세요.")
        _admin_pw = st.text_input("등록할 비밀번호 입력", type="password",
                                  key="admin_pw_input_ms")
        if st.button("해시 생성", key="gen_hash_ms"):
            if _admin_pw:
                st.code(_hash_password(_admin_pw), language=None)
                st.caption("위 해시를 복사해서 users 시트의 password_hash 컬럼에 "
                           "붙여넣으세요.")
            else:
                st.warning("비밀번호를 입력해주세요.")
