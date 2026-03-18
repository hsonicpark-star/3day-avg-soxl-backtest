import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import math
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import json
from pathlib import Path
import requests
import os
from streamlit_cookies_controller import CookieController

_cookie_mgr = CookieController()

# ── 실행 환경 감지 ──────────────────────────────────────────
# Streamlit Cloud는 HOME=/home/appuser 또는 환경변수로 식별
_IS_CLOUD = (
    os.environ.get("STREAMLIT_SHARING_MODE") == "1"
    or str(Path.home()) == "/home/appuser"
    or os.environ.get("IS_STREAMLIT_CLOUD", "") == "1"
)

# ── config 경로 ──────────────────────────────────────────────
# 로컬: C:\Users\{이름}\.usd-avg\config.json  (각자 PC에 독립 저장)
# 클라우드: 앱 디렉토리 (비민감 정보만, 공유 서버)
_OLD_CONFIG = Path(__file__).parent / "config.json"   # 이전 경로 (마이그레이션용)
if _IS_CLOUD:
    _CONFIG = _OLD_CONFIG
else:
    _CONFIG = Path.home() / ".usd-avg" / "config.json"
    _CONFIG.parent.mkdir(parents=True, exist_ok=True)
    # 이전 경로(앱 폴더)에 config가 있고 새 경로에 아직 없으면 자동 마이그레이션
    if _OLD_CONFIG.exists() and not _CONFIG.exists():
        try:
            import shutil
            shutil.copy2(_OLD_CONFIG, _CONFIG)
        except:
            pass

_SENSITIVE_KEYS    = {"tg_chat_id", "tg_token", "gs_url", "gs_sheet"}
_GLOBAL_CONFIG_KEYS = _SENSITIVE_KEYS  # ticker 네임스페이스가 아닌 루트 키들

def load_config(ticker: str = None):
    """ticker 지정 시 해당 ticker 네임스페이스 반환, 없으면 전체 반환."""
    if _CONFIG.exists():
        try:
            cfg = json.loads(_CONFIG.read_text(encoding="utf-8"))
            if ticker:
                return cfg.get(ticker, {})
            return cfg
        except:
            return {}
    return {}

def _load_full_config() -> dict:
    """항상 전체 config를 반환 (내부용)."""
    if _CONFIG.exists():
        try:
            return json.loads(_CONFIG.read_text(encoding="utf-8"))
        except:
            return {}
    return {}

def save_config(data: dict, ticker: str = None, sensitive: bool = False):
    """sensitive=True 이면 민감 정보 포함. 클라우드에서는 민감 정보 저장 안 함.
    ticker 지정 시 해당 ticker 네임스페이스에 저장, 없으면 루트에 저장."""
    try:
        full_cfg = _load_full_config()
        target = full_cfg.setdefault(ticker, {}) if ticker else full_cfg
        for k, v in data.items():
            if k in _SENSITIVE_KEYS and _IS_CLOUD:
                continue  # 클라우드에서 민감 정보 저장 차단
            target[k] = v
        _CONFIG.write_text(json.dumps(full_cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    except:
        pass

# ── 주문 히스토리 경로 ──────────────────────────────────────
if _IS_CLOUD:
    _HISTORY_FILE = Path(__file__).parent / "order_history.csv"
else:
    _HISTORY_FILE = Path.home() / ".usd-avg" / "order_history.csv"

def load_order_history() -> "pd.DataFrame":
    if _HISTORY_FILE.exists():
        try:
            return pd.read_csv(_HISTORY_FILE, encoding="utf-8-sig")
        except:
            return pd.DataFrame()
    return pd.DataFrame()

def append_order_history(rows: list):
    """오늘 주문 내역을 히스토리 CSV에 누적 저장."""
    import io as _io
    df_new = pd.DataFrame(rows)
    if _HISTORY_FILE.exists():
        df_old = load_order_history()
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_combined = df_new
    _HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_combined.to_csv(_HISTORY_FILE, index=False, encoding="utf-8-sig")

# 클라우드 서버에 혹시 남은 민감 정보 제거
if _IS_CLOUD:
    try:
        cfg = _load_full_config()
        if any(k in cfg for k in _SENSITIVE_KEYS):
            for k in _SENSITIVE_KEYS:
                cfg.pop(k, None)
            _CONFIG.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    except:
        pass

# ── ticker별 설정 관리 (멀티 계좌) ────────────────────────────
def _get_ticker_settings() -> dict:
    """등록된 모든 ticker 설정 반환 {ticker: {a_buy, a_sell, os_start, ...}}"""
    if _IS_CLOUD and st.session_state.get("logged_in"):
        raw = st.session_state.get("user_settings", {}).get("ticker_settings", "{}")
        try:
            ts = json.loads(raw) if isinstance(raw, str) else raw
            return ts if isinstance(ts, dict) else {}
        except:
            return {}
    else:
        full_cfg = _load_full_config()
        return {k: v for k, v in full_cfg.items()
                if k not in _GLOBAL_CONFIG_KEYS and isinstance(v, dict)}

def _save_ticker_setting(tk: str, data: dict):
    """ticker별 설정 저장 (로컬 config.json + 클라우드 Google Sheets 동기)."""
    save_config(data, tk)  # 로컬
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            raw = st.session_state.get("user_settings", {}).get("ticker_settings", "{}")
            ts  = json.loads(raw) if isinstance(raw, str) else {}
            if not isinstance(ts, dict):
                ts = {}
            ts[tk] = {**ts.get(tk, {}), **data}
            ts_json = json.dumps(ts, ensure_ascii=False)
            _save_user_settings_to_sheet(st.session_state.username, {"ticker_settings": ts_json})
            st.session_state.user_settings["ticker_settings"] = ts_json
        except Exception:
            pass

def _delete_ticker_setting(tk: str):
    """ticker 설정 삭제 (로컬 + 클라우드)."""
    full_cfg = _load_full_config()
    full_cfg.pop(tk, None)
    try:
        _CONFIG.write_text(json.dumps(full_cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    except:
        pass
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            raw = st.session_state.get("user_settings", {}).get("ticker_settings", "{}")
            ts  = json.loads(raw) if isinstance(raw, str) else {}
            ts.pop(tk, None)
            ts_json = json.dumps(ts, ensure_ascii=False)
            _save_user_settings_to_sheet(st.session_state.username, {"ticker_settings": ts_json})
            st.session_state.user_settings["ticker_settings"] = ts_json
        except Exception:
            pass

# ── gspread 인증 (로그인보다 먼저 정의되어야 함) ──────────────
_GS_SCOPES = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]

def _get_gspread_client():
    """Streamlit Cloud(st.secrets) 또는 로컬(service_account.json)로 gspread 인증."""
    import gspread
    from google.oauth2.service_account import Credentials
    try:
        if "gcp_service_account" in st.secrets:
            creds = Credentials.from_service_account_info(
                dict(st.secrets["gcp_service_account"]), scopes=_GS_SCOPES)
        else:
            _key_path = Path(__file__).parent / "service_account.json"
            creds = Credentials.from_service_account_file(str(_key_path), scopes=_GS_SCOPES)
        return gspread.authorize(creds)
    except Exception as e:
        raise RuntimeError(f"인증 실패: {e}")

# ── 인증 함수 ──────────────────────────────────────────────────
def _get_users_ws():
    """서비스 계정으로 users 시트 접근."""
    gc = _get_gspread_client()
    # st.secrets 키 직접 접근 (더 안전)
    try:
        url = st.secrets["admin_sheet_url"]
    except (KeyError, Exception):
        url = ""
    if not url:
        available = list(st.secrets.keys()) if hasattr(st.secrets, "keys") else "확인불가"
        raise RuntimeError(f"admin_sheet_url 없음. 현재 Secrets 키: {available}")
    return gc.open_by_url(url).worksheet("users")

def _authenticate(username: str, password: str):
    """인증 성공 시 사용자 정보 dict 반환, 실패 시 None."""
    import bcrypt
    ws = _get_users_ws()
    for row in ws.get_all_records():
        if row.get("username") == username:
            stored = row.get("password_hash", "")
            if stored and bcrypt.checkpw(password.encode(), stored.encode()):
                return dict(row)
    return None

def _save_user_settings_to_sheet(username: str, settings: dict):
    """users 시트에서 해당 유저 행의 설정 컬럼 업데이트. 없는 컬럼은 자동 추가."""
    ws = _get_users_ws()
    headers = ws.row_values(1)
    # 없는 컬럼 자동 추가
    for key in settings:
        if key not in ("username", "password_hash") and key not in headers:
            ws.update_cell(1, len(headers) + 1, key)
            headers.append(key)
    for i, row in enumerate(ws.get_all_records(), start=2):
        if row.get("username") == username:
            for key, val in settings.items():
                if key in headers and key not in ("username", "password_hash"):
                    ws.update_cell(i, headers.index(key) + 1, str(val))
            return

def _hash_password(plain: str) -> str:
    """bcrypt 해시 생성 (관리자 도구용)."""
    import bcrypt
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()

# ── 앱 초기화 ──────────────────────────────────────────────────
st.set_page_config(page_title="USD 종가평균매매 백테스트", layout="wide")

# ── 클라우드: 로그인 게이트 ────────────────────────────────────
if _IS_CLOUD:
    # 쿠키에서 자동 로그인 시도 (새로고침해도 로그인 유지)
    if not st.session_state.get("logged_in", False):
        try:
            _cookie_user = _cookie_mgr.get("usd_avg_user")
        except Exception:
            _cookie_user = None
        if _cookie_user:
            try:
                _ws   = _get_users_ws()
                _rows = _ws.get_all_records()
                _row  = next(
                    (r for r in _rows if str(r.get("username", "")).strip() == str(_cookie_user).strip()),
                    None
                )
                if _row:
                    st.session_state.logged_in    = True
                    st.session_state.username     = str(_cookie_user).strip()
                    st.session_state.user_settings = {
                        k: _row.get(k, "") for k in (
                            "tg_chat_id", "tg_token", "gs_url", "gs_sheet",
                            "a_buy", "a_sell", "sell_ratio", "divisions"
                        )
                    }
                    st.rerun()
            except Exception:
                pass  # 쿠키 자동 로그인 실패 → 수동 로그인 폼 표시

    if not st.session_state.get("logged_in", False):
        st.title("📈 종가평균매매 백테스트")
        st.markdown("---")
        with st.container():
            _, center, _ = st.columns([1, 1.2, 1])
            with center:
                st.subheader("🔐 로그인")
                with st.form("login_form"):
                    _u = st.text_input("아이디")
                    _p = st.text_input("비밀번호", type="password")
                    _ok = st.form_submit_button("로그인", type="primary", use_container_width=True)
                if _ok:
                    if not _u or not _p:
                        st.warning("아이디와 비밀번호를 입력해주세요.")
                    else:
                        with st.spinner("인증 중..."):
                            try:
                                _user = _authenticate(_u, _p)
                            except Exception as e:
                                _user = None
                                st.error(f"인증 서버 오류: {e}")
                        if _user:
                            st.session_state.logged_in    = True
                            st.session_state.username     = _u
                            st.session_state.user_settings = _user
                            # 30일 자동 로그인 쿠키 저장
                            try:
                                _cookie_mgr.set(
                                    "usd_avg_user", _u,
                                    expires=datetime.now() + timedelta(days=30),
                                )
                            except Exception:
                                pass
                            st.rerun()
                        else:
                            st.error("아이디 또는 비밀번호가 올바르지 않습니다.")

        # ── 관리자 해시 생성 도구 (로그인 없이 접근 가능) ──
        st.markdown("---")
        with st.expander("🔧 관리자 도구 — 비밀번호 해시 생성"):
            st.caption("users 시트에 등록할 bcrypt 해시를 생성합니다.")
            _admin_pw = st.text_input("등록할 비밀번호", type="password", key="login_admin_pw")
            if st.button("🔑 해시 생성", key="login_gen_hash"):
                if _admin_pw:
                    st.code(_hash_password(_admin_pw), language=None)
                    st.caption("👆 복사 후 users 시트 password_hash 컬럼에 붙여넣기")
                else:
                    st.warning("비밀번호를 입력해주세요.")
        st.stop()

st.title("📈 종가평균매매 백테스트 (LOC)")

# ──────────────────────────────────────────────
# 사이드바 공통 설정
# ──────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ 공통 설정")

    _PRESET_TICKERS = ["SOXL", "USD", "TQQQ", "직접입력"]
    _ticker_select = st.selectbox("종목코드 (Ticker)", _PRESET_TICKERS, index=0)
    if _ticker_select == "직접입력":
        ticker = st.text_input("티커 직접 입력", placeholder="예: NVDA, SPY, QQQ, TSLA").strip().upper()
        if not ticker:
            st.warning("티커를 입력해주세요.")
    else:
        ticker = _ticker_select

    st.markdown("---")
    st.subheader("전략 파라미터")

    # ── 저장된 파라미터 기본값 로드 ──
    def _sfloat(v, d):
        try: return float(v) if v not in ("", None) else d
        except: return d
    def _sint(v, d):
        try: return int(float(v)) if v not in ("", None) else d
        except: return d

    _cfg_sb = load_config(ticker)
    if _IS_CLOUD and st.session_state.get("logged_in"):
        _usercfg_sb = st.session_state.get("user_settings", {})
    else:
        _usercfg_sb = _cfg_sb

    _def_a_buy  = _sfloat(_usercfg_sb.get("a_buy"),      -0.005)
    _def_a_sell = _sfloat(_usercfg_sb.get("a_sell"),       0.009)
    _def_sr     = _sfloat(_usercfg_sb.get("sell_ratio"),  100.0)
    _def_div    = _sint  (_usercfg_sb.get("divisions"),   5)

    a_buy      = st.number_input("매수기준 (a값)", value=_def_a_buy,  step=0.001, format="%.4f")
    a_sell     = st.number_input("매도기준 (a값)", value=_def_a_sell, step=0.001, format="%.4f")
    sell_ratio = st.number_input("매도비율 (%)", value=_def_sr, step=10.0, min_value=0.0, max_value=100.0)
    divisions  = st.number_input("분할수", value=_def_div, min_value=1, step=1)

    # ── 파라미터 저장 버튼 ──
    if st.button("💾 파라미터 저장", use_container_width=True):
        _param_data = {
            "a_buy": float(a_buy), "a_sell": float(a_sell),
            "sell_ratio": float(sell_ratio), "divisions": int(divisions)
        }
        save_config(_param_data, ticker)
        if _IS_CLOUD and st.session_state.get("logged_in"):
            try:
                _save_user_settings_to_sheet(st.session_state.username, _param_data)
                st.session_state.user_settings.update(_param_data)
            except Exception as _e:
                st.warning(f"시트 저장 실패: {_e}")
        st.success("✅ 파라미터가 저장되었습니다!")

    st.markdown("---")
    st.subheader("백테스트 설정")
    col1, col2 = st.columns(2)
    start_date = col1.date_input("시작 일", datetime(2014, 1, 1).date())
    end_date   = col2.date_input("종료 일", datetime.today().date())
    initial_capital = st.number_input("초기 투자금 ($)", value=10000.0, step=1000.0)
    st.info(f"1회 분할 금액: ${initial_capital / divisions:,.2f}")

    st.markdown("---")
    data_source = st.radio(
        "📂 종가 데이터 소스",
        ["야후 파이낸스 (yfinance)", "엑셀 Daily_Close 시트"],
        index=0,
    )
    excel_file = None
    if data_source == "엑셀 Daily_Close 시트":
        excel_file = st.file_uploader("엑셀 파일 업로드 (.xlsx)", type=["xlsx"])
        st.caption("엑셀 내 **Daily_Close** 시트의 날짜/종가 두 컬럼이 사용됩니다.")

    # ── 사용자 정보 (클라우드 로그인 시) ──────────────
    if _IS_CLOUD and st.session_state.get("logged_in"):
        st.markdown("---")
        st.caption(f"👤 **{st.session_state.username}** 으로 로그인 중")
        if st.button("🚪 로그아웃", use_container_width=True):
            try:
                _cookie_mgr.remove("usd_avg_user")
            except Exception:
                pass
            for k in ("logged_in", "username", "user_settings"):
                st.session_state.pop(k, None)
            st.rerun()


# ──────────────────────────────────────────────
# 핵심 함수
# ──────────────────────────────────────────────
def buy_limit_price(p1, p2, a):
    return (p1 + p2) * (1 + a) / (2 - a)


def scalar(v):
    if isinstance(v, (pd.Series, np.ndarray)):
        return float(v.iloc[0] if isinstance(v, pd.Series) else v.flat[0])
    return float(v)


@st.cache_data(show_spinner=False)
def _download_price(ticker: str, start_str: str, end_str: str) -> pd.DataFrame:
    start = pd.to_datetime(start_str).date()
    end   = pd.to_datetime(end_str).date()

    def _to_close_df(raw):
        if raw is None or raw.empty:
            return pd.DataFrame()
        if isinstance(raw.columns, pd.MultiIndex):
            try:    raw = raw.xs(ticker, axis=1, level="Ticker")
            except: raw.columns = raw.columns.droplevel(1)
        if "Close" not in raw.columns:
            return pd.DataFrame()
        df = raw[["Close"]].copy()
        df.index = pd.to_datetime(df.index)
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        return df.dropna()

    # 방법 1: yf.download
    try:
        raw = yf.download(
            ticker,
            start=start - timedelta(days=15),
            end=end + timedelta(days=2),
            progress=False, auto_adjust=True,
        )
        df = _to_close_df(raw)
        if not df.empty:
            return df
    except Exception:
        pass

    # 방법 2: yf.Ticker.history (fallback)
    try:
        t = yf.Ticker(ticker)
        raw2 = t.history(
            start=start - timedelta(days=15),
            end=end + timedelta(days=2),
            auto_adjust=True,
        )
        if not raw2.empty and "Close" in raw2.columns:
            df2 = raw2[["Close"]].copy()
            df2.index = pd.to_datetime(df2.index).tz_localize(None)
            df2["Close"] = pd.to_numeric(df2["Close"], errors="coerce")
            return df2.dropna()
    except Exception:
        pass

    return pd.DataFrame()


def load_price_data(ticker, start, end, data_source, excel_file):
    if data_source == "엑셀 Daily_Close 시트" and excel_file is not None:
        xl = pd.ExcelFile(excel_file)
        df = xl.parse("Daily_Close")
        df.columns = ["Date", "Close"]
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date").sort_index()
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        return df.dropna()
    return _download_price(ticker, str(start), str(end))


def run_backtest(
    price_df, start_date, end_date,
    a_buy, a_sell, sell_ratio, divisions, initial_capital,
    return_history=False,
):
    sim_raw = price_df.loc[pd.to_datetime(start_date):pd.to_datetime(end_date)].copy()
    sim_raw["p1"] = sim_raw["Close"].shift(1)
    sim_raw["p2"] = sim_raw["Close"].shift(2)
    sim = sim_raw.dropna(subset=["p1", "p2"])
    if sim.empty:
        return None

    closes   = sim["Close"].values.astype(float)
    p1s      = sim["p1"].values.astype(float)
    p2s      = sim["p2"].values.astype(float)
    tgt_buy  = (p1s + p2s) * (1 + a_buy)  / (2 - a_buy)
    tgt_sell = (p1s + p2s) * (1 + a_sell) / (2 - a_sell)

    cash       = float(initial_capital)
    shares     = 0
    avg_cost   = 0.0
    prev_asset = float(initial_capital)
    assets     = np.empty(len(closes))
    buy_count  = sell_count = 0
    sell_pnls  = []          # 매도별 손익률(%) 기록
    history    = [] if return_history else None

    for i in range(len(closes)):
        x  = closes[i]
        tb = tgt_buy[i]
        ts = tgt_sell[i]
        current_chunk = prev_asset / divisions
        action = "-"; trade_shares = 0; trade_amount = 0.0

        if shares > 0 and x >= ts:
            sell_qty = math.floor(shares * (sell_ratio / 100.0))
            if sell_qty > 0:
                pnl_pct = (x / avg_cost - 1) * 100 if avg_cost > 0 else 0.0
                sell_pnls.append(pnl_pct)
                action = "SELL"; trade_shares = -sell_qty; trade_amount = sell_qty * x
                cash += trade_amount; shares -= sell_qty; sell_count += 1
                if shares == 0:
                    avg_cost = 0.0
        elif x <= tb:
            buy_qty = min(math.floor(current_chunk / x + 1e-9), math.floor(cash / x + 1e-9))
            if buy_qty > 0:
                avg_cost  = (avg_cost * shares + x * buy_qty) / (shares + buy_qty)
                action = "BUY"; trade_shares = buy_qty; trade_amount = buy_qty * x
                cash -= trade_amount; shares += buy_qty; buy_count += 1

        asset = cash + shares * x
        prev_asset = asset
        assets[i]  = asset

        if return_history:
            history.append({
                "날짜": sim.index[i].date(), "종가(x)": x,
                "전날(p1)": p1s[i], "전전날(p2)": p2s[i],
                "매수경계가": tb, "매도경계가": ts,
                "매매": action, "거래주수": trade_shares,
                "거래금액($)": trade_amount, "보유주수": shares,
                "현금($)": cash, "총자산($)": asset,
            })

    final_asset  = float(assets[-1])
    peak         = np.maximum.accumulate(assets)
    mdd          = float(((assets - peak) / peak).min())
    years        = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days / 365.25
    total_return = (final_asset / initial_capital) - 1.0
    cagr         = ((final_asset / initial_capital) ** (1.0 / years) - 1.0) if years > 0 else 0.0
    calmar       = cagr / abs(mdd) if mdd != 0 else 0.0

    _win_cnt  = sum(1 for p in sell_pnls if p > 0)
    _avg_pnl  = sum(sell_pnls) / len(sell_pnls) if sell_pnls else 0.0
    _max_pnl  = max(sell_pnls) if sell_pnls else 0.0
    _min_pnl  = min(sell_pnls) if sell_pnls else 0.0

    out = dict(
        final_asset=final_asset, total_return=total_return,
        cagr=cagr, mdd=mdd, calmar=calmar,
        buy_count=buy_count, sell_count=sell_count,
        win_count=_win_cnt, avg_pnl=_avg_pnl,
        max_pnl=_max_pnl, min_pnl=_min_pnl,
        assets=assets, dates=sim.index,
    )
    if return_history:
        out["history"] = pd.DataFrame(history)
    return out


def run_portfolio_for_ordersheet(
    price_df, start_date, ticker_name,
    a_buy, a_sell, sell_ratio, divisions, initial_capital,
):
    """백테스트를 오늘까지 실행하며 평균단가·티어·매도이력을 추적."""
    today = datetime.today().date()
    sim_raw = price_df.loc[pd.to_datetime(start_date):pd.to_datetime(today)].copy()
    sim_raw["p1"] = sim_raw["Close"].shift(1)
    sim_raw["p2"] = sim_raw["Close"].shift(2)
    sim = sim_raw.dropna(subset=["p1", "p2"])

    # p1/p2 계산에는 최소 2개 종가 필요
    all_closes = sim_raw["Close"].dropna().values.astype(float)
    if len(all_closes) < 2:
        return None

    cash        = float(initial_capital)
    shares      = 0
    prev_asset  = float(initial_capital)
    peak_asset  = float(initial_capital)
    avg_cost    = 0.0
    open_tiers  = []   # [{'date': Timestamp, 'price': float, 'qty': int}]
    sell_trades = []
    buy_trades  = []
    daily_log   = []

    if not sim.empty:
        closes   = sim["Close"].values.astype(float)
        p1s      = sim["p1"].values.astype(float)
        p2s      = sim["p2"].values.astype(float)
        tgt_buy  = (p1s + p2s) * (1 + a_buy)  / (2 - a_buy)
        tgt_sell = (p1s + p2s) * (1 + a_sell) / (2 - a_sell)
    else:
        closes = np.array([])  # 거래 없음, 빈 배열

    for i in range(len(closes)):
        x    = closes[i]
        tb   = tgt_buy[i]
        ts   = tgt_sell[i]
        date = sim.index[i]
        current_chunk = prev_asset / divisions
        _day_action = "-"
        _day_qty    = 0
        _day_amt    = 0.0

        if shares > 0 and x >= ts:
            sell_qty = math.floor(shares * (sell_ratio / 100.0))
            if sell_qty > 0:
                oldest_date  = open_tiers[0]["date"] if open_tiers else date
                holding_days = (date - oldest_date).days
                factor       = x / avg_cost if avg_cost > 0 else 0.0

                _date_val = date.date() if hasattr(date, "date") else date
                sell_trades.append({
                    "날짜":    str(_date_val),
                    "구분":    "매도",
                    "티커":    ticker_name,
                    "체결가":  x,
                    "avg_cost": avg_cost,
                    "수량":    sell_qty,
                    "금액($)": round(sell_qty * x, 2),
                    "보유기간(일)": holding_days,
                    "비고":    f"평단 ${avg_cost:.2f} → 수익률 {(x/avg_cost-1)*100:+.2f}%",
                })

                cash   += sell_qty * x
                shares -= sell_qty
                _day_action = "SELL"
                _day_qty    = -sell_qty   # 음수로 표시 (백테스트와 동일)
                _day_amt    = round(sell_qty * x, 2)

                # FIFO 티어 차감
                remaining = sell_qty
                while remaining > 0 and open_tiers:
                    if open_tiers[0]["qty"] <= remaining:
                        remaining -= open_tiers[0]["qty"]
                        open_tiers.pop(0)
                    else:
                        open_tiers[0]["qty"] -= remaining
                        remaining = 0

                # 평균단가 재계산
                if shares > 0 and open_tiers:
                    total_inv = sum(t["price"] * t["qty"] for t in open_tiers)
                    total_qty = sum(t["qty"] for t in open_tiers)
                    avg_cost  = total_inv / total_qty if total_qty > 0 else 0.0
                else:
                    avg_cost   = 0.0
                    open_tiers = []

        elif x <= tb:
            buy_qty = min(
                math.floor(current_chunk / x + 1e-9),
                math.floor(cash / x + 1e-9),
            )
            if buy_qty > 0:
                total_inv = avg_cost * shares + x * buy_qty
                shares   += buy_qty
                avg_cost  = total_inv / shares
                cash     -= buy_qty * x
                open_tiers.append({"date": date, "price": x, "qty": buy_qty})
                _date_val = date.date() if hasattr(date, "date") else date
                buy_trades.append({
                    "날짜":    str(_date_val),
                    "구분":    "매수",
                    "티커":    ticker_name,
                    "체결가":  x,
                    "수량":    buy_qty,
                    "금액($)": round(buy_qty * x, 2),
                    "비고":    f"평단 ${avg_cost:.2f} | 보유 {shares}주",
                })
                _day_action = "BUY"
                _day_qty    = buy_qty
                _day_amt    = round(buy_qty * x, 2)

        asset      = cash + shares * x
        prev_asset = asset
        peak_asset = max(peak_asset, asset)

        # 전체 날짜 기록 (백테스트 일별 상세표와 동일 형식)
        _date_val2   = date.date() if hasattr(date, "date") else date
        _oldest      = open_tiers[0]["date"] if open_tiers else None
        _oldest_date = _oldest.date() if _oldest and hasattr(_oldest, "date") else _oldest
        _hdays       = (date.date() - _oldest_date).days if _oldest_date else "-"
        daily_log.append({
            "날짜":       str(_date_val2),
            "종가(x)":    round(x, 4),
            "전날(p1)":   round(p1s[i], 4),
            "전전날(p2)": round(p2s[i], 4),
            "매수경계가": round(tb, 4),
            "매도경계가": round(ts, 4),
            "매매":       _day_action,
            "거래주수":   _day_qty,
            "거래금액($)":_day_amt,
            "보유주수":   shares,
            "보유기간":   _hdays if shares > 0 else "-",
            "현금($)":    round(cash, 2),
            "총자산($)":  round(asset, 2),
        })

    latest_price  = float(all_closes[-1])
    current_asset = cash + shares * latest_price
    total_return  = (current_asset - initial_capital) / initial_capital
    current_dd    = (current_asset - peak_asset) / peak_asset  # <= 0
    stock_weight  = (shares * latest_price / current_asset) if current_asset > 0 else 0.0
    years         = (today - pd.to_datetime(start_date).date()).days / 365.25
    cagr          = ((current_asset / initial_capital) ** (1.0 / years) - 1.0) if years > 0 else 0.0

    # 오늘 LOC 기준: 가장 최근 2개 종가
    p1_now = float(all_closes[-1])
    p2_now = float(all_closes[-2])
    next_buy_primary   = buy_limit_price(p1_now, p2_now, a_buy)
    next_buy_secondary = next_buy_primary * 0.95
    next_sell_target   = buy_limit_price(p1_now, p2_now, a_sell)

    min_tier_price = min(t["price"] for t in open_tiers) if open_tiers else 0.0
    chunk_now      = current_asset / divisions
    qty_primary    = math.floor(chunk_now / next_buy_primary + 1e-9) if next_buy_primary > 0 else 0
    min_str        = f"{min_tier_price:.2f}" if open_tiers else "-"

    pending_buys = [
        {
            "구분":   "매수", "티커": ticker_name,
            "주문가": next_buy_primary,
            "수량":   qty_primary,
            "금액":   qty_primary * next_buy_primary,
            "비고":   (f"LOC {next_buy_primary:.2f} - "
                       f"보유 티어 최저가({min_str}) "
                       f"목표매도가({next_sell_target:.2f})"),
        },
    ]

    return {
        "initial_capital":    initial_capital,
        "current_asset":      current_asset,
        "total_return":       total_return,
        "current_dd":         current_dd,
        "stock_weight":       stock_weight,
        "avg_cost":           avg_cost,
        "shares":             shares,
        "cash":               cash,
        "sell_trades":        sell_trades,
        "trade_history":      sorted(buy_trades + sell_trades, key=lambda r: r["날짜"]),
        "daily_log":          daily_log,
        "pending_buys":       pending_buys,
        "open_tiers":         open_tiers,
        "latest_price":       latest_price,
        "p1_now":             p1_now,
        "p2_now":             p2_now,
        "next_sell_target": next_sell_target,
        "next_buy_primary": next_buy_primary,
        "cagr":             cagr,
        "start_date":       start_date,
        "end_date":         today,
    }


# ──────────────────────────────────────────────
# 연도별 / 월별 성과 계산 (Tab 4 용)
# ──────────────────────────────────────────────
def compute_annual_stats(history_df, initial_capital):
    df = history_df.copy()
    df["날짜"] = pd.to_datetime(df["날짜"])
    df["Year"] = df["날짜"].dt.year
    rows = []
    prev_end = float(initial_capital)
    for yr in sorted(df["Year"].unique()):
        assets = df[df["Year"] == yr]["총자산($)"].values.astype(float)
        end_asset = float(assets[-1])
        annual_ret = (end_asset / prev_end - 1) * 100 if prev_end > 0 else 0.0
        all_a = np.concatenate([[prev_end], assets])
        peak  = np.maximum.accumulate(all_a)
        mdd   = float(((all_a - peak) / peak).min() * 100)
        rows.append({"연도": yr, "연간수익률(%)": round(annual_ret, 2), "MDD(%)": round(mdd, 2)})
        prev_end = end_asset
    return pd.DataFrame(rows)


def compute_monthly_pivot(history_df, initial_capital):
    df = history_df.copy()
    df["날짜"] = pd.to_datetime(df["날짜"])
    df["YM"] = df["날짜"].dt.to_period("M")
    monthly = []
    prev = float(initial_capital)
    for ym in sorted(df["YM"].unique()):
        end = float(df[df["YM"] == ym]["총자산($)"].iloc[-1])
        ret = (end / prev - 1) * 100 if prev > 0 else 0.0
        monthly.append({"Year": ym.year, "Month": ym.month, "Return": round(ret, 2)})
        prev = end
    mdf = pd.DataFrame(monthly)
    pivot = mdf.pivot(index="Year", columns="Month", values="Return")
    month_kr = {1:"1월",2:"2월",3:"3월",4:"4월",5:"5월",6:"6월",
                7:"7월",8:"8월",9:"9월",10:"10월",11:"11월",12:"12월"}
    pivot.columns = [month_kr.get(c, c) for c in pivot.columns]
    return pivot


# ──────────────────────────────────────────────
# 5티어 완전 투자 이벤트 분석
# ──────────────────────────────────────────────
def run_5tier_analysis(price_df, start_date, end_date, a_buy, a_sell, sell_ratio, divisions, initial_capital):
    """분할수(N) 이상 매수 후 매도된 사이클(N티어 완전 투자) 이벤트 추출."""
    sim_raw = price_df.loc[pd.to_datetime(start_date):pd.to_datetime(end_date)].copy()
    sim_raw["p1"] = sim_raw["Close"].shift(1)
    sim_raw["p2"] = sim_raw["Close"].shift(2)
    sim = sim_raw.dropna(subset=["p1", "p2"])
    if sim.empty:
        return []

    cash       = float(initial_capital)
    shares     = 0
    avg_cost   = 0.0
    open_tiers = []
    prev_asset = float(initial_capital)
    cycle_buys = []   # 현재 사이클의 매수 목록
    events     = []

    closes   = sim["Close"].values.astype(float)
    p1s      = sim["p1"].values.astype(float)
    p2s      = sim["p2"].values.astype(float)
    tgt_buy  = (p1s + p2s) * (1 + a_buy)  / (2 - a_buy)
    tgt_sell = (p1s + p2s) * (1 + a_sell) / (2 - a_sell)

    for i in range(len(closes)):
        x    = closes[i]
        tb   = tgt_buy[i]
        ts   = tgt_sell[i]
        date = sim.index[i]
        current_chunk = prev_asset / divisions

        if shares > 0 and x >= ts:
            sell_qty = math.floor(shares * (sell_ratio / 100.0))
            if sell_qty > 0:
                # N티어 이상 매수 사이클이면 이벤트 기록
                if len(cycle_buys) >= divisions:
                    nth  = cycle_buys[divisions - 1]
                    hold = (date.date() - nth["date"].date()).days
                    events.append({
                        "No":           len(events) + 1,
                        "5번째 매수일": str(nth["date"].date()),
                        "매도일":       str(date.date()),
                        "보유일수":     hold,
                        "5번째 매수가": round(nth["price"], 2),
                        "평균단가":     round(avg_cost, 2),
                        "매도가":       round(x, 2),
                        "손익률":       round((x / avg_cost - 1) * 100, 2) if avg_cost > 0 else 0,
                    })

                cash   += sell_qty * x
                shares -= sell_qty

                remaining = sell_qty
                while remaining > 0 and open_tiers:
                    if open_tiers[0]["qty"] <= remaining:
                        remaining -= open_tiers[0]["qty"]
                        open_tiers.pop(0)
                    else:
                        open_tiers[0]["qty"] -= remaining
                        remaining = 0

                if shares > 0 and open_tiers:
                    total_inv = sum(t["price"] * t["qty"] for t in open_tiers)
                    total_qty = sum(t["qty"] for t in open_tiers)
                    avg_cost  = total_inv / total_qty if total_qty > 0 else 0.0
                else:
                    avg_cost   = 0.0
                    open_tiers = []
                    cycle_buys = []   # 포지션 청산 → 사이클 초기화

        elif x <= tb:
            buy_qty = min(
                math.floor(current_chunk / x + 1e-9),
                math.floor(cash / x + 1e-9),
            )
            if buy_qty > 0:
                total_inv = avg_cost * shares + x * buy_qty
                shares   += buy_qty
                avg_cost  = total_inv / shares
                cash     -= buy_qty * x
                open_tiers.append({"date": date, "price": x, "qty": buy_qty})
                cycle_buys.append({"date": date, "price": x})

        asset      = cash + shares * x
        prev_asset = asset

    return events


# ──────────────────────────────────────────────
# 탭 구성
# ──────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 백테스트", "🔍 파라미터 최적화", "📋 오늘의 주문표", "📖 전략 소개 & 성과", "⚙️ 개인 설정"])


# ══════════════════════════════════════════════
# TAB 1 – 백테스트
# ══════════════════════════════════════════════
with tab1:
    if st.button("▶ 백테스트 실행", type="primary", key="run_bt"):
        with st.spinner("데이터 로드 및 시뮬레이션 중..."):
            price_df = load_price_data(ticker, start_date, end_date, data_source, excel_file)

        if price_df.empty:
            st.error("가격 데이터를 불러오지 못했습니다.")
            st.stop()

        result = run_backtest(
            price_df, start_date, end_date,
            a_buy, a_sell, sell_ratio, divisions, initial_capital,
            return_history=True,
        )
        if result is None:
            st.warning("선택된 기간 내 거래 데이터가 없습니다.")
            st.stop()

        # 성과 요약
        st.subheader("📊 성과 요약")
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("최종 자산 ($)",  f"${result['final_asset']:,.2f}", f"{result['total_return']*100:+.2f}%")
        m2.metric("CAGR",           f"{result['cagr']*100:.2f}%")
        m3.metric("MDD",            f"{result['mdd']*100:.2f}%")
        m4.metric("Calmar Ratio",   f"{result['calmar']:.3f}")
        m5.metric("총 매수 횟수",   f"{result['buy_count']} 회")
        m6.metric("총 매도 횟수",   f"{result['sell_count']} 회")

        # 당일 LOC 기준가
        st.subheader("📌 당일 (내일) LOC 예약 기준가")
        st.caption("백테스트 기간과 무관하게, 가장 최근의 실제 시장 종가 데이터를 기준으로 계산합니다.")
        if data_source == "엑셀 Daily_Close 시트" and excel_file is not None:
            today_p1 = scalar(price_df["Close"].iloc[-1])
            today_p2 = scalar(price_df["Close"].iloc[-2])
            today_ref = price_df.index[-1]
        else:
            recent_raw = yf.download(ticker, period="5d", progress=False, auto_adjust=True)
            if isinstance(recent_raw.columns, pd.MultiIndex):
                try:    recent_raw = recent_raw.xs(ticker, axis=1, level="Ticker")
                except: recent_raw.columns = recent_raw.columns.droplevel(1)
            today_p1  = scalar(recent_raw["Close"].iloc[-1])
            today_p2  = scalar(recent_raw["Close"].iloc[-2])
            today_ref = recent_raw.index[-1]

        next_date = today_ref + pd.Timedelta(days=1)
        if   next_date.weekday() == 5: next_date += pd.Timedelta(days=2)
        elif next_date.weekday() == 6: next_date += pd.Timedelta(days=1)

        st.dataframe(pd.DataFrame([{
            "예상 거래일":      next_date.date(),
            "p1 (전날 종가)":   today_p1,
            "p2 (전전날 종가)": today_p2,
            "당일 매수경계가":  buy_limit_price(today_p1, today_p2, a_buy),
            "당일 매도경계가":  buy_limit_price(today_p1, today_p2, a_sell),
        }]).style.format({
            "p1 (전날 종가)":   "${:,.5f}",
            "p2 (전전날 종가)": "${:,.5f}",
            "당일 매수경계가":  "${:,.5f}",
            "당일 매도경계가":  "${:,.5f}",
        }), hide_index=True, use_container_width=True)

        # 자산 추이
        st.subheader("📈 자산 추이")
        hist_df = result["history"]
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=hist_df["날짜"], y=hist_df["총자산($)"],
                                  mode="lines", name="총자산", line=dict(color="#2196F3", width=2)))
        buy_pts  = hist_df[hist_df["매매"] == "BUY"]
        sell_pts = hist_df[hist_df["매매"] == "SELL"]
        if not buy_pts.empty:
            fig.add_trace(go.Scatter(x=buy_pts["날짜"], y=buy_pts["총자산($)"],
                                      mode="markers", name="매수",
                                      marker=dict(color="red", size=7, symbol="triangle-up")))
        if not sell_pts.empty:
            fig.add_trace(go.Scatter(x=sell_pts["날짜"], y=sell_pts["총자산($)"],
                                      mode="markers", name="매도",
                                      marker=dict(color="green", size=7, symbol="triangle-down")))
        fig.update_layout(
            xaxis_title="Date", yaxis_title="Asset Value ($)",
            hovermode="x unified", height=450,
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig, use_container_width=True)

        # 일별 상세표
        st.subheader("🗓️ 일별 매매 상세표")
        colored = hist_df.style.format({
            "종가(x)": "${:,.4f}", "전날(p1)": "${:,.4f}",
            "전전날(p2)": "${:,.4f}", "매수경계가": "${:,.4f}",
            "매도경계가": "${:,.4f}", "거래주수": "{:,}",
            "거래금액($)": "${:,.2f}", "보유주수": "{:,}",
            "현금($)": "${:,.2f}", "총자산($)": "${:,.2f}",
        }).apply(
            lambda row: [
                "background-color: #ffdddd" if row["매매"] == "BUY"
                else ("background-color: #ddffdd" if row["매매"] == "SELL" else "")
                for _ in row
            ], axis=1,
        )
        st.dataframe(colored, use_container_width=True, height=500)

        csv = hist_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("💾 결과 CSV 다운로드", data=csv,
                           file_name=f"backtest_{ticker}.csv", mime="text/csv")


# ══════════════════════════════════════════════
# TAB 2 – 파라미터 최적화  (공통 헬퍼)
# ══════════════════════════════════════════════
def _show_opt_results(res_df, sort_col, ab_vals, as_vals, ticker, key_sfx):
    """최적화 결과 공통 표시 (상위 20, 히트맵, 산점도, CSV)"""
    st.subheader(f"🏆 상위 20개 결과  ({sort_col} 기준)")
    st.dataframe(res_df.head(20).style.format({
        "a_buy": "{:.4f}", "a_sell": "{:.4f}",
        "CAGR(%)": "{:.2f}%", "MDD(%)": "{:.2f}%",
        "Calmar": "{:.4f}", "총수익(%)": "{:.2f}%",
        "최종자산($)": "${:,.2f}",
    }), use_container_width=True)

    if ab_vals and as_vals and len(ab_vals) * len(as_vals) <= 2500:
        st.subheader(f"🗺️ 히트맵: a_buy × a_sell  →  {sort_col}")
        hmap_data = (
            res_df.groupby(["a_buy", "a_sell"])[sort_col].max().reset_index()
            .pivot(index="a_sell", columns="a_buy", values=sort_col)
        )
        show_text = len(ab_vals) * len(as_vals) <= 400
        fig_hmap = px.imshow(hmap_data, color_continuous_scale="RdYlGn",
                              labels={"x": "a_buy", "y": "a_sell", "color": sort_col},
                              aspect="auto", text_auto=".2f" if show_text else False)
        fig_hmap.update_layout(height=520)
        st.plotly_chart(fig_hmap, use_container_width=True)

    st.subheader("📊 리스크-수익 분포  (CAGR vs MDD)")
    fig_sc = px.scatter(res_df, x="MDD(%)", y="CAGR(%)", color=sort_col,
                         hover_data=["a_buy", "a_sell", "분할수", "매도비율", "Calmar"],
                         color_continuous_scale="RdYlGn")
    fig_sc.update_layout(height=450)
    st.plotly_chart(fig_sc, use_container_width=True)

    opt_csv = res_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("💾 최적화 결과 CSV 다운로드", data=opt_csv,
                       file_name=f"opt_{ticker}_{key_sfx}.csv", mime="text/csv",
                       key=f"dl_opt_{key_sfx}")


# ══════════════════════════════════════════════
# TAB 2 – 파라미터 최적화
# ══════════════════════════════════════════════
with tab2:
    st.subheader("🔍 파라미터 최적화")

    opt_method = st.radio(
        "최적화 방식",
        ["📊 그리드 탐색", "🎲 랜덤 탐색", "📈 워크포워드", "🧠 베이지안"],
        horizontal=True,
        key="opt_method",
    )
    _method_desc = {
        "📊 그리드 탐색": "모든 파라미터 조합을 완전 탐색합니다. 조합이 적을 때 가장 정확합니다.",
        "🎲 랜덤 탐색": "무작위로 N개 조합을 샘플링합니다. 탐색 공간이 클 때 빠르게 좋은 파라미터를 찾습니다.",
        "📈 워크포워드": "전체 기간을 IS(최적화)·OOS(검증) 윈도우로 분할해 과적합을 방지합니다. 실전에 가장 가까운 검증 방식입니다.",
        "🧠 베이지안": "Optuna TPE 알고리즘으로 스마트하게 탐색합니다. 적은 시도로 최적값에 빠르게 수렴합니다.",
    }
    st.caption(_method_desc[opt_method])

    # ── 공통 파라미터 범위 설정 ──────────────────
    with st.expander("파라미터 범위 설정", expanded=True):
        st.markdown("**매수 a값 범위 (a_buy)**")
        rc1, rc2, rc3 = st.columns(3)
        ab_min  = rc1.number_input("최솟값", value=-0.020, step=0.001, format="%.3f", key="ab_min")
        ab_max  = rc2.number_input("최댓값", value=-0.001, step=0.001, format="%.3f", key="ab_max")
        ab_step = rc3.number_input("간격",   value= 0.001, min_value=0.0001, step=0.001, format="%.4f", key="ab_step")

        st.markdown("**매도 a값 범위 (a_sell)**")
        rc4, rc5, rc6 = st.columns(3)
        as_min  = rc4.number_input("최솟값", value= 0.001, step=0.001, format="%.3f", key="as_min")
        as_max  = rc5.number_input("최댓값", value= 0.020, step=0.001, format="%.3f", key="as_max")
        as_step = rc6.number_input("간격",   value= 0.001, min_value=0.0001, step=0.001, format="%.4f", key="as_step")

        st.markdown("**분할수 범위**")
        rd1, rd2, rd3 = st.columns(3)
        dv_min  = rd1.number_input("최솟값", min_value=1, max_value=20, value=5,  step=1, key="dv_min")
        dv_max  = rd2.number_input("최댓값", min_value=1, max_value=20, value=5,  step=1, key="dv_max")
        dv_step = rd3.number_input("간격",   min_value=1, max_value=10, value=1,  step=1, key="dv_step")

        st.markdown("**매도비율 범위 (%)**")
        rs1, rs2, rs3 = st.columns(3)
        sr_min  = rs1.number_input("최솟값", min_value=10, max_value=100, value=100, step=10, key="sr_min")
        sr_max  = rs2.number_input("최댓값", min_value=10, max_value=100, value=100, step=10, key="sr_max")
        sr_step = rs3.number_input("간격",   min_value=10, max_value=50,  value=10,  step=10, key="sr_step")

        metric_key = st.selectbox("최적화 기준 지표", [
            "Calmar Ratio (CAGR / MDD)",
            "CAGR (%)",
            "총수익률 (%)",
            "MDD 최소화 (작을수록 좋음)",
        ])

    ab_vals = np.round(np.arange(ab_min, ab_max + ab_step * 0.5, ab_step), 6).tolist()
    as_vals = np.round(np.arange(as_min, as_max + as_step * 0.5, as_step), 6).tolist()
    dv_list = list(range(int(dv_min), int(dv_max) + 1, int(dv_step)))
    sr_list = list(range(int(sr_min), int(sr_max) + 1, int(sr_step)))
    if not dv_list: dv_list = [int(dv_min)]
    if not sr_list: sr_list = [int(sr_min)]
    n_total = len(ab_vals) * len(as_vals) * len(dv_list) * len(sr_list)

    # sort_col 미리 결정 (방식별 공통 사용)
    if "Calmar" in metric_key:    _sort_col, _sort_asc = "Calmar",    False
    elif "CAGR" in metric_key:    _sort_col, _sort_asc = "CAGR(%)",   False
    elif "총수익률" in metric_key: _sort_col, _sort_asc = "총수익(%)", False
    else:                          _sort_col, _sort_asc = "MDD(%)",    False

    # ── ① 그리드 탐색 ────────────────────────────
    if opt_method == "📊 그리드 탐색":
        info_msg = (f"예상 조합 수: **{n_total:,}개** "
                    f"(a_buy {len(ab_vals)} × a_sell {len(as_vals)} "
                    f"× 분할수 {len(dv_list)} × 매도비율 {len(sr_list)})")
        if n_total > 10000:
            st.error(info_msg + "  \n조합이 10,000개를 초과합니다. 범위를 줄이거나 간격을 늘려주세요.")
        elif n_total > 3000:
            st.warning(info_msg + "  \n조합이 많아 다소 시간이 걸릴 수 있습니다.")
        else:
            st.info(info_msg)

        if st.button("▶ 그리드 탐색 실행", type="primary", key="run_grid",
                     disabled=(n_total > 10000 or n_total == 0)):
            with st.spinner("가격 데이터 로드 중..."):
                price_df_opt = load_price_data(ticker, start_date, end_date, data_source, excel_file)
            if price_df_opt.empty:
                st.error("가격 데이터를 불러오지 못했습니다.")
                st.stop()

            progress     = st.progress(0.0, text="그리드 탐색 실행 중...")
            update_every = max(1, n_total // 100)
            rows, count  = [], 0

            for ab in ab_vals:
                for as_ in as_vals:
                    for dv in dv_list:
                        for sr in sr_list:
                            r = run_backtest(price_df_opt, start_date, end_date,
                                             ab, as_, sr, dv, initial_capital)
                            if r:
                                rows.append({
                                    "a_buy": ab, "a_sell": as_,
                                    "분할수": dv, "매도비율": sr,
                                    "CAGR(%)":     round(r["cagr"]         * 100, 2),
                                    "MDD(%)":      round(r["mdd"]          * 100, 2),
                                    "Calmar":      round(r["calmar"],             4),
                                    "총수익(%)":   round(r["total_return"] * 100, 2),
                                    "최종자산($)": round(r["final_asset"],        2),
                                    "매수횟수":    r["buy_count"],
                                    "매도횟수":    r["sell_count"],
                                })
                            count += 1
                            if count % update_every == 0:
                                progress.progress(min(count / n_total, 1.0),
                                                  text=f"실행 중... {count:,} / {n_total:,}")

            progress.progress(1.0, text="완료!")
            if not rows:
                st.error("유효한 결과가 없습니다.")
                st.stop()

            res_df = pd.DataFrame(rows).sort_values(_sort_col, ascending=_sort_asc).reset_index(drop=True)
            _show_opt_results(res_df, _sort_col, ab_vals, as_vals, ticker, "grid")

    # ── ② 랜덤 탐색 ──────────────────────────────
    elif opt_method == "🎲 랜덤 탐색":
        import random
        n_samples = st.number_input("샘플 수", min_value=50, max_value=5000,
                                    value=500, step=50, key="n_samples")
        st.info(f"랜덤으로 **{n_samples:,}개** 조합을 샘플링합니다. "
                f"(그리드 탐색 전체 {n_total:,}개 중 무작위 선택)")

        if st.button("▶ 랜덤 탐색 실행", type="primary", key="run_random"):
            with st.spinner("가격 데이터 로드 중..."):
                price_df_opt = load_price_data(ticker, start_date, end_date, data_source, excel_file)
            if price_df_opt.empty:
                st.error("가격 데이터를 불러오지 못했습니다.")
                st.stop()

            random.seed(42)
            sampled = [
                (round(random.uniform(ab_min, ab_max), 4),
                 round(random.uniform(as_min, as_max), 4),
                 random.choice(dv_list),
                 random.choice(sr_list))
                for _ in range(int(n_samples))
            ]
            progress = st.progress(0.0, text="랜덤 탐색 실행 중...")
            rows = []
            for i, (ab, as_, dv, sr) in enumerate(sampled):
                r = run_backtest(price_df_opt, start_date, end_date,
                                 ab, as_, sr, dv, initial_capital)
                if r:
                    rows.append({
                        "a_buy": ab, "a_sell": as_,
                        "분할수": dv, "매도비율": sr,
                        "CAGR(%)":     round(r["cagr"]         * 100, 2),
                        "MDD(%)":      round(r["mdd"]          * 100, 2),
                        "Calmar":      round(r["calmar"],             4),
                        "총수익(%)":   round(r["total_return"] * 100, 2),
                        "최종자산($)": round(r["final_asset"],        2),
                        "매수횟수":    r["buy_count"],
                        "매도횟수":    r["sell_count"],
                    })
                if i % max(1, int(n_samples) // 100) == 0:
                    progress.progress(min(i / int(n_samples), 1.0),
                                      text=f"실행 중... {i:,} / {int(n_samples):,}")

            progress.progress(1.0, text="완료!")
            if not rows:
                st.error("유효한 결과가 없습니다.")
                st.stop()

            res_df = pd.DataFrame(rows).sort_values(_sort_col, ascending=_sort_asc).reset_index(drop=True)
            _show_opt_results(res_df, _sort_col, None, None, ticker, "random")

    # ── ③ 워크포워드 ──────────────────────────────
    elif opt_method == "📈 워크포워드":
        from datetime import timedelta
        wf1, wf2 = st.columns(2)
        is_years  = wf1.number_input("IS(최적화) 기간 (년)", min_value=1, max_value=10, value=3, key="wf_is")
        oos_years = wf2.number_input("OOS(검증) 기간 (년)",  min_value=1, max_value=5,  value=1, key="wf_oos")

        st.info(
            f"📐 IS **{is_years}년** 최적화 → OOS **{oos_years}년** 검증을 슬라이딩 반복합니다.\n\n"
            f"그리드 조합 **{n_total:,}개** × 윈도우 수 만큼 백테스트가 실행됩니다."
        )

        if st.button("▶ 워크포워드 실행", type="primary", key="run_wfo"):
            with st.spinner("가격 데이터 로드 중..."):
                price_df_opt = load_price_data(ticker, start_date, end_date, data_source, excel_file)
            if price_df_opt.empty:
                st.error("가격 데이터를 불러오지 못했습니다.")
                st.stop()

            dates       = price_df_opt.index
            total_start = dates[0].date()
            total_end   = dates[-1].date()

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
                st.error("데이터 기간이 너무 짧아 윈도우를 생성할 수 없습니다. IS+OOS 기간을 줄여주세요.")
                st.stop()

            st.info(f"총 **{len(windows)}개** 윈도우 생성됨")
            total_steps = len(windows) * max(n_total, 1)
            progress    = st.progress(0.0, text="워크포워드 실행 중...")
            step_count  = 0
            wfo_rows    = []
            cur_capital = initial_capital

            for wi, (is_s, is_e, oos_s, oos_e) in enumerate(windows):
                best_score, best_params, best_is_r = -999.0, None, None

                for ab in ab_vals:
                    for as_ in as_vals:
                        for dv in dv_list:
                            for sr in sr_list:
                                r = run_backtest(price_df_opt, str(is_s), str(is_e),
                                                 ab, as_, sr, dv, initial_capital)
                                if r:
                                    if "Calmar" in metric_key:    score = r["calmar"]
                                    elif "CAGR" in metric_key:    score = r["cagr"] * 100
                                    elif "총수익률" in metric_key: score = r["total_return"] * 100
                                    else:                          score = -abs(r["mdd"] * 100)
                                    if score > best_score:
                                        best_score  = score
                                        best_params = (ab, as_, dv, sr)
                                        best_is_r   = r
                                step_count += 1
                                if step_count % max(1, total_steps // 200) == 0:
                                    progress.progress(
                                        min(step_count / total_steps, 0.99),
                                        text=f"윈도우 {wi+1}/{len(windows)} IS 최적화 중..."
                                    )

                if best_params is None:
                    continue

                ab_b, as_b, dv_b, sr_b = best_params
                oos_r = run_backtest(price_df_opt, str(oos_s), str(oos_e),
                                     ab_b, as_b, sr_b, dv_b, cur_capital)
                if oos_r is None:
                    continue

                wfo_rows.append({
                    "윈도우":      wi + 1,
                    "IS 기간":     f"{is_s} ~ {is_e}",
                    "OOS 기간":    f"{oos_s} ~ {oos_e}",
                    "Best a_buy":  ab_b,
                    "Best a_sell": as_b,
                    f"IS {_sort_col}": round(best_score, 3),
                    "OOS Calmar":  round(oos_r["calmar"],       3),
                    "OOS CAGR(%)": round(oos_r["cagr"]  * 100, 2),
                    "OOS MDD(%)":  round(oos_r["mdd"]   * 100, 2),
                    "시작($)":     round(cur_capital,           2),
                    "종료($)":     round(oos_r["final_asset"],  2),
                })
                cur_capital = oos_r["final_asset"]

            progress.progress(1.0, text="완료!")
            if not wfo_rows:
                st.error("유효한 OOS 결과가 없습니다.")
                st.stop()

            wfo_df    = pd.DataFrame(wfo_rows)
            total_ret = (cur_capital - initial_capital) / initial_capital

            # 종합 요약
            st.subheader("📊 워크포워드 종합 성과")
            wc1, wc2, wc3, wc4 = st.columns(4)
            wc1.metric("시작 자본",        f"${initial_capital:,.0f}")
            wc2.metric("최종 자본 (OOS)",  f"${cur_capital:,.0f}")
            wc3.metric("OOS 총 수익률",    f"{total_ret*100:+.2f}%")
            wc4.metric("윈도우 수",        f"{len(wfo_rows)}개")

            # 윈도우별 결과 테이블
            st.subheader("🪟 윈도우별 결과")
            st.dataframe(wfo_df.style.format({
                "Best a_buy":  "{:.4f}",
                "Best a_sell": "{:.4f}",
                "OOS Calmar":  "{:.3f}",
                "OOS CAGR(%)": "{:.2f}%",
                "OOS MDD(%)":  "{:.2f}%",
                "시작($)":     "${:,.2f}",
                "종료($)":     "${:,.2f}",
            }), use_container_width=True)

            # OOS CAGR 바차트
            fig_wfo = px.bar(
                wfo_df, x="윈도우", y="OOS CAGR(%)", color="OOS CAGR(%)",
                color_continuous_scale="RdYlGn", text_auto=".1f",
                title="윈도우별 OOS CAGR (%)"
            )
            fig_wfo.add_hline(y=0, line_dash="dash", line_color="gray")
            fig_wfo.update_layout(height=400)
            st.plotly_chart(fig_wfo, use_container_width=True)

            # OOS 자본 곡선
            fig_cap = px.line(
                wfo_df, x="윈도우", y="종료($)",
                title="OOS 자본 변화 (윈도우별 종료 자산)", markers=True
            )
            fig_cap.update_layout(height=380)
            st.plotly_chart(fig_cap, use_container_width=True)

            wfo_csv = wfo_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button("💾 워크포워드 결과 CSV", data=wfo_csv,
                               file_name=f"wfo_{ticker}.csv", mime="text/csv",
                               key="dl_wfo")

    # ── ④ 베이지안 (Optuna) ───────────────────────
    elif opt_method == "🧠 베이지안":
        try:
            import optuna as _optuna
            _optuna_ok = True
        except ImportError:
            _optuna_ok = False

        if not _optuna_ok:
            st.error("`optuna` 패키지가 설치되지 않았습니다. "
                     "`requirements.txt`에 `optuna>=3.6.0` 추가 후 재배포하세요.")
        else:
            bc1, _ = st.columns(2)
            n_trials = bc1.number_input("탐색 횟수 (trials)", min_value=50,
                                        max_value=2000, value=300, step=50, key="n_trials")
            st.info(
                f"Optuna TPE 알고리즘으로 **{n_trials}회** 스마트 탐색합니다.\n\n"
                f"그리드 탐색({n_total:,}개) 대비 적은 시도로 최적값에 근접합니다."
            )

            if st.button("▶ 베이지안 최적화 실행", type="primary", key="run_bayes"):
                with st.spinner("가격 데이터 로드 중..."):
                    price_df_opt = load_price_data(ticker, start_date, end_date, data_source, excel_file)
                if price_df_opt.empty:
                    st.error("가격 데이터를 불러오지 못했습니다.")
                    st.stop()

                _optuna.logging.set_verbosity(_optuna.logging.WARNING)
                progress     = st.progress(0.0, text="베이지안 탐색 실행 중...")
                trial_rows   = []
                _tc          = [0]

                def _objective(trial):
                    ab  = trial.suggest_float("a_buy",  ab_min, ab_max)
                    as_ = trial.suggest_float("a_sell", as_min, as_max)
                    dv  = trial.suggest_int("분할수",  int(dv_min), int(dv_max)) if dv_min != dv_max else int(dv_min)
                    sr  = trial.suggest_int("매도비율", int(sr_min), int(sr_max), step=int(sr_step)) if sr_min != sr_max else int(sr_min)
                    r   = run_backtest(price_df_opt, start_date, end_date,
                                       ab, as_, sr, dv, initial_capital)
                    if r is None:
                        return -999.0
                    if "Calmar" in metric_key:    score = r["calmar"]
                    elif "CAGR" in metric_key:    score = r["cagr"] * 100
                    elif "총수익률" in metric_key: score = r["total_return"] * 100
                    else:                          score = -abs(r["mdd"] * 100)
                    trial_rows.append({
                        "a_buy": round(ab, 4), "a_sell": round(as_, 4),
                        "분할수": dv, "매도비율": sr,
                        "CAGR(%)":     round(r["cagr"]         * 100, 2),
                        "MDD(%)":      round(r["mdd"]          * 100, 2),
                        "Calmar":      round(r["calmar"],             4),
                        "총수익(%)":   round(r["total_return"] * 100, 2),
                        "최종자산($)": round(r["final_asset"],        2),
                        "매수횟수":    r["buy_count"],
                        "매도횟수":    r["sell_count"],
                    })
                    _tc[0] += 1
                    if _tc[0] % max(1, int(n_trials) // 50) == 0:
                        progress.progress(min(_tc[0] / int(n_trials), 1.0),
                                          text=f"베이지안 탐색 중... {_tc[0]:,} / {int(n_trials):,}")
                    return score

                study = _optuna.create_study(
                    direction="maximize",
                    sampler=_optuna.samplers.TPESampler(seed=42)
                )
                study.optimize(_objective, n_trials=int(n_trials))
                progress.progress(1.0, text="완료!")

                if not trial_rows:
                    st.error("유효한 결과가 없습니다.")
                    st.stop()

                res_df = pd.DataFrame(trial_rows).sort_values(
                    _sort_col, ascending=_sort_asc
                ).reset_index(drop=True)

                best = study.best_params
                st.success(
                    f"🏆 최적 파라미터: a_buy=**{best['a_buy']:.4f}**, "
                    f"a_sell=**{best['a_sell']:.4f}**, "
                    f"분할수=**{best.get('분할수', int(dv_min))}**, "
                    f"매도비율=**{best.get('매도비율', int(sr_min))}%**"
                )

                _show_opt_results(res_df, _sort_col, None, None, ticker, "bayes")

                # 수렴 곡선
                st.subheader("📈 탐색 수렴 과정")
                _vals     = [t.value for t in study.trials if t.value is not None and t.value > -900]
                _best_cur = [max(_vals[:i+1]) for i in range(len(_vals))]
                fig_conv  = px.line(
                    y=_best_cur,
                    labels={"y": f"Best {_sort_col}", "index": "Trial"},
                    title="베이지안 최적화 수렴 곡선"
                )
                fig_conv.update_layout(height=380)
                st.plotly_chart(fig_conv, use_container_width=True)


# ══════════════════════════════════════════════
# TAB 3 – 오늘의 주문표 (멀티 계좌 렌더러)
# ══════════════════════════════════════════════
def _render_account_tab(tk: str, tk_cfg: dict, key_sfx: str):
    """ticker별 주문표 탭 렌더링. key_sfx로 위젯 key 충돌 방지."""
    _a_buy      = float(tk_cfg.get("a_buy",      -0.005))
    _a_sell     = float(tk_cfg.get("a_sell",      0.009))
    _sell_ratio = float(tk_cfg.get("sell_ratio",  100.0))
    _divisions  = int  (tk_cfg.get("divisions",   5))

    _raw_start   = tk_cfg.get("os_start",   "2024-01-01")
    _raw_capital = tk_cfg.get("os_capital",  10000.0)
    try:    _default_start = datetime.strptime(str(_raw_start), "%Y-%m-%d").date()
    except: _default_start = datetime(2024, 1, 1).date()
    try:    _default_capital = float(_raw_capital)
    except: _default_capital = 10000.0

    # ── 계좌 삭제 ──
    _del_col, _ = st.columns([1, 5])
    if _del_col.button(f"🗑️ {tk} 계좌 삭제", key=f"del_{key_sfx}", type="secondary"):
        st.session_state[f"del_confirm_{key_sfx}"] = True
    if st.session_state.get(f"del_confirm_{key_sfx}", False):
        st.warning(f"⚠️ **{tk} 계좌를 삭제하시겠습니까?** 저장된 설정이 모두 삭제됩니다.")
        _dc1, _dc2, _ = st.columns([1, 1, 4])
        if _dc1.button("✅ 삭제", key=f"del_ok_{key_sfx}", type="primary"):
            _delete_ticker_setting(tk)
            st.session_state.pop(f"del_confirm_{key_sfx}", None)
            st.rerun()
        if _dc2.button("❌ 취소", key=f"del_cancel_{key_sfx}"):
            st.session_state[f"del_confirm_{key_sfx}"] = False
            st.rerun()

    # ── 시작일 / 자본금 ──
    c1, c2 = st.columns(2)
    os_start   = c1.date_input("시작일", value=_default_start,
                                min_value=datetime(2000, 1, 1).date(),
                                max_value=datetime.today().date(),
                                key=f"os_start_{key_sfx}")
    os_capital = c2.number_input("시작 자본 ($)", value=_default_capital,
                                  step=1000.0, key=f"os_capital_{key_sfx}")

    # ── 자본 조정 ──
    with st.expander("💰 자본 조정 (증액 / 감액)"):
        st.caption("현재 자본금에 추가하거나 차감할 금액을 입력하세요.")
        _adj_history_raw = tk_cfg.get("capital_adj_history", "[]")
        try:
            _adj_history = json.loads(_adj_history_raw) if isinstance(_adj_history_raw, str) else _adj_history_raw
            if not isinstance(_adj_history, list): _adj_history = []
        except: _adj_history = []

        _adj_c1, _adj_c2 = st.columns([2, 1])
        _adj_amount = _adj_c1.number_input("조정 금액 ($)", value=0.0, step=500.0,
                                            help="증액: 양수 · 감액: 음수",
                                            key=f"capital_adj_input_{key_sfx}")
        _adj_c1.caption(
            f"적용 후 자본금: **${_default_capital + _adj_amount:,.0f}** "
            f"({'↑' if _adj_amount > 0 else '↓' if _adj_amount < 0 else '='} "
            f"${abs(_adj_amount):,.0f})"
        )
        _adj_memo = _adj_c1.text_input("메모 (선택)", placeholder="예: 3월 추가 입금",
                                        key=f"adj_memo_{key_sfx}")
        if _adj_c2.button("💰 적용", use_container_width=True,
                          key=f"apply_adj_{key_sfx}", disabled=(_adj_amount == 0)):
            _new_capital = _default_capital + _adj_amount
            if _new_capital <= 0:
                st.error("자본금은 0보다 커야 합니다.")
            else:
                _adj_history.append({
                    "날짜": datetime.today().strftime("%Y-%m-%d"),
                    "조정금액": float(_adj_amount),
                    "누적자본금": float(_new_capital),
                    "메모": _adj_memo or ("증액" if _adj_amount > 0 else "감액"),
                })
                _save_ticker_setting(tk, {
                    "os_capital": _new_capital,
                    "capital_adj_history": json.dumps(_adj_history, ensure_ascii=False)
                })
                st.success(f"✅ 자본금이 **${_new_capital:,.0f}**으로 업데이트되었습니다.")
                st.rerun()

        if _adj_history:
            st.markdown("---")
            st.markdown("**📋 자본 조정 이력**")
            _df_adj = pd.DataFrame(_adj_history)
            _df_adj["조정금액"]  = _df_adj["조정금액"].apply(lambda x: f"{'↑' if x>0 else '↓'} ${abs(x):,.0f}")
            _df_adj["누적자본금"] = _df_adj["누적자본금"].apply(lambda x: f"${x:,.0f}")
            st.dataframe(_df_adj[["날짜","조정금액","누적자본금","메모"]],
                         use_container_width=True, hide_index=True)
        else:
            st.info("아직 자본 조정 이력이 없습니다.")

        # 전체 초기화
        st.markdown("---")
        st.markdown("**🔄 전체 초기화**")
        st.caption("시작일·자본금·조정 이력을 모두 초기화합니다.")
        _rc1, _rc2, _rc3 = st.columns(3)
        _reset_start   = _rc1.date_input("새 시작일", value=datetime.today().date(),
                                          key=f"reset_start_{key_sfx}")
        _reset_capital = _rc2.number_input("새 시작 자본 ($)", value=_default_capital,
                                            step=1000.0, key=f"reset_capital_{key_sfx}")
        if _rc3.button("🔄 초기화", use_container_width=True,
                       key=f"do_reset_{key_sfx}", type="secondary"):
            st.session_state[f"reset_confirmed_{key_sfx}"] = True
        if st.session_state.get(f"reset_confirmed_{key_sfx}", False):
            st.warning(f"⚠️ **정말 초기화하시겠습니까?**  \n"
                       f"시작일: {_reset_start} / 자본금: ${_reset_capital:,.0f} / 조정 이력 전체 삭제")
            _conf_c1, _conf_c2 = st.columns(2)
            if _conf_c1.button("✅ 확인 (초기화)", type="primary", key=f"confirm_reset_{key_sfx}"):
                _save_ticker_setting(tk, {
                    "os_start": str(_reset_start),
                    "os_capital": float(_reset_capital),
                    "capital_adj_history": "[]",
                })
                st.session_state[f"reset_confirmed_{key_sfx}"] = False
                st.success(f"✅ 초기화 완료! 시작일: {_reset_start} / 자본금: ${_reset_capital:,.0f}")
                st.rerun()
            if _conf_c2.button("❌ 취소", key=f"cancel_reset_{key_sfx}"):
                st.session_state[f"reset_confirmed_{key_sfx}"] = False
                st.rerun()

    # ── 주문표 로드 ──
    if st.button("📋 주문표 로드", type="primary", key=f"run_os_{key_sfx}"):
        _save_ticker_setting(tk, {"os_start": str(os_start), "os_capital": os_capital})
        today = datetime.today().date()
        with st.spinner("데이터 로드 및 포트폴리오 시뮬레이션 중..."):
            price_df_os = load_price_data(tk, os_start, today, "야후파이낸스 (yfinance)", None)
        if price_df_os.empty:
            st.error("가격 데이터를 불러오지 못했습니다.")
            return

        res = run_portfolio_for_ordersheet(
            price_df_os, os_start, tk,
            _a_buy, _a_sell, _sell_ratio, _divisions, os_capital,
        )
        if res is None:
            st.warning("시뮬레이션 데이터가 없습니다.")
            return

        st.markdown(f"**{res['start_date']} ~ {res['end_date']}**")
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("시작 자본",  f"${res['initial_capital']:,.0f}")
        m2.metric("현재 자산",  f"${res['current_asset']:,.0f}",
                  delta=f"{res['total_return']*100:+.2f}%")
        m3.metric("수익률",     f"{res['total_return']*100:+.2f}%",
                  delta=f"CAGR {res['cagr']*100:.2f}%")
        m4.metric("현재 DD",    f"{abs(res['current_dd'])*100:.2f}%",
                  delta=f"{res['current_dd']*100:.2f}%", delta_color="inverse")
        m5.metric("주식 비중",  f"{res['stock_weight']*100:.1f}%")

        # 오늘의 LOC 주문
        lp, p1, p2 = res["latest_price"], res["p1_now"], res["p2_now"]
        st.subheader("📑 오늘의 LOC 주문")
        st.caption(f"p1(전일종가)=**${p1:,.2f}** · p2(전전일종가)=**${p2:,.2f}** · 최근가=**${lp:,.2f}**")

        today_orders = []
        if res["shares"] > 0:
            sell_qty = math.floor(res["shares"] * (_sell_ratio / 100.0))
            sell_tgt = res["next_sell_target"]
            today_orders.append({
                "구분": "매도", "티커": tk,
                "LOC 기준가": f"${sell_tgt:,.2f}", "1회매수금": "-",
                "예상수량": f"{sell_qty:,}주",
                "예상금액": f"${sell_qty * sell_tgt:,.2f}",
                "전일종가 대비": f"{(sell_tgt/lp-1)*100:+.2f}%" if lp > 0 else "-",
                "비고": (f"평단 ${res['avg_cost']:.2f} 대비 "
                         f"{(sell_tgt/res['avg_cost']-1)*100:+.2f}%  |  "
                         f"보유 {res['shares']:,}주 × {_sell_ratio:.0f}%"),
            })
        buy_p = res["next_buy_primary"]
        qty_p = res["pending_buys"][0]["수량"]
        today_orders.append({
            "구분": "매수", "티커": tk,
            "LOC 기준가": f"${buy_p:,.2f}",
            "1회매수금": f"${res['current_asset'] / _divisions:,.2f}",
            "예상수량": f"{qty_p:,}주",
            "예상금액": f"${qty_p * buy_p:,.2f}",
            "전일종가 대비": f"{(buy_p/lp-1)*100:+.2f}%" if lp > 0 else "-",
            "비고": res["pending_buys"][0]["비고"],
        })

        def _style_gubun(row):
            s = [""] * len(row)
            if "구분" in row.index:
                i = list(row.index).index("구분")
                s[i] = "color: #1565C0; font-weight: bold" if row["구분"] == "매도" else \
                        "color: #C62828; font-weight: bold" if row["구분"] == "매수" else ""
            return s

        st.dataframe(pd.DataFrame(today_orders).style.apply(_style_gubun, axis=1),
                     use_container_width=True, hide_index=True,
                     height=38 + 35 * len(today_orders))

        # 현재 보유 현황
        st.subheader("📦 현재 보유 현황")
        if res["shares"] > 0:
            avg_c = res["avg_cost"]
            hc = st.columns(6)
            hc[0].metric("보유주수",  f"{res['shares']:,}주")
            hc[1].metric("평균단가",  f"${avg_c:.2f}")
            hc[2].metric("현재가",    f"${lp:.2f}")
            hc[3].metric("평가금액",  f"${res['shares']*lp:,.2f}")
            hc[4].metric("평가손익",  f"${(lp-avg_c)*res['shares']:,.2f}",
                          delta=f"{(lp/avg_c-1)*100:+.2f}%" if avg_c > 0 else "")
            hc[5].metric("보유현금",  f"${res['cash']:,.2f}")
            if res["open_tiers"]:
                with st.expander(f"보유 티어 상세 ({len(res['open_tiers'])}개 배치)"):
                    tiers_rows = []
                    for t in res["open_tiers"]:
                        bd = t["date"].date() if hasattr(t["date"], "date") else t["date"]
                        tiers_rows.append({
                            "매수일": str(bd),
                            "매수가": f"${t['price']:.2f}",
                            "수량": f"{t['qty']:,}주",
                            "매수금액": f"${t['price']*t['qty']:,.2f}",
                            "현재손익률": f"{(lp/t['price']-1)*100:+.2f}%" if t['price'] > 0 else "-",
                            "보유일수": f"{(datetime.today().date()-bd).days}일",
                        })
                    st.dataframe(pd.DataFrame(tiers_rows), hide_index=True, use_container_width=True)
        else:
            st.info("현재 보유 주식 없음 (전량 현금)")
            st.metric("보유현금", f"${res['cash']:,.2f}")

        # 일별 매매 상세표
        st.divider()
        st.subheader("📅 일별 매매 상세표")
        _dl = res.get("daily_log", [])
        if _dl:
            _df_daily = pd.DataFrame(_dl).sort_values("날짜", ascending=False).reset_index(drop=True)
            _bc = (_df_daily["매매"] == "BUY").sum()
            _sc = (_df_daily["매매"] == "SELL").sum()
            st.caption(f"시작일 {res['start_date']} ~ {res['end_date']} | "
                       f"총 {_bc+_sc}건 (매수 {_bc}회 · 매도 {_sc}회)")
            _df_show = _df_daily.copy()
            for _col in ["종가(x)", "전날(p1)", "전전날(p2)", "매수경계가", "매도경계가"]:
                _df_show[_col] = _df_show[_col].apply(lambda v: f"${v:,.4f}")
            _df_show["거래금액($)"] = _df_show["거래금액($)"].apply(lambda v: f"${v:,.2f}" if v != 0 else "-")
            _df_show["현금($)"]    = _df_show["현금($)"].apply(lambda v: f"${v:,.2f}")
            _df_show["총자산($)"]  = _df_show["총자산($)"].apply(lambda v: f"${v:,.2f}")
            _df_show["거래주수"]   = _df_show["거래주수"].apply(lambda v: f"{v:,}" if v != 0 else "-")

            def _style_daily(row):
                if row["매매"] == "BUY":  return ["background-color: #FFF0F0"] * len(row)
                if row["매매"] == "SELL": return ["background-color: #F0FFF4"] * len(row)
                return [""] * len(row)
            def _style_action(val):
                if val == "BUY":  return "color: #C62828; font-weight: bold"
                if val == "SELL": return "color: #1565C0; font-weight: bold"
                return "color: #999"

            st.dataframe(_df_show.style.apply(_style_daily, axis=1)
                                        .applymap(_style_action, subset=["매매"]),
                         hide_index=True, use_container_width=True,
                         height=min(38 + 35 * len(_df_show), 600))

            import io as _io
            _today_dl = str(datetime.today().date()).replace("-", "")
            _dl1, _dl2, _ = st.columns([1, 1, 4])
            _csv_data = _df_daily.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            _dl1.download_button("📥 CSV 다운로드", data=_csv_data,
                                  file_name=f"{tk}_daily_history_{_today_dl}.csv",
                                  mime="text/csv", key=f"dl_csv_{key_sfx}", use_container_width=True)
            _buf = _io.BytesIO()
            with pd.ExcelWriter(_buf, engine="openpyxl") as _writer:
                _df_daily.to_excel(_writer, index=False, sheet_name="일별매매상세")
            _dl2.download_button("📥 엑셀 다운로드", data=_buf.getvalue(),
                                  file_name=f"{tk}_daily_history_{_today_dl}.xlsx",
                                  mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                  key=f"dl_xlsx_{key_sfx}", use_container_width=True)
        else:
            st.info("📭 시작일부터 오늘까지 데이터가 없습니다.")


with tab3:
    st.subheader("📋 오늘의 주문표")
    st.caption("종목별 포트폴리오를 추적하여 현황과 내일 LOC 주문을 표시합니다.")

    # 등록된 ticker 설정 전체 로드
    _all_tk_settings  = _get_ticker_settings()
    _registered_tickers = list(_all_tk_settings.keys())

    # ── 계좌 추가 ─────────────────────────────────────────────
    with st.expander("➕ 계좌 추가"):
        _add_presets = ["SOXL", "USD", "TQQQ", "직접입력"]
        _add_select  = st.selectbox("종목코드", _add_presets, key="add_tk_select")
        if _add_select == "직접입력":
            _add_tk = st.text_input("직접 입력", placeholder="예: NVDA, SPY, QQQ",
                                    key="add_tk_input").strip().upper()
        else:
            _add_tk = _add_select

        if _add_tk:
            _ac1, _ac2 = st.columns(2)
            _add_a_buy   = _ac1.number_input("매수 a값",    value=-0.005, step=0.001, format="%.4f", key="add_a_buy")
            _add_a_sell  = _ac2.number_input("매도 a값",    value=0.009,  step=0.001, format="%.4f", key="add_a_sell")
            _add_sr      = _ac1.number_input("매도비율 (%)", value=100.0,  step=10.0,                 key="add_sr")
            _add_div     = _ac2.number_input("분할수",       value=5,      min_value=1, step=1,        key="add_div")
            _add_start   = _ac1.date_input(  "시작일",       value=datetime(2024, 1, 1).date(),        key="add_os_start")
            _add_capital = _ac2.number_input("시작 자본 ($)", value=10000.0, step=1000.0,              key="add_os_capital")

            if st.button(f"✅ {_add_tk} 계좌 등록", type="primary", key="add_tk_btn"):
                if _add_tk in _registered_tickers:
                    st.warning(f"⚠️ {_add_tk} 계좌가 이미 등록되어 있습니다.")
                else:
                    _save_ticker_setting(_add_tk, {
                        "a_buy": float(_add_a_buy), "a_sell": float(_add_a_sell),
                        "sell_ratio": float(_add_sr), "divisions": int(_add_div),
                        "os_start": str(_add_start), "os_capital": float(_add_capital),
                    })
                    st.success(f"✅ {_add_tk} 계좌가 등록되었습니다!")
                    st.rerun()

    # ── 등록된 계좌 표시 ──────────────────────────────────────
    if not _registered_tickers:
        st.info("📭 등록된 계좌가 없습니다. '➕ 계좌 추가'를 눌러 첫 계좌를 등록하세요.")
    elif len(_registered_tickers) == 1:
        _tk = _registered_tickers[0]
        _render_account_tab(_tk, _all_tk_settings[_tk], _tk)
    else:
        _tabs_os = st.tabs([f"📊 {t}" for t in _registered_tickers])
        for _i, _tk in enumerate(_registered_tickers):
            with _tabs_os[_i]:
                _render_account_tab(_tk, _all_tk_settings[_tk], _tk)


# ══════════════════════════════════════════════
# TAB 4 – 전략 소개 & 성과
# ══════════════════════════════════════════════
with tab4:
    # ── 전략 설명 ──────────────────────────────
    st.subheader("📖 종가평균매매법 (3-Day LOC 전략) 이란?")

    left, right = st.columns([3, 2])

    with left:
        st.markdown("""
#### 전략 개요
**종가평균매매법**은 직전 2거래일의 종가(p1, p2)를 기준으로
당일 매수/매도 **LOC(Limit-On-Close)** 주문 기준가를 계산하는 퀀트 전략입니다.

주가가 최근 평균보다 **충분히 낮으면** 매수,
**충분히 높으면** 매도하는 평균 회귀 방식으로 작동합니다.

---

#### 매수 룰
- 당일 종가 **≤ 매수경계가** 이면 LOC 매수 체결
- 1회 매수금액 = 현재 총자산 ÷ 분할수(N)
- 매수 금액만큼 최대 가능한 정수 주수 매수

#### 매도 룰
- 보유 중이고 당일 종가 **≥ 매도경계가** 이면 LOC 매도 체결
- 보유 수량 × 매도비율(%) 만큼 매도

#### 포지션 관리
| 파라미터 | 설명 |
|---|---|
| a_buy | 매수경계가 조정값 (음수 → 평균 이하에서 매수) |
| a_sell | 매도경계가 조정값 (양수 → 평균 이상에서 매도) |
| 분할수 N | 자산을 N등분하여 1회 매수 금액 결정 |
| 매도비율 | 보유 수량 중 몇 %를 한 번에 매도할지 |
        """)

    with right:
        st.info("""
**경계가 공식**

```
p1  = 전일(D-1) 종가
p2  = 전전일(D-2) 종가
a   = 파라미터값

경계가 = (p1 + p2) × (1 + a)
              ÷ (2 - a)
```

- a < 0 → 평균보다 낮은 가격 (매수)
- a > 0 → 평균보다 높은 가격 (매도)
- |a| 클수록 경계가가 평균에서 더 멀어짐
        """)
        st.info("""
**LOC 주문이란?**

장 마감 직전 일정 가격 이하/이상이면
종가로 체결되는 조건부 시장가 주문입니다.

당일 오후 3시 55분(미국 기준) 이전에
기준가 조건을 확인 후 주문을 넣습니다.
        """)

    st.divider()

    # ── 성과 분석 ──────────────────────────────
    st.subheader("📊 전략 성과 분석")

    def _render_perf_analysis(tk, a_b, a_s, sr, div, init_cap, s_date, e_date):
        """ticker 하나의 성과 분석 전체를 렌더링."""
        with st.spinner(f"{tk} 데이터 로드 및 분석 중..."):
            _pdf = load_price_data(tk, s_date, e_date, "야후파이낸스 (yfinance)", None)
        if _pdf.empty:
            st.error(f"{tk}: 가격 데이터를 불러오지 못했습니다.")
            return

        _res = run_backtest(_pdf, s_date, e_date, a_b, a_s, sr, div, init_cap, return_history=True)
        if _res is None:
            st.warning(f"{tk}: 선택된 기간 내 거래 데이터가 없습니다.")
            return

        _hist = _res["history"]

        sm1, sm2, sm3, sm4 = st.columns(4)
        sm1.metric("전체 CAGR",    f"{_res['cagr']*100:.2f}%")
        sm2.metric("전체 수익률",  f"{_res['total_return']*100:+.2f}%")
        sm3.metric("최대 MDD",     f"{_res['mdd']*100:.2f}%")
        sm4.metric("Calmar Ratio", f"{_res['calmar']:.3f}")
        st.divider()

        st.subheader("📅 연도별 성과")
        _annual = compute_annual_stats(_hist, init_cap)
        def _color_ret(val):
            if isinstance(val, (int, float)):
                if val > 0: return "color: #2e7d32; font-weight:bold"
                if val < 0: return "color: #c62828; font-weight:bold"
            return ""
        st.dataframe(
            _annual.style.applymap(_color_ret, subset=["연간수익률(%)"])
                         .format({"연간수익률(%)": "{:+.2f}%", "MDD(%)": "{:.2f}%"}),
            hide_index=True, use_container_width=True)
        st.divider()

        st.subheader("🗓️ 월별 수익률 히트맵")
        _mp = compute_monthly_pivot(_hist, init_cap)
        _fig_m = px.imshow(_mp, color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
                           text_auto=".1f", labels={"x": "월", "y": "연도", "color": "수익률(%)"},
                           aspect="auto")
        _fig_m.update_layout(height=max(320, len(_mp) * 38 + 120),
                              coloraxis_colorbar=dict(title="수익률(%)"))
        st.plotly_chart(_fig_m, use_container_width=True)
        st.divider()

        st.subheader("📋 종합 성과 요약")
        _fa = _res.get("final_asset", init_cap)
        _sc, _wc = _res['sell_count'], _res['win_count']
        st.dataframe(pd.DataFrame({
            "항목": ["시작 자본", "최종 자산", "총 수익률", "CAGR (연복리)",
                     "MDD", "Calmar Ratio", "총 매도 횟수", "승률",
                     "평균 손익률", "최대 단일 수익", "최대 단일 손실"],
            "수치": [
                f"${init_cap:,.0f}", f"${_fa:,.0f}",
                f"{_res['total_return']*100:+.2f}%", f"{_res['cagr']*100:.1f}%",
                f"{_res['mdd']*100:.1f}%", f"{_res['calmar']:.3f}",
                f"{_sc}회",
                f"{_wc/_sc*100:.1f}%  ({_wc}승 {_sc-_wc}패)" if _sc > 0 else "-",
                f"{_res['avg_pnl']:+.2f}%", f"{_res['max_pnl']:+.2f}%",
                f"{_res['min_pnl']:+.2f}%",
            ],
        }), hide_index=True, use_container_width=True)
        st.divider()

        st.subheader(f"🎯 {div}티어 완전 투자 분석")
        st.caption(f"분할 매수 {div}회가 모두 체결된 사이클 분석")
        with st.spinner("5티어 분석 중..."):
            _t5 = run_5tier_analysis(_pdf, s_date, e_date, a_b, a_s, sr, div, init_cap)
        if _t5:
            _df5 = pd.DataFrame(_t5)
            _tot, _wins = len(_df5), int((_df5["손익률"] > 0).sum())
            _avg_h, _max_h = _df5["보유일수"].mean(), int(_df5["보유일수"].max())
            _tc1, _tc2, _tc3, _tc4 = st.columns(4)
            _tc1.metric("발생 횟수",   f"{_tot}회")
            _tc2.metric("승률",        f"{_wins/_tot*100:.1f}%  ({_wins}승 {_tot-_wins}패)")
            _tc3.metric("평균 보유기간", f"{_avg_h:.1f}일")
            _tc4.metric("최장 보유기간", f"{_max_h}일")
            st.info(
                f"**'{div}티어 완전 매수 후 무한 보유' 걱정은 거의 불필요합니다.**\n\n"
                f"최장 보유일은 **{_max_h}일**에 불과합니다. "
                f"매도 조건이 '직전 2일 평균 대비 +{a_s*100:.1f}%'이기 때문에 "
                f"주가가 조금만 반등해도 바로 매도가 트리거됩니다.\n\n"
                f"전체 {_tot}회 중 **{_wins}회 수익({_wins/_tot*100:.0f}%)** 으로 마감했습니다."
            )
            def _style_t5(row):
                return ["color: #2e7d32; font-weight:bold" if row["손익률"] > 0
                        else "color: #c62828; font-weight:bold" if row["손익률"] < 0
                        else "" for _ in row]
            st.markdown(f"**TOP 10 — {div}번째 티어 체결 후 가장 긴 보유 기간**")
            _top10 = _df5.nlargest(10, "보유일수").reset_index(drop=True)
            _top10.index += 1
            st.dataframe(_top10.style.apply(_style_t5, axis=1)
                                      .format({"5번째 매수가": "${:.2f}", "평균단가": "${:.2f}",
                                               "매도가": "${:.2f}", "손익률": "{:+.2f}%"}),
                         hide_index=False, use_container_width=True)
            _fig_h = px.histogram(_df5, x="보유일수", nbins=20,
                                   title=f"{div}티어 완전 투자 후 보유기간 분포",
                                   labels={"보유일수": "보유기간 (일)", "count": "횟수"},
                                   color_discrete_sequence=["#5C6BC0"])
            _fig_h.update_layout(height=320, bargap=0.1)
            st.plotly_chart(_fig_h, use_container_width=True)
            with st.expander(f"📋 전체 {_tot}회 상세 내역 보기"):
                st.dataframe(_df5.style.apply(_style_t5, axis=1)
                                        .format({"5번째 매수가": "${:.2f}", "평균단가": "${:.2f}",
                                                 "매도가": "${:.2f}", "손익률": "{:+.2f}%"}),
                             hide_index=True, use_container_width=True,
                             height=min(38 + 35 * len(_df5), 600))
        else:
            st.info(f"선택 기간 내 {div}티어 완전 투자 이벤트가 없습니다.")
        st.divider()

        st.subheader("💡 전략 인사이트 & 맥락 참고")
        st.warning(f"**다음 내용은 {tk} 백테스트 결과 해석입니다. 과거 성과가 미래 수익을 보장하지 않습니다.**")
        with st.container(border=True):
            st.markdown("""
**왜 이 전략이 변동성 높은 종목에서 잘 작동하나?**
- **장기 우상향 종목**일수록 백테스트 수치가 유리하게 나옵니다
- 단순 Buy & Hold 대비 **변동성을 활용**하여 추가 수익을 창출하는 구조입니다
- LOC 주문으로 **장 마감 기준가 확인 → 당일 체결**하여 신호 딜레이가 없습니다

**주요 지표 해석**
- **Calmar 1.0 이상**: 우수 / **2.0 이상**: 최상급
- **MDD**: 레버리지 ETF는 MDD가 크게 나올 수 있으므로 감내 가능한 수준인지 확인
- **승률**: 높은 승률도 손익비(평균 수익 vs 평균 손실)와 함께 고려 필요

**주의사항**
- 분할수(N)티어 모두 체결되면 **현금이 거의 소진**되므로 추가 하락 시 매수 불가
- 급락장(코로나, 금리 충격 등)에서는 **MDD가 일시적으로 크게 확대**될 수 있음
- 실제 거래에서는 **슬리피지, 수수료, 세금** 등이 수익률에 영향
- 전략 파라미터를 너무 자주 바꾸면 과최적화(overfitting) 위험
            """)

    # ── 분석 실행: 등록된 ticker 전체 ─────────────────────────
    _perf_tk_settings = _get_ticker_settings()
    if _perf_tk_settings:
        st.caption(f"등록된 계좌 기준으로 분석합니다: **{', '.join(_perf_tk_settings.keys())}**  "
                   f"(기간·초기자본은 사이드바 설정 사용)")
    else:
        st.caption("사이드바의 공통 설정(티커 · 파라미터 · 기간 · 초기 자본)을 기준으로 분석합니다.")

    if st.button("▶ 성과 분석 실행", type="primary", key="run_perf"):
        if _perf_tk_settings:
            # 등록된 ticker가 있으면 → 전체 순서대로 분석
            _tk_list = list(_perf_tk_settings.keys())
            if len(_tk_list) > 1:
                _perf_tabs = st.tabs([f"📊 {t}" for t in _tk_list])
                for _pi, _ptk in enumerate(_tk_list):
                    with _perf_tabs[_pi]:
                        _pcfg = _perf_tk_settings[_ptk]
                        _render_perf_analysis(
                            _ptk,
                            float(_pcfg.get("a_buy",     a_buy)),
                            float(_pcfg.get("a_sell",    a_sell)),
                            float(_pcfg.get("sell_ratio", sell_ratio)),
                            int  (_pcfg.get("divisions",  divisions)),
                            initial_capital, start_date, end_date,
                        )
            else:
                _ptk  = _tk_list[0]
                _pcfg = _perf_tk_settings[_ptk]
                _render_perf_analysis(
                    _ptk,
                    float(_pcfg.get("a_buy",      a_buy)),
                    float(_pcfg.get("a_sell",     a_sell)),
                    float(_pcfg.get("sell_ratio",  sell_ratio)),
                    int  (_pcfg.get("divisions",   divisions)),
                    initial_capital, start_date, end_date,
                )
        else:
            # 등록된 계좌 없으면 → 사이드바 ticker로 분석 (기존 동작)
            _render_perf_analysis(
                ticker, a_buy, a_sell, sell_ratio, divisions,
                initial_capital, start_date, end_date,
            )


# ══════════════════════════════════════════════
# TAB 5 – 개인 설정
# ══════════════════════════════════════════════
def _write_orders_to_sheet(gs_url: str, gs_sheet: str, res: dict,
                           _sell_ratio: float, _divisions: int, ticker_name: str):
    """시뮬레이션 결과를 구글시트 지정 탭 L4부터 기록."""
    gc = _get_gspread_client()
    sh = gc.open_by_url(gs_url)
    ws = sh.worksheet(gs_sheet)

    # L4:O 범위 초기화 (최대 10행)
    ws.batch_clear(["L4:O13"])

    rows = []
    # 매수 LOC
    buy_tgt = res["next_buy_primary"]
    buy_qty = res["pending_buys"][0]["수량"]
    rows.append(["매수", "LOC", round(buy_tgt, 2), buy_qty])

    # 매도 LOC (보유 시에만)
    if res["shares"] > 0:
        sell_qty = math.floor(res["shares"] * (_sell_ratio / 100.0))
        sell_tgt = res["next_sell_target"]
        rows.append(["매도", "LOC", round(sell_tgt, 2), sell_qty])

    # L4 = row 4, col 12 (L) → gspread update
    ws.update(range_name="L4", values=rows)
    return len(rows)


def _send_telegram(token: str, chat_id: str, text: str) -> dict:
    """텔레그램 Bot API로 메시지 전송. 결과 dict 반환."""
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        return resp.json()
    except Exception as e:
        return {"ok": False, "description": str(e)}


def _build_order_text(ticker_name: str, _a_buy: float, _a_sell: float,
                      _sell_ratio: float, _divisions: int,
                      _os_start, _os_capital: float) -> str:
    """Tab3와 동일한 시뮬레이션 엔진으로 오늘의 주문표를 텔레그램 텍스트로 변환."""
    try:
        today = datetime.today().date()
        price_df_tg = load_price_data(ticker_name, _os_start, today, "Yahoo Finance", None)
        if price_df_tg.empty:
            return "❌ 가격 데이터를 불러오지 못했습니다."

        res = run_portfolio_for_ordersheet(
            price_df_tg, _os_start, ticker_name,
            _a_buy, _a_sell, _sell_ratio, _divisions, _os_capital,
        )
        if res is None:
            return "❌ 시뮬레이션 데이터가 없습니다."

        lp   = res["latest_price"]
        p1   = res["p1_now"]
        p2   = res["p2_now"]
        today_str = today.strftime("%Y-%m-%d")

        lines = [
            f"📋 <b>오늘의 주문표</b> ({today_str})",
            f"전략: 종가평균매매",
            f"종목: {ticker_name}",
            f"직전 종가(p1): ${p1:,.2f}  |  전전 종가(p2): ${p2:,.2f}",
        ]

        # 매수
        buy_tgt = res["next_buy_primary"]
        buy_qty = res["pending_buys"][0]["수량"]
        lines.append(f"🔴 매수 LOC {buy_qty:,}주  ${buy_tgt:,.2f}")

        # 매도 (보유 시에만)
        if res["shares"] > 0:
            sell_qty = math.floor(res["shares"] * (_sell_ratio / 100.0))
            sell_tgt = res["next_sell_target"]
            lines.append(f"🔵 매도 LOC {sell_qty:,}주  ${sell_tgt:,.2f}")

        # 보유 현황 요약
        if res["shares"] > 0:
            pnl = (lp / res["avg_cost"] - 1) * 100 if res["avg_cost"] > 0 else 0
            lines += [
                f"📦 보유: {res['shares']:,}주  |  평단: ${res['avg_cost']:.2f}",
                f"   현재가: ${lp:.2f}  ({pnl:+.2f}%)  |  현금: ${res['cash']:,.2f}",
            ]
        else:
            lines.append(f"📦 보유주식 없음  |  현금: ${res['cash']:,.2f}")

        lines.append("※ 종가 LOC 주문 기준입니다.")
        return "\n".join(lines)

    except Exception as e:
        return f"주문표 생성 오류: {e}"


with tab5:
    st.subheader("⚙️ 개인 설정")

    _cfg5 = load_config()
    # 클라우드 로그인 시 Google Sheets에서 사용자 설정 가져오기
    _usercfg = st.session_state.get("user_settings", {}) if _IS_CLOUD else {}

    if _IS_CLOUD:
        st.info(f"☁️ **{st.session_state.get('username','')}** 으로 로그인 중 — 설정을 저장하면 다음 로그인 시 자동으로 불러옵니다.")
    else:
        st.success(f"🖥️ **로컬 PC 실행 중** — 설정이 `{_CONFIG}` 에 저장됩니다.")

    # ── 텔레그램 알림 설정 ─────────────────────────────────
    with st.container(border=True):
        col_title, col_help = st.columns([3, 1])
        with col_title:
            st.markdown("#### 💬 텔레그램 알림 설정")
            st.caption("포트폴리오 알림 및 주문 신호를 텔레그램으로 받을 수 있습니다.")
        with col_help:
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
.tg-help-box li { margin-bottom: 2px; }
.tg-tag {
    background: #D0E8FF; color: #1a5fa8;
    border-radius: 5px; padding: 1px 7px;
    font-family: monospace; font-size: 13px;
}
.tg-example-box {
    background: white; border: 1px solid #CBD5E1; border-radius: 8px;
    padding: 12px 16px; margin-top: 10px; font-size: 13px;
    color: #555;
}
.tg-example-val { color: #4A90D9; font-family: monospace; font-size: 13px; }
.tg-warn-box {
    background: #FFFBEB; border: 1px solid #F59E0B;
    border-radius: 10px; padding: 14px 18px; font-size: 14px; line-height: 2;
}
.tg-warn-title { font-weight: 700; color: #92400E; margin-bottom: 4px; }
.tg-sub-title { font-weight: 700; margin-bottom: 4px; }
</style>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">1</span> Bot Token 생성하기</div>
  <div class="tg-help-box">
    <ol>
      <li>텔레그램에서 <span class="tg-tag">@BotFather</span> 를 검색합니다.</li>
      <li>BotFather와 대화를 시작하고 <span class="tg-tag">/newbot</span> 을 입력합니다.</li>
      <li>봇의 이름을 입력합니다. (예: 3일평균 알림봇)</li>
      <li>봇의 username을 입력합니다. (반드시 <span class="tg-tag">_bot</span> 으로 끝나야 합니다. 예: 3days-avg_bot)</li>
      <li>성공하면 HTTP API Token이 발급됩니다. 이것이 <strong>Bot Token</strong>입니다.</li>
    </ol>
    <div class="tg-example-box">
      <div style="color:#888; font-size:12px; margin-bottom:4px;">Bot Token 예시:</div>
      <div class="tg-example-val">1234567890:ABCdefGHIjklMNOpqrSTUvwxYZ</div>
    </div>
  </div>
</div>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">2</span> Chat ID 확인하기</div>
  <div class="tg-help-box">
    <div class="tg-sub-title">방법 1: @userinfobot 사용</div>
    <ol>
      <li>텔레그램에서 <span class="tg-tag">@userinfobot</span> 을 검색합니다.</li>
      <li>대화를 시작하면 자동으로 Chat ID가 표시됩니다.</li>
    </ol>
    <div class="tg-sub-title" style="margin-top:10px;">방법 2: @RawDataBot 사용</div>
    <ol>
      <li>텔레그램에서 <span class="tg-tag">@RawDataBot</span> 을 검색합니다.</li>
      <li>대화를 시작하면 JSON 형식으로 정보가 표시되며, "id" 값이 Chat ID입니다.</li>
    </ol>
    <div class="tg-example-box">
      <div style="color:#888; font-size:12px; margin-bottom:4px;">Chat ID 예시:</div>
      <div class="tg-example-val">123456789</div>
    </div>
  </div>
</div>

<div class="tg-help-section">
  <div class="tg-help-title"><span class="tg-help-badge">3</span> 내 봇 시작하기 (중요!)</div>
  <div class="tg-warn-box">
    <div class="tg-warn-title">⚠ 반드시 내가 만든 봇을 시작해야 메시지를 받을 수 있습니다!</div>
    <ol>
      <li>텔레그램에서 내가 만든 봇을 검색합니다. (예: <span class="tg-tag">@3days-avg_bot</span>)</li>
      <li><span class="tg-tag">/start</span> 를 입력하여 봇을 시작합니다.</li>
      <li>이제 알림을 받을 준비가 완료되었습니다!</li>
    </ol>
  </div>
</div>
""", unsafe_allow_html=True)

        c1, c2 = st.columns(2)
        # 로컬이면 저장된 값 불러오기, 클라우드면 빈칸
        tg_chat_id = c1.text_input(
            "텔레그램 Chat ID",
            value=_cfg5.get("tg_chat_id", "") if not _IS_CLOUD else _usercfg.get("tg_chat_id", ""),
            placeholder="예: 1234567890",
            key="tg_chat_id_input",
        )
        tg_token = c2.text_input(
            "Bot Token",
            value=_cfg5.get("tg_token", "") if not _IS_CLOUD else _usercfg.get("tg_token", ""),
            placeholder="예: 123456789:AAF...",
            type="password",
            key="tg_token_input",
        )

        st.caption("📅 주문표는 매주 월~금 오후 3:00 (KST)에 텔레그램으로 자동 발송됩니다")

        btn_col1, btn_col2, spacer = st.columns([1, 1, 4])
        with btn_col1:
            if st.button("📨 주문표 테스트 발송", use_container_width=True, key="tg_test"):
                if not tg_chat_id or not tg_token:
                    st.warning("Chat ID와 Bot Token을 먼저 입력해주세요.")
                else:
                    _tg_all_settings = _get_ticker_settings()
                    if not _tg_all_settings:
                        st.warning("⚠️ 등록된 계좌가 없습니다. Tab3에서 계좌를 먼저 등록해주세요.")
                    else:
                        _tg_all_ok = True
                        for _tg_tk, _tg_cfg in _tg_all_settings.items():
                            with st.spinner(f"{_tg_tk} 시뮬레이션 & 발송 중..."):
                                try:
                                    _tg_start_d = datetime.strptime(
                                        _tg_cfg.get("os_start", "2024-01-01"), "%Y-%m-%d").date()
                                except:
                                    _tg_start_d = datetime(2024, 1, 1).date()
                                msg = _build_order_text(
                                    _tg_tk,
                                    float(_tg_cfg.get("a_buy",      -0.005)),
                                    float(_tg_cfg.get("a_sell",      0.009)),
                                    float(_tg_cfg.get("sell_ratio",  100.0)),
                                    int  (_tg_cfg.get("divisions",   5)),
                                    _tg_start_d,
                                    float(_tg_cfg.get("os_capital",  initial_capital)),
                                )
                                result = _send_telegram(tg_token, tg_chat_id, msg)
                            if result.get("ok"):
                                st.success(f"✅ {_tg_tk} 발송 성공!")
                            else:
                                _tg_all_ok = False
                                st.error(f"❌ {_tg_tk} 발송 실패: {result.get('description', '알 수 없는 오류')}")
        with btn_col2:
            if st.button("💾 저장하기", use_container_width=True, key="tg_save", type="primary"):
                if not tg_chat_id or not tg_token:
                    st.warning("Chat ID와 Bot Token을 모두 입력해주세요.")
                elif _IS_CLOUD:
                    with st.spinner("저장 중..."):
                        try:
                            _save_user_settings_to_sheet(
                                st.session_state.username,
                                {"tg_chat_id": tg_chat_id, "tg_token": tg_token})
                            st.session_state.user_settings.update(
                                {"tg_chat_id": tg_chat_id, "tg_token": tg_token})
                            st.success("✅ Google Sheets에 저장 완료!")
                        except Exception as e:
                            st.error(f"❌ 저장 실패: {e}")
                else:
                    save_config({"tg_chat_id": tg_chat_id, "tg_token": tg_token}, sensitive=True)
                    st.success(f"✅ 저장 완료! `{_CONFIG}`")

    st.write("")

    # ── 구글 스프레드시트 연동 ──────────────────────────────
    with st.container(border=True):
        col_title2, col_help2 = st.columns([3, 1])
        with col_title2:
            st.markdown("#### 🗂️ 구글 스프레드시트 연동")
            st.caption("포트폴리오 정보와 주문 신호를 구글 스프레드시트로 전송합니다.")
        with col_help2:
            with st.popover("❓ 구글 스프레드시트 URL 확인 & 권한 부여", use_container_width=True):
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
.gs-help-box li { margin-bottom: 2px; }
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
.gs-security-box {
    background: #F1F5F9; border-radius: 10px;
    padding: 14px 18px; font-size: 13px; color: #475569; line-height: 1.7;
    margin-bottom: 10px;
}
</style>

<div class="gs-help-section">
  <div class="gs-help-title"><span class="gs-help-badge">1</span> 새 스프레드시트 만들기</div>
  <div class="gs-help-box">
    <ol>
      <li><a href="https://sheets.google.com" target="_blank">Google Sheets</a>에 접속합니다.</li>
      <li><span class="gs-tag">+ 새로 만들기</span> 또는 <span class="gs-tag">빈 스프레드시트</span> 를 클릭합니다.</li>
      <li>스프레드시트 이름을 지정합니다. (예: 3일평균 포트폴리오)</li>
    </ol>
  </div>
</div>

<div class="gs-help-section">
  <div class="gs-help-title"><span class="gs-help-badge">2</span> 스프레드시트 URL 확인하기</div>
  <div class="gs-help-box">
    <div>브라우저 주소창에 표시된 URL을 복사합니다.</div>
    <div class="gs-example-box">
      <div style="color:#888; font-size:12px; margin-bottom:4px;">URL 형식:</div>
      <div class="gs-example-val">https://docs.google.com/spreadsheets/d/1ABC...XYZ/edit</div>
    </div>
    <div style="font-size:13px; color:#64748B; margin-top:8px;">
      * 전체 URL을 복사하면 됩니다. 뒤에 <span class="gs-tag">/edit</span> 가 있어도 괜찮습니다.
    </div>
  </div>
</div>

<div class="gs-help-section">
  <div class="gs-help-title"><span class="gs-help-badge">3</span> 서비스 계정에 편집 권한 부여 (중요!)</div>
  <div class="gs-warn-box">
    <div class="gs-warn-title">⚠ 앱이 스프레드시트에 데이터를 기록하려면 아래 이메일에 편집 권한을 부여해야 합니다.</div>
    <div class="gs-email-box">
      <div style="color:#888; font-size:12px; margin-bottom:4px;">서비스 계정 이메일:</div>
      <div class="gs-email-val">connectspreadsheet@sodium-gateway-485307-f3.iam.gserviceaccount.com</div>
    </div>
    <ol>
      <li>스프레드시트 우측 상단의 <span class="gs-tag">공유</span> 버튼을 클릭합니다.</li>
      <li>"사용자 및 그룹 추가" 입력란에 위 서비스 계정 이메일을 붙여넣습니다.</li>
      <li>권한을 <span class="gs-tag">편집자</span> 로 설정합니다.</li>
      <li><span class="gs-tag">보내기</span> 를 클릭합니다.</li>
    </ol>
  </div>
</div>

<div class="gs-security-box">
  <strong>보안 참고사항:</strong> 서비스 계정은 이 앱 전용 계정으로, 공유된 스프레드시트에만 접근할 수 있습니다.
  스프레드시트를 "링크가 있는 모든 사용자"로 공개할 필요 없이, 서비스 계정에만 권한을 부여하면 됩니다.
</div>
""", unsafe_allow_html=True)

        uc1, uc2 = st.columns([3, 1])
        gs_url = uc1.text_input(
            "스프레드시트 URL",
            value=_cfg5.get("gs_url", "") if not _IS_CLOUD else _usercfg.get("gs_url", ""),
            placeholder="https://docs.google.com/spreadsheets/d/...",
            key="gs_url_input",
        )
        gs_sheet = uc2.text_input(
            "시트 이름",
            value=_cfg5.get("gs_sheet", "종가평균") if not _IS_CLOUD else _usercfg.get("gs_sheet", "종가평균"),
            placeholder="종가평균",
            key="gs_sheet_input",
        )
        st.caption("* 스프레드시트에 서비스 계정 이메일을 편집자로 공유해주세요. (우측 상단 도움말 참고)")

        btn_col3, btn_col4, btn_col5 = st.columns(3)
        with btn_col3:
            if st.button("🔗 시트 연결 테스트", use_container_width=True, key="gs_test"):
                if not gs_url:
                    st.warning("스프레드시트 URL을 먼저 입력해주세요.")
                else:
                    try:
                        gc = _get_gspread_client()
                        sh = gc.open_by_url(gs_url)
                        st.success(f"✅ 연결 성공! 시트명: **{sh.title}**")
                    except Exception as e:
                        st.error(f"❌ 연결 실패: {e}")

        with btn_col4:
            if st.button("📊 주문 시트 전송", use_container_width=True, key="gs_send", type="primary"):
                if not gs_url:
                    st.warning("스프레드시트 URL을 먼저 입력해주세요.")
                else:
                    with st.spinner("시뮬레이션 & 시트 전송 중..."):
                        try:
                            _cfg_gs  = load_config()
                            _gs_start = _cfg_gs.get("os_start", "2024-01-01")
                            _gs_cap   = float(_cfg_gs.get("os_capital", initial_capital))
                            try:    _gs_start_d = datetime.strptime(_gs_start, "%Y-%m-%d").date()
                            except: _gs_start_d = datetime(2024, 1, 1).date()
                            _today = datetime.today().date()
                            _pdf = load_price_data(ticker, _gs_start_d, _today, "Yahoo Finance", None)
                            _res = run_portfolio_for_ordersheet(
                                _pdf, _gs_start_d, ticker,
                                a_buy, a_sell, sell_ratio, divisions, _gs_cap,
                            )
                            if _res is None:
                                st.error("시뮬레이션 데이터가 없습니다.")
                            else:
                                n = _write_orders_to_sheet(gs_url, gs_sheet, _res, sell_ratio, divisions, ticker)
                                st.success(f"✅ 구글시트 '{gs_sheet}' 탭 L4에 {n}건 전송 완료!")
                        except Exception as e:
                            st.error(f"❌ 전송 실패: {e}")

        with btn_col5:
            if st.button("💾 저장하기 ", use_container_width=True, key="gs_save", type="primary"):
                if not gs_url:
                    st.warning("스프레드시트 URL을 입력해주세요.")
                elif _IS_CLOUD:
                    with st.spinner("저장 중..."):
                        try:
                            _save_user_settings_to_sheet(
                                st.session_state.username,
                                {"gs_url": gs_url, "gs_sheet": gs_sheet})
                            st.session_state.user_settings.update(
                                {"gs_url": gs_url, "gs_sheet": gs_sheet})
                            st.success("✅ Google Sheets에 저장 완료!")
                        except Exception as e:
                            st.error(f"❌ 저장 실패: {e}")
                else:
                    save_config({"gs_url": gs_url, "gs_sheet": gs_sheet}, sensitive=True)
                    st.success(f"✅ 저장 완료! `{_CONFIG}`")

    # ── 관리자 도구: 비밀번호 해시 생성 ───────────────────────
    st.write("")
    with st.expander("🔧 관리자 도구 — 비밀번호 해시 생성 (users 시트 등록용)"):
        st.caption("새 사용자를 추가할 때 비밀번호를 bcrypt 해시로 변환하여 Google Sheets에 붙여넣으세요.")
        _admin_pw_input = st.text_input("등록할 비밀번호 입력", type="password", key="admin_pw_input")
        if st.button("🔑 해시 생성", key="gen_hash"):
            if _admin_pw_input:
                _hashed = _hash_password(_admin_pw_input)
                st.code(_hashed, language=None)
                st.caption("👆 위 해시를 복사해서 users 시트의 password_hash 컬럼에 붙여넣으세요.")
            else:
                st.warning("비밀번호를 입력해주세요.")
