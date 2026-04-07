import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import math
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
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

_SENSITIVE_KEYS    = {"tg_chat_id", "tg_token", "gs_url", "gs_sheet",
                      "sd_tg_chat_id", "sd_tg_token"}   # 표준편차 전용 텔레그램 키
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

# ── ticker별 실제 매매 히스토리 (B방식: 누적 파일 기반) ─────────────
def _get_ticker_history_file(tk: str) -> Path:
    if _IS_CLOUD:
        return Path(__file__).parent / f"history_{tk}.csv"
    return Path.home() / ".usd-avg" / f"history_{tk}.csv"

def _load_ticker_daily_history(tk: str) -> pd.DataFrame:
    """ticker별 누적 매매 히스토리 로드. Cloud: GSheets 우선, 로컬: CSV."""
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            import gspread as _gs
            gs_url = st.session_state.get("user_settings", {}).get("gs_url", "")
            if gs_url:
                client = _get_gspread_client()
                sh = client.open_by_url(gs_url)
                ws_name = f"{tk}_매매기록"
                try:
                    ws = sh.worksheet(ws_name)
                    records = ws.get_all_records()
                    return pd.DataFrame(records) if records else pd.DataFrame()
                except _gs.WorksheetNotFound:
                    return pd.DataFrame()
        except Exception:
            pass
    f = _get_ticker_history_file(tk)
    if f.exists():
        try:
            return pd.read_csv(f, encoding="utf-8-sig")
        except Exception:
            pass
    return pd.DataFrame()

def _save_ticker_daily_history(tk: str, daily_log: list):
    """시뮬레이션 결과 중 기존 히스토리에 없는 날짜만 누적 저장.
    파라미터를 바꿔도 과거 기록은 절대 변경되지 않음."""
    if not daily_log:
        return
    df_new = pd.DataFrame(daily_log)
    df_existing = _load_ticker_daily_history(tk)

    # 이미 기록된 날짜 제외
    if not df_existing.empty and "날짜" in df_existing.columns:
        existing_dates = set(df_existing["날짜"].astype(str))
        df_add = df_new[~df_new["날짜"].astype(str).isin(existing_dates)].copy()
    else:
        df_add = df_new.copy()

    if df_add.empty:
        return

    # 로컬 CSV 저장
    f = _get_ticker_history_file(tk)
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
                ws_name = f"{tk}_매매기록"
                try:
                    ws = sh.worksheet(ws_name)
                except _gs.WorksheetNotFound:
                    ws = sh.add_worksheet(title=ws_name, rows=5000, cols=25)
                    ws.append_row(df_add.columns.tolist())  # 헤더 추가
                # 새 행 일괄 추가
                rows_to_add = [[str(v) for v in row] for row in df_add.values.tolist()]
                if rows_to_add:
                    ws.append_rows(rows_to_add, value_input_option="RAW")
        except Exception:
            pass

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
def _parse_ticker_settings_json(raw) -> dict:
    """ticker_settings JSON 문자열을 안전하게 파싱. 빈값·파싱오류 모두 {} 반환."""
    if not raw or raw == "":
        return {}
    try:
        ts = json.loads(raw) if isinstance(raw, str) else raw
        return ts if isinstance(ts, dict) else {}
    except Exception:
        return {}

def _get_ticker_settings() -> dict:
    """종가평균매매 계좌 설정 반환 {ticker: {a_buy, a_sell, ...}}
    sd_ 접두어 키(표준편차매매 계좌)는 로컬/클라우드 모두 제외 — 공유 config 오염 방지.
    (과거 버그로 ticker_settings에 sd_SOXL 등이 잘못 저장된 경우도 걸러냄)"""
    if _IS_CLOUD and st.session_state.get("logged_in"):
        raw = st.session_state.get("user_settings", {}).get("ticker_settings", "") or ""
        ts  = _parse_ticker_settings_json(raw)
        clean = {k: v for k, v in ts.items() if not k.startswith("sd_")}
        # 오염 데이터가 있으면 GSheets도 조용히 정리
        if len(clean) != len(ts):
            try:
                ts_json = json.dumps(clean, ensure_ascii=False)
                st.session_state.user_settings["ticker_settings"] = ts_json
                _save_user_settings_to_sheet(st.session_state.username, {"ticker_settings": ts_json})
            except Exception:
                pass
        return clean
    else:
        full_cfg = _load_full_config()
        return {k: v for k, v in full_cfg.items()
                if k not in _GLOBAL_CONFIG_KEYS
                and isinstance(v, dict)
                and not k.startswith("sd_")}

def _save_ticker_setting(tk: str, data: dict) -> str:
    """ticker별 설정 저장 (로컬 config.json + 클라우드 Google Sheets 동기).
    성공 시 '' 반환, 실패 시 오류 메시지 반환."""
    save_config(data, tk)  # 로컬
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            raw = st.session_state.get("user_settings", {}).get("ticker_settings", "") or ""
            ts  = _parse_ticker_settings_json(raw)
            ts[tk] = {**ts.get(tk, {}), **data}
            ts_json = json.dumps(ts, ensure_ascii=False)
            # session_state 먼저 업데이트 (GSheets 실패해도 화면엔 반영)
            if "user_settings" not in st.session_state:
                st.session_state.user_settings = {}
            st.session_state.user_settings["ticker_settings"] = ts_json
            # GSheets 저장
            _save_user_settings_to_sheet(st.session_state.username, {"ticker_settings": ts_json})
            return ""
        except Exception as e:
            return f"저장 중 오류: {e}"
    return ""

def _delete_ticker_history(tk: str):
    """ticker 매매 히스토리 삭제 (로컬 CSV + 클라우드 GSheets 워크시트)."""
    # 로컬 CSV 삭제
    f = _get_ticker_history_file(tk)
    try:
        if f.exists():
            f.unlink()
    except Exception:
        pass
    # Cloud: GSheets 워크시트 삭제
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            import gspread as _gs
            gs_url = st.session_state.get("user_settings", {}).get("gs_url", "")
            if gs_url:
                client = _get_gspread_client()
                sh = client.open_by_url(gs_url)
                ws_name = f"{tk}_매매기록"
                try:
                    ws = sh.worksheet(ws_name)
                    sh.del_worksheet(ws)
                except _gs.WorksheetNotFound:
                    pass
        except Exception:
            pass

def _delete_ticker_setting(tk: str) -> str:
    """ticker 설정 + 매매 히스토리 삭제 (로컬 + 클라우드). 성공 시 '' 반환, 실패 시 오류 메시지 반환."""
    full_cfg = _load_full_config()
    full_cfg.pop(tk, None)
    try:
        _CONFIG.write_text(json.dumps(full_cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    except:
        pass
    # 매매 히스토리도 함께 삭제
    _delete_ticker_history(tk)
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            raw = st.session_state.get("user_settings", {}).get("ticker_settings", "") or ""
            ts  = _parse_ticker_settings_json(raw)
            ts.pop(tk, None)
            ts_json = json.dumps(ts, ensure_ascii=False)
            if "user_settings" not in st.session_state:
                st.session_state.user_settings = {}
            st.session_state.user_settings["ticker_settings"] = ts_json
            _save_user_settings_to_sheet(st.session_state.username, {"ticker_settings": ts_json})
            return ""
        except Exception as e:
            return f"삭제 중 오류: {e}"
    return ""

# ══════════════════════════════════════════════
# 표준편차매매 계좌 설정 CRUD  (키 접두사 "sd_")
# ══════════════════════════════════════════════
_SD_PFX = "sd_"   # 종가평균 설정과 구분하는 접두사

def _get_sd_ticker_settings() -> dict:
    """등록된 표준편차매매 계좌 설정 반환 {ticker: {k_buy, k_sell, …}}"""
    if _IS_CLOUD and st.session_state.get("logged_in"):
        raw = st.session_state.get("user_settings", {}).get("sd_ticker_settings", "") or ""
        try:    return json.loads(raw) if raw else {}
        except: return {}
    full_cfg = _load_full_config()
    return {k[len(_SD_PFX):]: v
            for k, v in full_cfg.items()
            if k.startswith(_SD_PFX) and isinstance(v, dict)}

def _save_sd_ticker_setting(tk: str, data: dict) -> str:
    """표준편차 ticker 설정 저장. 성공 시 '' 반환, 실패 시 오류 메시지.
    로컬: config.json에 'sd_{tk}' 키로 저장.
    Cloud: user_settings['sd_ticker_settings']에 저장 (_get_sd_ticker_settings와 동일 키)."""
    save_config(data, f"{_SD_PFX}{tk}")  # 로컬 저장
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            raw = st.session_state.get("user_settings", {}).get("sd_ticker_settings", "") or ""
            ts  = _parse_ticker_settings_json(raw)
            ts[tk] = {**ts.get(tk, {}), **data}
            ts_json = json.dumps(ts, ensure_ascii=False)
            if "user_settings" not in st.session_state:
                st.session_state.user_settings = {}
            st.session_state.user_settings["sd_ticker_settings"] = ts_json
            _save_user_settings_to_sheet(st.session_state.username, {"sd_ticker_settings": ts_json})
            return ""
        except Exception as e:
            return f"저장 중 오류: {e}"
    return ""

def _delete_sd_ticker_setting(tk: str) -> str:
    """표준편차 ticker 설정 + 매매 히스토리 삭제.
    로컬: config.json 'sd_{tk}' 키 제거 + sd_history_{tk}.csv 삭제.
    Cloud: sd_ticker_settings 제거 + GSheets 'sd_{tk}_매매기록' 워크시트 삭제."""
    # 로컬 config 제거
    full_cfg = _load_full_config()
    full_cfg.pop(f"{_SD_PFX}{tk}", None)
    try:
        _CONFIG.write_text(json.dumps(full_cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    except:
        pass
    # 로컬 히스토리 CSV 삭제
    f = _get_sd_history_file(tk)
    try:
        if f.exists():
            f.unlink()
    except Exception:
        pass
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            # 설정 제거
            raw = st.session_state.get("user_settings", {}).get("sd_ticker_settings", "") or ""
            ts  = _parse_ticker_settings_json(raw)
            ts.pop(tk, None)
            ts_json = json.dumps(ts, ensure_ascii=False)
            if "user_settings" not in st.session_state:
                st.session_state.user_settings = {}
            st.session_state.user_settings["sd_ticker_settings"] = ts_json
            _save_user_settings_to_sheet(st.session_state.username, {"sd_ticker_settings": ts_json})
            # GSheets 매매기록 워크시트 삭제
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
                    except _gs.WorksheetNotFound:
                        pass
            except Exception:
                pass
            return ""
        except Exception as e:
            return f"삭제 중 오류: {e}"
    return ""

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
st.set_page_config(page_title="📊 전략 백테스터", layout="wide")

# ── 클라우드: 로그인 게이트 ────────────────────────────────────
if _IS_CLOUD:
    # ── 쿠키 컴포넌트 초기화 대기 ──────────────────────────────
    # streamlit-cookies-controller 는 React 컴포넌트라서
    # 첫 렌더링에서 get()이 항상 None을 반환함.
    # JS가 쿠키값을 Python에 전달하면 자동 rerun이 발생하므로,
    # _cookie_ready 플래그로 "로딩 중 None" vs "진짜 없음 None" 을 구분.
    if not st.session_state.get("_cookie_ready", False):
        st.session_state["_cookie_ready"] = True
        st.stop()   # 컴포넌트 로딩 완료 후 자동 rerun 대기

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
                            "tg_chat_id",    "tg_token",        # 종가평균매매 텔레그램
                            "sd_tg_chat_id", "sd_tg_token",     # 표준편차매매 텔레그램
                            "gs_url", "gs_sheet",
                            "a_buy", "a_sell", "sell_ratio", "divisions",
                            "ticker_settings", "sd_ticker_settings",
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
                            # expires=30 : js-cookie 형식 (정수 = 일 수)
                            # datetime 객체를 넘기면 JSON 직렬화 실패 → 세션쿠키로 저장됨
                            try:
                                _cookie_mgr.set(
                                    "usd_avg_user", _u,
                                    expires=30,
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

_is_stdev_title = (st.session_state.get("strategy_radio", "📐 표준편차매매") == "📐 표준편차매매")
st.title("📐 표준편차매매" if _is_stdev_title else "📈 종가평균매매")

# ──────────────────────────────────────────────
# 사이드바 공통 설정
# ──────────────────────────────────────────────
with st.sidebar:
    st.header("📊 전략 백테스터")

    # ── 전략 선택 (드롭다운) ────────────────────
    _STRATEGIES = ["📐 표준편차매매", "📈 종가평균매매"]
    st.selectbox("전략 선택", _STRATEGIES, key="strategy_radio")
    st.markdown("---")

    st.subheader("📌 종목")
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
        _all_tk_cfg = _get_ticker_settings()
        _usercfg_sb = _all_tk_cfg.get(ticker, st.session_state.get("user_settings", {}))
    else:
        _usercfg_sb = _cfg_sb

    # ── 전략별 파라미터 UI ──────────────────────
    _is_stdev = (st.session_state.get("strategy_radio", "📐 표준편차매매") == "📐 표준편차매매")

    if _is_stdev:
        # 표준편차매매 파라미터
        # 안정형 표준 파라미터: k_buy=0.65, k_sell=0.45, sell_ratio=85%, divisions=5, σ=2일
        # (MDD -30% 이내 최고 Calmar 기준 최적화 결과)
        sd_sigma_period = st.number_input("σ 계산 기간 (일)", value=_sint(_usercfg_sb.get("sd_sigma_period"), 2),
                                          min_value=1, max_value=20, step=1)
        sd_k_buy  = st.number_input("k_buy  (매수 계수)", value=_sfloat(_usercfg_sb.get("sd_k_buy"),  0.65),
                                    min_value=0.0, max_value=5.0, step=0.05, format="%.2f")
        sd_k_sell = st.number_input("k_sell (매도 계수)", value=_sfloat(_usercfg_sb.get("sd_k_sell"), 0.45),
                                    min_value=0.0, max_value=5.0, step=0.05, format="%.2f")
        sell_ratio = st.number_input("매도비율 (%)", value=_sfloat(_usercfg_sb.get("sd_sell_ratio"), 85.0),
                                     step=5.0, min_value=0.0, max_value=100.0)
        divisions  = st.number_input("분할수 (티어 수)", value=_sint(_usercfg_sb.get("sd_divisions"), 5),
                                     min_value=1, step=1)
        sd_renewal = st.number_input("갱신 주기 (RENEWAL)", value=_sint(_usercfg_sb.get("sd_renewal"), 5),
                                     min_value=1, step=1)
        # 종가평균 파라미터 더미값 (호환용)
        a_buy = a_sell = 0.0; n_days = 2
    else:
        # 종가평균매매 파라미터
        # 안정형 표준 파라미터: a_buy=-0.0063, a_sell=+0.0075, sell_ratio=100%, divisions=5, 3일
        _def_a_buy  = _sfloat(_usercfg_sb.get("a_buy"),      -0.0063)
        _def_a_sell = _sfloat(_usercfg_sb.get("a_sell"),       0.0075)
        _def_sr     = _sfloat(_usercfg_sb.get("sell_ratio"),  100.0)
        _def_div    = _sint  (_usercfg_sb.get("divisions"),   5)
        _def_ndays  = _sint  (_usercfg_sb.get("n_days"),      2)
        a_buy      = st.number_input("매수기준 (a값)", value=_def_a_buy,  step=0.001, format="%.4f")
        a_sell     = st.number_input("매도기준 (a값)", value=_def_a_sell, step=0.001, format="%.4f")
        sell_ratio = st.number_input("매도비율 (%)", value=_def_sr, step=10.0, min_value=0.0, max_value=100.0)
        divisions  = st.number_input("분할수", value=_def_div, min_value=1, step=1)
        _ndays_display = st.select_slider(
            "이동평균 일수",
            options=[f"{i}일" for i in range(3, 12)],
            value=f"{int(_def_ndays) + 1}일",
        )
        n_days = int(_ndays_display.replace("일", "")) - 1
        # 표준편차 파라미터 더미값 (호환용)
        sd_sigma_period = 2; sd_k_buy = 0.65; sd_k_sell = 0.45; sd_renewal = 5

    st.caption("📌 파라미터 저장은 오늘의 주문표 탭에서 할 수 있습니다.")

    st.markdown("---")
    st.subheader("백테스트 설정")
    col1, col2 = st.columns(2)
    start_date = col1.date_input("시작 일", datetime(2010, 3, 15).date() if _is_stdev else datetime(2014, 1, 1).date())
    end_date   = col2.date_input("종료 일", datetime.today().date())
    initial_capital = st.number_input("초기 투자금 ($)", value=20000.0 if _is_stdev else 10000.0, step=1000.0)
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


def calc_boundary(prices, a):
    """N일 종가 리스트 [p1,p2,...,pN]로 LOC 경계가 계산. N=2이면 기존 공식과 동일."""
    n = len(prices)
    return sum(prices) * (1 + a) / (n - a)


def _prep_sim(sim_raw, n_days, a_buy, a_sell):
    """N일 이동평균 기반 매수/매도 경계가 벡터 계산.
    Returns: (sim_df, closes_arr, tgt_buy_arr, tgt_sell_arr)"""
    for k in range(1, n_days + 1):
        sim_raw[f"p{k}"] = sim_raw["Close"].shift(k)
    sim = sim_raw.dropna(subset=[f"p{n_days}"])
    if sim.empty:
        return sim, np.array([]), np.array([]), np.array([])
    psum = sum(sim[f"p{k}"].values.astype(float) for k in range(1, n_days + 1))
    closes   = sim["Close"].values.astype(float)
    tgt_buy  = psum * (1 + a_buy)  / (n_days - a_buy)
    tgt_sell = psum * (1 + a_sell) / (n_days - a_sell)
    return sim, closes, tgt_buy, tgt_sell


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
    return_history=False, n_days=2,
):
    sim_raw = price_df.loc[pd.to_datetime(start_date):pd.to_datetime(end_date)].copy()
    sim, closes, tgt_buy, tgt_sell = _prep_sim(sim_raw, n_days, a_buy, a_sell)
    if sim.empty:
        return None

    cash       = float(initial_capital)
    shares     = 0
    avg_cost   = 0.0
    prev_asset = float(initial_capital)
    assets     = np.empty(len(closes))
    cash_arr   = np.empty(len(closes))
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
            # LOC 주문: 수량은 기준가(tb) 기준, 체결은 실제 종가(x)
            buy_qty = min(math.floor(current_chunk / tb + 1e-9), math.floor(cash / tb + 1e-9))
            if buy_qty > 0:
                avg_cost  = (avg_cost * shares + x * buy_qty) / (shares + buy_qty)
                action = "BUY"; trade_shares = buy_qty; trade_amount = buy_qty * x
                cash -= trade_amount; shares += buy_qty; buy_count += 1

        asset = cash + shares * x
        prev_asset = asset
        assets[i]  = asset
        cash_arr[i] = cash

        if return_history:
            history.append({
                "날짜": sim.index[i].date(), "종가(x)": x,
                "전날(p1)": float(sim["p1"].values[i]), "전전날(p2)": float(sim["p2"].values[i]) if "p2" in sim.columns else float("nan"),
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
        sell_pnls_list=sell_pnls,
        cash_series=cash_arr,
    )
    if return_history:
        out["history"] = pd.DataFrame(history)
    return out


def run_portfolio_for_ordersheet(
    price_df, start_date, ticker_name,
    a_buy, a_sell, sell_ratio, divisions, initial_capital,
    n_days=2,
):
    """백테스트를 오늘까지 실행하며 평균단가·티어·매도이력을 추적."""
    today = datetime.today().date()
    sim_raw = price_df.loc[pd.to_datetime(start_date):pd.to_datetime(today)].copy()
    all_closes = sim_raw["Close"].dropna().values.astype(float)
    # N일 계산에는 최소 N개 종가 필요
    if len(all_closes) < n_days:
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
    _hold_tday  = 0    # 거래일 기준 보유기간 카운터 (첫 매수일 = 0)

    if not sim_raw.empty:
        sim, closes, tgt_buy, tgt_sell = _prep_sim(sim_raw.copy(), n_days, a_buy, a_sell)
    else:
        sim    = sim_raw
        closes = np.array([])
        tgt_buy = tgt_sell = np.array([])

    for i in range(len(closes)):
        x    = closes[i]
        tb   = tgt_buy[i]
        ts   = tgt_sell[i]
        date = sim.index[i]
        current_chunk = prev_asset / divisions
        _day_action  = "-"
        _day_qty     = 0
        _day_amt     = 0.0
        _day_pnl_amt = None   # 실현손익($) — SELL 시에만 기록
        _day_pnl_pct = None   # 실현손익률(%) — SELL 시에만 기록

        if shares > 0 and x >= ts:
            sell_qty = math.floor(shares * (sell_ratio / 100.0))
            if sell_qty > 0:
                holding_days = _hold_tday  # 거래일 기준 보유기간
                factor       = x / avg_cost if avg_cost > 0 else 0.0
                _day_pnl_amt = round((x - avg_cost) * sell_qty, 2) if avg_cost > 0 else 0.0
                _day_pnl_pct = round((x / avg_cost - 1) * 100, 2)  if avg_cost > 0 else 0.0

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
            # LOC 주문: 수량은 기준가(tb) 기준, 체결은 실제 종가(x)
            buy_qty = min(
                math.floor(current_chunk / tb + 1e-9),
                math.floor(cash / tb + 1e-9),
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
        # 거래일 기준 보유기간: 매수 진입 시 0, 이후 거래일마다 +1, 전량 매도 시 리셋
        if shares > 0:
            _hdays = _hold_tday
            _hold_tday += 1  # 다음 거래일 카운터 증가
        else:
            _hdays = "-"
            _hold_tday = 0  # 포지션 없을 때 리셋 (다음 매수 시 0부터 시작)
        daily_log.append({
            "날짜":          str(_date_val2),
            "종가(x)":       round(x, 4),
            "전날(p1)":      round(float(sim["p1"].values[i]), 4),
            "전전날(p2)":    round(float(sim["p2"].values[i]), 4) if "p2" in sim.columns else float("nan"),
            "매수경계가":    round(tb, 4),
            "매도경계가":    round(ts, 4),
            "매매":          _day_action,
            "거래주수":      _day_qty,
            "거래금액($)":   _day_amt,
            "실현손익($)":   _day_pnl_amt,   # SELL 시 실현손익, 나머지는 None
            "실현손익률(%)": _day_pnl_pct,   # SELL 시 실현손익률, 나머지는 None
            "보유주수":      shares,
            "보유기간":      _hdays if shares > 0 else "-",
            "현금($)":       round(cash, 2),
            "총자산($)":     round(asset, 2),
        })

    latest_price  = float(all_closes[-1])
    current_asset = cash + shares * latest_price
    total_return  = (current_asset - initial_capital) / initial_capital
    current_dd    = (current_asset - peak_asset) / peak_asset  # <= 0
    stock_weight  = (shares * latest_price / current_asset) if current_asset > 0 else 0.0
    years         = (today - pd.to_datetime(start_date).date()).days / 365.25
    cagr          = ((current_asset / initial_capital) ** (1.0 / years) - 1.0) if years > 0 else 0.0

    # 오늘 LOC 기준: 가장 최근 N개 종가
    p_now = [float(all_closes[-(k)]) for k in range(1, n_days + 1)]
    p1_now = p_now[0]
    p2_now = p_now[1] if len(p_now) > 1 else p_now[0]
    next_buy_primary   = calc_boundary(p_now, a_buy)
    next_buy_secondary = next_buy_primary * 0.95
    next_sell_target   = calc_boundary(p_now, a_sell)

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


# ══════════════════════════════════════════════
# 표준편차매매 엔진
# ══════════════════════════════════════════════
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

    매수: close ≤ prev_close×(1+σ×k_buy)  → floor(min(daily_invest, cash)/LOC) 주 매수
    매도: close ≥ prev_close×(1+σ×k_sell) → round(holdings×sell_ratio/100) 주 매도
    티어=1마다 총투자금 갱신 (RENEWAL 사이클 전 누적실현 기준)
    """
    df = price_df.copy()
    # σ 워밍업: price_df 전체(start_date 이전 버퍼 포함)로 σ 사전 계산
    # 포트폴리오 시뮬은 start_date 이후만 수행
    _df_sigma = df.loc[:pd.to_datetime(end_date)].copy()
    df        = _df_sigma.loc[pd.to_datetime(start_date):].copy()
    if df.empty or len(_df_sigma) < sigma_period + 2:
        return None

    # σ 사전 계산 (버퍼 포함 전체 구간 → 시작일부터 σ 유효)
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

    # 시뮬레이션 구간에 해당하는 σ 추출
    _sim_offset = _df_sigma.index.searchsorted(df.index[0])
    closes = df["Close"].values.astype(float)
    n      = len(closes)
    sigmas = _s_all[_sim_offset:_sim_offset + n]

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

        # σ 미확정 구간 (초기 sigma_period일) - 자산만 기록
        if np.isnan(sigma):
            assets_arr[i] = cash + holdings * close
            cash_arr[i]   = cash
            cum_realiz_hist.append(cum_realized)
            continue

        prev_close = closes[i-1]
        buy_loc    = prev_close * (1.0 + sigma * k_buy)
        sell_loc   = prev_close * (1.0 + sigma * k_sell)

        # ── 티어 순환 (1 → divisions → 1 → ...)
        tier = (tier % divisions) + 1

        # ── 총투자금 갱신 (tier=1, 첫 번째 이후부터)
        if tier == 1 and len(cum_realiz_hist) > 0:
            lookback = (renewal + 1) if len(cum_realiz_hist) > 2 * divisions else renewal
            prev_cum = cum_realiz_hist[-lookback] if len(cum_realiz_hist) >= lookback else 0.0
            delta    = cum_realized - prev_cum
            total_invest += delta * (pcr if delta >= 0 else lcr)

        daily_invest = total_invest / divisions
        prev_avg     = avg_cost
        prev_hold    = holdings

        # ── 매도 판정
        sell_qty = 0
        if holdings > 0 and close >= sell_loc:
            sell_qty = int(round(float(holdings) * sell_ratio / 100.0))

        # ── 매수 판정
        buy_qty = 0
        if close <= buy_loc:
            available = min(daily_invest, cash)
            buy_qty   = math.floor(available / buy_loc)

        # ── 체결 처리
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
                "실현손익":  round(sell_amt - prev_avg * sell_qty if sell_qty > 0 else 0, 2),
                "누적실현":  round(cum_realized, 2),
                "총투자금":  round(total_invest, 2),
                "예수금":    round(cash, 2),
                "총자산":    round(total_asset, 2),
            })

    # ── 성과 지표
    valid = assets_arr[~np.isnan(assets_arr)]
    if len(valid) == 0:
        return None

    final_asset  = float(assets_arr[-1])
    peak         = np.maximum.accumulate(np.where(np.isnan(assets_arr), np.inf, assets_arr))
    peak         = np.maximum.accumulate(assets_arr[~np.isnan(assets_arr)])
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
    )
    if return_history:
        out["history"] = pd.DataFrame(history)
    return out


def run_stdev_tier_analysis(hist_df: "pd.DataFrame", div4: int) -> list:
    """
    표준편차매매: 매도 이벤트 단위의 분할 매수 횟수별 성과 분석.

    표준편차매매는 sell_ratio<100% 부분 매도가 대부분이라 완전 청산(보유량=0)이
    드물게 발생한다.  그래서 '완전 청산 사이클' 대신 '매도 이벤트' 단위로 분석한다.

    각 매도 발생 시:
      - 직전 매도(또는 포지션 완전 청산) 이후 몇 번 매수가 있었는지 = 티어수
      - 티어수는 1 ~ div4 로 캡 (div4 이상은 div4 로 기록)
      - pnl = (매도가 / 평단가 - 1) × 100
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

        # ── 매도 처리 (엔진 순서: 매도 → 매수)
        if sell_qty > 0:
            n_tier    = max(1, min(buys_since_last_sell, div4))
            pnl       = (close / avg_cost - 1) * 100 if avg_cost > 0 else 0.0
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

            # 완전 청산 시에도 동일하게 리셋 (이미 위에서 처리됨)

        # ── 매수 처리
        if buy_qty > 0:
            buys_since_last_sell += 1
            if first_buy_idx_since is None:
                first_buy_idx_since = idx

    return events


def run_stdev_ordersheet(
    price_df, start_date,
    sigma_period=2, k_buy=0.55, k_sell=0.55,
    sell_ratio=75.0, divisions=5, renewal=5,
    pcr=1.0, lcr=1.0, initial_capital=20000.0,
):
    """오늘까지 시뮬레이션 → 현재 상태(티어·보유량·예수금·평단가) + 내일 주문가 반환.

    price_df 는 start_date 이전 버퍼(90일)를 포함해 로드된 전체 데이터.
    - σ 계산: price_df 전체의 최근 sigma_period 수익률 사용 (항상 최신 가격 기준)
    - 포트폴리오 시뮬: start_date 이후 데이터만 사용
    - start_date 이후 거래일이 없으면 초기 상태(0보유·전액현금) 반환
    """
    today = datetime.today().date()
    df_all = price_df.copy()

    # ── σ 계산: 전체 price_df의 최근 데이터 사용 ──────────
    closes_all = df_all["Close"].dropna().values.astype(float)
    if len(closes_all) < sigma_period + 1:
        return None   # σ 계산 불가 (데이터 자체가 너무 짧음)

    rets_last = [(closes_all[j] - closes_all[j-1]) / closes_all[j-1]
                 for j in range(len(closes_all) - sigma_period, len(closes_all))]
    sigma_next = float(np.std(rets_last, ddof=0))
    last_close = float(closes_all[-1])

    next_buy_loc  = round(last_close * (1.0 + sigma_next * k_buy),  2)
    next_sell_loc = round(last_close * (1.0 + sigma_next * k_sell), 2)

    # ── 포트폴리오 시뮬: start_date 이후 데이터 ───────────
    portfolio_df = df_all.loc[pd.to_datetime(start_date):pd.to_datetime(today)]

    if portfolio_df.empty:
        # start_date가 아직 미래이거나 거래일 없음 → 초기 상태
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
            # 시뮬 결과 없음 → 초기 상태로 fallback
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

    # ── 내일 예상 수량 ────────────────────────────────────
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
def run_5tier_analysis(price_df, start_date, end_date, a_buy, a_sell, sell_ratio, divisions, initial_capital, n_days=2):
    """분할수(N) 이상 매수 후 매도된 사이클(N티어 완전 투자) 이벤트 추출."""
    sim_raw = price_df.loc[pd.to_datetime(start_date):pd.to_datetime(end_date)].copy()
    sim, closes, tgt_buy, tgt_sell = _prep_sim(sim_raw, n_days, a_buy, a_sell)
    if sim.empty:
        return []

    cash       = float(initial_capital)
    shares     = 0
    avg_cost   = 0.0
    open_tiers = []
    prev_asset = float(initial_capital)
    cycle_buys = []   # 현재 사이클의 매수 목록
    events     = []

    # 거래일 인덱스 매핑 (Timestamp → 거래일 순번)
    _tday_idx = {ts: idx for idx, ts in enumerate(sim.index)}

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
                    hold = _tday_idx[date] - _tday_idx[nth["date"]]
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
            # LOC 주문: 수량은 기준가(tb) 기준, 체결은 실제 종가(x)
            buy_qty = min(
                math.floor(current_chunk / tb + 1e-9),
                math.floor(cash / tb + 1e-9),
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


def run_tier_breakdown_analysis(price_df, start_date, end_date, a_buy, a_sell, sell_ratio, divisions, initial_capital, n_days=2):
    """티어별 완전 청산 사이클 분석.
    포지션이 0이 될 때까지 추적하여, 각 사이클에서 몇 티어까지 매수됐는지 기록.
    1티어만 사고 매도, 2티어 사고 매도, ... N티어 사고 매도 각각 통계 산출.
    """
    sim_raw = price_df.loc[pd.to_datetime(start_date):pd.to_datetime(end_date)].copy()
    sim, closes, tgt_buy, tgt_sell = _prep_sim(sim_raw, n_days, a_buy, a_sell)
    if sim.empty:
        return []

    cash       = float(initial_capital)
    shares     = 0
    avg_cost   = 0.0
    open_tiers = []
    prev_asset = float(initial_capital)

    # 사이클 추적
    cycle_buys            = []   # 이번 사이클의 매수 내역
    cycle_total_invested  = 0.0  # 이번 사이클 총 투자금
    cycle_total_received  = 0.0  # 이번 사이클 총 매도 수익
    cycle_start_date      = None

    events = []

    # 거래일 인덱스 매핑 (Timestamp → 거래일 순번)
    _tday_idx = {ts: idx for idx, ts in enumerate(sim.index)}

    for i in range(len(closes)):
        x    = closes[i]
        tb   = tgt_buy[i]
        ts   = tgt_sell[i]
        date = sim.index[i]
        current_chunk = prev_asset / divisions

        if shares > 0 and x >= ts:
            sell_qty = math.floor(shares * (sell_ratio / 100.0))
            if sell_qty > 0:
                proceeds               = sell_qty * x
                cash                  += proceeds
                shares                -= sell_qty
                cycle_total_received  += proceeds

                # FIFO 티어 소진
                remaining = sell_qty
                while remaining > 0 and open_tiers:
                    if open_tiers[0]["qty"] <= remaining:
                        remaining -= open_tiers[0]["qty"]
                        open_tiers.pop(0)
                    else:
                        open_tiers[0]["qty"] -= remaining
                        remaining = 0

                if shares == 0:
                    # 포지션 완전 청산 → 사이클 기록
                    n_tiers   = len(cycle_buys)
                    hold_days = (_tday_idx[date] - _tday_idx[cycle_start_date]) if cycle_start_date else 0
                    pnl       = (cycle_total_received - cycle_total_invested) / cycle_total_invested * 100 \
                                if cycle_total_invested > 0 else 0.0
                    events.append({
                        "티어수":     n_tiers,
                        "시작일":     str(cycle_start_date.date()) if cycle_start_date else "",
                        "매도완료일": str(date.date()),
                        "보유일수":   hold_days,
                        "평균단가":   round(cycle_total_invested / sum(b["qty"] for b in cycle_buys), 2)
                                      if cycle_buys and sum(b["qty"] for b in cycle_buys) > 0 else 0.0,
                        "최종매도가": round(x, 2),
                        "손익률":     round(pnl, 2),
                    })
                    # 사이클 초기화
                    cycle_buys           = []
                    cycle_total_invested = 0.0
                    cycle_total_received = 0.0
                    cycle_start_date     = None
                    avg_cost             = 0.0
                    open_tiers           = []
                else:
                    # 부분 매도 → avg_cost 재계산
                    if open_tiers:
                        total_inv = sum(t["price"] * t["qty"] for t in open_tiers)
                        total_qty = sum(t["qty"] for t in open_tiers)
                        avg_cost  = total_inv / total_qty if total_qty > 0 else 0.0

        elif x <= tb:
            # LOC 주문: 수량은 기준가(tb) 기준, 체결은 실제 종가(x)
            buy_qty = min(
                math.floor(current_chunk / tb + 1e-9),
                math.floor(cash / tb + 1e-9),
            )
            if buy_qty > 0:
                total_inv             = avg_cost * shares + x * buy_qty
                shares               += buy_qty
                avg_cost              = total_inv / shares
                cash                 -= buy_qty * x
                cycle_total_invested += buy_qty * x
                open_tiers.append({"date": date, "price": x, "qty": buy_qty})
                cycle_buys.append({"date": date, "price": x, "qty": buy_qty})
                if cycle_start_date is None:
                    cycle_start_date = date

        prev_asset = cash + shares * x

    return events


def compute_sharpe_sortino(assets, risk_free_annual=0.04):
    """샤프 비율 & 소르티노 비율 (연환산)."""
    if len(assets) < 2:
        return 0.0, 0.0
    daily_ret = np.diff(assets) / assets[:-1]
    rf_daily  = risk_free_annual / 252
    excess    = daily_ret - rf_daily
    std_all   = np.std(excess, ddof=1)
    sharpe    = np.mean(excess) / std_all * np.sqrt(252) if std_all > 0 else 0.0
    downside  = excess[excess < 0]
    std_down  = np.std(downside, ddof=1) if len(downside) > 1 else 0.0
    sortino   = np.mean(excess) / std_down * np.sqrt(252) if std_down > 0 else 0.0
    return round(sharpe, 3), round(sortino, 3)


def compute_rolling_perf(assets, window_days=252):
    """롤링 CAGR(%) 및 MDD(%) 계산. 첫 window_days 구간은 NaN."""
    n = len(assets)
    rolling_cagr = np.full(n, np.nan)
    rolling_mdd  = np.full(n, np.nan)
    years = window_days / 252.0
    for i in range(window_days, n):
        sub  = assets[i - window_days: i + 1]
        cagr = (sub[-1] / sub[0]) ** (1.0 / years) - 1.0
        peak = np.maximum.accumulate(sub)
        mdd  = ((sub - peak) / peak).min()
        rolling_cagr[i] = round(cagr * 100, 2)
        rolling_mdd[i]  = round(mdd * 100, 2)
    return rolling_cagr, rolling_mdd


def compute_bnh(price_df, start_date, end_date, initial_capital):
    """Buy & Hold 자산 시계열 반환."""
    sub = price_df.loc[pd.to_datetime(start_date):pd.to_datetime(end_date), "Close"].dropna()
    if sub.empty:
        return np.array([]), pd.DatetimeIndex([])
    shares_bnh = initial_capital / float(sub.iloc[0])
    assets_bnh = sub.values.astype(float) * shares_bnh
    return assets_bnh, sub.index


# ──────────────────────────────────────────────
# 탭 구성
# ──────────────────────────────────────────────
_is_stdev = (st.session_state.get("strategy_radio", "📐 표준편차매매") == "📐 표준편차매매")

# ── 전략별 탭 구성 ────────────────────────────
if _is_stdev:
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 백테스트", "🔍 파라미터 최적화", "📋 오늘의 주문표", "📖 전략 소개 & 성과", "⚙️ 개인 설정",
    ])
else:
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 백테스트", "🔍 파라미터 최적화", "📋 오늘의 주문표",
        "📖 전략 소개 & 성과", "⚙️ 개인 설정",
    ])

with tab1:
  if _is_stdev:
    # ── 표준편차매매 백테스트 ──────────────────
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

        # ── 핵심 지표 ──
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

        # ── 자산 곡선 ──
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

        # ── 거래 히스토리 테이블 ──
        with st.expander("📋 거래 상세 내역", expanded=False):
            hist_df = res["history"].copy()
            hist_df["날짜"] = hist_df["날짜"].astype(str)
            st.dataframe(hist_df, hide_index=True, use_container_width=True, height=400)
            csv_bytes = hist_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button("⬇️ CSV 다운로드", csv_bytes, f"{tk_lbl}_stdev_backtest.csv", "text/csv")

        # ── 연도별 수익률 ──
        with st.expander("📅 연도별 성과", expanded=False):
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
  else:
    # ── 종가평균매매 기존 백테스트 ─────────────
    if st.button("▶ 백테스트 실행", type="primary", key="run_bt"):
        with st.spinner("데이터 로드 및 시뮬레이션 중..."):
            price_df = load_price_data(ticker, start_date, end_date, data_source, excel_file)

        if price_df.empty:
            st.error("가격 데이터를 불러오지 못했습니다.")
            st.stop()

        result = run_backtest(
            price_df, start_date, end_date,
            a_buy, a_sell, sell_ratio, divisions, initial_capital,
            return_history=True, n_days=n_days,
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
        _hist_disp = hist_df.copy()
        _hist_disp["매매"] = _hist_disp.apply(
            lambda r: f"BUY (${r['종가(x)']:.2f})"  if r["매매"] == "BUY"
                 else (f"SELL (${r['종가(x)']:.2f})" if r["매매"] == "SELL" else "-"),
            axis=1,
        )
        colored = _hist_disp.style.format({
            "종가(x)": "${:,.4f}", "전날(p1)": "${:,.4f}",
            "전전날(p2)": "${:,.4f}", "매수경계가": "${:,.4f}",
            "매도경계가": "${:,.4f}", "거래주수": "{:,}",
            "거래금액($)": "${:,.2f}", "보유주수": "{:,}",
            "현금($)": "${:,.2f}", "총자산($)": "${:,.2f}",
        }).apply(
            lambda row: [
                "background-color: #ffdddd" if str(row["매매"]).startswith("BUY")
                else ("background-color: #ddffdd" if str(row["매매"]).startswith("SELL") else "")
                for _ in row
            ], axis=1,
        )
        st.dataframe(colored, use_container_width=True, height=500)

        csv = hist_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("💾 결과 CSV 다운로드", data=csv,
                           file_name=f"backtest_{ticker}.csv", mime="text/csv")
  # ← else(종가평균) 블록 종료


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
  if _is_stdev:
    # ══════════════════════════════════════════
    # 표준편차매매 파라미터 최적화 (4종 방식)
    # ══════════════════════════════════════════
    st.subheader("🔍 표준편차매매 파라미터 최적화")

    sd_opt_method = st.radio(
        "최적화 방식",
        ["📊 그리드 탐색", "🎲 랜덤 탐색", "📈 워크포워드", "🧠 베이지안"],
        horizontal=True, key="sd_opt_method",
    )
    _sd_method_desc = {
        "📊 그리드 탐색": "k_buy × k_sell × 매도비율 × 분할수 모든 조합을 완전 탐색합니다. 조합이 적을 때 가장 정확합니다.",
        "🎲 랜덤 탐색":   "무작위로 N개 조합을 샘플링합니다. 탐색 공간이 클 때 빠르게 좋은 파라미터를 찾습니다.",
        "📈 워크포워드":  "전체 기간을 IS(최적화)·OOS(검증) 윈도우로 분할해 과적합을 방지합니다. 실전에 가장 가까운 검증 방식입니다.",
        "🧠 베이지안":    "Optuna TPE 알고리즘으로 스마트하게 탐색합니다. 적은 시도로 최적값에 빠르게 수렴합니다.",
    }
    st.caption(_sd_method_desc[sd_opt_method])

    # ── 공통 파라미터 범위 설정 ──────────────────
    with st.expander("파라미터 범위 설정", expanded=True):
        _sd_kb_same = st.checkbox("k_buy = k_sell 동일하게 설정", value=True, key="sd_kb_same")

        st.markdown("**k_buy 범위**  *(매수 σ 계수 — LOC = 전일종가 × (1 + σ × k_buy))*")
        _skc1, _skc2, _skc3 = st.columns(3)
        sd_kb_min  = _skc1.number_input("최솟값", value=0.20, step=0.05, format="%.2f", key="sd_kb_min")
        sd_kb_max  = _skc2.number_input("최댓값", value=1.50, step=0.05, format="%.2f", key="sd_kb_max")
        sd_kb_step = _skc3.number_input("간격 (스텝)",
                        value=0.05, min_value=0.01, step=0.05, format="%.2f", key="sd_kb_step",
                        help="k값을 이 간격으로 나눠서 탐색합니다. 예) 0.05 → 0.20, 0.25, 0.30 … 순서로 탐색")

        if not _sd_kb_same:
            st.markdown("**k_sell 범위**  *(매도 σ 계수)*")
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

    # ── 결과 표시 공통 함수 ──────────────────────
    def _show_stdev_opt_results(res_df, sort_col, kb_vals, ks_vals, ticker, key_sfx):
        st.subheader(f"🏆 상위 20개 결과  ({sort_col} 기준)")
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
            st.subheader(f"🗺️ 히트맵: k_buy × k_sell  →  {sort_col}")
            _hdf = res_df.groupby(["k_buy", "k_sell"])[sort_col].max().reset_index()
            _hpiv = _hdf.pivot(index="k_sell", columns="k_buy", values=sort_col)
            _show_txt = len(kb_vals) * len(ks_vals) <= 400
            fig_hmap = px.imshow(_hpiv, color_continuous_scale="RdYlGn",
                                  labels={"x": "k_buy", "y": "k_sell", "color": sort_col},
                                  aspect="auto", text_auto=".2f" if _show_txt else False)
            fig_hmap.update_layout(height=500)
            st.plotly_chart(fig_hmap, use_container_width=True)
        st.subheader("📊 리스크-수익 분포  (CAGR vs MDD)")
        _hover = [c for c in ["k_buy","k_sell","분할수","매도비율(%)","Calmar"] if c in res_df.columns]
        fig_sc = px.scatter(res_df, x="MDD(%)", y="CAGR(%)", color=sort_col,
                             hover_data=_hover, color_continuous_scale="RdYlGn")
        fig_sc.update_layout(height=420)
        st.plotly_chart(fig_sc, use_container_width=True)
        _ocsv = res_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("💾 최적화 결과 CSV", data=_ocsv,
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

    # ── ① 그리드 탐색 ───────────────────────────
    if sd_opt_method == "📊 그리드 탐색":
        _sd_info = (f"예상 조합 수: **{sd_n_total:,}개** "
                    f"(k_buy {len(sd_kb_vals)} × k_sell {len(sd_ks_vals)} "
                    f"× 분할수 {len(sd_dv_vals)} × 매도비율 {len(sd_sr_vals)})")
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
            rows, cnt = [], 0
            upd = max(1, sd_n_total // 100)
            for kb in sd_kb_vals:
                for ks in sd_ks_vals:
                    for dv in sd_dv_vals:
                        for sr in sd_sr_vals:
                            r = run_backtest_stdev(
                                price_df_opt, str(start_date), str(end_date),
                                sigma_period=sd_sigma_period, k_buy=kb, k_sell=ks,
                                sell_ratio=float(sr), divisions=int(dv), renewal=sd_renewal,
                                initial_capital=initial_capital,
                            )
                            if r: rows.append(_sd_make_row(kb, ks, sr, dv, r))
                            cnt += 1
                            if cnt % upd == 0:
                                progress.progress(min(cnt/sd_n_total,1.0), text=f"실행 중... {cnt:,}/{sd_n_total:,}")
            progress.progress(1.0, text="완료!")
            if not rows: st.error("유효한 결과가 없습니다."); st.stop()
            res_df = pd.DataFrame(rows).sort_values(_sd_sort_col, ascending=_sd_sort_asc).reset_index(drop=True)
            _show_stdev_opt_results(res_df, _sd_sort_col, sd_kb_vals,
                                    None if _sd_kb_same else sd_ks_vals, ticker, "grid")

    # ── ② 랜덤 탐색 ────────────────────────────
    elif sd_opt_method == "🎲 랜덤 탐색":
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
            rows = []
            for i, (kb, ks, dv, sr) in enumerate(sampled):
                r = run_backtest_stdev(
                    price_df_opt, str(start_date), str(end_date),
                    sigma_period=sd_sigma_period, k_buy=kb, k_sell=ks,
                    sell_ratio=float(sr), divisions=int(dv), renewal=sd_renewal,
                    initial_capital=initial_capital,
                )
                if r: rows.append(_sd_make_row(kb, ks, sr, dv, r))
                if i % max(1, int(sd_n_samples)//100) == 0:
                    progress.progress(min(i/int(sd_n_samples),1.0), text=f"실행 중... {i:,}/{int(sd_n_samples):,}")
            progress.progress(1.0, text="완료!")
            if not rows: st.error("유효한 결과가 없습니다."); st.stop()
            res_df = pd.DataFrame(rows).sort_values(_sd_sort_col, ascending=_sd_sort_asc).reset_index(drop=True)
            _show_stdev_opt_results(res_df, _sd_sort_col, None, None, ticker, "random")

    # ── ③ 워크포워드 ───────────────────────────
    elif sd_opt_method == "📈 워크포워드":
        wf1, wf2 = st.columns(2)
        sd_is_years  = wf1.number_input("IS(최적화) 기간 (년)", min_value=1, max_value=10, value=3, key="sd_wf_is")
        sd_oos_years = wf2.number_input("OOS(검증) 기간 (년)",  min_value=1, max_value=5,  value=1, key="sd_wf_oos")
        st.info(
            f"📐 IS **{sd_is_years}년** 최적화 → OOS **{sd_oos_years}년** 검증을 슬라이딩 반복합니다.\n\n"
            f"그리드 조합 **{sd_n_total:,}개** × 윈도우 수 만큼 백테스트가 실행됩니다."
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
            total_steps = len(windows) * max(sd_n_total, 1)
            progress    = st.progress(0.0, text="워크포워드 실행 중...")
            step_count  = 0
            wfo_rows    = []
            cur_capital = initial_capital
            for wi, (is_s, is_e, oos_s, oos_e) in enumerate(windows):
                best_score, best_params = -999.0, None
                for kb in sd_kb_vals:
                    for ks in sd_ks_vals:
                        for dv in sd_dv_vals:
                            for sr in sd_sr_vals:
                                r = run_backtest_stdev(
                                    price_df_opt, str(is_s), str(is_e),
                                    sigma_period=sd_sigma_period, k_buy=kb, k_sell=ks,
                                    sell_ratio=float(sr), divisions=int(dv), renewal=sd_renewal,
                                    initial_capital=initial_capital,
                                )
                                if r:
                                    if "Calmar"   in sd_metric_key: score = r["calmar"]
                                    elif "CAGR"   in sd_metric_key: score = r["cagr"] * 100
                                    elif "총수익률" in sd_metric_key: score = r["total_return"] * 100
                                    else:                            score = -abs(r["mdd"] * 100)
                                    if score > best_score:
                                        best_score  = score
                                        best_params = (kb, ks, dv, sr)
                                step_count += 1
                                if step_count % max(1, total_steps//200) == 0:
                                    progress.progress(min(step_count/total_steps, 0.99),
                                                      text=f"윈도우 {wi+1}/{len(windows)} IS 최적화 중...")
                if best_params is None: continue
                kb_b, ks_b, dv_b, sr_b = best_params
                oos_r = run_backtest_stdev(
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
            st.subheader("📊 워크포워드 종합 성과")
            wc1, wc2, wc3, wc4 = st.columns(4)
            wc1.metric("시작 자본",       f"${initial_capital:,.0f}")
            wc2.metric("최종 자본 (OOS)", f"${cur_capital:,.0f}")
            wc3.metric("OOS 총 수익률",   f"{total_ret*100:+.2f}%")
            wc4.metric("윈도우 수",       f"{len(wfo_rows)}개")
            st.subheader("🪟 윈도우별 결과")
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
            st.download_button("💾 워크포워드 결과 CSV", data=wfo_csv,
                               file_name=f"wfo_stdev_{ticker}.csv", mime="text/csv", key="sd_dl_wfo")

    # ── ④ 베이지안 (Optuna) ─────────────────────
    elif sd_opt_method == "🧠 베이지안":
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
                    r   = run_backtest_stdev(
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
                    f"🏆 최적 파라미터: k_buy=**{best['k_buy']:.3f}**, "
                    f"k_sell=**{best.get('k_sell', best['k_buy']):.3f}**, "
                    f"분할수=**{best.get('분할수', sd_dv_vals[0])}**, "
                    f"매도비율=**{best.get('매도비율', int(sd_sr_min))}%**"
                )
                _show_stdev_opt_results(res_df, _sd_sort_col, None, None, ticker, "bayes")
                st.subheader("📈 탐색 수렴 과정")
                _vals     = [t.value for t in study.trials if t.value is not None and t.value > -900]
                _best_cur = [max(_vals[:i+1]) for i in range(len(_vals))]
                fig_conv  = px.line(y=_best_cur,
                                    labels={"y": f"Best {_sd_sort_col}", "index": "Trial"},
                                    title="베이지안 최적화 수렴 곡선")
                fig_conv.update_layout(height=380)
                st.plotly_chart(fig_conv, use_container_width=True)
  else:
    # ── 종가평균 기존 최적화 ──────────────────
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
                                             ab, as_, sr, dv, initial_capital, n_days=n_days)
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
                                 ab, as_, sr, dv, initial_capital, n_days=n_days)
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
                                                 ab, as_, sr, dv, initial_capital, n_days=n_days)
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
                                     ab_b, as_b, sr_b, dv_b, cur_capital, n_days=n_days)
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
                                       ab, as_, sr, dv, initial_capital, n_days=n_days)
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
# 표준편차매매 히스토리 저장 헬퍼
# ══════════════════════════════════════════════
def _get_sd_history_file(tk: str) -> Path:
    if _IS_CLOUD:
        return Path(__file__).parent / f"sd_history_{tk}.csv"
    return Path.home() / ".usd-avg" / f"sd_history_{tk}.csv"

def _load_sd_daily_history(tk: str) -> pd.DataFrame:
    """Cloud: GSheets 우선 읽기. 로컬: CSV 읽기."""
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

# ══════════════════════════════════════════════
# 표준편차매매 계좌별 주문표 렌더러
# ══════════════════════════════════════════════
_SD_PRESETS_DB = {
    "SOXL": [
        {"label": "🚀 공격형",
         "k_buy": 0.70, "k_sell": 0.50, "sell_ratio": 60.0, "divisions": 5,
         "help": "CAGR 60.19%  |  MDD 41.26%  |  Calmar 1.46\n높은 수익률 추구, 변동성 감수"},
        {"label": "⚖️ 균형형",
         "k_buy": 0.65, "k_sell": 0.50, "sell_ratio": 75.0, "divisions": 5,
         "help": "CAGR 53.89%  |  MDD 33.62%  |  Calmar 1.60\n수익률과 안정성의 중간"},
        {"label": "🛡️ 안정형 ⭐",
         "k_buy": 0.65, "k_sell": 0.45, "sell_ratio": 85.0, "divisions": 5,
         "help": "CAGR 48.64%  |  MDD 28.02%  |  Calmar 1.74\n최저 MDD + 최고 Calmar — 안정적 운용 추천"},
    ],
}

def _render_sd_account_tab(tk: str, tk_cfg: dict, key_sfx: str):
    """표준편차매매 ticker별 주문표 탭 렌더링."""
    _kb   = float(tk_cfg.get("k_buy",       0.65))
    _ks   = float(tk_cfg.get("k_sell",      0.65))
    _sr   = float(tk_cfg.get("sell_ratio",  75.0))
    _div  = int  (tk_cfg.get("divisions",   5))
    _sp   = int  (tk_cfg.get("sigma_period", 2))
    _rn   = int  (tk_cfg.get("renewal",     5))

    _raw_start   = tk_cfg.get("os_start",   "2020-01-01")
    _raw_capital = tk_cfg.get("os_capital", 20000.0)
    try:    _def_start = datetime.strptime(str(_raw_start), "%Y-%m-%d").date()
    except: _def_start = datetime(2020, 1, 1).date()
    try:    _def_cap = float(_raw_capital)
    except: _def_cap = 20000.0

    # ── 계좌 삭제 ──────────────────────────────
    _del_c, _ = st.columns([1, 5])
    if _del_c.button(f"🗑️ {tk} 계좌 삭제", key=f"sd_del_{key_sfx}", type="secondary"):
        st.session_state[f"sd_del_confirm_{key_sfx}"] = True
    if st.session_state.get(f"sd_del_confirm_{key_sfx}", False):
        st.warning(f"⚠️ **{tk} 계좌를 삭제하시겠습니까?** 저장된 설정 및 매매 히스토리가 모두 삭제됩니다.")
        _dc1, _dc2, _ = st.columns([1, 1, 4])
        if _dc1.button("✅ 삭제", key=f"sd_del_ok_{key_sfx}", type="primary"):
            _delete_sd_ticker_setting(tk)
            st.session_state.pop(f"sd_del_confirm_{key_sfx}", None)
            st.rerun()
        if _dc2.button("❌ 취소", key=f"sd_del_cancel_{key_sfx}"):
            st.session_state[f"sd_del_confirm_{key_sfx}"] = False
            st.rerun()

    # ── 파라미터 표시 + 수정 ────────────────────
    with st.container(border=True):
        _p1,_p2,_p3,_p4,_p5,_p6 = st.columns(6)
        _p1.metric("k_buy",    f"{_kb:.2f}")
        _p2.metric("k_sell",   f"{_ks:.2f}")
        _p3.metric("매도비율",  f"{_sr:.0f}%")
        _p4.metric("분할수",    f"{_div}회")
        _p5.metric("σ 기간",   f"{_sp}일")
        _p6.metric("갱신 주기", f"{_rn}주기")

        with st.expander("✏️ 파라미터 수정"):
            _param_presets = _SD_PRESETS_DB.get(tk, [])
            if _param_presets:
                st.caption("💡 추천 프리셋 — 버튼 위에 마우스를 올리면 성과 지표를 확인할 수 있습니다.")
                _pp1, _pp2, _pp3 = st.columns(3)
                for _pi, (_ppc, _pp) in enumerate(zip([_pp1, _pp2, _pp3], _param_presets)):
                    if _ppc.button(_pp["label"], key=f"sd_preset_{_pi}_{key_sfx}",
                                   help=_pp["help"], use_container_width=True):
                        st.session_state[f"sd_ekb_{key_sfx}"]  = _pp["k_buy"]
                        st.session_state[f"sd_eks_{key_sfx}"]  = _pp["k_sell"]
                        st.session_state[f"sd_esr_{key_sfx}"]  = _pp["sell_ratio"]
                        st.session_state[f"sd_edv_{key_sfx}"]  = _pp["divisions"]
                        st.rerun()
                st.divider()
            else:
                st.caption("ℹ️ 이 종목은 추천 프리셋이 없습니다. 직접 파라미터를 입력해 주세요.")
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
            _new_sp  = _ep1.number_input("σ 계산 기간",  min_value=1, max_value=20, step=1,
                                          key=f"sd_esp_{key_sfx}")
            _new_rn  = _ep2.number_input("갱신 주기",    min_value=1, step=1,  key=f"sd_ern_{key_sfx}")
            if st.button("💾 파라미터 저장", key=f"sd_save_{key_sfx}", type="primary",
                         use_container_width=True):
                _save_sd_ticker_setting(tk, {
                    "k_buy": float(_new_kb), "k_sell": float(_new_ks),
                    "sell_ratio": float(_new_sr), "divisions": int(_new_div),
                    "sigma_period": int(_new_sp), "renewal": int(_new_rn),
                })
                for _sk3 in [f"sd_ekb_{key_sfx}", f"sd_eks_{key_sfx}", f"sd_esr_{key_sfx}",
                              f"sd_edv_{key_sfx}", f"sd_esp_{key_sfx}", f"sd_ern_{key_sfx}"]:
                    st.session_state.pop(_sk3, None)
                st.success(f"✅ {tk} 파라미터가 저장되었습니다!")
                st.rerun()

    # ── 시작일 / 자본금 ─────────────────────────
    _sc1, _sc2 = st.columns(2)
    _os_start = _sc1.date_input("시작일", value=_def_start,
                                 min_value=datetime(2000, 1, 1).date(),
                                 max_value=datetime.today().date(),
                                 key=f"sd_os_start_{key_sfx}")
    _os_cap   = _sc2.number_input("시작 자본 ($)", value=_def_cap,
                                   step=1000.0, key=f"sd_os_cap_{key_sfx}")

    # ── 자본 조정 (증액 / 감액) ─────────────────
    with st.expander("💰 자본 조정 (증액 / 감액)"):
        st.caption("현재 자본금에 추가하거나 차감할 금액을 입력하세요.")
        _sd_adj_raw = tk_cfg.get("capital_adj_history", "[]")
        try:
            _sd_adj_hist = json.loads(_sd_adj_raw) if isinstance(_sd_adj_raw, str) else _sd_adj_raw
            if not isinstance(_sd_adj_hist, list): _sd_adj_hist = []
        except: _sd_adj_hist = []

        _sadj_c1, _sadj_c2 = st.columns([2, 1])
        _sadj_amt = _sadj_c1.number_input("조정 금액 ($)", value=0.0, step=500.0,
                                           help="증액: 양수 · 감액: 음수",
                                           key=f"sd_adj_inp_{key_sfx}")
        _sadj_c1.caption(
            f"적용 후 자본금: **${_def_cap + _sadj_amt:,.0f}** "
            f"({'↑' if _sadj_amt > 0 else '↓' if _sadj_amt < 0 else '='} "
            f"${abs(_sadj_amt):,.0f})"
        )
        _sadj_memo = _sadj_c1.text_input("메모 (선택)", placeholder="예: 3월 추가 입금",
                                          key=f"sd_adj_memo_{key_sfx}")
        if _sadj_c2.button("💰 적용", use_container_width=True,
                            key=f"sd_apply_adj_{key_sfx}", disabled=(_sadj_amt == 0)):
            _new_cap = _def_cap + _sadj_amt
            if _new_cap <= 0:
                st.error("자본금은 0보다 커야 합니다.")
            else:
                _sd_adj_hist.append({
                    "날짜": datetime.today().strftime("%Y-%m-%d"),
                    "조정금액": float(_sadj_amt),
                    "누적자본금": float(_new_cap),
                    "메모": _sadj_memo or ("증액" if _sadj_amt > 0 else "감액"),
                })
                _save_sd_ticker_setting(tk, {
                    "os_capital": _new_cap,
                    "capital_adj_history": json.dumps(_sd_adj_hist, ensure_ascii=False),
                })
                st.success(f"✅ 자본금이 **${_new_cap:,.0f}**으로 업데이트되었습니다.")
                st.rerun()

        if _sd_adj_hist:
            st.markdown("---")
            st.markdown("**📋 자본 조정 이력**")
            _df_sadj = pd.DataFrame(_sd_adj_hist)
            _df_sadj["조정금액"]  = _df_sadj["조정금액"].apply(lambda x: f"{'↑' if x>0 else '↓'} ${abs(x):,.0f}")
            _df_sadj["누적자본금"] = _df_sadj["누적자본금"].apply(lambda x: f"${x:,.0f}")
            st.dataframe(_df_sadj[["날짜","조정금액","누적자본금","메모"]],
                         use_container_width=True, hide_index=True)
        else:
            st.info("아직 자본 조정 이력이 없습니다.")

        # 전체 초기화
        st.markdown("---")
        st.markdown("**🔄 전체 초기화**")
        st.caption("시작일·자본금·조정 이력을 모두 초기화합니다.")
        _src1, _src2, _src3 = st.columns(3)
        _sreset_start   = _src1.date_input("새 시작일", value=datetime.today().date(),
                                            key=f"sd_reset_start_{key_sfx}")
        _sreset_capital = _src2.number_input("새 시작 자본 ($)", value=_def_cap,
                                              step=1000.0, key=f"sd_reset_cap_{key_sfx}")
        if _src3.button("🔄 초기화", use_container_width=True,
                        key=f"sd_do_reset_{key_sfx}", type="secondary"):
            st.session_state[f"sd_reset_confirm_{key_sfx}"] = True
        if st.session_state.get(f"sd_reset_confirm_{key_sfx}", False):
            st.warning(f"⚠️ **정말 초기화하시겠습니까?**  \n"
                       f"시작일: {_sreset_start} / 자본금: ${_sreset_capital:,.0f} / 조정 이력 전체 삭제")
            _sc1r, _sc2r = st.columns(2)
            if _sc1r.button("✅ 확인 (초기화)", type="primary", key=f"sd_confirm_reset_{key_sfx}"):
                _save_sd_ticker_setting(tk, {
                    "os_start": str(_sreset_start),
                    "os_capital": float(_sreset_capital),
                    "capital_adj_history": "[]",
                })
                st.session_state[f"sd_reset_confirm_{key_sfx}"] = False
                st.success(f"✅ 초기화 완료! 시작일: {_sreset_start} / 자본금: ${_sreset_capital:,.0f}")
                st.rerun()
            if _sc2r.button("❌ 취소", key=f"sd_cancel_reset_{key_sfx}"):
                st.session_state[f"sd_reset_confirm_{key_sfx}"] = False
                st.rerun()

    # ── 주문표 로드 ──────────────────────────────
    _sd_ss  = f"sd_os_res_{key_sfx}"
    _sd_lbl = "🔄 새로고침" if st.session_state.get(_sd_ss) else "📥 주문표 로드"
    if st.button(_sd_lbl, type="primary", key=f"sd_run_os_{key_sfx}"):
        _save_sd_ticker_setting(tk, {"os_start": str(_os_start), "os_capital": _os_cap})
        with st.spinner("데이터 로드 및 시뮬레이션 중..."):
            _buf_start = (_os_start - timedelta(days=90))
            _pdf = load_price_data(tk, str(_buf_start), str(datetime.today().date()),
                                   "야후 파이낸스 (yfinance)", None)
        if _pdf is None or _pdf.empty:
            st.error("가격 데이터를 불러오지 못했습니다.")
        else:
            _sd_r = run_stdev_ordersheet(
                price_df=_pdf, start_date=str(_os_start),
                sigma_period=_sp, k_buy=_kb, k_sell=_ks,
                sell_ratio=_sr, divisions=_div, renewal=_rn,
                initial_capital=_os_cap,
            )
            if _sd_r is None:
                st.error("주문표 계산 실패. 데이터 기간을 늘려보세요.")
            else:
                st.session_state[_sd_ss] = _sd_r
                # 새 날짜 히스토리 누적 저장 (B방식)
                _save_sd_daily_history(tk, _sd_r.get("hist"))

    _sd_res = st.session_state.get(_sd_ss)
    if _sd_res is None:
        return

    # ── 기간 표시 ────────────────────────────────
    st.markdown(f"**{_os_start} ~ {datetime.today().date()}**")

    # ── 성과 요약 (5 metrics) ────────────────────
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

    _sm1, _sm2, _sm3, _sm4, _sm5 = st.columns(5)
    _sm1.metric("시작 자본",  f"${_os_cap:,.0f}")
    _sm2.metric("최종 자산",  f"${_final_a:,.0f}",
                delta=f"CAGR {_cagr*100:.2f}%")
    _sm3.metric("실현손익",   f"${_realized_pnl:+,.2f}",
                delta=f"시작자본 대비 {_realized_pct:+.2f}%")
    _sm4.metric("MDD",        f"{abs(_mdd)*100:.2f}%")
    _sm5.metric("주식 비중",
                f"{(_holdings*_lc)/(_holdings*_lc+_cash)*100:.1f}%" if (_holdings*_lc+_cash) > 0 else "0%")

    # ── 내일 LOC 주문 ────────────────────────────
    _buy_loc  = _sd_res["next_buy_loc"]
    _sell_loc = _sd_res["next_sell_loc"]
    _buy_qty  = _sd_res["est_buy_qty"]
    _sell_qty = _sd_res["est_sell_qty"]
    _nxt_t    = _sd_res["next_tier"]

    st.subheader(f"📑 오늘의 LOC 주문  ({datetime.today().strftime('%Y-%m-%d')})")
    st.caption(f"최근 종가: **${_lc:.2f}**  |  σ(오늘 예상): **{_sd_res['sigma_next']*100:.4f}%**  |  "
               f"다음 티어: **T{_nxt_t}**")

    _orders = []
    if _holdings > 0:
        _orders.append({
            "구분": "매도", "티커": tk,
            "LOC 기준가":    f"${_sell_loc:.2f}",
            "1회매수금":     "-",
            "예상수량":      f"{_sell_qty:,}주",
            "예상금액":      f"${_sell_qty * _sell_loc:,.0f}",
            "전일종가 대비": f"{(_sell_loc/_lc-1)*100:+.2f}%" if _lc > 0 else "-",
            "비고": f"보유 {_holdings:,}주 × {_sr:.0f}% | 평단 ${_avg_c:.2f}",
        })
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

    st.dataframe(pd.DataFrame(_orders).style.apply(_sd_style, axis=1),
                 use_container_width=True, hide_index=True,
                 height=38 + 35 * len(_orders))

    # ── 현재 보유 현황 ───────────────────────────
    st.subheader("📦 현재 보유 현황")
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

    # ── 일별 매매 상세표 ─────────────────────────
    st.divider()
    st.subheader("📅 일별 매매 상세표")
    _sd_hist_df = _load_sd_daily_history(tk)
    # 히스토리 파일 없으면 현재 시뮬 결과 fallback
    if _sd_hist_df.empty and _hdf is not None and not (hasattr(_hdf, "empty") and _hdf.empty):
        _sd_hist_df = _hdf.copy()
        _sd_hist_df["날짜"] = _sd_hist_df["날짜"].astype(str)

    if not _sd_hist_df.empty:
        _sd_daily = _sd_hist_df.sort_values("날짜", ascending=False).reset_index(drop=True)
        # 매매 유형 파악 (매수량/매도량 컬럼 기준)
        _bc = (_sd_daily["매수량"] > 0).sum() if "매수량" in _sd_daily.columns else 0
        _sc2 = (_sd_daily["매도량"] > 0).sum() if "매도량" in _sd_daily.columns else 0
        _hist_s = _sd_daily["날짜"].iloc[-1]
        _hist_e = _sd_daily["날짜"].iloc[0]
        st.caption(f"기록 {_hist_s} ~ {_hist_e} | 총 {_bc+_sc2}건 (매수 {_bc}회 · 매도 {_sc2}회)")
        st.info("📌 이 기록은 실제 주문표 로드 시점에 누적 저장된 데이터입니다. 파라미터를 변경해도 과거 기록은 변경되지 않습니다.", icon="ℹ️")

        _sd_show = _sd_daily.copy()
        # 숫자 컬럼 포맷
        for _col in ["종가", "매수LOC", "매도LOC"]:
            if _col in _sd_show.columns:
                _sd_show[_col] = _sd_show[_col].apply(lambda v: f"${float(v):,.4f}" if v == v else "-")
        for _col in ["예수금", "총자산"]:
            if _col in _sd_show.columns:
                _sd_show[_col] = _sd_show[_col].apply(lambda v: f"${float(v):,.2f}" if v == v else "-")
        for _col in ["매수량", "매도량", "보유량"]:
            if _col in _sd_show.columns:
                _sd_show[_col] = _sd_show[_col].apply(lambda v: f"{int(v):,}" if v == v else "-")

        # 실현손익 포맷 + 색상용 원본 보존
        _pnl_raw = pd.to_numeric(_sd_daily["실현손익"], errors="coerce") if "실현손익" in _sd_daily.columns else pd.Series([None]*len(_sd_daily))
        _sd_show["실현손익"] = _pnl_raw.apply(
            lambda v: f"+${v:,.2f}" if (not pd.isna(v) and v > 0)
               else (f"-${abs(v):,.2f}" if (not pd.isna(v) and v < 0)
               else "-")
        )
        # 매매 컬럼: BUY/SELL 표시
        def _fmt_trade(row):
            b = int(row.get("매수량", 0) or 0)
            s = int(row.get("매도량", 0) or 0)
            c = float(row.get("종가", 0) or 0)
            if b > 0: return f"BUY (${c:.2f})"
            if s > 0: return f"SELL (${c:.2f})"
            return "-"
        _sd_show["매매"] = _sd_daily.apply(_fmt_trade, axis=1)

        def _style_sd_daily(row):
            if str(row.get("매매","")).startswith("BUY"):  return ["background-color:#FFF0F0"]*len(row)
            if str(row.get("매매","")).startswith("SELL"): return ["background-color:#F0FFF4"]*len(row)
            return [""]*len(row)
        def _style_sd_action(val):
            if str(val).startswith("BUY"):  return "color:#C62828;font-weight:bold"
            if str(val).startswith("SELL"): return "color:#1565C0;font-weight:bold"
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
        _sdl1.download_button("📥 CSV 다운로드", data=_csv_data,
                               file_name=f"{tk}_sd_history_{_today_dl}.csv",
                               mime="text/csv", key=f"sd_dl_csv_{key_sfx}", use_container_width=True)
        _buf = _io.BytesIO()
        with pd.ExcelWriter(_buf, engine="openpyxl") as _writer:
            _sd_daily.to_excel(_writer, index=False, sheet_name="표준편차매매기록")
        _sdl2.download_button("📥 엑셀 다운로드", data=_buf.getvalue(),
                               file_name=f"{tk}_sd_history_{_today_dl}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               key=f"sd_dl_xlsx_{key_sfx}", use_container_width=True)
    else:
        st.info("📭 아직 기록된 매매 데이터가 없습니다. 주문표 로드 후 데이터가 누적됩니다.")


# ══════════════════════════════════════════════
# TAB 3 – 오늘의 주문표 (멀티 계좌 렌더러)
# ══════════════════════════════════════════════
def _render_account_tab(tk: str, tk_cfg: dict, key_sfx: str):
    """ticker별 주문표 탭 렌더링. key_sfx로 위젯 key 충돌 방지."""
    _a_buy      = float(tk_cfg.get("a_buy",      -0.005))
    _a_sell     = float(tk_cfg.get("a_sell",      0.009))
    _sell_ratio = float(tk_cfg.get("sell_ratio",  100.0))
    _divisions  = int  (tk_cfg.get("divisions",   5))
    _n_days     = int  (tk_cfg.get("n_days",      2))

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
        st.warning(f"⚠️ **{tk} 계좌를 삭제하시겠습니까?** 저장된 설정 및 매매 히스토리가 모두 삭제됩니다.")
        _dc1, _dc2, _ = st.columns([1, 1, 4])
        if _dc1.button("✅ 삭제", key=f"del_ok_{key_sfx}", type="primary"):
            _delete_ticker_setting(tk)
            st.session_state.pop(f"del_confirm_{key_sfx}", None)
            st.rerun()
        if _dc2.button("❌ 취소", key=f"del_cancel_{key_sfx}"):
            st.session_state[f"del_confirm_{key_sfx}"] = False
            st.rerun()

    # ── 적용 파라미터 표시 + 수정 ──
    with st.container(border=True):
        _p1, _p2, _p3, _p4, _p5 = st.columns(5)
        _p1.metric("매수기준 (a_buy)",  f"{_a_buy:.4f}")
        _p2.metric("매도기준 (a_sell)", f"{_a_sell:.4f}")
        _p3.metric("매도비율",          f"{_sell_ratio:.0f}%")
        _p4.metric("분할수",            f"{_divisions}회")
        _p5.metric("이동평균",          f"{_n_days + 1}일 평균")

        with st.expander("✏️ 파라미터 수정"):
            # ── 추천 프리셋 (ticker별) ──
            _ALL_PRESETS = {
                "SOXL": [
                    {"label": "🚀 공격형",    "a_buy": -0.0048, "a_sell": 0.0087, "sell_ratio": 100.0, "divisions": 4, "n_days": 2,
                     "help": "CAGR 52.98%  |  MDD 28.91%  |  Calmar 1.83\n높은 수익률 추구, 변동성 감수"},
                    {"label": "⚖️ 균형형",   "a_buy": -0.0048, "a_sell": 0.0096, "sell_ratio": 100.0, "divisions": 5, "n_days": 2,
                     "help": "CAGR 46.01%  |  MDD 25.08%  |  Calmar 1.83\n수익률과 안정성의 중간"},
                    {"label": "🛡️ 안정형 ⭐", "a_buy": -0.0063, "a_sell": 0.0075, "sell_ratio": 100.0, "divisions": 5, "n_days": 2,
                     "help": "CAGR 44.12%  |  MDD 22.94%  |  Calmar 1.92\n최저 MDD + 최고 Calmar — 안정적 운용 추천"},
                ],
                "USD": [
                    {"label": "🚀 공격형",    "a_buy": -0.0088, "a_sell": 0.0063, "sell_ratio": 100.0, "divisions": 4, "n_days": 2,
                     "help": "CAGR 37.64%  |  MDD 24.73%  |  Calmar 1.52\n높은 수익률 추구, 변동성 감수"},
                    {"label": "⚖️ 균형형",   "a_buy": -0.0111, "a_sell": 0.0063, "sell_ratio": 100.0, "divisions": 4, "n_days": 2,
                     "help": "CAGR 36.57%  |  MDD 20.51%  |  Calmar 1.78\n수익률과 안정성의 중간"},
                    {"label": "🛡️ 안정형 ⭐", "a_buy": -0.0111, "a_sell": 0.0063, "sell_ratio": 100.0, "divisions": 5, "n_days": 2,
                     "help": "CAGR 31.37%  |  MDD 16.03%  |  Calmar 1.96\n최저 MDD + 최고 Calmar — 안정적 운용 추천"},
                ],
            }
            _PARAM_PRESETS = _ALL_PRESETS.get(tk, [])
            if _PARAM_PRESETS:
                st.caption("💡 추천 프리셋 — 버튼 위에 마우스를 올리면 성과 지표를 확인할 수 있습니다.")
                _pc1, _pc2, _pc3 = st.columns(3)
                for _pi, (_pcol, _pr) in enumerate(zip([_pc1, _pc2, _pc3], _PARAM_PRESETS)):
                    if _pcol.button(_pr["label"], key=f"preset_{_pi}_{key_sfx}",
                                    help=_pr["help"], use_container_width=True):
                        st.session_state[f"edit_abuy_{key_sfx}"]  = _pr["a_buy"]
                        st.session_state[f"edit_asell_{key_sfx}"] = _pr["a_sell"]
                        st.session_state[f"edit_sr_{key_sfx}"]    = _pr["sell_ratio"]
                        st.session_state[f"edit_div_{key_sfx}"]   = _pr["divisions"]
                        st.session_state[f"edit_ndays_{key_sfx}"] = f"{_pr['n_days'] + 1}일"
                        st.rerun()
                st.divider()
            else:
                st.caption("ℹ️ 이 종목은 추천 프리셋이 없습니다. 직접 파라미터를 입력해 주세요.")
                st.divider()
            # session_state 초기화 (없을 때만 — 프리셋/저장 후 덮어쓰지 않음)
            _sk = {
                "abuy":  (f"edit_abuy_{key_sfx}",  _a_buy),
                "asell": (f"edit_asell_{key_sfx}", _a_sell),
                "sr":    (f"edit_sr_{key_sfx}",    _sell_ratio),
                "div":   (f"edit_div_{key_sfx}",   _divisions),
                "ndays": (f"edit_ndays_{key_sfx}", f"{_n_days + 1}일"),
            }
            for _k, (_skey, _sval) in _sk.items():
                if _skey not in st.session_state:
                    st.session_state[_skey] = _sval

            _ep1, _ep2, _ep3, _ep4, _ep5 = st.columns(5)
            _new_a_buy  = _ep1.number_input("매수기준 (a_buy)",  step=0.001, format="%.4f",
                                             key=f"edit_abuy_{key_sfx}")
            _new_a_sell = _ep2.number_input("매도기준 (a_sell)", step=0.001, format="%.4f",
                                             key=f"edit_asell_{key_sfx}")
            _new_sr     = _ep3.number_input("매도비율 (%)",      step=10.0,
                                             min_value=0.0, max_value=100.0,
                                             key=f"edit_sr_{key_sfx}")
            _new_div    = _ep4.number_input("분할수",            min_value=1, step=1,
                                             key=f"edit_div_{key_sfx}")
            _ndays_edit = _ep5.select_slider(
                "이동평균 일수", options=[f"{i}일" for i in range(3, 12)],
                key=f"edit_ndays_{key_sfx}",
            )
            if st.button("💾 파라미터 저장", key=f"save_param_{key_sfx}", type="primary",
                         use_container_width=True):
                _new_param = {
                    "a_buy": float(_new_a_buy), "a_sell": float(_new_a_sell),
                    "sell_ratio": float(_new_sr), "divisions": int(_new_div),
                    "n_days": int(_ndays_edit.replace("일", "")) - 1,
                }
                _save_ticker_setting(tk, _new_param)
                # 저장 후 session_state 초기화 → 다음 렌더에서 저장값으로 새로 로드
                for _skey, _ in _sk.values():
                    st.session_state.pop(_skey, None)
                st.success(f"✅ {tk} 파라미터가 저장되었습니다!")
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
    _ss_key = f"os_res_{key_sfx}"   # session_state에 결과 저장할 키
    _btn_label = "🔄 새로고침" if st.session_state.get(_ss_key) else "📋 주문표 로드"
    if st.button(_btn_label, type="primary", key=f"run_os_{key_sfx}"):
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
            n_days=_n_days,
        )
        if res is None:
            st.warning("시뮬레이션 데이터가 없습니다.")
            return
        st.session_state[_ss_key] = res  # 결과 저장 → 탭 이동 후에도 유지
        # 새 날짜 데이터만 누적 저장 (파라미터 바꿔도 기존 기록 불변)
        _save_ticker_daily_history(tk, res.get("daily_log", []))

    res = st.session_state.get(_ss_key)
    if res is None:
        return

    st.markdown(f"**{res['start_date']} ~ {res['end_date']}**")
    # 실현손익 합산 (SELL 거래의 실현손익($) 합계)
    _dl_all = res.get("daily_log", [])
    _realized_pnl = sum(
        r["실현손익($)"] for r in _dl_all
        if r.get("실현손익($)") is not None and r.get("실현손익($)") == r.get("실현손익($)")  # NaN 제외
    )
    _realized_ret_pct = (_realized_pnl / res['initial_capital'] * 100) if res['initial_capital'] else 0.0

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("시작 자본",  f"${res['initial_capital']:,.0f}")
    m2.metric("평가 자산",  f"${res['current_asset']:,.0f}",
              delta=f"CAGR {res['cagr']*100:.2f}%")
    m3.metric("실현손익",   f"${_realized_pnl:+,.2f}",
              delta=f"시작자본 대비 {_realized_ret_pct:+.2f}%")
    m4.metric("현재 DD",    f"{abs(res['current_dd'])*100:.2f}%",
              delta=f"{res['current_dd']*100:.2f}%", delta_color="inverse")
    m5.metric("주식 비중",  f"{res['stock_weight']*100:.1f}%")

    # 오늘의 LOC 주문
    lp, p1, p2 = res["latest_price"], res["p1_now"], res["p2_now"]
    st.subheader(f"📑 오늘의 LOC 주문  ({datetime.today().strftime('%Y-%m-%d')})")
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

    # 일별 매매 상세표 (파일 기반 누적 기록 — 파라미터 변경 무관)
    st.divider()
    st.subheader("📅 일별 매매 상세표")
    _df_hist = _load_ticker_daily_history(tk)
    if _df_hist.empty:
        # 히스토리 파일 없을 때 시뮬레이션 결과를 fallback으로 사용
        _dl = res.get("daily_log", [])
        _df_hist = pd.DataFrame(_dl) if _dl else pd.DataFrame()
    if not _df_hist.empty:
        _df_daily = _df_hist.sort_values("날짜", ascending=False).reset_index(drop=True)
        _bc = (_df_daily["매매"] == "BUY").sum()
        _sc = (_df_daily["매매"] == "SELL").sum()
        _hist_start = _df_daily["날짜"].iloc[-1]
        _hist_end   = _df_daily["날짜"].iloc[0]
        st.caption(f"기록 {_hist_start} ~ {_hist_end} | "
                   f"총 {_bc+_sc}건 (매수 {_bc}회 · 매도 {_sc}회)")
        st.info("📌 이 기록은 실제 주문표 로드 시점에 누적 저장된 데이터입니다. 파라미터를 변경해도 과거 기록은 변경되지 않습니다.", icon="ℹ️")
        _df_show = _df_daily.copy()
        for _col in ["종가(x)", "전날(p1)", "전전날(p2)", "매수경계가", "매도경계가"]:
            _df_show[_col] = _df_show[_col].apply(lambda v: f"${v:,.4f}")
        _df_show["거래금액($)"] = _df_show["거래금액($)"].apply(lambda v: f"${v:,.2f}" if v != 0 else "-")
        _df_show["현금($)"]    = _df_show["현금($)"].apply(lambda v: f"${v:,.2f}")
        _df_show["총자산($)"]  = _df_show["총자산($)"].apply(lambda v: f"${v:,.2f}")
        _df_show["거래주수"]   = _df_show["거래주수"].apply(lambda v: f"{v:,}" if v != 0 else "-")
        # 구버전 캐시 호환: 컬럼 없으면 None으로 채움
        # CSV 로드 시 문자열로 읽힐 수 있으므로 pd.to_numeric으로 강제 변환
        _pnl_amt_raw = _df_daily["실현손익($)"]   if "실현손익($)"   in _df_daily.columns else [None] * len(_df_daily)
        _pnl_pct_raw = _df_daily["실현손익률(%)"] if "실현손익률(%)" in _df_daily.columns else [None] * len(_df_daily)
        _pnl_amt_src = pd.to_numeric(pd.Series(_pnl_amt_raw, index=_df_daily.index), errors="coerce")
        _pnl_pct_src = pd.to_numeric(pd.Series(_pnl_pct_raw, index=_df_daily.index), errors="coerce")
        _df_show["실현손익($)"] = _pnl_amt_src.apply(
            lambda v: f"+${v:,.2f}" if (not pd.isna(v) and v > 0)
               else (f"-${abs(v):,.2f}" if (not pd.isna(v) and v < 0)
               else "-")
        )
        _df_show["실현손익률(%)"] = _pnl_pct_src.apply(
            lambda v: f"{v:+.2f}%" if not pd.isna(v) else "-"
        )
        # 매매 컬럼에 체결가 포함 (원본 _df_daily의 float 종가 사용)
        _df_show["매매"] = _df_daily.apply(
            lambda r: f"BUY (${r['종가(x)']:.2f})"  if r["매매"] == "BUY"
                 else (f"SELL (${r['종가(x)']:.2f})" if r["매매"] == "SELL" else "-"),
            axis=1,
        )

        def _style_daily(row):
            if str(row["매매"]).startswith("BUY"):  return ["background-color: #FFF0F0"] * len(row)
            if str(row["매매"]).startswith("SELL"): return ["background-color: #F0FFF4"] * len(row)
            return [""] * len(row)
        def _style_action(val):
            if str(val).startswith("BUY"):  return "color: #C62828; font-weight: bold"
            if str(val).startswith("SELL"): return "color: #1565C0; font-weight: bold"
            return "color: #999"
        def _style_pnl(val):
            if isinstance(val, str) and val.startswith("+"):  return "color: #1565C0; font-weight: bold"
            if isinstance(val, str) and val.startswith("-"):  return "color: #C62828; font-weight: bold"
            return "color: #999"

        st.dataframe(_df_show.style.apply(_style_daily, axis=1)
                                    .map(_style_action, subset=["매매"])
                                    .map(_style_pnl, subset=["실현손익($)", "실현손익률(%)"]),
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
        st.info("📭 아직 기록된 매매 데이터가 없습니다. 주문표 로드 후 데이터가 누적됩니다.")
  # ← else(종가평균) 블록 종료


with tab3:
  if _is_stdev:
    # ── 표준편차매매 멀티계좌 주문표 ─────────────
    st.subheader("📋 표준편차매매 — 오늘의 주문표")
    st.caption("종목별 계좌를 등록하고 오늘 LOC 매수/매도 주문가를 확인합니다.")

    _sd_all        = _get_sd_ticker_settings()
    _sd_registered = list(_sd_all.keys())

    # ── 계좌 추가 ──────────────────────────────
    with st.expander("➕ 계좌 추가"):
        _sd_preset_tickers = list(_SD_PRESETS_DB.keys()) + ["직접입력"]
        _sd_add_sel = st.selectbox("종목 선택", _sd_preset_tickers, key="sd_add_sel")
        _sd_add_tk  = (_sd_add_sel if _sd_add_sel != "직접입력"
                       else st.text_input("직접 입력 (예: TQQQ)", key="sd_add_custom").upper().strip())

        # 프리셋 버튼
        _sd_ap = _SD_PRESETS_DB.get(_sd_add_tk, [])
        if _sd_ap:
            st.caption("💡 추천 프리셋")
            _sap_cols = st.columns(len(_sd_ap))
            for _sai, (_sapc, _sapp) in enumerate(zip(_sap_cols, _sd_ap)):
                if _sapc.button(_sapp["label"], key=f"sd_addp_{_sai}",
                                help=_sapp["help"], use_container_width=True):
                    st.session_state["sd_add_kb_inp"]  = _sapp["k_buy"]
                    st.session_state["sd_add_ks_inp"]  = _sapp["k_sell"]
                    st.session_state["sd_add_sr_inp"]  = _sapp["sell_ratio"]
                    st.session_state["sd_add_dv_inp"]  = _sapp["divisions"]
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
        _sd_add_sp  = _add_c1.number_input("σ 계산 기간", value=2, min_value=1, max_value=20, step=1,
                                            key="sd_add_sp_inp")
        _sd_add_rn  = _add_c2.number_input("갱신 주기",   value=5, min_value=1, step=1,
                                            key="sd_add_rn_inp")

        if st.button("✅ 계좌 등록", type="primary", use_container_width=True, key="sd_add_reg"):
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
                st.success(f"✅ {_sd_add_tk} 계좌가 등록되었습니다!")
                st.rerun()

    # ── 등록된 계좌 렌더링 ─────────────────────
    if not _sd_registered:
        st.info("📭 등록된 계좌가 없습니다. 위 '계좌 추가'에서 종목을 등록하세요.")
    elif len(_sd_registered) == 1:
        _render_sd_account_tab(_sd_registered[0], _sd_all[_sd_registered[0]], _sd_registered[0])
    else:
        _sd_tabs = st.tabs([f"📊 {_t}" for _t in _sd_registered])
        for _si, _stk in enumerate(_sd_registered):
            with _sd_tabs[_si]:
                _render_sd_account_tab(_stk, _sd_all[_stk], _stk)

  else:
    # ── 종가평균 기존 주문표 ──────────────────
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
            # ── 추천 프리셋 (ticker별) ──
            _ADD_ALL_PRESETS = {
                "SOXL": [
                    {"label": "🚀 공격형",    "a_buy": -0.0048, "a_sell": 0.0087, "divisions": 4,
                     "help": "CAGR 52.98%  |  MDD 28.91%  |  Calmar 1.83\n높은 수익률 추구, 변동성 감수"},
                    {"label": "⚖️ 균형형",   "a_buy": -0.0048, "a_sell": 0.0096, "divisions": 5,
                     "help": "CAGR 46.01%  |  MDD 25.08%  |  Calmar 1.83\n수익률과 안정성의 중간"},
                    {"label": "🛡️ 안정형 ⭐", "a_buy": -0.0063, "a_sell": 0.0075, "divisions": 5,
                     "help": "CAGR 44.12%  |  MDD 22.94%  |  Calmar 1.92\n최저 MDD + 최고 Calmar — 안정적 운용 추천"},
                ],
                "USD": [
                    {"label": "🚀 공격형",    "a_buy": -0.0088, "a_sell": 0.0063, "divisions": 4,
                     "help": "CAGR 37.64%  |  MDD 24.73%  |  Calmar 1.52\n높은 수익률 추구, 변동성 감수"},
                    {"label": "⚖️ 균형형",   "a_buy": -0.0111, "a_sell": 0.0063, "divisions": 4,
                     "help": "CAGR 36.57%  |  MDD 20.51%  |  Calmar 1.78\n수익률과 안정성의 중간"},
                    {"label": "🛡️ 안정형 ⭐", "a_buy": -0.0111, "a_sell": 0.0063, "divisions": 5,
                     "help": "CAGR 31.37%  |  MDD 16.03%  |  Calmar 1.96\n최저 MDD + 최고 Calmar — 안정적 운용 추천"},
                ],
            }
            _ADD_PRESETS = _ADD_ALL_PRESETS.get(_add_tk, [])
            if _ADD_PRESETS:
                st.caption("💡 추천 프리셋 — 버튼 위에 마우스를 올리면 성과 지표를 확인할 수 있습니다.")
                _apc1, _apc2, _apc3 = st.columns(3)
                for _api, (_apcol, _apr) in enumerate(zip([_apc1, _apc2, _apc3], _ADD_PRESETS)):
                    if _apcol.button(_apr["label"], key=f"add_preset_{_api}",
                                     help=_apr["help"], use_container_width=True):
                        st.session_state["add_a_buy"]  = _apr["a_buy"]
                        st.session_state["add_a_sell"] = _apr["a_sell"]
                        st.session_state["add_div"]    = _apr["divisions"]
                        st.rerun()
                st.divider()
            else:
                st.caption("ℹ️ 이 종목은 추천 프리셋이 없습니다. 직접 파라미터를 입력해 주세요.")
                st.divider()
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
                    _err = _save_ticker_setting(_add_tk, {
                        "a_buy": float(_add_a_buy), "a_sell": float(_add_a_sell),
                        "sell_ratio": float(_add_sr), "divisions": int(_add_div),
                        "os_start": str(_add_start), "os_capital": float(_add_capital),
                    })
                    if _err:
                        st.error(f"❌ 계좌 등록 실패: {_err}")
                    else:
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
# 공통 헬퍼 함수 — TAB 5 (표준편차 설정)에서 사용
# ══════════════════════════════════════════════
def _build_sd_order_text(ticker_name: str, k_buy: float, k_sell: float,
                         sigma_period: int, sell_ratio: float, divisions: int,
                         renewal: int, _os_start=None, _os_capital: float = 20000.0) -> str:
    """표준편차매매 오늘의 주문표를 텔레그램 텍스트로 변환."""
    try:
        today    = datetime.today().date()
        buf_s    = (pd.to_datetime(str(_os_start)) - pd.DateOffset(days=90)).strftime("%Y-%m-%d")
        pdf_tg   = load_price_data(ticker_name, buf_s, str(today), "야후파이낸스 (yfinance)", None)
        if pdf_tg is None or pdf_tg.empty:
            return "❌ 가격 데이터를 불러오지 못했습니다."
        res = run_stdev_ordersheet(
            pdf_tg, str(_os_start),
            sigma_period=sigma_period, k_buy=k_buy, k_sell=k_sell,
            sell_ratio=sell_ratio, divisions=divisions, renewal=renewal,
            initial_capital=_os_capital,
        )
        if res is None:
            return "❌ 시뮬레이션 데이터가 없습니다."
        lp        = res["last_close"]
        sigma     = res["sigma_next"]
        today_str = today.strftime("%Y-%m-%d")
        lines = [
            f"📐 <b>표준편차매매 주문표</b> ({today_str})",
            f"종목: {ticker_name}",
            f"직전 종가: ${lp:,.2f}  |  σ = {sigma*100:.3f}%",
            f"━━━━━━━━━━━━━",
            f"🔴 매수 LOC  ${res['next_buy_loc']:,.2f}  (예상 {res['est_buy_qty']:,}주)",
        ]
        if res["holdings"] > 0:
            lines.append(f"🔵 매도 LOC  ${res['next_sell_loc']:,.2f}  (예상 {res['est_sell_qty']:,}주)")
            pnl = (lp / res["avg_cost"] - 1) * 100 if res["avg_cost"] > 0 else 0
            lines += [
                f"━━━━━━━━━━━━━",
                f"📦 보유: {res['holdings']:,}주  |  평단: ${res['avg_cost']:.2f}",
                f"   현재가: ${lp:.2f}  ({pnl:+.2f}%)  |  현금: ${res['cash']:,.2f}",
            ]
        else:
            lines.append(f"📦 보유주식 없음  |  현금: ${res['cash']:,.2f}")
        lines.append("※ 종가 LOC 주문 기준입니다.")
        return "\n".join(lines)
    except Exception as e:
        return f"주문표 생성 오류: {e}"


# ══════════════════════════════════════════════
# 공통 헬퍼 함수 — 표준편차/종가평균 양쪽에서 사용
# ══════════════════════════════════════════════
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


# ══════════════════════════════════════════════
# TAB 4 – 전략 소개 & 성과
# ══════════════════════════════════════════════
with tab4:
  if _is_stdev:
    # ── 표준편차매매 전략 소개 ──────────────────
    st.subheader("📖 표준편차매매 (σ-LOC 전략) 이란?")

    _sdl, _sdr = st.columns([3, 2])
    with _sdl:
        st.markdown("""
#### 전략 개요
**표준편차매매**는 직전 N거래일의 **일간 수익률 표준편차(σ)**를 기준으로
당일 매수/매도 **LOC(Limit-On-Close)** 주문 기준가를 계산하는 퀀트 전략입니다.

주가의 **변동성(σ)** 자체를 매매 기준으로 삼아,
변동이 클 때는 기준가가 벌어지고, 변동이 작을 때는 좁혀지는 **자동 적응형** 전략입니다.

---

#### 매수 룰
- 당일 종가 **≤ 매수LOC** 이면 LOC 매수 체결
- 매수LOC = 전일 종가 × (1 + σ × k_buy)  ← **k_buy < 0이면 하락 시 매수**
- 1회 매수금 = 총투자금 ÷ 분할수(N)

#### 매도 룰
- 보유 중이고 당일 종가 **≥ 매도LOC** 이면 LOC 매도 체결
- 매도LOC = 전일 종가 × (1 + σ × k_sell)  ← **k_sell > 0이면 상승 시 매도**
- 매도 수량 = 보유량 × 매도비율(%)

#### 티어 시스템
| 파라미터 | 설명 |
|---|---|
| k_buy | 매수 LOC 배율 (음수 → 하락폭의 배수만큼 내려가야 매수) |
| k_sell | 매도 LOC 배율 (양수 → 상승폭의 배수만큼 올라야 매도) |
| σ 기간 | 표준편차 계산에 사용할 직전 N일 수익률 |
| 분할수 N | 자산을 N등분하여 1회 매수 금액 결정 |
| 갱신 주기 | 총투자금 갱신 기준 사이클 수 |
| 매도비율 | 보유량 중 매도 비율 (100% = 전량 매도) |
        """)

    with _sdr:
        st.info("""
**LOC 기준가 공식**

```
σ   = stdevp(직전 N일 일간수익률)

매수LOC = 전일종가 × (1 + σ × k_buy)
매도LOC = 전일종가 × (1 + σ × k_sell)
```

- k_buy = -0.65 → σ의 0.65배 하락 시 매수
- k_sell = 0.50 → σ의 0.50배 상승 시 매도
- σ가 클수록(변동성 ↑) LOC 범위가 확대됨
        """)
        st.info("""
**종가평균매매와의 차이**

| 구분 | 종가평균 | 표준편차 |
|---|---|---|
| 기준 | 2일 이동평균 | 변동성(σ) |
| 매도 | 전량 청산 | 비율 청산 |
| 적응성 | 고정 | 자동 조정 |
| 변동성 장세 | 불리 | 유리 |
        """)
        st.success("""
**💡 핵심 장점**

변동성이 높을 때(공포장·급등장)
자동으로 기준가 범위가 넓어져
자연스럽게 매수 기회를 포착합니다.
        """)

    st.divider()

    # ── 전략 인사이트 & 맥락 참고 ──────────────────
    st.subheader("💡 전략 인사이트 & 맥락 참고")
    st.warning("**다음 내용은 표준편차매매 전략 해석입니다. 과거 성과가 미래 수익을 보장하지 않습니다.**")
    with st.container(border=True):
        st.markdown("""
**왜 σ(표준편차)를 기준으로 매매하나?**
- 변동성은 **시장마다 다르고, 시기마다 다릅니다.** 고정된 % 기준가는 급락장에선 너무 작고, 저변동기엔 너무 큽니다.
- σ를 기준으로 삼으면 **공포장에서는 기준가가 넓어지고, 안정장에서는 좁아지는** 자동 조정이 일어납니다.
- 즉, 시장이 소리를 지를수록 매수 기회가 자동으로 더 깊어지고, 조용할 때는 가볍게 매매합니다.

**k_buy / k_sell 부호의 직관적 해석**
- `k_buy < 0` : 전일 종가보다 **하락**해야 매수 (하락 시 추가매수 → 리스크 완화)
- `k_buy > 0` : 전일 종가보다 약간 **상승해도** 매수 (모멘텀 추종)
- `k_sell > 0` : 전일 종가보다 **상승**해야 매도 (상승분 익절)
- k_buy 와 k_sell 이 같은 절댓값이면, 매수 후 같은 방향으로 반등만 해도 즉시 매도 가능

**변동성이 '돈'이 되는 메커니즘**
```
변동성 ↑  →  LOC 범위 확대  →  매수·매도 기회 증가  →  실현 손익 누적
변동성 ↓  →  LOC 범위 축소  →  거래 소강  →  현금 유지 (방어)
```
SOXL 같은 3× 레버리지 ETF는 일간 변동이 크기 때문에, 이 전략의 **자동 적응 능력이 가장 극대화**됩니다.

**이 전략이 유독 강한 구간**
- 📈 **박스권·등락 반복 구간** : 오르면 팔고 내리면 사는 싸이클이 반복 → 수익 누적
- ⚡ **급락 후 빠른 반등 구간** : LOC가 자동 확대되어 저점 매수 → 반등 시 큰 수익
- 🔄 **높은 변동성 + 우상향** : 레버리지 ETF의 전형적 패턴, 최적 조합

**이 전략이 고통받는 구간**
- 📉 **단방향 지속 하락** : 계속 매수하지만 반등 없이 하락 → MDD 확대
- 😴 **초저변동 횡보** : σ가 너무 작아 LOC 범위가 너무 좁음 → 매매 거의 없음
- 🔴 **갭하락 충격** : 전일 σ와 오늘 실제 변동이 심하게 어긋나면 예상 기준가 무의미

**주요 지표 해석**
| 지표 | 기준 | 해석 |
|---|---|---|
| Calmar | > 1.0 우수, > 2.0 최상 | 연복리 ÷ MDD, 위험 대비 수익 |
| Sharpe | > 1.0 우수 | 변동성 대비 초과 수익 |
| Sortino | > 1.5 우수 | 하락 변동성만 기준 → 더 엄격 |
| MDD | 레버리지 ETF는 -50% 이상도 발생 | 심리적으로 견딜 수 있는지 확인 |
| 티어별 평균보유일 | 짧을수록 회전율↑ | 1티어 매도가 길면 반등이 느린 것 |

**실전 운용 주의사항**
- 분할수(N) 모두 체결되면 **현금이 소진** → 추가 하락 시 매수 불가 (가장 큰 리스크)
- LOC 주문 미체결 시 다음 날 다시 계산 (당일 종가 기준 확인 필수)
- σ 계산은 **직전 N일 종가 수익률** 기준 → 오전 중 예측값과 마감 실제값이 다를 수 있음
- **파라미터를 자주 바꾸면 과최적화(overfitting)** → 최소 3개월 이상 유지 권장
        """)

    st.divider()

    # ── 성과 분석 섹션 ────────────────────────────
    st.subheader("📊 전략 성과 분석")
    st.caption("백테스트 탭의 현재 파라미터와 종목 설정을 기반으로 분석합니다.")

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
        # 표준편차 히스토리는 "총자산" 이므로 컬럼명을 맞춰줌
        _col_map = {"총자산": "총자산($)", "예수금": "현금($)"}
        _hist4.rename(columns={k: v for k, v in _col_map.items() if k in _hist4.columns}, inplace=True)
        # 날짜 컬럼 문자열 통일
        _hist4["날짜"] = _hist4["날짜"].astype(str)

        # ── 6대 지표 ──────────────────────────────
        _sharpe4, _sortino4 = compute_sharpe_sortino(_r4["assets"])
        _s1,_s2,_s3,_s4,_s5,_s6 = st.columns(6)
        _s1.metric("전체 CAGR",     f"{_r4['cagr']*100:.2f}%")
        _s2.metric("전체 수익률",   f"{_r4['total_return']*100:+.2f}%")
        _s3.metric("최대 MDD",      f"{_r4['mdd']*100:.2f}%")
        _s4.metric("Calmar Ratio",  f"{_r4['calmar']:.3f}")
        _s5.metric("Sharpe Ratio",  f"{_sharpe4:.3f}")
        _s6.metric("Sortino Ratio", f"{_sortino4:.3f}")
        st.divider()

        # ── 연도별 성과 ───────────────────────────
        st.subheader("📅 연도별 성과")
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

        # ── 월별 히트맵 ───────────────────────────
        st.subheader("🗓️ 월별 수익률 히트맵")
        _mp4 = compute_monthly_pivot(_hist4, init_cap4)
        _fig_m4 = px.imshow(_mp4, color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
                            text_auto=".1f", labels={"x":"월","y":"연도","color":"수익률(%)"},
                            aspect="auto")
        _fig_m4.update_layout(height=max(320, len(_mp4)*38+120),
                               coloraxis_colorbar=dict(title="수익률(%)"))
        st.plotly_chart(_fig_m4, use_container_width=True)
        st.divider()

        # ── 종합 성과 요약 ───────────────────────
        st.subheader("📋 종합 성과 요약")
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

        # ── Buy & Hold 비교 ──────────────────────
        st.subheader("📈 Buy & Hold 비교")
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
                title=f"전략 vs Buy&Hold │ 전략 {_r4['total_return']*100:+.1f}% vs B&H {_bnh_ret4:+.1f}%",
                yaxis_title="자산 ($)", height=380, legend=dict(orientation="h", y=1.08))
            st.plotly_chart(_fig_bnh4, use_container_width=True)
            _bc1,_bc2,_bc3,_bc4 = st.columns(4)
            _bc1.metric("전략 총수익",  f"{_r4['total_return']*100:+.1f}%")
            _bc2.metric("B&H 총수익",   f"{_bnh_ret4:+.1f}%")
            _bc3.metric("전략 CAGR",    f"{_r4['cagr']*100:.1f}%")
            _bc4.metric("B&H CAGR",     f"{_bnh_cagr4:.1f}%")
        st.divider()

        # ── 드로다운 분석 ────────────────────────
        st.subheader("🌊 드로다운 (Underwater) 분석")
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

        # ── 고점 회복력 분석 ─────────────────────
        st.subheader("🔄 고점 회복력 분석")
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
            st.markdown("**📊 고점 회복 구간 시각화**")
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

        # ── 롤링 성과 분석 ───────────────────────
        st.subheader("📉 롤링 성과 분석")
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

        # ── 매도 손익률 분포 ─────────────────────
        st.subheader("📊 매도 손익률 분포")
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

        # ── 현금 활용률 ──────────────────────────
        st.subheader("💵 현금 활용률 & 매매 타이밍 패턴")
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

        # ── 티어별 매수 사이클 분석 ──────────────
        st.subheader("📊 티어별 매수 사이클 분석")
        st.caption(
            f"각 매도 이벤트를 '직전 매도 이후 매수 횟수'로 분류합니다.  \n"
            f"1티어 = 매수 1회 후 즉시 매도(빠른 반등), "
            f"{div4}티어 = 매수 {div4}회 이상 누적 후 매도(깊은 분할매수). "
            f"표준편차매매은 부분 매도(sell_ratio%)이므로 매도 이벤트 단위로 집계합니다."
        )
        _tier_ev4 = run_stdev_tier_analysis(_r4["history"], div4)
        if _tier_ev4:
            _tier_df4 = pd.DataFrame(_tier_ev4)

            # ── 티어별 요약 집계 (1 ~ div4, 마지막 티어는 ≥ div4 포함) ──
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

            # ── 발생횟수 + 평균손익률 이중 축 차트 ──
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

            # ── 전체 매도 이벤트 상세 내역 ──────────────
            with st.expander("📋 전체 매도 이벤트 상세 내역 보기"):
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

        # ── 파라미터 민감도 히트맵 ──────────────
        st.subheader("🎛️ 파라미터 민감도 분석")
        st.caption("현재 k_buy · k_sell 주변의 Calmar Ratio 분포. 과최적화 여부를 확인합니다.")
        with st.expander("🔍 민감도 히트맵 보기 (클릭하여 실행)", expanded=False):
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
                                     title="Calmar Ratio 히트맵 (k_buy × k_sell)")
            _fig_heat4.add_annotation(x=f"{ks:.2f}", y=f"{kb:.2f}",
                                       text="★ 현재", showarrow=True, arrowhead=2,
                                       font=dict(color="white", size=13, family="Arial Black"))
            _fig_heat4.update_layout(height=380)
            st.plotly_chart(_fig_heat4, use_container_width=True)
            st.caption("녹색일수록 Calmar Ratio가 높습니다. 현재 파라미터(★) 주변이 고르게 녹색이면 과최적화 위험이 낮습니다.")
        st.divider()

        # ── 무작위 기간 강건성 분석 ─────────────
        st.subheader("🎲 무작위 기간 강건성 분석")
        st.caption("2014~현재까지 1년(252 거래일) 구간 100개를 무작위 추출하여 백테스트를 반복합니다.")
        with st.expander("🔍 강건성 분석 실행 (클릭)", expanded=False):
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
                st.markdown(f"**📋 요약 통계 (n={_n_mc4})**")
                st.dataframe(pd.DataFrame(_mcs_rows), hide_index=True, use_container_width=True)

                # ── 100구간 상세 결과 표 ──
                with st.expander("📄 100구간 상세 결과 보기"):
                    _detail_rows4 = []
                    for _di4, (_psd4, _ped4) in enumerate(_mc_r4["periods"]):
                        _row4 = {
                            "#": _di4 + 1,
                            "시작일": _psd4, "종료일": _ped4,
                            "전략 수익률(%)": f"{_mc_r4['strat_ret'][_di4]:+.1f}%",
                            "전략 MDD(%)":    f"{_mc_r4['strat_mdd'][_di4]:.1f}%",
                        }
                        if _br_a4 is not None and _di4 < len(_mc_r4["bnh_ret"]):
                            _row4[f"{_act_tk} B&H 수익률(%)"] = f"{_mc_r4['bnh_ret'][_di4]:+.1f}%"
                            _row4[f"{_act_tk} B&H MDD(%)"]    = f"{_mc_r4['bnh_mdd'][_di4]:.1f}%"
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

    # ── 분석 실행 UI ─────────────────────────────
    # ── 분석 종목 선택: 등록된 계좌 우선 ────────────
    _sd_all_for_perf  = _get_sd_ticker_settings()
    _sd_reg_tickers   = list(_sd_all_for_perf.keys())
    # 등록 계좌 + 미등록 기본 후보 합산
    _perf_candidates  = _sd_reg_tickers if _sd_reg_tickers else [ticker]
    for _xt in ["SOXL","TQQQ","FNGU"]:
        if _xt not in _perf_candidates:
            _perf_candidates.append(_xt)

    with st.form(key="sd_perf_form"):
        _pf1, _pf2 = st.columns(2)
        _pf_s  = _pf1.date_input("분석 시작일", value=datetime(2014, 1, 1).date(),
                                   min_value=datetime(2010, 1, 1).date(),
                                   max_value=datetime.today().date(), key="sd_pf_s")
        _pf_e  = _pf2.date_input("분석 종료일",  value=datetime.today().date(),
                                   min_value=datetime(2010, 1, 1).date(),
                                   max_value=datetime.today().date(), key="sd_pf_e")
        _pf3, _pf4 = st.columns(2)
        _pf_cap4 = _pf3.number_input("시작 자본 ($)", value=20000.0, step=1000.0, key="sd_pf_cap")
        _pf_tk4  = _pf4.selectbox("분석 종목", _perf_candidates, key="sd_pf_tk",
                                   help="주문표 탭에 등록된 계좌가 목록 상단에 표시됩니다.")
        _submitted4 = st.form_submit_button("📊 성과 분석 실행", type="primary", use_container_width=True)

    if _submitted4 or st.session_state.get("sd_perf_done"):
        if _submitted4:
            st.session_state["sd_perf_done"] = True

        _act_tk = _pf_tk4 if _submitted4 else st.session_state.get("sd_pf_tk", ticker)

        # ── 파라미터: 등록 계좌 → 사이드바 순서로 우선 적용 ──
        _act_cfg = _sd_all_for_perf.get(_act_tk, {})
        if _act_cfg:
            # 등록된 계좌 파라미터 사용
            _act_kb  = float(_act_cfg.get("k_buy",       sd_k_buy))
            _act_ks  = float(_act_cfg.get("k_sell",      sd_k_sell))
            _act_sp  = int  (_act_cfg.get("sigma_period", sd_sigma_period))
            _act_rn  = int  (_act_cfg.get("renewal",      sd_renewal))
            _act_sr  = float(_act_cfg.get("sell_ratio",  sell_ratio))
            _act_div = int  (_act_cfg.get("divisions",   divisions))
            _act_cap = float(_act_cfg.get("os_capital",  _pf_cap4))
            st.info(
                f"📌 **{_act_tk} 등록 계좌 파라미터** 기준으로 분석합니다.  \n"
                f"k_buy={_act_kb:.2f} · k_sell={_act_ks:.2f} · 매도비율={_act_sr:.0f}% · "
                f"분할수={_act_div} · σ기간={_act_sp}일  \n"
                f"*(파라미터를 변경하려면 주문표 탭 → 파라미터 수정에서 저장하세요)*"
            )
        else:
            # 미등록 종목: 사이드바 파라미터 fallback
            _act_kb  = sd_k_buy
            _act_ks  = sd_k_sell
            _act_sp  = sd_sigma_period
            _act_rn  = sd_renewal
            _act_sr  = sell_ratio
            _act_div = divisions
            _act_cap = _pf_cap4
            st.warning(
                f"⚠️ **{_act_tk}**은 주문표 탭에 등록된 계좌가 없습니다.  \n"
                f"사이드바 파라미터(k_buy={_act_kb:.2f} · k_sell={_act_ks:.2f})로 분석합니다.  \n"
                f"*(주문표 탭에서 계좌를 등록하면 해당 파라미터로 자동 분석됩니다)*"
            )

        _render_sd_perf_analysis(
            _act_tk, _act_kb, _act_ks, _act_sp, _act_rn,
            _act_sr, _act_div, _act_cap, str(_pf_s), str(_pf_e),
        )

        # ── 매매법 간 성과 비교 (자동 실행) ──────────────────────
        st.divider()
        st.subheader("⚔️ 매매법 간 성과 비교")

        # 종가평균매매: 전략 고유의 안정형 표준 파라미터 고정
        # (두 전략은 방식이 다르므로 각자의 기준 파라미터로 독립 비교)
        _cmp_ab  = -0.0063   # a_buy  (-0.63%)
        _cmp_as  =  0.0075   # a_sell (+0.75%)
        _cmp_sr  = 100.0     # 매도비율 100%
        _cmp_div = 5         # 분할수 5회
        _cmp_n   = 2         # 내부값 2 = 3일 이동평균 (표시값-1 저장 방식)
        st.caption(
            f"📐 **표준편차매매**: 현재 설정 파라미터 (k_buy={_act_kb:.4f} · k_sell={_act_ks:.4f} · σ{_act_sp}일)  \n"
            f"📈 **종가평균매매**: 안정형 표준 파라미터 (a_buy=-0.63% · a_sell=+0.75% · 3일 이동평균 · 5분할)  \n"
            f"각 전략의 고유 파라미터 기준으로 **2014-01-01 ~ 오늘** 동일 종목·기간·자본($10,000)을 비교합니다."
        )

        # ── 비교용 기간: 위 분석 폼과 독립적으로 2014-01-01 ~ 오늘 고정 ──
        _cmp_s = "2014-01-01"
        _cmp_e = str(datetime.today().date())
        _cmp_cap = 10000.0   # 종가평균매매 기본 자본과 동일하게 통일

        # 가격 데이터 로드 (비교용 전체 기간)
        _pdf_cmp = load_price_data(_act_tk, _cmp_s, _cmp_e, "야후파이낸스 (yfinance)", None)
        if _pdf_cmp is None or _pdf_cmp.empty:
            st.warning(f"{_act_tk} 가격 데이터를 불러오지 못해 비교 분석을 생략합니다.")
        else:
            with st.spinner("매매법 비교 분석 중..."):
                # ① 표준편차매매
                _cmp_r_sd = run_backtest_stdev(
                    _pdf_cmp, _cmp_s, _cmp_e,
                    sigma_period=_act_sp, k_buy=_act_kb, k_sell=_act_ks,
                    sell_ratio=_act_sr, divisions=_act_div, renewal=_act_rn,
                    initial_capital=_cmp_cap,
                )
                # ② 종가평균매매
                _cmp_r_avg = run_backtest(
                    _pdf_cmp, _cmp_s, _cmp_e,
                    _cmp_ab, _cmp_as, _cmp_sr, _cmp_div, _cmp_cap, n_days=_cmp_n,
                )
                # ③ Buy & Hold
                _cmp_bnh_a, _cmp_bnh_d = compute_bnh(_pdf_cmp, _cmp_s, _cmp_e, _cmp_cap)

            # ── 정규화 수익 곡선 ─────────────────────
            _fig_cmp = go.Figure()
            _cmp_metrics = []

            if _cmp_r_sd:
                _sd_norm = _cmp_r_sd["assets"] / _cmp_cap * 100
                _fig_cmp.add_trace(go.Scatter(
                    x=[str(d.date()) for d in _cmp_r_sd["dates"]], y=_sd_norm.tolist(),
                    name="📐 표준편차매매", line=dict(color="#1565C0", width=2.5),
                ))
                _sh_sd, _so_sd = compute_sharpe_sortino(_cmp_r_sd["assets"])
                _cmp_metrics.append({
                    "전략": "📐 표준편차매매",
                    "파라미터": f"k_buy={_act_kb:.2f} · k_sell={_act_ks:.2f} · σ{_act_sp}일",
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
                    name="📈 종가평균매매", line=dict(color="#E53935", width=2, dash="dash"),
                ))
                _sh_avg, _so_avg = compute_sharpe_sortino(_cmp_r_avg["assets"])
                _cmp_metrics.append({
                    "전략": "📈 종가평균매매",
                    "파라미터": f"a_buy={_cmp_ab*100:.2f}% · a_sell={_cmp_as*100:.2f}% · {_cmp_n+1}일",
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
                    name="🔒 Buy & Hold", line=dict(color="#FB8C00", width=1.5, dash="dot"),
                ))
                _bnh_yrs    = (pd.to_datetime(_cmp_e) - pd.to_datetime(_cmp_s)).days / 365.25
                _bnh_tot    = _cmp_bnh_a[-1] / _cmp_bnh_a[0] - 1
                _bnh_cagr   = ((_cmp_bnh_a[-1] / _cmp_bnh_a[0]) ** (1 / _bnh_yrs) - 1) if _bnh_yrs > 0 else 0
                _bnh_peak   = np.maximum.accumulate(_cmp_bnh_a)
                _bnh_mdd    = float(((np.array(_cmp_bnh_a) - _bnh_peak) / _bnh_peak).min())
                _bnh_calmar = _bnh_cagr / abs(_bnh_mdd) if _bnh_mdd != 0 else 0
                _cmp_metrics.append({
                    "전략": "🔒 Buy & Hold",
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

            # ── 지표 비교 테이블 ────────────────────
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

                # ── 자동 인사이트 코멘트 ─────────────
                if _cmp_r_sd and _cmp_r_avg:
                    _sd_calmar  = _cmp_r_sd["calmar"]
                    _avg_calmar = _cmp_r_avg["calmar"]
                    _sd_ret     = _cmp_r_sd["total_return"] * 100
                    _avg_ret    = _cmp_r_avg["total_return"] * 100
                    _bnh_ret_v  = (_cmp_bnh_a[-1] / _cmp_bnh_a[0] - 1) * 100 if len(_cmp_bnh_a) > 0 else 0

                    if _sd_calmar > _avg_calmar:
                        st.success(
                            f"📐 **표준편차매매** Calmar 우위 ({_sd_calmar:.2f} vs {_avg_calmar:.2f})  \n"
                            f"→ 이 구간은 변동성 장세가 많아 σ 자동 적응 전략이 유리했습니다."
                        )
                    else:
                        st.info(
                            f"📈 **종가평균매매** Calmar 우위 ({_avg_calmar:.2f} vs {_sd_calmar:.2f})  \n"
                            f"→ 이 구간은 추세가 안정적이어서 고정 기준가 전략이 더 잘 맞았습니다."
                        )
                    if _sd_ret > _bnh_ret_v and _avg_ret > _bnh_ret_v:
                        st.success("✅ 두 전략 모두 Buy & Hold 대비 초과 수익을 기록했습니다.")
                    elif _sd_ret < _bnh_ret_v and _avg_ret < _bnh_ret_v:
                        st.warning("⚠️ 두 전략 모두 Buy & Hold에 미치지 못했습니다. 강한 단방향 상승장 구간입니다.")

  else:
    # ── 종가평균매매 전략 소개 & 성과 ─────────────
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
    - **기본 권장: 5분할** — 자산을 5등분하여 최대 5번까지 분할 매수

    #### 매도 룰
    - 보유 중이고 당일 종가 **≥ 매도경계가** 이면 LOC 매도 체결
    - 몇 티어를 매수했든 **보유 수량 100% 전량 매도** (한 번에 완전 청산)

    #### 포지션 관리
    | 파라미터 | 설명 |
    |---|---|
    | a_buy | 매수경계가 조정값 (음수 → 평균 이하에서 매수) |
    | a_sell | 매도경계가 조정값 (양수 → 평균 이상에서 매도) |
    | 분할수 N | 자산을 N등분하여 1회 매수 금액 결정 |
    | 매도비율 | 보유 수량 중 매도 비율 (기본 100% — 전량 매도) |
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

    def _compute_recovery_table(assets, dates, threshold=10.0):
        """고점 대비 threshold% 이상 하락 에피소드별 회복력 분석 테이블 반환."""
        records = []
        n = len(assets)
        if n == 0:
            return records
        peak_val  = float(assets[0])
        peak_idx  = 0
        in_dd     = False
        trough_val = peak_val
        trough_idx = 0
        for i in range(1, n):
            curr   = float(assets[i])
            dd_pct = (curr - peak_val) / peak_val * 100
            if not in_dd:
                if curr > peak_val:
                    peak_val  = curr
                    peak_idx  = i
                elif dd_pct <= -threshold:
                    in_dd      = True
                    trough_val = curr
                    trough_idx = i
            else:
                if curr < trough_val:
                    trough_val = curr
                    trough_idx = i
                if curr >= peak_val:
                    drop_rate = (trough_val - peak_val) / peak_val * 100
                    records.append({
                        "고점":         str(dates[peak_idx].date()),
                        "고점 평가액":   round(peak_val),
                        "최대하락 시점": str(dates[trough_idx].date()),
                        "저점 평가액":  round(trough_val),
                        "하락율(%)":    round(drop_rate, 2),
                        "회복 시점":    str(dates[i].date()),
                        "기간(일)":     (dates[i] - dates[peak_idx]).days,
                    })
                    in_dd      = False
                    peak_val   = curr
                    peak_idx   = i
                    trough_val = curr
                    trough_idx = i
        if in_dd:
            drop_rate = (trough_val - peak_val) / peak_val * 100
            records.append({
                "고점":         str(dates[peak_idx].date()),
                "고점 평가액":   round(peak_val),
                "최대하락 시점": str(dates[trough_idx].date()),
                "저점 평가액":  round(trough_val),
                "하락율(%)":    round(drop_rate, 2),
                "회복 시점":    "미회복",
                "기간(일)":     (dates[-1] - dates[peak_idx]).days,
            })
        return records

    def _render_perf_analysis(tk, a_b, a_s, sr, div, init_cap, s_date, e_date):
        """ticker 하나의 성과 분석 전체를 렌더링."""
        with st.spinner(f"{tk} 데이터 로드 및 분석 중..."):
            _pdf = load_price_data(tk, s_date, e_date, "야후파이낸스 (yfinance)", None)
        if _pdf.empty:
            st.error(f"{tk}: 가격 데이터를 불러오지 못했습니다.")
            return

        _res = run_backtest(_pdf, s_date, e_date, a_b, a_s, sr, div, init_cap, return_history=True, n_days=n_days)
        if _res is None:
            st.warning(f"{tk}: 선택된 기간 내 거래 데이터가 없습니다.")
            return

        _hist = _res["history"]

        _sharpe, _sortino = compute_sharpe_sortino(_res["assets"])
        sm1, sm2, sm3, sm4, sm5, sm6 = st.columns(6)
        sm1.metric("전체 CAGR",    f"{_res['cagr']*100:.2f}%")
        sm2.metric("전체 수익률",  f"{_res['total_return']*100:+.2f}%")
        sm3.metric("최대 MDD",     f"{_res['mdd']*100:.2f}%")
        sm4.metric("Calmar Ratio", f"{_res['calmar']:.3f}")
        sm5.metric("Sharpe Ratio", f"{_sharpe:.3f}")
        sm6.metric("Sortino Ratio",f"{_sortino:.3f}")
        st.divider()

        st.subheader("📅 연도별 성과")
        _annual = compute_annual_stats(_hist, init_cap)
        def _color_ret(val):
            if isinstance(val, (int, float)):
                if val > 0: return "color: #2e7d32; font-weight:bold"
                if val < 0: return "color: #c62828; font-weight:bold"
            return ""
        st.dataframe(
            _annual.style.map(_color_ret, subset=["연간수익률(%)"])
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

        # ── Buy & Hold 비교 ─────────────────────────────────
        st.subheader("📈 Buy & Hold 비교")
        st.caption("같은 기간 종목을 단순 보유했을 때와 전략 성과를 비교합니다.")
        _bnh_assets, _bnh_dates = compute_bnh(_pdf, s_date, e_date, init_cap)
        if len(_bnh_assets) > 0:
            _str_dates = [str(d.date()) for d in _res["dates"]]
            _bnh_dates_str = [str(d.date()) for d in _bnh_dates]
            _fig_bnh = go.Figure()
            _fig_bnh.add_trace(go.Scatter(
                x=_str_dates, y=_res["assets"].tolist(),
                name="종가평균매매 전략", line=dict(color="#1565C0", width=2),
            ))
            _fig_bnh.add_trace(go.Scatter(
                x=_bnh_dates_str, y=_bnh_assets.tolist(),
                name="Buy & Hold", line=dict(color="#EF5350", width=2, dash="dot"),
            ))
            _fig_bnh.add_hline(y=init_cap, line_dash="dash", line_color="#aaa",
                               annotation_text="시작 자본")
            _bnh_ret  = (_bnh_assets[-1] / _bnh_assets[0] - 1) * 100
            _bnh_yrs  = (pd.to_datetime(e_date) - pd.to_datetime(s_date)).days / 365.25
            _bnh_cagr = ((_bnh_assets[-1] / _bnh_assets[0]) ** (1 / _bnh_yrs) - 1) * 100 if _bnh_yrs > 0 else 0
            _fig_bnh.update_layout(
                title=f"전략 vs Buy&Hold │ 전략 수익 {_res['total_return']*100:+.1f}% vs B&H {_bnh_ret:+.1f}%",
                yaxis_title="자산 ($)", height=380,
                legend=dict(orientation="h", y=1.08),
            )
            st.plotly_chart(_fig_bnh, use_container_width=True)
            _bc1, _bc2, _bc3, _bc4 = st.columns(4)
            _bc1.metric("전략 총수익",    f"{_res['total_return']*100:+.1f}%")
            _bc2.metric("B&H 총수익",     f"{_bnh_ret:+.1f}%")
            _bc3.metric("전략 CAGR",      f"{_res['cagr']*100:.1f}%")
            _bc4.metric("B&H CAGR",       f"{_bnh_cagr:.1f}%")
        st.divider()

        # ── 드로다운 (Underwater) 차트 ───────────────────────
        st.subheader("🌊 드로다운 (Underwater) 분석")
        st.caption("고점 대비 현재 손실 비율 추이. 얼마나 깊이, 얼마나 오래 손실 구간에 있었는지 보여줍니다.")
        _peak_arr = np.maximum.accumulate(_res["assets"])
        _dd_arr   = (_res["assets"] - _peak_arr) / _peak_arr * 100
        _str_dates2 = [str(d.date()) for d in _res["dates"]]
        _fig_dd = go.Figure()
        _fig_dd.add_trace(go.Scatter(
            x=_str_dates2, y=_dd_arr.tolist(),
            fill="tozeroy", name="드로다운(%)",
            line=dict(color="#EF5350", width=1),
            fillcolor="rgba(239,83,80,0.25)",
        ))
        _fig_dd.add_hline(y=0, line_color="#888", line_width=1)
        _fig_dd.update_layout(
            yaxis_title="드로다운 (%)", height=300,
            yaxis=dict(tickformat=".1f"),
        )
        st.plotly_chart(_fig_dd, use_container_width=True)
        # 드로다운 구간 TOP5
        _dd_series = pd.Series(_dd_arr, index=_res["dates"])
        _in_dd = False; _dd_start = None; _dd_periods = []
        for _di, (_ddate, _dval) in enumerate(_dd_series.items()):
            if _dval < 0 and not _in_dd:
                _in_dd = True; _dd_start = _ddate; _dd_peak_val = _res["assets"][_di]
            elif _dval == 0 and _in_dd:
                _in_dd = False
                _sub_dd = _dd_series[_dd_start:_ddate]
                _dd_periods.append({
                    "시작일": str(_dd_start.date()), "회복일": str(_ddate.date()),
                    "기간(일)": (_ddate - _dd_start).days,
                    "최대낙폭(%)": round(float(_sub_dd.min()), 2),
                })
        if _dd_periods:
            _dd_df = pd.DataFrame(_dd_periods).nsmallest(5, "최대낙폭(%)").reset_index(drop=True)
            _dd_df.index += 1
            st.markdown("**Top 5 최대 낙폭 구간**")
            st.dataframe(_dd_df.style.format({"최대낙폭(%)": "{:.2f}%"}),
                         hide_index=False, use_container_width=True)
        st.divider()

        # ── 고점 회복력 분석 ──────────────────────────────────
        st.subheader("🔄 고점 회복력 분석")
        st.caption("고점 대비 10% 이상 하락이 발생한 모든 에피소드와 회복까지 걸린 기간을 정리합니다.")

        _rec_records = _compute_recovery_table(_res["assets"], _res["dates"], threshold=10.0)

        if _rec_records:
            _rec_df = pd.DataFrame(_rec_records).reset_index(drop=True)
            _rec_df.index += 1

            # 하락율 절댓값 표시용 복사본
            _rec_df_show = _rec_df.copy()
            _rec_df_show["고점 평가액"]  = _rec_df_show["고점 평가액"].apply(lambda v: f"${v:,}")
            _rec_df_show["저점 평가액"]  = _rec_df_show["저점 평가액"].apply(lambda v: f"${v:,}")
            _rec_df_show["하락율(%)"]    = _rec_df_show["하락율(%)"].apply(lambda v: f"{abs(v):.2f}%")
            _rec_df_show["기간(일)"]     = _rec_df_show["기간(일)"].apply(
                lambda v: f"{v}일" if isinstance(v, (int, float)) else str(v))

            # 미회복 행 강조
            def _highlight_unrecovered(row):
                return ["background-color: #fff3e0"] * len(row) \
                    if row["회복 시점"] == "미회복" else [""] * len(row)

            st.dataframe(
                _rec_df_show.style.apply(_highlight_unrecovered, axis=1),
                hide_index=False,
                use_container_width=True,
            )

            # 요약 지표
            _rc1, _rc2, _rc3, _rc4 = st.columns(4)
            _completed = [r for r in _rec_records if r["회복 시점"] != "미회복"]
            _avg_days   = int(np.mean([r["기간(일)"] for r in _completed])) if _completed else 0
            _max_days   = max([r["기간(일)"] for r in _completed], default=0)
            _max_drop   = min([r["하락율(%)"] for r in _rec_records])
            _rc1.metric("총 에피소드",        f"{len(_rec_records)}회")
            _rc2.metric("평균 회복 기간",     f"{_avg_days}일" if _completed else "-")
            _rc3.metric("최장 회복 기간",     f"{_max_days}일" if _completed else "-")
            _rc4.metric("최대 낙폭",          f"{abs(_max_drop):.2f}%")

            st.divider()

            # ── 회복력 차트 (전략 + B&H + 하락 음영 + 마커) ──
            st.markdown("**📊 고점 회복 구간 시각화**")
            st.caption("노란 음영: 10% 이상 하락 구간 / 초록 점: 고점 / 빨간 점: 저점")

            _fig_rec = go.Figure()

            # 전략 라인
            _str_dates_r = [str(d.date()) for d in _res["dates"]]
            _fig_rec.add_trace(go.Scatter(
                x=_str_dates_r, y=_res["assets"].tolist(),
                name="종가평균매매 전략",
                line=dict(color="#1565C0", width=2),
            ))

            # B&H 라인
            _bnh_a2, _bnh_d2 = compute_bnh(_pdf, s_date, e_date, init_cap)
            if len(_bnh_a2) > 0:
                _fig_rec.add_trace(go.Scatter(
                    x=[str(d.date()) for d in _bnh_d2], y=_bnh_a2.tolist(),
                    name="Buy & Hold",
                    line=dict(color="#FB8C00", width=1.5, dash="dot"),
                ))

            # 날짜 → 인덱스 맵
            _date_str_map = {str(d.date()): i for i, d in enumerate(_res["dates"])}

            for _ep in _rec_records:
                _xs = _ep["고점"]
                _xe = _ep["회복 시점"] if _ep["회복 시점"] != "미회복" else _str_dates_r[-1]
                # 노란 음영 (drawdown 구간)
                _fig_rec.add_vrect(
                    x0=_xs, x1=_xe,
                    fillcolor="rgba(255,236,153,0.35)",
                    layer="below", line_width=0,
                )
                # 고점 마커 (초록)
                _pi = _date_str_map.get(_xs)
                if _pi is not None:
                    _fig_rec.add_trace(go.Scatter(
                        x=[_xs], y=[float(_res["assets"][_pi])],
                        mode="markers",
                        marker=dict(color="#43A047", size=8, symbol="circle"),
                        showlegend=False, hovertemplate=f"고점: {_xs}<br>${float(_res['assets'][_pi]):,.0f}<extra></extra>",
                    ))
                # 저점 마커 (빨강)
                _ti = _date_str_map.get(_ep["최대하락 시점"])
                if _ti is not None:
                    _fig_rec.add_trace(go.Scatter(
                        x=[_ep["최대하락 시점"]], y=[float(_res["assets"][_ti])],
                        mode="markers",
                        marker=dict(color="#E53935", size=8, symbol="circle"),
                        showlegend=False, hovertemplate=f"저점: {_ep['최대하락 시점']}<br>${float(_res['assets'][_ti]):,.0f}<extra></extra>",
                    ))

            # 범례용 더미 트레이스
            _fig_rec.add_trace(go.Scatter(
                x=[None], y=[None], mode="markers",
                marker=dict(color="#43A047", size=8), name="고점",
            ))
            _fig_rec.add_trace(go.Scatter(
                x=[None], y=[None], mode="markers",
                marker=dict(color="#E53935", size=8), name="저점",
            ))

            _fig_rec.update_layout(
                yaxis_title="자산 ($)",
                height=420,
                legend=dict(orientation="h", y=1.08),
                hovermode="x unified",
            )
            st.plotly_chart(_fig_rec, use_container_width=True)
        else:
            st.info(f"분석 기간 중 10% 이상 하락 에피소드가 없습니다.")

        st.divider()

        # ── 롤링 성과 분석 ───────────────────────────────────
        st.subheader("📉 롤링 성과 분석")
        st.caption("구간별 성과 추이. 특정 시기에만 좋은 게 아닌지 검증합니다.")
        _roll_tabs = st.tabs(["1년 롤링", "2년 롤링", "3년 롤링"])
        for _rwin, _rtab in zip([252, 504, 756], _roll_tabs):
            with _rtab:
                _rc, _rm = compute_rolling_perf(_res["assets"], _rwin)
                _valid = ~np.isnan(_rc)
                if _valid.sum() > 0:
                    _rdates = [str(d.date()) for d, v in zip(_res["dates"], _valid) if v]
                    _fig_roll = go.Figure()
                    _fig_roll.add_trace(go.Scatter(
                        x=_rdates, y=_rc[_valid].tolist(),
                        name="롤링 CAGR(%)", line=dict(color="#1565C0", width=2), yaxis="y1",
                    ))
                    _fig_roll.add_trace(go.Scatter(
                        x=_rdates, y=_rm[_valid].tolist(),
                        name="롤링 MDD(%)", line=dict(color="#EF5350", width=1.5, dash="dot"), yaxis="y2",
                    ))
                    _fig_roll.add_hline(y=0, line_dash="dash", line_color="#aaa", yref="y1")
                    _fig_roll.update_layout(
                        yaxis=dict(title="롤링 CAGR (%)", side="left"),
                        yaxis2=dict(title="롤링 MDD (%)", side="right", overlaying="y"),
                        legend=dict(orientation="h", y=1.08),
                        height=340,
                    )
                    st.plotly_chart(_fig_roll, use_container_width=True)
                    _r1, _r2, _r3 = st.columns(3)
                    _r1.metric("평균 CAGR", f"{np.nanmean(_rc):+.1f}%")
                    _r2.metric("최고 CAGR", f"{np.nanmax(_rc):+.1f}%")
                    _r3.metric("최저 CAGR", f"{np.nanmin(_rc):+.1f}%")
                else:
                    st.info(f"분석 기간이 {_rwin // 252}년보다 짧아 롤링 분석이 불가합니다.")
        st.divider()

        # ── 수익률 분포 분석 ─────────────────────────────────
        st.subheader("📊 매도 손익률 분포")
        st.caption("매도 시마다 발생한 손익률의 분포. 수익/손실의 패턴을 분석합니다.")
        _pnl_list = _res.get("sell_pnls_list", [])
        if _pnl_list:
            _pnl_arr = np.array(_pnl_list)
            _skew    = float(pd.Series(_pnl_arr).skew())
            _kurt    = float(pd.Series(_pnl_arr).kurtosis())
            _fig_pnl = go.Figure()
            _fig_pnl.add_trace(go.Histogram(
                x=_pnl_arr.tolist(), nbinsx=30,
                marker_color=["#EF5350" if v < 0 else "#43A047" for v in _pnl_arr],
                name="손익률 빈도",
            ))
            _fig_pnl.add_vline(x=0, line_dash="dash", line_color="#333")
            _fig_pnl.add_vline(x=float(np.mean(_pnl_arr)), line_dash="dot",
                               line_color="#1565C0",
                               annotation_text=f"평균 {np.mean(_pnl_arr):+.2f}%",
                               annotation_position="top right")
            _fig_pnl.update_layout(
                xaxis_title="손익률 (%)", yaxis_title="빈도 (회)", height=320,
            )
            st.plotly_chart(_fig_pnl, use_container_width=True)
            _pd1, _pd2, _pd3, _pd4 = st.columns(4)
            _pd1.metric("평균 손익률",  f"{np.mean(_pnl_arr):+.2f}%")
            _pd2.metric("중앙값",       f"{np.median(_pnl_arr):+.2f}%")
            _pd3.metric("왜도 (Skew)",  f"{_skew:.3f}",
                        help="양수=우측 꼬리(큰 수익 가끔), 음수=좌측 꼬리(큰 손실 가끔)")
            _pd4.metric("첨도 (Kurt)",  f"{_kurt:.3f}",
                        help="높을수록 극단값(큰 수익/손실) 빈도 높음")
        else:
            st.info("매도 이력이 없어 분포 분석이 불가합니다.")
        st.divider()

        # ── 현금 활용률 & 매매 타이밍 ────────────────────────
        st.subheader("💵 현금 활용률 & 매매 타이밍 패턴")
        _cash_series = _res.get("cash_series", np.array([]))
        if len(_cash_series) > 0 and len(_res["assets"]) > 0:
            _inv_ratio = (1 - _cash_series / _res["assets"]) * 100  # 투자 비율(%)
            _cu1, _cu2, _cu3 = st.columns(3)
            _cu1.metric("평균 투자 비율", f"{np.mean(_inv_ratio):.1f}%",
                        help="현금이 아닌 주식에 투자된 비율의 평균")
            _cu2.metric("최대 투자 비율", f"{np.max(_inv_ratio):.1f}%")
            _cu3.metric("현금 보유 비율", f"{100 - np.mean(_inv_ratio):.1f}%")
            # 일별 원본 시리즈 (실제 피크 100% 보존)
            _ratio_s = pd.Series(
                _inv_ratio,
                index=pd.to_datetime([d.date() for d in _res["dates"]])
            )
            _total_days = (_ratio_s.index[-1] - _ratio_s.index[0]).days
            # 추세선용 이동평균 (원본은 건드리지 않음)
            if _total_days > 365:
                _win, _win_label = 5, "5일 이동평균 추세선"
            else:
                _win, _win_label = 0, ""
            _trend_s = _ratio_s.rolling(_win, min_periods=1).mean() if _win > 0 else None

            _x_dates   = [str(d.date()) for d in _ratio_s.index]
            _stk_ratio = _ratio_s.tolist()
            _ones      = [100.0] * len(_x_dates)

            _fig_cu = go.Figure()
            # ① 현금(회색) — 100% 배경
            _fig_cu.add_trace(go.Scatter(
                x=_x_dates, y=_ones,
                name="현금",
                mode="lines", line=dict(width=0),
                fill="tozeroy",
                fillcolor="rgba(200,200,200,0.6)",
                hoverinfo="skip",
            ))
            # ② 주식(ETF, 노란) — 일별 원본 영역 (100% 피크 보존)
            _fig_cu.add_trace(go.Scatter(
                x=_x_dates, y=_stk_ratio,
                name="주식(ETF)",
                mode="lines", line=dict(width=0),
                fill="tozeroy",
                fillcolor="rgba(255,179,0,0.75)",
                hovertemplate="%{x}<br>주식(ETF): %{y:.1f}%<extra></extra>",
            ))
            # ③ 추세선 — 이동평균 라인 (긴 기간일 때만)
            if _trend_s is not None:
                _fig_cu.add_trace(go.Scatter(
                    x=_x_dates, y=_trend_s.tolist(),
                    name=_win_label,
                    mode="lines",
                    line=dict(color="rgba(180,80,0,0.85)", width=1.5, dash="solid"),
                    hoverinfo="skip",
                ))
            _fig_cu.update_layout(
                yaxis_title="비율 (%)",
                yaxis=dict(
                    range=[0, 100],
                    tickvals=[0, 20, 40, 60, 80, 100],
                    ticktext=["0%", "20%", "40%", "60%", "80%", "100%"],
                ),
                height=300,
                legend=dict(
                    orientation="h", x=1.0, y=1.02,
                    xanchor="right", yanchor="bottom",
                    bgcolor="rgba(255,255,255,0.85)",
                    bordercolor="#ccc", borderwidth=1,
                ),
                hovermode="x unified",
                margin=dict(l=60, r=20, t=20, b=40),
            )
            st.plotly_chart(_fig_cu, use_container_width=True)
            if _win > 0:
                st.caption(f"※ 영역: 일별 실제 투자비율 (100% 피크 보존) | 주황 선: {_win_label}")

        if not _hist.empty:
            _buy_hist  = _hist[_hist["매매"] == "BUY"].copy()
            _sell_hist = _hist[_hist["매매"] == "SELL"].copy()
            if not _buy_hist.empty:
                _buy_hist["요일"] = pd.to_datetime(_buy_hist["날짜"]).dt.day_name()
                _sell_hist["요일"] = pd.to_datetime(_sell_hist["날짜"]).dt.day_name()
                _buy_hist["월"]  = pd.to_datetime(_buy_hist["날짜"]).dt.month
                _sell_hist["월"] = pd.to_datetime(_sell_hist["날짜"]).dt.month
                _dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday"]
                _buy_dow   = _buy_hist["요일"].value_counts().reindex(_dow_order, fill_value=0)
                _sell_dow  = _sell_hist["요일"].value_counts().reindex(_dow_order, fill_value=0)
                _dow_labels = ["월","화","수","목","금"]
                _fig_dow = go.Figure()
                _fig_dow.add_trace(go.Bar(x=_dow_labels, y=_buy_dow.values.tolist(),
                                          name="매수", marker_color="#EF5350"))
                _fig_dow.add_trace(go.Bar(x=_dow_labels, y=_sell_dow.values.tolist(),
                                          name="매도", marker_color="#43A047"))
                _fig_dow.update_layout(barmode="group", title="요일별 매매 빈도",
                                       yaxis_title="횟수", height=300)
                _buy_mon  = _buy_hist["월"].value_counts().sort_index()
                _sell_mon = _sell_hist["월"].value_counts().sort_index()
                _fig_mon = go.Figure()
                _fig_mon.add_trace(go.Bar(x=[f"{m}월" for m in _buy_mon.index],
                                          y=_buy_mon.values.tolist(),
                                          name="매수", marker_color="#EF5350"))
                _fig_mon.add_trace(go.Bar(x=[f"{m}월" for m in _sell_mon.index],
                                          y=_sell_mon.values.tolist(),
                                          name="매도", marker_color="#43A047"))
                _fig_mon.update_layout(barmode="group", title="월별 매매 빈도",
                                       yaxis_title="횟수", height=300)
                _tc1, _tc2 = st.columns(2)
                with _tc1:
                    st.plotly_chart(_fig_dow, use_container_width=True)
                with _tc2:
                    st.plotly_chart(_fig_mon, use_container_width=True)
        st.divider()

        # ── 파라미터 민감도 히트맵 ───────────────────────────
        st.subheader("🎛️ 파라미터 민감도 분석")
        st.caption("현재 a_buy · a_sell 주변의 Calmar Ratio 분포. 과최적화 여부를 확인합니다.")
        with st.expander("🔍 민감도 히트맵 보기 (클릭하여 실행)", expanded=False):
            _n_steps = 5
            _buy_range  = np.linspace(a_b - 0.005, a_b + 0.005, _n_steps)
            _sell_range = np.linspace(a_s - 0.005, a_s + 0.005, _n_steps)
            _heat = np.zeros((_n_steps, _n_steps))
            with st.spinner("민감도 분석 중... (25회 시뮬레이션)"):
                for _bi, _bv in enumerate(_buy_range):
                    for _si, _sv in enumerate(_sell_range):
                        _hr = run_backtest(_pdf, s_date, e_date, _bv, _sv, sr, div, init_cap, n_days=n_days)
                        _heat[_bi][_si] = _hr["calmar"] if _hr else 0.0
            _buy_labels  = [f"{v*100:.2f}%" for v in _buy_range]
            _sell_labels = [f"{v*100:.2f}%" for v in _sell_range]
            _fig_heat = px.imshow(
                _heat, x=_sell_labels, y=_buy_labels,
                color_continuous_scale="RdYlGn",
                labels={"x": "a_sell", "y": "a_buy", "color": "Calmar"},
                text_auto=".2f", aspect="auto",
                title="Calmar Ratio 히트맵 (a_buy × a_sell)",
            )
            _fig_heat.add_annotation(
                x=f"{a_s*100:.2f}%", y=f"{a_b*100:.2f}%",
                text="★ 현재", showarrow=True, arrowhead=2,
                font=dict(color="white", size=13, family="Arial Black"),
            )
            _fig_heat.update_layout(height=380)
            st.plotly_chart(_fig_heat, use_container_width=True)
            st.caption("녹색일수록 Calmar Ratio가 높습니다. 현재 파라미터(★) 주변이 고르게 녹색이면 과최적화 위험이 낮습니다.")
        st.divider()

        # ── 무작위 기간 강건성 분석 ──────────────────────────────
        st.subheader("🎲 무작위 기간 강건성 분석")
        st.caption(
            "2014~현재까지 1년(252 거래일) 구간 100개를 무작위 추출하여 백테스트를 반복합니다. "
            "시작 시점과 무관하게 전략이 일관된 성과를 내는지 확인합니다."
        )
        with st.expander("🔍 강건성 분석 실행 (클릭)", expanded=False):
            if st.button("▶ 무작위 100구간 분석 시작", key=f"mc_run_{tk}"):
                st.session_state[f"mc_result_{tk}"] = None  # 초기화
                with st.spinner("전체 가격 데이터 로드 중..."):
                    _mc_pdf = load_price_data(tk, "2014-01-01", str(pd.Timestamp.today().date()),
                                              "야후파이낸스 (yfinance)", None)
                if _mc_pdf.empty:
                    st.error("가격 데이터를 불러오지 못했습니다.")
                else:
                    _mc_closes = _mc_pdf["Close"].dropna()
                    _mc_idx    = _mc_closes.index
                    _WINDOW    = 252  # 1년 거래일
                    _mc_valid_starts = [i for i in range(len(_mc_idx) - _WINDOW)]
                    if len(_mc_valid_starts) < 100:
                        st.warning("데이터가 100구간 분석에 충분하지 않습니다.")
                    else:
                        import random as _rand
                        _rand.seed(None)  # 매 실행마다 다른 무작위
                        _mc_chosen = _rand.sample(_mc_valid_starts, 100)
                        _mc_strat_ret  = []
                        _mc_strat_mdd  = []
                        _mc_bnh_ret    = []
                        _mc_bnh_mdd    = []
                        _mc_periods    = []
                        _mc_prog = st.progress(0, text="시뮬레이션 중...")
                        for _ci, _si in enumerate(_mc_chosen):
                            _s_dt = str(_mc_idx[_si].date())
                            _e_dt = str(_mc_idx[_si + _WINDOW - 1].date())
                            _r = run_backtest(_mc_pdf, _s_dt, _e_dt, a_b, a_s, sr, div, init_cap, n_days=n_days)
                            if _r:
                                _mc_strat_ret.append(round(_r["total_return"] * 100, 2))
                                _mc_strat_mdd.append(round(abs(_r["mdd"]) * 100, 2))
                                _ba, _ = compute_bnh(_mc_pdf, _s_dt, _e_dt, init_cap)
                                if len(_ba) > 0:
                                    _bnh_tr = (_ba[-1] / _ba[0] - 1) * 100
                                    _bnh_pk = np.maximum.accumulate(_ba)
                                    _bnh_md = abs(float(((np.array(_ba) - _bnh_pk) / _bnh_pk).min())) * 100
                                    _mc_bnh_ret.append(round(_bnh_tr, 2))
                                    _mc_bnh_mdd.append(round(_bnh_md, 2))
                                _mc_periods.append((_s_dt, _e_dt))
                            _mc_prog.progress((_ci + 1) / 100, text=f"시뮬레이션 중... {_ci+1}/100")
                        _mc_prog.empty()
                        st.session_state[f"mc_result_{tk}"] = {
                            "strat_ret": _mc_strat_ret, "strat_mdd": _mc_strat_mdd,
                            "bnh_ret":   _mc_bnh_ret,   "bnh_mdd":   _mc_bnh_mdd,
                            "periods":   _mc_periods,
                            "bnh_ret_per":  _mc_bnh_ret,
                            "bnh_mdd_per":  _mc_bnh_mdd,
                        }

            _mc_res = st.session_state.get(f"mc_result_{tk}")
            if _mc_res:
                _sr_arr = np.array(_mc_res["strat_ret"])
                _sm_arr = np.array(_mc_res["strat_mdd"])
                _br_arr = np.array(_mc_res["bnh_ret"]) if _mc_res["bnh_ret"] else None
                _bm_arr = np.array(_mc_res["bnh_mdd"]) if _mc_res["bnh_mdd"] else None
                _n_mc   = len(_sr_arr)
                _strat_label = "종가평균 전략"
                _bnh_label   = f"{tk} B&H"

                # ── 요약 통계 표 ──
                def _mc_stats(arr, label):
                    return {
                        "구분": label,
                        "평균":       f"{np.mean(arr):+.1f}%",
                        "중앙값":     f"{np.median(arr):+.1f}%",
                        "표준편차":   f"{np.std(arr):.1f}%",
                        "최솟값":     f"{np.min(arr):+.1f}%",
                        "최댓값":     f"{np.max(arr):+.1f}%",
                        "양(+) 비율": f"{(arr > 0).sum() / len(arr) * 100:.0f}%",
                    }
                _mc_stat_rows = [_mc_stats(_sr_arr, f"{_strat_label} (1년 수익률)")]
                if _br_arr is not None:
                    _mc_stat_rows.append(_mc_stats(_br_arr, f"{_bnh_label} (1년 수익률)"))
                _mc_stat_rows.append({
                    "구분": f"{_strat_label} (MDD)",
                    "평균": f"{np.mean(_sm_arr):.1f}%", "중앙값": f"{np.median(_sm_arr):.1f}%",
                    "표준편차": f"{np.std(_sm_arr):.1f}%", "최솟값": f"{np.min(_sm_arr):.1f}%",
                    "최댓값": f"{np.max(_sm_arr):.1f}%", "양(+) 비율": "-",
                })
                if _bm_arr is not None:
                    _mc_stat_rows.append({
                        "구분": f"{_bnh_label} (MDD)",
                        "평균": f"{np.mean(_bm_arr):.1f}%", "중앙값": f"{np.median(_bm_arr):.1f}%",
                        "표준편차": f"{np.std(_bm_arr):.1f}%", "최솟값": f"{np.min(_bm_arr):.1f}%",
                        "최댓값": f"{np.max(_bm_arr):.1f}%", "양(+) 비율": "-",
                    })
                st.markdown(f"**📋 요약 통계 (n={_n_mc})**")
                st.dataframe(pd.DataFrame(_mc_stat_rows), hide_index=True, use_container_width=True)

                # ── 100구간 상세 결과 표 ──
                with st.expander("📄 100구간 상세 결과 보기"):
                    _detail_rows = []
                    for _di, (_psd, _ped) in enumerate(_mc_res["periods"]):
                        _row = {
                            "#": _di + 1,
                            "시작일": _psd, "종료일": _ped,
                            "전략 수익률(%)": f"{_mc_res['strat_ret'][_di]:+.1f}%",
                            "전략 MDD(%)":    f"{_mc_res['strat_mdd'][_di]:.1f}%",
                        }
                        if _br_arr is not None and _di < len(_mc_res["bnh_ret"]):
                            _row[f"{tk} B&H 수익률(%)"] = f"{_mc_res['bnh_ret'][_di]:+.1f}%"
                            _row[f"{tk} B&H MDD(%)"]    = f"{_mc_res['bnh_mdd'][_di]:.1f}%"
                        _detail_rows.append(_row)
                    _detail_df = pd.DataFrame(_detail_rows)

                    def _highlight_detail(row):
                        try:
                            ret = float(row["전략 수익률(%)"].replace("%","").replace("+",""))
                            color = "background-color: #e8f5e9" if ret > 0 else "background-color: #ffebee"
                            return [color] * len(row)
                        except Exception:
                            return [""] * len(row)

                    st.dataframe(
                        _detail_df.style.apply(_highlight_detail, axis=1),
                        hide_index=True, use_container_width=True,
                        height=min(38 + 35 * len(_detail_df), 500),
                    )

                # ── 차트: 수익률 분포 + MDD 분포 ──
                from scipy.stats import gaussian_kde as _kde

                # B&H 먼저(뒤), 전략 나중(앞) 순서로 그려야 전략이 앞에 표시됨
                _fig_mc = make_subplots(
                    rows=1, cols=2,
                    subplot_titles=["수익률 분포 (1년)", "최대 낙폭(MDD) 분포"],
                    horizontal_spacing=0.12,
                )

                def _add_hist_kde(fig, arr, color, name, row, col, legendgroup, showlegend):
                    fig.add_trace(go.Histogram(
                        x=arr.tolist(), nbinsx=20, name=name,
                        opacity=0.55, marker_color=color,
                        legendgroup=legendgroup, showlegend=showlegend,
                    ), row=row, col=col)
                    if len(arr) > 5:
                        _kd = _kde(arr)
                        _xr = np.linspace(arr.min() - arr.std(), arr.max() + arr.std(), 200)
                        _yr = _kd(_xr) * len(arr) * (arr.max() - arr.min()) / 20
                        fig.add_trace(go.Scatter(
                            x=_xr.tolist(), y=_yr.tolist(), name=name,
                            line=dict(color=color, width=2.5),
                            legendgroup=legendgroup, showlegend=False,
                        ), row=row, col=col)

                # B&H 먼저 (뒤에 배치)
                if _br_arr is not None:
                    _add_hist_kde(_fig_mc, _br_arr, "#FB8C00", _bnh_label,   1, 1, "bnh",    True)
                if _bm_arr is not None:
                    _add_hist_kde(_fig_mc, _bm_arr, "#FB8C00", _bnh_label,   1, 2, "bnh",    False)
                # 전략 나중 (앞에 배치)
                _add_hist_kde(_fig_mc, _sr_arr, "#1565C0", _strat_label, 1, 1, "strat",  True)
                _add_hist_kde(_fig_mc, _sm_arr, "#1565C0", _strat_label, 1, 2, "strat",  False)

                _fig_mc.add_vline(x=0, line_dash="dash", line_color="#555", row=1, col=1)
                _fig_mc.update_xaxes(title_text="1년 수익률 (%)", row=1, col=1)
                _fig_mc.update_xaxes(title_text="MDD (%)",        row=1, col=2)
                _fig_mc.update_yaxes(title_text="빈도 (구간수)",   row=1, col=1)
                _fig_mc.update_yaxes(title_text="빈도 (구간수)",   row=1, col=2)
                _fig_mc.update_layout(
                    height=460, barmode="overlay",
                    legend=dict(
                        orientation="v",
                        x=1.02, y=1.0,
                        xanchor="left", yanchor="top",
                        bgcolor="rgba(255,255,255,0.85)",
                        bordercolor="#ccc", borderwidth=1,
                    ),
                    margin=dict(r=140),
                )
                st.plotly_chart(_fig_mc, use_container_width=True)
                st.caption(
                    "전략이 B&H 대비 수익률 분포가 오른쪽에 집중(높은 수익)되고, "
                    "MDD 분포가 왼쪽에 집중(낮은 손실)될수록 강건한 전략입니다."
                )

                # ── 박스플롯: 수익률 + MDD ──
                st.markdown("**📦 박스플롯 — 분포 요약 (중앙값 · 사분위 · 이상치)**")
                _fig_box = make_subplots(
                    rows=1, cols=2,
                    subplot_titles=["수익률 박스플롯", "최대 낙폭(MDD) 박스플롯"],
                    horizontal_spacing=0.12,
                )
                # 수익률 박스플롯 — B&H 먼저(뒤), 전략 나중(앞)
                if _br_arr is not None:
                    _fig_box.add_trace(go.Box(
                        y=_br_arr.tolist(), name=_bnh_label,
                        marker_color="#FB8C00", boxmean=True,
                        legendgroup="bnh_box", showlegend=True,
                    ), row=1, col=1)
                _fig_box.add_trace(go.Box(
                    y=_sr_arr.tolist(), name=_strat_label,
                    marker_color="#1565C0", boxmean=True,
                    legendgroup="strat_box", showlegend=True,
                ), row=1, col=1)
                # MDD 박스플롯
                if _bm_arr is not None:
                    _fig_box.add_trace(go.Box(
                        y=_bm_arr.tolist(), name=_bnh_label,
                        marker_color="#FB8C00", boxmean=True,
                        legendgroup="bnh_box", showlegend=False,
                    ), row=1, col=2)
                _fig_box.add_trace(go.Box(
                    y=_sm_arr.tolist(), name=_strat_label,
                    marker_color="#1565C0", boxmean=True,
                    legendgroup="strat_box", showlegend=False,
                ), row=1, col=2)

                _fig_box.add_hline(y=0, line_dash="dash", line_color="#888", row=1, col=1)
                _fig_box.update_yaxes(title_text="수익률 (%)", row=1, col=1)
                _fig_box.update_yaxes(title_text="MDD (%)",    row=1, col=2)
                _fig_box.update_layout(
                    height=420,
                    legend=dict(
                        orientation="v",
                        x=1.02, y=1.0,
                        xanchor="left", yanchor="top",
                        bgcolor="rgba(255,255,255,0.85)",
                        bordercolor="#ccc", borderwidth=1,
                    ),
                    margin=dict(r=140),
                )
                st.plotly_chart(_fig_box, use_container_width=True)
                st.caption(
                    "박스: 25~75 백분위 구간 / 선: 중앙값 / 삼각형(▲): 평균 / 점: 이상치"
                )

        st.divider()

        # ── 티어별 매수 사이클 분석 ────────────────────────────
        st.subheader("📊 티어별 매수 사이클 분석")
        st.caption("포지션이 완전히 청산될 때까지를 1사이클로 보고, 사이클마다 몇 번 분할 매수가 발생했는지 분석합니다.")
        with st.spinner("티어별 분석 중..."):
            _tier_events = run_tier_breakdown_analysis(_pdf, s_date, e_date, a_b, a_s, sr, div, init_cap, n_days=n_days)
        if _tier_events:
            _tier_df = pd.DataFrame(_tier_events)
            # 티어별 집계
            _tier_summary_rows = []
            for _t in range(1, div + 1):
                _sub = _tier_df[_tier_df["티어수"] == _t]
                if len(_sub) == 0:
                    _tier_summary_rows.append({
                        "티어": f"{_t}티어",
                        "발생횟수": 0, "승수": 0, "패수": 0, "승률(%)": "-",
                        "평균보유일": "-", "평균손익률(%)": "-",
                        "최대수익(%)": "-", "최대손실(%)": "-",
                    })
                else:
                    _wins = int((_sub["손익률"] > 0).sum())
                    _loss = len(_sub) - _wins
                    _tier_summary_rows.append({
                        "티어": f"{_t}티어",
                        "발생횟수": len(_sub),
                        "승수": _wins,
                        "패수": _loss,
                        "승률(%)": f"{_wins/len(_sub)*100:.1f}%",
                        "평균보유일": f"{_sub['보유일수'].mean():.1f}일",
                        "평균손익률(%)": f"{_sub['손익률'].mean():+.2f}%",
                        "최대수익(%)": f"{_sub['손익률'].max():+.2f}%",
                        "최대손실(%)": f"{_sub['손익률'].min():+.2f}%",
                    })
            _tier_summary_df = pd.DataFrame(_tier_summary_rows)

            # 요약 테이블
            st.dataframe(_tier_summary_df, hide_index=True, use_container_width=True)

            # 차트: 발생 횟수 + 평균 손익률
            _tier_chart_data = pd.DataFrame({
                "티어": [f"{_t}티어" for _t in range(1, div + 1)],
                "발생횟수": [len(_tier_df[_tier_df["티어수"] == _t]) for _t in range(1, div + 1)],
                "평균손익률": [
                    round(_tier_df[_tier_df["티어수"] == _t]["손익률"].mean(), 2)
                    if len(_tier_df[_tier_df["티어수"] == _t]) > 0 else 0
                    for _t in range(1, div + 1)
                ],
            })
            _fig_tier = go.Figure()
            _fig_tier.add_trace(go.Bar(
                x=_tier_chart_data["티어"], y=_tier_chart_data["발생횟수"],
                name="발생횟수", marker_color="#5C6BC0", yaxis="y1",
            ))
            _fig_tier.add_trace(go.Scatter(
                x=_tier_chart_data["티어"], y=_tier_chart_data["평균손익률"],
                name="평균손익률(%)", mode="lines+markers+text",
                text=[f"{v:+.2f}%" for v in _tier_chart_data["평균손익률"]],
                textposition="top center",
                marker=dict(size=10, color="#EF5350"),
                line=dict(color="#EF5350", width=2),
                yaxis="y2",
            ))
            _fig_tier.update_layout(
                title=f"티어별 발생 횟수 & 평균 손익률",
                yaxis=dict(title="발생횟수 (회)", side="left"),
                yaxis2=dict(title="평균 손익률 (%)", side="right", overlaying="y",
                            zeroline=True, zerolinecolor="#aaa"),
                legend=dict(orientation="h", y=1.1),
                height=360, bargap=0.3,
            )
            st.plotly_chart(_fig_tier, use_container_width=True)

            # 상세 내역 expander
            with st.expander("📋 전체 사이클 상세 내역 보기"):
                def _style_tier(row):
                    return ["color: #2e7d32; font-weight:bold" if row["손익률"] > 0
                            else "color: #c62828; font-weight:bold" if row["손익률"] < 0
                            else "" for _ in row]
                _tier_df_disp = _tier_df.copy()
                _tier_df_disp["티어수"] = _tier_df_disp["티어수"].apply(lambda x: f"{x}티어")
                st.dataframe(
                    _tier_df_disp.style.apply(_style_tier, axis=1)
                                        .format({"평균단가": "${:.2f}", "최종매도가": "${:.2f}", "손익률": "{:+.2f}%"}),
                    hide_index=True, use_container_width=True,
                    height=min(38 + 35 * len(_tier_df_disp), 500),
                )
        else:
            st.info("선택 기간 내 완전 청산된 사이클이 없습니다.")
        st.divider()

        st.subheader(f"🎯 {div}티어 완전 투자 분석")
        st.caption(f"분할 매수 {div}회가 모두 체결된 사이클 분석")
        with st.spinner("5티어 분석 중..."):
            _t5 = run_5tier_analysis(_pdf, s_date, e_date, a_b, a_s, sr, div, init_cap, n_days=n_days)
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

        # ── 이동평균 N일별 성과 비교 ─────────────────────────────
        st.subheader("📐 이동평균 일수별 성과 비교")
        st.caption("이동평균 3일(N=2)~10일(N=9)까지 동일 파라미터로 백테스트하여 최적 이동평균 일수를 확인합니다.")
        with st.spinner("N일별 백테스트 실행 중..."):
            _nday_rows = []
            for _nd in range(2, 10):   # 내부 n_days: 2~9 → 표시: 3일~10일
                _nr = run_backtest(_pdf, s_date, e_date, a_b, a_s, sr, div, init_cap, n_days=_nd)
                if _nr is None:
                    continue
                _nsh, _nso = compute_sharpe_sortino(_nr["assets"])
                _nday_rows.append({
                    "이동평균": f"{_nd + 1}일",
                    "CAGR (%)":    round(_nr["cagr"]         * 100, 2),
                    "MDD (%)":     round(_nr["mdd"]          * 100, 2),
                    "Calmar":      round(_nr["calmar"],              3),
                    "Sharpe":      round(_nsh,                       3),
                    "Sortino":     round(_nso,                       3),
                    "총수익률 (%)": round(_nr["total_return"] * 100, 2),
                    "_nd": _nd,
                })

        if _nday_rows:
            _ndf = pd.DataFrame(_nday_rows)
            _cur_label = f"{n_days + 1}일"   # 현재 선택된 이동평균

            # ── 표 ──
            def _style_nday(row):
                bg = "background-color: #fff9c4; font-weight:bold" if row["이동평균"] == _cur_label else ""
                return [bg] * len(row)

            _ndf_show = _ndf.drop(columns=["_nd"]).copy()
            _ndf_show["CAGR (%)"]     = _ndf_show["CAGR (%)"].apply(lambda v: f"{v:+.2f}%")
            _ndf_show["MDD (%)"]      = _ndf_show["MDD (%)"].apply(lambda v: f"{v:.2f}%")
            _ndf_show["총수익률 (%)"] = _ndf_show["총수익률 (%)"].apply(lambda v: f"{v:+.2f}%")
            st.dataframe(
                _ndf_show.style.apply(_style_nday, axis=1),
                use_container_width=True, hide_index=True,
            )
            st.caption(f"🟡 노란색 행 = 현재 설정값 ({_cur_label})")

            # ── 차트: CAGR / MDD / Calmar 3개 나란히 ──
            _fc1, _fc2, _fc3 = st.columns(3)

            def _bar_chart(col, y_col, title, color, higher_better=True):
                _fig = go.Figure()
                for _, row in _ndf.iterrows():
                    is_cur = row["이동평균"] == _cur_label
                    _fig.add_trace(go.Bar(
                        x=[row["이동평균"]], y=[abs(row[y_col])],
                        marker_color="#FDD835" if is_cur else color,
                        marker_line_color="#F57F17" if is_cur else color,
                        marker_line_width=2 if is_cur else 0,
                        showlegend=False,
                        hovertemplate=f"{row['이동평균']}: {row[y_col]:+.3f}<extra></extra>",
                    ))
                _fig.update_layout(
                    title=dict(text=title, font=dict(size=13)),
                    height=280, margin=dict(l=30, r=10, t=40, b=30),
                    xaxis_title="이동평균", yaxis_title=y_col,
                    plot_bgcolor="white",
                )
                col.plotly_chart(_fig, use_container_width=True)

            _bar_chart(_fc1, "CAGR (%)",  "CAGR (%)",  "#1565C0")
            _bar_chart(_fc2, "MDD (%)",   "MDD (%)",   "#C62828")
            _bar_chart(_fc3, "Calmar",    "Calmar",    "#2E7D32")

            # 최적 N 자동 탐지 (Calmar 기준)
            _best_row = _ndf.loc[_ndf["Calmar"].idxmax()]
            _best_label = _best_row["이동평균"]
            if _best_label == _cur_label:
                st.success(f"✅ 현재 설정 **{_cur_label}**이 분석 기간 기준 Calmar 최적값입니다!")
            else:
                st.info(f"💡 분석 기간 기준 Calmar 최적 이동평균은 **{_best_label}** (현재: {_cur_label})")

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
        st.caption(
            f"등록된 계좌: **{', '.join(_perf_tk_settings.keys())}**  |  "
            f"현재 사이드바 선택 ticker(**{ticker}**)는 사이드바 파라미터 그대로 적용 · "
            f"나머지는 각자 저장된 파라미터 사용  |  기간·초기자본은 사이드바 설정 공통 적용"
        )
    else:
        st.caption("사이드바의 공통 설정(티커 · 파라미터 · 기간 · 초기 자본)을 기준으로 분석합니다.")

    def _resolve_params(ptk, pcfg):
        """사이드바에서 선택 중인 ticker → 사이드바 현재값 사용.
        다른 ticker → 각자 저장된 값 사용."""
        if ptk == ticker:
            return float(a_buy), float(a_sell), float(sell_ratio), int(divisions)
        return (
            float(pcfg.get("a_buy",      a_buy)),
            float(pcfg.get("a_sell",     a_sell)),
            float(pcfg.get("sell_ratio", sell_ratio)),
            int  (pcfg.get("divisions",  divisions)),
        )

    if st.button("▶ 성과 분석 실행", type="primary", key="run_perf"):
        # 실행 파라미터를 session_state에 저장 → rerun 후에도 유지
        st.session_state["perf_run_params"] = {
            "tk_settings": dict(_perf_tk_settings) if _perf_tk_settings else None,
            "ticker": ticker, "a_buy": float(a_buy), "a_sell": float(a_sell),
            "sell_ratio": float(sell_ratio), "divisions": int(divisions),
            "initial_capital": initial_capital,
            "start_date": start_date, "end_date": end_date,
        }

    def _do_render_perf():
        """session_state에 저장된 파라미터로 성과 분석 렌더링."""
        _prm = st.session_state.get("perf_run_params")
        if not _prm:
            return
        _p_tk_settings = _prm["tk_settings"]
        _p_ticker      = _prm["ticker"]
        _p_ab          = _prm["a_buy"];   _p_as = _prm["a_sell"]
        _p_sr          = _prm["sell_ratio"]; _p_dv = _prm["divisions"]
        _p_cap         = _prm["initial_capital"]
        _p_sd          = _prm["start_date"]; _p_ed = _prm["end_date"]

        def _resolve_saved(ptk, pcfg):
            if ptk == _p_ticker:
                return _p_ab, _p_as, _p_sr, _p_dv
            return (
                float(pcfg.get("a_buy",      _p_ab)),
                float(pcfg.get("a_sell",     _p_as)),
                float(pcfg.get("sell_ratio", _p_sr)),
                int  (pcfg.get("divisions",  _p_dv)),
            )

        if _p_tk_settings:
            _tk_list = list(_p_tk_settings.keys())
            if len(_tk_list) > 1:
                _perf_tabs = st.tabs([f"📊 {t}" for t in _tk_list])
                for _pi, _ptk in enumerate(_tk_list):
                    with _perf_tabs[_pi]:
                        _pcfg = _p_tk_settings[_ptk]
                        _pb, _ps, _psr, _pdv = _resolve_saved(_ptk, _pcfg)
                        _render_perf_analysis(_ptk, _pb, _ps, _psr, _pdv, _p_cap, _p_sd, _p_ed)
            else:
                _ptk  = _tk_list[0]
                _pcfg = _p_tk_settings[_ptk]
                _pb, _ps, _psr, _pdv = _resolve_saved(_ptk, _pcfg)
                _render_perf_analysis(_ptk, _pb, _ps, _psr, _pdv, _p_cap, _p_sd, _p_ed)

            # ── 종목 간 비교 ──────────────────────────────────────
            # 등록 계좌 기준 + 미등록 기본 종목(SOXL/USD) 자동 보완 → 항상 표시
            _CMP_DEFAULTS = {
                "SOXL": {"a_buy": -0.0063, "a_sell": 0.0075, "sell_ratio": 100.0, "divisions": 5, "n_days": 2},
                "USD":  {"a_buy": -0.0111, "a_sell": 0.0063, "sell_ratio": 100.0, "divisions": 5, "n_days": 2},
            }
            _cmp_all_settings = dict(_p_tk_settings)
            for _def_tk, _def_cfg in _CMP_DEFAULTS.items():
                if _def_tk not in _cmp_all_settings:
                    _cmp_all_settings[_def_tk] = _def_cfg
            _cmp_all_keys = list(_cmp_all_settings.keys())
            if len(_cmp_all_keys) > 1:
                st.divider()
                st.subheader("🔄 종목 간 성과 비교")
                st.caption("등록된 종목들의 수익 곡선 및 지표를 한 화면에서 비교합니다. (미등록 종목은 안정형 기본 파라미터 사용)")
                _colors = ["#1565C0", "#E53935", "#2E7D32", "#F57F17", "#6A1B9A"]
                _fig_cmp = go.Figure()
                _cmp_rows = []
                for _ci, _ctk in enumerate(_cmp_all_keys):
                    _ccfg = _cmp_all_settings[_ctk]
                    _cpdf = load_price_data(_ctk, _p_sd, _p_ed, "야후파이낸스 (yfinance)", None)
                    if _cpdf.empty:
                        continue
                    _cb, _cs, _csr, _cdv = _resolve_saved(_ctk, _ccfg)
                    _c_ndays = int(_ccfg.get("n_days", 2))
                    _cr = run_backtest(_cpdf, _p_sd, _p_ed, _cb, _cs, _csr, _cdv, _p_cap, n_days=_c_ndays)
                    if not _cr:
                        continue
                    _sharpe_c, _sortino_c = compute_sharpe_sortino(_cr["assets"])
                    _norm = _cr["assets"] / _p_cap * 100
                    _fig_cmp.add_trace(go.Scatter(
                        x=[str(d.date()) for d in _cr["dates"]], y=_norm.tolist(),
                        name=_ctk, line=dict(color=_colors[_ci % len(_colors)], width=2),
                    ))
                    _cmp_rows.append({
                        "종목": _ctk,
                        "총수익률": f"{_cr['total_return']*100:+.1f}%",
                        "CAGR": f"{_cr['cagr']*100:.1f}%",
                        "MDD": f"{_cr['mdd']*100:.1f}%",
                        "Calmar": f"{_cr['calmar']:.3f}",
                        "Sharpe": f"{_sharpe_c:.3f}",
                        "Sortino": f"{_sortino_c:.3f}",
                        "승률": f"{_cr['win_count']/_cr['sell_count']*100:.1f}%" if _cr['sell_count'] > 0 else "-",
                    })
                _fig_cmp.add_hline(y=100, line_dash="dash", line_color="#aaa", annotation_text="시작(100)")
                _fig_cmp.update_layout(
                    title="종목별 정규화 수익 곡선 (시작=100)",
                    yaxis_title="자산 지수 (시작=100)",
                    legend=dict(orientation="h", y=1.08), height=400,
                )
                st.plotly_chart(_fig_cmp, use_container_width=True)
                if _cmp_rows:
                    st.dataframe(pd.DataFrame(_cmp_rows), hide_index=True, use_container_width=True)
        else:
            _render_perf_analysis(_p_ticker, _p_ab, _p_as, _p_sr, _p_dv, _p_cap, _p_sd, _p_ed)

    _do_render_perf()


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


    # _send_telegram 은 최상위 스코프에 정의 (표준편차/종가평균 공용)


    def _build_order_text(ticker_name: str, _a_buy: float, _a_sell: float,
                      _sell_ratio: float, _divisions: int, _n_days: int = 2,
                      _os_start=None, _os_capital: float = 10000.0) -> str:
        """Tab3와 동일한 시뮬레이션 엔진으로 오늘의 주문표를 텔레그램 텍스트로 변환."""
        try:
            today = datetime.today().date()
            price_df_tg = load_price_data(ticker_name, _os_start, today, "Yahoo Finance", None)
            if price_df_tg.empty:
                return "❌ 가격 데이터를 불러오지 못했습니다."

            res = run_portfolio_for_ordersheet(
                price_df_tg, _os_start, ticker_name,
                _a_buy, _a_sell, _sell_ratio, _divisions, _os_capital,
                n_days=_n_days,
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




if _is_stdev:
    with tab5:
        st.subheader("⚙️ 개인 설정")

        _cfg5    = load_config()
        _usercfg = st.session_state.get("user_settings", {}) if _IS_CLOUD else {}

        if _IS_CLOUD:
            st.info(f"☁️ **{st.session_state.get('username','')}** 으로 로그인 중 — 설정을 저장하면 다음 로그인 시 자동으로 불러옵니다.")
        else:
            st.success(f"🖥️ **로컬 PC 실행 중** — 설정이 `{_CONFIG}` 에 저장됩니다.")

        # ── 텔레그램 알림 설정 ──────────────────────────────
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
          <li>텔레그램 앱에서 검색창에 <span class="tg-tag">@BotFather</span> 를 검색합니다.</li>
          <li>파란 체크 공식 계정을 선택하고 <span class="tg-tag">/start</span> 를 눌러 대화를 시작합니다.</li>
          <li><span class="tg-tag">/newbot</span> 을 입력합니다.</li>
          <li><strong>봇 표시 이름</strong>을 입력합니다. (예: <span class="tg-tag">표준편차 알림봇</span>) — 한글 가능, 자유롭게 설정</li>
          <li><strong>봇 username</strong>을 입력합니다. — 영문+숫자만 가능, 반드시 <span class="tg-tag">bot</span> 으로 끝나야 함<br>
              &nbsp;&nbsp;예: <span class="tg-tag">stdev_alert_bot</span> &nbsp;/&nbsp; <span class="tg-tag">my_soxl_bot</span></li>
          <li>성공 시 <strong>HTTP API Token</strong>이 발급됩니다. 이것이 <strong>Bot Token</strong>입니다.</li>
        </ol>
        <div class="tg-example-box">
          <div style="color:#888; font-size:12px; margin-bottom:4px;">Bot Token 예시 (발급 후 복사해서 아래 입력창에 붙여넣기):</div>
          <div class="tg-example-val">1234567890:ABCdefGHIjklMNOpqrSTUvwxYZ</div>
        </div>
      </div>
    </div>

    <div class="tg-help-section">
      <div class="tg-help-title"><span class="tg-help-badge">2</span> 내 봇 시작하기 (필수!)</div>
      <div class="tg-warn-box">
        <div class="tg-warn-title">⚠ 봇을 먼저 시작해야 Chat ID를 확인하고 메시지를 받을 수 있습니다!</div>
        <ol>
          <li>텔레그램 검색창에서 내가 만든 봇 username을 검색합니다. (예: <span class="tg-tag">@stdev_alert_bot</span>)</li>
          <li>봇 대화창에서 <span class="tg-tag">/start</span> 를 눌러 봇을 활성화합니다.</li>
          <li>봇에게 아무 메시지나 한 번 보냅니다. (Chat ID 확인을 위해 필요)</li>
        </ol>
      </div>
    </div>

    <div class="tg-help-section">
      <div class="tg-help-title"><span class="tg-help-badge">3</span> Chat ID 확인하기</div>
      <div class="tg-help-box">
        <div class="tg-sub-title">✅ 방법 1: getUpdates API 사용 (가장 확실한 방법)</div>
        <ol>
          <li>위 2단계에서 봇에게 메시지를 보낸 후, 아래 주소를 브라우저에 입력합니다.</li>
          <li><span class="tg-tag">{토큰값}</span> 부분을 발급받은 Bot Token으로 교체합니다.</li>
        </ol>
        <div class="tg-code-box">https://api.telegram.org/bot<span style="color:#fde047;">{토큰값}</span>/getUpdates</div>
        <ol start="3">
          <li>JSON 응답에서 <span class="tg-tag">"id"</span> 값을 찾습니다. 이것이 <strong>Chat ID</strong>입니다.</li>
        </ol>
        <div class="tg-example-box">
          <div style="color:#888; font-size:12px; margin-bottom:6px;">응답 예시:</div>
          <div style="font-family:monospace; font-size:12px; color:#333; line-height:1.8;">
            {"ok":true,"result":[{"message":{"chat":{<strong style="color:#e11d48;">"id": 123456789</strong>,"first_name":"홍길동"}}}]}
          </div>
        </div>
        <div class="tg-sub-title">방법 2: @userinfobot 사용 (간편)</div>
        <ol>
          <li>텔레그램에서 <span class="tg-tag">@userinfobot</span> 을 검색합니다.</li>
          <li><span class="tg-tag">/start</span> 를 누르면 자동으로 내 Chat ID가 표시됩니다.</li>
        </ol>
        <div class="tg-sub-title">방법 3: @RawDataBot 사용</div>
        <ol>
          <li>텔레그램에서 <span class="tg-tag">@RawDataBot</span> 을 검색합니다.</li>
          <li>아무 메시지나 보내면 JSON 형식으로 정보가 표시되며, <span class="tg-tag">"id"</span> 값이 Chat ID입니다.</li>
        </ol>
        <div class="tg-example-box">
          <div style="color:#888; font-size:12px; margin-bottom:4px;">Chat ID 예시 (숫자만, 복사해서 아래 입력창에 붙여넣기):</div>
          <div class="tg-example-val">123456789</div>
        </div>
      </div>
    </div>

    <div class="tg-help-section">
      <div class="tg-help-title"><span class="tg-help-badge">4</span> 연결 테스트</div>
      <div class="tg-tip-box">
        💡 Bot Token과 Chat ID를 입력한 후 아래 <strong>📨 주문표 테스트 발송</strong> 버튼을 눌러보세요.<br>
        메시지가 정상적으로 수신되면 설정 완료입니다! ✅
      </div>
    </div>
    """, unsafe_allow_html=True)

            c1, c2 = st.columns(2)
            # 표준편차 전용 키 (sd_tg_*) — 종가평균매매(tg_*)와 분리 저장
            sd_tg_chat_id = c1.text_input(
                "텔레그램 Chat ID",
                value=_cfg5.get("sd_tg_chat_id", "") if not _IS_CLOUD else _usercfg.get("sd_tg_chat_id", ""),
                placeholder="예: 1234567890",
                key="sd_tg_chat_id_input",
            )
            sd_tg_token = c2.text_input(
                "Bot Token",
                value=_cfg5.get("sd_tg_token", "") if not _IS_CLOUD else _usercfg.get("sd_tg_token", ""),
                placeholder="예: 123456789:AAF...",
                type="password",
                key="sd_tg_token_input",
            )
            st.caption("📅 주문표는 매주 월~금 오후 3:00 (KST)에 텔레그램으로 자동 발송됩니다")

            btn_col1, btn_col2, spacer = st.columns([1, 1, 4])
            with btn_col1:
                if st.button("📨 주문표 테스트 발송", use_container_width=True, key="sd_tg_test"):
                    if not sd_tg_chat_id or not sd_tg_token:
                        st.warning("Chat ID와 Bot Token을 먼저 입력해주세요.")
                    else:
                        _sd_settings = _get_sd_ticker_settings()
                        if not _sd_settings:
                            st.warning("⚠️ 등록된 표준편차 계좌가 없습니다. 오늘의 주문표 탭에서 계좌를 먼저 등록해주세요.")
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
                                    )
                                    result = _send_telegram(sd_tg_token, sd_tg_chat_id, msg)
                                if result.get("ok"):
                                    st.success(f"✅ {_sd_tk} 발송 성공!")
                                else:
                                    st.error(f"❌ {_sd_tk} 발송 실패: {result.get('description', '알 수 없는 오류')}")

            with btn_col2:
                if st.button("💾 저장하기", use_container_width=True, key="sd_tg_save", type="primary"):
                    if not sd_tg_chat_id or not sd_tg_token:
                        st.warning("Chat ID와 Bot Token을 모두 입력해주세요.")
                    elif _IS_CLOUD:
                        with st.spinner("저장 중..."):
                            try:
                                _save_user_settings_to_sheet(
                                    st.session_state.username,
                                    {"sd_tg_chat_id": sd_tg_chat_id, "sd_tg_token": sd_tg_token})
                                st.session_state.user_settings.update(
                                    {"sd_tg_chat_id": sd_tg_chat_id, "sd_tg_token": sd_tg_token})
                                st.success("✅ Google Sheets에 저장 완료!")
                            except Exception as e:
                                st.error(f"❌ 저장 실패: {e}")
                    else:
                        save_config({"sd_tg_chat_id": sd_tg_chat_id, "sd_tg_token": sd_tg_token}, sensitive=True)
                        st.success(f"✅ 저장 완료! `{_CONFIG}`")

        st.write("")

        # ── 구글 스프레드시트 연동 ──────────────────────────────
        with st.container(border=True):
            col_gs1, col_gs2 = st.columns([3, 1])
            with col_gs1:
                st.markdown("#### 🗂️ 구글 스프레드시트 연동")
                st.caption("포트폴리오 정보와 주문 신호를 구글 스프레드시트로 전송합니다.")
            with col_gs2:
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
          <li>스프레드시트 이름을 지정합니다. (예: 표준편차 포트폴리오)</li>
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

            _cfg5_gs = load_config()
            gs_url_sd = st.text_input(
                "스프레드시트 URL",
                value=_cfg5_gs.get("gs_url", "") if not _IS_CLOUD else _usercfg.get("gs_url", ""),
                placeholder="https://docs.google.com/spreadsheets/d/...",
                key="gs_url_input_sd",
            )
            st.caption("* 스프레드시트에 서비스 계정 이메일을 편집자로 공유해주세요. (우측 상단 도움말 참고)")

            # 표준편차 계좌별 시트 이름 매핑
            _gs_sd_settings = _get_sd_ticker_settings()
            _gs_sd_sheet_map = {}
            if _gs_sd_settings:
                st.markdown("##### 📋 종목별 시트 이름 매핑")
                st.caption("각 종목 데이터를 기록할 구글시트의 탭(시트) 이름을 입력하세요.")
                for _gs_sd_tk, _gs_sd_cfg in _gs_sd_settings.items():
                    _default_sheet = _gs_sd_cfg.get("gs_sheet", _gs_sd_tk)
                    _gs_sd_sheet_map[_gs_sd_tk] = st.text_input(
                        f"{_gs_sd_tk} 시트 이름",
                        value=_default_sheet,
                        key=f"gs_sheet_sd_{_gs_sd_tk}",
                    )
            else:
                st.info("📭 등록된 표준편차 계좌가 없습니다. 오늘의 주문표 탭에서 계좌를 먼저 등록해주세요.")

            st.write("")
            btn_gs1, btn_gs2, btn_gs3 = st.columns(3)
            with btn_gs1:
                if st.button("🔗 시트 연결 테스트", use_container_width=True, key="gs_test_sd"):
                    if not gs_url_sd:
                        st.warning("스프레드시트 URL을 먼저 입력해주세요.")
                    else:
                        try:
                            gc = _get_gspread_client()
                            sh = gc.open_by_url(gs_url_sd)
                            st.success(f"✅ 연결 성공! 스프레드시트: **{sh.title}**")
                        except Exception as e:
                            st.error(f"❌ 연결 실패: {e}")

            with btn_gs2:
                if st.button("📊 주문 시트 전송", use_container_width=True, key="gs_send_sd", type="primary"):
                    if not gs_url_sd:
                        st.warning("스프레드시트 URL을 먼저 입력해주세요.")
                    elif not _gs_sd_settings:
                        st.warning("등록된 표준편차 계좌가 없습니다.")
                    else:
                        for _gs_sd_tk, _gs_sd_cfg in _gs_sd_settings.items():
                            _sheet_nm = _gs_sd_sheet_map.get(_gs_sd_tk, _gs_sd_tk)
                            with st.spinner(f"{_gs_sd_tk} → '{_sheet_nm}' 전송 중..."):
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
                                        st.error(f"❌ {_gs_sd_tk}: 시뮬레이션 데이터가 없습니다.")
                                    else:
                                        # GSheets 기록
                                        gc = _get_gspread_client()
                                        sh = gc.open_by_url(gs_url_sd)
                                        ws = sh.worksheet(_sheet_nm)
                                        ws.batch_clear(["L4:O13"])
                                        rows_gs = [["매수", "LOC", _res_gs["next_buy_loc"], _res_gs["est_buy_qty"]]]
                                        if _res_gs["holdings"] > 0:
                                            rows_gs.append(["매도", "LOC", _res_gs["next_sell_loc"], _res_gs["est_sell_qty"]])
                                        ws.update(range_name="L4", values=rows_gs)
                                        st.success(f"✅ {_gs_sd_tk} → '{_sheet_nm}' L4에 {len(rows_gs)}건 전송 완료!")
                                except Exception as e:
                                    st.error(f"❌ {_gs_sd_tk} 전송 실패: {e}")

            with btn_gs3:
                if st.button("💾 저장하기 ", use_container_width=True, key="gs_save_sd", type="primary"):
                    if not gs_url_sd:
                        st.warning("스프레드시트 URL을 입력해주세요.")
                    else:
                        if _IS_CLOUD:
                            try:
                                _save_user_settings_to_sheet(st.session_state.username, {"gs_url": gs_url_sd})
                                st.session_state.user_settings.update({"gs_url": gs_url_sd})
                            except Exception as e:
                                st.error(f"❌ 저장 실패: {e}")
                        else:
                            save_config({"gs_url": gs_url_sd}, sensitive=True)
                        for _gs_sd_tk, _sheet_nm in _gs_sd_sheet_map.items():
                            _save_sd_ticker_setting(_gs_sd_tk, {"gs_sheet": _sheet_nm})
                        st.success("✅ URL 및 종목별 시트 이름 저장 완료!")

        st.write("")

        # ── 관리자 도구: 비밀번호 해시 생성 ─────────────────
        with st.expander("🔧 관리자 도구 — 비밀번호 해시 생성 (users 시트 등록용)"):
            st.caption("새 사용자를 추가할 때 비밀번호를 bcrypt 해시로 변환하여 Google Sheets에 붙여넣으세요.")
            _admin_pw_sd = st.text_input("등록할 비밀번호 입력", type="password", key="admin_pw_input_sd")
            if st.button("🔑 해시 생성", key="gen_hash_sd"):
                if _admin_pw_sd:
                    st.code(_hash_password(_admin_pw_sd), language=None)
                    st.caption("👆 위 해시를 복사해서 users 시트의 password_hash 컬럼에 붙여넣으세요.")
                else:
                    st.warning("비밀번호를 입력해주세요.")


if not _is_stdev:
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
          <li>텔레그램 앱에서 검색창에 <span class="tg-tag">@BotFather</span> 를 검색합니다.</li>
          <li>파란 체크 공식 계정을 선택하고 <span class="tg-tag">/start</span> 를 눌러 대화를 시작합니다.</li>
          <li><span class="tg-tag">/newbot</span> 을 입력합니다.</li>
          <li><strong>봇 표시 이름</strong>을 입력합니다. (예: <span class="tg-tag">3일평균 알림봇</span>) — 한글 가능, 자유롭게 설정</li>
          <li><strong>봇 username</strong>을 입력합니다. — 영문+숫자만 가능, 반드시 <span class="tg-tag">bot</span> 으로 끝나야 함<br>
              &nbsp;&nbsp;예: <span class="tg-tag">3days_avg_bot</span> &nbsp;/&nbsp; <span class="tg-tag">my_soxl_bot</span></li>
          <li>성공 시 <strong>HTTP API Token</strong>이 발급됩니다. 이것이 <strong>Bot Token</strong>입니다.</li>
        </ol>
        <div class="tg-example-box">
          <div style="color:#888; font-size:12px; margin-bottom:4px;">Bot Token 예시 (발급 후 복사해서 아래 입력창에 붙여넣기):</div>
          <div class="tg-example-val">1234567890:ABCdefGHIjklMNOpqrSTUvwxYZ</div>
        </div>
      </div>
    </div>

    <div class="tg-help-section">
      <div class="tg-help-title"><span class="tg-help-badge">2</span> 내 봇 시작하기 (필수!)</div>
      <div class="tg-warn-box">
        <div class="tg-warn-title">⚠ 봇을 먼저 시작해야 Chat ID를 확인하고 메시지를 받을 수 있습니다!</div>
        <ol>
          <li>텔레그램 검색창에서 내가 만든 봇 username을 검색합니다. (예: <span class="tg-tag">@3days_avg_bot</span>)</li>
          <li>봇 대화창에서 <span class="tg-tag">/start</span> 를 눌러 봇을 활성화합니다.</li>
          <li>봇에게 아무 메시지나 한 번 보냅니다. (Chat ID 확인을 위해 필요)</li>
        </ol>
      </div>
    </div>

    <div class="tg-help-section">
      <div class="tg-help-title"><span class="tg-help-badge">3</span> Chat ID 확인하기</div>
      <div class="tg-help-box">
        <div class="tg-sub-title">✅ 방법 1: getUpdates API 사용 (가장 확실한 방법)</div>
        <ol>
          <li>위 2단계에서 봇에게 메시지를 보낸 후, 아래 주소를 브라우저에 입력합니다.</li>
          <li><span class="tg-tag">{토큰값}</span> 부분을 발급받은 Bot Token으로 교체합니다.</li>
        </ol>
        <div class="tg-code-box">https://api.telegram.org/bot<span style="color:#fde047;">{토큰값}</span>/getUpdates</div>
        <ol start="3">
          <li>JSON 응답에서 <span class="tg-tag">"id"</span> 값을 찾습니다. 이것이 <strong>Chat ID</strong>입니다.</li>
        </ol>
        <div class="tg-example-box">
          <div style="color:#888; font-size:12px; margin-bottom:6px;">응답 예시:</div>
          <div style="font-family:monospace; font-size:12px; color:#333; line-height:1.8;">
            {"ok":true,"result":[{"message":{"chat":{<strong style="color:#e11d48;">"id": 123456789</strong>,"first_name":"홍길동"}}}]}
          </div>
        </div>
        <div class="tg-sub-title">방법 2: @userinfobot 사용 (간편)</div>
        <ol>
          <li>텔레그램에서 <span class="tg-tag">@userinfobot</span> 을 검색합니다.</li>
          <li><span class="tg-tag">/start</span> 를 누르면 자동으로 내 Chat ID가 표시됩니다.</li>
        </ol>
        <div class="tg-sub-title">방법 3: @RawDataBot 사용</div>
        <ol>
          <li>텔레그램에서 <span class="tg-tag">@RawDataBot</span> 을 검색합니다.</li>
          <li>아무 메시지나 보내면 JSON 형식으로 정보가 표시되며, <span class="tg-tag">"id"</span> 값이 Chat ID입니다.</li>
        </ol>
        <div class="tg-example-box">
          <div style="color:#888; font-size:12px; margin-bottom:4px;">Chat ID 예시 (숫자만, 복사해서 아래 입력창에 붙여넣기):</div>
          <div class="tg-example-val">123456789</div>
        </div>
      </div>
    </div>

    <div class="tg-help-section">
      <div class="tg-help-title"><span class="tg-help-badge">4</span> 연결 테스트</div>
      <div class="tg-tip-box">
        💡 Bot Token과 Chat ID를 입력한 후 아래 <strong>📨 텔레그램 테스트 전송</strong> 버튼을 눌러보세요.<br>
        메시지가 정상적으로 수신되면 설정 완료입니다! ✅
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
                                        n_days=int(_tg_cfg.get("n_days", 2)),
                                        _os_start=_tg_start_d,
                                        _os_capital=float(_tg_cfg.get("os_capital",  initial_capital)),
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

            gs_url = st.text_input(
                "스프레드시트 URL",
                value=_cfg5.get("gs_url", "") if not _IS_CLOUD else _usercfg.get("gs_url", ""),
                placeholder="https://docs.google.com/spreadsheets/d/...",
                key="gs_url_input",
            )
            st.caption("* 스프레드시트에 서비스 계정 이메일을 편집자로 공유해주세요. (우측 상단 도움말 참고)")

            # ── 종목별 시트 이름 매핑 ──────────────────────────────
            _gs_tk_settings = _get_ticker_settings()
            _gs_sheet_map   = {}   # {ticker: 입력된 시트 이름}

            if _gs_tk_settings:
                st.markdown("**📋 종목별 시트 이름 매핑**")
                st.caption("각 종목 데이터를 기록할 구글시트의 탭(시트) 이름을 입력하세요.")
                for _gs_tk, _gs_cfg in _gs_tk_settings.items():
                    _gs_default = _gs_cfg.get("gs_sheet", _gs_tk)
                    _gs_sheet_map[_gs_tk] = st.text_input(
                        f"{_gs_tk} 시트 이름",
                        value=_gs_default,
                        placeholder=f"예: {_gs_tk}",
                        key=f"gs_sheet_{_gs_tk}",
                    )
            else:
                st.info("📭 등록된 계좌가 없습니다. Tab3에서 계좌를 먼저 등록해주세요.")

            st.write("")
            btn_col3, btn_col4, btn_col5 = st.columns(3)
            with btn_col3:
                if st.button("🔗 시트 연결 테스트", use_container_width=True, key="gs_test"):
                    if not gs_url:
                        st.warning("스프레드시트 URL을 먼저 입력해주세요.")
                    else:
                        try:
                            gc = _get_gspread_client()
                            sh = gc.open_by_url(gs_url)
                            st.success(f"✅ 연결 성공! 스프레드시트: **{sh.title}**")
                        except Exception as e:
                            st.error(f"❌ 연결 실패: {e}")

            with btn_col4:
                if st.button("📊 주문 시트 전송", use_container_width=True, key="gs_send", type="primary"):
                    if not gs_url:
                        st.warning("스프레드시트 URL을 먼저 입력해주세요.")
                    elif not _gs_tk_settings:
                        st.warning("등록된 계좌가 없습니다.")
                    else:
                        for _gs_tk, _gs_cfg in _gs_tk_settings.items():
                            _sheet_name = _gs_sheet_map.get(_gs_tk, _gs_tk)
                            with st.spinner(f"{_gs_tk} → '{_sheet_name}' 전송 중..."):
                                try:
                                    try:    _gs_start_d = datetime.strptime(_gs_cfg.get("os_start", "2024-01-01"), "%Y-%m-%d").date()
                                    except: _gs_start_d = datetime(2024, 1, 1).date()
                                    _gs_cap  = float(_gs_cfg.get("os_capital", initial_capital))
                                    _gs_a_buy  = float(_gs_cfg.get("a_buy",     -0.005))
                                    _gs_a_sell = float(_gs_cfg.get("a_sell",     0.009))
                                    _gs_sr     = float(_gs_cfg.get("sell_ratio", 100.0))
                                    _gs_div    = int  (_gs_cfg.get("divisions",  5))
                                    _gs_ndays  = int  (_gs_cfg.get("n_days",    2))
                                    _pdf = load_price_data(_gs_tk, _gs_start_d, datetime.today().date(),
                                                           "야후파이낸스 (yfinance)", None)
                                    _res = run_portfolio_for_ordersheet(
                                        _pdf, _gs_start_d, _gs_tk,
                                        _gs_a_buy, _gs_a_sell, _gs_sr, _gs_div, _gs_cap,
                                        n_days=_gs_ndays,
                                    )
                                    if _res is None:
                                        st.error(f"❌ {_gs_tk}: 시뮬레이션 데이터가 없습니다.")
                                    else:
                                        n = _write_orders_to_sheet(gs_url, _sheet_name, _res, _gs_sr, _gs_div, _gs_tk)
                                        st.success(f"✅ {_gs_tk} → '{_sheet_name}' 탭 L4에 {n}건 전송 완료!")
                                except Exception as e:
                                    st.error(f"❌ {_gs_tk} 전송 실패: {e}")

            with btn_col5:
                if st.button("💾 저장하기 ", use_container_width=True, key="gs_save", type="primary"):
                    if not gs_url:
                        st.warning("스프레드시트 URL을 입력해주세요.")
                    else:
                        # gs_url 글로벌 저장
                        if _IS_CLOUD:
                            try:
                                _save_user_settings_to_sheet(st.session_state.username, {"gs_url": gs_url})
                                st.session_state.user_settings.update({"gs_url": gs_url})
                            except Exception as e:
                                st.error(f"❌ 저장 실패: {e}")
                        else:
                            save_config({"gs_url": gs_url}, sensitive=True)
                        # 종목별 시트 이름 저장 (ticker_settings에)
                        for _gs_tk, _sheet_name in _gs_sheet_map.items():
                            _save_ticker_setting(_gs_tk, {"gs_sheet": _sheet_name})
                        st.success("✅ URL 및 종목별 시트 이름 저장 완료!")

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
