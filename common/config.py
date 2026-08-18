from __future__ import annotations

import streamlit as st
import json
import os
from pathlib import Path
import pandas as pd

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
_OLD_CONFIG = Path(__file__).parent.parent / "config.json"   # 이전 경로 (마이그레이션용)
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
                      "sd_tg_chat_id", "sd_tg_token",
                      "sigma_tg_chat_id", "sigma_tg_token"}
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
    _HISTORY_FILE = Path(__file__).parent.parent / "order_history.csv"
else:
    _HISTORY_FILE = Path.home() / ".usd-avg" / "order_history.csv"

def load_order_history() -> pd.DataFrame:
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
        return Path(__file__).parent.parent / f"history_{tk}.csv"
    return Path.home() / ".usd-avg" / f"history_{tk}.csv"

def _load_ticker_ledger(tk: str):
    """원장 로드 (엄격 모드): (df, status) 반환. status:
      "ok"     — 원장 정상 (빈 원장 포함 → 시드 가능)
      "no_url" — Cloud에서 gs_url 미설정 → 원장 미사용 모드
                 (시뮬 기준 표시만, 원장 기록/정산/보정 비활성)
      "error"  — GSheets 접근 실패 (일시 장애) → 원장 처리 전부 스킵
    Cloud에서 로컬 CSV는 재배포 시 초기화되는 휘발성이라 fallback으로 쓰면
    '빈 원장'으로 오인 → 시뮬 재시드 → 원장 오염이 발생함."""
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            import gspread as _gs
            gs_url = st.session_state.get("user_settings", {}).get("gs_url", "")
            if not gs_url:
                return pd.DataFrame(), "no_url"
            client = _get_gspread_client()
            sh = client.open_by_url(gs_url)
            try:
                ws = sh.worksheet(f"{tk}_매매기록")
            except _gs.WorksheetNotFound:
                return pd.DataFrame(), "ok"
            records = ws.get_all_records()
            return (pd.DataFrame(records) if records else pd.DataFrame()), "ok"
        except Exception:
            return pd.DataFrame(), "error"
    f = _get_ticker_history_file(tk)
    if f.exists():
        try:
            return pd.read_csv(f, encoding="utf-8-sig"), "ok"
        except Exception:
            return pd.DataFrame(), "error"
    return pd.DataFrame(), "ok"


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


def _update_ticker_history_rows(tk: str, rows: list, changed_indices: list):
    """원장의 '예정' 행 정산 결과를 반영 (예정 → 체결/미체결 확정).

    B방식(과거 불변)의 유일한 예외: 예정 행은 미확정 주문이므로
    그날 자신의 종가로 1회 확정만 허용. 수량은 절대 재계산하지 않음.
    rows: 로드 순서 그대로의 dict 리스트 (cloud=시트 순서, local=CSV 순서)"""
    if not rows or not changed_indices:
        return
    df = pd.DataFrame(rows)
    # 로컬 CSV 전체 재기록 (행 순서 유지)
    try:
        f = _get_ticker_history_file(tk)
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
                ws = sh.worksheet(f"{tk}_매매기록")
                cols = list(df.columns)
                col_end = chr(ord('A') + len(cols) - 1)
                for ci in changed_indices:
                    vals = [[str(df.iloc[ci][c]) for c in cols]]
                    ws.update(values=vals,
                              range_name=f"A{ci + 2}:{col_end}{ci + 2}",
                              value_input_option="RAW")
        except Exception:
            pass


def _rewrite_ticker_daily_history(tk: str, rows: list, cols: list = None) -> tuple:
    """원장 전체를 rows로 교체 (행 삭제/보정 포함). rows: dict 리스트 (날짜 오름차순).
    현재 상태 보정 전용 — 잘못된 예정 행 제거 + 앵커 행 수정에 사용.
    Returns: (ok: bool, err: str) — Cloud에서 GSheets 기록 실패 시 ok=False.
    실패를 조용히 삼키면 보정이 안 됐는데 성공 표시 → 다음날 재발."""
    if cols is None:
        cols = list(rows[0].keys()) if rows else []
    df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    # 로컬 CSV 전체 재기록
    try:
        f = _get_ticker_history_file(tk)
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
                ws = sh.worksheet(f"{tk}_매매기록")
            except Exception:
                ws = sh.add_worksheet(title=f"{tk}_매매기록", rows=5000, cols=25)
            ws.clear()
            _data = [list(cols)] + [[str(r.get(c, "")) for c in cols] for r in rows]
            ws.update(values=_data, range_name="A1", value_input_option="RAW")
        except Exception as _e:
            return False, str(_e)
    return True, ""

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

# ── gspread 인증 (로그인보다 먼저 정의되어야 함) ──────────────
_GS_SCOPES = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]

def _get_gspread_client():
    """Streamlit Cloud(st.secrets) 또는 로컬(service_account.json)로 gspread 인증."""
    import gspread
    from google.oauth2.service_account import Credentials
    try:
        # 로컬에 secrets.toml이 아예 없으면 st.secrets 접근 자체가 예외를
        # 던지므로 (StreamlitSecretNotFoundError), 멤버십 검사를 감싼다.
        try:
            _has_secret = "gcp_service_account" in st.secrets
        except Exception:
            _has_secret = False
        if _has_secret:
            creds = Credentials.from_service_account_info(
                dict(st.secrets["gcp_service_account"]), scopes=_GS_SCOPES)
        else:
            _key_path = Path(__file__).parent.parent / "service_account.json"
            creds = Credentials.from_service_account_file(str(_key_path), scopes=_GS_SCOPES)
        return gspread.authorize(creds)
    except Exception as e:
        raise RuntimeError(f"인증 실패: {e}")


# ══════════════════════════════════════════════════════════════
# 통합 ticker 설정 CRUD
# ══════════════════════════════════════════════════════════════

def get_ticker_settings(prefix: str = "", settings_key: str = "ticker_settings",
                        exclude_prefix: str = None) -> dict:
    """등록된 계좌 설정 반환 {ticker: {...}}.

    Parameters
    ----------
    prefix : str
        로컬 config의 키 접두사. "" (종가평균), "sd_" (표준편차), "sigma_" (Sigma).
    settings_key : str
        클라우드 user_settings 내 JSON 키.
        "ticker_settings", "sd_ticker_settings", "sigma_ticker_settings".
    exclude_prefix : str | None
        결과에서 이 접두사로 시작하는 키를 제외. 종가평균은 "sd_" 를 전달하여
        과거 버그로 오염된 sd_SOXL 등을 걸러냄.
    """
    if _IS_CLOUD and st.session_state.get("logged_in"):
        raw = st.session_state.get("user_settings", {}).get(settings_key, "") or ""
        ts = _parse_ticker_settings_json(raw)
        if exclude_prefix:
            clean = {k: v for k, v in ts.items() if not k.startswith(exclude_prefix)}
            # 오염 데이터가 있으면 GSheets도 조용히 정리
            if len(clean) != len(ts):
                try:
                    from common.auth import _save_user_settings_to_sheet
                    ts_json = json.dumps(clean, ensure_ascii=False)
                    st.session_state.user_settings[settings_key] = ts_json
                    _save_user_settings_to_sheet(st.session_state.username, {settings_key: ts_json})
                except Exception:
                    pass
            return clean
        return ts
    else:
        full_cfg = _load_full_config()
        if prefix:
            return {k[len(prefix):]: v
                    for k, v in full_cfg.items()
                    if k.startswith(prefix) and isinstance(v, dict)}
        else:
            result = {k: v for k, v in full_cfg.items()
                      if k not in _GLOBAL_CONFIG_KEYS and isinstance(v, dict)}
            if exclude_prefix:
                result = {k: v for k, v in result.items() if not k.startswith(exclude_prefix)}
            return result


def save_ticker_setting(tk: str, data: dict, prefix: str = "",
                        settings_key: str = "ticker_settings") -> str:
    """ticker별 설정 저장 (로컬 config.json + 클라우드 Google Sheets 동기).
    성공 시 '' 반환, 실패 시 오류 메시지 반환."""
    save_config(data, f"{prefix}{tk}")  # 로컬
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            raw = st.session_state.get("user_settings", {}).get(settings_key, "") or ""
            ts  = _parse_ticker_settings_json(raw)
            ts[tk] = {**ts.get(tk, {}), **data}
            ts_json = json.dumps(ts, ensure_ascii=False)
            # session_state 먼저 업데이트 (GSheets 실패해도 화면엔 반영)
            if "user_settings" not in st.session_state:
                st.session_state.user_settings = {}
            st.session_state.user_settings[settings_key] = ts_json
            # GSheets 저장
            from common.auth import _save_user_settings_to_sheet
            _save_user_settings_to_sheet(st.session_state.username, {settings_key: ts_json})
            return ""
        except Exception as e:
            return f"저장 중 오류: {e}"
    return ""


def delete_ticker_setting(tk: str, prefix: str = "",
                          settings_key: str = "ticker_settings",
                          history_prefix: str = "") -> str:
    """ticker 설정 + 매매 히스토리 삭제 (로컬 + 클라우드). 성공 시 '' 반환, 실패 시 오류 메시지 반환."""
    # 로컬 config 제거
    full_cfg = _load_full_config()
    full_cfg.pop(f"{prefix}{tk}", None)
    try:
        _CONFIG.write_text(json.dumps(full_cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    except:
        pass

    # 로컬 히스토리 CSV 삭제
    hist_name = f"{history_prefix}history_{tk}.csv" if history_prefix else f"history_{tk}.csv"
    if _IS_CLOUD:
        f = Path(__file__).parent.parent / hist_name
    else:
        f = Path.home() / ".usd-avg" / hist_name
    try:
        if f.exists():
            f.unlink()
    except Exception:
        pass

    # Cloud: GSheets 워크시트 + 설정 제거
    if _IS_CLOUD and st.session_state.get("logged_in"):
        try:
            # 설정 제거
            raw = st.session_state.get("user_settings", {}).get(settings_key, "") or ""
            ts  = _parse_ticker_settings_json(raw)
            ts.pop(tk, None)
            ts_json = json.dumps(ts, ensure_ascii=False)
            if "user_settings" not in st.session_state:
                st.session_state.user_settings = {}
            st.session_state.user_settings[settings_key] = ts_json
            from common.auth import _save_user_settings_to_sheet
            _save_user_settings_to_sheet(st.session_state.username, {settings_key: ts_json})
            # GSheets 매매기록 워크시트 삭제
            try:
                import gspread as _gs
                gs_url = st.session_state.get("user_settings", {}).get("gs_url", "")
                if gs_url:
                    client = _get_gspread_client()
                    sh = client.open_by_url(gs_url)
                    ws_name = f"{history_prefix}{tk}_매매기록"
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
