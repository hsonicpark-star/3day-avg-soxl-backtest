from __future__ import annotations

import base64
import hashlib
import hmac
import time
from datetime import datetime, timedelta, timezone

import streamlit as st
# 쿠키 읽기/쓰기는 라이브러리의 프런트 컴포넌트만 직접 쓴다 (아래 '자동 로그인 쿠키' 참고)
from streamlit_cookies_controller.cookie_controller import (
    _cookie_controller as _cookie_component)

from common.config import _IS_CLOUD, _get_gspread_client


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


def _load_user_settings_from_sheet(username: str) -> dict | None:
    """users 시트에서 해당 유저 행을 새로 읽어 dict로 반환 (없으면 None).

    로그인 시 세션에 캐시된 설정은 다른 기기/탭에서 바뀐 뒤엔 낡아진다.
    저장 직전에 이 함수로 최신값을 기준 삼아야 낡은 캐시가 시트를 되덮는
    사고(설정이 예전 값으로 '되돌아오는' 현상)를 막을 수 있다."""
    ws = _get_users_ws()
    for row in ws.get_all_records():
        if row.get("username") == username:
            return dict(row)
    return None


def _save_user_settings_to_sheet(username: str, settings: dict):
    """users 시트에서 해당 유저 행의 설정 컬럼 업데이트. 없는 컬럼은 자동 추가.

    ⚠️ 새 시트의 기본 크기는 26열(A~Z)이다. 전략이 늘어 설정 키가 26개를 넘으면
       27번째 열(AA)에 쓰려다 다음 오류로 실패한다:
         APIError [400]: Range (users!AA1) exceeds grid limits.
                         Max rows: 1000, max columns: 26
       → 새 컬럼을 쓰기 전에 부족한 만큼 시트를 넓힌다.
    """
    ws = _get_users_ws()
    headers = ws.row_values(1)

    new_keys = [k for k in settings
                if k not in ("username", "password_hash") and k not in headers]
    if new_keys:
        need = len(headers) + len(new_keys)
        try:
            cur_cols = int(getattr(ws, "col_count", 0) or 0)
        except Exception:
            cur_cols = 0
        if cur_cols and need > cur_cols:
            # 여유분을 조금 더 두어 매번 확장하지 않도록 한다
            ws.add_cols(need - cur_cols + 5)
        for key in new_keys:
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


# ──────────────────────────────────────────────
# 자동 로그인 쿠키
# ──────────────────────────────────────────────
# 예전 구현의 문제 (2026-11-10 교체):
#   · 쿠키에 아이디를 그대로 넣고, 그 아이디가 users 시트에 있으면 비밀번호 없이
#     로그인시켰다 → 아이디만 알면 쿠키를 직접 만들어 로그인할 수 있었다.
#   · CookieController() 를 모듈 최상단에서 만들었는데 이 모듈은 서버 프로세스당 한 번만
#     import 된다 → 쿠키를 읽는 컴포넌트가 서버가 뜬 뒤 첫 세션에서만 그려졌다.
#   · set(expires=30) 은 라이브러리 안에서 (30).isoformat() 오류로 실패했고 except 로
#     삼켜졌다 → 쿠키가 한 번도 저장되지 않아 새 탭에서는 늘 로그인 화면이 떴다.
# 지금 구현:
#   · 쿠키 값 = 서명된 토큰 "v1.<아이디>.<만료시각>.<HMAC>" — 서버 비밀값 없이는 만들 수 없고,
#     비밀번호 해시를 서명에 넣어 비밀번호를 바꾸면 기존 토큰이 모두 무효가 된다.
#   · 컴포넌트는 세션마다(실행마다) 그린다. 응답이 올 때까지 잠깐 기다리고,
#     저장/삭제 컴포넌트는 브라우저가 실행할 시간을 주기 위해 몇 초간 계속 그려 둔다.
_COOKIE_NAME = "usd_avg_session"
_OLD_COOKIE_NAME = "usd_avg_user"     # 예전 평문 아이디 쿠키 — 읽지 않고, 로그아웃 때 지운다
_COOKIE_DAYS = 30
_COOKIE_WAIT_SEC = 3.0                # 쿠키 컴포넌트 응답을 기다리는 최대 시간
_COOKIE_OP_KEEP_SEC = 5.0             # 저장/삭제 컴포넌트를 계속 그려 두는 시간
# 자동 로그인 시 세션에 올리는 users 시트 컬럼 (password_hash 는 올리지 않는다)
_SESSION_KEYS = (
    "tg_chat_id",    "tg_token",        # 종가평균매매 텔레그램
    "sd_tg_chat_id", "sd_tg_token",     # 표준편차매매 텔레그램
    "gs_url", "gs_sheet",
    "a_buy", "a_sell", "sell_ratio", "divisions",
    "ticker_settings", "sd_ticker_settings",
    "dss_config",                       # DSS 동파법 설정
    "pdan_config",                      # 평단법 설정
    "tq200_config",                     # TQ200 설정
    "dstq_config",                      # 듀얼스나이퍼 TQ 설정
)


def _cookie_secret() -> bytes | None:
    """토큰 서명용 비밀값. 없으면 None → 자동 로그인만 꺼지고 수동 로그인은 그대로 된다.

    st.secrets["auth_cookie_secret"] 가 있으면 그것을, 없으면 이미 등록돼 있는
    서비스 계정 private_key 에서 파생한다 (Cloud 에 새 secret 을 추가하지 않아도 동작)."""
    try:
        raw = str(st.secrets.get("auth_cookie_secret", "") or "")
        if not raw:
            raw = str(st.secrets["gcp_service_account"]["private_key"])
    except Exception:
        return None
    if not raw:
        return None
    return hashlib.sha256(b"usd-avg-auth-cookie-v1|" + raw.encode()).digest()


def _sign(username: str, exp: int, password_hash: str) -> str | None:
    key = _cookie_secret()
    if key is None:
        return None
    msg = f"{username}\n{int(exp)}\n{password_hash}".encode()
    return hmac.new(key, msg, hashlib.sha256).hexdigest()


def _make_token(username: str, password_hash: str, now: float | None = None) -> str | None:
    """자동 로그인 토큰 발급. 서명 비밀값이 없으면 None."""
    exp = int((time.time() if now is None else now) + _COOKIE_DAYS * 86400)
    sig = _sign(username, exp, password_hash)
    if sig is None:
        return None
    u = base64.urlsafe_b64encode(username.encode()).decode().rstrip("=")
    return f"v1.{u}.{exp}.{sig}"


def _parse_token(token) -> tuple[str, int, str] | None:
    """토큰 → (아이디, 만료시각, 서명). 형식이 다르면 None (서명 검증은 하지 않는다)."""
    if not isinstance(token, str):
        return None
    parts = token.split(".")
    if len(parts) != 4 or parts[0] != "v1":
        return None
    try:
        u = base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)).decode()
        exp = int(parts[2])
    except Exception:
        return None
    if not u or not parts[3]:
        return None
    return u, exp, parts[3]


def _user_from_token(token, now: float | None = None) -> dict | None:
    """쿠키 토큰이 유효하면 users 시트의 해당 행(dict), 아니면 None."""
    parsed = _parse_token(token)
    if not parsed:
        return None
    username, exp, sig = parsed
    if exp < (time.time() if now is None else now):
        return None
    try:
        rows = _get_users_ws().get_all_records()
    except Exception:
        return None
    row = next((r for r in rows
                if str(r.get("username", "")).strip() == username.strip()), None)
    if not row:
        return None
    expected = _sign(username, exp, str(row.get("password_hash", "")))
    if expected is None or not hmac.compare_digest(expected, sig):
        return None
    return dict(row)


def _hidden_box():
    """쿠키 컴포넌트를 담는 보이지 않는 상자 (높이 0 iframe 이 만드는 빈 줄을 없앤다)."""
    st.html("<style>.st-key-auth_cookie_box{position:absolute;width:0;height:0;"
            "overflow:hidden}</style>")
    return st.container(key="auth_cookie_box")


def _queue_cookie_op(op: str, token: str = ""):
    """쿠키 저장("set")/삭제("remove")를 예약한다. 실제 실행은 _run_pending_cookie_op().

    저장 직후 바로 st.rerun() 하면 컴포넌트가 브라우저에서 실행되기 전에 지워진다.
    그래서 예약해 두고 이후 몇 초 동안의 실행마다 같은 컴포넌트를 계속 그린다."""
    exp = datetime.now(timezone.utc) + timedelta(days=_COOKIE_DAYS)
    st.session_state["_auth_cookie_op"] = {
        "op": op, "token": token, "t0": time.time(), "runs": 0,
        "expires": exp.strftime("%Y-%m-%dT%H:%M:%SZ")}


def _run_pending_cookie_op(box):
    job = st.session_state.get("_auth_cookie_op")
    if not job:
        return
    with box:
        if job["op"] == "set":
            _cookie_component(
                method="set", name=_COOKIE_NAME, value=job["token"],
                options={"path": "/", "expires": job["expires"],
                         "sameSite": "lax", "secure": True},
                key="_auth_cookie_set")
        else:
            for _i, _name in enumerate((_COOKIE_NAME, _OLD_COOKIE_NAME)):
                _cookie_component(method="remove", name=_name, options={"path": "/"},
                                  key=f"_auth_cookie_rm{_i}")
    job["runs"] += 1
    if job["runs"] >= 2 and time.time() - job["t0"] > _COOKIE_OP_KEEP_SEC:
        st.session_state.pop("_auth_cookie_op", None)


def logout():
    """로그아웃 — 세션을 비우고 자동 로그인 쿠키 삭제를 예약한다. 호출 후 st.rerun() 할 것."""
    for k in ("logged_in", "username", "user_settings"):
        st.session_state.pop(k, None)
    # 삭제가 브라우저에 반영되기 전의 낡은 쿠키로 곧바로 다시 로그인되지 않게 한다
    st.session_state["_auth_cookie_checked"] = True
    _queue_cookie_op("remove")


def render_login_gate():
    """클라우드 로그인 게이트. 로그인하지 않은 상태면 st.stop()으로 앱 실행을 중단한다.
    이 함수는 _IS_CLOUD 일 때만 호출해야 한다."""
    _box = _hidden_box()
    _run_pending_cookie_op(_box)          # 로그인 직후의 쿠키 저장 / 로그아웃 직후의 삭제
    if st.session_state.get("logged_in", False):
        return

    # ── 쿠키 자동 로그인 (새 탭 · 새로고침 · 브라우저 재시작 후에도 로그인 유지) ──
    # 쿠키 컴포넌트는 브라우저에서 뜬 뒤에야 값을 돌려준다 (그전에는 None).
    # 응답이 올 때까지 같은 컴포넌트를 계속 그리면서 최대 _COOKIE_WAIT_SEC 초 기다린다.
    if not st.session_state.get("_auth_cookie_checked", False):
        with _box:
            _cookies = _cookie_component(method="getAll", key="_auth_cookie_get",
                                         default=None)
        if _cookies is None:
            _t0 = st.session_state.setdefault("_auth_wait_t0", time.time())
            if time.time() - _t0 < _COOKIE_WAIT_SEC:
                st.caption("🔐 로그인 상태 확인 중…")
                time.sleep(0.25)
                st.rerun()
            # 시간 초과 → 아래 로그인 폼을 보여 준다. 컴포넌트는 그려 둔 상태라
            # 늦게라도 응답이 오면 그때 자동 로그인된다.
        else:
            st.session_state["_auth_cookie_checked"] = True
            _row = _user_from_token(_cookies.get(_COOKIE_NAME)
                                    if isinstance(_cookies, dict) else None)
            if _row:
                _name = str(_row.get("username", "")).strip()
                st.session_state.logged_in = True
                st.session_state.username = _name
                st.session_state.user_settings = {
                    k: _row.get(k, "") for k in _SESSION_KEYS}
                # 쓸 때마다 만료일을 30일 뒤로 다시 민다
                _tok = _make_token(_name, str(_row.get("password_hash", "")))
                if _tok:
                    _queue_cookie_op("set", _tok)
                st.rerun()

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
                            st.session_state["_auth_cookie_checked"] = True
                            # 30일 자동 로그인 쿠키 저장 (서명된 토큰) — 다음 실행부터
                            # 몇 초 동안 _run_pending_cookie_op() 가 브라우저에 쓴다
                            try:
                                _tok = _make_token(
                                    _u, str(_user.get("password_hash", "")))
                                if _tok:
                                    _queue_cookie_op("set", _tok)
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
