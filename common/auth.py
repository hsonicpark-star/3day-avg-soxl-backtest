from __future__ import annotations

import streamlit as st
from streamlit_cookies_controller import CookieController

from common.config import _IS_CLOUD, _get_gspread_client

_cookie_mgr = CookieController()


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


def render_login_gate():
    """클라우드 로그인 게이트. 로그인하지 않은 상태면 st.stop()으로 앱 실행을 중단한다.
    이 함수는 _IS_CLOUD 일 때만 호출해야 한다."""
    # ── 쿠키 컴포넌트 초기화 대기 ──────────────────────────────
    # streamlit-cookies-controller 는 React 컴포넌트라서
    # 첫 렌더링에서 get()이 항상 None을 반환함.
    # st.stop() 대신 time.sleep + st.rerun()을 사용하여
    # React 컴포넌트 로딩과 교착(deadlock)을 방지한다.
    _rerun_count = st.session_state.get("_cookie_rerun", 0)
    if _rerun_count < 2:
        st.session_state["_cookie_rerun"] = _rerun_count + 1
        import time
        time.sleep(0.4)
        st.rerun()

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
                            "dss_config",                       # DSS 동파법 설정
                            "pdan_config",                      # 평단법 설정
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
