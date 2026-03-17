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

# ── 실행 환경 감지 ──────────────────────────────────────────
# Streamlit Cloud는 HOME=/home/appuser 또는 환경변수로 식별
_IS_CLOUD = (
    os.environ.get("STREAMLIT_SHARING_MODE") == "1"
    or str(Path.home()) == "/home/appuser"
    or os.environ.get("IS_STREAMLIT_CLOUD", "") == "1"
)

# ── config 경로 ──────────────────────────────────────────────
# 로컬: C:\Users\{이름}\.soxl\config.json  (각자 PC에 독립 저장)
# 클라우드: 앱 디렉토리 (비민감 정보만, 공유 서버)
_OLD_CONFIG = Path(__file__).parent / "config.json"   # 이전 경로 (마이그레이션용)
if _IS_CLOUD:
    _CONFIG = _OLD_CONFIG
else:
    _CONFIG = Path.home() / ".soxl" / "config.json"
    _CONFIG.parent.mkdir(parents=True, exist_ok=True)
    # 이전 경로(앱 폴더)에 config가 있고 새 경로에 아직 없으면 자동 마이그레이션
    if _OLD_CONFIG.exists() and not _CONFIG.exists():
        try:
            import shutil
            shutil.copy2(_OLD_CONFIG, _CONFIG)
        except:
            pass

_SENSITIVE_KEYS = {"tg_chat_id", "tg_token", "gs_url", "gs_sheet"}

def load_config():
    if _CONFIG.exists():
        try:
            return json.loads(_CONFIG.read_text(encoding="utf-8"))
        except:
            return {}
    return {}

def save_config(data: dict, sensitive: bool = False):
    """sensitive=True 이면 민감 정보 포함. 클라우드에서는 민감 정보 저장 안 함."""
    try:
        cfg = load_config()
        for k, v in data.items():
            if k in _SENSITIVE_KEYS and _IS_CLOUD:
                continue  # 클라우드에서 민감 정보 저장 차단
            cfg[k] = v
        _CONFIG.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    except:
        pass

# 클라우드 서버에 혹시 남은 민감 정보 제거
if _IS_CLOUD:
    try:
        cfg = load_config()
        if any(k in cfg for k in _SENSITIVE_KEYS):
            for k in _SENSITIVE_KEYS:
                cfg.pop(k, None)
            _CONFIG.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    except:
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
    url = st.secrets.get("admin_sheet_url", "")
    if not url:
        raise RuntimeError("Streamlit Secrets에 admin_sheet_url이 설정되지 않았습니다.")
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
    """users 시트에서 해당 유저 행의 설정 컬럼 업데이트."""
    ws = _get_users_ws()
    headers = ws.row_values(1)
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
st.set_page_config(page_title="종가평균매매 백테스트", layout="wide")

# ── 클라우드: 로그인 게이트 ────────────────────────────────────
if _IS_CLOUD:
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
                            st.session_state.logged_in      = True
                            st.session_state.username        = _u
                            st.session_state.user_settings   = _user
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

    ticker = st.text_input("종목코드 (Ticker)", "SOXL")

    st.markdown("---")
    st.subheader("전략 파라미터")
    a_buy      = st.number_input("매수기준 (a값)", value=-0.005, step=0.001, format="%.4f")
    a_sell     = st.number_input("매도기준 (a값)", value= 0.009, step=0.001, format="%.4f")
    sell_ratio = st.number_input("매도비율 (%)", value=100.0, step=10.0, min_value=0.0, max_value=100.0)
    divisions  = st.number_input("분할수", value=5, min_value=1, step=1)

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
    prev_asset = float(initial_capital)
    assets     = np.empty(len(closes))
    buy_count  = sell_count = 0
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
                action = "SELL"; trade_shares = -sell_qty; trade_amount = sell_qty * x
                cash += trade_amount; shares -= sell_qty; sell_count += 1
        elif x <= tb:
            buy_qty = min(math.floor(current_chunk / x + 1e-9), math.floor(cash / x + 1e-9))
            if buy_qty > 0:
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

    out = dict(
        final_asset=final_asset, total_return=total_return,
        cagr=cagr, mdd=mdd, calmar=calmar,
        buy_count=buy_count, sell_count=sell_count,
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

        if shares > 0 and x >= ts:
            sell_qty = math.floor(shares * (sell_ratio / 100.0))
            if sell_qty > 0:
                oldest_date  = open_tiers[0]["date"] if open_tiers else date
                holding_days = (date - oldest_date).days
                factor       = x / avg_cost if avg_cost > 0 else 0.0

                sell_trades.append({
                    "구분":    "매도",
                    "티커":    ticker_name,
                    "주문가":  x,
                    "avg_cost": avg_cost,
                    "수량":    sell_qty,
                    "금액":    sell_qty * x,
                    "보유기간": holding_days,
                    "비고":    f"평단 ${avg_cost:.2f} × {factor:.4f} = ${x:.2f}",
                })

                cash   += sell_qty * x
                shares -= sell_qty

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

        asset      = cash + shares * x
        prev_asset = asset
        peak_asset = max(peak_asset, asset)

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
# TAB 2 – 파라미터 최적화
# ══════════════════════════════════════════════
with tab2:
    st.subheader("🔍 파라미터 그리드 탐색")
    st.caption("a_buy / a_sell 구간을 격자 탐색하여 최적 파라미터 조합을 찾습니다.")

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

        st.markdown("**분할수 / 매도비율**")
        rc7, rc8 = st.columns(2)
        div_opts       = rc7.multiselect("분할수",      options=[1,2,3,4,5,6,7,8,10], default=[5])
        sellratio_opts = rc8.multiselect("매도비율 (%)", options=[50,60,70,80,90,100],  default=[100])

        metric_key = st.selectbox("최적화 기준 지표", [
            "Calmar Ratio (CAGR / MDD)",
            "CAGR (%)",
            "총수익률 (%)",
            "MDD 최소화 (작을수록 좋음)",
        ])

    ab_vals = np.round(np.arange(ab_min, ab_max + ab_step * 0.5, ab_step), 6).tolist()
    as_vals = np.round(np.arange(as_min, as_max + as_step * 0.5, as_step), 6).tolist()
    dv_list = div_opts       if div_opts       else [5]
    sr_list = sellratio_opts if sellratio_opts else [100]
    n_total = len(ab_vals) * len(as_vals) * len(dv_list) * len(sr_list)

    info_msg = (f"예상 조합 수: **{n_total:,}개** "
                f"(a_buy {len(ab_vals)} × a_sell {len(as_vals)} "
                f"× 분할수 {len(dv_list)} × 매도비율 {len(sr_list)})")
    if n_total > 10000:
        st.error(info_msg + "  \n조합이 10,000개를 초과합니다. 범위를 줄이거나 간격을 늘려주세요.")
    elif n_total > 3000:
        st.warning(info_msg + "  \n조합이 많아 다소 시간이 걸릴 수 있습니다.")
    else:
        st.info(info_msg)

    if st.button("▶ 최적화 실행", type="primary", key="run_opt",
                 disabled=(n_total > 10000 or n_total == 0)):
        with st.spinner("가격 데이터 로드 중..."):
            price_df_opt = load_price_data(ticker, start_date, end_date, data_source, excel_file)
        if price_df_opt.empty:
            st.error("가격 데이터를 불러오지 못했습니다.")
            st.stop()

        progress     = st.progress(0.0, text="최적화 실행 중...")
        update_every = max(1, n_total // 100)
        rows  = []
        count = 0

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
                                              text=f"최적화 실행 중... {count:,} / {n_total:,}")

        progress.progress(1.0, text="완료!")
        if not rows:
            st.error("유효한 결과가 없습니다.")
            st.stop()

        res_df = pd.DataFrame(rows)
        if "Calmar" in metric_key:   sort_col, asc = "Calmar",    False
        elif "CAGR" in metric_key:   sort_col, asc = "CAGR(%)",   False
        elif "총수익률" in metric_key: sort_col, asc = "총수익(%)", False
        else:                         sort_col, asc = "MDD(%)",    False
        res_df = res_df.sort_values(sort_col, ascending=asc).reset_index(drop=True)

        st.subheader(f"🏆 상위 20개 결과  ({sort_col} 기준)")
        st.dataframe(res_df.head(20).style.format({
            "a_buy": "{:.4f}", "a_sell": "{:.4f}",
            "CAGR(%)": "{:.2f}%", "MDD(%)": "{:.2f}%",
            "Calmar": "{:.4f}", "총수익(%)": "{:.2f}%",
            "최종자산($)": "${:,.2f}",
        }), use_container_width=True)

        st.subheader(f"🗺️ 히트맵: a_buy × a_sell  →  {sort_col}")
        hmap_data = (
            res_df.groupby(["a_buy", "a_sell"])[sort_col].max().reset_index()
            .pivot(index="a_sell", columns="a_buy", values=sort_col)
        )
        show_text = (len(ab_vals) * len(as_vals)) <= 400
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
                           file_name=f"optimization_{ticker}.csv", mime="text/csv")


# ══════════════════════════════════════════════
# TAB 3 – 오늘의 주문표
# ══════════════════════════════════════════════
with tab3:
    st.subheader("📋 오늘의 주문표")
    st.caption("시뮬레이션 시작일부터 오늘까지 포트폴리오를 추적하여 현황과 내일 LOC 주문을 표시합니다.")

    # URL 파라미터 > config.json > 기본값 순으로 초기값 결정
    _qp  = st.query_params
    _cfg = load_config()

    _raw_start   = _qp.get("start")   or _cfg.get("os_start",   "2024-01-01")
    _raw_capital = _qp.get("capital") or str(_cfg.get("os_capital", initial_capital))
    try:    _default_start = datetime.strptime(_raw_start, "%Y-%m-%d").date()
    except: _default_start = datetime(2024, 1, 1).date()
    try:    _default_capital = float(_raw_capital)
    except: _default_capital = float(initial_capital)

    c1, c2 = st.columns(2)
    os_start = c1.date_input(
        "시작일",
        value=_default_start,
        min_value=datetime(2000, 1, 1).date(),
        max_value=datetime.today().date(),
        key="os_start",
    )
    os_capital = c2.number_input("시작 자본 ($)", value=_default_capital,
                                  step=1000.0, key="os_capital")

    if st.button("📋 주문표 로드", type="primary", key="run_os"):
        # URL 파라미터 & config.json 동시 저장
        st.query_params["start"]   = str(os_start)
        st.query_params["capital"] = str(int(os_capital))
        save_config({"os_start": str(os_start), "os_capital": os_capital})
        st.info(f"🔗 설정이 URL에 저장되었습니다. 주소창 URL을 즐겨찾기에 추가하면 다음에 같은 설정으로 바로 접속됩니다.\n\n`?start={os_start}&capital={int(os_capital)}`")
        today = datetime.today().date()
        with st.spinner("데이터 로드 및 포트폴리오 시뮬레이션 중..."):
            price_df_os = load_price_data(ticker, os_start, today, data_source, excel_file)

        if price_df_os.empty:
            st.error("가격 데이터를 불러오지 못했습니다.")
            st.stop()

        res = run_portfolio_for_ordersheet(
            price_df_os, os_start, ticker,
            a_buy, a_sell, sell_ratio, divisions, os_capital,
        )
        if res is None:
            st.warning("시뮬레이션 데이터가 없습니다.")
            st.stop()

        # ── 날짜 헤더 ──
        st.markdown(f"**{res['start_date']} ~ {res['end_date']}**")

        # ── 요약 카드 ──
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("시작 자본",  f"${res['initial_capital']:,.0f}")
        m2.metric("현재 자산",  f"${res['current_asset']:,.0f}",
                  delta=f"{res['total_return']*100:+.2f}%")
        m3.metric("수익률",     f"{res['total_return']*100:+.2f}%",
                  delta=f"CAGR {res['cagr']*100:.2f}%")
        m4.metric("현재 DD",    f"{abs(res['current_dd'])*100:.2f}%",
                  delta=f"{res['current_dd']*100:.2f}%", delta_color="inverse")
        m5.metric("주식 비중",  f"{res['stock_weight']*100:.1f}%")

        # ── 오늘의 LOC 주문 ──
        lp   = res["latest_price"]
        p1   = res["p1_now"]
        p2   = res["p2_now"]
        st.subheader("📑 오늘의 LOC 주문")
        st.caption(
            f"기준: p1(전일 종가) = **${p1:,.2f}** · "
            f"p2(전전일 종가) = **${p2:,.2f}** · "
            f"최근 확인 가격 = **${lp:,.2f}**"
        )

        today_orders = []

        # 매도 LOC (보유 시)
        if res["shares"] > 0:
            sell_qty_today = math.floor(res["shares"] * (sell_ratio / 100.0))
            sell_tgt       = res["next_sell_target"]
            vs_lp_sell     = (sell_tgt / lp - 1) * 100 if lp > 0 else 0
            vs_avg_sell    = (sell_tgt / res["avg_cost"] - 1) * 100 if res["avg_cost"] > 0 else 0
            today_orders.append({
                "구분":         "매도",
                "티커":         ticker,
                "LOC 기준가":   f"${sell_tgt:,.2f}",
                "1회매수금":    "-",
                "예상수량":     f"{sell_qty_today:,}주",
                "예상금액":     f"${sell_qty_today * sell_tgt:,.2f}",
                "전일종가 대비": f"{vs_lp_sell:+.2f}%",
                "비고":         (f"평단 ${res['avg_cost']:.2f} 대비 {vs_avg_sell:+.2f}%  |  "
                                  f"보유 {res['shares']:,}주 × {sell_ratio:.0f}%"),
            })

        # 매수 LOC
        buy_p    = res["next_buy_primary"]
        qty_p    = res["pending_buys"][0]["수량"]
        chunk    = res["current_asset"] / divisions
        vs_lp_bp = (buy_p / lp - 1) * 100 if lp > 0 else 0
        today_orders.append({
            "구분":         "매수",
            "티커":         ticker,
            "LOC 기준가":   f"${buy_p:,.2f}",
            "1회매수금":    f"${chunk:,.2f}",
            "예상수량":     f"{qty_p:,}주",
            "예상금액":     f"${qty_p * buy_p:,.2f}",
            "전일종가 대비": f"{vs_lp_bp:+.2f}%",
            "비고":         res["pending_buys"][0]["비고"],
        })

        df_order = pd.DataFrame(today_orders)

        def style_gubun(row):
            styles = [""] * len(row)
            col_list = list(row.index)
            if "구분" in col_list:
                idx = col_list.index("구분")
                if row["구분"] == "매도":
                    styles[idx] = "color: #1565C0; font-weight: bold"
                elif row["구분"] == "매수":
                    styles[idx] = "color: #C62828; font-weight: bold"
            return styles

        st.dataframe(
            df_order.style.apply(style_gubun, axis=1),
            use_container_width=True,
            hide_index=True,
            height=38 + 35 * len(today_orders),
        )

        # ── 현재 보유 현황 ──
        st.subheader("📦 현재 보유 현황")
        if res["shares"] > 0:
            lp       = res["latest_price"]
            avg_c    = res["avg_cost"]
            unrealized = (lp - avg_c) * res["shares"]
            hold_cols = st.columns(6)
            hold_cols[0].metric("보유주수",  f"{res['shares']:,}주")
            hold_cols[1].metric("평균단가",  f"${avg_c:.2f}")
            hold_cols[2].metric("현재가",    f"${lp:.2f}")
            hold_cols[3].metric("평가금액",  f"${res['shares']*lp:,.2f}")
            hold_cols[4].metric("평가손익",  f"${unrealized:,.2f}",
                                delta=f"{(lp/avg_c-1)*100:+.2f}%" if avg_c > 0 else "")
            hold_cols[5].metric("보유현금",  f"${res['cash']:,.2f}")

            # 티어 상세
            if res["open_tiers"]:
                with st.expander(f"보유 티어 상세  ({len(res['open_tiers'])}개 배치)"):
                    tiers_rows = []
                    for t in res["open_tiers"]:
                        buy_date = t["date"].date() if hasattr(t["date"], "date") else t["date"]
                        holding  = (datetime.today().date() - buy_date).days
                        pnl_pct  = (lp / t["price"] - 1) * 100 if t["price"] > 0 else 0
                        tiers_rows.append({
                            "매수일":    str(buy_date),
                            "매수가":    f"${t['price']:.2f}",
                            "수량":      f"{t['qty']:,}주",
                            "매수금액":  f"${t['price']*t['qty']:,.2f}",
                            "현재손익률": f"{pnl_pct:+.2f}%",
                            "보유일수":  f"{holding}일",
                        })
                    st.dataframe(pd.DataFrame(tiers_rows),
                                 hide_index=True, use_container_width=True)
        else:
            st.info("현재 보유 주식 없음 (전량 현금)")
            st.metric("보유현금", f"${res['cash']:,.2f}")


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
    st.caption("사이드바의 공통 설정(티커 · 파라미터 · 기간 · 초기 자본)을 기준으로 분석합니다.")

    if st.button("▶ 성과 분석 실행", type="primary", key="run_perf"):
        with st.spinner("데이터 로드 및 분석 중..."):
            price_df_perf = load_price_data(ticker, start_date, end_date, data_source, excel_file)

        if price_df_perf.empty:
            st.error("가격 데이터를 불러오지 못했습니다.")
            st.stop()

        res_p = run_backtest(
            price_df_perf, start_date, end_date,
            a_buy, a_sell, sell_ratio, divisions, initial_capital,
            return_history=True,
        )
        if res_p is None:
            st.warning("선택된 기간 내 거래 데이터가 없습니다.")
            st.stop()

        hist = res_p["history"]

        # 전체 요약
        sm1, sm2, sm3, sm4 = st.columns(4)
        sm1.metric("전체 CAGR",    f"{res_p['cagr']*100:.2f}%")
        sm2.metric("전체 수익률",  f"{res_p['total_return']*100:+.2f}%")
        sm3.metric("최대 MDD",     f"{res_p['mdd']*100:.2f}%")
        sm4.metric("Calmar Ratio", f"{res_p['calmar']:.3f}")

        st.divider()

        # 연도별 성과 테이블
        st.subheader("📅 연도별 성과")
        annual_df = compute_annual_stats(hist, initial_capital)

        def _color_ret(val):
            if isinstance(val, (int, float)):
                if val > 0:  return "color: #2e7d32; font-weight:bold"
                if val < 0:  return "color: #c62828; font-weight:bold"
            return ""

        st.dataframe(
            annual_df.style
                .applymap(_color_ret, subset=["연간수익률(%)"])
                .format({"연간수익률(%)": "{:+.2f}%", "MDD(%)": "{:.2f}%"}),
            hide_index=True, use_container_width=True,
        )

        st.divider()

        # 월별 수익률 히트맵
        st.subheader("🗓️ 월별 수익률 히트맵")
        monthly_pivot = compute_monthly_pivot(hist, initial_capital)

        fig_m = px.imshow(
            monthly_pivot,
            color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
            text_auto=".1f",
            labels={"x": "월", "y": "연도", "color": "수익률(%)"},
            aspect="auto",
        )
        fig_m.update_layout(
            height=max(320, len(monthly_pivot) * 38 + 120),
            coloraxis_colorbar=dict(title="수익률(%)"),
        )
        st.plotly_chart(fig_m, use_container_width=True)


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
**① Bot Token 발급**
1. 텔레그램에서 `@BotFather` 검색
2. `/newbot` 명령어 입력
3. 봇 이름 & 아이디 설정 후 **Token** 수령

**② Chat ID 확인**
1. 생성한 봇에 메시지 아무거나 전송
2. 브라우저에서 아래 URL 접속:
   `https://api.telegram.org/bot{TOKEN}/getUpdates`
3. `"chat":{"id": 숫자}` 부분이 Chat ID
""")

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
                    with st.spinner("시뮬레이션 & 발송 중..."):
                        _cfg_tg  = load_config()
                        _tg_start = _cfg_tg.get("os_start", "2024-01-01")
                        _tg_cap   = float(_cfg_tg.get("os_capital", initial_capital))
                        try:    _tg_start_d = datetime.strptime(_tg_start, "%Y-%m-%d").date()
                        except: _tg_start_d = datetime(2024, 1, 1).date()
                        msg = _build_order_text(
                            ticker, a_buy, a_sell, sell_ratio, divisions,
                            _tg_start_d, _tg_cap,
                        )
                        result = _send_telegram(tg_token, tg_chat_id, msg)
                    if result.get("ok"):
                        st.success("✅ 텔레그램 발송 성공!")
                    else:
                        st.error(f"❌ 발송 실패: {result.get('description', '알 수 없는 오류')}")
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
            with st.popover("❓ 시트 URL 확인 & 권한 부여", use_container_width=True):
                st.markdown("""
**① 스프레드시트 URL 복사**
- 구글 스프레드시트를 열고 주소창 URL 전체를 복사해 붙여넣기

**② 서비스 계정 이메일 공유**
- 스프레드시트 우측 상단 **공유** 클릭
- 아래 이메일을 **편집자**로 추가:

```
connectspreadsheet@sodium-gateway-485307-f3.iam.gserviceaccount.com
```

**③ 저장 후 테스트**
- URL 저장 → "시트 연결 테스트" 버튼으로 확인
""")

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
