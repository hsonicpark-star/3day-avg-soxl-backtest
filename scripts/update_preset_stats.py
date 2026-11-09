# -*- coding: utf-8 -*-
"""프리셋 성과 수치 재계산 → preset_stats.json (저장소 루트).

실행: python scripts/update_preset_stats.py
  - GitHub Actions(dual_sniper_mode.yml, 평일 10:00 KST)에서 매일 실행 후 커밋.
  - 로컬에서도 그대로 실행 가능.

대상: 종가평균 / 표준편차 / DSS / 듀얼스나이퍼 / IUO 의 프리셋 전부.
  프리셋 정의는 strategies/*.py 의 리터럴을 AST 로 읽는다 (Streamlit import 없음).
  각 전략은 앱과 동일한 엔진·동일한 가격 기준으로 계산한다.

원칙
  - 한 전략이 실패해도 나머지는 갱신하고, 실패한 전략의 기존 수치는 그대로 둔다.
  - 가격 데이터가 낡았거나(최근 종가 없음) 너무 짧으면 그 전략은 건너뛴다.
"""
import ast
import json
import os
import sys
import time
import traceback
from datetime import datetime

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from common.preset_stats import STATS_PATH, param_sig, item_key   # noqa: E402

CAP = 10000.0


# ── 프리셋 리터럴 추출 ────────────────────────────────────────
def extract_literal(rel_path: str, name: str):
    src = open(os.path.join(ROOT, rel_path), encoding="utf-8").read()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise KeyError(f"{rel_path}: {name} 를 찾지 못함")


# ── 가격 로드 ────────────────────────────────────────────────
def _drop_incomplete_today(df: pd.DataFrame) -> pd.DataFrame:
    now_et = pd.Timestamp.now(tz="America/New_York")
    if now_et < now_et.replace(hour=16, minute=30, second=0, microsecond=0):
        df = df[df.index.normalize() < pd.Timestamp(now_et.date())]
    return df


def load_adjusted(ticker: str, start: str = "2010-01-01") -> pd.DataFrame:
    """조정 종가('Close') — 종가평균·표준편차·IUO 웹과 같은 기준 (auto_adjust=True)."""
    import yfinance as yf
    last_err = None
    for i in range(3):
        try:
            d = yf.download(ticker, start=start, progress=False, auto_adjust=True)
            if isinstance(d.columns, pd.MultiIndex):
                d.columns = d.columns.get_level_values(0)
            d.index = pd.to_datetime(d.index).tz_localize(None)
            d = _drop_incomplete_today(d.dropna(subset=["Close"]))
            if len(d) > 500:
                return d
            last_err = RuntimeError(f"{ticker}: 데이터 {len(d)}행")
        except Exception as e:      # noqa: BLE001
            last_err = e
        time.sleep(3 + 4 * i)
    raise last_err


def check_fresh(index, label: str):
    """최근 종가가 10일 넘게 낡았으면 중단 (낡은 데이터로 수치를 덮어쓰지 않음)."""
    age = (pd.Timestamp(datetime.utcnow().date()) - pd.Timestamp(index[-1]).normalize()).days
    if age > 10:
        raise RuntimeError(f"{label}: 최근 종가 {pd.Timestamp(index[-1]).date()} ({age}일 전) — 낡음")


def metrics(equity: pd.Series):
    eq = equity.astype(float)
    years = (eq.index[-1] - eq.index[0]).days / 365.25
    cagr = (eq.iloc[-1] / eq.iloc[0]) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    mdd = float((eq / eq.cummax() - 1.0).min())
    return float(cagr), mdd


def entry(preset, cagr, mdd, start, end, win=None, note=None):
    e = {"sig": param_sig(preset), "cagr": round(float(cagr), 6), "mdd": round(float(mdd), 6),
         "calmar": round(float(cagr) / abs(float(mdd)), 4) if mdd else 0.0,
         "start": str(pd.Timestamp(start).date()), "end": str(pd.Timestamp(end).date())}
    if win is not None:
        e["win"] = round(float(win), 4)
    if note:
        e["note"] = note
    return e


# ── 전략별 계산 ───────────────────────────────────────────────
def run_avg():
    import avg_close_engine as E
    db = extract_literal("strategies/avg_close.py", "_AVG_PRESETS_DB")
    out = {}
    qqq = None
    for tk, presets in db.items():
        px = load_adjusted(tk)[["Close"]]
        check_fresh(px.index, f"종가평균/{tk}")
        s, e = px.index[0], px.index[-1]
        for p in presets:
            mode, note = None, None
            if p.get("rsi_mode"):
                # RSI 모드형: 앱과 같은 판정(QQQ 주간 RSI, DSS 와 같은 로더)을 거래일별로 붙여 계산
                if qqq is None:
                    from dss_engine import load_price_data as _dss_load
                    qqq = _dss_load("QQQ", "2009-01-01", "2027-01-01")
                    check_fresh(qqq.index, "종가평균/QQQ(RSI 모드)")
                mode = E.make_rsi_mode(qqq, px.index, datetime.utcnow().date(),
                                       p["up_a_buy"], p["up_a_sell"])
                note = "RSI 모드 반영"
            r = E.run_backtest(px, str(s.date()), str(e.date()), p["a_buy"], p["a_sell"],
                               p["sell_ratio"], p["divisions"], CAP, n_days=p.get("n_days", 2), mode=mode)
            win = r["win_count"] / r["sell_count"] if r["sell_count"] else None
            out[item_key("avg", tk, p["label"])] = entry(p, r["cagr"], r["mdd"], r["dates"][0], e, win, note)
    return out


def run_stdev():
    import stdev_engine as E
    db = extract_literal("strategies/stdev.py", "_SD_PRESETS_DB")
    out = {}
    for tk, presets in db.items():
        px = load_adjusted(tk)[["Close"]]
        check_fresh(px.index, f"표준편차/{tk}")
        s, e = px.index[0], px.index[-1]
        for p in presets:
            r = E.run_backtest_stdev(
                px, str(s.date()), str(e.date()),
                sigma_period=p["sigma_period"], k_buy=p["k_buy"], k_sell=p["k_sell"],
                sell_ratio=p["sell_ratio"], divisions=p["divisions"], renewal=p["renewal"],
                pcr=1.0, lcr=1.0, initial_capital=CAP, return_history=False)
            win = r["win_count"] / r["sell_count"] if r["sell_count"] else None
            out[item_key("stdev", tk, p["label"])] = entry(p, r["cagr"], r["mdd"], r["dates"][0], e, win)
    return out


def run_dss():
    from dss_engine import (load_price_data, build_weekly_rsi_series, build_mode_series,
                            DSSParams, run_backtest, parse_tier_weights)
    presets = extract_literal("strategies/dss.py", "_DSS_PRESETS")
    soxl = load_price_data("SOXL", "2009-06-01", "2027-01-01")
    qqq = load_price_data("QQQ", "2009-01-01", "2027-01-01")
    check_fresh(soxl.index, "DSS/SOXL")
    mode_series = build_mode_series(build_weekly_rsi_series(qqq))
    start, end = "2010-03-12", str(pd.Timestamp(soxl.index[-1]).date())
    out = {}
    for p in presets:
        params = DSSParams(
            sf_divisions=p["sf_div"], sf_max_hold=p["sf_hold"],
            sf_buy_pct=p["sf_buy"] / 100, sf_sell_pct=p["sf_sell"] / 100,
            ag_divisions=p["ag_div"], ag_max_hold=p["ag_hold"],
            ag_buy_pct=p["ag_buy"] / 100, ag_sell_pct=p["ag_sell"] / 100,
            initial_capital=CAP, fee_rate=p["fee_rate"] / 100,
            renewal_period=p["renewal_period"], pcr=p["pcr"] / 100, lcr=p["lcr"] / 100,
            sf_tier_weights=parse_tier_weights(p.get("sf_w", "")),
            ag_tier_weights=parse_tier_weights(p.get("ag_w", "")),
        )
        log = run_backtest(params=params, soxl_daily=soxl, mode_series=mode_series,
                           start_date=start, end_date=end)
        eq = pd.Series(log["총자산"].values.astype(float), index=pd.to_datetime(log["날짜"]))
        cagr, mdd = metrics(eq)
        out[item_key("dss", "SOXL", p["label"])] = entry(p, cagr, mdd, eq.index[0], eq.index[-1])
    return out


def run_dual():
    from dual_sniper_engine import (load_price_data, DualSniperParams, build_original_mode_map,
                                    load_original_modes, run_backtest)
    presets = extract_literal("strategies/dual_sniper.py", "_DS_PRESETS")
    px = load_price_data("SOXL", "2009-01-01", "2027-01-01")
    check_fresh(px.index, "듀얼스나이퍼/SOXL")
    modes = load_original_modes()
    if modes.empty:
        raise RuntimeError("dual_sniper_modes.csv 없음")
    start = str(pd.Timestamp(modes["date"].iloc[0]).date())
    mode_map = build_original_mode_map(px, extra_modes={})
    out = {}
    for p in presets:
        div = int(p["sf_div"])
        w = tuple(float(x) for x in str(p["sf_weights"]).replace(" ", "").split(",") if x != "")
        if len(w) != div:
            raise RuntimeError(f"{p['label']}: 방어 비중 개수 {len(w)} ≠ 분할 {div}")
        params = DualSniperParams(
            ag_divisions=p["ag_div"], ag_buy_pct=p["ag_buy"],
            ag_sell_alpha=p["ag_sell_alpha"], ag_hold_alpha=p["ag_hold_alpha"],
            sf_divisions=div, sf_buy_pct1=p["sf_buy1"], sf_buy_pct2=p["sf_buy2"],
            sf_sell_pct=p["sf_sell"], sf_ma_base=p["sf_ma_base"], sf_hold=p["sf_hold"],
            sf_tier_weights=w, initial_capital=CAP)
        res = run_backtest(px, params, mode_map=mode_map, start_date=start, return_trades=True)
        log = res[0] if isinstance(res, tuple) else res
        log = log[log["날짜"] >= pd.Timestamp(start)]
        eq = pd.Series(log["총자산"].values.astype(float), index=pd.to_datetime(log["날짜"]))
        cagr, mdd = metrics(eq)
        out[item_key("dual", "SOXL", p["label"])] = entry(
            p, cagr, mdd, eq.index[0], eq.index[-1], note="원전략 실제모드")
    return out


def run_iuo():
    from iuo_engine import IUOParams, run_backtest
    presets = extract_literal("strategies/iuo.py", "_IUO_PRESETS")
    px = load_adjusted("SOXL", "2009-01-01")
    check_fresh(px.index, "IUO/SOXL")
    # IUO 프리셋은 원래 2015-12-31 시작 기준으로 산출됨 (엔진 기본값) — 같은 기준 유지.
    # 검증: 2015-12-31~2026-04-10 공격형 CAGR 69.36% / MDD -44.05% = 기존 표기와 일치.
    start, end = "2015-12-31", str(px.index[-1].date())
    out = {}
    for p in presets:
        params = IUOParams(
            initial_capital=CAP, first_buy_ratio=p["first_buy_ratio"] / 100,
            buy1_pct=p["buy1_pct"] / 100, buy2_pct=p["buy2_pct"] / 100,
            sell_pct=p["sell_pct"] / 100, moc_days=int(p["moc_days"]),
            max_additional_buys=int(p["max_add_buys"]), divisions=int(p["divisions"]))
        r = run_backtest(params, px, None, start, end)
        log = pd.DataFrame(r["daily_log"])
        dcol = "날짜" if "날짜" in log.columns else log.columns[0]
        acol = next(c for c in ("총자산", "총자산($)", "total_asset") if c in log.columns)
        eq = pd.Series(pd.to_numeric(log[acol], errors="coerce").values, index=pd.to_datetime(log[dcol])).dropna()
        cagr, mdd = metrics(eq)
        sr = r.get("sell_records") or []
        win = None
        m = r.get("metrics") or {}
        if "승률(%)" in m:
            win = float(m["승률(%)"]) / 100
        out[item_key("iuo", "SOXL", p["label"])] = entry(p, cagr, mdd, eq.index[0], eq.index[-1], win)
    return out


RUNNERS = [("종가평균", "avg", run_avg), ("표준편차", "stdev", run_stdev), ("DSS", "dss", run_dss),
           ("듀얼스나이퍼", "dual", run_dual), ("IUO", "iuo", run_iuo)]


def main():
    try:
        old = json.load(open(STATS_PATH, encoding="utf-8"))
    except Exception:
        old = {}
    items = dict(old.get("items") or {})
    failed = []
    for name, key, fn in RUNNERS:
        t0 = time.time()
        try:
            new = fn()
            # 이 전략의 옛 항목 중 더 이상 없는 프리셋은 제거, 나머지는 교체
            for k in [k for k in items if k.startswith(key + "/") and k not in new]:
                items.pop(k)
            items.update(new)
            print(f"✅ {name}: {len(new)}개 갱신 ({time.time() - t0:.1f}s)")
        except Exception as e:      # noqa: BLE001
            failed.append(name)
            print(f"⚠️ {name}: 실패 — 기존 수치 유지 ({e})")
            traceback.print_exc()

    if items == (old.get("items") or {}):
        print("변경 없음 — 파일 유지")
        return 0
    out = {"generated": datetime.utcnow().strftime("%Y-%m-%dT%H:%MZ"),
           "basis": "초기 $10,000 · 종가 체결 가정 · 각 전략의 앱 엔진·가격 기준과 동일",
           "items": dict(sorted(items.items()))}
    with open(STATS_PATH, "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
        f.write("\n")
    print(f"저장: {STATS_PATH} ({len(items)}개 항목" + (f", 실패 {failed}" if failed else "") + ")")
    return 0


if __name__ == "__main__":
    sys.exit(main())
