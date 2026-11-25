"""
표준편차매매 V 엔진 검증 (2026-11-25)
  ① V 꺼짐 = 기존 결과와 동일 / full 엔진 = fast 엔진
  ② V 켜짐 = 검증 스크립트(sd_vol_regime_2026-11.py)의 후보 규칙과 총자산 일치
  ③ 주문표 수량 = 다음 날 엔진 체결 수량 (2024-01-02 시작 계좌, 매 거래일)
     주문표는 LOC 가격을 센트로 반올림해 수량을 내므로 엔진과 1주 차이가 날 수 있다 (V 와 무관한 기존 차이)
     → 1주 이내 일치율 + 위험 달 주문의 완전 일치율을 본다
  ④ 전환일: 시작일 전까지는 V 꺼짐과 동일
"""
import sys, os, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "scripts"))
import numpy as np
import pandas as pd
import importlib.util
import stdev_engine as E

_spec = importlib.util.spec_from_file_location("sd1", os.path.join(HERE, "reports", "sd_vol_regime_2026-11.py"))
S1 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(S1)


def main():
    import update_preset_stats as U
    from update_preset_stats import extract_literal
    os.chdir(HERE)
    _px = U.load_adjusted("SOXL")
    U.load_adjusted = lambda tk, *a, **k: _px      # 검증 스크립트와 같은 가격을 쓰도록 고정
    load_adjusted = U.load_adjusted
    D = S1.prepare()
    db = {p["label"]: p for p in extract_literal("strategies/stdev.py", "_SD_PRESETS_DB")["SOXL"]}
    px = load_adjusted("SOXL")[["Close"]]
    end = str(px.index[-1].date())
    V = E.sd_vreg("1")
    CAND = (1.0, 100.0, 0.0, 0.0, 0.5)
    for lab in ("균형형", "공격형"):
        p = db[lab]
        kw = dict(sigma_period=p["sigma_period"], k_buy=p["k_buy"], k_sell=p["k_sell"], sell_ratio=p["sell_ratio"],
                  divisions=p["divisions"], renewal=p["renewal"], initial_capital=10000.0)
        off = E.run_backtest_stdev(px, S1.START, end, **kw)
        on = E.run_backtest_stdev(px, S1.START, end, vreg=V, **kw)
        f_off = E.run_backtest_stdev_fast(px, S1.START, end, **kw)
        f_on = E.run_backtest_stdev_fast(px, S1.START, end, vreg=V, **kw)
        a = D["arr"][lab]
        ref0 = S1.sim(a["c"], a["pc"], a["sg"], D["risk"][0.7], a["p"], None)
        ref1 = S1.sim(a["c"], a["pc"], a["sg"], D["risk"][0.7], a["p"], CAND)
        print(f"[{lab}] ① 꺼짐 차이 {np.abs(off['assets'] - ref0).max():.6f} · fast 차이(꺼짐/켜짐) "
              f"{abs(off['final_asset'] - f_off['final_asset']):.6f} / {abs(on['final_asset'] - f_on['final_asset']):.6f}"
              f" | ② 켜짐 차이 {np.abs(on['assets'] - ref1).max():.6f}")
        print(f"     기준 {off['cagr']*100:.2f}% / {off['mdd']*100:.2f}% / {off['calmar']:.2f} → "
              f"V {on['cagr']*100:.2f}% / {on['mdd']*100:.2f}% / {on['calmar']:.2f}")

        # ③ 주문표 = 엔진 체결
        st = "2024-01-02"
        full = E.run_backtest_stdev(px, st, end, vreg=V, return_history=True, **kw)["history"]
        full["날짜"] = pd.to_datetime(full["날짜"])
        full = full.set_index("날짜")
        days = [d for d in px.index if d >= pd.Timestamp("2024-03-01")]
        nb = ns = ok_b = ok_s = n_risk = rb = rb_ok = rs = rs_ok = 0
        for i in range(len(days) - 1):
            d, nxt = days[i], days[i + 1]
            r = E.run_stdev_ordersheet(px[px.index <= d], st, vreg=V, **kw)
            if r["vol_regime"] == "위험":
                n_risk += 1
            row = full.loc[nxt]
            risk = r["vol_regime"] == "위험"
            if row["매수량"] > 0:
                nb += 1; ok_b += int(abs(row["매수량"] - r["est_buy_qty"]) <= 1)
                rb += int(risk); rb_ok += int(risk and row["매수량"] == r["est_buy_qty"])
            if row["매도량"] > 0:
                ns += 1; ok_s += int(row["매도량"] == r["est_sell_qty"])
                rs += int(risk); rs_ok += int(risk and row["매도량"] == r["est_sell_qty"])
        print(f"     ③ 주문표=체결: 매수(1주 이내) {ok_b}/{nb} · 매도 {ok_s}/{ns} | 위험 달: 매수 {rb_ok}/{rb} · 매도 {rs_ok}/{rs}"
              f" (위험 달 주문일 {n_risk}/{len(days) - 1})")

        # ④ 전환일
        vf = "2026-11-02"
        sw = E.run_backtest_stdev(px, S1.START, end, vreg=E.sd_vreg("1", vf), return_history=True, **kw)["history"]
        base = E.run_backtest_stdev(px, S1.START, end, return_history=True, **kw)["history"]
        m = pd.to_datetime(base["날짜"]) < pd.Timestamp(vf)
        same = (sw.loc[m, ["보유량", "예수금"]].values == base.loc[m, ["보유량", "예수금"]].values).all()
        diff_after = int((sw.loc[~m, "보유량"].values != base.loc[~m, "보유량"].values).sum())
        print(f"     ④ 전환일 {vf}: 그 전 보유·예수금 동일 = {same} · 이후 달라진 날 {diff_after}/{int((~m).sum())}")


if __name__ == "__main__":
    main()
