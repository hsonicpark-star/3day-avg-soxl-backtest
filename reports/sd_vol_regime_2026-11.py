"""
표준편차매매 × 변동성 레짐 본 검증 (2026-11-25)

레짐 = DSS 와 같은 정의 (dss_engine.vol_regime_table: 월말 SOXX 20일 변동성 상승 & 3년 백분위 ≥ 기준 → 다음 달 위험).
매수·매도 판정식은 그대로 두고, 위험 달에만 아래 값을 바꾼다 (전부 장 전 확정 정보).

  mult  하루 매수 한도 × 배율        (분할수 확대와 같은 효과: 4분할 ×0.5 = 8분할 금액)
  sr    매도 비율을 이 값으로         (None = 프리셋 그대로)
  kb    k_buy 에 더하는 값            (음수 = 더 깊이 빠진 날만 매수)
  ks    k_sell 에 더하는 값           (음수 = 더 빨리 매도)
  cap   보유 평가금 상한 (전일 총자산 대비) — 넘으면 신규 매수 안 함

엔진은 stdev_engine.run_backtest_stdev_fast 를 그대로 옮긴 실험용 루프 (규칙 없음 = 원본과 동일함을 먼저 확인).
검증: 전체 · 앞/뒤 절반 · 봉인(2024~) · 구간 4개 최소 — 각각 새로 시작한 백테스트.
"""
import sys, os, math, itertools, pickle, tempfile, time, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "scripts"))
import numpy as np
import pandas as pd
import multiprocessing as mp

START = "2010-03-12"
MID = "2018-07-02"
WINDOWS = {"전체": (START, None), "H1": (START, MID), "H2": (MID, None), "봉인": ("2024-01-02", None)}
SUBS = [(START, "2014-05-01"), ("2014-05-01", MID), (MID, "2022-09-01"), ("2022-09-01", None)]
THRS = (0.6, 0.7, 0.8)

MULTS = (1.0, 0.75, 0.5, 0.25, 0.0)
SRS = (None, 100.0)
KBS = (0.0, -0.3, -0.6)
KSS = (0.0, -0.3)
CAPS = (None, 0.7, 0.5)

_D = {}


def init_worker(pkl):
    with open(pkl, "rb") as f:
        _D.update(pickle.load(f))


def sim(closes, prevs, sigmas, risk, p, rule, cap0=10000.0):
    """run_backtest_stdev_fast 와 같은 순서·산식. rule=None 이면 원본과 동일. 총자산 배열 반환."""
    mult, sr_o, kb_d, ks_d, cap = rule if rule else (1.0, None, 0.0, 0.0, None)
    div, ren = p["divisions"], p["renewal"]
    k_buy, k_sell, sr = p["k_buy"], p["k_sell"], p["sell_ratio"] / 100.0
    n = len(closes)
    cash, hold, avg, cum, tot, tier = cap0, 0, 0.0, 0.0, cap0, 0
    buf = np.empty(n + 1)
    bi = 0
    out = np.empty(n)
    prev_asset = cap0
    for i in range(n):
        c, s = closes[i], sigmas[i]
        if s != s:
            out[i] = prev_asset = cash + hold * c
            buf[bi] = cum; bi += 1
            continue
        rk = rule is not None and risk[i]
        pc = prevs[i]
        b_loc = pc * (1.0 + s * (k_buy + (kb_d if rk else 0.0)))
        s_loc = pc * (1.0 + s * (k_sell + (ks_d if rk else 0.0)))
        tier = (tier % div) + 1
        if tier == 1 and bi > 0:
            lb = (ren + 1) if bi > 2 * div else ren
            pcum = buf[bi - lb] if bi >= lb else 0.0
            d = cum - pcum
            tot += d
        daily = tot / div
        sq = 0
        if hold > 0 and c >= s_loc:
            sq = int(round(float(hold) * ((sr_o / 100.0) if (rk and sr_o is not None) else sr)))
        bq = 0
        if c <= b_loc:
            avail = min(daily * (mult if rk else 1.0), cash)
            if rk and cap is not None:
                avail = min(avail, max(0.0, cap * prev_asset - hold * pc))
            bq = math.floor(avail / b_loc)
        if sq > 0:
            cum += c * sq - avg * sq
        rem = hold - sq
        nh = rem + bq
        if nh > 0:
            avg = (rem * avg + c * bq) / nh if bq > 0 else avg
        else:
            avg = 0.0
        hold = nh
        cash = cash - c * bq + c * sq
        out[i] = prev_asset = cash + hold * c
        buf[bi] = cum; bi += 1
    return out


def metric(pk, thr, s, e, rule):
    a = _D["arr"][pk]
    idx = _D["idx"]
    i0 = idx.searchsorted(pd.Timestamp(s))
    i1 = len(idx) if e is None else idx.searchsorted(pd.Timestamp(e))
    eq = sim(a["c"][i0:i1], a["pc"][i0:i1], a["sg"][i0:i1], _D["risk"][thr][i0:i1], a["p"], rule)
    y = (idx[i1 - 1] - idx[i0]).days / 365.25
    cg = (eq[-1] / 1e4) ** (1 / y) - 1
    md = abs((eq / np.maximum.accumulate(eq) - 1).min()) or 1e-9
    return cg, md, cg / md


def evaluate(item):
    pk, thr, rule = item
    out = dict(preset=pk, thr=thr, mult=rule[0] if rule else 1.0, sr=rule[1] if rule else None,
               kb=rule[2] if rule else 0.0, ks=rule[3] if rule else 0.0, cap=rule[4] if rule else None,
               기준=rule is None)
    for name, (s, e) in WINDOWS.items():
        cg, md, cm = metric(pk, thr, s, e, rule)
        if name == "전체":
            out.update(CAGR=cg * 100, MDD=-md * 100, Calmar=cm)
        else:
            out[name] = cm
            if name == "봉인":
                out.update(봉인CAGR=cg * 100, 봉인MDD=-md * 100)
    out["구간최소"] = min(metric(pk, thr, s, e, rule)[2] for s, e in SUBS)
    return out


def prepare():
    import stdev_engine as E
    from update_preset_stats import load_adjusted, extract_literal
    from dss_engine import vol_regime_table, load_regime_prices
    os.chdir(HERE)
    db = extract_literal("strategies/stdev.py", "_SD_PRESETS_DB")
    px = load_adjusted("SOXL")[["Close"]]
    c_all = px["Close"].values.astype(float)
    off = px.index.searchsorted(pd.Timestamp(START))
    idx = px.index[off:]
    arr = {}
    for p in db["SOXL"]:
        r = np.full(len(c_all), np.nan)
        r[1:] = c_all[1:] / c_all[:-1] - 1
        sg = np.full(len(c_all), np.nan)
        sp = p["sigma_period"]
        for i in range(sp, len(c_all)):
            w = r[i - sp:i]
            if not np.any(np.isnan(w)):
                sg[i] = np.std(w, ddof=0)
        arr[p["label"]] = dict(c=c_all[off:], pc=c_all[off - 1:-1], sg=sg[off:],
                               p={k: p[k] for k in ("k_buy", "k_sell", "sell_ratio", "divisions", "renewal")})
        # 원본 엔진과 일치 확인
        ref = E.run_backtest_stdev(px, START, str(px.index[-1].date()), sigma_period=sp, k_buy=p["k_buy"],
                                   k_sell=p["k_sell"], sell_ratio=p["sell_ratio"], divisions=p["divisions"],
                                   renewal=p["renewal"], initial_capital=10000.0)
        mine = sim(arr[p["label"]]["c"], arr[p["label"]]["pc"], arr[p["label"]]["sg"], None, arr[p["label"]]["p"], None)
        diff = float(np.nanmax(np.abs(mine - ref["assets"])))
        assert diff < 1e-6, (p["label"], diff)
    soxx = load_regime_prices()
    risk = {}
    for thr in THRS:
        t = vol_regime_table(soxx, thr)
        months = {m for m, g in zip(t["적용월"], t["레짐"]) if g == "위험"}
        risk[thr] = np.array([d.to_period("M") in months for d in idx])
    print(f"원본 엔진과 일치 확인 완료 · 데이터 ~ {idx[-1].date()} · 위험일 비율",
          {k: round(float(v.mean()), 3) for k, v in risk.items()})
    return dict(arr=arr, idx=idx, risk=risk)


def main():
    t0 = time.time()
    data = prepare()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
    pickle.dump(data, tmp)
    tmp.close()
    rules = [r for r in itertools.product(MULTS, SRS, KBS, KSS, CAPS) if r != (1.0, None, 0.0, 0.0, None)]
    items = [(pk, 0.7, None) for pk in data["arr"]]
    items += [(pk, thr, r) for pk in data["arr"] for thr in THRS for r in rules]
    with mp.Pool(7, initializer=init_worker, initargs=(tmp.name,)) as pool:
        df = pd.DataFrame(pool.map(evaluate, items, chunksize=8))
    os.unlink(tmp.name)
    df.to_csv(os.path.join(HERE, "reports", "sd_vol_regime_grid.csv"), index=False, encoding="utf-8-sig")
    f2 = lambda v: f"{v:.2f}"
    cols = ["thr", "mult", "sr", "kb", "ks", "cap", "CAGR", "MDD", "Calmar", "H1", "H2", "봉인", "구간최소"]
    for pk in data["arr"]:
        sub = df[df["preset"] == pk]
        b = sub[sub["기준"]].iloc[0]
        rest = sub[~sub["기준"]]
        ok = rest[(rest["Calmar"] > b["Calmar"]) & (rest["H1"] > b["H1"]) & (rest["H2"] > b["H2"])
                  & (rest["봉인"] > b["봉인"]) & (rest["구간최소"] > b["구간최소"])]
        print(f"\n##### {pk} — 기준 {b['CAGR']:.1f}% / {b['MDD']:.1f}% / {b['Calmar']:.2f} "
              f"(H1 {b['H1']:.2f} H2 {b['H2']:.2f} 봉인 {b['봉인']:.2f} 구간최소 {b['구간최소']:.2f})"
              f" — 전 검증 통과 {len(ok)}/{len(rest)}")
        print("Calmar 상위 10 (기준 70%):")
        print(rest[rest["thr"] == 0.7].sort_values("Calmar", ascending=False)[cols].head(10).to_string(index=False, float_format=f2))
        # 단일 레버 (하나만 바꿈)
        one = rest[(rest["thr"] == 0.7) & ((rest["mult"] != 1.0).astype(int) + rest["sr"].notna().astype(int)
                   + (rest["kb"] != 0).astype(int) + (rest["ks"] != 0).astype(int) + rest["cap"].notna().astype(int) == 1)]
        print("단일 레버 (기준 70%):")
        print(one[cols].to_string(index=False, float_format=f2))
        if len(ok):
            print(f"[전 검증 통과 상위 12 — 기준별 통과 수 {ok.groupby('thr').size().to_dict()}]")
            print(ok.sort_values("Calmar", ascending=False)[cols].head(12).to_string(index=False, float_format=f2))
    print(f"\n{len(df)} 조합, {time.time() - t0:.0f}초")


if __name__ == "__main__":
    main()
