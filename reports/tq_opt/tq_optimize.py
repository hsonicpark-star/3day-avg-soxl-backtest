# -*- coding: utf-8 -*-
"""TQ 모멘텀 5T (정량) 파라미터 최적화 — Calmar 극대화.

  대상 : 모드(ROC 기간·바닥/천장 경계) + 구간(바닥/중간/천장) × 5티어의
         시드비중·매수목표·매도목표·손절일수 + 매도날매수X·MOC날매수 + 티어계산 방식
  고정 : TQQQ · 원금 $50,000 · 복리 100/100 · 정량매수 · 5분할 · 수수료 0 · SEC 기본

  과최적화 방지
    학습(IS)   2015-01-01 ~ 2022-12-30 에서만 탐색
    검증(OOS)  2023-01-01 ~ 2026-09-28
    사전구간   2011-01-03 ~ 2014-12-31 (탐색에 전혀 쓰지 않은 과거, 센트 반올림 주의)
    강건성     파라미터를 흔든 이웃 20개의 IS Calmar 중앙값

  1단계 TPE(워커별 시드) → 2단계 국소 탐색(힐클라이밍) → 3단계 검증
  산출: reports/tq_opt/  (progress.log · stage1_*.json · stage2_*.json · final.json)
"""
import os, sys, io, json, copy, time, random, math, warnings, logging
from concurrent.futures import ProcessPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
APP = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, APP)
warnings.filterwarnings("ignore")
logging.disable(logging.CRITICAL)

_E = os.environ.get
IS_S, IS_E = _E("IS_S", "2015-01-01"), _E("IS_E", "2022-12-30")
OOS_S, OOS_E = _E("OOS_S", "2023-01-01"), _E("OOS_E", "2026-09-28")
PRE_S, PRE_E = "2011-01-03", "2014-12-31"
FULL_S, FULL_E = "2015-01-01", "2026-09-28"
MIN_TRADES_IS = int(_E("MIN_TRADES", 120))
OUT = os.path.join(HERE, _E("OUTDIR", ""))
os.makedirs(OUT, exist_ok=True)
SUBS = {"15_18": ("2015-01-01", "2018-12-31"), "19_22": ("2019-01-01", "2022-12-30"),
        "23_26": ("2023-01-01", "2026-09-28")}
WORKERS = int(os.environ.get("WORKERS", 7))
S1_TRIALS = int(os.environ.get("S1_TRIALS", 1500))
S2_ITERS = int(os.environ.get("S2_ITERS", 4000))
LOG = os.path.join(OUT, "progress.log")
LEVELS = ("바닥", "중간", "천장")


def log(msg):
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    with io.open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        print(line.encode("ascii", "replace").decode(), flush=True)


# ── 공용 (워커마다 1회 로드) ──────────────────────────────
_G = {}
_MF = {}


def _init():
    if _G:
        return
    from manse_engine import run_backtest, build_mode_frame, params_from_dict
    from common.pricedb import load_prices
    _G.update(run=run_backtest, bmf=build_mode_frame, pfd=params_from_dict,
              PR={t: load_prices(t) for t in ("TQQQ", "QQQ")},
              base=json.load(io.open(os.path.join(HERE, "base_params.json"), encoding="utf-8")))


def evaluate(pd_, s, e):
    """params dict → light metrics (모드 프레임은 모드 파라미터별 캐시)."""
    _init()
    p = _G["pfd"](pd_)
    key = (p.mom_days, round(p.mom_low, 6), round(p.mom_high, 6))
    mf = _MF.get(key)
    if mf is None:
        mf = _G["bmf"](p, _G["PR"])
        if len(_MF) > 400:
            _MF.clear()
        _MF[key] = mf
    try:
        return _G["run"](_G["PR"], p, start=s, end=e, mode_frame=mf, light=True)
    except Exception:
        return None


def score(m, min_trades=MIN_TRADES_IS):
    if not m or m.get("거래횟수", 0) < min_trades or not m.get("Calmar") \
            or math.isnan(m["Calmar"]) or m["최종자산"] <= 0:
        return -1.0
    return float(m["Calmar"])


# ── 파라미터 공간 (기준선 중심) ─────────────────────────────
def apply_vec(base, v):
    """평탄한 dict(v) → params dict."""
    d = copy.deepcopy(base)
    d["mom_days"] = int(v["mom_days"])
    d["mom_low"] = float(v["mom_low"])
    d["mom_high"] = float(v["mom_low"] + v["mom_gap"])
    d["tier_method"] = v["tier_method"]
    for lv in LEVELS:
        L = d["levels"][lv]
        L["no_buy_on_sell_day"] = bool(v[f"{lv}_nb"])
        L["moc_day_buy"] = bool(v[f"{lv}_mb"])
        ws = [max(1e-4, v[f"{lv}_t{i}_w"]) for i in range(1, 6)]
        tot = sum(ws)
        for i, t in enumerate(L["tiers"], 1):
            t["seed_w"] = ws[i - 1] / tot
            t["buy_gap"] = float(v[f"{lv}_t{i}_b"])
            t["sell_gap"] = float(v[f"{lv}_t{i}_s"])
            t["stop_days"] = int(max(1, round(v[f"{lv}_t{i}_d"])))
    return d


def base_vec(base):
    v = {"mom_days": base["mom_days"], "mom_low": base["mom_low"],
         "mom_gap": round(base["mom_high"] - base["mom_low"], 4),
         "tier_method": base["tier_method"]}
    for lv in LEVELS:
        L = base["levels"][lv]
        v[f"{lv}_nb"] = L["no_buy_on_sell_day"]
        v[f"{lv}_mb"] = L["moc_day_buy"]
        for i, t in enumerate(L["tiers"], 1):
            v[f"{lv}_t{i}_w"] = t["seed_w"]
            v[f"{lv}_t{i}_b"] = t["buy_gap"]
            v[f"{lv}_t{i}_s"] = t["sell_gap"]
            v[f"{lv}_t{i}_d"] = t["stop_days"]
    return v


def _snap(x, lo, step):
    return round(lo + round((x - lo) / step) * step, 6)


def suggest(trial, b):
    v = {"mom_days": trial.suggest_int("mom_days", 40, 160, step=5),
         "mom_low": trial.suggest_float("mom_low", -0.03, 0.05, step=0.0025),
         "mom_gap": trial.suggest_float("mom_gap", 0.02, 0.20, step=0.0025),
         "tier_method": trial.suggest_categorical("tier_method", ["보유", "빈자리"])}
    for lv in LEVELS:
        v[f"{lv}_nb"] = trial.suggest_categorical(f"{lv}_nb", [False, True])
        v[f"{lv}_mb"] = trial.suggest_categorical(f"{lv}_mb", [False, True])
        for i in range(1, 6):
            bb, bs, bd = b[f"{lv}_t{i}_b"], b[f"{lv}_t{i}_s"], b[f"{lv}_t{i}_d"]
            v[f"{lv}_t{i}_w"] = trial.suggest_float(f"{lv}_t{i}_w", 0.01, 1.0)
            v[f"{lv}_t{i}_b"] = trial.suggest_float(f"{lv}_t{i}_b", round(bb - 0.06, 4),
                                                    round(bb + 0.06, 4), step=0.0025)
            lo_s = round(max(0.005, bs * 0.4), 4)
            hi_s = _snap(max(lo_s + 0.01, bs * 2.0), lo_s, 0.0025)
            v[f"{lv}_t{i}_s"] = trial.suggest_float(f"{lv}_t{i}_s", lo_s, hi_s, step=0.0025)
            v[f"{lv}_t{i}_d"] = trial.suggest_int(f"{lv}_t{i}_d", max(1, int(bd * 0.3)),
                                                  max(3, int(bd * 2.5)))
    return v


def stage1(seed):
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _init()
    base = _G["base"]
    b = base_vec(base)
    best = []
    sampler = optuna.samplers.TPESampler(seed=seed, multivariate=True, group=True,
                                         n_startup_trials=150)
    st = optuna.create_study(direction="maximize", sampler=sampler)
    try:
        st.enqueue_trial(dict(b), skip_if_exists=True)     # 기준선을 첫 시도로
    except Exception:
        pass
    t0 = time.time()

    def obj(trial):
        v = suggest(trial, b)
        sc = score(evaluate(apply_vec(base, v), IS_S, IS_E))
        best.append((sc, v))
        return sc

    for k in range(max(1, S1_TRIALS // 250)):
        st.optimize(obj, n_trials=250)
        best.sort(key=lambda x: -x[0])
        del best[60:]
        with io.open(LOG, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%H:%M:%S')}   [1단계 w{seed}] {(k+1)*250}/{S1_TRIALS} "
                    f"최고 IS Calmar {best[0][0]:.3f} ({time.time()-t0:.0f}s)\n")
    json.dump([{"score": s, "v": v} for s, v in best],
              io.open(os.path.join(OUT, f"stage1_{seed}.json"), "w", encoding="utf-8"),
              ensure_ascii=False, default=float)
    return seed, best[0][0]


# ── 2단계: 국소 탐색 ──────────────────────────────────────
def mutate(v, rnd):
    w = dict(v)
    keys = [k for k in w if k != "tier_method"]
    for _ in range(rnd.choice([1, 1, 2, 2, 3, 4])):
        k = rnd.choice(keys)
        x = w[k]
        if isinstance(x, bool):
            w[k] = not x
        elif k == "mom_days":
            w[k] = int(min(200, max(20, x + rnd.choice([-10, -5, 5, 10]))))
        elif k.endswith("_d"):
            w[k] = int(max(1, x + rnd.choice([-1, 1]) * max(1, int(abs(x) * rnd.uniform(.03, .15)))))
        elif k.endswith("_w"):
            w[k] = max(0.005, x * math.exp(rnd.gauss(0, 0.25)))
        else:   # mom_low · mom_gap · _b · _s
            w[k] = round(x + rnd.choice([-1, 1]) * rnd.choice([0.0025, 0.005, 0.01]), 4)
            if k.endswith("_s") or k == "mom_gap":
                w[k] = max(0.005, w[k])
    if rnd.random() < 0.03:
        w["tier_method"] = "빈자리" if w["tier_method"] == "보유" else "보유"
    return w


def stage2(args):
    seed, start_v = args
    _init()
    base = _G["base"]
    rnd = random.Random(seed)
    cur = start_v
    cur_s = score(evaluate(apply_vec(base, cur), IS_S, IS_E))
    hist = [(cur_s, cur)]
    t0 = time.time()
    for it in range(1, S2_ITERS + 1):
        cand = mutate(cur, rnd)
        s = score(evaluate(apply_vec(base, cand), IS_S, IS_E))
        if s > cur_s:
            cur, cur_s = cand, s
            hist.append((s, cand))
        if it % 500 == 0:
            with io.open(LOG, "a", encoding="utf-8") as f:
                f.write(f"{time.strftime('%H:%M:%S')}   [2단계 s{seed}] {it}/{S2_ITERS} "
                        f"IS Calmar {cur_s:.3f} ({time.time()-t0:.0f}s)\n")
    hist.sort(key=lambda x: -x[0])
    json.dump([{"score": s, "v": v} for s, v in hist[:15]],
              io.open(os.path.join(OUT, f"stage2_{seed}.json"), "w", encoding="utf-8"),
              ensure_ascii=False, default=float)
    return seed, cur_s


# ── 3단계: 검증 ──────────────────────────────────────────
def jitter(v, rnd, pct=0.10):
    w = dict(v)
    for k, x in v.items():
        if isinstance(x, (bool, str)):
            continue
        if k.endswith("_b") or k == "mom_low":
            w[k] = x + rnd.uniform(-pct / 10, pct / 10)    # 매수목표·바닥경계 ±pct/10 (%p)
        elif k == "mom_days" or k.endswith("_d"):
            w[k] = max(1, int(round(x * (1 + rnd.uniform(-pct, pct)))))
        else:
            w[k] = max(0.001, x * (1 + rnd.uniform(-pct, pct)))
    return w


def validate(args):
    idx, v = args
    _init()
    base = _G["base"]
    d = apply_vec(base, v)
    rnd = random.Random(1000 + max(idx, 0))
    nb = sorted(score(evaluate(apply_vec(base, jitter(v, rnd, 0.10)), IS_S, IS_E)) for _ in range(20))
    nb5 = sorted(score(evaluate(apply_vec(base, jitter(v, rnd, 0.05)), IS_S, IS_E)) for _ in range(20))
    out = {"idx": idx, "v": v}
    for nm, (s, e) in {"IS": (IS_S, IS_E), "OOS": (OOS_S, OOS_E), "PRE": (PRE_S, PRE_E),
                       "FULL": (FULL_S, FULL_E), **SUBS}.items():
        m = evaluate(d, s, e) or {}
        out[nm] = {k: m.get(k) for k in ("CAGR", "MDD", "Calmar", "거래횟수", "승률", "최종자산")}
    d7 = copy.deepcopy(d)
    d7["fee"] = 0.0007
    m7 = evaluate(d7, FULL_S, FULL_E) or {}
    out["FULL_fee7"] = {k: m7.get(k) for k in ("CAGR", "MDD", "Calmar")}
    out["robust_med"] = nb5[len(nb5) // 2]          # ±5% 이웃 중앙값 (정렬 기준)
    out["robust_p25"] = nb5[len(nb5) // 4]
    out["robust10_med"] = nb[len(nb) // 2]          # ±10% 이웃 중앙값 (참고)
    return out


def _c(x):
    return f"{x:.3f}" if isinstance(x, (int, float)) and x is not None else "-"


def main():
    _init()
    base = _G["base"]
    log(f"=== TQ 모멘텀 5T 최적화 시작 · 워커 {WORKERS} · 1단계 {S1_TRIALS}×{WORKERS} "
        f"· 2단계 {S2_ITERS}×{WORKERS} · 학습 {IS_S}~{IS_E} · 검증 {OOS_S}~{OOS_E}")
    b0 = validate((-1, base_vec(base)))
    log(f"기준선  IS {_c(b0['IS']['Calmar'])} · OOS {_c(b0['OOS']['Calmar'])} · "
        f"PRE {_c(b0['PRE']['Calmar'])} · FULL {_c(b0['FULL']['Calmar'])} · "
        f"강건±5% {_c(b0['robust_med'])} · 강건±10% {_c(b0['robust10_med'])}")
    json.dump(b0, io.open(os.path.join(OUT, "baseline.json"), "w", encoding="utf-8"),
              ensure_ascii=False, default=float)

    with ProcessPoolExecutor(WORKERS) as ex:
        r1 = list(ex.map(stage1, range(1, WORKERS + 1)))
    log(f"1단계 완료: {r1}")
    pool = []
    for s in range(1, WORKERS + 1):
        pool += json.load(io.open(os.path.join(OUT, f"stage1_{s}.json"), encoding="utf-8"))
    pool.sort(key=lambda x: -x["score"])
    # 60차원은 TPE 가 기준선을 못 넘는 경우가 많다 → 대부분 기준선에서 출발
    n_tpe = min(2, WORKERS - 1)
    starts = [base_vec(base)] * (WORKERS - n_tpe) + [x["v"] for x in pool[:n_tpe]]

    with ProcessPoolExecutor(WORKERS) as ex:
        r2 = list(ex.map(stage2, [(100 + i, v) for i, v in enumerate(starts)]))
    log(f"2단계 완료: {r2}")
    for s in range(100, 100 + WORKERS):
        pool += json.load(io.open(os.path.join(OUT, f"stage2_{s}.json"), encoding="utf-8"))
    pool.sort(key=lambda x: -x["score"])
    uniq, seen = [], set()
    for x in pool:
        k = json.dumps(x["v"], sort_keys=True, default=float)
        if k in seen:
            continue
        seen.add(k)
        uniq.append(x)
        if len(uniq) >= 35:
            break

    with ProcessPoolExecutor(WORKERS) as ex:
        res = list(ex.map(validate, list(enumerate(x["v"] for x in uniq))))
    res.sort(key=lambda r: -min(r["robust_med"], r["OOS"]["Calmar"] or -1))
    json.dump({"baseline": b0, "candidates": res},
              io.open(os.path.join(OUT, "final.json"), "w", encoding="utf-8"),
              ensure_ascii=False, default=float)
    log("3단계 완료 — 상위 (정렬: min(강건 중앙값, OOS))")
    for r in res[:8]:
        log(f"  #{r['idx']:>2} IS {_c(r['IS']['Calmar'])} 강건±5% {_c(r['robust_med'])} "
            f"OOS {_c(r['OOS']['Calmar'])} PRE {_c(r['PRE']['Calmar'])} "
            f"FULL {_c(r['FULL']['Calmar'])} (CAGR {r['FULL']['CAGR']*100:.1f}% "
            f"MDD {r['FULL']['MDD']*100:.1f}%) 수수료0.07% {_c(r['FULL_fee7']['Calmar'])}")
    log("=== 끝")


if __name__ == "__main__":
    main()
