"""
T2 추세 레짐 견고성 점검 (2026-11-25) — t2_trend_regime_2026-11.py 에서 유일하게 일관된 후보

후보: SOXX 200일선 이격도 > +15% (과열) 인 날(전일 판정)에는 주식 비중 50% 넘으면 신규 매수 안 함
      표준편차 균형형V / 공격형V (V 규칙 위에 추가) + 기본 프리셋
점검
  ① 경계 흔들기 — 과열 기준 +10 / +12.5 / +15 / +17.5 / +20% × 상한 40 / 50 / 60 / 70%
  ② 상시 적용 — 과열 여부와 무관하게 항상 상한 50% (레버리지를 낮춘 것뿐인지)
  ③ 무작위 대조 — 과열 날짜 배열을 통째로 밀어서(순환 이동 300회) 같은 길이·같은 연속성의 가짜 신호
  ④ ~2025년 말 / 연도별 수익률
"""
import sys, os, warnings
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "scripts"))
import numpy as np
import pandas as pd
import importlib.util

_sp = importlib.util.spec_from_file_location("d1", os.path.join(HERE, "reports", "d1_drawdown_regime_2026-11.py"))
D1 = importlib.util.module_from_spec(_sp)
_sp.loader.exec_module(D1)
from dss_engine import load_regime_prices

START = D1.START
WIN = dict(D1.SD_WIN)


def run(a, idx, vmask, mask, cap, s=START, e=None):
    n = len(idx)
    arrs = D1.sd_arrays(n, vmask, mask, (1.0, None, cap) if mask is not None else None)
    i0 = idx.searchsorted(pd.Timestamp(s))
    i1 = n if e is None else idx.searchsorted(pd.Timestamp(e))
    eq = D1.sd_sim(a["c"][i0:i1], a["pc"][i0:i1], a["sg"][i0:i1], a["p"], *[x[i0:i1] for x in arrs])
    y = (idx[i1 - 1] - idx[i0]).days / 365.25
    cg = (eq[-1] / 1e4) ** (1 / y) - 1
    md = abs((eq / np.maximum.accumulate(eq) - 1).min())
    return cg * 100, -md * 100, cg / md, pd.Series(eq, index=idx[i0:i1])


def full(a, idx, vmask, mask, cap):
    r = run(a, idx, vmask, mask, cap)
    w = {k: run(a, idx, vmask, mask, cap, s, e)[2] for k, (s, e) in WIN.items()}
    mn = min(run(a, idx, vmask, mask, cap, s, e)[2] for s, e in D1.SD_SUBS)
    return r, w, mn


def line(tag, t):
    r, w, mn = t
    return f"{tag:<26} {r[0]:5.1f}% / {r[1]:6.1f}% / {r[2]:.2f} | H1 {w['H1']:.2f} H2 {w['H2']:.2f} 봉인 {w['봉인']:.2f} 최소 {mn:.2f}"


def main():
    import update_preset_stats as U
    os.chdir(HERE)
    soxx = load_regime_prices()
    _px = U.load_adjusted("SOXL")
    U.load_adjusted = lambda tk, *a, **k: _px
    D = D1.S1.prepare()
    idx, vm = D["idx"], D["risk"][0.7]
    disp = (soxx / soxx.rolling(200).mean() - 1.0).dropna()
    prev = disp.shift(1).dropna().reindex(idx, method="ffill").values
    oh = lambda thr: prev > thr
    m15 = oh(0.15)
    print(f"\n과열일(이격도 > 15%) {m15.mean()*100:.1f}% · V1 위험일과 겹침 {(m15 & vm).sum()}/{vm.sum()}일 · 현재 이격도 {disp.iloc[-1]*100:.1f}%")
    rng = np.random.default_rng(20261125)
    shifts = rng.integers(60, len(idx) - 60, size=300)
    for lab in ("균형형", "공격형"):
        a = D["arr"][lab]
        for base, v in (("V", vm), ("", None)):
            b = full(a, idx, v, None, None)
            c = full(a, idx, v, m15, 0.5)
            print(f"\n━━ 표준편차 {lab}{base}")
            print("   " + line("기준", b))
            print("   " + line("+ 과열 상한 50%", c))
            print("   ① 과열 기준 × 상한 (Calmar):")
            for thr in (0.10, 0.125, 0.15, 0.175, 0.20):
                print(f"      > {thr*100:4.1f}% ({oh(thr).mean()*100:4.1f}% 일) : " + " · ".join(
                    f"상한{int(cp*100)} {run(a, idx, v, oh(thr), cp)[2]:.2f}" for cp in (0.4, 0.5, 0.6, 0.7)))
            print("   " + line("② 상시 상한 50%", full(a, idx, v, np.ones(len(idx), bool), 0.5)))
            print("   " + line("② 과열 아닌 날만 상한 50%", full(a, idx, v, ~m15, 0.5)))
            rnd = [run(a, idx, v, np.roll(m15, int(k)), 0.5)[:3] for k in shifts]
            cal = np.array([x[2] for x in rnd]); mdd = np.array([x[1] for x in rnd])
            print(f"   ③ 순환 이동 300회: Calmar 상위 {(cal >= c[0][2]).mean()*100:.1f}% (중앙값 {np.median(cal):.2f}, 최고 {cal.max():.2f}) · "
                  f"MDD 상위 {(mdd >= c[0][1]).mean()*100:.1f}% (중앙값 {np.median(mdd):.1f}%)")
            b25, c25 = run(a, idx, v, None, None, START, "2026-01-01"), run(a, idx, v, m15, 0.5, START, "2026-01-01")
            print(f"   ④ ~2025년 말: 기준 {b25[0]:.1f}% / {b25[1]:.1f}% / {b25[2]:.2f} → {c25[0]:.1f}% / {c25[1]:.1f}% / {c25[2]:.2f}")
            yb = b[0][3].groupby(b[0][3].index.year).last().pct_change() * 100
            yc = c[0][3].groupby(c[0][3].index.year).last().pct_change() * 100
            d = (yc - yb).dropna()
            print(f"      연도별 수익률 차이(%p): " + " ".join(f"{y}:{v:+.0f}" for y, v in d.items())
                  + f" | 나은 해 {(d > 0).sum()}/{len(d)}")
            # 큰 낙폭 5건
            def top_dd(eq):
                dd = eq / eq.cummax() - 1
                out, s = [], dd.copy()
                for _ in range(5):
                    t = s.idxmin(); pk = eq[:t].idxmax()
                    rec = dd[t:][dd[t:] > -1e-9]
                    en = rec.index[0] if len(rec) else dd.index[-1]
                    out.append((pk.date(), t.date(), dd[t] * 100)); s[pk:en] = 0
                return out
            print("      큰 낙폭 5건 기준: " + " · ".join(f"{p}→{t} {v:.0f}%" for p, t, v in top_dd(b[0][3])))
            print("      큰 낙폭 5건 후보: " + " · ".join(f"{p}→{t} {v:.0f}%" for p, t, v in top_dd(c[0][3])))


if __name__ == "__main__":
    main()
