# -*- coding: utf-8 -*-
"""grid.csv 분석 — 구간 일관성 + 주변 값 안정성으로 후보 선별."""
import os, sys, numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
df = pd.read_csv(os.path.join(HERE, "grid.csv")).dropna()
for sfx in ("", "_a", "_b"):
    df["cal" + sfx] = df["cagr" + sfx] / df["mdd" + sfx].abs()
df["cal_min"] = df[["cal_a", "cal_b"]].min(axis=1)
# 주변 값 안정성: 같은 sigma/div/renewal 에서 k_buy·k_sell ±0.05, 매도비율 ±5 이웃들의 전체 Calmar 중앙값·최소
key = ["sigma", "div", "renewal", "k_buy", "k_sell", "sell_ratio"]
idx = df.set_index(key)["cal"].to_dict()
idm = df.set_index(key)["mdd"].to_dict()
def nb(r):
    v, m = [], []
    for dkb in (-0.05, 0, 0.05):
        for dks in (-0.05, 0, 0.05):
            for dsr in (-5, 0, 5):
                k = (r.sigma, r["div"], r.renewal, round(r.k_buy + dkb, 2), round(r.k_sell + dks, 2), r.sell_ratio + dsr)
                if k in idx:
                    v.append(idx[k]); m.append(idm[k])
    return pd.Series({"nb_med": np.median(v), "nb_min": min(v), "nb_mdd_worst": min(m), "nb_n": len(v)})
df = pd.concat([df, df.apply(nb, axis=1)], axis=1)
df.to_csv(os.path.join(HERE, "grid_scored.csv"), index=False)
pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30)
cols = ["sigma", "k_buy", "k_sell", "sell_ratio", "div", "renewal", "cagr", "mdd", "cal", "cal_a", "cal_b", "nb_med", "nb_min", "nb_mdd_worst"]
print("rows", len(df))
print("\n[분할수별] 중앙값 / 상위 5% 평균")
g = df.groupby("div")
print(pd.DataFrame({"cagr_med": g.cagr.median(), "mdd_med": g.mdd.median(), "cal_med": g.cal.median(),
                    "cal_top5%": g.cal.apply(lambda s: s[s >= s.quantile(.95)].mean()),
                    "mdd_best": g.mdd.max(), "calmin_top5%": g.cal_min.apply(lambda s: s[s >= s.quantile(.95)].mean())}).round(2))
print("\n[sigma별]"); print(df.groupby("sigma")[["cagr", "mdd", "cal", "cal_min"]].median().round(2))
print("\n[renewal = div vs 2*div]"); df["rn2"] = df.renewal == 2 * df["div"]; print(df.groupby("rn2")[["cagr", "mdd", "cal"]].median().round(2))
print("\n[전체 Calmar 상위 10]"); print(df.nlargest(10, "cal")[cols].round(2).to_string(index=False))
df["robust"] = df[["cal", "nb_med"]].min(axis=1).clip(upper=df["cal_min"] * 1.0 + 9)  # placeholder
df["score"] = np.minimum(df["nb_med"], df["cal"])            # 혼자 튀는 조합 배제
ok = df[(df.cal_a >= 1.0) & (df.cal_b >= 1.0) & (df.nb_n >= 18)]
print("\n앞·뒤 절반 모두 Calmar>=1 이고 이웃이 충분한 조합:", len(ok))
print("\n[안정성 점수 상위 15]"); print(ok.nlargest(15, "score")[cols].round(2).to_string(index=False))
for dv in sorted(df["div"].unique()):
    print(f"\n[{dv}분할 안정성 점수 상위 4]"); print(ok[ok["div"] == dv].nlargest(4, "score")[cols].round(2).to_string(index=False))
print("\n[MDD 구간별 최고 CAGR (이웃 최악 MDD 기준)]")
for lo in (-25, -30, -35, -40, -45):
    sub = ok[ok.nb_mdd_worst >= lo]
    if len(sub): print(f"이웃 최악 MDD >= {lo}%:"); print(sub.nlargest(3, "cagr")[cols].round(2).to_string(index=False))
print("\n[현재 프리셋]")
for n, (sg, kb, ks, sr, dv, rn) in {"공격형": (2, .65, .45, 85, 3, 3), "균형형": (2, .65, .55, 90, 6, 10), "안정형": (2, .65, .45, 85, 5, 5), "Ultra": (2, .55, .30, 95, 7, 7), "Sonic": (2, .55, .45, 95, 3, 3)}.items():
    r = df[(df.sigma == sg) & (df.k_buy == kb) & (df.k_sell == ks) & (df.sell_ratio == sr) & (df["div"] == dv) & (df.renewal == rn)]
    print(n, r[cols].round(2).to_string(index=False, header=False) if len(r) else "격자 밖")
