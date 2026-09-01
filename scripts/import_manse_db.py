"""
scripts/import_manse_db.py — 만능시트 구글시트 export(DB 탭 CSV)를 로컬 종가 DB로 적재

만능시트의 'DB' 탭은 여러 종목의 (날짜, 종가) 쌍이 가로로 나란히 배치되어 있다.
열 인덱스(0-based):
    E(4),  F(5)   → 투자 종목(TICKER)  일별 종가
    H(7),  I(8)   → 중심주가 종목(TICKER1) 주봉 종가
    K(10), L(11)  → 이평선 종목(TICKER2) 주봉 종가
    N(13), O(14)  → 이평선 종목(TICKER2) 일별 종가
    R(17), S(18)  → RSI 종목(TICKER3)   주봉 종가

주봉은 일봉에서 파생 가능하므로 **일별 종가만** 적재한다.

사용법
------
    python scripts/import_manse_db.py "D:/04.backtest/12.만능시트매매/... - DB.csv" \
        --daily SOXL:4,5 --daily QQQ:13,14

    # 인자 없이 실행하면 기본 폴더(12.만능시트매매)의 DB.csv 를 자동 탐색
    python scripts/import_manse_db.py
"""

from __future__ import annotations

import argparse
import glob
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.pricedb import import_pairs_from_csv, save_prices, db_status  # noqa: E402

DEFAULT_DIR = r"D:\04.backtest\12.만능시트매매"
# (티커, 날짜열, 종가열) — 만능시트 기본 세팅(SOXL 매매 / QQQ 지표) 기준
DEFAULT_DAILY = [("SOXL", 4, 5), ("QQQ", 13, 14)]


def run(csv_paths, daily_specs, overwrite=False):
    total = {}
    for path in csv_paths:
        if not os.path.exists(path):
            print(f"  [skip] 파일 없음: {path}")
            continue
        print(f"\n[읽는 중] {os.path.basename(path)}")
        for ticker, dcol, ccol in daily_specs:
            df = import_pairs_from_csv(path, dcol, ccol)
            if df.empty:
                print(f"  - {ticker}: 열 {dcol},{ccol} 에서 데이터를 찾지 못함")
                continue
            added = save_prices(ticker, df, overwrite=overwrite)
            total[ticker] = total.get(ticker, 0) + added
            print(f"  - {ticker}: {len(df):,}행 읽음 → {added:,}행 신규 적재 "
                  f"({df.index.min().date()} ~ {df.index.max().date()})")
    return total


def main():
    ap = argparse.ArgumentParser(description="만능시트 DB 탭 CSV → 로컬 종가 DB 적재")
    ap.add_argument("csv", nargs="*", help="DB 탭 CSV 경로 (생략 시 기본 폴더 자동 탐색)")
    ap.add_argument("--daily", action="append", default=None,
                    metavar="TICKER:날짜열,종가열",
                    help="예: --daily SOXL:4,5 --daily QQQ:13,14")
    ap.add_argument("--overwrite", action="store_true",
                    help="기존 저장값도 갱신 (기본은 새 날짜만 추가 — B방식)")
    args = ap.parse_args()

    csv_paths = args.csv
    if not csv_paths:
        csv_paths = sorted(glob.glob(os.path.join(DEFAULT_DIR, "*- DB.csv")))
        if not csv_paths:
            print(f"⛔ {DEFAULT_DIR} 에서 '- DB.csv' 를 찾지 못했습니다.")
            return 1

    if args.daily:
        specs = []
        for s in args.daily:
            tk, cols = s.split(":")
            d, c = cols.split(",")
            specs.append((tk.strip().upper(), int(d), int(c)))
    else:
        specs = DEFAULT_DAILY

    run(csv_paths, specs, overwrite=args.overwrite)

    print("\n=== 로컬 종가 DB 현황 ===")
    st = db_status()
    print(st.to_string(index=False) if len(st) else "(비어 있음)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
