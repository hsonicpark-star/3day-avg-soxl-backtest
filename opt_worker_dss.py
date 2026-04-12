"""
DSS 동파법 최적화 멀티프로세싱 워커 모듈.
워커 프로세스에서 soxl/mode_series 데이터를 공유하기 위해 별도 모듈로 분리.
"""

import numpy as np
from dss_engine import run_backtest_fast, DSSParams

# 워커 프로세스 전역 변수 (초기화 시 한 번만 설정)
_soxl = None
_mode_series = None


def init_worker(data_path):
    """워커 프로세스 초기화 — 파일에서 공유 데이터 로드 (1회)."""
    import pickle
    global _soxl, _mode_series
    with open(data_path, 'rb') as f:
        _soxl, _mode_series = pickle.load(f)


def run_single_bt(args):
    """
    단일 백테스트 실행 (워커).
    args: (sd, sh, sb, ss, ad, ah, ab, as_, p, l, s_date, e_date, cap, fee_pct, renew)
    """
    sd, sh, sb, ss, ad, ah, ab, as_, p, l, s_date, e_date, cap, fee_pct, renew = args

    params = DSSParams(
        sf_divisions=sd, sf_max_hold=sh,
        sf_buy_pct=sb / 100, sf_sell_pct=ss / 100,
        ag_divisions=ad, ag_max_hold=ah,
        ag_buy_pct=ab / 100, ag_sell_pct=as_ / 100,
        initial_capital=cap,
        fee_rate=fee_pct / 100,
        renewal_period=renew,
        pcr=p / 100, lcr=l / 100,
    )
    r = run_backtest_fast(params, _soxl, _mode_series, s_date, e_date)
    if r is None:
        return None

    final = r['final_asset']
    days = r['total_days']
    mdd_val = r['mdd'] * 100  # 이미 음수
    ret = (final / cap - 1) * 100
    yrs = days / 252
    cagr_val = ((final / cap) ** (1 / yrs) - 1) * 100 if yrs > 0 else 0
    calmar = abs(cagr_val / mdd_val) if mdd_val != 0 else 0

    return {
        'SF분할': sd, 'SF보유': sh, 'SF매수%': round(sb, 2), 'SF매도%': round(ss, 2),
        'AG분할': ad, 'AG보유': ah, 'AG매수%': round(ab, 2), 'AG매도%': round(as_, 2),
        'PCR%': p, 'LCR%': l,
        '최종자산($)': round(final, 2),
        '수익률(%)': round(ret, 2),
        'CAGR(%)': round(cagr_val, 2),
        'MDD(%)': round(mdd_val, 2),
        'Sharpe': 0,
        'Calmar': round(calmar, 3),
    }
