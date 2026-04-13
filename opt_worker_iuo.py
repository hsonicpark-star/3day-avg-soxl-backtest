"""
IUO 매매법 최적화 멀티프로세싱 워커 모듈.
워커 프로세스에서 price/qqq 데이터를 공유하기 위해 별도 모듈로 분리.
"""

from iuo_engine import run_backtest_fast, IUOParams

# 워커 프로세스 전역 변수 (초기화 시 한 번만 설정)
_price_data = None
_qqq_data = None


def init_worker(data_path):
    """워커 프로세스 초기화 — 파일에서 공유 데이터 로드 (1회)."""
    import pickle
    global _price_data, _qqq_data
    with open(data_path, 'rb') as f:
        _price_data, _qqq_data = pickle.load(f)


def run_single_bt(args):
    """
    단일 백테스트 실행 (워커).
    args: (fbr, b1, b2, sp, moc, maxb, div, s_date, e_date, cap)
    """
    fbr, b1, b2, sp, moc, maxb, div, s_date, e_date, cap = args

    params = IUOParams(
        initial_capital=cap,
        first_buy_ratio=fbr / 100,
        buy1_pct=b1 / 100,
        buy2_pct=b2 / 100,
        sell_pct=sp / 100,
        moc_days=moc,
        max_additional_buys=maxb,
        divisions=div,
    )
    r = run_backtest_fast(params, _price_data, _qqq_data, s_date, e_date)
    if r is None:
        return None

    final = r['final_asset']
    days = r['total_days']
    mdd_val = r['mdd'] * 100  # 이미 음수
    ret = (final / cap - 1) * 100
    yrs = days / 252
    cagr_val = ((final / cap) ** (1 / yrs) - 1) * 100 if yrs > 0 else 0
    calmar = abs(cagr_val / mdd_val) if mdd_val != 0 else 0
    win_rate = (r['win_count'] / r['sell_count'] * 100) if r['sell_count'] > 0 else 0

    return {
        '첫매수%': round(fbr, 1), '매수1%': round(b1, 2), '매수2%': round(b2, 1),
        '매도%': round(sp, 2), 'MOC': moc, '매수제한': maxb, '분할수': div,
        '최종자산($)': round(final, 2),
        '수익률(%)': round(ret, 2),
        'CAGR(%)': round(cagr_val, 2),
        'MDD(%)': round(mdd_val, 2),
        'Calmar': round(calmar, 3),
        '승률(%)': round(win_rate, 1),
    }
