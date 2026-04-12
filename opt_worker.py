"""종가평균매매 최적화 멀티프로세싱 워커."""
_price_df = None

def init_worker(data_path):
    import pickle
    global _price_df
    with open(data_path, 'rb') as f:
        _price_df = pickle.load(f)

def run_single_bt(args):
    from strategies.avg_close import run_backtest_fast
    ab, as_, sr, dv, start_date, end_date, initial_capital, n_days = args
    r = run_backtest_fast(_price_df, start_date, end_date, ab, as_, sr, dv, initial_capital, n_days=n_days)
    if r is None:
        return None
    return {
        'a_buy': round(ab, 4), 'a_sell': round(as_, 4),
        '분할수': dv, '매도비율': sr,
        'CAGR(%)': round(r['cagr'] * 100, 2),
        'MDD(%)': round(r['mdd'] * 100, 2),
        'Calmar': round(r['calmar'], 4),
        '총수익(%)': round(r['total_return'] * 100, 2),
        '최종자산($)': round(r['final_asset'], 2),
        '매수횟수': r['buy_count'],
        '매도횟수': r['sell_count'],
    }
