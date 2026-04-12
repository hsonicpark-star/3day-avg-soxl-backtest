"""표준편차매매 최적화 멀티프로세싱 워커."""
_price_df = None

def init_worker(data_path):
    import pickle
    global _price_df
    with open(data_path, 'rb') as f:
        _price_df = pickle.load(f)

def run_single_bt(args):
    from strategies.stdev import run_backtest_stdev_fast
    kb, ks, sr, dv, start_date, end_date, initial_capital, sigma_period, renewal = args
    r = run_backtest_stdev_fast(
        _price_df, start_date, end_date,
        sigma_period=sigma_period, k_buy=kb, k_sell=ks,
        sell_ratio=float(sr), divisions=int(dv), renewal=renewal,
        initial_capital=initial_capital,
    )
    if r is None:
        return None
    return {
        'k_buy': round(kb, 3), 'k_sell': round(ks, 3),
        '분할수': int(dv), '매도비율(%)': int(sr),
        'CAGR(%)': round(r['cagr'] * 100, 2),
        'MDD(%)': round(r['mdd'] * 100, 2),
        'Calmar': round(r['calmar'], 4),
        '총수익(%)': round(r['total_return'] * 100, 2),
        '최종자산($)': round(r['final_asset'], 2),
        '매수횟수': r['buy_count'],
        '매도횟수': r['sell_count'],
    }
