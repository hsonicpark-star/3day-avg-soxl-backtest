"""
엑셀과 동일한 로직으로 백테스트 실행 후 비교 (Daily_Close 2013년 데이터 포함)
"""
import pandas as pd
import math
import warnings
warnings.filterwarnings('ignore')

# ── 파라미터 (엑셀과 동일) ────────────────────────────────
a_buy  = -0.005
a_sell =  0.009
divisions = 5
initial_capital = 10000.0
start_date = '2014-01-01'
end_date   = '2025-12-31'

def buy_limit_price(p1, p2, a):
    return (p1 + p2) * (1 + a) / (2 - a)

# ── 종가 데이터: Daily_Close (전체 로드 후 기간 필터 먼저) ──
xl = pd.ExcelFile('종가평균 매매 백테스트 & 주문자동화 시트_pjh.xlsx')
df_price = xl.parse('Daily_Close')
df_price.columns = ['Date', 'Close']
df_price['Date'] = pd.to_datetime(df_price['Date'])
df_price = df_price.set_index('Date').sort_index()
df_price['Close'] = pd.to_numeric(df_price['Close'], errors='coerce')
df_price = df_price.dropna()

# 시뮬레이션 기간 필터 먼저 (엑셀과 동일: 시작일 이후에서만 shift)
sim_raw = df_price.loc[start_date:end_date].copy()
sim_raw['p1'] = sim_raw['Close'].shift(1)
sim_raw['p2'] = sim_raw['Close'].shift(2)
sim = sim_raw.dropna(subset=['p1','p2'])
sim['target_buy']  = buy_limit_price(sim['p1'], sim['p2'], a_buy)
sim['target_sell'] = buy_limit_price(sim['p1'], sim['p2'], a_sell)

# 초기 2일 경계가 확인
print("=== 시뮬레이션 시작 5일 경계가 ===")
print(sim[['Close','p1','p2','target_buy','target_sell']].head(5).to_string())
print()

# ── 시뮬레이션 ───────────────────────────────────────────
cash        = initial_capital
shares      = 0
prev_asset  = initial_capital
history = []

for date, row in sim.iterrows():
    x        = float(row['Close'])
    tgt_buy  = float(row['target_buy'])
    tgt_sell = float(row['target_sell'])

    action, trade_qty, trade_amt = '-', 0, 0.0

    current_chunk = prev_asset / divisions

    if shares > 0 and x >= tgt_sell:
        sell_qty  = shares  # 100% 매도
        action    = 'SELL'
        trade_qty = -sell_qty
        trade_amt = sell_qty * x
        cash     += trade_amt
        shares   -= sell_qty

    elif x <= tgt_buy:
        max_qty   = math.floor(current_chunk / x + 1e-9)
        avail_qty = math.floor(cash / x + 1e-9)
        buy_qty   = min(max_qty, avail_qty)
        if buy_qty > 0:
            action    = 'BUY'
            trade_qty = buy_qty
            trade_amt = buy_qty * x
            cash     -= trade_amt
            shares   += buy_qty

    asset = cash + shares * x
    prev_asset = asset

    history.append({
        'Date':           date.date().isoformat(),
        'TradeQty_sim':   trade_qty,
        'TotalAsset_sim': round(asset, 2),
    })

sim_df = pd.DataFrame(history)

# ── 엑셀 레퍼런스 파싱 ───────────────────────────────────
df0 = xl.parse(xl.sheet_names[0], header=None)
data_rows = df0.iloc[4:, :].copy()
data_rows.columns = range(len(data_rows.columns))
data_rows = data_rows[pd.to_datetime(data_rows[1], errors='coerce').notna()].copy()
data_rows[1] = pd.to_datetime(data_rows[1])
data_rows[2] = pd.to_numeric(data_rows[2], errors='coerce')
data_rows[3] = pd.to_numeric(data_rows[3], errors='coerce').fillna(0)
data_rows[8] = pd.to_numeric(data_rows[8], errors='coerce')

excel_df = pd.DataFrame({
    'Date':          data_rows[1].dt.date.astype(str),
    'TradeQty_xl':   data_rows[3].apply(lambda v: int(round(v)) if pd.notna(v) else 0).values,
    'TotalAsset_xl': data_rows[8].values,
}).reset_index(drop=True)

# ── 비교 ────────────────────────────────────────────────
merged = pd.merge(excel_df, sim_df, on='Date', how='inner')
merged['Qty_diff']   = (merged['TradeQty_xl']   - merged['TradeQty_sim']).abs()
merged['Asset_diff'] = (merged['TotalAsset_xl'] - merged['TotalAsset_sim']).abs().round(4)

print(f"공통 날짜: {len(merged)}일")
print(f"매매주수 불일치: {(merged['Qty_diff'] > 0).sum()}건")
print(f"총자산 불일치 (>0.1$): {(merged['Asset_diff'] > 0.1).sum()}건")
print()

diff = merged[(merged['Qty_diff'] > 0) | (merged['Asset_diff'] > 0.1)]
print(f"=== 불일치 행 (최대 20행) ===")
if len(diff) > 0:
    print(diff[['Date','TradeQty_xl','TradeQty_sim','TotalAsset_xl','TotalAsset_sim','Asset_diff']].head(20).to_string(index=False))
else:
    print(">>> 완전히 일치합니다! <<<")

print()
xl_last  = excel_df['TotalAsset_xl'].dropna().iloc[-1]
sim_last = sim_df['TotalAsset_sim'].iloc[-1]
print(f"엑셀 최종 자산:          ${xl_last:,.2f}")
print(f"시뮬레이션 최종 자산:     ${sim_last:,.2f}")
print(f"차이:                    ${abs(xl_last - sim_last):,.2f}")
