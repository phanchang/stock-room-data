# ==================================================
# ezmoney_data.py
# 統一投信（ezmoney）ETF 持股資料分析
# ==================================================

import pandas as pd
from pathlib import Path

# ----------------------
# 基本參數
# ----------------------
FUND = "ezmoney"
ETF_CODE = "00981A"
BASE_DIR = Path(__file__).resolve().parents[2]  # 戰情室根目錄
CSV_FILE = BASE_DIR / "data" / "clean" / FUND / f"{ETF_CODE}.csv"


# ----------------------
# 讀取過去 N 天資料
# ----------------------
def load_history(etf_code: str = None, days: int = 30) -> pd.DataFrame:
    """
    讀取 clean CSV，取過去 N 天資料
    etf_code: ETF 代號（例如 "00981A"）
    """
    if etf_code:
        csv_path = BASE_DIR / "data" / "clean" / FUND / f"{etf_code}.csv"
    else:
        csv_path = CSV_FILE

    csv_path_str = str(csv_path)

    print(f"📂 讀取檔案路徑: {csv_path_str}")
    print(f"📂 檔案是否存在: {csv_path.exists()}")

    if not csv_path.exists():
        raise FileNotFoundError(f"❌ 找不到 ezmoney clean CSV：{csv_path_str}")

    df = pd.read_csv(csv_path_str, parse_dates=['date'])
    df = df.sort_values(['date', 'stock_code'])

    print(f"✅ 成功讀取 {len(df)} 筆資料")

    if days:
        latest_date = df['date'].max()
        start_date = latest_date - pd.Timedelta(days=days - 1)
        df = df[df['date'] >= start_date]
        print(f"✅ 篩選後剩餘 {len(df)} 筆資料（過去 {days} 天）")

    return df


# ----------------------
# 最新 vs 前一交易日差異
# ----------------------
def compute_diff(df: pd.DataFrame, highlight_pct: float = 10) -> pd.DataFrame:

    if df.empty:
        return df

    available_dates = (
        df['date']
        .dropna()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )

    if len(available_dates) < 2:
        # 只有一天資料，直接回傳最新資料，不計算差異
        latest_date = available_dates[-1]

        latest = df[df['date'] == latest_date].copy()
        latest = latest.rename(columns={
            'shares': 'shares_today'
        })

        latest['shares_yesterday'] = 0
        latest['shares_change'] = 0
        latest['change_pct'] = 0.0
        latest['highlight'] = False
        latest['action_type'] = ''
        latest['compare_today'] = latest_date
        latest['compare_prev'] = pd.NaT

        # 欄位順序對齊一般 diff 結果
        return latest.reset_index(drop=True)

    latest_date = available_dates[-1]
    prev_date = available_dates[-2]

    latest = df[df['date'] == latest_date].copy()
    prev = df[df['date'] == prev_date].copy()

    merged = latest.merge(
        prev[['stock_code', 'shares']],
        on='stock_code',
        how='outer',
        suffixes=('_today', '_yesterday')
    )

    merged['shares_today'] = merged['shares_today'].fillna(0)
    merged['shares_yesterday'] = merged['shares_yesterday'].fillna(0)
    merged['stock_name'] = merged['stock_name'].fillna('')

    merged['shares_change'] = merged['shares_today'] - merged['shares_yesterday']

    def calc_change_pct(row):
        if row['shares_yesterday'] > 0:
            return (row['shares_change'] / row['shares_yesterday']) * 100
        elif row['shares_today'] > 0:
            return 100.0
        else:
            return 0.0

    merged['change_pct'] = merged.apply(calc_change_pct, axis=1)
    merged['highlight'] = merged['change_pct'].abs() >= highlight_pct

    merged['action_type'] = ''

    for idx, row in merged.iterrows():
        today = row['shares_today']
        yesterday = row['shares_yesterday']
        pct = row['change_pct']

        if yesterday == 0 and today > 0:
            merged.at[idx, 'action_type'] = '🆕 新買入'
        elif yesterday > 0 and today == 0:
            merged.at[idx, 'action_type'] = '🔴 完全賣出'
        elif pct >= 50:
            merged.at[idx, 'action_type'] = '🚀 大幅增持'
        elif pct <= -50:
            merged.at[idx, 'action_type'] = '⚠️ 大幅減持'
        elif 10 <= pct < 50:
            merged.at[idx, 'action_type'] = '📈 顯著增持'
        elif -50 < pct <= -10:
            merged.at[idx, 'action_type'] = '📉 顯著減持'

    merged = merged.sort_values('change_pct', ascending=False).reset_index(drop=True)
    merged['compare_today'] = latest_date
    merged['compare_prev'] = prev_date

    return merged


# ----------------------
# Top 10 持股趨勢
# ----------------------
def compute_top10_trend(df: pd.DataFrame, top_n: int = 10):

    latest_date = df['date'].max()
    latest_df = df[df['date'] == latest_date].copy()
    latest_df = latest_df.sort_values('weight', ascending=False).head(top_n)  # ✅ 改成 weight
    top10_codes = latest_df['stock_code'].tolist()

    df_top10 = df[df['stock_code'].isin(top10_codes)].copy()
    df_top10 = df_top10.sort_values(['date', 'stock_code'])

    result = []

    for code in top10_codes:
        stock_data = df_top10[df_top10['stock_code'] == code].copy()

        if len(stock_data) >= 2:
            first_date = stock_data['date'].min()
            last_date = stock_data['date'].max()
            first_shares = stock_data.iloc[0]['shares']
            last_shares = stock_data.iloc[-1]['shares']
            first_weight = stock_data.iloc[0]['weight']  # ✅ 新增：取得首日權重
            last_weight = stock_data.iloc[-1]['weight']  # ✅ 新增：取得最後權重
            stock_name = stock_data.iloc[0]['stock_name']

            change = last_shares - first_shares
            change_pct = (change / first_shares * 100) if first_shares > 0 else 0
            weight_change = last_weight - first_weight  # ✅ 新增：計算權重變化

            result.append({
                'stock_code': code,
                'stock_name': stock_name,
                'first_date': first_date,
                'last_date': last_date,
                'first_shares': first_shares,
                'last_shares': last_shares,
                'shares_change': change,
                'change_pct': change_pct,
                'first_weight': first_weight,  # ✅ 新增
                'last_weight': last_weight,  # ✅ 新增
                'weight_change': weight_change  # ✅ 新增
            })

    df_result = pd.DataFrame(result)

    # ===== 防呆：只有一天資料時，df_result 會是空的 =====
    if df_result.empty:
        return df_result, df_top10

    df_result = df_result.sort_values('last_weight', ascending=False)  # ✅ 改成 last_weight

    return df_result, df_top10

# ----------------------
# Top 10 每日資料（繪圖用）
# ----------------------
def get_top10_daily_data(df: pd.DataFrame, top_n: int = 10):

    latest_date = df['date'].max()
    latest_df = df[df['date'] == latest_date].copy()
    latest_df = latest_df.sort_values('shares', ascending=False).head(top_n)
    top10_codes = latest_df['stock_code'].tolist()

    df_daily = df[df['stock_code'].isin(top10_codes)].copy()
    df_daily = df_daily.sort_values(['stock_code', 'date'])

    return df_daily
