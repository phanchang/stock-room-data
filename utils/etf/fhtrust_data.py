# ==================================================
# fhtrust_data.py - 更新版
# 投信持股資料分析
# ==================================================

import pandas as pd
from pathlib import Path

# ----------------------
# 基本參數
# ----------------------
FUND = "fhtrust"
ETF_CODE = "00991A"
BASE_DIR = Path(__file__).resolve().parents[2]  # 戰情室根目錄
CSV_FILE = BASE_DIR / "data" / "clean" / FUND / f"{ETF_CODE}.csv"


# ----------------------
# 讀取過去 N 天資料
# ----------------------
def load_history(etf_code: str = None, days: int = 30) -> pd.DataFrame:
    """
    讀取 clean CSV，取過去 N 天資料
    etf_code: ETF 代號（例如 "00991A"），如果為 None 則使用預設值
    """
    if etf_code:
        # ✅ 動態生成路徑
        csv_path = BASE_DIR / "data" / "clean" / FUND / f"{etf_code}.csv"
    else:
        csv_path = CSV_FILE

    # ✅ 確保路徑是字串
    csv_path_str = str(csv_path)

    print(f"📂 讀取檔案路徑: {csv_path_str}")  # 除錯用
    print(f"📂 檔案是否存在: {csv_path.exists()}")  # 除錯用

    df = pd.read_csv(csv_path_str, parse_dates=['date'])
    df = df.sort_values(['date', 'stock_code'])

    print(f"✅ 成功讀取 {len(df)} 筆資料")  # 除錯用

    if days:
        latest_date = df['date'].max()
        start_date = latest_date - pd.Timedelta(days=days - 1)
        df = df[df['date'] >= start_date]
        print(f"✅ 篩選後剩餘 {len(df)} 筆資料（過去 {days} 天）")  # 除錯用

    return df


def compute_diff(df: pd.DataFrame, highlight_pct: float = 10) -> pd.DataFrame:
    """
    計算最新一次 vs 前一次「有資料的日期」的股數差異
    - 自動略過假日 / 缺資料日
    """

    if df.empty:
        return df

    # 取得所有有資料的日期（由新到舊）
    available_dates = (
        df['date']
        .dropna()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )

    # 若不足兩天資料，無法比較
    if len(available_dates) < 2:
        raise ValueError("⚠️ 可用日期不足，無法計算差異")

    latest_date = available_dates[-1]
    prev_date = available_dates[-2]

    latest = df[df['date'] == latest_date].copy()
    prev = df[df['date'] == prev_date].copy()

    # 合併
    merged = latest.merge(
        prev[['stock_code', 'shares']],
        on='stock_code',
        how='outer',
        suffixes=('_today', '_yesterday')
    )

    # 補齊缺失值
    merged['shares_today'] = merged['shares_today'].fillna(0)
    merged['shares_yesterday'] = merged['shares_yesterday'].fillna(0)
    merged['stock_name'] = merged['stock_name'].fillna('')

    # 計算差異
    merged['shares_change'] = merged['shares_today'] - merged['shares_yesterday']

    # 百分比變化（避免除以 0）
    def calc_change_pct(row):
        if row['shares_yesterday'] > 0:
            return (row['shares_change'] / row['shares_yesterday']) * 100
        elif row['shares_today'] > 0:
            return 100.0
        else:
            return 0.0

    merged['change_pct'] = merged.apply(calc_change_pct, axis=1)

    # 是否為大變動
    merged['highlight'] = merged['change_pct'].abs() >= highlight_pct

    # ========= 特殊動作判斷 =========
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

    # 排序：變動最大優先
    merged = merged.sort_values('change_pct', ascending=False).reset_index(drop=True)

    # 記錄比較日期（之後 UI 很好用）
    merged['compare_today'] = latest_date
    merged['compare_prev'] = prev_date

    return merged

# ----------------------
# 🆕 計算近一個月 Top 10 持股變化
# ----------------------
def compute_top10_trend(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    計算近一個月 Top 10 持股的持股變化趨勢（按持股百分比排序）
    """
    # 取得最新日期
    latest_date = df['date'].max()

    # 找出最新日期的 Top 10 持股（按 weight 排序）
    latest_df = df[df['date'] == latest_date].copy()
    latest_df = latest_df.sort_values('weight', ascending=False).head(top_n)  # ✅ 改回 weight

    # 🔍 DEBUG: 列印 Top 10
    print("\n" + "=" * 60)
    print("🔍 DEBUG: Top 10 股票（按持股百分比排序）")
    print("=" * 60)
    for idx, row in latest_df.iterrows():
        print(
            f"{row['stock_code']:6} {row['stock_name']:12} {row['weight']:>6.2f}%  股數: {row['shares']:>10,}")  # ✅ 改回 weight
    print("=" * 60 + "\n")

    top10_codes = latest_df['stock_code'].tolist()

    # 篩選出這些股票在整個時間範圍內的資料
    df_top10 = df[df['stock_code'].isin(top10_codes)].copy()
    df_top10 = df_top10.sort_values(['date', 'stock_code'])

    # 計算每檔股票的持股變化
    result = []
    for code in top10_codes:
        stock_data = df_top10[df_top10['stock_code'] == code].copy()

        if len(stock_data) >= 2:
            first_date = stock_data['date'].min()
            last_date = stock_data['date'].max()
            first_shares = stock_data[stock_data['date'] == first_date]['shares'].values[0]
            last_shares = stock_data[stock_data['date'] == last_date]['shares'].values[0]
            first_weight = stock_data[stock_data['date'] == first_date]['weight'].values[0]  # ✅ 改回 weight
            last_weight = stock_data[stock_data['date'] == last_date]['weight'].values[0]  # ✅ 改回 weight
            stock_name = stock_data['stock_name'].values[0]

            change = last_shares - first_shares
            change_pct = (change / first_shares * 100) if first_shares > 0 else 0
            weight_change = last_weight - first_weight  # ✅ 改回 weight

            result.append({
                'stock_code': code,
                'stock_name': stock_name,
                'first_date': first_date,
                'last_date': last_date,
                'first_shares': first_shares,
                'last_shares': last_shares,
                'shares_change': change,
                'change_pct': change_pct,
                'first_weight': first_weight,  # ✅ 改回 weight
                'last_weight': last_weight,  # ✅ 改回 weight
                'weight_change': weight_change  # ✅ 改回 weight
            })

    df_result = pd.DataFrame(result)
    df_result = df_result.sort_values('last_weight', ascending=False)  # ✅ 改回 weight

    # 🔍 DEBUG: 列印結果
    print("\n" + "=" * 60)
    print("🔍 DEBUG: Top 10 趨勢變化資料")
    print("=" * 60)
    for idx, row in df_result.iterrows():
        print(
            f"{row['stock_code']:6} {row['stock_name']:12} {row['last_weight']:>6.2f}% (變化: {row['weight_change']:>+6.2f}%)")  # ✅ 改回 weight
    print("=" * 60 + "\n")

    return df_result, df_top10

# ----------------------
# 🆕 取得 Top 10 每日持股資料（用於繪圖）
# ----------------------
def get_top10_daily_data(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    取得 Top 10 股票的每日持股數據（按持股百分比排序）
    """
    latest_date = df['date'].max()
    latest_df = df[df['date'] == latest_date].copy()

    # 🔧 改這裡：按 percentage 排序，而不是 shares
    latest_df = latest_df.sort_values('percentage', ascending=False).head(top_n)
    top10_codes = latest_df['stock_code'].tolist()

    df_daily = df[df['stock_code'].isin(top10_codes)].copy()
    df_daily = df_daily.sort_values(['stock_code', 'date'])

    return df_daily

# ----------------------
# 測試
# ----------------------
if __name__ == "__main__":
    df_hist = load_history(etf_code=ETF_CODE, days=30)
    print("📊 資料欄位名稱:")
    print(df_hist.columns.tolist())
    print("\n📊 前五筆資料:")
    print(df_hist.head())