# utils/indicators/break_30w.py

import pandas as pd


def calc_break_30w(df: pd.DataFrame) -> pd.DataFrame:
    """
    計算日級爆量突破 30 週均線 (150 日線)

    條件:
    1. 昨日收盤 < 150 日均線
    2. 今日收盤 > 150 日均線  (突破!)
    3. 今日成交量 > 30 日均量 * 2  (爆量!)

    Returns:
        添加 daily_break_30w 欄位的 DataFrame
    """
    df = df.copy()

    # 確保必要欄位存在
    required_cols = ['close', 'volume']
    if not all(col in df.columns for col in required_cols):
        print(f"⚠️ 缺少必要欄位: {required_cols}")
        return df

    # 計算 150 日均線 (30 週 * 5 日)
    df['ma150'] = df['close'].rolling(window=150, min_periods=1).mean()

    # 計算 30 日平均量
    df['vol_ma30'] = df['volume'].rolling(window=30, min_periods=1).mean()

    # 🆕 計算昨日的收盤價和 150 日均線
    df['prev_close'] = df['close'].shift(1)
    df['prev_ma150'] = df['ma150'].shift(1)

    # 🆕 突破條件:
    # 1. 昨日收盤 < 昨日 150 日均線 (在下方)
    # 2. 今日收盤 > 今日 150 日均線 (突破上方)
    # 3. 今日成交量 > 30 日均量 * 2 (爆量)
    df['daily_break_30w'] = (
            (df['prev_close'] < df['prev_ma150']) &  # ⭐ 昨天在下方
            (df['close'] > df['ma150']) &  # ⭐ 今天在上方
            (df['volume'] > df['vol_ma30'] * 2)  # ⭐ 爆量
    )

    return df


if __name__ == "__main__":
    # 測試用
    import pandas as pd

    # 建立測試資料 (模擬突破情境)
    test_df = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=200),
        'close': [100] * 148 + [99, 98] + [105, 110, 115] + [120] * 47,  # 第 150 天突破
        'volume': [1000] * 148 + [1000, 1000] + [5000, 6000, 3000] + [1000] * 47  # 第 150-151 天爆量
    })

    print("測試資料 (前後各 5 天):")
    print(test_df.iloc[145:155][['date', 'close', 'volume']])

    # 計算 indicator
    result = calc_break_30w(test_df)

    # 顯示有觸發的日期
    triggered = result[result['daily_break_30w'] == True]
    print(f"\n觸發日級爆量突破 30W 的日期:")
    print(triggered[['date', 'close', 'volume', 'ma150', 'vol_ma30', 'daily_break_30w']])