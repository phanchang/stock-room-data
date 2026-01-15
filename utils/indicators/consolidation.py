# utils/indicators/consolidation.py

import pandas as pd
import numpy as np


def calc_consolidation(df: pd.DataFrame, period_days=20, box_threshold=0.15, ma_squeeze_threshold=0.05) -> pd.DataFrame:
    """
    計算盤整指標 (Consolidation)

    邏輯：
    1. 箱體震幅 (Amplitude) < 15% (預設)
    2. 均線糾結 (MA20 與 MA60 差異) < 5% (預設)
    3. 股價位於季線 (MA60) 之上 (多頭整理)
    """
    # 避免修改原始資料
    df = df.copy()

    # 確保必要的欄位存在 (假設欄位名稱為 Title Case，若您的資料是小寫請自行調整)
    required_cols = ['Close', 'High', 'Low']
    for col in required_cols:
        if col not in df.columns:
            # 嘗試相容性處理 (例如全小寫)
            if col.lower() in df.columns:
                df[col] = df[col.lower()]
            else:
                # 若真的缺欄位，回傳全 False
                df['is_consolidating'] = False
                return df

    # 1. 計算均線
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA60'] = df['Close'].rolling(window=60).mean()

    # 2. 計算區間高低點 (箱體)
    df['Box_High'] = df['High'].rolling(window=period_days).max()
    df['Box_Low'] = df['Low'].rolling(window=period_days).min()

    # 3. 計算震幅 (Amplitude)
    # 避免除以 0 的保護機制
    df['Box_Amp'] = (df['Box_High'] - df['Box_Low']) / df['Box_Low'].replace(0, np.nan)

    # 4. 計算均線乖離 (Squeeze)
    df['MA_Div'] = abs(df['MA20'] - df['MA60']) / df['MA60'].replace(0, np.nan)

    # 5. 綜合判斷邏輯
    # 條件 A: 震幅壓縮
    cond_amp = df['Box_Amp'] <= box_threshold

    # 條件 B: 均線糾結
    cond_squeeze = df['MA_Div'] <= ma_squeeze_threshold

    # 條件 C: 趨勢支撐 (收盤價 > 季線) -> 可選，確保不是盤跌
    cond_trend = df['Close'] > df['MA60']

    # 產生結果欄位 (布林值)
    df['is_consolidating'] = (cond_amp & cond_squeeze & cond_trend)

    # 填補 NaN 為 False (前幾天算不出 MA 的部分)
    df['is_consolidating'] = df['is_consolidating'].fillna(False)

    return df