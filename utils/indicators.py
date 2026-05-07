import pandas as pd
import numpy as np


class Indicators:
    """
    通用技術指標計算庫 (Calculation Layer)
    原則：
    1. 輸入: DataFrame (必須包含 Open, High, Low, Close)
    2. 輸出: Series (單一指標) 或 DataFrame (多重指標如 KD, MACD)
    3. 不涉及 IO 操作
    """

    @staticmethod
    def cm_williams_vix_fix(df: pd.DataFrame, period: int = 22) -> pd.Series:
        """
        CM Williams Vix Fix 恐慌指標
        公式: WVF = ((Highest(Close, period) - Low) / Highest(Close, period)) * 100
        """
        # 確保資料夠長
        if len(df) < period:
            return pd.Series(0, index=df.index)

        highest_close = df['Close'].rolling(window=period).max()
        wvf = ((highest_close - df['Low']) / highest_close) * 100

        return wvf.fillna(0)

    @staticmethod
    def rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """ 相對強弱指標 (RSI) - Wilder's Smoothing """
        delta = df['Close'].diff()

        gain = (delta.where(delta > 0, 0)).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)

        avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return rsi.fillna(0)

    @staticmethod
    def macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        """ MACD (回傳 DIF, DEA, MACD柱狀) """
        ema_fast = df['Close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['Close'].ewm(span=slow, adjust=False).mean()

        dif = ema_fast - ema_slow
        dea = dif.ewm(span=signal, adjust=False).mean()
        hist = (dif - dea) * 2

        return pd.DataFrame({'DIF': dif, 'DEA': dea, 'MACD': hist}, index=df.index)

    @staticmethod
    def kd(df: pd.DataFrame, period: int = 9) -> pd.DataFrame:
        """ KD 隨機指標 (回傳 K, D) """
        low_min = df['Low'].rolling(window=period).min()
        high_max = df['High'].rolling(window=period).max()

        rsv = ((df['Close'] - low_min) / (high_max - low_min)) * 100
        rsv = rsv.fillna(50)

        k_values = []
        d_values = []
        k, d = 50, 50

        for val in rsv:
            k = (2 / 3) * k + (1 / 3) * val
            d = (2 / 3) * d + (1 / 3) * k
            k_values.append(k)
            d_values.append(d)

        return pd.DataFrame({'K': k_values, 'D': d_values}, index=df.index)