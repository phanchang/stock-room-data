# utils/strategies/technical.py
import pandas as pd
import numpy as np


class TechnicalStrategies:
    """
    技術指標策略庫 (嚴格版)
    """

    @staticmethod
    def break_30w_ma(df: pd.DataFrame) -> pd.Series:
        """
        策略：日級爆量突破 30 週均線
        條件：
        1. 收盤價突破 150日均線 (30週)
        2. 成交量 > 5日均量 * 2 (爆量)
        """
        if len(df) < 150:
            return pd.Series(False, index=df.index)

        ma_30w = df['Close'].rolling(window=150).mean()
        vol_ma_5 = df['Volume'].rolling(window=5).mean()

        # 昨收 <= 均線 AND 今收 > 均線
        cross_up = (df['Close'] > ma_30w) & (df['Close'].shift(1) <= ma_30w.shift(1))

        # 爆量
        volume_up = df['Volume'] > (vol_ma_5.shift(1) * 2.0)

        return cross_up & volume_up

    @staticmethod
    def consolidation(df: pd.DataFrame, period_days: int = 20, threshold: float = 0.15) -> pd.Series:
        """
        策略：量縮盤整 (嚴格版)
        條件：
        1. 區間震幅 < threshold (例如 10%)
        2. 當日成交量 < 20日均量 (量縮)
        3. 收盤價 > 60日均線 (季線之上，多頭排列)
        """
        if len(df) < 60:  # 需要算季線
            return pd.Series(False, index=df.index)

        # 1. 計算震幅
        rolling_max = df['Close'].rolling(window=period_days).max()
        rolling_min = df['Close'].rolling(window=period_days).min()
        amplitude = (rolling_max - rolling_min) / rolling_min
        is_flat = amplitude < threshold

        # 2. 計算量縮 (今天量 < 過去20天均量)
        vol_ma_20 = df['Volume'].rolling(window=20).mean()
        is_dry_volume = df['Volume'] < vol_ma_20

        # 3. 趨勢過濾 (股價在季線之上，不做空頭排列的盤整)
        ma_60 = df['Close'].rolling(window=60).mean()
        is_above_quarter = df['Close'] > ma_60

        return is_flat & is_dry_volume & is_above_quarter

    @staticmethod
    def strong_uptrend(df: pd.DataFrame) -> pd.Series:
        """
        策略：強勢多頭排列
        條件：
        1. 均線排列 5 > 10 > 20 > 60
        2. 季線 (60MA) 必須向上 (扣抵值概念簡化：今天股價 > 60天前股價)
        3. 今天必須是紅K (Close > Open)
        """
        if len(df) < 60:
            return pd.Series(False, index=df.index)

        ma5 = df['Close'].rolling(5).mean()
        ma10 = df['Close'].rolling(10).mean()
        ma20 = df['Close'].rolling(20).mean()
        ma60 = df['Close'].rolling(60).mean()

        # 均線排列
        alignment = (ma5 > ma10) & (ma10 > ma20) & (ma20 > ma60)

        # 季線趨勢向上 (簡單判斷：現在季線值 > 昨天季線值)
        trend_up = ma60 > ma60.shift(1)

        # 紅K棒 (稍微過濾掉雖然多頭排列但正在下殺的)
        is_red = df['Close'] > df['Open']

        return alignment & trend_up & is_red

        # utils/strategies/technical.py (請在類別內加入此方法)

    @staticmethod
    def breakout_n_days_high(df: pd.DataFrame, days: int = 30) -> pd.Series:
        """
        策略：收盤價創近 N 日新高
        定義：今天的收盤價 > 過去 N 天(不含今天)的最高價
        """
        if len(df) < days + 1:
            return pd.Series(False, index=df.index)

        # 取出「過去 N 天」的最高價 (用 High 欄位比較嚴格，若用 Close 也可以)
        # shift(1) 代表不包含今天，只看昨天以前的 N 天
        past_n_days_max = df['High'].shift(1).rolling(window=days).max()

        # 今天的收盤價 是否 突破過去 N 天最高
        is_breakout = df['Close'] > past_n_days_max

        return is_breakout
