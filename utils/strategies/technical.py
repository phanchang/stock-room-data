# utils/strategies/technical.py
import pandas as pd
import numpy as np
from utils.indicators import Indicators # 引用剛剛寫好的計算庫

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
    def above_ma(df: pd.DataFrame, window: int = 55) -> pd.Series:
        """ 策略：收盤價站上 N 日均線 """
        if len(df) < window:
            return pd.Series(False, index=df.index)

        ma = df['Close'].rolling(window=window).mean()
        return df['Close'] > ma

    @staticmethod
    def vix_green(df: pd.DataFrame, length: int = 22) -> pd.Series:
        """
        策略：Vix Fix 綠柱 (波動率低點)
        算法參考：WVF = ((Highest(Close, length) - Low) / Highest(Close, length)) * 100
        當 WVF 處於相對高檔時(代表股價相對低檔且波動大)，可能是底部。
        但在您的需求中 "綠柱" 通常指 CM Williams Vix Fix 的底部訊號。

        這裡實作簡化版：
        當收盤價創新高，Vix Fix 會很低 (灰色)。
        當股價急跌，Vix Fix 飆高 (綠色)。
        這裡我們篩選「Vix Fix 飆高」的日子 (通常是恐慌低點)。
        """
        if len(df) < length:
            return pd.Series(False, index=df.index)

        period_max = df['Close'].rolling(window=length).max()
        wvf = ((period_max - df['Low']) / period_max) * 100

        # 定義綠柱：WVF 超過 20日 Bollinger Upper Band (極端恐慌)
        # 或者簡單定義：WVF > 某個閾值 (例如 10%)
        # 這裡採用標準算法：WVF > 20日 WVF 的最高值 * 0.85 (相對高點)
        wvf_max_22 = wvf.rolling(window=length).max()

        is_green = wvf >= (wvf_max_22 * 0.90) & (wvf > 2.0)  # 修正係數可自訂
        return is_green


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
        # 🔥 修正：根據天數自動調整容許震幅 (天數越短，震幅要越小)
        # 如果外部傳入的 threshold 太大，這裡強制覆寫
        if period_days <= 5 and threshold > 0.04: threshold = 0.04
        if period_days <= 10 and threshold > 0.08: threshold = 0.08
        is_flat = amplitude < threshold

        # 2. 計算量縮 (今天量 < 過去20天均量)
        vol_ma_20 = df['Volume'].rolling(window=20).mean()
        is_dry_volume = df['Volume'] < vol_ma_20 * 0.75

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
    def near_ma_support(df: pd.DataFrame, window: int = 60, dist_pct: float = 0.02) -> pd.Series:
        """
        策略：回測均線支撐 (原本的 above_ma 太寬鬆)
        條件：
        1. 股價在均線之上
        2. 股價距離均線不到 2% (回測買點)
        3. 均線趨勢向上
        """
        if len(df) < window + 1:
            return pd.Series(False, index=df.index)

        ma = df['Close'].rolling(window=window).mean()

        # 條件1: 在均線之上
        is_above = df['Close'] > ma

        # 條件2: 離均線很近 (乖離率 < 2%)
        is_near = (df['Close'] - ma) / ma < dist_pct

        # 條件3: 均線本身是向上的 (扣抵值概念：今天MA > 昨天MA)
        ma_trend_up = ma > ma.shift(1)

        return is_above & is_near & ma_trend_up

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


    @staticmethod
    def vix_reversal(df: pd.DataFrame, period: int = 22) -> pd.Series:
        # 1. 計算 WVF (核心公式)
        wvf = Indicators.cm_williams_vix_fix(df, period)

        # 2. 計算 Bollinger Band 條件 (20日, 2倍標準差)
        wvf_ma = wvf.rolling(20).mean()
        wvf_std = wvf.rolling(20).std()
        upper_band = wvf_ma + (2.0 * wvf_std)

        # 3. 🔥 [補上] 計算 Range High 條件 (參考 Pine Script: lb=50, ph=0.85)
        # 意思：過去 50 天內 WVF 最高值的 85%
        range_high = wvf.rolling(50).max() * 0.85

        # 4. 定義「恐慌狀態」(綠柱)
        # 只要滿足 BB 上緣 OR 滿足 Range High，都算是綠柱
        is_green = (wvf >= upper_band) | (wvf >= range_high)

        # 5. 抓反轉訊號 (昨天綠 -> 今天灰)
        # shift(1) 代表昨天
        signal = (is_green.shift(1)) & (~is_green)

        return signal


