import pandas as pd
import numpy as np
import json
from pathlib import Path
from utils.indicators import Indicators


class TechnicalStrategies:
    """
    技術指標策略庫 - 整合 V3 (支援 JSON 設定與 30W 強規版)
    """
    @staticmethod
    def calculate_supertrend(df: pd.DataFrame, period: int = 10, multiplier: float = 3.0) -> pd.DataFrame:
        if len(df) < period:
            return pd.DataFrame(index=df.index, columns=['SuperTrend', 'Direction', 'Signal']).fillna(0)

        high, low, close = df['High'], df['Low'], df['Close']

        # 1. 計算 ATR
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()

        # 2. 基礎軌道
        hl2 = (high + low) / 2
        bu_vals = (hl2 + (multiplier * atr)).values
        bl_vals = (hl2 - (multiplier * atr)).values
        c_vals = close.values

        # 3. 初始化陣列
        n = len(df)
        final_upper = np.zeros(n)
        final_lower = np.zeros(n)
        supertrend = np.zeros(n)
        direction = np.ones(n)  # 預設 1 (多)

        # 找到第一個有 ATR 值的索引 (通常是 period-1)
        start_idx = period - 1

        # 初始化第一個有效位置
        final_upper[start_idx] = bu_vals[start_idx]
        final_lower[start_idx] = bl_vals[start_idx]

        for i in range(start_idx + 1, n):
            # Final Upper Band
            if (bu_vals[i] < final_upper[i - 1]) or (c_vals[i - 1] > final_upper[i - 1]):
                final_upper[i] = bu_vals[i]
            else:
                final_upper[i] = final_upper[i - 1]

            # Final Lower Band
            if (bl_vals[i] > final_lower[i - 1]) or (c_vals[i - 1] < final_lower[i - 1]):
                final_lower[i] = bl_vals[i]
            else:
                final_lower[i] = final_lower[i - 1]

            # 判斷趨勢轉換
            if direction[i - 1] == 1:
                if c_vals[i] < final_lower[i]:
                    direction[i] = -1
                    supertrend[i] = final_upper[i]
                else:
                    direction[i] = 1
                    supertrend[i] = final_lower[i]
            else:
                if c_vals[i] > final_upper[i]:
                    direction[i] = 1
                    supertrend[i] = final_lower[i]
                else:
                    direction[i] = -1
                    supertrend[i] = final_upper[i]

        # 4. 整理結果 (補回前段的 NaN)
        res = pd.DataFrame({
            'SuperTrend': supertrend,
            'Direction': direction
        }, index=df.index)

        # 將 start_idx 之前的 0 轉為 NaN 以免繪圖拉成一條橫線
        res.iloc[:start_idx, res.columns.get_loc('SuperTrend')] = np.nan

        # 計算 Signal
        res['Signal'] = res['Direction'].diff().fillna(0).apply(
            lambda x: 1 if x > 1 else (-1 if x < -1 else 0)
        )

        return res
    @staticmethod
    def get_config():
        """
        從 data/strategy_config.json 讀取策略參數，若失敗則使用預設值
        """
        # 定義設定檔路徑 (相對於此檔案的位置)
        config_path = Path(__file__).resolve().parent.parent.parent / "data" / "strategy_config.json"

        # 預設參數 (防呆用)
        default_cfg = {
            "trigger_min_gain": 0.10,
            "trigger_vol_multiplier": 1.1,
            "adhesive_weeks": 2,
            "adhesive_bias": 0.2,
            "shakeout_lookback": 12,
            "shakeout_max_depth": 0.35,
            "shakeout_underwater_limit": 10,
            "shakeout_prev_bias_limit": 0.20,
            "signal_lookback_days": 10,
            "debug_mode": True,
            "debug_date": "2025-06-06"
        }

        try:
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get('30w_strategy', default_cfg)
            return default_cfg
        except:
            return default_cfg

        # --- 檔案: modules/technical.py ---
        # 在檔案中找到 TechnicalStrategies 類別，加入與修改以下方法：

        @staticmethod
        def calculate_bollinger_bands(df: pd.DataFrame, window: int = 20, num_std: float = 2.0) -> pd.DataFrame:
            """📦 新增：計算布林通道寬度 (提供給 L3Score 或未來 K 線圖使用)"""
            if len(df) < window:
                return pd.DataFrame(index=df.index,
                                    columns=['BB_Middle', 'BB_Upper', 'BB_Lower', 'BB_Width_Pct']).fillna(0)

            # 兼容大小寫
            close_col = 'Close' if 'Close' in df.columns else 'close'

            middle = df[close_col].rolling(window=window).mean()
            std = df[close_col].rolling(window=window).std()
            upper = middle + (std * num_std)
            lower = middle - (std * num_std)
            # 寬度百分比 = (上軌 - 下軌) / 中軌 * 100
            width_pct = ((upper - lower) / middle) * 100

            return pd.DataFrame({
                'BB_Middle': middle, 'BB_Upper': upper, 'BB_Lower': lower, 'BB_Width_Pct': width_pct
            })

    @staticmethod
    def analyze_30w_breakout_details(df: pd.DataFrame, is_sector: bool = False) -> pd.DataFrame:
        """
        核心 30 週戰法判定 (已精確對齊 debug_30w.py 邏輯)
        """
        cfg = TechnicalStrategies.get_config()
        results = pd.DataFrame(index=df.index)
        results['Signal'] = 0
        results['Adh_Info'] = ""
        results['Shk_Info'] = ""

        if len(df) < 35: return results

        close, low, high, open_p, vol = df['Close'], df['Low'], df['High'], df['Open'], df['Volume']
        ma30 = close.rolling(window=30).mean()
        prev_ma30 = ma30.shift(1)
        prev_close = close.shift(1)
        prev_vol = vol.shift(1)

        # 放寬板塊指數門檻
        min_gain = 0.02 if is_sector else cfg.get('trigger_min_gain', 0.10)
        vol_mult = 1.0 if is_sector else cfg.get('trigger_vol_multiplier', 1.1)

        for i in range(30, len(df)):
            dt_str = df.index[i].strftime('%Y-%m-%d')
            is_debug = cfg.get('debug_mode', False) and (dt_str == cfg.get('debug_date'))

            p_c_val = prev_close.iloc[i]
            curr_ma = ma30.iloc[i]
            p_ma_val = prev_ma30.iloc[i]

            # 防呆：確保相關均線與上週收盤價存在
            if p_c_val == 0 or pd.isna(curr_ma) or pd.isna(p_ma_val): continue

            pct_change = (close.iloc[i] - p_c_val) / p_c_val

            # --- 基礎攻擊條件 ---
            fail_reasons = []
            if pct_change < min_gain: fail_reasons.append(f"漲幅未達標 (<{min_gain * 100}%)")
            if close.iloc[i] <= open_p.iloc[i]: fail_reasons.append("非紅K")
            if vol.iloc[i] < prev_vol.iloc[i] * vol_mult: fail_reasons.append("量能未達標")
            if close.iloc[i] <= curr_ma: fail_reasons.append("收盤未站上均線")

            if fail_reasons:
                if is_debug: print(f"--- 偵錯 {dt_str} --- 基礎門檻失敗: {', '.join(fail_reasons)}")
                continue

            is_adh, is_shk = False, False
            # 判斷基位：上週收盤距離 MA30 的乖離
            prev_bias = (p_c_val - curr_ma) / curr_ma

            # --- 1. 黏貼整理 ---
            if curr_ma > p_ma_val and prev_bias <= cfg.get('adhesive_bias', 0.12):
                start_adh = i - cfg.get('adhesive_weeks', 2)
                if start_adh >= 0:
                    is_adh_tmp, max_d = True, 0.0
                    for k in range(start_adh, i):
                        if pd.isna(ma30.iloc[k]): continue  # 容許更早期的 NA，不中斷
                        dev = max(abs(high.iloc[k] - ma30.iloc[k]), abs(low.iloc[k] - ma30.iloc[k])) / ma30.iloc[k]
                        if dev > cfg.get('adhesive_bias', 0.2):
                            is_adh_tmp = False
                            break
                        max_d = max(max_d, dev)
                    if is_adh_tmp:
                        is_adh = True
                        results.at[df.index[i], 'Adh_Info'] = f"{cfg.get('adhesive_weeks', 2)}w, ±{max_d * 100:.1f}%"

            # --- 2. 甩轎 ---
            if prev_bias <= cfg.get('shakeout_prev_bias_limit', 0.20):
                # 起漲點邏輯：上週收盤 >= 上週 MA30
                if curr_ma >= p_ma_val * 0.999 and p_c_val >= p_ma_val:
                    start_shk = max(0, i - cfg.get('shakeout_lookback', 12))
                    has_dip, uw_weeks = False, 0
                    for k in range(start_shk, i):
                        if pd.isna(ma30.iloc[k]): continue
                        if low.iloc[k] < ma30.iloc[k]: has_dip = True
                        if close.iloc[k] < ma30.iloc[k]: uw_weeks += 1

                    if is_debug:
                        print(f"--- 偵錯 {dt_str} --- 曾跌破={has_dip}, 水下={uw_weeks}w")

                    if has_dip and (0 < uw_weeks <= cfg.get('shakeout_underwater_limit', 10)):
                        is_shk = True
                        results.at[df.index[i], 'Shk_Info'] = f"Dip {uw_weeks}w"

            # 存入 Signal: 1=Adh, 2=Shk, 3=Both
            if is_adh and is_shk:
                results.at[df.index[i], 'Signal'] = 3
            elif is_adh:
                results.at[df.index[i], 'Signal'] = 1
            elif is_shk:
                results.at[df.index[i], 'Signal'] = 2

        return results
    # ==============================================================================
    # 💎 以下完整保留您原本所有的選股 Fuction，不做任何刪減
    # ==============================================================================

    @staticmethod
    def break_30w_ma(df: pd.DataFrame) -> pd.Series:
        if len(df) < 150: return pd.Series(False, index=df.index)
        ma_30w = df['Close'].rolling(window=150).mean()
        vol_ma_5 = df['Volume'].rolling(window=5).mean()
        return (df['Close'] > ma_30w) & (df['Close'].shift(1) <= ma_30w.shift(1)) & (
                    df['Volume'] > vol_ma_5.shift(1) * 2.0)

    @staticmethod
    def above_ma(df: pd.DataFrame, window: int = 55) -> pd.Series:
        if len(df) < window: return pd.Series(False, index=df.index)
        return df['Close'] > df['Close'].rolling(window=window).mean()

    @staticmethod
    def vix_green(df: pd.DataFrame, length: int = 22) -> pd.Series:
        if len(df) < length: return pd.Series(False, index=df.index)
        p_max = df['Close'].rolling(window=length).max()
        wvf = ((p_max - df['Low']) / p_max) * 100
        return wvf >= (wvf.rolling(window=length).max() * 0.90) & (wvf > 2.0)

    @staticmethod
    def consolidation(df: pd.DataFrame, period_days: int = 20, threshold: float = 0.15) -> pd.Series:
        if len(df) < 60: return pd.Series(False, index=df.index)
        r_max = df['Close'].rolling(window=period_days).max()
        r_min = df['Close'].rolling(window=period_days).min()
        amp = (r_max - r_min) / r_min
        vol_20 = df['Volume'].rolling(window=20).mean()
        ma_60 = df['Close'].rolling(window=60).mean()
        return (amp < threshold) & (df['Volume'] < vol_20 * 0.75) & (df['Close'] > ma_60)

    @staticmethod
    def strong_uptrend(df: pd.DataFrame) -> pd.Series:
        if len(df) < 60: return pd.Series(False, index=df.index)
        m5, m10, m20, m60 = [df['Close'].rolling(w).mean() for w in [5, 10, 20, 60]]
        return (m5 > m10) & (m10 > m20) & (m20 > m60) & (m60 > m60.shift(1)) & (df['Close'] > df['Open'])

    @staticmethod
    def near_ma_support(df: pd.DataFrame, window: int = 60, dist_pct: float = 0.02) -> pd.Series:
        if len(df) < window + 1: return pd.Series(False, index=df.index)
        ma = df['Close'].rolling(window=window).mean()
        return (df['Close'] > ma) & ((df['Close'] - ma) / ma < dist_pct) & (ma > ma.shift(1))

    @staticmethod
    def breakout_n_days_high(df: pd.DataFrame, days: int = 30) -> pd.Series:
        if len(df) < days + 1: return pd.Series(False, index=df.index)
        return df['Close'] > df['High'].shift(1).rolling(window=days).max()

    @staticmethod
    def vix_reversal(df: pd.DataFrame, period: int = 22) -> pd.Series:
        wvf = Indicators.cm_williams_vix_fix(df, period)
        upper = wvf.rolling(20).mean() + (2.0 * wvf.rolling(20).std())
        r_high = wvf.rolling(50).max() * 0.85
        is_green = (wvf >= upper) | (wvf >= r_high)
        return (is_green.shift(1)) & (~is_green)

    # ==============================================================================
    # 🎯 以下為新增的 V3 臨門一腳 (聽牌) 專用邏輯，完全不影響原先的突破邏輯
    # ==============================================================================
    @staticmethod
    def check_30w_standby(df: pd.DataFrame) -> pd.Series:
        """
        [精準對齊版] 30W 臨門一腳 (Standby Setup)
        邏輯：完全套用 30W黏貼 或 30W甩轎 的基位與型態規則。
        唯一的差別：將突破漲幅 (>=10%) 改為「聽牌漲幅」(0% ~ 9.85%)。
        """
        cfg = TechnicalStrategies.get_config()
        results = pd.Series(0, index=df.index)
        if len(df) < 35: return results

        close, low, high, open_p, vol = df['Close'], df['Low'], df['High'], df['Open'], df['Volume']
        ma30 = close.rolling(window=30).mean()
        prev_ma30 = ma30.shift(1)
        prev_close = close.shift(1)

        for i in range(30, len(df)):
            p_c_val = prev_close.iloc[i]
            curr_ma = ma30.iloc[i]
            p_ma_val = prev_ma30.iloc[i]

            # 確保 MA30 與前一根收盤價存在
            if p_c_val == 0 or pd.isna(curr_ma) or pd.isna(p_ma_val): continue

            # ==========================================
            # 💡 唯一不同的條件：聽牌漲幅必須在 0% ~ 9.85% 之間
            # ==========================================
            pct_change = (close.iloc[i] - p_c_val) / p_c_val

            if not (0 <= pct_change < 0.0985):
                continue

            # 準備攻擊的底線：今天收盤價必須已經踩在均線之上
            if close.iloc[i] < curr_ma:
                continue

            # ==========================================
            # 💡 以下 100% 複製 analyze_30w_breakout_details 的官方型態規則
            # ==========================================
            is_adh, is_shk = False, False
            prev_bias = (p_c_val - curr_ma) / curr_ma

            # --- 1. 黏貼整理 (官方規則) ---
            if curr_ma > p_ma_val and prev_bias <= cfg.get('adhesive_bias', 0.12):
                start_adh = i - cfg.get('adhesive_weeks', 2)
                if start_adh >= 0:
                    is_adh_tmp = True
                    for k in range(start_adh, i):
                        if pd.isna(ma30.iloc[k]): continue
                        dev = max(abs(high.iloc[k] - ma30.iloc[k]), abs(low.iloc[k] - ma30.iloc[k])) / ma30.iloc[k]
                        if dev > cfg.get('adhesive_bias', 0.2):
                            is_adh_tmp = False
                            break
                    if is_adh_tmp:
                        is_adh = True

            # --- 2. 甩轎 (官方規則) ---
            if prev_bias <= cfg.get('shakeout_prev_bias_limit', 0.20):
                if curr_ma >= p_ma_val * 0.999 and p_c_val >= p_ma_val:
                    start_shk = max(0, i - cfg.get('shakeout_lookback', 12))
                    has_dip, uw_weeks = False, 0
                    for k in range(start_shk, i):
                        if pd.isna(ma30.iloc[k]): continue
                        if low.iloc[k] < ma30.iloc[k]: has_dip = True
                        if close.iloc[k] < ma30.iloc[k]: uw_weeks += 1

                    if has_dip and (0 < uw_weeks <= cfg.get('shakeout_underwater_limit', 10)):
                        is_shk = True

            # 只要完美符合黏貼或甩轎其中之一，且漲幅落在 0~9.85%，即宣告聽牌！
            if is_adh or is_shk:
                results.iloc[i] = 1

        return results

    @staticmethod
    def calculate_bollinger_bands(df: pd.DataFrame, window: int = 20, num_std: float = 2.0) -> pd.DataFrame:
        """📦 新增：計算布林通道寬度 (提供給 L3Score 或未來 K 線圖使用)"""
        if len(df) < window:
            return pd.DataFrame(index=df.index, columns=['BB_Middle', 'BB_Upper', 'BB_Lower', 'BB_Width_Pct']).fillna(0)

        # 兼容大小寫
        close_col = 'Close' if 'Close' in df.columns else 'close'

        middle = df[close_col].rolling(window=window).mean()
        std = df[close_col].rolling(window=window).std()
        upper = middle + (std * num_std)
        lower = middle - (std * num_std)
        # 寬度百分比 = (上軌 - 下軌) / 中軌 * 100
        width_pct = ((upper - lower) / middle) * 100

        return pd.DataFrame({
            'BB_Middle': middle, 'BB_Upper': upper, 'BB_Lower': lower, 'BB_Width_Pct': width_pct
        })