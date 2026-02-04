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

    @staticmethod
    def analyze_30w_breakout_details(df: pd.DataFrame) -> pd.DataFrame:
        """
        核心 30 週戰法判定 - 支援日日選股與開盤基位判定
        """
        cfg = TechnicalStrategies.get_config()
        results = pd.DataFrame(index=df.index)
        results['Signal'] = 0
        results['Adh_Info'] = ""
        results['Shk_Info'] = ""

        if len(df) < 35: return results

        close, low, high, open_p, vol = df['Close'], df['Low'], df['High'], df['Open'], df['Volume']
        ma30 = close.rolling(window=30).mean()
        prev_ma30, prev_vol = ma30.shift(1), vol.shift(1)

        for i in range(30, len(df)):
            dt_str = df.index[i].strftime('%Y-%m-%d')
            is_debug = cfg.get('debug_mode', False) and (dt_str == cfg.get('debug_date'))

            prev_c = close.iloc[i - 1]
            if prev_c == 0 or pd.isna(ma30.iloc[i]): continue

            pct_change = (close.iloc[i] - prev_c) / prev_c
            curr_ma = ma30.iloc[i]
            p_ma_val = prev_ma30.iloc[i]

            # --- 基礎攻擊條件 ---
            fail_reasons = []
            if pct_change < cfg.get('trigger_min_gain', 0.10): fail_reasons.append("漲幅未達標")
            if close.iloc[i] <= open_p.iloc[i]: fail_reasons.append("非紅K")
            if vol.iloc[i] < prev_vol.iloc[i] * cfg.get('trigger_vol_multiplier', 1.1): fail_reasons.append(
                "量能未達標")
            if low.iloc[i] <= curr_ma: fail_reasons.append("盤中未脫離均線")

            if fail_reasons:
                if is_debug: print(f"--- 偵錯 {dt_str} --- 基礎門檻失敗: {', '.join(fail_reasons)}")
                continue

            is_adh, is_shk = False, False
            # 判斷基位：上週收盤距離 MA30 的乖離
            prev_bias = (prev_c - curr_ma) / curr_ma

            # --- 1. 黏貼整理 ---
            if curr_ma > p_ma_val and prev_bias <= 0.12:
                start_adh = i - cfg.get('adhesive_weeks', 2)
                if start_adh >= 0:
                    is_adh_tmp, max_d = True, 0.0
                    for k in range(start_adh, i):
                        dev = max(abs(high.iloc[k] - ma30.iloc[k]), abs(low.iloc[k] - ma30.iloc[k])) / ma30.iloc[k]
                        if dev > cfg.get('adhesive_bias', 0.2):
                            is_adh_tmp = False;
                            break
                        max_d = max(max_d, dev)
                    if is_adh_tmp:
                        is_adh = True
                        results.at[df.index[i], 'Adh_Info'] = f"{cfg.get('adhesive_weeks', 2)}w, ±{max_d * 100:.1f}%"

            # --- 2. 甩轎 ---
            if prev_bias <= cfg.get('shakeout_prev_bias_limit', 0.20):
                if curr_ma >= p_ma_val * 0.999 and prev_c >= ma30.iloc[i - 1]:
                    start_shk = max(0, i - cfg.get('shakeout_lookback', 12))
                    has_dip, valid_depth, uw_weeks = False, True, 0
                    for k in range(start_shk, i):
                        if low.iloc[k] < ma30.iloc[k]:
                            has_dip = True
                            if low.iloc[k] < ma30.iloc[k] * (1 - cfg.get('shakeout_max_depth', 0.35)):
                                valid_depth = False;
                                break
                        if close.iloc[k] < ma30.iloc[k]:
                            uw_weeks += 1

                    if is_debug:
                        print(f"--- 偵錯 {dt_str} --- 曾跌破={has_dip}, 深度合規={valid_depth}, 水下={uw_weeks}w")

                    if valid_depth and has_dip and (0 < uw_weeks <= cfg.get('shakeout_underwater_limit', 10)):
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