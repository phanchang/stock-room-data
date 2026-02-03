import pandas as pd
import numpy as np
from utils.indicators import Indicators


class TechnicalStrategies:
    """
    技術指標策略庫 (30週戰法核心邏輯 - 偵錯強規版)
    """

    # ==============================================================================
    # ⚙️ 策略參數與偵錯設定
    # ==============================================================================
    STRATEGY_CONFIG = {
        # --- 偵錯開關 ---
        'debug_mode': True,  # 預設開啟偵錯 LOG
        'debug_date': '2025-05-30',  # 輸入想 DEBUG 的日期 (格式: YYYY-MM-DD)

        # --- A. 攻擊訊號 (Trigger) ---
        'trigger_min_gain': 0.10,  # [漲幅] 本週漲幅 >= 10%
        'trigger_vol_multiplier': 1.1,  # [量增] 本週成交量 > 上週 * 1.1倍

        # --- B. 情境1：黏貼整理 (Adhesive) ---
        'adhesive_weeks': 2,
        'adhesive_bias': 0.2,

        # --- C. 情境2：甩轎 (Shakeout) ---
        'shakeout_lookback': 12,
        'shakeout_max_depth': 0.35,  # 提高到 35% 容許台燿等級洗盤
        'shakeout_underwater_limit': 10,  # 提高到 10 週
        'shakeout_prev_bias_limit': 0.15,  # [關鍵] 限制上週收盤乖離 15% 內，過濾高檔誤報
    }

    @staticmethod
    def analyze_30w_breakout_details(df: pd.DataFrame) -> pd.DataFrame:
        cfg = TechnicalStrategies.STRATEGY_CONFIG
        results = pd.DataFrame(index=df.index)
        results['Signal'] = 0
        results['Adh_Info'] = ""
        results['Shk_Info'] = ""

        if len(df) < 35: return results

        close, low, high, open_p, vol = df['Close'], df['Low'], df['High'], df['Open'], df['Volume']
        ma30 = close.rolling(window=30).mean()
        prev_ma30, prev_vol = ma30.shift(1), vol.shift(1)

        for i in range(30, len(df)):
            dt = df.index[i]
            dt_str = dt.strftime('%Y-%m-%d')
            is_debug_day = cfg['debug_mode'] and (dt_str == cfg['debug_date'])

            prev_c = close.iloc[i - 1]
            if prev_c == 0 or pd.isna(ma30.iloc[i]): continue

            # --- 數據準備 ---
            pct_change = (close.iloc[i] - prev_c) / prev_c
            curr_ma = ma30.iloc[i]
            p_ma = prev_ma30.iloc[i]
            # 判斷基準：上週收盤價距離 MA30 的位置
            prev_bias = (prev_c - curr_ma) / curr_ma

            # ------------------------------------------------------------------
            # 🛑 偵錯日誌：基礎數據區
            # ------------------------------------------------------------------
            if is_debug_day:
                print(f"\n{'=' * 20} 策略偵錯報告: {dt_str} {'=' * 20}")
                print(
                    f"[數據] 收盤: {close.iloc[i]:.2f}, 漲幅: {pct_change * 100:.2f}%, 量比: {vol.iloc[i] / prev_vol.iloc[i]:.2f}x")
                print(f"[均線] MA30: {curr_ma:.2f}, 上週MA30: {p_ma:.2f}, 上週收盤乖離: {prev_bias * 100:.2f}%")
                print(f"[條件檢查結果]:")

            # --- 1. 基礎攻擊條件判定 ---
            fail_reasons = []
            if pct_change < cfg['trigger_min_gain']: fail_reasons.append(f"漲幅未達標({pct_change * 100:.1f}% < 10%)")
            if close.iloc[i] <= open_p.iloc[i]: fail_reasons.append("本週為陰 K (收盤 <= 開盤)")
            if vol.iloc[i] < prev_vol.iloc[i] * cfg['trigger_vol_multiplier']: fail_reasons.append(
                f"量增不足({vol.iloc[i] / prev_vol.iloc[i]:.2f}x < 1.1x)")
            if low.iloc[i] <= curr_ma: fail_reasons.append(
                f"未脫離均線(最低價 {low.iloc[i]:.2f} 觸碰到 MA30 {curr_ma:.2f})")

            if fail_reasons:
                if is_debug_day: print(f"  ❌ 基礎攻擊條件未過: {', '.join(fail_reasons)}")
                continue
            elif is_debug_day:
                print("  ✅ 基礎攻擊條件: 通過")

            is_adh, is_shk = False, False

            # --- 2. 黏貼整理 (Adhesive) ---
            if curr_ma > p_ma and prev_bias <= 0.12:
                start_adh = i - cfg['adhesive_weeks']
                if start_adh >= 0:
                    is_adh_tmp, max_d = True, 0.0
                    for k in range(start_adh, i):
                        dev = max(abs(high.iloc[k] - ma30.iloc[k]), abs(low.iloc[k] - ma30.iloc[k])) / ma30.iloc[k]
                        if dev > cfg['adhesive_bias']:
                            is_adh_tmp = False;
                            break
                        max_d = max(max_d, dev)
                    if is_adh_tmp:
                        is_adh = True
                        results.at[df.index[i], 'Adh_Info'] = f"{cfg['adhesive_weeks']}w, ±{max_d * 100:.1f}%"
            elif is_debug_day:
                print(f"  ℹ️  情境1(黏貼): 未成立 (原因: MA30向下或上週乖離 > 12%)")

            # --- 3. 甩轎 (Shakeout) ---
            shk_fail = []
            if prev_bias > cfg['shakeout_prev_bias_limit']:
                shk_fail.append(f"起點乖離過大({prev_bias * 100:.1f}% > 15%，非起漲區)")

            if curr_ma < p_ma * 0.999:  # 容許微幅波動
                shk_fail.append("MA30 斜率向下")

            if close.iloc[i - 1] < ma30.iloc[i - 1]:
                shk_fail.append("發動前週(i-1)收盤仍在水下，未確認站回")

            if not shk_fail:
                start_shk = max(0, i - cfg['shakeout_lookback'])
                has_dip, valid_depth, uw_weeks = False, True, 0
                for k in range(start_shk, i):
                    if low.iloc[k] < ma30.iloc[k]:
                        has_dip = True
                        if low.iloc[k] < ma30.iloc[k] * (1 - cfg['shakeout_max_depth']):
                            valid_depth = False;
                            break
                    if close.iloc[k] < ma30.iloc[k]:
                        uw_weeks += 1

                if not has_dip:
                    shk_fail.append("回溯期內無跌破(Dip)紀錄")
                elif not valid_depth:
                    shk_fail.append(f"跌破深度超過限制({cfg['shakeout_max_depth'] * 100}%)")
                elif not (0 < uw_weeks <= cfg['shakeout_underwater_limit']):
                    shk_fail.append(f"水下週數({uw_weeks})超出範圍(1~{cfg['shakeout_underwater_limit']}週)")
                else:
                    is_shk = True
                    results.at[df.index[i], 'Shk_Info'] = f"Dip {uw_weeks}w"

            if is_debug_day:
                if is_shk:
                    print(f"  ✅ 情境2(甩轎): 通過 ({results.at[df.index[i], 'Shk_Info']})")
                else:
                    print(f"  ❌ 情境2(甩轎): 未過 ({', '.join(shk_fail)})")

            # 存入 Signal
            if is_adh and is_shk:
                results.at[df.index[i], 'Signal'] = 3
            elif is_adh:
                results.at[df.index[i], 'Signal'] = 1
            elif is_shk:
                results.at[df.index[i], 'Signal'] = 2

        return results

    # --- 以下保留原本所有方法，不刪減 ---
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