import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os
import math
import warnings
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    from utils.cache.manager import CacheManager
    from utils.strategies.technical import TechnicalStrategies
except ImportError:
    print("[Error] 找不到 utils.cache.manager")
    sys.exit(1)


def calculate_advanced_factors(df, sid=None):
    # [修正] 只要有數據就繼續，不要因為未滿 205 筆就回傳 None
    if df is None or len(df) == 0:
        return None

    # 初始化所有回傳值，預設為 0、-1 或 False，確保即使數據不足也不會導致欄位缺失
    factors = {
        '現價': 0.0, '漲幅5d': 0.0, '漲幅20d': 0.0, '漲幅60d': 0.0,
        'bb_width': 0.0, '量比': 0.0,
        'str_consol_5': 0, 'str_consol_10': 0, 'str_consol_20': 0, 'str_consol_60': 0,
        'str_ilss_sweep': 0, 'str_fake_breakdown': 0,
        'str_30w_adh': 0, 'str_30w_shk': 0, 'str_30w_info': "",
        'str_30w_week_offset': -1,
        'str_st_week_offset': -1,  # 🔥 新增 SuperTrend 買訊回溯欄位(週)
        'str_break_30w': 0, 'str_uptrend': 0, 'str_high_60': 0, 'str_high_30': 0,
        'str_ma55_sup': 0, 'str_ma200_sup': 0, 'str_vix_rev': 0
    }

    # 統一轉換欄位名稱
    df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'},
              inplace=True)

    # 1. 優先取得現價
    last_close_daily = df['Close'].iloc[-1]
    factors['現價'] = last_close_daily

    # 2. 計算基礎指標 (只要有 1 筆以上數據就能算，雖然 5d 需要 6 筆)
    if len(df) >= 2:
        factors['量比'] = round(df['Volume'].iloc[-1] / df['Volume'].tail(5).mean(), 2) if df['Volume'].tail(
            5).mean() > 0 else 0

    if len(df) >= 6:
        factors['漲幅5d'] = round(df['Close'].pct_change(5).iloc[-1] * 100, 2)

    if len(df) >= 21:
        factors['漲幅20d'] = round(df['Close'].pct_change(20).iloc[-1] * 100, 2)

        # 布林寬度計算
        ma20 = df['Close'].rolling(20).mean()
        std20 = df['Close'].rolling(20).std()
        bb_width_series = (4 * std20) / ma20 * 100
        factors['bb_width'] = round(bb_width_series.iloc[-1], 2) if not pd.isna(bb_width_series.iloc[-1]) else 0

        # 整理型態判斷
        factors['str_consol_5'] = int(bb_width_series.rolling(5).max().iloc[-1] < 10) if len(
            bb_width_series) >= 5 else 0
        factors['str_consol_10'] = int(bb_width_series.rolling(10).max().iloc[-1] < 12) if len(
            bb_width_series) >= 10 else 0
        factors['str_consol_20'] = int(bb_width_series.rolling(20).max().iloc[-1] < 15) if len(
            bb_width_series) >= 20 else 0

        # 假跌破判斷
        try:
            if (df['Close'].iloc[-2] < ma20.iloc[-2] and df['Close'].iloc[-1] > ma20.iloc[-1] and df['Close'].iloc[-1] >
                    df['Open'].iloc[-1]):
                factors['str_fake_breakdown'] = 1
        except:
            pass

    if len(df) >= 61:
        factors['漲幅60d'] = round(df['Close'].pct_change(60).iloc[-1] * 100, 2)
        factors['str_consol_60'] = int(bb_width_series.rolling(60).max().iloc[-1] < 18) if len(
            bb_width_series) >= 60 else 0

    # 3. 進階策略計算 (需要較長天數，例如 MA200 或 週線策略)
    if len(df) >= 200:
        def check_recent(series):
            return int(series.tail(3).any())

        # MA200 相關與 ILSS
        try:
            ma20 = df['Close'].rolling(20).mean()
            ma200 = df['Close'].rolling(200).mean()
            high_60 = df['High'].rolling(60).max()
            if (last_close_daily > ma200.iloc[-1]) and (ma200.iloc[-1] > ma200.iloc[-5]) and (
                    df['High'].tail(15) >= high_60.tail(15)).any():
                low_20d = df['Low'].rolling(20).min().shift(1)
                for i in range(3):
                    idx = -1 - i
                    s_level = min(low_20d.iloc[idx], ma20.iloc[idx]) if idx > -len(df) else 0
                    if s_level == 0: continue
                    break_depth = (s_level - df['Low'].iloc[idx]) / s_level
                    if (df['Low'].iloc[idx] < s_level) and (0.005 < break_depth < 0.08) and (
                            df['Volume'].iloc[idx] > (1.2 * df['Volume'].iloc[idx - 5:idx].mean())):
                        if (last_close_daily > s_level) and (last_close_daily > df['Open'].iloc[-1]) and (
                                last_close_daily > df['High'].iloc[idx]):
                            factors['str_ilss_sweep'] = 1
                            break
        except:
            pass

        # 其他技術特徵
        factors['str_break_30w'] = check_recent(TechnicalStrategies.break_30w_ma(df))
        factors['str_uptrend'] = int(TechnicalStrategies.strong_uptrend(df).iloc[-1])
        factors['str_high_60'] = check_recent(TechnicalStrategies.breakout_n_days_high(df, 60))
        factors['str_high_30'] = check_recent(TechnicalStrategies.breakout_n_days_high(df, 30))
        factors['str_ma55_sup'] = check_recent(TechnicalStrategies.near_ma_support(df, 55))
        factors['str_ma200_sup'] = check_recent(TechnicalStrategies.near_ma_support(df, 200))
        factors['str_vix_rev'] = check_recent(TechnicalStrategies.vix_reversal(df))

        # 4. 週線策略 (擴展回溯範圍)
        try:
            logic = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
            df_weekly = df.resample('W-FRI').agg(logic).dropna()

            # 🔥 --- 新增 SuperTrend 週線買訊回溯 ---
            if len(df_weekly) >= 10:
                try:
                    st_weekly = TechnicalStrategies.calculate_supertrend(df_weekly)
                    lookback_weeks = 26  # 往回追溯半年 (26週)
                    found_st_week = -1
                    max_idx = min(lookback_weeks, len(st_weekly) - 1)

                    for offset in range(max_idx + 1):
                        idx = -1 - offset
                        if st_weekly['Signal'].iloc[idx] == 1:
                            found_st_week = offset
                            break
                    factors['str_st_week_offset'] = found_st_week
                except Exception as e:
                    print(f"[Debug] ST週線計算錯誤 ({sid}): {e}")

            # 原本的 30W 策略需要 35+ 筆
            if len(df_weekly) >= 35:
                res_30w = TechnicalStrategies.analyze_30w_breakout_details(df_weekly)

                # 往回搜尋最多 52 週
                max_back = min(52, len(res_30w) - 1)
                found_offset = -1

                for offset in range(max_back + 1):
                    idx = -1 - offset
                    sig = res_30w['Signal'].iloc[idx]
                    if sig > 0:
                        found_offset = offset
                        factors['str_30w_week_offset'] = offset
                        factors['str_30w_adh'] = 1 if sig in [1, 3] else 0
                        factors['str_30w_shk'] = 1 if sig in [2, 3] else 0
                        factors[
                            'str_30w_info'] = f"({res_30w['Adh_Info'].iloc[idx] if sig in [1, 3] else res_30w['Shk_Info'].iloc[idx]})"
                        break
        except Exception as e:
            pass

    return factors


def main():
    print(f"[System] 因子運算啟動 (V5.5 - 整合ST訊號回溯) | {datetime.now():%H:%M:%S}")

    cache = CacheManager()
    raw_path = project_root / 'data' / 'temp' / 'chips_revenue_raw.csv'
    if not raw_path.exists():
        print("[Error] 找不到 chips_revenue_raw.csv")
        return

    raw_df = pd.read_csv(raw_path, dtype={'sid': str})
    tech_list = []
    symbols = cache.get_all_symbols(market='tw')
    total = len(symbols)
    if total == 0: total = 1

    for i, symbol in enumerate(symbols):
        sid = symbol.split('.')[0]
        if i % 50 == 0 or i == total - 1:
            pct = int((i + 1) / total * 100)
            print(f"PROGRESS: {pct}")
            print(f"   Processing: {i}/{total}...", end='\r')
            sys.stdout.flush()

        df = cache.load(symbol)
        factors = calculate_advanced_factors(df, sid=sid)
        if factors:
            factors['sid'] = sid
            tech_list.append(factors)

    print(f"\n[System] 計算完成，共 {len(tech_list)} 檔。")
    print("PROGRESS: 100")

    tech_df = pd.DataFrame(tech_list).set_index('sid')
    final_df = raw_df.merge(tech_df, on='sid', how='left')

    if '漲幅20d' in final_df.columns:
        final_df['RS強度'] = final_df['漲幅20d'].rank(pct=True) * 100
        final_df['RS強度'] = final_df['RS強度'].round(1)

    def get_strong_tags(row):
        tags = []

        # 🔥 --- ST 週線買訊標籤 ---
        st_week = row.get('str_st_week_offset', -1)
        if st_week == 0:
            tags.append('ST轉多(本週)')
        elif 0 < st_week <= 4:
            tags.append(f'ST轉多({int(st_week)}週前)')

        offset = row.get('str_30w_week_offset', -1)
        suffix = ""
        if offset == 0:
            suffix = "(本週)"
        elif offset > 0:
            suffix = f"({int(offset)}週前)"

        if row.get('str_30w_adh', 0) == 1: tags.append(f"30W黏貼{suffix}")
        if row.get('str_30w_shk', 0) == 1: tags.append(f"30W甩轎{suffix}")

        if row.get('str_consol_60', 0) == 1: tags.append('盤整60日')
        if row.get('str_consol_20', 0) == 1: tags.append('盤整20日')
        if row.get('str_consol_10', 0) == 1: tags.append('盤整10日')
        if row.get('str_consol_5', 0) == 1: tags.append('盤整5日')
        if row.get('bb_width', 100) < 5.0:
            tags.append('極度壓縮')
        elif row.get('bb_width', 100) < 8.0:
            tags.append('波動壓縮')
        if row.get('str_ilss_sweep', 0) == 1 and row.get('rev_cum_yoy', 0) > 0 and (
                row.get('m_net_today', 0) < 0 or row.get('m_sum_5d', 0) < 0): tags.append('主力掃單(ILSS)')
        if row.get('str_fake_breakdown', 0) == 1: tags.append('假跌破')
        if row.get('RS強度', 0) > 90: tags.append('超強勢')
        if row.get('漲幅60d', 0) > 30: tags.append('波段黑馬')
        if row.get('str_break_30w', 0) == 1: tags.append('突破30週')
        if row.get('str_uptrend', 0) == 1: tags.append('強勢多頭')
        if row.get('str_high_60', 0) == 1: tags.append('創季高')
        if row.get('str_high_30', 0) == 1: tags.append('創月高')
        if row.get('is_tu_yang', 0) == 1: tags.append('土洋對作')
        if row.get('t_streak', 0) >= 3: tags.append('投信認養')
        if row.get('m_net_today', 0) < -200: tags.append('散戶退場')
        if row.get('str_ma55_sup', 0) == 1: tags.append('回測季線')
        if row.get('str_ma200_sup', 0) == 1: tags.append('回測年線')
        if row.get('str_vix_rev', 0) == 1: tags.append('Vix反轉')
        return ','.join(tags)

    final_df['強勢特徵'] = final_df.apply(get_strong_tags, axis=1)

    chinese_map = {
        'sid': '股票代號', 'name': '股票名稱', 'industry': '產業別',
        'rev_yoy': '營收年增率(%)', 'rev_cum_yoy': '累計營收年增率(%)',
        'eps_q': '累計EPS',
        'eps_date': 'EPS年度/季',
        'rev_ym': '營收月份',
        't_sum_5d': '投信買賣超(5日)', 't_streak': '投信連買天數',
        'f_sum_5d': '外資買賣超(5日)', 'f_streak': '外資連買天數',
        'm_sum_5d': '融資增減(5日)', 'm_net_today': '融資增減(今日)',
        'pe': '本益比', 'yield': '殖利率(%)',
        '現價': '今日收盤價', '漲幅20d': '20日漲幅(%)', '漲幅60d': '3個月漲幅(%)',
        'bb_width': '布林寬度(%)', '量比': '成交量比', 'RS強度': 'RS強度', '強勢特徵': '強勢特徵標籤',
        'str_30w_week_offset': '30W訊號週數',
        'str_st_week_offset': 'ST買訊(週)'  # 🔥 對應中文名改回週線變數
    }

    output_df = final_df.copy().rename(columns=chinese_map)
    strategy_dir = project_root / 'data' / 'strategy_results'
    strategy_dir.mkdir(parents=True, exist_ok=True)
    final_df.to_parquet(strategy_dir / 'factor_snapshot.parquet')
    output_df.to_csv(strategy_dir / '戰情室今日快照_全中文版.csv', encoding='utf-8-sig', index=False)
    print("[System] 存檔完成。")


if __name__ == "__main__":
    main()