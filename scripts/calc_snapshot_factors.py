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
    if df is None or len(df) < 205: return None

    cfg = TechnicalStrategies.get_config()
    last_close_daily = df['close'].iloc[-1]
    df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'},
              inplace=True)

    # 1. 轉周線
    logic = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
    df_weekly = df.resample('W-FRI').agg(logic).dropna()
    if len(df_weekly) < 35: return None

    # 2. 30W 策略
    s_30w_adh_signal, s_30w_shk_signal, s_30w_info = False, False, ""
    s_30w_week_offset = -1  # 預設 -1 (無訊號)

    try:
        res_30w = TechnicalStrategies.analyze_30w_breakout_details(df_weekly)
        lookback_days = int(cfg.get('signal_lookback_days', 10))
        lookback_weeks = math.ceil(lookback_days / 5) + 1

        # 取最近 N 週
        recent_res = res_30w.iloc[-lookback_weeks:]

        # 尋找訊號發生時間 (優先找最近的)
        # i=0 代表最近一週 (本週)
        for i in range(len(recent_res)):
            # 倒著找：從最近的開始 (-1, -2...)
            idx = -1 - i
            if abs(idx) > len(recent_res): break

            sig = recent_res['Signal'].iloc[idx]
            if sig > 0:
                s_30w_week_offset = i  # 0=本週, 1=上週, 2=前週

                # 記錄訊號類型
                if sig in [1, 3]:
                    s_30w_adh_signal = True
                    s_30w_info = f"({recent_res['Adh_Info'].iloc[idx]})"
                if sig in [2, 3]:
                    s_30w_shk_signal = True
                    s_30w_info = f"({recent_res['Shk_Info'].iloc[idx]})"
                break  # 找到最近的一次就停止

        # --- DEBUG Log (竹陞) ---
        if sid == '6739':
            print(f"\n======== DEBUG: 6739 (Week Offset Check) ========")
            print(f"Offset (幾週前): {s_30w_week_offset}")
            print(f"Info: {s_30w_info}")
            print("=================================================\n")

    except Exception as e:
        if sid == '6739': print(f"DEBUG Error: {e}")
        pass

    # --- 日線指標 ---
    roc_5 = df['Close'].pct_change(5).iloc[-1] * 100
    roc_20 = df['Close'].pct_change(20).iloc[-1] * 100
    roc_60 = df['Close'].pct_change(60).iloc[-1] * 100
    vol_ratio = df['Volume'].iloc[-1] / df['Volume'].tail(5).mean() if df['Volume'].tail(5).mean() > 0 else 0

    ma20 = df['Close'].rolling(20).mean()
    std20 = df['Close'].rolling(20).std()
    bb_width_series = (4 * std20) / ma20 * 100
    current_bb_width = bb_width_series.iloc[-1] if not pd.isna(bb_width_series.iloc[-1]) else 0

    s_consol_5 = int(bb_width_series.rolling(5).max().iloc[-1] < 10)
    s_consol_10 = int(bb_width_series.rolling(10).max().iloc[-1] < 12)
    s_consol_20 = int(bb_width_series.rolling(20).max().iloc[-1] < 15)
    s_consol_60 = int(bb_width_series.rolling(60).max().iloc[-1] < 18)

    s_fake_breakdown = 0
    try:
        if (df['Close'].iloc[-2] < ma20.iloc[-2] and df['Close'].iloc[-1] > ma20.iloc[-1] and df['Close'].iloc[-1] >
                df['Open'].iloc[-1]):
            s_fake_breakdown = 1
    except:
        pass

    s_ilss_sweep = 0
    try:
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
                        s_ilss_sweep = 1
                        break
    except:
        pass

    def check_recent(series):
        return int(series.tail(3).any())

    return {
        '現價': last_close_daily,
        '漲幅5d': round(roc_5, 2), '漲幅20d': round(roc_20, 2), '漲幅60d': round(roc_60, 2),
        'bb_width': round(current_bb_width, 2), '量比': round(vol_ratio, 2),
        'str_consol_5': s_consol_5, 'str_consol_10': s_consol_10, 'str_consol_20': s_consol_20,
        'str_consol_60': s_consol_60,
        'str_ilss_sweep': s_ilss_sweep, 'str_fake_breakdown': s_fake_breakdown,
        'str_30w_adh': 1 if s_30w_adh_signal else 0,
        'str_30w_shk': 1 if s_30w_shk_signal else 0,
        'str_30w_info': s_30w_info,
        'str_30w_week_offset': s_30w_week_offset,  # 🔥 新增欄位
        'str_break_30w': check_recent(TechnicalStrategies.break_30w_ma(df)),
        'str_uptrend': int(TechnicalStrategies.strong_uptrend(df).iloc[-1]),
        'str_high_60': check_recent(TechnicalStrategies.breakout_n_days_high(df, 60)),
        'str_high_30': check_recent(TechnicalStrategies.breakout_n_days_high(df, 30)),
        'str_ma55_sup': check_recent(TechnicalStrategies.near_ma_support(df, 55)),
        'str_ma200_sup': check_recent(TechnicalStrategies.near_ma_support(df, 200)),
        'str_vix_rev': check_recent(TechnicalStrategies.vix_reversal(df))
    }


def main():
    print(f"[System] 因子運算啟動 (V5.4 - 週數偏移功能) | {datetime.now():%H:%M:%S}")

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

    print(f"[System] 計算完成，共 {len(tech_list)} 檔。")
    print("PROGRESS: 100")

    tech_df = pd.DataFrame(tech_list).set_index('sid')
    final_df = raw_df.merge(tech_df, on='sid', how='left')

    if '漲幅20d' in final_df.columns:
        final_df['RS強度'] = final_df['漲幅20d'].rank(pct=True) * 100
        final_df['RS強度'] = final_df['RS強度'].round(1)

    def get_strong_tags(row):
        tags = []
        # 在標籤中也顯示週數，方便一眼看到
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
        'rev_yoy': '營收年增率(%)', 'rev_cum_yoy': '累計營收年增率(%)', 'eps_q': '累計EPS',
        'rev_ym': '營收月份',
        't_sum_5d': '投信買賣超(5日)', 't_streak': '投信連買天數',
        'f_sum_5d': '外資買賣超(5日)', 'f_streak': '外資連買天數',
        'm_sum_5d': '融資增減(5日)', 'm_net_today': '融資增減(今日)',
        'pe': '本益比', 'yield': '殖利率(%)',
        '現價': '今日收盤價', '漲幅20d': '20日漲幅(%)', '漲幅60d': '3個月漲幅(%)',
        'bb_width': '布林寬度(%)', '量比': '成交量比', 'RS強度': 'RS強度', '強勢特徵': '強勢特徵標籤',
        'str_30w_week_offset': '訊號週數'  # 🔥 對應中文名
    }

    output_df = final_df.copy().rename(columns=chinese_map)
    strategy_dir = project_root / 'data' / 'strategy_results'
    strategy_dir.mkdir(parents=True, exist_ok=True)
    final_df.to_parquet(strategy_dir / 'factor_snapshot.parquet')
    output_df.to_csv(strategy_dir / '戰情室今日快照_全中文版.csv', encoding='utf-8-sig', index=False)
    print("[System] 存檔完成。")


if __name__ == "__main__":
    main()