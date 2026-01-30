import pandas as pd
import numpy as np
from pathlib import Path
import sys
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    from utils.cache.manager import CacheManager
    from utils.strategies.technical import TechnicalStrategies
except ImportError:
    print("❌ 找不到 utils.cache.manager，請檢查資料夾路徑。")
    sys.exit(1)


def calculate_advanced_factors(df):
    """
    計算技術因子與策略訊號 (V4.0 - 找回假跌破 + ILSS)
    """
    if df is None or len(df) < 205: return None

    last_close = df['close'].iloc[-1]
    df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'},
              inplace=True)

    # --- 1. 基礎技術指標 ---
    roc_5 = df['Close'].pct_change(5).iloc[-1] * 100
    roc_20 = df['Close'].pct_change(20).iloc[-1] * 100
    roc_60 = df['Close'].pct_change(60).iloc[-1] * 100

    vol_ratio = df['Volume'].iloc[-1] / df['Volume'].tail(5).mean() if df['Volume'].tail(5).mean() > 0 else 0

    # [New] 布林通道寬度
    ma20 = df['Close'].rolling(20).mean()
    std20 = df['Close'].rolling(20).std()

    bb_width_series = pd.Series(0.0, index=df.index)
    mask = ma20 > 0
    bb_width_series[mask] = (4 * std20[mask]) / ma20[mask] * 100
    current_bb_width = bb_width_series.iloc[-1]

    # 盤整判定
    s_consol_5 = int(bb_width_series.rolling(5).max().iloc[-1] < 10)
    s_consol_10 = int(bb_width_series.rolling(10).max().iloc[-1] < 12)
    s_consol_20 = int(bb_width_series.rolling(20).max().iloc[-1] < 15)
    s_consol_60 = int(bb_width_series.rolling(60).max().iloc[-1] < 18)

    # --- 2. 舊版假跌破 (Fake Breakdown) ---
    # 定義: 昨日收盤 < 月線 AND 今日收盤 > 月線 AND 今日收紅
    s_fake_breakdown = 0
    try:
        if (df['Close'].iloc[-2] < ma20.iloc[-2] and
                df['Close'].iloc[-1] > ma20.iloc[-1] and
                df['Close'].iloc[-1] > df['Open'].iloc[-1]):
            s_fake_breakdown = 1
    except:
        pass

    # --- 3. ILSS 主力掃單策略 (進階版) ---
    s_ilss_sweep = 0
    try:
        ma200 = df['Close'].rolling(200).mean()
        high_60 = df['High'].rolling(60).max()
        is_uptrend = (last_close > ma200.iloc[-1]) and (ma200.iloc[-1] > ma200.iloc[-5])
        had_breakout = (df['High'].tail(15) >= high_60.tail(15)).any()

        if is_uptrend and had_breakout:
            low_20d = df['Low'].rolling(20).min().shift(1)
            for i in range(3):
                idx = -1 - i
                s_level = min(low_20d.iloc[idx], ma20.iloc[idx]) if idx > -len(df) else 0
                if s_level == 0: continue
                break_depth = (s_level - df['Low'].iloc[idx]) / s_level
                is_breakdown = (df['Low'].iloc[idx] < s_level) and (0.005 < break_depth < 0.08)
                is_panic_vol = df['Volume'].iloc[idx] > (1.2 * df['Volume'].iloc[idx - 5:idx].mean())

                if is_breakdown and is_panic_vol:
                    is_reclaimed = (last_close > s_level) and (last_close > df['Open'].iloc[-1])
                    is_engulfing = (last_close > df['High'].iloc[idx])
                    if is_reclaimed and is_engulfing:
                        s_ilss_sweep = 1
                        break
    except:
        pass

    # --- 4. 其他輔助訊號 ---
    def check_recent(series):
        return int(series.tail(3).any())

    s_break_30w = check_recent(TechnicalStrategies.break_30w_ma(df))
    s_uptrend = int(TechnicalStrategies.strong_uptrend(df).iloc[-1])
    s_high_60 = check_recent(TechnicalStrategies.breakout_n_days_high(df, 60))
    s_high_30 = check_recent(TechnicalStrategies.breakout_n_days_high(df, 30))
    s_ma55_sup = check_recent(TechnicalStrategies.near_ma_support(df, 55))
    s_ma200_sup = check_recent(TechnicalStrategies.near_ma_support(df, 200))
    s_vix_rev = check_recent(TechnicalStrategies.vix_reversal(df))

    return {
        '現價': last_close,
        '漲幅5d': round(roc_5, 2),
        '漲幅20d': round(roc_20, 2),
        '漲幅60d': round(roc_60, 2),
        'bb_width': round(current_bb_width, 2),
        '量比': round(vol_ratio, 2),

        # 策略訊號
        'str_consol_5': s_consol_5,
        'str_consol_10': s_consol_10,
        'str_consol_20': s_consol_20,
        'str_consol_60': s_consol_60,
        'str_ilss_sweep': s_ilss_sweep,
        'str_fake_breakdown': s_fake_breakdown,  # [回來了]

        'str_break_30w': s_break_30w,
        'str_uptrend': s_uptrend,
        'str_high_60': s_high_60,
        'str_high_30': s_high_30,
        'str_ma55_sup': s_ma55_sup,
        'str_ma200_sup': s_ma200_sup,
        'str_vix_rev': s_vix_rev
    }


def main():
    print(f"🚀 戰情室因子運算啟動 (V4.0 - 修正版) | {datetime.now():%H:%M:%S}")
    cache = CacheManager()
    raw_path = project_root / 'data' / 'temp' / 'chips_revenue_raw.csv'
    if not raw_path.exists():
        print("❌ 找不到 chips_revenue_raw.csv")
        return

    raw_df = pd.read_csv(raw_path, dtype={'sid': str})

    tech_list = []
    symbols = cache.get_all_symbols(market='tw')
    total = len(symbols)

    for i, symbol in enumerate(symbols):
        if i % 200 == 0: print(f"   進度: {i}/{total}...", end='\r')
        df = cache.load(symbol)
        factors = calculate_advanced_factors(df)
        if factors:
            factors['sid'] = symbol.split('.')[0]
            tech_list.append(factors)

    print(f"✅ 計算完成，共 {len(tech_list)} 檔。")

    tech_df = pd.DataFrame(tech_list).set_index('sid')
    final_df = raw_df.merge(tech_df, on='sid', how='left')

    if '漲幅20d' in final_df.columns:
        final_df['RS強度'] = final_df['漲幅20d'].rank(pct=True) * 100
        final_df['RS強度'] = final_df['RS強度'].round(1)

    # --- 標籤生成 ---
    def get_strong_tags(row):
        tags = []

        # 1. 盤整
        if row.get('str_consol_60', 0) == 1: tags.append('盤整60日')
        if row.get('str_consol_20', 0) == 1: tags.append('盤整20日')
        if row.get('str_consol_10', 0) == 1: tags.append('盤整10日')
        if row.get('str_consol_5', 0) == 1: tags.append('盤整5日')

        bbw = row.get('bb_width', 100)
        if bbw < 5.0:
            tags.append('極度壓縮')
        elif bbw < 8.0:
            tags.append('波動壓縮')

        # 2. ILSS 與 假跌破 (並存)
        # 頂級訊號: ILSS
        if row.get('str_ilss_sweep', 0) == 1:
            if row.get('rev_cum_yoy', 0) > 0 and (row.get('m_net_today', 0) < 0 or row.get('m_sum_5d', 0) < 0):
                tags.append('主力掃單(ILSS)')

        # 一般訊號: 舊版假跌破 (只要破月線站回就算)
        if row.get('str_fake_breakdown', 0) == 1:
            tags.append('假跌破')

        # 3. 趨勢與型態
        if row.get('RS強度', 0) > 90: tags.append('超強勢')
        if row.get('漲幅60d', 0) > 30: tags.append('波段黑馬')

        if row.get('str_break_30w', 0) == 1: tags.append('突破30週')
        if row.get('str_uptrend', 0) == 1: tags.append('強勢多頭')
        if row.get('str_high_60', 0) == 1: tags.append('創季高')
        if row.get('str_high_30', 0) == 1: tags.append('創月高')

        # 4. 籌碼與支撐
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
        't_sum_5d': '投信買賣超(5日)', 't_streak': '投信連買天數',
        'f_sum_5d': '外資買賣超(5日)', 'f_streak': '外資連買天數',
        'm_sum_5d': '融資增減(5日)', 'm_net_today': '融資增減(今日)',
        'pe': '本益比', 'yield': '殖利率(%)',
        '現價': '今日收盤價', '漲幅20d': '20日漲幅(%)', '漲幅60d': '3個月漲幅(%)',
        'bb_width': '布林寬度(%)',
        '量比': '成交量比', 'RS強度': 'RS強度', '強勢特徵': '強勢特徵標籤'
    }

    output_df = final_df.copy().rename(columns=chinese_map)
    strategy_dir = project_root / 'data' / 'strategy_results'
    strategy_dir.mkdir(parents=True, exist_ok=True)
    final_df.to_parquet(strategy_dir / 'factor_snapshot.parquet')
    output_df.to_csv(strategy_dir / '戰情室今日快照_全中文版.csv', encoding='utf-8-sig', index=False)
    print("✅ V4.0 運算完成！")


if __name__ == "__main__":
    main()