import pandas as pd
import numpy as np
from pathlib import Path
import sys
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

# --- 路徑校準 ---
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    from utils.cache.manager import CacheManager
except ImportError:
    print("❌ 找不到 utils.cache.manager，請檢查資料夾路徑。")
    sys.exit(1)


def calculate_advanced_factors(df):
    if df is None or len(df) < 20: return None
    last_close = df['close'].iloc[-1]
    roc_20 = df['close'].pct_change(20).iloc[-1] * 100
    vcp_index = ((df['high'].tail(20) - df['low'].tail(20)) / df['close'].tail(20)).mean() * 100
    vol_today = df['volume'].iloc[-1]
    vol_ma5 = df['volume'].tail(5).mean()
    vol_ratio = vol_today / vol_ma5 if vol_ma5 > 0 else 0
    ma20 = df['close'].tail(20).mean()
    above_ma20 = 1 if last_close > ma20 else 0

    return {
        '現價': last_close,
        '漲幅20d': round(roc_20, 2),
        'VCP壓縮': round(vcp_index, 2),
        '量比': round(vol_ratio, 2),
        '站上月線': above_ma20
    }


def main():
    print(f"🚀 戰情室因子運算啟動 | {datetime.now():%H:%M:%S}")
    cache = CacheManager()

    # 1. 讀取籌碼原料
    raw_path = project_root / 'data' / 'temp' / 'chips_revenue_raw.csv'
    raw_df = pd.read_csv(raw_path, dtype={'sid': str})
    raw_df['sid'] = raw_df['sid'].str.strip()

    # 2. 讀取股票清單 (智慧欄位)
    sl = pd.read_csv(project_root / 'data' / 'stock_list.csv', dtype={'stock_id': str})
    sl_clean = sl.copy()
    sl_clean.columns = [c.lower() for c in sl_clean.columns]

    # 3. 計算技術因子
    tech_list = []
    symbols = cache.get_all_symbols(market='tw')
    for symbol in symbols:
        df = cache.load(symbol)
        factors = calculate_advanced_factors(df)
        if factors:
            factors['sid'] = symbol.split('.')[0]
            tech_list.append(factors)
    tech_df = pd.DataFrame(tech_list).set_index('sid')

    # 4. 數據整合
    final_df = raw_df.merge(tech_df, on='sid', how='left')

    # 5. 計算 RS 排名
    if '漲幅20d' in final_df.columns:
        final_df['RS強度'] = final_df['漲幅20d'].rank(pct=True) * 100
        final_df['RS強度'] = final_df['RS強度'].round(1)

    # 6. 生成標籤 (移除 Emoji)
    def get_strong_tags(row):
        tags = []
        if row.get('RS強度', 0) > 90: tags.append('超強勢')
        if row.get('VCP壓縮', 10) < 3.0: tags.append('波動壓縮')
        if row.get('t_streak', 0) >= 3: tags.append('投信認養')
        if row.get('m_net_today', 0) < -200: tags.append('散戶退場')
        return ','.join(tags)

    final_df['強勢特徵'] = final_df.apply(get_strong_tags, axis=1)

    # 7. 產出可讀版 CSV (100% 中文欄位對照)
    # 將所有英文 Key 映射為中文
    chinese_map = {
        'sid': '股票代號', 'name': '股票名稱', 'industry': '產業別', 'rev_ym': '營收年月',
        'rev_yoy': '營收年增率(%)', 'rev_now': '當月營收', 't_net_today': '投信買賣超(今日)',
        't_sum_5d': '投信買賣超(5日)', 't_sum_20d': '投信買賣超(20日)', 'f_net_today': '外資買賣超(今日)',
        'f_sum_5d': '外資買賣超(5日)', 'f_sum_20d': '外資買賣超(20日)', 't_streak': '投信連買天數',
        'f_streak': '外資連買天數', 'm_net_today': '融資增減(張)', 's_net_today': '融券增減(張)',
        'pe': '本益比', 'yield': '殖利率(%)', 'pbr': '股價淨值比', '現價': '今日收盤價',
        '漲幅20d': '20日漲幅(%)', 'VCP壓縮': 'VCP波動壓縮', '量比': '成交量比',
        '站上月線': '站上月線', 'RS強度': 'RS強度排名', '強勢特徵': '強勢特徵標籤'
    }

    output_df = final_df.copy().rename(columns=chinese_map)

    # 存檔
    strategy_dir = project_root / 'data' / 'strategy_results'
    strategy_dir.mkdir(parents=True, exist_ok=True)

    # 產出 Parquet (後台運算用)
    final_df.to_parquet(strategy_dir / 'factor_snapshot.parquet')

    # 產出全中文 CSV (帥爸對帳用)
    readable_csv = strategy_dir / '戰情室今日快照_全中文版.csv'
    output_df.to_csv(readable_csv, encoding='utf-8-sig', index=False)

    print("-" * 60)
    print(f"✅ V2.8 運算完成！")
    print(f"📄 產出檔案：{readable_csv.name}")
    print(f"🚀 2330/8299 最終校對：")
    check = final_df[final_df['sid'].isin(['2330', '8299'])]
    print(check[['sid', 'name', 'm_net_today', 's_net_today', '強勢特徵']].to_string(index=False))
    print("-" * 60)


if __name__ == "__main__":
    main()