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
    """計算技術因子 (新增多週期漲跌幅 5/10/20/60)"""
    # 至少要有 65 天資料才能算 60日(3個月) 漲跌幅
    if df is None or len(df) < 65: return None

    last_close = df['close'].iloc[-1]

    # [新增] 漲跌幅 5/10/20/60日
    roc_5 = df['close'].pct_change(5).iloc[-1] * 100
    roc_10 = df['close'].pct_change(10).iloc[-1] * 100
    roc_20 = df['close'].pct_change(20).iloc[-1] * 100
    roc_60 = df['close'].pct_change(60).iloc[-1] * 100

    vcp_index = ((df['high'].tail(20) - df['low'].tail(20)) / df['close'].tail(20)).mean() * 100

    vol_today = df['volume'].iloc[-1]
    vol_ma5 = df['volume'].tail(5).mean()
    vol_ratio = vol_today / vol_ma5 if vol_ma5 > 0 else 0

    ma20 = df['close'].tail(20).mean()
    above_ma20 = 1 if last_close > ma20 else 0

    return {
        '現價': last_close,
        '漲幅5d': round(roc_5, 2),
        '漲幅10d': round(roc_10, 2),
        '漲幅20d': round(roc_20, 2),
        '漲幅60d': round(roc_60, 2),
        'VCP壓縮': round(vcp_index, 2),
        '量比': round(vol_ratio, 2),
        '站上月線': above_ma20
    }


def main():
    print(f"🚀 戰情室因子運算啟動 (V3.0) | {datetime.now():%H:%M:%S}")
    cache = CacheManager()

    # 1. 讀取原料 (這是 update_chips_revenue V12.2 產出的檔案)
    raw_path = project_root / 'data' / 'temp' / 'chips_revenue_raw.csv'
    if not raw_path.exists():
        print("❌ 找不到 chips_revenue_raw.csv，請先執行 update_chips_revenue.py")
        return

    raw_df = pd.read_csv(raw_path, dtype={'sid': str})
    raw_df['sid'] = raw_df['sid'].str.strip()

    # 2. 計算技術因子
    tech_list = []
    symbols = cache.get_all_symbols(market='tw')

    print(f"⚙️  正在計算 {len(symbols)} 檔股票之技術指標 (含 ROC 5/10/20/60)...")
    for symbol in symbols:
        df = cache.load(symbol)
        factors = calculate_advanced_factors(df)
        if factors:
            factors['sid'] = symbol.split('.')[0]
            tech_list.append(factors)

    tech_df = pd.DataFrame(tech_list).set_index('sid')

    # 3. 數據整合
    final_df = raw_df.merge(tech_df, on='sid', how='left')

    # 4. 計算 RS 排名 (使用 20日 作為短期強度標準)
    if '漲幅20d' in final_df.columns:
        final_df['RS強度'] = final_df['漲幅20d'].rank(pct=True) * 100
        final_df['RS強度'] = final_df['RS強度'].round(1)

    # 5. 生成標籤
    def get_strong_tags(row):
        tags = []
        if row.get('RS強度', 0) > 90: tags.append('超強勢')
        if row.get('VCP壓縮', 10) < 3.0: tags.append('波動壓縮')
        if row.get('t_streak', 0) >= 3: tags.append('投信認養')
        # 如果融資大減 (散戶退場)，通常視為利多
        if row.get('m_net_today', 0) < -200: tags.append('散戶退場')
        if row.get('漲幅60d', 0) > 30: tags.append('波段黑馬')
        return ','.join(tags)

    final_df['強勢特徵'] = final_df.apply(get_strong_tags, axis=1)

    # 6. 中文對照 (這是重點：要把 V12.2 的新欄位接進來)
    chinese_map = {
        'sid': '股票代號', 'name': '股票名稱', 'industry': '產業別', 'rev_ym': '營收年月',
        'rev_yoy': '營收年增率(%)', 'rev_now': '當月營收',

        # 籌碼
        't_net_today': '投信買賣超(今日)',
        't_sum_5d': '投信買賣超(5日)', 't_sum_20d': '投信買賣超(20日)',
        'f_net_today': '外資買賣超(今日)',
        'f_sum_5d': '外資買賣超(5日)', 'f_sum_20d': '外資買賣超(20日)',
        't_streak': '投信連買天數', 'f_streak': '外資連買天數',

        # [關鍵新增] 資券 5/10/20日 累計
        'm_net_today': '融資增減(今日)', 'm_sum_5d': '融資增減(5日)',
        'm_sum_10d': '融資增減(10日)', 'm_sum_20d': '融資增減(20日)',
        's_net_today': '融券增減(今日)', 's_sum_5d': '融券增減(5日)',
        's_sum_10d': '融券增減(10日)', 's_sum_20d': '融券增減(20日)',

        # 估值 & 技術 (含新 ROC)
        'pe': '本益比', 'yield': '殖利率(%)', 'pbr': '股價淨值比', '現價': '今日收盤價',
        '漲幅5d': '5日漲幅(%)', '漲幅10d': '10日漲幅(%)',
        '漲幅20d': '20日漲幅(%)', '漲幅60d': '3個月漲幅(%)',
        'VCP壓縮': 'VCP波動壓縮', '量比': '成交量比',
        '站上月線': '站上月線', 'RS強度': 'RS強度排名', '強勢特徵': '強勢特徵標籤'
    }

    output_df = final_df.copy().rename(columns=chinese_map)

    # 存檔
    strategy_dir = project_root / 'data' / 'strategy_results'
    strategy_dir.mkdir(parents=True, exist_ok=True)

    final_df.to_parquet(strategy_dir / 'factor_snapshot.parquet')

    readable_csv = strategy_dir / '戰情室今日快照_全中文版.csv'
    output_df.to_csv(readable_csv, encoding='utf-8-sig', index=False)

    print("-" * 60)
    print(f"✅ V3.0 運算完成！")
    print(f"📄 產出檔案：{readable_csv.name}")

    # 最終驗證 Check
    print(f"🚀 數據完整性檢查 (2330/8299):")
    check = final_df[final_df['sid'].isin(['2330', '8299'])]
    # 顯示最重要的驗證欄位
    cols_to_check = ['sid', 'name', 'm_net_today', 'm_sum_5d', 'm_sum_20d', '漲幅5d', '漲幅60d']
    # 只顯示存在的欄位
    valid_cols = [c for c in cols_to_check if c in check.columns]
    print(check[valid_cols].to_string(index=False))
    print("-" * 60)


if __name__ == "__main__":
    main()