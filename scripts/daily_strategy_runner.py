# scripts/daily_strategy_runner.py

import os,sys
from pathlib import Path
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import time

# 設定專案根目錄
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.stock_list import get_stock_list
from utils.indicator_writer import write_daily_indicators
from utils.indicator_index import build_indicator_index
from utils.strategies.technical import TechnicalStrategies
# 確保輸出目錄存在
INDICATOR_DIR = PROJECT_ROOT / "data" / "indicators"
INDICATOR_DIR.mkdir(parents=True, exist_ok=True)

# 註冊你的策略
# scripts/daily_strategy_runner.py 的 STRATEGY_MAP 部分

STRATEGY_MAP = {
    "break_30w": lambda df: TechnicalStrategies.break_30w_ma(df),

    # 修改：把震幅改小一點，例如 10日盤整原本 12% 改成 10%
    # 加上 technical.py 新增的「量縮」條件，篩選出來的股票會少很多
    "consol_5": lambda df: TechnicalStrategies.consolidation(df, 5, 0.05),  # 5天內波動 < 5%
    "consol_10": lambda df: TechnicalStrategies.consolidation(df, 10, 0.08),  # 10天內波動 < 8%
    "consol_20": lambda df: TechnicalStrategies.consolidation(df, 20, 0.12),  # 20天內波動 < 12%
    "consol_60": lambda df: TechnicalStrategies.consolidation(df, 60, 0.20),  # 60天內波動 < 20%

    "strong_uptrend": lambda df: TechnicalStrategies.strong_uptrend(df),

    # 🟢 [新增] 創新高策略
    "high_30": lambda df: TechnicalStrategies.breakout_n_days_high(df, 30), # 創月新高
    "high_60": lambda df: TechnicalStrategies.breakout_n_days_high(df, 60), # 創季新高
}


def run_strategies():
    # 1. 檢查核心數據 (yFinance)
    price_path = 'data/indicators/daily_indicators.csv'
    if not os.path.exists(price_path):
        print(f"CRITICAL ERROR: {price_path} not found. Terminating.")
        sys.exit(1)

    df_price = pd.read_csv(price_path)

    # 2. 彈性檢查輔助數據 (Goodinfo)
    revenue_path = 'data/goodinfo/revenue_high.csv'
    if os.path.exists(revenue_path):
        print("Loading Revenue data...")
        df_rev = pd.read_csv(revenue_path)
        # 執行相關策略...
    else:
        print("WARNING: Revenue data missing. Skipping Revenue strategies.")

    # 執行其他不依賴 Revenue 的策略...

def process_single_stock(args):
    """處理單一股票"""
    stock_id, market = args
    stock_suffix = f"{stock_id}_{market}"

    # 建立路徑 (嘗試兩種格式)
    cache_path = PROJECT_ROOT / "data" / "cache" / "tw" / f"{stock_suffix}.parquet"
    if not cache_path.exists():
        cache_path_dot = PROJECT_ROOT / "data" / "cache" / "tw" / f"{stock_id}.{market}.parquet"
        if cache_path_dot.exists():
            cache_path = cache_path_dot
        else:
            return 0

    try:
        # 讀取 Parquet
        df = pd.read_parquet(cache_path)

        if df.empty:
            return 0

        # 1. 重設索引 (將 Date 變成欄位)
        df = df.reset_index()

        # 2. 🟢 [新增] 欄位名稱標準化 (關鍵修正！)
        # 將所有欄位轉為小寫，再針對特定欄位轉大寫開頭
        df.columns = [c.lower() for c in df.columns]

        rename_map = {
            'date': 'date',  # 保持小寫
            'open': 'Open',  # 轉大寫開頭
            'high': 'High',
            'low': 'Low',
            'close': 'Close',  # 策略需要 Close
            'volume': 'Volume',  # 策略需要 Volume
            'adj close': 'Adj Close'
        }
        df = df.rename(columns=rename_map)

        # 3. 檢查必要欄位
        if 'Close' not in df.columns or 'Volume' not in df.columns:
            # print(f"⚠️ {stock_id}: 缺欄位 {df.columns.tolist()}")
            return 0

        triggers = 0

        # 迴圈執行所有策略
        for strategy_name, func in STRATEGY_MAP.items():
            try:
                result_series = func(df)

                # 暫存結果
                col_name = strategy_name
                df[col_name] = result_series

                # 如果有訊號，寫入檔案
                if df[col_name].any():
                    write_daily_indicators(
                        df=df,
                        stock_id=stock_suffix,
                        indicator_cols=[col_name],
                        sub_folder=strategy_name,
                        market="tw"
                    )
                    triggers += 1
            except Exception as e:
                continue

        return triggers

    except Exception as e:
        return 0

def main():
    print("🚀 開始執行策略運算...")
    start_time = time.time()

    # 1. 獲取清單
    stock_list = get_stock_list(include_market=True)
    print(f"📋 共 {len(stock_list)} 檔股票")

    if not stock_list:
        print("❌ 錯誤：股票清單是空的！")
        return

    # 2. 平行運算
    total_triggers = 0
    # 注意：Windows 下如果 process_single_stock 噴錯，有時會看不到
    # 如果這裡還是沒反應，可以試著把 max_workers 改成 1 變成單執行緒除錯
    with ProcessPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(process_single_stock, stock_list))
        total_triggers = sum(results)

    # 3. 更新索引
    print("\n🔧 重建索引...")
    build_indicator_index()

    print(f"\n✅ 完成！耗時: {time.time() - start_time:.2f} 秒")
    print(f"🎯 累計觸發: {total_triggers} 次訊號")


if __name__ == "__main__":
    # 這裡呼叫你定義好的主函數
    run_strategies()