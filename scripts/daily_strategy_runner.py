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

    # [新增] 均線策略
    "support_ma_55": lambda df: TechnicalStrategies.near_ma_support(df, 55),
    "support_ma_200": lambda df: TechnicalStrategies.near_ma_support(df, 200),

    # [新增] Vix
    "vix_green": lambda df: TechnicalStrategies.vix_green(df),
}



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

    # --- 1. 環境檢查與準備 ---
    # 確保輸出目錄存在
    INDICATOR_DIR.mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / "data" / "goodinfo").mkdir(parents=True, exist_ok=True)

    # 檢查核心來源：股票清單
    stock_list_path = PROJECT_ROOT / "data" / "stock_list.csv"
    if not stock_list_path.exists():
        print(f"❌ 錯誤：找不到基礎清單 {stock_list_path}，終止程式。")
        return

    # 檢查核心數據：快取資料夾是否有資料
    cache_dir = PROJECT_ROOT / "data" / "cache" / "tw"
    if not cache_dir.exists() or not any(cache_dir.iterdir()):
        print(f"❌ 錯誤：{cache_dir} 無資料，請先執行更新股價腳本。")
        return

    # 彈性檢查：Goodinfo 數據 (僅提示，不中止)
    revenue_path = PROJECT_ROOT / "data" / "goodinfo" / "revenue_high.csv"
    if revenue_path.exists():
        print("✅ 偵測到月營收數據，後續策略將納入參考。")
    else:
        print("⚠️ 提示：缺少月營收數據，將跳過相關複合篩選。")

    # --- 2. 獲取股票清單 ---
    stock_list = get_stock_list(include_market=True)
    print(f"📋 共載入 {len(stock_list)} 檔股票進行分析")

    if not stock_list:
        print("❌ 錯誤：解析後的股票清單為空！")
        return

    # --- 3. 平行運算策略 ---
    total_triggers = 0
    # 在 GitHub Actions 環境下，建議 max_workers 不要太高，2-4 即可
    with ProcessPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(process_single_stock, stock_list))
        total_triggers = sum(results)

    # --- 4. 更新索引 ---
    print("\n🔧 正在重建指標索引 (build_indicator_index)...")
    build_indicator_index()

    print(f"\n✅ 全部完成！")
    print(f"⏱️ 總耗時: {time.time() - start_time:.2f} 秒")
    print(f"🎯 累計觸發: {total_triggers} 次策略訊號")


if __name__ == "__main__":
    # 修正：直接呼叫包含邏輯的 main()
    main()