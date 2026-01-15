# scripts/build_consolidation.py

import pandas as pd
from pathlib import Path
import sys

# 設定專案根目錄
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.indicators.consolidation import calc_consolidation
from utils.indicator_writer import write_daily_indicators
from utils.stock_list import get_stock_list


def process_single_stock(stock_id: str, market: str) -> bool:
    stock_suffix = f"{stock_id}_{market}"
    data_path = PROJECT_ROOT / "data" / "cache" / "tw" / f"{stock_suffix}.parquet"

    if not data_path.exists():
        return False

    try:
        df = pd.read_parquet(data_path)
        if df.empty: return False
        df = df.reset_index()
        if "Date" in df.columns: df = df.rename(columns={"Date": "date"})

        # 1. 極短線 (5日, <8%)
        df_5 = calc_consolidation(df, period_days=5, box_threshold=0.08)
        df['consol_5'] = df_5['is_consolidating']

        # 2. 中期 (10日, <12%)
        df_10 = calc_consolidation(df, period_days=10, box_threshold=0.12)
        df['consol_10'] = df_10['is_consolidating']

        # 3. 中長期 (20日, <15%)
        df_20 = calc_consolidation(df, period_days=20, box_threshold=0.15)
        df['consol_20'] = df_20['is_consolidating']

        # 4. 長期 (60日, <25%)
        df_60 = calc_consolidation(df, period_days=60, box_threshold=0.25)
        df['consol_60'] = df_60['is_consolidating']

        target_cols = ['consol_5', 'consol_10', 'consol_20', 'consol_60']

        # 如果完全沒有觸發任何一種，就不寫入
        if not df[target_cols].any().any():
            return False

        write_daily_indicators(
            df=df,
            stock_id=stock_suffix,
            indicator_cols=target_cols,
            sub_folder="consolidation",
            market="tw"
        )
        return True

    except Exception as e:
        print(f"❌ {stock_id}: 處理失敗 - {e}")
        return False


def main():
    print("🚀 開始掃描四種盤整形態 (5/10/20/60日)...")
    stock_list = get_stock_list(include_market=True)

    success_count = 0
    for i, (stock_id, market) in enumerate(stock_list, 1):
        if i % 100 == 0: print(f"Progress: [{i}/{len(stock_list)}]")
        if process_single_stock(stock_id, market):
            success_count += 1

    print(f"\n📊 掃描完成，共 {success_count} 檔股票符合任一條件")

    print("🔧 更新索引...")
    from utils.indicator_index import build_indicator_index
    build_indicator_index()


if __name__ == "__main__":
    main()