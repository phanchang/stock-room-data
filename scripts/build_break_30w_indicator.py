# scripts/build_break_30w_indicator.py

import pandas as pd
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.indicators.break_30w import calc_break_30w
from utils.indicator_writer import write_daily_indicators
from utils.stock_list import get_stock_list


def process_single_stock(stock_id: str, market: str) -> bool:
    """
    處理單一股票的 indicator 計算
    """
    stock_suffix = f"{stock_id}_{market}"
    data_path = PROJECT_ROOT / "data" / "cache" / "tw" / f"{stock_suffix}.parquet"

    if not data_path.exists():
        # print(f"⚠️ {stock_id}: 找不到快取檔案") # 減少雜訊
        return False

    try:
        # 讀取資料
        df = pd.read_parquet(data_path)

        if df.empty:
            return False

        # 重設索引
        df = df.reset_index()
        if "Date" in df.columns:
            df = df.rename(columns={"Date": "date"})

        # 計算 indicator
        df = calc_break_30w(df)

        # 檢查是否有觸發事件
        target_col = "daily_break_30w"
        if target_col not in df.columns or not df[target_col].any():
            return False

        # 🆕 寫入 parquet (關鍵修改：加入 sub_folder 參數)
        write_daily_indicators(
            df=df,
            stock_id=stock_suffix,
            indicator_cols=[target_col],
            sub_folder="break_30w",  # <--- 指定存入 break_30w 資料夾
            market="tw"
        )

        event_count = df[target_col].sum()
        print(f"✅ {stock_id}: 偵測到突破30週 ({event_count} 次)")
        return True

    except Exception as e:
        print(f"❌ {stock_id}: 處理失敗 - {e}")
        return False


def main():
    """批次處理所有台股 - 突破30週策略"""

    stock_list = get_stock_list(include_market=True)

    print(f"\n{'=' * 60}")
    print(f"🚀 開始掃描「突破30週均線」型態")
    print(f"🎯 目標股票總數: {len(stock_list)} 檔")
    print(f"{'=' * 60}\n")

    success_count = 0
    fail_count = 0

    for i, (stock_id, market) in enumerate(stock_list, 1):
        # 優化顯示：每 100 檔才印一次進度，避免洗版
        if i % 100 == 0:
            print(f"Progress: [{i}/{len(stock_list)}]")

        result = process_single_stock(stock_id, market)

        if result:
            success_count += 1
        else:
            fail_count += 1

    # 統計結果
    print(f"\n{'=' * 60}")
    print(f"📊 掃描完成")
    print(f"{'=' * 60}")
    print(f"✅ 符合突破定義: {success_count} 檔")
    print(f"📌 不符合或資料缺失: {fail_count} 檔")
    print(f"{'=' * 60}\n")

    # 重建索引
    print(f"🔧 更新指標索引 (Indicator Index)...")
    from utils.indicator_index import build_indicator_index
    build_indicator_index()
    print("🎉 完成！")

if __name__ == "__main__":
    main()