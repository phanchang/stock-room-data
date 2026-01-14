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

    Args:
        stock_id: 股票代號 (例如 "1101")
        market: 市場別 ("TW" 或 "TWO")

    Returns:
        bool: 是否成功處理
    """
    # 🆕 根據市場別決定檔名
    stock_suffix = f"{stock_id}_{market}"
    data_path = PROJECT_ROOT / "data" / "cache" / "tw" / f"{stock_suffix}.parquet"

    if not data_path.exists():
        print(f"⚠️ {stock_id}: 找不到快取檔案 ({stock_suffix}.parquet)")
        return False

    try:
        # 讀取資料
        df = pd.read_parquet(data_path)

        if df.empty:
            print(f"⚠️ {stock_id}: 資料為空")
            return False

        # 重設索引
        df = df.reset_index()
        if "Date" in df.columns:
            df = df.rename(columns={"Date": "date"})

        # 計算 indicator
        df = calc_break_30w(df)

        # 檢查是否有觸發事件
        if not df["daily_break_30w"].any():
            print(f"📌 {stock_id}: 無觸發事件")
            return False

        # 寫入 parquet
        write_daily_indicators(
            df=df,
            stock_id=stock_suffix,
            indicator_cols=["daily_break_30w"]
        )

        event_count = df["daily_break_30w"].sum()
        print(f"✅ {stock_id}: 成功處理 ({event_count} 個事件)")
        return True

    except Exception as e:
        print(f"❌ {stock_id}: 處理失敗 - {e}")
        return False


def main():
    """批次處理所有台股"""

    # 🆕 取得股票清單 (包含市場別)
    stock_list = get_stock_list(include_market=True)

    print(f"\n{'=' * 60}")
    print(f"🚀 開始處理 {len(stock_list)} 檔股票")
    print(f"{'=' * 60}\n")

    success_count = 0
    no_event_count = 0
    fail_count = 0

    for i, (stock_id, market) in enumerate(stock_list, 1):
        print(f"[{i}/{len(stock_list)}] {stock_id} ({market}) ", end="")

        result = process_single_stock(stock_id, market)

        if result:
            success_count += 1
        else:
            # 區分是無事件還是失敗
            stock_suffix = f"{stock_id}_{market}"
            data_path = PROJECT_ROOT / "data" / "cache" / "tw" / f"{stock_suffix}.parquet"
            if data_path.exists():
                no_event_count += 1
            else:
                fail_count += 1

    # 統計結果
    print(f"\n{'=' * 60}")
    print(f"📊 處理完成統計")
    print(f"{'=' * 60}")
    print(f"✅ 成功處理 (有事件): {success_count} 檔")
    print(f"📌 無觸發事件: {no_event_count} 檔")
    print(f"❌ 處理失敗 (無快取): {fail_count} 檔")
    print(f"{'=' * 60}\n")

    # 🆕 自動建立索引檔
    print(f"\n{'=' * 60}")
    print(f"🔧 建立索引檔...")
    print(f"{'=' * 60}\n")

    from utils.indicator_index import build_indicator_index
    build_indicator_index()

if __name__ == "__main__":
    main()
