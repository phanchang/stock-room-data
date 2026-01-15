# utils/indicator_index.py

import pandas as pd
from pathlib import Path
import json
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# 🆕 修改：指向 indicators 根目錄，不再寫死 tw
INDICATOR_PATH = PROJECT_ROOT / "data" / "indicators"
INDEX_PATH = PROJECT_ROOT / "data" / "indicators" / "index.json"


def build_indicator_index():
    """
    建立 indicator 索引檔
    會遞迴掃描 data/indicators 下所有子目錄的 parquet 檔

    格式: {
        "daily_break_30w": {
            "1101": ["2024-11-29", ...],
            ...
        },
        "is_consolidating": { ... }
    }
    """
    if not INDICATOR_PATH.exists():
        print("⚠️ Indicator 目錄不存在")
        return

    index = {}

    # 🆕 修改：使用 rglob (recursive glob) 掃描所有子目錄下的 parquet
    parquet_files = list(INDICATOR_PATH.rglob("*.parquet"))
    print(f"📊 掃描到 {len(parquet_files)} 個 indicator 檔案 (含子目錄)...")

    for file in parquet_files:
        # 檔名處理: "8182_TWO.parquet" -> "8182"
        # 假設檔名格式仍為 {stock_id}_{market}.parquet
        parts = file.stem.split('_')
        stock_id = parts[0] if parts else file.stem

        try:
            # 讀取檔案
            df = pd.read_parquet(file)

            # 取得所有 indicator 欄位 (排除 date)
            # 這邊很關鍵：它會自動抓取欄位名稱 (如 daily_break_30w 或 is_consolidating) 當作索引 Key
            indicator_cols = [col for col in df.columns if col != 'date']

            for indicator_name in indicator_cols:
                if indicator_name not in index:
                    index[indicator_name] = {}

                # 儲存有事件 (True) 的日期
                # 有些欄位可能是 0/1 或 True/False，這裡統一轉布林判斷
                events = df[df[indicator_name].astype(bool)]

                if not events.empty:
                    # 轉換日期格式
                    dates = events['date'].dt.strftime('%Y-%m-%d').tolist()

                    # 如果該股票已經有資料 (可能來自不同策略資料夾但同名指標? 雖然機率低)，合併之
                    if stock_id in index[indicator_name]:
                        existing_dates = set(index[indicator_name][stock_id])
                        existing_dates.update(dates)
                        index[indicator_name][stock_id] = sorted(list(existing_dates))
                    else:
                        index[indicator_name][stock_id] = dates

        except Exception as e:
            print(f"⚠️ {file.name} 處理失敗: {e}")

    # 寫入索引檔
    index_data = {
        "updated_at": datetime.now().isoformat(),
        "total_files_scanned": len(parquet_files),
        "indicators": index
    }

    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(INDEX_PATH, 'w', encoding='utf-8') as f:
        json.dump(index_data, f, ensure_ascii=False, indent=2)

    print(f"✅ 索引檔已建立: {INDEX_PATH}")

    # 統計
    for indicator_name, stocks in index.items():
        print(f"  📌 {indicator_name}: 涵蓋 {len(stocks)} 檔股票")


def load_indicator_index() -> dict:
    """載入 indicator 索引"""
    if not INDEX_PATH.exists():
        print("⚠️ 索引檔不存在, 建議執行 build_indicator_index()")
        return {}

    try:
        with open(INDEX_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get('indicators', {})
    except Exception as e:
        print(f"❌ 載入索引檔失敗: {e}")
        return {}


if __name__ == "__main__":
    build_indicator_index()