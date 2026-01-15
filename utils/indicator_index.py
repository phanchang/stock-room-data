# utils/indicator_index.py

import pandas as pd
from pathlib import Path
import json
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INDICATOR_PATH = PROJECT_ROOT / "data" / "indicators" / "tw"
INDEX_PATH = PROJECT_ROOT / "data" / "indicators" / "index.json"


def build_indicator_index():
    """
    建立 indicator 索引檔
    格式: {
        "daily_break_30w": {
            "1101": ["2024-11-29", "2024-12-02"],
            "2330": ["2024-12-10"],
            ...
        }
    }
    """
    if not INDICATOR_PATH.exists():
        print("⚠️ Indicator 目錄不存在")
        return

    index = {}

    parquet_files = list(INDICATOR_PATH.glob("*.parquet"))
    print(f"📊 掃描 {len(parquet_files)} 個 indicator 檔案...")

    for file in parquet_files:
        # ✅ 修正：使用 split 而非 replace
        parts = file.stem.split('_')  # "8182_TWO" → ["8182", "TWO"]
        stock_id = parts[0] if parts else file.stem

        try:
            df = pd.read_parquet(file)

            # 取得所有 indicator 欄位 (排除 date)
            indicator_cols = [col for col in df.columns if col != 'date']

            for indicator_name in indicator_cols:
                if indicator_name not in index:
                    index[indicator_name] = {}

                # 儲存有事件的日期
                events = df[df[indicator_name] == True]
                if not events.empty:
                    dates = events['date'].dt.strftime('%Y-%m-%d').tolist()
                    index[indicator_name][stock_id] = dates

        except Exception as e:
            print(f"⚠️ {file.name} 處理失敗: {e}")

    # 寫入索引檔
    index_data = {
        "updated_at": datetime.now().isoformat(),
        "total_stocks": len(parquet_files),
        "indicators": index
    }

    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(INDEX_PATH, 'w', encoding='utf-8') as f:
        json.dump(index_data, f, ensure_ascii=False, indent=2)

    print(f"✅ 索引檔已建立: {INDEX_PATH}")

    # 統計
    for indicator_name, stocks in index.items():
        print(f"  {indicator_name}: {len(stocks)} 檔股票")


def load_indicator_index() -> dict:
    """載入 indicator 索引"""
    if not INDEX_PATH.exists():
        print("⚠️ 索引檔不存在,建議執行 build_indicator_index()")
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