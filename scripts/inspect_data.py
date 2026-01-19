# scripts/inspect_data.py
import pandas as pd
from pathlib import Path
import sys

# 設定路徑
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "tw"


def main():
    # 找一個存在的檔案來檢查 (例如 1101)
    target_file = list(CACHE_DIR.glob("*.parquet"))[0]

    print(f"🔍 檢查檔案: {target_file.name}")

    try:
        df = pd.read_parquet(target_file)

        print("\n📊 資料結構:")
        print(f"  - 資料筆數 (Rows): {len(df)}")
        print(f"  - 欄位名稱 (Columns): {df.columns.tolist()}")

        print("\n👀 前 3 筆資料預覽:")
        print(df.head(3).to_string())

        # 診斷
        print("\n👨‍⚕️ 診斷報告:")
        if 'Close' in df.columns:
            print("  ✅ 有 'Close' 欄位")
        elif 'close' in df.columns:
            print("  ⚠️ 發現 'close' (小寫)，但策略程式可能在找大寫！")
        else:
            print("  ❌ 找不到收盤價欄位！")

        if len(df) < 150:
            print("  ⚠️ 資料筆數不足 150 筆，無法計算 30 週均線。")
        else:
            print("  ✅ 資料長度足夠。")

    except Exception as e:
        print(f"❌ 讀取失敗: {e}")


if __name__ == "__main__":
    main()