# scripts/check_files.py
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "tw"


def main():
    print(f"📂 檢查目錄: {CACHE_DIR}")

    if not CACHE_DIR.exists():
        print("❌ 目錄不存在！請先執行 init_cache_tw.py 下載資料。")
        return

    files = list(CACHE_DIR.glob("*.parquet"))
    print(f"📊 檔案總數: {len(files)}")

    if len(files) == 0:
        print("❌ 目錄是空的！請等待 init_cache_tw.py 下載完成。")
        return

    print("\n👀 前 5 個檔案名稱範例:")
    for f in files[:5]:
        print(f"  - {f.name}")

    print("\n💡 診斷結論:")
    sample = files[0].name
    if "_" in sample:
        print("✅ 格式為底線 (1101_TW.parquet) -> 策略程式應該讀得到。")
    elif ".TW" in sample or ".TWO" in sample:
        print("⚠️ 格式為點號 (1101.TW.parquet) -> 策略程式找不到檔案！需要修改。")
    else:
        print("❓ 未知格式，請回報檔名。")


if __name__ == "__main__":
    main()