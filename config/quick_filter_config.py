"""
快速選股條件設定檔
所有選股條件在這裡集中管理
"""
from pathlib import Path
from datetime import datetime

# 資料根目錄
DATA_ROOT = Path("utils/data/goodinfo")

# 選股條件設定
FILTER_CONDITIONS = {
    "突破30日新高": {
        "label": "突破30日新高",
        "data_dir": DATA_ROOT / "30high",
        "file_pattern": "*_突破30日新高.csv",
        "frequency": "daily",  # daily, weekly, monthly
        "color": "#FF6B6B",
        "description": "股價創30日新高"
    },
    "大戶持股增加": {
        "label": "大戶持股增加",
        "data_dir": DATA_ROOT / "holder_change",
        "file_pattern": "*_大戶持股週增減.csv",
        "frequency": "weekly",
        "color": "#4ECDC4",
        "description": ">1000張大戶增持"
    },
    "月營收創新高": {
        "label": "月營收創新高",
        "data_dir": DATA_ROOT / "revenue_high",
        "file_pattern": "*_月營收創新高.csv",
        "frequency": "monthly",
        "color": "#95E1D3",
        "description": "單月營收歷月新高"
    }
    # 🔧 未來擴充只需在這裡新增即可
}


def get_latest_file(data_dir: str, file_pattern: str) -> Path:
    """
    取得最新的檔案（根據檔名中的日期排序）
    ✅ 修正：確保正確排序
    """
    from pathlib import Path
    import re

    data_path = Path(data_dir)
    if not data_path.exists():
        print(f"⚠️ 目錄不存在: {data_path}")
        return None

    # 取得所有符合 pattern 的檔案
    files = list(data_path.glob(file_pattern))

    if not files:
        print(f"⚠️ 找不到符合 {file_pattern} 的檔案")
        return None

    # ✅ 提取檔名中的日期並排序（假設格式為 YYYYMMDD 或 YYMMDD）
    def extract_date_from_filename(filepath):
        filename = filepath.stem  # 不含副檔名
        # 嘗試找出 6 或 8 位數字（日期）
        matches = re.findall(r'\d{6,8}', filename)
        if matches:
            # 取最後一個數字（通常是日期）
            date_str = matches[-1]
            return int(date_str)  # 轉成整數方便排序
        return 0

    # ✅ 修正：直接賦值給新變數
    sorted_files = sorted(files, key=extract_date_from_filename, reverse=True)

    latest_file = sorted_files[0]
    print(f"✅ 載入最新檔案: {latest_file.name}")

    return latest_file