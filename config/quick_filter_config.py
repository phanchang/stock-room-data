# config/quick_filter_config.py

"""
快速選股條件設定檔
所有選股條件在這裡集中管理
"""
from pathlib import Path
import re

# 資料根目錄
DATA_ROOT = Path("utils/data/goodinfo")

# 選股條件設定
FILTER_CONDITIONS = {
    "突破30日新高": {
        "label": "突破30日新高",
        "data_dir": DATA_ROOT / "30high",
        "file_pattern": "*_突破30日新高.csv",
        "frequency": "daily",
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
    },
    "日級爆量突破30W": {
        "label": "日級爆量突破30W",
        "type": "indicator",
        "indicator": "daily_break_30w",
        "frequency": "daily",
        "color": "#E74C3C",
        "description": "當天爆量突破 30 週均線",
        "params": {
            "days": {"label": "近N日內", "type": "number", "default": 5, "min": 1, "max": 60}
        }
    },

    # =========== 盤整策略 (隱藏天數輸入框) ===========
    "極短線整理": {
        "label": "極短線 (5日)",
        "type": "indicator",
        "indicator": "consol_5",
        "frequency": "daily",
        "color": "#D7BDE2",
        "description": "5日極致壓縮 (<8%)",
        "hide_days": True,  # 隱藏輸入框
        "params": {}
    },
    "中期整理": {
        "label": "中期 (10日)",
        "type": "indicator",
        "indicator": "consol_10",
        "frequency": "daily",
        "color": "#AF7AC5",
        "description": "10日短波段整理 (<12%)",
        "hide_days": True,
        "params": {}
    },
    "中長期整理": {
        "label": "中長期 (20日)",
        "type": "indicator",
        "indicator": "consol_20",
        "frequency": "daily",
        "color": "#884EA0",
        "description": "月線級別整理 (<15%)",
        "hide_days": True,
        "params": {}
    },
    "長期整理": {
        "label": "長期 (60日)",
        "type": "indicator",
        "indicator": "consol_60",
        "frequency": "daily",
        "color": "#5B2C6F",
        "description": "季線大底 (<25%)",
        "hide_days": True,
        "params": {}
    }
}


def get_latest_file(data_dir: str, file_pattern: str) -> Path:
    """
    取得最新的檔案（根據檔名中的日期排序）
    """
    from pathlib import Path

    # 確保 data_dir 是 Path 物件
    if isinstance(data_dir, str):
        data_path = Path(data_dir)
    else:
        data_path = data_dir

    if not data_path.exists():
        # 靜默失敗或印出警告，視需求而定
        return None

    files = list(data_path.glob(file_pattern))
    if not files:
        return None

    def extract_date_from_filename(filepath):
        filename = filepath.stem
        matches = re.findall(r'\d{6,8}', filename)
        if matches:
            return int(matches[-1])
        return 0

    sorted_files = sorted(files, key=extract_date_from_filename, reverse=True)
    return sorted_files[0]