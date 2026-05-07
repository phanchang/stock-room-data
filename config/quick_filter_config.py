# config/quick_filter_config.py

from pathlib import Path

# 專案根目錄 (假設此檔案在 config/ 下，往上兩層就是根目錄)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 🟢 修改這裡：指向根目錄下的 data/goodinfo
GOODINFO_ROOT = PROJECT_ROOT / "data" / "goodinfo"

FILTER_CONDITIONS = {
    # === 外部爬蟲類 (Goodinfo) ===
    "突破30日新高": {
        "label": "突破30日新高",
        "type": "crawler",
        "data_dir": GOODINFO_ROOT / "30high",
        "file_pattern": "*_突破30日新高.csv",
        "frequency": "daily",
        "color": "#FF6B6B",
        "description": "股價創30日新高"
    },
    "大戶持股增加": {
        "label": "大戶持股增加",
        "type": "crawler",
        "data_dir": GOODINFO_ROOT / "holder_change",
        "file_pattern": "*_大戶持股週增減_*.csv", # 注意檔名模式可能變更
        "frequency": "weekly",
        "color": "#4ECDC4",
        "description": ">1000張大戶增持"
    },
    "月營收創新高": {
        "label": "月營收創新高",
        "type": "crawler",
        "data_dir": GOODINFO_ROOT / "revenue_high",
        "file_pattern": "*_月營收創新高.csv",
        "frequency": "monthly",
        "color": "#95E1D3",
        "description": "單月營收歷月新高"
    },

    # === 內部運算策略 (Strategies) ===
    # 這些是我們接下來要算的
    "日級爆量突破30W": {
        "label": "爆量突破30週",
        "type": "strategy",
        "strategy_name": "break_30w", # 對應 daily_strategy_runner 的 key
        "color": "#E74C3C",
        "description": "放量突破30週均線",
        "params": {
            "days": {"label": "近N日內", "default": 5}
        }
    },
    "極短線整理": {
        "label": "極短線 (5日)",
        "type": "strategy",
        "strategy_name": "consol_5",
        "color": "#D7BDE2",
        "description": "5日極致壓縮 (<8%)",
        "hide_days": True
    },
    "中期整理": {
        "label": "中期 (10日)",
        "type": "strategy",
        "strategy_name": "consol_10",
        "color": "#AF7AC5",
        "description": "10日短波段整理 (<12%)",
        "hide_days": True
    },
    "強勢多頭排列": {
        "label": "強勢多頭",
        "type": "strategy",
        "strategy_name": "strong_uptrend",
        "color": "#F1C40F",
        "description": "均線多頭排列 (5>10>20>60)",
        "hide_days": True
    }
}