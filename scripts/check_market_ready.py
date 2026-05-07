# 檔案路徑: scripts/check_market_ready.py
import sys
import os
import requests
from pathlib import Path
from dotenv import load_dotenv

# 取得專案根目錄並設定路徑
current_file = Path(__file__).resolve()
PROJECT_ROOT = current_file.parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 🔥 載入專案根目錄下的 .env 檔案
load_dotenv(PROJECT_ROOT / '.env')


def get_twse_latest_date(endpoint):
    """
    向證交所發送不帶日期的請求，證交所會自動回傳「最新可用」的資料與日期。
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    try:
        url = f"https://www.twse.com.tw/rwd/zh/{endpoint}?selectType=ALL&response=json"
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            data = response.json()
            if data.get("stat") == "OK":
                # 證交所回傳的日期格式為 YYYYMMDD (例如: "20240517")
                raw_date = data.get("date", "")
                if raw_date and len(raw_date) == 8:
                    # 轉換為 YYYY-MM-DD 格式以配合 UI 判定邏輯
                    return f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
    except Exception as e:
        pass

    return "無資料"


def main():
    try:
        # 1. 探測三大法人買賣超 (T86) - 通常 15:00 ~ 15:30 更新
        inst_date = get_twse_latest_date("fund/T86")

        # 2. 探測信用資券餘額 (MI_MARGN) - 通常 21:00 ~ 21:30 更新
        margin_date = get_twse_latest_date("marginTrading/MI_MARGN")

        # 輸出給 UI 讀取的特殊標記格式
        print(f"PROBE_INST_DATE:{inst_date}")
        print(f"PROBE_MARGIN_DATE:{margin_date}")

    except Exception as e:
        print(f"PROBE_ERROR:{e}")


if __name__ == "__main__":
    main()