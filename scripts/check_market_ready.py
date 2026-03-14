# 檔案路徑: scripts/check_market_ready.py
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# 取得專案根目錄並設定路徑
current_file = Path(__file__).resolve()
PROJECT_ROOT = current_file.parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 🔥 載入專案根目錄下的 .env 檔案
load_dotenv(PROJECT_ROOT / '.env')

from utils.moneydj_parser import MoneyDJParser


def main():
    try:
        # 用 2330 台積電作為全市場資料公布進度的風向標
        parser = MoneyDJParser("2330")
        data = parser.get_daily_chips(months=1)  # 只抓最少量的資料以求極速

        inst_list = data.get('institutional_investors', [])
        margin_list = data.get('margin_trading', [])

        inst_date = inst_list[0]['date'] if inst_list else "無資料"
        margin_date = margin_list[0]['date'] if margin_list else "無資料"

        # 輸出給 UI 讀取的特殊標記格式
        print(f"PROBE_INST_DATE:{inst_date}")
        print(f"PROBE_MARGIN_DATE:{margin_date}")

    except Exception as e:
        print(f"PROBE_ERROR:{e}")


if __name__ == "__main__":
    main()