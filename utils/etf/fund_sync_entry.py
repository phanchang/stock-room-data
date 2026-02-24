import sys
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv

# 1. 讀取 .env 檔案（如果存在的話）
load_dotenv()

# 設定台灣時區 (避免 GitHub Actions UTC 時間造成日期錯亂)
tw_tz = timezone(timedelta(hours=8))

# 1. 設定專案路徑，確保能 import 你的 scrapers 和 parsers
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]  # 往上推三層回到 戰情室/
sys.path.insert(0, str(project_root))

from utils.etf.modules.scrapers.ezmoney import EZMoneyScraper
from utils.etf.modules.scrapers.fhtrust import FHTrustScraper
from utils.etf.modules.scrapers.capitalfund import CapitalFundScraper  # ✨ 新增群益爬蟲
from utils.etf.modules.parsers.ezmoney_parser import EZMoneyParser
from utils.etf.modules.parsers.fhtrust_parser import FHTrustParser
from utils.etf.modules.parsers.capitalfund_parser import CapitalFundParser  # ✨ 新增群益解析器

# 設定路徑常數
CLEAN_DIR = project_root / 'data' / 'clean'
RAW_DIR = project_root / 'data' / 'raw'

def is_fund_updated(company, etf_code):
    """檢查特定的基金 CSV 是否已經包含今天的日期"""
    file_path = CLEAN_DIR / company / f"{etf_code}.csv"
    if not file_path.exists():
        return False

    try:
        df = pd.read_csv(file_path).tail(1)
        if df.empty:
            return False

        last_date = str(df['date'].iloc[0])
        today_date = datetime.now(tw_tz).strftime('%Y-%m-%d') # ✨ 強制使用台灣時間

        return last_date == today_date
    except Exception as e:
        print(f"檢查 {company} 時發生錯誤: {e}")
        return False

def count_files(directory):
    path = Path(directory)
    if not path.exists(): return 0
    return len(list(path.glob("*.xls*"))) + len(list(path.glob("*.json")))

def main():
    print(f"=== 基金同步任務啟動: {datetime.now(tw_tz).strftime('%Y-%m-%d %H:%M:%S')} (TW Time) ===")

    targets = [
        {
            'company': 'ezmoney',
            'code': '49YTW',
            'name': '00981A',
            'scraper_cls': EZMoneyScraper,
            'parser_cls': EZMoneyParser
        },
        {
            'company': 'fhtrust',
            'code': 'ETF23',
            'name': '00991A',
            'scraper_cls': FHTrustScraper,
            'parser_cls': FHTrustParser
        },
        # ✨ 將群益投信加入排程清單中
        {
            'company': 'capitalfund',
            'code': '399',
            'name': '00982A',
            'scraper_cls': CapitalFundScraper,
            'parser_cls': CapitalFundParser
        }
    ]

    any_new_data = False

    for t in targets:
        print(f"\n--- 檢查 {t['company']} ({t['name']}) ---")

        if is_fund_updated(t['company'], t['name']):
            print(f"✅ 今日資料已存在於 Clean CSV，跳過抓取。")
            continue

        save_dir = RAW_DIR / t['company'] / t['name']
        save_dir.mkdir(parents=True, exist_ok=True)

        files_before = count_files(save_dir)

        scraper = t['scraper_cls'](fund_code=t['code'], save_dir=str(save_dir))
        print(f"正在嘗試抓取資料...")
        scraper.fetch_and_save()

        files_after = count_files(save_dir)

        if files_after > files_before:
            print(f"✨ 偵測到新檔案 ({files_before} -> {files_after})！開始解析...")
            parser = t['parser_cls'](raw_dir=str(save_dir), clean_dir=str(CLEAN_DIR / t['company']))
            parser.parse_all_files()
            any_new_data = True
        else:
            print(f"ℹ️ 沒有新增檔案，不需要重新解析。")

    if not any_new_data:
        print("\n[結果] 本次任務無新檔案產生。")
    else:
        print("\n[結果] 偵測到新檔案，已完成解析並更新 CSV。")

if __name__ == "__main__":
    main()