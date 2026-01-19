import sys
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv  # 新增這行

# 1. 讀取 .env 檔案（如果存在的話）
load_dotenv()

# 1. 設定專案路徑，確保能 import 你的 scrapers 和 parsers
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]  # 往上推三層回到 戰情室/
sys.path.insert(0, str(project_root))

from utils.etf.modules.scrapers.ezmoney import EZMoneyScraper
from utils.etf.modules.scrapers.fhtrust import FHTrustScraper
from utils.etf.modules.parsers.ezmoney_parser import EZMoneyParser
from utils.etf.modules.parsers.fhtrust_parser import FHTrustParser

# 設定路徑常數
CLEAN_DIR = project_root / 'data' / 'clean'
RAW_DIR = project_root / 'data' / 'raw'


def is_fund_updated(company, etf_code):
    """檢查特定的基金 CSV 是否已經包含今天的日期"""
    file_path = CLEAN_DIR / company / f"{etf_code}.csv"
    if not file_path.exists():
        return False

    try:
        # 讀取最後幾行，加快檢查速度
        df = pd.read_csv(file_path).tail(1)
        if df.empty:
            return False

        last_date = str(df['date'].iloc[0])  # 格式應該是 2025-01-16
        today_date = datetime.now().strftime('%Y-%m-%d')

        return last_date == today_date
    except Exception as e:
        print(f"檢查 {company} 時發生錯誤: {e}")
        return False

def count_files(directory):
    """計算目錄下 excel 檔案的數量"""
    path = Path(directory)
    if not path.exists():
        return 0
    return len(list(path.glob("*.xls*")))


def main():
    print(f"=== 基金同步任務啟動: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

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
        }
    ]

    any_new_data = False

    for t in targets:
        print(f"\n--- 檢查 {t['company']} ({t['name']}) ---")

        # 1. 檢查是否已經完全更新完成 (Clean CSV 已經到今天)
        if is_fund_updated(t['company'], t['name']):
            print(f"✅ 今日資料已存在於 Clean CSV，跳過抓取。")
            continue

        save_dir = RAW_DIR / t['company'] / t['name']
        save_dir.mkdir(parents=True, exist_ok=True)

        # --- 改良點：記錄執行前的檔案數量 ---
        files_before = count_files(save_dir)

        scraper = t['scraper_cls'](fund_code=t['code'], save_dir=str(save_dir))
        print(f"正在嘗試抓取資料...")
        scraper.fetch_and_save()  # 執行抓取

        # --- 改良點：記錄執行後的檔案數量 ---
        files_after = count_files(save_dir)

        # 2. 如果檔案數量變多了，代表有補到舊資料或抓到新資料，就執行 Parser
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