import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# 載入 .env 環境變數
load_dotenv()

# 設定專案路徑 - 從當前檔案往上找到專案根目錄
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent

sys.path.insert(0, str(project_root))

from utils.etf.scheduler import ETFScheduler
from modules.scrapers.ezmoney import EZMoneyScraper
from modules.scrapers.fhtrust import FHTrustScraper

# 動態設定基礎目錄
# main.py 位置: 戰情室/modules/scrapers/main.py
# 目標路徑: 戰情室/data/raw
# 路徑推算: main.py -> scrapers -> modules -> 戰情室 -> /data/raw
BASE_DIR = current_file.parent.parent.parent / 'data' / 'raw'

# Proxy 設定 - 從 .env 讀取，家裡不用設定
PROXY_HOST = os.getenv('PROXY_HOST')
PROXY_PORT = os.getenv('PROXY_PORT')
PROXY = f"{PROXY_HOST}:{PROXY_PORT}" if PROXY_HOST and PROXY_PORT else None

SCRAPERS = {
    'ezmoney': {
        'class': EZMoneyScraper,
        'funds': [
            {'code': '49YTW', 'name': '00981A', 'dir': 'ezmoney/00981A'}
        ],
        'schedule_time': '15:30'
    },
    'fhtrust': {
        'class': FHTrustScraper,
        'funds': [
            {'code': 'ETF23', 'name': '00991A', 'dir': 'fhtrust/00991A'}
        ],
        'schedule_time': '15:30'
    }
}

def manual_fetch_all():
    """手動抓取所有投信"""
    print("=" * 60)
    print("手動抓取模式 - 抓取所有投信")
    print("=" * 60)

    for company, config in SCRAPERS.items():
        print(f"\n### {company.upper()} ###")
        scraper_class = config['class']

        for fund in config['funds']:
            print(f"\n處理 {fund['name']} ({fund['code']})...")
            save_dir = BASE_DIR / fund['dir']
            save_dir.mkdir(parents=True, exist_ok=True)

            scraper = scraper_class(
                fund_code=fund['code'],
                save_dir=str(save_dir),
                proxy=PROXY
            )

            scraper.fetch_and_save()


def manual_fetch_specific(company, fund_name=None):
    """手動抓取特定投信或基金"""
    print("=" * 60)
    print(f"手動抓取模式 - {company.upper()}")
    print("=" * 60)

    if company not in SCRAPERS:
        print(f"錯誤：找不到投信 '{company}'")
        print(f"可用的投信: {', '.join(SCRAPERS.keys())}")
        return

    config = SCRAPERS[company]
    scraper_class = config['class']

    funds_to_process = config['funds']
    if fund_name:
        funds_to_process = [f for f in config['funds'] if f['name'] == fund_name]
        if not funds_to_process:
            print(f"錯誤：找不到基金 '{fund_name}'")
            return

    for fund in funds_to_process:
        print(f"\n處理 {fund['name']} ({fund['code']})...")
        save_dir = BASE_DIR / fund['dir']
        save_dir.mkdir(parents=True, exist_ok=True)

        scraper = scraper_class(
            fund_code=fund['code'],
            save_dir=str(save_dir),
            proxy=PROXY
        )

        scraper.fetch_and_save()


def run_scheduler():
    """啟動排程器"""
    # 確保資料目錄存在
    BASE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\n📁 資料儲存路徑: {BASE_DIR}")
    print(f"🌐 Proxy 設定: {PROXY if PROXY else '未設定 (直連)'}\n")

    # 初始化排程器
    scheduler = ETFScheduler(
        scrapers_config=SCRAPERS,
        base_dir=str(BASE_DIR),
        proxy=PROXY,
        retry_interval=30,
        max_retry_until="23:59",
        startup_check_days=7  # 檢查範圍：過去7天，但只下載缺失的日期
    )
    scheduler.run()


def print_help():
    """顯示使用說明"""
    print("統一 ETF 爬蟲系統 - 使用說明")
    print("=" * 60)
    print("基本用法:")
    print("  python main.py                    # 啟動排程器")
    print("  python main.py --now              # 立即抓取所有投信")
    print("  python main.py --now ezmoney      # 立即抓取 EZMoney")
    print("  python main.py --now fhtrust      # 立即抓取復華投信")
    print("  python main.py --help             # 顯示此說明")
    print(f"\n📁 資料儲存位置: {BASE_DIR}")
    print("\n可用的投信:")
    for company, config in SCRAPERS.items():
        print(f"  - {company}")
        for fund in config['funds']:
            print(f"    * {fund['name']} ({fund['code']})")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cmd = sys.argv[1]

        if cmd in ['--now', '-n']:
            if len(sys.argv) > 2:
                company = sys.argv[2]
                fund_name = sys.argv[3] if len(sys.argv) > 3 else None
                manual_fetch_specific(company, fund_name)
            else:
                manual_fetch_all()

        elif cmd in ['--help', '-h']:
            print_help()

        else:
            print(f"未知參數: {cmd}")
            print_help()
    else:
        run_scheduler()