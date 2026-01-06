# main.py
import sys
import os
from modules.scrapers.ezmoney import EZMoneyScraper
from modules.scrapers.fhtrust import FHTrustScraper
from pathlib import Path
# 將專案根目錄加入 Python 路徑
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
from utils.etf.scheduler import ETFScheduler

BASE_DIR = r'C:\Users\andychang\Desktop\每日選股\戰情室\data\raw'
PROXY = '10.160.3.88:8080'

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
    print("=" * 60)
    print("手動抓取模式 - 抓取所有投信")
    print("=" * 60)

    for company, config in SCRAPERS.items():
        print(f"\n### {company.upper()} ###")
        scraper_class = config['class']

        for fund in config['funds']:
            print(f"\n處理 {fund['name']} ({fund['code']})...")
            save_dir = os.path.join(BASE_DIR, fund['dir'])

            scraper = scraper_class(
                fund_code=fund['code'],
                save_dir=save_dir,
                proxy=PROXY
            )

            scraper.fetch_and_save()


def manual_fetch_specific(company, fund_name=None):
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
        save_dir = os.path.join(BASE_DIR, fund['dir'])

        scraper = scraper_class(
            fund_code=fund['code'],
            save_dir=save_dir,
            proxy=PROXY
        )

        scraper.fetch_and_save()


def run_scheduler():
    scheduler = ETFScheduler(SCRAPERS, BASE_DIR, PROXY)
    scheduler.run()


def print_help():
    print("統一 ETF 爬蟲系統 - 使用說明")
    print("=" * 60)
    print("基本用法:")
    print("  python main.py                    # 啟動排程器")
    print("  python main.py --now              # 立即抓取所有投信")
    print("  python main.py --now ezmoney      # 立即抓取 EZMoney")
    print("  python main.py --now fhtrust      # 立即抓取復華投信")
    print("  python main.py --help             # 顯示此說明")
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