"""
Goodinfo 突破30日新高爬蟲
支援立即抓取和排程執行

使用方式:
1. 命令列:
   python crawler_goodinfo_30high.py --mode fetch         # 立即抓取 (有快取就跳過)
   python crawler_goodinfo_30high.py --mode fetch --force # 強制重新抓取
   python crawler_goodinfo_30high.py --mode schedule --time 14:30  # 排程執行

2. 在程式中使用:
   from utils.crawler_goodinfo_30high import Goodinfo30HighCrawler
   crawler = Goodinfo30HighCrawler()
   df = crawler.fetch_data()  # 自動檢查快取
"""

import sys
from pathlib import Path

# 確保可以 import utils 模組
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pandas as pd
import schedule
import time
from utils.crawler_goodinfo_base import GoodinfoBaseCrawler


class Goodinfo30HighCrawler(GoodinfoBaseCrawler):
    """突破30日新高爬蟲"""

    # 這支爬蟲的專屬設定
    URL = ('https://goodinfo.tw/tw/StockList.asp?MARKET_CAT=%E8%87%AA%E8%A8%82%E7%AF%A9%E9%81%B8'
           '&INDUSTRY_CAT=%E6%88%91%E7%9A%84%E6%A2%9D%E4%BB%B6&FL_ITEM0=%E6%88%90%E4%BA%A4%E5%83%B9'
           '%E5%89%B5%E8%BF%91%E6%9C%9F%E6%96%B0%E9%AB%98%2F%E6%96%B0%E4%BD%8E%E2%80%93%E5%89%B5'
           '%E8%BF%91n%E6%97%A5%E6%96%B0%E9%AB%98&FL_VAL_S0=30&FL_VAL_E0=&FL_ITEM1=%E6%88%90%E4%BA%A4'
           '%E5%BC%B5%E6%95%B8+%28%E5%BC%B5%29&FL_VAL_S1=250&FL_VAL_E1=&FL_ITEM2=&FL_VAL_S2='
           '&FL_VAL_E2=&FL_ITEM3=&FL_VAL_S3=&FL_VAL_E3=&FL_ITEM4=&FL_VAL_S4=&FL_VAL_E4='
           '&FL_ITEM5=&FL_VAL_S5=&FL_VAL_E5=&FL_ITEM6=&FL_VAL_S6=&FL_VAL_E6=&FL_ITEM7='
           '&FL_VAL_S7=&FL_VAL_E7=&FL_ITEM8=&FL_VAL_S8=&FL_VAL_E8=&FL_ITEM9=&FL_VAL_S9='
           '&FL_VAL_E9=&FL_ITEM10=&FL_VAL_S10=&FL_VAL_E10=&FL_ITEM11=&FL_VAL_S11=&FL_VAL_E11='
           '&FL_RULE0=%E7%94%A2%E6%A5%AD%E9%A1%9E%E5%88%A5%7C%7C%40%40ETF%40%40ETF&FL_RULE_CHK0=T'
           '&FL_RULE1=%E7%94%A2%E6%A5%AD%E9%A1%9E%E5%88%A5%7C%7C%E4%B8%8D%E5%8B%95%E7%94%A2%E6%8A%95'
           '%E8%B3%87%E4%BF%A1%E8%A8%97%E8%AD%89%E5%88%B8&FL_RULE_CHK1=T&FL_RULE2=%E7%94%A2%E6%A5%AD'
           '%E9%A1%9E%E5%88%A5%7C%7C%E5%AD%98%E8%A8%97%E6%86%91%E8%AD%89&FL_RULE_CHK2=T&FL_RULE3='
           '%E7%94%A2%E6%A5%AD%E9%A1%9E%E5%88%A5%7C%7C%40%40ETN%40%40ETN&FL_RULE_CHK3=T&FL_RULE4='
           '&FL_RULE5=&FL_RANK0=&FL_RANK1=&FL_RANK2=&FL_RANK3=&FL_RANK4=&FL_RANK5=&FL_FD0=&FL_FD1='
           '&FL_FD2=&FL_FD3=&FL_FD4=&FL_FD5=&FL_SHEET=%E6%BC%B2%E8%B7%8C%E5%8F%8A%E6%88%90%E4%BA%A4'
           '%E7%B5%B1%E8%A8%88&FL_SHEET2=%E4%B8%AD%E6%9C%9F%E7%B4%AF%E8%A8%88%E6%BC%B2%E8%B7%8C%E7%B5%B1'
           '%E8%A8%88&FL_MARKET=%E4%B8%8A%E5%B8%82%2F%E4%B8%8A%E6%AB%83&FL_QRY=%E6%9F%A5++%E8%A9%A2')

    FILENAME_SUFFIX = "突破30日新高"

    # 這支爬蟲要轉換的數值欄位
    NUMERIC_COLUMNS = [
        '成交', '漲跌 價', '漲跌 幅', '成交 張數',
        '今年 累計 漲跌價', '今年 累計 漲跌幅',
        '3個月 累計 漲跌價', '3個月 累計 漲跌幅',
        '半年 累計 漲跌價', '半年 累計 漲跌幅',
        '1年 累計 漲跌價', '1年 累計 漲跌幅',
        '2年 累計 漲跌價', '2年 累計 漲跌幅'
    ]

    def __init__(self):
        super().__init__(data_subdir="30high")

    def fetch_data(self, force: bool = False) -> pd.DataFrame:
        """
        抓取資料

        Args:
            force: 是否強制抓取（忽略本機快取）

        Returns:
            DataFrame
        """
        # 檢查是否已有今日資料
        if not force and self._file_exists_for_today(self.FILENAME_SUFFIX):
            self.logger.info(f"本機已有今日資料，跳過抓取")
            return self._load_today_data(self.FILENAME_SUFFIX)

        self.logger.info(f"開始抓取: {self.FILENAME_SUFFIX}")

        # 使用基礎類別的重試機制抓取
        df = self._fetch_with_retry(self.URL)

        # 轉換數值欄位
        df = self._convert_numeric_columns(df, self.NUMERIC_COLUMNS)

        self.logger.info(f"✓ 成功抓取 {len(df)} 筆資料")

        # 儲存檔案
        filepath = self._generate_filename(df, self.FILENAME_SUFFIX)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        self.logger.info(f"✓ 資料已儲存至: {filepath}")

        return df

    def schedule_daily_fetch(self, time_str: str = "14:30"):
        """
        排程每日抓取

        Args:
            time_str: 執行時間，格式 "HH:MM"
        """
        self.logger.info(f"設定排程: 每日 {time_str} 執行")

        def job():
            self.logger.info(f"排程任務執行中...")
            try:
                self.fetch_data(force=False)
            except Exception as e:
                self.logger.error(f"排程任務執行失敗: {str(e)}")

        schedule.every().day.at(time_str).do(job)

        self.logger.info("排程已啟動，按 Ctrl+C 停止")
        try:
            while True:
                schedule.run_pending()
                time.sleep(15)
        except KeyboardInterrupt:
            self.logger.info("排程已停止")


def main():
    """命令列入口"""
    import argparse

    parser = argparse.ArgumentParser(description='Goodinfo 突破30日新高爬蟲')
    parser.add_argument('--mode', choices=['fetch', 'schedule'], default='fetch',
                        help='執行模式: fetch(立即抓取) 或 schedule(排程執行)')
    parser.add_argument('--force', action='store_true',
                        help='強制抓取，忽略本機快取')
    parser.add_argument('--time', default='14:30',
                        help='排程時間 (格式: HH:MM)，預設 14:30')

    args = parser.parse_args()

    crawler = Goodinfo30HighCrawler()

    if args.mode == 'fetch':
        try:
            df = crawler.fetch_data(force=args.force)
            if df is not None:
                print(f"\n{'=' * 60}")
                print(f"✓ 成功抓取 {len(df)} 筆資料")
                print(f"{'=' * 60}\n")
                print(df.head(10))
                print(f"\n... (共 {len(df)} 筆)")
        except Exception as e:
            print(f"\n✗ 抓取失敗: {str(e)}")
            sys.exit(1)

    elif args.mode == 'schedule':
        crawler.schedule_daily_fetch(time_str=args.time)


if __name__ == "__main__":
    main()