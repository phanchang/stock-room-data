# scripts/crawler_goodinfo_revenue_high.py

import sys
from pathlib import Path

# 設定專案根目錄 (讓它能 import utils)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import time
from utils.crawler_goodinfo_base import GoodinfoBaseCrawler

class GoodinfoRevenueHighCrawler(GoodinfoBaseCrawler):
    """月營收歷年前幾高爬蟲"""

    # URL 設定
    URL = ('https://goodinfo.tw/tw/StockList.asp?MARKET_CAT=%E8%87%AA%E8%A8%82%E7%AF%A9%E9%81%B8'
           '&INDUSTRY_CAT=%E6%88%91%E7%9A%84%E6%A2%9D%E4%BB%B6&FL_ITEM0=%E5%96%AE%E6%9C%88%E7%87%9F'
           '%E6%94%B6%E6%AD%B7%E6%9C%88%E6%9C%80%E9%AB%98%E6%8E%92%E5%90%8D&FL_VAL_S0=1&FL_VAL_E0=3'
           '&FL_SHEET=%E7%87%9F%E6%94%B6%E7%8B%80%E6%B3%81&FL_MARKET=%E4%B8%8A%E5%B8%82%2F%E4%B8%8A%E6%AB%83')

    # 第二個 URL 用來抓更多細節
    URL2 = ('https://goodinfo.tw/tw/StockList.asp?MARKET_CAT=%E8%87%AA%E8%A8%82%E7%AF%A9%E9%81%B8'
            '&INDUSTRY_CAT=%E6%88%91%E7%9A%84%E6%A2%9D%E4%BB%B6&FL_ITEM0=%E5%96%AE%E6%9C%88%E7%87%9F'
            '%E6%94%B6%E6%AD%B7%E6%9C%88%E6%9C%80%E9%AB%98%E6%8E%92%E5%90%8D&FL_VAL_S0=1&FL_VAL_E0=3'
            '&FL_SHEET=%E6%9C%88%E7%87%9F%E6%94%B6%E5%89%B5%E7%B4%80%E9%8C%84%E7%B5%B1%E8%A8%88'
            '&FL_MARKET=%E4%B8%8A%E5%B8%82%2F%E4%B8%8A%E6%AB%83')

    FILENAME_SUFFIX = "月營收創新高"

    # 欄位定義 (注意 Goodinfo 欄位常有空格)
    NUMERIC_COLUMNS = [
        '單月 營收 (億)', '單月 營收 月增 (%)', '單月 營收 年增 (%)',
        '累月 營收 (億)', '累月 營收 年增 (%)'
    ]

    def __init__(self):
        # 資料會存在 data/goodinfo/revenue_high/
        super().__init__(data_subdir="revenue_high")

    def fetch_data(self, force: bool = False) -> pd.DataFrame:
        if not force and self._file_exists_for_today(self.FILENAME_SUFFIX):
            print("本機已有資料，跳過抓取")
            return self._load_today_data(self.FILENAME_SUFFIX)

        print(f"🚀 開始抓取: {self.FILENAME_SUFFIX}")

        try:
            # 抓取第一張表
            print("正在抓取營收狀況...")
            df1 = self._fetch_with_retry(self.URL)

            time.sleep(3) # 休息一下

            # 抓取第二張表 (創紀錄統計)
            print("正在抓取創紀錄統計...")
            df2 = self._fetch_with_retry(self.URL2)

            # 合併
            print("合併資料中...")
            # 只取 df2 獨有的欄位
            cols_to_use = df2.columns.difference(df1.columns).tolist()
            cols_to_use.append('代號') # 用來對照

            df = pd.merge(df1, df2[cols_to_use], on='代號', how='left')

            # 清理欄位空格 (方便後續處理)
            df.columns = df.columns.str.replace(' ', '')

            # 數值轉換
            numeric_cols_cleaned = [c.replace(' ', '') for c in self.NUMERIC_COLUMNS]
            df = self._convert_numeric_columns(df, numeric_cols_cleaned)

            # 儲存
            filepath = self._generate_filename(df, self.FILENAME_SUFFIX)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"✅ 成功儲存至: {filepath}")

            return df

        except Exception as e:
            print(f"❌ 抓取失敗: {e}")
            return None

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', default='fetch')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()

    crawler = GoodinfoRevenueHighCrawler()

    if args.mode == 'fetch':
        crawler.fetch_data(force=args.force)

if __name__ == "__main__":
    main()