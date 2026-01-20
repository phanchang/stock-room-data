# scripts/crawler_goodinfo_revenue_high.py

import sys
from pathlib import Path

# 設定專案根目錄
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from utils.crawler_goodinfo_base import GoodinfoBaseCrawler
# --- ❗ 把必要的等待工具加回來 ---
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


class GoodinfoRevenueHighCrawler(GoodinfoBaseCrawler):
    """月營收歷年前幾高爬蟲 (穩定修正版)"""

    URL1 = ('https://goodinfo.tw/tw/StockList.asp?MARKET_CAT=%E8%87%AA%E8%A8%82%E7%AF%A9%E9%81%B8'
            '&INDUSTRY_CAT=%E6%88%91%E7%9A%84%E6%A2%9D%E4%BB%B6&FL_ITEM0=%E5%96%AE%E6%9C%88%E7%87%9F'
            '%E6%94%B6%E6%AD%B7%E6%9C%88%E6%9C%80%E9%AB%98%E6%8E%92%E5%90%8D&FL_VAL_S0=1&FL_VAL_E0=3'
            '&FL_SHEET=%E7%87%9F%E6%94%B6%E7%8B%80%E6%B3%81&FL_MARKET=%E4%B8%8A%E5%B8%82%2F%E4%B8%8A%E6%ab%83')

    URL2 = ('https://goodinfo.tw/tw/StockList.asp?MARKET_CAT=%E8%87%AA%E8%A8%82%E7%AF%A9%E9%81%B8'
            '&INDUSTRY_CAT=%E6%88%91%E7%9A%84%E6%A2%9D%E4%BB%B6&FL_ITEM0=%E5%96%AE%E6%9C%88%E7%87%9F'
            '%E6%94%B6%E6%AD%B7%E6%9C%88%E6%9C%80%E9%AB%98%E6%8E%92%E5%90%8D&FL_VAL_S0=1&FL_VAL_E0=3'
            '&FL_SHEET=%E6%9C%88%E7%87%9F%E6%94%B6%E5%89%B5%E7%B4%80%E9%8C%84%E7%B5%B1%E8%A8%88'
            '&FL_MARKET=%E4%B8%8A%E5%B8%82%2F%E4%B8%8A%E6%ab%83')

    FILENAME_SUFFIX = "月營收創新高"

    NUMERIC_COLUMNS = [
        '單月 營收 (億)', '單月 營收 月增 (%)', '單月 營收 年增 (%)',
        '累月 營收 (億)', '累月 營收 年增 (%)'
    ]

    def __init__(self):
        super().__init__(data_subdir="revenue_high")

    def fetch_data(self, force: bool = False) -> pd.DataFrame:
        if not force and self._file_exists_for_today(self.FILENAME_SUFFIX):
            self.logger.info("本機已有資料，跳過抓取")
            return self._load_today_data(self.FILENAME_SUFFIX)

        self.logger.info(f"🚀 開始抓取 (穩定修正策略): {self.FILENAME_SUFFIX}")

        try:
            self.driver = self._setup_driver()

            # --- 抓取第一張表 ---
            self.logger.info("正在抓取第一張表 (營收狀況)...")
            self.driver.get(self.URL1)

            # --- ❗ 把耐心加回來 ---
            self.logger.info("等待表格 #1 出現...")
            WebDriverWait(self.driver, self.WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.ID, "tblStockList"))
            )

            df1 = self._parse_goodinfo_table()
            self.logger.info("✅ 第一張表抓取成功")

            # --- 抓取第二張表 ---
            self.logger.info("正在抓取第二張表 (創紀錄統計)...")
            self.driver.get(self.URL2)

            # --- ❗ 再次加上耐心 ---
            self.logger.info("等待表格 #2 出現...")
            WebDriverWait(self.driver, self.WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.ID, "tblStockList"))
            )

            df2 = self._parse_goodinfo_table()
            self.logger.info("✅ 第二張表抓取成功")

            # --- 合併資料 ---
            self.logger.info("合併資料中...")
            cols_to_use = df2.columns.difference(df1.columns).tolist()
            cols_to_use.append('代號')
            df = pd.merge(df1, df2[cols_to_use], on='代號', how='left')

            df.columns = df.columns.str.replace(' ', '')
            numeric_cols_cleaned = [c.replace(' ', '') for c in self.NUMERIC_COLUMNS]
            df = self._convert_numeric_columns(df, numeric_cols_cleaned)

            filepath = self._generate_filename(df, self.FILENAME_SUFFIX)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            self.logger.info(f"✅ 成功儲存至: {filepath}")

            return df

        except Exception as e:
            self.logger.error(f"❌ 抓取失敗: {e}")
            if self.driver:
                self.driver.save_screenshot('logs/error_screenshot.png')
            return None
        finally:
            self._cleanup_driver()


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