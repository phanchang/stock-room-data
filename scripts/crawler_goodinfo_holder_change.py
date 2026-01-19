# scripts/crawler_goodinfo_holder_change.py

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from utils.crawler_goodinfo_base import GoodinfoBaseCrawler

class GoodinfoHolderChangeCrawler(GoodinfoBaseCrawler):
    """大戶持股週增減爬蟲"""

    # URL (預設抓 >1000張)
    URL_TEMPLATE = (
        'https://goodinfo.tw/tw/StockList.asp?MARKET_CAT=%E8%87%AA%E8%A8%82%E7%AF%A9%E9%81%B8'
        '&INDUSTRY_CAT=%E6%88%91%E7%9A%84%E6%A2%9D%E4%BB%B6&FL_ITEM0=%E5%88%86%E7%B4%9A%E6%8C%81'
        '%E6%9C%89%E5%BC%B5%E6%95%B8%E9%80%B1%E5%A2%9E%E6%B8%9B%28%25%29%E2%80%93%EF%BC%9E1000'
        '%E5%BC%B5%E2%80%93%E7%95%B6%E9%80%B1&FL_VAL_S0={min_val}&FL_VAL_E0=&FL_SHEET=%E8%82%A1%E6%9D%B1'
        '%E6%8C%81%E8%82%A1%E5%88%86%E7%B4%9A_%E9%80%B1%E7%B5%B1%E8%A8%88&FL_MARKET=%E4%B8%8A%E5%B8%82%2F%E4%B8%8A%E6%AB%83'
    )

    FILENAME_SUFFIX = "大戶持股週增減"

    def __init__(self):
        super().__init__(data_subdir="holder_change")

    def fetch_data(self, force: bool = False, min_change: float = 0.5) -> pd.DataFrame:
        url = self.URL_TEMPLATE.format(min_val=min_change)
        suffix = f"{self.FILENAME_SUFFIX}_min{min_change}"

        if not force and self._file_exists_for_today(suffix):
            print("本機已有資料，跳過抓取")
            return self._load_today_data(suffix)

        print(f"🚀 開始抓取: {suffix}")

        try:
            df = self._fetch_with_retry(url)

            # 清理欄位空格
            df.columns = df.columns.str.replace(' ', '')

            # 轉換數值
            # 找出所有包含 '%' 或 '張' 的欄位都轉轉看
            for col in df.columns:
                if '%' in col or '張' in col or '價' in col:
                    df = self._convert_numeric_columns(df, [col])

            filepath = self._generate_filename(df, suffix)
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
    parser.add_argument('--min', type=float, default=0.5)
    args = parser.parse_args()

    crawler = GoodinfoHolderChangeCrawler()

    if args.mode == 'fetch':
        crawler.fetch_data(force=args.force, min_change=args.min)

if __name__ == "__main__":
    main()