"""
Goodinfo 分級持有張數增減爬蟲 (大戶持股變化)
抓取持股 >600張、>800張、>1000張 的週增減資料

使用方式:
1. 命令列:
   python crawler_goodinfo_holder_change.py --mode fetch                    # 使用預設參數 (0.01, 空)
   python crawler_goodinfo_holder_change.py --mode fetch --min 0.5 --max 5  # 自訂參數範圍
   python crawler_goodinfo_holder_change.py --mode fetch --force            # 強制重新抓取
   python crawler_goodinfo_holder_change.py --mode schedule --time 09:30    # 排程執行

2. 在程式中使用:
   from utils.crawler_goodinfo_holder_change import GoodinfoHolderChangeCrawler
   crawler = GoodinfoHolderChangeCrawler()
   df = crawler.fetch_data(min_change=0.01, max_change='')
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


class GoodinfoHolderChangeCrawler(GoodinfoBaseCrawler):
    """
    大戶持股變化爬蟲

    ⭐ 複製此模板建立新爬蟲時，需要修改的地方已標註 [需修改]
    """

    # ==================== [需修改] 爬蟲基本設定 ====================

    # URL 基礎模板 (不含參數)
    # 提示: 將 FL_VAL_S0 和 FL_VAL_E0 的值改為 {min_val} 和 {max_val}
    URL_TEMPLATE = (
        'https://goodinfo.tw/tw/StockList.asp?MARKET_CAT=%E8%87%AA%E8%A8%82%E7%AF%A9%E9%81%B8'
        '&INDUSTRY_CAT=%E6%88%91%E7%9A%84%E6%A2%9D%E4%BB%B6&FL_ITEM0=%E5%88%86%E7%B4%9A%E6%8C%81'
        '%E6%9C%89%E5%BC%B5%E6%95%B8%E9%80%B1%E5%A2%9E%E6%B8%9B%28%25%29%E2%80%93%EF%BC%9E1000'
        '%E5%BC%B5%E2%80%93%E7%95%B6%E9%80%B1&FL_VAL_S0={min_val}&FL_VAL_E0={max_val}&FL_ITEM1='
        '&FL_VAL_S1=&FL_VAL_E1=&FL_ITEM2=&FL_VAL_S2=&FL_VAL_E2=&FL_ITEM3=&FL_VAL_S3=&FL_VAL_E3='
        '&FL_ITEM4=&FL_VAL_S4=&FL_VAL_E4=&FL_ITEM5=&FL_VAL_S5=&FL_VAL_E5=&FL_ITEM6=&FL_VAL_S6='
        '&FL_VAL_E6=&FL_ITEM7=&FL_VAL_S7=&FL_VAL_E7=&FL_ITEM8=&FL_VAL_S8=&FL_VAL_E8=&FL_ITEM9='
        '&FL_VAL_S9=&FL_VAL_E9=&FL_ITEM10=&FL_VAL_S10=&FL_VAL_E10=&FL_ITEM11=&FL_VAL_S11='
        '&FL_VAL_E11=&FL_RULE0=%E7%94%A2%E6%A5%AD%E9%A1%9E%E5%88%A5%7C%7C%E5%AD%98%E8%A8%97'
        '%E6%86%91%E8%AD%89&FL_RULE_CHK0=T&FL_RULE1=%E7%94%A2%E6%A5%AD%E9%A1%9E%E5%88%A5%7C%7C'
        '%E4%B8%8D%E5%8B%95%E7%94%A2%E6%8A%95%E8%B3%87%E4%BF%A1%E8%A8%97%E8%AD%89%E5%88%B8'
        '&FL_RULE_CHK1=T&FL_RULE2=%E7%94%A2%E6%A5%AD%E9%A1%9E%E5%88%A5%7C%7C%40%40ETF%40%40ETF'
        '&FL_RULE_CHK2=T&FL_RULE3=%E7%94%A2%E6%A5%AD%E9%A1%9E%E5%88%A5%7C%7C%40%40ETN%40%40ETN'
        '&FL_RULE_CHK3=T&FL_RULE4=&FL_RULE5=&FL_RANK0=&FL_RANK1=&FL_RANK2=&FL_RANK3=&FL_RANK4='
        '&FL_RANK5=&FL_FD0=&FL_FD1=&FL_FD2=&FL_FD3=&FL_FD4=&FL_FD5=&FL_SHEET=%E8%82%A1%E6%9D%B1'
        '%E6%8C%81%E8%82%A1%E5%88%86%E7%B4%9A_%E9%80%B1%E7%B5%B1%E8%A8%88&FL_SHEET2=%E6%8C%81'
        '%E6%9C%89%E5%BC%B5%E6%95%B8%28%E8%90%AC%E5%BC%B5%29%E2%80%93%E5%8D%80%E9%96%93%E5%88%86'
        '%E7%B4%9A%28%EF%BC%9E600%E5%BC%B5%29&FL_MARKET=%E4%B8%8A%E5%B8%82%2F%E4%B8%8A%E6%AB%83'
        '&FL_QRY=%E6%9F%A5++%E8%A9%A2'
    )

    # 預設參數值
    DEFAULT_MIN_CHANGE = 0.01  # FL_VAL_S0 的預設值
    DEFAULT_MAX_CHANGE = ''  # FL_VAL_E0 的預設值 (空字串表示無上限)

    # 檔案名稱後綴 (會出現在檔名中)
    FILENAME_SUFFIX = "大戶持股週增減"

    # 資料儲存子目錄名稱
    DATA_SUBDIR = "holder_change"

    # ==================== [需修改] 欄位設定 ====================

    # 基本數值欄位 (直接轉換為數值)
    BASIC_NUMERIC_COLUMNS = [
        '成交', '漲跌 價', '漲跌 幅',
        '持張 ＞600 ≦800 (萬張)',
        '＞600 ≦800 增減 (張)',
        '＞600 ≦800 增減 (%)',
        '持張 ＞800 ≦1千 (萬張)',
        '＞800 ≦1千 增減 (張)',
        '＞800 ≦1千 增減 (%)',
        '持張 ＞1千 (萬張)',
        '＞1千 增減 (張)',
        '＞1千 增減 (%)'
    ]

    # 保留為文字的欄位 (例如日期、週別等)
    TEXT_COLUMNS = [
        '持股 資料 週別',
        '更新 日期'
    ]

    # ==================== 以下程式碼通常不需要修改 ====================

    def __init__(self):
        """初始化爬蟲"""
        super().__init__(data_subdir=self.DATA_SUBDIR)
        self.current_min_change = self.DEFAULT_MIN_CHANGE
        self.current_max_change = self.DEFAULT_MAX_CHANGE

    def _build_url(self, min_change: float = None, max_change: str = None) -> str:
        """
        建立完整 URL

        Args:
            min_change: 最小變化百分比 (對應 FL_VAL_S0)
            max_change: 最大變化百分比 (對應 FL_VAL_E0)，空字串表示無上限

        Returns:
            完整 URL
        """
        if min_change is None:
            min_change = self.DEFAULT_MIN_CHANGE
        if max_change is None:
            max_change = self.DEFAULT_MAX_CHANGE

        # 儲存當前參數 (用於生成檔名)
        self.current_min_change = min_change
        self.current_max_change = max_change

        return self.URL_TEMPLATE.format(min_val=min_change, max_val=max_change)

    def _generate_filename_with_params(self, df: pd.DataFrame) -> Path:
        """
        生成包含參數的檔案名稱

        Returns:
            完整檔案路徑
        """
        date_str = self._parse_date_from_dataframe(df)

        # 檔名包含參數資訊
        if self.current_max_change == '':
            param_str = f"min{self.current_min_change}"
        else:
            param_str = f"min{self.current_min_change}_max{self.current_max_change}"

        filename = f"{date_str}_{self.FILENAME_SUFFIX}_{param_str}.csv"
        return self.data_dir / filename

    def fetch_data(self, force: bool = False, min_change: float = None, max_change: str = None) -> pd.DataFrame:
        """
        抓取資料

        Args:
            force: 是否強制抓取（忽略本機快取）
            min_change: 最小變化百分比，預設 0.01
            max_change: 最大變化百分比，預設空字串（無上限）

        Returns:
            DataFrame
        """
        # 建立 URL
        url = self._build_url(min_change, max_change)

        # 生成對應的檔案名稱後綴
        if self.current_max_change == '':
            param_suffix = f"{self.FILENAME_SUFFIX}_min{self.current_min_change}"
        else:
            param_suffix = f"{self.FILENAME_SUFFIX}_min{self.current_min_change}_max{self.current_max_change}"

        # 檢查是否已有今日資料
        if not force and self._file_exists_for_today(param_suffix):
            self.logger.info(
                f"本機已有今日資料 (參數: min={self.current_min_change}, max={self.current_max_change})，跳過抓取")
            return self._load_today_data(param_suffix)

        self.logger.info(
            f"開始抓取: {self.FILENAME_SUFFIX} (參數: min={self.current_min_change}, max={self.current_max_change})")

        # 使用基礎類別的重試機制抓取
        df = self._fetch_with_retry(url)

        # 轉換基本數值欄位
        df = self._convert_numeric_columns(df, self.BASIC_NUMERIC_COLUMNS)

        # 清理文字欄位
        for col in self.TEXT_COLUMNS:
            if col in df.columns:
                self.logger.info(f"清理文字欄位: {col}")
                df[col] = df[col].astype(str).str.strip()

        self.logger.info(f"✓ 成功抓取 {len(df)} 筆資料")

        # 儲存檔案
        filepath = self._generate_filename_with_params(df)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        self.logger.info(f"✓ 資料已儲存至: {filepath}")

        return df

    def schedule_daily_fetch(self, time_str: str = "09:30", min_change: float = None, max_change: str = None):
        """
        排程每日抓取

        Args:
            time_str: 執行時間，格式 "HH:MM"，預設 09:30
            min_change: 最小變化百分比
            max_change: 最大變化百分比
        """
        self.logger.info(
            f"設定排程: 每日 {time_str} 執行 (參數: min={min_change or self.DEFAULT_MIN_CHANGE}, max={max_change or self.DEFAULT_MAX_CHANGE})")

        def job():
            self.logger.info(f"排程任務執行中...")
            try:
                self.fetch_data(force=False, min_change=min_change, max_change=max_change)
            except Exception as e:
                self.logger.error(f"排程任務執行失敗: {str(e)}")

        schedule.every().day.at(time_str).do(job)

        self.logger.info("排程已啟動，按 Ctrl+C 停止")
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            self.logger.info("排程已停止")

    # ==================== [可選修改] 自訂篩選方法 ====================

    def get_big_buyer_stocks(self, min_holder_1000_change: float = 0.5) -> pd.DataFrame:
        """
        取得大戶(>1000張)大量買進的股票

        Args:
            min_holder_1000_change: >1000張持股變化百分比門檻

        Returns:
            篩選後的 DataFrame
        """
        df = self.fetch_data()

        if '＞1千 增減 (%)' in df.columns:
            df = df[df['＞1千 增減 (%)'] >= min_holder_1000_change]

        return df


def main():
    """命令列入口"""
    import argparse

    parser = argparse.ArgumentParser(description='Goodinfo 大戶持股週增減爬蟲')
    parser.add_argument('--mode', choices=['fetch', 'schedule', 'filter'], default='fetch',
                        help='執行模式: fetch(立即抓取), schedule(排程執行), filter(條件篩選)')
    parser.add_argument('--force', action='store_true',
                        help='強制抓取，忽略本機快取')
    parser.add_argument('--time', default='09:30',
                        help='排程時間 (格式: HH:MM)，預設 09:30')

    # ==================== [需修改] 參數設定 ====================
    parser.add_argument('--min', type=float, default=0.01,
                        help='最小變化百分比 (FL_VAL_S0)，預設 0.01')
    parser.add_argument('--max', type=str, default='',
                        help='最大變化百分比 (FL_VAL_E0)，預設空字串（無上限）')
    parser.add_argument('--filter-1000', type=float,
                        help='篩選 >1000張 持股變化百分比，例如: --filter-1000 1.0')

    args = parser.parse_args()

    crawler = GoodinfoHolderChangeCrawler()

    if args.mode == 'fetch':
        try:
            df = crawler.fetch_data(force=args.force, min_change=args.min, max_change=args.max)
            if df is not None:
                print(f"\n{'=' * 100}")
                print(f"✓ 成功抓取 {len(df)} 筆大戶持股變化資料")
                print(f"  參數: 最小變化 >= {args.min}%", end='')
                if args.max:
                    print(f", 最大變化 <= {args.max}%")
                else:
                    print(f" (無上限)")
                print(f"{'=' * 100}\n")

                # 顯示前 15 筆
                display_cols = ['代號', '名稱', '成交', '漲跌 幅', '持股 資料 週別',
                                '＞1千 (萬張)', '＞1千 增減 (張)', '＞1千 增減 (%)',
                                '＞800 ≦1千 增減 (%)', '＞600 ≦800 增減 (%)']
                available_cols = [col for col in display_cols if col in df.columns]

                pd.set_option('display.max_columns', None)
                pd.set_option('display.width', None)
                pd.set_option('display.float_format', lambda x: f'{x:.2f}' if pd.notna(x) else 'N/A')

                print(df[available_cols].head(15).to_string(index=False))
                print(f"\n... (共 {len(df)} 筆)")

        except Exception as e:
            print(f"\n✗ 抓取失敗: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    elif args.mode == 'schedule':
        crawler.schedule_daily_fetch(time_str=args.time, min_change=args.min, max_change=args.max)

    elif args.mode == 'filter':
        try:
            if args.filter_1000:
                df = crawler.get_big_buyer_stocks(min_holder_1000_change=args.filter_1000)
                print(f"\n{'=' * 100}")
                print(f"✓ 找到 {len(df)} 檔大戶買進股票")
                print(f"  條件: >1000張持股變化 >= {args.filter_1000}%")
                print(f"{'=' * 100}\n")

                display_cols = ['代號', '名稱', '成交', '漲跌 幅',
                                '＞1千 (萬張)', '＞1千 增減 (張)', '＞1千 增減 (%)']
                available_cols = [col for col in display_cols if col in df.columns]

                if len(df) > 0:
                    pd.set_option('display.float_format', lambda x: f'{x:.2f}' if pd.notna(x) else 'N/A')
                    print(df[available_cols].to_string(index=False))
                else:
                    print("沒有符合條件的股票")
            else:
                print("請使用 --filter-1000 指定篩選條件")
        except Exception as e:
            print(f"\n✗ 篩選失敗: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)


if __name__ == "__main__":
    main()