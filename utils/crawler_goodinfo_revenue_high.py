"""
Goodinfo 月營收歷年前幾高爬蟲 (合併版)
抓取單月營收歷月最高排名 1-3 的股票，並合併兩個 URL 的欄位

使用方式:
1. 命令列:
   python crawler_goodinfo_revenue_high.py --mode fetch         # 立即抓取 (有快取就跳過)
   python crawler_goodinfo_revenue_high.py --mode fetch --force # 強制重新抓取
   python crawler_goodinfo_revenue_high.py --mode schedule --time 09:10  # 排程執行

2. 在程式中使用:
   from utils.crawler_goodinfo_revenue_high import GoodinfoRevenueHighCrawler
   crawler = GoodinfoRevenueHighCrawler()
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
import re
from utils.crawler_goodinfo_base import GoodinfoBaseCrawler
import os
print("=" * 60, flush=True)
print("🔍 診斷資訊:", flush=True)
print(f"當前工作目錄: {os.getcwd()}", flush=True)
print(f"__file__: {__file__}", flush=True)
print(f"__file__.parent: {Path(__file__).parent}", flush=True)
print(f"__file__.parent.parent: {Path(__file__).parent.parent}", flush=True)

CHROMEDRIVER_PATH = Path(__file__).parent.parent / "chromedriver-win64" / "chromedriver.exe"
print(f"ChromeDriver 路徑: {CHROMEDRIVER_PATH}", flush=True)
print(f"ChromeDriver 存在: {CHROMEDRIVER_PATH.exists()}", flush=True)

if not CHROMEDRIVER_PATH.exists():
    print(f"❌ ChromeDriver 不存在!", flush=True)
    print(f"當前目錄內容: {list(Path(os.getcwd()).iterdir())[:10]}", flush=True)
print("=" * 60, flush=True)
sys.stdout.flush()
class GoodinfoRevenueHighCrawler(GoodinfoBaseCrawler):
    """月營收歷年前幾高爬蟲 (合併版)"""

    # URL 1: 月營收創紀錄統計 (包含排名、創紀錄月數等欄位)
    URL = ('https://goodinfo.tw/tw/StockList.asp?MARKET_CAT=%E8%87%AA%E8%A8%82%E7%AF%A9%E9%81%B8'
           '&INDUSTRY_CAT=%E6%88%91%E7%9A%84%E6%A2%9D%E4%BB%B6&FL_ITEM0=%E5%96%AE%E6%9C%88%E7%87%9F'
           '%E6%94%B6%E6%AD%B7%E6%9C%88%E6%9C%80%E9%AB%98%E6%8E%92%E5%90%8D&FL_VAL_S0=1&FL_VAL_E0=3'
           '&FL_ITEM1=&FL_VAL_S1=&FL_VAL_E1=&FL_ITEM2=&FL_VAL_S2=&FL_VAL_E2=&FL_ITEM3=&FL_VAL_S3='
           '&FL_VAL_E3=&FL_ITEM4=&FL_VAL_S4=&FL_VAL_E4=&FL_ITEM5=&FL_VAL_S5=&FL_VAL_E5=&FL_ITEM6='
           '&FL_VAL_S6=&FL_VAL_E6=&FL_ITEM7=&FL_VAL_S7=&FL_VAL_E7=&FL_ITEM8=&FL_VAL_S8=&FL_VAL_E8='
           '&FL_ITEM9=&FL_VAL_S9=&FL_VAL_E9=&FL_ITEM10=&FL_VAL_S10=&FL_VAL_E10=&FL_ITEM11='
           '&FL_VAL_S11=&FL_VAL_E11=&FL_RULE0=&FL_RULE1=&FL_RULE2=&FL_RULE3=&FL_RULE4=&FL_RULE5='
           '&FL_RANK0=&FL_RANK1=&FL_RANK2=&FL_RANK3=&FL_RANK4=&FL_RANK5=&FL_FD0=&FL_FD1=&FL_FD2='
           '&FL_FD3=&FL_FD4=&FL_FD5=&FL_SHEET=%E7%87%9F%E6%94%B6%E7%8B%80%E6%B3%81&FL_SHEET2='
           '%E6%9C%88%E7%87%9F%E6%94%B6%E5%89%B5%E7%B4%80%E9%8C%84%E7%B5%B1%E8%A8%88&FL_MARKET='
           '%E4%B8%8A%E5%B8%82%2F%E4%B8%8A%E6%AB%83&FL_QRY=%E6%9F%A5++%E8%A9%A2')

    # URL 2: 月營收狀況 (包含月增、年增等詳細欄位)
    URL2 = ('https://goodinfo.tw/tw/StockList.asp?MARKET_CAT=%E8%87%AA%E8%A8%82%E7%AF%A9%E9%81%B8'
            '&INDUSTRY_CAT=%E6%88%91%E7%9A%84%E6%A2%9D%E4%BB%B6&FL_ITEM0=%E5%96%AE%E6%9C%88%E7%87%9F'
            '%E6%94%B6%E6%AD%B7%E6%9C%88%E6%9C%80%E9%AB%98%E6%8E%92%E5%90%8D&FL_VAL_S0=1&FL_VAL_E0=3'
            '&FL_ITEM1=&FL_VAL_S1=&FL_VAL_E1=&FL_ITEM2=&FL_VAL_S2=&FL_VAL_E2=&FL_ITEM3=&FL_VAL_S3='
            '&FL_VAL_E3=&FL_ITEM4=&FL_VAL_S4=&FL_VAL_E4=&FL_ITEM5=&FL_VAL_S5=&FL_VAL_E5=&FL_ITEM6='
            '&FL_VAL_S6=&FL_VAL_E6=&FL_ITEM7=&FL_VAL_S7=&FL_VAL_E7=&FL_ITEM8=&FL_VAL_S8=&FL_VAL_E8='
            '&FL_ITEM9=&FL_VAL_S9=&FL_VAL_E9=&FL_ITEM10=&FL_VAL_S10=&FL_VAL_E10=&FL_ITEM11='
            '&FL_VAL_S11=&FL_VAL_E11=&FL_RULE0=&FL_RULE1=&FL_RULE2=&FL_RULE3=&FL_RULE4=&FL_RULE5='
            '&FL_RANK0=&FL_RANK1=&FL_RANK2=&FL_RANK3=&FL_RANK4=&FL_RANK5=&FL_FD0=&FL_FD1=&FL_FD2='
            '&FL_FD3=&FL_FD4=&FL_FD5=&FL_SHEET=%E7%87%9F%E6%94%B6%E7%8B%80%E6%B3%81&FL_SHEET2='
            '%E6%9C%88%E7%87%9F%E6%94%B6%E7%8B%80%E6%B3%81&FL_MARKET='
            '%E4%B8%8A%E5%B8%82%2F%E4%B8%8A%E6%AB%83&FL_QRY=%E6%9F%A5++%E8%A9%A2')

    FILENAME_SUFFIX = "月營收創新高"

    # 基本數值欄位 (包含兩個 URL 的數值欄位)
    BASIC_NUMERIC_COLUMNS = [
        '成交', '漲跌 價', '漲跌 幅', '成交 張數',
        '單月 營收 (億)', '累月 營收 (億)',
        '單月 營收 月增 (億)', '單月 營收 月增 (%)',
        '單月 營收 年增 (億)', '單月 營收 年增 (%)',
        '累月 營收 年增 (億)', '累月 營收 年增 (%)'
    ]

    # 需要解析為數值的欄位（排名）
    RANK_COLUMNS = [
        '單月 營收 歷月 排名',
        '單月 營收 歷年 排名',
        '累月 營收 歷年 排名'
    ]

    # 保留為文字的欄位（含增減資訊）
    TEXT_COLUMNS = [
        '單月 營收 創紀錄 月數',
        '單月 營收 連增減 月數',
        '單月 營收 創紀錄 年數',
        '累月 營收 創紀錄 年數',
        '累月 營收 連增減 年數',
        '月營收 增減 註記'  # 新增: 包含 ⊕ ⊙ 等符號
    ]

    def __init__(self):
        super().__init__(data_subdir="revenue_high")

    def _parse_goodinfo_rank_column(self, series: pd.Series) -> pd.Series:
        """
        解析 Goodinfo 的排名欄位
        例如: "1高" -> 1, "2高" -> 2, "增→減" -> 0, "3高" -> 3

        Args:
            series: 原始欄位

        Returns:
            解析後的數值欄位
        """
        def extract_rank(value):
            if pd.isna(value):
                return None

            value_str = str(value).strip()

            # 如果包含"增"或"減"但沒有數字，返回 0
            if ('增' in value_str or '減' in value_str) and not any(c.isdigit() for c in value_str):
                return 0

            # 提取數字
            match = re.search(r'(\d+)', value_str)
            if match:
                return int(match.group(1))

            return None

        return series.apply(extract_rank)

    def _parse_goodinfo_month_year_column(self, series: pd.Series) -> pd.Series:
        """
        解析包含月數/年數的欄位
        例如: "95個月高" -> 95, "29年高" -> 29, "連2增" -> 2, "3增→減" -> 0, "4減→增" -> 0

        Args:
            series: 原始欄位

        Returns:
            解析後的數值欄位
        """
        def extract_number(value):
            if pd.isna(value):
                return None

            value_str = str(value).strip()

            # 處理 "X增→減" 或 "X減→增" (表示轉折，返回 0)
            if '→' in value_str:
                return 0

            # 處理 "連X增" 或 "連X減" 的情況
            if '連' in value_str:
                match = re.search(r'連(\d+)', value_str)
                if match:
                    num = int(match.group(1))
                    # 如果是"連X減"，返回負數
                    if '減' in value_str:
                        return -num
                    return num

            # 一般情況，提取數字
            match = re.search(r'(\d+)', value_str)
            if match:
                return int(match.group(1))

            return None

        return series.apply(extract_number)

    def fetch_data(self, force: bool = False) -> pd.DataFrame:
        """
        抓取資料並合併兩個 URL 的欄位

        Args:
            force: 是否強制抓取（忽略本機快取）

        Returns:
            DataFrame (合併後的完整資料)
        """
        # 檢查是否已有今日資料
        if not force and self._file_exists_for_today(self.FILENAME_SUFFIX):
            self.logger.info(f"本機已有今日資料，跳過抓取")
            return self._load_today_data(self.FILENAME_SUFFIX)

        self.logger.info(f"開始抓取: {self.FILENAME_SUFFIX}")

        # 最多重試 3 次整個抓取流程
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                # === 步驟 1: 抓取 URL1 (月營收創紀錄統計) ===
                self.logger.info(f"[嘗試 {attempt}/{max_attempts}] 抓取 URL1: 月營收創紀錄統計欄位")
                df1 = self._fetch_with_retry(self.URL)
                self.logger.info(f"✓ URL1 抓取成功，{len(df1)} 筆資料，{len(df1.columns)} 個欄位")

                # === 步驟 2: 等待一段時間，避免被網站限制 ===
                wait_time = 3 + (attempt - 1) * 2  # 第1次等3秒，第2次等5秒，第3次等7秒
                self.logger.info(f"等待 {wait_time} 秒後抓取第二個 URL...")
                time.sleep(wait_time)

                # === 步驟 3: 抓取 URL2 (月營收狀況) ===
                self.logger.info(f"[嘗試 {attempt}/{max_attempts}] 抓取 URL2: 月營收狀況欄位")
                df2 = self._fetch_with_retry(self.URL2)
                self.logger.info(f"✓ URL2 抓取成功，{len(df2)} 筆資料，{len(df2.columns)} 個欄位")

                # 如果成功抓取兩個 URL，跳出重試迴圈
                break

            except Exception as e:
                self.logger.error(f"✗ 第 {attempt} 次嘗試失敗: {str(e)}")
                if attempt < max_attempts:
                    wait_time = 10 * attempt  # 失敗後等更久：10秒、20秒、30秒
                    self.logger.info(f"等待 {wait_time} 秒後重試...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"✗ 已達最大重試次數，抓取失敗")
                    raise

        # === 步驟 4: 合併兩個 DataFrame ===
        self.logger.info("開始合併兩個資料表...")

        # 找出 df2 中獨有的欄位（排除 df1 已有的）
        df1_cols = set(df1.columns)
        df2_unique_cols = [col for col in df2.columns if col not in df1_cols]

        self.logger.info(f"df2 獨有欄位: {df2_unique_cols}")

        # df2 保留：代號(用於合併) + 獨有欄位
        cols_to_keep = ['代號'] + df2_unique_cols
        df2_filtered = df2[cols_to_keep].copy()

        self.logger.info(f"準備合併，df2_filtered 欄位: {list(df2_filtered.columns)}")

        # 使用左連接合併（以 df1 為主）
        df = pd.merge(df1, df2_filtered, on='代號', how='left', suffixes=('', '_dup'))

        # 移除可能的重複欄位
        df = df.loc[:, ~df.columns.str.endswith('_dup')]

        self.logger.info(f"✓ 合併完成，共 {len(df)} 筆資料，{len(df.columns)} 個欄位")

        # === 步驟 5: 清理欄位名稱（移除空白） ===
        original_cols = df.columns.tolist()
        df.columns = df.columns.str.replace(' ', '', regex=False)
        self.logger.info(f"✓ 欄位名稱已清理（移除空白）")

        # 同時更新欄位定義列表（用於後續處理）
        basic_numeric_cols = [col.replace(' ', '') for col in self.BASIC_NUMERIC_COLUMNS]
        rank_cols = [col.replace(' ', '') for col in self.RANK_COLUMNS]
        text_cols = [col.replace(' ', '') for col in self.TEXT_COLUMNS]

        # === 步驟 6: 轉換數值欄位 ===
        df = self._convert_numeric_columns(df, basic_numeric_cols)

        # === 步驟 7: 解析排名欄位 ===
        for col in rank_cols:
            if col in df.columns:
                self.logger.info(f"解析排名欄位: {col}")
                df[col] = self._parse_goodinfo_rank_column(df[col])

        # === 步驟 8: 保留文字欄位 ===
        for col in text_cols:
            if col in df.columns:
                self.logger.info(f"保留文字欄位: {col}")
                df[col] = df[col].astype(str).str.strip()
                # 清理 'nan' 字串
                df[col] = df[col].replace('nan', '')

        self.logger.info(f"✓ 資料處理完成")

        # === 步驟 7: 儲存檔案 ===
        filepath = self._generate_filename(df, self.FILENAME_SUFFIX)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        self.logger.info(f"✓ 資料已儲存至: {filepath}")

        return df

    def schedule_daily_fetch(self, time_str: str = "09:10"):
        """
        排程每日抓取

        Args:
            time_str: 執行時間，格式 "HH:MM"，預設 09:10 (營收公告後)
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
                time.sleep(60)
        except KeyboardInterrupt:
            self.logger.info("排程已停止")

    def get_filtered_stocks(self,
                           max_monthly_rank: int = None,
                           max_yearly_rank: int = None,
                           min_revenue: float = None,
                           min_mom_growth: float = None,
                           min_yoy_growth: float = None,
                           growth_pattern: str = None,
                           has_positive_note: bool = None) -> pd.DataFrame:
        """
        取得符合條件的月營收創新高股票 (進階版)

        Args:
            max_monthly_rank: 單月營收歷月排名最大值 (例如: 3 代表只要前3名)
            max_yearly_rank: 單月營收歷年排名最大值
            min_revenue: 單月營收最小值 (億)
            min_mom_growth: 單月營收月增率最小值 (%)
            min_yoy_growth: 單月營收年增率最小值 (%)
            growth_pattern: 成長模式篩選，例如: "連" (包含連續成長), "增" (包含增), "減" (包含減)
            has_positive_note: 是否有正向註記 (⊕)

        Returns:
            篩選後的 DataFrame
        """
        df = self.fetch_data()

        # 原有的排名和營收篩選（欄位名稱已無空白）
        if max_monthly_rank is not None and '單月營收歷月排名' in df.columns:
            df = df[df['單月營收歷月排名'] <= max_monthly_rank]

        if max_yearly_rank is not None and '單月營收歷年排名' in df.columns:
            df = df[df['單月營收歷年排名'] <= max_yearly_rank]

        if min_revenue is not None and '單月營收(億)' in df.columns:
            df = df[df['單月營收(億)'] >= min_revenue]

        # 新增: 月增率篩選
        if min_mom_growth is not None and '單月營收月增(%)' in df.columns:
            df = df[df['單月營收月增(%)'] >= min_mom_growth]

        # 新增: 年增率篩選
        if min_yoy_growth is not None and '單月營收年增(%)' in df.columns:
            df = df[df['單月營收年增(%)'] >= min_yoy_growth]

        # 成長模式篩選
        if growth_pattern is not None and '單月營收連增減月數' in df.columns:
            df = df[df['單月營收連增減月數'].str.contains(growth_pattern, na=False)]

        # 新增: 正向註記篩選
        if has_positive_note is not None and '月營收增減註記' in df.columns:
            if has_positive_note:
                df = df[df['月營收增減註記'].str.contains('⊕', na=False)]
            else:
                df = df[~df['月營收增減註記'].str.contains('⊕', na=False)]

        return df


def main():
    """命令列入口"""
    import argparse

    parser = argparse.ArgumentParser(description='Goodinfo 月營收創新高爬蟲 (合併版)')
    parser.add_argument('--mode', choices=['fetch', 'schedule', 'filter', 'test'], default='fetch',
                       help='執行模式: fetch(立即抓取), schedule(排程執行), filter(條件篩選), test(測試模式)')
    parser.add_argument('--force', action='store_true',
                       help='強制抓取，忽略本機快取 (預設: 有快取就用快取)')
    parser.add_argument('--time', default='09:10',
                       help='排程時間 (格式: HH:MM)，預設 09:10')
    parser.add_argument('--monthly-rank', type=int,
                       help='單月營收歷月排名最大值，例如: --monthly-rank 3 (只要前3名)')
    parser.add_argument('--yearly-rank', type=int,
                       help='單月營收歷年排名最大值')
    parser.add_argument('--min-revenue', type=float,
                       help='單月營收最小值(億)，例如: --min-revenue 10')
    parser.add_argument('--min-mom-growth', type=float,
                       help='單月營收月增率最小值(%)，例如: --min-mom-growth 10')
    parser.add_argument('--min-yoy-growth', type=float,
                       help='單月營收年增率最小值(%)，例如: --min-yoy-growth 20')
    parser.add_argument('--growth-pattern', type=str,
                       help='成長模式，例如: --growth-pattern "連3增" 或 "增" 或 "→"')
    parser.add_argument('--positive-note', action='store_true',
                       help='只顯示有正向註記(⊕)的股票')

    args = parser.parse_args()

    crawler = GoodinfoRevenueHighCrawler()

    if args.mode == 'test':
        # ============================================================
        # 測試區塊 - 用於開發測試，正式使用後可移除
        # ============================================================
        print("\n" + "="*100)
        print("測試模式: 驗證合併功能")
        print("="*100 + "\n")

        try:
            df = crawler.fetch_data(force=True)

            print(f"✓ 資料抓取成功")
            print(f"  總筆數: {len(df)}")
            print(f"  總欄位數: {len(df.columns)}")
            print(f"\n所有欄位:")
            for i, col in enumerate(df.columns, 1):
                print(f"  {i:2d}. {col}")

            # 檢查關鍵欄位
            print(f"\n關鍵欄位檢查:")
            key_cols = ['代號', '名稱', '單月營收(億)', '單月營收月增(%)',
                       '單月營收年增(%)', '月營收增減註記']
            for col in key_cols:
                exists = "✓" if col in df.columns else "✗"
                print(f"  {exists} {col}")

            # 顯示前5筆完整資料
            print(f"\n前5筆資料預覽:")
            display_cols = ['代號', '名稱', '成交', '營收月份', '單月營收(億)',
                           '單月營收月增(%)', '單月營收年增(%)',
                           '單月營收歷月排名', '月營收增減註記']
            available_cols = [col for col in display_cols if col in df.columns]
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            print(df[available_cols].head(5).to_string(index=False))

            # 測試進階篩選
            print(f"\n測試進階篩選: 月增率 > 10% 且 年增率 > 20%")
            filtered = crawler.get_filtered_stocks(
                min_mom_growth=10,
                min_yoy_growth=20
            )
            print(f"  符合條件: {len(filtered)} 檔")
            if len(filtered) > 0:
                print(filtered[available_cols].head(3).to_string(index=False))

            print(f"\n✓ 測試完成，功能正常")

        except Exception as e:
            print(f"\n✗ 測試失敗: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        # ============================================================
        # 測試區塊結束
        # ============================================================

    elif args.mode == 'fetch':
        try:
            df = crawler.fetch_data(force=args.force)
            if df is not None:
                print(f"\n{'='*100}")
                print(f"✓ 成功抓取 {len(df)} 筆月營收創新高股票 (完整版)")
                print(f"{'='*100}\n")

                # 顯示前 15 筆
                display_cols = ['代號', '名稱', '成交', '漲跌幅', '營收月份',
                               '單月營收(億)', '單月營收月增(%)', '單月營收年增(%)',
                               '單月營收歷月排名', '月營收增減註記']
                available_cols = [col for col in display_cols if col in df.columns]

                # 格式化顯示
                pd.set_option('display.max_columns', None)
                pd.set_option('display.width', None)
                pd.set_option('display.float_format', lambda x: f'{x:.2f}' if pd.notna(x) else 'N/A')

                print(df[available_cols].head(15).to_string(index=False))
                print(f"\n... (共 {len(df)} 筆)")

                # 顯示統計資訊
                print(f"\n統計資訊:")
                print(f"  歷月第1名: {len(df[df['單月營收歷月排名'] == 1])} 檔")
                print(f"  歷年第1名: {len(df[df['單月營收歷年排名'] == 1])} 檔")

                if '單月營收月增(%)' in df.columns:
                    mom_positive = df[df['單月營收月增(%)'] > 0]
                    print(f"  月增率 > 0%: {len(mom_positive)} 檔")

                if '單月營收年增(%)' in df.columns:
                    yoy_positive = df[df['單月營收年增(%)'] > 0]
                    print(f"  年增率 > 0%: {len(yoy_positive)} 檔")

                if '月營收增減註記' in df.columns:
                    positive_notes = df[df['月營收增減註記'].str.contains('⊕', na=False)]
                    print(f"  有正向註記(⊕): {len(positive_notes)} 檔")

        except Exception as e:
            print(f"\n✗ 抓取失敗: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    elif args.mode == 'schedule':
        crawler.schedule_daily_fetch(time_str=args.time)

    elif args.mode == 'filter':
        try:
            df = crawler.get_filtered_stocks(
                max_monthly_rank=args.monthly_rank,
                max_yearly_rank=args.yearly_rank,
                min_revenue=args.min_revenue,
                min_mom_growth=args.min_mom_growth,
                min_yoy_growth=args.min_yoy_growth,
                growth_pattern=args.growth_pattern,
                has_positive_note=args.positive_note if hasattr(args, 'positive_note') else None
            )

            conditions = []
            if args.monthly_rank:
                conditions.append(f"歷月排名 <= {args.monthly_rank}")
            if args.yearly_rank:
                conditions.append(f"歷年排名 <= {args.yearly_rank}")
            if args.min_revenue:
                conditions.append(f"單月營收 >= {args.min_revenue} 億")
            if args.min_mom_growth:
                conditions.append(f"月增率 >= {args.min_mom_growth}%")
            if args.min_yoy_growth:
                conditions.append(f"年增率 >= {args.min_yoy_growth}%")
            if args.growth_pattern:
                conditions.append(f"連增減模式包含 '{args.growth_pattern}'")
            if args.positive_note:
                conditions.append("有正向註記(⊕)")

            print(f"\n{'='*100}")
            print(f"✓ 找到 {len(df)} 檔符合條件的股票")
            if conditions:
                print(f"  條件: {' & '.join(conditions)}")
            print(f"{'='*100}\n")

            display_cols = ['代號', '名稱', '成交', '營收月份', '單月營收(億)',
                           '單月營收月增(%)', '單月營收年增(%)',
                           '單月營收歷月排名', '單月營收歷年排名',
                           '月營收增減註記']
            available_cols = [col for col in display_cols if col in df.columns]

            if len(df) > 0:
                pd.set_option('display.float_format', lambda x: f'{x:.2f}' if pd.notna(x) else 'N/A')
                print(df[available_cols].to_string(index=False))
            else:
                print("沒有符合條件的股票")
        except Exception as e:
            print(f"\n✗ 篩選失敗: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)


if __name__ == "__main__":
    print(f"DEBUG: 程式開始執行")
    print(f"DEBUG: Python 路徑: {sys.path}")
    print(f"DEBUG: 當前目錄: {Path.cwd()}")
    print(f"DEBUG: 專案根目錄: {project_root}")

    try:
        main()
    except Exception as e:
        print(f"\n程式執行失敗:")
        print(f"錯誤類型: {type(e).__name__}")
        print(f"錯誤訊息: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)