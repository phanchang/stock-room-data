"""
Goodinfo 爬蟲基礎類別
提供所有 Goodinfo 爬蟲共用的功能
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import pandas as pd
import time
import io
from pathlib import Path
from datetime import datetime
import logging
import os

# 確保 logs 目錄存在
Path("logs").mkdir(exist_ok=True)

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


class GoodinfoBaseCrawler:
    """Goodinfo 爬蟲基礎類別"""

    # 共用設定 - 請根據你的環境修改
    CHROMEDRIVER_PATH = r"C:\Users\andychang\Desktop\每日選股\chromedriver-win64\chromedriver.exe"
    DATA_ROOT_DIR = Path("data/goodinfo")
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    WAIT_TIMEOUT = 10

    def __init__(self, data_subdir: str = None):
        """
        初始化

        Args:
            data_subdir: 資料子目錄名稱，例如 "30high", "60high"
        """
        self.data_subdir = data_subdir
        self.data_dir = self.DATA_ROOT_DIR / data_subdir if data_subdir else self.DATA_ROOT_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.driver = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def _setup_driver(self):
        """初始化 WebDriver (所有 Goodinfo 爬蟲共用)"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--lang=zh-TW")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )

        service = Service(self.CHROMEDRIVER_PATH)
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    def _cleanup_driver(self):
        """清理 WebDriver"""
        if self.driver:
            self.driver.quit()
            self.driver = None

    def _parse_goodinfo_table(self, table_id: str = "tblStockList") -> pd.DataFrame:
        """
        解析 Goodinfo 表格 (共用邏輯)

        Args:
            table_id: 表格 ID

        Returns:
            DataFrame
        """
        page_source = self.driver.page_source

        # Goodinfo 編碼處理
        try:
            page_source = page_source.encode('latin1').decode('utf-8', errors='ignore')
        except:
            pass

        soup = BeautifulSoup(page_source, 'lxml')
        data_table = soup.select_one(f'#{table_id}')

        if not data_table:
            raise ValueError(f"找不到表格 ID: {table_id}")

        df_list = pd.read_html(io.StringIO(str(data_table)))
        if not df_list:
            raise ValueError("表格解析失敗")

        df = df_list[0]

        # 移除重複的標題列
        if '代號' in df.columns:
            df = df[df['代號'] != '代號']

        df = df.reset_index(drop=True)

        return df

    def _convert_numeric_columns(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """
        轉換數值欄位

        Args:
            df: DataFrame
            columns: 要轉換的欄位清單

        Returns:
            轉換後的 DataFrame
        """
        for col in columns:
            if col in df.columns:
                df[col] = (df[col].astype(str)
                          .str.replace('+', '')
                          .str.replace(',', '')
                          .str.strip())
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def _parse_date_from_dataframe(self, df: pd.DataFrame) -> str:
        """
        從 DataFrame 解析更新日期

        Returns:
            格式化的日期字串 YYYYMMDD
        """
        if '更新 日期' in df.columns and len(df) > 0:
            date_str = str(df['更新 日期'].iloc[0])
            if '/' in date_str:
                month, day = date_str.split('/')
                current_year = datetime.now().year
                return f"{current_year}{month.zfill(2)}{day.zfill(2)}"

        return datetime.now().strftime("%Y%m%d")

    def _generate_filename(self, df: pd.DataFrame, suffix: str) -> Path:
        """
        生成檔案名稱

        Args:
            df: DataFrame
            suffix: 檔案名稱後綴

        Returns:
            完整檔案路徑
        """
        date_str = self._parse_date_from_dataframe(df)
        filename = f"{date_str}_{suffix}.csv"
        return self.data_dir / filename

    def _file_exists_for_today(self, suffix: str) -> bool:
        """檢查今天的資料檔案是否已存在"""
        today = datetime.now().strftime("%Y%m%d")
        pattern = f"{today}_{suffix}.csv"
        filepath = self.data_dir / pattern
        return filepath.exists()

    def _load_today_data(self, suffix: str) -> pd.DataFrame:
        """載入今天的資料"""
        today = datetime.now().strftime("%Y%m%d")
        filepath = self.data_dir / f"{today}_{suffix}.csv"
        return pd.read_csv(filepath, encoding='utf-8-sig')

    def _fetch_with_retry(self, url: str, table_id: str = "tblStockList") -> pd.DataFrame:
        """
        帶重試機制的抓取

        Args:
            url: 目標 URL
            table_id: 表格 ID

        Returns:
            DataFrame
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                self.logger.info(f"第 {attempt + 1} 次嘗試 (共 {self.MAX_RETRIES} 次)")

                self._setup_driver()
                self.driver.get(url)

                wait = WebDriverWait(self.driver, self.WAIT_TIMEOUT)
                wait.until(EC.presence_of_element_located((By.ID, table_id)))
                self.logger.info("✓ 表格載入成功")

                time.sleep(2)

                df = self._parse_goodinfo_table(table_id)
                self._cleanup_driver()

                return df

            except TimeoutException:
                self.logger.warning(f"✗ 連線逾時")
            except Exception as e:
                self.logger.error(f"✗ 發生錯誤: {str(e)}")
            finally:
                self._cleanup_driver()

            if attempt < self.MAX_RETRIES - 1:
                self.logger.info(f"等待 {self.RETRY_DELAY} 秒後重試...")
                time.sleep(self.RETRY_DELAY)

        raise Exception("已達最大重試次數，抓取失敗")

    def load_data(self, date: str = None, suffix: str = None) -> pd.DataFrame:
        """
        載入本機資料

        Args:
            date: 日期 YYYYMMDD，預設今天
            suffix: 檔案後綴

        Returns:
            DataFrame
        """
        if date is None:
            date = datetime.now().strftime("%Y%m%d")

        if suffix is None:
            raise ValueError("必須指定 suffix")

        filepath = self.data_dir / f"{date}_{suffix}.csv"

        if not filepath.exists():
            raise FileNotFoundError(f"找不到檔案: {filepath}")

        return pd.read_csv(filepath, encoding='utf-8-sig')

    def list_available_dates(self, suffix: str = None) -> list:
        """
        列出可用的日期清單

        Args:
            suffix: 檔案後綴

        Returns:
            日期清單
        """
        if suffix:
            pattern = f"*_{suffix}.csv"
        else:
            pattern = "*.csv"

        files = sorted(self.data_dir.glob(pattern))
        dates = [f.stem.split('_')[0] for f in files]
        return dates