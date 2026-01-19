# utils/crawler_goodinfo_base.py

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time
import io
from pathlib import Path
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# 設定日誌
Path("logs").mkdir(exist_ok=True)
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

    # Driver 路徑
    CHROMEDRIVER_PATH = Path(__file__).resolve().parent.parent / "chromedriver-win64" / "chromedriver.exe"

    # 資料儲存根目錄
    DATA_ROOT_DIR = Path(__file__).resolve().parent.parent / "data" / "goodinfo"

    MAX_RETRIES = 3
    RETRY_DELAY = 5
    WAIT_TIMEOUT = 20

    def __init__(self, data_subdir: str = None):
        self.data_subdir = data_subdir
        self.data_dir = self.DATA_ROOT_DIR / data_subdir if data_subdir else self.DATA_ROOT_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.driver = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def _setup_driver(self):
        """設定 Chrome driver (含 Proxy 與 NO_PROXY 設定)"""

        # 1. 載入 .env
        env_path = self.DATA_ROOT_DIR.parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)

        # 🟢 [關鍵修正] 設定 NO_PROXY
        # 告訴 Python：連線到本機 (localhost) 時，絕對不要走 Proxy！
        # 這能解決 Access Denied 錯誤
        os.environ['NO_PROXY'] = 'localhost,127.0.0.1,::1'
        os.environ['no_proxy'] = 'localhost,127.0.0.1,::1'

        options = webdriver.ChromeOptions()

        # 基本設定
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')

        # 🟢 注入 Proxy 設定給 Chrome 瀏覽器 (這是給瀏覽器看網頁用的)
        proxy = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")
        if proxy:
            proxy_clean = proxy.replace("http://", "").replace("https://", "")
            self.logger.info(f"🔒 Chrome 使用 Proxy: {proxy_clean}")
            options.add_argument(f'--proxy-server=http://{proxy_clean}')

        # SSL/TLS 相關設定
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
        options.add_argument('--disable-web-security')

        # 偽裝設定
        options.add_argument(
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # 頁面載入策略
        options.page_load_strategy = 'eager'

        # 判斷環境
        if os.environ.get('GITHUB_ACTIONS') == 'true':
            driver = webdriver.Chrome(options=options)
        else:
            # 本機環境
            if self.CHROMEDRIVER_PATH.exists():
                service = Service(str(self.CHROMEDRIVER_PATH))
                try:
                    driver = webdriver.Chrome(service=service, options=options)
                except Exception as e:
                    self.logger.warning(f"指定 Driver 啟動失敗 ({e})，嘗試自動尋找...")
                    driver = webdriver.Chrome(options=options)
            else:
                self.logger.info("找不到指定 Driver，嘗試系統路徑...")
                driver = webdriver.Chrome(options=options)

        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        return driver

    def _cleanup_driver(self):
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

    def _parse_goodinfo_table(self, table_id: str = "tblStockList") -> pd.DataFrame:
        page_source = self.driver.page_source

        try:
            page_source = page_source.encode('latin1').decode('utf-8', errors='ignore')
        except:
            pass

        soup = BeautifulSoup(page_source, 'lxml')
        data_table = soup.select_one(f'#{table_id}')

        if not data_table:
            # 檢查是否被擋
            if "刷新過快" in page_source or "請稍後" in page_source:
                raise ValueError("被網站阻擋 (Rate Limit)")
            raise ValueError(f"找不到表格 ID: {table_id}")

        df_list = pd.read_html(io.StringIO(str(data_table)))
        if not df_list:
            raise ValueError("表格解析失敗")

        df = df_list[0]
        if '代號' in df.columns:
            df = df[df['代號'] != '代號']
        df = df.reset_index(drop=True)
        return df

    def _convert_numeric_columns(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        for col in columns:
            if col in df.columns:
                df[col] = (df[col].astype(str)
                           .str.replace('+', '')
                           .str.replace(',', '')
                           .str.strip())
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def _parse_date_from_dataframe(self, df: pd.DataFrame) -> str:
        if '更新 日期' in df.columns and len(df) > 0:
            date_str = str(df['更新 日期'].iloc[0])
            if '/' in date_str:
                parts = date_str.split('/')
                if len(parts) == 2:
                    month, day = parts
                    current_year = datetime.now().year
                    return f"{current_year}{month.zfill(2)}{day.zfill(2)}"
                elif len(parts) == 3:
                    return f"{parts[0]}{parts[1].zfill(2)}{parts[2].zfill(2)}"
        return datetime.now().strftime("%Y%m%d")

    def _generate_filename(self, df: pd.DataFrame, suffix: str) -> Path:
        date_str = self._parse_date_from_dataframe(df)
        filename = f"{date_str}_{suffix}.csv"
        return self.data_dir / filename

    def _file_exists_for_today(self, suffix: str) -> bool:
        today = datetime.now().strftime("%Y%m%d")
        for file in self.data_dir.glob(f"{today}_*{suffix}*.csv"):
            return True
        return False

    def _load_today_data(self, suffix: str) -> pd.DataFrame:
        today = datetime.now().strftime("%Y%m%d")
        files = list(self.data_dir.glob(f"{today}_*{suffix}*.csv"))
        if files:
            return pd.read_csv(files[0], encoding='utf-8-sig')
        return None

    def _fetch_with_retry(self, url: str, table_id: str = "tblStockList") -> pd.DataFrame:
        for attempt in range(self.MAX_RETRIES):
            try:
                self.logger.info(f"第 {attempt + 1} 次嘗試連線...")
                self.driver = self._setup_driver()

                self.driver.implicitly_wait(20)
                self.driver.set_page_load_timeout(90)

                self.driver.get(url)

                wait = WebDriverWait(self.driver, self.WAIT_TIMEOUT)
                wait.until(EC.presence_of_element_located((By.ID, table_id)))

                time.sleep(3 + attempt * 2)

                df = self._parse_goodinfo_table(table_id)
                return df

            except Exception as e:
                self.logger.warning(f"嘗試失敗: {e}")
            finally:
                self._cleanup_driver()

            time.sleep(self.RETRY_DELAY)

        raise Exception("已達最大重試次數，抓取失敗")