# utils/crawler_goodinfo_base.py

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
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
    """Goodinfo 爬蟲基礎類別 (極速暴力版 - 回歸 strategy='none')"""

    CHROMEDRIVER_PATH = Path(__file__).resolve().parent.parent / "chromedriver-win64" / "chromedriver.exe"
    DATA_ROOT_DIR = Path(__file__).resolve().parent.parent / "data" / "goodinfo"

    MAX_RETRIES = 3
    RETRY_DELAY = 5
    # 這是「輪詢」的最大時間，不是連線時間。15秒內沒看到表格就重試。
    POLLING_TIMEOUT = 20

    def __init__(self, data_subdir: str = None):
        self.data_subdir = data_subdir
        self.data_dir = self.DATA_ROOT_DIR / data_subdir if data_subdir else self.DATA_ROOT_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.driver = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def _setup_driver(self):
        """設定 Chrome driver"""
        env_path = self.DATA_ROOT_DIR.parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)

        options = webdriver.ChromeOptions()

        # === 🚀 極限效能優化 (回歸這一版) ===
        # 1. 徹底禁用圖片、CSS、字型
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.stylesheets": 2,
            "profile.managed_default_content_settings.fonts": 2,
        }
        options.add_experimental_option("prefs", prefs)
        options.add_argument('--blink-settings=imagesEnabled=false')

        # 2. 策略：None (網址打出去立刻回傳，不等轉圈圈)
        # 這是解決 Timeout 和本機 15秒完成的關鍵！
        options.page_load_strategy = 'none'

        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')

        # 禁用干擾項
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking')

        # 偽裝
        options.add_argument(
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        options.add_argument('--ignore-certificate-errors')

        # === 環境感知 (這是本機成功的關鍵，不能改壞) ===
        is_github_actions = os.environ.get('GITHUB_ACTIONS') == 'true'

        if is_github_actions:
            self.logger.info("☁️ 雲端環境：極速模式啟動 (自動 Driver)")
            driver = webdriver.Chrome(options=options)
        else:
            self.logger.info("🏠 本機環境：極速模式啟動 (Proxy + 指定 Driver)")

            # 設定 NO_PROXY 避免 localhost 被擋 (本機必須)
            os.environ['NO_PROXY'] = 'localhost,127.0.0.1,::1'

            proxy = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")
            if proxy:
                proxy_clean = proxy.replace("http://", "").replace("https://", "")
                options.add_argument(f'--proxy-server=http://{proxy_clean}')
                self.logger.info(f"🔒 Chrome Proxy 已啟用")

            if self.CHROMEDRIVER_PATH.exists():
                service = Service(str(self.CHROMEDRIVER_PATH))
                try:
                    driver = webdriver.Chrome(service=service, options=options)
                except:
                    driver = webdriver.Chrome(options=options)
            else:
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
        # 在解析前，先嘗試停止網頁繼續載入 (斷尾求生)
        try:
            self.driver.execute_script("window.stop();")
        except:
            pass

        try:
            page_source = self.driver.page_source
        except:
            raise ConnectionError("瀏覽器已死")

        try:
            page_source = page_source.encode('latin1').decode('utf-8', errors='ignore')
        except:
            pass

        soup = BeautifulSoup(page_source, 'lxml')
        data_table = soup.select_one(f'#{table_id}')

        if not data_table:
            if "刷新過快" in str(soup) or "請稍後" in str(soup):
                raise ValueError("被網站阻擋 (Rate Limit)")
            raise ValueError(f"表格尚未出現 ({table_id})")

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

                if self.driver:
                    self._cleanup_driver()
                self.driver = self._setup_driver()

                # 設定 Timeout
                self.driver.set_page_load_timeout(30)
                self.driver.set_script_timeout(30)

                # 1. 發送請求 (因為 strategy='none'，這裡會瞬間返回)
                self.driver.get(url)

                # 2. 手動輪詢 (Polling) 等待表格出現
                # 這是最關鍵的一步：我們不等網頁跑完，我們只盯著表格 ID
                elapsed = 0
                found = False
                check_interval = 1  # 每秒檢查一次

                while elapsed < self.POLLING_TIMEOUT:
                    try:
                        # 檢查元素是否存在 (不需要完整載入，只要 DOM 有就好)
                        element = self.driver.find_element(By.ID, table_id)
                        if element:
                            found = True
                            self.logger.info(f"✓ 在 {elapsed} 秒時偵測到表格")
                            break
                    except:
                        pass

                    time.sleep(check_interval)
                    elapsed += check_interval

                if not found:
                    self.logger.warning("等待表格逾時，但嘗試強制解析看看...")

                # 3. 強制解析
                df = self._parse_goodinfo_table(table_id)
                return df

            except Exception as e:
                self.logger.warning(f"嘗試失敗: {e}")
            finally:
                self._cleanup_driver()

            time.sleep(self.RETRY_DELAY)

        raise Exception("已達最大重試次數，抓取失敗")