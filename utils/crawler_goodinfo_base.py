# utils/crawler_goodinfo_base.py

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, StaleElementReferenceException
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
    """Goodinfo 爬蟲基礎類別 (雲端防崩潰版)"""

    CHROMEDRIVER_PATH = Path(__file__).resolve().parent.parent / "chromedriver-win64" / "chromedriver.exe"
    DATA_ROOT_DIR = Path(__file__).resolve().parent.parent / "data" / "goodinfo"

    MAX_RETRIES = 3
    RETRY_DELAY = 10
    WAIT_TIMEOUT = 20

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

        # === 🚀 穩定性關鍵設定 ===
        # 1. 禁用圖片與多媒體 (節省記憶體)
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.stylesheets": 2,
            "profile.default_content_setting_values.notifications": 2
        }
        options.add_experimental_option("prefs", prefs)
        options.add_argument('--blink-settings=imagesEnabled=false')

        # 2. 策略改回 'eager' (none 在某些環境會導致 socket 斷線)
        # eager: DOM 載入完就回傳，不等圖片
        options.page_load_strategy = 'eager'

        # 3. 雲端環境必備參數 (防崩潰)
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')  # 解決容器記憶體不足
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--remote-debugging-port=9222')  # 🟢 關鍵：確保 WebDriver 能連上 Chrome

        # 4. 偽裝與忽略錯誤
        options.add_argument(
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-infobars')

        # === 環境感知 ===
        is_github_actions = os.environ.get('GITHUB_ACTIONS') == 'true'

        if is_github_actions:
            self.logger.info("☁️ 雲端環境：啟動 Linux Driver")
            driver = webdriver.Chrome(options=options)
        else:
            self.logger.info("🏠 本機環境：啟動 Windows Driver")
            os.environ['NO_PROXY'] = 'localhost,127.0.0.1,::1'

            proxy = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")
            if proxy:
                proxy_clean = proxy.replace("http://", "").replace("https://", "")
                options.add_argument(f'--proxy-server=http://{proxy_clean}')
                self.logger.info(f"🔒 Proxy: {proxy_clean}")

            if self.CHROMEDRIVER_PATH.exists():
                service = Service(str(self.CHROMEDRIVER_PATH))
                try:
                    driver = webdriver.Chrome(service=service, options=options)
                except:
                    driver = webdriver.Chrome(options=options)
            else:
                driver = webdriver.Chrome(options=options)

        return driver

    def _cleanup_driver(self):
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

    def _parse_goodinfo_table(self, table_id: str = "tblStockList") -> pd.DataFrame:
        try:
            page_source = self.driver.page_source
        except Exception as e:
            raise ConnectionError(f"瀏覽器通訊失敗: {e}")

        try:
            page_source = page_source.encode('latin1').decode('utf-8', errors='ignore')
        except:
            pass

        soup = BeautifulSoup(page_source, 'lxml')

        # 檢查是否被擋
        if "刷新過快" in str(soup):
            raise ValueError("被 Goodinfo 阻擋 (Rate Limit)")

        data_table = soup.select_one(f'#{table_id}')

        if not data_table:
            # 嘗試找所有表格，有時候廣告會把 ID 擠掉
            if len(soup.select('table')) > 0:
                raise ValueError(f"頁面有表格但 ID 不符 ({table_id})")
            raise ValueError("頁面載入不完整 (找不到表格)")

        df_list = pd.read_html(io.StringIO(str(data_table)))
        if not df_list:
            raise ValueError("表格解析失敗")

        df = df_list[0]
        if '代號' in df.columns:
            df = df[df['代號'] != '代號']
        df = df.reset_index(drop=True)
        return df

    # ==================== NEW METHOD START ====================
    def _click_and_get_updated_table(self, click_target_xpath: str, table_id: str = "tblStockList") -> pd.DataFrame:
        """
        點擊指定元素，智能等待 Goodinfo 的主要資料表更新，然後回傳新的 DataFrame。
        這是處理多頁籤 (Tab) 網站，避免重複載入完整頁面的核心方法。

        :param click_target_xpath: The XPath for the element to click (e.g., a tab link).
        :param table_id: The ID of the data table to monitor for updates.
        :return: A pandas DataFrame of the updated table, or None if it fails.
        """
        try:
            self.logger.info(f"🔗 正在點擊頁籤: {click_target_xpath}")
            wait = WebDriverWait(self.driver, 30) # 等待 30 秒

            # 1. 找到舊的表格元素，以便後續判斷它是否已過時 (stale)
            old_table = self.driver.find_element(By.ID, table_id)

            # 2. 點擊目標頁籤
            tab_to_click = wait.until(EC.element_to_be_clickable((By.XPATH, click_target_xpath)))
            tab_to_click.click()

            # 3. 等待，直到舊的表格元素不再存在於 DOM 中 (stale)
            #    這表示 AJAX 已經觸發，頁面正在更新表格
            self.logger.info("⏳ 等待表格資料更新...")
            wait.until(EC.staleness_of(old_table))

            # 4. 等待新的表格完全載入
            wait.until(EC.presence_of_element_located((By.ID, table_id)))
            self.logger.info("✅ 表格更新完成")

            # 5. 回傳新的表格資料
            df = self._parse_goodinfo_table(table_id)
            return df

        except TimeoutException:
            self.logger.error(f"❌ 點擊後等待表格更新超時: {click_target_xpath}")
            return None
        except Exception as e:
            self.logger.error(f"❌ 點擊或解析更新後的表格時發生錯誤: {e}")
            return None
    # ===================== NEW METHOD END =====================

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
                self.logger.info(f"第 {attempt + 1} 次嘗試連線後sleep")
                self.driver.set_page_load_timeout(15)
                self.driver.set_script_timeout(15)

                # 發送請求
                self.driver.get(url)

                # 等待表格出現
                try:
                    wait = WebDriverWait(self.driver, self.WAIT_TIMEOUT)
                    wait.until(EC.presence_of_element_located((By.ID, table_id)))
                    self.logger.info("✓ 偵測到表格")
                except TimeoutException:
                    self.logger.warning("等待逾時，嘗試直接解析...")

                # 解析
                df = self._parse_goodinfo_table(table_id)
                return df

            except Exception as e:
                self.logger.warning(f"嘗試失敗: {e}")
                # 失敗後多等一下，避開鎖 IP
                time.sleep(10 + attempt * 5)

            finally:
                self._cleanup_driver()

            time.sleep(self.RETRY_DELAY)

        raise Exception("已達最大重試次數，抓取失敗")