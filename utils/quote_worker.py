import time
import datetime
import re
import random
from PyQt6.QtCore import QThread, pyqtSignal

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


# 若您的環境需要 Service，可自行取消註解
# from selenium.webdriver.chrome.service import Service

class QuoteWorker(QThread):
    # 回傳格式: {stock_id: {realtime: {...}, info: {...}}}
    quote_updated = pyqtSignal(dict)
    oneshot_finished = pyqtSignal()

    # 定義解析欄位 (來自您的範例)
    FIELD_MAP = {
        '成交': '成交', '漲跌': '漲跌', '漲跌幅': '漲跌幅',
        '單量': '單量', '總量': '總量', '金額': r'金額\(?億\)?',
        '開盤': '開盤', '最高': '最高', '最低': '最低', '均價': '均價',
        '買價': '買價', '賣價': '賣價', '內盤': r'內盤\(?張\)?', '外盤': r'外盤\(?張\)?',
        '本益比': '本益比', '市值': r'市值\(?億\)?'
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self.is_running = False
        self.monitoring_stocks = []
        self.source = 'cmoney'
        self.mode = 'continuous'
        self.driver = None

    def set_monitoring_stocks(self, stock_list, source='unknown'):
        # 只取股票代號 (去除 _TW 尾綴)
        self.monitoring_stocks = list(set([s.split('_')[0] for s in stock_list if s]))
        self.source = source

    def set_mode(self, mode='continuous'):
        self.mode = mode

    def toggle_monitoring(self, enable):
        if enable:
            if not self.isRunning():
                self.is_running = True
                self.start()
        else:
            self.stop()

    def stop(self):
        """溫柔停止：設定旗標，讓 Loop 跑完當前股票後退出"""
        print("🛑 [QuoteWorker] 收到停止指令...")
        self.is_running = False
        # 不強制 terminate，讓 run() 裡的 finally 區塊去關閉瀏覽器

    def _init_driver(self):
        """初始化 Chrome Driver"""
        print("🔧 [QuoteWorker] 正在啟動 Chrome Driver (Headless)...")
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")  # 背景執行
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument("--log-level=3")

        # 直接初始化
        driver = webdriver.Chrome(options=chrome_options)
        return driver

    def parse_cmoney_text(self, text):
        """使用 Regex 解析 CMoney 網頁文字"""
        result = {}
        clean_text = text.replace('\n', ' ')
        for field_name, keyword in self.FIELD_MAP.items():
            pattern = rf"{keyword}\s*([+-]?[\d,]+\.?\d*%?)"
            match = re.search(pattern, clean_text)
            val = match.group(1) if match else "-"
            # 移除逗號以便後續轉換數值
            if val != "-":
                val = val.replace(',', '')
            result[field_name] = val
        return result

    def convert_to_ui_format(self, stock_id, cmoney_data):
        """將 CMoney 的中文欄位轉換為 UI 顯示用的標準格式"""

        def safe_float(v):
            try:
                return float(v)
            except:
                return 0.0

        latest_price = safe_float(cmoney_data.get('成交', 0))

        return {
            'realtime': {
                'latest_trade_price': latest_price,
                'open': safe_float(cmoney_data.get('開盤', 0)),
                'high': safe_float(cmoney_data.get('最高', 0)),
                'low': safe_float(cmoney_data.get('最低', 0)),
                'close': latest_price,
                'trade_volume': safe_float(cmoney_data.get('單量', 0)),
                'accumulate_trade_volume': safe_float(cmoney_data.get('總量', 0)),
                'best_bid_price': [safe_float(cmoney_data.get('買價', 0))],
                'best_ask_price': [safe_float(cmoney_data.get('賣價', 0))],
                # 這裡計算不出昨收，UI 會自己去 Cache 抓，傳 0 即可
                'previous_close': 0
            },
            'info': {
                'time': datetime.datetime.now().strftime('%H:%M:%S'),
                'date': datetime.datetime.now().strftime('%Y%m%d')
            }
        }

    def run(self):
        print(f"🚀 [QuoteWorker] CMoney 爬蟲啟動 | 監控數: {len(self.monitoring_stocks)} | 模式: {self.mode}")

        try:
            self.driver = self._init_driver()
            print(">>> 瀏覽器啟動成功！")

            while self.is_running:
                if not self.monitoring_stocks:
                    time.sleep(1)
                    continue

                # 依序抓取每一檔股票
                for stock_id in self.monitoring_stocks:
                    if not self.is_running: break

                    url = f"https://www.cmoney.tw/forum/stock/{stock_id}"
                    try:
                        # print(f"🔍 [QuoteWorker] 正在讀取 {stock_id}...")
                        self.driver.get(url)

                        # 等待網頁載入 (依您範例設定 2~3 秒，為了流暢度設為 2)
                        time.sleep(2)

                        body_text = self.driver.find_element(By.TAG_NAME, "body").text
                        # 擷取前 5000 字元解析即可
                        content_snapshot = body_text[:5000]

                        raw_data = self.parse_cmoney_text(content_snapshot)

                        # 檢查是否有抓到有效成交價
                        if raw_data.get('成交') == '-':
                            print(f"⚠️ [QuoteWorker] {stock_id} 暫無數據 (可能載入不全)")
                        else:
                            # 轉換並發送訊號
                            ui_data = self.convert_to_ui_format(stock_id, raw_data)
                            self.quote_updated.emit({stock_id: ui_data})
                            # print(f"✅ [QuoteWorker] 更新 {stock_id}: {raw_data['成交']}")

                    except Exception as e:
                        print(f"❌ [QuoteWorker] {stock_id} 抓取失敗: {e}")

                    # 每一檔中間休息一下，避免被鎖 (隨機 1~2 秒)
                    if self.is_running:
                        time.sleep(random.uniform(1.0, 2.0))

                # 一輪結束
                if self.mode == 'oneshot':
                    print("🏁 [QuoteWorker] 單次更新完成")
                    self.oneshot_finished.emit()
                    self.is_running = False
                    break

                # 輪詢模式下，每輪休息
                if self.is_running:
                    print("💤 [QuoteWorker] 本輪結束，休息 5 秒...")
                    time.sleep(5)

        except Exception as e:
            print(f"🔥 [QuoteWorker] Driver 發生錯誤: {e}")
        finally:
            if self.driver:
                print("🛑 [QuoteWorker] 關閉 Chrome Driver...")
                self.driver.quit()
                self.driver = None
            print("✅ [QuoteWorker] 執行緒安全退出")