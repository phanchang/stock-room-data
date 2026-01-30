import os
import time
import twstock
from datetime import datetime, time as dt_time
from PyQt6.QtCore import QThread, pyqtSignal, QMutex
from dotenv import load_dotenv

load_dotenv()


class QuoteWorker(QThread):
    quote_updated = pyqtSignal(dict)
    status_msg = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.is_running = True
        self.mutex = QMutex()

        self.monitored_sources = {}
        self.monitoring_codes = set()
        self.cache = {}
        self.force_next_run = False

    def set_monitoring_stocks(self, stock_ids: list, source="default"):
        clean_ids = set()
        for sid in stock_ids:
            clean_ids.add(sid.split('_')[0].strip())

        self.mutex.lock()
        try:
            self.monitored_sources[source] = clean_ids
            new_monitoring_set = set()
            for s_ids in self.monitored_sources.values():
                new_monitoring_set.update(s_ids)

            # 🔥 [修正] 只要名單有傳進來，不管有沒有變，都強制喚醒 Worker 跑一次
            # 這樣可以確保切換群組時，一定會立即去抓最新資料
            self.monitoring_codes = new_monitoring_set
            self.force_next_run = True

            print(f"🕵️ [Worker] 收到監控請求 (來源: {source}), 數量: {len(clean_ids)}, 強制執行: ON")

        finally:
            self.mutex.unlock()

    def get_latest_from_cache(self, stock_id):
        clean_id = stock_id.split('_')[0]
        data = self.cache.get(clean_id)
        if data:
            # print(f"⚡ [Worker Cache] 取出 {clean_id} 成功") # Log 太吵可註解
            pass
        return data

    def stop(self):
        self.is_running = False
        self.wait()

    def is_trading_time(self):
        # 🔥 [修正] 只要被標記強制執行，就無視時間限制
        if self.force_next_run:
            return True

        now = datetime.now()
        if now.weekday() > 4: return False
        current_time = now.time()
        return dt_time(8, 45) <= current_time <= dt_time(13, 50)

    def _fix_missing_price(self, raw_data):
        real = raw_data.get('realtime', {})
        latest = None

        # 嘗試解析最新成交價 (排除 '-' 或空字串)
        try:
            val = real.get('latest_trade_price')
            if val and val != '-':
                latest = float(val)
        except:
            pass

        # 補救措施 1: 用收盤價
        if latest is None:
            try:
                val = real.get('close')
                if val and val != '-':
                    latest = float(val)
            except:
                pass

        # 補救措施 2: 用開盤價
        if latest is None:
            try:
                val = real.get('open')
                if val and val != '-':
                    latest = float(val)
            except:
                pass

        # 回填修正後的數值
        if latest is not None:
            real['latest_trade_price'] = str(latest)

        return raw_data

    def run(self):
        self.status_msg.emit("報價引擎啟動...")
        self.force_next_run = True  # 開機強制跑一次

        while self.is_running:
            target_list = []
            self.mutex.lock()
            try:
                target_list = list(self.monitoring_codes)
            finally:
                self.mutex.unlock()

            if not target_list:
                self.msleep(1000)
                continue

            # 檢查是否需要執行
            if not self.is_trading_time():
                self.status_msg.emit("非交易時間 (待機中)")
                for _ in range(10):
                    if not self.is_running or self.force_next_run: break
                    self.msleep(1000)
                continue

            try:
                # print(f"🔄 [Worker] 開始抓取 {len(target_list)} 檔股票...")

                chunk_size = 10
                for i in range(0, len(target_list), chunk_size):
                    if not self.is_running: break
                    batch = target_list[i:i + chunk_size]

                    # 抓取資料
                    data = twstock.realtime.get(batch)

                    if data:
                        processed = {}
                        if isinstance(data, dict):
                            if 'success' in data and not data['success']:
                                pass
                            else:
                                for k, v in data.items():
                                    if k == 'success': continue
                                    if v.get('success'):
                                        fixed_v = self._fix_missing_price(v)
                                        processed[k] = fixed_v
                                        self.cache[k] = fixed_v

                                        # 🔥 [Log] 證明抓到資料了！印出代號、時間、價格
                                        info = v.get('info', {})
                                        real = v.get('realtime', {})
                                        print(
                                            f"✅ [Data] {k} | Time: {info.get('time')} | Price: {real.get('latest_trade_price')}")

                        if processed:
                            self.quote_updated.emit(processed)

                    self.msleep(200)  # 稍微間隔避免被鎖

                # 跑完一輪後，關閉強制旗標 (除非在盤中)
                self.force_next_run = False

            except Exception as e:
                print(f"❌ [Worker Error] {e}")
                self.msleep(1000)

            # 每輪休息
            for _ in range(30):
                if not self.is_running or self.force_next_run: break
                self.msleep(100)