# utils/etf/modules/scrapers/capitalfund.py
import os
import requests
import json
import time  # ✨ 新增：用於強制減速防鎖 IP
from datetime import datetime, timedelta
from pathlib import Path


class CapitalFundScraper:
    def __init__(self, fund_code="399", save_dir="data/raw/capitalfund/00982A"):
        self.fund_code = fund_code
        self.save_dir = Path(save_dir)
        self.base_url = "https://www.capitalfund.com.tw/CFWeb/api/etf/buyback"

        # 建立資料夾
        self.save_dir.mkdir(parents=True, exist_ok=True)

    def fetch_data(self, target_date: datetime):
        """取得指定日期的 JSON 資料"""

        # ✨ 關鍵修改 1：Payload 日期校正
        # 為了取得 T 日收盤資料，API Payload 直接帶 T 日的 16:00:00.000Z
        formatted_date = f"{target_date.strftime('%Y-%m-%d')}T16:00:00.000Z"

        payload = {
            "fundId": str(self.fund_code),
            "date": formatted_date
        }

        # 🟢 恢復你原本可用的 Headers
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://www.capitalfund.com.tw/etf/product/detail/{self.fund_code}/portfolio",
            "Origin": "https://www.capitalfund.com.tw"
        }

        # 🟢 恢復你原本可用的 Proxy 讀取邏輯
        proxies = {
            'http': os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy'),
            'https': os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy')
        }
        if not proxies.get('http'): proxies = None

        try:
            response = requests.post(self.base_url, json=payload, headers=headers, proxies=proxies, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"[群益投信] 下載失敗 ({target_date.strftime('%Y-%m-%d')}): {e}")
            return None

    def get_missing_dates(self, lookback_days=30):
        """取得需要補齊的日期列表（排除週末）"""
        missing_dates = []
        today = datetime.now()

        # 往前推算 30 天
        for i in range(lookback_days, -1, -1):
            check_date = today - timedelta(days=i)
            # 排除六日 (5=週六, 6=週日)
            if check_date.weekday() < 5:
                # 配合新版 parser，統一使用 YYYYMMDD.json 格式
                filename = check_date.strftime("%Y%m%d.json")
                if not (self.save_dir / filename).exists():
                    missing_dates.append(check_date)

        return missing_dates

    def fetch_and_save(self):
        """主執行邏輯：檢查缺失日期並下載"""
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [群益投信] 開始檢查...")

        missing_dates = self.get_missing_dates()
        if not missing_dates:
            print(f"[群益投信] 所有資料已是最新 ✅")
            return True

        print(f"[群益投信] 發現 {len(missing_dates)} 個缺失營業日，開始抓取...")

        downloaded_count = 0
        for date_obj in missing_dates:
            date_str = date_obj.strftime("%Y-%m-%d")
            print(f"  - 嘗試抓取: {date_str}")

            data = self.fetch_data(date_obj)

            # 檢查是否真的有資料
            if data and data.get("data") and data.get("data").get("stocks"):
                stocks = data["data"]["stocks"]
                if not stocks:
                    print(f"    ⚠️ API回傳為空陣列")
                    # 即使失敗也要休息，避免頻繁撞牆
                    time.sleep(3)
                    continue

                # ✨ 關鍵修改 2：拆包檢查真實日期，防堵連假/休市假資料
                raw_internal_date = str(stocks[0].get("date1", ""))  # e.g., "2026/2/24 上午 12:00:00"
                try:
                    date_part = raw_internal_date.split(' ')[0]
                    date_parts = date_part.replace('-', '/').split('/')
                    if len(date_parts) == 3:
                        y, m, d = date_parts
                        internal_date_parsed = f"{y}-{m.zfill(2)}-{d.zfill(2)}"
                    else:
                        internal_date_parsed = ""
                except Exception as e:
                    print(f"    ⚠️ 內部日期解析失敗: {e}")
                    time.sleep(3)
                    continue

                # 比對：如果 API 回傳的內部日期 <= 我們查詢的 T 日
                # 代表投信還沒更新，或是遇到連假 (因為 T 日的收盤資料，投信一定會標記為 T+1 或之後)
                if internal_date_parsed <= date_str:
                    print(f"    ⏭️ 拒收 {date_str} (因休市或未更新，API回傳舊檔 {internal_date_parsed})")
                    time.sleep(3)
                    continue

                # 通過所有檢驗，寫入 JSON
                filename = date_obj.strftime("%Y%m%d.json")
                filepath = self.save_dir / filename

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                print(f"    ✅ 成功儲存: {filename}")
                downloaded_count += 1
            else:
                print(f"    ⚠️ 無資料或連線失敗")

            # ✨ 核心防禦：每次迴圈結束，強迫休息 3-4 秒，避免被防火牆當成機器人攻擊
            time.sleep(3)

        print(f"[群益投信] 任務完成，共下載 {downloaded_count} 筆新資料。")


if __name__ == "__main__":
    scraper = CapitalFundScraper()
    scraper.fetch_and_save()