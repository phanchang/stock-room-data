import os
import requests
import json
from datetime import datetime, timedelta
from pathlib import Path


class CapitalFundScraper:
    def __init__(self, fund_code="399", save_dir="data/raw/capitalfund/00982A", proxy=None):
        self.fund_code = fund_code
        self.save_dir = Path(save_dir)
        self.proxy = proxy
        self.base_url = "https://www.capitalfund.com.tw/CFWeb/api/etf/buyback"
        self.save_dir.mkdir(parents=True, exist_ok=True)

    def fetch_data(self, target_date: datetime):
        utc_date = target_date - timedelta(hours=8)
        formatted_date = utc_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        payload = {"fundId": str(self.fund_code), "date": formatted_date}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*"
        }

        # Proxy 邏輯
        proxies = {'http': os.environ.get('HTTP_PROXY'), 'https': os.environ.get('HTTPS_PROXY')}
        if self.proxy:
            proxies = {'http': f"http://{self.proxy}", 'https': f"http://{self.proxy}"}
        if not proxies.get('http'): proxies = None

        try:
            response = requests.post(self.base_url, json=payload, headers=headers, proxies=proxies, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return None

    def get_missing_dates(self, lookback_days=30):
        missing_dates = []
        today = datetime.now()
        for i in range(lookback_days, -1, -1):
            check_date = today - timedelta(days=i)
            if check_date.weekday() < 5:
                # 配合 scheduler，檔名改為 YYYYMMDD.json
                filename = check_date.strftime("%Y%m%d.json")
                if not (self.save_dir / filename).exists():
                    missing_dates.append(check_date)
        return missing_dates

    def fetch_and_save(self):
        now = datetime.now()
        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [群益投信] 開始下載...")
        missing_dates = self.get_missing_dates()

        if not missing_dates:
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [群益投信] 所有資料已是最新 ✅")
            return True

        downloaded_count = 0
        for date_obj in missing_dates:
            date_str = date_obj.strftime("%Y-%m-%d")
            data = self.fetch_data(date_obj)

            if data and data.get("data") and data["data"].get("stocks"):
                filename = date_obj.strftime("%Y%m%d.json")
                filepath = self.save_dir / filename
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"[群益投信] ✅ 成功下載: {date_str}")
                downloaded_count += 1
            else:
                print(f"[群益投信] ⚠️ 無資料: {date_str}")

        return downloaded_count > 0