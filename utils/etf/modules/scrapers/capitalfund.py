# utils/etf/modules/scrapers/capitalfund.py
import os
import requests
import json
import time
from datetime import datetime, timedelta, timezone  # ✨ 加入 timezone
from pathlib import Path


class CapitalFundScraper:
    def __init__(self, fund_code="399", save_dir="data/raw/capitalfund/00982A"):
        self.fund_code = fund_code
        self.save_dir = Path(save_dir)
        self.base_url = "https://www.capitalfund.com.tw/CFWeb/api/etf/buyback"
        self.save_dir.mkdir(parents=True, exist_ok=True)

    def fetch_data(self, target_date: datetime):
        # 送出 T 日的 16:00:00.000Z 查詢
        formatted_date = f"{target_date.strftime('%Y-%m-%d')}T16:00:00.000Z"
        payload = {"fundId": str(self.fund_code), "date": formatted_date}

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://www.capitalfund.com.tw/etf/product/detail/{self.fund_code}/portfolio",
            "Origin": "https://www.capitalfund.com.tw"
        }

        proxies = {
            'http': os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy'),
            'https': os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy')
        }
        if not proxies.get('http'): proxies = None

        try:
            response = requests.post(self.base_url, json=payload, headers=headers, proxies=proxies, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"    ❌ 連線錯誤/逾時 ({target_date.strftime('%Y-%m-%d')}): {e}")
            return None

    def get_missing_dates(self, lookback_days=30):
        # ✨ 這行就是剛剛不小心消失的關鍵！必須先建立空陣列
        missing_dates = []

        # ✨ 強制台灣時區
        tw_tz = timezone(timedelta(hours=8))
        today = datetime.now(tw_tz)

        # 破譯修正：每一天都查！不再排除六日，因為 API 會把星期五跟連假前的資料藏在假日的查詢結果裡
        for i in range(lookback_days, -1, -1):
            check_date = today - timedelta(days=i)
            missing_dates.append(check_date)

        return missing_dates

    def fetch_and_save(self):
        # ✨ 強制台灣時區
        tw_tz = timezone(timedelta(hours=8))
        now = datetime.now(tw_tz)

        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [群益投信] 開始檢查...")

        missing_dates = self.get_missing_dates()
        if not missing_dates:
            print(f"[群益投信] 所有資料已是最新 ✅")
            return True

        print(f"[群益投信] 掃描過去 {len(missing_dates)} 天的每一天 (含假日)，尋找隱藏資料...")

        downloaded_count = 0
        for date_obj in missing_dates:
            date_str = date_obj.strftime("%Y-%m-%d")
            print(f"  - 送出查詢: {date_str}")

            data = self.fetch_data(date_obj)

            # ✨ 安全防護：避免 NoneType 報錯
            raw_data = data.get("data") if isinstance(data, dict) else None

            if raw_data and isinstance(raw_data, dict) and raw_data.get("pcf") and raw_data.get("stocks"):

                real_trade_date = raw_data["pcf"].get("date2", "")

                if not real_trade_date:
                    print(f"    ⚠️ 無法取得真實交易日 (date2)，跳過。")
                    time.sleep(3)
                    continue

                filename = real_trade_date.replace("-", "") + ".json"
                filepath = self.save_dir / filename

                # 依賴真實交易日防呆
                if filepath.exists():
                    print(f"    ⏭️ 重複/已存：API 吐出的真實交易日 {real_trade_date} 已經有了。")
                    time.sleep(3)
                    continue

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                print(f"    ✅ 成功撈回隱藏資料: {filename} (隱藏在 {date_str} 的查詢中)")
                downloaded_count += 1
            else:
                print(f"    ⚠️ 此日查詢無資料 (正常現象，代表當日無生效資料)")

            # 乖乖睡覺防 Ban
            time.sleep(3)

        print(f"[群益投信] 掃描完成！共補齊 {downloaded_count} 筆真實資料。")


if __name__ == "__main__":
    scraper = CapitalFundScraper()
    scraper.fetch_and_save()