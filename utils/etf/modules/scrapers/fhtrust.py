# modules/fhtrust.py
import requests
from datetime import datetime, timedelta
import os


class FHTrustScraper:
    def __init__(self, fund_code, save_dir, proxy="10.160.3.88"):
        self.fund_code = fund_code
        self.save_dir = save_dir
        self.proxy = proxy
        self.base_url = 'https://www.fhtrust.com.tw/api/assetsExcel'

    def fetch_excel(self, date_str):
        """下載 Excel 檔案

        Args:
            date_str: 日期字串，格式 YYYYMMDD

        Returns:
            tuple: (content, success) - Excel 內容和是否成功
        """
        url = f'{self.base_url}/{self.fund_code}/{date_str}'

        # --- 修改這裡：自動抓取系統環境變數的 Proxy ---
        proxies = {
            'http': os.environ.get('HTTP_PROXY'),
            'https': os.environ.get('HTTPS_PROXY')
        }
        if not proxies['http']: proxies = None
        # ------------------------------------------
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': f'https://www.fhtrust.com.tw/ETF/etf_detail/{self.fund_code}'
        }

        try:
            response = requests.get(url, headers=headers, proxies=proxies, timeout=30)
            response.raise_for_status()

            # 檢查 Content-Type
            content_type = response.headers.get('Content-Type', '')

            # 如果是 JSON，表示沒有資料
            if 'json' in content_type.lower():
                return None, False

            # 檢查是否真的是 Excel 檔案
            if 'excel' in content_type.lower() or 'spreadsheet' in content_type.lower():
                return response.content, True

            # 檢查檔案大小（太小可能是錯誤訊息）
            if len(response.content) < 1024:  # 小於 1KB
                return None, False

            # 如果都通過，假設是 Excel
            return response.content, True

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None, False
            print(f"[復華投信] HTTP 錯誤: {e}")
            return None, False
        except Exception as e:
            print(f"[復華投信] 下載失敗: {e}")
            return None, False

    def save_excel(self, content, date_str):
        """儲存 Excel 檔案"""
        os.makedirs(self.save_dir, exist_ok=True)

        # 將 YYYYMMDD 轉換為 YYYY_MM_DD
        formatted_date = f"{date_str[:4]}_{date_str[4:6]}_{date_str[6:8]}"
        filename = f'{formatted_date}.xlsx'
        filepath = os.path.join(self.save_dir, filename)

        with open(filepath, 'wb') as f:
            f.write(content)

        return filepath

    def file_exists(self, date_str):
        """檢查指定日期的檔案是否存在"""
        formatted_date = f"{date_str[:4]}_{date_str[4:6]}_{date_str[6:8]}"
        filename = f'{formatted_date}.xlsx'
        filepath = os.path.join(self.save_dir, filename)
        return os.path.exists(filepath)

    def get_last_data_date(self):
        """從目錄中的檔案取得最後一筆資料日期"""
        if not os.path.exists(self.save_dir):
            return None

        files = [f for f in os.listdir(self.save_dir) if f.endswith('.xlsx')]
        if not files:
            return None

        # 提取日期並排序
        dates = []
        for filename in files:
            try:
                # 檔案名稱格式: YYYY_MM_DD.xlsx
                date_str = filename.replace('.xlsx', '').replace('_', '')
                if len(date_str) == 8 and date_str.isdigit():
                    date_obj = datetime.strptime(date_str, '%Y%m%d')
                    dates.append(date_obj)
            except:
                continue

        return max(dates) if dates else None

    def get_missing_dates(self, lookback_days=30):
        """
        取得需要補齊的日期列表（排除週末）

        Args:
            lookback_days: 最多往前追溯幾天

        Returns:
            list: 需要補齊的日期列表 (datetime objects)
        """
        last_date = self.get_last_data_date()
        today = datetime.now().date()

        # 如果沒有任何資料，從往前 lookback_days 天開始
        if last_date is None:
            start_date = datetime.now() - timedelta(days=lookback_days)
        else:
            start_date = last_date + timedelta(days=1)

        missing_dates = []
        current_date = start_date

        while current_date.date() <= today:
            # 只加入工作日（週一到週五）
            if current_date.weekday() < 5:
                missing_dates.append(current_date)
            current_date += timedelta(days=1)

        return missing_dates

    def fetch_and_save(self):
        """
        抓取並儲存

        邏輯說明:
        1. 檢查缺失的日期
        2. 逐一嘗試下載每個缺失日期的資料
        3. 最後檢查今天的資料是否已取得
        """
        now = datetime.now()
        today_str = now.strftime('%Y%m%d')
        today_date = now.date()

        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [復華投信] 開始下載...")

        # 取得需要補齊的日期
        missing_dates = self.get_missing_dates()

        if not missing_dates:
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [復華投信] 所有資料已是最新 ✅")
            return True

        print(f"[復華投信] 發現 {len(missing_dates)} 個缺失日期需要補齊")

        # 顯示需要補齊的日期（最多顯示前5個）
        for i, date in enumerate(missing_dates[:5]):
            print(f"  - {date.strftime('%Y-%m-%d')} ({date.strftime('%A')})")
        if len(missing_dates) > 5:
            print(f"  ... 還有 {len(missing_dates) - 5} 個日期")

        # 逐一嘗試下載
        downloaded_count = 0
        today_downloaded = False

        for date in missing_dates:
            date_str = date.strftime('%Y%m%d')
            date_formatted = date.strftime('%Y-%m-%d')

            # 檢查檔案是否已存在（雙重確認）
            if self.file_exists(date_str):
                print(f"[復華投信] {date_formatted} 資料已存在，跳過")
                if date.date() == today_date:
                    today_downloaded = True
                continue

            # 嘗試下載
            print(f"[復華投信] 嘗試下載: {date_formatted}")
            content, success = self.fetch_excel(date_str)

            if success and content:
                # 儲存檔案
                try:
                    saved_path = self.save_excel(content, date_str)
                    file_size = len(content) / 1024

                    print(f"[復華投信] ✅ 成功下載: {date_formatted}")
                    print(f"  - 大小: {file_size:.2f} KB")
                    print(f"  - 檔案: {saved_path}")

                    downloaded_count += 1

                    if date.date() == today_date:
                        today_downloaded = True

                except Exception as e:
                    print(f"[復華投信] ❌ 儲存失敗 ({date_formatted}): {e}")
            else:
                print(f"[復華投信] ⚠️ 無資料: {date_formatted}")

        # 總結
        print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] [復華投信] 下載完成")
        print(f"  - 成功下載: {downloaded_count} 個檔案")

        # 判斷是否成功
        if today_downloaded:
            print(f"  - 狀態: ✅ 今日資料已取得")
            return True
        elif downloaded_count > 0:
            print(f"  - 狀態: ⚠️ 已補齊部分資料，但尚未更新到今日")
            return False
        else:
            print(f"  - 狀態: ❌ 尚未有新資料可下載")
            return False