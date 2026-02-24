# modules/ezmoney.py
import requests
from bs4 import BeautifulSoup
import json
import html
import pandas as pd
from datetime import datetime, timezone, timedelta
import os


class EZMoneyScraper:
    def __init__(self, fund_code, save_dir, proxy="10.160.3.88"):
        self.fund_code = fund_code
        self.save_dir = save_dir
        self.proxy = proxy
        self.base_url = 'https://www.ezmoney.com.tw/ETF/Fund/Info'

    def fetch_data(self):
        """抓取資料"""
        url = f'{self.base_url}?fundCode={self.fund_code}'

        # --- 修改這裡：自動抓取系統環境變數的 Proxy ---
        proxies = {
            'http': os.environ.get('HTTP_PROXY'),
            'https': os.environ.get('HTTPS_PROXY')
        }
        # 如果環境變數沒設定，設定為 None 讓 requests 直連
        if not proxies['http']: proxies = None
        # ------------------------------------------
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        try:
            response = requests.get(url, headers=headers, proxies=proxies, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            data_asset_div = soup.find('div', {'id': 'DataAsset'})

            if not data_asset_div:
                return None

            data_content = data_asset_div.get('data-content')
            if not data_content:
                return None

            decoded_data = html.unescape(data_content)
            data = json.loads(decoded_data)

            return data

        except Exception as e:
            print(f"[EZMoney] 抓取失敗: {e}")
            return None

    def parse_data(self, data):
        """解析資料"""
        nav_info = {}
        holdings = []
        data_date = None

        for item in data:
            # 基本資料
            if item['Group'] == '1':
                nav_info[item['AssetName']] = {
                    '值': item['Value'],
                    '幣別': item.get('MoneyType', ''),
                    '更新時間': item.get('EditDate', '')
                }
                if not data_date and item.get('EditDate'):
                    data_date = item['EditDate'].split('T')[0]

            # 持股明細
            if item['Group'] == '2' and item.get('Details'):
                for detail in item['Details']:
                    holdings.append({
                        '股票代碼': detail['DetailCode'],
                        '股票名稱': detail['DetailName'],
                        '持股數': detail['Share'],
                        '市值': detail['Amount'],
                        '淨值佔比(%)': detail['NavRate'],
                        '更新日期': detail.get('TranDate', '').split('T')[0]
                    })

        return nav_info, holdings, data_date

    def save_to_excel(self, nav_info, holdings, data_date):
        """儲存為 Excel"""
        os.makedirs(self.save_dir, exist_ok=True)

        filename = data_date.replace('-', '_') + '.xlsx'
        filepath = os.path.join(self.save_dir, filename)

        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            nav_df = pd.DataFrame([
                {'項目': k, '數值': v['值'], '幣別': v['幣別'], '更新時間': v['更新時間']}
                for k, v in nav_info.items()
            ])
            nav_df.to_excel(writer, sheet_name='基金淨值', index=False)

            if holdings:
                holdings_df = pd.DataFrame(holdings)
                holdings_df.to_excel(writer, sheet_name='持股明細', index=False)

        return filepath

    def file_exists(self, data_date):
        """檢查指定日期的檔案是否存在"""
        filename = data_date.replace('-', '_') + '.xlsx'
        filepath = os.path.join(self.save_dir, filename)
        return os.path.exists(filepath)

    def fetch_and_save(self):
        """
        抓取並儲存
        修正 Bug: 嚴格比對資料內部的 data_date，防止假日抓到舊資料卻重複儲存
        """
        # ✨ 強制台灣時區
        tw_tz = timezone(timedelta(hours=8))
        now = datetime.now(tw_tz)
        today_str = now.strftime('%Y-%m-%d')
        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [統一投信] 開始抓取...")

        # 抓取資料
        data = self.fetch_data()
        if not data:
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [統一投信] 抓取失敗")
            return False

        nav_info, holdings, data_date = self.parse_data(data)

        if not data_date:
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [統一投信] 無法取得資料日期")
            return False

        # --- Bug 修正：統一日期格式為 YYYY-MM-DD 以利比對 ---
        data_date = pd.to_datetime(data_date).strftime('%Y-%m-%d')

        # 檢查抓到的資料日期檔案是否已存在 (依據資料內容日期判斷)
        if self.file_exists(data_date):
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [統一投信] 資料已存在: {data_date}")

            # 如果抓到的是今天的資料且已存在，視為成功
            if data_date == today_str:
                print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [統一投信] 今日資料已是最新 ✅")
                return True
            else:
                # 抓到的是舊資料（例如假日時抓到前一天的資料）且已存在
                print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [統一投信] 伺服器尚未更新到今日資料，目前仍為 {data_date}")
                return False

        # 檔案不存在，儲存資料
        try:
            # save_to_excel 內部應使用 data_date 作為檔名 (如 2026_02_11.xlsx)
            saved_path = self.save_to_excel(nav_info, holdings, data_date)

            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [統一投信] 成功儲存資料")
            print(f"  - 日期: {data_date}")
            print(f"  - 淨值: {nav_info.get('每單位淨值', {}).get('值', 'N/A')}")
            print(f"  - 持股: {len(holdings)} 檔")
            print(f"  - 檔案: {saved_path}")

            # 判斷是否為今日資料
            if data_date == today_str:
                print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [統一投信] ✅ 今日資料已取得")
                return True
            else:
                print(
                    f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [統一投信] ⚠️ 已補儲存 {data_date} 資料，但伺服器尚未更新今日資料")
                return False

        except Exception as e:
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [統一投信] 儲存失敗: {e}")
            return False