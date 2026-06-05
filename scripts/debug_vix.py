import os
import requests
import pandas as pd
import time
from dotenv import load_dotenv

load_dotenv()


def get_proxies():
    proxies = {"http": os.getenv("HTTP_PROXY"), "https": os.getenv("HTTPS_PROXY")}
    return {k: v for k, v in proxies.items() if v}


def fetch_realtime_bulletin():
    """1. 即時攔截公佈欄 (JSON API) - 完整資料版"""
    url = "https://mops.twse.com.tw/mops/api/home_page/t51sb10"
    payload = {"count": "0", "marketKind": ""}
    headers = {
        "Accept": "*/*", "Content-Type": "application/json",
        "Origin": "https://mops.twse.com.tw",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/148.0.0.0 Safari/537.36"
    }
    try:
        response = requests.post(url, json=payload, headers=headers, proxies=get_proxies(), timeout=15)
        if response.status_code == 200:
            data = response.json()
            if 'result' in data and 'data' in data['result']:
                df = pd.DataFrame(data['result']['data'])
                # 移除 .head(3)，回傳當日所有營業收入資訊以利後續統計
                return df[df['subject'].str.contains("營業收入資訊", na=False)]
    except Exception as e:
        print(f"[!] 公佈欄抓取失敗: {e}")
    return pd.DataFrame()


def fetch_revenue_details(stock_id):
    """2. 透過新版 JSON API 抓取營收明細，並防止 Key 被覆寫"""
    url = "https://mops.twse.com.tw/mops/api/t05st10_ifrs"
    payload = {
        "companyId": str(stock_id), "dataType": "1", "month": "", "year": "", "subsidiaryCompanyId": ""
    }
    headers = {
        "Accept": "*/*", "Content-Type": "application/json",
        "Origin": "https://mops.twse.com.tw",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/148.0.0.0 Safari/537.36"
    }
    try:
        response = requests.post(url, json=payload, headers=headers, proxies=get_proxies(), timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'result' in data and 'data' in data['result']:
                raw_data = data['result']['data']

                rev_dict = {}
                for row in raw_data:
                    k, v = "", ""
                    if isinstance(row, dict):
                        keys = list(row.keys())
                        if len(keys) >= 2:
                            k, v = str(row[keys[0]]).strip(), str(row[keys[1]]).strip()
                    elif isinstance(row, list) and len(row) >= 2:
                        k, v = str(row[0]).strip(), str(row[1]).strip()

                    if k:
                        # [防覆寫機制] 遇到重複的 Key，自動加上序號
                        original_k = k
                        counter = 1
                        while k in rev_dict:
                            k = f"{original_k}_{counter}"
                            counter += 1
                        rev_dict[k] = v
                return rev_dict
    except Exception as e:
        print(f"[!] 明細抓取發生錯誤: {e}")
    return None


if __name__ == "__main__":
    print("=== 開始測試：戰情室即時營收狙擊 (全欄位防漏版) ===")
    print("[*] 正在監控最新公佈欄...\n")

    bulletin_df = fetch_realtime_bulletin()

    if not bulletin_df.empty:
        # --- 新增：資料驗證區塊 ---
        total_count = len(bulletin_df)
        earliest_time = bulletin_df['time'].min()
        latest_time = bulletin_df['time'].max()

        print("-" * 50)
        print(f"[*] 【資料驗證】截至目前共有: {total_count} 筆營收公布資料。")
        print(f"[*] 【資料驗證】今天最早公布時間為: {earliest_time}")
        print(f"[*] 【資料驗證】今天最新公布時間為: {latest_time}")
        print("-" * 50)

        # 為了避免頻繁請求導致被鎖 IP，驗證完畢後依然只取前 3 筆進行明細測試
        test_df = bulletin_df.head(3)

        for _, row in test_df.iterrows():
            stock_id = row['companyId']
            stock_name = row['companyAbbreviation']
            pub_time = row['time']

            print(f"[*] [{pub_time}] 正在獲取 {stock_id} {stock_name} 的營收明細...")
            time.sleep(1.5)

            details = fetch_revenue_details(stock_id)

            if details:
                print(f"      -> 成功取得 {len(details)} 個欄位，完整資料如下：")
                for k, v in details.items():
                    # 如果遇到超長備註，稍微截斷以保持版面乾淨
                    display_v = v if len(v) < 50 else v[:47] + "..."
                    print(f"         {k}: {display_v}")
            else:
                print(f"      -> [!] 獲取明細失敗")
            print("-" * 50)
    else:
        print("[*] 目前公佈欄無營業收入資訊。")

    print("=== 測試結束 ===")