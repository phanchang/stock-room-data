import requests
import pandas as pd
import urllib3
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_env_proxies():
    http_proxy = os.getenv("HTTP_PROXY")
    https_proxy = os.getenv("HTTPS_PROXY")
    if http_proxy:
        return {"http": http_proxy, "https": https_proxy or http_proxy}
    return None


def fetch_3day_margin_final_structure():
    proxies = get_env_proxies()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    # 測試最近 3 個交易日
    test_dates = []
    offset = 1
    while len(test_dates) < 3:
        dt = datetime.now() - timedelta(days=offset)
        if dt.weekday() < 5: test_dates.append(dt)
        offset += 1

    print(f"📡 連線模式: {'Proxy' if proxies else '直接連線'}")
    print(f"🎯 目標：解析 tables 巢狀結構 (上市 tables[1], 上櫃 tables[0])")
    print("-" * 65)

    all_data = []

    for dt in test_dates:
        d_str = dt.strftime('%Y%m%d')
        d_roc = f"{dt.year - 1911}/{dt.strftime('%m/%d')}"

        print(f"📅 {d_str} ... ", end="")

        # --- 1. 上市 (2330) ---
        try:
            url_l = f"https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date={d_str}&selectType=ALL"
            res = requests.get(url_l, headers=headers, proxies=proxies, timeout=15, verify=False).json()

            # 🔥 關鍵修正：從 tables 裡面找資料
            if 'tables' in res and len(res['tables']) > 1:
                target_table = res['tables'][1]  # 通常第二張表是個股明細
                # 簡單確認一下標題有沒有 "融資融券"
                if '融資' in target_table.get('title', ''):
                    df = pd.DataFrame(target_table['data'])
                    # 這裡沒有 fields key 在 data 同級，是分開的，我們直接用索引硬解最穩
                    # 上市索引: [0]=代號, [2]=融資買, [3]=融資賣, [4]=現金償

                    row = df[df[0].str.strip() == '2330']
                    if not row.empty:
                        r = row.iloc[0]
                        # 轉數值 (去除逗號)
                        raw_buy = float(r[2].replace(',', ''))
                        raw_sell = float(r[3].replace(',', ''))
                        raw_cash = float(r[4].replace(',', ''))

                        # 單位判斷 (上市 MI_MARGN 通常是股)
                        unit_factor = 1000 if raw_buy > 5000 else 1
                        net = (raw_buy - raw_sell - raw_cash) / unit_factor

                        all_data.append({'Date': d_str, 'Sid': '2330', 'Net': int(net)})
                        print(f"[上市: {int(net):+}張] ", end="")
                else:
                    print("[上市無目標表] ", end="")
            else:
                print(f"[上市無 tables] ", end="")
        except Exception as e:
            print(f"[上市錯誤] ", end="")

        # --- 2. 上櫃 (5536) ---
        try:
            url_o = f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&d={d_roc}&s=0,asc"
            res = requests.get(url_o, headers=headers, proxies=proxies, timeout=15, verify=False).json()

            # 🔥 關鍵修正：從 tables 裡面找資料
            if 'tables' in res and len(res['tables']) > 0:
                target_table = res['tables'][0]  # 上櫃通常第一張表就是
                df = pd.DataFrame(target_table['data'])
                # 上櫃索引: [0]=代號, [3]=資買, [4]=資賣, [5]=現償

                row = df[df[0].str.strip() == '5536']
                if not row.empty:
                    r = row.iloc[0]
                    buy = float(str(r[3]).replace(',', ''))
                    sell = float(str(r[4]).replace(',', ''))
                    cash = float(str(r[5]).replace(',', ''))

                    # 上櫃通常是張，不需除以 1000
                    net = buy - sell - cash
                    all_data.append({'Date': d_str, 'Sid': '5536', 'Net': int(net)})
                    print(f"[上櫃: {int(net):+}張]")
            else:
                print("[上櫃無 tables]")
        except Exception as e:
            print(f"[上櫃錯誤: {e}]")

        time.sleep(1)

    # --- 輸出報告 ---
    print("\n" + "=" * 65)
    if all_data:
        df_res = pd.DataFrame(all_data)
        print("📊 3日資券淨增減 (單位: 張)")
        # 整理成 Pivot Table 方便看
        pivot = df_res.pivot(index='Date', columns='Sid', values='Net')
        print(pivot.sort_index(ascending=False))

        print("-" * 65)
        if '2330' in pivot.columns:
            print(f"💡 2330 (上市) 3日累計: {pivot['2330'].sum():+} 張")
        if '5536' in pivot.columns:
            print(f"💡 5536 (上櫃) 3日累計: {pivot['5536'].sum():+} 張")
    else:
        print("❌ 無數據。")
    print("=" * 65)


if __name__ == "__main__":
    fetch_3day_margin_final_structure()