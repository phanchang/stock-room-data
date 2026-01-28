import sys
from pathlib import Path
import pandas as pd
import time
import os
import requests
import urllib3
from dotenv import load_dotenv

# 1. 禁用 SSL 安全警告 (解決家裡環境的 SSL 報錯)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = PROJECT_ROOT / "data" / "stock_list.csv"


def setup_env():
    """載入環境變數並智慧偵測 Proxy"""
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    proxy = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")
    if proxy:
        try:
            # 測試 Proxy 是否能通
            requests.get("https://isin.twse.com.tw", proxies={'http': proxy, 'https': proxy}, timeout=3, verify=False)
            print(f"🔒 Proxy 偵測成功，正在套用: {proxy}")
            return {'http': proxy, 'https': proxy}
        except:
            print("⚠️ 偵測到 Proxy 設定但連線失敗 (可能在非公司環境)，自動切換為直接連線。")
    else:
        print("🌐 未偵測到 Proxy，使用直接連線模式。")
    return None


def fetch_isin_table(mode_code, market_type, proxies):
    """抓取證交所資料並保持原始英文格式"""
    url = f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode_code}"
    print(f"☁️ 正在下載 {market_type} 資料...")

    try:
        # 使用 requests 處理 SSL 與 Proxy
        response = requests.get(url, proxies=proxies, timeout=15, verify=False)
        response.encoding = 'cp950'

        # 解析 HTML
        dfs = pd.read_html(response.text, header=0)
        if not dfs: return []

        df = dfs[0]
        # 篩選股票 (CFI Code 為 ESVUFR)
        df = df[df['CFICode'] == 'ESVUFR'].copy()

        stock_data = []
        for _, row in df.iterrows():
            raw_parts = str(row.iloc[0]).split()
            if len(raw_parts) >= 2:
                # --- [欄位完全對齊] 標頭: stock_id, name, market, industry ---
                stock_data.append({
                    "stock_id": raw_parts[0].strip(),
                    "name": raw_parts[1].strip(),
                    "market": market_type,
                    "industry": row.iloc[4] if len(row) > 4 else ""
                })

        print(f"✅ 取得 {len(stock_data)} 筆 {market_type} 股票資料")
        return stock_data
    except Exception as e:
        print(f"❌ 下載失敗 {market_type}: {e}")
        return []


def main():
    proxies = setup_env()

    print("🚀 開始更新股票清單 (來源: 證交所 ISIN)")
    print("=" * 60)

    all_stocks = []
    # 1. 抓取上市 (Mode=2) -> TW
    all_stocks.extend(fetch_isin_table(2, "TW", proxies))
    time.sleep(1)
    # 2. 抓取上櫃 (Mode=4) -> TWO
    all_stocks.extend(fetch_isin_table(4, "TWO", proxies))

    if not all_stocks:
        print("❌ 錯誤: 沒有抓取到任何資料，請檢查網路設定。")
        return

    # 3. 輸出 CSV (標頭: stock_id, name, market, industry)
    df = pd.DataFrame(all_stocks)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    print("=" * 60)
    print(f"✨ 任務完成！stock_list.csv 已產出。\n📂 位置: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()