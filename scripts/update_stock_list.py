import sys
from pathlib import Path
import pandas as pd
import time
import os
import requests
import urllib3
import io
from dotenv import load_dotenv

# 1. 禁用 SSL 安全警告
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
            requests.get("https://isin.twse.com.tw", proxies={'http': proxy, 'https': proxy}, timeout=3, verify=False)
            print(f"🔒 Proxy 偵測成功，正在套用: {proxy}")
            return {'http': proxy, 'https': proxy}
        except:
            print("⚠️ Proxy 連線失敗，切換為直接連線。")
    return None


def fetch_isin_table(mode_code, market_type, proxies):
    """
    抓取證交所資料並篩選「純」普通股
    策略：寬鬆 CFI 檢查 + 嚴格代號檢查
    """
    url = f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode_code}"
    print(f"☁️ 正在下載 {market_type} 資料...")

    try:
        response = requests.get(url, proxies=proxies, timeout=15, verify=False)
        response.encoding = 'cp950'

        # 解析 HTML
        dfs = pd.read_html(io.StringIO(response.text), header=0)
        if not dfs: return []

        df = dfs[0]

        # 檢查欄位數量
        if df.shape[1] < 6: return []

        # 重新命名欄位
        # [有價證券代號及名稱, ISIN Code, 上市日, 市場別, 產業別, CFICode, 備註]
        df.columns = ['id_name', 'isin', 'date', 'market', 'industry', 'cfi', 'remark']

        stock_data = []
        for _, row in df.iterrows():
            cfi = str(row['cfi']).strip()
            id_name = str(row['id_name']).strip()

            # 切割代號與名稱
            parts = id_name.split(maxsplit=1)

            if len(parts) == 2:
                stock_id, name = parts

                # ================= 核心篩選邏輯 =================
                # 1. CFI Code 必須是 'ES' 開頭 (Equity Shares, 股票類)
                #    這會保留 ESVUFR, ESVUFA... 但排除 ETF (C開頭), 權證 (W/R開頭)
                is_equity = cfi.startswith('ES')

                # 2. 代號長度必須是 4 碼 (標準普通股)
                #    排除 00878 (5碼), 2881A (5碼特別股), 權證 (6碼)
                is_4_digit = len(stock_id) == 4

                # 3. 排除 TDR (91開頭) 與 ETF (00開頭，如0050)
                is_not_tdr = not stock_id.startswith('91')
                is_not_etf_id = not stock_id.startswith('00')

                if is_equity and is_4_digit and is_not_tdr and is_not_etf_id:
                    stock_data.append({
                        "stock_id": stock_id,
                        "name": name,
                        "market": market_type,
                        "industry": row['industry'] if pd.notna(row['industry']) else ""
                    })

        print(f"✅ 取得 {len(stock_data)} 筆 {market_type} 普通股")
        return stock_data

    except Exception as e:
        print(f"❌ 下載失敗 {market_type}: {str(e)}")
        return []


def main():
    proxies = setup_env()
    print("🚀 開始更新股票清單")
    print("🎯 目標: 上市櫃普通股 (含 KY 股，排除 ETF、TDR、權證、興櫃)")
    print("=" * 60)

    all_stocks = []

    # 1. 上市 (TWSE) - Mode 2
    all_stocks.extend(fetch_isin_table(2, "TWSE", proxies))
    time.sleep(1)

    # 2. 上櫃 (TPEx) - Mode 4
    all_stocks.extend(fetch_isin_table(4, "TPEx", proxies))

    # 不抓取 Mode 5 (興櫃)

    if not all_stocks:
        print("❌ 錯誤: 沒有抓取到任何資料。")
        return

    # 輸出 CSV
    df = pd.DataFrame(all_stocks)

    # 確保目錄存在
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    print("=" * 60)
    print(f"✨ 任務完成！共 {len(df)} 檔股票。")
    print(f"🔎 驗證 TPK-KY (3673): {'3673' in df['stock_id'].values}")  # 程式會自動告訴你有沒有抓到
    print(f"📂 檔案位置: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()