# scripts/update_stock_list.py

import sys
from pathlib import Path
import pandas as pd
import time
import os
from dotenv import load_dotenv  # 🟢 [新增] 匯入 dotenv

# 設定專案根目錄
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# 資料儲存路徑
OUTPUT_PATH = PROJECT_ROOT / "data" / "stock_list.csv"


# 🟢 [新增] Proxy 設定函式
def setup_env():
    """載入環境變數與設定 Proxy"""
    # 載入 .env 檔案
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    # 檢查是否有設定 Proxy (名稱依據你的 .env 設定，通常是 HTTP_PROXY 或 COMPANY_PROXY)
    # 這裡假設你的 .env 裡是用 HTTP_PROXY
    proxy = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")

    if proxy:
        print(f"🔒 偵測到 Proxy 設定，正在套用...")
        # 設定系統環境變數，Pandas/Requests 會自動讀取這些變數
        os.environ['http_proxy'] = proxy
        os.environ['https_proxy'] = proxy
        os.environ['HTTP_PROXY'] = proxy
        os.environ['HTTPS_PROXY'] = proxy
    else:
        print("🌐 未偵測到 Proxy，使用直接連線")


def fetch_isin_table(mode_code: int, market_type: str):
    """
    從證交所 ISIN 網站抓取資料
    """
    url = f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode_code}"
    print(f"☁️ 正在下載{market_type}資料: {url} ...")

    try:
        # 使用 pandas 直接讀取 HTML 表格
        # Pandas 會自動讀取 os.environ 中的 http_proxy 設定
        dfs = pd.read_html(url, encoding='cp950', header=0)

        if not dfs:
            print(f"❌ 無法解析表格: {url}")
            return []

        df = dfs[0]

        # 找出包含 'CFICode' 的那一行當作真正的 header
        mask = df.iloc[:, 5] == 'CFICode'
        if mask.any():
            header_idx = df[mask].index[0]
            df.columns = df.iloc[header_idx]
            df = df.iloc[header_idx + 1:].copy()

        # 篩選條件：CFICode 必須是 'ESVUFR' (股票)
        df = df[df['CFICode'] == 'ESVUFR'].copy()

        stock_data = []

        for _, row in df.iterrows():
            raw_code_name = str(row.iloc[0])
            parts = raw_code_name.split()

            if len(parts) >= 2:
                stock_id = parts[0].strip()
                name = parts[1].strip()

                if stock_id.isdigit():
                    stock_data.append({
                        "stock_id": stock_id,
                        "name": name,
                        "market": market_type,
                        "industry": row.iloc[4] if len(row) > 4 else ""
                    })

        print(f"✅ 取得 {len(stock_data)} 筆 {market_type} 股票資料")
        return stock_data

    except Exception as e:
        print(f"❌ 下載失敗 {market_type}: {e}")
        return []


def main():
    # 🟢 [新增] 執行環境設定
    setup_env()

    print("🚀 開始更新股票清單 (來源: 證交所 ISIN 本國有價證券)")
    print("=" * 60)

    all_stocks = []

    # 1. 抓取上市 (Mode=2) -> TW
    stocks_tw = fetch_isin_table(2, "TW")
    all_stocks.extend(stocks_tw)

    # 休息一下
    time.sleep(2)

    # 2. 抓取上櫃 (Mode=4) -> TWO
    stocks_two = fetch_isin_table(4, "TWO")
    all_stocks.extend(stocks_two)

    if not all_stocks:
        print("❌ 錯誤: 沒有抓取到任何資料，請檢查網路或 Proxy 設定。")
        return

    # 3. 轉為 DataFrame 並存檔
    df = pd.DataFrame(all_stocks)
    df = df.sort_values("stock_id")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    print("=" * 60)
    print(f"🎉 更新完成！")
    print(f"📂 檔案位置: {OUTPUT_PATH}")
    print(f"📊 總筆數: {len(df)}")
    print(f"📈 上市: {len(df[df['market'] == 'TW'])}")
    print(f"📉 上櫃: {len(df[df['market'] == 'TWO'])}")
    print("\n👀 前 5 筆預覽:")
    print(df.head().to_string(index=False))


if __name__ == "__main__":
    main()