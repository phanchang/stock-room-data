# app.py 核心片段
import pandas as pd
from utils.cache.manager import CacheManager

# --- 設定 GitHub 資料路徑 ---
GITHUB_USER = "phanchang"
GITHUB_REPO = "stock-room-data"
BASE_RAW_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/main/data/cache"

def get_remote_data_url(symbol):
    """根據代號判斷市場並回傳 GitHub Raw URL"""
    market = 'tw' if ('.TW' in symbol or '.TWO' in symbol) else 'us'
    safe_name = symbol.replace('.', '_')
    return f"{BASE_RAW_URL}/{market}/{safe_name}.parquet"

# --- 修改 Dash 的資料載入邏輯 ---
def load_stock_data(symbol):
    """
    取代原本的 downloader.update_single
    現在只負責從遠端讀取已經抓好的 Parquet
    """
    url = get_remote_data_url(symbol)
    try:
        # pandas 可以直接 read_parquet(url)
        df = pd.read_parquet(url)
        print(f"成功從 GitHub 載入 {symbol}")
        return df
    except Exception as e:
        print(f"遠端讀取失敗 {symbol}: {e}")
        # 備援：嘗試讀取本地快取 (開發環境用)
        cm = CacheManager()
        return cm.load(symbol)

# --- 在 Callback 中使用 ---
@app.callback(...)
def update_chart(selected_ticker):
    df = load_stock_data(selected_ticker)
    if df is None: return go.Figure()
    # ... 剩下的繪圖邏輯 5000 行 ...