import pandas as pd
import requests
from pathlib import Path
import time


class DataDownloader:
    def __init__(self):
        self.base_url = "https://raw.githubusercontent.com/phanchang/stock-room-data/main"
        self.cache_dir = Path("data/cache")
        self.tw_cache = self.cache_dir / "tw"

        self.tw_cache.mkdir(parents=True, exist_ok=True)

        # 🔥 [Cache] 記憶體快取：{ "2330": ("TW", df_obj), ... }
        # 這樣第二次存取同一檔股票時，連硬碟都不用讀，直接從 RAM 拿
        self.memory_cache = {}

    def update_stock_list_from_github(self):
        """ 從 GitHub 下載最新的股票清單 """
        url = f"{self.base_url}/data/stock_list.csv"
        local_path = Path("data/stock_list.csv")
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                return True
        except Exception as e:
            print(f"❌ 清單下載失敗: {e}")
        return False

    def discover_market(self, stock_id):
        """
        自動偵測市場 (TW vs TWO) + 記憶體快取
        優先順序: RAM -> Disk -> Network
        """
        # 1. 🔥 第一層：檢查記憶體快取 (最快)
        if stock_id in self.memory_cache:
            # print(f"⚡ [RAM] 秒讀快取: {stock_id}")
            return self.memory_cache[stock_id]

        # 2. 第二層：檢查本地硬碟 (Parquet)
        for mkt in ["TW", "TWO"]:
            local_path = self.tw_cache / f"{stock_id}_{mkt}.parquet"
            if local_path.exists():
                try:
                    df = pd.read_parquet(local_path)
                    # 存入記憶體快取
                    self.memory_cache[stock_id] = (mkt, df)
                    return mkt, df
                except:
                    pass

                    # 3. 第三層：雲端試錯 (最慢，只在第一次發生)
        # 先試 TW
        df = self._download_parquet(stock_id, "TW")
        if df is not None:
            self.memory_cache[stock_id] = ("TW", df)
            return "TW", df

        # 再試 TWO
        df = self._download_parquet(stock_id, "TWO")
        if df is not None:
            self.memory_cache[stock_id] = ("TWO", df)
            return "TWO", df

        # 真的找不到
        return "TW", None

    def update_kline_data(self, stock_id, market):
        """ 指定市場下載 (舊相容模式) """
        # 1. 檢查記憶體
        if stock_id in self.memory_cache:
            cached_market, cached_df = self.memory_cache[stock_id]
            if cached_market == market:
                return cached_df

        local_path = self.tw_cache / f"{stock_id}_{market}.parquet"

        # 2. 檢查硬碟
        if local_path.exists():
            try:
                df = pd.read_parquet(local_path)
                self.memory_cache[stock_id] = (market, df)  # 更新快取
                return df
            except:
                pass

        # 3. 下載
        df = self._download_parquet(stock_id, market)
        if df is not None:
            self.memory_cache[stock_id] = (market, df)  # 更新快取

        return df

    def _download_parquet(self, stock_id, market):
        """ 內部方法：執行實際下載 """
        filename = f"{stock_id}_{market}.parquet"
        url = f"{self.base_url}/data/cache/tw/{filename}"
        local_path = self.tw_cache / filename

        try:
            # print(f"☁️ [Network] 下載中: {filename}")
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                return pd.read_parquet(local_path)
        except Exception as e:
            pass

        return None