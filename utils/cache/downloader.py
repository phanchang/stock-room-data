"""
資料下載器模組
負責從 yfinance 下載股票資料並更新快取
(已修正：強制不復權 + 支援強制更新指令傳遞)
"""

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import json
import os
import sys

# 處理相對引用問題
try:
    from .manager import CacheManager
except ImportError:
    # 直接執行時，加入父目錄到路徑
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent))
    from manager import CacheManager


class StockDownloader:
    """股票資料下載器"""

    def __init__(self, cache_manager: Optional[CacheManager] = None,
                 proxy: Optional[str] = None):
        """
        初始化下載器
        """
        self.cache = cache_manager or CacheManager()
        self.logger = self.cache.logger

        # 設定 proxy
        self.proxy = proxy or self._get_proxy_from_env()
        if self.proxy:
            self._setup_proxy()
            self.logger.info(f"使用 Proxy: {self.proxy}")
        else:
            self.logger.info("未使用 Proxy（直連）")

    def _get_proxy_from_env(self) -> Optional[str]:
        """從環境變數或 .env 讀取 proxy 設定"""
        proxy = os.environ.get('STOCK_PROXY') or os.environ.get('HTTP_PROXY')

        if not proxy:
            env_file = '.env'
            if os.path.exists(env_file):
                try:
                    with open(env_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip()
                            if line.startswith('STOCK_PROXY='):
                                proxy = line.split('=', 1)[1].strip()
                                break
                except Exception as e:
                    self.logger.debug(f"讀取 .env 失敗: {e}")

        return proxy

    def _setup_proxy(self):
        """設定 yfinance 的 proxy"""
        import requests
        os.environ['HTTP_PROXY'] = self.proxy
        os.environ['HTTPS_PROXY'] = self.proxy

    def download(self, symbol: str, start: Optional[str] = None,
                 period: str = '500d') -> Optional[pd.DataFrame]:
        """
        從 yfinance 下載資料 (修正版：強制抓取原始股價)
        """
        try:
            download_symbol = symbol
            if '.TW' in symbol or '.TWO' in symbol:
                download_symbol = symbol.replace('_', '.')

            self.logger.debug(f"下載代號: {download_symbol}")

            ticker = yf.Ticker(download_symbol)

            # 🔥 關鍵修正 1：強制不復權，解決小數點問題
            history_kwargs = {
                'auto_adjust': False,  # 關閉自動復權 (關鍵!)
                'actions': True        # 保留除權息資訊
            }

            if start:
                history_kwargs['start'] = start
            else:
                history_kwargs['period'] = period

            df = ticker.history(**history_kwargs)

            if df.empty:
                self.logger.warning(f"無資料: {symbol}")
                return None

            # 🔥 關鍵修正 2：移除 Adj Close 並統一欄位名稱
            if 'Adj Close' in df.columns:
                df = df.drop(columns=['Adj Close'])

            available_cols = df.columns.tolist()
            col_mapping = {
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            }

            selected_cols = [col for col in col_mapping.keys() if col in available_cols]
            df = df[selected_cols].copy()
            df.columns = [col_mapping[col] for col in selected_cols]

            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)

            # 確保 Volume 是整數
            if 'volume' in df.columns:
                df['volume'] = df['volume'].fillna(0).astype(int)

            self.logger.info(f"下載 {symbol}: {len(df)} 筆")
            return df

        except Exception as e:
            self.logger.error(f"下載失敗 {symbol}: {e}")
            return None

    def update_single(self, symbol: str, force: bool = False, check_today: bool = True) -> Optional[pd.DataFrame]:
        """
        更新單一股票 (支援強制重抓邏輯)
        """
        self.logger.info(f"{'=' * 50}")
        self.logger.info(f"更新: {symbol}")

        # 載入現有資料
        existing_df = self.cache.load(symbol)

        # 情境1: 首次下載或強制更新
        if existing_df is None or force:
            reason = "強制更新" if force else "首次下載"
            self.logger.info(f"{reason}，下載完整資料...")
            df = self.download(symbol, period='500d')

        else:
            last_date = existing_df.index[-1]
            today = pd.Timestamp.now().normalize()
            missing_days = (today - last_date).days

            self.logger.info(f"最後更新: {last_date.date()}, 缺失 {missing_days} 天")

            if check_today and missing_days > 0:
                weekday = today.weekday()
                if weekday == 5:
                    expected_last_date = today - pd.Timedelta(days=1)
                elif weekday == 6:
                    expected_last_date = today - pd.Timedelta(days=2)
                else:
                    import datetime
                    now = datetime.datetime.now()
                    if now.hour < 14 or (now.hour == 14 and now.minute < 30):
                        expected_last_date = today - pd.Timedelta(days=1)
                    else:
                        expected_last_date = today

                if last_date >= expected_last_date:
                    self.logger.info(f"✓ 資料已是最新 (最後日期: {last_date.date()}, 預期: {expected_last_date.date()})")
                    return existing_df
                else:
                    self.logger.info(f"需要更新 (預期: {expected_last_date.date()})")

            if missing_days <= 30:
                self.logger.info(f"增量更新 {missing_days} 天...")
                start_date = (last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                new_df = self.download(symbol, start=start_date)

                if new_df is not None and not new_df.empty:
                    df = self.cache.merge_data(existing_df, new_df)
                    self.logger.info(f"✓ 新增 {len(new_df)} 筆資料")
                else:
                    self.logger.info("無新資料（可能是非交易日）")
                    df = existing_df
            else:
                self.logger.warning(f"缺失過久 ({missing_days} 天)，重新下載...")
                df = self.download(symbol, period='500d')

        if df is not None:
            if existing_df is None or not existing_df.equals(df):
                if self.cache.save(symbol, df):
                    return df
            else:
                self.logger.info("✓ 資料無變化，不更新檔案")
                return df

        return None

    # 🔥 關鍵修正 3：加入 force 參數並傳遞給單檔更新
    def batch_update(self, symbols: List[str], max_workers: int = 3,
                     delay: float = 0.5, force: bool = False) -> Dict[str, List[str]]:
        """
        批次更新多檔股票 (支援傳遞 force 指令)
        """
        self.logger.info(f"\n{'=' * 60}")
        self.logger.info(f"批次更新開始: {len(symbols)} 檔股票")
        self.logger.info(f"平行數量: {max_workers}")
        self.logger.info(f"強制模式: {'是' if force else '否'}") # 👈 檢查這裡
        self.logger.info(f"{'=' * 60}\n")

        results = {'success': [], 'failed': []}
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for symbol in symbols:
                # 🔥 這裡把 force 傳遞下去
                future = executor.submit(self.update_single, symbol, force=force)
                futures[future] = symbol
                time.sleep(delay)

            for i, future in enumerate(as_completed(futures), 1):
                symbol = futures[future]
                try:
                    result = future.result()
                    if result is not None:
                        results['success'].append(symbol)
                        self.logger.info(f"[{i}/{len(symbols)}] ✓ {symbol}")
                    else:
                        results['failed'].append(symbol)
                except Exception as e:
                    self.logger.error(f"[{i}/{len(symbols)}] ✗ {symbol}: {e}")
                    results['failed'].append(symbol)

        elapsed = time.time() - start_time
        self._save_update_log(results, elapsed)
        return results

    # 🔥 關鍵修正 4：讓最外層函式也支援傳遞 force
    def batch_update_with_progress(self, symbols: List[str],
                                   batch_size: int = 200,
                                   max_workers: int = 3,
                                   force: bool = False) -> Dict[str, List[str]]:
        """
        分批次更新 (支援傳遞 force 指令)
        """
        total_results = {'success': [], 'failed': []}
        total_batches = (len(symbols) + batch_size - 1) // batch_size

        self.logger.info(f"\n{'=' * 60}")
        self.logger.info(f"分批更新開始 (強制模式: {'是' if force else '否'})") # 👈 檢查這裡
        self.logger.info(f"{'=' * 60}\n")

        for i in range(0, len(symbols), batch_size):
            batch_num = i // batch_size + 1
            batch = symbols[i:i + batch_size]

            self.logger.info(f"\n📦 批次 {batch_num}/{total_batches}")
            # 🔥 這裡把 force 傳遞下去
            results = self.batch_update(batch, max_workers=max_workers, force=force)

            total_results['success'].extend(results['success'])
            total_results['failed'].extend(results['failed'])

            if batch_num < total_batches:
                time.sleep(10)

        return total_results

    def _save_update_log(self, results: Dict[str, List[str]], elapsed: float):
        """儲存更新日誌 (略)"""
        # ... (保持原樣) ...
        pass

    def _update_summary(self):
        """更新統計 (略)"""
        # ... (保持原樣) ...
        pass