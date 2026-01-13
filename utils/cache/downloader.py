"""
資料下載器模組
負責從 yfinance 下載股票資料並更新快取
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

        Args:
            cache_manager: 快取管理器實例（若無則自動建立）
            proxy: 代理伺服器（如 'http://10.160.3.88:8080'）
                   若為 None，會自動從環境變數讀取
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
        """
        從環境變數或 .env 讀取 proxy 設定

        優先順序：
        1. STOCK_PROXY 環境變數
        2. HTTP_PROXY 環境變數
        3. .env 檔案中的 STOCK_PROXY

        Returns:
            proxy URL 或 None
        """
        # 從環境變數讀取
        proxy = os.environ.get('STOCK_PROXY') or os.environ.get('HTTP_PROXY')

        if not proxy:
            # 嘗試從 .env 讀取
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
        # yfinance 使用 requests，需設定 session proxy
        import requests

        # 設定環境變數（讓 yfinance 內部的 requests 使用）
        os.environ['HTTP_PROXY'] = self.proxy
        os.environ['HTTPS_PROXY'] = self.proxy

        # 也可以直接修改 yfinance 的 session（更可靠）
        # 但這需要在每次下載時處理，所以用環境變數比較簡單

    def download(self, symbol: str, start: Optional[str] = None,
                 period: str = '500d') -> Optional[pd.DataFrame]:
        """
        從 yfinance 下載資料

        Args:
            symbol: 股票代號
            start: 起始日期（YYYY-MM-DD）
            period: 下載期間（如 '500d', '2y'）

        Returns:
            DataFrame 或 None
        """
        try:
            # 台股代號轉換：移除 .TW / .TWO 後綴
            # yfinance 對台股的格式要求是純數字 + .TW
            download_symbol = symbol

            # 確保台股格式正確
            if '.TW' in symbol or '.TWO' in symbol:
                # 2330.TW -> 2330.TW (保持)
                # 但如果是 2330_TW (從檔名來的)，要轉回 2330.TW
                download_symbol = symbol.replace('_', '.')

            self.logger.debug(f"下載代號: {download_symbol}")

            ticker = yf.Ticker(download_symbol)

            if start:
                df = ticker.history(start=start)
            else:
                df = ticker.history(period=period)

            if df.empty:
                self.logger.warning(f"無資料: {symbol}")
                return None

            # 只保留必要欄位並重新命名
            available_cols = df.columns.tolist()

            # yfinance 可能回傳的欄位名稱
            col_mapping = {
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            }

            # 只選取存在的欄位
            selected_cols = [col for col in col_mapping.keys() if col in available_cols]
            df = df[selected_cols].copy()
            df.columns = [col_mapping[col] for col in selected_cols]

            # 移除時區資訊（統一為 naive datetime）
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)

            self.logger.info(f"下載 {symbol}: {len(df)} 筆")
            return df

        except Exception as e:
            self.logger.error(f"下載失敗 {symbol}: {e}")
            return None

    def update_single(self, symbol: str, force: bool = False, check_today: bool = True) -> Optional[pd.DataFrame]:
        """
        更新單一股票（智慧增量更新）

        處理情境：
        1. 首次下載（快取不存在）
        2. 正常增量更新（缺失 1-30 天）
        3. 斷線補齊（缺失 > 30 天，重新下載）
        4. 強制更新（force=True）

        Args:
            symbol: 股票代號
            force: 強制重新下載全部
            check_today: 是否檢查今天有無交易（避免非交易日重複下載）

        Returns:
            更新後的 DataFrame
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
            # 檢查最後更新日期
            last_date = existing_df.index[-1]
            today = pd.Timestamp.now().normalize()
            missing_days = (today - last_date).days

            self.logger.info(f"最後更新: {last_date.date()}, 缺失 {missing_days} 天")

            # ✅ 改進的判斷邏輯：檢查是否需要更新
            if check_today and missing_days > 0:
                # 判斷今天是否為交易日（台股：週一到週五）
                weekday = today.weekday()  # 0=週一, 6=週日

                # 計算預期的最後交易日
                if weekday == 5:  # 週六
                    expected_last_date = today - pd.Timedelta(days=1)  # 週五
                elif weekday == 6:  # 週日
                    expected_last_date = today - pd.Timedelta(days=2)  # 週五
                else:  # 週間（週一到週五）
                    # 如果現在還沒收盤（14:30前），最新應該是昨天
                    import datetime
                    now = datetime.datetime.now()
                    if now.hour < 14 or (now.hour == 14 and now.minute < 30):
                        expected_last_date = today - pd.Timedelta(days=1)
                    else:
                        expected_last_date = today

                # ✅ 關鍵修正：如果已經有預期日期的資料，才是最新
                if last_date >= expected_last_date:
                    self.logger.info(f"✓ 資料已是最新 (最後日期: {last_date.date()}, 預期: {expected_last_date.date()})")
                    return existing_df
                else:
                    self.logger.info(f"需要更新 (預期: {expected_last_date.date()})")

            # ✅ 移除原本的 missing_days <= 1 判斷（這是錯誤的邏輯）

            # 情境2: 正常增量更新（缺失 <= 30天）
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

            # 情境3: 缺失過久，重新下載
            else:
                self.logger.warning(f"缺失過久 ({missing_days} 天)，重新下載...")
                df = self.download(symbol, period='500d')

        # 儲存並返回
        if df is not None:
            # 品質檢查
            quality = self.cache.check_data_quality(df, symbol)

            # ✅ 只有在資料真的有變化時才儲存
            if existing_df is None or not existing_df.equals(df):
                if self.cache.save(symbol, df):
                    return df
            else:
                self.logger.info("✓ 資料無變化，不更新檔案")
                return df

        return None

    def batch_update(self, symbols: List[str], max_workers: int = 3,
                     delay: float = 0.5) -> Dict[str, List[str]]:
        """
        批次更新多檔股票（平行處理）

        Args:
            symbols: 股票代號列表
            max_workers: 最大平行數量（建議 3-5，避免 API 限流）
            delay: 每個請求之間的延遲秒數

        Returns:
            {'success': [...], 'failed': [...]}
        """
        self.logger.info(f"\n{'=' * 60}")
        self.logger.info(f"批次更新開始: {len(symbols)} 檔股票")
        self.logger.info(f"平行數量: {max_workers}")
        self.logger.info(f"{'=' * 60}\n")

        results = {'success': [], 'failed': []}
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任務
            futures = {}
            for symbol in symbols:
                future = executor.submit(self.update_single, symbol)
                futures[future] = symbol
                time.sleep(delay)  # 避免同時發送太多請求

            # 收集結果
            for i, future in enumerate(as_completed(futures), 1):
                symbol = futures[future]
                try:
                    result = future.result()
                    if result is not None:
                        results['success'].append(symbol)
                        self.logger.info(f"[{i}/{len(symbols)}] ✓ {symbol}")
                    else:
                        results['failed'].append(symbol)
                        self.logger.warning(f"[{i}/{len(symbols)}] ✗ {symbol}")
                except Exception as e:
                    self.logger.error(f"[{i}/{len(symbols)}] ✗ {symbol}: {e}")
                    results['failed'].append(symbol)

        # 統計
        elapsed = time.time() - start_time
        self.logger.info(f"\n{'=' * 60}")
        self.logger.info(f"批次更新完成")
        self.logger.info(f"✓ 成功: {len(results['success'])} 檔")
        self.logger.info(f"✗ 失敗: {len(results['failed'])} 檔")
        self.logger.info(f"⏱ 耗時: {elapsed:.1f} 秒")
        if results['failed']:
            self.logger.info(f"失敗清單: {results['failed'][:10]}")  # 只顯示前10個
        self.logger.info(f"{'=' * 60}\n")

        # 儲存更新記錄
        self._save_update_log(results, elapsed)

        return results

    def batch_update_with_progress(self, symbols: List[str],
                                   batch_size: int = 200,
                                   max_workers: int = 3) -> Dict[str, List[str]]:
        """
        分批次更新（適合大量股票，如全台股 2000 檔）

        Args:
            symbols: 股票代號列表
            batch_size: 每批次數量
            max_workers: 平行數量

        Returns:
            總計結果 {'success': [...], 'failed': [...]}
        """
        total_results = {'success': [], 'failed': []}
        total_batches = (len(symbols) + batch_size - 1) // batch_size

        self.logger.info(f"\n{'=' * 60}")
        self.logger.info(f"分批更新開始")
        self.logger.info(f"總數: {len(symbols)} 檔")
        self.logger.info(f"批次大小: {batch_size} 檔")
        self.logger.info(f"總批次數: {total_batches}")
        self.logger.info(f"{'=' * 60}\n")

        for i in range(0, len(symbols), batch_size):
            batch_num = i // batch_size + 1
            batch = symbols[i:i + batch_size]

            self.logger.info(f"\n📦 批次 {batch_num}/{total_batches}")
            self.logger.info(f"   股票: {batch[0]} ~ {batch[-1]}")

            # 更新這批
            results = self.batch_update(batch, max_workers=max_workers)

            # 累積結果
            total_results['success'].extend(results['success'])
            total_results['failed'].extend(results['failed'])

            # 批次間休息（避免過度請求）
            if batch_num < total_batches:
                rest_time = 10
                self.logger.info(f"   休息 {rest_time} 秒...")
                time.sleep(rest_time)

        # 總結
        self.logger.info(f"\n{'=' * 60}")
        self.logger.info(f"全部更新完成")
        self.logger.info(f"✓ 總成功: {len(total_results['success'])} 檔")
        self.logger.info(f"✗ 總失敗: {len(total_results['failed'])} 檔")
        self.logger.info(f"成功率: {len(total_results['success'])/len(symbols)*100:.1f}%")
        self.logger.info(f"{'=' * 60}\n")

        return total_results

    def _save_update_log(self, results: Dict[str, List[str]], elapsed: float):
        """
        儲存更新日誌

        Args:
            results: 更新結果
            elapsed: 耗時秒數
        """
        metadata_dir = self.cache.metadata_dir

        # 判斷主要市場
        market = 'tw' if any('.TW' in s or '.TWO' in s
                            for s in results['success'][:5]) else 'us'

        log_file = metadata_dir / f"{market}_update.json"

        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'market': market,
            'success_count': len(results['success']),
            'failed_count': len(results['failed']),
            'elapsed_seconds': round(elapsed, 1),
            'failed_symbols': results['failed'][:50]  # 最多記錄 50 個
        }

        # 讀取現有日誌
        if log_file.exists():
            with open(log_file, 'r', encoding='utf-8') as f:
                logs = json.load(f)
        else:
            logs = []

        # 添加新日誌（最多保留最近 30 筆）
        logs.append(log_entry)
        logs = logs[-30:]

        # 儲存
        with open(log_file, 'w', encoding='utf-8') as f:
            json.dump(logs, f, indent=2, ensure_ascii=False)

        # 更新總覽
        self._update_summary()

    def _update_summary(self):
        """更新總覽統計"""
        summary_file = self.cache.metadata_dir / 'summary.json'
        cache_info = self.cache.get_cache_info()

        summary = {
            'last_update': datetime.now().isoformat(),
            'tw': {
                'total': cache_info['tw_stocks'],
                'size_mb': cache_info['tw_size_mb']
            },
            'us': {
                'total': cache_info['us_stocks'],
                'size_mb': cache_info['us_size_mb']
            },
            'total': {
                'stocks': cache_info['total_stocks'],
                'size_mb': cache_info['total_size_mb']
            }
        }

        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)


if __name__ == '__main__':
    # 測試範例

    # 方式1: 明確指定 proxy（公司）
    # downloader = StockDownloader(proxy='http://10.160.3.88:8080')

    # 方式2: 自動從環境變數讀取（推薦）
    downloader = StockDownloader()

    print("=== 測試單一股票下載 ===")
    df = downloader.update_single('2330.TW')
    if df is not None:
        print(f"\n2330.TW 最近 5 筆:")
        print(df.tail())

    print("\n=== 測試批次下載 ===")
    test_symbols = ['2317.TW', 'AAPL', 'TSLA']
    results = downloader.batch_update(test_symbols, max_workers=2)

    print("\n=== 快取資訊 ===")
    info = downloader.cache.get_cache_info()
    for key, value in info.items():
        print(f"{key}: {value}")