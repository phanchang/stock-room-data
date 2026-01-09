"""
快取管理核心模組
負責 Parquet 檔案的載入、儲存、更新邏輯
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import json
import logging
from typing import Optional, List, Dict


class CacheManager:
    """股票資料快取管理器"""

    # 快取策略設定
    MIN_CACHE_DAYS = 250      # 最少保留 250 天（約 1 年）
    MAX_CACHE_DAYS = 500      # 最多保留 500 天（約 2 年）
    MAX_GAP_DAYS = 10         # 資料缺口警告閾值

    # 必要欄位
    REQUIRED_COLUMNS = ['open', 'high', 'low', 'close', 'volume']

    def __init__(self, base_dir: str = 'data/cache'):
        """
        初始化快取管理器

        Args:
            base_dir: 快取根目錄（相對於專案根目錄）
        """
        # 找到專案根目錄（向上找到包含 StockWarRoom.py 的目錄）
        current = Path(__file__).resolve()
        project_root = None

        # 向上搜尋，直到找到專案根目錄的標記檔案
        for parent in current.parents:
            if (parent / 'StockWarRoom.py').exists() or \
               (parent / '.env').exists():
                project_root = parent
                break

        # 如果找不到，使用當前工作目錄
        if project_root is None:
            project_root = Path.cwd()

        # 設定快取目錄（絕對路徑）
        self.project_root = project_root
        self.base_dir = project_root / base_dir
        self.tw_dir = self.base_dir / 'tw'
        self.us_dir = self.base_dir / 'us'
        self.metadata_dir = self.base_dir / 'metadata'

        # 建立目錄
        self._init_directories()

        # 設定日誌
        self._setup_logger()

    def _init_directories(self):
        """初始化目錄結構"""
        for dir_path in [self.tw_dir, self.us_dir, self.metadata_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

    def _setup_logger(self):
        """設定日誌"""
        # 日誌目錄也使用專案根目錄
        log_dir = self.project_root / 'utils' / 'logs'
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = log_dir / f"cache_{datetime.now():%Y%m%d}.log"

        # 避免重複設定
        logger = logging.getLogger('CacheManager')
        if not logger.handlers:
            logger.setLevel(logging.INFO)

            # 檔案處理器
            fh = logging.FileHandler(log_file, encoding='utf-8')
            fh.setLevel(logging.INFO)

            # 終端處理器
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)

            # 格式
            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)

            logger.addHandler(fh)
            logger.addHandler(ch)

        self.logger = logger

    def _get_market(self, symbol: str) -> str:
        """
        判斷股票所屬市場

        Args:
            symbol: 股票代號（如 '2330.TW', 'AAPL'）

        Returns:
            'tw' 或 'us'
        """
        return 'tw' if ('.TW' in symbol or '.TWO' in symbol) else 'us'

    def _get_stock_path(self, symbol: str) -> Path:
        """
        取得股票快取檔案路徑

        Args:
            symbol: 股票代號

        Returns:
            完整檔案路徑
        """
        market = self._get_market(symbol)
        market_dir = self.tw_dir if market == 'tw' else self.us_dir

        # 檔名處理：2330.TW -> 2330_TW.parquet
        safe_name = symbol.replace('.', '_')
        return market_dir / f"{safe_name}.parquet"

    def exists(self, symbol: str) -> bool:
        """
        檢查股票快取是否存在

        Args:
            symbol: 股票代號

        Returns:
            是否存在
        """
        return self._get_stock_path(symbol).exists()

    def load(self, symbol: str, days: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        載入股票快取資料

        Args:
            symbol: 股票代號
            days: 載入最近幾天（None = 全部）

        Returns:
            DataFrame 或 None（若不存在或錯誤）
        """
        stock_path = self._get_stock_path(symbol)

        if not stock_path.exists():
            self.logger.debug(f"快取不存在: {symbol}")
            return None

        try:
            df = pd.read_parquet(stock_path)

            # 確保日期索引
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)

            # 排序
            df = df.sort_index()

            # 只取最近 N 天
            if days:
                df = df.tail(days)

            self.logger.debug(f"載入 {symbol}: {len(df)} 筆")
            return df

        except Exception as e:
            self.logger.error(f"載入失敗 {symbol}: {e}")
            return None

    def save(self, symbol: str, df: pd.DataFrame) -> bool:
        """
        儲存股票資料到快取

        Args:
            symbol: 股票代號
            df: 股票資料（必須包含 date index 和必要欄位）

        Returns:
            是否成功
        """
        if df is None or df.empty:
            self.logger.warning(f"資料為空，略過儲存: {symbol}")
            return False

        stock_path = self._get_stock_path(symbol)

        try:
            # 確保日期索引
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)

            # 排序、去重
            df = df.sort_index()
            df = df[~df.index.duplicated(keep='last')]

            # 驗證必要欄位
            missing_cols = set(self.REQUIRED_COLUMNS) - set(df.columns)
            if missing_cols:
                self.logger.error(f"{symbol} 缺少欄位: {missing_cols}")
                return False

            # 清理舊資料（保留最近 MAX_CACHE_DAYS）
            if len(df) > self.MAX_CACHE_DAYS:
                df = df.tail(self.MAX_CACHE_DAYS)
                self.logger.debug(f"{symbol} 清理舊資料，保留最近 {self.MAX_CACHE_DAYS} 天")

            # 儲存（使用 snappy 壓縮）
            df.to_parquet(stock_path, compression='snappy', index=True)

            self.logger.info(f"✓ 儲存 {symbol}: {len(df)} 筆資料")
            return True

        except Exception as e:
            self.logger.error(f"✗ 儲存失敗 {symbol}: {e}")
            return False

    def get_last_date(self, symbol: str) -> Optional[pd.Timestamp]:
        """
        取得股票最後一筆資料的日期

        Args:
            symbol: 股票代號

        Returns:
            最後日期 或 None
        """
        df = self.load(symbol)
        if df is not None and not df.empty:
            return df.index[-1]
        return None

    def merge_data(self, old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        """
        合併舊資料與新資料

        Args:
            old_df: 舊資料
            new_df: 新資料

        Returns:
            合併後的 DataFrame
        """
        if new_df is None or new_df.empty:
            return old_df

        if old_df is None or old_df.empty:
            return new_df

        # 合併
        merged = pd.concat([old_df, new_df])

        # 去重（保留最新）
        merged = merged[~merged.index.duplicated(keep='last')]

        # 排序
        merged = merged.sort_index()

        self.logger.debug(
            f"合併資料: {len(old_df)} + {len(new_df)} = {len(merged)}"
        )

        return merged

    def check_data_quality(self, df: pd.DataFrame, symbol: str) -> Dict[str, any]:
        """
        檢查資料品質

        Args:
            df: 股票資料
            symbol: 股票代號

        Returns:
            品質報告字典
        """
        issues = {
            'has_gaps': False,
            'has_missing': False,
            'has_invalid': False,
            'gaps': [],
            'missing_columns': [],
            'details': []
        }

        # 檢查缺口
        date_diff = df.index.to_series().diff()
        large_gaps = date_diff[date_diff > pd.Timedelta(days=self.MAX_GAP_DAYS)]

        if not large_gaps.empty:
            issues['has_gaps'] = True
            for gap_date, gap_size in large_gaps.items():
                gap_info = {
                    'date': gap_date,
                    'days': gap_size.days
                }
                issues['gaps'].append(gap_info)
                self.logger.warning(
                    f"⚠️  {symbol} 資料缺口: {gap_date.date()} "
                    f"(間隔 {gap_size.days} 天)"
                )

        # 檢查缺失值
        missing = df.isnull().sum()
        if missing.any():
            issues['has_missing'] = True
            issues['missing_columns'] = missing[missing > 0].to_dict()
            self.logger.warning(f"⚠️  {symbol} 有缺失值: {dict(missing[missing > 0])}")

        # 檢查異常值（價格 <= 0）
        price_cols = ['open', 'high', 'low', 'close']
        if (df[price_cols] <= 0).any().any():
            issues['has_invalid'] = True
            self.logger.error(f"❌ {symbol} 有異常價格（<= 0）")

        return issues

    def get_cache_info(self) -> Dict[str, any]:
        """
        取得快取統計資訊

        Returns:
            統計資訊字典
        """
        tw_files = list(self.tw_dir.glob('*.parquet'))
        us_files = list(self.us_dir.glob('*.parquet'))

        tw_size = sum(f.stat().st_size for f in tw_files)
        us_size = sum(f.stat().st_size for f in us_files)
        total_size = tw_size + us_size

        return {
            'tw_stocks': len(tw_files),
            'us_stocks': len(us_files),
            'total_stocks': len(tw_files) + len(us_files),
            'tw_size_mb': round(tw_size / 1024 / 1024, 2),
            'us_size_mb': round(us_size / 1024 / 1024, 2),
            'total_size_mb': round(total_size / 1024 / 1024, 2),
            'cache_dir': str(self.base_dir)
        }

    def get_all_symbols(self, market: Optional[str] = None) -> List[str]:
        """
        取得所有已快取的股票代號

        Args:
            market: 指定市場 ('tw', 'us', None=全部)

        Returns:
            股票代號列表
        """
        symbols = []

        if market is None or market == 'tw':
            for file in self.tw_dir.glob('*.parquet'):
                # 2330_TW.parquet -> 2330.TW
                symbol = file.stem.replace('_', '.')
                symbols.append(symbol)

        if market is None or market == 'us':
            for file in self.us_dir.glob('*.parquet'):
                symbol = file.stem.replace('_', '.')
                symbols.append(symbol)

        return sorted(symbols)

    def delete(self, symbol: str) -> bool:
        """
        刪除股票快取

        Args:
            symbol: 股票代號

        Returns:
            是否成功
        """
        stock_path = self._get_stock_path(symbol)

        if stock_path.exists():
            try:
                stock_path.unlink()
                self.logger.info(f"已刪除快取: {symbol}")
                return True
            except Exception as e:
                self.logger.error(f"刪除失敗 {symbol}: {e}")
                return False
        else:
            self.logger.warning(f"快取不存在: {symbol}")
            return False


if __name__ == '__main__':
    # 測試範例
    cache = CacheManager()

    # 測試資料
    test_data = pd.DataFrame({
        'open': [100, 101, 102],
        'high': [105, 106, 107],
        'low': [99, 100, 101],
        'close': [103, 104, 105],
        'volume': [1000000, 1100000, 1200000]
    }, index=pd.date_range('2025-01-01', periods=3))

    # 測試儲存
    print("測試儲存...")
    cache.save('TEST.TW', test_data)

    # 測試載入
    print("\n測試載入...")
    loaded = cache.load('TEST.TW')
    print(loaded)

    # 測試統計
    print("\n快取資訊:")
    info = cache.get_cache_info()
    for key, value in info.items():
        print(f"  {key}: {value}")

    # 清理測試資料
    print("\n清理測試資料...")
    cache.delete('TEST.TW')