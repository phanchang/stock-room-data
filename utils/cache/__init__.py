"""
快取管理模組

提供股票資料的本地快取管理功能：
- CacheManager: 快取的儲存、載入、管理
- StockDownloader: 從 yfinance 下載並更新快取
"""

from .manager import CacheManager
from .downloader import StockDownloader

__all__ = ['CacheManager', 'StockDownloader']
__version__ = '1.0.0'