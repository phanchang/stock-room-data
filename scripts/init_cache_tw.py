"""
台股資料初始化腳本

首次執行：下載所有台股的歷史資料（約 2000 檔）
建議在週末或非交易時間執行

使用方式：
    python scripts/init_cache_tw.py --force
"""

import sys
from pathlib import Path
import os
from dotenv import load_dotenv

# 加入專案根目錄到路徑
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import argparse
from datetime import datetime
from utils.cache import StockDownloader  # 👈 這支檔案裡面的 auto_adjust=False 記得要改！

def setup_env():
    """載入環境變數與設定 Proxy"""
    env_path = project_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    proxy = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")
    if proxy:
        print(f"🔒 偵測到 Proxy 設定，正在套用至 yfinance...")
        os.environ['http_proxy'] = proxy
        os.environ['https_proxy'] = proxy
        os.environ['HTTP_PROXY'] = proxy
        os.environ['HTTPS_PROXY'] = proxy
    else:
        print("🌐 未偵測到 Proxy，使用直接連線")

def load_tw_symbols():
    """從 data/stock_list.csv 載入所有台股代號"""
    print("載入台股清單...")
    list_file = project_root / 'data' / 'stock_list.csv'

    if not list_file.exists():
        print(f"❌ 找不到清單檔案: {list_file}")
        return []

    try:
        df = pd.read_csv(list_file, dtype={'stock_id': str})
        symbols = []
        for _, row in df.iterrows():
            stock_id = row['stock_id']
            market = row['market']
            if market == 'TW':
                symbols.append(f"{stock_id}.TW")
            elif market == 'TWO':
                symbols.append(f"{stock_id}.TWO")

        print(f"  總計: {len(symbols)} 檔\n")
        return symbols
    except Exception as e:
        print(f"❌ 讀取清單失敗: {e}")
        return []

def get_latest_trading_date():
    """取得台股最新的交易日期"""
    try:
        import yfinance as yf
        twii = yf.Ticker("^TWII")
        hist = twii.history(period="10d")
        if not hist.empty:
            return pd.Timestamp(hist.index[-1].date())
    except:
        pass
    return pd.Timestamp.now().normalize()

def filter_existing_symbols(downloader, symbols, force=False):
    """過濾已存在的股票"""
    if force:
        print("強制模式：將重新下載所有股票\n")
        return symbols

    print("檢查已快取的股票...")
    existing = downloader.cache.get_all_symbols(market='tw')
    need_download = [s for s in symbols if s not in existing]
    print(f"  已快取: {len(existing)} 檔, 需下載: {len(need_download)} 檔\n")
    return need_download


def main():
    """主程式"""

    # 🟢 執行環境設定 (Proxy 等)
    setup_env()

    # 參數解析
    parser = argparse.ArgumentParser(description='台股資料初始化')
    parser.add_argument('--batch-size', type=int, default=200,
                        help='每批次下載數量（預設 200）')
    parser.add_argument('--workers', type=int, default=3,
                        help='平行下載數量（預設 3，建議 3-5）')
    parser.add_argument('--force', action='store_true',
                        help='強制重新下載全部（忽略已快取）')
    parser.add_argument('--limit', type=int, default=None,
                        help='限制下載數量（測試用）')
    parser.add_argument('--start-from', type=int, default=0,
                        help='從第 N 檔開始（用於中斷後繼續）')
    parser.add_argument('--skip-check', action='store_true',
                        help='跳過已是最新的股票（加速每日更新）')
    parser.add_argument('--auto', action='store_true',
                        help='自動執行，不等待使用者確認')

    args = parser.parse_args()

    # 標題顯示
    print("=" * 70)
    print(" " * 20 + "台股資料初始化 (2026 修復版)")
    print("=" * 70)
    print(f"開始時間: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"強制下載: {'是' if args.force else '否'}")
    print("=" * 70 + "\n")

    # 初始化下載器
    downloader = StockDownloader()

    # 載入股票清單
    symbols = load_tw_symbols()
    if not symbols:
        return

    # 1. 決定要下載的清單
    if args.skip_check:
        print("每日更新模式：檢查所有股票是否需要更新...")
        latest_trading_date = get_latest_trading_date()
        need_update = []
        for symbol in symbols:
            last_date = downloader.cache.get_last_date(symbol)
            if last_date is None or last_date < latest_trading_date:
                need_update.append(symbol)
        symbols_to_download = need_update
    else:
        symbols_to_download = filter_existing_symbols(downloader, symbols, args.force)

    # 2. 處理測試限制或中斷繼續
    if args.start_from > 0:
        symbols_to_download = symbols_to_download[args.start_from:]
    if args.limit:
        symbols_to_download = symbols_to_download[:args.limit]

    if not symbols_to_download:
        print("💡 所有股票都已是最新！使用 --force 可強制重新下載。")
        return

    # 🔥🔥🔥 修正 1：物理刪除舊檔案 (處理檔名點變底線的問題) 🔥🔥🔥
    if args.force:
        print(f"🔥 強制模式啟動：正在清除 {len(symbols_to_download)} 檔的舊資料...")
        tw_cache_dir = project_root / 'data' / 'cache' / 'tw'
        deleted_count = 0

        if tw_cache_dir.exists():
            for symbol in symbols_to_download:
                # 這裡最關鍵：將 2330.TW 轉為 2330_TW.parquet
                safe_filename = symbol.replace('.', '_')
                file_path = tw_cache_dir / f"{safe_filename}.parquet"

                if file_path.exists():
                    try:
                        file_path.unlink()
                        deleted_count += 1
                    except:
                        pass
        print(f"   ✅ 已成功物理刪除 {deleted_count} 個舊檔案，準備重新下載！\n")

    print(f"即將下載 {len(symbols_to_download)} 檔台股資料")
    print(f"預估時間: {len(symbols_to_download) * 0.5 / 60:.1f} 分鐘")

    if not args.auto:
        try:
            input("\n按 Enter 開始，或 Ctrl+C 取消...")
        except KeyboardInterrupt:
            return

    start_time = datetime.now()

    try:
        # 🔥🔥🔥 修正 2：將 force 參數傳遞給 batch_update_with_progress 🔥🔥🔥
        results = downloader.batch_update_with_progress(
            symbols_to_download,
            batch_size=args.batch_size,
            max_workers=args.workers,
            force=args.force  # 👈 關鍵：沒有這行，下載器會判定為一般更新而跳過
        )

        elapsed = (datetime.now() - start_time).total_seconds()

        print("\n" + "=" * 70)
        print(" " * 25 + "更新完成")
        print("=" * 70)
        print(f"✓ 成功: {len(results['success'])} 檔")
        print(f"✗ 失敗: {len(results['failed'])} 檔")
        print(f"⏱ 總耗時: {elapsed / 60:.1f} 分鐘")
        print("=" * 70 + "\n")

    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print("使用者中斷，已保存現有資料。")
        print("=" * 70 + "\n")

if __name__ == '__main__':
    main()