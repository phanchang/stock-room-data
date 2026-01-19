"""
台股資料初始化腳本

首次執行：下載所有台股的歷史資料（約 2000 檔）
建議在週末或非交易時間執行

使用方式：
    python scripts/init_cache_tw.py

或指定選項：
    python scripts/init_cache_tw.py --batch-size 100 --workers 3
"""

import sys
from pathlib import Path
import os # 🟢 新增
from dotenv import load_dotenv # 🟢 新增

# 加入專案根目錄到路徑
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import argparse
from datetime import datetime
from utils.cache import StockDownloader

# 🟢 [新增] Proxy 設定函式
def setup_env():
    """載入環境變數與設定 Proxy"""
    # 載入 .env 檔案
    env_path = project_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    # 檢查是否有設定 Proxy
    proxy = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")

    if proxy:
        print(f"🔒 偵測到 Proxy 設定，正在套用至 yfinance...")
        # 設定系統環境變數，yfinance/requests 會自動讀取這些變數
        os.environ['http_proxy'] = proxy
        os.environ['https_proxy'] = proxy
        os.environ['HTTP_PROXY'] = proxy
        os.environ['HTTPS_PROXY'] = proxy
    else:
        print("🌐 未偵測到 Proxy，使用直接連線")

def load_tw_symbols():
    """
    從 data/stock_list.csv 載入所有台股代號
    """
    print("載入台股清單...")

    list_file = project_root / 'data' / 'stock_list.csv'

    if not list_file.exists():
        print(f"❌ 找不到清單檔案: {list_file}")
        print("💡 請先執行: python scripts/update_stock_list.py")
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

        print(f"  上市: {len([s for s in symbols if s.endswith('.TW')])} 檔")
        print(f"  上櫃: {len([s for s in symbols if s.endswith('.TWO')])} 檔")
        print(f"  總計: {len(symbols)} 檔\n")

        return symbols

    except Exception as e:
        print(f"❌ 讀取清單失敗: {e}")
        return []


def get_latest_trading_date():
    """
    取得台股最新的交易日期
    """
    print("取得台股最新交易日...")

    try:
        import yfinance as yf

        # 使用台股加權指數
        twii = yf.Ticker("^TWII")

        # 下載最近 10 天的資料
        hist = twii.history(period="10d")

        if not hist.empty:
            latest_date = hist.index[-1]
            latest_date = pd.Timestamp(latest_date.date())

            print(f"  ✓ 台股最新交易日: {latest_date.date()}")
            print(f"  ✓ 今天日期: {pd.Timestamp.now().date()}\n")

            return latest_date
        else:
            print("  ⚠️  無法從 Yahoo Finance 取得台股指數資料")
            raise ValueError("無法取得台股指數")

    except Exception as e:
        print(f"  ⚠️  查詢台股指數失敗: {e}")
        print(f"  可能是 Proxy 問題或網路不穩")
        print(f"  暫時使用今天作為參考日期\n")
        return pd.Timestamp.now().normalize()


def filter_existing_symbols(downloader, symbols, force=False):
    """
    過濾已存在的股票
    """
    if force:
        print("強制模式：將重新下載所有股票\n")
        return symbols

    print("檢查已快取的股票...")
    existing = downloader.cache.get_all_symbols(market='tw')
    need_download = [s for s in symbols if s not in existing]

    print(f"  已快取: {len(existing)} 檔")
    print(f"  需下載: {len(need_download)} 檔\n")

    return need_download


def main():
    """主程式"""

    # 🟢 [新增] 執行環境設定 (最重要的一步！)
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

    # 標題
    print("=" * 70)
    print(" " * 20 + "台股資料初始化")
    print("=" * 70)
    print(f"開始時間: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"批次大小: {args.batch_size} 檔")
    print(f"平行數量: {args.workers}")
    print(f"強制下載: {'是' if args.force else '否'}")
    print(f"跳過檢查: {'是' if args.skip_check else '否'}")
    print(f"自動模式: {'是' if args.auto else '否'}")
    if args.limit:
        print(f"限制數量: {args.limit} 檔（測試模式）")
    if args.start_from > 0:
        print(f"從第 {args.start_from} 檔開始")
    print("=" * 70 + "\n")

    # 初始化下載器
    downloader = StockDownloader()

    # 載入股票清單
    symbols = load_tw_symbols()

    if not symbols:
        return

    # 過濾已存在的股票
    if args.skip_check:
        print("每日更新模式：檢查所有股票是否需要更新...")
        existing = downloader.cache.get_all_symbols(market='tw')
        symbols_to_check = [s for s in symbols if s in existing]
        print(f"  已快取: {len(existing)} 檔")
        print(f"  待檢查: {len(symbols_to_check)} 檔\n")

        latest_trading_date = get_latest_trading_date()

        need_update = []
        total = len(symbols_to_check)

        for idx, symbol in enumerate(symbols_to_check, 1):
            if idx % 100 == 0 or idx == total:
                print(f"  檢查進度: {idx}/{total} ({idx / total * 100:.1f}%)")

            last_date = downloader.cache.get_last_date(symbol)

            if last_date is None or last_date < latest_trading_date:
                need_update.append(symbol)

        print(f"  ✓ 台股最新交易日: {latest_trading_date.date()}")
        print(f"  ✓ 需要更新的股票: {len(need_update)} 檔\n")

        symbols_to_download = need_update
    else:
        symbols_to_download = filter_existing_symbols(downloader, symbols, args.force)

    if not symbols_to_download:
        print("=" * 70)
        print(" " * 25 + "完成")
        print("=" * 70)
        print(f"✓ 成功: 0 檔")
        print(f"✗ 失敗: 0 檔")
        print(f"⏱ 總耗時: 0.0 分鐘")
        print(f"完成時間: {datetime.now():%Y-%m-%d %H:%M:%S}")
        print("\n💡 所有股票都已是最新！")
        print("提示：使用 --force 可強制重新下載")
        print("=" * 70 + "\n")
        return

    if args.start_from > 0:
        symbols_to_download = symbols_to_download[args.start_from:]
        print(f"跳過前 {args.start_from} 檔，剩餘 {len(symbols_to_download)} 檔\n")

    if args.limit:
        symbols_to_download = symbols_to_download[:args.limit]
        print(f"測試模式：只下載前 {args.limit} 檔\n")

    print(f"即將下載 {len(symbols_to_download)} 檔台股資料")
    print(f"預估時間: {len(symbols_to_download) * 0.5 / 60:.1f} 分鐘")

    if not args.auto:
        print("\n按 Ctrl+C 可隨時中斷（已下載的資料會保留）\n")
        try:
            input("按 Enter 開始，或 Ctrl+C 取消...")
        except KeyboardInterrupt:
            print("\n\n已取消")
            return
    else:
        print("\n自動模式：立即開始下載...\n")

    print("\n" + "=" * 70)
    print("開始下載...")
    print("=" * 70 + "\n")

    start_time = datetime.now()

    try:
        results = downloader.batch_update_with_progress(
            symbols_to_download,
            batch_size=args.batch_size,
            max_workers=args.workers
        )

        elapsed = (datetime.now() - start_time).total_seconds()

        print("\n" + "=" * 70)
        print(" " * 25 + "完成")
        print("=" * 70)
        print(f"✓ 成功: {len(results['success'])} 檔")
        print(f"✗ 失敗: {len(results['failed'])} 檔")
        print(f"⏱ 總耗時: {elapsed / 60:.1f} 分鐘")
        print(f"完成時間: {datetime.now():%Y-%m-%d %H:%M:%S}")

        if results['failed']:
            print(f"\n失敗清單（前 20 個）:")
            for symbol in results['failed'][:20]:
                print(f"  - {symbol}")

            failed_file = project_root / 'data' / 'cache' / 'metadata' / 'failed_symbols.txt'
            failed_file.parent.mkdir(parents=True, exist_ok=True)
            with open(failed_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(results['failed']))
            print(f"\n完整失敗清單已儲存: {failed_file}")

        info = downloader.cache.get_cache_info()
        print("\n快取統計:")
        print(f"  台股: {info['tw_stocks']} 檔")
        print(f"  大小: {info['tw_size_mb']:.1f} MB")

        print("=" * 70 + "\n")

    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print(" " * 22 + "使用者中斷")
        print("=" * 70)
        print("已下載的資料已保存")
        existing_count = len(downloader.cache.get_all_symbols(market='tw'))
        print(f"下次執行時使用 --start-from {existing_count} 繼續")
        print("=" * 70 + "\n")


if __name__ == '__main__':
    main()