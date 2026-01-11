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

# 加入專案根目錄到路徑
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import argparse
from datetime import datetime
from utils.cache import StockDownloader


def load_tw_symbols():
    """
    從 StockList 載入所有台股代號

    Returns:
        List[str]: 台股代號列表（格式：2330.TW）
    """
    print("載入台股清單...")

    symbols = []

    # 讀取上市股票
    twse_file = project_root / 'StockList' / 'TWSE_ESVUFR.csv'
    if twse_file.exists():
        try:
            df = pd.read_csv(twse_file, encoding='utf-8')

            # 欄位名稱可能是 '股票代號及名稱' 或 'symbol'
            if '股票代號及名稱' in df.columns:
                col = '股票代號及名稱'
            elif 'symbol' in df.columns:
                col = 'symbol'
            else:
                col = df.columns[0]

            # 提取股票代號（移除中文名稱）
            twse_symbols = df[col].astype(str).tolist()
            twse_symbols = [extract_stock_code(s) for s in twse_symbols]
            twse_symbols = [s for s in twse_symbols if s]  # 移除空值

            # 加上 .TW 後綴
            twse_symbols = [f"{s}.TW" for s in twse_symbols]
            symbols.extend(twse_symbols)
            print(f"  上市: {len(twse_symbols)} 檔")
        except Exception as e:
            print(f"  ⚠️  讀取上市股票失敗: {e}")

    # 讀取上櫃股票
    two_file = project_root / 'StockList' / 'TWO_ESVUFR.csv'
    if two_file.exists():
        try:
            df = pd.read_csv(two_file, encoding='utf-8')

            if '股票代號及名稱' in df.columns:
                col = '股票代號及名稱'
            elif 'symbol' in df.columns:
                col = 'symbol'
            else:
                col = df.columns[0]

            two_symbols = df[col].astype(str).tolist()
            two_symbols = [extract_stock_code(s) for s in two_symbols]
            two_symbols = [s for s in two_symbols if s]

            # 加上 .TWO 後綴
            two_symbols = [f"{s}.TWO" for s in two_symbols]
            symbols.extend(two_symbols)
            print(f"  上櫃: {len(two_symbols)} 檔")
        except Exception as e:
            print(f"  ⚠️  讀取上櫃股票失敗: {e}")

    print(f"  總計: {len(symbols)} 檔\n")

    return symbols


def extract_stock_code(text):
    """
    從文字中提取股票代號

    支援格式：
    - '1101　台泥' -> '1101'
    - '2330' -> '2330'
    - '1101 台泥' -> '1101'

    Args:
        text: 原始文字

    Returns:
        股票代號（純數字）
    """
    import re

    text = str(text).strip()

    # 用正則表達式提取開頭的數字
    match = re.match(r'^(\d+)', text)
    if match:
        return match.group(1)

    return None


def filter_existing_symbols(downloader, symbols, force=False):
    """
    過濾已存在的股票（避免重複下載）

    Args:
        downloader: StockDownloader 實例
        symbols: 所有股票列表
        force: 是否強制重新下載

    Returns:
        需要下載的股票列表
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

    args = parser.parse_args()

    # 標題
    print("=" * 70)
    print(" " * 20 + "台股資料初始化")
    print("=" * 70)
    print(f"開始時間: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"批次大小: {args.batch_size} 檔")
    print(f"平行數量: {args.workers}")
    print(f"強制下載: {'是' if args.force else '否'}")
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
        print("❌ 未找到任何股票！請檢查 StockList/ 目錄")
        return

    # 過濾已存在的股票
    symbols_to_download = filter_existing_symbols(downloader, symbols, args.force)

    if not symbols_to_download:
        print("✅ 所有股票都已快取！")
        print("\n提示：使用 --force 可強制重新下載")
        return

    # 處理起始位置
    if args.start_from > 0:
        symbols_to_download = symbols_to_download[args.start_from:]
        print(f"跳過前 {args.start_from} 檔，剩餘 {len(symbols_to_download)} 檔\n")

    # 限制數量（測試用）
    if args.limit:
        symbols_to_download = symbols_to_download[:args.limit]
        print(f"測試模式：只下載前 {args.limit} 檔\n")

    # 確認
    print(f"即將下載 {len(symbols_to_download)} 檔台股資料")
    print(f"預估時間: {len(symbols_to_download) * 0.5 / 60:.1f} 分鐘")
    print("\n按 Ctrl+C 可隨時中斷（已下載的資料會保留）\n")

    try:
        input("按 Enter 開始，或 Ctrl+C 取消...")
    except KeyboardInterrupt:
        print("\n\n已取消")
        return

    print("\n" + "=" * 70)
    print("開始下載...")
    print("=" * 70 + "\n")

    # 開始下載
    start_time = datetime.now()

    try:
        results = downloader.batch_update_with_progress(
            symbols_to_download,
            batch_size=args.batch_size,
            max_workers=args.workers
        )

        # 完成統計
        elapsed = (datetime.now() - start_time).total_seconds()

        print("\n" + "=" * 70)
        print(" " * 25 + "完成")
        print("=" * 70)
        print(f"✓ 成功: {len(results['success'])} 檔")
        print(f"✗ 失敗: {len(results['failed'])} 檔")
        print(f"⏱ 總耗時: {elapsed / 60:.1f} 分鐘")
        print(f"完成時間: {datetime.now():%Y-%m-%d %H:%M:%S}")

        # 失敗清單
        if results['failed']:
            print(f"\n失敗清單（前 20 個）:")
            for symbol in results['failed'][:20]:
                print(f"  - {symbol}")

            # 儲存完整失敗清單
            failed_file = project_root / 'data' / 'cache' / 'metadata' / 'failed_symbols.txt'
            with open(failed_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(results['failed']))
            print(f"\n完整失敗清單已儲存: {failed_file}")

        # 快取資訊
        print("\n快取統計:")
        info = downloader.cache.get_cache_info()
        print(f"  台股: {info['tw_stocks']} 檔")
        print(f"  大小: {info['tw_size_mb']:.1f} MB")

        print("=" * 70 + "\n")

    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print(" " * 22 + "使用者中斷")
        print("=" * 70)
        print("已下載的資料已保存")
        print(f"下次執行時使用 --start-from {args.start_from + len(downloader.cache.get_all_symbols(market='tw'))} 繼續")
        print("=" * 70 + "\n")


if __name__ == '__main__':
    main()