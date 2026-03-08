import sys
import os
import json
import time
import random
import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# 取得專案根目錄
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.moneydj_parser import MoneyDJParser

# 設定資料存檔路徑
DATA_DIR = Path(PROJECT_ROOT) / "data" / "fundamentals"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# 判斷今日盤後時間 (下午 3 點後算今日資料已產出)
NOW = datetime.now()
IS_AFTER_MARKET = NOW.hour >= 15
TODAY_STR = NOW.strftime('%Y-%m-%d')


def load_all_stocks():
    """ 從 stock_list.csv 讀取全部股票 """
    csv_path = Path(PROJECT_ROOT) / "data" / "stock_list.csv"
    if not csv_path.exists():
        print(f"❌ 找不到股票清單: {csv_path}")
        return []

    import pandas as pd
    try:
        df = pd.read_csv(csv_path, dtype={'stock_id': str})
        return df['stock_id'].tolist()
    except Exception as e:
        print(f"❌ 讀取清單失敗: {e}")
        return []


def is_updated_today(file_path):
    """
    檢查檔案是否在「今天下午 3 點後」或「今天之內」已經更新過。
    若是，則跳過抓取，這是節省時間的關鍵。
    """
    if not file_path.exists():
        return False

    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)

    # 如果是同一天
    if mtime.date() == NOW.date():
        # 如果現在已經收盤(15:00後)，且檔案也是15:00後更新的 -> 跳過
        if IS_AFTER_MARKET and mtime.hour >= 15:
            return True
        # 如果現在是盤中或早上，但檔案已經是今天更新的 -> 勉強算跳過(避免重複跑)
        if not IS_AFTER_MARKET:
            return True

    return False


def process_stock(sid):
    """ 單一股票的處理邏輯 """
    sid = str(sid).strip()
    file_path = DATA_DIR / f"{sid}.json"

    # 🔥 極速優化：如果今天已經更新過，直接跳過 (Smart Skip)
    if is_updated_today(file_path):
        return sid, True, "⏩ Skipped (Already Updated Today)"

    # 讀取舊資料
    if file_path.exists():
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except Exception:
            existing_data = {"sid": sid}
    else:
        existing_data = {"sid": sid}

    try:
        # 建立解析器 (這裡假設 MoneyDJParser 內部每次請求會 new session，若能共用 session 會更快)
        parser = MoneyDJParser(sid)

        # 抓取資料
        new_data = parser.get_daily_chips(months=6)

        if not new_data['institutional_investors'] and not new_data['margin_trading']:
            # 失敗時稍作休息，避免是 IP 被暫時阻擋
            time.sleep(1)
            return sid, False, "⚠️ No Data"

        existing_data['last_updated'] = new_data['last_updated']
        existing_data['institutional_investors'] = new_data['institutional_investors']
        existing_data['margin_trading'] = new_data['margin_trading']

        # 清理舊欄位
        #existing_data.pop('chips', None)
        #existing_data.pop('chips_history', None)
        # 更新它負責的欄位
        existing_data['last_updated'] = new_data['last_updated']
        existing_data['institutional_investors'] = new_data['institutional_investors']
        existing_data['margin_trading'] = new_data['margin_trading']

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=2, ensure_ascii=False)

        # ⚡ 速度優化：縮短單檔等待時間 (0.2 ~ 0.5s)
        # 依靠多執行緒 + 批次休息來控制頻率，而不是單檔長睡眠
        time.sleep(random.uniform(0.2, 0.5))
        return sid, True, "✅ Saved"

    except Exception as e:
        return sid, False, f"❌ Error: {e}"


def run_daily_update(stock_list, workers=12, chunk_size=20):
    total = len(stock_list)
    # workers 提升到 12，chunk 縮小到 20 以便更頻繁回報進度與插入休息
    print(f"📊 啟動【每日籌碼更新】極速版 (Workers: {workers})，目標 {total} 檔...")
    start_time = time.time()

    success_count = 0
    fail_count = 0
    skip_count = 0

    # 將總清單切割成多個批次 (Chunks)
    chunks = [stock_list[i:i + chunk_size] for i in range(0, total, chunk_size)]

    for chunk_idx, chunk in enumerate(chunks):
        # UI 進度回報 (Format: PROGRESS: 0~100)
        current_progress = int((chunk_idx * chunk_size) / total * 100)
        print(f"PROGRESS: {current_progress}")
        print(f"📦 批次 {chunk_idx + 1}/{len(chunks)} 處理中...", flush=True)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_sid = {executor.submit(process_stock, sid): sid for sid in chunk}

            for future in as_completed(future_to_sid):
                sid, is_success, msg = future.result()
                if "Skipped" in msg:
                    skip_count += 1
                elif is_success:
                    success_count += 1
                else:
                    fail_count += 1

                # 只印出錯誤或特定訊息，避免 Log 刷太快
                if not is_success or "Skipped" not in msg:
                    print(f"[{success_count + skip_count + fail_count}/{total}] {sid} {msg}")

        # ⚡ 速度優化：批次間的休息時間縮短 (3~6秒)，保持連線活躍但不過度
        if chunk_idx < len(chunks) - 1:
            time.sleep(random.uniform(3.0, 6.0))

    elapsed = time.time() - start_time
    print(f"PROGRESS: 100")
    print(f"\n🎉 執行完畢！總耗時: {elapsed:.2f} 秒")
    print(f"   ✅ 成功更新: {success_count}")
    print(f"   ⏩ 跳過(已更): {skip_count}")
    print(f"   ❌ 失敗: {fail_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update Daily Chips from MoneyDJ (Fast Mode)')
    parser.add_argument('--start', type=int, default=0, help='起始索引')
    parser.add_argument('--end', type=int, default=None, help='結束索引')
    # 預設參數調優：workers=12 (平衡速度與被擋風險), chunk=20
    parser.add_argument('--workers', type=int, default=12, help='執行緒數量')
    parser.add_argument('--chunk', type=int, default=20, help='每個批次的數量')
    args = parser.parse_args()

    target_list = load_all_stocks()
    if not target_list:
        print("❌ 無法取得股票清單，程式結束")
        sys.exit(1)

    start_idx = args.start
    end_idx = args.end if args.end is not None else len(target_list)
    sliced_list = target_list[start_idx:end_idx]

    print(f"🔧 範圍模式：索引 {start_idx} 到 {end_idx}，共 {len(sliced_list)} 檔")

    if len(sliced_list) > 0:
        run_daily_update(sliced_list, workers=args.workers, chunk_size=args.chunk)
    else:
        print("⚠️ 範圍內沒有任何股票可以執行")