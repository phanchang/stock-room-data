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
    檢查檔案是否已經更新過 (分段式智慧判斷)：
    1. 早上 (00~15點): 只要今天有更新過就跳過。
    2. 下午 (15~21點): 法人公布，檔案必須是今天 15:00 後更新的才跳過。
    3. 晚上 (21點後): 融資券公布，檔案必須是今天 21:00 後更新的才跳過。
    """
    if not file_path.exists():
        return False

    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)

    # 如果連日期都不是今天，絕對不能跳過 (一定要更新)
    if mtime.date() != NOW.date():
        return False

    # 已經是同一天的情況下，根據「現在的時間」進行分段檢查
    if NOW.hour >= 21:
        # 晚上 9 點後 (資券公布期)：檔案必須是晚上 9 點後才更新的
        return mtime.hour >= 21
    elif NOW.hour >= 15:
        # 下午 3 點後 (法人公布期)：檔案必須是下午 3 點後才更新的
        return mtime.hour >= 15
    else:
        # 早上盤中 (看昨天資料)：只要今天是同一天更新的都算過關
        return True


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
    print(f"📊 啟動【每日籌碼更新】極速版 (Workers: {workers})，目標 {total} 檔...")
    start_time = time.time()

    success_count = 0
    fail_count = 0
    skip_count = 0

    chunks = [stock_list[i:i + chunk_size] for i in range(0, total, chunk_size)]

    for chunk_idx, chunk in enumerate(chunks):
        current_progress = int((chunk_idx * chunk_size) / total * 100)
        print(f"PROGRESS: {current_progress}")
        print(f"📦 批次 {chunk_idx + 1}/{len(chunks)} 處理中...", flush=True)

        batch_did_network_request = False  # 追蹤這個批次是否有發送真實網路請求

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_sid = {executor.submit(process_stock, sid): sid for sid in chunk}

            for future in as_completed(future_to_sid):
                sid, is_success, msg = future.result()
                if "Skipped" in msg:
                    skip_count += 1
                elif is_success:
                    success_count += 1
                    batch_did_network_request = True  # 有成功抓取代表有發送請求
                else:
                    fail_count += 1
                    batch_did_network_request = True  # 失敗通常也是因為發了請求被擋

                if not is_success or "Skipped" not in msg:
                    print(f"[{success_count + skip_count + fail_count}/{total}] {sid} {msg}")

        # 🚀 終極優化：如果這個批次全部都是 Skipped，就不要傻傻等待 3~6 秒！
        if chunk_idx < len(chunks) - 1 and batch_did_network_request:
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