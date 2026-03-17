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

NOW = datetime.now()
TODAY_STR = NOW.strftime('%Y-%m-%d')
TODAY_STR_SLASH = NOW.strftime('%Y/%m/%d')  # 容許不同日期格式比對


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


def is_updated_today(file_path, mode):
    """
    檢查指定 mode 的資料，今天是否已經更新過。
    如果 JSON 裡面的該欄位最新日期是今天，就跳過。
    """
    if not file_path.exists():
        return False

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if mode in ['inst', 'all']:
            inst = data.get("institutional_investors", [])
            if inst:
                latest_date = inst[0].get("date", "")
                if TODAY_STR in latest_date or TODAY_STR_SLASH in latest_date:
                    if mode == 'inst': return True
            elif mode == 'inst':
                return False

        if mode in ['margin', 'all']:
            margin = data.get("margin_trading", [])
            if margin:
                latest_date = margin[0].get("date", "")
                if TODAY_STR in latest_date or TODAY_STR_SLASH in latest_date:
                    if mode == 'margin': return True
                    # 如果是 all，必須兩者都是今天才算 True
                    inst = data.get("institutional_investors", [])
                    if inst:
                        inst_date = inst[0].get("date", "")
                        return (TODAY_STR in inst_date or TODAY_STR_SLASH in inst_date)
            return False

    except Exception:
        return False

    return False


def process_stock(sid, mode):
    """ 單一股票的處理邏輯，加入 mode 分流與資料保護 """
    sid = str(sid).strip()
    file_path = DATA_DIR / f"{sid}.json"

    # 🔥 Smart Skip: 若指定的 mode 今天已更新過，直接跳過
    if is_updated_today(file_path, mode):
        return sid, True, f"⏩ Skipped ({mode} Already Updated Today)"

    # 讀取舊資料 (為了保護不需要更新的欄位)
    if file_path.exists():
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except Exception:
            existing_data = {"sid": sid}
    else:
        existing_data = {"sid": sid}

    try:
        parser = MoneyDJParser(sid)

        # ⚠️ 注意: 這裡不修改 parser 核心邏輯，依然呼叫原本的方法
        # 但在存檔時，我們會根據 mode 決定要拿新資料還是保留舊資料
        new_data = parser.get_daily_chips(months=6)

        if not new_data.get('institutional_investors') and not new_data.get('margin_trading'):
            time.sleep(1)
            return sid, False, "⚠️ No Data"

        existing_data['last_updated'] = new_data.get('last_updated', str(NOW))

        # 🎯 核心分流存檔邏輯 (保護不需要更新的資料)
        if mode in ['inst', 'all']:
            existing_data['institutional_investors'] = new_data.get('institutional_investors', [])

        if mode in ['margin', 'all']:
            existing_data['margin_trading'] = new_data.get('margin_trading', [])

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=2, ensure_ascii=False)

        time.sleep(random.uniform(0.2, 0.5))
        return sid, True, f"✅ Saved ({mode})"

    except Exception as e:
        return sid, False, f"❌ Error: {e}"


def run_daily_update(stock_list, workers=12, chunk_size=20, mode='all'):
    total = len(stock_list)
    print(f"📊 啟動【每日籌碼更新】極速版 (Workers: {workers}, Mode: {mode})，目標 {total} 檔...")
    start_time = time.time()

    success_count = 0
    fail_count = 0
    skip_count = 0

    chunks = [stock_list[i:i + chunk_size] for i in range(0, total, chunk_size)]

    for chunk_idx, chunk in enumerate(chunks):
        current_progress = int((chunk_idx * chunk_size) / total * 100)
        print(f"PROGRESS: {current_progress}")
        print(f"📦 批次 {chunk_idx + 1}/{len(chunks)} 處理中...", flush=True)

        batch_did_network_request = False

        with ThreadPoolExecutor(max_workers=workers) as executor:
            # 將 mode 參數傳遞給 process_stock
            future_to_sid = {executor.submit(process_stock, sid, mode): sid for sid in chunk}

            for future in as_completed(future_to_sid):
                sid, is_success, msg = future.result()
                if "Skipped" in msg:
                    skip_count += 1
                elif is_success:
                    success_count += 1
                    batch_did_network_request = True
                else:
                    fail_count += 1
                    batch_did_network_request = True

                if not is_success or "Skipped" not in msg:
                    print(f"[{success_count + skip_count + fail_count}/{total}] {sid} {msg}")

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
    parser.add_argument('--workers', type=int, default=12, help='執行緒數量')
    parser.add_argument('--chunk', type=int, default=20, help='每個批次的數量')

    # 🔥 新增模式選擇參數
    parser.add_argument('--mode', type=str, choices=['all', 'inst', 'margin'], default='all',
                        help='更新模式：all(全更新), inst(僅三大法人), margin(僅資券)')
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
        run_daily_update(sliced_list, workers=args.workers, chunk_size=args.chunk, mode=args.mode)
    else:
        print("⚠️ 範圍內沒有任何股票可以執行")