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


def process_financials(sid, mode='revenue'):
    """
    單一股票基本面處理邏輯
    修正版：強制更新深度資料，暫時關閉 Smart Skip
    """
    sid = str(sid).strip()
    file_path = DATA_DIR / f"{sid}.json"

    # 1. 先讀取舊資料
    existing_data = {"sid": sid}
    file_exists = file_path.exists()

    if file_exists:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except Exception:
            pass  # 讀取失敗就當作空的

    # 2. 🔥 [已停用] 聰明的跳過邏輯
    # 原因：當 Parser 邏輯改變 (例如增加深度) 時，這個邏輯會阻擋更新。
    # 若要恢復加速，請自行取消下方的註解。
    '''
    if file_exists:
        mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
        is_today = mtime.date() == NOW.date()

        if is_today:
            has_revenue = 'revenue' in existing_data and len(existing_data['revenue']) > 0
            has_profit = 'profitability' in existing_data and len(existing_data['profitability']) > 0

            # 如果模式是 'revenue' 且已經有營收 -> 跳過
            if mode == 'revenue' and has_revenue:
                return sid, True, "⏩ Skipped (Revenue exists)"

            # 如果模式是 'full' 且已經有財報 -> 跳過
            if mode == 'full' and has_profit and has_revenue:
                return sid, True, "⏩ Skipped (Full data exists)"
    '''

    # 3. 開始抓取
    try:
        parser = MoneyDJParser(sid)
        updates = {}

        # 根據模式決定抓取範圍
        if mode == 'full':
            # 全抓：適合 3,5,8,11 月財報季
            time.sleep(random.uniform(0.5, 1.0))
            # 🔥 強制指定深度，確保 Parser 吃到參數
            updates["profitability"] = parser.get_profitability_quarterly(limit=12)
            updates["yearly_perf"] = parser.get_yearly_performance(limit=5)
            updates["balance_sheet"] = parser.get_balance_sheet(limit=8)
            updates["revenue"] = parser.get_monthly_revenue(limit=24)
            updates["cash_flow"] = parser.get_cash_flow(limit=8)
            req_count = 5
        else:
            # 只抓營收：適合平時
            time.sleep(random.uniform(0.1, 0.3))
            updates["revenue"] = parser.get_monthly_revenue(limit=24)
            req_count = 1

        # 檢查是否有抓到東西
        has_data = any(len(v) > 0 for v in updates.values() if isinstance(v, list))
        if not has_data:
            return sid, False, "⚠️ 無資料或被擋"

        # 4. 更新欄位 (Merge) - 確保不覆蓋籌碼資料
        for key, value in updates.items():
            if value:
                existing_data[key] = value

        # 5. 寫入存檔
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=2, ensure_ascii=False)

        return sid, True, f"✅ Updated ({'Full' if req_count > 1 else 'Rev'})"

    except Exception as e:
        return sid, False, f"❌ Error: {e}"


def run_financials_update(stock_list, workers=12, chunk_size=50, mode='revenue'):
    total = len(stock_list)
    mode_str = "🔥 全面財報 (慢)" if mode == 'full' else "⚡ 僅月營收 (快)"
    print(f"📊 啟動【基本面更新】{mode_str} (Workers: {workers})，目標 {total} 檔...")

    start_time = time.time()
    success_count = 0
    fail_count = 0
    skip_count = 0

    chunks = [stock_list[i:i + chunk_size] for i in range(0, total, chunk_size)]

    for chunk_idx, chunk in enumerate(chunks):
        pct = int((chunk_idx * chunk_size) / total * 100)
        print(f"PROGRESS: {pct}")
        print(f"📦 批次 {chunk_idx + 1}/{len(chunks)}...", flush=True)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_sid = {executor.submit(process_financials, sid, mode): sid for sid in chunk}

            for future in as_completed(future_to_sid):
                sid, is_success, msg = future.result()
                if "Skipped" in msg:
                    skip_count += 1
                elif is_success:
                    success_count += 1
                else:
                    fail_count += 1

                if not is_success or "Skipped" not in msg:
                    print(f"[{success_count + skip_count + fail_count}/{total}] {sid} {msg}")

        if chunk_idx < len(chunks) - 1:
            if mode == 'full':
                time.sleep(random.uniform(4.0, 7.0))
            else:
                time.sleep(random.uniform(1.5, 3.0))

    elapsed = time.time() - start_time
    print(f"PROGRESS: 100")
    print(f"\n🎉 執行完畢！總耗時: {elapsed:.2f} 秒")
    print(f"   ✅ 成功: {success_count}")
    print(f"   ⏩ 跳過: {skip_count}")
    print(f"   ❌ 失敗: {fail_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update Fundamentals (Smart Mode)')
    parser.add_argument('--start', type=int, default=0, help='起始索引')
    parser.add_argument('--end', type=int, default=None, help='結束索引')
    parser.add_argument('--workers', type=int, default=12, help='執行緒數量 (預設12)')
    parser.add_argument('--chunk', type=int, default=50, help='批次大小')
    parser.add_argument('--full', action='store_true', help='開啟全財報抓取模式')

    args = parser.parse_args()

    target_list = load_all_stocks()
    if not target_list:
        sys.exit(1)

    mode = 'full' if args.full else 'revenue'

    if args.full and args.workers > 12:
        print("⚠️ Full 模式下自動將 Workers 降至 12 以策安全")
        args.workers = 12
    elif not args.full and args.workers == 12:
        args.workers = 16

    start_idx = args.start
    end_idx = args.end if args.end is not None else len(target_list)
    sliced_list = target_list[start_idx:end_idx]

    if len(sliced_list) > 0:
        run_financials_update(sliced_list, workers=args.workers, chunk_size=args.chunk, mode=mode)
    else:
        print("⚠️ 範圍內沒有任何股票")