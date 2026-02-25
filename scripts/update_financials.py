import sys
import os
import json
import time
import random
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# 取得專案根目錄 (StockWarRoomV3) 並強制置頂加入 Python 路徑
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.moneydj_parser import MoneyDJParser

# 設定資料存檔路徑
DATA_DIR = Path("data/fundamentals")
DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_all_stocks():
    """ 從 stock_list.csv 讀取全部股票 """
    csv_path = Path("data/stock_list.csv")
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


def process_financials(sid):
    """ 單一股票基本面(財報/營收)處理邏輯 """
    sid = str(sid).strip()
    file_path = DATA_DIR / f"{sid}.json"

    # 1. 讀取舊資料 (為了保留籌碼資料)
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

        # 2. 分別抓取 5 大基本面指標
        # 如果有些沒資料就放空陣列，這在 parser 裡面已經寫好了防呆
        new_financials = {
            "profitability": parser.get_profitability_quarterly(),
            "yearly_perf": parser.get_yearly_performance(),
            "balance_sheet": parser.get_balance_sheet(),
            "revenue": parser.get_monthly_revenue(),
            "cash_flow": parser.get_cash_flow()
        }

        # 簡單檢查一下是不是全部都抓空了 (可能遇到下市或無資料股票)
        has_data = any(len(v) > 0 for v in new_financials.values() if isinstance(v, list))
        if not has_data:
            return sid, False, "⚠️ 無財報/營收資料 (可能無資料或被擋)"

        # 3. 覆蓋更新基本面欄位
        for key, value in new_financials.items():
            if value:  # 只有當抓到新資料時才覆蓋，避免網路錯誤洗掉舊資料
                existing_data[key] = value

        # 4. 寫回存檔
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=2, ensure_ascii=False)

        # 單檔股票處理完的微小間隔 (因為發了 5 個 request，讓它喘一下)
        time.sleep(random.uniform(1.5, 3.5))
        return sid, True, "✅ 基本面已更新"

    except Exception as e:
        return sid, False, f"❌ Error: {e}"


def run_financials_update(stock_list, workers=4, chunk_size=50):
    total = len(stock_list)
    print(f"📊 啟動【季/月報基本面更新】排程 (多執行緒: {workers})，預計處理 {total} 檔...")
    start_time = time.time()

    success_count = 0
    fail_count = 0

    chunks = [stock_list[i:i + chunk_size] for i in range(0, total, chunk_size)]

    for chunk_idx, chunk in enumerate(chunks):
        print(f"\n📦 開始處理第 {chunk_idx + 1}/{len(chunks)} 批次 (本批次 {len(chunk)} 檔)...")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_sid = {executor.submit(process_financials, sid): sid for sid in chunk}

            for future in as_completed(future_to_sid):
                sid, is_success, msg = future.result()
                if is_success:
                    success_count += 1
                else:
                    fail_count += 1
                print(f"[{success_count + fail_count}/{total}] {sid} {msg}", flush=True)

        if chunk_idx < len(chunks) - 1:
            # 因為財報抓取 Request 較多，全域休息時間稍微拉長一點點會更安全
            pause_time = random.uniform(20.0, 30.0)
            print(f"⏳ 批次 {chunk_idx + 1} 完成。全域防鎖 IP 休息 {pause_time:.1f} 秒...\n")
            time.sleep(pause_time)

    elapsed = time.time() - start_time
    print(f"\n🎉 執行完畢！總耗時: {elapsed:.2f} 秒 (成功: {success_count}, 失敗: {fail_count})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update Fundamentals from MoneyDJ')
    parser.add_argument('--start', type=int, default=0, help='起始索引')
    parser.add_argument('--end', type=int, default=None, help='結束索引')
    parser.add_argument('--workers', type=int, default=4, help='執行緒數量')
    parser.add_argument('--chunk', type=int, default=50, help='每個批次的數量')
    args = parser.parse_args()

    target_list = load_all_stocks()
    if not target_list:
        print("❌ 無法取得股票清單，程式結束")
        sys.exit(1)

    start_idx = args.start
    end_idx = args.end if args.end is not None else len(target_list)
    sliced_list = target_list[start_idx:end_idx]

    print(f"🔧 範圍模式：執行清單索引 {start_idx} 到 {end_idx}，共 {len(sliced_list)} 檔")

    if len(sliced_list) > 0:
        run_financials_update(sliced_list, workers=args.workers, chunk_size=args.chunk)
    else:
        print("⚠️ 範圍內沒有任何股票可以執行")