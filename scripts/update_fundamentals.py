import sys
import os
import json
import time
import random
import argparse
from pathlib import Path
from datetime import datetime

# 取得專案根目錄 (StockWarRoomV3) 並強制置頂加入 Python 路徑
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.moneydj_parser import MoneyDJParser

# 設定資料存檔路徑
DATA_DIR = Path("data/fundamentals")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# 測試用的股票清單 (替換為你實際關注的標的)
TEST_STOCKS = ['3665', '6664', '8358']

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

def run_update(stock_list, force=False):
    total = len(stock_list)
    print(f"📋 預計更新 {total} 檔基本面資料...")

    for i, sid in enumerate(stock_list):
        sid = str(sid).strip()
        print(f"[{i + 1}/{total}] 處理 {sid} ...", end=" ", flush=True)

        file_path = DATA_DIR / f"{sid}.json"
        existing_data = {"sid": sid}

        # ==========================================
        # 1. 讀取舊資料 (為了歷史籌碼的累積合併)
        # ==========================================
        if file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except Exception:
                print("⚠️ 讀取舊檔失敗，將重新建立", end=" ")

        # ==========================================
        # 2. 爬取最新資料
        # ==========================================
        try:
            parser = MoneyDJParser(sid)
            # 取得 Parser 整合後的資料
            new_data = parser.get_full_analysis()

            if not new_data:
                print("⚠️ No Data (抓取失敗或被擋)")
                continue

            # ==========================================
            # 3. 核心合併邏輯
            # ==========================================
            # A/B 類資料 (季報、年報、月營收、資產負債、現金流量)：直接覆蓋最新
            # 這裡已將 Key 值與 moneydj_parser.py 的輸出對齊
            for key in ['profitability', 'yearly_perf', 'balance_sheet', 'revenue', 'cash_flow']:
                if new_data.get(key):
                    existing_data[key] = new_data[key]

            # C 類資料 (每日籌碼)：使用「覆蓋式」累積機制
            if 'chips' in new_data and new_data['chips']:
                new_chip = new_data['chips']
                new_date = new_chip.get('data_date')

                if 'chips_history' not in existing_data:
                    existing_data['chips_history'] = []

                # --- 檢查日期是否已存在 ---
                # 尋找是否有相同日期的舊紀錄索引
                existing_index = next((idx for idx, c in enumerate(existing_data['chips_history'])
                                      if c.get('data_date') == new_date), None)

                if existing_index is not None:
                    # 如果日期相同（例如晚上重跑修正外資數據），直接覆蓋
                    existing_data['chips_history'][existing_index] = new_chip
                    print(f"🔄 籌碼更新({new_date})", end=" ")
                else:
                    # 如果是新日期，則新增
                    existing_data['chips_history'].append(new_chip)
                    print(f"➕ 籌碼新增({new_date})", end=" ")

                # --- 滾動視窗機制 ---
                # 保留過去 60 筆 (約一季) 的每日籌碼
                max_records = 60
                if len(existing_data['chips_history']) > max_records:
                    existing_data['chips_history'] = existing_data['chips_history'][-max_records:]

            # ==========================================
            # 4. 寫回存檔 (覆蓋寫入已包含舊歷史的 existing_data)
            # ==========================================
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, indent=2, ensure_ascii=False)
            print("✅ Saved")

        except Exception as e:
            print(f"❌ Error: {e}")

        # ==========================================
        # 5. 智慧排程與防封鎖 (Anti-Ban)
        # ==========================================
        # 每次抓取隨機延遲 1.5 ~ 3.5 秒
        time.sleep(random.uniform(1.5, 3.5))

        # 每跑 50 檔強制長休息
        if (i + 1) % 50 == 0 and (i + 1) != total:
            pause_time = random.uniform(30.0, 45.0)
            print(f"\n⏳ 已處理 {i + 1} 檔，為防止鎖 IP，啟動長休息 {pause_time:.1f} 秒...\n")
            time.sleep(pause_time)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update Fundamental Data from MoneyDJ')
    parser.add_argument('--test', action='store_true', help='僅測試模式 (跑 TEST_STOCKS 清單)')
    parser.add_argument('--force', action='store_true', help='強制更新')
    args = parser.parse_args()

    if args.test:
        print("🔧 進入測試模式 (Test Mode)")
        target_list = TEST_STOCKS
    else:
        target_list = load_all_stocks()
        if not target_list:
            print("❌ 無法取得股票清單，程式結束")
            sys.exit(1)

    run_update(target_list, force=args.force)