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
    print(f"📋 預計更新 {total} 檔基本面資料 (含 6 個月法人與資券歷史)...")

    for i, sid in enumerate(stock_list):
        sid = str(sid).strip()
        print(f"[{i + 1}/{total}] 處理 {sid} ...", end=" ", flush=True)

        file_path = DATA_DIR / f"{sid}.json"
        existing_data = {"sid": sid, "last_updated": ""}

        # ==========================================
        # 1. 讀取舊資料 (保留未更新的欄位)
        # ==========================================
        if file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except Exception:
                print("⚠️ 讀取舊檔失敗，將重新建立", end=" ")

        # ==========================================
        # 2. 爬取最新資料 (現在包含完整的歷史 List)
        # ==========================================
        try:
            parser = MoneyDJParser(sid)
            # 取得 Parser 整合後的資料
            # 包含：profitability, yearly_perf, balance_sheet, revenue, cash_flow,
            #       institutional_investors, margin_trading
            new_data = parser.get_full_analysis()

            if not new_data:
                print("⚠️ No Data (抓取失敗或被擋)")
                continue

            # ==========================================
            # 3. 核心更新邏輯 (直接覆蓋模式)
            # ==========================================
            # 因為 parser 現在直接回傳完整的歷史列表 (List[Dict])，
            # 所以我們不需要再做手動 append 或去重複，直接覆蓋即可保持資料最新且完整。

            update_keys = [
                'last_updated',  # 更新時間
                'profitability',  # 獲利能力 (季)
                'yearly_perf',  # 經營績效 (年)
                'balance_sheet',  # 資產負債 (存貨/合約負債)
                'revenue',  # 月營收
                'cash_flow',  # 現金流量
                'institutional_investors',  # 三大法人 (6個月歷史)
                'margin_trading'  # 融資融券 (6個月歷史)
            ]

            data_updated = False
            for key in update_keys:
                # 只有當新資料存在且不為空時才更新，避免爬蟲失敗把舊資料洗掉
                if new_data.get(key):
                    existing_data[key] = new_data[key]
                    data_updated = True

            # 移除舊版邏輯遺留的 key (如果存在)，保持 JSON 乾淨
            if 'chips' in existing_data:
                del existing_data['chips']
            if 'chips_history' in existing_data:
                del existing_data['chips_history']

            # ==========================================
            # 4. 寫回存檔
            # ==========================================
            if data_updated:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(existing_data, f, indent=2, ensure_ascii=False)
                print("✅ Saved")
            else:
                print("⚠️ 無有效新資料可寫入")

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