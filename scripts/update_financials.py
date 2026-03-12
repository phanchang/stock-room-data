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

# 💡 自動計算目標營收月份 (永遠是上個月)
# 例如：現在是 2026年3月 -> 目標是 115/02
if NOW.month == 1:
    TARGET_YEAR_ROC = NOW.year - 1911 - 1
    TARGET_MONTH = 12
else:
    TARGET_YEAR_ROC = NOW.year - 1911
    TARGET_MONTH = NOW.month - 1

TARGET_REV_MONTH = f"{TARGET_YEAR_ROC:03d}/{TARGET_MONTH:02d}"


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

    # 2. 🔥 全新聰明跳過邏輯 (攔截網路請求)
    # 如果我們只是要更新營收，且本地檔案已經有最新的月份，就直接跳過，不浪費網路資源！
    if mode == 'revenue' and 'revenue' in existing_data and len(existing_data['revenue']) > 0:
        latest_local_month = existing_data['revenue'][0].get('month', '')

        # 字串比對：如果本地檔案的月份 >= 目標月份 (例如 115/02 >= 115/02)
        if latest_local_month >= TARGET_REV_MONTH:
            return sid, True, f"⏩ Skipped (本地已有 {latest_local_month}，免連線)"

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
        has_changes = False
        for key, value in updates.items():
            if value:
                # 如果舊資料沒有這個 key，或是新抓到的內容跟舊的不一樣
                if key not in existing_data or existing_data[key] != value:
                    existing_data[key] = value
                    has_changes = True

        # 5. 寫入存檔
        if has_changes:
            # 🔥 只有在資料有變動時，才更新時間戳記並寫入硬碟
            existing_data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, indent=2, ensure_ascii=False)
            return sid, True, f"✨ Updated ({'Full' if req_count > 1 else 'Rev'} - 有新資料)"
        else:
            # 資料完全沒變，不碰檔案，不增加硬碟 I/O
            return sid, True, f"✅ Checked (無變動，略過存檔)"

    except Exception as e:
        return sid, False, f"❌ Error: {e}"


def run_financials_update(stock_list, workers=12, chunk_size=50, mode='revenue'):
    total = len(stock_list)
    mode_str = "🔥 全面財報 (慢)" if mode == 'full' else "⚡ 僅月營收 (快)"
    print(f"📊 啟動【基本面更新】{mode_str} (Workers: {workers})，總數 {total} 檔...")

    start_time = time.time()

    # 🔥 升級版計數器
    updated_count = 0  # 實際抓到新資料且寫入硬碟
    checked_count = 0  # 上網抓了，但網站還沒更新 (沒寫入硬碟)
    skip_count = 0  # 本地已經是最新，連網都不用 (聰明跳過)
    fail_count = 0  # 發生錯誤

    chunks = [stock_list[i:i + chunk_size] for i in range(0, total, chunk_size)]

    for chunk_idx, chunk in enumerate(chunks):
        pct = int((chunk_idx * chunk_size) / total * 100)
        print(f"PROGRESS: {pct}")
        print(f"📦 批次 {chunk_idx + 1}/{len(chunks)}...", flush=True)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_sid = {executor.submit(process_financials, sid, mode): sid for sid in chunk}

            for future in as_completed(future_to_sid):
                sid, is_success, msg = future.result()

                # 根據回傳訊息分類計數
                if "Skipped" in msg:
                    skip_count += 1
                elif is_success:
                    if "Updated" in msg:
                        updated_count += 1
                    elif "Checked" in msg:
                        checked_count += 1
                    else:
                        updated_count += 1  # 防呆
                else:
                    fail_count += 1

                # 畫面上只印出「有去連線」或「失敗」的股票，保持終端機乾淨
                if not is_success or "Skipped" not in msg:
                    current_processed = updated_count + checked_count + fail_count
                    print(f"[{current_processed}] {sid} {msg}")

        if chunk_idx < len(chunks) - 1:
            if mode == 'full':
                time.sleep(random.uniform(4.0, 7.0))
            else:
                time.sleep(random.uniform(1.5, 3.0))

    elapsed = time.time() - start_time
    print(f"PROGRESS: 100")

    # 🔥 終極清晰版 Log 報表
    print(f"\n🎉 執行完畢！總耗時: {elapsed:.2f} 秒")
    print("-" * 40)
    print(f"   總處理檔數: {total} 檔")
    print(f"   ⏩ 免連線跳過 (本地已最新): {skip_count} 檔")
    print("-" * 40)
    print(f"   🎯 實際發送網路請求: {updated_count + checked_count} 檔")
    print(f"      ✨ 成功更新寫入: {updated_count} 檔")
    print(f"      ✅ 網站尚未公布: {checked_count} 檔 (略過存檔)")
    print(f"   ❌ 執行失敗: {fail_count} 檔")
    print("-" * 40)

def find_outdated_stocks(target_month="115/01"):
    """自動掃描資料夾，找出營收停留在 target_month (含) 以前的股票代號"""
    outdated = []
    print(f"🔍 正在掃描本機資料，尋找營收卡在 {target_month} 以前的落後股票...")

    for file_path in DATA_DIR.glob("*.json"):
        sid = file_path.stem
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                rev = data.get("revenue", [])
                if rev:
                    latest_month = rev[0].get("month", "")
                    # 字串比對：例如 "114/12" <= "115/01" 會成立
                    if latest_month and latest_month <= target_month:
                        outdated.append(sid)
        except Exception:
            pass

    return outdated


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update Fundamentals (Smart Mode)')
    parser.add_argument('--start', type=int, default=0, help='起始索引')
    parser.add_argument('--end', type=int, default=None, help='結束索引')
    parser.add_argument('--workers', type=int, default=12, help='執行緒數量 (預設12)')
    parser.add_argument('--chunk', type=int, default=50, help='批次大小')
    parser.add_argument('--full', action='store_true', help='開啟全財報抓取模式')

    # 💡 保留手動指定的功能 (備用)
    parser.add_argument('--stocks', type=str, default="", help='指定特定股票代號 (例: 8240,1468)')
    # 💡 終極武器：自動掃描落後名單
    parser.add_argument('--auto-fix', type=str, default="", help='自動掃描舊資料 (輸入要淘汰的月份，如 115/01)')

    args = parser.parse_args()

    # 決定要跑的名單：自動掃描 vs 手動指定 vs 全市場
    if args.auto_fix:
        target_list = find_outdated_stocks(args.auto_fix)
        print(f"🎯 自動掃描完畢！共發現 {len(target_list)} 檔股票需要救援！")
        print(f"🔍 釘子戶名單：{target_list}")
        if not target_list:
            print("🎉 恭喜！所有股票的營收都是最新版，沒有落後名單。")
            sys.exit(0)
    elif args.stocks:
        target_list = [s.strip() for s in args.stocks.split(',')]
        print(f"🎯 啟動指定股票模式，共 {len(target_list)} 檔")
    else:
        target_list = load_all_stocks()
        if not target_list:
            sys.exit(1)

    mode = 'full' if args.full else 'revenue'

    if args.full and args.workers > 12:
        print("⚠️ Full 模式下自動將 Workers 降至 12 以策安全")
        args.workers = 12
    elif not args.full and args.workers == 12:
        args.workers = 16

    # 切片邏輯：只有在跑「全市場」時才做切片
    if not args.auto_fix and not args.stocks:
        start_idx = args.start
        end_idx = args.end if args.end is not None else len(target_list)
        sliced_list = target_list[start_idx:end_idx]
    else:
        sliced_list = target_list

    if len(sliced_list) > 0:
        run_financials_update(sliced_list, workers=args.workers, chunk_size=args.chunk, mode=mode)
    else:
        print("⚠️ 範圍內沒有任何股票")