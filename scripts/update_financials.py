import sys
import os
import json
import time
import random
import argparse
import subprocess
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

# 💡 快取總表：用來記錄營收、財報月份與「最後連線日期」
META_FILE = DATA_DIR / "meta_index.json"

NOW = datetime.now()
TODAY_STR = NOW.strftime("%Y-%m-%d")  # 取得今日日期字串 (例如: 2024-04-01)

# 💡 自動計算目標營收月份 (永遠是上個月)
if NOW.month == 1:
    TARGET_YEAR_ROC = NOW.year - 1911 - 1
    TARGET_MONTH = 12
else:
    TARGET_YEAR_ROC = NOW.year - 1911
    TARGET_MONTH = NOW.month - 1

TARGET_REV_MONTH = f"{TARGET_YEAR_ROC:03d}/{TARGET_MONTH:02d}"

# 💡 目標最新財報季 (依據當前月份推算)
if NOW.month in [1, 2, 3]:
    TARGET_EPS_Q = f"{NOW.year - 1}.3Q"
elif NOW.month in [4, 5]:
    TARGET_EPS_Q = f"{NOW.year - 1}.4Q"
elif NOW.month in [6, 7, 8]:
    TARGET_EPS_Q = f"{NOW.year}.1Q"
elif NOW.month in [9, 10, 11]:
    TARGET_EPS_Q = f"{NOW.year}.2Q"
else:
    TARGET_EPS_Q = f"{NOW.year}.3Q"


def load_all_stocks():
    csv_path = Path(PROJECT_ROOT) / "data" / "stock_list.csv"
    if not csv_path.exists():
        return []
    import pandas as pd
    try:
        df = pd.read_csv(csv_path, dtype={'stock_id': str})
        return df['stock_id'].tolist()
    except:
        return []


def load_meta_index():
    """讀取快取總表"""
    if META_FILE.exists():
        try:
            with open(META_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {}


def update_global_meta(meta_updates):
    """將批次更新狀態與「最後檢查日期」寫入總表"""
    if not meta_updates: return

    meta_data = load_meta_index()

    for sid, data in meta_updates.items():
        if isinstance(meta_data.get(sid), str):
            meta_data[sid] = {"rev": meta_data[sid], "eps": "", "last_check": ""}
        if sid not in meta_data:
            meta_data[sid] = {"rev": "", "eps": "", "last_check": ""}

        if "rev" in data: meta_data[sid]["rev"] = data["rev"]
        if "eps" in data: meta_data[sid]["eps"] = data["eps"]
        if "last_check" in data: meta_data[sid]["last_check"] = data["last_check"]

    try:
        with open(META_FILE, 'w', encoding='utf-8') as f:
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ 更新快取清單失敗: {e}")


def process_financials(sid, mode='revenue'):
    sid = str(sid).strip()
    file_path = DATA_DIR / f"{sid}.json"
    existing_data = {"sid": sid}

    if file_path.exists():
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except:
            pass

    latest_local_rev = ""
    if 'revenue' in existing_data and len(existing_data['revenue']) > 0:
        latest_local_rev = existing_data['revenue'][0].get('month', '')

    latest_local_eps = ""
    if 'profitability' in existing_data and len(existing_data['profitability']) > 0:
        latest_local_eps = existing_data['profitability'][0].get('quarter', '')

    # 3. 開始抓取網路資料
    try:
        parser = MoneyDJParser(sid)
        updates = {}

        if mode == 'full':
            time.sleep(random.uniform(0.5, 1.0))
            updates["profitability"] = parser.get_profitability_quarterly(limit=12)
            updates["yearly_perf"] = parser.get_yearly_performance(limit=5)
            updates["balance_sheet"] = parser.get_balance_sheet(limit=8)
            updates["revenue"] = parser.get_monthly_revenue(limit=24)
            updates["cash_flow"] = parser.get_cash_flow(limit=8)
            req_count = 5
        else:
            time.sleep(random.uniform(0.1, 0.3))
            updates["revenue"] = parser.get_monthly_revenue(limit=24)
            req_count = 1

        has_data = any(len(v) > 0 for v in updates.values() if isinstance(v, list))
        if not has_data:
            return sid, False, "⚠️ 無資料或被擋", latest_local_rev, latest_local_eps

        # 4. 更新欄位 (Merge)
        has_changes = False
        for key, value in updates.items():
            if value:
                if key not in existing_data or existing_data[key] != value:
                    existing_data[key] = value
                    has_changes = True

        if 'revenue' in existing_data and len(existing_data['revenue']) > 0:
            latest_local_rev = existing_data['revenue'][0].get('month', '')
        if 'profitability' in existing_data and len(existing_data['profitability']) > 0:
            latest_local_eps = existing_data['profitability'][0].get('quarter', '')

        # 5. 寫入存檔
        if has_changes:
            existing_data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, indent=2, ensure_ascii=False)
            return sid, True, f"✨ Updated ({'Full' if req_count > 1 else 'Rev'} - 有新資料)", latest_local_rev, latest_local_eps
        else:
            return sid, True, f"✅ Checked (網站未公佈，無變動)", latest_local_rev, latest_local_eps

    except Exception as e:
        return sid, False, f"❌ Error: {e}", latest_local_rev, latest_local_eps


def run_financials_update(stock_list, workers=12, chunk_size=50, mode='revenue'):
    total = len(stock_list)
    mode_str = "🔥 全面財報" if mode == 'full' else "⚡ 僅月營收"
    print(f"📊 啟動【基本面更新】{mode_str} (總數 {total} 檔)...")

    # 🔥 核心優化：發起連線前，直接在主執行緒過濾名單！
    meta_data = load_meta_index()
    active_stocks = []
    skip_target_count = 0
    skip_cooldown_count = 0

    for sid in stock_list:
        sid_str = str(sid)
        rec = meta_data.get(sid_str, {})
        if isinstance(rec, str): rec = {"rev": rec, "eps": "", "last_check": ""}

        # 1. 如果資料已經是最新，直接跳過
        is_target_met = False
        if mode == 'revenue' and rec.get("rev", "") >= TARGET_REV_MONTH:
            is_target_met = True
        elif mode == 'full' and rec.get("eps", "") >= TARGET_EPS_Q and rec.get("rev", "") >= TARGET_REV_MONTH:
            is_target_met = True

        if is_target_met:
            skip_target_count += 1
            continue

        # 2. 如果今天已經檢查過，且沒新資料，啟動冷卻跳過 (保護 MoneyDJ 不被連線)
        if rec.get("last_check", "") == TODAY_STR:
            skip_cooldown_count += 1
            continue

        # 需要真正連線的股票
        active_stocks.append(sid_str)

    print(f"🛡️ 記憶體預先攔截：")
    print(f"   - ⏩ 達標免連線: {skip_target_count} 檔")
    print(f"   - 💤 單日冷卻中: {skip_cooldown_count} 檔 (今日已問過，不再打擾 MoneyDJ)")
    print(f"   - 🚀 準備實體連線: {len(active_stocks)} 檔")
    sys.stdout.flush()

    if not active_stocks:
        print("\n🎉 所有股票皆已是最新狀態，或處於單日冷卻中，本次無需發起網路連線！")
        trigger_snapshot()
        return

    start_time = time.time()
    updated_count = 0
    checked_count = 0
    fail_count = 0
    meta_updates = {}

    chunks = [active_stocks[i:i + chunk_size] for i in range(0, len(active_stocks), chunk_size)]

    for chunk_idx, chunk in enumerate(chunks):
        pct = int((chunk_idx * chunk_size) / len(active_stocks) * 100)
        print(f"PROGRESS: {pct}")
        print(f"📦 網路連線批次 {chunk_idx + 1}/{len(chunks)}...", flush=True)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_sid = {executor.submit(process_financials, sid, mode): sid for sid in chunk}

            for future in as_completed(future_to_sid):
                sid, is_success, msg, rev_month, eps_q = future.result()

                # 寫入包含「今日已檢查」的時間戳記
                meta_updates[sid] = {"rev": rev_month, "eps": eps_q, "last_check": TODAY_STR}

                if is_success:
                    if "Updated" in msg:
                        updated_count += 1
                    elif "Checked" in msg:
                        checked_count += 1
                else:
                    fail_count += 1

                current_processed = updated_count + checked_count + fail_count
                print(f"[{current_processed}] {sid} {msg}", flush=True)
        update_global_meta(meta_updates)

        if chunk_idx < len(chunks) - 1:
            time.sleep(random.uniform(2.0, 4.0) if mode == 'full' else random.uniform(1.0, 2.0))



    elapsed = time.time() - start_time
    print(f"PROGRESS: 100")
    time.sleep(1.5)

    print("\n" + "=" * 50)
    print(f"🎉 【{mode_str}】 執行完畢！ (網路連線耗時: {elapsed:.1f} 秒)")
    print("=" * 50)
    print(f" 📂 原始總名單: {total} 檔")
    print(f" 🛡️ 記憶體防護攔截: {skip_target_count + skip_cooldown_count} 檔 (達標或冷卻中)")
    print("-" * 50)
    print(f" 🎯 實際發起網路請求: {updated_count + checked_count} 檔")
    print(f"    ✨ 成功抓取新資料寫入: {updated_count} 檔")
    print(f"    ✅ 網站尚未公佈新資料: {checked_count} 檔")
    print(f" ❌ 執行失敗/超時: {fail_count} 檔")
    print("=" * 50 + "\n")
    sys.stdout.flush()

    trigger_snapshot()


def trigger_snapshot():
    print("🚀 偵測到財報更新完畢，準備自動更新【戰情室快照】大表...", flush=True)
    possible_scripts = ["calc_snapshot_factors.py", "calc_snapshot.py", "calculate_snapshot.py"]
    script_path = None

    for script in possible_scripts:
        p = Path(PROJECT_ROOT) / "scripts" / script
        if p.exists(): script_path = p; break
        p2 = Path(PROJECT_ROOT) / script
        if p2.exists(): script_path = p2; break

    if script_path:
        try:
            print(f"⚙️ 執行: {script_path.name}")
            sys.stdout.flush()
            subprocess.run([sys.executable, str(script_path)], check=True)
            print("\n✅ 戰情室大表 (Snapshot) 自動重新計算成功！")
        except Exception as e:
            print(f"\n❌ 快照計算失敗，請手動執行 ({e})")
    else:
        print("\n⚠️ 找不到 calc_snapshot 系列腳本，請手動執行。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update Fundamentals (Smart Mode)')
    parser.add_argument('--start', type=int, default=0, help='起始索引')
    parser.add_argument('--end', type=int, default=None, help='結束索引')
    parser.add_argument('--workers', type=int, default=12, help='執行緒數量 (預設12)')
    parser.add_argument('--chunk', type=int, default=50, help='批次大小')
    parser.add_argument('--full', action='store_true', help='開啟全財報抓取模式')
    parser.add_argument('--stocks', type=str, default="", help='指定特定股票代號 (例: 8240,1468)')

    args = parser.parse_args()
    mode = 'full' if args.full else 'revenue'

    if args.stocks:
        target_list = [s.strip() for s in args.stocks.split(',')]
    else:
        target_list = load_all_stocks()
        if not target_list: sys.exit(1)

    if args.full and args.workers > 12:
        args.workers = 12
    elif not args.full and args.workers == 12:
        args.workers = 16

    if not args.stocks:
        start_idx = args.start
        end_idx = args.end if args.end is not None else len(target_list)
        sliced_list = target_list[start_idx:end_idx]
    else:
        sliced_list = target_list

    if len(sliced_list) > 0:
        run_financials_update(sliced_list, workers=args.workers, chunk_size=args.chunk, mode=mode)
    else:
        print("⚠️ 範圍內沒有任何股票需要更新")