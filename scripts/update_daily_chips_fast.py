# 檔案路徑: scripts/update_daily_chips_fast.py
import sys
import os
import json
import time
import requests
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# 取得專案根目錄
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 載入環境變數 (公司 Proxy 設定)
load_dotenv(PROJECT_ROOT / '.env')

DATA_DIR = PROJECT_ROOT / "data" / "fundamentals"
DATA_DIR.mkdir(parents=True, exist_ok=True)


# ==========================================
# 基礎工具函數
# ==========================================
def clean_num(x):
    """清洗數字字串，移除逗號與百分比"""
    if pd.isna(x): return 0.0
    if isinstance(x, str):
        x = x.replace(',', '').replace('%', '').strip()
        if not x or x in ['---', 'X']: return 0.0
    try:
        return float(x)
    except:
        return 0.0


def get_tw_date_str(date_obj):
    """轉換為民國年字串 (例如 115/03/17)"""
    return f"{date_obj.year - 1911}/{date_obj.strftime('%m/%d')}"


def calc_pct(a, b):
    """計算百分比，先乘 100 再取小數點後 2 位，解決 0.0 的問題"""
    return round((a / b) * 100, 2) if b and b != 0 else 0.0


def to_shares_round(val):
    """轉換為張數並對齊 MoneyDJ 的整數四捨五入邏輯"""
    return round(val / 1000, 0)


# ==========================================
# 核心網路爬蟲與解析 (Pandas O(1) 處理)
# ==========================================
class TaiwanStockDataFetcher:
    def __init__(self, target_date):
        self.date = target_date  # datetime object
        self.twse_date = self.date.strftime('%Y%m%d')
        self.tpex_date = get_tw_date_str(self.date)

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        # 讀取 .env 中的 Proxy 設定
        http_proxy = os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy')
        https_proxy = os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy')
        self.proxies = {}
        if http_proxy: self.proxies['http'] = http_proxy
        if https_proxy: self.proxies['https'] = https_proxy

        if self.proxies:
            print(f"🌐 已偵測並套用 Proxy 設定: {self.proxies}")

    def _get_csv_lines(self, url):
        res = requests.get(url, headers=self.headers, proxies=self.proxies, timeout=15)
        res.encoding = 'utf-8'  # 避免亂碼
        if res.status_code != 200: return []
        text = res.text.encode('latin1', errors='ignore').decode('big5', errors='ignore') if 'big5' in res.headers.get(
            'content-type', '').lower() else res.text
        return [line.strip() for line in text.split('\n') if line.strip()]

    def _get_json_data(self, url):
        res = requests.get(url, headers=self.headers, proxies=self.proxies, timeout=15)
        if res.status_code != 200: return None
        try:
            return res.json()
        except:
            return None

    # --- 三大法人買賣超 ---
    def fetch_inst(self):
        records = []
        # 1. 上市 (TWSE)
        print(f"📡 抓取 TWSE 三大法人...")
        url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={self.twse_date}&selectType=ALL&response=csv"
        lines = self._get_csv_lines(url)
        for line in lines:
            cols = [c.strip('"').strip() for c in line.split('","')]
            if len(cols) >= 19 and cols[0].isdigit():
                f_buy_sell = to_shares_round(clean_num(cols[4]) + clean_num(cols[7]))
                t_buy_sell = to_shares_round(clean_num(cols[10]))
                d_buy_sell = to_shares_round(clean_num(cols[11]))
                records.append({
                    'sid': cols[0],
                    'f_buy_sell': f_buy_sell,
                    't_buy_sell': t_buy_sell,
                    'd_buy_sell': d_buy_sell,
                    'total_buy_sell': f_buy_sell + t_buy_sell + d_buy_sell
                })

        # 2. 上櫃 (TPEx) - 加入了 &o=json
        print(f"📡 抓取 TPEx 三大法人...")
        url = f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=EW&t=D&d={self.tpex_date}&o=json"
        jdata = self._get_json_data(url)
        if jdata and 'tables' in jdata and len(jdata['tables']) > 0 and jdata['tables'][0].get('data'):
            for row in jdata['tables'][0]['data']:
                if len(row) >= 24 and str(row[0]).isdigit():
                    f_buy_sell = to_shares_round(clean_num(row[10]))
                    t_buy_sell = to_shares_round(clean_num(row[13]))
                    d_buy_sell = to_shares_round(clean_num(row[22]))
                    records.append({
                        'sid': str(row[0]),
                        'f_buy_sell': f_buy_sell,
                        't_buy_sell': t_buy_sell,
                        'd_buy_sell': d_buy_sell,
                        'total_buy_sell': f_buy_sell + t_buy_sell + d_buy_sell
                    })
        return pd.DataFrame(records).set_index('sid') if records else pd.DataFrame()

    # --- 外資持股統計 ---
    def fetch_foreign_hold(self):
        records = []
        # 1. 上市 (改用最新 RWD API)
        print(f"📡 抓取 TWSE 外資持股...")
        url = f"https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS?response=csv&date={self.twse_date}&selectType=ALLBUT0999"
        lines = self._get_csv_lines(url)
        for line in lines:
            cols = [c.strip('"').strip() for c in line.split('","')]
            if len(cols) >= 10 and cols[0].isdigit():
                records.append({
                    'sid': cols[0],
                    'issued_shares': to_shares_round(clean_num(cols[3])),
                    'f_hold': to_shares_round(clean_num(cols[5])),
                    'f_hold_pct': clean_num(cols[7])
                })

        # 2. 上櫃 (加入 &o=json)
        print(f"📡 抓取 TPEx 外資持股...")
        url = f"https://www.tpex.org.tw/web/stock/3insti/qfii/qfii_result.php?l=zh-tw&d={self.tpex_date}&o=json"
        jdata = self._get_json_data(url)
        if jdata and 'tables' in jdata and len(jdata['tables']) > 0 and jdata['tables'][0].get('data'):
            for row in jdata['tables'][0]['data']:
                if len(row) >= 8 and (str(row[0]).isdigit() or str(row[1]).isdigit()):
                    sid = str(row[0]) if str(row[0]).isdigit() else str(row[1])
                    records.append({
                        'sid': sid,
                        'issued_shares': to_shares_round(clean_num(row[3])),
                        'f_hold': to_shares_round(clean_num(row[5])),
                        'f_hold_pct': clean_num(row[7] if len(row) > 7 else 0)
                    })
        return pd.DataFrame(records).set_index('sid') if records else pd.DataFrame()

    # --- 信用交易(融資券) ---
    def fetch_margin(self):
        records = []
        # 1. 上市 (改用最新 RWD API)
        print(f"📡 抓取 TWSE 信用交易...")
        url = f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?response=csv&date={self.twse_date}&selectType=ALL"
        lines = self._get_csv_lines(url)
        for line in lines:
            cols = [c.strip('"').strip() for c in line.split('","')]
            if len(cols) >= 15 and cols[0].isdigit():
                limit = clean_num(cols[7])
                balance = clean_num(cols[6])
                records.append({
                    'sid': cols[0],
                    'fin_buy': clean_num(cols[2]), 'fin_sell': clean_num(cols[3]), 'fin_repay': clean_num(cols[4]),
                    'fin_balance': balance, 'fin_change': balance - clean_num(cols[5]),
                    'fin_limit': limit, 'fin_usage': calc_pct(balance, limit),
                    'short_buy': clean_num(cols[8]), 'short_sell': clean_num(cols[9]),
                    'short_repay': clean_num(cols[10]),
                    'short_balance': clean_num(cols[12]), 'short_change': clean_num(cols[12]) - clean_num(cols[11]),
                    'offset': clean_num(cols[14])
                })

        # 2. 上櫃 (加入 &o=json)
        print(f"📡 抓取 TPEx 信用交易...")
        url = f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&d={self.tpex_date}&o=json"
        jdata = self._get_json_data(url)
        if jdata and 'tables' in jdata and len(jdata['tables']) > 0 and jdata['tables'][0].get('data'):
            for row in jdata['tables'][0]['data']:
                if len(row) >= 19 and str(row[0]).isdigit():
                    balance = clean_num(row[6])
                    limit = clean_num(row[9])
                    records.append({
                        'sid': str(row[0]),
                        'fin_buy': clean_num(row[3]), 'fin_sell': clean_num(row[4]), 'fin_repay': clean_num(row[5]),
                        'fin_balance': balance, 'fin_change': balance - clean_num(row[2]),
                        'fin_limit': limit, 'fin_usage': calc_pct(balance, limit),
                        'short_sell': clean_num(row[11]), 'short_buy': clean_num(row[12]),
                        'short_repay': clean_num(row[13]),
                        'short_balance': clean_num(row[14]), 'short_change': clean_num(row[14]) - clean_num(row[10]),
                        'offset': clean_num(row[18])
                    })
        return pd.DataFrame(records).set_index('sid') if records else pd.DataFrame()


# ==========================================
# 寫入本機 JSON 處理器 (滾動運算)
# ==========================================
def process_local_json(sid, date_str, inst_row, margin_row):
    file_path = DATA_DIR / f"{sid}.json"
    if not file_path.exists():
        data = {"sid": sid, "institutional_investors": [], "margin_trading": []}
    else:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            return False, "Read Error"

    # --- 處理三大法人 ---
    if inst_row is not None and not inst_row.empty:
        prev_inst = data.get('institutional_investors', [])
        baseline_idx = 1 if prev_inst and prev_inst[0]['date'] == date_str else 0

        prev_t_hold = prev_inst[baseline_idx].get('invest_trust_hold', 0) if len(prev_inst) > baseline_idx else 0
        prev_d_hold = prev_inst[baseline_idx].get('dealer_hold', 0) if len(prev_inst) > baseline_idx else 0

        curr_t_hold = max(0, prev_t_hold + inst_row.get('t_buy_sell', 0))
        curr_d_hold = max(0, prev_d_hold + inst_row.get('d_buy_sell', 0))

        issued = inst_row.get('issued_shares', 0)
        t_pct = calc_pct(curr_t_hold, issued)
        d_pct = calc_pct(curr_d_hold, issued)

        f_hold_pct = inst_row.get('f_hold_pct', 0)
        total_legal_pct = round(f_hold_pct + t_pct + d_pct, 2)

        new_inst = {
            "date": date_str,
            "foreign_buy_sell": round(inst_row.get('f_buy_sell', 0), 0),
            "invest_trust_buy_sell": round(inst_row.get('t_buy_sell', 0), 0),
            "dealer_buy_sell": round(inst_row.get('d_buy_sell', 0), 0),
            "total_buy_sell": round(inst_row.get('total_buy_sell', 0), 0),
            "foreign_hold": round(inst_row.get('f_hold', 0), 0),
            "invest_trust_hold": round(curr_t_hold, 0),
            "dealer_hold": round(curr_d_hold, 0),
            "total_hold": round(inst_row.get('f_hold', 0) + curr_t_hold + curr_d_hold, 0),
            "foreign_hold_pct": round(f_hold_pct, 2),
            "total_legal_pct": total_legal_pct
        }

        if prev_inst and prev_inst[0]['date'] == date_str:
            prev_inst[0] = new_inst
        else:
            prev_inst.insert(0, new_inst)
        data['institutional_investors'] = prev_inst

    # --- 處理信用交易 ---
    if margin_row is not None and not margin_row.empty:
        prev_margin = data.get('margin_trading', [])
        short_bal = margin_row.get('short_balance', 0)
        fin_bal = margin_row.get('fin_balance', 0)
        ratio = calc_pct(short_bal, fin_bal)

        new_margin = {
            "date": date_str,
            "fin_buy": round(margin_row.get('fin_buy', 0), 0),
            "fin_sell": round(margin_row.get('fin_sell', 0), 0),
            "fin_repay": round(margin_row.get('fin_repay', 0), 0),
            "fin_balance": round(margin_row.get('fin_balance', 0), 0),
            "fin_change": round(margin_row.get('fin_change', 0), 0),
            "fin_limit": round(margin_row.get('fin_limit', 0), 0),
            "fin_usage": round(margin_row.get('fin_usage', 0), 2),
            "short_sell": round(margin_row.get('short_sell', 0), 0),
            "short_buy": round(margin_row.get('short_buy', 0), 0),
            "short_repay": round(margin_row.get('short_repay', 0), 0),
            "short_balance": round(short_bal, 0),
            "short_change": round(margin_row.get('short_change', 0), 0),
            "ratio": ratio,
            "offset": round(margin_row.get('offset', 0), 0)
        }

        if prev_margin and prev_margin[0]['date'] == date_str:
            prev_margin[0] = new_margin
        else:
            prev_margin.insert(0, new_margin)
        data['margin_trading'] = prev_margin

    data['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True, "Success"
    except Exception as e:
        return False, str(e)


# ==========================================
# 自動追溯引擎與主程式
# ==========================================
def get_local_latest_date():
    """探測本機資料的最新日期"""
    for test_sid in ["2330", "0050"]:
        test_path = DATA_DIR / f"{test_sid}.json"
        if test_path.exists():
            try:
                with open(test_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    inst = data.get('institutional_investors', [])
                    if inst:
                        return datetime.strptime(inst[0]['date'], '%Y-%m-%d')
            except:
                continue
    return datetime.now() - timedelta(days=3)


def run_for_date(target_date, mode, target_stocks):
    date_str = target_date.strftime('%Y-%m-%d')
    print(f"\n▶️ 開始處理日期: {date_str} (模式: {mode})", flush=True)

    fetcher = TaiwanStockDataFetcher(target_date)
    df_inst = pd.DataFrame()
    df_margin = pd.DataFrame()

    if mode in ['all', 'inst']:
        df_i = fetcher.fetch_inst()
        df_f = fetcher.fetch_foreign_hold()

        # 只要三大法人抓成功，就勇敢存下來！
        if not df_i.empty:
            if not df_f.empty:
                df_inst = df_i.join(df_f, how='left').fillna(0)
            else:
                df_inst = df_i.copy()
            print(f"  ✅ 三大法人合併完成", flush=True)

    if mode in ['all', 'margin']:
        df_margin = fetcher.fetch_margin()
        if not df_margin.empty:
            print(f"  ✅ 信用交易合併完成", flush=True)

    # 如果兩個都空，才代表真的沒資料
    if df_inst.empty and df_margin.empty:
        print(f"  ⏩ {date_str} 查無資料 (可能是假日、尚未公佈或休市)，跳過。", flush=True)
        return False

    print(f"  📦 寫入 JSON 中...", flush=True)
    success_count = 0
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {}
        for sid in target_stocks:
            inst_row = df_inst.loc[sid] if not df_inst.empty and sid in df_inst.index else None
            margin_row = df_margin.loc[sid] if not df_margin.empty and sid in df_margin.index else None

            if inst_row is not None or margin_row is not None:
                futures[executor.submit(process_local_json, sid, date_str, inst_row, margin_row)] = sid

        total_tasks = len(futures)
        from concurrent.futures import as_completed
        for i, future in enumerate(as_completed(futures)):
            ok, msg = future.result()
            if ok: success_count += 1

            if total_tasks > 0 and i % max(1, total_tasks // 20) == 0:
                print(f"PROGRESS: {int((i / total_tasks) * 100)}", flush=True)

    print(f"PROGRESS: 100", flush=True)
    print(f"  🎉 {date_str} 更新完畢！共寫入 {success_count} 檔。", flush=True)
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, default="", help='指定特定日期 YYYYMMDD')
    parser.add_argument('--mode', type=str, choices=['all', 'inst', 'margin'], default='all', help='更新模式')
    args = parser.parse_args()

    start_time = time.time()
    print(f"🚀 啟動極速籌碼更新引擎 (模式: {args.mode})")

    csv_path = PROJECT_ROOT / "data" / "stock_list.csv"
    if not csv_path.exists():
        print("❌ 找不到 stock_list.csv")
        return
    target_stocks = pd.read_csv(csv_path, dtype={'stock_id': str})['stock_id'].tolist()

    if args.date:
        target_date = datetime.strptime(args.date, '%Y%m%d')
        run_for_date(target_date, args.mode, target_stocks)
    else:
        local_latest = get_local_latest_date()
        today = datetime.now()

        current_date = local_latest + timedelta(days=1)
        if local_latest.date() == today.date():
            current_date = today

        print(f"🔍 本機最新資料日期為 {local_latest.strftime('%Y-%m-%d')}")

        if current_date.date() > today.date():
            print("✅ 資料已是最新的，無需更新。")
        else:
            while current_date.date() <= today.date():
                if current_date.date() == today.date() and today.hour < 15:
                    print(f"⏳ 今天尚未收盤公佈資料，略過。")
                    break

                run_for_date(current_date, args.mode, target_stocks)
                current_date += timedelta(days=1)
                time.sleep(1)

    print(f"PROGRESS: 100")
    print(f"\n⏱️ 總耗時: {time.time() - start_time:.2f} 秒")


if __name__ == "__main__":
    main()