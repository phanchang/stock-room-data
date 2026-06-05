# 檔案路徑: scripts/update_daily_chips_v2.py
import sys
import os
import time
import requests
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from dotenv import load_dotenv

# 🔥 嘗試載入極速 JSON 庫，若無則降級使用原生 json
try:
    import orjson

    HAS_ORJSON = True
except ImportError:
    import json

    HAS_ORJSON = False

# 取得專案根目錄
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / '.env')

DATA_DIR = PROJECT_ROOT / "data" / "fundamentals"
DATA_DIR.mkdir(parents=True, exist_ok=True)


# ==========================================
# 基礎工具函數
# ==========================================
def clean_num(x):
    if pd.isna(x): return 0.0
    if isinstance(x, str):
        x = x.replace(',', '').replace('%', '').strip()
        if not x or x in ['---', 'X']: return 0.0
    try:
        return float(x)
    except:
        return 0.0


def get_tw_date_str(date_obj):
    return f"{date_obj.year - 1911}/{date_obj.strftime('%m/%d')}"


def calc_pct(a, b):
    return round((a / b) * 100, 2) if b and b != 0 else 0.0


def to_shares_round(val):
    return round(val / 1000, 0)


# ==========================================
# 高速 JSON 讀寫封裝
# ==========================================
def fast_read_json(file_path):
    try:
        with open(file_path, 'rb') as f:
            if HAS_ORJSON:
                return orjson.loads(f.read())
            else:
                return json.loads(f.read().decode('utf-8'))
    except:
        return None


def fast_write_json(file_path, data):
    try:
        with open(file_path, 'wb') as f:
            if HAS_ORJSON:
                f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))
            else:
                f.write(json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8'))
        return True
    except:
        return False


# ==========================================
# 核心網路爬蟲與解析 (維持原樣)
# ==========================================
class TaiwanStockDataFetcher:
    def __init__(self, target_date):
        self.date = target_date
        self.twse_date = self.date.strftime('%Y%m%d')
        self.tpex_date = get_tw_date_str(self.date)
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

        self.proxies = {}
        http_p = os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy')
        https_p = os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy')
        if http_p: self.proxies['http'] = http_p
        if https_p: self.proxies['https'] = https_p

    def _get_csv_lines(self, url):
        try:
            res = requests.get(url, headers=self.headers, proxies=self.proxies, timeout=15)
            if res.status_code != 200: return []
            res.encoding = 'big5' if 'big5' in res.headers.get('content-type', '').lower() else 'utf-8'
            return [line.strip() for line in res.text.split('\n') if line.strip()]
        except:
            return []

    def _get_json_data(self, url):
        try:
            res = requests.get(url, headers=self.headers, proxies=self.proxies, timeout=15)
            return res.json() if res.status_code == 200 else None
        except:
            return None

    def _extract_tpex_data(self, j):
        if not j: return []
        if 'aaData' in j: return j['aaData']
        if 'tables' in j and len(j['tables']) > 0 and 'data' in j['tables'][0]: return j['tables'][0]['data']
        if 'data' in j: return j['data']
        return []

    def fetch_inst(self):
        records = []
        lines = self._get_csv_lines(
            f"https://www.twse.com.tw/rwd/zh/fund/T86?date={self.twse_date}&selectType=ALL&response=csv")
        for line in lines:
            cols = [c.strip('"').strip() for c in line.split('","')]
            if len(cols) >= 19 and cols[0].isdigit():
                f = to_shares_round(clean_num(cols[4]) + clean_num(cols[7]))
                t = to_shares_round(clean_num(cols[10]))
                d = to_shares_round(clean_num(cols[11]))
                records.append(
                    {'sid': cols[0], 'f_buy_sell': f, 't_buy_sell': t, 'd_buy_sell': d, 'total_buy_sell': f + t + d})

        j = self._get_json_data(
            f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=EW&t=D&d={self.tpex_date}&o=json")
        for row in self._extract_tpex_data(j):
            if len(row) >= 24:
                sid = str(row[0]).strip()
                if sid.isdigit() and len(sid) >= 4:
                    f = to_shares_round(clean_num(row[10]))
                    t = to_shares_round(clean_num(row[13]))
                    d = to_shares_round(clean_num(row[22]))
                    records.append(
                        {'sid': sid, 'f_buy_sell': f, 't_buy_sell': t, 'd_buy_sell': d, 'total_buy_sell': f + t + d})
        return pd.DataFrame(records).set_index('sid') if records else pd.DataFrame()

    def fetch_foreign_hold(self):
        records = []
        lines = self._get_csv_lines(
            f"https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS?response=csv&date={self.twse_date}&selectType=ALLBUT0999")
        for line in lines:
            cols = [c.strip('"').strip() for c in line.split('","')]
            if len(cols) >= 10 and cols[0].isdigit():
                records.append({'sid': cols[0], 'issued_shares': to_shares_round(clean_num(cols[3])),
                                'f_hold': to_shares_round(clean_num(cols[5])), 'f_hold_pct': clean_num(cols[7])})

        j = self._get_json_data(
            f"https://www.tpex.org.tw/web/stock/3insti/qfii/qfii_result.php?l=zh-tw&d={self.tpex_date}&o=json")
        for row in self._extract_tpex_data(j):
            if len(row) >= 10:
                sid = str(row[1]).strip()
                if sid.isdigit() and len(sid) >= 4:
                    records.append({'sid': sid, 'issued_shares': to_shares_round(clean_num(row[3])),
                                    'f_hold': to_shares_round(clean_num(row[5])), 'f_hold_pct': clean_num(row[7])})
        return pd.DataFrame(records).set_index('sid') if records else pd.DataFrame()

    def fetch_margin(self):
        records = []
        lines = self._get_csv_lines(
            f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?response=csv&date={self.twse_date}&selectType=ALL")
        for line in lines:
            cols = [c.strip('"').strip() for c in line.split('","')]
            if len(cols) >= 15 and cols[0].isdigit():
                bal, limit = clean_num(cols[6]), clean_num(cols[7])
                records.append({
                    'sid': cols[0], 'fin_buy': clean_num(cols[2]), 'fin_sell': clean_num(cols[3]),
                    'fin_repay': clean_num(cols[4]),
                    'fin_balance': bal, 'fin_change': bal - clean_num(cols[5]), 'fin_limit': limit,
                    'fin_usage': calc_pct(bal, limit),
                    'short_sell': clean_num(cols[9]), 'short_buy': clean_num(cols[8]),
                    'short_repay': clean_num(cols[10]),
                    'short_balance': clean_num(cols[12]), 'short_change': clean_num(cols[12]) - clean_num(cols[11]),
                    'offset': clean_num(cols[14])
                })

        j = self._get_json_data(
            f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&d={self.tpex_date}&o=json")
        for row in self._extract_tpex_data(j):
            if len(row) >= 19:
                sid = str(row[0]).strip()
                if sid.isdigit() and len(sid) >= 4:
                    bal, limit = clean_num(row[6]), clean_num(row[9])
                    records.append({
                        'sid': sid, 'fin_buy': clean_num(row[3]), 'fin_sell': clean_num(row[4]),
                        'fin_repay': clean_num(row[5]),
                        'fin_balance': bal, 'fin_change': bal - clean_num(row[2]), 'fin_limit': limit,
                        'fin_usage': calc_pct(bal, limit),
                        'short_sell': clean_num(row[11]), 'short_buy': clean_num(row[12]),
                        'short_repay': clean_num(row[13]),
                        'short_balance': clean_num(row[14]), 'short_change': clean_num(row[14]) - clean_num(row[10]),
                        'offset': clean_num(row[18])
                    })
        return pd.DataFrame(records).set_index('sid') if records else pd.DataFrame()


# ==========================================
# 獨立函數：處理 JSON 儲存 (供多進程調用)
# ==========================================
# ==========================================
# 獨立函數：處理 JSON 儲存 (供多進程調用)
# ==========================================
def process_local_json_worker(sid, date_str, inst_row_dict, margin_row_dict, data_dir_str):
    file_path = Path(data_dir_str) / f"{sid}.json"

    data = fast_read_json(file_path)
    if not data:
        data = {"sid": sid, "institutional_investors": [], "margin_trading": []}

    # --- 內部工具：尋找比當前日期舊的「前一筆資料」供繼承 ---
    def get_prev_data(records_list, target_date):
        for r in records_list:
            if r['date'] < target_date: return r, True
        return {}, False

    # --- 內部工具：安全寫入並確保日期由大到小排序 ---
    def upsert_and_sort(records_list, new_record):
        updated = False
        for i, r in enumerate(records_list):
            if r['date'] == new_record['date']:
                records_list[i] = new_record
                updated = True
                break
        if not updated: records_list.append(new_record)
        records_list.sort(key=lambda x: x['date'], reverse=True)
        return records_list

    if inst_row_dict:
        prev_inst = data.get('institutional_investors', [])
        prev_data, has_history = get_prev_data(prev_inst, date_str)

        issued = inst_row_dict.get('issued_shares', 0)
        if issued <= 0: issued = prev_data.get('issued_shares', 0)

        f_hold_pct = inst_row_dict.get('f_hold_pct', 0)
        f_hold_vol = inst_row_dict.get('f_hold', 0)
        if f_hold_pct <= 0 and has_history:
            f_hold_pct = prev_data.get('foreign_hold_pct', 0)
            f_hold_vol = prev_data.get('foreign_hold', 0)

        prev_t_hold = prev_data.get('invest_trust_hold', 0)
        prev_d_hold = prev_data.get('dealer_hold', 0)
        curr_t_hold = max(0, prev_t_hold + inst_row_dict.get('t_buy_sell', 0))
        curr_d_hold = max(0, prev_d_hold + inst_row_dict.get('d_buy_sell', 0))

        t_pct = calc_pct(curr_t_hold, issued)
        d_pct = calc_pct(curr_d_hold, issued)
        total_legal_pct = round(f_hold_pct + t_pct + d_pct, 2)

        new_inst = {
            "date": date_str, "foreign_buy_sell": round(inst_row_dict.get('f_buy_sell', 0), 0),
            "invest_trust_buy_sell": round(inst_row_dict.get('t_buy_sell', 0), 0),
            "dealer_buy_sell": round(inst_row_dict.get('d_buy_sell', 0), 0),
            "total_buy_sell": round(inst_row_dict.get('total_buy_sell', 0), 0),
            "foreign_hold": round(f_hold_vol, 0), "invest_trust_hold": round(curr_t_hold, 0),
            "dealer_hold": round(curr_d_hold, 0), "total_hold": round(f_hold_vol + curr_t_hold + curr_d_hold, 0),
            "foreign_hold_pct": round(f_hold_pct, 2), "total_legal_pct": total_legal_pct, "issued_shares": issued
        }
        data['institutional_investors'] = upsert_and_sort(prev_inst, new_inst)

    if margin_row_dict:
        prev_margin = data.get('margin_trading', [])
        new_margin = {
            "date": date_str, "fin_buy": round(margin_row_dict.get('fin_buy', 0), 0),
            "fin_sell": round(margin_row_dict.get('fin_sell', 0), 0),
            "fin_repay": round(margin_row_dict.get('fin_repay', 0), 0),
            "fin_balance": round(margin_row_dict.get('fin_balance', 0), 0),
            "fin_change": round(margin_row_dict.get('fin_change', 0), 0),
            "fin_limit": round(margin_row_dict.get('fin_limit', 0), 0),
            "fin_usage": round(margin_row_dict.get('fin_usage', 0), 2),
            "short_sell": round(margin_row_dict.get('short_sell', 0), 0),
            "short_buy": round(margin_row_dict.get('short_buy', 0), 0),
            "short_repay": round(margin_row_dict.get('short_repay', 0), 0),
            "short_balance": round(margin_row_dict.get('short_balance', 0), 0),
            "short_change": round(margin_row_dict.get('short_change', 0), 0),
            "ratio": calc_pct(margin_row_dict.get('short_balance', 0), margin_row_dict.get('fin_balance', 0)),
            "offset": round(margin_row_dict.get('offset', 0), 0)
        }
        data['margin_trading'] = upsert_and_sort(prev_margin, new_margin)

    data['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    success = fast_write_json(file_path, data)
    return success, "Success" if success else "Write Error"

# ==========================================
# 引擎控制
# ==========================================
def get_dates_to_fetch(mode):
    today = datetime.now()
    today_str = today.strftime('%Y-%m-%d')
    # 嚴格限制只檢查過去 5 個「日曆天」的缺口，避免被異常的單邊歷史資料卡死
    cutoff_date_str = (today - timedelta(days=5)).strftime('%Y-%m-%d')

    target_dates = set()
    latest_d = '1970-01-01'

    # 檢查幾檔指標股的狀態
    for sid in ["2330", "0050", "2303"]:
        p = DATA_DIR / f"{sid}.json"
        if not p.exists(): continue
        data = fast_read_json(p)
        if not data: continue

        # 改用時間區間過濾，捨棄會造成錯位的 [:30] 陣列切片
        inst_dates = set(
            r['date'] for r in data.get('institutional_investors', []) if cutoff_date_str <= r['date'] <= today_str)
        margin_dates = set(
            r['date'] for r in data.get('margin_trading', []) if cutoff_date_str <= r['date'] <= today_str)

        if mode == 'all':
            gaps = (inst_dates - margin_dates) | (margin_dates - inst_dates)
        elif mode == 'margin':
            gaps = inst_dates - margin_dates
        elif mode == 'inst':
            gaps = margin_dates - inst_dates
        else:
            gaps = set()

        target_dates.update(gaps)

        # 找出本機最新的日期基準
        all_dates = list(inst_dates | margin_dates)
        if all_dates:
            local_max = max(all_dates)
            if local_max > latest_d:
                latest_d = local_max

    # 補上從最新日期到今天的未更新日期
    if latest_d != '1970-01-01':
        current = datetime.strptime(latest_d, '%Y-%m-%d') + timedelta(days=1)
        while current.date() <= today.date():
            target_dates.add(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
    else:
        # 若完全無資料，預設抓最近 3 天
        for i in range(3):
            target_dates.add((today - timedelta(days=i)).strftime('%Y-%m-%d'))

    sorted_dates = sorted(list(target_dates))
    return [datetime.strptime(d, '%Y-%m-%d') for d in sorted_dates]


def run_for_date(target_date, mode, target_stocks):
    date_str = target_date.strftime('%Y-%m-%d')
    print(f"\n▶️ 開始處理日期: {date_str} (模式: {mode})", flush=True)
    fetcher = TaiwanStockDataFetcher(target_date)
    df_inst, df_margin = pd.DataFrame(), pd.DataFrame()

    if mode in ['all', 'inst']:
        df_i, df_f = fetcher.fetch_inst(), fetcher.fetch_foreign_hold()
        if not df_i.empty or not df_f.empty:
            df_inst = pd.concat([df_i, df_f], axis=1).fillna(0)
        else:
            df_inst = pd.DataFrame()

    if mode in ['all', 'margin']:
        df_margin = fetcher.fetch_margin()

    if df_inst.empty and df_margin.empty:
        print(f"  ⏩ {date_str} 查無資料，跳過。", flush=True)
        return False

    # 轉為字典加速尋找，避免傳遞龐大 DataFrame 給子進程
    dict_inst = df_inst.to_dict('index') if not df_inst.empty else {}
    dict_margin = df_margin.to_dict('index') if not df_margin.empty else {}

    success_count = 0
    data_dir_str = str(DATA_DIR)

    # 🔥 核心升級：改用 ProcessPoolExecutor 榨乾多核心效能
    # max_workers 預設會使用系統所有的 CPU 核心數
    with ProcessPoolExecutor() as executor:
        futures = {}
        for sid in target_stocks:
            i_row = dict_inst.get(sid, None)
            m_row = dict_margin.get(sid, None)
            if i_row is not None or m_row is not None:
                # 提交任務到其他 CPU 核心
                futures[executor.submit(process_local_json_worker, sid, date_str, i_row, m_row, data_dir_str)] = sid

        total = len(futures)
        for i, future in enumerate(as_completed(futures)):
            if future.result()[0]: success_count += 1
            if total > 0 and i % max(1, total // 20) == 0:
                print(f"PROGRESS: {int((i / total) * 100)}", flush=True)

    print(f"  🎉 {date_str} 更新完畢！共寫入 {success_count} 檔。", flush=True)
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, default="")
    parser.add_argument('--mode', type=str, choices=['all', 'inst', 'margin'], default='all')
    args = parser.parse_args()

    start_time = time.time()
    engine_name = "orjson (極速)" if HAS_ORJSON else "原生 json (標準)"
    print(f"🚀 啟動精準籌碼更新引擎 V2 | 模式: {args.mode} | 引擎: {engine_name}")

    csv_path = PROJECT_ROOT / "data" / "stock_list.csv"
    if not csv_path.exists(): return
    target_stocks = pd.read_csv(csv_path, dtype={'stock_id': str})['stock_id'].tolist()

    any_success = False

    if args.date:
        any_success = run_for_date(datetime.strptime(args.date, '%Y%m%d'), args.mode, target_stocks)
    else:
        # 取得需要抓取的精確日期列表 (只含缺口與最新未抓取的日子)
        dates_to_fetch = get_dates_to_fetch(args.mode)

        if not dates_to_fetch:
            print("🔍 [本機狀態] 無偵測到任何缺口或需更新日期。")
        else:
            print(f"🔍 [本機狀態] 偵測到需更新/補缺的精確日期共 {len(dates_to_fetch)} 天。")

        today = datetime.now()
        for current_date in dates_to_fetch:
            if current_date.date() == today.date():
                if args.mode == 'margin' and today.hour < 21:
                    print(f"⏳ 今天({current_date.strftime('%m/%d')})資券尚未公布，跳過。")
                    continue
                elif today.hour < 15:
                    print(f"⏳ 今天({current_date.strftime('%m/%d')})尚未收盤，跳過。")
                    continue

            if run_for_date(current_date, args.mode, target_stocks):
                any_success = True
            time.sleep(0.5)

    if not any_success:
        print("\n[EMPTY_UPDATE] 🈳 資料已是最新。")
    else:
        print("\n[DATA_UPDATED] ✅ 資料更新成功。")
    print(f"PROGRESS: 100\n⏱️ 總耗時: {time.time() - start_time:.2f} 秒")


if __name__ == "__main__":
    main()