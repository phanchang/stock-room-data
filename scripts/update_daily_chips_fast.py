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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

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
# 核心網路爬蟲與解析
# ==========================================
# ==========================================
# 核心網路爬蟲與解析
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
        """🛡️ 萬用 JSON 解析：解決櫃買中心 API 格式不統一的歷史共業"""
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
# 處理 JSON 儲存 (包含資料繼承邏輯)
# ==========================================
def process_local_json(sid, date_str, inst_row, margin_row):
    file_path = DATA_DIR / f"{sid}.json"
    if not file_path.exists():
        data = {"sid": sid, "institutional_investors": [], "margin_trading": []}
    else:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except:
            return False, "Read Error"

    if inst_row is not None:
        prev_inst = data.get('institutional_investors', [])
        base_idx = 0
        if prev_inst and prev_inst[0]['date'] == date_str: base_idx = 1

        has_history = len(prev_inst) > base_idx
        prev_data = prev_inst[base_idx] if has_history else {}

        # 繼承股本，避免比率歸零
        issued = inst_row.get('issued_shares', 0)
        if issued <= 0: issued = prev_data.get('issued_shares', 0)

        # 繼承外資持股，防止資料斷崖
        f_hold_pct = inst_row.get('f_hold_pct', 0)
        f_hold_vol = inst_row.get('f_hold', 0)
        if f_hold_pct <= 0 and has_history:
            f_hold_pct = prev_data.get('foreign_hold_pct', 0)
            f_hold_vol = prev_data.get('foreign_hold', 0)

        prev_t_hold = prev_data.get('invest_trust_hold', 0)
        prev_d_hold = prev_data.get('dealer_hold', 0)
        curr_t_hold = max(0, prev_t_hold + inst_row.get('t_buy_sell', 0))
        curr_d_hold = max(0, prev_d_hold + inst_row.get('d_buy_sell', 0))

        t_pct = calc_pct(curr_t_hold, issued)
        d_pct = calc_pct(curr_d_hold, issued)
        total_legal_pct = round(f_hold_pct + t_pct + d_pct, 2)

        new_inst = {
            "date": date_str, "foreign_buy_sell": round(inst_row.get('f_buy_sell', 0), 0),
            "invest_trust_buy_sell": round(inst_row.get('t_buy_sell', 0), 0),
            "dealer_buy_sell": round(inst_row.get('d_buy_sell', 0), 0),
            "total_buy_sell": round(inst_row.get('total_buy_sell', 0), 0),
            "foreign_hold": round(f_hold_vol, 0), "invest_trust_hold": round(curr_t_hold, 0),
            "dealer_hold": round(curr_d_hold, 0), "total_hold": round(f_hold_vol + curr_t_hold + curr_d_hold, 0),
            "foreign_hold_pct": round(f_hold_pct, 2), "total_legal_pct": total_legal_pct, "issued_shares": issued
        }
        if prev_inst and prev_inst[0]['date'] == date_str:
            prev_inst[0] = new_inst
        else:
            prev_inst.insert(0, new_inst)
        data['institutional_investors'] = prev_inst

    if margin_row is not None:
        prev_margin = data.get('margin_trading', [])
        new_margin = {
            "date": date_str, "fin_buy": round(margin_row.get('fin_buy', 0), 0),
            "fin_sell": round(margin_row.get('fin_sell', 0), 0), "fin_repay": round(margin_row.get('fin_repay', 0), 0),
            "fin_balance": round(margin_row.get('fin_balance', 0), 0),
            "fin_change": round(margin_row.get('fin_change', 0), 0),
            "fin_limit": round(margin_row.get('fin_limit', 0), 0),
            "fin_usage": round(margin_row.get('fin_usage', 0), 2),
            "short_sell": round(margin_row.get('short_sell', 0), 0),
            "short_buy": round(margin_row.get('short_buy', 0), 0),
            "short_repay": round(margin_row.get('short_repay', 0), 0),
            "short_balance": round(margin_row.get('short_balance', 0), 0),
            "short_change": round(margin_row.get('short_change', 0), 0),
            "ratio": calc_pct(margin_row.get('short_balance', 0), margin_row.get('fin_balance', 0)),
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
    except:
        return False, "Write Error"


# ==========================================
# 引擎控制 (包含你的原始計時邏輯)
# ==========================================
def get_local_latest_date(mode):
    key = 'margin_trading' if mode == 'margin' else 'institutional_investors'
    for sid in ["2330", "0050", "2303"]:
        p = DATA_DIR / f"{sid}.json"
        if p.exists():
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    d = json.load(f).get(key, [])
                    if d: return datetime.strptime(d[0]['date'], '%Y-%m-%d')
            except:
                pass
    return datetime.now() - timedelta(days=3)


def run_for_date(target_date, mode, target_stocks):
    date_str = target_date.strftime('%Y-%m-%d')
    print(f"\n▶️ 開始處理日期: {date_str} (模式: {mode})", flush=True)
    fetcher = TaiwanStockDataFetcher(target_date)
    df_inst, df_margin = pd.DataFrame(), pd.DataFrame()

    if mode in ['all', 'inst']:
        df_i, df_f = fetcher.fetch_inst(), fetcher.fetch_foreign_hold()

        # 🛡️ 關鍵修復：改用 Outer Join (pd.concat)，任一邊有資料絕對保留！
        if not df_i.empty or not df_f.empty:
            df_inst = pd.concat([df_i, df_f], axis=1).fillna(0)
        else:
            df_inst = pd.DataFrame()

    if mode in ['all', 'margin']:
        df_margin = fetcher.fetch_margin()

    if df_inst.empty and df_margin.empty:
        print(f"  ⏩ {date_str} 查無資料，跳過。", flush=True)
        return False

    success_count = 0
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {}
        for sid in target_stocks:
            i_row = df_inst.loc[sid] if not df_inst.empty and sid in df_inst.index else None
            m_row = df_margin.loc[sid] if not df_margin.empty and sid in df_margin.index else None
            if i_row is not None or m_row is not None:
                futures[executor.submit(process_local_json, sid, date_str, i_row, m_row)] = sid
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
    print(f"🚀 啟動極速籌碼更新引擎 | 模式: {args.mode}")

    csv_path = PROJECT_ROOT / "data" / "stock_list.csv"
    if not csv_path.exists(): return
    target_stocks = pd.read_csv(csv_path, dtype={'stock_id': str})['stock_id'].tolist()

    any_success = False

    if args.date:
        # 手動補單一日期，例如: python script.py --date 20260318
        any_success = run_for_date(datetime.strptime(args.date, '%Y%m%d'), args.mode, target_stocks)
    else:
        latest = get_local_latest_date(args.mode)
        print(f"🔍 [本機狀態] {args.mode} 資料最新日期: {latest.strftime('%Y-%m-%d')}")

        # --- 🤖 智慧自動補坑邏輯 ---
        # 檢查 2330 (指標股) 最後一天的資料是否異常 (比率為 0)
        need_repair = False
        try:
            with open(DATA_DIR / "2330.json", 'r', encoding='utf-8') as f:
                last_data = json.load(f).get('institutional_investors', [])
                if last_data and last_data[0]['total_legal_pct'] == 0:
                    need_repair = True
        except:
            pass

        if need_repair:
            print("⚠️ 偵測到上次更新數據不完整 (0.0%)，自動啟動修復模式...")
            current_date = latest  # 從最後一天開始重跑
        else:
            current_date = latest + timedelta(days=1)  # 正常往後跑
        # ------------------------

        today = datetime.now()
        while current_date.date() <= today.date():
            if current_date.date() == today.date():
                if args.mode == 'margin' and today.hour < 21:
                    print(f"⏳ 今天({current_date.strftime('%m/%d')})資券尚未公布，跳過。");
                    break
                elif today.hour < 15:
                    print(f"⏳ 今天({current_date.strftime('%m/%d')})尚未收盤，跳過。");
                    break

            if run_for_date(current_date, args.mode, target_stocks): any_success = True
            current_date += timedelta(days=1)
            time.sleep(0.5)

    if not any_success:
        print("\n[EMPTY_UPDATE] 🈳 資料已是最新。")
    else:
        print("\n[DATA_UPDATED] ✅ 資料更新成功。")
    print(f"PROGRESS: 100\n⏱️ 總耗時: {time.time() - start_time:.2f} 秒")


if __name__ == "__main__":
    main()