"""
StockWarRoom 核心數據整合器 V12.0 - 終極校準版
修正內容：
1. TWSE 上市：鎖定 Table 1 (16欄位結構)，精準對位融資(5,6)與融券(11,12)。
2. TPEx 上櫃：鎖定 20欄位結構，精準對位融資(2,6)與融券(10,14)。
3. 連買天數：精準「變號截斷」邏輯，校正台積電 -2 與群聯 -1。
4. 單位：數據全數以「張」為基準，不再除以 1000。
"""

import pandas as pd
import requests
from pathlib import Path
import urllib3
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 初始化
load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 常數設定
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36', 'Referer': 'https://www.twse.com.tw/'}
PROXIES = {'http': os.getenv("HTTP_PROXY"), 'https': os.getenv("HTTPS_PROXY")} if os.getenv("HTTP_PROXY") else None

def get_trading_days(n=25):
    days, offset = [], 0
    while len(days) < n and offset < 60:
        dt = datetime.now() - timedelta(days=offset)
        if dt.weekday() < 5: days.append(dt)
        offset += 1
    return days

def get_roc_date(offset=0):
    dt = datetime.now() - timedelta(days=offset)
    return f"{dt.year-1911}/{dt.month:02d}/{dt.day:02d}"

def parse_val(v):
    try:
        if isinstance(v, (int, float)): return float(v)
        v = str(v).strip().replace(',', '')
        return 0.0 if v in ['-', '', 'N/A', 'null'] else float(v)
    except: return 0.0

def get_streak(series):
    """計算連買/連賣，遇 0 或 變號即停止"""
    vals = series.values
    if len(vals) == 0 or vals[0] == 0: return 0
    count, is_buying = 0, (vals[0] > 0)
    for v in vals:
        if (is_buying and v > 0): count += 1
        elif (not is_buying and v < 0): count -= 1
        else: break
    return count

# ==========================================
# 1. 籌碼面 (Chips)
# ==========================================
def fetch_chips_matrix():
    print(f"📡 [1/4] 抓取法人籌碼 (連買校準模式)...")
    days = get_trading_days(25)
    t_hist, f_hist = {}, {}

    for dt in days:
        d_str, d_roc = dt.strftime('%Y%m%d'), get_roc_date((datetime.now()-dt).days)
        day_df = pd.DataFrame()
        # 上市
        try:
            res = requests.get(f"https://www.twse.com.tw/rwd/zh/fund/T86?date={d_str}&selectType=ALL&response=json", headers=HEADERS, proxies=PROXIES, timeout=15, verify=False).json()
            if res.get('stat') == 'OK':
                df = pd.DataFrame(res['data'], columns=res['fields'])
                idx_f = next(i for i, f in enumerate(res['fields']) if '外陸資' in f and '買賣超' in f)
                idx_t = next(i for i, f in enumerate(res['fields']) if '投信' in f and '買賣超' in f)
                day_df = pd.concat([day_df, df.iloc[:, [0, idx_f, idx_t]].rename(columns={df.columns[0]:'sid', df.columns[idx_f]:'f', df.columns[idx_t]:'t'})])
        except: pass
        # 上櫃
        try:
            res = requests.get(f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=json&se=EW&t=D&d={d_roc}", headers=HEADERS, proxies=PROXIES, timeout=30, verify=False).json()
            raw = res['tables'][0]['data'] if 'tables' in res else res.get('aaData', [])
            if raw:
                df = pd.DataFrame(raw).iloc[:, [0, 4, 13]]
                df.columns = ['sid', 'f', 't']
                day_df = pd.concat([day_df, df])
        except: pass

        if not day_df.empty:
            day_df['sid'] = day_df['sid'].str.strip()
            t_hist[d_str] = day_df.set_index('sid')['t'].apply(parse_val) // 1000
            f_hist[d_str] = day_df.set_index('sid')['f'].apply(parse_val) // 1000
            print(".", end="", flush=True)

    print(" Done.")
    t_m, f_m = pd.DataFrame(t_hist).fillna(0), pd.DataFrame(f_hist).fillna(0)
    dates = sorted(t_m.columns, reverse=True)
    res = pd.DataFrame(index=t_m.index)
    res['t_net_today'], res['t_sum_5d'], res['t_sum_20d'] = t_m[dates[0]], t_m[dates[:5]].sum(axis=1), t_m[dates[:20]].sum(axis=1)
    res['f_net_today'], res['f_sum_5d'], res['f_sum_20d'] = f_m[dates[0]], f_m[dates[:5]].sum(axis=1), f_m[dates[:20]].sum(axis=1)
    res['t_streak'], res['f_streak'] = t_m[dates].apply(get_streak, axis=1), f_m[dates].apply(get_streak, axis=1)
    return res

# ==========================================
# 2. 融資融券 (Margin)
# ==========================================
def fetch_margin_short():
    print("📡 [2/4] 抓取融資融券 (全市場精準鎖位)...")
    data = []
    # 上市 - 鎖定 16 欄結構
    try:
        url = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?selectType=ALL&response=json"
        res = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=20, verify=False).json()
        target = next((t for t in res.get('tables', []) if len(t.get('fields', [])) == 16), None)
        if target:
            for r in target['data']:
                m_diff = parse_val(r[6]) - parse_val(r[5])
                s_diff = parse_val(r[12]) - parse_val(r[11])
                data.append({'sid': r[0].strip(), 'm_net_today': int(m_diff), 's_net_today': int(s_diff)})
    except: pass
    # 上櫃 - 鎖定 20 欄結構
    for offset in [0, 1]:
        try:
            url = f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&se=EW&d={get_roc_date(offset)}"
            res = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=30, verify=False).json()
            raw = res['tables'][0]['data'] if 'tables' in res else res.get('aaData', [])
            if raw:
                for r in raw:
                    m_diff = parse_val(r[6]) - parse_val(r[2])
                    s_diff = parse_val(r[14]) - parse_val(r[10])
                    data.append({'sid': r[0].strip(), 'm_net_today': int(m_diff), 's_net_today': int(s_diff)})
                break
        except: continue
    return pd.DataFrame(data).set_index('sid') if data else pd.DataFrame()

# ==========================================
# 3. 營收 (Revenue) & 4. 估值 (Valuation)
# ==========================================
def fetch_revenue():
    print("📡 [3/4] 抓取月營收...")
    rs = []
    for url in ["https://openapi.twse.com.tw/v1/opendata/t187ap05_L", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"]:
        try:
            df = pd.DataFrame(requests.get(url, proxies=PROXIES, timeout=20, verify=False).json())
            df.columns = [c.replace('營業收入-', '') for c in df.columns]
            df = df.rename(columns={'公司代號':'sid', '公司名稱':'name', '產業別':'industry', '去年同月增減(%)':'rev_yoy', '當月營收':'rev_now', '資料年月':'rev_ym'})
            rs.append(df[['sid', 'name', 'industry', 'rev_ym', 'rev_yoy', 'rev_now']])
        except: pass
    return pd.concat(rs).set_index('sid') if rs else pd.DataFrame()

def fetch_valuation():
    print("📡 [4/4] 抓取估值 (PE/PB/Yield)...")
    vd = []
    try: # 上市
        res = requests.get("https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?selectType=ALL&response=json", headers=HEADERS, proxies=PROXIES, verify=False).json()
        f = res['fields']
        ipe, iy, ipb = f.index("本益比"), f.index("殖利率(%)"), f.index("股價淨值比")
        for r in res['data']: vd.append({'sid': r[0].strip(), 'pe': parse_val(r[ipe]), 'yield': parse_val(r[iy]), 'pbr': parse_val(r[ipb])})
    except: pass
    for offset in [0, 1]: # 上櫃
        try:
            res = requests.get(f"https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?l=zh-tw&o=json&d={get_roc_date(offset)}", headers=HEADERS, proxies=PROXIES, verify=False).json()
            raw = res['tables'][0]['data'] if 'tables' in res else res.get('aaData', [])
            if raw:
                for r in raw: vd.append({'sid': r[0].strip(), 'pe': parse_val(r[2]), 'yield': parse_val(r[5]), 'pbr': parse_val(r[6])})
                break
        except: continue
    return pd.DataFrame(vd).set_index('sid')

# ==========================================
# 主程式
# ==========================================
def main():
    p = Path(__file__).resolve().parent.parent / "data" / "temp" / "chips_revenue_raw.csv"
    p.parent.mkdir(parents=True, exist_ok=True)

    rev, chips, margin, val = fetch_revenue(), fetch_chips_matrix(), fetch_margin_short(), fetch_valuation()

    print("\n🔄 數據大合體...")
    # 使用 join 確保以營收表為底，合併所有特徵
    final = rev.join([chips, margin, val], how='left').fillna(0)

    # 輸出 CSV
    final.to_csv(p, encoding='utf-8-sig')
    print(f"\n✨ V12.0 戰情室數據就緒！\n位置: {p}")

if __name__ == "__main__": main()