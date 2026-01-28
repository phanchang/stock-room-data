"""
StockWarRoom 核心數據整合器 V12.2 - 資券矩陣精準版
基於 V12.0 (User Verified) 進行擴充：
1. [資券] 升級為 25日 歷史矩陣，以計算 5/10/20 日累計。
2. [校正] 上櫃資券邏輯嚴格遵守 Schema：融資(6-2), 融券(14-10)。
3. [防護] 加入請求延遲，避免 307 封鎖。
"""

import pandas as pd
import requests
from pathlib import Path
import urllib3
import os
import time
import random
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

def get_roc_date(dt):
    # 修正: 配合矩陣迴圈，直接傳入 datetime 物件轉換
    return f"{dt.year-1911}/{dt.month:02d}/{dt.day:02d}"

def parse_val(v):
    try:
        if isinstance(v, (int, float)): return float(v)
        v = str(v).strip().replace(',', '')
        return 0.0 if v in ['-', '', 'N/A', 'null'] else float(v)
    except: return 0.0

def get_streak(series):
    """計算連買/連賣"""
    vals = series.values
    if len(vals) == 0 or vals[0] == 0: return 0
    count, is_buying = 0, (vals[0] > 0)
    for v in vals:
        if (is_buying and v > 0): count += 1
        elif (not is_buying and v < 0): count -= 1
        else: break
    return count

# ==========================================
# 1. 籌碼面 (Chips) - V12.0 原封不動
# ==========================================
def fetch_chips_matrix():
    print(f"📡 [1/4] 抓取法人籌碼 (連買校準模式)...")
    days = get_trading_days(25)
    t_hist, f_hist = {}, {}

    for dt in days:
        d_str = dt.strftime('%Y%m%d')
        # 配合 V12 邏輯的日期格式
        d_roc = f"{dt.year-1911}/{dt.month:02d}/{dt.day:02d}"

        day_df = pd.DataFrame()

        # 為了安全，稍微休息一下
        time.sleep(random.uniform(1.0, 2.0))

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
# 2. 融資融券 (Margin) - 升級矩陣模式
# ==========================================
def fetch_margin_matrix():
    print("📡 [2/4] 抓取資券變化 (歷史回溯矩陣)...")
    days = get_trading_days(25)
    m_hist, s_hist = {}, {}

    for dt in days:
        d_str = dt.strftime('%Y%m%d')
        d_roc = f"{dt.year-1911}/{dt.month:02d}/{dt.day:02d}"
        day_df = pd.DataFrame()

        # [安全保護] 避免連續請求導致 307，強制休息
        time.sleep(random.uniform(2.0, 3.0))

        # 上市 (TWSE)
        try:
            url = f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={d_str}&selectType=ALL&response=json"
            res = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=15, verify=False).json()
            if res.get('stat') == 'OK':
                target = next((t for t in res.get('tables', []) if len(t.get('fields', [])) == 16), None)
                if target:
                    temp = []
                    for r in target['data']:
                        # V12 邏輯: 6(今)-5(昨), 12(今)-11(昨)
                        m_diff = parse_val(r[6]) - parse_val(r[5])
                        s_diff = parse_val(r[12]) - parse_val(r[11])
                        temp.append({'sid': r[0].strip(), 'm': int(m_diff), 's': int(s_diff)})
                    day_df = pd.concat([day_df, pd.DataFrame(temp)])
        except: pass

        # 上櫃 (TPEx) - [關鍵修正] 使用您提供的正確 Schema 索引
        try:
            url = f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&se=EW&d={d_roc}&t=D"
            res = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=15, verify=False).json()
            raw = res['tables'][0]['data'] if 'tables' in res else res.get('aaData', [])
            if raw:
                temp = []
                for r in raw:
                    # 依據 Schema: [2]前資, [6]資餘額 => 6-2
                    m_diff = parse_val(r[6]) - parse_val(r[2])
                    # 依據 Schema: [10]前券, [14]券餘額 => 14-10
                    s_diff = parse_val(r[14]) - parse_val(r[10])
                    temp.append({'sid': r[0].strip(), 'm': int(m_diff), 's': int(s_diff)})
                day_df = pd.concat([day_df, pd.DataFrame(temp)])
        except: pass

        if not day_df.empty:
            m_hist[d_str] = day_df.set_index('sid')['m']
            s_hist[d_str] = day_df.set_index('sid')['s']
            print(".", end="", flush=True)
        else:
            print("x", end="", flush=True)

    print(" Done.")

    # 建立 DataFrame 並計算累計
    m_m, s_m = pd.DataFrame(m_hist).fillna(0), pd.DataFrame(s_hist).fillna(0)
    if m_m.empty: return pd.DataFrame()

    dates = sorted(m_m.columns, reverse=True)
    res = pd.DataFrame(index=m_m.index)

    # 這裡加入您要的 1日/5日/10日/20日
    res['m_net_today'] = m_m[dates[0]]
    res['m_sum_5d'] = m_m[dates[:5]].sum(axis=1)
    res['m_sum_10d'] = m_m[dates[:10]].sum(axis=1) # 新增
    res['m_sum_20d'] = m_m[dates[:20]].sum(axis=1)

    res['s_net_today'] = s_m[dates[0]]
    res['s_sum_5d'] = s_m[dates[:5]].sum(axis=1)
    res['s_sum_10d'] = s_m[dates[:10]].sum(axis=1) # 新增
    res['s_sum_20d'] = s_m[dates[:20]].sum(axis=1)

    return res

# ==========================================
# 3. 營收 (Revenue) - V12.0 原封不動
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

# ==========================================
# 4. 估值 (Valuation) - V12.0 原封不動
# ==========================================
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
        dt = datetime.now() - timedelta(days=offset)
        d_roc = f"{dt.year-1911}/{dt.month:02d}/{dt.day:02d}"
        try:
            res = requests.get(f"https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?l=zh-tw&o=json&d={d_roc}", headers=HEADERS, proxies=PROXIES, verify=False).json()
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

    # 改用 fetch_margin_matrix
    rev, chips, margin, val = fetch_revenue(), fetch_chips_matrix(), fetch_margin_matrix(), fetch_valuation()

    print("\n🔄 數據大合體...")
    final = rev.join([chips, margin, val], how='left').fillna(0)

    final.to_csv(p, encoding='utf-8-sig')
    print(f"\n✨ V12.2 戰情室數據就緒！\n位置: {p}")

    if '2330' in final.index:
         print(f"📊 2330 資: {final.loc['2330'][['m_net_today', 'm_sum_5d']].to_dict()}")

if __name__ == "__main__": main()