"""
StockWarRoom 核心數據整合器 V12.7 - 進度回報修復版
加入 PROGRESS 關鍵字輸出，解決 SettingsModule 進度條不動的問題。
"""

import pandas as pd
import requests
from pathlib import Path
import urllib3
import os
import time
import random
import sys  # 必要：用於 flush stdout
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 初始化
load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 常數設定
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://www.twse.com.tw/'}
PROXIES = {'http': os.getenv("HTTP_PROXY"), 'https': os.getenv("HTTPS_PROXY")} if os.getenv("HTTP_PROXY") else None


def report_progress(val):
    """回報進度給 UI 監聽 (格式: PROGRESS: 數字)"""
    print(f"PROGRESS: {int(val)}")
    sys.stdout.flush()


def get_trading_days(n=25):
    days, offset = [], 0
    while len(days) < n and offset < 60:
        dt = datetime.now() - timedelta(days=offset)
        if dt.weekday() < 5: days.append(dt)
        offset += 1
    return days


def parse_val(v):
    if v is None: return 0.0
    try:
        if isinstance(v, (int, float)): return float(v)
        v = str(v).strip().replace(',', '')
        if v in ['-', '', 'N/A', 'null', '0.00']: return 0.0
        return float(v)
    except:
        return 0.0


def get_streak(series):
    vals = series.values
    if len(vals) == 0 or vals[0] == 0: return 0
    count, is_buying = 0, (vals[0] > 0)
    for v in vals:
        if (is_buying and v > 0):
            count += 1
        elif (not is_buying and v < 0):
            count -= 1
        else:
            break
    return count


# ==========================================
# 1. 籌碼面 (Chips) - 進度 0% -> 40%
# ==========================================
def fetch_chips_matrix():
    print(f"📡 [1/5] 抓取法人籌碼 (含土洋對作邏輯)...")
    days = get_trading_days(25)
    t_hist, f_hist = {}, {}
    total_days = len(days)

    for i, dt in enumerate(days):
        # 計算進度 0~40%
        current_pct = 0 + (i / total_days) * 40
        report_progress(current_pct)

        d_str = dt.strftime('%Y%m%d')
        d_roc = f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"
        day_df = pd.DataFrame()

        time.sleep(random.uniform(0.5, 1.0))

        try:
            res = requests.get(f"https://www.twse.com.tw/rwd/zh/fund/T86?date={d_str}&selectType=ALL&response=json",
                               headers=HEADERS, proxies=PROXIES, timeout=15, verify=False).json()
            if res.get('stat') == 'OK':
                df = pd.DataFrame(res['data'], columns=res['fields'])
                idx_f = next(i for i, f in enumerate(res['fields']) if '外陸資' in f and '買賣超' in f)
                idx_t = next(i for i, f in enumerate(res['fields']) if '投信' in f and '買賣超' in f)
                day_df = pd.concat([day_df, df.iloc[:, [0, idx_f, idx_t]].rename(
                    columns={df.columns[0]: 'sid', df.columns[idx_f]: 'f', df.columns[idx_t]: 't'})])
        except:
            pass
        try:
            res = requests.get(
                f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=json&se=EW&t=D&d={d_roc}",
                headers=HEADERS, proxies=PROXIES, timeout=30, verify=False).json()
            raw = res['tables'][0]['data'] if 'tables' in res else res.get('aaData', [])
            if raw:
                df = pd.DataFrame(raw).iloc[:, [0, 4, 13]]
                df.columns = ['sid', 'f', 't']
                day_df = pd.concat([day_df, df])
        except:
            pass

        if not day_df.empty:
            day_df['sid'] = day_df['sid'].str.strip()
            t_hist[d_str] = day_df.set_index('sid')['t'].apply(parse_val) // 1000
            f_hist[d_str] = day_df.set_index('sid')['f'].apply(parse_val) // 1000
            print(".", end="", flush=True)

    print(" Done.")

    # 建立矩陣
    t_m, f_m = pd.DataFrame(t_hist).fillna(0), pd.DataFrame(f_hist).fillna(0)
    dates = sorted(t_m.columns, reverse=True)
    res = pd.DataFrame(index=t_m.index)

    if not t_m.empty:
        # 基礎數據
        res['t_net_today'] = t_m[dates[0]]
        res['f_net_today'] = f_m[dates[0]]
        res['t_sum_5d'] = t_m[dates[:5]].sum(axis=1)
        res['f_sum_5d'] = f_m[dates[:5]].sum(axis=1)
        res['t_sum_20d'] = t_m[dates[:20]].sum(axis=1)
        res['f_sum_20d'] = f_m[dates[:20]].sum(axis=1)
        res['t_streak'] = t_m[dates].apply(get_streak, axis=1)
        res['f_streak'] = f_m[dates].apply(get_streak, axis=1)

        # 土洋對作邏輯
        dates_10 = dates[:10]
        f_10d_matrix = f_m[dates_10]
        t_10d_matrix = t_m[dates_10]
        res['f_buy_days_10'] = (f_10d_matrix > 0).sum(axis=1)
        res['t_sell_days_10'] = (t_10d_matrix < 0).sum(axis=1)
        res['f_sum_10d'] = f_10d_matrix.sum(axis=1)
        res['t_sum_10d'] = t_10d_matrix.sum(axis=1)
        res['f_sum_3d'] = f_m[dates[:3]].sum(axis=1)

        cond_consistency = (res['f_buy_days_10'] >= 7) & (res['t_sell_days_10'] >= 7)
        cond_magnitude = (res['f_sum_10d'] > 0) & (res['t_sum_10d'] < 0)
        cond_absorb = res['f_sum_10d'] > res['t_sum_10d'].abs()
        cond_recency = res['f_sum_3d'] > 0
        res['is_tu_yang'] = (cond_consistency & cond_magnitude & cond_absorb & cond_recency).astype(int)

    return res


# ==========================================
# 2. 融資融券 (Margin) - 進度 40% -> 70%
# ==========================================
def fetch_margin_matrix():
    print("\n📡 [2/5] 抓取資券變化 (歷史回溯矩陣)...")
    days = get_trading_days(25)
    m_hist, s_hist = {}, {}
    total_days = len(days)

    for i, dt in enumerate(days):
        # 計算進度 40~70%
        current_pct = 40 + (i / total_days) * 30
        report_progress(current_pct)

        d_str = dt.strftime('%Y%m%d')
        d_roc = f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"
        day_df = pd.DataFrame()
        time.sleep(random.uniform(1.0, 1.5))

        try:
            url = f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={d_str}&selectType=ALL&response=json"
            res = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=15, verify=False).json()
            if res.get('stat') == 'OK':
                target = next((t for t in res.get('tables', []) if len(t.get('fields', [])) == 16), None)
                if target:
                    temp = []
                    for r in target['data']:
                        m_diff = parse_val(r[6]) - parse_val(r[5])
                        s_diff = parse_val(r[12]) - parse_val(r[11])
                        temp.append({'sid': r[0].strip(), 'm': int(m_diff), 's': int(s_diff)})
                    day_df = pd.concat([day_df, pd.DataFrame(temp)])
        except:
            pass

        try:
            url = f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&se=EW&d={d_roc}&t=D"
            res = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=15, verify=False).json()
            raw = res['tables'][0]['data'] if 'tables' in res else res.get('aaData', [])
            if raw:
                temp = []
                for r in raw:
                    m_diff = parse_val(r[6]) - parse_val(r[2])
                    s_diff = parse_val(r[14]) - parse_val(r[10])
                    temp.append({'sid': r[0].strip(), 'm': int(m_diff), 's': int(s_diff)})
                day_df = pd.concat([day_df, pd.DataFrame(temp)])
        except:
            pass

        if not day_df.empty:
            m_hist[d_str] = day_df.set_index('sid')['m']
            s_hist[d_str] = day_df.set_index('sid')['s']
            print(".", end="", flush=True)
        else:
            print("x", end="", flush=True)

    print(" Done.")
    m_m, s_m = pd.DataFrame(m_hist).fillna(0), pd.DataFrame(s_hist).fillna(0)
    if m_m.empty: return pd.DataFrame()

    dates = sorted(m_m.columns, reverse=True)
    res = pd.DataFrame(index=m_m.index)
    res['m_net_today'] = m_m[dates[0]]
    res['m_sum_5d'] = m_m[dates[:5]].sum(axis=1)
    res['m_sum_10d'] = m_m[dates[:10]].sum(axis=1)
    res['m_sum_20d'] = m_m[dates[:20]].sum(axis=1)
    res['s_net_today'] = s_m[dates[0]]
    res['s_sum_5d'] = s_m[dates[:5]].sum(axis=1)
    res['s_sum_10d'] = s_m[dates[:10]].sum(axis=1)
    res['s_sum_20d'] = s_m[dates[:20]].sum(axis=1)
    return res


# ==========================================
# 3. 營收 (Revenue) - 進度 70% -> 80%
# ==========================================
def fetch_revenue():
    print("\n📡 [3/5] 抓取月營收 (含累計年增)...")
    report_progress(75)

    rs = []
    for url in ["https://openapi.twse.com.tw/v1/opendata/t187ap05_L",
                "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"]:
        try:
            res = requests.get(url, proxies=PROXIES, timeout=20, verify=False).json()
            df = pd.DataFrame(res)
            df.columns = [c.replace('累計營業收入-', '').replace('營業收入-', '') for c in df.columns]

            df['rev_now'] = df.apply(lambda r: parse_val(r.get('當月營收', 0)), axis=1)
            df['rev_yoy'] = df.apply(lambda r: parse_val(r.get('去年同月增減(%)', 0)), axis=1)
            df['rev_cum_yoy'] = df.apply(lambda r: parse_val(r.get('前期比較增減(%)', 0)), axis=1)

            df = df.rename(columns={'公司代號': 'sid', '公司名稱': 'name', '產業別': 'industry', '資料年月': 'rev_ym'})
            if 'sid' in df.columns:
                rs.append(df[['sid', 'name', 'industry', 'rev_ym', 'rev_yoy', 'rev_now', 'rev_cum_yoy']])
        except Exception as e:
            print(f"Error fetching revenue ({url[-10:]}): {e}")

    report_progress(80)
    return pd.concat(rs).set_index('sid') if rs else pd.DataFrame()


# ==========================================
# 4. 估值 (Valuation) - 進度 80% -> 90%
# ==========================================
def fetch_valuation():
    print("📡 [4/5] 抓取估值 (PE/PB/Yield)...")
    report_progress(85)

    valid_days = get_trading_days(5)
    vd = []

    if valid_days:
        for dt in valid_days[:3]:
            d_str = dt.strftime('%Y%m%d')
            d_roc = f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"
            twse_found = False
            tpex_found = False

            try:
                url = f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?date={d_str}&selectType=ALL&response=json"
                res = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=10, verify=False).json()
                if res.get('stat') == 'OK' and 'data' in res:
                    f = res['fields']
                    iy, ipe, ipb = f.index("殖利率(%)"), f.index("本益比"), f.index("股價淨值比")
                    for r in res['data']:
                        vd.append({'sid': r[0].strip(), 'pe': parse_val(r[ipe]), 'yield': parse_val(r[iy]),
                                   'pbr': parse_val(r[ipb])})
                    twse_found = True
            except:
                pass

            try:
                url = f"https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?l=zh-tw&o=json&d={d_roc}"
                res = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=10, verify=False).json()
                raw = res['tables'][0]['data'] if 'tables' in res else []
                if raw:
                    for r in raw:
                        vd.append({'sid': r[0].strip(), 'pe': parse_val(r[2]), 'yield': parse_val(r[5]), 'pbr': parse_val(r[6])})
                    tpex_found = True
            except:
                pass

            if twse_found or tpex_found:
                print(f"   ✅ 成功對齊最後交易日數據 (日期: {d_str})")
                break

    report_progress(90)
    return pd.DataFrame(vd).set_index('sid') if vd else pd.DataFrame()


# ==========================================
# 5. EPS - 進度 90% -> 95%
# ==========================================
def fetch_eps_data():
    print("📡 [5/5] 抓取最新季 EPS...")
    report_progress(92)

    eps_list = []
    urls = ["https://openapi.twse.com.tw/v1/opendata/t187ap14_L",
            "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O"]

    for url in urls:
        try:
            res = requests.get(url, proxies=PROXIES, timeout=20, verify=False).json()
            for r in res:
                sid = r.get('公司代號') or r.get('SecuritiesCompanyCode')
                val_str = r.get('基本每股盈餘(元)') or r.get('基本每股盈餘')
                year = r.get('年度') or r.get('Year')
                quarter = r.get('季別') or r.get('Season')
                if sid and year and quarter:
                    eps_date = f"{str(year).strip()}Q{str(quarter).strip()}"
                    eps_list.append({'sid': str(sid).strip(), 'eps_q': parse_val(val_str), 'eps_date': eps_date})
        except:
            pass

    report_progress(95)
    return pd.DataFrame(eps_list).set_index('sid') if eps_list else pd.DataFrame()


# ==========================================
# 主程式
# ==========================================
def main():
    print("[System] 啟動全市場籌碼更新 (V12.7)")
    report_progress(0)

    p = Path(__file__).resolve().parent.parent / "data" / "temp" / "chips_revenue_raw.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    project_root = Path(__file__).resolve().parent.parent
    white_list_path = project_root / "data" / "stock_list.csv"

    # 1. 載入白名單
    white_df = pd.read_csv(white_list_path, dtype={'stock_id': str})
    valid_sids = set(white_df['stock_id'].tolist())
    print(f"📋 載入白名單完成，共 {len(valid_sids)} 檔標的。")

    # 2. 執行五大抓取
    rev = fetch_revenue()
    chips = fetch_chips_matrix()
    margin = fetch_margin_matrix()
    val = fetch_valuation()
    eps = fetch_eps_data()

    # 3. 合併與過濾
    print("\n🔄 數據大合體...")
    report_progress(98)

    base = rev if not rev.empty else chips
    final = base.join([chips, margin, val, eps], how='left').fillna(0)

    if 'sid' not in final.columns:
        final = final.reset_index()

    final['sid'] = final['sid'].astype(str).str.strip()
    before_count = len(final)
    final = final[final['sid'].isin(valid_sids)]
    after_count = len(final)
    print(f"🎯 白名單過濾完成：{before_count} -> {after_count}")

    final.to_csv(p, index=False, encoding='utf-8-sig')

    print(f"\n✨ 數據更新完成！\n位置: {p}")
    report_progress(100)

if __name__ == "__main__": main()