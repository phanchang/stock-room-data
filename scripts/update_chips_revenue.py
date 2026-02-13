"""
StockWarRoom 核心數據整合器 V12.6 - 土洋對作增強版
1. [新增] 計算外資與投信近10日買賣天數、近3日動能。
2. [新增] 產出 'is_tu_yang' 訊號 (投信連賣且外資吃貨)。
3. [修復] 欄位與EPS抓取邏輯保持 V12.5 的穩定性。
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
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://www.twse.com.tw/'}
PROXIES = {'http': os.getenv("HTTP_PROXY"), 'https': os.getenv("HTTPS_PROXY")} if os.getenv("HTTP_PROXY") else None


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
        # 處理證交所常見的無數據符號
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
# 1. 籌碼面 (Chips) - 土洋對作增強版
# ==========================================
def fetch_chips_matrix():
    print(f"📡 [1/5] 抓取法人籌碼 (含土洋對作邏輯)...")
    # 抓取 25 天，確保有足夠樣本計算 10日/20日 數據
    days = get_trading_days(25)
    t_hist, f_hist = {}, {}

    for dt in days:
        d_str = dt.strftime('%Y%m%d')
        d_roc = f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"
        day_df = pd.DataFrame()

        # 避免被鎖 IP
        time.sleep(random.uniform(0.8, 1.5))

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
            # 轉為張數 (除以1000)
            t_hist[d_str] = day_df.set_index('sid')['t'].apply(parse_val) // 1000
            f_hist[d_str] = day_df.set_index('sid')['f'].apply(parse_val) // 1000
            print(".", end="", flush=True)

    print(" Done.")

    # 建立矩陣 (Columns 為日期，從最新到最舊)
    t_m, f_m = pd.DataFrame(t_hist).fillna(0), pd.DataFrame(f_hist).fillna(0)
    dates = sorted(t_m.columns, reverse=True)
    res = pd.DataFrame(index=t_m.index)

    # --- 基礎數據 ---
    res['t_net_today'] = t_m[dates[0]]
    res['f_net_today'] = f_m[dates[0]]
    res['t_sum_5d'] = t_m[dates[:5]].sum(axis=1)
    res['f_sum_5d'] = f_m[dates[:5]].sum(axis=1)
    res['t_sum_20d'] = t_m[dates[:20]].sum(axis=1)
    res['f_sum_20d'] = f_m[dates[:20]].sum(axis=1)
    res['t_streak'] = t_m[dates].apply(get_streak, axis=1)
    res['f_streak'] = f_m[dates].apply(get_streak, axis=1)

    # --- [New] 進階籌碼特徵 (土洋對作專用) ---
    dates_10 = dates[:10]
    f_10d_matrix = f_m[dates_10]
    t_10d_matrix = t_m[dates_10]

    # 2. 持續性計算 (Count Days)
    res['f_buy_days_10'] = (f_10d_matrix > 0).sum(axis=1)
    res['t_sell_days_10'] = (t_10d_matrix < 0).sum(axis=1)

    # 3. 累積量計算 (近10天)
    res['f_sum_10d'] = f_10d_matrix.sum(axis=1)
    res['t_sum_10d'] = t_10d_matrix.sum(axis=1)

    # 4. 近期動能 (近3天)
    res['f_sum_3d'] = f_m[dates[:3]].sum(axis=1)

    # 5. [判定] 土洋對作 Flag
    # 邏輯：A. 10天內外資買>6天 且 投信賣>6天; B. 總量外資買投信賣; C. 外資吃貨量大; D. 近3日沒落跑
    cond_consistency = (res['f_buy_days_10'] >= 7) & (res['t_sell_days_10'] >= 7)
    cond_magnitude = (res['f_sum_10d'] > 0) & (res['t_sum_10d'] < 0)
    cond_absorb = res['f_sum_10d'] > res['t_sum_10d'].abs()
    cond_recency = res['f_sum_3d'] > 0

    res['is_tu_yang'] = (cond_consistency & cond_magnitude & cond_absorb & cond_recency).astype(int)

    return res


# ==========================================
# 2. 融資融券 (Margin)
# ==========================================
def fetch_margin_matrix():
    print("📡 [2/5] 抓取資券變化 (歷史回溯矩陣)...")
    days = get_trading_days(25)
    m_hist, s_hist = {}, {}

    for dt in days:
        d_str = dt.strftime('%Y%m%d')
        d_roc = f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"
        day_df = pd.DataFrame()
        time.sleep(random.uniform(2.0, 3.0))

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
# 3. 營收 (Revenue)
# ==========================================
def fetch_revenue():
    print("📡 [3/5] 抓取月營收 (含累計年增)...")
    rs = []
    for url in ["https://openapi.twse.com.tw/v1/opendata/t187ap05_L",
                "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"]:
        try:
            res = requests.get(url, proxies=PROXIES, timeout=20, verify=False).json()
            df = pd.DataFrame(res)

            # [Fix] 先清洗長字串再清洗短字串
            df.columns = [c.replace('累計營業收入-', '').replace('營業收入-', '') for c in df.columns]

            df['rev_now'] = df.apply(lambda r: parse_val(r.get('當月營收', 0)), axis=1)
            df['rev_yoy'] = df.apply(lambda r: parse_val(r.get('去年同月增減(%)', 0)), axis=1)
            df['rev_cum_yoy'] = df.apply(lambda r: parse_val(r.get('前期比較增減(%)', 0)), axis=1)

            df = df.rename(columns={'公司代號': 'sid', '公司名稱': 'name', '產業別': 'industry', '資料年月': 'rev_ym'})

            if 'sid' in df.columns:
                rs.append(df[['sid', 'name', 'industry', 'rev_ym', 'rev_yoy', 'rev_now', 'rev_cum_yoy']])

        except Exception as e:
            print(f"Error fetching revenue ({url[-10:]}): {e}")

    return pd.concat(rs).set_index('sid') if rs else pd.DataFrame()


# ==========================================
# 4. 估值 (Valuation) - 修正版
# ==========================================
# ==========================================
# 4. 估值 (Valuation) - 效能與長假優化版
# ==========================================
def fetch_valuation():
    print("📡 [4/5] 抓取估值 (PE/PB/Yield)...")

    # 🔥 [關鍵優化]：直接取得最後一個有效的交易日日期物件
    # 不再用迴圈盲目猜，而是直接問 get_trading_days
    valid_days = get_trading_days(5)
    if not valid_days:
        print("   ❌ 無法取得有效交易日日期")
        return pd.DataFrame()

    vd = []

    # 我們試著從最近的一個交易日開始抓，若失敗再抓前一個
    for dt in valid_days[:3]:  # 通常前 1-2 個就一定會中
        d_str = dt.strftime('%Y%m%d')  # 上市用格式: 20260211
        d_roc = f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"  # 上櫃用格式: 115/02/11

        twse_data_found = False
        tpex_data_found = False

        # 1. 抓上市 (TWSE)
        try:
            url = f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?date={d_str}&selectType=ALL&response=json"
            res = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=10, verify=False).json()
            if res.get('stat') == 'OK' and 'data' in res:
                f = res['fields']
                iy, ipe, ipb = f.index("殖利率(%)"), f.index("本益比"), f.index("股價淨值比")
                for r in res['data']:
                    vd.append({'sid': r[0].strip(), 'pe': parse_val(r[ipe]), 'yield': parse_val(r[iy]),
                               'pbr': parse_val(r[ipb])})
                twse_data_found = True
        except:
            pass

        # 2. 抓上櫃 (TPEX)
        try:
            url = f"https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?l=zh-tw&o=json&d={d_roc}"
            res = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=10, verify=False).json()
            raw = res['tables'][0]['data'] if 'tables' in res and len(res['tables']) > 0 else []
            if raw:
                for r in raw:
                    vd.append(
                        {'sid': r[0].strip(), 'pe': parse_val(r[2]), 'yield': parse_val(r[5]), 'pbr': parse_val(r[6])})
                tpex_data_found = True
        except:
            pass

        # 如果這一天有抓到任何資料，就代表這就是最後一個交易日，直接結束不再往回找
        if twse_data_found or tpex_data_found:
            print(f"   ✅ 成功對齊最後交易日數據 (日期: {d_str})")
            break

    return pd.DataFrame(vd).set_index('sid') if vd else pd.DataFrame()

# ==========================================
# 5. EPS (New)
# ==========================================
# --- update_chips_revenue.py 修正部分 ---

def fetch_eps_data():
    print("📡 [5/5] 抓取最新季 EPS 與年度季別...")
    eps_list = []
    urls = ["https://openapi.twse.com.tw/v1/opendata/t187ap14_L",
            "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O"]

    for url in urls:
        try:
            res = requests.get(url, proxies=PROXIES, timeout=20, verify=False).json()
            for r in res:
                sid = r.get('公司代號') or r.get('SecuritiesCompanyCode')
                val_str = r.get('基本每股盈餘(元)') or r.get('基本每股盈餘')

                # 抓取年度與季別
                year = r.get('年度') or r.get('Year')
                quarter = r.get('季別') or r.get('Season')  # 有些 API 用 Season

                if sid and year and quarter:
                    # 合併為 114Q4 這種格式
                    eps_date = f"{str(year).strip()}Q{str(quarter).strip()}"
                    eps_list.append({
                        'sid': str(sid).strip(),
                        'eps_q': parse_val(val_str),
                        'eps_date': eps_date  # 新增合併欄位
                    })
        except:
            pass

    return pd.DataFrame(eps_list).set_index('sid') if eps_list else pd.DataFrame()

# ==========================================
# 主程式
# ==========================================
def main():
    p = Path(__file__).resolve().parent.parent / "data" / "temp" / "chips_revenue_raw.csv"
    p.parent.mkdir(parents=True, exist_ok=True)

    if PROXIES: print(f"🔒 使用 Proxy 模式")

    # 載入白名單
    project_root = Path(__file__).resolve().parent.parent
    white_list_path = project_root / "data" / "stock_list.csv"

    white_df = pd.read_csv(white_list_path, dtype={'stock_id': str})
    valid_sids = set(white_df['stock_id'].tolist())
    print(f"📋 載入白名單完成，共 {len(valid_sids)} 檔標的。")


    rev = fetch_revenue()
    chips = fetch_chips_matrix()
    margin = fetch_margin_matrix()
    val = fetch_valuation()
    eps = fetch_eps_data()

    print("\n🔄 數據大合體...")
    base = rev if not rev.empty else chips
    final = base.join([chips, margin, val, eps], how='left').fillna(0)

    if 'sid' not in final.columns:
        final = final.reset_index()

    # 🔥 [關鍵修正]：比對白名單，僅保留存在於 stock_list.csv 的 sid
    final['sid'] = final['sid'].astype(str).str.strip()
    before_count = len(final)
    final = final[final['sid'].isin(valid_sids)]
    after_count = len(final)
    print(f"🎯 白名單過濾完成：從 {before_count} 檔過濾至 {after_count} 檔 (已剔除存託憑證等標的)")

    final.to_csv(p, index=False, encoding='utf-8-sig')
    print(f"\n✨ V12.6 戰情室數據就緒！\n位置: {p}")


if __name__ == "__main__": main()