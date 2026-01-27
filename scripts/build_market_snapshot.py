import pandas as pd
import requests
from pathlib import Path
import urllib3
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 1. 初始化
load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 參數設定
LOOKBACK_WINDOW = 22  # 多抓幾天備用，確保能取足 20 個交易日
PROXIES = {'http': os.getenv("HTTP_PROXY"), 'https': os.getenv("HTTPS_PROXY")} if os.getenv("HTTP_PROXY") else None
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://www.tpex.org.tw/'
}


def get_trading_days(n=22):
    """取得最近 N 個交易日日期"""
    days = []
    offset = 0
    while len(days) < n and offset < 40:
        dt = datetime.now() - timedelta(days=offset)
        if dt.weekday() < 5:
            days.append(dt)
        offset += 1
    return days


def fetch_chips_matrix():
    """抓取全市場 20 日籌碼矩陣"""
    trading_days = get_trading_days(LOOKBACK_WINDOW)
    trust_history = {}
    foreign_history = {}

    print(f"📡 正在拉取過去 {len(trading_days)} 個交易日籌碼...")

    for dt in trading_days:
        d_str = dt.strftime('%Y%m%d')
        d_slash = dt.strftime('%Y/%m/%d')
        day_chips = pd.DataFrame()

        # A. 上市
        try:
            url_l = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={d_str}&selectType=ALL&response=json"
            resp_l = requests.get(url_l, headers=HEADERS, proxies=PROXIES, timeout=10, verify=False)
            data_l = resp_l.json()
            if 'data' in data_l:
                df_l = pd.DataFrame(data_l['data'], columns=data_l['fields'])
                df_l = df_l.rename(
                    columns={'證券代號': 'sid', '外陸資買賣超股數(不含外資自營商)': 'f_net', '投信買賣超股數': 't_net'})
                df_l['sid'] = df_l['sid'].str.strip()
                df_l['f_net'] = df_l['f_net'].str.replace(',', '').astype(float) // 1000
                df_l['t_net'] = df_l['t_net'].str.replace(',', '').astype(float) // 1000
                day_chips = pd.concat([day_chips, df_l[['sid', 'f_net', 't_net']]])
        except:
            pass

        # B. 上櫃 (使用驗證成功的精準索引 [4, 13])
        try:
            url_o = f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=en-us&o=json&se=EW&t=D&d={d_slash}"
            resp_o = requests.get(url_o, headers=HEADERS, proxies=PROXIES, timeout=10, verify=False)
            data_o = resp_o.json()
            if data_o.get('tables') and data_o['tables'][0].get('data'):
                df_o = pd.DataFrame(data_o['tables'][0]['data'])
                df_o = df_o.rename(columns={0: 'sid', 4: 'f_net', 13: 't_net'})
                df_o['sid'] = df_o['sid'].str.strip()
                df_o['f_net'] = df_o['f_net'].str.replace(',', '').astype(float) // 1000
                df_o['t_net'] = df_o['t_net'].str.replace(',', '').astype(float) // 1000
                day_chips = pd.concat([day_chips, df_o[['sid', 'f_net', 't_net']]])
        except:
            pass

        if not day_chips.empty:
            trust_history[d_str] = day_chips.set_index('sid')['t_net']
            foreign_history[d_str] = day_chips.set_index('sid')['f_net']
            print(f" ✅ {d_str}", end="")
            time.sleep(0.5)  # 微秒延遲

    print("\n🧮 正在計算法人動能指標...")
    t_matrix = pd.DataFrame(trust_history).fillna(0)
    f_matrix = pd.DataFrame(foreign_history).fillna(0)

    # 排序日期由新到舊
    dates = sorted(t_matrix.columns, reverse=True)
    t_matrix = t_matrix[dates]

    # 加工欄位計算
    stats = pd.DataFrame(index=t_matrix.index)
    stats['t_net_today'] = t_matrix[dates[0]]
    stats['t_sum_5d'] = t_matrix[dates[:5]].sum(axis=1)
    stats['t_sum_20d'] = t_matrix[dates[:20]].sum(axis=1)

    # 買超佔比 (解決雜訊問題)
    stats['t_ratio_10d'] = (t_matrix[dates[:10]] > 0).sum(axis=1) / 10

    # 嚴格連買天數
    def get_streak(row):
        count = 0
        for v in row:
            if v > 0:
                count += 1
            else:
                break
        return count

    stats['t_streak'] = t_matrix.apply(get_streak, axis=1)

    return stats, dates[0]


def fetch_revenue():
    """抓取月營收 (OpenAPI)"""
    print("📡 正在抓取最新月營收數據...")
    urls = [
        "https://openapi.twse.com.tw/v1/opendata/t187ap05_L",
        "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"
    ]
    all_rev = []
    for url in urls:
        try:
            r = requests.get(url, proxies=PROXIES, timeout=15, verify=False)
            df = pd.DataFrame(r.json())
            df.columns = [c.replace('營業收入-', '') for c in df.columns]
            df = df.rename(
                columns={'公司代號': 'sid', '公司名稱': 'name', '產業別': 'industry', '去年同月增減(%)': 'rev_yoy',
                         '資料年月': 'rev_ym'})
            all_rev.append(df[['sid', 'name', 'industry', 'rev_ym', 'rev_yoy']])
        except:
            pass
    return pd.concat(all_rev).set_index('sid') if all_rev else pd.DataFrame()


def main():
    project_root = Path(__file__).resolve().parent.parent
    output_path = project_root / "data" / "summary" / "market_daily_snapshot.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 1. 執行加工運算
    chips_stats, chips_date = fetch_chips_matrix()
    rev_df = fetch_revenue()

    # 2. 合併
    final_df = rev_df.join(chips_stats, how='left').fillna(0)
    final_df['chips_date'] = chips_date

    # 3. 標註訊號 (Signal)
    # 邏輯：投信10日買超佔比 > 0.6 且 5日累計為正 = 積極關注
    def define_signal(row):
        if row['t_ratio_10d'] >= 0.7 and row['t_sum_5d'] > 0: return "🔥🔥強勢認養"
        if row['t_streak'] >= 3: return "🚀剛發動"
        if row['t_sum_5d'] > 0 and row['t_ratio_10d'] < 0.4: return "⚠️大買後連賣"
        return ""

    final_df['signal'] = final_df.apply(define_signal, axis=1)

    # 4. 存檔與預覽
    final_df.to_csv(output_path, encoding='utf-8-sig')
    print("-" * 50)
    print(f"✨ 快照產製完成！位置: {output_path}")
    print(f"📊 篩選結果 (投信連買股):")
    print(final_df[final_df['t_streak'] >= 3][['name', 'rev_yoy', 't_streak', 't_sum_5d', 'signal']].head(10))
    print("-" * 50)


if __name__ == "__main__":
    main()