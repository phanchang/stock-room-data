# ==================================================
# utils/crawler_margin_trading.py
# 融資融券（日資料） - 強制 Proxy 版
# ==================================================

import requests
import urllib3
import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
from dotenv import load_dotenv  # 🟢 引入 dotenv

# 載入 .env 檔案
load_dotenv()

# 關閉 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- headers ---
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}


def roc_to_ad(date_str: str):
    """民國日期轉西元 datetime"""
    try:
        roc_year, month, day = date_str.split("/")
        year = int(roc_year) + 1911
        return datetime(year, int(month), int(day))
    except:
        return None


def clean_number(val: str):
    """去逗號、% → float"""
    if val is None:
        return None
    val = val.replace(",", "").replace("%", "")
    try:
        return float(val)
    except ValueError:
        return None


def get_margin_trading(stock_code: str) -> pd.DataFrame:
    """
    抓取融資融券資料（近 9 個月）
    邏輯：優先讀取 .env -> 其次自動偵測 -> 最後直連
    """

    # 🟢 1. 優先從 .env 讀取 (最高優先權)
    env_http = os.getenv("HTTP_PROXY")
    env_https = os.getenv("HTTPS_PROXY")

    proxies = {}

    if env_http or env_https:
        print(f"🌐 [網路] 使用 .env 設定的 Proxy: {env_http}")
        if env_http: proxies['http'] = env_http
        if env_https: proxies['https'] = env_https
    else:
        # 2. 如果 .env 沒設定，才嘗試系統自動偵測
        sys_proxies = urllib.request.getproxies()
        if sys_proxies:
            print(f"🌐 [網路] 偵測到系統 Proxy: {sys_proxies}")
            proxies = sys_proxies
        else:
            print(f"🌐 [網路] 無代理 (直連模式)")

    # ---------- 日期區間 ----------
    end_date = datetime.today()
    from_date = end_date - relativedelta(months=9)

    end_date_str = f"{end_date.year}-{end_date.month}-{end_date.day}"
    from_date_str = f"{from_date.year}-{from_date.month}-{from_date.day}"

    url = (
        "https://concords.moneydj.com/z/zc/zcn/zcn.djhtm"
        f"?a={stock_code}&c={from_date_str}&d={end_date_str}"
    )

    try:
        res = requests.get(
            url,
            headers=HEADERS,
            proxies=proxies,  # ✅ 使用決定好的 Proxy
            timeout=15,
            verify=False
        )
        res.encoding = "big5"
    except Exception as e:
        print(f"❌ [連線失敗] 無法連線至 MoneyDJ: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(res.text, "html.parser")

    columns = [
        "date",
        "fin_buy", "fin_sell", "fin_repay", "fin_balance",
        "fin_change", "fin_limit", "fin_usage",
        "short_sell", "short_buy", "short_repay", "short_balance",
        "short_change", "ratio", "offset"
    ]

    rows = []

    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 15:
            continue

        first_text = tds[0].get_text(strip=True)

        if (
                "/" not in first_text
                or len(first_text) != 9
                or not first_text[:3].isdigit()
        ):
            continue

        raw = [td.get_text(strip=True) for td in tds[:15]]

        dt = roc_to_ad(raw[0])
        if dt is None: continue

        row = [
            dt,
            *[clean_number(v) for v in raw[1:]]
        ]

        rows.append(row)

    df = pd.DataFrame(rows, columns=columns)
    if not df.empty:
        df.sort_values("date", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        print(f"✅ [成功] 抓取到 {len(df)} 筆資券資料")

    return df


if __name__ == "__main__":
    df = get_margin_trading("2330")
    print(df.head())