# ==================================================
# crawler_fa.py
# 三大法人爬蟲（直接抓網頁欄位，不自行計算）
# 含 proxy + headers
# 抓最近 12 個月資料
# ==================================================

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# --- proxy 設定 ---
PROXY = "http://10.160.3.88:8080"
proxies = {
    "http": PROXY,
    "https": PROXY
}

# --- headers ---
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}

def convert_date(tw_date: str) -> datetime:
    """民國日期 → datetime"""
    y, m, d = map(int, tw_date.split("/"))
    return datetime(y + 1911, m, d)

def get_fa_ren(stock_code: str) -> pd.DataFrame:
    """
    取得三大法人最近 6 個月資料（含持股比重）
    """
    today = datetime.today()
    six_months_ago = today - relativedelta(months=12)

    c_str = six_months_ago.strftime("%Y-%m-%d")
    d_str = today.strftime("%Y-%m-%d")

    url = f"https://concords.moneydj.com/z/zc/zcl/zcl.djhtm?a={stock_code}&c={c_str}&d={d_str}"

    try:
        res = requests.get(url, headers=HEADERS, proxies=proxies, timeout=10)
        res.encoding = "big5"
    except Exception as e:
        print(f"❌ 網頁抓取失敗: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(res.text, "html.parser")
    rows = soup.find_all("tr")
    records = []

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) == 11:
            date_text = tds[0].get_text(strip=True).replace("\xa0", "")
            if date_text[:3].isdigit():  # 例如 114/12/17
                row = [td.get_text(strip=True).replace(",", "") for td in tds]
                records.append(row)

    if not records:
        print("❌ 沒抓到資料，請檢查 proxy 或網頁結構")
        return pd.DataFrame()

    df = pd.DataFrame(records, columns=[
        "日期",
        "外資買賣超股數",
        "投信買賣超股數",
        "自營商買賣超股數",
        "單日合計買賣超",
        "外資持股",
        "投信持股",
        "自營商持股",
        "單日合計持股",
        "外資持股比重",
        "三大法人持股比重"
    ])

    # 數值欄位轉換（遇到 '--' 會變 NaN）
    num_cols = [
        "外資買賣超股數", "投信買賣超股數", "自營商買賣超股數",
        "單日合計買賣超",
        "外資持股", "投信持股", "自營商持股", "單日合計持股"
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c].replace("--", None), errors="coerce")

    # 百分比欄位處理
    pct_cols = ["外資持股比重", "三大法人持股比重"]
    for c in pct_cols:
        df[c] = df[c].replace("--", None)               # '--' → NaN
        df[c] = df[c].astype(str).str.replace("%", "") # 移除百分比符號
        df[c] = pd.to_numeric(df[c], errors="coerce")  # 轉 float

    # 日期轉換
    df["日期"] = df["日期"].apply(convert_date)

    # 最新在最上
    df = df.sort_values("日期", ascending=False).reset_index(drop=True)

    return df

# ==================================================
# 測試
# ==================================================
if __name__ == "__main__":
    stock = "2330"
    df = get_fa_ren(stock)

    if df.empty:
        print("❌ 無資料")
    else:
        print(df)
