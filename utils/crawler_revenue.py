# ==================================================
# crawler_revenue.py
# 月營收爬蟲（MoneyDJ）
# 抓最近 37 個月，由新到舊
# ==================================================

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# --- proxy（沿用你三大法人的設定） ---
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

def convert_tw_ym(tw_ym: str) -> datetime:
    """
    民國 年/月 → datetime (當月第一天)
    例：114/11 → 2025-11-01
    """
    y, m = map(int, tw_ym.split("/"))
    return datetime(y + 1911, m, 1)

def get_monthly_revenue(stock_code: str, months: int = 37) -> pd.DataFrame:
    """
    取得最新 N 個月營收資料
    """
    url = f"https://concords.moneydj.com/z/zc/zch/zch_{stock_code}.djhtm"

    try:
        res = requests.get(url, headers=HEADERS, proxies=proxies, timeout=10)
        res.encoding = "big5"
    except Exception as e:
        print(f"? 網頁抓取失敗: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(res.text, "html.parser")

    rows = soup.find_all("tr")
    records = []

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) == 7:
            ym_text = tds[0].get_text(strip=True)
            if ym_text[:3].isdigit():  # 例如 114/11
                row = [td.get_text(strip=True).replace(",", "") for td in tds]
                records.append(row)

    if not records:
        print("? 沒抓到營收資料，請檢查網頁結構")
        return pd.DataFrame()

    df = pd.DataFrame(records, columns=[
        "年月",
        "營收",
        "月增率",
        "去年同期",
        "年增率",
        "累計營收",
        "累計年增率"
    ])

    # --- 型別轉換 ---
    df["日期"] = df["年月"].apply(convert_tw_ym)

    num_cols = ["營收", "去年同期", "累計營收"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    pct_cols = ["月增率", "年增率", "累計年增率"]
    for c in pct_cols:
        df[c] = df[c].str.replace("%", "", regex=False)
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 最新在最上
    df = df.sort_values("日期", ascending=False).reset_index(drop=True)

    # 只取最新 N 個月
    df = df.head(months)

    return df

# ==================================================
# 測試
# ==================================================
if __name__ == "__main__":
    stock = "2330"
    df = get_monthly_revenue(stock)

    if df.empty:
        print("? 無資料")
    else:
        print(df[[
            "年月", "營收", "月增率", "年增率", "累計營收"
        ]])
