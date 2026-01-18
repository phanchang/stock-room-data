# ==================================================
# crawler_profitability.py
# 獲利能力分析（季報）
# 資料來源：MoneyDJ
# 直接抓網頁表格欄位，不自行計算
# 含 proxy + headers
# ==================================================

import os
import urllib3  # 1. 新增：用來關閉警告
import requests
from bs4 import BeautifulSoup
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- headers ---
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}

def get_profitability(stock_code: str) -> pd.DataFrame:
    """
    取得獲利能力分析（季報）
    """
    curr_proxy = os.environ.get("HTTP_PROXY")
    proxies_config = {"http": curr_proxy, "https": curr_proxy} if curr_proxy else None

    url = f"https://concords.moneydj.com/z/zc/zce/zce_{stock_code}.djhtm"

    try:
        res = requests.get(
            url,
            headers=HEADERS,
            proxies=proxies_config,  # ✅ 這裡使用函數內定義的變數
            timeout=10,
            verify=False  # ✅ 解決 SSLError
        )
        res.encoding = "big5"
    except Exception as e:
        print(f"? 網頁抓取失敗: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(res.text, "html.parser")

    table = soup.find("table", id="oMainTable")
    if not table:
        print("? 找不到獲利能力表格")
        return pd.DataFrame()

    rows = table.find_all("tr")
    records = []

    for tr in rows:
        tds = tr.find_all("td")
        # 正常資料列會有 11 欄
        if len(tds) == 11:
            first_col = tds[0].get_text(strip=True)
            # 季別格式例如：114.3Q
            if "." in first_col and "Q" in first_col:
                row = [
                    td.get_text(strip=True).replace(",", "")
                    for td in tds
                ]
                records.append(row)

    if not records:
        print("? 未抓到任何獲利能力資料")
        return pd.DataFrame()

    df = pd.DataFrame(records, columns=[
        "季別",
        "營業收入",
        "營業成本",
        "營業毛利",
        "毛利率",
        "營業利益",
        "營益率",
        "業外收支",
        "稅前淨利",
        "稅後淨利",
        "EPS"
    ])

    # 數值欄位轉換
    num_cols = [
        "營業收入",
        "營業成本",
        "營業毛利",
        "營業利益",
        "業外收支",
        "稅前淨利",
        "稅後淨利",
        "EPS"
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 百分比欄位處理
    df["毛利率"] = (
        df["毛利率"]
        .astype(str)
        .str.replace("%", "", regex=False)
    )
    df["營益率"] = (
        df["營益率"]
        .astype(str)
        .str.replace("%", "", regex=False)
    )

    df["毛利率"] = pd.to_numeric(df["毛利率"], errors="coerce")
    df["營益率"] = pd.to_numeric(df["營益率"], errors="coerce")

    # 最新季在最上
    df = df.reset_index(drop=True)
    # 只取前 20 筆資料
    df = df.head(20)
    return df


# ==================================================
# 測試
# ==================================================
if __name__ == "__main__":
    stock = "2330"
    df = get_profitability(stock)

    if df.empty:
        print("? 無資料")
    else:
        print("? 獲利能力資料如下：")
        print(df)
