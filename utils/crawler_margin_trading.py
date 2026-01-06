# ==================================================
# crawler_margin_trading.py
# 融資融券（日資料）
# 資料來源：MoneyDJ
# ==================================================

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
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

def roc_to_ad(date_str: str):
    """民國日期轉西元 datetime"""
    roc_year, month, day = date_str.split("/")
    year = int(roc_year) + 1911
    return datetime(year, int(month), int(day))


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
    """

    # ---------- 日期區間 ----------
    end_date = datetime.today()
    from_date = end_date - relativedelta(months=9)

    end_date_str = f"{end_date.year}-{end_date.month}-{end_date.day}"
    from_date_str = f"{from_date.year}-{from_date.month}-{from_date.day}"

    url = (
        "https://concords.moneydj.com/z/zc/zcn/zcn.djhtm"
        f"?a={stock_code}&c={from_date_str}&d={end_date_str}"
    )

    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers,proxies=proxies, timeout=15)
    resp.encoding = "big5"

    soup = BeautifulSoup(resp.text, "html.parser")

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

    # ✅ 只接受真正的「資料列」（民國日期）
    # 排除：表頭、合計列、空列
        if (
            "/" not in first_text
            or len(first_text) != 9      # 114/12/22
            or not first_text[:3].isdigit()
        ):
            continue

        raw = [td.get_text(strip=True) for td in tds[:15]]

        row = [
            roc_to_ad(raw[0]),
            *[clean_number(v) for v in raw[1:]]
        ]

        rows.append(row)

    df = pd.DataFrame(rows, columns=columns)
    df.sort_values("date",ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


# ==================================================
# 簡單列印測試
# ==================================================
if __name__ == "__main__":
    df = get_margin_trading("2330")

    print("?? 融資融券資料（前 5 筆）")
    print(df.head())

    print("\n?? 欄位型態")
    print(df.dtypes)

    print(f"\n?? 總筆數：{len(df)}")
