import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv

load_dotenv()


def debug_print_tables(sid="6197"):
    url = f"https://norway.twsthr.info/StockHolders.aspx?stock={sid}"
    headers = {'User-Agent': 'Mozilla/5.0'}

    http_proxy = os.getenv("HTTP_PROXY", os.getenv("http_proxy"))
    https_proxy = os.getenv("HTTPS_PROXY", os.getenv("https_proxy"))
    proxies = {}
    if http_proxy: proxies["http"] = http_proxy
    if https_proxy: proxies["https"] = https_proxy

    print(f"🔍 抓取網頁中: {url}")
    try:
        res = requests.get(url, headers=headers, proxies=proxies, timeout=8)
        soup = BeautifulSoup(res.text, 'html.parser')

        tables = soup.find_all('table')
        print(f"✅ 共找到 {len(tables)} 個 table，印出前幾列供比對結構：\n")

        for i, tbl in enumerate(tables):
            print(f"--- Table [{i}] ---")
            rows = tbl.find_all('tr')
            # 每個 table 只印前 4 列來找特徵
            for j, tr in enumerate(rows[:4]):
                cells = [c.get_text(strip=True).replace('\xa0', '') for c in tr.find_all(['td', 'th'])]
                print(f"  Row {j}: {cells}")
            print("-" * 30 + "\n")

    except Exception as e:
        print(f"❌ 錯誤: {e}")


if __name__ == "__main__":
    debug_print_tables("6197")