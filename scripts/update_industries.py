# 檔案路徑: scripts/update_industries.py
import requests
from bs4 import BeautifulSoup
import re
import time
import os
import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# 強制即時輸出，避免進度條卡頓
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
load_dotenv(project_root / '.env')


def main():
    print("PROGRESS: 0")
    print("🚀 啟動 MoneyDJ 兩層式產業分類更新作業...")

    proxies = {}
    http_proxy = os.getenv('HTTP_PROXY') or os.getenv('http_proxy')
    https_proxy = os.getenv('HTTPS_PROXY') or os.getenv('https_proxy')
    if http_proxy: proxies['http'] = http_proxy
    if https_proxy: proxies['https'] = https_proxy

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    base_url = "https://www.moneydj.com/z/zh/zha/zha.djhtm"

    print("⏳ [1/2] 正在抓取產業分類樹...")
    try:
        res = requests.get(base_url, headers=headers, proxies=proxies, timeout=15)
        res.encoding = 'big5'
        soup = BeautifulSoup(res.text, 'html.parser')
    except Exception as e:
        print(f"❌ 取得主分類頁面失敗: {e}")
        return

    table = soup.find('table', id='oMainTable')
    if not table:
        print("❌ 找不到目標表格結構")
        return

    industry_map = []
    rows = table.find_all('tr')
    for row in rows[2:]:
        tds = row.find_all('td', recursive=False)
        if len(tds) >= 2:
            main_ind_tag = tds[0].find('a')
            if not main_ind_tag: continue
            main_ind = main_ind_tag.text.strip()

            sub_ind_tags = tds[1].find_all('a')
            for a in sub_ind_tags:
                sub_ind = a.text.strip()
                href = a.get('href', '')
                if href:
                    industry_map.append({
                        'main_ind': main_ind, 'sub_ind': sub_ind,
                        'url': f"https://www.moneydj.com{href}"
                    })

    print(f"✅ 成功取得 {len(industry_map)} 個細產業分類！")

    print("⏳ [2/2] 開始逐一抓取各分類成分股...")
    stock_data = {}
    total = len(industry_map)

    for i, item in enumerate(industry_map):
        pct = int(10 + (i / total) * 85)
        print(f"PROGRESS: {pct}")

        url, main_ind, sub_ind = item['url'], item['main_ind'], item['sub_ind']
        print(f"  [{i + 1}/{total}] 擷取: {main_ind} - {sub_ind}...")

        try:
            res = requests.get(url, headers=headers, proxies=proxies, timeout=10)
            res.encoding = 'big5'
            matches = re.findall(r"Link2Stk\('[a-zA-Z]*(\d{4,5})'\)[^>]*>(?:\d{4,5})?([^<]+)</a>", res.text)

            for sid, sname in set(matches):
                if sid not in stock_data:
                    stock_data[sid] = {'dj_main_ind': main_ind, 'dj_sub_ind': sub_ind}
                else:
                    if sub_ind not in stock_data[sid]['dj_sub_ind']:
                        stock_data[sid]['dj_sub_ind'] += f",{sub_ind}"
            time.sleep(1.0)
        except Exception as e:
            print(f"    ❌ 失敗: {e}")

    print("PROGRESS: 95")
    df_result = pd.DataFrame.from_dict(stock_data, orient='index')
    if not df_result.empty:
        df_result.index.name = 'sid'
        df_result.reset_index(inplace=True)

        save_path = project_root / 'data' / 'dj_industry.csv'
        df_result.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"✅ 更新完成！共涵蓋 {len(df_result)} 檔股票，已存至 {save_path}")
    print("PROGRESS: 100")


if __name__ == "__main__":
    main()