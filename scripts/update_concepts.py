# 檔案路徑: scripts/update_concepts.py
import requests
import pandas as pd
import time
import re
import os
import sys
from collections import defaultdict
from pathlib import Path
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

# 載入專案根目錄的 .env 以取得 Proxy 設定
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
load_dotenv(project_root / '.env')


def main():
    print(f"PROGRESS: 0")
    print("🚀 啟動 MoneyDJ 概念股全面更新作業...")

    # 建立 Proxy 字典
    proxies = {}
    http_proxy = os.getenv('HTTP_PROXY') or os.getenv('http_proxy')
    https_proxy = os.getenv('HTTPS_PROXY') or os.getenv('https_proxy')
    if http_proxy: proxies['http'] = http_proxy
    if https_proxy: proxies['https'] = https_proxy
    if proxies:
        print(f"🌐 偵測到 Proxy 設定，將透過 Proxy 連線: {proxies}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # 1. 自動抓取「所有概念股分類」清單
    print("⏳ 正在取得最新的概念股分類總表...")
    base_url = "https://www.moneydj.com/z/zg/zge_EH001185_1.djhtm"
    try:
        res = requests.get(base_url, headers=headers, proxies=proxies, timeout=15)
        res.encoding = 'big5'
        # 抓取 <select name="M1"> 裡面的所有 option
        options = re.findall(r'<option value="(EH\d+)"[^>]*>([^<]+)</option>', res.text)
        if not options:
            print("❌ 無法取得概念股選單，請確認網路連線。")
            return
        print(f"✅ 成功取得 {len(options)} 種概念股分類！")
    except Exception as e:
        print(f"❌ 取得分類總表失敗: {e}")
        return

    # 2. 開始逐一爬取
    stock_tags = defaultdict(list)
    total = len(options)

    for i, (concept_id, concept_name) in enumerate(options):
        # 回報進度給 Settings UI (保留前 5% 與後 5% 緩衝，這裡佔 90%)
        pct = int(5 + (i / total) * 90)
        print(f"PROGRESS: {pct}")

        url = f"https://www.moneydj.com/z/zg/zge_{concept_id}_1.djhtm"
        print(f"  [{i + 1}/{total}] 擷取: {concept_name}...")

        try:
            res = requests.get(url, headers=headers, proxies=proxies, timeout=10)
            res.encoding = 'big5'

            matches = re.findall(r"GenLink2stk\('[a-zA-Z]*(\d{4,5})','([^']+)'\)", res.text)

            found_count = 0
            for sid, sname in set(matches):
                if concept_name not in stock_tags[sid]:
                    stock_tags[sid].append(concept_name)
                    found_count += 1

            time.sleep(1.0)  # 禮貌性延遲1秒
        except Exception as e:
            print(f"    ❌ 失敗: {e}")

    # 3. 儲存結果
    print("PROGRESS: 95")
    print("⏳ 正在整理資料並儲存...")
    result_data = [{'sid': sid, 'sub_concepts': ','.join(tags)} for sid, tags in stock_tags.items()]
    df_concepts = pd.DataFrame(result_data)

    save_dir = project_root / 'data'
    save_dir.mkdir(parents=True, exist_ok=True)
    save_path = save_dir / 'concept_tags.csv'

    df_concepts.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"✅ 更新完成！共涵蓋 {len(df_concepts)} 檔股票，已存至 {save_path}")
    print("PROGRESS: 100")


if __name__ == "__main__":
    main()