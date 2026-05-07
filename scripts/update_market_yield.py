# 檔案路徑: scripts/update_market_yield.py
import sys
import os
import json
import requests
import warnings
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8')
warnings.filterwarnings('ignore')

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
load_dotenv(project_root / '.env')


def parse_val(v):
    try:
        return float(str(v).replace(',', '').replace('%', '').strip())
    except:
        return 0.0


def fetch_market_valuation():
    print("[System] 啟動全市場本益比、殖利率同步...")
    vd = {}

    proxies = {}
    if os.getenv('HTTP_PROXY'): proxies['http'] = os.getenv('HTTP_PROXY')
    if os.getenv('HTTPS_PROXY'): proxies['https'] = os.getenv('HTTPS_PROXY')
    if proxies: print("🌐 偵測到 Proxy 設定，將透過 Proxy 連線")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Connection': 'keep-alive'
    }

    days, offset = [], 0
    while len(days) < 5 and offset < 15:
        dt = datetime.now() - timedelta(days=offset)
        if dt.weekday() < 5: days.append(dt)
        offset += 1

    print("PROGRESS: 10")  # 抓取前進度回報

    for dt in days:
        d_str = dt.strftime('%Y%m%d')
        d_roc = f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"
        print(f"▶ 嘗試抓取日期: {d_str}...", end=" ", flush=True)
        found_data = False

        # 上市 (TWSE)
        try:
            url = f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?date={d_str}&selectType=ALL&response=json"
            res_twse = requests.get(url, headers=headers, proxies=proxies, timeout=10, verify=False)
            if res_twse.status_code == 200:
                res = res_twse.json()
                if res.get('stat') == 'OK' and 'data' in res:
                    f = res['fields']
                    iy, ipe, ipb = f.index("殖利率(%)"), f.index("本益比"), f.index("股價淨值比")
                    for r in res['data']:
                        vd[r[0].strip()] = {'yield': parse_val(r[iy]), 'pe': parse_val(r[ipe]),
                                            'pbr': parse_val(r[ipb])}
                    found_data = True
            else:
                print(f"[上市 HTTP {res_twse.status_code}]", end=" ")
        except Exception:
            print(f"[上市錯誤]", end=" ")

        print("PROGRESS: 50")  # 完成上市回報

        # 上櫃 (TPEx)
        try:
            url = f"https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?l=zh-tw&o=json&d={d_roc}"
            res_tpex = requests.get(url, headers=headers, proxies=proxies, timeout=10, verify=False)
            if res_tpex.status_code == 200:
                res = res_tpex.json()
                raw = res['tables'][0]['data'] if 'tables' in res else []
                if raw:
                    for r in raw:
                        vd[r[0].strip()] = {'pe': parse_val(r[2]), 'yield': parse_val(r[5]), 'pbr': parse_val(r[6])}
                    found_data = True
            else:
                print(f"[上櫃 HTTP {res_tpex.status_code}]", end=" ")
        except Exception:
            print(f"[上櫃錯誤]", end=" ")

        if found_data:
            print(f"✅ 成功! (共取得 {len(vd)} 筆)")
            break
        else:
            print("❌ 無資料，嘗試前一工作日")

    return vd


def main():
    valuation_dict = fetch_market_valuation()

    # 將抓取結果存成本地檔案
    data_dir = project_root / 'data'
    data_dir.mkdir(parents=True, exist_ok=True)
    save_path = data_dir / 'market_yield.json'

    if valuation_dict:
        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(valuation_dict, f, ensure_ascii=False, indent=2)
        print(f"💾 估值資料已儲存至: {save_path.name}")
    else:
        print("⚠️ 警告: 未能抓取到任何估值資料。")

    print("PROGRESS: 100")


if __name__ == "__main__":
    main()