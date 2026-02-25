import requests
from bs4 import BeautifulSoup
import time
import random
import os
import urllib3
import re
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta

# 1. 忽略 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 2. 載入 .env
project_root = Path(__file__).resolve().parent.parent
env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(env_path)


class MoneyDJParser:
    BASE_URL = "https://concords.moneydj.com/z/zc"
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://concords.moneydj.com/'
    }

    # === 中英文欄位對照表 (Data Dictionary) ===
    FIELD_MAP = {
        "quarter": "季別",
        "year": "年度",
        "month": "月份",
        "gross_margin": "毛利率(%)",
        "op_margin": "營益率(%)",
        "net_pre_tax": "稅前淨利(百萬)",
        "net_after_tax": "稅後淨利(百萬)",
        "eps": "EPS(元)",
        "eps_yearly": "年度EPS(元)",
        "inventory": "存貨(百萬)",
        "contract_liab": "合約負債(流動)(百萬)",
        "rev_yoy": "月營收年增率(%)",
        "rev_cum_yoy": "月營收累計年增率(%)",
        "op_cash_flow": "來自營運之現金流量(百萬)"
    }

    def __init__(self, sid):
        self.sid = str(sid).strip()
        self.proxies = None
        http_proxy = os.getenv("HTTP_PROXY")
        https_proxy = os.getenv("HTTPS_PROXY")
        if http_proxy or https_proxy:
            self.proxies = {"http": http_proxy, "https": https_proxy}

    def _get_soup(self, url):
        """ 通用請求函式，回傳 Soup """
        try:
            time.sleep(random.uniform(0.6, 1.5))
            res = requests.get(url, headers=self.HEADERS, proxies=self.proxies, timeout=15, verify=False)
            res.encoding = 'big5'

            if res.status_code != 200:
                print(f"⚠️ Status {res.status_code} for {url}")
                return None
            return BeautifulSoup(res.text, 'html.parser')
        except Exception as e:
            print(f"❌ Exception for {url}: {e}")
            return None

    def _clean_val(self, val_str):
        """ 清洗數值：移除逗號、百分比、空白，轉為 float """
        if not val_str: return 0.0
        val_str = str(val_str).strip().replace(',', '').replace('%', '').replace('--', '')
        if val_str in ['-', '', 'N/A', 'nan']:
            return 0.0
        try:
            return float(val_str)
        except:
            return 0.0

    def _roc_to_ad(self, date_str):
        """ 民國日期轉西元字串 YYYY-MM-DD """
        try:
            y, m, d = date_str.split("/")
            return f"{int(y) + 1911}-{int(m):02d}-{int(d):02d}"
        except:
            return date_str

    # ==========================================
    # 1. 獲利能力 (季報) - ZCE
    # ==========================================
    def get_profitability_quarterly(self, limit=4):
        url = f"{self.BASE_URL}/zce/zce_{self.sid}.djhtm"
        soup = self._get_soup(url)
        if not soup: return []

        table = soup.find("table", id="oMainTable")
        if not table: return []

        results = []
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) >= 11:
                quarter = tds[0].get_text(strip=True)
                if '.' in quarter and 'Q' in quarter:
                    item = {
                        "quarter": quarter,
                        "gross_margin": self._clean_val(tds[4].get_text()),
                        "op_margin": self._clean_val(tds[6].get_text()),
                        "net_pre_tax": self._clean_val(tds[8].get_text()),
                        "net_after_tax": self._clean_val(tds[9].get_text()),
                        "eps": self._clean_val(tds[10].get_text())
                    }
                    results.append(item)
                    if len(results) >= limit: break
        return results

    # ==========================================
    # 2. 經營績效 (年報) - ZCDJ
    # ==========================================
    def get_yearly_performance(self, limit=3):
        url = f"{self.BASE_URL}/zcdj/zcdj_{self.sid}.djhtm"
        soup = self._get_soup(url)
        if not soup: return []

        table = soup.find("table", id="oMainTable")
        if not table: return []

        results = []
        rows = table.find_all("tr")
        eps_index = -1

        header_row = soup.find("tr", id="oScrollMenu") or (rows[0] if rows else None)
        if header_row:
            for i, td in enumerate(header_row.find_all("td")):
                txt = td.get_text(strip=True)
                if "稅後" in txt and "盈餘" in txt:
                    eps_index = i
                    break

        if eps_index == -1: return []

        for tr in rows[1:]:
            tds = tr.find_all("td")
            if len(tds) > eps_index:
                year_str = tds[0].get_text(strip=True)
                if year_str.isdigit() and len(year_str) <= 3:
                    results.append({
                        "year": year_str,
                        "eps_yearly": self._clean_val(tds[eps_index].get_text())
                    })
                    if len(results) >= limit: break
        return results

    # ==========================================
    # 3. 資產負債表 - ZCPA (矩陣式)
    # ==========================================
    def get_balance_sheet(self, limit=5):
        url = f"{self.BASE_URL}/zcp/zcpa/zcpa_{self.sid}.djhtm"
        soup = self._get_soup(url)
        if not soup: return []

        rows = soup.find_all("div", class_="table-row")
        if not rows: return []

        quarters = [cell.get_text(strip=True) for cell in rows[0].find_all("span", class_="table-cell")[1:]]
        inventories, contract_liabs = [0] * len(quarters), [0] * len(quarters)

        for row in rows:
            cells = row.find_all("span", class_="table-cell")
            if not cells: continue
            title = cells[0].get_text(strip=True)

            if title == "存貨":
                inventories = [self._clean_val(c.get_text()) for c in cells[1:]]
            if "合約負債" in title and "流動" in title and "非" not in title:
                contract_liabs = [self._clean_val(c.get_text()) for c in cells[1:]]
            elif title == "合約負債" and all(v == 0 for v in contract_liabs):
                contract_liabs = [self._clean_val(c.get_text()) for c in cells[1:]]

        results = []
        for i in range(min(len(quarters), limit)):
            results.append({
                "quarter": quarters[i],
                "inventory": inventories[i] if i < len(inventories) else 0,
                "contract_liab": contract_liabs[i] if i < len(contract_liabs) else 0
            })
        return results

    # ==========================================
    # 4. 月營收 - ZCH
    # ==========================================
    def get_monthly_revenue(self, limit=6):
        url = f"{self.BASE_URL}/zch/zch_{self.sid}.djhtm"
        soup = self._get_soup(url)
        if not soup: return []

        results = []
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) >= 7:
                date_str = tds[0].get_text(strip=True)
                if '/' in date_str and len(date_str) >= 5 and date_str[0].isdigit():
                    try:
                        results.append({
                            "month": date_str,
                            "rev_yoy": self._clean_val(tds[4].get_text()),
                            "rev_cum_yoy": self._clean_val(tds[6].get_text())
                        })
                    except:
                        continue
                    if len(results) >= limit: break
        return results

    # ==========================================
    # 5. 現金流量表 - ZC3 (矩陣式)
    # ==========================================
    def get_cash_flow(self, limit=5):
        url = f"{self.BASE_URL}/zc3/zc3_{self.sid}.djhtm"
        soup = self._get_soup(url)
        if not soup: return []

        rows = soup.find_all("div", class_="table-row")
        if not rows: return []

        quarters = [cell.get_text(strip=True) for cell in rows[0].find_all("span", class_="table-cell")[1:]]
        op_cash = [0] * len(quarters)

        for row in rows:
            cells = row.find_all("span", class_="table-cell")
            if not cells: continue
            if "來自營運" in cells[0].get_text(strip=True) and "現金流量" in cells[0].get_text(strip=True):
                op_cash = [self._clean_val(c.get_text()) for c in cells[1:]]
                break

        results = []
        for i in range(min(len(quarters), limit)):
            results.append({
                "quarter": quarters[i],
                "op_cash_flow": op_cash[i] if i < len(op_cash) else 0
            })
        return results

    # ==========================================
    # 6. 三大法人持股 (過去 N 個月) - ZCL
    # ==========================================
    def get_institutional_investors(self, months=6):
        today = datetime.today()
        start_date = today - relativedelta(months=months)
        c_str = start_date.strftime("%Y-%m-%d")
        d_str = today.strftime("%Y-%m-%d")

        url = f"{self.BASE_URL}/zcl/zcl.djhtm?a={self.sid}&c={c_str}&d={d_str}"
        soup = self._get_soup(url)
        if not soup: return []

        results = []
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) == 11:
                date_text = tds[0].get_text(strip=True).replace("\xa0", "")
                if date_text[:3].isdigit() and "/" in date_text:
                    results.append({
                        "date": self._roc_to_ad(date_text),
                        "foreign_buy_sell": self._clean_val(tds[1].get_text()),
                        "invest_trust_buy_sell": self._clean_val(tds[2].get_text()),
                        "dealer_buy_sell": self._clean_val(tds[3].get_text()),
                        "total_buy_sell": self._clean_val(tds[4].get_text()),
                        "foreign_hold": self._clean_val(tds[5].get_text()),
                        "invest_trust_hold": self._clean_val(tds[6].get_text()),
                        "dealer_hold": self._clean_val(tds[7].get_text()),
                        "total_hold": self._clean_val(tds[8].get_text()),
                        "foreign_hold_pct": self._clean_val(tds[9].get_text()),
                        "total_legal_pct": self._clean_val(tds[10].get_text())
                    })
        return results

    # ==========================================
    # 7. 融資融券餘額 (過去 N 個月) - ZCN
    # ==========================================
    def get_margin_trading(self, months=6):
        today = datetime.today()
        start_date = today - relativedelta(months=months)
        c_str = start_date.strftime("%Y-%m-%d")
        d_str = today.strftime("%Y-%m-%d")

        url = f"{self.BASE_URL}/zcn/zcn.djhtm?a={self.sid}&c={c_str}&d={d_str}"
        soup = self._get_soup(url)
        if not soup: return []

        results = []
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) >= 15:
                date_text = tds[0].get_text(strip=True).replace("\xa0", "")
                if date_text[:3].isdigit() and "/" in date_text and len(date_text.split("/")) == 3:
                    results.append({
                        "date": self._roc_to_ad(date_text),
                        "fin_buy": self._clean_val(tds[1].get_text()),
                        "fin_sell": self._clean_val(tds[2].get_text()),
                        "fin_repay": self._clean_val(tds[3].get_text()),
                        "fin_balance": self._clean_val(tds[4].get_text()),
                        "fin_change": self._clean_val(tds[5].get_text()),
                        "fin_limit": self._clean_val(tds[6].get_text()),
                        "fin_usage": self._clean_val(tds[7].get_text()),
                        "short_sell": self._clean_val(tds[8].get_text()),
                        "short_buy": self._clean_val(tds[9].get_text()),
                        "short_repay": self._clean_val(tds[10].get_text()),
                        "short_balance": self._clean_val(tds[11].get_text()),
                        "short_change": self._clean_val(tds[12].get_text()),
                        "ratio": self._clean_val(tds[13].get_text()),
                        "offset": self._clean_val(tds[14].get_text())
                    })
        return results

    # ==========================================
    # 專為每日更新設計 (只抓法人與資券)
    # ==========================================
    def get_daily_chips(self, months=6):
        return {
            "sid": self.sid,
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
            "institutional_investors": self.get_institutional_investors(months=months),
            "margin_trading": self.get_margin_trading(months=months)
        }

    # ==========================================
    # 整合執行
    # ==========================================
    def get_full_analysis(self):
        return {
            "sid": self.sid,
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
            "profitability": self.get_profitability_quarterly(),
            "yearly_perf": self.get_yearly_performance(),
            "balance_sheet": self.get_balance_sheet(),
            "revenue": self.get_monthly_revenue(),
            "cash_flow": self.get_cash_flow(),
            "institutional_investors": self.get_institutional_investors(months=6),
            "margin_trading": self.get_margin_trading(months=6)
        }


if __name__ == "__main__":
    # 本機測試
    test_sid = "3665"
    print(f"🚀 [Test] 測試 MoneyDJ 爬蟲 (BeautifulSoup 版): {test_sid} ...")

    parser = MoneyDJParser(test_sid)

    print("\n--- 1. 獲利能力 (季報) ---")
    print(parser.get_profitability_quarterly())

    print("\n--- 2. 經營績效 (年報 EPS) ---")
    print(parser.get_yearly_performance())

    print("\n--- 3. 資產負債 (存貨/合約負債) ---")
    print(parser.get_balance_sheet())

    print("\n--- 4. 月營收 (YoY) ---")
    print(parser.get_monthly_revenue())

    print("\n--- 5. 現金流量 (營運) ---")
    print(parser.get_cash_flow())

    print("\n--- 6. 三大法人持股 (前 3 筆測試) ---")
    inst_data = parser.get_institutional_investors(months=6)
    print(inst_data[:3] if inst_data else "無資料")

    print("\n--- 7. 融資融券 (前 3 筆測試) ---")
    margin_data = parser.get_margin_trading(months=6)
    print(margin_data[:3] if margin_data else "無資料")

    print(f"\n✅ 測試完成 (三大法人總筆數: {len(inst_data)}, 融資融券總筆數: {len(margin_data)})")