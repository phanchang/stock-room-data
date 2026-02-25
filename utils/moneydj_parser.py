import requests
from bs4 import BeautifulSoup
import time
import random
import os
import urllib3
import re
from pathlib import Path
from dotenv import load_dotenv

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
        "op_cash_flow": "來自營運之現金流量(百萬)",
        "foreign_hold_pct": "外資持股比例(%)",
        "invest_trust_hold_pct": "投信持股比例(%)",
        "dealer_hold_pct": "自營商持股比例(%)",
        "margin_balance_pct": "融資餘額比例(%)",
        "short_balance_pct": "融券餘額比例(%)",
        "total_legal_pct": "三大法人合計比例(%)"
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
            res.encoding = 'big5'  # MoneyDJ 固定編碼

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
        val_str = str(val_str).strip().replace(',', '').replace('%', '')
        if val_str in ['-', '', 'N/A', 'nan']:
            return 0.0
        try:
            return float(val_str)
        except:
            return 0.0

    # ==========================================
    # 1. 獲利能力 (季報) - ZCE (已修正)
    # 邏輯：每一列是一個季度，第0欄是季別
    # ==========================================
    def get_profitability_quarterly(self, limit=4):
        url = f"{self.BASE_URL}/zce/zce_{self.sid}.djhtm"
        soup = self._get_soup(url)
        if not soup: return []

        table = soup.find("table", id="oMainTable")
        if not table: return []

        results = []
        rows = table.find_all("tr")

        for tr in rows:
            tds = tr.find_all("td")
            # MoneyDJ ZCE 標準表格通常有 11 欄
            # 0:季別, 4:毛利率, 6:營益率, 8:稅前, 9:稅後, 10:EPS
            if len(tds) >= 11:
                quarter = tds[0].get_text(strip=True)

                # 檢查是否為季別格式 (例如 114.3Q)
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
    # 抓取：最新3個年度的稅後每股盈餘(元)
    # ==========================================
    def get_yearly_performance(self, limit=3):
        url = f"{self.BASE_URL}/zcdj/zcdj_{self.sid}.djhtm"
        soup = self._get_soup(url)
        if not soup: return []

        table = soup.find("table", id="oMainTable")
        if not table: return []

        results = []
        rows = table.find_all("tr")

        # 先找出 EPS 在第幾欄 (因為年報欄位較多，可能有變動)
        eps_index = -1

        # 標題列通常在 id="oScrollMenu"
        header_row = soup.find("tr", id="oScrollMenu")
        if not header_row and len(rows) > 0:
            header_row = rows[0]

        if header_row:
            for i, td in enumerate(header_row.find_all("td")):
                txt = td.get_text(strip=True)
                if "稅後" in txt and "盈餘" in txt:
                    eps_index = i
                    break

        if eps_index == -1: return []

        for tr in rows[1:]:  # 跳過標題
            tds = tr.find_all("td")
            if len(tds) > eps_index:
                year_str = tds[0].get_text(strip=True)
                if year_str.isdigit() and len(year_str) <= 3:  # 確保第一欄是年度 (ex: 113)
                    results.append({
                        "year": year_str,
                        "eps_yearly": self._clean_val(tds[eps_index].get_text())
                    })
                    if len(results) >= limit: break
        return results

    # ==========================================
    # 3. 資產負債表 - ZCPA (矩陣式)
    # 抓取：存貨、合約負債－流動
    # ==========================================
    def get_balance_sheet(self, limit=5):
        url = f"{self.BASE_URL}/zcp/zcpa/zcpa_{self.sid}.djhtm"
        soup = self._get_soup(url)
        if not soup: return []

        table = soup.find("table", id="oMainTable")
        if not table: return []

        rows = soup.find_all("div", class_="table-row")  # MoneyDJ 特殊 div 表格
        if not rows: return []

        quarters = []
        # 第一列是期別
        q_row_cells = rows[0].find_all("span", class_="table-cell")
        for cell in q_row_cells[1:]:
            quarters.append(cell.get_text(strip=True))

        inventories = []
        contract_liabs = []

        # 預設值
        if not inventories: inventories = [0] * len(quarters)
        if not contract_liabs: contract_liabs = [0] * len(quarters)

        for row in rows:
            cells = row.find_all("span", class_="table-cell")
            if not cells: continue

            title = cells[0].get_text(strip=True)

            if title == "存貨":  # 精確比對
                inventories = [self._clean_val(c.get_text()) for c in cells[1:]]

            if "合約負債" in title and "流動" in title and "非" not in title:
                contract_liabs = [self._clean_val(c.get_text()) for c in cells[1:]]
            elif title == "合約負債" and all(v == 0 for v in contract_liabs):  # 備用
                contract_liabs = [self._clean_val(c.get_text()) for c in cells[1:]]

        results = []
        count = min(len(quarters), limit)
        for i in range(count):
            results.append({
                "quarter": quarters[i],
                "inventory": inventories[i] if i < len(inventories) else 0,
                "contract_liab": contract_liabs[i] if i < len(contract_liabs) else 0
            })
        return results

    # ==========================================
    # 4. 月營收 - ZCH
    # 抓取：去年同期年增率、累計年增率
    # ==========================================
    def get_monthly_revenue(self, limit=6):
        url = f"{self.BASE_URL}/zch/zch_{self.sid}.djhtm"
        soup = self._get_soup(url)
        if not soup: return []

        rows = soup.find_all("tr")
        results = []

        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) >= 7:
                date_str = tds[0].get_text(strip=True)
                # 驗證日期格式 114/01
                if '/' in date_str and len(date_str) >= 5 and date_str[0].isdigit():
                    try:
                        # 排除標題列 (標題不會是數字開頭)
                        yoy = self._clean_val(tds[4].get_text())
                        cum_yoy = self._clean_val(tds[6].get_text())

                        results.append({
                            "month": date_str,
                            "rev_yoy": yoy,
                            "rev_cum_yoy": cum_yoy
                        })
                    except:
                        continue
                    if len(results) >= limit: break
        return results

    # ==========================================
    # 5. 現金流量表 - ZC3 (矩陣式)
    # 抓取：來自營運之現金流量
    # ==========================================
    def get_cash_flow(self, limit=5):
        url = f"{self.BASE_URL}/zc3/zc3_{self.sid}.djhtm"
        soup = self._get_soup(url)
        if not soup: return []

        rows = soup.find_all("div", class_="table-row")
        if not rows: return []

        quarters = []
        q_row_cells = rows[0].find_all("span", class_="table-cell")
        for cell in q_row_cells[1:]:
            quarters.append(cell.get_text(strip=True))

        op_cash = [0] * len(quarters)

        for row in rows:
            cells = row.find_all("span", class_="table-cell")
            if not cells: continue
            title = cells[0].get_text(strip=True)

            if "來自營運" in title and "現金流量" in title:
                op_cash = [self._clean_val(c.get_text()) for c in cells[1:]]
                break

        results = []
        count = min(len(quarters), limit)
        for i in range(count):
            results.append({
                "quarter": quarters[i],
                "op_cash_flow": op_cash[i] if i < len(op_cash) else 0
            })
        return results

    # ==========================================
    # 6. 籌碼分佈 (解析 zcj 頁面) - 2026/02/25 優化版
    # ==========================================
    def get_chips_distribution(self):
        url = f"{self.BASE_URL}/zcj/zcj_{self.sid}.djhtm"
        soup = self._get_soup(url)
        if not soup: return {}

        data = {}

        # --- 1. 抓取資料日期 (標記資料定錨點) ---
        # 結構: <div class="t11">日期：02/24</div>
        date_div = soup.find("div", class_="t11")
        if date_div:
            date_text = date_div.get_text(strip=True)
            # 取得 "02/24"
            data["data_date"] = date_text.replace("日期：", "")

        # --- 2. 解析表格資料 ---
        # 標籤特徵: 名稱在 td[0], 比例在 td[3]
        target_map = {
            "外資持股": "foreign_hold_pct",
            "投信持股": "invest_trust_hold_pct",
            "自營商持股": "dealer_hold_pct",
            "融資餘額": "margin_balance_pct",
            "融券餘額": "short_balance_pct"
        }

        # 遍歷所有 tr，不論大小寫
        for tr in soup.find_all(re.compile('^tr$', re.I)):
            tds = tr.find_all(re.compile('^td$', re.I))
            if len(tds) >= 4:
                # 取得名稱並清洗空白與特殊字元
                name = tds[0].get_text(strip=True).replace('\xa0', '')

                if name in target_map:
                    # 比例固定在第四個 td (Index 3)
                    val_str = tds[3].get_text(strip=True)
                    data[target_map[name]] = self._clean_val(val_str)

        # --- 3. 手動加總三大法人合計 (為了大表呈現) ---
        legal_list = ["foreign_hold_pct", "invest_trust_hold_pct", "dealer_hold_pct"]
        if any(key in data for key in legal_list):
            total = sum(data.get(key, 0.0) for key in legal_list)
            data["total_legal_pct"] = round(total, 2)

        return data


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
            "chips": self.get_chips_distribution()
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

    print("\n--- 6. 籌碼分佈 (佔比) ---")
    print(parser.get_chips_distribution())

    print("\n✅ 測試完成")