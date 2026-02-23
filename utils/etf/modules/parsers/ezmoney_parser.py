# ==================================================
# ezmoney_parser.py
# 解析 統一投信 ezmoney ETF 每日持股 Excel
# ==================================================

import pandas as pd
from pathlib import Path


class EZMoneyParser:
    """EZMoney ETF 資料解析器"""

    def __init__(self, raw_dir, clean_dir, etf_code="00981A"):
        """
        Args:
            raw_dir: 原始資料目錄
            clean_dir: 清理後資料目錄
            etf_code: ETF 代碼
        """
        self.raw_dir = Path(raw_dir)
        self.clean_dir = Path(clean_dir)
        self.etf_code = etf_code
        self.out_file = Path(clean_dir) / f"{etf_code}.csv"

    def parse_excel(self, file_path: Path) -> pd.DataFrame:
        """
        解析單個 Excel（只讀「持股明細」分頁）
        回傳標準欄位 DataFrame
        修正 Bug: 比對 Excel 內部日期與檔名日期，若不符則視為重複資料剔除
        """
        # 1️⃣ 讀取「持股明細」分頁
        try:
            df = pd.read_excel(file_path, sheet_name="持股明細")
        except ValueError:
            raise ValueError(f"❌ {file_path.name} 找不到『持股明細』分頁")

        # 2️⃣ 欄位 alias 對應
        column_map = {
            "股票代碼": "stock_code",
            "股票代號": "stock_code",
            "股票名稱": "stock_name",
            "持股數": "shares",
            "值佔比(%)": "weight",
            "淨值佔比(%)": "weight",
            "更新日期": "date",
        }

        # 3️⃣ 篩出我們要的欄位
        available_cols = {}
        for raw_col, std_col in column_map.items():
            if raw_col in df.columns:
                available_cols[raw_col] = std_col

        required_std_cols = {"stock_code", "stock_name", "shares", "weight", "date"}
        if set(available_cols.values()) != required_std_cols:
            raise KeyError(
                f"❌ {file_path.name} 欄位不完整\n"
                f"目前欄位：{df.columns.tolist()}"
            )

        df = df[list(available_cols.keys())].rename(columns=available_cols)

        # 4️⃣ 數字欄位清理
        df["shares"] = (
            df["shares"]
            .astype(str)
            .str.replace(",", "", regex=False)
            .astype(float)  # 先轉 float 以處理 1445000.0 這種字串
            .astype(int)
        )

        df["weight"] = (
            df["weight"]
            .astype(str)
            .str.replace("%", "", regex=False)
            .astype(float)
        )

        # 5️⃣ 日期格式統一與校驗 Bug 修正
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        # ✨ Bug 修正核心：檢查內部日期與檔案名稱日期是否一致
        # 檔名可能是 2026_02_12.xlsx，轉換為 2026-02-12
        file_date_str = file_path.stem.replace('_', '-')
        internal_date_str = df["date"].iloc[0]

        if internal_date_str != file_date_str:
            # 如果日期不符（例如 2/12 檔案內寫的是 2/11 資料），回傳空表不予解析
            print(f"⚠️  排除重複資料: {file_path.name} (內部日期 {internal_date_str} 與檔名不符)")
            return pd.DataFrame(columns=['stock_code', 'stock_name', 'shares', 'weight', 'date'])

        # 6️⃣ 確保資料不重複 (雙重保險)
        df = df.drop_duplicates(subset=['stock_code', 'date'], keep='last')

        return df
    def parse_all_files(self):
        """解析所有檔案並輸出 CSV"""
        # 1️⃣ 防呆：RAW_DIR 必須存在
        if not self.raw_dir.exists():
            raise FileNotFoundError(f"❌ RAW_DIR 不存在：{self.raw_dir}")

        # 2️⃣ 找到所有 Excel
        files = sorted(self.raw_dir.rglob("*.xls*"))
        if not files:
            print(f"⚠️ RAW_DIR 底下找不到 Excel 檔案：{self.raw_dir}")
            return

        print(f"📂 找到 {len(files)} 個 Excel，開始解析 {self.etf_code} ...")

        # 3️⃣ 解析所有 Excel
        all_rows = []
        for f in files:
            try:
                all_rows.append(self.parse_excel(f))
            except Exception as e:
                print(f"⚠️ 解析失敗：{f.name}")
                print(e)

        if not all_rows:
            raise RuntimeError("❌ 沒有任何檔案成功解析")

        # 4️⃣ 合併 DataFrame
        result = pd.concat(all_rows, ignore_index=True)
        # ✨ 新增防呆：確保同日期、同代碼的股票只會出現一筆（保留最後一筆）
        result = result.drop_duplicates(subset=['date', 'stock_code'], keep='last')

        # 5️⃣ 排序（日期 + 權重）
        result = result.sort_values(
            by=["date", "weight"], ascending=[True, False]
        )

        # 6️⃣ 建立 clean 資料夾
        self.out_file.parent.mkdir(parents=True, exist_ok=True)

        # 7️⃣ 輸出 CSV
        result.to_csv(self.out_file, index=False, encoding="utf-8-sig")

        print(f"✅ 解析完成，輸出到：{self.out_file}")


# 保留原本的執行方式
def run():
    """向下相容：舊的執行方式"""
    BASE_DIR = Path(__file__).resolve().parents[3]  # 調整為專案根目錄
    raw_dir = BASE_DIR / "戰情室" / "data" / "raw" / "ezmoney" / "00981A"
    clean_dir = BASE_DIR / "戰情室" / "data" / "clean" / "ezmoney"

    parser = EZMoneyParser(raw_dir, clean_dir)
    parser.parse_all_files()


if __name__ == "__main__":
    run()