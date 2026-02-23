# ==================================================
# fhtrust_parser.py
# 解析復華台灣未來50主動式ETF每日持股 Excel
# 輸出標準化 CSV
# ==================================================

import pandas as pd
from pathlib import Path


class FHTrustParser:
    """復華投信 ETF 資料解析器"""

    def __init__(self, raw_dir, clean_dir, etf_code="00991A"):
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
        解析單個 Excel，回傳標準欄位 DataFrame
        標準欄位: stock_code, stock_name, shares, weight, date
        修正：加入資料去重與轉型防呆
        """
        # 1️⃣ 先讀 Excel（header=None，找真正標題行）
        df_raw = pd.read_excel(file_path, header=None)

        # 2️⃣ 找到包含 '證券代號' 的行，作為 header
        header_row_idx = df_raw.index[
            df_raw.apply(
                lambda row: row.astype(str).str.contains('證券代號').any(),
                axis=1
            )
        ][0]

        # 3️⃣ 重新讀 Excel，使用找到的 header row
        df = pd.read_excel(file_path, header=header_row_idx)

        # 4️⃣ 選取欄位並改名
        try:
            df = df[['證券代號', '證券名稱', '股數', '權重(%)']]
        except KeyError as e:
            print(f"❌ Excel {file_path.name} 欄位不正確，請確認")
            print("目前欄位:", df.columns.tolist())
            raise e

        df.columns = ['stock_code', 'stock_name', 'shares', 'weight']

        # 5️⃣ 數字欄位清理 (修正：增加 float 轉換以應對可能的小數點格式)
        df['shares'] = df['shares'].astype(str).str.replace(',', '', regex=False).astype(float).astype(int)
        df['weight'] = df['weight'].astype(str).str.replace('%', '', regex=False).astype(float)

        # 6️⃣ 加日期欄（從檔名抽取）
        date_str = file_path.stem  # 例如 '2026_02_11'
        df['date'] = date_str.replace('_', '-')

        # ✨ 7️⃣ 防呆：確保同一份檔案內沒有重複的代碼，並移除可能的空白行
        df = df.dropna(subset=['stock_code'])
        df = df.drop_duplicates(subset=['stock_code', 'date'], keep='last')

        return df

    def parse_all_files(self):
        """解析所有檔案並輸出 CSV"""
        # 1️⃣ 防呆：RAW_DIR 必須存在
        if not self.raw_dir.exists():
            raise FileNotFoundError(f"❌ RAW_DIR 不存在：{self.raw_dir}")

        # 2️⃣ 找到所有 Excel
        files = list(self.raw_dir.glob("*.xls*"))
        if not files:
            print(f"⚠️ RAW_DIR 底下找不到 Excel 檔案：{self.raw_dir}")
            return

        print(f"📂 找到 {len(files)} 個 Excel 檔案，開始解析 {self.etf_code} ...")

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

        # 5️⃣ 排序（日期 + 權重）
        result = result.sort_values(
            by=['date', 'weight'],
            ascending=[True, False]
        )

        # 6️⃣ 建立 clean 資料夾
        self.out_file.parent.mkdir(parents=True, exist_ok=True)

        # 7️⃣ 輸出 CSV
        result.to_csv(self.out_file, index=False, encoding='utf-8-sig')

        print(f"✅ 解析完成，輸出到：{self.out_file}")


# 保留原本的執行方式
def run():
    """向下相容：舊的執行方式"""
    BASE_DIR = Path(__file__).resolve().parents[3]  # 調整為專案根目錄
    raw_dir = BASE_DIR / "戰情室" / "data" / "raw" / "fhtrust" / "00991A"
    clean_dir = BASE_DIR / "戰情室" / "data" / "clean" / "fhtrust"

    parser = FHTrustParser(raw_dir, clean_dir)
    parser.parse_all_files()


if __name__ == "__main__":
    run()