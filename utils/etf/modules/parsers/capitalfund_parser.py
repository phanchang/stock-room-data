# utils/etf/modules/parsers/capitalfund_parser.py
import pandas as pd
import json
from pathlib import Path


class CapitalFundParser:
    def __init__(self, raw_dir, clean_dir, etf_code="00982A"):
        self.raw_dir = Path(raw_dir)
        self.clean_dir = Path(clean_dir)
        self.etf_code = etf_code
        self.out_file = self.clean_dir / f"{etf_code}.csv"

    def parse_json(self, file_path: Path) -> pd.DataFrame:
        """解析單個 JSON，回傳標準欄位 DataFrame"""
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        stocks = data.get("data", {}).get("stocks", [])
        if not stocks:
            return pd.DataFrame()

        df = pd.DataFrame(stocks)

        # 1. 篩選與重新命名欄位
        col_map = {
            "stocNo": "stock_code",
            "stocName": "stock_name",
            "share": "shares",
            "weight": "weight"
        }
        df = df[list(col_map.keys()) + ["date1"]].rename(columns=col_map)

        # 2. 處理日期 ("2026/2/11 上午 12:00:00" -> "2026-02-11")
        # 拆分字串，只取前面日期的部分，然後轉為 YYYY-MM-DD
        df["date"] = df["date1"].apply(lambda x: pd.to_datetime(str(x).split(' ')[0]).strftime("%Y-%m-%d"))

        # ✨ 新增：日期校驗防呆
        # 檢查 JSON 內容日期 (2026-02-11 -> 20260211) 是否與檔名 (20260211) 一致
        internal_date = df["date"].iloc[0].replace('-', '')
        file_date = file_path.stem
        if internal_date != file_date:
            print(f"⚠️  排除重複資料: {file_path.name} (內容日期 {df['date'].iloc[0]} 與檔名不符)")
            return pd.DataFrame()

        df = df.drop(columns=["date1"])

        # 3. 確保型別正確
        df["shares"] = df["shares"].astype(float).astype(int)  # 有些 JSON 會是 1445000.0，先轉浮點再轉整數
        df["weight"] = df["weight"].astype(float)

        return df

    def parse_all_files(self):
        """解析目錄下所有 JSON 並合併輸出 CSV"""
        if not self.raw_dir.exists():
            print(f"⚠️ RAW_DIR 不存在：{self.raw_dir}")
            return

        files = sorted(self.raw_dir.rglob("*.json"))
        if not files:
            print(f"⚠️ 找不到 JSON 檔案：{self.raw_dir}")
            return

        print(f"📂 找到 {len(files)} 個 JSON，開始解析 {self.etf_code} ...")

        all_rows = []
        for f in files:
            try:
                df = self.parse_json(f)
                if not df.empty:
                    all_rows.append(df)
            except Exception as e:
                print(f"⚠️ 解析失敗：{f.name} ({e})")

        if not all_rows:
            print("❌ 沒有解析出任何有效資料")
            return

        # 合併資料
        result = pd.concat(all_rows, ignore_index=True)

        # ✨ 新增：全域去重防呆
        result = result.drop_duplicates(subset=['stock_code', 'date'], keep='last')

        # 排序與輸出
        result = result.sort_values(by=["date", "weight"], ascending=[True, False])

        self.out_file.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(self.out_file, index=False, encoding="utf-8-sig")
        print(f"✅ 解析完成，輸出到：{self.out_file}")