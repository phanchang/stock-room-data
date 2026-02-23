import pandas as pd
import json
from pathlib import Path


class CapitalFundParser:
    def __init__(self, raw_dir, clean_dir, etf_code="00982A"):
        self.raw_dir = Path(raw_dir)
        self.clean_dir = Path(clean_dir)
        self.etf_code = etf_code
        self.out_file = self.clean_dir / f"{etf_code}.csv"

    def parse_all_files(self):
        if not self.raw_dir.exists():
            return

        files = sorted(self.raw_dir.rglob("*.json"))
        if not files:
            return

        all_rows = []
        for f in files:
            with open(f, "r", encoding="utf-8") as file:
                data = json.load(file)
            stocks = data.get("data", {}).get("stocks", [])
            if not stocks: continue

            df = pd.DataFrame(stocks)
            col_map = {"stocNo": "stock_code", "stocName": "stock_name", "share": "shares", "weight": "weight"}
            df = df[list(col_map.keys()) + ["date1"]].rename(columns=col_map)
            df["date"] = df["date1"].apply(lambda x: pd.to_datetime(str(x).split(' ')[0]).strftime("%Y-%m-%d"))
            df = df.drop(columns=["date1"])
            df["shares"] = df["shares"].astype(float).astype(int)
            df["weight"] = df["weight"].astype(float)
            all_rows.append(df)

        if all_rows:
            result = pd.concat(all_rows, ignore_index=True)
            result = result.sort_values(by=["date", "weight"], ascending=[True, False])
            self.out_file.parent.mkdir(parents=True, exist_ok=True)
            result.to_csv(self.out_file, index=False, encoding="utf-8-sig")
            print(f"✅ [群益投信] 解析完成，輸出到：{self.out_file}")