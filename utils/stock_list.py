# utils/stock_list.py

import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def get_stock_list(include_market: bool = False) -> list[str] | list[tuple[str, str]]:
    """
    取得所有上市上櫃股票代號

    Args:
        include_market: 是否包含市場別資訊

    Returns:
        - include_market=False: ['1101', '2330', ...]
        - include_market=True: [('1101', 'TW'), ('8182', 'TWO'), ...]
    """
    stock_list_path = PROJECT_ROOT / "data" / "stock_list.csv"

    if not stock_list_path.exists():
        print(f"⚠️ 找不到 stock_list.csv: {stock_list_path}")
        print(f"💡 嘗試從快取目錄讀取...")
        return get_stock_list_from_cache(include_market=include_market)

    try:
        df = pd.read_csv(stock_list_path, dtype=str)

        # 可能的欄位名稱
        possible_cols = ['代號', 'code', 'stock_code', '證券代號', 'symbol', 'stock_id']

        for col in possible_cols:
            if col in df.columns:
                stock_list = df[col].astype(str).str.strip().tolist()
                # 過濾掉非數字的代號 (例如 ETF 的英文代號)
                stock_list = [s for s in stock_list if s.isdigit()]

                if include_market:
                    # 如果有市場別欄位就使用，否則預設 TW
                    if 'market' in df.columns or '市場' in df.columns:
                        market_col = 'market' if 'market' in df.columns else '市場'
                        stock_list = [(code, df.loc[i, market_col]) for i, code in enumerate(stock_list)]
                    else:
                        stock_list = [(code, 'TW') for code in stock_list]

                print(f"✅ 從 stock_list.csv 載入 {len(stock_list)} 檔股票")
                return stock_list

        print(f"⚠️ stock_list.csv 找不到股票代號欄位,可用欄位: {df.columns.tolist()}")
        return get_stock_list_from_cache(include_market=include_market)

    except Exception as e:
        print(f"❌ 讀取 stock_list.csv 失敗: {e}")
        return get_stock_list_from_cache(include_market=include_market)


def get_stock_list_from_cache(include_market: bool = False) -> list[str] | list[tuple[str, str]]:
    """
    從快取目錄讀取所有股票代號 (備用方案)

    Args:
        include_market: 是否包含市場別資訊

    Returns:
        - include_market=False: ['1101', '2330', ...]
        - include_market=True: [('1101', 'TW'), ('8182', 'TWO'), ...]
    """
    cache_dir = PROJECT_ROOT / "data" / "cache" / "tw"

    if not cache_dir.exists():
        print(f"❌ 快取目錄不存在: {cache_dir}")
        return []

    try:
        # 取得所有 .parquet 檔案
        parquet_files = list(cache_dir.glob("*.parquet"))

        if not parquet_files:
            print(f"⚠️ 快取目錄是空的")
            return []

        # 從檔名提取股票代號
        stock_list = []
        for file in parquet_files:
            # ✅ 修正：使用 split 而非 replace
            parts = file.stem.split('_')  # "1101_TW" → ["1101", "TW"], "8182_TWO" → ["8182", "TWO"]

            if not parts:
                continue

            stock_id = parts[0]
            market = parts[1] if len(parts) > 1 else "TW"

            if stock_id.isdigit():  # 只保留純數字代號
                if include_market:
                    stock_list.append((stock_id, market))
                else:
                    stock_list.append(stock_id)

        # 去重並排序
        if include_market:
            stock_list = sorted(set(stock_list), key=lambda x: x[0])
        else:
            stock_list = sorted(set(stock_list))

        print(f"✅ 從快取目錄載入 {len(stock_list)} 檔股票")
        return stock_list

    except Exception as e:
        print(f"❌ 從快取目錄讀取失敗: {e}")
        return []


def get_stock_name_mapping() -> dict[str, str]:
    """
    取得股票代號 → 名稱的對照表

    Returns:
        dict: {"1101": "台泥", "2330": "台積電", ...}
    """
    mapping = {}

    # 方法1: 從 StockList CSV 讀取
    twse_file = PROJECT_ROOT / 'StockList' / 'TWSE_ESVUFR.csv'
    two_file = PROJECT_ROOT / 'StockList' / 'TWO_ESVUFR.csv'

    for csv_file in [twse_file, two_file]:
        if not csv_file.exists():
            continue

        try:
            df = pd.read_csv(csv_file, encoding='utf-8')

            # 找出代號和名稱欄位
            code_col = None
            name_col = None

            # 可能的欄位名稱
            code_candidates = ['股票代號及名稱', '代號', 'code', 'stock_code', '證券代號']
            name_candidates = ['名稱', 'name', 'stock_name', '證券名稱']

            for col in df.columns:
                if any(c in col for c in code_candidates) and code_col is None:
                    code_col = col
                if any(c in col for c in name_candidates) and name_col is None:
                    name_col = col

            # 如果有合併欄位 (如 "1101　台泥")
            if code_col and '股票代號及名稱' in code_col:
                for raw_text in df[code_col].astype(str):
                    # 分離代號和名稱
                    parts = raw_text.split()
                    if len(parts) >= 2:
                        code = parts[0].strip()
                        name = parts[1].strip()
                        if code.isdigit():
                            mapping[code] = name

            # 如果有獨立的代號和名稱欄位
            elif code_col and name_col:
                for idx, row in df.iterrows():
                    code = str(row[code_col]).strip()
                    name = str(row[name_col]).strip()

                    # 從代號中提取純數字
                    import re
                    match = re.match(r'^(\d+)', code)
                    if match:
                        code = match.group(1)
                        mapping[code] = name

        except Exception as e:
            print(f"⚠️ 讀取 {csv_file.name} 失敗: {e}")
            continue

    # 方法2 (備用): 從快取的 CSV 檔讀取
    if not mapping:
        stock_list_csv = PROJECT_ROOT / "data" / "stock_list.csv"
        if stock_list_csv.exists():
            try:
                df = pd.read_csv(stock_list_csv, dtype=str)
                if '代號' in df.columns and '名稱' in df.columns:
                    for _, row in df.iterrows():
                        code = str(row['代號']).strip()
                        name = str(row['名稱']).strip()
                        if code.isdigit():
                            mapping[code] = name
            except Exception as e:
                print(f"⚠️ 讀取 stock_list.csv 失敗: {e}")

    if mapping:
        print(f"✅ 載入 {len(mapping)} 檔股票名稱對照")
    else:
        print(f"⚠️ 無法載入股票名稱對照表")

    return mapping


if __name__ == "__main__":
    # 測試用
    print("=" * 60)
    print("測試 1: get_stock_list()")
    print("=" * 60)
    stocks = get_stock_list()
    print(f"\n📋 股票清單 (前 20 檔):")
    print(stocks[:20])
    print(f"\n總共: {len(stocks)} 檔")

    print("\n" + "=" * 60)
    print("測試 2: get_stock_list(include_market=True)")
    print("=" * 60)
    stocks_with_market = get_stock_list(include_market=True)
    print(f"\n📋 股票清單 (前 20 檔):")
    print(stocks_with_market[:20])

    print("\n" + "=" * 60)
    print("測試 3: get_stock_name_mapping()")
    print("=" * 60)
    name_mapping = get_stock_name_mapping()

    # 顯示前 10 個對照
    for i, (code, name) in enumerate(list(name_mapping.items())[:10]):
        print(f"{code}: {name}")

    print(f"\n總共: {len(name_mapping)} 檔")