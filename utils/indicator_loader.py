# utils/indicator_loader.py

from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
from utils.indicator_index import load_indicator_index

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INDICATOR_PATH = PROJECT_ROOT / "data" / "indicators" / "tw"


def load_indicator_stocks(
        indicator_name: str,
        days: int | None = None
) -> set[str]:
    """
    回傳符合某一 indicator 的股票代號集合 (使用索引檔加速)

    Args:
        indicator_name: 指標名稱 (如 "daily_break_30w")
        days: 近N日內 (None = 不限時間)

    Returns:
        符合條件的股票代號集合
    """

    # 🆕 優先使用索引檔 (超快!)
    index = load_indicator_index()

    if indicator_name in index:
        stocks_dict = index[indicator_name]  # {stock_id: [dates]}

        # 如果不限時間,直接回傳所有股票
        if days is None:
            matched = set(stocks_dict.keys())
            print(f"📌 {indicator_name} (索引): {len(matched)} 檔")
            return matched

        # 🆕 如果有時間限制,過濾日期
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        matched = set()

        for stock_id, dates in stocks_dict.items():
            # 檢查是否有任何日期在範圍內
            if any(date >= cutoff_date for date in dates):
                matched.add(stock_id)

        print(f"📌 {indicator_name} (近{days}日): {len(matched)} 檔")
        return matched

    # 🔄 備用方案:掃描 parquet 檔案 (較慢)
    print(f"⚠️ 索引檔沒有 {indicator_name},使用掃描模式...")
    return load_indicator_stocks_legacy(indicator_name, days)


def load_indicator_stocks_legacy(
        indicator_name: str,
        days: int | None = None
) -> set[str]:
    """備用方案:直接掃描 parquet 檔案"""

    if not INDICATOR_PATH.exists():
        return set()

    cutoff_date = None
    if days is not None:
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days)

    matched = set()

    for p in INDICATOR_PATH.glob("*.parquet"):
        # ✅ 修正：使用 split 而非 replace
        parts = p.stem.split('_')  # "8182_TWO" → ["8182", "TWO"]
        stock_id = parts[0] if parts else p.stem  # 取第一個部分作為股票代碼

        try:
            df = pd.read_parquet(p)
        except Exception:
            continue

        if indicator_name not in df.columns:
            continue

        if cutoff_date is not None:
            df = df[df["date"] >= cutoff_date]

        if df[indicator_name].any():
            matched.add(stock_id)

    return matched


if __name__ == "__main__":
    # 測試用
    print("\n=== 測試索引模式 ===")
    stocks = load_indicator_stocks("daily_break_30w")
    print(f"全部: {len(stocks)} 檔")
    print(f"範例: {list(stocks)[:10]}")

    print("\n=== 測試時間過濾 ===")
    stocks_recent = load_indicator_stocks("daily_break_30w", days=30)
    print(f"近30日: {len(stocks_recent)} 檔")