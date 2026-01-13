from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INDICATOR_PATH = PROJECT_ROOT / "data" / "indicators" / "tw"


def load_indicator_stocks(
        indicator_name: str,
        days: int | None = None  # 🆕 新增參數
) -> set[str]:
    """
    回傳符合某一 indicator 的股票代號集合

    Args:
        indicator_name: 指標名稱 (如 "daily_break_30w")
        days: 近N日內 (None = 不限時間)

    Returns:
        符合條件的股票代號集合
    """

    if not INDICATOR_PATH.exists():
        return set()

    # 🆕 計算時間範圍
    cutoff_date = None
    if days is not None:
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days)

    matched = set()

    for p in INDICATOR_PATH.glob("*.parquet"):
        stock_id = p.stem  # 1101_TW

        try:
            df = pd.read_parquet(p)
        except Exception:
            continue

        if indicator_name not in df.columns:
            continue

        # 🆕 時間過濾
        if cutoff_date is not None:
            df = df[df["date"] >= cutoff_date]

        # 檢查是否有 True
        if df[indicator_name].any():
            matched.add(stock_id.replace("_TW", ""))

    return matched