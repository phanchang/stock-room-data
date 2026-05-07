# utils/indicator_loader.py

from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
from utils.indicator_index import load_indicator_index

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# 🆕 修正路徑指向 indicators 根目錄，而非 tw
INDICATOR_ROOT = PROJECT_ROOT / "data" / "indicators"


def load_indicator_stocks(
        indicator_name: str,
        days: int | None = None,
        strategy_folder: str | None = None # 🆕 選填：若需使用 legacy 掃描，需指定資料夾
) -> set[str]:
    """
    回傳符合某一 indicator 的股票代號集合 (使用索引檔加速)

    Args:
        indicator_name: 指標欄位名稱 (如 "daily_break_30w")
        days: 近N日內 (None = 不限時間)
        strategy_folder: (Legacy用) 若索引失效，需指定去哪個資料夾掃描 (如 "break_30w")

    Returns:
        符合條件的股票代號集合
    """

    # 1. 優先使用索引檔 (最快，不受資料夾結構影響)
    index = load_indicator_index()

    if indicator_name in index:
        stocks_dict = index[indicator_name]  # {stock_id: [dates]}

        if days is None:
            matched = set(stocks_dict.keys())
            print(f"📌 {indicator_name} (索引): {len(matched)} 檔")
            return matched

        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        matched = set()

        for stock_id, dates in stocks_dict.items():
            if any(date >= cutoff_date for date in dates):
                matched.add(stock_id)

        print(f"📌 {indicator_name} (近{days}日): {len(matched)} 檔")
        return matched

    # 2. 備用方案: 掃描檔案
    # 如果索引找不到，且沒有提供 strategy_folder，就無法掃描
    if not strategy_folder:
        print(f"⚠️ 索引無資料且未指定 strategy_folder，無法載入 {indicator_name}")
        return set()

    print(f"⚠️ 索引檔沒有 {indicator_name}，嘗試掃描資料夾: {strategy_folder}...")
    return load_indicator_stocks_legacy(indicator_name, strategy_folder, days)


def load_indicator_stocks_legacy(
        indicator_name: str,
        strategy_folder: str,  # 🆕 必須指定資料夾
        days: int | None = None
) -> set[str]:
    """備用方案: 直接掃描指定策略資料夾下的 parquet 檔案"""

    # 組合路徑: data/indicators/{strategy_folder}/tw
    # 這裡預設掃描 tw，若有美股需求可再擴充
    target_path = INDICATOR_ROOT / strategy_folder / "tw"

    if not target_path.exists():
        print(f"❌ 路徑不存在: {target_path}")
        return set()

    cutoff_date = None
    if days is not None:
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days)

    matched = set()

    for p in target_path.glob("*.parquet"):
        parts = p.stem.split('_')
        stock_id = parts[0] if parts else p.stem

        try:
            df = pd.read_parquet(p)
        except Exception:
            continue

        if indicator_name not in df.columns:
            continue

        filter_df = df
        if cutoff_date is not None:
            filter_df = df[df["date"] >= cutoff_date]

        if filter_df[indicator_name].any():
            matched.add(stock_id)

    return matched