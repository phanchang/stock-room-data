# utils/indicator_writer.py

from pathlib import Path
import pandas as pd

# 修改為指向 indicators 根目錄 (不再寫死 tw)
INDICATOR_ROOT = Path(__file__).resolve().parent.parent / "data" / "indicators"


def write_daily_indicators(
        df: pd.DataFrame,
        stock_id: str,
        indicator_cols: list[str],
        sub_folder: str = "common",  # 🆕 新增：指定策略子資料夾名稱
        market: str = "tw"  # 🆕 新增：市場別 (預設 tw)
):
    """
    將日級 indicator 事件寫入 indicators parquet

    Args:
        df: 資料來源
        stock_id: 股票代號 (如 2330_TW)
        indicator_cols: 要保留的指標欄位
        sub_folder: 策略名稱 (如 "break_30w", "consolidation")，將建立獨立資料夾
        market: 市場 (tw/us)，建立第二層資料夾
    """

    # 檢查是否有任何觸發
    if not df[indicator_cols].any().any():
        return

    event_df = df.loc[
        df[indicator_cols].any(axis=1),
        ["date"] + indicator_cols
    ].copy()

    event_df["date"] = pd.to_datetime(event_df["date"])

    # 🆕 動態建構路徑: data/indicators/{sub_folder}/{market}/
    output_dir = INDICATOR_ROOT / sub_folder / market
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{stock_id}.parquet"
    event_df.to_parquet(output_path, index=False)

    # print(f"✅ indicators 已輸出：{output_path}") # 除錯用