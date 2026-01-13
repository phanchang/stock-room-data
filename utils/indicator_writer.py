# utils/indicator_writer.py

from pathlib import Path
import pandas as pd


INDICATOR_BASE_PATH = Path(__file__).resolve().parent.parent / "data" / "indicators" / "tw"
INDICATOR_BASE_PATH.mkdir(parents=True, exist_ok=True)


def write_daily_indicators(
    df: pd.DataFrame,
    stock_id: str,
    indicator_cols: list[str]
):
    """
    將日級 indicator 事件寫入 indicators parquet
    """

    event_df = df.loc[
        df[indicator_cols].any(axis=1),
        ["date"] + indicator_cols
    ].copy()

    event_df["date"] = pd.to_datetime(event_df["date"])

    output_path = INDICATOR_BASE_PATH / f"{stock_id}.parquet"
    event_df.to_parquet(output_path, index=False)

    print(f"✅ indicators 已輸出：{output_path}")
    print(event_df)
