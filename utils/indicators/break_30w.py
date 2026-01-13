# utils/indicators/break_30w.py

import pandas as pd


def calc_break_30w(df: pd.DataFrame) -> pd.DataFrame:
    """
    計算日級爆量突破 30W
    回傳 df，新增欄位 daily_break_30w
    """

    df = df.copy()

    df["ma150"] = df["close"].rolling(150).mean()
    df["vol_ma30"] = df["volume"].rolling(30).mean()

    df["daily_break_30w"] = (
        (df["close"] > df["ma150"]) &
        (df["volume"] > df["vol_ma30"] * 2)
    )

    return df
