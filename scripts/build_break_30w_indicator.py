import pandas as pd
from pathlib import Path
import sys

# 專案 root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.indicators.break_30w import calc_break_30w
from utils.indicator_writer import write_daily_indicators


STOCK_ID = "1101_TW"
DATA_PATH = PROJECT_ROOT / "data" / "cache" / "tw" / f"{STOCK_ID}.parquet"

df = pd.read_parquet(DATA_PATH)

df = df.reset_index().rename(columns={"Date": "date"})

df = calc_break_30w(df)

write_daily_indicators(
    df=df,
    stock_id=STOCK_ID,
    indicator_cols=["daily_break_30w"]
)
