# scripts/check_latest_signals.py
import json
from pathlib import Path
from datetime import datetime

# 設定路徑
PROJECT_ROOT = Path(__file__).resolve().parent.parent
INDEX_PATH = PROJECT_ROOT / "data" / "indicators" / "index.json"


def main():
    if not INDEX_PATH.exists():
        print("❌ 找不到索引檔，請先執行 daily_strategy_runner.py")
        return

    print("📖 讀取策略索引...")
    with open(INDEX_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)

    indicators = data.get('indicators', {})

    # 1. 找出整個資料庫中「最新」的一天是哪一天
    # (遍歷所有策略、所有股票的日期，找最大值)
    all_dates = set()
    for strat_name, stocks in indicators.items():
        for stock_id, dates in stocks.items():
            all_dates.update(dates)

    if not all_dates:
        print("⚠️ 資料庫中沒有任何訊號日期")
        return

    latest_date = max(all_dates)
    print(f"📅 資料庫最新交易日: {latest_date}")
    print("=" * 50)

    # 2. 查詢該日期的訊號
    total_hits = 0
    for strat_name, stocks in indicators.items():
        # 找出這策略在「這一天」有訊號的股票
        today_hits = []
        for stock_id, dates in stocks.items():
            if latest_date in dates:
                today_hits.append(stock_id)

        count = len(today_hits)
        total_hits += count

        print(f"Strategy: {strat_name:<15} | 觸發: {count:3d} 檔")
        if count > 0:
            # 只印前 5 檔範例
            preview = ", ".join(today_hits[:5])
            if count > 5:
                preview += "..."
            print(f"  👉 {preview}")
        print("-" * 50)

    print(f"🎯 {latest_date} 當日訊號總數: {total_hits}")


if __name__ == "__main__":
    main()