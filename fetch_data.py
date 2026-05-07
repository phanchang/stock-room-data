import json
from utils.cache.downloader import StockDownloader


def load_tickers():
    with open('tickers.json', 'r') as f:
        return json.load(f)


def main():
    tickers_config = load_tickers()
    # 合併台股與美股清單
    all_symbols = tickers_config['tw_stocks'] + tickers_config['us_stocks']

    if not all_symbols:
        print("清單為空，停止任務。")
        return

    downloader = StockDownloader()

    print(f"--- 開始抓取任務 (總數: {len(all_symbols)}) ---")

    # 使用你原本寫好的分批更新邏輯，這對未來 2000 檔非常關鍵
    # batch_size=50 表示每 50 檔會休息一下
    results = downloader.batch_update_with_progress(
        all_symbols,
        batch_size=50,
        max_workers=3
    )

    print(f"--- 任務完成 ---")
    print(f"成功: {len(results['success'])}")
    print(f"失敗: {len(results['failed'])}")


if __name__ == "__main__":
    main()