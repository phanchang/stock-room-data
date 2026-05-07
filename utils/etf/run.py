# run.py
"""
ETF 資料處理完整流程
1. 下載原始資料（main.py）
2. 解析資料（parse.py）
"""

import sys
from datetime import datetime

# Import 下載功能
from main import manual_fetch_all, manual_fetch_specific, run_scheduler

# Import 解析功能
from parse import parse_all as parse_all_data, parse_specific as parse_specific_data


def run_full_pipeline(company=None):
    """
    執行完整流程：下載 → 解析

    Args:
        company: 指定投信，None 表示全部
    """
    print("=" * 60)
    print("ETF 資料處理完整流程")
    print("=" * 60)
    print(f"開始時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 步驟 1：下載原始資料
    print("\n" + "=" * 60)
    print("步驟 1/2：下載原始資料")
    print("=" * 60)

    try:
        if company:
            manual_fetch_specific(company)
        else:
            manual_fetch_all()
        print("✅ 下載完成")
    except Exception as e:
        print(f"❌ 下載失敗: {e}")
        return False

    # 步驟 2：解析資料
    print("\n" + "=" * 60)
    print("步驟 2/2：解析資料")
    print("=" * 60)

    try:
        if company:
            parse_specific_data(company)
        else:
            parse_all_data()
        print("✅ 解析完成")
    except Exception as e:
        print(f"❌ 解析失敗: {e}")
        return False

    print("\n" + "=" * 60)
    print("🎉 所有流程執行完畢！")
    print(f"結束時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    return True


def print_help():
    """顯示使用說明"""
    print("ETF 資料處理系統 - 使用說明")
    print("=" * 60)
    print("\n【完整流程】下載 + 解析")
    print("  python run.py                 # 處理所有投信")
    print("  python run.py ezmoney         # 只處理 EZMoney")
    print("  python run.py fhtrust         # 只處理復華投信")
    print("  python run.py capitalfund     # 只處理群益投信")  # 👈 新增這行

    print("\n【僅下載】")
    print("  python main.py --now          # 立即下載所有投信")
    print("  python main.py --now ezmoney  # 只下載 EZMoney")
    print("  python main.py                # 啟動排程器")

    print("\n【僅解析】")
    print("  python parse.py --all         # 解析所有投信")
    print("  python parse.py ezmoney       # 只解析 EZMoney")

    print("\n【其他】")
    print("  python run.py --help          # 顯示此說明")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cmd = sys.argv[1]

        if cmd in ['--help', '-h']:
            print_help()
        elif cmd in ['ezmoney', 'fhtrust', 'capitalfund']:
            run_full_pipeline(company=cmd)
        else:
            print(f"未知參數: {cmd}")
            print_help()
    else:
        # 預設：處理所有投信
        run_full_pipeline()
