# parse.py
import sys
import os
from pathlib import Path
from modules.parsers.ezmoney_parser import EZMoneyParser
from modules.parsers.fhtrust_parser import FHTrustParser

# ===== 重要：正確設定專案根目錄 =====
# 方法 1：使用絕對路徑（推薦，最穩定）
#BASE_DIR = Path(r'C:\Users\andychang\Desktop\每日選股\戰情室')

# 方法 2：從 parse.py 位置往上推算（如果目錄結構固定）
# BASE_DIR = Path(__file__).resolve().parents[2]  # 往上兩層到戰情室

# 設定專案路徑 - 從當前檔案往上找到專案根目錄
current_file = Path(__file__).resolve()
RAW_DIR = current_file.parent.parent.parent / 'data' / 'raw'
CLEAN_DIR = current_file.parent.parent.parent / 'data' / 'clean'
#RAW_DIR = BASE_DIR / 'data' / 'raw'
#CLEAN_DIR = BASE_DIR / 'data' / 'clean'

PARSERS = {
    'ezmoney': {
        'class': EZMoneyParser,
        'funds': [
            {
                'name': '00981A',
                'raw_dir': RAW_DIR / 'ezmoney' / '00981A',
                'clean_dir': CLEAN_DIR / 'ezmoney'
            }
        ]
    },
    'fhtrust': {
        'class': FHTrustParser,
        'funds': [
            {
                'name': '00991A',
                'raw_dir': RAW_DIR / 'fhtrust' / '00991A',
                'clean_dir': CLEAN_DIR / 'fhtrust'
            }
        ]
    }
}


def parse_all():
    """解析所有投信的資料"""
    print("=" * 60)
    print("開始清理資料")
    print("=" * 60)

    for company, config in PARSERS.items():
        print(f"\n### {company.upper()} ###")
        parser_class = config['class']

        for fund in config['funds']:
            print(f"\n處理 {fund['name']}...")
            print(f"Raw Dir: {fund['raw_dir']}")
            print(f"Clean Dir: {fund['clean_dir']}")

            # 檢查目錄是否存在
            if not fund['raw_dir'].exists():
                print(f"⚠️ 原始資料目錄不存在：{fund['raw_dir']}")
                continue

            parser = parser_class(
                raw_dir=fund['raw_dir'],
                clean_dir=fund['clean_dir']
            )

            try:
                parser.parse_all_files()
            except Exception as e:
                print(f"❌ 解析失敗：{e}")


def parse_specific(company):
    """解析特定投信"""
    if company not in PARSERS:
        print(f"錯誤：找不到 '{company}'")
        print(f"可用的投信: {', '.join(PARSERS.keys())}")
        return

    config = PARSERS[company]
    parser_class = config['class']

    for fund in config['funds']:
        print(f"\n處理 {fund['name']}...")
        print(f"Raw Dir: {fund['raw_dir']}")
        print(f"Clean Dir: {fund['clean_dir']}")

        if not fund['raw_dir'].exists():
            print(f"⚠️ 原始資料目錄不存在：{fund['raw_dir']}")
            continue

        parser = parser_class(
            raw_dir=fund['raw_dir'],
            clean_dir=fund['clean_dir']
        )

        try:
            parser.parse_all_files()
        except Exception as e:
            print(f"❌ 解析失敗：{e}")


def print_help():
    """顯示使用說明"""
    print("ETF 資料解析器 - 使用說明")
    print("=" * 60)
    print("基本用法:")
    print("  python parse.py --all         # 解析所有投信")
    print("  python parse.py ezmoney       # 只解析 EZMoney")
    print("  python parse.py fhtrust       # 只解析復華投信")
    print("  python parse.py --help        # 顯示此說明")
    print("\n可用的投信:")
    for company, config in PARSERS.items():
        print(f"  - {company}")
        for fund in config['funds']:
            print(f"    * {fund['name']}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cmd = sys.argv[1]

        if cmd == '--all':
            parse_all()
        elif cmd in ['--help', '-h']:
            print_help()
        elif cmd in PARSERS:
            parse_specific(cmd)
        else:
            print(f"未知參數: {cmd}")
            print_help()
    else:
        # 預設執行全部
        parse_all()