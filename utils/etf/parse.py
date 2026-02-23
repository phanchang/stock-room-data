# parse.py
import sys
import os
from pathlib import Path

# ===== 1. 設定專案路徑 (必須在 import 自訂模組之前) =====
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# ===== 2. 引入模組 (改用絕對路徑，並加入群益) =====
from utils.etf.modules.parsers.ezmoney_parser import EZMoneyParser
from utils.etf.modules.parsers.fhtrust_parser import FHTrustParser
from utils.etf.modules.parsers.capitalfund_parser import CapitalFundParser

RAW_DIR = project_root / 'data' / 'raw'
CLEAN_DIR = project_root / 'data' / 'clean'

PARSERS = {
    'ezmoney': {
        'class': EZMoneyParser,
        'funds': [
            {'name': '00981A', 'raw_dir': RAW_DIR / 'ezmoney' / '00981A', 'clean_dir': CLEAN_DIR / 'ezmoney'}
        ]
    },
    'fhtrust': {
        'class': FHTrustParser,
        'funds': [
            {'name': '00991A', 'raw_dir': RAW_DIR / 'fhtrust' / '00991A', 'clean_dir': CLEAN_DIR / 'fhtrust'}
        ]
    },
    'capitalfund': {
        'class': CapitalFundParser,
        'funds': [
            {'name': '00982A', 'raw_dir': RAW_DIR / 'capitalfund' / '00982A', 'clean_dir': CLEAN_DIR / 'capitalfund'}
        ]
    }
}

# ... 下面的 parse_all, parse_specific 等函式完全不用動 ...


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