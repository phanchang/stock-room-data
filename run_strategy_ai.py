import sys
from pathlib import Path

# 設定路徑
current_file = Path(__file__).resolve()
project_root = current_file.parent
sys.path.insert(0, str(project_root))

from utils.etf.modules.parsers.ezmoney_parser import EZMoneyParser

print("=== 開始強制修復本機 CSV ===")

# 1. 重建 00981A (會把 raw 裡面幾十個 excel 重新合併成正確的 CSV)
print("\n[1] 正在重建 00981A...")
p1 = EZMoneyParser(
    raw_dir="data/raw/ezmoney/00981A",
    clean_dir="data/clean/ezmoney",
    etf_code="00981A"
)
p1.parse_all_files()

# 2. 重建 00403A
print("\n[2] 正在重建 00403A...")
p2 = EZMoneyParser(
    raw_dir="data/raw/ezmoney/00403A",
    clean_dir="data/clean/ezmoney",
    etf_code="00403A"
)
p2.parse_all_files()

print("\n🎉 修復完成！請重新啟動你的 ETF UI 介面。")