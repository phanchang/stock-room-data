# 檔案路徑: scripts/build_industry_kline.py
import pandas as pd
import numpy as np
import json
from pathlib import Path
import sys
from datetime import datetime

# 設定專案根目錄
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.cache.manager import CacheManager


def load_sector_mappings(project_root):
    """讀取 MDJ 產業與概念股標籤，回傳 tag -> list of sids"""
    sector_dict = {}

    # 1. 讀取 MDJ 細產業
    dj_path = project_root / 'data' / 'dj_industry.csv'
    if dj_path.exists():
        dj_df = pd.read_csv(dj_path, dtype=str)
        for _, row in dj_df.iterrows():
            sid = str(row['sid']).strip()
            # 處理多重標籤 (逗號分隔)
            sub_inds = str(row['dj_sub_ind']).split(',')
            for tag in sub_inds:
                tag = tag.strip()
                if tag and tag != 'nan':
                    sector_dict.setdefault(tag, set()).add(sid)

    # 2. 讀取概念股標籤
    concept_path = project_root / 'data' / 'concept_tags.csv'
    if concept_path.exists():
        concept_df = pd.read_csv(concept_path, dtype=str)
        for _, row in concept_df.iterrows():
            sid = str(row['sid']).strip()
            concepts = str(row['sub_concepts']).split(',')
            for tag in concepts:
                tag = tag.strip()
                if tag and tag != 'nan':
                    sector_dict.setdefault(tag, set()).add(sid)

    return sector_dict


def load_issued_shares(project_root, all_sids):
    """從 fundamentals json 中讀取最新發行股數"""
    shares_dict = {}
    fund_dir = project_root / 'data' / 'fundamentals'

    for sid in all_sids:
        json_path = fund_dir / f"{sid}.json"
        if json_path.exists():
            try:
                with open(json_path, 'r', encoding='utf-8') as jf:
                    jdata = json.load(jf)
                    inst_data = jdata.get('institutional_investors', [])
                    if inst_data:
                        shares = float(inst_data[0].get('issued_shares', 0) or 0)
                        if shares > 0:
                            shares_dict[sid] = shares
            except Exception:
                pass
    return shares_dict


def main():
    print(f"[System] 板塊 K 線合成作業啟動 (V9.0) | {datetime.now():%H:%M:%S}")

    # 1. 載入標籤與過濾
    sector_dict = load_sector_mappings(project_root)
    all_needed_sids = set(sid for sids in sector_dict.values() for sid in sids)
    shares_dict = load_issued_shares(project_root, all_needed_sids)

    # 過濾條件：成分股需 >= 5 檔
    valid_sectors = {}
    for tag, sids in sector_dict.items():
        # 只保留有發行股數資料的 SID
        valid_sids = [sid for sid in sids if sid in shares_dict]
        if len(valid_sids) >= 5:
            valid_sectors[tag] = valid_sids

    print(f"📊 共篩選出 {len(valid_sectors)} 個有效板塊 (成分股 >= 5 檔)")

    # 2. 啟動 Cache Manager 載入 K 線
    print("⏳ 正在將 K 線資料載入記憶體 (CacheManager)...")
    cache = CacheManager()

    # 快取已讀取的 DataFrame 以加速多板塊合成
    stock_dfs = {}
    for sid in all_needed_sids:
        df = cache.load(f"{sid}.TW")
        if df is None or df.empty:
            df = cache.load(f"{sid}.TWO")

        if df is not None and not df.empty:
            # 確保欄位存在且重置 Index 讓 Date 變成欄位以便後續合併
            if 'Date' not in df.columns:
                df = df.reset_index()

            # 準備合成所需欄位 (防呆：若無 adj_close，用 close 頂替)
            need_cols = ['Date', 'adj_open', 'adj_high', 'adj_low', 'adj_close', 'volume']
            missing = [c for c in need_cols if c not in df.columns]

            if missing:
                for c in missing:
                    raw_col = c.replace('adj_', '')
                    if raw_col in df.columns:
                        df[c] = df[raw_col]
                    else:
                        df[c] = 0.0

            # 只取需要的欄位並帶入股數與 SID
            df_subset = df[need_cols].copy()
            df_subset['shares'] = shares_dict.get(sid, 0)
            stock_dfs[sid] = df_subset

    # 3. 執行板塊虛擬 K 線合成
    output_dir = project_root / 'data' / 'cache' / 'sector'
    output_dir.mkdir(parents=True, exist_ok=True)

    total = len(valid_sectors)
    for i, (tag, sids) in enumerate(valid_sectors.items()):
        if i % 20 == 0 or i == total - 1:
            print(f"   ⏳ 正在合成板塊 K 線: {i + 1}/{total} [{tag}]...")

        dfs_to_concat = [stock_dfs[sid] for sid in sids if sid in stock_dfs]
        if not dfs_to_concat:
            continue

        # 合併該板塊所有成分股
        sector_df = pd.concat(dfs_to_concat, ignore_index=True)

        # 計算市值 (加權價)
        for col in ['adj_open', 'adj_high', 'adj_low', 'adj_close']:
            sector_df[f'weighted_{col}'] = sector_df[col] * sector_df['shares']

        # 依日期 GroupBy 進行聚合
        agg_logic = {
            'weighted_adj_open': 'sum',
            'weighted_adj_high': 'sum',
            'weighted_adj_low': 'sum',
            'weighted_adj_close': 'sum',
            'shares': 'sum',
            'volume': 'sum'
        }

        daily_sector = sector_df.groupby('Date').agg(agg_logic).reset_index()
        daily_sector = daily_sector[daily_sector['shares'] > 0].copy()

        # 產出虛擬 K 線 (市值加權平均價)
        result_df = pd.DataFrame()
        result_df['Date'] = daily_sector['Date']

        # 板塊指數視為已還原，故 raw 與 adj 數值相同
        for col in ['open', 'high', 'low', 'close']:
            val = daily_sector[f'weighted_adj_{col}'] / daily_sector['shares']
            result_df[col] = val.round(2)
            result_df[f'adj_{col}'] = val.round(2)

        result_df['volume'] = daily_sector['volume'].astype(int)
        result_df['dividends'] = 0.0  # 指數無除息缺口

        # 設定 Date 為 index 以符合你既有 Parquet 格式
        result_df.set_index('Date', inplace=True)
        result_df.sort_index(inplace=True)

        # 印出前幾個板塊的驗證資訊
        if i < 3:
            print(f"\n[驗證] 板塊: {tag} | 成分股數: {len(sids)}")
            print(result_df[['adj_close', 'volume']].tail(3))
            print("-" * 30)

        # 存檔 (加上 IDX_ 前綴)
        save_path = output_dir / f"IDX_{tag}.parquet"
        result_df.to_parquet(save_path)

    print(f"\n[System] 板塊合成作業完成！共產出 {total} 個 IDX_*.parquet 檔案。")


if __name__ == "__main__":
    main()