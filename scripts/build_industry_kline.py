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

    dj_path = project_root / 'data' / 'dj_industry.csv'
    concept_path = project_root / 'data' / 'concept_tags.csv'

    if dj_path.exists():
        dj_df = pd.read_csv(dj_path, dtype=str)
        for _, row in dj_df.iterrows():
            sid = str(row['sid']).strip()
            sub_inds = str(row['dj_sub_ind']).split(',')
            for tag in sub_inds:
                tag = tag.strip()
                if tag and tag != 'nan':
                    sector_dict.setdefault(tag, set()).add(sid)

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
    """優先從 shares_cache.json 讀取最新發行股數，解決雲地時間差問題"""
    shares_dict = {}
    shares_cache_path = project_root / 'data' / 'shares_cache.json'

    if shares_cache_path.exists():
        try:
            with open(shares_cache_path, 'r', encoding='utf-8') as f:
                shares_dict = json.load(f)
        except Exception:
            pass

    if not shares_dict:
        snapshot_path = project_root / 'data' / 'strategy_results' / 'factor_snapshot.parquet'
        if snapshot_path.exists():
            try:
                df = pd.read_parquet(snapshot_path)
                for _, row in df.iterrows():
                    sid = str(row.get('sid', '')).strip()
                    shares = float(row.get('issued_shares', 0) or 0)
                    if sid and shares > 0:
                        shares_dict[sid] = shares
            except Exception:
                pass

    if not shares_dict:
        for sid in all_sids:
            shares_dict[sid] = 1.0

    return shares_dict


def main():
    print(f"[System] 板塊 K 線合成作業啟動 (V9.2) | {datetime.now():%H:%M:%S}")

    sector_dict = load_sector_mappings(project_root)
    all_needed_sids = set(sid for sids in sector_dict.values() for sid in sids)
    shares_dict = load_issued_shares(project_root, all_needed_sids)

    valid_sectors = {}
    for tag, sids in sector_dict.items():
        valid_sids = [sid for sid in sids if sid in shares_dict]
        if len(valid_sids) >= 5:
            valid_sectors[tag] = valid_sids

    print("⏳ 正在將 K 線資料載入記憶體 (CacheManager)...")
    cache = CacheManager()

    stock_dfs = {}
    for sid in all_needed_sids:
        df = cache.load(f"{sid}.TW")
        if df is None or df.empty:
            df = cache.load(f"{sid}.TWO")

        if df is not None and not df.empty:
            if 'Date' not in df.columns:
                df = df.reset_index()

            need_cols = ['Date', 'adj_open', 'adj_high', 'adj_low', 'adj_close', 'volume']
            missing = [c for c in need_cols if c not in df.columns]

            if missing:
                for c in missing:
                    raw_col = c.replace('adj_', '')
                    if raw_col in df.columns:
                        df[c] = df[raw_col]
                    else:
                        df[c] = 0.0

            df_subset = df[need_cols].copy()
            df_subset['shares'] = shares_dict.get(sid, 0)
            # 強制轉型 Date 為時間格式，以便後續順利對齊
            df_subset['Date'] = pd.to_datetime(df_subset['Date'])
            stock_dfs[sid] = df_subset

    output_dir = project_root / 'data' / 'cache' / 'sector'
    output_dir.mkdir(parents=True, exist_ok=True)

    total = len(valid_sectors)
    last_pct = -1  # 👇 新增：紀錄上一次的百分比

    for i, (tag, sids) in enumerate(valid_sectors.items()):
        progress_pct = int(((i + 1) / total) * 100)

        # 👇 修改：只有當百分比跳動時才印出，並用 \r 讓終端機維持單行刷新
        if progress_pct > last_pct:
            print(f"\rPROGRESS: {progress_pct} | ⏳ 正在合成: {i + 1}/{total} [{tag}]...{' ' * 10}", end="", flush=True)
            last_pct = progress_pct

        dfs_to_concat = [stock_dfs[sid] for sid in sids if sid in stock_dfs]
        if not dfs_to_concat:
            continue

        # 🚀 核心修復：強制對齊所有成分股的時間軸
        all_dates = pd.Index([])
        for df in dfs_to_concat:
            all_dates = all_dates.union(df['Date'])
        all_dates = all_dates.sort_values()

        aligned_dfs = []
        for df in dfs_to_concat:
            df_indexed = df.set_index('Date')
            # 排除極少數可能發生的日期重複問題
            df_indexed = df_indexed[~df_indexed.index.duplicated(keep='last')]
            # 依據板塊的完整時間軸重建 Index
            df_reindexed = df_indexed.reindex(all_dates)

            # 價格遇到空缺，維持前一天的收盤價 (向前填充)
            for col in ['adj_open', 'adj_high', 'adj_low', 'adj_close']:
                df_reindexed[col] = df_reindexed[col].ffill()

            # 成交量遇到空缺，補為 0
            df_reindexed['volume'] = df_reindexed['volume'].fillna(0)

            # 股數防呆填充
            df_reindexed['shares'] = df_reindexed['shares'].ffill().bfill()

            df_reindexed.index.name = 'Date'
            df_reindexed = df_reindexed.reset_index()
            aligned_dfs.append(df_reindexed)

        # 合併對齊後的成分股
        sector_df = pd.concat(aligned_dfs, ignore_index=True)

        # 計算市值 (加權價)
        for col in ['adj_open', 'adj_high', 'adj_low', 'adj_close']:
            sector_df[f'weighted_{col}'] = sector_df[col] * sector_df['shares']

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

        result_df = pd.DataFrame()
        result_df['Date'] = daily_sector['Date']

        for col in ['open', 'high', 'low', 'close']:
            val = daily_sector[f'weighted_adj_{col}'] / daily_sector['shares']
            result_df[col] = val.round(2)
            result_df[f'adj_{col}'] = val.round(2)

        result_df['volume'] = daily_sector['volume'].astype(int)
        result_df['dividends'] = 0.0

        result_df.set_index('Date', inplace=True)
        result_df.sort_index(inplace=True)

        save_path = output_dir / f"IDX_{tag}.parquet"
        result_df.to_parquet(save_path)

    print(f"\n[System] 板塊合成作業完成！共產出 {total} 個 IDX_*.parquet 檔案。")
    print("PROGRESS: 100", flush=True)


if __name__ == "__main__":
    main()