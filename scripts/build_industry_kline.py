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


def convert_tw_month_to_date(tw_month_str):
    """將民國年月 '115/03' 轉換為西元月底日期 '2026-03-31'，方便與日線對齊"""
    try:
        y, m = tw_month_str.split('/')
        year = int(y) + 1911
        month = int(m)
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        return next_month - pd.Timedelta(days=1)
    except:
        return pd.NaT


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
    print(f"[System] 板塊 K 線合成作業啟動 (V9.3 - 融合高階基本面) | {datetime.now():%H:%M:%S}")

    sector_dict = load_sector_mappings(project_root)
    all_needed_sids = set(sid for sids in sector_dict.values() for sid in sids)
    shares_dict = load_issued_shares(project_root, all_needed_sids)

    valid_sectors = {}
    for tag, sids in sector_dict.items():
        valid_sids = [sid for sid in sids if sid in shares_dict]
        if len(valid_sids) >= 5:
            valid_sectors[tag] = valid_sids

    print("⏳ 正在將 K 線與基本面資料載入記憶體 (CacheManager & JSON)...")
    cache = CacheManager()

    # --- 預載 JSON 基礎面資料 ---
    json_dir = project_root / 'data' / 'fundamentals'
    stock_fundamentals = {}
    for sid in all_needed_sids:
        j_path = json_dir / f"{sid}.json"
        if j_path.exists():
            try:
                with open(j_path, 'r', encoding='utf-8') as f:
                    stock_fundamentals[sid] = json.load(f)
            except:
                pass

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
            df_subset['Date'] = pd.to_datetime(df_subset['Date'])
            stock_dfs[sid] = df_subset

    output_dir = project_root / 'data' / 'cache' / 'sector'
    output_dir.mkdir(parents=True, exist_ok=True)

    total = len(valid_sectors)
    last_pct = -1

    for i, (tag, sids) in enumerate(valid_sectors.items()):
        progress_pct = int(((i + 1) / total) * 100)

        if progress_pct > last_pct:
            print(f"\rPROGRESS: {progress_pct} | ⏳ 正在合成: {i + 1}/{total} [{tag}]...{' ' * 10}", end="", flush=True)
            last_pct = progress_pct

        dfs_to_concat = [stock_dfs[sid] for sid in sids if sid in stock_dfs]
        if not dfs_to_concat:
            continue

        # --- 1. 合成價格與成交量 K 線 (維持既有邏輯) ---
        all_dates = pd.Index([])
        for df in dfs_to_concat:
            all_dates = all_dates.union(df['Date'])
        all_dates = all_dates.sort_values()

        aligned_dfs = []
        for df in dfs_to_concat:
            df_indexed = df.set_index('Date')
            df_indexed = df_indexed[~df_indexed.index.duplicated(keep='last')]
            df_reindexed = df_indexed.reindex(all_dates)

            for col in ['adj_open', 'adj_high', 'adj_low', 'adj_close']:
                df_reindexed[col] = df_reindexed[col].ffill()

            df_reindexed['volume'] = df_reindexed['volume'].fillna(0)
            df_reindexed['shares'] = df_reindexed['shares'].ffill().bfill()

            df_reindexed.index.name = 'Date'
            df_reindexed = df_reindexed.reset_index()
            aligned_dfs.append(df_reindexed)

        sector_df = pd.concat(aligned_dfs, ignore_index=True)

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

        # --- 2. 結合基本面與籌碼擴散率 ---
        daily_inst_records = []
        monthly_rev_records = []

        # 只取有在 K 線計算內的成分股
        actual_sids = [sid for sid in sids if sid in stock_dfs and sid in stock_fundamentals]

        for sid in actual_sids:
            jdata = stock_fundamentals[sid]

            # 法人籌碼
            for row in jdata.get('institutional_investors', []):
                dt = pd.to_datetime(row.get('date'))
                if pd.isna(dt): continue
                is_buying = 1 if (float(row.get('foreign_buy_sell', 0)) + float(
                    row.get('invest_trust_buy_sell', 0)) > 0) else 0
                daily_inst_records.append({'Date': dt, 'sid': sid, 'is_buying': is_buying})

            # 月營收
            for row in jdata.get('revenue', []):
                dt = convert_tw_month_to_date(row.get('month', ''))
                if pd.isna(dt): continue
                yoy = float(row.get('rev_yoy', 0))
                monthly_rev_records.append({'Date': dt, 'sid': sid, 'rev_yoy': yoy, 'is_growing': 1 if yoy > 0 else 0})

        # 彙整日頻率籌碼
        if daily_inst_records:
            df_inst = pd.DataFrame(daily_inst_records)
            inst_agg = df_inst.groupby('Date')['is_buying'].mean().reset_index()
            inst_agg['Legal_Diffusion'] = (inst_agg['is_buying'] * 100).round(2)
            inst_agg = inst_agg.set_index('Date')[['Legal_Diffusion']]
            result_df = result_df.join(inst_agg, how='left')

        result_df['Legal_Diffusion'] = result_df.get('Legal_Diffusion', 0.0).fillna(0.0)

        # 彙整月頻率營收並前向填充 (ffill) 至日線
        if monthly_rev_records:
            df_rev = pd.DataFrame(monthly_rev_records)
            rev_agg = df_rev.groupby('Date').agg(Rev_Diffusion=('is_growing', 'mean'), YoY_Median=('rev_yoy', 'median'))
            rev_agg['Rev_Diffusion'] = (rev_agg['Rev_Diffusion'] * 100).round(2)
            rev_agg['YoY_Median'] = rev_agg['YoY_Median'].round(2)
            rev_agg = rev_agg.sort_index()
            rev_agg['YoY_Accel'] = rev_agg['YoY_Median'].diff().round(2)

            # 將月營收重新索引到 K 線的日期長度，並往下填充
            full_idx = pd.date_range(start=result_df.index.min(), end=result_df.index.max(), freq='D')
            rev_daily = rev_agg.reindex(full_idx).ffill()
            rev_daily.index.name = 'Date'

            result_df = result_df.join(rev_daily, how='left')
            # 填充還沒有營收公布之前的早期空缺
            result_df[['Rev_Diffusion', 'YoY_Median', 'YoY_Accel']] = result_df[
                ['Rev_Diffusion', 'YoY_Median', 'YoY_Accel']].ffill().fillna(0.0)
        else:
            result_df['Rev_Diffusion'] = 0.0
            result_df['YoY_Median'] = 0.0
            result_df['YoY_Accel'] = 0.0

        # --- 3. 存檔 ---
        save_path = output_dir / f"IDX_{tag}.parquet"
        result_df.to_parquet(save_path)

    print(f"\n[System] 板塊合成作業完成！共產出 {total} 個 IDX_*.parquet 檔案 (含基本面)。")
    print("PROGRESS: 100", flush=True)


if __name__ == "__main__":
    main()