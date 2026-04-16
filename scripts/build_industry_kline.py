# 檔案路徑: scripts/build_industry_kline.py
import pandas as pd
import numpy as np
import json
from pathlib import Path
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing

# 設定專案根目錄
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.cache.manager import CacheManager

# ── 效能優化 1：月份字串轉日期加上快取，避免每筆營收記錄重複計算 ────────────
_tw_month_cache = {}

def convert_tw_month_to_date(tw_month_str):
    """將民國年月 '115/03' 轉換為西元月底日期；結果快取避免重複計算"""
    if tw_month_str in _tw_month_cache:
        return _tw_month_cache[tw_month_str]
    try:
        y, m = tw_month_str.split('/')
        year = int(y) + 1911
        month = int(m)
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        result = next_month - pd.Timedelta(days=1)
    except:
        result = pd.NaT
    _tw_month_cache[tw_month_str] = result
    return result


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


# ── 效能優化 2：JSON 預載改為多執行緒並行 I/O ────────────────────────────────
def _load_one_json(args):
    """單一 JSON 讀取工作，供 ThreadPoolExecutor 呼叫"""
    sid, path = args
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return sid, json.load(f)
    except Exception:
        return sid, None


def load_all_fundamentals(json_dir, all_sids, max_workers=16):
    """用執行緒池並行讀取所有 JSON，I/O 密集型任務效果顯著"""
    tasks = [(sid, json_dir / f"{sid}.json")
             for sid in all_sids if (json_dir / f"{sid}.json").exists()]
    result = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for sid, data in pool.map(_load_one_json, tasks):
            if data is not None:
                result[sid] = data
    return result


# ── 效能優化 3：單一板塊合成邏輯獨立成頂層函式，供多進程呼叫 ────────────────
def build_one_sector(args):
    """
    計算單一板塊的 IDX parquet，回傳 (tag, True/None)。
    頂層函式才能被 ProcessPoolExecutor pickle。
    """
    import pandas as pd
    import numpy as np
    import json
    from pathlib import Path

    tag, sids, stock_dfs_records, stock_fundamentals, output_dir_str = args

    # 月份快取在子進程內重新建立
    _local_month_cache = {}
    def _tw_month(s):
        if s in _local_month_cache:
            return _local_month_cache[s]
        try:
            from datetime import datetime as dt
            y, m = s.split('/')
            year = int(y) + 1911
            month = int(m)
            if month == 12:
                nxt = dt(year + 1, 1, 1)
            else:
                nxt = dt(year, month + 1, 1)
            res = nxt - pd.Timedelta(days=1)
        except:
            res = pd.NaT
        _local_month_cache[s] = res
        return res

    try:
        # 還原 stock_dfs
        stock_dfs = {}
        for sid, (recs, cols) in stock_dfs_records.items():
            df = pd.DataFrame(recs, columns=cols)
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date')
            stock_dfs[sid] = df

        dfs_to_concat = [stock_dfs[sid] for sid in sids if sid in stock_dfs]
        if not dfs_to_concat:
            return tag, None

        # ── 1. 合成加權 K 線 ─────────────────────────────────────────────────
        all_dates = pd.DatetimeIndex(
            sorted(set().union(*[set(df.index) for df in dfs_to_concat]))
        )
        aligned = []
        for df in dfs_to_concat:
            df = df[~df.index.duplicated(keep='last')]
            df = df.reindex(all_dates)
            df[['adj_open', 'adj_high', 'adj_low', 'adj_close']] = \
                df[['adj_open', 'adj_high', 'adj_low', 'adj_close']].ffill()
            df['volume'] = df['volume'].fillna(0)
            df['shares'] = df['shares'].ffill().bfill()
            aligned.append(df)

        sector_df = pd.concat(aligned)
        for col in ['adj_open', 'adj_high', 'adj_low', 'adj_close']:
            sector_df[f'w_{col}'] = sector_df[col] * sector_df['shares']

        daily = sector_df.groupby(sector_df.index).agg(
            w_adj_open=('w_adj_open', 'sum'),
            w_adj_high=('w_adj_high', 'sum'),
            w_adj_low=('w_adj_low', 'sum'),
            w_adj_close=('w_adj_close', 'sum'),
            shares=('shares', 'sum'),
            volume=('volume', 'sum'),
        )
        daily = daily[daily['shares'] > 0]

        result_df = pd.DataFrame(index=daily.index)
        result_df.index.name = 'Date'
        for col in ['open', 'high', 'low', 'close']:
            val = (daily[f'w_adj_{col}'] / daily['shares']).round(2)
            result_df[col] = val
            result_df[f'adj_{col}'] = val
        result_df['volume'] = daily['volume'].astype(int)
        result_df['dividends'] = 0.0
        result_df.sort_index(inplace=True)

        # ── 2. 等權平均漲幅 ──────────────────────────────────────────────────
        pct_cols = {}
        for sid in [s for s in sids if s in stock_dfs]:
            s_close = stock_dfs[sid]['adj_close']
            pct_cols[sid] = s_close.pct_change(fill_method=None) * 100

        if pct_cols:
            eq_df = pd.DataFrame(pct_cols)
            result_df['Equal_Pct_1d'] = eq_df.mean(axis=1).reindex(result_df.index).round(4).fillna(0.0)
            # ── 效能優化 4：NumPy 向量化複利計算，取代 rolling.apply + lambda ──
            ep = result_df['Equal_Pct_1d'].values
            log1p = np.log1p(ep / 100.0)
            roll5 = np.array([np.sum(log1p[max(0, k-4):k+1]) for k in range(len(log1p))])
            result_df['Equal_Pct_5d'] = np.round(np.expm1(roll5) * 100, 4)
            result_df['Equal_Pct_5d'] = result_df['Equal_Pct_5d'].fillna(0.0)
        else:
            result_df['Equal_Pct_1d'] = 0.0
            result_df['Equal_Pct_5d'] = 0.0

        # ── 3. 基本面籌碼擴散率 ──────────────────────────────────────────────
        actual_sids = [s for s in sids if s in stock_dfs and s in stock_fundamentals]
        daily_inst = []
        monthly_rev = []

        for sid in actual_sids:
            jdata = stock_fundamentals[sid]
            for row in jdata.get('institutional_investors', []):
                dt_val = pd.to_datetime(row.get('date'))
                if pd.isna(dt_val): continue
                is_buying = 1 if (float(row.get('foreign_buy_sell', 0)) +
                                  float(row.get('invest_trust_buy_sell', 0)) > 0) else 0
                daily_inst.append({'Date': dt_val, 'is_buying': is_buying})

            for row in jdata.get('revenue', []):
                dt_val = _tw_month(row.get('month', ''))
                if pd.isna(dt_val): continue
                yoy = float(row.get('rev_yoy', 0))
                monthly_rev.append({'Date': dt_val, 'rev_yoy': yoy,
                                    'is_growing': 1 if yoy > 0 else 0})

        if daily_inst:
            df_inst = pd.DataFrame(daily_inst)
            inst_agg = (df_inst.groupby('Date')['is_buying'].mean() * 100).round(2).rename('Legal_Diffusion')
            result_df = result_df.join(inst_agg, how='left')
        result_df['Legal_Diffusion'] = result_df.get('Legal_Diffusion', pd.Series(0.0, index=result_df.index)).fillna(0.0)

        if monthly_rev:
            df_rev = pd.DataFrame(monthly_rev)
            rev_agg = df_rev.groupby('Date').agg(
                Rev_Diffusion=('is_growing', 'mean'),
                YoY_Median=('rev_yoy', 'median')
            )
            rev_agg['Rev_Diffusion'] = (rev_agg['Rev_Diffusion'] * 100).round(2)
            rev_agg['YoY_Median'] = rev_agg['YoY_Median'].round(2)
            rev_agg['YoY_Accel'] = rev_agg['YoY_Median'].diff().round(2)
            full_idx = pd.date_range(start=result_df.index.min(), end=result_df.index.max(), freq='D')
            rev_daily = rev_agg.reindex(full_idx).ffill()
            rev_daily.index.name = 'Date'
            result_df = result_df.join(rev_daily, how='left')
            result_df[['Rev_Diffusion', 'YoY_Median', 'YoY_Accel']] = \
                result_df[['Rev_Diffusion', 'YoY_Median', 'YoY_Accel']].ffill().fillna(0.0)
        else:
            result_df['Rev_Diffusion'] = 0.0
            result_df['YoY_Median'] = 0.0
            result_df['YoY_Accel'] = 0.0

        # ── 4. 存檔 ──────────────────────────────────────────────────────────
        save_path = Path(output_dir_str) / f"IDX_{tag}.parquet"
        result_df.to_parquet(save_path)
        return tag, True

    except Exception as e:
        return tag, None


def main():
    print(f"[System] 板塊 K 線合成作業啟動 (V9.4 - 並行加速版) | {datetime.now():%H:%M:%S}")

    sector_dict = load_sector_mappings(project_root)
    all_needed_sids = set(sid for sids in sector_dict.values() for sid in sids)
    shares_dict = load_issued_shares(project_root, all_needed_sids)

    valid_sectors = {}
    for tag, sids in sector_dict.items():
        valid_sids = [sid for sid in sids if sid in shares_dict]
        if len(valid_sids) >= 5:
            valid_sectors[tag] = valid_sids

    # ── 效能優化 2：並行讀取 JSON ────────────────────────────────────────────
    print("⏳ 並行載入基本面 JSON (16 執行緒)...")
    t0 = datetime.now()
    json_dir = project_root / 'data' / 'fundamentals'
    stock_fundamentals = load_all_fundamentals(json_dir, all_needed_sids, max_workers=16)
    print(f"   完成，耗時 {(datetime.now()-t0).total_seconds():.1f}s，共 {len(stock_fundamentals)} 筆")

    # ── K 線讀取（維持單執行緒，CacheManager 不保證 thread-safe）───────────
    print("⏳ 載入 K 線資料...")
    t1 = datetime.now()
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
            for c in need_cols:
                if c not in df.columns:
                    raw = c.replace('adj_', '')
                    df[c] = df[raw] if raw in df.columns else 0.0
            df_subset = df[need_cols].copy()
            df_subset['shares'] = shares_dict.get(sid, 0)
            df_subset['Date'] = pd.to_datetime(df_subset['Date'])
            stock_dfs[sid] = df_subset
    print(f"   完成，耗時 {(datetime.now()-t1).total_seconds():.1f}s，共 {len(stock_dfs)} 檔")

    output_dir = project_root / 'data' / 'cache' / 'sector'
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── 效能優化 3：序列化後多進程並行合成 ──────────────────────────────────
    print("⏳ 序列化 K 線資料...")
    stock_dfs_records = {
        sid: (df.reset_index().to_dict('records'), list(df.reset_index().columns))
        for sid, df in stock_dfs.items()
    }

    n_workers = min(multiprocessing.cpu_count(), len(valid_sectors), 8)
    print(f"⚡ 啟動 {n_workers} 個子進程並行合成 {len(valid_sectors)} 個板塊...")

    task_args = [
        (tag, sids,
         {sid: stock_dfs_records[sid] for sid in sids if sid in stock_dfs_records},
         {sid: stock_fundamentals.get(sid, {}) for sid in sids},
         str(output_dir))
        for tag, sids in valid_sectors.items()
    ]

    t2 = datetime.now()
    done = 0
    total = len(task_args)
    last_pct = -1

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(build_one_sector, arg): arg[0] for arg in task_args}
        for future in as_completed(futures):
            tag_done, ok = future.result()
            done += 1
            pct = int(done / total * 100)
            if pct > last_pct:
                print(f"\rPROGRESS: {pct} | ✅ {done}/{total} [{tag_done}]{' '*15}", end="", flush=True)
                last_pct = pct

    elapsed = (datetime.now() - t2).total_seconds()
    print(f"\n[System] 板塊合成完成！共 {total} 個 IDX_*.parquet (含基本面 + 等權漲幅)，合成耗時 {elapsed:.1f}s。")
    print("PROGRESS: 100", flush=True)


if __name__ == "__main__":
    main()