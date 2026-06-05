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

# ── 全域共享變數（繞過進程通訊瓶頸） ──────────────────────────────────
GLOBAL_STOCK_DFS = {}
GLOBAL_INST_MATRIX = None  # 全市場法人買賣狀態矩陣 (Date x Sid)
GLOBAL_REV_MATRIX = None  # 全市場營收增長狀態矩陣 (Date x Sid)


# ── 向量化民國年月轉換（利用 Pandas Series 加速，完全拔除逐筆迴圈） ──────────
def vectorize_tw_months(series):
    """傳入 pd.Series 格式的民國年月 (例如 '115/03')，一次性向量化轉換成西元月底日"""
    if series.empty:
        return pd.to_datetime(series)

    # 提取民國年與月份
    splitted = series.str.split('/')
    y_roc = splitted.str[0].astype(float).fillna(0).astype(int)
    m = splitted.str[1].astype(float).fillna(1).astype(int)

    y_ad = y_roc + 1911

    # 計算下個月的第一天
    next_m = m + 1
    next_y = y_ad + (next_m > 12).astype(int)
    next_m = np.where(next_m > 12, 1, next_m)

    # 構造西元字串再由 Pandas 向量化轉換，最後減去 1 天得到月底日
    # 🔒 安全防禦：將 NumPy ndarray 轉換為 pd.Series，才能正確發揮 .str.zfill(2) 向量化補零的功能
    next_m_series = pd.Series(next_m).astype(str).str.zfill(2)
    dt_str = pd.Series(next_y).astype(str) + '-' + next_m_series + '-01'

    # 重新對齊原本的 index 結構，避免 pivot table 對帳失敗
    dt_str.index = series.index
    return pd.to_datetime(dt_str, errors='coerce') - pd.Timedelta(days=1)


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


def _load_one_json(args):
    """單一 JSON 純 I/O 讀取工作，供 ThreadPoolExecutor 呼叫，絕不在此處解析時間"""
    sid, path = args
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return sid, json.load(f)
    except Exception:
        return sid, None


def load_all_fundamentals(json_dir, all_sids, max_workers=16):
    """用執行緒池純粹並行讀取所有 JSON，回歸純 I/O 密集型優勢"""
    tasks = [(sid, json_dir / f"{sid}.json") for sid in all_sids if (json_dir / f"{sid}.json").exists()]
    result = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for sid, data in pool.map(_load_one_json, tasks):
            if data is not None:
                result[sid] = data
    return result


# ── 子進程記憶體初始化 ──────────────────────────────────────────────────
def init_worker(stock_dfs, inst_matrix, rev_matrix):
    global GLOBAL_STOCK_DFS, GLOBAL_INST_MATRIX, GLOBAL_REV_MATRIX
    GLOBAL_STOCK_DFS = stock_dfs
    GLOBAL_INST_MATRIX = inst_matrix
    GLOBAL_REV_MATRIX = rev_matrix


# ── 核心合成邏輯 ─────────────────────────────────────────────────────────
def build_one_sector(args):
    tag, sids, output_dir_str = args
    global GLOBAL_STOCK_DFS, GLOBAL_INST_MATRIX, GLOBAL_REV_MATRIX

    try:
        dfs_to_concat = [GLOBAL_STOCK_DFS[sid] for sid in sids if sid in GLOBAL_STOCK_DFS]
        if not dfs_to_concat:
            return tag, None

        # ── 1. 合成加權 K 線 ─────────────────────────────────────────────────
        all_dates = pd.DatetimeIndex(sorted(set().union(*[set(df.index) for df in dfs_to_concat])))
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

        # ── 2. 等權平均漲幅 (向量化複利) ──────────────────────────────────────
        valid_sids = [s for s in sids if s in GLOBAL_STOCK_DFS]
        pct_cols = {sid: GLOBAL_STOCK_DFS[sid]['adj_close'].pct_change(fill_method=None) * 100 for sid in valid_sids}

        if pct_cols:
            eq_df = pd.DataFrame(pct_cols)
            result_df['Equal_Pct_1d'] = eq_df.mean(axis=1).reindex(result_df.index).round(4).fillna(0.0)
            ep = result_df['Equal_Pct_1d'].values
            log1p = np.log1p(ep / 100.0)
            roll5 = np.array([np.sum(log1p[max(0, k - 4):k + 1]) for k in range(len(log1p))])
            result_df['Equal_Pct_5d'] = np.round(np.expm1(roll5) * 100, 4).fillna(0.0)
        else:
            result_df['Equal_Pct_1d'] = 0.0
            result_df['Equal_Pct_5d'] = 0.0

        # ── 3. 全張全域矩陣極速橫向切片 ───────────────────────────────────────
        sector_sids = [s for s in sids if s in GLOBAL_INST_MATRIX.columns]
        if sector_sids:
            result_df['Legal_Diffusion'] = (GLOBAL_INST_MATRIX[sector_sids].mean(axis=1, skipna=True) * 100).reindex(
                result_df.index).round(2).fillna(0.0)
        else:
            result_df['Legal_Diffusion'] = 0.0

        # 基本面營收擴散率與中位數
        sector_rev_sids = [s for s in sids if s in GLOBAL_REV_MATRIX.columns]
        if sector_rev_sids:
            sub_rev = GLOBAL_REV_MATRIX[sector_rev_sids]

            is_growing_mat = np.where(pd.isna(sub_rev), np.nan, np.where(sub_rev > 0, 1, 0))
            with np.errstate(empty='ignore'):
                rev_diff_series = pd.Series(np.nanmean(is_growing_mat, axis=1) * 100, index=sub_rev.index)

            median_series = sub_rev.median(axis=1, skipna=True)
            accel_series = median_series.diff()

            full_idx = pd.date_range(start=result_df.index.min(), end=result_df.index.max(), freq='D')
            rev_daily = pd.DataFrame({
                'Rev_Diffusion': rev_diff_series,
                'YoY_Median': median_series,
                'YoY_Accel': accel_series
            }).reindex(full_idx).ffill()
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
    print(f"[System] 板塊 K 線合成作業啟動 (V9.6 - 終極矩陣向量化版) | {datetime.now():%H:%M:%S}")

    sector_dict = load_sector_mappings(project_root)
    all_needed_sids = set(sid for sids in sector_dict.values() for sid in sids)
    shares_dict = load_issued_shares(project_root, all_needed_sids)

    valid_sectors = {}
    for tag, sids in sector_dict.items():
        valid_sids = [sid for sid in sids if sid in shares_dict]
        if len(valid_sids) >= 5:
            valid_sectors[tag] = valid_sids

    # ── 1. 執行緒池純粹載入 JSON (繞過 GIL 計算干擾) ─────────────────────────
    print("⏳ 並行載入基本面 JSON (16 執行緒)...")
    t0 = datetime.now()
    json_dir = project_root / 'data' / 'fundamentals'
    stock_fundamentals = load_all_fundamentals(json_dir, all_needed_sids, max_workers=16)
    print(f"   完成，耗時 {(datetime.now() - t0).total_seconds():.1f}s，共 {len(stock_fundamentals)} 筆")

    # ── 2. 🟢 真正的 Pandas 向量化大矩陣極速建構 ─────────────────────────────
    print("⏳ 正在利用 Pandas 向量化建構全域大型矩陣...")
    t_mat = datetime.now()

    inst_flat = []
    rev_flat = []

    # 僅做輕量化的 list 收集，絕不在迴圈內呼叫 to_datetime
    for sid, jdata in stock_fundamentals.items():
        inst_rows = jdata.get('institutional_investors', [])
        if inst_rows:
            for r in inst_rows:
                inst_flat.append((r.get('date'), sid,
                                  float(r.get('foreign_buy_sell', 0)) + float(r.get('invest_trust_buy_sell', 0))))

        rev_rows = jdata.get('revenue', [])
        if rev_rows:
            for r in rev_rows:
                rev_flat.append((r.get('month'), sid, float(r.get('rev_yoy', 0))))

    # 核心精髓：一整批丟給 Pandas 進行 C 語言級別的向量化解析
    if inst_flat:
        df_inst_all = pd.DataFrame(inst_flat, columns=['Date', 'sid', 'net_buy'])
        df_inst_all['Date'] = pd.to_datetime(df_inst_all['Date'], errors='coerce')
        df_inst_all = df_inst_all.dropna(subset=['Date'])
        # 向量化判定買賣狀態
        df_inst_all['is_buying'] = np.where(df_inst_all['net_buy'] > 0, 1.0, 0.0)
        inst_matrix = df_inst_all.pivot_table(index='Date', columns='sid', values='is_buying')
    else:
        inst_matrix = pd.DataFrame()

    if rev_flat:
        df_rev_all = pd.DataFrame(rev_flat, columns=['Month', 'sid', 'rev_yoy'])
        # 呼叫全向量化民國年月轉換，1秒鐘刷完幾萬筆！
        df_rev_all['Date'] = vectorize_tw_months(df_rev_all['Month'])
        df_rev_all = df_rev_all.dropna(subset=['Date'])
        rev_matrix = df_rev_all.pivot_table(index='Date', columns='sid', values='rev_yoy')
    else:
        rev_matrix = pd.DataFrame()

    print(
        f"   矩陣化完成！籌碼矩陣規模: {inst_matrix.shape} | 營收矩陣規模: {rev_matrix.shape} | 總耗時: {(datetime.now() - t_mat).total_seconds():.1f}s")

    print("⏳ 載入 K 線資料並預先轉換時間索引...")
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
            df_subset = df_subset.set_index('Date')
            stock_dfs[sid] = df_subset
    print(f"   完成，耗時 {(datetime.now() - t1).total_seconds():.1f}s，共 {len(stock_dfs)} 檔")

    output_dir = project_root / 'data' / 'cache' / 'sector'
    output_dir.mkdir(parents=True, exist_ok=True)

    n_workers = min(multiprocessing.cpu_count(), len(valid_sectors), 8)
    print(f"⚡ 啟動 {n_workers} 個子進程並行合成 {len(valid_sectors)} 個板塊 (終極記憶體矩陣共享機制)...")

    task_args = [(tag, sids, str(output_dir)) for tag, sids in valid_sectors.items()]

    t2 = datetime.now()
    done = 0
    total = len(task_args)
    last_pct = -1

    with ProcessPoolExecutor(
            max_workers=n_workers,
            initializer=init_worker,
            initargs=(stock_dfs, inst_matrix, rev_matrix)
    ) as executor:
        futures = {executor.submit(build_one_sector, arg): arg[0] for arg in task_args}
        for future in as_completed(futures):
            tag_done, ok = future.result()
            done += 1
            pct = int(done / total * 100)
            if pct > last_pct:
                print(f"\rPROGRESS: {pct} | ✅ {done}/{total} [{tag_done}]{' ' * 15}", end="", flush=True)
                last_pct = pct

    elapsed = (datetime.now() - t2).total_seconds()
    print(f"\n[System] 板塊合成完成！共 {total} 個 IDX_*.parquet，合成耗時 {elapsed:.1f}s。")
    print("PROGRESS: 100", flush=True)


if __name__ == "__main__":
    main()