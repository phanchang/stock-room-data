import os
import pandas as pd
import numpy as np
from pathlib import Path


def scan_breakout_patterns(df: pd.DataFrame) -> pd.DataFrame:
    """
    量化型態掃描 + 獨立法人吃貨特徵萃取
    """
    df = df.copy()
    # 統一標準欄位名稱
    col_map = {c: c.capitalize() for c in df.columns if c.lower() in ['open', 'high', 'low', 'close', 'volume']}
    df = df.rename(columns=col_map)
    df = df.sort_index()

    if len(df) < 300:
        df['Signal_Breakout'] = False
        return df

    # 1. 均線與流動性
    df['MA5'] = df['Close'].rolling(window=5).mean()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA60'] = df['Close'].rolling(window=60).mean()
    df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()

    # 2. 均線收斂度 (Spread)
    df['MA_Max'] = df[['MA5', 'MA20', 'MA60']].max(axis=1)
    df['MA_Min'] = df[['MA5', 'MA20', 'MA60']].min(axis=1)
    df['Spread'] = (df['MA_Max'] - df['MA_Min']) / df['MA_Min']

    # 計算「整理期間長度」
    df['Is_Tight'] = (df['Spread'] <= 0.06).astype(int)
    df['Consolidation_Days'] = df['Is_Tight'].groupby((df['Is_Tight'] != df['Is_Tight'].shift()).cumsum()).cumsum()

    # 3. 位階與型態條件
    roll_max_240 = df['High'].rolling(window=240).max()
    roll_min_240 = df['Low'].rolling(window=240).min()
    df['Percentile'] = (df['Close'] - roll_min_240) / (roll_max_240 - roll_min_240)

    cond_bottom = df['Percentile'] <= 0.40
    cond_shrink_vol = df['Volume'].rolling(window=5).max() < (df['Vol_MA20'] * 0.7)
    df['Signal_Consolidation'] = cond_bottom & (df['Consolidation_Days'] >= 10) & cond_shrink_vol

    # ==================================================
    # 🕵️ 4. 法人獨立籌碼吃貨判定 (找尋歷史資料中的籌碼欄位)
    # ==================================================
    # 動態尋找你的 parquet 檔案中可能的外資/投信欄位名稱
    foreign_col = next((c for c in df.columns if c.lower() in ['foreign', '外資買賣超', '外資', 'foreign_buy']), None)
    trust_col = next(
        (c for c in df.columns if c.lower() in ['trust', 'investment_trust', '投信買賣超', '投信', 'it_buy']), None)

    cond_chip = pd.Series(True, index=df.index)  # 預設 True(如果資料庫沒籌碼欄位就不阻擋)
    df['Chip_Status'] = "無籌碼資料"

    if foreign_col and trust_col:
        # 計算近 20 日的累積買賣超與買進天數
        f_sum_20 = df[foreign_col].rolling(20).sum()
        t_sum_20 = df[trust_col].rolling(20).sum()
        f_buy_days = (df[foreign_col] > 0).rolling(20).sum()
        t_buy_days = (df[trust_col] > 0).rolling(20).sum()

        # 情境 A: 投信波段吃貨 (連買8天以上，或累積買超大於均量)
        cond_it_buy = (t_buy_days >= 8) | (t_sum_20 > df['Vol_MA20'] * 0.5)

        # 情境 B: 外資波段吃貨 (買超過10天，或累積大買)
        cond_f_buy = (f_buy_days >= 10) | (f_sum_20 > df['Vol_MA20'] * 1.5)

        # 情境 C: 土洋對作 (外資狂倒貨 < 0，但投信默默接刀 > 0 且買超過5天)
        cond_diverge = (f_sum_20 < 0) & (t_sum_20 > 0) & (t_buy_days >= 5)

        cond_chip = cond_it_buy | cond_f_buy | cond_diverge

        # 標記狀態供驗證顯示
        conditions = [cond_diverge, cond_it_buy, cond_f_buy]
        choices = ["⚔️ 土洋對作", "🏦 投信吃貨", "🌍 外資吃貨"]
        df['Chip_Status'] = np.select(conditions, choices, default="無明顯吃貨")

    # ==================================================

    # 5. 突破特徵 (加入籌碼過濾)
    cond_recent_consolidation = df['Signal_Consolidation'].rolling(window=20).max() == 1
    cond_above_ma60 = df['Close'] > df['MA60']
    cond_breakout_vol = (df['Volume'] >= df['Vol_MA20'] * 1.5)
    cond_red_k = (df['Close'] > df['Open']) & (((df['Close'] - df['Open']) / df['Open']) > 0.02)

    # 🚀 最終訊號：整理 + 均線之上 + 爆量紅K + 法人有在吃貨 + 流動性過濾
    df['Signal_Breakout'] = (cond_recent_consolidation & cond_above_ma60 &
                             cond_breakout_vol & cond_red_k & cond_chip)
    df['Signal_Breakout'] = df['Signal_Breakout'] & (df['Vol_MA20'] >= 500)

    return df


def run_backtest():
    print("==================================================")
    print("🔬 量化勝率回測引擎啟動：【整理突破 + 法人獨立吃貨偵測】")
    print("   - 停利目標：未來 100 天內達到 +30%")
    print("   - 停損防護：未來 100 天內跌破 -15%")
    print("   - 籌碼濾網：嚴格判定外資或投信的獨立吃貨行為")
    print("==================================================")

    base_path = Path(__file__).resolve().parent.parent
    snapshot_path = base_path / "data" / "strategy_results" / "factor_snapshot.parquet"
    cache_dir = base_path / "data" / "cache" / "tw"

    if not snapshot_path.exists():
        print("找不到快照檔案")
        return

    df_snap = pd.read_parquet(snapshot_path)
    if 'industry' in df_snap.columns:
        df_target = df_snap[df_snap['industry'].str.contains('電子|半導體|光電|網通|零組件|電腦', na=False)]
    else:
        df_target = df_snap

    trades = []

    for idx, row in df_target.iterrows():
        sid = str(row['sid']).strip()
        name = row['name']

        target_file = cache_dir / f"{sid}_TW.parquet"
        if not target_file.exists():
            target_file = cache_dir / f"{sid}_TWO.parquet"
            if not target_file.exists(): continue

        try:
            df_k = pd.read_parquet(target_file)
            if 'Date' in df_k.columns:
                df_k['Date'] = pd.to_datetime(df_k['Date'],
                                              unit='ms' if str(df_k['Date'].dtype) in ['int64', 'float64'] else None)
                df_k.set_index('Date', inplace=True)
            df_k = df_k.sort_index()

            # 取得訊號
            df_res = scan_breakout_patterns(df_k)

            # 尋找交易機會 (扣除最後 100 天)
            eval_df = df_res.iloc[:-100]
            breakout_indices = np.where(eval_df['Signal_Breakout'])[0]

            last_trade_idx = -999
            COOLDOWN_DAYS = 40

            for b_idx in breakout_indices:
                if b_idx - last_trade_idx < COOLDOWN_DAYS:
                    continue

                last_trade_idx = b_idx
                entry_date = eval_df.index[b_idx]
                entry_price = eval_df['Close'].iloc[b_idx]
                consolidation_len = eval_df['Consolidation_Days'].iloc[b_idx - 1] if b_idx > 0 else 0
                chip_status = eval_df['Chip_Status'].iloc[b_idx]

                # 未滿足明確吃貨特徵者略過 (如果資料庫有籌碼欄位的話)
                if chip_status == "無明顯吃貨":
                    continue

                # 未來 100 天路徑模擬
                future_window = df_res.iloc[b_idx + 1: b_idx + 101]
                if len(future_window) < 100: continue

                max_high = future_window['High'].max()
                min_low = future_window['Low'].min()

                max_gain = ((max_high - entry_price) / entry_price) * 100
                max_drop = ((min_low - entry_price) / entry_price) * 100

                outcome = 'TIE'
                days_to_outcome = 100

                for f_i, (f_date, f_row) in enumerate(future_window.iterrows()):
                    if f_row['Low'] <= entry_price * 0.85:
                        outcome = 'LOSS'
                        days_to_outcome = f_i + 1
                        break

                    if f_row['High'] >= entry_price * 1.15:
                        outcome = 'WIN'
                        days_to_outcome = f_i + 1
                        break

                trades.append({
                    'sid': sid,
                    'name': name,
                    'entry_date': entry_date.strftime('%Y-%m-%d'),
                    'entry_price': entry_price,
                    'consolidation_len': consolidation_len,
                    'chip_status': chip_status,
                    'max_gain': max_gain,
                    'max_drop': max_drop,
                    'outcome': outcome,
                    'days_to_outcome': days_to_outcome
                })

        except Exception as e:
            continue

    # ==========================
    # 統計與輸出報表
    # ==========================
    if not trades:
        print("❌ 未找到任何符合條件的交易紀錄 (可能您的資料庫缺乏法人買賣超欄位，或是條件過於嚴苛)。")
        return

    df_trades = pd.DataFrame(trades)

    wins = len(df_trades[df_trades['outcome'] == 'WIN'])
    losses = len(df_trades[df_trades['outcome'] == 'LOSS'])
    ties = len(df_trades[df_trades['outcome'] == 'TIE'])
    total = len(df_trades)

    win_rate = (wins / total) * 100 if total > 0 else 0
    strict_win_rate = (wins / (wins + losses)) * 100 if (wins + losses) > 0 else 0

    print(f"\n📊 【加入籌碼濾網後 - 回測總結】 (共掃描 {total} 筆獨立訊號)")
    print(f"   ➤ 成功達標 (+30%): {wins} 筆")
    print(f"   ➤ 停損出場 (-15%): {losses} 筆")
    print(f"   ➤ 盤整未明 (卡資金): {ties} 筆")
    print(f"   ----------------------------------")
    print(f"   🏆 絕對勝率 (Win/Total): {win_rate:.1f}%")
    print(f"   ⚔️ 實戰勝率 (排除平局): {strict_win_rate:.1f}%\n")

    print("👁️ 【抽樣 15 筆明細供線圖驗證】")
    sample_trades = df_trades.sample(n=min(15, total), random_state=42).sort_values('entry_date')
    for _, t in sample_trades.iterrows():
        icon = "🟢" if t['outcome'] == 'WIN' else "🔴" if t['outcome'] == 'LOSS' else "⚪"
        print(
            f"{icon} {t['entry_date']} | {t['sid']} {t['name']:<5} | [{t['chip_status']}] | 盤整:{t['consolidation_len']:>2.0f}天 | 期間最低:{t['max_drop']:>6.1f}% | 最高:{t['max_gain']:>6.1f}% | 結果: {t['outcome']} ({t['days_to_outcome']}天觸發)")


if __name__ == "__main__":
    run_backtest()