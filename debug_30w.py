import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# 引入你的策略模組以取得設定
from utils.strategies.technical import TechnicalStrategies


def check_status(condition, title, value_str, rule_str):
    """輔助印出單項檢查結果"""
    mark = "[O]" if condition else "[X]"
    print(f"  {mark} {title:<12}: {value_str:<15} | 規則: {rule_str}")
    return condition


def debug_30w_interactive():
    print("=" * 60)
    print(" 戰情室 30W 策略交談式除錯工具 ".center(60))
    print("=" * 60)

    stock_id = input("1. 請輸入股號 (例如 2360): ").strip()
    target_date_str = input("2. 請輸入要驗證的日期 (YYYY-MM-DD): ").strip()

    try:
        target_dt = pd.to_datetime(target_date_str)
    except Exception:
        print("❌ 日期格式錯誤，請使用 YYYY-MM-DD。")
        return

    # 1. 讀取資料並決定後綴 (TW / TWO)
    raw_id = stock_id.split('_')[0]
    base_dir = Path("data/cache/tw")
    csv_path = Path("data/stock_list.csv")
    suffix = "_TW"  # 預設為上市

    if csv_path.exists():
        try:
            df_list = pd.read_csv(csv_path, dtype=str)
            mask = df_list.apply(lambda row: row.astype(str).str.contains(raw_id).any(), axis=1)
            if mask.any():
                row_text = " ".join(df_list[mask].iloc[0].astype(str).tolist()).upper()
                if '上櫃' in row_text or 'OTC' in row_text or 'TWO' in row_text:
                    suffix = "_TWO"
        except Exception as e:
            print(f"⚠️ 讀取 stock_list.csv 發生錯誤: {e}")

    path = base_dir / f"{raw_id}{suffix}.parquet"
    if not path.exists():
        fallback_tw = base_dir / f"{raw_id}_TW.parquet"
        fallback_two = base_dir / f"{raw_id}_TWO.parquet"
        if fallback_tw.exists():
            path = fallback_tw
        elif fallback_two.exists():
            path = fallback_two

    if not path.exists():
        print(f"❌ 找不到本地資料檔: {path}")
        return

    try:
        df_source = pd.read_parquet(path)
        df_source.columns = [c.capitalize() for c in df_source.columns]
        df_source.index = pd.to_datetime(df_source.index)
    except Exception as e:
        print(f"❌ 讀取資料失敗: {e}")
        return

    # 2. 轉換為週線
    rule = 'W-FRI'
    logic = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
    agg_dict = {k: v for k, v in logic.items() if k in df_source.columns}
    df = df_source.resample(rule).agg(agg_dict).dropna()

    if len(df) < 35:
        print("❌ 資料筆數不足 35 週，無法計算 30W 均線。")
        return

    # 3. 計算 MA30 與前置變數
    df['MA30'] = df['Close'].rolling(window=30).mean()
    df['Prev_Close'] = df['Close'].shift(1)

    if target_dt not in df.index:
        closest_idx = df.index.get_indexer([target_dt], method='nearest')[0]
        aligned_dt = df.index[closest_idx]
        print(f"\n⚠️ 提示: {target_date_str} 非週線結算日，自動對齊至最近週線: {aligned_dt.strftime('%Y-%m-%d')}")
        target_dt = aligned_dt

    i = df.index.get_loc(target_dt)
    if i < 30:
        print("❌ 該日期之前的數據不足 30 週。")
        return

    cfg = TechnicalStrategies.get_config()
    curr = df.iloc[i]
    prev = df.iloc[i - 1]

    prev_c = prev['Close']
    curr_ma = curr['MA30']
    p_ma_val = prev['MA30']

    pct_change = (curr['Close'] - prev_c) / prev_c
    prev_bias = (prev_c - curr_ma) / curr_ma

    print(f"\n=== [0] 基礎攻擊條件檢查 ({target_dt.strftime('%Y-%m-%d')}) ===")

    b1 = check_status(pct_change >= cfg.get('trigger_min_gain', 0.10), "當週漲幅", f"{pct_change * 100:.2f}%",
                      f">= {cfg.get('trigger_min_gain', 0.10) * 100}%")
    b2 = check_status(curr['Close'] > curr['Open'], "K線型態", f"收({curr['Close']}) > 開({curr['Open']})", "必須是紅K")
    b3 = check_status(curr['Volume'] >= prev['Volume'] * cfg.get('trigger_vol_multiplier', 1.1), "當週量能",
                      f"{curr['Volume']:.0f}", f">= 上週量 * {cfg.get('trigger_vol_multiplier', 1.1)}")
    b4 = check_status(curr['Close'] > curr_ma, "收盤站上均線", f"收 {curr['Close']:.2f}",
                      f"> 當週 MA30 ({curr_ma:.2f})")

    basic_pass = b1 and b2 and b3 and b4
    base_fails = []
    if not b1: base_fails.append("漲幅未達標")
    if not b2: base_fails.append("非紅K")
    if not b3: base_fails.append("量能未達標")
    if not b4: base_fails.append("收盤未站上均線")

    print("\n=== [1] 30W 黏貼 (Adhesive) 條件檢查 ===")
    adh_fails = []

    a1 = check_status(curr_ma > p_ma_val, "均線方向", f"{curr_ma:.2f} > {p_ma_val:.2f}", "MA30 必須向上")
    if not a1: adh_fails.append("MA30未向上")

    a2 = check_status(prev_bias <= cfg.get('adhesive_bias', 0.12), "上週基位乖離", f"{prev_bias * 100:.2f}%",
                      f"<= {cfg.get('adhesive_bias', 0.12) * 100}%")
    if not a2: adh_fails.append("基位乖離率過高")

    a3 = False
    start_adh = i - cfg.get('adhesive_weeks', 2)
    if a1 and a2 and start_adh >= 0:
        max_d = 0.0
        is_adh_tmp = True
        print(
            f"  [掃描過去 {cfg.get('adhesive_weeks', 2)} 週偏離率 (規則: <= {cfg.get('adhesive_bias', 0.2) * 100}%)]:")
        for k in range(start_adh, i):
            dt_k = df.index[k].strftime('%m-%d')
            dev = max(abs(df['High'].iloc[k] - df['MA30'].iloc[k]), abs(df['Low'].iloc[k] - df['MA30'].iloc[k])) / \
                  df['MA30'].iloc[k]
            mark = "[O]" if dev <= cfg.get('adhesive_bias', 0.2) else "[X]"
            print(f"    {mark} {dt_k} 偏離: {dev * 100:.2f}%")
            if dev > cfg.get('adhesive_bias', 0.2):
                is_adh_tmp = False
            max_d = max(max_d, dev)
        a3 = is_adh_tmp
        if not a3: adh_fails.append(f"過去 {cfg.get('adhesive_weeks', 2)} 週內震幅偏離過大")
    elif start_adh < 0:
        adh_fails.append("歷史資料不足以檢查黏貼期")

    adh_pass = basic_pass and a1 and a2 and a3

    print("\n=== [2] 30W 甩轎 (Shakeout) 條件檢查 ===")
    shk_fails = []

    s1 = check_status(prev_bias <= cfg.get('shakeout_prev_bias_limit', 0.20), "上週基位乖離", f"{prev_bias * 100:.2f}%",
                      f"<= {cfg.get('shakeout_prev_bias_limit', 0.20) * 100}%")
    if not s1: shk_fails.append("基位乖離率過高")

    s2 = check_status(curr_ma >= p_ma_val * 0.999, "均線下彎容忍", f"{curr_ma:.2f}",
                      f">= 上週*0.999 ({p_ma_val * 0.999:.2f})")
    if not s2: shk_fails.append("MA30明顯下彎")

    s3 = check_status(prev_c >= p_ma_val, "起漲點", f"{prev_c:.2f}", f">= 上週 MA30 ({p_ma_val:.2f})")
    if not s3: shk_fails.append("上週收盤低於均線")

    s4, s6 = False, False
    start_shk = max(0, i - cfg.get('shakeout_lookback', 12))
    if s1 and s2 and s3:
        has_dip = False
        uw_weeks = 0

        print(f"  [掃描過去 {cfg.get('shakeout_lookback', 12)} 週跌破狀況 (不限深度)]:")
        for k in range(start_shk, i):
            l_val, m_val, c_val = df['Low'].iloc[k], df['MA30'].iloc[k], df['Close'].iloc[k]
            if l_val < m_val:
                has_dip = True
            if c_val < m_val:
                uw_weeks += 1

        s4 = check_status(has_dip, "曾跌破均線", "是" if has_dip else "否", "必須有跌破紀錄")
        if not s4: shk_fails.append("期間內未曾跌破 MA30")

        s6 = check_status(0 < uw_weeks <= cfg.get('shakeout_underwater_limit', 10), "收盤水下週數", f"{uw_weeks} 週",
                          f"1 ~ {cfg.get('shakeout_underwater_limit', 10)} 週")
        if not s6: shk_fails.append("水下週數不符規定")

    # 移除 s5 (深度合規) 判斷
    shk_pass = basic_pass and s1 and s2 and s3 and s4 and s6

    print(f"\n==================================================")
    print(f" 最終結論：{stock_id} 於 {target_dt.strftime('%Y-%m-%d')}")
    print(f"==================================================")

    adh_all_fails = base_fails + adh_fails
    if adh_pass:
        print("✅ 因為 1.基礎攻擊條件 2.黏貼條件 皆有符合，所以 30W 黏貼 有跳出。")
    else:
        print(f"❌ 因為 1.{', '.join(adh_all_fails)} 沒有符合，所以 30W 黏貼 沒有跳出。")

    print("-" * 50)

    shk_all_fails = base_fails + shk_fails
    if shk_pass:
        print("✅ 因為 1.基礎攻擊條件 2.甩轎條件 皆有符合，所以 30W 甩轎 有跳出。")
    else:
        print(f"❌ 因為 1.{', '.join(shk_all_fails)} 沒有符合，所以 30W 甩轎 沒有跳出。")


if __name__ == "__main__":
    debug_30w_interactive()