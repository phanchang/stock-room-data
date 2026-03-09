import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os
import math
import warnings
from datetime import datetime
import json
from unittest.mock import MagicMock

# ==========================================
# 🔧 1. 系統路徑強制修正
# ==========================================
sys.stdout.reconfigure(encoding='utf-8')
warnings.filterwarnings('ignore')

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# ==========================================
# 🛡️ 2. 防呆機制：Mock yfinance
# ==========================================
try:
    import yfinance
except ImportError:
    dummy_yf = MagicMock()
    sys.modules["yfinance"] = dummy_yf
    print("[Warning] 系統未安裝 yfinance，已啟用 Mock 模式以略過錯誤 (不影響台股計算)。")

try:
    from utils.cache.manager import CacheManager
    from utils.strategies.technical import TechnicalStrategies
except ImportError as e:
    print(f"[Error] 匯入 utils 模組失敗: {e}")
    sys.exit(1)


# ==========================================
# 核心邏輯 - 技術面 (維持不變)
# ==========================================
def calculate_advanced_factors(df, sid=None):
    if df is None or len(df) == 0:
        return None

    factors = {
        '現價': 0.0, '漲幅1d': 0.0, '漲幅5d': 0.0, '漲幅20d': 0.0, '漲幅60d': 0.0,
        'bb_width': 0.0, '量比': 0.0,
        'str_consol_5': 0, 'str_consol_10': 0, 'str_consol_20': 0, 'str_consol_60': 0,
        'str_ilss_sweep': 0, 'str_fake_breakdown': 0,
        'str_30w_adh': 0, 'str_30w_shk': 0, 'str_30w_info': "",
        'str_30w_week_offset': -1, 'str_st_week_offset': -1,
        'str_break_30w': 0, 'str_uptrend': 0, 'str_high_60': 0, 'str_high_30': 0,
        'str_ma55_sup': 0, 'str_ma200_sup': 0, 'str_vix_rev': 0
    }

    col_map = {k: v for k, v in
               {'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}.items() if
               k in df.columns}
    if col_map: df.rename(columns=col_map, inplace=True)

    if 'Close' not in df.columns: return None

    last_close_daily = df['Close'].iloc[-1]
    factors['現價'] = last_close_daily

    if len(df) >= 2:
        factors['漲幅1d'] = round(df['Close'].pct_change(1).iloc[-1] * 100, 2)
        if 'Volume' in df.columns:
            vol_mean = df['Volume'].tail(5).mean()
            factors['量比'] = round(df['Volume'].iloc[-1] / vol_mean, 2) if vol_mean > 0 else 0

    if len(df) >= 6: factors['漲幅5d'] = round(df['Close'].pct_change(5).iloc[-1] * 100, 2)

    if len(df) >= 21:
        factors['漲幅20d'] = round(df['Close'].pct_change(20).iloc[-1] * 100, 2)
        ma20 = df['Close'].rolling(20).mean()
        std20 = df['Close'].rolling(20).std()
        bb_width_series = (4 * std20) / ma20 * 100
        factors['bb_width'] = round(bb_width_series.iloc[-1], 2) if not pd.isna(bb_width_series.iloc[-1]) else 0

        factors['str_consol_5'] = int(bb_width_series.rolling(5).max().iloc[-1] < 10) if len(
            bb_width_series) >= 5 else 0
        factors['str_consol_10'] = int(bb_width_series.rolling(10).max().iloc[-1] < 12) if len(
            bb_width_series) >= 10 else 0
        factors['str_consol_20'] = int(bb_width_series.rolling(20).max().iloc[-1] < 15) if len(
            bb_width_series) >= 20 else 0

        try:
            if (df['Close'].iloc[-2] < ma20.iloc[-2] and df['Close'].iloc[-1] > ma20.iloc[-1] and df['Close'].iloc[-1] >
                    df['Open'].iloc[-1]):
                factors['str_fake_breakdown'] = 1
        except:
            pass

    if len(df) >= 61:
        factors['漲幅60d'] = round(df['Close'].pct_change(60).iloc[-1] * 100, 2)
        factors['str_consol_60'] = int(bb_width_series.rolling(60).max().iloc[-1] < 18) if len(
            bb_width_series) >= 60 else 0

    if len(df) >= 200:
        def check_recent(series):
            return int(series.tail(3).any())

        try:
            ma20 = df['Close'].rolling(20).mean()
            ma200 = df['Close'].rolling(200).mean()
            high_60 = df['High'].rolling(60).max()
            if (last_close_daily > ma200.iloc[-1]) and (ma200.iloc[-1] > ma200.iloc[-5]) and (
                    df['High'].tail(15) >= high_60.tail(15)).any():
                low_20d = df['Low'].rolling(20).min().shift(1)
                for i in range(3):
                    idx = -1 - i
                    s_level = min(low_20d.iloc[idx], ma20.iloc[idx]) if idx > -len(df) else 0
                    if s_level == 0: continue
                    break_depth = (s_level - df['Low'].iloc[idx]) / s_level
                    if (df['Low'].iloc[idx] < s_level) and (0.005 < break_depth < 0.08) and (
                            df['Volume'].iloc[idx] > (1.2 * df['Volume'].iloc[idx - 5:idx].mean())):
                        if (last_close_daily > s_level) and (last_close_daily > df['Open'].iloc[-1]) and (
                                last_close_daily > df['High'].iloc[idx]):
                            factors['str_ilss_sweep'] = 1
                            break
        except:
            pass

        factors['str_break_30w'] = check_recent(TechnicalStrategies.break_30w_ma(df))
        factors['str_uptrend'] = int(TechnicalStrategies.strong_uptrend(df).iloc[-1])
        factors['str_high_60'] = check_recent(TechnicalStrategies.breakout_n_days_high(df, 60))
        factors['str_high_30'] = check_recent(TechnicalStrategies.breakout_n_days_high(df, 30))
        factors['str_ma55_sup'] = check_recent(TechnicalStrategies.near_ma_support(df, 55))
        factors['str_ma200_sup'] = check_recent(TechnicalStrategies.near_ma_support(df, 200))
        factors['str_vix_rev'] = check_recent(TechnicalStrategies.vix_reversal(df))

        try:
            logic = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
            df_weekly = df.resample('W-FRI').agg(logic).dropna()
            if len(df_weekly) >= 10:
                try:
                    st_weekly = TechnicalStrategies.calculate_supertrend(df_weekly)
                    found_st_week = -1
                    max_idx = min(26, len(st_weekly) - 1)
                    for offset in range(max_idx + 1):
                        if st_weekly['Signal'].iloc[-1 - offset] == 1:
                            found_st_week = offset
                            break
                    factors['str_st_week_offset'] = found_st_week
                except:
                    pass

            if len(df_weekly) >= 35:
                res_30w = TechnicalStrategies.analyze_30w_breakout_details(df_weekly)
                max_back = min(52, len(res_30w) - 1)
                for offset in range(max_back + 1):
                    idx = -1 - offset
                    sig = res_30w['Signal'].iloc[idx]
                    if sig > 0:
                        factors['str_30w_week_offset'] = offset
                        factors['str_30w_adh'] = 1 if sig in [1, 3] else 0
                        factors['str_30w_shk'] = 1 if sig in [2, 3] else 0
                        factors[
                            'str_30w_info'] = f"({res_30w['Adh_Info'].iloc[idx] if sig in [1, 3] else res_30w['Shk_Info'].iloc[idx]})"
                        break
        except:
            pass
    return factors


# ==========================================
# 深度 JSON 特徵轉換 (財報深度、籌碼比例、營收)
# ==========================================
def calculate_json_factors(row):
    sid_str = str(row['sid']).strip()
    json_path = project_root / 'data' / 'fundamentals' / f"{sid_str}.json"

    # 初始化預設空值 (使用語義化前綴分組，名稱對齊 strategy_module)
    res = {
        # 【營收群組】
        'rev_ym': '', 'rev_yoy': 0.0, 'rev_cum_yoy': 0.0,
        # 【EPS 獲利群組 - 修正名稱以對齊介面】
        'fund_eps_year': '', 'fund_eps_cum': 0.0,
        'eps_latest_q': '', 'eps_latest_val': 0.0,
        'eps_qoq': 0.0, 'eps_yoy': 0.0,
        'eps_cum_yoy': 0.0,
        # 【籌碼群組 (比例)】
        'margin_diff_5d': 0.0, 'legal_diff_5d': 0.0,
        'margin_diff_20d': 0.0, 'legal_diff_20d': 0.0,
        # 【資產負債群組】
        'fund_contract_qoq': 0.0, 'fund_inventory_qoq': 0.0, 'fund_op_cash_flow': 0.0
    }

    if not json_path.exists(): return pd.Series(res)

    try:
        with open(json_path, 'r', encoding='utf-8') as jf:
            jdata = json.load(jf)
    except Exception:
        return pd.Series(res)

    # 1. 營收 (Revenue) - 🔥 修正 Key 讀取錯誤 (yoy -> rev_yoy)
    rev_data = jdata.get('revenue', [])
    if len(rev_data) > 0:
        latest_rev = rev_data[0]
        res['rev_ym'] = latest_rev.get('month', '')
        # 修正：JSON 中的 Key 是 rev_yoy 不是 yoy
        res['rev_yoy'] = float(latest_rev.get('rev_yoy', 0) or 0)
        # 修正：JSON 中的 Key 是 rev_cum_yoy 不是 cum_yoy
        res['rev_cum_yoy'] = float(latest_rev.get('rev_cum_yoy', 0) or 0)

    # 2. 資產負債表 (合約負債與庫存)
    bs = jdata.get('balance_sheet', [])
    if len(bs) >= 2:
        try:
            cl_0, cl_1 = float(bs[0].get('contract_liab', 0) or 0), float(bs[1].get('contract_liab', 0) or 0)
            if cl_1 > 0: res['fund_contract_qoq'] = round(((cl_0 - cl_1) / cl_1) * 100, 2)

            inv_0, inv_1 = float(bs[0].get('inventory', 0) or 0), float(bs[1].get('inventory', 0) or 0)
            if inv_1 > 0: res['fund_inventory_qoq'] = round(((inv_0 - inv_1) / inv_1) * 100, 2)
        except:
            pass

    # 3. 現金流
    cf = jdata.get('cash_flow', [])
    if len(cf) > 0:
        res['fund_op_cash_flow'] = float(cf[0].get('op_cash_flow', 0) or 0)

    # 4. 籌碼集中度 (比例) - 修正欄位名稱 (chip_ -> 無前綴或對齊)
    margin_data = jdata.get('margin_trading', [])
    inst_data = jdata.get('institutional_investors', [])

    if len(margin_data) >= 5 and len(inst_data) >= 5:
        try:
            res['margin_diff_5d'] = round(
                float(margin_data[0].get('fin_usage', 0)) - float(margin_data[4].get('fin_usage', 0)), 2)
            res['legal_diff_5d'] = round(
                float(inst_data[0].get('total_legal_pct', 0)) - float(inst_data[4].get('total_legal_pct', 0)), 2)
        except:
            pass

    if len(margin_data) >= 20 and len(inst_data) >= 20:
        try:
            res['margin_diff_20d'] = round(
                float(margin_data[0].get('fin_usage', 0)) - float(margin_data[19].get('fin_usage', 0)), 2)
            res['legal_diff_20d'] = round(
                float(inst_data[0].get('total_legal_pct', 0)) - float(inst_data[19].get('total_legal_pct', 0)), 2)
        except:
            pass

    # 5. EPS 深度解析 (QoQ, YoY, 累計YoY)
    prof = jdata.get('profitability', [])
    if len(prof) > 0:
        try:
            latest_q_str = prof[0].get('quarter', '')  # e.g., '114.3Q'
            if '.' in latest_q_str:
                res['eps_latest_q'] = latest_q_str
                res['eps_latest_val'] = float(prof[0].get('eps', 0))

                # 解析最新年份與季度 -> 轉為 fund_eps_year
                # 🔥 修正：使用者希望此欄位顯示 "114.3Q" 而非只有年，因此保留完整字串
                res['fund_eps_year'] = latest_q_str

                latest_y, latest_q = latest_q_str.replace('Q', '').split('.')
                latest_y, latest_q = int(latest_y), int(latest_q)

                # 尋找計算目標的標籤
                prev_q_str = f"{latest_y}.{latest_q - 1}Q" if latest_q > 1 else f"{latest_y - 1}.4Q"
                last_y_q_str = f"{latest_y - 1}.{latest_q}Q"

                sum_this_year = 0.0
                sum_last_year = 0.0
                eps_prev_q = None
                eps_last_y_q = None

                for p in prof:
                    q_str = p.get('quarter', '')
                    val = float(p.get('eps', 0) or 0)

                    # 抓取單季供 QoQ / YoY 使用
                    if q_str == prev_q_str: eps_prev_q = val
                    if q_str == last_y_q_str: eps_last_y_q = val

                    # 累計 EPS 邏輯 (今年 vs 去年同期)
                    try:
                        y, q = q_str.replace('Q', '').split('.')
                        y, q = int(y), int(q)
                        if y == latest_y and q <= latest_q:
                            sum_this_year += val
                        elif y == latest_y - 1 and q <= latest_q:
                            sum_last_year += val
                    except:
                        pass

                res['fund_eps_cum'] = round(sum_this_year, 2)  # 🔥 修正 Key

                # 計算單季 QoQ
                if eps_prev_q is not None and eps_prev_q != 0:
                    res['eps_qoq'] = round(((res['eps_latest_val'] - eps_prev_q) / abs(eps_prev_q)) * 100, 2)

                # 計算單季 YoY
                if eps_last_y_q is not None and eps_last_y_q != 0:
                    res['eps_yoy'] = round(((res['eps_latest_val'] - eps_last_y_q) / abs(eps_last_y_q)) * 100, 2)

                # 計算累計 YoY
                if sum_last_year != 0:
                    res['eps_cum_yoy'] = round(((sum_this_year - sum_last_year) / abs(sum_last_year)) * 100, 2)
        except:
            pass

    return pd.Series(res)


def main():
    print(f"[System] 因子運算啟動 (V6.5 - EPS顯示格式與營收修復版) | {datetime.now():%H:%M:%S}")

    try:
        cache = CacheManager()
    except Exception as e:
        print(f"[Error] CacheManager 初始化失敗: {e}")
        return

    raw_path = project_root / 'data' / 'temp' / 'chips_revenue_raw.csv'
    if not raw_path.exists():
        print("[Error] 找不到 chips_revenue_raw.csv，請先執行廣度籌碼更新")
        return

    raw_df = pd.read_csv(raw_path, dtype={'sid': str})

    # 🌟 防呆：清理 raw_df 中的舊營收與 EPS 欄位，避免干擾 JSON 深度數據
    drop_cols = ['rev_ym', 'rev_yoy', 'rev_cum_yoy', 'eps_q', 'eps_date', 'eps_cum']
    raw_df = raw_df.drop(columns=[c for c in drop_cols if c in raw_df.columns])

    tech_list = []
    target_sids = raw_df['sid'].unique().tolist()
    total = len(target_sids)
    print(f"📊 預計計算股票數量: {total}")

    for i, sid in enumerate(target_sids):
        sid = str(sid)
        if i % 20 == 0 or i == total - 1:
            pct = int((i + 1) / total * 100)
            print(f"PROGRESS: {pct}")
            print(f"   Processing: {i}/{total} ({sid})...", end='\r')
            sys.stdout.flush()

        try:
            df = cache.load(f"{sid}.TW")
            if df is None or df.empty: df = cache.load(f"{sid}.TWO")

            if df is not None and not df.empty:
                factors = calculate_advanced_factors(df, sid=sid)
                if factors:
                    factors['sid'] = sid
                    tech_list.append(factors)
        except Exception:
            continue

    print(f"\n[System] 技術指標計算完成，共 {len(tech_list)} 檔成功。")
    print("PROGRESS: 100")

    if not tech_list:
        print("[Warning] 無技術指標產出，請檢查 K 線資料是否完整")
        return

    tech_df = pd.DataFrame(tech_list).set_index('sid')
    final_df = raw_df.merge(tech_df, on='sid', how='left')

    if '漲幅20d' in final_df.columns:
        final_df['RS強度'] = final_df['漲幅20d'].rank(pct=True) * 100
        final_df['RS強度'] = final_df['RS強度'].round(1)

    def get_strong_tags(row):
        tags = []

        # --- 1. 趨勢與突破 ---
        st_week = row.get('str_st_week_offset', -1)
        if st_week == 0:
            tags.append('ST轉多(本週)')
        elif 0 < st_week <= 4:
            tags.append(f'ST轉多({int(st_week)}週前)')

        if row.get('str_break_30w', 0) == 1: tags.append('突破30週')
        if row.get('str_high_60', 0) == 1: tags.append('創季高')
        if row.get('str_high_30', 0) == 1: tags.append('創月高')
        if row.get('str_uptrend', 0) == 1: tags.append('強勢多頭')
        if row.get('漲幅60d', 0) > 30: tags.append('波段黑馬')
        if row.get('RS強度', 0) > 90: tags.append('超強勢')

        # --- 2. 壓縮與整理 ---
        bb_width = row.get('bb_width', 100)
        if bb_width < 5.0:
            tags.append('極度壓縮')
        elif bb_width < 8.0:
            tags.append('波動壓縮')

        if row.get('str_consol_5', 0) == 1: tags.append('盤整5日')
        if row.get('str_consol_10', 0) == 1: tags.append('盤整10日')
        if row.get('str_consol_20', 0) == 1: tags.append('盤整20日')
        if row.get('str_consol_60', 0) == 1: tags.append('盤整60日')

        # --- 3. 籌碼與主力 ---
        if row.get('str_ilss_sweep', 0) == 1 and row.get('rev_cum_yoy', 0) > 0 and (
                row.get('m_net_today', 0) < 0 or row.get('m_sum_5d', 0) < 0):
            tags.append('主力掃單(ILSS)')
        if row.get('t_streak', 0) >= 3: tags.append('投信認養')
        if row.get('m_net_today', 0) <= -200: tags.append('散戶退場')  # 根據 UI Tooltip：單日大減200張
        if row.get('is_tu_yang', 0) == 1: tags.append('土洋對作')

        # 隱藏擴充：籌碼過濾 法人波段吸籌 + 短線點火
        if row.get('legal_diff_20d', 0) > 1.5 and row.get('t_net_today', 0) > 0: tags.append('波段吸籌發動')

        # --- 4. 特殊型態 (修復遺失的標籤) ---
        offset = row.get('str_30w_week_offset', -1)
        suffix = "(本週)" if offset == 0 else (f"({int(offset)}週前)" if offset > 0 else "")

        if row.get('str_30w_adh', 0) == 1: tags.append(f"30W黏貼後突破{suffix}")  # 修正字眼對齊UI
        if row.get('str_30w_shk', 0) == 1: tags.append(f"30W甩轎{suffix}")

        if row.get('str_fake_breakdown', 0) == 1: tags.append('假跌破')
        if row.get('str_ma55_sup', 0) == 1: tags.append('回測季線')  # 🌟 修復
        if row.get('str_ma200_sup', 0) == 1: tags.append('回測年線')  # 🌟 修復
        if row.get('str_vix_rev', 0) == 1: tags.append('Vix反轉')  # 🌟 修復

        return ','.join(tags)

    print("[System] 正在進行深度 JSON 特徵轉換 (寫入 Parquet 數值)...")
    json_factors_df = final_df.apply(calculate_json_factors, axis=1)

    # 將 final_df 設為 sid index 方便 concat
    final_df.set_index('sid', inplace=True)
    json_factors_df.index = final_df.index
    final_df = pd.concat([final_df, json_factors_df], axis=1).reset_index()

    final_df['強勢特徵'] = final_df.apply(get_strong_tags, axis=1)

    # ==========================================
    # 🌍 中英文欄位映射 (結構化對齊)
    # ==========================================
    chinese_map = {
        'sid': '股票代號', 'name': '股票名稱', 'industry': '產業別',
        # 【營收群組】
        'rev_ym': '最新營收月',
        'rev_yoy': '營收YoY(%)',
        'rev_cum_yoy': '累計營收YoY(%)',
        # 【EPS 獲利群組】
        'fund_eps_year': 'EPS年度',
        'fund_eps_cum': '最新EPS(累)',
        'eps_latest_q': '最新EPS季',
        'eps_latest_val': '單季EPS(元)',
        'eps_qoq': 'EPS季增率_QoQ(%)',
        'eps_yoy': 'EPS年增率_YoY(%)',
        'eps_cum_yoy': '累計EPS_YoY(%)',
        # 【籌碼比例群組 (波段)】
        'legal_diff_5d': '法人5日增減(%)',
        'margin_diff_5d': '融資5日增減(%)',
        'legal_diff_20d': '法人20日增減(%)',
        'margin_diff_20d': '融資20日增減(%)',
        # 【籌碼張數群組 (短線動能)】
        't_net_today': '投信買賣超(今)', 't_sum_5d': '投信買賣超(5日)', 't_streak': '投信連買天數',
        'f_net_today': '外資買賣超(今)', 'f_sum_5d': '外資買賣超(5日)', 'f_streak': '外資連買天數',
        'm_net_today': '融資增減(今)', 'm_sum_5d': '融資增減(5日)',
        # 【資產負債與估值】
        'fund_contract_qoq': '合約負債季增(%)',
        'fund_inventory_qoq': '庫存季增(%)',
        'fund_op_cash_flow': '最新營業現金流',
        'pe': '本益比', 'yield': '殖利率(%)',
        # 【技術面群組】
        '現價': '今日收盤價', '漲幅1d': '今日漲幅(%)', '漲幅20d': '20日漲幅(%)', '漲幅60d': '3個月漲幅(%)',
        'bb_width': '布林寬度(%)', '量比': '成交量比', 'RS強度': 'RS強度', '強勢特徵': '強勢特徵標籤',
        'str_30w_week_offset': '30W起漲週數(前)', 'str_st_week_offset': 'ST買訊(週)'
    }

    # 只保留 map 裡面有定義的欄位，確保報表乾淨
    final_cols = [c for c in final_df.columns if c in chinese_map]
    output_df = final_df[final_cols].rename(columns=chinese_map)

    strategy_dir = project_root / 'data' / 'strategy_results'
    strategy_dir.mkdir(parents=True, exist_ok=True)
    final_df.to_parquet(strategy_dir / 'factor_snapshot.parquet')
    output_df.to_csv(strategy_dir / '戰情室今日快照_全中文版.csv', encoding='utf-8-sig', index=False)
    print(f"[System] 存檔完成: {strategy_dir}")


if __name__ == "__main__":
    main()