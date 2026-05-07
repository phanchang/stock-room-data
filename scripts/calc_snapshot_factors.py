# 檔案路徑: scripts/calc_snapshot_factors.py
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import argparse
import os
import warnings
from datetime import datetime
import json
from unittest.mock import MagicMock
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8')
warnings.filterwarnings('ignore')

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

load_dotenv(project_root / '.env')

try:
    import yfinance
except ImportError:
    dummy_yf = MagicMock()
    sys.modules["yfinance"] = dummy_yf

try:
    from utils.cache.manager import CacheManager
    from utils.strategies.technical import TechnicalStrategies
except ImportError as e:
    print(f"[Error] 匯入 utils 模組失敗: {e}")
    sys.exit(1)

# ==========================================
# 核心邏輯 - 技術面 (維持你原本的完美邏輯，無任何刪減)
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
        'str_ma55_sup': 0, 'str_ma200_sup': 0, 'str_vix_rev': 0,
        'str_30w_standby': 0,
        '今日成交股數': 0.0
    }

    # 🔥 大表運算分離原則：優先使用還原報價進行技術分析與漲跌幅計算
    if 'adj_close' in df.columns:
        col_map = {
            'adj_open': 'Open',
            'adj_high': 'High',
            'adj_low': 'Low',
            'adj_close': 'Close',
            'volume': 'Volume',
            'close': 'Raw_Close'  # 保留原始收盤價供現價使用
        }
    else:
        # 防呆相容舊快取
        col_map = {
            'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'
        }

    # 只對應存在的欄位並重命名
    col_map = {k: v for k, v in col_map.items() if k in df.columns}
    if col_map:
        df.rename(columns=col_map, inplace=True)

    if 'Close' not in df.columns: return None

    # 防呆機制：若無原始收盤價欄位，用還原價頂替以防當機
    if 'Raw_Close' not in df.columns:
        df['Raw_Close'] = df['Close']

    # 1️⃣ 真實市場資訊：使用「未還原」的原始股價
    last_raw_close = df['Raw_Close'].iloc[-1]
    factors['現價'] = last_raw_close

    # 2️⃣ 技術指標基底：使用「已還原」的股價
    adj_last_close = df['Close'].iloc[-1]

    if len(df) >= 2:
        # 漲幅改用還原價格運算
        factors['漲幅1d'] = round(df['Close'].pct_change(1).iloc[-1] * 100, 2)
        if 'Volume' in df.columns:
            vol_mean = df['Volume'].tail(5).mean()
            factors['量比'] = round(df['Volume'].iloc[-1] / vol_mean, 2) if vol_mean > 0 else 0
            factors['今日成交股數'] = float(df['Volume'].iloc[-1])

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

            # 🔥 關鍵修正：ILSS主力掃單邏輯必須統一使用「還原現價(adj_last_close)」與還原均線對比
            if (adj_last_close > ma200.iloc[-1]) and (ma200.iloc[-1] > ma200.iloc[-5]) and (
                    df['High'].tail(15) >= high_60.tail(15)).any():
                low_20d = df['Low'].rolling(20).min().shift(1)
                for i in range(3):
                    idx = -1 - i
                    s_level = min(low_20d.iloc[idx], ma20.iloc[idx]) if idx > -len(df) else 0
                    if s_level == 0: continue
                    break_depth = (s_level - df['Low'].iloc[idx]) / s_level
                    if (df['Low'].iloc[idx] < s_level) and (0.005 < break_depth < 0.08) and (
                            df['Volume'].iloc[idx] > (1.2 * df['Volume'].iloc[idx - 5:idx].mean())):
                        if (adj_last_close > s_level) and (adj_last_close > df['Open'].iloc[-1]) and (
                                adj_last_close > df['High'].iloc[idx]):
                            factors['str_ilss_sweep'] = 1
                            break
        except:
            pass

        # === 呼叫外部 Technical 策略群 ===
        # (由於 df 的 Open/High/Low/Close 已被替換為還原股價，策略皆自動平滑過渡)
        factors['str_break_30w'] = check_recent(TechnicalStrategies.break_30w_ma(df))
        factors['str_uptrend'] = int(TechnicalStrategies.strong_uptrend(df).iloc[-1])
        factors['str_high_60'] = check_recent(TechnicalStrategies.breakout_n_days_high(df, 60))
        factors['str_high_30'] = check_recent(TechnicalStrategies.breakout_n_days_high(df, 30))
        factors['str_ma55_sup'] = check_recent(TechnicalStrategies.near_ma_support(df, 55))
        factors['str_ma200_sup'] = check_recent(TechnicalStrategies.near_ma_support(df, 200))
        factors['str_vix_rev'] = check_recent(TechnicalStrategies.vix_reversal(df))

        # 檔案路徑: scripts/calc_snapshot_factors.py
        # ... (其他程式碼不變) ...

        try:
            logic = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
            # 1. 產生標準的「日曆週」資料 (確保歷史均線與 expanded_kline.py 完全一致)
            df_weekly = df.resample('W-FRI').agg(logic).dropna()

            # =========================================================
            # 🚀 終極解決方案：動態不完整週校準 (僅校正量能，不破壞歷史收盤)
            # =========================================================
            df_weekly_live = df_weekly.copy()
            if len(df) >= 5 and len(df_weekly_live) >= 1:
                last_idx = df_weekly_live.index[-1]
                last_5d_vol = df['Volume'].tail(5).sum()
                if df_weekly_live.at[last_idx, 'Volume'] < last_5d_vol:
                    df_weekly_live.at[last_idx, 'Volume'] = last_5d_vol

            # -------------------- 訊號判定區 --------------------
            if len(df_weekly) >= 10:
                # (下方維持原有的訊號計算邏輯不變 ...)
                # 歷史訊號用日曆週，確保圖表一致性
                st_weekly_hist = TechnicalStrategies.calculate_supertrend(df_weekly)
                # 當前訊號用滾動週，確保星期一的敏銳度
                st_weekly_live = TechnicalStrategies.calculate_supertrend(df_weekly_live)

                found_st_week = -1
                for offset in range(min(26, len(st_weekly_hist) - 1) + 1):
                    # 只有本週(offset=0)採用 live 數據，歷史均採用 hist 數據
                    sig = st_weekly_live['Signal'].iloc[-1] if offset == 0 else st_weekly_hist['Signal'].iloc[
                        -1 - offset]
                    if sig == 1:
                        found_st_week = offset
                        break
                factors['str_st_week_offset'] = found_st_week

            if len(df_weekly) >= 35:
                # 歷史與滾動雙軌運算
                res_30w_hist = TechnicalStrategies.analyze_30w_breakout_details(df_weekly)
                res_30w_live = TechnicalStrategies.analyze_30w_breakout_details(df_weekly_live)

                for offset in range(min(52, len(res_30w_hist) - 1) + 1):
                    idx = -1 - offset

                    # 只有本週(offset=0)採用 live 數據，歷史均採用 hist 數據
                    # 注意：這裡使用 df_weekly_live 取得的訊號，其索引應與原始 df_weekly 對齊
                    # 如果 offset=0 且 df_weekly_live 的最後一筆是本週的資料，就用它
                    # 否則就用 df_weekly_hist 的歷史資料
                    if offset == 0 and not df_weekly_live.empty and df_weekly_live.index[-1] == df_weekly.index[-1]:
                        sig = res_30w_live['Signal'].iloc[-1]
                        adh = res_30w_live['Adh_Info'].iloc[-1]
                        shk = res_30w_live['Shk_Info'].iloc[-1]
                    else:
                        sig = res_30w_hist['Signal'].iloc[idx]
                        adh = res_30w_hist['Adh_Info'].iloc[idx]
                        shk = res_30w_hist['Shk_Info'].iloc[idx]

                    if sig > 0:
                        factors['str_30w_week_offset'] = offset
                        factors['str_30w_adh'] = 1 if sig in [1, 3] else 0
                        factors['str_30w_shk'] = 1 if sig in [2, 3] else 0
                        factors['str_30w_info'] = f"({adh if sig in [1, 3] else shk})"
                        break

                # 聽牌 (Standby) 同樣使用 live 版本，讓星期一/二的聽牌判定更精準
                try:
                    standby_res = TechnicalStrategies.check_30w_standby(df_weekly_live)
                    factors['str_30w_standby'] = int(standby_res.iloc[-1])
                except Exception:
                    pass
        except Exception as e:
            # 捕獲所有異常，避免單一股票的錯誤導致整個排程中斷
            # print(f"⚠️ 股票 {sid} 30W週線因子計算失敗: {e}") # Debug用，正式部署可移除
            pass

        # 👇 插入此段測試日誌
        if sid in ['2330']:
            print(f"\n[驗證] 代號: {sid} | 原始收盤價: {last_raw_close:.2f} | 還原收盤價: {adj_last_close:.2f}")
            print(f"      漲幅 -> 1d: {factors['漲幅1d']}%, 5d: {factors['漲幅5d']}%, 20d: {factors['漲幅20d']}%")
            print(f"      指標 -> 布林寬度: {factors['bb_width']}%, ST買訊(週前): {factors['str_st_week_offset']}")
        # 👆 插入到這裡結束

    return factors

# ==========================================
# 深度 JSON 特徵轉換 (維持你原本的邏輯)
# ==========================================
def calculate_json_factors(sid_str):
    json_path = project_root / 'data' / 'fundamentals' / f"{sid_str}.json"

    res = {
        'rev_ym': '', 'rev_yoy': 0.0, 'rev_cum_yoy': 0.0,
        'fund_eps_year': '', 'fund_eps_cum': 0.0, 'eps_latest_q': '', 'eps_latest_val': 0.0,
        'eps_qoq': 0.0, 'eps_yoy': 0.0, 'eps_cum_yoy': 0.0,
        'margin_diff_5d': 0.0, 'legal_diff_5d': 0.0,
        'margin_diff_20d': 0.0, 'legal_diff_20d': 0.0,
        'f_diff_5d': 0.0, 'f_diff_10d': 0.0, 'f_diff_20d': 0.0, 'f_diff_60d': 0.0, 'f_diff_120d': 0.0,
        't_diff_5d': 0.0, 't_diff_10d': 0.0, 't_diff_20d': 0.0, 't_diff_60d': 0.0, 't_diff_120d': 0.0,
        't_net_today': 0.0, 't_sum_5d': 0.0, 't_sum_10d': 0.0, 't_sum_20d': 0.0, 't_streak': 0, 't_sell_days_10': 0,
        't_buy_days_5': 0,
        'f_net_today': 0.0, 'f_sum_5d': 0.0, 'f_sum_10d': 0.0, 'f_sum_20d': 0.0, 'f_streak': 0, 'f_buy_days_10': 0,
        'f_sum_3d': 0.0,
        'm_net_today': 0.0, 'm_sum_5d': 0.0, 'm_sum_10d': 0.0, 'm_sum_20d': 0.0, 'is_tu_yang': 0,
        'fund_contract_qoq': 0.0, 'fund_inventory_qoq': 0.0, 'fund_op_cash_flow': 0.0,
        'issued_shares': 0.0, 'invest_trust_hold_pct': 0.0, 'foreign_hold_pct': 0.0
    }

    if not json_path.exists(): return res

    try:
        with open(json_path, 'r', encoding='utf-8') as jf:
            jdata = json.load(jf)
    except Exception:
        return res

    # 1. 營收
    rev_data = jdata.get('revenue', [])
    res['rev_highest_months'] = 0
    res['rev_consecutive_highs'] = 0

    if len(rev_data) > 0:
        res['rev_ym'] = rev_data[0].get('month', '')
        res['rev_yoy'] = float(rev_data[0].get('rev_yoy', 0) or 0)
        res['rev_cum_yoy'] = float(rev_data[0].get('rev_cum_yoy', 0) or 0)

        # 🚀 新增：營收創高運算邏輯
        try:
            # 取出所有月份的營收數值 (最新到最舊)
            rev_vals = [float(r.get('revenue', 0) or 0) for r in rev_data]
            n_months = len(rev_vals)

            if n_months >= 2:
                latest_rev = rev_vals[0]

                # A. 運算「創多少個月來新高」
                highest_m = 0
                for i in range(1, n_months):
                    if latest_rev > rev_vals[i]:
                        highest_m = i
                    else:
                        break  # 碰到比自己高的就停止
                res['rev_highest_months'] = highest_m

                # B. 運算「連續幾個月創歷史(資料區間)新高」
                consecutive = 0
                for i in range(n_months - 1):
                    current_month_rev = rev_vals[i]
                    # 如果當月營收 大於 過去所有月份的最大值
                    past_max = max(rev_vals[i + 1:]) if (i + 1) < n_months else 0
                    if current_month_rev > past_max and current_month_rev > 0:
                        consecutive += 1
                    else:
                        break
                res['rev_consecutive_highs'] = consecutive
        except Exception as e:
            pass

    # 2. 資產負債與現金流
    bs = jdata.get('balance_sheet', [])
    if len(bs) >= 2:
        try:
            cl_0, cl_1 = float(bs[0].get('contract_liab', 0) or 0), float(bs[1].get('contract_liab', 0) or 0)
            if cl_1 > 0: res['fund_contract_qoq'] = round(((cl_0 - cl_1) / cl_1) * 100, 2)
            inv_0, inv_1 = float(bs[0].get('inventory', 0) or 0), float(bs[1].get('inventory', 0) or 0)
            if inv_1 > 0: res['fund_inventory_qoq'] = round(((inv_0 - inv_1) / inv_1) * 100, 2)
        except: pass
    cf = jdata.get('cash_flow', [])
    if len(cf) > 0: res['fund_op_cash_flow'] = float(cf[0].get('op_cash_flow', 0) or 0)

    # 3. EPS
    prof = jdata.get('profitability', [])
    if len(prof) > 0:
        try:
            latest_q_str = prof[0].get('quarter', '')
            if '.' in latest_q_str:
                res['eps_latest_q'] = latest_q_str
                res['eps_latest_val'] = float(prof[0].get('eps', 0))
                res['fund_eps_year'] = latest_q_str
                latest_y, latest_q = int(latest_q_str.split('.')[0]), int(latest_q_str.split('.')[1].replace('Q', ''))
                prev_q_str = f"{latest_y}.{latest_q - 1}Q" if latest_q > 1 else f"{latest_y - 1}.4Q"
                last_y_q_str = f"{latest_y - 1}.{latest_q}Q"
                sum_this, sum_last, eps_prev, eps_last_y = 0.0, 0.0, None, None

                for p in prof:
                    q_str, val = p.get('quarter', ''), float(p.get('eps', 0) or 0)
                    if q_str == prev_q_str: eps_prev = val
                    if q_str == last_y_q_str: eps_last_y = val
                    try:
                        y, q = int(q_str.split('.')[0]), int(q_str.split('.')[1].replace('Q', ''))
                        if y == latest_y and q <= latest_q: sum_this += val
                        elif y == latest_y - 1 and q <= latest_q: sum_last += val
                    except: pass

                res['fund_eps_cum'] = round(sum_this, 2)
                if eps_prev is not None and eps_prev != 0: res['eps_qoq'] = round(((res['eps_latest_val'] - eps_prev) / abs(eps_prev)) * 100, 2)
                if eps_last_y is not None and eps_last_y != 0: res['eps_yoy'] = round(((res['eps_latest_val'] - eps_last_y) / abs(eps_last_y)) * 100, 2)
                if sum_last != 0: res['eps_cum_yoy'] = round(((sum_this - sum_last) / abs(sum_last)) * 100, 2)
        except: pass

    # 4. 籌碼運算
    inst_data = jdata.get('institutional_investors', [])
    margin_data = jdata.get('margin_trading', [])

    if inst_data:
        market_dates = [d['date'] for d in inst_data]
        df_inst = pd.DataFrame(inst_data).set_index('date')
        df_margin = pd.DataFrame(margin_data).set_index('date') if margin_data else pd.DataFrame()

        d_1, d_3, d_5, d_10, d_20 = market_dates[:1], market_dates[:3], market_dates[:5], market_dates[:10], market_dates[:20]

        def get_sum(df, dates, col): return df.reindex(dates)[col].fillna(0).sum() if not df.empty and col in df.columns else 0.0
        def get_streak(df, col):
            if df.empty or col not in df.columns: return 0
            series = df.reindex(market_dates)[col].fillna(0).values
            if len(series) == 0 or series[0] == 0: return 0
            count, is_buying = 0, (series[0] > 0)
            for v in series:
                if is_buying and v > 0: count += 1
                elif not is_buying and v < 0: count -= 1
                else: break
            return count

        res['t_net_today'] = get_sum(df_inst, d_1, 'invest_trust_buy_sell')
        res['f_net_today'] = get_sum(df_inst, d_1, 'foreign_buy_sell')
        res['m_net_today'] = get_sum(df_margin, d_1, 'fin_change')

        res['t_sum_5d'], res['t_sum_10d'], res['t_sum_20d'] = get_sum(df_inst, d_5, 'invest_trust_buy_sell'), get_sum(df_inst, d_10, 'invest_trust_buy_sell'), get_sum(df_inst, d_20, 'invest_trust_buy_sell')
        res['f_sum_5d'], res['f_sum_10d'], res['f_sum_20d'] = get_sum(df_inst, d_5, 'foreign_buy_sell'), get_sum(df_inst, d_10, 'foreign_buy_sell'), get_sum(df_inst, d_20, 'foreign_buy_sell')
        res['m_sum_5d'], res['m_sum_10d'], res['m_sum_20d'] = get_sum(df_margin, d_5, 'fin_change'), get_sum(df_margin, d_10, 'fin_change'), get_sum(df_margin, d_20, 'fin_change')
        res['f_sum_3d'] = get_sum(df_inst, d_3, 'foreign_buy_sell')

        res['t_streak'], res['f_streak'] = get_streak(df_inst, 'invest_trust_buy_sell'), get_streak(df_inst, 'foreign_buy_sell')
        # 取最新一日的發行張數與持股比例
        if not df_inst.empty:
            iss = float(df_inst.iloc[0].get('issued_shares', 0) or 0)
            t_hold = float(df_inst.iloc[0].get('invest_trust_hold', 0) or 0)
            res['issued_shares'] = iss
            res['invest_trust_hold_pct'] = round((t_hold / iss * 100), 2) if iss > 0 else 0.0
            res['foreign_hold_pct'] = float(df_inst.iloc[0].get('foreign_hold_pct', 0) or 0)

        inst_10d = df_inst.reindex(d_10).fillna(0)
        inst_5d = df_inst.reindex(d_5).fillna(0)  # 👈 抓取近5日區間
        res['f_buy_days_10'] = (inst_10d['foreign_buy_sell'] > 0).sum()
        res['t_sell_days_10'] = (inst_10d['invest_trust_buy_sell'] < 0).sum()
        res['t_buy_days_5'] = int((inst_5d['invest_trust_buy_sell'] > 0).sum())  # 👈 計算投信近5日買超天數
        # 這裡先給預設值 0，稍後在大表組合後，統一依據「強勢特徵標籤」來連動同步
        res['is_tu_yang'] = 0

        def get_val(df, date, col):
            if df.empty or col not in df.columns: return 0.0
            past = df[col].sort_index()[lambda x: x.index <= date]
            return float(past.iloc[-1]) if not past.empty else 0.0

        # 👇 從 def get_val 下方的 if len(market_dates) >= 5: 開始，替換到 return res 之前 👇
        if not df_inst.empty:
            # 確保參與計算的欄位為數值
            for col in ['invest_trust_hold', 'foreign_hold', 'foreign_hold_pct', 'issued_shares']:
                if col in df_inst.columns:
                    df_inst[col] = pd.to_numeric(df_inst[col], errors='coerce').fillna(0)

            if 'foreign_hold_pct' not in df_inst.columns:
                df_inst['foreign_hold_pct'] = 0.0

            # 💡 核心修正：歷史資料通常沒有 issued_shares，我們抓「最新一日」的股本來統一換算歷史比例
            latest_iss = float(df_inst['issued_shares'].iloc[0]) if 'issued_shares' in df_inst.columns else 0.0

            if 'invest_trust_hold' in df_inst.columns:
                if latest_iss > 0:
                    df_inst['t_hold_pct'] = (df_inst['invest_trust_hold'] / latest_iss) * 100
                elif 'foreign_hold' in df_inst.columns:
                    # 💡 終極備案：如果連最新股本都沒有，利用「外資持股張數」與「外資持股比例」精準數學反推投信比例
                    df_inst['t_hold_pct'] = np.where(
                        (df_inst['foreign_hold'] > 0) & (df_inst['foreign_hold_pct'] > 0),
                        (df_inst['invest_trust_hold'] / df_inst['foreign_hold']) * df_inst['foreign_hold_pct'],
                        0.0
                    )
                else:
                    df_inst['t_hold_pct'] = 0.0
            else:
                df_inst['t_hold_pct'] = 0.0

            d0 = market_dates[0]
            # 計算外資與投信的 5/10/20/60/120 日增減
            for p in [5, 10, 20, 60, 120]:
                dp = market_dates[p] if len(market_dates) > p else market_dates[-1] if len(market_dates) > 0 else d0
                res[f'f_diff_{p}d'] = round(
                    get_val(df_inst, d0, 'foreign_hold_pct') - get_val(df_inst, dp, 'foreign_hold_pct'), 2)
                res[f't_diff_{p}d'] = round(get_val(df_inst, d0, 't_hold_pct') - get_val(df_inst, dp, 't_hold_pct'), 2)

            # 保留原有的總法人與融資計算
        if len(market_dates) >= 5:
            d0, d5 = market_dates[0], market_dates[4]
            res['legal_diff_5d'] = round(
                get_val(df_inst, d0, 'total_legal_pct') - get_val(df_inst, d5, 'total_legal_pct'), 2)
            res['margin_diff_5d'] = round(get_val(df_margin, d0, 'fin_usage') - get_val(df_margin, d5, 'fin_usage'), 2)
        if len(market_dates) >= 20:
            d0, d20 = market_dates[0], market_dates[19]
            res['legal_diff_20d'] = round(
                get_val(df_inst, d0, 'total_legal_pct') - get_val(df_inst, d20, 'total_legal_pct'), 2)
            res['margin_diff_20d'] = round(get_val(df_margin, d0, 'fin_usage') - get_val(df_margin, d20, 'fin_usage'),
                                           2)
    return res


def get_strong_tags(row):
    tags = []
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

    bb_width = row.get('bb_width', 100)
    if bb_width < 5.0:
        tags.append('極度壓縮')
    elif bb_width < 8.0:
        tags.append('波動壓縮')

    if row.get('str_consol_5', 0) == 1: tags.append('盤整5日')
    if row.get('str_consol_10', 0) == 1: tags.append('盤整10日')
    if row.get('str_consol_20', 0) == 1: tags.append('盤整20日')
    if row.get('str_consol_60', 0) == 1: tags.append('盤整60日')

    if row.get('str_ilss_sweep', 0) == 1 and row.get('rev_cum_yoy', 0) > 0 and (
            row.get('m_net_today', 0) < 0 or row.get('m_sum_5d', 0) < 0): tags.append('主力掃單(ILSS)')

    # === 投信認養 (近5日買3日版，嚴格濾網) ===
    iss_shares = row.get('issued_shares', 0)
    it_pct = row.get('invest_trust_hold_pct', 0)
    t_net = row.get('t_net_today', 0)
    vol = row.get('今日成交股數', 0)
    t_buy_5d = row.get('t_buy_days_5', 0)

    c1 = (0 < iss_shares < 500000)
    c2 = (1.0 <= it_pct <= 8.0)
    c3 = (t_buy_5d >= 3 and t_net > 0)
    c4 = (t_net * 1000 / vol * 100 > 10.0) if vol > 0 else False

    if c1 and c2 and c3 and c4:
        tags.append('投信認養')
    # ===============================

    if row.get('m_net_today', 0) <= -200: tags.append('散戶退場')

    # === 終極版：土洋對作 (10日與20日雙雷達波段換手 + 5日價格點火) ===
    t_10d, f_10d = row.get('t_sum_10d', 0), row.get('f_sum_10d', 0)
    t_20d, f_20d = row.get('t_sum_20d', 0), row.get('f_sum_20d', 0)
    pct_5d = row.get('漲幅5d', 0)

    # 判定投信接刀成功 (內資軋外資)：近10日或近20日，投信買盤吃掉外資賣壓 30% 以上
    cond_t_win_10 = (t_10d > 0 and f_10d < 0 and t_10d >= abs(f_10d) * 0.3)
    cond_t_win_20 = (t_20d > 0 and f_20d < 0 and t_20d >= abs(f_20d) * 0.3)

    if (cond_t_win_10 or cond_t_win_20) and pct_5d > 0:
        tags.append('土洋對作(投勝)')

    # 判定外資掃貨成功 (外資軋內資)：近10日或近20日，外資買盤吃掉投信賣壓 30% 以上
    cond_f_win_10 = (f_10d > 0 and t_10d < 0 and f_10d >= abs(t_10d) * 0.3)
    cond_f_win_20 = (f_20d > 0 and t_20d < 0 and f_20d >= abs(t_20d) * 0.3)

    if (cond_f_win_10 or cond_f_win_20) and pct_5d > 0:
        tags.append('土洋對作(外勝)')
    # ================================================

    if row.get('legal_diff_20d', 0) > 1.5 and row.get('t_net_today', 0) > 0: tags.append('波段吸籌發動')

    offset = row.get('str_30w_week_offset', -1)
    suffix = "(本週)" if offset == 0 else (f"({int(offset)}週前)" if offset > 0 else "")
    if row.get('str_30w_adh', 0) == 1: tags.append(f"30W黏貼後突破{suffix}")
    if row.get('str_30w_shk', 0) == 1: tags.append(f"30W甩轎{suffix}")
    if row.get('str_fake_breakdown', 0) == 1: tags.append('假跌破')
    if row.get('str_ma55_sup', 0) == 1: tags.append('回測季線')
    if row.get('str_ma200_sup', 0) == 1: tags.append('回測年線')
    if row.get('str_vix_rev', 0) == 1: tags.append('Vix反轉')
    if row.get('str_30w_standby', 0) == 1: tags.append('30W臨門一腳')

    # === 營收創高標籤 ===
    rev_highest = row.get('rev_highest_months', 0)
    rev_consec = row.get('rev_consecutive_highs', 0)

    if rev_highest >= 23:
        tags.append('營收創兩年高')
    elif rev_highest >= 11:
        tags.append('營收創年高')

    if rev_consec >= 2:
        tags.append('營收連創新高')  # 給 UI Checkbox 統一比對用的
        tags.append(f'連{int(rev_consec)}月創高')  # 看實際是幾個月用的


    return ','.join(tags)


# 👇 插入在 def main(): 之上 👇
import concurrent.futures
import multiprocessing


def worker_full_calc(args):
    """分配給單一 CPU 核心的工作包 (完整運算)"""
    sid, name, industry, concept, dj_main, dj_sub, val_data = args
    try:
        # 每個核心獨立初始化 Cache，避免多進程搶佔資源或 pickling 錯誤
        from utils.cache.manager import CacheManager
        cache = CacheManager()

        df = cache.load(f"{sid}.TW")
        if df is None or df.empty:
            df = cache.load(f"{sid}.TWO")

        tech_factors = calculate_advanced_factors(df, sid=sid)
        if tech_factors is None:
            return None

        json_factors = calculate_json_factors(sid)

        merged = {
            'sid': sid,
            'name': name,
            'industry': industry,
            'sub_concepts': concept,
            'dj_main_ind': dj_main,
            'dj_sub_ind': dj_sub,
            **tech_factors,
            **json_factors,
            **val_data
        }
        return merged
    except Exception as e:
        # 發生錯誤時回傳 None，不中斷整體運行
        return None


def worker_fast_patch(args):
    """分配給單一 CPU 核心的工作包 (熱更新籌碼)"""
    sid, val_data = args
    try:
        j_factors = calculate_json_factors(sid)
        return {'sid': sid, **j_factors, **val_data}
    except:
        return None


# 👆 插入結束 👆

# 👇 整段替換既有的 def main(): 👇
def main():
    print(f"[System] 因子運算啟動 (V8.5 - 多進程平行運算版) | {datetime.now():%H:%M:%S}")

    # 讀取本地字典檔 (這部分維持不變，僅讀取一次)
    valuation_dict = {}
    yield_path = project_root / 'data' / 'market_yield.json'
    if yield_path.exists():
        try:
            with open(yield_path, 'r', encoding='utf-8') as f:
                valuation_dict = json.load(f)
            print(f"✅ 成功載入本地估值資料: {len(valuation_dict)} 檔")
        except Exception as e:
            print(f"⚠️ 讀取 market_yield.json 失敗: {e}")

    concept_dict = {}
    concept_path = project_root / 'data' / 'concept_tags.csv'
    if concept_path.exists():
        try:
            concept_df = pd.read_csv(concept_path, dtype=str)
            for _, row in concept_df.iterrows(): concept_dict[str(row['sid']).strip()] = str(row['sub_concepts'])
            print(f"✅ 成功載入概念股標籤: {len(concept_dict)} 檔")
        except Exception as e:
            print(f"⚠️ 讀取 concept_tags.csv 失敗: {e}")

    dj_dict = {}
    dj_path = project_root / 'data' / 'dj_industry.csv'
    if dj_path.exists():
        try:
            dj_df = pd.read_csv(dj_path, dtype=str)
            for _, row in dj_df.iterrows():
                dj_dict[str(row['sid']).strip()] = {'dj_main_ind': str(row['dj_main_ind']).strip(),
                                                    'dj_sub_ind': str(row['dj_sub_ind']).strip()}
            print(f"✅ 成功載入 MDJ 細產業標籤: {len(dj_dict)} 檔")
        except Exception as e:
            print(f"⚠️ 讀取 dj_industry.csv 失敗: {e}")

    white_list_path = project_root / 'data' / 'stock_list.csv'
    if not white_list_path.exists(): return print("[Error] 找不到 stock_list.csv 白名單")

    try:
        white_df = pd.read_csv(white_list_path, dtype=str)
    except:
        white_df = pd.read_csv(white_list_path, dtype=str, sep='\t')

    stock_dict = {}
    for _, row in white_df.iterrows():
        sid = str(row.get('stock_id', '')).strip()
        if sid: stock_dict[sid] = {'name': str(row.get('name', '未知')).strip(),
                                   'industry': str(row.get('industry', '未分類')).strip()}

    target_sids = list(stock_dict.keys())
    total = len(target_sids)
    print(f"📊 預計計算股票數量: {total} 檔")

    # 🌟 核心防卡死機制：取得 CPU 核心數，強制保留 2 顆核心給系統，最少使用 1 顆
    cpu_cores = multiprocessing.cpu_count()
    max_workers = max(1, cpu_cores - 2)
    print("-" * 40)
    print(f"🚀 啟動多核心引擎：使用 {max_workers} / {cpu_cores} 核心平行運算 (已保留系統資源防卡死)")
    print("-" * 40)

    # 準備工作包
    tasks = []
    for sid in target_sids:
        tasks.append((
            sid, stock_dict[sid]['name'], stock_dict[sid]['industry'],
            concept_dict.get(sid, ""), dj_dict.get(sid, {}).get('dj_main_ind', ""),
            dj_dict.get(sid, {}).get('dj_sub_ind', ""),
            valuation_dict.get(sid, {'pe': 0.0, 'pbr': 0.0, 'yield': 0.0})
        ))

    final_list = []

    # 🔥 正式派發多進程運算
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker_full_calc, task): task[0] for task in tasks}

        # as_completed 會在任何一檔股票算完時立刻回傳，不需照順序等，效率極高
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            result = future.result()
            if result:
                final_list.append(result)

            # 每處理 50 檔回報一次進度，避免洗畫面
            if (i + 1) % 50 == 0 or (i + 1) == total:
                pct = int((i + 1) / total * 100)
                print(f"PROGRESS: {pct}")
                print(f"   ⚡ 運算進度: {i + 1}/{total} 檔完成...")

    print("-" * 40)
    print(f"[System] 運算完成，共產出 {len(final_list)} 檔大表。")
    print("PROGRESS: 100")

    if not final_list: return

    final_df = pd.DataFrame(final_list)

    if '漲幅20d' in final_df.columns:
        final_df['RS強度'] = final_df['漲幅20d'].rank(pct=True) * 100
        final_df['RS強度'] = final_df['RS強度'].round(1)

    final_df['強勢特徵'] = final_df.apply(get_strong_tags, axis=1)
    final_df['is_tu_yang'] = final_df['強勢特徵'].apply(lambda x: 1 if '土洋對作' in str(x) else 0)

    # (中文字典對照維持你原本的，不用改)
    chinese_map = {
        'sid': '股票代號', 'name': '股票名稱', 'industry': '產業別',
        'sub_concepts': '概念股標籤', 'dj_main_ind': 'MDJ主產業', 'dj_sub_ind': 'MDJ細產業',
        'rev_ym': '最新營收月', 'rev_yoy': '營收YoY(%)', 'rev_cum_yoy': '累計營收YoY(%)',
        'rev_highest_months': '營收創高(月數)',
        'rev_consecutive_highs': '連創高(期數)',
        'fund_eps_year': 'EPS年度', 'fund_eps_cum': '最新EPS(累)', 'eps_latest_q': '最新EPS季',
        'eps_latest_val': '單季EPS(元)', 'eps_qoq': 'EPS季增率_QoQ(%)', 'eps_yoy': 'EPS年增率_YoY(%)',
        'eps_cum_yoy': '累計EPS_YoY(%)',
        'f_diff_5d': '外資5日增(%)', 'f_diff_10d': '外資10日增(%)', 'f_diff_20d': '外資20日增(%)',
        'f_diff_60d': '外資60日增(%)', 'f_diff_120d': '外資120日增(%)',
        't_diff_5d': '投信5日增(%)', 't_diff_10d': '投信10日增(%)', 't_diff_20d': '投信20日增(%)',
        't_diff_60d': '投信60日增(%)', 't_diff_120d': '投信120日增(%)',
        'legal_diff_5d': '法人5日增減(%)', 'margin_diff_5d': '融落5日增減(%)',
        'legal_diff_20d': '法人20日增減(%)', 'margin_diff_20d': '融資20日增減(%)',
        't_net_today': '投信買賣超(今)', 't_sum_5d': '投信買賣超(5日)', 't_sum_10d': '投信買賣超(10日)',
        't_sum_20d': '投信買賣超(20日)', 't_streak': '投信連買天數',
        'f_net_today': '外資買賣超(今)', 'f_sum_5d': '外資買賣超(5日)', 'f_sum_10d': '外資買賣超(10日)',
        'f_sum_20d': '外資買賣超(20日)', 'f_streak': '外資連買天數',
        'invest_trust_hold_pct': '投信持股(%)', 'foreign_hold_pct': '外資持股(%)',
        'm_net_today': '融資增減(今)', 'm_sum_5d': '融資增減(5日)', 'm_sum_10d': '融資增減(10日)',
        'm_sum_20d': '融資增減(20日)',
        'fund_contract_qoq': '合約負債季增(%)', 'fund_inventory_qoq': '庫存季增(%)',
        'fund_op_cash_flow': '最新營業現金流',
        'pe': '本益比', 'yield': '殖利率(%)', 'pbr': '股價淨值比',
        '現價': '今日收盤價', '漲幅1d': '今日漲幅(%)', '漲幅5d': '5日漲幅(%)', '漲幅20d': '20日漲幅(%)',
        '漲幅60d': '3個月漲幅(%)',
        'bb_width': '布林寬度(%)', '量比': '成交量比', 'RS強度': 'RS強度', '強勢特徵': '強勢特徵標籤',
        'str_30w_week_offset': '30W起漲週數(前)', 'str_st_week_offset': 'ST買訊(週)'
    }

    final_cols = [c for c in final_df.columns if c in chinese_map]
    output_df = final_df[final_cols].rename(columns=chinese_map)

    strategy_dir = project_root / 'data' / 'strategy_results'
    strategy_dir.mkdir(parents=True, exist_ok=True)
    final_df.to_parquet(strategy_dir / 'factor_snapshot.parquet')
    output_df.to_csv(strategy_dir / '戰情室今日快照_全中文版.csv', encoding='utf-8-sig', index=False)
    print(f"[System] 存檔完成: {strategy_dir}")


# 👆 整段替換結束 👆


import argparse  # 記得在檔案最上方加入


# ... (保留你原本的所有計算邏輯，不更動) ...

# ==========================================
# 🚀 極速熱更新 (Hot-Patch)：不重算 K 線，僅注入最新 JSON 籌碼
# ==========================================
# 👇 整段替換既有的 def run_fast_patch(): 👇
def run_fast_patch():
    print(f"[System] 啟動極速熱更新模式 (多進程版) | {datetime.now():%H:%M:%S}")

    parquet_path = project_root / 'data' / 'strategy_results' / 'factor_snapshot.parquet'
    if not parquet_path.exists():
        print("[Error] 找不到舊的大表，請先執行一次完整更新。")
        return

    df = pd.read_parquet(parquet_path)
    if 'sid' not in df.columns: return

    df.set_index('sid', inplace=True)
    target_sids = df.index.astype(str).tolist()

    valuation_dict = {}
    yield_path = project_root / 'data' / 'market_yield.json'
    if yield_path.exists():
        try:
            with open(yield_path, 'r', encoding='utf-8') as f:
                valuation_dict = json.load(f)
        except:
            pass

    # 🌟 核心防卡死機制
    cpu_cores = multiprocessing.cpu_count()
    max_workers = max(1, cpu_cores - 2)
    print(f"📊 正在將 {len(target_sids)} 檔股票籌碼注入大表 (使用 {max_workers} 核心)...")

    tasks = [(sid, valuation_dict.get(sid, {'pe': 0.0, 'pbr': 0.0, 'yield': 0.0})) for sid in target_sids]
    updated_rows = []

    # 🔥 正式派發多進程運算
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker_fast_patch, task): task[0] for task in tasks}
        total = len(tasks)
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            result = future.result()
            if result:
                updated_rows.append(result)

            if (i + 1) % 200 == 0 or (i + 1) == total:
                print(f"   ⚡ 熱更新進度: {i + 1}/{total} 檔讀取完成...")

    df_new_chips = pd.DataFrame(updated_rows)
    df_new_chips.set_index('sid', inplace=True)

    df.update(df_new_chips)
    df.reset_index(inplace=True)

    df['強勢特徵'] = df.apply(get_strong_tags, axis=1)
    df['is_tu_yang'] = df['強勢特徵'].apply(lambda x: 1 if '土洋對作' in str(x) else 0)

    strategy_dir = project_root / 'data' / 'strategy_results'
    df.to_parquet(strategy_dir / 'factor_snapshot.parquet')

    chinese_map = {
        'sid': '股票代號', 'name': '股票名稱', 'industry': '產業別',
        'sub_concepts': '概念股標籤', 'dj_main_ind': 'MDJ主產業', 'dj_sub_ind': 'MDJ細產業',
        'rev_ym': '最新營收月', 'rev_yoy': '營收YoY(%)', 'rev_cum_yoy': '累計營收YoY(%)',
        'rev_highest_months': '營收創高(月數)',
        'rev_consecutive_highs': '連創高(期數)',
        'fund_eps_year': 'EPS年度', 'fund_eps_cum': '最新EPS(累)', 'eps_latest_q': '最新EPS季',
        'eps_latest_val': '單季EPS(元)', 'eps_qoq': 'EPS季增率_QoQ(%)', 'eps_yoy': 'EPS年增率_YoY(%)',
        'eps_cum_yoy': '累計EPS_YoY(%)',
        'legal_diff_5d': '法人5日增減(%)', 'margin_diff_5d': '融落5日增減(%)',
        'legal_diff_20d': '法人20日增減(%)', 'margin_diff_20d': '融資20日增減(%)',
        't_net_today': '投信買賣超(今)', 't_sum_5d': '投信買賣超(5日)', 't_sum_10d': '投信買賣超(10日)',
        't_sum_20d': '投信買賣超(20日)', 't_streak': '投信連買天數',
        'f_net_today': '外資買賣超(今)', 'f_sum_5d': '外資買賣超(5日)', 'f_sum_10d': '外資買賣超(10日)',
        'f_sum_20d': '外資買賣超(20日)', 'f_streak': '外資連買天數',
        'invest_trust_hold_pct': '投信持股(%)', 'foreign_hold_pct': '外資持股(%)',
        'm_net_today': '融資增減(今)', 'm_sum_5d': '融資增減(5日)', 'm_sum_10d': '融資增減(10日)',
        'm_sum_20d': '融資增減(20日)',
        'fund_contract_qoq': '合約負債季增(%)', 'fund_inventory_qoq': '庫存季增(%)',
        'fund_op_cash_flow': '最新營業現金流',
        'pe': '本益比', 'yield': '殖利率(%)', 'pbr': '股價淨值比',
        '現價': '今日收盤價', '漲幅1d': '今日漲幅(%)', '漲幅5d': '5日漲幅(%)', '漲幅20d': '20日漲幅(%)',
        '漲幅60d': '3個月漲幅(%)',
        'bb_width': '布林寬度(%)', '量比': '成交量比', 'RS強度': 'RS強度', '強勢特徵': '強勢特徵標籤',
        'str_30w_week_offset': '30W起漲週數(前)', 'str_st_week_offset': 'ST買訊(週)'
    }
    final_cols = [c for c in df.columns if c in chinese_map]
    output_df = df[final_cols].rename(columns=chinese_map)
    output_df.to_csv(strategy_dir / '戰情室今日快照_全中文版.csv', encoding='utf-8-sig', index=False)

    print(f"[System] 熱更新完成！耗時極短。")
    print("PROGRESS: 100")


# 👆 整段替換結束 👆


# ==========================================
# 修改主入口
# ==========================================
if __name__ == "__main__":
    import multiprocessing

    multiprocessing.freeze_support()

    parser = argparse.ArgumentParser()
    parser.add_argument('--fast', action='store_true', help='啟用極速熱更新模式')
    args = parser.parse_args()

    if args.fast:
        run_fast_patch()
    else:
        main()
