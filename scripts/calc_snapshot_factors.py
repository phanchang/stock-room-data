import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os
import math
import warnings
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    from utils.cache.manager import CacheManager
    from utils.strategies.technical import TechnicalStrategies
except ImportError:
    print("[Error] 找不到 utils.cache.manager")
    sys.exit(1)


def calculate_advanced_factors(df, sid=None):
    if df is None or len(df) == 0:
        return None

    factors = {
        '現價': 0.0, '漲幅1d': 0.0, '漲幅5d': 0.0, '漲幅20d': 0.0, '漲幅60d': 0.0,
        'bb_width': 0.0, '量比': 0.0,
        'str_consol_5': 0, 'str_consol_10': 0, 'str_consol_20': 0, 'str_consol_60': 0,
        'str_ilss_sweep': 0, 'str_fake_breakdown': 0,
        'str_30w_adh': 0, 'str_30w_shk': 0, 'str_30w_info': "",
        'str_30w_week_offset': -1,
        'str_st_week_offset': -1,
        'str_break_30w': 0, 'str_uptrend': 0, 'str_high_60': 0, 'str_high_30': 0,
        'str_ma55_sup': 0, 'str_ma200_sup': 0, 'str_vix_rev': 0
    }

    df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'},
              inplace=True)

    last_close_daily = df['Close'].iloc[-1]
    factors['現價'] = last_close_daily

    if len(df) >= 2:
        factors['漲幅1d'] = round(df['Close'].pct_change(1).iloc[-1] * 100, 2)
        factors['量比'] = round(df['Volume'].iloc[-1] / df['Volume'].tail(5).mean(), 2) if df['Volume'].tail(
            5).mean() > 0 else 0

    if len(df) >= 6:
        factors['漲幅5d'] = round(df['Close'].pct_change(5).iloc[-1] * 100, 2)

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
                    lookback_weeks = 26
                    found_st_week = -1
                    max_idx = min(lookback_weeks, len(st_weekly) - 1)

                    for offset in range(max_idx + 1):
                        idx = -1 - offset
                        if st_weekly['Signal'].iloc[idx] == 1:
                            found_st_week = offset
                            break
                    factors['str_st_week_offset'] = found_st_week
                except Exception as e:
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
        except Exception as e:
            pass

    return factors


def main():
    print(f"[System] 因子運算啟動 (V5.9 - 整合5日籌碼與UI修復) | {datetime.now():%H:%M:%S}")

    cache = CacheManager()
    raw_path = project_root / 'data' / 'temp' / 'chips_revenue_raw.csv'
    if not raw_path.exists():
        print("[Error] 找不到 chips_revenue_raw.csv")
        return

    raw_df = pd.read_csv(raw_path, dtype={'sid': str})
    tech_list = []
    symbols = cache.get_all_symbols(market='tw')
    total = len(symbols)
    if total == 0: total = 1

    for i, symbol in enumerate(symbols):
        sid = symbol.split('.')[0]
        if i % 50 == 0 or i == total - 1:
            pct = int((i + 1) / total * 100)
            print(f"PROGRESS: {pct}")
            print(f"   Processing: {i}/{total}...", end='\r')
            sys.stdout.flush()

        df = cache.load(symbol)
        factors = calculate_advanced_factors(df, sid=sid)
        if factors:
            factors['sid'] = sid
            tech_list.append(factors)

    print(f"\n[System] 計算完成，共 {len(tech_list)} 檔。")
    print("PROGRESS: 100")

    tech_df = pd.DataFrame(tech_list).set_index('sid')
    final_df = raw_df.merge(tech_df, on='sid', how='left')

    if '漲幅20d' in final_df.columns:
        final_df['RS強度'] = final_df['漲幅20d'].rank(pct=True) * 100
        final_df['RS強度'] = final_df['RS強度'].round(1)

    def get_strong_tags(row):
        tags = []
        st_week = row.get('str_st_week_offset', -1)
        if st_week == 0:
            tags.append('ST轉多(本週)')
        elif 0 < st_week <= 4:
            tags.append(f'ST轉多({int(st_week)}週前)')

        offset = row.get('str_30w_week_offset', -1)
        suffix = ""
        if offset == 0:
            suffix = "(本週)"
        elif offset > 0:
            suffix = f"({int(offset)}週前)"

        if row.get('str_30w_adh', 0) == 1: tags.append(f"30W黏貼後突破{suffix}")
        if row.get('str_30w_shk', 0) == 1: tags.append(f"30W甩轎{suffix}")

        if row.get('str_consol_60', 0) == 1: tags.append('盤整60日')
        if row.get('str_consol_20', 0) == 1: tags.append('盤整20日')
        if row.get('str_consol_10', 0) == 1: tags.append('盤整10日')
        if row.get('str_consol_5', 0) == 1: tags.append('盤整5日')
        if row.get('bb_width', 100) < 5.0:
            tags.append('極度壓縮')
        elif row.get('bb_width', 100) < 8.0:
            tags.append('波動壓縮')
        if row.get('str_ilss_sweep', 0) == 1 and row.get('rev_cum_yoy', 0) > 0 and (
                row.get('m_net_today', 0) < 0 or row.get('m_sum_5d', 0) < 0): tags.append('主力掃單(ILSS)')
        if row.get('str_fake_breakdown', 0) == 1: tags.append('假跌破')
        if row.get('RS強度', 0) > 90: tags.append('超強勢')
        if row.get('漲幅60d', 0) > 30: tags.append('波段黑馬')
        if row.get('str_break_30w', 0) == 1: tags.append('突破30週')
        if row.get('str_uptrend', 0) == 1: tags.append('強勢多頭')
        if row.get('str_high_60', 0) == 1: tags.append('創季高')
        if row.get('str_high_30', 0) == 1: tags.append('創月高')
        if row.get('is_tu_yang', 0) == 1: tags.append('土洋對作')
        if row.get('t_streak', 0) >= 3: tags.append('投信認養')
        if row.get('m_net_today', 0) < -200: tags.append('散戶退場')
        if row.get('str_ma55_sup', 0) == 1: tags.append('回測季線')
        if row.get('str_ma200_sup', 0) == 1: tags.append('回測年線')
        if row.get('str_vix_rev', 0) == 1: tags.append('Vix反轉')
        return ','.join(tags)

    import json

    def calculate_json_factors(row):
        sid_str = str(row['sid'])
        json_path = project_root / 'data' / 'fundamentals' / f"{sid_str}.json"

        res = {
            'fund_contract_qoq': 0.0,
            'fund_inventory_qoq': 0.0,
            'fund_op_cash_flow': 0.0,
            'margin_diff_5d': 0.0,  # 🔥 新增: 融資 5 日增減
            'legal_diff_5d': 0.0,  # 🔥 新增: 法人 5 日增減
            'margin_diff_20d': 0.0,
            'legal_diff_20d': 0.0,
            'fund_eps_cum': 0.0,
            'fund_eps_year': ''
        }

        if not json_path.exists():
            return pd.Series(res)

        try:
            with open(json_path, 'r', encoding='utf-8') as jf:
                jdata = json.load(jf)
        except Exception:
            return pd.Series(res)

        bs = jdata.get('balance_sheet', [])
        if len(bs) >= 2:
            try:
                cl_0 = float(bs[0].get('contract_liab', 0))
                cl_1 = float(bs[1].get('contract_liab', 0))
                if cl_1 > 0:
                    res['fund_contract_qoq'] = round(((cl_0 - cl_1) / cl_1) * 100, 2)
            except:
                pass

        if len(bs) >= 2:
            try:
                inv_0 = float(bs[0].get('inventory', 0))
                inv_1 = float(bs[1].get('inventory', 0))
                if inv_1 > 0:
                    res['fund_inventory_qoq'] = round(((inv_0 - inv_1) / inv_1) * 100, 2)
            except:
                pass

        cf = jdata.get('cash_flow', [])
        if len(cf) > 0:
            try:
                res['fund_op_cash_flow'] = float(cf[0].get('op_cash_flow', 0))
            except:
                pass

        margin_data = jdata.get('margin_trading', [])
        inst_data = jdata.get('institutional_investors', [])

        # 🔥 計算 5 日籌碼差異
        if len(margin_data) >= 5 and len(inst_data) >= 5:
            try:
                mb_latest = float(margin_data[0].get('fin_usage', 0))
                mb_5d = float(margin_data[4].get('fin_usage', 0))
                tl_latest = float(inst_data[0].get('total_legal_pct', 0))
                tl_5d = float(inst_data[4].get('total_legal_pct', 0))

                res['margin_diff_5d'] = round(mb_latest - mb_5d, 2)
                res['legal_diff_5d'] = round(tl_latest - tl_5d, 2)
            except:
                pass

        # 計算 20 日籌碼差異
        if len(margin_data) >= 20 and len(inst_data) >= 20:
            try:
                mb_latest = float(margin_data[0].get('fin_usage', 0))
                mb_20d = float(margin_data[19].get('fin_usage', 0))
                tl_latest = float(inst_data[0].get('total_legal_pct', 0))
                tl_20d = float(inst_data[19].get('total_legal_pct', 0))

                res['margin_diff_20d'] = round(mb_latest - mb_20d, 2)
                res['legal_diff_20d'] = round(tl_latest - tl_20d, 2)
            except:
                pass

        prof = jdata.get('profitability', [])
        if len(prof) > 0:
            try:
                latest_q = prof[0].get('quarter', '')
                if '.' in latest_q:
                    latest_year = latest_q.split('.')[0]
                    res['fund_eps_year'] = latest_year

                    cum_eps = 0.0
                    for p in prof:
                        q_str = p.get('quarter', '')
                        if q_str.startswith(latest_year + '.'):
                            cum_eps += float(p.get('eps', 0.0))
                    res['fund_eps_cum'] = round(cum_eps, 2)
            except:
                pass

        return pd.Series(res)

    print("[System] 正在進行深度 JSON 特徵轉換 (寫入 Parquet 數值)...")
    json_factors_df = final_df.apply(calculate_json_factors, axis=1)
    final_df = pd.concat([final_df, json_factors_df], axis=1)

    final_df['強勢特徵'] = final_df.apply(get_strong_tags, axis=1)

    chinese_map = {
        'sid': '股票代號', 'name': '股票名稱', 'industry': '產業別',
        'rev_yoy': '營收年增率(%)', 'rev_cum_yoy': '累計營收年增率(%)',
        'rev_ym': '營收月份',
        't_sum_5d': '投信買賣超(5日)', 't_streak': '投信連買天數',
        'f_sum_5d': '外資買賣超(5日)', 'f_streak': '外資連買天數',
        'm_sum_5d': '融資增減(5日)', 'm_net_today': '融資增減(今日)',
        'pe': '本益比', 'yield': '殖利率(%)',
        '現價': '今日收盤價', '漲幅1d': '今日漲幅(%)', '漲幅20d': '20日漲幅(%)', '漲幅60d': '3個月漲幅(%)',
        'bb_width': '布林寬度(%)', '量比': '成交量比', 'RS強度': 'RS強度', '強勢特徵': '強勢特徵標籤',
        'str_30w_week_offset': '30W起漲週數(前)',
        'str_st_week_offset': 'ST買訊(週)',
        'fund_contract_qoq': '合約負債季增(%)',
        'fund_inventory_qoq': '庫存季增(%)',
        'fund_op_cash_flow': '最新營業現金流',
        'margin_diff_5d': '融資5日增減(%)',  # 🔥 新增
        'legal_diff_5d': '法人5日增減(%)',  # 🔥 新增
        'margin_diff_20d': '融資20日增減(%)',
        'legal_diff_20d': '法人20日增減(%)',
        'fund_eps_cum': '最新EPS(累)',
        'fund_eps_year': '最新EPS年度'
    }

    output_df = final_df.copy().rename(columns=chinese_map)
    strategy_dir = project_root / 'data' / 'strategy_results'
    strategy_dir.mkdir(parents=True, exist_ok=True)
    final_df.to_parquet(strategy_dir / 'factor_snapshot.parquet')
    output_df.to_csv(strategy_dir / '戰情室今日快照_全中文版.csv', encoding='utf-8-sig', index=False)
    print("[System] 存檔完成。")


if __name__ == "__main__":
    main()