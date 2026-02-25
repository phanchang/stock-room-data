import pandas as pd
import numpy as np
from pathlib import Path
import sys

# 確保載入 utils
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.strategies.technical import TechnicalStrategies


def generate_explosive_master_table():
    print("🚀 啟動【科技股加權升級版】三維度量化選股模型...")

    chips_path = project_root / "data" / "temp" / "chips_revenue_raw.csv"
    if not chips_path.exists():
        print(f"❌ 找不到籌碼檔案: {chips_path}")
        return

    df_chips = pd.read_csv(chips_path, dtype={'sid': str})

    numeric_cols = ['rev_yoy', 'rev_cum_yoy', 'pe', 'yield', 'f_sum_5d', 't_sum_5d',
                    'm_sum_5d', 's_sum_5d', 'f_streak', 't_streak', 'is_tu_yang']
    for col in numeric_cols:
        if col in df_chips.columns:
            df_chips[col] = pd.to_numeric(df_chips[col], errors='coerce').fillna(0)

    # 標記是否為科技/電子股 (產業紅利)
    tech_keywords = ['半導體', '電子', '電腦', '光電', '通信', '網通', '零組件', '資訊']
    df_chips['is_tech'] = df_chips['industry'].astype(str).apply(lambda x: any(k in x for k in tech_keywords)).astype(
        int)

    master_records = []
    base_cache_path = project_root / "data" / "cache" / "tw"

    print("🔄 正在讀取 K 線，執行獨立裝甲特徵運算...")
    for idx, row in df_chips.iterrows():
        sid = row['sid']
        path_tw = base_cache_path / f"{sid}_TW.parquet"
        path_two = base_cache_path / f"{sid}_TWO.parquet"
        file_path = path_two if path_two.exists() else path_tw

        tech = {
            'above_150ma': 0, 'is_consolidation': 0, 'comp_days_60': 0,
            'has_30w_sig_15d': 0, 'has_vol_spike_15d': 0, 'has_shk_15d': 0,
            'strong_uptrend': 0, 'supertrend_dir': 0, 'breakout_high': 0
        }

        if file_path.exists():
            try:
                df_kline = pd.read_parquet(file_path)
                if len(df_kline) >= 160:
                    df_kline.rename(columns=lambda x: x.capitalize() if x.lower() in ['open', 'high', 'low', 'close',
                                                                                      'volume'] else x, inplace=True)
                    ma150 = df_kline['Close'].rolling(150).mean()

                    try:
                        tech['above_150ma'] = int(df_kline['Close'].iloc[-1] > ma150.iloc[-1])
                    except:
                        pass

                    try:
                        ma20 = df_kline['Close'].rolling(20).mean()
                        std20 = df_kline['Close'].rolling(20).std()
                        bb_width = (4 * std20) / ma20 * 100
                        # 稍微放寬科技股的壓縮定義至 18，避免錯殺
                        tech['is_consolidation'] = int(bb_width.iloc[-1] < 25)
                        tech['comp_days_60'] = int((bb_width.tail(60) < 25).sum())
                    except:
                        pass

                    try:
                        vol_20ma = df_kline['Volume'].rolling(20).mean()
                        is_vol_spike = df_kline['Volume'] > (1.5 * vol_20ma.shift(1))
                        tech['has_vol_spike_15d'] = int(is_vol_spike.tail(15).any())
                    except:
                        pass

                    try:
                        adh_series = (abs(df_kline['Close'] - ma150) / ma150) < 0.04
                        shk_series = (df_kline['Close'] > ma150) & (df_kline['Close'].shift(1) < ma150.shift(1))
                        has_adh = adh_series.tail(15).any()
                        has_shk = shk_series.tail(15).any()

                        if hasattr(TechnicalStrategies, 'strategy_30w_adherence'):
                            res = TechnicalStrategies.strategy_30w_adherence(df_kline)
                            has_adh = res.tail(15).any() if isinstance(res, pd.Series) else bool(res)

                        if hasattr(TechnicalStrategies, 'strategy_30w_shakeout'):
                            res = TechnicalStrategies.strategy_30w_shakeout(df_kline)
                            has_shk = res.tail(15).any() if isinstance(res, pd.Series) else bool(res)

                        tech['has_30w_sig_15d'] = int(has_adh or has_shk)
                        tech['has_shk_15d'] = int(has_shk)
                    except:
                        pass

                    try:
                        st_df = TechnicalStrategies.calculate_supertrend(df_kline)
                        tech['supertrend_dir'] = int(st_df['Direction'].iloc[-1]) if not st_df.empty else 0
                        tech['strong_uptrend'] = int(TechnicalStrategies.strong_uptrend(df_kline).iloc[-1])
                        tech['breakout_high'] = int(
                            TechnicalStrategies.breakout_n_days_high(df_kline, window=20).iloc[-1])
                    except:
                        pass

            except Exception as e:
                pass

        merged_row = {**row.to_dict(), **tech}
        master_records.append(merged_row)

    df_master = pd.DataFrame(master_records).fillna(0)

    print("⚡ 正在執行階梯式計分與【科技股產業加權】...")

    # 【修正 EPS 為 0 的問題】用 PE > 0 且 PE < 40 替代有賺錢的證明
    is_profitable = ((df_master['pe'] > 0) & (df_master['pe'] < 40)).astype(int)

    # ==========================================
    # 🛡️ 部隊一：黃金潛伏 (滿分 12分，含科技加權)
    # ==========================================
    score_t1_base = is_profitable * 2 + (df_master['rev_yoy'] > 5).astype(int) + \
                    (df_master['rev_yoy'] > 15).astype(int) + (df_master['rev_yoy'] > 30).astype(int)
    score_t1_chips = ((df_master['f_sum_5d'] + df_master['t_sum_5d']) > 0).astype(int) * 2 + \
                     ((df_master['f_streak'] >= 3) | (df_master['t_streak'] >= 3)).astype(int) * 2
    score_t1_retail = (df_master['m_sum_5d'] < 0).astype(int)

    # 科技股加權 (+2分)
    score_t1_tech_bonus = df_master['is_tech'] * 1

    df_master['T1_Score'] = score_t1_base + score_t1_chips + score_t1_retail + score_t1_tech_bonus
    df_master['T1_Valid'] = ((df_master['above_150ma'] == 1) & (df_master['is_consolidation'] == 1)).astype(int)

    # ==========================================
    # ⚔️ 部隊二：突破交易 (滿分 12分，含科技加權)
    # ==========================================
    score_t2_comp = (df_master['comp_days_60'] > 10).astype(int) + (df_master['comp_days_60'] > 20).astype(int) + \
                    (df_master['comp_days_60'] > 30).astype(int) + (df_master['comp_days_60'] > 40).astype(int)
    score_t2_tech = (df_master['has_shk_15d'] == 1).astype(int) * 2
    score_t2_base = is_profitable * 2 + (df_master['rev_yoy'] > 0).astype(int) * 2
    score_t2_tech_bonus = df_master['is_tech'] * 2

    df_master['T2_Score'] = score_t2_comp + score_t2_tech + score_t2_base + score_t2_tech_bonus
    df_master['T2_Valid'] = ((df_master['above_150ma'] == 1) &
                             (df_master['has_30w_sig_15d'] == 1) &
                             (df_master['has_vol_spike_15d'] == 1)).astype(int)

    # ==========================================
    # 🚀 部隊三：強勢追價 (滿分 12分，含科技加權)
    # ==========================================
    score_t3_rev = (df_master['rev_yoy'] > 20).astype(int) * 4
    score_t3_short = (df_master['s_sum_5d'] > 0).astype(int) * 3
    score_t3_break = (df_master['breakout_high'] == 1).astype(int) * 3
    score_t3_tech_bonus = df_master['is_tech'] * 2

    df_master['T3_Score'] = score_t3_rev + score_t3_short + score_t3_break + score_t3_tech_bonus
    df_master['T3_Valid'] = ((df_master['strong_uptrend'] == 1) &
                             (df_master['supertrend_dir'] == 1) &
                             ((df_master['f_streak'] >= 3) | (df_master['t_streak'] >= 3))).astype(int)

    # ==========================================
    # 篩選與產出報表
    # ==========================================
    valid_df = df_master[(df_master['pe'] < 100) | (df_master['pe'] == 0)]

    t1_top10 = valid_df[valid_df['T1_Valid'] == 1].sort_values(by=['T1_Score', 'rev_yoy'],
                                                               ascending=[False, False]).head(10)
    t2_top10 = valid_df[valid_df['T2_Valid'] == 1].sort_values(by=['T2_Score', 'comp_days_60'],
                                                               ascending=[False, False]).head(10)
    t3_top10 = valid_df[valid_df['T3_Valid'] == 1].sort_values(by=['T3_Score', 'rev_yoy'],
                                                               ascending=[False, False]).head(10)

    output_path = project_root / "data" / "temp" / "master_explosive_table.csv"
    df_master.to_csv(output_path, index=False, encoding='utf-8-sig')

    print("\n🛡️ 【戰略一：黃金科技潛伏 Top 10】 (不接刀、盤整中、本益比保護、法人偷買):")
    if not t1_top10.empty:
        t1_disp = t1_top10[['sid', 'name', 'industry', 'T1_Score', 'rev_yoy', 'pe', 'comp_days_60']]
        t1_disp.columns = ['代號', '名稱', '產業', '分數(滿12)', '營收年增%', 'PE', '近60日壓縮天數']
        print(t1_disp.to_string(index=False))
    else:
        print("  無符合標的")

    print("\n⚔️ 【戰略二：突破交易 Top 10】 (近15日帶量與30W訊號、壓縮越久越高分):")
    if not t2_top10.empty:
        t2_disp = t2_top10[['sid', 'name', 'industry', 'T2_Score', 'comp_days_60', 'has_shk_15d', 'pe']]
        t2_disp.columns = ['代號', '名稱', '產業', '分數(滿12)', '壓縮天數', '有無甩轎', 'PE']
        print(t2_disp.to_string(index=False))
    else:
        print("  無符合標的")

    print("\n🚀 【戰略三：強勢追價 Top 10】 (絕對多頭、法人狂買、軋空與業績創高):")
    if not t3_top10.empty:
        t3_disp = t3_top10[['sid', 'name', 'industry', 'T3_Score', 'rev_yoy', 'f_streak', 't_streak']]
        t3_disp.columns = ['代號', '名稱', '產業', '分數(滿12)', '營收年增%', '外資連買', '投信連買']
        print(t3_disp.to_string(index=False))
    else:
        print("  無符合標的")

    # ==========================================
    # 精準化 AI Deep Research Prompt (只挑最值得驗證的)
    # ==========================================
    def get_prompt_targets(df):
        if df.empty: return "無"
        # 優先挑選科技股，再依分數排序，最多挑 2 檔
        tech_targets = df[df['is_tech'] == 1].head(2)
        if tech_targets.empty: tech_targets = df.head(1)
        return "、".join([f"{row['name']}({row['sid']})" for _, row in tech_targets.iterrows()])

    t1_targets = get_prompt_targets(t1_top10)
    t2_targets = get_prompt_targets(t2_top10)
    t3_targets = get_prompt_targets(t3_top10)

    prompt_content = f"""你現在是一位掌管百億資金的華爾街高盛(Proprietary Trading)首席量化經理人。
我剛透過一套嚴格的【三維度戰略選股模型】(過濾了接刀風險，加入了半導體/電子產業權重、以及PE本益比保護)，篩選出了以下名單：

【🛡️ 黃金潛伏部隊 Top 10】：
{t1_top10[['sid', 'name', 'industry', 'T1_Score', 'rev_yoy', 'pe']].to_string(index=False) if not t1_top10.empty else "無"}

【⚔️ 突破交易部隊 Top 10】：
{t2_top10[['sid', 'name', 'industry', 'T2_Score', 'comp_days_60']].to_string(index=False) if not t2_top10.empty else "無"}

【🚀 強勢追價部隊 Top 10】：
{t3_top10[['sid', 'name', 'industry', 'T3_Score', 'rev_yoy']].to_string(index=False) if not t3_top10.empty else "無"}

請針對這三個部隊中，系統特挑出必須「Double Confirm」的核心科技標的：
- 潛伏驗證：【{t1_targets}】
- 突破驗證：【{t2_targets}】
- 追價驗證：【{t3_targets}】

進行「深度盡職調查 (Deep Research)」。(❗務必使用 Google Search 查證最新法說會與新聞，不幻想數據)

請提供【差異化交易劇本與利多查證】：
1. 潛伏部隊：挖掘其營收基本面轉機(是否接到大廠訂單?)，並評估目前的 PE 是否確實低估？給出左側買點區間。
2. 突破部隊：查證近期的爆量利多是什麼？嚴格給出「跌破起漲點或150日線」的技術面防守線。
3. 追價部隊：這波法人狂買是真外資還是隔日沖？評估市場是否過熱，給出移動停利防守價位。
"""
    prompt_path = project_root / "data" / "temp" / "daily_gemini_prompt.txt"
    with open(prompt_path, "w", encoding="utf-8") as f:
        f.write(prompt_content)
    print(f"\n🤖 高盛級 AI 深度研究 Prompt 已自動生成並完成目標聚焦：{prompt_path}")


if __name__ == "__main__":
    generate_explosive_master_table()