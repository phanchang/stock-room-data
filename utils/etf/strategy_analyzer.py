import pandas as pd
import numpy as np
from pathlib import Path
import sys
from datetime import datetime

# ==================================================
# 1. 專案路徑初始化
# ==================================================
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


class StrategyAnalyzer:
    """主動式 ETF 策略分析器：雙榜單系統 (重金押寶榜 vs 潛伏偷買榜)"""

    def __init__(self):
        self.clean_dir = project_root / "data" / "clean"
        self.cache_dir = project_root / "data" / "cache" / "tw"
        self.all_data = []
        self.stock_market_map = {}
        self.latest_report_date = None

        self.load_market_info()

    def load_market_info(self):
        csv_path = project_root / "data" / "stock_list.csv"
        if csv_path.exists():
            try:
                for enc in ['utf-8', 'utf-8-sig', 'big5']:
                    try:
                        df = pd.read_csv(csv_path, dtype=str, encoding=enc)
                        df.columns = [c.lower().strip() for c in df.columns]
                        code_col = next((col for col in ['stock_id', 'code', 'id'] if col in df.columns), None)

                        if code_col and 'market' in df.columns:
                            for _, row in df.iterrows():
                                sid = str(row[code_col]).strip()
                                market = str(row['market']).strip().upper()
                                self.stock_market_map[sid] = market
                            break
                    except:
                        continue
            except Exception as e:
                print(f"❌ 讀取市場資訊失敗: {e}")

    def get_market_suffix(self, stock_id):
        return self.stock_market_map.get(str(stock_id), "TW")

    def load_all_clean_data(self):
        print(f"🔍 正在掃描資料夾: {self.clean_dir}")
        for csv_file in self.clean_dir.rglob("*.csv"):
            if csv_file.name.startswith("._") or "stock_list" in csv_file.name:
                continue
            try:
                df = pd.read_csv(csv_file)
                df['date'] = pd.to_datetime(df['date'])
                id_col = 'stock_code' if 'stock_code' in df.columns else 'stock_id'

                df = df.rename(columns={id_col: 'stock_id', 'stock_name': 'name'})
                df['stock_id'] = df['stock_id'].astype(str)
                df['etf_source'] = f"{csv_file.parent.name}_{csv_file.stem}"

                for col in ['shares', 'weight']:
                    if col in df.columns and df[col].dtype == 'object':
                        df[col] = df[col].astype(str).str.replace(',', '').str.replace('%', '')
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                self.all_data.append(df)
            except Exception as e:
                pass

    def analyze_dual_leaderboards(self):
        """計算雙榜單所需的所有歷史與價格特徵"""
        if not self.all_data: return pd.DataFrame()

        combined_df = pd.concat(self.all_data, ignore_index=True)
        daily_summary = combined_df.groupby(['date', 'stock_id', 'name'])['shares'].sum().reset_index()

        dates = sorted(daily_summary['date'].unique())
        if len(dates) < 5:
            print("⚠️ 歷史資料過少，無法分析。")
            return pd.DataFrame()

        self.latest_report_date = dates[-1].strftime('%Y-%m-%d')

        t_now = dates[-1]
        t_5 = dates[-5] if len(dates) >= 5 else dates[0]
        t_20 = dates[-20] if len(dates) >= 20 else dates[0]
        t_60 = dates[-60] if len(dates) >= 60 else dates[0]

        analysis_results = []
        stocks = daily_summary['stock_id'].unique()

        print(f"📈 啟動雙榜單籌碼追蹤引擎...")

        for stock_id in stocks:
            stock_data = daily_summary[daily_summary['stock_id'] == stock_id].sort_values('date')
            name = stock_data['name'].iloc[0]

            def get_shares(t_date):
                return stock_data[stock_data['date'] == t_date]['shares'].sum() if t_date in stock_data[
                    'date'].values else 0

            shares_now = get_shares(t_now)
            shares_5d_ago = get_shares(t_5)
            shares_20d_ago = get_shares(t_20)
            shares_60d_ago = get_shares(t_60)

            diff_5d = shares_now - shares_5d_ago
            diff_20d = shares_now - shares_20d_ago
            diff_60d = shares_now - shares_60d_ago

            # 條件過濾：我們只看「近5天有在買」的活水股
            if diff_5d <= 0: continue

            market = self.get_market_suffix(stock_id)
            price_path = self.cache_dir / f"{stock_id}_{market}.parquet"

            current_price = 0
            estimated_cost_60d = 0
            net_buy_value_60d = 0

            if price_path.exists():
                try:
                    price_df = pd.read_parquet(price_path)
                    price_df.columns = [c.capitalize() for c in price_df.columns]
                    price_df.index = pd.to_datetime(price_df.index).tz_localize(None)

                    if not price_df.empty:
                        current_price = price_df.iloc[-1]['Close']

                        stock_data_60 = stock_data[stock_data['date'] >= t_60].copy()
                        stock_data_60['share_change'] = stock_data_60['shares'].diff().fillna(0)

                        total_buy_cost = 0
                        total_buy_shares = 0

                        for _, row in stock_data_60.iterrows():
                            if row['share_change'] > 0:
                                p_date = row['date']
                                if p_date in price_df.index:
                                    daily_price = price_df.loc[p_date, 'Close']
                                else:
                                    nearest_idx = price_df.index.get_indexer([p_date], method='nearest')[0]
                                    daily_price = price_df.iloc[nearest_idx]['Close']

                                total_buy_cost += (row['share_change'] * daily_price)
                                total_buy_shares += row['share_change']

                        if total_buy_shares > 0:
                            estimated_cost_60d = total_buy_cost / total_buy_shares
                            net_buy_value_60d = total_buy_cost / 100000000
                except Exception:
                    pass

            analysis_results.append({
                '代號': stock_id,
                '名稱': name,
                '當前股數': shares_now,
                '20日前股數': shares_20d_ago,
                '近5日增減': diff_5d,
                '近20日增減': diff_20d,
                '一季耗資(億)': round(net_buy_value_60d, 0),
                '最新收盤價': current_price,
                '投信季成本': round(estimated_cost_60d, 2)
            })

        result_df = pd.DataFrame(analysis_results)
        if result_df.empty: return result_df

        result_df['乖離(%)'] = np.where(result_df['投信季成本'] > 0,
                                        ((result_df['最新收盤價'] - result_df['投信季成本']) / result_df[
                                            '投信季成本'] * 100), 0)
        result_df['乖離(%)'] = result_df['乖離(%)'].round(2)

        return result_df

    def generate_ai_prompt(self, df):
            """產出雙榜單與防幻覺查證指令的 AI Prompt"""

            # ==========================================
            # 榜單 A：重金押寶榜 (依耗資金額排序 Top 15)
            # ==========================================
            list_a = df.sort_values('一季耗資(億)', ascending=False).head(15).copy()
            list_a['當前股數'] = list_a['當前股數'].apply(lambda x: f"{x:,}")
            list_a_view = list_a[
                ['代號', '名稱', '當前股數', '近5日增減', '近20日增減', '一季耗資(億)', '投信季成本', '乖離(%)']]

            # ==========================================
            # 榜單 B：潛伏偷買榜
            # ==========================================
            exclude_ids = list_a['代號'].tolist()
            list_b_candidates = df[~df['代號'].isin(exclude_ids)].copy()

            stealth_mask = (list_b_candidates['20日前股數'] == 0) | (
                    list_b_candidates['近5日增減'] == list_b_candidates['近20日增減'])
            list_b = list_b_candidates[stealth_mask].sort_values('近5日增減', ascending=False).head(15)

            list_b['當前股數'] = list_b['當前股數'].apply(lambda x: f"{x:,}")
            list_b_view = list_b[
                ['代號', '名稱', '當前股數', '近5日增減', '近20日增減', '一季耗資(億)', '投信季成本', '乖離(%)']]

            # 取得當前真實年份與月份，做為防呆錨點
            current_year = pd.to_datetime(self.latest_report_date).year
            current_month = pd.to_datetime(self.latest_report_date).month

            prompt = f"""# 📅 台股主動式 ETF 【雙榜單】籌碼戰情報表

    ## 📊 第一階段：量化數據基準
    **資料庫基準日：** {self.latest_report_date} (請注意現在的年份是 {current_year} 年 {current_month} 月)

    ### 🏆 榜單 A：重金押寶榜 (Top 15 主力資金流向)
    > 說明：投信近一季砸下最多「絕對金額」的核心標的 (單位為新台幣『億』元)。
    {list_a_view.to_markdown(index=False) if not list_a_view.empty else "無符合條件標的"}

    ### 🥷 榜單 B：破蛋潛伏偷買榜 (Top 15 零到一黑馬)
    > 說明：特徵為「過去 20 天沒買，最近突然連續買進 / 從零建倉」。
    {list_b_view.to_markdown(index=False) if not list_b_view.empty else "無符合條件標的"}

    ---

    ## 🤖 第二階段：AI 操盤手深度推斷與【強制多方查證指令】

    你是一位具備 20 年經驗的台股量化與基本面操盤手。請基於上方雙榜單執行分析。

    ⚠️ 【最高指導原則：絕對客觀與防時間幻覺】⚠️
    1. 引用數據做推理與研究「必須」使用 Google search 進行多方來源交叉查證。
    2. 絕不允許自己產生、捏造或猜測真實數據。
    3. 引用任何數據做呈現或計算，必須在該段落明確附上「來源網站與資料出處」。
    4. 🛑【時間防偽警告】：現在是 {current_year} 年 {current_month} 月。搜尋新聞與財報時，請嚴格過濾時間！**絕對禁止**把 {current_year - 1} 年或更早的舊新聞當成現在的事件！

    ### 🔍 任務 1：主流板塊 vs 潛伏板塊對比分析
    * 觀察【榜單 A】，目前投信重兵集結在哪 1~2 個產業？(表內的耗資單位為「億」，請直接以億為單位解讀，例如 81.15 就是 81.15 億)
    * 觀察【榜單 B】，投信正在偷偷佈局哪些產業？這是否暗示資金有高低位階轉換的跡象？

    ### 🕵️‍♂️ 任務 2：雙榜單核心標的客觀事實查證 (Fact-Checking)
    請從【榜單 A】與【榜單 B】各挑選 2 檔，**強制使用 Google 搜尋查證**近一個月內 ({current_year}年) 的重大事件。
    * **時間防呆檢查**：找到新聞後，請先確認新聞發布年份是否為 {current_year} 年。如果是舊聞請捨棄。
    * **潛伏股防呆**：對於【榜單 B】，如果你查不到任何 {current_year} 年的近期利多新聞，請直接寫明「經查證，近期無 {current_year} 年相關新聞發布」。代表該股處於「無聲建倉期」，絕對禁止拿舊新聞填補或捏造！
    * **輸出格式要求**：(以表格呈現)
      | 所屬榜單 | 股票名稱 | 近期真實催化劑 (查無近期新聞請誠實填寫) | 發生年份 | 資料來源 (必須附上 URL) |

    ### 🎯 任務 3：明日實戰交易清單推斷 (Actionable Plan)
    基於法人籌碼節奏與你的客觀查證，挑選出 3 檔最值得列入明日觀察名單的股票。請依序條列，並再次附上支撐你論點的 {current_year} 年最新資料出處網址。
    """
            return prompt

    def run(self):
        print(f"=== 戰情室雙榜單分析系統啟動: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

        self.load_all_clean_data()
        result_df = self.analyze_dual_leaderboards()

        if not result_df.empty:
            prompt = self.generate_ai_prompt(result_df)

            output_path = project_root / "data" / "daily_ai_prompt.txt"
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(prompt)

            print(f"\n✅ 雙榜單深度分析報表產出成功！")
            print(f"📍 檔案位置: {output_path}")
            print("-" * 50)
            print("💡 提示：現在 AI 能夠明確區分「重金動能股」與「無聲潛伏股」了，快貼給 Gemini 測試看看！")
            print("-" * 50)
        else:
            print("⚠️ 警告：無法計算出波段數據。")


if __name__ == "__main__":
    analyzer = StrategyAnalyzer()
    analyzer.run()