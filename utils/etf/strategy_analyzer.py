import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os
from datetime import datetime

# ==================================================
# 1. 專案路徑初始化
# ==================================================
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


class StrategyAnalyzer:
    """主動式 ETF 策略分析器：整合多投信籌碼動向"""

    def __init__(self):
        self.clean_dir = project_root / "data" / "clean"
        self.all_data = []
        self.latest_report_dates = {}

    def load_all_clean_data(self):
        """動態掃描 data/clean 下所有投信的 CSV 檔案並標準化"""
        print(f"🔍 正在掃描資料夾: {self.clean_dir}")
        for csv_file in self.clean_dir.rglob("*.csv"):
            if csv_file.name.startswith("._") or "stock_list" in csv_file.name:
                continue
            try:
                df = pd.read_csv(csv_file)
                df['date'] = pd.to_datetime(df['date'])
                id_col = 'stock_code' if 'stock_code' in df.columns else 'stock_id'
                df = df.drop_duplicates(subset=['date', id_col], keep='last')

                # 欄位別名處理
                df = df.rename(columns={
                    'stock_code': 'stock_id',
                    'code': 'stock_id',
                    'stock_name': 'name'
                })

                df['stock_id'] = df['stock_id'].astype(str)
                # 標記來源 e.g., capitalfund_00982A
                source_tag = f"{csv_file.parent.name}_{csv_file.stem}"
                df['etf_source'] = source_tag

                self.all_data.append(df)
            except Exception as e:
                print(f"⚠️ 讀取檔案 {csv_file.name} 發生錯誤: {e}")

    def get_individual_diffs(self):
        """針對每檔 ETF 獨立計算其最後兩個交易日的增減"""
        all_diffs = []

        for df in self.all_data:
            source = df['etf_source'].iloc[0]
            dates = sorted(df['date'].unique())

            if len(dates) < 2:
                print(f"ℹ️ {source} 資料天數不足，跳過比對")
                continue

            latest_t = dates[-1]
            prev_t = dates[-2]
            self.latest_report_dates[source] = latest_t.strftime('%Y-%m-%d')

            # 提取最新與前一次資料
            df_now = df[df['date'] == latest_t].copy()
            df_prev = df[df['date'] == prev_t].copy()

            # 合併比對
            merged = pd.merge(
                df_now, df_prev,
                on=['stock_id', 'name', 'etf_source'],
                how='outer',
                suffixes=('_now', '_prev')
            ).fillna(0)

            # 計算股數變動
            merged['shares_diff'] = merged['shares_now'] - merged['shares_prev']

            # 💡 關鍵：過濾掉沒有變動的，以及賣出的（我們專注於買入共識）
            merged = merged[merged['shares_diff'] > 0]

            # 標記是否為「新進榜」
            merged['action'] = np.where(merged['shares_prev'] == 0, "🆕新買入", "📈增持")

            all_diffs.append(merged)

        return pd.concat(all_diffs, ignore_index=True) if all_diffs else pd.DataFrame()

    def generate_ai_prompt(self, diff_df):
            """產出餵給 Gemini 的深度分析 Prompt (強化客觀、全面與查證紀律)"""

            # 1. 處理共識標的 (Consensus)
            consensus = diff_df.groupby(['stock_id', 'name']).agg({
                'etf_source': 'count',
                'shares_diff': 'sum',
                'weight_now': 'mean',
                'action': lambda x: "/".join(set(x))
            }).rename(columns={'etf_source': '投信家數', 'shares_diff': '總加碼股數',
                               'weight_now': '平均權重(%)'}).sort_values('投信家數', ascending=False)

            consensus_table = consensus[consensus['投信家數'] > 1].reset_index()

            # 2. 處理黑馬標的 (Top Buys)
            dark_horses = diff_df.sort_values('shares_diff', ascending=False).head(15)
            # 欄位中文化與美化
            dark_horses_table = dark_horses[
                ['stock_id', 'name', 'etf_source', 'action', 'shares_diff', 'weight_now']].rename(
                columns={
                    'stock_id': '代號', 'name': '名稱', 'etf_source': 'ETF來源',
                    'action': '動作', 'shares_diff': '加碼股數', 'weight_now': '當前權重(%)'
                }
            )

            # 3. 組裝日期 Metadata
            date_meta = "\n".join([f"- {k}: {v}" for k, v in self.latest_report_dates.items()])

            # 4. 建立 Markdown (導入強格式與嚴格查證指令)
            prompt = f"""# 📅 台股主動式 ETF 籌碼交叉戰情報表

    ## 📊 第一階段：量化數據基準 (客觀事實)
    **資料庫基準日：**
    {date_meta}

    ### 🎯 投信高度共識標的 (多檔 ETF 同步增持)
    > 說明：下表為跨家投信同時買入的個股。共識度越高，代表法人資金匯聚的客觀事實。
    {consensus_table.to_markdown(index=False) if not consensus_table.empty else "今日暫無多家共識標的。"}

    ### 🚀 單一投信顯著加碼/新進榜黑馬 (Top 15)
    > 說明：下表為各家 ETF 單日加碼張數最顯著的標的。
    {dark_horses_table.to_markdown(index=False) if not dark_horses_table.empty else "無明顯加碼標的。"}

    ---

    ## 🤖 第二階段：AI 深度研究與客觀分析指令

    你是一位具備 20 年經驗的台股量化與基本面操盤手。請基於上方【第一階段】的客觀籌碼數據，執行以下分析任務。

    ⚠️ 【最高指導原則：絕對客觀與真實】⚠️
    1. 引用數據做推理與研究「必須」使用 Google search 進行多方來源交叉查證。
    2. 絕不允許自己產生、捏造或猜測真實數據。
    3. 引用任何數據做呈現或計算，必須在該段落明確附上「來源網站與資料出處」。

    ### 🔍 任務 1：資金板塊與產業綜觀 (Macroscopic View)
    請客觀觀察上方的「共識標的」與「黑馬標的」，歸納出目前投信資金正在流向哪些「具體產業」或「概念板塊」（例如：AI 伺服器、網通、低基期傳產等）。
    * **要求**：請用 1-2 段話精要總結目前的資金輪廓，不可過度發散，僅針對有出現在上方表格的標的進行產業分類歸納。

    ### 🕵️‍♂️ 任務 2：重點標的基本面與事件查證 (Fact-Checking)
    針對「共識標的」清單，以及「黑馬標的」中加碼最顯著的前三名個股，強制使用 Google 搜尋查證近一週內的重大事件。
    * **輸出格式要求**：請以「表格」呈現查證結果，必須包含以下欄位：
      | 股票名稱 | 近期催化劑 (法說會/營收/財報等客觀事實) | 法人/外資動態新聞 | 資料來源 (必須附上 URL 或明確的媒體來源) |

    ### 🎯 任務 3：明日客觀交易觀察清單 (Actionable & Predictable)
    綜合「籌碼共識數據」與「任務 2 的網路查證事實」，客觀篩選出明早開盤最值得關注的 3 檔股票。
    * **輸出格式要求**：請嚴格依照以下格式條列，確保每次輸出的可預測性。
      1. **[股票代號/名稱]**
         * **籌碼面客觀事實**：(如：2家投信共識買入，或單一投信大買 XX 股)
         * **基本面/消息面支撐**：(引用任務 2 查證到的事實，並附註來源)
         * **技術面/型態觀察點**：(若能搜尋到近期股價位階或法人目標價，請客觀列出；若無則寫「無特殊資訊」)
    """
            return prompt

    def run(self):
        print(f"=== 戰情室分析系統啟動: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

        self.load_all_clean_data()
        if not self.all_data:
            print("❌ 錯誤：找不到任何 clean 資料，請先執行爬蟲與 Parser。")
            return

        diff_df = self.get_individual_diffs()

        if not diff_df.empty:
            prompt = self.generate_ai_prompt(diff_df)

            # 儲存到 data 資料夾
            output_path = project_root / "data" / "daily_ai_prompt.txt"
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(prompt)

            print(f"\n✅ 分析報表產出成功！")
            print(f"📍 檔案位置: {output_path}")
            print("-" * 50)
            print("💡 操作提示：請將檔案內容貼給 Gemini 1.5 或 2.0 Pro，讓它開始執行網路查證與研報撰寫。")
            print("-" * 50)
        else:
            print("⚠️ 警告：計算後無任何股數變動資料（可能今日各投信皆未更新股數）。")


if __name__ == "__main__":
    analyzer = StrategyAnalyzer()
    analyzer.run()