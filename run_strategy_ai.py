import pandas as pd
import json
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')


class StockStrategyEngine:
    def __init__(self, snapshot_path="data/strategy_results/factor_snapshot.parquet",
                 fundamentals_dir="data/fundamentals"):
        """
        初始化 AI 選股引擎
        :param snapshot_path: 修正為真正有 60 項特徵的 parquet 快照表
        :param fundamentals_dir: 深度基本面 JSON 庫的路徑
        """
        self.snapshot_path = snapshot_path
        self.fundamentals_dir = Path(fundamentals_dir)
        self.df_snapshot = self.load_snapshot()

    def load_snapshot(self):
        """讀取資料源 A：快照大表 (Parquet格式)"""
        path = Path(self.snapshot_path)
        if path.exists():
            print(f"[系統] 成功讀取快照大表: {self.snapshot_path}")
            # 支援讀取 parquet 格式
            if path.suffix == '.parquet':
                return pd.read_parquet(path)
            else:
                return pd.read_csv(path, dtype={'sid': str})
        else:
            print(f"[警告] 找不到快照大表: {self.snapshot_path}")
            return pd.DataFrame()

    def load_json(self, sid):
        """讀取資料源 B：個股深度 JSON"""
        json_path = self.fundamentals_dir / f"{sid}.json"
        if json_path.exists():
            with open(json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None

    # ==========================================
    # 技術面策略模組 (讀取 Parquet Snapshot 大表)
    # ==========================================
    def tech_break_30w(self, row):
        return row.get('str_break_30w', 0) == 1 and row.get('量比', 0) > 1.5

    def tech_adh_30w(self, row):
        return row.get('str_30w_adh', 0) == 1 and row.get('str_30w_week_offset', 999) <= 2

    def tech_shk_30w(self, row):
        return row.get('str_30w_shk', 0) == 1 and row.get('str_30w_week_offset', 999) <= 2

    # ==========================================
    # 優化版：技術面策略模組
    # ==========================================
    def tech_consolidation(self, row):
        """
        底部盤整：布林通道極致壓縮
        優化：不僅要滿足 20或60日收斂，我們加上布林帶寬(bb_width)的雙重確認 (假設小於15%)
        """
        is_consol = row.get('str_consol_60', 0) == 1 or row.get('str_consol_20', 0) == 1
        bb_narrow = row.get('bb_width', 999) < 15.0  # 確保帶寬真的夠窄，沒有被突然放大的雜訊干擾
        return is_consol and bb_narrow

    # ==========================================
    def fund_contract_liab(self, json_data):
        """
        基本面轉機：合約負債 連兩季成長
        優化：不僅要成長，每季至少要有 5% 以上的實質增幅，排除會計微調雜訊。
        """
        bs = json_data.get('balance_sheet', [])
        if len(bs) >= 3:
            try:
                cl_0 = float(bs[0].get('contract_liab', 0))
                cl_1 = float(bs[1].get('contract_liab', 0))
                cl_2 = float(bs[2].get('contract_liab', 0))
                # 連兩季成長，且增幅大於 5%
                return (cl_0 > cl_1 * 1.05) and (cl_1 > cl_2 * 1.05)
            except Exception:
                pass
        return False

    def fund_inventory(self, json_data):
        """
        基本面轉機：庫存連兩降 且 最新營業現金流 > 0
        優化：庫存每季至少要消化下降 3% 以上，代表是真實的去化。
        """
        bs = json_data.get('balance_sheet', [])
        cf = json_data.get('cash_flow', [])

        inv_cond = False
        if len(bs) >= 3:
            try:
                inv_0 = float(bs[0].get('inventory', 0))
                inv_1 = float(bs[1].get('inventory', 0))
                inv_2 = float(bs[2].get('inventory', 0))
                # 庫存下降至少 3%
                inv_cond = (inv_0 < inv_1 * 0.97) and (inv_1 < inv_2 * 0.97)
            except Exception:
                pass

        ocf_cond = False
        if len(cf) > 0:
            try:
                ocf_cond = float(cf[0].get('op_cash_flow', 0)) > 0
            except Exception:
                pass
        else:
            ocf_cond = True

        return inv_cond and ocf_cond

    def chips_divergence(self, json_data):
        """
        極致籌碼面：近20日 融資降 + 法人升
        優化：加上實質門檻。融資至少要退場 0.5%，法人至少要進場 0.5%，才有「換手」意義。
        """
        margin_data = json_data.get('margin_trading', [])
        inst_data = json_data.get('institutional_investors', [])

        if len(margin_data) >= 20 and len(inst_data) >= 20:
            try:
                mb_latest = float(margin_data[0].get('fin_usage', 0))
                mb_past = float(margin_data[19].get('fin_usage', 0))

                tl_latest = float(inst_data[0].get('total_legal_pct', 0))
                tl_past = float(inst_data[19].get('total_legal_pct', 0))

                mb_diff = mb_past - mb_latest  # 融資下降幅度
                tl_diff = tl_latest - tl_past  # 法人上升幅度

                # 融資降幅 > 0.5% 且 法人持股增幅 > 0.5%
                return (mb_diff >= 0.5) and (tl_diff >= 0.5)
            except Exception:
                pass
        return False
    # ==========================================
    # 核心交集執行邏輯
    # ==========================================
    def run_strategy(self):
        if self.df_snapshot.empty:
            return []

        results = []
        print("[系統] 開始進行多因子交集比對...")

        for _, row in self.df_snapshot.iterrows():
            sid = str(row['sid'])
            name = row['name']

            # 1. 乖離過大過濾
            if row.get('漲幅60d', 0) > 30:
                continue

            # 2. 技術面判斷 (至少符合一項)
            tech_matches = []
            if self.tech_break_30w(row): tech_matches.append('剛起漲(突破)')
            if self.tech_adh_30w(row): tech_matches.append('剛起漲(黏貼)')
            if self.tech_shk_30w(row): tech_matches.append('剛起漲(甩轎)')
            if self.tech_consolidation(row): tech_matches.append('底部盤整')

            if not tech_matches:
                continue

                # 3. 讀取 JSON 並進行基本/籌碼面判斷 (至少符合一項)
            json_data = self.load_json(sid)
            if not json_data:
                continue

            fund_matches = []
            if self.fund_contract_liab(json_data): fund_matches.append('合約負債連增(隱形大單)')
            if self.fund_inventory(json_data): fund_matches.append('庫存去化且現金流為正')
            if self.chips_divergence(json_data): fund_matches.append('籌碼背離(散戶退法人進)')

            if not fund_matches:
                continue

                # 4. 成功交集：加入結果
            results.append({
                'sid': sid,
                'name': name,
                'tech_reasons': tech_matches,
                'fund_reasons': fund_matches
            })

        print(f"[系統] 比對完成，共篩選出 {len(results)} 檔高潛力個股。")
        return results

    def generate_gemini_prompt(self, results):
        if not results:
            return "目前沒有符合雙重交集條件的股票。"

        prompt = "請扮演專業台股分析師。我有一份透過AI策略引擎嚴選出的潛力股名單，均符合「技術面剛起漲或盤整」，且「基本面/籌碼面具備轉機利多」。\n"
        prompt += "請針對以下股票，利用Google Search查閱最新法說會資訊、新聞報導，並多方交叉查證其「實質利多」與「潛在風險」：\n\n"

        for item in results:
            tech_str = "、".join(item['tech_reasons'])
            fund_str = "、".join(item['fund_reasons'])
            prompt += f"【{item['sid']} {item['name']}】\n"
            prompt += f"🔸 技術面訊號：{tech_str}\n"
            prompt += f"🔸 基本/籌碼利多：{fund_str}\n\n"

        prompt += "回覆要求：請以條理清晰的方式，針對每檔股票列出：\n"
        prompt += "1. 最新基本面動態 (訂單、庫存去化、資本支出狀況)\n"
        prompt += "2. 法說會最新展望與法人觀點 (請附上資料來源網站)\n"
        prompt += "3. 操作上需留意的潛在風險\n"

        return prompt


if __name__ == "__main__":
    # 建立引擎實例
    engine = StockStrategyEngine(
        snapshot_path="data/strategy_results/factor_snapshot.parquet",
        fundamentals_dir="data/fundamentals"
    )

    # 執行策略
    selected_stocks = engine.run_strategy()

    if selected_stocks:
        print(f"\n🏆 【AI 策略引擎初選名單】 共 {len(selected_stocks)} 檔 🏆")
        print("=" * 60)

        # 建立分類字典，將股票依照「策略組合」歸類
        from collections import defaultdict

        strategy_groups = defaultdict(list)

        for stock in selected_stocks:
            # 將該檔股票符合的所有技術與基本面進行交叉配對
            for tech in stock['tech_reasons']:
                for fund in stock['fund_reasons']:
                    group_name = f"{tech} ＋ {fund}"
                    strategy_groups[group_name].append(f"{stock['sid']} {stock['name']}")

        # 依照策略群組印出
        for group_name, stock_list in sorted(strategy_groups.items()):
            print(f"▼ 【策略組合：{group_name}】 (共 {len(stock_list)} 檔)")

            # 每 5 檔換一行，方便閱讀
            for i in range(0, len(stock_list), 5):
                print("  " + "、".join(stock_list[i:i + 5]))
            print("-" * 60)

        # 產生送給 Gemini 的 Prompt (依然維持個股結構，以利 AI 查證)
        #print("\n🤖 【自動生成 AI 查證 Prompt】 🤖")
        prompt = engine.generate_gemini_prompt(selected_stocks)
        #print(prompt)