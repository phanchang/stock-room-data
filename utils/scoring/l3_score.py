# 檔案路徑: utils/scoring/l3_score.py
import pandas as pd
from utils.scoring.trend_factors import TrendScorer


class L3Scorer:
    @staticmethod
    def calculate_score(rs_score: float, fundamental_data: dict, kline_df: pd.DataFrame, tags: str,
                        boll_width: float) -> dict:
        """
        計算 L3 個股綜合評分與黑馬標籤
        滿分 100 分 = RS(30) + 基本面(25) + 籌碼與成本(30) + 技術型態(15)
        """
        # 1. 基礎動能 RS (30分)
        s_rs = round(min(30.0, (rs_score / 100) * 30), 1)

        # 2. 基本面爆發 (25分)
        rev_data = fundamental_data.get('revenue', [])
        prof_data = fundamental_data.get('profitability', [])

        rev_res = TrendScorer.calc_revenue_momentum(rev_data)
        prof_res = TrendScorer.calc_profit_purity(prof_data)

        s_fund = 0
        if rev_res['score'] == 25:
            s_fund += 20
        elif rev_res['score'] == 15:
            s_fund += 10

        if prof_res['score'] == 15: s_fund += 5

        # 3. 籌碼與成本優勢 (30分)
        inst_data = fundamental_data.get('institutional_investors', [])
        vwap_res = TrendScorer.calc_institutional_vwap(inst_data, kline_df)

        s_chip = 0
        if vwap_res['status'] == "✅ 安全建倉區":
            s_chip = 30
        elif vwap_res['status'] == "⚠️ 乖離過熱 (結帳風險)":
            s_chip = 0  # 乖離過熱絕對不給分，卡死追高風險
        elif "淨賣超" in vwap_res['status']:
            s_chip = 0  # 均價轉壓力，不給分

        # 4. 技術型態與布林 (15分)
        s_tech = 0
        tags_lower = str(tags).lower()
        if '30w' in tags_lower or '創季高' in tags_lower:
            s_tech += 8

        # 布林甜蜜帶：20%~60% 剛發動最好，大於 80% 代表過熱
        try:
            bw = float(boll_width)
            if 20 <= bw <= 60:
                s_tech += 7
            elif bw > 80:
                s_tech = 0
        except (ValueError, TypeError):
            pass

        total_score = round(s_rs + s_fund + s_chip + s_tech, 1)

        # 5. 黑馬 4 條件判定 (嚴格篩選)
        is_dark_horse = (
                (rs_score >= 85) and
                (vwap_res['status'] == "✅ 安全建倉區") and
                ('30w' in tags_lower) and
                (prof_res['score'] > 0)  # 排除狂炒作但本業不賺錢的紙上富貴
        )

        return {
            "L3Score": total_score,
            "components": {
                "rs_score": s_rs,
                "fundamental_score": s_fund,
                "chip_score": s_chip,
                "tech_score": s_tech
            },
            "vwap_info": vwap_res,
            "rev_info": rev_res,  # 🔥 新增這行：把營收明細傳出來
            "prof_info": prof_res,  # 🔥 新增這行：把獲利明細傳出來
            "is_dark_horse": is_dark_horse
        }