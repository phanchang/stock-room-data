import sys
import os
import json
import pandas as pd
from pathlib import Path

# ==========================================
# 0. 系統路徑設定與模組載入
# ==========================================
# 取得專案根目錄
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# 載入專案自帶的歷史股價快取引擎
from utils.cache.manager import CacheManager

try:
    # 匯入 Snapshot 因子運算模組
    from scripts.calc_snapshot_factors import calculate_advanced_factors
except ImportError:
    from calc_snapshot_factors import calculate_advanced_factors

DATA_DIR = Path("data/fundamentals")
cache = CacheManager()


# ==========================================
# 1. 技術面位階狀態機 (完全接軌 Snapshot 因子)
# ==========================================
class TechnicalPhaseClassifier:
    @staticmethod
    def classify_phase(df: pd.DataFrame) -> dict:
        if df is None or len(df) < 60:
            return {"phase": "資料不足", "close": 0, "msg": "K線數量不足"}

        # 呼叫核心函數，取得所有 str_ 開頭的強勢特徵
        factors = calculate_advanced_factors(df)
        if not factors:
            return {"phase": "計算失敗", "close": 0, "msg": "因子運算失敗"}

        breakout_reasons = []
        consolidation_reasons = []

        # 🟢 判斷【剛起漲】 (主力發動的第一根)
        # 1. 爆量突破 30W 均線 (突破 + 量比 >= 1.5)
        if factors.get('str_break_30w', 0) == 1 and factors.get('量比', 0) >= 1.5:
            breakout_reasons.append("🔥爆量突破30W")

        # 2. 30W 黏貼起漲 (黏貼且發生在近2週內)
        if factors.get('str_30w_adh', 0) == 1 and factors.get('str_30w_week_offset', 99) <= 2:
            breakout_reasons.append("🎯30W黏貼起漲")

        # 3. 甩轎翻紅 (甩轎且發生在近2週內)
        if factors.get('str_30w_shk', 0) == 1 and factors.get('str_30w_week_offset', 99) <= 2:
            breakout_reasons.append("🚀甩轎翻紅")

        # 🔵 判斷【底部盤整】 (真正的左側佈局，尚未發動)
        if factors.get('str_consol_60', 0) == 1:
            consolidation_reasons.append("60日極致收斂")
        elif factors.get('str_consol_20', 0) == 1:
            consolidation_reasons.append("20日短線收斂")

        # 🎯 狀態機嚴格分發
        if breakout_reasons:
            phase = f"剛起漲 ({' + '.join(breakout_reasons)})"
        elif consolidation_reasons:
            phase = f"底部盤整 ({' + '.join(consolidation_reasons)})"
        elif factors.get('漲幅60d', 0) > 30:
            phase = "高檔已反映 (避開)"
        elif factors.get('str_uptrend', 0) == 1:
            phase = "多頭行進中 (沿均線上漲)"
        else:
            phase = "左側接刀/空頭區間 (避開)"

        return {
            "phase": phase,
            "close": factors.get('現價', 0),
            "msg": f"量比:{factors.get('量比', 0):.2f}, 60d漲幅:{factors.get('漲幅60d', 0):.1f}%"
        }


# ==========================================
# 2. 基本面與籌碼面策略庫
# ==========================================
class FundamentalStrategies:
    @staticmethod
    def check_contract_liability_growth(data: dict) -> tuple:
        """ 策略一：合約負債連續兩季成長 (隱形訂單爆發) """
        bs = data.get('balance_sheet', [])
        if len(bs) < 3: return False, "資料不足3季"
        q1, q2, q3 = bs[0].get('contract_liab', 0), bs[1].get('contract_liab', 0), bs[2].get('contract_liab', 0)

        if not q3 or not q2 or not q1 or q3 == 0: return False, "無有效的數據"
        if q1 > q2 and q2 > q3:
            return True, f"合約負債半年激增 {((q1 - q3) / q3) * 100:.1f}% (Q3:{q3} -> Q1:{q1})"
        return False, ""

    @staticmethod
    def check_chips_divergence(data: dict, window: int = 20) -> tuple:
        """ 策略二：極致左側籌碼背離 (散戶退場，法人吃貨) """
        history = data.get('chips_history', [])
        if not history or len(history) < 2: return False, ""

        recent = history[-window:]
        margin_diff = recent[-1].get('margin_balance_pct', 0) - recent[0].get('margin_balance_pct', 0)
        legal_diff = recent[-1].get('total_legal_pct', 0) - recent[0].get('total_legal_pct', 0)

        if margin_diff < 0 and legal_diff > 0:
            return True, f"近 {len(recent)} 日極致背離：融資退場 {margin_diff:.2f}%, 法人大買 {legal_diff:.2f}%"
        return False, ""

    @staticmethod
    def check_inventory_turnaround(data: dict) -> tuple:
        """ 策略三：存貨去化拐點 (景氣循環谷底擒龍) """
        bs = data.get('balance_sheet', [])
        cf = data.get('cash_flow', [])
        if len(bs) < 3 or not cf: return False, "資料不足"

        q1_inv, q2_inv, q3_inv = bs[0].get('inventory', 0), bs[1].get('inventory', 0), bs[2].get('inventory', 0)
        q1_ocf = cf[0].get('op_cash_flow', 0)

        # 核心邏輯：存貨連兩降 + 最新一季現金流轉正
        if q3_inv != 0 and q1_inv < q2_inv < q3_inv and q1_ocf > 0:
            decrease_pct = ((q3_inv - q1_inv) / q3_inv) * 100
            msg = f"存貨連兩季下降(去化達 {decrease_pct:.1f}%)，且單季現金流轉正({q1_ocf})"
            return True, msg
        return False, ""


# ==========================================
# 3. 執行引擎主程式
# ==========================================
def load_price_data(sid: str) -> pd.DataFrame:
    df = cache.load(f"{sid}.TW")
    if df is not None and not df.empty: return df
    df = cache.load(f"{sid}.TWO")
    if df is not None and not df.empty: return df
    return pd.DataFrame()


def run_screener():
    print("🚀 啟動 StockWarRoom AI 終極雙重濾網引擎...\n")
    candidates = []

    for file_path in DATA_DIR.glob("*.json"):
        sid = file_path.stem
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                stock_data = json.load(f)
        except Exception:
            continue

        # 🟢 第一層：基本面/籌碼面 (任中其一即入選)
        matched_reasons = []
        is_f1, msg_f1 = FundamentalStrategies.check_contract_liability_growth(stock_data)
        if is_f1: matched_reasons.append(f"📦 隱形大單: {msg_f1}")

        is_f2, msg_f2 = FundamentalStrategies.check_chips_divergence(stock_data)
        if is_f2: matched_reasons.append(f"🕵️ 主力吃貨: {msg_f2}")

        is_f3, msg_f3 = FundamentalStrategies.check_inventory_turnaround(stock_data)
        if is_f3: matched_reasons.append(f"📉 谷底擒龍: {msg_f3}")

        if not matched_reasons: continue

        # 🔵 第二層：技術面狀態機
        df_price = load_price_data(sid)
        tech_info = TechnicalPhaseClassifier.classify_phase(df_price)
        phase = tech_info['phase']

        # ⛔ 剔除危險標的
        if "避開" in phase or "計算失敗" in phase or "資料不足" in phase:
            print(f"⏩ 【{sid}】具備基本面利多，但技術面為【{phase}】，自動剔除。")
            continue

        candidates.append({
            "sid": sid,
            "reasons": matched_reasons,
            "tech_info": tech_info
        })

    print(f"\n🎯 篩選完成！共精選出 {len(candidates)} 檔具備基本面利多且位階絕佳的黃金標的：\n" + "-" * 60)

    for stock in candidates:
        sid = stock['sid']
        t_info = stock['tech_info']
        reasons_str = "\n   ".join(stock['reasons'])

        print(f"🔸 【{sid}】")
        print(f"   {reasons_str}")
        print(f"   📈 技術位階: {t_info['phase']} ({t_info['msg']}, 現價: {t_info['close']})")

        ai_prompt = (
            f"請以專業證券分析師的角度，分析台股代號 {sid}。\n"
            f"系統偵測到該股出現以下極佳的先行指標：\n"
            f"   {reasons_str}\n"
            f"且技術面目前處於「{t_info['phase']}」，位階剛好在絕佳買點。\n"
            f"請查閱該公司近期的法說會與產業新聞，分析主力持續吃貨或基本面轉機的潛在利多為何？並評估此時佈局的風險。"
        )
        print(f"   🤖 AI 驗證 Prompt:\n   {ai_prompt}\n" + "-" * 60)


if __name__ == "__main__":
    run_screener()