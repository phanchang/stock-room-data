import json
import pandas as pd
import numpy as np
import traceback
import os
import re
import collections
from pathlib import Path
from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                             QFrame, QGridLayout, QProgressBar, QScrollArea, QWidget, QPushButton)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from dotenv import load_dotenv

load_dotenv()


class LLMWorker(QThread):
    finished = pyqtSignal(str)
    error = pyqtSignal(str)
    chunk_received = pyqtSignal(str)

    def __init__(self, prompt):
        super().__init__()
        self.prompt = prompt
        self.api_key = os.getenv("GEMINI_API_KEY")
        self.http_proxy = os.getenv("HTTP_PROXY", os.getenv("http_proxy"))
        self.https_proxy = os.getenv("HTTPS_PROXY", os.getenv("https_proxy"))

    def run(self):
        if not self.api_key:
            self.error.emit("❌ 找不到 GEMINI_API_KEY。請檢查 .env 檔案設定。")
            return

        try:
            if self.http_proxy:
                os.environ['http_proxy'] = self.http_proxy
                os.environ['HTTP_PROXY'] = self.http_proxy
            if self.https_proxy:
                os.environ['https_proxy'] = self.https_proxy
                os.environ['HTTPS_PROXY'] = self.https_proxy

            try:
                from google import genai
            except ImportError:
                self.error.emit("❌ 缺少新版套件！請在終端機執行: pip install google-genai")
                return

            client = genai.Client(api_key=self.api_key)

            models_to_try = [
                'gemini-2.5-flash',
                'gemini-2.0-flash',
                'gemini-2.0-flash-lite',
                'gemini-1.5-flash'
            ]

            for model_name in models_to_try:
                try:
                    print(f"[系統] 嘗試使用最新架構呼叫模型: {model_name} ...", end=" ")

                    response_stream = client.models.generate_content_stream(
                        model=model_name,
                        contents=self.prompt
                    )

                    print("✅ 成功！開始串流輸出...")
                    self.chunk_received.emit(f"*(⚡ 成功突破限制！本次選用模型: {model_name})*\n\n")

                    full_text = ""
                    for chunk in response_stream:
                        if chunk.text:
                            full_text += chunk.text
                            self.chunk_received.emit(chunk.text)

                    self.finished.emit(full_text)
                    return

                except Exception as e:
                    print(f"❌ 失敗: {e}")
                    continue

            self.error.emit(
                "❌ 所有模型均無免費額度或連線失敗。\n\n"
                "Google 系統判定你這個 Google 帳號完全不享有免費 API 權限，或是 Proxy 阻擋了所有連線。\n"
                "💡 最終解法：請登出目前的 Google 帳號，使用一個全新的「個人 @gmail.com」重新申請 API 金鑰。"
            )

        except Exception as e:
            self.error.emit(f"❌ API 系統錯誤：\n{str(e)[:300]}")


class StockInsightDashboard(QDialog):
    def __init__(self, sid, row_data, full_df, parent=None):
        super().__init__(parent)
        self.sid = sid
        self.row_data = row_data
        self.full_df = full_df
        self.json_data = self._load_json_data(sid)

        self.setWindowModality(Qt.WindowModality.NonModal)
        self.setModal(False)
        self.setWindowFlags(self.windowFlags() |
                            Qt.WindowType.WindowMinimizeButtonHint |
                            Qt.WindowType.WindowMaximizeButtonHint)

        self.llm_font_size = 15
        self.llm_raw_text = "點擊右上方按鈕，將彙整四大象限與深度財報數據，發送給 AI 進行綜合推演..."

        name = self.row_data.get('name', self.row_data.get('股票名稱', ''))
        self.setWindowTitle(f"🔍 {sid} {name} - 操盤手終極決策看板 V8.9")
        self.setMinimumSize(1250, 950)

        self.setStyleSheet("""
            QDialog { background-color: #0B0E14; }
            QLabel { font-family: 'Microsoft JhengHei'; color: #E0E6ED; }
            .card { background-color: #151A22; border: 1px solid #2B3544; border-radius: 10px; padding: 15px; }
            .title { color: #00E5FF; font-size: 18px; font-weight: bold; border-bottom: 1px solid #2B3544; padding-bottom: 8px; margin-bottom: 12px; }
            .highlight-box { background-color: #1E2632; border-radius: 6px; padding: 8px; margin-bottom: 4px; }
        """)
        self.init_ui()

    def _change_llm_font_size(self, delta):
        self.llm_font_size += delta
        self.llm_font_size = max(10, min(self.llm_font_size, 32))
        self._update_llm_text_display()

    def _update_llm_text_display(self):
        text = self.llm_raw_text
        formatted_text = re.sub(r'### (.*?)\n',
                                f'<br><span style="color: #00E5FF; font-size: {self.llm_font_size + 2}px; font-weight: bold;">\\1</span><br>',
                                text)
        formatted_text = re.sub(r'\*\*(.*?)\*\*', r'<span style="color: #FFD700; font-weight: bold;">\1</span>',
                                formatted_text)
        formatted_text = formatted_text.replace('\n', '<br>')
        formatted_text = formatted_text.replace('```markdown', '').replace('```', '')

        self.lbl_llm_result.setStyleSheet(f"font-size: {self.llm_font_size}px; color: #FFFFFF; line-height: 1.6;")
        self.lbl_llm_result.setText(formatted_text)

    def _trigger_llm_analysis(self, m):
        self.btn_ask_ai.setEnabled(False)
        self.btn_ask_ai.setText("⏳ AI 深度推演中...")
        self.llm_raw_text = "🧠 正在讀取 JSON 深度財報（三率、合約負債）與土洋籌碼陣列，重新建模中...\n\n"
        self._update_llm_text_display()

        name = self.row_data.get('name', self.row_data.get('股票名稱', ''))
        tags = str(self.row_data.get('強勢特徵標籤', self.row_data.get('強勢特徵', '')))

        rev_list = self.json_data.get('revenue', [])[:6]
        raw_rev_str = "\n".join(
            [f"   - {r.get('month', '')}: 單月 YoY {r.get('rev_yoy', 0)}% | 累計 YoY {r.get('rev_cum_yoy', 0)}%" for r
             in rev_list]) if rev_list else "   - 無近期營收明細"

        prof_list = self.json_data.get('profitability', [])[:4]
        raw_eps_str = "\n".join([
                                    f"   - {p.get('quarter', '')}: EPS {p.get('eps', 0)}元 | 毛利率 {p.get('gross_margin', 0)}% | 營益率 {p.get('op_margin', 0)}% | 淨利率 {p.get('net_margin', 0)}%"
                                    for p in prof_list]) if prof_list else "   - 無近期財報明細"

        bs_list = self.json_data.get('balance_sheet', [])[:4]
        raw_bs_str = "\n".join([
                                   f"   - {b.get('quarter', '')}: 合約負債 {b.get('contract_liab', 0):,} 千元 | 存貨 {b.get('inventory', 0):,} 千元"
                                   for b in bs_list]) if bs_list else "   - 無近期合約負債/存貨明細"

        inst_list = self.json_data.get('institutional_investors', [])[:5]
        margin_list = {m['date']: m for m in self.json_data.get('margin_trading', [])[:5]}
        raw_inst_str = ""
        if inst_list:
            for d in inst_list:
                date = d.get('date', '')
                f_buy = d.get('foreign_buy_sell', 0)
                t_buy = d.get('invest_trust_buy_sell', 0)
                l_pct = d.get('total_legal_pct', 0)
                m_change = margin_list.get(date, {}).get('fin_change', 0)
                raw_inst_str += f"   - {date}: 外資 {f_buy:,} 張 | 投信 {t_buy:,} 張 | 融資(散戶) {m_change:,} 張 | 法人總持股 {l_pct}%\n"
        else:
            raw_inst_str = "   - 無近期籌碼明細"

        prompt = f"""
你是一位冷靜、客觀且擁有 20 年台股實戰經驗的「頂級機構操盤手與產業分析師」。
我將提供一檔股票的【基本資料】與【近期的深度原始數據(Raw Data)】。
請解讀數據背後的故事，並用 Markdown 格式輸出。

【⚠️ 輸出格式與排版要求】
請將你認為「最關鍵的數據轉折」、「最核心的結論」或「強烈的風險/機會提示」，使用 `**` 包起來（例如：`**毛利率連續三季攀升**` 或 `**投信連買但散戶大退場**`）。
系統前端會自動將 `**` 包起來的文字渲染為「亮黃色醒目提示」。請盡情在重要的洞察上使用這個語法！

請包含以下四大板塊：

### 1. 🏢 企業護城河與產業地位
- 調用你的內部知識，這家公司 ({self.sid} {name}) 的核心業務是什麼？
- 它的競爭優勢(護城河)或近期市場題材在哪裡？

### 2. 📊 基本面深度解碼
- **營收軌跡**：從近6個月的 YoY 來看，業績是「加速爆發」、「穩健」、「觸底反彈」還是「衰退」？
- **三率與 EPS**：毛利率與營益率有出現結構性改善嗎？
- **領先指標**：合約負債(未來營收)或存貨水位的變化，暗示了未來一兩季的什麼狀況？

### 3. 🏦 籌碼對決與技術面共振
- **土洋與散戶心理**：從近 5 日「外資/投信/融資」的每日買賣來看，主力是在「堅定吸籌」、「土洋對作」還是「倒貨給散戶」？
- 配合目前的技術面均線與布林帶寬，有沒有出現「型態與籌碼」的完美共振？

### 4. ⚔️ 操盤手實戰劇本推演
- 綜合上述，當前價位的「風險報酬比」如何？
- 給出具體的應對策略：目前適合進場嗎？防守點位或出場訊號該看什麼？

---
【輸入資料區】
- 代號名稱：{self.sid} {name} 
- 綜合戰力：{m['power_score']} 分 / 均線狀態：{m['ma_status']} / 創高狀態：{m['high_status']}
{m.get('industry_text', '')}
{m.get('catalyst_text', '')}
- 布林帶寬：{self.get_val('布林寬度(%)', 'bb_width', 100):.1f}%
- 系統自動特徵：{tags if tags else '無'}

【近 6 個月營收動能 (近->遠)】
{raw_rev_str}

【近 4 季獲利品質 (近->遠)】
{raw_eps_str}

【近 4 季領先指標 (近->遠)】
{raw_bs_str}

【近 5 日土洋與散戶籌碼對決 (近->遠)】
{raw_inst_str}
"""
        self.llm_worker = LLMWorker(prompt)
        self.llm_worker.chunk_received.connect(self._on_llm_chunk)
        self.llm_worker.finished.connect(self._on_llm_success)
        self.llm_worker.error.connect(self._on_llm_error)

        self.llm_raw_text = ""
        self.llm_worker.start()

    def _on_llm_chunk(self, chunk_text):
        self.llm_raw_text += chunk_text
        self._update_llm_text_display()

    def _on_llm_success(self, full_text):
        self.btn_ask_ai.setEnabled(True)
        self.btn_ask_ai.setText("🚀 重新呼叫 AI 推演")
        self._update_llm_text_display()

    def _on_llm_error(self, err_msg):
        self.btn_ask_ai.setEnabled(True)
        self.btn_ask_ai.setText("❌ 重試 API 請求")
        self.lbl_llm_result.setStyleSheet(f"font-size: {self.llm_font_size}px; color: #FF4D4D; line-height: 1.6;")
        self.lbl_llm_result.setText(err_msg)

    def _load_json_data(self, sid):
        path = Path(__file__).resolve().parent.parent / "data" / "fundamentals" / f"{sid}.json"
        if path.exists():
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {}

    def get_val(self, key_tw, key_en, default=0.0):
        val = self.row_data.get(key_tw, self.row_data.get(key_en, default))
        try:
            return float(val) if val not in ['', None, 'N/A', 'nan'] else default
        except:
            return default

    def calculate_final_metrics(self):
        m = {
            "power_score": 0, "ind_rank": "N/A", "ind_name": "未分類",
            "inst_cost": 0.0, "inst_dist_pct": 0.0,
            "vcp_label": "", "rev_high_cnt": 0, "eps_high_cnt": 0,
            "ma_status": "均線糾結或空頭", "is_bull_ma": False,
            "high_status": "未達一年新高", "high_color": "#FFFFFF",
            "st_status": "計算中...", "st_color": "#E0E6ED",
            "w30_status": "無 30W 突破訊號", "w30_color": "#E0E6ED",
            "current_price": self.get_val('今日收盤價', '現價'),
            "real_eps_yoy": 0.0,
            "real_rev_yoy": self.get_val('營收YoY(%)', 'rev_yoy'),
            "real_ld_5": 0.0, "real_ld_20": 0.0, "real_ld_60": 0.0,
            "acc_eps_year": "", "acc_eps_val": 0.0, "acc_eps_yoy": None, "acc_eps_qs": 0,
            "industry_text": "🏭 同業地位：(無細產業資料)",
            "catalyst_text": "🔥 資金風口：(無明顯題材)"
        }

        df_k = None
        base_path = Path(__file__).resolve().parent.parent
        tw_path = base_path / "data" / "cache" / "tw" / f"{self.sid}_TW.parquet"
        two_path = base_path / "data" / "cache" / "tw" / f"{self.sid}_TWO.parquet"

        try:
            if tw_path.exists():
                df_k = pd.read_parquet(tw_path)
            elif two_path.exists():
                df_k = pd.read_parquet(two_path)

            if df_k is not None and not df_k.empty:
                if 'Date' in df_k.columns:
                    if str(df_k['Date'].dtype) in ['int64', 'float64']:
                        df_k['Date'] = pd.to_datetime(df_k['Date'], unit='ms')
                    else:
                        df_k['Date'] = pd.to_datetime(df_k['Date'])
                    df_k.set_index('Date', inplace=True)
                df_k = df_k.sort_index()

                for col in ['close', 'high', 'low', 'open']:
                    if col in df_k.columns: df_k[col] = pd.to_numeric(df_k[col], errors='coerce')

                cur_p = df_k['close'].iloc[-1]
                cur_h = df_k['high'].iloc[-1]

                ma55 = df_k['close'].rolling(55).mean().iloc[-1]
                ma150 = df_k['close'].rolling(150).mean().iloc[-1]
                ma200 = df_k['close'].rolling(200).mean().iloc[-1]
                m['is_bull_ma'] = (cur_p > ma55) and (ma55 > ma150) and (ma150 > ma200)
                m['ma_status'] = "🚀 完美多頭 (C > 55 > 150 > 200)" if m['is_bull_ma'] else "⚠️ 非完美多頭或盤整"

                if len(df_k) >= 250:
                    past_250_max = df_k['high'].tail(250).max()
                    if cur_h >= past_250_max:
                        m['high_status'] = "🔥 今日創 250 日 (一年) 新高"
                        m['high_color'] = "#FF4D4D"
                    elif cur_p >= past_250_max * 0.97:
                        m['high_status'] = "📈 逼近一年高點 (差距 < 3%)"
                        m['high_color'] = "#FFD700"
                    else:
                        dist = ((past_250_max - cur_p) / past_250_max) * 100
                        m['high_status'] = f"距 250 日高點還差 {dist:.1f}%"

                try:
                    logic = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'}
                    df_w = df_k.resample('W-FRI').agg({k: v for k, v in logic.items() if k in df_k.columns}).dropna()

                    if len(df_w) >= 15:
                        high, low, close = df_w['high'], df_w['low'], df_w['close']
                        tr1 = high - low
                        tr2 = (high - close.shift(1)).abs()
                        tr3 = (low - close.shift(1)).abs()
                        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                        atr = tr.rolling(10).mean()

                        hl2 = (high + low) / 2
                        bu_vals = hl2 + (3.0 * atr)
                        bl_vals = hl2 - (3.0 * atr)
                        c_vals = close.values

                        n = len(df_w)
                        final_upper = np.zeros(n)
                        final_lower = np.zeros(n)
                        direction = np.ones(n)

                        start_idx = 9
                        final_upper[start_idx] = bu_vals.iloc[start_idx]
                        final_lower[start_idx] = bl_vals.iloc[start_idx]

                        for i in range(start_idx + 1, n):
                            if bu_vals.iloc[i] < final_upper[i - 1] or c_vals[i - 1] > final_upper[i - 1]:
                                final_upper[i] = bu_vals.iloc[i]
                            else:
                                final_upper[i] = final_upper[i - 1]

                            if bl_vals.iloc[i] > final_lower[i - 1] or c_vals[i - 1] < final_lower[i - 1]:
                                final_lower[i] = bl_vals.iloc[i]
                            else:
                                final_lower[i] = final_lower[i - 1]

                            if direction[i - 1] == 1:
                                if c_vals[i] < final_lower[i]:
                                    direction[i] = -1
                                else:
                                    direction[i] = 1
                            else:
                                if c_vals[i] > final_upper[i]:
                                    direction[i] = 1
                                else:
                                    direction[i] = -1

                        curr_dir = direction[-1]
                        last_signal_idx = -1

                        for i in range(n - 1, start_idx, -1):
                            if direction[i] != direction[i - 1]:
                                last_signal_idx = i
                                break

                        if last_signal_idx == -1:
                            last_signal_idx = start_idx
                            weeks_ago = (n - 1) - start_idx
                            past_price = c_vals[start_idx]
                            ret = ((cur_p - past_price) / past_price) * 100 if past_price > 0 else 0
                            prefix = f"超過 {weeks_ago}"
                        else:
                            weeks_ago = (n - 1) - last_signal_idx
                            past_price = c_vals[last_signal_idx]
                            ret = ((cur_p - past_price) / past_price) * 100 if past_price > 0 else 0
                            prefix = f"維持 {weeks_ago}"

                        if curr_dir == 1:
                            m[
                                'st_status'] = f"🔴 Buy 多方 ({prefix} 週) | 報酬: > {ret:+.1f}%" if "超過" in prefix else f"🔴 Buy 多方 ({prefix} 週) | 報酬: {ret:+.1f}%"
                            m['st_color'] = "#FF4D4D"
                        else:
                            m[
                                'st_status'] = f"🟢 Sell 空方 ({prefix} 週) | 報酬: < {ret:+.1f}%" if "超過" in prefix else f"🟢 Sell 空方 ({prefix} 週) | 報酬: {ret:+.1f}%"
                            m['st_color'] = "#00E676"
                except Exception as e:
                    m['st_status'] = "計算失敗"

                w30_off = self.get_val('30W起漲週數(前)', 'str_30w_week_offset', -1)
                if w30_off >= 0:
                    days_ago = int(w30_off * 5)
                    is_limit = False
                    if days_ago >= len(df_k):
                        days_ago = len(df_k) - 1
                        is_limit = True

                    past_price = df_k['close'].iloc[-(days_ago + 1)]
                    ret = ((cur_p - past_price) / past_price) * 100 if past_price > 0 else 0

                    if is_limit:
                        m['w30_status'] = f"🛰️ 30W 突破 (發動超過 {int(w30_off)} 週) | 報酬: > {ret:+.1f}%"
                    else:
                        m['w30_status'] = f"🛰️ 30W 突破 (發動 {int(w30_off)} 週) | 報酬: {ret:+.1f}%"
                    m['w30_color'] = "#FF4D4D" if ret > 0 else "#00E676"

                inst_list = self.json_data.get('institutional_investors', [])
                if inst_list and len(inst_list) > 0:
                    latest_pct = float(inst_list[0].get('total_legal_pct', 0))

                    def get_past_pct(days_ago):
                        idx = min(days_ago, len(inst_list) - 1)
                        return float(inst_list[idx].get('total_legal_pct', 0))

                    if len(inst_list) >= 5: m['real_ld_5'] = latest_pct - get_past_pct(5)
                    if len(inst_list) >= 20: m['real_ld_20'] = latest_pct - get_past_pct(20)
                    if len(inst_list) >= 60:
                        m['real_ld_60'] = latest_pct - get_past_pct(60)
                    else:
                        m['real_ld_60'] = latest_pct - get_past_pct(len(inst_list) - 1)

                if inst_list:
                    tv, ws = 0, 0
                    for d in inst_list[:20]:
                        if d['date'] in df_k.index and d['total_buy_sell'] > 0:
                            p = float(df_k.loc[d['date'], 'close'])
                            ws += (p * d['total_buy_sell'])
                            tv += d['total_buy_sell']
                    if tv > 0:
                        m["inst_cost"] = ws / tv
                        m["inst_dist_pct"] = ((cur_p - m["inst_cost"]) / m["inst_cost"]) * 100
        except Exception as e:
            pass

        # ==========================================
        # 🔥 雙軌並行動態計算：同業地位(個人在微型產業的排名) + 資金風口(題材在全市場的熱度排名)
        # ==========================================
        try:
            ret_col = '漲幅1d' if '漲幅1d' in self.full_df.columns else '今日漲幅(%)'
            concept_col = 'sub_concepts' if 'sub_concepts' in self.full_df.columns else '概念股標籤'
            sub_ind_col = 'dj_sub_ind' if 'dj_sub_ind' in self.full_df.columns else 'MDJ細產業'
            sid_col = 'sid' if 'sid' in self.full_df.columns else '股票代號'

            if not self.full_df.empty and ret_col in self.full_df.columns:
                self.full_df[ret_col] = pd.to_numeric(self.full_df[ret_col], errors='coerce')

                # --- 1. 計算「同業地位」 (找出最精細的產業，算自己的名次) ---
                if sub_ind_col in self.full_df.columns and sid_col in self.full_df.columns:
                    my_inds_str = str(self.row_data.get(sub_ind_col, self.row_data.get('MDJ細產業', '')))
                    my_inds = [ind.strip() for ind in my_inds_str.split(',') if ind.strip()]

                    best_ind_display = None
                    min_count = 9999  # 用來尋找「最精細」(競爭者最少) 的產業

                    for ind in my_inds:
                        if not ind or ind == 'nan': continue

                        # 找出全市場屬於這個細產業的所有股票
                        mask = self.full_df[sub_ind_col].astype(str).str.contains(ind, regex=False, na=False)
                        group = self.full_df[mask].copy()
                        count = len(group)

                        if count >= 2:  # 至少有 2 檔同行才能排名
                            # 依據今日漲幅對該產業所有股票進行排名
                            group = group.sort_values(by=ret_col, ascending=False).reset_index(drop=True)

                            # 找出自己在同業中的名次
                            my_rank_list = group.index[group[sid_col].astype(str) == str(self.sid)].tolist()
                            if my_rank_list:
                                my_rank = my_rank_list[0] + 1

                                # 優先顯示「最精準」(檔數最少) 的產業，例如 9家的「光纖被動元件」優先於 50家的「光通訊」
                                if count < min_count:
                                    min_count = count
                                    # 如果排名前 3，給予紅色高亮顯示龍頭地位
                                    rank_color = "#FF4D4D" if my_rank <= 3 else "#FFFFFF"
                                    best_ind_display = f"🏭 同業地位：[{ind}] (今日漲幅居同業第 <b style='color:{rank_color};'>{my_rank}</b> / {count} 名)"

                    if best_ind_display:
                        m['industry_text'] = best_ind_display

                # --- 2. 計算「資金風口」 (抓出市場最強題材) ---
                if concept_col in self.full_df.columns:
                    all_tags_dict = {}
                    for idx, row in self.full_df.iterrows():
                        tags = str(row[concept_col])
                        if tags and tags != 'nan':
                            for t in tags.split(','):
                                t = t.strip()
                                if t:
                                    if t not in all_tags_dict:
                                        all_tags_dict[t] = []
                                    if pd.notna(row[ret_col]):
                                        all_tags_dict[t].append(row[ret_col])

                    valid_tags = []
                    for t, rets in all_tags_dict.items():
                        count = len(rets)
                        if 2 <= count <= 45:  # 嚴格過濾大鍋炒標籤 (保留 2~45 檔的精細概念)
                            valid_tags.append({
                                'tag': t,
                                'mean_return': sum(rets) / count,
                                'count': count
                            })

                    if valid_tags:
                        # 計算全市場所有題材的排名
                        valid_tags.sort(key=lambda x: x['mean_return'], reverse=True)
                        for i, t_data in enumerate(valid_tags):
                            t_data['rank'] = i + 1
                        total_tags = len(valid_tags)

                        # 抓出這支股票身上的標籤
                        my_tags_str = str(self.row_data.get(concept_col, self.row_data.get('概念股標籤', '')))
                        my_tags = [t.strip() for t in my_tags_str.split(',') if t.strip()]

                        # 過濾出這支股票有參與的有效排行
                        my_tag_performance = [t for t in valid_tags if t['tag'] in my_tags]

                        if my_tag_performance:
                            # 挑選這檔股票身上「今天熱度最高」的那個題材風口來顯示
                            my_tag_performance.sort(key=lambda x: x['mean_return'], reverse=True)
                            top_t = my_tag_performance[0]

                            color = "#FF4D4D" if top_t['mean_return'] > 0 else "#00E676" if top_t[
                                                                                                'mean_return'] < 0 else "#FFFFFF"
                            sign = "+" if top_t['mean_return'] > 0 else ""
                            m[
                                'catalyst_text'] = f"🔥 資金風口：[{top_t['tag']}] (族群均漲 <span style='color:{color};'>{sign}{top_t['mean_return']:.2f}%</span> ｜ 題材排行 <b>{top_t['rank']}</b> / {total_tags})"

        except Exception as e:
            print(f"[系統] 雙軌排名計算發生錯誤: {traceback.format_exc()}")
        # ==========================================

        bbw = self.get_val('布林寬度(%)', 'bb_width', 100.0)
        if bbw <= 5.0:
            m['vcp_label'] = f"🌪️ 極度壓縮 (<5%) | 隨時表態"
        elif bbw <= 10.0:
            m['vcp_label'] = f"📉 良好收縮 (5~10%) | 優良 VCP"
        elif bbw <= 20.0:
            m['vcp_label'] = f"📊 正常波動 (10~20%) | 盤整中"
        else:
            m['vcp_label'] = f"🌊 趨勢發散或劇烈震盪 (>20%)"

        rev_list = self.json_data.get('revenue', [])
        if rev_list:
            cur_r = float(rev_list[0].get('rev_yoy', 0) or 0)
            for r in rev_list[1:]:
                if cur_r > float(r.get('rev_yoy', 0) or 0):
                    m["rev_high_cnt"] += 1
                else:
                    break

        prof_list = self.json_data.get('profitability', [])
        eps_yoy = self.get_val('EPS年增率_YoY(%)', 'eps_yoy')

        if prof_list:
            year_data = collections.defaultdict(list)
            for p in prof_list:
                q_str = p.get('quarter', '')
                if '.' in q_str:
                    try:
                        y, q = q_str.split('.')
                        y = int(y)
                        q = int(q.replace('Q', ''))
                        year_data[y].append({'q': q, 'eps': float(p.get('eps', 0) or 0)})
                    except:
                        pass

            if year_data:
                latest_year = max(year_data.keys())
                latest_qs = sorted(year_data[latest_year], key=lambda x: x['q'])
                m['acc_eps_year'] = str(latest_year)
                m['acc_eps_qs'] = len(latest_qs)
                m['acc_eps_val'] = sum(x['eps'] for x in latest_qs)

                prev_year = latest_year - 1
                if prev_year in year_data:
                    prev_qs_data = {x['q']: x['eps'] for x in year_data[prev_year]}
                    prev_sum = 0
                    valid_comparison = True
                    for q_info in latest_qs:
                        if q_info['q'] in prev_qs_data:
                            prev_sum += prev_qs_data[q_info['q']]
                        else:
                            valid_comparison = False
                            break

                    if valid_comparison and prev_sum != 0:
                        m['acc_eps_yoy'] = ((m['acc_eps_val'] - prev_sum) / abs(prev_sum)) * 100

            cur_eps = float(prof_list[0].get('eps', 0) or 0)
            try:
                latest_q_str = prof_list[0].get('quarter', '')
                if '.' in latest_q_str:
                    latest_y, latest_q = int(latest_q_str.split('.')[0]), int(
                        latest_q_str.split('.')[1].replace('Q', ''))
                    target_q_str = f"{latest_y - 1}.{latest_q}Q"
                    for p in prof_list[1:]:
                        if p.get('quarter') == target_q_str:
                            last_y_eps = float(p.get('eps', 0) or 0)
                            if last_y_eps != 0:
                                eps_yoy = ((cur_eps - last_y_eps) / abs(last_y_eps)) * 100
                            break
            except:
                pass

            for p in prof_list[1:]:
                if cur_eps > float(p.get('eps', 0) or 0):
                    m["eps_high_cnt"] += 1
                else:
                    break

        m['real_eps_yoy'] = eps_yoy

        rs_val = self.get_val('RS強度', 'RS強度')
        score = (rs_val * 0.5) + (min(max(m['real_rev_yoy'], -10), 30) * 1.5 + 50) * 0.3 + 20
        m["power_score"] = int(min(max(score, 0), 100))

        return m

    def init_ui(self):
        root_lay = QVBoxLayout(self)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("border:none; background:transparent;")
        content = QWidget()
        layout = QVBoxLayout(content)
        m = self.calculate_final_metrics()

        header = QFrame()
        header.setProperty("class", "card")
        h_lay = QVBoxLayout(header)
        top_h = QHBoxLayout()
        name = self.row_data.get('name', self.row_data.get('股票名稱', ''))

        lbl_title = QLabel(f"🎯 {self.sid} {name}")
        lbl_title.setStyleSheet("font-size: 28px; font-weight: bold; color: #FFF;")
        top_h.addWidget(lbl_title)
        top_h.addStretch()

        lbl_price = QLabel(f"現價: {m['current_price']:.2f}")
        lbl_price.setStyleSheet("font-size: 24px; font-weight: bold; color: #FFD700;")
        top_h.addWidget(lbl_price)
        h_lay.addLayout(top_h)

        score_lay = QHBoxLayout()
        lbl_score_title = QLabel("🔥 綜合戰力指標:")
        lbl_score_title.setStyleSheet("font-size: 16px; font-weight:bold; color: #FFFFFF;")
        score_lay.addWidget(lbl_score_title)

        pb = QProgressBar()
        pb.setValue(m["power_score"])
        pb.setFormat(f"{m['power_score']} pts")
        pb.setStyleSheet(
            "QProgressBar { border: 1px solid #333; border-radius: 8px; height: 22px; text-align: center; font-weight: bold; color:#FFF;} QProgressBar::chunk { background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #FF4D4D, stop:1 #FFD700); border-radius: 7px;}")
        score_lay.addWidget(pb, 3)

        rank_lay = QVBoxLayout()
        rank_lay.setSpacing(4)

        lbl_ind = QLabel(m['industry_text'])
        lbl_ind.setStyleSheet("color: #E0E6ED; font-size: 14px;")
        lbl_ind.setTextFormat(Qt.TextFormat.RichText)

        lbl_cat = QLabel(m['catalyst_text'])
        lbl_cat.setStyleSheet("color: #00E5FF; font-size: 14px; font-weight: bold;")
        lbl_cat.setTextFormat(Qt.TextFormat.RichText)

        rank_lay.addWidget(lbl_ind)
        rank_lay.addWidget(lbl_cat)

        score_lay.addLayout(rank_lay, 5)
        h_lay.addLayout(score_lay)
        layout.addWidget(header)

        grid = QGridLayout()
        grid.setSpacing(15)

        tech_card = QFrame()
        tech_card.setProperty("class", "card")
        t_lay = QVBoxLayout(tech_card)
        t_title = QLabel("📈 技術面與策略訊號")
        t_title.setStyleSheet(
            "color: #00E5FF; font-size: 18px; font-weight: bold; border-bottom: 1px solid #2B3544; padding-bottom: 8px; margin-bottom: 12px;")
        t_lay.addWidget(t_title)
        t_lay.addLayout(self._create_row("均線排列", m['ma_status'], "#FF4D4D" if m['is_bull_ma'] else "#E0E6ED"))
        t_lay.addLayout(self._create_row("創高狀態 (250T)", m['high_status'], m['high_color']))
        t_lay.addLayout(self._create_row("VCP 布林帶寬(BBW)",
                                         f"{self.get_val('布林寬度(%)', 'bb_width', 100.0):.1f}% ➔ {m['vcp_label']}",
                                         "#FFD700"))
        lbl_track = QLabel("🎯 策略買訊追蹤")
        lbl_track.setStyleSheet("color:#FFFFFF; margin-top:10px; font-weight: bold;")
        t_lay.addWidget(lbl_track)
        t_lay.addLayout(self._create_row("SuperTrend (週)", m['st_status'], m['st_color']))
        t_lay.addLayout(self._create_row("30W 均線突破 (週)", m['w30_status'], m['w30_color']))
        t_lay.addStretch()
        grid.addWidget(tech_card, 0, 0)

        chip_card = QFrame()
        chip_card.setProperty("class", "card")
        c_lay = QVBoxLayout(chip_card)
        c_title = QLabel("🏦 籌碼面動能與持股變化")
        c_title.setStyleSheet(
            "color: #00E5FF; font-size: 18px; font-weight: bold; border-bottom: 1px solid #2B3544; padding-bottom: 8px; margin-bottom: 12px;")
        c_lay.addWidget(c_title)
        dist_str = f"{m['inst_dist_pct']:+.2f}%"
        c_lay.addLayout(self._create_row("🛡️ 法人 20 日均線成本", f"{m['inst_cost']:.2f} (距現價 {dist_str})",
                                         self._color(m['inst_dist_pct'])))
        t_5d, f_5d, m_5d = self.get_val('投信買賣超(5日)', 't_sum_5d'), self.get_val('外資買賣超(5日)',
                                                                                     'f_sum_5d'), self.get_val(
            '融資增減(5日)', 'm_sum_5d')
        c_lay.addLayout(self._create_row("近5日投信", f"{int(t_5d):,} 張", self._color(t_5d)))
        c_lay.addLayout(self._create_row("近5日外資", f"{int(f_5d):,} 張", self._color(f_5d)))
        c_lay.addLayout(
            self._create_row("近5日融資 (散戶)", f"{int(m_5d):,} 張 {'(退場 👍)' if m_5d < 0 else '(進場 ⚠️)'}",
                             "#FF4D4D" if m_5d < 0 else "#00E676"))
        lbl_legal = QLabel("📊 法人波段持股比例變化")
        lbl_legal.setStyleSheet("color:#FFFFFF; margin-top:10px; font-weight: bold;")
        c_lay.addWidget(lbl_legal)
        c_lay.addLayout(self._create_row("短線 (近 5 日)", f"{m['real_ld_5']:+.2f}%", self._color(m['real_ld_5'])))
        c_lay.addLayout(self._create_row("中線 (近 20 日)", f"{m['real_ld_20']:+.2f}%", self._color(m['real_ld_20'])))
        c_lay.addLayout(
            self._create_row("長線 (近 60 日季線)", f"{m['real_ld_60']:+.2f}%", self._color(m['real_ld_60'])))
        tags = str(self.row_data.get('強勢特徵標籤', self.row_data.get('強勢特徵', '')))
        if '土洋對作' in tags:
            lbl_warn = QLabel("⚔️ 警示：觸發【土洋對作】換手特徵")
            lbl_warn.setStyleSheet(
                "color:#000; font-weight:bold; background:#FFD700; padding:5px; border-radius:4px; margin-top:5px;")
            c_lay.addWidget(lbl_warn)
        c_lay.addStretch()
        grid.addWidget(chip_card, 0, 1)

        fund_card = QFrame()
        fund_card.setProperty("class", "card")
        f_lay = QVBoxLayout(fund_card)
        f_title = QLabel("💼 基本面與領先指標")
        f_title.setStyleSheet(
            "color: #00E5FF; font-size: 18px; font-weight: bold; border-bottom: 1px solid #2B3544; padding-bottom: 8px; margin-bottom: 12px;")
        f_lay.addWidget(f_title)

        if m.get('acc_eps_year'):
            eps_val = m['acc_eps_val']
            eps_color = self._color(eps_val)

            if m.get('acc_eps_yoy') is not None:
                yoy_val = m['acc_eps_yoy']
                yoy_color = self._color(yoy_val)
                val_text = f"<span style='color: {eps_color};'>{eps_val:.2f}元</span> <span style='color: {yoy_color};'>(YoY {yoy_val:+.2f}%)</span>"
            else:
                val_text = f"<span style='color: {eps_color};'>{eps_val:.2f}元</span>"

            f_lay.addLayout(self._create_row(
                f"🌟 {m['acc_eps_year']}年累計 EPS ({m['acc_eps_qs']}季)",
                val_text,
                "#FFFFFF"
            ))

        cl_qoq, inv_qoq = self.get_val('合約負債季增(%)', 'fund_contract_qoq'), self.get_val('庫存季增(%)',
                                                                                             'fund_inventory_qoq')
        rev_high_str = f"(創 {m['rev_high_cnt']} 個月新高)" if m['rev_high_cnt'] > 0 else ""
        eps_high_str = f"(創 {m['eps_high_cnt']} 季新高)" if m['eps_high_cnt'] > 0 else ""
        f_lay.addLayout(self._create_row("最新營收 YoY", f"{m['real_rev_yoy']:+.2f}% {rev_high_str}",
                                         self._color(m['real_rev_yoy'])))
        f_lay.addLayout(self._create_row("單季 EPS YoY", f"{m['real_eps_yoy']:+.2f}% {eps_high_str}",
                                         self._color(m['real_eps_yoy'])))
        f_lay.addLayout(self._create_row("本益比 (PE)", f"{self.get_val('本益比', 'pe'):.1f} 倍", "#FFFFFF"))
        lbl_hide = QLabel("🔭 財報隱藏領先指標")
        lbl_hide.setStyleSheet("color:#FFFFFF; margin-top:10px; font-weight: bold;")
        f_lay.addWidget(lbl_hide)
        f_lay.addLayout(self._create_row("合約負債 (未來營收)", f"季增 {cl_qoq:+.2f}%", self._color(cl_qoq)))
        f_lay.addLayout(self._create_row("存貨水位", f"季增 {inv_qoq:+.2f}%", "#FFD700" if inv_qoq > 10 else "#FFFFFF"))
        f_lay.addStretch()
        grid.addWidget(fund_card, 1, 0)

        sum_card = QFrame()
        sum_card.setStyleSheet("""
            QFrame {
                background-color: #151A22; 
                border: 1px solid #2B3544; 
                border-left: 6px solid #00E5FF; 
                border-radius: 10px; 
                padding: 15px;
            }
        """)
        s_lay = QVBoxLayout(sum_card)
        s_title = QLabel("📝 操盤手 AI 綜合導覽")
        s_title.setStyleSheet(
            "color: #00E5FF; font-size: 18px; font-weight: bold; border-bottom: 1px solid #2B3544; padding-bottom: 8px; margin-bottom: 12px;")
        s_lay.addWidget(s_title)

        res = []
        if m['power_score'] >= 80 and m['is_bull_ma']:
            res.append("<span style='color:#FF4D4D;'>🔥 【頂級多頭】技術面完美且動能強勁，具備市場領頭羊特質。</span>")
        if '壓縮' in m['vcp_label'] or '收縮' in m['vcp_label']:
            res.append("<span style='color:#FFD700;'>⏳ 【VCP 壓縮】布林帶寬極度收斂，隨時可能帶量表態發動。</span>")
        if m['inst_dist_pct'] > 20:
            res.append(
                "<span style='color:#00E676;'>⚠️ 【過度乖離】現價距離法人成本逾 20%，追高風險極大，慎防獲利了結賣壓。</span>")
        if t_5d > 0 and m_5d < 0:
            res.append(
                "<span style='color:#FF4D4D;'>💎 【籌碼優異】投信連續買超且散戶(融資)退場，籌碼乾淨，由主力控盤。</span>")
        if m['real_ld_20'] > 2.0:
            res.append("<span style='color:#FF4D4D;'>🏦 【中線吸籌】近一個月法人大舉增持超過 2%，機構買盤明顯進駐。</span>")
        if m['real_ld_60'] > 5.0:
            res.append("<span style='color:#FF4D4D;'>📈 【長線佈局】近一季法人總持股爆增逾 5%，具備長線波段保護傘。</span>")
        if cl_qoq > 15:
            res.append(
                "<span style='color:#FF4D4D;'>📈 【營收潛力】合約負債雙位數大增，暗示未來幾個月營收具備強大爆發潛力。</span>")
        if '🟢 Sell' in m['st_status']:
            res.append("<span style='color:#00E676;'>⚠️ 【空頭警示】目前週線指標翻空，建議保守觀望，切忌貿然接刀。</span>")

        final_text = "<br><br>".join(
            res) if res else "<span style='color:#FFFFFF;'>目前處於趨勢觀察期或盤整階段，建議維持基本部位，觀察型態是否表態。</span>"
        final_lbl = QLabel(final_text)
        final_lbl.setStyleSheet("font-size: 16px; font-weight: bold; line-height: 1.6;")
        final_lbl.setWordWrap(True)
        s_lay.addWidget(final_lbl)
        s_lay.addStretch()
        grid.addWidget(sum_card, 1, 1)

        layout.addLayout(grid)

        llm_card = QFrame()
        llm_card.setProperty("class", "card")
        llm_card.setStyleSheet(llm_card.styleSheet() + "border: 1px solid #9C27B0; margin-top: 15px;")
        llm_lay = QVBoxLayout(llm_card)

        llm_header_lay = QHBoxLayout()
        llm_title = QLabel("🤖 頂級操盤手 LLM 深度解析 (Beta)")
        llm_title.setStyleSheet("color: #E040FB; font-size: 18px; font-weight: bold;")

        self.btn_font_down = QPushButton("A-")
        self.btn_font_up = QPushButton("A+")
        for btn in [self.btn_font_down, self.btn_font_up]:
            btn.setStyleSheet("""
                QPushButton { background-color: #37474F; color: white; border-radius: 4px; padding: 4px 12px; font-weight: bold; font-size: 14px;}
                QPushButton:hover { background-color: #455A64; }
            """)
        self.btn_font_down.clicked.connect(lambda: self._change_llm_font_size(-1))
        self.btn_font_up.clicked.connect(lambda: self._change_llm_font_size(1))

        self.btn_ask_ai = QPushButton("🚀 呼叫 AI 進行全盤數據推演")
        self.btn_ask_ai.setStyleSheet("""
            QPushButton { background-color: #6A1B9A; color: white; font-weight: bold; font-size: 14px; border-radius: 6px; padding: 6px 15px; }
            QPushButton:hover { background-color: #8E24AA; }
            QPushButton:disabled { background-color: #4A148C; color: #BBBBBB; }
        """)
        self.btn_ask_ai.clicked.connect(lambda: self._trigger_llm_analysis(m))

        llm_header_lay.addWidget(llm_title)
        llm_header_lay.addStretch()
        llm_header_lay.addWidget(self.btn_font_down)
        llm_header_lay.addWidget(self.btn_font_up)
        llm_header_lay.addWidget(self.btn_ask_ai)
        llm_lay.addLayout(llm_header_lay)

        self.lbl_llm_result = QLabel()
        self.lbl_llm_result.setWordWrap(True)
        self.lbl_llm_result.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._update_llm_text_display()

        llm_lay.addWidget(self.lbl_llm_result)
        layout.addWidget(llm_card)

        scroll.setWidget(content)
        root_lay.addWidget(scroll)

    def _create_row(self, label_text, val_text, val_color="#FFFFFF"):
        lay = QHBoxLayout()
        lbl = QLabel(label_text)
        lbl.setStyleSheet("color: #FFFFFF; font-size: 15px;")
        val = QLabel(val_text)
        val.setStyleSheet(f"color: {val_color}; font-weight: bold; font-size: 15px;")
        val.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        box = QFrame()
        box.setProperty("class", "highlight-box")
        box_lay = QHBoxLayout(box)
        box_lay.setContentsMargins(8, 6, 8, 6)
        box_lay.addWidget(lbl)
        box_lay.addStretch()
        box_lay.addWidget(val)

        lay.addWidget(box)
        return lay

    def _color(self, val):
        try:
            v = float(val)
            if v > 0: return "#FF4D4D"  # Red
            if v < 0: return "#00E676"  # Green
            return "#FFFFFF"
        except:
            return "#FFFFFF"