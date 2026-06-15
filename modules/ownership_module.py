# 檔案路徑: modules/ownership_module.py
import sys
import os
import json
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
                             QTableWidget, QTableWidgetItem, QHeaderView,
                             QLabel, QFrame, QAbstractItemView, QApplication,
                             QPushButton, QStackedWidget)
from PyQt6.QtGui import QColor, QBrush, QFont, QPainter, QPen
from PyQt6.QtCore import Qt, pyqtSignal, QThread

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


class OwnershipWorker(QThread):
    """讀取大戶股權分佈資料的背景執行緒 (扁平化精準解析版)"""
    data_loaded = pyqtSignal(dict)

    def __init__(self, project_root, stock_id):
        super().__init__()
        self.project_root = Path(project_root)
        self.stock_id = stock_id

    def run(self):
        import requests
        from bs4 import BeautifulSoup
        import re
        import os
        from datetime import datetime
        from dotenv import load_dotenv
        import pandas as pd

        load_dotenv()
        sid = self.stock_id.split('_')[0]
        url = f"https://norway.twsthr.info/StockHolders.aspx?stock={sid}"
        headers = {'User-Agent': 'Mozilla/5.0'}

        http_proxy = os.getenv("HTTP_PROXY", os.getenv("http_proxy"))
        https_proxy = os.getenv("HTTPS_PROXY", os.getenv("https_proxy"))
        proxies = {}
        if http_proxy: proxies["http"] = http_proxy
        if https_proxy: proxies["https"] = https_proxy

        try:
            # 1. 抓取網頁原始 HTML
            res = requests.get(url, headers=headers, proxies=proxies, timeout=8)
            soup = BeautifulSoup(res.text, 'html.parser')

            # 2. 解析「上表（Summary Table）」：抓取「總張數與人數」
            summary_map = {}
            for tbl in soup.find_all('table'):
                text = tbl.get_text()
                if '總股東人數' in text and '集保總張數' in text and '平均張數' in text:
                    for tr in tbl.find_all('tr'):
                        cells = tr.find_all(['td', 'th'])
                        texts = [c.get_text(strip=True).replace('\xa0', '').replace(',', '') for c in cells]
                        texts = [t for t in texts if t]
                        if len(texts) >= 5 and re.match(r'^\d{8}$', texts[0]):
                            try:
                                summary_map[texts[0]] = {
                                    'total_lots': int(texts[1]),
                                    'holders': int(texts[2])
                                }
                            except:
                                continue
                    break

            # 3. 讀取本地 Parquet K 線以取得真實最新收盤價
            df_k = None
            p1 = self.project_root / "data" / "cache" / "tw" / f"{self.stock_id}.parquet"
            p2 = self.project_root / "data" / "cache" / "tw" / f"{sid}_TW.parquet"
            p3 = self.project_root / "data" / "cache" / "tw" / f"{sid}_TWO.parquet"
            for p in [p1, p2, p3]:
                if p.exists():
                    try:
                        df_k = pd.read_parquet(p)
                        break
                    except Exception:
                        continue

            latest_price = 50.0  # 預設回退價
            if df_k is not None and not df_k.empty:
                if 'Date' in df_k.columns:
                    df_k['Date'] = pd.to_datetime(df_k['Date'])
                    df_k.set_index('Date', inplace=True)
                elif not isinstance(df_k.index, pd.DatetimeIndex):
                    df_k.index = pd.to_datetime(df_k.index)
                df_k = df_k.sort_index()
                if 'close' in df_k.columns:
                    df_k['close'] = pd.to_numeric(df_k['close'], errors='coerce')
                    if len(df_k) > 0:
                        latest_price = float(df_k['close'].iloc[-1])

            # 人數防呆預設值
            latest_lots = 100000
            latest_holders = 10000
            if summary_map:
                first_key = list(summary_map.keys())[0]
                latest_lots = summary_map[first_key]['total_lots']
                latest_holders = summary_map[first_key]['holders']

            # 4. 🚀 扁平化直接解析：直接繞過 Table 標籤，搜尋全網頁所有包含 'lDS' 類別的 TR 行
            parsed_data = []
            target_rows = soup.find_all('tr', class_=lambda c: c and 'lDS' in c)

            for tr in target_rows:
                cells = tr.find_all('td')
                texts = [c.get_text(strip=True).replace('\xa0', '').replace(',', '') for c in cells]

                # 🚀 動態對位：在陣列中尋找 8 位數日期
                date_idx = -1
                for idx, text in enumerate(texts):
                    if re.match(r'^\d{8}$', text):
                        date_idx = idx
                        break

                # 找到日期且後面欄位足夠，即進行百分比加總
                if date_idx != -1 and len(texts) >= date_idx + 16:
                    try:
                        date_str = texts[date_idx]

                        # 自日期往後算 12, 13, 14, 15 欄，分別對應: 400-600, 600-800, 800-1000, 1000以上
                        val_400_600 = float(texts[date_idx + 12])
                        val_600_800 = float(texts[date_idx + 13])
                        val_800_1000 = float(texts[date_idx + 14])
                        val_1000_above = float(texts[date_idx + 15])

                        # 計算大戶持股比例
                        pct_400 = round(val_400_600 + val_600_800 + val_800_1000 + val_1000_above, 2)
                        pct_800 = round(val_800_1000 + val_1000_above, 2)
                        pct_1000 = round(val_1000_above, 2)

                        # 合併 Summary 人數數據
                        sum_info = summary_map.get(date_str, {'total_lots': latest_lots, 'holders': latest_holders})

                        parsed_data.append({
                            'date_str': date_str,
                            'date_obj': datetime.strptime(date_str, '%Y%m%d'),
                            'total_lots': sum_info['total_lots'],
                            'holders': sum_info['holders'],
                            'pct_400': pct_400,
                            'pct_800': pct_800,
                            'pct_1000': pct_1000
                        })
                    except Exception:
                        continue

            parsed_data.sort(key=lambda x: x['date_obj'], reverse=True)
            if len(parsed_data) < 5:
                return

            latest = parsed_data[0]
            market_cap = latest['total_lots'] * 1000 * latest_price

            # 5. 動態判定大戶定義門檻
            if market_cap < 5_000_000_000:
                whale_key, whale_label, cap_desc = 'pct_400', "≥ 400張", "小型股"
            elif market_cap <= 30_000_000_000:
                whale_key, whale_label, cap_desc = 'pct_800', "≥ 800張", "中型股"
            else:
                whale_key, whale_label, cap_desc = 'pct_1000', "≥ 1000張", "大型股"

            work_data = parsed_data[:5]
            whale_history, retail_history, price_history, dates = [], [], [], []
            whale_streak_arr, retail_streak_arr = [], []

            # 6. 計算大戶與散戶的週訊號方向
            for i in range(len(work_data) - 1):
                curr, prev = work_data[i], work_data[i + 1]
                w_diff = curr[whale_key] - prev[whale_key]

                curr_retail = 100.0 - curr[whale_key]
                prev_retail = 100.0 - prev[whale_key]
                r_diff = curr_retail - prev_retail

                whale_streak_arr.append("▲" if w_diff > 0 else ("▼" if w_diff < 0 else "➖"))
                retail_streak_arr.append("▲" if r_diff > 0 else ("▼" if r_diff < 0 else "➖"))

            whale_streak_arr.append("➖")
            retail_streak_arr.append("➖")

            # 7. 彙整歷史 5 週繪圖數據
            for d in work_data:
                dates.append(f"{d['date_obj'].month:02d}/{d['date_obj'].day:02d}")
                whale_history.append(d[whale_key])
                retail_history.append(round(100.0 - d[whale_key], 2))

                close_val = 50.0
                if df_k is not None and not df_k.empty and 'close' in df_k.columns:
                    past_prices = df_k.loc[df_k.index <= d['date_obj']]
                    if not past_prices.empty:
                        val = past_prices['close'].iloc[-1]
                        if pd.notna(val):
                            close_val = float(val)
                price_history.append(close_val)

            dates.reverse()
            whale_history.reverse()
            retail_history.reverse()
            whale_streak_arr.reverse()
            retail_streak_arr.reverse()
            price_history.reverse()

            # 8. 進階籌碼評等判定（複合長短線動能）
            past_4w = work_data[4]
            w_change = latest[whale_key] - past_4w[whale_key]
            r_change = -w_change

            short_w_change = latest[whale_key] - work_data[2][whale_key]

            if w_change > 0:
                if short_w_change >= -0.1:
                    concentration_status = "🔥 籌碼集中 (大戶進/散戶退)"
                else:
                    concentration_status = "⚠️ 高檔鬆動 (大戶退/散戶進)"
            else:
                if short_w_change >= 0.1:
                    concentration_status = "🔄 底部復甦 (大戶進/散戶退)"
                else:
                    concentration_status = "💀 籌碼渙散 (大戶退/散戶進)"

            # 9. 解析級距大表 (維持最後一週的 15 級細分明細)
            tiers_data = []
            try:
                for tbl in soup.find_all('table'):
                    text = tbl.get_text()
                    if '持股張數分級' in text and '1-999股' in text:
                        for tr in tbl.find_all('tr'):
                            cells = [c.get_text(strip=True).replace(',', '') for c in tr.find_all(['td', 'th'])]
                            if len(cells) >= 9 and re.match(r'^(1-999股|\d+-\d+張|\d+,?\d*張以上)$', cells[1]):
                                try:
                                    r_name = cells[1]
                                    holders = int(cells[2])
                                    shares = int(float(cells[3]))
                                    pct = float(cells[4])
                                    prev_pct = float(cells[8]) if cells[8] else pct
                                    change = pct - prev_pct

                                    t_type = "normal"
                                    if "以上" in r_name or "400-" in r_name or "600-" in r_name or "800-" in r_name:
                                        t_type = "whale"
                                    elif "1-999" in r_name or "1-5張" in r_name or "5-10張" in r_name:
                                        t_type = "retail"

                                    tiers_data.append({
                                        "range": r_name,
                                        "holders": holders,
                                        "shares": shares,
                                        "pct": pct,
                                        "change": round(change, 2),
                                        "type": t_type
                                    })
                                except Exception:
                                    continue
                        break
                tiers_data.reverse()
            except Exception as e:
                print(f"級距解析錯誤: {e}")

            result = {
                "stock_id": self.stock_id,
                "tier_badge": f"{cap_desc} ・ 大戶門檻 {whale_label}",
                "metrics": {
                    "whale_pct": latest[whale_key], "whale_change_4w": round(w_change, 2),
                    "retail_pct": latest['holders'], "retail_change_4w": r_change,
                    "threshold": whale_label, "concentration_status": concentration_status
                },
                "whale_streak": whale_streak_arr,
                "whale_streak_txt": f"近四週變化 {w_change:+.2f}%",
                "retail_streak": retail_streak_arr,
                "retail_streak_txt": f"近四週變化 {r_change:+.2f}%",
                "trend": {
                    "dates": dates,
                    "whale_history": whale_history,
                    "retail_history": retail_history,
                    "price_history": price_history
                },
                "tiers": tiers_data
            }
            self.data_loaded.emit(result)
        except Exception as e:
            print(f"❌ [股權模組] 爬蟲發生錯誤: {e}")


class OwnershipModule(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_stock_id = ""
        self.current_stock_name = ""
        self.worker = None
        self.trend_data = None

        self.project_root = Path(__file__).resolve().parent.parent
        self.init_ui()

    def init_ui(self):
        self.setStyleSheet("background-color: #000000; color: #FFFFFF;")

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # ==================== 1. 標準化 Header 區域 ====================
        header_widget = QWidget()
        header_widget.setFixedHeight(45)
        header_widget.setStyleSheet("background-color: #050505; border-bottom: 1px solid #333;")
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(10, 0, 10, 0)
        header_layout.setSpacing(15)

        self.lbl_stock_info = QLabel("請選擇股票")
        self.lbl_stock_info.setStyleSheet(
            "color: #FFFF00; font-weight: bold; font-size: 18px; font-family: 'Microsoft JhengHei';")

        sep = QLabel("|")
        sep.setStyleSheet("color: #666; font-size: 16px;")

        title = QLabel("股權分佈")
        title.setStyleSheet("color: #00FFFF; font-weight: bold; font-size: 16px;")

        self.lbl_tier_badge = QLabel("—")
        self.lbl_tier_badge.setStyleSheet(
            "color: #FFFF00; font-size: 13px; font-weight: bold; background: #222; padding: 2px 6px; border-radius: 4px; border: 1px solid #555;")
        self.lbl_tier_badge.setMinimumWidth(180)
        self.info_label = QLabel("移動滑鼠至圖表查看數據...")
        self.info_label.setMinimumWidth(550)
        self.info_label.setStyleSheet("font-family: 'Consolas'; font-size: 13px; color: #AAAAAA;")
        self.info_label.setTextFormat(Qt.TextFormat.RichText)

        # 切換視圖按鈕
        self.btn_toggle_view = QPushButton("切換至表格")
        self.btn_toggle_view.setFixedSize(85, 26)
        self.btn_toggle_view.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_toggle_view.setStyleSheet("""
            QPushButton { background: #222; color: #00E5FF; border: 1px solid #00E5FF; border-radius: 4px; font-weight: bold; font-size: 12px; }
            QPushButton:hover { background: #004444; color: white; }
        """)
        self.btn_toggle_view.clicked.connect(self.toggle_view)

        header_layout.addWidget(self.lbl_stock_info)
        header_layout.addWidget(sep)
        header_layout.addWidget(title)
        header_layout.addWidget(QLabel("  "))
        header_layout.addWidget(self.lbl_tier_badge)
        header_layout.addWidget(QLabel("  "))
        header_layout.addWidget(self.info_label)
        header_layout.addStretch()
        header_layout.addWidget(self.btn_toggle_view)

        main_layout.addWidget(header_widget)

        # ==================== 2. 內容區域 ====================
        content_widget = QWidget()
        content_layout = QHBoxLayout(content_widget)
        content_layout.setContentsMargins(4, 4, 4, 4)
        content_layout.setSpacing(6)

        # --- 左側面：數據指標與信號區 ---
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(2, 2, 2, 2)
        left_layout.setSpacing(6)

        grid_layout = QGridLayout()
        grid_layout.setSpacing(4)

        # --- 找到此段落並進行修改 ---
        self.card_whale = self._create_metric_card("大戶持股%", "0.0%", "—", "#FF3333")

        # 🚀 修正：卡片標題改為 散戶持股%
        self.card_retail = self._create_metric_card("散戶持股%", "0.0%", "—", "#00FF33")

        self.card_thresh = self._create_metric_card("大戶定義門檻", "—", "市值 ≥ 1億", "#FFFFFF")
        self.card_status = self._create_metric_card("籌碼集中度評等", "—", "同步觀測中", "#FFFF00")

        grid_layout.addWidget(self.card_whale, 0, 0)
        grid_layout.addWidget(self.card_retail, 0, 1)
        grid_layout.addWidget(self.card_thresh, 1, 0)
        grid_layout.addWidget(self.card_status, 1, 1)
        left_layout.addLayout(grid_layout)

        self.streak_box = QFrame()
        self.streak_box.setStyleSheet("background-color: #121212; border: 1px solid #444; border-radius: 4px;")
        streak_layout = QVBoxLayout(self.streak_box)
        streak_layout.setContentsMargins(8, 8, 8, 8)
        streak_layout.setSpacing(8)

        whale_row = QHBoxLayout()
        whale_title = QLabel("大戶週訊號")
        whale_title.setStyleSheet("color: #FFFFFF; font-size: 13px; font-weight: bold;")
        self.layout_whale_dots = QHBoxLayout()
        self.layout_whale_dots.setSpacing(4)
        self.lbl_whale_streak_txt = QLabel("—")
        self.lbl_whale_streak_txt.setStyleSheet("color: #FF3333; font-size: 12px; font-weight: bold;")
        whale_row.addWidget(whale_title)
        whale_row.addLayout(self.layout_whale_dots)
        whale_row.addStretch()
        whale_row.addWidget(self.lbl_whale_streak_txt)
        streak_layout.addLayout(whale_row)

        retail_row = QHBoxLayout()
        retail_title = QLabel("散戶週訊號")
        retail_title.setStyleSheet("color: #FFFFFF; font-size: 13px; font-weight: bold;")
        self.layout_retail_dots = QHBoxLayout()
        self.layout_retail_dots.setSpacing(4)
        self.lbl_retail_streak_txt = QLabel("—")
        self.lbl_retail_streak_txt.setStyleSheet("color: #00FF33; font-size: 12px; font-weight: bold;")
        retail_row.addWidget(retail_title)
        retail_row.addLayout(self.layout_retail_dots)
        retail_row.addStretch()
        retail_row.addWidget(self.lbl_retail_streak_txt)
        streak_layout.addLayout(retail_row)

        left_layout.addWidget(self.streak_box)
        left_layout.addStretch()
        content_layout.addWidget(left_panel, stretch=4)

        # --- 右側面：圖表與結構明細表 (使用 QStackedWidget 切換) ---
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(2, 2, 2, 2)
        right_layout.setSpacing(6)

        self.right_stack = QStackedWidget()

        # Index 0: 趨勢折線圖
        self.fig = Figure(facecolor='#000000')
        self.canvas = FigureCanvas(self.fig)
        self.right_stack.addWidget(self.canvas)

        # Index 1: 級距大表
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["持股級距", "張數", "佔股本", "週變動"])
        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; gridline-color: #444; color: #FFFFFF; border: 1px solid #444; font-size: 13px; font-family: 'Consolas', 'Microsoft JhengHei'; }
            QHeaderView::section { background-color: #1A1A1A; color: #00FFFF; font-weight: bold; height: 32px; border: 1px solid #444; font-size: 13px; }
            QTableWidget::item { padding: 4px; }
        """)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        self.right_stack.addWidget(self.table)

        right_layout.addWidget(self.right_stack)
        self.right_stack.setMaximumHeight(280)  # 限制圖表/表格最大高度使其變矮
        right_layout.addStretch()  # 加入彈性空間將圖表往上推，與左側對齊
        content_layout.addWidget(right_panel, stretch=6)
        main_layout.addWidget(content_widget)

        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)

    def toggle_view(self):
        current = self.right_stack.currentIndex()
        if current == 0:
            self.right_stack.setCurrentIndex(1)
            self.btn_toggle_view.setText("切換至圖表")
            self.btn_toggle_view.setStyleSheet("""
                QPushButton { background: #004444; color: #FFF; border: 1px solid #00E5FF; border-radius: 4px; font-weight: bold; font-size: 12px; }
                QPushButton:hover { background: #006666; color: white; }
            """)
        else:
            self.right_stack.setCurrentIndex(0)
            self.btn_toggle_view.setText("切換至表格")
            self.btn_toggle_view.setStyleSheet("""
                QPushButton { background: #222; color: #00E5FF; border: 1px solid #00E5FF; border-radius: 4px; font-weight: bold; font-size: 12px; }
                QPushButton:hover { background: #004444; color: white; }
            """)

    def _create_metric_card(self, title, val, sub, val_color):
        card = QFrame()
        card.setStyleSheet("background-color: #121212; border: 1px solid #444; border-radius: 4px;")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(2)

        lbl_title = QLabel(title)
        lbl_title.setStyleSheet("color: #CCCCCC; font-size: 12px; font-weight: bold;")

        lbl_val = QLabel(val)
        lbl_val.setStyleSheet(f"color: {val_color}; font-size: 22px; font-weight: bold; font-family: 'Consolas';")

        lbl_sub = QLabel(sub)
        lbl_sub.setStyleSheet("color: #AAAAAA; font-size: 12px; font-weight: bold;")

        layout.addWidget(lbl_title)
        layout.addWidget(lbl_val)
        layout.addWidget(lbl_sub)

        card.lbl_val = lbl_val
        card.lbl_sub = lbl_sub
        return card

    def load_ownership_data(self, stock_id, stock_name=""):
        if stock_id == self.current_stock_id and self.table.rowCount() > 0:
            return

        self.current_stock_id = stock_id
        self.current_stock_name = stock_name

        # 修正 2：切換股票時先清空畫面，呈現等待狀態
        display_id = stock_id.split('_')[0]
        self.lbl_stock_info.setText(f"{display_id} {stock_name} (資料擷取中...)")
        self.info_label.setText("正在連線抓取集保最新籌碼動態，請稍候...")

        self.card_whale.lbl_val.setText("讀取中...")
        self.card_whale.lbl_sub.setText("—")
        self.card_retail.lbl_val.setText("讀取中...")
        self.card_retail.lbl_sub.setText("—")
        self.card_thresh.lbl_val.setText("—")
        self.card_status.lbl_val.setText("—")
        self.lbl_whale_streak_txt.setText("—")
        self.lbl_retail_streak_txt.setText("—")

        for layout in [self.layout_whale_dots, self.layout_retail_dots]:
            while layout.count():
                item = layout.takeAt(0)
                if item.widget(): item.widget().deleteLater()

        self.fig.clear()
        self.canvas.draw()
        self.table.setRowCount(0)

        if self.worker is not None and self.worker.isRunning():
            try:
                self.worker.data_loaded.disconnect()
            except:
                pass

        self.worker = OwnershipWorker(self.project_root, stock_id)
        self.worker.data_loaded.connect(self.update_ui)
        self.worker.start()

    def update_ui(self, data):
        display_id = self.current_stock_id.split('_')[0]
        self.lbl_stock_info.setText(f"{display_id} {self.current_stock_name}" if self.current_stock_name else f"{display_id}")
        self.info_label.setText("移動滑鼠至圖表查看數據...")
        self.lbl_tier_badge.setText(data.get("tier_badge", "—"))

        m = data["metrics"]
        self.card_whale.lbl_val.setText(f"{m['whale_pct']}%")
        w_change = m['whale_change_4w']
        w_color = "#FF3333" if w_change > 0 else ("#00FF33" if w_change < 0 else "#FFFFFF")
        self.card_whale.lbl_sub.setText(f"{w_change:+.2f}% 近四週")
        self.card_whale.lbl_sub.setStyleSheet(f"color: {w_color}; font-size: 12px; font-weight: bold;")

        # 🚀 修正：將散戶數值格式化改為百分比(%)
        retail_pct = round(100.0 - m['whale_pct'], 2)
        self.card_retail.lbl_val.setText(f"{retail_pct}%")
        r_change = -w_change
        r_color = "#FF3333" if r_change > 0 else ("#00FF33" if r_change < 0 else "#FFFFFF")
        self.card_retail.lbl_sub.setText(f"{r_change:+.2f}% 近四週")
        self.card_retail.lbl_sub.setStyleSheet(f"color: {r_color}; font-size: 12px; font-weight: bold;")

        self.card_thresh.lbl_val.setText(m['threshold'])
        self.card_status.lbl_val.setText(m['concentration_status'])
        if "集中" in m['concentration_status']:
            self.card_status.lbl_val.setStyleSheet("color: #FF3333; font-size: 16px; font-weight: bold; font-family: 'Microsoft JhengHei';")
        elif "渙散" in m['concentration_status']:
            self.card_status.lbl_val.setStyleSheet("color: #00FF33; font-size: 16px; font-weight: bold; font-family: 'Microsoft JhengHei';")
        else:
            self.card_status.lbl_val.setStyleSheet("color: #FFFF00; font-size: 16px; font-weight: bold; font-family: 'Microsoft JhengHei';")

        self._render_streak_dots(self.layout_whale_dots, data["whale_streak"], is_whale=True)
        self.lbl_whale_streak_txt.setText(data["whale_streak_txt"])
        self.lbl_whale_streak_txt.setStyleSheet(f"color: {w_color}; font-size: 12px; font-weight: bold;")

        # 🚀 修正：散戶週訊號文字修改
        self._render_streak_dots(self.layout_retail_dots, data["retail_streak"], is_whale=False)
        self.lbl_retail_streak_txt.setText(f"近四週變化 {r_change:+.2f}%")
        self.lbl_retail_streak_txt.setStyleSheet(f"color: {r_color}; font-size: 12px; font-weight: bold;")

        self.trend_data = data["trend"]
        self._render_chart(self.trend_data)
        self._render_table(data["tiers"])

    def _render_streak_dots(self, layout, streak_list, is_whale):
        while layout.count():
            item = layout.takeAt(0)
            if item.widget(): item.widget().deleteLater()

        for sign in streak_list:
            dot = QLabel()
            dot.setAlignment(Qt.AlignmentFlag.AlignCenter)
            dot.setFixedSize(24, 24)

            # 改用實心三角形，並套用亮紅亮綠字體
            if sign in ["▲", "⬆", "+"]:
                arrow = "▲"
                bg_color = "rgba(255, 51, 51, 0.15)"
                txt_color = "#FF3333"  # 亮紅
                border_color = "#FF3333"
            elif sign in ["▼", "⬇", "-"]:
                arrow = "▼"
                bg_color = "rgba(0, 255, 51, 0.15)"
                txt_color = "#00FF33"  # 亮綠
                border_color = "#00FF33"
            else:
                arrow = "➖"
                bg_color = "#222"
                txt_color = "#888"
                border_color = "#666"

            dot.setText(arrow)
            dot.setStyleSheet(f"""
                background-color: {bg_color}; color: {txt_color}; 
                border: 1px solid {border_color}; border-radius: 12px;
                font-family: 'Segoe UI', 'Microsoft JhengHei';
                font-size: 14px; font-weight: 900;
            """)
            layout.addWidget(dot)

    def _render_chart(self, trend_data):
        self.fig.clear()
        self.fig.subplots_adjust(top=0.82, bottom=0.15, left=0.1, right=0.9)
        self.ax1 = self.fig.add_subplot(111)
        self.ax2 = self.ax1.twinx()  # 右邊 Y 軸給收盤價

        # 🚀 修正：移除 ax3（不需要第三軸），讓大戶與散戶線共用 ax1
        self.ax1.set_facecolor('#000000')

        dates = trend_data["dates"]
        x = np.arange(len(dates))

        line_price = self.ax2.plot(x, trend_data["price_history"], color='#FFFF00', linestyle='--', linewidth=1.5,
                                   marker='s', markersize=4, label='收盤價', alpha=0.9)

        # 🚀 大戶與散戶都畫在 ax1 (左 Y 軸百分比)
        line_whale = self.ax1.plot(x, trend_data["whale_history"], color='#FF3333', linewidth=2.5, marker='o',
                                   markersize=6, label='大戶%')
        line_retail = self.ax1.plot(x, trend_data["retail_history"], color='#00FF33', linewidth=2.5, marker='^',
                                    markersize=6, label='散戶%')

        # 在最後一個收盤價數據點上，標註最後的股價
        if len(x) > 0:
            last_idx = len(x) - 1
            last_price = trend_data["price_history"][-1]
            self.ax2.annotate(
                f"{last_price:.1f}",
                xy=(last_idx, last_price),
                xytext=(0, 10),
                textcoords="offset points",
                ha='center',
                va='bottom',
                color='#FFFF00',
                weight='bold',
                fontsize=11,
                bbox=dict(boxstyle="round,pad=0.2", fc="#111111", ec="#FFFF00", lw=1, alpha=0.9)
            )

        self.ax1.set_xticks(x)
        self.ax1.set_xticklabels(dates, color='#E0E0E0', fontsize=11)
        self.ax1.set_ylabel('持股比例 (%)', color='#E0E0E0', fontsize=10)  # 修正標籤
        self.ax2.set_ylabel('收盤價', color='#FFFF00', fontsize=10)

        self.ax1.grid(True, color='#444444', linestyle=':', alpha=0.8)
        self.ax1.tick_params(axis='y', colors='#E0E0E0', labelsize=10)
        self.ax2.tick_params(axis='y', colors='#FFFF00', labelsize=10)

        for spine in self.ax1.spines.values(): spine.set_edgecolor('#555555')
        for spine in self.ax2.spines.values(): spine.set_edgecolor('#555555')

        lns = line_whale + line_retail + line_price
        labs = [l.get_label() for l in lns]
        self.ax1.legend(lns, labs, bbox_to_anchor=(0.5, 1.25), loc='upper center', ncol=3,
                        facecolor='#111111', edgecolor='#555555', labelcolor='#FFFFFF', fontsize=11, borderaxespad=0.)
        self.canvas.draw()

    def on_mouse_move(self, event):
        if not event.inaxes or self.trend_data is None:
            return

        idx = int(round(event.xdata))
        dates = self.trend_data["dates"]

        if 0 <= idx < len(dates):
            date_str = dates[idx]
            whale = self.trend_data["whale_history"][idx]
            retail = self.trend_data["retail_history"][idx]
            price = self.trend_data["price_history"][idx]

            w_diff_html = ""
            r_diff_html = ""

            if idx > 0:
                w_diff = whale - self.trend_data["whale_history"][idx - 1]
                r_diff = retail - self.trend_data["retail_history"][idx - 1]

                w_color = "#FF3333" if w_diff > 0 else ("#00FF33" if w_diff < 0 else "#FFFFFF")
                r_color = "#FF3333" if r_diff > 0 else ("#00FF33" if r_diff < 0 else "#FFFFFF")

                w_diff_html = f" <span style='color:{w_color};'>({w_diff:+.2f}%)</span>"
                # 🚀 修正：懸停提示改為百分比差值
                r_diff_html = f" <span style='color:{r_color};'>({r_diff:+.2f}%)</span>"

            # 🚀 修正：■ 散戶單位改為 %
            html = (
                f"<span style='color:#FFFFFF;'>{date_str}</span> | "
                f"<span style='color:#FF3333;'>■ 大戶: {whale:.2f}%{w_diff_html}</span> | "
                f"<span style='color:#00FF33;'>■ 散戶: {retail:.2f}%{r_diff_html}</span> | "
                f"<span style='color:#FFFF00;'>■ 收盤價: {price}</span>"
            )
            self.info_label.setText(html)

    def _render_table(self, tiers):
        self.table.setRowCount(len(tiers))
        for i, row in enumerate(tiers):
            c_range = QTableWidgetItem(row["range"])
            c_shares = QTableWidgetItem(f"{row['shares']:,}")
            c_pct = QTableWidgetItem(f"{row['pct']:.1f}%")
            c_change = QTableWidgetItem(f"{row['change']:+.1f}%")

            c_shares.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            c_pct.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            c_change.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            c_change.setForeground(QColor("#FF3333" if row["change"] >= 0 else "#00FF33"))

            bg_hex = "#000000"
            if row["type"] == "whale":
                bg_hex = "#2A0A0A"
                c_range.setForeground(QColor("#FF6666"))
            elif row["type"] == "retail":
                bg_hex = "#052210"
                c_range.setForeground(QColor("#66FF66"))

            for item in [c_range, c_shares, c_pct, c_change]:
                item.setBackground(QBrush(QColor(bg_hex)))

            self.table.setItem(i, 0, c_range)
            self.table.setItem(i, 1, c_shares)
            self.table.setItem(i, 2, c_pct)
            self.table.setItem(i, 3, c_change)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = OwnershipModule()
    win.load_ownership_data("6274_TW", "台燿")
    win.resize(800, 480)
    win.show()
    sys.exit(app.exec())