import sys
import json
import pandas as pd
import numpy as np
from io import StringIO
import requests
from pathlib import Path
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QComboBox,
                             QLabel, QTableWidget, QTableWidgetItem, QFrame,
                             QSplitter, QApplication, QHeaderView, QTabWidget,
                             QPushButton, QStackedWidget)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QColor, QFont, QBrush

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

plt.style.use('dark_background')
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


# --- 單檔數據抓取線程 ---
class ETFDataWorker(QThread):
    data_fetched = pyqtSignal(pd.DataFrame, str)

    def __init__(self, etf_id, provider):
        super().__init__()
        self.etf_id = etf_id
        self.provider = provider

    def run(self):
        local_path = Path(f"data/clean/{self.provider}/{self.etf_id}.csv")
        github_url = f"https://raw.githubusercontent.com/phanchang/stock-room-data/main/data/clean/{self.provider}/{self.etf_id}.csv"

        df = pd.DataFrame()
        try:
            print(f"🚀 [ETF] 優先讀取雲端資料: {github_url}")
            response = requests.get(github_url, timeout=10)
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text))
            else:
                if local_path.exists(): df = pd.read_csv(local_path)

            if not df.empty:
                df.columns = [c.lower().strip() for c in df.columns]
                rename_map = {}
                if 'stock_code' in df.columns: rename_map['stock_code'] = 'stock_id'
                if 'code' in df.columns: rename_map['code'] = 'stock_id'
                if 'stock_name' in df.columns: rename_map['stock_name'] = 'name'
                if rename_map: df.rename(columns=rename_map, inplace=True)

                for col in ['shares', 'weight']:
                    if col in df.columns and df[col].dtype == 'object':
                        df[col] = df[col].str.replace(',', '').str.replace('%', '')
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                if 'stock_id' in df.columns: df['stock_id'] = df['stock_id'].astype(str)
                self.data_fetched.emit(df, self.etf_id)
            else:
                self.data_fetched.emit(pd.DataFrame(), self.etf_id)

        except Exception as e:
            try:
                if local_path.exists():
                    df = pd.read_csv(local_path)
                    self.data_fetched.emit(df, self.etf_id)
                else:
                    self.data_fetched.emit(pd.DataFrame(), self.etf_id)
            except:
                self.data_fetched.emit(pd.DataFrame(), self.etf_id)


# --- 多檔 ETF 融合抓取線程 ---
class MultiETFDataWorker(QThread):
    multi_data_fetched = pyqtSignal(pd.DataFrame)

    def __init__(self, mapping):
        super().__init__()
        self.mapping = mapping

    def run(self):
        all_data = []
        for etf_id, (provider, _) in self.mapping.items():
            github_url = f"https://raw.githubusercontent.com/phanchang/stock-room-data/main/data/clean/{provider}/{etf_id}.csv"
            local_path = Path(f"data/clean/{provider}/{etf_id}.csv")

            df = pd.DataFrame()
            try:
                res = requests.get(github_url, timeout=5)
                if res.status_code == 200:
                    df = pd.read_csv(StringIO(res.text))
                elif local_path.exists():
                    df = pd.read_csv(local_path)
            except:
                if local_path.exists(): df = pd.read_csv(local_path)

            if not df.empty:
                df.columns = [c.lower().strip() for c in df.columns]
                rename_map = {}
                if 'stock_code' in df.columns: rename_map['stock_code'] = 'stock_id'
                if 'code' in df.columns: rename_map['code'] = 'stock_id'
                if 'stock_name' in df.columns: rename_map['stock_name'] = 'name'
                if rename_map: df.rename(columns=rename_map, inplace=True)

                for col in ['shares', 'weight']:
                    if col in df.columns and df[col].dtype == 'object':
                        df[col] = df[col].str.replace(',', '').str.replace('%', '')
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                df['stock_id'] = df['stock_id'].astype(str)
                df['etf_id'] = etf_id
                if 'date' in df.columns: df['date'] = pd.to_datetime(df['date'])
                all_data.append(df)

        if all_data:
            self.multi_data_fetched.emit(pd.concat(all_data, ignore_index=True))
        else:
            self.multi_data_fetched.emit(pd.DataFrame())


# --- 主程式模組 ---
class ActiveETFModule(QWidget):
    stock_clicked_signal = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.current_df = None
        self.bar_data = None
        self.line_data = None
        self.stock_market_map = {}
        self.industry_map = {}
        self.latest_data = pd.DataFrame()
        self.merged_data = pd.DataFrame()
        self.multi_etf_df = pd.DataFrame()

        self.mapping = {
            "00981A": ("ezmoney", "統一-00981A (統一台股增長)"),
            "00403A": ("ezmoney", "統一-00403A (統一高息優選)"),
            "00991A": ("fhtrust", "復華-00991A (復華未來50)"),
            "00982A": ("capitalfund", "群益-00982A (台灣精選強棒)")
        }

        self.load_market_info()
        self.load_industry_info()
        self.init_ui()
        self.load_multi_etf_data()

    def load_market_info(self):
        csv_path = Path("data/stock_list.csv")
        if csv_path.exists():
            try:
                for enc in ['utf-8', 'utf-8-sig', 'big5']:
                    try:
                        df = pd.read_csv(csv_path, dtype=str, encoding=enc)
                        df.columns = [c.lower().strip() for c in df.columns]
                        code_col = 'stock_id' if 'stock_id' in df.columns else 'code' if 'code' in df.columns else 'id'
                        if code_col and 'market' in df.columns:
                            for _, row in df.iterrows(): self.stock_market_map[str(row[code_col]).strip()] = str(
                                row['market']).strip().upper()
                            break
                    except:
                        continue
            except:
                pass

    def load_industry_info(self):
        csv_path = Path("data/dj_industry.csv")
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path, dtype=str)
                for _, row in df.iterrows(): self.industry_map[str(row['sid']).strip()] = str(
                    row['dj_main_ind']).strip()
            except:
                pass

    def get_market_suffix(self, stock_id):
        return self.stock_market_map.get(str(stock_id), "TW")

    def load_watchlists_to_combo(self):
        json_path = Path("data/watchlist.json")
        self.combo_compare.blockSignals(True)
        self.combo_compare.clear()
        self.combo_compare.addItem("🔍 不比對")
        if json_path.exists():
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for key in data.keys(): self.combo_compare.addItem(f"📌 {key}")
            except:
                pass
        self.combo_compare.blockSignals(False)

    def init_ui(self):
        self.setStyleSheet("background-color: #0E0E0E; color: #E0E0E0;")
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)

        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("""
            QTabWidget::pane { border: 1px solid #333; }
            QTabBar::tab { background: #111; color: #888; padding: 10px 20px; font-size: 15px; font-weight: bold; border: 1px solid #333; border-bottom: none; }
            QTabBar::tab:selected { background: #222; color: #00E5FF; }
        """)

        self.tab_single = QWidget()
        self.tab_multi = QWidget()
        self.tabs.addTab(self.tab_single, "📊 單檔深度解析")
        self.tabs.addTab(self.tab_multi, "🌐 四大金剛戰情中心")
        main_layout.addWidget(self.tabs)

        self.init_single_tab()
        self.init_multi_tab()

    # --- 第一頁：單檔分析 (保持原樣) ---
    def init_single_tab(self):
        layout = QHBoxLayout(self.tab_single)
        layout.setContentsMargins(5, 5, 5, 5)
        splitter = QSplitter(Qt.Orientation.Horizontal)

        left_panel = QFrame()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)

        self.combo = QComboBox()
        self.combo.addItems([v[1] for v in self.mapping.values()])
        self.combo.setStyleSheet("QComboBox { background: #222; color: #FFF; padding: 5px; font-size: 14px; }")
        self.combo.currentIndexChanged.connect(self.on_combo_change)
        left_layout.addWidget(self.combo)

        top_bar = QWidget()
        top_layout = QHBoxLayout(top_bar)
        top_layout.setContentsMargins(0, 5, 0, 0)
        lbl_top = QLabel("🔥 持股排行")
        lbl_top.setStyleSheet("color: #FFD700; font-weight: bold;")
        self.combo_compare = QComboBox()
        self.load_watchlists_to_combo()
        self.combo_compare.setStyleSheet("QComboBox { background: #222; color: #FFF; padding: 2px; font-size: 13px; }")
        self.combo_compare.currentIndexChanged.connect(self.render_table)
        top_layout.addWidget(lbl_top)
        top_layout.addStretch()
        top_layout.addWidget(self.combo_compare)
        left_layout.addWidget(top_bar)

        self.stock_table = QTableWidget()
        self.stock_table.setColumnCount(4)
        self.stock_table.setHorizontalHeaderLabels(["代號", "名稱", "權重", "張數(千)"])
        self.stock_table.verticalHeader().setVisible(False)
        self.stock_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.stock_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self.stock_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.stock_table.setStyleSheet("""
            QTableWidget { background: #121212; border: 1px solid #333; gridline-color: #222; font-size: 14px; }
            QTableWidget::item:selected { background: #2A2A2A; color: #00E5FF; }
            QHeaderView::section { background: #1A1A1A; color: #888; border: none; padding: 4px; font-weight: bold; }
        """)
        header = self.stock_table.horizontalHeader()
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.stock_table.cellClicked.connect(self.on_table_clicked)
        left_layout.addWidget(self.stock_table)

        right_panel = QFrame()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(10, 0, 0, 0)

        right_header = QWidget()
        rh_layout = QHBoxLayout(right_header)
        rh_layout.setContentsMargins(0, 0, 0, 0)
        self.lbl_etf_info = QLabel("")
        self.lbl_etf_info.setStyleSheet("color: #FFFF00; font-weight: bold; font-size: 16px;")
        self.info_label = QLabel("💡 移動滑鼠查看數據 / 點擊長條圖連動個股")
        self.info_label.setStyleSheet("color: #888; font-size: 13px;")
        rh_layout.addWidget(self.lbl_etf_info)
        rh_layout.addStretch()
        rh_layout.addWidget(self.info_label)
        right_layout.addWidget(right_header)

        self.fig_change = Figure(facecolor='#0E0E0E')
        self.canvas_change = FigureCanvas(self.fig_change)
        self.canvas_change.mpl_connect('motion_notify_event', self.on_bar_hover)
        self.canvas_change.mpl_connect('button_press_event', self.on_bar_click)

        self.fig_trend = Figure(facecolor='#0E0E0E')
        self.canvas_trend = FigureCanvas(self.fig_trend)
        self.canvas_trend.mpl_connect('motion_notify_event', self.on_line_hover)

        right_layout.addWidget(self.canvas_change, stretch=4)
        right_layout.addWidget(self.canvas_trend, stretch=6)

        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setStretchFactor(0, 35)
        splitter.setStretchFactor(1, 65)
        layout.addWidget(splitter)
        self.on_combo_change(0)

    # --- 第二頁：四大金剛戰情中心 UI ---
    def init_multi_tab(self):
        layout = QVBoxLayout(self.tab_multi)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        nav_bar = QWidget()
        nav_layout = QHBoxLayout(nav_bar)
        nav_layout.setContentsMargins(0, 0, 0, 0)

        self.btn_overlap = QPushButton("🔲 持股重疊矩陣")
        self.btn_consensus = QPushButton("📈 籌碼共識追蹤")
        self.btn_style = QPushButton("🎯 選股風格雷達")
        self.btn_radar = QPushButton("📡 每日進出動向")
        self.nav_btns = [self.btn_overlap, self.btn_consensus, self.btn_style, self.btn_radar]

        for idx, btn in enumerate(self.nav_btns):
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn.setFixedHeight(35)
            btn.clicked.connect(lambda checked, i=idx: self.switch_multi_view(i))
            nav_layout.addWidget(btn)
        nav_layout.addStretch()
        layout.addWidget(nav_bar)

        self.multi_stack = QStackedWidget()
        self.page_overlap = QWidget()
        self.page_consensus = QWidget()
        self.page_style = QWidget()
        self.page_radar = QWidget()

        self.setup_overlap_page()
        self.setup_consensus_page()
        self.setup_style_page()
        self.setup_radar_page()

        self.multi_stack.addWidget(self.page_overlap)
        self.multi_stack.addWidget(self.page_consensus)
        self.multi_stack.addWidget(self.page_style)
        self.multi_stack.addWidget(self.page_radar)

        layout.addWidget(self.multi_stack)
        self.switch_multi_view(0)  # 預設顯示持股重疊

    def switch_multi_view(self, index):
        self.multi_stack.setCurrentIndex(index)
        for i, btn in enumerate(self.nav_btns):
            if i == index:
                btn.setStyleSheet(
                    "QPushButton { background: #00E5FF; color: #000; border: none; border-radius: 5px; font-size: 14px; font-weight: bold; padding: 0 15px; }")
            else:
                btn.setStyleSheet(
                    "QPushButton { background: #222; color: #AAA; border: 1px solid #444; border-radius: 5px; font-size: 14px; font-weight: bold; padding: 0 15px; } QPushButton:hover { background: #333; }")

    # --- 模組 1：持股重疊 UI ---
    def setup_overlap_page(self):
        layout = QVBoxLayout(self.page_overlap)

        # 矩陣表
        self.table_matrix = self.create_basic_table("🔲 ETF 兩兩重疊率矩陣 (該列ETF包含幾%的欄ETF持股)", [])
        layout.addWidget(self.table_matrix, 1)

        # 核心持股表
        self.table_core = self.create_basic_table("⭐ 三家以上共同持有之核心持股",
                                                  ["股票代號", "名稱", "持有家數", "名單", "合計權重"])
        layout.addWidget(self.table_core, 2)

    # --- 模組 2：籌碼共識 UI ---
    def setup_consensus_page(self):
        layout = QHBoxLayout(self.page_consensus)
        # 🔥 修改標題加上「近 3 日」
        self.table_buy = self.create_basic_table("↗️ 近3日共同加碼 Top 15 (2家以上同步買進)",
                                                 ["股票", "名稱", "動作ETF", "合計買進張數"])
        self.table_sell = self.create_basic_table("↘️ 近3日共同減碼 Top 15 (2家以上同步賣出)",
                                                  ["股票", "名稱", "動作ETF", "合計賣出張數"])
        layout.addWidget(self.table_buy)
        layout.addWidget(self.table_sell)

    # --- 模組 3：選股風格 UI ---
    def setup_style_page(self):
        layout = QVBoxLayout(self.page_style)

        # 🔥 新增說明 Hint
        hint_lbl = QLabel(
            "💡 說明：【前十大集中度】為前10大持股權重總和，數值越高代表越「重押」少數個股；產業會列出佈局最重的前三大成分股。")
        hint_lbl.setStyleSheet("color: #888; font-size: 13px; margin-bottom: 5px;")
        layout.addWidget(hint_lbl)

        self.table_industry = self.create_basic_table("🎯 ETF 產業與風格偏好",
                                                      ["ETF 代號", "第一大產業 (前三大成分股)",
                                                       "第二大產業 (前三大成分股)", "前十大集中度"])
        self.table_industry.table_ref.setWordWrap(True)  # 🔥 允許表格多行顯示
        layout.addWidget(self.table_industry)

    # --- 模組 4：進出動向 UI ---
    def setup_radar_page(self):
        layout = QHBoxLayout(self.page_radar)
        left_layout = QVBoxLayout()
        self.table_new = self.create_basic_table("🆕 今日新增持股", ["ETF", "股票", "名稱", "權重"])
        self.table_out = self.create_basic_table("🗑️ 今日完全移除", ["ETF", "股票", "名稱", "前日權重"])
        left_layout.addWidget(self.table_new)
        left_layout.addWidget(self.table_out)

        right_layout = QVBoxLayout()
        self.table_change = self.create_basic_table("↕️ 大幅調整排行", ["ETF", "股票", "名稱", "張數變化", "權重變化"])
        right_layout.addWidget(self.table_change)

        layout.addLayout(left_layout, 1)
        layout.addLayout(right_layout, 1)

    def create_basic_table(self, title, headers):
        container = QWidget()
        vbox = QVBoxLayout(container)
        vbox.setContentsMargins(0, 0, 0, 0)
        lbl = QLabel(title)
        lbl.setStyleSheet("color: #FFD700; font-weight: bold; font-size: 15px; padding: 5px 0;")
        table = QTableWidget()
        table.setColumnCount(len(headers))
        if headers:
            table.setHorizontalHeaderLabels(headers)
        table.verticalHeader().setVisible(False)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setStyleSheet("""
            QTableWidget { background: #121212; border: 1px solid #333; color: #EEE; }
            QHeaderView::section { background: #222; color: #AAA; border: none; padding: 4px; font-weight: bold;}
        """)

        # 🔥 修正太平洋間距：讓名稱類型的欄位伸展，其餘縮緊對齊
        if headers:
            header = table.horizontalHeader()
            for i, h in enumerate(headers):
                if h in ["名稱", "名單", "第一大產業 (前三大成分股)", "第二大產業 (前三大成分股)"]:
                    header.setSectionResizeMode(i, QHeaderView.ResizeMode.Stretch)
                else:
                    header.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)

        vbox.addWidget(lbl)
        vbox.addWidget(table)
        container.table_ref = table
        return container

    # --- 背景運算與四大模組資料填寫 ---
    def load_multi_etf_data(self):
        self.multi_worker = MultiETFDataWorker(self.mapping)
        self.multi_worker.multi_data_fetched.connect(self.process_multi_data)
        self.multi_worker.start()

    def process_multi_data(self, df):
        if df.empty: return
        self.multi_etf_df = df
        print(f"🌐 [四大金剛] 合併資料完成！總筆數: {len(df)}")
        self.update_all_modules()

    def update_all_modules(self):
        if self.multi_etf_df.empty: return
        df = self.multi_etf_df.copy()

        etf_latest_stocks = {}
        etf_latest_df = {}
        all_changes = []  # 用於雷達 (單日)
        consensus_changes = []  # 用於共識 (近3日)

        etfs = list(self.mapping.keys())

        for etf in etfs:
            sub_df = df[df['etf_id'] == etf]
            dates = sorted(sub_df['date'].unique())
            if not dates: continue

            t0 = dates[-1]
            df_t0 = sub_df[sub_df['date'] == t0]
            etf_latest_stocks[etf] = set(df_t0['stock_id'])
            etf_latest_df[etf] = df_t0

            # --- 單日變化 (供每日動向雷達使用) ---
            if len(dates) >= 2:
                t1 = dates[-2]
                merged_1d = pd.merge(df_t0, sub_df[sub_df['date'] == t1], on=['stock_id', 'name'],
                                     suffixes=('_now', '_prev'), how='outer').fillna(0)
                for _, row in merged_1d.iterrows():
                    all_changes.append({
                        'etf': etf, 'stock_id': str(row['stock_id']), 'name': str(row['name']),
                        'w_now': row['weight_now'], 'w_prev': row['weight_prev'],
                        's_now': row['shares_now'], 's_prev': row['shares_prev'],
                        's_diff': row['shares_now'] - row['shares_prev'],
                        'w_diff': row['weight_now'] - row['weight_prev']
                    })

            # --- 近 3 日變化 (供籌碼共識使用) --- 🔥 新增邏輯
            if len(dates) >= 2:
                t3_idx = max(0, len(dates) - 4)  # 往前推 3 個交易日 (含今天共4天)
                t3 = dates[t3_idx]
                merged_3d = pd.merge(df_t0, sub_df[sub_df['date'] == t3], on=['stock_id', 'name'],
                                     suffixes=('_now', '_prev'), how='outer').fillna(0)
                for _, row in merged_3d.iterrows():
                    consensus_changes.append({
                        'etf': etf, 'stock_id': str(row['stock_id']), 'name': str(row['name']),
                        's_diff': row['shares_now'] - row['shares_prev']
                    })

        # --- 模組 1：重疊矩陣與核心 (不變) ---
        matrix_table = self.table_matrix.table_ref
        matrix_table.setColumnCount(len(etfs))
        matrix_table.setHorizontalHeaderLabels(etfs)
        matrix_table.setVerticalHeaderLabels(etfs)
        matrix_table.verticalHeader().setVisible(True)
        matrix_table.setRowCount(len(etfs))
        for i, row_etf in enumerate(etfs):
            set_a = etf_latest_stocks.get(row_etf, set())
            for j, col_etf in enumerate(etfs):
                set_b = etf_latest_stocks.get(col_etf, set())
                if i == j:
                    item = QTableWidgetItem("—")
                    item.setBackground(QColor("#222"))
                elif not set_a:
                    item = QTableWidgetItem("0%")
                else:
                    overlap_pct = len(set_a & set_b) / len(set_a) * 100
                    item = QTableWidgetItem(f"{overlap_pct:.0f}%")
                    alpha = int(min(255, (overlap_pct / 100) * 200 + 30))
                    item.setBackground(QColor(100, 100, 200, alpha))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                matrix_table.setItem(i, j, item)
        matrix_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

        stock_to_etfs, stock_names, stock_weights = {}, {}, {}
        for etf, d_df in etf_latest_df.items():
            for _, r in d_df.iterrows():
                sid, name, w = str(r['stock_id']), str(r['name']), r['weight']
                if sid not in stock_to_etfs: stock_to_etfs[sid] = []; stock_weights[sid] = 0
                stock_to_etfs[sid].append(etf)
                stock_names[sid] = name
                stock_weights[sid] += w

        core_list = [
            [sid, stock_names[sid], f"{len(el)} 家", ", ".join(el), f"{stock_weights[sid]:.2f}%", stock_weights[sid]]
            for sid, el in stock_to_etfs.items() if len(el) >= 3]
        core_list.sort(key=lambda x: x[5], reverse=True)
        self._fill_table(self.table_core.table_ref, [c[:5] for c in core_list], QColor("#00E5FF"))

        # --- 模組 2：籌碼共識 (改用近 3 日的 consensus_changes) --- 🔥 改寫
        buy_counts, sell_counts = {}, {}
        for c in consensus_changes:
            sid, name, etf, diff = c['stock_id'], c['name'], c['etf'], c['s_diff']
            if diff > 0:
                if sid not in buy_counts: buy_counts[sid] = {'name': name, 'etfs': [], 'total_diff': 0}
                buy_counts[sid]['etfs'].append(etf)
                buy_counts[sid]['total_diff'] += diff
            elif diff < 0:
                if sid not in sell_counts: sell_counts[sid] = {'name': name, 'etfs': [], 'total_diff': 0}
                sell_counts[sid]['etfs'].append(etf)
                sell_counts[sid]['total_diff'] += diff

        buy_list = [[k, v['name'], ", ".join(v['etfs']), f"+{int(v['total_diff'] / 1000):,} 張", v['total_diff']] for
                    k, v in buy_counts.items() if len(v['etfs']) >= 2]
        sell_list = [[k, v['name'], ", ".join(v['etfs']), f"{int(v['total_diff'] / 1000):,} 張", v['total_diff']] for
                     k, v in sell_counts.items() if len(v['etfs']) >= 2]
        buy_list.sort(key=lambda x: x[4], reverse=True)
        sell_list.sort(key=lambda x: x[4])

        self._fill_table(self.table_buy.table_ref, [b[:4] for b in buy_list[:15]], QColor("#FF3333"))
        self._fill_table(self.table_sell.table_ref, [s[:4] for s in sell_list[:15]], QColor("#00FF00"))

        # --- 模組 3：選股風格 (加入前三大成分股明細) --- 🔥 改寫
        style_list = []
        for etf, d_df in etf_latest_df.items():
            if d_df.empty: continue
            d_df = d_df.copy()
            d_df['industry'] = d_df['stock_id'].map(self.industry_map).fillna('其他')
            ind_weights = d_df.groupby('industry')['weight'].sum().sort_values(ascending=False)

            def get_ind_str(idx):
                if len(ind_weights) > idx:
                    ind_name, ind_w = ind_weights.index[idx], ind_weights.iloc[idx]
                    # 抓出該產業權重最高的前3名股票
                    top_stocks = d_df[d_df['industry'] == ind_name].sort_values('weight', ascending=False).head(3)
                    stocks_str = "、".join([f"{r['name']}{r['weight']:.1f}%" for _, r in top_stocks.iterrows()])
                    return f"【{ind_name}】佔 {ind_w:.1f}%\n({stocks_str})"
                return "-"

            top10_w = d_df.sort_values('weight', ascending=False).head(10)['weight'].sum()
            style_list.append([etf, get_ind_str(0), get_ind_str(1), f"{top10_w:.1f}%"])

        self._fill_table(self.table_industry.table_ref, style_list, QColor("#FFD700"))
        self.table_industry.table_ref.resizeRowsToContents()  # 🔥 讓表格根據多行文字自動長高

        # --- 模組 4：進出動向雷達 (單日變化) ---
        new_list, out_list, change_list = [], [], []
        for c in all_changes:
            if c['s_prev'] == 0 and c['s_now'] > 0:
                new_list.append([c['etf'], c['stock_id'], c['name'], f"{c['w_now']:.2f}%"])
            elif c['s_prev'] > 0 and c['s_now'] == 0:
                out_list.append([c['etf'], c['stock_id'], c['name'], f"{c['w_prev']:.2f}%"])
            elif abs(c['s_diff']) > 0:
                change_list.append([c['etf'], c['stock_id'], c['name'], c['s_diff'], c['w_diff']])

        change_list.sort(key=lambda x: abs(x[3]), reverse=True)

        self._fill_table(self.table_new.table_ref, new_list, QColor("#FF3333"))
        self._fill_table(self.table_out.table_ref, out_list, QColor("#00FF00"))

        table_c = self.table_change.table_ref
        table_c.setRowCount(min(20, len(change_list)))
        for i, row in enumerate(change_list[:20]):
            for j in range(3): table_c.setItem(i, j, QTableWidgetItem(str(row[j])))
            s_diff, w_diff = row[3], row[4]
            s_str = f"{int(s_diff / 1000):+,} 張"
            w_str = f"{w_diff:+.2f}%"
            c = QColor("#FF3333") if s_diff > 0 else QColor("#00FF00")
            it_s, it_w = QTableWidgetItem(s_str), QTableWidgetItem(w_str)
            it_s.setForeground(c);
            it_w.setForeground(c)
            table_c.setItem(i, 3, it_s);
            table_c.setItem(i, 4, it_w)

    def _fill_table(self, table, data_list, color=None):
        table.setRowCount(len(data_list))
        for i, row in enumerate(data_list):
            for j, val in enumerate(row):
                item = QTableWidgetItem(str(val))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter if j > 1 else Qt.AlignmentFlag.AlignLeft)
                if color and j == len(row) - 1: item.setForeground(color)
                table.setItem(i, j, item)

    # --- 單檔操作邏輯保留 ---
    def on_combo_change(self, index):
        keys = list(self.mapping.keys())
        if index < len(keys):
            etf_id = keys[index]
            provider = self.mapping[etf_id][0]
            self.lbl_etf_info.setText(self.mapping[etf_id][1])
            self.stock_table.setRowCount(0)
            self.fig_change.clear()
            self.canvas_change.draw()
            self.fig_trend.clear()
            self.canvas_trend.draw()
            self.worker = ETFDataWorker(etf_id, provider)
            self.worker.data_fetched.connect(self.process_data)
            self.worker.start()

    def process_data(self, df, etf_id):
        if df.empty: return
        if 'date' in df.columns: df['date'] = pd.to_datetime(df['date'])
        self.current_df = df
        dates = sorted(df['date'].unique())
        if len(dates) < 1: return
        self.latest_date, self.latest_data = dates[-1], df[df['date'] == dates[-1]].sort_values('weight',
                                                                                                ascending=False)
        self.load_watchlists_to_combo()

        if len(dates) >= 2:
            self.merged_data = pd.merge(self.latest_data, df[df['date'] == dates[-2]], on=['stock_id', 'name'],
                                        suffixes=('_now', '_prev'), how='outer').fillna(0)
            self.merged_data['share_diff'] = self.merged_data['shares_now'] - self.merged_data['shares_prev']
            self.merged_data['pct_change'] = self.merged_data.apply(
                lambda r: (r['share_diff'] / r['shares_prev'] * 100) if r['shares_prev'] > 0 else 0, axis=1)

            def ca(r):
                y, t, p = r['shares_prev'], r['shares_now'], r['pct_change']
                if y == 0 and t > 0: return '★新買入'
                if y > 0 and t == 0: return '●清空'
                if p >= 50: return '▲大增'
                if p <= -50: return '▼大減'
                if 10 <= p < 50: return '△增持'
                if -50 < p <= -10: return '▽減持'
                return None

            self.merged_data['action_type'] = self.merged_data.apply(ca, axis=1)
            self.plot_changes(
                self.merged_data[self.merged_data['share_diff'].abs() > 0].sort_values('share_diff', key=abs,
                                                                                       ascending=False).head(
                    12).sort_values('pct_change'), self.latest_date)
        else:
            self.plot_changes(pd.DataFrame(), self.latest_date)

        self.render_table()
        if not self.latest_data.empty:
            fid, fname = str(self.latest_data.iloc[0]['stock_id']), str(self.latest_data.iloc[0]['name'])
            self.plot_trend(fid, fname, self.get_market_suffix(fid))
            self.stock_table.selectRow(0)

    def render_table(self):
        if self.latest_data.empty: return

        cl = []
        if self.combo_compare.currentText() != "🔍 不比對":
            try:
                cl = json.load(open(Path("data/watchlist.json"), 'r', encoding='utf-8')).get(
                    self.combo_compare.currentText().replace("📌 ", ""), [])
            except:
                pass

        self.stock_table.setRowCount(len(self.latest_data))
        for i, r in enumerate(self.latest_data.itertuples()):
            sid, nm, w, sh = str(r.stock_id), str(r.name), r.weight, r.shares
            ar, sc = "", QColor("#FFD700") if i < 10 else QColor("#AAA")

            if sid in cl:
                df = self.merged_data[self.merged_data['stock_id'] == sid].iloc[0][
                    'share_diff'] if not self.merged_data.empty and sid in self.merged_data['stock_id'].values else 0
                ar, sc = (" ⬆", QColor("#FF3333")) if df > 0 else (" ⬇", QColor("#00FF00")) if df < 0 else (" -",
                                                                                                            QColor(
                                                                                                                "#FFD700"))

            items = [QTableWidgetItem(x) for x in
                     (sid, nm, f"{w:.2f}%", f"{int(sh / 1000) if pd.notna(sh) else 0:,}{ar}")]

            # 🔥 修正字體加粗的寫法
            font = QFont()
            if i < 10 or ar:
                font.setBold(True)

            for j, it in enumerate(items):
                it.setTextAlignment(
                    Qt.AlignmentFlag.AlignCenter if j == 0 else Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter if j > 1 else Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                it.setFont(font)
                it.setForeground(sc if j == 3 else QColor("#FFD700") if i < 10 else QColor("#AAA"))
                self.stock_table.setItem(i, j, it)

    def plot_changes(self, df, dt):
        self.fig_change.clear();
        ax = self.fig_change.add_subplot(111);
        ax.set_facecolor('#0E0E0E')
        self.bar_data = df.reset_index(drop=True)
        if self.bar_data.empty: ax.text(0.5, 0.5, "期間無明顯持股變動", ha='center', va='center', color='#555',
                                        fontsize=16); ax.axis('off'); self.canvas_change.draw(); return
        y = np.arange(len(self.bar_data))
        ax.barh(y, self.bar_data['pct_change'],
                color=['#FF3333' if x >= 0 else '#00FF00' for x in self.bar_data['share_diff']], height=0.6)
        ax.set_yticks(y);
        ax.set_yticklabels(self.bar_data['name'], fontsize=11, fontweight='bold', color='#DDD');
        ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=100))
        ax.set_title(f" 資金流向戰報 (日期: {dt.strftime('%Y-%m-%d')})", color='white', fontsize=13, fontweight='bold',
                     loc='left');
        ax.tick_params(colors='#AAA', labelsize=10);
        ax.grid(axis='x', color='#333', ls=':')
        for s in ax.spines.values(): s.set_visible(False)
        self.canvas_change.draw()

    def plot_trend(self, sid, snm, mkt="TW"):
        if self.current_df is None: return
        td = self.current_df[self.current_df['stock_id'] == str(sid)].copy()
        if td.empty: return
        if td['date'].max() < self.current_df['date'].max():
            nr = td.iloc[-1].copy();
            nr['date'], nr['shares'], nr['weight'] = self.current_df['date'].max(), 0, 0.0
            td = pd.concat([td, pd.DataFrame([nr])], ignore_index=True)
        td = td.sort_values('date')
        pd_df = pd.DataFrame()
        p_pth = Path(f"data/cache/tw/{sid}_{mkt}.parquet")
        if p_pth.exists():
            try:
                pdf = pd.read_parquet(p_pth);
                pdf.columns = [c.capitalize() for c in pdf.columns];
                pdf.index = pd.to_datetime(pdf.index).tz_localize(None)
                pd_df = pdf[pdf.index >= td['date'].min()].copy()
            except:
                pass

        self.fig_trend.clear();
        ax1 = self.fig_trend.add_subplot(111);
        ax1.set_facecolor('#0E0E0E')
        ax2, ax3 = ax1.twinx(), ax1.twinx();
        ax3.spines['right'].set_position(('outward', 60))
        ax1.set_zorder(10);
        ax2.set_zorder(11);
        ax3.set_zorder(1);
        ax1.patch.set_visible(False);
        ax2.patch.set_visible(False)

        l3, = ax3.plot(td['date'], td['shares'], color='#FFF', lw=0.8, alpha=0.6, label='庫存(細線)')
        l1, = ax1.plot(td['date'], td['weight'], color='#00E5FF', lw=2, marker='o', ms=4, label='權重')
        ax1.fill_between(td['date'], td['weight'], color='#00E5FF', alpha=0.1)

        l2 = l4 = None
        if not pd_df.empty:
            l2, = ax2.plot(pd_df.index, pd_df['Close'], color='#FFD700', lw=1.5, ls='--', alpha=0.9, label='股價(右)')
            ts, tc, dcs = 0, 0.0, []
            for _, r in td.iterrows():
                try:
                    pidx = pd_df.index.get_indexer([r['date']], method='ffill')[0]
                except:
                    pidx = -1
                p = pd_df['Close'].iloc[pidx] if pidx != -1 else 0
                if p > 0:
                    if r['shares'] > ts:
                        tc += (r['shares'] - ts) * p; ts = r['shares']
                    elif r['shares'] < ts:
                        if r['shares'] == 0:
                            ts, tc = 0, 0.0
                        else:
                            tc = tc * (r['shares'] / ts); ts = r['shares']
                dcs.append(tc / ts if ts > 0 else 0.0)
            td['avg_cost'] = dcs
            if (td['shares'] > 0).any(): l4, = ax2.plot(td.loc[td['shares'] > 0, 'date'],
                                                        td.loc[td['shares'] > 0, 'avg_cost'], color='#FF00FF', ls=':',
                                                        marker='o', ms=4, lw=1.5, alpha=0.7, label='成本')
            lp = pd_df['Close'].iloc[-1];
            ax2.plot(pd_df.index[-1], lp, marker='o', color='#FFD700', ms=6)
            ax2.text(pd_df.index[-1], lp, f" {lp:.1f}", color='#FFD700', fontsize=10, fontweight='bold', va='center',
                     bbox=dict(boxstyle="round,pad=0.2", fc="#222", ec="#FFD700", alpha=0.8))

        self.line_data = {'etf': td, 'price': pd_df}
        ax1.yaxis.set_major_formatter(mtick.PercentFormatter())
        ax1.set_title(f" {sid} {snm}", color='white', fontsize=12, fontweight='bold', loc='left')
        ax1.tick_params(axis='y', colors='#00E5FF');
        ax2.tick_params(axis='y', colors='#FFD700')
        ax1.grid(True, color='#222', ls=':')
        for ax in [ax1, ax2, ax3]:
            for s in ax.spines.values(): s.set_visible(False)
        ax1.legend(handles=[l for l in [l1, l2, l3, l4] if l], loc='upper left', frameon=False, labelcolor='white')
        self.canvas_trend.draw()

    def on_bar_click(self, e):
        if not e.inaxes or self.bar_data is None or e.inaxes != self.fig_change.axes[0]: return
        y = int(round(e.ydata))
        if 0 <= y < len(self.bar_data):
            sid = str(self.bar_data.iloc[y]['stock_id'])
            for i in range(self.stock_table.rowCount()):
                if self.stock_table.item(i, 0).text() == sid: self.stock_table.selectRow(i); break
            self.plot_trend(sid, str(self.bar_data.iloc[y]['name']), self.get_market_suffix(sid))

    def on_bar_hover(self, e):
        if not e.inaxes or self.bar_data is None: return
        y = int(round(e.ydata))
        if 0 <= y < len(self.bar_data) and abs(e.ydata - y) < 0.4:
            r = self.bar_data.iloc[y]
            self.info_label.setText(
                f"<span style='color:#FFF; font-weight:bold;'>{r['name']}</span> | <span style='color:#AAA;'>{int(r['shares_prev'] / 1000)}→{int(r['shares_now'] / 1000)}張</span> | <span style='color:{'#FF3333' if r['share_diff'] >= 0 else '#00FF00'}; font-weight:bold;'>{r['action_type'] or ''} {int(r['share_diff'] / 1000):+,} ({r['pct_change']:+.1f}%)</span>")

    def on_line_hover(self, e):
        if not e.inaxes or self.line_data is None: return
        import matplotlib.dates as mdates
        try:
            dt = mdates.num2date(e.xdata).replace(tzinfo=None)
        except:
            return
        td, pd_df = self.line_data['etf'], self.line_data['price']
        r = td.loc[(td['date'] - dt).abs().idxmin()]
        idx = td.index.get_loc(r.name)
        txt = p_val = ""
        if idx > 0:
            pr = td.iloc[idx - 1]
            diff = r['shares'] - pr['shares']
            pct = (diff / pr['shares']) * 100 if pr['shares'] > 0 else 0
            txt = f" | <span style='color:{'#FF3333' if diff >= 0 else '#00FF00'};'>{int(diff / 1000):+,}張 ({pct:+.2f}%)</span>"
        if not pd_df.empty:
            try:
                p_val = f" | <span style='color:#FFD700;'>股價:{pd_df.iloc[pd_df.index.get_indexer([r['date']], method='nearest')[0]]['Close']:.1f}</span>"
            except:
                pass
        self.info_label.setText(
            f"<span style='color:#DDD;'>{r['date'].strftime('%Y-%m-%d')}</span> | <span style='color:#00E5FF;'>權重:{r['weight']}%</span> | <span style='color:#FFF;'>持股:{int(r['shares'] / 1000):,}張</span>{txt}{p_val} | <span style='color:#FF00FF;'>成本:{r.get('avg_cost', 0):.1f}</span>")

    def on_table_clicked(self, r, c):
        sid = self.stock_table.item(r, 0).text()
        self.plot_trend(sid, self.stock_table.item(r, 1).text(), self.get_market_suffix(sid))
        self.stock_clicked_signal.emit(f"{sid}_{self.get_market_suffix(sid)}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = ActiveETFModule()
    win.resize(1200, 700)
    win.show()
    sys.exit(app.exec())