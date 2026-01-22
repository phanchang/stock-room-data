import sys
import json
import pandas as pd
from pathlib import Path
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, QTableWidget,
                             QTableWidgetItem, QHeaderView, QCheckBox, QPushButton,
                             QScrollArea, QSplitter, QGroupBox, QRadioButton, QButtonGroup,
                             QMenu, QMessageBox, QAbstractItemView, QApplication)
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QColor, QAction

# 引用現有的 Helper
from utils.indicator_index import load_indicator_index


class StrategyModule(QWidget):
    stock_clicked_signal = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.indicator_index = {}
        self.stock_list_df = None
        self.rev_data = {}
        self.chip_data = {}

        self.strategies_map = {
            "📊 5日盤整 (量縮)": "consol_5",
            "📊 10日盤整 (量縮)": "consol_10",
            "📊 20日盤整 (量縮)": "consol_20",
            "📊 60日盤整 (量縮)": "consol_60",
            "🚀 突破 30週均線 (爆量)": "break_30w",
            "🚀 創 30日新高": "high_30",
            "🚀 創 60日新高": "high_60",
            "🔥 強勢多頭排列": "strong_uptrend",
            "📈 回測 55MA 支撐 (均線向上)": "support_ma_55",
            "📈 回測 200MA 支撐 (均線向上)": "support_ma_200",
            "🟢 Vix 恐慌反轉 (綠柱轉灰)": "vix_reversal",
        }

        self.columns_config = [
            ("id", "代號", 60),
            ("name", "名稱", 80),
            ("close", "股價", 70),
            ("pct_5d", "5日%", 60),
            ("pct_3m", "3月%", 60),
            ("pct_6m", "半年%", 60),
            ("rev_mom", "營收MoM", 70),
            ("rev_yoy", "營收YoY", 70),
            ("holder_w", "法人買賣", 80),  # 顯示張數
            ("eps_acc", "累計EPS", 70)
        ]

        self.init_data()
        self.init_ui()

    def init_data(self):
        """ 載入所有需要的靜態資料與彙總表 """
        self.indicator_index = load_indicator_index()

        # 1. 股票基本資料
        try:
            self.stock_list_df = pd.read_csv("data/stock_list.csv", dtype=str)
            self.stock_list_df.set_index('stock_id', inplace=True)
            # 處理欄位名稱 (轉小寫去空白)
            self.stock_list_df.columns = [c.lower().strip() for c in self.stock_list_df.columns]
        except Exception:
            self.stock_list_df = pd.DataFrame()

        # 2. 載入全市場月營收 (來自 crawler_bulk_summary.py)
        self.rev_data = {}
        try:
            rev_path = Path("data/summary/all_revenue.csv")
            if rev_path.exists():
                df = pd.read_csv(rev_path, dtype=str)
                df.columns = [c.lower().strip() for c in df.columns]

                for _, row in df.iterrows():
                    sid = row.get('stock_id')
                    if sid:
                        self.rev_data[str(sid)] = {
                            'mom': row.get('mom', '-'),
                            'yoy': row.get('yoy', '-')
                        }
        except Exception as e:
            print(f"⚠️ 載入營收資料失敗: {e}")

        # 3. 載入全市場籌碼 (來自 crawler_bulk_summary.py)
        self.chip_data = {}
        try:
            chip_path = Path("data/summary/all_chips.csv")
            if chip_path.exists():
                df = pd.read_csv(chip_path, dtype=str)
                df.columns = [c.lower().strip() for c in df.columns]
                # 欄位是 stock_id, net_buy, net_buy_sheets
                for _, row in df.iterrows():
                    sid = row.get('stock_id')
                    if sid:
                        self.chip_data[str(sid)] = row.get('net_buy_sheets', '-')
        except Exception as e:
            print(f"⚠️ 載入籌碼資料失敗: {e}")

    def init_ui(self):
        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        # --- 左側：控制面板 ---
        control_panel = QWidget()
        control_panel.setFixedWidth(280)
        control_panel.setStyleSheet("background: #111; border-right: 1px solid #333;")
        c_layout = QVBoxLayout(control_panel)

        title = QLabel("策略選股濾網")
        title.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 16px; margin-bottom: 10px;")
        c_layout.addWidget(title)

        # Logic Group
        logic_group = QGroupBox("篩選邏輯")
        logic_group.setStyleSheet(
            "QGroupBox { border: 1px solid #444; margin-top: 10px; color: #DDD; font-weight: bold; }")
        l_layout = QHBoxLayout(logic_group)

        self.rb_intersect = QRadioButton("交集 (完全符合)")
        self.rb_union = QRadioButton("聯集 (符合任一)")
        self.rb_intersect.setChecked(True)

        self.logic_btn_group = QButtonGroup(self)
        self.logic_btn_group.addButton(self.rb_intersect)
        self.logic_btn_group.addButton(self.rb_union)
        self.logic_btn_group.buttonClicked.connect(self.run_screening)

        for rb in [self.rb_union, self.rb_intersect]:
            rb.setStyleSheet(
                "QRadioButton { color: #BBB; } QRadioButton::indicator:checked { background-color: #00E5FF; border: 2px solid #00E5FF; border-radius: 6px; }")

        l_layout.addWidget(self.rb_intersect)
        l_layout.addWidget(self.rb_union)
        c_layout.addWidget(logic_group)

        # Checkboxes
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("border: none; background: transparent;")

        scroll_content = QWidget()
        self.checks_layout = QVBoxLayout(scroll_content)
        self.checks_layout.setSpacing(8)

        self.checkboxes = {}
        for text, key in self.strategies_map.items():
            chk = QCheckBox(text)
            chk.setStyleSheet("""
                QCheckBox { color: #CCC; font-size: 14px; spacing: 5px; }
                QCheckBox::indicator { width: 18px; height: 18px; border: 1px solid #555; border-radius: 3px; background: #222; }
                QCheckBox::indicator:checked { background: #00E5FF; border-color: #00E5FF; }
                QCheckBox:disabled { color: #555; }
            """)

            if key not in self.indicator_index:
                chk.setText(f"{text} (無資料)")
                chk.setEnabled(False)

            self.checkboxes[key] = chk
            self.checks_layout.addWidget(chk)

        self.checks_layout.addStretch()
        scroll.setWidget(scroll_content)
        c_layout.addWidget(scroll)

        # Run Button
        btn_run = QPushButton("執行篩選")
        btn_run.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_run.setFixedHeight(40)
        btn_run.setStyleSheet("""
            QPushButton { background: #00E5FF; color: #000; font-weight: bold; font-size: 14px; border-radius: 5px; }
            QPushButton:hover { background: #00FFFF; }
            QPushButton:pressed { background: #00CCCC; }
        """)
        btn_run.clicked.connect(self.run_screening)
        c_layout.addWidget(btn_run)

        # --- 右側：結果表格 ---
        result_panel = QWidget()
        result_panel.setStyleSheet("background: #000;")
        r_layout = QVBoxLayout(result_panel)

        self.lbl_status = QLabel("請勾選左側策略並執行篩選...")
        self.lbl_status.setStyleSheet("color: #888; padding: 5px;")
        r_layout.addWidget(self.lbl_status)

        self.table = QTableWidget()
        self.setup_table()
        r_layout.addWidget(self.table)

        splitter.addWidget(control_panel)
        splitter.addWidget(result_panel)
        splitter.setStretchFactor(1, 1)

        main_layout.addWidget(splitter)

    def setup_table(self):
        col_names = [c[1] for c in self.columns_config]
        self.table.setColumnCount(len(col_names))
        self.table.setHorizontalHeaderLabels(col_names)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setAlternatingRowColors(True)

        self.table.setStyleSheet("""
            QTableWidget { background: #000; border: none; gridline-color: #222; color: #dcdcdc; font-size: 14px; }
            QTableWidget::item { padding: 4px; border-bottom: 1px solid #111; }
            QTableWidget::item:selected { background: #333; color: #FFF; }
            QTableWidget::item:hover { background: #1A1A1A; }
            QHeaderView::section { background: #111; color: #BBB; padding: 4px; border: none; font-weight: bold; border-bottom: 2px solid #333; }
        """)

        header = self.table.horizontalHeader()
        for i, cfg in enumerate(self.columns_config):
            header.setSectionResizeMode(i, QHeaderView.ResizeMode.Fixed)
            self.table.setColumnWidth(i, cfg[2])

        self.table.cellDoubleClicked.connect(self.on_row_double_clicked)
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.open_context_menu)

    def run_screening(self):
        selected_keys = [k for k, chk in self.checkboxes.items() if chk.isChecked()]

        if not selected_keys:
            self.table.setRowCount(0)
            self.lbl_status.setText("⚠️ 請至少勾選一個策略")
            return

        self.lbl_status.setText("篩選運算中...")
        QApplication.processEvents()

        # 設定回溯天數
        lookback_days = 3
        import datetime
        today = datetime.date.today()
        cutoff_date_str = (today - datetime.timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        sets = []
        for key in selected_keys:
            stock_data = self.indicator_index.get(key, {})
            valid_stocks = set()
            for stock_id, dates in stock_data.items():
                if dates:
                    last_date = dates[-1]
                    if last_date >= cutoff_date_str:
                        valid_stocks.add(stock_id)
            sets.append(valid_stocks)

        if not sets:
            final_stocks = set()
        else:
            if self.rb_intersect.isChecked():
                final_stocks = set.intersection(*sets)
            else:
                final_stocks = set.union(*sets)

        final_list = sorted(list(final_stocks))

        self.lbl_status.setText(f"篩選完成：近 {lookback_days} 日符合共 {len(final_list)} 檔")
        self.populate_table(final_list)

    def populate_table(self, stock_ids):
        self.table.setRowCount(0)
        self.table.setSortingEnabled(False)

        for row_idx, stock_id in enumerate(stock_ids):
            self.table.insertRow(row_idx)

            # 1. 取得基本資料
            market = "TW"
            name = stock_id
            if not self.stock_list_df.empty and stock_id in self.stock_list_df.index:
                name = self.stock_list_df.loc[stock_id].get('name', stock_id)
                market = self.stock_list_df.loc[stock_id].get('market', 'TW').upper()

            # 2. 讀取 K 線計算價格與漲幅
            price = 0.0
            pct_5d = 0.0
            pct_3m = 0.0
            pct_6m = 0.0

            parquet_path = Path(f"data/cache/tw/{stock_id}_{market}.parquet")
            if parquet_path.exists():
                try:
                    df = pd.read_parquet(parquet_path)
                    if not df.empty:
                        closes = df['Close'].values
                        price = closes[-1]

                        def calc_pct(days):
                            if len(closes) > days:
                                ref = closes[-(days + 1)]
                                if ref > 0: return ((price - ref) / ref) * 100
                            return 0.0

                        pct_5d = calc_pct(5)
                        pct_3m = calc_pct(60)
                        pct_6m = calc_pct(120)
                except:
                    pass

            # 3. 查表取得外部資料
            rev_info = self.rev_data.get(stock_id, {})
            rev_mom = rev_info.get('mom', '-')
            rev_yoy = rev_info.get('yoy', '-')
            holder_w = self.chip_data.get(stock_id, '-')
            eps_acc = "-"  # EPS 目前暫無彙總表

            # 4. 填入表格
            # ID
            item_id = QTableWidgetItem(stock_id)
            item_id.setData(Qt.ItemDataRole.UserRole, f"{stock_id}_{market}")
            self.table.setItem(row_idx, 0, item_id)

            # Name
            self.table.setItem(row_idx, 1, QTableWidgetItem(name))

            # Price
            it_price = QTableWidgetItem(f"{price:.1f}")
            it_price.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            self.table.setItem(row_idx, 2, it_price)

            # Pct Columns (5d, 3m, 6m)
            for c_idx, val in enumerate([pct_5d, pct_3m, pct_6m]):
                txt = f"{val:+.1f}%" if val != 0 else "-"
                it = QTableWidgetItem(txt)
                self._colorize_item(it, val)
                self.table.setItem(row_idx, 3 + c_idx, it)

            # Revenue (MoM, YoY)
            for c_idx, val_str in enumerate([rev_mom, rev_yoy]):
                txt = str(val_str) + "%" if val_str != '-' else '-'
                it = QTableWidgetItem(txt)
                self._colorize_text_val(it, val_str)
                self.table.setItem(row_idx, 6 + c_idx, it)

            # Chips (Holder)
            # 法人買賣張數，不加 %
            txt_holder = str(holder_w) if holder_w != '-' else '-'
            it_holder = QTableWidgetItem(txt_holder)
            self._colorize_text_val(it_holder, holder_w)
            self.table.setItem(row_idx, 8, it_holder)

            # EPS
            it_eps = QTableWidgetItem(eps_acc)
            it_eps.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.table.setItem(row_idx, 9, it_eps)

        self.table.setSortingEnabled(True)

    def _colorize_item(self, item, val):
        """ 處理數值型態的上色 """
        item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        if val > 0:
            item.setForeground(QColor("#FF3333"))
        elif val < 0:
            item.setForeground(QColor("#00FF00"))
        else:
            item.setForeground(QColor("#dcdcdc"))

    def _colorize_text_val(self, item, text_val):
        """ 處理字串型態的上色 """
        item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        try:
            # 移除 % 或 , 等符號後轉 float 判斷正負
            clean_val = str(text_val).replace('%', '').replace(',', '').strip()
            val = float(clean_val)
            if val > 0:
                item.setForeground(QColor("#FF3333"))
            elif val < 0:
                item.setForeground(QColor("#00FF00"))
            else:
                item.setForeground(QColor("#dcdcdc"))
        except:
            item.setForeground(QColor("#dcdcdc"))

    def on_row_double_clicked(self, row, col):
        item = self.table.item(row, 0)
        full_id = item.data(Qt.ItemDataRole.UserRole)
        self.stock_clicked_signal.emit(full_id)

    def open_context_menu(self, pos):
        menu = QMenu()
        add_action = QAction("➕ 加入自選清單", self)
        add_action.triggered.connect(self.add_to_watchlist)
        menu.addAction(add_action)
        menu.exec(self.table.viewport().mapToGlobal(pos))

    def add_to_watchlist(self):
        rows = self.table.selectionModel().selectedRows()
        # 這裡需要傳遞給 Main App 處理，目前先 print
        for r in rows:
            print(f"Add: {self.table.item(r.row(), 0).text()}")