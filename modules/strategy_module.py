import sys
import pandas as pd
from pathlib import Path
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QTableView, QHeaderView, QGroupBox, QComboBox,
                             QDoubleSpinBox, QPushButton, QCheckBox,
                             QAbstractItemView, QMenu, QMessageBox, QSplitter,
                             QScrollArea, QFrame, QDialog, QGridLayout, QDialogButtonBox)
from PyQt6.QtCore import Qt, pyqtSignal, QAbstractTableModel, QSortFilterProxyModel
from PyQt6.QtGui import QColor, QAction, QCursor

# --- 設定檔 ---

# 1. 欄位設定 (順序: 基本 -> 動能 -> 籌碼 -> 估值 -> 訊號)
COLUMN_CONFIG = {
    'sid': {'name': '代號', 'show': True, 'tip': '股票代號'},
    'name': {'name': '名稱', 'show': True, 'tip': '股票名稱'},
    'industry': {'name': '產業', 'show': True, 'tip': '所屬產業類別'},
    '現價': {'name': '股價', 'show': True, 'tip': '最新收盤價'},

    '漲幅5d': {'name': '5日%', 'show': False, 'tip': '近5日漲跌幅 (短線動能)'},
    '漲幅20d': {'name': '月漲幅', 'show': True, 'tip': '近20日漲跌幅 (波段強度)'},
    '漲幅60d': {'name': '季漲幅', 'show': True, 'tip': '近60日漲跌幅 (中長線趨勢)'},
    '量比': {'name': '量比', 'show': True, 'tip': '今日量 / 5日均量 (>1量增, >2爆量)'},
    'VCP壓縮': {'name': '波動度', 'show': True, 'tip': 'VCP指數，越低(<5)代表籌碼越安定'},

    'm_sum_5d': {'name': '融資5日', 'show': True, 'tip': '融資近5日增減 (負數代表散戶退場)'},
    't_sum_5d': {'name': '投信5日', 'show': True, 'tip': '投信近5日買賣超 (正數代表認養)'},
    'f_sum_5d': {'name': '外資5日', 'show': False, 'tip': '外資近5日買賣超'},

    'pe': {'name': '本益比', 'show': True, 'tip': '股價 / EPS (<15便宜)'},
    'pbr': {'name': '股淨比', 'show': False, 'tip': '股價 / 淨值 (<1低估)'},
    'yield': {'name': '殖利率', 'show': True, 'tip': '現金股利 / 股價 (>4%高息)'},

    '強勢特徵': {'name': '強勢特徵', 'show': True, 'tip': '系統自動偵測的策略訊號'}
}

# 2. 強勢特徵說明 (Tooltip)
TAG_DESCRIPTIONS = {
    '超強勢': 'RS強度 > 90，全市場最強的前 10% 股票',
    '波動壓縮': 'VCP < 3%，股價狹幅盤整，籌碼極度安定，可能變盤',
    '投信認養': '投信連續買超 3 天以上',
    '散戶退場': '融資今日大幅減少 > 200 張',
    '波段黑馬': '近一季漲幅 > 30%，趨勢向上',
    '突破30週': '股價帶量突破 30 週均線 (MA150)',
    '創季高': '股價創近 60 日新高',
    '20日盤整': '近 20 日股價在箱型區間整理',
    '策略_強勢多頭': '均線多頭排列 (MA5 > MA20 > MA60)',
}


# --- 欄位選擇視窗 (一次選完再關閉) ---
class ColumnSelectorDialog(QDialog):
    def __init__(self, config, parent=None):
        super().__init__(parent)
        self.setWindowTitle("👁️ 欄位顯示設定")
        self.config = config
        self.checkboxes = {}
        self.init_ui()
        self.setStyleSheet(
            "QDialog { background: #222; color: #FFF; } QCheckBox { color: #EEE; font-size: 14px; padding: 5px; }")

    def init_ui(self):
        layout = QVBoxLayout(self)
        grid = QGridLayout()

        row, col = 0, 0
        for key, info in self.config.items():
            chk = QCheckBox(info['name'])
            chk.setChecked(info['show'])
            chk.setToolTip(info['tip'])
            self.checkboxes[key] = chk
            grid.addWidget(chk, row, col)
            col += 1
            if col > 2:  # 3欄換行
                col = 0
                row += 1

        layout.addLayout(grid)

        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def get_selection(self):
        return {k: chk.isChecked() for k, chk in self.checkboxes.items()}


# --- 資料模型 ---
class StrategyTableModel(QAbstractTableModel):
    def __init__(self, df=pd.DataFrame(), visible_cols=[]):
        super().__init__()
        self._df = df
        self.visible_cols = visible_cols

    def update_data(self, df, visible_cols):
        self.beginResetModel()
        self._df = df
        self.visible_cols = visible_cols
        self.endResetModel()

    def rowCount(self, parent=None):
        return self._df.shape[0]

    def columnCount(self, parent=None):
        return len(self.visible_cols)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid(): return None
        col_key = self.visible_cols[index.column()]
        value = self._df.iloc[index.row()][col_key]

        if role == Qt.ItemDataRole.DisplayRole:
            if isinstance(value, (int, float)):
                if '漲幅' in col_key or 'yield' in col_key or 'VCP' in col_key: return f"{value:.2f}%"
                if col_key in ['pe', 'pbr', '量比']: return f"{value:.2f}"
                return f"{value:,.0f}"
            return str(value)

        if role == Qt.ItemDataRole.ForegroundRole:
            if isinstance(value, (int, float)):
                if '漲幅' in col_key or 'sum' in col_key or '買賣超' in col_key:
                    if value > 0: return QColor("#FF4444")
                    if value < 0: return QColor("#00CC00")
            if col_key == '強勢特徵' and value: return QColor("#FFD700")
            return QColor("#E0E0E0")

        if role == Qt.ItemDataRole.TextAlignmentRole:
            if isinstance(value, (int, float)) or col_key in ['現價']:
                return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
            return Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter

        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal:
            col_key = self.visible_cols[section]
            config = COLUMN_CONFIG.get(col_key, {})
            if role == Qt.ItemDataRole.DisplayRole: return config.get('name', col_key)
            if role == Qt.ItemDataRole.ToolTipRole: return config.get('tip', '')
        if orientation == Qt.Orientation.Vertical and role == Qt.ItemDataRole.DisplayRole:
            return str(section + 1)
        return None


# --- 三段式排序 Proxy Model ---
class ThreeStateSortProxy(QSortFilterProxyModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.last_col = -1
        self.sort_state = 0  # 0: None, 1: Asc, 2: Desc

    def sort(self, column, order):
        # 覆寫排序邏輯
        self.layoutAboutToBeChanged.emit()

        if column != self.last_col:
            # 換欄位，重置為 Asc
            self.sort_state = 1
            super().sort(column, Qt.SortOrder.AscendingOrder)
        else:
            # 同欄位，循環狀態
            self.sort_state = (self.sort_state + 1) % 3
            if self.sort_state == 0:
                # 復原 (設回 -1 代表不排序)
                super().sort(-1, Qt.SortOrder.AscendingOrder)
            elif self.sort_state == 1:
                super().sort(column, Qt.SortOrder.AscendingOrder)
            else:
                super().sort(column, Qt.SortOrder.DescendingOrder)

        self.last_col = column
        self.layoutChanged.emit()


class StrategyModule(QWidget):
    stock_clicked_signal = pyqtSignal(str)
    request_add_watchlist = pyqtSignal(str, str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.full_df = pd.DataFrame()
        self.display_df = pd.DataFrame()
        self.stock_list_df = pd.DataFrame()

        self.init_ui()
        self.load_stock_list()
        self.load_data()

    def init_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # --- 左側：控制面板 ---
        control_widget = QWidget()
        control_widget.setFixedWidth(280)
        # 深灰底，邊框
        control_widget.setStyleSheet("background-color: #1A1A1A; border-right: 1px solid #333;")
        ctrl_layout = QVBoxLayout(control_widget)
        ctrl_layout.setSpacing(12)
        ctrl_layout.setContentsMargins(10, 15, 10, 15)

        # 標題
        title = QLabel("🎯 戰略選股濾網")
        title.setStyleSheet("font-size: 16px; font-weight: bold; color: #00E5FF; border: none;")
        ctrl_layout.addWidget(title)

        # 功能按鈕
        btn_layout = QHBoxLayout()
        self.btn_reload = QPushButton("🔄 重新載入")
        self.btn_reload.setToolTip("當您在後台執行完運算腳本後，\n點此按鈕可立即讀取最新數據，無需重啟程式。")
        self.btn_reload.setStyleSheet("""
            QPushButton { background: #333; color: white; border: 1px solid #555; padding: 6px; border-radius: 4px; }
            QPushButton:hover { border-color: #00E5FF; background: #444; }
        """)
        self.btn_reload.clicked.connect(self.load_data)

        self.btn_cols = QPushButton("👁️ 欄位顯示")
        self.btn_cols.setToolTip("開啟視窗勾選想要顯示的欄位")
        self.btn_cols.setStyleSheet(self.btn_reload.styleSheet())
        self.btn_cols.clicked.connect(self.open_column_selector)

        btn_layout.addWidget(self.btn_reload)
        btn_layout.addWidget(self.btn_cols)
        ctrl_layout.addLayout(btn_layout)

        line = QFrame()
        line.setFrameShape(QFrame.Shape.HLine)
        line.setStyleSheet("color: #444;")
        ctrl_layout.addWidget(line)

        # 1. 產業
        lbl_ind = QLabel("📂 產業類別:")
        lbl_ind.setStyleSheet("color: #DDD; font-weight: bold; border: none;")
        ctrl_layout.addWidget(lbl_ind)

        self.combo_industry = QComboBox()
        # 下拉選單 CSS：強制白字，解決黑字問題
        self.combo_industry.setStyleSheet("""
            QComboBox { 
                padding: 5px; background: #252525; color: #FFF; border: 1px solid #555; border-radius: 3px;
            }
            QComboBox::drop-down { border: none; }
            QComboBox::down-arrow { image: none; border-left: 2px solid #888; border-bottom: 2px solid #888; width: 8px; height: 8px; transform: rotate(-45deg); margin-right: 5px;}
            QComboBox QAbstractItemView {
                background: #333; color: #FFF; selection-background-color: #00E5FF; selection-color: #000;
            }
        """)
        self.combo_industry.addItem("全部")
        self.combo_industry.currentIndexChanged.connect(self.apply_filters)
        ctrl_layout.addWidget(self.combo_industry)

        # 2. 數值篩選
        gb_val = QGroupBox("📊 數值過濾")
        # 群組框 CSS
        gb_val.setStyleSheet("""
            QGroupBox { border: 1px solid #444; margin-top: 8px; padding-top: 15px; font-weight: bold; color: #00E5FF; }
        """)
        gb_layout = QVBoxLayout(gb_val)
        gb_layout.setSpacing(10)

        # SpinBox CSS：修復上下按鈕
        spin_style = """
            QDoubleSpinBox { background: #222; color: #FFF; border: 1px solid #555; padding: 2px; }
            QDoubleSpinBox::up-button, QDoubleSpinBox::down-button { width: 15px; background: #444; }
            QDoubleSpinBox::up-button:hover, QDoubleSpinBox::down-button:hover { background: #666; }
        """

        def add_filter_row(label, spin_widget):
            row = QHBoxLayout()
            lbl = QLabel(label)
            lbl.setStyleSheet("color: #CCC; border: none;")
            row.addWidget(lbl)
            spin_widget.setStyleSheet(spin_style)
            spin_widget.valueChanged.connect(self.apply_filters)
            row.addWidget(spin_widget)
            gb_layout.addLayout(row)

        self.spin_yield = QDoubleSpinBox()
        self.spin_yield.setSuffix("%")
        self.spin_yield.setRange(0, 20)
        self.spin_yield.setSingleStep(0.5)
        add_filter_row("殖利率 >", self.spin_yield)

        self.spin_roc20 = QDoubleSpinBox()
        self.spin_roc20.setSuffix("%")
        self.spin_roc20.setRange(-50, 500)
        add_filter_row("月漲幅 >", self.spin_roc20)

        self.spin_pe = QDoubleSpinBox()
        self.spin_pe.setRange(0, 200)
        add_filter_row("本益比 <", self.spin_pe)
        self.spin_pe.setValue(0)  # 0 代表不限

        self.spin_vol_ratio = QDoubleSpinBox()
        self.spin_vol_ratio.setRange(0, 50)
        self.spin_vol_ratio.setSingleStep(0.1)
        add_filter_row("量比 >", self.spin_vol_ratio)

        ctrl_layout.addWidget(gb_val)

        # 3. 特徵標籤
        lbl_tag = QLabel("🔥 強勢特徵 (符合任一):")
        lbl_tag.setStyleSheet("color: #DDD; font-weight: bold; margin-top: 10px; border: none;")
        ctrl_layout.addWidget(lbl_tag)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("background: transparent; border: none;")
        self.tag_container = QWidget()
        self.tag_layout = QVBoxLayout(self.tag_container)
        self.tag_layout.setContentsMargins(2, 0, 0, 0)
        self.tag_layout.setSpacing(2)
        scroll.setWidget(self.tag_container)
        ctrl_layout.addWidget(scroll)

        # 狀態
        self.lbl_status = QLabel("就緒")
        self.lbl_status.setStyleSheet("color: #888; margin-top: 5px; border: none; font-size: 12px;")
        ctrl_layout.addWidget(self.lbl_status)

        self.chk_tags = []

        # --- 右側：表格 ---
        table_widget = QWidget()
        table_layout = QVBoxLayout(table_widget)
        table_layout.setContentsMargins(0, 0, 0, 0)

        self.table_view = QTableView()
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table_view.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table_view.setSortingEnabled(True)

        self.table_view.setStyleSheet("""
            QTableView { 
                background-color: #080808; 
                color: #E0E0E0; 
                gridline-color: #333; 
                alternate-background-color: #121212;
                font-size: 14px;
                border: none;
            }
            QHeaderView::section { 
                background-color: #2D2D2D; 
                color: #FFF; 
                padding: 5px; 
                border: none; 
                border-right: 1px solid #444;
                border-bottom: 1px solid #444;
                font-weight: bold;
            }
            QHeaderView::section:hover { background-color: #444; }
            QHeaderView::down-arrow { image: none; border-left: 5px solid transparent; border-right: 5px solid transparent; border-top: 5px solid #00E5FF; margin-right: 5px; }
            QHeaderView::up-arrow { image: none; border-left: 5px solid transparent; border-right: 5px solid transparent; border-bottom: 5px solid #00E5FF; margin-right: 5px; }

            QTableView::item:selected { background-color: #004466; color: #FFF; }
            QToolTip { background-color: #222; color: #FFF; border: 1px solid #00E5FF; padding: 5px; }
        """)

        self.model = StrategyTableModel()
        # 使用自訂的 3段式排序 Proxy
        self.proxy_model = ThreeStateSortProxy()
        self.proxy_model.setSourceModel(self.model)
        self.table_view.setModel(self.proxy_model)

        self.table_view.doubleClicked.connect(self.on_table_double_clicked)
        self.table_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self.open_context_menu)

        table_layout.addWidget(self.table_view)

        splitter.addWidget(control_widget)
        splitter.addWidget(table_widget)
        splitter.setStretchFactor(1, 1)
        layout.addWidget(splitter)

    def load_stock_list(self):
        try:
            path = Path(__file__).resolve().parent.parent / "data" / "stock_list.csv"
            if path.exists():
                self.stock_list_df = pd.read_csv(path, dtype=str)
                if 'stock_id' in self.stock_list_df.columns:
                    self.stock_list_df.set_index('stock_id', inplace=True)
        except:
            pass

    def load_data(self):
        try:
            base_path = Path(__file__).resolve().parent.parent
            f_path = base_path / "data" / "strategy_results" / "factor_snapshot.parquet"
            if not f_path.exists():
                f_path = base_path / "data" / "strategy_results" / "戰情室今日快照_全中文版.csv"

            if not f_path.exists():
                self.lbl_status.setText("❌ 無數據")
                return

            if f_path.suffix == '.parquet':
                df = pd.read_parquet(f_path)
            else:
                df = pd.read_csv(f_path)

            if df.empty: return

            self.full_df = df.copy()
            if 'sid' in self.full_df.columns:
                self.full_df['sid'] = self.full_df['sid'].astype(str).str.strip()

            # 強制轉換數值，確保排序正確
            for col in self.full_df.columns:
                if '漲幅' in col or 'sum' in col or col in ['現價', '量比', 'VCP壓縮', 'pe', 'pbr', 'yield']:
                    self.full_df[col] = pd.to_numeric(self.full_df[col], errors='coerce').fillna(0)

            self.update_industry_combo()
            self._update_tag_checkboxes()
            self.apply_filters()

            self.lbl_status.setText(f"數據更新時間: {pd.Timestamp.now().strftime('%H:%M:%S')}")

        except Exception as e:
            print(f"❌ 載入失敗: {e}")
            self.lbl_status.setText("數據錯誤")

    def update_industry_combo(self):
        if 'industry' in self.full_df.columns:
            industries = ["全部"] + sorted(self.full_df['industry'].dropna().unique().tolist())
            curr = self.combo_industry.currentText()
            self.combo_industry.blockSignals(True)
            self.combo_industry.clear()
            self.combo_industry.addItems(industries)
            if curr in industries: self.combo_industry.setCurrentText(curr)
            self.combo_industry.blockSignals(False)

    def _update_tag_checkboxes(self):
        # 清空
        while self.tag_layout.count():
            child = self.tag_layout.takeAt(0)
            if child.widget(): child.widget().deleteLater()
        self.chk_tags.clear()

        if '強勢特徵' not in self.full_df.columns: return

        all_tags = set()
        for tags in self.full_df['強勢特徵'].dropna():
            for t in str(tags).split(','):
                t = t.strip()
                if t: all_tags.add(t)

        for tag in sorted(list(all_tags)):
            chk = QCheckBox(tag)
            # Checkbox CSS: 白字
            chk.setStyleSheet("QCheckBox { color: #EEE; } QCheckBox::indicator:checked { background-color: #00E5FF; }")
            # 設定提示文字
            tip = TAG_DESCRIPTIONS.get(tag, "策略特徵")
            chk.setToolTip(tip)

            chk.stateChanged.connect(self.apply_filters)
            self.tag_layout.addWidget(chk)
            self.chk_tags.append(chk)

    def open_column_selector(self):
        """ 開啟欄位選擇視窗 """
        dlg = ColumnSelectorDialog(COLUMN_CONFIG, self)
        if dlg.exec():
            new_selection = dlg.get_selection()
            for k, v in new_selection.items():
                COLUMN_CONFIG[k]['show'] = v
            self.apply_filters()

    def apply_filters(self):
        if self.full_df.empty: return
        df = self.full_df.copy()

        # 1. 產業
        ind = self.combo_industry.currentText()
        if ind != "全部": df = df[df['industry'] == ind]

        # 2. 數值
        if self.spin_yield.value() > 0 and 'yield' in df.columns:
            df = df[df['yield'] >= self.spin_yield.value()]

        if self.spin_roc20.value() != 0 and '漲幅20d' in df.columns:
            df = df[df['漲幅20d'] >= self.spin_roc20.value()]

        if self.spin_pe.value() > 0 and 'pe' in df.columns:
            df = df[(df['pe'] > 0) & (df['pe'] <= self.spin_pe.value())]

        if self.spin_vol_ratio.value() > 0 and '量比' in df.columns:
            df = df[df['量比'] >= self.spin_vol_ratio.value()]

        # 3. 標籤
        selected_tags = [chk.text() for chk in self.chk_tags if chk.isChecked()]
        if selected_tags and '強勢特徵' in df.columns:
            mask = df['強勢特徵'].apply(lambda x: any(t in str(x) for t in selected_tags))
            df = df[mask]

        # 4. 顯示資料
        visible_cols = [k for k, v in COLUMN_CONFIG.items() if v['show'] and k in df.columns]
        self.display_df = df[visible_cols].copy()

        # 預設排序 (若原本沒排序)
        if '漲幅20d' in self.display_df.columns and self.proxy_model.sort_state == 0:
            self.display_df = self.display_df.sort_values('漲幅20d', ascending=False)

        self.model.update_data(self.display_df, visible_cols)
        # 刷新 Proxy
        self.proxy_model.invalidate()

        self.lbl_status.setText(f"篩選結果: {len(self.display_df)} 檔")

        # 調整欄寬
        header = self.table_view.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        for i, col in enumerate(visible_cols):
            if col == '強勢特徵':
                header.resizeSection(i, 200)
            elif col == 'name':
                header.resizeSection(i, 80)
            else:
                header.resizeSection(i, 70)

    def on_table_double_clicked(self, index):
        # 透過 Proxy 找回原始 Row
        src_idx = self.proxy_model.mapToSource(index)
        row = src_idx.row()
        sid = str(self.display_df.iloc[row]['sid'])

        # 自動判斷市場
        market = "TW"
        if not self.stock_list_df.empty and sid in self.stock_list_df.index:
            m_code = str(self.stock_list_df.loc[sid, 'market']).strip().upper()
            if m_code in ['TWO', 'OTC', '上櫃']: market = "TWO"

        full_id = f"{sid}_{market}"
        print(f"📡 發送訊號: {full_id}")
        self.stock_clicked_signal.emit(full_id)

    def open_context_menu(self, pos):
        menu = QMenu()
        menu.setStyleSheet(
            "QMenu { background: #222; color: #FFF; border: 1px solid #555; } QMenu::item:selected { background: #004466; }")

        add_menu = QMenu("➕ 加入自選群組", self)
        groups = ["我的持股", "觀察名單", "高股息"]
        for g in groups:
            action = QAction(g, self)
            action.triggered.connect(lambda checked, group=g: self.add_to_watchlist(group))
            add_menu.addAction(action)
        menu.addMenu(add_menu)
        menu.exec(QCursor.pos())

    def add_to_watchlist(self, group_name):
        rows = self.table_view.selectionModel().selectedRows()
        count = 0
        for idx in rows:
            src_idx = self.proxy_model.mapToSource(idx)
            sid = str(self.display_df.iloc[src_idx.row()]['sid'])
            self.request_add_watchlist.emit(sid, group_name)
            count += 1
        QMessageBox.information(self, "完成", f"已加入 {count} 檔至 {group_name}")