import sys
import pandas as pd
from pathlib import Path
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QTableView, QHeaderView, QGroupBox, QComboBox,
                             QDoubleSpinBox, QPushButton, QCheckBox,
                             QAbstractItemView, QMenu, QMessageBox, QSplitter,
                             QScrollArea, QFrame, QDialog, QGridLayout,
                             QDialogButtonBox, QRadioButton, QButtonGroup, QToolButton,
                             QSizePolicy)
from PyQt6.QtCore import Qt, pyqtSignal, QAbstractTableModel, QSortFilterProxyModel, QThread, QTimer, QSize
from PyQt6.QtGui import QColor, QAction, QCursor, QFont

# ==========================================
# 設定區塊 (V3.9.1 - 全特徵回歸版)
# ==========================================
COLUMN_CONFIG = {
    'sid': {'name': '代號', 'show': True, 'tip': '股票代號', 'type': 'str'},
    'name': {'name': '名稱', 'show': True, 'tip': '股票名稱', 'type': 'str'},
    'industry': {'name': '產業', 'show': True, 'tip': '所屬產業類別', 'type': 'str'},

    '現價': {'name': '股價', 'show': True, 'tip': '最新收盤價', 'type': 'num'},
    '漲幅20d': {'name': '月漲幅', 'show': True, 'tip': '近20日漲跌幅', 'type': 'num'},
    'RS強度': {'name': 'RS強度', 'show': True, 'tip': '相對強度 (1-99)', 'type': 'num'},

    # [Fix] 名稱修正為 布林寬度(%)
    'bb_width': {'name': '布林寬度(%)', 'show': True, 'tip': '布林通道寬度 (愈小愈壓縮，<5%極致)', 'type': 'num'},
    'VCP壓縮': {'name': 'VCP(舊)', 'show': False, 'tip': '舊版VCP係數', 'type': 'num'},

    '量比': {'name': '量比', 'show': True, 'tip': '今日量 / 5日均量', 'type': 'num'},

    # 籌碼
    't_net_today': {'name': '投信今日', 'show': False, 'tip': '投信今日買賣超', 'type': 'num'},
    't_sum_5d': {'name': '投信5日', 'show': True, 'tip': '投信5日累計', 'type': 'num'},
    't_streak': {'name': '投信連買', 'show': True, 'tip': '投信連續買超天數', 'type': 'num'},
    'f_sum_5d': {'name': '外資5日', 'show': True, 'tip': '外資5日累計', 'type': 'num'},
    'f_streak': {'name': '外資連買', 'show': True, 'tip': '外資連續買超天數', 'type': 'num'},
    'm_sum_5d': {'name': '融資5日', 'show': True, 'tip': '融資5日累計', 'type': 'num'},

    # 基本面
    'rev_yoy': {'name': '月YoY%', 'show': True, 'tip': '最新月營收 YoY%', 'type': 'num'},
    'rev_cum_yoy': {'name': '累營YoY%', 'show': True, 'tip': '當年累計營收 YoY%', 'type': 'num'},
    'eps_q': {'name': 'EPS(累)', 'show': True, 'tip': '累計季 EPS', 'type': 'num'},
    'pe': {'name': '本益比', 'show': True, 'tip': 'PE Ratio', 'type': 'num'},
    'yield': {'name': '殖利率', 'show': True, 'tip': '現金殖利率', 'type': 'num'},

    '強勢特徵': {'name': '強勢特徵', 'show': True, 'tip': '策略觸發訊號', 'type': 'str'}
}

DEFAULT_ACTIVE_FILTERS = ['bb_width', 'RS強度', '量比', '漲幅20d', 't_streak']

NUMERIC_FILTER_CONFIG = [
    # [Fix] 名稱同步修正
    {'key': 'bb_width', 'label': '布林寬度(%)', 'min': 0, 'max': 50, 'step': 0.5, 'suffix': '%'},

    {'key': 'RS強度', 'label': 'RS強度', 'min': 0, 'max': 99, 'step': 1.0, 'suffix': ''},
    {'key': '量比', 'label': '量比 (倍)', 'min': 0, 'max': 50, 'step': 0.5, 'suffix': ''},
    {'key': '漲幅20d', 'label': '月漲幅 (%)', 'min': -50, 'max': 500, 'step': 5.0, 'suffix': '%'},
    {'key': 'rev_cum_yoy', 'label': '累營年增 (%)', 'min': -50, 'max': 5000, 'step': 5.0, 'suffix': '%'},
    {'key': 'pe', 'label': '本益比', 'min': 0, 'max': 200, 'step': 1.0, 'suffix': ''},
    {'key': 'yield', 'label': '殖利率 (%)', 'min': 0, 'max': 100, 'step': 1.0, 'suffix': '%'},
    {'key': 't_streak', 'label': '投信連買(日)', 'min': 0, 'max': 30, 'step': 1, 'suffix': ''},
    {'key': 'f_streak', 'label': '外資連買(日)', 'min': 0, 'max': 30, 'step': 1, 'suffix': ''},
    {'key': 'm_sum_5d', 'label': '融資5日(張)', 'min': -50000, 'max': 50000, 'step': 100, 'suffix': ''},
]

TAG_CATEGORIES = {
    "🔥 趨勢型態": ["主力掃單(ILSS)", "土洋對作", "超強勢", "突破30週", "創季高", "創月高", "強勢多頭", "波段黑馬",
                   "假跌破"],
    "📉 整理型態": ["極度壓縮", "波動壓縮", "盤整5日", "盤整10日", "盤整20日", "盤整60日", "Vix反轉"],
    "💰 籌碼支撐": ["投信認養", "散戶退場", "回測季線", "回測年線"]
}

TAG_TOOLTIPS = {
    '主力掃單(ILSS)': 'MA200向上 + 跌破新低後爆量站回 + 營收優 + 散戶退',
    '極度壓縮': '布林寬度 < 5%，極致籌碼沉澱',
    '波動壓縮': '布林寬度 < 8%，進入整理區間',
    '盤整60日': '近60日布林寬度皆 < 18% (季級別大底)',
    '土洋對作': '投信在賣但外資持續吃貨 (籌碼換手)',
    '超強勢': 'RS 強度 > 90，市場前 10% 強勢股',
    '波段黑馬': '近60日漲幅 > 30%',
    '回測年線': '股價回測 200 日均線 (年線) 有撐',
    'Vix反轉': '恐慌指數 Vix 短線反轉',
}

GLOBAL_STYLE = """
    QWidget { font-family: "Microsoft JhengHei", "Segoe UI"; font-size: 16px; background-color: #000; color: #EEE; }
    QDialog, QMessageBox { background-color: #111; border: 1px solid #333; }
    QPushButton, QToolButton { 
        background-color: #222; color: #CCC; border: 1px solid #444; 
        padding: 6px; border-radius: 4px; font-weight: bold; font-size: 14px;
    }
    QPushButton:hover, QToolButton:hover { background-color: #333; border-color: #00E5FF; color: #FFF; }
    QDoubleSpinBox { background: #000; color: #00E5FF; border: 1px solid #444; padding: 4px; font-weight: bold; font-size: 16px; }
    QComboBox { background: #000; color: #FFF; border: 1px solid #444; padding: 6px; }
    QComboBox::drop-down { border: none; }
    QComboBox QAbstractItemView { background: #111; color: #FFF; selection-background-color: #00E5FF; selection-color: #000; }
    QToolTip { background-color: #000; color: #00E5FF; border: 1px solid #555; padding: 5px; }
    QMenu { background-color: #111; color: #FFF; border: 1px solid #555; }
    QMenu::item:selected { background-color: #00E5FF; color: #000; }
    QCheckBox { background: transparent; color: #DDD; }
    QCheckBox::indicator:checked { background-color: #00E5FF; border: 1px solid #00E5FF; }
    QCheckBox::indicator:unchecked { background-color: #111; border: 1px solid #555; }

    QLabel.category-label { color: #00E5FF; font-weight: bold; font-size: 18px; margin-top: 10px; margin-bottom: 2px; border-bottom: 1px solid #333; background: transparent; }
"""


class DataLoaderThread(QThread):
    data_loaded = pyqtSignal(pd.DataFrame)
    error_occurred = pyqtSignal(str)

    def run(self):
        try:
            base_path = Path(__file__).resolve().parent.parent
            f_path = base_path / "data" / "strategy_results" / "factor_snapshot.parquet"
            csv_path = base_path / "data" / "strategy_results" / "戰情室今日快照_全中文版.csv"
            df = pd.DataFrame()
            if f_path.exists():
                df = pd.read_parquet(f_path)
            elif csv_path.exists():
                df = pd.read_csv(csv_path)
            else:
                self.error_occurred.emit("無數據");
                return
            if 'sid' in df.columns: df['sid'] = df['sid'].astype(str).str.strip()
            for col in df.columns:
                if col in COLUMN_CONFIG and COLUMN_CONFIG[col]['type'] == 'num':
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            self.data_loaded.emit(df)
        except Exception as e:
            self.error_occurred.emit(str(e))


class FilterSelectionDialog(QDialog):
    def __init__(self, all_filters, active_keys, parent=None):
        super().__init__(parent)
        self.setWindowTitle("⚙️ 自訂濾網項目")
        self.all_filters = all_filters
        self.checkboxes = {}
        self.active_keys = active_keys
        self.setStyleSheet(GLOBAL_STYLE)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        grid = QGridLayout()
        row, col = 0, 0
        sorted_configs = sorted(self.all_filters, key=lambda x: x['key'] not in self.active_keys)
        for cfg in sorted_configs:
            key = cfg['key']
            chk = QCheckBox(cfg['label'])
            chk.setChecked(key in self.active_keys)
            self.checkboxes[key] = chk
            grid.addWidget(chk, row, col)
            col += 1
            if col > 2: col = 0; row += 1
        layout.addLayout(grid)
        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def get_selected_keys(self):
        return [k for k, chk in self.checkboxes.items() if chk.isChecked()]


class ColumnSelectorDialog(QDialog):
    def __init__(self, config, parent=None):
        super().__init__(parent)
        self.setWindowTitle("👁️ 欄位設定")
        self.config = config
        self.checkboxes = {}
        self.setStyleSheet(GLOBAL_STYLE)
        self.resize(600, 400)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        groups = {
            "基礎資訊": ['sid', 'name', 'industry', '現價', '漲幅20d', 'RS強度', 'bb_width'],
            "籌碼分析": ['t_sum_5d', 't_streak', 'f_sum_5d', 'f_streak', 'm_sum_5d', '量比'],
            "基本面": ['rev_yoy', 'rev_cum_yoy', 'eps_q', 'pe', 'yield'],
            "進階/其他": []
        }
        categorized_keys = set([k for g in groups.values() for k in g])
        groups["進階/其他"] = [k for k in self.config.keys() if k not in categorized_keys]

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        content_widget = QWidget()
        main_grid = QVBoxLayout(content_widget)

        for group_name, keys in groups.items():
            if not keys: continue
            lbl = QLabel(group_name)
            lbl.setStyleSheet("color: #00E5FF; font-weight: bold; margin-top: 10px; border-bottom: 1px solid #333;")
            main_grid.addWidget(lbl)

            grid = QGridLayout()
            row, col = 0, 0
            for key in keys:
                if key not in self.config: continue
                info = self.config[key]
                chk = QCheckBox(info['name'])
                chk.setChecked(info['show'])
                self.checkboxes[key] = chk
                grid.addWidget(chk, row, col)
                col += 1
                if col > 3: col = 0; row += 1
            main_grid.addLayout(grid)

        main_grid.addStretch()
        scroll.setWidget(content_widget)
        layout.addWidget(scroll)

        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def get_selection(self):
        return {k: chk.isChecked() for k, chk in self.checkboxes.items()}


class RangeFilterWidget(QWidget):
    value_changed = pyqtSignal()

    def __init__(self, config):
        super().__init__()
        self.key = config['key']
        self.config = config
        self.setStyleSheet("background: transparent;")

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(5)

        self.lbl_name = QLabel(config['label'])
        self.lbl_name.setStyleSheet("color: #DDD; font-size: 15px; border:none; background: transparent;")
        self.lbl_name.setFixedWidth(110)
        layout.addWidget(self.lbl_name)

        self.spin_min = QDoubleSpinBox()
        self.setup_spin(self.spin_min, config['min'], config['suffix'])
        self.spin_min.setFixedWidth(90)
        layout.addWidget(self.spin_min)

        lbl_tilde = QLabel("~")
        lbl_tilde.setStyleSheet("color:#555; border:none; background:transparent;")
        lbl_tilde.setFixedWidth(15)
        lbl_tilde.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(lbl_tilde)

        self.spin_max = QDoubleSpinBox()
        self.setup_spin(self.spin_max, config['max'], config['suffix'])
        self.spin_max.setFixedWidth(90)
        layout.addWidget(self.spin_max)

        layout.addStretch()

    def setup_spin(self, spin, default_val, suffix):
        spin.setRange(-999999, 999999)
        spin.setDecimals(1 if '張' not in self.config.get('label', '') else 0)
        spin.setSingleStep(self.config['step'])
        spin.setSuffix(suffix)
        spin.setValue(default_val)
        spin.valueChanged.connect(self.emit_change)

    def emit_change(self): self.value_changed.emit()

    def is_modified(self): return (self.spin_min.value() != self.config['min']) or (
            self.spin_max.value() != self.config['max'])

    def reset(self):
        self.spin_min.blockSignals(True);
        self.spin_max.blockSignals(True)
        self.spin_min.setValue(self.config['min']);
        self.spin_max.setValue(self.config['max'])
        self.spin_min.blockSignals(False);
        self.spin_max.blockSignals(False);
        self.emit_change()


class StrategyTableModel(QAbstractTableModel):
    def __init__(self, df=pd.DataFrame(), visible_cols=[]):
        super().__init__()
        self._df = df;
        self.visible_cols = visible_cols

    def update_data(self, df, visible_cols):
        self.beginResetModel();
        self._df = df;
        self.visible_cols = visible_cols;
        self.endResetModel()

    def rowCount(self, parent=None):
        return self._df.shape[0]

    def columnCount(self, parent=None):
        return len(self.visible_cols)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid(): return None
        col_key = self.visible_cols[index.column()]
        value = self._df.iloc[index.row()][col_key]

        if role == Qt.ItemDataRole.UserRole: return value
        if role == Qt.ItemDataRole.DisplayRole:
            if isinstance(value, (int, float)):
                if col_key in ['RS強度', 'pe', '量比', 'eps_q']: return f"{value:.1f}"
                if 'rev_now' in col_key: return f"{value:,.0f}"
                if '漲幅' in col_key or 'yield' in col_key or 'width' in col_key or 'yoy' in col_key: return f"{value:.2f}%"
                if 'sum' in col_key or 'net' in col_key: return f"{value:,.0f}"
                if 'streak' in col_key: return f"{int(value)}"
                return f"{value:,.2f}"
            return str(value)
        if role == Qt.ItemDataRole.ToolTipRole:
            if col_key == '強勢特徵' and isinstance(value, str):
                tags = [t.strip() for t in value.split(',')]
                tips = [f"• {t}: {TAG_TOOLTIPS.get(t, '')}" for t in tags]
                return "\n".join(tips)
            return COLUMN_CONFIG.get(col_key, {}).get('tip', '')
        if role == Qt.ItemDataRole.ForegroundRole:
            if isinstance(value, (int, float)):
                if '漲幅' in col_key or 'sum' in col_key or '買賣超' in col_key or 'yoy' in col_key or 'eps' in col_key or 'streak' in col_key:
                    if value > 0: return QColor("#FF4444")
                    if value < 0: return QColor("#00CC00")
            if col_key == '強勢特徵' and value:
                if 'ILSS' in str(value): return QColor("#FF00FF")  # ILSS 特殊色 (洋紅)
                if '土洋' in str(value): return QColor("#FFFF00")  # 黃色
                return QColor("#E0E0E0")
            return QColor("#E0E0E0")
        if role == Qt.ItemDataRole.TextAlignmentRole:
            if isinstance(value, (int, float)) or col_key == '現價':
                return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
            return Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal:
            col_key = self.visible_cols[section]
            config = COLUMN_CONFIG.get(col_key, {})
            if role == Qt.ItemDataRole.DisplayRole: return config.get('name', col_key)
            if role == Qt.ItemDataRole.ToolTipRole: return config.get('tip', '')
        return None


class NumericSortProxy(QSortFilterProxyModel):
    def lessThan(self, left, right):
        l_val = self.sourceModel().data(left, Qt.ItemDataRole.UserRole)
        r_val = self.sourceModel().data(right, Qt.ItemDataRole.UserRole)
        if l_val is None: l_val = -999999;
        if r_val is None: r_val = -999999
        try:
            return float(l_val) < float(r_val)
        except:
            return str(l_val) < str(r_val)


class StrategyModule(QWidget):
    stock_clicked_signal = pyqtSignal(str)
    request_add_watchlist = pyqtSignal(str, str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet(GLOBAL_STYLE)
        self.full_df = pd.DataFrame()
        self.display_df = pd.DataFrame()
        self.stock_list_df = pd.DataFrame()
        self.dynamic_filters = []
        self.active_filter_keys = DEFAULT_ACTIVE_FILTERS.copy()
        self.is_filters_expanded = True

        self.debounce_timer = QTimer()
        self.debounce_timer.setSingleShot(True)
        self.debounce_timer.interval = 300
        self.debounce_timer.timeout.connect(self.apply_filters_real)

        self.init_ui()
        self.load_stock_list()
        QTimer.singleShot(100, self.load_data)

    def init_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # === 左側控制面板 ===
        control_widget = QWidget()
        control_widget.setFixedWidth(400)
        control_widget.setStyleSheet("background-color: #050505; border-right: 1px solid #222;")
        ctrl_layout = QVBoxLayout(control_widget)
        ctrl_layout.setSpacing(10)
        ctrl_layout.setContentsMargins(10, 15, 10, 15)

        # 1. 標題與重整
        title_box = QHBoxLayout()
        title = QLabel("🎯 戰略選股")
        title.setStyleSheet(
            "font-size: 24px; font-weight: bold; color: #00E5FF; border: none; background: transparent;")
        title_box.addWidget(title)
        title_box.addStretch()
        self.btn_reload = QToolButton();
        self.btn_reload.setText("🔄");
        self.btn_reload.clicked.connect(self.load_data)
        self.btn_cols = QToolButton();
        self.btn_cols.setText("👁️");
        self.btn_cols.clicked.connect(self.open_column_selector)
        title_box.addWidget(self.btn_reload);
        title_box.addWidget(self.btn_cols)
        ctrl_layout.addLayout(title_box)
        ctrl_layout.addWidget(self._create_hline())

        # 2. 產業選擇
        lbl_ind = QLabel("📂 產業類別")
        lbl_ind.setProperty("class", "category-label")
        ctrl_layout.addWidget(lbl_ind)
        self.combo_industry = QComboBox()
        self.combo_industry.addItem("全部")
        self.combo_industry.currentIndexChanged.connect(self.apply_filters_debounce)
        ctrl_layout.addWidget(self.combo_industry)

        # 3. 數值過濾
        filter_header_box = QHBoxLayout()
        lbl_val = QLabel("📊 數值過濾")
        lbl_val.setProperty("class", "category-label")

        self.btn_filter_setting = QToolButton()
        self.btn_filter_setting.setObjectName("setting_btn")
        self.btn_filter_setting.setText("⚙️")
        self.btn_filter_setting.setToolTip("選擇要顯示的濾網")
        self.btn_filter_setting.clicked.connect(self.open_filter_setting)

        self.btn_toggle_filters = QToolButton()
        self.btn_toggle_filters.setObjectName("toggle_btn")
        self.btn_toggle_filters.setText("▼")
        self.btn_toggle_filters.clicked.connect(self.toggle_filters)

        filter_header_box.addWidget(lbl_val)
        filter_header_box.addStretch()
        filter_header_box.addWidget(self.btn_filter_setting)
        filter_header_box.addWidget(self.btn_toggle_filters)
        ctrl_layout.addLayout(filter_header_box)

        self.filter_container_widget = QWidget()
        self.filter_layout = QVBoxLayout(self.filter_container_widget)
        self.filter_layout.setContentsMargins(0, 0, 0, 0)
        self.filter_layout.setSpacing(5)

        self.btn_reset = QPushButton("🧹 清除條件")
        self.btn_reset.setFixedSize(120, 30)
        self.btn_reset.setStyleSheet("color: #666; font-weight: bold;")
        self.btn_reset.clicked.connect(self.reset_filters)

        self.filter_area = QWidget()
        filter_area_layout = QVBoxLayout(self.filter_area)
        filter_area_layout.setContentsMargins(0, 0, 0, 0)
        filter_area_layout.addWidget(self.btn_reset, alignment=Qt.AlignmentFlag.AlignRight)
        filter_area_layout.addWidget(self.filter_container_widget)

        ctrl_layout.addWidget(self.filter_area)
        self.rebuild_filter_ui()

        # 4. 強勢特徵 (Tag)
        lbl_tag = QLabel("🔥 強勢特徵")
        lbl_tag.setProperty("class", "category-label")
        ctrl_layout.addWidget(lbl_tag)

        logic_layout = QHBoxLayout()
        self.logic_group = QButtonGroup(self)
        self.rb_and = QRadioButton("交集 (AND)");
        self.rb_or = QRadioButton("聯集 (OR)")
        self.rb_and.setStyleSheet("color: #AAA; border:none; background: transparent;")
        self.rb_or.setStyleSheet("color: #AAA; border:none; background: transparent;")
        self.rb_and.setChecked(True)
        self.logic_group.addButton(self.rb_and);
        self.logic_group.addButton(self.rb_or)
        self.rb_and.toggled.connect(self.apply_filters_debounce)
        self.rb_or.toggled.connect(self.apply_filters_debounce)
        logic_layout.addWidget(self.rb_and);
        logic_layout.addWidget(self.rb_or)
        ctrl_layout.addLayout(logic_layout)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("background: transparent; border: none;")
        self.tag_container = QWidget()
        self.tag_layout = QVBoxLayout(self.tag_container)
        self.tag_layout.setContentsMargins(0, 0, 0, 0)
        scroll.setWidget(self.tag_container)
        ctrl_layout.addWidget(scroll)

        self.lbl_status = QLabel("就緒");
        self.lbl_status.setStyleSheet(
            "color: #666; font-size: 14px; margin-top: 5px; border:none; background: transparent;")
        ctrl_layout.addWidget(self.lbl_status)
        self.chk_tags = {}

        # === 右側表格 ===
        table_widget = QWidget()
        table_layout = QVBoxLayout(table_widget)
        table_layout.setContentsMargins(0, 0, 0, 0)
        self.table_view = QTableView()
        self.table_view.setAlternatingRowColors(False)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table_view.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.table_view.verticalHeader().setVisible(False)
        self.table_view.horizontalHeader().setStretchLastSection(True)
        self.table_view.setSortingEnabled(True)
        self.table_view.setStyleSheet("""
            QTableView { background-color: #000000; color: #E0E0E0; gridline-color: #222; font-size: 16px; border: none; }
            QHeaderView::section { background-color: #111; color: #AAA; padding: 6px; border-right: 1px solid #222; border-bottom: 2px solid #333; font-weight: bold; font-size: 16px; }
            QTableView::item:selected { background-color: #004466; color: #FFF; }
        """)

        self.model = StrategyTableModel()
        self.proxy_model = NumericSortProxy()
        self.proxy_model.setSourceModel(self.model)
        self.table_view.setModel(self.proxy_model)
        self.table_view.doubleClicked.connect(self.on_table_double_clicked)
        self.table_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self.open_context_menu)
        table_layout.addWidget(self.table_view)

        splitter.addWidget(control_widget);
        splitter.addWidget(table_widget);
        splitter.setStretchFactor(1, 1)
        layout.addWidget(splitter)

    def _create_hline(self):
        line = QFrame();
        line.setFrameShape(QFrame.Shape.HLine);
        line.setStyleSheet("color: #222;")
        return line

    def toggle_filters(self):
        self.is_filters_expanded = not self.is_filters_expanded
        self.filter_area.setVisible(self.is_filters_expanded)
        self.btn_toggle_filters.setText("▼" if self.is_filters_expanded else "▶")

    def open_filter_setting(self):
        dlg = FilterSelectionDialog(NUMERIC_FILTER_CONFIG, self.active_filter_keys, self)
        if dlg.exec():
            self.active_filter_keys = dlg.get_selected_keys()
            self.rebuild_filter_ui()
            self.apply_filters_real()

    def rebuild_filter_ui(self):
        self.clear_layout(self.filter_layout)
        self.dynamic_filters.clear()
        for cfg in NUMERIC_FILTER_CONFIG:
            if cfg['key'] in self.active_filter_keys:
                widget = RangeFilterWidget(cfg)
                widget.value_changed.connect(self.apply_filters_debounce)
                self.filter_layout.addWidget(widget)
                self.dynamic_filters.append(widget)

    def load_stock_list(self):
        try:
            path = Path(__file__).resolve().parent.parent / "data" / "stock_list.csv"
            if path.exists(): self.stock_list_df = pd.read_csv(path, dtype=str).set_index('stock_id')
        except:
            pass

    def load_data(self):
        self.btn_reload.setEnabled(False);
        self.lbl_status.setText("⏳")
        self.loader_thread = DataLoaderThread()
        self.loader_thread.data_loaded.connect(self.on_data_loaded)
        self.loader_thread.error_occurred.connect(self.on_load_error)
        self.loader_thread.start()

    def on_data_loaded(self, df):
        self.full_df = df;
        self.update_industry_combo();
        self._update_tag_checkboxes();
        self.apply_filters_real()
        self.lbl_status.setText(f"✅ {len(df)} 檔");
        self.btn_reload.setEnabled(True)

    def on_load_error(self, msg):
        QMessageBox.critical(self, "錯誤", msg);
        self.btn_reload.setEnabled(True)

    def update_industry_combo(self):
        if 'industry' in self.full_df.columns:
            industries = ["全部"] + sorted(self.full_df['industry'].dropna().unique().tolist())
            curr = self.combo_industry.currentText();
            self.combo_industry.blockSignals(True);
            self.combo_industry.clear()
            self.combo_industry.addItems(industries)
            if curr in industries: self.combo_industry.setCurrentText(curr)
            self.combo_industry.blockSignals(False)

    def clear_layout(self, layout):
        if layout is None: return
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
            else:
                self.clear_layout(item.layout())

    def _update_tag_checkboxes(self):
        self.clear_layout(self.tag_layout)
        self.chk_tags.clear()
        if '強勢特徵' not in self.full_df.columns: return

        self.tag_layout.setSpacing(2)
        self.tag_layout.setContentsMargins(0, 0, 0, 0)

        all_tags = set()
        for tags in self.full_df['強勢特徵'].dropna():
            for t in str(tags).split(','):
                if t.strip(): all_tags.add(t.strip())

        used_tags = set()
        for cat, tag_list in TAG_CATEGORIES.items():
            avail = [t for t in tag_list if t in all_tags]
            if not avail: continue

            lbl = QLabel(cat)
            lbl.setProperty("class", "category-label")  # CSS
            self.tag_layout.addWidget(lbl)

            grid = QGridLayout()
            grid.setVerticalSpacing(2)
            grid.setHorizontalSpacing(10)
            grid.setContentsMargins(0, 2, 0, 5)

            row, col = 0, 0
            for tag in avail:
                chk = QCheckBox(tag)
                chk.setStyleSheet("color: #DDD;")
                chk.setCursor(Qt.CursorShape.PointingHandCursor)
                chk.setToolTip(TAG_TOOLTIPS.get(tag, "無說明"))
                chk.stateChanged.connect(self.apply_filters_debounce)
                self.chk_tags[tag] = chk
                grid.addWidget(chk, row, col)
                used_tags.add(tag)
                col += 1
                if col > 1: col = 0; row += 1
            self.tag_layout.addLayout(grid)

        others = sorted(list(all_tags - used_tags))
        if others:
            lbl = QLabel("📋 其他")
            lbl.setProperty("class", "category-label")
            self.tag_layout.addWidget(lbl)

            grid = QGridLayout()
            grid.setVerticalSpacing(2)
            row, col = 0, 0
            for tag in others:
                chk = QCheckBox(tag)
                chk.setStyleSheet("color: #DDD;")
                chk.setToolTip(TAG_TOOLTIPS.get(tag, "無說明"))
                chk.stateChanged.connect(self.apply_filters_debounce)
                self.chk_tags[tag] = chk
                grid.addWidget(chk, row, col);
                col += 1
                if col > 1: col = 0; row += 1
            self.tag_layout.addLayout(grid)

        self.tag_layout.addStretch()

    def open_column_selector(self):
        dlg = ColumnSelectorDialog(COLUMN_CONFIG, self)
        if dlg.exec():
            new_selection = dlg.get_selection()
            for k, v in new_selection.items(): COLUMN_CONFIG[k]['show'] = v
            self.apply_filters_real()

    def reset_filters(self):
        self.combo_industry.setCurrentIndex(0)
        for w in self.dynamic_filters: w.reset()
        for chk in self.chk_tags.values(): chk.setChecked(False)
        self.rb_and.setChecked(True)
        self.proxy_model.sort(-1)
        self.apply_filters_real()

    def apply_filters_debounce(self):
        self.debounce_timer.start()

    def apply_filters_real(self):
        if self.full_df.empty: return
        df = self.full_df.copy()
        ind = self.combo_industry.currentText()
        if ind != "全部": df = df[df['industry'] == ind]

        is_dirty = False
        for w in self.dynamic_filters:
            if w.is_modified(): is_dirty = True
            key = w.key
            if key not in df.columns: continue
            min_val, max_val = w.spin_min.value(), w.spin_max.value()
            default_min, default_max = w.config['min'], w.config['max']
            if min_val != default_min: df = df[df[key] >= min_val]
            if max_val != default_max: df = df[df[key] <= max_val]

        if is_dirty:
            self.btn_reset.setStyleSheet("color: #FF5555; font-weight: bold; border: 1px solid #FF5555;")
        else:
            self.btn_reset.setStyleSheet("color: #666; font-weight: bold;")

        selected_tags = [t for t, chk in self.chk_tags.items() if chk.isChecked()]
        if selected_tags and '強勢特徵' in df.columns:
            df['強勢特徵'] = df['強勢特徵'].fillna("")
            if self.rb_and.isChecked():
                for tag in selected_tags: df = df[df['強勢特徵'].str.contains(tag, regex=False)]
            else:
                pattern = "|".join([str(t) for t in selected_tags])
                df = df[df['強勢特徵'].str.contains(pattern, regex=True)]

        visible_cols = [k for k, v in COLUMN_CONFIG.items() if v['show'] and k in df.columns]
        self.display_df = df[visible_cols].copy()

        if '漲幅20d' in self.display_df.columns and self.proxy_model.sortColumn() == -1:
            self.display_df = self.display_df.sort_values('漲幅20d', ascending=False)

        self.model.update_data(self.display_df, visible_cols)
        self.proxy_model.invalidate()
        self.lbl_status.setText(f"篩選結果: {len(self.display_df)} 檔")

        header = self.table_view.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        for i, col in enumerate(visible_cols):
            if col == '強勢特徵':
                header.resizeSection(i, 220)
            elif col == 'name':
                header.resizeSection(i, 110)
            else:
                header.resizeSection(i, 90)

    def on_table_double_clicked(self, index):
        src_idx = self.proxy_model.mapToSource(index)
        row = src_idx.row()
        sid = str(self.display_df.iloc[row]['sid'])
        market = "TW"
        if not self.stock_list_df.empty and sid in self.stock_list_df.index:
            m_code = str(self.stock_list_df.loc[sid, 'market']).strip().upper()
            if m_code in ['TWO', 'OTC', '上櫃']: market = "TWO"
        self.stock_clicked_signal.emit(f"{sid}_{market}")

    def open_context_menu(self, pos):
        menu = QMenu()
        add_menu = QMenu("➕ 加入自選群組", self)
        for g in ["我的持股", "觀察名單", "高股息"]:
            action = QAction(g, self)
            action.triggered.connect(lambda _, group=g: self.add_to_watchlist(group))
            add_menu.addAction(action)
        menu.addMenu(add_menu)
        menu.exec(QCursor.pos())

    def add_to_watchlist(self, group_name):
        rows = self.table_view.selectionModel().selectedRows()
        if not rows: return
        count = 0
        for idx in rows:
            src_idx = self.proxy_model.mapToSource(idx)
            sid = str(self.display_df.iloc[src_idx.row()]['sid'])
            self.request_add_watchlist.emit(sid, group_name)
            count += 1
        QMessageBox.information(self, "完成", f"已將 {count} 檔加入「{group_name}」。")