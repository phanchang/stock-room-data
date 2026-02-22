import sys
import json
import pandas as pd
from pathlib import Path
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QTableView, QHeaderView, QGroupBox, QComboBox,
                             QDoubleSpinBox, QPushButton, QCheckBox,
                             QAbstractItemView, QMenu, QMessageBox, QSplitter,
                             QScrollArea, QFrame, QDialog, QGridLayout,
                             QDialogButtonBox, QRadioButton, QButtonGroup, QToolButton,
                             QSizePolicy, QInputDialog, QLineEdit, QListWidget, QListWidgetItem)
from PyQt6.QtCore import Qt, pyqtSignal, QAbstractTableModel, QSortFilterProxyModel, QThread, QTimer, QSize
from PyQt6.QtGui import QColor, QAction, QCursor, QFont

# ==========================================
# 1. 全欄位設定
# ==========================================
FULL_COLUMN_SPECS = {
    'sid': {'name': '代號', 'show': True, 'tip': '股票代號', 'type': 'str'},
    'name': {'name': '名稱', 'show': True, 'tip': '股票名稱', 'type': 'str'},
    'rev_ym': {'name': '營收月', 'show': True, 'tip': '資料所屬月份 (如 11301 代表 2024年1月)', 'type': 'str'},    'industry': {'name': '產業', 'show': True, 'tip': '所屬產業類別', 'type': 'str'},
    '現價': {'name': '股價', 'show': True, 'tip': '最新收盤價', 'type': 'num'},
    '漲幅5d': {'name': '5日%', 'show': False, 'tip': '近5日漲跌幅', 'type': 'num'},
    '漲幅20d': {'name': '月漲幅%', 'show': True, 'tip': '近20日漲跌幅', 'type': 'num'},
    '漲幅60d': {'name': '季漲幅%', 'show': False, 'tip': '近60日漲跌幅', 'type': 'num'},
    'RS強度': {'name': 'RS強度', 'show': True, 'tip': '相對強度 (1-99)', 'type': 'num'},
    'bb_width': {'name': '布林寬%', 'show': True, 'tip': '布林通道寬度 (愈小愈壓縮)', 'type': 'num'},
    '量比': {'name': '量比', 'show': True, 'tip': '今日量 / 5日均量', 'type': 'num'},
    't_net_today': {'name': '投今日', 'show': False, 'tip': '投信今日買賣超', 'type': 'num'},
    't_sum_5d': {'name': '投5日', 'show': True, 'tip': '投信5日累計買賣超', 'type': 'num'},
    't_sum_10d': {'name': '投10日', 'show': False, 'tip': '投信10日累計買賣超', 'type': 'num'},
    't_sum_20d': {'name': '投20日', 'show': False, 'tip': '投信20日累計買賣超', 'type': 'num'},
    't_streak': {'name': '投連買', 'show': True, 'tip': '投信連續買超天數', 'type': 'num'},
    'f_net_today': {'name': '外今日', 'show': False, 'tip': '外資今日買賣超', 'type': 'num'},
    'f_sum_5d': {'name': '外5日', 'show': True, 'tip': '外資5日累計買賣超', 'type': 'num'},
    'f_sum_10d': {'name': '外10日', 'show': False, 'tip': '外資10日累計買賣超', 'type': 'num'},
    'f_sum_20d': {'name': '外20日', 'show': False, 'tip': '外資20日累計買賣超', 'type': 'num'},
    'f_streak': {'name': '外連買', 'show': True, 'tip': '外資連續買超天數', 'type': 'num'},
    'm_net_today': {'name': '資今日', 'show': False, 'tip': '融資今日增減', 'type': 'num'},
    'm_sum_5d': {'name': '資5日', 'show': True, 'tip': '融資5日累計', 'type': 'num'},
    'm_sum_10d': {'name': '資10日', 'show': False, 'tip': '融資10日累計', 'type': 'num'},
    'm_sum_20d': {'name': '資20日', 'show': False, 'tip': '融資20日累計', 'type': 'num'},
    'rev_yoy': {'name': '月YoY%', 'show': True, 'tip': '最新月營收年增率', 'type': 'num'},
    'rev_cum_yoy': {'name': '累營YoY%', 'show': True, 'tip': '當年累計營收年增率', 'type': 'num'},
    'eps_q': {'name': 'EPS(累)', 'show': True, 'tip': '累計季 EPS', 'type': 'num'},
    'eps_date': {'name': 'EPS季別', 'show': True, 'tip': 'EPS數據所屬年度與季別', 'type': 'str'}, # 🔥 新增這行
    'pe': {'name': 'PE', 'show': True, 'tip': '本益比', 'type': 'num'},
    'pbr': {'name': 'PB', 'show': False, 'tip': '股價淨值比', 'type': 'num'},
    'yield': {'name': '殖利率%', 'show': True, 'tip': '現金殖利率', 'type': 'num'},
    'is_tu_yang': {'name': '土洋對作', 'show': False, 'tip': '1=符合土洋對作訊號', 'type': 'num'},
    '強勢特徵': {'name': '強勢特徵', 'show': True, 'tip': '策略觸發訊號標籤', 'type': 'str'},
    'str_30w_week_offset': {'name': '訊號週數', 'show': True, 'tip': '0=本週, 1=上週...', 'type': 'num'},
    'str_st_week_offset': {'name': 'ST買訊(週)', 'show': True, 'tip': '距離最近一次週線SuperTrend買訊週數 (0=本週)', 'type': 'num'}
}

# ==========================================
# 2. 全數值過濾設定
# ==========================================
FULL_FILTER_SPECS = [
    {'key': '現價', 'label': '股價', 'min': 0, 'max': 5000, 'step': 10, 'suffix': ''},
    {'key': '漲幅5d', 'label': '5日漲幅(%)', 'min': -50, 'max': 100, 'step': 1.0, 'suffix': '%'},
    {'key': '漲幅20d', 'label': '月漲幅(%)', 'min': -50, 'max': 200, 'step': 1.0, 'suffix': '%'},
    {'key': '漲幅60d', 'label': '季漲幅(%)', 'min': -50, 'max': 500, 'step': 5.0, 'suffix': '%'},
    {'key': 'RS強度', 'label': 'RS強度', 'min': 0, 'max': 99, 'step': 1.0, 'suffix': ''},
    {'key': 'bb_width', 'label': '布林寬(%)', 'min': 0, 'max': 50, 'step': 0.5, 'suffix': '%'},
    {'key': '量比', 'label': '量比(倍)', 'min': 0, 'max': 50, 'step': 0.5, 'suffix': ''},
    {'key': 't_streak', 'label': '投信連買(日)', 'min': 0, 'max': 30, 'step': 1, 'suffix': ''},
    {'key': 't_net_today', 'label': '投信今日(張)', 'min': -20000, 'max': 20000, 'step': 100, 'suffix': ''},
    {'key': 't_sum_5d', 'label': '投信5日(張)', 'min': -50000, 'max': 50000, 'step': 100, 'suffix': ''},
    {'key': 't_sum_10d', 'label': '投信10日(張)', 'min': -50000, 'max': 50000, 'step': 100, 'suffix': ''},
    {'key': 't_sum_20d', 'label': '投信20日(張)', 'min': -100000, 'max': 100000, 'step': 500, 'suffix': ''},
    {'key': 'f_streak', 'label': '外資連買(日)', 'min': 0, 'max': 30, 'step': 1, 'suffix': ''},
    {'key': 'f_net_today', 'label': '外資今日(張)', 'min': -50000, 'max': 50000, 'step': 500, 'suffix': ''},
    {'key': 'f_sum_5d', 'label': '外資5日(張)', 'min': -100000, 'max': 100000, 'step': 500, 'suffix': ''},
    {'key': 'f_sum_10d', 'label': '外資10日(張)', 'min': -100000, 'max': 100000, 'step': 500, 'suffix': ''},
    {'key': 'f_sum_20d', 'label': '外資20日(張)', 'min': -200000, 'max': 200000, 'step': 1000, 'suffix': ''},
    {'key': 'm_net_today', 'label': '融資今日(張)', 'min': -20000, 'max': 20000, 'step': 100, 'suffix': ''},
    {'key': 'm_sum_5d', 'label': '融資5日(張)', 'min': -50000, 'max': 50000, 'step': 100, 'suffix': ''},
    {'key': 'm_sum_10d', 'label': '融資10日(張)', 'min': -50000, 'max': 50000, 'step': 100, 'suffix': ''},
    {'key': 'rev_yoy', 'label': '月營收年增(%)', 'min': -100, 'max': 1000, 'step': 5.0, 'suffix': '%'},
    {'key': 'rev_cum_yoy', 'label': '累營年增(%)', 'min': -100, 'max': 1000, 'step': 5.0, 'suffix': '%'},
    {'key': 'eps_q', 'label': 'EPS(元)', 'min': -10, 'max': 100, 'step': 0.5, 'suffix': ''},
    {'key': 'pe', 'label': '本益比', 'min': 0, 'max': 200, 'step': 1.0, 'suffix': ''},
    {'key': 'pbr', 'label': '股價淨值比', 'min': 0, 'max': 20, 'step': 0.1, 'suffix': ''},
    {'key': 'yield', 'label': '殖利率(%)', 'min': 0, 'max': 20, 'step': 0.5, 'suffix': '%'},
    {'key': 'str_30w_week_offset', 'label': '訊號週數(前)', 'min': -1, 'max': 52, 'step': 1, 'suffix': '週'},
    {'key': 'str_st_week_offset', 'label': 'ST買訊(前)', 'min': -1, 'max': 26, 'step': 1, 'suffix': '週'}
]

DEFAULT_ACTIVE_FILTERS = ['str_30w_week_offset', '量比', '漲幅20d']

# 🔥 修正重點：新增 30W 選項
TAG_CATEGORIES = {
    "🔥 趨勢型態": ["ST轉多", "30W黏貼", "30W甩轎", "主力掃單(ILSS)", "土洋對作", "超強勢", "突破30週", "創季高", "創月高", "強勢多頭", "波段黑馬", "假跌破"],
    "📉 整理型態": ["極度壓縮", "波動壓縮", "盤整5日", "盤整10日", "盤整20日", "盤整60日", "Vix反轉"],
    "💰 籌碼支撐": ["投信認養", "散戶退場", "回測季線", "回測年線"]
}

TAG_TOOLTIPS = {
    'ST轉多': '近 4 週內週線 SuperTrend 指標由空翻多，觸發波段買進訊號',
    '30W黏貼': 'MA30 走平且股價在均線附近 ±12% 震盪',
    '30W甩轎': 'MA30 向上，股價回測跌破均線並在 10 週內站回',
    '主力掃單(ILSS)': '[嚴格] MA200上 + 假跌破掃單 + 爆量站回 + 營收增 + 融資減',
    '假跌破': '舊版策略：昨破月線、今站回 (純技術面)',
    '極度壓縮': '布林寬度 < 5%，極致籌碼沉澱',
    '土洋對作': '投信賣、外資買 (籌碼換手)',
    '超強勢': 'RS 強度 > 90，市場前 10% 強勢股',
}

GLOBAL_STYLE = """
    QWidget { font-family: "Microsoft JhengHei", "Segoe UI"; font-size: 16px; background-color: #000; color: #EEE; }
    QDialog, QMessageBox, QInputDialog { background-color: #111; border: 1px solid #333; color: #EEE; }
    QPushButton, QToolButton { 
        background-color: #222; color: #CCC; border: 1px solid #444; 
        padding: 6px; border-radius: 4px; font-weight: bold; font-size: 14px;
    }
    QPushButton:hover, QToolButton:hover { background-color: #333; border-color: #00E5FF; color: #FFF; }
    QDoubleSpinBox { background: #000; color: #00E5FF; border: 1px solid #444; padding: 4px; font-weight: bold; font-size: 16px; }
    QComboBox { background: #000; color: #FFF; border: 1px solid #444; padding: 6px; }
    QComboBox::drop-down { border: none; }
    QComboBox QAbstractItemView { background: #111; color: #FFF; selection-background-color: #00E5FF; selection-color: #000; }
    QCheckBox { background: transparent; color: #DDD; }
    QCheckBox::indicator:checked { background-color: #00E5FF; border: 1px solid #00E5FF; }
    QListWidget { background-color: #111; border: 1px solid #333; color: #FFF; }
    QListWidget::item { padding: 5px; }
    QListWidget::item:selected { background-color: #004466; color: #FFF; }
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
                self.error_occurred.emit("無數據")
                return
            if 'sid' in df.columns: df['sid'] = df['sid'].astype(str).str.strip()
            for col in df.columns:
                if col in FULL_COLUMN_SPECS and FULL_COLUMN_SPECS[col]['type'] == 'num':
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            self.data_loaded.emit(df)
        except Exception as e:
            self.error_occurred.emit(str(e))


class FilterSelectionDialog(QDialog):
    def __init__(self, all_filters, active_keys, parent=None):
        super().__init__(parent)
        self.setWindowTitle("⚙️ 濾網顯示設定")
        self.all_filters = all_filters
        self.checkboxes = {}
        self.active_keys = active_keys
        self.setStyleSheet(GLOBAL_STYLE)
        self.resize(500, 600)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        lbl = QLabel("請勾選要顯示在主畫面的濾網：")
        lbl.setStyleSheet("color: #AAA; margin-bottom: 10px;")
        layout.addWidget(lbl)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        content = QWidget()
        grid = QGridLayout(content)
        row, col = 0, 0
        for cfg in self.all_filters:
            key = cfg['key']
            chk = QCheckBox(cfg['label'])
            chk.setChecked(key in self.active_keys)
            self.checkboxes[key] = chk
            grid.addWidget(chk, row, col)
            col += 1
            if col > 2: col = 0; row += 1
        scroll.setWidget(content)
        layout.addWidget(scroll)
        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def get_selected_keys(self):
        return [k for k, chk in self.checkboxes.items() if chk.isChecked()]


class ColumnSelectorDialog(QDialog):
    def __init__(self, config, current_order, parent=None):
        super().__init__(parent)
        self.setWindowTitle("👁️ 欄位顯示與排序")
        self.config = config
        self.current_order = current_order
        self.setStyleSheet(GLOBAL_STYLE)
        self.resize(350, 600)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        lbl = QLabel("💡 拖曳可調整順序，勾選決定是否顯示")
        lbl.setStyleSheet("color: #00E5FF; font-weight: bold;")
        layout.addWidget(lbl)
        self.list_widget = QListWidget()
        self.list_widget.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)
        processed_keys = set()
        for key in self.current_order:
            if key in self.config:
                self._add_item(key)
                processed_keys.add(key)
        for key in self.config.keys():
            if key not in processed_keys:
                self._add_item(key)
        layout.addWidget(self.list_widget)
        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _add_item(self, key):
        info = self.config[key]
        item = QListWidgetItem(info['name'])
        item.setData(Qt.ItemDataRole.UserRole, key)
        item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
        item.setCheckState(Qt.CheckState.Checked if info['show'] else Qt.CheckState.Unchecked)
        self.list_widget.addItem(item)

    def get_result(self):
        new_order = []
        new_show = {}
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            key = item.data(Qt.ItemDataRole.UserRole)
            is_checked = (item.checkState() == Qt.CheckState.Checked)
            new_order.append(key)
            new_show[key] = is_checked
        return new_order, new_show


class RangeFilterWidget(QWidget):
    value_changed = pyqtSignal()

    def __init__(self, config):
        super().__init__()
        self.key = config['key']
        self.config = config
        self.setStyleSheet("background: transparent;")
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self.lbl_name = QLabel(config['label'])
        self.lbl_name.setStyleSheet("color: #DDD; font-size: 14px; border:none;")
        self.lbl_name.setFixedWidth(100)
        layout.addWidget(self.lbl_name)
        self.spin_min = QDoubleSpinBox()
        self.setup_spin(self.spin_min, config['min'], config['suffix'])
        self.spin_min.setFixedWidth(80)
        layout.addWidget(self.spin_min)
        lbl_tilde = QLabel("~")
        lbl_tilde.setStyleSheet("color:#555; border:none;")
        lbl_tilde.setFixedWidth(10)
        lbl_tilde.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(lbl_tilde)
        self.spin_max = QDoubleSpinBox()
        self.setup_spin(self.spin_max, config['max'], config['suffix'])
        self.spin_max.setFixedWidth(80)
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
        self.spin_min.blockSignals(True)
        self.spin_max.blockSignals(True)
        self.spin_min.setValue(self.config['min'])
        self.spin_max.setValue(self.config['max'])
        self.spin_min.blockSignals(False)
        self.spin_max.blockSignals(False)
        self.emit_change()


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

        # 取代 StrategyTableModel 內的 data 函式
    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
            if not index.isValid(): return None
            col_key = self.visible_cols[index.column()]
            value = self._df.iloc[index.row()][col_key]
            if role == Qt.ItemDataRole.UserRole: return value
            if role == Qt.ItemDataRole.DisplayRole:
                if isinstance(value, (int, float)):
                    if col_key in ['RS強度', 'pe', 'pbr', '量比', 'eps_q']: return f"{value:.1f}"
                    if 'rev_now' in col_key: return f"{value:,.0f}"
                    if '漲幅' in col_key or 'yield' in col_key or 'width' in col_key or 'yoy' in col_key: return f"{value:.2f}%"
                    if 'sum' in col_key or 'net' in col_key: return f"{value:,.0f}"
                    if 'streak' in col_key or 'offset' in col_key: return f"{int(value)}"
                    return f"{value:,.2f}"
                return str(value)
            if role == Qt.ItemDataRole.ToolTipRole:
                if col_key == '強勢特徵' and isinstance(value, str):
                    tags = [t.strip() for t in value.split(',')]
                    tips = [f"• {t}: {TAG_TOOLTIPS.get(t, '')}" for t in tags]
                    return "\n".join(tips)
                return FULL_COLUMN_SPECS.get(col_key, {}).get('tip', '')
            if role == Qt.ItemDataRole.ForegroundRole:
                if isinstance(value, (int, float)):
                    if '漲幅' in col_key or 'sum' in col_key or '買賣超' in col_key or 'yoy' in col_key or 'eps' in col_key or 'streak' in col_key:
                        if value > 0: return QColor("#FF4444")
                        if value < 0: return QColor("#00CC00")
                if col_key == '強勢特徵' and value:
                    if 'ST剛轉多' in str(value): return QColor("#FF3333")
                    if '30W' in str(value): return QColor("#00E5FF")  # 亮藍色
                    if 'ILSS' in str(value): return QColor("#FF00FF")  # 紫紅色
                    if '土洋' in str(value): return QColor("#FFFF00")  # 亮黃色
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
            config = FULL_COLUMN_SPECS.get(col_key, {})
            if role == Qt.ItemDataRole.DisplayRole: return config.get('name', col_key)
            if role == Qt.ItemDataRole.ToolTipRole: return config.get('tip', '')
        return None


class NumericSortProxy(QSortFilterProxyModel):
    def lessThan(self, left, right):
        l_val = self.sourceModel().data(left, Qt.ItemDataRole.UserRole)
        r_val = self.sourceModel().data(right, Qt.ItemDataRole.UserRole)
        if l_val is None: l_val = -999999
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
        self.watchlist_data = {}

        self.settings_dir = Path(__file__).resolve().parent.parent / "data" / "settings"
        self.settings_dir.mkdir(parents=True, exist_ok=True)
        self.col_order_file = self.settings_dir / "column_order.json"
        self.col_show_file = self.settings_dir / "column_show.json"

        self.column_order = []
        self.load_column_settings()

        self.dynamic_filters = []
        self.active_filter_keys = DEFAULT_ACTIVE_FILTERS.copy()

        self.is_filters_expanded = True
        self.debounce_timer = QTimer()
        self.debounce_timer.setSingleShot(True)
        self.debounce_timer.interval = 300
        self.debounce_timer.timeout.connect(self.apply_filters_real)

        self.init_ui()
        self.load_watchlist_json()
        QTimer.singleShot(100, self.load_data)

    def init_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # === 左側面板 ===
        control_widget = QWidget()
        control_widget.setFixedWidth(420)
        control_widget.setStyleSheet("background-color: #050505; border-right: 1px solid #222;")
        ctrl_layout = QVBoxLayout(control_widget)
        ctrl_layout.setSpacing(10)
        ctrl_layout.setContentsMargins(10, 10, 10, 10)

        header_widget = QWidget()
        header_widget.setFixedHeight(45)
        header_widget.setStyleSheet("background: #111; border-bottom: 1px solid #333;")
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(5, 5, 5, 5)

        self.txt_search = QLineEdit()
        self.txt_search.setPlaceholderText("🔍 搜尋...")
        self.txt_search.setFixedWidth(120)
        self.txt_search.setStyleSheet("background: #222; color: #FFF; border: 1px solid #444; font-size: 14px;")
        self.txt_search.returnPressed.connect(self.on_search_triggered)

        title = QLabel("戰略選股")
        title.setStyleSheet("font-size: 16px; font-weight: bold; color: #00E5FF; border: none; background: transparent;")

        self.btn_reload = QToolButton()
        self.btn_reload.setText("🔄")
        self.btn_reload.setToolTip("重新整理 (資料更新後請點此)")
        self.btn_reload.clicked.connect(self.load_data)

        self.btn_cols = QToolButton()
        self.btn_cols.setText("👁️")
        self.btn_cols.clicked.connect(self.open_column_selector)

        header_layout.addWidget(self.txt_search)
        header_layout.addWidget(title)
        header_layout.addStretch()
        header_layout.addWidget(self.btn_reload)
        header_layout.addWidget(self.btn_cols)
        ctrl_layout.addWidget(header_widget)

        lbl_ind = QLabel("📂 類別與自選")
        lbl_ind.setProperty("class", "category-label")
        ctrl_layout.addWidget(lbl_ind)
        self.combo_industry = QComboBox()
        self.combo_industry.addItem("全部")
        self.combo_industry.currentIndexChanged.connect(self.apply_filters_debounce)
        ctrl_layout.addWidget(self.combo_industry)

        filter_header_box = QHBoxLayout()
        lbl_val = QLabel("📊 數值過濾")
        lbl_val.setProperty("class", "category-label")

        self.btn_filter_setting = QToolButton()
        self.btn_filter_setting.setText("⚙️")
        self.btn_filter_setting.clicked.connect(self.open_filter_setting)

        self.btn_toggle_filters = QToolButton()
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
        self.btn_reset.clicked.connect(self.reset_filters)
        self.filter_area = QWidget()
        filter_area_layout = QVBoxLayout(self.filter_area)
        filter_area_layout.setContentsMargins(0, 0, 0, 0)
        filter_area_layout.addWidget(self.btn_reset, alignment=Qt.AlignmentFlag.AlignRight)
        filter_area_layout.addWidget(self.filter_container_widget)
        ctrl_layout.addWidget(self.filter_area)
        self.rebuild_filter_ui()

        lbl_tag = QLabel("🔥 強勢特徵")
        lbl_tag.setProperty("class", "category-label")
        ctrl_layout.addWidget(lbl_tag)
        logic_layout = QHBoxLayout()
        self.logic_group = QButtonGroup(self)
        self.rb_and = QRadioButton("交集 (AND)")
        self.rb_or = QRadioButton("聯集 (OR)")
        self.rb_and.setStyleSheet("color: #AAA; border:none; background: transparent;")
        self.rb_or.setStyleSheet("color: #AAA; border:none; background: transparent;")
        self.rb_and.setChecked(True)
        self.logic_group.addButton(self.rb_and)
        self.logic_group.addButton(self.rb_or)
        self.rb_and.toggled.connect(self.apply_filters_debounce)
        self.rb_or.toggled.connect(self.apply_filters_debounce)
        logic_layout.addWidget(self.rb_and)
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

        self.lbl_status = QLabel("就緒")
        self.lbl_status.setStyleSheet(
            "color: #666; font-size: 14px; margin-top: 5px; border:none; background:transparent;")
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

        self.table_view.horizontalHeader().setSectionsMovable(True)
        self.table_view.horizontalHeader().sectionMoved.connect(self.on_header_moved)

        self.table_view.setSortingEnabled(True)
        self.table_view.setStyleSheet("""
            QTableView { background-color: #000000; color: #E0E0E0; gridline-color: #222; font-size: 16px; font-family: 'Consolas', 'Microsoft JhengHei'; border: none; }
            QHeaderView::section { background-color: #111; color: #AAA; padding: 6px; border-right: 1px solid #222; border-bottom: 2px solid #333; font-weight: bold; font-size: 14px; }
            QTableView::item:selected { background-color: #004466; color: #FFF; }
        """)
        self.model = StrategyTableModel()
        self.proxy_model = NumericSortProxy()
        self.proxy_model.setSourceModel(self.model)
        self.table_view.setModel(self.proxy_model)
        self.table_view.doubleClicked.connect(self.on_table_double_clicked)
        self.table_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self.open_context_menu)
        self.table_view.selectionModel().currentChanged.connect(self.on_current_row_changed)
        table_layout.addWidget(self.table_view)

        splitter.addWidget(control_widget)
        splitter.addWidget(table_widget)
        splitter.setStretchFactor(1, 1)
        layout.addWidget(splitter)

    def load_column_settings(self):
        if self.col_show_file.exists():
            try:
                with open(self.col_show_file, 'r') as f:
                    saved_show = json.load(f)
                    for k, v in saved_show.items():
                        if k in FULL_COLUMN_SPECS:
                            FULL_COLUMN_SPECS[k]['show'] = v
            except:
                pass

        default_order = list(FULL_COLUMN_SPECS.keys())
        if self.col_order_file.exists():
            try:
                with open(self.col_order_file, 'r') as f:
                    saved_order = json.load(f)
                    valid_saved = [k for k in saved_order if k in FULL_COLUMN_SPECS]
                    missing = [k for k in default_order if k not in valid_saved]
                    self.column_order = valid_saved + missing
            except:
                self.column_order = default_order
        else:
            self.column_order = default_order

    def save_column_settings(self):
        try:
            with open(self.col_order_file, 'w') as f:
                json.dump(self.column_order, f)
            show_state = {k: v['show'] for k, v in FULL_COLUMN_SPECS.items()}
            with open(self.col_show_file, 'w') as f:
                json.dump(show_state, f)
        except Exception as e:
            print(f"Error saving settings: {e}")

    def on_header_moved(self, logicalIndex, oldVisualIndex, newVisualIndex):
        QTimer.singleShot(100, self._sync_order_from_visual)

    def _sync_order_from_visual(self):
        header = self.table_view.horizontalHeader()
        visible_cols = self.model.visible_cols
        new_visual_keys = []
        for i in range(header.count()):
            logical_idx = header.logicalIndex(i)
            if logical_idx < len(visible_cols):
                new_visual_keys.append(visible_cols[logical_idx])
        current_hidden = [k for k in self.column_order if k not in new_visual_keys]
        self.column_order = new_visual_keys + current_hidden
        self.save_column_settings()

    def on_current_row_changed(self, current, previous):
        if current.isValid():
            self.table_view.scrollTo(current, QAbstractItemView.ScrollHint.EnsureVisible)

    def toggle_filters(self):
        self.is_filters_expanded = not self.is_filters_expanded
        self.filter_area.setVisible(self.is_filters_expanded)
        self.btn_toggle_filters.setText("▼" if self.is_filters_expanded else "▶")

    def open_filter_setting(self):
        dlg = FilterSelectionDialog(FULL_FILTER_SPECS, self.active_filter_keys, self)
        if dlg.exec():
            self.active_filter_keys = dlg.get_selected_keys()
            self.rebuild_filter_ui()
            self.apply_filters_real()

    def open_column_selector(self):
        dlg = ColumnSelectorDialog(FULL_COLUMN_SPECS, self.column_order, self)
        if dlg.exec():
            new_order, new_show = dlg.get_result()
            self.column_order = new_order
            for k, show in new_show.items():
                FULL_COLUMN_SPECS[k]['show'] = show
            self.save_column_settings()
            self.apply_filters_real()

    def rebuild_filter_ui(self):
        self.clear_layout(self.filter_layout)
        self.dynamic_filters.clear()
        for cfg in FULL_FILTER_SPECS:
            if cfg['key'] in self.active_filter_keys:
                widget = RangeFilterWidget(cfg)
                widget.value_changed.connect(self.apply_filters_debounce)
                self.filter_layout.addWidget(widget)
                self.dynamic_filters.append(widget)

    def load_watchlist_json(self):
        try:
            path = Path(__file__).resolve().parent.parent / "data" / "watchlist.json"
            if path.exists():
                with open(path, 'r', encoding='utf-8') as f:
                    self.watchlist_data = json.load(f)
            else:
                self.watchlist_data = {}
        except Exception as e:
            print(f"Error loading watchlist json: {e}")
            self.watchlist_data = {}

    def load_data(self):
        self.load_watchlist_json()
        self.btn_reload.setEnabled(False)
        self.lbl_status.setText("⏳")
        self.loader_thread = DataLoaderThread()
        self.loader_thread.data_loaded.connect(self.on_data_loaded)
        self.loader_thread.error_occurred.connect(self.on_load_error)
        self.loader_thread.start()

    def on_data_loaded(self, df):
        self.full_df = df
        self.update_industry_combo()
        self._update_tag_checkboxes()
        self.apply_filters_real()
        self.lbl_status.setText(f"✅ {len(df)} 檔")
        self.btn_reload.setEnabled(True)

    def on_load_error(self, msg):
        QMessageBox.critical(self, "錯誤", msg)
        self.btn_reload.setEnabled(True)

    def update_industry_combo(self):
        items = ["全部"]
        if self.watchlist_data:
            for group_name in self.watchlist_data.keys():
                items.append(f"[自選] {group_name}")

        if 'industry' in self.full_df.columns:
            industries = sorted(self.full_df['industry'].dropna().unique().tolist())
            items.extend(industries)

        curr = self.combo_industry.currentText()
        self.combo_industry.blockSignals(True)
        self.combo_industry.clear()
        self.combo_industry.addItems(items)
        if curr in items: self.combo_industry.setCurrentText(curr)
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
        self.tag_layout.setSpacing(2)
        self.tag_layout.setContentsMargins(0, 0, 0, 0)

        for cat, tag_list in TAG_CATEGORIES.items():
            lbl = QLabel(cat)
            lbl.setProperty("class", "category-label")
            self.tag_layout.addWidget(lbl)
            grid = QGridLayout()
            grid.setVerticalSpacing(2)
            grid.setHorizontalSpacing(10)
            grid.setContentsMargins(0, 2, 0, 5)
            row, col = 0, 0
            for tag in tag_list:
                chk = QCheckBox(tag)
                chk.setStyleSheet("color: #DDD;")
                chk.setCursor(Qt.CursorShape.PointingHandCursor)
                chk.setToolTip(TAG_TOOLTIPS.get(tag, "無說明"))
                chk.stateChanged.connect(self.apply_filters_debounce)
                self.chk_tags[tag] = chk
                grid.addWidget(chk, row, col)
                col += 1
                if col > 1: col = 0; row += 1
            self.tag_layout.addLayout(grid)
        self.tag_layout.addStretch()

    def reset_filters(self):
        self.combo_industry.setCurrentIndex(0)
        self.txt_search.clear()
        for w in self.dynamic_filters: w.reset()
        for chk in self.chk_tags.values(): chk.setChecked(False)
        self.rb_and.setChecked(True)
        self.proxy_model.sort(-1)
        self.apply_filters_real()

    def apply_filters_debounce(self):
        self.debounce_timer.start()

    def on_search_triggered(self):
        self.apply_filters_real()

    def apply_filters_real(self):
        if self.full_df.empty: return
        df = self.full_df.copy()

        search_txt = self.txt_search.text().strip()
        if search_txt:
            mask = df['sid'].str.contains(search_txt) | df['name'].str.contains(search_txt)
            df = df[mask]

        ind = self.combo_industry.currentText()
        if ind.startswith("[自選] "):
            group_name = ind.replace("[自選] ", "")
            if group_name in self.watchlist_data:
                target_sids = self.watchlist_data[group_name]
                target_sids = [str(x).strip() for x in target_sids]
                df = df[df['sid'].isin(target_sids)]
        elif ind != "全部":
            df = df[df['industry'] == ind]

        is_dirty = False
        for w in self.dynamic_filters:
            if w.is_modified(): is_dirty = True
            key = w.key
            if key not in df.columns: continue
            min_val, max_val = w.spin_min.value(), w.spin_max.value()
            default_min, default_max = w.config['min'], w.config['max']
            if min_val != default_min: df = df[df[key] >= min_val]
            if max_val != default_max: df = df[df[key] <= max_val]

        is_tag_dirty = any(chk.isChecked() for chk in self.chk_tags.values())
        if is_dirty or is_tag_dirty or ind != "全部" or search_txt:
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

        visible_cols = []
        for key in self.column_order:
            if key in FULL_COLUMN_SPECS and FULL_COLUMN_SPECS[key]['show']:
                if key in df.columns:
                    visible_cols.append(key)

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
            elif 'sum' in col or 'net' in col:
                header.resizeSection(i, 100)
            else:
                header.resizeSection(i, 80)

    def on_table_double_clicked(self, index):
        src_idx = self.proxy_model.mapToSource(index)
        row = src_idx.row()
        sid = str(self.display_df.iloc[row]['sid'])
        market = "TW"
        base_cache_path = Path(__file__).resolve().parent.parent / "data" / "cache" / "tw"
        path_two = base_cache_path / f"{sid}_TWO.parquet"

        if path_two.exists():
            market = "TWO"
        else:
            path_tw = base_cache_path / f"{sid}_TW.parquet"
            if path_tw.exists():
                market = "TW"

        print(f"DEBUG: Strategy Double Click: {sid} -> {market}")
        self.stock_clicked_signal.emit(f"{sid}_{market}")

    def open_context_menu(self, pos):
        menu = QMenu()
        add_menu = QMenu("➕ 加入自選群組", self)
        for g in self.watchlist_data.keys():
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
        QMessageBox.information(self, "完成", f"已請求將 {count} 檔加入「{group_name}」。")