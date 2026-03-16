import sys
import json
import time
import pandas as pd
from pathlib import Path
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QTableView, QHeaderView, QGroupBox, QComboBox,
                             QDoubleSpinBox, QPushButton, QCheckBox,
                             QAbstractItemView, QMenu, QMessageBox, QSplitter,
                             QScrollArea, QFrame, QDialog, QGridLayout,
                             QDialogButtonBox, QRadioButton, QButtonGroup, QToolButton,
                             QSizePolicy, QInputDialog, QLineEdit, QListWidget, QListWidgetItem,
                             QCompleter)
from PyQt6.QtCore import Qt, pyqtSignal, QAbstractTableModel, QSortFilterProxyModel, QThread, QTimer, QSize
from PyQt6.QtGui import QColor, QAction, QCursor, QFont
from modules.stock_insight_dashboard import StockInsightDashboard

# ==========================================
# 1. 全欄位設定
# ==========================================
FULL_COLUMN_SPECS = {
    'insight': {'name': '詳', 'show': True, 'tip': '個股深度決策導覽', 'type': 'str'},
    'sid': {'name': '代號', 'show': True, 'tip': '股票代號', 'type': 'str'},
    'name': {'name': '名稱', 'show': True, 'tip': '股票名稱', 'type': 'str'},
    'rev_ym': {'name': '營收月', 'show': True, 'tip': '資料所屬月份', 'type': 'str'},
    'industry': {'name': '產業', 'show': True, 'tip': '所屬產業類別', 'type': 'str'},
    'sub_concepts': {'name': '族群', 'show': True, 'tip': '所屬概念股與次產業族群', 'type': 'str'},
    '現價': {'name': '股價', 'show': True, 'tip': '最新收盤價', 'type': 'num'},
    '漲幅1d': {'name': '今日%', 'show': True, 'tip': '今日漲跌幅', 'type': 'num'},
    '漲幅5d': {'name': '5日%', 'show': False, 'tip': '近5日漲跌幅', 'type': 'num'},
    '漲幅20d': {'name': '月漲幅%', 'show': True, 'tip': '近20日漲跌幅', 'type': 'num'},
    '漲幅60d': {'name': '季漲幅%', 'show': False, 'tip': '近60日漲跌幅', 'type': 'num'},
    'RS強度': {'name': 'RS強度', 'show': True, 'tip': '相對強度 (1-99)', 'type': 'num'},
    'bb_width': {'name': '布林寬%', 'show': True, 'tip': '布林通道寬度', 'type': 'num'},
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
    'fund_eps_cum': {'name': 'EPS(累)', 'show': True, 'tip': '最新年度累計 EPS', 'type': 'num'},
    'fund_eps_year': {'name': 'EPS年度', 'show': True, 'tip': '最新EPS所屬年度', 'type': 'str'},
    'pe': {'name': 'PE', 'show': True, 'tip': '本益比', 'type': 'num'},
    'pbr': {'name': 'PB', 'show': False, 'tip': '股價淨值比', 'type': 'num'},
    'yield': {'name': '殖利率%', 'show': True, 'tip': '現金殖利率', 'type': 'num'},
    'is_tu_yang': {'name': '土洋對作', 'show': False, 'tip': '1=符合土洋對作訊號', 'type': 'num'},
    '強勢特徵': {'name': '強勢特徵', 'show': True, 'tip': '策略觸發訊號標籤', 'type': 'str'},
    'str_30w_week_offset': {'name': '30W起漲週數(前)', 'show': True, 'tip': '距離30W訊號週數 (0=本週)', 'type': 'num'},
    'str_st_week_offset': {'name': 'ST買訊(前)', 'show': True, 'tip': '距離最近一次週線買訊週數', 'type': 'num'},
    'fund_contract_qoq': {'name': '合約季增(%)', 'show': True, 'tip': '合約負債季增率(%) (大於0較佳)', 'type': 'num'},
    'fund_inventory_qoq': {'name': '庫存季增(%)', 'show': True, 'tip': '庫存季增率(%) (建議 <= 0 代表庫存去化)',
                           'type': 'num'},
    'fund_op_cash_flow': {'name': '營業現金流', 'show': False, 'tip': '最新營業現金流 (建議 > 0)', 'type': 'num'},

    # 🔥 加入 5日 欄位，保留 20日
    'margin_diff_5d': {'name': '融資5日增減(%)', 'show': True, 'tip': '融資使用率 5日增減 (建議 <= 0)', 'type': 'num'},
    'legal_diff_5d': {'name': '法人5日增減(%)', 'show': True, 'tip': '三大法人持股 5日增減 (建議 >= 0)', 'type': 'num'},
    'margin_diff_20d': {'name': '融資20日增減(%)', 'show': False, 'tip': '融資使用率 20日增減 (建議 <= 0)',
                        'type': 'num'},
    'legal_diff_20d': {'name': '法人20日增減(%)', 'show': False, 'tip': '三大法人持股 20日增減 (建議 >= 0)',
                       'type': 'num'},

    'dj_main_ind': {'name': '主業', 'show': False, 'tip': 'MoneyDJ 主產業分類', 'type': 'str'},
    'dj_sub_ind': {'name': '細產業', 'show': True, 'tip': 'MoneyDJ 細產業分類', 'type': 'str'},
}

# ==========================================
# 2. 全數值過濾設定 (加入 Category 分類與5日籌碼)
# ==========================================
FULL_FILTER_SPECS = [
    {'category': '📈 技術面', 'key': '現價', 'label': '股價', 'min': 0, 'max': 5000, 'step': 10, 'suffix': ''},
    {'category': '📈 技術面', 'key': '漲幅1d', 'label': '今日漲幅(%)', 'min': -20, 'max': 20, 'step': 1.0,
     'suffix': '%'},
    {'category': '📈 技術面', 'key': '漲幅5d', 'label': '5日漲幅(%)', 'min': -50, 'max': 100, 'step': 1.0,
     'suffix': '%'},
    {'category': '📈 技術面', 'key': '漲幅20d', 'label': '月漲幅(%)', 'min': -50, 'max': 200, 'step': 1.0,
     'suffix': '%'},
    {'category': '📈 技術面', 'key': '漲幅60d', 'label': '季漲幅(%)', 'min': -50, 'max': 500, 'step': 5.0,
     'suffix': '%'},
    {'category': '📈 技術面', 'key': 'RS強度', 'label': 'RS強度', 'min': 0, 'max': 99, 'step': 1.0, 'suffix': ''},
    {'category': '📈 技術面', 'key': 'bb_width', 'label': '布林寬(%)', 'min': 0, 'max': 50, 'step': 0.5, 'suffix': '%'},
    {'category': '📈 技術面', 'key': '量比', 'label': '量比(倍)', 'min': 0, 'max': 50, 'step': 0.5, 'suffix': ''},
    {'category': '📈 技術面', 'key': 'str_30w_week_offset', 'label': '30W起漲週數(前)', 'min': -1, 'max': 52, 'step': 1,
     'suffix': '週'},
    {'category': '📈 技術面', 'key': 'str_st_week_offset', 'label': 'ST買訊(前)', 'min': -1, 'max': 26, 'step': 1,
     'suffix': '週'},

    {'category': '💰 籌碼面', 'key': 't_streak', 'label': '投信連買(日)', 'min': 0, 'max': 30, 'step': 1, 'suffix': ''},
    {'category': '💰 籌碼面', 'key': 't_net_today', 'label': '投信今日(張)', 'min': -20000, 'max': 20000, 'step': 100,
     'suffix': ''},
    {'category': '💰 籌碼面', 'key': 't_sum_5d', 'label': '投信5日(張)', 'min': -50000, 'max': 50000, 'step': 100,
     'suffix': ''},
    {'category': '💰 籌碼面', 'key': 't_sum_20d', 'label': '投信20日(張)', 'min': -100000, 'max': 100000, 'step': 500,
     'suffix': ''},
    {'category': '💰 籌碼面', 'key': 'f_streak', 'label': '外資連買(日)', 'min': 0, 'max': 30, 'step': 1, 'suffix': ''},
    {'category': '💰 籌碼面', 'key': 'f_net_today', 'label': '外資今日(張)', 'min': -50000, 'max': 50000, 'step': 500,
     'suffix': ''},
    {'category': '💰 籌碼面', 'key': 'f_sum_5d', 'label': '外資5日(張)', 'min': -100000, 'max': 100000, 'step': 500,
     'suffix': ''},
    {'category': '💰 籌碼面', 'key': 'f_sum_20d', 'label': '外資20日(張)', 'min': -200000, 'max': 200000, 'step': 1000,
     'suffix': ''},
    {'category': '💰 籌碼面', 'key': 'm_net_today', 'label': '融資今日(張)', 'min': -20000, 'max': 20000, 'step': 100,
     'suffix': ''},
    {'category': '💰 籌碼面', 'key': 'm_sum_5d', 'label': '融資5日(張)', 'min': -50000, 'max': 50000, 'step': 100,
     'suffix': ''},

    # 🔥 加入 5日 濾網
    {'category': '💰 籌碼面', 'key': 'legal_diff_5d', 'label': '法人5日增(%)', 'min': -100, 'max': 100, 'step': 0.5,
     'suffix': '%'},
    {'category': '💰 籌碼面', 'key': 'margin_diff_5d', 'label': '融資5日增(%)', 'min': -100, 'max': 100, 'step': 0.5,
     'suffix': '%'},
    {'category': '💰 籌碼面', 'key': 'legal_diff_20d', 'label': '法人20日增(%)', 'min': -100, 'max': 100, 'step': 0.5,
     'suffix': '%'},
    {'category': '💰 籌碼面', 'key': 'margin_diff_20d', 'label': '融資20日增(%)', 'min': -100, 'max': 100, 'step': 0.5,
     'suffix': '%'},

    {'category': '📖 基本面', 'key': 'rev_yoy', 'label': '月營收年增(%)', 'min': -100, 'max': 1000, 'step': 5.0,
     'suffix': '%'},
    {'category': '📖 基本面', 'key': 'rev_cum_yoy', 'label': '累營年增(%)', 'min': -100, 'max': 1000, 'step': 5.0,
     'suffix': '%'},
    {'category': '📖 基本面', 'key': 'fund_eps_cum', 'label': '累計EPS(元)', 'min': -50, 'max': 200, 'step': 0.5,
     'suffix': ''},
    {'category': '📖 基本面', 'key': 'pe', 'label': '本益比', 'min': 0, 'max': 200, 'step': 1.0, 'suffix': ''},
    {'category': '📖 基本面', 'key': 'pbr', 'label': '股價淨值比', 'min': 0, 'max': 20, 'step': 0.1, 'suffix': ''},
    {'category': '📖 基本面', 'key': 'yield', 'label': '殖利率(%)', 'min': 0, 'max': 20, 'step': 0.5, 'suffix': '%'},
    {'category': '📖 基本面', 'key': 'fund_contract_qoq', 'label': '合約季增(%)', 'min': -100, 'max': 500, 'step': 1.0,
     'suffix': '%'},
    {'category': '📖 基本面', 'key': 'fund_inventory_qoq', 'label': '庫存季增(%)', 'min': -100, 'max': 500, 'step': 1.0,
     'suffix': '%'},
    {'category': '📖 基本面', 'key': 'fund_op_cash_flow', 'label': '營業現金流', 'min': -99999999, 'max': 99999999,
     'step': 100, 'suffix': ''}
]

# 🔥 預設的 6 個濾網條件 (改為 5日籌碼)
DEFAULT_ACTIVE_FILTERS = [
    'str_30w_week_offset', 'str_st_week_offset', 'rev_cum_yoy',
    'fund_eps_cum', 'legal_diff_5d', 'margin_diff_5d'
]

# 🔥 重新組織特徵分類 (30W黏貼後突破 移至特殊型態)
TAG_CATEGORIES = {
    "📈 趨勢與突破": ["ST轉多", "突破30週", "創季高", "創月高", "強勢多頭", "波段黑馬", "超強勢"],
    "📉 壓縮與整理": ["極度壓縮", "波動壓縮", "盤整5日", "盤整10日", "盤整20日", "盤整60日"],
    "💰 籌碼與主力": ["主力掃單(ILSS)", "投信認養", "散戶退場", "土洋對作"],
    "⚠️ 特殊型態": ["30W黏貼後突破", "30W甩轎", "假跌破", "回測季線", "回測年線", "Vix反轉"]
}

# 🔥 補齊所有標籤的 Tooltip
TAG_TOOLTIPS = {
    'ST轉多': '近 4 週內週線 SuperTrend 指標由空翻多，觸發波段買進訊號',
    '突破30週': '股價剛由下往上突破 30 週移動平均線',
    '創季高': '股價突破近 60 日 (3個月) 以來最高價',
    '創月高': '股價突破近 30 日 (1個月) 以來最高價',
    '強勢多頭': '均線呈現完美多頭排列 (MA5 > 10 > 20 > 60)',
    '波段黑馬': '近 60 日累積漲幅已經超過 30%',
    '超強勢': 'RS 強度大於 90，屬於全市場前 10% 的強勢股',
    '30W黏貼後突破': 'MA30 走平且股價在均線附近震盪沉澱後，首度帶量突破起漲',
    '極度壓縮': '布林通道寬度小於 5%，呈現極致的籌碼沉澱與無波動狀態',
    '波動壓縮': '布林通道寬度小於 8%，波動率正在收斂',
    '盤整5日': '布林寬度近 5 日皆處於收斂狀態',
    '盤整10日': '布林寬度近 10 日皆處於收斂狀態',
    '盤整20日': '布林寬度近 20 日皆處於收斂狀態',
    '盤整60日': '布林寬度長達 60 日皆處於收斂狀態 (大底成型)',
    '主力掃單(ILSS)': '[嚴格] MA200上 + 假跌破掃單 + 爆量站回 + 營收成長 + 融資退場',
    '投信認養': '投信連續買超天數達到 3 天以上',
    '散戶退場': '融資今日單日大減超過 200 張',
    '土洋對作': '投信與外資買賣方向相反 (通常指投信買、外資賣的籌碼換手)',
    '30W甩轎': 'MA30 趨勢向上，股價回測跌破均線後，在 10 週內迅速站回',
    '假跌破': '股價昨日跌破月線(MA20)，今日立刻反彈站回月線之上',
    '回測季線': '股價回測並防守住季線 (MA60) 支撐',
    '回測年線': '股價回測並防守住年線 (MA200) 支撐',
    'Vix反轉': '恐慌指數(Vix)飆高後出現極端反轉訊號，代表短線可能見底'
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
        self.resize(350, 500)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        lbl = QLabel("請勾選要顯示的數值過濾器：")
        lbl.setStyleSheet("color: #00E5FF; font-weight: bold;")
        layout.addWidget(lbl)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("background: transparent; border: none;")
        content = QWidget()
        vbox = QVBoxLayout(content)

        for cfg in self.all_filters:
            chk = QCheckBox(f"[{cfg['category']}] {cfg['label']}")
            chk.setChecked(cfg['key'] in self.active_keys)
            self.checkboxes[cfg['key']] = chk
            vbox.addWidget(chk)

        vbox.addStretch()
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
        # 🔥 修改此處：寬度從 100 增加至 115，避免長文字被蓋住
        self.lbl_name.setFixedWidth(115)
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
        spin.setRange(-999999999, 999999999)
        spin.setDecimals(
            1 if '張' not in self.config.get('label', '') and '流' not in self.config.get('label', '') else 0)
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

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid(): return None
        col_key = self.visible_cols[index.column()]

        if col_key == 'insight' and role == Qt.ItemDataRole.DisplayRole:
            return "🔍"

        value = self._df.iloc[index.row()][col_key]

        if role == Qt.ItemDataRole.UserRole: return value

        if role == Qt.ItemDataRole.DisplayRole:
            if isinstance(value, (int, float)):
                if col_key == '現價':
                    fmt_val = f"{value:,.2f}"
                    if fmt_val.endswith('.00'):
                        return fmt_val[:-3]
                    elif fmt_val.endswith('0'):
                        return fmt_val[:-1]
                    return fmt_val

                if col_key in ['RS強度', 'pe', 'pbr', '量比', 'fund_eps_cum']: return f"{value:.1f}"
                if 'rev_now' in col_key: return f"{value:,.0f}"
                if '漲幅' in col_key or 'yield' in col_key or 'width' in col_key or 'yoy' in col_key or 'qoq' in col_key or 'diff' in col_key: return f"{value:.2f}%"
                if 'sum' in col_key or 'net' in col_key or 'cash_flow' in col_key: return f"{value:,.0f}"
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
            if col_key == '現價':
                pct_1d = self._df.iloc[index.row()].get('漲幅1d', 0)
                if pct_1d >= 9.5 or pct_1d <= -9.5: return QColor("#FFFFFF")
                if pct_1d > 0: return QColor("#FF4444")
                if pct_1d < 0: return QColor("#00CC00")
                return QColor("#E0E0E0")

            if isinstance(value, (int, float)):
                if '漲幅' in col_key or 'sum' in col_key or '買賣超' in col_key or 'yoy' in col_key or 'eps' in col_key or 'streak' in col_key or 'qoq' in col_key or 'diff' in col_key or 'cash_flow' in col_key:
                    if value > 0: return QColor("#FF4444")
                    if value < 0: return QColor("#00CC00")

            if col_key == '強勢特徵' and value:
                if 'ST剛轉多' in str(value): return QColor("#FF3333")
                if '30W' in str(value): return QColor("#00E5FF")
                if 'ILSS' in str(value): return QColor("#FF00FF")
                if '土洋' in str(value): return QColor("#FFFF00")
                return QColor("#E0E0E0")
            return QColor("#E0E0E0")

        if role == Qt.ItemDataRole.BackgroundRole:
            if col_key == '現價':
                pct_1d = self._df.iloc[index.row()].get('漲幅1d', 0)
                if pct_1d >= 9.5: return QColor("#880000")
                if pct_1d <= -9.5: return QColor("#004400")
            return None

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

        self.last_load_time = 0

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
        title.setStyleSheet(
            "font-size: 16px; font-weight: bold; color: #00E5FF; border: none; background: transparent;")

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

        # 👇 替換開始 (雙層佈局與智慧搜尋選單) 👇

        # 🌟 下拉選單按鈕的專屬樣式
        editable_combo_style = """
                    QComboBox { background: #111; color: #FFF; border: 1px solid #444; padding: 4px; font-size: 14px; }
                    QComboBox::drop-down { border-left: 1px solid #444; width: 24px; background: #222; }
                    QComboBox::drop-down:hover { background: #333; }
                    QComboBox::down-arrow { width: 0px; height: 0px; border-left: 5px solid transparent; border-right: 5px solid transparent; border-top: 6px solid #00E5FF; }
                    QComboBox QAbstractItemView { background: #111; color: #FFF; selection-background-color: #00E5FF; selection-color: #000; outline: none; }
                """

        # 🌟 智慧搜尋下拉清單 (Completer) 的專屬樣式，讓它在暗黑模式下超明顯
        completer_style = """
                    QListView { background-color: #1A1A1A; color: #00E5FF; border: 1px solid #00E5FF; font-size: 15px; font-weight: bold; }
                    QListView::item { padding: 8px; }
                    QListView::item:selected { background-color: #0066CC; color: #FFF; }
                """

        # 第一層：基本/自選 & 概念題材
        row1_layout = QHBoxLayout()

        v_ind = QVBoxLayout();
        v_ind.setSpacing(2)
        lbl_ind = QLabel("📂 基本/自選");
        lbl_ind.setProperty("class", "category-label")
        self.combo_industry = QComboBox()
        self.combo_industry.addItem("全部")
        self.combo_industry.currentIndexChanged.connect(self.apply_filters_debounce)
        v_ind.addWidget(lbl_ind);
        v_ind.addWidget(self.combo_industry)

        v_con = QVBoxLayout();
        v_con.setSpacing(2)
        lbl_con = QLabel("🏷️ 概念題材 (打字自動篩選)");
        lbl_con.setProperty("class", "category-label")
        self.combo_concept = QComboBox()
        self.combo_concept.addItem("全部")
        self.combo_concept.setEditable(True)
        self.combo_concept.setStyleSheet(editable_combo_style)
        self.combo_concept.lineEdit().setPlaceholderText("下拉選擇，或輸入關鍵字...")

        # 💡 設定概念題材的自動完成器 (打字即時篩選選單)
        comp_con = self.combo_concept.completer()
        comp_con.setFilterMode(Qt.MatchFlag.MatchContains)
        comp_con.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        comp_con.popup().setStyleSheet(completer_style)

        self.combo_concept.activated.connect(self.apply_filters_debounce)
        self.combo_concept.lineEdit().returnPressed.connect(self.apply_filters_debounce)
        # 💡 魔法在這裡：讓使用者按下 Enter 後，強制彈出篩選後的選單！
        self.combo_concept.lineEdit().returnPressed.connect(comp_con.complete)

        v_con.addWidget(lbl_con);
        v_con.addWidget(self.combo_concept)
        row1_layout.addLayout(v_ind)
        row1_layout.addLayout(v_con)
        ctrl_layout.addLayout(row1_layout)

        # 第二層：MDJ 產業細分 (獨立一行)
        v_dj = QVBoxLayout();
        v_dj.setSpacing(2)
        lbl_dj = QLabel("🏭 產業細分 (打字自動篩選)");
        lbl_dj.setProperty("class", "category-label")
        self.combo_dj_ind = QComboBox()
        self.combo_dj_ind.addItem("全部")
        self.combo_dj_ind.setEditable(True)
        self.combo_dj_ind.setStyleSheet(editable_combo_style)
        self.combo_dj_ind.lineEdit().setPlaceholderText("下拉選擇，或輸入主業/細產業...")

        # 💡 設定產業細分的自動完成器
        comp_dj = self.combo_dj_ind.completer()
        comp_dj.setFilterMode(Qt.MatchFlag.MatchContains)
        comp_dj.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        comp_dj.popup().setStyleSheet(completer_style)

        self.combo_dj_ind.activated.connect(self.apply_filters_debounce)
        self.combo_dj_ind.lineEdit().returnPressed.connect(self.apply_filters_debounce)
        # 💡 魔法在這裡：按下 Enter 強制彈出篩選後的選單！
        self.combo_dj_ind.lineEdit().returnPressed.connect(comp_dj.complete)

        v_dj.addWidget(lbl_dj);
        v_dj.addWidget(self.combo_dj_ind)
        ctrl_layout.addLayout(v_dj)
        # 👆 替換結束 👆

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
        self.table_view.clicked.connect(self.on_table_clicked)
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

    def showEvent(self, event):
        super().showEvent(event)
        base_path = Path(__file__).resolve().parent.parent
        f_path = base_path / "data" / "strategy_results" / "factor_snapshot.parquet"
        csv_path = base_path / "data" / "strategy_results" / "戰情室今日快照_全中文版.csv"

        latest_mtime = 0
        if f_path.exists():
            latest_mtime = max(latest_mtime, f_path.stat().st_mtime)
        elif csv_path.exists():
            latest_mtime = max(latest_mtime, csv_path.stat().st_mtime)

        if latest_mtime > getattr(self, 'last_load_time', 0):
            print("🔄 偵測到背景資料已更新，自動重新整理選股畫面...")
            self.load_data()

    def on_data_loaded(self, df):
        self.full_df = df
        self.update_industry_combo()
        self._update_tag_checkboxes()
        self.apply_filters_real()
        self.lbl_status.setText(f"✅ {len(df)} 檔")
        self.btn_reload.setEnabled(True)
        self.last_load_time = time.time()

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

        # 👇 新增：動態產生概念股下拉選單 👇
        concept_items = ["全部"]
        if 'sub_concepts' in self.full_df.columns:
            all_tags = set()
            for tags in self.full_df['sub_concepts'].dropna():
                if tags:
                    # 分割逗號並去除空白
                    for t in str(tags).split(','):
                        if t.strip(): all_tags.add(t.strip())
            concept_items.extend(sorted(list(all_tags)))

            # 👇 替換概念與產業下拉選單的更新邏輯 👇
            curr_concept = self.combo_concept.currentText()
            self.combo_concept.blockSignals(True)
            self.combo_concept.clear()
            self.combo_concept.addItems(concept_items)
            # 就算打的字不完全等於選單項目，也要保留在搜尋框內
            if curr_concept: self.combo_concept.setCurrentText(curr_concept)
            self.combo_concept.blockSignals(False)

            # 動態產生 MDJ 細產業選單
            dj_items = ["全部"]
            if 'dj_sub_ind' in self.full_df.columns and 'dj_main_ind' in self.full_df.columns:
                dj_set = set()
                for _, row in self.full_df[['dj_main_ind', 'dj_sub_ind']].dropna().iterrows():
                    m_ind = row['dj_main_ind']
                    for sub in str(row['dj_sub_ind']).split(','):
                        if sub.strip():
                            dj_set.add(f"[{m_ind}] {sub.strip()}")
                dj_items.extend(sorted(list(dj_set)))

            curr_dj = self.combo_dj_ind.currentText()
            self.combo_dj_ind.blockSignals(True)
            self.combo_dj_ind.clear()
            self.combo_dj_ind.addItems(dj_items)
            # 保留使用者輸入的產業搜尋字
            if curr_dj: self.combo_dj_ind.setCurrentText(curr_dj)
            self.combo_dj_ind.blockSignals(False)
        # 👇 新增：動態產生 MDJ 細產業選單 (格式: [主業] 細產業) 👇
        dj_items = ["全部"]
        if 'dj_sub_ind' in self.full_df.columns and 'dj_main_ind' in self.full_df.columns:
            dj_set = set()
            for _, row in self.full_df[['dj_main_ind', 'dj_sub_ind']].dropna().iterrows():
                m_ind = row['dj_main_ind']
                for sub in str(row['dj_sub_ind']).split(','):
                    if sub.strip():
                        dj_set.add(f"[{m_ind}] {sub.strip()}")
            dj_items.extend(sorted(list(dj_set)))

        curr_dj = self.combo_dj_ind.currentText()
        self.combo_dj_ind.blockSignals(True)
        self.combo_dj_ind.clear()
        self.combo_dj_ind.addItems(dj_items)
        if curr_dj in dj_items: self.combo_dj_ind.setCurrentText(curr_dj)
        self.combo_dj_ind.blockSignals(False)

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
        self.combo_concept.setCurrentIndex(0)  # <-- 清除概念題材
        self.combo_dj_ind.setCurrentIndex(0)   # <-- 清除產業細分
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

        # 1. 處理上方搜尋框 (全域搜尋)
        search_txt = self.txt_search.text().strip()
        if search_txt:
            mask = df['sid'].str.contains(search_txt) | df['name'].str.contains(search_txt)
            if 'sub_concepts' in df.columns:
                mask = mask | df['sub_concepts'].fillna('').str.contains(search_txt)
            if 'dj_sub_ind' in df.columns:
                mask = mask | df['dj_sub_ind'].fillna('').str.contains(search_txt)
            df = df[mask]

        # 2. 處理 📂 基本/自選 過濾
        ind = self.combo_industry.currentText()
        if ind.startswith("[自選] "):
            group_name = ind.replace("[自選] ", "")
            if group_name in self.watchlist_data:
                target_sids = self.watchlist_data[group_name]
                target_sids = [str(x).strip() for x in target_sids]
                df = df[df['sid'].isin(target_sids)]
        elif ind != "全部":
            df = df[df['industry'] == ind]

        # 3. 處理 🏷️ 概念題材過濾 (支援手打字)
        concept_txt = self.combo_concept.currentText().strip()
        if concept_txt != "全部" and concept_txt != "" and 'sub_concepts' in df.columns:
            df = df[df['sub_concepts'].fillna('').str.contains(concept_txt, regex=False)]

        # 4. 處理 🏭 MDJ 產業細分過濾 (智慧雙重比對)
        dj_txt = self.combo_dj_ind.currentText().strip()
        if dj_txt != "全部" and dj_txt != "" and 'dj_sub_ind' in df.columns and 'dj_main_ind' in df.columns:
            if "] " in dj_txt:
                # 情況 A：使用者用滑鼠點選完整的分類，例如 "[半導體] IC設計"
                target_sub = dj_txt.split("] ")[-1]
                df = df[df['dj_sub_ind'].fillna('').str.contains(target_sub, regex=False)]
            else:
                # 情況 B：使用者自己打關鍵字，例如 "半導體" 或 "車用" (主業跟細產業都找)
                mask_dj = df['dj_main_ind'].fillna('').str.contains(dj_txt, regex=False) | \
                          df['dj_sub_ind'].fillna('').str.contains(dj_txt, regex=False)
                df = df[mask_dj]

        # 5. 處理數值滑桿過濾 (SpinBoxes)
        is_dirty = False
        for w in self.dynamic_filters:
            if w.is_modified(): is_dirty = True
            key = w.key
            if key not in df.columns: continue
            min_val, max_val = w.spin_min.value(), w.spin_max.value()
            default_min, default_max = w.config['min'], w.config['max']
            if min_val != default_min: df = df[df[key] >= min_val]
            if max_val != default_max: df = df[df[key] <= max_val]

        # 6. 處理按鈕亮燈狀態與強勢特徵標籤 (Checkboxes)
        is_tag_dirty = any(chk.isChecked() for chk in self.chk_tags.values())
        if is_dirty or is_tag_dirty or ind != "全部" or search_txt or (concept_txt != "全部" and concept_txt != "") or (dj_txt != "全部" and dj_txt != ""):
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

        # 7. 整理最終顯示的 DataFrame
        df['insight'] = ''
        visible_cols = []

        if 'insight' in FULL_COLUMN_SPECS and FULL_COLUMN_SPECS['insight'].get('show', True):
            visible_cols.append('insight')

        for key in self.column_order:
            if key == 'insight': continue
            if key in FULL_COLUMN_SPECS and FULL_COLUMN_SPECS[key]['show']:
                if key in df.columns:
                    visible_cols.append(key)

        self.display_df = df[visible_cols].copy()

        # 8. 預設排序邏輯
        if '漲幅20d' in self.display_df.columns and self.proxy_model.sortColumn() == -1:
            self.display_df = self.display_df.sort_values('漲幅20d', ascending=False)

        # 9. 更新畫面與表格寬度
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

        # 10. 將焦點選回代號，保留鍵盤跳轉功能
        if 'sid' in visible_cols and len(self.display_df) > 0:
            self.table_view.setCurrentIndex(self.model.index(0, visible_cols.index('sid')))

    def on_table_clicked(self, index):
        """當單擊表格時，判斷是否點擊了放大鏡欄位"""
        col_idx = index.column()
        visible_cols = self.model.visible_cols
        col_key = visible_cols[col_idx]

        if col_key == 'insight':
            src_idx = self.proxy_model.mapToSource(index)
            row_data = self.display_df.iloc[src_idx.row()]
            sid = str(row_data['sid'])

            try:
                # 直接使用，不再從內部 import
                dlg = StockInsightDashboard(sid, row_data, self.full_df, self)
                dlg.show()
            except Exception as e:
                print(f"[Insight Log] 彈出視窗失敗: {e}")
                import traceback
                traceback.print_exc()

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