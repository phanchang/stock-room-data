import sys
import json
import time
import pandas as pd
import webbrowser
from PyQt6.QtWidgets import QApplication  # 如果原本沒有 QApplication 請補上
from pathlib import Path
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QTableView, QHeaderView, QGroupBox, QComboBox,
                             QDoubleSpinBox, QPushButton, QCheckBox,
                             QAbstractItemView, QMenu, QMessageBox, QSplitter,
                             QScrollArea, QFrame, QDialog, QGridLayout,
                             QDialogButtonBox, QRadioButton, QButtonGroup, QToolButton,
                             QSizePolicy, QInputDialog, QLineEdit, QListWidget, QListWidgetItem,
                             QCompleter,QTabWidget)
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

    /* 🌟 新增：確保所有提示框(Tooltip)字體超清晰亮眼 */
    QToolTip { background-color: #1E2632; color: #FFFFFF; border: 1px solid #00E5FF; font-size: 15px; padding: 4px; font-weight: bold; }

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

    /* 🌟 新增：分頁標籤 (TabWidget) 的亮眼設計 */
    QTabWidget::pane { border: 1px solid #333; background: #050505; border-radius: 4px; }
    QTabBar::tab { background: #151A22; color: #888; padding: 10px 12px; border: 1px solid #222; border-bottom: none; font-weight: bold; font-size: 15px; }
    QTabBar::tab:selected { background: #222; color: #00E5FF; border: 1px solid #00E5FF; border-bottom: none; }
    QTabBar::tab:hover:!selected { background: #1E2632; color: #FFF; }
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
        self.checked_sids = set()  # 用於記錄打勾的股票代號

    def update_data(self, df, visible_cols):
        self.beginResetModel()
        self._df = df
        self.visible_cols = visible_cols
        # 過濾後，只保留還在當前表格內的打勾名單
        current_sids = set(df['sid'].astype(str)) if 'sid' in df.columns else set()
        self.checked_sids = self.checked_sids.intersection(current_sids)
        self.endResetModel()

    def rowCount(self, parent=None):
        return self._df.shape[0]

    def columnCount(self, parent=None):
        return len(self.visible_cols) + 1  # 第 0 欄為 Checkbox

    def flags(self, index):
        if not index.isValid(): return Qt.ItemFlag.NoItemFlags
        base_flags = Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable
        if index.column() == 0:  # 讓第 0 欄可以被打勾
            return base_flags | Qt.ItemFlag.ItemIsUserCheckable
        return base_flags

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid(): return None

        # 處理第 0 欄 (Checkbox)
        if index.column() == 0:
            sid = str(self._df.iloc[index.row()].get('sid', ''))
            if role == Qt.ItemDataRole.CheckStateRole:
                return Qt.CheckState.Checked if sid in self.checked_sids else Qt.CheckState.Unchecked
            return None

        # 處理資料欄位 (因為多了 Checkbox，所以可視欄位 index 要減 1)
        col_key = self.visible_cols[index.column() - 1]
        value = self._df.iloc[index.row()][col_key]

        if col_key == 'insight' and role == Qt.ItemDataRole.DisplayRole:
            return "🔍"
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
                if 'ST轉多' in str(value): return QColor("#FF3333")
                if '30W' in str(value): return QColor("#00E5FF")
                if 'ILSS' in str(value): return QColor("#FF00FF")
                if '土洋' in str(value): return QColor("#FFFF00")
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

    def setData(self, index, value, role=Qt.ItemDataRole.EditRole):
        if index.isValid() and role == Qt.ItemDataRole.CheckStateRole and index.column() == 0:
            sid = str(self._df.iloc[index.row()].get('sid', ''))
            if value == Qt.CheckState.Checked.value:
                self.checked_sids.add(sid)
            else:
                self.checked_sids.discard(sid)
            self.dataChanged.emit(index, index, [Qt.ItemDataRole.CheckStateRole])
            return True
        return False

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal:
            if section == 0:
                if role == Qt.ItemDataRole.DisplayRole: return "選"
                return None
            col_key = self.visible_cols[section - 1]
            config = FULL_COLUMN_SPECS.get(col_key, {})
            if role == Qt.ItemDataRole.DisplayRole: return config.get('name', col_key)
            if role == Qt.ItemDataRole.ToolTipRole: return config.get('tip', '')
        return None


class NumericSortProxy(QSortFilterProxyModel):
    def lessThan(self, left, right):
        if left.column() == 0:  # 支援以打勾狀態排序 (修復 PyQt6 Enum 比對錯誤)
            l_chk = self.sourceModel().data(left, Qt.ItemDataRole.CheckStateRole)
            r_chk = self.sourceModel().data(right, Qt.ItemDataRole.CheckStateRole)
            l_val = l_chk.value if l_chk else 0
            r_val = r_chk.value if r_chk else 0
            return l_val < r_val

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


        # ==========================================
        # 建立 Tab Widget (標籤分頁) 解決筆電版面擁擠問題
        # ==========================================
        self.tab_widget = QTabWidget()
        self.tab_widget.setStyleSheet("background: transparent;")

        # ------------------------------------------
        # Tab 1: 🔍 基礎分類
        # ------------------------------------------
        tab_search = QWidget()
        lay_search = QVBoxLayout(tab_search)
        lay_search.setSpacing(10)

        editable_combo_style = """
                    QComboBox { background: #111; color: #FFF; border: 1px solid #444; padding: 4px; font-size: 14px; }
                    QComboBox::drop-down { border-left: 1px solid #444; width: 24px; background: #222; }
                    QComboBox::drop-down:hover { background: #333; }
                    QComboBox QAbstractItemView { background: #111; color: #FFF; selection-background-color: #00E5FF; selection-color: #000; outline: none; }
                """
        completer_style = """
                    QListView { background-color: #1A1A1A; color: #00E5FF; border: 1px solid #00E5FF; font-size: 15px; font-weight: bold; }
                    QListView::item { padding: 8px; }
                    QListView::item:selected { background-color: #0066CC; color: #FFF; }
                """

        lbl_ind = QLabel("📂 基本/自選");
        lbl_ind.setProperty("class", "category-label")
        self.combo_industry = QComboBox()
        self.combo_industry.addItem("全部")
        self.combo_industry.currentIndexChanged.connect(self.apply_filters_debounce)

        lbl_con = QLabel("🏷️ 概念題材 (打字自動篩選)");
        lbl_con.setProperty("class", "category-label")
        self.combo_concept = QComboBox()
        self.combo_concept.addItem("全部")
        self.combo_concept.setEditable(True)
        self.combo_concept.setStyleSheet(editable_combo_style)
        self.combo_concept.lineEdit().setPlaceholderText("輸入關鍵字...")
        comp_con = self.combo_concept.completer()
        comp_con.setFilterMode(Qt.MatchFlag.MatchContains)
        comp_con.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        comp_con.popup().setStyleSheet(completer_style)
        self.combo_concept.activated.connect(self.apply_filters_debounce)
        self.combo_concept.lineEdit().returnPressed.connect(self.apply_filters_debounce)
        self.combo_concept.lineEdit().returnPressed.connect(comp_con.complete)

        lbl_dj = QLabel("🏭 產業細分 (打字自動篩選)");
        lbl_dj.setProperty("class", "category-label")
        self.combo_dj_ind = QComboBox()
        self.combo_dj_ind.addItem("全部")
        self.combo_dj_ind.setEditable(True)
        self.combo_dj_ind.setStyleSheet(editable_combo_style)
        self.combo_dj_ind.lineEdit().setPlaceholderText("輸入主業/細產業...")
        comp_dj = self.combo_dj_ind.completer()
        comp_dj.setFilterMode(Qt.MatchFlag.MatchContains)
        comp_dj.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        comp_dj.popup().setStyleSheet(completer_style)
        self.combo_dj_ind.activated.connect(self.apply_filters_debounce)
        self.combo_dj_ind.lineEdit().returnPressed.connect(self.apply_filters_debounce)
        self.combo_dj_ind.lineEdit().returnPressed.connect(comp_dj.complete)

        lay_search.addWidget(lbl_ind)
        lay_search.addWidget(self.combo_industry)
        lay_search.addWidget(lbl_con)
        lay_search.addWidget(self.combo_concept)
        lay_search.addWidget(lbl_dj)
        lay_search.addWidget(self.combo_dj_ind)
        lay_search.addStretch()

        # ------------------------------------------
        # Tab 2: 📊 數值濾網 (保留你最大的微調權力)
        # ------------------------------------------
        tab_numeric = QWidget()
        lay_numeric = QVBoxLayout(tab_numeric)
        lay_numeric.setContentsMargins(0, 5, 0, 0)

        filter_header_box = QHBoxLayout()
        self.btn_reset = QPushButton("🧹 清除條件")
        self.btn_reset.setFixedSize(100, 30)
        self.btn_reset.clicked.connect(self.reset_filters)
        self.btn_filter_setting = QToolButton()
        self.btn_filter_setting.setText("⚙️ 設定欄位")
        self.btn_filter_setting.clicked.connect(self.open_filter_setting)

        filter_header_box.addWidget(self.btn_reset)
        filter_header_box.addStretch()
        filter_header_box.addWidget(self.btn_filter_setting)
        lay_numeric.addLayout(filter_header_box)

        # 數值濾網捲動區
        scroll_num = QScrollArea()
        scroll_num.setWidgetResizable(True)
        scroll_num.setStyleSheet("background: transparent; border: none;")
        self.filter_container_widget = QWidget()
        self.filter_layout = QVBoxLayout(self.filter_container_widget)
        self.filter_layout.setContentsMargins(5, 5, 5, 5)
        self.filter_layout.setSpacing(5)
        scroll_num.setWidget(self.filter_container_widget)
        lay_numeric.addWidget(scroll_num)

        self.rebuild_filter_ui()  # 建立滑桿

        # ------------------------------------------
        # Tab 3: 🔥 特徵標籤
        # ------------------------------------------
        tab_tags = QWidget()
        lay_tags = QVBoxLayout(tab_tags)

        logic_layout = QHBoxLayout()
        self.logic_group = QButtonGroup(self)
        self.rb_and = QRadioButton("交集 (AND)")
        self.rb_or = QRadioButton("聯集 (OR)")
        self.rb_and.setChecked(True)
        self.logic_group.addButton(self.rb_and)
        self.logic_group.addButton(self.rb_or)
        self.rb_and.toggled.connect(self.apply_filters_debounce)
        self.rb_or.toggled.connect(self.apply_filters_debounce)
        logic_layout.addWidget(self.rb_and)
        logic_layout.addWidget(self.rb_or)
        lay_tags.addLayout(logic_layout)

        scroll_tags = QScrollArea()
        scroll_tags.setWidgetResizable(True)
        scroll_tags.setStyleSheet("background: transparent; border: none;")
        self.tag_container = QWidget()
        self.tag_layout = QVBoxLayout(self.tag_container)
        self.tag_layout.setContentsMargins(0, 0, 0, 0)
        scroll_tags.setWidget(self.tag_container)
        lay_tags.addWidget(scroll_tags)

        # ------------------------------------------
        # Tab 4: 🏆 戰略劇本 (AI 推薦設定，點擊自動帶入參數)
        # ------------------------------------------
        tab_presets = QWidget()
        lay_presets = QVBoxLayout(tab_presets)
        lay_presets.setSpacing(15)

        lbl_preset_desc = QLabel(
            "💡 點選下方劇本，系統將為您「自動填入」對應的數值濾網。\n填寫後會跳轉至數值區，您可依今日盤勢進行彈性微調。")
        lbl_preset_desc.setStyleSheet("color: #FFD700; font-weight: bold; font-size: 14px;")
        lbl_preset_desc.setWordWrap(True)
        lay_presets.addWidget(lbl_preset_desc)

        presets = [
            ("🏦 暗渡陳倉 (法人默默吃貨)", "股價波動極小，但三大法人近一個月偷偷吸籌碼 > 1.5%，且散戶融資退場。"),
            ("🔭 春江水暖 (合約負債爆發)", "不管現在營收好不好，合約負債(未來訂單)季增 > 15%，且本業有現金流入。"),
            ("🎯 30W 臨門一腳 (極致壓縮)", "布林帶寬壓縮至 6% 以下，長線季線翻揚，且今日量縮至 0.6 倍以下，隨時噴發。"),
            ("⚔️ 破底翻黃金坑 (主力洗盤)", "觸發假跌破或甩轎訊號，且近五日融資大減 300 張以上，清洗浮額乾淨。")
        ]

        for p_name, p_desc in presets:
            btn = QPushButton(p_name)
            btn.setStyleSheet("""
                        QPushButton { background-color: #1A237E; color: #00E5FF; border: 1px solid #3F51B5; padding: 10px; font-size: 16px; border-radius: 6px; text-align: left; font-weight: bold;} 
                        QPushButton:hover { background-color: #283593; color: #FFFFFF; border: 1px solid #00E5FF;}
                    """)
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn.clicked.connect(lambda checked, n=p_name: self.apply_preset(n))

            lbl_desc = QLabel(p_desc)
            lbl_desc.setStyleSheet("color: #BBBBBB; font-size: 13.5px; margin-bottom: 5px;")
            lbl_desc.setWordWrap(True)

            lay_presets.addWidget(btn)
            lay_presets.addWidget(lbl_desc)

        lay_presets.addStretch()

        # 將分頁加入 TabWidget
        self.tab_widget.addTab(tab_search, "🔍 分類")
        self.tab_widget.addTab(tab_numeric, "📊 數值")
        self.tab_widget.addTab(tab_tags, "🔥 特徵")
        self.tab_widget.addTab(tab_presets, "🏆 劇本")

        ctrl_layout.addWidget(self.tab_widget)

        self.lbl_status = QLabel("就緒")
        self.lbl_status.setStyleSheet(
            "color: #00E5FF; font-size: 14px; margin-top: 5px; border:none; background:transparent; font-weight: bold;")
        ctrl_layout.addWidget(self.lbl_status)

        self.chk_tags = {}
        # ==========================================
        # 建立右側表格區塊 (頂部 AI 智慧指揮列 + 表格)
        # ==========================================
        table_widget = QWidget()
        table_layout = QVBoxLayout(table_widget)
        table_layout.setContentsMargins(0, 0, 0, 0)
        table_layout.setSpacing(0)

        # 🚀 頂部 AI 智慧指揮列 (絕對不會被筆電下方截斷)
        self.ai_command_bar = QWidget()
        self.ai_command_bar.setStyleSheet("background-color: #0A0F16; border-bottom: 2px solid #2B3544;")
        self.ai_command_bar.setFixedHeight(50)
        ai_layout = QHBoxLayout(self.ai_command_bar)
        ai_layout.setContentsMargins(15, 5, 15, 5)

        self.lbl_table_status = QLabel("篩選結果: 0 檔  |  已勾選: 0 檔")
        self.lbl_table_status.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 16px; border: none;")
        self.btn_select_all = QPushButton("☑️全選")
        self.btn_deselect_all = QPushButton("🔲全不選")
        for btn in [self.btn_select_all, self.btn_deselect_all]:
            btn.setStyleSheet("""
                        QPushButton { background: transparent; color: #AAA; border: 1px solid #444; border-radius: 3px; padding: 2px 6px; font-size: 13px; font-weight: bold;}
                        QPushButton:hover { background: #333; color: #FFF; border: 1px solid #00E5FF; }
                    """)
        self.btn_select_all.clicked.connect(self.select_all_rows)
        self.btn_deselect_all.clicked.connect(self.deselect_all_rows)

        self.btn_ai_run = QPushButton("🚀 啟動 AI 聯網深掘")
        self.btn_ai_copy = QPushButton("📋 複製備援 (限 10 檔)")
        self.btn_ai_copy.clicked.connect(self.copy_ai_prompt_to_clipboard)
        self.btn_ai_history = QPushButton("📂 歷史戰報")

        for btn in [self.btn_ai_run, self.btn_ai_copy, self.btn_ai_history]:
            btn.setStyleSheet("""
                        QPushButton { background-color: #151A22; color: #666; border: 1px solid #333; padding: 6px 15px; font-weight: bold; border-radius: 4px; font-size: 14px;}
                        QPushButton:enabled { background-color: #1A237E; color: #00E5FF; border: 1px solid #3F51B5; }
                        QPushButton:enabled:hover { background-color: #283593; color: #FFF; }
                    """)
            if btn != self.btn_ai_history:
                btn.setEnabled(False)

        ai_layout.addWidget(self.lbl_table_status)
        ai_layout.addSpacing(15)  # 加一點小間距
        ai_layout.addWidget(self.btn_select_all)
        ai_layout.addWidget(self.btn_deselect_all)
        ai_layout.addStretch()
        ai_layout.addWidget(self.btn_ai_history)
        ai_layout.addWidget(self.btn_ai_copy)
        ai_layout.addWidget(self.btn_ai_run)

        table_layout.addWidget(self.ai_command_bar)

        # 📊 資料表格
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
                    QTableView::indicator { width: 18px; height: 18px; } /* 放大 Checkbox */
                    QTableView::indicator:checked { background-color: #00E5FF; border: 1px solid #00E5FF; border-radius: 3px; }
                """)

        self.model = StrategyTableModel()
        self.proxy_model = NumericSortProxy()
        self.proxy_model.setSourceModel(self.model)
        self.table_view.setModel(self.proxy_model)

        # 連接勾選狀態改變的事件，即時更新頂部指揮列
        self.model.dataChanged.connect(self.update_ai_command_bar)

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

    def apply_preset(self, preset_name):
        """套用戰略劇本：自動帶入參數，並切換回數值濾網頁面讓使用者微調"""
        # 1. 先清空現有條件
        self.reset_filters()

        # 2. 定義劇本參數字典 (對應 FULL_FILTER_SPECS 的 key)
        rules = {}
        target_tags = []

        if "暗渡陳倉" in preset_name:
            rules = {
                "漲幅20d": {"min": -5, "max": 8},
                "bb_width": {"max": 12},
                "legal_diff_20d": {"min": 1.5},
                "margin_diff_20d": {"max": 0}
            }
        elif "春江水暖" in preset_name:
            rules = {
                "fund_contract_qoq": {"min": 15},
                "fund_op_cash_flow": {"min": 0}
            }
        elif "30W 臨門一腳" in preset_name:
            rules = {
                "bb_width": {"max": 6},
                "漲幅60d": {"min": 0, "max": 20},
                "量比": {"max": 0.6}
            }
        elif "破底翻黃金坑" in preset_name:
            rules = {
                "m_sum_5d": {"max": -300}
            }
            target_tags = ["假跌破", "主力掃單(ILSS)", "30W甩轎"]
            self.rb_or.setChecked(True)  # 只要滿足其中一個洗盤特徵即可

        # 3. 確保這些欄位在畫面上是有顯示的 (如果原本被隱藏，就自動幫使用者打開)
        needed_keys = list(rules.keys())
        need_rebuild = False
        for k in needed_keys:
            if k not in self.active_filter_keys:
                self.active_filter_keys.append(k)
                need_rebuild = True

        if need_rebuild:
            self.rebuild_filter_ui()

        # 4. 將數值填入 SpinBox 中
        for w in self.dynamic_filters:
            if w.key in rules:
                if "min" in rules[w.key]:
                    w.spin_min.setValue(rules[w.key]["min"])
                if "max" in rules[w.key]:
                    w.spin_max.setValue(rules[w.key]["max"])

        # 5. 打勾對應的特徵標籤
        for tag in target_tags:
            if tag in self.chk_tags:
                self.chk_tags[tag].setChecked(True)

        # 6. 自動跳轉到【📊 數值濾網】分頁 (Index 1)，讓使用者可以即時查看並微調
        self.tab_widget.setCurrentIndex(1)

        # 7. 觸發篩選
        self.apply_filters_debounce()



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

        # 10. 更新畫面與表格寬度
        self.model.update_data(self.display_df, visible_cols)
        self.proxy_model.invalidate()
        self.update_ai_command_bar()  # 同步更新狀態列

        header = self.table_view.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.resizeSection(0, 40)  # Checkbox 欄位寬度

        for i, col in enumerate(visible_cols):
            real_idx = i + 1  # 扣除 Checkbox 偏移
            if col == '強勢特徵':
                header.resizeSection(real_idx, 220)
            elif col == 'name':
                header.resizeSection(real_idx, 110)
            elif 'sum' in col or 'net' in col:
                header.resizeSection(real_idx, 100)
            else:
                header.resizeSection(real_idx, 80)

    def on_table_clicked(self, index):
        """當單擊表格時，判斷是否點擊了放大鏡欄位"""
        col_idx = index.column()
        if col_idx == 0: return  # 點擊 Checkbox 由 Model 處理，略過

        visible_cols = self.model.visible_cols
        col_key = visible_cols[col_idx - 1]  # 扣除 Checkbox 偏移

        if col_key == 'insight':
            src_idx = self.proxy_model.mapToSource(index)
            row_data = self.display_df.iloc[src_idx.row()]
            sid = str(row_data['sid'])

            try:
                dlg = StockInsightDashboard(sid, row_data, self.full_df, self)
                dlg.show()
            except Exception as e:
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

    def update_ai_command_bar(self, top_left=None, bottom_right=None, roles=None):
        """動態更新頂部 AI 指揮列的數量與按鈕狀態"""
        total_count = len(self.display_df)
        checked_count = len(self.model.checked_sids)

        # 狀態文字更新
        self.lbl_table_status.setText(f"篩選結果: {total_count} 檔  |  已勾選: {checked_count} 檔")

        # 按鈕燈號邏輯 (1~5走API，1~10走備援)
        if checked_count == 0:
            self.btn_ai_run.setEnabled(False)
            self.btn_ai_copy.setEnabled(False)
            self.btn_ai_run.setText("🚀 啟動 AI 聯網深掘")
        elif 1 <= checked_count <= 5:
            self.btn_ai_run.setEnabled(True)
            self.btn_ai_copy.setEnabled(True)
            self.btn_ai_run.setText(f"🚀 啟動 AI 聯網深掘 ({checked_count} 檔)")
        elif 6 <= checked_count <= 10:
            self.btn_ai_run.setEnabled(False)  # 為了保護 API 額度反灰
            self.btn_ai_copy.setEnabled(True)
            self.btn_ai_run.setText("⚠️ 超過 5 檔，請使用備援")
        else:
            self.btn_ai_run.setEnabled(False)
            self.btn_ai_copy.setEnabled(False)
            self.btn_ai_run.setText("⛔ 數量過多，請減少勾選")

    def select_all_rows(self):
        """一鍵全選當前畫面上篩選出的所有股票"""
        if self.display_df.empty: return
        # 將畫面上所有的 sid 加入 set
        self.model.checked_sids = set(self.display_df['sid'].astype(str))

        # 觸發畫面更新 (僅更新第 0 欄)
        top_left = self.model.index(0, 0)
        bottom_right = self.model.index(self.model.rowCount() - 1, 0)
        self.model.dataChanged.emit(top_left, bottom_right, [Qt.ItemDataRole.CheckStateRole])
        self.update_ai_command_bar()

    def deselect_all_rows(self):
        """一鍵清除所有打勾狀態"""
        self.model.checked_sids.clear()

        if self.display_df.empty: return
        top_left = self.model.index(0, 0)
        bottom_right = self.model.index(self.model.rowCount() - 1, 0)
        self.model.dataChanged.emit(top_left, bottom_right, [Qt.ItemDataRole.CheckStateRole])
        self.update_ai_command_bar()

    def copy_ai_prompt_to_clipboard(self):
        """組裝打勾股票的數據，產生防幻覺提示詞，複製到剪貼簿並開啟 Gemini 網頁版"""
        if not self.model.checked_sids:
            QMessageBox.warning(self, "警告", "請先在表格左側勾選至少一檔股票！")
            return

        # 1. 取得畫面上被打勾的股票資料
        selected_df = self.display_df[self.display_df['sid'].astype(str).isin(self.model.checked_sids)]

        # 2. 組裝個股數據字串
        stock_info_str = ""
        for _, row in selected_df.iterrows():
            sid = str(row.get('sid', ''))
            name = str(row.get('name', ''))
            ind = str(row.get('industry', ''))
            dj_sub = str(row.get('dj_sub_ind', ''))
            tags = str(row.get('強勢特徵', ''))
            bbw = row.get('bb_width', 0)

            # 安全抓取可能有或沒有的數值
            legal_diff = row.get('legal_diff_20d', 0) if 'legal_diff_20d' in selected_df.columns else 0
            margin_diff = row.get('margin_diff_20d', 0) if 'margin_diff_20d' in selected_df.columns else 0
            rev_qoq = row.get('fund_contract_qoq', 0) if 'fund_contract_qoq' in selected_df.columns else 0

            stock_info_str += f"- 【{sid} {name}】 產業別: {ind} ({dj_sub})\n"
            stock_info_str += f"  * 內部量化特徵: {tags if tags else '無明顯特徵'}\n"
            stock_info_str += f"  * 關鍵數據: 布林寬度 {bbw:.1f}%, 法人20日增減 {legal_diff}%, 融資20日增減 {margin_diff}%, 合約負債季增 {rev_qoq}%\n\n"

        # 3. 組合黃金防護提示詞
        prompt = f"""[角色與核心紀律]
你是一位擁有 20 年經驗的頂尖台股量化與產業分析師。
你的任務是針對我提供的股票清單進行深度聯網查證與潛力評估。

⚠️ 嚴格禁令： 
1. 絕不允許產生幻覺或捏造資訊。引用數據做推理與研究必須使用 Google search，並做多方來源交叉查證，不自己產生真實數據。
2. 所有外部題材、法說會內容、法人報告或營收消息，優先參考 MoneyDJ、CNYES鉅亨網、Yahoo股市、UDN、玩股網、Goodinfo 等權威網站。
3. 引用數據做呈現或計算必須附上來源網站資料與確切日期。
4. 若查無近期明確消息，請誠實回報「查無近期重大催化劑」，不可硬編理由。

[輸入數據 - 內部量化嚴選名單]
{stock_info_str}
[輸出格式要求]
請以 Markdown 格式輸出，並包含以下三大區塊：

### 第一部分：個股客觀查證與短評
針對每一檔股票，請嚴格比對內部數據與你查證的外部資訊。
* **內部數據解讀**：簡述目前籌碼與型態結構。
* **外部聯網查證**：詳述近兩週的產業鏈動態、法說會重點或產品利多。
* **來源依據**：[網站名稱] (發布日期)

### 第二部分：綜合總結
總觀這幾檔股票，資金板塊是否有共同流向某個特定細產業的趨勢？目前的盤面氣氛適合積極進場還是耐心潛伏？

### 第三部分：潛力爆發排名與理由
請將這幾檔股票進行排名（由最具爆發潛力到最末），並給出具體排名的理由。排名需綜合考量「技術面聽牌完整度」與「題材爆發力」。
"""

        # 4. 寫入作業系統剪貼簿
        QApplication.clipboard().setText(prompt)

        # 5. 自動開啟 Gemini 網頁版
        try:
            webbrowser.open("https://gemini.google.com/app")
        except:
            pass  # 若開啟失敗(某些作業系統限制)，不影響剪貼簿功能

        # 6. 彈出成功通知
        QMessageBox.information(self, "✅ 備援提示詞已複製",
                                f"已成功將 {len(selected_df)} 檔股票的深度查證提示詞複製到剪貼簿！\n\n"
                                "已為您開啟 Gemini 網頁版，請直接貼上 (Ctrl+V) 即可進行深度分析。")