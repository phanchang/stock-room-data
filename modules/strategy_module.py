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
    'invest_trust_hold_pct': {'name': '投信持股%', 'show': True, 'tip': '最新投信持股比例', 'type': 'num'},
    'foreign_hold_pct': {'name': '外資持股%', 'show': True, 'tip': '最新外資持股比例', 'type': 'num'},
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
    {'category': '💰 籌碼面', 'key': 'invest_trust_hold_pct', 'label': '投信持股(%)', 'min': 0, 'max': 100, 'step': 0.1, 'suffix': '%'},
    {'category': '💰 籌碼面', 'key': 'foreign_hold_pct', 'label': '外資持股(%)', 'min': 0, 'max': 100, 'step': 0.1, 'suffix': '%'},
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
    "💰 籌碼與主力": ["主力掃單(ILSS)", "投信認養", "散戶退場", "土洋對作(投勝)", "土洋對作(外勝)"],
    "⚠️ 特殊型態": ["30W臨門一腳", "30W黏貼後突破", "30W甩轎", "假跌破", "回測季線", "回測年線", "Vix反轉"]
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
    '30W臨門一腳': '【聽牌】30W均線走平或向上，股價在均線 -9%~+8% 內(一根漲停可站回)且近週漲幅未達10%。具備近4週極致收斂或甩轎未逾8週的準備型態',
    '30W黏貼後突破': 'MA30 走平且股價在均線附近震盪沉澱後，首度帶量突破起漲',
    '極度壓縮': '布林通道寬度小於 5%，呈現極致的籌碼沉澱與無波動狀態',
    '波動壓縮': '布林通道寬度小於 8%，波動率正在收斂',
    '盤整5日': '布林寬度近 5 日皆處於收斂狀態',
    '盤整10日': '布林寬度近 10 日皆處於收斂狀態',
    '盤整20日': '布林寬度近 20 日皆處於收斂狀態',
    '盤整60日': '布林寬度長達 60 日皆處於收斂狀態 (大底成型)',
    '主力掃單(ILSS)': '[嚴格] MA200上 + 假跌破掃單 + 爆量站回 + 營收成長 + 融資退場',
    '投信認養': '[嚴格] 股本<50萬張 + 持股1~8% + 5日內買超3日 + 單日買盤佔成交量>10%',
    '散戶退場': '融資今日單日大減超過 200 張',
    '土洋對作(投勝)': '外資近10日或20日賣超，投信波段承接(達賣壓30%以上)，且近5日股價已點火上漲',
    '土洋對作(外勝)': '投信近10日或20日賣超，外資波段承接(達賣壓30%以上)，且近5日股價已點火上漲',
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
        layout.setContentsMargins(0, 2, 0, 2)

        self.lbl_name = QLabel(config['label'])
        self.lbl_name.setStyleSheet("color: #DDD; font-size: 14px; border:none;")
        # 🚀 寬度由 115 增加至 140，確保長文字如「30W起漲週數(前)」不被遮住
        self.lbl_name.setFixedWidth(140)
        layout.addWidget(self.lbl_name)

        self.spin_min = QDoubleSpinBox()
        self.setup_spin(self.spin_min, config['min'], config['suffix'])
        self.spin_min.setMinimumWidth(90) # 🚀 確保輸入框夠寬
        layout.addWidget(self.spin_min)

        lbl_tilde = QLabel("~")
        lbl_tilde.setStyleSheet("color:#555; border:none;")
        lbl_tilde.setFixedWidth(12)
        lbl_tilde.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(lbl_tilde)

        self.spin_max = QDoubleSpinBox()
        self.setup_spin(self.spin_max, config['max'], config['suffix'])
        self.spin_max.setMinimumWidth(90) # 🚀 確保輸入框夠寬
        layout.addWidget(self.spin_max)
        layout.addStretch()

    def setup_spin(self, spin, default_val, suffix):
        spin.setRange(-999999999, 999999999)
        spin.setDecimals(1 if '%' in suffix or 'RS' in self.config['label'] else 0)
        spin.setSingleStep(self.config['step'])
        spin.setSuffix(suffix)
        spin.setValue(default_val)

        # 🚀 優化 1：關閉鍵盤追蹤。
        # 當使用者在框內「打字」時，不會每輸入一個字就觸發篩選，
        # 而是在使用者按 Enter 或點擊滑鼠離開輸入框後才執行，大幅減少 Hang。
        spin.setKeyboardTracking(False)

        spin.valueChanged.connect(self.emit_change)

    def emit_change(self):
        self.value_changed.emit()

    def is_modified(self):
        return (abs(self.spin_min.value() - self.config['min']) > 0.001) or (
                abs(self.spin_max.value() - self.config['max']) > 0.001)

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
        if index.column() == 0:
            sid = str(self._df.iloc[index.row()].get('sid', ''))
            if role == Qt.ItemDataRole.CheckStateRole:
                return Qt.CheckState.Checked if sid in self.checked_sids else Qt.CheckState.Unchecked
            return None

        col_key = self.visible_cols[index.column() - 1]
        value = self._df.iloc[index.row()][col_key]

        if col_key == 'insight' and role == Qt.ItemDataRole.DisplayRole: return "🔍"
        if role == Qt.ItemDataRole.UserRole: return value

        if role == Qt.ItemDataRole.DisplayRole:
            if isinstance(value, (int, float)):
                if col_key == '現價': return f"{value:,.2f}".rstrip('0').rstrip('.')
                if col_key in ['RS強度', 'pe', 'pbr', '量比', 'fund_eps_cum']: return f"{value:.1f}"
                if any(x in col_key for x in ['漲幅', 'yield', 'width', 'yoy', 'qoq', 'diff']): return f"{value:.2f}%"
                if any(x in col_key for x in ['sum', 'net', 'cash_flow']): return f"{value:,.0f}"
                if any(x in col_key for x in ['streak', 'offset']): return f"{int(value)}"
                return f"{value:,.2f}"
            return str(value)

        # ==========================================
        # 🚀 漲跌停板計算規則 (台股漲跌幅限制 10%，因升降單位取 >= 9.85% 最準確)
        # ==========================================
        pct_1d = self._df.iloc[index.row()].get('漲幅1d', 0)
        is_limit_up = (pct_1d >= 9.85)
        is_limit_down = (pct_1d <= -9.85)

        # 🚀 字體顏色 (ForegroundRole)
        if role == Qt.ItemDataRole.ForegroundRole:
            # 漲跌停亮燈時，為了配深色底，字體強制為「純白色」
            if (is_limit_up or is_limit_down) and col_key in ['現價', '漲幅1d']:
                return QColor("#FFFFFF")

            # 一般紅綠字體邏輯
            if col_key == '現價':
                if pct_1d > 0: return QColor("#FF4444")
                if pct_1d < 0: return QColor("#00CC00")
            if isinstance(value, (int, float)):
                if any(x in col_key for x in ['漲幅', 'sum', 'net', 'yoy', 'eps', 'streak', 'qoq', 'diff', 'cash_flow']):
                    if value > 0: return QColor("#FF4444")
                    if value < 0: return QColor("#00CC00")
            return QColor("#E0E0E0")

        # 🚀 背景顏色 (BackgroundRole) - 製作亮燈底色
        if role == Qt.ItemDataRole.BackgroundRole:
            # 漲停板：漂亮不刺眼的「實心暗紅色」
            if is_limit_up and col_key in ['現價', '漲幅1d']:
                return QColor("#D32F2F")
            # 跌停板：漂亮不刺眼的「實心暗綠色」
            if is_limit_down and col_key in ['現價', '漲幅1d']:
                return QColor("#2E7D32")

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
    def sort(self, column, order):
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        try:
            self.setDynamicSortFilter(False)
            super().sort(column, order)
        finally:
            self.setDynamicSortFilter(True)
            QApplication.restoreOverrideCursor()

    def lessThan(self, left, right):
        source = self.sourceModel()
        l_row, r_row = left.row(), right.row()

        # 🚀 極速優化：使用 .values (Numpy Array) 直接讀取記憶體，避開 .iloc 造成的龐大物件生成負擔
        if left.column() == 0:
            sids = source._df['sid'].values
            l_checked = str(sids[l_row]) in source.checked_sids
            r_checked = str(sids[r_row]) in source.checked_sids
            return bool(l_checked < r_checked)

        col_key = source.visible_cols[left.column() - 1]
        vals = source._df[col_key].values

        l_val = vals[l_row]
        r_val = vals[r_row]

        if pd.isna(l_val): l_val = -999999
        if pd.isna(r_val): r_val = -999999

        try:
            return bool(float(l_val) < float(r_val))
        except (ValueError, TypeError):
            return bool(str(l_val) < str(r_val))


class ZeroWidthVerticalHeader(QHeaderView):
    """最高階防禦：物理抹除寬度，強制回傳 QSize(0, 0)，徹底粉碎黑邊與拖拉熱區"""

    def __init__(self, parent=None):
        super().__init__(Qt.Orientation.Vertical, parent)
        self.setHidden(True)
        self.setDefaultSectionSize(38)  # 維持舒適的列高 38px

    def sizeHint(self):
        # ☢️ 核心殺招：不管 Qt 怎麼算，寬度永遠是 0！
        return QSize(0, 0)


class FrozenTableView(QTableView):
    """終極完美版：徹底消滅黑邊 + 強化 Checkbox 視覺框線"""

    def __init__(self, model, parent=None):
        super().__init__(parent)
        self.setModel(model)

        # 🚀 1. 替換為我們自訂的「絕對零寬度」標頭
        self.setVerticalHeader(ZeroWidthVerticalHeader(self))

        self.setSortingEnabled(True)
        self.horizontalHeader().setSortIndicatorShown(True)
        self.horizontalHeader().setHighlightSections(False)

        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)

        # 🌟 Checkbox SVG 黑勾勾圖示
        b64_check = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxNiIgaGVpZ2h0PSIxNiIgdmlld0JveD0iMCAwIDI0IDI0IiBmaWxsPSJub25lIiBzdHJva2U9IiMwMDAwMDAiIHN0cm9rZS13aWR0aD0iMyIgc3Ryb2tlLWxpbmVjYXA9InJvdW5kIiBzdHJva2UtbGluZWpvaW49InJvdW5kIj48cG9seWxpbmUgcG9pbnRzPSIyMCA2IDkgMTcgNCAxMiI+PC9wb2x5bGluZT48L3N2Zz4="
        b64_down = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxNiIgaGVpZ2h0PSIxNiI+PHBhdGggZD0iTSAzIDYgaCAxMCBsIC01IDYgeiIgZmlsbD0iIzAwRTVGRiIvPjwvc3ZnPg=="
        b64_up = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxNiIgaGVpZ2h0PSIxNiI+PHBhdGggZD0iTSAzIDEwIGggMTAgbCAtNSAtNiB6IiBmaWxsPSIjMDBFNUZGIi8+PC9zdmc+"

        # 🚀 主表樣式 (強化 Checkbox 方框)
        self.setStyleSheet(f"""
            QTableView {{ background-color: #000; gridline-color: #222; border: none; }}
            QTableView::item:selected {{ background-color: #111; color: #EEE; }} 
            QTableView:focus {{ outline: none; }}

            /* 🌟 超清晰 Checkbox 方框設計 */
            QTableView::indicator {{
                width: 20px; 
                height: 20px; 
                border: 2px solid #666; 
                border-radius: 4px; 
                background-color: #1A1A1A;
                margin-left: 2px;
            }}
            QTableView::indicator:hover {{ border: 2px solid #00E5FF; background-color: #222; }}
            QTableView::indicator:checked {{
                background-color: #00E5FF; 
                border: 2px solid #00E5FF; 
                image: url("{b64_check}");
            }}
        """)

        self.horizontalHeader().setStyleSheet(f"""
            QHeaderView::section {{ 
                background-color: #111; color: #FFF; font-weight: bold; 
                border: 1px solid #333; padding-left: 4px; padding-right: 18px; 
            }}
            QHeaderView::down-arrow {{ width: 14px; height: 14px; subcontrol-position: right center; right: 2px; image: url("{b64_down}"); }}
            QHeaderView::up-arrow {{ width: 14px; height: 14px; subcontrol-position: right center; right: 2px; image: url("{b64_up}"); }}
        """)

        # 🚀 2. 建立凍結層
        self.frozen_view = QTableView(self)
        self.init_frozen_view()

        # 事件連動
        self.horizontalHeader().sectionResized.connect(self.update_section_width)
        self.verticalHeader().sectionResized.connect(self.update_section_height)
        self.frozen_view.verticalScrollBar().valueChanged.connect(self.verticalScrollBar().setValue)
        self.verticalScrollBar().valueChanged.connect(self.frozen_view.verticalScrollBar().setValue)
        self.horizontalHeader().sortIndicatorChanged.connect(self.sync_sort_indicator_to_frozen)
        self.frozen_view.horizontalHeader().sortIndicatorChanged.connect(self.sync_sort_indicator_to_main)

    def sync_sort_indicator_to_frozen(self, logicalIndex, order):
        self.frozen_view.horizontalHeader().blockSignals(True)
        self.frozen_view.horizontalHeader().setSortIndicator(logicalIndex, order)
        self.frozen_view.horizontalHeader().blockSignals(False)

    def sync_sort_indicator_to_main(self, logicalIndex, order):
        self.horizontalHeader().blockSignals(True)
        self.horizontalHeader().setSortIndicator(logicalIndex, order)
        self.horizontalHeader().blockSignals(False)

    def init_frozen_view(self):
        self.frozen_view.setModel(self.model())

        # 🚀 凍結層同樣套用「絕對零寬度」標頭
        self.frozen_view.setVerticalHeader(ZeroWidthVerticalHeader(self.frozen_view))

        self.frozen_view.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.frozen_view.setSortingEnabled(True)
        self.frozen_view.horizontalHeader().setSortIndicatorShown(True)
        self.frozen_view.horizontalHeader().setHighlightSections(False)
        self.frozen_view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.frozen_view.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)

        b64_check = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxNiIgaGVpZ2h0PSIxNiIgdmlld0JveD0iMCAwIDI0IDI0IiBmaWxsPSJub25lIiBzdHJva2U9IiMwMDAwMDAiIHN0cm9rZS13aWR0aD0iMyIgc3Ryb2tlLWxpbmVjYXA9InJvdW5kIiBzdHJva2UtbGluZWpvaW49InJvdW5kIj48cG9seWxpbmUgcG9pbnRzPSIyMCA2IDkgMTcgNCAxMiI+PC9wb2x5bGluZT48L3N2Zz4="
        self.frozen_view.setStyleSheet(f"""
            QTableView {{ background-color: #000; border-right: 2px solid #00E5FF; gridline-color: #222; }}
            QTableView::item:selected {{ background-color: #111; color: #EEE; }}
            QHeaderView::section {{ background-color: #111; color: #FFF; font-weight: bold; border: 1px solid #333; padding-left: 4px; padding-right: 18px; }}

            /* 🌟 凍結層超清晰 Checkbox */
            QTableView::indicator {{
                width: 20px; height: 20px; border: 2px solid #666; border-radius: 4px; background-color: #1A1A1A; margin-left: 2px;
            }}
            QTableView::indicator:hover {{ border: 2px solid #00E5FF; background-color: #222; }}
            QTableView::indicator:checked {{
                background-color: #00E5FF; border: 2px solid #00E5FF; image: url("{b64_check}");
            }}
        """)

        self.frozen_view.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.frozen_view.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.frozen_view.show()

        self.update_frozen_columns()
        self.update_frozen_geometry()

    def update_frozen_columns(self):
        for i in range(self.model().columnCount()):
            self.frozen_view.setColumnHidden(i, i >= 4)

    def update_section_width(self, logicalIndex, oldSize, newSize):
        if logicalIndex < 4:
            self.frozen_view.setColumnWidth(logicalIndex, newSize)
            self.update_frozen_geometry()

    def update_section_height(self, logicalIndex, oldSize, newSize):
        self.frozen_view.setRowHeight(logicalIndex, newSize)

    def update_frozen_geometry(self):
        if not hasattr(self, 'frozen_view') or self.frozen_view is None:
            return

        width = 0
        for i in range(4):
            if not self.isColumnHidden(i):
                width += self.columnWidth(i)

        self.frozen_view.setGeometry(self.frameWidth(), self.frameWidth(),
                                     width, self.viewport().height() + self.horizontalHeader().height())

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self.update_frozen_geometry()

    def updateGeometries(self):
        super().updateGeometries()
        if hasattr(self, 'frozen_view'):
            self.update_frozen_geometry()

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

        # === 1. 加入劇本狀態與全局清除按鈕 (插入在 tab_widget 之前) ===
        status_layout = QHBoxLayout()
        status_layout.setContentsMargins(0, 0, 0, 5)

        self.lbl_active_preset = QLabel("當前狀態：預設過濾")
        self.lbl_active_preset.setStyleSheet("""
                            color: #FFD700; 
                            background-color: #1A1A1A; 
                            padding: 8px; 
                            border: 1px solid #333;
                            border-radius: 4px; 
                            font-size: 14px; 
                            font-weight: bold;
                        """)

        # 🌟 全局共用：清除條件按鈕 (配上警示色的暗黑風格)
        self.btn_reset = QPushButton("🧹 清除條件")
        self.btn_reset.setFixedSize(100, 36)
        self.btn_reset.setStyleSheet("""
                    QPushButton { background-color: #331111; color: #FF6666; border: 1px solid #552222; border-radius: 4px; font-weight: bold; font-size: 14px;}
                    QPushButton:hover { background-color: #551111; color: #FF9999; border: 1px solid #FF4444;}
                """)
        self.btn_reset.clicked.connect(self.reset_filters)

        status_layout.addWidget(self.lbl_active_preset)
        status_layout.addStretch()
        status_layout.addWidget(self.btn_reset)

        ctrl_layout.addLayout(status_layout)
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
        # === 統一後的下拉選單樣式變數 (確保復用) ===
        editable_combo_style = """
                    QComboBox { background: #111; color: #FFF; border: 1px solid #444; padding: 4px; font-size: 14px; }
                    QComboBox::drop-down { border-left: 1px solid #444; width: 24px; background: #222; }
                    QComboBox::drop-down:hover { background: #333; }
                    QComboBox::down-arrow { width: 0px; height: 0px; border-left: 5px solid transparent; border-right: 5px solid transparent; border-top: 6px solid #00E5FF; }
                    QComboBox QAbstractItemView { background: #111; color: #FFF; selection-background-color: #00E5FF; selection-color: #000; outline: none; border: 1px solid #444; }
                """
        completer_style = """
                    QListView { background-color: #1A1A1A; color: #00E5FF; border: 1px solid #00E5FF; font-size: 15px; font-weight: bold; }
                    QListView::item { padding: 8px; background-color: #111; color: #EEE; }
                    QListView::item:selected { background-color: #0066CC; color: #FFF; }
                """

        lbl_ind = QLabel("📂 基本/自選 (可打字搜尋)");
        lbl_ind.setProperty("class", "category-label")

        # 🚀 修正：將 combo_industry 改為可編輯並套用樣式
        self.combo_industry = QComboBox()
        self.combo_industry.setEditable(True)
        self.combo_industry.setStyleSheet(editable_combo_style)
        self.combo_industry.lineEdit().setPlaceholderText("選擇產業或自選清單...")

        # 套用智慧搜尋器 (解決卡頓)
        comp_ind = QCompleter(self.combo_industry.model(), self.combo_industry)
        comp_ind.setFilterMode(Qt.MatchFlag.MatchContains)
        comp_ind.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        comp_ind.popup().setStyleSheet(completer_style)
        self.combo_industry.setCompleter(comp_ind)

        self.combo_industry.activated.connect(self.apply_filters_debounce)
        # 防止打字按 Enter 時沒反應
        self.combo_industry.lineEdit().returnPressed.connect(self.apply_filters_debounce)

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

        # 僅保留「設定欄位」按鈕靠右對齊
        self.btn_filter_setting = QToolButton()
        self.btn_filter_setting.setText("⚙️ 設定欄位")
        self.btn_filter_setting.clicked.connect(self.open_filter_setting)

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
            ("🎯 30W 臨門一腳 (極致壓縮)","中長線打底具備『30W聽牌』型態，且今日日線布林寬度壓縮 < 8%、成交量極度萎縮 < 0.8倍，處於隨時點火發動的窒息量狀態。"),
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
        self.model = StrategyTableModel()
        self.proxy_model = NumericSortProxy()
        self.proxy_model.setSourceModel(self.model)

        # 🚀 這裡換成 FrozenTableView (固定欄位版本)
        self.table_view = FrozenTableView(self.proxy_model)
        self.table_view.setModel(self.proxy_model)

        # 連接勾選狀態改變的事件，即時更新頂部指揮列
        self.model.dataChanged.connect(self.update_ai_command_bar)

        self.table_view.doubleClicked.connect(self.on_table_double_clicked)
        self.table_view.clicked.connect(self.on_table_clicked)
        self.table_view.frozen_view.doubleClicked.connect(self.on_table_double_clicked)
        self.table_view.frozen_view.clicked.connect(self.on_table_clicked)
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
        # 🚀 修正 Bug：打開設定前，先備份當前畫面上所有的數值設定 (包含劇本自動帶入的數值)
        current_values = {}
        for w in self.dynamic_filters:
            current_values[w.key] = {
                'min': w.spin_min.value(),
                'max': w.spin_max.value()
            }

        dlg = FilterSelectionDialog(FULL_FILTER_SPECS, self.active_filter_keys, self)
        if dlg.exec():
            # 更新要顯示的欄位名單
            self.active_filter_keys = dlg.get_selected_keys()

            # 重新建立 UI (此時所有的滑桿都會變成預設值)
            self.rebuild_filter_ui()

            # 🚀 修正 Bug：將剛剛備份的數值「回填」回去（如果該欄位還在顯示名單中的話）
            for w in self.dynamic_filters:
                if w.key in current_values:
                    w.spin_min.blockSignals(True)
                    w.spin_max.blockSignals(True)
                    w.spin_min.setValue(current_values[w.key]['min'])
                    w.spin_max.setValue(current_values[w.key]['max'])
                    w.spin_min.blockSignals(False)
                    w.spin_max.blockSignals(False)

            # 最後套用真實的資料過濾
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

        # 🚀 重新套用搜尋器，確保項目更新後打字搜尋依然流暢
        if self.combo_industry.completer():
            self.combo_industry.completer().setModel(self.combo_industry.model())

        if curr: self.combo_industry.setCurrentText(curr)
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
                # 🚀 關鍵修復：必須先立即隱藏並解除父節點，再進行記憶體刪除
                # 否則新的元件加進來時，舊的還在畫面上，就會全部擠成一團並引發嚴重卡頓！
                widget.hide()
                widget.setParent(None)
                widget.deleteLater()
            else:
                sub_layout = item.layout()
                if sub_layout:
                    self.clear_layout(sub_layout)

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
        """套用戰略劇本：自動帶入參數"""
        self.lbl_active_preset.setText(f"🔥 當前劇本：{preset_name}")

        # 🚀 關鍵修正：先清空舊的「額外欄位」，回到預設狀態，避免欄位越加越多
        self.active_filter_keys = DEFAULT_ACTIVE_FILTERS.copy()

        rules = {}
        target_tags = []

        if "暗渡陳倉" in preset_name:
            rules = {"漲幅20d": {"min": -5, "max": 8}, "bb_width": {"max": 12}, "legal_diff_20d": {"min": 1.5}}
        elif "春江水暖" in preset_name:
            rules = {"fund_contract_qoq": {"min": 15}, "fund_op_cash_flow": {"min": 0}}
        elif "破底翻黃金坑" in preset_name:
            rules = {"m_sum_5d": {"max": -300}}
            target_tags = ["假跌破", "30W甩轎"]
        elif "30W 臨門一腳" in preset_name:
            # 只要布林帶壓縮 < 8，且標籤命中「聽牌」即可
            rules = {"bb_width": {"max": 8}, "量比": {"max": 0.8}}
            target_tags = ["30W臨門一腳"]

        # 🚀 確保劇本需要的欄位被加入顯示名單中
        for k in rules.keys():
            if k not in self.active_filter_keys:
                self.active_filter_keys.append(k)

        # 重建數值濾網 UI (這樣就會只顯示預設 + 此劇本相關的欄位)
        self.rebuild_filter_ui()

        # 填入數值
        for w in self.dynamic_filters:
            if w.key in rules:
                if "min" in rules[w.key]: w.spin_min.setValue(rules[w.key]["min"])
                if "max" in rules[w.key]: w.spin_max.setValue(rules[w.key]["max"])

        # 清除舊標籤並打勾新標籤
        for chk in self.chk_tags.values(): chk.setChecked(False)
        for tag in target_tags:
            if tag in self.chk_tags: self.chk_tags[tag].setChecked(True)

        self.tab_widget.setCurrentIndex(1)  # 切換到數值分頁
        self.apply_filters_debounce()

    def reset_filters(self):
        """完全清空篩選條件，回復預設"""
        self.lbl_active_preset.setText("當前狀態：預設過濾")

        # 🚀 效能優化 1：暫停下拉選單與搜尋框的訊號，避免清空時觸發不必要的過濾
        for combo in [self.combo_industry, self.combo_concept, self.combo_dj_ind]:
            combo.blockSignals(True)
            combo.setCurrentIndex(0)
            combo.blockSignals(False)

        self.txt_search.blockSignals(True)
        self.txt_search.clear()
        self.txt_search.blockSignals(False)

        # 回到預設的 6 組欄位
        self.active_filter_keys = DEFAULT_ACTIVE_FILTERS.copy()

        # 重建數值 UI
        self.rebuild_filter_ui()

        # 🚀 效能優化 2 (關鍵)：阻擋 Checkbox 訊號，避免瞬間發出幾十次重新運算事件導致卡頓 6~8 秒
        for chk in self.chk_tags.values():
            chk.blockSignals(True)
            chk.setChecked(False)
            chk.blockSignals(False)

        self.rb_and.blockSignals(True)
        self.rb_and.setChecked(True)
        self.rb_and.blockSignals(False)

        # 統一由這裡乾淨俐落地執行一次實質過濾
        self.apply_filters_real()

    def apply_filters_debounce(self):
        self.debounce_timer.start()

    def on_search_triggered(self):
        self.apply_filters_real()

    def apply_filters_real(self):
        if self.full_df.empty: return
        # 🚀 優化 2：讓系統有時間處理「旋轉等待」或「按鈕點擊」的繪製
        QApplication.processEvents()

        self.setUpdatesEnabled(False)  # 暫停畫面重繪
        try:
            df = self.full_df.copy()
            df['insight'] = ''

            # A. 基本搜尋
            search_txt = self.txt_search.text().strip()
            if search_txt:
                mask = df['sid'].str.contains(search_txt, case=False, na=False) | \
                       df['name'].str.contains(search_txt, case=False, na=False)
                df = df[mask]

            # B. 產業/自選/概念/細產業 (🚀 修復下拉選單無反應問題)
            ind = self.combo_industry.currentText()
            if ind.startswith("[自選] "):
                group_name = ind.replace("[自選] ", "")
                df = df[df['sid'].isin(self.watchlist_data.get(group_name, []))]
            elif ind != "全部" and ind != "":
                df = df[df['industry'] == ind]

            concept = self.combo_concept.currentText().strip()
            if concept and concept != "全部" and 'sub_concepts' in df.columns:
                # 確保欄位不是 NaN，並且進行字串模糊比對
                df = df[df['sub_concepts'].fillna("").str.contains(concept, regex=False)]
                print(f"👉 [驗證] 套用概念題材 '{concept}'，剩餘檔數: {len(df)}")

            dj_ind = self.combo_dj_ind.currentText().strip()
            if dj_ind and dj_ind != "全部" and 'dj_sub_ind' in df.columns:
                # 下拉清單格式為 "[主業] 細產業"，我們提取出 "細產業" 來做比對
                sub_ind_target = dj_ind.split("] ")[-1] if "] " in dj_ind else dj_ind
                df = df[df['dj_sub_ind'].fillna("").str.contains(sub_ind_target, regex=False)]
                print(f"👉 [驗證] 套用細產業 '{sub_ind_target}'，剩餘檔數: {len(df)}")

            # C. 數值過濾 🚀 (核心修正：僅過濾「有被更動」的滑桿)
            is_numeric_dirty = False
            for w in self.dynamic_filters:
                if w.is_modified():
                    is_numeric_dirty = True
                    df = df[(df[w.key] >= w.spin_min.value()) & (df[w.key] <= w.spin_max.value())]

            # D. 特徵標籤
            selected_tags = [t for t, chk in self.chk_tags.items() if chk.isChecked()]
            if selected_tags and '強勢特徵' in df.columns:
                df['強勢特徵'] = df['強勢特徵'].fillna("")
                import re  # 確保有載入正則套件
                if self.rb_and.isChecked():
                    # 加入 regex=False，強迫 Pandas 當作純文字比對，忽略括號的特殊意義
                    for tag in selected_tags:
                        df = df[df['強勢特徵'].str.contains(tag, regex=False, na=False)]
                else:
                    # 在 OR 聯集模式下，使用 re.escape 把括號跳脫 (變成 \(\))
                    pattern = "|".join([re.escape(str(t)) for t in selected_tags])
                    df = df[df['強勢特徵'].str.contains(pattern, regex=True, na=False)]

            # 整理欄位顯示 (🚀 關鍵修復：強制將 詳、代號、名稱 永遠鎖死在最前面)
            visible_cols = []
            if FULL_COLUMN_SPECS.get('insight', {}).get('show', True): visible_cols.append('insight')
            if 'sid' in df.columns: visible_cols.append('sid')
            if 'name' in df.columns: visible_cols.append('name')

            for key in self.column_order:
                # 扣除已經在最前面的三個欄位，剩下的才依序加入
                if key not in ['insight', 'sid', 'name'] and FULL_COLUMN_SPECS.get(key, {}).get('show'):
                    if key in df.columns: visible_cols.append(key)

            self.display_df = df[visible_cols].copy()
            self.model.update_data(self.display_df, visible_cols)
            self.proxy_model.invalidate()
            self.lbl_status.setText(f"篩選結果: {len(self.display_df)} 檔")
            self.update_ai_command_bar()

            # 🚀 優化 3：僅在結果數量發生變化時，才考慮調整寬度，節省佈局計算成本
            if not hasattr(self, '_last_count') or self._last_count != len(self.display_df):
                QTimer.singleShot(1, self.adjust_table_layout)
                self._last_count = len(self.display_df)

        finally:
            self.setUpdatesEnabled(True)

    def adjust_table_layout(self):
        """處理表格寬度"""
        if not hasattr(self, 'table_view'): return

        # 🚀 稍微加寬，配合 38px 欄高更順眼
        self.table_view.setColumnWidth(0, 45)  # 選
        self.table_view.setColumnWidth(1, 45)  # 詳
        self.table_view.setColumnWidth(2, 85)  # 代號
        self.table_view.setColumnWidth(3, 115) # 名稱

        # 僅針對特定欄位進行寬度設定
        for i, col in enumerate(self.model.visible_cols):
            if col == '強勢特徵':
                self.table_view.setColumnWidth(i + 1, 220)
                break

        if hasattr(self.table_view, 'update_frozen_geometry'):
            self.table_view.update_frozen_geometry()

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
        self.model.checked_sids = set(self.display_df['sid'].astype(str))

        top_left = self.model.index(0, 0)
        bottom_right = self.model.index(self.model.rowCount() - 1, 0)
        self.model.dataChanged.emit(top_left, bottom_right, [Qt.ItemDataRole.CheckStateRole])
        self.update_ai_command_bar()

        # 🔥 加上這行防禦：全選後強制校準凍結層佈局
        if hasattr(self, 'table_view'): self.table_view.update_frozen_geometry()

    def deselect_all_rows(self):
        """一鍵清除所有打勾狀態"""
        self.model.checked_sids.clear()

        if self.display_df.empty: return
        top_left = self.model.index(0, 0)
        bottom_right = self.model.index(self.model.rowCount() - 1, 0)
        self.model.dataChanged.emit(top_left, bottom_right, [Qt.ItemDataRole.CheckStateRole])
        self.update_ai_command_bar()

        # 🔥 加上這行防禦：全不選後強制校準凍結層佈局
        if hasattr(self, 'table_view'): self.table_view.update_frozen_geometry()

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