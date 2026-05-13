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
                             QPushButton, QStackedWidget, QScrollArea, QGridLayout,
                             QSizePolicy)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QColor, QFont

import matplotlib
matplotlib.use('QtAgg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

plt.style.use('dark_background')
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# ─── 全域色系 ────────────────────────────────────────────
BG_DEEP  = "#080B10"
BG_PANEL = "#0D1117"
BG_CARD  = "#111820"
BG_ROW   = "#0C1420"
BORDER   = "#1A2535"
ACCENT   = "#00D4FF"
GOLD     = "#F5C518"
GREEN    = "#00E676"
RED      = "#FF3D57"
PURPLE   = "#A78BFA"
TEXT_PRI = "#E2E8F0"
TEXT_SEC = "#7C8FA3"
TEXT_DIM = "#3A4A5C"

COMBO_STYLE = f"""
    QComboBox {{
        background: {BG_CARD}; color: {ACCENT};
        padding: 6px 12px; font-size: 13px; font-weight: bold;
        border: 1px solid {BORDER}; border-radius: 6px;
    }}
    QComboBox::drop-down {{ border: none; width: 20px; }}
    QComboBox QAbstractItemView {{
        background: {BG_CARD}; color: {TEXT_PRI};
        selection-background-color: #162030;
        border: 1px solid {BORDER};
    }}
"""

TABLE_STYLE = f"""
    QTableWidget {{
        background: {BG_PANEL}; border: none;
        gridline-color: {BG_CARD}; font-size: 13px;
        selection-background-color: transparent; outline: none;
    }}
    QTableWidget::item {{
        padding: 5px 8px; border-bottom: 1px solid {BG_CARD};
    }}
    QTableWidget::item:selected {{
        background: #12243A; color: {ACCENT};
    }}
    QTableWidget::item:hover {{ background: #0A1826; }}
    QHeaderView::section {{
        background: #09111C; color: {TEXT_PRI};
        border: none; border-bottom: 1px solid {BORDER};
        padding: 5px 8px; font-size: 12px; font-weight: bold;
        letter-spacing: 1px;
    }}
"""

PANEL_STYLE = f"background: {BG_PANEL}; border-radius: 8px; border: 1px solid {BORDER};"


def panel(style=None):
    f = QFrame()
    f.setStyleSheet(f"QFrame {{ {style or PANEL_STYLE} }}")
    return f


def section_title(text, color=TEXT_PRI, size=13):
    lbl = QLabel(text)
    lbl.setStyleSheet(f"color: {color}; font-size: {size}px; font-weight: bold; "
                      f"padding: 0; background: transparent; border: none;")
    return lbl


def dim_label(text, size=11):
    lbl = QLabel(text)
    lbl.setStyleSheet(f"color: {TEXT_SEC}; font-size: {size}px; background: transparent; border: none;")
    return lbl


def setup_table(table, stretch_col=None, row_height=26):
    table.verticalHeader().setVisible(False)
    table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
    table.setShowGrid(False)
    table.setStyleSheet(TABLE_STYLE)
    table.setAlternatingRowColors(True)
    table.setStyleSheet(TABLE_STYLE + f"QTableWidget {{ alternate-background-color: {BG_ROW}; }}")
    table.verticalHeader().setDefaultSectionSize(row_height)
    if stretch_col is not None:
        h = table.horizontalHeader()
        for i in range(table.columnCount()):
            mode = QHeaderView.ResizeMode.Stretch if i == stretch_col else QHeaderView.ResizeMode.ResizeToContents
            h.setSectionResizeMode(i, mode)


def titem(text, fg=TEXT_PRI, align=Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, bold=False):
    it = QTableWidgetItem(str(text))
    it.setForeground(QColor(fg))
    it.setTextAlignment(align)
    if bold:
        f = QFont(); f.setBold(True); it.setFont(f)
    return it


# ─── 數據線程 ────────────────────────────────────────────

class ETFDataWorker(QThread):
    data_fetched = pyqtSignal(pd.DataFrame, str)

    def __init__(self, etf_id, provider):
        super().__init__()
        self.etf_id = etf_id
        self.provider = provider

    def run(self):
        url = f"https://raw.githubusercontent.com/phanchang/stock-room-data/main/data/clean/{self.provider}/{self.etf_id}.csv"
        lp = Path(f"data/clean/{self.provider}/{self.etf_id}.csv")
        df = pd.DataFrame()
        try:
            r = requests.get(url, timeout=10)
            df = pd.read_csv(StringIO(r.text)) if r.status_code == 200 else (pd.read_csv(lp) if lp.exists() else df)
        except:
            if lp.exists(): df = pd.read_csv(lp)
        self.data_fetched.emit(self._norm(df) if not df.empty else df, self.etf_id)

    def _norm(self, df):
        df.columns = [c.lower().strip() for c in df.columns]
        rm = {}
        if 'stock_code' in df.columns: rm['stock_code'] = 'stock_id'
        if 'code' in df.columns: rm['code'] = 'stock_id'
        if 'stock_name' in df.columns: rm['stock_name'] = 'name'
        if rm: df.rename(columns=rm, inplace=True)
        for col in ['shares', 'weight']:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col].str.replace(',', '').str.replace('%', ''), errors='coerce')
        if 'stock_id' in df.columns:
            df['stock_id'] = df['stock_id'].astype(str).str.replace('.TW', '', regex=False).str.replace('.TWO', '',regex=False).str.strip()
        return df


class MultiETFDataWorker(QThread):
    multi_data_fetched = pyqtSignal(pd.DataFrame)

    def __init__(self, mapping):
        super().__init__()
        self.mapping = mapping

    def run(self):
        all_data = []
        for etf_id, (provider, _) in self.mapping.items():
            url = f"https://raw.githubusercontent.com/phanchang/stock-room-data/main/data/clean/{provider}/{etf_id}.csv"
            lp = Path(f"data/clean/{provider}/{etf_id}.csv")
            df = pd.DataFrame()
            try:
                r = requests.get(url, timeout=5)
                df = pd.read_csv(StringIO(r.text)) if r.status_code == 200 else (pd.read_csv(lp) if lp.exists() else df)
            except:
                if lp.exists(): df = pd.read_csv(lp)
            if not df.empty:
                df.columns = [c.lower().strip() for c in df.columns]
                rm = {}
                if 'stock_code' in df.columns: rm['stock_code'] = 'stock_id'
                if 'code' in df.columns: rm['code'] = 'stock_id'
                if 'stock_name' in df.columns: rm['stock_name'] = 'name'
                if rm: df.rename(columns=rm, inplace=True)
                for col in ['shares', 'weight']:
                    if col in df.columns and df[col].dtype == 'object':
                        df[col] = pd.to_numeric(df[col].str.replace(',', '').str.replace('%', ''), errors='coerce')
                df['stock_id'] = df['stock_id'].astype(str)
                df['etf_id'] = etf_id
                if 'date' in df.columns: df['date'] = pd.to_datetime(df['date'])
                all_data.append(df)
        self.multi_data_fetched.emit(pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame())


# ─── 主程式 ──────────────────────────────────────────────

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
        self._style_etf_data = {}   # cache for style page click interaction
        self._style_ind_colors = {}

        self.mapping = {
            "00981A": ("ezmoney", "統一-00981A (統一台股增長)"),
            "00403A": ("ezmoney", "統一-00403A (統一高息優選)"),
            "00991A": ("fhtrust",    "復華-00991A (復華未來50)"),
            "00982A": ("capitalfund","群益-00982A (台灣精選強棒)")
        }

        self.load_market_info()
        self.load_industry_info()
        self.init_ui()
        self.load_multi_etf_data()

    # ─── 輔助載入 ──────────────────────────────────────

    def load_market_info(self):
        p = Path("data/stock_list.csv")
        if not p.exists(): return
        for enc in ['utf-8', 'utf-8-sig', 'big5']:
            try:
                df = pd.read_csv(p, dtype=str, encoding=enc)
                df.columns = [c.lower().strip() for c in df.columns]
                cc = next((c for c in ['stock_id', 'code', 'id'] if c in df.columns), None)
                if cc and 'market' in df.columns:
                    for _, row in df.iterrows():
                        self.stock_market_map[str(row[cc]).strip()] = str(row['market']).strip().upper()
                    break
            except: continue

    def load_industry_info(self):
        p = Path("data/dj_industry.csv")
        if not p.exists():
            print("[警告] 找不到 data/dj_industry.csv")
            return

        # 嘗試不同的編碼格式，避免中文亂碼導致載入失敗
        for enc in ['utf-8', 'utf-8-sig', 'big5']:
            try:
                # 不依賴特定的欄位名稱，直接讀取欄位位置 (第0欄=代號, 第1欄=產業)
                df = pd.read_csv(p, header=None, dtype=str, encoding=enc)
                if not df.empty:
                    # 判斷第一列是否為標題列 (如果第一列第一欄不是數字，則視為標題跳過)
                    first_val = str(df.iloc[0, 0]).strip()
                    start_idx = 1 if not first_val.isnumeric() else 0

                    for i in range(start_idx, len(df)):
                        sid = str(df.iloc[i, 0]).strip()
                        ind = str(df.iloc[i, 1]).strip()
                        if sid:
                            self.industry_map[sid] = ind
                break  # 成功讀取就跳出迴圈
            except Exception:
                continue

    def get_market_suffix(self, sid):
        return self.stock_market_map.get(str(sid), "TW")

    def load_watchlists_to_combo(self):
        self.combo_compare.blockSignals(True)
        self.combo_compare.clear()
        self.combo_compare.addItem("不比對")
        p = Path("data/watchlist.json")
        if p.exists():
            try:
                for key in json.load(open(p, 'r', encoding='utf-8')).keys():
                    self.combo_compare.addItem(key)
            except: pass
        self.combo_compare.blockSignals(False)

    # ─── UI 初始化 ──────────────────────────────────────

    def init_ui(self):
        self.setStyleSheet(f"""
            * {{ font-family: 'Microsoft JhengHei', 'Microsoft YaHei', sans-serif; }}
            QWidget {{ background: {BG_DEEP}; color: {TEXT_PRI}; }}
            QSplitter::handle {{ background: {BORDER}; }}
            QScrollBar:vertical {{ background: {BG_PANEL}; width: 5px; border-radius: 2px; }}
            QScrollBar::handle:vertical {{ background: {BORDER}; border-radius: 2px; min-height: 20px; }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0; }}
        """)

        ml = QVBoxLayout(self)
        ml.setContentsMargins(0, 0, 0, 0)
        ml.setSpacing(0)
        ml.addWidget(self._make_topbar())

        self.tabs = QTabWidget()
        self.tabs.setStyleSheet(f"""
            QTabWidget::pane {{ border: none; background: {BG_DEEP}; }}
            QTabBar {{ background: {BG_PANEL}; }}
            QTabBar::tab {{
                background: {BG_PANEL}; color: {TEXT_DIM};
                padding: 10px 22px; font-size: 13px; font-weight: bold;
                border: none; border-bottom: 2px solid transparent;
            }}
            QTabBar::tab:selected {{ color: {ACCENT}; border-bottom: 2px solid {ACCENT}; background: {BG_PANEL}; }}
            QTabBar::tab:hover:!selected {{ color: {TEXT_SEC}; }}
        """)
        self.tab_single = QWidget()
        self.tab_multi  = QWidget()
        self.tabs.addTab(self.tab_single, "單檔深度解析")
        self.tabs.addTab(self.tab_multi,  "ETF戰情中心")
        ml.addWidget(self.tabs)

        self.init_single_tab()
        self.init_multi_tab()

    def _make_topbar(self):
        bar = QFrame()
        bar.setFixedHeight(42)
        bar.setStyleSheet(f"QFrame {{ background: {BG_PANEL}; border-bottom: 1px solid {BORDER}; }}")
        lay = QHBoxLayout(bar)
        lay.setContentsMargins(16, 0, 16, 0)
        t = QLabel("ETF  主動持股戰情室")
        t.setStyleSheet(f"color: {TEXT_PRI}; font-size: 14px; font-weight: bold; letter-spacing: 2px;")
        s = QLabel("主動型 ETF 籌碼追蹤")
        s.setStyleSheet(f"color: {TEXT_DIM}; font-size: 11px;")
        lay.addWidget(t); lay.addStretch(); lay.addWidget(s)
        return bar

    # ─── 頁一：單檔 ──────────────────────────────────────

    def init_single_tab(self):
        lay = QHBoxLayout(self.tab_single)
        lay.setContentsMargins(8, 8, 8, 8)
        lay.setSpacing(8)

        sp = QSplitter(Qt.Orientation.Horizontal)

        # 左：選ETF + 比對 + 持股表
        left = QFrame()
        left.setStyleSheet(f"QFrame {{ {PANEL_STYLE} }}")
        ll = QVBoxLayout(left)
        ll.setContentsMargins(10, 10, 10, 10)
        ll.setSpacing(6)

        self.combo = QComboBox()
        self.combo.addItems([v[1] for v in self.mapping.values()])
        self.combo.setStyleSheet(COMBO_STYLE)
        self.combo.currentIndexChanged.connect(self.on_combo_change)
        ll.addWidget(self.combo)

        cmp_row = QWidget()
        cr = QHBoxLayout(cmp_row)
        cr.setContentsMargins(0, 0, 0, 0)
        cr.setSpacing(6)
        cr.addWidget(dim_label("比對清單"))
        self.combo_compare = QComboBox()
        self.load_watchlists_to_combo()
        self.combo_compare.setStyleSheet(f"""
            QComboBox {{ background:{BG_CARD}; color:{TEXT_PRI}; padding:4px 8px;
                         font-size:12px; border:1px solid {BORDER}; border-radius:4px; }}
            QComboBox::drop-down {{ border:none; }}
            QComboBox QAbstractItemView {{ background:{BG_CARD}; color:{TEXT_PRI};
                selection-background-color:#162030; border:1px solid {BORDER}; }}
        """)
        self.combo_compare.currentIndexChanged.connect(self.render_table)
        cr.addWidget(self.combo_compare, 1)
        ll.addWidget(cmp_row)

        ll.addWidget(section_title("持股排行", GOLD, 12))

        self.stock_table = QTableWidget()
        self.stock_table.setColumnCount(4)
        self.stock_table.setHorizontalHeaderLabels(["代號", "名稱", "權重", "張數(千)"])
        self.stock_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.stock_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        setup_table(self.stock_table, stretch_col=1)
        self.stock_table.cellClicked.connect(self.on_table_clicked)
        ll.addWidget(self.stock_table)

        # 右：資訊列 + 圖
        right = QFrame()
        right.setStyleSheet(f"QFrame {{ {PANEL_STYLE} }}")
        rl = QVBoxLayout(right)
        rl.setContentsMargins(12, 8, 12, 8)
        rl.setSpacing(6)

        irow = QWidget()
        ir = QHBoxLayout(irow)
        ir.setContentsMargins(0, 4, 0, 4)
        self.lbl_etf_info = section_title("", GOLD, 15)
        self.info_label = QLabel("移動滑鼠查看數據  ·  點擊長條圖連動個股")
        self.info_label.setStyleSheet(
            f"color: {TEXT_PRI}; font-size: 13px; font-weight: bold; background: transparent; border: none;")
        ir.addWidget(self.lbl_etf_info); ir.addStretch(); ir.addWidget(self.info_label)
        rl.addWidget(irow)

        div = QFrame(); div.setFixedHeight(1)
        div.setStyleSheet(f"background: {BORDER};")
        rl.addWidget(div)

        self.fig_change = Figure(facecolor=BG_PANEL)
        self.canvas_change = FigureCanvas(self.fig_change)
        self.canvas_change.setStyleSheet(f"background:{BG_PANEL};")
        self.canvas_change.mpl_connect('motion_notify_event', self.on_bar_hover)
        self.canvas_change.mpl_connect('button_press_event', self.on_bar_click)

        self.fig_trend = Figure(facecolor=BG_PANEL)
        self.canvas_trend = FigureCanvas(self.fig_trend)
        self.canvas_trend.setStyleSheet(f"background:{BG_PANEL};")
        self.canvas_trend.mpl_connect('motion_notify_event', self.on_line_hover)

        rl.addWidget(self.canvas_change, stretch=4)
        rl.addWidget(self.canvas_trend,  stretch=6)

        sp.addWidget(left); sp.addWidget(right)
        sp.setStretchFactor(0, 38); sp.setStretchFactor(1, 62)
        lay.addWidget(sp)
        self.on_combo_change(0)

    # ─── 頁二：四大金剛 ──────────────────────────────────

    def init_multi_tab(self):
        lay = QVBoxLayout(self.tab_multi)
        lay.setContentsMargins(8, 8, 8, 8)
        lay.setSpacing(8)

        # 導航列：純文字式 Tab Bar 風格
        nav = QFrame()
        nav.setFixedHeight(40)
        nav.setStyleSheet(f"QFrame {{ background:{BG_PANEL}; border-radius:6px; border:1px solid {BORDER}; }}")
        nl = QHBoxLayout(nav)
        nl.setContentsMargins(6, 4, 6, 4)
        nl.setSpacing(2)

        nav_items = [
            ("核心持股地圖", ACCENT),
            ("籌碼共識追蹤", ACCENT),
            ("ETF 風格對比",  ACCENT),
            ("每日進出動向", ACCENT),
        ]
        self.nav_btns = []
        for idx, (name, color) in enumerate(nav_items):
            btn = QPushButton(name)
            btn.setFixedHeight(32)
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn.setProperty("nc", color)
            btn.clicked.connect(lambda _, i=idx: self.switch_view(i))
            self.nav_btns.append((btn, color))
            nl.addWidget(btn)
        nl.addStretch()
        lay.addWidget(nav)

        self.multi_stack = QStackedWidget()
        self.page_core      = QWidget()
        self.page_consensus = QWidget()
        self.page_style     = QWidget()
        self.page_radar     = QWidget()
        self.setup_core_page()
        self.setup_consensus_page()
        self.setup_style_page()
        self.setup_radar_page()
        for pg in [self.page_core, self.page_consensus, self.page_style, self.page_radar]:
            self.multi_stack.addWidget(pg)
        lay.addWidget(self.multi_stack)
        self.switch_view(0)

    def switch_view(self, idx):
        self.multi_stack.setCurrentIndex(idx)
        for i, (btn, color) in enumerate(self.nav_btns):
            if i == idx:
                btn.setStyleSheet(f"""
                    QPushButton {{
                        background: {color}18; color: {color};
                        border: 1px solid {color}50; border-radius: 5px;
                        font-size: 13px; font-weight: bold; padding: 0 14px;
                    }}
                """)
            else:
                btn.setStyleSheet(f"""
                    QPushButton {{
                        background: transparent; color: {TEXT_DIM};
                        border: 1px solid transparent; border-radius: 5px;
                        font-size: 13px; font-weight: bold; padding: 0 14px;
                    }}
                    QPushButton:hover {{ color: {TEXT_SEC}; background: {BG_CARD}; }}
                """)

    # ─── 模組1：核心持股地圖 ────────────────────────────
    # 左：散點圖（x=合計權重, y=持有家數, size=權重, color=持有家數）
    def setup_core_page(self):
        lay = QVBoxLayout(self.page_core)
        lay.setContentsMargins(0, 0, 0, 0)
        sp = QSplitter(Qt.Orientation.Horizontal)

        # 左：核心持股散點圖
        left = QFrame()
        left.setStyleSheet(f"QFrame {{ {PANEL_STYLE} }}")
        ll = QVBoxLayout(left)
        ll.setContentsMargins(12, 10, 12, 8)
        ll.setSpacing(6)

        lh = QWidget()
        lhr = QHBoxLayout(lh)
        lhr.setContentsMargins(0, 0, 0, 0)
        lhr.addWidget(section_title("核心持股地圖", ACCENT, 13))
        lhr.addStretch()
        lhr.addWidget(dim_label("僅顯示前20  ·  點擊氣泡放大名稱並連動右側列表"))
        ll.addWidget(lh)

        self.fig_core = Figure(facecolor=BG_PANEL)
        self.canvas_core = FigureCanvas(self.fig_core)
        self.canvas_core.setStyleSheet(f"background:{BG_PANEL};")
        self.canvas_core.mpl_connect('button_press_event', self.on_core_click)
        self.canvas_core.mpl_connect('motion_notify_event', self.on_core_hover)
        ll.addWidget(self.canvas_core)
        self._core_scatter_data = []   # list of (x, y, sid, name)
        self._core_selected_idx = None  # index of highlighted bubble

        # 右：核心持股表
        right = QFrame()
        right.setStyleSheet(f"QFrame {{ {PANEL_STYLE} }}")
        rl = QVBoxLayout(right)
        rl.setContentsMargins(12, 10, 12, 8)
        rl.setSpacing(6)

        rh = QWidget()
        rhr = QHBoxLayout(rh)
        rhr.setContentsMargins(0, 0, 0, 0)
        rhr.addWidget(section_title("核心共識持股", ACCENT, 13))
        rhr.addStretch()
        rhr.addWidget(dim_label("3家以上共同持有 · 依合計權重排序"))
        rl.addWidget(rh)

        self.table_core = QTableWidget()
        self.table_core.setColumnCount(5)
        self.table_core.setHorizontalHeaderLabels(["代號", "名稱", "家數", "持有ETF", "合計權重"])
        self.table_core.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table_core.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        setup_table(self.table_core, stretch_col=3)
        h = self.table_core.horizontalHeader()
        h.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        h.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        h.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        h.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)      # ETF 欄拉寬
        h.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        rl.addWidget(self.table_core)

        sp.addWidget(left); sp.addWidget(right)
        sp.setStretchFactor(0, 52); sp.setStretchFactor(1, 48)
        lay.addWidget(sp)

    # ─── 模組2：籌碼共識 ────────────────────────────────
    # 上：雙向 bar chart（買進/賣出量化視覺化）
    # 下：買進/賣出對照表（無任何邊框色條）

    def setup_consensus_page(self):
        lay = QVBoxLayout(self.page_consensus)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(8)

        # 圖區
        chart_panel = QFrame()
        chart_panel.setStyleSheet(f"QFrame {{ {PANEL_STYLE} }}")
        cl = QVBoxLayout(chart_panel)
        cl.setContentsMargins(12, 10, 12, 8)
        cl.setSpacing(4)

        ch = QWidget()
        chr_l = QHBoxLayout(ch)
        chr_l.setContentsMargins(0, 0, 0, 0)
        chr_l.addWidget(section_title("近3日籌碼共識", TEXT_PRI, 13))
        chr_l.addStretch()
        self.lbl_buy_cnt  = QLabel("—")
        self.lbl_sell_cnt = QLabel("—")
        self.lbl_buy_cnt.setStyleSheet(f"color:{RED}; font-size:12px; font-weight:bold;")
        self.lbl_sell_cnt.setStyleSheet(f"color:{GREEN}; font-size:12px; font-weight:bold;")
        chr_l.addWidget(self.lbl_buy_cnt)
        chr_l.addSpacing(12)
        chr_l.addWidget(self.lbl_sell_cnt)
        cl.addWidget(ch)

        self.fig_consensus = Figure(facecolor=BG_PANEL)
        self.canvas_consensus = FigureCanvas(self.fig_consensus)
        self.canvas_consensus.setStyleSheet(f"background:{BG_PANEL};")
        self.canvas_consensus.setFixedHeight(220)
        cl.addWidget(self.canvas_consensus)
        lay.addWidget(chart_panel)

        # 表區：買/賣並排，無任何色框
        tbl_w = QWidget()
        tbl_l = QHBoxLayout(tbl_w)
        tbl_l.setContentsMargins(0, 0, 0, 0)
        tbl_l.setSpacing(8)

        # 買進表
        bp = QFrame()
        bp.setStyleSheet(f"QFrame {{ {PANEL_STYLE} }}")
        bl = QVBoxLayout(bp)
        bl.setContentsMargins(12, 10, 12, 8)
        bl.setSpacing(4)

        bh = QWidget()
        bhr = QHBoxLayout(bh)
        bhr.setContentsMargins(0, 0, 0, 0)
        bhr.addWidget(section_title("共同加碼 Top 15", RED, 12))
        bhr.addStretch()
        bhr.addWidget(dim_label("近3日 2家以上同步買進"))
        bl.addWidget(bh)

        self.table_buy = QTableWidget()
        self.table_buy.setColumnCount(4)
        self.table_buy.setHorizontalHeaderLabels(["代號", "名稱", "參與ETF", "合計張數"])
        setup_table(self.table_buy, stretch_col=1)
        bl.addWidget(self.table_buy)
        tbl_l.addWidget(bp)

        # 賣出表
        sp2 = QFrame()
        sp2.setStyleSheet(f"QFrame {{ {PANEL_STYLE} }}")
        sl = QVBoxLayout(sp2)
        sl.setContentsMargins(12, 10, 12, 8)
        sl.setSpacing(4)

        sh2 = QWidget()
        shr = QHBoxLayout(sh2)
        shr.setContentsMargins(0, 0, 0, 0)
        shr.addWidget(section_title("共同減碼 Top 15", GREEN, 12))
        shr.addStretch()
        shr.addWidget(dim_label("近3日 2家以上同步賣出"))
        sl.addWidget(sh2)

        self.table_sell = QTableWidget()
        self.table_sell.setColumnCount(4)
        self.table_sell.setHorizontalHeaderLabels(["代號", "名稱", "參與ETF", "合計張數"])
        setup_table(self.table_sell, stretch_col=1)
        sl.addWidget(self.table_sell)
        tbl_l.addWidget(sp2)

        lay.addWidget(tbl_w, 1)

    # ─── 模組3：ETF 風格對比 ─────────────────────────────
    # 左：產業堆疊 bar chart（可點擊 ETF 高亮詳情）
    # 右：互動式卡片區（點擊 ETF 後展示該 ETF 詳細產業分解 + 前10大集中度指示器）

    def setup_style_page(self):
        lay = QVBoxLayout(self.page_style)
        lay.setContentsMargins(0, 0, 0, 0)
        sp = QSplitter(Qt.Orientation.Horizontal)

        # ── 左：ETF 頁籤 + 單一大圓餅圖 ──────────────────
        left = QFrame()
        left.setStyleSheet(f"QFrame {{ {PANEL_STYLE} }}")
        ll = QVBoxLayout(left)
        ll.setContentsMargins(12, 10, 12, 8)
        ll.setSpacing(6)

        # ETF 頁籤列（動態，依 self.mapping 生成，易於擴充）
        tab_row = QWidget()
        tr = QHBoxLayout(tab_row)
        tr.setContentsMargins(0, 0, 0, 0)
        tr.setSpacing(0)
        self.style_etf_btns = []   # (etf_id, QPushButton)
        for etf_id in self.mapping.keys():
            btn = QPushButton(etf_id)
            btn.setFixedHeight(34)
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn.clicked.connect(lambda _, eid=etf_id: self._style_select_etf(eid))
            self.style_etf_btns.append((etf_id, btn))
            tr.addWidget(btn)
        tr.addStretch()
        ll.addWidget(tab_row)
        ll.addWidget(dim_label("點擊扇形區塊 → 右側顯示該產業持股明細", 11))

        # 單一大圓餅圖 canvas（依選中 ETF 重繪）
        self.fig_style = Figure(facecolor=BG_PANEL)
        self.canvas_style = FigureCanvas(self.fig_style)
        self.canvas_style.setStyleSheet(f"background:{BG_PANEL};")
        self.canvas_style.mpl_connect('button_press_event', self._on_pie_click)
        ll.addWidget(self.canvas_style, 1)

        # ── 右：持股明細表 ────────────────────────────────
        right = QFrame()
        right.setStyleSheet(f"QFrame {{ {PANEL_STYLE} }}")
        rl = QVBoxLayout(right)
        rl.setContentsMargins(12, 10, 12, 8)
        rl.setSpacing(6)

        self.lbl_holdings_title = section_title("← 選擇左側 ETF 頁籤，再點擊扇形", TEXT_SEC, 13)
        rl.addWidget(self.lbl_holdings_title)

        self.table_holdings = QTableWidget()
        self.table_holdings.setColumnCount(3)
        self.table_holdings.setHorizontalHeaderLabels(["代號", "名稱", "權重"])
        setup_table(self.table_holdings, stretch_col=1)
        h = self.table_holdings.horizontalHeader()
        h.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        h.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        h.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        rl.addWidget(self.table_holdings, 1)

        sp.addWidget(left); sp.addWidget(right)
        sp.setStretchFactor(0, 60); sp.setStretchFactor(1, 40)
        lay.addWidget(sp)

        self._selected_style_etf = None
        self._pie_wedge_data = {}   # {ind_label: wedge_patch} for current single pie

    def _update_style_etf_btns(self, selected_id):
        """Highlight the selected ETF tab button."""
        for eid, btn in self.style_etf_btns:
            if eid == selected_id:
                btn.setStyleSheet(f"""
                    QPushButton {{
                        background: {ACCENT}22; color: {ACCENT};
                        border: none; border-bottom: 3px solid {ACCENT};
                        font-size: 13px; font-weight: bold; padding: 0 16px;
                    }}
                """)
            else:
                btn.setStyleSheet(f"""
                    QPushButton {{
                        background: transparent; color: {TEXT_SEC};
                        border: none; border-bottom: 3px solid transparent;
                        font-size: 13px; padding: 0 16px;
                    }}
                    QPushButton:hover {{ color: {TEXT_PRI}; background: {BG_CARD}; }}
                """)

    def _style_select_etf(self, etf_id):
        """Switch to a different ETF tab – redraw the single large pie."""
        self._selected_style_etf = etf_id
        self._update_style_etf_btns(etf_id)
        # Reset holdings panel
        self.lbl_holdings_title.setText(f"{etf_id}  ·  點擊扇形查看該產業持股")
        self.lbl_holdings_title.setStyleSheet(
            f"color: {GOLD}; font-size: 13px; font-weight: bold; background: transparent; border: none;")
        self.table_holdings.setRowCount(0)
        self._draw_single_pie(etf_id)

    # kept for compatibility (called from _build_style_data)
    def show_style_detail(self, etf_id):
        self._style_select_etf(etf_id)

    def _draw_single_pie(self, etf_id):
        """Draw one large donut chart for the given ETF."""
        self.fig_style.clear()
        self.fig_style.patch.set_facecolor(BG_PANEL)
        self._pie_wedge_data = {}

        d = self._style_etf_data.get(etf_id)
        if d is None:
            ax = self.fig_style.add_subplot(111)
            ax.text(0.5, 0.5, '資料載入中...', ha='center', va='center',
                    color=TEXT_DIM, fontsize=14, transform=ax.transAxes)
            ax.axis('off')
            self.canvas_style.draw()
            return

        colors = self._style_ind_colors
        # Palette 備用，主要由 _build_style_data 提供的 colors 決定
        palette = [ACCENT, GOLD, RED, GREEN, PURPLE, "#FF6B35", "#1ABC9C"]

        iw = d['ind_weights']
        top8 = iw.head(8)
        others_w = max(0, 100 - top8.sum())
        labels = list(top8.index) + (['其他'] if others_w > 1 else [])
        sizes = list(top8.values) + ([others_w] if others_w > 1 else [])

        # 強制指定「其他」為低調的暗色 (TEXT_DIM)
        pie_colors = []
        for i, lbl in enumerate(labels):
            if lbl == '其他':
                pie_colors.append(TEXT_DIM)
            else:
                pie_colors.append(colors.get(lbl, palette[i % len(palette)]))

        ax = self.fig_style.add_subplot(111)
        ax.set_facecolor(BG_PANEL)

        wedges, _, autotexts = ax.pie(
            sizes, labels=None, colors=pie_colors,
            autopct=lambda p: f'{p:.1f}%' if p > 4 else '',
            pctdistance=0.72, startangle=90,
            wedgeprops=dict(width=0.52, edgecolor=BG_PANEL, linewidth=2)
        )
        for at in autotexts:
            at.set_color('white');
            at.set_fontsize(11);
            at.set_fontweight('bold')

        # ETF name + top10 concentration in centre
        top10_w = d['top10_w']
        ax.text(0, 0.08, etf_id, ha='center', va='center',
                color=GOLD, fontsize=16, fontweight='bold')
        ax.text(0, -0.15, f"前十大\n{top10_w:.1f}%", ha='center', va='center',
                color=TEXT_PRI, fontsize=10)

        # Legend on right
        ax.legend(wedges, labels, loc='center left', bbox_to_anchor=(0.92, 0.5),
                  frameon=False, labelcolor=TEXT_PRI, fontsize=11, handlelength=1.2)

        # Store wedge→label for click
        self._pie_wedge_data = {lbl: w for lbl, w in zip(labels, wedges)}

        self.fig_style.tight_layout(pad=1.5)
        self.canvas_style.draw()

    def _on_pie_click(self, event):
        """Click on a pie wedge → show holdings for that industry."""
        if not event.inaxes: return
        for lbl, wp in self._pie_wedge_data.items():
            if hasattr(wp, 'contains') and wp.contains(event)[0]:
                self._show_industry_holdings(self._selected_style_etf, lbl)
                return

    def _show_industry_holdings(self, etf_id, ind_name):
        if not etf_id: return
        d = self._style_etf_data.get(etf_id)
        if d is None: return
        ddf = d['df'].copy()

        is_other = (ind_name == '其他')

        if is_other:
            # 找出前 8 大產業，反向篩選出屬於「其他」的所有股票
            top_inds = set(d['ind_weights'].head(8).index)
            subset = ddf[~ddf['industry'].isin(top_inds)].sort_values('weight', ascending=False)
        else:
            subset = ddf[ddf['industry'] == ind_name].sort_values('weight', ascending=False)

        self.lbl_holdings_title.setText(f"{etf_id}  ·  {ind_name}  持股明細")
        self.lbl_holdings_title.setStyleSheet(
            f"color: {GOLD}; font-size: 13px; font-weight: bold; background: transparent; border: none;")
        self.table_holdings.setRowCount(len(subset))

        for i, row in enumerate(subset.itertuples()):
            self.table_holdings.setItem(i, 0, titem(str(row.stock_id), ACCENT, Qt.AlignmentFlag.AlignCenter, True))

            # [關鍵優化]：如果是點擊「其他」，在名稱後方加上它真正的產業名稱，解除疑惑
            if is_other and row.industry != '其他':
                display_name = f"{row.name} <{row.industry}>"
            else:
                display_name = str(row.name)

            self.table_holdings.setItem(i, 1, titem(display_name, TEXT_PRI))
            self.table_holdings.setItem(i, 2, titem(f"{row.weight:.2f}%", GOLD,
                                                    Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, True))

    # ─── 模組4：每日進出動向 ─────────────────────────────
    # 上：進出概覽圖（bar chart：新增為紅點、移除為灰點、調整量為雙向bar）
    # 下：新增/移除表 + 調整排行表（三欄並排，乾淨無邊條）

    def setup_radar_page(self):
        lay = QVBoxLayout(self.page_radar)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(8)

        # 下：三欄表格，乾淨無多餘邊框
        tbl_row = QWidget()
        tr_l = QHBoxLayout(tbl_row)
        tr_l.setContentsMargins(0, 0, 0, 0)
        tr_l.setSpacing(8)

        # 新增
        np_panel = QFrame()
        np_panel.setStyleSheet(f"QFrame {{ {PANEL_STYLE} }}")
        np_l = QVBoxLayout(np_panel)
        np_l.setContentsMargins(12, 10, 12, 8)
        np_l.setSpacing(4)
        np_l.addWidget(section_title("今日新增持股", RED, 12))
        self.table_new = QTableWidget()
        self.table_new.setColumnCount(4)
        self.table_new.setHorizontalHeaderLabels(["ETF", "代號", "名稱", "權重"])
        setup_table(self.table_new, stretch_col=2)
        np_l.addWidget(self.table_new)
        tr_l.addWidget(np_panel)

        # 移除
        op_panel = QFrame()
        op_panel.setStyleSheet(f"QFrame {{ {PANEL_STYLE} }}")
        op_l = QVBoxLayout(op_panel)
        op_l.setContentsMargins(12, 10, 12, 8)
        op_l.setSpacing(4)
        op_l.addWidget(section_title("今日完全移除", TEXT_SEC, 12))
        self.table_out = QTableWidget()
        self.table_out.setColumnCount(4)
        self.table_out.setHorizontalHeaderLabels(["ETF", "代號", "名稱", "前日權重"])
        setup_table(self.table_out, stretch_col=2)
        op_l.addWidget(self.table_out)
        tr_l.addWidget(op_panel)

        # 調整排行
        cp_panel = QFrame()
        cp_panel.setStyleSheet(f"QFrame {{ {PANEL_STYLE} }}")
        cp_l2 = QVBoxLayout(cp_panel)
        cp_l2.setContentsMargins(12, 10, 12, 8)
        cp_l2.setSpacing(4)
        cp_l2.addWidget(section_title("大幅調整排行", PURPLE, 12))
        self.table_change = QTableWidget()
        self.table_change.setColumnCount(5)
        self.table_change.setHorizontalHeaderLabels(["ETF", "代號", "名稱", "張數變化", "權重變化"])
        setup_table(self.table_change, stretch_col=2)
        cp_l2.addWidget(self.table_change)
        tr_l.addWidget(cp_panel)

        lay.addWidget(tbl_row, 1)

    # ─── 數據載入與處理 ──────────────────────────────────

    def load_multi_etf_data(self):
        self.mw = MultiETFDataWorker(self.mapping)
        self.mw.multi_data_fetched.connect(self.process_multi_data)
        self.mw.start()

    def process_multi_data(self, df):
        if df.empty: return
        self.multi_etf_df = df
        self.update_all_modules()

    def update_all_modules(self):
        if self.multi_etf_df.empty: return
        df = self.multi_etf_df.copy()
        etfs = list(self.mapping.keys())
        etf_latest_stocks, etf_latest_df = {}, {}
        all_changes, consensus_changes = [], []

        for etf in etfs:
            sub = df[df['etf_id'] == etf]
            dates = sorted(sub['date'].unique())
            if not dates: continue
            t0 = dates[-1]
            df0 = sub[sub['date'] == t0]
            etf_latest_stocks[etf] = set(df0['stock_id'])
            etf_latest_df[etf] = df0

            if len(dates) >= 2:
                t1 = dates[-2]
                m1 = pd.merge(df0, sub[sub['date'] == t1], on=['stock_id', 'name'],
                              suffixes=('_now', '_prev'), how='outer').fillna(0)
                for _, r in m1.iterrows():
                    all_changes.append({'etf': etf, 'stock_id': str(r['stock_id']), 'name': str(r['name']),
                                        'w_now': r['weight_now'], 'w_prev': r['weight_prev'],
                                        's_now': r['shares_now'], 's_prev': r['shares_prev'],
                                        's_diff': r['shares_now'] - r['shares_prev'],
                                        'w_diff': r['weight_now'] - r['weight_prev']})
                t3 = dates[max(0, len(dates) - 4)]
                m3 = pd.merge(df0, sub[sub['date'] == t3], on=['stock_id', 'name'],
                              suffixes=('_now', '_prev'), how='outer').fillna(0)
                for _, r in m3.iterrows():
                    consensus_changes.append({'etf': etf, 'stock_id': str(r['stock_id']),
                                              'name': str(r['name']),
                                              's_diff': r['shares_now'] - r['shares_prev']})

        # ── 模組1 ──
        self._build_core_data(etf_latest_df)

        # ── 模組2 ──
        self._build_consensus(consensus_changes)

        # ── 模組3 ──
        self._build_style_data(etf_latest_df)

        # ── 模組4 ──
        self._build_radar_data(all_changes)

    # ─── 模組1 資料 ──────────────────────────────────────

    def _build_core_data(self, etf_latest_df):
        s2e, s_nm, s_wt = {}, {}, {}
        for etf, ddf in etf_latest_df.items():
            for _, r in ddf.iterrows():
                sid, nm, w = str(r['stock_id']), str(r['name']), r['weight']
                if sid not in s2e: s2e[sid] = []; s_wt[sid] = 0
                s2e[sid].append(etf); s_nm[sid] = nm; s_wt[sid] += w

        core = [[sid, s_nm[sid], len(el), ", ".join(el), s_wt[sid]]
                for sid, el in s2e.items() if len(el) >= 3]
        core.sort(key=lambda x: x[4], reverse=True)

        # 填表
        self.table_core.setRowCount(len(core))
        for i, (sid, nm, cnt, etf_str, wt) in enumerate(core):
            clr = RED if wt > 20 else GOLD if wt > 10 else TEXT_PRI
            self.table_core.setItem(i, 0, titem(sid, ACCENT, Qt.AlignmentFlag.AlignCenter, True))
            self.table_core.setItem(i, 1, titem(nm))
            cnt_clr = RED if cnt == 4 else GOLD
            self.table_core.setItem(i, 2, titem(f"{cnt}家", cnt_clr, Qt.AlignmentFlag.AlignCenter, True))
            # ETF 欄：完整文字 + tooltip 顯示完整內容
            etf_item = titem(etf_str, TEXT_PRI)
            etf_item.setToolTip(etf_str)   # hover tooltip 顯示完整 ETF 清單
            self.table_core.setItem(i, 3, etf_item)
            self.table_core.setItem(i, 4, titem(f"{wt:.2f}%", clr, Qt.AlignmentFlag.AlignCenter, True))

        # 繪製散點圖
        self._draw_core_scatter(core)

    def _draw_core_scatter(self, core):
        # Only show top 20 for readability
        display_core = core[:20]
        self.fig_core.clear()
        ax = self.fig_core.add_subplot(111)
        ax.set_facecolor(BG_CARD)
        self.fig_core.patch.set_facecolor(BG_PANEL)

        if not display_core:
            ax.text(0.5, 0.5, '資料載入中...', ha='center', va='center',
                    color=TEXT_DIM, fontsize=14, transform=ax.transAxes)
            ax.axis('off'); self.canvas_core.draw(); return

        xs   = [c[4] for c in display_core]
        cnts = [c[2] for c in display_core]
        nms  = [c[1] for c in display_core]
        sids = [c[0] for c in display_core]

        cnt_colors = {3: ACCENT + "CC", 4: RED + "CC"}
        scatter_colors = [cnt_colors.get(c, TEXT_SEC + "CC") for c in cnts]
        sizes = [max(300, w * 35) for w in xs]   # larger bubbles for top 20

        sc = ax.scatter(range(len(display_core)), xs, s=sizes, c=scatter_colors,
                        alpha=0.88, edgecolors='none', zorder=5)
        self._core_scatter_data = list(zip(range(len(display_core)), xs, sids, nms))
        self._core_labels = []   # store text artists for highlight

        # Draw all labels with normal size
        for i, (sid, nm, w, cnt) in enumerate(zip(sids, nms, xs, cnts)):
            y_off = w * 0.10 + (3.0 if i % 2 == 1 else 0.8)
            txt = ax.text(i, w + y_off, nm, ha='center', va='bottom',
                          color=TEXT_PRI if cnt == 4 else TEXT_SEC,
                          fontsize=9, fontweight='bold' if cnt == 4 else 'normal',
                          clip_on=True)
            self._core_labels.append(txt)

        self._core_selected_idx = None

        # 圖例
        leg = [mpatches.Patch(color=RED,   label='4家共同持有'),
               mpatches.Patch(color=ACCENT, label='3家共同持有')]
        ax.legend(handles=leg, frameon=False, labelcolor=TEXT_PRI,
                  fontsize=10, loc='upper right')

        # [關鍵修正]: 動態拉高 Y 軸天花板，預留 35% 空間，防止台積電等大權重文字被切掉
        max_w = max(xs) if xs else 50
        ax.set_ylim(-2, max_w * 1.35)

        ax.set_xticks([]); ax.set_xlim(-0.8, len(display_core) - 0.2)
        ax.set_ylabel("合計權重 (%)", color=TEXT_SEC, fontsize=10)
        ax.tick_params(colors=TEXT_SEC, labelsize=9)
        ax.grid(axis='y', color=BORDER, linestyle=':', alpha=0.5)
        for s in ax.spines.values(): s.set_visible(False)
        ax.set_title("共識持股分布  ·  前20大（圓圈越大=合計權重越高）",
                     color=TEXT_PRI, fontsize=10, loc='left', pad=8)

        self.fig_core.tight_layout(pad=1.2)
        self.canvas_core.draw()

    def on_core_click(self, e):
        if not e.inaxes or not self._core_scatter_data: return
        # Find closest bubble by x distance
        best_i, best_dist = 0, float('inf')
        for list_i, (xi, yi, sid, nm) in enumerate(self._core_scatter_data):
            d = abs(e.xdata - xi)
            if d < best_dist:
                best_dist = d; best_i = list_i
        if best_dist > 0.6: return
        xi, yi, sid, nm = self._core_scatter_data[best_i]

        # Highlight: reset all labels, enlarge clicked one
        if hasattr(self, '_core_labels') and self._core_labels:
            for j, txt in enumerate(self._core_labels):
                if j == best_i:
                    txt.set_fontsize(13); txt.set_color(GOLD)
                    txt.set_fontweight('bold')
                else:
                    txt.set_fontsize(9)
                    txt.set_color(TEXT_PRI if self._core_scatter_data[j][2] == 4 else TEXT_SEC
                                  if j < len(self._core_scatter_data) else TEXT_SEC)
            self.canvas_core.draw()

        # Scroll table to row
        for i in range(self.table_core.rowCount()):
            if self.table_core.item(i, 0) and self.table_core.item(i, 0).text() == sid:
                self.table_core.selectRow(i)
                self.table_core.scrollToItem(self.table_core.item(i, 0))
                break

    def on_core_hover(self, e):
        pass  # 可擴展 tooltip

    # ─── 模組2 資料 ──────────────────────────────────────

    def _build_consensus(self, consensus_changes):
        buy_d, sell_d = {}, {}
        for c in consensus_changes:
            sid, nm, etf, diff = c['stock_id'], c['name'], c['etf'], c['s_diff']
            if diff > 0:
                if sid not in buy_d: buy_d[sid] = {'name': nm, 'etfs': [], 'tot': 0}
                buy_d[sid]['etfs'].append(etf); buy_d[sid]['tot'] += diff
            elif diff < 0:
                if sid not in sell_d: sell_d[sid] = {'name': nm, 'etfs': [], 'tot': 0}
                sell_d[sid]['etfs'].append(etf); sell_d[sid]['tot'] += diff

        buy_list  = [[k, v['name'], ", ".join(v['etfs']), v['tot']]
                     for k, v in buy_d.items() if len(v['etfs']) >= 2]
        sell_list = [[k, v['name'], ", ".join(v['etfs']), v['tot']]
                     for k, v in sell_d.items() if len(v['etfs']) >= 2]
        buy_list.sort(key=lambda x: x[3], reverse=True)
        sell_list.sort(key=lambda x: x[3])

        self.lbl_buy_cnt.setText(f"加碼共識 {len(buy_list)} 檔")
        self.lbl_sell_cnt.setText(f"減碼共識 {len(sell_list)} 檔")

        # 繪圖：雙向橫 bar
        self._draw_consensus_chart(buy_list[:12], sell_list[:12])

        # 填表
        self._fill_consensus_table(self.table_buy,  buy_list[:15],  RED,   positive=True)
        self._fill_consensus_table(self.table_sell, sell_list[:15], GREEN, positive=False)

    def _draw_consensus_chart(self, buy_list, sell_list):
        self.fig_consensus.clear()
        self.fig_consensus.patch.set_facecolor(BG_PANEL)
        ax = self.fig_consensus.add_subplot(111)
        ax.set_facecolor(BG_CARD)

        labels, vals = [], []
        for sid, nm, _, tot in (buy_list + sell_list)[:20]:
            labels.append(nm)
            vals.append(int(tot / 1000))

        if not labels:
            ax.text(0.5, 0.5, '等待資料...', ha='center', va='center',
                    color=TEXT_DIM, fontsize=12, transform=ax.transAxes)
            ax.axis('off'); self.canvas_consensus.draw(); return

        y = np.arange(len(labels))
        colors = [RED if v > 0 else GREEN for v in vals]
        ax.barh(y, vals, color=colors, height=0.6, alpha=0.85, edgecolor='none')
        ax.set_yticks(y)
        ax.set_yticklabels(labels, color=TEXT_PRI, fontsize=10, fontweight='bold')
        ax.axvline(0, color=TEXT_SEC, linewidth=1.2)
        ax.set_xlabel("張數（千張）", color=TEXT_PRI, fontsize=10, fontweight='bold')
        ax.tick_params(axis='x', colors=TEXT_PRI, labelsize=9)
        ax.tick_params(axis='y', colors=TEXT_PRI, labelsize=10)
        ax.xaxis.label.set_color(TEXT_PRI)
        ax.grid(axis='x', color=BORDER, linestyle=':', alpha=0.5)
        for s in ax.spines.values(): s.set_visible(False)

        self.fig_consensus.tight_layout(pad=1.2)
        self.canvas_consensus.draw()

    def _fill_consensus_table(self, table, data, clr, positive=True):
        table.setRowCount(len(data))
        for i, (sid, nm, etfs, tot) in enumerate(data):
            amt = f"+{int(tot/1000):,}張" if positive else f"{int(tot/1000):,}張"
            table.setItem(i, 0, titem(sid, ACCENT, Qt.AlignmentFlag.AlignCenter, True))
            table.setItem(i, 1, titem(nm, TEXT_PRI, bold=True))
            etf_item = titem(etfs, TEXT_PRI)     # 高對比色，完整顯示
            etf_item.setToolTip(etfs)            # tooltip 完整展示
            table.setItem(i, 2, etf_item)
            table.setItem(i, 3, titem(amt, clr, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, True))

    # ─── 模組3 資料 ──────────────────────────────────────

    def _build_style_data(self, etf_latest_df):
        # 擴充調色盤避免撞色，移除可能與背景太接近的暗色
        palette = [
            ACCENT, GOLD, RED, GREEN, PURPLE,
            "#FF9F43", "#00CECB", "#FF6B81", "#A8E6CF", "#D6A2E8", "#FD7272"
        ]
        all_inds = {}

        for etf, ddf in etf_latest_df.items():
            if ddf.empty: continue
            ddf = ddf.copy()

            # 完全依賴成功載入的 dj_industry.csv
            ddf['industry'] = ddf['stock_id'].map(self.industry_map).fillna('其他')

            # 驗證機制：如果在 dj_industry.csv 裡面真的找不到，印出來供你事後補齊
            others_df = ddf[ddf['industry'] == '其他']
            if not others_df.empty:
                print(
                    f"[資料驗證] {etf} 找不到分類，被歸為'其他': {list(others_df['stock_id'] + ' ' + others_df['name'])}")

            iw = ddf.groupby('industry')['weight'].sum().sort_values(ascending=False)
            top10 = ddf.sort_values('weight', ascending=False).head(10)['weight'].sum()
            self._style_etf_data[etf] = {'df': ddf, 'ind_weights': iw, 'top10_w': top10}
            for ind, w in iw.items():
                if ind != '其他':  # 統計主要產業來分配顏色
                    all_inds[ind] = all_inds.get(ind, 0) + w

        top_inds = sorted(all_inds, key=all_inds.get, reverse=True)
        self._style_ind_colors = {ind: palette[i % len(palette)] for i, ind in enumerate(top_inds)}

        # 預設顯示第一個 ETF
        first_etf = list(self.mapping.keys())[0]
        self._style_select_etf(first_etf)

    # ─── 模組4 資料 ──────────────────────────────────────

    def _build_radar_data(self, all_changes):
        new_list, out_list, chg_list = [], [], []
        for c in all_changes:
            if c['s_prev'] == 0 and c['s_now'] > 0:
                new_list.append([c['etf'], c['stock_id'], c['name'], f"{c['w_now']:.2f}%"])
            elif c['s_prev'] > 0 and c['s_now'] == 0:
                out_list.append([c['etf'], c['stock_id'], c['name'], f"{c['w_prev']:.2f}%"])
            elif abs(c['s_diff']) > 0:
                chg_list.append([c['etf'], c['stock_id'], c['name'], c['s_diff'], c['w_diff']])
        chg_list.sort(key=lambda x: abs(x[3]), reverse=True)

        # 填表
        self._fill_simple_table(self.table_new, new_list, RED)
        self._fill_simple_table(self.table_out, out_list, TEXT_SEC)
        self._fill_change_table(chg_list[:20])

    def _draw_radar_chart(self, new_list, out_list, chg_list):
        self.fig_radar.clear()
        self.fig_radar.patch.set_facecolor(BG_PANEL)
        ax = self.fig_radar.add_subplot(111)
        ax.set_facecolor(BG_CARD)

        if not chg_list and not new_list and not out_list:
            ax.text(0.5, 0.5, '今日無變動', ha='center', va='center',
                    color=TEXT_DIM, fontsize=13, transform=ax.transAxes)
            ax.axis('off'); self.canvas_radar.draw(); return

        labels = [f"{r[2]}({r[0]})" for r in chg_list[:16]]
        vals   = [int(r[3] / 1000) for r in chg_list[:16]]
        colors = [RED if v > 0 else GREEN for v in vals]

        y = np.arange(len(labels))
        ax.barh(y, vals, color=colors, height=0.58, alpha=0.85, edgecolor='none')

        # 新增/移除 圓點 overlay（在最左/最右顯示 badge）
        y_offset = len(labels)
        for j, row in enumerate(new_list[:5]):
            ax.plot(0, y_offset + j, 'o', color=RED, ms=8, zorder=5)
            ax.text(0.5, y_offset + j, f"★新增  {row[2]}({row[0]})",
                    va='center', color=RED, fontsize=8.5)
        for j, row in enumerate(out_list[:5]):
            ax.plot(0, y_offset + len(new_list) + j, 'o', color=TEXT_DIM, ms=8, zorder=5)
            ax.text(0.5, y_offset + len(new_list) + j, f"✕移除  {row[2]}({row[0]})",
                    va='center', color=TEXT_SEC, fontsize=8.5)

        all_labels = labels + [''] * (len(new_list[:5]) + len(out_list[:5]))
        ax.set_yticks(range(len(all_labels)))
        ax.set_yticklabels(labels + [r[2] for r in new_list[:5]] + [r[2] for r in out_list[:5]],
                           color=TEXT_PRI, fontsize=9)
        ax.set_yticklabels(ax.get_yticklabels())
        # override: use simple labels
        ax.set_yticks(y); ax.set_yticklabels(labels, color=TEXT_PRI, fontsize=9)
        ax.axvline(0, color=BORDER, linewidth=1)
        ax.set_xlabel("張數變化（千張）", color=TEXT_SEC, fontsize=9)
        ax.tick_params(colors=TEXT_SEC, labelsize=9)
        ax.grid(axis='x', color=BORDER, linestyle=':', alpha=0.5)
        for s in ax.spines.values(): s.set_visible(False)

        self.fig_radar.tight_layout(pad=1.2)
        self.canvas_radar.draw()

    def _fill_simple_table(self, table, data, clr):
        table.setRowCount(len(data))
        for i, row in enumerate(data):
            table.setItem(i, 0, titem(row[0], ACCENT, Qt.AlignmentFlag.AlignCenter))
            table.setItem(i, 1, titem(row[1], TEXT_SEC, Qt.AlignmentFlag.AlignCenter))
            table.setItem(i, 2, titem(row[2]))
            table.setItem(i, 3, titem(row[3], clr, Qt.AlignmentFlag.AlignCenter, True))

    def _fill_change_table(self, chg_list):
        self.table_change.setRowCount(len(chg_list))
        for i, row in enumerate(chg_list):
            clr = RED if row[3] > 0 else GREEN
            table_change = self.table_change
            table_change.setItem(i, 0, titem(row[0], ACCENT, Qt.AlignmentFlag.AlignCenter))
            table_change.setItem(i, 1, titem(row[1], TEXT_SEC, Qt.AlignmentFlag.AlignCenter))
            table_change.setItem(i, 2, titem(row[2]))
            table_change.setItem(i, 3, titem(f"{int(row[3]/1000):+,}張", clr,
                                             Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, True))
            table_change.setItem(i, 4, titem(f"{row[4]:+.2f}%", clr,
                                             Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, True))

    # ─── 單檔操作邏輯 ─────────────────────────────────────

    def on_combo_change(self, index):
        keys = list(self.mapping.keys())
        if index < len(keys):
            etf_id, provider = keys[index], self.mapping[keys[index]][0]
            self.lbl_etf_info.setText(self.mapping[keys[index]][1])
            self.stock_table.setRowCount(0)
            self.fig_change.clear(); self.canvas_change.draw()
            self.fig_trend.clear(); self.canvas_trend.draw()
            self.worker = ETFDataWorker(etf_id, provider)
            self.worker.data_fetched.connect(self.process_data)
            self.worker.start()

    def process_data(self, df, etf_id):
        if df.empty: return
        if 'date' in df.columns: df['date'] = pd.to_datetime(df['date'])
        self.current_df = df
        dates = sorted(df['date'].unique())
        if not dates: return
        self.latest_date = dates[-1]
        self.latest_data = df[df['date'] == dates[-1]].sort_values('weight', ascending=False)
        self.load_watchlists_to_combo()

        if len(dates) >= 2:
            self.merged_data = pd.merge(
                self.latest_data, df[df['date'] == dates[-2]],
                on=['stock_id', 'name'], suffixes=('_now', '_prev'), how='outer').fillna(0)
            self.merged_data['share_diff'] = self.merged_data['shares_now'] - self.merged_data['shares_prev']
            self.merged_data['pct_change'] = self.merged_data.apply(
                lambda r: (r['share_diff'] / r['shares_prev'] * 100) if r['shares_prev'] > 0 else 0, axis=1)

            def ca(r):
                y, t, p = r['shares_prev'], r['shares_now'], r['pct_change']
                if y == 0 and t > 0: return '★新買入'
                if y > 0 and t == 0: return '●清空'
                if p >= 50:  return '▲大增'
                if p <= -50: return '▼大減'
                if 10 <= p < 50:   return '△增持'
                if -50 < p <= -10: return '▽減持'
                return None

            self.merged_data['action_type'] = self.merged_data.apply(ca, axis=1)
            top12 = (self.merged_data[self.merged_data['share_diff'].abs() > 0]
                     .sort_values('share_diff', key=abs, ascending=False)
                     .head(12).sort_values('pct_change'))
            self.plot_changes(top12, self.latest_date)
        else:
            self.plot_changes(pd.DataFrame(), self.latest_date)

        self.render_table()
        if not self.latest_data.empty:
            fid = str(self.latest_data.iloc[0]['stock_id'])
            self.plot_trend(fid, str(self.latest_data.iloc[0]['name']), self.get_market_suffix(fid))
            self.stock_table.selectRow(0)

    def render_table(self):
        if self.latest_data.empty: return
        cl = []
        wl = self.combo_compare.currentText()
        if wl != "不比對":
            try:
                cl = json.load(open(Path("data/watchlist.json"), 'r', encoding='utf-8')).get(wl, [])
            except: pass

        self.stock_table.setRowCount(len(self.latest_data))
        for i, r in enumerate(self.latest_data.itertuples()):
            sid, nm, w, sh = str(r.stock_id), str(r.name), r.weight, r.shares
            ar, sc = "", GOLD if i < 10 else TEXT_SEC

            if sid in cl and not self.merged_data.empty and sid in self.merged_data['stock_id'].values:
                diff = self.merged_data[self.merged_data['stock_id'] == sid].iloc[0]['share_diff']
                ar, sc = (" ⬆", RED) if diff > 0 else (" ⬇", GREEN) if diff < 0 else (" —", GOLD)

            bold_top = i < 10
            self.stock_table.setItem(i, 0, titem(sid, ACCENT, Qt.AlignmentFlag.AlignCenter, bold_top))
            self.stock_table.setItem(i, 1, titem(nm, GOLD if i < 10 else TEXT_PRI, bold=bold_top))
            self.stock_table.setItem(i, 2, titem(f"{w:.2f}%", TEXT_SEC,
                                                  Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter))
            self.stock_table.setItem(i, 3, titem(f"{int(sh/1000) if pd.notna(sh) else 0:,}{ar}", sc,
                                                  Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, bool(ar)))

    def plot_changes(self, df, dt):
        self.fig_change.clear()
        ax = self.fig_change.add_subplot(111)
        ax.set_facecolor(BG_CARD)
        self.fig_change.patch.set_facecolor(BG_PANEL)
        self.bar_data = df.reset_index(drop=True)
        if self.bar_data.empty:
            ax.text(0.5, 0.5, "期間無明顯持股變動", ha='center', va='center',
                    color=TEXT_DIM, fontsize=13); ax.axis('off')
            self.canvas_change.draw(); return

        y = np.arange(len(self.bar_data))
        ax.barh(y, self.bar_data['pct_change'],
                color=[RED if x >= 0 else GREEN for x in self.bar_data['share_diff']],
                height=0.55, alpha=0.85, edgecolor='none')
        ax.set_yticks(y)
        ax.set_yticklabels(self.bar_data['name'], fontsize=10.5, fontweight='bold', color=TEXT_PRI)
        ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=100))
        ax.set_title(f"資金流向  ·  {dt.strftime('%Y-%m-%d')}", color=TEXT_DIM, fontsize=10, loc='left', pad=6)
        ax.tick_params(colors=TEXT_SEC, labelsize=9)
        ax.grid(axis='x', color=BORDER, linestyle=':', alpha=0.5)
        ax.axvline(0, color=BORDER, linewidth=1)
        for s in ax.spines.values(): s.set_visible(False)
        self.fig_change.tight_layout(pad=1.2)
        self.canvas_change.draw()

    def plot_trend(self, sid, snm, mkt="TW"):
        if self.current_df is None: return
        td = self.current_df[self.current_df['stock_id'] == str(sid)].copy()
        if td.empty: return
        if td['date'].max() < self.current_df['date'].max():
            nr = td.iloc[-1].copy()
            nr['date'], nr['shares'], nr['weight'] = self.current_df['date'].max(), 0, 0.0
            td = pd.concat([td, pd.DataFrame([nr])], ignore_index=True)
        td = td.sort_values('date')

        pd_df = pd.DataFrame()
        p_pth = Path(f"data/cache/tw/{sid}_{mkt}.parquet")
        if p_pth.exists():
            try:
                pdf = pd.read_parquet(p_pth)
                pdf.columns = [c.capitalize() for c in pdf.columns]
                pdf.index = pd.to_datetime(pdf.index).tz_localize(None)
                pd_df = pdf[pdf.index >= td['date'].min()].copy()
            except: pass

        self.fig_trend.clear()
        self.fig_trend.patch.set_facecolor(BG_PANEL)
        ax1 = self.fig_trend.add_subplot(111)
        ax1.set_facecolor(BG_CARD)
        ax2 = ax1.twinx(); ax3 = ax1.twinx()
        ax3.spines['right'].set_position(('outward', 60))
        ax1.set_zorder(10); ax2.set_zorder(11); ax3.set_zorder(2)
        ax1.patch.set_visible(False); ax2.patch.set_visible(False)

        # 庫存張數線：紫色，確保可見，單位=千張（軸用逗號格式，不加K）
        shares_in_k = td['shares'] / 1000
        l3, = ax3.plot(td['date'], shares_in_k, color=PURPLE, lw=1.8,
                       marker='o', ms=3, alpha=0.75, label='庫存(千張)')
        ax3.tick_params(axis='y', colors=PURPLE, labelsize=8)
        # 移除科學記號，用千張數值直接顯示（如 3.4 代表3400張）
        ax3.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x:.1f}'))

        l1, = ax1.plot(td['date'], td['weight'], color=ACCENT, lw=2, marker='o', ms=3.5, label='權重')
        ax1.fill_between(td['date'], td['weight'], color=ACCENT, alpha=0.07)

        l2 = l4 = None
        if not pd_df.empty:
            l2, = ax2.plot(pd_df.index, pd_df['Close'], color=GOLD, lw=1.5, ls='--', alpha=0.9, label='股價')
            ts, tc, dcs = 0, 0.0, []
            for _, r in td.iterrows():
                try: pidx = pd_df.index.get_indexer([r['date']], method='ffill')[0]
                except: pidx = -1
                p = pd_df['Close'].iloc[pidx] if pidx != -1 else 0
                if p > 0:
                    if r['shares'] > ts: tc += (r['shares'] - ts) * p; ts = r['shares']
                    elif r['shares'] < ts:
                        tc = (tc * r['shares'] / ts) if r['shares'] > 0 else 0.0; ts = r['shares']
                dcs.append(tc / ts if ts > 0 else 0.0)
            td['avg_cost'] = dcs
            if (td['shares'] > 0).any():
                cost_sub = td.loc[td['shares'] > 0]
                l4, = ax2.plot(cost_sub['date'], cost_sub['avg_cost'],
                               color='#FF6EC7', ls=':', marker='o', ms=3.5, lw=1.5, alpha=0.8, label='成本')
                # 最後一天成本標注
                last_cost_row = cost_sub.iloc[-1]
                lc = last_cost_row['avg_cost']
                if lc > 0:
                    ax2.plot(last_cost_row['date'], lc, 'o', color='#FF6EC7', ms=6, zorder=6)
                    ax2.text(last_cost_row['date'], lc, f" {lc:.1f}",
                             color='#FF6EC7', fontsize=10, fontweight='bold',
                             va='center', bbox=dict(boxstyle="round,pad=0.3", fc=BG_CARD, ec='#FF6EC7', alpha=0.9))
            lp = pd_df['Close'].iloc[-1]
            ax2.plot(pd_df.index[-1], lp, 'o', color=GOLD, ms=6, zorder=5)
            ax2.text(pd_df.index[-1], lp, f" {lp:.1f}", color=GOLD, fontsize=10, fontweight='bold',
                     va='center', bbox=dict(boxstyle="round,pad=0.3", fc=BG_CARD, ec=GOLD, alpha=0.9))

        self.line_data = {'etf': td, 'price': pd_df}
        ax1.yaxis.set_major_formatter(mtick.PercentFormatter())
        ax1.set_title(f"  {sid}  {snm}", color=TEXT_PRI, fontsize=12, fontweight='bold', loc='left')
        ax1.tick_params(axis='y', colors=ACCENT, labelsize=9)
        ax2.tick_params(axis='y', colors=GOLD, labelsize=9)
        ax1.tick_params(axis='x', colors=TEXT_SEC, labelsize=9)
        ax1.grid(True, color=BORDER, linestyle=':', alpha=0.4)
        for ax in [ax1, ax2, ax3]:
            for s in ax.spines.values(): s.set_visible(False)
        ax1.legend(handles=[l for l in [l1, l2, l3, l4] if l],
                   loc='upper left', frameon=False, labelcolor=TEXT_PRI, fontsize=9)
        self.fig_trend.tight_layout(pad=1.2)
        self.canvas_trend.draw()

    def on_bar_click(self, e):
        if not e.inaxes or self.bar_data is None: return
        if not self.fig_change.axes: return
        if e.inaxes != self.fig_change.axes[0]: return
        y = int(round(e.ydata))
        if 0 <= y < len(self.bar_data):
            sid = str(self.bar_data.iloc[y]['stock_id'])
            for i in range(self.stock_table.rowCount()):
                if self.stock_table.item(i, 0) and self.stock_table.item(i, 0).text() == sid:
                    self.stock_table.selectRow(i); break
            self.plot_trend(sid, str(self.bar_data.iloc[y]['name']), self.get_market_suffix(sid))

    def on_bar_hover(self, e):
        if not e.inaxes or self.bar_data is None: return
        y = int(round(e.ydata))
        if 0 <= y < len(self.bar_data) and abs(e.ydata - y) < 0.4:
            r = self.bar_data.iloc[y]
            dc = RED if r['share_diff'] >= 0 else GREEN
            self.info_label.setText(
                f"<span style='color:{TEXT_PRI};font-weight:bold;'>{r['name']}</span>"
                f"<span style='color:{TEXT_DIM};'> | </span>"
                f"<span style='color:{TEXT_SEC};'>{int(r['shares_prev']/1000)}→{int(r['shares_now']/1000)} 張</span>"
                f"<span style='color:{TEXT_DIM};'> | </span>"
                f"<span style='color:{dc};font-weight:bold;'>{r['action_type'] or ''} {int(r['share_diff']/1000):+,} ({r['pct_change']:+.1f}%)</span>")

    def on_line_hover(self, e):
        if not e.inaxes or self.line_data is None: return
        import matplotlib.dates as mdates
        try: dt = mdates.num2date(e.xdata).replace(tzinfo=None)
        except: return
        td, pd_df = self.line_data['etf'], self.line_data['price']
        r = td.loc[(td['date'] - dt).abs().idxmin()]
        idx = td.index.get_loc(r.name)
        txt = p_val = ""
        if idx > 0:
            pr = td.iloc[idx - 1]
            diff = r['shares'] - pr['shares']
            pct = (diff / pr['shares'] * 100) if pr['shares'] > 0 else 0
            dc = RED if diff >= 0 else GREEN
            txt = f" <span style='color:{dc};'>{int(diff/1000):+,}張 ({pct:+.2f}%)</span>"
        if not pd_df.empty:
            try:
                p_val = f" <span style='color:{GOLD};'>股價:{pd_df.iloc[pd_df.index.get_indexer([r['date']], method='nearest')[0]]['Close']:.1f}</span>"
            except: pass
        self.info_label.setText(
            f"<span style='color:{TEXT_SEC};'>{r['date'].strftime('%Y-%m-%d')}</span>"
            f" <span style='color:{ACCENT};'>權重:{r['weight']}%</span>"
            f" <span style='color:{TEXT_PRI};'>持股:{int(r['shares']/1000):,}張</span>"
            f"{txt}{p_val}"
            f" <span style='color:#FF6EC7;'>成本:{r.get('avg_cost', 0):.1f}</span>")

    def on_table_clicked(self, row, col):
        if not self.stock_table.item(row, 0): return
        sid = self.stock_table.item(row, 0).text()
        nm  = self.stock_table.item(row, 1).text() if self.stock_table.item(row, 1) else ""
        self.plot_trend(sid, nm, self.get_market_suffix(sid))
        self.stock_clicked_signal.emit(f"{sid}_{self.get_market_suffix(sid)}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    win = ActiveETFModule()
    win.resize(1440, 860)
    win.setWindowTitle("ETF 主動持股戰情室")
    win.show()
    sys.exit(app.exec())