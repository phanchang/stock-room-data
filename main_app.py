import sys
import os
from pathlib import Path
import pandas as pd

# 設定模組搜尋路徑
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QHBoxLayout,
                             QVBoxLayout, QPushButton, QStackedWidget,
                             QLabel, QButtonGroup, QGridLayout, QTabWidget)
from PyQt6.QtCore import Qt

# Import 各個功能模組
from modules.kline_module import KLineModule
from modules.revenue_module import RevenueModule
from modules.stock_list_module import StockListModule
from modules.institutional_module import InstitutionalModule
from modules.margin_module import MarginModule
from modules.eps_module import EPSModule
from modules.ratio_module import RatioModule


class SideMenu(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedWidth(70)  # 側邊選單寬度
        self.setStyleSheet("background-color: #111; border-right: 1px solid #222;")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 20, 5, 20)
        layout.setSpacing(15)

        self.button_group = QButtonGroup(self)
        self.button_group.setExclusive(True)

        self.btn_warroom = self._create_menu_btn("戰情", 0)
        self.btn_strategy = self._create_menu_btn("選股", 1)
        self.btn_market = self._create_menu_btn("市場", 2)

        self.btn_warroom.setChecked(True)

        layout.addWidget(self.btn_warroom)
        layout.addWidget(self.btn_strategy)
        layout.addWidget(self.btn_market)
        layout.addStretch()

    def _create_menu_btn(self, text, id):
        btn = QPushButton(text)
        btn.setFixedSize(60, 60)
        btn.setCheckable(True)
        btn.setStyleSheet("""
            QPushButton { 
                background-color: #222; color: #555; border-radius: 8px; 
                font-size: 14px; font-weight: bold; border: 1px solid #333;
            }
            QPushButton:checked { background-color: #00FFFF; color: #000; }
        """)
        self.button_group.addButton(btn, id)
        return btn


class StockWarRoomV3(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("StockWarRoom V3 - 戰情矩陣")
        self.resize(1600, 950)
        self.setStyleSheet("background-color: #000000;")

        self.init_ui()
        self.connect_signals()
        self.load_initial_data()

    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        self.side_menu = SideMenu()
        main_layout.addWidget(self.side_menu)

        self.pages = QStackedWidget()
        self.warroom_page = QWidget()
        warroom_layout = QGridLayout(self.warroom_page)
        warroom_layout.setContentsMargins(4, 4, 4, 4)
        warroom_layout.setSpacing(4)

        # --- 1. 左上：股票清單 ---
        self.list_module = StockListModule()

        # --- 2. 右上：K線模組 ---
        self.kline_module = KLineModule()

        # --- 3. 左下：籌碼面分頁 (法人 + 資券) ---
        self.chips_tabs = self._create_tab_widget()
        self.inst_module = InstitutionalModule()
        self.margin_module = MarginModule()

        self.chips_tabs.addTab(self.inst_module, "三大法人")
        self.chips_tabs.addTab(self.margin_module, "資券變化")

        # --- 4. 右下：基本面分頁 (營收 + EPS + 三率) ---
        self.fund_tabs = self._create_tab_widget()
        self.revenue_module = RevenueModule()
        self.eps_module = EPSModule()
        self.ratio_module = RatioModule()

        self.fund_tabs.addTab(self.revenue_module, "月營收")
        self.fund_tabs.addTab(self.eps_module, "EPS")
        self.fund_tabs.addTab(self.ratio_module, "三率")

        # --- 佈局配置 (Row, Col) ---
        warroom_layout.addWidget(self.list_module, 0, 0)
        warroom_layout.addWidget(self.kline_module, 0, 1)
        warroom_layout.addWidget(self.chips_tabs, 1, 0)
        warroom_layout.addWidget(self.fund_tabs, 1, 1)

        # 比例調整 (左35% 右65% | 上55% 下45%)
        warroom_layout.setColumnStretch(0, 35)
        warroom_layout.setColumnStretch(1, 65)
        warroom_layout.setRowStretch(0, 55)
        warroom_layout.setRowStretch(1, 45)

        self.pages.addWidget(self.warroom_page)
        self.pages.addWidget(QLabel("選股策略頁面", alignment=Qt.AlignmentFlag.AlignCenter))
        self.pages.addWidget(QLabel("市場焦點頁面", alignment=Qt.AlignmentFlag.AlignCenter))

        main_layout.addWidget(self.pages)

    def _create_tab_widget(self):
        """ 統一的 Tab 樣式工廠方法 """
        tabs = QTabWidget()
        tabs.setStyleSheet("""
            QTabWidget::pane { border: 1px solid #333; background: #000; }
            QTabBar::tab { 
                background: #111; color: #888; padding: 6px 20px; 
                border-top: 2px solid transparent; font-weight: bold;
            }
            QTabBar::tab:selected { 
                background: #1A1A1A; color: #00E5FF; 
                border-top: 2px solid #00E5FF; /* 上方亮條 */
            }
            QTabBar::tab:hover { color: #FFF; }
        """)
        return tabs

    def connect_signals(self):
        self.side_menu.button_group.idClicked.connect(self.pages.setCurrentIndex)

        # 綁定所有模組的連動
        self.list_module.stock_selected.connect(self.kline_module.load_stock_data)
        self.list_module.stock_selected.connect(self.inst_module.load_inst_data)
        self.list_module.stock_selected.connect(self.margin_module.load_margin_data)
        self.list_module.stock_selected.connect(self.revenue_module.load_revenue_data)
        self.list_module.stock_selected.connect(self.eps_module.load_eps_data)
        self.list_module.stock_selected.connect(self.ratio_module.load_ratio_data)

    def load_initial_data(self):
        # 🟢 定義 mock_df 讓 list_module 有初始資料
        mock_df = pd.DataFrame([
            {'id': '2330_TW', 'name': '台積電', 'price': 1050, 'pct_5': 2.5},
            {'id': '2317_TW', 'name': '鴻海', 'price': 210.5, 'pct_5': -1.2},
            {'id': '2454_TW', 'name': '聯發科', 'price': 1200, 'pct_5': 0.8},
            {'id': '3008_TW', 'name': '大立光', 'price': 2500, 'pct_5': 3.1}
        ])

        self.list_module.load_data(mock_df)

        # 預設載入第一檔
        if not mock_df.empty:
            fid = mock_df.iloc[0]['id']
            self.kline_module.load_stock_data(fid)
            self.inst_module.load_inst_data(fid)
            self.margin_module.load_margin_data(fid)
            self.revenue_module.load_revenue_data(fid)
            self.eps_module.load_eps_data(fid)
            self.ratio_module.load_ratio_data(fid)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = StockWarRoomV3()
    window.show()
    sys.exit(app.exec())