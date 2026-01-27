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
from modules.active_etf_module import ActiveETFModule
from modules.strategy_module import StrategyModule


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

        # --- 側邊選單 ---
        self.side_menu = SideMenu()
        main_layout.addWidget(self.side_menu)

        # --- 右側堆疊頁面 ---
        self.pages = QStackedWidget()

        # Page 0: 戰情室
        self.warroom_page = QWidget()
        warroom_layout = QGridLayout(self.warroom_page)
        warroom_layout.setContentsMargins(4, 4, 4, 4)
        warroom_layout.setSpacing(4)

        self.list_module = StockListModule()
        self.kline_module = KLineModule()
        self.inst_module = InstitutionalModule()
        self.margin_module = MarginModule()
        self.revenue_module = RevenueModule()
        self.eps_module = EPSModule()
        self.ratio_module = RatioModule()

        self.chips_tabs = self._create_tab_widget()
        self.chips_tabs.addTab(self.inst_module, "三大法人")
        self.chips_tabs.addTab(self.margin_module, "資券變化")

        self.fund_tabs = self._create_tab_widget()
        self.fund_tabs.addTab(self.revenue_module, "月營收")
        self.fund_tabs.addTab(self.eps_module, "EPS")
        self.fund_tabs.addTab(self.ratio_module, "三率")

        warroom_layout.addWidget(self.list_module, 0, 0)
        warroom_layout.addWidget(self.kline_module, 0, 1)
        warroom_layout.addWidget(self.chips_tabs, 1, 0)
        warroom_layout.addWidget(self.fund_tabs, 1, 1)

        warroom_layout.setColumnStretch(0, 35)
        warroom_layout.setColumnStretch(1, 65)
        warroom_layout.setRowStretch(0, 45)
        warroom_layout.setRowStretch(1, 55)

        self.pages.addWidget(self.warroom_page)

        # Page 1: 選股策略
        self.strategy_page = StrategyModule()
        self.pages.addWidget(self.strategy_page)

        # Page 2: 市場焦點
        self.market_page = ActiveETFModule()
        self.pages.addWidget(self.market_page)

        main_layout.addWidget(self.pages)

    def _create_tab_widget(self):
        tabs = QTabWidget()
        tabs.setStyleSheet("""
            QTabWidget::pane { border: 1px solid #333; background: #000; }
            QTabBar::tab { 
                background: #111; color: #888; padding: 6px 20px; 
                border-top: 2px solid transparent; font-weight: bold;
            }
            QTabBar::tab:selected { 
                background: #1A1A1A; color: #00E5FF; 
                border-top: 2px solid #00E5FF;
            }
            QTabBar::tab:hover { color: #FFF; }
        """)
        return tabs

    def connect_signals(self):
        # 頁面切換
        self.side_menu.button_group.idClicked.connect(self.pages.setCurrentIndex)

        # 🔥 [修改] 統一使用 on_stock_changed 來處理所有選股連動
        # 這樣可以在這裡統一查詢中文名稱，再傳給 KLineModule
        self.list_module.stock_selected.connect(self.on_stock_changed)

        # 市場焦點連動
        self.market_page.stock_clicked_signal.connect(self.on_stock_changed)

        # 策略頁面連動
        self.strategy_page.stock_clicked_signal.connect(self.on_strategy_stock_clicked)
        self.strategy_page.request_add_watchlist.connect(self.on_add_watchlist_request)

    def get_stock_name(self, full_stock_id):
        """ 🔥 [新增] 輔助函式：從 StockListModule 的 DataFrame 查中文名稱 """
        try:
            stock_id = full_stock_id.split('_')[0]
            # 確保 list_module 已經載入過資料
            if hasattr(self.list_module, 'stock_list_df') and not self.list_module.stock_list_df.empty:
                df = self.list_module.stock_list_df
                if stock_id in df.index:
                    return df.loc[stock_id, 'name']
        except Exception:
            pass
        return ""

    def on_stock_changed(self, full_stock_id):
        """ 🔥 [新增] 統一處理選股邏輯 """
        # 1. 取得股票名稱
        stock_name = self.get_stock_name(full_stock_id)

        # 2. 通知 KLine (傳入 ID 和 Name)
        self.kline_module.load_stock_data(full_stock_id, stock_name)

        # 3. 通知其他模組 (只需 ID)
        self.inst_module.load_inst_data(full_stock_id)
        self.margin_module.load_margin_data(full_stock_id)
        self.revenue_module.load_revenue_data(full_stock_id)
        self.eps_module.load_eps_data(full_stock_id)
        self.ratio_module.load_ratio_data(full_stock_id)

    def on_strategy_stock_clicked(self, stock_id_full):
        """ 策略選股點擊後的行為 """
        # 直接呼叫統一介面，保持行為一致
        self.on_stock_changed(stock_id_full)

        # 自動切換回「戰情 (Page 0)」
        self.side_menu.button_group.button(0).setChecked(True)
        self.pages.setCurrentIndex(0)

    def on_add_watchlist_request(self, stock_id, group_name):
        self.list_module.add_stock_to_group(stock_id, group_name)

    def load_initial_data(self):
        # 觸發列表刷新
        self.list_module.refresh_table()

        # 預設載入清單中的第一檔
        if self.list_module.table.rowCount() > 0:
            item = self.list_module.table.item(0, 0)
            if item:
                code = item.text()
                market = item.data(Qt.ItemDataRole.UserRole)
                fid = f"{code}_{market}"

                # 取得名稱 (從列表的第二欄 '名稱' 抓取最準)
                name_item = self.list_module.table.item(0, 1)
                name = name_item.text() if name_item else ""

                print(f"🚀 [系統啟動] 預設載入: {fid} {name}")

                # 🔥 [修改] 呼叫統一介面 (其實可以直接 call on_stock_changed，但為了明確傳入 name，手動 call 也行)
                self.kline_module.load_stock_data(fid, name)

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