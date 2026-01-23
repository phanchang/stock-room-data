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
        # 🟢 設定一個較大的初始解析度，確保 2x2 矩陣有空間
        self.resize(1600, 950)
        self.setStyleSheet("background-color: #000000;")

        self.init_ui()
        self.connect_signals()
        self.load_initial_data()

    def init_ui(self):
        # 設定中央區塊
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # 1. 整個視窗的主要佈局 (水平排列：左邊是選單，右邊是內容頁)
        main_layout = QHBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # --- 2. 加入側邊選單 (關鍵：要放在 Pages 之前) ---
        self.side_menu = SideMenu()
        main_layout.addWidget(self.side_menu)

        # --- 3. 建立右側堆疊頁面 ---
        self.pages = QStackedWidget()

        # ==========================================
        # Page 0: 戰情室 (原本的 2x2 矩陣佈局)
        # ==========================================
        self.warroom_page = QWidget()
        warroom_layout = QGridLayout(self.warroom_page)
        warroom_layout.setContentsMargins(4, 4, 4, 4)
        warroom_layout.setSpacing(4)

        # 實例化各個模組
        self.list_module = StockListModule()
        self.kline_module = KLineModule()
        self.inst_module = InstitutionalModule()
        self.margin_module = MarginModule()
        self.revenue_module = RevenueModule()
        self.eps_module = EPSModule()
        self.ratio_module = RatioModule()

        # 建立 Tab 分頁 (左下 & 右下)
        self.chips_tabs = self._create_tab_widget()
        self.chips_tabs.addTab(self.inst_module, "三大法人")
        self.chips_tabs.addTab(self.margin_module, "資券變化")

        self.fund_tabs = self._create_tab_widget()
        self.fund_tabs.addTab(self.revenue_module, "月營收")
        self.fund_tabs.addTab(self.eps_module, "EPS")
        self.fund_tabs.addTab(self.ratio_module, "三率")

        # 放入 Grid (位置配置)
        warroom_layout.addWidget(self.list_module, 0, 0)  # 左上
        warroom_layout.addWidget(self.kline_module, 0, 1)  # 右上
        warroom_layout.addWidget(self.chips_tabs, 1, 0)  # 左下
        warroom_layout.addWidget(self.fund_tabs, 1, 1)  # 右下

        # 設定比例 (左35% 右65% | 上45% 下55%)
        warroom_layout.setColumnStretch(0, 35)
        warroom_layout.setColumnStretch(1, 65)
        warroom_layout.setRowStretch(0, 45)
        warroom_layout.setRowStretch(1, 55)

        # 將戰情室頁面加入 Stack
        self.pages.addWidget(self.warroom_page)

        # ==========================================
        # Page 1: 選股策略 (本次新增)
        # ==========================================
        self.strategy_page = StrategyModule()
        self.pages.addWidget(self.strategy_page)

        # ==========================================
        # Page 2: 市場焦點 (ETF)
        # ==========================================
        self.market_page = ActiveETFModule()
        self.pages.addWidget(self.market_page)

        # --- 4. 將堆疊頁面加入主佈局 ---
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

        # 1. 股票清單 (左上) 連動其他模組
        self.list_module.stock_selected.connect(self.kline_module.load_stock_data)
        self.list_module.stock_selected.connect(self.inst_module.load_inst_data)
        self.list_module.stock_selected.connect(self.margin_module.load_margin_data)
        self.list_module.stock_selected.connect(self.revenue_module.load_revenue_data)
        self.list_module.stock_selected.connect(self.eps_module.load_eps_data)
        self.list_module.stock_selected.connect(self.ratio_module.load_ratio_data)

        # 🟢 2. 新增：市場焦點 (ETF) 連動其他模組
        # 當在 ETF 頁面點擊股票時，自動更新戰情室的數據
        self.market_page.stock_clicked_signal.connect(self.kline_module.load_stock_data)
        self.market_page.stock_clicked_signal.connect(self.inst_module.load_inst_data)
        self.market_page.stock_clicked_signal.connect(self.margin_module.load_margin_data)
        self.market_page.stock_clicked_signal.connect(self.revenue_module.load_revenue_data)
        self.market_page.stock_clicked_signal.connect(self.eps_module.load_eps_data)
        self.market_page.stock_clicked_signal.connect(self.ratio_module.load_ratio_data)

        # 並且自動切回戰情室分頁 (Page 0)，讓使用者看到詳細數據 (可選)
        # self.market_page.stock_clicked_signal.connect(lambda: self.pages.setCurrentIndex(0))
        # 3. 策略頁面連動
        self.strategy_page.stock_clicked_signal.connect(self.on_strategy_stock_clicked)
        # 串接策略頁面的「加入自選」請求
        self.strategy_page.request_add_watchlist.connect(self.on_add_watchlist_request)

    def on_add_watchlist_request(self, stock_id, group_name):
        # 呼叫 StockListModule 的方法
        # 注意：您需要在 StockListModule 實作 add_stock_by_code(stock_id, group_name)
        self.list_module.add_stock_to_group(stock_id, group_name)

    def on_strategy_stock_clicked(self, stock_id_full):
        """ 策略選股點擊後的行為 """
        self.kline_module.load_stock_data(stock_id_full)  # K線
        self.inst_module.load_inst_data(stock_id_full)  # 三大法人

        # --- 補上這四行 ---
        self.margin_module.load_margin_data(stock_id_full)  # 資券
        self.revenue_module.load_revenue_data(stock_id_full)  # 月營收
        self.eps_module.load_eps_data(stock_id_full)  # EPS
        self.ratio_module.load_ratio_data(stock_id_full)  # 三率

        # 2. 自動切換回「戰情 (Page 0)」頁面查看詳細圖表
        self.side_menu.button_group.button(0).setChecked(True)
        self.pages.setCurrentIndex(0)

    def load_initial_data(self):
        # 🟢 修正：補齊 StockListModule 所需的所有欄位，避免 KeyError

        # 直接觸發一次列表刷新 (這會去抓真實資料)
        self.list_module.refresh_table()

        # 預設載入清單中的第一檔 (如果有資料的話)
        # 這裡我們稍微改寫一下，讓它自動去抓 Table 第一列的代號
        if self.list_module.table.rowCount() > 0:
            item = self.list_module.table.item(0, 0)
            if item:
                code = item.text()
                market = item.data(Qt.ItemDataRole.UserRole)
                fid = f"{code}_{market}"

                print(f"🚀 [系統啟動] 預設載入: {fid}")
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