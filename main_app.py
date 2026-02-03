import sys
import os
from pathlib import Path
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QHBoxLayout,
                             QVBoxLayout, QPushButton, QStackedWidget,
                             QButtonGroup, QGridLayout, QTabWidget,
                             QMessageBox, QProgressDialog, QSizePolicy)
from PyQt6.QtCore import Qt, QTimer

# 設定模組搜尋路徑
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

# Import Utils
from utils.quote_worker import QuoteWorker

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
        self.setFixedWidth(70)
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
        self.setWindowTitle("StockWarRoom V3 - 戰情矩陣 (極速版)")
        self.resize(1600, 950)
        self.setStyleSheet("""
            QMainWindow { background-color: #000000; }
            QMessageBox { background-color: #222; color: white; }
            QPushButton { background-color: #444; color: white; border: 1px solid #555; padding: 5px; }
        """)

        # 1. 建立共享的 Worker
        self.shared_worker = QuoteWorker(self)
        # 🔥 [修正] 註解掉這行，讓 Driver 不會一開程式就跑出來
        # self.shared_worker.start()

        # 狀態變數
        self.current_stock_id = None
        self.current_stock_name = ""

        self.init_ui()
        self.connect_signals()

        # 延遲載入初始資料
        QTimer.singleShot(500, self.load_initial_data)

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

        # 建立模組
        self.list_module = StockListModule(shared_worker=self.shared_worker)
        self.kline_module = KLineModule(shared_worker=self.shared_worker)

        self.inst_module = InstitutionalModule()
        self.margin_module = MarginModule()
        self.revenue_module = RevenueModule()
        self.eps_module = EPSModule()
        self.ratio_module = RatioModule()

        # 建立 Tab (連接切換事件，實現 Lazy Loading)
        self.chips_tabs = self._create_tab_widget()
        self.chips_tabs.addTab(self.inst_module, "三大法人")
        self.chips_tabs.addTab(self.margin_module, "資券變化")
        self.chips_tabs.currentChanged.connect(self.on_tab_changed)  # Lazy Load Trigger

        self.fund_tabs = self._create_tab_widget()
        self.fund_tabs.addTab(self.revenue_module, "月營收")
        self.fund_tabs.addTab(self.eps_module, "EPS")
        self.fund_tabs.addTab(self.ratio_module, "三率")
        self.fund_tabs.currentChanged.connect(self.on_tab_changed)  # Lazy Load Trigger

        # Layout 設定
        for widget in [self.list_module, self.kline_module, self.chips_tabs, self.fund_tabs]:
            widget.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Ignored)

        warroom_layout.addWidget(self.list_module, 0, 0)
        warroom_layout.addWidget(self.kline_module, 0, 1)
        warroom_layout.addWidget(self.chips_tabs, 1, 0)
        warroom_layout.addWidget(self.fund_tabs, 1, 1)

        warroom_layout.setColumnStretch(0, 50)
        warroom_layout.setColumnStretch(1, 50)
        warroom_layout.setRowStretch(0, 55)  # K線圖高一點
        warroom_layout.setRowStretch(1, 45)

        self.pages.addWidget(self.warroom_page)

        # Page 1 & 2
        self.strategy_page = StrategyModule()
        self.pages.addWidget(self.strategy_page)
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
                background: #1A1A1A; color: #00E5FF; border-top: 2px solid #00E5FF;
            }
            QTabBar::tab:hover { color: #FFF; }
        """)
        return tabs

    def connect_signals(self):
        self.side_menu.button_group.idClicked.connect(self.pages.setCurrentIndex)
        self.list_module.stock_selected.connect(self.on_stock_changed)
        self.market_page.stock_clicked_signal.connect(self.on_stock_changed)
        self.strategy_page.stock_clicked_signal.connect(self.on_strategy_stock_clicked)
        self.strategy_page.request_add_watchlist.connect(self.on_add_watchlist_request)

    def on_stock_changed(self, full_stock_id):
        """核心優化：切換股票時，只載入最必要的 K 線，其他模組採用 Lazy Load"""
        self.current_stock_id = full_stock_id
        clean_id = full_stock_id.split('_')[0]

        # 嘗試從 DB 獲取名稱
        stock_name = ""
        if hasattr(self.list_module, 'stock_db'):
            info = self.list_module.stock_db.get(clean_id)
            if info: stock_name = info.get('name', '')
        self.current_stock_name = stock_name

        print(f"DEBUG: 切換股票 {full_stock_id} ({stock_name}) - 啟動極速載入")

        # 1. 優先載入 K 線 (這是最重要的)
        if hasattr(self, 'kline_module'):
            self.kline_module.load_stock_data(full_stock_id, stock_name)

        # 2. 載入「當前可見」的 Tab 數據
        self.update_visible_tabs()

    def update_visible_tabs(self):
        """只更新當前顯示的 Tab，避免一次載入所有數據導致卡頓"""
        if not self.current_stock_id: return

        # 處理 Chips Tabs (左下)
        current_chips = self.chips_tabs.currentWidget()
        if current_chips == self.inst_module:
            self.inst_module.load_inst_data(self.current_stock_id, self.current_stock_name)
        elif current_chips == self.margin_module:
            self.margin_module.load_margin_data(self.current_stock_id, self.current_stock_name)

        # 處理 Fund Tabs (右下)
        current_fund = self.fund_tabs.currentWidget()
        if current_fund == self.revenue_module:
            self.revenue_module.load_revenue_data(self.current_stock_id, self.current_stock_name)
        elif current_fund == self.eps_module:
            self.eps_module.load_eps_data(self.current_stock_id, self.current_stock_name)
        elif current_fund == self.ratio_module:
            self.ratio_module.load_ratio_data(self.current_stock_id, self.current_stock_name)

    def on_tab_changed(self, index):
        """當使用者切換 Tab 時，才去載入該 Tab 的數據"""
        self.update_visible_tabs()

    def on_strategy_stock_clicked(self, stock_id_full):
        self.on_stock_changed(stock_id_full)
        self.side_menu.button_group.button(0).setChecked(True)
        self.pages.setCurrentIndex(0)

    def on_add_watchlist_request(self, stock_id, group_name):
        self.list_module.add_stock_to_group(stock_id, group_name)

    def load_initial_data(self):
        self.list_module.refresh_table()
        #if self.list_module.table.rowCount() > 0:
        #    item = self.list_module.table.item(0, 0)
        #    if item:
        #        code = item.text()
        #        market = item.data(Qt.ItemDataRole.UserRole)
        #        fid = f"{code}_{market}"
        #        self.on_stock_changed(fid)

    def closeEvent(self, event):
        reply = QMessageBox.question(self, '確認退出', '確定要關閉系統嗎？',
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            if self.shared_worker.isRunning():
                self.shared_worker.stop()
                self.shared_worker.wait(1000)
            import matplotlib.pyplot as plt
            plt.close('all')
            event.accept()
        else:
            event.ignore()


if __name__ == "__main__":
    os.environ["QT_AUTO_SCREEN_SCALE_FACTOR"] = "1"
    app = QApplication(sys.argv)
    window = StockWarRoomV3()
    window.showMaximized()
    sys.exit(app.exec())