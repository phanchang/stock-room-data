import sys
import os

# 系統環境設定完畢後，再 import 你的自訂模組
from pathlib import Path
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QStackedWidget, QGridLayout,
                             QSizePolicy, QFrame, QPushButton, QLabel, QTabWidget,QMessageBox)
from PyQt6.QtCore import Qt, pyqtSignal,QTimer
import traceback

from modules.sector_dashboard import SectorDashboard

def exception_hook(exctype, value, tb):
    print("💥 偵測到未捕獲的錯誤:")
    traceback.print_exception(exctype, value, tb)
    sys.exit(1)


sys.excepthook = exception_hook

current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

from utils.quote_worker import QuoteWorker

from modules.kline_module import KLineModule
from modules.revenue_module import RevenueModule
from modules.stock_list_module import StockListModule
from modules.institutional_module import InstitutionalModule
from modules.margin_module import MarginModule
from modules.eps_module import EPSModule
from modules.ratio_module import RatioModule
from modules.active_etf_module import ActiveETFModule
from modules.strategy_module import StrategyModule
from modules.settings_module import SettingsModule


class SideMenu(QFrame):
    menu_selected = pyqtSignal(int)

    def __init__(self):
        super().__init__()
        self.setFixedWidth(60)
        self.setStyleSheet("""
            QFrame {
                background-color: #1e1e1e;
                border-right: 1px solid #333333;
            }
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 10, 0, 10)
        layout.setSpacing(10)

        # 加入板塊按鈕
        self.btn_warroom = self._create_nav_btn("戰情室", "chart-line", True)
        self.btn_strategy = self._create_nav_btn("選股", "filter", False)
        self.btn_market = self._create_nav_btn("市場", "globe", False)
        self.btn_sector = self._create_nav_btn("板塊", "th-large", False)  # <-- 新增板塊按鈕
        self.btn_settings = self._create_nav_btn("設定", "cog", False)

        layout.addWidget(self.btn_warroom)
        layout.addWidget(self.btn_strategy)
        layout.addWidget(self.btn_market)
        layout.addWidget(self.btn_sector)  # <-- 加入 Layout

        layout.addStretch()
        layout.addWidget(self.btn_settings)

        # 將板塊按鈕加入列管，才能正常切換 highlight 狀態
        self.nav_btns = [self.btn_warroom, self.btn_strategy, self.btn_market, self.btn_sector, self.btn_settings]

        # 綁定點擊事件與對應的 Page Index
        self.btn_warroom.clicked.connect(lambda: self._on_btn_clicked(self.btn_warroom, 0))
        self.btn_strategy.clicked.connect(lambda: self._on_btn_clicked(self.btn_strategy, 1))
        self.btn_market.clicked.connect(lambda: self._on_btn_clicked(self.btn_market, 2))
        self.btn_settings.clicked.connect(lambda: self._on_btn_clicked(self.btn_settings, 3))
        self.btn_sector.clicked.connect(lambda: self._on_btn_clicked(self.btn_sector, 4))  # <-- 對應 Page 4

    def _create_nav_btn(self, text, icon_name, is_active=False):
        btn = QPushButton(text)
        btn.setCheckable(True)
        btn.setChecked(is_active)
        btn.setCursor(Qt.CursorShape.PointingHandCursor)
        btn.setFixedHeight(50)

        # 深色專業風格設定
        btn.setStyleSheet("""
            QPushButton {
                background-color: transparent;
                color: #888888;
                border: none;
                border-left: 3px solid transparent;
                font-size: 14px;
                font-weight: bold;
            }
            QPushButton:hover {
                color: #ffffff;
                background-color: #2a2a2a;
            }
            QPushButton:checked {
                color: #ffffff;
                background-color: #2a2a2a;
                border-left: 3px solid #007acc;
            }
        """)
        return btn

    def _on_btn_clicked(self, clicked_btn, index):
        for btn in self.nav_btns:
            if btn != clicked_btn:
                btn.setChecked(False)
        clicked_btn.setChecked(True)
        self.menu_selected.emit(index)


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

        self.shared_worker = QuoteWorker(self)

        self.current_stock_id = None
        self.current_stock_name = ""

        self.init_ui()
        self.connect_signals()

        QTimer.singleShot(500, self.load_initial_data)

    def init_ui(self):
        """建構三窗格 UI 佈局"""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        main_layout = QHBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        self.side_menu = SideMenu()
        main_layout.addWidget(self.side_menu)

        self.pages = QStackedWidget()

        # Page 0: 戰情室
        self.warroom_page = QWidget()
        warroom_layout = QGridLayout(self.warroom_page)
        warroom_layout.setContentsMargins(4, 4, 4, 4)
        warroom_layout.setSpacing(4)

        self.list_module = StockListModule(shared_worker=self.shared_worker)
        self.kline_module = KLineModule(shared_worker=self.shared_worker)

        self.inst_module = InstitutionalModule()
        self.margin_module = MarginModule()
        self.revenue_module = RevenueModule()
        self.eps_module = EPSModule()
        self.ratio_module = RatioModule()

        self.chips_tabs = self._create_tab_widget()
        self.chips_tabs.addTab(self.inst_module, "三大法人")
        self.chips_tabs.addTab(self.margin_module, "資券變化")
        self.chips_tabs.currentChanged.connect(self.on_tab_changed)

        self.fund_tabs = self._create_tab_widget()
        self.fund_tabs.addTab(self.revenue_module, "月營收")
        self.fund_tabs.addTab(self.eps_module, "EPS")
        self.fund_tabs.addTab(self.ratio_module, "三率")
        self.fund_tabs.currentChanged.connect(self.on_tab_changed)

        for widget in [self.list_module, self.kline_module, self.chips_tabs, self.fund_tabs]:
            widget.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Ignored)

        warroom_layout.addWidget(self.list_module, 0, 0)
        warroom_layout.addWidget(self.kline_module, 0, 1)
        warroom_layout.addWidget(self.chips_tabs, 1, 0)
        warroom_layout.addWidget(self.fund_tabs, 1, 1)

        warroom_layout.setColumnStretch(0, 50)
        warroom_layout.setColumnStretch(1, 50)
        warroom_layout.setRowStretch(0, 48)
        warroom_layout.setRowStretch(1, 52)

        self.pages.addWidget(self.warroom_page)

        # Page 1: 選股
        self.strategy_page = StrategyModule()
        self.pages.addWidget(self.strategy_page)

        # Page 2: 市場
        self.market_page = ActiveETFModule()
        self.pages.addWidget(self.market_page)

        # Page 3: 設定
        self.settings_page = SettingsModule()
        self.pages.addWidget(self.settings_page)

        # Page 4: 板塊輪動 (新增的 V9.0 模組)
        import os
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.sector_dashboard = SectorDashboard(project_root=current_dir)
        self.pages.addWidget(self.sector_dashboard)

        # 暫時隱藏雙擊切換功能，待 K 線引擎通暢後再來銜接
        self.sector_dashboard.stock_double_clicked.connect(self.on_strategy_stock_clicked)
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
        #self.side_menu.button_group.idClicked.connect(self.pages.setCurrentIndex)
        self.side_menu.menu_selected.connect(self.pages.setCurrentIndex)
        self.pages.currentChanged.connect(self.on_page_changed)

        self.list_module.stock_selected.connect(self.on_stock_changed)
        self.market_page.stock_clicked_signal.connect(self.on_stock_changed)
        self.strategy_page.stock_clicked_signal.connect(self.on_strategy_stock_clicked)
        self.strategy_page.request_add_watchlist.connect(self.on_add_watchlist_request)
        # 👇 新增這行，接通 K 線模組的加入清單訊號！
        self.kline_module.request_add_watchlist.connect(self.on_add_watchlist_request)

    def on_page_changed(self, index):
        if index == 0:
            self.list_module.refresh_table()
            if self.current_stock_id:
                print(f"DEBUG: 保持連動股票 {self.current_stock_id}，並重新載入最新數據")
                self.kline_module.load_stock_data(self.current_stock_id, self.current_stock_name)
                self.update_visible_tabs()
            else:
                print("DEBUG: 目前無選定股票，自動選取第一筆")
                self.auto_select_first_stock()

    def on_stock_changed(self, full_stock_id):
        if full_stock_id == self.current_stock_id and self.current_stock_id is not None:
            return
        self.current_stock_id = full_stock_id
        clean_id = full_stock_id.split('_')[0]

        stock_name = ""
        if hasattr(self.list_module, 'stock_db'):
            info = self.list_module.stock_db.get(clean_id)
            if info: stock_name = info.get('name', '')
        self.current_stock_name = stock_name

        if hasattr(self, 'kline_module'):
            self.kline_module.load_stock_data(full_stock_id, stock_name)

        self.update_visible_tabs()

    def update_visible_tabs(self):
        if not self.current_stock_id: return

        current_chips = self.chips_tabs.currentWidget()
        if current_chips == self.inst_module:
            self.inst_module.load_inst_data(self.current_stock_id, self.current_stock_name)
        elif current_chips == self.margin_module:
            self.margin_module.load_margin_data(self.current_stock_id, self.current_stock_name)

        current_fund = self.fund_tabs.currentWidget()
        if current_fund == self.revenue_module:
            self.revenue_module.load_revenue_data(self.current_stock_id, self.current_stock_name)
        elif current_fund == self.eps_module:
            self.eps_module.load_eps_data(self.current_stock_id, self.current_stock_name)
        elif current_fund == self.ratio_module:
            self.ratio_module.load_ratio_data(self.current_stock_id, self.current_stock_name)

    def on_tab_changed(self, index):
        self.update_visible_tabs()

    def on_strategy_stock_clicked(self, stock_id_full):
        """當選股分頁或板塊雙擊股票時"""
        self.current_stock_id = stock_id_full
        clean_id = stock_id_full.split('_')[0].split('.')[0] # 兼顧 2330_TW 或是 2330.TW 格式

        stock_name = ""
        if hasattr(self.list_module, 'stock_db'):
            info = self.list_module.stock_db.get(clean_id)
            if info: stock_name = info.get('name', '')
        self.current_stock_name = stock_name

        # 背景靜默更新戰情室與擴充 K 線圖
        if hasattr(self, 'kline_module'):
            self.kline_module.load_stock_data(stock_id_full, stock_name)
        self.update_visible_tabs()

        # 🔥 跳轉邏輯修復：通知左側選單點擊了第一個按鈕 (Index 0 = 戰情室)
        self.side_menu._on_btn_clicked(self.side_menu.btn_warroom, 0)

    def on_add_watchlist_request(self, stock_id, group_name):
        self.list_module.add_stock_to_group(stock_id, group_name)

    def load_initial_data(self):
        self.list_module.refresh_table()
        QTimer.singleShot(500, self.auto_select_first_stock)

    def auto_select_first_stock(self):
        try:
            if hasattr(self, 'list_module') and self.list_module:
                self.list_module.force_trigger_first_selection()
            else:
                print("DEBUG: 找不到 list_module，無法自動選取")
        except Exception as e:
            print(f"💥 自動選取失敗: {e}")

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