import sys
import os
from pathlib import Path
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QHBoxLayout,
                             QVBoxLayout, QPushButton, QStackedWidget,
                             QButtonGroup, QGridLayout, QTabWidget,
                             QMessageBox, QProgressDialog, QSizePolicy)
from PyQt6.QtCore import Qt, QTimer
import traceback
def exception_hook(exctype, value, tb):
    print("💥 偵測到未捕獲的錯誤:")
    traceback.print_exception(exctype, value, tb)
    sys.exit(1)
sys.excepthook = exception_hook

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
from modules.settings_module import SettingsModule


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
        self.btn_settings = self._create_menu_btn("設定", 3)

        self.btn_warroom.setChecked(True)

        layout.addWidget(self.btn_warroom)
        layout.addWidget(self.btn_strategy)
        layout.addWidget(self.btn_market)
        layout.addStretch()
        layout.addWidget(self.btn_settings)

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

        # 狀態變數
        self.current_stock_id = None
        self.current_stock_name = ""

        self.init_ui()
        self.connect_signals()

        # 延遲載入初始資料 (並嘗試選取第一檔)
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

        # 建立 Tab
        self.chips_tabs = self._create_tab_widget()
        self.chips_tabs.addTab(self.inst_module, "三大法人")
        self.chips_tabs.addTab(self.margin_module, "資券變化")
        self.chips_tabs.currentChanged.connect(self.on_tab_changed)

        self.fund_tabs = self._create_tab_widget()
        self.fund_tabs.addTab(self.revenue_module, "月營收")
        self.fund_tabs.addTab(self.eps_module, "EPS")
        self.fund_tabs.addTab(self.ratio_module, "三率")
        self.fund_tabs.currentChanged.connect(self.on_tab_changed)

        # Layout 設定
        for widget in [self.list_module, self.kline_module, self.chips_tabs, self.fund_tabs]:
            widget.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Ignored)

        warroom_layout.addWidget(self.list_module, 0, 0)
        warroom_layout.addWidget(self.kline_module, 0, 1)
        warroom_layout.addWidget(self.chips_tabs, 1, 0)
        warroom_layout.addWidget(self.fund_tabs, 1, 1)

        warroom_layout.setColumnStretch(0, 50)
        warroom_layout.setColumnStretch(1, 50)
        warroom_layout.setRowStretch(0, 55)
        warroom_layout.setRowStretch(1, 45)

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

        # 監聽頁面切換，實現自動重繪
        self.pages.currentChanged.connect(self.on_page_changed)

        self.list_module.stock_selected.connect(self.on_stock_changed)
        self.market_page.stock_clicked_signal.connect(self.on_stock_changed)
        self.strategy_page.stock_clicked_signal.connect(self.on_strategy_stock_clicked)
        self.strategy_page.request_add_watchlist.connect(self.on_add_watchlist_request)

    def on_page_changed(self, index):
        """當頁面切換時觸發"""
        # 如果切換回戰情室 (index 0) 且有當前股票，強制刷新 K 線
        # 這解決了「下載完資料後，回到戰情室 K 線不會更新」的問題
        if index == 0:
            # 🔥 修正點：只有在當前沒股票時，才自動選第一支
            # 如果是從選股頁跳過來的，current_stock_id 已經設定好了，這裡不重複執行
            if not self.current_stock_id:
                self.auto_select_first_stock()

    def on_stock_changed(self, full_stock_id):
        # 🔥 [防閃退關鍵] 如果股票代號跟上次一樣，就不要重跑，防止無限循環觸發
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
        self.on_stock_changed(stock_id_full)
        self.side_menu.button_group.button(0).setChecked(True)
        self.pages.setCurrentIndex(0)

    def on_add_watchlist_request(self, stock_id, group_name):
        self.list_module.add_stock_to_group(stock_id, group_name)

    def load_initial_data(self):
        # 重新整理列表
        self.list_module.refresh_table()
        # 稍後自動選取第一支，解決「預設要畫第一筆資料」的需求
        QTimer.singleShot(500, self.auto_select_first_stock)

    def auto_select_first_stock(self):
        """嘗試自動選取列表中的第一支股票"""
        try:
            # 嘗試存取 list_module 的 table (假設名稱為 table 或 stock_table)
            table = getattr(self.list_module, 'table', None)
            if not table:
                table = getattr(self.list_module, 'stock_table', None)

            if table and table.rowCount() > 0:
                table.selectRow(0)
                # 模擬點擊或直接觸發邏輯，這裡假設 list_module 有處理 selection change
                # 如果無法觸發，我們手動獲取第一列的 ID
                item = table.item(0, 0)  # 假設 ID 在第一欄
                if item:
                    # 這裡只能猜測 ID 格式，通常 list module 會發送訊號
                    # 我們這裡依賴 list_module 自己的 selection behavior
                    # 或者我們可以呼叫 list_module 的某個方法
                    pass
                print("DEBUG: 已自動選取第一列股票")
        except Exception as e:
            print(f"DEBUG: 自動選取失敗: {e}")

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