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
                             QLabel, QButtonGroup, QGridLayout, QTabWidget,
                             QMessageBox, QProgressDialog)
from PyQt6.QtCore import Qt, QTimer

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
        self.setWindowTitle("StockWarRoom V3 - 戰情矩陣")
        self.resize(1600, 950)
        self.setStyleSheet("""
            QMainWindow { background-color: #000000; }
            QMessageBox { background-color: #222; color: white; }
            QMessageBox QLabel { color: white; }
            QPushButton { background-color: #444; color: white; border: 1px solid #555; padding: 5px; }
        """)

        # 1. 🔥 建立唯一的報價引擎 (核心大腦)
        self.shared_worker = QuoteWorker(self)
        self.shared_worker.start()

        # 2. 初始化 UI (並將大腦傳遞給器官)
        self.init_ui()

        # 3. 連接信號與槽
        self.connect_signals()

        # 4. 延遲載入初始資料 (避免 UI 尚未繪製完成就大量運算導致卡頓)
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

        # 🔥🔥🔥 [絕對修正] 傳入 shared_worker 給 UI 元件 🔥🔥🔥
        # 這是解決 StockList 與 KLine 不更新、不連動的關鍵
        self.list_module = StockListModule(shared_worker=self.shared_worker)
        self.kline_module = KLineModule(shared_worker=self.shared_worker)

        # 其他靜態資料模組
        self.inst_module = InstitutionalModule()
        self.margin_module = MarginModule()
        self.revenue_module = RevenueModule()
        self.eps_module = EPSModule()
        self.ratio_module = RatioModule()

        # 建立 Tab
        self.chips_tabs = self._create_tab_widget()
        self.chips_tabs.addTab(self.inst_module, "三大法人")
        self.chips_tabs.addTab(self.margin_module, "資券變化")

        self.fund_tabs = self._create_tab_widget()
        self.fund_tabs.addTab(self.revenue_module, "月營收")
        self.fund_tabs.addTab(self.eps_module, "EPS")
        self.fund_tabs.addTab(self.ratio_module, "三率")

        # 加入 Layout
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

    def closeEvent(self, event):
        reply = QMessageBox.question(self, '確認退出', '確定要關閉系統嗎？',
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)

        if reply == QMessageBox.StandardButton.Yes:
            progress = QProgressDialog("正在安全停止引擎...", None, 0, 0, self)
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.show()
            QApplication.processEvents()

            # 🔥 強制停止 Shared Worker
            if self.shared_worker.isRunning():
                self.shared_worker.stop()
                self.shared_worker.wait(1000)

            # 關閉 Matplotlib 資源
            import matplotlib.pyplot as plt
            plt.close('all')
            event.accept()
        else:
            event.ignore()

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

        # 選股連動
        self.list_module.stock_selected.connect(self.on_stock_changed)
        self.market_page.stock_clicked_signal.connect(self.on_stock_changed)
        self.strategy_page.stock_clicked_signal.connect(self.on_strategy_stock_clicked)
        self.strategy_page.request_add_watchlist.connect(self.on_add_watchlist_request)

    def get_stock_name(self, full_stock_id):
        try:
            stock_id = full_stock_id.split('_')[0]
            if hasattr(self.list_module, 'stock_db') and self.list_module.stock_db:
                data = self.list_module.stock_db.get(stock_id)
                if data: return data.get('name', '')
        except Exception:
            pass
        return ""

    def on_stock_changed(self, full_stock_id):
        """ 🔥 統一處理選股邏輯，並傳遞股票名稱給所有模組 """

        # 1. 解析代號與名稱
        stock_id = full_stock_id  # 例如 "2330_TW"
        clean_id = stock_id.split('_')[0]
        stock_name = ""

        # 從 StockList 的資料庫中查找名稱
        if hasattr(self, 'list_module') and hasattr(self.list_module, 'stock_db'):
            stock_info = self.list_module.stock_db.get(clean_id)
            if stock_info:
                stock_name = stock_info.get('name', '')

        print(f"DEBUG: 切換股票 {stock_id} ({stock_name})")

        # 2. 通知 KLine (這會觸發 Worker 去抓最新報價)
        if hasattr(self, 'kline_module'):
            self.kline_module.load_stock_data(stock_id, stock_name)

        # 3. 通知各個分析分頁 (依序傳入 ID 與 名稱)
        if hasattr(self, 'inst_module'):
            self.inst_module.load_inst_data(stock_id, stock_name)

        if hasattr(self, 'margin_module'):
            self.margin_module.load_margin_data(stock_id, stock_name)

        if hasattr(self, 'revenue_module'):
            self.revenue_module.load_revenue_data(stock_id, stock_name)

        if hasattr(self, 'eps_module'):
            self.eps_module.load_eps_data(stock_id, stock_name)

        if hasattr(self, 'ratio_module'):
            self.ratio_module.load_ratio_data(stock_id, stock_name)

    def on_strategy_stock_clicked(self, stock_id_full):
        self.on_stock_changed(stock_id_full)
        self.side_menu.button_group.button(0).setChecked(True)
        self.pages.setCurrentIndex(0)

    def on_add_watchlist_request(self, stock_id, group_name):
        self.list_module.add_stock_to_group(stock_id, group_name)

    def load_initial_data(self):
        # 1. 觸發列表刷新 (這會讓 Worker 開始工作)
        self.list_module.refresh_table()

        # 2. 預設載入清單中的第一檔
        if self.list_module.table.rowCount() > 0:
            item = self.list_module.table.item(0, 0)
            if item:
                code = item.text()
                market = item.data(Qt.ItemDataRole.UserRole)
                fid = f"{code}_{market}"

                # 取得名稱
                name_item = self.list_module.table.item(0, 1)
                name = name_item.text() if name_item else ""

                print(f"🚀 [系統啟動] 預設載入: {fid} {name}")
                self.on_stock_changed(fid)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = StockWarRoomV3()
    window.show()
    sys.exit(app.exec())