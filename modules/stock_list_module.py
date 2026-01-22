import sys
import json
import pandas as pd
from pathlib import Path
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QTableWidget, QTableWidgetItem,
                             QHeaderView, QAbstractItemView, QApplication, QLineEdit,
                             QHBoxLayout, QPushButton, QCompleter, QMenu, QComboBox)
from PyQt6.QtCore import pyqtSignal, Qt, QStringListModel
from PyQt6.QtGui import QColor, QFont, QAction

# 引入下載器 (回到基礎用法)
from utils.data_downloader import DataDownloader

# 預設清單 (當沒有存檔時使用)
DEFAULT_WATCHLISTS = {
    "我的持股": ["6664", "3665", "8358", "6274", "8261"],
    "觀察名單": ["2330", "2317", "2603"],
    "自選3": []
}


class StockListModule(QWidget):
    stock_selected = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.stock_db = {}
        self.downloader = DataDownloader()

        # 🔥 1. 初始化路徑與載入清單
        self.json_path = Path("data/watchlist.json")
        self.watchlists = self.load_watchlists()
        self.current_group = list(self.watchlists.keys())[0]  # 預設選第一個群組

        # 載入股票代號對照表 (同步執行，確保穩定)
        self.load_stock_list_db()

        self.columns_config = [
            ("id", "代號", 60),
            ("name", "名稱", 80),
            ("price", "收盤", 70),
            ("pct_5", "5日%", 65),
            ("pct_10", "10日%", 65),
            ("pct_m", "月%", 65),
            ("rev_yoy", "營收YoY", 0)  # 隱藏或最後
        ]

        self.init_ui()

        # 初始刷新
        self.refresh_table()

    def load_watchlists(self):
        """ 讀取 JSON 存檔，如果沒有則回傳預設值 """
        if self.json_path.exists():
            try:
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    print("📂 [System] 已載入自選股存檔")
                    return data
            except Exception as e:
                print(f"⚠️ 讀取存檔失敗: {e}，使用預設值")

        return DEFAULT_WATCHLISTS.copy()

    def save_watchlists(self):
        """ 儲存目前的清單到 JSON """
        try:
            # 確保目錄存在
            self.json_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.json_path, 'w', encoding='utf-8') as f:
                json.dump(self.watchlists, f, ensure_ascii=False, indent=4)
            print("💾 [System] 自選股已儲存")
        except Exception as e:
            print(f"❌ 儲存失敗: {e}")

    def load_stock_list_db(self):
        """ 讀取 stock_list.csv (同步) """
        try:
            self.downloader.update_stock_list_from_github()  # 嘗試同步最新
        except:
            pass

        csv_path = Path("data/stock_list.csv")
        if not csv_path.exists(): return

        try:
            df = pd.read_csv(csv_path, dtype=str)
            df.columns = [c.lower().strip() for c in df.columns]

            code_col = next((c for c in ['stock_id', 'code', 'id'] if c in df.columns), None)

            if code_col:
                df[code_col] = df[code_col].str.strip()
                df['name'] = df['name'].str.strip()
                if 'market' in df.columns:
                    df['market'] = df['market'].str.strip().str.upper()

                self.stock_db = {}
                for _, row in df.iterrows():
                    self.stock_db[row[code_col]] = {
                        "name": row['name'],
                        "market": row.get('market', 'TW')
                    }
                print(f"✅ DB 載入完成: {len(self.stock_db)} 筆")
        except:
            pass

    def init_ui(self):
        self.setStyleSheet("background-color: #000000; color: #FFFFFF;")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(5)

        # Top
        top_container = QWidget()
        top_container.setStyleSheet("background: #111; border-bottom: 1px solid #333;")
        top_layout = QHBoxLayout(top_container)
        top_layout.setContentsMargins(5, 5, 5, 5)

        self.group_combo = QComboBox()
        self.group_combo.addItems(list(self.watchlists.keys()))
        self.group_combo.setStyleSheet("background: #222; color: #FFF; border: 1px solid #444;")
        self.group_combo.currentTextChanged.connect(self.change_group)

        self.input_stock = QLineEdit()
        self.input_stock.setPlaceholderText("🔍 代號/名稱")
        self.input_stock.setStyleSheet("background: #222; color: #FFF; border: 1px solid #444;")
        self.input_stock.returnPressed.connect(self.add_stock_from_input)

        self.completer = QCompleter()
        self.completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self.completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.input_stock.setCompleter(self.completer)
        self.update_completer_model()

        top_layout.addWidget(self.group_combo, 3)
        top_layout.addWidget(self.input_stock, 7)
        layout.addWidget(top_container)

        # Table
        self.table = QTableWidget()
        self.setup_table_columns()
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; gridline-color: #222; font-size: 14px; border: none; }
            QHeaderView::section { background-color: #1A1A1A; color: #888; border: none; padding: 4px; font-weight: bold; }
            QTableWidget::item:selected { background-color: #333; color: #FFF; } 
        """)

        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.open_context_menu)
        self.table.cellClicked.connect(self.on_row_clicked)

        layout.addWidget(self.table)

    def setup_table_columns(self):
        col_names = [cfg[1] for cfg in self.columns_config]
        self.table.setColumnCount(len(col_names))
        self.table.setHorizontalHeaderLabels(col_names)
        header = self.table.horizontalHeader()
        for i, (key, name, width) in enumerate(self.columns_config):
            if width > 0:
                header.setSectionResizeMode(i, QHeaderView.ResizeMode.Fixed)
                self.table.setColumnWidth(i, width)
            else:
                header.setSectionResizeMode(i, QHeaderView.ResizeMode.Stretch)

    def update_completer_model(self):
        search_list = []
        for code, info in self.stock_db.items():
            name = info.get('name', '')
            search_list.append(f"{code} {name}")
        model = QStringListModel(search_list)
        self.completer.setModel(model)

    def change_group(self, group_name):
        self.current_group = group_name
        self.refresh_table()

    def add_stock_from_input(self):
        text = self.input_stock.text().strip()
        if not text: return
        code = text.split(' ')[0]

        market = "TW"
        if code in self.stock_db:
            info = self.stock_db[code]
            market = info['market']

        current_list = self.watchlists[self.current_group]
        if code not in current_list:
            current_list.insert(0, code)
            self.input_stock.clear()

            # 🔥 2. 加入後立即存檔
            self.save_watchlists()

            self.refresh_table()

            # 自動選取新增的那一行
            self.table.selectRow(0)
            self.stock_selected.emit(f"{code}_{market}")
        else:
            print(f"⚠️ {code} 已在清單中")

    def refresh_table(self):
        # 這裡使用同步邏輯，確保穩定性
        self.table.setRowCount(0)
        current_list = self.watchlists[self.current_group]

        for i, code in enumerate(current_list):
            self.table.insertRow(i)

            info = self.stock_db.get(code, {"name": code, "market": "TW"})
            market = info['market']

            # 下載數據 (同步，可能會微卡，但絕對穩定)
            df = self.downloader.update_kline_data(code, market)

            # ID
            item_id = QTableWidgetItem(code)
            item_id.setData(Qt.ItemDataRole.UserRole, market)
            item_id.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.table.setItem(i, 0, item_id)

            # Name
            self.table.setItem(i, 1, QTableWidgetItem(info['name']))

            # Price & Indicators
            price = 0
            pcts = [0, 0, 0, 0]  # 5d, 10d, m, yoy

            if df is not None and not df.empty:
                try:
                    if 'Date' in df.columns:
                        df['Date'] = pd.to_datetime(df['Date'])
                        df = df.sort_values('Date')

                    col_close = 'close' if 'close' in df.columns else 'Close'
                    latest_close = df.iloc[-1][col_close]
                    price = latest_close

                    def calc_pct(n):
                        if len(df) > n:
                            prev = df.iloc[-(n + 1)][col_close]
                            if prev > 0: return ((latest_close - prev) / prev) * 100
                        return 0.0

                    pcts[0] = calc_pct(5)
                    pcts[1] = calc_pct(10)
                    pcts[2] = calc_pct(20)
                except:
                    pass

            # 填入價格
            item_price = QTableWidgetItem(f"{price:.1f}" if price > 0 else "-")
            item_price.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            self.table.setItem(i, 2, item_price)

            # 填入漲跌幅
            for j, val in enumerate(pcts):
                txt = f"{val:+.2f}%" if val != 0 else "-"
                it = QTableWidgetItem(txt)
                if val > 0:
                    it.setForeground(QColor("#FF3333"))
                elif val < 0:
                    it.setForeground(QColor("#00FF00"))
                else:
                    it.setForeground(QColor("#FFFFFF"))
                it.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                self.table.setItem(i, 3 + j, it)

    def on_row_clicked(self, row, col):
        item = self.table.item(row, 0)
        stock_id = item.text()
        market = item.data(Qt.ItemDataRole.UserRole)
        full_id = f"{stock_id}_{market}"
        self.stock_selected.emit(full_id)

    def open_context_menu(self, position):
        menu = QMenu()
        del_action = QAction("🗑️ 刪除", self)
        del_action.triggered.connect(self.delete_stock)
        menu.addAction(del_action)
        menu.exec(self.table.viewport().mapToGlobal(position))

    def delete_stock(self):
        row = self.table.currentRow()
        if row >= 0:
            code = self.table.item(row, 0).text()
            current_list = self.watchlists[self.current_group]
            if code in current_list:
                current_list.remove(code)
                self.table.removeRow(row)

                # 🔥 3. 刪除後立即存檔
                self.save_watchlists()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = StockListModule()
    win.resize(400, 600)
    win.show()
    sys.exit(app.exec())