import sys
import pandas as pd
from pathlib import Path
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QTableWidget, QTableWidgetItem,
                             QHeaderView, QAbstractItemView, QApplication, QLineEdit,
                             QHBoxLayout, QPushButton, QCompleter, QMenu, QComboBox)
from PyQt6.QtCore import pyqtSignal, Qt, QStringListModel
from PyQt6.QtGui import QColor, QFont, QAction

# 內建備用清單 (萬一 CSV 真的讀不到時的保險)
DEFAULT_STOCKS = {
    "2330": {"name": "台積電", "market": "TW"},
    "2317": {"name": "鴻海", "market": "TW"},
    "2454": {"name": "聯發科", "market": "TW"},
    "2603": {"name": "長榮", "market": "TW"},
    "6664": {"name": "群翊", "market": "TWO"},
    "3434": {"name": "哲固", "market": "TWO"}  # 內建補上這檔測試
}


class StockListModule(QWidget):
    stock_selected = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.stock_db = {}  # Code -> {name, market}

        self.watchlists = {
            "我的持股": ["2330", "2317"],
            "觀察名單": ["6664", "2603"],
            "自選3": []
        }
        self.current_group = "我的持股"

        self.columns_config = [
            ("id", "代號", False),
            ("name", "名稱", False),
            ("price", "收盤", False),
            ("pct_5", "5日%", False),
            ("pct_10", "10日%", False),
            ("pct_m", "月%", False),
            ("rev_yoy", "營收YoY", False)
        ]

        self.load_stock_list_db()
        self.init_ui()
        self.refresh_table()

    def load_stock_list_db(self):
        """ 強化版 CSV 讀取：自動判斷 stock_id 或 code """
        csv_path = Path("data/stock_list.csv")
        print(f"📂 [StockList] 正在讀取清單: {csv_path.absolute()}")

        loaded = False
        if csv_path.exists():
            # 嘗試多種編碼
            for enc in ['utf-8', 'utf-8-sig', 'big5']:
                try:
                    df = pd.read_csv(csv_path, dtype=str, encoding=enc)

                    # 欄位正規化：轉小寫、去空白
                    df.columns = [c.lower().strip() for c in df.columns]
                    print(f"🔍 [StockList] ({enc}) 欄位偵測: {df.columns.tolist()}")

                    # 判斷代號欄位是 stock_id 還是 code
                    code_col = None
                    if 'stock_id' in df.columns:
                        code_col = 'stock_id'
                    elif 'code' in df.columns:
                        code_col = 'code'
                    elif 'id' in df.columns:
                        code_col = 'id'

                    if code_col and 'name' in df.columns:
                        # 資料清洗
                        df[code_col] = df[code_col].str.strip()
                        df['name'] = df['name'].str.strip()

                        # 讀取 market 欄位 (若無則預設 TW)
                        if 'market' in df.columns:
                            df['market'] = df['market'].str.strip().str.upper()

                        for _, row in df.iterrows():
                            code = row[code_col]
                            name = row['name']
                            market = row.get('market', 'TW')

                            self.stock_db[code] = {"name": name, "market": market}

                        print(f"✅ [StockList] 成功載入 {len(self.stock_db)} 筆資料")

                        # 自我檢查：3434 是否正確載入？
                        if "3434" in self.stock_db:
                            info = self.stock_db["3434"]
                            print(f"🎯 [Check] 3434 載入成功: {info} (Market 正確應為 TWO)")
                        else:
                            print("❌ [Check] 3434 依然不在 DB 中 (請檢查 CSV 內容)")

                        loaded = True
                        break  # 成功就跳出編碼迴圈
                    else:
                        print(f"⚠️ [StockList] ({enc}) 缺少關鍵欄位 (需 stock_id/code 與 name)")

                except Exception as e:
                    print(f"⚠️ [StockList] ({enc}) 讀取失敗: {e}")

        if not loaded:
            self.stock_db = DEFAULT_STOCKS.copy()
            print(f"⚠️ [StockList] CSV 讀取失敗，切換至內建備用清單 ({len(self.stock_db)} 筆)")

    def init_ui(self):
        self.setStyleSheet("background-color: #000000; color: #FFFFFF;")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(5)

        # 1. Top Control
        top_container = QWidget()
        top_container.setStyleSheet("background: #111; border-bottom: 1px solid #333;")
        top_layout = QHBoxLayout(top_container)
        top_layout.setContentsMargins(5, 5, 5, 5)

        self.group_combo = QComboBox()
        self.group_combo.addItems(list(self.watchlists.keys()))
        self.group_combo.setStyleSheet("background: #222; color: #FFF; border: 1px solid #444;")
        self.group_combo.currentTextChanged.connect(self.change_group)

        self.input_stock = QLineEdit()
        self.input_stock.setPlaceholderText("🔍 代號/名稱 (Enter加入)")
        self.input_stock.setStyleSheet("background: #222; color: #FFF; border: 1px solid #444;")

        self.completer = QCompleter()
        self.completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self.completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.update_completer_model()
        self.input_stock.setCompleter(self.completer)
        self.input_stock.returnPressed.connect(self.add_stock_from_input)

        top_layout.addWidget(self.group_combo, 3)
        top_layout.addWidget(self.input_stock, 7)
        layout.addWidget(top_container)

        # 2. Table
        self.table = QTableWidget()
        self.setup_table_columns()

        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; gridline-color: #222; font-size: 13px; border: none; }
            QHeaderView::section { background-color: #1A1A1A; color: #888; border: none; padding: 4px; font-weight: bold; }
            QTableWidget::item:selected { background-color: #333; } 
        """)

        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.open_context_menu)
        self.table.horizontalHeader().setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.horizontalHeader().customContextMenuRequested.connect(self.open_header_menu)
        self.table.cellClicked.connect(self.on_row_clicked)

        layout.addWidget(self.table)

    def setup_table_columns(self):
        col_names = [cfg[1] for cfg in self.columns_config]
        self.table.setColumnCount(len(col_names))
        self.table.setHorizontalHeaderLabels(col_names)

        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self.table.setColumnWidth(0, 60)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)
        self.table.setColumnWidth(1, 90)
        for i in range(2, len(col_names)):
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

        # 查表邏輯
        if code in self.stock_db:
            info = self.stock_db[code]
            market = info['market']
            stock_name = info['name']
            print(f"🔎 [Lookup] 查表成功: {code} -> {stock_name} ({market})")
        elif code.isdigit() and len(code) == 4:
            # 查不到但符合格式，預設 TW (這是不完美的 fallback，但總比沒有好)
            market = "TW"
            stock_name = code
            print(f"⚠️ [Lookup] 查無此股，預設上市: {code} -> {stock_name} ({market})")
        else:
            print(f"❌ [Lookup] 無效代號: {code}")
            return

        # 加入清單
        current_list = self.watchlists[self.current_group]
        if code not in current_list:
            current_list.insert(0, code)
            self.refresh_table()
            self.input_stock.clear()

            # 連動
            full_id = f"{code}_{market}"
            self.stock_selected.emit(full_id)
            print(f"✅ [Action] 加入並連動: {full_id} ({stock_name})")
        else:
            print(f"⚠️ {code} 已在清單中")

    def refresh_table(self):
        current_list = self.watchlists[self.current_group]
        data = []
        import random

        for code in current_list:
            info = self.stock_db.get(code, {"name": code, "market": "TW"})

            data.append({
                "id": code,
                "name": info['name'],
                "market": info['market'],
                "price": random.uniform(50, 1000),
                "pct_5": random.uniform(-5, 5),
                "pct_10": random.uniform(-10, 10),
                "pct_m": random.uniform(-15, 15),
                "rev_yoy": random.uniform(-20, 20)
            })

        self.load_data(pd.DataFrame(data))

    def load_data(self, df):
        self.table.setRowCount(0)
        if df.empty: return

        self.table.setRowCount(len(df))
        for i, row in df.iterrows():
            sid = str(row['id'])

            item_id = QTableWidgetItem(sid)
            # 儲存正確的 market
            item_id.setData(Qt.ItemDataRole.UserRole, row.get('market', 'TW'))

            item_name = QTableWidgetItem(str(row['name']))
            item_price = QTableWidgetItem(f"{row['price']:.1f}")

            val_items = []
            keys = ["pct_5", "pct_10", "pct_m", "rev_yoy"]
            for k in keys:
                val = float(row.get(k, 0))
                it = QTableWidgetItem(f"{val:.2f}%")
                if val > 0:
                    it.setForeground(QColor("#FF3333"))
                elif val < 0:
                    it.setForeground(QColor("#00FF00"))
                else:
                    it.setForeground(QColor("#FFFFFF"))
                it.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                val_items.append(it)

            item_id.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            item_name.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            item_price.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

            self.table.setItem(i, 0, item_id)
            self.table.setItem(i, 1, item_name)
            self.table.setItem(i, 2, item_price)
            for j, it in enumerate(val_items):
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

    def open_header_menu(self, position):
        menu = QMenu()
        for i, (key, name, hidden) in enumerate(self.columns_config):
            action = QAction(name, self, checkable=True)
            action.setChecked(not self.table.isColumnHidden(i))
            action.triggered.connect(lambda checked, idx=i: self.table.setColumnHidden(idx, not checked))
            menu.addAction(action)
        menu.exec(self.table.horizontalHeader().viewport().mapToGlobal(position))

    def delete_stock(self):
        row = self.table.currentRow()
        if row >= 0:
            code = self.table.item(row, 0).text()
            current_list = self.watchlists[self.current_group]
            if code in current_list:
                current_list.remove(code)
                self.refresh_table()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = StockListModule()
    win.resize(400, 600)
    win.show()
    sys.exit(app.exec())