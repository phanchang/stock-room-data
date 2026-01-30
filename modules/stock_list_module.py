import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QTableWidget, QTableWidgetItem,
                             QHeaderView, QAbstractItemView, QApplication, QLineEdit,
                             QHBoxLayout, QPushButton, QCompleter, QMenu, QComboBox, QMessageBox)
from PyQt6.QtCore import pyqtSignal, Qt, QStringListModel
from PyQt6.QtGui import QColor, QAction, QFont

from utils.data_downloader import DataDownloader
from utils.quote_worker import QuoteWorker

DEFAULT_WATCHLISTS = {
    "我的持股": ["6664", "3665", "8358", "6274", "8261"],
    "觀察名單": ["2330", "2317", "2603"],
    "自選3": []
}


class StockListModule(QWidget):
    stock_selected = pyqtSignal(str)

    def __init__(self, parent=None, shared_worker=None):
        super().__init__(parent)
        self.stock_db = {}
        self.downloader = DataDownloader()

        # 歷史價格快取 (用於計算漲跌幅基準)
        self.history_cache = {}

        # 1. 載入設定
        self.json_path = Path("data/watchlist.json")
        self.watchlists = self.load_watchlists()
        self.current_group = list(self.watchlists.keys())[0]

        # 2. 載入 DB
        self.load_stock_list_db()

        # 3. 表格欄位設定
        self.columns_config = [
            ("id", "代號", 70),
            ("name", "名稱", 90),
            ("price", "成交", 80),
            ("change_pct", "漲跌%", 80),
            ("tick_vol", "單量", 65),
            ("total_vol", "總量", 75),
            ("time", "時間", 85),
        ]

        # 4. 初始化 Worker
        if shared_worker:
            self.quote_worker = shared_worker
        else:
            print("⚠️ [StockList] 未收到 Shared Worker，啟動獨立 Worker")
            self.quote_worker = QuoteWorker(self)
            self.quote_worker.start()

        self.quote_worker.quote_updated.connect(self.update_streaming_data)
        self.init_ui()

    def load_watchlists(self):
        if self.json_path.exists():
            try:
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return DEFAULT_WATCHLISTS.copy()

    def save_watchlists(self):
        try:
            self.json_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.json_path, 'w', encoding='utf-8') as f:
                json.dump(self.watchlists, f, ensure_ascii=False, indent=4)
        except:
            pass

    def load_stock_list_db(self):
        csv_path = Path("data/stock_list.csv")
        if not csv_path.exists():
            try:
                self.downloader.update_stock_list_from_github()
            except:
                pass
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path, dtype=str)
                df.columns = [c.lower().strip() for c in df.columns]
                code_col = next((c for c in ['stock_id', 'code', 'id'] if c in df.columns), None)
                if code_col:
                    self.stock_db = {
                        str(row[code_col]).strip(): {
                            "name": str(row['name']).strip(),
                            "market": str(row.get('market', 'TW')).strip().upper()
                        } for _, row in df.iterrows()
                    }
            except:
                pass

    def init_ui(self):
        self.setStyleSheet("background-color: #000000; color: #FFFFFF;")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # --- Top Container ---
        top_container = QWidget()
        top_container.setStyleSheet("background: #111; border-bottom: 1px solid #333;")
        top_layout = QHBoxLayout(top_container)
        top_layout.setContentsMargins(5, 5, 5, 5)

        self.group_combo = QComboBox()
        self.group_combo.addItems(list(self.watchlists.keys()))
        self.group_combo.setStyleSheet("""
            QComboBox { background: #222; color: #FFF; border: 1px solid #444; font-size: 14px; padding: 2px; }
            QComboBox::drop-down { border: none; }
        """)
        self.group_combo.currentTextChanged.connect(self.change_group)

        self.input_stock = QLineEdit()
        self.input_stock.setPlaceholderText("🔍 輸入代號按 Enter 快查")
        self.input_stock.setStyleSheet("background: #222; color: #FFF; border: 1px solid #444; font-size: 14px;")

        # 🔥 修改邏輯：Enter 鍵觸發「快速查看 (不新增)」
        self.input_stock.returnPressed.connect(self.quick_search)

        self.completer = QCompleter()
        self.completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self.completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.input_stock.setCompleter(self.completer)
        self.update_completer_model()

        # 🔥 「+」按鈕：明確定義為「新增到清單」
        self.btn_add = QPushButton("+")
        self.btn_add.setFixedSize(30, 24)
        self.btn_add.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_add.setToolTip("將此股加入清單")
        self.btn_add.setStyleSheet("""
            QPushButton { background: #00E5FF; color: #000; border-radius: 3px; font-weight: bold; font-size: 16px; }
            QPushButton:hover { background: #FFFFFF; }
        """)
        self.btn_add.clicked.connect(self.add_stock_to_list)

        top_layout.addWidget(self.group_combo, 3)
        top_layout.addWidget(self.input_stock, 6)
        top_layout.addWidget(self.btn_add, 1)
        layout.addWidget(top_container)

        # --- Table ---
        self.table = QTableWidget()
        self.setup_table_columns()
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; border: none; font-size: 16px; font-family: 'Microsoft JhengHei', 'Consolas'; }
            QHeaderView::section { background-color: #1A1A1A; color: #BBB; border: none; padding: 6px; font-size: 14px; font-weight: bold; }
            QTableWidget::item { padding-right: 5px; padding-left: 5px; border-bottom: 1px solid #222; }
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
        search_list = [f"{c} {i['name']}" for c, i in self.stock_db.items()]
        self.completer.setModel(QStringListModel(search_list))

    def change_group(self, group_name):
        self.current_group = group_name
        self.refresh_table()

    def _resolve_stock_code(self, text):
        """ 輔助函數：從輸入文字解析出正確的代號 """
        text = text.strip().upper()
        if not text: return None

        # 情況 A: 直接輸入代號 "2330"
        if text in self.stock_db:
            return text

        # 情況 B: 輸入 "2330 台積電"
        code_part = text.split(' ')[0]
        if code_part in self.stock_db:
            return code_part

        # 情況 C: 輸入中文名稱 "台積電" (簡單遍歷查找)
        for code, info in self.stock_db.items():
            if info['name'] == text:
                return code

        return None

    def quick_search(self):
        """ 🔥 新功能：Enter 鍵觸發，只查看不新增 """
        text = self.input_stock.text()
        code = self._resolve_stock_code(text)

        if not code:
            return

        # 1. 檢查是否已在目前清單中
        items = self.table.findItems(code, Qt.MatchFlag.MatchExactly)

        if items:
            # A. 在清單內：選中該行，正常連動
            row = items[0].row()
            self.table.selectRow(row)
            self.on_row_clicked(row, 0)
            self.input_stock.clear()  # 清空讓視野乾淨
        else:
            # B. 不在清單內：發送訊號連動，但取消表格選取 (表示是外部查詢)
            market = self.stock_db.get(code, {}).get('market', 'TW')
            self.table.clearSelection()  # 🔥 取消選取，視覺上區隔
            self.stock_selected.emit(f"{code}_{market}")
            # 這裡選擇不清空輸入框，方便使用者知道自己正在查哪支

    def add_stock_to_list(self):
        """ 🔥 點擊 + 號：強制新增到清單 """
        text = self.input_stock.text()
        code = self._resolve_stock_code(text)

        if not code:
            QMessageBox.warning(self, "錯誤", f"找不到股票: {text}")
            return

        current_list = self.watchlists[self.current_group]

        # 檢查是否重複
        if code in current_list:
            QMessageBox.information(self, "提示", f"{code} 已在清單中")
            self.quick_search()  # 直接定位
            return

        # 新增邏輯
        current_list.insert(0, code)
        self.save_watchlists()
        self.input_stock.clear()
        self.refresh_table()

        # 新增後自動選中第一行
        self.table.selectRow(0)
        market = self.stock_db.get(code, {}).get('market', 'TW')
        self.stock_selected.emit(f"{code}_{market}")

    def on_row_clicked(self, row, col):
        item = self.table.item(row, 0)
        if item:
            code = item.text()
            market = item.data(Qt.ItemDataRole.UserRole)
            self.stock_selected.emit(f"{code}_{market}")

    def open_context_menu(self, position):
        idx = self.table.indexAt(position)
        if not idx.isValid(): return

        menu = QMenu()
        menu.setStyleSheet(
            "QMenu { background-color: #333; color: white; border: 1px solid #555; } QMenu::item:selected { background-color: #555; }")

        del_action = QAction("🗑️ 刪除此股", self)
        del_action.triggered.connect(lambda: self.delete_stock(idx.row()))
        menu.addAction(del_action)

        menu.exec(self.table.viewport().mapToGlobal(position))

    def delete_stock(self, row):
        if row >= 0:
            code = self.table.item(row, 0).text()
            current_list = self.watchlists[self.current_group]
            if code in current_list:
                current_list.remove(code)
                self.table.removeRow(row)
                self.save_watchlists()
                self.quote_worker.set_monitoring_stocks(current_list, source='watchlist')

    # --- 核心邏輯 (顯示與計算) ---

    def _get_color_and_fmt(self, current, ref):
        if ref == 0: return QColor("#FFFFFF"), "0.00%"
        pct = ((current - ref) / ref) * 100
        color = QColor("#FF3333") if current > ref else (QColor("#00FF00") if current < ref else QColor("#FFFFFF"))
        return color, f"{pct:+.2f}%"

    def _set_cell(self, row, col, text, color=None):
        item = self.table.item(row, col) or QTableWidgetItem()
        self.table.setItem(row, col, item)
        item.setText(str(text))
        if color:
            item.setForeground(color)
        else:
            item.setForeground(QColor("#FFFFFF"))
        item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        item.setFont(QFont("Consolas", 12))

    def update_streaming_data(self, data):
        for code, stock_data in data.items():
            items = self.table.findItems(code, Qt.MatchFlag.MatchExactly)
            if not items: continue

            target_item = None
            for item in items:
                if item.column() == 0:
                    target_item = item
                    break
            if not target_item: continue
            row = target_item.row()

            real = stock_data.get('realtime', {})
            info = stock_data.get('info', {})

            try:
                l_price = real.get('latest_trade_price')
                close_p = real.get('close')
                latest = float(l_price) if l_price and l_price != '-' else 0
                final = float(close_p) if close_p and close_p != '-' else 0
                price = latest if latest > 0 else final

                if price == 0: continue

                cached_hist = self.history_cache.get(code, {})
                prev_close = cached_hist.get('prev', 0)
                if prev_close == 0:
                    api_prev = real.get('previous_close')
                    prev_close = float(api_prev) if api_prev and api_prev != '-' else 0

                item_price = self.table.item(row, 2) or QTableWidgetItem()
                self.table.setItem(row, 2, item_price)
                item_price.setText(f"{price:.2f}")
                item_price.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                item_price.setFont(QFont("Consolas", 12, QFont.Weight.Bold))

                if prev_close > 0:
                    color, pct_str = self._get_color_and_fmt(price, prev_close)
                    item_price.setForeground(color)
                    self._set_cell(row, 3, pct_str, color)
                else:
                    item_price.setForeground(QColor("#FFFFFF"))
                    self._set_cell(row, 3, "-")

                tick_vol = real.get('trade_volume', '-')
                self._set_cell(row, 4, tick_vol, QColor("#FFFF00"))

                total_vol = real.get('accumulate_trade_volume', '-')
                self._set_cell(row, 5, total_vol)

                raw_time = info.get('time', '-')
                if raw_time and ' ' in raw_time:
                    display_time = raw_time.split(' ')[1]
                else:
                    display_time = raw_time
                self._set_cell(row, 6, display_time, QColor("#AAAAAA"))

            except Exception as e:
                pass

    def refresh_table(self):
        self.table.setRowCount(0)
        self.history_cache = {}
        current_list = self.watchlists.get(self.current_group, [])

        if hasattr(self, 'quote_worker'):
            self.quote_worker.set_monitoring_stocks(current_list, source='watchlist')

        for i, code in enumerate(current_list):
            self.table.insertRow(i)
            info = self.stock_db.get(code, {"name": code, "market": "TW"})
            market = info['market']
            path = Path(f"data/cache/tw/{code}_{market}.parquet")

            item_id = QTableWidgetItem(code)
            item_id.setData(Qt.ItemDataRole.UserRole, market)
            item_id.setForeground(QColor("#00E5FF"))
            item_id.setFont(QFont("Consolas", 12, QFont.Weight.Bold))
            self.table.setItem(i, 0, item_id)

            item_name = QTableWidgetItem(info['name'])
            item_name.setFont(QFont("Microsoft JhengHei", 12))
            self.table.setItem(i, 1, item_name)

            for col in range(2, 7): self._set_cell(i, col, "-")

            if path.exists():
                try:
                    df = pd.read_parquet(path)
                    if not df.empty:
                        last_close = df.iloc[-1]['close']
                        self.history_cache[code] = {'prev': last_close}
                        self._set_cell(i, 2, f"{last_close:.2f}")

                        if len(df) >= 2:
                            prev_of_prev = df.iloc[-2]['close']
                            c, s = self._get_color_and_fmt(last_close, prev_of_prev)
                            self.table.item(i, 2).setForeground(c)
                            self._set_cell(i, 3, s, c)
                except Exception as e:
                    print(f"Parquet Error {code}: {e}")

            if hasattr(self, 'quote_worker') and hasattr(self.quote_worker, 'get_latest_from_cache'):
                cached_data = self.quote_worker.get_latest_from_cache(code)
                if cached_data:
                    self.update_streaming_data({code: {'realtime': cached_data, 'info': {'time': 'Cached'}}})