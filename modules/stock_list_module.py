import sys
import json
import datetime
import pandas as pd
import copy  # <-- 新增這行 (用於深拷貝字典)
from pathlib import Path
from PyQt6.QtCore import pyqtSignal, Qt, QStringListModel, QTimer
from PyQt6.QtGui import QColor, QAction, QFont, QBrush

from utils.data_downloader import DataDownloader
from utils.quote_worker import QuoteWorker
from PyQt6.QtWidgets import QStyledItemDelegate, QStyle
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QTableWidget, QTableWidgetItem,
                             QHeaderView, QAbstractItemView, QLineEdit,
                             QHBoxLayout, QPushButton, QCompleter, QMenu, QComboBox,
                             QMessageBox, QDialog, QInputDialog, QListWidget)

DEFAULT_WATCHLISTS = {
    "我的持股": ["2330", "2317", "2603"],
    "觀察名單": []
}


class WatchlistEditDialog(QDialog):
    def __init__(self, watchlists, stock_db, parent=None):
        super().__init__(parent)
        self.setWindowTitle("編輯群組與持股名單")
        self.setMinimumSize(500, 400)
        # 使用深拷貝，避免在按下取消時修改到原本的資料
        self.watchlists = copy.deepcopy(watchlists)
        self.stock_db = stock_db
        self.current_group = None
        self.init_ui()

    def init_ui(self):
        self.setStyleSheet("""
            QDialog { background-color: #222; color: #FFF; }
            QLabel { color: #FFF; font-weight: bold; }
            QLineEdit { background-color: #111; color: #FFF; border: 1px solid #444; padding: 4px; }
            QListWidget { background-color: #111; color: #FFF; border: 1px solid #444; font-family: 'Consolas', 'Microsoft JhengHei'; font-size: 14px; }
            QListWidget::item { padding: 4px; }
            QListWidget::item:selected { background-color: #444; }
            QPushButton { background: #333; color: #FFF; border: 1px solid #555; padding: 4px; border-radius: 2px;}
            QPushButton:hover { background: #555; }
            QPushButton:pressed { background: #222; }
        """)

        main_layout = QVBoxLayout(self)
        h_layout = QHBoxLayout()

        # ====== 左側：群組列表 ======
        left_layout = QVBoxLayout()
        self.group_list = QListWidget()
        self.group_list.addItems(list(self.watchlists.keys()))
        self.group_list.currentRowChanged.connect(self.on_group_selected)

        btn_add_group = QPushButton("➕ 新增群組")
        btn_rename_group = QPushButton("✏️ 重新命名")
        btn_del_group = QPushButton("🗑️ 刪除群組")

        btn_add_group.clicked.connect(self.add_group)
        btn_rename_group.clicked.connect(self.rename_group)
        btn_del_group.clicked.connect(self.delete_group)

        left_layout.addWidget(self.group_list)
        left_layout.addWidget(btn_add_group)
        left_layout.addWidget(btn_rename_group)
        left_layout.addWidget(btn_del_group)

        # ====== 右側：持股列表 ======
        right_layout = QVBoxLayout()

        # 🌟 搜尋與新增股票區塊
        add_stock_layout = QHBoxLayout()
        self.input_add_stock = QLineEdit()
        self.input_add_stock.setPlaceholderText("🔍 輸入代號或名稱")
        self.input_add_stock.returnPressed.connect(self.add_stock)

        # 綁定自動完成 (Completer)
        self.completer = QCompleter()
        self.completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self.input_add_stock.setCompleter(self.completer)
        search_list = [f"{c} {info.get('name', '')}" for c, info in self.stock_db.items()]
        self.completer.setModel(QStringListModel(search_list))

        btn_add_stock = QPushButton("➕ 加入持股")
        btn_add_stock.clicked.connect(self.add_stock)

        add_stock_layout.addWidget(self.input_add_stock, 4)
        add_stock_layout.addWidget(btn_add_stock, 1)

        self.stock_list = QListWidget()

        btn_move_up = QPushButton("🔼 上移")
        btn_move_down = QPushButton("🔽 下移")
        btn_del_stock = QPushButton("🗑️ 刪除持股")

        btn_move_up.clicked.connect(self.move_stock_up)
        btn_move_down.clicked.connect(self.move_stock_down)
        btn_del_stock.clicked.connect(self.delete_stock)

        right_btn_layout = QHBoxLayout()
        right_btn_layout.addWidget(btn_move_up)
        right_btn_layout.addWidget(btn_move_down)
        right_btn_layout.addWidget(btn_del_stock)

        right_layout.addLayout(add_stock_layout)
        right_layout.addWidget(self.stock_list)
        right_layout.addLayout(right_btn_layout)

        h_layout.addLayout(left_layout, 1)
        h_layout.addLayout(right_layout, 2)  # 讓右邊持股區塊寬一點

        # ====== 底部：儲存與取消 ======
        bottom_layout = QHBoxLayout()
        bottom_layout.addStretch()
        btn_save = QPushButton("💾 儲存並套用")
        btn_save.setStyleSheet("background: #004D40; font-weight: bold; padding: 6px 12px;")
        btn_cancel = QPushButton("取消")
        btn_cancel.setStyleSheet("padding: 6px 12px;")

        btn_save.clicked.connect(self.accept)
        btn_cancel.clicked.connect(self.reject)

        bottom_layout.addWidget(btn_cancel)
        bottom_layout.addWidget(btn_save)

        main_layout.addLayout(h_layout)
        main_layout.addLayout(bottom_layout)

        if self.group_list.count() > 0:
            self.group_list.setCurrentRow(0)

    # ================== 邏輯處理 ==================

    def on_group_selected(self, index):
        self.stock_list.clear()
        if index >= 0:
            self.current_group = self.group_list.item(index).text()
            for code in self.watchlists[self.current_group]:
                name = self.stock_db.get(code, {}).get("name", "")
                display_text = f"{code}  {name}".strip() if name else code
                self.stock_list.addItem(display_text)

    def add_stock(self):
        if not self.current_group:
            QMessageBox.warning(self, "提示", "請先選擇左側的群組！")
            return

        text = self.input_add_stock.text().strip().upper()
        if not text:
            return

        # 取出開頭的代號部分
        code = text.split(' ')[0]

        # 若輸入合法，則加入名單
        if code in self.stock_db or code.isalnum():
            curr_list = self.watchlists[self.current_group]
            if code not in curr_list:
                # 寫入暫存資料
                curr_list.insert(0, code)
                # 更新 UI 顯示
                name = self.stock_db.get(code, {}).get("name", "")
                display_text = f"{code}  {name}".strip() if name else code
                self.stock_list.insertItem(0, display_text)

            self.input_add_stock.clear()
            self.stock_list.setCurrentRow(0)

    def add_group(self):
        text, ok = QInputDialog.getText(self, "新增群組", "請輸入新群組名稱:")
        if ok and text:
            if text in self.watchlists:
                QMessageBox.warning(self, "錯誤", "群組名稱已存在！")
                return
            self.watchlists[text] = []
            self.group_list.addItem(text)
            self.group_list.setCurrentRow(self.group_list.count() - 1)

    def rename_group(self):
        item = self.group_list.currentItem()
        if not item: return
        old_name = item.text()
        text, ok = QInputDialog.getText(self, "重新命名", "請輸入新群組名稱:", text=old_name)
        if ok and text and text != old_name:
            if text in self.watchlists:
                QMessageBox.warning(self, "錯誤", "群組名稱已存在！")
                return
            new_dict = {}
            for k, v in self.watchlists.items():
                if k == old_name:
                    new_dict[text] = v
                else:
                    new_dict[k] = v
            self.watchlists = new_dict
            item.setText(text)
            self.current_group = text

    def delete_group(self):
        item = self.group_list.currentItem()
        if not item: return
        if self.group_list.count() <= 1:
            QMessageBox.warning(self, "錯誤", "至少需要保留一個群組！")
            return

        reply = QMessageBox.question(self, "確認刪除", f"確定要刪除群組「{item.text()}」及其所有持股嗎？")
        if reply == QMessageBox.StandardButton.Yes:
            del self.watchlists[item.text()]
            self.group_list.takeItem(self.group_list.row(item))

    def move_stock_up(self):
        row = self.stock_list.currentRow()
        if row > 0 and self.current_group:
            item = self.stock_list.takeItem(row)
            self.stock_list.insertItem(row - 1, item)
            self.stock_list.setCurrentRow(row - 1)
            lst = self.watchlists[self.current_group]
            lst[row], lst[row - 1] = lst[row - 1], lst[row]

    def move_stock_down(self):
        row = self.stock_list.currentRow()
        if row >= 0 and row < self.stock_list.count() - 1 and self.current_group:
            item = self.stock_list.takeItem(row)
            self.stock_list.insertItem(row + 1, item)
            self.stock_list.setCurrentRow(row + 1)
            lst = self.watchlists[self.current_group]
            lst[row], lst[row + 1] = lst[row + 1], lst[row]

    def delete_stock(self):
        row = self.stock_list.currentRow()
        if row >= 0 and self.current_group:
            self.stock_list.takeItem(row)
            del self.watchlists[self.current_group][row]

class BackgroundDelegate(QStyledItemDelegate):
    def paint(self, painter, option, index):
        # 若該列未被選取，且有設定背景色，則強制在底層填滿背景
        if not (option.state & QStyle.StateFlag.State_Selected):
            bg_brush = index.data(Qt.ItemDataRole.BackgroundRole)
            if bg_brush:
                painter.fillRect(option.rect, bg_brush)
        # 接著呼叫原生的 paint 來畫出文字與原本的 CSS 底線
        super().paint(painter, option, index)

class NumericTableWidgetItem(QTableWidgetItem):
    def __lt__(self, other):
        try:
            return float(self.text().replace(',', '').replace('%', '')) < float(
                other.text().replace(',', '').replace('%', ''))
        except ValueError:
            return super().__lt__(other)


class StockListModule(QWidget):
    stock_selected = pyqtSignal(str)

    def __init__(self, parent=None, shared_worker=None):
        super().__init__(parent)
        self.stock_db = {}
        self.downloader = DataDownloader()
        self.history_cache = {}
        self.row_mapping = {}
        self.has_auto_selected = False

        self.json_path = Path("data/watchlist.json")
        self.watchlists = self.load_watchlists()
        if not self.watchlists:
            self.watchlists = DEFAULT_WATCHLISTS.copy()
        self.current_group = list(self.watchlists.keys())[0]

        self.load_stock_list_db()

        self.columns_config = [
            ("id", "代號", 65),
            ("name", "名稱", 80),
            ("price", "成交", 80),
            ("change_val", "漲跌", 80),
            ("change_pct", "漲跌%", 75),
            ("tick_vol", "單量(張)", 65),
            ("total_vol", "總量(張)", 75),
            ("time", "時間", 0),
        ]

        if shared_worker:
            self.quote_worker = shared_worker
        else:
            self.quote_worker = QuoteWorker(self)

        self.quote_worker.quote_updated.connect(self.update_streaming_data)

        if hasattr(self.quote_worker, 'oneshot_finished'):
            self.quote_worker.oneshot_finished.connect(self.on_oneshot_finished)

        self.init_ui()
        self.check_if_data_up_to_date()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        top_container = QWidget()
        top_container.setStyleSheet("background: #111; border-bottom: 1px solid #333;")
        top_layout = QHBoxLayout(top_container)
        top_layout.setContentsMargins(5, 5, 5, 5)

        self.group_combo = QComboBox()
        self.group_combo.addItems(list(self.watchlists.keys()))
        self.group_combo.setCurrentText(self.current_group)
        self.group_combo.setStyleSheet("""
            QComboBox { background: #222; color: #FFFFFF; border: 1px solid #444; }
            QComboBox QAbstractItemView { background: #222; color: #FFFFFF; selection-background-color: #444; }
        """)
        self.group_combo.currentTextChanged.connect(self.change_group)

        # 新增編輯群組按鈕
        self.btn_edit_group = QPushButton("✏️")
        self.btn_edit_group.setFixedSize(28, 24)
        self.btn_edit_group.setToolTip("編輯群組與持股名單")
        self.btn_edit_group.setStyleSheet("QPushButton { background: #333; color: #FFF; border: 1px solid #555; } QPushButton:hover { background: #444; }")
        self.btn_edit_group.clicked.connect(self.open_edit_dialog)

        self.btn_monitor = QPushButton("▶ 即時")
        self.btn_monitor.setFixedSize(60, 24)
        self.btn_monitor.setCheckable(True)
        self.btn_monitor.setStyleSheet("""
            QPushButton { background: #333; color: #AAA; border: 1px solid #555; font-weight: bold; }
            QPushButton:checked { background: #00FF00; color: #000; }
            QPushButton:disabled { background: #222; color: #555; border: 1px solid #333; }
        """)
        self.btn_monitor.clicked.connect(self.toggle_quote_monitor)

        self.input_stock = QLineEdit()
        self.input_stock.setPlaceholderText("🔍 輸入代號")
        self.input_stock.setStyleSheet("background: #222; color: #FFF; border: 1px solid #444;")
        self.input_stock.returnPressed.connect(self.quick_search)

        self.completer = QCompleter()
        self.completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self.input_stock.setCompleter(self.completer)
        self.update_completer_model()

        self.btn_add = QPushButton("+")
        self.btn_add.setFixedSize(30, 24)
        self.btn_add.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_add.clicked.connect(self.add_stock_to_list)

        top_layout.addWidget(self.group_combo, 2)
        top_layout.addWidget(self.btn_edit_group, 0)  # 加入編輯按鈕
        top_layout.addWidget(self.btn_monitor, 1)
        top_layout.addWidget(self.input_stock, 5)
        top_layout.addWidget(self.btn_add, 1)
        layout.addWidget(top_container)

        self.table = QTableWidget()
        self.setup_table_columns()
        self.table.verticalHeader().setVisible(False)

        self.table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self.table.setStyleSheet("""
                    QTableWidget { background-color: #000000; font-family: 'Consolas', 'Microsoft JhengHei'; font-size: 14px; }
                    QHeaderView::section { background-color: #1A1A1A; color: #BBB; border: none; font-weight: bold; }
                    QTableWidget::item { background-color: transparent; border-bottom: 1px solid #222; padding-right: 5px; }
                    QTableWidget::item:selected { background-color: #444; color: #FFF; }
                """)
        self.table.setItemDelegate(BackgroundDelegate(self.table))
        self.table.cellClicked.connect(self.on_row_clicked)
        self.table.customContextMenuRequested.connect(self.open_context_menu)
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)

        layout.addWidget(self.table)
        self.refresh_table()

    def _auto_select_first_row(self):
        if self.table.rowCount() > 0:
            self.table.selectRow(0)
            item = self.table.item(0, 0)
            if item:
                self._emit_smart_stock_id(item.text())
            self.has_auto_selected = True

    def check_if_data_up_to_date(self):
        now = datetime.datetime.now()
        is_after_hours = now.hour >= 14 or (now.hour == 13 and now.minute >= 35)

        current_list = self.watchlists.get(self.current_group, [])
        if not current_list: return

        sample_code = current_list[0]
        cache_path = Path(f"data/cache/tw/{sample_code}_TW.parquet")
        if not cache_path.exists():
            cache_path = Path(f"data/cache/tw/{sample_code}_TWO.parquet")

        is_data_fresh = False
        if cache_path.exists():
            try:
                mtime = datetime.datetime.fromtimestamp(cache_path.stat().st_mtime)
                if mtime.date() == now.date() and is_after_hours:
                    is_data_fresh = True
            except:
                pass

        if is_data_fresh:
            self.btn_monitor.setText("已更新")
            self.btn_monitor.setToolTip("今日盤後資料已下載，無須即時更新")
            self.btn_monitor.setDisabled(True)
            self.btn_monitor.setStyleSheet(
                "QPushButton { background: #222; color: #00E5FF; border: 1px solid #004444; }")
        else:
            self.btn_monitor.setDisabled(False)
            self.btn_monitor.setText("▶ 即時")

    def toggle_quote_monitor(self, checked):
        if not hasattr(self, 'quote_worker'): return

        if checked:
            if not self.quote_worker.isRunning():
                self.quote_worker.start()

            current_list = self.watchlists.get(self.current_group, [])
            self.quote_worker.set_monitoring_stocks(current_list, source='watchlist')

            now = datetime.datetime.now()
            is_trading_hours = (
                    (now.hour == 8 and now.minute >= 45) or
                    (now.hour > 8 and now.hour < 13) or
                    (now.hour == 13 and now.minute <= 35)
            )

            if is_trading_hours:
                self.btn_monitor.setText("■ 停止")
                if hasattr(self.quote_worker, 'set_mode'):
                    self.quote_worker.set_mode('continuous')
                self.quote_worker.toggle_monitoring(True)
            else:
                self.btn_monitor.setText("更新中...")
                if hasattr(self.quote_worker, 'set_mode'):
                    self.quote_worker.set_mode('oneshot')
                self.quote_worker.toggle_monitoring(True)

        else:
            self.btn_monitor.setText("▶ 即時")
            self.quote_worker.toggle_monitoring(False)

    def on_oneshot_finished(self):
        self.btn_monitor.setChecked(False)
        self.btn_monitor.setText("▶ 即時")

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
        if not csv_path.exists(): return
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

        self.table.setColumnHidden(5, True)

    def update_completer_model(self):
        search_list = [f"{c} {i['name']}" for c, i in self.stock_db.items()]
        self.completer.setModel(QStringListModel(search_list))

    def _resolve_stock_code(self, text):
        text = text.strip().upper()
        if not text: return None
        code_part = text.split(' ')[0]
        if code_part in self.stock_db:
            return code_part
        if code_part.isalnum():
            return code_part
        return None

    def quick_search(self):
        text = self.input_stock.text()
        code = self._resolve_stock_code(text)
        if code: self._emit_smart_stock_id(code)

    def add_stock_to_list(self):
        code = self._resolve_stock_code(self.input_stock.text())
        if not code: return
        curr = self.watchlists[self.current_group]
        if code not in curr:
            curr.insert(0, code)
            self.save_watchlists()
            self.refresh_table()
            self.input_stock.clear()

    def add_stock_to_group(self, stock_code, group_name):
        clean_code = stock_code.split('_')[0]
        if group_name not in self.watchlists:
            self.watchlists[group_name] = []

        if clean_code not in self.watchlists[group_name]:
            self.watchlists[group_name].insert(0, clean_code)
            self.save_watchlists()

            # 如果當前不在被加入的群組，強制切換過去 (防止訊號重複觸發，先阻擋 Signal)
            if self.current_group != group_name:
                self.group_combo.blockSignals(True)
                self.group_combo.setCurrentText(group_name)
                self.current_group = group_name
                self.group_combo.blockSignals(False)

            # 🌟 透過 QTimer 延遲 100 毫秒重繪，就算一次塞入 10 檔也只會順暢重繪一次！
            QTimer.singleShot(100, self.refresh_table)

    def open_context_menu(self, pos):
        selected_items = self.table.selectedItems()
        if not selected_items: return

        rows = set(item.row() for item in selected_items)

        menu = QMenu()
        menu.setStyleSheet(
            "QMenu { background: #222; color: #FFF; border: 1px solid #555; } QMenu::item:selected { background: #444; }")
        act = QAction(f"🗑️ 刪除選取的 {len(rows)} 檔", self)
        act.triggered.connect(lambda: self.delete_stocks(rows))
        menu.addAction(act)
        menu.exec(self.table.viewport().mapToGlobal(pos))

    def delete_stocks(self, rows):
        codes_to_delete = [self.table.item(r, 0).text() for r in rows]
        curr = self.watchlists[self.current_group]

        for code in codes_to_delete:
            if code in curr:
                curr.remove(code)

        self.save_watchlists()
        self.refresh_table()

    def change_group(self, group_name):
        self.current_group = group_name
        self.refresh_table()

    def open_edit_dialog(self):
        # 🌟 呼叫對話框時，額外傳入 self.stock_db 給它查詢名稱使用
        dialog = WatchlistEditDialog(self.watchlists, self.stock_db, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            # 接收修改後的資料
            self.watchlists = dialog.watchlists
            self.save_watchlists()

            # 更新下拉選單 UI
            self.group_combo.blockSignals(True)
            self.group_combo.clear()
            self.group_combo.addItems(list(self.watchlists.keys()))

            # 嘗試維持原本所在的群組，若被刪除則退回第一個
            if self.current_group not in self.watchlists:
                self.current_group = list(self.watchlists.keys())[0] if self.watchlists else ""

            if self.current_group:
                self.group_combo.setCurrentText(self.current_group)
            self.group_combo.blockSignals(False)

            self.refresh_table()

    def _emit_smart_stock_id(self, code):
        market = "TW"
        if code in self.stock_db:
            market = self.stock_db[code].get('market', 'TW')
        suffix = "_TWO" if market in ['TWO', 'OTC', '上櫃'] else "_TW"
        self.stock_selected.emit(f"{code}{suffix}")

    def on_row_clicked(self, row, col):
        item = self.table.item(row, 0)
        if item: self._emit_smart_stock_id(item.text())


    def _set_cell(self, row, col, text, color=None, bg_color=None, is_num=False):
        if is_num:
            item = NumericTableWidgetItem(str(text))
        else:
            item = QTableWidgetItem(str(text))

        if color:
            item.setForeground(color)
        else:
            item.setForeground(QColor("white"))

        if bg_color:
            # 🔥 關鍵修正：在 PyQt6 中，背景必須套上 QBrush(畫刷) 才能成功渲染！
            item.setBackground(QBrush(QColor(bg_color)))

        item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.table.setItem(row, col, item)

    def refresh_table(self):
        self.table.setUpdatesEnabled(False)
        self.table.setSortingEnabled(False)

        try:
            self.table.setRowCount(0)
            self.history_cache = {}
            self.row_mapping = {}

            current_list = self.watchlists.get(self.current_group, [])
            self.table.setRowCount(len(current_list))

            if hasattr(self, 'quote_worker') and self.btn_monitor.isChecked():
                self.quote_worker.set_monitoring_stocks(current_list, source='watchlist')

            base_cache_path = Path("data/cache/tw")

            for i, code in enumerate(current_list):
                self.row_mapping[code] = i
                info = self.stock_db.get(code, {"name": code, "market": "TW"})

                last_close = 0
                target_file = None

                for suffix in ["_TW.parquet", "_TWO.parquet", ".parquet"]:
                    p = base_cache_path / f"{code}{suffix}"
                    if p.exists():
                        target_file = p
                        break

                item_id = QTableWidgetItem(code)
                item_id.setForeground(QColor("#00E5FF"))
                item_id.setFont(QFont("Consolas", 11, QFont.Weight.Bold))
                item_id.setData(Qt.ItemDataRole.UserRole, info['market'])
                self.table.setItem(i, 0, item_id)

                item_name = QTableWidgetItem(info['name'])
                item_name.setForeground(QColor("white"))
                self.table.setItem(i, 1, item_name)

                change_str, pct_str = "-", "-"
                fg_color = QColor("#CCCCCC")
                bg_color = None
                vol_str = "-"

                if target_file:
                    try:
                        df = pd.read_parquet(target_file)
                        if not df.empty and len(df) >= 1:
                            cols = {c.lower(): c for c in df.columns}
                            c_col = cols.get('close')
                            v_col = cols.get('volume')

                            if c_col:
                                last_close = float(df.iloc[-1][c_col])
                                self.history_cache[code] = {'prev': last_close}

                            if c_col and len(df) >= 2:
                                prev_close = float(df.iloc[-2][c_col])
                                change = last_close - prev_close
                                pct = (change / prev_close) * 100 if prev_close != 0 else 0

                                change_str = f"{change:+.2f}"
                                pct_str = f"{pct:+.2f}%"

                                if pct >= 9.5:
                                    fg_color = QColor("#FFFFFF")  # 漲停字體維持白色以確保對比度
                                    bg_color = "#D32F2F"  # 恢復明顯的實心紅底色
                                elif pct <= -9.5:
                                    fg_color = QColor("#FFFFFF")  # 跌停字體維持白色
                                    bg_color = "#2E7D32"  # 恢復明顯的實心綠底色
                                elif change > 0:
                                    fg_color = QColor("#FF3333")
                                elif change < 0:
                                    fg_color = QColor("#00FF00")
                                else:
                                    fg_color = QColor("#FFFFFF")

                            if v_col:
                                volume = float(df.iloc[-1][v_col])
                                vol_str = f"{int(volume / 1000):,}"

                    except Exception as e:
                        print(f"Error reading {target_file}: {e}")

                if last_close > 0:
                    self._set_cell(i, 2, f"{last_close:.2f}", fg_color, bg_color=bg_color, is_num=True)
                    self._set_cell(i, 3, change_str, fg_color, bg_color=bg_color, is_num=True)
                    self._set_cell(i, 4, pct_str, fg_color, bg_color=bg_color, is_num=True)
                    self._set_cell(i, 5, vol_str, QColor("#FFFF00"), is_num=True)
                    self._set_cell(i, 6, vol_str, QColor("#CCCCCC"), is_num=True)
                else:
                    for c in range(2, 7):
                        self._set_cell(i, c, "-", is_num=True)

        except Exception as e:
            print(f"Error refreshing table: {e}")
        finally:
            self.table.setSortingEnabled(True)
            self.table.setUpdatesEnabled(True)

            if not self.has_auto_selected:
                QTimer.singleShot(500, self._auto_select_first_row)

    def force_trigger_first_selection(self):
        if self.table.rowCount() > 0:
            self.table.selectRow(0)
            item = self.table.item(0, 0)
            if item:
                self.on_row_clicked(0, 0)

    def update_streaming_data(self, data):
        self.table.setUpdatesEnabled(False)
        try:
            def safe_float(v):
                try:
                    return float(v)
                except:
                    return 0.0

            def to_lots_str(v):
                if v == '-' or v == '' or v is None: return '-'
                try:
                    return f"{int(float(v) / 1000):,}"
                except:
                    return '-'

            for code, stock_data in data.items():
                row = self.row_mapping.get(code)
                if row is None: continue

                real = stock_data.get('realtime', {})
                info = stock_data.get('info', {})

                try:
                    latest = safe_float(real.get('latest_trade_price'))
                    close_p = safe_float(real.get('close'))
                    price = latest if latest > 0 else close_p
                    if price == 0: continue

                    api_prev = safe_float(real.get('previous_close'))
                    prev_close = api_prev if api_prev > 0 else self.history_cache.get(code, {}).get('prev', 0)

                    color = QColor("white")
                    bg_color = None
                    change_str = "-"
                    pct_str = "-"

                    if prev_close > 0:
                        change = price - prev_close
                        pct = (change / prev_close) * 100

                        change_str = f"{change:+.2f}"
                        pct_str = f"{pct:+.2f}%"

                        if pct >= 9.5:
                            color = QColor("#FFFFFF")  # 漲停字體維持白色
                            bg_color = "#D32F2F"  # 恢復明顯的實心紅底色
                        elif pct <= -9.5:
                            color = QColor("#FFFFFF")  # 跌停字體維持白色
                            bg_color = "#2E7D32"  # 恢復明顯的實心綠底色
                        elif change > 0:
                            color = QColor("#FF3333")
                        elif change < 0:
                            color = QColor("#00FF00")

                    self._set_cell(row, 2, f"{price:.2f}", color, bg_color=bg_color, is_num=True)
                    self._set_cell(row, 3, change_str, color, bg_color=bg_color, is_num=True)
                    self._set_cell(row, 4, pct_str, color, bg_color=bg_color, is_num=True)

                    vol_str = to_lots_str(real.get('trade_volume', '-'))
                    total_vol_str = to_lots_str(real.get('accumulate_trade_volume', '-'))

                    self._set_cell(row, 5, vol_str, QColor("#FFFF00"), is_num=True)
                    self._set_cell(row, 6, total_vol_str, QColor("#CCCCCC"), is_num=True)

                    t = info.get('time', '-')
                    if ' ' in t: t = t.split(' ')[1]
                    self._set_cell(row, 7, t, QColor("#AAA"))

                except Exception:
                    pass
        finally:
            self.table.setUpdatesEnabled(True)