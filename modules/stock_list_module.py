import sys
import json
import datetime
import pandas as pd
from pathlib import Path
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QTableWidget, QTableWidgetItem,
                             QHeaderView, QAbstractItemView, QLineEdit,
                             QHBoxLayout, QPushButton, QCompleter, QMenu, QComboBox, QMessageBox)
# 🔥 [修正] 這裡補上了 QTimer
from PyQt6.QtCore import pyqtSignal, Qt, QStringListModel, QTimer
from PyQt6.QtGui import QColor, QAction, QFont, QBrush

from utils.data_downloader import DataDownloader
from utils.quote_worker import QuoteWorker

DEFAULT_WATCHLISTS = {
    "我的持股": ["2330", "2317", "2603"],
    "觀察名單": []
}


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
            ("price", "成交", 75),
            ("change_val", "漲跌", 65),
            ("change_pct", "漲跌%", 75),
            ("tick_vol", "單量", 60),
            ("total_vol", "總量", 70),
            ("time", "時間", 0),
        ]

        if shared_worker:
            self.quote_worker = shared_worker
        else:
            self.quote_worker = QuoteWorker(self)
            # self.quote_worker.start()  <-- 確保這裡沒有啟動

        self.quote_worker.quote_updated.connect(self.update_streaming_data)

        if hasattr(self.quote_worker, 'oneshot_finished'):
            self.quote_worker.oneshot_finished.connect(self.on_oneshot_finished)

        self.init_ui()
        self.check_if_data_up_to_date()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        top_container = QWidget()
        top_container.setStyleSheet("background: #111; border-bottom: 1px solid #333;")
        top_layout = QHBoxLayout(top_container)
        top_layout.setContentsMargins(5, 5, 5, 5)

        self.group_combo = QComboBox()
        self.group_combo.addItems(list(self.watchlists.keys()))
        self.group_combo.setCurrentText(self.current_group)
        self.group_combo.setStyleSheet("QComboBox { background: #222; color: #FFF; border: 1px solid #444; }")
        self.group_combo.currentTextChanged.connect(self.change_group)

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
        self.btn_add.clicked.connect(self.add_stock_to_list)

        top_layout.addWidget(self.group_combo, 2)
        top_layout.addWidget(self.btn_monitor, 1)
        top_layout.addWidget(self.input_stock, 5)
        top_layout.addWidget(self.btn_add, 1)
        layout.addWidget(top_container)

        self.table = QTableWidget()
        self.setup_table_columns()
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; font-family: 'Consolas', 'Microsoft JhengHei'; font-size: 14px; }
            QHeaderView::section { background-color: #1A1A1A; color: #BBB; border: none; font-weight: bold; }
            QTableWidget::item { border-bottom: 1px solid #222; padding-right: 5px; }
            QTableWidget::item:selected { background-color: #333; color: #FFF; }
        """)

        self.table.cellClicked.connect(self.on_row_clicked)
        self.table.customContextMenuRequested.connect(self.open_context_menu)
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)

        layout.addWidget(self.table)

        self.refresh_table()

    def _auto_select_first_row(self):
        """啟動時自動選取列表中的第一支股票"""
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
            # 🔥 關鍵修正：確保只有按下去時才啟動 Driver
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

    def update_completer_model(self):
        search_list = [f"{c} {i['name']}" for c, i in self.stock_db.items()]
        self.completer.setModel(QStringListModel(search_list))

    def _resolve_stock_code(self, text):
        text = text.strip().upper()
        if not text: return None
        if text in self.stock_db: return text
        code_part = text.split(' ')[0]
        if code_part in self.stock_db: return code_part
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

    def open_context_menu(self, pos):
        idx = self.table.indexAt(pos)
        if not idx.isValid(): return
        menu = QMenu()
        act = QAction("🗑️ 刪除", self)
        act.triggered.connect(lambda: self.delete_stock(idx.row()))
        menu.addAction(act)
        menu.exec(self.table.viewport().mapToGlobal(pos))

    def delete_stock(self, row):
        code = self.table.item(row, 0).text()
        curr = self.watchlists[self.current_group]
        if code in curr:
            curr.remove(code)
            self.save_watchlists()
            self.refresh_table()

    def change_group(self, group_name):
        self.current_group = group_name
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

    def _set_cell(self, row, col, text, color=None, is_num=False):
        if is_num:
            item = NumericTableWidgetItem(str(text))
        else:
            item = QTableWidgetItem(str(text))

        if color:
            item.setForeground(color)
        else:
            item.setForeground(QColor("white"))

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

            # 如果按鈕是開啟狀態，才設定監控
            if hasattr(self, 'quote_worker') and self.btn_monitor.isChecked():
                self.quote_worker.set_monitoring_stocks(current_list, source='watchlist')

            base_cache_path = Path("data/cache/tw")

            for i, code in enumerate(current_list):
                self.row_mapping[code] = i
                info = self.stock_db.get(code, {"name": code, "market": "TW"})

                # 🔥 [修正] 改用廣泛比對，不依賴 DB 內的 market 欄位文字
                last_close = 0
                target_file = None
                # 同時檢查所有可能的後綴
                for suffix in ["_TW.parquet", "_TWO.parquet", ".parquet"]:
                    p = base_cache_path / f"{code}{suffix}"
                    if p.exists():
                        target_file = p
                        break

                if target_file:
                    try:
                        df = pd.read_parquet(target_file)
                        if not df.empty:
                            # 🔥 [修正] 同時相容 'close' 與 'Close' 欄位
                            cols = df.columns
                            close_col = 'close' if 'close' in cols else 'Close'
                            if close_col in cols:
                                last_close = float(df.iloc[-1][close_col])
                                self.history_cache[code] = {'prev': last_close}
                    except Exception as e:
                        print(f"Error reading {target_file}: {e}")

                # 建立 ID Item
                item_id = QTableWidgetItem(code)
                item_id.setForeground(QColor("#00E5FF"))
                item_id.setFont(QFont("Consolas", 11, QFont.Weight.Bold))
                item_id.setData(Qt.ItemDataRole.UserRole, info['market'])
                self.table.setItem(i, 0, item_id)

                # 建立名稱 Item
                item_name = QTableWidgetItem(info['name'])
                item_name.setForeground(QColor("white"))
                self.table.setItem(i, 1, item_name)

                # 填入成交價 (有抓到快取就填入，沒抓到就顯示 -)
                if last_close > 0:
                    self._set_cell(i, 2, f"{last_close:.2f}", QColor("#CCCCCC"), is_num=True)
                else:
                    self._set_cell(i, 2, "-", is_num=True)

                # 其他欄位初始清空
                for c in range(3, 8):
                    self._set_cell(i, c, "-", is_num=True)

        except Exception as e:
            print(f"Error refreshing table: {e}")
        finally:
            self.table.setSortingEnabled(True)
            self.table.setUpdatesEnabled(True)

            # 延遲觸發選取
            if not self.has_auto_selected:
                QTimer.singleShot(500, self._auto_select_first_row)

    # 在 stock_list_module.py 的 StockListModule 類別中新增此方法
    def force_trigger_first_selection(self):
        """強制選取表格中的第一支股票並發出選取訊號"""
        if self.table.rowCount() > 0:
            # 選取第一列
            self.table.selectRow(0)
            # 取得第一欄的 Item (股票代號)
            item = self.table.item(0, 0)
            if item:
                # 呼叫既有的點擊處理邏輯，這會觸發訊號發送給 main_app
                self.on_row_clicked(0, 0)

    def update_streaming_data(self, data):
        self.table.setUpdatesEnabled(False)
        try:
            def safe_float(v):
                try:
                    return float(v)
                except:
                    return 0.0

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
                    change_str = "-"
                    pct_str = "-"

                    if prev_close > 0:
                        change = price - prev_close
                        pct = (change / prev_close) * 100
                        if change > 0:
                            color = QColor("#FF3333")
                        elif change < 0:
                            color = QColor("#00FF00")
                        change_str = f"{change:+.2f}"
                        pct_str = f"{pct:+.2f}%"

                    self._set_cell(row, 2, f"{price:.2f}", color, is_num=True)
                    self._set_cell(row, 3, change_str, color, is_num=True)
                    self._set_cell(row, 4, pct_str, color, is_num=True)

                    vol = real.get('trade_volume', '-')
                    total_vol = real.get('accumulate_trade_volume', '-')

                    self._set_cell(row, 5, str(vol), QColor("#FFFF00"), is_num=True)
                    self._set_cell(row, 6, str(total_vol), None, is_num=True)

                    t = info.get('time', '-')
                    if ' ' in t: t = t.split(' ')[1]
                    self._set_cell(row, 7, t, QColor("#AAA"))

                except Exception:
                    pass
        finally:
            self.table.setUpdatesEnabled(True)