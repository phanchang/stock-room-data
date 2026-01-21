import sys
import pandas as pd
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QTableWidget, QTableWidgetItem,
                             QHeaderView, QApplication, QComboBox, QAbstractItemView)
from PyQt6.QtCore import pyqtSignal, Qt
from PyQt6.QtGui import QColor, QFont


class StockListModule(QWidget):
    stock_selected = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        self.setStyleSheet("background-color: #000000; color: #FFFFFF; border: 1px solid #222;")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        # 1. 下拉選單
        self.list_selector = QComboBox()
        self.list_selector.addItems(["⭐ 我的自選清單 A", "⭐ 我的自選清單 B", "🚀 每日策略篩選"])
        self.list_selector.setStyleSheet("""
            QComboBox { 
                background-color: #1A1A1A; color: #00FFFF; border: 1px solid #444; 
                padding: 10px; font-size: 18px; font-weight: bold; border-radius: 5px;
            }
        """)
        layout.addWidget(self.list_selector)

        # 2. 股票表格
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["代碼", "名稱", "現價", "5日%"])

        self.table.setStyleSheet("""
            QTableWidget { 
                background-color: #000000; color: #FFFFFF; gridline-color: #222; 
                border: none; font-size: 16px; font-family: 'Microsoft JhengHei';
            }
            QHeaderView::section { 
                background-color: #111; color: #00FFFF; font-weight: bold; 
                height: 45px; border: 1px solid #333; font-size: 15px;
            }
        """)

        # 🟢 修正遮擋：自動調整欄位寬度
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)  # 預設平分
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)  # 名稱依長度

        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(50)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self.table.itemClicked.connect(self._on_item_clicked)
        layout.addWidget(self.table)

    def load_data(self, df):
        self.table.setRowCount(len(df))
        for i, row in df.iterrows():
            items = [
                QTableWidgetItem(str(row['id']).replace('_TW', '')),
                QTableWidgetItem(str(row['name'])),
                QTableWidgetItem(f"{row['price']:.2f}"),
                QTableWidgetItem(f"{row['pct_5']:+.1f}%")
            ]

            # 正紅負綠
            items[3].setForeground(QColor("#FF3333" if row['pct_5'] >= 0 else "#00FF00"))
            items[3].setFont(QFont("Consolas", 18, QFont.Weight.Bold))

            for j, item in enumerate(items):
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(i, j, item)

    def _on_item_clicked(self, item):
        sid = self.table.item(item.row(), 0).text() + "_TW"
        self.stock_selected.emit(sid)