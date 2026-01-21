import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
                             QTableWidgetItem, QHeaderView, QApplication, QLabel)
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtCore import pyqtSignal, Qt
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


class RatioModule(QWidget):
    stock_changed = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.stock_changed.connect(self.load_ratio_data)
        self.plot_df = None
        self.init_ui()

    def init_ui(self):
        self.setStyleSheet("background-color: #000000; color: #FFFFFF;")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # 1. Header
        header_widget = QWidget()
        header_widget.setFixedHeight(35)
        header_widget.setStyleSheet("background-color: #050505; border-bottom: 1px solid #333;")
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(10, 0, 10, 0)

        title = QLabel("獲利能力指標 (三率)")
        title.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 14px;")

        self.info_label = QLabel(" 移至圖表查看三率數值")
        self.info_label.setStyleSheet("font-family: 'Consolas'; font-size: 12px; color: #888;")
        self.info_label.setTextFormat(Qt.TextFormat.RichText)

        header_layout.addWidget(title)
        header_layout.addWidget(self.info_label)
        header_layout.addStretch()
        layout.addWidget(header_widget)

        # 2. Canvas
        self.fig = Figure(facecolor='#000000')
        self.canvas = FigureCanvas(self.fig)
        layout.addWidget(self.canvas, stretch=6)

        # 3. Table
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["季度", "毛利率", "營益率", "淨利率"])
        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; gridline-color: #333; color: #FFF; border: none; font-size: 13px; }
            QHeaderView::section { background-color: #1A1A1A; color: #00FFFF; font-weight: bold; height: 30px; border: 1px solid #333; }
        """)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table, stretch=4)

        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)

    def load_ratio_data(self, stock_id):
        # 暫時使用模擬數據
        df = self._generate_mock_data()
        self.update_ui(df)

    def _generate_mock_data(self):
        quarters = []
        for year in [2024, 2025]:
            for q in range(1, 5):
                quarters.append(f"{year}Q{q}")

        # 模擬三率遞減 (毛利 > 營益 > 淨利)
        gross = np.random.uniform(40, 55, 8)
        op = gross - np.random.uniform(10, 15, 8)
        net = op - np.random.uniform(2, 5, 8)

        return pd.DataFrame({
            'Quarter': quarters,
            'Gross': gross,
            'Operating': op,
            'Net': net
        })

    def update_ui(self, df):
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        ax.set_facecolor('#000000')

        self.plot_df = df.copy()
        x = np.arange(len(df))

        # 繪製三條折線
        # 毛利(紫), 營益(橘), 淨利(藍)
        ax.plot(x, df['Gross'], color='#E040FB', marker='o', linewidth=2, label='毛利率')
        ax.plot(x, df['Operating'], color='#FF9100', marker='s', linewidth=2, label='營益率')
        ax.plot(x, df['Net'], color='#2979FF', marker='^', linewidth=2, label='淨利率')

        # 軸設定
        ax.set_xticks(x)
        ax.set_xticklabels(df['Quarter'], color='white', fontsize=9)
        ax.tick_params(axis='y', colors='white', labelsize=9)
        ax.grid(True, color='#333', linestyle=':')

        # 邊框
        for spine in ax.spines.values():
            spine.set_edgecolor('#444')

        self.canvas.draw()

        # 表格更新
        table_df = df.sort_index(ascending=False)
        self.table.setRowCount(len(table_df))

        for i, (idx, row) in enumerate(table_df.iterrows()):
            items = [
                QTableWidgetItem(row['Quarter']),
                QTableWidgetItem(f"{row['Gross']:.1f}%"),
                QTableWidgetItem(f"{row['Operating']:.1f}%"),
                QTableWidgetItem(f"{row['Net']:.1f}%")
            ]

            # 對應折線顏色
            items[1].setForeground(QColor("#E040FB"))
            items[2].setForeground(QColor("#FF9100"))
            items[3].setForeground(QColor("#2979FF"))

            for j, item in enumerate(items):
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(i, j, item)

    def on_mouse_move(self, event):
        if not event.inaxes or self.plot_df is None: return

        idx = int(round(event.xdata))
        if 0 <= idx < len(self.plot_df):
            row = self.plot_df.iloc[idx]
            q = row['Quarter']

            html = (
                f"<span style='color:#DDD;'>{q}</span> | "
                f"<span style='color:#E040FB;'>■ 毛利:{row['Gross']:.1f}%</span>  "
                f"<span style='color:#FF9100;'>■ 營益:{row['Operating']:.1f}%</span>  "
                f"<span style='color:#2979FF;'>■ 淨利:{row['Net']:.1f}%</span>"
            )
            self.info_label.setText(html)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = RatioModule()
    win.load_ratio_data("2330")
    win.resize(600, 400)
    win.show()
    sys.exit(app.exec())