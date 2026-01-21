import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# 補齊 UI 必要元件
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
                             QTableWidgetItem, QHeaderView, QApplication, QLabel)
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtCore import pyqtSignal, Qt
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


class EPSModule(QWidget):
    stock_changed = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.stock_changed.connect(self.load_eps_data)
        self.plot_df = None
        self.init_ui()

    def init_ui(self):
        self.setStyleSheet("background-color: #000000; color: #FFFFFF;")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # 1. 標題與資訊列
        header_widget = QWidget()
        header_widget.setFixedHeight(35)
        header_widget.setStyleSheet("background-color: #050505; border-bottom: 1px solid #333;")
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(10, 0, 10, 0)

        title = QLabel("每股盈餘 (EPS) 季度趨勢")
        title.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 14px;")

        self.info_label = QLabel(" 移至圖表查看數據")
        self.info_label.setStyleSheet("font-family: 'Consolas'; font-size: 12px; color: #888;")
        self.info_label.setTextFormat(Qt.TextFormat.RichText)

        header_layout.addWidget(title)
        header_layout.addWidget(self.info_label)
        header_layout.addStretch()
        layout.addWidget(header_widget)

        # 2. 畫布
        self.fig = Figure(facecolor='#000000')
        self.canvas = FigureCanvas(self.fig)
        layout.addWidget(self.canvas, stretch=6)

        # 3. 表格
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["季度", "EPS", "季增率", "年增率"])
        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; gridline-color: #333; color: #FFF; border: none; font-size: 13px; }
            QHeaderView::section { background-color: #1A1A1A; color: #00FFFF; font-weight: bold; height: 30px; border: 1px solid #333; }
        """)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table, stretch=4)

        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)

    def load_eps_data(self, stock_id):
        # 暫時使用模擬數據，待串接 Goodinfo
        df = self._generate_mock_data()
        self.update_ui(df)

    def _generate_mock_data(self):
        # 模擬過去 8 季
        quarters = []
        for year in [2024, 2025]:
            for q in range(1, 5):
                quarters.append(f"{year}Q{q}")

        eps = np.random.uniform(1.5, 5.0, 8)
        # 模擬偶爾虧損
        if np.random.random() > 0.8: eps[2] = -0.5

        return pd.DataFrame({
            'Quarter': quarters,
            'EPS': eps,
            'QoQ': np.random.uniform(-10, 20, 8),
            'YoY': np.random.uniform(-5, 25, 8)
        })

    def update_ui(self, df):
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        ax.set_facecolor('#000000')

        self.plot_df = df.copy()
        x = np.arange(len(df))

        # 繪製柱狀圖 (紅正綠負)
        colors = ['#FF3333' if v >= 0 else '#00FF00' for v in df['EPS']]
        bars = ax.bar(x, df['EPS'], color=colors, alpha=0.9, width=0.6)

        # 軸設定
        ax.set_xticks(x)
        ax.set_xticklabels(df['Quarter'], color='white', fontsize=9, rotation=0)
        ax.tick_params(axis='y', colors='white', labelsize=9)
        ax.grid(True, color='#333', linestyle=':', axis='y')

        for spine in ax.spines.values():
            spine.set_edgecolor('#444')

        self.canvas.draw()

        # 表格更新
        table_df = df.sort_index(ascending=False)  # 最新季度在最上面
        self.table.setRowCount(len(table_df))

        for i, (idx, row) in enumerate(table_df.iterrows()):
            items = [
                QTableWidgetItem(row['Quarter']),
                QTableWidgetItem(f"{row['EPS']:.2f}元"),
                QTableWidgetItem(f"{row['QoQ']:+.2f}%"),
                QTableWidgetItem(f"{row['YoY']:+.2f}%")
            ]

            # 顏色邏輯
            items[1].setForeground(QColor("#FFCC00"))  # EPS 金色
            items[2].setForeground(QColor("#FF3333" if row['QoQ'] >= 0 else "#00FF00"))
            items[3].setForeground(QColor("#FF3333" if row['YoY'] >= 0 else "#00FF00"))

            for j, item in enumerate(items):
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(i, j, item)

    def on_mouse_move(self, event):
        if not event.inaxes or self.plot_df is None: return

        # 抓取 Bar
        # 簡單判定：X軸座標四捨五入
        idx = int(round(event.xdata))
        if 0 <= idx < len(self.plot_df):
            row = self.plot_df.iloc[idx]
            q = row['Quarter']
            eps = row['EPS']
            yoy = row['YoY']

            color = "#FF3333" if eps >= 0 else "#00FF00"

            html = (
                f"<span style='color:#DDD;'>{q}</span> | "
                f"<span style='color:{color}; font-weight:bold;'>■ EPS:{eps:.2f}元</span> | "
                f"<span style='color:#FFF;'>YoY: {yoy:+.2f}%</span>"
            )
            self.info_label.setText(html)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = EPSModule()
    win.load_eps_data("2330")
    win.resize(600, 400)
    win.show()
    sys.exit(app.exec())