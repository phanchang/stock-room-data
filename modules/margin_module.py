import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
                             QTableWidgetItem, QHeaderView, QApplication, QLabel, QPushButton, QSizePolicy)
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtCore import pyqtSignal, Qt, QThread

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

from utils.crawler_margin_trading import get_margin_trading

plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


class MarginWorker(QThread):
    data_fetched = pyqtSignal(pd.DataFrame)

    def __init__(self, stock_id):
        super().__init__()
        self.stock_id = stock_id

    def run(self):
        clean_id = self.stock_id.split('_')[0].split('.')[0]
        try:
            print(f"🚀 [爬蟲啟動] 正在抓取 {clean_id} 的資券資料...")
            df = get_margin_trading(clean_id)
            self.data_fetched.emit(df)
        except Exception as e:
            print(f"❌ [爬蟲錯誤] {e}")
            self.data_fetched.emit(pd.DataFrame())


class MarginModule(QWidget):
    stock_changed = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_stock_id = ""
        self.current_stock_name = ""
        self.raw_df = None
        self.plot_df = None
        self.worker = None
        self.stock_changed.connect(self.load_margin_data)
        self.init_ui()

    def init_ui(self):
        self.setStyleSheet("background-color: #000000; color: #FFFFFF;")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # 1. Header
        header_widget = QWidget()
        header_widget.setFixedHeight(45)
        header_widget.setStyleSheet("background-color: #050505; border-bottom: 1px solid #333;")
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(10, 0, 10, 0)
        header_layout.setSpacing(15)

        self.lbl_stock_info = QLabel("請選擇股票")
        self.lbl_stock_info.setStyleSheet(
            "color: #FFFF00; font-weight: bold; font-size: 18px; font-family: 'Microsoft JhengHei';")

        sep = QLabel("|")
        sep.setStyleSheet("color: #444; font-size: 16px;")

        title = QLabel("資券籌碼分析")
        title.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 16px;")

        self.info_label = QLabel("移動滑鼠查看數據...")
        self.info_label.setFixedWidth(400)
        self.info_label.setStyleSheet("font-family: 'Consolas'; font-size: 13px; color: #888;")
        self.info_label.setTextFormat(Qt.TextFormat.RichText)

        self.lbl_update_date = QLabel("")
        self.lbl_update_date.setStyleSheet(
            "color: #FF8800; font-size: 12px; border: 1px solid #555; padding: 2px 4px; border-radius: 3px;")
        self.lbl_update_date.setVisible(False)

        self.btn_toggle_chart = QPushButton("切換視圖")
        self.btn_toggle_chart.setFixedSize(80, 26)
        self.btn_toggle_chart.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_toggle_chart.setStyleSheet("""
            QPushButton { background: #333; color: #CCC; border: 1px solid #555; border-radius: 3px; font-size: 12px; }
            QPushButton:hover { background: #555; color: white; }
        """)
        self.btn_toggle_chart.clicked.connect(self.toggle_chart_visibility)

        header_layout.addWidget(self.lbl_stock_info)
        header_layout.addWidget(sep)
        header_layout.addWidget(title)
        header_layout.addWidget(self.info_label)
        header_layout.addStretch()
        header_layout.addWidget(self.lbl_update_date)
        header_layout.addWidget(self.btn_toggle_chart)

        layout.addWidget(header_widget)

        # 2. Canvas
        self.fig = Figure(facecolor='#000000')
        self.canvas = FigureCanvas(self.fig)
        self.canvas.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        layout.addWidget(self.canvas, stretch=6)

        # 3. Table
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["日期", "融資餘額", "融資增減", "融券餘額", "融券增減"])
        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; gridline-color: #333; color: #FFF; border: none; font-size: 15px; font-family: 'Consolas', 'Microsoft JhengHei'; }
            QHeaderView::section { background-color: #1A1A1A; color: #00FFFF; font-weight: bold; height: 32px; border: 1px solid #333; font-size: 13px; }
            QTableWidget::item { padding: 4px; }
        """)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table, stretch=4)

        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)

    def toggle_chart_visibility(self):
        is_visible = self.canvas.isVisible()
        self.canvas.setVisible(not is_visible)

        if is_visible:
            self.btn_toggle_chart.setText("顯示圖表")
            self.btn_toggle_chart.setStyleSheet("background: #004444; color: white; border: 1px solid #00E5FF;")
        else:
            self.btn_toggle_chart.setText("隱藏圖表")
            self.btn_toggle_chart.setStyleSheet("background: #333; color: #CCC; border: 1px solid #555;")

    def load_margin_data(self, stock_id, stock_name=""):
        self.current_stock_id = stock_id
        self.current_stock_name = stock_name

        display_id = stock_id.split('_')[0]

        # 🔥 修正：顯示代號+名稱
        if stock_name:
            self.lbl_stock_info.setText(f"{display_id} {stock_name}")
        else:
            self.lbl_stock_info.setText(f"{display_id}")

        self.info_label.setText("⏳ 抓取中...")
        self.lbl_update_date.setVisible(False)
        self.table.setRowCount(0)
        self.fig.clear()
        self.canvas.draw()

        if self.worker is not None and self.worker.isRunning():
            self.worker.terminate()

        self.worker = MarginWorker(stock_id)
        self.worker.data_fetched.connect(self.on_data_received)
        self.worker.start()

    def on_data_received(self, df):
        if df.empty:
            self.info_label.setText("❌ 查無資料")
            return

        self.info_label.setText("✅ 更新完成")
        self.raw_df = df

        # 更新日期
        if not df.empty and 'date' in df.columns:
            last_date = df['date'].max()
            self.lbl_update_date.setText(f"資料日期: {last_date.strftime('%Y-%m-%d')}")
            self.lbl_update_date.setVisible(True)

        self.update_ui(df)

    def update_ui(self, df):
        self.fig.clear()
        self.ax1 = self.fig.add_subplot(111)
        self.ax2 = self.ax1.twinx()
        self.ax1.set_facecolor('#000000')

        self.plot_df = df.head(60).iloc[::-1].reset_index(drop=True)

        x = np.arange(len(self.plot_df))
        dates = self.plot_df['date'].dt.strftime('%m/%d').tolist()

        width = 0.35
        self.ax1.bar(x - width / 2, self.plot_df['fin_balance'], width, color='#FF3333', label='融資', alpha=0.8)
        self.ax1.bar(x + width / 2, self.plot_df['short_balance'], width, color='#00FF00', label='融券', alpha=0.8)

        self.ax2.plot(x, self.plot_df['ratio'], color='#FFFF00', linewidth=1.5, marker='o', markersize=3,
                      label='券資比')

        self.ax1.set_xticks(x[::5])
        self.ax1.set_xticklabels(dates[::5], color='white', fontsize=8)
        self.ax1.tick_params(axis='y', colors='#FF8888', labelsize=8)
        self.ax2.tick_params(axis='y', colors='#FFFF88', labelsize=8)
        self.ax1.grid(True, color='#333', linestyle=':', alpha=0.5)

        for ax in [self.ax1, self.ax2]:
            for spine in ax.spines.values():
                spine.set_edgecolor('#444')

        self.canvas.draw()

        display_df = df.head(20)
        self.table.setRowCount(len(display_df))
        for i, row in display_df.iterrows():
            items = [
                QTableWidgetItem(row['date'].strftime('%m-%d')),
                QTableWidgetItem(f"{int(row['fin_balance']):,}"),
                QTableWidgetItem(f"{int(row['fin_change']):+,}"),
                QTableWidgetItem(f"{int(row['short_balance']):,}"),
                QTableWidgetItem(f"{int(row['short_change']):+,}")
            ]

            items[2].setForeground(QColor("#FF3333" if row['fin_change'] >= 0 else "#00FF00"))
            items[4].setForeground(QColor("#FF3333" if row['short_change'] >= 0 else "#00FF00"))

            for j, item in enumerate(items):
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(i, j, item)

    def on_mouse_move(self, event):
        if not event.inaxes or self.plot_df is None: return

        idx = int(round(event.xdata))
        if 0 <= idx < len(self.plot_df):
            row = self.plot_df.iloc[idx]
            date_str = row['date'].strftime('%m/%d')
            mb = int(row['fin_balance'])
            sb = int(row['short_balance'])
            ratio = row['ratio']

            html = (
                f"<span style='color:#DDD;'>{date_str}</span> | "
                f"<span style='color:#FF3333;'>■ 融資:{mb:,}</span> | "
                f"<span style='color:#00FF00;'>■ 融券:{sb:,}</span> | "
                f"<span style='color:#FFFF00;'>■ 券資比:{ratio}%</span>"
            )
            self.info_label.setText(html)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MarginModule()
    win.load_margin_data("2330_TW", "台積電")
    win.resize(600, 500)
    win.show()
    sys.exit(app.exec())
