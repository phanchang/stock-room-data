import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# UI 元件
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
                             QTableWidgetItem, QHeaderView, QApplication, QLabel)
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtCore import pyqtSignal, Qt, QThread

# 圖表元件
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

# 🟢 引用你的爬蟲 (請確保 crawler_margin_trading.py 在 utils 資料夾下)
from utils.crawler_margin_trading import get_margin_trading

plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


# --- 背景工作執行緒 (避免卡死介面) ---
class MarginWorker(QThread):
    data_fetched = pyqtSignal(pd.DataFrame)

    def __init__(self, stock_id):
        super().__init__()
        self.stock_id = stock_id

    def run(self):
        # 呼叫你的爬蟲函數 (輸入純數字代號)
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
        self.raw_df = None
        self.plot_df = None
        self.worker = None  # 儲存 thread 實體
        self.stock_changed.connect(self.load_margin_data)
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

        title = QLabel("資券籌碼分析 (即時爬蟲)")
        title.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 14px;")

        self.info_label = QLabel(" 等待資料載入...")
        # 🟢 核心修正：設定固定寬度
        self.info_label.setFixedWidth(600)
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
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["日期", "融資餘額", "融資增減", "融券餘額", "融券增減"])
        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; gridline-color: #333; color: #FFF; border: none; font-size: 13px; }
            QHeaderView::section { background-color: #1A1A1A; color: #00FFFF; font-weight: bold; height: 30px; border: 1px solid #333; }
        """)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table, stretch=4)

        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)

    def load_margin_data(self, stock_id):
        self.info_label.setText("⏳ 正在連線 MoneyDJ 抓取中...")
        self.table.setRowCount(0)  # 清空表格
        self.fig.clear()
        self.canvas.draw()

        # 啟動背景執行緒
        if self.worker is not None and self.worker.isRunning():
            self.worker.terminate()  # 如果有舊的在跑，先停掉

        self.worker = MarginWorker(stock_id)
        self.worker.data_fetched.connect(self.on_data_received)
        self.worker.start()

    def on_data_received(self, df):
        if df.empty:
            self.info_label.setText("❌ 查無資料或連線失敗")
            return

        self.info_label.setText("✅ 資料更新完成")

        # 欄位對應 (Crawler -> UI)
        # 你的爬蟲欄位: date, fin_balance, fin_change, short_balance, short_change, ratio
        self.raw_df = df
        self.update_ui(df)

    def update_ui(self, df):
        self.fig.clear()
        self.ax1 = self.fig.add_subplot(111)
        self.ax2 = self.ax1.twinx()
        self.ax1.set_facecolor('#000000')

        # 取最近 60 天畫圖
        self.plot_df = df.head(60).iloc[::-1].reset_index(drop=True)  # 反轉順序讓舊在左、新在右

        x = np.arange(len(self.plot_df))
        dates = self.plot_df['date'].dt.strftime('%m/%d').tolist()

        # 繪圖
        width = 0.35
        # fin_balance = 融資餘額, short_balance = 融券餘額
        self.ax1.bar(x - width / 2, self.plot_df['fin_balance'], width, color='#FF3333', label='融資', alpha=0.8)
        self.ax1.bar(x + width / 2, self.plot_df['short_balance'], width, color='#00FF00', label='融券', alpha=0.8)

        # 券資比
        self.ax2.plot(x, self.plot_df['ratio'], color='#FFFF00', linewidth=1.5, marker='o', markersize=3,
                      label='券資比')

        # 軸設定
        self.ax1.set_xticks(x[::5])
        self.ax1.set_xticklabels(dates[::5], color='white', fontsize=8)
        self.ax1.tick_params(axis='y', colors='#FF8888', labelsize=8)
        self.ax2.tick_params(axis='y', colors='#FFFF88', labelsize=8)
        self.ax1.grid(True, color='#333', linestyle=':', alpha=0.5)

        for ax in [self.ax1, self.ax2]:
            for spine in ax.spines.values():
                spine.set_edgecolor('#444')

        self.canvas.draw()

        # 更新表格 (顯示前 20 筆)
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

            # 顏色: 增減欄位紅正綠負
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
    win.load_margin_data("2330")  # 測試用
    win.resize(600, 400)
    win.show()
    sys.exit(app.exec())