import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from dotenv import load_dotenv  # 確保讀取 Proxy

# UI 元件
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
                             QTableWidgetItem, QHeaderView, QApplication, QLabel)
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtCore import pyqtSignal, Qt, QThread

# 圖表元件
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

# 🟢 引入爬蟲
from utils.crawler_revenue import get_monthly_revenue

plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


# --- 背景執行緒 ---
class RevenueWorker(QThread):
    data_fetched = pyqtSignal(pd.DataFrame)

    def __init__(self, stock_id):
        super().__init__()
        self.stock_id = stock_id

    def run(self):
        load_dotenv()  # 確保 Proxy 環境變數已載入
        clean_id = self.stock_id.split('_')[0].split('.')[0]
        try:
            print(f"🚀 [爬蟲啟動] 正在抓取 {clean_id} 的月營收...")
            df = get_monthly_revenue(clean_id)
            self.data_fetched.emit(df)
        except Exception as e:
            print(f"❌ [爬蟲錯誤] {e}")
            self.data_fetched.emit(pd.DataFrame())


class RevenueModule(QWidget):
    stock_changed = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.stock_changed.connect(self.load_revenue_data)
        self.current_years = []
        self.year_data_map = {}
        self.year_colors = []
        self.worker = None
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

        title = QLabel("月營收成長趨勢 (即時爬蟲)")
        title.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 14px;")

        self.info_label = QLabel(" 等待資料載入...")
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
        self.table.setHorizontalHeaderLabels(["月份", "營收(億)", "單月YoY", "累月YoY"])
        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; gridline-color: #444; color: #FFF; border: none; font-size: 13px; }
            QHeaderView::section { background-color: #1A1A1A; color: #FFFFFF; font-weight: bold; height: 32px; border: 1px solid #333; }
        """)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table, stretch=4)

        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)

    def load_revenue_data(self, stock_id):
        self.info_label.setText("⏳ 正在連線 MoneyDJ...")
        self.table.setRowCount(0)
        self.fig.clear()
        self.canvas.draw()

        if self.worker is not None and self.worker.isRunning():
            self.worker.terminate()

        self.worker = RevenueWorker(stock_id)
        self.worker.data_fetched.connect(self.on_data_received)
        self.worker.start()

    def on_data_received(self, df):
        if df.empty:
            self.info_label.setText("❌ 查無資料")
            return

        self.info_label.setText("✅ 資料更新完成")

        # 資料處理
        # Crawler Columns: [年月, 營收, 月增率, 去年同期, 年增率, 累計營收, 累計年增率, 日期]
        # 轉換單位：千元 -> 億元 (除以 100,000)
        df['Revenue'] = df['營收'] / 100000
        df['YoY'] = df['年增率']
        df['Cum_YoY'] = df['累計年增率']
        df['Date'] = pd.to_datetime(df['日期'])
        df['Year'] = df['Date'].dt.year
        df['Month'] = df['Date'].dt.month

        # 取得最新三個年度
        recent_years = sorted(df['Year'].unique(), reverse=True)[:3]
        self.update_ui(df, recent_years)

    def update_ui(self, df, years):
        self.fig.clear()
        self.ax = self.fig.add_subplot(111)
        self.ax.set_facecolor('#000000')

        self.current_years = years
        self.year_data_map = {}
        colors = ['#FF8C00', '#FFD700', '#FF69B4']  # 橘, 金, 粉
        self.year_colors = colors

        all_revs = []
        for i, year in enumerate(years):
            yd = df[df['Year'] == year].sort_values('Month')
            self.year_data_map[year] = yd
            if not yd.empty:
                self.ax.plot(yd['Month'], yd['Revenue'],
                             color=colors[i], marker='o', label=f'{year}',
                             linewidth=1.5, markersize=4)
                all_revs.extend(yd['Revenue'].tolist())

        # Y 軸縮放
        if all_revs:
            ymin, ymax = min(all_revs), max(all_revs)
            margin = (ymax - ymin) * 0.2
            self.ax.set_ylim(max(0, ymin - margin), ymax + margin)

        self.ax.set_xticks(range(1, 13))
        self.ax.set_xticklabels([f'{m}月' for m in range(1, 13)], color='#FFFFFF', fontsize=9)
        self.ax.tick_params(axis='y', colors='#FFFFFF', labelsize=9)
        self.ax.grid(True, color='#222', linestyle=':')

        for spine in self.ax.spines.values():
            spine.set_edgecolor('#555')

        self.canvas.draw()

        # 更新表格
        display_df = df.sort_values('Date', ascending=False).head(36).reset_index(drop=True)
        self.table.setRowCount(len(display_df))
        max_rev = display_df['Revenue'].max() if not display_df.empty else 0

        for i, row in display_df.iterrows():
            rev = row['Revenue']
            yoy = row.get('YoY', 0)
            cum_yoy = row.get('Cum_YoY', 0)

            items = [
                QTableWidgetItem(row['Date'].strftime('%Y-%m')),
                QTableWidgetItem(f"{rev:.2f}"),
                QTableWidgetItem(f"{yoy:+.1f}%"),
                QTableWidgetItem(f"{cum_yoy:+.1f}%")
            ]

            # 樣式：營收創新高
            is_high = (rev == max_rev)
            if is_high:
                for item in items:
                    item.setBackground(QColor(180, 140, 0))
                    item.setForeground(QColor(0, 0, 0))
                    item.setFont(QFont("Consolas", 11, QFont.Weight.Bold))
            else:
                items[1].setForeground(QColor("#FFFF00"))
                items[2].setForeground(QColor("#FF3333" if yoy >= 0 else "#00FF00"))
                items[3].setForeground(QColor("#FF3333" if cum_yoy >= 0 else "#00FF00"))

            for j, item in enumerate(items):
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(i, j, item)

    def on_mouse_move(self, event):
        if not event.inaxes: return

        month = int(round(event.xdata))
        if 1 <= month <= 12:
            html_parts = [f"<span style='color:#DDD;'>{month}月</span>"]

            for i, year in enumerate(self.current_years):
                yd = self.year_data_map.get(year)
                if yd is not None:
                    row = yd[yd['Month'] == month]
                    if not row.empty:
                        rev = row.iloc[0]['Revenue']
                        color = self.year_colors[i]
                        html_parts.append(f"<span style='color:{color};'>■ {year}:{rev:.1f}億</span>")

            self.info_label.setText(" | ".join(html_parts))


if __name__ == "__main__":
    app = QApplication(sys.argv)
    module = RevenueModule()
    module.load_revenue_data("2330")
    module.resize(500, 800)
    module.show()
    sys.exit(app.exec())