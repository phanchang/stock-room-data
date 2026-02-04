import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from dotenv import load_dotenv

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
                             QTableWidgetItem, QHeaderView, QApplication, QLabel, QSizePolicy, QPushButton)
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtCore import pyqtSignal, Qt, QThread

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

from utils.crawler_revenue import get_monthly_revenue

plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


class RevenueWorker(QThread):
    data_fetched = pyqtSignal(pd.DataFrame)

    def __init__(self, stock_id):
        super().__init__()
        self.stock_id = stock_id

    def run(self):
        load_dotenv()
        clean_id = self.stock_id.split('_')[0].split('.')[0]
        try:
            print(f"🚀 [爬蟲啟動] 正在抓取 {clean_id} 的月營收...")
            df = get_monthly_revenue(clean_id)
            self.data_fetched.emit(df)
        except Exception as e:
            print(f"❌ [爬蟲錯誤] {e}")
            self.data_fetched.emit(pd.DataFrame())


class RevenueModule(QWidget):
    # 訊號通常用於內部通知，這裡暫時保留
    stock_changed = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_stock_id = ""
        self.current_stock_name = ""
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

        # 1. Header (標準化標題列)
        header_widget = QWidget()
        header_widget.setFixedHeight(45)  # 稍微加高以容納大字體
        header_widget.setStyleSheet("background-color: #050505; border-bottom: 1px solid #333;")

        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(10, 0, 10, 0)
        header_layout.setSpacing(15)

        # 🔥 [Item 1] 股票資訊 (代號+名稱)
        self.lbl_stock_info = QLabel("請選擇股票")
        self.lbl_stock_info.setStyleSheet(
            "color: #FFFF00; font-weight: bold; font-size: 18px; font-family: 'Microsoft JhengHei';")

        # 分隔線
        sep = QLabel("|")
        sep.setStyleSheet("color: #444; font-size: 16px;")

        # 模組標題
        title = QLabel("月營收成長趨勢")
        title.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 16px;")

        # 滑鼠互動資訊
        self.info_label = QLabel("移動滑鼠查看數據...")
        self.info_label.setStyleSheet("font-family: 'Consolas'; font-size: 13px; color: #888;")
        self.info_label.setFixedWidth(350)  # 給予固定寬度避免抖動

        # 🔥 [Item 5] 資料日期標籤
        self.lbl_update_date = QLabel("")
        self.lbl_update_date.setStyleSheet(
            "color: #FF8800; font-size: 12px; border: 1px solid #555; padding: 2px 4px; border-radius: 3px;")
        self.lbl_update_date.setVisible(False)

        # 🔥 [Item 6] 切換視圖按鈕
        self.btn_toggle_chart = QPushButton("切換視圖")
        self.btn_toggle_chart.setFixedSize(80, 26)
        self.btn_toggle_chart.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_toggle_chart.setStyleSheet("""
            QPushButton { background: #333; color: #CCC; border: 1px solid #555; border-radius: 3px; font-size: 12px; }
            QPushButton:hover { background: #555; color: white; }
        """)
        self.btn_toggle_chart.clicked.connect(self.toggle_chart_visibility)

        # 依序加入 Layout
        header_layout.addWidget(self.lbl_stock_info)
        header_layout.addWidget(sep)
        header_layout.addWidget(title)
        header_layout.addWidget(self.info_label)
        header_layout.addStretch()  # 彈簧
        header_layout.addWidget(self.lbl_update_date)
        header_layout.addWidget(self.btn_toggle_chart)

        layout.addWidget(header_widget)

        # 2. Canvas (圖表)
        self.fig = Figure(facecolor='#000000')
        self.canvas = FigureCanvas(self.fig)
        self.canvas.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        layout.addWidget(self.canvas, stretch=6)

        # 3. Table (表格)
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["月份", "營收(億)", "單月YoY", "累月YoY"])
        # 表格樣式優化
        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; gridline-color: #444; color: #FFF; border: none; font-size: 15px; font-family: 'Consolas', 'Microsoft JhengHei'; }
            QHeaderView::section { background-color: #1A1A1A; color: #FFFFFF; font-weight: bold; height: 32px; border: 1px solid #333; font-size: 13px; }
            QTableWidget::item { padding: 4px; }
        """)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table, stretch=4)

        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)

    def toggle_chart_visibility(self):
        """ 切換圖表顯示/隱藏，最大化表格空間 """
        is_visible = self.canvas.isVisible()
        self.canvas.setVisible(not is_visible)

        if is_visible:
            self.btn_toggle_chart.setText("顯示圖表")
            self.btn_toggle_chart.setStyleSheet("background: #004444; color: white; border: 1px solid #00E5FF;")
        else:
            self.btn_toggle_chart.setText("隱藏圖表")
            self.btn_toggle_chart.setStyleSheet("background: #333; color: #CCC; border: 1px solid #555;")

    # 🔥 [關鍵修正] 接收 stock_name 參數
    def load_revenue_data(self, stock_id, stock_name=""):
        # 1. 緩存機制：如果股票跟上次一樣且資料已存在，就不要重抓
        if stock_id == self.current_stock_id and self.table.rowCount() > 0:
            return
        self.current_stock_id = stock_id
        self.current_stock_name = stock_name

        # 解析顯示用代號 (去除 _TW)
        display_id = stock_id.split('_')[0]

        # 更新左上角資訊
        if stock_name:
            self.lbl_stock_info.setText(f"{display_id} {stock_name}")
        else:
            self.lbl_stock_info.setText(f"{display_id}")

        self.info_label.setText("⏳ 連線 MoneyDJ...")
        self.table.setRowCount(0)
        self.fig.clear()
        self.canvas.draw()
        self.lbl_update_date.setVisible(False)

        if self.worker is not None:
            # 重要：先斷開訊號連接，防止舊的 Worker 回傳資料觸發 UI 繪圖
            try:
                self.worker.data_fetched.disconnect()
            except:
                pass

            if self.worker.isRunning():
                # 不要用 terminate()，讓它跑完或在 Worker 內部下 flag
                # 這裡我們選擇直接放生它（它跑完會自動結束），但不再接收它的訊號
                pass

        self.worker = RevenueWorker(stock_id)
        self.worker.data_fetched.connect(self.on_data_received)
        self.worker.start()

    def on_data_received(self, df):
        if df.empty:
            self.info_label.setText("❌ 查無資料")
            return

        self.info_label.setText("✅ 更新完成")

        try:
            df['Revenue'] = df['營收'] / 100000
            df['YoY'] = df['年增率']
            df['Cum_YoY'] = df['累計年增率']
            df['Date'] = pd.to_datetime(df['日期'])
            df['Year'] = df['Date'].dt.year
            df['Month'] = df['Date'].dt.month

            # 更新資料日期標籤
            if not df.empty:
                last_date = df['Date'].max()
                self.lbl_update_date.setText(f"資料日期: {last_date.strftime('%Y-%m')}")
                self.lbl_update_date.setVisible(True)

            recent_years = sorted(df['Year'].unique(), reverse=True)[:3]
            self.update_ui(df, recent_years)
        except Exception as e:
            print(f"Data process error: {e}")
            self.info_label.setText("❌ 資料格式錯誤")

    def update_ui(self, df, years):
        self.fig.clear()
        self.ax = self.fig.add_subplot(111)
        self.ax.set_facecolor('#000000')

        self.current_years = years
        self.year_data_map = {}
        colors = ['#FF8C00', '#FFD700', '#FF69B4']
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

        # 加入圖例
        self.ax.legend(facecolor='#111', edgecolor='#333', labelcolor='white', fontsize=8, loc='upper left')

        self.canvas.draw()

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

            is_high = (rev == max_rev)
            if is_high:
                for item in items:
                    item.setBackground(QColor(180, 140, 0))
                    item.setForeground(QColor(0, 0, 0))
                    item.setFont(QFont("Consolas", 12, QFont.Weight.Bold))
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
    # 測試時模擬傳入名稱
    module.load_revenue_data("2330_TW", "台積電")
    module.resize(600, 800)
    module.show()
    sys.exit(app.exec())
