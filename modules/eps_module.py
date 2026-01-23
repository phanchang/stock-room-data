import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
                             QTableWidgetItem, QHeaderView, QApplication, QLabel)
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtCore import pyqtSignal, Qt, QThread

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

# 🔥 引入您的爬蟲
try:
    from utils.crawler_profitability import get_profitability
except ImportError:
    # 方便測試用，如果路徑不同請自行調整
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from utils.crawler_profitability import get_profitability

plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


# 🟢 背景工作者：負責爬蟲，避免視窗卡死
class EPSWorker(QThread):
    data_loaded = pyqtSignal(pd.DataFrame)

    def __init__(self, stock_id):
        super().__init__()
        self.stock_id = stock_id

    from dotenv import load_dotenv  # 記得 import

    # 在 run() 裡第一行加入
    load_dotenv()
    def run(self):
        print(f"🕷️ [EPS] 正在爬取 {self.stock_id} 獲利能力...")
        df = get_profitability(self.stock_id)
        self.data_loaded.emit(df)


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

        # 1. Header
        header_widget = QWidget()
        header_widget.setFixedHeight(35)
        header_widget.setStyleSheet("background-color: #050505; border-bottom: 1px solid #333;")
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(10, 0, 10, 0)

        title = QLabel("每股盈餘 (EPS) 季度趨勢")
        title.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 14px;")

        self.info_label = QLabel(" 載入數據中...")
        self.info_label.setStyleSheet("font-family: 'Consolas'; font-size: 12px; color: #888;")

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
        self.table.setHorizontalHeaderLabels(["季度", "EPS", "季增率", "年增率"])
        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; gridline-color: #333; color: #FFF; border: none; font-size: 13px; }
            QHeaderView::section { background-color: #1A1A1A; color: #00FFFF; font-weight: bold; height: 30px; border: 1px solid #333; }
        """)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table, stretch=4)

        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)

    def load_eps_data(self, full_stock_id):
        stock_id = full_stock_id.split('_')[0]

        # 🔥 修正重點：強制 UI 狀態重置
        self.info_label.setText(f"⏳ 正在更新 {stock_id} 數據...")
        self.info_label.setStyleSheet("font-family: 'Consolas'; font-size: 12px; color: #YELLOW;")  # 亮黃色提示

        # 1. 清空舊圖表
        self.fig.clear()
        self.canvas.draw()

        # 2. 清空舊表格 (這很重要，不然會誤以為沒更新)
        self.table.setRowCount(0)

        # 3. 停止舊的 Worker (如果還在跑)
        if hasattr(self, 'worker') and self.worker.isRunning():
            self.worker.terminate()
            self.worker.wait()

        # 4. 啟動新任務
        self.worker = EPSWorker(stock_id)
        self.worker.data_loaded.connect(self.process_data)
        self.worker.start()
    def process_data(self, df):
        if df.empty:
            self.info_label.setText("⚠️ 查無 EPS 資料")
            return

        try:
            # 1. 整理欄位
            df = df.rename(columns={'季別': 'Quarter'})

            # 2. 轉為數值 (確保 EPS 是 float)
            df['EPS'] = pd.to_numeric(df['EPS'], errors='coerce').fillna(0)

            # 3. 計算增長率 (需先轉為舊->新排序)
            df_calc = df.iloc[::-1].copy()  # 反轉為 時間小 -> 時間大
            df_calc['QoQ'] = df_calc['EPS'].pct_change(periods=1) * 100
            df_calc['YoY'] = df_calc['EPS'].pct_change(periods=4) * 100  # 假設一年四季

            # 4. 轉回 新 -> 舊 用於顯示
            final_df = df_calc.iloc[::-1].copy()
            final_df = final_df.fillna(0)  # 把 NaN 補 0

            self.info_label.setText("✅ 數據更新完成")
            self.update_ui(final_df)

        except Exception as e:
            print(f"❌ [EPS] 處理錯誤: {e}")
            self.info_label.setText("❌ 數據解析錯誤")

    def update_ui(self, df):
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        ax.set_facecolor('#000000')

        # 只取前 8 季顯示，避免圖太擠
        plot_data = df.head(8).iloc[::-1]  # 轉為舊->新以利繪圖
        self.plot_df = plot_data.copy()

        x = np.arange(len(plot_data))
        eps_values = plot_data['EPS'].values

        # 繪製柱狀圖
        colors = ['#FF3333' if v >= 0 else '#00FF00' for v in eps_values]
        ax.bar(x, eps_values, color=colors, alpha=0.9, width=0.6)

        # 軸設定
        ax.set_xticks(x)
        ax.set_xticklabels(plot_data['Quarter'], color='white', fontsize=9, rotation=0)
        ax.tick_params(axis='y', colors='white', labelsize=9)
        ax.grid(True, color='#333', linestyle=':', axis='y')

        for spine in ax.spines.values():
            spine.set_edgecolor('#444')

        self.canvas.draw()

        # 表格更新 (顯示所有抓到的數據)
        self.table.setRowCount(len(df))
        for i, (idx, row) in enumerate(df.iterrows()):
            items = [
                QTableWidgetItem(str(row['Quarter'])),
                QTableWidgetItem(f"{row['EPS']:.2f}元"),
                QTableWidgetItem(f"{row['QoQ']:+.2f}%" if row['QoQ'] != 0 else "-"),
                QTableWidgetItem(f"{row['YoY']:+.2f}%" if row['YoY'] != 0 else "-")
            ]

            items[1].setForeground(QColor("#FFCC00"))
            items[2].setForeground(QColor("#FF3333" if row['QoQ'] >= 0 else "#00FF00"))
            items[3].setForeground(QColor("#FF3333" if row['YoY'] >= 0 else "#00FF00"))

            for j, item in enumerate(items):
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(i, j, item)

    def on_mouse_move(self, event):
        if not event.inaxes or self.plot_df is None: return
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
    win.load_eps_data("2330")  # 測試用
    win.resize(600, 400)
    win.show()
    sys.exit(app.exec())