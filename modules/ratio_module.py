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

try:
    from utils.crawler_profitability import get_profitability
except ImportError:
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from utils.crawler_profitability import get_profitability

plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


class RatioWorker(QThread):
    data_loaded = pyqtSignal(pd.DataFrame)

    def __init__(self, stock_id):
        super().__init__()
        self.stock_id = stock_id

    def run(self):
        print(f"🕷️ [Ratio] 正在爬取 {self.stock_id} 三率資料...")
        df = get_profitability(self.stock_id)
        self.data_loaded.emit(df)


class RatioModule(QWidget):
    stock_changed = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_stock_id = ""
        self.current_stock_name = ""
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

        title = QLabel("獲利能力指標 (三率)")
        title.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 16px;")

        self.info_label = QLabel("移動滑鼠查看數據...")
        self.info_label.setFixedWidth(400)
        self.info_label.setStyleSheet("font-family: 'Consolas'; font-size: 13px; color: #888;")

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
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["季度", "毛利率", "營益率", "淨利率"])
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

    def load_ratio_data(self, stock_id, stock_name=""):
        self.current_stock_id = stock_id
        self.current_stock_name = stock_name

        display_id = stock_id.split('_')[0]

        # 🔥 修正：顯示代號+名稱
        if stock_name:
            self.lbl_stock_info.setText(f"{display_id} {stock_name}")
        else:
            self.lbl_stock_info.setText(f"{display_id}")

        self.info_label.setText(f"⏳ 更新數據中...")
        self.lbl_update_date.setVisible(False)
        self.fig.clear()
        self.canvas.draw()
        self.table.setRowCount(0)

        if hasattr(self, 'worker') and self.worker.isRunning():
            self.worker.terminate()
            self.worker.wait()

        self.worker = RatioWorker(display_id)
        self.worker.data_loaded.connect(self.process_data)
        self.worker.start()

    def process_data(self, df):
        if df.empty:
            self.info_label.setText("⚠️ 查無三率資料")
            return

        try:
            df = df.rename(columns={'季別': 'Quarter'})
            for col in ['毛利率', '營益率', '營業收入', '稅後淨利']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

            df['淨利率'] = df.apply(
                lambda row: (row['稅後淨利'] / row['營業收入'] * 100) if row['營業收入'] != 0 else 0,
                axis=1
            )
            df['Gross'] = df['毛利率']
            df['Operating'] = df['營益率']
            df['Net'] = df['淨利率']

            # 更新日期
            if not df.empty:
                sorted_quarters = sorted(df['Quarter'], reverse=True)
                if sorted_quarters:
                    self.lbl_update_date.setText(f"資料季度: {sorted_quarters[0]}")
                    self.lbl_update_date.setVisible(True)

            self.info_label.setText("✅ 更新完成")
            self.update_ui(df)

        except Exception as e:
            print(f"❌ [Ratio] 處理錯誤: {e}")
            self.info_label.setText("❌ 數據解析錯誤")

    def update_ui(self, df):
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        ax.set_facecolor('#000000')

        plot_data = df.head(8).iloc[::-1]
        self.plot_df = plot_data.copy()

        x = np.arange(len(plot_data))

        ax.plot(x, plot_data['Gross'], color='#E040FB', marker='o', linewidth=2, label='毛利率')
        ax.plot(x, plot_data['Operating'], color='#FF9100', marker='s', linewidth=2, label='營益率')
        ax.plot(x, plot_data['Net'], color='#2979FF', marker='^', linewidth=2, label='淨利率')

        ax.set_xticks(x)
        ax.set_xticklabels(plot_data['Quarter'], color='white', fontsize=9)
        ax.tick_params(axis='y', colors='white', labelsize=9)
        ax.grid(True, color='#333', linestyle=':')
        ax.legend(facecolor='#111', edgecolor='#333', labelcolor='white', fontsize=8)

        for spine in ax.spines.values():
            spine.set_edgecolor('#444')

        self.canvas.draw()

        self.table.setRowCount(len(df))
        for i, (idx, row) in enumerate(df.iterrows()):
            items = [
                QTableWidgetItem(str(row['Quarter'])),
                QTableWidgetItem(f"{row['Gross']:.1f}%"),
                QTableWidgetItem(f"{row['Operating']:.1f}%"),
                QTableWidgetItem(f"{row['Net']:.1f}%")
            ]

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
                f"<span style='color:#E040FB;'>■ 毛:{row['Gross']:.1f}%</span> "
                f"<span style='color:#FF9100;'>■ 營:{row['Operating']:.1f}%</span> "
                f"<span style='color:#2979FF;'>■ 淨:{row['Net']:.1f}%</span>"
            )
            self.info_label.setText(html)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = RatioModule()
    win.load_ratio_data("2330_TW", "台積電")
    win.resize(600, 500)
    win.show()
    sys.exit(app.exec())