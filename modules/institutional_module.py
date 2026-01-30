import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from dotenv import load_dotenv

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
                             QTableWidgetItem, QHeaderView, QApplication, QLabel, QPushButton, QSizePolicy)
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtCore import pyqtSignal, Qt, QThread

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

from utils.crawler_fa import get_fa_ren

plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


class InstitutionalWorker(QThread):
    data_fetched = pyqtSignal(pd.DataFrame)

    def __init__(self, stock_id):
        super().__init__()
        self.stock_id = stock_id

    def run(self):
        load_dotenv()
        clean_id = self.stock_id.split('_')[0].split('.')[0]
        try:
            print(f"🚀 [法人爬蟲] 正在抓取 {clean_id} 的三大法人資料...")
            df = get_fa_ren(clean_id)
            self.data_fetched.emit(df)
        except Exception as e:
            print(f"❌ [法人爬蟲] 失敗: {e}")
            self.data_fetched.emit(pd.DataFrame())


class InstitutionalModule(QWidget):
    stock_changed = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_stock_id = ""
        self.current_stock_name = ""
        self.raw_df = None
        self.plot_df = None
        self.worker = None

        self.CLR_FOREIGN = '#FF4500'
        self.CLR_TRUST = '#FFD700'
        self.CLR_DEALER = '#00FFFF'
        self.CLR_TOTAL = '#FFFFFF'

        self.stock_changed.connect(self.load_inst_data)
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

        title = QLabel("三大法人買賣超")
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
        layout.addWidget(self.canvas, stretch=7)

        # 3. Table
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["期間", "外資", "投信", "自營", "合計"])
        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; gridline-color: #333; color: #FFF; border: none; font-size: 15px; font-family: 'Consolas', 'Microsoft JhengHei'; }
            QHeaderView::section { background-color: #1A1A1A; color: #00FFFF; font-weight: bold; height: 32px; border: 1px solid #333; font-size: 13px; }
            QTableWidget::item { padding: 4px; }
        """)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table, stretch=3)

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

    def load_inst_data(self, stock_id, stock_name=""):
        self.current_stock_id = stock_id
        self.current_stock_name = stock_name

        display_id = stock_id.split('_')[0]

        # 🔥 修正：顯示代號+名稱
        if stock_name:
            self.lbl_stock_info.setText(f"{display_id} {stock_name}")
        else:
            self.lbl_stock_info.setText(f"{display_id}")

        self.info_label.setText("⏳ 更新數據中...")
        self.lbl_update_date.setVisible(False)
        self.table.setRowCount(0)
        self.fig.clear()
        self.canvas.draw()

        if self.worker is not None and self.worker.isRunning():
            self.worker.terminate()

        self.worker = InstitutionalWorker(stock_id)
        self.worker.data_fetched.connect(self.on_data_received)
        self.worker.start()

    def on_data_received(self, df):
        if df.empty:
            self.info_label.setText("❌ 查無資料")
            return

        self.info_label.setText("✅ 更新完成")
        df['Date'] = pd.to_datetime(df['日期'])
        df['Foreign'] = df['外資買賣超股數']
        df['Trust'] = df['投信買賣超股數']
        df['Dealer'] = df['自營商買賣超股數']

        # 更新日期
        if not df.empty:
            last_date = df['Date'].max()
            self.lbl_update_date.setText(f"資料日期: {last_date.strftime('%Y-%m-%d')}")
            self.lbl_update_date.setVisible(True)

        self.raw_df = df
        self.update_ui(df)

    def update_ui(self, df):
        self.fig.clear()
        self.ax1 = self.fig.add_subplot(111)
        self.ax1.set_facecolor('#000000')

        self.plot_df = df.head(30).iloc[::-1].reset_index(drop=True)
        x = np.arange(len(self.plot_df))

        colors = [self.CLR_FOREIGN, self.CLR_TRUST, self.CLR_DEALER]
        inst_cols = ['Foreign', 'Trust', 'Dealer']
        pos_bottom, neg_bottom = np.zeros(len(self.plot_df)), np.zeros(len(self.plot_df))

        for i, col in enumerate(inst_cols):
            vals = self.plot_df[col].fillna(0).values
            p_mask = vals > 0
            p_vals = np.where(p_mask, vals, 0)
            self.ax1.bar(x, p_vals, bottom=pos_bottom, color=colors[i], label=col, alpha=0.9, width=0.7)
            pos_bottom += p_vals

            n_mask = vals <= 0
            n_vals = np.where(n_mask, vals, 0)
            self.ax1.bar(x, n_vals, bottom=neg_bottom, color=colors[i], alpha=0.9, width=0.7)
            neg_bottom += n_vals

        self.ax1.axhline(0, color='#555', linewidth=0.8)
        self.ax1.tick_params(colors='#888', labelsize=8)

        date_labels = [d.strftime('%m/%d') for d in self.plot_df['Date']]
        step = max(1, len(x) // 6)
        self.ax1.set_xticks(x[::step])
        self.ax1.set_xticklabels(date_labels[::step])

        for spine in self.ax1.spines.values():
            spine.set_edgecolor('#444')

        self.ax1.legend(facecolor='#111', edgecolor='#333', labelcolor='white', fontsize=8, loc='upper left')

        self.canvas.draw()

        periods = [1, 5, 10, 20]
        period_labels = ["1日", "5日", "10日", "20日"]
        self.table.setRowCount(len(periods))

        for i, p in enumerate(periods):
            sub_df = df.head(p)
            self.table.setItem(i, 0, QTableWidgetItem(period_labels[i]))
            vals = []
            for j, col in enumerate(inst_cols):
                total = sub_df[col].sum()
                vals.append(total)
                item = QTableWidgetItem(f"{int(total):+,d}")
                item.setForeground(QColor("#FF3333" if total >= 0 else "#00FF00"))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(i, j + 1, item)

            g_total = sum(vals)
            item_t = QTableWidgetItem(f"{int(g_total):+,d}")
            item_t.setForeground(QColor("#FF3333" if g_total >= 0 else "#00FF00"))
            item_t.setFont(QFont("Consolas", 12, QFont.Weight.Bold))
            item_t.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.table.setItem(i, 4, item_t)

    def on_mouse_move(self, event):
        if not event.inaxes or self.plot_df is None: return

        x_idx = int(round(event.xdata))
        if 0 <= x_idx < len(self.plot_df):
            data = self.plot_df.iloc[x_idx]
            f = int(data['Foreign'])
            t = int(data['Trust'])
            d = int(data['Dealer'])
            date_str = data['Date'].strftime('%m/%d')
            total = f + t + d

            html_text = (
                f"<span style='color:#DDD;'>{date_str}</span> | "
                f"<span style='color:{self.CLR_FOREIGN};'>■ 外資:{f:+,d}</span> "
                f"<span style='color:{self.CLR_TRUST};'>■ 投信:{t:+,d}</span> "
                f"<span style='color:{self.CLR_DEALER};'>■ 自營:{d:+,d}</span> | "
                f"<span style='color:{self.CLR_TOTAL};'>合計:{total:+,d}</span>"
            )
            self.info_label.setText(html_text)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    module = InstitutionalModule()
    module.load_inst_data("2330_TW", "台積電")
    module.resize(800, 500)
    module.show()
    sys.exit(app.exec())