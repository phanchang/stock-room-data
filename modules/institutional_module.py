import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from dotenv import load_dotenv

# UI 元件
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
                             QTableWidgetItem, QHeaderView, QApplication, QLabel)
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtCore import pyqtSignal, Qt, QThread

# 圖表元件
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

# 🟢 引入爬蟲
from utils.crawler_fa import get_fa_ren

plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


# --- 背景執行緒 ---
class InstitutionalWorker(QThread):
    data_fetched = pyqtSignal(pd.DataFrame)

    def __init__(self, stock_id):
        super().__init__()
        self.stock_id = stock_id

    def run(self):
        load_dotenv()
        clean_id = self.stock_id.split('_')[0].split('.')[0]
        try:
            print(f"🚀 [爬蟲啟動] 正在抓取 {clean_id} 的三大法人...")
            df = get_fa_ren(clean_id)
            self.data_fetched.emit(df)
        except Exception as e:
            print(f"❌ [爬蟲錯誤] {e}")
            self.data_fetched.emit(pd.DataFrame())


class InstitutionalModule(QWidget):
    stock_changed = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.raw_df = None
        self.plot_df = None
        self.worker = None

        # 法人顏色定義
        self.CLR_FOREIGN = '#FF4500'  # 橘紅
        self.CLR_TRUST = '#FFD700'  # 金黃
        self.CLR_DEALER = '#00FFFF'  # 亮青
        self.CLR_TOTAL = '#FFFFFF'  # 白色

        self.stock_changed.connect(self.load_inst_data)
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

        title = QLabel("三大法人買賣超 (即時爬蟲)")
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
        layout.addWidget(self.canvas, stretch=7)

        # 3. Table
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["期間", "外資", "投信", "自營", "合計"])
        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; gridline-color: #333; color: #FFF; border: none; font-size: 13px; }
            QHeaderView::section { background-color: #1A1A1A; color: #00FFFF; font-weight: bold; height: 32px; border: 1px solid #333; }
        """)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table, stretch=3)

        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)

    def load_inst_data(self, stock_id):
        self.info_label.setText("⏳ 正在連線 MoneyDJ...")
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

        self.info_label.setText("✅ 資料更新完成")

        # 資料處理
        # Crawler Columns: [日期, 外資買賣超股數, 投信買賣超股數, 自營商買賣超股數...]
        df['Date'] = pd.to_datetime(df['日期'])

        # 單位換算：股 -> 張 (除以1000)
        df['Foreign'] = df['外資買賣超股數'] / 1000
        df['Trust'] = df['投信買賣超股數'] / 1000
        df['Dealer'] = df['自營商買賣超股數'] / 1000

        # 這裡需要股價資料來畫線，暫時先不畫股價線，專注於籌碼柱狀圖
        # 如果未來要畫股價，可以從 kline_module 共享資料，或再抓一次

        self.raw_df = df
        self.update_ui(df)

    def update_ui(self, df):
        self.fig.clear()
        self.ax1 = self.fig.add_subplot(111)
        self.ax1.set_facecolor('#000000')

        # 繪圖數據 (取前30筆)
        self.plot_df = df.head(30).sort_values('Date').reset_index(drop=True)
        x = np.arange(len(self.plot_df))

        # 繪製堆疊圖
        colors = [self.CLR_FOREIGN, self.CLR_TRUST, self.CLR_DEALER]
        inst_cols = ['Foreign', 'Trust', 'Dealer']
        pos_bottom, neg_bottom = np.zeros(len(self.plot_df)), np.zeros(len(self.plot_df))

        for i, col in enumerate(inst_cols):
            vals = self.plot_df[col].values
            # 處理 NaN
            vals = np.nan_to_num(vals)

            # 正值堆疊
            p_mask = vals > 0
            p_vals = np.where(p_mask, vals, 0)
            self.ax1.bar(x, p_vals, bottom=pos_bottom, color=colors[i], label=col, alpha=0.9, width=0.7)
            pos_bottom += p_vals

            # 負值堆疊
            n_mask = vals <= 0
            n_vals = np.where(n_mask, vals, 0)
            self.ax1.bar(x, n_vals, bottom=neg_bottom, color=colors[i], alpha=0.9, width=0.7)
            neg_bottom += n_vals

        self.ax1.axhline(0, color='#555', linewidth=0.8)
        self.ax1.tick_params(colors='#888', labelsize=8)

        for spine in self.ax1.spines.values():
            spine.set_edgecolor('#444')

        self.canvas.draw()

        # 表格更新 (累計張數)
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
            item_t.setFont(QFont("Consolas", 11, QFont.Weight.Bold))
            item_t.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.table.setItem(i, 4, item_t)

    def on_mouse_move(self, event):
        if not event.inaxes or self.plot_df is None: return

        idx = int(round(event.xdata))
        if 0 <= idx < len(self.plot_df):
            data = self.plot_df.iloc[idx]
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
                f"<span style='color:{self.CLR_TOTAL};'>合計:{total:+,d}張</span>"
            )
            self.info_label.setText(html_text)
            self.canvas.draw_idle()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    module = InstitutionalModule()
    module.load_inst_data("2330_TW")
    module.resize(800, 600)
    module.show()
    sys.exit(app.exec())