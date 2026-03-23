import sys
import re
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


class EPSWorker(QThread):
    data_loaded = pyqtSignal(pd.DataFrame)

    def __init__(self, stock_id):
        super().__init__()
        self.stock_id = stock_id

    def run(self):
        from dotenv import load_dotenv
        load_dotenv()
        print(f"🕷️ [EPS] 正在爬取 {self.stock_id} 獲利能力...")
        df = get_profitability(self.stock_id)
        self.data_loaded.emit(df)


class EPSModule(QWidget):
    stock_changed = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_stock_id = ""
        self.current_stock_name = ""
        self.stock_changed.connect(self.load_eps_data)
        self.plot_df = None
        self.init_ui()

    def init_ui(self):
        self.setStyleSheet("background-color: #000000; color: #FFFFFF;")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # 1. Header (修改為兩層架構以容納更多資訊)
        header_widget = QWidget()
        header_widget.setFixedHeight(65)  # 👑 加高以容納兩行文字
        header_widget.setStyleSheet("background-color: #050505; border-bottom: 1px solid #333;")

        header_vbox = QVBoxLayout(header_widget)
        header_vbox.setContentsMargins(10, 5, 10, 5)
        header_vbox.setSpacing(5)

        row1_layout = QHBoxLayout()
        row2_layout = QHBoxLayout()

        # 🔥 [Row 1] 股票資訊與標題
        self.lbl_stock_info = QLabel("請選擇股票")
        self.lbl_stock_info.setStyleSheet(
            "color: #FFFF00; font-weight: bold; font-size: 18px; font-family: 'Microsoft JhengHei';")

        sep = QLabel("|")
        sep.setStyleSheet("color: #444; font-size: 16px;")

        # 標題
        title = QLabel("每股盈餘 (EPS) 季度趨勢")
        title.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 16px;")

        # 資料日期
        self.lbl_update_date = QLabel("")
        self.lbl_update_date.setStyleSheet(
            "color: #FF8800; font-size: 12px; border: 1px solid #555; padding: 2px 4px; border-radius: 3px;")
        self.lbl_update_date.setVisible(False)

        # 切換按鈕
        self.btn_toggle_chart = QPushButton("切換視圖")
        self.btn_toggle_chart.setFixedSize(80, 26)
        self.btn_toggle_chart.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_toggle_chart.setStyleSheet("""
            QPushButton { background: #333; color: #CCC; border: 1px solid #555; border-radius: 3px; font-size: 12px; }
            QPushButton:hover { background: #555; color: white; }
        """)
        self.btn_toggle_chart.clicked.connect(self.toggle_chart_visibility)

        row1_layout.addWidget(self.lbl_stock_info)
        row1_layout.addWidget(sep)
        row1_layout.addWidget(title)
        row1_layout.addStretch()
        row1_layout.addWidget(self.lbl_update_date)
        row1_layout.addWidget(self.btn_toggle_chart)

        # 🔥 [Row 2] 滑鼠浮動數據 與 👑 新增的固定累計數據
        self.info_label = QLabel("移動滑鼠查看數據...")
        self.info_label.setStyleSheet("font-family: 'Consolas'; font-size: 13px; color: #888;")
        self.info_label.setFixedWidth(350)

        # 👑 新增：顯示最新一期累計資料的 Label
        self.lbl_cumulative_info = QLabel("")
        self.lbl_cumulative_info.setStyleSheet("font-family: 'Consolas', 'Microsoft JhengHei'; font-size: 14px;")

        row2_layout.addWidget(self.info_label)
        row2_layout.addStretch()
        row2_layout.addWidget(self.lbl_cumulative_info)  # 放在右側

        header_vbox.addLayout(row1_layout)
        header_vbox.addLayout(row2_layout)
        layout.addWidget(header_widget)

        # 2. Canvas
        self.fig = Figure(facecolor='#000000')
        self.canvas = FigureCanvas(self.fig)
        self.canvas.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        layout.addWidget(self.canvas, stretch=6)

        # 3. Table
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["季度", "EPS", "季增率", "年增率"])
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

    def load_eps_data(self, stock_id, stock_name=""):
        # 1. 檢查是否為重複請求 (防抖動第一層)
        if stock_id == self.current_stock_id and self.table.rowCount() > 0:
            return

        self.current_stock_id = stock_id
        self.current_stock_name = stock_name

        display_id = stock_id.split('_')[0]
        self.lbl_stock_info.setText(f"{display_id} {stock_name}" if stock_name else f"{display_id}")
        self.info_label.setText(f"⏳ 更新數據中...")
        self.lbl_cumulative_info.setText("")
        self.lbl_update_date.setVisible(False)

        # 清空 UI (保持原本功能)
        self.fig.clear()
        self.canvas.draw()
        self.table.setRowCount(0)

        # 2. 安全處理舊的 Worker：斷開訊號而非終止執行緒
        if hasattr(self, 'worker') and self.worker is not None:
            try:
                self.worker.data_loaded.disconnect()
            except (TypeError, RuntimeError):
                pass

        # 3. 啟動新 Worker
        self.worker = EPSWorker(display_id)
        self.worker.data_loaded.connect(self.process_data)
        self.worker.start()

    def process_data(self, df):
        if df.empty:
            self.info_label.setText("⚠️ 查無 EPS 資料")
            return

        try:
            df = df.rename(columns={'季別': 'Quarter'})
            df['EPS'] = pd.to_numeric(df['EPS'], errors='coerce').fillna(0)

            # 👑 新增：從季別字串解析出 年份(Year) 與 季度(Q) 用於累計運算
            def extract_yq(x):
                m = re.findall(r'\d+', str(x))
                if len(m) >= 2:
                    return int(m[0]), int(m[-1])  # 支援 114.4Q 或 2024Q3 各種格式
                return 0, 0

            yq_df = pd.DataFrame(df['Quarter'].apply(extract_yq).tolist(), columns=['Year', 'Q'])
            df['Year'] = yq_df['Year'].values
            df['Q'] = yq_df['Q'].values

            # 更新資料日期 (取最新的季度)
            if not df.empty:
                sorted_quarters = sorted(df['Quarter'].astype(str), reverse=True)
                if sorted_quarters:
                    self.lbl_update_date.setText(f"資料季度: {sorted_quarters[0]}")
                    self.lbl_update_date.setVisible(True)

            # 計算增長率
            df_calc = df.iloc[::-1].copy()
            df_calc['QoQ'] = df_calc['EPS'].pct_change(periods=1) * 100
            df_calc['YoY'] = df_calc['EPS'].pct_change(periods=4) * 100

            final_df = df_calc.iloc[::-1].copy().fillna(0)

            self.info_label.setText("✅ 更新完成")

            # 👑 執行累加標籤計算
            self.update_cumulative_label(df)

            self.update_ui(final_df)

        except Exception as e:
            print(f"❌ [EPS] 處理錯誤: {e}")
            self.info_label.setText("❌ 數據解析錯誤")

    # 👑 新增：計算最新一季度的同年度累加與去年同期的比較
    def update_cumulative_label(self, df):
        try:
            valid_df = df[df['Year'] > 0]
            if valid_df.empty: return

            # 抓取最新資料的年/季
            latest_year = valid_df['Year'].max()
            latest_q = valid_df[valid_df['Year'] == latest_year]['Q'].max()

            # 計算今年 1 ~ 最新季度的加總
            curr_mask = (valid_df['Year'] == latest_year) & (valid_df['Q'] <= latest_q)
            curr_sum = valid_df[curr_mask]['EPS'].sum()

            # 計算去年 1 ~ 最新季度的加總
            prev_year = latest_year - 1
            prev_mask = (valid_df['Year'] == prev_year) & (valid_df['Q'] <= latest_q)
            prev_sum = valid_df[prev_mask]['EPS'].sum()

            # EPS 經常有負轉正的情況，因此分母取絕對值
            if prev_sum != 0:
                yoy = ((curr_sum - prev_sum) / abs(prev_sum)) * 100
            else:
                yoy = 0

            color = "#FF3333" if yoy >= 0 else "#00FF00"
            text = f"💡 截至 {latest_year}年 1~{latest_q}季累計EPS: <span style='color:#FFF; font-weight:bold;'>{curr_sum:.2f}元</span> | YoY: <span style='color:{color}; font-weight:bold;'>{yoy:+.2f}%</span>"
            self.lbl_cumulative_info.setText(text)

        except Exception as e:
            print(f"Cumulative EPS error: {e}")

    def update_ui(self, df):
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        ax.set_facecolor('#000000')

        plot_data = df.head(8).iloc[::-1]
        self.plot_df = plot_data.copy()

        x = np.arange(len(plot_data))
        eps_values = plot_data['EPS'].values

        colors = ['#FF3333' if v >= 0 else '#00FF00' for v in eps_values]
        ax.bar(x, eps_values, color=colors, alpha=0.9, width=0.6)

        ax.set_xticks(x)
        ax.set_xticklabels(plot_data['Quarter'], color='white', fontsize=9, rotation=0)
        ax.tick_params(axis='y', colors='white', labelsize=9)
        ax.grid(True, color='#333', linestyle=':', axis='y')

        for spine in ax.spines.values():
            spine.set_edgecolor('#444')

        self.canvas.draw()

        self.table.setRowCount(len(df))
        for i, (idx, row) in enumerate(df.iterrows()):
            items = [
                QTableWidgetItem(str(row['Quarter'])),
                QTableWidgetItem(f"{row['EPS']:.2f}元"),
                QTableWidgetItem(f"{row['QoQ']:+.2f}%" if row['QoQ'] != 0 else "-"),
                QTableWidgetItem(f"{row['YoY']:+.2f}%" if row['YoY'] != 0 else "-")
            ]

            items[1].setForeground(QColor("#FFCC00"))
            items[1].setFont(QFont("Consolas", 12, QFont.Weight.Bold))
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
    win.load_eps_data("2330_TW", "台積電")
    win.resize(600, 500)
    win.show()
    sys.exit(app.exec())