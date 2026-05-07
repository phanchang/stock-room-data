import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from dotenv import load_dotenv

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
                             QTableWidgetItem, QHeaderView, QApplication, QLabel,
                             QPushButton, QSizePolicy, QComboBox, QSplitter)
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtCore import pyqtSignal, Qt, QThread

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

# 若有路徑問題請確認此 import
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

        self.is_table_expanded = False
        self.saved_splitter_sizes = []

        self.default_info_text = (
            f"<span style='color:#999;'>💡 將滑鼠移至圖表上方可查看每日詳細數據...</span> &nbsp;|&nbsp; "
            f"<span style='color:{self.CLR_FOREIGN};'>■ 外資</span> &nbsp;"
            f"<span style='color:{self.CLR_TRUST};'>■ 投信</span> &nbsp;"
            f"<span style='color:{self.CLR_DEALER};'>■ 自營</span>"
        )

        self.stock_changed.connect(self.load_inst_data)
        self.init_ui()

    def init_ui(self):
        self.setStyleSheet("background-color: #000000; color: #FFFFFF;")

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(5, 5, 5, 5)
        main_layout.setSpacing(5)

        # 1. 頂部控制列
        control_bar = QWidget()
        control_bar.setFixedHeight(40)
        control_bar.setStyleSheet("background-color: #0A0A0A; border-radius: 5px;")
        control_layout = QHBoxLayout(control_bar)
        control_layout.setContentsMargins(10, 0, 10, 0)
        control_layout.setSpacing(15)

        self.lbl_stock_info = QLabel("請選擇股票")
        self.lbl_stock_info.setStyleSheet("color: #FFFF00; font-weight: bold; font-size: 18px;")

        sep = QLabel("|")
        sep.setStyleSheet("color: #555; font-size: 16px;")

        title = QLabel("三大法人買賣超")
        title.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 15px;")

        self.combo_cumsum = QComboBox()
        self.combo_cumsum.addItems(["全部累計", "外資累計", "投信累計", "自營商累計"])
        self.combo_cumsum.setCursor(Qt.CursorShape.PointingHandCursor)
        self.combo_cumsum.setStyleSheet("""
            QComboBox { background-color: #222; color: #FFF; border: 1px solid #555; border-radius: 3px; padding: 3px 8px; font-size: 13px; }
            QComboBox::drop-down { border-left: 1px solid #555; }
        """)
        self.combo_cumsum.currentIndexChanged.connect(self.update_plot)

        self.lbl_update_date = QLabel("")
        self.lbl_update_date.setStyleSheet("color: #FF8800; font-size: 12px; font-family: 'Consolas';")
        self.lbl_update_date.setVisible(False)

        self.btn_toggle_chart = QPushButton("隱藏圖表")
        self.btn_toggle_chart.setFixedSize(85, 28)
        self.btn_toggle_chart.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_toggle_chart.setStyleSheet("""
            QPushButton { background: #004444; color: white; border: 1px solid #00E5FF; border-radius: 4px; font-size: 13px; font-weight: bold; }
            QPushButton:hover { background: #006666; }
        """)
        self.btn_toggle_chart.clicked.connect(self.toggle_chart_visibility)

        control_layout.addWidget(self.lbl_stock_info)
        control_layout.addWidget(sep)
        control_layout.addWidget(title)
        control_layout.addWidget(self.combo_cumsum)
        control_layout.addStretch()
        control_layout.addWidget(self.lbl_update_date)
        control_layout.addWidget(self.btn_toggle_chart)
        main_layout.addWidget(control_bar)

        # 2. 獨立的游標資訊列
        self.info_label = QLabel(self.default_info_text)
        self.info_label.setFixedHeight(25)
        self.info_label.setStyleSheet(
            "font-family: 'Consolas', 'Microsoft JhengHei'; font-size: 14px; padding-left: 10px;")
        main_layout.addWidget(self.info_label)

        # 3. 核心可拖拉區域 (Splitter)
        self.splitter = QSplitter(Qt.Orientation.Vertical)
        self.splitter.setStyleSheet("""
            QSplitter::handle { background-color: #333; height: 3px; }
            QSplitter::handle:hover { background-color: #00E5FF; }
        """)

        # --- Top: Canvas ---
        self.canvas_widget = QWidget()
        canvas_layout = QVBoxLayout(self.canvas_widget)
        canvas_layout.setContentsMargins(0, 0, 0, 0)
        self.fig = Figure(facecolor='#050505')
        self.canvas = FigureCanvas(self.fig)
        canvas_layout.addWidget(self.canvas)
        self.splitter.addWidget(self.canvas_widget)

        # --- Bottom: Table Container ---
        self.table_container = QWidget()
        table_layout = QVBoxLayout(self.table_container)
        table_layout.setContentsMargins(0, 0, 0, 0)
        table_layout.setSpacing(0)

        table_toolbar = QWidget()
        table_toolbar.setFixedHeight(26)
        table_toolbar.setStyleSheet(
            "background-color: #111; border-top: 1px solid #333; border-left: 1px solid #333; border-right: 1px solid #333;")
        toolbar_layout = QHBoxLayout(table_toolbar)
        toolbar_layout.setContentsMargins(10, 0, 5, 0)

        lbl_table_title = QLabel("每日買賣超明細")
        lbl_table_title.setStyleSheet("color: #888; font-size: 12px;")

        self.btn_expand_table = QPushButton("▲ 放大")
        self.btn_expand_table.setFixedSize(60, 20)
        self.btn_expand_table.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_expand_table.setStyleSheet("""
            QPushButton { background: transparent; color: #00E5FF; border: 1px solid #00E5FF; border-radius: 3px; font-size: 12px; }
            QPushButton:hover { background: #004444; color: #FFF; }
        """)
        self.btn_expand_table.clicked.connect(self.toggle_table_expansion)

        toolbar_layout.addWidget(lbl_table_title)
        toolbar_layout.addStretch()
        toolbar_layout.addWidget(self.btn_expand_table)
        table_layout.addWidget(table_toolbar)

        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet("""
            QTableWidget { background-color: #000000; alternate-background-color: #111111; gridline-color: #222; color: #FFF; border: 1px solid #333; border-top: none; font-size: 14px; font-family: 'Consolas', 'Microsoft JhengHei'; }
            QHeaderView::section { background-color: #1A1A1A; color: #00FFFF; font-weight: bold; height: 30px; border: 1px solid #222; border-top: none; font-size: 13px; }
            QTableWidget::item { padding: 2px; }
            QScrollBar:vertical { background: #111; width: 12px; }
            QScrollBar::handle:vertical { background: #555; border-radius: 6px; }
        """)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(26)

        table_layout.addWidget(self.table)
        self.splitter.addWidget(self.table_container)

        self.splitter.setSizes([600, 400])
        main_layout.addWidget(self.splitter)

        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)
        self.canvas.mpl_connect('axes_leave_event', self.on_mouse_leave)

    def toggle_table_expansion(self):
        if not self.is_table_expanded:
            self.saved_splitter_sizes = self.splitter.sizes()
            total_size = sum(self.saved_splitter_sizes)
            self.splitter.setSizes([0, total_size])
            self.btn_expand_table.setText("▼ 縮小")
            self.btn_expand_table.setStyleSheet("""
                QPushButton { background: #00E5FF; color: #000; border: 1px solid #00E5FF; border-radius: 3px; font-size: 12px; font-weight: bold;}
                QPushButton:hover { background: #00B3CC; }
            """)
            self.is_table_expanded = True
        else:
            if self.saved_splitter_sizes:
                self.splitter.setSizes(self.saved_splitter_sizes)
            else:
                self.splitter.setSizes([600, 400])
            self.btn_expand_table.setText("▲ 放大")
            self.btn_expand_table.setStyleSheet("""
                QPushButton { background: transparent; color: #00E5FF; border: 1px solid #00E5FF; border-radius: 3px; font-size: 12px; }
                QPushButton:hover { background: #004444; color: #FFF; }
            """)
            self.is_table_expanded = False

    def toggle_chart_visibility(self):
        is_visible = self.canvas_widget.isVisible()

        self.canvas_widget.setVisible(not is_visible)
        self.combo_cumsum.setVisible(not is_visible)
        self.info_label.setVisible(not is_visible)

        if is_visible:
            self.btn_toggle_chart.setText("顯示圖表")
            self.btn_toggle_chart.setStyleSheet("""
                QPushButton { background: #333; color: #CCC; border: 1px solid #555; border-radius: 4px; font-size: 13px; }
                QPushButton:hover { background: #555; color: white; }
            """)
            self.btn_expand_table.parent().setVisible(False)
        else:
            self.btn_toggle_chart.setText("隱藏圖表")
            self.btn_toggle_chart.setStyleSheet("""
                QPushButton { background: #004444; color: white; border: 1px solid #00E5FF; border-radius: 4px; font-size: 13px; font-weight: bold; }
                QPushButton:hover { background: #006666; }
            """)
            self.btn_expand_table.parent().setVisible(True)

        if self.raw_df is not None and not self.raw_df.empty:
            self.update_table()

    def load_inst_data(self, stock_id, stock_name=""):
        if stock_id == self.current_stock_id and self.table.rowCount() > 0:
            return

        self.current_stock_id = stock_id
        self.current_stock_name = stock_name

        display_id = stock_id.split('_')[0]
        self.lbl_stock_info.setText(f"{display_id} {stock_name}" if stock_name else f"{display_id}")

        self.info_label.setText("<span style='color:#999;'>⏳ 正在下載最新資料...</span>")
        self.lbl_update_date.setVisible(False)
        self.table.setRowCount(0)
        self.fig.clear()
        try:
            self.canvas.draw()
        except Exception:
            pass

        if self.worker is not None:
            try:
                self.worker.data_fetched.disconnect()
            except:
                pass

        self.worker = InstitutionalWorker(stock_id)
        self.worker.data_fetched.connect(self.on_data_received)
        self.worker.start()

    def on_data_received(self, df):
        if df.empty:
            self.info_label.setText("<span style='color:#FF4444;'>❌ 查無資料</span>")
            return

        self.info_label.setText(self.default_info_text)

        df['Date'] = pd.to_datetime(df['日期'])

        # ✨ 加上 .fillna(0) 解決 NaN 無法轉為 int 的問題
        df['Foreign'] = df['外資買賣超股數'].fillna(0)
        df['Trust'] = df['投信買賣超股數'].fillna(0)
        df['Dealer'] = df['自營商買賣超股數'].fillna(0)

        if not df.empty:
            last_date = df['Date'].max()
            self.lbl_update_date.setText(f"最後更新: {last_date.strftime('%Y-%m-%d')}")
            self.lbl_update_date.setVisible(True)

        self.raw_df = df

        self.update_plot()
        self.update_table()

    def update_plot(self):
        if self.raw_df is None or self.raw_df.empty:
            return

        self.fig.clear()
        self.ax1 = self.fig.add_subplot(111)
        self.ax1.set_facecolor('#050505')

        self.plot_df = self.raw_df.head(60).iloc[::-1].reset_index(drop=True)

        # ✨ 在這裡預先算好四種累計數值，存入 dataframe 供後續連動讀取
        self.plot_df['Cum_Total'] = (self.plot_df['Foreign'] + self.plot_df['Trust'] + self.plot_df['Dealer']).cumsum()
        self.plot_df['Cum_Foreign'] = self.plot_df['Foreign'].cumsum()
        self.plot_df['Cum_Trust'] = self.plot_df['Trust'].cumsum()
        self.plot_df['Cum_Dealer'] = self.plot_df['Dealer'].cumsum()

        x = np.arange(len(self.plot_df))
        colors = [self.CLR_FOREIGN, self.CLR_TRUST, self.CLR_DEALER]
        inst_cols = ['Foreign', 'Trust', 'Dealer']

        pos_bottom, neg_bottom = np.zeros(len(self.plot_df)), np.zeros(len(self.plot_df))

        for i, col in enumerate(inst_cols):
            vals = self.plot_df[col].fillna(0).values
            p_mask = vals > 0
            p_vals = np.where(p_mask, vals, 0)
            self.ax1.bar(x, p_vals, bottom=pos_bottom, color=colors[i], alpha=0.85, width=0.6)
            pos_bottom += p_vals

            n_mask = vals <= 0
            n_vals = np.where(n_mask, vals, 0)
            self.ax1.bar(x, n_vals, bottom=neg_bottom, color=colors[i], alpha=0.85, width=0.6)
            neg_bottom += n_vals

        self.ax2 = self.ax1.twinx()
        combo_idx = self.combo_cumsum.currentIndex()

        if combo_idx == 0:
            cumsum_data = self.plot_df['Cum_Total']
            line_color = self.CLR_TOTAL
        elif combo_idx == 1:
            cumsum_data = self.plot_df['Cum_Foreign']
            line_color = self.CLR_FOREIGN
        elif combo_idx == 2:
            cumsum_data = self.plot_df['Cum_Trust']
            line_color = self.CLR_TRUST
        else:
            cumsum_data = self.plot_df['Cum_Dealer']
            line_color = self.CLR_DEALER

        self.ax2.plot(x, cumsum_data, color=line_color, linewidth=2, marker='o', markersize=3)

        self.ax1.axhline(0, color='#666', linewidth=1)
        self.ax1.tick_params(colors='#AAA', labelsize=9)
        self.ax2.tick_params(colors='#AAA', labelsize=9)

        date_labels = [d.strftime('%m/%d') for d in self.plot_df['Date']]
        step = max(1, len(x) // 8)
        self.ax1.set_xticks(x[::step])
        self.ax1.set_xticklabels(date_labels[::step])

        for ax in [self.ax1, self.ax2]:
            for spine in ax.spines.values():
                spine.set_edgecolor('#333')

            # ✨ 新增防呆機制：確保畫布大小有效才執行 tight_layout
        width, height = self.canvas.get_width_height()
        if width > 0 and height > 0:
            try:
                self.fig.tight_layout()
            except Exception as e:
                print(f"⚠️ [圖表排版] tight_layout 忽略錯誤: {e}")

        # 無論排版是否成功，都執行繪製
        try:
            self.canvas.draw()
        except Exception:
            pass

    def update_table(self):
        if self.raw_df is None or self.raw_df.empty:
            return

        inst_cols = ['Foreign', 'Trust', 'Dealer']

        if self.canvas_widget.isVisible():
            self.table.setHorizontalHeaderLabels(["日期", "外資", "投信", "自營", "合計"])
            sub_df = self.raw_df.head(60)
            self.table.setRowCount(len(sub_df))

            for i, (_, row) in enumerate(sub_df.iterrows()):
                date_str = row['Date'].strftime('%m/%d')
                item_date = QTableWidgetItem(date_str)
                item_date.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                item_date.setForeground(QColor("#AAA"))
                self.table.setItem(i, 0, item_date)

                vals = []
                for j, col in enumerate(inst_cols):
                    val = row[col]
                    vals.append(val)
                    item = QTableWidgetItem(f"{int(val):+,d}")
                    item.setForeground(QColor("#FF4444" if val >= 0 else "#00FF00"))
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    self.table.setItem(i, j + 1, item)

                g_total = sum(vals)
                item_t = QTableWidgetItem(f"{int(g_total):+,d}")
                item_t.setForeground(QColor("#FF4444" if g_total >= 0 else "#00FF00"))
                item_t.setFont(QFont("Consolas", 11, QFont.Weight.Bold))
                item_t.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(i, 4, item_t)
        else:
            self.table.setHorizontalHeaderLabels(["期間累計", "外資", "投信", "自營", "總合計"])
            periods = [1, 5, 10, 20, 60]
            period_labels = ["近 1 日", "近 5 日", "近 10 日", "近 20 日", "近 60 日"]
            self.table.setRowCount(len(periods))

            for i, p in enumerate(periods):
                sub_df = self.raw_df.head(p)
                item_period = QTableWidgetItem(period_labels[i])
                item_period.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                item_period.setForeground(QColor("#FFF"))
                self.table.setItem(i, 0, item_period)

                vals = []
                for j, col in enumerate(inst_cols):
                    total = sub_df[col].sum()
                    vals.append(total)
                    item = QTableWidgetItem(f"{int(total):+,d}")
                    item.setForeground(QColor("#FF4444" if total >= 0 else "#00FF00"))
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    self.table.setItem(i, j + 1, item)

                g_total = sum(vals)
                item_t = QTableWidgetItem(f"{int(g_total):+,d}")
                item_t.setForeground(QColor("#FF4444" if g_total >= 0 else "#00FF00"))
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
            daily_total = f + t + d

            # 根據下拉選單，抓取對應的累積數值
            combo_idx = self.combo_cumsum.currentIndex()
            if combo_idx == 0:
                cum_val = int(data['Cum_Total'])
                cum_color = self.CLR_TOTAL
            elif combo_idx == 1:
                cum_val = int(data['Cum_Foreign'])
                cum_color = self.CLR_FOREIGN
            elif combo_idx == 2:
                cum_val = int(data['Cum_Trust'])
                cum_color = self.CLR_TRUST
            else:
                cum_val = int(data['Cum_Dealer'])
                cum_color = self.CLR_DEALER

            # 💡 調整重點：
            # 1. 拿掉粗體 (font-weight:bold) 避免數字變寬
            # 2. 「單日合」縮短為「合」
            # 3. 既然顏色已經對應了，後面的「外資/投信/自營累計」統一縮短為「📈累計」
            # 4. 整體字體強制設定為 13px，確保筆電螢幕絕對塞得下
            html_text = (
                f"<span style='font-size:13px;'>"
                f"<span style='color:#FFF; background-color:#333; padding:2px;'>{date_str}</span> &nbsp;|&nbsp; "
                f"<span style='color:{self.CLR_FOREIGN};'>■外資:{f:+,d}</span> &nbsp;"
                f"<span style='color:{self.CLR_TRUST};'>■投信:{t:+,d}</span> &nbsp;"
                f"<span style='color:{self.CLR_DEALER};'>■自營:{d:+,d}</span> &nbsp;"
                f"<span style='color:#888;'>(合:{daily_total:+,d})</span> &nbsp;|&nbsp; "
                f"<span style='color:{cum_color};'>📈累計:{cum_val:+,d}</span>"
                f"</span>"
            )
            self.info_label.setText(html_text)

    def on_mouse_leave(self, event):
        if self.raw_df is not None and not self.raw_df.empty:
            self.info_label.setText(self.default_info_text)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    module = InstitutionalModule()
    module.load_inst_data("2330_TW", "台積電")
    module.resize(900, 650)
    module.show()
    sys.exit(app.exec())