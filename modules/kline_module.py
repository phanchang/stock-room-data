import sys
import pandas as pd
import numpy as np
from pathlib import Path

# UI 元件
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QPushButton, QButtonGroup, QSizePolicy)
from PyQt6.QtCore import pyqtSignal, Qt
from PyQt6.QtGui import QColor, QFont, QCursor

# 圖表元件
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.dates as mdates

# 設定風格
plt.style.use('dark_background')
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


class KLineModule(QWidget):
    stock_selected = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_stock_id = None
        self.raw_df = None
        self.display_df = None

        # 視圖設定
        self.timeframe = 'D'
        self.visible_candles = 80
        self.scroll_pos = 0
        self.is_dragging = False
        self.last_mouse_x = 0

        # 繪圖物件參照
        self.ax1 = None
        self.ax2 = None
        self.current_view_df = None
        self.cross_v1 = None
        self.cross_h1 = None
        self.cross_v2 = None

        self.init_ui()

    def init_ui(self):
        self.setStyleSheet("background-color: #000000; color: #FFFFFF;")

        # 強制開啟滑鼠追蹤
        self.setMouseTracking(True)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # --- 1. 控制列 ---
        control_widget = QWidget()
        control_widget.setFixedHeight(40)
        control_widget.setStyleSheet("background-color: #050505; border-bottom: 1px solid #222;")
        control_layout = QHBoxLayout(control_widget)
        control_layout.setContentsMargins(10, 0, 10, 0)
        control_layout.setSpacing(15)

        title = QLabel("技術分析")
        title.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 14px;")

        self.btn_group = QButtonGroup(self)
        self.btn_day = self._create_tf_btn("日", "D")
        self.btn_week = self._create_tf_btn("周", "W")
        self.btn_month = self._create_tf_btn("月", "M")
        self.btn_day.setChecked(True)

        self.btn_group.addButton(self.btn_day)
        self.btn_group.addButton(self.btn_week)
        self.btn_group.addButton(self.btn_month)

        control_layout.addWidget(title)
        control_layout.addWidget(self.btn_day)
        control_layout.addWidget(self.btn_week)
        control_layout.addWidget(self.btn_month)
        control_layout.addStretch()
        layout.addWidget(control_widget)

        # --- 2. 資訊列 (高度加高以容納兩行) ---
        self.info_bg = QWidget()
        self.info_bg.setFixedHeight(50)
        self.info_bg.setStyleSheet("background-color: #0A0A0A; border-bottom: 1px solid #333;")
        info_layout = QHBoxLayout(self.info_bg)
        info_layout.setContentsMargins(10, 2, 10, 2)  # 上下留點邊距

        self.info_label = QLabel("準備就緒")
        self.info_label.setStyleSheet(
            "font-family: 'Consolas', 'Microsoft JhengHei'; font-size: 13px; color: #CCC; font-weight: bold;")
        self.info_label.setTextFormat(Qt.TextFormat.RichText)
        self.info_label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)

        info_layout.addWidget(self.info_label)
        layout.addWidget(self.info_bg)

        # --- 3. 畫布 ---
        self.fig = Figure(facecolor='#000000')
        self.canvas = FigureCanvas(self.fig)
        self.canvas.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.canvas.updateGeometry()

        # 綁定事件
        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)
        self.canvas.mpl_connect('scroll_event', self.on_scroll_zoom)
        self.canvas.mpl_connect('button_press_event', self.on_mouse_press)
        self.canvas.mpl_connect('button_release_event', self.on_mouse_release)

        layout.addWidget(self.canvas, stretch=1)

    def _create_tf_btn(self, text, tf):
        btn = QPushButton(text)
        btn.setFixedSize(40, 24)
        btn.setCheckable(True)
        btn.setCursor(Qt.CursorShape.PointingHandCursor)
        btn.setStyleSheet("""
            QPushButton { background-color: #222; color: #888; border: 1px solid #444; border-radius: 3px; font-weight: bold; } 
            QPushButton:checked { background-color: #00E5FF; color: #000; border: 1px solid #00E5FF; }
        """)
        btn.clicked.connect(lambda: self.change_timeframe(tf))
        return btn

    def change_timeframe(self, tf):
        self.timeframe = tf
        if self.raw_df is not None:
            self.process_data()
            self.redraw_chart()

    def load_stock_data(self, stock_id):
        self.current_stock_id = stock_id
        display_id = stock_id.split('_')[0]
        self.info_label.setText(
            f"<span style='color:#00E5FF; font-size:16px;'>{display_id}</span> <span style='color:#888;'>載入中...</span>")

        # 先清空，避免殘留
        self.fig.clear()
        self.canvas.draw()

        path = Path(f"data/cache/tw/{stock_id}.parquet")

        if path.exists():
            try:
                df = pd.read_parquet(path)
                # 欄位標準化
                df.columns = [c.capitalize() for c in df.columns]
                if 'Adj close' in df.columns: df.rename(columns={'Adj close': 'Adj Close'}, inplace=True)
                df.index = pd.to_datetime(df.index)

                self.raw_df = df

                # 計算與繪圖
                self.process_data()
                self.redraw_chart()

                # 初始顯示最後一筆數據
                if self.display_df is not None and not self.display_df.empty:
                    last_row = self.display_df.iloc[-1]
                    self.update_info_label(last_row, last_row.name)

            except Exception as e:
                self.info_label.setText(f"<span style='color:#FF0000;'>❌ 資料讀取失敗: {e}</span>")
        else:
            self.info_label.setText(
                f"<span style='color:#FF0000;'>❌ 找不到資料: {display_id}</span> <span style='color:#888;'>(請確認已執行爬蟲)</span>")
            self.fig.clear()
            self.canvas.draw()

    def process_data(self):
        if self.raw_df is None: return
        df = self.raw_df.copy()

        # 週期重取樣
        if self.timeframe == 'W':
            df = df.resample('W-FRI').agg(
                {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
        elif self.timeframe == 'M':
            df = df.resample('ME').agg(
                {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()

        # 計算 MA
        df['MA5'] = df['Close'].rolling(5).mean()
        df['MA20'] = df['Close'].rolling(20).mean()
        df['MA60'] = df['Close'].rolling(60).mean()

        self.display_df = df
        self.scroll_pos = 0

    def redraw_chart(self):
        if self.display_df is None: return
        self.fig.clear()

        # 計算可視範圍
        total_len = len(self.display_df)
        end_idx = total_len - self.scroll_pos
        start_idx = max(0, end_idx - self.visible_candles)
        if end_idx > total_len: end_idx = total_len
        if start_idx < 0: start_idx = 0

        view_df = self.display_df.iloc[start_idx:end_idx].copy()
        self.current_view_df = view_df

        # Y 軸自動縮放
        if not view_df.empty:
            v_high = view_df['High'].max()
            v_low = view_df['Low'].min()
            margin = (v_high - v_low) * 0.05
            if margin == 0: margin = v_high * 0.01
            ylim_min, ylim_max = v_low - margin, v_high + margin
        else:
            ylim_min, ylim_max = 0, 100

        # 建立子圖
        gs = self.fig.add_gridspec(2, 1, height_ratios=[3, 1], hspace=0.02)
        ax1 = self.fig.add_subplot(gs[0])
        ax2 = self.fig.add_subplot(gs[1], sharex=ax1)
        self.ax1, self.ax2 = ax1, ax2

        # 準備數據
        x = np.arange(len(view_df))
        opens = view_df['Open'].values
        closes = view_df['Close'].values
        highs = view_df['High'].values
        lows = view_df['Low'].values

        up = closes >= opens
        down = closes < opens
        col_up, col_down = '#FF3333', '#00FF00'

        # 繪製 K 線
        ax1.vlines(x[up], opens[up], closes[up], color=col_up, linewidth=3)
        ax1.vlines(x[up], lows[up], highs[up], color=col_up, linewidth=0.8)
        ax1.vlines(x[down], opens[down], closes[down], color=col_down, linewidth=3)
        ax1.vlines(x[down], lows[down], highs[down], color=col_down, linewidth=0.8)

        # 繪製 MA
        ax1.plot(x, view_df['MA5'].values, color='#FFFFFF', lw=1, alpha=0.8)
        ax1.plot(x, view_df['MA20'].values, color='#FFFF00', lw=1, alpha=0.8)
        ax1.plot(x, view_df['MA60'].values, color='#FF00FF', lw=1, alpha=0.8)
        ax1.set_ylim(ylim_min, ylim_max)

        # 繪製成交量
        ax2.bar(x[up], view_df['Volume'][up], color=col_up, alpha=0.9)
        ax2.bar(x[down], view_df['Volume'][down], color=col_down, alpha=0.9)

        # 去除科學記號
        from matplotlib.ticker import ScalarFormatter
        y_fmt = ScalarFormatter(useOffset=False)
        y_fmt.set_scientific(False)
        ax2.yaxis.set_major_formatter(y_fmt)

        # X 軸智慧標籤
        date_strs = []
        tick_indices = []
        dates = view_df.index
        last_year = None
        step = max(1, len(view_df) // 8)

        for i in range(0, len(dates), step):
            d = dates[i]
            tick_indices.append(i)
            if self.timeframe == 'D':
                if last_year is None or d.year != last_year:
                    date_strs.append(d.strftime('%Y/%m/%d'))
                    last_year = d.year
                else:
                    date_strs.append(d.strftime('%m/%d'))
            else:
                if last_year is None or d.year != last_year:
                    date_strs.append(f"{d.year}年\n{d.month}月")
                    last_year = d.year
                else:
                    date_strs.append(f"{d.month}月")

        ax1.set_xticks(tick_indices)
        ax2.set_xticks(tick_indices)
        ax2.set_xticklabels(date_strs, rotation=0, fontsize=9, color='#AAA')

        # 樣式與邊框
        for ax in [ax1, ax2]:
            ax.grid(True, color='#222', linestyle=':')
            ax.set_facecolor('#000000')
            for spine in ax.spines.values():
                spine.set_edgecolor('#444')
            ax.tick_params(colors='#888')
            ax.yaxis.tick_right()

        plt.setp(ax1.get_xticklabels(), visible=False)
        self.fig.subplots_adjust(left=0.01, right=0.92, top=0.98, bottom=0.15)

        # 建立十字線物件 (預設隱藏)
        self.cross_v1 = ax1.axvline(0, color='#888', linestyle='--', linewidth=0.8, visible=False)
        self.cross_h1 = ax1.axhline(0, color='#888', linestyle='--', linewidth=0.8, visible=False)
        self.cross_v2 = ax2.axvline(0, color='#888', linestyle='--', linewidth=0.8, visible=False)

        self.canvas.draw()

    # --- 互動事件 ---
    def on_scroll_zoom(self, event):
        if event.button == 'up':
            self.visible_candles = max(20, self.visible_candles - 10)
        elif event.button == 'down':
            self.visible_candles = min(len(self.display_df), self.visible_candles + 10)
        self.redraw_chart()

    def on_mouse_press(self, event):
        if event.button == 1:
            self.is_dragging = True
            self.last_mouse_x = event.xdata

    def on_mouse_release(self, event):
        if event.button == 1:
            self.is_dragging = False

    def on_mouse_move(self, event):
        # 確保資料存在
        if self.current_view_df is None or self.current_view_df.empty: return
        if not event.inaxes: return

        # 拖曳平移
        if self.is_dragging and event.xdata is not None and self.last_mouse_x is not None:
            dx = int(self.last_mouse_x - event.xdata)
            if abs(dx) > 0:
                self.scroll_pos = max(0, min(len(self.display_df) - self.visible_candles, self.scroll_pos - dx))
                self.redraw_chart()
                return

        # 十字線與數據顯示
        try:
            x_idx = int(round(event.xdata))
            # 邊界檢查
            if 0 <= x_idx < len(self.current_view_df):
                # 更新十字線位置
                if self.cross_v1:
                    self.cross_v1.set_xdata([x_idx])
                    self.cross_v1.set_visible(True)
                if self.cross_h1:
                    self.cross_h1.set_ydata([event.ydata])
                    self.cross_h1.set_visible(True)
                if self.cross_v2:
                    self.cross_v2.set_xdata([x_idx])
                    self.cross_v2.set_visible(True)

                self.canvas.draw_idle()

                # 更新文字
                row = self.current_view_df.iloc[x_idx]
                self.update_info_label(row, row.name)

        except Exception:
            pass  # 避免報錯影響體驗

    def update_info_label(self, row, date_val):
        """ 格式化數據顯示：兩行式 """
        display_id = self.current_stock_id.split('_')[0] if self.current_stock_id else ""
        date_str = date_val.strftime('%Y/%m/%d')

        # 顏色邏輯
        c_open = row['Open']
        c_close = row['Close']
        color = "#FF3333" if c_close >= c_open else "#00FF00"

        # 第一行：代號 日期 O H L C
        line1 = (
            f"<span style='color:#00E5FF; font-size:16px; font-weight:bold;'>{display_id}</span>  "
            f"<span style='color:#DDD;'>{date_str}</span>&nbsp;&nbsp;"
            f"O:<span style='color:#FFF;'>{row['Open']:.1f}</span>&nbsp;"
            f"H:<span style='color:#FFF;'>{row['High']:.1f}</span>&nbsp;"
            f"L:<span style='color:#FFF;'>{row['Low']:.1f}</span>&nbsp;"
            f"C:<span style='color:{color}; font-weight:bold;'>{row['Close']:.1f}</span>"
        )

        # 第二行：Vol MA5 MA20 MA60
        line2 = (
            f"Vol:<span style='color:#FFFF00;'>{int(row['Volume']):,}</span>&nbsp;&nbsp;"
            f"<span style='color:#FFF;'>MA5:{row['MA5']:.1f}</span>&nbsp;"
            f"<span style='color:#FFFF00;'>MA20:{row['MA20']:.1f}</span>&nbsp;"
            f"<span style='color:#FF00FF;'>MA60:{row['MA60']:.1f}</span>"
        )

        self.info_label.setText(f"{line1}<br>{line2}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = KLineModule()
    window.resize(1000, 600)
    window.show()
    sys.exit(app.exec())