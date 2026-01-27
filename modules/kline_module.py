import sys
import pandas as pd
import numpy as np
from pathlib import Path
from modules.expanded_kline import ExpandedKLineWindow

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QPushButton, QButtonGroup, QSizePolicy)
from PyQt6.QtCore import pyqtSignal, Qt
from PyQt6.QtGui import QColor, QFont, QCursor

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.dates as mdates

plt.style.use('dark_background')
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


class KLineModule(QWidget):
    stock_selected = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_stock_id = None
        self.current_stock_name = ""
        self.current_df = None
        self.raw_df = None
        self.display_df = None

        self.timeframe = 'D'
        self.visible_candles = 80
        self.scroll_pos = 0
        self.is_dragging = False
        self.last_mouse_x = None

        self.ax1 = None
        self.ax2 = None
        self.current_view_df = None

        self.cross_v1 = None
        self.cross_h1 = None
        self.cross_v2 = None
        self.y_label_text = None
        self.y_label_box = None

        self.init_ui()

    def init_ui(self):
        self.setStyleSheet("background-color: #000000; color: #FFFFFF;")
        self.setMouseTracking(True)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # 1. 控制列
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

        control_layout.addWidget(title)
        control_layout.addWidget(self.btn_day)
        control_layout.addWidget(self.btn_week)
        control_layout.addWidget(self.btn_month)

        control_layout.addStretch()

        btn_expand = QPushButton("⛶")
        btn_expand.setFixedSize(30, 30)
        btn_expand.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_expand.setToolTip("開啟進階戰情室")
        btn_expand.setStyleSheet("""
            QPushButton {
                background-color: transparent; color: #888;
                border: 1px solid #444; border-radius: 4px;
                font-size: 18px; padding-bottom: 3px;
            }
            QPushButton:hover {
                background-color: #333; color: #00E5FF; border-color: #00E5FF;
            }
        """)
        btn_expand.clicked.connect(self.open_war_room)
        control_layout.addWidget(btn_expand)

        layout.addWidget(control_widget)

        # 2. 資訊列
        self.info_bg = QWidget()
        self.info_bg.setFixedHeight(50)
        self.info_bg.setStyleSheet("background-color: #0A0A0A; border-bottom: 1px solid #333;")
        info_layout = QHBoxLayout(self.info_bg)
        info_layout.setContentsMargins(10, 2, 10, 2)

        self.info_label = QLabel("準備就緒")
        self.info_label.setStyleSheet(
            "font-family: 'Consolas', 'Microsoft JhengHei'; font-size: 13px; color: #CCC; font-weight: bold;")
        self.info_label.setTextFormat(Qt.TextFormat.RichText)
        self.info_label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)

        info_layout.addWidget(self.info_label)
        layout.addWidget(self.info_bg)

        # 3. 畫布
        self.fig = Figure(facecolor='#000000')
        self.canvas = FigureCanvas(self.fig)
        self.canvas.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

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
        self.btn_group.addButton(btn)
        return btn

    def open_war_room(self):
        if self.current_df is None or self.current_df.empty:
            return

        dialog = ExpandedKLineWindow(
            stock_id=self.current_stock_id,
            df=self.current_df,
            stock_name=self.current_stock_name,
            parent=self
        )
        dialog.exec()

    def change_timeframe(self, tf):
        self.timeframe = tf
        if self.raw_df is not None:
            self.process_data()
            self.redraw_chart()
            if self.display_df is not None and not self.display_df.empty:
                last_row = self.display_df.iloc[-1]
                self.update_info_label(last_row, last_row.name)

    def load_stock_data(self, stock_id: str, stock_name: str = ""):
        self.current_stock_id = stock_id
        self.current_stock_name = stock_name

        display_id = stock_id.split('_')[0]
        display_text = f"{display_id} {stock_name}" if stock_name else display_id

        self.info_label.setText(
            f"<span style='color:#00E5FF; font-size:16px;'>{display_text}</span> "
            f"<span style='color:#888;'>載入中...</span>"
        )
        self.fig.clear()
        self.canvas.draw()

        path = Path(f"data/cache/tw/{stock_id}.parquet")

        if path.exists():
            try:
                df = pd.read_parquet(path)
                df.columns = [c.capitalize() for c in df.columns]
                if 'Adj close' in df.columns:
                    df.rename(columns={'Adj close': 'Adj Close'}, inplace=True)
                df.index = pd.to_datetime(df.index)

                self.raw_df = df
                self.process_data()
                self.redraw_chart()

                if self.display_df is not None and not self.display_df.empty:
                    last_row = self.display_df.iloc[-1]
                    self.update_info_label(last_row, last_row.name)

            except Exception as e:
                import traceback
                traceback.print_exc()
                self.info_label.setText(f"<span style='color:#FF0000;'>❌ 資料讀取失敗: {e}</span>")
        else:
            self.info_label.setText(
                f"<span style='color:#FF0000;'>❌ 找不到資料: {display_text}</span> "
                f"<span style='color:#888;'>(請確認已執行爬蟲)</span>"
            )
            self.fig.clear()
            self.canvas.draw()

    def process_data(self):
        if self.raw_df is None: return

        df_source = self.raw_df.copy()

        if self.timeframe == 'D':
            df = df_source
        else:
            rule = 'W-FRI' if self.timeframe == 'W' else 'ME'
            logic = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
            agg_dict = {k: v for k, v in logic.items() if k in df_source.columns}
            df = df_source.resample(rule).agg(agg_dict).dropna()

            if not df.empty:
                real_last_date = df_source.index[-1]
                if df.index[-1] > real_last_date:
                    idx_list = df.index.tolist()
                    idx_list[-1] = real_last_date
                    df.index = pd.DatetimeIndex(idx_list)

        df['MA5'] = df['Close'].rolling(5).mean()
        df['MA20'] = df['Close'].rolling(20).mean()
        df['MA60'] = df['Close'].rolling(60).mean()

        df['PrevClose'] = df['Close'].shift(1)
        df['PrevClose'].iloc[0] = df['Open'].iloc[0]

        df['Change'] = df['Close'] - df['PrevClose']
        df['PctChange'] = (df['Change'] / df['PrevClose']) * 100

        self.display_df = df
        self.current_df = df
        self.scroll_pos = 0

    def redraw_chart(self):
        if self.display_df is None: return
        self.fig.clear()

        total_len = len(self.display_df)
        max_scroll = max(0, total_len - self.visible_candles)
        self.scroll_pos = max(0, min(self.scroll_pos, max_scroll))

        end_idx = total_len - self.scroll_pos
        start_idx = max(0, end_idx - self.visible_candles)

        view_df = self.display_df.iloc[start_idx:end_idx].copy()
        self.current_view_df = view_df

        if not view_df.empty:
            v_high = view_df['High'].max()
            v_low = view_df['Low'].min()
            padding = (v_high - v_low) * 0.05
            if padding == 0: padding = v_high * 0.01
            ylim_min, ylim_max = v_low - padding, v_high + padding
        else:
            ylim_min, ylim_max = 0, 100

        gs = self.fig.add_gridspec(2, 1, height_ratios=[3, 1], hspace=0.02)
        ax1 = self.fig.add_subplot(gs[0])
        ax2 = self.fig.add_subplot(gs[1], sharex=ax1)
        self.ax1, self.ax2 = ax1, ax2

        x = np.arange(len(view_df))
        opens = view_df['Open'].values
        closes = view_df['Close'].values
        highs = view_df['High'].values
        lows = view_df['Low'].values

        # 🔥 [修正] K棒邏輯：比較 Close vs Open
        up = closes > opens
        down = closes < opens
        flat = closes == opens

        col_up, col_down, col_flat = '#FF3333', '#00FF00', '#FFFFFF'
        width = 0.6

        # 1. 畫紅棒
        # vlines(x, ymin, ymax): 這裡用來畫影線
        ax1.vlines(x[up], lows[up], highs[up], color=col_up, lw=0.8)
        # bar(x, height, bottom): 畫實體
        ax1.bar(x[up], closes[up] - opens[up], width, bottom=opens[up], color=col_up)

        # 2. 畫綠棒
        ax1.vlines(x[down], lows[down], highs[down], color=col_down, lw=0.8)
        # height = open - close (正值), bottom = close
        ax1.bar(x[down], opens[down] - closes[down], width, bottom=closes[down], color=col_down)

        # 3. 畫平盤 (十字線)
        if np.any(flat):
            # 影線
            ax1.vlines(x[flat], lows[flat], highs[flat], color=col_flat, lw=0.8)
            # 🔥 [修正] 實體改畫水平線
            # hlines(y, xmin, xmax)
            ax1.hlines(closes[flat], x[flat] - width / 2, x[flat] + width / 2, colors=col_flat, lw=2)

        # 畫 MA
        ax1.plot(x, view_df['MA5'].values, color='#FFFF00', lw=1, alpha=0.9, label='MA5')
        ax1.plot(x, view_df['MA20'].values, color='#FF8800', lw=1, alpha=0.9, label='MA20')
        ax1.plot(x, view_df['MA60'].values, color='#00FFFF', lw=1, alpha=0.9, label='MA60')
        ax1.set_ylim(ylim_min, ylim_max)

        # 畫成交量 (跟隨 K 棒顏色)
        ax2.bar(x[up], view_df['Volume'][up], color=col_up, alpha=0.9)
        ax2.bar(x[down], view_df['Volume'][down], color=col_down, alpha=0.9)
        ax2.bar(x[flat], view_df['Volume'][flat], color=col_flat, alpha=0.9)

        # X 軸標籤設定 (維持原樣)
        date_strs = []
        tick_indices = []
        dates = view_df.index
        last_val = None
        step = max(1, len(view_df) // 8)

        for i in range(0, len(dates), step):
            d = dates[i]
            tick_indices.append(i)
            if self.timeframe == 'D':
                val = d.year
                if last_val != val:
                    date_strs.append(d.strftime('%Y/%m/%d'))
                    last_val = val
                else:
                    date_strs.append(d.strftime('%m/%d'))
            else:
                date_strs.append(d.strftime('%Y-%m'))

        ax1.set_xticks(tick_indices)
        ax2.set_xticks(tick_indices)
        ax2.set_xticklabels(date_strs, rotation=0, fontsize=9, color='#AAA')
        ax1.tick_params(labelbottom=False)

        # 樣式設定 (維持原樣)
        for ax in [ax1, ax2]:
            ax.grid(True, color='#222', linestyle=':')
            ax.set_facecolor('#000000')
            for spine in ax.spines.values():
                spine.set_edgecolor('#444')
            ax.tick_params(colors='#888')
            ax.yaxis.tick_right()

        self.fig.subplots_adjust(left=0.01, right=0.92, top=0.98, bottom=0.15)

        # 重繪十字線 (如果存在)
        # 🔥 [修正] 這裡原本被包在 if self.cross_v1: 裡面，導致第一次無法建立
        # 請拿掉 if，讓它每次都執行：

        self.cross_v1 = ax1.axvline(0, color='white', ls='--', lw=0.8, visible=False)
        self.cross_h1 = ax1.axhline(0, color='white', ls='--', lw=0.8, visible=False)
        self.cross_v2 = ax2.axvline(0, color='white', ls='--', lw=0.8, visible=False)

        # Y軸查價標籤 (初始化)
        props = dict(boxstyle='square', facecolor='#FF00FF', alpha=0.9, edgecolor='none')
        self.y_label_text = ax1.text(1.02, 0, "", transform=ax1.get_yaxis_transform(),
                                     color='white', fontsize=10, fontweight='bold',
                                     va='center', ha='left', bbox=props, visible=False)

        self.canvas.draw()

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
        if self.current_view_df is None or self.current_view_df.empty: return
        if not event.inaxes:
            if self.cross_v1: self.cross_v1.set_visible(False)
            if self.cross_h1: self.cross_h1.set_visible(False)
            if self.cross_v2: self.cross_v2.set_visible(False)
            if self.y_label_text: self.y_label_text.set_visible(False)
            self.canvas.draw_idle()
            return

        if self.is_dragging and event.xdata is not None and self.last_mouse_x is not None:
            dx = int(self.last_mouse_x - event.xdata)
            if abs(dx) > 0:
                max_scroll = len(self.display_df) - self.visible_candles
                self.scroll_pos = max(0, min(max_scroll, self.scroll_pos + dx))
                self.redraw_chart()
                return

        try:
            x_idx = int(round(event.xdata))

            if 0 <= x_idx < len(self.current_view_df):
                self.cross_v1.set_xdata([x_idx])
                self.cross_v1.set_visible(True)
                self.cross_v2.set_xdata([x_idx])
                self.cross_v2.set_visible(True)

                if event.inaxes == self.ax1:
                    price = event.ydata
                    self.cross_h1.set_ydata([price])
                    self.cross_h1.set_visible(True)
                    self.y_label_text.set_text(f"{price:.2f}")
                    self.y_label_text.set_y(price)
                    self.y_label_text.set_visible(True)
                else:
                    self.cross_h1.set_visible(False)
                    self.y_label_text.set_visible(False)

                self.canvas.draw_idle()

                row = self.current_view_df.iloc[x_idx]
                self.update_info_label(row, row.name)

        except Exception:
            pass

    def update_info_label(self, row, date_val):
        display_id = self.current_stock_id.split('_')[0] if self.current_stock_id else ""
        name = self.current_stock_name
        date_str = date_val.strftime('%Y/%m/%d')

        p = row['PrevClose']

        def get_fmt(val, base):
            if val > base: return "#FF3333"
            if val < base: return "#00FF00"
            return "#FFFFFF"

        c_open = get_fmt(row['Open'], p)
        c_high = get_fmt(row['High'], p)
        c_low = get_fmt(row['Low'], p)
        c_close = get_fmt(row['Close'], p)

        change = row['Change']
        pct = row['PctChange']
        sign = "+" if change > 0 else ""
        c_change = c_close

        line1 = (
            f"<span style='color:#FFFF00; font-size:16px; font-weight:bold;'>{display_id} {name}</span>  "
            f"<span style='color:#DDD;'>{date_str}</span>&nbsp;&nbsp;"
            f"O:<span style='color:{c_open};'>{row['Open']:.2f}</span>&nbsp;"
            f"H:<span style='color:{c_high};'>{row['High']:.2f}</span>&nbsp;"
            f"L:<span style='color:{c_low};'>{row['Low']:.2f}</span>&nbsp;"
            f"C:<span style='color:{c_close}; font-weight:bold;'>{row['Close']:.2f}</span>&nbsp;"
            f"<span style='color:{c_change};'>({sign}{change:.2f} / {sign}{pct:.2f}%)</span>"
        )

        line2 = (
            f"Vol:<span style='color:#FFFF00;'>{int(row['Volume']):,}</span>&nbsp;&nbsp;"
            f"<span style='color:#FFFF00;'>MA5:{row['MA5']:.2f}</span>&nbsp;"
            f"<span style='color:#FF8800;'>MA20:{row['MA20']:.2f}</span>&nbsp;"
            f"<span style='color:#00FFFF;'>MA60:{row['MA60']:.2f}</span>"
        )

        self.info_label.setText(f"{line1}<br>{line2}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = KLineModule()
    window.resize(1000, 600)
    window.show()
    sys.exit(app.exec())