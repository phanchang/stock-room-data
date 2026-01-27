import numpy as np
import pandas as pd
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                             QComboBox, QPushButton, QButtonGroup, QWidget)

# Matplotlib
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.gridspec as gridspec
import matplotlib.ticker as ticker

from utils.indicators import Indicators


class ExpandedKLineWindow(QDialog):
    """
    進階戰情室 (Ultimate V4)
    修正：資訊列改用 QLabel 實作多色顯示、六大數值獨立變色、代號去後綴
    """

    def __init__(self, stock_id, df, stock_name="", parent=None):
        super().__init__(parent)

        # 1. 代號去後綴 (5536_TWO -> 5536)
        self.display_id = stock_id.split('_')[0]
        self.stock_name = stock_name

        self.setWindowTitle(f"StockWarRoom - {self.display_id} {stock_name}")
        self.resize(1200, 800)
        self.setStyleSheet("background-color: #121212; color: white;")

        # 資料處理
        self.df_source = df.copy()
        self.df_source.index = pd.to_datetime(self.df_source.index)
        self.df_source = self.df_source.sort_index()

        self.current_df = self.df_source

        # 預設設定
        self.current_indicator = "Vix Fix"
        self.current_tf = "D"

        # 狀態變數
        self.last_mouse_idx = -1
        self.is_dragging = False
        self.last_drag_x = None

        # 繪圖物件
        self.y_label_box = None
        self.y_label_text = None

        self.init_ui()
        self.update_data_frequency("D")

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # --- 1. 頂部工具列 ---
        toolbar = QHBoxLayout()
        toolbar.setContentsMargins(10, 8, 10, 8)
        toolbar.setSpacing(15)

        # 股票資訊 (顯示 ID + Name) -> 亮黃色
        info_title = QLabel(f"{self.display_id} {self.stock_name}")
        info_title.setStyleSheet("font-size: 20px; font-weight: bold; color: #FFFF00; margin-right: 10px;")
        toolbar.addWidget(info_title)

        # 週期切換
        self.btn_group = QButtonGroup(self)
        self.btn_day = self._create_tf_btn("日", "D")
        self.btn_week = self._create_tf_btn("周", "W")
        self.btn_month = self._create_tf_btn("月", "M")
        self.btn_day.setChecked(True)

        toolbar.addWidget(self.btn_day)
        toolbar.addWidget(self.btn_week)
        toolbar.addWidget(self.btn_month)

        line = QLabel("|")
        line.setStyleSheet("color: #555; font-size: 16px;")
        toolbar.addWidget(line)

        # 指標選擇
        lbl_ind = QLabel("副圖:")
        lbl_ind.setStyleSheet("font-size: 14px; color: #00E5FF;")
        toolbar.addWidget(lbl_ind)

        self.combo = QComboBox()
        self.combo.addItems(["Volume", "Vix Fix", "KD", "MACD", "RSI"])
        self.combo.setCurrentText(self.current_indicator)
        self.combo.setStyleSheet("""
            QComboBox { background: #333; color: white; padding: 4px; font-size: 14px; border: 1px solid #555; }
            QComboBox QAbstractItemView { background: #333; selection-background-color: #00E5FF; }
        """)
        self.combo.currentTextChanged.connect(self.on_indicator_changed)
        toolbar.addWidget(self.combo)

        toolbar.addStretch()

        # 關閉
        btn_close = QPushButton("關閉")
        btn_close.clicked.connect(self.close)
        btn_close.setStyleSheet("background: #444; color: white; padding: 5px 15px;")
        toolbar.addWidget(btn_close)

        layout.addLayout(toolbar)

        # --- 2. 資訊列 (新增：類似 KLineModule 的 HTML Label) ---
        self.info_bg = QWidget()
        self.info_bg.setFixedHeight(40)  # 高度適中
        self.info_bg.setStyleSheet(
            "background-color: #0A0A0A; border-bottom: 1px solid #333; border-top: 1px solid #333;")
        info_layout = QHBoxLayout(self.info_bg)
        info_layout.setContentsMargins(10, 0, 10, 0)

        self.info_label = QLabel("準備就緒")
        self.info_label.setStyleSheet(
            "font-family: 'Consolas', 'Microsoft JhengHei'; font-size: 14px; color: #CCC; font-weight: bold;")
        self.info_label.setTextFormat(Qt.TextFormat.RichText)
        self.info_label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)

        info_layout.addWidget(self.info_label)
        layout.addWidget(self.info_bg)

        # --- 3. Matplotlib 畫布 ---
        self.figure = Figure(facecolor='#121212')
        self.canvas = FigureCanvas(self.figure)
        layout.addWidget(self.canvas)

        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)
        self.canvas.mpl_connect('scroll_event', self.on_scroll)
        self.canvas.mpl_connect('button_press_event', self.on_mouse_press)
        self.canvas.mpl_connect('button_release_event', self.on_mouse_release)

    def _create_tf_btn(self, text, code):
        btn = QPushButton(text)
        btn.setCheckable(True)
        btn.setFixedSize(40, 30)
        btn.setStyleSheet("""
            QPushButton { background: #222; color: #AAA; border: 1px solid #444; }
            QPushButton:checked { background: #00E5FF; color: black; font-weight: bold; border: 1px solid #00E5FF; }
        """)
        btn.clicked.connect(lambda: self.update_data_frequency(code))
        self.btn_group.addButton(btn)
        return btn

    def update_data_frequency(self, tf_code):
        self.current_tf = tf_code

        if tf_code == 'D':
            self.current_df = self.df_source.copy()
        else:
            rule = 'W-FRI' if tf_code == 'W' else 'ME'
            logic = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
            agg_dict = {k: v for k, v in logic.items() if k in self.df_source.columns}

            self.current_df = self.df_source.resample(rule).agg(agg_dict).dropna()

            if not self.current_df.empty:
                real_last_date = self.df_source.index[-1]
                if self.current_df.index[-1] > real_last_date:
                    idx_list = self.current_df.index.tolist()
                    idx_list[-1] = real_last_date
                    self.current_df.index = pd.DatetimeIndex(idx_list)

        # 計算昨收與漲跌 (用於變色)
        self.current_df['PrevClose'] = self.current_df['Close'].shift(1)

        # 🔥 [修正] 解決 FutureWarning ChainedAssignmentError
        if not self.current_df.empty:
            # 用 iloc[0, column_index] 的方式賦值，確保改到原始資料
            col_idx = self.current_df.columns.get_loc('PrevClose')
            self.current_df.iloc[0, col_idx] = self.current_df.iloc[0]['Open']

        self.current_df['Change'] = self.current_df['Close'] - self.current_df['PrevClose']
        self.current_df['PctChange'] = (self.current_df['Change'] / self.current_df['PrevClose']) * 100

        self.x_vals = np.arange(len(self.current_df))
        self.last_mouse_idx = -1

        self.plot_chart_structure()

        total_len = len(self.current_df)
        start = max(0, total_len - 120)
        self.update_view_range(start, total_len)

        # 初始顯示最後一筆
        if not self.current_df.empty:
            self._update_info_label(len(self.current_df) - 1)

    def plot_chart_structure(self):
        self.figure.clear()

        # 設定圖表佈局 (GridSpec)
        gs = gridspec.GridSpec(2, 1, height_ratios=[3, 1])
        gs.update(left=0.05, right=0.92, top=0.98, bottom=0.05, hspace=0.03)

        # --- 主圖 (K線) ---
        self.ax1 = self.figure.add_subplot(gs[0])
        self.ax1.set_facecolor('#121212')
        self.ax1.grid(True, color='#333', linestyle='--', alpha=0.5)
        self.ax1.tick_params(axis='x', labelbottom=False, colors='white')
        self.ax1.tick_params(axis='y', colors='white')
        self.ax1.yaxis.tick_right()  # Y軸刻度放右邊

        # --- 副圖 (指標) ---
        self.ax2 = self.figure.add_subplot(gs[1], sharex=self.ax1)
        self.ax2.set_facecolor('#121212')
        self.ax2.grid(True, color='#333', linestyle='--', alpha=0.5)
        self.ax2.tick_params(axis='x', colors='white')
        self.ax2.tick_params(axis='y', colors='white')
        self.ax2.yaxis.tick_right()  # Y軸刻度放右邊

        # 設定 X 軸日期格式
        def format_date(x, pos):
            idx = int(round(x))
            if 0 <= idx < len(self.current_df):
                return self.current_df.index[idx].strftime('%Y-%m-%d')
            return ""

        self.ax2.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
        self.ax2.xaxis.set_major_locator(ticker.MaxNLocator(nbins=8))

        # 繪製 K 線與指標 (呼叫另一個函式)
        self.draw_candles_and_indicators()

        # 🔥 [關鍵新增] 初始化互動元件 (十字線)
        # 預設先設為 visible=False，等滑鼠移動時再顯示
        self.vline_main = self.ax1.axvline(x=0, color='white', ls='--', lw=0.8, visible=False)
        self.vline_sub = self.ax2.axvline(x=0, color='white', ls='--', lw=0.8, visible=False)
        self.hline_main = self.ax1.axhline(y=0, color='white', ls='--', lw=0.8, visible=False)

        # 🔥 [關鍵新增] 初始化 Y 軸查價標籤 (Price Tag)
        props = dict(boxstyle='square', facecolor='#FF00FF', alpha=0.9, edgecolor='none')
        self.y_label_text = self.ax1.text(
            1.0, 0, "",
            transform=self.ax1.get_yaxis_transform(),  # 讓 x=1.0 代表軸的最右邊
            ha='left', va='center',
            color='white', fontweight='bold', fontsize=10,
            bbox=props, visible=False
        )

        self.canvas.draw()

        def format_date(x, pos):
            idx = int(round(x))
            if 0 <= idx < len(self.current_df):
                return self.current_df.index[idx].strftime('%Y-%m-%d')
            return ""

        self.ax2.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
        self.ax2.xaxis.set_major_locator(ticker.MaxNLocator(nbins=8))

        self.draw_candles_and_indicators()

        self.vline_main = self.ax1.axvline(x=0, color='white', ls='--', lw=0.8, visible=False)
        self.vline_sub = self.ax2.axvline(x=0, color='white', ls='--', lw=0.8, visible=False)
        self.hline_main = self.ax1.axhline(y=0, color='white', ls='--', lw=0.8, visible=False)

        self.canvas.draw()

    def draw_candles_and_indicators(self):
        df = self.current_df
        x = self.x_vals

        width = 0.6

        # 🔥 [修正] K棒顏色判斷：依據「收盤 vs 開盤」
        # 收 > 開 = 紅
        up = df[df['Close'] > df['Open']]
        # 收 < 開 = 綠
        down = df[df['Close'] < df['Open']]
        # 收 = 開 = 白 (十字線)
        flat = df[df['Close'] == df['Open']]

        def get_idx(sub):
            return [df.index.get_loc(i) for i in sub.index]

        # --- 1. 畫紅棒 (Close > Open) ---
        if not up.empty:
            idx = get_idx(up)
            # 實體: 高度 = 收 - 開, 底部 = 開
            self.ax1.bar(idx, up['Close'] - up['Open'], width, bottom=up['Open'], color='#FF3333', edgecolor='#FF3333')
            # 上下影線
            self.ax1.vlines(idx, up['Low'], up['High'], color='#FF3333', lw=0.8)

        # --- 2. 畫綠棒 (Close < Open) ---
        if not down.empty:
            idx = get_idx(down)
            # 實體: 高度 = 開 - 收, 底部 = 收 (Matplotlib bar高度需為正，或者用 bottom=Open, height=Close-Open)
            # 這裡用 bottom=Open, height=Close-Open 會是負值，Matplotlib 支援，或者反過來寫
            # 統一寫法：底部=Close, 高度=Open-Close
            self.ax1.bar(idx, up['Open'] - up['Close'] if False else down['Open'] - down['Close'], width,
                         bottom=down['Close'], color='#00FF00', edgecolor='#00FF00')
            # 上下影線
            self.ax1.vlines(idx, down['Low'], down['High'], color='#00FF00', lw=0.8)

        # --- 3. 畫平盤/十字線 (Close == Open) ---
        if not flat.empty:
            idx = np.array(get_idx(flat))
            # 🔥 [修正] 平盤沒有實體，改畫水平線 (Cross/Doji)
            # xmin, xmax 是相對座標
            self.ax1.hlines(flat['Close'], idx - width / 2, idx + width / 2, colors='#FFFFFF', lw=2)
            # 上下影線
            self.ax1.vlines(idx, flat['Low'], flat['High'], color='#FFFFFF', lw=0.8)

        # 畫均線
        self.ax1.plot(x, df['Close'].rolling(5).mean(), color='yellow', lw=1, label='MA5')
        self.ax1.plot(x, df['Close'].rolling(20).mean(), color='orange', lw=1, label='MA20')
        self.ax1.plot(x, df['Close'].rolling(60).mean(), color='cyan', lw=1, label='MA60')

        # 副圖
        self.ax2.clear()
        self.ax2.grid(True, color='#333', linestyle='--', alpha=0.5)
        self.ax2.set_facecolor('#121212')
        self.ax2.yaxis.tick_right()

        def format_date(x, pos):
            idx = int(round(x))
            if 0 <= idx < len(df): return df.index[idx].strftime('%Y-%m-%d')
            return ""

        self.ax2.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
        self.ax2.xaxis.set_major_locator(ticker.MaxNLocator(nbins=8))

        name = self.current_indicator

        if name == "Vix Fix":
            wvf = Indicators.cm_williams_vix_fix(df)
            wvf_ma = wvf.rolling(20).mean()
            wvf_std = wvf.rolling(20).std()
            upper_band = wvf_ma + (2.0 * wvf_std)
            range_high = wvf.rolling(50).max() * 0.85

            self.ax2.plot(x, upper_band, color='#FF00FF', lw=1, alpha=0.7)
            self.ax2.plot(x, range_high, color='#00E5FF', lw=1, alpha=0.7)

            is_panic = (wvf >= upper_band) | (wvf >= range_high)
            cols = ['lime' if p else 'gray' for p in is_panic]
            self.ax2.bar(x, wvf, color=cols, width=1.0, alpha=0.8)

        elif name == "KD":
            kd = Indicators.kd(df)
            self.ax2.plot(x, kd['K'], color='orange', lw=1)
            self.ax2.plot(x, kd['D'], color='cyan', lw=1)
            self.ax2.axhline(80, color='#555', ls='--')
            self.ax2.axhline(20, color='#555', ls='--')

        elif name == "MACD":
            m = Indicators.macd(df)
            cols = ['red' if v >= 0 else 'green' for v in m['MACD']]
            self.ax2.bar(x, m['MACD'], color=cols, width=1.0)
            self.ax2.plot(x, m['DIF'], color='yellow', lw=1)
            self.ax2.plot(x, m['DEA'], color='cyan', lw=1)

        elif name == "RSI":
            rsi = Indicators.rsi(df)
            self.ax2.plot(x, rsi, color='yellow', lw=1)
            self.ax2.axhline(70, color='red', ls='--')
            self.ax2.axhline(30, color='green', ls='--')

        else:  # Volume
            # 🔥 [修正] 成交量顏色跟隨 K 棒顏色 (Close vs Open)
            cols = []
            for i in range(len(df)):
                c = df['Close'].iloc[i]
                o = df['Open'].iloc[i]
                if c > o:
                    cols.append('#FF3333')
                elif c < o:
                    cols.append('#00FF00')
                else:
                    cols.append('#FFFFFF')
            self.ax2.bar(x, df['Volume'], color=cols, width=1.0)

    def on_indicator_changed(self, text):
        self.current_indicator = text
        self.draw_candles_and_indicators()
        xlim = self.ax1.get_xlim()
        self.update_view_range(xlim[0], xlim[1])

    def update_view_range(self, start, end):
        start = max(0, int(start))
        end = min(len(self.current_df), int(end))
        if end - start < 5: return

        self.ax1.set_xlim(start, end)
        self.ax2.set_xlim(start, end)

        view_df = self.current_df.iloc[start:end]
        if not view_df.empty:
            y_min, y_max = view_df['Low'].min(), view_df['High'].max()
            pad = (y_max - y_min) * 0.05
            self.ax1.set_ylim(y_min - pad, y_max + pad)

        self.canvas.draw_idle()

    def on_mouse_move(self, event):
        if not event.inaxes:
            # 滑鼠移出畫布：隱藏 Y 軸標籤
            if self.y_label_text: self.y_label_text.set_visible(False)
            self.canvas.draw_idle()
            return

        # 1. 拖曳平移 (Panning)
        if self.is_dragging and self.last_drag_x is not None:
            dx = event.xdata - self.last_drag_x
            xlim = self.ax1.get_xlim()
            self.update_view_range(xlim[0] - dx, xlim[1] - dx)
            self.last_drag_x = event.xdata
            return

        x_idx = int(round(event.xdata))

        # 2. Y軸查價標籤更新 (優化版：只更新數值與位置，不重建)
        if event.inaxes == self.ax1:
            price = event.ydata
            if self.y_label_text:
                self.y_label_text.set_text(f"{price:.2f}")
                self.y_label_text.set_y(price)
                self.y_label_text.set_visible(True)

        # 效能優化：如果 X 軸沒變，且滑鼠不在主圖(不需要更新Y軸線)，則跳過重畫
        if x_idx == self.last_mouse_idx:
            if event.inaxes != self.ax1: return

        self.last_mouse_idx = x_idx

        # 3. 十字線與資訊列更新
        if 0 <= x_idx < len(self.current_df):
            # 垂直線 (上下同步)
            self.vline_main.set_xdata([x_idx])
            self.vline_main.set_visible(True)
            self.vline_sub.set_xdata([x_idx])
            self.vline_sub.set_visible(True)

            # 水平線 (只在主圖顯示)
            if event.inaxes == self.ax1:
                self.hline_main.set_ydata([event.ydata])
                self.hline_main.set_visible(True)
            else:
                self.hline_main.set_visible(False)

            # 更新上方資訊列
            self._update_info_label(x_idx)
            self.canvas.draw_idle()

    def _update_info_label(self, idx):
        """ 🔥 核心：生成 HTML 彩色資訊列 """
        row = self.current_df.iloc[idx]
        dt_str = self.current_df.index[idx].strftime('%Y-%m-%d')

        # 1. 顏色計算 (全部比對 PrevClose)
        p = row['PrevClose']

        def get_fmt(val, base):
            if val > base: return "#FF3333"  # 紅
            if val < base: return "#00FF00"  # 綠
            return "#FFFFFF"  # 白

        c_open = get_fmt(row['Open'], p)
        c_high = get_fmt(row['High'], p)
        c_low = get_fmt(row['Low'], p)
        c_close = get_fmt(row['Close'], p)

        # 漲跌幅
        change = row['Change']
        pct = row['PctChange']
        sign = "+" if change > 0 else ""
        c_change = c_close  # 漲跌顏色跟收盤一樣

        # 2. 副圖數據
        sub_info = ""
        ind = self.current_indicator
        if ind == "Vix Fix":
            v = Indicators.cm_williams_vix_fix(self.current_df).iloc[idx]
            sub_info = f"Vix:<span style='color:#00E5FF;'>{v:.2f}</span>"
        elif ind == "KD":
            k = Indicators.kd(self.current_df)['K'].iloc[idx]
            d = Indicators.kd(self.current_df)['D'].iloc[idx]
            sub_info = f"K:<span style='color:orange;'>{k:.1f}</span> D:<span style='color:cyan;'>{d:.1f}</span>"
        elif ind == "MACD":
            m = Indicators.macd(self.current_df).iloc[idx]
            sub_info = f"DIF:<span style='color:yellow;'>{m['DIF']:.2f}</span> DEA:<span style='color:cyan;'>{m['DEA']:.2f}</span> OSC:{m['MACD']:.2f}"
        elif ind == "RSI":
            r = Indicators.rsi(self.current_df).iloc[idx]
            sub_info = f"RSI:<span style='color:yellow;'>{r:.1f}</span>"
        else:
            sub_info = f"Vol:<span style='color:yellow;'>{int(row['Volume']):,}</span>"

        # 3. 組合 HTML
        # 代號/日期 (亮黃/灰) | O H L C (各自變色) | 漲跌 (變色) | 副圖
        html = (
            f"<span style='color:#DDD;'>{dt_str}</span>&nbsp;&nbsp;"
            f"O:<span style='color:{c_open};'>{row['Open']:.2f}</span>&nbsp;"
            f"H:<span style='color:{c_high};'>{row['High']:.2f}</span>&nbsp;"
            f"L:<span style='color:{c_low};'>{row['Low']:.2f}</span>&nbsp;"
            f"C:<span style='color:{c_close}; font-weight:bold;'>{row['Close']:.2f}</span>&nbsp;"
            f"<span style='color:{c_change};'>({sign}{change:.2f} / {sign}{pct:.2f}%)</span>&nbsp;&nbsp;|&nbsp;&nbsp;"
            f"{sub_info}"
        )

        self.info_label.setText(html)

    def on_scroll(self, event):
        if not event.inaxes: return
        xlim = self.ax1.get_xlim()
        curr_range = xlim[1] - xlim[0]
        mouse_x = event.xdata
        if mouse_x is None: return

        scale = 0.8 if event.button == 'up' else 1.2
        new_range = curr_range * scale

        rel = (mouse_x - xlim[0]) / curr_range
        new_start = mouse_x - new_range * rel
        self.update_view_range(new_start, new_start + new_range)

    def on_mouse_press(self, event):
        if event.button == 1:
            self.is_dragging = True
            self.last_drag_x = event.xdata

    def on_mouse_release(self, event):
        self.is_dragging = False
        self.last_drag_x = None