import sys
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
from pathlib import Path
# 🟢 修正：補上 QLabel, QHBoxLayout, QFrame 等必要的 UI 元件
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QComboBox,
                             QApplication, QLabel, QFrame)
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from PyQt6.QtCore import pyqtSignal, Qt

# 中文與字體支援
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


class KLineModule(QWidget):
    # 定義訊號，讓外部可以叫這個模組切換股票
    stock_changed = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.raw_df = None
        self.display_df = None
        self.current_plot_df = None
        self.view_limit = 100
        self.current_stock_id = ""

        self.init_ui()

    def init_ui(self):
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)
        self.setStyleSheet("background-color: #000000;")

        # 1. 統一標題與資訊列 (Top Info Bar)
        self.info_bar = QWidget()
        self.info_bar.setFixedHeight(40)  # 稍微加高
        self.info_bar.setStyleSheet("background-color: #050505; border-bottom: 1px solid #333;")
        info_layout = QHBoxLayout(self.info_bar)
        info_layout.setContentsMargins(10, 0, 10, 0)

        # 標題 (左上角)
        self.title_label = QLabel("技術分析 (K線/MA)")
        self.title_label.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 16px;")
        info_layout.addWidget(self.title_label)

        # 數據顯示區 (中間 - 支援 HTML 彩色顯示)
        self.data_label = QLabel("請移動滑鼠查看數據")
        self.data_label.setStyleSheet("font-family: 'Consolas'; font-size: 13px; color: #888;")
        self.data_label.setTextFormat(Qt.TextFormat.RichText)  # 確保支援 HTML
        info_layout.addWidget(self.data_label)

        info_layout.addStretch()

        # 週期切換 (右側)
        self.period_combo = QComboBox()
        self.period_combo.addItems(["日線", "週線", "月線"])
        self.period_combo.setStyleSheet("color: white; background-color: #222; border: 1px solid #444;")
        self.period_combo.currentTextChanged.connect(self.change_period)
        info_layout.addWidget(self.period_combo)

        self.main_layout.addWidget(self.info_bar)

        # 2. 畫布
        self.fig = Figure(facecolor='#000000')
        self.canvas = FigureCanvas(self.fig)
        self.main_layout.addWidget(self.canvas)

        # 綁定事件
        self.canvas.mpl_connect('scroll_event', self.on_scroll)
        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)

    def load_stock_data(self, stock_id):
        # 嘗試讀取快取
        path = Path(f"data/cache/tw/{stock_id}.parquet")
        if not path.exists():
            return False

        df = pd.read_parquet(path)

        # 欄位標準化 (轉為首字大寫)
        df.columns = [c.capitalize() for c in df.columns]
        if 'Adj close' in df.columns:
            df = df.rename(columns={'Adj close': 'Adj Close'})

        # --- 預先計算均線 (為了顯示數值) ---
        df['MA5'] = df['Close'].rolling(5).mean()
        df['MA20'] = df['Close'].rolling(20).mean()
        df['MA60'] = df['Close'].rolling(60).mean()

        self.raw_df = df
        self.current_stock_id = stock_id
        self.change_period()  # 觸發繪圖
        return True

    def change_period(self):
        if self.raw_df is None: return
        period = self.period_combo.currentText()
        df = self.raw_df.copy()

        if period in ["週線", "月線"]:
            rule = 'W' if period == "週線" else 'M'
            # Resample 邏輯
            agg_dict = {
                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last',
                'Volume': 'sum'
            }
            # 如果有 MA 也要處理，但通常重算比較準，這裡先簡單取 last
            if 'MA5' in df.columns: agg_dict['MA5'] = 'last'
            if 'MA20' in df.columns: agg_dict['MA20'] = 'last'
            if 'MA60' in df.columns: agg_dict['MA60'] = 'last'

            df = df.resample(rule).agg(agg_dict).dropna()

        self.display_df = df
        self.update_plot()

    def update_plot(self):
        if self.display_df is None: return
        self.fig.clear()

        # 取出要畫的範圍
        self.current_plot_df = self.display_df.tail(self.view_limit).copy()

        if self.current_plot_df.empty: return

        ymin, ymax = self.current_plot_df['Low'].min(), self.current_plot_df['High'].max()
        padding = (ymax - ymin) * 0.1

        # 設定 K 線樣式 (紅漲綠跌)
        mc = mpf.make_marketcolors(up='#ff333a', down='#00d16d', inherit=True)
        my_style = mpf.make_mpf_style(
            base_mpf_style='charles', marketcolors=mc,
            facecolor='#000000', figcolor='#000000', gridcolor='#1a1a1a'
        )

        # 建立子圖 (上:K線, 下:成交量)
        self.ax1, self.ax2 = self.fig.subplots(2, 1, sharex=True, gridspec_kw={'height_ratios': [3, 1]})
        self.fig.subplots_adjust(hspace=0.01, left=0.06, right=0.94, top=0.98, bottom=0.05)

        # 繪製均線 (不使用 mav 參數，改用 addplot 才能確保數值對應)
        apds = [
            mpf.make_addplot(self.current_plot_df['MA5'], ax=self.ax1, color='#ffffff', width=0.8),
            mpf.make_addplot(self.current_plot_df['MA20'], ax=self.ax1, color='#ff9900', width=0.8),
            mpf.make_addplot(self.current_plot_df['MA60'], ax=self.ax1, color='#ff00ff', width=0.8)
        ]

        # 繪製 K 線
        mpf.plot(self.current_plot_df, type='candle', ax=self.ax1, volume=self.ax2,
                 style=my_style, addplot=apds, datetime_format='%Y-%m')

        self.ax1.set_ylim(ymin - padding, ymax + padding)
        for ax in [self.ax1, self.ax2]:
            ax.set_facecolor('#000000')
            ax.tick_params(axis='both', colors='#888888', labelsize=8)

        # 初始化十字線 (Crosshair)
        self.v_line = self.ax1.axvline(color='#ffffff', linestyle='--', linewidth=0.7, alpha=0.5, visible=False)
        self.v_line2 = self.ax2.axvline(color='#ffffff', linestyle='--', linewidth=0.7, alpha=0.5, visible=False)
        self.h_line = self.ax1.axhline(color='#ffffff', linestyle='--', linewidth=0.7, alpha=0.5, visible=False)

        self.canvas.draw()

    def on_mouse_move(self, event):
        if not event.inaxes or self.current_plot_df is None:
            return

        # 找出游標對應的 K 棒索引
        x_idx = int(round(event.xdata))
        if 0 <= x_idx < len(self.current_plot_df):
            data = self.current_plot_df.iloc[x_idx]
            date_str = self.current_plot_df.index[x_idx].strftime('%Y-%m-%d')

            # 漲跌顏色判斷
            close_price = data['Close']
            open_price = data['Open']
            pct = ((close_price - open_price) / open_price) * 100
            price_color = '#FF3333' if close_price >= open_price else '#00FF00'

            # 組合 HTML 字串 (彩色方塊 ■)
            # MA5:白(#FFF), MA20:橘(#FF9900), MA60:紫(#FF00FF)
            html = (
                f"<span style='color:#DDD;'>{date_str}</span> | "
                f"<span style='color:{price_color}; font-weight:bold;'>Close:{close_price:.1f} ({pct:+.2f}%)</span> | "
                f"Vol:{int(data['Volume']):,} | "
                f"<span style='color:#FFFFFF;'>■ MA5:{data['MA5']:.1f}</span>  "
                f"<span style='color:#FF9900;'>■ MA20:{data['MA20']:.1f}</span>  "
                f"<span style='color:#FF00FF;'>■ MA60:{data['MA60']:.1f}</span>"
            )

            # 更新上方 Info Bar
            self.data_label.setText(html)

            # 更新十字線位置
            self.v_line.set_xdata([x_idx])
            self.v_line2.set_xdata([x_idx])
            self.h_line.set_ydata([event.ydata])

            self.v_line.set_visible(True)
            self.v_line2.set_visible(True)
            self.h_line.set_visible(True)

            self.canvas.draw_idle()

    def on_scroll(self, event):
        if event.button == 'up':
            self.view_limit = max(10, int(self.view_limit * 0.8))
        else:
            self.view_limit = min(len(self.display_df), int(self.view_limit * 1.2))
        self.update_plot()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    test_win = KLineModule()

    # 測試用：自動抓取 data/cache/tw 下的第一個檔案
    cache_path = Path("data/cache/tw")
    if cache_path.exists():
        first_file = next(cache_path.glob("*.parquet"), None)
        if first_file:
            print(f"Testing with: {first_file.stem}")
            test_win.load_stock_data(first_file.stem)
        else:
            print("No parquet files found in data/cache/tw")

    test_win.resize(1000, 600)
    test_win.show()
    sys.exit(app.exec())