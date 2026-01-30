import sys
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path

from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                             QComboBox, QPushButton, QButtonGroup, QWidget, QApplication)

# Matplotlib
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.gridspec as gridspec
import matplotlib.ticker as ticker

# Internal Tools
from utils.indicators import Indicators
from utils.quote_worker import QuoteWorker


class ExpandedKLineWindow(QDialog):
    """
    Advanced War Room (Expanded KLine) - V3.2
    """

    # 🔥 Sync with KLineModule
    MA_CONFIG = {
        'D': [5, 20, 60],
        'W': [13, 34],
        'M': [3, 6, 12]
    }

    MA_COLORS = {
        5: '#FFFF00', 10: '#FF00FF', 20: '#FF8800',
        60: '#00FFFF', 13: '#AAFF00', 34: '#FFAA00',
        3: '#FFFF00', 6: '#FF8800', 12: '#00FFFF'
    }

    def __init__(self, stock_id, df, stock_name="", parent=None):
        super().__init__(parent)

        # 1. Basic Properties
        self.stock_id = stock_id
        self.stock_name = stock_name
        self.display_id = stock_id.split('_')[0]

        self.setWindowTitle(f"進階戰情室 - {self.display_id} {stock_name}")
        self.resize(1200, 850)
        self.setStyleSheet("background-color: #121212; color: white;")

        self.is_closing = False

        # 2. Data Init
        self.df_source = df.copy()
        if not self.df_source.empty:
            self.df_source.index = pd.to_datetime(self.df_source.index)
        self.current_df = self.df_source

        # 3. State Variables
        self.current_indicator = "Vix Fix"
        self.current_tf = "D"
        self.last_mouse_idx = -1
        self.is_dragging = False
        self.last_drag_x = None

        # Store indicator values for labels
        self.ind_values = {}

        # 4. Init UI
        self.init_ui()

        # 5. Init Chart Structure
        self.plot_chart_structure()

        # 6. Init Quote Worker
        self.quote_worker = QuoteWorker(self)
        self.quote_worker.set_monitoring_stocks([self.stock_id])
        self.quote_worker.quote_updated.connect(self.on_realtime_quote)
        self.quote_worker.start()

        # Initial Load
        self.update_data_frequency("D")

    def closeEvent(self, event):
        self.is_closing = True
        if hasattr(self, 'quote_worker') and self.quote_worker.isRunning():
            try:
                self.quote_worker.quote_updated.disconnect()
            except:
                pass
            self.quote_worker.stop()
            self.quote_worker.wait(1000)
        super().closeEvent(event)

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # --- Toolbar ---
        toolbar = QHBoxLayout()
        toolbar.setContentsMargins(15, 10, 15, 10)
        toolbar.setSpacing(15)

        self.info_title = QLabel(f"{self.display_id} {self.stock_name}")
        self.info_title.setStyleSheet("font-size: 22px; font-weight: bold; color: #FFFF00;")
        toolbar.addWidget(self.info_title)

        self.btn_group = QButtonGroup(self)
        self.btn_day = self._create_tf_btn("日", "D")
        self.btn_week = self._create_tf_btn("周", "W")
        self.btn_month = self._create_tf_btn("月", "M")
        self.btn_day.setChecked(True)

        toolbar.addWidget(self.btn_day)
        toolbar.addWidget(self.btn_week)
        toolbar.addWidget(self.btn_month)

        toolbar.addWidget(QLabel("|"))

        toolbar.addWidget(QLabel("副圖:"))
        self.combo = QComboBox()
        self.combo.addItems(["Volume", "Vix Fix", "KD", "MACD", "RSI"])
        self.combo.setCurrentText(self.current_indicator)
        self.combo.setStyleSheet("""
            QComboBox { background: #333; color: white; padding: 5px; border: 1px solid #555; }
        """)
        self.combo.currentTextChanged.connect(self.on_indicator_changed)
        toolbar.addWidget(self.combo)

        toolbar.addStretch()

        btn_close = QPushButton("關閉視窗")
        btn_close.clicked.connect(self.close)
        btn_close.setStyleSheet("background: #444; color: white; padding: 5px 20px; border-radius: 4px;")
        toolbar.addWidget(btn_close)

        layout.addLayout(toolbar)

        # --- Info Bar (HTML) ---
        self.info_bg = QWidget()
        self.info_bg.setFixedHeight(45)
        self.info_bg.setStyleSheet(
            "background-color: #0A0A0A; border-top: 1px solid #333; border-bottom: 1px solid #333;")
        info_layout = QHBoxLayout(self.info_bg)

        self.info_label = QLabel("正在同步即時數據...")
        self.info_label.setStyleSheet("font-family: 'Consolas', 'Microsoft JhengHei'; font-size: 15px; color: #CCC;")
        self.info_label.setTextFormat(Qt.TextFormat.RichText)
        info_layout.addWidget(self.info_label)
        layout.addWidget(self.info_bg)

        # --- Canvas ---
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
        btn.setFixedSize(45, 30)
        btn.setStyleSheet("""
            QPushButton { background: #222; color: #AAA; border: 1px solid #444; border-radius: 3px; }
            QPushButton:checked { background: #00E5FF; color: black; font-weight: bold; }
        """)
        btn.clicked.connect(lambda: self.update_data_frequency(code))
        self.btn_group.addButton(btn)
        return btn

    def update_stock_data(self, stock_id, df, stock_name=""):
        self.stock_id = stock_id
        self.stock_name = stock_name
        self.display_id = stock_id.split('_')[0]

        self.setWindowTitle(f"進階戰情室 - {self.display_id} {stock_name}")
        self.info_title.setText(f"{self.display_id} {stock_name}")

        self.df_source = df.copy()
        if not self.df_source.empty:
            self.df_source.index = pd.to_datetime(self.df_source.index)

        self.update_data_frequency(self.current_tf)
        self.quote_worker.set_monitoring_stocks([stock_id])

    def on_indicator_changed(self, name):
        self.current_indicator = name
        self.draw_candles_and_indicators()
        # Redraw info label to show new indicator values
        if not self.current_df.empty:
            self._update_info_label(len(self.current_df) - 1)

    def on_realtime_quote(self, data_dict):
        if self.is_closing: return
        if self.current_df is None or self.current_df.empty: return
        if self.current_tf != 'D': return

        target_key = self.stock_id.split('_')[0]
        if target_key not in data_dict: return

        quote = data_dict[target_key]
        real = quote.get('realtime', {})

        try:
            latest = float(real.get('latest_trade_price', 0) or 0)
            close = float(real.get('close', 0) or 0)
            trade_price = latest if latest > 0 else close
            if trade_price == 0: return

            open_p = float(real.get('open', 0) or 0)
            high_p = float(real.get('high', 0) or 0)
            low_p = float(real.get('low', 0) or 0)
            vol = float(real.get('accumulate_trade_volume', 0) or 0)

            if open_p == 0: open_p = trade_price
            if high_p == 0: high_p = trade_price
            if low_p == 0: low_p = trade_price

            high_p = max(high_p, trade_price, open_p)
            low_p = min(low_p, trade_price, open_p)

            today_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            last_idx = self.current_df.index[-1]
            last_date = last_idx.replace(hour=0, minute=0, second=0, microsecond=0)

            need_redraw = False

            # Update Logic
            if today_date > last_date:
                prev_close = self.current_df.iloc[-1]['Close']
                change = trade_price - prev_close
                pct_change = (change / prev_close) * 100 if prev_close != 0 else 0

                new_data = {
                    'Open': open_p, 'High': high_p, 'Low': low_p, 'Close': trade_price,
                    'Volume': vol,
                    'PrevClose': prev_close, 'Change': change, 'PctChange': pct_change
                }
                # Init MA cols with NaN
                for ma in self.MA_CONFIG.get(self.current_tf, []):
                    new_data[f'MA{ma}'] = np.nan

                new_row = pd.Series(new_data, name=today_date)
                self.current_df = pd.concat([self.current_df, pd.DataFrame([new_row])])
                need_redraw = True

            elif today_date == last_date:
                current_vol = self.current_df.at[last_idx, 'Volume']
                current_close = self.current_df.at[last_idx, 'Close']

                if abs(current_close - trade_price) > 0.0001 or abs(current_vol - vol) > 0.0001:
                    self.current_df.at[last_idx, 'Open'] = open_p
                    self.current_df.at[last_idx, 'High'] = high_p
                    self.current_df.at[last_idx, 'Low'] = low_p
                    self.current_df.at[last_idx, 'Close'] = trade_price
                    self.current_df.at[last_idx, 'Volume'] = vol

                    pc = self.current_df.at[last_idx, 'PrevClose']
                    if pc != 0:
                        self.current_df.at[last_idx, 'Change'] = trade_price - pc
                        self.current_df.at[last_idx, 'PctChange'] = ((trade_price - pc) / pc) * 100
                    need_redraw = True

            if need_redraw:
                # Re-calculate MAs and Indicators
                self._calculate_ma()
                self.x_vals = np.arange(len(self.current_df))
                self.draw_candles_and_indicators()
                self._update_info_label(len(self.current_df) - 1)

        except Exception as e:
            print(f"DEBUG: Expanded KLine update error: {e}")

    def update_data_frequency(self, tf_code):
        self.current_tf = tf_code
        if tf_code == 'D':
            self.current_df = self.df_source.copy()
        else:
            rule = 'W-FRI' if tf_code == 'W' else 'ME'
            logic = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
            self.current_df = self.df_source.resample(rule).agg(logic).dropna()

        # Basic Calcs
        self.current_df['PrevClose'] = self.current_df['Close'].shift(1)
        if not self.current_df.empty:
            self.current_df.iloc[0, self.current_df.columns.get_loc('PrevClose')] = self.current_df.iloc[0]['Open']

        self.current_df['Change'] = self.current_df['Close'] - self.current_df['PrevClose']
        self.current_df['PctChange'] = (self.current_df['Change'] / self.current_df['PrevClose']) * 100

        self._calculate_ma()

        self.x_vals = np.arange(len(self.current_df))
        self.draw_candles_and_indicators()

        total = len(self.current_df)
        self.update_view_range(max(0, total - 120), total)
        if total > 0: self._update_info_label(total - 1)

    def _calculate_ma(self):
        """ 🔥 Dynamic MA Calculation based on Config """
        ma_list = self.MA_CONFIG.get(self.current_tf, [])
        for ma in ma_list:
            self.current_df[f'MA{ma}'] = self.current_df['Close'].rolling(ma).mean()

    def plot_chart_structure(self):
        self.figure.clear()
        gs = gridspec.GridSpec(2, 1, height_ratios=[3, 1])
        gs.update(left=0.06, right=0.94, top=0.96, bottom=0.06, hspace=0.05)

        self.ax1 = self.figure.add_subplot(gs[0])
        self.ax2 = self.figure.add_subplot(gs[1], sharex=self.ax1)

        for ax in [self.ax1, self.ax2]:
            ax.set_facecolor('#121212')
            ax.grid(True, color='#222', ls='--', alpha=0.5)
            ax.tick_params(colors='white', labelsize=9)
            ax.yaxis.tick_right()

        self.vline1 = self.ax1.axvline(0, color='white', ls='--', lw=0.7, visible=False)
        self.vline2 = self.ax2.axvline(0, color='white', ls='--', lw=0.7, visible=False)
        self.hline1 = self.ax1.axhline(0, color='white', ls='--', lw=0.7, visible=False)

        # 🔥 Price Tag Label
        props = dict(boxstyle='square', facecolor='#FF00FF', alpha=0.9, edgecolor='none')
        self.y_label_text = self.ax1.text(1.01, 0, "", transform=self.ax1.get_yaxis_transform(),
                                          color='white', fontsize=10, fontweight='bold',
                                          va='center', ha='left', bbox=props, visible=False)

    def draw_candles_and_indicators(self):
        self.ax1.clear()
        self.ax2.clear()
        self.plot_chart_structure()

        df = self.current_df
        x = self.x_vals
        width = 0.6

        # --- Main Chart ---
        up = df['Close'] >= df['Open']
        down = df['Close'] < df['Open']

        self.ax1.bar(x[up], df['Close'][up] - df['Open'][up], width, bottom=df['Open'][up],
                     color='#FF3333', edgecolor='#FF3333', linewidth=0.8)
        self.ax1.vlines(x[up], df['Low'][up], df['High'][up], color='#FF3333', lw=1)

        self.ax1.bar(x[down], df['Open'][down] - df['Close'][down], width, bottom=df['Close'][down],
                     color='#00FF00', edgecolor='#00FF00', linewidth=0.8)
        self.ax1.vlines(x[down], df['Low'][down], df['High'][down], color='#00FF00', lw=1)

        # 🔥 Draw MAs (Dynamic)
        ma_list = self.MA_CONFIG.get(self.current_tf, [])
        for ma in ma_list:
            col_name = f'MA{ma}'
            if col_name in df.columns:
                color = self.MA_COLORS.get(ma, '#FFFFFF')
                self.ax1.plot(x, df[col_name], color=color, lw=1, alpha=0.8)

        # --- Sub Chart ---
        name = self.current_indicator
        bg_color = '#121212'

        # Reset indicator values storage for label display
        self.ind_values = {}

        if name == "Vix Fix":
            # 1. Calculation
            wvf = Indicators.cm_williams_vix_fix(df, period=22)
            wvf_std = wvf.rolling(window=20).std()
            wvf_sma = wvf.rolling(window=20).mean()
            upper_band = wvf_sma + (2.0 * wvf_std)
            range_high = wvf.rolling(window=50).max() * 0.85

            # 2. Colors
            is_green = (wvf >= upper_band) | (wvf >= range_high)
            bar_colors = np.where(is_green, '#00FF00', '#444444')

            # 3. Plot
            self.ax2.bar(x, wvf, color=bar_colors, width=width, edgecolor=bg_color, linewidth=0.5)
            self.ax2.plot(x, upper_band, color='#00FFFF', lw=1, alpha=0.6)  # Cyan for Upper
            self.ax2.plot(x, range_high, color='#FFA500', lw=1, alpha=0.9)  # 🔥 Orange for Range High

            # 4. Store for labels
            self.current_df['Ind_Main'] = wvf
            self.current_df['Ind_Sub1'] = upper_band
            self.current_df['Ind_Sub2'] = range_high
            self.ind_values = {'name': 'Vix', 'main': 'WVF', 'sub1': 'UB', 'sub2': 'RH'}

        elif name == "KD":
            kd = Indicators.kd(df)
            self.ax2.plot(x, kd['K'], color='orange', lw=1)
            self.ax2.plot(x, kd['D'], color='cyan', lw=1)
            self.ax2.axhline(80, color='#555', ls='--', lw=0.5)
            self.ax2.axhline(20, color='#555', ls='--', lw=0.5)

            self.current_df['Ind_Main'] = kd['K']
            self.current_df['Ind_Sub1'] = kd['D']
            self.ind_values = {'name': 'KD', 'main': 'K', 'sub1': 'D'}

        elif name == "Volume":
            colors = ['#FF3333' if c >= o else '#00FF00' for c, o in zip(df['Close'], df['Open'])]
            self.ax2.bar(x, df['Volume'], color=colors, width=width, edgecolor=bg_color, linewidth=0.5)

            self.current_df['Ind_Main'] = df['Volume']
            self.ind_values = {'name': 'Vol', 'main': 'Vol'}

        # ... (Add MACD/RSI similar logic if needed)

        self.canvas.draw_idle()

    def _update_info_label(self, idx):
        if idx < 0 or idx >= len(self.current_df): return
        row = self.current_df.iloc[idx]
        dt = self.current_df.index[idx].strftime('%Y-%m-%d')
        p = row['PrevClose']

        def get_c(v, b):
            return "#FF3333" if v > b else "#00FF00" if v < b else "#FFFFFF"

        # 🔥 1. Removed Stock ID/Name (Only Date + Data)
        base_html = (
            f"<span style='color:#888; font-size:15px;'>{dt}</span> &nbsp;&nbsp; "
            f"O:<span style='color:{get_c(row['Open'], p)};'>{row['Open']:.2f}</span> "
            f"H:<span style='color:{get_c(row['High'], p)};'>{row['High']:.2f}</span> "
            f"L:<span style='color:{get_c(row['Low'], p)};'>{row['Low']:.2f}</span> "
            f"C:<span style='color:{get_c(row['Close'], p)};'>{row['Close']:.2f}</span> "
            f"<span style='color:{get_c(row['Close'], p)};'>({row['PctChange']:+.2f}%)</span>"
        )

        # 🔥 2. Dynamic MA Info
        ma_html = ""
        ma_list = self.MA_CONFIG.get(self.current_tf, [])
        for ma in ma_list:
            col = f'MA{ma}'
            if col in row and not pd.isna(row[col]):
                color = self.MA_COLORS.get(ma, '#FFF')
                ma_html += f" &nbsp;<span style='color:{color};'>MA{ma}:{row[col]:.2f}</span>"

        # 🔥 3. Indicator Values
        ind_html = ""
        if self.ind_values and 'Ind_Main' in row:
            name = self.ind_values.get('name', '')
            v_main = row['Ind_Main']

            if name == 'Vol':
                ind_html = f" &nbsp;| &nbsp;Vol:<span style='color:#FFFF00;'>{int(v_main):,}</span>"
            elif name == 'Vix':
                v_ub = row.get('Ind_Sub1', 0)
                v_rh = row.get('Ind_Sub2', 0)
                # Color logic: Green if > UB or RH
                is_g = v_main >= v_ub or v_main >= v_rh
                c_vix = "#00FF00" if is_g else "#AAA"
                ind_html = (f" &nbsp;| &nbsp;Vix:<span style='color:{c_vix};'>{v_main:.2f}</span> "
                            f"<span style='color:#00FFFF;'>UB:{v_ub:.2f}</span> "
                            f"<span style='color:#FFA500;'>RH:{v_rh:.2f}</span>")
            elif name == 'KD':
                k = v_main
                d = row.get('Ind_Sub1', 0)
                ind_html = (f" &nbsp;| &nbsp;K:<span style='color:orange;'>{k:.2f}</span> "
                            f"D:<span style='color:cyan;'>{d:.2f}</span>")

        final_html = base_html + ma_html + ind_html
        self.info_label.setText(final_html)

    def on_mouse_move(self, event):
        if not event.inaxes:
            self.vline1.set_visible(False)
            self.vline2.set_visible(False)
            self.hline1.set_visible(False)
            self.y_label_text.set_visible(False)
            self.canvas.draw_idle()
            return

        x_idx = int(round(event.xdata))
        if 0 <= x_idx < len(self.current_df):
            self.vline1.set_xdata([x_idx])
            self.vline1.set_visible(True)
            self.vline2.set_xdata([x_idx])
            self.vline2.set_visible(True)

            if event.inaxes == self.ax1:
                price = event.ydata
                self.hline1.set_ydata([price])
                self.hline1.set_visible(True)

                # 🔥 Update Price Tag
                self.y_label_text.set_text(f"{price:.2f}")
                self.y_label_text.set_y(price)
                self.y_label_text.set_visible(True)
            else:
                self.hline1.set_visible(False)
                self.y_label_text.set_visible(False)

            self._update_info_label(x_idx)
            self.canvas.draw_idle()

    def update_view_range(self, start, end):
        total_len = len(self.current_df)
        r = end - start
        if r >= total_len:
            start, end = 0, total_len
        else:
            if end > total_len:
                diff = end - total_len
                end = total_len
                start -= diff
            if start < 0: start = 0

        self.ax1.set_xlim(start, end)
        self.ax2.set_xlim(start, end)

        view = self.current_df.iloc[int(start):int(end)]
        if not view.empty:
            y_min, y_max = view['Low'].min(), view['High'].max()
            if pd.notna(y_min) and pd.notna(y_max):
                pad = (y_max - y_min) * 0.05
                self.ax1.set_ylim(y_min - pad, y_max + pad)

        self.canvas.draw_idle()

    def on_scroll(self, event):
        if not event.inaxes: return
        xlim = self.ax1.get_xlim()
        cur_len = xlim[1] - xlim[0]
        scale_factor = 0.8 if event.button == 'up' else 1.2
        new_len = cur_len * scale_factor
        mouse_x = event.xdata
        if mouse_x is None: return

        rel_pos = (mouse_x - xlim[0]) / cur_len
        new_start = mouse_x - new_len * rel_pos
        new_end = mouse_x + new_len * (1 - rel_pos)
        self.update_view_range(new_start, new_end)

    def on_mouse_press(self, event):
        if event.button == 1:
            if event.dblclick:
                total = len(self.current_df)
                self.update_view_range(max(0, total - 120), total)
                return
            self.is_dragging = True
            self.last_drag_x = event.xdata

    def on_mouse_release(self, event):
        self.is_dragging = False