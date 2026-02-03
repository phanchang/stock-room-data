import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

from modules.expanded_kline import ExpandedKLineWindow
from utils.quote_worker import QuoteWorker

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QPushButton, QButtonGroup, QSizePolicy,
                             QApplication, QMessageBox, QProgressDialog)
from PyQt6.QtCore import pyqtSignal, Qt, QTimer
from PyQt6.QtGui import QColor, QFont, QCursor

import matplotlib.pyplot as plt
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.gridspec as gridspec
import matplotlib.ticker as ticker

plt.style.use('dark_background')
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


class ChartOverlay(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setVisible(False)
        self.setStyleSheet("background-color: rgba(10, 10, 10, 230);")
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, False)

        layout = QVBoxLayout(self)
        self.lbl_text = QLabel("載入中...")
        self.lbl_text.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_text.setStyleSheet("color: #00E5FF; font-size: 24px; font-weight: bold; background: transparent;")
        layout.addWidget(self.lbl_text)

    def show_loading(self, text="載入中..."):
        self.lbl_text.setText(text)
        if self.parent():
            self.resize(self.parent().size())
        self.show()
        self.raise_()


class KLineModule(QWidget):
    stock_selected = pyqtSignal(str)

    MA_CONFIG = {
        'D': [5, 10, 22, 55, 200],
        'W': [30],
        'M': [3, 6, 12]
    }

    DEFAULT_VISIBLE_MA = {
        'D': [55, 200],
        'W': [30],
        'M': [3, 6]
    }

    MA_COLORS = {
        5: '#FFFF00', 10: '#FF00FF', 22: '#00FF00', 55: '#FF8800', 200: '#00FFFF',
        30: '#FFAA00',
        3: '#FFFF00', 6: '#FF8800', 12: '#00FFFF'
    }

    def __init__(self, parent=None, shared_worker=None):
        super().__init__(parent)
        self.current_stock_id = None
        self.current_stock_name = ""
        self.current_df = None
        self.display_df = None
        self.raw_df = None

        self.is_closing = False
        self.timeframe = 'D'
        self.visible_candles = 80
        self.scroll_pos = 0
        self.is_dragging = False
        self.last_mouse_x = None

        self.ax1 = None
        self.ax2 = None
        self.current_view_df = None
        self.cross_v1 = None
        self.y_label_text = None

        self.init_ui()
        self.init_chart_structure()
        self.overlay = ChartOverlay(self)
        self.waiting_for_realtime = False

        if shared_worker:
            self.quote_worker = shared_worker
        else:
            print("⚠️ [KLine] 未收到 Shared Worker，啟動獨立 Worker")
            self.quote_worker = QuoteWorker(self)
            # 🔥 [修正1] 這裡也不要偷跑 start()，等待外部觸發
            # self.quote_worker.start()

        self.quote_worker.quote_updated.connect(self.on_realtime_quote)

    def resizeEvent(self, event):
        if hasattr(self, 'overlay') and self.overlay.isVisible():
            self.overlay.resize(self.size())
        super().resizeEvent(event)

    def closeEvent(self, event):
        self.is_closing = True
        try:
            self.quote_worker.quote_updated.disconnect(self.on_realtime_quote)
        except:
            pass
        plt.close(self.fig)
        event.accept()

    def init_ui(self):
        self.setStyleSheet("background-color: #000000; color: #FFFFFF;")
        self.setMouseTracking(True)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        control_widget = QWidget()
        control_widget.setFixedHeight(45)
        control_widget.setStyleSheet("background-color: #050505; border-bottom: 1px solid #222;")

        control_layout = QHBoxLayout(control_widget)
        control_layout.setContentsMargins(10, 0, 10, 0)
        control_layout.setSpacing(15)

        title = QLabel("技術分析")
        title.setStyleSheet("color: #00E5FF; font-weight: bold; font-size: 16px;")

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

        self.lbl_data_date = QLabel("資料日期: --/--")
        self.lbl_data_date.setStyleSheet("color: #FF8800; font-weight: bold; font-size: 13px; margin-right: 10px;")
        control_layout.addWidget(self.lbl_data_date)

        btn_expand = QPushButton("⛶")
        btn_expand.setFixedSize(35, 30)
        btn_expand.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_expand.clicked.connect(self.open_war_room)
        btn_expand.setStyleSheet(
            "background: transparent; color: #AAA; font-size: 20px; border: 1px solid #444; border-radius: 4px;")
        btn_expand.setToolTip("開啟進階戰情室")
        control_layout.addWidget(btn_expand)

        layout.addWidget(control_widget)

        self.info_bg = QWidget()
        self.info_bg.setFixedHeight(50)
        self.info_bg.setStyleSheet("background-color: #0A0A0A; border-bottom: 1px solid #333;")
        info_layout = QHBoxLayout(self.info_bg)
        info_layout.setContentsMargins(10, 2, 10, 2)

        self.info_label = QLabel("準備就緒")
        self.info_label.setStyleSheet(
            "font-family: 'Consolas', 'Microsoft JhengHei'; font-size: 14px; color: #CCC; font-weight: bold;")
        self.info_label.setTextFormat(Qt.TextFormat.RichText)
        self.info_label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)

        info_layout.addWidget(self.info_label)
        layout.addWidget(self.info_bg)

        self.warning_label = QLabel("⚠️ 資料過期，請執行 [更新下載]", self)
        self.warning_label.setStyleSheet("""
            background-color: rgba(255, 0, 0, 180);
            color: white;
            font-weight: bold;
            font-size: 14px;
            padding: 5px 10px;
            border-radius: 4px;
        """)
        self.warning_label.move(60, 55)
        self.warning_label.hide()

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
        btn.setFixedSize(45, 28)
        btn.setCheckable(True)
        btn.setStyleSheet("""
            QPushButton { background-color: #222; color: #888; border: 1px solid #444; border-radius: 3px; font-weight: bold; font-size: 13px; } 
            QPushButton:checked { background-color: #00E5FF; color: #000; border: 1px solid #00E5FF; }
        """)
        btn.clicked.connect(lambda: self.change_timeframe(tf))
        self.btn_group.addButton(btn)
        return btn

    def open_war_room(self):
        if self.current_df is None or self.current_df.empty:
            return
        self.expanded_dialog = ExpandedKLineWindow(
            stock_id=self.current_stock_id,
            df=self.display_df,
            stock_name=self.current_stock_name,
            parent=self
        )
        self.expanded_dialog.show()

    def change_timeframe(self, tf):
        self.timeframe = tf
        if self.raw_df is not None:
            self.overlay.show_loading(f"切換週期: {tf}")
            QApplication.processEvents()
            self.process_data()
            self.redraw_chart()
            self.overlay.hide()

            if self.display_df is not None and not self.display_df.empty:
                last_row = self.display_df.iloc[-1]
                self.update_info_label(last_row, last_row.name)
                self.update_date_indicator()

    def update_date_indicator(self):
        if self.display_df is not None and not self.display_df.empty:
            last_date = self.display_df.index[-1]
            self.lbl_data_date.setText(f"資料日期: {last_date.strftime('%Y/%m/%d')}")
        else:
            self.lbl_data_date.setText("資料日期: --/--")

    def init_chart_structure(self):
        gs = gridspec.GridSpec(2, 1, height_ratios=[3, 1], hspace=0.02)
        self.ax1 = self.fig.add_subplot(gs[0])
        self.ax2 = self.fig.add_subplot(gs[1], sharex=self.ax1)

        for ax in [self.ax1, self.ax2]:
            ax.set_facecolor('#000000')
            ax.tick_params(colors='#888')
            ax.yaxis.tick_right()
            ax.ticklabel_format(style='plain', axis='y')

        self.fig.subplots_adjust(left=0.01, right=0.92, top=0.98, bottom=0.15)

    def load_stock_data(self, stock_id: str, stock_name: str = ""):
        self.current_stock_id = stock_id
        self.current_stock_name = stock_name

        if hasattr(self, 'overlay'):
            self.overlay.setGeometry(0, 0, self.width(), self.height())
            self.overlay.show_loading(f"載入中: {stock_name}")
            QApplication.processEvents()

        try:
            self.quote_worker.quote_updated.disconnect(self.on_realtime_quote)
        except:
            pass

        # 設定監控目標，但如果不按即時，Worker 不會動作，所以不會有資料進來
        self.quote_worker.set_monitoring_stocks([stock_id], source='kline')

        path = Path(f"data/cache/tw/{stock_id}.parquet")
        if not path.exists() and "_" in stock_id:
            raw_id = stock_id.split('_')[0]
            fallback_path = Path(f"data/cache/tw/{raw_id}.parquet")
            if fallback_path.exists():
                path = fallback_path

        data_loaded = False
        if path.exists():
            try:
                # 只讀取 Parquet，這是最乾淨的歷史資料
                df = pd.read_parquet(path)
                df.columns = [c.capitalize() for c in df.columns]
                df.index = pd.to_datetime(df.index)
                self.raw_df = df

                self.process_data()

                if hasattr(self, 'warning_label'):
                    self.warning_label.hide()

                self.update_date_indicator()

                # 🔥 [修正2] 這裡原本有一段嘗試去 quote_worker 拿 cache 的程式碼
                # 請直接刪除，確保 K 線圖只顯示 Parquet 檔案裡的內容
                # 如果使用者沒按即時更新，這裡就不會有今天的資料

                self.redraw_chart()
                QTimer.singleShot(100, lambda: self.canvas.draw())
                data_loaded = True

            except Exception as e:
                print(f"DEBUG: 讀取錯誤 {e}")

        if not data_loaded:
            if self.ax1: self.ax1.clear()
            if self.ax2: self.ax2.clear()
            self.canvas.draw()
            self.lbl_data_date.setText("資料日期: 無資料")

        if hasattr(self, 'overlay'):
            self.overlay.hide()

        if not self.is_closing:
            self.quote_worker.quote_updated.connect(self.on_realtime_quote)

        if self.display_df is not None and not self.display_df.empty:
            last_row = self.display_df.iloc[-1]
            self.update_info_label(last_row, last_row.name)

        if hasattr(self, 'expanded_dialog') and self.expanded_dialog.isVisible():
            self.expanded_dialog.update_stock_data(
                self.current_stock_id,
                self.display_df,
                self.current_stock_name
            )

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

        self._calc_indicators(df)
        self.current_df = df.copy()
        self.display_df = df
        self.scroll_pos = 0

    def _calc_indicators(self, df):
        ma_list = self.MA_CONFIG.get(self.timeframe, [])
        for ma in ma_list:
            df[f'MA{ma}'] = df['Close'].rolling(ma).mean()

        df['PrevClose'] = df['Close'].shift(1)
        if not df.empty:
            df.iloc[0, df.columns.get_loc('PrevClose')] = df.iloc[0]['Open']
        df['Change'] = df['Close'] - df['PrevClose']
        df['PctChange'] = (df['Change'] / df['PrevClose']) * 100

    def on_realtime_quote(self, data_dict):
        if self.is_closing: return
        if self.display_df is None or self.display_df.empty: return
        if self.timeframe != 'D': return

        target_key = self.current_stock_id.split('_')[0]
        if target_key not in data_dict: return

        quote = data_dict[target_key]
        real = quote.get('realtime', {})
        info = quote.get('info', {})

        def safe_float(v):
            if v == '-' or v == '' or v is None: return 0.0
            try:
                return float(v)
            except:
                return 0.0

        try:
            latest = safe_float(real.get('latest_trade_price'))
            close_p = safe_float(real.get('close'))

            trade_price = latest if latest > 0 else close_p
            if trade_price == 0:
                bid = safe_float(real.get('best_bid_price', [0])[0])
                ask = safe_float(real.get('best_ask_price', [0])[0])
                if bid > 0:
                    trade_price = bid
                elif ask > 0:
                    trade_price = ask

            if trade_price == 0: return

            vol = safe_float(real.get('accumulate_trade_volume'))
            open_p = safe_float(real.get('open'))
            high_p = safe_float(real.get('high'))
            low_p = safe_float(real.get('low'))

            if open_p == 0: open_p = trade_price
            if high_p == 0: high_p = trade_price
            if low_p == 0: low_p = trade_price
            high_p = max(high_p, trade_price)
            low_p = min(low_p, trade_price)

            date_str = info.get('date', '')
            if len(date_str) == 8:
                quote_date = datetime.strptime(date_str, "%Y%m%d")
            else:
                quote_date = datetime.now()

            quote_date = quote_date.replace(hour=0, minute=0, second=0, microsecond=0)

            last_idx = self.display_df.index[-1]
            last_date = last_idx.replace(hour=0, minute=0, second=0, microsecond=0)

            need_redraw = False

            if quote_date > last_date:
                prev_close = self.display_df.iloc[-1]['Close']
                change = trade_price - prev_close
                pct = (change / prev_close) * 100 if prev_close != 0 else 0

                new_row = pd.Series({
                    'Open': open_p, 'High': high_p, 'Low': low_p, 'Close': trade_price,
                    'Volume': vol, 'PrevClose': prev_close, 'Change': change, 'PctChange': pct
                }, name=quote_date)

                for ma in self.MA_CONFIG.get('D', []): new_row[f'MA{ma}'] = np.nan

                self.display_df = pd.concat([self.display_df, pd.DataFrame([new_row])])
                need_redraw = True

            elif quote_date == last_date:
                curr_close = self.display_df.at[last_idx, 'Close']
                curr_vol = self.display_df.at[last_idx, 'Volume']
                curr_high = self.display_df.at[last_idx, 'High']
                curr_low = self.display_df.at[last_idx, 'Low']

                if abs(curr_close - trade_price) > 0.001 or abs(curr_vol - vol) > 0.1 or \
                        high_p > curr_high or low_p < curr_low:

                    self.display_df.at[last_idx, 'Open'] = open_p
                    self.display_df.at[last_idx, 'High'] = max(curr_high, high_p)
                    self.display_df.at[last_idx, 'Low'] = min(curr_low, low_p)
                    self.display_df.at[last_idx, 'Close'] = trade_price
                    self.display_df.at[last_idx, 'Volume'] = vol

                    pc = self.display_df.at[last_idx, 'PrevClose']
                    if pc > 0:
                        self.display_df.at[last_idx, 'Change'] = trade_price - pc
                        self.display_df.at[last_idx, 'PctChange'] = ((trade_price - pc) / pc) * 100

                    need_redraw = True

            if need_redraw:
                self._calc_indicators(self.display_df)
                self.redraw_chart()
                if hasattr(self, 'expanded_dialog') and self.expanded_dialog.isVisible():
                    self.expanded_dialog.on_realtime_quote(data_dict)

        except Exception as e:
            print(f"DEBUG: KLine Update Error: {e}")

    def redraw_chart(self):
        if self.is_closing or self.display_df is None: return

        if self.ax1 is None: self.init_chart_structure()

        self.ax1.clear()
        self.ax2.clear()

        total_len = len(self.display_df)
        max_scroll = max(0, total_len - self.visible_candles)
        self.scroll_pos = max(0, min(self.scroll_pos, max_scroll))

        end_idx = total_len - self.scroll_pos
        start_idx = max(0, end_idx - self.visible_candles)

        view_df = self.display_df.iloc[start_idx:end_idx].copy()
        self.current_view_df = view_df

        view_df['Volume'] = pd.to_numeric(view_df['Volume'], errors='coerce').fillna(0)

        if not view_df.empty:
            v_high = view_df['High'].max()
            v_low = view_df['Low'].min()
            padding = (v_high - v_low) * 0.05
            if padding == 0: padding = v_high * 0.01
            ylim_min, ylim_max = v_low - padding, v_high + padding
        else:
            ylim_min, ylim_max = 0, 100

        for ax in [self.ax1, self.ax2]:
            ax.grid(True, color='#222', linestyle=':')
            for spine in ax.spines.values():
                spine.set_edgecolor('#444')
            ax.yaxis.tick_right()
            ax.ticklabel_format(style='plain', axis='y', useOffset=False)

        self.ax1.tick_params(labelbottom=False)

        x = np.arange(len(view_df))
        opens = view_df['Open'].values
        closes = view_df['Close'].values
        highs = view_df['High'].values
        lows = view_df['Low'].values

        up = closes > opens
        down = closes < opens
        flat = closes == opens
        col_up, col_down, col_flat = '#FF3333', '#00FF00', '#FFFFFF'
        width = 0.6

        self.ax1.vlines(x[up], lows[up], highs[up], color=col_up, lw=0.8)
        self.ax1.bar(x[up], closes[up] - opens[up], width, bottom=opens[up], color=col_up)

        self.ax1.vlines(x[down], lows[down], highs[down], color=col_down, lw=0.8)
        self.ax1.bar(x[down], opens[down] - closes[down], width, bottom=closes[down], color=col_down)

        if np.any(flat):
            self.ax1.vlines(x[flat], lows[flat], highs[flat], color=col_flat, lw=0.8)
            self.ax1.hlines(closes[flat], x[flat] - width / 2, x[flat] + width / 2, colors=col_flat, lw=2)

        visible_list = self.DEFAULT_VISIBLE_MA.get(self.timeframe, [])
        ma_list = self.MA_CONFIG.get(self.timeframe, [])

        for ma in ma_list:
            col_name = f'MA{ma}'
            if col_name in view_df.columns:
                if ma in visible_list:
                    color = self.MA_COLORS.get(ma, '#FFFFFF')
                    self.ax1.plot(x, view_df[col_name].values, color=color, lw=1, alpha=0.9, label=f'MA{ma}')

        self.ax1.set_ylim(ylim_min, ylim_max)
        self.ax1.set_xlim(-0.5, len(view_df) - 0.5)

        self.ax2.bar(x[up], view_df['Volume'][up], color=col_up, alpha=0.9)
        self.ax2.bar(x[down], view_df['Volume'][down], color=col_down, alpha=0.9)
        self.ax2.bar(x[flat], view_df['Volume'][flat], color=col_flat, alpha=0.9)

        if not view_df.empty:
            vol_max = view_df['Volume'].max()
            if pd.isna(vol_max) or vol_max <= 0:
                vol_max = 1
            self.ax2.set_ylim(0, vol_max * 1.05)

        self.ax2.set_xlim(-0.5, len(view_df) - 0.5)

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
                date_strs.append(d.strftime('%Y/%m/%d') if last_val != val else d.strftime('%m/%d'))
                last_val = val
            else:
                date_strs.append(d.strftime('%Y-%m'))

        self.ax1.set_xticks(tick_indices)
        self.ax2.set_xticks(tick_indices)
        self.ax2.set_xticklabels(date_strs, rotation=0, fontsize=9, color='#AAA')

        self.cross_v1 = self.ax1.axvline(0, color='white', ls='--', lw=0.8, visible=False)
        self.cross_h1 = self.ax1.axhline(0, color='white', ls='--', lw=0.8, visible=False)
        self.cross_v2 = self.ax2.axvline(0, color='white', ls='--', lw=0.8, visible=False)

        props = dict(boxstyle='square', facecolor='#FF00FF', alpha=0.9, edgecolor='none')
        self.y_label_text = self.ax1.text(1.01, 0, "", transform=self.ax1.get_yaxis_transform(),
                                          color='white', fontsize=10, fontweight='bold',
                                          va='center', ha='left', bbox=props, visible=False)

        self.canvas.draw_idle()

    def on_scroll_zoom(self, event):
        if event.button == 'up':
            self.visible_candles = max(20, self.visible_candles - 10)
        elif event.button == 'down':
            self.visible_candles = min(len(self.display_df), self.visible_candles + 10)
        self.redraw_chart()

    def on_mouse_press(self, event):
        if event.button == 1:
            if event.dblclick:
                self.visible_candles = 80
                self.scroll_pos = 0
                self.redraw_chart()
                return
            self.is_dragging = True
            self.last_mouse_x = event.xdata

    def on_mouse_release(self, event):
        self.is_dragging = False

    def on_mouse_move(self, event):
        if self.is_closing or self.current_view_df is None or self.current_view_df.empty: return
        if self.overlay.isVisible(): return

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

        ma_str = f"Vol:<span style='color:#FFFF00;'>{int(row['Volume']):,}</span>&nbsp;&nbsp;"
        ma_list = self.MA_CONFIG.get(self.timeframe, [])
        visible_list = self.DEFAULT_VISIBLE_MA.get(self.timeframe, [])

        for ma in ma_list:
            if ma in visible_list:
                col_name = f'MA{ma}'
                if col_name in row:
                    color = self.MA_COLORS.get(ma, '#FFF')
                    val = row[col_name]
                    if not pd.isna(val):
                        ma_str += f"<span style='color:{color};'>MA{ma}:{val:.2f}</span>&nbsp;"

        self.info_label.setText(f"{line1}<br>{ma_str}")