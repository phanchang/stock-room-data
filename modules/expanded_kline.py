import sys
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path

from PyQt6.QtCore import Qt, pyqtSignal, QEvent
from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                             QComboBox, QPushButton, QButtonGroup, QWidget,
                             QApplication, QCheckBox, QFrame)
from PyQt6.QtGui import QColor, QPalette, QFont

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
    Advanced War Room (Expanded KLine) - V7.0 (Precision Layout)
    修正:
    1. MA Overlay: 強制定位於主圖(ax1)網格內部左上角。
    2. Sub Overlay: 強制定位於副圖(ax2)網格內部左上角。
    3. 使用 get_position() 精準換算 Qt 像素座標。
    """

    MA_CONFIG = {
        'D': [5, 10, 22, 55, 200],
        'W': [30],
        'M': [3, 6, 12]
    }

    MA_COLORS = {
        5: '#FFFF00', 10: '#FF00FF', 22: '#00FF00', 55: '#FF8800', 200: '#00FFFF',
        30: '#FFAA00',
        3: '#FFFF00', 6: '#FF8800', 12: '#00FFFF'
    }

    def __init__(self, stock_id, df, stock_name="", parent=None):
        super().__init__(parent)

        self.stock_id = stock_id
        self.stock_name = stock_name
        self.display_id = stock_id.split('_')[0]

        self.setWindowTitle(f"進階戰情室 - {self.display_id} {stock_name}")
        self.resize(1400, 900)
        self.setWindowFlags(
            self.windowFlags() | Qt.WindowType.WindowMaximizeButtonHint | Qt.WindowType.WindowMinimizeButtonHint)

        # 全局樣式
        self.setStyleSheet("""
            QDialog { background-color: #121212; color: #E0E0E0; }
            QLabel { font-family: 'Microsoft JhengHei', 'Consolas', sans-serif; }
            QComboBox { 
                background: #252525; color: #E0E0E0; font-size: 11px; font-weight: bold;
                border: 1px solid #444; border-radius: 2px; padding-left: 5px;
            }
            QComboBox::drop-down { border: none; width: 15px; }
            QComboBox QAbstractItemView {
                background: #252525; color: #E0E0E0; selection-background-color: #00E5FF; selection-color: black;
            }
        """)

        self.is_closing = False

        self.df_source = df.copy()
        if not self.df_source.empty:
            self.df_source.index = pd.to_datetime(self.df_source.index)
        self.current_df = self.df_source

        self.current_indicator = "Vix Fix"
        self.current_tf = "D"

        self.visible_candles = 250
        self.scroll_pos = 0
        self.is_dragging = False
        self.last_drag_x = None

        self.ma_checks = {}

        # 1. UI 初始化
        self.init_ui()

        # 2. 建立懸浮控制項
        self.init_overlays()

        # 3. 繪圖結構
        self.plot_chart_structure()

        # 4. 啟動 Worker
        self.quote_worker = QuoteWorker(self)
        self.quote_worker.set_monitoring_stocks([self.stock_id])
        self.quote_worker.quote_updated.connect(self.on_realtime_quote)
        self.quote_worker.start()

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

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self.reposition_overlays()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # --- Top Toolbar ---
        toolbar = QHBoxLayout()
        toolbar.setContentsMargins(15, 8, 15, 8)
        toolbar.setSpacing(15)

        self.info_title = QLabel(f"{self.display_id} {self.stock_name}")
        self.info_title.setStyleSheet("font-size: 22px; font-weight: bold; color: #FFD700;")
        toolbar.addWidget(self.info_title)

        toolbar.addStretch()

        self.btn_group = QButtonGroup(self)
        self.btn_day = self._create_tf_btn("日", "D")
        self.btn_week = self._create_tf_btn("周", "W")
        self.btn_month = self._create_tf_btn("月", "M")
        self.btn_day.setChecked(True)

        toolbar.addWidget(self.btn_day)
        toolbar.addWidget(self.btn_week)
        toolbar.addWidget(self.btn_month)

        layout.addLayout(toolbar)

        # --- Info Bar ---
        self.info_bg = QWidget()
        self.info_bg.setFixedHeight(28)
        self.info_bg.setStyleSheet(
            "background-color: #0F0F0F; border-bottom: 1px solid #333; border-top: 1px solid #333;")
        info_layout = QHBoxLayout(self.info_bg)
        info_layout.setContentsMargins(10, 0, 10, 0)

        self.info_label = QLabel("正在同步數據...")
        self.info_label.setStyleSheet("font-family: 'Consolas', monospace; font-size: 14px; color: #CCC;")
        self.info_label.setTextFormat(Qt.TextFormat.RichText)
        info_layout.addWidget(self.info_label)
        layout.addWidget(self.info_bg)

        # --- Chart Canvas ---
        self.figure = Figure(facecolor='#121212')
        self.canvas = FigureCanvas(self.figure)
        layout.addWidget(self.canvas)

        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)
        self.canvas.mpl_connect('scroll_event', self.on_scroll)
        self.canvas.mpl_connect('button_press_event', self.on_mouse_press)
        self.canvas.mpl_connect('button_release_event', self.on_mouse_release)

    def init_overlays(self):
        """建立懸浮在圖表上的控制項"""

        # --- 1. MA Overlay (主圖左上角) ---
        self.ma_overlay = QFrame(self)
        # 背景設為半透明黑 (rgba 0,0,0, 0.6)，避免文字與 K 線混雜
        self.ma_overlay.setStyleSheet("""
            QFrame { 
                background-color: rgba(0, 0, 0, 180); 
                border-bottom-right-radius: 8px; 
                border: 1px solid #333; 
                border-left: none; border-top: none;
            }
        """)
        ma_layout = QHBoxLayout(self.ma_overlay)
        ma_layout.setContentsMargins(5, 2, 5, 2)
        ma_layout.setSpacing(8)

        for ma in [5, 10, 22, 55, 200]:
            chk = QCheckBox(f"MA{ma}")
            color = self.MA_COLORS.get(ma, '#FFF')
            chk.setStyleSheet(f"""
                QCheckBox {{ color: {color}; font-weight: bold; font-size: 11px; font-family: 'Consolas'; }}
                QCheckBox::indicator {{ width: 10px; height: 10px; background: #222; border: 1px solid #666; }}
                QCheckBox::indicator:checked {{ background: {color}; border: 1px solid {color}; }}
            """)
            chk.setChecked(True)
            chk.stateChanged.connect(self.draw_candles_and_indicators)
            self.ma_checks[ma] = chk
            ma_layout.addWidget(chk)

        # --- 2. Sub Overlay (副圖左上角) ---
        self.sub_overlay = QFrame(self)
        # 同樣半透明背景
        self.sub_overlay.setStyleSheet("""
            QFrame { 
                background-color: rgba(0, 0, 0, 180); 
                border-bottom-right-radius: 0px;
            }
        """)
        sub_layout = QHBoxLayout(self.sub_overlay)
        sub_layout.setContentsMargins(5, 2, 10, 2)
        sub_layout.setSpacing(10)

        # 切換選單
        self.combo = QComboBox()
        self.combo.addItems(["Volume", "Vix Fix", "KD", "MACD", "RSI"])
        self.combo.setCurrentText(self.current_indicator)
        self.combo.setFixedSize(85, 20)
        self.combo.currentTextChanged.connect(self.on_indicator_changed)
        sub_layout.addWidget(self.combo)

        # 數值 Label
        self.sub_val_label = QLabel("")
        self.sub_val_label.setStyleSheet(
            "font-family: 'Consolas', monospace; font-size: 12px; font-weight: bold; background: transparent;")
        self.sub_val_label.setTextFormat(Qt.TextFormat.RichText)
        self.sub_val_label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
        sub_layout.addWidget(self.sub_val_label)

        sub_layout.addStretch()

        self.ma_overlay.show()
        self.sub_overlay.show()

    def reposition_overlays(self):
        """
        精準定位懸浮控制項 - 修正版
        加入 Canvas 在視窗中的偏移量 (x, y)，解決蓋住標題與錯位問題。
        """
        if not hasattr(self, 'ax1') or not hasattr(self, 'ax2'): return

        try:
            # 1. 取得 Canvas 在父視窗中的區域 (包含 x, y 偏移量)
            canvas_rect = self.canvas.geometry()

            # Canvas 的左上角在視窗中的座標
            canvas_left = canvas_rect.x()
            canvas_top = canvas_rect.y()
            canvas_w = canvas_rect.width()
            canvas_h = canvas_rect.height()

            # --- 1. MA Panel -> 定位在 ax1 (主圖) 的 Top-Left 內部 ---
            bbox1 = self.ax1.get_position()

            # 計算相對於 Canvas 的像素座標
            # (1 - bbox1.y1) 是因為 Matplotlib 原點在左下，Qt 在左上
            rel_x1 = int(bbox1.x0 * canvas_w)
            rel_y1 = int((1 - bbox1.y1) * canvas_h)

            # 加上 Canvas 本身的偏移量，才是視窗中的正確位置
            final_x1 = canvas_left + rel_x1 + 4  # +4 微調內縮
            final_y1 = canvas_top + rel_y1 + 4  # +4 微調內縮

            self.ma_overlay.move(final_x1, final_y1)
            self.ma_overlay.adjustSize()
            self.ma_overlay.raise_()  # 確保浮在最上層

            # --- 2. Sub Panel -> 定位在 ax2 (副圖) 的 Top-Left 內部 ---
            bbox2 = self.ax2.get_position()

            rel_x2 = int(bbox2.x0 * canvas_w)
            rel_y2 = int((1 - bbox2.y1) * canvas_h)

            final_x2 = canvas_left + rel_x2
            final_y2 = canvas_top + rel_y2 + 2  # 微調

            self.sub_overlay.move(final_x2, final_y2)

            # 設定寬度與圖表一致，避免太寬遮擋或太窄切字
            chart_width_px = int(bbox2.width * canvas_w)
            self.sub_overlay.setFixedWidth(chart_width_px)

            self.sub_overlay.raise_()

        except Exception as e:
            print(f"Overlay error: {e}")

    def _create_tf_btn(self, text, code):
        btn = QPushButton(text)
        btn.setCheckable(True)
        btn.setFixedSize(45, 28)
        btn.setStyleSheet("""
            QPushButton { background: #222; color: #AAA; border: 1px solid #444; border-radius: 3px; font-size: 13px; font-family: 'Microsoft JhengHei'; }
            QPushButton:checked { background: #00E5FF; color: black; font-weight: bold; border: 1px solid #00E5FF; }
            QPushButton:hover { border: 1px solid #777; color: #FFF; }
        """)
        btn.clicked.connect(lambda: self.update_data_frequency(code))
        self.btn_group.addButton(btn)
        return btn

    def update_stock_data(self, stock_id, df, stock_name=""):
        self.stock_id = stock_id
        self.stock_name = stock_name
        self.display_id = stock_id.split('_')[0]
        self.info_title.setText(f"{self.display_id} {stock_name}")
        self.setWindowTitle(f"進階戰情室 - {self.display_id} {stock_name}")

        self.df_source = df.copy()
        if not self.df_source.empty:
            self.df_source.index = pd.to_datetime(self.df_source.index)

        self.update_data_frequency(self.current_tf)
        self.quote_worker.set_monitoring_stocks([stock_id])

    def on_indicator_changed(self, name):
        self.current_indicator = name
        self.draw_candles_and_indicators()
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
        info = quote.get('info', {})

        # 🔥 [緊急修復] 加入 safe_float
        def safe_float(v):
            if v == '-' or v == '' or v is None: return 0.0
            try:
                return float(v)
            except:
                return 0.0

        try:
            # 🔥 使用 safe_float
            latest = safe_float(real.get('latest_trade_price'))
            close = safe_float(real.get('close'))
            trade_price = latest if latest > 0 else close
            if trade_price == 0: return

            open_p = safe_float(real.get('open'))
            high_p = safe_float(real.get('high'))
            low_p = safe_float(real.get('low'))
            vol = safe_float(real.get('accumulate_trade_volume'))

            if open_p == 0: open_p = trade_price
            if high_p == 0: high_p = trade_price
            if low_p == 0: low_p = trade_price

            high_p = max(high_p, trade_price, open_p)
            low_p = min(low_p, trade_price, open_p)

            # (以下邏輯保持不變，直接複製原本的日期判斷邏輯即可)
            date_str = info.get('date', '')
            if date_str and len(date_str) == 8:
                try:
                    today_date = datetime.strptime(date_str, "%Y%m%d")
                except:
                    today_date = datetime.now()
            else:
                now = datetime.now()
                if now.weekday() > 4:
                    today_date = self.current_df.index[-1]
                else:
                    today_date = now
            today_date = today_date.replace(hour=0, minute=0, second=0, microsecond=0)
            last_idx = self.current_df.index[-1]
            last_date = last_idx.replace(hour=0, minute=0, second=0, microsecond=0)

            need_redraw = False
            if today_date > last_date:
                prev_close = self.current_df.iloc[-1]['Close']
                change = trade_price - prev_close
                pct_change = (change / prev_close) * 100 if prev_close != 0 else 0
                new_data = {'Open': open_p, 'High': high_p, 'Low': low_p, 'Close': trade_price, 'Volume': vol,
                            'PrevClose': prev_close, 'Change': change, 'PctChange': pct_change}
                for ma in self.MA_CONFIG.get(self.current_tf, []): new_data[f'MA{ma}'] = np.nan
                new_row = pd.Series(new_data, name=today_date)
                self.current_df = pd.concat([self.current_df, pd.DataFrame([new_row])])
                need_redraw = True
            elif today_date == last_date:
                current_vol = self.current_df.at[last_idx, 'Volume']
                current_close = self.current_df.at[last_idx, 'Close']

                # 簡單判斷變動
                if abs(current_close - trade_price) > 0.0001 or abs(current_vol - vol) > 0.0001:
                    self.current_df.at[last_idx, 'Open'] = open_p
                    self.current_df.at[last_idx, 'High'] = max(self.current_df.at[last_idx, 'High'], high_p)
                    self.current_df.at[last_idx, 'Low'] = min(self.current_df.at[last_idx, 'Low'], low_p)
                    self.current_df.at[last_idx, 'Close'] = trade_price
                    self.current_df.at[last_idx, 'Volume'] = vol
                    pc = self.current_df.at[last_idx, 'PrevClose']
                    if pc != 0:
                        self.current_df.at[last_idx, 'Change'] = trade_price - pc
                        self.current_df.at[last_idx, 'PctChange'] = ((trade_price - pc) / pc) * 100
                    need_redraw = True

            if need_redraw:
                self._calculate_ma()
                self.draw_candles_and_indicators()
                self._update_info_label(len(self.current_df) - 1)
        except Exception as e:
            pass

    def update_data_frequency(self, tf_code):
        self.current_tf = tf_code
        if tf_code == 'D':
            self.current_df = self.df_source.copy()
            self.ma_overlay.show()
        else:
            rule = 'W-FRI' if tf_code == 'W' else 'ME'
            logic = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
            self.current_df = self.df_source.resample(rule).agg(logic).dropna()
            self.ma_overlay.hide()

        self.current_df['PrevClose'] = self.current_df['Close'].shift(1)
        if not self.current_df.empty:
            self.current_df.iloc[0, self.current_df.columns.get_loc('PrevClose')] = self.current_df.iloc[0]['Open']

        self.current_df['Change'] = self.current_df['Close'] - self.current_df['PrevClose']
        self.current_df['PctChange'] = (self.current_df['Change'] / self.current_df['PrevClose']) * 100

        self._calculate_ma()

        total = len(self.current_df)
        self.visible_candles = min(250, total)
        self.scroll_pos = 0

        self.draw_candles_and_indicators()
        if total > 0: self._update_info_label(total - 1)

    def _calculate_ma(self):
        ma_list = self.MA_CONFIG.get(self.current_tf, [])
        for ma in ma_list:
            self.current_df[f'MA{ma}'] = self.current_df['Close'].rolling(ma).mean()

    def plot_chart_structure(self):
        self.figure.clear()

        # GridSpec 設定
        gs = gridspec.GridSpec(2, 1, height_ratios=[3, 1])
        # hspace 設小一點 (0.05)，讓主副圖靠緊，因為控制項已經移到副圖內部了
        gs.update(left=0.04, right=0.96, top=0.98, bottom=0.04, hspace=0.05)

        self.ax1 = self.figure.add_subplot(gs[0])
        self.ax2 = self.figure.add_subplot(gs[1], sharex=self.ax1)

        for ax in [self.ax1, self.ax2]:
            ax.set_facecolor('#121212')
            ax.grid(True, color='#2A2A2A', ls='-', lw=0.8)
            ax.tick_params(axis='y', colors='#888', labelsize=10, labelright=True, labelleft=False)
            ax.tick_params(axis='x', colors='#888', labelsize=10)
            ax.yaxis.tick_right()
            ax.ticklabel_format(style='plain', axis='y')
            for spine in ax.spines.values():
                spine.set_edgecolor('#333')

        self.ax1.tick_params(labelbottom=False)

        # 十字游標線
        self.vline1 = self.ax1.axvline(0, color='#666', ls='--', lw=0.8, visible=False)
        self.vline2 = self.ax2.axvline(0, color='#666', ls='--', lw=0.8, visible=False)
        self.hline1 = self.ax1.axhline(0, color='#666', ls='--', lw=0.8, visible=False)

        # 價格標籤
        props = dict(boxstyle='square,pad=0.2', facecolor='#00E5FF', alpha=0.9, edgecolor='none')
        self.y_label_text = self.ax1.text(1.01, 0, "", transform=self.ax1.get_yaxis_transform(),
                                          color='black', fontsize=10, fontweight='bold',
                                          va='center', ha='left', bbox=props, visible=False)

        self.canvas.draw_idle()
        self.reposition_overlays()

    def draw_candles_and_indicators(self):
        self.ax1.clear()
        self.ax2.clear()
        self.plot_chart_structure()  # 重設 grid/ticks

        total_len = len(self.current_df)
        max_scroll = max(0, total_len - self.visible_candles)
        self.scroll_pos = max(0, min(self.scroll_pos, max_scroll))

        end_idx = total_len - int(self.scroll_pos)
        start_idx = max(0, end_idx - int(self.visible_candles))

        view_df = self.current_df.iloc[start_idx:end_idx].copy()
        if view_df.empty: return

        x = np.arange(len(view_df))

        # --- 1. 主圖 K線 ---
        up = view_df['Close'] >= view_df['Open']
        down = view_df['Close'] < view_df['Open']

        self.ax1.bar(x[up], view_df['Close'][up] - view_df['Open'][up], 0.6, bottom=view_df['Open'][up],
                     color='#FF3333', edgecolor='#FF3333', linewidth=0.8)
        self.ax1.vlines(x[up], view_df['Low'][up], view_df['High'][up], color='#FF3333', lw=1)

        self.ax1.bar(x[down], view_df['Open'][down] - view_df['Close'][down], 0.6, bottom=view_df['Close'][down],
                     color='#00FF00', edgecolor='#00FF00', linewidth=0.8)
        self.ax1.vlines(x[down], view_df['Low'][down], view_df['High'][down], color='#00FF00', lw=1)

        # --- 2. 主圖 MA ---
        ma_list = self.MA_CONFIG.get(self.current_tf, [])
        for ma in ma_list:
            is_visible = True
            if self.current_tf == 'D' and ma in self.ma_checks:
                is_visible = self.ma_checks[ma].isChecked()

            if is_visible:
                col_name = f'MA{ma}'
                if col_name in view_df.columns:
                    color = self.MA_COLORS.get(ma, '#FFFFFF')
                    self.ax1.plot(x, view_df[col_name].values, color=color, lw=1.2, alpha=0.9, label=f'MA{ma}')

        v_high = view_df['High'].max()
        v_low = view_df['Low'].min()
        if pd.notna(v_high) and pd.notna(v_low):
            pad = (v_high - v_low) * 0.05
            self.ax1.set_ylim(v_low - pad, v_high + pad)
        self.ax1.set_xlim(-0.5, len(view_df) - 0.5)

        # --- 3. 副圖 指標繪製 ---
        name = self.current_indicator

        if name == "Vix Fix":
            full_wvf = Indicators.cm_williams_vix_fix(self.current_df, period=22)
            wvf_view = full_wvf.iloc[start_idx:end_idx]

            full_std = full_wvf.rolling(20).std()
            full_ma = full_wvf.rolling(20).mean()
            full_upper = full_ma + (2.0 * full_std)
            full_rh = full_wvf.rolling(50).max() * 0.85

            v_upper = full_upper.iloc[start_idx:end_idx]
            v_rh = full_rh.iloc[start_idx:end_idx]

            is_green = (wvf_view >= v_upper) | (wvf_view >= v_rh)
            bar_colors = np.where(is_green, '#00FF00', '#444')

            self.ax2.bar(x, wvf_view, color=bar_colors, width=0.6, edgecolor='#121212', linewidth=0.5)
            self.ax2.plot(x, v_upper.values, color='#00FFFF', lw=1, alpha=0.6)
            self.ax2.plot(x, v_rh.values, color='#FFA500', lw=1, alpha=0.9)

        elif name == "KD":
            kd = Indicators.kd(self.current_df)
            k_view = kd['K'].iloc[start_idx:end_idx]
            d_view = kd['D'].iloc[start_idx:end_idx]

            self.ax2.plot(x, k_view.values, color='#FFA500', lw=1.2)  # K: Orange
            self.ax2.plot(x, d_view.values, color='#00FFFF', lw=1.2)  # D: Cyan
            self.ax2.axhline(80, color='#555', ls='--', lw=0.5)
            self.ax2.axhline(20, color='#555', ls='--', lw=0.5)
            self.ax2.set_ylim(0, 100)

        elif name == "Volume":
            colors = ['#FF3333' if c >= o else '#00FF00' for c, o in zip(view_df['Close'], view_df['Open'])]
            self.ax2.bar(x, view_df['Volume'], color=colors, width=0.6, edgecolor='#121212', linewidth=0.5)

        elif name == "MACD":
            exp12 = self.current_df['Close'].ewm(span=12, adjust=False).mean()
            exp26 = self.current_df['Close'].ewm(span=26, adjust=False).mean()
            macd = exp12 - exp26
            signal = macd.ewm(span=9, adjust=False).mean()
            hist = macd - signal

            macd_v = macd.iloc[start_idx:end_idx]
            sig_v = signal.iloc[start_idx:end_idx]
            hist_v = hist.iloc[start_idx:end_idx]

            self.ax2.plot(x, macd_v.values, color='#00FFFF', lw=1)  # DIF
            self.ax2.plot(x, sig_v.values, color='#FFA500', lw=1)  # DEA

            hist_colors = np.where(hist_v >= 0, '#FF3333', '#00FF00')
            self.ax2.bar(x, hist_v.values, color=hist_colors, width=0.6, alpha=0.8)
            self.ax2.axhline(0, color='#555', lw=0.5)

        elif name == "RSI":
            delta = self.current_df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))

            rsi_v = rsi.iloc[start_idx:end_idx]
            self.ax2.plot(x, rsi_v.values, color='#00E5FF', lw=1.2)
            self.ax2.axhline(70, color='#FF3333', ls='--', lw=0.5)
            self.ax2.axhline(30, color='#00FF00', ls='--', lw=0.5)
            self.ax2.set_ylim(0, 100)

        # X軸標籤
        date_strs = []
        tick_indices = []
        dates = view_df.index
        last_val = None
        step = max(1, len(view_df) // 8)
        for i in range(0, len(dates), step):
            d = dates[i]
            tick_indices.append(i)
            if self.current_tf == 'D':
                val = d.year
                date_strs.append(d.strftime('%Y/%m/%d') if last_val != val else d.strftime('%m/%d'))
                last_val = val
            else:
                date_strs.append(d.strftime('%Y-%m'))

        self.ax1.set_xticks(tick_indices)
        self.ax2.set_xticks(tick_indices)
        self.ax2.set_xticklabels(date_strs, rotation=0, fontsize=9, color='#AAA')
        self.ax2.set_xlim(-0.5, len(view_df) - 0.5)

        # 初始化數值顯示
        if len(view_df) > 0:
            self._update_sub_chart_values(total_len - 1)
            self._update_info_label(total_len - 1)

        self.canvas.draw_idle()

    def _update_info_label(self, idx):
        if idx < 0 or idx >= len(self.current_df): return
        row = self.current_df.iloc[idx]
        dt = self.current_df.index[idx].strftime('%Y-%m-%d')
        p = row['PrevClose']

        def get_c(v, b):
            return "#FF3333" if v > b else "#00FF00" if v < b else "#FFFFFF"

        base_html = (
            f"<span style='color:#DDD; font-weight:bold;'>{dt}</span> &nbsp; "
            f"O:<span style='color:{get_c(row['Open'], p)};'>{row['Open']:.2f}</span> "
            f"H:<span style='color:{get_c(row['High'], p)};'>{row['High']:.2f}</span> "
            f"L:<span style='color:{get_c(row['Low'], p)};'>{row['Low']:.2f}</span> "
            f"C:<span style='color:{get_c(row['Close'], p)};'>{row['Close']:.2f}</span> "
            f"<span style='color:{get_c(row['Close'], p)};'>({row['PctChange']:+.2f}%)</span>"
        )
        self.info_label.setText(base_html)

    def _update_sub_chart_values(self, idx):
        """更新副圖標題列的數值顯示"""
        if idx < 0 or idx >= len(self.current_df): return

        name = self.current_indicator
        html_text = ""

        def span(text, color):
            return f"<span style='color:{color}; font-weight:bold;'>{text}</span>"

        if name == "Volume":
            vol = self.current_df['Volume'].iloc[idx]
            close = self.current_df['Close'].iloc[idx]
            open_p = self.current_df['Open'].iloc[idx]
            c = "#FF3333" if close >= open_p else "#00FF00"
            html_text = span(f"Vol: {int(vol):,}", c)

        elif name == "KD":
            kd = Indicators.kd(self.current_df)
            k = kd['K'].iloc[idx]
            d = kd['D'].iloc[idx]
            html_text = f"{span(f'K: {k:.2f}', '#FFA500')} &nbsp; {span(f'D: {d:.2f}', '#00FFFF')}"

        elif name == "Vix Fix":
            wvf = Indicators.cm_williams_vix_fix(self.current_df, period=22)
            val = wvf.iloc[idx]
            full_std = wvf.rolling(20).std()
            full_ma = wvf.rolling(20).mean()
            upper = (full_ma + (2.0 * full_std)).iloc[idx]
            rh = (wvf.rolling(50).max() * 0.85).iloc[idx]
            is_active = val >= upper or val >= rh
            color = "#00FF00" if is_active else "#999"
            html_text = f"{span(f'Vix: {val:.2f}', color)} &nbsp; {span(f'Upper: {upper:.2f}', '#00FFFF')} &nbsp; {span(f'RH: {rh:.2f}', '#FFA500')}"

        elif name == "MACD":
            exp12 = self.current_df['Close'].ewm(span=12, adjust=False).mean()
            exp26 = self.current_df['Close'].ewm(span=26, adjust=False).mean()
            macd = exp12 - exp26
            signal = macd.ewm(span=9, adjust=False).mean()
            hist = macd - signal
            v_macd = macd.iloc[idx]
            v_sig = signal.iloc[idx]
            v_hist = hist.iloc[idx]
            c_hist = "#FF3333" if v_hist >= 0 else "#00FF00"
            html_text = f"{span(f'DIF: {v_macd:.2f}', '#00FFFF')} &nbsp; {span(f'DEA: {v_sig:.2f}', '#FFA500')} &nbsp; {span(f'MACD: {v_hist:.2f}', c_hist)}"

        elif name == "RSI":
            delta = self.current_df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            val = rsi.iloc[idx]
            html_text = span(f"RSI: {val:.2f}", "#00E5FF")

        self.sub_val_label.setText(f"&nbsp;&nbsp;{html_text}")

    def on_mouse_move(self, event):
        if not event.inaxes:
            self.vline1.set_visible(False)
            self.vline2.set_visible(False)
            self.hline1.set_visible(False)
            self.y_label_text.set_visible(False)
            self.canvas.draw_idle()
            return

        if self.is_dragging and event.xdata is not None and self.last_drag_x is not None:
            dx = int(self.last_drag_x - event.xdata)
            if abs(dx) > 0:
                max_scroll = len(self.current_df) - self.visible_candles
                self.scroll_pos = max(0, min(max_scroll, self.scroll_pos + dx))
                self.draw_candles_and_indicators()
                return

        try:
            x_idx_view = int(round(event.xdata))
            total_len = len(self.current_df)
            end_idx = total_len - int(self.scroll_pos)
            start_idx = max(0, end_idx - int(self.visible_candles))
            real_idx = start_idx + x_idx_view

            if 0 <= real_idx < total_len:
                self.vline1.set_xdata([x_idx_view])
                self.vline1.set_visible(True)
                self.vline2.set_xdata([x_idx_view])
                self.vline2.set_visible(True)

                if event.inaxes == self.ax1:
                    price = event.ydata
                    self.hline1.set_ydata([price])
                    self.hline1.set_visible(True)
                    self.y_label_text.set_text(f"{price:.2f}")
                    self.y_label_text.set_y(price)
                    self.y_label_text.set_visible(True)
                else:
                    self.hline1.set_visible(False)
                    self.y_label_text.set_visible(False)

                self._update_info_label(real_idx)
                self._update_sub_chart_values(real_idx)
                self.canvas.draw_idle()
        except:
            pass

    def on_scroll(self, event):
        if not event.inaxes: return
        if event.button == 'up':
            self.visible_candles = max(20, self.visible_candles - 10)
        elif event.button == 'down':
            self.visible_candles = min(len(self.current_df), self.visible_candles + 10)
        self.draw_candles_and_indicators()

    def on_mouse_press(self, event):
        if event.button == 1:
            if event.dblclick:
                self.visible_candles = 250
                self.scroll_pos = 0
                self.draw_candles_and_indicators()
                return
            self.is_dragging = True
            self.last_drag_x = event.xdata

    def on_mouse_release(self, event):
        self.is_dragging = False