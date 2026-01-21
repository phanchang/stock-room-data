import sys
import pandas as pd
import numpy as np
from io import StringIO
import requests
from pathlib import Path
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QComboBox,
                             QLabel, QTableWidget, QTableWidgetItem, QFrame,
                             QSplitter, QApplication, QHeaderView)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QColor, QFont

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.dates as mdates

# 設定 matplotlib 風格
plt.style.use('dark_background')
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False


# --- 數據抓取線程 ---
class ETFDataWorker(QThread):
    data_fetched = pyqtSignal(pd.DataFrame, str)

    def __init__(self, etf_id, provider):
        super().__init__()
        self.etf_id = etf_id
        self.provider = provider

    def run(self):
        url = f"https://raw.githubusercontent.com/phanchang/stock-room-data/main/data/clean/{self.provider}/{self.etf_id}.csv"
        try:
            print(f"🚀 [ETF] 下載中: {url}")
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                csv_data = StringIO(response.text)
                df = pd.read_csv(csv_data)

                df.columns = [c.lower().strip() for c in df.columns]

                rename_map = {}
                if 'stock_code' in df.columns: rename_map['stock_code'] = 'stock_id'
                if 'code' in df.columns: rename_map['code'] = 'stock_id'
                if 'stock_name' in df.columns: rename_map['stock_name'] = 'name'
                if rename_map:
                    df.rename(columns=rename_map, inplace=True)

                for col in ['shares', 'weight']:
                    if col in df.columns and df[col].dtype == 'object':
                        df[col] = df[col].str.replace(',', '').str.replace('%', '')
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                if 'stock_id' in df.columns:
                    df['stock_id'] = df['stock_id'].astype(str)

                self.data_fetched.emit(df, self.etf_id)
            else:
                self.data_fetched.emit(pd.DataFrame(), self.etf_id)
        except Exception as e:
            print(f"❌ [ETF] 連線錯誤: {e}")
            self.data_fetched.emit(pd.DataFrame(), self.etf_id)


# --- 主動式 ETF 模組 ---
class ActiveETFModule(QWidget):
    stock_clicked_signal = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.current_df = None
        self.bar_data = None
        self.line_data = None

        self.mapping = {
            "00981A": ("ezmoney", "統一-00981A (統一台股增長)"),
            "00991A": ("fhtrust", "復華-00991A (復華未來50)")
        }
        self.init_ui()

    def init_ui(self):
        self.setStyleSheet("background-color: #0E0E0E; color: #E0E0E0;")

        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(5, 5, 5, 5)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        # --- 左側面板 ---
        left_panel = QFrame()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)

        title = QLabel("🚀 主動式基金戰情")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #00E5FF; margin-bottom: 5px;")

        self.combo = QComboBox()
        items = [v[1] for v in self.mapping.values()]
        self.combo.addItems(items)
        self.combo.setStyleSheet("""
            QComboBox { background: #1A1A1A; color: #FFF; border: 1px solid #333; padding: 5px; font-size: 14px; }
            QComboBox::drop-down { border: none; }
        """)
        self.combo.currentIndexChanged.connect(self.on_combo_change)

        # 🟢 改用 QTableWidget 實現完美對齊
        self.stock_table = QTableWidget()
        self.stock_table.setColumnCount(3)
        self.stock_table.setHorizontalHeaderLabels(["代號", "名稱", "權重"])
        self.stock_table.verticalHeader().setVisible(False)
        self.stock_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.stock_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self.stock_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)

        # 表格樣式：無格線、黑底
        self.stock_table.setStyleSheet("""
            QTableWidget { 
                background: #121212; border: 1px solid #333; gridline-color: #222; font-size: 14px; 
            }
            QTableWidget::item { padding: 5px; border-bottom: 1px solid #222; }
            QTableWidget::item:selected { background: #2A2A2A; color: #00E5FF; }
            QHeaderView::section { background: #1A1A1A; color: #888; border: none; padding: 4px; font-weight: bold; }
        """)

        # 欄位寬度調整
        header = self.stock_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)  # 代號
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)  # 名稱自動延伸
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)  # 權重

        self.stock_table.cellClicked.connect(self.on_table_clicked)

        left_layout.addWidget(title)
        left_layout.addWidget(self.combo)
        left_layout.addWidget(QLabel("🔥 持股權重排行 (Top 10)"))
        left_layout.addWidget(self.stock_table)

        # --- 右側面板 ---
        right_panel = QFrame()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(10, 0, 0, 0)

        self.info_label = QLabel(" 💡 移動滑鼠至圖表查看數據")
        self.info_label.setFixedHeight(30)
        self.info_label.setStyleSheet("background: #050505; color: #888; padding: 5px; font-family: Consolas;")
        self.info_label.setTextFormat(Qt.TextFormat.RichText)

        # 圖表 1: 特殊變化 (Bar)
        self.fig_change = Figure(facecolor='#0E0E0E')
        self.canvas_change = FigureCanvas(self.fig_change)
        # 綁定 Hover
        self.canvas_change.mpl_connect('motion_notify_event', self.on_bar_hover)

        # 圖表 2: 趨勢圖 (Line + Thin Curve)
        self.fig_trend = Figure(facecolor='#0E0E0E')
        self.canvas_trend = FigureCanvas(self.fig_trend)
        self.canvas_trend.mpl_connect('motion_notify_event', self.on_line_hover)

        right_layout.addWidget(self.info_label)
        right_layout.addWidget(self.canvas_change, stretch=4)
        right_layout.addWidget(self.canvas_trend, stretch=6)

        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 7)

        main_layout.addWidget(splitter)

        self.on_combo_change(0)

    def on_combo_change(self, index):
        keys = list(self.mapping.keys())
        if index < len(keys):
            etf_id = keys[index]
            provider = self.mapping[etf_id][0]
            self.load_data(etf_id, provider)

    def load_data(self, etf_id, provider):
        self.stock_table.setRowCount(0)
        self.fig_change.clear()
        self.canvas_change.draw()
        self.fig_trend.clear()
        self.canvas_trend.draw()

        self.worker = ETFDataWorker(etf_id, provider)
        self.worker.data_fetched.connect(self.process_data)
        self.worker.start()

    def process_data(self, df, etf_id):
        if df.empty: return

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])

        self.current_df = df
        dates = sorted(df['date'].unique())
        if len(dates) < 1: return
        latest_date = dates[-1]

        # 🟢 更新 Table (Top 10 Highlight)
        latest_data = df[df['date'] == latest_date].sort_values('weight', ascending=False)

        self.stock_table.setRowCount(len(latest_data))
        for idx, row in enumerate(latest_data.itertuples()):
            sid = str(row.stock_id)
            name = str(row.name)
            weight = row.weight

            # 建立表格項目
            item_id = QTableWidgetItem(sid)
            item_name = QTableWidgetItem(name)
            item_weight = QTableWidgetItem(f"{weight}%")

            # 對齊
            item_id.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            item_name.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            item_weight.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

            # Top 10 Highlight
            color = QColor("#FFD700") if idx < 10 else QColor("#AAAAAA")
            font = QFont()
            if idx < 10: font.setBold(True)

            for item in [item_id, item_name, item_weight]:
                item.setForeground(color)
                item.setFont(font)
                self.stock_table.setItem(idx, [item_id, item_name, item_weight].index(item), item)

        # 繪製變化圖
        if len(dates) >= 2:
            prev_date = dates[-2]
            prev_data = df[df['date'] == prev_date]

            merged = pd.merge(latest_data, prev_data, on=['stock_id', 'name'], suffixes=('_now', '_prev'),
                              how='outer').fillna(0)
            merged['share_diff'] = merged['shares_now'] - merged['shares_prev']
            merged['pct_change'] = merged.apply(
                lambda r: (r['share_diff'] / r['shares_prev'] * 100) if r['shares_prev'] > 0 else 0, axis=1)

            def classify_action(row):
                yesterday = row['shares_prev']
                today = row['shares_now']
                pct = row['pct_change']
                if yesterday == 0 and today > 0:
                    return '★新買入'
                elif yesterday > 0 and today == 0:
                    return '●清空'
                elif pct >= 50:
                    return '▲大增'
                elif pct <= -50:
                    return '▼大減'
                elif 10 <= pct < 50:
                    return '△增持'
                elif -50 < pct <= -10:
                    return '▽減持'
                else:
                    return None

            merged['action_type'] = merged.apply(classify_action, axis=1)
            filtered_df = merged.dropna(subset=['action_type']).copy()
            filtered_df = filtered_df.sort_values('pct_change', ascending=True)

            self.plot_changes(filtered_df)

            if not latest_data.empty:
                first_id = str(latest_data.iloc[0]['stock_id'])
                first_name = str(latest_data.iloc[0]['name'])
                self.plot_trend(first_id, first_name)

    def plot_changes(self, df):
        self.fig_change.clear()

        if df.empty:
            ax = self.fig_change.add_subplot(111)
            ax.text(0.5, 0.5, "今日無重大持股異動", ha='center', va='center', color='#555', fontsize=16)
            ax.axis('off')
            self.canvas_change.draw()
            return

        ax = self.fig_change.add_subplot(111)
        self.bar_data = df.reset_index(drop=True)
        colors = ['#FF3333' if x >= 0 else '#00FF00' for x in self.bar_data['share_diff']]
        y_pos = np.arange(len(self.bar_data))

        # 🟢 畫 Bar (不顯示文字了)
        ax.barh(y_pos, self.bar_data['pct_change'], color=colors, align='center')

        ax.set_yticks(y_pos)
        ax.set_yticklabels(self.bar_data['name'], fontsize=11, fontweight='bold', color='#DDD')
        ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=100))

        ax.set_title("🔥 重大持股異動戰報", color='white', fontsize=14, fontweight='bold', loc='left', pad=10)
        ax.tick_params(colors='#AAA', labelsize=10)
        ax.grid(axis='x', color='#333', linestyle=':')

        for spine in ax.spines.values():
            spine.set_visible(False)

        self.canvas_change.draw()

    def on_table_clicked(self, row, col):
        # 從表格獲取代號與名稱
        sid = self.stock_table.item(row, 0).text()
        name = self.stock_table.item(row, 1).text()
        self.plot_trend(sid, name)
        self.stock_clicked_signal.emit(f"{sid}_TW")

    def on_stock_clicked(self, item):  # 保留相容性
        pass

    def plot_trend(self, stock_id, stock_name):
        if self.current_df is None: return

        trend_data = self.current_df[self.current_df['stock_id'] == str(stock_id)].sort_values('date')

        price_data = pd.DataFrame()
        price_path = Path(f"data/cache/tw/{stock_id}_TW.parquet")
        if price_path.exists():
            price_df = pd.read_parquet(price_path)
            price_df.columns = [c.capitalize() for c in price_df.columns]
            if not trend_data.empty:
                min_date = trend_data['date'].min()
                price_data = price_df[price_df.index >= min_date].copy()

        self.fig_trend.clear()
        ax1 = self.fig_trend.add_subplot(111)
        ax2 = ax1.twinx()
        ax3 = ax1.twinx()

        ax3.spines['right'].set_position(('outward', 60))

        ax1.set_zorder(10)
        ax2.set_zorder(11)
        ax3.set_zorder(1)
        ax1.patch.set_visible(False)
        ax2.patch.set_visible(False)

        self.line_data = {
            'etf': trend_data,
            'price': price_data,
            'ax1': ax1,
            'ax2': ax2,
            'ax3': ax3
        }

        l3, = ax3.plot(trend_data['date'], trend_data['shares'], color='#FFFFFF', linewidth=0.8, alpha=0.6,
                       linestyle='-', label='庫存股數(細線)')
        l1, = ax1.plot(trend_data['date'], trend_data['weight'], color='#00E5FF', linewidth=2, marker='o', markersize=4,
                       label='持股權重')
        ax1.fill_between(trend_data['date'], trend_data['weight'], color='#00E5FF', alpha=0.1)

        if not trend_data.empty:
            min_w = trend_data['weight'].min()
            max_w = trend_data['weight'].max()
            margin = (max_w - min_w) * 0.1 if max_w != min_w else 0.5
            ax1.set_ylim(max(0, min_w - margin), max_w + margin)

        l2 = None
        if not price_data.empty:
            l2, = ax2.plot(price_data.index, price_data['Close'], color='#FFD700', linewidth=1.5, linestyle='--',
                           alpha=0.9, label='股價(右)')
        else:
            ax2.text(0.5, 0.5, "無本地股價資料", transform=ax2.transAxes, color='#555', ha='center', va='center')

        ax1.yaxis.set_major_formatter(mtick.PercentFormatter())
        ax1.set_ylabel("持股權重 %", color='#00E5FF')
        ax2.set_ylabel(f"{stock_name} 股價", color='#FFD700')
        ax3.set_yticks([])
        ax3.spines['right'].set_visible(False)

        ax1.set_title(f"📈 {stock_id} {stock_name} - 權重、股價與庫存趨勢", color='white', fontsize=12, fontweight='bold',
                      loc='left')
        ax1.tick_params(axis='y', colors='#00E5FF')
        ax1.tick_params(axis='x', colors='#AAA')
        ax2.tick_params(axis='y', colors='#FFD700')
        ax1.grid(True, color='#222', linestyle=':')

        for ax in [ax1, ax2]:
            for spine in ax.spines.values():
                spine.set_visible(False)

        lines = [l1] + ([l2] if l2 else []) + [l3]
        ax1.legend(handles=lines, loc='upper left', frameon=False, labelcolor='white')

        self.canvas_trend.draw()

    def on_bar_hover(self, event):
        if not event.inaxes or self.bar_data is None: return
        y_pos = int(round(event.ydata))

        if 0 <= y_pos < len(self.bar_data) and abs(event.ydata - y_pos) < 0.4:
            row = self.bar_data.iloc[y_pos]
            diff = int(row['share_diff'])
            shares_prev = int(row['shares_prev'])
            shares_now = int(row['shares_now'])
            pct = float(row['pct_change'])
            action = row['action_type']
            color = "#FF3333" if diff >= 0 else "#00FF00"

            # 🟢 修正：增加單位「張」(除以1000)
            shares_prev_k = int(shares_prev / 1000)
            shares_now_k = int(shares_now / 1000)
            diff_k = int(diff / 1000)

            html = (
                f"<span style='color:#FFF; font-weight:bold;'>{row['name']}</span> | "
                f"<span style='color:#AAA;'>{shares_prev_k}→{shares_now_k}張</span> | "
                f"<span style='color:{color}; font-weight:bold;'>{action} {diff_k:+,} ({pct:+.1f}%)</span>"
            )
            self.info_label.setText(html)

    def on_line_hover(self, event):
        if not event.inaxes or self.line_data is None: return
        import matplotlib.dates as mdates
        try:
            dt = mdates.num2date(event.xdata).replace(tzinfo=None)
        except:
            return
        etf_df = self.line_data['etf']
        closest_idx = (etf_df['date'] - dt).abs().idxmin()
        row = etf_df.loc[closest_idx]
        change_text = ""
        row_idx = etf_df.index.get_loc(closest_idx)
        if row_idx > 0:
            prev_row = etf_df.iloc[row_idx - 1]
            diff = row['shares'] - prev_row['shares']
            pct = (diff / prev_row['shares']) * 100 if prev_row['shares'] > 0 else 0
            color = "#FF3333" if diff >= 0 else "#00FF00"

            # 🟢 修正：增加單位「張」
            diff_k = int(diff / 1000)
            change_text = f" | <span style='color:{color};'>{diff_k:+,}張 ({pct:+.2f}%)</span>"

        price_val = "--"
        price_df = self.line_data['price']
        if not price_df.empty:
            try:
                p_idx = price_df.index.get_indexer([row['date']], method='nearest')[0]
                price_val = f"{price_df.iloc[p_idx]['Close']:.1f}"
            except:
                pass

        html = (
            f"<span style='color:#DDD;'>{row['date'].strftime('%Y-%m-%d')}</span> | "
            f"<span style='color:#00E5FF;'>權重:{row['weight']}%</span> | "
            f"<span style='color:#FFF;'>持股:{int(row['shares'] / 1000):,}張</span>"
            f"{change_text} | "
            f"<span style='color:#FFD700;'>股價:{price_val}</span>"
        )
        self.info_label.setText(html)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = ActiveETFModule()
    win.resize(1200, 700)
    win.show()
    sys.exit(app.exec())