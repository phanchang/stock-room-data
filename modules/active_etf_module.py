import sys
import json
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
        local_path = Path(f"data/clean/{self.provider}/{self.etf_id}.csv")
        github_url = f"https://raw.githubusercontent.com/phanchang/stock-room-data/main/data/clean/{self.provider}/{self.etf_id}.csv"

        df = pd.DataFrame()

        try:
            if local_path.exists():
                print(f"🏠 [ETF] 讀取本地資料: {local_path}")
                df = pd.read_csv(local_path)
            else:
                print(f"🚀 [ETF] 讀取雲端資料: {github_url}")
                response = requests.get(github_url, timeout=10)
                if response.status_code == 200:
                    csv_data = StringIO(response.text)
                    df = pd.read_csv(csv_data)
                else:
                    print(f"⚠️ [ETF] 雲端無資料或連線失敗，狀態碼: {response.status_code}")

            if not df.empty:
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
            print(f"❌ [ETF] 解析或連線錯誤: {e}")
            self.data_fetched.emit(pd.DataFrame(), self.etf_id)


# --- 主動式 ETF 模組 ---
class ActiveETFModule(QWidget):
    stock_clicked_signal = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.current_df = None
        self.bar_data = None
        self.line_data = None
        self.stock_market_map = {}
        self.latest_data = pd.DataFrame()
        self.merged_data = pd.DataFrame()

        self.mapping = {
            "00981A": ("ezmoney", "統一-00981A (統一台股增長)"),
            "00991A": ("fhtrust", "復華-00991A (復華未來50)"),
            "00982A": ("capitalfund", "群益-00982A (台灣精選強棒)")
        }

        self.load_market_info()
        self.init_ui()

    def load_market_info(self):
        csv_path = Path("data/stock_list.csv")
        if csv_path.exists():
            try:
                for enc in ['utf-8', 'utf-8-sig', 'big5']:
                    try:
                        df = pd.read_csv(csv_path, dtype=str, encoding=enc)
                        df.columns = [c.lower().strip() for c in df.columns]
                        code_col = None
                        if 'stock_id' in df.columns:
                            code_col = 'stock_id'
                        elif 'code' in df.columns:
                            code_col = 'code'
                        elif 'id' in df.columns:
                            code_col = 'id'

                        if code_col and 'market' in df.columns:
                            for _, row in df.iterrows():
                                sid = str(row[code_col]).strip()
                                market = str(row['market']).strip().upper()
                                self.stock_market_map[sid] = market
                            break
                    except:
                        continue
            except Exception as e:
                print(f"❌ [ETF] 讀取市場資訊失敗: {e}")

    def get_market_suffix(self, stock_id):
        return self.stock_market_map.get(str(stock_id), "TW")

    def load_watchlists_to_combo(self):
        """讀取 watchlist.json 並更新比對下拉選單"""
        json_path = Path("data/watchlist.json")
        self.combo_compare.blockSignals(True)
        self.combo_compare.clear()
        self.combo_compare.addItem("🔍 不比對")
        if json_path.exists():
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for key in data.keys():
                        self.combo_compare.addItem(f"📌 {key}")
            except Exception as e:
                print(f"❌ [ETF] 讀取觀察清單失敗: {e}")
        self.combo_compare.blockSignals(False)

    def init_ui(self):
        self.setStyleSheet("background-color: #0E0E0E; color: #E0E0E0;")
        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(5, 5, 5, 5)
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # --- 左側面板 ---
        left_panel = QFrame()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)

        left_header = QWidget()
        left_header.setFixedHeight(45)
        left_header.setStyleSheet("background: #050505; border-bottom: 1px solid #333;")
        lh_layout = QHBoxLayout(left_header)
        lh_layout.setContentsMargins(5, 0, 5, 0)

        lbl_title = QLabel("主動式基金")
        lbl_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #00E5FF;")

        self.combo = QComboBox()
        items = [v[1] for v in self.mapping.values()]
        self.combo.addItems(items)
        self.combo.setStyleSheet("""
            QComboBox { background: #222; color: #FFF; border: 1px solid #444; padding: 4px; font-size: 14px; }
            QComboBox::drop-down { border: none; }
        """)
        self.combo.currentIndexChanged.connect(self.on_combo_change)

        lh_layout.addWidget(lbl_title)
        lh_layout.addStretch()
        left_layout.addWidget(left_header)
        left_layout.addWidget(self.combo)

        # 🔥 功能 3: 增加比對清單列
        top_bar = QWidget()
        top_layout = QHBoxLayout(top_bar)
        top_layout.setContentsMargins(0, 5, 0, 0)
        lbl_top = QLabel("🔥 持股排行")
        lbl_top.setStyleSheet("color: #FFD700; font-weight: bold;")

        self.combo_compare = QComboBox()
        self.load_watchlists_to_combo()
        self.combo_compare.setStyleSheet("""
            QComboBox { background: #222; color: #FFF; border: 1px solid #444; padding: 2px; font-size: 13px; }
            QComboBox::drop-down { border: none; }
        """)
        self.combo_compare.currentIndexChanged.connect(self.render_table)

        top_layout.addWidget(lbl_top)
        top_layout.addStretch()
        top_layout.addWidget(self.combo_compare)
        left_layout.addWidget(top_bar)

        # 🔥 功能 2: 增加張數欄位
        self.stock_table = QTableWidget()
        self.stock_table.setColumnCount(4)
        self.stock_table.setHorizontalHeaderLabels(["代號", "名稱", "權重", "張數(千)"])
        self.stock_table.verticalHeader().setVisible(False)
        self.stock_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.stock_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self.stock_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)

        self.stock_table.setStyleSheet("""
            QTableWidget { background: #121212; border: 1px solid #333; gridline-color: #222; font-size: 14px; font-family: 'Consolas', 'Microsoft JhengHei'; }
            QTableWidget::item { padding: 5px; border-bottom: 1px solid #222; }
            QTableWidget::item:selected { background: #2A2A2A; color: #00E5FF; }
            QHeaderView::section { background: #1A1A1A; color: #888; border: none; padding: 4px; font-weight: bold; }
        """)

        header = self.stock_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self.stock_table.cellClicked.connect(self.on_table_clicked)

        left_layout.addWidget(self.stock_table)

        # --- 右側面板 ---
        right_panel = QFrame()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(10, 0, 0, 0)

        right_header = QWidget()
        right_header.setFixedHeight(45)
        right_header.setStyleSheet("background: #050505; border-bottom: 1px solid #333;")
        rh_layout = QHBoxLayout(right_header)
        rh_layout.setContentsMargins(5, 0, 5, 0)

        self.lbl_etf_info = QLabel("")
        self.lbl_etf_info.setStyleSheet("color: #FFFF00; font-weight: bold; font-size: 16px;")

        self.info_label = QLabel(" 💡 移動滑鼠查看數據 / 點擊長條圖連動個股")
        self.info_label.setStyleSheet("font-family: 'Consolas'; font-size: 13px; color: #888;")

        rh_layout.addWidget(self.lbl_etf_info)
        rh_layout.addStretch()
        rh_layout.addWidget(self.info_label)

        right_layout.addWidget(right_header)

        self.fig_change = Figure(facecolor='#0E0E0E')
        self.canvas_change = FigureCanvas(self.fig_change)
        self.canvas_change.mpl_connect('motion_notify_event', self.on_bar_hover)
        self.canvas_change.mpl_connect('button_press_event', self.on_bar_click)

        self.fig_trend = Figure(facecolor='#0E0E0E')
        self.canvas_trend = FigureCanvas(self.fig_trend)
        self.canvas_trend.mpl_connect('motion_notify_event', self.on_line_hover)

        right_layout.addWidget(self.canvas_change, stretch=4)
        right_layout.addWidget(self.canvas_trend, stretch=6)

        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        # 🔥 微調切割比例，讓左側 4 個欄位完整顯示不用滾動
        splitter.setStretchFactor(0, 35)
        splitter.setStretchFactor(1, 65)

        main_layout.addWidget(splitter)
        self.on_combo_change(0)

    def on_combo_change(self, index):
        keys = list(self.mapping.keys())
        if index < len(keys):
            etf_id = keys[index]
            provider = self.mapping[etf_id][0]
            etf_name = self.mapping[etf_id][1]

            self.lbl_etf_info.setText(etf_name)
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
        self.latest_date = dates[-1]

        self.latest_data = df[df['date'] == self.latest_date].sort_values('weight', ascending=False)
        self.load_watchlists_to_combo()

        if len(dates) >= 2:
            prev_date = dates[-2]
            prev_data = df[df['date'] == prev_date]

            self.merged_data = pd.merge(self.latest_data, prev_data, on=['stock_id', 'name'],
                                        suffixes=('_now', '_prev'), how='outer').fillna(0)
            self.merged_data['share_diff'] = self.merged_data['shares_now'] - self.merged_data['shares_prev']
            self.merged_data['pct_change'] = self.merged_data.apply(
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

            self.merged_data['action_type'] = self.merged_data.apply(classify_action, axis=1)
            self.merged_data['abs_diff'] = self.merged_data['share_diff'].abs()

            active_df = self.merged_data[self.merged_data['abs_diff'] > 0].copy()
            active_df = active_df.sort_values('abs_diff', ascending=False).head(12)

            final_df = active_df.sort_values('pct_change', ascending=True)
            self.plot_changes(final_df, self.latest_date)
        else:
            self.merged_data = pd.DataFrame()
            self.plot_changes(pd.DataFrame(), self.latest_date)

        self.render_table()

        if not self.latest_data.empty:
            first_id = str(self.latest_data.iloc[0]['stock_id'])
            first_name = str(self.latest_data.iloc[0]['name'])
            market = self.get_market_suffix(first_id)
            self.plot_trend(first_id, first_name, market)
            self.stock_table.selectRow(0)

    def render_table(self):
        """將 Table 渲染邏輯獨立，方便下拉選單切換時即時更新箭頭"""
        if getattr(self, 'latest_data', None) is None or self.latest_data.empty: return

        # 讀取目前選取的比對清單
        selected_compare = self.combo_compare.currentText()
        compare_list = []
        if selected_compare != "🔍 不比對":
            group_name = selected_compare.replace("📌 ", "")
            json_path = Path("data/watchlist.json")
            if json_path.exists():
                try:
                    with open(json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        compare_list = data.get(group_name, [])
                except:
                    pass

        self.stock_table.setRowCount(len(self.latest_data))
        for idx, row in enumerate(self.latest_data.itertuples()):
            sid = str(row.stock_id)
            name = str(row.name)
            weight = row.weight
            shares = row.shares
            shares_k = int(shares / 1000) if pd.notna(shares) else 0

            arrow = ""
            shares_color = QColor("#FFD700") if idx < 10 else QColor("#AAAAAA")

            # 🔥 功能 3: 判斷是否在選取的清單中，並給予對應的箭頭與顏色
            if sid in compare_list:
                diff = 0
                if hasattr(self, 'merged_data') and not self.merged_data.empty:
                    match = self.merged_data[self.merged_data['stock_id'] == sid]
                    if not match.empty:
                        diff = match.iloc[0]['share_diff']

                if diff > 0:
                    arrow = " ⬆"
                    shares_color = QColor("#FF3333")  # 紅色代表增持
                elif diff < 0:
                    arrow = " ⬇"
                    shares_color = QColor("#00FF00")  # 綠色代表減持
                else:
                    arrow = " -"
                    shares_color = QColor("#FFFF00")  # 黃色代表吻合但無變化

            item_id = QTableWidgetItem(sid)
            item_name = QTableWidgetItem(name)
            item_weight = QTableWidgetItem(f"{weight:.2f}%")
            item_shares = QTableWidgetItem(f"{shares_k:,}{arrow}")

            item_id.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            item_name.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            item_weight.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            item_shares.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

            font = QFont()
            if idx < 10 or arrow:
                font.setBold(True)

            base_color = QColor("#FFD700") if idx < 10 else QColor("#AAAAAA")

            for item in [item_id, item_name, item_weight]:
                item.setForeground(base_color)
                item.setFont(font)

            item_shares.setForeground(shares_color)
            item_shares.setFont(font)

            self.stock_table.setItem(idx, 0, item_id)
            self.stock_table.setItem(idx, 1, item_name)
            self.stock_table.setItem(idx, 2, item_weight)
            self.stock_table.setItem(idx, 3, item_shares)

    def plot_changes(self, df, data_date):
        self.fig_change.clear()
        ax = self.fig_change.add_subplot(111)
        ax.set_facecolor('#0E0E0E')  # 🔥 統一背景顏色

        self.bar_data = df.reset_index(drop=True)

        if self.bar_data.empty:
            ax.text(0.5, 0.5, "期間無明顯持股變動", ha='center', va='center', color='#555', fontsize=16)
            ax.axis('off')
            self.canvas_change.draw()
            return

        colors = ['#FF3333' if x >= 0 else '#00FF00' for x in self.bar_data['share_diff']]
        y_pos = np.arange(len(self.bar_data))
        ax.barh(y_pos, self.bar_data['pct_change'], color=colors, align='center', height=0.6)

        ax.set_yticks(y_pos)
        ax.set_yticklabels(self.bar_data['name'], fontsize=11, fontweight='bold', color='#DDD')
        ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=100))

        ax.set_ylim(-0.5, 11.5)

        date_str = data_date.strftime('%Y-%m-%d')
        ax.set_title(f" 資金流向戰報 Top 12 (日期: {date_str})", color='white', fontsize=13, fontweight='bold',
                     loc='left', pad=10)
        ax.tick_params(colors='#AAA', labelsize=10)
        ax.grid(axis='x', color='#333', linestyle=':')
        for spine in ax.spines.values():
            spine.set_visible(False)
        self.canvas_change.draw()

    def on_bar_click(self, event):
        """處理點擊長條圖連動"""
        if not event.inaxes or self.bar_data is None: return
        if event.inaxes != self.fig_change.axes[0]: return

        y_pos = int(round(event.ydata))
        if 0 <= y_pos < len(self.bar_data):
            row = self.bar_data.iloc[y_pos]
            sid = str(row['stock_id'])
            name = str(row['name'])
            market = self.get_market_suffix(sid)

            for i in range(self.stock_table.rowCount()):
                if self.stock_table.item(i, 0).text() == sid:
                    self.stock_table.selectRow(i)
                    break

            self.plot_trend(sid, name, market)
            self.stock_clicked_signal.emit(f"{sid}_{market}")

    def on_table_clicked(self, row, col):
        sid = self.stock_table.item(row, 0).text()
        name = self.stock_table.item(row, 1).text()
        market = self.get_market_suffix(sid)
        self.plot_trend(sid, name, market)
        self.stock_clicked_signal.emit(f"{sid}_{market}")

    def plot_trend(self, stock_id, stock_name, market="TW"):
        if self.current_df is None: return

        # 找出 ETF 整體資料最新的日期
        global_max_date = self.current_df['date'].max()

        trend_data = self.current_df[self.current_df['stock_id'] == str(stock_id)].copy()
        if trend_data.empty: return

        # 功能 1 修復: 如果這檔股票被清空了，最新日期會缺少這筆，我們手動補上 0 的數據讓曲線下跌
        if trend_data['date'].max() < global_max_date:
            new_row = trend_data.iloc[-1].copy()
            new_row['date'] = global_max_date
            new_row['shares'] = 0
            new_row['weight'] = 0.0
            trend_data = pd.concat([trend_data, pd.DataFrame([new_row])], ignore_index=True)

        trend_data = trend_data.sort_values('date')

        price_data = pd.DataFrame()
        price_path = Path(f"data/cache/tw/{stock_id}_{market}.parquet")

        if price_path.exists():
            try:
                price_df = pd.read_parquet(price_path)
                price_df.columns = [c.capitalize() for c in price_df.columns]
                price_df.index = pd.to_datetime(price_df.index).tz_localize(None)

                if not trend_data.empty:
                    min_date = trend_data['date'].min()
                    price_data = price_df[price_df.index >= min_date].copy()
            except Exception as e:
                print(f"❌ [圖表除錯] 讀取股價 {stock_id} 發生錯誤: {e}")

        self.fig_trend.clear()
        ax1 = self.fig_trend.add_subplot(111)
        ax1.set_facecolor('#0E0E0E')  # 🔥 統一背景顏色

        ax2 = ax1.twinx()
        ax3 = ax1.twinx()

        ax3.spines['right'].set_position(('outward', 60))
        ax1.set_zorder(10)
        ax2.set_zorder(11)
        ax3.set_zorder(1)
        ax1.patch.set_visible(False)
        ax2.patch.set_visible(False)

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
        l4 = None
        if not price_data.empty:
            l2, = ax2.plot(price_data.index, price_data['Close'], color='#FFD700', linewidth=1.5, linestyle='--',
                           alpha=0.9, label='股價(右)')

            avg_cost = 0.0
            total_shares = 0
            total_cost = 0.0
            daily_costs = []  # 🔥 存放每天的成本

            for _, row in trend_data.iterrows():
                d = row['date']
                shares = row['shares']
                current_cost = 0.0

                try:
                    p_idx = price_data.index.get_indexer([d], method='ffill')[0]
                    if p_idx != -1:
                        price = price_data['Close'].iloc[p_idx]
                    else:
                        price = 0
                except:
                    price = 0

                if price > 0:
                    if shares > total_shares:
                        added = shares - total_shares
                        total_cost += added * price
                        total_shares = shares
                    elif shares < total_shares:
                        if shares == 0:
                            total_shares = 0
                            total_cost = 0.0
                        else:
                            total_cost = total_cost * (shares / total_shares)
                            total_shares = shares

                if total_shares > 0:
                    current_cost = total_cost / total_shares

                daily_costs.append(current_cost)

            # 🔥 將每日成本存回 dataframe，並繪製動態成本點與線
            trend_data['avg_cost'] = daily_costs
            cost_mask = trend_data['shares'] > 0  # 只在有持股時畫成本線
            if cost_mask.any():
                l4, = ax2.plot(trend_data.loc[cost_mask, 'date'], trend_data.loc[cost_mask, 'avg_cost'],
                               color='#FF00FF', linestyle=':', marker='o', markersize=4,
                               linewidth=1.5, alpha=0.7, label='動態成本')

            if total_shares > 0:
                avg_cost = total_cost / total_shares

            last_date = price_data.index[-1]
            last_price = price_data['Close'].iloc[-1]
            ax2.plot(last_date, last_price, marker='o', color='#FFD700', markersize=6)

            bbox_price = dict(boxstyle="round,pad=0.2", fc="#222222", ec="#FFD700", alpha=0.8)
            ax2.text(last_date, last_price, f" {last_price:.1f}", color='#FFD700',
                     fontsize=10, fontweight='bold', va='center', ha='left', bbox=bbox_price)

            # 🔥 固定產出最後的成本價格 (保持原本邏輯)
            if avg_cost > 0:
                ax2.axhline(avg_cost, color='#FF00FF', linestyle='--', linewidth=1, alpha=0.4)

                ymin, ymax = ax2.get_ylim()
                if ymin == 0 and ymax == 1:
                    ymin = price_data['Close'].min() * 0.95
                    ymax = price_data['Close'].max() * 1.05
                    ax2.set_ylim(ymin, ymax)

                text_y = max(ymin + (ymax - ymin) * 0.05, min(ymax - (ymax - ymin) * 0.05, avg_cost))

                bbox_cost = dict(boxstyle="larrow,pad=0.3", fc="#FF00FF", ec="none", alpha=0.3)
                ax2.text(price_data.index[0], text_y, f"成本: {avg_cost:.1f}",
                         color='#FFDDFF', fontsize=10, fontweight='bold',
                         ha='left', va='center', bbox=bbox_cost)

        else:
            ax2.text(0.5, 0.5, f"無本地股價 ({stock_id}_{market})", transform=ax2.transAxes, color='#555', ha='center',
                     va='center')

        # 寫回更新過的 dataframe 以供 hover 使用
        self.line_data = {
            'etf': trend_data,
            'price': price_data,
            'ax1': ax1,
            'ax2': ax2,
            'ax3': ax3
        }

        ax1.yaxis.set_major_formatter(mtick.PercentFormatter())
        ax1.set_ylabel("持股權重 %", color='#00E5FF')
        ax2.set_ylabel(f"{stock_name} 股價", color='#FFD700')
        ax3.set_yticks([])
        ax3.spines['right'].set_visible(False)

        ax1.set_title(f" {stock_id} {stock_name} - 權重、股價與成本", color='white', fontsize=12, fontweight='bold',
                      loc='left')
        ax1.tick_params(axis='y', colors='#00E5FF')
        ax1.tick_params(axis='x', colors='#AAA')
        ax2.tick_params(axis='y', colors='#FFD700')
        ax1.grid(True, color='#222', linestyle=':')

        for ax in [ax1, ax2]:
            for spine in ax.spines.values():
                spine.set_visible(False)

        lines = [l1]
        if l2: lines.append(l2)
        lines.append(l3)
        if l4: lines.append(l4)
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
            action = row['action_type'] if row['action_type'] else ""
            color = "#FF3333" if diff >= 0 else "#00FF00"

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

        # 🔥 抓取計算好的動態成本
        cost_val = "--"
        if 'avg_cost' in row and row['avg_cost'] > 0:
            cost_val = f"{row['avg_cost']:.1f}"

        html = (
            f"<span style='color:#DDD;'>{row['date'].strftime('%Y-%m-%d')}</span> | "
            f"<span style='color:#00E5FF;'>權重:{row['weight']}%</span> | "
            f"<span style='color:#FFF;'>持股:{int(row['shares'] / 1000):,}張</span>"
            f"{change_text} | "
            f"<span style='color:#FFD700;'>股價:{price_val}</span> | "
            f"<span style='color:#FF00FF;'>成本:{cost_val}</span>"
        )
        self.info_label.setText(html)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = ActiveETFModule()
    win.resize(1200, 700)
    win.show()
    sys.exit(app.exec())