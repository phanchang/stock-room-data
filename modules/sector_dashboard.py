# 檔案路徑: modules/sector_dashboard.py
import os
import pandas as pd
from pathlib import Path
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QSplitter,
                             QTableWidget, QTableWidgetItem, QHeaderView,
                             QGroupBox, QLabel, QAbstractItemView, QLineEdit, QComboBox)
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QColor, QBrush


class NumericItem(QTableWidgetItem):
    def __lt__(self, other):
        try:
            return float(self.data(Qt.ItemDataRole.UserRole)) < float(other.data(Qt.ItemDataRole.UserRole))
        except (ValueError, TypeError):
            return super().__lt__(other)


class SectorDashboard(QWidget):
    stock_double_clicked = pyqtSignal(str)

    def __init__(self, project_root):
        super().__init__()
        self.project_root = Path(project_root)
        self.sector_dir = self.project_root / 'data' / 'cache' / 'sector'
        self.snapshot_path = self.project_root / 'data' / 'strategy_results' / 'factor_snapshot.parquet'

        self.snapshot_df = None
        self.sector_members = {}

        self.init_data()
        self.init_ui()
        self.load_sectors_to_table()

    def init_data(self):
        if self.snapshot_path.exists():
            try:
                self.snapshot_df = pd.read_parquet(self.snapshot_path)
                if 'sid' in self.snapshot_df.columns:
                    self.snapshot_df['股票代號'] = self.snapshot_df['sid'].astype(str)
                    self.snapshot_df['股票名稱'] = self.snapshot_df.get('name', '')
                    self.snapshot_df['今日收盤價'] = self.snapshot_df.get('現價', 0)
                    self.snapshot_df['今日漲幅(%)'] = self.snapshot_df.get('漲幅1d', 0)
                    self.snapshot_df['5日漲幅(%)'] = self.snapshot_df.get('漲幅5d', 0)
                    self.snapshot_df['RS強度'] = self.snapshot_df.get('RS強度', 0)
                    self.snapshot_df['法人5日增(%)'] = self.snapshot_df.get('legal_diff_5d', 0)
                    self.snapshot_df['融資5日增(%)'] = self.snapshot_df.get('margin_diff_5d', 0)
                    self.snapshot_df['強勢特徵標籤'] = self.snapshot_df.get('強勢特徵', '')

                    if 'str_30w_week_offset' in self.snapshot_df.columns:
                        self.snapshot_df['30W距離'] = pd.to_numeric(self.snapshot_df['str_30w_week_offset'],
                                                                    errors='coerce').fillna(99)
                    else:
                        self.snapshot_df['30W距離'] = 99.0

                    if 'str_st_week_offset' in self.snapshot_df.columns:
                        self.snapshot_df['ST距離'] = pd.to_numeric(self.snapshot_df['str_st_week_offset'],
                                                                   errors='coerce').fillna(99)
                    else:
                        self.snapshot_df['ST距離'] = 99.0
            except Exception as e:
                pass

        self.sector_types = {}  # 新增：記錄板塊分類屬性
        for file_name, col_name, s_type in [('dj_industry.csv', 'dj_sub_ind', '細產業'),
                                            ('concept_tags.csv', 'sub_concepts', '概念股')]:
            path = self.project_root / 'data' / file_name
            if path.exists():
                df = pd.read_csv(path, dtype=str)
                for _, row in df.iterrows():
                    sid = str(row['sid']).strip()
                    for tag in str(row[col_name]).split(','):
                        tag = tag.strip()
                        if tag and tag != 'nan':
                            self.sector_members.setdefault(tag, set()).add(sid)
                            # 將該板塊標記為 細產業 或 概念股
                            if tag not in self.sector_types:
                                self.sector_types[tag] = s_type

    def init_ui(self):
        self.setStyleSheet("""
                    QGroupBox {
                        border: 1px solid #3E3E42;
                        border-radius: 6px;
                        margin-top: 18px;
                        font-weight: bold;
                    }
                    QGroupBox::title {
                        subcontrol-origin: margin;
                        subcontrol-position: top left;
                        padding: 0 8px;
                        color: #FFD700; /* 使用亮金黃色，在黑底中絕對顯眼且具備層次感 */
                        font-size: 15px;
                    }
                """)
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(5, 5, 5, 5)
        self.main_splitter = QSplitter(Qt.Orientation.Horizontal)

        self.left_group = QGroupBox("🔥 板塊熱度龍虎榜")
        left_layout = QVBoxLayout()

        sector_search_layout = QHBoxLayout()
        sector_search_layout.setContentsMargins(0, 0, 0, 5)

        # ====== 新增：板塊分類過濾選單 ======
        self.combo_sector_type = QComboBox()
        self.combo_sector_type.addItems(["全部板塊", "細產業", "概念股"])
        self.combo_sector_type.setStyleSheet(
            "background: #222; color: #FFF; border: 1px solid #444; padding: 4px; font-weight: bold;")
        self.combo_sector_type.currentTextChanged.connect(self.filter_sectors)
        sector_search_layout.addWidget(self.combo_sector_type)

        self.combo_sector_filter = QComboBox()
        self.combo_sector_filter.addItems([
            "-- ⚡ 尋找起漲板塊 --",
            "🔥30W 剛突破",
            "🔥ST 剛轉多",
            "🚀 營收資金雙殺 (全面啟動)",  # 👇 新增強勢過濾器
            "💰 法人全面點火",
            "📈 營收加速爆發"
        ])
        self.combo_sector_filter.setStyleSheet(
            "background: #222; color: #FFD700; border: 1px solid #444; padding: 4px; font-weight: bold;")
        self.combo_sector_filter.currentTextChanged.connect(self.on_sector_quick_filter)
        sector_search_layout.addWidget(self.combo_sector_filter)

        self.txt_search_sector = QLineEdit()
        self.txt_search_sector.setPlaceholderText("🔍 搜尋板塊名稱...")
        self.txt_search_sector.setStyleSheet("background: #222; color: #FFF; border: 1px solid #444; padding: 4px;")
        self.txt_search_sector.textChanged.connect(self.filter_sectors)
        sector_search_layout.addWidget(self.txt_search_sector)

        left_layout.addLayout(sector_search_layout)

        # 👇 更新：插入 3 個新的高階數據欄位，總計 10 個欄位
        self.sector_table = self.create_styled_table(
            ['分類', '板塊名稱', '型態', '法人擴散', '營收廣度', 'YoY加速', '5日(%)', '今日(%)', '量比', '檔數'])
        self.sector_table.itemSelectionChanged.connect(self.on_sector_selected)
        left_layout.addWidget(self.sector_table)
        self.left_group.setLayout(left_layout)

        self.right_splitter = QSplitter(Qt.Orientation.Vertical)

        self.kline_group = QGroupBox("📊 板塊專屬 K 線 (市值加權合成)")
        self.kline_layout = QVBoxLayout()
        self.kline_placeholder = QLabel("請從左側選擇板塊以載入 K 線圖...")
        self.kline_placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.kline_placeholder.setStyleSheet("color: gray; font-size: 14px;")
        self.kline_layout.addWidget(self.kline_placeholder)
        self.kline_group.setLayout(self.kline_layout)

        self.constituents_group = QGroupBox("💎 成分股尋寶 (雙擊聯動戰情室)")
        constituents_layout = QVBoxLayout()

        search_layout = QHBoxLayout()
        search_layout.setContentsMargins(0, 0, 0, 5)

        self.combo_quick_filter = QComboBox()
        self.combo_quick_filter.addItems([
            "-- ⚡ 快速特徵 --",
            "🔥 30W剛起漲(近3週)", "🔥 ST剛轉多(近3週)",
            "創月高", "創季高", "突破30週", "強勢多頭",
            "投信認養", "極度壓縮", "假跌破", "主力掃單(ILSS)"
        ])
        self.combo_quick_filter.setStyleSheet(
            "background: #222; color: #00E5FF; border: 1px solid #444; padding: 4px; font-weight: bold; font-size: 13px;")
        self.combo_quick_filter.currentTextChanged.connect(self.on_quick_filter_changed)
        search_layout.addWidget(self.combo_quick_filter)

        self.txt_search_const = QLineEdit()
        self.txt_search_const.setPlaceholderText("🔍 或手動過濾特徵與名稱 (例如: 投信、2330)...")
        self.txt_search_const.setStyleSheet(
            "background: #222; color: #FFF; border: 1px solid #444; padding: 4px; font-size: 14px;")
        self.txt_search_const.textChanged.connect(self.filter_constituents)
        search_layout.addWidget(self.txt_search_const)

        constituents_layout.addLayout(search_layout)

        cols = ['代號', '名稱', '收盤價', '今日(%)', '5日(%)', '法人5日增(%)', '融資5日增(%)', 'RS強度', '強勢特徵',
                '30W距離', 'ST距離']
        self.const_table = self.create_styled_table(cols)
        self.const_table.cellDoubleClicked.connect(self.on_stock_double_clicked)

        self.const_table.horizontalHeader().setSectionResizeMode(8, QHeaderView.ResizeMode.Stretch)
        self.const_table.setColumnHidden(9, True)
        self.const_table.setColumnHidden(10, True)

        constituents_layout.addWidget(self.const_table)
        self.constituents_group.setLayout(constituents_layout)

        self.right_splitter.addWidget(self.kline_group)
        self.right_splitter.addWidget(self.constituents_group)
        self.right_splitter.setSizes([550, 450])

        self.main_splitter.addWidget(self.left_group)
        self.main_splitter.addWidget(self.right_splitter)
        self.main_splitter.setSizes([380, 820])

        main_layout.addWidget(self.main_splitter)

    def reload_dashboard(self):
        """🔄 重新載入硬碟最新數據並更新 UI (提供給外部或頁籤切換時呼叫)"""
        # 1. 重新讀取最新的 factor_snapshot.parquet
        self.init_data()

        # 2. 重新讀取緩存資料夾內的 IDX_*.parquet 並重繪左側列表
        self.load_sectors_to_table()

        # 3. 清空右側 K 線圖，避免殘留舊板塊的畫面
        if hasattr(self, 'kline_widget') and self.kline_widget is not None:
            self.kline_widget.setParent(None)
            self.kline_widget.deleteLater()
            self.kline_widget = None

        # 4. 恢復 K 線圖的 Placeholder 提示文字
        for i in reversed(range(self.kline_layout.count())):
            widget = self.kline_layout.itemAt(i).widget()
            if widget is not None:
                widget.setParent(None)
                widget.deleteLater()

        self.kline_placeholder = QLabel("請從左側選擇板塊以載入 K 線圖...")
        self.kline_placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.kline_placeholder.setStyleSheet("color: gray; font-size: 14px;")
        self.kline_layout.addWidget(self.kline_placeholder)

        # 5. 清空下方成分股列表
        self.const_table.setRowCount(0)



    # 替換整個 create_styled_table function
    def create_styled_table(self, columns):
        table = QTableWidget()
        table.setColumnCount(len(columns))
        table.setHorizontalHeaderLabels(columns)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.verticalHeader().setVisible(False)
        table.setAlternatingRowColors(True)

        header = table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        header.setSortIndicatorShown(True)
        header.setMinimumSectionSize(65)
        # 👇 修正白色斷層：強制最後一欄（檔數）填滿剩餘的右側空間
        header.setStretchLastSection(True)

        # 👇 徹底覆蓋所有邊角與底層背景為暗黑主題
        table.setStyleSheet("""
            QTableWidget { background-color: #121212; alternate-background-color: #1A1A1A; color: #E0E0E0; gridline-color: #333; border: 1px solid #333; }
            QTableWidget::item:selected { background-color: #004466; color: #FFF; }

            QHeaderView { background-color: #222; border: none; }
            QHeaderView::section { background-color: #222; color: #00E5FF; padding: 4px; border: 1px solid #333; font-weight: bold; font-size: 13px;}

            QScrollBar:vertical { border: none; background: #121212; width: 12px; }
            QScrollBar::handle:vertical { background: #555; border-radius: 6px; min-height: 20px; }
            QScrollBar::handle:vertical:hover { background: #777; }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { border: none; background: none; }

            QScrollBar:horizontal { border: none; background: #121212; height: 12px; }
            QScrollBar::handle:horizontal { background: #555; border-radius: 6px; min-width: 20px; }
            QScrollBar::handle:horizontal:hover { background: #777; }
            QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { border: none; background: none; }

            QTableCornerButton::section { background-color: #121212; border: 1px solid #333; }
            QAbstractScrollArea::corner { background-color: #121212; border: none; }
        """)
        return table

    def format_number_item(self, val, is_pct=False):
        item = NumericItem()
        try:
            val_float = float(val)
            if pd.isna(val_float):
                item.setData(Qt.ItemDataRole.UserRole, -999.0)
                item.setText("-")
                return item

            item.setData(Qt.ItemDataRole.UserRole, val_float)
            text = f"{val_float:.2f}"
            if is_pct: text += "%"
            item.setText(text)

            if val_float > 0:
                item.setForeground(QBrush(QColor('#FF5252')))
            elif val_float < 0:
                item.setForeground(QBrush(QColor('#4CAF50')))
            else:
                item.setForeground(QBrush(QColor('#E0E0E0')))
        except:
            item.setData(Qt.ItemDataRole.UserRole, 0.0)
            item.setText(str(val))
        return item

    def on_sector_quick_filter(self, text):
        self.txt_search_sector.blockSignals(True)  # 暫停文字框的連動，避免重複觸發
        if "⚡" in text:
            self.txt_search_sector.clear()
        elif "30W" in text:
            self.txt_search_sector.setText("🔥30W")
        elif "ST" in text:
            self.txt_search_sector.setText("🔥ST")
        else:
            self.txt_search_sector.clear()  # 若為新增的高階數值過濾，清空文字搜尋框

        self.txt_search_sector.blockSignals(False)
        self.filter_sectors()  # 強制執行過濾判定

    def load_sectors_to_table(self):
        self.sector_table.setSortingEnabled(False)
        self.sector_table.setRowCount(0)

        if not self.sector_dir.exists(): return

        idx_files = list(self.sector_dir.glob("IDX_*.parquet"))
        for file_path in idx_files:
            sector_name = file_path.stem.replace("IDX_", "")
            try:
                df = pd.read_parquet(file_path)
                if len(df) < 6: continue

                close_today = df['adj_close'].iloc[-1]
                close_1d = df['adj_close'].iloc[-2]
                close_5d = df['adj_close'].iloc[-6]

                pct_1d = ((close_today - close_1d) / close_1d) * 100
                pct_5d = ((close_today - close_5d) / close_5d) * 100

                vol_today = df['volume'].iloc[-1]
                vol_5d_avg = df['volume'].iloc[-6:-1].mean()
                vol_ratio = (vol_today / vol_5d_avg) if vol_5d_avg > 0 else 0

                member_count = len(self.sector_members.get(sector_name, []))

                df_w = df.copy()
                df_w.index = pd.to_datetime(df_w.index)
                df_w = df_w.resample('W-FRI').agg({
                    'adj_open': 'first', 'adj_high': 'max', 'adj_low': 'min', 'adj_close': 'last', 'volume': 'sum'
                }).dropna()
                df_w = df_w.rename(
                    columns={'adj_open': 'Open', 'adj_high': 'High', 'adj_low': 'Low', 'adj_close': 'Close',
                             'volume': 'Volume'})

                is_30w_break = False
                is_st_break = False

                if len(df_w) > 30:
                    df_w['MA30'] = df_w['Close'].rolling(30).mean()
                    for i in range(-3, 0):
                        if df_w['Close'].iloc[i] > df_w['MA30'].iloc[i] and df_w['Close'].iloc[i - 1] <= \
                                df_w['MA30'].iloc[i - 1]:
                            is_30w_break = True
                            break

                    try:
                        from utils.strategies.technical import TechnicalStrategies
                        st_df = TechnicalStrategies.calculate_supertrend(df_w)
                        if (st_df['Signal'].iloc[-3:] == 1).any():
                            is_st_break = True
                    except:
                        pass

                tags = []
                if is_30w_break: tags.append("🔥30W")
                if is_st_break: tags.append("🔥ST")
                tag_str = ",".join(tags)

                row = self.sector_table.rowCount()
                self.sector_table.insertRow(row)

                # --- 欄位 0: 分類 ---
                sector_type = self.sector_types.get(sector_name, "未知")
                type_item = NumericItem(sector_type)
                type_item.setData(Qt.ItemDataRole.UserRole, 0)
                if sector_type == '細產業':
                    type_item.setForeground(QBrush(QColor('#00E5FF')))  # 藍綠色區隔
                else:
                    type_item.setForeground(QBrush(QColor('#FF9800')))  # 橘黃色區隔
                self.sector_table.setItem(row, 0, type_item)

                # --- 欄位 1: 板塊名稱 ---
                name_item = NumericItem(sector_name)
                name_item.setData(Qt.ItemDataRole.UserRole, 0)
                self.sector_table.setItem(row, 1, name_item)

                # --- 欄位 2: 型態 ---
                tag_item = NumericItem(tag_str)
                tag_item.setData(Qt.ItemDataRole.UserRole, 0)
                tag_item.setForeground(QBrush(QColor('#FFD700')))
                self.sector_table.setItem(row, 2, tag_item)

                # 👇 新增提取三個高階指標 (取最後一天的值)
                legal_diff = df['Legal_Diffusion'].iloc[-1] if 'Legal_Diffusion' in df.columns else 0.0
                rev_diff = df['Rev_Diffusion'].iloc[-1] if 'Rev_Diffusion' in df.columns else 0.0
                yoy_accel = df['YoY_Accel'].iloc[-1] if 'YoY_Accel' in df.columns else 0.0

                # --- 欄位 3 ~ 5: 核心高階數據 ---
                legal_item = self.format_number_item(legal_diff, True)
                if legal_diff >= 50: legal_item.setForeground(QBrush(QColor('#00E5FF')))
                self.sector_table.setItem(row, 3, legal_item)

                rev_item = self.format_number_item(rev_diff, True)
                if rev_diff >= 50: rev_item.setForeground(QBrush(QColor('#40C4FF')))
                self.sector_table.setItem(row, 4, rev_item)

                accel_item = self.format_number_item(yoy_accel, True)
                if yoy_accel > 0:
                    accel_item.setForeground(QBrush(QColor('#FF5252')))
                elif yoy_accel < 0:
                    accel_item.setForeground(QBrush(QColor('#4CAF50')))
                self.sector_table.setItem(row, 5, accel_item)

                # --- 欄位 6 ~ 9: 原有數據延後 ---
                self.sector_table.setItem(row, 6, self.format_number_item(pct_5d, True))
                self.sector_table.setItem(row, 7, self.format_number_item(pct_1d, True))

                ratio_item = self.format_number_item(vol_ratio, False)
                if vol_ratio >= 1.2: ratio_item.setForeground(QBrush(QColor('#FFD700')))
                self.sector_table.setItem(row, 8, ratio_item)

                count_item = self.format_number_item(member_count, False)
                count_item.setText(str(member_count))
                self.sector_table.setItem(row, 9, count_item)
            except Exception as e:
                pass

        self.sector_table.setSortingEnabled(True)
        # 👇 修正：因為欄位擴增，原本排 5日漲幅 的欄位從第 3 欄變成第 6 欄了
        self.sector_table.sortItems(6, Qt.SortOrder.DescendingOrder)

    def filter_sectors(self, *args):
        search_text = self.txt_search_sector.text().lower()
        type_filter = self.combo_sector_type.currentText()
        quick_filter = self.combo_sector_filter.currentText()  # 取得上方下拉選單狀態

        for i in range(self.sector_table.rowCount()):
            match_search = False
            match_type = False
            match_quick = True  # 預設數值過濾過關

            # 1. 檢查分類是否符合 (欄位 0)
            type_item = self.sector_table.item(i, 0)
            if type_filter == "全部板塊" or (type_item and type_item.text() == type_filter):
                match_type = True

            # 2. 檢查關鍵字是否符合 (欄位 1:名稱, 欄位 2:型態)
            if not search_text:
                match_search = True
            else:
                for col in [1, 2]:
                    item = self.sector_table.item(i, col)
                    if item and search_text in item.text().lower():
                        match_search = True
                        break

            # 3. 檢查高階數值過濾 (若使用者有選)
            if quick_filter == "🚀 營收資金雙殺 (全面啟動)":
                legal_val = float(self.sector_table.item(i, 3).data(Qt.ItemDataRole.UserRole))
                rev_val = float(self.sector_table.item(i, 4).data(Qt.ItemDataRole.UserRole))
                accel_val = float(self.sector_table.item(i, 5).data(Qt.ItemDataRole.UserRole))
                # 嚴格條件：資金擴散>=50% 且 營收廣度>=50% 且 營收正在加速(>0)
                if legal_val < 50 or rev_val < 50 or accel_val <= 0:
                    match_quick = False

            elif quick_filter == "💰 法人全面點火":
                legal_val = float(self.sector_table.item(i, 3).data(Qt.ItemDataRole.UserRole))
                if legal_val < 50:
                    match_quick = False

            elif quick_filter == "📈 營收加速爆發":
                rev_val = float(self.sector_table.item(i, 4).data(Qt.ItemDataRole.UserRole))
                accel_val = float(self.sector_table.item(i, 5).data(Qt.ItemDataRole.UserRole))
                if rev_val < 50 or accel_val <= 0:
                    match_quick = False

            # 必須同時滿足分類、關鍵字、數值快篩條件，否則隱藏該列
            self.sector_table.setRowHidden(i, not (match_search and match_type and match_quick))

    def on_sector_selected(self):
        selected_items = self.sector_table.selectedItems()
        if not selected_items: return
        row = selected_items[0].row()

        # 注意：名稱原本在欄位 0，現在改為欄位 1
        sector_name = self.sector_table.item(row, 1).text()

        self.update_kline_view(sector_name)
        self.update_constituents_table(sector_name)

        self.combo_quick_filter.blockSignals(True)
        self.combo_quick_filter.setCurrentIndex(0)
        self.combo_quick_filter.blockSignals(False)
        self.txt_search_const.blockSignals(True)
        self.txt_search_const.clear()
        self.txt_search_const.blockSignals(False)

    def update_kline_view(self, sector_name):
        """🚀 終極修復：暴力對齊所有 DataFrame 欄位，並強制從硬碟讀取最新資料"""
        sid = f"IDX_{sector_name}"
        try:
            # --- 修正點：直接從實體檔案讀取，繞過 CacheManager 的記憶體快取 ---
            file_path = self.sector_dir / f"{sid}.parquet"
            if not file_path.exists():
                raise ValueError(f"找不到板塊資料: {file_path}")

            df = pd.read_parquet(file_path)
            # --- 修正點結束 ---

            # 1. 全部轉小寫處理，徹底防堵大小寫造成 KeyError
            df.columns = [str(c).lower() for c in df.columns]

            # 2. 保證核心 5 大欄位必定存在 (找不到就拿 adj_ 補，再沒有就補 0)
            core_cols = ['open', 'high', 'low', 'close', 'volume']
            for c in core_cols:
                if c not in df.columns:
                    if f"adj_{c}" in df.columns:
                        df[c] = df[f"adj_{c}"]
                    else:
                        df[c] = 0.0

            # 3. 保證 Adj_ 系列必定存在 (因為引擎切換『還原』時會強制存取)
            for c in core_cols:
                if f"adj_{c}" not in df.columns:
                    df[f"adj_{c}"] = df[c]

            # 4. 重新命名為引擎唯一認可的大小寫格式
            rename_map = {
                'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume',
                'adj_open': 'Adj_open', 'adj_high': 'Adj_high', 'adj_low': 'Adj_low', 'adj_close': 'Adj_close'
            }
            df = df.rename(columns=rename_map)

            # 5. 防呆：確保 Dividends 存在且欄位沒有重複
            if 'Dividends' not in df.columns: df['Dividends'] = 0.0
            df = df.loc[:, ~df.columns.duplicated()]

            # 🚀 完美保留狀態：如果 kline_widget 已經存在，就只「抽換底層資料」！
            if hasattr(self, 'kline_widget') and self.kline_widget is not None:
                self.kline_widget.update_stock_data(sid, df, f"板塊: {sector_name}")
            else:
                for i in reversed(range(self.kline_layout.count())):
                    widget = self.kline_layout.itemAt(i).widget()
                    if widget is not None:
                        widget.setParent(None)
                        widget.deleteLater()

                from modules.expanded_kline import ExpandedKLineWindow
                self.kline_widget = ExpandedKLineWindow(stock_id=sid, df=df, stock_name=f"板塊: {sector_name}")
                self.kline_widget.setWindowFlags(Qt.WindowType.Widget)
                self.kline_layout.addWidget(self.kline_widget)

        except Exception as e:
            err = QLabel(f"K線載入失敗: {e}")
            err.setStyleSheet("color: #FF5252; font-size: 14px;")
            self.kline_layout.addWidget(err)

    def update_constituents_table(self, sector_name):
        self.const_table.setSortingEnabled(False)
        self.const_table.setRowCount(0)

        if self.snapshot_df is None or self.snapshot_df.empty: return

        sids = self.sector_members.get(sector_name, set())
        filtered_df = self.snapshot_df[self.snapshot_df['股票代號'].isin(sids)]

        for _, row_data in filtered_df.iterrows():
            row = self.const_table.rowCount()
            self.const_table.insertRow(row)

            sid_item = NumericItem(str(row_data.get('股票代號', '')))
            sid_item.setData(Qt.ItemDataRole.UserRole, 0)
            self.const_table.setItem(row, 0, sid_item)

            name_item = NumericItem(str(row_data.get('股票名稱', '')))
            name_item.setData(Qt.ItemDataRole.UserRole, 0)
            self.const_table.setItem(row, 1, name_item)

            self.const_table.setItem(row, 2, self.format_number_item(row_data.get('今日收盤價', 0)))
            self.const_table.setItem(row, 3, self.format_number_item(row_data.get('今日漲幅(%)', 0), True))
            self.const_table.setItem(row, 4, self.format_number_item(row_data.get('5日漲幅(%)', 0), True))
            self.const_table.setItem(row, 5, self.format_number_item(row_data.get('法人5日增(%)', 0), True))
            self.const_table.setItem(row, 6, self.format_number_item(row_data.get('融資5日增(%)', 0), True))
            self.const_table.setItem(row, 7, self.format_number_item(row_data.get('RS強度', 0)))

            tags = str(row_data.get('強勢特徵標籤', ''))
            tag_item = NumericItem(tags)
            tag_item.setData(Qt.ItemDataRole.UserRole, 0)
            tag_item.setToolTip(tags.replace(",", "\n"))
            self.const_table.setItem(row, 8, tag_item)

            self.const_table.setItem(row, 9, self.format_number_item(row_data.get('30W距離', 99)))
            self.const_table.setItem(row, 10, self.format_number_item(row_data.get('ST距離', 99)))

        self.const_table.setSortingEnabled(True)
        self.const_table.sortItems(7, Qt.SortOrder.DescendingOrder)

    def on_quick_filter_changed(self, text):
        if "⚡" in text:
            self.txt_search_const.clear()
        else:
            self.txt_search_const.setText(text)

    def filter_constituents(self, text):
        text = text.lower()
        is_30w_special = "30w剛起漲" in text
        is_st_special = "st剛轉多" in text

        for i in range(self.const_table.rowCount()):
            match = False

            if is_30w_special:
                w30_item = self.const_table.item(i, 9)
                tag_item = self.const_table.item(i, 8)
                if w30_item and tag_item:
                    dist = float(w30_item.data(Qt.ItemDataRole.UserRole))
                    # 修正：距離必須介於 0~3 (排除負數異常值)，且文字標籤內確實包含 30w
                    if 0 <= dist <= 3 and '30w' in tag_item.text().lower():
                        match = True
            elif is_st_special:
                st_item = self.const_table.item(i, 10)
                tag_item = self.const_table.item(i, 8)
                if st_item and tag_item:
                    dist = float(st_item.data(Qt.ItemDataRole.UserRole))
                    # 修正：距離必須介於 0~3，且文字標籤內確實包含 st
                    if 0 <= dist <= 3 and 'st' in tag_item.text().lower():
                        match = True
            elif text == "" or "-- ⚡" in text:
                match = True
            else:
                # 修正：擴增其他過濾條件的模糊比對，避免下拉名稱與實際 Tag 略有出入
                search_keywords = [text]
                if "投信認養" in text:
                    search_keywords = ["投信"]
                elif "突破30週" in text:
                    search_keywords = ["30w突破", "突破30w", "30w黏貼後突破"]
                elif "主力掃單" in text:
                    search_keywords = ["ilss", "主力"]

                for col in [0, 1, 8]:
                    item = self.const_table.item(i, col)
                    if item:
                        item_text = item.text().lower()
                        if any(kw in item_text for kw in search_keywords):
                            match = True
                            break

            self.const_table.setRowHidden(i, not match)

    def on_stock_double_clicked(self, row, col):
        sid_item = self.const_table.item(row, 0)
        if sid_item:
            sid = sid_item.text()

            market = "TW"
            base_cache_path = self.project_root / "data" / "cache" / "tw"
            path_two = base_cache_path / f"{sid}_TWO.parquet"
            if path_two.exists():
                market = "TWO"

            formatted_sid = f"{sid}_{market}"
            self.stock_double_clicked.emit(formatted_sid)