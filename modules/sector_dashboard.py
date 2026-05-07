# 檔案路徑: modules/sector_dashboard.py
import os
import json
from utils.scoring.l3_score import L3Scorer
import pandas as pd
from pathlib import Path
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QSplitter,
                             QTableWidget, QTableWidgetItem, QHeaderView,
                             QGroupBox, QLabel, QAbstractItemView, QLineEdit, QComboBox,
                             QStyledItemDelegate, QStyle)
from PyQt6.QtGui import QColor, QBrush, QPainter
from PyQt6.QtCore import Qt, pyqtSignal


# 👇 新增：負責繪製儲存格背景進度條的 Delegate
class DataBarDelegate(QStyledItemDelegate):
    def __init__(self, color_hex, parent=None):
        super().__init__(parent)
        self.bar_color = QColor(color_hex)
        # 設定微光透明度，避免遮擋文字，在黑底更具質感
        self.bar_color.setAlpha(60)

    def paint(self, painter, option, index):
        painter.save()

        # 1. 繪製被選中時的底層高光色，保留原生選取體驗
        if option.state & QStyle.StateFlag.State_Selected:
            painter.fillRect(option.rect, QColor('#004466'))

        # 2. 讀取真實數值 (UserRole) 並繪製橫向背景進度條
        try:
            val = float(index.data(Qt.ItemDataRole.UserRole))
            pct = max(0.0, min(100.0, val)) / 100.0
            if pct > 0:
                from PyQt6.QtCore import QRect
                bar_rect = QRect(option.rect.x(), option.rect.y(), int(option.rect.width() * pct), option.rect.height())
                painter.fillRect(bar_rect, self.bar_color)
        except (ValueError, TypeError):
            pass

        painter.restore()

        # 3. 呼叫父類方法繪製文字與既有樣式
        # 關鍵技巧：清空預設背景筆刷，避免父類將我們剛畫好的進度條蓋掉
        option.backgroundBrush = QBrush(Qt.GlobalColor.transparent)
        super().paint(painter, option, index)

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
        # 🔥 關鍵修復 1：必須先清空舊的分類與成分股快取，否則重新載入會發生新舊資料重疊
        self.sector_members = {}
        self.sector_types = {}

        try:
            self._last_snapshot_mtime = self.snapshot_path.stat().st_mtime if self.snapshot_path.exists() else 0
        except:
            self._last_snapshot_mtime = 0

        if self.snapshot_path.exists():
            try:
                # 🔥 強制清除舊有記憶體參照
                self.snapshot_df = None
                # 🔥 關鍵修復 2：加上 engine='pyarrow', memory_map=False，拒絕作業系統的記憶體殘影，強制從硬碟重讀
                self.snapshot_df = pd.read_parquet(self.snapshot_path, engine='pyarrow', memory_map=False).copy()

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
                print(f"Error loading snapshot: {e}")

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
                        color: #FFD700;
                        font-size: 15px;
                    }
                    /* 🔥 新增：強制將所有懸停提示改為黑底與暗色邊框 */
                    QToolTip {
                        background-color: #1A1A1A;
                        color: #E0E0E0;
                        border: 1px solid #555555;
                        font-family: Arial, sans-serif;
                    }
                """)
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(5, 5, 5, 5)
        self.main_splitter = QSplitter(Qt.Orientation.Horizontal)

        self.left_group = QGroupBox("🔥 板塊熱度龍虎榜")
        left_layout = QVBoxLayout()

        sector_search_layout = QHBoxLayout()
        sector_search_layout.setContentsMargins(0, 0, 0, 5)

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
            "🚀 營收資金雙殺 (全面啟動)",
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

        # 👇 更新：型態移至檔數前，總計 11 個欄位
        cols = ['評級', '分類', '板塊名稱', '法人擴散', '營收廣度', 'YoY加速', '5日(%)', '今日(%)', '量比', '型態',
                '檔數']
        self.sector_table = self.create_styled_table(cols)

        # 👇 新增：加入詳細的表頭 Hover Hint (Tooltip)
        tips = {
            0: "綜合評分：\n👑 王者：法人/營收高擴散且YoY加速中\n⚡ 爆發：YoY營收暴力加速\n🔥 增溫：法人或營收具備高擴散性",
            3: "法人擴散率：\n該板塊內【近5日法人呈現淨買超】的成分股比例",
            4: "營收廣度：\n該板塊內【近一月營收年增(YoY) > 0】的成分股比例",
            5: "YoY 加速：\n該板塊內【近一月YoY > 近三月YoY】(成長加速) 的成分股比例",
            6: "5日(%)：\n所有成分股5日漲幅的簡單平均\n(反映族群廣度，不受大市值股主導)",
            7: "今日(%)：\n所有成分股今日漲幅的簡單平均",
            8: "量比：\n今日族群總成交量 / 近5日平均成交量\n(量比 > 1.2 倍會顯示黃色高亮)"
        }
        for col_idx, tip_text in tips.items():
            self.sector_table.horizontalHeaderItem(col_idx).setToolTip(tip_text)

        # 👇 為今日%與5日%欄位加上 Tooltip，說明採用等權平均算法
        _eq_tip = "等權平均漲幅：所有成分股漲幅的簡單平均\n反映族群廣度，不受高價股或大市值股主導\n（K 線圖仍使用市值加權合成）"
        self.sector_table.horizontalHeaderItem(6).setToolTip(_eq_tip)
        self.sector_table.horizontalHeaderItem(7).setToolTip(_eq_tip)

        # 👇 修正 Data Bar 綁定欄位 (平移至 3 與 4)
        self.legal_delegate = DataBarDelegate('#00E5FF', self.sector_table)
        self.rev_delegate = DataBarDelegate('#FF9800', self.sector_table)
        self.sector_table.setItemDelegateForColumn(3, self.legal_delegate)
        self.sector_table.setItemDelegateForColumn(4, self.rev_delegate)

        self.sector_table.itemSelectionChanged.connect(self.on_sector_selected)
        left_layout.addWidget(self.sector_table)
        self.left_group.setLayout(left_layout)

        self.right_splitter = QSplitter(Qt.Orientation.Vertical)
        # 🔥 新增：L2 族群健康度面板 (4 燈號)
        self.l2_group = QGroupBox("🩺 Layer 2 族群健康度 (族群中位數)")
        self.l2_group.setFixedHeight(95)
        self.l2_layout = QHBoxLayout()
        self.l2_labels = {}

        headers = ["訂單能見度\n(合約負債季增)", "盈利加速\n(EPS QoQ)", "法人資金\n(近5日淨買)",
                   "籌碼健康\n(近5日融資)"]
        for title in headers:
            lbl = QLabel(f"{title}\n-")
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            lbl.setStyleSheet(
                "background-color: #1A1A1A; color: #E0E0E0; border: 1px solid #444; border-radius: 6px; padding: 4px; font-size: 13px;")
            self.l2_layout.addWidget(lbl)
            self.l2_labels[title.split('\n')[0]] = lbl

        self.l2_group.setLayout(self.l2_layout)
        # 🔥 新增結束

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

        # 增加 L3Score 與黑馬欄位
        cols_const = ['代號', '名稱', '收盤價', '今日(%)', '5日(%)', '法人5日增(%)', '融資5日增(%)', 'RS強度',
                      '強勢特徵', '30W距離', 'ST距離', 'L3Score', '黑馬']
        self.const_table = self.create_styled_table(cols_const)
        self.const_table.cellDoubleClicked.connect(self.on_stock_double_clicked)

        # 針對 L3Score (Col 11) 掛上深紫色進度條視覺
        self.l3_delegate = DataBarDelegate('#7B1FA2', self.const_table)
        self.const_table.setItemDelegateForColumn(11, self.l3_delegate)

        # 🔥 新增：強制固定並加寬 L3Score 與黑馬欄位，避免文字被擠壓
        self.const_table.horizontalHeader().setSectionResizeMode(11, QHeaderView.ResizeMode.Fixed)
        self.const_table.setColumnWidth(11, 100)  # 給予 100px 寬裕空間
        self.const_table.horizontalHeader().setSectionResizeMode(12, QHeaderView.ResizeMode.Fixed)
        self.const_table.setColumnWidth(12, 50)

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
        self.right_splitter.addWidget(self.l2_group)  # 🔥 加入 L2 面板
        self.right_splitter.addWidget(self.kline_group)
        self.right_splitter.addWidget(self.constituents_group)
        self.right_splitter.setSizes([95, 450, 450])  # 調整比例

    def reload_dashboard(self):
        """🔄 重新載入硬碟最新數據並更新 UI (提供給外部或頁籤切換時呼叫)"""
        import gc

        # 🔥 關鍵修復 3：強制切斷舊 DataFrame 的關聯並呼叫系統回收記憶體，徹底解開檔案鎖
        self.snapshot_df = None
        gc.collect()

        # 1. 重新從硬碟讀取最新鮮的大表與分類資料
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
                # 🔥 加上 memory_map=False
                df = pd.read_parquet(file_path, engine='pyarrow', memory_map=False).copy()
                if len(df) < 6: continue

                close_today = df['adj_close'].iloc[-1]
                close_1d = df['adj_close'].iloc[-2]
                close_5d = df['adj_close'].iloc[-6]

                # K 線圖仍使用加權合成價格（保留原始邏輯，供技術分析用）
                # 左側龍虎榜改用等權平均漲幅，反映族群廣度而非大市值主導
                if 'Equal_Pct_1d' in df.columns and df['Equal_Pct_1d'].iloc[-1] != 0.0:
                    pct_1d = float(df['Equal_Pct_1d'].iloc[-1])
                else:
                    # 等權欄位不存在（舊版 parquet）時，退回加權算法並標記
                    pct_1d = ((close_today - close_1d) / close_1d) * 100

                if 'Equal_Pct_5d' in df.columns and df['Equal_Pct_5d'].iloc[-1] != 0.0:
                    pct_5d = float(df['Equal_Pct_5d'].iloc[-1])
                else:
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

                # =========================================================
                # 🚀 板塊專屬：動態不完整週校準 (Hybrid Rolling Patch)
                # 確保板塊龍虎榜的 🔥30W 與 🔥ST 在星期一/二也能敏銳觸發
                # =========================================================
                df_w_live = df_w.copy()
                if len(df) >= 5 and len(df_w_live) >= 1:
                    last_idx = df_w_live.index[-1]
                    last_5d_vol = df['volume'].tail(5).sum()

                    # 僅當本週累積量小於近 5 日總量時，才進行量能修補
                    if df_w_live.at[last_idx, 'Volume'] < last_5d_vol:
                        df_w_live.at[last_idx, 'Volume'] = last_5d_vol

                if len(df_w) > 30:
                    df_w_live['MA30'] = df_w_live['Close'].rolling(30).mean()

                    try:
                        from utils.strategies.technical import TechnicalStrategies
                        # 🚀 修正 3：讓左側清單也使用嚴格版判定 (並開啟 is_sector=True 放寬門檻)
                        sig_df = TechnicalStrategies.analyze_30w_breakout_details(df_w_live, is_sector=True)
                        if (sig_df['Signal'].iloc[-3:] > 0).any():
                            is_30w_break = True

                        st_df = TechnicalStrategies.calculate_supertrend(df_w_live)
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

                legal_diff = df['Legal_Diffusion'].iloc[-1] if 'Legal_Diffusion' in df.columns else 0.0
                rev_diff = df['Rev_Diffusion'].iloc[-1] if 'Rev_Diffusion' in df.columns else 0.0
                yoy_accel = df['YoY_Accel'].iloc[-1] if 'YoY_Accel' in df.columns else 0.0

                # --- 欄位 0: AI 熱力評級燈號 (新增 Hint) ---
                rating = ""
                if legal_diff >= 80 and rev_diff >= 80 and yoy_accel > 0:
                    rating = "👑"
                elif yoy_accel > 15:
                    rating = "⚡"
                elif legal_diff >= 80 or rev_diff >= 80:
                    rating = "🔥"

                rating_item = NumericItem(rating)
                rating_item.setData(Qt.ItemDataRole.UserRole, 0)
                rating_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                # 👇 新增 Hint 說明
                rating_item.setToolTip(
                    "👑: 法人與營收 >= 80% 且 YoY加速 > 0\n⚡: YoY加速極度暴力 (> 15%)\n🔥: 法人或營收 >= 80%")
                self.sector_table.setItem(row, 0, rating_item)

                # --- 欄位 1: 分類 ---
                sector_type = self.sector_types.get(sector_name, "未知")
                type_item = NumericItem(sector_type)
                type_item.setData(Qt.ItemDataRole.UserRole, 0)
                if sector_type == '細產業':
                    type_item.setForeground(QBrush(QColor('#00E5FF')))
                else:
                    type_item.setForeground(QBrush(QColor('#FF9800')))
                self.sector_table.setItem(row, 1, type_item)

                # --- 欄位 2: 板塊名稱 ---
                name_item = NumericItem(sector_name)
                name_item.setData(Qt.ItemDataRole.UserRole, 0)
                self.sector_table.setItem(row, 2, name_item)

                # --- 欄位 3 ~ 5: 核心高階數據 (修正字體亮度並加粗) ---
                legal_item = self.format_number_item(legal_diff, True)
                legal_item.setForeground(QBrush(QColor('#FFFFFF')))  # 強制亮白
                font = legal_item.font()
                font.setBold(True)
                legal_item.setFont(font)
                self.sector_table.setItem(row, 3, legal_item)

                rev_item = self.format_number_item(rev_diff, True)
                rev_item.setForeground(QBrush(QColor('#FFFFFF')))  # 強制亮白
                rev_item.setFont(font)
                self.sector_table.setItem(row, 4, rev_item)

                accel_item = self.format_number_item(yoy_accel, True)
                if yoy_accel > 0:
                    accel_item.setForeground(QBrush(QColor('#FF5252')))
                elif yoy_accel < 0:
                    accel_item.setForeground(QBrush(QColor('#4CAF50')))
                self.sector_table.setItem(row, 5, accel_item)

                # --- 欄位 6 ~ 8: 價量數據 ---
                self.sector_table.setItem(row, 6, self.format_number_item(pct_5d, True))
                self.sector_table.setItem(row, 7, self.format_number_item(pct_1d, True))

                ratio_item = self.format_number_item(vol_ratio, False)
                if vol_ratio >= 1.2: ratio_item.setForeground(QBrush(QColor('#FFD700')))
                self.sector_table.setItem(row, 8, ratio_item)

                # --- 欄位 9: 型態 (搬移至此) ---
                tag_item = NumericItem(tag_str)
                tag_item.setData(Qt.ItemDataRole.UserRole, 0)
                tag_item.setForeground(QBrush(QColor('#FFD700')))
                self.sector_table.setItem(row, 9, tag_item)

                # --- 欄位 10: 檔數 ---
                count_item = self.format_number_item(member_count, False)
                count_item.setText(str(member_count))
                self.sector_table.setItem(row, 10, count_item)
            except Exception as e:
                pass

        self.sector_table.setSortingEnabled(True)
        # 5日漲幅現在位於 Index 6
        self.sector_table.sortItems(6, Qt.SortOrder.DescendingOrder)

    def filter_sectors(self, *args):
        search_text = self.txt_search_sector.text().lower()
        type_filter = self.combo_sector_type.currentText()
        quick_filter = self.combo_sector_filter.currentText()

        for i in range(self.sector_table.rowCount()):
            match_search = False
            match_type = False
            match_quick = True

            # 1. 檢查分類是否符合 (欄位 1)
            type_item = self.sector_table.item(i, 1)
            if type_filter == "全部板塊" or (type_item and type_item.text() == type_filter):
                match_type = True

            # 2. 檢查關鍵字是否符合 (名稱改為 2, 型態改為 9)
            if not search_text:
                match_search = True
            else:
                for col in [2, 9]:
                    item = self.sector_table.item(i, col)
                    if item and search_text in item.text().lower():
                        match_search = True
                        break

            # 3. 檢查高階數值過濾 (欄位再度平移對應)
            if quick_filter == "🚀 營收資金雙殺 (全面啟動)":
                legal_val = float(self.sector_table.item(i, 3).data(Qt.ItemDataRole.UserRole))
                rev_val = float(self.sector_table.item(i, 4).data(Qt.ItemDataRole.UserRole))
                accel_val = float(self.sector_table.item(i, 5).data(Qt.ItemDataRole.UserRole))
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

            self.sector_table.setRowHidden(i, not (match_search and match_type and match_quick))

    def on_sector_selected(self):
        selected_items = self.sector_table.selectedItems()
        if not selected_items: return
        row = selected_items[0].row()

        # 👇 板塊名稱欄位已平移至 Index 2
        sector_name = self.sector_table.item(row, 2).text()
        self.update_l2_panel(sector_name)
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

            # 🔥 加上 memory_map=False
            df = pd.read_parquet(file_path, engine='pyarrow', memory_map=False).copy()
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

    def update_l2_panel(self, sector_name):
        """即時掃描板塊所有成分股 JSON，聚合 L2 族群健康度指標 (修正數學邏輯與欄位精準度)"""
        sids = self.sector_members.get(sector_name, set())
        contract_liabs, eps_qoqs, inst_5d_sums, margin_5d_sums = [], [], [], []

        print(f"\n🕵️ [L2 面板] 開始掃描 {sector_name} ({len(sids)} 檔)...")

        for sid in sids:
            json_path = self.project_root / "data" / "fundamentals" / f"{sid}.json"
            if not json_path.exists(): continue
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # 1. 訂單能見度 (合約負債季增)
                bs_list = data.get('balance_sheet', [])
                if bs_list:
                    df = pd.DataFrame(bs_list)
                    t_col = 'quarter' if 'quarter' in df.columns else ('date' if 'date' in df.columns else None)
                    if t_col and 'contract_liab' in df.columns:
                        df = df.sort_values(t_col).dropna(subset=['contract_liab'])
                        if len(df) >= 2:
                            curr = float(df['contract_liab'].iloc[-1])
                            prev = float(df['contract_liab'].iloc[-2])
                            if prev != 0:  # 加上 abs() 修正負數基期的變化率問題
                                contract_liabs.append((curr - prev) / abs(prev) * 100)

                # 2. 盈利加速 (EPS QoQ)
                prof_list = data.get('profitability', [])
                if prof_list:
                    df = pd.DataFrame(prof_list)
                    t_col = 'quarter' if 'quarter' in df.columns else ('date' if 'date' in df.columns else None)
                    if t_col and 'eps' in df.columns:
                        df = df.sort_values(t_col).dropna(subset=['eps'])
                        if len(df) >= 2:
                            curr = float(df['eps'].iloc[-1])
                            prev = float(df['eps'].iloc[-2])
                            if prev != 0:  # 加上 abs() 確保虧損收斂時會算出正的成長率
                                eps_qoqs.append((curr - prev) / abs(prev) * 100)

                # 3. 法人資金 (近5日淨買) - 直接抓 total_buy_sell 最準確
                inst_list = data.get('institutional_investors', [])
                if inst_list:
                    df = pd.DataFrame(inst_list)
                    if 'date' in df.columns and 'total_buy_sell' in df.columns:
                        df = df.sort_values('date')
                        # 強制轉為數值後加總最後 5 筆
                        inst_5d_sums.append(pd.to_numeric(df['total_buy_sell'], errors='coerce').tail(5).sum())

                # 4. 籌碼健康 (近5日融資增減) - 直接抓 fin_change 最準確
                margin_list = data.get('margin_trading', [])
                if margin_list:
                    df = pd.DataFrame(margin_list)
                    if 'date' in df.columns and 'fin_change' in df.columns:
                        df = df.sort_values('date')
                        margin_5d_sums.append(pd.to_numeric(df['fin_change'], errors='coerce').tail(5).sum())

            except Exception as e:
                print(f"   ❌ {sid} 解析錯誤: {e}")

        # 計算中位數並更新 UI (過濾掉異常值)
        import numpy as np
        m_contract = np.nanmedian(contract_liabs) if contract_liabs else np.nan
        m_eps = np.nanmedian(eps_qoqs) if eps_qoqs else np.nan
        m_inst = np.nanmedian(inst_5d_sums) if inst_5d_sums else np.nan
        m_margin = np.nanmedian(margin_5d_sums) if margin_5d_sums else np.nan

        # 呼叫 set_l2_card (傳入明確的好壞門檻)
        self.set_l2_card("訂單能見度", "訂單能見度\n(合約負債季增)", m_contract, is_pct=True, good_thresh=5.0,
                         bad_thresh=-5.0, lower_is_better=False)
        self.set_l2_card("盈利加速", "盈利加速\n(EPS QoQ)", m_eps, is_pct=True, good_thresh=10.0, bad_thresh=-10.0,
                         lower_is_better=False)
        self.set_l2_card("法人資金", "法人資金\n(近5日中位數)", m_inst, is_pct=False, good_thresh=500, bad_thresh=-500,
                         lower_is_better=False)
        # 融資是籌碼，越低(大減)越好，因此 lower_is_better=True
        self.set_l2_card("籌碼健康", "籌碼健康\n(融資5日中位數)", m_margin, is_pct=False, good_thresh=-100,
                         bad_thresh=500, lower_is_better=True)

    def set_l2_card(self, key, title, val, is_pct, good_thresh, bad_thresh, lower_is_better=False):
        """控制 L2 燈號與文字顏色，強制執行台股邏輯(紅好綠壞)與暗黑高亮對比"""
        lbl = self.l2_labels.get(key)
        if not lbl: return

        # 無資料防呆，使用灰色
        if pd.isna(val):
            lbl.setText(f"{title}\n無資料")
            lbl.setStyleSheet("background-color: #1A1A1A; color: #888888; border: 1px solid #444; border-radius: 6px;")
            return

        val_str = f"{val:+.1f}%" if is_pct else f"{int(val):+d} 張"

        # 預設中性色 (亮灰色)
        color = "#E0E0E0"
        border = "#555555"

        # 判斷邏輯：lower_is_better 專門用來處理「融資」這種越低越好的數據
        is_good = False
        is_bad = False

        if lower_is_better:
            is_good = val <= good_thresh  # 融資大減 -> 好
            is_bad = val >= bad_thresh  # 融資大增 -> 壞
        else:
            is_good = val >= good_thresh  # EPS/法人買超大增 -> 好
            is_bad = val <= bad_thresh  # EPS/法人買超大減 -> 壞

        # 上色 (採用高亮度螢光紅與螢光綠，解決暗底看不清的問題)
        if is_good:
            color = "#FF4D4D"  # 亮紅色 (表示多頭/健康)
            border = "#D32F2F"
        elif is_bad:
            color = "#00E676"  # 亮綠色 (表示空頭/不健康)
            border = "#2E7D32"

        lbl.setText(f"{title}\n{val_str}")
        lbl.setStyleSheet(
            f"background-color: #1A1A1A; color: {color}; border: 1px solid {border}; "
            f"border-radius: 6px; font-weight: bold; font-size: 13px;"
        )

    def update_constituents_table(self, sector_name):
        # 🔥 新增防呆：即時檢查硬碟大表時間戳。若外部腳本剛算完，UI 應立即刷新記憶體，實現無縫接軌！
        try:
            current_mtime = self.snapshot_path.stat().st_mtime if self.snapshot_path.exists() else 0
            last_mtime = getattr(self, '_last_snapshot_mtime', 0)
            if current_mtime > last_mtime or self.snapshot_df is None:
                self.init_data()
        except Exception:
            pass

        self.const_table.setSortingEnabled(False)
        self.const_table.setRowCount(0)

        if self.snapshot_df is None or self.snapshot_df.empty: return

        sids = self.sector_members.get(sector_name, set())
        filtered_df = self.snapshot_df[self.snapshot_df['股票代號'].isin(sids)]

        for _, row_data in filtered_df.iterrows():
            row = self.const_table.rowCount()
            self.const_table.insertRow(row)

            sid = str(row_data.get('股票代號', ''))

            # --- 1. 原有基礎欄位寫入 ---
            sid_item = NumericItem(sid)
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

            rs_val = row_data.get('RS強度', 0)
            self.const_table.setItem(row, 7, self.format_number_item(rs_val))

            tags = str(row_data.get('強勢特徵標籤', ''))
            tag_item = NumericItem(tags)
            tag_item.setData(Qt.ItemDataRole.UserRole, 0)
            tag_item.setToolTip(tags.replace(",", "\n"))
            self.const_table.setItem(row, 8, tag_item)

            self.const_table.setItem(row, 9, self.format_number_item(row_data.get('30W距離', 99)))
            self.const_table.setItem(row, 10, self.format_number_item(row_data.get('ST距離', 99)))

            # --- 2. 即時載入該股的 JSON 與 K 線資料供 L3 評分使用 ---
            fundamental_data = {}
            json_path = self.project_root / "data" / "fundamentals" / f"{sid}.json"
            if json_path.exists():
                try:
                    with open(json_path, 'r', encoding='utf-8') as f:
                        fundamental_data = json.load(f)
                except:
                    pass

            kline_df = pd.DataFrame()
            parquet_tw = self.project_root / "data" / "cache" / "tw" / f"{sid}_TW.parquet"
            parquet_two = self.project_root / "data" / "cache" / "tw" / f"{sid}_TWO.parquet"
            kline_path = parquet_tw if parquet_tw.exists() else (parquet_two if parquet_two.exists() else None)

            if kline_path:
                try:
                    # 🔥 加上 memory_map=False
                    kline_df = pd.read_parquet(kline_path, engine='pyarrow', memory_map=False).copy()
                    kline_df.columns = [str(c).lower() for c in kline_df.columns]
                    if isinstance(kline_df.index, pd.DatetimeIndex):
                        kline_df['date'] = kline_df.index
                    elif 'date' not in kline_df.columns and 'timestamp' in kline_df.columns:
                        kline_df['date'] = pd.to_datetime(kline_df['timestamp'], unit='ms')
                except:
                    pass

            # --- 3. 計算 L3Score 與 黑馬標記 ---
            # --- 3. 計算 L3Score 與 黑馬標記 ---
            l3_score_val = 0.0
            is_dark_horse = False
            tooltip_html = ""
            name_str = str(row_data.get('股票名稱', ''))

            if not kline_df.empty and fundamental_data:
                # 預設抓取 snapshot 的值，若無則為 50.0
                boll_width = row_data.get('布林寬度(%)', 50.0)

                # 👇👇👇 新增：呼叫 technical 即時算出真實的布林寬度
                try:
                    from utils.strategies.technical import TechnicalStrategies
                    bb_df = TechnicalStrategies.calculate_bollinger_bands(kline_df, window=20)
                    if not bb_df.empty and not pd.isna(bb_df['BB_Width_Pct'].iloc[-1]):
                        boll_width = float(bb_df['BB_Width_Pct'].iloc[-1])
                except Exception as e:
                    pass
                # 👆👆👆 新增結束

                try:
                    # 將剛剛算出的 boll_width 傳入 L3Scorer
                    l3_result = L3Scorer.calculate_score(float(rs_val), fundamental_data, kline_df, tags,
                                                         float(boll_width))
                    l3_score_val = l3_result.get('L3Score', 0.0)
                    is_dark_horse = l3_result.get('is_dark_horse', False)

                    # 💎 萃取 V3 核心資訊組成 HTML Tooltip (這裡維持你原本的完美設計)
                    vwap = l3_result.get('vwap_info', {})
                    rev = l3_result.get('rev_info', {})
                    prof = l3_result.get('prof_info', {})

                    tooltip_html = f"""
                                    <div style='font-family: Arial, sans-serif; font-size: 13px; color: #E0E0E0;'>
                                        <b style='color: #FFD700; font-size: 14px;'>{sid} {name_str}</b> | L3Score: <b>{l3_score_val}</b> {'★黑馬' if is_dark_horse else ''}<br>
                                        <hr style='background-color: #555; height: 1px; border: none; margin: 4px 0;'>
                                        <b style='color: #00E5FF;'>[法人成本]</b> {vwap.get('status', '')}<br>
                                        均價: {vwap.get('vwap', 0)} | 乖離率: <b style='color: #FF5252;'>{vwap.get('bias_pct', 0)}%</b><br>
                                        <br>
                                        <b style='color: #FF9800;'>[營收動能]</b> {rev.get('status', '')} (斜率: {rev.get('slope', 0)}%)<br>
                                        <b style='color: #69F0AE;'>[獲利純度]</b> {prof.get('status', '')} (OP季增: {prof.get('qoq', 0)}%)<br>
                                        <br>
                                        <b style='color: #E0E0E0;'>[技術特徵]</b> RS: {rs_val} | 布林: {boll_width:.2f}%
                                    </div>
                                    """
                except Exception as e:
                    pass

            # 寫入 L3Score (Col 11)
            l3_item = self.format_number_item(l3_score_val, False)
            font = l3_item.font()
            font.setBold(True)
            l3_item.setFont(font)
            l3_item.setForeground(QBrush(QColor('#FFFFFF')))
            if tooltip_html: l3_item.setToolTip(tooltip_html)
            self.const_table.setItem(row, 11, l3_item)

            # 寫入 黑馬標記 (Col 12)
            horse_str = "★" if is_dark_horse else ""
            horse_item = NumericItem(horse_str)
            horse_item.setData(Qt.ItemDataRole.UserRole, 1 if is_dark_horse else 0)
            horse_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            if is_dark_horse:
                horse_item.setForeground(QBrush(QColor('#FFD700')))
            if tooltip_html: horse_item.setToolTip(tooltip_html)
            self.const_table.setItem(row, 12, horse_item)

            if tooltip_html:
                sid_item.setToolTip(tooltip_html)
                name_item.setToolTip(tooltip_html)

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