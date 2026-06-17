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


# 👇 負責繪製儲存格背景進度條的 Delegate
class DataBarDelegate(QStyledItemDelegate):
    def __init__(self, color_hex, parent=None):
        super().__init__(parent)
        self.bar_color = QColor(color_hex)
        # 設定微光透明度，避免遮擋文字，在黑底更具質感
        self.bar_color.setAlpha(60)

    def paint(self, painter, option, index):
        painter.save()

        # 1. 繪製被選中時的底層高光色，保留原生選取体验
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

        # 背景執行緒與進度條物件參照控制
        self.worker = None
        self.progress_dialog = None
        self.global_worker = None
        self.global_progress = None

        self.init_data()
        self.init_ui()
        self.load_sectors_to_table()

    def init_data(self):
        # 必須先清空舊的分類與成分股快取，否則重新載入會發生新舊資料重疊
        self.sector_members = {}
        self.sector_types = {}
        self.all_l3_precomputed = False
        self.precomputed_dark_horse_sids = set()

        try:
            self._last_snapshot_mtime = self.snapshot_path.stat().st_mtime if self.snapshot_path.exists() else 0
        except:
            self._last_snapshot_mtime = 0

        if self.snapshot_path.exists():
            try:
                # 強制清除舊有記憶體參照
                self.snapshot_df = None
                # 加上 engine='pyarrow', memory_map=False，拒絕作業系統的記憶體殘影，強制從硬碟重讀
                self.snapshot_df = pd.read_parquet(self.snapshot_path, engine='pyarrow', memory_map=False).copy()

                #print("【驗證】當前策略大表的所有欄位:", self.snapshot_df.columns.tolist())

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
            "📈 營收加速爆發",
            "⭐ 內含黑馬特選股"
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

        cols = ['評級', '分類', '板塊名稱', '法人擴散', '營收廣度', 'YoY加速', '5日(%)', '今日(%)', '量比', '型態',
                '檔數']
        self.sector_table = self.create_styled_table(cols)

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

        _eq_tip = "等權平均漲幅：所有成分股漲幅的簡單平均\n反映族群廣度，不受高價股或大市值股主導\n（K 線圖仍使用市值加權合成）"
        self.sector_table.horizontalHeaderItem(6).setToolTip(_eq_tip)
        self.sector_table.horizontalHeaderItem(7).setToolTip(_eq_tip)

        self.legal_delegate = DataBarDelegate('#00E5FF', self.sector_table)
        self.rev_delegate = DataBarDelegate('#FF9800', self.sector_table)
        self.sector_table.setItemDelegateForColumn(3, self.legal_delegate)
        self.sector_table.setItemDelegateForColumn(4, self.rev_delegate)

        self.sector_table.itemSelectionChanged.connect(self.on_sector_selected)
        left_layout.addWidget(self.sector_table)
        self.left_group.setLayout(left_layout)

        self.right_splitter = QSplitter(Qt.Orientation.Vertical)
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

        cols_const = ['代號', '名稱', '收盤價', '今日(%)', '5日(%)', '法人5日增(%)', '融資5日增(%)', 'RS強度',
                      '強勢特徵', '30W距離', 'ST距離', 'L3Score', '黑馬']
        self.const_table = self.create_styled_table(cols_const)
        self.const_table.cellDoubleClicked.connect(self.on_stock_double_clicked)

        self.l3_delegate = DataBarDelegate('#7B1FA2', self.const_table)
        self.const_table.setItemDelegateForColumn(11, self.l3_delegate)

        self.const_table.horizontalHeader().setSectionResizeMode(11, QHeaderView.ResizeMode.Fixed)
        self.const_table.setColumnWidth(11, 100)
        self.const_table.horizontalHeader().setSectionResizeMode(12, QHeaderView.ResizeMode.Fixed)
        self.const_table.setColumnWidth(12, 50)

        self.const_table.horizontalHeader().setSectionResizeMode(8, QHeaderView.ResizeMode.Stretch)
        self.const_table.setColumnHidden(9, True)
        self.const_table.setColumnHidden(10, True)

        # 👇 請在此處插入以下 10 行程式碼 👇
        const_tips = {
            3: "今日(%)：今日收盤漲跌幅",
            4: "5日(%)：近 5 個交易日累積漲跌幅",
            5: "法人5日增(%)：近 5 日三大法人買賣超佔股本比例",
            6: "融資5日增(%)：近 5 日融資增減佔股本比例",
            11: "L3Score：結合技術面、籌碼面、基本面的 L3 綜合評分",
            12: "黑馬：符合籌碼沉澱、營收爆發、剛起漲等條件的特選股"
        }
        for col_idx, tip_text in const_tips.items():
            item = self.const_table.horizontalHeaderItem(col_idx)
            if item:
                item.setToolTip(tip_text)
        # 👆 插入結束 👆

        constituents_layout.addWidget(self.const_table)
        self.constituents_group.setLayout(constituents_layout)

        self.right_splitter.addWidget(self.kline_group)
        self.right_splitter.addWidget(self.constituents_group)
        self.right_splitter.setSizes([550, 450])

        self.main_splitter.addWidget(self.left_group)
        self.main_splitter.addWidget(self.right_splitter)
        self.main_splitter.setSizes([380, 820])

        main_layout.addWidget(self.main_splitter)
        self.right_splitter.addWidget(self.l2_group)
        self.right_splitter.addWidget(self.kline_group)
        self.right_splitter.addWidget(self.constituents_group)
        self.right_splitter.setSizes([95, 450, 450])

    def reload_dashboard(self):
        """🔄 重新載入硬碟最新數據並更新 UI"""
        import gc

        self.snapshot_df = None
        gc.collect()

        self.init_data()
        self.load_sectors_to_table()

        if hasattr(self, 'kline_widget') and self.kline_widget is not None:
            self.kline_widget.setParent(None)
            self.kline_widget.deleteLater()
            self.kline_widget = None

        for i in reversed(range(self.kline_layout.count())):
            widget = self.kline_layout.itemAt(i).widget()
            if widget is not None:
                widget.setParent(None)
                widget.deleteLater()

        self.kline_placeholder = QLabel("請從左側選擇板塊以載入 K 線圖...")
        self.kline_placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.kline_placeholder.setStyleSheet("color: gray; font-size: 14px;")
        self.kline_layout.addWidget(self.kline_placeholder)

        self.const_table.setRowCount(0)

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
        header.setStretchLastSection(True)

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
        self.txt_search_sector.blockSignals(True)
        if "⚡" in text:
            self.txt_search_sector.clear()
        elif "30W" in text:
            self.txt_search_sector.setText("🔥30W")
        elif "ST" in text:
            self.txt_search_sector.setText("🔥ST")
        else:
            self.txt_search_sector.clear()

        self.txt_search_sector.blockSignals(False)

        # 🚀 若選擇「內含黑馬特選股」且尚未預先運算，則啟動背景全域運算執行緒
        if text == "⭐ 內含黑馬特選股" and not getattr(self, 'all_l3_precomputed', False):
            self.run_global_l3_precomputation()
            return

        # 點擊其餘快速篩選時，立即強制使選單與左側表格反灰
        from PyQt6.QtWidgets import QApplication
        self.combo_sector_filter.setEnabled(False)

        QApplication.processEvents()

        try:
            self.filter_sectors()
        finally:
            self.combo_sector_filter.setEnabled(True)
            self.sector_table.setEnabled(True)

    def run_global_l3_precomputation(self):
        """🚀 背景並行計算所有候選成分股的 L3 評分，Non-Modal 允許使用者在分析時繼續操作其他功能"""
        if self.snapshot_df is None or self.snapshot_df.empty:
            return

        # 收集所有板塊的成分股代號聯集
        all_member_sids = set()
        for sids in self.sector_members.values():
            all_member_sids.update(sids)

        # 僅針對 RS強度 >= 50 或具有強勢標籤的個股進行深度評分，節省運算資源
        df_to_compute = self.snapshot_df[
            self.snapshot_df['股票代號'].isin(all_member_sids) &
            ((self.snapshot_df['RS強度'] >= 50) | (self.snapshot_df['強勢特徵標籤'] != ''))
        ]
        total_stocks = len(df_to_compute)

        # 1. 建立進度條 - 使用 NonModal，允許使用者繼續進行其他操作
        self.global_progress = QProgressDialog("🔍 正在全面掃描所有板塊成分股，精準篩選黃金黑馬特選股...", "取消掃描", 0, total_stocks, self)
        self.global_progress.setWindowModality(Qt.WindowModality.NonModal)
        self.global_progress.setWindowTitle("深度黑馬掃描中 (可操作其他功能)")
        self.global_progress.setStyleSheet("""
            QProgressDialog { background-color: #1E1E1E; color: #E0E0E0; border: 1px solid #444; }
            QLabel { color: #FFD700; font-size: 13px; font-weight: bold; }
            QProgressBar { border: 1px solid #555; background: #2D2D2D; text-align: center; color: #FFF; border-radius: 4px; }
            QProgressBar::chunk { background-color: #FF9800; }
            QPushButton { background: #3E3E42; color: #FFF; padding: 4px 10px; border: 1px solid #555; border-radius: 4px; }
            QPushButton:hover { background: #505054; }
        """)
        self.global_progress.show()

        # 2. 為了讓使用者操作其他功能，【不】對其他 UI 組件進行禁用限制，保持高互動性
        self.combo_sector_filter.setStyleSheet(
            "background: #111; color: #888; border: 1px solid #444; padding: 4px; font-weight: bold;")

        # 3. 啟動背景掃描執行緒
        self.global_worker = GlobalL3ComputeWorker(self.project_root, df_to_compute, self)
        self.global_progress.canceled.connect(self.on_global_worker_cancelled)

        self.global_worker.progress_updated.connect(self.on_global_worker_progress)
        self.global_worker.finished_computation.connect(self.on_global_worker_finished)
        self.global_worker.start()

    def on_global_worker_progress(self, completed, total, current_stock_name):
        if hasattr(self, 'global_progress') and self.global_progress:
            self.global_progress.setLabelText(f"⏳ 正在分析: {current_stock_name}\n({completed} / {total})")
            self.global_progress.setValue(completed)

    def on_global_worker_finished(self, dark_horse_sids):
        self.precomputed_dark_horse_sids = dark_horse_sids
        self.all_l3_precomputed = True

        #print(f"[DEBUG1] dark_horse_sids 數量: {len(dark_horse_sids)}")
        #print(f"[DEBUG2] dark_horse_sids 前5個: {list(dark_horse_sids)[:5]}")

        first_sector = next(iter(self.sector_members))
        sample_sids = list(self.sector_members[first_sector])[:5]
        #print(f"[DEBUG3] sector_members 第一個板塊 '{first_sector}' 的sid格式: {sample_sids}")

        #print(f"[DEBUG4] combo_sector_filter 目前選項: '{self.combo_sector_filter.currentText()}'")
        #print(f"[DEBUG5] sector_table rowCount: {self.sector_table.rowCount()}")

        if hasattr(self, 'global_progress') and self.global_progress:
            # ✅ 關鍵修復：先斷開 canceled 訊號再 close()
            # 原本 close() 會觸發 canceled → on_global_worker_cancelled → setCurrentIndex(0) → 黑馬選項被重置
            try:
                self.global_progress.canceled.disconnect()
            except RuntimeError:
                pass
            self.global_progress.close()
            self.global_progress = None

        self.global_worker = None

        # 恢復 UI 選單狀態
        self.combo_sector_filter.setEnabled(True)
        self.sector_table.setEnabled(True)
        self.combo_sector_filter.setStyleSheet(
            "background: #222; color: #FFD700; border: 1px solid #444; padding: 4px; font-weight: bold;")

        # ✅ 此時 combo 仍維持「⭐ 內含黑馬特選股」，filter_sectors() 會正確讀到並過濾
        self.filter_sectors()
        self.sector_table.hide()
        self.sector_table.show()

    def on_global_worker_cancelled(self):
        if hasattr(self, 'global_worker') and self.global_worker:
            self.global_worker.cancel()
            self.global_worker.wait()
            self.global_worker = None

        self.combo_sector_filter.setEnabled(True)
        self.sector_table.setEnabled(True)
        self.combo_sector_filter.setStyleSheet(
            "background: #222; color: #FFD700; border: 1px solid #444; padding: 4px; font-weight: bold;")
        self.combo_sector_filter.setCurrentIndex(0)

    def load_sectors_to_table(self):
        self.sector_table.setSortingEnabled(False)
        self.sector_table.setRowCount(0)

        if not self.sector_dir.exists(): return

        idx_files = list(self.sector_dir.glob("IDX_*.parquet"))
        for file_path in idx_files:
            sector_name = file_path.stem.replace("IDX_", "")
            try:
                df = pd.read_parquet(file_path, engine='pyarrow', memory_map=False).copy()
                if len(df) < 6: continue

                close_today = df['adj_close'].iloc[-1]
                close_1d = df['adj_close'].iloc[-2]
                close_5d = df['adj_close'].iloc[-6]

                if 'Equal_Pct_1d' in df.columns and df['Equal_Pct_1d'].iloc[-1] != 0.0:
                    pct_1d = float(df['Equal_Pct_1d'].iloc[-1])
                else:
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

                df_w_live = df_w.copy()
                if len(df) >= 5 and len(df_w_live) >= 1:
                    last_idx = df_w_live.index[-1]
                    last_5d_vol = df['volume'].tail(5).sum()

                    if df_w_live.at[last_idx, 'Volume'] < last_5d_vol:
                        df_w_live.at[last_idx, 'Volume'] = last_5d_vol

                if len(df_w) > 30:
                    df_w_live['MA30'] = df_w_live['Close'].rolling(30).mean()

                    try:
                        from utils.strategies.technical import TechnicalStrategies
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
                rating_item.setToolTip(
                    "👑: 法人與營收 >= 80% 且 YoY加速 > 0\n⚡: YoY加速極度暴力 (> 15%)\n🔥: 法人或營收 >= 80%")
                self.sector_table.setItem(row, 0, rating_item)

                sector_type = self.sector_types.get(sector_name, "未知")
                type_item = NumericItem(sector_type)
                type_item.setData(Qt.ItemDataRole.UserRole, 0)
                if sector_type == '細產業':
                    type_item.setForeground(QBrush(QColor('#00E5FF')))
                else:
                    type_item.setForeground(QBrush(QColor('#FF9800')))
                self.sector_table.setItem(row, 1, type_item)

                name_item = NumericItem(sector_name)
                name_item.setData(Qt.ItemDataRole.UserRole, 0)
                self.sector_table.setItem(row, 2, name_item)

                legal_item = self.format_number_item(legal_diff, True)
                legal_item.setForeground(QBrush(QColor('#FFFFFF')))
                font = legal_item.font()
                font.setBold(True)
                legal_item.setFont(font)
                self.sector_table.setItem(row, 3, legal_item)

                rev_item = self.format_number_item(rev_diff, True)
                rev_item.setForeground(QBrush(QColor('#FFFFFF')))
                rev_item.setFont(font)
                self.sector_table.setItem(row, 4, rev_item)

                accel_item = self.format_number_item(yoy_accel, True)
                if yoy_accel > 0:
                    accel_item.setForeground(QBrush(QColor('#FF5252')))
                elif yoy_accel < 0:
                    accel_item.setForeground(QBrush(QColor('#4CAF50')))
                self.sector_table.setItem(row, 5, accel_item)

                self.sector_table.setItem(row, 6, self.format_number_item(pct_5d, True))
                self.sector_table.setItem(row, 7, self.format_number_item(pct_1d, True))

                ratio_item = self.format_number_item(vol_ratio, False)
                if vol_ratio >= 1.2: ratio_item.setForeground(QBrush(QColor('#FFD700')))
                self.sector_table.setItem(row, 8, ratio_item)

                tag_item = NumericItem(tag_str)
                tag_item.setData(Qt.ItemDataRole.UserRole, 0)
                tag_item.setForeground(QBrush(QColor('#FFD700')))
                self.sector_table.setItem(row, 9, tag_item)

                count_item = self.format_number_item(member_count, False)
                count_item.setText(str(member_count))
                self.sector_table.setItem(row, 10, count_item)
            except Exception as e:
                pass

        self.sector_table.setSortingEnabled(True)
        self.sector_table.sortItems(6, Qt.SortOrder.DescendingOrder)

    def filter_sectors(self, *args):
        # 🛡️ 關鍵修復：解決 QTableWidget 在 SortingEnabled=True 下呼叫 setRowHidden 會失效、不自動更新的 Qt 經典 Bug
        sorting_was_enabled = self.sector_table.isSortingEnabled()
        self.sector_table.setSortingEnabled(False)

        search_text = self.txt_search_sector.text().lower()
        type_filter = self.combo_sector_type.currentText()
        quick_filter = self.combo_sector_filter.currentText()
        #print(f"[FILTER] quick_filter='{quick_filter}', all_l3_precomputed={getattr(self, 'all_l3_precomputed', False)}")

        # 優先使用背景預先算出的精準黑馬集合，若無則退回基礎標籤粗篩
        use_precise_l3 = getattr(self, 'all_l3_precomputed', False)
        dark_horse_sids = set()

        if quick_filter == "⭐ 內含黑馬特選股":
            if use_precise_l3:
                dark_horse_sids = getattr(self, 'precomputed_dark_horse_sids', set())
            elif self.snapshot_df is not None and not self.snapshot_df.empty:
                try:
                    rs_cond = self.snapshot_df['RS強度'] >= 75
                    tags_cond = self.snapshot_df['強勢特徵標籤'].str.contains("黑馬|起漲|轉多|突破|壓縮|ilss|主力|特徵", na=False, case=False)
                    dark_horse_sids = set(self.snapshot_df[rs_cond & tags_cond]['股票代號'].astype(str).tolist())
                except Exception as e:
                    pass

        hidden_count = 0
        for i in range(self.sector_table.rowCount()):
            match_search = False
            match_type = False
            match_quick = True

            type_item = self.sector_table.item(i, 1)
            if type_filter == "全部板塊" or (type_item and type_item.text() == type_filter):
                match_type = True

            if not search_text:
                match_search = True
            else:
                for col in [2, 9]:
                    item = self.sector_table.item(i, col)
                    if item and search_text in item.text().lower():
                        match_search = True
                        break

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

            elif quick_filter == "⭐ 內含黑馬特選股":
                s_name_item = self.sector_table.item(i, 2)
                if s_name_item:
                    s_name = s_name_item.text()
                    sids = self.sector_members.get(s_name, set())
                    has_dark_horse = not sids.isdisjoint(dark_horse_sids)
                    if not has_dark_horse:
                        match_quick = False

            self.sector_table.setRowHidden(i, not (match_search and match_type and match_quick))
            if not (match_search and match_type and match_quick):  # 加這兩行
                hidden_count += 1
        print(f"[FILTER] 總行數={self.sector_table.rowCount()}, hidden={hidden_count}, visible={self.sector_table.rowCount() - hidden_count}")  # 加這行
        # 恢復排序狀態
        self.sector_table.setSortingEnabled(sorting_was_enabled)

    def on_sector_selected(self):
        selected_items = self.sector_table.selectedItems()
        if not selected_items: return
        row = selected_items[0].row()

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
        sid = f"IDX_{sector_name}"
        try:
            file_path = self.sector_dir / f"{sid}.parquet"
            if not file_path.exists():
                raise ValueError(f"找不到板塊資料: {file_path}")

            df = pd.read_parquet(file_path, engine='pyarrow', memory_map=False).copy()
            df.columns = [str(c).lower() for c in df.columns]

            core_cols = ['open', 'high', 'low', 'close', 'volume']
            for c in core_cols:
                if c not in df.columns:
                    if f"adj_{c}" in df.columns:
                        df[c] = df[f"adj_{c}"]
                    else:
                        df[c] = 0.0

            for c in core_cols:
                if f"adj_{c}" not in df.columns:
                    df[f"adj_{c}"] = df[c]

            rename_map = {
                'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume',
                'adj_open': 'Adj_open', 'adj_high': 'Adj_high', 'adj_low': 'Adj_low', 'adj_close': 'Adj_close'
            }
            df = df.rename(columns=rename_map)

            if 'Dividends' not in df.columns: df['Dividends'] = 0.0
            df = df.loc[:, ~df.columns.duplicated()]

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
        sids = self.sector_members.get(sector_name, set())
        contract_liabs, eps_qoqs, inst_5d_sums, margin_5d_sums = [], [], [], []

        for sid in sids:
            json_path = self.project_root / "data" / "fundamentals" / f"{sid}.json"
            if not json_path.exists(): continue
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                bs_list = data.get('balance_sheet', [])
                if bs_list:
                    df = pd.DataFrame(bs_list)
                    t_col = 'quarter' if 'quarter' in df.columns else ('date' if 'date' in df.columns else None)
                    if t_col and 'contract_liab' in df.columns:
                        df = df.sort_values(t_col).dropna(subset=['contract_liab'])
                        if len(df) >= 2:
                            curr = float(df['contract_liab'].iloc[-1])
                            prev = float(df['contract_liab'].iloc[-2])
                            if prev != 0:
                                contract_liabs.append((curr - prev) / abs(prev) * 100)

                prof_list = data.get('profitability', [])
                if prof_list:
                    df = pd.DataFrame(prof_list)
                    t_col = 'quarter' if 'quarter' in df.columns else ('date' if 'date' in df.columns else None)
                    if t_col and 'eps' in df.columns:
                        df = df.sort_values(t_col).dropna(subset=['eps'])
                        if len(df) >= 2:
                            curr = float(df['eps'].iloc[-1])
                            prev = float(df['eps'].iloc[-2])
                            if prev != 0:
                                eps_qoqs.append((curr - prev) / abs(prev) * 100)

                inst_list = data.get('institutional_investors', [])
                if inst_list:
                    df = pd.DataFrame(inst_list)
                    if 'date' in df.columns and 'total_buy_sell' in df.columns:
                        df = df.sort_values('date')
                        inst_5d_sums.append(pd.to_numeric(df['total_buy_sell'], errors='coerce').tail(5).sum())

                margin_list = data.get('margin_trading', [])
                if margin_list:
                    df = pd.DataFrame(margin_list)
                    if 'date' in df.columns and 'fin_change' in df.columns:
                        df = df.sort_values('date')
                        margin_5d_sums.append(pd.to_numeric(df['fin_change'], errors='coerce').tail(5).sum())

            except Exception as e:
                pass

        import numpy as np
        m_contract = np.nanmedian(contract_liabs) if contract_liabs else np.nan
        m_eps = np.nanmedian(eps_qoqs) if eps_qoqs else np.nan
        m_inst = np.nanmedian(inst_5d_sums) if inst_5d_sums else np.nan
        m_margin = np.nanmedian(margin_5d_sums) if margin_5d_sums else np.nan

        self.set_l2_card("訂單能見度", "訂單能見度\n(合約負債季增)", m_contract, is_pct=True, good_thresh=5.0,
                         bad_thresh=-5.0, lower_is_better=False)
        self.set_l2_card("盈利加速", "盈利加速\n(EPS QoQ)", m_eps, is_pct=True, good_thresh=10.0, bad_thresh=-10.0,
                         lower_is_better=False)
        self.set_l2_card("法人資金", "法人資金\n(近5日中位數)", m_inst, is_pct=False, good_thresh=500, bad_thresh=-500,
                         lower_is_better=False)
        self.set_l2_card("籌碼健康", "籌碼健康\n(融資5日中位數)", m_margin, is_pct=False, good_thresh=-100,
                         bad_thresh=500, lower_is_better=True)

    def set_l2_card(self, key, title, val, is_pct, good_thresh, bad_thresh, lower_is_better=False):
        lbl = self.l2_labels.get(key)
        if not lbl: return

        if pd.isna(val):
            lbl.setText(f"{title}\n無資料")
            lbl.setStyleSheet("background-color: #1A1A1A; color: #888888; border: 1px solid #444; border-radius: 6px;")
            return

        val_str = f"{val:+.1f}%" if is_pct else f"{int(val):+d} 張"

        color = "#E0E0E0"
        border = "#555555"

        is_good = False
        is_bad = False

        if lower_is_better:
            is_good = val <= good_thresh
            is_bad = val >= bad_thresh
        else:
            is_good = val >= good_thresh
            is_bad = val <= bad_thresh

        if is_good:
            color = "#FF4D4D"
            border = "#D32F2F"
        elif is_bad:
            color = "#00E676"
            border = "#2E7D32"

        lbl.setText(f"{title}\n{val_str}")
        lbl.setStyleSheet(
            f"background-color: #1A1A1A; color: {color}; border: 1px solid {border}; "
            f"border-radius: 6px; font-weight: bold; font-size: 13px;"
        )

    def update_constituents_table(self, sector_name):
        try:
            current_mtime = self.snapshot_path.stat().st_mtime if self.snapshot_path.exists() else 0
            last_mtime = getattr(self, '_last_snapshot_mtime', 0)
            if current_mtime > last_mtime or self.snapshot_df is None:
                self.init_data()
        except:
            pass

        if self.worker is not None and self.worker.isRunning():
            self.worker.cancel()
            self.worker.wait()
            self.worker = None

        self.const_table.setSortingEnabled(False)
        self.const_table.setRowCount(0)

        if self.snapshot_df is None or self.snapshot_df.empty: return

        sids = self.sector_members.get(sector_name, set())
        filtered_df = self.snapshot_df[self.snapshot_df['股票代號'].isin(sids)]
        total_stocks = len(filtered_df)
        if total_stocks == 0: return

        self.progress_dialog = QProgressDialog("💎 正在深入計算成分股 L3 評分與黑馬指標...", "取消運算", 0, total_stocks,
                                               self)
        self.progress_dialog.setWindowModality(Qt.WindowModality.WindowModal)
        self.progress_dialog.setMinimumDuration(150)
        self.progress_dialog.setWindowTitle("請稍候")
        self.progress_dialog.setStyleSheet("""
            QProgressDialog { background-color: #1E1E1E; color: #E0E0E0; border: 1px solid #444; }
            QLabel { color: #00E5FF; font-size: 13px; font-weight: bold; }
            QProgressBar { border: 1px solid #555; background: #2D2D2D; text-align: center; color: #FFF; border-radius: 4px; }
            QProgressBar::chunk { background-color: #7B1FA2; }
            QPushButton { background: #3E3E42; color: #FFF; padding: 4px 10px; border: 1px solid #555; border-radius: 4px; }
            QPushButton:hover { background: #505054; }
        """)

        self.worker = L3ComputeWorker(self.project_root, filtered_df, self)
        self.progress_dialog.canceled.connect(self.on_worker_cancelled_by_user)

        self.worker.progress_updated.connect(self.on_worker_progress_updated)
        self.worker.stock_computed.connect(self.on_worker_stock_computed)
        self.worker.all_finished.connect(self.on_worker_all_finished)

        self.worker.start()

    def on_worker_progress_updated(self, completed, total, current_stock_name):
        if self.progress_dialog:
            self.progress_dialog.setLabelText(f"⏳ 正在分析: {current_stock_name}\n({completed} / {total})")
            self.progress_dialog.setValue(completed)

    def on_worker_stock_computed(self, data):
        row = self.const_table.rowCount()
        self.const_table.insertRow(row)

        sid = data["sid"]

        sid_item = NumericItem(sid)
        sid_item.setData(Qt.ItemDataRole.UserRole, 0)
        self.const_table.setItem(row, 0, sid_item)

        name_item = NumericItem(data["name"])
        name_item.setData(Qt.ItemDataRole.UserRole, 0)
        self.const_table.setItem(row, 1, name_item)

        self.const_table.setItem(row, 2, self.format_number_item(data["close"]))
        self.const_table.setItem(row, 3, self.format_number_item(data["pct_1d"], True))
        self.const_table.setItem(row, 4, self.format_number_item(data["pct_5d"], True))
        self.const_table.setItem(row, 5, self.format_number_item(data["legal_5d"], True))
        self.const_table.setItem(row, 6, self.format_number_item(data["margin_5d"], True))

        rs_val = data["rs"]
        self.const_table.setItem(row, 7, self.format_number_item(rs_val))

        tag_item = NumericItem(data["tags"])
        tag_item.setData(Qt.ItemDataRole.UserRole, 0)
        tag_item.setToolTip(data["tags"].replace(",", "\n"))
        self.const_table.setItem(row, 8, tag_item)

        self.const_table.setItem(row, 9, self.format_number_item(data["d30w"]))
        self.const_table.setItem(row, 10, self.format_number_item(data["dst"]))

        l3_val = data["score"]
        l3_item = self.format_number_item(l3_val, False)
        font = l3_item.font()
        font.setBold(True)
        l3_item.setFont(font)
        l3_item.setForeground(QBrush(QColor('#FFFFFF')))
        if data["tooltip"]: l3_item.setToolTip(data["tooltip"])
        self.const_table.setItem(row, 11, l3_item)

        is_horse = data["is_horse"]
        horse_str = "★" if is_horse else ""
        horse_item = NumericItem(horse_str)
        horse_item.setData(Qt.ItemDataRole.UserRole, 1 if is_horse else 0)
        horse_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        if is_horse:
            horse_item.setForeground(QBrush(QColor('#FFD700')))
        if data["tooltip"]: horse_item.setToolTip(data["tooltip"])
        self.const_table.setItem(row, 12, horse_item)

        if data["tooltip"]:
            sid_item.setToolTip(data["tooltip"])
            name_item.setToolTip(data["tooltip"])

    def on_worker_all_finished(self):
        if self.progress_dialog:
            self.progress_dialog.close()
            self.progress_dialog = None
        self.const_table.setSortingEnabled(True)
        self.worker = None

    def on_worker_cancelled_by_user(self):
        if self.worker is not None:
            self.worker.cancel()
            self.worker = None
        self.const_table.setSortingEnabled(True)

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
                    if 0 <= dist <= 3 and '30w' in tag_item.text().lower():
                        match = True
            elif is_st_special:
                st_item = self.const_table.item(i, 10)
                tag_item = self.const_table.item(i, 8)
                if st_item and tag_item:
                    dist = float(st_item.data(Qt.ItemDataRole.UserRole))
                    if 0 <= dist <= 3 and 'st' in tag_item.text().lower():
                        match = True
            elif text == "" or "-- ⚡" in text:
                match = True
            else:
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


from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtWidgets import QProgressDialog


class L3ComputeWorker(QThread):
    """負責在背景非阻塞執行 L3Score 即時算分與黑馬判定的背景執行緒"""
    progress_updated = pyqtSignal(int, int, str)
    stock_computed = pyqtSignal(dict)
    all_finished = pyqtSignal()

    def __init__(self, project_root, filtered_df, parent=None):
        super().__init__(parent)
        self.project_root = Path(project_root)
        self.tasks = filtered_df.to_dict('records')
        self._is_cancelled = False

    def cancel(self):
        self._is_cancelled = True

    def run(self):
        import builtins
        original_print = builtins.print
        builtins.print = lambda *args, **kwargs: None

        try:
            total = len(self.tasks)
            for idx, row in enumerate(self.tasks):
                if self._is_cancelled:
                    break

                row_dict = row._asdict() if hasattr(row, '_asdict') else row
                sid = str(row_dict.get('股票代號', ''))
                rs_val = row_dict.get('RS強度', 0)
                tags = str(row_dict.get('強勢特徵標籤', ''))
                name_str = str(row_dict.get('股票名稱', ''))

                self.progress_updated.emit(idx, total, f"{sid} {name_str}")

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
                        kline_df = pd.read_parquet(kline_path, engine='pyarrow', memory_map=False).copy()
                        kline_df.columns = [str(c).lower() for c in kline_df.columns]
                        if isinstance(kline_df.index, pd.DatetimeIndex):
                            kline_df['date'] = kline_df.index
                        elif 'date' not in kline_df.columns and 'timestamp' in kline_df.columns:
                            kline_df['date'] = pd.to_datetime(kline_df['timestamp'], unit='ms')
                    except:
                        pass

                l3_score_val = 0.0
                is_dark_horse = False
                tooltip_html = ""

                if not kline_df.empty and fundamental_data:
                    boll_width = row_dict.get('布林寬度(%)', 50.0)
                    try:
                        from utils.strategies.technical import TechnicalStrategies
                        bb_df = TechnicalStrategies.calculate_bollinger_bands(kline_df, window=20)
                        if not bb_df.empty and not pd.isna(bb_df['BB_Width_Pct'].iloc[-1]):
                            boll_width = float(bb_df['BB_Width_Pct'].iloc[-1])
                    except:
                        pass

                    try:
                        l3_result = L3Scorer.calculate_score(float(rs_val), fundamental_data, kline_df, tags,
                                                             float(boll_width))
                        l3_score_val = l3_result.get('L3Score', 0.0)
                        is_dark_horse = l3_result.get('is_dark_horse', False)

                        vwap = l3_result.get('vwap_info', {})
                        rev = l3_result.get('rev_info', {})
                        prof = l3_result.get('prof_info', {})

                        breakdown_parts = []
                        if float(rs_val) >= 80:
                            breakdown_parts.append("🟢 RS技術強勢分 (+30)")
                        elif float(rs_val) >= 50:
                            breakdown_parts.append("🟡 RS技術中等分 (+15)")

                        v_status = vwap.get('status', '')
                        if "站上" in v_status or "站在" in v_status:
                            breakdown_parts.append("🟢 法人成本安全分 (+25)")

                        rev_slope = float(rev.get('slope', 0))
                        if rev_slope > 10:
                            breakdown_parts.append(f"🟢 營收動能噴發分 ({rev_slope:+.1f}%) (+25)")
                        elif rev_slope > 0:
                            breakdown_parts.append(f"🟡 營收穩定成長分 ({rev_slope:+.1f}%) (+15)")

                        op_qoq = float(prof.get('qoq', 0))
                        if op_qoq > 20:
                            breakdown_parts.append(f"🟢 獲利加速品質分 ({op_qoq:+.1f}%) (+20)")

                        score_breakdown_html = "<br>".join(breakdown_parts) if breakdown_parts else "基本技術與價量基本分"

                        reasons = []
                        if is_dark_horse:
                            if boll_width < 15:
                                reasons.append(f"⚡ 布林通道極度收斂 ({boll_width:.1f}%) - 蓄勢突破")
                            if "ilss" in tags.lower() or "主力" in tags.lower():
                                reasons.append("🐳 主力暗中掃單特徵 (ILSS技術特徵)")
                            if float(rs_val) >= 85:
                                reasons.append(f"📈 RS強度高達 {rs_val:.1f} - 超越市場大眾")
                            vwap_bias = float(vwap.get('bias_pct', 0))
                            if 0 <= vwap_bias <= 5:
                                reasons.append(f"🛡️ 貼近法人成本安全邊際 (乖離僅 {vwap_bias:.1f}%)")
                            if not reasons:
                                reasons.append("✨ 符合『籌碼沉澱 + 營收爆發 + 剛起漲』之黃金交叉")
                        reasons_html = "<br>".join(reasons) if reasons else "不符合黑馬指標條件"

                        tooltip_html = f"""
                        <div style='font-family: Arial, sans-serif; font-size: 13px; color: #E0E0E0; line-height: 1.45;'>
                            <b style='color: #FFD700; font-size: 14px;'>{sid} {name_str}</b> | L3Score: <b style='color: #B388FF;'>{l3_score_val:.1f}</b> {'<span style="color:#FFD700;">★黑馬特選股</span>' if is_dark_horse else ''}<br>
                            <hr style='background-color: #555; height: 1px; border: none; margin: 5px 0;'>

                            <b style='color: #00FFCC;'>📊 【L3Score 得分細項拆解】:</b><br>
                            <span style='color: #FFFFFF;'>{score_breakdown_html}</span><br>
                            <br>
                            <b style='color: #FFD700;'>🎯 【黑馬選股判定依據】:</b><br>
                            <span style='color: #FFB74D; font-weight: bold;'>{reasons_html}</span><br>
                            <hr style='background-color: #555; height: 1px; border: none; margin: 5px 0;'>

                            <b style='color: #00E5FF;'>[法人成本]</b> {vwap.get('status', '')}<br>
                            均價: {vwap.get('vwap', 0)} | 乖離率: <b style='color: #FF5252;'>{vwap_bias}%</b><br>
                            <b style='color: #FF9800;'>[營收動能]</b> {rev.get('status', '')} (斜率: {rev.get('slope', 0)}%)<br>
                            <b style='color: #69F0AE;'>[獲利純度]</b> {prof.get('status', '')} (營業利益季增: {prof.get('qoq', 0)}%)<br>
                        </div>
                        """
                    except:
                        pass

                res_dict = {
                    "sid": sid, "name": name_str,
                    "close": row_dict.get('今日收盤價', 0),
                    "pct_1d": row_dict.get('今日漲幅(%)', 0),
                    "pct_5d": row_dict.get('5日漲幅(%)', 0),
                    "legal_5d": row_dict.get('法人5日增(%)', 0),
                    "margin_5d": row_dict.get('融資5日增(%)', 0),
                    "rs": rs_val, "tags": tags,
                    "d30w": row_dict.get('30W距離', 99),
                    "dst": row_dict.get('ST距離', 99),
                    "score": l3_score_val, "is_horse": is_dark_horse,
                    "tooltip": tooltip_html
                }
                self.stock_computed.emit(res_dict)

            self.progress_updated.emit(total, total, "計算完成")
        finally:
            builtins.print = original_print

        # 👇 修改：發送完成訊號，並在當前是「黑馬」選單時自動刷新畫面
        self.finished.emit()


class GlobalL3ComputeWorker(QThread):
    """🚀 負責背景掃描與並行計算『全體板塊成分股』L3 評分的執行執行緒"""
    progress_updated = pyqtSignal(int, int, str)
    finished_computation = pyqtSignal(set)

    def __init__(self, project_root, df_to_compute, parent=None):
        super().__init__(parent)
        self.project_root = Path(project_root)
        self.tasks = df_to_compute.to_dict('records')
        self._is_cancelled = False

    def cancel(self):
        self._is_cancelled = True

    def run(self):
        import builtins
        original_print = builtins.print
        builtins.print = lambda *args, **kwargs: None

        # ✨ 1. 建立集合，負責收集全市場的黑馬股代號
        dark_horse_sids = set()

        try:
            total = len(self.tasks)
            for idx, row in enumerate(self.tasks):
                if self._is_cancelled:
                    break

                row_dict = row._asdict() if hasattr(row, '_asdict') else row
                sid = str(row_dict.get('股票代號', ''))
                rs_val = row_dict.get('RS強度', 0)
                tags = str(row_dict.get('強勢特徵標籤', ''))
                name_str = str(row_dict.get('股票名稱', ''))

                # ✨ 2. 降頻渲染：每 5 檔股票才更新一次 UI，防止訊號風暴癱瘓主執行緒
                if idx % 5 == 0 or idx == total - 1:
                    self.progress_updated.emit(idx, total, f"{sid} {name_str}")

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
                        kline_df = pd.read_parquet(kline_path, engine='pyarrow', memory_map=False).copy()
                        kline_df.columns = [str(c).lower() for c in kline_df.columns]
                        if isinstance(kline_df.index, pd.DatetimeIndex):
                            kline_df['date'] = kline_df.index
                        elif 'date' not in kline_df.columns and 'timestamp' in kline_df.columns:
                            kline_df['date'] = pd.to_datetime(kline_df['timestamp'], unit='ms')
                    except:
                        pass

                is_dark_horse = False

                if not kline_df.empty and fundamental_data:
                    boll_width = row_dict.get('布林寬度(%)', 50.0)
                    try:
                        from utils.strategies.technical import TechnicalStrategies
                        bb_df = TechnicalStrategies.calculate_bollinger_bands(kline_df, window=20)
                        if not bb_df.empty and not pd.isna(bb_df['BB_Width_Pct'].iloc[-1]):
                            boll_width = float(bb_df['BB_Width_Pct'].iloc[-1])
                    except:
                        pass

                    try:
                        rs_float = float(rs_val) if rs_val else 0.0
                        boll_float = float(boll_width) if boll_width else 50.0

                        from utils.scoring.l3_score import L3Scorer
                        l3_result = L3Scorer.calculate_score(rs_float, fundamental_data, kline_df, tags, boll_float)
                        is_dark_horse = l3_result.get('is_dark_horse', False)

                        # ✨ 3. 如果判定為黑馬特選股，立刻加入集合
                        if is_dark_horse:
                            dark_horse_sids.add(sid)

                    except Exception as e:
                        print(f"❌ {sid} 運算失敗: {e}")
                        pass

                # ✨ 4. 這裡原本錯誤呼叫了未定義的 stock_computed，已被徹底拔除。

            # ✨ 5. 掃描結束後，將收集到的黑馬名單發送回主 UI
            if not self._is_cancelled:
                self.finished_computation.emit(dark_horse_sids)

        finally:
            builtins.print = original_print

        self.finished.emit()