import sys
import json
import os
import re
import time
import zipfile
from pathlib import Path
from datetime import datetime

import pandas as pd

# 🎨 1. 介面與元件引用
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QPushButton, QGridLayout, QDoubleSpinBox,
                             QSpinBox, QScrollArea, QMessageBox, QProgressBar,
                             QTextEdit, QFrame, QApplication, QComboBox)

from PyQt6.QtCore import Qt, pyqtSignal, QProcess, QProcessEnvironment

# ==========================================
# 🎨 2. 介面基礎樣式 (含 Tooltip 修復)
# ==========================================
STYLES = """
    QWidget { font-family: "Segoe UI", "Microsoft JhengHei"; background-color: #121212; color: #E0E0E0; }
    QFrame#Card { background-color: #1E1E1E; border-radius: 12px; border: 1px solid #3E3E42; }
    QLabel#Title { font-size: 26px; font-weight: bold; color: #00E5FF; margin-bottom: 10px; }
    QLabel#CardTitle { font-size: 18px; font-weight: bold; color: #FFFFFF; }
    QLabel#Label { font-size: 16px; color: #FFFFFF; font-weight: bold; }
    QLabel#Value { font-size: 16px; font-weight: bold; color: #00E5FF; }
    QLabel#Warning { font-size: 16px; font-weight: bold; color: #FF5252; }
    QLabel#Success { font-size: 16px; font-weight: bold; color: #00E676; }
    QLabel#Desc { font-size: 14px; color: #BBBBBB; font-style: normal; }
    QLabel#StrategyTime { font-size: 14px; color: #FFEB3B; font-weight: bold; margin-right: 10px; }

    QDoubleSpinBox, QSpinBox {
        background-color: #2D2D30; border: 1px solid #555; border-radius: 4px; padding: 8px 10px;
        font-size: 20px; color: #00E5FF; font-weight: bold; min-width: 120px; max-width: 160px;
    }
    QProgressBar {
        border: 1px solid #555; border-radius: 6px; text-align: center;
        background-color: #252526; color: white; font-weight: bold; min-height: 20px;
    }
    QProgressBar::chunk { background-color: #00E5FF; border-radius: 5px; }
    QTextEdit { background-color: #1E1E1E; border: 1px solid #3E3E42; border-radius: 6px; font-family: Consolas; color: #CCC; font-size: 14px; }

    QToolTip { 
        color: #FFFFFF; 
        background-color: #2D2D30; 
        border: 1px solid #00E5FF; 
        border-radius: 4px;
        padding: 5px;
        font-size: 14px;
    }
"""

BTN_ACTION = """
    QPushButton { background-color: #0066CC; border: 2px solid #004C99; border-radius: 6px; padding: 10px 15px; font-size: 16px; color: white; font-weight: bold; }
    QPushButton:hover { background-color: #3399FF; border: 2px solid #FFFFFF; }
    QPushButton:disabled { background-color: #222222; border: 2px solid #333333; color: #666666; }
"""
BTN_CHECK = """
    QPushButton { background-color: #00897B; border: 2px solid #00695C; border-radius: 6px; padding: 10px 15px; font-size: 16px; color: white; font-weight: bold; }
    QPushButton:hover { background-color: #26A69A; border: 2px solid #FFFFFF; }
    QPushButton:disabled { background-color: #222222; border: 2px solid #333333; color: #666666; }
"""
BTN_DANGER = """
    QPushButton { background-color: #D32F2F; border: 2px solid #B71C1C; border-radius: 6px; padding: 10px 15px; font-size: 16px; color: white; font-weight: bold; }
    QPushButton:hover { background-color: #FF5252; border: 2px solid #FFFFFF; }
    QPushButton:disabled { background-color: #222222; border: 2px solid #333333; color: #666666; }
"""
BTN_RESET = """
    QPushButton { background-color: #555555; border: 2px solid #444444; border-radius: 6px; padding: 10px 15px; font-size: 16px; color: #DDDDDD; font-weight: bold; }
    QPushButton:hover { background-color: #888888; border: 2px solid #FFFFFF; color: #FFFFFF; }
    QPushButton:disabled { background-color: #222222; border: 2px solid #333333; color: #666666; }
"""
BTN_TOGGLE = """
    QPushButton { background-color: #1E1E1E; border: 1px solid #3E3E42; text-align: left; font-size: 16px; color: #00E5FF; padding: 10px 15px; border-radius: 6px; font-weight: bold; }
    QPushButton:hover { background-color: #333337; border: 1px solid #00E5FF; color: #FFFFFF; }
"""


class ScriptRunner(QProcess):
    output_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)

    def __init__(self, script_path, args=None, use_python=True):
        super().__init__()
        self.script_path = script_path
        self.args = args or []
        self.use_python = use_python

        env = QProcessEnvironment.systemEnvironment()
        env.insert("PYTHONIOENCODING", "utf-8")
        self.setProcessEnvironment(env)
        self.setProcessChannelMode(QProcess.ProcessChannelMode.MergedChannels)
        self.readyReadStandardOutput.connect(self.handle_output)

    def start_script(self):
        if self.use_python:
            self.start(sys.executable, [str(self.script_path)] + self.args)
        else:
            self.start(str(self.script_path), self.args)

    def handle_output(self):
        try:
            data = self.readAllStandardOutput()
            text = bytes(data).decode('utf-8', errors='replace')
            match = re.search(r"PROGRESS:\s*(\d+)", text)
            if match:
                self.progress_signal.emit(int(match.group(1)))
            self.output_signal.emit(text)
        except:
            pass


class SettingsModule(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet(STYLES)
        self.project_root = Path(__file__).resolve().parent.parent
        self.config_path = self.project_root / "data" / "strategy_config.json"
        self.strategy_result_path = self.project_root / "data" / "strategy_results" / "factor_snapshot.parquet"
        self.is_editing = False
        self.original_params = {}
        self.market_ready = False

        self.init_ui()
        self.load_config()
        self.check_strategy_time()
        self.run_status_probe()
        self.set_inputs_enabled(False)

    def _create_label(self, text, style_class, tooltip=""):
        lbl = QLabel(text)
        lbl.setObjectName(style_class)
        if tooltip: lbl.setToolTip(tooltip)
        return lbl

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(30, 30, 30, 30)
        main_layout.setSpacing(20)

        main_layout.addWidget(self._create_label("系統控制台 System Dashboard", "Title"))

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("background: transparent; border: none;")

        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setSpacing(20)
        content_layout.setContentsMargins(0, 0, 20, 0)

        # ==========================================
        # 卡片 1: 取得 K 線 (來源切分)
        # ==========================================
        card_kline = QFrame()
        card_kline.setObjectName("Card")
        l_kline = QVBoxLayout(card_kline)
        l_kline.setContentsMargins(20, 20, 20, 20)
        l_kline.addWidget(self._create_label("☁️ 第一步：取得最新 K 線資料", "CardTitle"))
        l_kline.addWidget(self._create_label(
            "說明: 雲端 Actions 已於每日下午打包好 K 線。點擊下載 ZIP 瞬間套用；若雲端異常才用本機備援。", "Desc"))

        kline_btn_layout = QHBoxLayout()
        self.lbl_cloud_time = self._create_label("雲端狀態: 尚未檢查", "Value")

        self.btn_check_cloud = QPushButton("🔄 檢查雲端 ZIP")
        self.btn_check_cloud.setStyleSheet(BTN_RESET)
        self.btn_check_cloud.clicked.connect(self.check_cloud_status)

        self.btn_download_zip = QPushButton("☁️ [首選] 下載並套用雲端資料")
        self.btn_download_zip.setStyleSheet(BTN_ACTION)
        self.btn_download_zip.setEnabled(False)
        self.btn_download_zip.clicked.connect(self.download_cloud_data)

        self.btn_fallback_kline = QPushButton("💻 [備援] 抓取全市場 K 線 (init_cache)")
        self.btn_fallback_kline.setStyleSheet(BTN_DANGER)
        self.btn_fallback_kline.clicked.connect(self.run_fallback_kline)

        kline_btn_layout.addWidget(self.lbl_cloud_time)
        kline_btn_layout.addStretch()
        kline_btn_layout.addWidget(self.btn_check_cloud)
        kline_btn_layout.addWidget(self.btn_download_zip)
        kline_btn_layout.addWidget(self.btn_fallback_kline)
        l_kline.addLayout(kline_btn_layout)
        content_layout.addWidget(card_kline)

        # ==========================================
        # 卡片 2: 狀態對齊
        # ==========================================
        card_status = QFrame()
        card_status.setObjectName("Card")
        l_status = QVBoxLayout(card_status)
        l_status.setContentsMargins(20, 20, 20, 20)
        l_status.addWidget(self._create_label("🛡️ 本機資料狀態檢驗", "CardTitle"))
        l_status.addSpacing(10)

        h1 = QHBoxLayout()
        h1.addWidget(self._create_label("K 線最新日期:", "Label"))
        self.lbl_kline_date = self._create_label("--", "Value")
        h1.addWidget(self.lbl_kline_date)
        h1.addSpacing(40)
        h1.addWidget(self._create_label("籌碼最新日期:", "Label"))
        self.lbl_chips_date = self._create_label("--", "Value")
        h1.addWidget(self.lbl_chips_date)
        h1.addStretch()
        l_status.addLayout(h1)

        h2 = QHBoxLayout()
        h2.addWidget(self._create_label("資料對齊狀態:", "Label"))
        self.lbl_align_status = self._create_label("檢查中...", "Value")
        h2.addWidget(self.lbl_align_status)
        h2.addStretch()
        l_status.addLayout(h2)

        h3 = QHBoxLayout()
        h3.addWidget(self._create_label("基本面最新狀態:", "Label"))
        self.lbl_fund_status = self._create_label("--", "Desc")
        h3.addWidget(self.lbl_fund_status)
        h3.addStretch()
        l_status.addLayout(h3)

        content_layout.addWidget(card_status)

        # ==========================================
        # 卡片 3: 第二步：本機策略核心排程 (修正版)
        # ==========================================
        card_pipeline = QFrame()
        card_pipeline.setObjectName("Card")
        l_pipe = QVBoxLayout(card_pipeline)
        l_pipe.setContentsMargins(20, 20, 20, 20)
        l_pipe.addWidget(self._create_label("💻 第二步：本機盤後策略運算排程", "CardTitle"))
        l_pipe.addWidget(
            self._create_label("說明: 每日 K 線就緒後，請執行【核心排程】更新個股深度籌碼並自動產出最終策略大表。", "Desc"))
        l_pipe.addSpacing(10)

        # 1. 核心排程 (Core) - 最重要
        self.btn_pipe_core = QPushButton("🚀 [核心排程] 深度籌碼與策略大表運算\n(daily_chips ➔ calc_factors)")
        self.btn_pipe_core.setStyleSheet(BTN_ACTION)
        self.btn_pipe_core.setToolTip("耗時約15~20分。\n依序執行：\n1. 抓取JSON個股深度籌碼\n2. 結合最新K線產出策略大表")
        self.btn_pipe_core.clicked.connect(self.run_core_pipeline)
        l_pipe.addWidget(self.btn_pipe_core)

        # 2. 廣度大表 (Broad/Optional) - 次要
        self.btn_pipe_broad = QPushButton("📊 [可選] 全市場廣度籌碼快照 (update_chips_revenue)")
        self.btn_pipe_broad.setStyleSheet(BTN_CHECK)
        self.btn_pipe_broad.setToolTip("供全市場1700檔掃描用。\n若策略僅依賴白名單深度籌碼，此步驟可忽略。")
        self.btn_pipe_broad.clicked.connect(self.run_pipeline_broad_snapshot)
        l_pipe.addWidget(self.btn_pipe_broad)

        # 3. 基本面 (Finance) - 左右佈局
        finance_layout = QHBoxLayout()
        finance_layout.setSpacing(10)  # 左右間距

        # 左邊：下拉選單 (含三種模式)
        self.combo_finance_mode = QComboBox()
        self.combo_finance_mode.addItems([
            "⚡ 僅更新月營收 (快)",
            "🔥 更新營收+財報 (慢)",
            "📄 僅更新財報 (慢)"
        ])
        self.combo_finance_mode.setStyleSheet("""
            QComboBox { 
                background-color: #2D2D30; color: #EEE; border: 1px solid #555; 
                padding: 10px; font-size: 16px; border-radius: 6px; font-weight: bold;
            }
            QComboBox::drop-down { border: 0px; }
            QComboBox QAbstractItemView {
                background-color: #2D2D30; color: #EEE; selection-background-color: #0066CC;
            }
        """)

        # 右邊：執行按鈕
        self.btn_pipe_finance = QPushButton("🏦 執行基本面更新")
        self.btn_pipe_finance.setStyleSheet(BTN_RESET)
        self.btn_pipe_finance.setToolTip(
            "依據左側選單執行：\n⚡ 僅營收: 每月10號用 (約5分)\n🔥 營收+財報: 財報季用 (約30分)\n📄 僅財報: 同上，適合只需更新季報時\n\n⚠️ 更新後會自動執行運算，確保數據進入大表")
        self.btn_pipe_finance.clicked.connect(self.run_pipeline_financials)

        finance_layout.addWidget(self.combo_finance_mode, 4)  # 佔 40%
        finance_layout.addWidget(self.btn_pipe_finance, 6)  # 佔 60%
        l_pipe.addLayout(finance_layout)

        content_layout.addWidget(card_pipeline)

        # ==========================================
        # 卡片 4: 策略參數
        # ==========================================
        card_param = QFrame()
        card_param.setObjectName("Card")
        l_param = QVBoxLayout(card_param)
        l_param.setContentsMargins(20, 20, 20, 20)
        l_param.addWidget(self._create_label("⚙️ 系統參數設定", "CardTitle"))

        self.btn_toggle_30w = QPushButton("▶ 30W 策略參數設定 (點擊展開)")
        self.btn_toggle_30w.setStyleSheet(BTN_TOGGLE)
        self.btn_toggle_30w.clicked.connect(self.toggle_30w_params)
        l_param.addWidget(self.btn_toggle_30w)

        self.container_30w = QWidget()
        l_30w = QVBoxLayout(self.container_30w)
        l_30w.setContentsMargins(10, 10, 0, 0)

        header_layout = QHBoxLayout()
        self.lbl_strategy_time = self._create_label("上次運算: --", "StrategyTime")
        self.btn_edit = QPushButton("🔧 編輯模式")
        self.btn_edit.setStyleSheet(BTN_CHECK)
        self.btn_edit.clicked.connect(self.toggle_edit_mode)
        self.btn_save_recalc = QPushButton("⚡ 僅重算策略因子")
        self.btn_save_recalc.setStyleSheet(BTN_ACTION)
        self.btn_save_recalc.setToolTip("不抓取任何新資料，僅依據目前的參數重新運算策略")
        self.btn_save_recalc.clicked.connect(self.handle_action_click)

        header_layout.addWidget(self.lbl_strategy_time)
        header_layout.addStretch()
        header_layout.addWidget(self.btn_edit)
        header_layout.addSpacing(10)
        header_layout.addWidget(self.btn_save_recalc)
        l_30w.addLayout(header_layout)

        grid_p = QGridLayout()
        grid_p.setVerticalSpacing(10)
        self.inputs = {}
        self.params_def = [
            ('trigger_min_gain', '觸發漲幅門檻', 'float', 0.10, 0.0, 0.5, 0.01, '最低要求漲幅'),
            ('trigger_vol_multiplier', '觸發量能倍數', 'float', 1.1, 1.0, 10.0, 0.1, '當日量需大於 N 倍'),
            ('adhesive_weeks', '黏貼週數', 'int', 2, 1, 10, 1, '均線糾結週數'),
            ('adhesive_bias', '黏貼乖離率', 'float', 0.12, 0.01, 0.5, 0.01, '均線距離容許值'),
            ('shakeout_lookback', '甩轎回溯週數', 'int', 12, 4, 52, 1, '大跌甩轎檢查'),
            ('shakeout_max_depth', '甩轎最大深度', 'float', 0.35, 0.05, 0.9, 0.05, '甩轎跌幅限制'),
            ('shakeout_underwater_limit', '甩轎水下限期', 'int', 10, 1, 20, 1, '水下最大週數'),
            ('shakeout_prev_bias_limit', '甩轎前乖離限', 'float', 0.15, 0.05, 0.5, 0.01, '起漲前乖離限制'),
            ('signal_lookback_days', '訊號顯示天數', 'int', 10, 1, 60, 1, '顯示最近 N 天訊號'),
        ]

        for i, (key, label, ptype, default, vmin, vmax, vstep, tip) in enumerate(self.params_def):
            grid_p.addWidget(self._create_label(label, "Label", tooltip=tip), i, 0)
            inp = QDoubleSpinBox() if ptype == 'float' else QSpinBox()
            if ptype == 'float': inp.setDecimals(2)
            inp.setRange(vmin, vmax)
            inp.setSingleStep(vstep)
            inp.setValue(default)
            grid_p.addWidget(inp, i, 1)
            self.inputs[key] = inp
            grid_p.addWidget(self._create_label(tip, "Desc"), i, 2)

        l_30w.addLayout(grid_p)
        self.container_30w.setVisible(False)
        content_layout.addWidget(card_param)

        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setFixedHeight(120)
        content_layout.addWidget(self.log_output)

        self.progress = QProgressBar()
        self.progress.setValue(0)
        self.progress.setTextVisible(True)
        content_layout.addWidget(self.progress)

        scroll.setWidget(content)
        main_layout.addWidget(scroll)

        for inp in self.inputs.values():
            inp.valueChanged.connect(self.update_action_button_text)

    # ==========================================
    # 狀態與防呆探測
    # ==========================================
    def run_status_probe(self):
        kline_date = "無資料"
        chips_date = "無資料"
        fund_date = "無資料"

        kline_paths = [
            self.project_root / "data" / "cache" / "tw" / "2330_TW.parquet",
            self.project_root / "data" / "cache" / "tw" / "1101_TW.parquet"
        ]
        for p in kline_paths:
            if p.exists():
                try:
                    df = pd.read_parquet(p)
                    if not df.empty:
                        dt = df.index[-1]
                        if isinstance(dt, (int, float)): dt = pd.to_datetime(dt)
                        kline_date = dt.strftime('%Y-%m-%d')
                        break
                except:
                    pass

        json_paths = [
            self.project_root / "data" / "fundamentals" / "2330.json",
            self.project_root / "data" / "2330.json"
        ]
        for p in json_paths:
            if p.exists():
                try:
                    mtime = p.stat().st_mtime
                    fund_date = f"本機最後執行: {datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')} (供參考)"
                    with open(p, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        inst = data.get("institutional_investors", [])
                        if inst: chips_date = inst[0].get("date", "無資料")
                        break
                except:
                    pass

        self.lbl_kline_date.setText(kline_date)
        self.lbl_chips_date.setText(chips_date)
        self.lbl_fund_status.setText(fund_date)

        if kline_date == chips_date and kline_date != "無資料":
            self.lbl_align_status.setText("🟢 K線與籌碼已對齊")
            self.lbl_align_status.setObjectName("Success")
        elif kline_date == "無資料" and chips_date == "無資料":
            self.lbl_align_status.setText("⚪ 尚無資料")
            self.lbl_align_status.setObjectName("Value")
        else:
            self.lbl_align_status.setText("🟡 日期未對齊 (請執行核心排程更新籌碼)")
            self.lbl_align_status.setObjectName("Warning")

        self.lbl_align_status.style().unpolish(self.lbl_align_status)
        self.lbl_align_status.style().polish(self.lbl_align_status)

        now = datetime.now()
        is_weekday = now.weekday() < 5
        self.market_ready = not (is_weekday and 8 <= now.hour < 14)

    def _foolproof_check(self, task_name):
        if not self.market_ready:
            reply = QMessageBox.warning(
                self, "盤中執行警示",
                f"目前台股尚未收盤結算。\n\n現在執行「{task_name}」可能會抓到盤中不完整數據。\n確定要強制執行嗎？",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            return reply == QMessageBox.StandardButton.Yes
        return True

    # ==========================================
    # 排程指令 (Pipeline Core Logic)
    # ==========================================
    def run_fallback_kline(self):
        """備援：全市場 K 線抓取"""
        if not self._foolproof_check("備援全市場 K 線"): return
        self.task_start_time = time.time()
        self.log("🚀 [備援] 啟動: 本機全市場 K 線更新 (init_cache)...", True)
        self._disable_all_buttons()
        self.progress.setFormat("⏳ 正在更新 K 線資料 - %p%")
        self.runner_kline = ScriptRunner(self.project_root / "scripts" / "init_cache_tw.py", ["--skip-check", "--auto"])
        self.runner_kline.output_signal.connect(self.log)
        self.runner_kline.progress_signal.connect(self.progress.setValue)
        self.runner_kline.finished.connect(self.on_pipeline_finished)
        self.runner_kline.start_script()

    def run_core_pipeline(self):
        """核心流程：先跑深度籌碼 (JSON)，完成後自動觸發策略計算 (Parquet)"""
        if not self._foolproof_check("深度籌碼與策略運算"): return
        self.task_start_time = time.time()
        self.log("🚀 [核心] 啟動: 深度籌碼更新 (update_daily_chips.py) (1/2)...", True)
        self._disable_all_buttons()
        self.progress.setFormat("⏳ 抓取深度籌碼中 (1/2) - %p%")

        self.runner_daily = ScriptRunner(self.project_root / "scripts" / "update_daily_chips.py")
        self.runner_daily.output_signal.connect(self.log)
        self.runner_daily.progress_signal.connect(self.progress.setValue)
        # 連動：當 daily chips 跑完，自動觸發 calc_factors
        self.runner_daily.finished.connect(self._proceed_to_calc_factors)
        self.runner_daily.start_script()

    def _proceed_to_calc_factors(self):
        """核心流程下半部：讀取最新 JSON 產生策略快照"""
        if hasattr(self, 'task_start_time'):
            self._log_duration(self.task_start_time, "第一階段(抓取資料)耗時")
        if not self.save_config(): return
        self.log("⚙️ 啟動: 融合深度籌碼與策略運算 (calc_snapshot_factors.py) (2/2)...", False)
        self.progress.setFormat("⏳ 計算策略因子 (2/2) - %p%")

        self.runner_calc = ScriptRunner(self.project_root / "scripts" / "calc_snapshot_factors.py")
        self.runner_calc.output_signal.connect(self.log)
        self.runner_calc.progress_signal.connect(self.progress.setValue)
        self.runner_calc.finished.connect(self.on_pipeline_finished)
        self.runner_calc.start_script()

    def run_pipeline_broad_snapshot(self):
        """廣度快照：抓取籌碼 -> 自動計算策略"""
        if not self._foolproof_check("廣度籌碼快照"): return
        self.task_start_time = time.time()
        self.log("📊 [廣度] 啟動: 抓取籌碼與營收大表 (update_chips_revenue.py)...", True)
        self._disable_all_buttons()
        self.progress.setFormat("⏳ 產生全市場大表中 (1/2) - %p%")
        self.runner_snapshot = ScriptRunner(self.project_root / "scripts" / "update_chips_revenue.py")
        self.runner_snapshot.output_signal.connect(self.log)
        self.runner_snapshot.progress_signal.connect(self.progress.setValue)
        self.runner_snapshot.finished.connect(self._proceed_to_calc_factors)

        self.runner_snapshot.start_script()

    def run_pipeline_financials(self):
        """基本面更新：根據下拉選單決定模式"""
        # 0: 營收, 1: 全財報, 2: 僅財報
        mode_idx = self.combo_finance_mode.currentIndex()

        # 顯示名稱對照
        mode_names = ["僅更新月營收", "更新營收與財報", "僅更新財報"]
        mode_name = mode_names[mode_idx]

        if not self._foolproof_check(f"基本面更新 ({mode_name})"): return

        self.task_start_time = time.time()
        self.log(f"🏦 [附加] 啟動: {mode_name} (update_financials.py)...", True)
        self._disable_all_buttons()
        self.progress.setFormat("⏳ 抓取資料中 - %p%")

        # 參數邏輯：索引 1 和 2 都呼叫 --full (全開模式)
        args = []
        if mode_idx >= 1:
            args = ["--full"]

        self.runner_finance = ScriptRunner(self.project_root / "scripts" / "update_financials.py", args)
        self.runner_finance.output_signal.connect(self.log)
        self.runner_finance.progress_signal.connect(self.progress.setValue)
        # 🔥🔥🔥 修正：這裡必須連動到 _proceed_to_calc_factors 才能進大表
        self.runner_finance.finished.connect(self._proceed_to_calc_factors)
        self.runner_finance.start_script()

    def _disable_all_buttons(self):
        self.btn_check_cloud.setEnabled(False)
        self.btn_download_zip.setEnabled(False)
        self.btn_fallback_kline.setEnabled(False)
        self.btn_pipe_core.setEnabled(False)
        self.btn_pipe_broad.setEnabled(False)
        self.btn_pipe_finance.setEnabled(False)
        self.btn_save_recalc.setEnabled(False)
        self.progress.setRange(0, 100)
        self.progress.setValue(0)

    def on_pipeline_finished(self):
        self.log("✅ 執行完畢！")
        if hasattr(self, 'task_start_time'):
            self._log_duration(self.task_start_time, "總執行耗時")
        self.progress.setValue(100)
        self.progress.setFormat("✅ 作業結束")

        self.btn_check_cloud.setEnabled(True)
        self.btn_fallback_kline.setEnabled(True)
        self.btn_pipe_core.setEnabled(True)
        self.btn_pipe_broad.setEnabled(True)
        self.btn_pipe_finance.setEnabled(True)
        self.btn_save_recalc.setEnabled(True)

        self.check_strategy_time()
        self.run_status_probe()

    # ==========================================
    # 雲端處理
    # ==========================================
    def check_cloud_status(self):
        self.log("📡 檢查雲端中...", True)
        self.btn_check_cloud.setEnabled(False)
        self.progress.setRange(0, 0)
        self.runner_fetch = ScriptRunner("git", ["fetch", "origin", "main"], use_python=False)
        self.runner_fetch.finished.connect(lambda exitCode, exitStatus: self.read_remote_json())
        self.runner_fetch.start_script()

    def read_remote_json(self):
        self.status_runner = ScriptRunner("git", ["show", "origin/main:data/data_status.json"], use_python=False)
        self.status_runner.output_signal.connect(self.parse_remote_status)
        self.status_runner.start_script()

    def parse_remote_status(self, text):
        self.progress.setRange(0, 100)
        self.progress.setValue(100)
        self.btn_check_cloud.setEnabled(True)
        try:
            match = re.search(r'\{.*\}', text, re.DOTALL)
            remote_time = json.loads(match.group(0)).get('update_time', 'Unknown') if match else 'Unknown'
            self.lbl_cloud_time.setText(f"雲端狀態: {remote_time}")
            zip_path = self.project_root / "data" / "daily_data.zip"
            has_zip = zip_path.exists()
            self.btn_download_zip.setEnabled(has_zip or remote_time != 'Unknown')
            self.btn_download_zip.setText("📦 發現 ZIP，立即套用" if has_zip else f"☁️ 下載 ({remote_time})")
        except:
            pass

    def download_cloud_data(self):
        zip_path = self.project_root / "data" / "daily_data.zip"
        self.btn_download_zip.setEnabled(False)
        self.log("📡 啟動雲端數據下載...", True)
        self.progress.setRange(0, 0)
        if zip_path.exists():
            self.unzip_data()
            return
        self.dl_runner = ScriptRunner("git",
                                      ["checkout", "origin/main", "--", "data/daily_data.zip", "data/data_status.json"],
                                      use_python=False)
        self.dl_runner.output_signal.connect(self.log)
        self.dl_runner.finished.connect(lambda ec, es: self.unzip_data())
        self.dl_runner.start_script()

    def unzip_data(self):
        self.log("🔓 正在解壓縮並套用數據內容...")
        zip_path = self.project_root / "data" / "daily_data.zip"
        extract_target = self.project_root / "data"

        if zip_path.exists():
            try:
                with zipfile.ZipFile(zip_path, 'r') as z:
                    z.extractall(extract_target)
                time.sleep(0.2)
                os.remove(zip_path)
                self.log("✅ 數據套用成功，暫存包已清理。")
            except Exception as e:
                self.log(f"❌ 解壓縮失敗: {e}")
        else:
            self.log("❌ 找不到 ZIP 檔案。")

        self.btn_download_zip.setEnabled(True)
        self.btn_download_zip.setText("🔄 重新檢查雲端")
        self.progress.setRange(0, 100)
        self.progress.setValue(100)
        self.run_status_probe()

    # ==========================================
    # 策略參數與重算邏輯
    # ==========================================
    def set_inputs_enabled(self, enabled):
        self.is_editing = enabled
        for inp in self.inputs.values(): inp.setEnabled(enabled)
        self.btn_edit.setText("🔒 取消編輯" if enabled else "🔧 編輯模式")
        self.btn_edit.setStyleSheet(BTN_RESET if enabled else BTN_CHECK)
        if not enabled: self.update_action_button_text()

    def update_action_button_text(self):
        has_changed = any(
            abs(inp.value() - self.original_params.get(key, 0)) > 0.0001 for key, inp in self.inputs.items())
        self.btn_save_recalc.setText("💾 儲存並重算策略因子" if has_changed else "⚡ 僅重算策略因子")

    def toggle_30w_params(self):
        vis = self.container_30w.isVisible()
        self.container_30w.setVisible(not vis)
        self.btn_toggle_30w.setText("▼ 30W 策略參數設定 (點擊收合)" if not vis else "▶ 30W 策略參數設定 (點擊展開)")

    def toggle_edit_mode(self):
        if not self.is_editing:
            self.original_params = {k: inp.value() for k, inp in self.inputs.items()}
            self.set_inputs_enabled(True)
        else:
            for k, val in self.original_params.items(): self.inputs[k].setValue(val)
            self.set_inputs_enabled(False)

    def handle_action_click(self):
        self.task_start_time = time.time()
        txt = self.btn_save_recalc.text()
        self._disable_all_buttons()
        self.btn_save_recalc.setText("⏳ 執行中...")
        if "儲存" in txt:
            self.save_config()
            self.log("✅ 參數已儲存並啟動計算")
        else:
            self.log("🚀 參數未變動，直接執行重算")

        self.progress.setFormat("⏳ 單獨計算策略因子 - %p%")
        self.runner_recalc_only = ScriptRunner(self.project_root / "scripts" / "calc_snapshot_factors.py")
        self.runner_recalc_only.output_signal.connect(self.log)
        self.runner_recalc_only.progress_signal.connect(self.progress.setValue)
        self.runner_recalc_only.finished.connect(self.on_pipeline_finished)
        self.runner_recalc_only.start_script()

    def check_strategy_time(self):
        if self.strategy_result_path.exists():
            ts = self.strategy_result_path.stat().st_mtime
            self.lbl_strategy_time.setText(f"上次運算: {datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M')}")
        else:
            self.lbl_strategy_time.setText("上次運算: 無資料")

    def log(self, t, clear=False):
        if clear: self.log_output.clear()
        self.log_output.append(t.strip())
        self.log_output.verticalScrollBar().setValue(self.log_output.verticalScrollBar().maximum())

    def load_config(self):
        default_cfg = {k: d for k, _, _, d, _, _, _, _ in self.params_def}
        try:
            if self.config_path.exists():
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    cfg = json.load(f).get('30w_strategy', default_cfg)
            else:
                cfg = default_cfg
        except:
            cfg = default_cfg
        for key, inp in self.inputs.items():
            if key in cfg: inp.setValue(cfg[key])

    def _log_duration(self, start_time, label="執行耗時"):
        if start_time is None: return
        elapsed = time.time() - start_time
        m, s = divmod(elapsed, 60)
        self.log(f"⏱️ {label}: {int(m)}分 {int(s)}秒")

    def save_config(self):
        data = {"30w_strategy": {k: inp.value() for k, inp in self.inputs.items()}}
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            return True
        except:
            return False