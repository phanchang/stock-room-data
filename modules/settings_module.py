import sys
import json
import os
import re
import shutil
import zipfile
import time
from pathlib import Path
from datetime import datetime
import pandas as pd

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QPushButton, QGridLayout, QDoubleSpinBox,
                             QSpinBox, QScrollArea, QMessageBox, QProgressBar,
                             QTextEdit, QFrame, QProgressDialog, QApplication)

from PyQt6.QtCore import Qt, QTimer, QProcess, pyqtSignal, QProcessEnvironment

# ==========================================
# 🎨 1. 介面基礎樣式 (拿掉奇怪的輸入框設計，回歸乾淨大字體)
# ==========================================
STYLES = """
    QWidget { font-family: "Segoe UI", "Microsoft JhengHei"; background-color: #121212; color: #E0E0E0; }

    QFrame#Card { background-color: #1E1E1E; border-radius: 12px; border: 1px solid #3E3E42; }

    QLabel#Title { font-size: 26px; font-weight: bold; color: #00E5FF; margin-bottom: 10px; }
    QLabel#CardTitle { font-size: 18px; font-weight: bold; color: #FFFFFF; }
    QLabel#Label { font-size: 16px; color: #FFFFFF; font-weight: bold; }
    QLabel#Value { font-size: 16px; font-weight: bold; color: #00E5FF; }
    QLabel#Desc { font-size: 14px; color: #BBBBBB; font-style: normal; }
    QLabel#StrategyTime { font-size: 14px; color: #FFEB3B; font-weight: bold; margin-right: 10px; }

    /* 單純把輸入框變大、背景變暗，不蓋掉系統預設的上下按鈕 */
    QDoubleSpinBox, QSpinBox {
        background-color: #2D2D30; 
        border: 1px solid #555; 
        border-radius: 4px;
        padding: 8px 10px;
        font-size: 20px; /* 字體放大 */
        color: #00E5FF; 
        font-weight: bold;
        min-width: 120px; 
        max-width: 160px;
    }

    /* 進度條與日誌區塊 */
    QProgressBar {
        border: 1px solid #555; border-radius: 6px; text-align: center;
        background-color: #252526; color: white; font-weight: bold;
        min-height: 20px;
    }
    QProgressBar::chunk { background-color: #00E5FF; border-radius: 5px; }
    QTextEdit { background-color: #1E1E1E; border: 1px solid #3E3E42; border-radius: 6px; font-family: Consolas; color: #CCC; font-size: 14px; }
"""

# ==========================================
# 🎨 2. 獨立按鈕樣式 (絕對保證 hover 會變色)
# ==========================================
BTN_ACTION = """
    QPushButton { background-color: #0066CC; border: 2px solid #004C99; border-radius: 6px; padding: 10px 15px; font-size: 16px; color: white; font-weight: bold; }
    QPushButton:hover { background-color: #3399FF; border: 2px solid #FFFFFF; }
    QPushButton:pressed { background-color: #004C99; border: 2px solid #003366; }
    QPushButton:disabled { background-color: #222222; border: 2px solid #333333; color: #666666; }
"""
BTN_CHECK = """
    QPushButton { background-color: #00897B; border: 2px solid #00695C; border-radius: 6px; padding: 10px 15px; font-size: 16px; color: white; font-weight: bold; }
    QPushButton:hover { background-color: #26A69A; border: 2px solid #FFFFFF; }
    QPushButton:pressed { background-color: #004D40; border: 2px solid #00332B; }
    QPushButton:disabled { background-color: #222222; border: 2px solid #333333; color: #666666; }
"""
BTN_DANGER = """
    QPushButton { background-color: #D32F2F; border: 2px solid #B71C1C; border-radius: 6px; padding: 10px 15px; font-size: 16px; color: white; font-weight: bold; }
    QPushButton:hover { background-color: #FF5252; border: 2px solid #FFFFFF; }
    QPushButton:pressed { background-color: #B71C1C; border: 2px solid #7F0000; }
    QPushButton:disabled { background-color: #222222; border: 2px solid #333333; color: #666666; }
"""
BTN_RESET = """
    QPushButton { background-color: #555555; border: 2px solid #444444; border-radius: 6px; padding: 10px 15px; font-size: 16px; color: #DDDDDD; font-weight: bold; }
    QPushButton:hover { background-color: #888888; border: 2px solid #FFFFFF; color: #FFFFFF; }
    QPushButton:pressed { background-color: #333333; border: 2px solid #222222; }
    QPushButton:disabled { background-color: #222222; border: 2px solid #333333; color: #666666; }
"""
BTN_TOGGLE = """
    QPushButton { background-color: #1E1E1E; border: 1px solid #3E3E42; text-align: left; font-size: 16px; color: #00E5FF; padding: 10px 15px; border-radius: 6px; font-weight: bold; }
    QPushButton:hover { background-color: #333337; border: 1px solid #00E5FF; color: #FFFFFF; }
    QPushButton:pressed { background-color: #111111; border: 1px solid #0099CC; }
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

        self.init_ui()
        self.load_config()
        self.check_local_status()
        self.check_strategy_time()
        self.set_inputs_enabled(False)

    def set_inputs_enabled(self, enabled):
        self.is_editing = enabled
        for inp in self.inputs.values():
            inp.setEnabled(enabled)

        self.btn_edit.setText("🔒 取消編輯" if enabled else "🔧 進入編輯模式")
        # 🔥 直接切換獨立樣式
        self.btn_edit.setStyleSheet(BTN_RESET if enabled else BTN_CHECK)

        if not enabled:
            self.update_action_button_text()

    def update_action_button_text(self):
        has_changed = False
        for key, inp in self.inputs.items():
            if abs(inp.value() - self.original_params.get(key, 0)) > 0.0001:
                has_changed = True
                break
        self.btn_save_recalc.setText("💾 儲存並重算" if has_changed else "⚡ 僅重算")

    def _create_label(self, text, style_class, tooltip=""):
        lbl = QLabel(text)
        lbl.setObjectName(style_class)
        if tooltip: lbl.setToolTip(tooltip)
        return lbl

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(30, 30, 30, 30)
        main_layout.setSpacing(20)

        header = self._create_label("系統控制台 System Dashboard", "Title")
        main_layout.addWidget(header)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("background: transparent; border: none;")

        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setSpacing(25)
        content_layout.setContentsMargins(0, 0, 50, 0)

        card_data = QFrame()
        card_data.setObjectName("Card")
        l_data = QVBoxLayout(card_data)
        l_data.setContentsMargins(25, 25, 25, 25)

        l_data.addWidget(self._create_label("☁️ 雲端運算與同步 (Cloud Sync)", "CardTitle"))

        grid_data = QGridLayout()
        grid_data.setVerticalSpacing(15)
        self.lbl_local_time = self._create_label("--", "Value")
        self.lbl_cloud_time = self._create_label("尚未檢查", "Value")

        grid_data.addWidget(self._create_label("本機資料時間:", "Label"), 0, 0)
        grid_data.addWidget(self.lbl_local_time, 0, 1)
        grid_data.addWidget(self._create_label("說明: 目前硬碟中 Parquet 的最後交易日", "Desc"), 0, 2)

        grid_data.addWidget(self._create_label("雲端最新運算:", "Label"), 1, 0)
        grid_data.addWidget(self.lbl_cloud_time, 1, 1)
        grid_data.addWidget(self._create_label("說明: GitHub 每天 15:30 產出的版本", "Desc"), 1, 2)

        grid_data.setColumnStretch(2, 1)
        l_data.addLayout(grid_data)
        l_data.addSpacing(15)

        btn_layout = QHBoxLayout()
        self.btn_check_cloud = QPushButton("🔄 檢查雲端是否有新資料")
        self.btn_check_cloud.setStyleSheet(BTN_CHECK)
        self.btn_check_cloud.clicked.connect(self.check_cloud_status)

        self.btn_download_zip = QPushButton("☁️ 下載並套用雲端結果 (ZIP)")
        self.btn_download_zip.setStyleSheet(BTN_ACTION)
        self.btn_download_zip.setEnabled(False)
        self.btn_download_zip.clicked.connect(self.download_cloud_data)

        self.btn_force_local = QPushButton("⚡ 本機重跑 (三部曲全面更新)")
        self.btn_force_local.setStyleSheet(BTN_DANGER)
        self.btn_force_local.setToolTip("警告：這將啟動 K線更新 ➔ 籌碼營收抓取 ➔ 策略計算")
        self.btn_force_local.clicked.connect(self.run_full_update_local)

        btn_layout.addWidget(self.btn_check_cloud)
        btn_layout.addWidget(self.btn_download_zip)
        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_force_local)
        l_data.addLayout(btn_layout)
        content_layout.addWidget(card_data)

        card_param = QFrame()
        card_param.setObjectName("Card")
        l_param = QVBoxLayout(card_param)
        l_param.setContentsMargins(25, 25, 25, 25)

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
        self.lbl_strategy_time.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        self.btn_edit = QPushButton("🔧 進入編輯模式")
        self.btn_edit.setStyleSheet(BTN_CHECK)
        self.btn_edit.clicked.connect(self.toggle_edit_mode)

        self.btn_reset = QPushButton("↺ 恢復預設")
        self.btn_reset.setStyleSheet(BTN_RESET)
        self.btn_reset.clicked.connect(self.restore_defaults)

        self.btn_save_recalc = QPushButton("⚡ 僅重算")
        self.btn_save_recalc.setStyleSheet(BTN_ACTION)
        self.btn_save_recalc.clicked.connect(self.handle_action_click)

        header_layout.addWidget(self.lbl_strategy_time)
        header_layout.addStretch()
        header_layout.addWidget(self.btn_edit)
        header_layout.addSpacing(10)
        header_layout.addWidget(self.btn_reset)
        header_layout.addSpacing(10)
        header_layout.addWidget(self.btn_save_recalc)

        l_30w.addLayout(header_layout)

        line = QFrame()
        line.setFrameShape(QFrame.Shape.HLine)
        line.setFrameShadow(QFrame.Shadow.Sunken)
        line.setStyleSheet("background-color: #333; max-height: 1px; margin: 10px 0px;")
        l_30w.addWidget(line)

        grid_p = QGridLayout()
        grid_p.setVerticalSpacing(12)
        self.inputs = {}
        self.params_def = [
            ('trigger_min_gain', '觸發漲幅門檻', 'float', 0.10, 0.0, 0.5, 0.01, '最低要求的漲幅 (例如 0.10 代表 10%)'),
            ('trigger_vol_multiplier', '觸發量能倍數', 'float', 1.1, 1.0, 10.0, 0.1, '當日成交量需大於 N 倍均量'),
            ('adhesive_weeks', '黏貼週數', 'int', 2, 1, 10, 1, '均線糾結至少維持幾週'),
            ('adhesive_bias', '黏貼乖離率', 'float', 0.12, 0.01, 0.5, 0.01, '均線間的距離容許值'),
            ('shakeout_lookback', '甩轎回溯週數', 'int', 12, 4, 52, 1, '檢查過去 N 週內是否有大跌甩轎'),
            ('shakeout_max_depth', '甩轎最大深度', 'float', 0.35, 0.05, 0.9, 0.05, '甩轎最深跌幅限制'),
            ('shakeout_underwater_limit', '甩轎水下限期', 'int', 10, 1, 20, 1, '股價潛伏在水下的最大週數'),
            ('shakeout_prev_bias_limit', '甩轎前乖離限', 'float', 0.15, 0.05, 0.5, 0.01, '起漲前的均線乖離率限制'),
            ('signal_lookback_days', '訊號顯示天數', 'int', 10, 1, 60, 1, '只顯示最近 N 天出現訊號的股票'),
        ]

        for i, (key, label, ptype, default, vmin, vmax, vstep, tip) in enumerate(self.params_def):
            lbl_item = self._create_label(label, "Label", tooltip=tip)
            grid_p.addWidget(lbl_item, i, 0)

            if ptype == 'float':
                inp = QDoubleSpinBox()
                inp.setDecimals(2)
            else:
                inp = QSpinBox()

            inp.setRange(vmin, vmax)
            inp.setSingleStep(vstep)
            inp.setValue(default)
            grid_p.addWidget(inp, i, 1)
            self.inputs[key] = inp

            desc_item = self._create_label(tip, "Desc")
            grid_p.addWidget(desc_item, i, 2)

        grid_p.setColumnStretch(2, 1)
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

    def toggle_30w_params(self):
        is_visible = self.container_30w.isVisible()
        self.container_30w.setVisible(not is_visible)
        if not is_visible:
            self.btn_toggle_30w.setText("▼ 30W 策略參數設定 (點擊收合)")
        else:
            self.btn_toggle_30w.setText("▶ 30W 策略參數設定 (點擊展開)")

    def restore_defaults(self):
        reply = QMessageBox.question(self, "恢復預設",
                                     "確定要將所有參數恢復為系統建議值嗎？\n(需要按下 [儲存並重算] 才會生效)",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            for key, label, ptype, default, vmin, vmax, vstep, tip in self.params_def:
                if key in self.inputs:
                    self.inputs[key].setValue(default)
            self.log("↺ 已恢復參數預設值 (請記得按儲存)")

    def toggle_edit_mode(self):
        if not self.is_editing:
            self.original_params = {k: inp.value() for k, inp in self.inputs.items()}
            self.set_inputs_enabled(True)
        else:
            for k, val in self.original_params.items():
                self.inputs[k].setValue(val)
            self.set_inputs_enabled(False)

    def handle_action_click(self):
        original_text = self.btn_save_recalc.text()
        self.btn_save_recalc.setEnabled(False)
        self.btn_force_local.setEnabled(False)
        self.btn_save_recalc.setText("⏳ 執行中...")

        if original_text == "💾 儲存並重算":
            self.save_config()
            self.log("✅ 參數已儲存並啟動計算")
        else:
            self.log("🚀 參數未變動，直接執行重算")

        self.save_and_recalc()
        self.set_inputs_enabled(False)

    def check_local_status(self):
        try:
            path = self.project_root / "data" / "cache" / "tw" / "2303_TW.parquet"
            if not path.exists():
                path = self.project_root / "data" / "cache" / "tw" / "1101_TW.parquet"

            if path.exists():
                df = pd.read_parquet(path)
                if not df.empty:
                    last_date = df.index[-1]
                    if isinstance(last_date, (int, float)):
                        dt = pd.to_datetime(last_date)
                    else:
                        dt = last_date
                    time_str = dt.strftime('%Y-%m-%d')
                    self.lbl_local_time.setText(f"<span style='color:#00E676'>{time_str}</span>")
                    return
            self.lbl_local_time.setText("<span style='color:#FF5252'>無快取資料</span>")
        except Exception as e:
            self.lbl_local_time.setText(f"<span style='color:#FF5252'>讀取失敗</span>")
            self.log(f"讀取 Parquet 錯誤: {e}")

    def check_strategy_time(self):
        if self.strategy_result_path.exists():
            ts = self.strategy_result_path.stat().st_mtime
            dt_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M')
            self.lbl_strategy_time.setText(f"上次運算: {dt_str}")
        else:
            self.lbl_strategy_time.setText("上次運算: 無資料")

    def check_cloud_status(self):
        self.log("📡 檢查雲端中...", True)
        self.btn_check_cloud.setEnabled(False)
        self.runner = ScriptRunner("git", ["fetch", "origin", "main"], use_python=False)
        self.runner.finished.connect(lambda exitCode, exitStatus: self.read_remote_json())
        self.runner.start_script()

    def read_remote_json(self):
        self.status_runner = ScriptRunner("git", ["show", "origin/main:data/data_status.json"], use_python=False)
        self.status_runner.output_signal.connect(self.parse_remote_status)
        self.status_runner.start_script()

    def parse_remote_status(self, text):
        self.btn_check_cloud.setEnabled(True)
        try:
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if not match:
                self.log("❌ 錯誤：找不到遠端 JSON。")
                return

            remote_time = json.loads(match.group(0)).get('update_time', 'Unknown')
            self.lbl_cloud_time.setText(f"<span style='color:#00E5FF'>{remote_time}</span>")

            zip_path = self.project_root / "data" / "daily_data.zip"
            has_zip = zip_path.exists()

            cache_dir = self.project_root / "data" / "cache" / "tw"
            has_files = cache_dir.exists() and any(cache_dir.glob("*.parquet"))

            local_time = "0000-00-00 00:00"
            status_file = self.project_root / "data" / "data_status.json"
            if status_file.exists():
                try:
                    with open(status_file, 'r') as f:
                        local_time = json.load(f).get('update_time', local_time)
                except:
                    pass

            self.log(f"🔎 狀態: [檔案: {has_files}] [Zip包: {has_zip}] [本機: {local_time}] [雲端: {remote_time}]")

            should_update = False
            button_text = "目前已是最新"

            if remote_time > local_time:
                should_update = True
                button_text = f"☁️ 下載並套用 ({remote_time})"
            elif has_zip:
                should_update = True
                button_text = "📦 偵測到新資料包，直接套用"
            elif not has_files or any(x in self.lbl_local_time.text() for x in ["待解壓縮", "無資料"]):
                should_update = True
                button_text = "📦 執行解壓縮套用" if has_zip else f"☁️ 下載並套用 ({remote_time})"

            self.btn_download_zip.setEnabled(should_update)
            self.btn_download_zip.setText(button_text)

        except Exception as e:
            self.log(f"❌ 解析錯誤: {e}")
            self.btn_download_zip.setEnabled(True)

    def download_cloud_data(self):
        zip_path = self.project_root / "data" / "daily_data.zip"
        self.btn_download_zip.setEnabled(False)
        self.progress.setValue(0)
        self.progress.setRange(0, 0)
        self.progress.setFormat("📡 正在從雲端獲取數據包...")
        self.log("📡 啟動雲端數據下載...", True)

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
        zip_path = self.project_root / "data" / "daily_data.zip"
        extract_target = self.project_root / "data"

        if not zip_path.exists():
            self.progress.setRange(0, 100)
            self.progress.setFormat("❌ 錯誤：找不到數據包 (ZIP)")
            self.log("❌ 錯誤：找不到數據包 (ZIP)")
            self.btn_download_zip.setEnabled(True)
            return

        self.log("🔓 正在解壓縮並套用數據內容...")
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                file_list = z.infolist()
                total_files = len(file_list)

                self.progress.setRange(0, total_files)

                for i, file in enumerate(file_list):
                    z.extract(file, extract_target)
                    self.progress.setValue(i + 1)
                    if i % 10 == 0 or i == total_files - 1:
                        percent = int((i + 1) / total_files * 100)
                        self.progress.setFormat(f"📦 正在套用數據: {percent}%")
                        QApplication.processEvents()

            self.log("✅ 數據套用成功。")
            self.check_local_status()

            if zip_path.exists():
                try:
                    time.sleep(0.2)
                    os.remove(zip_path)
                    self.log("🧹 暫存數據包已清理。")
                except Exception as e:
                    self.log(f"⚠️ 暫存檔自動刪除失敗(請手動刪除): {e}")

            self.progress.setFormat("✅ 資料同步完成")
            self.progress.setValue(total_files)
            QMessageBox.information(self, "成功", "雲端數據已成功同步並套用！")

        except Exception as e:
            self.log(f"❌ 解壓過程出錯: {str(e)}")
            self.progress.setRange(0, 100)
            self.progress.setFormat("❌ 套用失敗")

        self.btn_download_zip.setEnabled(True)
        self.btn_download_zip.setText("🔄 重新檢查雲端")

    def run_full_update_local(self):
        self.log("🚀 本機全面更新啟動 (1/3): 下載最新 K 線...", True)
        self.btn_force_local.setEnabled(False)
        self.btn_save_recalc.setEnabled(False)
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setFormat("⏳ 正在更新 K 線資料 (1/3) - %p%")

        self.runner_step1 = ScriptRunner(self.project_root / "scripts" / "init_cache_tw.py", ["--skip-check", "--auto"])
        self.runner_step1.output_signal.connect(self.log)
        self.runner_step1.progress_signal.connect(self.progress.setValue)

        self.runner_step1.finished.connect(lambda ec, es: self.run_update_chips_revenue())
        self.runner_step1.start_script()

    def run_update_chips_revenue(self):
        self.log("📊 K線更新完成。開始抓取籌碼與營收 (2/3)...", False)
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setFormat("⏳ 正在產生籌碼營收底稿 (2/3) - %p%")

        self.runner_step2 = ScriptRunner(self.project_root / "scripts" / "update_chips_revenue.py")
        self.runner_step2.output_signal.connect(self.log)
        self.runner_step2.progress_signal.connect(self.progress.setValue)

        self.runner_step2.finished.connect(lambda ec, es: self.save_and_recalc())
        self.runner_step2.start_script()

    def save_and_recalc(self):
        if not self.save_config(): return

        raw_path = self.project_root / "data" / "temp" / "chips_revenue_raw.csv"
        if not raw_path.exists():
            self.log("⚠️ 偵測到缺少籌碼底稿 (chips_revenue_raw.csv)，自動啟動補抓程序...")
            self.run_update_chips_revenue()
            return

        self.log("⚙️ 正在計算技術與籌碼因子 (3/3)...", False)
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setFormat("⏳ 正在計算策略因子 (3/3) - %p%")

        self.runner_step3 = ScriptRunner(self.project_root / "scripts" / "calc_snapshot_factors.py")
        self.runner_step3.output_signal.connect(self.log)
        self.runner_step3.progress_signal.connect(self.progress.setValue)

        self.runner_step3.finished.connect(lambda ec, es: self.on_recalc_finished())
        self.runner_step3.start_script()

    def on_recalc_finished(self):
        self.log("✅ 運算完成！")
        self.progress.setValue(100)
        self.progress.setFormat("✅ 策略快照與數據已全部更新完畢")
        self.check_strategy_time()
        self.check_local_status()

        self.btn_force_local.setEnabled(True)
        self.btn_save_recalc.setEnabled(True)
        self.update_action_button_text()

    def log(self, t, clear=False):
        if clear: self.log_output.clear()
        self.log_output.append(t.strip())
        self.log_output.verticalScrollBar().setValue(self.log_output.verticalScrollBar().maximum())

    def load_config(self):
        default_cfg = {k: d for k, _, _, d, _, _, _, _ in self.params_def}
        try:
            if self.config_path.exists():
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    cfg = data.get('30w_strategy', default_cfg)
            else:
                cfg = default_cfg
        except:
            cfg = default_cfg
        for key, inp in self.inputs.items():
            if key in cfg: inp.setValue(cfg[key])

    def save_config(self):
        new_cfg = {k: inp.value() for k, inp in self.inputs.items()}
        data = {"30w_strategy": new_cfg}
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            return True
        except:
            return False