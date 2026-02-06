import sys
import json
import os
import re
import shutil
import zipfile
import time  # <--- 確保這行有加進去
from pathlib import Path
from datetime import datetime
# 請修改檔案頂部的這一行
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QPushButton, QGridLayout, QDoubleSpinBox,
                             QSpinBox, QScrollArea, QMessageBox, QProgressBar,
                             QTextEdit, QFrame, QProgressDialog, QApplication) # <--- 確保有 QApplication

from PyQt6.QtCore import Qt, QTimer, QProcess, pyqtSignal # <--- 確保有 QProcess
# --- 美學 CSS ---
STYLES = """
    QWidget { font-family: "Segoe UI", "Microsoft JhengHei"; background-color: #121212; color: #E0E0E0; }
    QFrame.Card { background-color: #1E1E1E; border-radius: 12px; border: 1px solid #3E3E42; }

    QLabel.Title { font-size: 26px; font-weight: bold; color: #00E5FF; margin-bottom: 10px; }
    QLabel.CardTitle { font-size: 18px; font-weight: bold; color: #FFFFFF; }

    QLabel.Label { font-size: 16px; color: #FFFFFF; font-weight: bold; }
    QLabel.Value { font-size: 16px; font-weight: bold; color: #00E5FF; }
    QLabel.Desc { font-size: 14px; color: #BBBBBB; font-style: normal; }

    /* 策略時間標籤 */
    QLabel.StrategyTime { font-size: 14px; color: #FFEB3B; font-weight: bold; margin-right: 10px; }

    /* --- 輸入框與微調按鈕優化 --- */
    QDoubleSpinBox, QSpinBox {
        background-color: #2D2D30; 
        border: 1px solid #555; 
        border-radius: 4px;
        padding: 8px 10px;
        font-size: 18px;
        color: #00E5FF; 
        font-weight: bold;
        min-width: 100px; 
        max-width: 140px;
    }

    QDoubleSpinBox::up-button, QSpinBox::up-button,
    QDoubleSpinBox::down-button, QSpinBox::down-button {
        width: 35px;
        border-left: 1px solid #555;
        background-color: #3A3A3A;
        border-radius: 0px 4px 4px 0px;
    }

    QDoubleSpinBox::up-button:hover, QSpinBox::up-button:hover,
    QDoubleSpinBox::down-button:hover, QSpinBox::down-button:hover {
        background-color: #555555;
    }

    QDoubleSpinBox::up-button:pressed, QSpinBox::up-button:pressed,
    QDoubleSpinBox::down-button:pressed, QSpinBox::down-button:pressed {
        background-color: #00E5FF;
    }

    QDoubleSpinBox::up-arrow, QSpinBox::up-arrow,
    QDoubleSpinBox::down-arrow, QSpinBox::down-arrow {
        width: 12px; height: 12px;
    }

    QPushButton {
        background-color: #3A3A3A; 
        border: 1px solid #555; 
        border-radius: 6px;
        padding: 8px 15px; 
        font-size: 15px; 
        color: white;
        font-weight: bold;
    }
    QPushButton:hover { background-color: #505050; border-color: #FFF; }

    QPushButton.ActionBtn { background-color: #0078D4; border-color: #0099FF; }
    QPushButton.ActionBtn:hover { background-color: #1084E0; }

    QPushButton.CheckBtn { background-color: #009688; border-color: #4DB6AC; }
    QPushButton.CheckBtn:hover { background-color: #26A69A; }

    QPushButton.DangerBtn { background-color: #C62828; border-color: #E57373; }
    QPushButton.DangerBtn:hover { background-color: #D32F2F; }

    QPushButton.ResetBtn { background-color: #444; border-color: #888; color: #DDD; }
    QPushButton.ResetBtn:hover { background-color: #666; color: #FFF; border-color: #FFF; }

    QProgressBar {
        border: 1px solid #555; border-radius: 6px; text-align: center;
        background-color: #252526; color: white; font-weight: bold;
        min-height: 20px;
    }
    QProgressBar::chunk { background-color: #00E5FF; border-radius: 5px; }
    QTextEdit { background-color: #1E1E1E; border: 1px solid #3E3E42; border-radius: 6px; font-family: Consolas; color: #CCC; }
"""


class ScriptRunner(QProcess):
    output_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)  # 新增進度訊號

    def __init__(self, script_path, args=None, use_python=True):
        super().__init__()
        self.script_path = script_path
        self.args = args or []
        self.use_python = use_python
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

            # 解析進度條 (假設腳本輸出 PROGRESS: 50)
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

        # 策略結果檔案路徑 (用來檢查最後運算時間)
        self.strategy_result_path = self.project_root / "data" / "strategy_results" / "factor_snapshot.parquet"

        self.init_ui()
        self.load_config()
        self.check_local_status()
        self.check_strategy_time()  # 檢查策略時間

    def _create_label(self, text, style_class, tooltip=""):
        lbl = QLabel(text)
        lbl.setProperty("class", style_class)
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

        # === 雲端卡片 ===
        card_data = QFrame()
        card_data.setProperty("class", "Card")
        l_data = QVBoxLayout(card_data)
        l_data.setContentsMargins(25, 25, 25, 25)

        l_data.addWidget(self._create_label("☁️ 雲端運算與同步 (Cloud Sync)", "CardTitle"))

        grid_data = QGridLayout()
        grid_data.setVerticalSpacing(15)
        self.lbl_local_time = self._create_label("--", "Value")
        self.lbl_cloud_time = self._create_label("尚未檢查", "Value")

        grid_data.addWidget(self._create_label("本機資料時間:", "Label"), 0, 0)
        grid_data.addWidget(self.lbl_local_time, 0, 1)
        grid_data.addWidget(self._create_label("說明: 目前硬碟中的股價版本", "Desc"), 0, 2)

        grid_data.addWidget(self._create_label("雲端最新運算:", "Label"), 1, 0)
        grid_data.addWidget(self.lbl_cloud_time, 1, 1)
        grid_data.addWidget(self._create_label("說明: GitHub 每天 15:30 產出的版本", "Desc"), 1, 2)

        grid_data.setColumnStretch(2, 1)
        l_data.addLayout(grid_data)
        l_data.addSpacing(15)

        btn_layout = QHBoxLayout()
        self.btn_check_cloud = QPushButton("🔄 檢查雲端是否有新資料")
        self.btn_check_cloud.setProperty("class", "CheckBtn")
        self.btn_check_cloud.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_check_cloud.clicked.connect(self.check_cloud_status)

        self.btn_download_zip = QPushButton("☁️ 下載並套用雲端結果 (ZIP)")
        self.btn_download_zip.setProperty("class", "ActionBtn")
        self.btn_download_zip.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_download_zip.setEnabled(False)
        self.btn_download_zip.clicked.connect(self.download_cloud_data)

        self.btn_force_local = QPushButton("⚡ 本機重跑")
        self.btn_force_local.setProperty("class", "DangerBtn")
        self.btn_force_local.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_force_local.clicked.connect(self.run_full_update_local)

        btn_layout.addWidget(self.btn_check_cloud)
        btn_layout.addWidget(self.btn_download_zip)
        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_force_local)
        l_data.addLayout(btn_layout)
        content_layout.addWidget(card_data)

        # === 策略參數卡片 ===
        card_param = QFrame()
        card_param.setProperty("class", "Card")
        l_param = QVBoxLayout(card_param)
        l_param.setContentsMargins(25, 25, 25, 25)

        # 標題列
        header_layout = QHBoxLayout()
        header_label = self._create_label("📈 策略參數微調", "CardTitle")

        # 新增：策略上次運算時間標籤
        self.lbl_strategy_time = self._create_label("上次運算: --", "StrategyTime")
        self.lbl_strategy_time.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        self.btn_reset = QPushButton("↺ 恢復預設")
        self.btn_reset.setProperty("class", "ResetBtn")
        self.btn_reset.setToolTip("將所有參數重置為系統建議值")
        self.btn_reset.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_reset.clicked.connect(self.restore_defaults)

        self.btn_save_recalc = QPushButton("💾 儲存並重算")
        self.btn_save_recalc.setProperty("class", "ActionBtn")
        self.btn_save_recalc.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_save_recalc.clicked.connect(self.save_and_recalc)

        header_layout.addWidget(header_label)
        header_layout.addStretch()
        header_layout.addWidget(self.lbl_strategy_time)  # 放在按鈕左邊
        header_layout.addWidget(self.btn_reset)
        header_layout.addSpacing(10)
        header_layout.addWidget(self.btn_save_recalc)

        l_param.addLayout(header_layout)

        line = QFrame()
        line.setFrameShape(QFrame.Shape.HLine)
        line.setFrameShadow(QFrame.Shadow.Sunken)
        line.setStyleSheet("background-color: #00E5FF; max-height: 2px;")
        l_param.addWidget(line)
        l_param.addSpacing(10)

        grid_p = QGridLayout()
        self.inputs = {}
        self.params_def = [
            ('trigger_min_gain', '觸發漲幅門檻', 'float', 0.10, 0.0, 0.5, 0.01, '最低要求的漲幅 (例如 0.10 代表 10%)'),
            ('trigger_vol_multiplier', '觸發量能倍數', 'float', 1.1, 1.0, 10.0, 0.1,
             '當日成交量需大於 N 倍均量 (例如 1.1 倍)'),
            ('adhesive_weeks', '黏貼週數', 'int', 2, 1, 10, 1, '均線糾結至少維持幾週'),
            ('adhesive_bias', '黏貼乖離率', 'float', 0.12, 0.01, 0.5, 0.01, '均線間的距離容許值 (0.12 = 12%)'),
            ('shakeout_lookback', '甩轎回溯週數', 'int', 12, 4, 52, 1, '檢查過去 N 週內是否有大跌甩轎'),
            ('shakeout_max_depth', '甩轎最大深度', 'float', 0.35, 0.05, 0.9, 0.05, '甩轎最深跌幅限制 (0.35 = 35%)'),
            ('shakeout_underwater_limit', '甩轎水下限期', 'int', 10, 1, 20, 1, '股價潛伏在水下的最大週數'),
            ('shakeout_prev_bias_limit', '甩轎前乖離限', 'float', 0.15, 0.05, 0.5, 0.01,
             '起漲前的均線乖離率限制 (0.15 = 15%)'),
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
            inp.setToolTip(tip)

            grid_p.addWidget(inp, i, 1)
            self.inputs[key] = inp

            desc_item = self._create_label(tip, "Desc")
            grid_p.addWidget(desc_item, i, 2)

        grid_p.setColumnStretch(2, 1)
        l_param.addLayout(grid_p)
        content_layout.addWidget(card_param)

        # Log & Progress
        self.log_output = QTextEdit();
        self.log_output.setReadOnly(True);
        self.log_output.setFixedHeight(120)
        content_layout.addWidget(self.log_output)

        # 進度條
        self.progress = QProgressBar()
        self.progress.setValue(0)
        self.progress.setTextVisible(True)
        content_layout.addWidget(self.progress)

        scroll.setWidget(content)
        main_layout.addWidget(scroll)

    def restore_defaults(self):
        reply = QMessageBox.question(self, "恢復預設",
                                     "確定要將所有參數恢復為系統建議值嗎？\n(需要按下 [儲存並重算] 才會生效)",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            for key, label, ptype, default, vmin, vmax, vstep, tip in self.params_def:
                if key in self.inputs:
                    self.inputs[key].setValue(default)
            self.log("↺ 已恢復參數預設值 (請記得按儲存)")

    def check_local_status(self):
        status_file = self.project_root / "data" / "data_status.json"
        cache_dir = self.project_root / "data" / "cache" / "tw"
        has_files = cache_dir.exists() and any(cache_dir.glob("*.parquet"))

        if status_file.exists():
            try:
                with open(status_file, 'r') as f:
                    time_str = json.load(f).get('update_time', '未知')
                    if has_files:
                        self.lbl_local_time.setText(f"<span style='color:#00E676'>{time_str}</span>")
                    else:
                        self.lbl_local_time.setText(f"<span style='color:#FF5252'>⚠️ 待解壓縮 ({time_str})</span>")
            except:
                self.lbl_local_time.setText("格式錯誤")
        else:
            self.lbl_local_time.setText("無資料")

    def check_strategy_time(self):
        """檢查策略快照的最後修改時間"""
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
        self.runner.finished.connect(self.read_remote_json)
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

            # 定義 Zip 路徑
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

            # 在 Log 中多顯示 Zip 狀態，方便除錯
            self.log(f"🔎 狀態: [檔案: {has_files}] [Zip包: {has_zip}] [本機: {local_time}] [雲端: {remote_time}]")

            should_update = False
            button_text = "目前已是最新"

            # --- 核心邏輯優化 ---
            # 1. 優先判斷時間：如果雲端比較新，一定要下載
            if remote_time > local_time:
                should_update = True
                button_text = f"☁️ 下載並套用 ({remote_time})"

            # 2. 如果時間一樣，但發現有 Zip 檔（代表剛 pull 過），允許直接套用
            elif has_zip:
                should_update = True
                button_text = "📦 偵測到新資料包，直接套用"

            # 3. 基礎檢查：如果根本沒檔案，或顯示待解壓縮，就要開啟按鈕
            elif not has_files or any(x in self.lbl_local_time.text() for x in ["待解壓縮", "無資料"]):
                should_update = True
                # 沒檔案時，看是要從雲端拉還是解壓現有的
                button_text = "📦 執行解壓縮套用" if has_zip else f"☁️ 下載並套用 ({remote_time})"

            self.btn_download_zip.setEnabled(should_update)
            self.btn_download_zip.setText(button_text)

        except Exception as e:
            self.log(f"❌ 解析錯誤: {e}")
            self.btn_download_zip.setEnabled(True)

    import time  # 建議在檔案頂部補上 import time，用來優化動畫視覺感

    def download_cloud_data(self):
        zip_path = self.project_root / "data" / "daily_data.zip"

        # 建立進度對話框（統一視覺體驗）
        self.progress_dialog = QProgressDialog("正在準備數據套用...", None, 0, 0, self)
        self.progress_dialog.setWindowTitle("系統同步中")
        self.progress_dialog.setWindowModality(Qt.WindowModality.WindowModal)
        self.progress_dialog.setCancelButton(None)
        self.progress_dialog.show()
        QApplication.processEvents()  # 強制顯示對話框

        # 邏輯 A：如果本地已經有檔案（剛 pull 過）
        if zip_path.exists():
            self.log("📦 偵測到本地已存在數據包，直接開始套用...", True)
            self.progress_dialog.setLabelText("偵測到本地數據，正在執行解壓縮...")
            # 稍微停 0.5 秒讓使用者看到 Log，才不會覺得沒反應
            QApplication.processEvents()
            time.sleep(0.5)
            self.unzip_data()
            return

        # 邏輯 B：執行 Git 下載
        self.log("📡 執行 git checkout 從遠端獲取 zip...", True)
        self.btn_download_zip.setEnabled(False)
        self.progress.setRange(0, 0)  # 讓下方的進度條進入忙碌跑動模式

        self.dl_runner = ScriptRunner("git",
                                      ["checkout", "origin/main", "--", "data/daily_data.zip", "data/data_status.json"],
                                      use_python=False)
        self.dl_runner.output_signal.connect(self.log)
        self.dl_runner.finished.connect(self.unzip_data)
        self.dl_runner.start_script()

    def unzip_data(self):
        zip_path = self.project_root / "data" / "daily_data.zip"
        extract_target = self.project_root / "data"

        # 修改：先確保進度對話框的文字正確
        if hasattr(self, 'progress_dialog') and self.progress_dialog:
            self.progress_dialog.setLabelText("正在解壓縮數據，請稍候...")

        if not zip_path.exists():
            self.log("❌ 錯誤：找不到數據包 (ZIP)")
            if self.progress_dialog: self.progress_dialog.close()
            self.progress.setRange(0, 100)
            return

        self.log("🔓 正在解壓縮並套用數據內容...")
        success = False
        try:
            # 1. 執行解壓縮
            with zipfile.ZipFile(zip_path, 'r') as z:
                file_list = z.infolist()
                total_files = len(file_list)
                self.progress.setRange(0, total_files)

                for i, file in enumerate(file_list):
                    z.extract(file, extract_target)
                    self.progress.setValue(i + 1)
                    if i % 5 == 0:
                        QApplication.processEvents()

            # 2. 解壓成功後，先標記成功
            success = True
            self.log("✅ 數據解壓縮完成。")

        except Exception as e:
            self.log(f"❌ 解壓過程出錯: {str(e)}")

        # 關鍵修正：確保 zipfile 已經完全關閉（離開 with 區塊）後，再處理後續與刪除
        if success:
            try:
                self.check_local_status()
                # 嘗試刪除 ZIP，若被佔用則提示但不報錯
                if zip_path.exists():
                    # 稍微等待 handle 釋放
                    QApplication.processEvents()
                    os.remove(zip_path)
                    self.log("🧹 暫存數據包已清理。")
            except Exception as cleanup_e:
                self.log(f"⚠️ 數據已套用，但暫存檔清理失敗 (請手動刪除): {str(cleanup_e)}")

            self.progress.setRange(0, 100)
            self.progress.setValue(100)
            if self.progress_dialog: self.progress_dialog.close()
            QMessageBox.information(self, "成功", "數據已套用！\n本機資料時間已更新。")
        else:
            if self.progress_dialog: self.progress_dialog.close()
            self.progress.setRange(0, 100)

    def run_full_update_local(self):
        self.log("🚀 本機更新開始...", True)
        self.runner = ScriptRunner(self.project_root / "scripts" / "init_cache_tw.py", ["--auto", "--force"])
        self.runner.finished.connect(self.save_and_recalc)
        self.runner.start_script()

    def save_and_recalc(self):
        if not self.save_config(): return
        self.log("正在計算策略...", True)
        self.progress.setValue(0)  # 重置進度條

        self.runner = ScriptRunner(self.project_root / "scripts" / "calc_snapshot_factors.py")
        self.runner.output_signal.connect(self.log)
        self.runner.progress_signal.connect(self.progress.setValue)  # 連接進度訊號

        # 運算完成後，更新時間標籤
        self.runner.finished.connect(self.on_recalc_finished)
        self.runner.start_script()

    def on_recalc_finished(self):
        self.log("✅ 運算完成！")
        self.progress.setValue(100)
        self.check_strategy_time()  # 更新右上角時間

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