import sys
import json
import os
import re
import shutil
import zipfile
from pathlib import Path
from datetime import datetime
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QPushButton, QGridLayout, QDoubleSpinBox,
                             QSpinBox, QScrollArea, QMessageBox, QProgressBar, QTextEdit, QFrame)
from PyQt6.QtCore import Qt, QTimer, QProcess, pyqtSignal

# --- 美學 CSS (保持原本你喜歡的樣式) ---
STYLES = """
    QWidget { font-family: "Segoe UI", "Microsoft JhengHei"; background-color: #121212; color: #E0E0E0; }
    QFrame.Card { background-color: #1E1E1E; border-radius: 12px; border: 1px solid #3E3E42; }
    QLabel.Title { font-size: 26px; font-weight: bold; color: #00E5FF; margin-bottom: 10px; }
    QLabel.CardTitle { font-size: 18px; font-weight: bold; color: #FFFFFF; border-bottom: 2px solid #00E5FF; padding-bottom: 5px; }
    QLabel.Label { font-size: 16px; color: #DDDDDD; font-weight: 500; }
    QLabel.Value { font-size: 16px; font-weight: bold; color: #FFFFFF; }
    QLabel.Desc { font-size: 14px; color: #888; font-style: italic; }

    QDoubleSpinBox, QSpinBox {
        background-color: #2D2D30; border: 1px solid #555; border-radius: 4px;
        padding: 6px; font-size: 16px; color: #00E5FF; font-weight: bold;
        min-width: 100px; max-width: 140px;
    }
    QPushButton {
        background-color: #505050; border: 1px solid #777; border-radius: 6px;
        padding: 8px 15px; font-size: 15px; color: white;
    }
    QPushButton:hover { background-color: #666; border-color: #FFF; }
    QPushButton.ActionBtn { background-color: #0078D4; border-color: #0099FF; font-weight: bold; }
    QPushButton.ActionBtn:hover { background-color: #1084E0; }
    QPushButton.DangerBtn { background-color: #C62828; border-color: #E57373; }
    QPushButton.DangerBtn:hover { background-color: #D32F2F; }
    QProgressBar {
        border: 1px solid #555; border-radius: 6px; text-align: center;
        background-color: #252526; color: white; font-weight: bold;
    }
    QProgressBar::chunk { background-color: #00E5FF; border-radius: 5px; }
    QTextEdit { background-color: #1E1E1E; border: 1px solid #3E3E42; border-radius: 6px; font-family: Consolas; color: #CCC; }
"""


class ScriptRunner(QProcess):
    output_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)

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
            self.output_signal.emit(text)
        except:
            pass


class SettingsModule(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet(STYLES)
        self.project_root = Path(__file__).resolve().parent.parent
        self.config_path = self.project_root / "data" / "strategy_config.json"

        self.init_ui()
        self.load_config()
        self.check_local_status()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(30, 30, 30, 30)
        main_layout.setSpacing(20)

        header = QLabel("系統控制台 System Dashboard")
        header.setProperty("class", "Title")
        main_layout.addWidget(header)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("background: transparent; border: none;")

        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setSpacing(25)
        content_layout.setContentsMargins(0, 0, 50, 0)

        # === 卡片 1: 雲端同步狀態 ===
        card_data = QFrame()
        card_data.setProperty("class", "Card")
        l_data = QVBoxLayout(card_data)
        l_data.setContentsMargins(25, 25, 25, 25)

        title_data = QLabel("☁️ 雲端運算與同步 (Cloud Sync)")
        title_data.setProperty("class", "CardTitle")
        l_data.addWidget(title_data)

        grid_data = QGridLayout()
        grid_data.setVerticalSpacing(15)
        grid_data.setHorizontalSpacing(15)

        # 標籤
        self.lbl_local_time = QLabel("--")
        self.lbl_local_time.setProperty("class", "Value")

        self.lbl_cloud_time = QLabel("尚未檢查")
        self.lbl_cloud_time.setProperty("class", "Value")

        # 排版
        grid_data.addWidget(QLabel("本機資料時間:"), 0, 0)
        grid_data.addWidget(self.lbl_local_time, 0, 1)
        grid_data.addWidget(QLabel("說明: 你電腦上目前的股價版本"), 0, 2)

        grid_data.addWidget(QLabel("雲端最新運算:"), 1, 0)
        grid_data.addWidget(self.lbl_cloud_time, 1, 1)
        grid_data.addWidget(QLabel("說明: GitHub Actions 每天下午跑完的時間"), 1, 2)

        grid_data.setColumnStretch(2, 1)
        for i in range(grid_data.rowCount()):
            item0 = grid_data.itemAtPosition(i, 0)
            if item0:
                item0.widget().setProperty("class", "Label")
                item0.widget().setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            item2 = grid_data.itemAtPosition(i, 2)
            if item2: item2.widget().setProperty("class", "Desc")

        l_data.addLayout(grid_data)

        # 按鈕區
        btn_layout = QHBoxLayout()
        self.btn_check_cloud = QPushButton("🔄 檢查雲端是否有新資料")
        self.btn_check_cloud.clicked.connect(self.check_cloud_status)

        self.btn_download_zip = QPushButton("☁️ 下載並套用雲端結果 (ZIP)")
        self.btn_download_zip.setProperty("class", "ActionBtn")
        self.btn_download_zip.setEnabled(False)  # 檢查到新資料才啟用
        self.btn_download_zip.clicked.connect(self.download_cloud_data)

        # 保留原本的手動更新，以防萬一
        self.btn_force_local = QPushButton("⚡ 本機重跑 (慢)")
        self.btn_force_local.setProperty("class", "DangerBtn")
        self.btn_force_local.clicked.connect(self.run_full_update_local)

        btn_layout.addWidget(self.btn_check_cloud)
        btn_layout.addWidget(self.btn_download_zip)
        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_force_local)
        l_data.addLayout(btn_layout)

        content_layout.addWidget(card_data)

        # === 卡片 2: 策略參數 (保持不變) ===
        card_param = QFrame()
        card_param.setProperty("class", "Card")
        l_param = QVBoxLayout(card_param)
        l_param.setContentsMargins(25, 25, 25, 25)

        title_param = QLabel("📈 策略參數微調 (僅影響本機重算)")
        title_param.setProperty("class", "CardTitle")
        l_param.addWidget(title_param)

        grid_param = QGridLayout()
        grid_param.setVerticalSpacing(12)
        grid_param.setHorizontalSpacing(15)

        self.inputs = {}
        params = [
            ('trigger_min_gain', '觸發漲幅門檻', 'float', 0.0, 0.5, 0.01, '預設 0.10'),
            ('trigger_vol_multiplier', '觸發量能倍數', 'float', 1.0, 10.0, 0.1, '預設 1.1'),
            ('adhesive_weeks', '黏貼週數', 'int', 1, 10, 1, '預設 2 週'),
            ('adhesive_bias', '黏貼乖離率', 'float', 0.01, 0.5, 0.01, '預設 0.12'),
            ('shakeout_lookback', '甩轎回溯週數', 'int', 4, 52, 1, '預設 12 週'),
            ('shakeout_max_depth', '甩轎最大深度', 'float', 0.05, 0.9, 0.05, '預設 0.35'),
            ('shakeout_underwater_limit', '甩轎水下限期', 'int', 1, 20, 1, '預設 10 週'),
            ('shakeout_prev_bias_limit', '甩轎前乖離限', 'float', 0.05, 0.5, 0.01, '預設 0.15'),
            ('signal_lookback_days', '訊號顯示天數', 'int', 1, 60, 1, '顯示近 N 天'),
        ]

        for i, (key, label, ptype, vmin, vmax, step, tip) in enumerate(params):
            lbl = QLabel(label)
            lbl.setProperty("class", "Label")
            lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

            if ptype == 'float':
                inp = QDoubleSpinBox()
                inp.setDecimals(2)
            else:
                inp = QSpinBox()
            inp.setRange(vmin, vmax)
            inp.setSingleStep(step)

            desc = QLabel(tip)
            desc.setProperty("class", "Desc")

            grid_param.addWidget(lbl, i, 0)
            grid_param.addWidget(inp, i, 1)
            grid_param.addWidget(desc, i, 2)
            self.inputs[key] = inp

        grid_param.setColumnStretch(2, 1)
        l_param.addLayout(grid_param)

        self.btn_save_recalc = QPushButton("💾 儲存並用現有資料重算")
        self.btn_save_recalc.setProperty("class", "ActionBtn")
        self.btn_save_recalc.clicked.connect(self.save_and_recalc)
        l_param.addWidget(self.btn_save_recalc)

        content_layout.addWidget(card_param)

        # Log & Progress
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setFixedHeight(120)
        content_layout.addWidget(self.log_output)

        self.progress = QProgressBar()
        self.progress.setValue(0)
        content_layout.addWidget(self.progress)

        content_layout.addStretch()
        scroll.setWidget(content)
        main_layout.addWidget(scroll)

    def load_config(self):
        # (保持原樣...)
        default_cfg = {
            "trigger_min_gain": 0.10, "trigger_vol_multiplier": 1.1,
            "adhesive_weeks": 2, "adhesive_bias": 0.12,
            "shakeout_lookback": 12, "shakeout_max_depth": 0.35,
            "shakeout_underwater_limit": 10, "shakeout_prev_bias_limit": 0.15,
            "signal_lookback_days": 10
        }
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

    def check_local_status(self):
        status_file = self.project_root / "data" / "data_status.json"
        if status_file.exists():
            try:
                with open(status_file, 'r') as f:
                    data = json.load(f)
                    self.lbl_local_time.setText(f"<span style='color:#00E676'>{data.get('update_time', '未知')}</span>")
            except:
                self.lbl_local_time.setText("格式錯誤")
        else:
            self.lbl_local_time.setText("無資料")

    # --- 新的核心功能：檢查雲端 ---
    def check_cloud_status(self):
        self.log("📡 正在連線 GitHub 檢查最新版本...", True)
        self.btn_check_cloud.setEnabled(False)
        # 使用 git fetch origin main (不合併) 來更新 remote 資訊
        self.runner = ScriptRunner("git", ["fetch", "origin", "main"], use_python=False)
        self.runner.output_signal.connect(self.log)
        self.runner.finished.connect(self.read_remote_json)
        self.runner.start_script()

    def read_remote_json(self):
        # 讀取遠端的 data_status.json 內容而不 checkout
        self.log("正在讀取遠端時間戳記...")
        self.status_runner = ScriptRunner("git", ["show", "origin/main:data/data_status.json"], use_python=False)
        self.status_runner.output_signal.connect(self.parse_remote_status)
        self.status_runner.start_script()

    def parse_remote_status(self, text):
        self.btn_check_cloud.setEnabled(True)
        try:
            # git show 可能會包含一些 header，我們嘗試找 JSON 部分
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match:
                data = json.loads(match.group(0))
                remote_time = data.get('update_time', 'Unknown')
                self.lbl_cloud_time.setText(f"<span style='color:#00E5FF'>{remote_time}</span>")

                self.log(f"✅ 雲端最新資料時間: {remote_time}")

                # 簡單比對 (字串比對即可，因為格式固定)
                local_txt = self.lbl_local_time.text().replace("<span style='color:#00E676'>", "").replace("</span>",
                                                                                                           "")
                if remote_time > local_txt:
                    self.btn_download_zip.setEnabled(True)
                    self.btn_download_zip.setText(f"☁️ 下載新資料 ({remote_time})")
                    self.log("🚀 發現新資料！請按藍色按鈕下載。")
                else:
                    self.btn_download_zip.setEnabled(False)
                    self.log("目前已是最新資料。")
            else:
                self.log("無法讀取遠端 JSON，可能檔案不存在或格式錯誤。")
                self.lbl_cloud_time.setText("讀取失敗")
        except Exception as e:
            self.log(f"解析錯誤: {e}")

    # --- 新的核心功能：下載並解壓 ---
    def download_cloud_data(self):
        self.btn_download_zip.setEnabled(False)
        self.log("📦 開始下載 data/daily_data.zip ...", True)

        # 使用 git checkout 下載單一檔案 (比 pull 整個 repo 快且安全)
        self.dl_runner = ScriptRunner("git",
                                      ["checkout", "origin/main", "--", "data/daily_data.zip", "data/data_status.json"],
                                      use_python=False)
        self.dl_runner.output_signal.connect(self.log)
        self.dl_runner.finished.connect(self.unzip_data)
        self.dl_runner.start_script()

    def unzip_data(self):
        zip_path = self.project_root / "data" / "daily_data.zip"
        if not zip_path.exists():
            self.log("❌ 下載失敗：找不到 zip 檔")
            return

        self.log("解壓縮資料中...")
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # 解壓到 data/ 目錄 (因為壓縮時包含了 cache/tw... 路徑)
                extract_path = self.project_root / "data"
                zip_ref.extractall(extract_path)

            self.log("✅ 解壓縮完成！資料已更新。")
            self.check_local_status()  # 更新介面顯示
            QMessageBox.information(self, "成功", "雲端資料已成功套用！")

            # 自動刪除 zip 節省空間
            os.remove(zip_path)

        except Exception as e:
            self.log(f"❌ 解壓失敗: {str(e)}")

    def run_full_update_local(self):
        reply = QMessageBox.question(self, "確認", "本機重跑需要很長時間，建議使用雲端下載。確定要跑？")
        if reply != QMessageBox.StandardButton.Yes: return
        self.log("🚀 開始本機下載...", True)
        script = self.project_root / "scripts" / "init_cache_tw.py"
        self.runner = ScriptRunner(script, ["--auto", "--force"], use_python=True)
        self.runner.output_signal.connect(self.log)
        self.runner.finished.connect(lambda: self.save_and_recalc())
        self.runner.start_script()

    def save_and_recalc(self):
        if not self.save_config(): return
        self.log("正在計算策略...", True)
        script = self.project_root / "scripts" / "calc_snapshot_factors.py"
        self.runner = ScriptRunner(script, use_python=True)
        self.runner.output_signal.connect(self.log)
        self.runner.finished.connect(lambda: self.log("✅ 運算完成！"))
        self.runner.start_script()

    def log(self, text, clear=False):
        if clear: self.log_output.clear()
        self.log_output.append(text.strip())