import sys
import json
import os
import re
import time
import zipfile
from pathlib import Path
from datetime import datetime
import requests

import pandas as pd

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QPushButton, QGridLayout, QDoubleSpinBox,
                             QSpinBox, QScrollArea, QMessageBox, QProgressBar,
                             QTextEdit, QFrame, QComboBox)

from PyQt6.QtCore import Qt, pyqtSignal, QProcess, QProcessEnvironment, QThread

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
        padding: 6px;
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

    # 1. 新增 cwd 參數
    def __init__(self, script_path, args=None, use_python=True, cwd=None):
        super().__init__()
        self.script_path = script_path
        self.args = args or []
        self.use_python = use_python

        # 2. 如果有指定 cwd，設定 QProcess 的執行目錄
        if cwd:
            self.setWorkingDirectory(str(cwd))

        env = QProcessEnvironment.systemEnvironment()
        env.insert("PYTHONIOENCODING", "utf-8")
        env.insert("PYTHONUNBUFFERED", "1")
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


class UnzipWorker(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(bool)

    def __init__(self, zip_path, extract_target):
        super().__init__()
        self.zip_path = zip_path
        self.extract_target = Path(extract_target)

    def run(self):
        try:
            self.log_signal.emit("🔓 正在解壓縮並執行智慧合併 (背景處理中)...")
            with zipfile.ZipFile(self.zip_path, 'r') as z:
                members = z.infolist()
                total = len(members)

                for i, member in enumerate(members):
                    target_path = self.extract_target / member.filename

                    if "fundamentals" in member.filename and member.filename.endswith(".json") and target_path.exists():
                        try:
                            with z.open(member) as zf:
                                self._smart_merge_json(target_path, zf)
                        except Exception as e:
                            pass
                    else:
                        z.extract(member, self.extract_target)

                    if i % max(1, total // 100) == 0:
                        self.progress_signal.emit(int((i / total) * 100))

            time.sleep(0.5)
            os.remove(self.zip_path)
            self.log_signal.emit("✅ 雲端資料套用成功 (已保留本機營收/財報狀態)。")
            self.progress_signal.emit(100)
            self.finished_signal.emit(True)
        except Exception as e:
            self.log_signal.emit(f"❌ 解壓縮失敗: {e}")
            self.finished_signal.emit(False)

    def _smart_merge_json(self, local_path, zf):
        try:
            with open(local_path, 'r', encoding='utf-8') as f:
                local_data = json.load(f)
        except:
            local_data = {}

        try:
            zip_data = json.loads(zf.read().decode('utf-8'))
        except:
            zip_data = {}

        merged_data = zip_data.copy()

        protected_keys = ['revenue', 'profitability', 'balance_sheet', 'cash_flow']
        for key in protected_keys:
            if key in local_data and local_data[key]:
                merged_data[key] = local_data[key]

        def get_date(data, key):
            items = data.get(key, [])
            return items[0].get("date", "1970-01-01") if items else "1970-01-01"

        if get_date(local_data, "institutional_investors") >= get_date(zip_data, "institutional_investors"):
            merged_data["institutional_investors"] = local_data.get("institutional_investors", [])

        if get_date(local_data, "margin_trading") >= get_date(zip_data, "margin_trading"):
            merged_data["margin_trading"] = local_data.get("margin_trading", [])

        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, indent=2, ensure_ascii=False)


class OfficialProbeWorker(QThread):
    """向官方詢問最新資料日期 (以證交所為基準，並檢查櫃買中心是否已跟上)"""
    log_signal = pyqtSignal(str)
    result_signal = pyqtSignal(dict)

    def run(self):
        self.log_signal.emit("🔍 [官方探測] 正在獲取最新發布日期與資料驗證...")
        headers = {'User-Agent': 'Mozilla/5.0'}
        status = {
            'twse_inst_date': '', 'tpex_inst_date': '',
            'twse_margin_date': '', 'tpex_margin_date': ''
        }

        try:
            # 1. 上市法人
            # ⚠️ 修復：移除 selectType=ALL，避免 TWSE API 沒給日期時回傳系統初始的 20171218
            url_twse_inst = "https://www.twse.com.tw/rwd/zh/fund/T86?response=json"
            self.log_signal.emit(f"   🔗 [Debug] 請求上市法人 URL: {url_twse_inst}")
            r1 = requests.get(url_twse_inst, headers=headers, timeout=5)
            if r1.status_code == 200 and r1.json().get('stat') == 'OK':
                raw_d1 = r1.json().get('date', '')
                self.log_signal.emit(f"   💡 [Debug] 上市法人回傳 date 原始值: {raw_d1}")
                status['twse_inst_date'] = raw_d1

            # 2. 上市資券
            # ⚠️ 修復：同樣移除 selectType=ALL，避免 API 預設回傳奇怪的未來時間 (如 2026)
            url_twse_margin = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?response=json"
            self.log_signal.emit(f"   🔗 [Debug] 請求上市資券 URL: {url_twse_margin}")
            r2 = requests.get(url_twse_margin, headers=headers, timeout=5)
            if r2.status_code == 200 and r2.json().get('stat') == 'OK':
                raw_d2 = r2.json().get('date', '')
                self.log_signal.emit(f"   💡 [Debug] 上市資券回傳 date 原始值: {raw_d2}")
                status['twse_margin_date'] = raw_d2

            # === 民國年轉換函數 ===
            def to_tpex_date(twse_date):
                if not twse_date or len(twse_date) != 8: return ""
                return f"{int(twse_date[:4]) - 1911}/{twse_date[4:6]}/{twse_date[6:8]}"

            # 3. 上櫃法人 (帶入上市法人的日期，並檢查是否真的有資料)
            d_inst_tpex = to_tpex_date(status['twse_inst_date'])
            if d_inst_tpex:
                url_tpex_inst = f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=EW&t=D&d={d_inst_tpex}&o=json"
                self.log_signal.emit(f"   🔗 [Debug] 請求上櫃法人 URL: {url_tpex_inst}")
                r3 = requests.get(url_tpex_inst, headers=headers, timeout=5)
                if r3.status_code == 200:
                    j3 = r3.json()
                    # 防呆檢查機制
                    if ('tables' in j3 and len(j3['tables']) > 0 and len(j3['tables'][0].get('data', [])) > 0) or ('aaData' in j3 and len(j3['aaData']) > 0):
                        status['tpex_inst_date'] = status['twse_inst_date'] # 格式對齊統一為 YYYYMMDD

            # 4. 上櫃資券 (帶入上市資券的日期，並檢查是否真的有資料)
            d_margin_tpex = to_tpex_date(status['twse_margin_date'])
            if d_margin_tpex:
                url_tpex_margin = f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&d={d_margin_tpex}&o=json"
                self.log_signal.emit(f"   🔗 [Debug] 請求上櫃資券 URL: {url_tpex_margin}")
                r4 = requests.get(url_tpex_margin, headers=headers, timeout=5)
                if r4.status_code == 200:
                    j4 = r4.json()
                    # 防呆檢查機制
                    if ('tables' in j4 and len(j4['tables']) > 0 and j4['tables'][0].get('totalCount', 0) > 0) or (j4.get('iTotalRecords', 0) > 0):
                        status['tpex_margin_date'] = status['twse_margin_date'] # 格式對齊統一為 YYYYMMDD

        except Exception as e:
            self.log_signal.emit(f"⚠️ [官方探測] 連線發生異常: {e}")

        self.result_signal.emit(status)

class FetchStatusWorker(QThread):
    """向 GitHub 直接請求 JSON 狀態檔"""
    result_signal = pyqtSignal(dict)
    def run(self):
        try:
            url = "https://raw.githubusercontent.com/phanchang/stock-room-data/main/data/data_status.json"
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                self.result_signal.emit(r.json())
            else:
                self.result_signal.emit({"error": f"HTTP {r.status_code}"})
        except Exception as e:
            self.result_signal.emit({"error": str(e)})

class DownloadZipWorker(QThread):
    """向 GitHub 直接下載 ZIP 壓縮檔"""
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(bool)
    log_signal = pyqtSignal(str)

    def __init__(self, target_path):
        super().__init__()
        self.target_path = target_path

    def run(self):
        url = "https://raw.githubusercontent.com/phanchang/stock-room-data/main/data/daily_data.zip"
        try:
            self.log_signal.emit("📡 連線至 GitHub 開始下載...")
            with requests.get(url, stream=True, timeout=15) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                downloaded = 0
                with open(self.target_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=65536): # 64KB 一次
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0:
                                self.progress_signal.emit(int((downloaded / total_size) * 100))
            self.progress_signal.emit(100)
            self.finished_signal.emit(True)
        except Exception as e:
            self.log_signal.emit(f"❌ 下載失敗: {e}")
            self.finished_signal.emit(False)


class SettingsModule(QWidget):
    data_updated = pyqtSignal()
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

        # ----------------- 卡片 1: 取得 K 線 -----------------
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

        # ----------------- 卡片 2: 狀態對齊 -----------------
        card_status = QFrame()
        card_status.setObjectName("Card")
        l_status = QVBoxLayout(card_status)
        l_status.setContentsMargins(20, 20, 20, 20)
        l_status.addWidget(self._create_label("🛡️ 本機資料狀態檢驗", "CardTitle"))
        l_status.addSpacing(10)

        h1 = QHBoxLayout()
        h1.addWidget(self._create_label("K 線最新:", "Label"))
        self.lbl_kline_date = self._create_label("--", "Value")
        h1.addWidget(self.lbl_kline_date)
        h1.addSpacing(20)

        h1.addWidget(self._create_label("法人最新:", "Label"))
        self.lbl_inst_date = self._create_label("--", "Value")
        h1.addWidget(self.lbl_inst_date)
        h1.addSpacing(20)

        h1.addWidget(self._create_label("資券最新:", "Label"))
        self.lbl_margin_date = self._create_label("--", "Value")
        h1.addWidget(self.lbl_margin_date)
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

        # ----------------- 卡片 3: 第二步：本機策略核心排程 -----------------
        card_pipeline = QFrame()
        card_pipeline.setObjectName("Card")
        l_pipe = QVBoxLayout(card_pipeline)
        l_pipe.setContentsMargins(20, 20, 20, 20)
        l_pipe.addWidget(self._create_label("💻 第二步：本機盤後策略運算排程", "CardTitle"))
        l_pipe.addWidget(
            self._create_label("說明: 每日 K 線就緒後，請執行【核心排程】更新個股深度籌碼並自動產出最終策略大表。", "Desc"))
        l_pipe.addSpacing(10)

        chips_layout = QHBoxLayout()
        chips_layout.setSpacing(10)
        self.combo_chips_mode = QComboBox()
        self.combo_chips_mode.addItems(
            ["🔄 完整更新 (耗時, 預設)", "📊 僅抓三大法人 (下午 17:00 後)", "💰 僅抓信用資券 (晚上 21:30 後)"])
        self.combo_chips_mode.setStyleSheet("""
            QComboBox { background-color: #2D2D30; color: #EEE; border: 1px solid #555; padding: 10px; font-size: 16px; border-radius: 6px; font-weight: bold;}
            QComboBox::drop-down { border: 0px; }
            QComboBox QAbstractItemView { background-color: #2D2D30; color: #EEE; selection-background-color: #0066CC; }
        """)

        self.btn_pipe_core = QPushButton("🚀 [核心排程] 深度籌碼與策略運算")
        self.btn_pipe_core.setStyleSheet(BTN_ACTION)
        self.btn_pipe_core.setToolTip("依據左側選單執行籌碼抓取，隨後產出策略大表。")
        # (大約在第 247 行，找到這段並在下方加入)
        self.btn_pipe_core = QPushButton("🚀 [核心排程] 深度籌碼與策略運算")
        self.btn_pipe_core.setStyleSheet(BTN_ACTION)
        self.btn_pipe_core.setToolTip("依據左側選單執行籌碼抓取，隨後產出策略大表。")
        self.btn_pipe_core.clicked.connect(self.run_pre_flight_check)

        # 👇 新增：深夜一條龍專用按鈕 👇
        self.btn_night_dragon = QPushButton("🌙 [深夜吃到飽] 官方嚴格驗證 + 一鍵全下載運算")
        self.btn_night_dragon.setStyleSheet("""
                    QPushButton { background-color: #6A1B9A; border: 2px solid #4A148C; border-radius: 6px; padding: 10px 15px; font-size: 16px; color: white; font-weight: bold; }
                    QPushButton:hover { background-color: #8E24AA; border: 2px solid #FFFFFF; }
                    QPushButton:disabled { background-color: #222222; border: 2px solid #333333; color: #666666; }
                """)
        self.btn_night_dragon.clicked.connect(self.run_night_dragon_pipeline)

        # 原本的排版改為把這顆新按鈕加進去
        chips_layout.addWidget(self.combo_chips_mode, 3)
        chips_layout.addWidget(self.btn_pipe_core, 4)
        chips_layout.addWidget(self.btn_night_dragon, 4)
        l_pipe.addLayout(chips_layout)
        finance_layout = QHBoxLayout()
        finance_layout.setSpacing(10)
        self.combo_finance_mode = QComboBox()
        self.combo_finance_mode.addItems(["⚡ 僅更新月營收 (快)", "🔥 更新營收+財報 (慢)", "📄 僅更新財報 (慢)"])
        self.combo_finance_mode.setStyleSheet("""
            QComboBox { background-color: #2D2D30; color: #EEE; border: 1px solid #555; padding: 10px; font-size: 16px; border-radius: 6px; font-weight: bold;}
            QComboBox::drop-down { border: 0px; }
            QComboBox QAbstractItemView { background-color: #2D2D30; color: #EEE; selection-background-color: #0066CC; }
        """)

        self.btn_pipe_finance = QPushButton("🏦 執行基本面更新")
        self.btn_pipe_finance.setStyleSheet(BTN_RESET)
        self.btn_pipe_finance.setToolTip("依據左側選單執行。\n⚠️ 更新後會自動觸發因子大表重算。")
        self.btn_pipe_finance.clicked.connect(self.run_pipeline_financials)

        finance_layout.addWidget(self.combo_finance_mode, 4)
        finance_layout.addWidget(self.btn_pipe_finance, 6)
        l_pipe.addLayout(finance_layout)

        # 🔥 這裡把概念與產業按鈕加回來了！
        concept_layout = QHBoxLayout()
        self.btn_pipe_concepts = QPushButton("🏷️ 更新題材概念股")
        self.btn_pipe_concepts.setStyleSheet(BTN_RESET)
        self.btn_pipe_concepts.clicked.connect(self.run_pipeline_concepts)

        self.btn_pipe_mdj_ind = QPushButton("🏭 更新 MDJ 細產業")
        self.btn_pipe_mdj_ind.setStyleSheet(BTN_RESET)
        self.btn_pipe_mdj_ind.clicked.connect(self.run_pipeline_mdj_ind)

        concept_layout.addWidget(self.btn_pipe_concepts)
        concept_layout.addWidget(self.btn_pipe_mdj_ind)
        l_pipe.addLayout(concept_layout)
        content_layout.addWidget(card_pipeline)

        # ----------------- 卡片 4: 策略參數 -----------------
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
        inst_date = "無資料"
        margin_date = "無資料"
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
                        margin = data.get("margin_trading", [])
                        if inst: inst_date = inst[0].get("date", "無資料")
                        if margin: margin_date = margin[0].get("date", "無資料")
                        break
                except:
                    pass

        self.lbl_kline_date.setText(kline_date)
        self.lbl_inst_date.setText(inst_date)
        self.lbl_margin_date.setText(margin_date)
        self.lbl_fund_status.setText(fund_date)

        if kline_date == inst_date == margin_date and kline_date != "無資料":
            self.lbl_align_status.setText("🟢 系統資料已完全對齊 (K線/法人/資券)")
            self.lbl_align_status.setObjectName("Success")
        elif kline_date == inst_date and margin_date != kline_date and kline_date != "無資料":
            self.lbl_align_status.setText("🟡 法人已齊，資券尚未更新 (請於 21:00 後再次執行)")
            self.lbl_align_status.setObjectName("Warning")
        elif kline_date == "無資料" and inst_date == "無資料":
            self.lbl_align_status.setText("⚪ 尚無資料")
            self.lbl_align_status.setObjectName("Value")
        else:
            self.lbl_align_status.setText("🔴 日期未對齊 (請執行核心排程更新籌碼)")
            self.lbl_align_status.setObjectName("Warning")

        self.lbl_align_status.style().unpolish(self.lbl_align_status)
        self.lbl_align_status.style().polish(self.lbl_align_status)

        now = datetime.now()
        is_weekday = now.weekday() < 5
        self.market_ready = not (is_weekday and 8 <= now.hour < 14)

    def run_pipeline_mdj_ind(self):
        if not self._foolproof_check("MDJ 細產業更新"): return
        self.task_start_time = time.time()
        self.log("🏭 [附加] 啟動: 兩層式產業分類抓取 (update_industries.py)...", True)
        self._disable_all_buttons()
        self.progress.setFormat("⏳ 抓取 MDJ 產業分類中 - %p%")

        self.runner_mdj_ind = ScriptRunner(self.project_root / "scripts" / "update_industries.py")
        self.runner_mdj_ind.output_signal.connect(self.log)
        self.runner_mdj_ind.progress_signal.connect(self.progress.setValue)
        self.runner_mdj_ind.finished.connect(self._proceed_to_calc_factors)
        self.runner_mdj_ind.start_script()

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

    def run_pre_flight_check(self):
        if not self._foolproof_check("深度籌碼與策略運算"): return
        self._disable_all_buttons()
        self.progress.setFormat("🔍 正在極速探測 MoneyDJ 今日資料公布狀態...")
        self.probe_text = ""

        self.runner_probe = ScriptRunner(self.project_root / "scripts" / "check_market_ready.py")
        self.runner_probe.output_signal.connect(self._collect_probe_output)
        self.runner_probe.finished.connect(self._analyze_probe_and_decide)
        self.runner_probe.start_script()

    def _collect_probe_output(self, text):
        self.probe_text += text

    def _analyze_probe_and_decide(self):
        inst_match = re.search(r"PROBE_INST_DATE:(.*)", self.probe_text)
        margin_match = re.search(r"PROBE_MARGIN_DATE:(.*)", self.probe_text)

        inst_date = inst_match.group(1).strip() if inst_match else "未知"
        margin_date = margin_match.group(1).strip() if margin_match else "未知"

        now = datetime.now()
        today_str1 = now.strftime('%Y/%m/%d')
        today_str2 = now.strftime('%Y-%m-%d')

        is_inst_today = (today_str1 in inst_date) or (today_str2 in inst_date)
        is_margin_today = (today_str1 in margin_date) or (today_str2 in margin_date)

        if now.weekday() < 5 and now.hour >= 15:
            # 🔥 如果使用者選擇 "僅抓三大法人"，就不需要警告資券還沒公布
            mode_idx = self.combo_chips_mode.currentIndex()
            if mode_idx != 1 and is_inst_today and not is_margin_today:
                reply = QMessageBox.warning(
                    self, "融資券尚未公布 ⚠️",
                    f"【MoneyDJ 即時狀態探測】\n"
                    f"📊 法人資料：{inst_date} (已公布)\n"
                    f"💰 融資券資料：{margin_date} (尚未公布)\n\n"
                    f"⚠️ 台灣股市融資券通常在 21:00 後才會完整公布。\n"
                    f"現在執行會導致今日融資券資料缺漏，確定要強制執行嗎？",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                )
                if reply == QMessageBox.StandardButton.No:
                    self.log("❌ 使用者取消執行：等待融資券公布。")
                    self._cancel_and_restore_ui()
                    return

        self.run_core_pipeline()

    def _cancel_and_restore_ui(self):
        self.btn_check_cloud.setEnabled(True)
        self.btn_fallback_kline.setEnabled(True)
        self.btn_pipe_core.setEnabled(True)
        self.btn_pipe_finance.setEnabled(True)
        self.btn_save_recalc.setEnabled(True)
        if "下載" in self.btn_download_zip.text() or "套用" in self.btn_download_zip.text():
            self.btn_download_zip.setEnabled(True)

        self.progress.setValue(0)
        self.progress.setFormat("已取消排程")

    def run_core_pipeline(self):
        """啟動核心排程：包含籌碼抓取、估值更新與因子計算"""
        if not self._foolproof_check("深度籌碼與策略運算"): return
        self.task_start_time = time.time()

        # 預設為 False，只有腳本回傳 [DATA_UPDATED] 才會變成 True
        self.found_new_data = False

        # --- 重新定義 args 邏輯 (修正 NameError) ---
        mode_idx = self.combo_chips_mode.currentIndex()
        args = []
        mode_log = "完整更新"
        if mode_idx == 1:
            args = ["--mode", "inst"]
            mode_log = "僅抓三大法人"
        elif mode_idx == 2:
            args = ["--mode", "margin"]
            mode_log = "僅抓信用資券"

        self.log(f"🚀 [核心] 啟動: 深度籌碼更新 ({mode_log}) (1/4)...", True)
        self._disable_all_buttons()
        self.progress.setFormat("⏳ 抓取深度籌碼中 (1/4) - %p%")

        # 啟動腳本
        self.runner_daily = ScriptRunner(self.project_root / "scripts" / "update_daily_chips_v2.py", args)

        # 攔截輸出以判斷是否有新資料
        self.runner_daily.output_signal.connect(self._check_if_no_data)
        self.runner_daily.progress_signal.connect(self.progress.setValue)

        # 結束後根據狀態決定是否往下跑
        self.runner_daily.finished.connect(self._handle_daily_finished)
        self.runner_daily.start_script()

    def _handle_daily_finished(self, exitCode):
        if exitCode != 0:
            self.log("❌ 籌碼抓取腳本發生錯誤 (Crash)，已自動終止排程。")
            self.on_pipeline_finished()
            return

        if not getattr(self, 'found_new_data', False):
            self.log("🛑 偵測到今日無新籌碼，自動取消後續運算。")
            self.on_pipeline_finished()
            return

        mode_idx = self.combo_chips_mode.currentIndex()

        if mode_idx == 1:
            # 🕒 【下午 17:00 模式：僅抓法人】
            self.log("✅ 三大法人已更新完畢！")
            self.log("⚡ 啟動極速熱更新：將最新法人資料注入戰情室大表 (不重算 K 線)...")
            self.progress.setFormat("⏳ 極速注入籌碼資料中...")

            # 呼叫 --fast 極速注入，算完直接結束
            self.runner_calc = ScriptRunner(self.project_root / "scripts" / "calc_snapshot_factors.py", ["--fast"])
            self.runner_calc.output_signal.connect(self.log)
            self.runner_calc.progress_signal.connect(self.progress.setValue)
            self.runner_calc.finished.connect(self.on_pipeline_finished)
            self.runner_calc.start_script()

        elif mode_idx == 2:
            # 🌙 【晚上 21:30 模式：僅抓資券】
            self.log("✅ 信用資券已更新完畢！")
            self.log("⚡ 啟動極速熱更新：將最新資券資料注入戰情室大表 (不重算 K 線)...")
            self.progress.setFormat("⏳ 極速注入籌碼資料中...")

            # 呼叫 --fast 極速注入，算完直接結束 (因為產業板塊 K 線跟資券無關，不需要每天重配)
            self.runner_calc = ScriptRunner(self.project_root / "scripts" / "calc_snapshot_factors.py", ["--fast"])
            self.runner_calc.output_signal.connect(self.log)
            self.runner_calc.progress_signal.connect(self.progress.setValue)
            self.runner_calc.finished.connect(self.on_pipeline_finished)
            self.runner_calc.start_script()

        elif mode_idx == 0:
            # 🌑 【深夜：完整更新】(如果你有幾天沒開電腦，要從頭跑的時候用)
            self.log("✅ 雙籌碼已更新完畢！準備執行晚間最終結算...")
            self._proceed_to_market_yield()

    def _check_if_no_data(self, text):
        """攔截腳本日誌，判斷執行狀態"""
        self.log(text)

        # 1. 偵測是否成功更新
        if "[DATA_UPDATED]" in text:
            self.found_new_data = True

        # 2. 偵測是否為空更新或報錯
        if "[EMPTY_UPDATE]" in text or "Traceback" in text:
            self.found_new_data = False

    def _handle_daily_finished(self, exitCode):
        if exitCode != 0:
            self.log("❌ 籌碼抓取腳本發生錯誤(Crash)，停止後續排程。")
            self.on_pipeline_finished()
        else:
            self._proceed_to_market_yield()

    # 🔥 新增：用來攔截腳本印出來的文字，判斷有沒有抓到資料
    def _check_if_no_data(self, text):
        self.log(text)
        # 如果出現腳本崩潰(Traceback)或我們自定義的空標記，就設為 False
        if "Traceback" in text or "[EMPTY_UPDATE]" in text:
            self.log("🛑 偵測到腳本錯誤或無新資料，將取消後續運算。", False)
            self.found_new_data = False
        if "[DATA_UPDATED]" in text:
            self.found_new_data = True

    def _proceed_to_market_yield(self):
        # 🔥 新增煞車機制：如果上面發現沒抓到資料，就停止執行
        if getattr(self, 'found_new_data', True) is False:
            self.log("🛑 偵測到今日查無新籌碼資料，自動取消後續運算以節省時間。")
            self.on_pipeline_finished()
            return

        self.log("📊 [核心] 啟動: 全市場估值更新 (update_market_yield.py) (2/4)...", False)
        self.progress.setFormat("⏳ 抓取全市場估值 (2/4) - %p%")

        self.runner_yield = ScriptRunner(self.project_root / "scripts" / "update_market_yield.py")
        self.runner_yield.output_signal.connect(self.log)
        self.runner_yield.progress_signal.connect(self.progress.setValue)
        self.runner_yield.finished.connect(self._proceed_to_calc_factors)
        self.runner_yield.start_script()

    def run_pipeline_financials(self):
        mode_idx = self.combo_finance_mode.currentIndex()
        mode_names = ["僅更新月營收", "更新營收與財報", "僅更新財報"]
        mode_name = mode_names[mode_idx]
        if not self._foolproof_check(f"基本面更新 ({mode_name})"): return

        self.task_start_time = time.time()
        self.log(f"🏦 [附加] 啟動: {mode_name} (update_financials.py)...", True)
        self._disable_all_buttons()
        self.progress.setFormat("⏳ 抓取資料中 - %p%")

        args = []
        if mode_idx >= 1: args = ["--full"]

        self.runner_finance = ScriptRunner(self.project_root / "scripts" / "update_financials.py", args)
        self.runner_finance.output_signal.connect(self.log)
        self.runner_finance.progress_signal.connect(self.progress.setValue)
        self.runner_finance.finished.connect(self._proceed_to_calc_factors)
        self.runner_finance.start_script()

    def run_pipeline_concepts(self):
        if not self._foolproof_check("概念股分類更新"): return

        self.task_start_time = time.time()
        self.log("🏷️ [附加] 啟動: 概念股族群抓取 (update_concepts.py)...", True)
        self._disable_all_buttons()
        self.btn_pipe_concepts.setEnabled(False)
        self.progress.setFormat("⏳ 抓取概念股分類中 - %p%")

        self.runner_concepts = ScriptRunner(self.project_root / "scripts" / "update_concepts.py")
        self.runner_concepts.output_signal.connect(self.log)
        self.runner_concepts.progress_signal.connect(self.progress.setValue)
        self.runner_concepts.finished.connect(self._proceed_to_calc_factors)
        self.runner_concepts.start_script()

    def _proceed_to_calc_factors(self):
        if hasattr(self, 'task_start_time'): self._log_duration(self.task_start_time, "資料抓取耗時")
        if not self.save_config(): return
        self.log("⚙️ 啟動: 融合深度籌碼與策略運算 (calc_snapshot_factors.py) (3/4)...", False)
        self.progress.setFormat("⏳ 計算策略因子 - %p%")

        self.runner_calc = ScriptRunner(self.project_root / "scripts" / "calc_snapshot_factors.py")
        self.runner_calc.output_signal.connect(self.log)
        self.runner_calc.progress_signal.connect(self.progress.setValue)
        # 🔥 修改點：算完大表後，不要結束，改去呼叫 _proceed_to_build_industry
        self.runner_calc.finished.connect(self._proceed_to_build_industry)
        self.runner_calc.start_script()

    # 👇 新增：第四階段，合成板塊 K 線 👇
    def _proceed_to_build_industry(self, exitCode):
        if exitCode != 0:
            self.log("❌ 策略因子計算發生錯誤，停止後續排程。")
            self.on_pipeline_finished()
            return

        self.log("📊 啟動: 市值加權板塊合成 (build_industry_kline.py) (4/4)...", False)
        self.progress.setFormat("⏳ 正在合成板塊 K 線 (最後階段) - %p%")

        # 啟動腳本
        self.runner_industry = ScriptRunner(self.project_root / "scripts" / "build_industry_kline.py")
        self.runner_industry.output_signal.connect(self.log)
        self.runner_industry.progress_signal.connect(self.progress.setValue)
        # 結束後才呼叫 on_pipeline_finished
        self.runner_industry.finished.connect(self.on_pipeline_finished)
        self.runner_industry.start_script()
    # 👆 新增結束 👆

    # ==========================================
    # 🌙 深夜一條龍模式 (官方嚴格驗證 -> 雲端解壓縮 -> 雙籌碼 -> 估值 -> 完整因子 -> 產業板塊)
    # ==========================================
    def run_night_dragon_pipeline(self):
        if not self._foolproof_check("深夜一鍵全下載"): return
        self._disable_all_buttons()
        self.btn_night_dragon.setEnabled(False)
        self.log("==========================================", True)
        self.log("🌙 啟動【深夜吃到飽】一條龍排程")
        self.progress.setRange(0, 0)
        self.progress.setFormat("🔍 正在向證交所與櫃買中心確認今日資料...")

        # 啟動官方探測
        self.probe_worker = OfficialProbeWorker()
        self.probe_worker.log_signal.connect(self.log)
        self.probe_worker.result_signal.connect(self._on_dragon_probe_result)
        self.probe_worker.start()

    def _on_dragon_probe_result(self, status):
        self.progress.setRange(0, 100)
        self.progress.setValue(0)

        # 日期標準化函數 (YYYYMMDD 轉 YYYY-MM-DD)
        def normalize_date(d_str):
            if not d_str or len(d_str) != 8: return "尚未公布"
            return f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:8]}"

        d_twse_inst = normalize_date(status.get('twse_inst_date'))
        d_twse_margin = normalize_date(status.get('twse_margin_date'))
        d_tpex_inst = normalize_date(status.get('tpex_inst_date'))
        d_tpex_margin = normalize_date(status.get('tpex_margin_date'))

        dates = [d_twse_inst, d_twse_margin, d_tpex_inst, d_tpex_margin]

        # 判斷四大資料是否都已經有日期 (如果有人是"尚未公布"，就代表今天資料還不齊全)
        if "尚未公布" in dates:
            official_ready_date = "尚未公布"
        else:
            official_ready_date = min(dates)  # 如果都公布了，取最舊的那天為「全面就緒日」

        log_msg = (
            f"📊 官方資料最新就緒狀態：\n"
            f"   - 上市法人: {d_twse_inst}\n"
            f"   - 上櫃法人: {d_tpex_inst}\n"
            f"   - 上市資券: {d_twse_margin}\n"
            f"   - 上櫃資券: {d_tpex_margin}\n"
            f"   📌 官方全面就緒日期: {official_ready_date}"
        )
        self.log(log_msg)

        # 取得本機最新的 K 線日期
        local_date = self.lbl_kline_date.text()
        if local_date == "無資料" or local_date == "--":
            local_date = "1970-01-01"

        # 核心邏輯：如果官方已全面就緒，且日期「大於」本機，無條件放行飆速下載！
        if official_ready_date != "尚未公布" and official_ready_date > local_date:
            self.log(f"🟢 發現官方有新資料 ({official_ready_date} > 本機 {local_date})，放行飆速下載！")
            self.task_start_time = time.time()
            self._dragon_step_1_git_pull()
        else:
            if official_ready_date == "尚未公布":
                reason = "部分官方資料尚未公布 (請留意是否為晚上九點前)。"
            else:
                reason = f"官方全面就緒資料 ({official_ready_date}) 並未大於本機資料 ({local_date})。"

            self.log(f"🛑 {reason}")
            reply = QMessageBox.question(
                self, "資料已是最新 / 尚未發布",
                f"{log_msg}\n\n本機目前資料日期: {local_date}\n\n"
                f"⚠️ {reason}\n\n"
                f"請問仍要強制重新下載並執行運算嗎？",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.Yes:
                self.log("⚠️ 使用者選擇強制重新執行...")
                self.task_start_time = time.time()
                self._dragon_step_1_git_pull()
            else:
                self.log("✅ 已取消執行。")
                self.on_pipeline_finished()
                self.btn_night_dragon.setEnabled(True)

    def _dragon_step_1_git_pull(self):
        self.log("📡 [1/6] 啟動高速直接下載 K 線資料包...")
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setFormat("📦 下載雲端 K 線中 - %p%")

        zip_path = self.project_root / "data" / "daily_data.zip"
        # 如果舊的 ZIP 還在，先刪除避免干擾
        if zip_path.exists():
            try:
                os.remove(zip_path)
            except:
                pass

        self.dragon_dl = DownloadZipWorker(zip_path)
        self.dragon_dl.log_signal.connect(self.log)
        self.dragon_dl.progress_signal.connect(self.progress.setValue)
        self.dragon_dl.finished_signal.connect(self._dragon_step_1_5_checkout)
        self.dragon_dl.start()

    def _dragon_step_1_5_checkout(self, success):
        if not success:
            self.log("❌ 雲端下載失敗，終止排程。")
            self.on_pipeline_finished()
            return
        # 下載成功，呼叫原本的解壓縮步驟
        self._dragon_step_2_unzip()

    def _dragon_step_2_unzip(self):
        zip_path = self.project_root / "data" / "daily_data.zip"
        if not zip_path.exists():
            self.log("❌ 找不到 ZIP 檔案，請確認網路連線。")
            self.on_pipeline_finished()
            return

        self.log("🔓 [2/6] 解壓縮並套用 K 線...")
        self.progress.setRange(0, 100)
        self.progress.setFormat("🔓 解壓縮 K 線中 - %p%")

        self.dragon_unzip = UnzipWorker(zip_path, self.project_root / "data")
        self.dragon_unzip.progress_signal.connect(self.progress.setValue)
        self.dragon_unzip.finished_signal.connect(self._dragon_step_3_chips)
        self.dragon_unzip.start()

    def _dragon_step_3_chips(self, success):
        if not success:
            self.log("❌ K 線解壓縮失敗，終止排程。")
            self.on_pipeline_finished()
            return

        self.log("💰 [3/6] 正在抓取全市場雙籌碼 (法人+資券)...")
        self.progress.setFormat("⏳ 抓取深度籌碼中 - %p%")
        self.dragon_chips = ScriptRunner(self.project_root / "scripts" / "update_daily_chips_v2.py",
                                         ["--mode", "all"])
        self.dragon_chips.output_signal.connect(self.log)
        self.dragon_chips.progress_signal.connect(self.progress.setValue)
        self.dragon_chips.finished.connect(self._dragon_step_4_yield)
        self.dragon_chips.start_script()

    def _dragon_step_4_yield(self, exitCode):
        if exitCode != 0: return self.on_pipeline_finished()
        self.log("⚖️ [4/6] 正在抓取全市場估值...")
        self.progress.setFormat("⏳ 抓取市場估值中 - %p%")
        self.dragon_yield = ScriptRunner(self.project_root / "scripts" / "update_market_yield.py")
        self.dragon_yield.output_signal.connect(self.log)
        self.dragon_yield.progress_signal.connect(self.progress.setValue)
        self.dragon_yield.finished.connect(self._dragon_step_5_calc)
        self.dragon_yield.start_script()

    def _dragon_step_5_calc(self, exitCode):
        if exitCode != 0: return self.on_pipeline_finished()
        self.log("🧠 [5/6] 正在執行完整因子運算 (Full Calc)...")
        self.progress.setRange(0, 0)
        self.progress.setFormat("⚙️ 計算技術指標與策略大表中...")
        # ⚠️ 注意：這裡絕對不能加 --fast，因為要吃剛剛解壓縮的新 K 線
        self.dragon_calc = ScriptRunner(self.project_root / "scripts" / "calc_snapshot_factors.py")
        self.dragon_calc.output_signal.connect(self.log)
        self.dragon_calc.progress_signal.connect(self.progress.setValue)
        self.dragon_calc.finished.connect(self._dragon_step_6_industry)
        self.dragon_calc.start_script()

    def _dragon_step_6_industry(self, exitCode):
        if exitCode != 0: return self.on_pipeline_finished()
        self.log("🏭 [6/6] 正在合成市值加權產業板塊...")
        self.progress.setRange(0, 100)
        self.progress.setFormat("⏳ 產業板塊合成中 - %p%")
        self.dragon_ind = ScriptRunner(self.project_root / "scripts" / "build_industry_kline.py")
        self.dragon_ind.output_signal.connect(self.log)
        self.dragon_ind.progress_signal.connect(self.progress.setValue)

        # 完成後呼叫原本的 finish 恢復按鈕狀態
        self.dragon_ind.finished.connect(self._on_dragon_complete)
        self.dragon_ind.start_script()

    def _on_dragon_complete(self):
        self.btn_night_dragon.setEnabled(True)
        self.on_pipeline_finished()

    def _disable_all_buttons(self):
        self.btn_check_cloud.setEnabled(False)
        self.btn_download_zip.setEnabled(False)
        self.btn_fallback_kline.setEnabled(False)
        self.btn_pipe_core.setEnabled(False)
        self.btn_pipe_finance.setEnabled(False)
        self.btn_save_recalc.setEnabled(False)
        self.btn_pipe_concepts.setEnabled(False)
        self.btn_pipe_mdj_ind.setEnabled(False)
        self.btn_night_dragon.setEnabled(False)
        self.progress.setRange(0, 100)
        self.progress.setValue(0)

    def on_pipeline_finished(self):
        self.log("✅ 執行完畢！")
        if hasattr(self, 'task_start_time'): self._log_duration(self.task_start_time, "總執行耗時")

        # 強制顯示綠色滿格的 100%
        self.progress.setRange(0, 100)
        self.progress.setValue(100)
        self.progress.setFormat("✅ 作業結束")

        self.btn_check_cloud.setEnabled(True)
        self.btn_fallback_kline.setEnabled(True)
        self.btn_pipe_core.setEnabled(True)
        self.btn_pipe_finance.setEnabled(True)
        self.btn_save_recalc.setEnabled(True)
        self.btn_pipe_concepts.setEnabled(True)
        self.btn_pipe_mdj_ind.setEnabled(True)

        self.check_strategy_time()
        self.run_status_probe()

        # 👇 新增這行：向外廣播「資料已經全數更新完畢」的信號！
        self.data_updated.emit()

    # ==========================================
    # 雲端與參數編輯
    # ==========================================
    def check_cloud_status(self):
        self.log("📡 透過網頁 API 檢查雲端中...", True)
        self.btn_check_cloud.setEnabled(False)
        self.progress.setRange(0, 0)
        self.progress.setFormat("📡 讀取雲端狀態...")

        self.status_worker = FetchStatusWorker()
        self.status_worker.result_signal.connect(self.parse_remote_status)
        self.status_worker.start()

    def parse_remote_status(self, data):
        self.progress.setRange(0, 100)
        self.progress.setValue(100)
        self.btn_check_cloud.setEnabled(True)

        if "error" in data:
            self.log(f"⚠️ 無法取得雲端時間: {data['error']}")
            self.lbl_cloud_time.setText("雲端狀態: 讀取失敗")
            self.btn_download_zip.setEnabled(False)
            return

        remote_time = data.get('update_time', 'Unknown')
        self.lbl_cloud_time.setText(f"雲端狀態: {remote_time}")

        zip_path = self.project_root / "data" / "daily_data.zip"
        has_zip = zip_path.exists()
        self.btn_download_zip.setEnabled(True)
        self.btn_download_zip.setText("📦 發現 ZIP，立即套用" if has_zip else f"☁️ 下載 ({remote_time})")

    def download_cloud_data(self):
        zip_path = self.project_root / "data" / "daily_data.zip"
        self.btn_download_zip.setEnabled(False)

        if zip_path.exists():
            self.log("📦 本機已有 ZIP 檔，直接開始解壓縮...", True)
            self.unzip_data()
            return

        self.log("📡 啟動雲端直接下載...", True)
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setFormat("📡 下載雲端 ZIP 檔中 - %p%")

        self.dl_worker = DownloadZipWorker(zip_path)
        self.dl_worker.log_signal.connect(self.log)
        self.dl_worker.progress_signal.connect(self.progress.setValue)
        self.dl_worker.finished_signal.connect(self._on_download_finished)
        self.dl_worker.start()

    def _on_download_finished(self, success):
        if success:
            self.unzip_data()
        else:
            self.btn_download_zip.setEnabled(True)
            self.btn_download_zip.setText("🔄 重新檢查雲端")
            self.progress.setFormat("❌ 下載失敗")


    def unzip_data(self):
        zip_path = self.project_root / "data" / "daily_data.zip"
        extract_target = self.project_root / "data"

        if not zip_path.exists():
            self.log("❌ 找不到 ZIP 檔案。")
            self.btn_download_zip.setEnabled(True)
            self.btn_download_zip.setText("🔄 重新檢查雲端")
            self.progress.setRange(0, 100)
            self.progress.setValue(0)
            self.progress.setFormat("❌ 下載失敗")
            return

        self._disable_all_buttons()
        # 🔥 恢復成有具體數字的進度條
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setFormat("🔓 解壓縮與智慧合併中 - %p%")

        self.unzip_thread = UnzipWorker(zip_path, extract_target)
        self.unzip_thread.log_signal.connect(self.log)
        self.unzip_thread.progress_signal.connect(self.progress.setValue)
        self.unzip_thread.finished_signal.connect(self._on_unzip_finished)
        self.unzip_thread.start()

    def _on_unzip_finished(self, success):
        self.btn_check_cloud.setEnabled(True)
        self.btn_fallback_kline.setEnabled(True)
        self.btn_pipe_core.setEnabled(True)
        self.btn_pipe_finance.setEnabled(True)
        self.btn_save_recalc.setEnabled(True)
        self.btn_pipe_concepts.setEnabled(True)
        self.btn_pipe_mdj_ind.setEnabled(True)

        self.btn_download_zip.setEnabled(True)
        self.btn_download_zip.setText("🔄 重新檢查雲端")

        if success:
            self.log("✅ 雲端 K 線套用完成！正在結合本機基本面自動重算大表...")
            self.progress.setFormat("✅ K線就緒，自動結合本機營收重算中...")
            self.run_status_probe()
            # 🔥 自動結合最新K線與本機基本面重算大表
            self.handle_action_click()
        else:
            self.progress.setFormat("❌ 解壓縮發生錯誤")
            self.run_status_probe()

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

        self.progress.setRange(0, 0)
        self.progress.setFormat("⚙️ 龐大數據運算中 (約需 15~30 秒)，請耐心等候...")

        self.runner_recalc_only = ScriptRunner(self.project_root / "scripts" / "calc_snapshot_factors.py")
        self.runner_recalc_only.output_signal.connect(self.log)
        self.runner_recalc_only.progress_signal.connect(self.progress.setValue)

        # 🔥 關鍵修正：將原本的 self.on_pipeline_finished 改為 self._proceed_to_build_industry
        # 讓系統算完策略因子後，自動接續去合成最新的板塊 K 線！
        self.runner_recalc_only.finished.connect(self._proceed_to_build_industry)

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

