from datetime import datetime, timedelta
import time
import os
from utils.etf.parse import parse_specific as parse_data

class ETFScheduler:
    def __init__(self, scrapers_config, base_dir, proxy, retry_interval=30, max_retry_until="23:59",
                 startup_check_days=7):
        self.scrapers_config = scrapers_config
        self.base_dir = base_dir
        self.proxy = proxy
        self.retry_interval = retry_interval
        self.max_retry_until = max_retry_until
        self.startup_check_days = startup_check_days  # 新增：啟動時檢查天數
        self.last_run_date = None
        self.schedule_status = {}

    def is_time_reached(self, target_time_str):
        """檢查是否到達指定時間"""
        now = datetime.now()
        target_time = datetime.strptime(target_time_str, '%H:%M').time()
        return now.time() >= target_time

    def is_past_max_time(self):
        """檢查是否超過最晚時間"""
        now = datetime.now()
        max_time = datetime.strptime(self.max_retry_until, '%H:%M').time()
        return now.time() > max_time

    def get_last_data_date(self, save_dir):
        """
        從目錄中的檔案取得最後一筆資料日期
        檔案名稱格式: YYYYMMDD.xlsx 或 YYYYMMDD.xls
        """
        if not os.path.exists(save_dir):
            return None

        #files = [f for f in os.listdir(save_dir) if f.endswith(('.xlsx', '.xls'))]
        files = [f for f in os.listdir(save_dir) if f.endswith(('.xlsx', '.xls', '.json'))]
        if not files:
            return None

        # 提取日期並排序
        dates = []
        for filename in files:
            try:
                # 假設檔案名稱格式為 YYYYMMDD.xlsx
                date_str = filename.split('.')[0]
                if len(date_str) == 8 and date_str.isdigit():
                    date_obj = datetime.strptime(date_str, '%Y%m%d')
                    dates.append(date_obj)
            except:
                continue

        return max(dates) if dates else None

    def get_missing_dates(self, save_dir, lookback_days=30):
        """
        取得需要補齊的日期列表（排除週末）

        Args:
            save_dir: 資料儲存目錄
            lookback_days: 最多往前追溯幾天

        Returns:
            list: 需要補齊的日期列表
        """
        last_date = self.get_last_data_date(save_dir)
        today = datetime.now().date()

        # 如果沒有任何資料，從往前 lookback_days 天開始
        if last_date is None:
            start_date = datetime.now() - timedelta(days=lookback_days)
        else:
            start_date = last_date + timedelta(days=1)

        missing_dates = []
        current_date = start_date

        while current_date.date() <= today:
            # 只加入工作日（週一到週五）
            if current_date.weekday() < 5:
                missing_dates.append(current_date)
            current_date += timedelta(days=1)

        return missing_dates

    def startup_backfill(self):
        """
        啟動時智能檢查並補齊缺失的資料
        只下載真正缺失的日期，不是無腦下載過去N天
        """
        print("\n" + "=" * 60)
        print("🔍 啟動補齊檢查")
        print("=" * 60)
        print(f"檢查範圍: 過去 {self.startup_check_days} 天內的工作日\n")

        total_missing = 0
        backfill_tasks = []

        # 先掃描所有投信的缺失情況
        for company, config in self.scrapers_config.items():
            for fund in config['funds']:
                save_dir = os.path.join(self.base_dir, fund['dir'])

                # 智能檢查：只找出真正缺失的日期
                missing_dates = self.get_missing_dates(save_dir, lookback_days=self.startup_check_days)

                if missing_dates:
                    total_missing += len(missing_dates)
                    backfill_tasks.append((company, config, fund, missing_dates))
                    print(f"📂 {company} - {fund['name']} ({fund['code']})")
                    print(f"   ⚠️  缺失 {len(missing_dates)} 個工作日的檔案:")

                    # 顯示缺失的日期
                    display_dates = missing_dates[-5:] if len(missing_dates) > 5 else missing_dates
                    for date in display_dates:
                        print(f"      - {date.strftime('%Y-%m-%d (%A)')}")
                    if len(missing_dates) > 5:
                        print(f"      ... 還有 {len(missing_dates) - 5} 個日期")
                    print()

        if total_missing == 0:
            print("✅ 所有資料完整，無需補齊\n")
            return

        # 執行智能補齊
        print(f"🚀 開始補齊 {total_missing} 個缺失檔案...")
        print(f"   涉及 {len(backfill_tasks)} 個基金\n")

        # 記錄哪些投信有成功下載新檔案
        companies_with_new_data = set()

        for company, config, fund, missing_dates in backfill_tasks:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 📥 補齊 {company} - {fund['name']}...")
            print(f"   目標日期: {missing_dates[0].strftime('%Y-%m-%d')} ~ {missing_dates[-1].strftime('%Y-%m-%d')}")

            save_dir = os.path.join(self.base_dir, fund['dir'])
            scraper_class = config['class']

            # 記錄補齊前的檔案數
            #files_before = len([f for f in os.listdir(save_dir)
            #                    if f.endswith(('.xlsx', '.xls'))]) if os.path.exists(save_dir) else 0
            files_before = len(
                [f for f in os.listdir(save_dir) if f.endswith(('.xlsx', '.xls', '.json'))]) if os.path.exists(
                save_dir) else 0

            scraper = scraper_class(
                fund_code=fund['code'],
                save_dir=save_dir,
                proxy=self.proxy
            )

            # 執行下載（不管返回值，我們自己判斷是否成功）
            scraper.fetch_and_save()

            # 檢查實際下載了多少檔案
            files_after = len([f for f in os.listdir(save_dir)
                               if f.endswith(('.xlsx', '.xls'))]) if os.path.exists(save_dir) else 0
            new_files = files_after - files_before

            # 只要有新檔案就算成功
            if new_files > 0:
                print(f"   ✅ 成功下載 {new_files} 個檔案")
                companies_with_new_data.add(company)
            else:
                print(f"   ℹ️  無新檔案（可能網站尚未更新）")
            print()

        # 在所有下載完成後，對有新資料的投信執行解析
        if companies_with_new_data:
            print("\n" + "=" * 60)
            print("🔄 開始執行資料解析")
            print("=" * 60)
            for company in companies_with_new_data:
                self._run_parser(company)
            print()

        print("=" * 60)
        print("✅ 補齊作業完成")
        print("=" * 60 + "\n")

    def execute_scraper(self, company, config, target_dates=None):
        """
        執行指定投信的爬蟲

        Args:
            company: 投信名稱
            config: 投信設定
            target_dates: 指定要下載的日期列表（可選）
        """
        scraper_class = config['class']
        all_success = True
        downloaded_new_data = False

        for fund in config['funds']:
            print(f"\n處理 {company} - {fund['name']} ({fund['code']})...")
            save_dir = os.path.join(self.base_dir, fund['dir'])

            # 如果沒有指定日期，檢查缺失日期
            if target_dates is None:
                missing_dates = self.get_missing_dates(save_dir)
                if missing_dates:
                    print(f"發現 {len(missing_dates)} 個缺失日期需要補齊:")
                    for date in missing_dates[-5:]:  # 只顯示最近5個
                        print(f"  - {date.strftime('%Y-%m-%d')}")
                    if len(missing_dates) > 5:
                        print(f"  ... 還有 {len(missing_dates) - 5} 個日期")
                else:
                    print(f"無缺失日期")
            else:
                missing_dates = target_dates

            # 記錄執行前的檔案數量
            files_before = len([f for f in os.listdir(save_dir)
                                if f.endswith(('.xlsx', '.xls'))]) if os.path.exists(save_dir) else 0

            # 執行下載（爬蟲會自動處理多日期）
            scraper = scraper_class(
                fund_code=fund['code'],
                save_dir=save_dir,
                proxy=self.proxy
            )

            success = scraper.fetch_and_save()

            # 檢查是否有新增檔案
            files_after = len([f for f in os.listdir(save_dir)
                               if f.endswith(('.xlsx', '.xls'))]) if os.path.exists(save_dir) else 0

            if success and files_after > files_before:
                downloaded_new_data = True
                print(f"✅ 成功下載 {files_after - files_before} 個檔案")

            key = f"{company}_{fund['code']}"
            self.schedule_status[key] = success

            if not success:
                all_success = False

        # 只有下載到新資料才執行解析
        if all_success and downloaded_new_data:
            self._run_parser(company)
        elif all_success and not downloaded_new_data:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {company} 今日資料已存在，跳過解析")
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {company} 下載失敗，稍後重試")

    def _run_parser(self, company):
        """執行解析器"""
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {company} 下載完成，開始解析...")

        try:
            import sys
            from pathlib import Path

            project_root = Path(__file__).resolve().parents[2]
            if str(project_root) not in sys.path:
                sys.path.insert(0, str(project_root))

            from parse import parse_specific

            parse_specific(company)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {company} 解析完成 ✅")

        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {company} 解析失敗: {e}")

    def check_all_success(self):
        """檢查所有投信是否都成功"""
        return all(self.schedule_status.values())

    def run(self):
        """運行排程器"""
        print("=" * 60)
        print("統一 ETF 爬蟲系統 - 排程器")
        print("=" * 60)
        print(f"投信數量: {len(self.scrapers_config)}")
        for company, config in self.scrapers_config.items():
            print(f"  - {company}: {len(config['funds'])} 檔基金")
            print(f"    排程時間: {config['schedule_time']}")
        print(f"重試間隔: {self.retry_interval} 分鐘")
        print(f"最晚重試: {self.max_retry_until}")
        print("=" * 60)

        # 🆕 啟動時先執行補齊檢查
        self.startup_backfill()

        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 排程器已啟動...")
        print("按 Ctrl+C 可停止程式\n")

        try:
            while True:
                now = datetime.now()
                today = now.date()

                if self.last_run_date != today:
                    self.last_run_date = today
                    self.schedule_status = {}
                    print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] 新的一天開始")

                for company, config in self.scrapers_config.items():
                    schedule_time = config['schedule_time']

                    executed_keys = [k for k in self.schedule_status.keys() if k.startswith(company)]
                    all_executed = len(executed_keys) == len(config['funds'])

                    if self.is_time_reached(schedule_time) and not all_executed:
                        print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] {company} 到達排程時間")
                        self.execute_scraper(company, config)

                if self.schedule_status and not self.check_all_success():
                    if not self.is_past_max_time():
                        print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] 有任務失敗，{self.retry_interval} 分鐘後重試...")
                        time.sleep(self.retry_interval * 60)
                        continue
                    else:
                        print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] 已超過最晚重試時間，今日停止")

                time.sleep(60)

        except KeyboardInterrupt:
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 排程器已停止")