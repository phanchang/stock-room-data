🎛️ 按鈕與功能清單
卡片一：取得 K 線 (第一步)
按鈕：🔄 檢查雲端 ZIP (btn_check_cloud)
呼叫函式：check_cloud_status
主要功能：透過 Git 指令去遠端 (Origin/Main) 檢查 data_status.json，獲取最新的雲端資料打包時間，並更新介面顯示。
按鈕：☁️ [首選] 下載並套用雲端資料 (btn_download_zip)
呼叫函式：download_cloud_data ➡️ unzip_data
主要功能：透過 Git checkout 下載雲端已打包好的 daily_data.zip。解壓縮時會執行智慧合併（保留本機的財報/營收資料不被覆蓋），完成後自動觸發策略因子重算。
按鈕：💻 [備援] 抓取全市場 K 線 (btn_fallback_kline)
呼叫函式：run_fallback_kline
執行腳本：scripts/init_cache_tw.py
主要功能：當雲端異常時的備用方案，由本機端親自連線去抓取全市場的最新的 K 線資料。
卡片三：本機盤後策略運算排程 (第二步)
按鈕：🚀 [核心排程] 深度籌碼與策略運算 (btn_pipe_core)
呼叫函式：run_pre_flight_check ➡️ run_core_pipeline ➡️ _proceed_to_market_yield ➡️ _proceed_to_calc_factors
執行腳本：依序執行 update_daily_chips_fast.py ➡️ update_market_yield.py ➡️ calc_snapshot_factors.py
主要功能：每日收盤後的核心排程。會先檢查融資券是否已公布，接著依據下拉選單抓取籌碼（三大法人/資券），再更新全市場估值，最後統整產出策略因子大表。
按鈕：🏦 執行基本面更新 (btn_pipe_finance)
呼叫函式：run_pipeline_financials
執行腳本：scripts/update_financials.py (完成後接續 calc_snapshot_factors.py)
主要功能：依據下拉選單抓取營收或財報，抓完後自動觸發策略因子重算。
按鈕：🏷️ 更新題材概念股 (btn_pipe_concepts)
呼叫函式：run_pipeline_concepts
執行腳本：scripts/update_concepts.py (完成後接續 calc_snapshot_factors.py)
主要功能：爬取台股概念股族群分類，抓完後自動觸發策略因子重算。
按鈕：🏭 更新 MDJ 細產業 (btn_pipe_mdj_ind)
呼叫函式：run_pipeline_mdj_ind
執行腳本：scripts/update_industries.py (完成後接續 calc_snapshot_factors.py)
主要功能：爬取 MoneyDJ 的兩層式細產業分類，抓完後自動觸發策略因子重算。
卡片四：系統參數設定
按鈕：▶ 30W 策略參數設定 (點擊展開) (btn_toggle_30w)
呼叫函式：toggle_30w_params
主要功能：純 UI 互動，用來展開或收合策略參數的設定面板。
按鈕：🔧 編輯模式 / 🔒 取消編輯 (btn_edit)
呼叫函式：toggle_edit_mode
主要功能：解鎖參數輸入框以供修改。若按下「取消」，會把數值復原回原本讀取到的狀態。
按鈕：⚡ 僅重算策略因子 / 💾 儲存並重算策略因子 (btn_save_recalc)
呼叫函式：handle_action_click ➡️ save_config (若有修改)
執行腳本：scripts/calc_snapshot_factors.py
主要功能：如果參數有修改，會先將設定存入 strategy_config.json。接著在不抓取新資料的情況下，直接用現有本機資料搭配新參數，重新運算一次策略大表。