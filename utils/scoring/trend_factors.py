# 檔案路徑: utils/scoring/trend_factors.py
import pandas as pd
import numpy as np


class TrendScorer:
    @staticmethod
    def _get_time_col(df):
        """動態尋找資料集中的時間欄位，確保排序基準正確"""
        possible_cols = ['date', 'Date', 'quarter', 'month', 'period', 'timestamp', '年月', '日期', 'Time', 'time']
        for col in possible_cols:
            if col in df.columns:
                return col
        return None

    @staticmethod
    def calc_revenue_momentum(revenue_data: list) -> dict:
        """計算營收動能：近3個月 YoY 加速度斜率"""
        df = pd.DataFrame(revenue_data)
        if df.empty or 'rev_yoy' not in df.columns:
            return {"score": 0, "status": "無資料", "slope": 0.0}

        time_col = TrendScorer._get_time_col(df)
        if time_col:
            df = df.sort_values(time_col)

        df = df.tail(6).copy()
        df['yoy_accel'] = df['rev_yoy'].diff()
        slope_3m = df['yoy_accel'].tail(3).mean()

        if pd.isna(slope_3m):
            return {"score": 0, "status": "資料不足", "slope": 0.0}

        if slope_3m > 5:
            return {"score": 25, "status": "🔥 營收爆發", "slope": round(slope_3m, 2)}
        elif slope_3m > 0:
            return {"score": 15, "status": "📈 營收加速", "slope": round(slope_3m, 2)}
        else:
            return {"score": 0, "status": "📉 動能趨緩", "slope": round(slope_3m, 2)}

    @staticmethod
    def calc_profit_purity(profit_data: list) -> dict:
        """計算本業獲利純度：營業利益率 (OP Margin) 是否連續成長"""
        df = pd.DataFrame(profit_data)
        if df.empty or 'op_margin' not in df.columns:
            return {"score": 0, "status": "無資料", "qoq": 0.0}

        time_col = TrendScorer._get_time_col(df)
        if time_col:
            df = df.sort_values(time_col)

        df = df.tail(4).copy()
        df['op_margin_qoq'] = df['op_margin'].diff()
        latest_qoq = df['op_margin_qoq'].iloc[-1]

        if pd.isna(latest_qoq):
            return {"score": 0, "status": "資料不足", "qoq": 0.0}

        if latest_qoq > 0:
            return {"score": 15, "status": "✅ 本業轉強", "qoq": round(latest_qoq, 2)}
        else:
            return {"score": 0, "status": "⚠️ 本業衰退", "qoq": round(latest_qoq, 2)}

    @staticmethod
    def calc_margin_chip_trend(margin_data: list) -> dict:
        """計算籌碼動態：近 5 日融資淨變化"""
        df = pd.DataFrame(margin_data)
        if df.empty or 'fin_balance' not in df.columns:
            return {"trend_5d": 0.0}

        time_col = TrendScorer._get_time_col(df)
        if time_col:
            df = df.sort_values(time_col)

        df = df.tail(10).copy()
        df['fin_diff'] = df['fin_balance'].diff()
        trend_5d = df['fin_diff'].tail(5).sum()

        return {"trend_5d": float(trend_5d)}

    @staticmethod
    def calc_institutional_vwap(inst_data: list, kline_df: pd.DataFrame) -> dict:
        """
        計算近 20 日法人建倉成本線 (VWAP) 與當前乖離率
        """
        inst_df = pd.DataFrame(inst_data)
        if inst_df.empty:
            return {"vwap": 0.0, "bias_pct": 0.0, "status": "無法人資料"}

        # 1. 對齊 JSON 的時間 (強制 .dt.normalize() 抹除時分秒)
        time_col_inst = TrendScorer._get_time_col(inst_df)
        if not time_col_inst:
            return {"vwap": 0.0, "bias_pct": 0.0, "status": "無法人時間欄位"}
        inst_df['date'] = pd.to_datetime(inst_df[time_col_inst], errors='coerce').dt.normalize()

        # 2. 動態尋找外資與投信的買賣超欄位
        net_buy_series = pd.Series(0.0, index=inst_df.index)
        found_inst_cols = False
        for col in inst_df.columns:
            col_lower = col.lower()
            if ('foreign' in col_lower or 'trust' in col_lower) and (
                    'buy_sell' in col_lower or 'diff' in col_lower or 'change' in col_lower):
                net_buy_series += pd.to_numeric(inst_df[col], errors='coerce').fillna(0)
                found_inst_cols = True

        if not found_inst_cols:
            return {"vwap": 0.0, "bias_pct": 0.0, "status": "找不到法人買賣超欄位"}

        inst_df['net_buy'] = net_buy_series

        # 3. 對齊 K 線的時間
        kline_df = kline_df.copy()
        time_col_k = TrendScorer._get_time_col(kline_df)
        if not time_col_k:
            if 'Date' in kline_df.columns:
                time_col_k = 'Date'
            else:
                return {"vwap": 0.0, "bias_pct": 0.0, "status": "無K線時間欄位"}

        if pd.api.types.is_numeric_dtype(kline_df[time_col_k]) and kline_df[time_col_k].max() > 10000000000:
            kline_df['date'] = pd.to_datetime(kline_df[time_col_k], unit='ms').dt.normalize()
        else:
            kline_df['date'] = pd.to_datetime(kline_df[time_col_k], errors='coerce').dt.normalize()

        # 4. 合併資料並取近 20 日
        merged = pd.merge(kline_df, inst_df, on='date', how='inner')
        if merged.empty:
            return {"vwap": 0.0, "bias_pct": 0.0, "status": "價量資料無法對齊"}

        merged = merged.sort_values('date').tail(20)

        # 🔥 提前抓取最新收盤價
        latest_close = merged['close'].iloc[-1]

        # 🕵️ 加入印出機制，讓數據自己說話
        print("\n🕵️ [內部除錯] 實際對齊的 K 線與法人買賣超資料 (近 20 日)：")
        print(merged[['date', 'close', 'net_buy']].to_string(index=False))
        print(f"👉 20日總淨買超加總: {merged['net_buy'].sum()} 張\n")

        # 5. 計算 VWAP (只取有買的那些天來算加權成本)
        buy_days = merged[merged['net_buy'] > 0]
        latest_close = merged['close'].iloc[-1]

        # 如果這 20 天連一天都沒買，才回傳 0
        if buy_days.empty:
            return {
                "vwap": 0.0,
                "bias_pct": 0.0,
                "status": "📉 20日零買盤(無支撐)",
                "latest_close": round(latest_close, 2)
            }

        # 算出這 20 天內「有買進的日子」的加權平均成本
        vwap = (buy_days['net_buy'] * buy_days['close']).sum() / buy_days['net_buy'].sum()
        bias_pct = ((latest_close - vwap) / vwap) * 100

        # 再依據 20 日「總淨買賣超」來決定最終燈號狀態
        total_net = merged['net_buy'].sum()

        if total_net < 0:
            status = "⚠️ 總體淨賣超 (均價轉壓力)"
        elif bias_pct > 15:
            status = "⚠️ 乖離過熱 (結帳風險)"
        elif bias_pct < 0:
            status = "🚨 跌破成本 (停損警示)"
        else:
            status = "✅ 安全建倉區"

        return {
            "vwap": round(vwap, 2),
            "bias_pct": round(bias_pct, 2),
            "status": status,
            "latest_close": round(latest_close, 2)
        }