# ==================================================
# StockWarRoom.py
# 股票戰情室 - 技術面、三大法人、營收 + 左側股票清單 + Loading
# ==================================================

import os
import pandas as pd
import yfinance as yf
import plotly.graph_objs as go
import dash
from dash import Dash, html, dcc, Input, Output, State, dash_table
from datetime import datetime
from utils.crawler_fa import get_fa_ren
from utils.crawler_revenue import get_monthly_revenue
from utils.crawler_profitability import get_profitability
from utils.crawler_margin_trading import get_margin_trading
from utils.etf.fhtrust_data import load_history, compute_diff
# 1️⃣ 在檔案開頭的 import 區塊新增：
from utils.etf import fhtrust_data, ezmoney_data
from config.quick_filter_config import FILTER_CONDITIONS, get_latest_file
# ==================================================
# 1. Proxy 設定
# ==================================================
PROXY = "http://10.160.3.88:8080"
os.environ["HTTP_PROXY"] = PROXY
os.environ["HTTPS_PROXY"] = PROXY

# ==================================================
# 2. 載入股票清單
# ==================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STOCKLIST_DIR = os.path.join(BASE_DIR, "StockList")
TWSE_FILE = os.path.join(STOCKLIST_DIR, "TWSE_ESVUFR.csv")
TWO_FILE = os.path.join(STOCKLIST_DIR, "TWO_ESVUFR.csv")

df_twse = pd.read_csv(TWSE_FILE, dtype=str)
df_two = pd.read_csv(TWO_FILE, dtype=str)


def extract_code_from_name_col(df):
    col = "股票代號及名稱"
    return set(df[col].astype(str).str.extract(r"^(\d{4,5})")[0].dropna())


twse_codes = extract_code_from_name_col(df_twse)
two_codes = extract_code_from_name_col(df_two)


def get_yahoo_symbol(stock_code: str) -> str:
    if stock_code in twse_codes: return f"{stock_code}.TW"
    if stock_code in two_codes: return f"{stock_code}.TWO"
    raise ValueError(f"找不到股票代號：{stock_code}")


def get_stock_name(stock_code):
    for df in (df_twse, df_two):
        m = df[df["股票代號及名稱"].str.startswith(stock_code)]
        if not m.empty: return m.iloc[0]["股票代號及名稱"].split()[1]
    return stock_code


# ==================================================
# 🔄 通用函數：建立股票分析頁籤內容
# ==================================================
def build_stock_tabs_content(stock_code: str, selected_tab: str, period: str = "D", prefix: str = ""):
    """通用函數：根據選擇的頁籤返回對應內容"""

    # ========== 取得所有資料 ==========
    data = {}

    # K線
    try:
        data['k'] = get_kline_data(stock_code, period)
    except Exception as e:
        data['k'] = None
        data['k_error'] = str(e)

    # 三大法人
    try:
        data['fa'] = get_fa_ren(stock_code)
    except Exception as e:
        data['fa'] = None
        data['fa_error'] = str(e)

    # 營收
    try:
        df_rev_raw = get_monthly_revenue(stock_code)
        data['rev'] = df_rev_raw.copy()
        data['rev']["year"] = data['rev']["日期"].dt.year
        data['rev']["month"] = data['rev']["日期"].dt.month
        data['rev'].rename(columns={"營收": "revenue"}, inplace=True)
        data['rev'] = data['rev'].sort_values(["year", "month"], ascending=[False, False])
    except Exception as e:
        data['rev'] = None
        data['rev_error'] = str(e)

    # 資券
    try:
        data['margin'] = get_margin_trading(stock_code)
    except Exception as e:
        data['margin'] = None
        data['margin_error'] = str(e)

    # ========== 技術面 ==========
    if selected_tab == "tab-tech":
        if data['k'] is not None:
            # ⭐ 修正：加上 prefix 避免 ID 衝突
            return html.Div([
                html.Div([
                    html.Label("選擇週期:", style={"marginRight": "10px", "fontWeight": "bold"}),
                    dcc.RadioItems(
                        id=f"{prefix}period-radio",  # ⭐ 加上前綴
                        options=[
                            {"label": "日線", "value": "D"},
                            {"label": "週線", "value": "W"},
                            {"label": "月線", "value": "M"}
                        ],
                        value=period,
                        inline=True,
                        labelStyle={"marginRight": "15px"}
                    )
                ], style={"marginBottom": "15px", "padding": "10px",
                          "backgroundColor": "#f0f0f0", "borderRadius": "5px"}),

                dcc.Graph(
                    figure=build_chart(data['k'], stock_code, period),
                    style={"width": "100%", "height": "520px"}
                )
            ])
        else:
            return html.Div(f"K線抓取失敗: {data.get('k_error', '未知錯誤')}",
                            style={"color": "red", "padding": "20px"})

    # ========== 籌碼面 ==========
    elif selected_tab == "tab-chips":
        chips_sections = []

        # 1. 三大法人
        if data['fa'] is not None and data['k'] is not None:
            fa_content = html.Div([
                build_fa_summary_table(data['fa'], stock_code),
                dcc.Graph(
                    figure=build_fa_price_chart(data['fa'], stock_code),
                    style={"width": "100%", "height": "400px"}
                ),
                build_fa_detail_table(data['fa'])
            ])
        else:
            fa_content = html.Div(
                f"三大法人資料抓取失敗: {data.get('fa_error', '未知錯誤')}",
                style={"color": "red", "padding": "20px"}
            )

        chips_sections.append(
            create_accordion_section(
                section_id="chips-fa",
                title="📊 三大法人",
                content=fa_content,
                is_open=False,
                prefix=prefix  # ⭐ 傳入 prefix
            )
        )

        # 2. 資券
        if data['margin'] is not None:
            margin_content = build_margin_section(data['margin'], stock_code)
        else:
            margin_content = html.Div(
                f"資券資料抓取失敗: {data.get('margin_error', '未知錯誤')}",
                style={"color": "red", "padding": "20px"}
            )

        chips_sections.append(
            create_accordion_section(
                section_id="chips-margin",
                title="💰 資券",
                content=margin_content,
                is_open=False,
                prefix=prefix
            )
        )

        # 3. 主力進出
        chips_sections.append(
            create_accordion_section(
                section_id="chips-main",
                title="🎯 主力進出",
                content=html.Div("主力進出資料待實作",
                                 style={"padding": "20px", "textAlign": "center", "color": "gray"}),
                is_open=False,
                prefix=prefix
            )
        )

        # 4. 股權分布
        chips_sections.append(
            create_accordion_section(
                section_id="chips-ownership",
                title="📈 股權分布",
                content=html.Div("股權分布資料待實作",
                                 style={"padding": "20px", "textAlign": "center", "color": "gray"}),
                is_open=False,
                prefix=prefix
            )
        )

        return html.Div(chips_sections, style={"width": "100%"})

    # ========== 財務面 ==========
    elif selected_tab == "tab-revenue":
        financial_sections = []

        # 1. 月營收
        if data['rev'] is not None:
            revenue_content = build_revenue_section(data['rev'], stock_code)
        else:
            revenue_content = html.Div(
                f"營收資料抓取失敗: {data.get('rev_error', '未知錯誤')}",
                style={"color": "red", "padding": "20px"}
            )

        financial_sections.append(
            create_accordion_section(
                section_id="revenue",
                title="📊 月營收",
                content=revenue_content,
                is_open=False,
                prefix=prefix
            )
        )

        # 2. EPS
        financial_sections.append(
            create_accordion_section(
                section_id="eps",
                title="💰 EPS (每股盈餘)",
                content=html.Div([
                    html.Div([
                        html.Label("選擇視圖:", style={"marginRight": "10px", "fontWeight": "bold"}),
                        dcc.RadioItems(
                            id=f"{prefix}eps-view-radio",  # ⭐ 加上前綴
                            options=[
                                {"label": "單季", "value": "quarter"},
                                {"label": "累季", "value": "cumulative"},
                                {"label": "年度", "value": "yearly"}
                            ],
                            value="quarter",
                            inline=True,
                            labelStyle={"marginRight": "15px"}
                        )
                    ], style={"marginBottom": "15px", "padding": "10px",
                              "backgroundColor": "#f0f0f0", "borderRadius": "5px"}),

                    html.Div(
                        id=f"{prefix}eps-content-container",  # ⭐ 加上前綴
                        children=build_eps_section(stock_code, view_type="quarter", n_quarters=12)
                    )
                ]),
                is_open=False,
                prefix=prefix
            )
        )

        # 3. 毛利率
        financial_sections.append(
            create_accordion_section(
                section_id="gross-margin",
                title="📈 毛利率",
                content=build_gross_margin_section(stock_code),
                is_open=False,
                prefix=prefix
            )
        )

        # 4. 財報摘要
        financial_sections.append(
            create_accordion_section(
                section_id="financial-statement",
                title="📋 財報摘要",
                content=html.Div([
                    build_financial_statement_table(stock_code)
                ], style={"padding": "10px"}),
                is_open=False,
                prefix=prefix
            )
        )

        return html.Div(financial_sections, style={"width": "100%"})

    return html.Div("請選擇分頁", style={"padding": "20px"})

# ==================================================
# 🆕 快速選股相關函數
# ==================================================
import sys

sys.path.insert(0, 'config')


def load_filter_data(condition_name: str) -> pd.DataFrame:
    """
    載入指定條件的資料
    ✅ 加入除錯訊息
    """
    config = FILTER_CONDITIONS.get(condition_name)
    if not config:
        print(f"❌ 找不到條件設定: {condition_name}")
        return None

    filepath = get_latest_file(config["data_dir"], config["file_pattern"])
    if not filepath or not filepath.exists():
        print(f"❌ 找不到檔案: {condition_name}")
        return None

    try:
        df = pd.read_csv(filepath, encoding='utf-8-sig')

        # ✅ 顯示載入資訊
        print(f"📂 {condition_name}: 載入 {filepath.name} ({len(df)} 筆資料)")
        print(f"   欄位: {list(df.columns[:5])}...")  # 顯示前5個欄位

        # ✅ 檢查是否有股票代號欄位
        possible_code_columns = ['代號', '股票代號', 'code', 'stock_code', '證券代號']
        has_code_column = any(col in df.columns for col in possible_code_columns)

        if not has_code_column:
            print(f"⚠️ 警告：{condition_name} 沒有股票代號欄位！")
            print(f"   實際欄位: {list(df.columns)}")
            return None

        return df

    except Exception as e:
        print(f"❌ 載入 {condition_name} 失敗: {e}")
        return None

def merge_filter_results(selected_conditions: list) -> pd.DataFrame:
    """合併多個條件的結果 (AND 邏輯)"""
    if not selected_conditions:
        return pd.DataFrame()

    result_df = load_filter_data(selected_conditions[0])
    if result_df is None:
        return pd.DataFrame()

    # ✅ 統一欄位名稱：將所有可能的股票代號欄位改名為 '代號'
    def normalize_stock_code_column(df):
        """將各種股票代號欄位統一為 '代號'"""
        possible_columns = ['代號', '股票代號', 'code', 'stock_code', '證券代號']
        for col in possible_columns:
            if col in df.columns and col != '代號':
                df = df.rename(columns={col: '代號'})
                break

        # 確保 '代號' 是字串型態且去除空白
        if '代號' in df.columns:
            df['代號'] = df['代號'].astype(str).str.strip()

        return df

    result_df = normalize_stock_code_column(result_df)

    # ✅ 確保有 '代號' 欄位
    if '代號' not in result_df.columns:
        print(f"⚠️ 警告：{selected_conditions[0]} 沒有股票代號欄位")
        return pd.DataFrame()

    # 取得第一個條件的股票代號集合
    common_codes = set(result_df['代號'])

    # ✅ 依序與其他條件取交集
    for condition in selected_conditions[1:]:
        df = load_filter_data(condition)
        if df is None:
            print(f"⚠️ 警告：無法載入 {condition} 的資料")
            continue

        df = normalize_stock_code_column(df)

        if '代號' not in df.columns:
            print(f"⚠️ 警告：{condition} 沒有股票代號欄位")
            continue

        # 取交集
        condition_codes = set(df['代號'])
        common_codes = common_codes & condition_codes

        print(f"📊 {condition}: {len(condition_codes)} 檔 | 交集後剩餘: {len(common_codes)} 檔")

    # ✅ 根據交集結果篩選第一個資料集
    result_df = result_df[result_df['代號'].isin(common_codes)]

    # ✅ 只保留基本欄位
    display_cols = ['代號', '名稱', '成交', '漲跌幅']

    # 處理欄位名稱變體
    column_mapping = {
        '股票名稱': '名稱',
        'name': '名稱',
        '收盤價': '成交',
        'price': '成交',
        '漲跌 幅': '漲跌幅',
        '漲跌(%)': '漲跌幅',
        'change_pct': '漲跌幅'
    }

    result_df = result_df.rename(columns=column_mapping)

    # 只保留存在的欄位
    available_cols = [col for col in display_cols if col in result_df.columns]

    if not available_cols:
        return result_df  # 如果沒有標準欄位，返回原始資料

    return result_df[available_cols].reset_index(drop=True)

def build_quick_filter_layout():
    """
    建立快速選股介面

    上方: 條件選擇按鈕
    右上: 篩選結果表格
    右下: 重用戰情室的三個頁籤
    """
    # 條件選擇按鈕區
    condition_buttons = html.Div([
        html.Div([
            html.Button(
                config["label"],
                id={"type": "filter-btn", "index": name},
                n_clicks=0,
                style={
                    "margin": "5px",
                    "padding": "10px 20px",
                    "backgroundColor": "white",
                    "border": f"2px solid {config['color']}",
                    "borderRadius": "8px",
                    "cursor": "pointer",
                    "fontSize": "14px",
                    "transition": "all 0.3s"
                }
            )
            for name, config in FILTER_CONDITIONS.items()
        ], style={"display": "flex", "flexWrap": "wrap", "gap": "10px"})
    ], style={"marginBottom": "20px", "padding": "15px", "backgroundColor": "#f8f9fa", "borderRadius": "10px"})

    # 結果表格區 (初始為空)
    result_table = html.Div(
        id="filter-result-table",
        children=[
            html.Div(
                "請選擇條件",
                style={"padding": "30px", "textAlign": "center", "color": "#999"}
            )
        ],
        style={"marginBottom": "20px"}
    )

    # 重用戰情室的詳細資訊區
    detail_section = html.Div(
        id="filter-detail-section",
        children=[],
        style={"marginTop": "20px"}
    )

    return html.Div([
        html.H3("🎯 快速選股", style={"marginBottom": "15px"}),
        condition_buttons,
        result_table,
        detail_section
    ])

# ==================================================
# 3. K 線資料
# ==================================================
def get_kline_data(stock_code: str, period_type: str) -> pd.DataFrame:
    yahoo_symbol = get_yahoo_symbol(stock_code)
    interval_map = {"D": ("1d", "1y"), "W": ("1wk", "2y"), "M": ("1mo", "5y")}
    interval, period = interval_map[period_type]

    df = yf.download(
        yahoo_symbol,
        interval=interval,
        period=period,
        progress=False,
        auto_adjust=False
    )

    if df.empty: raise ValueError("Yahoo 無資料")
    if isinstance(df.columns, pd.MultiIndex): df.columns = [c[0] for c in df.columns]

    df = df.dropna().reset_index()
    df.rename(columns={"index": "Date"}, inplace=True)
    return df


# ==================================================
# 技術面 K 線圖表
# ==================================================
def build_chart(df, stock_code, period_type):
    """
    技術面 K 線圖表
    - 優化 hover 體驗：使用 unified mode
    - 均線不會遮蔽 K 線資訊
    """
    x = list(range(len(df)))
    stock_name = get_stock_name(stock_code)
    period_name = {"D": "日線", "W": "週線", "M": "月線"}[period_type]

    # 確保 Date 是 datetime 格式
    df['Date'] = pd.to_datetime(df['Date'])

    # 計算漲跌
    df['漲跌'] = df['Close'] - df['Close'].shift(1)
    df['漲跌'] = df['漲跌'].fillna(0)  # 第一筆沒有前一日，設為0

    vol_colors = [
        "red" if i > 0 and df.Close.iloc[i] > df.Close.iloc[i - 1] else
        "green" if i > 0 and df.Close.iloc[i] < df.Close.iloc[i - 1] else
        "lightgray" for i in range(len(df))
    ]

    fig = go.Figure()

    # K線 - 自訂完整 hover 資訊
    hover_text = []
    for i, row in df.iterrows():
        text = (f"日期: {row['Date'].strftime('%Y-%m-%d')}<br>"
                f"開: {row['Open']:.2f}<br>"
                f"高: {row['High']:.2f}<br>"
                f"低: {row['Low']:.2f}<br>"
                f"收: {row['Close']:.2f}<br>"
                f"漲跌: {row['漲跌']:+.2f}")
        hover_text.append(text)

    fig.add_trace(go.Candlestick(
        x=x,
        open=df.Open,
        high=df.High,
        low=df.Low,
        close=df.Close,
        increasing_line_color="red",
        decreasing_line_color="green",
        name="股價",
        yaxis="y1",
        text=hover_text,
        hoverinfo="text"
    ))

    max_vol = df.Volume.max()

    # 成交量 - 加入日期
    vol_hover_text = []
    for i, row in df.iterrows():
        text = (f"成交量: {row['Volume']:,.0f}")
        vol_hover_text.append(text)

    fig.add_trace(go.Bar(
        x=x,
        y=df.Volume,
        marker_color=vol_colors,
        opacity=0.35,
        name="成交量",
        yaxis="y2",
        text=vol_hover_text,
        hoverinfo="text"
    ))

    # 均線設定
    if period_type == "D":
        ma_dict = {"MA5": 5, "MA10": 10, "MA55": 55, "MA200": 200}
    elif period_type == "W":
        ma_dict = {"MA30": 30}
    else:
        ma_dict = {"MA12": 12}

    for ma, p in ma_dict.items():
        if len(df) >= p:
            ma_values = df.Close.rolling(p).mean()

            # 均線 hover - 加入日期
            ma_hover_text = []
            for i, row in df.iterrows():
                val = ma_values.iloc[i]
                if pd.notna(val):
                    text = (f"{ma}: {val:.2f}")
                else:
                    text = ""
                ma_hover_text.append(text)

            fig.add_trace(go.Scatter(
                x=x,
                y=ma_values,
                mode="lines",
                line=dict(width=1),
                name=ma,
                text=ma_hover_text,
                hoverinfo="text"
            ))

    # 設定 X 軸日期顯示
    if period_type == "D":
        tickvals = []
        ticktext = []
        current_month = None
        for i, date in enumerate(df['Date']):
            if date.month != current_month:
                tickvals.append(i)
                ticktext.append(date.strftime('%Y-%m'))
                current_month = date.month

    elif period_type == "W":
        tickvals = []
        ticktext = []
        last_quarter = None
        for i, date in enumerate(df['Date']):
            quarter = (date.month - 1) // 3
            year_quarter = (date.year, quarter)
            if year_quarter != last_quarter and quarter % 1 == 0:
                tickvals.append(i)
                ticktext.append(date.strftime('%Y-%m'))
                last_quarter = year_quarter

    else:
        tickvals = []
        ticktext = []
        current_year = None
        for i, date in enumerate(df['Date']):
            if date.year != current_year:
                tickvals.append(i)
                ticktext.append(date.strftime('%Y'))
                current_year = date.year

    # 圖表配置
    fig.update_layout(
        title=f"{stock_name} ({stock_code}) {period_name}",
        height=520,
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=50, r=50, t=50, b=40),
        legend=dict(orientation="h", y=1.02),
        yaxis=dict(title="股價", side="left", showgrid=True, zeroline=False),
        yaxis2=dict(
            title="成交量",
            overlaying="y",
            side="right",
            showgrid=False,
            rangemode="tozero",
            range=[0, max_vol * 1.5]
        ),
        xaxis=dict(
            tickmode='array',
            tickvals=tickvals,
            ticktext=ticktext,
            tickangle=-45,
            showgrid=True,
            gridcolor="rgba(200,200,200,0.3)"
        ),
        # ⭐ 關鍵優化：改善 hover 體驗
        hovermode="x unified",  # 統一顯示該 X 位置的所有資訊
        hoverdistance=100,  # 增加觸發距離
        spikedistance=1000,  # 增加輔助線觸發距離
        showlegend=True  # 顯示圖例
    )

    return fig


# ==================================================
# 財務面月營收分析區塊
# ==================================================
def build_revenue_section(df_rev, stock_code):
    """
    月營收分析區塊
    - 上方：近三年營收折線圖
    - 下方：月營收資料表
    - 🆕 紅綠邏輯只處理「整個資料集中真正最新的那一筆」
    - 🆕 營收新高/次高標註
    """

    # ✅ 確保資料由新到舊排序（最新資料在最上面）
    if "日期" in df_rev.columns:
        df_rev = df_rev.sort_values("日期", ascending=False).reset_index(drop=True)
    elif "year" in df_rev.columns and "month" in df_rev.columns:
        df_rev = df_rev.sort_values(["year", "month"], ascending=[False, False]).reset_index(drop=True)

    # ========== 🆕 找出整個資料集中真正最新的那一筆 ==========
    latest_idx = 0  # 排序後第一筆就是最新
    latest_year_month = df_rev.loc[latest_idx, "年月"] if "年月" in df_rev.columns else None

    # ---------- 圖表 ----------
    rev_chart = dcc.Graph(
        figure=build_revenue_chart(df_rev, stock_code),
        style={"width": "100%", "height": "420px"}
    )

    # ========== 🆕 判斷營收新高/次高 ==========
    try:
        # 取最近 60 個月的營收資料
        recent_60m = df_rev.head(60).copy()

        # 確保 revenue 欄位是數值型態
        recent_60m["revenue_numeric"] = pd.to_numeric(recent_60m["revenue"], errors='coerce')

        # 移除 NaN 值
        valid_revenues = recent_60m["revenue_numeric"].dropna()

        if len(valid_revenues) > 0:
            # 最新一期營收
            latest_revenue = pd.to_numeric(df_rev.loc[latest_idx, "revenue"], errors='coerce')

            if pd.notna(latest_revenue):
                # 排序找出最高和次高
                sorted_revenues = sorted(valid_revenues.tolist(), reverse=True)

                # 標註新高或次高（只標註在最新那一筆）
                if latest_revenue == sorted_revenues[0]:
                    df_rev.loc[latest_idx, "revenue"] = f"{latest_revenue:,.0f} (新高)"
                elif len(sorted_revenues) > 1 and latest_revenue == sorted_revenues[1]:
                    df_rev.loc[latest_idx, "revenue"] = f"{latest_revenue:,.0f} (次高)"
                else:
                    # 一般情況，格式化顯示
                    df_rev.loc[latest_idx, "revenue"] = f"{latest_revenue:,.0f}"
            else:
                df_rev.loc[latest_idx, "revenue"] = str(df_rev.loc[latest_idx, "revenue"])

        # 其他列的營收也格式化（避免顯示問題）
        for i in range(len(df_rev)):
            if i != latest_idx:
                rev_val = pd.to_numeric(df_rev.loc[i, "revenue"], errors='coerce')
                if pd.notna(rev_val):
                    df_rev.loc[i, "revenue"] = f"{rev_val:,.0f}"
                else:
                    df_rev.loc[i, "revenue"] = str(df_rev.loc[i, "revenue"])

    except Exception as e:
        print(f"標註營收新高/次高時發生錯誤: {e}")
        # 發生錯誤時仍格式化營收欄位
        for i in range(len(df_rev)):
            rev_val = pd.to_numeric(df_rev.loc[i, "revenue"], errors='coerce')
            if pd.notna(rev_val):
                df_rev.loc[i, "revenue"] = f"{rev_val:,.0f}"

    # ========== 🔧 表格紅綠樣式（使用 filter_query 精確匹配最新那一筆）==========
    style_conditional = []

    if latest_year_month:
        # 轉義特殊字符，避免 filter_query 語法錯誤
        safe_year_month = latest_year_month.replace("{", "{{").replace("}", "}}")

        # 月增率 - 只對最新那一筆套用
        try:
            month_change = pd.to_numeric(df_rev.loc[latest_idx, "月增率"], errors='coerce')
            if pd.notna(month_change):
                if month_change >= 0:
                    style_conditional.append({
                        "if": {
                            "filter_query": f'{{年月}} = "{safe_year_month}"',
                            "column_id": "月增率"
                        },
                        "backgroundColor": "#d60000",
                        "color": "white",
                        "fontWeight": "bold"
                    })
                else:
                    style_conditional.append({
                        "if": {
                            "filter_query": f'{{年月}} = "{safe_year_month}"',
                            "column_id": "月增率"
                        },
                        "backgroundColor": "#007500",
                        "color": "white",
                        "fontWeight": "bold"
                    })
        except Exception as e:
            print(f"月增率樣式設定錯誤: {e}")

        # 年增率 - 只對最新那一筆套用
        try:
            year_change = pd.to_numeric(df_rev.loc[latest_idx, "年增率"], errors='coerce')
            if pd.notna(year_change):
                if year_change >= 0:
                    style_conditional.append({
                        "if": {
                            "filter_query": f'{{年月}} = "{safe_year_month}"',
                            "column_id": "年增率"
                        },
                        "backgroundColor": "#d60000",
                        "color": "white",
                        "fontWeight": "bold"
                    })
                else:
                    style_conditional.append({
                        "if": {
                            "filter_query": f'{{年月}} = "{safe_year_month}"',
                            "column_id": "年增率"
                        },
                        "backgroundColor": "#007500",
                        "color": "white",
                        "fontWeight": "bold"
                    })
        except Exception as e:
            print(f"年增率樣式設定錯誤: {e}")

        # 累計年增率 - 只對最新那一筆套用
        try:
            cumulative_change = pd.to_numeric(df_rev.loc[latest_idx, "累計年增率"], errors='coerce')
            if pd.notna(cumulative_change):
                if cumulative_change >= 0:
                    style_conditional.append({
                        "if": {
                            "filter_query": f'{{年月}} = "{safe_year_month}"',
                            "column_id": "累計年增率"
                        },
                        "backgroundColor": "#d60000",
                        "color": "white",
                        "fontWeight": "bold"
                    })
                else:
                    style_conditional.append({
                        "if": {
                            "filter_query": f'{{年月}} = "{safe_year_month}"',
                            "column_id": "累計年增率"
                        },
                        "backgroundColor": "#007500",
                        "color": "white",
                        "fontWeight": "bold"
                    })
        except Exception as e:
            print(f"累計年增率樣式設定錯誤: {e}")

        # 營收新高/次高 - 只對最新那一筆套用
        try:
            revenue_value = str(df_rev.loc[latest_idx, "revenue"])
            if "(新高)" in revenue_value or "(次高)" in revenue_value:
                style_conditional.append({
                    "if": {
                        "filter_query": f'{{年月}} = "{safe_year_month}"',
                        "column_id": "revenue"
                    },
                    #"backgroundColor": "#d60000",
                    "color": "#d60000",
                    #"color": "white",
                    "fontWeight": "bold"
                })
        except Exception as e:
            print(f"營收樣式設定錯誤: {e}")

    # ---------- 表格 ----------
    rev_table = dash_table.DataTable(
        columns=[
            {"name": "年月", "id": "年月"},
            {"name": "營收", "id": "revenue"},
            {"name": "月增率", "id": "月增率"},
            {"name": "去年同期", "id": "去年同期"},
            {"name": "年增率", "id": "年增率"},
            {"name": "累計營收", "id": "累計營收"},
            {"name": "累計年增率", "id": "累計年增率"},
        ],
        data=df_rev[
            ["年月", "revenue", "月增率", "去年同期", "年增率", "累計營收", "累計年增率"]
        ].to_dict("records"),
        page_size=10,
        style_table={"overflowX": "auto"},
        style_cell={
            "textAlign": "center",
            "padding": "6px",
            "fontSize": "13px"
        },
        style_data_conditional=style_conditional
    )

    return html.Div([rev_chart, rev_table])

# 三大法人圖（始終使用日線數據）
# ==================================================
def build_fa_price_chart(df_fa, stock_code):
    """
    三大法人圖表：
    - 固定使用日線股價（不受技術面週期影響）
    - X軸以7天為單位標示日期
    - 最新資料的日期也要標示
    """
    stock_name = get_stock_name(stock_code)
    title_text = f"{stock_name} ({stock_code}) 三大法人每日買賣超 + 股價"

    # 固定取得日線資料
    try:
        df_k_daily = get_kline_data(stock_code, "D")
    except Exception as e:
        # 如果無法取得日線，返回錯誤圖表
        fig = go.Figure()
        fig.update_layout(
            title=title_text,
            annotations=[{
                "text": f"無法取得日線數據<br>{e}",
                "xref": "paper", "yref": "paper",
                "x": 0.5, "y": 0.5,
                "showarrow": False,
                "font": {"size": 14, "color": "red"}
            }]
        )
        return fig

    df_k_dates = pd.DataFrame({"Date": df_k_daily["Date"]})
    df_fa = df_fa.rename(columns={"日期": "Date"})
    df_fa["Date"] = pd.to_datetime(df_fa["Date"], errors="coerce")
    df_merged = pd.merge(df_k_dates, df_fa[["Date", "外資買賣超股數", "投信買賣超股數", "自營商買賣超股數"]],
                         on="Date", how="left").fillna(0)

    # 確保 Date 是 datetime 格式
    df_merged["Date"] = pd.to_datetime(df_merged["Date"])

    x = list(range(len(df_merged)))
    hover_text = df_merged["Date"].dt.strftime("%Y-%m-%d")

    fig = go.Figure()
    for col, name in zip(["外資買賣超股數", "投信買賣超股數", "自營商買賣超股數"],
                         ["外資", "投信", "自營商"]):
        fig.add_trace(go.Bar(
            x=x, y=df_merged[col], name=name,
            hovertemplate="%{text}<br>" + name + ": %{y}", text=hover_text, textposition="none"
        ))

    fig.add_trace(go.Scatter(
        x=x,
        y=df_k_daily["Close"],
        mode="lines",
        name="收盤價",
        line=dict(color="black", width=1.5),
        yaxis="y2",
        hovertemplate="日期: %{text}<br>股價: %{y}<extra></extra>",
        text=hover_text
    ))

    sum_pos = df_merged[["外資買賣超股數", "投信買賣超股數", "自營商買賣超股數"]].sum(axis=1).clip(lower=0).max()
    sum_neg = df_merged[["外資買賣超股數", "投信買賣超股數", "自營商買賣超股數"]].sum(axis=1).clip(upper=0).min()
    limit = max(sum_pos, abs(sum_neg)) * 1.1

    # 設定 X 軸刻度：每7天一個刻度 + 最新日期
    tickvals = []
    ticktext = []
    for i in range(0, len(df_merged), 7):
        tickvals.append(i)
        ticktext.append(df_merged["Date"].iloc[i].strftime("%Y-%m-%d"))

    # 加入最新日期（如果最後一個點不在7天倍數上）
    if len(df_merged) - 1 not in tickvals:
        tickvals.append(len(df_merged) - 1)
        ticktext.append(df_merged["Date"].iloc[-1].strftime("%Y-%m-%d"))

    fig.update_layout(
        title=title_text,
        barmode="relative",
        height=400,
        margin=dict(l=50, r=50, t=50, b=40),
        plot_bgcolor="white",
        paper_bgcolor="white",
        yaxis=dict(range=[-limit, limit], zeroline=True, zerolinecolor="black", title="買賣超(股)"),
        yaxis2=dict(overlaying="y", side="right", title="股價", showgrid=False),
        xaxis=dict(
            tickmode='array',
            tickvals=tickvals,
            ticktext=ticktext,
            tickangle=-45,
            showgrid=True,
            gridcolor="rgba(200,200,200,0.3)"
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        transition_duration=0
    )
    return fig


def build_fa_detail_table(df_fa):
    """
    建立三大法人明細表
    """
    if df_fa is None or df_fa.empty:
        return html.Div("❌ 無法取得三大法人資料", style={"color": "red", "padding": "20px"})

    # ✅ 格式化日期欄位
    df_fa_display = df_fa.copy()
    df_fa_display["日期"] = pd.to_datetime(df_fa_display["日期"]).dt.strftime("%Y-%m-%d")

    return dash_table.DataTable(
        columns=[{"name": c, "id": c} for c in df_fa_display.columns],
        data=df_fa_display.to_dict("records"),
        hidden_columns=["外資持股", "投信持股", "自營商持股", "單日合計持股"],
        style_table={"overflowX": "auto"},
        page_size=10,
        style_cell={"textAlign": "center", "padding": "5px"}
    )

def build_fa_summary_table(df_fa, stock_code):
    """
    建立三大法人買賣總表
    - 顯示最新資料日期
    - 兩個頁籤：單日 / 累計
    """

    if df_fa is None or df_fa.empty:
        return html.Div("❌ 無法取得三大法人資料", style={"color": "red", "padding": "20px"})

    # 確保日期欄位是 datetime 格式
    df_fa = df_fa.copy()
    df_fa["日期"] = pd.to_datetime(df_fa["日期"], errors='coerce')

    # 排序：最新日期在最上面
    df_fa = df_fa.sort_values("日期", ascending=False).reset_index(drop=True)

    # 最新資料日期
    latest_date = df_fa.loc[0, "日期"]
    date_str = latest_date.strftime("%Y/%m/%d")

    # ========== 單日頁籤 ==========
    def calculate_streak(df, column):
        """
        計算連買連賣
        Returns: (direction, days, total_shares)
            direction: "買" or "賣"
            days: 連續天數
            total_shares: 累計張數
        """
        if df.empty or column not in df.columns:
            return None, 0, 0

        latest_value = pd.to_numeric(df.loc[0, column], errors='coerce')
        if pd.isna(latest_value):
            return None, 0, 0

        direction = "買" if latest_value > 0 else "賣"
        days = 0
        total_shares = 0

        for i in range(len(df)):
            value = pd.to_numeric(df.loc[i, column], errors='coerce')
            if pd.isna(value):
                break

            if direction == "買" and value > 0:
                days += 1
                total_shares += value
            elif direction == "賣" and value < 0:
                days += 1
                total_shares += value
            else:
                break

        return direction, days, abs(total_shares)

    # 計算各法人的連買連賣
    foreign_dir, foreign_days, foreign_total = calculate_streak(df_fa, "外資買賣超股數")
    trust_dir, trust_days, trust_total = calculate_streak(df_fa, "投信買賣超股數")
    dealer_dir, dealer_days, dealer_total = calculate_streak(df_fa, "自營商買賣超股數")

    # 三大法人合計
    df_fa["三大法人合計"] = (
            pd.to_numeric(df_fa["外資買賣超股數"], errors='coerce').fillna(0) +
            pd.to_numeric(df_fa["投信買賣超股數"], errors='coerce').fillna(0) +
            pd.to_numeric(df_fa["自營商買賣超股數"], errors='coerce').fillna(0)
    )
    total_dir, total_days, total_total = calculate_streak(df_fa, "三大法人合計")

    # 最新一日的買賣超
    latest_foreign = pd.to_numeric(df_fa.loc[0, "外資買賣超股數"], errors='coerce')
    latest_trust = pd.to_numeric(df_fa.loc[0, "投信買賣超股數"], errors='coerce')
    latest_dealer = pd.to_numeric(df_fa.loc[0, "自營商買賣超股數"], errors='coerce')
    latest_total = pd.to_numeric(df_fa.loc[0, "三大法人合計"], errors='coerce')

    # 格式化連買連賣文字（合併張數資訊）
    def format_streak_with_shares(direction, days, total, latest_value):
        if latest_value == 0:
            if days <= 1:
                # 昨天也是 0
                return "平→平"
            else:
                # 從連買/連賣轉為平盤
                return f"連{days - 1}{direction}→平"

        if days <= 1:
            # 判斷方向轉換
            if direction == "買":
                return f"賣→買 共{total:,.0f}張"
            else:
                return f"買→賣 共{total:,.0f}張"
        # 連買連賣
        return f"連{days}{direction} 共{total:,.0f}張"

    single_day_data = [
        {"法人": "外資", "買賣超": f"{latest_foreign:+,.0f}",
         "連買連賣": format_streak_with_shares(foreign_dir, foreign_days, foreign_total, latest_foreign)},
        {"法人": "投信", "買賣超": f"{latest_trust:+,.0f}",
         "連買連賣": format_streak_with_shares(trust_dir, trust_days, trust_total, latest_trust)},
        {"法人": "自營商", "買賣超": f"{latest_dealer:+,.0f}",
         "連買連賣": format_streak_with_shares(dealer_dir, dealer_days, dealer_total, latest_dealer)},
        {"法人": "三大法人", "買賣超": f"{latest_total:+,.0f}",
         "連買連賣": format_streak_with_shares(total_dir, total_days, total_total, latest_total)},
    ]

    single_day_style = []
    for i, row in enumerate(single_day_data):
        # 買賣超顏色
        value = float(row["買賣超"].replace(",", ""))
        if value > 0:
            single_day_style.append({
                "if": {"row_index": i, "column_id": "買賣超"},
                "color": "#d60000",
                "fontWeight": "bold"
            })
        elif value < 0:
            single_day_style.append({
                "if": {"row_index": i, "column_id": "買賣超"},
                "color": "#007500",
                "fontWeight": "bold"
            })
        # 如果是 0，不加樣式（保持黑色）

        # 連買連賣顏色（包含張數資訊）
        streak_text = row["連買連賣"]
        if "平" in streak_text:
            continue
        if "連" in streak_text and "買" in streak_text:
            # 連買 → 紅色
            single_day_style.append({
                "if": {"row_index": i, "column_id": "連買連賣"},
                "color": "#d60000",
                "fontWeight": "bold"
            })
        elif "連" in streak_text and "賣" in streak_text:
            # 連賣 → 綠色
            single_day_style.append({
                "if": {"row_index": i, "column_id": "連買連賣"},
                "color": "#007500",
                "fontWeight": "bold"
            })
        elif "賣→買" in streak_text:
            # 賣轉買 → 紅色
            single_day_style.append({
                "if": {"row_index": i, "column_id": "連買連賣"},
                "color": "#d60000",
                "fontWeight": "bold"
            })
        elif "買→賣" in streak_text:
            # 買轉賣 → 綠色
            single_day_style.append({
                "if": {"row_index": i, "column_id": "連買連賣"},
                "color": "#007500",
                "fontWeight": "bold"
            })

    single_day_table = dash_table.DataTable(
        columns=[
            {"name": "單位(張)", "id": "法人"},
            {"name": "買賣超", "id": "買賣超"},
            {"name": "連買連賣", "id": "連買連賣"}
        ],
        data=single_day_data,
        style_table={"overflowX": "auto"},
        style_cell={
            "textAlign": "center",
            "padding": "10px",
            "fontSize": "14px"
        },
        style_header={
            "fontWeight": "bold",
            "backgroundColor": "#f0f0f0"
        },
        style_data_conditional=single_day_style
    )

    # ========== 累計頁籤 ==========
    def calculate_cumulative(df, column, days):
        """計算指定天數的累計買賣超"""
        if len(df) < days:
            return None
        values = pd.to_numeric(df.head(days)[column], errors='coerce').fillna(0)
        return values.sum()

    # 計算各期間的累計
    periods = {
        "2日": 2,
        "3日": 3,
        "5日": 5,
        "10日": 10,
        "1個月": 20,
        "3個月": 60,
        "半年": 120,
        "1年": 240
    }

    cumulative_data = []
    for investor_type, column in [
        ("外資", "外資買賣超股數"),
        ("投信", "投信買賣超股數"),
        ("自營商", "自營商買賣超股數"),
        ("三大法人", "三大法人合計")
    ]:
        row = {"法人": investor_type}
        for period_name, period_days in periods.items():
            value = calculate_cumulative(df_fa, column, period_days)
            row[period_name] = f"{value:+,.0f}" if value is not None else "N/A"
        cumulative_data.append(row)

    # 累計表格樣式（紅綠標示）
    cumulative_style = []
    for row_idx, row in enumerate(cumulative_data):
        for col_name in periods.keys():
            try:
                value_str = row[col_name]
                if value_str != "N/A":
                    value = float(value_str.replace(",", ""))
                    if value > 0:
                        cumulative_style.append({
                            "if": {"row_index": row_idx, "column_id": col_name},
                            "color": "#d60000",
                            "fontWeight": "bold"
                        })
                    elif value < 0:
                        cumulative_style.append({
                            "if": {"row_index": row_idx, "column_id": col_name},
                            "color": "#007500",
                            "fontWeight": "bold"
                        })
            except:
                pass

    cumulative_table = dash_table.DataTable(
        columns=[
            {"name": "單位(張)", "id": "法人"},
            {"name": "2日", "id": "2日"},
            {"name": "3日", "id": "3日"},
            {"name": "5日", "id": "5日"},
            {"name": "10日", "id": "10日"},
            {"name": "1個月", "id": "1個月"},
            {"name": "3個月", "id": "3個月"},
            {"name": "半年", "id": "半年"},
            {"name": "1年", "id": "1年"}
        ],
        data=cumulative_data,
        style_table={"overflowX": "auto"},
        style_cell={
            "textAlign": "center",
            "padding": "10px",
            "fontSize": "14px"
        },
        style_header={
            "fontWeight": "bold",
            "backgroundColor": "#f0f0f0"
        },
        style_data_conditional=cumulative_style
    )

    # ========== 組合總表 ==========
    return html.Div([
        # 資料日期
        html.Div(
            f"資料日期：{date_str}",
            style={
                "fontSize": "16px",
                "fontWeight": "bold",
                "marginBottom": "15px",
                "padding": "10px",
                "backgroundColor": "#e8f4f8",
                "borderRadius": "5px"
            }
        ),

        # 兩個小頁籤
        dcc.Tabs(
            id="fa-summary-tabs",
            value="single-day",
            children=[
                dcc.Tab(label="單日", value="single-day", children=[
                    html.Div(single_day_table, style={"padding": "15px"})
                ]),
                dcc.Tab(label="累計", value="cumulative", children=[
                    html.Div(cumulative_table, style={"padding": "15px"})
                ])
            ]
        )
    ], style={"marginBottom": "30px"})


# ==================================================
# 🆕 主動式 ETF 分析區塊
# ==================================================

# 2️⃣ 新增 ETF 配置字典（放在全域變數區）
ETF_CONFIG = {
    "復華": {
        "00991A": "復華台灣未來50主動式ETF"
    },
    "統一": {
        "00981A": "統一台股增長主動式ETF基金"
    }
    # 未來可擴充：
    # "元大": {
    #     "0056": "元大高股息"
    # }
}


def get_etf_list():
    """取得所有可選的 ETF 列表"""
    etf_list = []
    for fund_name, etfs in ETF_CONFIG.items():
        for etf_code, etf_name in etfs.items():
            etf_list.append({
                "label": f"{fund_name} - {etf_name} ({etf_code})",
                "value": f"{fund_name}_{etf_code}"
            })
    return etf_list


def build_etf_special_actions_section(df_diff):

    df_special = df_diff[df_diff['action_type'].notna() & (df_diff['action_type'] != '')].copy()

    if df_special.empty:
        return html.Div("✅ 今日無特殊持股異動")

    # 🔴 關鍵修正：重設 index，對齊 DataTable row_index
    df_special = df_special.reset_index(drop=True)

    df_special['shares_today_fmt'] = df_special['shares_today'].apply(lambda x: f"{x:,.0f}")
    df_special['shares_yesterday_fmt'] = df_special['shares_yesterday'].apply(lambda x: f"{x:,.0f}")
    df_special['shares_change_fmt'] = df_special['shares_change'].apply(lambda x: f"{x:+,.0f}")
    df_special['change_pct_fmt'] = df_special['change_pct'].apply(lambda x: f"{x:+.2f}%")

    style_conditional = []

    for i, row in df_special.iterrows():
        action = str(row['action_type']).strip()

        if '新買入' in action:
            bg, fg = "white", "red"
        elif '完全賣出' in action:
            bg, fg = "black", "white"
        elif '大幅增持' in action:
            bg, fg = "red", "yellow"
        elif '顯著增持' in action:
            bg, fg = "#ffb1b1", "black"
        elif '大幅減持' in action:
            bg, fg = "green", "white"
        elif '顯著減持' in action:
            bg, fg = "#b1ffb1", "black"
        else:
            continue

        style_conditional.append({
            "if": {
                "row_index": i,
                "column_id": "action_type"
            },
            "backgroundColor": bg,
            "color": fg,
            "fontWeight": "bold"
        })

    table = dash_table.DataTable(
        columns=[
            {"name": "動作類型", "id": "action_type"},
            {"name": "股票代號", "id": "stock_code"},
            {"name": "股票名稱", "id": "stock_name"},
            {"name": "今日股數", "id": "shares_today_fmt"},
            {"name": "昨日股數", "id": "shares_yesterday_fmt"},
            {"name": "變化股數", "id": "shares_change_fmt"},
            {"name": "變化幅度", "id": "change_pct_fmt"},
        ],
        data=df_special[
            [
                "action_type",
                "stock_code",
                "stock_name",
                "shares_today_fmt",
                "shares_yesterday_fmt",
                "shares_change_fmt",
                "change_pct_fmt",
            ]
        ].to_dict("records"),
        style_data_conditional=style_conditional,
        style_cell={"textAlign": "center", "padding": "10px"},
        style_table={"overflowX": "auto"},
    )

    return html.Div([
        html.H4(f"🚨 特殊持股異動 ({len(df_special)} 檔)"),
        table
    ])

def build_etf_top10_trend_chart(df_top10_trend):
    """
    🆕 建立 Top 10 持股變化圖表（近一個月）
    """
    fig = go.Figure()

    # 準備顏色（正數綠色，負數紅色）
    colors = ['#27ae60' if x > 0 else '#e74c3c' for x in df_top10_trend['change_pct']]

    fig.add_trace(go.Bar(
        y=df_top10_trend['stock_code'].astype(str) + " " + df_top10_trend['stock_name'],
        x=df_top10_trend['change_pct'],
        orientation='h',
        marker_color=colors,
        text=df_top10_trend['change_pct'].apply(lambda x: f"{x:+.2f}%"),
        textposition='auto',
        customdata=df_top10_trend[['first_shares', 'last_shares', 'shares_change']],
        hovertemplate=(
                "<b>%{y}</b><br>" +
                "期初股數: %{customdata[0]:,.0f}<br>" +
                "期末股數: %{customdata[1]:,.0f}<br>" +
                "變化: %{customdata[2]:+,.0f} (%{x:+.2f}%)<extra></extra>"
        )
    ))

    fig.update_layout(
        title="Top 10 持股近一個月變化",
        xaxis_title="變化幅度 (%)",
        yaxis_title="",
        height=450,
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=180, r=50, t=50, b=50)
    )

    return fig


def build_etf_top10_trend_line_chart(df: pd.DataFrame):
    """
    繪製 Top 10 持股的趨勢折線圖
    """
    # 🔍 DEBUG
    print("\n" + "=" * 60)
    print("🔍 DEBUG: 繪圖函數收到的股票")
    print("=" * 60)

    unique_stocks = df.groupby('stock_code').agg({
        'stock_name': 'first',
        'weight': 'last'
    }).sort_values('weight', ascending=False)

    for code, row in unique_stocks.iterrows():
        print(f"{code:6} {row['stock_name']:12} 最後百分比: {row['weight']:>6.2f}%")
    print("=" * 60 + "\n")

    df['weight'] = pd.to_numeric(df['weight'], errors='coerce')

    # 🔧 取得最新日期的權重，用於排序
    latest_date = df['date'].max()
    latest_weights = df[df['date'] == latest_date].set_index('stock_code')['weight'].to_dict()

    # 🔧 按最新權重排序股票代碼（由高到低）
    sorted_codes = sorted(
        df['stock_code'].unique(),
        key=lambda x: latest_weights.get(x, 0),
        reverse=True
    )

    fig = go.Figure()

    # 🔧 按照排序後的順序繪製（這樣 legend 和 hover 都會按照權重排序）
    for stock_code in sorted_codes:
        stock_df = df[df['stock_code'] == stock_code].sort_values('date')
        stock_name = stock_df.iloc[0]['stock_name']

        fig.add_trace(go.Scatter(
            x=stock_df['date'],
            y=stock_df['weight'],
            mode='lines+markers',
            name=f"{stock_code} {stock_name}",
            line=dict(width=2),
            marker=dict(size=6),
            # 🔧 自訂 hover 顯示格式
            hovertemplate='<b>%{fullData.name}</b><br>' +
                          #'日期: %{x|%Y-%m-%d}<br>' +
                          '權重: %{y:.2f}%' +
                          '<extra></extra>'
        ))

    fig.update_layout(
        title="Top 10 持股百分比趨勢",
        xaxis_title="日期",
        yaxis_title="持股百分比 (%)",
        hovermode='x unified',
        height=500,
        # 🔧 將 legend 放到圖表右側外面
        legend=dict(
            orientation="v",  # 垂直排列
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02,  # 放在圖表右邊外側（1.02 表示圖表寬度的 102% 位置）
            bgcolor="rgba(255, 255, 255, 0.8)",  # 半透明白色背景
            bordercolor="lightgray",
            borderwidth=1
        ),
        # 🔧 調整圖表邊距，為 legend 留出空間
        margin=dict(r=200)  # 右側留 200px 給 legend
    )

    return fig

def build_etf_holdings_chart(df_diff, etf_code):
    """
    建立持股變化圖表
    - 只顯示有變化的股票
    - 依據變化幅度排序
    """
    # 過濾出有變化的股票
    df_changed = df_diff[df_diff['shares_change'] != 0].copy()

    if df_changed.empty:
        fig = go.Figure()
        fig.update_layout(
            title=f"{etf_code} 持股變化",
            annotations=[{
                "text": "今日無持股變化",
                "xref": "paper", "yref": "paper",
                "x": 0.5, "y": 0.5,
                "showarrow": False,
                "font": {"size": 16, "color": "gray"}
            }]
        )
        return fig

    # 取前 20 大變化
    df_plot = df_changed.head(20).copy()

    # 準備顏色（正數紅色，負數綠色）
    colors = ['#d60000' if x > 0 else '#007500' for x in df_plot['change_pct']]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=df_plot['stock_code'].astype(str) + " " + df_plot['stock_name'].astype(str),
        x=df_plot['change_pct'],
        orientation='h',
        marker_color=colors,
        text=df_plot['change_pct'].apply(lambda x: f"{x:+.2f}%"),
        textposition='auto',
        customdata=df_plot[['shares_today', 'shares_yesterday', 'shares_change']],
        hovertemplate=(
                "<b>%{y}</b><br>" +
                "今日股數: %{customdata[0]:,.0f}<br>" +
                "昨日股數: %{customdata[1]:,.0f}<br>" +
                "變化: %{customdata[2]:+,.0f} (%{x:+.2f}%)<extra></extra>"
        )
    ))

    fig.update_layout(
        title=f"{etf_code} 持股變化 Top 20",
        xaxis_title="變化幅度 (%)",
        yaxis_title="",
        height=600,
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=150, r=50, t=50, b=50)
    )

    return fig


def build_etf_holdings_table(df_latest):
    """
    📋 ETF 最新持股表
    - 僅顯示最新日期
    - 依持股權重由大到小排序
    - Top 1–10 以底色標示
    """

    df_table = df_latest.copy()

    # 確保型別正確
    df_table['weight'] = pd.to_numeric(df_table['weight'], errors='coerce')
    df_table['shares'] = pd.to_numeric(df_table['shares'], errors='coerce')

    # 依權重排序
    df_table = df_table.sort_values(by='weight', ascending=False).reset_index(drop=True)

    # 格式化顯示
    df_table['shares'] = df_table['shares'].apply(lambda x: f"{x:,.0f}")
    df_table['weight'] = df_table['weight'].apply(lambda x: f"{x:.2f}%")

    # Top 10 樣式
    style_conditional = []

    for i in range(len(df_table)):
        if i < 10:
            style_conditional.append({
                "if": {"row_index": i},
                "backgroundColor": "rgba(52, 152, 219, 0.15)",  # 淡藍色
                "fontWeight": "bold"
            })

    return dash_table.DataTable(
        columns=[
            {"name": "排名", "id": "rank"},
            {"name": "股票代號", "id": "stock_code"},
            {"name": "股票名稱", "id": "stock_name"},
            {"name": "持股股數", "id": "shares"},
            {"name": "持股權重", "id": "weight"},
        ],
        data=[
            {
                "rank": i + 1,
                "stock_code": row["stock_code"],
                "stock_name": row["stock_name"],
                "shares": row["shares"],
                "weight": row["weight"]
            }
            for i, row in df_table.iterrows()
        ],
        page_size=15,
        style_table={"overflowX": "auto"},
        style_cell={
            "textAlign": "center",
            "padding": "8px",
            "fontSize": "13px"
        },
        style_data_conditional=style_conditional,
        sort_action="native"
    )


def build_etf_section(selected_etf):
    """
    建立主動式 ETF 完整區塊
    """
    if not selected_etf:
        return html.Div([
            html.Div(
                "請選擇要分析的 ETF",
                style={
                    "padding": "30px",
                    "textAlign": "center",
                    "color": "gray",
                    "fontSize": "16px"
                }
            )
        ])

    try:
        # 解析選擇的 ETF
        fund_name, etf_code = selected_etf.split("_")
        # ===============================
        # 依投信切換資料來源
        # ===============================
        if fund_name == "復華":
            data_api = fhtrust_data
        elif fund_name == "統一":
            data_api = ezmoney_data
        else:
            raise ValueError(f"未知投信：{fund_name}")

        print(f"🔍 載入 ETF: {selected_etf}")  # 除錯用
        print(f"📂 基金: {fund_name}, 代號: {etf_code}")  # 除錯用

        # ✅ 傳入 etf_code 參數
        df_hist =  data_api.load_history(etf_code=etf_code, days=30)
        print(f"✅ 載入 {len(df_hist)} 筆歷史資料")  # 除錯用
        print(f"📅 日期範圍: {df_hist['date'].min()} ~ {df_hist['date'].max()}")  # 除錯用

        latest_date = df_hist['date'].max()
        df_latest = df_hist[df_hist['date'] == latest_date]

        df_diff =  data_api.compute_diff(df_hist, highlight_pct=10)
        print(f"✅ 計算差異完成，共 {len(df_diff)} 檔股票")  # 除錯用

        df_top10_trend, df_top10_data =  data_api.compute_top10_trend(df_hist, top_n=10)
        print(f"✅ Top 10 分析完成")  # 除錯用

        # 取得最新日期
        latest_date = df_hist['date'].max()
        date_str = latest_date.strftime("%Y/%m/%d")

        # 統計資訊
        total_stocks = len(df_diff)
        changed_stocks = len(df_diff[df_diff['shares_change'] != 0])
        special_actions = len(df_diff[df_diff['action_type'] != ''])

        print(f"📊 統計: 總持股 {total_stocks}, 變化 {changed_stocks}, 特殊 {special_actions}")  # 除錯用

        return html.Div([
            # 📊 資訊卡片
            html.Div([
                html.Div([
                    html.H4(f"📊 {ETF_CONFIG[fund_name][etf_code]}",
                            style={"margin": "0", "color": "#2c3e50"}),
                    html.P(f"資料日期：{date_str}",
                           style={"margin": "5px 0", "color": "#7f8c8d"})
                ], style={"flex": "1"}),

                html.Div([
                    html.Div([
                        html.Span("持股數量", style={"fontSize": "12px", "color": "#7f8c8d"}),
                        html.H3(f"{total_stocks}", style={"margin": "5px 0", "color": "#3498db"})
                    ], style={"textAlign": "center", "padding": "10px"}),

                    html.Div([
                        html.Span("變化股票", style={"fontSize": "12px", "color": "#7f8c8d"}),
                        html.H3(f"{changed_stocks}", style={"margin": "5px 0", "color": "#e67e22"})
                    ], style={"textAlign": "center", "padding": "10px"}),

                    html.Div([
                        html.Span("特殊異動", style={"fontSize": "12px", "color": "#7f8c8d"}),
                        html.H3(f"{special_actions}", style={"margin": "5px 0", "color": "#e74c3c"})
                    ], style={"textAlign": "center", "padding": "10px"})
                ], style={"display": "flex", "gap": "20px"})
            ], style={
                "display": "flex",
                "justifyContent": "space-between",
                "alignItems": "center",
                "padding": "20px",
                "backgroundColor": "#ecf0f1",
                "borderRadius": "10px",
                "marginBottom": "20px"
            }),

            # 🚨 特殊動作區塊
            build_etf_special_actions_section(df_diff),

            html.Hr(style={"margin": "30px 0"}),

            # 🏆 Top 10 持股近一個月變化
            html.Div([
                html.H4("🏆 Top 10 持股近一個月變化", style={"marginBottom": "15px"}),

                # 變化幅度橫條圖
                #dcc.Graph(
                #    figure=build_etf_top10_trend_chart(df_top10_trend),
                #    style={"width": "100%", "height": "450px"}
                #),

                # 趨勢折線圖
                dcc.Graph(
                    figure=build_etf_top10_trend_line_chart(df_top10_data),
                    style={"width": "100%", "height": "500px", "marginTop": "20px"}
                )
            ], style={"marginTop": "30px"}),

            html.Hr(style={"margin": "30px 0"}),

            # 📈 所有持股變化
            html.Div([
                html.H4("📈 所有持股變化", style={"marginBottom": "15px"}),

                dcc.Graph(
                    figure=build_etf_holdings_chart(df_diff, etf_code),
                    style={"width": "100%", "height": "600px"}
                ),

                html.Div([
                    html.H4("📋 持股明細", style={"marginBottom": "10px", "marginTop": "20px"}),
                    build_etf_holdings_table(df_latest)
                ])
            ])
        ])

    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        print(f"❌ 錯誤詳情:\n{error_detail}")  # 除錯用

        return html.Div([
            html.Div(
                f"❌ 資料載入失敗：{str(e)}",
                style={
                    "padding": "30px",
                    "textAlign": "center",
                    "color": "red",
                    "fontSize": "16px"
                }
            ),
            html.Pre(
                error_detail,
                style={
                    "padding": "20px",
                    "backgroundColor": "#f8f8f8",
                    "border": "1px solid #ddd",
                    "borderRadius": "5px",
                    "fontSize": "12px",
                    "overflowX": "auto"
                }
            )
        ])


# ==================================================
# 營收圖
# ==================================================
def build_revenue_chart(df_rev, stock_code):
    stock_name = get_stock_name(stock_code)
    title_text = f"{stock_name} ({stock_code}) 近三年營收(月)"

    recent_years = sorted(df_rev["year"].unique(), reverse=True)[0:3]
    fig = go.Figure()
    for y in recent_years:
        df_year = df_rev[df_rev["year"] == y].sort_values("month")
        fig.add_trace(go.Scatter(
            x=df_year["month"],
            y=df_year["revenue"],
            mode="lines+markers",
            name=str(y)
        ))
    fig.update_layout(
        title=title_text,
        xaxis={"title": "月份", "tickmode": "linear"},
        yaxis={"title": "營收(仟元)"},
        height=420,
        legend=dict(traceorder="normal"),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    return fig


# ==================================================
# 🆕 6-1️⃣ EPS 圖表（重製版：單季/累季/年度切換）
# ==================================================
def build_eps_chart(stock_code, view_type="quarter", n_quarters=12):
    """
    EPS 圖表（單一視圖）
    view_type: "quarter" (單季), "cumulative" (累季), "yearly" (年度)
    """
    stock_name = get_stock_name(stock_code)

    try:
        df = get_profitability(stock_code)
    except Exception as e:
        fig = go.Figure()
        fig.update_layout(
            title=f"{stock_name} ({stock_code}) EPS 分析",
            annotations=[{
                "text": f"EPS 資料抓取失敗<br>{e}",
                "xref": "paper", "yref": "paper",
                "x": 0.5, "y": 0.5,
                "showarrow": False,
                "font": {"size": 14, "color": "red"}
            }]
        )
        return fig, None

    if df.empty or "EPS" not in df.columns:
        fig = go.Figure()
        fig.update_layout(
            title=f"{stock_name} ({stock_code}) EPS 分析",
            annotations=[{
                "text": "無 EPS 資料",
                "xref": "paper", "yref": "paper",
                "x": 0.5, "y": 0.5,
                "showarrow": False,
                "font": {"size": 16, "color": "gray"}
            }]
        )
        return fig, None

    # ========== 資料整理 ==========
    df = df.copy()
    df = df[::-1].reset_index(drop=True)  # 舊 → 新

    # 解析年份和季度
    def parse_season(season_str):
        import re
        season_str = str(season_str).strip()

        match = re.search(r'(\d{4})[\-\s]*Q?(\d)', season_str)
        if match:
            return int(match.group(1)), int(match.group(2))

        match = re.search(r'(\d{2,3})\.(\d)', season_str)
        if match:
            return int(match.group(1)) + 1911, int(match.group(2))

        match = re.search(r'(\d{3})(\d{2})', season_str)
        if match:
            year = int(match.group(1)) + 1911
            month = int(match.group(2))
            quarter = (month - 1) // 3 + 1
            return year, quarter

        return None, None

    df["年"] = None
    df["季"] = None
    for i, row in df.iterrows():
        year, quarter = parse_season(row["季別"])
        df.loc[i, "年"] = year
        df.loc[i, "季"] = quarter

    df = df[df["年"].notna()].copy().reset_index(drop=True)
    df["年"] = df["年"].astype(int)
    df["季"] = df["季"].astype(int)

    if df.empty:
        fig = go.Figure()
        fig.update_layout(
            title=f"{stock_name} ({stock_code}) EPS 分析",
            annotations=[{
                "text": "季別格式無法解析",
                "xref": "paper", "yref": "paper",
                "x": 0.5, "y": 0.5,
                "showarrow": False,
                "font": {"size": 16, "color": "red"}
            }]
        )
        return fig, None

    # ========== 計算各項指標 ==========
    # 1. 季增率（QoQ）
    df["季增率"] = None
    for i in range(1, len(df)):
        eps_prev = df.iloc[i - 1]["EPS"]
        eps_curr = df.iloc[i]["EPS"]
        if eps_prev != 0:
            df.loc[i, "季增率"] = (eps_curr - eps_prev) / abs(eps_prev) * 100

    # 2. 年增率（YoY）
    df["年增率"] = None
    for i in range(4, len(df)):
        eps_prev = df.iloc[i - 4]["EPS"]
        eps_curr = df.iloc[i]["EPS"]
        if eps_prev != 0:
            df.loc[i, "年增率"] = (eps_curr - eps_prev) / abs(eps_prev) * 100

    # 3. 累計 EPS
    df["累計EPS"] = 0.0
    for year in df["年"].unique():
        year_data = df[df["年"] == year]
        for idx in year_data.index:
            quarter = df.loc[idx, "季"]
            cumulative = df[(df["年"] == year) & (df["季"] <= quarter)]["EPS"].sum()
            df.loc[idx, "累計EPS"] = cumulative

    # 4. 累計年增率
    df["累計年增率"] = None
    for i, row in df.iterrows():
        year, quarter = row["年"], row["季"]
        cum_current = row["累計EPS"]

        last_year_same_q = df[(df["年"] == year - 1) & (df["季"] == quarter)]
        if not last_year_same_q.empty:
            cum_last_year = last_year_same_q.iloc[0]["累計EPS"]
            if cum_last_year != 0:
                df.loc[i, "累計年增率"] = (cum_current - cum_last_year) / abs(cum_last_year) * 100

    # ========== 根據 view_type 準備資料和圖表 ==========
    fig = go.Figure()

    if view_type == "quarter":
        # 單季視圖
        df_view = df.tail(n_quarters).copy().reset_index(drop=True)
        df_table = df_view[::-1].copy().reset_index(drop=True)
        df_table["年度/季別"] = df_table["年"].astype(str) + " Q" + df_table["季"].astype(str)
        df_table = df_table[["年度/季別", "EPS", "季增率", "年增率"]].round(2)
        df_table.rename(columns={"季增率": "QoQ(%)", "年增率": "YoY(%)", "EPS": "每股盈餘"}, inplace=True)

        # 繪製圖表
        fig.add_trace(go.Bar(
            x=df_view["季別"],
            y=df_view["EPS"],
            name="單季 EPS",
            marker_color="#9ccee7",
            customdata=df_view[["年", "季", "EPS"]],
            hovertemplate="<b>%{customdata[0]}年 Q%{customdata[1]}</b><br>" +
                          "每股盈餘: %{customdata[2]:.2f} 元<extra></extra>"
        ))

        fig.add_trace(go.Scatter(
            x=df_view["季別"],
            y=df_view["季增率"],
            name="季增率(%)",
            mode="lines+markers",
            line=dict(width=2, color="red"),
            marker=dict(size=6),
            yaxis="y2",
            hovertemplate="季增率: %{y:.2f}%<extra></extra>"
        ))

        fig.add_trace(go.Scatter(
            x=df_view["季別"],
            y=df_view["年增率"],
            name="年增率(%)",
            mode="lines+markers",
            line=dict(width=2, color="orange"),
            marker=dict(size=6),
            yaxis="y2",
            hovertemplate="年增率: %{y:.2f}%<extra></extra>"
        ))

        title_suffix = "單季"

    elif view_type == "cumulative":
        # 累季視圖
        df_view = df.tail(n_quarters).copy().reset_index(drop=True)
        df_table = df_view[::-1].copy().reset_index(drop=True)
        df_table["年度/季別"] = df_table["年"].astype(str) + " Q" + df_table["季"].astype(str)
        df_table = df_table[["年度/季別", "累計EPS", "累計年增率"]].round(2)
        df_table.rename(columns={"累計EPS": "每股盈餘", "累計年增率": "YoY(%)"}, inplace=True)

        # 繪製圖表
        fig.add_trace(go.Bar(
            x=df_view["季別"],
            y=df_view["累計EPS"],
            name="累計 EPS",
            marker_color="#9ccee7",
            customdata=df_view[["年", "季", "累計EPS"]],
            hovertemplate="<b>%{customdata[0]}年 Q%{customdata[1]}</b><br>" +
                          "累計盈餘: %{customdata[2]:.2f} 元<extra></extra>"
        ))

        fig.add_trace(go.Scatter(
            x=df_view["季別"],
            y=df_view["累計年增率"],
            name="累計年增率(%)",
            mode="lines+markers",
            line=dict(width=2, color="orange"),
            marker=dict(size=6),
            yaxis="y2",
            hovertemplate="累計年增率: %{y:.2f}%<extra></extra>"
        ))

        title_suffix = "累季"

    else:  # yearly
        # 年度視圖
        current_year = df["年"].max()
        df_yearly = df[df["季"] == 4].copy()
        df_yearly = df_yearly[df_yearly["年"] < current_year]
        df_yearly = df_yearly.tail(5).copy().reset_index(drop=True)

        # 計算年度年增率
        df_yearly["年度年增率"] = None
        for i in range(1, len(df_yearly)):
            eps_prev = df_yearly.iloc[i - 1]["累計EPS"]
            eps_curr = df_yearly.iloc[i]["累計EPS"]
            if eps_prev != 0:
                df_yearly.loc[i, "年度年增率"] = (eps_curr - eps_prev) / abs(eps_prev) * 100

        df_view = df_yearly
        df_table = df_yearly[::-1].copy().reset_index(drop=True)
        df_table = df_table[["年", "累計EPS", "年度年增率"]].round(2)
        df_table.rename(columns={"年": "年度", "累計EPS": "每股盈餘", "年度年增率": "YoY(%)"}, inplace=True)

        # 繪製圖表
        fig.add_trace(go.Bar(
            x=df_view["年"].astype(str),
            y=df_view["累計EPS"],
            name="年度 EPS",
            marker_color="#9ccee7",
            customdata=df_view[["年", "累計EPS"]],
            hovertemplate="<b>%{customdata[0]}年</b><br>" +
                          "全年盈餘: %{customdata[1]:.2f} 元<extra></extra>"
        ))

        fig.add_trace(go.Scatter(
            x=df_view["年"].astype(str),
            y=df_view["年度年增率"],
            name="年度年增率(%)",
            mode="lines+markers",
            line=dict(width=2, color="orange"),
            marker=dict(size=6),
            yaxis="y2",
            hovertemplate="年度年增率: %{y:.2f}%<extra></extra>"
        ))

        title_suffix = "年度"

    # ========== 版面設定 ==========
    fig.update_layout(
        title=f"{stock_name} ({stock_code}) EPS 分析 - {title_suffix}",
        height=450,
        margin=dict(l=60, r=60, t=80, b=60),
        plot_bgcolor="white",
        paper_bgcolor="white",
        hovermode="x unified",
        yaxis=dict(
            title="EPS (元)",
            side="left",
            showgrid=True,
            zeroline=True
        ),
        yaxis2=dict(
            title="成長率 (%)",
            side="right",
            overlaying="y",
            showgrid=False,
            zeroline=True,
            zerolinecolor="black"
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.15,
            xanchor="center",
            x=0.5
        )
    )

    return fig, df_table


def build_eps_section(stock_code, view_type="quarter", n_quarters=12):
    """
    建立完整的 EPS 區塊（圖表 + 表格）
    🔧 只對最新一筆資料套用紅綠樣式
    """
    # 取得圖表和表格資料
    fig, df_table = build_eps_chart(stock_code, view_type=view_type, n_quarters=n_quarters)

    if df_table is None or df_table.empty:
        return html.Div("暫無表格資料", style={"padding": "20px", "textAlign": "center", "color": "gray"})

    # ========== 1️⃣ 四捨五入到小數點第二位 ==========
    # 🔧 修改：先轉換為數值型態再四捨五入
    if "QoQ(%)" in df_table.columns:
        df_table["QoQ(%)"] = pd.to_numeric(df_table["QoQ(%)"], errors='coerce').round(2)
    if "YoY(%)" in df_table.columns:
        df_table["YoY(%)"] = pd.to_numeric(df_table["YoY(%)"], errors='coerce').round(2)

    # ========== 2️⃣ 單季模式：標註新高/次高 ==========
    if view_type == "quarter":
        try:
            # 取得完整資料來比對近 20 季
            from utils.crawler_profitability import get_profitability
            df_full = get_profitability(stock_code)

            if not df_full.empty and "EPS" in df_full.columns:
                # 解析季別
                def parse_season(season_str):
                    import re
                    season_str = str(season_str).strip()

                    match = re.search(r'(\d{4})[\-\s]*Q?(\d)', season_str)
                    if match:
                        return int(match.group(1)), int(match.group(2))

                    match = re.search(r'(\d{2,3})\.(\d)', season_str)
                    if match:
                        return int(match.group(1)) + 1911, int(match.group(2))

                    match = re.search(r'(\d{3})(\d{2})', season_str)
                    if match:
                        year = int(match.group(1)) + 1911
                        month = int(match.group(2))
                        quarter = (month - 1) // 3 + 1
                        return year, quarter

                    return None, None

                df_full = df_full.copy()
                df_full["年"] = None
                df_full["季"] = None
                for i, row in df_full.iterrows():
                    year, quarter = parse_season(row["季別"])
                    df_full.loc[i, "年"] = year
                    df_full.loc[i, "季"] = quarter

                df_full = df_full[df_full["年"].notna()].copy().reset_index(drop=True)
                df_full = df_full[::-1].reset_index(drop=True)  # 舊 → 新

                # 取最近 20 季的 EPS
                recent_20q = df_full.tail(20)["EPS"].tolist()

                # 最新一季的 EPS
                if len(recent_20q) > 0:
                    latest_eps = df_table.loc[0, "每股盈餘"]

                    # 排序找出排名
                    sorted_eps = sorted(recent_20q, reverse=True)

                    if latest_eps == sorted_eps[0]:
                        # 新高
                        df_table.loc[0, "每股盈餘"] = f"{latest_eps:.2f} (新高)"
                    elif len(sorted_eps) > 1 and latest_eps == sorted_eps[1]:
                        # 次高
                        df_table.loc[0, "每股盈餘"] = f"{latest_eps:.2f} (次高)"
                    else:
                        # 一般情況，確保格式化
                        df_table.loc[0, "每股盈餘"] = f"{latest_eps:.2f}"
        except Exception as e:
            print(f"標註新高/次高時發生錯誤: {e}")

    # ========== 準備表格的條件樣式（使用 filter_query 只標記最新一筆）==========
    style_conditional = []

    # 🔧 記錄最新一筆的識別值
    if len(df_table) > 0:
        if view_type == "yearly":
            latest_key = df_table.loc[0, "年度"] if "年度" in df_table.columns else None
            key_column = "年度"
        else:
            latest_key = df_table.loc[0, "年度/季別"] if "年度/季別" in df_table.columns else None
            key_column = "年度/季別"
    else:
        latest_key = None
        key_column = None

    if latest_key and len(df_table) >= 1:
        # 轉義特殊字符
        safe_key = str(latest_key).replace("{", "{{").replace("}", "}}")

        try:
            # YoY 紅綠標示
            if "YoY(%)" in df_table.columns:
                first_row_yoy = df_table.loc[0, "YoY(%)"]
                if pd.notna(first_row_yoy):
                    if first_row_yoy >= 0:
                        style_conditional.append({
                            "if": {
                                "filter_query": f'{{{key_column}}} = "{safe_key}"',
                                "column_id": "YoY(%)"
                            },
                            "backgroundColor": "#d60000",
                            "color": "white",
                            "fontWeight": "bold"
                        })
                    else:
                        style_conditional.append({
                            "if": {
                                "filter_query": f'{{{key_column}}} = "{safe_key}"',
                                "column_id": "YoY(%)"
                            },
                            "backgroundColor": "#007500",
                            "color": "white",
                            "fontWeight": "bold"
                        })
        except Exception as e:
            print(f"YoY 樣式設定錯誤: {e}")

        try:
            # QoQ 紅綠標示（僅單季模式）
            if "QoQ(%)" in df_table.columns:
                first_row_qoq = df_table.loc[0, "QoQ(%)"]
                if pd.notna(first_row_qoq):
                    if first_row_qoq >= 0:
                        style_conditional.append({
                            "if": {
                                "filter_query": f'{{{key_column}}} = "{safe_key}"',
                                "column_id": "QoQ(%)"
                            },
                            "backgroundColor": "#d60000",
                            "color": "white",
                            "fontWeight": "bold"
                        })
                    else:
                        style_conditional.append({
                            "if": {
                                "filter_query": f'{{{key_column}}} = "{safe_key}"',
                                "column_id": "QoQ(%)"
                            },
                            "backgroundColor": "#007500",
                            "color": "white",
                            "fontWeight": "bold"
                        })
        except Exception as e:
            print(f"QoQ 樣式設定錯誤: {e}")

        try:
            # 新高/次高紅色標註樣式
            if view_type == "quarter":
                eps_value = str(df_table.loc[0, "每股盈餘"])
                if "(新高)" in eps_value or "(次高)" in eps_value:
                    style_conditional.append({
                        "if": {
                            "filter_query": f'{{{key_column}}} = "{safe_key}"',
                            "column_id": "每股盈餘"
                        },
                        "color": "#d60000",
                        "fontWeight": "bold"
                    })
        except Exception as e:
            print(f"EPS 新高/次高樣式設定錯誤: {e}")

    # ========== 建立表格 ==========
    # 確保所有數值列都格式化為字串（避免 NaN 顯示問題）
    df_table_display = df_table.copy()
    for col in df_table_display.columns:
        # ⛔ 排除不該套小數格式的欄位
        if col in ["年度", "年度/季別"]:
            df_table_display[col] = df_table_display[col].apply(
                lambda x: str(int(x)) if pd.notna(x) and str(x).replace('.', '', 1).isdigit() else str(x)
            )
        elif col != "每股盈餘":
            df_table_display[col] = df_table_display[col].apply(
                lambda x: f"{x:.2f}" if pd.notna(x) and isinstance(x, (int, float)) else str(x)
            )

    eps_table = dash_table.DataTable(
        columns=[{"name": col, "id": col} for col in df_table_display.columns],
        data=df_table_display.to_dict("records"),
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "center", "padding": "6px", "fontSize": "13px"},
        style_data_conditional=style_conditional,
        page_size=10
    )

    return html.Div([
        # 圖表
        dcc.Graph(
            figure=fig,
            style={"width": "100%", "height": "450px"}
        ),
        # 表格
        eps_table
    ])

def build_gross_margin_section(stock_code):
    """
    建立「毛利率 / 營利率 / 稅後淨利率」分析區塊
    🔧 只對最新一筆資料套用紅綠樣式
    """

    # --------------------------------------------------
    # 1️⃣ 取得獲利能力資料
    # --------------------------------------------------
    df = get_profitability(stock_code)

    if df is None or df.empty:
        return html.Div("❌ 無法取得毛利率資料", style={"color": "red"})

    # --------------------------------------------------
    # 2️⃣ 數值欄位轉型（保險處理）
    # --------------------------------------------------
    num_cols = [
        "營業收入",
        "營業毛利",
        "毛利率",
        "營業利益",
        "營益率",
        "稅後淨利"
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # --------------------------------------------------
    # 3️⃣ 計算「稅後淨利率 (%)」
    # --------------------------------------------------
    df["稅後淨利率"] = (df["稅後淨利"] / df["營業收入"]) * 100

    # --------------------------------------------------
    # 4️⃣ 為表格排序（最新一期在最上）
    # --------------------------------------------------
    df_table = df.sort_values("季別", ascending=False).reset_index(drop=True)

    # 🔧 記錄最新一筆的季別
    latest_season = df_table.loc[0, "季別"] if len(df_table) > 0 else None

    # --------------------------------------------------
    # 5️⃣ 為圖表排序（舊→新，最新在最右邊）
    # --------------------------------------------------
    df_chart = df[::-1].reset_index(drop=True)  # ✅ 反轉順序

    # --------------------------------------------------
    # 6️⃣ 建立折線圖（使用 df_chart）
    # --------------------------------------------------
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_chart["季別"],
        y=df_chart["毛利率"],
        mode="lines+markers",
        name="毛利率 (%)"
    ))

    fig.add_trace(go.Scatter(
        x=df_chart["季別"],
        y=df_chart["營益率"],
        mode="lines+markers",
        name="營利率 (%)"
    ))

    fig.add_trace(go.Scatter(
        x=df_chart["季別"],
        y=df_chart["稅後淨利率"],
        mode="lines+markers",
        name="稅後淨利率 (%)"
    ))

    fig.update_layout(
        title=f"{get_stock_name(stock_code)} ({stock_code}) 獲利率趨勢",
        yaxis_title="百分比 (%)",
        height=380,
        legend=dict(orientation="h", y=-0.25),
        plot_bgcolor="white",
        paper_bgcolor="white"
    )

    # --------------------------------------------------
    # 7️⃣ DataTable 條件樣式（使用 filter_query 只標記最新一筆）
    # --------------------------------------------------
    style_conditional = []

    if latest_season and len(df_table) >= 2:
        # 轉義特殊字符
        safe_season = str(latest_season).replace("{", "{{").replace("}", "}}")

        # 取得最新一筆和前一筆的數值
        latest_gross = df_table.loc[0, "毛利率"]
        prev_gross = df_table.loc[1, "毛利率"]

        latest_operating = df_table.loc[0, "營益率"]
        prev_operating = df_table.loc[1, "營益率"]

        latest_net = df_table.loc[0, "稅後淨利率"]
        prev_net = df_table.loc[1, "稅後淨利率"]

        # 毛利率
        if pd.notna(latest_gross) and pd.notna(prev_gross):
            style_conditional.append({
                "if": {
                    "filter_query": f'{{季別}} = "{safe_season}"',
                    "column_id": "毛利率"
                },
                "backgroundColor": "#d60000" if latest_gross >= prev_gross else "#007500",
                "color": "white",
                "fontWeight": "bold"
            })

        # 營益率
        if pd.notna(latest_operating) and pd.notna(prev_operating):
            style_conditional.append({
                "if": {
                    "filter_query": f'{{季別}} = "{safe_season}"',
                    "column_id": "營益率"
                },
                "backgroundColor": "#d60000" if latest_operating >= prev_operating else "#007500",
                "color": "white",
                "fontWeight": "bold"
            })

        # 稅後淨利率
        if pd.notna(latest_net) and pd.notna(prev_net):
            style_conditional.append({
                "if": {
                    "filter_query": f'{{季別}} = "{safe_season}"',
                    "column_id": "稅後淨利率"
                },
                "backgroundColor": "#d60000" if latest_net >= prev_net else "#007500",
                "color": "white",
                "fontWeight": "bold"
            })

    # --------------------------------------------------
    # 8️⃣ 建立 DataTable（使用 df_table）
    # --------------------------------------------------
    table = dash_table.DataTable(
        columns=[
            {"name": "季別", "id": "季別"},
            {"name": "毛利率 (%)", "id": "毛利率"},
            {"name": "營利率 (%)", "id": "營益率"},
            {"name": "稅後淨利率 (%)", "id": "稅後淨利率"},
            {"name": "EPS", "id": "EPS"}
        ],
        data=df_table[[
            "季別", "毛利率", "營益率", "稅後淨利率", "EPS"
        ]].round(2).to_dict("records"),
        style_table={"overflowX": "auto"},
        style_cell={
            "textAlign": "center",
            "padding": "6px",
            "fontSize": "13px"
        },
        style_data_conditional=style_conditional,
        page_size=8
    )

    # --------------------------------------------------
    # 9️⃣ 圖 + 表一起回傳
    # --------------------------------------------------
    return html.Div([
        dcc.Graph(figure=fig),
        table
    ])

# ==================================================
# 🆕 資券分析區塊
# ==================================================
def build_margin_chart(df_margin, stock_code):
    """
    資券趨勢圖：
    - 融資餘額（左軸，柱狀圖）
    - 融券餘額（左軸，柱狀圖）
    - 券資比（右軸，折線圖）
    - 收盤價（右軸，折線圖）
    """
    stock_name = get_stock_name(stock_code)

    # 確保日期格式正確
    df_margin = df_margin.copy()
    df_margin["date"] = pd.to_datetime(df_margin["date"])

    # ✅ 過濾掉空值資料（假日或無交易日）
    df_margin = df_margin.dropna(subset=["fin_balance", "short_balance", "ratio"])

    # 由舊到新排序（圖表用）
    df_chart = df_margin.sort_values("date").reset_index(drop=True)
    # ✅ 就是這裡（唯一正確位置）
    if df_chart.empty:
        return None
    # ✅ 取得對應期間的日線資料
    try:
        df_k_daily = get_kline_data(stock_code, "D")
        df_k_daily["Date"] = pd.to_datetime(df_k_daily["Date"])

        # 合併股價資料到資券資料
        df_chart = pd.merge(
            df_chart,
            df_k_daily[["Date", "Close"]],
            left_on="date",
            right_on="Date",
            how="left"
        )
        df_chart.rename(columns={"Close": "stock_price"}, inplace=True)
        has_stock_price = True
    except Exception as e:
        print(f"無法取得股價資料: {e}")
        has_stock_price = False

    # ✅ 使用索引作為 X 軸（讓線圖連續）
    x = list(range(len(df_chart)))

    # ✅ 設定 X 軸刻度（每 7 天顯示一次日期）
    tickvals = []
    ticktext = []
    for i in range(0, len(df_chart), 7):
        tickvals.append(i)
        ticktext.append(df_chart["date"].iloc[i].strftime("%Y-%m-%d"))

    # 加入最新日期
    if len(df_chart) - 1 not in tickvals:
        tickvals.append(len(df_chart) - 1)
        ticktext.append(df_chart["date"].iloc[-1].strftime("%Y-%m-%d"))

    # ✅ 準備 hover 用的日期文字
    hover_dates = df_chart["date"].dt.strftime("%Y-%m-%d")

    fig = go.Figure()

    # 融資餘額（紅色柱狀圖）
    fig.add_trace(go.Bar(
        x=x,
        y=df_chart["fin_balance"],
        name="融資餘額",
        marker_color="rgba(255, 99, 71, 0.6)",
        yaxis="y1",
        customdata=hover_dates,
        hovertemplate="融資餘額: %{y:,.0f} 張<extra></extra>"
    ))

    # 融券餘額（綠色柱狀圖）
    fig.add_trace(go.Bar(
        x=x,
        y=df_chart["short_balance"],
        name="融券餘額",
        marker_color="rgba(60, 179, 113, 0.6)",
        yaxis="y1",
        customdata=hover_dates,
        hovertemplate="融券餘額: %{y:,.0f} 張<extra></extra>"
    ))

    # 券資比（黑色折線）
    fig.add_trace(go.Scatter(
        x=x,
        y=df_chart["ratio"],
        name="券資比 (%)",
        mode="lines+markers",
        line=dict(color="#BA543F", width=2),
        marker=dict(size=4),
        yaxis="y2",
        customdata=hover_dates,
        hovertemplate="券資比: %{y:.2f}%<extra></extra>"
    ))

    # ✅ 新增：日線股價（灰色折線，右軸 y3）
    if has_stock_price:
        fig.add_trace(go.Scatter(
            x=x,
            y=df_chart["stock_price"],
            name="收盤價",
            mode="lines",
            line=dict(color="rgba(128, 128, 128, 0.5)", width=1.5),
            yaxis="y3",
            customdata=hover_dates,
            hovertemplate="日期: %{customdata}<br>收盤價: %{y:.2f}<extra></extra>"
        ))

    fig.update_layout(
        title=f"{stock_name} ({stock_code}) 資券趨勢",
        height=420,
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=60, r=60, t=50, b=40),
        hovermode="x",  # ✅ 改為 "x" 而不是 "x unified"
        xaxis=dict(
            tickmode='array',
            tickvals=tickvals,
            ticktext=ticktext,
            tickangle=-45,
            showgrid=True,
            gridcolor="rgba(200,200,200,0.3)"
        ),
        yaxis=dict(
            title="餘額（張）",
            side="left",
            showgrid=True,
            zeroline=True
        ),
        yaxis2=dict(
            title="券資比 (%)",
            side="right",
            overlaying="y",
            showgrid=False,
            zeroline=True,
            position=0.95
        ),
        yaxis3=dict(
            title="股價",
            side="right",
            overlaying="y",
            showgrid=False,
            zeroline=False,
            anchor="free",
            position=1.0
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        )
    )

    return fig


def build_margin_section(df_margin, stock_code):
    """
    資券分析區塊：
    - 上方：趨勢圖
    - 下方：資料表格
    🔧 若無有效資券資料，整個區塊不顯示
    """

    import pandas as pd
    from dash import html, dcc
    from dash import dash_table

    # ---------- 0️⃣ 基本防呆 ----------
    if df_margin is None or df_margin.empty:
        return html.Div()

    # ---------- 1️⃣ 判斷是否有「有效資券資料」（關鍵） ----------
    df_check = df_margin.copy()

    # 只關心餘額是否真的有值
    for col in ["fin_balance", "short_balance"]:
        if col in df_check.columns:
            df_check[col] = pd.to_numeric(df_check[col], errors="coerce").fillna(0)
        else:
            df_check[col] = 0

    has_valid_data = (
        (df_check["fin_balance"] != 0) |
        (df_check["short_balance"] != 0)
    ).any()

    stock_name = get_stock_name(stock_code)
    # ❌ 全為 0 / 無資券資料 → 顯示提示文字，不畫圖、不畫表
    if not has_valid_data:
        return html.Div([
            html.Div(
                f"{stock_name}{(stock_code)} 無資券資料",
                style={
                    "color": "#999",
                    "fontSize": "14px",
                    "padding": "8px 0"
                }
            )
        ])

    # ---------- 2️⃣ 圖表 ----------
    fig = build_margin_chart(df_margin, stock_code)
    if fig is None:
        return html.Div()

    margin_chart = dcc.Graph(
        figure=fig,
        style={"width": "100%", "height": "420px"}
    )

    # ---------- 3️⃣ 表格（最新在最上） ----------
    df_table = df_margin.copy()
    df_table["date"] = pd.to_datetime(df_table["date"], errors="coerce")
    df_table = df_table.dropna(subset=["date"])
    df_table = df_table.sort_values("date", ascending=False)
    df_table["date"] = df_table["date"].dt.strftime("%Y-%m-%d")

    if df_table.empty:
        return html.Div()

    # ---------- 4️⃣ 最新一筆紅綠樣式 ----------
    latest_date = df_table.iloc[0]["date"]

    style_conditional = []

    # 融資增減
    fin_change_val = pd.to_numeric(df_table.iloc[0].get("fin_change"), errors="coerce")
    if pd.notna(fin_change_val):
        style_conditional.append({
            "if": {
                "filter_query": f'{{date}} = "{latest_date}"',
                "column_id": "fin_change"
            },
            "backgroundColor": "#d60000" if fin_change_val > 0 else "#007500" if fin_change_val < 0 else "white",
            "color": "white" if fin_change_val != 0 else "black",
            "fontWeight": "bold" if fin_change_val != 0 else "normal"
        })

    # 融券增減
    short_change_val = pd.to_numeric(df_table.iloc[0].get("short_change"), errors="coerce")
    if pd.notna(short_change_val):
        style_conditional.append({
            "if": {
                "filter_query": f'{{date}} = "{latest_date}"',
                "column_id": "short_change"
            },
            "backgroundColor": "#d60000" if short_change_val > 0 else "#007500" if short_change_val < 0 else "white",
            "color": "white" if short_change_val != 0 else "black",
            "fontWeight": "bold" if short_change_val != 0 else "normal"
        })

    # ---------- 5️⃣ 表格 ----------
    margin_table = dash_table.DataTable(
        columns=[
            {"name": "日期", "id": "date"},
            {"name": "融資買進", "id": "fin_buy"},
            {"name": "融資賣出", "id": "fin_sell"},
            {"name": "融資餘額", "id": "fin_balance"},
            {"name": "融資增減", "id": "fin_change"},
            {"name": "融券賣出", "id": "short_sell"},
            {"name": "融券買進", "id": "short_buy"},
            {"name": "融券餘額", "id": "short_balance"},
            {"name": "融券增減", "id": "short_change"},
            {"name": "券資比", "id": "ratio"},
            {"name": "資券當沖", "id": "offset"}
        ],
        data=df_table[[
            "date", "fin_buy", "fin_sell", "fin_balance", "fin_change",
            "short_sell", "short_buy", "short_balance", "short_change",
            "ratio", "offset"
        ]].to_dict("records"),
        page_size=10,
        style_table={"overflowX": "auto"},
        style_cell={
            "textAlign": "center",
            "padding": "6px",
            "fontSize": "13px"
        },
        style_data_conditional=style_conditional
    )

    return html.Div([margin_chart, margin_table])

# ==================================================
# 🆕 6-3️⃣ 財報摘要 (未來實作)
# ==================================================
def build_financial_statement_table(stock_code):
    """
    未來實作: 從資料源抓取財報資料
    目前先回傳範例表格
    """
    return dash_table.DataTable(
        columns=[
            {"name": "年度", "id": "year"},
            {"name": "營收", "id": "revenue"},
            {"name": "毛利", "id": "gross_profit"},
            {"name": "營業利益", "id": "operating_income"},
            {"name": "淨利", "id": "net_income"}
        ],
        data=[
            {"year": "2024", "revenue": "尚未實作", "gross_profit": "-", "operating_income": "-", "net_income": "-"}
        ],
        style_cell={"textAlign": "center", "padding": "5px"},
        style_header={"fontWeight": "bold", "backgroundColor": "#f0f0f0"}
    )


# ==================================================
# 7️⃣ Dash App
# ==================================================
app = Dash(__name__, suppress_callback_exceptions=True)
app.title = "Stock War Room"

def build_war_room_layout():
    return html.Div([
        html.Div([
            dcc.Input(id="stock-input", placeholder="輸入股票代碼", style={"width": "160px"}),
            html.Button("查詢", id="query-btn", style={"marginLeft": "10px"})
        ], style={"marginBottom": "10px"}),

        dcc.Tabs(
            id="tabs",
            value="tab-tech",
            children=[
                dcc.Tab(label="技術面", value="tab-tech"),
                dcc.Tab(label="財務面", value="tab-revenue"),
                dcc.Tab(label="籌碼面", value="tab-chips"),
                dcc.Tab(label="三大法人", value="tab-fa")
            ],
            style={"width": "100%"}
        ),

        dcc.Loading(
            id="loading-tabs-content",
            type="circle",
            children=html.Div(id="tabs-content", style={"width": "100%", "marginTop": "20px"})
        )
    ])


# ==================================================
# 8️⃣ Layout (左側：功能入口 + 動態區域 | 右側：主顯示區)
# ==================================================
app.layout = html.Div([
    # 隱藏的 Store 元件用來儲存 period 狀態
    dcc.Store(id='period-store', data='D'),

    html.Div([
        # ===== 左側區域 (15%) =====
        html.Div([
            # ===== 1️⃣ 最上方：功能入口選擇器（永遠顯示）=====
            html.Div([
                html.Div("功能入口", style={"fontWeight": "bold", "marginBottom": "5px"}),
                dcc.RadioItems(
                    id="entry-selector",
                    options=[
                        {"label": "戰情室", "value": "A"},
                        {"label": "擴充模組", "value": "B"},
                        {"label": "快速選股", "value": "C"}
                    ],
                    value="A",
                    inline=False
                ),
                html.Hr()
            ], style={"marginBottom": "10px"}),

            # ===== 2️⃣ 戰情室專用區域（預設顯示，其他模式隱藏）=====
            html.Div(
                id="war-room-left-panel",
                children=[
                    # 股票清單上傳
                    dcc.Upload(
                        id="upload-file",
                        children=html.Button("上傳股票檔案"),
                        multiple=False,
                        style={"marginBottom": "10px"}
                    ),

                    # 股票清單表格
                    dash_table.DataTable(
                        id="stock-list-table",
                        columns=[{"name": "股票代號(名稱)", "id": "stock"}],
                        style_table={"overflowY": "auto", "height": "60vh"},
                        style_cell={"textAlign": "left", "padding": "5px"},
                        row_selectable="single",
                        selected_rows=[]
                    )
                ],
                style={"display": "block"}  # 預設顯示（戰情室模式）
            ),

            # ===== 3️⃣ 快速選股專用區域（預設隱藏）=====
            html.Div(
                id="quick-filter-buttons-container",
                children=[],
                style={"display": "none"}
            )

        ], style={
            "width": "15%",
            "display": "inline-block",
            "verticalAlign": "top",
            "paddingRight": "1%"
        }),

        # ===== 右側主顯示區 (75%) =====
        html.Div(
            id="right-panel",
            style={
                "width": "84%",
                "display": "inline-block",
                "verticalAlign": "top"
            }
        )
    ])
])


# ==================================================
# 9️⃣ Callback: 上傳檔案解析
# ==================================================
@app.callback(
    Output("stock-list-table", "data"),
    Input("upload-file", "contents"),
    State("upload-file", "filename")
)
def parse_uploaded_file(contents, filename):
    if contents is None:
        return []
    import base64, io
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')), dtype=str)
        elif filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(io.BytesIO(decoded))
        else:
            return []
    except Exception as e:
        print(f"檔案解析失敗: {e}")
        return []

    if "code" not in df.columns:
        df.rename(columns={df.columns[0]: "code"}, inplace=True)
    if "name" not in df.columns:
        df["name"] = df["code"].apply(lambda x: get_stock_name(str(x)))

    df["stock"] = df.apply(lambda r: f"{r['code']} {r['name']}", axis=1)
    return df[["stock"]].to_dict("records")


# ==================================================
# 🔟 Callback: 左側選中行同步更新輸入框
# ==================================================
@app.callback(
    Output("stock-input", "value"),
    Output("query-btn", "n_clicks"),
    Input("stock-list-table", "selected_rows"),
    State("stock-list-table", "data"),
    State("query-btn", "n_clicks")
)
def sync_stock_input(selected_rows, table_data, current_clicks):
    if selected_rows and table_data:
        stock_code = table_data[selected_rows[0]]["stock"].split()[0]
        new_clicks = (current_clicks or 0) + 1
        return stock_code, new_clicks
    return dash.no_update, dash.no_update


# ==================================================
# 1️⃣1️⃣ Callback: 查詢並更新右側Tabs
# ==================================================
@app.callback(
    Output("tabs-content", "children"),
    Output("period-store", "data"),
    Input("query-btn", "n_clicks"),
    Input("tabs", "value"),
    State("stock-input", "value"),
    State("period-store", "data")
)
def update_tab_content(n_clicks, selected_tab, stock_code, current_period):
    if not stock_code:
        return "請選擇股票代號", current_period

    period = current_period if current_period else "D"

    # ✅ 直接呼叫通用函數
    # ⭐ 呼叫時加上 prefix="war-"
    content = build_stock_tabs_content(
        stock_code=stock_code,
        selected_tab=selected_tab,
        period=period,
        prefix="war-"  # ⭐ 戰情室使用 "war-" 前綴
    )

    return content, period

# ==================================================
# 🆕 輔助函數: 建立可摺疊區塊
# ==================================================
def create_accordion_section(section_id, title, content, is_open=False, prefix=""):
    """
    建立單一 Accordion 區塊
    ✅ 使用 Pattern-Matching ID 避免衝突
    """
    full_id = f"{prefix}{section_id}"

    return html.Div([
        # 標題列
        html.Div(
            [
                html.Span(title, style={"fontWeight": "bold", "fontSize": "16px"}),
                html.Span(
                    "▼" if is_open else "▶",
                    id={"type": "accordion-arrow", "id": full_id},  # ⭐ Pattern-Matching
                    style={"float": "right", "fontSize": "14px"}
                )
            ],
            id={"type": "accordion-header", "id": full_id},  # ⭐ Pattern-Matching
            style={
                "padding": "12px 15px",
                "backgroundColor": "#e8f4f8",
                "cursor": "pointer",
                "borderRadius": "5px",
                "marginBottom": "5px",
                "border": "1px solid #ccc",
                "userSelect": "none"
            },
            n_clicks=0
        ),
        # 內容區
        html.Div(
            content,
            id={"type": "accordion-content", "id": full_id},  # ⭐ Pattern-Matching
            style={
                "display": "block" if is_open else "none",
                "padding": "15px",
                "backgroundColor": "#f9f9f9",
                "border": "1px solid #ddd",
                "borderTop": "none",
                "borderRadius": "0 0 5px 5px",
                "marginBottom": "15px"
            }
        )
    ], style={"marginBottom": "10px"})


# ==================================================
# 🆕 統一的 Accordion 展開/收合 Callback
# ==================================================
@app.callback(
    Output({"type": "accordion-content", "id": dash.dependencies.MATCH}, "style"),
    Output({"type": "accordion-arrow", "id": dash.dependencies.MATCH}, "children"),
    Input({"type": "accordion-header", "id": dash.dependencies.MATCH}, "n_clicks"),
    State({"type": "accordion-content", "id": dash.dependencies.MATCH}, "style"),
    prevent_initial_call=True
)
def toggle_accordion_section(n_clicks, current_style):
    """統一處理所有 Accordion 的展開/收合"""
    if not n_clicks:
        return dash.no_update, dash.no_update

    is_open = current_style.get("display") == "block"
    new_style = current_style.copy()
    new_style["display"] = "none" if is_open else "block"
    arrow = "▶" if is_open else "▼"

    return new_style, arrow
""""
@app.callback(
    Output("content-eps", "style"),
    Output("arrow-eps", "children"),
    Input("header-eps", "n_clicks"),
    State("content-eps", "style"),
    prevent_initial_call=True
)
def toggle_eps_section(n_clicks, current_style):
    is_open = current_style.get("display") == "block"
    new_style = current_style.copy()
    new_style["display"] = "none" if is_open else "block"
    arrow = "▶" if is_open else "▼"
    return new_style, arrow
"""

@app.callback(
    Output("content-gross-margin", "style"),
    Output("arrow-gross-margin", "children"),
    Input("header-gross-margin", "n_clicks"),
    State("content-gross-margin", "style"),
    prevent_initial_call=True
)
def toggle_gross_margin_section(n_clicks, current_style):
    is_open = current_style.get("display") == "block"
    new_style = current_style.copy()
    new_style["display"] = "none" if is_open else "block"
    arrow = "▶" if is_open else "▼"
    return new_style, arrow


@app.callback(
    Output("content-financial-statement", "style"),
    Output("arrow-financial-statement", "children"),
    Input("header-financial-statement", "n_clicks"),
    State("content-financial-statement", "style"),
    prevent_initial_call=True
)
def toggle_financial_statement_section(n_clicks, current_style):
    is_open = current_style.get("display") == "block"
    new_style = current_style.copy()
    new_style["display"] = "none" if is_open else "block"
    arrow = "▶" if is_open else "▼"
    return new_style, arrow


# ==================================================
# 4️⃣ 新增 ETF Accordion 展開/收合 callback
# ==================================================
@app.callback(
    Output("content-chips-etf", "style"),
    Output("arrow-chips-etf", "children"),
    Input("header-chips-etf", "n_clicks"),
    State("content-chips-etf", "style"),
    prevent_initial_call=True
)
def toggle_chips_etf_section(n_clicks, current_style):
    is_open = current_style.get("display") == "block"
    new_style = current_style.copy()
    new_style["display"] = "none" if is_open else "block"
    arrow = "▶" if is_open else "▼"
    return new_style, arrow

# ==================================================
# 5️⃣ 新增 ETF 選擇器 callback
# ==================================================
@app.callback(
    Output("etf-content-container", "children"),
    Input("etf-selector", "value"),
    #prevent_initial_call=True
)
def update_etf_content(selected_etf):
    """
    根據選擇的 ETF 更新內容
    """
    return build_etf_section(selected_etf)
# ==================================================
# 🆕 Callback: 籌碼面 Accordion 展開/收合
# ==================================================
@app.callback(
    Output("content-chips-fa", "style"),
    Output("arrow-chips-fa", "children"),
    Input("header-chips-fa", "n_clicks"),
    State("content-chips-fa", "style"),
    prevent_initial_call=True
)
def toggle_chips_fa_section(n_clicks, current_style):
    is_open = current_style.get("display") == "block"
    new_style = current_style.copy()
    new_style["display"] = "none" if is_open else "block"
    arrow = "▶" if is_open else "▼"
    return new_style, arrow


@app.callback(
    Output("content-chips-margin", "style"),
    Output("arrow-chips-margin", "children"),
    Input("header-chips-margin", "n_clicks"),
    State("content-chips-margin", "style"),
    prevent_initial_call=True
)
def toggle_chips_margin_section(n_clicks, current_style):
    is_open = current_style.get("display") == "block"
    new_style = current_style.copy()
    new_style["display"] = "none" if is_open else "block"
    arrow = "▶" if is_open else "▼"
    return new_style, arrow


@app.callback(
    Output("content-chips-main", "style"),
    Output("arrow-chips-main", "children"),
    Input("header-chips-main", "n_clicks"),
    State("content-chips-main", "style"),
    prevent_initial_call=True
)
def toggle_chips_main_section(n_clicks, current_style):
    is_open = current_style.get("display") == "block"
    new_style = current_style.copy()
    new_style["display"] = "none" if is_open else "block"
    arrow = "▶" if is_open else "▼"
    return new_style, arrow


@app.callback(
    Output("content-chips-ownership", "style"),
    Output("arrow-chips-ownership", "children"),
    Input("header-chips-ownership", "n_clicks"),
    State("content-chips-ownership", "style"),
    prevent_initial_call=True
)
def toggle_chips_ownership_section(n_clicks, current_style):
    is_open = current_style.get("display") == "block"
    new_style = current_style.copy()
    new_style["display"] = "none" if is_open else "block"
    arrow = "▶" if is_open else "▼"
    return new_style, arrow


# ==================================================
# 🆕 Callback: 監聽週期選擇器變化
# ==================================================
@app.callback(
    Output("period-store", "data", allow_duplicate=True),
    Output("tabs-content", "children", allow_duplicate=True),
    Input("period-radio", "value"),
    State("stock-input", "value"),
    prevent_initial_call=True
)
def update_period(period, stock_code):
    if not stock_code or not period:
        return dash.no_update, dash.no_update

    try:
        df_k = get_kline_data(stock_code, period)
        return period, html.Div([
            html.Div([
                html.Label("選擇週期:", style={"marginRight": "10px", "fontWeight": "bold"}),
                dcc.RadioItems(
                    id="period-radio",
                    options=[
                        {"label": "日線", "value": "D"},
                        {"label": "週線", "value": "W"},
                        {"label": "月線", "value": "M"}
                    ],
                    value=period,
                    inline=True,
                    labelStyle={"marginRight": "15px"}
                )
            ], style={"marginBottom": "15px", "padding": "10px", "backgroundColor": "#f0f0f0", "borderRadius": "5px"}),

            dcc.Graph(
                figure=build_chart(df_k, stock_code, period),
                style={"width": "100%", "height": "520px"}
            )
        ])
    except Exception as e:
        return period, html.Div(f"更新失敗: {e}")


# ==================================================
# 1️⃣2️⃣ Callback: 高亮左側選中的股票
# ==================================================
@app.callback(
    Output("stock-list-table", "style_data_conditional"),
    Input("stock-input", "value"),
    State("stock-list-table", "data")
)
def highlight_selected_row(stock_code, table_data):
    if not table_data or not stock_code:
        return []

    style = []
    for i, row in enumerate(table_data):
        row_code = row["stock"].split()[0]
        if row_code == stock_code:
            style.append({
                "if": {"row_index": i},
                "backgroundColor": "#FFD700",
                "color": "black",
                "fontWeight": "bold"
            })
    return style


# ==================================================
# 🆕 Callback: EPS 視圖切換
# ==================================================
@app.callback(
    Output("eps-content-container", "children"),
    Input("eps-view-radio", "value"),
    State("stock-input", "value"),
    prevent_initial_call=True
)
def update_eps_view(view_type, stock_code):
    """
    根據選擇的視圖類型更新 EPS 圖表和表格
    """
    if not stock_code:
        return dash.no_update

    return build_eps_section(stock_code, view_type=view_type, n_quarters=12)


@app.callback(
    Output("right-panel", "children"),
    Output("war-room-left-panel", "style"),  # 🆕 控制戰情室左側區域
    Output("quick-filter-buttons-container", "style"),
    Output("quick-filter-buttons-container", "children"),
    Input("entry-selector", "value")
)
def switch_entry(entry):
    # ========== A 入口：戰情室 ==========
    if entry == "A":
        return (
            build_war_room_layout(),
            {"display": "block"},  # ✅ 顯示戰情室左側區域
            {"display": "none"},  # 隱藏篩選按鈕
            []
        )

    # ========== B 入口：擴充模組 ==========
    elif entry == "B":
        etf_content = html.Div([
            html.H3("🎯 主動式 ETF 分析", style={"marginBottom": "20px"}),

            # ETF 選擇器
            html.Div([
                html.Label("選擇 ETF:", style={"marginRight": "10px", "fontWeight": "bold"}),
                dcc.Dropdown(
                    id="etf-selector",
                    options=get_etf_list(),
                    value="復華_00991A",
                    clearable=False,
                    style={"width": "400px"}
                )
            ], style={"marginBottom": "20px"}),

            # 動態內容區域
            html.Div(
                id="etf-content-container",
                children=build_etf_section("復華_00991A")
            )
        ])

        return (
            html.Div(etf_content, style={"padding": "20px"}),
            {"display": "none"},  # ✅ 隱藏戰情室左側區域
            {"display": "none"},  # 隱藏篩選按鈕
            []
        )

    # ========== C 入口：快速選股 ==========
    elif entry == "C":
        # 右側佈局
        right_layout = html.Div([
            # 上方：篩選結果表格
            html.Div(
                id="filter-result-container",
                children=[
                    html.Div(
                        "請在左側選擇篩選條件",
                        style={
                            "padding": "50px",
                            "textAlign": "center",
                            "color": "#999",
                            "fontSize": "16px"
                        }
                    )
                ],
                style={
                    "height": "40vh",
                    "overflowY": "auto",
                    "marginBottom": "20px",
                    "border": "1px solid #ddd",
                    "borderRadius": "5px",
                    "backgroundColor": "#f9f9f9"
                }
            ),

            # 下方：戰情室頁籤
            html.Div(
                id="filter-detail-tabs-container",
                children=[],
                style={"height": "55vh", "overflowY": "auto"}
            )
        ])

        # 左側按鈕內容
        filter_buttons = html.Div([
            html.Div(
                "篩選條件",
                style={
                    "fontWeight": "bold",
                    "marginBottom": "10px",
                    "fontSize": "14px",
                    "color": "#2c3e50"
                }
            ),
            html.Div([
                html.Button(
                    config["label"],
                    id={"type": "filter-btn", "index": name},
                    n_clicks=0,
                    style={
                        "width": "100%",
                        "margin": "5px 0",
                        "padding": "8px",
                        "backgroundColor": "white",
                        "border": f"2px solid {config['color']}",
                        "borderRadius": "5px",
                        "cursor": "pointer",
                        "fontSize": "12px",
                        "transition": "all 0.3s"
                    }
                )
                for name, config in FILTER_CONDITIONS.items()
            ])
        ])

        return (
            right_layout,
            {"display": "none"},  # ✅ 隱藏戰情室左側區域
            {"display": "block", "marginTop": "10px"},  # 顯示篩選按鈕
            filter_buttons
        )

    # 預設返回
    return (
        html.Div("請選擇功能", style={"padding": "20px", "textAlign": "center"}),
        {"display": "none"},
        {"display": "none"},
        []
    )
@app.callback(
    Output("stock-panel", "style"),
    Input("entry-selector", "value")
)
def toggle_stock_panel(entry):
    """
    戰情室(A)：顯示左側股票相關功能
    其他入口(B)：全部隱藏
    """
    if entry == "A":
        return {"marginTop": "10px"}
    return {"display": "none"}


# ==================================================
# 🆕 快速選股 Callbacks
# ==================================================

# 1. 條件按鈕點擊處理 (切換選中狀態)


# 2. 更新篩選結果表格

# 3. 點選表格某列時，下方顯示該股票的詳細資訊 (重用戰情室)
@app.callback(
    Output("filter-detail-section", "children"),
    Input("filter-result-datatable", "selected_rows"),
    State("filter-result-datatable", "data"),
    prevent_initial_call=True
)
def show_stock_detail(selected_rows, table_data):
    """點選表格列時，下方顯示該股票的技術面/財務面/籌碼面"""
    if not selected_rows or not table_data:
        return []

    stock_code = table_data[selected_rows[0]]["代號"]

    # 🔄 重用戰情室的頁籤邏輯
    # 取得資料
    try:
        df_k = get_kline_data(stock_code, "D")
    except:
        df_k = None

    try:
        df_fa = get_fa_ren(stock_code)
    except:
        df_fa = None

    try:
        df_rev_raw = get_monthly_revenue(stock_code)
        df_rev = df_rev_raw.copy()
        df_rev["year"] = df_rev["日期"].dt.year
        df_rev["month"] = df_rev["日期"].dt.month
        df_rev.rename(columns={"營收": "revenue"}, inplace=True)
        df_rev = df_rev.sort_values(["year", "month"], ascending=[False, False])
    except:
        df_rev = None

    try:
        df_margin = get_margin_trading(stock_code)
    except:
        df_margin = None

    # 建立三個頁籤 (直接重用戰情室的內容)
    return html.Div([
        html.Hr(style={"margin": "30px 0"}),
        html.H4(f"📊 {get_stock_name(stock_code)} ({stock_code}) 詳細資訊"),

        dcc.Tabs(
            id="filter-detail-tabs",
            value="tab-tech",
            children=[
                dcc.Tab(label="技術面", value="tab-tech"),
                dcc.Tab(label="財務面", value="tab-revenue"),
                dcc.Tab(label="籌碼面", value="tab-chips")
            ]
        ),

        html.Div(
            id="filter-detail-tabs-content",
            children=build_chart(df_k, stock_code, "D") if df_k is not None else html.Div("無K線資料"),
            style={"marginTop": "20px"}
        )
    ])


# 4. 處理下方頁籤切換 (重用戰情室邏輯)
@app.callback(
    Output("filter-detail-tabs-content", "children"),
    Input("filter-detail-tabs", "value"),
    State("filter-result-datatable", "selected_rows"),
    State("filter-result-datatable", "data"),
    prevent_initial_call=True
)
def update_filter_detail_tabs(selected_tab, selected_rows, table_data):
    """切換頁籤時更新內容 (直接複製 update_tab_content 的邏輯)"""
    if not selected_rows or not table_data:
        return dash.no_update

    stock_code = table_data[selected_rows[0]]["代號"]

    # 🔄 這裡直接複製貼上 update_tab_content 的邏輯
    # ... (為節省篇幅，這裡省略，實際程式碼就是複製 update_tab_content 的內容)
    pass


# ==================================================
# 🆕 快速選股 Callbacks
# ==================================================

# 1️⃣ 切換按鈕選中狀態
@app.callback(
    Output({"type": "filter-btn", "index": dash.dependencies.ALL}, "style"),
    Input({"type": "filter-btn", "index": dash.dependencies.ALL}, "n_clicks"),
    State({"type": "filter-btn", "index": dash.dependencies.ALL}, "id"),
    prevent_initial_call=True
)
def toggle_filter_button(n_clicks_list, button_ids):
    """切換按鈕選中狀態"""
    styles = []
    for i, btn_id in enumerate(button_ids):
        condition_name = btn_id["index"]
        config = FILTER_CONDITIONS[condition_name]
        n_clicks = n_clicks_list[i] or 0

        # 奇數次點擊 = 選中
        if n_clicks % 2 == 1:
            styles.append({
                "width": "100%",
                "margin": "5px 0",
                "padding": "8px",
                "backgroundColor": config["color"],  # 🔴 選中
                "color": "white",
                "border": f"2px solid {config['color']}",
                "borderRadius": "5px",
                "cursor": "pointer",
                "fontSize": "12px",
                "fontWeight": "bold",
                "transition": "all 0.3s"
            })
        else:
            styles.append({
                "width": "100%",
                "margin": "5px 0",
                "padding": "8px",
                "backgroundColor": "white",
                "border": f"2px solid {config['color']}",
                "borderRadius": "5px",
                "cursor": "pointer",
                "fontSize": "12px",
                "transition": "all 0.3s"
            })

    return styles


# 2️⃣ 更新右上方表格
@app.callback(
    Output("filter-result-container", "children"),
    Input({"type": "filter-btn", "index": dash.dependencies.ALL}, "n_clicks"),
    State({"type": "filter-btn", "index": dash.dependencies.ALL}, "id"),
    prevent_initial_call=True
)
def update_filter_result(n_clicks_list, button_ids):
    """根據選中的條件更新結果表格"""
    # 找出選中的條件
    selected_conditions = []
    for i, btn_id in enumerate(button_ids):
        n_clicks = n_clicks_list[i] or 0
        if n_clicks % 2 == 1:
            selected_conditions.append(btn_id["index"])

    if not selected_conditions:
        return html.Div(
            "請在左側選擇篩選條件",
            style={"padding": "50px", "textAlign": "center", "color": "#999", "fontSize": "16px"}
        )

    # ✅ 加入除錯訊息
    print("\n" + "="*60)
    print(f"🔍 開始篩選，條件: {selected_conditions}")
    print("="*60)

    # 合併資料
    df_result = merge_filter_results(selected_conditions)

    print(f"\n✅ 篩選完成，共 {len(df_result)} 檔股票")
    print("="*60 + "\n")

    if df_result.empty:
        return html.Div([
            html.Div(
                f"已選擇: {', '.join(selected_conditions)}",
                style={"padding": "10px", "fontWeight": "bold", "backgroundColor": "#e8f4f8"}
            ),
            html.Div(
                "❌ 沒有符合所有條件的股票",
                style={"color": "red", "textAlign": "center", "padding": "30px"}
            )
        ])

    return html.Div([
        html.Div(
            f"✅ 已選擇: {', '.join(selected_conditions)} | 找到 {len(df_result)} 檔股票",
            style={
                "padding": "10px",
                "fontWeight": "bold",
                "backgroundColor": "#d4edda",
                "color": "#155724",
                "borderRadius": "5px 5px 0 0"
            }
        ),
        dash_table.DataTable(
            id="filter-result-datatable",
            columns=[{"name": col, "id": col} for col in df_result.columns],
            data=df_result.to_dict("records"),
            row_selectable="single",
            selected_rows=[],
            style_table={"overflowX": "auto"},
            style_cell={"textAlign": "center", "padding": "8px", "fontSize": "13px"},
            style_header={"fontWeight": "bold", "backgroundColor": "#f0f0f0"},
            page_size=10
        )
    ])


# 3️⃣ 點選表格 → 右下方顯示戰情室頁籤
@app.callback(
    Output("filter-detail-tabs-container", "children"),
    Input("filter-result-datatable", "selected_rows"),
    State("filter-result-datatable", "data"),
    prevent_initial_call=True
)
def show_stock_detail_tabs(selected_rows, table_data):
    """點選股票後，右下方顯示戰情室頁籤"""
    if not selected_rows or not table_data:
        return []

    stock_code = str(table_data[selected_rows[0]]["代號"])
    stock_name = get_stock_name(stock_code)

    return html.Div([
        html.Div(
            f"📊 {stock_name} ({stock_code})",
            style={
                "fontSize": "18px",
                "fontWeight": "bold",
                "marginBottom": "15px",
                "color": "#2c3e50"
            }
        ),

        dcc.Tabs(
            id="filter-detail-tabs",
            value="tab-tech",
            children=[
                dcc.Tab(label="技術面", value="tab-tech"),
                dcc.Tab(label="財務面", value="tab-revenue"),
                dcc.Tab(label="籌碼面", value="tab-chips")
            ]
        ),

        dcc.Loading(
            id="loading-filter-detail",
            type="circle",
            children=html.Div(
                id="filter-detail-content",
                # ⭐ 修改這裡：預設載入技術面內容
                children=build_stock_tabs_content(stock_code, "tab-tech", period="D", prefix="filter-"),
                style={"marginTop": "15px"}
            )
        )
    ])
# 4️⃣ 處理右下方頁籤切換（完全重用戰情室邏輯）
@app.callback(
    Output("filter-detail-content", "children"),
    Input("filter-detail-tabs", "value"),
    State("filter-result-datatable", "selected_rows"),
    State("filter-result-datatable", "data"),
    prevent_initial_call=True
)
def update_filter_detail_content(selected_tab, selected_rows, table_data):
    if not selected_rows or not table_data:
        return dash.no_update

    stock_code = str(table_data[selected_rows[0]]["代號"])

    # ⭐ 加上 prefix="filter-"
    content = build_stock_tabs_content(stock_code, selected_tab, period="D", prefix="filter-")

    return content

# ==================================================
# 🆕 快速選股 - EPS 視圖切換
# ==================================================
@app.callback(
    Output("filter-eps-content-container", "children"),
    Input("filter-eps-view-radio", "value"),
    State("filter-result-datatable", "selected_rows"),
    State("filter-result-datatable", "data"),
    prevent_initial_call=True
)
def update_filter_eps_view(view_type, selected_rows, table_data):
    """快速選股 - 切換 EPS 視圖"""
    if not selected_rows or not table_data or not view_type:
        return dash.no_update

    stock_code = str(table_data[selected_rows[0]]["代號"])
    return build_eps_section(stock_code, view_type=view_type, n_quarters=12)


# ==================================================
# 🆕 快速選股 - 技術面週期切換
# ==================================================
@app.callback(
    Output("filter-detail-content", "children", allow_duplicate=True),
    Input("filter-period-radio", "value"),
    State("filter-result-datatable", "selected_rows"),
    State("filter-result-datatable", "data"),
    State("filter-detail-tabs", "value"),
    prevent_initial_call=True
)
def update_filter_period(period, selected_rows, table_data, selected_tab):
    """快速選股 - 切換技術面週期"""
    if not selected_rows or not table_data or not period:
        return dash.no_update

    stock_code = str(table_data[selected_rows[0]]["代號"])

    content = build_stock_tabs_content(
        stock_code=stock_code,
        selected_tab=selected_tab,
        period=period,
        prefix="filter-"
    )

    return content
# ==================================================
# 1️⃣3️⃣ Run App
# ==================================================
if __name__ == "__main__":
    app.run(debug=True)
