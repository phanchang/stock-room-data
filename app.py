import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output

# --- 1. 設定基礎網址 ---
# 請將這裡換成你的 GitHub 帳號與專案名
GITHUB_BASE_URL = "https://raw.githubusercontent.com/phanchang/stock-room-data/main/data/cache"


def load_data_from_github(symbol):
    """根據代號判斷市場並從 GitHub 讀取 Parquet"""
    market = 'tw' if ('.TW' in symbol or '.TWO' in symbol) else 'us'
    safe_name = symbol.replace('.', '_')
    url = f"{GITHUB_BASE_URL}/{market}/{safe_name}.parquet"

    try:
        print(f"正在從雲端讀取: {url}")
        # pandas 可以直接讀取網址
        df = pd.read_parquet(url)
        return df
    except Exception as e:
        print(f"讀取雲端資料失敗 ({symbol}): {e}")
        return None


# --- 2. 測試 Dash App ---
app = Dash(__name__)

app.layout = html.Div([
    html.H1("股票戰情室 - 雲端資料版"),
    dcc.Dropdown(
        id='stock-selector',
        options=[{'label': '鴻海', 'value': '2317.TW'}],  # 先用你剛抓成功的這支測試
        value='2317.TW'
    ),
    dcc.Graph(id='stock-chart')
])


@app.callback(
    Output('stock-chart', 'figure'),
    Input('stock-selector', 'value')
)
def update_chart(selected_symbol):
    df = load_data_from_github(selected_symbol)

    if df is None or df.empty:
        return go.Figure().update_layout(title="無資料")

    # 這裡放你原本的 5000 行繪圖邏輯
    fig = go.Figure(data=[go.Candlestick(
        x=df.index,
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close']
    )])

    fig.update_layout(title=f"{selected_symbol} 股價圖 (資料來源: GitHub)")
    return fig


if __name__ == '__main__':
    app.run(debug=True)  # 新版寫法