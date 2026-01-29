# 請替換 update_chips_revenue.py 最下方的 main 函式

def main():
    p = Path(__file__).resolve().parent.parent / "data" / "temp" / "chips_revenue_raw.csv"
    p.parent.mkdir(parents=True, exist_ok=True)

    if PROXIES:
        print(f"🔒 使用 Proxy 模式: {list(PROXIES.keys())}")

    # 執行所有抓取函式
    rev = fetch_revenue()
    chips = fetch_chips_matrix()
    margin = fetch_margin_matrix()
    val = fetch_valuation()
    eps = fetch_eps_data()

    print("\n🔄 數據大合體...")
    base = rev if not rev.empty else chips

    # [CRITICAL Fix] 列表 join 不支援 lsuffix/rsuffix，且欄位名稱已唯一，直接移除後綴參數
    final = base.join([chips, margin, val, eps], how='left').fillna(0)

    if 'sid' not in final.columns:
        final = final.reset_index()

    final.to_csv(p, index=False, encoding='utf-8-sig')
    print(f"\n✨ V12.5 戰情室數據就緒！\n位置: {p}")

    # 🔍 [雙重驗證] 同時檢查 2330 (上市) 與 8299 (上櫃)
    check_list = ['2330', '8299']
    print(f"\n📊 數據抽樣檢查 (上市 vs 上櫃):")
    print("-" * 60)
    for sid in check_list:
        if sid in final['sid'].values:
            row = final[final['sid'] == sid].iloc[0]
            # 取出數值並排版
            rev_yoy = row.get('rev_yoy', 0)
            rev_cum = row.get('rev_cum_yoy', 0)
            eps_val = row.get('eps_q', 0)

            print(f"🔹 {sid}:")
            print(f"   - 營收年增 (rev_yoy)     : {rev_yoy:>8.2f} %")
            print(f"   - 累營年增 (rev_cum_yoy) : {rev_cum:>8.2f} %  <-- 確認這裡是否有值")
            print(f"   - 最新 EPS (eps_q)       : {eps_val:>8.2f} 元")
            print("-" * 60)
        else:
            print(f"⚠️ 找不到 {sid} 的資料，請確認是否為交易日或 API 回傳異常。")


if __name__ == "__main__": main()