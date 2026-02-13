import os

# ================= 配置區 =================
# 這裡設定要「完全無視」的資料夾
IGNORE_DIRS = {
    'venv', '.git', '__pycache__', '.idea', '.vscode',
    'build', 'dist', 'node_modules', 'env', 'migrations',
    'static', 'assets', 'images', 'tmp', 'temp', 'logs' ,'data','chromedriver-win64','logs'
}

# 這裡設定要「無視」的特定檔案
IGNORE_FILES = {
    'bundle_code.py', 'package-lock.json', 'yarn.lock', 'pnpm-lock.yaml',
    'poetry.lock', 'Pipfile.lock', '.DS_Store', 'db.sqlite3','testStocl.py','verify_fundamentals.py',
    'verify_missing_logic.py','debug_vix.py','__init__.py'
}

# 這裡設定要「無視」的副檔名 (StockWarRoom 重點：排除數據檔!)
IGNORE_EXTENSIONS = {
    '.csv', '.json', '.txt', '.log', '.xml', '.svg', '.png', '.parquet', '.jpg', '.pyc'
}

# 只讀取這些「純程式碼」
ALLOWED_EXTENSIONS = {'.py', '.js', '.jsx', '.ts', '.tsx', '.html', '.css', '.sql', '.md', '.sh'}


# =========================================

def bundle_project(output_file='stockwarroom_context.txt'):
    token_estimate = 0
    file_count = 0

    with open(output_file, 'w', encoding='utf-8') as f:
        # 寫入一個 System Header，讓 AI 知道這是什麼
        f.write("# StockWarRoom Project Context\n")
        f.write("# Generated for Gemini Context Window\n\n")

        for root, dirs, files in os.walk('.'):
            # 1. 排除資料夾 (原地修改 dirs 列表)
            dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]

            for file in files:
                # 2. 檢查檔案名稱
                if file in IGNORE_FILES: continue

                # 3. 檢查副檔名
                ext = os.path.splitext(file)[1].lower()
                if ext in IGNORE_EXTENSIONS: continue  # 顯式排除
                if ext not in ALLOWED_EXTENSIONS: continue  # 只允許白名單

                file_path = os.path.join(root, file)

                # 寫入檔案內容
                f.write(f"\n{'=' * 50}\n")
                f.write(f"FILE_PATH: {file_path}\n")
                f.write(f"{'=' * 50}\n\n")

                try:
                    with open(file_path, 'r', encoding='utf-8') as source_f:
                        content = source_f.read()
                        f.write(content)
                        # 粗略估算 Token (1 token ~= 4 chars)
                        token_estimate += len(content) / 4
                        file_count += 1
                except Exception as e:
                    print(f"Skipping {file_path}: {e}")

                f.write("\n")

    print(f"✅ 打包完成！")
    print(f"📄 檔案位置: {output_file}")
    print(f"📊 包含檔案數: {file_count}")
    print(f"🔢 預估 Token: {int(token_estimate)} (如果不超過 50,000 是最棒的)")


if __name__ == "__main__":
    bundle_project()