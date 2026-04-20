@echo off
chcp 65001 >nul
setlocal

cd /d %~dp0

if not exist .venv (
  python -m venv .venv
)

.\.venv\Scripts\python -m pip install -r requirements-build.txt

.\.venv\Scripts\python -m PyInstaller ^
  --noconfirm ^
  --clean ^
  --onefile ^
  --windowed ^
  --paths src ^
  --name ExcelTableMapper ^
  --collect-all openai ^
  --collect-all psycopg ^
  --collect-all socksio ^
  --add-data "assets\seed_word_cache.sqlite3;assets" ^
  desktop_app.py

echo.
echo 构建完成：dist\ExcelTableMapper.exe
pause
