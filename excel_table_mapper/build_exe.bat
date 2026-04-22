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
  --name TrafficAnalysisTool ^
  --collect-all openai ^
  --collect-all psycopg ^
  --collect-all socksio ^
  --collect-all tkinterdnd2 ^
  --add-data "assets\seed_word_cache.sqlite3;assets" ^
  desktop_app.py

echo.
echo 构建完成：dist\TrafficAnalysisTool.exe
pause
