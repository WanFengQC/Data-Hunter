@echo off
setlocal

set "ROOT_DIR=%~dp0"
set "BACKEND_DIR=%ROOT_DIR%backend"
set "VENV_ACTIVATE=%BACKEND_DIR%\.venv\Scripts\activate.bat"
set "VENV_PYTHON=%BACKEND_DIR%\.venv\Scripts\python.exe"
set "BACKEND_PORT=8000"

call :kill_port "%BACKEND_PORT%"

if not exist "%BACKEND_DIR%\app\main.py" (
  echo [ERROR] Cannot find backend\app\main.py
  echo         Script location: %ROOT_DIR%
  pause
  exit /b 1
)

if not exist "%VENV_PYTHON%" (
  echo [ERROR] venv python not found:
  echo         %VENV_PYTHON%
  echo Please create it first:
  echo   cd backend
  echo   python -m venv .venv
  echo   .venv\Scripts\python -m pip install -r requirements.txt
  pause
  exit /b 1
)

if not exist "%VENV_ACTIVATE%" (
  echo [ERROR] venv activate script not found:
  echo         %VENV_ACTIVATE%
  pause
  exit /b 1
)

cd /d "%BACKEND_DIR%"

if not exist "requirements.txt" (
  echo [ERROR] requirements.txt not found in %BACKEND_DIR%
  pause
  exit /b 1
)

call "%VENV_ACTIVATE%"

if not defined VIRTUAL_ENV (
  echo [ERROR] Failed to activate virtual environment.
  pause
  exit /b 1
)

echo.
echo Virtual env: %VIRTUAL_ENV%
python -c "import sys; print('Python:', sys.executable)"
echo.
echo Starting FastAPI server...
echo URL: http://127.0.0.1:%BACKEND_PORT%/docs
echo.

python -m uvicorn app.main:app --reload --host 0.0.0.0 --port %BACKEND_PORT%

if errorlevel 1 (
  echo.
  echo [ERROR] Server exited with failure.
  pause
  exit /b 1
)

endlocal
goto :eof

:kill_port
set "TARGET_PORT=%~1"
echo [INFO] Checking port %TARGET_PORT% ...
for /f "tokens=5" %%P in ('netstat -ano ^| findstr /R /C:":%TARGET_PORT% .*LISTENING"') do (
  if not "%%P"=="0" (
    echo [INFO] Killing PID %%P using port %TARGET_PORT% ...
    taskkill /PID %%P /F >nul 2>&1
  )
)
exit /b 0
