@echo off
setlocal

set "ROOT_DIR=%~dp0"
set "FRONTEND_DIR=%ROOT_DIR%frontend"
set "FRONTEND_PORT=5173"
set "VITE_API_TARGET=http://127.0.0.1:8000"

call :kill_port "%FRONTEND_PORT%"

if not exist "%FRONTEND_DIR%\package.json" (
  echo [ERROR] Cannot find frontend\package.json
  echo         Script location: %ROOT_DIR%
  pause
  exit /b 1
)

cd /d "%FRONTEND_DIR%"

if not exist "node_modules" (
  echo [INFO] node_modules not found, running npm install...
  call npm install
  if errorlevel 1 (
    echo [ERROR] npm install failed.
    pause
    exit /b 1
  )
)

echo.
echo Starting frontend dev server...
echo URL: http://127.0.0.1:%FRONTEND_PORT%
echo API Target: %VITE_API_TARGET%
echo.

call npm run dev

if errorlevel 1 (
  echo.
  echo [ERROR] Frontend server exited with failure.
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
