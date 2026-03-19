@echo off
setlocal

set "ROOT_DIR=%~dp0"
set "FRONTEND_DIR=%ROOT_DIR%frontend"

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
echo URL: http://127.0.0.1:5173
echo.

call npm run dev

if errorlevel 1 (
  echo.
  echo [ERROR] Frontend server exited with failure.
  pause
  exit /b 1
)

endlocal
