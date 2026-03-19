@echo off
setlocal

set "ROOT_DIR=%~dp0"
set "BACKEND_SCRIPT=%ROOT_DIR%start_backend.bat"
set "FRONTEND_SCRIPT=%ROOT_DIR%start_frontend.bat"

if not exist "%BACKEND_SCRIPT%" (
  echo [ERROR] start_backend.bat not found.
  pause
  exit /b 1
)

if not exist "%FRONTEND_SCRIPT%" (
  echo [ERROR] start_frontend.bat not found.
  pause
  exit /b 1
)

echo Starting backend and frontend in separate windows...

start "Data Hunter Backend" /d "%ROOT_DIR%" "%BACKEND_SCRIPT%"
start "Data Hunter Frontend" /d "%ROOT_DIR%" "%FRONTEND_SCRIPT%"

endlocal
