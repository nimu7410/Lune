@echo off
setlocal EnableExtensions
cd /d "%~dp0"

set "VENV_PYTHON=%CD%\venv\Scripts\python.exe"

if not exist "%VENV_PYTHON%" (
    echo [1/5] Creating the project virtual environment...
    where py >nul 2>nul
    if not errorlevel 1 (
        py -3.13 -m venv "%CD%\venv"
        if errorlevel 1 py -3 -m venv "%CD%\venv"
    ) else (
        python -m venv "%CD%\venv"
    )
    if errorlevel 1 goto :failed
) else (
    echo [1/5] Using the existing project virtual environment.
)

echo [2/5] Updating pip...
"%VENV_PYTHON%" -m pip install --upgrade pip
if errorlevel 1 goto :failed

echo [3/5] Installing Lune dependencies...
"%VENV_PYTHON%" -m pip install -r "%CD%\requirements.txt"
if errorlevel 1 goto :failed

echo [4/5] Installing the Chromium runtime used for broker login...
"%VENV_PYTHON%" -m playwright install chromium
if errorlevel 1 goto :failed

echo [5/5] Verifying the environment...
"%VENV_PYTHON%" -m pip check
if errorlevel 1 goto :failed

echo.
echo Lune dependencies installed successfully.
exit /b 0

:failed
echo.
echo Installation failed. Review the error shown above.
exit /b 1
