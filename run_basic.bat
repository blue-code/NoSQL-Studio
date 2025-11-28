@echo off
echo =====================================
echo MongoDB & Redis Query Tool (Basic)
echo =====================================
echo.

REM Check if virtual environment exists
if not exist "venv" (
    echo Error: Virtual environment not found!
    echo Please run setup.bat first to set up the environment.
    echo.
    pause
    exit /b 1
)

echo Starting basic version...
echo.

REM Activate virtual environment and run the application
call venv\Scripts\activate.bat
python db_query_tool.py

if errorlevel 1 (
    echo.
    echo Error: Application failed to start
    pause
    exit /b 1
)
