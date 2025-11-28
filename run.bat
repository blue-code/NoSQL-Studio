@echo off
echo =====================================
echo MongoDB & Redis Query Tool - Advanced
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

echo Starting advanced version...
echo.
echo Features:
echo - Connection Profiles
echo - Query History and Favorites
echo - Data Editing (Add/Edit/Delete)
echo - Export/Import (JSON, CSV)
echo - Performance Monitoring
echo - Advanced MongoDB Tools
echo - Multiple Query Tabs
echo - And much more!
echo.

REM Activate virtual environment and run the application
call venv\Scripts\activate.bat
python db_query_tool_advanced.py

if errorlevel 1 (
    echo.
    echo Error: Application failed to start
    echo.
    echo To use the basic version, run: run_basic.bat
    pause
    exit /b 1
)
