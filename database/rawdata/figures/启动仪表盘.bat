@echo off
chcp 65001 >nul
title Metal Market Analytics Dashboard

echo ========================================
echo    Metal Market Analytics Dashboard
echo ========================================
echo.
echo Starting Streamlit server...
echo.
echo The dashboard will open automatically in your browser.
echo Press Ctrl+C to stop the server.
echo ========================================
echo.

cd /d "%~dp0"
start http://localhost:8501
D:\anaconda3\envs\stat3612\python.exe -m streamlit run web.py

pause
