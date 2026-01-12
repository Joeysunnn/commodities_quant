@echo off
chcp 65001 >nul
title Metal Market Analytics Dashboard - Quick Launcher

echo.
echo ╔════════════════════════════════════════════════════════╗
echo ║    Metal Market Analytics Dashboard - 一键启动         ║
echo ╚════════════════════════════════════════════════════════╝
echo.
echo [1/3] 正在检查端口占用...
netstat -ano | findstr :8501 >nul
if %errorlevel%==0 (
    echo ⚠️  端口8501已被占用，正在尝试关闭旧进程...
    for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8501 ^| findstr LISTENING') do (
        taskkill /F /PID %%a >nul 2>&1
    )
    timeout /t 2 >nul
)

echo ✓ 端口检查完成
echo.
echo [2/3] 正在启动Streamlit服务器...
echo     服务器地址: http://localhost:8501
echo.

cd /d "%~dp0"

REM 在后台启动Streamlit，等待3秒后打开浏览器
start /B "" D:\anaconda3\envs\stat3612\python.exe -m streamlit run web.py --server.headless=true

echo [3/3] 等待服务器启动...
timeout /t 5 >nul

echo ✓ 正在打开浏览器...
start http://localhost:8501

echo.
echo ═══════════════════════════════════════════════════════
echo  ✓ 启动完成！
echo  
echo  仪表盘已在浏览器中打开
echo  如需停止服务器，请关闭此窗口
echo ═══════════════════════════════════════════════════════
echo.

REM 保持窗口打开
D:\anaconda3\envs\stat3612\python.exe -m streamlit run web.py

