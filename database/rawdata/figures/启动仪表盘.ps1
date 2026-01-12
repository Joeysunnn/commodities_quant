# Metal Market Analytics Dashboard Launcher
# 启动仪表盘并自动打开浏览器

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Metal Market Analytics Dashboard" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Starting Streamlit server..." -ForegroundColor Green
Write-Host ""
Write-Host "The dashboard will open automatically in your browser." -ForegroundColor White
Write-Host "Press Ctrl+C to stop the server." -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 切换到脚本所在目录
Set-Location -Path $PSScriptRoot

# 等待2秒后自动打开浏览器
Start-Job -ScriptBlock {
    Start-Sleep -Seconds 3
    Start-Process "http://localhost:8501"
} | Out-Null

# 启动Streamlit
& D:\anaconda3\envs\stat3612\python.exe -m streamlit run web.py

# 保持窗口打开
Read-Host -Prompt "Press Enter to exit"
