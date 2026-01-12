# Metal Market Analytics Dashboard - Quick Launcher
# 一键启动脚本（PowerShell版本）

$Host.UI.RawUI.WindowTitle = "Metal Market Analytics Dashboard"

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║    Metal Market Analytics Dashboard - 一键启动         ║" -ForegroundColor Yellow
Write-Host "╚════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# 切换到脚本所在目录
Set-Location -Path $PSScriptRoot

Write-Host "[1/3] 正在检查端口占用..." -ForegroundColor White
$portInUse = Get-NetTCPConnection -LocalPort 8501 -ErrorAction SilentlyContinue
if ($portInUse) {
    Write-Host "⚠️  端口8501已被占用，正在尝试关闭旧进程..." -ForegroundColor Yellow
    $portInUse | ForEach-Object {
        Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
}
Write-Host "✓ 端口检查完成" -ForegroundColor Green
Write-Host ""

Write-Host "[2/3] 正在启动Streamlit服务器..." -ForegroundColor White
Write-Host "     服务器地址: http://localhost:8501" -ForegroundColor Cyan
Write-Host ""

# 启动Streamlit服务器（在后台）
$job = Start-Job -ScriptBlock {
    param($pythonPath, $workDir)
    Set-Location $workDir
    & $pythonPath -m streamlit run web.py --server.headless=true
} -ArgumentList "D:\anaconda3\envs\stat3612\python.exe", $PSScriptRoot

Write-Host "[3/3] 等待服务器启动..." -ForegroundColor White
Start-Sleep -Seconds 5

# 测试服务器是否启动成功
$maxRetries = 10
$retryCount = 0
$serverReady = $false

while ($retryCount -lt $maxRetries -and -not $serverReady) {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8501" -TimeoutSec 1 -ErrorAction Stop
        $serverReady = $true
    } catch {
        $retryCount++
        Start-Sleep -Seconds 1
        Write-Host "." -NoNewline
    }
}

Write-Host ""

if ($serverReady) {
    Write-Host "✓ 服务器启动成功！" -ForegroundColor Green
    Write-Host "✓ 正在打开浏览器..." -ForegroundColor Green
    Start-Process "http://localhost:8501"
    
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host " ✓ 启动完成！" -ForegroundColor Green
    Write-Host " " -ForegroundColor White
    Write-Host " 仪表盘已在浏览器中打开" -ForegroundColor White
    Write-Host " 如需停止服务器，请按 Ctrl+C 或关闭此窗口" -ForegroundColor Yellow
    Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host ""
    
    # 显示实时日志
    Write-Host "正在监控服务器状态..." -ForegroundColor White
    Write-Host "(按 Ctrl+C 停止)" -ForegroundColor Gray
    Write-Host ""
    
    # 等待作业完成或用户中断
    try {
        Wait-Job -Job $job | Out-Null
    } catch {
        Write-Host "正在停止服务器..." -ForegroundColor Yellow
    } finally {
        Stop-Job -Job $job -ErrorAction SilentlyContinue
        Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
    }
    
} else {
    Write-Host "✗ 服务器启动失败，请检查错误信息" -ForegroundColor Red
    Receive-Job -Job $job
    Stop-Job -Job $job -ErrorAction SilentlyContinue
    Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
}

Write-Host ""
Write-Host "按任意键退出..." -ForegroundColor Gray
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
