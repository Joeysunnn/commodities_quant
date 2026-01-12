# 创建桌面快捷方式
# Create Desktop Shortcut for Dashboard

$DesktopPath = [Environment]::GetFolderPath("Desktop")
$ShortcutPath = Join-Path $DesktopPath "Metal Dashboard.lnk"
$TargetPath = "C:\Users\lenovo\Desktop\winter\rawdata\figures\一键启动.bat"
$IconLocation = "C:\Windows\System32\shell32.dll,13"  # 图表图标

$WScriptShell = New-Object -ComObject WScript.Shell
$Shortcut = $WScriptShell.CreateShortcut($ShortcutPath)
$Shortcut.TargetPath = $TargetPath
$Shortcut.WorkingDirectory = "C:\Users\lenovo\Desktop\winter\rawdata\figures"
$Shortcut.Description = "Metal Market Analytics Dashboard - 一键启动"
$Shortcut.IconLocation = $IconLocation
$Shortcut.Save()

Write-Host "✓ 桌面快捷方式创建成功！" -ForegroundColor Green
Write-Host ""
Write-Host "快捷方式位置：$ShortcutPath" -ForegroundColor Cyan
Write-Host ""
Write-Host "现在您可以双击桌面上的 'Metal Dashboard' 图标启动仪表盘！" -ForegroundColor Yellow
Write-Host ""
Read-Host "按回车键退出"
