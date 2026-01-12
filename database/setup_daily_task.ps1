# Windows Task Scheduler Setup Script
# Daily Data Update Tasks Configuration
# - daily_auto_all.py: Run at 09:30 daily
# - lme_auto_update.py: Run at 10:00, 12:00, 14:00, 16:00, 18:00, 20:00 daily

# Check for administrator privileges
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Red
    Write-Host "  [ERROR] Administrator privileges required!" -ForegroundColor Red
    Write-Host "============================================" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please run PowerShell as Administrator:" -ForegroundColor Yellow
    Write-Host "  1. Right-click PowerShell icon" -ForegroundColor White
    Write-Host "  2. Select 'Run as Administrator'" -ForegroundColor White
    Write-Host "  3. Run this script again" -ForegroundColor White
    Write-Host ""
    pause
    exit 1
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Daily Data Update Tasks Setup" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Configure paths
$ScriptDir = "D:\UserFiles\Desktop\commodities\database"
$DailyScript = Join-Path $ScriptDir "daily_auto_all.py"
$LMEScript = Join-Path $ScriptDir "LME\lme_auto_update.py"
$LMEWorkDir = Join-Path $ScriptDir "LME"

# Check if scripts exist
Write-Host "[INFO] Checking script files..." -ForegroundColor Yellow

if (-not (Test-Path $DailyScript)) {
    Write-Host "[ERROR] Cannot find daily_auto_all.py" -ForegroundColor Red
    Write-Host "   Path: $DailyScript" -ForegroundColor Yellow
    exit 1
}
Write-Host "  [OK] $DailyScript" -ForegroundColor Green

if (-not (Test-Path $LMEScript)) {
    Write-Host "[ERROR] Cannot find lme_auto_update.py" -ForegroundColor Red
    Write-Host "   Path: $LMEScript" -ForegroundColor Yellow
    exit 1
}
Write-Host "  [OK] $LMEScript" -ForegroundColor Green

# Find Python executable
Write-Host ""
Write-Host "[INFO] Searching for Python..." -ForegroundColor Yellow

$PythonPaths = @(
    "D:\anaconda3\envs\stat3612\python.exe",
    "D:\anaconda3\python.exe",
    "C:\Python312\python.exe",
    "C:\Python311\python.exe",
    "C:\Python310\python.exe",
    (Get-Command python -ErrorAction SilentlyContinue).Source
)

$PythonExe = $null
foreach ($path in $PythonPaths) {
    if ($path -and (Test-Path $path)) {
        $PythonExe = $path
        break
    }
}

if (-not $PythonExe) {
    Write-Host "[ERROR] Cannot find Python" -ForegroundColor Red
    Write-Host "   Please install Python and add to PATH" -ForegroundColor Yellow
    exit 1
}

Write-Host "[OK] Python: $PythonExe" -ForegroundColor Green

# Get current user
$CurrentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name

# Common task settings
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)

# ======================================================================
# Task 1: DailyMetalsDataUpdate (Daily at 09:30)
# ======================================================================

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Task 1: Daily Data Update (09:30)" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

$Task1Name = "DailyMetalsDataUpdate"
$Task1Desc = "Daily metals data update - COMEX, GLD, SLV, LBMA, Price Data, SHEX"

# Check and remove old task
$ExistingTask1 = Get-ScheduledTask -TaskName $Task1Name -ErrorAction SilentlyContinue
if ($ExistingTask1) {
    Write-Host "[INFO] Removing old task: $Task1Name" -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $Task1Name -Confirm:$false
}

# Create trigger and action
$Trigger1 = New-ScheduledTaskTrigger -Daily -At "09:30"
$Action1 = New-ScheduledTaskAction `
    -Execute $PythonExe `
    -Argument "`"$DailyScript`"" `
    -WorkingDirectory $ScriptDir

try {
    Register-ScheduledTask `
        -TaskName $Task1Name `
        -Description $Task1Desc `
        -Trigger $Trigger1 `
        -Action $Action1 `
        -Settings $Settings `
        -User $CurrentUser `
        -RunLevel Highest `
        -Force | Out-Null
    
    Write-Host "[OK] Task created: $Task1Name" -ForegroundColor Green
    Write-Host "     Schedule: Daily at 09:30" -ForegroundColor White
    Write-Host "     Script: $DailyScript" -ForegroundColor White
} catch {
    Write-Host "[ERROR] Failed to create task: $Task1Name" -ForegroundColor Red
    Write-Host "   $_" -ForegroundColor Yellow
}

# ======================================================================
# Task 2: LME_DataUpdate (Daily at 10:00, 12:00, 14:00, 16:00, 18:00, 20:00)
# ======================================================================

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Task 2: LME Data Update (6 times daily)" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

$Task2Name = "LME_DataUpdate"
$Task2Desc = "LME copper inventory update - 6 times daily at 10:00, 12:00, 14:00, 16:00, 18:00, 20:00"

# Check and remove old task
$ExistingTask2 = Get-ScheduledTask -TaskName $Task2Name -ErrorAction SilentlyContinue
if ($ExistingTask2) {
    Write-Host "[INFO] Removing old task: $Task2Name" -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $Task2Name -Confirm:$false
}

# Create multiple triggers (10:00, 12:00, 14:00, 16:00, 18:00, 20:00)
$LMETriggers = @(
    (New-ScheduledTaskTrigger -Daily -At "10:00"),
    (New-ScheduledTaskTrigger -Daily -At "12:00"),
    (New-ScheduledTaskTrigger -Daily -At "14:00"),
    (New-ScheduledTaskTrigger -Daily -At "16:00"),
    (New-ScheduledTaskTrigger -Daily -At "18:00"),
    (New-ScheduledTaskTrigger -Daily -At "20:00")
)

$Action2 = New-ScheduledTaskAction `
    -Execute $PythonExe `
    -Argument "`"$LMEScript`" --headless" `
    -WorkingDirectory $LMEWorkDir

try {
    Register-ScheduledTask `
        -TaskName $Task2Name `
        -Description $Task2Desc `
        -Trigger $LMETriggers `
        -Action $Action2 `
        -Settings $Settings `
        -User $CurrentUser `
        -RunLevel Highest `
        -Force | Out-Null
    
    Write-Host "[OK] Task created: $Task2Name" -ForegroundColor Green
    Write-Host "     Schedule: Daily at 10:00, 12:00, 14:00, 16:00, 18:00, 20:00" -ForegroundColor White
    Write-Host "     Script: $LMEScript" -ForegroundColor White
} catch {
    Write-Host "[ERROR] Failed to create task: $Task2Name" -ForegroundColor Red
    Write-Host "   $_" -ForegroundColor Yellow
}

# ======================================================================
# Summary
# ======================================================================

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host "  [SUCCESS] Tasks configured!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host ""
Write-Host "Created scheduled tasks:" -ForegroundColor Cyan
Write-Host "  1. $Task1Name" -ForegroundColor White
Write-Host "     - Schedule: Daily at 09:30" -ForegroundColor Gray
Write-Host "     - Script: daily_auto_all.py" -ForegroundColor Gray
Write-Host ""
Write-Host "  2. $Task2Name" -ForegroundColor White
Write-Host "     - Schedule: Daily at 10:00, 12:00, 14:00, 16:00, 18:00, 20:00" -ForegroundColor Gray
Write-Host "     - Script: LME/lme_auto_update.py" -ForegroundColor Gray
Write-Host ""
Write-Host "Management commands:" -ForegroundColor Cyan
Write-Host "  - View tasks: taskschd.msc" -ForegroundColor Yellow
Write-Host "  - Run now: Start-ScheduledTask -TaskName 'TaskName'" -ForegroundColor Yellow
Write-Host "  - Disable: Disable-ScheduledTask -TaskName 'TaskName'" -ForegroundColor Yellow
Write-Host "  - Delete: Unregister-ScheduledTask -TaskName 'TaskName'" -ForegroundColor Yellow
Write-Host ""
Write-Host "Log files:" -ForegroundColor Cyan
Write-Host "  - $ScriptDir\daily_auto_all.log" -ForegroundColor Yellow
Write-Host ""

# Ask for test run
Write-Host "Test run now? (Y/N): " -NoNewline -ForegroundColor Cyan
$testRun = Read-Host

if ($testRun -eq 'Y' -or $testRun -eq 'y') {
    Write-Host ""
    Write-Host "Select task to test:" -ForegroundColor Cyan
    Write-Host "  1. DailyMetalsDataUpdate (daily_auto_all.py)" -ForegroundColor White
    Write-Host "  2. LME_DataUpdate (lme_auto_update.py)" -ForegroundColor White
    Write-Host "  3. Run both" -ForegroundColor White
    Write-Host ""
    Write-Host "Enter (1/2/3): " -NoNewline -ForegroundColor Cyan
    $choice = Read-Host
    
    if ($choice -eq '1' -or $choice -eq '3') {
        Write-Host "[INFO] Starting DailyMetalsDataUpdate..." -ForegroundColor Yellow
        Start-ScheduledTask -TaskName $Task1Name
        Write-Host "[OK] Task started" -ForegroundColor Green
    }
    
    if ($choice -eq '2' -or $choice -eq '3') {
        Write-Host "[INFO] Starting LME_DataUpdate..." -ForegroundColor Yellow
        Start-ScheduledTask -TaskName $Task2Name
        Write-Host "[OK] Task started" -ForegroundColor Green
    }
    
    Write-Host ""
    Write-Host "Check Task Scheduler for results" -ForegroundColor Yellow
}

Write-Host ""
pause
