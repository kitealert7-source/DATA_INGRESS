# Anti-Gravity Daily Pipeline Wrapper
# Called by invoke_preflight.ps1 when decision == RUN_DAILY
# Responsibilities: Execute master daily pipeline, propagate exit code.
# Version: 5.0 (fixed exception handling for subprocess stderr)

$ErrorActionPreference = "Continue"

# Pinned Python Interpreter (deterministic execution)
$PYTHON = "C:\Users\faraw\AppData\Local\Programs\Python\Python311\python.exe"

$DataIngress = "C:\Users\faraw\Documents\DATA_INGRESS"
$DailyPipeline = "$DataIngress\engines\ops\daily_pipeline.py"
$LogDir = "$DataIngress\logs\SCHEDULER"
$LogFile = "$LogDir\pipeline_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"

# Ensure log directory exists
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$timestamp | $Message" | Out-File -Append -FilePath $LogFile
    Write-Output $Message
}

# Set working directory
Set-Location $DataIngress

# Load Telegram credentials from user-level env (set via setx)
# alerts.py reads these at import time; must be present in THIS process before Python starts
$env:TELEGRAM_BOT_TOKEN = [System.Environment]::GetEnvironmentVariable("TELEGRAM_BOT_TOKEN", "User")
$env:TELEGRAM_CHAT_ID   = [System.Environment]::GetEnvironmentVariable("TELEGRAM_CHAT_ID",   "User")

if (-not $env:TELEGRAM_BOT_TOKEN -or -not $env:TELEGRAM_CHAT_ID) {
    Write-Log "WARNING: Telegram credentials not found in user environment - alerts will be silent"
}

Write-Log "=== DAILY PIPELINE INVOCATION ==="
Write-Log "Working Directory: $(Get-Location)"
Write-Log "Python (Pinned): $PYTHON"
Write-Log "Master Script: $DailyPipeline"

# Execute master daily pipeline with pinned Python
# Note: Using Start-Process to avoid PowerShell exception issues with stderr
$process = Start-Process -FilePath $PYTHON -ArgumentList $DailyPipeline -WorkingDirectory $DataIngress -Wait -NoNewWindow -PassThru -RedirectStandardOutput "$LogDir\stdout_temp.log" -RedirectStandardError "$LogDir\stderr_temp.log"

$exitCode = $process.ExitCode

# Append stdout to main log
if (Test-Path "$LogDir\stdout_temp.log") {
    Get-Content "$LogDir\stdout_temp.log" | ForEach-Object { Write-Log "PIPELINE: $_" }
    Remove-Item "$LogDir\stdout_temp.log" -Force
}

# Append stderr to main log (if any)
if (Test-Path "$LogDir\stderr_temp.log") {
    $stderrContent = Get-Content "$LogDir\stderr_temp.log" -Raw
    if ($stderrContent.Trim()) {
        Write-Log "STDERR: $stderrContent"
    }
    Remove-Item "$LogDir\stderr_temp.log" -Force
}

Write-Log "Daily Pipeline Exit Code: $exitCode"

if ($exitCode -eq 0) {
    Write-Log "=== DAILY PIPELINE COMPLETE (SUCCESS) ==="
}
else {
    Write-Log "=== DAILY PIPELINE FAILED (Exit: $exitCode) ==="
}

exit $exitCode
