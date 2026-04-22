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

# Load Telegram credentials — try user-scope env first, then fall back to .secrets/telegram.env.
# Task Scheduler sessions sometimes don't inherit user-level env vars even with Interactive logon.
# The .secrets/ file is the authoritative fallback and is never committed (see .gitignore).
$env:TELEGRAM_BOT_TOKEN = [System.Environment]::GetEnvironmentVariable("TELEGRAM_BOT_TOKEN", "User")
$env:TELEGRAM_CHAT_ID   = [System.Environment]::GetEnvironmentVariable("TELEGRAM_CHAT_ID",   "User")

if (-not $env:TELEGRAM_BOT_TOKEN -or -not $env:TELEGRAM_CHAT_ID) {
    $TelegramSecretsFile = "$DataIngress\.secrets\telegram.env"
    if (Test-Path $TelegramSecretsFile) {
        Get-Content $TelegramSecretsFile | Where-Object { $_ -match '^\s*([^#=\s][^=]*)=(.+)$' } | ForEach-Object {
            $k = $Matches[1].Trim(); $v = $Matches[2].Trim()
            if ($k -eq "TELEGRAM_BOT_TOKEN") { $env:TELEGRAM_BOT_TOKEN = $v }
            if ($k -eq "TELEGRAM_CHAT_ID")   { $env:TELEGRAM_CHAT_ID   = $v }
        }
        Write-Log "Telegram credentials loaded from .secrets/telegram.env"
    }
}

if (-not $env:TELEGRAM_BOT_TOKEN -or -not $env:TELEGRAM_CHAT_ID) {
    Write-Log "WARNING: Telegram credentials not found in user environment or .secrets/telegram.env - alerts will be silent"
}

Write-Log "=== DAILY PIPELINE INVOCATION ==="
Write-Log "Working Directory: $(Get-Location)"
Write-Log "Python (Pinned): $PYTHON"
Write-Log "Master Script: $DailyPipeline"

# Execute master daily pipeline with pinned Python
# Note: Using Start-Process to avoid PowerShell exception issues with stderr
$stdoutTmp = "$LogDir\stdout_temp_$PID.log"
$stderrTmp = "$LogDir\stderr_temp_$PID.log"

$process = Start-Process -FilePath $PYTHON -ArgumentList $DailyPipeline -WorkingDirectory $DataIngress -Wait -NoNewWindow -PassThru -RedirectStandardOutput $stdoutTmp -RedirectStandardError $stderrTmp

$exitCode = $process.ExitCode

# Append stdout to main log
if (Test-Path $stdoutTmp) {
    Get-Content $stdoutTmp | ForEach-Object { Write-Log "PIPELINE: $_" }
    Remove-Item $stdoutTmp -Force
}

# Append stderr to main log (if any)
if (Test-Path $stderrTmp) {
    $stderrContent = Get-Content $stderrTmp -Raw
    if ($stderrContent.Trim()) {
        Write-Log "STDERR: $stderrContent"
    }
    Remove-Item $stderrTmp -Force
}

Write-Log "Daily Pipeline Exit Code: $exitCode"

if ($exitCode -eq 0) {
    Write-Log "=== DAILY PIPELINE COMPLETE (SUCCESS) ==="
}
else {
    Write-Log "=== DAILY PIPELINE FAILED (Exit: $exitCode) ==="
}

exit $exitCode
