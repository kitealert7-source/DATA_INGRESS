# Anti-Gravity Preflight Invocation Wrapper
# Called by Windows Task Scheduler (Daily 00:15 UTC)
# Responsibilities: Run preflight, capture decision, dispatch.
# Version: 3.0 (pinned Python interpreter, file-based logging)

$ErrorActionPreference = "Stop"

# Pinned Python Interpreter (deterministic execution)
$PYTHON = "C:\Users\faraw\AppData\Local\Programs\Python\Python311\python.exe"

$DataIngress = "C:\Users\faraw\Documents\DATA_INGRESS"
$PreflightScript = "$DataIngress\engines\ops\preflight_check.py"
$DailyPipeline = "$DataIngress\engines\ops\invoke_daily_pipeline.ps1"
$DecisionFile = "$DataIngress\PREFLIGHT_DECISION.json"
$LogDir = "$DataIngress\logs\SCHEDULER"
$LogFile = "$LogDir\scheduler_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"

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

Set-Location $DataIngress

Write-Log "=== PREFLIGHT AGENT INVOCATION ==="
Write-Log "Trigger: SCHEDULER"
Write-Log "Working Directory: $(Get-Location)"
Write-Log "Python (Pinned): $PYTHON"

# Step 1: Run Preflight
Write-Log "Running preflight_check.py..."
try {
    $output = & $PYTHON $PreflightScript "SCHEDULER" 2>&1
    $exitCode = $LASTEXITCODE
    Write-Log "Preflight Output: $output"
    Write-Log "Preflight Exit Code: $exitCode"
    
    if ($exitCode -ne 0) {
        Write-Log "ERROR: Preflight agent failed with exit code: $exitCode"
        exit $exitCode
    }
}
catch {
    Write-Log "EXCEPTION: Preflight execution error: $_"
    exit 1
}

# Step 2: Read Decision Token
if (-not (Test-Path $DecisionFile)) {
    Write-Log "HARD_STOP: Decision file not found: $DecisionFile"
    exit 1
}

$DecisionJson = Get-Content $DecisionFile | ConvertFrom-Json
$Decision = $DecisionJson.decision
$Reason = $DecisionJson.reason
Write-Log "Preflight Decision: $Decision"
Write-Log "Reason: $Reason"

# Step 3: Dispatch
switch ($Decision) {
    "RUN_DAILY" {
        Write-Log "Dispatching to Daily Pipeline..."
        try {
            & $DailyPipeline 2>&1 | ForEach-Object { Write-Log "PIPELINE: $_" }
            Write-Log "Daily Pipeline Exit Code: $LASTEXITCODE"
            exit $LASTEXITCODE
        }
        catch {
            Write-Log "EXCEPTION in Daily Pipeline: $_"
            exit 1
        }
    }
    "RUN_RECOVERY" {
        Write-Log "RECOVERY_REQUIRED: Gap or failure detected. Reason: $Reason"
        Write-Log "Manual investigation required - pipeline will NOT run automatically."
        try {
            & $PYTHON "$DataIngress\engines\ops\alerts.py" "RECOVERY_REQUIRED" "Preflight: $Reason - manual intervention needed. Check DATA_INGRESS logs."
        } catch {
            Write-Log "WARN: Alert dispatch failed: $_"
        }
        exit 1
    }
    "NO_ACTION" {
        Write-Log "NO_ACTION: System already up to date. Exiting cleanly."
        exit 0
    }
    "HARD_STOP" {
        Write-Log "HARD_STOP: Critical preflight failure. Reason: $Reason"
        try {
            & $PYTHON "$DataIngress\engines\ops\alerts.py" "HARD_STOP" "Preflight HARD_STOP: $Reason - pipeline aborted."
        } catch {
            Write-Log "WARN: Alert dispatch failed: $_"
        }
        exit 1
    }
    default {
        Write-Log "UNKNOWN DECISION: $Decision. Aborting."
        exit 1
    }
}
