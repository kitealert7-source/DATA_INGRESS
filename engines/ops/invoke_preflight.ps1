# Anti-Gravity Preflight Invocation Wrapper
# Called by Windows Task Scheduler (Daily 05:45 + Boot).
# Responsibilities: Run preflight, capture decision, dispatch.
# Version: 4.1 (mutex guard first; early-exit guard; silent no-op once daily data is done)

# --- Concurrent-invocation guard (cross-session named mutex) ---
# MUST be the first executable code in this script. No variables, no logging,
# no filesystem access before this point — both the primary (user) and any
# shadow (SYSTEM) invocation must hit this gate identically before diverging.
# The OS releases the mutex when this PowerShell process exits.
$__createdNew = $false
try {
    $script:__PreflightMutex = [System.Threading.Mutex]::new($false, 'Global\AntiGravity_Preflight', [ref]$__createdNew)
    if (-not $script:__PreflightMutex.WaitOne(0)) {
        exit 0  # Another preflight is running — silent no-op
    }
} catch {
    # Mutex unavailable (rare) — fall through and let the pipeline lock catch any race.
}
# --- End concurrent-invocation guard ---

$ErrorActionPreference = "Stop"

# --- Early-exit guard (PowerShell-level, no Python, no logging) ---
# If today's data is already confirmed successful, exit immediately and silently.
# This ensures the login/unlock trigger is a no-op for the rest of the day.
$GovernancePath = "C:\Users\faraw\Documents\Anti_Gravity_DATA_ROOT\governance\last_successful_daily_run.json"
$TodayUTC = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd")
if (Test-Path $GovernancePath) {
    try {
        $gov = Get-Content $GovernancePath -Raw | ConvertFrom-Json
        if ($gov.last_run_date -eq $TodayUTC -and $gov.status -eq "SUCCESS") {
            exit 0  # Data already updated today — done for the day
        }
    } catch {
        # JSON unreadable — fall through to full preflight
    }
}
# --- End early-exit guard ---

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
            # Use Start-Process so we get a reliable .ExitCode regardless of
            # $ErrorActionPreference.  Piping a native command through ForEach-Object
            # in PS 5.1 with ErrorActionPreference=Stop wraps stderr as NativeCommandError
            # (terminating error) AND resets $LASTEXITCODE — both bugs avoided here.
            $ts          = Get-Date -Format "yyyyMMddHHmmss"
            $stdoutTmp   = "$LogDir\pipeline_stdout_$ts.tmp"
            $stderrTmp   = "$LogDir\pipeline_stderr_$ts.tmp"

            $proc = Start-Process -FilePath "powershell.exe" `
                -ArgumentList "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", $DailyPipeline `
                -Wait -PassThru -NoNewWindow `
                -RedirectStandardOutput $stdoutTmp `
                -RedirectStandardError  $stderrTmp

            $pipelineExit = $proc.ExitCode

            # Flush captured output into the session log
            foreach ($f in @($stdoutTmp, $stderrTmp)) {
                if (Test-Path $f) {
                    Get-Content $f | ForEach-Object { Write-Log "PIPELINE: $_" }
                    Remove-Item $f -Force -ErrorAction SilentlyContinue
                }
            }

            Write-Log "Daily Pipeline Exit Code: $pipelineExit"
            exit $pipelineExit
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
