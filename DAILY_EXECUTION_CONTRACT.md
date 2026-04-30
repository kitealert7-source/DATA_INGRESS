# Anti-Gravity Daily Execution Contract & Scheduler Spec

**Authorization**: Anti-Gravity Daily Execution Orchestrator
**Date**: 2026-01-29
**Last Updated**: 2026-04-30
**Status**: ENFORCED

---

## 1. Execution Contract (Plain English)

### The Preflight Rule
No agent shall execute any data modification or ingestion without first obtaining a valid, same-day **Preflight Decision Token**. This token is the sole authority for execution.

### The Trigger Model
- **Primary**: A dedicated OS scheduler task invokes the Preflight Agent daily at **00:15 UTC**.
- **Secondary (Startup)**: An At Startup trigger (2-minute delay) invokes the Preflight Agent on every machine boot. This closes the gap when the machine is powered off at 00:15 UTC. The preflight idempotency gate (`NO_ACTION`) prevents double-runs if the daily trigger already fired.

Both triggers call the same `invoke_preflight.ps1` entry point. No code distinction between triggers — the preflight gate handles all cases.

### The Routing Logic
The Preflight Agent evaluates system state and emits exactly one token:
1. `RUN_DAILY` → Authorizes standard daily pipeline
2. `RUN_RECOVERY` → Alerts operator (Telegram + log), exits 1. Manual investigation required — pipeline does NOT run automatically.
3. `NO_ACTION` → System already up to date. Exits cleanly (code 0).
4. `HARD_STOP` → Alerts operator (Telegram + log), exits 1. Critical failure requiring human intervention.

### Failure Semantics
- **No Retries**: Failed runs require human diagnosis.
- **No Loops**: Each trigger fires once per event (boot or daily schedule).
- **State Preservation**: Failures leave the system in a safe, non-corrupt state.
- **RUN_RECOVERY is not automatic**: It signals a gap or failure requiring manual investigation — the pipeline will not attempt self-repair.
- **Phase 1.5 is a hard-fail gate**: `assert_raw_coverage.py` exits 1 if any (sym, tf) tuple in `EXPECTED_COVERAGE` is missing or stale beyond threshold. The pipeline halts before Phase 2; governance is not updated. This converts "Phase 1 didn't crash" into "every expected dataset is fresh."

---

## 2. Scheduler Configuration (Windows Task Scheduler)

**Task Name**: `AntiGravity_Daily_Preflight`

### Trigger 1 — Daily Schedule
- **Type**: Daily
- **Time**: 05:45 local (= 00:15 UTC, PKT UTC+5)
- **Recur**: Every 1 day

### Trigger 2 — At Startup (added 2026-03-27)
- **Type**: At startup
- **Delay**: PT2M (2 minutes — allows network and MT5 to initialise)
- **Purpose**: Catches up missed runs when machine was off at 00:15 UTC

### Trigger 3 — Windows AAR shadow (involuntary, mitigated 2026-04-22)
Windows Task Scheduler may spawn a shadow instance of this task under
`\Agent Activation Runtime\AntiGravity_Daily_Preflight` running as
`NT AUTHORITY\SYSTEM`, in parallel with the primary instance. This is a
side effect of the `InteractiveToken + WakeToRun=true + BootTrigger` combination.
It cannot be disabled at the task level without storing a password on the
account (blocked on accounts that sign in via Windows Hello PIN only).

**Mitigation**: a cross-session named mutex `Global\AntiGravity_Preflight` is
acquired as the first executable line of `invoke_preflight.ps1`. Any second
invocation (user or SYSTEM) fails `WaitOne(0)` and exits silently with code 0
before reading governance state or launching Python.

### Action
- **Program**: `powershell.exe`
- **Arguments**: `-ExecutionPolicy Bypass -File C:\Users\faraw\Documents\DATA_INGRESS\engines\ops\invoke_preflight.ps1`
- **Start in**: `C:\Users\faraw\Documents\DATA_INGRESS\`

### Settings
- **Allow task to be run on demand**: CHECKED
- **Stop the task if it runs longer than**: 1 Hour
- **If the task is already running**: Do not start a new instance
- **Start the task as soon as possible after a scheduled start is missed**: CHECKED (`StartWhenAvailable=True`, applied 2026-04-28 via UAC — closes the silent-skip gap when the machine is on but the 00:15 UTC window was narrowly missed)

---

## 3. Idempotency Guarantee

Idempotency is enforced by **four independent layers** (see §6 for per-layer guarantees). The OS scheduler's `MultipleInstancesPolicy=IgnoreNew` is no longer treated as authoritative — Windows bypassed it on 2026-04-22 when two triggers raced within ~100 ms (primary + AAR shadow). The layers in §6 each independently prevent double-execution.

The preflight gate is fully idempotent. Running it multiple times on the same day is safe:

| Scenario | Outcome |
|----------|---------|
| Machine on at 00:15 → daily trigger fires | `RUN_DAILY` → pipeline runs |
| Machine boots later in the day, daily trigger already ran | `NO_ACTION` → exits cleanly |
| Machine boots, daily trigger never ran (was off at 00:15) | `RUN_DAILY` → pipeline runs, catches up all missed bars |
| Machine on, pipeline already succeeded today | `NO_ACTION` → exits cleanly, governance file is current |
| Two triggers fire within ~100ms (primary + AAR shadow) | Mutex rejects second; silent exit code 0 before preflight runs |

---

## 4. Alerting

`invoke_preflight.ps1` calls `engines/ops/alerts.py` (Telegram) on:
- `RUN_RECOVERY` → event type `RECOVERY_REQUIRED`
- `HARD_STOP` → event type `HARD_STOP`

`daily_pipeline.py` sends alerts on:
- Any phase failure → event type `PIPELINE_FAILED`
- Successful completion → event type `PIPELINE_COMPLETE`
- Phase 2.5 anomaly → event type `MISSING_RATE_ANOMALY`

All alerts are silent no-ops if `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` are not set.

---

## 5. Post-Setup Validation Checklist

- [x] **Daily trigger**: Fires at 05:45 local (00:15 UTC)
- [x] **Startup trigger**: Added 2026-03-27, PT2M delay, confirmed in task XML
- [x] **Idempotency**: `NO_ACTION` confirmed when pipeline already ran today
- [x] **RUN_RECOVERY alerting**: Exits 1, sends Telegram alert (was silent no-op prior to 2026-03-27)
- [x] **HARD_STOP alerting**: Exits 1, sends Telegram alert
- [ ] **Boot recovery test**: On next reboot, confirm preflight log appears ~2 min after boot with `RUN_DAILY` or `NO_ACTION`
- [x] **AAR shadow defense**: `Global\AntiGravity_Preflight` mutex in `invoke_preflight.ps1` (added 2026-04-22 after duplicate-trigger incident)
- [x] **Lock hardening**: Atomic `O_EXCL` acquisition with PID + `create_time` identity in `daily_pipeline.py` (2026-04-22; verified clean run 2026-04-23)

---

## 6. Concurrency Model (added 2026-04-23)

Daily execution is single-instance-per-day by design. Four independent layers enforce this:

| # | Layer | Location | Mechanism |
|---|-------|----------|-----------|
| 1 | Preflight early-exit | `invoke_preflight.ps1` (after mutex) | Reads `last_successful_daily_run.json`; exits 0 if `last_run_date == today_utc && status == SUCCESS` |
| 2 | Preflight mutex | `invoke_preflight.ps1` (first executable line) | `Global\AntiGravity_Preflight` named mutex; `WaitOne(0)` — non-blocking, cross-session (user + SYSTEM) |
| 3 | Pipeline lock | `daily_pipeline.py` | `state/daily_pipeline.lock` acquired via `O_CREAT\|O_EXCL\|O_WRONLY`; PID + process `create_time` identity (1 ms tolerance); `fsync` durability; `atexit` release |
| 4 | Governance file | `Anti_Gravity_DATA_ROOT/governance/last_successful_daily_run.json` | Single-writer, rewritten only after all phases pass |

The Windows Task Scheduler's own `MultipleInstancesPolicy=IgnoreNew` is **not** in this list — it fails open when triggers race within its sampling window, and Windows fires an involuntary AAR-shadow copy of `InteractiveToken` + `WakeToRun` tasks as `NT AUTHORITY\SYSTEM`. See §2 Trigger 3.

### Lock guarantees (Layer 3 detail)

- **Acquisition**: `os.open(lock_path, O_CREAT | O_EXCL | O_WRONLY)` — atomic at the OS level; racers receive `FileExistsError` with no partial state.
- **Content**: `{"pid": <int>, "create_time": <float>}` written and fsynced before the handle is closed.
- **Liveness check on existing lock**:
  - If `psutil.Process(pid).create_time()` raises (process gone) *or* differs from the recorded value by more than 1 ms → lock is stale; the incoming process overwrites it and proceeds.
  - Otherwise → the holder is genuinely alive; the incoming process exits 1 and alerts (`PIPELINE_FAILED`).
- **PID reuse**: rejected by the 1 ms `create_time` tolerance — a reused PID on a rebooted machine will have a different creation timestamp.
- **Release**: `atexit` handler deletes the lock on clean exit, abnormal exit, or signal.
- **No retry loop**: `O_EXCL` is atomic, so `FileExistsError` is terminal — a second attempt would race the same way.

### Mutex guarantees (Layer 2 detail)

- **Placement**: first executable statement of `invoke_preflight.ps1`. No variables, no logging, no filesystem access before the gate. Both primary and shadow invocations hit it identically.
- **Scope**: `Global\` prefix — visible across logon sessions, including the SYSTEM-owned AAR shadow.
- **Release**: held for the life of the owning `powershell.exe` process; released by the OS when the process exits (normal, error, or kill).
- **Failure mode**: if mutex instantiation itself throws (e.g. SID lacks `SeCreateGlobalPrivilege`), the script falls through to Layer 3 — no behavior divergence versus a machine without the mutex.

---

**End of Contract**
