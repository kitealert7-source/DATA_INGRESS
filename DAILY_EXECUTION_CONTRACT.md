# Anti-Gravity Daily Execution Contract & Scheduler Spec

**Authorization**: Anti-Gravity Daily Execution Orchestrator
**Date**: 2026-01-29
**Last Updated**: 2026-03-27
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

### Action
- **Program**: `powershell.exe`
- **Arguments**: `-ExecutionPolicy Bypass -File C:\Users\faraw\Documents\DATA_INGRESS\engines\ops\invoke_preflight.ps1`
- **Start in**: `C:\Users\faraw\Documents\DATA_INGRESS\`

### Settings
- **Allow task to be run on demand**: CHECKED
- **Stop the task if it runs longer than**: 1 Hour
- **If the task is already running**: Do not start a new instance

---

## 3. Idempotency Guarantee

The preflight gate is fully idempotent. Running it multiple times on the same day is safe:

| Scenario | Outcome |
|----------|---------|
| Machine on at 00:15 → daily trigger fires | `RUN_DAILY` → pipeline runs |
| Machine boots later in the day, daily trigger already ran | `NO_ACTION` → exits cleanly |
| Machine boots, daily trigger never ran (was off at 00:15) | `RUN_DAILY` → pipeline runs, catches up all missed bars |
| Machine on, pipeline already succeeded today | `NO_ACTION` → exits cleanly, governance file is current |

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

---

**End of Contract**
