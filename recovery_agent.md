# RECOVERY AGENT STANDARD (ANTI-GRAVITY CANONICAL)

This document governs ALL data recovery operations.
Authoritative for agents and humans executing recovery protocols.

---

## 0.0 PREFLIGHT AUTHORITY (MANDATORY)

Before ANY recovery action, the agent MUST:

1. Locate the authoritative Preflight Decision file.
2. Verify the decision was generated on the current UTC date.
3. Verify the decision token matches `RUN_RECOVERY`.

If the decision is missing, stale, or not `RUN_RECOVERY`:
STOP IMMEDIATELY. REFUSE EXECUTION.

Strict Adherence:
- Recovery Agent proceeds ONLY with `RUN_RECOVERY`.
- Data Agent proceeds ONLY with `RUN_DAILY`.
- No cross-role execution permitted.

---

## 1. Governance & Protocol

This agent executes the protocol defined in:
`ANTI_GRAVITY_DATA_ROOT/governance/RECOVERY.md`

The agent MUST:
1. Load `RECOVERY.md`.
2. Acknowledge the "Data Recovery & Integrity Protocol".
3. Verify strict adherence to the "Reconstruction Order" (Section 6).
4. Enforce "Prohibited Actions" (Section 11).

---

## 2. Recovery Scope

Recovery Agent is authorized to:
- Perform Incremental Backfills (bridging gaps).
- Rebuild CLEAN/RESEARCH from validated RAW.
- Patch lineage if integrity is proven.

Recovery Agent is FORBIDDEN from:
- Fabricating data.
- Ignoring governance constraints.
- Modifying `last_successful_daily_run.json` manually (automated update allowed only after full success).

---

## 3. Completion

After recovery:
1. Validate all recovered datasets (100% Pass).
2. Resume standard pipeline pre-checks.
3. Report success to User.

End of document.
