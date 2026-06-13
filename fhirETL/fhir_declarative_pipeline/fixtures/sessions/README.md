# Session Logs

Chronological development session summaries for the `fhir_declarative_pipeline` bundle.

Each file captures one focused work session: what was built, what broke, what
decisions were made, and what remains. Written at session end to preserve
context across conversation boundaries.

## Naming Convention

```
YYYY-MM-DD_<slug>.md
```

- Date = calendar day the session occurred
- Slug = 3–6 word kebab-case summary of the primary work
- Multiple sessions on the same day get distinct slugs

## Structure

Every session log follows a consistent template:

| Section | Purpose |
|---|---|
| **Goals** | What the session set out to accomplish |
| **Problems Discovered** | Issues found during the work |
| **Root Causes & Fixes** | Diagnosis table mapping problem → cause → resolution |
| **Work Completed** | Tables of files created/modified with details |
| **Decisions** | Design choices and their rationale |
| **Verification** | How correctness was confirmed (row counts, syntax checks, pipeline runs) |
| **Known Remaining Issues** | Numbered list of open items |
| **Next Steps** | Immediate follow-up actions |

## Index

| Date | Session | Summary |
|---|---|---|
| 2026-06-13 | [gold-yaml-completion-and-overrides](2026-06-13_gold-yaml-completion-and-overrides.md) | Completed all 20 YAML fixtures, created gold_overrides.py for location + bridge, cleaned up entity_resolution.py (741→279 lines), updated PROJECT_MEMORY.md |

## Usage

- **Start of session**: Scan recent logs to understand current project state
- **End of session**: Write a new log capturing decisions and open issues
- **Debugging**: Search logs for prior occurrences of an error or pattern
- **Onboarding**: Read chronologically to understand how the architecture evolved
