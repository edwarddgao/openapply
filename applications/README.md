# Applications

`status.jsonl` is the application ledger. Each line records the latest observed status for a role, including submitted, blocked, needs-input, or unknown outcomes.

`codex-subagent-logs/` contains ignored runtime logs and final subagent messages from application attempts. These files can include personal details and form-fill traces, so they are intentionally not tracked.
