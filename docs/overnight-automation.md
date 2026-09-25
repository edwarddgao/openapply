# Overnight application automation

OpenApply runs overnight as a macOS LaunchAgent. It does not use a hosted agent
automation and does not create remote tasks. The scheduled process launches the
existing external `claude -p` application agents directly, on Sonnet 5.

## Nightly workflow

At 05:00 local time, `com.openapply.overnight`:

1. acquires a non-overlapping run lock;
2. validates disk space, applicant configuration, Chrome, the `claude` CLI, agent-browser,
   and the `gws` Gmail refresh token;
3. refreshes all public ATS data locally;
4. converts the UTC day's snapshot to Parquet;
5. builds the candidate-specific shortlist locally;
6. selects only unhandled fresh roles using the application ledger;
7. applies to at most 200 roles serially with profile-backed Chrome; and
8. writes one end-of-run summary.

The application ledger prevents repeats. Anything ever submitted is never
retried, and current `blocked` or `needs_input` roles are skipped unless an
operator explicitly requests a retry.

## Requirements

- The Mac must be plugged in, online, logged into the desktop session, and awake
  when the job starts. The service runs under `caffeinate` after launch, so it
  stays awake while processing. A sleeping Mac runs a missed calendar job after
  waking, not during the night; keep an Amphetamine session active if overnight
  timing matters.
- Chrome's Default profile must remain signed in.
- `gws auth status` must report `token_valid: true`. Repair it with:

  ```bash
  gws auth logout
  gws auth login --readonly --services gmail
  ```

- The repository virtualenv must exist at `.venv/`.

## Operations

Install or update the persistent schedule without starting a run immediately:

```bash
.venv/bin/python scripts/overnightctl.py install
```

Validate the complete refresh and shortlist path without submitting anything:

```bash
.venv/bin/python scripts/run_overnight.py --no-apply
```

Inspect the service and latest completed/failed summary:

```bash
.venv/bin/python scripts/overnightctl.py status
```

Pause future runs and safely stop a current run:

```bash
.venv/bin/python scripts/overnightctl.py stop
.venv/bin/python scripts/overnightctl.py pause
```

Resume the schedule:

```bash
.venv/bin/python scripts/overnightctl.py resume
```

Start immediately only when intentional—this can submit applications:

```bash
.venv/bin/python scripts/overnightctl.py start
```

Remove the schedule while preserving ledgers, summaries, and logs:

```bash
.venv/bin/python scripts/overnightctl.py uninstall
```

## Output and recovery

- Latest summary: `applications/overnight/latest-summary.json`
- Per-run summary: `applications/overnight/date=YYYY-MM-DD/run=*/summary.json`
- Per-run unresolved details: `applications/overnight/date=YYYY-MM-DD/run=*/unresolved.json`
- Per-run pipeline log: `applications/overnight/date=YYYY-MM-DD/run=*/pipeline.log`
- LaunchAgent output: `applications/overnight/launchd.out.log` and
  `applications/overnight/launchd.err.log`

If a run fails, the summary's `error` and failed stage identify whether the
problem was preflight, refresh, shortlist generation, or application execution.
After repairing the problem, use `overnightctl.py start`. The ledger makes the
restart idempotent: already handled roles are skipped.

The runner refuses to start when another pipeline holds its lock, another
`scripts/apply.py` process is active, fewer than 12 GiB are free, configuration
JSON is invalid, or Gmail authentication is unhealthy.
