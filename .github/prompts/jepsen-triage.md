# Jepsen Failure Triage

This file supplements `investigate.md` with Jepsen-specific guidance.
Read it when investigating a `jepsen/*` roachtest failure.

## Key Files (in priority order)

1. **`invoke.log`** — stdout/stderr from the Jepsen JAR. The most
   informative artifact for understanding what went wrong.
2. **`jepsen.log`** — structured Jepsen test log. Only exists if setup
   succeeded and the workload started.
3. **Per-node `cockroach.stderr`** — CockroachDB process output per
   node. Useful for correlating node-level errors.

Also read `pkg/cmd/roachtest/tests/jepsen.go` for the test harness
logic, known-exception handling, and artifact collection code.

## Triage Steps

### 1. Classify the top-level error from `test.log`

- `"timed out"` at `jepsen.go` — 40-minute overall timeout hit.
  Proceed to `invoke.log` to determine which phase hung.
- `"COMMAND_PROBLEM: exit status 254"` — Jepsen exited with error
  (exit code 255, remapped). Check `invoke.log` for the cause.

### 2. Determine the failure phase from `invoke.log`

Grep for these patterns:

| Pattern | Meaning |
|---------|---------|
| `"Oh jeez, I'm sorry, Jepsen broke"` | Jepsen crashed. Read the `"Caused by"` chain on subsequent lines. |
| `"Run complete"` | Workload finished — failure is in analysis or teardown. If absent, test hung during setup or execution. |
| `"Analyzing..."` (no result after) | Analysis-phase hang, typically generating HTML timeline on a large history. |
| `PSQLException` | SQL error from CockroachDB — examine the message for the underlying cause. |
| `RuntimeException: timeout` in `setup!` | Nemesis disrupted the cluster before workers connected. |
| `ntpdate` + `"no server suitable for synchronization found"` | NTP teardown failure. |

### 3. On timeout, check the JVM thread dump

When the test times out, `invoke.log` contains a JVM thread dump
(from `pkill -QUIT java`). Look for threads stuck in:

- `clj_ssh.ssh$ssh_exec` — stuck SSH command on a remote node.
- `jepsen.checker.timeline$html` — analysis phase generating HTML.
- `CyclicBarrier.await` — threads waiting at a synchronization
  barrier; find the thread that is *not* at the barrier to
  identify what is blocking progress.

### 4. Check the consistency verdict in `jepsen.log`

- `"Everything looks good! ✓"` or `:valid? true` — consistency
  check passed.
- `"Analysis invalid!"` or `:valid? false` — **consistency
  violation detected**. This is the most critical finding.
- `:fail` and `:info` operation results — individual operation
  errors during the workload.

### 5. Check for known benign exceptions

The test harness (`jepsen.go`) auto-ignores certain exceptions.
If the error matches one of these, the failure would normally be
skipped. Cross-reference with the grep in `jepsen.go:~414`:
`BrokenBarrierException`, `InterruptedException`,
`ArrayIndexOutOfBoundsException`, `NullPointerException`,
`clj-ssh scp failure`, `RuntimeException: Connection to`.

## Failure Context

Jepsen tests go through distinct phases: setup, workload
execution, analysis, and teardown. Determining which phase failed
is more important than matching specific error messages — use the
triage steps above to identify the phase, then investigate the
root cause within that phase.

If the failure is in teardown (e.g., NTP errors), the workload
and analysis may have completed successfully — always check the
consistency verdict in `jepsen.log` before concluding the test
failed.

`PSQLException` in `invoke.log` indicates a SQL-level error from
CockroachDB. Read the full exception message and trace to
understand the cause — do not assume it matches a previously
seen pattern.

## Cross-build correlation

If multiple `jepsen/*` tests fail in the same TeamCity build,
check whether they share a common failure pattern (e.g., all
timed out in setup). This suggests a systemic issue with the
build environment rather than individual test bugs.
