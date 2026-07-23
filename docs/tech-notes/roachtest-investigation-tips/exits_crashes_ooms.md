# Investigating Node Exits, Crashes, and OOMs

When investigating a node reported as exiting unexpectedly, look for messages, stack traces, and
exit codes, either logged by the node itself as it crashed or by the system if it killed it.

## Common Exit Scenarios

- **Panic/log.Fatal**: Check `logs/N.unredacted/cockroach.stderr.log` for stack traces and fatal
  messages.
- **Disk Stall**: Look for "disk stall detected: unable to sync log files" in stderr/main logs.

## Investigation Steps

1. Check `logs/N.unredacted/cockroach.exit.log` files for exit codes (only nodes that crashed will
   have these).
2. Search stderr logs: `grep -n "panic\|fatal\|abort" logs/*/unredacted/cockroach.stderr.log`.
3. Check for disk stalls: `grep "disk stall detected" logs/*/unredacted/cockroach.log`.
4. Look for oomkiller messages in `N.dmesg.txt` files, and messages about cockroach in
   `artifacts/*.journalctl.txt`.

## Timeline and Context Analysis

Before diving into crash details, establish the timeline context:

**Timing Analysis:**
- When did crashes occur relative to test start and test failure?
- Did the test fail immediately after crashes or continue running?
- For tests that ran significantly longer after crashes, the crash may not be the direct cause.

**Test Type Context:**
- **Node shutdown tests**: Crashes are often expected behavior - focus on whether the test
  properly handled the shutdown.
- **Chaos tests**: Some crashes may be intentionally induced - check if the crash matches the
  chaos scenario.
- **Performance tests**: Crashes may indicate resource exhaustion under load.
- **Restore/backup tests**: Crashes during long-running operations may indicate coordination
  issues.

**Check for Intentional Node Stops:**
Many tests intentionally stop nodes as part of their test scenario. Before investigating a crash
as a problem:
- Search the test log for messages about stopping/killing nodes (e.g., "stopping node", "killing
  node", "shutdown").
- Look for test framework calls that intentionally terminate nodes.
- Check if the crash timing aligns with expected test operations.

**Failure Mode Assessment:**
- **Immediate failures after crash**: Crash is likely the direct cause.
- **Timeout failures with early crashes**: Investigate what prevented the test from completing
  or failing fast.
- **Connection errors to crashed nodes**: Expected after crash, but investigate what was trying
  to connect and why.

## Exit Code Reference

Check `artifacts/test.log` and the crashed node's stderr for exit codes:

- **Exit code 7**: Disk stall detected - look for "disk stall detected: unable to sync log
  files".
- **Exit code 10**: Disk full - check disk space and storage errors.
- **Exit code 132**: Illegal instruction (SIGILL) - hardware/build issue.
- **Exit code 134**: Assertion failure (SIGABRT) - internal consistency check failed.
- **Exit code 137**: SIGKILL - process was forcibly terminated (OOM killer, manual kill, or
  system limits).
- **Exit code 139**: Segmentation fault (SIGSEGV) - look for stack traces in stderr.

## Investigation Priority

When multiple issues are present, investigate in this order:

1. **Direct test failure mechanism first** - timeout, assertion failure, etc.
2. **Timeline correlation** - do crashes correlate with the direct failure?
3. **Crash details** - only after understanding the failure context.
