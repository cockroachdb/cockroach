---
name: deflake
description: >
  Deflake a flaky unit test by reproducing the failure deterministically first,
  then fixing once and for all. Use when asked to fix, deflake, or investigate
  a flaky test. Accepts a GitHub issue URL, test name, or description of the
  flake.
---

# Deflake: Reproduce First, Fix Once and For All

Deflake a flaky test by deterministically reproducing the failure before
attempting any fix. The reproduction proves the root cause and validates
the fix — no guessing, no "add observability and wait".

## Strategy

1. **Investigate** the root cause (read code, understand the race/timing).
2. **Brief** — output a structured summary of findings and repro plan for
   the user to review before touching code.
3. **Reproduce** the flake deterministically, by any dirty means necessary.
4. **Fix** the test (or source code) with proof that the fix handles the
   reproduced scenario.
5. **Commit** as three separate commits: repro, fix, revert of repro.

The repro commit is "proof of work" for the reviewer — it demonstrates
the failure mechanism and may surface bigger issues to area experts.
The fix commit is applied on top so the test passes. The third commit
reverts the repro injection, so the three commits squashed together
produce a clean diff with just the fix.

## Input

This skill accepts a GitHub issue URL, a test name, or a description:

```
/deflake https://github.com/cockroachdb/cockroach/issues/167535
/deflake TestFoo
```

The issue provides the test name, failure logs, stack traces, and prior
analysis from other engineers — all critical context for investigation.
Every flaky test should have an issue filed; if one doesn't exist, file
it first with `/file-crdb-issue`.

Use `-i` for interactive mode (pause after each step for confirmation):

```
/deflake -i https://github.com/cockroachdb/cockroach/issues/167535
```

Use `--gceworker` when running on a GCE worker with many cores and high
RAM. This enables stress testing with high parallelism for repro and
fix verification:

```
/deflake --gceworker https://github.com/cockroachdb/cockroach/issues/167535
```

Use `--remote` together with `--gceworker` to enable EngFlow remote
execution for stress testing. This runs test executions on remote workers,
dramatically improving throughput (e.g., 50k runs in ~8 min). Requires
valid EngFlow credentials (`engflow_auth login mesolite.cluster.engflow.com`):

```
/deflake --gceworker --remote https://github.com/cockroachdb/cockroach/issues/167535
```

**If no GitHub issue is provided**, the skill will prompt the user for
failure logs, stack traces, or error messages. These are critical for
investigation — without them, the skill must rely solely on code reading,
which is slower and less reliable. When prompting, ask:

> Do you have failure logs, stack traces, or error output from the flaky
> test? If so, please paste them here (or provide a path to a log file).
> This significantly speeds up root cause analysis.

## Workflow

**This skill runs autonomously by default** — it goes through all steps
without pausing for confirmation. This works well for most flakes that
can be one-shotted.

If the user passes `-i` (e.g., `/deflake -i <issue>`), run in
**interactive mode**: after completing each step, STOP and present
findings or proposed changes. Wait for explicit confirmation before
moving to the next step.

### Step 1: Understand the Flake

Gather context from the provided input (GitHub issue, test name, error
logs, stack traces).

- If given a GitHub issue URL:
  - Fetch the issue body with `gh issue view <url>`.
  - Fetch comments with `gh issue view <url> --comments` — these often
    contain prior analysis, stack traces, or hypotheses from other
    engineers that save significant investigation time.
  - Extract the test name, failure message, and any linked CI artifacts.
  - **Download CI artifacts**: Look for EngFlow invocation URLs
    (`mesolite.cluster.engflow.com/invocations/...`) in the issue body
    and comments. If found, use the `/engflow-artifacts` skill to
    download test logs:
    1. Extract the invocation ID from the URL.
    2. Run `targets` to find the failed target.
    3. Run `list` to find the shard with artifacts.
    4. Run `download` to get `test.log` and `outputs.zip`.
    5. Read the downloaded logs for stack traces, goroutine dumps,
       race detector output, and error messages.
  - If no EngFlow URLs are found in the issue, check if the issue
    contains inline logs or stack traces and use those.
- If given only a test name or description (no GitHub issue):
  - **Prompt the user for logs**: Ask if they have failure logs, stack
    traces, or error output they can paste or point to. These are the
    single most valuable input for diagnosis.
  - If the user provides logs, analyze them for the failure message,
    goroutine dumps, race detector output, or timeout information.
  - If the user provides an EngFlow URL, use the `/engflow-artifacts`
    skill to download the artifacts.
  - If the user has no logs, proceed with code reading alone but note
    that investigation may take longer.
- Read the failing test and the code it exercises.
- Identify the timing/race/ordering assumption that makes it flaky.
- Look for related fixes in git history (`git log --oneline --all --grep`
  on the test name) — similar flakes in sibling tests often share root
  causes.
- Check if the issue links to CI artifacts, traces, or prior analysis.

### Step 2: Present an Investigation Briefing

**STOP and output a structured briefing before doing any code changes.**
This is a checkpoint — the user should be able to course-correct before
you start modifying test code.

Output the following sections using this template:

```
## Deflake Briefing: <TestName>

### What the test does
<1-3 sentences: what behavior the test exercises, what subsystem it
covers, and what it's trying to prove. Include the key abstractions
involved (e.g., "optimistic evaluation", "closed timestamps",
"lease transfers").>

### How it works
<Brief description of the test mechanics: how it sets up state, what
operations it runs concurrently, what synchronization it uses (channels,
filters, atomics), and what assertions it makes. Focus on the moving
parts — a reviewer unfamiliar with the test should be able to follow
the failure explanation below.>

### What's failing
<The specific failure mode: timeout, wrong result, panic, deadlock.
Quote the error message or stack trace if available. Name the specific
subtest(s) that fail if known.>

### Root cause hypothesis
<Your best understanding of WHY it flakes. Describe the race or timing
assumption: what ordering does the test assume, and what real-world
event (GC, intent resolution, lease transfer, etc.) violates it? Be
specific about the code path — name functions, line numbers, and the
chain of events.>

### What we know vs. don't know
- **Known:** <facts established from code reading, issue context, CI
  artifacts, git history>
- **Unknown:** <open questions, things that need verification, alternate
  hypotheses not yet ruled out>

### Related precedent
<Any similar flakes fixed before — same test, sibling tests, or same
failure pattern in other tests. Include PR links if found. Note what
approach worked.>

### Repro strategy
<Concrete plan for how to force the flake. Which injection technique,
where to hook in, what to block, and why this should trigger the exact
failure mode. If there are multiple hypotheses, state which one this
strategy targets and what you'd try next if it doesn't work.>
```

**Checkpoint (interactive mode only):** Ask the user to confirm the
briefing or provide corrections before proceeding.

### Step 3: Reproduce the Flake

The goal is a test modification that makes the flake happen
**deterministically or near-deterministically**. Any dirty trick is fair
game — the repro commit is squashed before merge.

**This is often an iterative loop:** form a hypothesis, add
instrumentation or logging to confirm it, run the test, analyze the
output, and refine if the hypothesis was wrong or ambiguous. Add
`t.Log`, `log.Infof`, or `fmt.Fprintf(os.Stderr, ...)` liberally in
the code paths you suspect — observability helps confirm or disprove
hypotheses faster than guessing. Throwaway debug logging can stay in
the repro commit; the revert commit (commit 3) removes it.

**Add permanent diagnostic logging while you're here.** If the flake
involved a conflict check, retry path, or fallback that was previously
silent, add a `log.Eventf` or trace event at the point of failure.
Unlike throwaway `t.Log` calls, these survive the revert commit and
ship with the fix. This makes future investigations of similar flakes
faster without requiring a reproduction first. Do this now — it's much
easier to identify the right logging points while you're actively
tracing the code paths.

**Common reproduction techniques:**

- **Inject a blocking operation** via the test's eval filter, request
  filter, or existing testing knobs. Hold a latch/lock/lease that
  creates the exact conflict the flake depends on.
- **Add a sleep or channel synchronization** to force the race ordering
  that triggers the bug.
- **Inject an error** at the right point using testing knobs or mock
  interfaces.
- **Reduce timeouts** to make timeout-dependent flakes fire faster.
- **Disable retries or caching** that mask the underlying issue.
- **Use `atomic.Bool` + channels** for fine-grained control over when
  injected operations block and unblock.
- **Use global variables and dirty hacks freely.** The repro commit is
  squashed — don't waste time adding proper testing knobs or plumbing
  through clean interfaces. Package-level `var` declarations, direct
  access to unexported fields via test files, and other shortcuts are
  all fine. Do NOT add new testing knobs to production code for the
  repro.

**Injection pattern (common for latch/lock conflicts):**

```go
// Add state for the injected operation.
var injectConflict atomic.Bool
injectBlockCh := make(chan struct{}, 1)
injectBlockedCh := make(chan struct{}, 1)

// In the eval/request filter:
if shouldInject(req) && injectConflict.Load() {
    injectBlockedCh <- struct{}{} // signal: injection is holding the resource
    <-injectBlockCh               // block until test says to release
    return nil
}

// In the test body:
injectConflict.Store(true)
go func() {
    // Send the operation that will be caught by the filter
    _, pErr := tc.SendWrapped(&injectedReq)
    injectErrCh <- pErr
}()
<-injectBlockedCh // wait for injection to take hold
// Now the resource is held — proceed with the operation that flakes
```

**Add detailed comments to the test:**

As part of the repro commit, add comprehensive comments to the test that
explain everything a newcomer to the codebase would need to understand.
These comments should survive the squash and remain in the final fix
commit. Include:

- **Function-level doc comment**: What the test is asserting, what
  subsystem/abstraction it exercises, and how the mechanism works at a
  high level. Explain the relevant architecture (e.g., how latches work,
  how optimistic evaluation works, what a closed timestamp is) — don't
  assume the reader knows any of this. Use a step-by-step numbered
  explanation of the code path under test.
- **What failed and why (flake history)**: Describe the exact failure
  mode, the chain of events that causes it, and link to the issue(s).
  This goes in the function doc comment so future readers don't have to
  re-investigate.
- **Inline comments on test mechanics**: Explain the channels, atomics,
  filters, and synchronization — what each piece controls and why it's
  needed. Label major phases (e.g., "Step 1: block the write",
  "Step 2: send the read").
- **Test case comments**: For table-driven tests, explain the reasoning
  behind each case — why a particular input leads to `interferes=true`
  vs. `false`, what the narrowed spans look like for each limit value,
  etc.
- **Assertion comments**: Explain what each assertion proves. If the
  assertion is indirect (e.g., "the fact that this doesn't deadlock
  proves the optimistic path was taken"), say so explicitly.

The goal: a reader with zero context should be able to read the test
comments alone and fully understand what's being tested, how the
mechanism works, and what exactly went wrong.

**Verify the repro works:**

Run the specific flaky subtest(s) and confirm they fail deterministically.

**Without `--gceworker`** (default — local machine):

```bash
./dev test --stress <package> --filter "^<TestName>$" -v --timeout=45s
```

**With `--gceworker`** (GCE worker — stress with high parallelism).
Determine the machine's CPU count and available RAM first, then run:

```bash
P=100  # adjust to machine's CPU count
HOST_RAM=$(free -g | awk '/^Mem:/{print $2}')
./dev test --stress <package> --filter "^<TestName>$" \
  -- --jobs $P \
  --local_resources=cpu=$P \
  --local_resources=memory=${HOST_RAM}
```

**With `--gceworker --remote`** (GCE worker + EngFlow remote execution).
Tests execute on remote EngFlow workers, enabling massive parallelism:

```bash
P=100  # adjust based on desired parallelism
./dev test --stress <package> --filter "^<TestName>$" \
  -- --config=engflow --jobs $P
```

Add `--race` if the flake is a data race. The stress run hammers the
test with many parallel instances — if the repro injection is correct,
failures should appear quickly.

The test should fail (timeout, wrong result, panic) reliably. If it
passes, the injection isn't targeting the right thing — iterate.

**If the repro doesn't work after reasonable attempts** (2-3 different
hypotheses tried), don't keep guessing. Instead, pivot to an
**observability-only PR** that makes the flake self-diagnosing the next
time it fires in CI:

1. **Add targeted vmodule logging** to the code paths involved in your
   hypotheses. Use `log.VEventf(ctx, 2, ...)` or `log.VInfof(ctx, 2, ...)`
   so the logging is silent by default but activates with
   `--vmodule=<file>=2`. This keeps the logging cheap in production.
2. **Scope the logging to the test.** Prefer adding logging that is
   useful within the specific test's code paths rather than blanketing
   an entire subsystem. A few well-placed log lines at decision points
   (e.g., "took retry path because X", "conflict detected with txn Y",
   "timed out waiting for Z after Nms") are far more useful than
   verbose logging everywhere.
3. **Set vmodule in the test itself.** Add a `log.SetVModule` call at
   the top of the test (or use the `--test_env` flag) so that when the
   flake fires in CI, the relevant vmodule logs are captured
   automatically in the test output without anyone needing to
   re-trigger with special flags.
4. **Add a `t.Logf` breadcrumb** at key synchronization points in the
   test (channel sends/receives, goroutine launches, assertion checks)
   so the test output shows the ordering of events when it fails.
5. **Keep the PR small.** The goal is a well-contained change — just
   the logging additions, no refactoring. Include a clear description
   of what each log line answers and what hypotheses remain open.
6. **Skip the test** using `/skip-test-with-issue` if it's failing
   frequently enough to block CI. Document your hypotheses and what
   the new logging is designed to answer in the issue.

Use `/commit-helper` to create a single commit with the observability
improvements. This is a legitimate and valuable outcome — it turns an
opaque flake into one that explains itself next time.

**Checkpoint (interactive mode only):** Show the user the repro results
and confirm before proceeding to the fix.

### Step 4: Fix the Flake

With the root cause proven by the repro, apply the minimal fix. Common
fix patterns:

- **Timeout-based deadlock avoidance**: Replace a blocking `<-ch` with a
  `select` that has a timeout fallback, unblocking the dependency chain
  when optimistic assumptions fail.
- **Retry/fallback tolerance**: Accept that the operation under test may
  take a different (still correct) code path, and handle both outcomes.
- **Remove the timing assumption**: Restructure the test so correctness
  doesn't depend on operation ordering.
- **Fix the source code**: If the test exposed a real bug in production
  code, fix that instead of weakening the test.

**After applying the fix**, verify:

**Without `--gceworker`** (default — local machine):

```bash
# The test passes with the injection still active
./dev test --stress <package> --filter "^<TestName>$" -v
```

**With `--gceworker`** (GCE worker — stress with high parallelism):

```bash
P=100  # adjust to machine's CPU count
HOST_RAM=$(free -g | awk '/^Mem:/{print $2}')
./dev test --stress <package> --filter "^<TestName>$" \
  -- --jobs $P \
  --local_resources=cpu=$P \
  --local_resources=memory=${HOST_RAM}
```

**With `--gceworker --remote`** (GCE worker + EngFlow remote execution):

```bash
P=100  # adjust based on desired parallelism
./dev test --stress <package> --filter "^<TestName>$" \
  -- --config=engflow --jobs $P
```

Add `--race` if the original flake was a data race. Stress for several
minutes to confirm stability.

In both cases, if some injected subtests hit the timeout fallback (by
design), that's expected — the key thing is they complete without
deadlock or failure.

**Checkpoint (interactive mode only):** Show the user the fix approach
and test results before committing.

### Step 5: Evaluate Fix Quality

Before committing, critically evaluate whether the fix is robust or just
masking the problem. Ask:

1. **Does the fix address the root cause?** A good fix eliminates the
   race, timing assumption, or incorrect invariant. A bad fix adds a
   `time.Sleep`, swallows an error, or loosens an assertion to make the
   symptom go away.
2. **Would this fix survive a code review from an area expert?** If the
   fix requires deep knowledge you don't have, or touches production code
   in ways you're not confident about, it may be too risky.
3. **Is the fix testable?** The repro injection should prove the fix
   works. If you can't demonstrate that the fix handles the reproduced
   scenario, it's suspect.
4. **Is it a test-only fix or a production code fix?** If the root cause
   is in production code but you're only patching the test, that's a red
   flag — the real bug is still there.

**If the fix is too hacky, speculative, or just covering the symptom**,
do NOT commit a questionable fix. Instead:

1. **Add observability**: Add `log.Eventf`, trace events, or detailed
   `t.Logf` statements at the key code paths involved in the flake. This
   makes the next investigation (by you or a human) significantly faster.
2. **Skip the test with an issue**: Use the `/skip-test-with-issue` skill
   to disable the test with a tracking issue. Include your investigation
   findings (root cause hypothesis, repro strategy, what you tried) in
   the issue so the next person doesn't start from scratch.
3. **Commit as two commits**: one for the observability additions, one
   for the skip. The observability commit should be a clean, permanent
   improvement — not throwaway debug logging.

This is the honest outcome when a flake is too complex to fix
confidently in one session. Shipping observability + skip is far better
than shipping a bad fix that hides the problem or introduces new issues.

### Step 6: Commit as Three Commits

Structure the change as three commits on a feature branch. The three
commits squashed together should produce a clean diff — just the fix,
no repro scaffolding.

**Commit 1 — Repro (squashed before merge):**

Contains the injection mechanism and detailed comments explaining the
test, the architecture it exercises, and the flake mechanism. The
original (broken) test logic is preserved, so the test **intentionally
deadlocks/fails** — proving the repro works. This commit should NOT
include the fix.

```
<package>: repro flake in <TestName>

<Explain the injection mechanism and what it simulates.>

This commit intentionally <deadlocks/fails> the test to demonstrate
the repro. The next commit fixes the test.

Epic: none
Informs: #<issue>
Release note: None
```

**Commit 2 — Fix:**

Contains the actual fix (timeout handling, source code fix, etc.) plus
any diagnostic logging. Applied on top of the repro commit, so the test
now passes with the injection active.

```
<package>: fix flake in <TestName>

<Explain the root cause and the fix.>

Fixes: #<issue>
Release note: None
```

**Commit 3 — Revert repro:**

Reverts the injection from commit 1, leaving only the clean fix. This
commit should be mechanical — remove the injected variables, channels,
filter additions, and any dirty hacks. The detailed test comments
should be kept if they improve readability.

```
<package>: revert repro injection for <TestName>

Remove the injection mechanism from the repro commit now that the fix
is in place. The three commits squash cleanly to just the fix.

Epic: none
Informs: #<issue>
Release note: None
```

### Step 7: Commit and Stop

Use the `/commit-helper` skill to create the three commits with properly
formatted messages and release notes.

After committing, **stop**. Do not run `gh pr create` or create a PR.
Present a summary of what was done and let the user decide when and how
to create the PR.

## Reference: Example PR

See cockroachdb/cockroach#166996 for a complete example of this strategy
applied to a rangefeed test flake.

## When NOT to Use This Skill

- **Stress-reproducible flakes**: If `--count=100` or `--stress` reproduces
  it reliably, a targeted injection may not be needed — but the fix
  should still be proven against the reproduction.
- **Flakes in roachtests or integration tests**: The feedback loop is too
  slow. Consider `/reduce-unoptimized-query-oracle` or
  `/skip-test-with-issue` instead.
- **Flakes where the root cause is already obvious**: Skip straight to
  the fix, but still verify with multiple runs.
