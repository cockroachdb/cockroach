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
5. **Commit** as two separate commits: repro commit + fix commit.

The repro commit is "proof of work" for the reviewer — it demonstrates
the failure mechanism and may surface bigger issues to area experts. It
gets squashed before merge.

## Input

This skill accepts an argument — typically a GitHub issue URL:

```
/deflake https://github.com/cockroachdb/cockroach/issues/167535
```

It can also accept a test name or freeform description:

```
/deflake TestReplicaLatchingOptimisticEvaluationKeyLimit
```

If an issue URL is provided, fetch the issue body and comments to
extract the test name, failure logs, stack traces, and any prior
analysis.

## Workflow

**This skill is interactive.** After completing each step, STOP and
present your findings or proposed changes to the user. Wait for explicit
confirmation before moving to the next step. This gives the user a
chance to course-correct, provide additional context, or skip ahead at
every stage.

### Step 1: Understand the Flake

Gather context from the provided input (GitHub issue, test name, error
logs, stack traces).

- If given a GitHub issue URL:
  - Fetch the issue body with `gh issue view <url>`.
  - Fetch comments with `gh issue view <url> --comments` — these often
    contain prior analysis, stack traces, or hypotheses from other
    engineers that save significant investigation time.
  - Extract the test name, failure message, and any linked CI artifacts.
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

**Checkpoint:** Ask the user to confirm the briefing or provide
corrections before proceeding. This is especially important here — the
user may redirect the investigation before any code is modified.

### Step 3: Reproduce the Flake

The goal is a test modification that makes the flake happen
**deterministically or near-deterministically**. Any dirty trick is fair
game — the repro commit is squashed before merge.

**Common reproduction techniques:**

- **Inject a blocking operation** via the test's eval filter, request
  filter, or testing knobs. Hold a latch/lock/lease that creates the
  exact conflict the flake depends on.
- **Add a sleep or channel synchronization** to force the race ordering
  that triggers the bug.
- **Inject an error** at the right point using testing knobs or mock
  interfaces.
- **Reduce timeouts** to make timeout-dependent flakes fire faster.
- **Disable retries or caching** that mask the underlying issue.
- **Use `atomic.Bool` + channels** for fine-grained control over when
  injected operations block and unblock.

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

**Verify the repro works:**

Run the specific flaky subtest(s) and confirm they fail deterministically:

```bash
./dev test <package> -f='<TestName>/<subtest>' -v --timeout=45s --count=1
```

The test should fail (timeout, wrong result, panic) reliably. If it
passes, the injection isn't targeting the right thing — iterate.

**Checkpoint:** Show the user the repro results (test output, failure
mode). Confirm the repro matches the original flake before proceeding
to the fix.

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

```bash
# The test passes with the injection still active
./dev test <package> -f='<TestName>' -v --count=3
```

Run at least 3 times to confirm stability. If some injected subtests hit
the timeout fallback (by design), that's expected — the key thing is they
complete without deadlock or failure.

**Checkpoint:** Show the user the fix approach and test results. Confirm
the fix is acceptable before committing.

### Step 5: Add Diagnostic Logging (Optional)

If the flake involved a conflict check, retry path, or fallback that was
previously silent, consider adding a `log.Eventf` or trace event at the
point of failure. This makes future investigations of similar flakes
easier without requiring a reproduction first.

### Step 6: Commit as Two Commits

Structure the change as two commits on a feature branch:

**Commit 1 — Repro (to be squashed before merge):**

Contains only the injection mechanism. The original (broken) test logic
is preserved, so the test **intentionally deadlocks/fails** — proving
the repro works. This commit should NOT include the fix.

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

### Step 7: Create the PR

Create a PR with both commits. The description should explain:

- The root cause of the flake
- How the repro works (briefly)
- How the fix handles the scenario
- That the first commit is proof-of-work and should be squashed before merge

Use the `/commit-helper` skill for formatting commit messages and the PR.

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
