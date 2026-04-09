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

This skill requires a GitHub issue URL:

```
/deflake https://github.com/cockroachdb/cockroach/issues/167535
```

The issue provides the test name, failure logs, stack traces, and prior
analysis from other engineers — all critical context for investigation.
Every flaky test should have an issue filed; if one doesn't exist, file
it first with `/file-crdb-issue`.

Use `-i` for interactive mode (pause after each step for confirmation):

```
/deflake -i https://github.com/cockroachdb/cockroach/issues/167535
```

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
hypotheses faster than guessing. This debug logging can stay in the
repro commit; the revert commit (commit 3) removes it.

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

Run the specific flaky subtest(s) and confirm they fail deterministically:

```bash
./dev test <package> -f='<TestName>/<subtest>' -v --timeout=45s --count=1
```

The test should fail (timeout, wrong result, panic) reliably. If it
passes, the injection isn't targeting the right thing — iterate.

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

```bash
# The test passes with the injection still active
./dev test <package> -f='<TestName>' -v --count=3
```

Run at least 3 times to confirm stability. If some injected subtests hit
the timeout fallback (by design), that's expected — the key thing is they
complete without deadlock or failure.

**Checkpoint (interactive mode only):** Show the user the fix approach
and test results before committing.

### Step 5: Add Diagnostic Logging (Optional)

If the flake involved a conflict check, retry path, or fallback that was
previously silent, consider adding a `log.Eventf` or trace event at the
point of failure. This makes future investigations of similar flakes
easier without requiring a reproduction first.

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

### Step 7: Create the PR

Create a PR with all three commits. The description should explain:

- The root cause of the flake
- How the repro works (briefly)
- How the fix handles the scenario
- That the commits are structured as repro → fix → revert-repro, and
  squash cleanly to just the fix

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
