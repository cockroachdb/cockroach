---
name: run-roachtest
description: Run a single CockroachDB roachtest end-to-end: pick local vs. user's GCE worker, launch detached on worker via tmux + `roachstress.sh` with long-poll done-notification and tail. Use whenever user asks to run/stress/kick off a roachtest, or just modified one and next step is running it. Single test + single iteration only; nightly loops belong elsewhere.
---

# run-roachtest

Run a CockroachDB roachtest from end to end. The skill handles three
things that are easy to get wrong:

1. **Where** to run (local vs. the user's GCE worker).
2. **How** to launch on the worker so the run survives a closed laptop
   lid and Claude stays informed when it finishes.
3. **How** to give the user a no-friction way to peek at progress.

Requires the `roachdev worker` CLI configured (provisions and addresses
the user's GCE worker without a name argument).

## When to use this skill

- The user says "run X", "kick off X", "stress X", "try X on the
  worker", "run this test", etc., where X is a roachtest.
- The user just wrote or modified a roachtest and the next move is to
  run it.

Do NOT use this skill for:
- Multi-test sweeps or nightly-style loops (different shape — wrap
  many invocations).
- Builds/unit tests (use `./dev test`).
- Investigating an existing failed roachtest run (use the teamcity /
  engflow-artifacts skills).

## Step 1 — Decide local vs. remote

Pick local or remote as follows.

**The user's wording is the strongest signal.** Honor it directly:

| Phrase | Action |
|--------|--------|
| "locally", "on this machine", "on my mac" | local |
| "on the worker", "on the agent", "remotely", "on gceworker", "on the gce worker" | remote |
| Otherwise | inspect the test (below) |

When wording is silent, read the test source under
`pkg/cmd/roachtest/tests/` and consider:

- **Linux-only mechanics** — `cgroups`, `iptables`, disk stalls,
  `dmsetup`, network namespaces, sysctls. → remote.
- **Heavy clusters** — anything provisioning more than a couple of
  nodes, or specifying high `spec.CPU(...)`. Local mac will choke.
  → remote.
- **Local-mode bypass** — many heavy tests check `c.IsLocal()` and
  short-circuit to a smoke test. If present, local IS viable; warn
  the user it will be a smoke test rather than the real thing.
- **Toy/acceptance tests** — `acceptance/*` and similarly named
  tests are usually fine locally.

If after looking at the test you still cannot tell, ask the user. A
30-second clarification beats a 2-hour run on the wrong machine.

## Step 2a — Local run

Use `roachstress.sh -l` from the repo root. It builds locally and
runs in-process:

```bash
bash pkg/cmd/roachtest/roachstress.sh -c 1 -u -l '<test-name>'
```

Tee to a log so output is inspectable mid-run. Run as a background
Bash task (`run_in_background: true`). No tmux needed — local runs
inherit the user's session.

That's it for local. The rest of the skill is the remote workflow.

## Step 2b — Remote run on the user's GCE worker

`roachdev worker ssh` (and the other `roachdev worker` subcommands)
default to the current user's worker when no name is given — e.g.
`roachdev worker ssh -- 'hostname'` returns the user's worker
hostname. **Always omit the worker name** so this skill works
unmodified for every developer.

The repo on the worker lives at
`~/go/src/github.com/cockroachdb/cockroach` (standard `$GOPATH` layout
that `roachdev` provisions).

### Step 2b.1 — Pre-flight

Run these in parallel via SSH (single round-trip):

```bash
roachdev worker ssh -- '
  tmux ls 2>&1;
  echo ---;
  pgrep -af roachstress | head -10;
  echo ---;
  cd ~/go/src/github.com/cockroachdb/cockroach && git rev-parse --abbrev-ref HEAD && git rev-parse HEAD
'
```

Inspect the output:

- **Worker stopped** — `roachdev worker resume` first.
- **Existing tmux sessions** — note their names so the new session
  name does not collide.
- **Running roachstress** — another agent or the user is in the
  middle of a test. Do NOT wipe `artifacts/` or destroy the worker.
  Pick a unique session/log/signal name. **Before touching the
  checkout (branch switch, fetch, reset), confirm those tests are
  past their build phase** — see Step 2b.1.5.
- **Branch / SHA** — see "Getting your code on the worker" below.

### Step 2b.1.5 — Is the in-progress test past its build phase?

If pre-flight showed another `roachstress` already running, **before
touching the checkout** (branch switch, fetch, reset) you must
confirm its build phase is done — otherwise you corrupt its build.

See `references/concurrent-runs.md` for the check command, the
"build is done" criteria, and the long-poll wait-for-build loop.
Read it only when this case applies; the common case (no concurrent
run) skips straight to Step 2b.2.

### Step 2b.2 — Getting your code on the worker

If the test is unmodified upstream code (e.g. `acceptance/build-info`),
just use whatever branch/SHA the worker happens to be on. `roachstress.sh`
caches binaries by SHA, so reusing the existing checkout reuses cached
binaries — fast startup.

If the test depends on local changes:

1. Push the branch from the local worktree to the user's GitHub fork:
   `git push -u origin <branch>`.
2. On the worker, fetch and check out:
   ```bash
   roachdev worker ssh -- '
     cd ~/go/src/github.com/cockroachdb/cockroach &&
     git fetch origin <branch> &&
     git checkout <branch> &&
     git reset --hard origin/<branch>
   '
   ```
   This is safe even with other tests running, **provided Step 2b.1.5
   confirmed they are past the build phase**. The new SHA gets a
   fresh `artifacts/<new-sha>/` directory.

**Worktree fallback (rare).** If you cannot wait for the in-progress
build to finish, `git worktree add ../cockroach-<branch>
origin/<branch>` and run `roachstress.sh` from the worktree path.
Downside: bazel cache contention slows both builds, and the worktree
needs cleanup later.

### Step 2b.3 — Launch in detached tmux

Pick three unique names so concurrent runs do not collide:

| Slot | Suggested form |
|------|----------------|
| tmux session | `rt-<short-test-name>` |
| log file | `~/rt-<short-test-name>.log` |
| wait-for signal | `rt-<short-test-name>-done` |

For example: `rt-build-info`, `~/rt-build-info.log`,
`rt-build-info-done`.

Launch:

```bash
roachdev worker ssh -- "tmux new-session -d -s <SESSION> \"cd <REPO> && bash pkg/cmd/roachtest/roachstress.sh -c 1 -u <TEST> 2>&1 | tee <LOG>; echo EXIT=\\\$? >> <LOG>; tmux wait-for -S <SIGNAL>\""
```

Why this exact form:

- `tmux new-session -d` detaches immediately — SSH returns, the run
  keeps going. Closing the laptop is fine.
- `bash pkg/cmd/roachtest/roachstress.sh` from the repo root: the
  script demands its own working directory.
- `-u`: build the full `cockroach` binary with the DB Console UI baked
  in (instead of `cockroach-short`). This makes the cluster's admin UI
  available — useful when you want to hand the user a clickable
  http://...:26258 link for live debugging. Adds ~1-2 min to the first
  build per SHA; cached afterward.
- `2>&1 | tee <LOG>`: captures stdout + stderr into a single
  inspectable file.
- `echo EXIT=$? >> <LOG>`: records the script's exit code so the
  notification can report success/failure without re-grepping.
- `tmux wait-for -S <SIGNAL>`: signals a tmux semaphore as the very
  last thing. Step 2b.4 blocks on the matching `wait-for <SIGNAL>`.
- The `\\\$?` escaping: outer SSH consumes one layer of `\`, the outer
  shell another — three backslashes leave one literal `\$?` for tmux
  to pass through to bash so the exit-code expansion happens inside
  tmux's window, not at SSH-time.

After launch, verify the session is alive:

```bash
roachdev worker ssh -- 'tmux ls | grep <SESSION>; sleep 5; tail -30 <LOG>'
```

The early log should show `roachstress.sh` locating binaries and
starting the test.

### Step 2b.4 — Long-poll notification

Spawn a Bash task in background that blocks on the tmux semaphore.
When the run finishes, the SSH command returns, the background task
completes, and Claude gets a task-completion notification.

```bash
roachdev worker ssh -- 'tmux wait-for <SIGNAL> && echo SIGNAL_RECEIVED && tail -40 <LOG>'
```

Run with `run_in_background: true`. Set the Bash `timeout` to the
expected max duration plus a safety margin (e.g. 6h for a long
perturbation test, 10min for `acceptance/build-info`).

This is **long-poll**, not poll: the worker-side `tmux wait-for`
blocks indefinitely on a kernel-cheap semaphore wait. There is no
periodic checking — Claude simply gets one notification at the end.

When the notification arrives:
- Read the task output (it ends with the last 40 log lines + EXIT
  code).
- Summarize for the user: pass/fail, duration, and where the
  artifacts live (`artifacts/<sha>/<runid>/<test-name>/run_1/`).

### Step 2b.5 — Long-running tail (peek window)

Spawn a second background Bash task that runs `tail -F` against the
log:

```bash
roachdev worker ssh -- 'tail -F <LOG>'
```

Run with `run_in_background: true` and a long timeout. This task is
NOT meant to complete; it lets the user (and Claude) read live output
through `BashOutput` / `/tasks` without re-SSHing each time.

Tell the user the task ID so they know which background task to
peek at.

## Step 3 — Reporting back

Initial message after launch — keep it short:

```
Launched <test> on the worker.
- tmux session: <SESSION>
- log: <LOG>
- notify task: <ID>      (fires when run finishes)
- tail task:   <ID>      (live log, peek anytime)
```

When the notify task fires, post a one-paragraph result: pass/fail,
duration, key error if it failed, artifact path on the worker, and a
suggested `roachdev worker scp` command to download anything
specific the user might want.

## Pitfalls and gotchas

- **Closing SSH does not stop tmux.** `tmux new-session -d` detaches
  before SSH returns. The run is robust to lid-close, network drop,
  and Claude's bash timeout.
- **Concurrent runs share `artifacts/`.** Each `roachstress.sh`
  invocation gets its own `artifacts/<sha>/<runid>/` subdir, so
  collision is fine. But `rm -rf artifacts/` would nuke another
  agent's work — never do this without checking `pgrep -af
  roachstress`.
- **Branch switches affect anyone building from the same checkout.**
  If another test is still in its build phase, switching breaks it.
  See Step 2b.1.5 — wait for the build to finish, then switch in
  place. Worktrees are a last resort.
- **`./dev` vs `./pkg/cmd/roachtest/roachstress.sh`.** Always use
  `roachstress.sh` for roachtests — it cross-compiles `cockroach`,
  `workload`, and `roachtest` for the cluster's arch and caches by
  SHA. `./dev test` is for unit tests and will not produce the right
  binaries.

## Artifact paths

After a successful run, artifacts live at:

```
~/go/src/github.com/cockroachdb/cockroach/artifacts/<sha>/<runid>/<test-name>/run_1/
```

Useful files inside `run_1/`:

- `test.log` — main test log; start here when debugging.
- `<N>.perf/stats.json` — workload per-second tick data (node `<N>`
  is the workload node, usually the highest-numbered).
- `<N>.perf/phases.json` — perturbation baseline / perturbation /
  recovery boundaries (RFC3339Nano).
- `1.perf/stats.json` — aggregated ratio summary stats (perturbation
  tests only).
- `run_*.log` — individual command logs (cockroach start, workload
  invocations, etc).

Download with:

```bash
roachdev worker scp -- ':<path-on-worker>' /local/path
```

## Quick reference: full remote launch

```bash
# 1. Pre-flight.
roachdev worker ssh -- '
  tmux ls; echo ---;
  pgrep -af roachstress;
  echo ---;
  cd ~/go/src/github.com/cockroachdb/cockroach && git rev-parse HEAD
'

# 2. (If needed) push and fetch your branch.
git push -u origin <branch>
roachdev worker ssh -- '
  cd ~/go/src/github.com/cockroachdb/cockroach &&
  git fetch origin <branch> &&
  git reset --hard origin/<branch>
'

# 3. Launch detached.
roachdev worker ssh -- "tmux new-session -d -s rt-<name> \"cd ~/go/src/github.com/cockroachdb/cockroach && bash pkg/cmd/roachtest/roachstress.sh -c 1 -u <test> 2>&1 | tee ~/rt-<name>.log; echo EXIT=\\\$? >> ~/rt-<name>.log; tmux wait-for -S rt-<name>-done\""

# 4. Notify task (run_in_background: true).
roachdev worker ssh -- 'tmux wait-for rt-<name>-done && echo SIGNAL_RECEIVED && tail -40 ~/rt-<name>.log'

# 5. Tail task (run_in_background: true).
roachdev worker ssh -- 'tail -F ~/rt-<name>.log'
```
