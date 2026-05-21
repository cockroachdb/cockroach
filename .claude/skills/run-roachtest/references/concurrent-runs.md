# Concurrent runs: is the in-progress test past its build phase?

Read this when Step 2b.1 of `SKILL.md` shows another `roachstress` is
already running on the worker and you need to decide whether it's safe
to switch branches / fetch / reset the checkout.

`roachstress.sh` runs `./dev build cockroach-short --cross=linux`,
`./dev build workload --cross=linux`, and `./dev build roachtest`
synchronously **before** invoking the roachtest binary. While any of
those builds are running, the working tree must stay on the original
SHA — switching branches, fetching, or resetting will corrupt the
in-progress build.

Once a roachstress process is past its build phase, the binaries are
cached at `artifacts/<that-sha>/{cockroach,workload,roachtest}` and
the working tree no longer matters to it. Switching to a new branch
is then safe — your new run will land in `artifacts/<new-sha>/` and
trigger its own builds in isolation.

## The check (single SSH round-trip)

```bash
roachdev worker ssh -- '
  cd ~/go/src/github.com/cockroachdb/cockroach &&
  sha=$(git rev-parse --short HEAD) &&
  echo SHA=$sha;
  echo ===dev_build===;
  pgrep -af "dev build" | grep -v pgrep;
  echo ===binaries===;
  ls -la artifacts/$sha/{cockroach,cockroach-short,workload,roachtest} 2>&1;
  echo ===test.log===;
  find artifacts/$sha -maxdepth 5 -name test.log -exec ls -la {} \;
'
```

Build phase is **done** when ALL of:

- No `./dev build` processes (dev_build section is empty).
- `artifacts/<sha>/cockroach` (or `cockroach-short`) AND `workload`
  AND `roachtest` all exist.
- A `test.log` exists for at least one in-progress run (proves
  roachstress reached `roachtest run`, which only happens after the
  build copies in `roachstress.sh:152`).

The bazel daemon (`pgrep -af bazel` showing `A-server.jar` /
`bazel-remote`) is a long-lived server, **not** an active build —
ignore it.

## If build is done

Safe to `git fetch && git checkout <branch> && git reset --hard
origin/<branch>`. Proceed to Step 2b.3.

## If build is in progress

Back off and retry. Use a 5-minute sleep loop on the worker so it's a
single long-poll, not a chatty re-SSH:

```bash
roachdev worker ssh -- '
  cd ~/go/src/github.com/cockroachdb/cockroach &&
  while true; do
    sha=$(git rev-parse --short HEAD)
    if ! pgrep -f "dev build" >/dev/null \
       && [ -f artifacts/$sha/cockroach -o -f artifacts/$sha/cockroach-short ] \
       && [ -f artifacts/$sha/workload ] \
       && [ -f artifacts/$sha/roachtest ] \
       && find artifacts/$sha -maxdepth 5 -name test.log | grep -q .; then
      echo BUILD_DONE_AT=$(date -Iseconds)
      break
    fi
    sleep 300
  done
'
```

Run this with `run_in_background: true` and a generous timeout (e.g.
1h — builds rarely take that long). Claude gets notified when it
returns. Then proceed to Step 2b.2.

If the user is waiting and the in-progress build is expected to take
> ~30 min, tell them — they may prefer to use a worktree (last-resort
fallback in Step 2b.2) rather than block.
