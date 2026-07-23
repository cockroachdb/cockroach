# Roachtests

Roachtests are CockroachDB's integration/system tests. They spin up cloud VM
clusters (or local processes with `--local`) and run end-to-end scenarios.

## Running roachtests

The recommended way to run roachtests is via `roachstress.sh` from the
repository root:

```bash
# Run a test once (default is 10 iterations).
bash pkg/cmd/roachtest/roachstress.sh -c 1 'perturbation/full/restart'

# Run locally (uses local processes instead of cloud VMs).
# Not all tests support this — check the test implementation first.
# Many heavier tests require cloud VMs or Linux-specific features (cgroups, etc).
bash pkg/cmd/roachtest/roachstress.sh -c 1 -l 'acceptance/build-info'
```

`roachstress.sh` handles cross-compiling `cockroach`, `workload`, and
`roachtest`, caches binaries per commit SHA under `artifacts/<sha>/`, and
invokes `roachtest run` with the right flags. It is the simplest way to go
from a code change to a running test.

### Running roachtest directly

If you invoke `roachtest run` directly (e.g. during iterative development on
the test harness itself), remember that `roachtest` is a compiled Go binary.
After modifying test code, you must rebuild before running:

```bash
./dev build roachtest
```

The built binary lands in `bin/roachtest`. When not using `roachstress.sh`, you
also need pre-built `cockroach` and `workload` binaries to pass via
`--cockroach` and `--workload`.

## Test registration

Tests are registered in Go files under `tests/` via `r.Add(registry.TestSpec{...})`.
The `Name` field determines the test name used for filtering.

## Artifacts

Test artifacts are written to the `--artifacts` directory. When using
`roachstress.sh`, this is `artifacts/<sha>/<timestamp>/`, with a symlink at
`artifacts/latest` pointing to the most recent run.

The directory structure under the artifacts root:

```
<test-name>/run_<N>/
  test.log            # main test log — start here when debugging failures
  <N>.perf/           # perf data collected from node N (e.g. stats.json)
  run_*.log           # individual command logs (cockroach start, workload, etc.)
  test-post-assertions.log
  test-teardown.log
```

`test.log` is almost always the first file to look at.
