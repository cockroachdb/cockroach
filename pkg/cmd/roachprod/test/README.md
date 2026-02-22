# roachprod Functional Tests

This directory contains two Go packages:
* `framework` a lightweight testing framework for functional validation of roachprod through the CLI using real services.
* `tests` contains the tests that are organized loosely by function and `roachprod` command

## Why

`roachprod`'s existing unit tests cover internal logic but do not exercise roachprod commands against real cloud infrastructure. By running against real services, these tests catch failures that only surface when commands hit actual cloud provider endpoints. We also get a level of end to end code path coverage that more closely resembles real usage.

These tests also validate a baseline level of correctness without relying on `roachtest` or manual testing when making roachprod changes.

## Writing a Basic Test

A basic test scenario follows three phases: **setup**, **execute**, and **assert**. Setup
initializes a `RoachprodTest` which manages the test lifecycle (unique cluster
name, command execution, cleanup on exit). Execute runs roachprod CLI commands
via `Run`, `RunExpectSuccess`, or `RunExpectFailure`. Assert verifies cluster state using the
`Assert*` methods, which query `roachprod list --json` under the hood.

```go
func TestExample(t *testing.T) {
    // Setup: initialize test harness. Generates a unique cluster name,
    // locates the roachprod binary, and registers cleanup (cluster destroy)
    // to run automatically when the test finishes.
    // Accepts functional options: e.g. WithTimeout (command timeout),
    // WithClusterName (override generated name), WithSeed (reproducible RNG),
    // DisableCleanup (keep cluster after test), etc.
    rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

    // Execute: run roachprod CLI commands via Run (returns result),
    // RunExpectSuccess (fails test on error), or RunExpectFailure (fails
    // test on success). Arguments are passed directly to the roachprod
    // binary (e.g., "create <cluster> -n 3 --clouds gce").
    rpt.RunExpectSuccess("create", rpt.ClusterName(),
        "-n", "3",
        "--clouds", "gce",
        "--lifetime", "1h",
    )

    // Assert: verify cluster state. Assert methods fetch cluster info via
    // `roachprod list --json` (cached after the first call).
    // See assertions.go for the full list of available assertions.
    rpt.AssertClusterExists()
    rpt.AssertClusterNodeCount(3)
    rpt.AssertClusterCloud("gce")

    // Since assertions cache cluster info under the hood, if the cluster is
    // modified, the cache will need to be invalidated so the new cluster info
    // is fetched.
    rpt.RunExpectSuccess("extend", rpt.ClusterName(), "--lifetime=1h")
    rpt.InvalidateClusterCache()
    rpt.AssertClusterLifetime(2 * time.Hour)
}
```
The framework was designed to be lightweight, minimal, but also convenient. The goal is to make test writing as easy as possible.

## Randomized Testing

The framework supports randomized testing to cover the large space of roachprod configuration combinations (machine type, architecture, storage, zones, etc.). Enumerating every combination of create arguments on a regular cadence wouldn't be an efficient use of time and resources. Instead, with a combination of dedicated test cases to explicitly cover more common configurations, and randomized tests to push against edge cases to find gaps, we can guarantee a baseline level of quality while also making efforts to continuously find issues.

`RandomGCECreateOptions` generates a valid, internally consistent configuration using the test's seeded RNG.
```go
func TestExample(t *testing.T) {

    // ... setup (see example above) ...

    opts := framework.RandomGCECreateOptions(rpt.Rand())
    t.Logf("Random config (seed=%d): %s", rpt.Seed(), opts.String())
    args := opts.ToCreateArgs(rpt.ClusterName())
    rpt.RunExpectSuccess(args...)

    // ... assert ...
}
```

Seeds are logged automatically and can be reproduced with `COCKROACH_RANDOM_SEED=<seed>` or as a functional option with `NewRoachprodTest` `framework.WithSeed(<seed>)`.

## Running Locally

For gcloud setup with `roachprod` see the [roachprod tutorial](https://cockroachlabs.atlassian.net/wiki/spaces/TE/pages/144408811/Roachprod+Tutorial)


### Test Locally with bazel

```bash
# Run a single test within a bazel target
# assumes ROACHPROD_USER is set (see roachprod tutorial above)
# --test_output=streamed: output is streamed to terminal
# --nocache_test_results: (optional) force re-run even if test inputs are the same (source files, dependencies, args, etc.)
# -test.v: Go Test argument for verbose output (prints t.Log on success, default is just on failure)
# -test.run: Go Test argument for test filtering
# (optional write to file for convenience)
bazel test //pkg/cmd/roachprod/test/tests:create \
  --test_env=ROACHPROD_USER \
  --test_output=streamed \
  --nocache_test_results \
  --test_arg=-test.v \
  --test_arg=-test.run=TestCreateARM64 \
  2>&1 | tee roachprod_test.log

# Run a specific bazel test target
bazel test //pkg/cmd/roachprod/test/tests:create \
  --test_env=ROACHPROD_USER \
  --test_output=streamed \
  --nocache_test_results \
  --test_arg=-test.v \
  2>&1 | tee roachprod_test.log

# Run all tests
# Note: recommended to run this in ci to leverage bazci's artifact organization and teamcity's test reporting view
bazel test //pkg/cmd/roachprod/test/tests:all \
  --test_env=ROACHPROD_USER \
  --test_output=streamed \
  --nocache_test_results \
  --test_arg=-test.v \
  2>&1 | tee roachprod_test.log
```
If you just want to use the framework without creating any cloud resources, you can run
* `//pkg/cmd/roachprod/test/tests:version`
* `//pkg/cmd/roachprod/test/tests:list` 


### Debug mode (keep cluster after test)

```bash
ROACHPROD_SKIP_CLEANUP=true bazel test //pkg/cmd/roachprod/test/tests:create \
  --test_env=ROACHPROD_SKIP_CLEANUP \
  ...
```

## Running in CI (TeamCity)

The `build/teamcity/cockroach/nightlies/roachprod_weekly.sh` script defines the [teamcity build configuration](https://teamcity.cockroachdb.com/buildConfiguration/Cockroach_Nightlies_RoachprodFunctionalTestsWeekly)

Build runs on a weekly cadence, might be adjusted over time.

## Important Details

### Bazel

#### Why are there so many `go_test` rules?

Each `go_test` rule in `BUILD.bazel` produces a separate Bazel test target. This serves two purposes:
1. **Selective execution** -- individual tests can be run by target name (e.g., `bazel test //pkg/cmd/roachprod/test/tests:create`) without relying on `--test_filter`.
2. **Artifact isolation** -- Bazel writes test artifacts (`test.log`, `test.xml`) to a per-target directory under `bazel-testlogs/`, with the directory name matching the `go_test` rule name. `bazci` stages these artifacts to the CI artifacts directory, preserving the per-target structure.

If these tests were under a single `go_test` rule, we would only be able to filter tests based on the test name. While a good naming convention could achieve the same thing, I'd rather not rely on test names unless we have to. Also one of `bazci`'s convenience features is split artifacts and logs by test target, so by splitting we get an organized artifacts directory vs a single large hard to parse file.  

#### Test Discovery Isolation

These tests are tagged `integration` in `BUILD.bazel`, which excludes them from `./dev test`. The `./dev test` command runs the `//pkg:all_tests` suite, which filters out any target tagged `integration`. These are not unit tests.

Note: These tests are still picked up by `bazel test //pkg/...`.

### Implicitly Tested Commands

Some commands are not explicitly tested because the framework uses them internally, so any breakage would surface immediately e.g. `run`, `list`. An argument could be made for adding these for the sake of completeness.

## Randomized Create Coverage

`RandomGCECreateOptions` picks a machine type from `SupportedGCEMachineTypes`
and derives compatible settings (architecture, storage, local SSD, zones) from
`gcedb`. The machine type list is chosen to cover distinct roachprod code paths,
not to exhaustively test every GCE family.

## See Also

- [roachprod_weekly.sh](../../../../build/teamcity/cockroach/nightlies/roachprod_weekly.sh) -- CI script
- [pkg/roachprod](../../../roachprod/) -- Main roachprod package

## TODOs
* CI
  * Add Github Action support
* Coverage
  * CRDB related roachprod command coverage
  * Utility related roachprod command coverage
  * AWS, Azure, IBM coverage
* Roachprod Roadmap
  * Support Centralized Roachprod