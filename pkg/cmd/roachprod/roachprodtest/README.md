# roachprodtest

A testing framework for end-to-end validation of the roachprod cluster management CLI. These tests create real cloud infrastructure to verify roachprod's cluster lifecycle operations work correctly.

## What is this?

This package provides:
- **Test framework** (`TestOptions`) for writing roachprod E2E tests
- **Cloud tests** that create real GCE clusters and validate roachprod commands
- **Assertion helpers** for verifying cluster state (zone, node count, machine type, etc.)
- **Randomized testing** utilities for testing diverse configurations

Tests execute actual `roachprod` commands (create, list, destroy, etc.) against real cloud providers to ensure the CLI works end-to-end.

## Running Tests

All tests are in a single bazel target. Use `--test_filter` to run specific subsets.

### Quick tests (no cloud resources)

```bash
# Set ROACHPROD_USER (required for all tests)
export ROACHPROD_USER=your-username

# Run help/version tests (fast, no cloud setup needed)
bazel test //pkg/cmd/roachprod/roachprodtest:roachprodtest_test \
  --test_env=ROACHPROD_USER \
  --test_filter="TestRoachprod*"
```

### Cloud tests (creates real GCE VMs)

```bash
# 1. Set up environment
gcloud auth login
gcloud config set project cockroach-ephemeral
export ROACHPROD_USER=your-username  # Required for cluster naming

# 2. Run all cloud tests
bazel test //pkg/cmd/roachprod/roachprodtest:roachprodtest_test \
  --test_env=ROACHPROD_USER \
  --test_env=GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/gcloud/application_default_credentials.json" \
  --test_filter="TestCloud*" \
  --test_timeout=1800 \
  --test_output=all

# 3. Run specific test
bazel test //pkg/cmd/roachprod/roachprodtest:roachprodtest_test \
  --test_env=ROACHPROD_USER \
  --test_filter=TestCloudCreate \
  --test_output=all

# 4. Run all tests (cloud + non-cloud)
bazel test //pkg/cmd/roachprod/roachprodtest:roachprodtest_test \
  --test_env=ROACHPROD_USER \
  --test_output=all
```

**Debug mode** (keep cluster after test):
```bash
ROACHPROD_SKIP_CLEANUP=1 bazel test //pkg/cmd/roachprod/roachprodtest:roachprodtest_test \
  --test_env=ROACHPROD_USER \
  --test_filter=TestCloudCreate
```

### In CI (TeamCity)

The `roachprod_weekly.sh` script runs cloud tests weekly:

```bash
./build/teamcity/cockroach/nightlies/roachprod_weekly.sh
```

- Filters to cloud tests: `--test_filter="TestCloud*"`
- Runs each test 3 times for flake detection
- Timeout: 30 minutes per test
- User: `teamcity`

## Writing a Test

### Basic Structure

```go
func TestCloudCreate(t *testing.T) {
    t.Parallel()
    rpt := NewRoachprodTest(t, WithTimeout(10*time.Minute))

    // Create a 3-node GCE cluster
    rpt.RunExpectSuccess("create", rpt.ClusterName(),
        "-n", "3",
        "--clouds", "gce",
        "--lifetime", "1h",
    )

    // Verify cluster state
    rpt.AssertClusterExists()
    rpt.AssertClusterNodeCount(3)
    rpt.AssertClusterCloud("gce")
}
```

### Test Framework Abstractions

#### 1. TestOptions

The main test context that manages cluster lifecycle and execution:

```go
type TestOptions struct {
    t              *testing.T
    roachprodPath  string
    clusterName    string        // Auto-generated unique name
    timeout        time.Duration
    cleanupCluster bool
    rng            *rand.Rand    // Seeded RNG for reproducibility
    seed           int64
    clusterInfoCache *types.Cluster  // Cached cluster state
}
```

**Creation:**
```go
rpt := NewRoachprodTest(t,
    WithTimeout(10*time.Minute),
    WithClusterName("my-cluster"),
    WithSeed(12345),
    DisableCleanup(),
)
```

**Cleanup:**
- Automatic via `t.Cleanup()` - cluster destroyed after test
- Skip with `DisableCleanup()` option or `ROACHPROD_SKIP_CLEANUP=1` env var

#### 2. Command Execution

Execute roachprod commands:

```go
// Run command and get result
result := rpt.Run("list", "--json")

// Require success
rpt.RunExpectSuccess("create", rpt.ClusterName(), "-n", "3", "--clouds", "gce")

// Require failure
rpt.RunExpectFailure("create", "invalid-cluster-name")

// Check result
if result.Success() {
    t.Logf("Stdout: %s", result.Stdout)
    t.Logf("Stderr: %s", result.Stderr)
}
```

#### 3. Assertions

Verify cluster state (uses cached cluster info to avoid redundant API calls):

```go
// Basic assertions
rpt.AssertClusterExists()
rpt.AssertClusterNodeCount(3)
rpt.AssertClusterCloud("gce")

// Configuration assertions
rpt.AssertClusterZone("us-east1-a")
rpt.AssertClusterMachineType("n2-standard-4")
rpt.AssertClusterArchitecture("amd64")
rpt.AssertClusterLifetime(1 * time.Hour)

// Cache management
info := rpt.GetClusterInfo()          // Returns cached or fetches
info = rpt.RefreshClusterInfo()       // Force fresh fetch
rpt.InvalidateClusterCache()          // Mark cache stale
```

**Caching:** The first assertion fetches cluster info via `roachprod list --json`. Subsequent assertions reuse the cached data. Invalidate the cache after operations that modify cluster state.

#### 4. Operations

High-level cluster operations:

```go
// Cluster lifecycle
rpt.Destroy()
result := rpt.Status()

// More operations in operations.go
```

#### 5. Randomized Testing

Generate random configurations for broader test coverage.

**Cloud Provider Support:** The `RandomizedClusterConfig` struct uses the `vm.ProviderOpts` interface, making it cloud-provider agnostic. Currently, `RandomGCECreateOptions()` generates GCE-specific configurations, but the struct can be extended to support AWS, Azure, IBM, and other providers.

```go
func TestCloudCreateRandomized(t *testing.T) {
    rpt := NewRoachprodTest(t)

    // Generate random GCE options (uses seeded RNG)
    opts := RandomGCECreateOptions(rpt.Rand())
    t.Logf("Random config (seed=%d): %s", rpt.Seed(), opts.String())

    // Create cluster with random options
    args := opts.ToCreateArgs(rpt.ClusterName())
    rpt.RunExpectSuccess(args...)

    // Verify configuration
    rpt.AssertClusterNodeCount(opts.NumNodes)

    // Type assert to GCE options for verification
    gceOpts := opts.ProviderOpts.(*gce.ProviderOpts)
    rpt.AssertClusterMachineType(gceOpts.MachineType)
}
```

**Reproducibility:** All randomization uses `rpt.Rand()` which is seeded. The seed is logged and can be reproduced with:
```bash
COCKROACH_RANDOM_SEED=12345 bazel test ...
# or
WithSeed(12345)
```

### Example: Zone Distribution Test

```go
func TestCloudCreateWithZoneCounts(t *testing.T) {
    t.Parallel()
    rpt := NewRoachprodTest(t, WithTimeout(10*time.Minute))

    // Create cluster: us-east1-a:2,us-west1-b:1 (3 total nodes)
    rpt.RunExpectSuccess("create", rpt.ClusterName(),
        "-n", "3",
        "--clouds", "gce",
        "--lifetime", "1h",
        "--gce-zones", "us-east1-a:2,us-west1-b:1",
    )

    // Verify zone distribution
    info := rpt.GetClusterInfo()
    zoneCounts := make(map[string]int)
    for _, vm := range info.VMs {
        zoneCounts[vm.Zone]++
    }

    require.Equal(t, 2, zoneCounts["us-east1-a"])
    require.Equal(t, 1, zoneCounts["us-west1-b"])
}
```

## File Organization

```
pkg/cmd/roachprod/roachprodtest/
├── testutils.go       # TestOptions, framework core
├── operations.go      # High-level operations (Destroy, Status, etc.)
├── assertions.go      # Cluster state assertions (zone, count, etc.)
├── testconfig.go      # Random config generation (RandomizedClusterConfig)
├── help_test.go       # Tests for 'roachprod --help'
├── version_test.go    # Tests for 'roachprod version'
├── create_test.go     # Tests for 'roachprod create'
├── list_test.go       # Tests for 'roachprod list'
└── BUILD.bazel        # Bazel configuration (single test target)
```

## Configuration Options

### Functional Options Pattern

```go
// Timeout for roachprod commands
WithTimeout(10 * time.Minute)

// Custom cluster name (auto-generated by default)
WithClusterName("my-cluster")

// Skip automatic cleanup
DisableCleanup()

// Specific random seed for reproducibility
WithSeed(12345)
```

### Environment Variables

- `ROACHPROD_USER` - **Required**. Username prefix for cluster names (e.g., "teamcity", "your-username")
- `ROACHPROD_BINARY` - Path to roachprod binary (auto-detected in Bazel)
- `ROACHPROD_SKIP_CLEANUP` - Skip cluster cleanup (set to any value)
- `COCKROACH_RANDOM_SEED` - Random seed for reproducible test runs

## Debugging

### Keep cluster after test failure

```bash
# Option 1: Environment variable
ROACHPROD_SKIP_CLEANUP=1 bazel test //pkg/cmd/roachprod/roachprodtest:roachprodtest_test \
  --test_env=ROACHPROD_USER \
  --test_filter=TestCloudCreate

# Option 2: In code
rpt := NewRoachprodTest(t, DisableCleanup())
```

### View detailed logs

```bash
bazel test //pkg/cmd/roachprod/roachprodtest:roachprodtest_test \
  --test_env=ROACHPROD_USER \
  --test_output=all \
  --test_arg=-test.v
```

### Reproduce randomized test

```bash
# From test logs: "Random seed: 1234567890"
COCKROACH_RANDOM_SEED=1234567890 bazel test ... --test_filter=TestCloudCreateRandomized
```

### Manual cleanup

```bash
# List all clusters
./bin/roachprod list --mine

# Destroy specific cluster
./bin/roachprod destroy <cluster-name>
```

## Best Practices

1. **Always use `t.Parallel()`** for cloud tests (they're slow and independent)
2. **Set appropriate timeouts** - cluster creation takes 5-10 minutes
3. **Use assertions** instead of manual checks - they cache cluster info
4. **Invalidate cache** after operations that modify cluster state
5. **Use random configs** to increase test coverage
6. **Log the seed** for reproducibility
7. **Tag tests properly** in BUILD.bazel (size, tags)

## Troubleshooting

### "Could not find roachprod binary"

```bash
# Build it first
./dev build roachprod

# Or set path explicitly
export ROACHPROD_BINARY=/path/to/roachprod
```

### "Failed to parse cluster info JSON"

The stdout from `roachprod list` may contain warning messages before the JSON. The framework automatically skips to the first `{` character, but if parsing still fails, check for:
- Cloud provider authentication errors in logs
- Malformed JSON output

### Test timeouts

Increase timeout for slow operations:
```go
rpt := NewRoachprodTest(t, WithTimeout(15*time.Minute))
```

Or in Bazel:
```bash
bazel test ... --test_timeout=1800  # 30 minutes in seconds
```

## See Also

- [roachprod_weekly.sh](../../../../build/teamcity/cockroach/nightlies/roachprod_weekly.sh) - TeamCity CI script
- [pkg/roachprod](../../../roachprod/) - Main roachprod package
