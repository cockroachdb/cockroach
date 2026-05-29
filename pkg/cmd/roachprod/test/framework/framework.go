// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package framework provides a test framework for running roachprod
// commands against real cloud infrastructure. It manages cluster
// lifecycle (creation, cleanup), command execution, and provides
// assertions for verifying cluster state.
//
// The central type is RoachprodTest, which wraps a *testing.T and
// provides methods for running roachprod commands, asserting cluster
// properties, and generating randomized configurations.
//
// Each test gets a uniquely named cluster that is automatically
// destroyed via t.Cleanup. All randomization uses a seeded RNG that
// is logged for reproducibility.
//
// Typical usage:
//
//	func TestExample(t *testing.T) {
//	    rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))
//
//	    rpt.RunExpectSuccess("create", rpt.ClusterName(),
//	        "-n", "3",
//	        "--clouds", "gce",
//	        "--lifetime", "1h",
//	    )
//
//	    rpt.AssertClusterExists()
//	    rpt.AssertClusterNodeCount(3)
//	    rpt.AssertClusterCloud("gce")
//	}
//
// For randomized testing:
//
//	func TestRandomized(t *testing.T) {
//	    rpt := framework.NewRoachprodTest(t, framework.WithTimeout(15*time.Minute))
//
//	    opts := framework.RandomGCECreateOptions(rpt.Rand())
//	    t.Logf("Config (seed=%d): %s", rpt.Seed(), opts.String())
//
//	    rpt.RunExpectSuccess(opts.ToCreateArgs(rpt.ClusterName())...)
//	    rpt.AssertClusterNodeCount(opts.NumNodes)
//	}
//
// Configuration options:
//
//	WithTimeout(d)      - set command timeout (default 5m)
//	WithClusterName(s)  - override auto-generated cluster name
//	WithSeed(n)         - set RNG seed (default: random, or COCKROACH_RANDOM_SEED)
//	DisableCleanup()    - skip cluster destruction (or set ROACHPROD_SKIP_CLEANUP)
package framework

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// RoachprodTest holds options and state for running e2e tests
type RoachprodTest struct {
	t              *testing.T
	roachprodPath  string
	clusterName    string
	timeout        time.Duration
	cleanupCluster bool
	rng            *rand.Rand
	seed           int64
	// clusterInfoCache caches the result of GetClusterInfo to avoid redundant
	// roachprod list calls. nil indicates the cache is stale/empty.
	clusterInfoCache *types.Cluster
}

// Option is a functional option for configuring RoachprodTest
type Option func(*RoachprodTest)

// WithTimeout sets the timeout for command execution
func WithTimeout(timeout time.Duration) Option {
	return func(opts *RoachprodTest) {
		opts.timeout = timeout
	}
}

// WithClusterName sets a custom cluster name
func WithClusterName(name string) Option {
	return func(opts *RoachprodTest) {
		opts.clusterName = name
	}
}

// DisableCleanup disables automatic cluster cleanup (useful for debugging)
func DisableCleanup() Option {
	return func(opts *RoachprodTest) {
		opts.cleanupCluster = false
	}
}

// WithSeed sets a specific random seed for reproducible test runs.
// This takes precedence over the COCKROACH_RANDOM_SEED environment variable.
func WithSeed(seed int64) Option {
	return func(opts *RoachprodTest) {
		opts.seed = seed
		opts.rng = rand.New(rand.NewSource(seed))
	}
}

// NewRoachprodTest creates a new test options with a unique cluster name
func NewRoachprodTest(t *testing.T, opts ...Option) *RoachprodTest {
	tc := &RoachprodTest{
		t:              t,
		roachprodPath:  findRoachprodBinary(t),
		clusterName:    generateClusterName(t),
		timeout:        5 * time.Minute,
		cleanupCluster: true,
	}

	// Apply options
	for _, opt := range opts {
		opt(tc)
	}

	// Check environment variable to skip cleanup (useful for debugging)
	if os.Getenv("ROACHPROD_SKIP_CLEANUP") != "" {
		tc.cleanupCluster = false
		t.Logf("ROACHPROD_SKIP_CLEANUP is set - cluster will be preserved after test")
	}

	// Initialize RNG and seed if not already set by WithSeed option
	if tc.rng == nil {
		// NewLockedPseudoRand automatically checks COCKROACH_RANDOM_SEED env var
		tc.rng, tc.seed = randutil.NewLockedPseudoRand()
	}

	// Log the seed for reproducibility
	t.Logf("Random seed: %d (use COCKROACH_RANDOM_SEED=%d or WithSeed(%d) to reproduce)",
		tc.seed, tc.seed, tc.seed)

	// Register cleanup function
	t.Cleanup(func() {
		if tc.cleanupCluster {
			tc.destroyCluster()
		} else {
			t.Logf("Skipping cluster cleanup - cluster %s will remain active", tc.clusterName)
		}
	})

	return tc
}

// ClusterName returns the cluster name for this test
func (tc *RoachprodTest) ClusterName() string {
	return tc.clusterName
}

// Rand returns the random number generator for this test
func (tc *RoachprodTest) Rand() *rand.Rand {
	return tc.rng
}

// Seed returns the random seed used for this test
func (tc *RoachprodTest) Seed() int64 {
	return tc.seed
}

// RunResult contains the result of a roachprod command execution
type RunResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
	Err      error
}

// Success returns true if the command succeeded
func (r *RunResult) Success() bool {
	return r.ExitCode == 0 && r.Err == nil
}

// Run executes a roachprod CLI command and returns the result.
// roachprodArgs are passed directly to the roachprod binary
// (e.g., "create", "my-cluster", "-n", "3", "--clouds", "gce").
func (tc *RoachprodTest) Run(roachprodArgs ...string) *RunResult {
	tc.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, tc.roachprodPath, roachprodArgs...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	tc.t.Logf("Running: roachprod %s", strings.Join(roachprodArgs, " "))

	err := cmd.Run()
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		}
	}

	result := &RunResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exitCode,
		Err:      err,
	}

	tc.t.Logf("Exit code: %d", exitCode)
	if result.Stdout != "" {
		tc.t.Logf("Stdout: %s", result.Stdout)
	}
	if result.Stderr != "" {
		tc.t.Logf("Stderr: %s", result.Stderr)
	}

	return result
}

// RunExpectSuccess runs a roachprod command and requires it to succeed.
func (tc *RoachprodTest) RunExpectSuccess(roachprodArgs ...string) *RunResult {
	tc.t.Helper()
	result := tc.Run(roachprodArgs...)
	require.True(tc.t, result.Success(),
		"Expected command to succeed but it failed:\nStdout: %s\nStderr: %s\nError: %v",
		result.Stdout, result.Stderr, result.Err)
	return result
}

// RunExpectFailure runs a roachprod command and requires it to fail.
func (tc *RoachprodTest) RunExpectFailure(roachprodArgs ...string) *RunResult {
	tc.t.Helper()
	result := tc.Run(roachprodArgs...)
	require.False(tc.t, result.Success(),
		"Expected command to fail but it succeeded:\nStdout: %s",
		result.Stdout)
	return result
}

// findRoachprodBinary locates the roachprod binary. Resolution order:
//  1. ROACHPROD_BINARY env var (set by Bazel go_test rules via $(location))
//  2. PATH lookup (fallback for running tests with go test instead of bazel test)
func findRoachprodBinary(t *testing.T) string {
	t.Helper()

	if path := os.Getenv("ROACHPROD_BINARY"); path != "" {
		resolved := resolveBazelBinaryPath(path)
		if _, err := os.Stat(resolved); err == nil {
			t.Logf("Using roachprod binary: %s", resolved)
			return resolved
		}
		t.Logf("Warning: ROACHPROD_BINARY=%s not found, trying PATH", path)
	}

	if path, err := exec.LookPath("roachprod"); err == nil {
		t.Logf("Using roachprod from PATH: %s", path)
		return path
	}

	t.Fatal("Could not find roachprod binary. " +
		"Build with './dev build roachprod' or set ROACHPROD_BINARY")
	return ""
}

// resolveBazelBinaryPath translates a Bazel build-time path to a test-runtime
// path. This is needed because Bazel uses two separate directory trees:
//
//   - Output tree (bazel-out/): where built artifacts live, organized by
//     platform and build config (e.g. darwin_arm64-fastbuild, k8-opt).
//   - Runfiles tree ($TEST_SRCDIR/): where test dependencies are staged at
//     runtime, organized by workspace name and package path.
//
// ROACHPROD_BINARY is set via $(location //pkg/cmd/roachprod) in the go_test
// rule, which resolves to an output-tree path like:
//
//	bazel-out/darwin_arm64-fastbuild/bin/pkg/cmd/roachprod/roachprod_/roachprod
//
// But at test runtime, the binary is in the runfiles tree at:
//
//	$TEST_SRCDIR/com_github_cockroachdb_cockroach/pkg/cmd/roachprod/roachprod_/roachprod
//
// This function strips the "bazel-out/<config>/bin/" prefix to extract the
// workspace-relative path, then prepends the runfiles root. If the path is
// already absolute or TEST_SRCDIR is not set, it returns the path unchanged.
func resolveBazelBinaryPath(path string) string {
	if strings.HasPrefix(path, "/") {
		return path
	}
	testSrcDir := os.Getenv("TEST_SRCDIR")
	if testSrcDir == "" {
		return path
	}

	// Strip "bazel-out/<config>/bin/" prefix generically.
	runfilesPath := path
	const bazelBinMarker = "/bin/"
	if strings.HasPrefix(path, "bazel-out/") {
		if idx := strings.Index(path, bazelBinMarker); idx != -1 {
			runfilesPath = path[idx+len(bazelBinMarker):]
		}
	}

	fullPath := fmt.Sprintf("%s/com_github_cockroachdb_cockroach/%s", testSrcDir, runfilesPath)
	if _, err := os.Stat(fullPath); err == nil {
		return fullPath
	}
	// Runfiles path didn't work; return original path as fallback.
	return path
}

// generateClusterName generates a unique cluster name for the test
func generateClusterName(t *testing.T) string {
	t.Helper()

	// Read username from ROACHPROD_USER environment variable
	// Roachprod uses this env var to prefix cluster names
	username := os.Getenv("ROACHPROD_USER")
	if username == "" {
		t.Fatal("ROACHPROD_USER environment variable must be set")
	}
	username = strings.ReplaceAll(username, ".", "")

	// Sanitize test name to create a valid cluster name
	testName := strings.ToLower(t.Name())
	testName = strings.ReplaceAll(testName, "/", "-")
	testName = strings.ReplaceAll(testName, "_", "-")
	testName = strings.ReplaceAll(testName, " ", "-")

	// Truncate if too long and add timestamp for uniqueness
	if len(testName) > 20 {
		testName = testName[:20]
	}

	timestamp := timeutil.Now().UnixNano()
	return fmt.Sprintf("%s-%s-%d", username, testName, timestamp)
}

// extractJSON finds and returns the JSON object from roachprod stdout output.
// roachprod list --json may write warning/error messages to stdout before or
// after the JSON (e.g., IBM provider failures, AWS auth errors). The JSON
// encoder uses SetIndent("", "  "), so the top-level opening brace is always
// "{" alone on its own line and the closing "}" likewise — error messages
// won't match this pattern.
func extractJSON(stdout string) (string, error) {
	lines := strings.Split(stdout, "\n")
	start := -1
	end := -1
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if start == -1 && trimmed == "{" {
			start = i
		}
		if trimmed == "}" {
			end = i
		}
	}
	if start == -1 || end == -1 || end < start {
		return "", fmt.Errorf("no JSON object found in output")
	}
	return strings.Join(lines[start:end+1], "\n"), nil
}

// GetClusterInfo returns cached cluster information or fetches it if the cache is stale.
// To force a fresh fetch, use RefreshClusterInfo() or InvalidateClusterCache() first.
func (tc *RoachprodTest) GetClusterInfo() *types.Cluster {
	tc.t.Helper()

	if tc.clusterInfoCache != nil {
		return tc.clusterInfoCache
	}
	return tc.RefreshClusterInfo()
}

// RefreshClusterInfo fetches fresh cluster information from roachprod and caches it.
// Use this when you need to ensure you have the latest cluster state.
func (tc *RoachprodTest) RefreshClusterInfo() *types.Cluster {
	tc.t.Helper()

	result := tc.Run("list", "--json", "--pattern", tc.clusterName)
	require.True(tc.t, result.Success(),
		"Failed to get cluster info:\nStdout: %s\nStderr: %s\nError: %v",
		result.Stdout, result.Stderr, result.Err)

	jsonData, err := extractJSON(result.Stdout)
	require.NoError(tc.t, err, "Failed to find JSON in roachprod list output:\n%s", result.Stdout)

	var listOutput cloud.Cloud
	err = json.Unmarshal([]byte(jsonData), &listOutput)
	require.NoError(tc.t, err, "Failed to parse cluster info JSON: %s", jsonData)

	// Extract our cluster from the map
	clusterInfo, ok := listOutput.Clusters[tc.clusterName]
	require.True(tc.t, ok, "Cluster %s not found in list output", tc.clusterName)

	// Cache the result
	tc.clusterInfoCache = clusterInfo
	return clusterInfo
}

// InvalidateClusterCache marks the cached cluster information as stale.
// The next call to GetClusterInfo() will fetch fresh data from roachprod.
// Call this after operations that modify cluster state (e.g., extend, start, stop).
func (tc *RoachprodTest) InvalidateClusterCache() {
	tc.clusterInfoCache = nil
}
