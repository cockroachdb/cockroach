// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprodtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestOptions holds options and state for running e2e tests
type TestOptions struct {
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

// Option is a functional option for configuring TestOptions
type Option func(*TestOptions)

// WithTimeout sets the timeout for command execution
func WithTimeout(timeout time.Duration) Option {
	return func(opts *TestOptions) {
		opts.timeout = timeout
	}
}

// WithClusterName sets a custom cluster name
func WithClusterName(name string) Option {
	return func(opts *TestOptions) {
		opts.clusterName = name
	}
}

// DisableCleanup disables automatic cluster cleanup (useful for debugging)
func DisableCleanup() Option {
	return func(opts *TestOptions) {
		opts.cleanupCluster = false
	}
}

// WithSeed sets a specific random seed for reproducible test runs.
// This takes precedence over the COCKROACH_RANDOM_SEED environment variable.
func WithSeed(seed int64) Option {
	return func(opts *TestOptions) {
		opts.seed = seed
		opts.rng = rand.New(rand.NewSource(seed))
	}
}

// NewRoachprodTest creates a new test options with a unique cluster name
func NewRoachprodTest(t *testing.T, opts ...Option) *TestOptions {
	tc := &TestOptions{
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
func (tc *TestOptions) ClusterName() string {
	return tc.clusterName
}

// Rand returns the random number generator for this test
func (tc *TestOptions) Rand() *rand.Rand {
	return tc.rng
}

// Seed returns the random seed used for this test
func (tc *TestOptions) Seed() int64 {
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

// Run executes a roachprod command and returns the result
func (tc *TestOptions) Run(args ...string) *RunResult {
	tc.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, tc.roachprodPath, args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	tc.t.Logf("Running: roachprod %s", strings.Join(args, " "))

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

// RunExpectSuccess runs a command and requires it to succeed
func (tc *TestOptions) RunExpectSuccess(args ...string) *RunResult {
	tc.t.Helper()
	result := tc.Run(args...)
	require.True(tc.t, result.Success(),
		"Expected command to succeed but it failed:\nStdout: %s\nStderr: %s\nError: %v",
		result.Stdout, result.Stderr, result.Err)
	return result
}

// RunExpectFailure runs a command and requires it to fail
func (tc *TestOptions) RunExpectFailure(args ...string) *RunResult {
	tc.t.Helper()
	result := tc.Run(args...)
	require.False(tc.t, result.Success(),
		"Expected command to fail but it succeeded:\nStdout: %s",
		result.Stdout)
	return result
}

// findRoachprodBinary locates the roachprod binary
func findRoachprodBinary(t *testing.T) string {
	t.Helper()

	// First, check if ROACHPROD_BINARY environment variable is set (Bazel sets this)
	path := os.Getenv("ROACHPROD_BINARY")
	t.Logf("DEBUG: ROACHPROD_BINARY env var = '%s'", path)

	if path != "" {
		// If path is relative, resolve it relative to TEST_SRCDIR (Bazel runfiles)
		if !strings.HasPrefix(path, "/") {
			if testSrcDir := os.Getenv("TEST_SRCDIR"); testSrcDir != "" {
				// The path in ROACHPROD_BINARY is relative to the bazel-out directory
				// In runfiles, it's at: com_github_cockroachdb_cockroach/pkg/cmd/roachprod/roachprod_/roachprod
				// The ROACHPROD_BINARY is: bazel-out/darwin_arm64-fastbuild/bin/pkg/cmd/roachprod/roachprod_/roachprod
				// Extract just the last part: pkg/cmd/roachprod/roachprod_/roachprod
				runfilesPath := strings.Replace(path, "bazel-out/darwin_arm64-fastbuild/bin/", "", 1)
				runfilesPath = strings.Replace(runfilesPath, "bazel-out/darwin_arm64-opt/bin/", "", 1)
				runfilesPath = strings.Replace(runfilesPath, "bazel-out/k8-fastbuild/bin/", "", 1)
				runfilesPath = strings.Replace(runfilesPath, "bazel-out/k8-opt/bin/", "", 1)

				fullPath := fmt.Sprintf("%s/com_github_cockroachdb_cockroach/%s", testSrcDir, runfilesPath)
				t.Logf("DEBUG: Trying runfiles path: %s", fullPath)
				if stat, err := os.Stat(fullPath); err == nil {
					t.Logf("Using roachprod binary from runfiles: %s (size=%d)", fullPath, stat.Size())
					return fullPath
				} else {
					t.Logf("DEBUG: Stat error for %s: %v", fullPath, err)
				}
			}
		}

		// Try the path as-is (might be absolute or relative to cwd)
		t.Logf("DEBUG: Checking if path exists as-is: %s", path)
		if stat, err := os.Stat(path); err == nil {
			t.Logf("Using roachprod binary from ROACHPROD_BINARY: %s (size=%d)", path, stat.Size())
			return path
		} else {
			t.Logf("Warning: ROACHPROD_BINARY=%s stat error: %v", path, err)
		}
	}

	// Try to find roachprod in PATH
	t.Logf("DEBUG: Searching for roachprod in PATH")
	if path, err := exec.LookPath("roachprod"); err == nil {
		t.Logf("Found roachprod in PATH: %s", path)
		return path
	}

	t.Fatal("Could not find roachprod binary. Please build it with './dev build roachprod' or set ROACHPROD_BINARY environment variable")
	return ""
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

	timestamp := time.Now().Unix()
	return fmt.Sprintf("%s-%s-%d", username, testName, timestamp)
}
