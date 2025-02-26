// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"archive/zip"
	"bytes"
	"context"
	enc_hex "encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/cockroachdb/cockroach/pkg/backup"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestZipContainsAllInternalTables verifies that we don't add new internal tables
// without also taking them into account in a `debug zip`. If this test fails,
// add your table to either of the []string slices referenced in the test (which
// are used by `debug zip`) or add it as an exception after having verified that
// it indeed should not be collected (this is rare).
// NB: if you're adding a new one, you'll also have to update TestZip.
func TestZipContainsAllInternalTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	rows, err := db.Query(`
SELECT concat('crdb_internal.', table_name) as name
FROM [ SELECT table_name FROM [ SHOW TABLES FROM crdb_internal ] ]
WHERE
table_name NOT IN (
	-- allowlisted tables that don't need to be in debug zip
	'backward_dependencies',
	'builtin_functions',
	'cluster_contended_keys',
	'cluster_contended_indexes',
	'cluster_contended_tables',
	'cluster_execution_insights',
	'cluster_inflight_traces',
	'cluster_txn_execution_insights',
	'cross_db_references',
	'databases',
	'forward_dependencies',
	'gossip_network',
	'index_columns',
  'index_spans',
  'kv_builtin_function_comments',
	'kv_catalog_comments',
	'kv_catalog_descriptor',
	'kv_catalog_namespace',
	'kv_catalog_zones',
	'kv_repairable_catalog_corruptions',
	'kv_dropped_relations',
	'kv_inherited_role_members',
	'kv_flow_control_handles',
	'kv_flow_control_handles_v2',
	'kv_flow_controller',
	'kv_flow_controller_v2',
	'kv_flow_token_deductions',
	'kv_flow_token_deductions_v2',
	'lost_descriptors_with_data',
	'table_columns',
	'table_row_statistics',
	'ranges',
	'ranges_no_leases',
	'predefined_comments',
	'session_trace',
	'session_variables',
	'table_spans',
	'tables',
	'cluster_statement_statistics',
	'statement_activity',
	'statement_statistics_persisted',
	'statement_statistics_persisted_v22_2',
	'store_liveness_support_for',
	'store_liveness_support_from',
	'cluster_transaction_statistics',
	'statement_statistics',
	'transaction_activity',
	'transaction_statistics_persisted',
	'transaction_statistics_persisted_v22_2',
	'transaction_statistics',
	'tenant_usage_details',
	'pg_catalog_table_is_implemented',
	'fully_qualified_names'
)
ORDER BY name ASC`)
	assert.NoError(t, err)

	var tables []string
	for rows.Next() {
		var table string
		assert.NoError(t, rows.Scan(&table))
		tables = append(tables, table)
	}
	tables = append(tables, "crdb_internal.probe_ranges_1s_read_limit_100")
	sort.Strings(tables)

	var exp []string
	exp = append(exp, zipInternalTablesPerNode.GetTables()...)
	for _, t := range zipInternalTablesPerCluster.GetTables() {
		t = strings.TrimPrefix(t, `"".`)
		exp = append(exp, t)
	}
	sort.Strings(exp)

	assert.Equal(t, exp, tables)
}

// This tests the operation of zip over secure clusters.
func TestZip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "test too slow under race")

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.Cleanup()

	out, err := c.RunWithCapture("debug zip --concurrency=1 --cpu-profile-duration=1s " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages.
	out = eraseNonDeterministicZipOutput(out)

	// We use datadriven simply to read the golden output file; we don't actually
	// run any commands. Using datadriven allows TESTFLAGS=-rewrite.
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "zip", "testzip"), func(t *testing.T, td *datadriven.TestData) string {
		return out
	})
}

// This tests the operation of zip over secure clusters.
func TestZipQueryFallback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "test too slow under race")

	existing := zipInternalTablesPerCluster["crdb_internal.transaction_contention_events"]

	// Avoid leaking configuration changes after the tests end.
	defer func() {
		zipInternalTablesPerCluster["crdb_internal.transaction_contention_events"] = existing
	}()

	zipInternalTablesPerCluster["crdb_internal.transaction_contention_events"] = TableRegistryConfig{
		nonSensitiveCols: existing.nonSensitiveCols,
		// We want this to fail to trigger the fallback.
		customQueryUnredacted:         "SELECT FAIL;",
		customQueryUnredactedFallback: existing.customQueryUnredactedFallback,
	}

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.Cleanup()

	out, err := c.RunWithCapture("debug zip --concurrency=1 --cpu-profile-duration=1s " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages.
	out = eraseNonDeterministicZipOutput(out)

	// We use datadriven simply to read the golden output file; we don't actually
	// run any commands. Using datadriven allows TESTFLAGS=-rewrite.
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "zip", "testzip_fallback"), func(t *testing.T, td *datadriven.TestData) string {
		return out
	})
}

// This tests the operation of redacted zip over secure clusters.
func TestZipRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()

	skip.UnderRace(t, "test too slow under race")

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.Cleanup()

	out, err := c.RunWithCapture("debug zip --concurrency=1 --cpu-profile-duration=1s --redact " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages.
	out = eraseNonDeterministicZipOutput(out)

	// We use datadriven simply to read the golden output file; we don't actually
	// run any commands. Using datadriven allows TESTFLAGS=-rewrite.
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "zip", "testzip_redacted"), func(t *testing.T, td *datadriven.TestData) string {
		return out
	})
}

// This tests the operation of zip using --include-goroutine-stacks.
func TestZipIncludeGoroutineStacks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "test too slow under race")

	tests := []struct {
		name           string
		includeStacks  bool
		outputFileName string
	}{
		{
			name:           "includes goroutine stacks",
			includeStacks:  true,
			outputFileName: "testzip_include_goroutine_stacks",
		},
		{
			name:           "excludes goroutine stacks",
			includeStacks:  false,
			outputFileName: "testzip_exclude_goroutine_stacks",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir, cleanupFn := testutils.TempDir(t)
			defer cleanupFn()

			c := NewCLITest(TestCLIParams{
				StoreSpecs: []base.StoreSpec{{
					Path: dir,
				}},
			})
			defer c.Cleanup()
			cmd := "debug zip --concurrency=1 --cpu-profile-duration=1s "
			if !tc.includeStacks {
				cmd = cmd + "--include-goroutine-stacks=false "
			}

			out, err := c.RunWithCapture(cmd + os.DevNull)
			if err != nil {
				t.Fatal(err)
			}

			// Strip any non-deterministic messages.
			out = eraseNonDeterministicZipOutput(out)

			// We use datadriven simply to read the golden output file; we don't actually
			// run any commands. Using datadriven allows TESTFLAGS=-rewrite.
			datadriven.RunTest(t, datapathutils.TestDataPath(t, "zip", tc.outputFileName),
				func(t *testing.T, td *datadriven.TestData) string {
					return out
				},
			)
		})
	}
}

// This tests the operation of zip using --include-range-info.
func TestZipIncludeRangeInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "test too slow under race")

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.Cleanup()

	out, err := c.RunWithCapture("debug zip --concurrency=1 --cpu-profile-duration=1s --include-range-info " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages.
	out = eraseNonDeterministicZipOutput(out)

	// We use datadriven simply to read the golden output file; we don't actually
	// run any commands. Using datadriven allows TESTFLAGS=-rewrite.
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "zip", "testzip_include_range_info"),
		func(t *testing.T, td *datadriven.TestData) string {
			return out
		},
	)
}

// This tests the operation of zip using --include-range-info=false.
func TestZipExcludeRangeInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "test too slow under race")

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.Cleanup()

	out, err := c.RunWithCapture(
		"debug zip --concurrency=1 --cpu-profile-duration=1s --include-range-info=false " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages.
	out = eraseNonDeterministicZipOutput(out)

	// We use datadriven simply to read the golden output file; we don't actually
	// run any commands. Using datadriven allows TESTFLAGS=-rewrite.
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "zip", "testzip_exclude_range_info"),
		func(t *testing.T, td *datadriven.TestData) string {
			return out
		},
	)
}

// This tests the operation of zip running concurrently.
func TestConcurrentZip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We want a low timeout so that the test doesn't take forever;
	// however low timeouts make race runs flaky with false positives.
	skip.UnderShort(t)
	skip.UnderRace(t)

	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	// Reduce the number of output log files to just what's expected.
	defer sc.SetupSingleFileLogging()()

	ctx := context.Background()

	// Three nodes. We want to see what `zip` thinks when one of the nodes is down.
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			Insecure:          true,
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Zip it. We fake a CLI test context for this.
	c := TestCLI{
		t:        t,
		Server:   tc.Server(0),
		Insecure: true,
	}
	defer func(prevStderr *os.File) { stderr = prevStderr }(stderr)
	stderr = os.Stdout

	out, err := c.RunWithCapture("debug zip --timeout=30s --cpu-profile-duration=0s " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages.
	out = eraseNonDeterministicZipOutput(out)

	// Sort the lines to remove non-determinism in the concurrent execution.
	lines := strings.Split(out, "\n")
	sort.Strings(lines)
	out = strings.TrimSpace(strings.Join(lines, "\n"))
	// Remove all "dumping SQL tables" messages since non-deterministic order in
	// which the original messages interleve with other messages mean the number
	// of them after each series is collapsed is also non-derministic.
	out = regexp.MustCompile(`<dumping SQL tables>\n`).ReplaceAllString(out, "")

	// We use datadriven simply to read the golden output file; we don't actually
	// run any commands. Using datadriven allows TESTFLAGS=-rewrite.
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "zip", "testzip_concurrent"), func(t *testing.T, td *datadriven.TestData) string {
		return out
	})
}

func TestZipSpecialNames(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)
	skip.UnderRace(t)

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.Cleanup()

	c.RunWithArgs([]string{"sql", "-e", `
create database "a:b";
create database "a-b";
create database "a-b-1";
create database "SYSTEM";
create table "SYSTEM.JOBS"(x int);
create database "../system";
create table defaultdb."a:b"(x int);
create table defaultdb."a-b"(x int);
create table defaultdb."pg_catalog.pg_class"(x int);
create table defaultdb."../system"(x int);
`})

	out, err := c.RunWithCapture("debug zip --concurrency=1 --cpu-profile-duration=0 " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	re := regexp.MustCompile(`(?m)^.*(table|database).*$`)
	out = strings.Join(re.FindAllString(out, -1), "\n")

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "zip", "specialnames"),
		func(t *testing.T, td *datadriven.TestData) string {
			return out
		})
}

// This tests the operation of zip over unavailable clusters.
//
// We cannot combine this test with TestZip above because TestPartialZip
// needs a TestCluster, the TestCluster hides its SSL certs, and we
// need the SSL certs dir to run a CLI test securely.
func TestUnavailableZip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 141565)

	skip.UnderShort(t)
	// Race builds make the servers so slow that they report spurious
	// unavailability.
	skip.UnderRace(t)

	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)
	// Reduce the number of output log files to just what's expected.
	defer sc.SetupSingleFileLogging()()

	// unavailableCh is used by the replica command filter
	// to conditionally block requests and simulate unavailability.
	var unavailableCh atomic.Value
	closedCh := make(chan struct{})
	close(closedCh)
	unavailableCh.Store(closedCh)
	knobs := &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(ctx context.Context,
			br *kvpb.BatchRequest) *kvpb.Error {
			if br.Header.GatewayNodeID == 2 {
				// For node 2 connections, block all replica requests.
				select {
				case <-unavailableCh.Load().(chan struct{}):
				case <-ctx.Done():
				}
			} else if br.Header.GatewayNodeID == 1 {
				// For node 1 connections, only block requests to table data ranges.
				if br.Requests[0].GetInner().Header().Key.Compare(keys.
					TableDataMin) >= 0 {
					select {
					case <-unavailableCh.Load().(chan struct{}):
					case <-ctx.Done():
					}
				}
			}
			return nil
		},
	}

	// Make a 3-node cluster, with an option to block replica requests.
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,

			Insecure: true,
			Knobs:    base.TestingKnobs{Store: knobs},
		}})
	defer tc.Stopper().Stop(context.Background())

	// Sanity test: check that a simple SQL operation works against node 1.
	if _, err := tc.ServerConn(0).Exec("SELECT * FROM system.users"); err != nil {
		t.Fatal(err)
	}

	// Block querying table data from node 1.
	// Block all replica requests from node 2.
	ch := make(chan struct{})
	unavailableCh.Store(ch)
	defer close(ch)

	// Run debug zip against node 1.
	debugZipCommand :=
		"debug zip --concurrency=1 --cpu-profile-duration=0 " + os.
			DevNull + " --timeout=.5s"

	t.Run("server 1", func(t *testing.T) {
		c := TestCLI{
			Server:   tc.Server(0),
			Insecure: true,
		}

		out, err := c.RunWithCapture(debugZipCommand)
		require.NoError(t, err)

		// Assert debug zip output for cluster, node 1, node 2, node 3.
		assert.NotEmpty(t, out)
		clusterOut := []string{
			"[cluster] requesting nodes... received response...",
			"[cluster] requesting liveness... received response...",
		}
		expectedOut := clusterOut
		for i := 1; i < tc.NumServers()+1; i++ {
			nodeOut := baseZipOutput(i)

			expectedOut = append(expectedOut, nodeOut...)

			// If the request to nodes failed, we can't expect the remaining
			// nodes to be present in the debug zip output.
			if i == 1 && strings.Contains(out,
				"[cluster] requesting nodes: last request failed") {
				break
			}
		}

		containsAssert(t, out, expectedOut)
	})

	t.Run("server 2", func(t *testing.T) {
		// Run debug zip against node 2.
		c := TestCLI{
			Server:   tc.Server(1),
			Insecure: true,
		}

		out, err := c.RunWithCapture(debugZipCommand)
		require.NoError(t, err)

		// Assert debug zip output for cluster, node 2.
		assert.NotEmpty(t, out)
		assert.NotContains(t, out, "[node 1]")
		assert.NotContains(t, out, "[node 3]")

		clusterOut := []string{
			"[cluster] requesting nodes... received response...",
			"[cluster] requesting liveness... received response...",
		}
		nodeOut := baseZipOutput(2)
		expectedOut := append(clusterOut, nodeOut...)

		containsAssert(t, out, expectedOut)
	})
}

func containsAssert(t *testing.T, actual string, expected []string) {
	var logOut bool
	for _, line := range expected {
		if !strings.Contains(actual, line) {
			assertFail := fmt.Sprintf("output does not contain %#v", line)
			if !logOut {
				assertFail = fmt.Sprintf(
					"the following output does not contain expected lines:\n%#v\n",
					actual) + assertFail
				logOut = true
			}
			assert.Fail(t, assertFail)
		}
	}
}

func baseZipOutput(nodeId int) []string {
	output := []string{
		fmt.Sprintf("[node %d] using SQL connection URL", nodeId),
		fmt.Sprintf("[node %d] requesting stacks... received response...", nodeId),
		fmt.Sprintf("[node %d] requesting stacks with labels... received response...", nodeId),
		fmt.Sprintf("[node %d] requesting heap profile list... received response...", nodeId),
		fmt.Sprintf("[node %d] requesting goroutine dump list... received response...", nodeId),
		fmt.Sprintf("[node %d] requesting cpu profile list... received response...", nodeId),
		fmt.Sprintf("[node %d] requesting log files list... received response...", nodeId),
		fmt.Sprintf("[node %d] requesting ranges... received response...", nodeId),
	}
	return output
}

func eraseNonDeterministicZipOutput(out string) string {
	re := regexp.MustCompile(`(?m)postgresql://.*$`)
	out = re.ReplaceAllString(out, `postgresql://...`)
	re = regexp.MustCompile(`(?m)SQL address: .*$`)
	out = re.ReplaceAllString(out, `SQL address: ...`)
	re = regexp.MustCompile(`(?m)^\[node \d+\] \[log file:.*$` + "\n")
	out = re.ReplaceAllString(out, ``)
	re = regexp.MustCompile(`(?m)RPC connection to .*$`)
	out = re.ReplaceAllString(out, `RPC connection to ...`)
	re = regexp.MustCompile(`(?m)dial tcp .*$`)
	out = re.ReplaceAllString(out, `dial tcp ...`)
	re = regexp.MustCompile(`(?m)rpc error: .*$`)
	out = re.ReplaceAllString(out, `rpc error: ...`)
	re = regexp.MustCompile(`(?m)timed out after.*$`)
	out = re.ReplaceAllString(out, `timed out after...`)
	re = regexp.MustCompile(`(?m)failed to connect to .*$`)
	out = re.ReplaceAllString(out, `failed to connect to ...`)

	// The number of memory profiles previously collected is not deterministic.
	re = regexp.MustCompile(`(?m)^\[node \d+\] \d+ heap profiles found$`)
	out = re.ReplaceAllString(out, `[node ?] ? heap profiles found`)
	re = regexp.MustCompile(`(?m)^\[node \d+\] \d+ goroutine dumps found$`)
	out = re.ReplaceAllString(out, `[node ?] ? goroutine dumps found`)
	re = regexp.MustCompile(`(?m)^\[node \d+\] \d+ cpu profiles found$`)
	out = re.ReplaceAllString(out, `[node ?] ? cpu profiles found`)
	re = regexp.MustCompile(`(?m)^\[node \d+\] retrieving cpuprof.*$` + "\n")
	out = re.ReplaceAllString(out, ``)
	re = regexp.MustCompile(`(?m)^\[node \d+\] \d+ log files found$`)
	out = re.ReplaceAllString(out, `[node ?] ? log files found`)
	re = regexp.MustCompile(`(?m)^\[node \d+\] retrieving (memprof|memstats|memmonitoring).*$` + "\n")
	out = re.ReplaceAllString(out, ``)
	re = regexp.MustCompile(`(?m)^\[node \d+\] writing profile.*$` + "\n")
	out = re.ReplaceAllString(out, ``)
	re = regexp.MustCompile(`(?m)^\[node \d+\] writing dump.*$` + "\n")
	out = re.ReplaceAllString(out, ``)
	re = regexp.MustCompile(`(?m)^\[node \d+\] retrieving goroutine_dump.*$` + "\n")
	out = re.ReplaceAllString(out, ``)

	return out
}

// This tests the operation of zip over partial clusters.
//
// We cannot combine this test with TestZip above because TestPartialZip
// needs a TestCluster, the TestCluster hides its SSL certs, and we
// need the SSL certs dir to run a CLI test securely.
func TestPartialZip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We want a low timeout so that the test doesn't take forever;
	// however low timeouts make race runs flaky with false positives.
	skip.UnderShort(t)
	skip.UnderRace(t)

	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)
	// Reduce the number of output log files to just what's expected.
	defer sc.SetupSingleFileLogging()()

	ctx := context.Background()

	// Three nodes. We want to see what `zip` thinks when one of the nodes is down.
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			Insecure:          true,
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Switch off the second node.
	tc.StopServer(1)

	// Zip it. We fake a CLI test context for this.
	c := TestCLI{
		t:        t,
		Server:   tc.Server(0),
		Insecure: true,
	}
	defer func(prevStderr *os.File) { stderr = prevStderr }(stderr)
	stderr = os.Stdout

	out, err := c.RunWithCapture("debug zip --concurrency=1 --cpu-profile-duration=0s " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages.
	t.Log(out)
	out = eraseNonDeterministicZipOutput(out)

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "zip", "partial1"),
		func(t *testing.T, td *datadriven.TestData) string {
			return out
		})

	// Now do it again and exclude the down node explicitly.
	out, err = c.RunWithCapture("debug zip " + os.DevNull + " --concurrency=1 --exclude-nodes=2 --cpu-profile-duration=0")
	if err != nil {
		t.Fatal(err)
	}

	out = eraseNonDeterministicZipOutput(out)
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "zip", "partial1_excluded"),
		func(t *testing.T, td *datadriven.TestData) string {
			return out
		})

	// Now mark the stopped node as decommissioned, and check that zip
	// skips over it automatically. We specifically use --wait=none because
	// we're decommissioning a node in a 3-node cluster, so there's no node to
	// up-replicate the under-replicated ranges to.
	{
		_, err := c.RunWithCapture(fmt.Sprintf("node decommission --checks=skip --wait=none %d", 2))
		if err != nil {
			t.Fatal(err)
		}
	}

	// We use .Override() here instead of SET CLUSTER SETTING in SQL to
	// override the 1m15s minimum placed on the cluster setting. There
	// is no risk to see the override bumped due to a gossip update
	// because this setting is not otherwise set in the test cluster.
	s := tc.Server(0)
	liveness.TimeUntilNodeDead.Override(ctx, &s.ClusterSettings().SV, liveness.TestTimeUntilNodeDead)

	// This last case may take a little while to converge. To make this work with datadriven and at the same
	// time retain the ability to use the `-rewrite` flag, we use a retry loop within that already checks the
	// output ahead of time and retries for some time if necessary.
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "zip", "partial2"),
		func(t *testing.T, td *datadriven.TestData) string {
			f := func() string {
				out, err := c.RunWithCapture("debug zip --concurrency=1 --cpu-profile-duration=0 " + os.DevNull)
				if err != nil {
					t.Fatal(err)
				}

				// Strip any non-deterministic messages.
				return eraseNonDeterministicZipOutput(out)
			}

			var out string
			_ = testutils.SucceedsSoonError(func() error {
				out = f()
				if out != td.Expected {
					return errors.New("output did not match (yet)")
				}
				return nil
			})
			return out
		})
}

// This checks that SQL retry errors are properly handled.
func TestZipRetries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{Insecure: true})
	defer s.Stopper().Stop(context.Background())

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	zipName := filepath.Join(dir, "test.zip")

	func() {
		out, err := os.Create(zipName)
		if err != nil {
			t.Fatal(err)
		}
		z := newZipper(out)
		defer func() {
			if err := z.close(); err != nil {
				t.Fatal(err)
			}
		}()

		// Lower the buffer size so that an error is returned when running the
		// generate_series query.
		sqlURL := url.URL{
			Scheme:   "postgres",
			User:     url.User(username.RootUser),
			Host:     s.AdvSQLAddr(),
			RawQuery: "sslmode=disable&results_buffer_size=16KiB",
		}
		sqlConn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, sqlURL.String())
		defer func() {
			if err := sqlConn.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		zr := zipCtx.newZipReporter("test")
		zr.sqlOutputFilenameExtension = "json"
		zc := debugZipContext{
			z:              z,
			clusterPrinter: zr,
			timeout:        3 * time.Second,
		}
		if err := zc.dumpTableDataForZip(
			zr,
			sqlConn,
			"test",
			`generate_series(1,15000) as t(x)`,
			TableQuery{query: `select if(x<11000,x,crdb_internal.force_retry('1h')) from generate_series(1,15000) as t(x)`},
		); err != nil {
			t.Fatal(err)
		}
	}()

	r, err := zip.OpenReader(zipName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	var fileList bytes.Buffer
	for _, f := range r.File {
		fmt.Fprintln(&fileList, f.Name)
	}
	const expected = `test/generate_series(1,15000) as t(x).json
test/generate_series(1,15000) as t(x).json.err.txt
test/generate_series(1,15000) as t(x).1.json
test/generate_series(1,15000) as t(x).1.json.err.txt
test/generate_series(1,15000) as t(x).2.json
test/generate_series(1,15000) as t(x).2.json.err.txt
test/generate_series(1,15000) as t(x).3.json
test/generate_series(1,15000) as t(x).3.json.err.txt
test/generate_series(1,15000) as t(x).4.json
test/generate_series(1,15000) as t(x).4.json.err.txt
`
	assert.Equal(t, expected, fileList.String())
}

// This checks that SQL retry errors are properly handled.
func TestZipFallback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{Insecure: true})
	defer s.Stopper().Stop(context.Background())

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	zipName := filepath.Join(dir, "test.zip")

	func() {
		out, err := os.Create(zipName)
		if err != nil {
			t.Fatal(err)
		}
		z := newZipper(out)
		defer func() {
			if err := z.close(); err != nil {
				t.Fatal(err)
			}
		}()

		// Lower the buffer size so that an error is returned when running the
		// generate_series query.
		sqlURL := url.URL{
			Scheme: "postgres",
			User:   url.User(username.RootUser),
			Host:   s.AdvSQLAddr(),
		}
		sqlConn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, sqlURL.String())
		defer func() {
			if err := sqlConn.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		zr := zipCtx.newZipReporter("test")
		zr.sqlOutputFilenameExtension = "json"
		zc := debugZipContext{
			z:              z,
			clusterPrinter: zr,
			timeout:        3 * time.Second,
		}
		if err := zc.dumpTableDataForZip(
			zr,
			sqlConn,
			"test",
			`test_table_fail`,
			TableQuery{
				query: `SELECT blah`,
			},
		); err != nil {
			t.Fatal(err)
		}
		if err := zc.dumpTableDataForZip(
			zr,
			sqlConn,
			"test",
			`test_table_succeed`,
			TableQuery{
				query:    `SELECT blah`,
				fallback: `SELECT 1`,
			},
		); err != nil {
			t.Fatal(err)
		}
	}()

	r, err := zip.OpenReader(zipName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	var fileList bytes.Buffer
	for _, f := range r.File {
		fmt.Fprintln(&fileList, f.Name)
	}
	const expected = `test/test_table_fail.json
test/test_table_fail.json.err.txt
test/test_table_succeed.json
test/test_table_succeed.json.err.txt
test/test_table_succeed.fallback.json
`
	assert.Equal(t, expected, fileList.String())
}

// This test the operation of zip over secure clusters.
func TestToHex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()
	c := NewCLITest(TestCLIParams{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.Cleanup()

	// Create a job to have non-empty system.jobs table.
	c.RunWithArgs([]string{"sql", "-e", "CREATE STATISTICS foo FROM system.namespace"})

	_, err := c.RunWithCapture("debug zip --concurrency=1 --cpu-profile-duration=0 " + dir + "/debug.zip")
	if err != nil {
		t.Fatal(err)
	}

	r, err := zip.OpenReader(dir + "/debug.zip")
	if err != nil {
		t.Fatal(err)
	}

	type hexField struct {
		idx int
		msg protoutil.Message
	}
	// Stores index and type of marshaled messages in the table row.
	// Negative indices work from the end - this is needed because parsing the
	// fields is not always precise as there can be spaces in the fields but the
	// hex fields are always in the end of the row and they don't contain spaces.
	hexFiles := map[string][]hexField{
		"debug/system.descriptor.txt": {
			{idx: 1, msg: &descpb.Descriptor{}},
		},
	}

	for _, f := range r.File {
		fieldsToCheck, ok := hexFiles[f.Name]
		if !ok {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()

		buf := new(bytes.Buffer)
		if _, err = buf.ReadFrom(rc); err != nil {
			t.Fatal(err)
		}
		// Skip header.
		if _, err = buf.ReadString('\n'); err != nil {
			t.Fatal(err)
		}
		line, err := buf.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}

		fields := strings.Fields(line)
		for _, hf := range fieldsToCheck {
			i := hf.idx
			if i < 0 {
				i = len(fields) + i
			}
			// [2:] to skip \x
			bts, err := enc_hex.DecodeString(fields[i][2:])
			if err != nil {
				t.Fatal(err)
			}
			if err := protoutil.Unmarshal(bts, hf.msg); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNodeRangeSelection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	testData := []struct {
		args         []string
		wantIncluded []int
		wantExcluded []int
	}{
		{[]string{""}, []int{1, 200, 3}, nil},
		{[]string{"--nodes=1"}, []int{1}, []int{2, 3, 100}},
		{[]string{"--nodes=1,3"}, []int{1, 3}, []int{2, 4, 100}},
		{[]string{"--nodes=1-3"}, []int{1, 2, 3}, []int{4, 100}},
		{[]string{"--nodes=1-3,5"}, []int{1, 2, 3, 5}, []int{4, 100}},
		{[]string{"--nodes=1-3,5,10-20"}, []int{2, 5, 15}, []int{7}},
		{[]string{"--nodes=1,1-2"}, []int{1, 2}, []int{3, 100}},
		{[]string{"--nodes=1-3", "--exclude-nodes=2"}, []int{1, 3}, []int{2, 4, 100}},
		{[]string{"--nodes=1,3", "--exclude-nodes=3"}, []int{1}, []int{2, 3, 4, 100}},
		{[]string{"--exclude-nodes=2-7"}, []int{1, 8, 9, 10}, []int{2, 3, 4, 5, 6, 7}},
	}

	f := debugZipCmd.Flags()
	for _, tc := range testData {
		initCLIDefaults()
		if err := f.Parse(tc.args); err != nil {
			t.Fatalf("Parse(%#v) got unexpected error: %v", tc.args, err)
		}

		for _, wantIncluded := range tc.wantIncluded {
			assert.True(t, zipCtx.nodes.isIncluded(roachpb.NodeID(wantIncluded)))
		}
		for _, wantExcluded := range tc.wantExcluded {
			assert.False(t, zipCtx.nodes.isIncluded(roachpb.NodeID(wantExcluded)))
		}
	}
}

func TestZipJobTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestDoesNotWorkWithSharedProcessModeButWeDontKnowWhyYet(
			base.TestTenantProbabilistic, 112950,
		),
		Insecure: true,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer s.Stopper().Stop(context.Background())
	blockCh := make(chan struct{})
	defer jobs.TestingRegisterConstructor(jobspb.TypeImport,
		func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return jobstest.FakeResumer{
				OnResume: func(ctx context.Context) error {
					<-blockCh
					return nil
				},
			}
		}, jobs.UsesTenantCostControl)()
	runner := sqlutils.MakeSQLRunner(sqlDB)
	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()
	zipName := filepath.Join(dir, "test.zip")

	// Run a backup that completes, we should not see a trace for this job.
	runner.Exec(t, `CREATE TABLE foo (id INT)`)
	runner.Exec(t, `BACKUP TABLE foo INTO 'userfile:///completes'`)

	// Run a restore that completes, we should not see a trace for this job.
	runner.Exec(t, `CREATE DATABASE test`)
	runner.Exec(t, `RESTORE TABLE foo FROM LATEST IN 'userfile:///completes' WITH into_db = 'test'`)

	triggerJobAndWaitForRun := func(jobQuery string) jobspb.JobID {
		var jobID jobspb.JobID
		runner.QueryRow(t, jobQuery).Scan(&jobID)
		jobutils.WaitForJobToRun(t, runner, jobID)
		return jobID
	}
	sqlURL, cleanupFn := s.ApplicationLayer().PGUrl(t, serverutils.User(username.RootUser))
	defer cleanupFn()

	options := url.Values{}
	options.Add("sslmode", "disable")
	sqlURL.RawQuery = options.Encode()

	sqlConn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, sqlURL.String())
	defer func() {
		if err := sqlConn.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	var expectedFilesList strings.Builder
	var importJobID jobspb.JobID
	var importJobID2 jobspb.JobID
	func() {
		out, err := os.Create(zipName)
		if err != nil {
			t.Fatal(err)
		}
		z := newZipper(out)
		defer func() {
			if err := z.close(); err != nil {
				t.Fatal(err)
			}
		}()

		runner.Exec(t, `CREATE TABLE x (id INT PRIMARY KEY, n INT, s STRING)`)
		importJobID = triggerJobAndWaitForRun(`IMPORT INTO x CSV DATA ('workload:///csv/bank/bank?rows=100&version=1.0.0') WITH detached`)
		importJobID2 = triggerJobAndWaitForRun(`IMPORT INTO x CSV DATA ('workload:///csv/bank/bank?rows=100&version=1.0.0') WITH detached`)
		expectedFilesList.WriteString(fmt.Sprintf("/jobs/%d/.*/trace.zip\n", importJobID))
		expectedFilesList.WriteString(fmt.Sprintf("/jobs/%d/.*/trace.zip\n", importJobID2))

		zr := zipCtx.newZipReporter("test")
		zc := debugZipContext{
			z:                z,
			clusterPrinter:   zr,
			timeout:          3 * time.Second,
			firstNodeSQLConn: sqlConn,
		}
		if err := zc.dumpTraceableJobTraces(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	r, err := zip.OpenReader(zipName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	var fileList strings.Builder
	for _, f := range r.File {
		fmt.Fprintln(&fileList, f.Name)
	}
	require.Regexp(t, expectedFilesList.String(), fileList.String())
	close(blockCh)
	jobutils.WaitForJobToSucceed(t, runner, importJobID)
	jobutils.WaitForJobToSucceed(t, runner, importJobID2)
}

// This test the command flags values set during command execution.
func TestCommandFlags(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()
	c := NewCLITest(TestCLIParams{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.Cleanup()

	_, err := c.RunWithCapture("debug zip --concurrency=1 --cpu-profile-duration=0 --exclude-nodes=1" +
		" --redact --nodes=1 --exclude-files=*.log --include-goroutine-stacks --include-running-job-traces " + dir + "/debug.zip")
	if err != nil {
		t.Fatal(err)
	}

	r, err := zip.OpenReader(dir + "/debug.zip")
	if err != nil {
		t.Fatal(err)
	}

	for _, f := range r.File {
		if f.Name == "debug/debug_zip_command_flags.txt" {
			rc, err := f.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer rc.Close()

			actualFlags, err := io.ReadAll(rc)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, " --concurrency=1 --cpu-profile-duration=0s --exclude-files=[*.log] --exclude-nodes=1"+
				" --include-goroutine-stacks=true --include-running-job-traces=true --insecure=false --nodes=1 --redact=true",
				string(actualFlags))
			return
		}
	}
	assert.Fail(t, "debug/debug_zip_command_flags.txt is not generated")

	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
}

// This tests the operation of zip over excluded nodes.
func TestPartialZipForExcludedNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()
	c := NewCLITest(TestCLIParams{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.Cleanup()
	zipName := filepath.Join(dir, "debug.zip")

	// We want a low timeout so that the test doesn't take forever;
	// however low timeouts make race runs flaky with false positives.
	skip.UnderShort(t)
	skip.UnderRace(t)

	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)
	// Reduce the number of output log files to just what's expected.
	defer sc.SetupSingleFileLogging()()

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			Insecure:          true,
		},
	})
	defer tc.Stopper().Stop(ctx)

	_, err := c.RunWithCapture("debug zip --concurrency=1 --exclude-nodes=1 --cpu-profile-duration=0 " + dir + "/debug.zip")
	require.NoError(t, err)

	r, err := zip.OpenReader(zipName)
	defer func() { _ = r.Close() }()
	require.NoError(t, err)

	d, err := r.Open("debug/nodes/1")
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()

	require.True(t, oserror.IsNotExist(err), "node directory should not be present in the zip")
}

// This tests the operation of zip over excluded nodes.
func TestIncludeFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()
	c := NewCLITest(TestCLIParams{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.Cleanup()
	zipName := filepath.Join(dir, "debug.zip")

	// We want a low timeout so that the test doesn't take forever;
	// however low timeouts make race runs flaky with false positives.
	skip.UnderShort(t)
	skip.UnderRace(t)

	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)
	// Reduce the number of output log files to just what's expected.
	defer sc.SetupSingleFileLogging()()

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			Insecure:          true,
		},
	})
	defer tc.Stopper().Stop(ctx)

	_, err := c.RunWithCapture("debug zip --concurrency=1 --include-files=*.json --cpu-profile-duration=0 " + dir + "/debug.zip")
	require.NoError(t, err)

	r, err := zip.OpenReader(zipName)
	defer func() { _ = r.Close() }()
	require.NoError(t, err)

	for _, f := range r.File {
		fmt.Println(f.Name)
	}

	d, _ := r.Open("debug/nodes/1")
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
}

func trimNonDeterministicZipOutputFiles(out string) string {
	re := regexp.MustCompile(`(?m).*\.log$`)
	out = re.ReplaceAllString(out, `deterministic.log`)

	//Below files are generated non-deterministically on eng-flow.
	re = regexp.MustCompile(`(?m).*job_message\.txt$` + "\n")
	out = re.ReplaceAllString(out, ``)
	re = regexp.MustCompile(`(?m).*job_progress\.txt$` + "\n")
	out = re.ReplaceAllString(out, ``)
	re = regexp.MustCompile(`(?m).*job_progress_history\.txt$` + "\n")
	out = re.ReplaceAllString(out, ``)
	re = regexp.MustCompile(`(?m).*job_status\.txt$` + "\n")
	out = re.ReplaceAllString(out, ``)
	re = regexp.MustCompile(`(?m).*(memprof|memstats|memmonitoring).*\.(txt|pprof)$` + "\n")
	out = re.ReplaceAllString(out, ``)
	re = regexp.MustCompile(`(?m).*goroutine_dump.*\.txt\.gz$` + "\n")
	out = re.ReplaceAllString(out, ``)
	return out
}

func TestZipIncludeAndExcludeFilesDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, "testdata/zip/file-filters", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			// We want a low timeout so that the test doesn't take forever;
			// however low timeouts make race runs flaky with false positives.
			skip.UnderShort(t)
			skip.UnderRace(t)

			sc := log.ScopeWithoutShowLogs(t)
			defer sc.Close(t)
			// Reduce the number of output log files to just what's expected.
			defer sc.SetupSingleFileLogging()()

			ctx := context.Background()

			tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
					Insecure:          true,
				},
			})
			defer tc.Stopper().Stop(ctx)
			commandFlag := d.Cmd
			commandArgs := d.CmdArgs
			for _, arg := range commandArgs {
				commandFlag = commandFlag + " " + arg.String()
			}

			dir, cleanupFn := testutils.TempDir(t)
			defer cleanupFn()
			c := NewCLITest(TestCLIParams{
				StoreSpecs: []base.StoreSpec{{
					Path: dir,
				}},
			})
			defer c.Cleanup()

			zipName := filepath.Join(dir, "debug.zip")
			var command = "debug zip " + dir + "/debug.zip --concurrency=1 --cpu-profile-duration=1s " + commandFlag
			_, err := c.RunWithCapture(command)
			if err != nil {
				t.Fatal(err)
			}

			r, _ := zip.OpenReader(zipName)
			defer func() {
				if r != nil {
					_ = r.Close()
				}
			}()

			var fileLists []string
			for _, f := range r.File {
				fileLists = append(fileLists, f.Name)
			}
			sort.Strings(fileLists)
			fileList := strings.Join(fileLists, "\n")
			fileList = trimNonDeterministicZipOutputFiles(fileList)
			fmt.Println(fileList)

			err = r.Close()
			if err != nil {
				t.Fatal(err)
			}
			return fileList
		})
	})
}
