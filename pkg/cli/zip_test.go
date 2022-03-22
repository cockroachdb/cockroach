// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"archive/zip"
	"bytes"
	"context"
	enc_hex "encoding/hex"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
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
	'cluster_inflight_traces',
	'cross_db_references',
	'databases',
	'forward_dependencies',
	'index_columns',
	'lost_descriptors_with_data',
	'table_columns',
	'table_row_statistics',
	'ranges',
	'ranges_no_leases',
	'predefined_comments',
	'session_trace',
	'session_variables',
	'tables',
	'cluster_statement_statistics',
	'cluster_transaction_statistics',
	'statement_statistics',
	'transaction_statistics',
	'tenant_usage_details',
  'pg_catalog_table_is_implemented'
)
ORDER BY name ASC`)
	assert.NoError(t, err)

	var tables []string
	for rows.Next() {
		var table string
		assert.NoError(t, rows.Scan(&table))
		tables = append(tables, table)
	}
	tables = append(
		tables,
		"system.jobs",
		"system.descriptor",
		"system.namespace",
		"system.scheduled_jobs",
		"system.replication_constraint_stats",
		"system.replication_critical_localities",
		"system.replication_stats",
		"system.settings",
	)
	sort.Strings(tables)

	var exp []string
	exp = append(exp, debugZipTablesPerNode...)
	for _, t := range debugZipTablesPerCluster {
		t = strings.TrimPrefix(t, `"".`)
		exp = append(exp, t)
	}
	sort.Strings(exp)

	assert.Equal(t, exp, tables)
}

// This tests the operation of zip over secure clusters.
func TestZip(t *testing.T) {
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

	out, err := c.RunWithCapture("debug zip --concurrency=1 --cpu-profile-duration=1s " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages.
	out = eraseNonDeterministicZipOutput(out)

	// We use datadriven simply to read the golden output file; we don't actually
	// run any commands. Using datadriven allows TESTFLAGS=-rewrite.
	datadriven.RunTest(t, testutils.TestDataPath(t, "zip", "testzip"), func(t *testing.T, td *datadriven.TestData) string {
		return out
	})
}

// This tests the operation of zip running concurrently.
func TestConcurrentZip(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{ServerArgs: params})
	defer tc.Stopper().Stop(ctx)

	// Zip it. We fake a CLI test context for this.
	c := TestCLI{
		t:          t,
		TestServer: tc.Server(0).(*server.TestServer),
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

	// We use datadriven simply to read the golden output file; we don't actually
	// run any commands. Using datadriven allows TESTFLAGS=-rewrite.
	datadriven.RunTest(t, testutils.TestDataPath(t, "zip", "testzip_concurrent"), func(t *testing.T, td *datadriven.TestData) string {
		return out
	})
}

func TestZipSpecialNames(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	datadriven.RunTest(t, testutils.TestDataPath(t, "zip", "specialnames"),
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
	skip.WithIssue(t, 53306, "flaky test")

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
		TestingRequestFilter: func(ctx context.Context, _ roachpb.BatchRequest) *roachpb.Error {
			select {
			case <-unavailableCh.Load().(chan struct{}):
			case <-ctx.Done():
			}
			return nil
		},
	}

	// Make a 2-node cluster, with an option to make the first node unavailable.
	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {Insecure: true, Knobs: base.TestingKnobs{Store: knobs}},
			1: {Insecure: true},
		},
		ServerArgs: params,
	})
	defer tc.Stopper().Stop(context.Background())

	// Sanity test: check that a simple operation works.
	if _, err := tc.ServerConn(0).Exec("SELECT * FROM system.users"); err != nil {
		t.Fatal(err)
	}

	// Make the first two nodes unavailable.
	ch := make(chan struct{})
	unavailableCh.Store(ch)
	defer close(ch)

	// Zip it. We fake a CLI test context for this.
	c := TestCLI{
		t:          t,
		TestServer: tc.Server(0).(*server.TestServer),
	}
	defer func(prevStderr *os.File) { stderr = prevStderr }(stderr)
	stderr = os.Stdout

	// Keep the timeout short so that the test doesn't take forever.
	out, err := c.RunWithCapture("debug zip --concurrency=1 --cpu-profile-duration=0 " + os.DevNull + " --timeout=.5s")
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages.
	out = eraseNonDeterministicZipOutput(out)

	// In order to avoid non-determinism here, we erase the output of
	// the range retrieval.
	re := regexp.MustCompile(`(?m)^(requesting ranges.*found|writing: debug/nodes/\d+/ranges).*\n`)
	out = re.ReplaceAllString(out, ``)

	datadriven.RunTest(t, testutils.TestDataPath(t, "zip", "unavailable"),
		func(t *testing.T, td *datadriven.TestData) string {
			return out
		})
}

func eraseNonDeterministicZipOutput(out string) string {
	re := regexp.MustCompile(`(?m)postgresql://.*$`)
	out = re.ReplaceAllString(out, `postgresql://...`)
	re = regexp.MustCompile(`(?m)SQL address: .*$`)
	out = re.ReplaceAllString(out, `SQL address: ...`)
	re = regexp.MustCompile(`(?m)log file.*$`)
	out = re.ReplaceAllString(out, `log file ...`)
	re = regexp.MustCompile(`(?m)RPC connection to .*$`)
	out = re.ReplaceAllString(out, `RPC connection to ...`)
	re = regexp.MustCompile(`(?m)dial tcp .*$`)
	out = re.ReplaceAllString(out, `dial tcp ...`)
	re = regexp.MustCompile(`(?m)rpc error: .*$`)
	out = re.ReplaceAllString(out, `rpc error: ...`)

	// The number of memory profiles previously collected is not deterministic.
	re = regexp.MustCompile(`(?m)^\[node \d+\] \d+ heap profiles found$`)
	out = re.ReplaceAllString(out, `[node ?] ? heap profiles found`)
	re = regexp.MustCompile(`(?m)^\[node \d+\] retrieving (memprof|memstats).*$` + "\n")
	out = re.ReplaceAllString(out, ``)
	re = regexp.MustCompile(`(?m)^\[node \d+\] writing profile.*$` + "\n")
	out = re.ReplaceAllString(out, ``)

	//out = strings.ReplaceAll(out, "\n\n", "\n")
	return out
}

// This tests the operation of zip over partial clusters.
//
// We cannot combine this test with TestZip above because TestPartialZip
// needs a TestCluster, the TestCluster hides its SSL certs, and we
// need the SSL certs dir to run a CLI test securely.
func TestPartialZip(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{ServerArgs: params})
	defer tc.Stopper().Stop(ctx)

	// Switch off the second node.
	tc.StopServer(1)

	// Zip it. We fake a CLI test context for this.
	c := TestCLI{
		t:          t,
		TestServer: tc.Server(0).(*server.TestServer),
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

	datadriven.RunTest(t, testutils.TestDataPath(t, "zip", "partial1"),
		func(t *testing.T, td *datadriven.TestData) string {
			return out
		})

	// Now do it again and exclude the down node explicitly.
	out, err = c.RunWithCapture("debug zip " + os.DevNull + " --concurrency=1 --exclude-nodes=2 --cpu-profile-duration=0")
	if err != nil {
		t.Fatal(err)
	}

	out = eraseNonDeterministicZipOutput(out)
	datadriven.RunTest(t, testutils.TestDataPath(t, "zip", "partial1_excluded"),
		func(t *testing.T, td *datadriven.TestData) string {
			return out
		})

	// Now mark the stopped node as decommissioned, and check that zip
	// skips over it automatically. We specifically use --wait=none because
	// we're decommissioning a node in a 3-node cluster, so there's no node to
	// up-replicate the under-replicated ranges to.
	{
		_, err := c.RunWithCapture(fmt.Sprintf("node decommission --wait=none %d", 2))
		if err != nil {
			t.Fatal(err)
		}
	}

	// We use .Override() here instead of SET CLUSTER SETTING in SQL to
	// override the 1m15s minimum placed on the cluster setting. There
	// is no risk to see the override bumped due to a gossip update
	// because this setting is not otherwise set in the test cluster.
	s := tc.Server(0)
	kvserver.TimeUntilStoreDead.Override(ctx, &s.ClusterSettings().SV, kvserver.TestTimeUntilStoreDead)

	// This last case may take a little while to converge. To make this work with datadriven and at the same
	// time retain the ability to use the `-rewrite` flag, we use a retry loop within that already checks the
	// output ahead of time and retries for some time if necessary.
	datadriven.RunTest(t, testutils.TestDataPath(t, "zip", "partial2"),
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

	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	s, _, _ := serverutils.StartServer(t, params)
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

		sqlURL := url.URL{
			Scheme:   "postgres",
			User:     url.User(security.RootUser),
			Host:     s.ServingSQLAddr(),
			RawQuery: "sslmode=disable",
		}
		sqlConn := sqlConnCtx.MakeSQLConn(ioutil.Discard, ioutil.Discard, sqlURL.String())
		defer func() {
			if err := sqlConn.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		zr := zipCtx.newZipReporter("test")
		zc := debugZipContext{
			z:       z,
			timeout: 3 * time.Second,
		}
		if err := zc.dumpTableDataForZip(
			zr,
			sqlConn,
			"test",
			`generate_series(1,15000) as t(x)`,
			`select if(x<11000,x,crdb_internal.force_retry('1h')) from generate_series(1,15000) as t(x)`,
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
	const expected = `test/generate_series(1,15000) as t(x).txt
test/generate_series(1,15000) as t(x).txt.err.txt
test/generate_series(1,15000) as t(x).1.txt
test/generate_series(1,15000) as t(x).1.txt.err.txt
test/generate_series(1,15000) as t(x).2.txt
test/generate_series(1,15000) as t(x).2.txt.err.txt
test/generate_series(1,15000) as t(x).3.txt
test/generate_series(1,15000) as t(x).3.txt.err.txt
test/generate_series(1,15000) as t(x).4.txt
test/generate_series(1,15000) as t(x).4.txt.err.txt
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
	// fields is not alway s precise as there can be spaces in the fields but the
	// hex fields are always in the end of the row and they don't contain spaces.
	hexFiles := map[string][]hexField{
		"debug/system.jobs.txt": {
			{idx: -2, msg: &jobspb.Payload{}},
			{idx: -1, msg: &jobspb.Progress{}},
		},
		"debug/system.descriptor.txt": {
			{idx: 2, msg: &descpb.Descriptor{}},
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
			bts, err := enc_hex.DecodeString(fields[i])
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
