// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package explain_test

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestOutputBuilder(t *testing.T) {
	example := func(flags explain.Flags) *explain.OutputBuilder {
		ob := explain.NewOutputBuilder(flags)
		ob.AddField("distributed", "true")
		ob.EnterMetaNode("meta")
		{
			ob.EnterNode(
				"render",
				colinfo.ResultColumns{{Name: "a", Typ: types.Int}, {Name: "b", Typ: types.String}},
				colinfo.ColumnOrdering{
					{ColIdx: 0, Direction: encoding.Ascending},
					{ColIdx: 1, Direction: encoding.Descending},
				},
			)
			ob.AddField("render 0", "foo")
			ob.AddField("render 1", "bar")
			{
				ob.EnterNode("join", colinfo.ResultColumns{{Name: "x", Typ: types.Int}}, nil)
				ob.AddField("type", "outer")
				{
					{
						ob.EnterNode("scan", colinfo.ResultColumns{{Name: "x", Typ: types.Int}}, nil)
						ob.AddField("table", "foo")
						ob.LeaveNode()
					}
					{
						ob.EnterNode("scan", nil, nil) // Columns should show up as "()".
						ob.AddField("table", "bar")
						ob.LeaveNode()
					}
				}
				ob.LeaveNode()
			}
			ob.LeaveNode()
		}
		ob.LeaveNode()
		return ob
	}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "output"), func(t *testing.T, d *datadriven.TestData) string {
		var flags explain.Flags
		for _, arg := range d.CmdArgs {
			switch arg.Key {
			case "verbose":
				flags.Verbose = true
			case "types":
				flags.Verbose = true
				flags.ShowTypes = true
			default:
				panic(fmt.Sprintf("unknown argument %s", arg.Key))
			}
		}
		ob := example(flags)
		switch d.Cmd {
		case "string":
			return ob.BuildString()

		case "tree":
			treeYaml, err := yaml.Marshal(ob.BuildProtoTree())
			if err != nil {
				panic(err)
			}
			return string(treeYaml)

		default:
			panic(fmt.Sprintf("unknown command %s", d.Cmd))
		}
	})
}

func TestEmptyOutputBuilder(t *testing.T) {
	ob := explain.NewOutputBuilder(explain.Flags{Verbose: true})
	if str := ob.BuildString(); str != "" {
		t.Errorf("expected empty string, got '%s'", str)
	}
	if rows := ob.BuildStringRows(); len(rows) != 0 {
		t.Errorf("expected no rows, got %v", rows)
	}
}

func TestMaxDiskSpillUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, "CREATE TABLE t (a PRIMARY KEY, b) AS SELECT i, i FROM generate_series(1, 10) AS g(i)")

	maxDiskUsageRE := regexp.MustCompile(`max sql temp disk usage: (\d+)`)
	queryMatchRE := func(query string, re *regexp.Regexp) bool {
		rows, err := conn.QueryContext(ctx, query)
		assert.NoError(t, err)
		for rows.Next() {
			var res string
			assert.NoError(t, rows.Scan(&res))
			if matches := re.FindStringSubmatch(res); len(matches) > 0 {
				return true
			}
		}
		return false
	}

	// Use very low workmem limit so that the disk spilling happens.
	sqlDB.Exec(t, "SET distsql_workmem = '2B';")
	// knob above.
	assert.True(t, queryMatchRE(`EXPLAIN ANALYZE (VERBOSE, DISTSQL) select * from t join t AS x on t.b=x.a`, maxDiskUsageRE), "didn't find max sql temp disk usage: in explain")
	assert.False(t, queryMatchRE(`EXPLAIN ANALYZE (VERBOSE, DISTSQL) select * from t `, maxDiskUsageRE), "found unexpected max sql temp disk usage: in explain")

}

func TestCPUTimeEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, "multinode cluster setup times out under stress")
	skip.UnderRace(t, "multinode cluster setup times out under race")

	if !grunning.Supported {
		return
	}

	const numNodes = 3
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	if tc.DefaultTenantDeploymentMode().IsExternal() {
		tc.GrantTenantCapabilities(ctx, t, serverutils.TestTenantID(),
			map[tenantcapabilitiespb.ID]string{tenantcapabilitiespb.CanAdminRelocateRange: "true"})
	}

	db := sqlutils.MakeSQLRunner(tc.Conns[0])

	runQuery := func(query string, hideCPU bool) {
		rows := db.QueryStr(t, "EXPLAIN ANALYZE "+query)
		var err error
		var foundCPU bool
		var cpuTime time.Duration
		for _, row := range rows {
			if len(row) != 1 {
				t.Fatalf("expected one column")
			}
			if strings.Contains(row[0], "sql cpu time") {
				foundCPU = true
				cpuStr := strings.Split(row[0], " ")
				require.Equal(t, len(cpuStr), 4)
				cpuTime, err = time.ParseDuration(cpuStr[3])
				require.NoError(t, err)
				break
			}
		}
		if hideCPU {
			require.Falsef(t, foundCPU, "expected not to output CPU time for query: %s", query)
		} else {
			require.NotZerof(t, cpuTime, "expected nonzero CPU time for query: %s", query)
		}
	}

	// Mutation queries shouldn't output CPU time.
	runQuery("CREATE TABLE t (x INT PRIMARY KEY, y INT);", true /* hideCPU */)
	runQuery("INSERT INTO t (SELECT t, t%127 FROM generate_series(1, 10000) g(t));", true /* hideCPU */)

	// Split the table across the nodes in order to make the following test cases
	// more interesting.
	for _, stmt := range []string{
		`ALTER TABLE t SPLIT AT VALUES (2500)`,
		`ALTER TABLE t SPLIT AT VALUES (5000)`,
		`ALTER TABLE t SPLIT AT VALUES (7500)`,
		`ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1], 2500)`,
		`ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[2], 5000)`,
		`ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[3], 7500)`,
	} {
		testutils.SucceedsSoon(t, func() error {
			_, err := db.DB.ExecContext(ctx, stmt)
			return err
		})
	}

	runQuery("SELECT * FROM t;", false /* hideCPU */)
	runQuery("SELECT count(*) FROM t;", false /* hideCPU */)
	runQuery("SELECT * FROM (SELECT * FROM t WHERE x > 2000 AND x < 3000) s1 JOIN t ON s1.x = t.x", false /* hideCPU */)
	runQuery("SELECT * FROM (VALUES (1), (2), (3)) v(a) INNER LOOKUP JOIN t ON a = x", false /* hideCPU */)
	runQuery("SELECT count(*) FROM generate_series(1, 100000)", false /* hideCPU */)
}

// TestContentionTimeOnWrites verifies that the contention encountered during a
// mutation is reported on EXPLAIN ANALYZE output.
func TestContentionTimeOnWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(conn)
	runner.Exec(t, "CREATE TABLE t (k INT PRIMARY KEY, v INT)")

	// The test involves three goroutines:
	// - the main goroutine acts as the coordinator. It first waits for worker 1
	//   to perform its mutation in an open txn, then blocks until worker 2
	//   begins executing its mutation, then unblocks worker 1 and waits for
	//   both workers to exit.
	// - worker 1 goroutine performs a mutation without committing a txn. It
	//   notifies the main goroutine by closing 'sem' once the mutation has been
	//   performed. It then blocks until 'commitCh' is closed by the main
	//   goroutine which allows worker 2 to experience contention.
	// - worker 2 goroutine performs a mutation via EXPLAIN ANALYZE. This query
	//   will be blocked until worker 1 commits its txn, so it should see
	//   contention time reported in the output.

	sem := make(chan struct{})
	errCh := make(chan error, 1)
	commitCh := make(chan struct{})
	go func() {
		defer close(errCh)
		// Ensure that sem is always closed (in case we encounter an error
		// before the mutation is performed).
		var closedSem bool
		defer func() {
			if !closedSem {
				close(sem)
			}
		}()
		txn, err := conn.Begin()
		if err != nil {
			errCh <- err
			return
		}
		_, err = txn.Exec("INSERT INTO t VALUES (1, 1)")
		if err != nil {
			errCh <- err
			return
		}
		// Notify the main goroutine that the mutation has been performed.
		close(sem)
		closedSem = true
		// Block until the main goroutine tells us that we're good to commit.
		<-commitCh
		if err = txn.Commit(); err != nil {
			errCh <- err
			return
		}
	}()

	// Block until the mutation of worker 1 is done.
	<-sem
	// Check that no error was encountered before that.
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	var foundContention, foundLockWait, foundLatchWait bool
	errCh2 := make(chan error, 1)
	go func() {
		defer close(errCh2)
		// Execute the mutation via EXPLAIN ANALYZE and check whether the
		// contention is reported.
		contentionRE := regexp.MustCompile(`cumulative time spent due to contention.*`)
		lockRE := regexp.MustCompile(`cumulative time spent in the lock table`)
		latchRE := regexp.MustCompile(`cumulative time spent waiting to acquire latches`)
		rows := runner.Query(t, "EXPLAIN ANALYZE UPSERT INTO t VALUES (1, 2)")
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				errCh2 <- err
				return
			}
			foundContention = foundContention || contentionRE.MatchString(line)
			foundLockWait = foundLockWait || lockRE.MatchString(line)
			foundLatchWait = foundLatchWait || latchRE.MatchString(line)
		}
	}()

	// Continuously poll the cluster queries until we see that the query that
	// should be experiencing contention has started executing.
	for {
		row := runner.QueryRow(t, "SELECT count(*) FROM [SHOW CLUSTER QUERIES] WHERE query LIKE '%EXPLAIN ANALYZE UPSERT%'")
		var count int
		row.Scan(&count)
		// Sleep for non-trivial amount of time to allow for worker 2 to start
		// (if it hasn't already) and to experience the contention (if it has
		// started).
		time.Sleep(time.Second)
		if count == 2 {
			// We stop polling once we see 2 queries matching the LIKE pattern:
			// the mutation query from worker 2 and the polling query itself.
			break
		}
	}

	// Allow worker 1 to commit which should unblock both workers.
	close(commitCh)

	// Wait for both workers to exit. Also perform sanity checks that the
	// workers didn't run into any errors.
	err := <-errCh
	require.NoError(t, err)
	err = <-errCh2
	require.NoError(t, err)

	// Meat of the test - verify that the contention was reported.
	require.True(t, foundContention)

	// Verify that either lock or latch wait time was reported. The contention
	// time is (usually) the sum of these two.
	require.True(t, foundLockWait || foundLatchWait)
}

func TestRetryFields(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, "CREATE SEQUENCE s")

	retryCountRE := regexp.MustCompile(`number of transaction retries: (\d+)`)
	retryTimeRE := regexp.MustCompile(`time spent retrying the transaction: ([\d\.]+)[µsm]+`)

	const query = "EXPLAIN ANALYZE SELECT IF(nextval('s')<=3, crdb_internal.force_retry('1h'::INTERVAL), 0)"
	rows, err := conn.QueryContext(ctx, query)
	assert.NoError(t, err)
	var output strings.Builder
	var foundCount, foundTime bool
	for rows.Next() {
		var res string
		assert.NoError(t, rows.Scan(&res))
		output.WriteString(res)
		output.WriteString("\n")
		if matches := retryCountRE.FindStringSubmatch(res); len(matches) > 0 {
			foundCount = true
		}
		if matches := retryTimeRE.FindStringSubmatch(res); len(matches) > 0 {
			foundTime = true
		}
	}
	assert.Truef(t, foundCount, "expected to find transaction retries, full output:\n\n%s", output.String())
	assert.Truef(t, foundTime, "expected to find time spent retrying, full output:\n\n%s", output.String())
}

// TestMaximumMemoryUsage verifies that "maximum memory usage" statistic is
// reported correctly in distributed plans. It is a regression test for #143617.
func TestMaximumMemoryUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, "multinode cluster setup times out under stress")
	skip.UnderRace(t, "multinode cluster setup times out under race")

	const numNodes = 3
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	if tc.DefaultTenantDeploymentMode().IsExternal() {
		tc.GrantTenantCapabilities(ctx, t, serverutils.TestTenantID(),
			map[tenantcapabilitiespb.ID]string{tenantcapabilitiespb.CanAdminRelocateRange: "true"})
	}

	// Set up such a distributed plan where memory-intensive aggregation occurs
	// on the remote nodes whereas the gateway only merges streams of final
	// results from remote nodes.
	db := sqlutils.MakeSQLRunner(tc.Conns[0])
	db.Query(t, "CREATE TABLE t (k INT PRIMARY KEY, bucket INT, v STRING);")
	db.Query(t, "INSERT INTO t SELECT i, i % 4, repeat('a', 1000) FROM generate_series(1, 10000) AS g(i);")
	db.Query(t, "ALTER TABLE t SPLIT AT VALUES (5001);")
	testutils.SucceedsSoon(t, func() error {
		// Wrap this query in a retry loop since it might hit expected errors
		// for some time.
		_, err := db.DB.ExecContext(ctx, "ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[2], 1), (ARRAY[3], 5001)")
		return err
	})

	rows := db.QueryStr(t, "EXPLAIN ANALYZE SELECT max(v) FROM t GROUP BY bucket;")
	var output strings.Builder
	maxMemoryRE := regexp.MustCompile(`maximum memory usage: ([\d\.]+) MiB`)
	var maxMemoryUsage float64
	for _, row := range rows {
		output.WriteString(row[0])
		output.WriteString("\n")
		s := strings.TrimSpace(row[0])
		if matches := maxMemoryRE.FindStringSubmatch(s); len(matches) > 0 {
			var err error
			maxMemoryUsage, err = strconv.ParseFloat(matches[1], 64)
			require.NoError(t, err)
		}
	}
	require.Greaterf(t, maxMemoryUsage, 5.0, "expected maximum memory usage to be at least 5 MiB, full output:\n\n%s", output.String())
}
