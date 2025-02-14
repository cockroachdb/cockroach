// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
)

func TestTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRace(t, "does too much work under race, see: "+
		"https://github.com/cockroachdb/cockroach/pull/56343#issuecomment-733577377")

	defer log.Scope(t).Close(t)

	// These are always appended, even without the test specifying it.
	alwaysOptionalSpans := []string{
		"drain",
		"pendingLeaseRequest: requesting lease",
		"outbox",
		"request range lease",
		"range lookup",
		"local proposal",
		"admissionWorkQueueWait",
		"index recommendation",
	}
	// Depending on whether the data is local or not, we may not see these
	// spans. Only applicable with distsql=on.
	distsqlOptionalSpans := []string{
		"setup-flow-async",
		"/cockroach.sql.distsqlrun.DistSQL/SetupFlow",
		"/cockroach.sql.distsqlrun.DistSQL/FlowStream",
		"noop",
	}
	nonVectorizedExpSpans := []string{
		"session recording",
		"sql txn",
		"sql query",
		"optimizer",
		"flow",
		"table reader",
		"consuming rows",
		"txn coordinator send",
		"dist sender send",
		"/cockroach.roachpb.Internal/Batch",
		"commit sql txn",
	}
	vectorizedExpSpans := []string{
		"session recording",
		"sql txn",
		"sql query",
		"optimizer",
		"flow",
		"batch flow coordinator",
		"colbatchscan",
		"consuming rows",
		"txn coordinator send",
		"dist sender send",
		"/cockroach.roachpb.Internal/Batch",
		"commit sql txn",
	}

	getRows := func(t *testing.T, sqlDB *gosql.DB, distsql, vectorize string, useShowTraceFor bool) (*gosql.Rows, string, error) {
		if _, err := sqlDB.Exec(fmt.Sprintf("SET distsql = %s", distsql)); err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec(fmt.Sprintf("SET vectorize = %s", vectorize)); err != nil {
			t.Fatal(err)
		}
		if vectorize == "on" {
			// Disable the direct columnar scans to make the vectorized planning
			// deterministic.
			if _, err := sqlDB.Exec(`SET direct_columnar_scans_enabled = false`); err != nil {
				t.Fatal(err)
			}
		}

		// Run some query with tracing enabled.
		if _, err := sqlDB.Exec("SET tracing = on; SELECT * FROM test.foo; SET tracing = off"); err != nil {
			t.Fatal(err)
		}

		// Get the full trace to be used if the test fails.
		rows, err := sqlDB.Query("SELECT message FROM crdb_internal.session_trace")
		if err != nil {
			t.Fatal(err)
		}
		var trace strings.Builder
		if err = func() error {
			for rows.Next() {
				var msg string
				if err := rows.Scan(&msg); err != nil {
					return err
				}
				fmt.Fprintln(&trace, msg)
			}
			return rows.Close()
		}(); err != nil {
			t.Fatal(err)
		}
		if trace.Len() == 0 {
			t.Fatalf("empty trace")
		}
		// Check that execution stats collected during the above SELECT
		// statement are output via the ComponentStats payload.
		if !strings.Contains(trace.String(), "ComponentStats") {
			t.Fatalf("no stat messages found")
		}

		if useShowTraceFor {
			rows, err = sqlDB.Query(
				"SELECT DISTINCT operation AS op FROM [SHOW TRACE FOR SESSION] " +
					"WHERE operation IS NOT NULL ORDER BY op")
			return rows, trace.String(), err
		}
		rows, err = sqlDB.Query(
			"SELECT DISTINCT operation AS op FROM crdb_internal.session_trace " +
				"WHERE operation IS NOT NULL ORDER BY op")
		return rows, trace.String(), err
	}

	testData := []struct {
		name            string
		distSQL         string
		vectorize       string
		useShowTraceFor bool
	}{
		{
			name:            "Session",
			distSQL:         "off",
			vectorize:       "off",
			useShowTraceFor: false,
		},
		{
			name:            "SessionDistSQL",
			distSQL:         "on",
			vectorize:       "off",
			useShowTraceFor: false,
		},
		{
			name:            "ShowTraceFor",
			distSQL:         "off",
			vectorize:       "off",
			useShowTraceFor: true,
		},
		{
			name:            "ShowTraceForDistSQL",
			distSQL:         "on",
			vectorize:       "off",
			useShowTraceFor: true,
		},
		{
			name:            "ShowTraceForVectorized",
			distSQL:         "off",
			vectorize:       "on",
			useShowTraceFor: true,
		},
	}

	// Create a cluster. We'll run sub-tests using each node of this cluster.
	const numNodes = 3
	cluster := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{})
	defer cluster.Stopper().Stop(context.Background())

	clusterDB := cluster.ServerConn(0)
	if _, err := clusterDB.Exec(`CREATE DATABASE test;`); err != nil {
		t.Fatal(err)
	}
	if _, err := clusterDB.Exec(`
		--- test.foo is a single range table.
		CREATE TABLE test.foo (id INT PRIMARY KEY);
		--- test.bar is a multi-range table.
		CREATE TABLE test.bar (id INT PRIMARY KEY);`); err != nil {
		t.Fatal(err)
	}
	if _, err := clusterDB.Exec(`ALTER TABLE  test.bar SPLIT AT VALUES (5);`); err != nil {
		t.Fatal(err)
	}

	for _, test := range testData {
		optionalSpans := append([]string{}, alwaysOptionalSpans...)
		if test.distSQL == "on" {
			optionalSpans = append(optionalSpans, distsqlOptionalSpans...)
		}
		expSpans := nonVectorizedExpSpans
		if test.vectorize == "on" {
			expSpans = vectorizedExpSpans
		}
		sort.Strings(expSpans)

		t.Run(test.name, func(t *testing.T) {
			// Session tracing needs to work regardless of whether tracing is enabled, so
			// we're going to test both cases.
			//
			// We'll also check traces from all nodes. The point is to be sure that we
			// test a node that is different than the leaseholder for the range, so that
			// the trace contains remote spans.
			for _, enableTr := range []bool{false, true} {
				name := "TracingOff"
				if enableTr {
					name = "TracingOn"
				}
				t.Run(name, func(t *testing.T) {
					for i := 0; i < numNodes; i++ {
						t.Run(fmt.Sprintf("node-%d", i), func(t *testing.T) {
							// Use a new "session" for each sub-test rather than
							// cluster.ServerConn() so that per-session state has a known
							// value. This is necessary for the check below that the
							// session_trace starts empty.
							//
							// TODO(andrei): Pull the check for an empty session_trace out of
							// the sub-tests so we can use cluster.ServerConn(i) here.
							pgURL, cleanup := pgurlutils.PGUrl(
								t, cluster.Server(i).AdvSQLAddr(), "TestTrace", url.User(username.RootUser))
							defer cleanup()
							q := pgURL.Query()
							// This makes it easier to test with the `tracing` sesssion var.
							q.Add("enable_implicit_transaction_for_batch_statements", "false")
							pgURL.RawQuery = q.Encode()
							sqlDB, err := gosql.Open("postgres", pgURL.String())
							if err != nil {
								t.Fatal(err)
							}
							defer sqlDB.Close()

							sqlDB.SetMaxOpenConns(1)

							// Run a non-traced read to acquire a lease on the table, so that the
							// traced read below doesn't need to take a lease. Tracing a lease
							// acquisition incurs some spans that are too fragile to test here.
							if _, err := sqlDB.Exec(`SELECT * FROM test.foo LIMIT 1`); err != nil {
								t.Fatal(err)
							}

							if _, err := cluster.ServerConn(0).Exec(
								fmt.Sprintf(`SET CLUSTER SETTING trace.debug_http_endpoint.enabled = %t`, enableTr),
							); err != nil {
								t.Fatal(err)
							}

							// Sanity check that new sessions don't have trace info on them.
							row := sqlDB.QueryRow("SELECT count(1) FROM crdb_internal.session_trace")
							var count int
							if err := row.Scan(&count); err != nil {
								t.Fatal(err)
							}
							if count != 0 {
								t.Fatalf("expected crdb_internal.session_trace to be empty "+
									"at the beginning of a session, but it wasn't. Count: %d.", count)
							}

							rows, trace, err := getRows(t, sqlDB, test.distSQL, test.vectorize, test.useShowTraceFor)
							if err != nil {
								t.Fatal(err)
							}
							defer rows.Close()

							ignoreSpan := func(op string) bool {
								for _, s := range optionalSpans {
									if strings.Contains(op, s) {
										return true
									}
								}
								return false
							}

							r := 0
							for rows.Next() {
								var op string
								if err := rows.Scan(&op); err != nil {
									t.Fatal(err)
								}
								if ignoreSpan(op) {
									continue
								}

								if r >= len(expSpans) {
									t.Fatalf("extra span: %s\n\n%s", op, trace)
								} else if op != expSpans[r] {
									t.Fatalf("expected span: %q, got: %q\n\n%s", expSpans[r], op, trace)
								}
								r++
							}
							if r < len(expSpans) {
								t.Fatalf("missing expected spans: %s\n\n%s", expSpans[r:], trace)
							}
						})
					}
				})
			}
		})
	}
}

// TestTraceFieldDecomposition checks that SHOW TRACE is able to decompose
// the parts of a trace/log message into different columns properly.
func TestTraceFieldDecomposition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	query := "SELECT 42"

	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				BeforeExecute: func(ctx context.Context, stmt string, descriptors *descs.Collection) {
					if strings.Contains(stmt, query) {
						// We need to check a tag containing brackets (e.g. an
						// IPv6 address).  See #18558.
						taggedCtx := logtags.AddTag(ctx, "hello", "[::666]")
						// We use log.Infof here (instead of log.Event) to ensure
						// the trace message contains also a file name prefix. See
						// #19453/#20085.
						log.Infof(taggedCtx, "world")
					}
				},
			},
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	sqlDB.SetMaxOpenConns(1)

	if _, err := sqlDB.Exec("SET tracing = ON"); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(query); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec("SET tracing = OFF"); err != nil {
		t.Fatal(err)
	}

	t.Run("SHOW TRACE", func(t *testing.T) {
		rows, err := sqlDB.Query(`SELECT message, tag, location FROM [SHOW TRACE FOR SESSION]`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		ok := false
		for rows.Next() {
			var msg, ct, loc []byte
			if err := rows.Scan(&msg, &ct, &loc); err != nil {
				t.Fatal(err)
			}
			t.Logf("received trace: %q // %q // %q", msg, ct, loc)
			// Check that brackets are properly balanced.
			if len(ct) > 0 && ct[0] == '[' {
				if ct[len(ct)-1] != ']' {
					t.Errorf("tag starts with open bracket but does not close it: %q", ct)
				}
			}
			c1 := strings.Count(string(ct), "[")
			c2 := strings.Count(string(ct), "]")
			if c1 != c2 {
				t.Errorf("mismatched brackets: %q", ct)
			}
			// Check that the expected message was received.
			if string(msg) == "world" &&
				strings.Contains(string(ct), "hello=[::666]") &&
				strings.Contains(string(loc), ".go") {
				ok = true
			}
			// Check that the fields don't have heading or trailing whitespaces.
			for _, b := range [][]byte{msg, ct, loc} {
				if len(b) > 0 && (b[0] == ' ' || b[len(b)-1] == ' ') {
					t.Errorf("unexpected whitespace: %q", b)
				}
			}
		}
		if !ok {
			t.Fatal("expected message not found in trace")
		}
	})

	t.Run("SHOW COMPACT TRACE", func(t *testing.T) {
		rows, err := sqlDB.Query(`SELECT message, tag FROM [SHOW COMPACT TRACE FOR SESSION];`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		ok := false
		for rows.Next() {
			var msg, ct []byte
			if err := rows.Scan(&msg, &ct); err != nil {
				t.Fatal(err)
			}
			t.Logf("received trace: %q // %q", msg, ct)
			// Check that brackets are properly balanced.
			if len(ct) > 0 && ct[0] == '[' {
				if ct[len(ct)-1] != ']' {
					t.Errorf("tag starts with open bracket but does not close it: %q", ct)
				}
			}
			c1 := strings.Count(string(ct), "[")
			c2 := strings.Count(string(ct), "]")
			if c1 != c2 {
				t.Errorf("mismatched brackets: %q", ct)
			}
			// Check that the expected message was received.
			if strings.HasSuffix(string(msg), " world") &&
				strings.Contains(string(ct), "hello=[::666]") &&
				strings.Contains(string(msg), ".go") {
				ok = true
			}
			// Check that the fields don't have heading or trailing whitespaces.
			for _, b := range [][]byte{msg, ct} {
				if len(b) > 0 && (b[0] == ' ' || b[len(b)-1] == ' ') {
					t.Errorf("unexpected whitespace: %q", b)
				}
			}
		}
		if !ok {
			t.Fatal("expected message not found in trace")
		}
	})

}

func TestKVTraceWithCountStar(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test that we don't crash if we try to do a KV trace
	// on a COUNT(*) query (#19846).
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, "CREATE DATABASE test")
	r.Exec(t, "CREATE TABLE test.a (a INT PRIMARY KEY, b INT)")
	r.Exec(t, "INSERT INTO test.a VALUES (1,1), (2,2)")
	r.Exec(t, "SET tracing = on,kv; SELECT count(*) FROM test.a; SET tracing = off")
}

func TestKVTraceDistSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test that kv tracing works in distsql.
	const numNodes = 2
	cluster := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			UseDatabase: "test",
		},
	})
	defer cluster.Stopper().Stop(context.Background())

	r := sqlutils.MakeSQLRunner(cluster.ServerConn(0))
	r.Exec(t, "CREATE DATABASE test")
	r.Exec(t, "CREATE TABLE test.a (a INT PRIMARY KEY, b INT)")
	r.Exec(t, "INSERT INTO test.a VALUES (1,1), (2,2)")
	r.Exec(t, "ALTER TABLE a SPLIT AT VALUES(1)")
	r.Exec(t, "SET tracing = on,kv; SELECT count(*) FROM test.a; SET tracing = off")

	for node := 0; node < 2; node++ {
		rows := r.Query(t,
			fmt.Sprintf(`SELECT tag
			 FROM [SHOW KV TRACE FOR SESSION]
			 WHERE tag LIKE '%%%d%%'`, node))
		if !rows.Next() {
			t.Fatalf("no message from n%d found", node)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}

	rows := r.Query(t, `SELECT *
           FROM [SHOW KV TRACE FOR SESSION]
           WHERE MESSAGE LIKE '%fetched: %'`)
	if !rows.Next() {
		t.Fatal("No kv messages found")
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
}

// Test that tracing works with DistSQL. Namely, test that traces for processors
// running remotely are collected.
func TestTraceDistSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	countStmt := "SELECT count(1) FROM test.a"
	recCh := make(chan tracingpb.Recording, 2)

	const numNodes = 2
	cluster := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			UseDatabase: "test",
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					WithStatementTrace: func(trace tracingpb.Recording, stmt string) {
						if stmt == countStmt {
							recCh <- trace
						}
					},
				},
			},
		},
	})
	defer cluster.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(cluster.ServerConn(0))
	// TODO(yuzefovich): tracing in the vectorized engine is very limited since
	// only wrapped processors and the materializers use it outside of the
	// stats information propagation. We should fix that (#55821).
	r.Exec(t, "SET vectorize=off")
	r.Exec(t, "CREATE DATABASE test")
	r.Exec(t, "CREATE TABLE test.a (a INT PRIMARY KEY)")
	// Put the table on the 2nd node so that the flow is planned on the 2nd node
	// and the spans corresponding to the processors travel through DistSQL
	// producer metadata to the gateway.
	r.Exec(t, "ALTER TABLE test.a EXPERIMENTAL_RELOCATE VALUES (ARRAY[2], 0)")
	// Run the statement twice. The first time warms up the range cache, making
	// the planning predictable for the 2nd run.
	r.Exec(t, countStmt)
	r.Exec(t, countStmt)
	// Ignore the trace for the first stmt.
	<-recCh

	rec := <-recCh
	sp, ok := rec.FindSpan("table reader")
	require.True(t, ok, "table reader span not found")
	require.Empty(t, rec.OrphanSpans())
	// Check that the table reader indeed came from a remote note.
	anonTagGroup := sp.FindTagGroup(tracingpb.AnonymousTagGroupName)
	require.NotNil(t, anonTagGroup)
	val, ok := anonTagGroup.FindTag("node")
	require.True(t, ok)
	require.Equal(t, "2", val)
}

// Test the sql.trace.stmt.enable_threshold cluster setting.
func TestStatementThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	sql.TraceStmtThreshold.Override(ctx, &settings.SV, 1*time.Nanosecond)
	args := base.TestServerArgs{
		Settings: settings,
	}
	// Check that the server starts (no crash).
	s, db, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)
	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, "select 1")
	// TODO(andrei): check the logs for traces somehow.
}
