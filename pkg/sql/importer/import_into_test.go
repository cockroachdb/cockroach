// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// TestProtectedTimestampsDuringImportInto ensures that the timestamp at which
// a table is taken offline is protected during an IMPORT INTO job to ensure
// that if data is imported into a range it can be reverted in the case of
// cancellation or failure.
func TestProtectedTimestampsDuringImportInto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// A sketch of the test is as follows:
	//
	//  * Create a table foo to import into.
	//  * Set a 1 second gcttl for foo.
	//  * Start an import into with two HTTP backed CSV files where
	//    one server will serve a row and the other will block until
	//    it's signaled.
	//  * Manually enqueue the ranges for GC and ensure that at least one
	//    range ran the GC.
	//  * Force the IMPORT to fail.
	//  * Ensure that it was rolled back.
	//  * Ensure that we can GC after the job has finished.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	s := tc.Server(0).ApplicationLayer()
	tenantSettings := s.ClusterSettings()
	protectedts.PollInterval.Override(ctx, &tenantSettings.SV, 100*time.Millisecond)

	tc.WaitForNodeLiveness(t)
	require.NoError(t, tc.WaitForFullReplication())

	conn := tc.ServerConn(0)
	runner := sqlutils.MakeSQLRunner(conn)
	runner.Exec(t, "CREATE TABLE foo (k INT PRIMARY KEY, v BYTES)")
	runner.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds = 1;")
	rRand, _ := randutil.NewTestRand()
	writeGarbage := func(from, to int) {
		for i := from; i < to; i++ {
			runner.Exec(t, "UPSERT INTO foo VALUES ($1, $2)", i, randutil.RandBytes(rRand, 1<<10))
		}
	}
	writeGarbage(3, 10)
	rowsBeforeImportInto := runner.QueryStr(t, "SELECT * FROM foo")

	mkServer := func(method string, handler func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == method {
				handler(w, r)
			}
		}))
	}
	srv1 := mkServer("GET", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("1,asdfasdfasdfasdf"))
	})
	defer srv1.Close()
	// Let's start an import into this table of ours.
	allowResponse := make(chan struct{})
	srv2 := mkServer("GET", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-allowResponse:
		case <-ctx.Done(): // Deal with test failures.
		}
		w.WriteHeader(500)
	})
	defer srv2.Close()

	importErrCh := make(chan error, 1)
	go func() {
		_, err := conn.Exec(`IMPORT INTO foo (k, v) CSV DATA ($1, $2)`,
			srv1.URL, srv2.URL)
		importErrCh <- err
	}()

	testutils.SucceedsSoon(t, func() error {
		row := conn.QueryRow("SELECT job_id FROM [SHOW JOBS] ORDER BY created DESC LIMIT 1")
		var jobID string
		return row.Scan(&jobID)
	})

	time.Sleep(3 * time.Second) // Wait for the data to definitely be expired and GC to run.
	gcTable := func(skipShouldQueue bool) (traceStr string) {
		// Note: we cannot use SHOW RANGES FROM TABLE here because 'foo'
		// is being imported and is not ready yet.
		rows := runner.Query(t, `
SELECT raw_start_key
FROM [SHOW RANGES FROM TABLE foo WITH KEYS]
ORDER BY raw_start_key ASC`)
		var traceBuf strings.Builder
		for rows.Next() {
			var startKey roachpb.Key
			require.NoError(t, rows.Scan(&startKey))
			s, repl := getFirstStoreReplica(t, tc.Server(0), startKey)
			traceCtx, rec := tracing.ContextWithRecordingSpan(ctx, s.GetStoreConfig().Tracer(), "trace-enqueue")
			_, err := s.Enqueue(traceCtx, "mvccGC", repl, skipShouldQueue, false /* async */)
			require.NoError(t, err)
			fmt.Fprintf(&traceBuf, "%s\n", rec().String())
		}
		require.NoError(t, rows.Err())
		return traceBuf.String()
	}

	// We should have refused to GC over the timestamp which we needed to protect.
	gcTable(true /* skipShouldQueue */)

	// Unblock the blocked import request.
	close(allowResponse)

	require.Regexp(t, "error response from server: 500 Internal Server Error", <-importErrCh)

	runner.CheckQueryResultsRetry(t, "SELECT * FROM foo", rowsBeforeImportInto)

	// Write some fresh garbage.

	// Wait for the ranges to learn about the removed record and ensure that we
	// can GC from the range soon.
	// This regex matches when all float priorities other than 0.00000. It does
	// this by matching either a float >= 1 (e.g. 1230.012) or a float < 1 (e.g.
	// 0.000123).
	matchNonZero := "[1-9]\\d*\\.\\d+|0\\.\\d*[1-9]\\d*"
	nonZeroProgressRE := regexp.MustCompile(fmt.Sprintf("priority=(%s)", matchNonZero))
	testutils.SucceedsSoon(t, func() error {
		writeGarbage(3, 10)
		if trace := gcTable(false /* skipShouldQueue */); !nonZeroProgressRE.MatchString(trace) {
			return fmt.Errorf("expected %v in trace: %v", nonZeroProgressRE, trace)
		}
		return nil
	})
}

func getFirstStoreReplica(
	t *testing.T, s serverutils.TestServerInterface, key roachpb.Key,
) (*kvserver.Store, *kvserver.Replica) {
	t.Helper()
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	var repl *kvserver.Replica
	testutils.SucceedsSoon(t, func() error {
		repl = store.LookupReplica(roachpb.RKey(key))
		if repl == nil {
			return errors.New(`could not find replica`)
		}
		return nil
	})
	return store, repl
}

// TestImportIntoWithUDTArray verifies that we can support importing data into a
// table with a column typed as an array of user-defined types.
func TestImportIntoWithUDTArray(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
	})
	defer srv.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)
	runner.Exec(t, `
CREATE TYPE weekday AS ENUM('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday');
CREATE TABLE shifts (employee STRING, days weekday[]);
INSERT INTO shifts VALUES ('John', ARRAY['Monday', 'Wednesday', 'Friday']);
INSERT INTO shifts VALUES ('Bob', ARRAY['Tuesday', 'Thursday']);
`)
	// Sanity check that we currently have the expected state.
	expected := [][]string{
		{"John", "{Monday,Wednesday,Friday}"},
		{"Bob", "{Tuesday,Thursday}"},
	}
	runner.CheckQueryResults(t, "SELECT * FROM shifts;", expected)
	// Export has to run in a separate implicit txn.
	runner.Exec(t, `EXPORT INTO CSV 'nodelocal://1/export1/' FROM SELECT * FROM shifts;`)
	// Now clear the table since we'll be importing into it.
	runner.Exec(t, `DELETE FROM shifts WHERE true;`)
	runner.CheckQueryResults(t, "SELECT count(*) FROM shifts;", [][]string{{"0"}})
	// Import two rows once.
	runner.Exec(t, "IMPORT INTO shifts CSV DATA ('nodelocal://1/export1/export*-n*.0.csv');")
	runner.CheckQueryResults(t, "SELECT * FROM shifts;", expected)
	// Import two rows again - we'll now have four rows in the table.
	runner.Exec(t, "IMPORT INTO shifts CSV DATA ('nodelocal://1/export1/export*-n*.0.csv');")
	runner.CheckQueryResults(t, "SELECT * FROM shifts;", append(expected, expected...))

	// We currently don't support importing into a table that has columns with
	// UDTs with the same name but different schemas.
	runner.Exec(t, `
CREATE SCHEMA short;
CREATE TYPE short.weekday AS ENUM('M', 'Tu', 'W', 'Th', 'F');
DROP TABLE shifts;
CREATE TABLE shifts (employee STRING, days weekday[], days_short short.weekday[]);
INSERT INTO shifts VALUES ('John', ARRAY['Monday', 'Wednesday', 'Friday'], ARRAY['M', 'W', 'F']);
INSERT INTO shifts VALUES ('Bob', ARRAY['Tuesday', 'Thursday'], ARRAY['Tu', 'Th']);
`)
	runner.Exec(t, `EXPORT INTO CSV 'nodelocal://1/export2/' FROM SELECT * FROM shifts;`)
	runner.ExpectErr(
		t,
		".*tables with multiple user-defined types with the same name are currently unsupported.*",
		"IMPORT INTO shifts CSV DATA ('nodelocal://1/export2/export*-n*.0.csv');",
	)
}

// TestImportIntoWithCompetingTransactionCommits verifies that IMPORT INTO
// succeeds when a competing transaction tries to commit and insert rows into the
// table being imported to.
func TestImportIntoWithCompetingTransactionCommits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
	})
	defer srv.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)

	// Create and populate test table.
	runner.Exec(t, `CREATE TABLE foo (k INT PRIMARY KEY, v STRING)`)
	runner.Exec(t, `INSERT INTO foo VALUES (1, 'initial')`)

	// Export the data.
	runner.Exec(t, `EXPORT INTO CSV 'nodelocal://1/export/' FROM SELECT 2, 'imported'`)

	// Track whether the knob was invoked.
	var knobInvoked bool
	knobCh := make(chan struct{})

	// Set up the testing knob to inject a competing transaction.
	s := srv.ApplicationLayer()
	registry := s.JobRegistry().(*jobs.Registry)
	registry.TestingWrapResumerConstructor(
		jobspb.TypeImport,
		func(resumer jobs.Resumer) jobs.Resumer {
			resumer.(interface {
				TestingSetBeforeInitialRowCountKnob(fn func() error)
			}).TestingSetBeforeInitialRowCountKnob(func() error {
				if !knobInvoked {
					knobInvoked = true
					// Start a competing transaction that tries and fails to insert to the table.
					go func() {
						innerDB := srv.ApplicationLayer().SQLConn(t)
						_, err := innerDB.Exec(`INSERT INTO foo VALUES (3, 'competing')`)
						require.ErrorContains(t, err, "pq: relation \"foo\" is offline: importing")
						close(knobCh)
					}()
					// Wait for the competing transaction to complete.
					<-knobCh
				}
				return nil
			})
			return resumer
		})

	runner.Exec(t, `IMPORT INTO foo (k, v) CSV DATA ('nodelocal://1/export/export*-n*.0.csv')`)

	require.True(t, knobInvoked, "expected knob to be invoked")

	runner.CheckQueryResults(t, `SELECT * FROM foo ORDER BY k`, [][]string{
		{"1", "initial"},
		{"2", "imported"},
	})
}

// TestImportIntoNonEmptyTableRowCountCheck verifies that IMPORT INTO a
// non-empty table correctly computes the expected row count as the sum of the
// initial row count and the imported rows, and that the row count validation
// inspect job succeeds. It also verifies that a deliberately wrong expected
// count causes the import to fail in sync mode.
func TestImportIntoNonEmptyTableRowCountCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
	})
	defer srv.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)

	// Use sync mode so the import blocks until the inspect job completes. If
	// the row count is wrong the import statement itself will fail.
	runner.Exec(t, `SET CLUSTER SETTING bulkio.import.row_count_validation.mode = 'sync'`)

	runner.Exec(t, `CREATE TABLE foo (k INT PRIMARY KEY, v INT)`)
	runner.Exec(t, `INSERT INTO foo SELECT i, i*10 FROM generate_series(1, 100) AS g(i)`)

	runner.Exec(t, `EXPORT INTO CSV 'nodelocal://1/export/' FROM SELECT i, i*10 FROM generate_series(101, 200) AS g(i)`)

	t.Run("match", func(t *testing.T) {
		runner.Exec(t, `IMPORT INTO foo (k, v) CSV DATA ('nodelocal://1/export/export*-n*.0.csv')`)
		runner.CheckQueryResults(t, `SELECT count(*) FROM foo`, [][]string{{"200"}})
	})

	// Re-export so we can import again with a fresh set of rows.
	runner.Exec(t, `EXPORT INTO CSV 'nodelocal://1/export2/' FROM SELECT i, i*10 FROM generate_series(201, 300) AS g(i)`)

	t.Run("mismatch", func(t *testing.T) {
		// Inject a row count offset so the expected count is wrong, causing the
		// inspect row count check to fail.
		s := srv.ApplicationLayer()
		registry := s.JobRegistry().(*jobs.Registry)
		registry.TestingWrapResumerConstructor(
			jobspb.TypeImport,
			func(resumer jobs.Resumer) jobs.Resumer {
				resumer.(interface {
					TestingSetExpectedRowCountOffset(offset int64)
				}).TestingSetExpectedRowCountOffset(5)
				return resumer
			})

		_, err := db.Exec(`IMPORT INTO foo (k, v) CSV DATA ('nodelocal://1/export2/export*-n*.0.csv')`)
		require.Error(t, err)
		require.ErrorContains(t, err, "INSPECT found inconsistencies")

		// Extract the inspect job ID from the error hint and verify the error
		// details contain the expected and actual row counts.
		var pqErr *pq.Error
		require.True(t, errors.As(err, &pqErr))
		var inspectJobID int64
		_, err = fmt.Sscanf(pqErr.Hint, "Run 'SHOW INSPECT ERRORS FOR JOB %d WITH DETAILS' for more information.", &inspectJobID)
		require.NoError(t, err)

		var errorType, databaseName, schemaName, tableName, primaryKey, aost, details string
		var jobID int64
		runner.QueryRow(t,
			fmt.Sprintf(`SHOW INSPECT ERRORS FOR JOB %d WITH DETAILS`, inspectJobID),
		).Scan(&errorType, &databaseName, &schemaName, &tableName, &primaryKey, &jobID, &aost, &details)
		t.Logf("SHOW INSPECT ERRORS FOR JOB %d WITH DETAILS:\n"+
			"  error_type:     %s\n"+
			"  database_name:  %s\n"+
			"  schema_name:    %s\n"+
			"  table_name:     %s\n"+
			"  primary_key:    %s\n"+
			"  job_id:         %d\n"+
			"  aost:           %s\n"+
			"  details:        %s",
			inspectJobID, errorType, databaseName, schemaName, tableName, primaryKey, jobID, aost, details)
		require.Contains(t, details, `"actual": 300`)
		require.Contains(t, details, `"expected": 305`)
	})
}

// TestImportIntoRowCountCheckAfterPause verifies that the INSPECT row count
// validation passes after an import is paused and resumed. This exercises two
// fixes:
//   - For empty tables, the initial row count must not be re-computed on resume
//     (since the ingested data would be included, inflating the count).
//   - For resumed imports, the expected row count must include rows ingested in
//     previous runs, since r.res.Rows only reflects the current run.
func TestImportIntoRowCountCheckAfterPause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "uses short job adoption intervals that don't work well in slow test configurations")

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	adoptInterval := 500 * time.Millisecond
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: &jobs.TestingKnobs{
				IntervalOverrides: jobs.TestingIntervalOverrides{
					Adopt:  &adoptInterval,
					Cancel: &adoptInterval,
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)

	// Use sync mode so the import blocks until the inspect job completes. If
	// the row count is wrong the import statement itself will fail.
	runner.Exec(t, `SET CLUSTER SETTING bulkio.import.row_count_validation.mode = 'sync'`)

	s := srv.ApplicationLayer()
	registry := s.JobRegistry().(*jobs.Registry)

	for _, tc := range []struct {
		name      string
		setupSQL  string
		totalRows int
	}{
		{
			name:      "empty_table",
			setupSQL:  "",
			totalRows: 10,
		},
		{
			name:      "nonempty_table",
			setupSQL:  `INSERT INTO nonempty_table SELECT i, i*10 FROM generate_series(1, 5) AS g(i)`,
			totalRows: 15,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Flush job progress after every batch so that resume_pos is saved
			// before the pausepoint fires. Without this, the second ingest on
			// resume would re-process all rows (since resume_pos would be 0),
			// masking the need for the previouslyImportedRows accounting.
			registry.TestingWrapResumerConstructor(
				jobspb.TypeImport,
				func(resumer jobs.Resumer) jobs.Resumer {
					resumer.(interface {
						TestingSetAlwaysFlushJobProgress()
					}).TestingSetAlwaysFlushJobProgress()
					return resumer
				})

			// Use a unique table name per subtest to avoid conflicts from
			// tables left offline by a previous subtest's paused import.
			runner.Exec(t, fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v INT)`, tc.name))

			if tc.setupSQL != "" {
				runner.Exec(t, tc.setupSQL)
			}

			runner.Exec(t, fmt.Sprintf(
				`EXPORT INTO CSV 'nodelocal://1/export_%s/' FROM SELECT i, i*10 FROM generate_series(6, 15) AS g(i)`,
				tc.name))

			// Set pausepoint to pause after ingest.
			runner.Exec(t, `SET CLUSTER SETTING jobs.debug.pausepoints = 'import.after_ingest'`)

			// Run import. The statement returns with a pausepoint error.
			_, err := db.Exec(fmt.Sprintf(
				`IMPORT INTO %s (k, v) CSV DATA ('nodelocal://1/export_%s/export*-n*.0.csv')`,
				tc.name, tc.name))
			require.Error(t, err)
			require.ErrorContains(t, err, "pause point")

			// Find the paused import job. The job transitions from
			// pause-requested to paused asynchronously, so retry until
			// it reaches the paused state.
			var jobID int64
			testutils.SucceedsSoon(t, func() error {
				return db.QueryRow(
					`SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'IMPORT' AND status = 'paused'`,
				).Scan(&jobID)
			})

			// Clear pausepoint and resume the job.
			runner.Exec(t, `SET CLUSTER SETTING jobs.debug.pausepoints = ''`)
			runner.Exec(t, fmt.Sprintf(`RESUME JOB %d`, jobID))

			// Wait for job to succeed. If the INSPECT row count check fails,
			// the job will be in 'failed' state instead.
			testutils.SucceedsSoon(t, func() error {
				var status, errStr string
				if err := db.QueryRow(
					fmt.Sprintf(`SELECT status, COALESCE(error, '') FROM [SHOW JOB %d]`, jobID),
				).Scan(&status, &errStr); err != nil {
					return err
				}
				if status == string(jobs.StateFailed) {
					return errors.Newf("import job %d failed: %s", jobID, errStr)
				}
				if status != string(jobs.StateSucceeded) {
					return errors.Newf("job %d status: %s", jobID, status)
				}
				return nil
			})

			runner.CheckQueryResults(t,
				fmt.Sprintf(`SELECT count(*) FROM %s`, tc.name),
				[][]string{{fmt.Sprintf("%d", tc.totalRows)}})
		})
	}
}
