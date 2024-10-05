// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestCreateStatsControlJob tests that PAUSE JOB, RESUME JOB, and CANCEL JOB
// work as intended on CREATE STATISTICS jobs.
func TestCreateStatsControlJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer func(oldSamplerInterval int, oldSampleAgggregatorInterval time.Duration) {
		rowexec.SamplerProgressInterval = oldSamplerInterval
		rowexec.SampleAggregatorProgressInterval = oldSampleAgggregatorInterval
	}(rowexec.SamplerProgressInterval, rowexec.SampleAggregatorProgressInterval)
	rowexec.SamplerProgressInterval = 100
	rowexec.SampleAggregatorProgressInterval = time.Millisecond

	var allowRequest chan struct{}
	filter, setTableID := createStatsRequestFilter(&allowRequest)
	var serverArgs base.TestServerArgs
	serverArgs.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	serverArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: filter,
	}

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, serverArgs)
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	// Disable auto stats so that they don't interfere with the test.
	sqlDB.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;")
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (x INT PRIMARY KEY)`)
	var tID descpb.ID
	sqlDB.QueryRow(t, `SELECT 'd.t'::regclass::int`).Scan(&tID)
	sqlDB.Exec(t, `INSERT INTO d.t SELECT generate_series(1,1000)`)

	t.Run("cancel", func(t *testing.T) {
		// Test that CREATE STATISTICS can be canceled.
		query := `CREATE STATISTICS s1 FROM d.t`

		setTableID(tID)
		if _, err := runCreateStatsJob(ctx, t, sqlDB, &allowRequest, "CANCEL", query); err == nil {
			t.Fatal("expected an error")
		}

		// There should be no results here.
		sqlDB.CheckQueryResults(t,
			`SELECT statistics_name, column_names, row_count FROM [SHOW STATISTICS FOR TABLE d.t]`,
			[][]string{})
	})

	t.Run("pause", func(t *testing.T) {
		// Test that CREATE STATISTICS can be paused and resumed.
		query := `CREATE STATISTICS s2 FROM d.t`

		jobID, err := runCreateStatsJob(ctx, t, sqlDB, &allowRequest, "PAUSE", query)
		if !testutils.IsError(err, "pause") && !testutils.IsError(err, "liveness") {
			t.Fatalf("unexpected: %v", err)
		}

		// There should be no results here.
		sqlDB.CheckQueryResults(t,
			`SELECT statistics_name, column_names, row_count FROM [SHOW STATISTICS FOR TABLE d.t]`,
			[][]string{})

		testutils.SucceedsSoon(t, func() error {
			_, err := sqlDB.DB.ExecContext(context.Background(), `RESUME JOB $1`, jobID)
			return err
		})
		jobutils.WaitForJobToSucceed(t, sqlDB, jobID)

		// Now the job should have succeeded in producing stats.
		sqlDB.CheckQueryResults(t,
			`SELECT statistics_name, column_names, row_count FROM [SHOW STATISTICS FOR TABLE d.t]`,
			[][]string{
				{"s2", "{x}", "1000"},
			})
	})
}

// runCreateStatsJob runs the provided CREATE STATISTICS job control statement,
// initializing, notifying and closing the chan at the passed pointer (see below
// for why) and returning the jobID and error result.
//
// PAUSE JOB and CANCEL JOB are racy in that it's hard to guarantee that the job
// is still running when executing a PAUSE or CANCEL -- or that the job has even
// started running. To synchronize, we can install a store response filter which
// does a blocking receive for the ScanRequest used by our job. Later, when we
// want to guarantee the job is in progress, we do exactly one blocking send.
// When this send completes, we know the job has started, as we've seen one
// expected response. We also know the job has not finished, because we're
// blocking all future responses until we close the channel, and our operation
// is large enough that it will generate more than one of the expected response.
func runCreateStatsJob(
	ctx context.Context,
	t *testing.T,
	db *sqlutils.SQLRunner,
	allowProgressIota *chan struct{},
	op string,
	query string,
	args ...interface{},
) (jobspb.JobID, error) {
	*allowProgressIota = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := db.DB.ExecContext(ctx, query, args...)
		errCh <- err
	}()
	select {
	case *allowProgressIota <- struct{}{}:
	case err := <-errCh:
		return 0, errors.Wrapf(err, "query returned before expected: %s", query)
	}
	var jobID jobspb.JobID
	db.QueryRow(t, `SELECT id FROM system.jobs WHERE job_type = 'CREATE STATS' ORDER BY created DESC LIMIT 1`).Scan(&jobID)
	db.Exec(t, fmt.Sprintf("%s JOB %d", op, jobID))
	*allowProgressIota <- struct{}{}
	close(*allowProgressIota)
	return jobID, <-errCh
}

func TestCreateStatisticsCanBeCancelled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var allowRequest chan struct{}

	var serverArgs base.TestServerArgs
	filter, setTableID := createStatsRequestFilter(&allowRequest)
	serverArgs.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	serverArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: filter,
	}

	ctx := context.Background()
	tc, conn, _ := serverutils.StartServer(t, serverArgs)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (x INT PRIMARY KEY)  WITH (sql_stats_automatic_collection_enabled = false)`)
	sqlDB.Exec(t, `INSERT INTO d.t SELECT generate_series(1,1000)`)
	var tID descpb.ID
	sqlDB.QueryRow(t, `SELECT 'd.t'::regclass::int`).Scan(&tID)
	setTableID(tID)

	// Run CREATE STATISTICS and wait for it to create the job.
	allowRequest = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := conn.Exec(`CREATE STATISTICS s1 FROM d.t`)
		errCh <- err
	}()
	allowRequest <- struct{}{}
	setTableID(descpb.InvalidID)
	testutils.SucceedsSoon(t, func() error {
		row := conn.QueryRow("SELECT query_id FROM [SHOW CLUSTER STATEMENTS] WHERE query LIKE 'CREATE STATISTICS%';")
		var queryID string
		if err := row.Scan(&queryID); err != nil {
			return err
		}
		_, err := conn.Exec("CANCEL QUERIES VALUES ((SELECT query_id FROM [SHOW CLUSTER STATEMENTS] WHERE query LIKE 'CREATE STATISTICS%'));")
		return err
	})
	// Allow the filter to pass everything until an error is received.
	var err error
	testutils.SucceedsSoon(t, func() error {
		// Assume something will fail.
		err = errors.AssertionFailedf("failed for create stats to cancel")
		for {
			select {
			case err = <-errCh:
				return nil
			case allowRequest <- struct{}{}:
			default:
				return err
			}
		}
	})
	close(allowRequest)
	require.ErrorContains(t, err, "pq: query execution canceled")
}

func TestAtMostOneRunningCreateStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var allowRequest chan struct{}

	filter, setTableID := createStatsRequestFilter(&allowRequest)
	var params base.TestClusterArgs
	params.ServerArgs.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: filter,
	}
	params.ServerArgs.DefaultTestTenant = base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109379)

	ctx := context.Background()
	const nodes = 1
	tc := testcluster.StartTestCluster(t, nodes, params)
	defer tc.Stopper().Stop(ctx)
	conn := tc.ApplicationLayer(0).SQLConn(t)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (x INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO d.t SELECT generate_series(1,1000)`)
	var tID descpb.ID
	sqlDB.QueryRow(t, `SELECT 'd.t'::regclass::int`).Scan(&tID)
	setTableID(tID)

	// Start a CREATE STATISTICS run and wait until it's done one scan.
	allowRequest = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := conn.Exec(`CREATE STATISTICS s1 FROM d.t`)
		errCh <- err
	}()
	select {
	case allowRequest <- struct{}{}:
	case err := <-errCh:
		t.Fatal(err)
	}
	autoStatsRunShouldFail := func() {
		_, err := conn.Exec(`CREATE STATISTICS __auto__ FROM d.t`)
		expected := "another CREATE STATISTICS job is already running"
		if !testutils.IsError(err, expected) {
			t.Fatalf("expected '%s' error, but got %v", expected, err)
		}
	}

	// Attempt to start an automatic stats run. It should fail.
	autoStatsRunShouldFail()

	// PAUSE JOB does not block until the job is paused but only requests it.
	// Wait until the job is set to paused.
	var jobID jobspb.JobID
	sqlDB.QueryRow(t, `SELECT id FROM system.jobs ORDER BY created DESC LIMIT 1`).Scan(&jobID)
	opts := retry.Options{
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     time.Second,
		Multiplier:     2,
	}
	if err := retry.WithMaxAttempts(context.Background(), opts, 10, func() error {
		_, err := sqlDB.DB.ExecContext(context.Background(), `PAUSE JOB $1`, jobID)
		if err != nil {
			t.Fatal(err)
		}
		var status string
		sqlDB.QueryRow(t, `SELECT status FROM system.jobs WHERE id = $1 LIMIT 1`, jobID).Scan(&status)
		if status != "paused" {
			return errors.New("could not pause job")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// Starting another automatic stats run should still fail.
	autoStatsRunShouldFail()

	// Attempt to start a regular stats run. It should succeed.
	errCh2 := make(chan error)
	go func() {
		_, err := conn.Exec(`CREATE STATISTICS s2 FROM d.t`)
		errCh2 <- err
	}()
	select {
	case allowRequest <- struct{}{}:
	case err := <-errCh:
		t.Fatal(err)
	case err := <-errCh2:
		t.Fatal(err)
	}
	close(allowRequest)

	// Verify that the second job completed successfully.
	if err := <-errCh2; err != nil {
		t.Fatalf("create stats job should have completed: %s", err)
	}

	// Verify that the first job completed successfully.
	sqlDB.Exec(t, fmt.Sprintf("RESUME JOB %d", jobID))
	jobutils.WaitForJobToSucceed(t, sqlDB, jobID)
	<-errCh
}

func TestDeleteFailedJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	serverArgs := base.TestServerArgs{Knobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()}}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: serverArgs})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ApplicationLayer(0).SQLConn(t)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (x INT PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE TABLE d.u (x INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO d.t SELECT generate_series(1,1000)`)
	sqlDB.Exec(t, `INSERT INTO d.u SELECT generate_series(1,1000)`)

	// Start two CREATE STATISTICS runs at once.
	errCh1 := make(chan error)
	go func() {
		_, err := conn.Exec(`CREATE STATISTICS __auto__ FROM d.t`)
		errCh1 <- err
	}()
	errCh2 := make(chan error)
	go func() {
		_, err := conn.Exec(`CREATE STATISTICS __auto__ FROM d.u`)
		errCh2 <- err
	}()

	err1 := <-errCh1
	err2 := <-errCh2

	// At least one of the jobs should have succeeded.
	if err1 != nil && err2 != nil {
		t.Fatalf("one job should have succeeded but both failed. err1:%v, err2:%v", err1, err2)
	}

	// Check that if one of the jobs failed, it was deleted and doesn't show up in
	// SHOW AUTOMATIC JOBS.
	// Note: if this test fails, it will likely show up by using stressrace.
	if res := sqlDB.QueryStr(t,
		`SELECT statement, status, error FROM [SHOW AUTOMATIC JOBS] WHERE status = $1`,
		jobs.StatusFailed,
	); len(res) != 0 {
		t.Fatalf("job should have been deleted but found: %v", res)
	}
}

// TestCreateStatsProgress tests that progress reporting works correctly
// for the CREATE STATISTICS job.
func TestCreateStatsProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer func(oldProgressInterval time.Duration) {
		rowexec.SampleAggregatorProgressInterval = oldProgressInterval
	}(rowexec.SampleAggregatorProgressInterval)
	rowexec.SampleAggregatorProgressInterval = time.Nanosecond

	defer func(oldProgressInterval int) {
		rowexec.SamplerProgressInterval = oldProgressInterval
	}(rowexec.SamplerProgressInterval)
	rowexec.SamplerProgressInterval = 10

	skip.UnderRace(t, "the test is too sensitive to overload")
	skip.UnderDeadlock(t, "the test is too sensitive to overload")

	getLastCreateStatsJobID := func(t testing.TB, db *sqlutils.SQLRunner) jobspb.JobID {
		var jobID jobspb.JobID
		db.QueryRow(t, "SELECT id FROM system.jobs WHERE status = 'running' AND "+
			"job_type = 'CREATE STATS' ORDER BY created DESC LIMIT 1").Scan(&jobID)
		return jobID
	}

	var allowRequest chan struct{}
	var allowRequestClosed bool
	// Make sure that we unblock the test server in all scenarios with test
	// failures.
	defer func() {
		if !allowRequestClosed {
			close(allowRequest)
		}
	}()
	filter, setTableID := createStatsRequestFilter(&allowRequest)
	var params base.TestServerArgs
	params.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: filter,
	}
	params.Knobs.DistSQL = &execinfra.TestingKnobs{
		// Force the stats job to iterate through the input rows instead of reading
		// them all at once.
		TableReaderBatchBytesLimit: 100,
	}

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (i INT8 PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO d.t SELECT generate_series(1,1000)`)
	var tID descpb.ID
	sqlDB.QueryRow(t, `SELECT 'd.t'::regclass::int`).Scan(&tID)
	setTableID(tID)

	getFractionCompleted := func(jobID jobspb.JobID) float32 {
		var progress *jobspb.Progress
		testutils.SucceedsSoon(t, func() error {
			progress = jobutils.GetJobProgress(t, sqlDB, jobID)
			if progress.Progress == nil {
				return errors.Errorf("progress is nil. jobID: %d", jobID)
			}
			return nil
		})
		return progress.Progress.(*jobspb.Progress_FractionCompleted).FractionCompleted
	}

	const query = `CREATE STATISTICS s1 FROM d.t`

	// Start a CREATE STATISTICS run and wait until it has scanned part of the
	// table.
	allowRequest = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := conn.Exec(query)
		errCh <- err
	}()
	// Ten iterations here allows us to read some of the rows but not all.
	for i := 0; i < 10; i++ {
		select {
		case allowRequest <- struct{}{}:
		case err := <-errCh:
			if err == nil {
				t.Fatalf("query unexpectedly finished")
			} else {
				t.Fatal(err)
			}
		}
	}

	// Fetch the new job ID since we know it's running now.
	jobID := getLastCreateStatsJobID(t, sqlDB)

	// Ensure that 0 progress has been recorded since there are no existing
	// stats available to estimate progress.
	fractionCompleted := getFractionCompleted(jobID)
	if fractionCompleted != 0 {
		t.Fatalf(
			"create stats should not have recorded progress, but progress is %f",
			fractionCompleted,
		)
	}

	// Allow the job to complete and verify that the client didn't see anything
	// amiss.
	close(allowRequest)
	allowRequestClosed = true
	if err := <-errCh; err != nil {
		t.Fatalf("create stats job should have completed: %s", err)
	}

	// Verify that full progress is now recorded.
	fractionCompleted = getFractionCompleted(jobID)
	if fractionCompleted != 1 {
		t.Fatalf(
			"create stats should have recorded full progress, but progress is %f",
			fractionCompleted,
		)
	}

	// Invalidate the stats cache so that we can be sure to get the latest stats.
	s.ExecutorConfig().(sql.ExecutorConfig).TableStatsCache.InvalidateTableStats(ctx, tID)

	// Start another CREATE STATISTICS run and wait until it has scanned part of
	// the table.
	allowRequest = make(chan struct{})
	allowRequestClosed = false
	go func() {
		_, err := conn.Exec(query)
		errCh <- err
	}()
	// Ten iterations here allows us to read some of the rows but not all.
	for i := 0; i < 10; i++ {
		select {
		case allowRequest <- struct{}{}:
		case err := <-errCh:
			if err == nil {
				t.Fatalf("query unexpectedly finished")
			} else {
				t.Fatal(err)
			}
		}
	}

	// Fetch the new job ID since we know it's running now.
	jobID = getLastCreateStatsJobID(t, sqlDB)

	// Ensure that partial progress has been recorded since there are existing
	// stats available.
	fractionCompleted = getFractionCompleted(jobID)
	if fractionCompleted <= 0 || fractionCompleted > 0.99 {
		t.Fatalf(
			"create stats should have recorded partial progress, but progress is %f",
			fractionCompleted,
		)
	}

	// Allow the job to complete and verify that the client didn't see anything
	// amiss.
	close(allowRequest)
	allowRequestClosed = true
	if err := <-errCh; err != nil {
		t.Fatalf("create stats job should have completed: %s", err)
	}

	// Verify that full progress is now recorded.
	fractionCompleted = getFractionCompleted(jobID)
	if fractionCompleted != 1 {
		t.Fatalf(
			"create stats should have recorded full progress, but progress is %f",
			fractionCompleted,
		)
	}
}

func TestCreateStatsAsOfTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.ApplicationLayer(0).SQLConn(t))
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (x INT PRIMARY KEY)`)

	var ts1 []uint8
	sqlDB.QueryRow(t, `
			INSERT INTO d.t VALUES (1)
			RETURNING cluster_logical_timestamp();
		`).Scan(&ts1)

	sqlDB.Exec(t, `INSERT INTO d.t VALUES (2)`)

	sqlDB.Exec(t, fmt.Sprintf("CREATE STATISTICS s FROM d.t AS OF SYSTEM TIME %s", string(ts1)))

	// Check that we only see the first row, not the second.
	sqlDB.CheckQueryResults(t,
		`SELECT statistics_name, column_names, row_count FROM [SHOW STATISTICS FOR TABLE d.t]`,
		[][]string{
			{"s", "{x}", "1"},
		})
}

// Create a blocking request filter for the actions related to CREATE
// STATISTICS, i.e. Scanning a user table. See discussion on runCreateStatsJob
// for where this might be useful.
//
// Note that it only supports system tenants as well as the secondary tenant
// with serverutils.TestTenantID() tenant ID.
func createStatsRequestFilter(
	allowProgressIota *chan struct{},
) (kvserverbase.ReplicaRequestFilter, func(descpb.ID)) {
	var tableToBlock atomic.Value
	tableToBlock.Store(descpb.InvalidID)
	// We must create this request filter before we start the server, so we
	// don't know whether we're running against the test tenant or not. Thus, we
	// will always try the codec for the first test tenant ID, and if it doesn't
	// work, we fallback to the system tenant codec.
	possibleCodec := keys.MakeSQLCodec(serverutils.TestTenantID())
	return func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
		if req, ok := ba.GetArg(kvpb.Scan); ok {
			key := req.(*kvpb.ScanRequest).Key
			if strippedKey, err := possibleCodec.StripTenantPrefix(key); err == nil {
				key = strippedKey
			}
			_, tableID, _ := encoding.DecodeUvarintAscending(key)
			// Ensure that the tableID is what we expect it to be.
			if tableID > 0 && descpb.ID(tableID) == tableToBlock.Load() {
				// Read from the channel twice to allow jobutils.RunJob to complete
				// even though there is only one ScanRequest.
				<-*allowProgressIota
				<-*allowProgressIota
			}
		}
		return nil
	}, func(id descpb.ID) { tableToBlock.Store(id) }
}
