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
// for why) and returning the jobID and error result. PAUSE JOB and CANCEL JOB
// are racy in that it's hard to guarantee that the job is still running when
// executing a PAUSE or CANCEL -- or that the job has even started running. To
// synchronize, we can install a store response filter which does a blocking
// receive for one of the responses used by our job (for example, Export for a
// BACKUP). Later, when we want to guarantee the job is in progress, we do
// exactly one blocking send. When this send completes, we know the job has
// started, as we've seen one expected response. We also know the job has not
// finished, because we're blocking all future responses until we close the
// channel, and our operation is large enough that it will generate more than
// one of the expected response.
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

// TestAtMostOneRunningCreateStats tests that auto stat jobs (full or partial)
// don't run when a full stats job is running. It also tests that manual stat
// jobs (full or partial) are always allowed to run.
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

	autoFullStatsRunShouldFail := func() {
		_, err := conn.Exec(`CREATE STATISTICS __auto__ FROM d.t`)
		expected := "another CREATE STATISTICS job is already running"
		if !testutils.IsError(err, expected) {
			t.Fatalf("expected '%s' error, but got %v", expected, err)
		}
	}
	autoPartialStatsRunShouldFail := func() {
		_, err := conn.Exec(`CREATE STATISTICS __auto_partial__ FROM d.t USING EXTREMES`)
		expected := "another CREATE STATISTICS job is already running"
		if !testutils.IsError(err, expected) {
			t.Fatalf("expected '%s' error, but got %v", expected, err)
		}
	}

	// Start a full stat run and let it complete so that future partial stats can
	// be collected
	allowRequest = make(chan struct{})
	initialFullStatErrCh := make(chan error)
	go func() {
		_, err := conn.Exec(`CREATE STATISTICS full_statistic FROM d.t`)
		initialFullStatErrCh <- err
	}()
	close(allowRequest)
	if err := <-initialFullStatErrCh; err != nil {
		t.Fatalf("create stats job should have completed: %s", err)
	}

	// Start a manual full stat run and wait until it's done one scan. This will
	// be the stat job that runs in the background as we test the behavior of new
	// stat jobs.
	allowRequest = make(chan struct{})
	runningManualFullStatErrCh := make(chan error)
	go func() {
		_, err := conn.Exec(`CREATE STATISTICS s1 FROM d.t`)
		runningManualFullStatErrCh <- err
	}()
	select {
	case allowRequest <- struct{}{}:
	case err := <-runningManualFullStatErrCh:
		t.Fatal(err)
	}

	// Attempt to start automatic full and partial stats runs. Both should fail.
	autoFullStatsRunShouldFail()
	autoPartialStatsRunShouldFail()

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

	// Starting automatic full and partial stats run should still fail.
	autoFullStatsRunShouldFail()
	autoPartialStatsRunShouldFail()

	// Attempt to start manual full and partial stat runs. Both should succeed.
	manualFullStatErrCh := make(chan error)
	go func() {
		_, err := conn.Exec(`CREATE STATISTICS s2 FROM d.t`)
		manualFullStatErrCh <- err
	}()
	manualPartialStatErrCh := make(chan error)
	go func() {
		_, err := conn.Exec(`CREATE STATISTICS ps1 FROM d.t USING EXTREMES`)
		manualPartialStatErrCh <- err
	}()

	select {
	case allowRequest <- struct{}{}:
	case err := <-runningManualFullStatErrCh:
		t.Fatal(err)
	case err := <-manualFullStatErrCh:
		t.Fatal(err)
	case err := <-manualPartialStatErrCh:
		t.Fatal(err)
	}

	// Allow the running full stat job and the new full and partial stat jobs to complete.
	close(allowRequest)

	// Verify that the manual full and partial stat jobs completed successfully.
	if err := <-manualFullStatErrCh; err != nil {
		t.Fatalf("create stats job should have completed: %s", err)
	}
	if err := <-manualPartialStatErrCh; err != nil {
		t.Fatalf("create partial stats job should have completed: %s", err)
	}

	// Verify that the running full stat job completed successfully.
	sqlDB.Exec(t, fmt.Sprintf("RESUME JOB %d", jobID))
	jobutils.WaitForJobToSucceed(t, sqlDB, jobID)
	<-runningManualFullStatErrCh
}

// TestBackgroundAutoPartialStats tests that a running auto partial stats job
// doesn't prevent any new full or partial stat jobs from running, except for
// auto partial stat jobs on the same table.
func TestBackgroundAutoPartialStats(t *testing.T) {
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
	sqlDB.Exec(t, `CREATE TABLE d.t1 (x INT PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE TABLE d.t2 (x INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO d.t1 SELECT generate_series(1,1000)`)
	sqlDB.Exec(t, `INSERT INTO d.t2 SELECT generate_series(1,1000)`)
	var t1ID descpb.ID
	sqlDB.QueryRow(t, `SELECT 'd.t1'::regclass::int`).Scan(&t1ID)
	setTableID(t1ID)

	// Collect full stats on both tables so that future partial stats can be
	// collected
	allowRequest = make(chan struct{})
	close(allowRequest)
	if _, err := conn.Exec(`CREATE STATISTICS full_statistic FROM d.t1`); err != nil {
		t.Fatalf("create stats job should have completed: %s", err)
	}
	if _, err := conn.Exec(`CREATE STATISTICS full_statistic FROM d.t2`); err != nil {
		t.Fatalf("create stats job should have completed: %s", err)
	}

	// Start an auto partial stat run on t1 and wait until it's done one scan.
	// This will be the stat job that runs in the background as we test the
	// behavior of new stat jobs.
	allowRequest = make(chan struct{})
	runningAutoPartialStatErrCh := make(chan error)
	go func() {
		_, err := conn.Exec(`CREATE STATISTICS __auto_partial__ FROM d.t1 USING EXTREMES`)
		runningAutoPartialStatErrCh <- err
	}()
	select {
	case allowRequest <- struct{}{}:
	case err := <-runningAutoPartialStatErrCh:
		t.Fatal(err)
	}

	// Attempt to start a simultaneous auto full stat run. It should succeed.
	autoFullStatErrCh := make(chan error)
	go func() {
		_, err := conn.Exec(`CREATE STATISTICS __auto__ FROM d.t1`)
		autoFullStatErrCh <- err
	}()

	select {
	case allowRequest <- struct{}{}:
	case err := <-runningAutoPartialStatErrCh:
		t.Fatal(err)
	case err := <-autoFullStatErrCh:
		t.Fatal(err)
	}

	// Allow both auto stat jobs to complete.
	close(allowRequest)

	// Verify that both jobs completed successfully.
	if err := <-autoFullStatErrCh; err != nil {
		t.Fatalf("create auto full stats job should have completed: %s", err)
	}
	if err := <-runningAutoPartialStatErrCh; err != nil {
		t.Fatalf("create auto partial stats job should have completed: %s", err)
	}

	// Start another auto partial stat run and wait until it's done one scan.
	allowRequest = make(chan struct{})
	runningAutoPartialStatErrCh = make(chan error)
	go func() {
		_, err := conn.Exec(`CREATE STATISTICS __auto_partial__ FROM d.t1 USING EXTREMES`)
		runningAutoPartialStatErrCh <- err
	}()
	select {
	case allowRequest <- struct{}{}:
	case err := <-runningAutoPartialStatErrCh:
		t.Fatal(err)
	}

	// Attempt to start a simultaneous auto partial stat run on the same table.
	// It should fail.
	_, err := conn.Exec(`CREATE STATISTICS __auto_partial__ FROM d.t1 USING EXTREMES`)
	expected := "another CREATE STATISTICS job is already running"
	if !testutils.IsError(err, expected) {
		t.Fatalf("expected '%s' error, but got %v", expected, err)
	}

	// Attempt to start a simultaneous auto partial stat run on a different table.
	// It should succeed.
	autoPartialStatErrCh := make(chan error)
	go func() {
		_, err := conn.Exec(`CREATE STATISTICS __auto_partial__ FROM d.t2 USING EXTREMES`)
		autoPartialStatErrCh <- err
	}()

	select {
	case allowRequest <- struct{}{}:
	case err = <-runningAutoPartialStatErrCh:
		t.Fatal(err)
	case err = <-autoPartialStatErrCh:
		t.Fatal(err)
	}

	// Allow both auto partial stat jobs to complete.
	close(allowRequest)

	// Verify that both jobs completed successfully.
	if err = <-autoPartialStatErrCh; err != nil {
		t.Fatalf("create auto partial stats job should have completed: %s", err)
	}
	if err = <-runningAutoPartialStatErrCh; err != nil {
		t.Fatalf("create auto partial stats job should have completed: %s", err)
	}
}

func TestDeleteFailedJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	serverArgs := base.TestServerArgs{Knobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()}}
	s, conn, _ := serverutils.StartServer(t, serverArgs)
	defer s.Stopper().Stop(ctx)
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
		`SELECT job_id, status, error FROM [SHOW AUTOMATIC JOBS] WHERE status = $1`,
		jobs.StateFailed,
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

	var allowRequest chan struct{}
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

	var allowRequestClosed bool
	// Make sure that we unblock the test server in all scenarios with test
	// failures.
	defer func() {
		if !allowRequestClosed {
			close(allowRequest)
		}
	}()

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (i INT8 PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO d.t SELECT generate_series(1,1000)`)
	var tID descpb.ID
	sqlDB.QueryRow(t, `SELECT 'd.t'::regclass::int`).Scan(&tID)
	setTableID(tID)

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
	fractionCompleted := getFractionCompleted(t, sqlDB, jobID)
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
	fractionCompleted = getFractionCompleted(t, sqlDB, jobID)
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
	fractionCompleted = getFractionCompleted(t, sqlDB, jobID)
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
	fractionCompleted = getFractionCompleted(t, sqlDB, jobID)
	if fractionCompleted != 1 {
		t.Fatalf(
			"create stats should have recorded full progress, but progress is %f",
			fractionCompleted,
		)
	}
}

func TestCreateStatsUsingExtremesProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer func(oldProgressInterval time.Duration) {
		rowexec.SampleAggregatorProgressInterval = oldProgressInterval
	}(rowexec.SampleAggregatorProgressInterval)
	rowexec.SampleAggregatorProgressInterval = time.Nanosecond

	defer func(oldProgressInterval int) {
		rowexec.SamplerProgressInterval = oldProgressInterval
	}(rowexec.SamplerProgressInterval)
	rowexec.SamplerProgressInterval = 1

	skip.UnderRace(t, "the test is too sensitive to overload")
	skip.UnderDeadlock(t, "the test is too sensitive to overload")

	var allowRequest chan struct{}
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

	var allowRequestClosed bool
	// Make sure that we unblock the test server in all scenarios with test
	// failures.
	defer func() {
		if !allowRequestClosed {
			close(allowRequest)
		}
	}()

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (a INT8 PRIMARY KEY, b INT8, INDEX(b))`)
	sqlDB.Exec(t, `INSERT INTO d.t SELECT x, x from generate_series(1, 100) as g(x)`)
	var tID descpb.ID
	sqlDB.QueryRow(t, `SELECT 'd.t'::regclass::int`).Scan(&tID)
	setTableID(tID)

	const fullStatQuery = `CREATE STATISTICS s1 FROM d.t`

	// Start a CREATE STATISTICS run.
	allowRequest = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := conn.Exec(fullStatQuery)
		errCh <- err
	}()

	// Allow the job to complete and verify that the client didn't see anything
	// amiss.
	close(allowRequest)
	allowRequestClosed = true
	if err := <-errCh; err != nil {
		t.Fatalf("create stats job should have completed: %s", err)
	}

	// Invalidate the stats cache so that we can be sure to get the latest stats.
	s.ExecutorConfig().(sql.ExecutorConfig).TableStatsCache.InvalidateTableStats(ctx, tID)

	sqlDB.Exec(t, `INSERT INTO d.t SELECT x, x from generate_series(101, 1000) as g(x)`)

	const partialStatQuery = `CREATE STATISTICS s2 FROM d.t USING EXTREMES`

	// Start a CREATE STATISTICS USING EXTREMES run that will scan two indexes.
	allowRequest = make(chan struct{})
	allowRequestClosed = false
	go func() {
		_, err := conn.Exec(partialStatQuery)
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

	var fractionCompleted float32
	prevFractionCompleted := getFractionCompleted(t, sqlDB, jobID)

	// Allow the job to progress until it finishes scanning both indexes.
Loop:
	for {
		select {
		case allowRequest <- struct{}{}:
			// Ensure that job progress never regresses throughout both index scans.
			fractionCompleted = getFractionCompleted(t, sqlDB, jobID)
			if fractionCompleted < prevFractionCompleted {
				close(errCh)
				t.Fatalf("create partial stats job should not regress progress between indexes: %f -> %f", prevFractionCompleted, fractionCompleted)
			}
			prevFractionCompleted = fractionCompleted
		case err := <-errCh:
			if err == nil {
				// Create partial stats job is now completed
				break Loop
			} else {
				t.Fatalf("create partial stats job should have completed: %s", err)
			}
		}
	}

	close(allowRequest)
	allowRequestClosed = true

	// Verify that full progress is now recorded.
	fractionCompleted = getFractionCompleted(t, sqlDB, jobID)
	if fractionCompleted != 1 {
		t.Fatalf(
			"create partial stats should have recorded full progress, but progress is %f",
			fractionCompleted,
		)
	}
}

func TestCreateStatsAsOfTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(conn)
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
				// Read from the channel twice to allow runCreateStatsJob to
				// complete even though there is only one ScanRequest.
				// TODO(yuzefovich): only some tests need this behavior.
				// Consider asking the caller how many times we should receive
				// from the channel.
				<-*allowProgressIota
				<-*allowProgressIota
			}
		}
		return nil
	}, func(id descpb.ID) { tableToBlock.Store(id) }
}

func getLastCreateStatsJobID(t testing.TB, db *sqlutils.SQLRunner) jobspb.JobID {
	var jobID jobspb.JobID
	db.QueryRow(t, "SELECT id FROM system.jobs WHERE status = 'running' AND "+
		"job_type = 'CREATE STATS' ORDER BY created DESC LIMIT 1").Scan(&jobID)
	return jobID
}

func getFractionCompleted(t testing.TB, sqlDB *sqlutils.SQLRunner, jobID jobspb.JobID) float32 {
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
