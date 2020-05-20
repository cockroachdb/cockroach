// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// TestCreateStatsControlJob tests that PAUSE JOB, RESUME JOB, and CANCEL JOB
// work as intended on create statistics jobs.
func TestCreateStatsControlJob(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	// Test with 3 nodes and rowexec.SamplerProgressInterval=100 to ensure
	// that progress metadata is sent correctly after every 100 input rows.
	const nodes = 3
	defer func(oldSamplerInterval int, oldSampleAgggregatorInterval time.Duration) {
		rowexec.SamplerProgressInterval = oldSamplerInterval
		rowexec.SampleAggregatorProgressInterval = oldSampleAgggregatorInterval
	}(rowexec.SamplerProgressInterval, rowexec.SampleAggregatorProgressInterval)
	rowexec.SamplerProgressInterval = 100
	rowexec.SampleAggregatorProgressInterval = time.Millisecond

	var allowRequest chan struct{}

	var serverArgs base.TestServerArgs
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: createStatsRequestFilter(&allowRequest),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, params)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (x INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO d.t SELECT generate_series(1,1000)`)

	t.Run("cancel", func(t *testing.T) {
		// Test that CREATE STATISTICS can be canceled.
		query := fmt.Sprintf(`CREATE STATISTICS s1 FROM d.t`)

		if _, err := jobutils.RunJob(
			t, sqlDB, &allowRequest, []string{"cancel"}, query,
		); err == nil {
			t.Fatal("expected an error")
		}

		// There should be no results here.
		sqlDB.CheckQueryResults(t,
			`SELECT statistics_name, column_names, row_count FROM [SHOW STATISTICS FOR TABLE d.t]`,
			[][]string{})
	})

	t.Run("pause", func(t *testing.T) {
		// Test that CREATE STATISTICS can be paused and resumed.
		query := fmt.Sprintf(`CREATE STATISTICS s2 FROM d.t`)

		jobID, err := jobutils.RunJob(
			t, sqlDB, &allowRequest, []string{"PAUSE"}, query,
		)
		if !testutils.IsError(err, "pause") && !testutils.IsError(err, "liveness") {
			t.Fatalf("unexpected: %v", err)
		}

		// There should be no results here.
		sqlDB.CheckQueryResults(t,
			`SELECT statistics_name, column_names, row_count FROM [SHOW STATISTICS FOR TABLE d.t]`,
			[][]string{})
		opts := retry.Options{
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     time.Second,
			Multiplier:     2,
		}
		if err := retry.WithMaxAttempts(context.Background(), opts, 10, func() error {
			_, err := sqlDB.DB.ExecContext(context.Background(), `RESUME JOB $1`, jobID)
			return err
		}); err != nil {
			t.Fatal(err)
		}

		jobutils.WaitForJob(t, sqlDB, jobID)

		// Now the job should have succeeded in producing stats.
		sqlDB.CheckQueryResults(t,
			`SELECT statistics_name, column_names, row_count FROM [SHOW STATISTICS FOR TABLE d.t]`,
			[][]string{
				{"s2", "{x}", "1000"},
			})
	})
}

// TestCreateStatsLivenessWithRestart tests that a node liveness transition
// during CREATE STATISTICS correctly resumes after the node executing the job
// becomes non-live (from the perspective of the jobs registry).
func TestCreateStatsLivenessWithRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldAdoptInterval, oldCancelInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldAdoptInterval
		jobs.DefaultCancelInterval = oldCancelInterval
	}(jobs.DefaultAdoptInterval, jobs.DefaultCancelInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond
	jobs.DefaultCancelInterval = 100 * time.Millisecond

	const nodes = 1
	nl := jobs.NewFakeNodeLiveness(nodes)
	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			RegistryLiveness: nl,
		},
	}

	var allowRequest chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: createStatsRequestFilter(&allowRequest),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, params)
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `CREATE DATABASE liveness`)
	sqlDB.Exec(t, `CREATE TABLE liveness.t (i INT8 PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO liveness.t SELECT generate_series(1,1000)`)

	const query = `CREATE STATISTICS s1 FROM liveness.t`

	// Start a CREATE STATISTICS run and wait until it's done one scan.
	allowRequest = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := conn.Exec(query)
		errCh <- err
	}()
	select {
	case allowRequest <- struct{}{}:
	case err := <-errCh:
		t.Fatal(err)
	}

	// Fetch the new job ID and lease since we know it's running now.
	var jobID int64
	originalLease := &jobspb.Progress{}
	{
		var expectedLeaseBytes []byte
		sqlDB.QueryRow(
			t, `SELECT id, progress FROM system.jobs ORDER BY created DESC LIMIT 1`,
		).Scan(&jobID, &expectedLeaseBytes)
		if err := protoutil.Unmarshal(expectedLeaseBytes, originalLease); err != nil {
			t.Fatal(err)
		}
	}

	// Make the node non-live and wait for cancellation.
	nl.FakeSetExpiration(1, hlc.MinTimestamp)
	// Wait for the registry cancel loop to run and cancel the job.
	<-nl.SelfCalledCh
	<-nl.SelfCalledCh
	close(allowRequest)
	err := <-errCh
	if !testutils.IsError(err, "job .*: node liveness error") {
		t.Fatalf("unexpected: %v", err)
	}

	// Ensure that complete progress has not been recorded.
	partialProgress := jobutils.GetJobProgress(t, sqlDB, jobID)
	if partialProgress.Progress != nil &&
		partialProgress.Progress.(*jobspb.Progress_FractionCompleted).FractionCompleted == 1 {
		t.Fatal("create stats should not have recorded progress")
	}

	// Make the node live again.
	nl.FakeSetExpiration(1, hlc.MaxTimestamp)

	// The registry should now adopt the job and resume it.
	jobutils.WaitForJob(t, sqlDB, jobID)

	// Verify that the job lease was updated.
	rescheduledProgress := jobutils.GetJobProgress(t, sqlDB, jobID)
	if rescheduledProgress.ModifiedMicros <= originalLease.ModifiedMicros {
		t.Fatalf("expecting rescheduled job to have a later modification time: %d vs %d",
			rescheduledProgress.ModifiedMicros, originalLease.ModifiedMicros)
	}

	// Verify that progress is now recorded.
	if rescheduledProgress.Progress.(*jobspb.Progress_FractionCompleted).FractionCompleted != 1 {
		t.Fatal("create stats should have recorded progress")
	}

	// Now the job should have succeeded in producing stats.
	sqlDB.CheckQueryResults(t,
		`SELECT statistics_name, column_names, row_count FROM [SHOW STATISTICS FOR TABLE liveness.t]`,
		[][]string{
			{"s1", "{i}", "1000"},
		})
}

// TestCreateStatsLivenessWithLeniency tests that a temporary node liveness
// transition during CREATE STATISTICS doesn't cancel the job, but allows the
// owning node to continue processing.
func TestCreateStatsLivenessWithLeniency(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldAdoptInterval, oldCancelInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldAdoptInterval
		jobs.DefaultCancelInterval = oldCancelInterval
	}(jobs.DefaultAdoptInterval, jobs.DefaultCancelInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond
	jobs.DefaultCancelInterval = 100 * time.Millisecond

	const nodes = 1
	nl := jobs.NewFakeNodeLiveness(nodes)
	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			RegistryLiveness: nl,
		},
	}

	var allowRequest chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: createStatsRequestFilter(&allowRequest),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, params)
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	// We want to know exactly how much leniency is configured.
	sqlDB.Exec(t, `SET CLUSTER SETTING jobs.registry.leniency = '1m'`)
	sqlDB.Exec(t, `CREATE DATABASE liveness`)
	sqlDB.Exec(t, `CREATE TABLE liveness.t (i INT8 PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO liveness.t SELECT generate_series(1,1000)`)

	const query = `CREATE STATISTICS s1 FROM liveness.t`

	// Start a CREATE STATISTICS run and wait until it's done one scan.
	allowRequest = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := conn.Exec(query)
		errCh <- err
	}()
	select {
	case allowRequest <- struct{}{}:
	case err := <-errCh:
		t.Fatal(err)
	}

	// Fetch the new job ID and lease since we know it's running now.
	var jobID int64
	originalLease := &jobspb.Payload{}
	{
		var expectedLeaseBytes []byte
		sqlDB.QueryRow(
			t, `SELECT id, payload FROM system.jobs ORDER BY created DESC LIMIT 1`,
		).Scan(&jobID, &expectedLeaseBytes)
		if err := protoutil.Unmarshal(expectedLeaseBytes, originalLease); err != nil {
			t.Fatal(err)
		}
	}

	// Make the node slightly tardy.
	nl.FakeSetExpiration(1, hlc.Timestamp{
		WallTime: hlc.UnixNano() - (15 * time.Second).Nanoseconds(),
	})

	// Wait for the registry cancel loop to run and not cancel the job.
	<-nl.SelfCalledCh
	<-nl.SelfCalledCh
	close(allowRequest)

	// Set the node to be fully live again.  This prevents the registry
	// from canceling all of the jobs if the test node is saturated
	// and the create stats runs slowly.
	nl.FakeSetExpiration(1, hlc.MaxTimestamp)

	// Verify that the client didn't see anything amiss.
	if err := <-errCh; err != nil {
		t.Fatalf("create stats job should have completed: %s", err)
	}

	// The job should have completed normally.
	jobutils.WaitForJob(t, sqlDB, jobID)
}

func TestAtMostOneRunningCreateStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldAdoptInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldAdoptInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	var allowRequest chan struct{}

	var serverArgs base.TestServerArgs
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: createStatsRequestFilter(&allowRequest),
	}

	ctx := context.Background()
	const nodes = 1
	tc := testcluster.StartTestCluster(t, nodes, params)
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (x INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO d.t SELECT generate_series(1,1000)`)

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
		expectErrCh := make(chan error, 1)
		go func() {
			_, err := conn.Exec(`CREATE STATISTICS __auto__ FROM d.t`)
			expectErrCh <- err
		}()
		select {
		case err := <-expectErrCh:
			expected := "another CREATE STATISTICS job is already running"
			if !testutils.IsError(err, expected) {
				t.Fatalf("expected '%s' error, but got %v", expected, err)
			}
		case <-time.After(time.Second):
			panic("CREATE STATISTICS job which was expected to fail, timed out instead")
		}
	}

	// Attempt to start an automatic stats run. It should fail.
	autoStatsRunShouldFail()

	// PAUSE JOB does not bloack until the job is paused but only requests it.
	// Wait until the job is set to paused.
	var jobID int64
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
	jobutils.WaitForJob(t, sqlDB, jobID)
	<-errCh
}

func TestDeleteFailedJob(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldAdoptInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldAdoptInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
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

	defer func(oldProgressInterval time.Duration) {
		rowexec.SampleAggregatorProgressInterval = oldProgressInterval
	}(rowexec.SampleAggregatorProgressInterval)
	rowexec.SampleAggregatorProgressInterval = time.Nanosecond

	defer func(oldProgressInterval int) {
		rowexec.SamplerProgressInterval = oldProgressInterval
	}(rowexec.SamplerProgressInterval)
	rowexec.SamplerProgressInterval = 10

	resetKVBatchSize := row.TestingSetKVBatchSize(10)
	defer resetKVBatchSize()

	var allowRequest chan struct{}
	var serverArgs base.TestServerArgs
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: createStatsRequestFilter(&allowRequest),
	}

	ctx := context.Background()
	const nodes = 1
	tc := testcluster.StartTestCluster(t, nodes, params)
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (i INT8 PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO d.t SELECT generate_series(1,1000)`)

	getFractionCompleted := func(jobID int64) float32 {
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
			t.Fatal(err)
		}
	}

	// Fetch the new job ID since we know it's running now.
	jobID := jobutils.GetLastJobID(t, sqlDB)

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

	// Start another CREATE STATISTICS run and wait until it has scanned part of
	// the table.
	allowRequest = make(chan struct{})
	go func() {
		_, err := conn.Exec(query)
		errCh <- err
	}()
	// Ten iterations here allows us to read some of the rows but not all.
	for i := 0; i < 10; i++ {
		select {
		case allowRequest <- struct{}{}:
		case err := <-errCh:
			t.Fatal(err)
		}
	}

	// Fetch the new job ID since we know it's running now.
	jobID = jobutils.GetLastJobID(t, sqlDB)

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

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
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

// Create a blocking request filter for the actions related
// to CREATE STATISTICS, i.e. Scanning a user table. See discussion
// on jobutils.RunJob for where this might be useful.
func createStatsRequestFilter(allowProgressIota *chan struct{}) kvserverbase.ReplicaRequestFilter {
	return func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		if req, ok := ba.GetArg(roachpb.Scan); ok {
			_, tableID, _ := encoding.DecodeUvarintAscending(req.(*roachpb.ScanRequest).Key)
			// Ensure that the tableID is within the expected range for a table,
			// but is not a system table.
			if tableID > 0 && tableID < 100 && !sqlbase.IsReservedID(sqlbase.ID(tableID)) {
				// Read from the channel twice to allow jobutils.RunJob to complete
				// even though there is only one ScanRequest.
				<-*allowProgressIota
				<-*allowProgressIota
			}
		}
		return nil
	}
}
