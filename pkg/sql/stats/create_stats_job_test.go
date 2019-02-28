// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package stats_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// TestCreateStatsControlJob tests that PAUSE JOB, RESUME JOB, and CANCEL JOB
// work as intended on create statistics jobs.
func TestCreateStatsControlJob(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	// Test with 3 nodes and distsqlrun.SamplerProgressInterval=100 to ensure
	// that progress metadata is sent correctly after every 100 input rows.
	const nodes = 3
	defer func(oldSamplerInterval int, oldSampleAgggregatorInterval time.Duration) {
		distsqlrun.SamplerProgressInterval = oldSamplerInterval
		distsqlrun.SampleAggregatorProgressInterval = oldSampleAgggregatorInterval
	}(distsqlrun.SamplerProgressInterval, distsqlrun.SampleAggregatorProgressInterval)
	distsqlrun.SamplerProgressInterval = 100
	distsqlrun.SampleAggregatorProgressInterval = time.Millisecond

	var allowRequest chan struct{}

	var serverArgs base.TestServerArgs
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
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
		); !testutils.IsError(err, "job canceled") {
			t.Fatalf("expected 'job canceled' error, but got %+v", err)
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
		if !testutils.IsError(err, "job paused") {
			t.Fatalf("unexpected: %v", err)
		}

		// There should be no results here.
		sqlDB.CheckQueryResults(t,
			`SELECT statistics_name, column_names, row_count FROM [SHOW STATISTICS FOR TABLE d.t]`,
			[][]string{})

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, jobID))
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
	params.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
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
	params.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
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
	params.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
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

	// Attempt to start an automatic stats run. It should fail.
	_, err := conn.Exec(`CREATE STATISTICS __auto__ FROM d.t`)
	expected := "another CREATE STATISTICS job is already running"
	if !testutils.IsError(err, expected) {
		t.Fatalf("expected '%s' error, but got %v", expected, err)
	}

	// Pause the job. Starting another automatic stats run should still fail.
	var jobID int64
	sqlDB.QueryRow(t, `SELECT id FROM system.jobs ORDER BY created DESC LIMIT 1`).Scan(&jobID)
	sqlDB.Exec(t, fmt.Sprintf("PAUSE JOB %d", jobID))

	_, err = conn.Exec(`CREATE STATISTICS __auto__ FROM d.t`)
	expected = "another CREATE STATISTICS job is already running"
	if !testutils.IsError(err, expected) {
		t.Fatalf("expected '%s' error, but got %v", expected, err)
	}

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

	sqlDB.Exec(t, fmt.Sprintf("CREATE STATISTICS s FROM d.t AS OF SYSTEM TIME %s", ts1))

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
func createStatsRequestFilter(allowProgressIota *chan struct{}) storagebase.ReplicaRequestFilter {
	return func(ba roachpb.BatchRequest) *roachpb.Error {
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
