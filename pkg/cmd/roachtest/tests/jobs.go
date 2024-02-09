// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var (
	tableCount                = 5000
	nodeCount                 = 4
	tableNamePrefix           = "t"
	tableSchema               = "(id INT PRIMARY KEY, s STRING)"
	showJobsTimeout           = time.Minute * 2
	pollerMinFrequencySeconds = 30
	roachtestTimeout          = time.Minute * 45
)

func registerJobs(r registry.Registry) {
	jobsSpec := r.MakeClusterSpec(nodeCount)

	r.Add(registry.TestSpec{
		Name:              "jobs/contention",
		Owner:             registry.OwnerDisasterRecovery,
		Cluster:           jobsSpec,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		CompatibleClouds:  registry.OnlyGCE,
		Suites:            registry.Suites(registry.Nightly),
		Timeout:           roachtestTimeout,
		Run:               runJobsContention,
	})
}

// runJobsContention spins up a cluster to run a large number of jobs with
// frequent interactions to the job system. The test also runs SHOW JOBS
// periodically to assert it returns in a reasonable amount of itme.
func runJobsContention(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	if c.IsLocal() {
		tableCount = 10
		showJobsTimeout = 10 * time.Second
		pollerMinFrequencySeconds = 5
	}
	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, "CREATE DATABASE d")
	sqlDB.Exec(t, "SET DATABASE=d")

	rng, seed := randutil.NewPseudoRand()
	t.L().Printf("Rand seed: %d", seed)

	done := make(chan struct{})
	earlyExit := make(chan struct{}, 1)
	m := c.NewMonitor(ctx)

	m.Go(func(ctx context.Context) error {
		defer close(done)
		testTimer := timeutil.NewTimer()
		workloadDuration := roachtestTimeout - time.Minute*10
		if c.IsLocal() {
			workloadDuration = time.Minute * 1
		}
		testTimer.Reset(workloadDuration)
		select {
		case <-earlyExit:
		case <-testTimer.C:
			testTimer.Read = true
		}
		return nil
	})

	randomPoller := func(f func(ctx context.Context, t test.Test, c cluster.Cluster, rng *rand.Rand) error) func(ctx context.Context) error {

		return func(ctx context.Context) error {
			pTimer := timeutil.NewTimer()
			defer pTimer.Stop()
			for {
				waitTime := time.Duration(rng.Intn(pollerMinFrequencySeconds)+1) * time.Second
				pTimer.Reset(waitTime)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-done:
					return nil
				case <-pTimer.C:
					pTimer.Read = true
					if err := f(ctx, t, c, rng); err != nil {
						earlyExit <- struct{}{}
						return err
					}
				}
			}
		}
	}

	m.Go(randomPoller(checkJobQueryLatency))

	m.Go(randomPoller(pauseResumeChangefeeds))

	createTablesWithChangefeeds(ctx, t, c, rng)

	// TODO(msbutler): consider adding a schema change workload to the existing
	// tables to further stress the job system.

	m.Wait()
	checkJobSystemHealth(ctx, t, c, rng)
}

func createTablesWithChangefeeds(
	ctx context.Context, t test.Test, c cluster.Cluster, rng *rand.Rand,
) {
	t.L().Printf("Creating %d tables with changefeeds", tableCount)

	sqlDBs := make([]*sqlutils.SQLRunner, nodeCount)
	for i := 0; i < nodeCount; i++ {
		conn := c.Conn(ctx, t.L(), i+1)
		sqlDBs[i] = sqlutils.MakeSQLRunner(conn)
		defer conn.Close()
	}

	sqlDBs[0].Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`)
	for i := 0; i < tableCount; i++ {
		sqlDB := sqlDBs[rng.Intn(nodeCount)]
		tableName := tableNamePrefix + fmt.Sprintf("%d", i)
		sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE %s %s`, tableName, tableSchema))
		sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO %s VALUES (1, 'x'),(2,'y')`, tableName))
		sqlDB.Exec(t, fmt.Sprintf("CREATE CHANGEFEED FOR %s INTO 'null://' WITH gc_protect_expires_after='5m', protect_data_from_gc_on_pause", tableName))
		if i%(tableCount/5) == 0 {
			t.L().Printf("Created %d tables so far", i)
		}
	}
	t.L().Printf("Finished creating tables with changefeeds")
}

func checkJobQueryLatency(
	ctx context.Context, t test.Test, c cluster.Cluster, rng *rand.Rand,
) error {
	conn := c.Conn(ctx, t.L(), rng.Intn(nodeCount)+1)
	defer conn.Close()
	queryBegin := timeutil.Now()
	var err error
	if err := timeutil.RunWithTimeout(ctx, "show-jobs", showJobsTimeout, func(ctx context.Context) error {
		_, err = conn.ExecContext(ctx, "SHOW JOBS")
		return err
	}); err != nil {
		t.L().Printf("Show jobs query exceeded max latency of %.2f. Ran in %.2f. Try to grab explain analyze", showJobsTimeout, timeutil.Since(queryBegin).Seconds())
		explainErr := timeutil.RunWithTimeout(ctx, "show-jobs-explain", showJobsTimeout*10, func(ctx context.Context) error {
			var explainAnalyze string
			explainErr := conn.QueryRowContext(ctx, "EXPLAIN ANALYZE (VERBOSE) SHOW JOBS").Scan(&explainAnalyze)
			t.L().Printf("%s", explainAnalyze)
			return explainErr
		})
		return errors.CombineErrors(err, explainErr)
	}
	t.L().Printf("SHOW JOBS ran in %.2f seconds", timeutil.Since(queryBegin).Seconds())
	// TODO(msbutler): once SHOW JOB ID no longer conducts a full scan, test its latency on a random changefeed job.
	return nil
}

func pauseResumeChangefeeds(
	ctx context.Context, t test.Test, c cluster.Cluster, rng *rand.Rand,
) error {
	conn := c.Conn(ctx, t.L(), rng.Intn(nodeCount)+1)
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, "SELECT job_id FROM [SHOW CHANGEFEED JOBS]")
	if err != nil {
		return err
	}
	defer rows.Close()

	var jobs []jobspb.JobID
	for rows.Next() {
		var jobID jobspb.JobID
		if err := rows.Scan(&jobID); err != nil {
			return err
		}
		jobs = append(jobs, jobID)
	}
	rows.Close()

	if len(jobs) < tableCount/10 {
		return nil
	}

	jobAction := func(cmd string, count int) {
		errCount := 0
		for i := 0; i < count; i++ {
			jobIdx := rng.Intn(len(jobs))
			_, err := conn.Exec(cmd, jobs[jobIdx])
			if err != nil {
				errCount++
			}
		}
		t.L().Printf("Failed to run %s on %d of %d jobs", cmd, errCount, count)
	}
	jobAction("PAUSE JOB $1", len(jobs)/10)
	jobAction("RESUME JOB $1", len(jobs)/10)
	return nil
}

func checkJobSystemHealth(ctx context.Context, t test.Test, c cluster.Cluster, rng *rand.Rand) {
	conn := c.Conn(ctx, t.L(), rng.Intn(nodeCount)+1)
	defer conn.Close()

	var failedAssertions []string

	sqlDB := sqlutils.MakeSQLRunner(conn)

	// Check that there aren't any orphaned job info rows
	var orphanedInfoRowCount int
	sqlDB.QueryRow(t, `SELECT count(*) FROM system.job_info
	WHERE job_id NOT IN (SELECT id FROM system.jobs)`).Scan(&orphanedInfoRowCount)
	if orphanedInfoRowCount != 0 {
		failedAssertions = append(failedAssertions, fmt.Sprintf("there exists %d orphaned job info rows", orphanedInfoRowCount))
	}

	// Check that there aren't any orphaned job rows
	var orphanedRowCount int
	sqlDB.QueryRow(t, `SELECT count(*) FROM system.jobs
 WHERE id NOT IN (SELECT job_id FROM system.job_info)`).Scan(&orphanedRowCount)
	if orphanedRowCount != 0 {
		failedAssertions = append(failedAssertions, fmt.Sprintf("there exists %d orphaned job rows", orphanedRowCount))
	}

	// Check that some changefeed jobs were canceled by the pts poller
	//
	// TODO(msbutler): once we speed up cancel-requested processing, we ought to
	// assert that relatively jobs are in the cancel requested state.
	var canceledChangefeedCount int
	sqlDB.QueryRow(t, `SELECT count(*) FROM system.jobs WHERE job_type='CHANGEFEED' AND status='canceled';`).Scan(&canceledChangefeedCount)
	if canceledChangefeedCount == 0 {
		failedAssertions = append(failedAssertions, "No changefeed jobs were in the canceled state")
	}

	// Check That the Poller job has run at least once. We expect it to run quite slowly, but hopefully it's run at least once.
	var lastRun time.Time
	sqlDB.QueryRow(t, "SELECT last_run FROM system.jobs WHERE job_type = 'POLL JOBS STATS';").Scan(&lastRun)
	t.L().Printf("Last run Poller Run %s", lastRun)
	if lastRun.Compare(timeutil.Now().Add(-time.Minute*40)) < 0 {
		failedAssertions = append(failedAssertions, fmt.Sprintf("last POLL JOB STATS update %s is over 40 minutes old", lastRun))
	}

	require.Equal(t, 0, len(failedAssertions), failedAssertions)
}
