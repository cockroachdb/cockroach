// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
	workloadDuration          = roachtestTimeout - time.Minute*10
)

func registerJobs(r registry.Registry) {
	jobsSpec := r.MakeClusterSpec(nodeCount)

	r.Add(registry.TestSpec{
		Name:              "jobs/stress",
		Owner:             registry.OwnerDisasterRecovery,
		Cluster:           jobsSpec,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		CompatibleClouds:  registry.OnlyGCE,
		Suites:            registry.Suites(registry.Nightly),
		Timeout:           roachtestTimeout,
		Run:               runJobsStress,
	})
}

// runJobsStress spins up a cluster to run a large number of jobs with
// frequent interactions to the job system. The test also runs SHOW JOBS
// periodically to assert it returns in a reasonable amount of itme.
func runJobsStress(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	if c.IsLocal() {
		tableCount = 250
		showJobsTimeout = 30 * time.Second
		pollerMinFrequencySeconds = 5
		workloadDuration = time.Minute * 5

	}
	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, "CREATE DATABASE d")
	sqlDB.Exec(t, "SET DATABASE=d")

	// Because this roachtest spins up and pauses/cancels 5k changefeed jobs
	// really quickly, run the adopt interval which by default only runs every 30s
	// and adopts 10 jobs at a time.
	sqlDB.Exec(t, "SET CLUSTER SETTING jobs.registry.interval.adopt='5s'")

	rng, seed := randutil.NewLockedPseudoRand()
	t.L().Printf("Rand seed: %d", seed)

	done := make(chan struct{})
	earlyExit := make(chan struct{}, 1)
	m := c.NewMonitor(ctx)

	m.Go(func(ctx context.Context) error {
		defer close(done)
		var testTimer timeutil.Timer
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
			var pTimer timeutil.Timer
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
		//nolint:deferloop TODO(#137605)
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

// checkJobQueryLatency asserts that the queries "SHOW JOBS" and 'SHOW JOB
// $changefeedJob" run under a specific latency.
func checkJobQueryLatency(
	ctx context.Context, t test.Test, c cluster.Cluster, rng *rand.Rand,
) error {
	conn := c.Conn(ctx, t.L(), rng.Intn(nodeCount)+1)
	defer conn.Close()
	if err := checkQueryLatency(ctx, "SHOW JOBS", conn, t.L(), showJobsTimeout); err != nil {
		return err
	}

	var randomChangefeedJobID int
	if err := conn.QueryRowContext(ctx, "SELECT job_id FROM [SHOW JOBS] ORDER BY random() LIMIT 1").Scan(&randomChangefeedJobID); err != nil {
		return err
	}

	if err := checkQueryLatency(ctx, redact.Sprintf("SHOW JOB %d", randomChangefeedJobID), conn, t.L(), showJobsTimeout/10); err != nil {
		return err
	}
	return nil
}

func checkQueryLatency(
	ctx context.Context,
	query redact.RedactableString,
	conn *gosql.DB,
	l *logger.Logger,
	timeout time.Duration,
) error {
	queryBegin := timeutil.Now()
	var err error
	if err := timeutil.RunWithTimeout(ctx, query, timeout, func(ctx context.Context) error {
		_, err = conn.ExecContext(ctx, query.StripMarkers())
		return err
	}); err != nil {
		l.Printf("%s query exceeded max latency of %.2f seconds. Ran in %.2f seconds. Try to grab explain analyze", query, timeout.Seconds(), timeutil.Since(queryBegin).Seconds())
		explainErr := timeutil.RunWithTimeout(ctx, redact.Sprintf("%s explain", query), timeout*10, func(ctx context.Context) error {
			var explainAnalyze string
			explainErr := conn.QueryRowContext(ctx, fmt.Sprintf("EXPLAIN ANALYZE (VERBOSE) %s", query)).Scan(&explainAnalyze)
			l.Printf("%s", explainAnalyze)
			return explainErr
		})
		return errors.CombineErrors(err, explainErr)
	}
	l.Printf("%s ran in %.2f seconds", query, timeutil.Since(queryBegin).Seconds())
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
	sqlDB.QueryRow(t, `SELECT count(*) FROM system.jobs WHERE job_type='CHANGEFEED' AND status='canceled'`).Scan(&canceledChangefeedCount)
	if canceledChangefeedCount == 0 {
		failedAssertions = append(failedAssertions, "No changefeed jobs were in the canceled state")
	}

	require.Equal(t, 0, len(failedAssertions), failedAssertions)
}
