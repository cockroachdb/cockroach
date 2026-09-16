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
	"strings"
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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

var (
	tableCount                = 5000
	nodeCount                 = 20
	tableNamePrefix           = "t"
	tableSchema               = "(id INT PRIMARY KEY, s STRING)"
	showJobsTimeout           = time.Minute * 2
	showChangefeedJobsTimeout = showJobsTimeout * 5
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
		CompatibleClouds:  registry.OnlyGCE,
		Suites:            registry.Suites(registry.Weekly),
		Timeout:           roachtestTimeout,
		Benchmark:         true,
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
		showJobsTimeout = 30 * time.Minute
		pollerMinFrequencySeconds = 5
		workloadDuration = time.Minute * 5
	}
	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, "CREATE DATABASE d")
	sqlDB.Exec(t, "SET DATABASE=d")

	// Simulate contention of a cluster 10x the size by reducing job registry
	// intervals.
	sqlDB.Exec(t, "SET CLUSTER SETTING jobs.registry.interval.base=0.1")

	rng, seed := randutil.NewLockedPseudoRand()
	t.L().Printf("Rand seed: %d", seed)

	done := make(chan struct{})
	earlyExit := make(chan struct{}, 1)
	group := t.NewErrorGroup()

	group.Go(func(ctx context.Context, l *logger.Logger) error {
		defer close(done)
		var testTimer timeutil.Timer
		testTimer.Reset(workloadDuration)
		select {
		case <-earlyExit:
			l.Printf("Exiting early")
		case <-testTimer.C:
			l.Printf("workload duration of %s elapsed", workloadDuration)
		}
		return nil
	})

	poller := func(waitTime time.Duration, f func(ctx context.Context, t test.Test, c cluster.Cluster, rng *rand.Rand) error) func(ctx context.Context, _ *logger.Logger) error {

		return func(ctx context.Context, _ *logger.Logger) error {
			var pTimer timeutil.Timer
			defer pTimer.Stop()
			for {
				pTimer.Reset(waitTime)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-done:
					return nil
				case <-pTimer.C:
					if err := f(ctx, t, c, rng); err != nil {
						t.L().Printf("Error running periodic function: %s", err)
						earlyExit <- struct{}{}
						return err
					}
				}
			}
		}
	}
	for i := 0; i < nodeCount; i++ {
		group.Go(poller(time.Second, checkJobQueryLatency))
	}

	waitTime := time.Duration(rng.Intn(pollerMinFrequencySeconds)+1) * time.Second
	jobIDToTableName := make(map[jobspb.JobID]string)
	var jobIDToTableNameMu syncutil.Mutex

	controlChangefeedsWithMap := func(ctx context.Context, t test.Test, c cluster.Cluster, rng *rand.Rand) error {
		return controlChangefeeds(ctx, t, c, rng, jobIDToTableName, &jobIDToTableNameMu)
	}
	group.Go(poller(waitTime, controlChangefeedsWithMap))

	group.Go(poller(time.Minute*5, splitScatterMergeJobsTable))

	group.Go(poller(time.Minute, checkPersistentUnclaimedJobs))

	group.Go(func(ctx context.Context, _ *logger.Logger) error {
		createTablesWithChangefeeds(ctx, t, c, rng, done, jobIDToTableName, &jobIDToTableNameMu)
		return nil
	})

	// TODO(msbutler): consider adding a schema change workload to the existing
	// tables to further stress the job system.

	require.NoError(t, group.WaitE())
	checkJobSystemHealth(ctx, t, c, rng)
	require.NoError(t, checkJobQueryLatency(ctx, t, c, rng))
}

func createTablesWithChangefeeds(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	rng *rand.Rand,
	doneChan chan struct{},
	jobIDToTableName map[jobspb.JobID]string,
	jobIDToTableNameMu *syncutil.Mutex,
) {
	t.L().Printf("Creating %d tables with changefeeds", tableCount)

	sqlDBs := make([]*sqlutils.SQLRunner, nodeCount)
	for i := 0; i < nodeCount; i++ {
		conn := c.Conn(ctx, t.L(), i+1)
		sqlDBs[i] = sqlutils.MakeSQLRunner(conn)
		defer conn.Close() //nolint:deferloop
	}
	sqlDBs[0].Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`)

	for i := 0; i < tableCount; i++ {
		sqlDB := sqlDBs[rng.Intn(nodeCount)]
		tableName := tableNamePrefix + fmt.Sprintf("%d", i)
		sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE %s %s`, tableName, tableSchema))
		sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO %s VALUES (1, 'x'),(2,'y')`, tableName))

		var jobID jobspb.JobID
		sqlDB.QueryRow(t, fmt.Sprintf("CREATE CHANGEFEED FOR %s INTO 'null://' WITH gc_protect_expires_after='6m', protect_data_from_gc_on_pause", tableName)).Scan(&jobID)

		jobIDToTableNameMu.Lock()
		jobIDToTableName[jobID] = tableName
		jobIDToTableNameMu.Unlock()

		if i%(tableCount/5) == 0 {
			t.L().Printf("Created %d tables so far", i)
		}

		select {
		case <-doneChan:
			t.L().Printf("Exiting table creation early")
			return
		default:
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

	toLog := false
	if rng.Intn(100) == 0 {
		toLog = true
	}
	defer conn.Close()
	if err := checkQueryLatency(ctx, "SHOW JOBS", conn, t.L(), showJobsTimeout, toLog); err != nil {
		return err
	}

	if err := checkQueryLatency(ctx, "SHOW CHANGEFEED JOBS", conn, t.L(), showChangefeedJobsTimeout, toLog); err != nil {
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
	toLog bool,
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
	if toLog {
		l.Printf("%s ran in %.2f seconds", query, timeutil.Since(queryBegin).Seconds())
	}
	return nil
}

func controlChangefeeds(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	rng *rand.Rand,
	jobIDToTableName map[jobspb.JobID]string,
	jobIDToTableNameMu *syncutil.Mutex,
) error {
	conn := c.Conn(ctx, t.L(), rng.Intn(nodeCount)+1)
	defer conn.Close()

	jobAction := func(action, gatherQuery, actionQuery string, pctAction float32) error {

		rows, err := conn.QueryContext(ctx, gatherQuery)
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

		errCount := 0
		recreatedCount := 0
		count := int(float32(len(jobs)) * pctAction)
		for i := 0; i < count; i++ {
			jobIdx := rng.Intn(len(jobs))
			jobID := jobs[jobIdx]
			_, err := conn.Exec(actionQuery, jobID)
			if err != nil {
				errCount++
				// Try to recreate the changefeed
				jobIDToTableNameMu.Lock()
				tableName, exists := jobIDToTableName[jobID]
				jobIDToTableNameMu.Unlock()
				if !exists {
					t.L().Printf("No table name found for job %d, skipping recreation", jobID)
					continue
				}
				var newJobID jobspb.JobID
				err := conn.QueryRowContext(ctx, fmt.Sprintf("CREATE CHANGEFEED FOR %s INTO 'null://' WITH gc_protect_expires_after='10m', protect_data_from_gc_on_pause", tableName)).Scan(&newJobID)
				if err == nil {
					jobIDToTableNameMu.Lock()
					jobIDToTableName[newJobID] = tableName
					jobIDToTableNameMu.Unlock()
					recreatedCount++
				} else {
					t.L().Printf("Failed to recreate changefeed for table %s (job %d): %v", tableName, jobID, err)
				}
			}
		}
		t.L().Printf("%s on %d of %d jobs, recreated %d changefeeds, of %d total eligible jobs", action, count-errCount, count, recreatedCount, len(jobs))
		return nil
	}
	if err := jobAction("pause running jobs", "SELECT id from system.jobs where job_type='CHANGEFEED' and status='running'", "PAUSE JOB $1", 0.2); err != nil {
		return err
	}
	if err := jobAction("resume paused jobs", "SELECT id from system.jobs where job_type='CHANGEFEED' and status='paused'", "RESUME JOB $1", 0.2); err != nil {
		return err
	}

	// This call will recreate up to 200 canceled jobs. The resume query will obviously fail on cancelled
	// jobs.
	return jobAction("resume canceled jobs", "SELECT id from system.jobs where job_type='CHANGEFEED' and status='canceled' order by created DESC limit 200", "RESUME JOB $1", 1)
}

func splitScatterMergeJobsTable(
	ctx context.Context, t test.Test, c cluster.Cluster, rng *rand.Rand,
) error {
	conn := c.Conn(ctx, t.L(), rng.Intn(nodeCount)+1)
	defer conn.Close()

	_, err := conn.ExecContext(ctx, "ALTER TABLE system.jobs SPLIT AT (select id from system.jobs order by random() limit 3)")
	if err != nil {
		t.L().Printf("Error splitting %s", err)
	}

	return nil
}

func checkPersistentUnclaimedJobs(
	ctx context.Context, t test.Test, c cluster.Cluster, rng *rand.Rand,
) error {
	conn := c.Conn(ctx, t.L(), rng.Intn(nodeCount)+1)
	defer conn.Close()

	sqlDB := sqlutils.MakeSQLRunner(conn)

	// First, get currently unclaimed running jobs
	rows, err := sqlDB.DB.QueryContext(ctx, `
		SELECT id
		FROM system.jobs
		WHERE claim_instance_id IS NULL
		AND status NOT IN ('paused', 'canceled', 'failed');
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var currentUnclaimedJobIDs []jobspb.JobID
	for rows.Next() {
		var jobID jobspb.JobID
		if err := rows.Scan(&jobID); err != nil {
			return err
		}
		currentUnclaimedJobIDs = append(currentUnclaimedJobIDs, jobID)
	}
	rows.Close()

	// If no unclaimed jobs, nothing to check
	if len(currentUnclaimedJobIDs) == 0 {
		return nil
	}

	// Check progressively back in time (1m, 2m, 3m, 4m, 5m)
	for minutesAgo := 1; minutesAgo <= 5; minutesAgo++ {
		// Build the job ID list for the WHERE IN clause
		jobIDList := make([]string, len(currentUnclaimedJobIDs))
		for i, id := range currentUnclaimedJobIDs {
			jobIDList[i] = fmt.Sprintf("%d", id)
		}
		jobIDsStr := "(" + strings.Join(jobIDList, ", ") + ")"

		t.L().Printf("Checking for %d persistent unclaimed jobs from %d minutes ago", len(jobIDsStr), minutesAgo)

		// Query unclaimed jobs at this time in the past using AOST, filtered by current unclaimed job IDs
		query := fmt.Sprintf(`
			SELECT id
			FROM system.jobs AS OF SYSTEM TIME '-%dm'
			WHERE claim_instance_id IS NULL
			AND status = 'running'
			AND id IN %s
		`, minutesAgo, jobIDsStr)

		rows, err := sqlDB.DB.QueryContext(ctx, query)
		if err != nil {
			return err
		}

		var stillUnclaimedJobIDs []jobspb.JobID
		for rows.Next() {
			var jobID jobspb.JobID
			if err := rows.Scan(&jobID); err != nil {
				rows.Close()
				return err
			}
			stillUnclaimedJobIDs = append(stillUnclaimedJobIDs, jobID)
		}
		rows.Close()

		// If none of the current unclaimed jobs were unclaimed at this time, they're all new
		if len(stillUnclaimedJobIDs) == 0 {
			return nil
		}

		// If we've reached 5 minutes and still have unclaimed jobs, that's an error
		if minutesAgo == 5 {
			return errors.Errorf("there exists %d jobs that have been persistently unclaimed for at least 5 minutes: %v", len(stillUnclaimedJobIDs), stillUnclaimedJobIDs)
		}

		// Update the list to only check the jobs that are still unclaimed
		currentUnclaimedJobIDs = stillUnclaimedJobIDs
	}

	return errors.New("unreachable code reached in checkPersistentUnclaimedJobs")
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
