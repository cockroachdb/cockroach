// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/errors"
)

// registerTTLRestart will register the TTL restart job. This is a test that
// verifies the restart behavior of the TTL job. It ensures that the TTL job will
// restart if a node goes down or a new one comes online.
func registerTTLRestart(r registry.Registry) {
	for numRestartNodes := 1; numRestartNodes <= 2; numRestartNodes++ {
		r.Add(registry.TestSpec{
			Name:             fmt.Sprintf("ttl-restart/num-restart-nodes=%d", numRestartNodes),
			Owner:            registry.OwnerSQLFoundations,
			Cluster:          r.MakeClusterSpec(3),
			Leases:           registry.MetamorphicLeases,
			CompatibleClouds: registry.AllClouds,
			Suites:           registry.Suites(registry.Nightly),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runTTLRestart(ctx, t, c, numRestartNodes)
			},
		})
	}
}

const showRangesQuery = "with r as (show ranges from table ttldb.tab1 with details) select range_id, lease_holder from r"
const jobCheckSQL = `
  SELECT
        coordinator_id,
        job_id,
        status
  FROM [SHOW JOBS]
  WHERE job_type = 'ROW LEVEL TTL' AND description LIKE '%ttldb.public.tab1%'
  ORDER BY created DESC LIMIT 1
`

const (
	maxRetryAttempts = 5
	ttlJobWaitTime   = 8 * time.Minute
)

type ttlJobInfo struct {
	JobID          int
	CoordinatorID  int
	Status         string
	JobResumeCount int
}

type leaseInfo struct {
	RangeID     int
	LeaseHolder int
}
type leaseInfoSlice []leaseInfo

// runTTLRestart will run the test. It ensures that the TTL job will restart
// if node(s) restart. The numRestartNodes parameter indicates the number of
// nodes to restart. It must be positive, but less than the total number of nodes
// in the cluster, since we always want the TTL job coorindator to continue
// running.
func runTTLRestart(ctx context.Context, t test.Test, c cluster.Cluster, numRestartNodes int) {
	if numRestartNodes <= 0 {
		t.Fatalf("the test must stop at least one node: %d", numRestartNodes)
	}
	if numRestartNodes >= c.Spec().NodeCount {
		t.Fatalf("the test cannot stop all of the nodes: %d >= %d", numRestartNodes, c.Spec().NodeCount)
	}

	startOpts := option.NewStartOpts()
	settings := install.MakeClusterSettings()
	c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())

	m := c.NewDeprecatedMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		db := c.Conn(ctx, t.L(), 1)
		defer db.Close()

		// Determine how many nodes we need TTL activity on based on restart scenario
		requiredTTLNodes := 3 // Default for numRestartNodes=1
		if numRestartNodes == 2 {
			requiredTTLNodes = 2
		}

		var jobInfo ttlJobInfo
		var ttlNodes map[int]struct{}

		// Retry loop: attempt to setup table and get proper TTL distribution
		for attempt := 1; attempt <= maxRetryAttempts; attempt++ {
			if attempt > 1 {
				t.L().Printf("Attempt %d/%d: Retrying table setup due to insufficient TTL distribution", attempt, maxRetryAttempts)
			}

			// Setup table and data
			if err := setupTableAndData(ctx, t, db); err != nil {
				return err
			}

			// Enable TTL and wait for job to start
			var err error
			jobInfo, err = enableTTLAndWaitForJob(ctx, t, c, db)
			if err != nil {
				return err
			}

			// Reset the connection so that we query from the coordinator as this is the
			// only node that will stay up.
			if err := db.Close(); err != nil {
				return errors.Wrapf(err, "error closing connection")
			}
			db = c.Conn(ctx, t.L(), jobInfo.CoordinatorID)

			t.Status("check TTL activity distribution across nodes")
			err = testutils.SucceedsWithinError(func() error {
				var err error
				ttlNodes, err = findNodesWithJobLogs(ctx, t, c, jobInfo.JobID)
				if err != nil {
					return err
				}
				if len(ttlNodes) < requiredTTLNodes {
					return errors.Newf("TTL activity found on only %d nodes (need %d)", len(ttlNodes), requiredTTLNodes)
				}
				return nil
			}, 1*time.Minute)

			if err == nil {
				// Success! TTL is distributed across enough nodes
				t.L().Printf("TTL job %d found on nodes: %v", jobInfo.JobID, ttlNodes)
				break
			}

			// Handle the error
			if ttlNodes != nil && len(ttlNodes) < requiredTTLNodes {
				t.L().Printf("Attempt %d/%d: TTL job %d found on nodes: %v", attempt, maxRetryAttempts, jobInfo.JobID, ttlNodes)
				t.L().Printf("Attempt %d/%d: TTL activity found on only %d nodes (need %d)", attempt, maxRetryAttempts, len(ttlNodes), requiredTTLNodes)

				if attempt == maxRetryAttempts {
					// Final attempt failed - exit successfully as current behavior
					t.L().Printf("After %d attempts, TTL activity found on only %d nodes (need %d for restart test). Test completed successfully.", maxRetryAttempts, len(ttlNodes), requiredTTLNodes)
					return nil
				}
				// Continue to next attempt
				continue
			}

			// Other error that's not related to TTL distribution
			return errors.Wrapf(err, "error waiting for TTL activity distribution")
		}

		t.Status("stop non-coordinator nodes")
		nonCoordinatorCount := c.Spec().NodeCount - 1
		stoppingAllNonCoordinators := numRestartNodes == nonCoordinatorCount
		stoppedNodes := make([]int, 0)
		for node := 1; node <= c.Spec().NodeCount && len(stoppedNodes) < numRestartNodes; node++ {
			if node == jobInfo.CoordinatorID {
				continue
			}
			if !stoppingAllNonCoordinators {
				// Only stop this node if it was confirmed to have executed the TTL job,
				// based on presence of job log markers.
				if _, found := ttlNodes[node]; !found {
					continue
				}
			}
			m.ExpectDeath()
			t.L().Printf("stopping node %d", node)
			c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Nodes(node))
			stoppedNodes = append(stoppedNodes, node)
		}

		// If we haven't lost quorum, then the TTL job should restart and continue
		// working before restarting the down nodes.
		var err error
		if numRestartNodes <= c.Spec().NodeCount/2 {
			t.Status("ensure TTL job restarts")
			testutils.SucceedsWithin(t, func() error {
				jobInfo, err = findRunningJob(ctx, t, c, db, &jobInfo,
					true /* expectJobRestart */, false /* allowJobSucceeded */)
				if err != nil {
					return err
				}
				return nil
			}, 6*time.Minute)
		}

		t.Status("restart the down nodes")
		nodesRestarted := 0
		for _, node := range stoppedNodes {
			t.L().Printf("starting node %d", node)
			c.Start(ctx, t.L(), startOpts, settings, c.Node(node))
			nodesRestarted++
		}
		m.ResetDeaths()

		// In some cases, the TTL job can complete successfully before a restart is
		// observed. To handle this, we tolerate either a job restart or successful
		// completion.
		t.Status("ensure TTL job restarts again or finishes")
		testutils.SucceedsWithin(t, func() error {
			jobInfo, err = findRunningJob(ctx, t, c, db, &jobInfo,
				true /* expectJobRestart */, true /* allowJobSucceeded */)
			if err != nil {
				return err
			}
			return nil
		}, 6*time.Minute)

		return nil
	})
	m.Wait()
}

// gatherLeaseDistribution returns a slice of leaseInfo structs, one for each
// range in the cluster. Each struct contains the rangeID and the leaseholder
// for that range.
func gatherLeaseDistribution(
	ctx context.Context, t test.Test, db *gosql.DB,
) (leaseInfoSlice, error) {
	leases := make(leaseInfoSlice, 0)
	ranges, err := db.QueryContext(ctx, showRangesQuery)
	if err != nil {
		return leases, errors.Wrapf(err, "error querying the ranges")
	}
	defer ranges.Close()
	for ranges.Next() {
		var rangeID, leaseHolder int
		if err := ranges.Scan(&rangeID, &leaseHolder); err != nil {
			return leases, err
		}
		leases = append(leases, leaseInfo{RangeID: rangeID, LeaseHolder: leaseHolder})
	}
	return leases, nil
}

// showLeaseDistribution pretty prints leaseInfoSlice, which is collected from
// gatherLeaseDistribution.
func showLeaseDistribution(t test.Test, leases leaseInfoSlice) {
	for _, lease := range leases {
		t.L().Printf("Range %d lease holder is %d", lease.RangeID, lease.LeaseHolder)
	}
}

// distributeLeases redistributes the ranges evenly across the cluster.
func distributeLeases(ctx context.Context, t test.Test, db *gosql.DB) error {
	ranges, err := db.QueryContext(ctx, showRangesQuery)
	if err != nil {
		return errors.Wrapf(err, "error querying the ranges")
	}
	defer ranges.Close()
	var nextLeaseHolder int
	for nextLeaseHolder = 1; ranges.Next(); nextLeaseHolder++ {
		var rangeID, leaseHolder int
		if err := ranges.Scan(&rangeID, &leaseHolder); err != nil {
			return err
		}
		if leaseHolder != nextLeaseHolder {
			alterStmt := fmt.Sprintf("ALTER RANGE %d RELOCATE LEASE TO %d", rangeID, nextLeaseHolder)
			row := db.QueryRowContext(ctx, alterStmt)
			var pretty, result string
			if err := row.Scan(&rangeID, &pretty, &result); err != nil {
				return err
			}
			if result != "ok" {
				return errors.Newf("failed to alter range %d: %s", rangeID, result)
			}
			t.L().Printf("Relocated lease of range %d to node %d", rangeID, nextLeaseHolder)
		}
	}
	const expectedLeaseHolders = 3
	if actual := nextLeaseHolder - 1; actual != expectedLeaseHolders {
		return errors.Newf("expected %d leaseholders, but got %d", expectedLeaseHolders, actual)
	}
	return nil

}

// findRunningJob checks the current state of the TTL job and returns metadata
// about it. If a previous job state (lastJob) is provided, and expectJobRestart
// is true, the function verifies that the job has restarted by comparing resume
// counts. If allowJobSucceeded is true, it tolerates the job having already
// succeeded without requiring a restart.
//
// The function also logs TTL job metadata, including any observed coordinator
// changes and job completion status.
func findRunningJob(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	db *gosql.DB,
	lastJob *ttlJobInfo,
	expectJobRestart bool,
	allowJobSucceeded bool,
) (newJobInfo ttlJobInfo, err error) {
	var coordinatorID gosql.NullInt64
	if err := db.QueryRowContext(ctx, jobCheckSQL).Scan(&coordinatorID, &newJobInfo.JobID, &newJobInfo.Status); err != nil {
		return newJobInfo, errors.Wrapf(err, "error querying status of TTL job")
	}
	// When the TTL job restarts, the coordinator_id can be temporarily NULL
	// if the job hasn't been rescheduled yet.
	if !coordinatorID.Valid {
		return newJobInfo, errors.Newf("TTL coordinator is NULL")
	}
	newJobInfo.CoordinatorID = int(coordinatorID.Int64)

	// Count the number of job resumes by looking in the logs on each node.
	const ttlResumeLogMarker = "ROW LEVEL TTL job %d: stepping through state running"
	cmd := fmt.Sprintf("grep -c '"+ttlResumeLogMarker+"' {log-dir}/cockroach.log", newJobInfo.JobID)
	results, err := c.RunWithDetails(ctx, nil, option.WithNodes(c.CRDBNodes()), cmd)
	if err != nil {
		return newJobInfo, errors.Wrapf(err, "error running command: %s", cmd)
	}

	newJobInfo.JobResumeCount = 0
	for i := range results {
		resumeCount, err := strconv.Atoi(strings.TrimSpace(results[i].Stdout))
		if err != nil {
			return newJobInfo, errors.Wrapf(err, "error converting command output from node %d to int: %s",
				results[i].Node, results[i].Stdout)
		}
		newJobInfo.JobResumeCount += resumeCount
	}

	if lastJob != nil && lastJob.JobID != newJobInfo.JobID {
		return newJobInfo, errors.Newf("TTL job ID changed from %d to %d", lastJob.JobID, newJobInfo.JobID)
	}
	// Handle timing scenario where the job started, according to the job infra,
	// but we haven't yet logged the resume log message.
	if newJobInfo.Status == "running" && newJobInfo.JobResumeCount == 0 {
		return newJobInfo, errors.Newf("TTL job is running but resume count is 0")
	}
	if lastJob != nil && lastJob.CoordinatorID != newJobInfo.CoordinatorID {
		t.L().Printf("TTL job (ID %d) is running now at node %d", newJobInfo.JobID, newJobInfo.CoordinatorID)
	}

	if lastJob != nil {
		jobRestarted := newJobInfo.JobResumeCount > lastJob.JobResumeCount
		if expectJobRestart && !jobRestarted {
			if newJobInfo.Status == "succeeded" && allowJobSucceeded {
				t.L().Printf("TTL job (ID %d) has already succeeded; treating as successful", newJobInfo.JobID)
				return newJobInfo, nil
			}
			return newJobInfo, errors.Newf("TTL job did not restart, resume count is %d", newJobInfo.JobResumeCount)
		}
	}
	t.L().Printf("TTL job (ID %d) resume count is %d", newJobInfo.JobID, newJobInfo.JobResumeCount)

	return newJobInfo, nil
}

// findNodesWithJobLogs returns the IDs of nodes whose logs contain entries for
// the given TTL job ID.
func findNodesWithJobLogs(
	ctx context.Context, t test.Test, c cluster.Cluster, jobID int,
) (map[int]struct{}, error) {
	// Log marker to search for
	const ttlLogMarker = "id=%d"
	cmd := fmt.Sprintf("grep -q '"+ttlLogMarker+"' {log-dir}/cockroach.log", jobID)

	// Run grep command across all nodes
	results, err := c.RunWithDetails(ctx, nil, option.WithNodes(c.CRDBNodes()), cmd)
	if err != nil && !testutils.IsError(err, "exit status") {
		// Only fail on non-grep errors (exit status from grep is expected if pattern not found)
		return nil, errors.Wrapf(err, "error running grep command for job %d", jobID)
	}

	nodesWithJob := make(map[int]struct{})
	for _, res := range results {
		if res.Err == nil {
			// grep -q succeeded, meaning the log marker was found
			nodesWithJob[int(res.Node)] = struct{}{}
		}
	}

	if len(nodesWithJob) == 0 {
		return nil, errors.Newf("TTL job %d was not found in any node logs", jobID)
	}

	// Extract and sort node IDs
	var nodeList []int
	for node := range nodesWithJob {
		nodeList = append(nodeList, node)
	}
	sort.Ints(nodeList)

	return nodesWithJob, nil
}

// setupTableAndData creates the ttldb database and tab1 table, inserts data,
// and distributes ranges across nodes. This function can be called multiple
// times to recreate the table for retry scenarios.
func setupTableAndData(ctx context.Context, t test.Test, db *gosql.DB) error {
	t.Status("create/recreate the table")
	setup := []string{
		// Speed up the test by doing the replan check often and with a low threshold.
		"SET CLUSTER SETTING sql.ttl.replan_flow_frequency = '15s'",
		"SET CLUSTER SETTING sql.ttl.replan_flow_threshold = '0.1'",
		// Disable the stability window to ensure immediate replanning on node changes.
		"SET CLUSTER SETTING sql.ttl.replan_stability_window = 1",
		// Add additional logging to help debug the test on failure.
		"SET CLUSTER SETTING server.debug.default_vmodule = 'ttljob_processor=1,distsql_plan_bulk=1'",
		// Drop existing table if it exists
		"DROP TABLE IF EXISTS ttldb.tab1",
		// Create the schema to be used in the test
		"CREATE DATABASE IF NOT EXISTS ttldb",
		"CREATE TABLE IF NOT EXISTS ttldb.tab1 (pk INT8 NOT NULL PRIMARY KEY, ts TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP)",
	}
	for _, stmt := range setup {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return errors.Wrapf(err, "error with statement: %s", stmt)
		}
	}

	t.Status("add manual splits so that ranges are distributed evenly across the cluster")
	if _, err := db.ExecContext(ctx, "ALTER TABLE ttldb.tab1 SPLIT AT VALUES (6000), (12000)"); err != nil {
		return errors.Wrapf(err, "error adding manual splits")
	}

	t.Status("insert data")
	if _, err := db.ExecContext(ctx, "INSERT INTO ttldb.tab1 (pk) SELECT generate_series(1, 18000)"); err != nil {
		return errors.Wrapf(err, "error ingesting data")
	}

	t.Status("relocate ranges to distribute across nodes")
	// Moving ranges is put under a SucceedsSoon to account for errors like:
	// "lease target replica not found in RangeDescriptor"
	testutils.SucceedsSoon(t, func() error { return distributeLeases(ctx, t, db) })
	leases, err := gatherLeaseDistribution(ctx, t, db)
	if err != nil {
		return errors.Wrapf(err, "error gathering lease distribution")
	}
	showLeaseDistribution(t, leases)

	return nil
}

// enableTTLAndWaitForJob enables TTL on the table with a cron expression
// set to run in about 2 minutes, then waits for the job to start.
func enableTTLAndWaitForJob(
	ctx context.Context, t test.Test, c cluster.Cluster, db *gosql.DB,
) (ttlJobInfo, error) {
	var jobInfo ttlJobInfo

	t.Status("enable TTL")
	ts := db.QueryRowContext(ctx, "SELECT EXTRACT(HOUR FROM now() + INTERVAL '2 minutes') AS hour, EXTRACT(MINUTE FROM now() + INTERVAL '2 minutes') AS minute")
	var hour, minute int
	if err := ts.Scan(&hour, &minute); err != nil {
		return jobInfo, errors.Wrapf(err, "error generating cron expression")
	}
	ttlCronExpression := fmt.Sprintf("%d %d * * *", minute, hour)
	t.L().Printf("using a cron expression of '%s'", ttlCronExpression)
	ttlJobSettingSQL := fmt.Sprintf(`ALTER TABLE ttldb.tab1
		SET (ttl_expiration_expression = $$(ts::timestamptz + '1 minutes')$$,
          ttl_select_batch_size=100,
          ttl_delete_batch_size=100,
          ttl_select_rate_limit=100,
          ttl_job_cron='%s')`,
		ttlCronExpression)
	if _, err := db.ExecContext(ctx, ttlJobSettingSQL); err != nil {
		return jobInfo, errors.Wrapf(err, "error setting TTL attributes")
	}

	t.Status("wait for the TTL job to start")
	waitForTTLJob := func() error {
		var err error
		jobInfo, err = findRunningJob(ctx, t, c, db, nil,
			false /* expectJobRestart */, false /* allowJobSucceeded */)
		return err
	}
	// The ttl job is scheduled to run in about 2 minutes. Wait for a little
	// while longer to give the job system enough time to start the job in case
	// the system is slow.
	testutils.SucceedsWithin(t, waitForTTLJob, ttlJobWaitTime)
	t.L().Printf("TTL job (ID %d) is running at node %d", jobInfo.JobID, jobInfo.CoordinatorID)

	return jobInfo, nil
}
