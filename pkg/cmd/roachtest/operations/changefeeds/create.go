// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeeds

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// runCreateChangefeeds creates a new changefeed job if the number of current changefeeds is below the allowed maximum.
func runCreateChangefeeds(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	// Establish a connection to the cluster.
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() {
		_ = conn.Close()
	}()

	// Fetch all changefeed jobs that are running or paused.
	allCFJobs, err := fetchAllChangefeedJobsWithStatuses(ctx, conn, jobs.StatusRunning, jobs.StatusPaused)
	if err != nil {
		o.Fatal(err)
	}

	// Get the max allowed changefeeds from environment variables.
	mcf, err := helpers.EnvOrDefaultInt(maxChangefeeds, defaultEnvValuesInt[maxChangefeeds])
	if err != nil {
		o.Fatal(err)
	}

	// If the number of existing changefeeds is below the max limit, create a new one.
	if len(allCFJobs) < mcf {
		// Select a random database and table to create the changefeed for.
		dbName := helpers.PickRandomDB(ctx, o, conn, helpers.SystemDBs)
		tableName := helpers.PickRandomTable(ctx, o, conn, dbName)

		// Attempt to create the changefeed.
		err = createChangefeed(ctx, o, conn, allCFJobs, dbName, tableName)
		if err != nil {
			o.Fatal(err)
		}
	} else {
		o.Status(fmt.Sprintf("total changefeeds %d are more than or equal to the maximum changefeeds criteria %d",
			len(allCFJobs), mcf))
	}

	return nil // No cleanup operation needed.
}

// createChangefeed creates a new changefeed job for the given table and sink.
func createChangefeed(
	ctx context.Context,
	o operation.Operation,
	conn *gosql.DB,
	allCFJobs []*jobDetails,
	dbName, tableName string,
) error {
	// Fetch the changefeed options for all jobs.
	// The payload details are updated in the jobDetails. This is used for sink configs and options.
	err := updatePayloadForJobs(ctx, conn, allCFJobs)
	if err != nil {
		return err
	}
	options := make([]string, 0)
	// Define the sink where the changefeed output will be sent.
	sink, sinkOptions, err := getSinkConfigs(ctx, allCFJobs)
	if err != nil {
		return err
	}
	options = append(options, sinkOptions...)
	// Calculate whether the new changefeed should include an initial scan.
	scanOption, err := calculateScanOption(allCFJobs)
	if err != nil {
		return err
	}
	options = append(options, scanOption)

	o.Status(fmt.Sprintf("creating changefeed job to sink %s with options %v", sink, options))

	// Construct and execute the SQL statement to create the changefeed.
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE CHANGEFEED FOR TABLE %s.%s INTO '%s' WITH %s;",
		dbName, tableName, sink, strings.Join(options, ",")))
	return err
}

// getSinkConfigs returns the sink uri along with the options for creating the changefeed.
// this will be extended later for more sinks
func getSinkConfigs(_ context.Context, _ []*jobDetails) (string, []string, error) {
	return "null://", make([]string, 0), nil
}

// calculateScanOption determines whether the new changefeed should have an initial scan based on existing jobs.
func calculateScanOption(allCFJobs []*jobDetails) (string, error) {
	scanOnCountPercent := 0
	if len(allCFJobs) > 0 {
		scanOnCount := 0
		// Count the number of jobs that have initial scan set to 'yes' or "only".
		for _, j := range allCFJobs {
			if v, ok := j.payload.Opts[changefeedbase.OptInitialScan]; ok {
				if v == "yes" || v == "only" {
					scanOnCount++
				}
			}
		}
		// Calculate the percentage of jobs with initial scan 'yes' or 'only'.
		scanOnCountPercent = scanOnCount * 100 / len(allCFJobs)
	}
	// Get the maximum percentage of changefeeds that can have initial scans enabled.
	maxPctScanOn, err := helpers.EnvOrDefaultInt(maxPctChangeFeedsScanOn, defaultEnvValuesInt[maxPctChangeFeedsScanOn])
	if err != nil {
		return "", err // Return error if fetching environment variable fails.
	}

	// Randomly decide whether the new changefeed should have an initial scan based on the max percentage allowed.
	r, _ := randutil.NewPseudoRand()
	initialScanValue := "no"
	if scanOnCountPercent < maxPctScanOn && randutil.RandIntInRange(r, 0, 100) < maxPctScanOn {
		initialScanValue = "yes"
		// this will select initial_scan as "yes" or "only" with 50% probability.
		if randutil.RandIntInRange(r, 0, 100) < 50 {
			initialScanValue = "only"
		}
	}
	resolved := ""
	if initialScanValue != "only" {
		resolved = ",resolved"
	}
	return fmt.Sprintf("%s='%s'%s", changefeedbase.OptInitialScan, initialScanValue, resolved), nil
}

// waitForJobToCancel waits until the job's status becomes "canceled" or "failed".
func waitForJobToCreate(ctx context.Context, conn *gosql.DB, createdJob *jobDetails) error {
	var err error
	timeout := time.Now().Add(pollForStatusTimeout)
	// Poll the job status until it is either canceled or failed.
	for createdJob.status != jobs.StatusRunning {
		// Get the latest status of the job.
		createdJob.status, err = getJobIDStatus(ctx, conn, createdJob.jobID)
		if err != nil {
			return err
		}
		if timeout.After(time.Now()) {
			return fmt.Errorf("timed out waiting for the job %s to run", createdJob.jobID)
		}
		// Wait for the pollForStatusInterval duration before checking the status again.
		time.Sleep(pollForStatusInterval)
	}
	return nil
}
