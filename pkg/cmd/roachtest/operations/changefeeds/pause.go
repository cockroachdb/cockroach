// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeeds

import (
	"context"
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// runPauseChangefeeds pauses a percentage of running changefeed jobs.
func runPauseChangefeeds(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {

	// Connect to the cluster and ensure the connection is closed after use.
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() {
		_ = conn.Close()
	}()

	// Fetch all running changefeed jobs.
	allRunningCFJobs, err := fetchAllChangefeedJobsWithStatuses(ctx, conn, jobs.StatusRunning)
	if err != nil {
		o.Fatal(err)
	}

	// Retrieve the minimum number of changefeeds to keep running.
	mcf, err := helpers.EnvOrDefaultInt(minChangefeeds, defaultEnvValuesInt[minChangefeeds])
	if err != nil {
		o.Fatal(err)
	}

	// If the number of changefeed jobs exceeds the minimum, start pausing jobs.
	if len(allRunningCFJobs) >= mcf {
		err = pauseChangefeeds(ctx, o, conn, allRunningCFJobs)
		if err != nil {
			o.Fatal(err)
		}
	} else {
		o.Status(fmt.Sprintf("total running changefeeds %d are less than the minimum changefeeds criteria %d",
			len(allRunningCFJobs), mcf))
	}

	// Return a nil cleanup function as there's no specific cleanup required here.
	return nil
}

// pauseChangefeeds pauses a percentage of changefeed jobs.
func pauseChangefeeds(
	ctx context.Context, o operation.Operation, conn *gosql.DB, allCFJobs []*jobDetails,
) error {

	// Retrieve the percentage of changefeeds to pause from the environment.
	pct, err := helpers.EnvOrDefaultInt(pctChangefeedsToPause, defaultEnvValuesInt[pctChangefeedsToPause])
	if err != nil {
		return err
	}

	// Initialize a random number generator.
	r, _ := randutil.NewPseudoRand()

	// Calculate the total number of jobs to pause based on the percentage.
	totalJobsToPause := pct * len(allCFJobs) / 100
	o.Status(fmt.Sprintf("Pausing %d jobs out of %d", totalJobsToPause, len(allCFJobs)))

	// Loop through and pause jobs until the required number is paused.
	for totalJobsToPause > 0 {
		// Randomly select a job to pause.
		jobIndex := randutil.RandIntInRange(r, 0, len(allCFJobs))
		jobToPause := allCFJobs[jobIndex]

		// Skip if the job is already paused.
		if jobToPause.status == jobs.StatusPaused {
			continue
		}

		// Log the pause action and execute the pause command for the selected job.
		o.Status("Pausing job ", jobToPause.jobID)
		statement := fmt.Sprintf("PAUSE JOB %s;", jobToPause.jobID)
		_, err = conn.ExecContext(ctx, statement)
		if err != nil {
			return err
		}

		// Wait for the job to be fully paused.
		err = waitForJobToPause(ctx, conn, jobToPause)
		if err != nil {
			return err
		}

		// Log the successful pausing and decrement the remaining jobs to pause.
		o.Status(fmt.Sprintf("Job %s is paused", jobToPause.jobID))
		totalJobsToPause--
	}

	return err
}

// waitForJobToPause waits until the job's status becomes "paused" or "failed".
func waitForJobToPause(ctx context.Context, conn *gosql.DB, jobToPause *jobDetails) error {
	var err error

	// Poll the job status until it is either paused or failed.
	for jobToPause.status != jobs.StatusPaused && jobToPause.status != jobs.StatusFailed {
		// Get the latest status of the job.
		jobToPause.status, err = getJobIDStatus(ctx, conn, jobToPause.jobID)
		if err != nil {
			return err
		}

		// Wait for the pollForStatusInterval duration before checking the status again.
		time.Sleep(pollForStatusInterval)
	}
	return nil
}
