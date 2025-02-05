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

// runCancelChangefeeds cancels a running or paused changefeed job.
func runCancelChangefeeds(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {

	// Connect to the cluster and ensure the connection is closed after use.
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() {
		_ = conn.Close()
	}()

	// Fetch all running or paused changefeed jobs.
	allCFJobs, err := fetchAllChangefeedJobsWithStatuses(ctx, conn, jobs.StatusRunning, jobs.StatusPaused)
	if err != nil {
		o.Fatal(err)
	}

	// Retrieve the minimum number of changefeeds to keep running.
	mcf, err := helpers.EnvOrDefaultInt(minChangefeeds, defaultEnvValuesInt[minChangefeeds])
	if err != nil {
		o.Fatal(err)
	}

	// If the number of changefeed jobs exceeds the minimum, start canceling jobs.
	if len(allCFJobs) >= mcf {
		err = cancelChangefeed(ctx, o, conn, allCFJobs)
		if err != nil {
			o.Fatal(err)
		}
	} else {
		o.Status(fmt.Sprintf("total running or paused changefeeds %d are less than the minimum changefeeds criteria %d",
			len(allCFJobs), mcf))
	}

	// Return a nil cleanup function as there's no specific cleanup required here.
	return nil
}

// cancelChangefeed cancels a randomly selected changefeed jobs.
func cancelChangefeed(
	ctx context.Context, o operation.Operation, conn *gosql.DB, allCFJobs []*jobDetails,
) error {

	// Initialize a random number generator.
	r, _ := randutil.NewPseudoRand()

	// Randomly select a job to cancel.
	jobIndex := randutil.RandIntInRange(r, 0, len(allCFJobs))
	jobToCancel := allCFJobs[jobIndex]

	o.Status("Cancelling job ", jobToCancel.jobID)
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CANCEL JOB %s;", jobToCancel.jobID))
	if err != nil {
		return err
	}

	// Wait for the job to be fully canceled.
	err = waitForJobToCancel(ctx, conn, jobToCancel)
	if err != nil {
		return err
	}

	// Log the successful cancellation and decrement the remaining jobs to cancel.
	o.Status(fmt.Sprintf("Job %s is cancelled", jobToCancel.jobID))
	return err
}

// waitForJobToCancel waits until the job's status becomes "canceled" or "failed".
func waitForJobToCancel(ctx context.Context, conn *gosql.DB, jobToCancel *jobDetails) error {
	var err error

	// Poll the job status until it is either canceled or failed.
	for jobToCancel.status != jobs.StatusCanceled && jobToCancel.status != jobs.StatusFailed {
		// Get the latest status of the job.
		jobToCancel.status, err = getJobIDStatus(ctx, conn, jobToCancel.jobID)
		if err != nil {
			return err
		}

		// Wait for the pollForStatusInterval duration before checking the status again.
		time.Sleep(pollForStatusInterval)
	}
	return nil
}
