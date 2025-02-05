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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/jobs"
)

// runResumeChangefeeds resumes all paused changefeed jobs by interacting with the cluster.
func runResumeChangefeeds(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	// Establish a connection to the cluster.
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() {
		_ = conn.Close()
	}()

	// Fetch all changefeed jobs with the status 'paused'.
	allPausedCFJobs, err := fetchAllChangefeedJobsWithStatuses(ctx, conn, jobs.StatusPaused)
	if err != nil {
		o.Fatal(err)
	}

	if len(allPausedCFJobs) == 0 {
		o.Status("no jobs are available to resume.")
		return nil
	}
	// Resume the fetched changefeed jobs.
	err = resumeChangefeeds(ctx, o, conn, allPausedCFJobs)
	if err != nil {
		o.Fatal(err)
	}

	return nil // No cleanup operation needed.
}

// resumeChangefeeds resumes all paused changefeed jobs in the provided job list.
func resumeChangefeeds(
	ctx context.Context, o operation.Operation, conn *gosql.DB, allCFJobs []*jobDetails,
) error {
	// Loop through each job and attempt to resume it.
	for _, jobToResume := range allCFJobs {
		o.Status("Resuming job ", jobToResume.jobID)
		statement := fmt.Sprintf("RESUME JOB %s;", jobToResume.jobID)
		_, err := conn.ExecContext(ctx, statement)
		if err != nil {
			return err // Return error if the SQL statement execution fails.
		}

		// Wait for the job to be successfully resumed.
		err = waitForJobToResume(ctx, conn, jobToResume)
		if err != nil {
			return err // Return error if the job fails to resume.
		}

		o.Status(fmt.Sprintf("Job %s is resumed", jobToResume.jobID))
	}
	return nil // Return nil if all jobs are resumed successfully.
}

// waitForJobToResume checks if a changefeed job has successfully resumed.
func waitForJobToResume(ctx context.Context, conn *gosql.DB, jobToResume *jobDetails) error {
	var err error
	// Poll the job's status until it is either running or failed.
	for jobToResume.status != jobs.StatusRunning && jobToResume.status != jobs.StatusFailed {
		// Fetch the current status of the job from the database.
		jobToResume.status, err = getJobIDStatus(ctx, conn, jobToResume.jobID)
		if err != nil {
			return err
		}

		// Wait for the pollForStatusInterval duration before checking the status again.
		time.Sleep(pollForStatusInterval)
	}
	return nil
}
