// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeeds

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// Constants defining environment variables to manage changefeed jobs.
const (
	maxChangefeeds          = "OPS_MAX_CF"             // Max number of changefeeds allowed.
	maxPctChangeFeedsScanOn = "OPS_MAX_PCT_CF_SCAN_ON" // Max percentage of changefeeds with scan "yes" or "only".
)

// jobDetails contains the job ID and its current status as obtained from the DB.
type jobDetails struct {
	jobID   string                    // Unique ID of the changefeed job.
	status  jobs.Status               // Current status of the job (e.g., running, paused, etc.).
	payload *jobspb.ChangefeedDetails // required payload details for the job
}

// updatePayloadForJobs fetches additional changefeed payload details for specific jobs from the database
// and populates the same in the jobdetails
func updatePayloadForJobs(ctx context.Context, conn *gosql.DB, jobs []*jobDetails) error {
	if len(jobs) == 0 {
		return nil
	}
	jobIDs := make([]string, 0)
	jobIDToDetails := make(map[string]*jobDetails)
	// Collect all job IDs for query.
	for i := 0; i < len(jobs); i++ {
		j := jobs[i]
		jobIDs = append(jobIDs, j.jobID)
		jobIDToDetails[j.jobID] = j
	}

	// Construct SQL query to get changefeed options for specific jobs.
	stmt := fmt.Sprintf("select job_id, "+
		"crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Payload', value)->'changefeed'->'opts', "+
		"crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Payload', value)->'changefeed'->'sink_uri'"+
		" from system.job_info where job_id in ('%s') and info_key = 'legacy_payload';", strings.Join(jobIDs, "', '"))

	// Execute query and process results.
	return helpers.ExecuteQuery(ctx, func(rowValues []string) error {
		options := make(map[string]string)
		err := json.Unmarshal([]byte(rowValues[1]), &options)
		jobID := rowValues[0]
		jobIDToDetails[jobID].payload = &jobspb.ChangefeedDetails{
			Opts: options, SinkURI: rowValues[2],
		}
		return err
	}, conn, stmt)
}

// fetchAllChangefeedJobsWithStatuses fetches all changefeed jobs with the given statuses.
func fetchAllChangefeedJobsWithStatuses(
	ctx context.Context, conn *gosql.DB, statuses ...jobs.Status,
) ([]*jobDetails, error) {
	// Build SQL query to fetch jobs with specified statuses.
	statement := buildQueryToGetCFJobsWithStatuses(statuses...)
	details := make([]*jobDetails, 0)

	// Execute query and populate job details.
	err := helpers.ExecuteQuery(ctx, func(rowValues []string) error {
		details = append(details, &jobDetails{
			jobID:  rowValues[0],
			status: jobs.Status(rowValues[1]),
		})
		return nil
	}, conn, statement)

	return details, err
}

// buildQueryToGetCFJobsWithStatuses constructs the SQL query to fetch changefeed jobs.
func buildQueryToGetCFJobsWithStatuses(statuses ...jobs.Status) string {
	whereClause := ""
	// If statuses are provided, append them to the WHERE clause.
	if len(statuses) > 0 {
		statusStrs := make([]string, 0)
		for _, s := range statuses {
			statusStrs = append(statusStrs, string(s))
		}
		whereClause = fmt.Sprintf(" WHERE status in ('%s')", strings.Join(statusStrs, "', '"))
	}

	// Construct and return the final SQL statement.
	return fmt.Sprintf("SELECT job_id, status FROM [SHOW CHANGEFEED JOBS]%s;", whereClause)
}

// getJobIDStatus retrieves the current status of a specific job by job ID.
func getJobIDStatus(ctx context.Context, conn *gosql.DB, jobID string) (jobs.Status, error) {
	// Construct SQL query to get job status by job ID.
	statement := fmt.Sprintf("SELECT status FROM [SHOW CHANGEFEED JOBS] WHERE job_id='%s';", jobID)

	var status jobs.Status
	// Execute query and extract the status value.
	err := helpers.ExecuteQuery(ctx, func(rowValues []string) error {
		status = jobs.Status(rowValues[0])
		return nil
	}, conn, statement)

	return status, err
}

// changeStateForChangefeed executes the specified action (e.g., PAUSE, CANCEL) on a randomly
// selected changefeed job from the list of jobs in currentStatuses.
// It confirms the success by checking if the job transitions to one of the expectedFinalStatuses.
func changeStateForChangefeed(
	ctx context.Context,
	o operation.Operation,
	conn *gosql.DB,
	action string,
	currentStatuses, expectedFinalStatuses []jobs.Status,
) error {
	// Fetch all changefeed jobs with the given current statuses.
	cfJobs, err := fetchAllChangefeedJobsWithStatuses(ctx, conn, currentStatuses...)
	if err != nil {
		o.Fatal(err)
	}
	// If no jobs found, log and exit.
	if len(cfJobs) == 0 {
		o.Status(fmt.Sprintf("No jobs found for %s", action))
		return nil
	}

	// Initialize a random number generator to select a job randomly.
	r, _ := randutil.NewPseudoRand()

	// Randomly pick a job to perform the action on.
	jobIndex := randutil.RandIntInRange(r, 0, len(cfJobs))
	jobToAction := cfJobs[jobIndex]

	// Execute the action (e.g., PAUSE or CANCEL) on the chosen job.
	o.Status(fmt.Sprintf("Executing %s on job %s", action, jobToAction.jobID))
	_, err = conn.ExecContext(ctx, fmt.Sprintf("%s JOB %s;", action, jobToAction.jobID))
	if err != nil {
		return err
	}

	// Wait for the job to transition to one of the expected statuses (e.g., paused, canceled).
	err = waitForJobToComplete(ctx, conn, action, jobToAction, expectedFinalStatuses)
	if err != nil {
		return err
	}

	// Log the successful completion of the action.
	o.Status(fmt.Sprintf("%s for Job %s", action, jobToAction.jobID))
	return err
}

// waitForJobToComplete polls the job's status until it transitions to one of the expectedFinalStatuses
// (e.g., paused, canceled), or until a timeout occurs.
func waitForJobToComplete(
	ctx context.Context,
	conn *gosql.DB,
	action string,
	jobToAction *jobDetails,
	expectedFinalStatuses []jobs.Status,
) error {
	var err error
	// Set a timeout for how long we wait for the job to reach the final status.
	timeout := time.Now().Add(pollForStatusTimeout)
	// Continuously poll the job status until it reaches one of the expected final statuses or fails.
	for {
		// Get the current status of the job.
		jobToAction.status, err = getJobIDStatus(ctx, conn, jobToAction.jobID)
		if err != nil {
			return err
		}
		// If the job fails, return an error.
		if jobToAction.status == jobs.StatusFailed {
			return fmt.Errorf("job %s failed while waiting for %s to complete", jobToAction.jobID, action)
		}
		// Check if the job has reached one of the expected final statuses (e.g., paused or canceled).
		for _, expectedFinalStatus := range expectedFinalStatuses {
			if jobToAction.status == expectedFinalStatus {
				return nil
			}
		}
		// If the timeout is exceeded, abort the operation.
		if time.Now().After(timeout) {
			return fmt.Errorf("%s on job %s failed due to timeout", action, jobToAction.jobID)
		}
		// Sleep for a specified interval before polling again.
		time.Sleep(pollForStatusInterval)
	}
}
