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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/jobs"
)

// Constants defining environment variables to manage changefeed jobs.
const (
	maxChangefeeds          = "OPS_MAX_CF"             // Max number of changefeeds allowed.
	minChangefeeds          = "OPS_MIN_CF"             // Min number of changefeeds before pausing/cancelling jobs.
	pctChangefeedsToPause   = "OPS_PCT_PAUSE_CF"       // Percentage of changefeeds to pause.
	maxPctChangeFeedsScanOn = "OPS_MAX_PCT_CF_SCAN_ON" // Max percentage of changefeeds scan-on state.
)

// jobDetails contains the job ID and its current status as obtained from the DB.
type jobDetails struct {
	jobID   string      // Unique ID of the changefeed job.
	status  jobs.Status // Current status of the job (e.g., running, paused, etc.).
	payload struct {    // required payload details for the job
		options map[string]string // the configured options
		sinkUri string            // the sink URI
	}
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
		jobIDToDetails[jobID].payload = struct {
			options map[string]string
			sinkUri string
		}{options: options, sinkUri: rowValues[2]}
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
	// Initialize WHERE clause to filter only changefeed jobs.
	whereClause := []string{"job_type='CHANGEFEED'"}

	// If statuses are provided, append them to the WHERE clause.
	if len(statuses) > 0 {
		statusStrs := make([]string, 0)
		for _, s := range statuses {
			statusStrs = append(statusStrs, string(s))
		}
		whereClause = append(whereClause,
			fmt.Sprintf("status in ('%s')", strings.Join(statusStrs, "', '")))
	}

	// Construct and return the final SQL statement.
	return fmt.Sprintf("SELECT job_id, status FROM [SHOW JOBS] WHERE %s;", strings.Join(whereClause, " AND "))
}

// getJobIDStatus retrieves the current status of a specific job by job ID.
func getJobIDStatus(ctx context.Context, conn *gosql.DB, jobID string) (jobs.Status, error) {
	// Construct SQL query to get job status by job ID.
	statement := fmt.Sprintf("SELECT status FROM [SHOW JOBS] WHERE job_type='CHANGEFEED' and job_id='%s';", jobID)

	var status jobs.Status
	// Execute query and extract the status value.
	err := helpers.ExecuteQuery(ctx, func(rowValues []string) error {
		status = jobs.Status(rowValues[0])
		return nil
	}, conn, statement)

	return status, err
}
