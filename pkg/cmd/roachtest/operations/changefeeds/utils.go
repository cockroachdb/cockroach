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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Constants defining environment variables to manage changefeed jobs.
const (
	maxChangefeeds          = "OPS_MAX_CF"             // Max number of changefeeds allowed.
	maxPctChangeFeedsScanOn = "OPS_MAX_PCT_CF_SCAN_ON" // Max percentage of changefeeds with scan "yes" or "only".
)

// jobDetails contains the job ID and its current state as obtained from the DB.
type jobDetails struct {
	jobID   string                    // Unique ID of the changefeed job.
	state   jobs.State                // Current state of the job (e.g., running, paused, etc.).
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
		if err != nil {
			return err
		}
		jobID := rowValues[0]
		jobIDToDetails[jobID].payload = &jobspb.ChangefeedDetails{
			Opts: options, SinkURI: rowValues[2],
		}
		return err
	}, conn, stmt)
}

// fetchAllChangefeedJobsWithStates fetches all changefeed jobs with the given states.
func fetchAllChangefeedJobsWithStates(
	ctx context.Context, conn *gosql.DB, states ...jobs.State,
) ([]*jobDetails, error) {
	// Build SQL query to fetch jobs with specified states.
	statement := buildQueryToGetCFJobsWithStates(states...)
	details := make([]*jobDetails, 0)

	// Execute query and populate job details.
	err := helpers.ExecuteQuery(ctx, func(rowValues []string) error {
		details = append(details, &jobDetails{
			jobID: rowValues[0],
			state: jobs.State(rowValues[1]),
		})
		return nil
	}, conn, statement)

	return details, err
}

// buildQueryToGetCFJobsWithStates constructs the SQL query to fetch changefeed jobs.
func buildQueryToGetCFJobsWithStates(states ...jobs.State) string {
	whereClause := ""
	// If states are provided, append them to the WHERE clause.
	if len(states) > 0 {
		stateStrs := make([]string, 0)
		for _, s := range states {
			stateStrs = append(stateStrs, string(s))
		}
		whereClause = fmt.Sprintf(" WHERE status in ('%s')", strings.Join(stateStrs, "', '"))
	}

	// Construct and return the final SQL statement.
	return fmt.Sprintf("SELECT job_id, status FROM [SHOW CHANGEFEED JOBS]%s;", whereClause)
}

// getJobIDState retrieves the current state of a specific job by job ID.
func getJobIDState(ctx context.Context, conn *gosql.DB, jobID string) (jobs.State, error) {
	// Construct SQL query to get job state by job ID.
	statement := fmt.Sprintf("SELECT status FROM [SHOW CHANGEFEED JOBS] WHERE job_id='%s';", jobID)

	var state jobs.State
	// Execute query and extract the state value.
	err := helpers.ExecuteQuery(ctx, func(rowValues []string) error {
		state = jobs.State(rowValues[0])
		return nil
	}, conn, statement)

	return state, err
}

// changeStateForChangefeed executes the specified action (e.g., PAUSE, CANCEL) on a randomly
// selected changefeed job from the list of jobs in currentStates.
// It confirms the success by checking if the job transitions to one of the expectedFinalStates.
// If no existing jobs are found it attempts to create a changefeed.
// It uses the checkAndCreateChangefeed function to determine if a new changefeed can be created.
func changeStateOrCreateChangefeed(
	ctx context.Context,
	o operation.Operation,
	conn *gosql.DB,
	action string,
	currentStates, expectedFinalStates []jobs.State,
) error {
	// Fetch all changefeed jobs with the given current states.
	cfJobs, err := fetchAllChangefeedJobsWithStates(ctx, conn, currentStates...)
	if err != nil {
		o.Fatal(err)
	}
	// If no jobs found, log and exit.
	if len(cfJobs) == 0 {
		o.Status(fmt.Sprintf("No jobs found for %s. attempting create...", action))
		err = checkAndCreateChangefeed(ctx, o, conn, true)
		if err != nil {
			return err
		}
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

	// Wait for the job to transition to one of the expected states (e.g., paused, canceled).
	err = waitForJobToComplete(ctx, conn, action, jobToAction, expectedFinalStates)
	if err != nil {
		return err
	}

	// Log the successful completion of the action.
	o.Status(fmt.Sprintf("%s for Job %s", action, jobToAction.jobID))
	return err
}

// waitForJobToComplete polls the job's state until it transitions to one of the expectedFinalStates
// (e.g., paused, canceled), or until a timeout occurs.
func waitForJobToComplete(
	ctx context.Context,
	conn *gosql.DB,
	action string,
	jobToAction *jobDetails,
	expectedFinalStates []jobs.State,
) error {
	var err error
	// Set a timeout for how long we wait for the job to reach the final state.
	timeout := timeutil.Now().Add(pollForStateTimeout)
	// Continuously poll the job state until it reaches one of the expected final states or fails.
	for {
		// Get the current state of the job.
		jobToAction.state, err = getJobIDState(ctx, conn, jobToAction.jobID)
		if err != nil {
			return err
		}
		// If the job fails, return an error.
		if jobToAction.state == jobs.StateFailed {
			return fmt.Errorf("job %s failed while waiting for %s to complete", jobToAction.jobID, action)
		}
		// Check if the job has reached one of the expected final states (e.g., paused or canceled).
		for _, expectedFinalState := range expectedFinalStates {
			if jobToAction.state == expectedFinalState {
				return nil
			}
		}
		// If the timeout is exceeded, abort the operation.
		if timeutil.Now().After(timeout) {
			return fmt.Errorf("%s on job %s failed due to timeout", action, jobToAction.jobID)
		}
		// Sleep for a specified interval before polling again.
		time.Sleep(pollForStateInterval)
	}
}

// checkAndCreateChangefeed checks if there are any existing changefeed jobs,
// and if the createOnlyIfNoJobs flag is true, it returns early if any jobs exist.
// If the number of changefeed jobs is less than the maximum allowed limit,
// it randomly selects a database and table, then attempts to create a new changefeed job for that table.
// The function returns true if it successfully creates or evaluates the need for a changefeed,
// and false if any errors occur during the process.
func checkAndCreateChangefeed(
	ctx context.Context, o operation.Operation, conn *gosql.DB, createOnlyIfNoJobs bool,
) error {
	// Fetch all changefeed jobs that are running or paused.
	allCFJobs, err := fetchAllChangefeedJobsWithStates(ctx, conn, jobs.StateRunning, jobs.StatePaused)
	if err != nil {
		return err
	}
	// If there are existing changefeeds and the function is instructed to
	// create only if no jobs exist, return early.
	if len(allCFJobs) > 0 && createOnlyIfNoJobs {
		o.Status(fmt.Sprintf("No jobs created as %d active jobs already exists", len(allCFJobs)))
		return nil
	}

	// Get the max allowed changefeeds from environment variables.
	mcf, err := helpers.EnvOrDefaultInt(maxChangefeeds, defaultEnvValuesInt[maxChangefeeds])
	if err != nil {
		return err
	}

	// If the number of existing changefeeds is below the max limit, create a new one.
	if len(allCFJobs) < mcf {
		// Select a random database and table to create the changefeed for.
		dbName := helpers.PickRandomDB(ctx, o, conn, helpers.SystemDBs)
		tableName := helpers.PickRandomTable(ctx, o, conn, dbName)

		// Attempt to create the changefeed.
		initialScanValue, err := createChangefeed(ctx, o, conn, allCFJobs, dbName, tableName)
		if err != nil {
			return err
		}
		return waitForCreateJobToComplete(ctx, conn, initialScanValue)
	} else {
		o.Status(fmt.Sprintf("total changefeeds %d are more than or equal to the maximum changefeeds criteria %d",
			len(allCFJobs), mcf))
	}
	return nil
}

// waitForCreateJobToComplete waits for the latest changefeed job to reach a final state (either running or failed).
// It polls the job status at regular intervals, checking the state of the job and whether the high-water mark
// is set, which indicates progress. If the timeout is exceeded or the job fails, an error is returned.
func waitForCreateJobToComplete(
	ctx context.Context, conn *gosql.DB, initialScanValue string,
) (err error) {
	// Set a timeout for how long we wait for the job to reach the final state.
	timeout := timeutil.Now().Add(pollForStateTimeout)
	jobID := ""
	var state jobs.State
	var highWaterTimestamp hlc.Timestamp

	// Query to get the most recent changefeed job's ID, state, and high-water timestamp.
	statement := "SELECT job_id, status, COALESCE(high_water_timestamp, '0') " +
		"FROM [SHOW CHANGEFEED JOBS] order by created desc limit 1;"
	err = helpers.ExecuteQuery(ctx, func(rowValues []string) error {
		jobID = rowValues[0]
		state = jobs.State(rowValues[1])
		highWaterTimestamp, err = hlc.ParseHLC(rowValues[2])
		return err
	}, conn, statement)
	if err != nil {
		return err
	}

	// Loop to poll for the job status until the desired state is reached or timeout occurs.
	for {
		// If the timeout is exceeded, abort the operation.
		if timeutil.Now().After(timeout) {
			return fmt.Errorf("create failed for job %s due to timeout", jobID)
		}

		// If the job failed, return an error.
		if state == jobs.StateFailed {
			return fmt.Errorf("job %s failed", jobID)
		} else if state == jobs.StateRunning {
			if initialScanValue == "no" {
				// For initial_scan = "no", wait until the high-water timestamp is set.
				if !highWaterTimestamp.IsEmpty() {
					break
				}
			} else {
				// high-water timestamp is not required to be checked for jobs which isn't doing an initial scan.
				break
			}
		}
		// Sleep for a specified interval before polling again.
		time.Sleep(pollForStateInterval)

		// Query to fetch the current status and high-water mark of the job.
		statement = fmt.Sprintf("SELECT status, high_water_timestamp FROM [SHOW CHANGEFEED JOBS] where job_id='%s';", jobID)

		// Execute the query to fetch updated job details.
		err = helpers.ExecuteQuery(ctx, func(rowValues []string) error {
			state = jobs.State(rowValues[0])
			highWaterTimestamp, err = hlc.ParseHLC(rowValues[1])
			return err
		}, conn, statement)
		if err != nil {
			return err
		}
	}
	return
}

// createChangefeed creates a new changefeed job for the given table and sink.
// initial scan value is returned as it is needed for checking the job status
func createChangefeed(
	ctx context.Context,
	o operation.Operation,
	conn *gosql.DB,
	allCFJobs []*jobDetails,
	dbName, tableName string,
) (string, error) {
	// Fetch the changefeed options for all jobs.
	// The payload details are updated in the jobDetails. This is used for sink configs and options.
	err := updatePayloadForJobs(ctx, conn, allCFJobs)
	if err != nil {
		return "", err
	}
	options := make([]string, 0)
	// Define the sink where the changefeed output will be sent.
	sink, sinkOptions, err := getSinkConfigs(ctx, allCFJobs)
	if err != nil {
		return "", err
	}
	options = append(options, sinkOptions...)
	// Calculate whether the new changefeed should include an initial scan.
	initialScanValue, scanOption, err := calculateScanOption(allCFJobs)
	if err != nil {
		return "", err
	}
	options = append(options, scanOption)

	o.Status(fmt.Sprintf("creating changefeed job to sink %s with options %v", sink, options))

	// Construct and execute the SQL statement to create the changefeed.
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE CHANGEFEED FOR TABLE %s.%s INTO '%s' WITH %s;",
		dbName, tableName, sink, strings.Join(options, ",")))
	return initialScanValue, err
}

// getSinkConfigs returns the sink uri along with the options for creating the changefeed.
// this will be extended later for more sinks
func getSinkConfigs(_ context.Context, _ []*jobDetails) (string, []string, error) {
	return "null://", make([]string, 0), nil
}

// calculateScanOption determines whether the new changefeed should have an initial scan based on existing jobs.
func calculateScanOption(allCFJobs []*jobDetails) (string, string, error) {
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
		return "", "", err // Return error if fetching environment variable fails.
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
	return initialScanValue, fmt.Sprintf("%s='%s'%s", changefeedbase.OptInitialScan, initialScanValue, resolved), nil
}
