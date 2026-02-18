// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeeds

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// Constants defining environment variables to manage changefeed jobs.
const (
	maxChangefeeds          = "OPS_MAX_CF"             // Max number of changefeeds allowed.
	maxPctChangeFeedsScanOn = "OPS_MAX_PCT_CF_SCAN_ON" // Max percentage of changefeeds with scan "yes" or "only".
)

// jobDetails contains the job ID and its current state as obtained from the DB.
type jobDetails struct {
	jobID              string                    // Unique ID of the changefeed job.
	state              jobs.State                // Current state of the job (e.g., running, paused, etc.).
	payload            *jobspb.ChangefeedDetails // required payload details for the job
	highWaterTimestamp hlc.Timestamp             // high watermark timestamp
}

// configSetter defines a callback function used by parseConfigs to apply each
// parsed sink and its associated percentage. This allows custom handling of
// each config entry during parsing (e.g., storing in a map or validating).
type configSetter func(key string, value int) error

// getJobsUpdatedWithPayload fetches additional changefeed payload details for specific jobs from the database.
// This returns a new slice of jobDetails with the updated payload details without mutating the one in input.
func getJobsUpdatedWithPayload(
	ctx context.Context, conn *gosql.DB, jobs []*jobDetails,
) ([]*jobDetails, error) {
	if len(jobs) == 0 {
		return jobs, nil
	}
	// create a new slice of jobDetails to avoid mutation.
	jd := make([]*jobDetails, len(jobs))
	jobIDs := make([]string, 0)
	jobIDToDetails := make(map[string]*jobDetails)
	// Collect all job IDs for query.
	for i := 0; i < len(jobs); i++ {
		j := *jobs[i]
		jobIDs = append(jobIDs, j.jobID)
		jd[i] = &j
		jobIDToDetails[j.jobID] = &j
	}

	// Construct SQL query to get changefeed options for specific jobs.
	stmt := fmt.Sprintf("select job_id, value "+
		"from system.job_info where job_id in ('%s') and info_key = 'legacy_payload';", strings.Join(jobIDs, "', '"))

	// Execute query and process results.
	err := helpers.ExecuteQuery(ctx, func(rowValues []string) error {
		payload := &jobspb.Payload{}
		err := protoutil.Unmarshal([]byte(rowValues[1]), payload)
		if err != nil {
			return err
		}
		jobID := rowValues[0]
		jobIDToDetails[jobID].payload = payload.GetChangefeed()
		return nil
	}, conn, stmt)
	return jd, err
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
		highWaterTimestamp, err := hlc.ParseHLC(rowValues[2])
		if err != nil {
			return err
		}
		details = append(details, &jobDetails{
			jobID:              rowValues[0],
			state:              jobs.State(rowValues[1]),
			highWaterTimestamp: highWaterTimestamp,
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
	return fmt.Sprintf("SELECT job_id, status, COALESCE(high_water_timestamp, '0') FROM [SHOW CHANGEFEED JOBS]%s;", whereClause)
}

// getJobIDStateWithWatermark retrieves the current state and the high watermark of a specific job by job ID.
func getJobIDStateAndHighWM(
	ctx context.Context, conn *gosql.DB, jobID string,
) (state jobs.State, highWaterTimestamp hlc.Timestamp, err error) {
	// Construct SQL query to get job state by job ID.
	statement := fmt.Sprintf("SELECT status, COALESCE(high_water_timestamp, '0') FROM [SHOW CHANGEFEED JOBS] WHERE job_id='%s';", jobID)

	// Execute query and extract the state value.
	err = helpers.ExecuteQuery(ctx, func(rowValues []string) error {
		state = jobs.State(rowValues[0])
		highWaterTimestamp, err = hlc.ParseHLC(rowValues[1])
		return err
	}, conn, statement)

	return state, highWaterTimestamp, err
}

// changeStateOrCreateChangefeed executes the specified action (e.g., PAUSE, CANCEL) on a randomly
// selected changefeed job from the list of jobs in currentStates.
// It confirms the success by checking if the job transitions to one of the expectedFinalStates.
// If no existing jobs are found it attempts to create a changefeed.
// It uses the checkAndCreateChangefeed function to determine if a new changefeed can be created.
func changeStateOrCreateChangefeed(
	ctx context.Context,
	o operation.Operation,
	c cluster.Cluster,
	action string,
	currentStates, expectedFinalStates []jobs.State,
) error {
	// Connect to the cluster and ensure the connection is closed after use.
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() {
		_ = conn.Close()
	}()
	// Fetch all changefeed jobs with the given current states.
	cfJobs, err := fetchAllChangefeedJobsWithStates(ctx, conn, currentStates...)
	if err != nil {
		return err
	}
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
	err = waitForJobState(ctx, conn, action, jobToAction, expectedFinalStates)
	if err != nil {
		return err
	}

	// Log the successful completion of the action.
	o.Status(fmt.Sprintf("%s complete for Job %s", action, jobToAction.jobID))
	return nil
}

// waitForJobState polls the job's state until it transitions to one of the expectedFinalStates
// (e.g., running, paused, canceled), or until a timeout occurs.
func waitForJobState(
	ctx context.Context,
	conn *gosql.DB,
	action string,
	jobToAction *jobDetails,
	expectedFinalStates []jobs.State,
) error {
	var err error
	// Set a timeout for how long we wait for the job to reach the final state.
	ctxTimeout, cancel := context.WithTimeout(ctx, pollForStateTimeout)
	defer cancel()
	for {
		// If the job fails, return an error.
		if jobToAction.state == jobs.StateFailed {
			return fmt.Errorf("job %s failed while waiting for %s to complete", jobToAction.jobID, action)
		}
		// Check if the job has reached one of the expected final states (e.g., paused or canceled).
		for _, expectedFinalState := range expectedFinalStates {
			if jobToAction.state == expectedFinalState {
				if jobToAction.state == jobs.StateRunning {
					// we need to wait until the high-water timestamp is set for job state "running"
					if !jobToAction.highWaterTimestamp.IsEmpty() {
						return nil
					}
				} else {
					return nil
				}
			}
		}
		select {
		case <-ctxTimeout.Done():
			// If the timeout is exceeded, the operation is aborted
			return fmt.Errorf("%s on job %s failed due to timeout", action, jobToAction.jobID)
		case <-time.After(pollForStateInterval):
			// the current state of the job is obtained.
			jobToAction.state, jobToAction.highWaterTimestamp, err = getJobIDStateAndHighWM(ctxTimeout, conn, jobToAction.jobID)
			if err != nil {
				return err
			}
		}
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
	if len(allCFJobs) >= mcf {
		o.Status(fmt.Sprintf("total changefeeds %d are more than or equal to the maximum changefeeds criteria %d",
			len(allCFJobs), mcf))
	} else {
		// If the number of existing changefeeds is below the max limit, create a new one.
		// Select a random database and table to create the changefeed for.
		dbName := helpers.PickRandomDB(ctx, o, conn, helpers.SystemDBs)
		tableName := helpers.PickRandomTable(ctx, o, conn, dbName)

		// Attempt to create the changefeed.
		err = createChangefeed(ctx, o, conn, allCFJobs, dbName, tableName)
		if err != nil {
			return err
		}
		// createdJob is the job that was just created. It is identified by querying the
		// changefeed job with the latest created timestamp
		var createdJob *jobDetails
		statement := "SELECT job_id, status, COALESCE(high_water_timestamp, '0') " +
			"FROM [SHOW CHANGEFEED JOBS] order by created desc limit 1;"
		err = helpers.ExecuteQuery(ctx, func(rowValues []string) error {
			highWaterTimestamp, err := hlc.ParseHLC(rowValues[2])
			createdJob = &jobDetails{
				jobID:              rowValues[0],
				state:              jobs.State(rowValues[1]),
				payload:            nil,
				highWaterTimestamp: highWaterTimestamp,
			}
			return err
		}, conn, statement)
		if err != nil {
			return err
		}
		return waitForJobState(ctx, conn, "CREATE", createdJob,
			[]jobs.State{jobs.StateRunning, jobs.StateSucceeded})
	}
	return nil
}

// createChangefeed creates a new changefeed job for the given table and sink.
// initial scan value is returned as it is needed for checking the job status
func createChangefeed(
	ctx context.Context,
	o operation.Operation,
	conn *gosql.DB,
	allCFJobs []*jobDetails,
	dbName, tableName string,
) error {
	// Fetch the changefeed options for all jobs.
	// The payload details are updated in the jobDetails. This is used for sink configs and options.
	allCFJobs, err := getJobsUpdatedWithPayload(ctx, conn, allCFJobs)
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

	o.Status(fmt.Sprintf("creating changefeed job to sink %s with options %v on table %s.%s",
		sink, options, dbName, tableName))

	// Ensure rangefeeds are enabled, as they are required for changefeeds.
	_, err = conn.ExecContext(ctx, "SET CLUSTER SETTING kv.rangefeed.enabled = true")
	if err != nil {
		return err
	}

	// Construct and execute the SQL statement to create the changefeed.
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE CHANGEFEED FOR TABLE %s.%s INTO '%s' WITH %s;",
		dbName, tableName, sink, strings.Join(options, ",")))
	return err
}

// ParseConfigs exported for testing
func ParseConfigs(config string, cb configSetter) error {
	if config == "" {
		return fmt.Errorf("config string cannot be empty")
	}

	configsArr := strings.Split(config, ",")
	totalCount := 0

	for _, c := range configsArr {
		parts := strings.Split(c, ":")
		if len(parts) != 2 {
			return fmt.Errorf("invalid config format: %s", c)
		}

		key := parts[0]
		valueStr := parts[1]

		value, err := strconv.Atoi(valueStr)
		if err != nil {
			return fmt.Errorf("invalid percentage value in config '%s'", c)
		}

		if value < 0 || value > 100 {
			return fmt.Errorf("percentage value out of range in config '%s': must be between 0 and 100", c)
		}

		err = cb(key, value)
		if err != nil {
			return err
		}

		totalCount += value
	}

	if totalCount != 100 {
		return fmt.Errorf("all sinks must sum to 100%%, but total is %d%%", totalCount)
	}

	return nil
}

func selectSink(sinks map[string]int) string {
	choice := rand.Intn(100)
	sum := 0
	for sink, pct := range sinks {
		sum += pct
		if choice < sum {
			return sink
		}
	}
	return "null" // Default fallback if no valid selection
}

// getSinkConfigs returns the sink uri along with the options for creating the changefeed.
// this will be extended later for more sinks
func getSinkConfigs(_ context.Context, _ []*jobDetails) (string, []string, error) {
	sinkConfigEnv := os.Getenv("SINK_CONFIG")
	if sinkConfigEnv == "" {
		return "null://", []string{}, nil // Default to null sink if env is not set
	}

	sinks := make(map[string]int)
	uris := make(map[string]string)

	err := ParseConfigs(sinkConfigEnv, func(sink string, value int) error {
		sinks[sink] = value
		uriEnv := fmt.Sprintf("SINK_CONFIG_%s", strings.ToUpper(sink))
		uri, exists := os.LookupEnv(uriEnv)
		if !exists {
			return fmt.Errorf("environment variable %s not found for sink %s", uriEnv, sink)
		}
		uris[sink] = uri
		return nil
	})
	if err != nil {
		// Default to null sink on parsing error
		return "null://", []string{}, nil //nolint:returnerrcheck
	}

	selectedSink := selectSink(sinks)
	selectedURI, exists := uris[selectedSink]
	if !exists {
		return "null://", []string{}, nil // Default to null sink if selection fails
	}

	return selectedURI, []string{}, nil
}

// calculateScanOption determines whether the new changefeed should have an initial scan based on existing jobs.
func calculateScanOption(allCFJobs []*jobDetails) (string, error) {
	scanOnCountPercent := 0
	if len(allCFJobs) > 0 {
		scanOnCount := 0
		// Count the number of jobs that have initial scan set to 'yes' or "only".
		for _, j := range allCFJobs {
			if j.payload == nil {
				continue
			}
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
