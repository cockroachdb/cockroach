// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// test selector is responsible for selecting the tests for running in the roachtest
// it generates a csv file with test details

package sfselector

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	sf "github.com/snowflakedb/gosnowflake"
	"google.golang.org/api/option"
)

const (
	account   = "lt53838.us-central1.gcp"
	database  = "DATAMART_PROD"
	schema    = "TEAMCITY"
	warehouse = "COMPUTE_WH"

	// project, bucket and testsFileLocation are for uploading the csv for each cloud in GCS
	project           = "cockroach-testeng-infra"
	bucket            = "roachtest-data"
	testsFileLocation = "tests/selected"
	testsCsvExtension = "csv"
	// sfUsernameEnv and sfUsernameEnv are the environment variables that are used for Snowflake access
	sfUsernameEnv = "ROACHPROD_SF_USERNAME"
	sfPasswordEnv = "ROACHPROD_SF_PASSWORD"

	// preparedQuery is the snowflake query executed to fetch the tests
	preparedQuery = `with builds as (
	-- select all the build IDs in the last "forPastDays" days
	select 
  	ID as run_id,
  	min(start_date) as first_run, -- get the first time the test was run
  	max(start_date) as last_run, -- -- get the last time the test was run
  from DATAMART_PROD.TEAMCITY.BUILDS
  where
    start_date > dateadd(DAY, ?, current_date()) -- last "forPastDays" days
    and lower(status) = 'success' -- consider only successful builds
    and branch_name = 'master' -- consider only builds from master branch
    and lower(name) like ? -- name is based on the suite and cloud e.g. '%roachtest nightly - gce%'
	group by 1
), test_stats as (
	-- for all the build IDs select do a inner join on all the tests 
	select test_name, -- disctinct test names
		count(case when duration>0 then test_name end) as total_runs, -- all runs with duration>0 (excluding all ignored test runs)
		-- count by status
		count(case when status='SUCCESS' then test_name end) as success_count,
		count(case when status='FAILURE' then test_name end) as failure_count,
		-- get the first_run and last_run only if the status is not UNKNOWN. This returns nil for runs that have never run
		min(case when status!='UNKNOWN' then b.first_run end) as first_run,
		max(case when status!='UNKNOWN' then b.last_run end) as last_run,
		sum(duration) as total_duration -- the total duration of the test
	from DATAMART_PROD.TEAMCITY.TESTS t
	  -- inner join as explained in the beginning 
		inner join builds b on
			b.run_id=t.build_id
  where test_name is not null -- not interested in tests with no names
	group  by 1
)
select
	test_name,
  case when
    -- mark as selected if
    failure_count > 0 or -- the test has failed at least once in the past "forPastDays" days
    first_run > dateadd(DAY, ?, current_date()) or -- recently added test - test has not run for more than "firstRunOn" days
    last_run < dateadd(DAY, ?, current_date()) or -- the test has been run for last "lastRunOn" days
    last_run is null -- the test is always ignored till now
  then 'yes' else 'no' end as selected,
   -- average duration - this is set to 0 if the test is never run (total_runs=0)
	case when total_runs > 0 then total_duration/total_runs else 0 end as avg_duration,
	total_runs,
from test_stats
order by selected desc, total_runs`
)

// supported suites
var suites = map[string]string{
	"nightly": "roachtest nightly",
}

// supported clouds
var clouds = map[string]struct{}{
	"gce":   {},
	"aws":   {},
	"azure": {},
}

// SelectTestsReq is the request for SelectTests
type SelectTestsReq struct {
	ForPastDays, FirstRunOn, LastRunOn, SelectFromSuccessPct int
	Cloud, Suite                                             string
	DryRun                                                   bool
}

// validate validates the SelectTestsReq
func (r *SelectTestsReq) validate() error {
	if _, ok := clouds[r.Cloud]; !ok {
		keys := make([]string, 0)
		for c := range clouds {
			keys = append(keys, c)
		}
		return fmt.Errorf("unsupported cloud %s! Supported clouds: %v", r.Cloud, keys)
	}
	if _, ok := suites[r.Suite]; !ok {
		return fmt.Errorf("unsupported suite %s", r.Suite)
	}
	return nil
}

// testDetails contains the results fetched from the query
type testDetails struct {
	columnHeaders []string
	testInfos     [][]string // the fetched data
}

// SelectTests is responsible for:
// 1. fetching the data from snowflake using the criteria provided
// 2. uploading the results in CSV format to cloud storage
func SelectTests(ctx context.Context, req *SelectTestsReq) error {
	fmt.Printf("getting test details.\n")
	r, err := getTestDetails(ctx, req)
	if err != nil {
		return err
	}
	csvBuffer, err := getCSVInMemory(r)
	if err != nil {
		return err
	}
	fmt.Printf("uploading %s cloud test details to the cloud...\n", req.Cloud)
	if req.DryRun {
		fmt.Println(csvBuffer.String())
		return nil
	}
	return uploadCSVToGCS(csvBuffer, req.Cloud, req.Suite)
}

// getTestDetails gets the test details from snowflake
func getTestDetails(ctx context.Context, req *SelectTestsReq) (*testDetails, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	db, err := getConnect(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	statement, err := db.Prepare(preparedQuery)
	if err != nil {
		return nil, err
	}
	// add the parameters in sequence
	rows, err := statement.QueryContext(context.Background(),
		req.ForPastDays*-1, fmt.Sprintf("%%%s - %s%%", suites[req.Suite], req.Cloud),
		req.FirstRunOn*-1, req.LastRunOn*-1)
	if err != nil {
		return nil, err
	}
	// All the column headers
	colHeaders, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	// Will be used to read data while iterating rows.
	colPointers := make([]interface{}, len(colHeaders))
	colContainer := make([]string, len(colHeaders))
	for i := range colPointers {
		colPointers[i] = &colContainer[i]
	}
	// skipped tests are maintained separately
	// this helps in considering them for running based on selectFromSuccessPct
	skippedTests := make([][]string, 0)
	td := &testDetails{
		columnHeaders: colHeaders,
		testInfos:     make([][]string, 0),
	}
	selectedTestCount := 0 // used for printing the number of tests selected
	for rows.Next() {
		_ = rows.Scan(colPointers...)
		testInfos := make([]string, len(colContainer))
		copy(testInfos, colContainer)
		if testInfos[1] == "no" {
			// skipped based on query
			skippedTests = append(skippedTests, testInfos)
		} else {
			// selected for running
			td.testInfos = append(td.testInfos, testInfos)
			selectedTestCount++
		}
	}
	if req.SelectFromSuccessPct > 0 && len(skippedTests) > 0 {
		// need to select some tests from th success list (skipped tests)
		numberOfTestsToSelect := len(skippedTests) * req.SelectFromSuccessPct / 100
		// the tests are sorted by the number of runs. So, simply iterate over teh list
		// and select the first count of "numberOfTestsToSelect"
		for i := range skippedTests {
			if i == numberOfTestsToSelect {
				break
			}
			// mark the skipped tests as selected="yes"
			skippedTests[i][1] = "yes"
			selectedTestCount++
		}
	}
	// add all the skipped tests (some may have been selected based on selectFromSuccessPct)
	td.testInfos = append(td.testInfos, skippedTests...)
	fmt.Printf("%d out of %d tests selected!\n", selectedTestCount, len(td.testInfos))
	return td, nil
}

// getConnect makes connection to snowflake and returns the connection.
func getConnect(ctx context.Context) (*gosql.DB, error) {
	username, password, err := getSFCreds(ctx)
	if err != nil {
		return nil, err
	}

	dsn, err := sf.DSN(&sf.Config{
		Account:   account,
		Database:  database,
		Schema:    schema,
		Warehouse: warehouse,
		Password:  password,
		User:      username,
	})
	if err != nil {
		return nil, err
	}
	db, err := gosql.Open("snowflake", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// getCSVInMemory creates a CSV file in memory from a testDetails
func getCSVInMemory(td *testDetails) (*bytes.Buffer, error) {
	buffer := &bytes.Buffer{}
	w := csv.NewWriter(buffer)
	defer w.Flush()
	if err := w.Write(td.columnHeaders); err != nil {
		return nil, err
	}
	if err := w.WriteAll(td.testInfos); err != nil {
		return nil, err
	}
	return buffer, nil
}

// uploadCSVToGCS uploads a CSV file in memory to Google Cloud Storage
func uploadCSVToGCS(csvBuffer *bytes.Buffer, cloud, suite string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeFullControl), option.WithQuotaProject(project))
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer func() { _ = client.Close() }()

	wc := client.Bucket(bucket).Object(
		fmt.Sprintf("%s-%s-%s.%s", testsFileLocation, suite, cloud, testsCsvExtension)).NewWriter(ctx)
	if _, err = io.Copy(wc, csvBuffer); err != nil {
		return fmt.Errorf("failed to write CSV to GCS: %w", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %w", err)
	}

	return nil
}

// getSFCreds gets the snowflake credentials from the secrets manager
func getSFCreds(_ context.Context) (string, string, error) {
	username := os.Getenv(sfUsernameEnv)
	password := os.Getenv(sfPasswordEnv)
	if username == "" {
		return "", "", fmt.Errorf("environment variable %s is not set", sfUsernameEnv)
	}
	if password == "" {
		return "", "", fmt.Errorf("environment variable %s is not set", sfPasswordEnv)
	}
	return username, password, nil
}
