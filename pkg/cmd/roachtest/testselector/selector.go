// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testselector

import (
	"context"
	gosql "database/sql"
	_ "embed"
	"fmt"
	"os"
	"strconv"

	sf "github.com/snowflakedb/gosnowflake"
)

const (
	defaultForPastDays = 30
	defaultFirstRunOn  = 20
	defaultLastRunOn   = 7

	account   = "lt53838.us-central1.gcp"
	database  = "DATAMART_PROD"
	schema    = "TEAMCITY"
	warehouse = "COMPUTE_WH"

	// sfUsernameEnv and sfPasswordEnv are the environment variables that are used for Snowflake access
	sfUsernameEnv = "SFUSER"
	sfPasswordEnv = "SFPASSWORD"
)

//go:embed snowflake_query.sql
var preparedQuery string

// supported suites
var suites = map[string]string{
	"nightly": "roachtest nightly",
}

// TestDetails has the details of the test as fetched from snowflake
type TestDetails struct {
	Name                string // test name
	Selected            bool   // whether a test is Selected or not
	AvgDurationInMillis int64  // average duration of the test
}

// SelectTestsReq is the request for CategoriseTests
type SelectTestsReq struct {
	ForPastDays, // number of days data to consider for test selection
	FirstRunOn, // number of days to consider for the first time the test is run
	LastRunOn, // number of days to consider for the last time the test is run
	SelectFromSuccessPct int //percentage of tests to be Selected for running from the successful test list sorted by number of runs
	Cloud, // the cloud where the tests were run
	Suite string // the test suite for which the selection is done
}

// NewDefaultSelectTestsReq returns a new SelectTestsReq with default values populated
func NewDefaultSelectTestsReq(selectFromSuccessPct int, cloud, suite string) *SelectTestsReq {
	return &SelectTestsReq{
		ForPastDays:          defaultForPastDays,
		FirstRunOn:           defaultFirstRunOn,
		LastRunOn:            defaultLastRunOn,
		SelectFromSuccessPct: selectFromSuccessPct,
		Cloud:                cloud,
		Suite:                suite,
	}
}

// CategoriseTests returns the tests categorized based on the snowflake query
// The tests are Selected by selector.go based on certain criteria:
// 1. the number of time a test has been successfully running
// 2. the test is new
// 3. the test has not been run for a while
// 4. a subset of the successful tests based on SelectTestReq.SelectFromSuccessPct
// It returns all the tests. The selected tests have the value TestDetails.Selected as true
func CategoriseTests(ctx context.Context, req *SelectTestsReq) ([]*TestDetails, error) {
	db, err := getConnect(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	statement, err := db.Prepare(preparedQuery)
	if err != nil {
		return nil, err
	}
	// get the current branch from the teamcity environment
	currentBranch := os.Getenv("TC_BUILD_BRANCH")
	if currentBranch == "" {
		currentBranch = "master"
	}
	// add the parameters in sequence
	rows, err := statement.QueryContext(ctx, req.ForPastDays*-1, currentBranch,
		fmt.Sprintf("%%%s - %s%%", suites[req.Suite], req.Cloud),
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
	// selectedTestDetails are all the tests that are selected from snowflake query
	selectedTestDetails := make([]*TestDetails, 0)
	// skipped tests are maintained separately
	// this helps in considering them for running based on further select criteria like selectFromSuccessPct
	skippedTests := make([]*TestDetails, 0)
	for rows.Next() {
		err = rows.Scan(colPointers...)
		if err != nil {
			return nil, err
		}
		testInfos := make([]string, len(colContainer))
		copy(testInfos, colContainer)
		// selected columns:
		// 0. TEST_NAME
		// 1. SELECTED (yes/no)
		// 2. AVG_DURATION
		testDetails := &TestDetails{
			Name:                testInfos[0],
			Selected:            testInfos[1] != "no",
			AvgDurationInMillis: getDuration(testInfos[2]),
		}
		if testDetails.Selected {
			// selected for running
			selectedTestDetails = append(selectedTestDetails, testDetails)
		} else {
			// skipped based on query
			skippedTests = append(skippedTests, testDetails)
		}
	}
	if req.SelectFromSuccessPct > 0 && len(skippedTests) > 0 {
		// need to select some tests from the skipped tests
		numberOfTestsToSelect := len(skippedTests) * req.SelectFromSuccessPct / 100
		// the tests are sorted by the number of runs. So, simply iterate over the list
		// and select the first count of "numberOfTestsToSelect"
		for i := 0; i < numberOfTestsToSelect; i++ {
			skippedTests[i].Selected = true
		}
	}
	// add all the test. The information can be used for further processing
	return append(selectedTestDetails, skippedTests...), nil
}

// getDuration extracts the duration from the snowflake query duration field
func getDuration(durationStr string) int64 {
	duration, _ := strconv.ParseInt(durationStr, 10, 64)
	return duration
}

// getConnect makes connection to snowflake and returns the connection.
func getConnect(ctx context.Context) (*gosql.DB, error) {
	username, password, err := getSFCreds()
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

// getSFCreds gets the snowflake credentials from the secrets manager
func getSFCreds() (string, string, error) {
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
