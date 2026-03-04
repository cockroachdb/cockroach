// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testselector

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	gosql "database/sql"
	_ "embed"
	"encoding/pem"
	"fmt"
	"math/rand"
	"os"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
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

	// sfUsernameEnv and sfPrivateKey are the environment variables that are used for Snowflake access
	sfUsernameEnv = "SNOWFLAKE_USER"
	sfPrivateKey  = "SNOWFLAKE_PVT_KEY"

	// DataTestNameIndex and the following corresponds to the index of the row where the data is returned
	DataTestNameIndex = 0
	DataSelectedIndex = 1
	DataDurationIndex = 2
	DataLastPreempted = 3
	DataFirstRunIndex = 4

	// maxNeverRunSelectedPct is the maximum percentage of tests that can be auto-selected
	// due to first_run being null (never run). This safety valve prevents CI overload.
	maxNeverRunSelectedPct = 0.5
)

// AllRows are all the rows returned by snowflake. This is used for testing
var AllRows = []string{"name", "selected", "avg_duration", "last_failure_is_preempt", "first_run"}

//go:embed snowflake_query.sql
var PreparedQuery string

// SqlConnectorFunc is the function to get a sql connector
var SqlConnectorFunc = gosql.Open

// supported suites
var suites = map[string]string{
	"nightly": "roachtest nightly",
}

// TestDetails has the details of the test as fetched from snowflake
type TestDetails struct {
	Name                 string  // test name
	Selected             bool    // whether a test is Selected or not
	AvgDurationInMillis  int64   // average duration of the test
	LastFailureIsPreempt bool    // last failure is due to a VM preemption
	FirstRun             *string // first run timestamp (nil if never run)
}

// SelectTestsReq is the request for CategoriseTests
type SelectTestsReq struct {
	currentBranch string // the current branch for which the selection is being done

	forPastDays int // number of days data to consider for test selection
	firstRunOn  int // number of days to consider for the first time the test is run
	lastRunOn   int // number of days to consider for the last time the test is run

	cloud spec.Cloud // the cloud where the tests were run
	suite string     // the test suite for which the selection is done
}

// NewDefaultSelectTestsReq returns a new SelectTestsReq with default values populated
func NewDefaultSelectTestsReq(cloud spec.Cloud, suite string) *SelectTestsReq {
	currentBranch := os.Getenv("TC_BUILD_BRANCH")
	// Default to master if no TC branch env var is set.
	if currentBranch == "" {
		currentBranch = "master"
	}

	// On non-master branches (e.g., release branches), disable the FirstRunOn
	// criterion by setting it to 0. This prevents marking all tests as "new"
	// when a release branch is first created, since they won't have execution
	// history on that branch yet.
	firstRunOn := defaultFirstRunOn
	if currentBranch != "master" {
		firstRunOn = 0
	}

	return &SelectTestsReq{
		currentBranch: currentBranch,
		forPastDays:   defaultForPastDays,
		firstRunOn:    firstRunOn,
		lastRunOn:     defaultLastRunOn,
		cloud:         cloud,
		suite:         suite,
	}
}

// CategoriseTests returns the tests categorised based on the snowflake query.
// The tests are Selected by selector.go based on certain criteria:
// 1. Tests that have failed recently
// 2. Tests that are new (within firstRunOn days)
// 3. Tests that have not been run for a while (lastRunOn days)
// 4. Tests that have never run (first_run IS NULL in Snowflake)
//
// It returns all the tests with TestDetails.Selected as true for selected tests.
// The tests are sorted by the last run and is used for further test selection criteria,
// so the order should not be modified.
//
// For first run scenarios (e.g., new release branch with no Snowflake history),
// falls back to querying master branch data to determine test selection.
//
// A safety valve is applied in all scenarios to prevent too many never-run tests
// from being selected at once. See applySafetyValve for details.
func CategoriseTests(ctx context.Context, req *SelectTestsReq) ([]*TestDetails, error) {
	allTestDetails, err := querySnowflake(ctx, req)
	if err != nil {
		return nil, err
	}

	// Handle first run scenario: if Snowflake returned no test history for the current branch
	// (e.g., new release branch), fall back to querying master branch data.
	//
	// How this achieves full coverage over multiple days:
	// Day 1: Uses master's test selection (e.g., 480/1000 tests). The 480 selected tests run
	//        and get recorded in Snowflake. The 520 skipped tests are also recorded in Snowflake
	//        as "IGNORED" entries with first_run=NULL (never actually executed).
	// Day 2+: Snowflake now has 1000 entries for this branch. The SQL query auto-selects the 520
	//         tests with first_run=NULL. Safety valve caps this to prevent overload, and tests
	//         gradually cycle through until all achieve coverage within 2-3 days.
	if len(allTestDetails) == 0 && req.currentBranch != "master" {
		masterReq := &SelectTestsReq{
			currentBranch: "master",
			forPastDays:   req.forPastDays,
			firstRunOn:    defaultFirstRunOn, // use master's firstRunOn setting
			lastRunOn:     req.lastRunOn,
			cloud:         req.cloud,
			suite:         req.suite,
		}
		allTestDetails, err = querySnowflake(ctx, masterReq)
		if err != nil {
			return nil, err
		}
	}

	// Apply safety valve to prevent too many never-run tests from being selected.
	// This applies to all scenarios (normal operation, first-run, etc.) but is most
	// impactful on Day 2 of new branches when many skipped tests from Day 1 are
	// auto-selected due to first_run IS NULL.
	applySafetyValve(allTestDetails, maxNeverRunSelectedPct)

	return allTestDetails, nil
}

// querySnowflake queries Snowflake for test execution history and returns test details.
func querySnowflake(ctx context.Context, req *SelectTestsReq) ([]*TestDetails, error) {
	db, err := getConnect(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	statement, err := db.Prepare(PreparedQuery)
	if err != nil {
		return nil, err
	}
	defer func() { _ = statement.Close() }()
	// add the parameters in sequence
	rows, err := statement.QueryContext(ctx, req.forPastDays*-1, req.currentBranch,
		fmt.Sprintf("%%%s - %s%%", suites[req.suite], req.cloud),
		req.firstRunOn*-1, req.lastRunOn*-1)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
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
	// allTestDetails are all the tests that are returned by the snowflake query
	allTestDetails := make([]*TestDetails, 0)
	for rows.Next() {
		err = rows.Scan(colPointers...)
		if err != nil {
			return nil, err
		}
		testInfos := make([]string, len(colContainer))
		copy(testInfos, colContainer)
		// selected columns:
		// 0. test name
		// 1. whether a test is Selected or not
		// 2. average duration of the test
		// 3. last failure is due to an infra flake
		// 4. first run timestamp (empty string if never run)
		var firstRun *string
		if testInfos[DataFirstRunIndex] != "" {
			firstRun = &testInfos[DataFirstRunIndex]
		}
		testDetails := &TestDetails{
			Name:                 testInfos[DataTestNameIndex],
			Selected:             testInfos[DataSelectedIndex] != "no",
			AvgDurationInMillis:  getDuration(testInfos[DataDurationIndex]),
			LastFailureIsPreempt: testInfos[DataLastPreempted] == "yes",
			FirstRun:             firstRun,
		}
		allTestDetails = append(allTestDetails, testDetails)
	}

	return allTestDetails, nil
}

// applySafetyValve ensures that no more than maxPct of tests are auto-selected
// due to first_run being null (never run). If exceeded, it randomly deselects
// half of the never-run tests to prevent CI overload.
//
// Example scenario with 100 tests and maxPct=0.5 on a new release branch:
//
// Day 1 (new branch, uses master branch fallback):
//   - Master selects 48 tests → 48 tests run on new branch, 52 skipped
//   - All 100 tests recorded in Snowflake: 48 with first_run set, 52 as "IGNORED" with first_run=NULL
//
// Day 2 (Snowflake has entries from Day 1):
//   - 48 tests that ran: have first_run set → go through normal selection criteria
//   - 52 tests that were skipped: first_run IS NULL in Snowflake → auto-selected by SQL
//   - Safety valve triggers: 52/100 = 52% > 50% threshold
//   - Randomly deselects half of never-run tests: 52/2 = 26 tests deselected
//   - Result: 26 never-run tests selected + existing selections = controlled load
//
// Day 3:
//   - Remaining ~26 tests with first_run=NULL are auto-selected
//   - 26/100 = 26% < 50% threshold → safety valve does not trigger
//   - All tests achieve coverage by Day 3 while respecting the 50% safety limit
//
// Note: Skipped tests are recorded in Snowflake as "IGNORED" entries, which is why
// they appear in Day 2+ queries and can be auto-selected based on first_run IS NULL.
func applySafetyValve(allTestDetails []*TestDetails, maxPct float64) {
	totalTests := len(allTestDetails)
	if totalTests == 0 {
		return
	}

	// Find tests that were auto-selected due to first_run being null
	var neverRunSelected []*TestDetails
	for _, td := range allTestDetails {
		if td.Selected && td.FirstRun == nil {
			neverRunSelected = append(neverRunSelected, td)
		}
	}

	// Check if they exceed the threshold
	selectedPct := float64(len(neverRunSelected)) / float64(totalTests)
	if selectedPct <= maxPct {
		return // within limits
	}

	// Exceeds threshold: randomly deselect half of them
	numToDeselect := len(neverRunSelected) / 2
	rand.Shuffle(len(neverRunSelected), func(i, j int) {
		neverRunSelected[i], neverRunSelected[j] = neverRunSelected[j], neverRunSelected[i]
	})

	for i := 0; i < numToDeselect; i++ {
		neverRunSelected[i].Selected = false
	}
}

// getDuration extracts the duration from the snowflake query duration field
func getDuration(durationStr string) int64 {
	duration, _ := strconv.ParseInt(durationStr, 10, 64)
	return duration
}

// getConnect makes connection to snowflake and returns the connection.
func getConnect(_ context.Context) (*gosql.DB, error) {
	dsn, err := getDSN()
	if err != nil {
		return nil, err
	}
	db, err := SqlConnectorFunc("snowflake", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// getDSN returns the dataSource name for snowflake driver
func getDSN() (string, error) {
	username, privateKeyStr, err := getSFCreds()
	if err != nil {
		return "", err
	}
	privateKey, err := loadPrivateKey(privateKeyStr)
	if err != nil {
		return "", err
	}
	return sf.DSN(&sf.Config{
		Account:       account,
		Database:      database,
		Schema:        schema,
		Warehouse:     warehouse,
		Authenticator: sf.AuthTypeJwt,
		User:          username,
		PrivateKey:    privateKey,
	})
}

// getSFCreds gets the snowflake credentials from the secrets manager
func getSFCreds() (string, string, error) {
	username := os.Getenv(sfUsernameEnv)
	privateKey := os.Getenv(sfPrivateKey)
	if username == "" {
		return "", "", fmt.Errorf("environment variable %s is not set", sfUsernameEnv)
	}
	if privateKey == "" {
		return "", "", fmt.Errorf("environment variable %s is not set", sfPrivateKey)
	}
	return username, privateKey, nil
}

// loadPrivateKey loads an RSA private key by parsing the PEM-encoded key bytes.
func loadPrivateKey(privateKeyString string) (privateKey *rsa.PrivateKey, err error) {
	block, _ := pem.Decode([]byte(privateKeyString))
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block containing the key")
	}

	var parsedKey interface{}
	if parsedKey, err = x509.ParsePKCS8PrivateKey(block.Bytes); err != nil {
		parsedKey, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, err
		}
	}

	privateKey, ok := parsedKey.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("not an RSA private key")
	}

	return privateKey, nil
}
