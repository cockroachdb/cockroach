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
	"os"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
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
)

// AllRows are all the rows returned by snowflake. This is used for testing
var AllRows = []string{"name", "selected", "avg_duration", "last_failure_is_preempt"}

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
	Name                 string // test name
	Selected             bool   // whether a test is Selected or not
	AvgDurationInMillis  int64  // average duration of the test
	LastFailureIsPreempt bool   // last failure is due to a VM preemption
}

// SelectTestsReq is the request for CategoriseTests
type SelectTestsReq struct {
	ForPastDays int // number of days data to consider for test selection
	FirstRunOn  int // number of days to consider for the first time the test is run
	LastRunOn   int // number of days to consider for the last time the test is run

	Cloud spec.Cloud // the cloud where the tests were run
	Suite string     // the test suite for which the selection is done
}

// NewDefaultSelectTestsReq returns a new SelectTestsReq with default values populated
func NewDefaultSelectTestsReq(cloud spec.Cloud, suite string) *SelectTestsReq {
	return &SelectTestsReq{
		ForPastDays: defaultForPastDays,
		FirstRunOn:  defaultFirstRunOn,
		LastRunOn:   defaultLastRunOn,
		Cloud:       cloud,
		Suite:       suite,
	}
}

// CategoriseTests returns the tests categorised based on the snowflake query
// The tests are Selected by selector.go based on certain criteria:
// 1. the number of time a test has been successfully running
// 2. the test is new
// 3. the test has not been run for a while
// It returns all the tests. The selected tests have the value TestDetails.Selected as true
// The tests are sorted by the last run and is used for further test selection criteria. So, the order should not be modified.
func CategoriseTests(
	ctx context.Context, logger *logger.Logger, req *SelectTestsReq,
) ([]*TestDetails, error) {
	currentBranch := os.Getenv("TC_BUILD_BRANCH")
	if currentBranch == "" {
		currentBranch = "master"
	}

	// Can't do this because the literal `%V exists in the query bc `like `%VMs ....``
	//debugQuery := strings.ReplaceAll(PreparedQuery, "?", "%v")
	//logger.Printf(debugQuery)
	//logger.Printf(debugQuery, req.ForPastDays*-1, currentBranch,
	//	fmt.Sprintf("%%%s - %s%%", suites[req.Suite], req.Cloud),
	//	req.FirstRunOn*-1, req.LastRunOn*-1)

	// Print Query & Query Params

	logger.Printf("Query Params:\n%d\n%s\n%s\n%d\n%d", req.ForPastDays*-1, currentBranch,
		fmt.Sprintf("%%%s - %s%%", suites[req.Suite], req.Cloud),
		req.FirstRunOn*-1, req.LastRunOn*-1)

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
	// get the current branch from the teamcity environment
	//currentBranch := os.Getenv("TC_BUILD_BRANCH")
	//if currentBranch == "" {
	//	currentBranch = "master"
	//}
	// add the parameters in sequence
	rows, err := statement.QueryContext(ctx, req.ForPastDays*-1, currentBranch,
		fmt.Sprintf("%%%s - %s%%", suites[req.Suite], req.Cloud),
		req.FirstRunOn*-1, req.LastRunOn*-1)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	//debugQuery := strings.ReplaceAll(PreparedQuery, "?", "%v")
	//logger.Printf(debugQuery, req.ForPastDays*-1, currentBranch,
	//	fmt.Sprintf("%%%s - %s%%", suites[req.Suite], req.Cloud),
	//	req.FirstRunOn*-1, req.LastRunOn*-1)

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
		testDetails := &TestDetails{
			Name:                 testInfos[DataTestNameIndex],
			Selected:             testInfos[DataSelectedIndex] != "no",
			AvgDurationInMillis:  getDuration(testInfos[DataDurationIndex]),
			LastFailureIsPreempt: testInfos[DataLastPreempted] == "yes",
		}
		allTestDetails = append(allTestDetails, testDetails)
	}
	return allTestDetails, nil
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
