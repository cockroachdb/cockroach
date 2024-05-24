// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// test selector is responsible for selecting the tests for running in the roachtest nightlies

package main

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"os"
	"strings"

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

	// bucket and secretsLocation are for fetching the creds from store
	bucket            = "roachtest-data"
	secretsLocation   = "secrets/sfcreds"
	testsFileLocation = "tests/selected"
	testsCsvExtension = "csv"
	project           = "cockroach-ephemeral"
	// secretsDelimiter is used as a delimiter between username and password
	// in the secrets file
	secretsDelimiter = "--||--"
	// preparedQuery is the snowflake query executed to fetch the tests
	preparedQuery = `with builds as (
	select 
  	ID as run_id,
  	min(start_date) as first_run,
  	max(start_date) as last_run,
  from DATAMART_PROD.TEAMCITY.BUILDS
  where
    start_date > DATEADD(DAY, ?, CURRENT_DATE())
    and lower(status) = 'success'
    and branch_name = 'master'
    and lower(name) like ?
	group by 1
), test_stats as (
	select test_name,
		count(case when duration>0 then test_name end) as total_runs,
		count(case when status='SUCCESS' then test_name end) as success_count,
		count(case when status='FAILURE' then test_name end) as failure_count,
		min(case when status!='UNKNOWN' then b.first_run end) as first_run,
		max(case when status!='UNKNOWN' then b.last_run end) as last_run,
		sum(duration) as total_duration
	from DATAMART_PROD.TEAMCITY.TESTS t
		inner join builds b on
			b.run_id=t.build_id
  where test_name is not null
	group  by 1
)
select
	test_name,
  case when
    failure_count>0 or
    first_run > DATEADD(DAY, ?, CURRENT_DATE()) or
    last_run<DATEADD(DAY, ?, CURRENT_DATE()) or
    last_run is null
  then 'yes' else 'no' end as selected,
	case when total_runs>0 then total_duration/total_runs else 0 end as avg_duration,
	total_runs,
from test_stats
order by selected desc, total_runs`
)

var suites = map[string]string{
	"nightly": "roachtest nightly",
}

// testDetails contains the results fetched from the query
type testDetails struct {
	columnHeaders []string
	testInfos     [][]string // the fetched data
}

// SelectTests is responsible for:
// 1. fetching the data from snowflake using the criteria provided
// 2. uploading the results in CSV format to cloud storage
func SelectTests(
	ctx context.Context,
	forPastDays, firstRunOn, lastRunOn, selectFromSuccessPct int,
	cloud, suite string,
) error {
	fmt.Printf("getting test details.\n")
	r, err := getTestDetails(ctx, forPastDays, firstRunOn, lastRunOn, selectFromSuccessPct, cloud, suite)
	if err != nil {
		return err
	}
	csvBuffer, err := getCSVInMemory(r)
	if err != nil {
		return err
	}
	fmt.Printf("uploading %s cloud test details to the cloud...\n", cloud)
	return uploadCSVToGCS(csvBuffer, cloud, suite)
}

// getTestDetails gets the test details from snowflake
func getTestDetails(
	ctx context.Context,
	forPastDays, firstRunOn, lastRunOn, selectFromSuccessPct int,
	cloud, suite string,
) (*testDetails, error) {
	db, err := getConnect(ctx)
	if err != nil {
		return nil, err
	}
	statement, err := db.Prepare(preparedQuery)
	if err != nil {
		return nil, err
	}
	if cloud == "" {
		return nil, fmt.Errorf("unsupported cloud %s", cloud)
	}
	if _, ok := suites[suite]; !ok {
		return nil, fmt.Errorf("unsupported suite %s", suite)
	}
	rows, err := statement.QueryContext(context.Background(), forPastDays*-1,
		fmt.Sprintf("%%%s - %s%%", suites[suite], cloud), firstRunOn*-1, lastRunOn*-1)
	if err != nil {
		return nil, err
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	// Will be used to read data while iterating rows.
	colPointers := make([]interface{}, len(cols))
	colContainer := make([]string, len(cols))
	for i := range colPointers {
		colPointers[i] = &colContainer[i]
	}
	skippedTests := make([][]string, 0)
	td := &testDetails{
		columnHeaders: cols,
		testInfos:     make([][]string, 0),
	}
	selectedTestCount := 0
	for rows.Next() {
		_ = rows.Scan(colPointers...)
		testInfos := make([]string, len(colContainer))
		copy(testInfos, colContainer)
		if testInfos[1] == "no" {
			skippedTests = append(skippedTests, testInfos)
		} else {
			td.testInfos = append(td.testInfos, testInfos)
			selectedTestCount++
		}
	}
	if selectFromSuccessPct > 0 && len(skippedTests) > 0 {
		numberOfTests := len(skippedTests) * selectFromSuccessPct / 100
		for i := range skippedTests {
			if i == numberOfTests {
				break
			}
			skippedTests[i][0] = "yes"
			selectedTestCount++
		}
	}
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
	options := []option.ClientOption{option.WithScopes(storage.ScopeFullControl)}
	cj := os.Getenv("GOOGLE_EPHEMERAL_CREDENTIALS")
	if len(cj) != 0 {
		options = append(options, option.WithCredentialsJSON([]byte(cj)))
	} else {
		fmt.Printf("GOOGLE_EPHEMERAL_CREDENTIALS env is not set.\n")
	}
	client, err := storage.NewClient(ctx, options...)
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

// getSFCreds gets the snowflake credentials from rhe cloud store
func getSFCreds(ctx context.Context) (string, string, error) {
	client, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadOnly))
	if err != nil {
		return "", "", fmt.Errorf("failed to create GCS client: %w", err)
	}
	obj := client.Bucket(bucket).Object(secretsLocation)
	r, err := obj.NewReader(ctx)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = r.Close() }()
	body, err := io.ReadAll(r)
	creds := string(body)
	if err != nil {
		return "", "", err
	}
	userPass := strings.Split(creds, secretsDelimiter)
	return userPass[0], userPass[1], nil
}
