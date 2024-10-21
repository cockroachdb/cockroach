// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/testselector"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	registry.OverrideTeams(team.Map{
		OwnerUnitTest.ToTeamAlias(): {},
	})
}

func makeRegistry(names ...string) testRegistryImpl {
	r := makeTestRegistry()
	dummyRun := func(context.Context, test.Test, cluster.Cluster) {}

	for _, name := range names {
		r.Add(registry.TestSpec{
			Name:             name,
			Owner:            OwnerUnitTest,
			Run:              dummyRun,
			Cluster:          spec.MakeClusterSpec(0),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
		})
	}

	return r
}

func TestSampleSpecs(t *testing.T) {
	r := makeRegistry("abc/1234", "abc/5678", "abc/9292", "abc/2313", "abc/5656", "abc/2233", "abc/1893", "def/1234", "ghi", "jkl/1234")
	filter, err := registry.NewTestFilter([]string{})
	if err != nil {
		t.Fatal(err)
	}

	for _, f := range []float64{0.01, 0.5, 1.0} {
		t.Run(fmt.Sprintf("Sample-%.3f", f), func(t *testing.T) {
			specs, _ := testsToRun(r, filter, false /* runSkipped */, f /* selectProbability */, false /* print */)

			matched := map[string]int{"abc": 0, "def": 0, "ghi": 0, "jkl": 0}
			for _, s := range specs {
				prefix := strings.Split(s.Name, "/")[0]
				matched[prefix]++
			}

			for prefix, count := range matched {
				if count == 0 {
					t.Errorf("expected match but none found for prefix %s", prefix)
				}
			}
		})
	}

	filter, err = registry.NewTestFilter([]string{"abc"})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []float64{0.01, 0.5, 1.0} {
		t.Run(fmt.Sprintf("Sample-abc-%.3f", f), func(t *testing.T) {
			specs, _ := testsToRun(r, filter, false /* runSkipped */, f /* selectProbability */, false /* print */)

			matched := map[string]int{"abc": 0, "def": 0, "ghi": 0, "jkl": 0}
			for _, s := range specs {
				prefix := strings.Split(s.Name, "/")[0]
				matched[prefix]++
			}

			for prefix, count := range matched {
				if prefix == "abc" {
					if count == 0 {
						t.Errorf("expected match but none found for prefix %s", prefix)
					}
				} else if count > 0 {
					t.Errorf("unexpected match for prefix %s", prefix)
				}
			}
		})
	}
}

func Test_updateSpecForSelectiveTests(t *testing.T) {
	ctx := context.Background()
	var mock sqlmock.Sqlmock
	var db *gosql.DB
	var err error
	_ = os.Setenv("SFUSER", "dummy_user")
	_ = os.Setenv("SFPASSWORD", "dummy_password")
	testselector.SqlConnectorFunc = func(_, _ string) (*gosql.DB, error) {
		return db, err
	}
	t.Run("expect CategoriseTests to fail which causes fall back to run all tests", func(t *testing.T) {
		// The failure of the CategoriseTests does not cause any failure. It falls back to run all the tests in the spec
		db, mock, err = sqlmock.New()
		require.Nil(t, err)
		specs, _ := getTestSelectionMockData()
		mock.ExpectPrepare(regexp.QuoteMeta(testselector.PreparedQuery)).WillReturnError(fmt.Errorf("failed to prepare"))
		updateSpecForSelectiveTests(ctx, specs, func(format string, args ...interface{}) {
			t.Logf(format, args...)
		})
		for _, s := range specs {
			if !strings.Contains(s.Name, "skipped") {
				require.Empty(t, s.Skip)
			} else {
				require.Equal(t, "test spec skip", s.Skip)
			}
		}
	})
	t.Run("expect no failure", func(t *testing.T) {
		db, mock, err = sqlmock.New()
		require.Nil(t, err)
		oldSuite := roachtestflags.Suite
		roachtestflags.Suite = registry.Nightly
		defer func() {
			roachtestflags.Suite = oldSuite
		}()
		oldSuccessfulTestsSelectPct := roachtestflags.SuccessfulTestsSelectPct
		roachtestflags.SuccessfulTestsSelectPct = 0.30
		defer func() {
			roachtestflags.SuccessfulTestsSelectPct = oldSuccessfulTestsSelectPct
		}()
		specs, rows := getTestSelectionMockData()
		mock.ExpectPrepare(regexp.QuoteMeta(testselector.PreparedQuery))
		mock.ExpectQuery(regexp.QuoteMeta(testselector.PreparedQuery)).WillReturnRows(rows)
		specsLengthBefore := len(specs)
		updateSpecForSelectiveTests(ctx, specs, func(format string, args ...interface{}) {
			t.Logf(format, args...)
		})
		require.Equal(t, specsLengthBefore, len(specs))
		for _, s := range specs {
			if strings.Contains(s.Name, "success_skip_selector") {
				require.Equal(t, "test selector", s.Skip, s.Name)
				require.Equal(t, "test skipped because it is stable and selective-tests is set.", s.SkipDetails, s.Name)
			} else if strings.Contains(s.Name, "skipped") {
				require.Equal(t, "test spec skip", s.Skip, s.Name)
				require.Equal(t, "test spec skip test", s.SkipDetails, s.Name)
			} else {
				require.Empty(t, s.Skip, s.Name)
			}
			if strings.Contains(s.Name, "_preempted") {
				require.True(t, s.IsLastFailurePreempt(), s.Name)
			} else {
				require.False(t, s.IsLastFailurePreempt(), s.Name)
			}
		}
	})
	// We run the randomized tests 100 times for 100 randomly generated tests
	t.Run("run with randomised data", func(t *testing.T) {
		iteration := 0
		totalIterations := 100

		// Too many goroutines causes this test to OOM.
		if util.RaceEnabled {
			t.Log("race enabled, reducing totalIterations to 10")
			totalIterations = 10
		}
		totalTestCount := 100
		oldSuite := roachtestflags.Suite
		roachtestflags.Suite = registry.Nightly
		defer func() {
			roachtestflags.Suite = oldSuite
		}()
		oldSuccessfulTestsSelectPct := roachtestflags.SuccessfulTestsSelectPct
		roachtestflags.SuccessfulTestsSelectPct = 0.30
		defer func() {
			roachtestflags.SuccessfulTestsSelectPct = oldSuccessfulTestsSelectPct
		}()
		// each iteration is run in a go routine
		wg := sync.WaitGroup{}
		// we need to ensure that "SqlConnectorFunc" returns the right DB for each iteration. So, this locks
		// each iteration till the db is returned
		mu := syncutil.Mutex{}
		for iteration < totalIterations {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				dbs, mocks, err := sqlmock.New()
				require.Nil(t, err)
				specs, data, rows, toBeSelectedTestsMap := getRandomisedTests(t, totalTestCount)
				mocks.ExpectPrepare(regexp.QuoteMeta(testselector.PreparedQuery))
				mocks.ExpectQuery(regexp.QuoteMeta(testselector.PreparedQuery)).WillReturnRows(rows)
				specsLengthBefore := len(specs)
				// we need to keep a count of all already skipped tests. This is to assert that the skip is not overwritten.
				countOfSpecSkippedTests := 0
				for _, s := range specs {
					if s.Skip != "" {
						countOfSpecSkippedTests++
					}
				}
				// the iteration locks till the SqlConnectorFunc returns the current DB.
				mu.Lock()
				testselector.SqlConnectorFunc = func(_, _ string) (*gosql.DB, error) {
					defer mu.Unlock()
					return dbs, err
				}
				updateSpecForSelectiveTests(ctx, specs, func(format string, args ...interface{}) {
					t.Logf(format, args...)
				})
				require.Equal(t, specsLengthBefore, len(specs))
				// dataMap is used of assertions
				dataMap := make(map[string][]string)
				for _, d := range data {
					dataMap[d[testselector.DataTestNameIndex]] = d
				}
				// allTestsToBeSelected keeps a count of all tests that are there in spec and toBeSelectedTestsMap
				// the count should be equal to the length of toBeSelectedTestsMap which ensures that all the tests
				// we selected
				allTestsToBeSelected := 0
				// get the count of skipped tests that are not skipped by test selector.
				// this should remain the same as countOfSpecSkippedTests.
				skippedTestsInSpec := 0
				for _, s := range specs {
					if s.Skip == getSkippedMessage(s.Name) {
						skippedTestsInSpec++
					}
					// if the test is present in dataMap and snowflake response has "last_failure_is_preempt=true", the same
					// must be marked in the spec
					if d, ok := dataMap[s.Name]; ok && d[testselector.DataLastPreempted] == "yes" {
						require.True(t, s.IsLastFailurePreempt())
					} else {
						require.False(t, s.IsLastFailurePreempt())
					}
					// if the test is present in dataMap and snowflake response and the test is skipped by test selector
					// the entry dataMap must be "selected=no"
					if d, ok := dataMap[s.Name]; ok && s.Skip == "test selector" {
						require.Equal(t, "no", d[testselector.DataSelectedIndex])
					}
					// if a test is in toBeSelectedTestsMap, this test has to be selected by test selector unless it is skipped already
					// So, the Skip is either blank or has a value as "<test name> skipped"
					if _, ok := toBeSelectedTestsMap[s.Name]; ok {
						allTestsToBeSelected++
						assert.True(t, s.Skip == "" || s.Skip == getSkippedMessage(s.Name), s.Skip)
					}
				}
				require.Equal(t, countOfSpecSkippedTests, skippedTestsInSpec)
				require.Equal(t, allTestsToBeSelected, len(toBeSelectedTestsMap))
			}(iteration)
			iteration++
		}
		wg.Wait()
	})
}

func getSkippedMessage(testName string) string {
	return fmt.Sprintf("%s skipped", testName)
}

// getRandomisedTests returns "totalTests" number of randomised tests.
// Out of the totalTests,
// > 0-90% of the tests are added as tests in specs
// > 10-100% of the tests are added as snowflake returned tests
// So, the overlap is 80%
// The above tests are shuffled and randomly added different conditions.
// The function returns:
// > List of test specs
// > The rows to be returned by the snowflake as [][]string
// > The rows as *sqlmock.Rows
// > The map of tests that will be selected based on the percent criteria
func getRandomisedTests(
	t *testing.T, totalTests int,
) ([]registry.TestSpec, [][]string, *sqlmock.Rows, map[string]struct{}) {
	r, _ := randutil.NewTestRand()
	testNames, err := generateUniqueStrings(r, totalTests, 10, 100)
	require.Nil(t, err)
	// As mentioned,
	// 0% to 90% of tests are added to test spec
	// 10% to 100% of tests are added to snowflake tests
	// So, the overlap is 80%
	sfStart := int(float32(len(testNames)) * 0.1)
	specEnd := int(float32(len(testNames)) * 0.9)
	// testForSF are the name of tests which are returned by SF query
	testForSF := shuffleStrings(r, testNames[sfStart:])
	// testForSpecs are the name of tests which are in the test spec
	testForSpecs := shuffleStrings(r, testNames[:specEnd])
	// commonSuccessTestMap contains all test that are present in both specs and snowflake minus the tests that are
	// marked as "selected=true"
	commonSuccessTestMap := make(map[string]struct{})
	for _, tn := range testNames[sfStart:specEnd] {
		// all the common tests are added first. The tests marked as "selected=true" will be removed later.
		commonSuccessTestMap[tn] = struct{}{}
	}
	rows := sqlmock.NewRows(testselector.AllRows)
	// data is created and returned for easier assertions
	data := make([][]string, len(testForSF))
	// testSelected is a randomised value which is the number of tests that are marked as "selected=true"
	testSelected := randutil.RandIntInRange(r, 0, int(0.6*float32(len(testForSF))))
	for i, sfn := range testForSF {
		selected := "no"
		lastFailureIsPreempt := "no"
		if i < testSelected {
			// mark the tests as selected sequentially
			selected = "yes"
			// delete the entry from the success map as this is selected.
			delete(commonSuccessTestMap, sfn)
			// from the selected tests mark a few tests as "last_failure_is_preempt=yes"
			numTestsPreempted := randutil.RandIntInRange(r, 0, testSelected)
			if numTestsPreempted <= int(0.05*float64(testSelected)) {
				lastFailureIsPreempt = "yes"
			}
		}
		// generate a random duration value
		duration := randutil.RandIntInRange(r, 0, 10000000)
		// populate the data
		data[i] = []string{sfn, selected, strconv.Itoa(duration + 1), lastFailureIsPreempt}
		// add the same to the rows
		rows.FromCSVString(strings.Join(data[i], ","))
	}
	// numberOfTestsToSelect is based on the same criteria of successful percent selection
	// Here, commonSuccessTestMap contains all the successful tests, so, we have calculated the percent from that
	numberOfTestsToSelect := int(math.Ceil(float64(len(commonSuccessTestMap)) * roachtestflags.SuccessfulTestsSelectPct))
	selectedTestCount := 0
	// toBeSelectedTestsMap contains all the tests that will be selected based on the success percent criteria
	toBeSelectedTestsMap := make(map[string]struct{})
	// iterating over all the data in sequence to identify the exact tests that will be selected as a part of
	// the percentage  criteria
	for _, d := range data {
		// if the test name is not in commonSuccessTestMap, this should be skipped as this test is not in spec
		if _, ok := commonSuccessTestMap[d[testselector.DataTestNameIndex]]; !ok {
			continue
		}
		// add the test to the toBeSelectedTestsMap
		toBeSelectedTestsMap[d[testselector.DataTestNameIndex]] = struct{}{}
		// if the selectedTestCount exceeds numberOfTestsToSelect, we stop.
		selectedTestCount++
		if selectedTestCount >= numberOfTestsToSelect {
			break
		}
	}
	// populate the test specs
	specs := make([]registry.TestSpec, len(testForSpecs))
	for i, sn := range testForSpecs {
		s := registry.TestSpec{Name: sn}
		// randomly mark "Randomized=true" for 1% of tests
		randomizedTests := randutil.RandIntInRange(r, 0, len(testForSpecs))
		if randomizedTests <= int(0.01*float64(len(testForSpecs))) {
			s.Randomized = true
		}
		// randomly mark test as opted out for 3% of tests
		optOutTests := randutil.RandIntInRange(r, 0, len(testForSpecs))
		if optOutTests <= int(0.03*float64(len(testForSpecs))) {
			s.TestSelectionOptOutSuites = registry.Suites(registry.Nightly)
		}
		// randomly mark test as skipped out for 5% of tests
		skippedTests := randutil.RandIntInRange(r, 0, len(testForSpecs))
		if skippedTests <= int(0.05*float64(len(testForSpecs))) {
			s.Skip = getSkippedMessage(sn)
			s.SkipDetails = fmt.Sprintf("%s skipped by spec", sn)
		}
		s.Suites = registry.Suites(registry.Nightly, registry.Weekly)
		specs[i] = s
	}
	return specs, data, rows, toBeSelectedTestsMap
}

// getTestSelectionMockData returns the mock data as:
// 1. List of test specs.
// 2. List of rows to be returned by snowflake.
// The data follows a convention:
// all tests which contains "_skipped" are skipped in the spec
// all tests which contains "_selected" are selected="yes"
// all tests which contains "_preempted" are last_failure_is_preempt="yes"
// all tests which contains "_skip_selector" are skipped based on the criteria
// all tests which contains "_missing" are missing in the test spec
// all tests which contains "_success" are selected="no"
// all tests which contains "new_test" are present in specs, but missing in snowflake rows
// all tests which contains "_randomized" are set in spec as Randomized=true
// Other than the above conventions, the name of the test is randomly put to represent random names for tests.
// the test names are also shuffled to ensure that the test is more close to real data.
func getTestSelectionMockData() ([]registry.TestSpec, *sqlmock.Rows) {
	specs := []registry.TestSpec{
		{Name: "t2_selected_preempted"}, // selected by test selector for selected="yes"
		{Name: "t_skipped_selected", Skip: "test spec skip", SkipDetails: "test spec skip test"},
		{Name: "t1_success"},                       // selected by test selector based on the percentage criteria
		{Name: "t_randomized", Randomized: true},   // not skipped by test selector as Randomized=true
		{Name: "t_new_test_missing_in_sf_query_1"}, // selected even as this is missing in sf query result
		{Name: "t2_success"},                       // selected by test selector based on the percentage criteria
		{Name: "t3_success"},                       // selected by test selector based on the percentage criteria
		{Name: "t_opt_out", TestSelectionOptOutSuites: registry.Suites(registry.Nightly)}, // selected as this test is opted out of Nightly
		// opt out suite does not match, so, even if opt out is mentioned, the test is skipped
		{Name: "t6_success_skip_selector", TestSelectionOptOutSuites: registry.Suites(registry.Weekly)},
		{Name: "t_skipped_new_test", Skip: "test spec skip", SkipDetails: "test spec skip test"},     // skipped in spec
		{Name: "t_skipped_not_selected", Skip: "test spec skip", SkipDetails: "test spec skip test"}, // skipped in spec
		{Name: "t5_success_skip_selector"},       // skipped by test selector based on the percentage criteria
		{Name: "t1_selected"},                    // selected by test selector for selected="yes"
		{Name: "t8_success_skip_selector"},       // skipped by test selector based on the percentage criteria
		{Name: "t_new_test_missing_in_sf_query"}, // selected even as this is missing in sf query result
		{Name: "t10_success_skip_selector"},      // skipped by test selector based on the percentage criteria
	}
	for _, s := range specs {
		s.Suites = registry.Suites(registry.Nightly, registry.Weekly)
	}
	// data are the rows returned by snowflake. This is a 2-dimensional list of strings represent the rows
	// and columns that are returned. Each column has values for 4 rows in sequence:
	// "name", "selected", "avg_duration", "last_failure_is_preempt"
	data := [][]string{
		{"t1_selected", "yes", "1", "no"},
		{"t2_selected_preempted", "yes", "2", "yes"},
		{"t_skipped_selected", "yes", "3", "no"},
		// tests from here are selected under percent criteria
		{"t1_success", "no", "4", "no"},
		{"t2_success", "no", "6", "no"},
		{"t3_success", "no", "7", "no"},
		// test is opted out from test selection
		{"t_opt_out", "no", "8", "no"},
		{"t4_missing", "no", "9", "no"},
		// tests from here are skipped as these are beyond the percentage criteria
		{"t5_success_skip_selector", "no", "10", "no"},
		{"t6_success_skip_selector", "no", "11", "no"},
		// the test is skipped by test selector, but is already skipped in spec
		{"t_skipped_not_selected", "no", "12", "no"},
		{"t7_missing", "no", "13", "no"},
		{"t8_success_skip_selector", "no", "14", "no"},
		{"t9_missing", "no", "15", "no"},
		{"t10_success_skip_selector", "no", "16", "no"},
		// test is Randomized=true, so, will always be selected even being at the last in the list
		{"t_randomized", "no", "5", "no"},
	}
	// rows are the rows that are returned by snowflake. The list of string represents the columns of each row
	rows := sqlmock.NewRows(testselector.AllRows)
	// rows are populated with the data
	for _, ds := range data {
		rows.FromCSVString(strings.Join(ds, ","))
	}
	return specs, rows
}

// generateUniqueStrings generates the requested number of unique random strings
// with random lengths between minLength and maxLength
func generateUniqueStrings(r *rand.Rand, totalSpecs, minLength, maxLength int) ([]string, error) {
	uniqueStrings := make(map[string]struct{})
	var result []string

	for len(uniqueStrings) < totalSpecs {
		// this generates a random string length within the specified range
		strLength := randutil.RandIntInRange(r, minLength, maxLength)

		// Generate a random string of the chosen length
		randomStr := randutil.RandString(r, strLength, randutil.PrintableKeyAlphabet)

		// we need to ensure that the string is unique
		if _, exists := uniqueStrings[randomStr]; !exists {
			uniqueStrings[randomStr] = struct{}{}
			result = append(result, randomStr)
		}
	}

	return result, nil
}

func shuffleStrings(r *rand.Rand, strings []string) []string {
	r.Shuffle(len(strings), func(i, j int) {
		strings[i], strings[j] = strings[j], strings[i]
	})
	return strings
}
