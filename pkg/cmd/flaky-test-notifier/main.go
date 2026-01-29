// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// Command flaky-test-notifier notifies about flaky tests in the CockroachDB repo.
package main

import (
	"cmp"
	"context"
	"crypto/rsa"
	"crypto/x509"
	gosql "database/sql"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"maps"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/cockroach/pkg/internal/codeowners"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	sf "github.com/snowflakedb/gosnowflake"
)

const (
	account   = "lt53838.us-central1.gcp"
	database  = "DATAMART_PROD"
	schema    = "TEAMCITY"
	warehouse = "COMPUTE_WH"

	// snowflakeUsernameEnv and snowflakePrivateKeyEnv are the environment
	// variables that are used for Snowflake access
	snowflakeUsernameEnv   = "SNOWFLAKE_USER"
	snowflakePrivateKeyEnv = "SNOWFLAKE_PRIVATE_KEY"

	// githubAPITokenEnv is used for posting GitHub issues
	githubAPITokenEnv = "GITHUB_API_TOKEN"

	failRateCutoff = 0.05
	maxAlerts      = 3

	description = `Example usage:

    ./bin/flaky-test-notifier [options]

The following options are available:

    lookback-days (default 7): configure how many days to look back for flaky tests.
    dry-run: do not post any GitHub issues. Just print what would be posted.
    use-test-data: instead of looking up test data, use a hard-coded set of "flaky tests" (for testing).

The following environment variables must be set:

    SNOWFLAKE_USER
    SNOWFLAKE_PRIVATE_KEY
    GITHUB_API_TOKEN
`
)

var (
	flags                     = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	dryRun                    = flags.Bool("dry-run", false, "if true, only print what would be done without making any changes")
	lookbackDays              = flags.Int("lookback-days", 7, "number of days to look back for flaky tests")
	useTestData               = flags.Bool("use-test-data", false, "if true, use hard-coded test data instead of querying Snowflake")
	snowflakeUsername         = os.Getenv(snowflakeUsernameEnv)
	snowflakePrivateKeyString = os.Getenv(snowflakePrivateKeyEnv)
	githubAPIToken            = os.Getenv(githubAPITokenEnv)

	tcHost *url.URL = func() *url.URL {
		u, err := url.Parse("https://teamcity.cockroachdb.com")
		if err != nil {
			panic(err)
		}
		return u
	}()

	co             *codeowners.CodeOwners
	codeownersOnce sync.Once
)

func initCodeOwners() {
	codeownersOnce.Do(func() {
		co_, err := codeowners.DefaultLoadCodeOwners()
		if err != nil {
			panic(err)
		}
		co = co_
	})
}

//go:embed failed-tc-tests.sql
var failedTeamCityTestsQuery string

//go:embed failed-tc-engflow-tests.sql
var failedEngflowTestsQuery string

//go:embed failed-github-engflow-tests.sql
var failedGithubEngflowTestsQuery string

type TestFailure interface {
	Build() string // the name of the build, like unit_tests.
	Package() string
	Test() string
	Links() []*url.URL
	FailureRate() float64 // 0.0-1.0
	TotalRuns() int64     // total number of runs (passes + failures)
}

// A TeamCityTest is a test that is executed directly on TeamCity, not via
// remote execution.
type TeamCityTest struct {
	BuildType    string
	BuildName    string
	TestName     string
	PassCount    int64
	FailedBuilds []int64
}

func (tc *TeamCityTest) Build() string {
	return tc.BuildName
}

func (tc *TeamCityTest) Package() string {
	return trimPackagePrefix(strings.TrimSpace(strings.Split(tc.TestName, ":")[0]))
}

func (tc *TeamCityTest) Test() string {
	return strings.TrimSpace(strings.Split(tc.TestName, ":")[1])
}

func (tc *TeamCityTest) Links() []*url.URL {
	var ret []*url.URL
	for _, b := range tc.FailedBuilds {
		ret = append(ret, tcHost.JoinPath("buildConfiguration", tc.BuildType, strconv.FormatInt(b, 10)))
	}
	return ret
}

func (tc *TeamCityTest) FailureRate() float64 {
	return float64(len(tc.FailedBuilds)) / float64(tc.PassCount+int64(len(tc.FailedBuilds)))
}

func (tc *TeamCityTest) TotalRuns() int64 {
	return tc.PassCount + int64(len(tc.FailedBuilds))
}

// A TeamCityEngflowTest is a test that is executed via EngFlow remote execution,
// but driven by a machine in TeamCity.
type TeamCityEngflowTest struct {
	BuildType    string
	BuildName    string
	Label        string
	TestName     string
	Server       string
	PassCount    int64
	FailedBuilds []string // Array of invocation IDs
}

func (tce *TeamCityEngflowTest) Build() string {
	return tce.BuildName
}

func (tce *TeamCityEngflowTest) Package() string {
	return labelToPkg(tce.Label)
}

func (tce *TeamCityEngflowTest) Test() string {
	return tce.TestName
}

func (tce *TeamCityEngflowTest) Links() []*url.URL {
	return invocationsToLinks(tce.Label, tce.Server, tce.FailedBuilds)
}

func (tce *TeamCityEngflowTest) FailureRate() float64 {
	return float64(len(tce.FailedBuilds)) / float64(tce.PassCount+int64(len(tce.FailedBuilds)))
}

func (tce *TeamCityEngflowTest) TotalRuns() int64 {
	return tce.PassCount + int64(len(tce.FailedBuilds))
}

// A GithubEngflowTest is a test that is executed via EngFlow remote execution,
// but driven by a machine in GitHub Actions.
type GithubEngflowTest struct {
	Server       string
	BuildType    string
	Label        string
	TestName     string
	PassCount    int64
	FailedBuilds []string // Array of invocation IDs
}

func (get *GithubEngflowTest) Build() string {
	return get.BuildType
}

func (get *GithubEngflowTest) Package() string {
	return labelToPkg(get.Label)
}

func (get *GithubEngflowTest) Test() string {
	return get.TestName
}

func (get *GithubEngflowTest) Links() []*url.URL {
	return invocationsToLinks(get.Label, get.Server, get.FailedBuilds)
}

func (get *GithubEngflowTest) FailureRate() float64 {
	return float64(len(get.FailedBuilds)) / float64(get.PassCount+int64(len(get.FailedBuilds)))
}

func (get *GithubEngflowTest) TotalRuns() int64 {
	return get.PassCount + int64(len(get.FailedBuilds))
}

func isSubtest(test string) bool {
	return strings.Contains(test, "/")
}

func labelToPkg(label string) string {
	label = strings.TrimPrefix(label, "//")
	return strings.Split(label, ":")[0]
}

func invocationsToLinks(label string, server string, invocations []string) []*url.URL {
	host, err := url.Parse(fmt.Sprintf("https://%s.cluster.engflow.com", server))
	if err != nil {
		panic(err)
	}
	var ret []*url.URL
	for _, b := range invocations {
		u := host.JoinPath("invocations", "default", b)
		base64Target := base64.StdEncoding.EncodeToString([]byte(label))
		u.Fragment = fmt.Sprintf("targets-%s", base64Target)
		ret = append(ret, u)
	}
	return ret
}

func summarizeTest(t TestFailure) {
	var urlsToPrint []string
	for _, link := range t.Links() {
		urlsToPrint = append(urlsToPrint, link.String())
	}
	fmt.Printf("%s: %s.%s (%+v), %f\n", t.Build(), t.Package(), t.Test(), urlsToPrint, t.FailureRate())
}

func trimPackagePrefix(p string) string {
	return strings.TrimPrefix(p, "github.com/cockroachdb/cockroach/")
}

// cmpFailureRate is a helper function used for sorting the tests by failure rate.
// Notice that the return values are flipped from how they're expected to be
// since we sort in descending order.
func cmpFailureRate(a, b float64) int {
	return -cmp.Compare(a, b)
}

func usage() {
	fmt.Fprint(flags.Output(), description)
	flags.PrintDefaults()
	fmt.Println("")
}

func main() {
	flags.Usage = usage
	if err := flags.Parse(os.Args[1:]); err != nil {
		usage()
		log.Fatal(err)
	}

	if !*useTestData {
		if snowflakeUsername == "" {
			log.Fatalf("must set %s\n", snowflakeUsernameEnv)
		}
		if snowflakePrivateKeyString == "" {
			log.Fatalf("must set %s\n", snowflakePrivateKeyEnv)
		}
	}
	if githubAPIToken == "" && !*dryRun {
		log.Fatalf("must set %s (or use --dry-run)\n", githubAPITokenEnv)
	}

	ctx := context.Background()

	if err := impl(ctx); err != nil {
		log.Fatal(err)
	}
}

func impl(ctx context.Context) error {
	var allTests []TestFailure

	if *useTestData {
		allTests = loadTestData()
	} else {
		db, err := connectToSnowflake(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		tcTests, err := loadTeamCityTests(ctx, db)
		if err != nil {
			return err
		}

		tcEngflowTests, err := loadTCEngflowTests(ctx, db)
		if err != nil {
			return err
		}

		ghEngflowTests, err := loadGithubEngflowTests(ctx, db)
		if err != nil {
			return err
		}

		// Aggregate all tests into a single slice
		for _, t := range tcTests {
			allTests = append(allTests, t)
		}
		for _, t := range tcEngflowTests {
			allTests = append(allTests, t)
		}
		for _, t := range ghEngflowTests {
			allTests = append(allTests, t)
		}
	}

	testsToAlert := chooseTests(allTests)

	fmt.Printf("\nFound %d flaky tests to alert on:\n", len(testsToAlert))
	if *dryRun {
		for _, fs := range testsToAlert {
			fmt.Printf("Test %s.%s:\n", fs[0].Package(), fs[0].Test())
			for _, t := range fs {
				summarizeTest(t)
			}
			fmt.Println("")
		}

		fmt.Println("Dry run mode - not posting any issues")
		return nil
	}

	// Post GitHub issues for flaky tests
	if err := postGitHubIssues(ctx, testsToAlert); err != nil {
		return fmt.Errorf("posting GitHub issues: %w", err)
	}

	return nil
}

func loadTeamCityTests(ctx context.Context, db *gosql.DB) ([]*TeamCityTest, error) {
	fmt.Println("loading tests run on TC...")
	statement, err := db.Prepare(failedTeamCityTestsQuery)
	if err != nil {
		return nil, fmt.Errorf("preparing query: %w", err)
	}
	defer func() { _ = statement.Close() }()

	rows, err := statement.QueryContext(ctx, *lookbackDays)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tests []*TeamCityTest
	for rows.Next() {
		var test TeamCityTest
		var failedBuildsRaw string
		if err := rows.Scan(&test.BuildType, &test.BuildName, &test.TestName, &test.PassCount, &failedBuildsRaw); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		// Parse the Snowflake array string into []int64.
		// No, the Go SQL API cannot handle slices. :(
		test.FailedBuilds, err = parseSnowflakeArray(failedBuildsRaw)
		if err != nil {
			return nil, fmt.Errorf("parsing failed builds array: %w", err)
		}

		tests = append(tests, &test)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	fmt.Printf("loaded %d tests\n", len(tests))

	return tests, nil
}

func loadTCEngflowTests(ctx context.Context, db *gosql.DB) ([]*TeamCityEngflowTest, error) {
	fmt.Println("loading EngFlow tests run in TC...")
	statement, err := db.Prepare(failedEngflowTestsQuery)
	if err != nil {
		return nil, fmt.Errorf("preparing query: %w", err)
	}
	defer func() { _ = statement.Close() }()

	rows, err := statement.QueryContext(ctx, *lookbackDays, *lookbackDays)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tests []*TeamCityEngflowTest
	for rows.Next() {
		var test TeamCityEngflowTest
		var failedBuildsRaw string
		if err := rows.Scan(&test.BuildType, &test.BuildName, &test.Label, &test.TestName, &test.Server, &test.PassCount, &failedBuildsRaw); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		// Parse the Snowflake array string into []string
		test.FailedBuilds, err = parseSnowflakeStringArray(failedBuildsRaw)
		if err != nil {
			return nil, fmt.Errorf("parsing failed builds array: %w", err)
		}

		tests = append(tests, &test)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	fmt.Printf("loaded %d tests\n", len(tests))

	return tests, nil
}

func loadGithubEngflowTests(ctx context.Context, db *gosql.DB) ([]*GithubEngflowTest, error) {
	fmt.Println("loading EngFlow tests run in GitHub Actions...")
	statement, err := db.Prepare(failedGithubEngflowTestsQuery)
	if err != nil {
		return nil, fmt.Errorf("preparing query: %w", err)
	}
	defer func() { _ = statement.Close() }()

	rows, err := statement.QueryContext(ctx, *lookbackDays)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tests []*GithubEngflowTest
	for rows.Next() {
		var test GithubEngflowTest
		var failedBuildsRaw string
		if err := rows.Scan(&test.Server, &test.BuildType, &test.Label, &test.TestName, &test.PassCount, &failedBuildsRaw); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		// Parse the Snowflake array string into []string
		test.FailedBuilds, err = parseSnowflakeStringArray(failedBuildsRaw)
		if err != nil {
			return nil, fmt.Errorf("parsing failed builds array: %w", err)
		}

		tests = append(tests, &test)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	fmt.Printf("loaded %d tests\n", len(tests))

	return tests, nil
}

// loadTestData returns hard-coded test data for testing purposes.
func loadTestData() []TestFailure {
	return []TestFailure{
		&TeamCityTest{
			BuildType:    "Cockroach_UnitTests",
			BuildName:    "Cockroach Unit Tests",
			TestName:     "pkg/cmd/dev: TestDataDriven",
			PassCount:    5,
			FailedBuilds: []int64{12345, 12346, 12347, 12350, 12355},
		},
	}
}

// parseSnowflakeArray parses a Snowflake array string (JSON format) into a slice of int64
func parseSnowflakeArray(arrayStr string) ([]int64, error) {
	if arrayStr == "" || arrayStr == "[]" {
		return []int64{}, nil
	}

	var result []int64
	if err := json.Unmarshal([]byte(arrayStr), &result); err != nil {
		return nil, fmt.Errorf("failed to parse array: %w", err)
	}

	return result, nil
}

// parseSnowflakeStringArray parses a Snowflake array string (JSON format) into a slice of strings
func parseSnowflakeStringArray(arrayStr string) ([]string, error) {
	if arrayStr == "" || arrayStr == "[]" {
		return []string{}, nil
	}

	var result []string
	if err := json.Unmarshal([]byte(arrayStr), &result); err != nil {
		return nil, fmt.Errorf("failed to parse array: %w", err)
	}

	return result, nil
}

func connectToSnowflake(_ context.Context) (*gosql.DB, error) {
	dsn, err := getDSN()
	if err != nil {
		return nil, err
	}
	db, err := gosql.Open("snowflake", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// getDSN returns the dataSource name for snowflake driver
func getDSN() (string, error) {
	privateKey, err := loadPrivateKey()
	if err != nil {
		return "", err
	}
	return sf.DSN(&sf.Config{
		Account:       account,
		Database:      database,
		Schema:        schema,
		Warehouse:     warehouse,
		Authenticator: sf.AuthTypeJwt,
		User:          snowflakeUsername,
		PrivateKey:    privateKey,
	})
}

// loadPrivateKey loads an RSA private key by parsing the PEM-encoded key bytes.
func loadPrivateKey() (privateKey *rsa.PrivateKey, err error) {
	block, _ := pem.Decode([]byte(snowflakePrivateKeyString))
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

func chooseTests(failures []TestFailure) [][]TestFailure {
	// Group tests by Package() and Test()
	type testKey struct {
		pkg  string
		test string
	}
	grouped := make(map[testKey][]TestFailure)
	for _, t := range failures {
		// Skip sub-tests: the parent test will fail too, we'll get that failure.
		if isSubtest(t.Test()) {
			continue
		}
		key := testKey{pkg: t.Package(), test: t.Test()}
		grouped[key] = append(grouped[key], t)
	}

	// Sort so each test has the build with the highest failure rate in front.
	for _, failures := range grouped {
		slices.SortFunc(failures, func(a, b TestFailure) int {
			return cmpFailureRate(a.FailureRate(), b.FailureRate())
		})
	}

	var groupedFailures [][]TestFailure
	for fails := range maps.Values(grouped) {
		groupedFailures = append(groupedFailures, fails)
	}

	slices.SortFunc(groupedFailures, func(a, b []TestFailure) int {
		// NB: the slices are already sorted by failure rate in
		// descending order so we can just look at the first element.
		return cmpFailureRate(a[0].FailureRate(), b[0].FailureRate())
	})

	cutoff := 0
	for cutoff < len(groupedFailures) {
		if cutoff >= maxAlerts {
			break
		}
		if groupedFailures[cutoff][0].FailureRate() < failRateCutoff {
			break
		}
		cutoff += 1
	}

	return groupedFailures[0:cutoff]
}

// postGitHubIssues posts GitHub issues for the given flaky tests.
func postGitHubIssues(ctx context.Context, testsToAlert [][]TestFailure) error {
	initCodeOwners()

	opts := &issues.Options{
		Token:  githubAPIToken,
		Org:    "cockroachdb",
		Repo:   "cockroach",
		Branch: "master",
	}

	for _, failures := range testsToAlert {
		first := failures[0]
		pkgName := first.Package()
		testName := first.Test()

		// Get code owners for this test.
		teams, logs := co.GetTestOwner(pkgName, testName)
		for _, line := range logs {
			log.Println(line)
		}

		var mentions []string
		labels := []string{issues.TestFailureLabel}
		for _, tm := range teams {
			if !tm.SilenceMentions {
				var hasAliases bool
				for al, purp := range tm.Aliases {
					if purp == team.PurposeUnittest {
						hasAliases = true
						mentions = append(mentions, "@"+strings.TrimSpace(string(al)))
					}
				}
				if !hasAliases {
					mentions = append(mentions, "@"+string(tm.Name()))
				}
			}
			labels = append(labels, tm.Labels()...)
		}

		formatter := &flakyTestFormatter{failures: failures}

		req := issues.PostRequest{
			PackageName:     strings.TrimPrefix(pkgName, "pkg/"),
			TestName:        testName,
			Labels:          labels,
			MentionOnCreate: mentions,
		}

		result, err := issues.Post(ctx, log.Default(), formatter, req, opts)
		if err != nil {
			return fmt.Errorf("posting issue for %s.%s: %w", pkgName, testName, err)
		}
		fmt.Printf("Posted issue for %s.%s: %s\n", pkgName, testName, result)
	}

	return nil
}
