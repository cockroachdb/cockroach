// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

var issueRegexp = regexp.MustCompile(`See: https://[^\s]+issue-v/(\d+)/[^\s]+`)

type status int

const (
	statusPass status = iota
	statusFail
	statusSkip
)

// This `startOpts` option configures in-memory databases to use a
// fixed (30%) amount of memory and is used in a variety of client
// library tests.
var sqlClientsInMemoryDB = option.InMemoryDB(0.3)

// alterZoneConfigAndClusterSettings changes the zone configurations so that GC
// occurs more quickly and jobs are retained for less time. This is useful for
// most ORM tests because they create/drop/alter tables frequently, which can
// cause thousands of table descriptors and schema change jobs to accumulate
// rapidly, thereby decreasing performance.
func alterZoneConfigAndClusterSettings(
	ctx context.Context, t test.Test, version string, c cluster.Cluster, nodeIdx int,
) error {
	db, err := c.ConnE(ctx, t.L(), nodeIdx)
	if err != nil {
		return err
	}
	defer db.Close()

	createUserStmt := `CREATE USER test_admin`
	if c.IsSecure() {
		createUserStmt = `CREATE USER test_admin WITH PASSWORD 'testpw'`
	}

	for _, cmd := range []string{
		createUserStmt,
		`GRANT admin TO test_admin`,
		`ALTER RANGE default CONFIGURE ZONE USING num_replicas = 1, gc.ttlseconds = 30;`,
		`ALTER TABLE system.public.jobs CONFIGURE ZONE USING num_replicas = 1, gc.ttlseconds = 30;`,
		`ALTER RANGE meta CONFIGURE ZONE USING num_replicas = 1, gc.ttlseconds = 30;`,
		`ALTER RANGE system CONFIGURE ZONE USING num_replicas = 1, gc.ttlseconds = 30;`,
		`ALTER RANGE liveness CONFIGURE ZONE USING num_replicas = 1, gc.ttlseconds = 30;`,

		`SET CLUSTER SETTING kv.range_merge.queue_interval = '50ms'`,
		`SET CLUSTER SETTING jobs.registry.interval.cancel = '180s';`,
		`SET CLUSTER SETTING jobs.registry.interval.gc = '30s';`,
		`SET CLUSTER SETTING jobs.retention_time = '15s';`,
		`SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;`,
		`SET CLUSTER SETTING kv.range_split.by_load_merge_delay = '5s';`,

		// Test with SCRAM password authentication.
		`SET CLUSTER SETTING server.user_login.password_encryption = 'scram-sha-256';`,

		// Enable experimental/preview/compatibility features.
		`SET CLUSTER SETTING sql.defaults.experimental_temporary_tables.enabled = 'true';`,
		`ALTER ROLE ALL SET multiple_active_portals_enabled = 'true';`,
		`ALTER ROLE ALL SET serial_normalization = 'sql_sequence_cached'`,
		`ALTER ROLE ALL SET statement_timeout = '60s'`,
		`ALTER ROLE ALL SET default_transaction_isolation = 'read committed'`,
		`ALTER ROLE ALL SET autocommit_before_ddl = 'true'`,
	} {
		if _, err := db.ExecContext(ctx, cmd); err != nil {
			return err
		}
	}

	return nil
}

// ormTestsResults is a helper struct to be used in all roachtests for ORMs and
// drivers' compatibility.
type ormTestsResults struct {
	currentFailures, allTests                    []string
	failUnexpectedCount, failExpectedCount       int
	ignoredCount, skipCount, unexpectedSkipCount int
	passUnexpectedCount, passExpectedCount       int
	// Put all the results in a giant map of [testname]result.
	results map[string]string
	// Put all issue hints in a map of [testname]issue.
	allIssueHints map[string]string
	runTests      map[string]struct{}
}

func newORMTestsResults() *ormTestsResults {
	return &ormTestsResults{
		results:       make(map[string]string),
		allIssueHints: make(map[string]string),
		runTests:      make(map[string]struct{}),
	}
}

// summarizeAll summarizes the result of running an ORM or a driver test suite
// against a cockroach node. If an unexpected result is observed (for example,
// a test unexpectedly failed or passed), a new blocklist is populated.
func (r *ormTestsResults) summarizeAll(
	t test.Test, ormName, blocklistName string, expectedFailures blocklist, version, tag string,
) {
	// Collect all the tests that were not run.
	notRunCount := 0
	for testName, issue := range expectedFailures {
		if _, ok := r.runTests[testName]; ok {
			continue
		}
		r.allTests = append(r.allTests, testName)
		r.results[testName] = fmt.Sprintf("--- FAIL: %s - %s (not run)", testName, maybeAddGithubLink(issue))
		notRunCount++
	}

	// Log all the test results. We re-order the tests alphabetically here.
	sort.Strings(r.allTests)
	for _, test := range r.allTests {
		result, ok := r.results[test]
		if !ok {
			t.Fatalf("can't find %s in test result list", test)
		}
		t.L().Printf("%s\n", result)
	}

	t.L().Printf("------------------------\n")

	r.summarizeFailed(
		t, ormName, blocklistName, expectedFailures, version, tag, notRunCount,
	)
}

// summarizeFailed prints out the results of running an ORM or a driver test
// suite against a cockroach node. It is similar to summarizeAll except that it
// doesn't pay attention to all the tests - only to the failed ones.
// If a test suite outputs only the failures, then this method should be used.
func (r *ormTestsResults) summarizeFailed(
	t test.Test,
	ormName, blocklistName string,
	expectedFailures blocklist,
	version, latestTag string,
	notRunCount int,
) {
	var bResults strings.Builder
	fmt.Fprintf(&bResults, "Tests run on Cockroach %s\n", version)
	fmt.Fprintf(&bResults, "Tests run against %s %s\n", ormName, latestTag)
	totalTestsRun := r.passExpectedCount + r.passUnexpectedCount + r.failExpectedCount + r.failUnexpectedCount
	fmt.Fprintf(&bResults, "%d Total Tests Run\n",
		totalTestsRun,
	)
	if totalTestsRun == 0 {
		t.Fatal("No tests ran! Fix the testing commands.")
	}

	p := func(msg string, count int) {
		testString := "tests"
		if count == 1 {
			testString = "test"
		}
		fmt.Fprintf(&bResults, "%d %s %s\n", count, testString, msg)
	}
	p("passed", r.passUnexpectedCount+r.passExpectedCount)
	p("failed", r.failUnexpectedCount+r.failExpectedCount)
	p("skipped", r.skipCount)
	p("ignored", r.ignoredCount)
	p("passed unexpectedly", r.passUnexpectedCount)
	p("failed unexpectedly", r.failUnexpectedCount)
	p("expected failed but skipped", r.unexpectedSkipCount)
	p("expected failed but not run", notRunCount)

	fmt.Fprintf(&bResults, "---\n")
	for _, result := range r.results {
		if strings.Contains(result, "unexpected") {
			fmt.Fprintf(&bResults, "%s\n", result)
		}
	}

	fmt.Fprintf(&bResults, "For a full summary look at the %s artifacts \n", ormName)
	t.L().Printf("%s\n", bResults.String())
	t.L().Printf("------------------------\n")

	if r.failUnexpectedCount > 0 || r.passUnexpectedCount > 0 ||
		notRunCount > 0 || r.unexpectedSkipCount > 0 {
		// Create a new blocklist so we can easily update this test.
		sort.Strings(r.currentFailures)
		var b strings.Builder
		fmt.Fprintf(&b, "Here is new %s blocklist that can be used to update the test:\n\n", ormName)
		fmt.Fprintf(&b, "var %s = blocklist{\n", blocklistName)

		prevName := ""
		for _, testName := range r.currentFailures {
			// Avoid putting duplicates in the map. Since the currentFailures was
			// sorted earlier, we just need to keep track of the previous element.
			// The npgsql suite has duplicate test names since it seems to run
			// some tests twice.
			if testName == prevName {
				continue
			}
			prevName = testName
			issue := expectedFailures[testName]
			if len(issue) == 0 || issue == "unknown" {
				issue = r.allIssueHints[testName]
			}
			if len(issue) == 0 {
				issue = "unknown"
			}
			fmt.Fprintf(&b, "  `%s`: \"%s\",\n", testName, issue)
		}
		fmt.Fprintf(&b, "}\n\n")
		t.L().Printf("\n\n%s\n\n", b.String())
		t.L().Printf("------------------------\n")
		t.Fatalf("\n%s\nAn updated blocklist (%s) is available in the artifacts' %s log\n",
			bResults.String(),
			blocklistName,
			ormName,
		)
	}
}
