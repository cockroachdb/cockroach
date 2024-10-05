// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package skip

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic/metamorphicutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// SkippableTest is a testing.TB with Skip methods.
type SkippableTest interface {
	Helper()
	Skip(...interface{})
	Skipf(string, ...interface{})
}

// WithIssue skips this test, logging the given issue ID as the reason.
func WithIssue(t SkippableTest, githubIssueID int, args ...interface{}) {
	t.Helper()
	maybeSkip(t, fmt.Sprintf("https://github.com/cockroachdb/cockroach/issues/%d", githubIssueID),
		args...)
}

// Unimplemented skips this test case, loggint the given issue ID. It
// is included in addition to WithIssue to allow the caller to signal
// that this test is not being skipped because of a bug, but rather
// because of an unimplemented feature.
func Unimplemented(t SkippableTest, githubIssueID int, args ...interface{}) {
	t.Helper()
	maybeSkip(t, withIssue("unimplemented", githubIssueID), args...)
}

// IgnoreLint skips this test, explicitly marking it as not a test that
// should be tracked as a "skipped test" by external tools. You should use this
// if, for example, your test should only be run in Race mode.
//
// Does not respect COCKROACH_FORCE_RUN_SKIPPED_TESTS.
func IgnoreLint(t SkippableTest, args ...interface{}) {
	t.Helper()
	t.Skip(args...)
}

// IgnoreLintf is like IgnoreLint, and it also takes a format string.
//
// Does not respect COCKROACH_FORCE_RUN_SKIPPED_TESTS.
func IgnoreLintf(t SkippableTest, format string, args ...interface{}) {
	t.Helper()
	t.Skipf(format, args...)
}

// UnderDeadlock skips this test if the deadlock detector is enabled.
func UnderDeadlock(t SkippableTest, args ...interface{}) {
	t.Helper()
	if syncutil.DeadlockEnabled {
		maybeSkip(t, "disabled under deadlock detector", args...)
	}
}

// UnderDeadlockWithIssue skips this test if the deadlock detector is enabled,
// logging the given issue ID as the reason.
func UnderDeadlockWithIssue(t SkippableTest, githubIssueID int, args ...interface{}) {
	t.Helper()
	if syncutil.DeadlockEnabled {
		maybeSkip(t, withIssue("disabled under deadlock detector", githubIssueID), args...)
	}
}

// UnderRace skips this test if the race detector is enabled.
func UnderRace(t SkippableTest, args ...interface{}) {
	t.Helper()
	if util.RaceEnabled {
		maybeSkip(t, "disabled under race", args...)
	}
}

// UnderRaceWithIssue skips this test if the race detector is enabled,
// logging the given issue ID as the reason.
func UnderRaceWithIssue(t SkippableTest, githubIssueID int, args ...interface{}) {
	t.Helper()
	if util.RaceEnabled {
		maybeSkip(t, withIssue("disabled under race", githubIssueID), args...)
	}
}

// UnderBazelWithIssue skips this test if we are building inside bazel,
// logging the given issue ID as the reason.
func UnderBazelWithIssue(t SkippableTest, githubIssueID int, args ...interface{}) {
	t.Helper()
	if bazel.BuiltWithBazel() {
		maybeSkip(t, withIssue("disabled under bazel", githubIssueID), args...)
	}
}

// Ignore unused warnings.
var _ = UnderBazelWithIssue

// UnderShort skips this test if the -short flag is specified.
func UnderShort(t SkippableTest, args ...interface{}) {
	t.Helper()
	if testing.Short() {
		maybeSkip(t, "disabled under -short", args...)
	}
}

// UnderStress skips this test when running under stress.
func UnderStress(t SkippableTest, args ...interface{}) {
	t.Helper()
	if Stress() {
		maybeSkip(t, "disabled under stress", args...)
	}
}

// UnderStressWithIssue skips this test when running under stress, logging the
// given issue ID as the reason.
func UnderStressWithIssue(t SkippableTest, githubIssueID int, args ...interface{}) {
	t.Helper()
	if Stress() {
		maybeSkip(t, withIssue("disabled under stress", githubIssueID), args...)
	}
}

// UnderMetamorphic skips this test during metamorphic runs, which are tests
// run with the metamorphic build tag.
func UnderMetamorphic(t SkippableTest, args ...interface{}) {
	t.Helper()
	if metamorphicutil.IsMetamorphicBuild {
		maybeSkip(t, "disabled under metamorphic", args...)
	}
}

// UnderMetamorphicWithIssue skips this test during metamorphic runs, which are
// tests run with the metamorphic build tag, logging the given issue ID as the
// reason.
func UnderMetamorphicWithIssue(t SkippableTest, githubIssueID int, args ...interface{}) {
	t.Helper()
	if metamorphicutil.IsMetamorphicBuild {
		maybeSkip(t, withIssue("disabled under metamorphic", githubIssueID), args...)
	}
}

// UnderNonTestBuild skips this test if the build does not have the crdb_test
// tag.
func UnderNonTestBuild(t SkippableTest) {
	if !buildutil.CrdbTestBuild {
		maybeSkip(t, "crdb_test tag required for this test")
	}
}

// UnderDuress skips the test if we are running under any of the
// conditions we have observed as producing slow builds.
func UnderDuress(t SkippableTest, args ...interface{}) {
	t.Helper()
	if Duress() {
		skipReason := fmt.Sprintf("duress (current config %s)", testConfig())
		maybeSkip(t, skipReason, args...)
	}
}

// UnderDuressWithIssue skips the test if we are running under any of the
// conditions we have observed as producing slow builds.
func UnderDuressWithIssue(t SkippableTest, githubIssueID int, args ...interface{}) {
	t.Helper()
	if Duress() {
		skipReason := fmt.Sprintf("duress (current config %s)", testConfig())
		maybeSkip(t, withIssue(skipReason, githubIssueID), args...)
	}
}

// Duress catures the conditions that currently lead us to
// believe that tests may be slower than normal.
func Duress() bool {
	return util.RaceEnabled || Stress() || syncutil.DeadlockEnabled
}

// UnderBench returns true iff a test is currently running under `go
// test -bench`.  When true, tests should avoid writing data on
// stdout/stderr from goroutines that run asynchronously with the
// test.
func UnderBench() bool {
	// We use here the understanding that `go test -bench` runs the
	// test executable with `-test.bench 1`.
	f := flag.Lookup("test.bench")
	return f != nil && f.Value.String() != ""
}

// UnderRemoteExecution skips the given test under remote test execution.
func UnderRemoteExecutionWithIssue(t SkippableTest, githubIssueID int, args ...interface{}) {
	t.Helper()
	isRemote := os.Getenv("REMOTE_EXEC")
	if len(isRemote) > 0 {
		maybeSkip(t, withIssue("disabled under race", githubIssueID), args...)
	}

}

func testConfig() string {
	configs := []string{}
	if Stress() {
		configs = append(configs, "stress")
	}
	if util.RaceEnabled {
		configs = append(configs, "race")
	}
	if syncutil.DeadlockEnabled {
		configs = append(configs, "deadlock")
	}
	return strings.Join(configs, ",")
}

func withIssue(reason string, githubIssueID int) string {
	return fmt.Sprintf(
		"%s. issue: https://github.com/cockroachdb/cockroach/issues/%d",
		reason,
		githubIssueID,
	)
}

var forceRunSkippedTests = envutil.EnvOrDefaultBool("COCKROACH_FORCE_RUN_SKIPPED_TESTS", false)

func maybeSkip(t SkippableTest, reason string, args ...interface{}) {
	if forceRunSkippedTests {
		return
	}

	t.Skip(append([]interface{}{reason}, args...)...)
}
