// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package skip

import (
	"flag"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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
	t.Skip(append([]interface{}{
		fmt.Sprintf("https://github.com/cockroachdb/cockroach/issues/%d", githubIssueID)},
		args...))
}

// IgnoreLint skips this test, explicitly marking it as not a test that
// should be tracked as a "skipped test" by external tools. You should use this
// if, for example, your test should only be run in Race mode.
func IgnoreLint(t SkippableTest, args ...interface{}) {
	t.Helper()
	t.Skip(args...)
}

// IgnoreLintf is like IgnoreLint, and it also takes a format string.
func IgnoreLintf(t SkippableTest, format string, args ...interface{}) {
	t.Helper()
	t.Skipf(format, args...)
}

// UnderDeadlock skips this test if the deadlock detector is enabled.
func UnderDeadlock(t SkippableTest, args ...interface{}) {
	t.Helper()
	if syncutil.DeadlockEnabled {
		t.Skip(append([]interface{}{"disabled under deadlock detector"}, args...))
	}
}

// UnderDeadlockWithIssue skips this test if the deadlock detector is enabled,
// logging the given issue ID as the reason.
func UnderDeadlockWithIssue(t SkippableTest, githubIssueID int, args ...interface{}) {
	t.Helper()
	if syncutil.DeadlockEnabled {
		t.Skip(append([]interface{}{fmt.Sprintf(
			"disabled under deadlock detector. issue: https://github.com/cockroachdb/cockroach/issue/%d",
			githubIssueID,
		)}, args...))
	}
}

// UnderRace skips this test if the race detector is enabled.
func UnderRace(t SkippableTest, args ...interface{}) {
	t.Helper()
	if util.RaceEnabled {
		t.Skip(append([]interface{}{"disabled under race"}, args...))
	}
}

// UnderRaceWithIssue skips this test if the race detector is enabled,
// logging the given issue ID as the reason.
func UnderRaceWithIssue(t SkippableTest, githubIssueID int, args ...interface{}) {
	t.Helper()
	if util.RaceEnabled {
		t.Skip(append([]interface{}{fmt.Sprintf(
			"disabled under race. issue: https://github.com/cockroachdb/cockroach/issue/%d", githubIssueID,
		)}, args...))
	}
}

// UnderBazel skips this test if run under bazel.
func UnderBazel(t SkippableTest, args ...interface{}) {
	t.Helper()
	if bazel.BuiltWithBazel() {
		t.Skip(append([]interface{}{"disabled under bazel"}, args...))
	}
}

// UnderBazelWithIssue skips this test if we are building inside bazel,
// logging the given issue ID as the reason.
func UnderBazelWithIssue(t SkippableTest, githubIssueID int, args ...interface{}) {
	t.Helper()
	if bazel.BuiltWithBazel() {
		t.Skip(append([]interface{}{fmt.Sprintf(
			"disabled under bazel. issue: https://github.com/cockroachdb/cockroach/issue/%d", githubIssueID,
		)}, args...))
	}
}

// Ignore unused warnings.
var _ = UnderBazelWithIssue

// UnderShort skips this test if the -short flag is specified.
func UnderShort(t SkippableTest, args ...interface{}) {
	t.Helper()
	if testing.Short() {
		t.Skip(append([]interface{}{"disabled under -short"}, args...))
	}
}

// UnderStress skips this test when running under stress.
func UnderStress(t SkippableTest, args ...interface{}) {
	t.Helper()
	if NightlyStress() {
		t.Skip(append([]interface{}{"disabled under stress"}, args...))
	}
}

// UnderStressRace skips this test during stressrace runs, which are tests
// run under stress with the -race flag.
func UnderStressRace(t SkippableTest, args ...interface{}) {
	t.Helper()
	if NightlyStress() && util.RaceEnabled {
		t.Skip(append([]interface{}{"disabled under stressrace"}, args...))
	}
}

// UnderMetamorphic skips this test during metamorphic runs, which are tests
// run with the metamorphic build tag.
func UnderMetamorphic(t SkippableTest, args ...interface{}) {
	t.Helper()
	if util.IsMetamorphicBuild() {
		t.Skip(append([]interface{}{"disabled under metamorphic"}, args...))
	}
}

// UnderNonTestBuild skips this test if the build does not have the crdb_test
// tag.
func UnderNonTestBuild(t SkippableTest) {
	if !buildutil.CrdbTestBuild {
		t.Skip("crdb_test tag required for this test")
	}
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
