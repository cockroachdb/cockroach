// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util"
)

// SkipWithIssue skips this test, logging the given issue ID as the reason.
func SkipWithIssue(t testing.TB, githubIssueID int, args ...interface{}) {
	t.Skipf("https://github.com/cockroachdb/cockroach/issue/%d", append([]interface{}{githubIssueID}, args...))
}

// SkipWithIssuef skips this test, logging the given issue ID as the reason,
// with format arguments.
func SkipWithIssuef(t testing.TB, githubIssueID int, format string, args ...interface{}) {
	t.Skipf("https://github.com/cockroachdb/cockroach/issue/%d: "+format, append([]interface{}{githubIssueID}, args...))
}

// SkipIgnoreLint skips this test, explicitly marking it as not a test that
// should be tracked as a "skipped test" by external tools. You should use this
// if, for example, your test should only be run in Race mode.
func SkipIgnoreLint(t testing.TB, args ...interface{}) {
	t.Skip(args...)
}

// SkipIgnoreLintf is like SkipIgnoreLint, and it also takes a format string.
func SkipIgnoreLintf(t testing.TB, format string, args ...interface{}) {
	t.Skipf(format, args...)
}

// SkipUnderRace skips this test if the race detector is enabled.
func SkipUnderRace(t testing.TB, args ...interface{}) {
	if util.RaceEnabled {
		t.Skip(append([]interface{}{"disabled under race"}, args...))
	}
}

// SkipUnderShort skips this test if the -short flag is enabled.
func SkipUnderShort(t testing.TB, args ...interface{}) {
	if testing.Short() {
		t.Skip(append([]interface{}{"disabled under -short"}, args...))
	}
}

// SkipUnderShort skips this test if the -short flag is enabled.
func SkipUnderStress(t testing.TB, args ...interface{}) {
	if NightlyStress() {
		t.Skip(append([]interface{}{"disabled under stress"}, args...))
	}
}
