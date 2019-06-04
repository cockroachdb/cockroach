// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package errorutil

import "testing"

// Renumber lines so they're stable no matter what changes above. (We
// could make the regexes accept any string of digits, but we also
// want to make sure that the correct line numbers get captured).
//
//line error_test.go:1000

func TestUnexpectedWithIssueErrorf(t *testing.T) {
	err := UnexpectedWithIssueErrorf(1234, "args: %d %s %f", 1, "two", 3.0)
	exp := "unexpected error: args: 1 two 3.000000\n" +
		"We've been trying to track this particular issue down. Please report your " +
		"reproduction at https://github.com/cockroachdb/cockroach/issues/1234 unless " +
		"that issue seems to have been resolved (in which case you might want to " +
		"update crdb to a newer version)."
	if err.Error() != exp {
		t.Errorf("Expected message:\n  %s\ngot:\n  %s", exp, err.Error())
	}

	safeMsg := err.(UnexpectedWithIssueErr).SafeMessage()
	exp = "issue #1234: error_test.go:1002: args: %d %s %f | int; string; float64"
	if safeMsg != exp {
		t.Errorf("Expected SafeMessage:\n%s\ngot:\n%s", exp, safeMsg)
	}
}
