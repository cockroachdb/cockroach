// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package errorutil

import (
	"fmt"
	"strings"
	"testing"
)

// Renumber lines so they're stable no matter what changes above. (We
// could make the regexes accept any string of digits, but we also
// want to make sure that the correct line numbers get captured).
//
//line error_test.go:1000

func TestUnexpectedWithIssueErrorf(t *testing.T) {
	err := UnexpectedWithIssueErrorf(1234, "args: %d %s %f", 1, "two", 3.0)
	exp := "unexpected error: args: 1 two 3.000000"
	if err.Error() != exp {
		t.Errorf("expected message:\n  %s\ngot:\n  %s", exp, err.Error())
	}

	safeMsg := fmt.Sprintf("%+v", err)
	reqHint := "We've been trying to track this particular issue down. Please report your " +
		"reproduction at https://github.com/cockroachdb/cockroach/issues/1234 unless " +
		"that issue seems to have been resolved (in which case you might want to " +
		"update crdb to a newer version)."
	if !strings.Contains(safeMsg, reqHint) {
		t.Errorf("expected substring in error\n%s\ngot:\n%s", exp, safeMsg)
	}

	// Check that the issue number is present in the safe details.
	exp = "issue #1234"
	if !strings.Contains(safeMsg, exp) {
		t.Errorf("expected substring in error\n%s\ngot:\n%s", exp, safeMsg)
	}
}
