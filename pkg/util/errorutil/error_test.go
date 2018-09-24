// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package errorutil

import (
	"testing"
)

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
	exp = "issue #1234: error_test.go:22: args: %d %s %f | int; string; float64"
	if safeMsg != exp {
		t.Errorf("Expected SafeMessage:\n%s\ngot:\n%s", exp, safeMsg)
	}
}
