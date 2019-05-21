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
	"fmt"
	"strings"
	"testing"
)

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
	exp = "-- safe details:\nissue #%d\n-- arg 0: 1234"
	if !strings.Contains(safeMsg, exp) {
		t.Errorf("expected substring in error\n%s\ngot:\n%s", exp, safeMsg)
	}
}
