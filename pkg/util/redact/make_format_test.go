// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package redact

import (
	"fmt"
	"testing"
)

// TestMakeFormat verifies that the makeFormat() helper is able to
// reproduce the format given as input to fmt function.
func TestMakeFormat(t *testing.T) {
	testData := []string{
		"%c", "%v", "%q",
		"%3f", "%.3f", "%2.3f",
		"%# v", "%012s",
		"%+v", "%-12s",
	}

	for _, test := range testData {
		justV, revFmt := getFormat(test)
		if (test == "%v") != justV {
			t.Errorf("%q: expected justV %v, got %v", test, test == "%v", justV)
		}
		if revFmt != test {
			t.Errorf("%q: got %q instead", test, revFmt)
		}
	}
}

type formatTester struct {
	fn func(fmt.State, rune)
}

func (f formatTester) Format(s fmt.State, verb rune) {
	f.fn(s, verb)
}

func getFormat(testFmt string) (justV bool, revFmt string) {
	f := formatTester{func(s fmt.State, verb rune) {
		justV, revFmt = MakeFormat(s, verb)
	}}
	_ = fmt.Sprintf(testFmt, f)
	return
}
