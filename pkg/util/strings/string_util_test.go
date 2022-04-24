// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package strings

import "testing"

type StringTest struct {
	ToTest   string
	DupeChar rune
	Expected string
}

func TestCollapseDupeChar(t *testing.T) {
	tests := []StringTest{
		{"%%%%%%%%this is a test%%%%%%%%%", '%', "%this is a test%"},
		{"%%%%%%test1%%%%%%%test2%%%%%test3%%%%", '%', "%test1%test2%test3%"},
		{"I work on ddddddifferent characters", 'd', "I work on different characters"},
	}

	for _, test := range tests {
		result := CollapseDupeChar(test.ToTest, test.DupeChar)

		if result != test.Expected {
			t.Errorf("expected %s but got %s", test.Expected, result)
		}
	}
}

func BenchmarkNoDupe(b *testing.B) {
	toTest := "%test%"

	for n := 0; n < b.N; n++ {
		CollapseDupeChar(toTest, '%')
	}
}

func BenchmarkSingleDupe(b *testing.B) {
	toTest := "%test%%%%"

	for n := 0; n < b.N; n++ {
		CollapseDupeChar(toTest, '%')
	}
}

func BenchmarkMultipleDupe(b *testing.B) {
	toTest := "%%%%%test%%%%"

	for n := 0; n < b.N; n++ {
		CollapseDupeChar(toTest, '%')
	}
}

func BenchmarkSpacedDupe(b *testing.B) {
	toTest := "%%%spaced%%%dupe%%%"

	for n := 0; n < b.N; n++ {
		CollapseDupeChar(toTest, '%')
	}
}
