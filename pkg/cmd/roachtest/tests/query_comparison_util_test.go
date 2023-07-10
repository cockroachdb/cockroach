// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestUnsortedMatricesDiff is a unit test for the
// unsortedMatricesDiffWithFloatComp() and unsortedMatricesDiff() utility
// functions.
func TestUnsortedMatricesDiff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		name        string
		colTypes    []string
		t1, t2      [][]string
		exactMatch  bool
		approxMatch bool
	}{
		{
			name:       "float exact match",
			colTypes:   []string{"FLOAT8"},
			t1:         [][]string{{"1.2345678901234567"}},
			t2:         [][]string{{"1.2345678901234567"}},
			exactMatch: true,
		},
		{
			name:        "float approx match",
			colTypes:    []string{"FLOAT8"},
			t1:          [][]string{{"1.2345678901234563"}},
			t2:          [][]string{{"1.2345678901234564"}},
			exactMatch:  false,
			approxMatch: true,
		},
		{
			name:        "float no match",
			colTypes:    []string{"FLOAT8"},
			t1:          [][]string{{"1.234567890123"}},
			t2:          [][]string{{"1.234567890124"}},
			exactMatch:  false,
			approxMatch: false,
		},
		{
			name:        "multi float approx match",
			colTypes:    []string{"FLOAT8"},
			t1:          [][]string{{"1.2345678901234567", "1.2345678901234567"}},
			t2:          [][]string{{"1.2345678901234567", "1.2345678901234568"}},
			exactMatch:  false,
			approxMatch: true,
		},
		{
			name:        "string no match",
			colTypes:    []string{"STRING"},
			t1:          [][]string{{"hello"}},
			t2:          [][]string{{"world"}},
			exactMatch:  false,
			approxMatch: false,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			match := unsortedMatricesDiff(tc.t1, tc.t2)
			if tc.exactMatch && match != "" {
				t.Fatalf("unsortedMatricesDiff: expected exact match, got diff: %s", match)
			} else if !tc.exactMatch && match == "" {
				t.Fatalf("unsortedMatricesDiff: expected no exact match, got no diff")
			}

			var err error
			match, err = unsortedMatricesDiffWithFloatComp(tc.t1, tc.t2, tc.colTypes)
			if err != nil {
				t.Fatal(err)
			}
			if tc.exactMatch && match != "" {
				t.Fatalf("unsortedMatricesDiffWithFloatComp: expected exact match, got diff: %s", match)
			} else if !tc.exactMatch && tc.approxMatch && match != "" {
				t.Fatalf("unsortedMatricesDiffWithFloatComp: expected approx match, got diff: %s", match)
			} else if !tc.exactMatch && !tc.approxMatch && match == "" {
				t.Fatalf("unsortedMatricesDiffWithFloatComp: expected no approx match, got no diff")
			}
		})
	}
}
