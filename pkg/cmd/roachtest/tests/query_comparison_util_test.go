// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
			colTypes:    []string{"FLOAT8", "FLOAT8"},
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
		{
			name:       "mixed types match",
			colTypes:   []string{"STRING", "FLOAT8"},
			t1:         [][]string{{"hello", "1.2345678901234567"}},
			t2:         [][]string{{"hello", "1.2345678901234567"}},
			exactMatch: true,
		},
		{
			name:        "mixed types float approx match",
			colTypes:    []string{"STRING", "FLOAT8"},
			t1:          [][]string{{"hello", "1.23456789012345678"}},
			t2:          [][]string{{"hello", "1.23456789012345679"}},
			exactMatch:  false,
			approxMatch: true,
		},
		{
			name:        "mixed types no match",
			colTypes:    []string{"STRING", "FLOAT8"},
			t1:          [][]string{{"hello", "1.2345678901234567"}},
			t2:          [][]string{{"world", "1.2345678901234567"}},
			exactMatch:  false,
			approxMatch: false,
		},
		{
			name:        "different col count",
			colTypes:    []string{"STRING"},
			t1:          [][]string{{"hello", "1.2345678901234567"}},
			t2:          [][]string{{"world", "1.2345678901234567"}},
			exactMatch:  false,
			approxMatch: false,
		},
		{
			name:        "different row count",
			colTypes:    []string{"STRING", "FLOAT8"},
			t1:          [][]string{{"hello", "1.2345678901234567"}, {"aloha", "2.345"}},
			t2:          [][]string{{"world", "1.2345678901234567"}},
			exactMatch:  false,
			approxMatch: false,
		},
		{
			name:       "multi row unsorted",
			colTypes:   []string{"STRING", "FLOAT8"},
			t1:         [][]string{{"hello", "1.2345678901234567"}, {"world", "1.2345678901234560"}},
			t2:         [][]string{{"world", "1.2345678901234560"}, {"hello", "1.2345678901234567"}},
			exactMatch: true,
		},
		{
			name:        "multi row 0 matches -0",
			colTypes:    []string{"FLOAT4"},
			t1:          [][]string{{"+Inf"}, {"1e-45"}, {"0"}, {"0"}, {"-0.0039"}, {"-Inf"}},
			t2:          [][]string{{"+Inf"}, {"1e-45"}, {"-0"}, {"0"}, {"-0.0039"}, {"-Inf"}},
			exactMatch:  false,
			approxMatch: true,
		},
		{
			name:        "multi row 0 in array matches -0 in array",
			colTypes:    []string{"[]FLOAT4"},
			t1:          [][]string{{"NULL"}, {"{1e-45}"}, {"{0,-0}"}, {"{-0.0039,-Inf"}},
			t2:          [][]string{{"NULL"}, {"{1e-45}"}, {"{-0,0}"}, {"{-0.0039,-Inf"}},
			exactMatch:  false,
			approxMatch: true,
		},
		{
			name:        "decimals with trailing zeroes",
			colTypes:    []string{"DECIMAL"},
			t1:          [][]string{{"1.20"}, {"1.000"}},
			t2:          [][]string{{"1.200"}, {"1"}},
			exactMatch:  false,
			approxMatch: true,
		},
		{
			name:        "decimals with non-trailing zeroes",
			colTypes:    []string{"DECIMAL"},
			t1:          [][]string{{"10"}, {"1.000"}},
			t2:          [][]string{{"1.0"}, {"1000"}},
			exactMatch:  false,
			approxMatch: false,
		},
		{
			name:        "decimals with trailing zeroes in array",
			colTypes:    []string{"[]DECIMAL"},
			t1:          [][]string{{"1.0,1.000"}, {"3.0,4.000"}},
			t2:          [][]string{{"1.00,1"}, {"3.00,4"}},
			exactMatch:  false,
			approxMatch: true,
		},
		{
			name:        "multi row 0 in array matches -0 in array, lib/pq type name",
			colTypes:    []string{"_FLOAT4"}, // this is how []FLOAT4 is named in lib/pq driver
			t1:          [][]string{{"NULL"}, {"{-1}"}, {"{-0}"}, {"{0}"}, {"{NaN}"}},
			t2:          [][]string{{"NULL"}, {"{-1}"}, {"{0}"}, {"{0}"}, {"{NaN}"}},
			exactMatch:  false,
			approxMatch: true,
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
