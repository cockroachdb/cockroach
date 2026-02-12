// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fuzzystrmatch

import (
	"fmt"
	"strings"
	"testing"
)

func TestDaitchMokotoff(t *testing.T) {
	// Test cases from PostgreSQL's contrib/fuzzystrmatch regression tests.
	tt := []struct {
		Source   string
		Expected []string
	}{
		{"", nil},
		{"Augsburg", []string{"054795"}},
		{"Breuer", []string{"791900"}},
		{"Freud", []string{"793000"}},
		{"Halberstadt", []string{"587433", "587943"}},
		{"Mannheim", []string{"665600"}},
		{"Chernowitz", []string{"496740", "596740"}},
		{"Cherkassy", []string{"495400", "595400"}},
		{"Kleinman", []string{"586660"}},
		{"Berlin", []string{"798600"}},
		{"Ceniow", []string{"467000", "567000"}},
		{"Tsenyuv", []string{"467000"}},
		{"Holubica", []string{"587400", "587500"}},
		{"Golubitsa", []string{"587400"}},
		{"BIERSCHBACH", []string{
			"745740", "745750", "747400", "747500",
			"794574", "794575", "794740", "794750",
		}},
		{"George", []string{"595000"}},
		{"John", []string{"160000", "460000"}},
		{"BESST", []string{"743000"}},
		{"BOUEY", []string{"710000"}},
		{"HANNMANN", []string{"566600"}},
		{"CJC", []string{
			"400000", "440000", "450000", "540000", "545000", "550000",
		}},
	}

	for _, tc := range tt {
		t.Run(tc.Source, func(t *testing.T) {
			got := DaitchMokotoff(tc.Source)
			if !dmSlicesEqual(got, tc.Expected) {
				t.Errorf("DaitchMokotoff(%q):\n  got:  %s\n  want: %s",
					tc.Source,
					fmt.Sprintf("{%s}", strings.Join(got, ",")),
					fmt.Sprintf("{%s}", strings.Join(tc.Expected, ",")))
			}
		})
	}
}

func dmSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
