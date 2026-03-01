// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fuzzystrmatch

import (
	"fmt"
	"slices"
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

		// Non-ASCII / UTF-8 inputs (PostgreSQL compatibility).
		{"Müller", []string{"689000"}},
		{"Schäfer", []string{"479000"}},
		{"Straßburg", []string{"294795"}},
		{"Éregon", []string{"095600"}},
		{"gąszczu", []string{"540000", "564000"}},
		{"brzęczy", []string{"744000", "746400", "794400", "794640"}},
		{"ţamas", []string{"364000", "464000"}},
		{"țamas", []string{"364000", "464000"}},
		{"ZĄBEK", []string{"467500", "475000"}},
	}

	for _, tc := range tt {
		t.Run(tc.Source, func(t *testing.T) {
			got := DaitchMokotoff(tc.Source)
			if !slices.Equal(got, tc.Expected) {
				t.Errorf("DaitchMokotoff(%q):\n  got:  %s\n  want: %s",
					tc.Source,
					fmt.Sprintf("{%s}", strings.Join(got, ",")),
					fmt.Sprintf("{%s}", strings.Join(tc.Expected, ",")))
			}
		})
	}
}

func TestDMApplyCodesDeduplicatesEquivalentBranches(t *testing.T) {
	branches := []dmBranch{{}, {}}
	codes := []dmCodes{
		{"5", "5", "5"},
		{"5", "5", "5"},
	}

	got := dmApplyCodes(branches, codes, 0)
	if len(got) != 1 {
		t.Fatalf("expected 1 branch after deduplication, got %d", len(got))
	}

	want := dmBranch{
		code:      [dmCodeLen]byte{'5'},
		length:    1,
		lastDigit: '5',
	}
	if got[0] != want {
		t.Fatalf("unexpected deduped branch: got %+v, want %+v", got[0], want)
	}
}
