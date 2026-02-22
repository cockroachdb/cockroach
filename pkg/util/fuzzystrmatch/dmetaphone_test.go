// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fuzzystrmatch

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestDMetaphone(t *testing.T) {
	// Test cases verified against PostgreSQL dmetaphone()/dmetaphone_alt() output.
	tt := []struct {
		Source          string
		ExpectedPrimary string
		ExpectedAlt     string
	}{
		{"gumbo", "KMP", "KMP"},
		{"Richard", "RXRT", "RKRT"},
		{"", "", ""},
		{"aubrey", "APR", "APR"},
		{"lee", "L", "L"},
		{"Smith", "SM0", "XMT"},
		{"Schmidt", "XMT", "SMT"},
		{"school", "SKL", "SKL"},
		{"edgar", "ATKR", "ATKR"},
		{"edge", "AJ", "AJ"},
		{"caesar", "SSR", "SSR"},
		{"Jose", "HS", "HS"},
		{"Thomas", "TMS", "TMS"},
		{"harper", "HRPR", "HRPR"},
		{"knight", "NT", "NT"},
		{"gnat", "NT", "NT"},
		{"pneumatic", "NMTK", "NMTK"},
		{"wrack", "RK", "RK"},
		{"ghislane", "JLN", "JLN"},
		{"ghee", "K", "K"},
		{"laugh", "LF", "LF"},
		{"bough", "P", "P"},
		{"broughton", "PRTN", "PRTN"},
		{"tagliaro", "TKLR", "TLR"},
		{"danger", "TNJR", "TNKR"},
		{"biaggi", "PJ", "PK"},
		{"gallegos", "KLKS", "KKS"},
		{"sugar", "XKR", "SKR"},
		{"island", "ALNT", "ALNT"},
		{"filipowicz", "FLPT", "FLPF"},
		{"zhao", "J", "J"},
		// Words with plain GG (not AGGI/OGGI) should emit "K".
		{"foggy", "FK", "FK"},
		{"egg", "AK", "AK"},
		// Words with TT/TD: the T should still be emitted.
		{"butter", "PTR", "PTR"},
		{"mudd", "MT", "MT"},
		{"mutter", "MTR", "MTR"},
	}

	for _, tc := range tt {
		t.Run(tc.Source, func(t *testing.T) {
			primary := DMetaphone(tc.Source)
			alt := DMetaphoneAlt(tc.Source)
			if primary != tc.ExpectedPrimary {
				t.Errorf("DMetaphone(%q): got primary %q, want %q",
					tc.Source, primary, tc.ExpectedPrimary)
			}
			if alt != tc.ExpectedAlt {
				t.Errorf("DMetaphoneAlt(%q): got alt %q, want %q",
					tc.Source, alt, tc.ExpectedAlt)
			}
		})
	}

	// Run random test cases to make sure we don't panic.
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 1000; i++ {
		l := rng.Int31n(20)
		b := make([]byte, l)
		_, _ = rng.Read(b)
		_ = DMetaphone(string(b))
		_ = DMetaphoneAlt(string(b))
	}
}
