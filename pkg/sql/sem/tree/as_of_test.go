// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func BenchmarkDecimalToHLC(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	inputs := make([]*apd.Decimal, b.N)
	for n := 0; n < b.N; n++ {
		dec, cond, err := apd.NewFromString(fmt.Sprintf("%d.%010d", rng.Int63(), rng.Int31()))
		if err != nil {
			b.Fatal(err)
		}
		if _, err := cond.GoError(apd.DefaultTraps); err != nil {
			b.Fatal(err)
		}
		inputs[n] = dec
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := DecimalToHLC(inputs[n])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestDecimalToHLC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var testCases = []struct {
		name     string
		input    string
		expected hlc.Timestamp
		err      bool
	}{
		{
			name:     "no logical clock",
			input:    "123456789",
			expected: hlc.Timestamp{WallTime: 123456789},
		},
		{
			name:     "empty logical clock",
			input:    "123456789.0000000000",
			expected: hlc.Timestamp{WallTime: 123456789},
		},
		{
			name:     "logical clock present",
			input:    "123456789.0000000123",
			expected: hlc.Timestamp{WallTime: 123456789, Logical: 123},
		},
		{
			name:     "physical clock too large",
			input:    "18446744073709551616", // 1<<64, too large for (signed) int64
			expected: hlc.Timestamp{},
			err:      true,
		},
		{
			name:     "negative physical clock",
			input:    "-123456789.0000000000",
			expected: hlc.Timestamp{},
			err:      true,
		},
		{
			name:     "logical clock too large, but within 10 digits",
			input:    "123456789.9999999999",
			expected: hlc.Timestamp{},
			err:      true,
		},
		{
			name:     "logical clock too large, more than 10 digits",
			input:    "123456789.00000000001",
			expected: hlc.Timestamp{},
			err:      true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			dec, cond, err := apd.NewFromString(testCase.input)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cond.GoError(apd.DefaultTraps); err != nil {
				t.Fatal(err)
			}
			actual, err := DecimalToHLC(dec)
			if err == nil && testCase.err {
				t.Fatal("expected an error but got none")
			}
			if err != nil && !testCase.err {
				t.Fatalf("expected no error but got one: %v", err)
			}
			if !actual.Equal(testCase.expected) {
				t.Fatalf("incorrect timestamp: expected parsing %q to get %q, got %q", testCase.input, testCase.expected.String(), actual.String())
			}
		})
	}
}
