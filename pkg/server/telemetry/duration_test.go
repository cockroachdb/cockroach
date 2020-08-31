// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package telemetry_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
)

func TestBucketDuration(t *testing.T) {
	testData := []struct {
		input    string
		expected string
	}{
		{"0s", "0s"},
		{"1ns", "1ns"},
		{"200ms", "100ms"},
		{"450µs", "100µs"},
		{"100µs10ns", "100µs"},
		{"100ms10µs", "100ms"},
		{"1s", "1s"},
		{"5s30ms", "1s"},
		{"10s30ms", "10s"},
		{"25s30ms", "20s"},
		{"1m", "1m"},
		{"1m10s", "1m"},
		{"5m100ms", "1m"},
		{"25m", "20m"},
		{"20m10s", "20m"},
		{"1h", "1h"},
		{"1h10s", "1h"},
		{"3h10s", "3h"},
		{"3h40m", "3h30m"},
		{"23h59m", "23h30m"},
		{"24h", "24h"},
		{"24h10s", "24h1s"},
		{"26h10ms", "24h1s"},
		{"72h", "72h"},
		{"77h10m", "72h0m1s"},
		{"240h", "240h"},   // 10 days
		{"264h", "240h"},   // 11 days
		{"2400h", "2400h"}, // 100 days
	}

	for _, tc := range testData {
		input, err := time.ParseDuration(tc.input)
		if err != nil {
			t.Fatalf("%s: %v", tc.input, err)
		}
		expected, err := time.ParseDuration(tc.expected)
		if err != nil {
			t.Fatalf("%s: %v", tc.input, err)
		}
		if actual := telemetry.BucketDuration(input); actual != expected {
			t.Errorf("%s (%d): expected %q (%d), got %q (%d)",
				tc.input, int64(input),
				expected, int64(expected),
				actual, int64(actual))
		}

		negInput := -input
		negExpected := -expected
		if actual := telemetry.BucketDuration(negInput); actual != negExpected {
			t.Errorf("-%s (%d): expected %q (%d), got %q (%d)",
				tc.input, int64(negInput),
				expected, int64(negExpected),
				actual, int64(actual))
		}
	}
}
