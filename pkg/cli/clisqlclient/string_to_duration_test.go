// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlclient

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestStringToDuration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		input       string
		output      time.Duration
		expectedErr string
	}{
		{"00:00:00", 0, ""},
		{"01:02:03", time.Hour + 2*time.Minute + 3*time.Second, ""},
		{"11:22:33", 11*time.Hour + 22*time.Minute + 33*time.Second, ""},
		{"1234:22:33", 1234*time.Hour + 22*time.Minute + 33*time.Second, ""},
		{"01:02:03.4", time.Hour + 2*time.Minute + 3*time.Second + 400*time.Millisecond, ""},
		{"01:02:03.004", time.Hour + 2*time.Minute + 3*time.Second + 4*time.Millisecond, ""},
		{"01:02:03.123456", time.Hour + 2*time.Minute + 3*time.Second + 123456*time.Microsecond, ""},
		{"1001:02:03.123456", 1001*time.Hour + 2*time.Minute + 3*time.Second + 123456*time.Microsecond, ""},
		{"00:00", 0, "invalid format"},
		{"00.00.00", 0, "invalid format"},
		{"00:00:00:000000000", 0, "invalid format"},
		{"00:00:00.000000000", 0, "invalid format"},
		{"123 00:00:00.000000000", 0, "invalid format"},
	}

	for _, tc := range testCases {
		v, err := stringToDuration(tc.input)
		if !testutils.IsError(err, tc.expectedErr) {
			t.Errorf("%s: expected error %q, got: %v", tc.input, tc.expectedErr, err)
		}
		if err == nil {
			if v != tc.output {
				t.Errorf("%s: expected %v, got %v", tc.input, tc.output, v)
			}
		}
	}
}
