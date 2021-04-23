// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestExtractTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		filename string
		expected string
	}{
		// v1.0 log file names.
		{"cockroach.kenax.kena.2017-08-16T13_31_00+02_00.028222.log", "2017-08-16 13:31"},
		// v1.1 and later log files.
		{"cockroach.kenax.kena.2021-04-23T09_18_23Z.026682.log", "2021-04-23 09:18"},
		// v19.1 goroutine dumps.
		{"goroutine_dump.double_since_last_dump.2021-04-23T09_18_23.1231", "2021-04-23 09:18"},
		// v20.x and later goroutine dumps.
		{"goroutine_dump.2021-03-11T08_13_57.498.double_since_last_dump.000001137.txt", "2021-03-11 08:13"},
		// v20.1 and later memstats.
		{"memstats.2021-04-22T18_31_54.413.371441664.txt", "2021-04-22 18:31"},
		// v1.0 heap profile names.
		{"memprof.2021-04-22T18_31_54.413", "2021-04-22 18:31"},
		// Transient profile names at some variant of v19.x or v20.x.
		{"memprof.fraction_system_memory.000000019331059712_2020-03-04T16_58_39.54.pprof", "2020-03-04 16:58"},
		{"memprof.000000000030536024_2020-06-15T13_19_19.543", "2020-06-15 13:19"},
		// v20.1 transition format.
		{"memprof.2020-06-15T13_19_19.54.123456", "2020-06-15 13:19"},
		// v20.2 format and later.
		{"memprof.2020-06-15T13_19_19.123.123132.pprof", "2020-06-15 13:19"},
	}

	for _, tc := range testCases {
		tm := extractTimeFromFileName(tc.filename)
		if s := formatTimeSimple(tm); s != tc.expected {
			t.Errorf("%s: expected %q, got %q", tc.filename, tc.expected, s)
		}
	}
}
